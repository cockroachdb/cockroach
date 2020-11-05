// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/partitionccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/jackc/pgx"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func init() {
	cloud.RegisterKMSFromURIFactory(MakeTestKMS, "testkms")
}

type sqlDBKey struct {
	server string
	user   string
}

type datadrivenTestState struct {
	servers    map[string]serverutils.TestServerInterface
	dataDirs   map[string]string
	sqlDBs     map[sqlDBKey]*gosql.DB
	cleanupFns []func()
}

func (d *datadrivenTestState) cleanup(ctx context.Context) {
	for _, db := range d.sqlDBs {
		db.Close()
	}
	for _, s := range d.servers {
		s.Stopper().Stop(ctx)
	}
	for _, f := range d.cleanupFns {
		f()
	}
}

func (d *datadrivenTestState) addServer(
	t *testing.T, name, iodir string, allowImplicitAccess bool,
) {
	var tc serverutils.TestClusterInterface
	var cleanup func()
	params := base.TestClusterArgs{}
	if allowImplicitAccess {
		params.ServerArgs.Knobs.BackupRestore = &sql.BackupRestoreTestingKnobs{
			AllowImplicitAccess: true,
		}
	}
	if iodir == "" {
		_, tc, _, iodir, cleanup = backupRestoreTestSetupWithParams(t, singleNode, 0, InitNone, params)
	} else {
		_, tc, _, cleanup = backupRestoreTestSetupEmptyWithParams(t, singleNode, iodir, InitNone, params)
	}
	d.servers[name] = tc.Server(0)
	d.dataDirs[name] = iodir
	d.cleanupFns = append(d.cleanupFns, cleanup)
}

func (d *datadrivenTestState) getIODir(t *testing.T, server string) string {
	dir, ok := d.dataDirs[server]
	if !ok {
		t.Fatalf("server %s does not exist", server)
	}
	return dir
}

func (d *datadrivenTestState) getSQLDB(t *testing.T, server string, user string) *gosql.DB {
	key := sqlDBKey{server, user}
	if db, ok := d.sqlDBs[key]; ok {
		return db
	}
	addr := d.servers[server].ServingSQLAddr()
	pgURL, cleanup := sqlutils.PGUrl(t, addr, "TestBackupRestoreDataDriven", url.User(user))
	d.cleanupFns = append(d.cleanupFns, cleanup)
	db, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	d.sqlDBs[key] = db
	return db
}

func newDatadrivenTestState() datadrivenTestState {
	return datadrivenTestState{
		servers:  make(map[string]serverutils.TestServerInterface),
		dataDirs: make(map[string]string),
		sqlDBs:   make(map[sqlDBKey]*gosql.DB),
	}
}

// TestBackupRestoreDataDriven is a datadriven test to test standard
// backup/restore interactions involving setting up clusters and running
// different SQL commands. The test files are in testdata/backup-restore.
// It has the following commands:
//
// - "new-server name=<name> [share-io-dir=<name>]": create a new server with
//   the input name. It takes in an optional share-io-dir argument to share an
//   IO directory with an existing server. This is useful when restoring from a
//   backup taken in another server.
// - "exec-sql server=<name>": executes the input SQL query on the target server.
//   By default, server is the last created server.
// - "query-sql server=<name>": executes the input SQL query on the target server
//   and expects that the results are as desired. By default, server is the last
//   created server.
// - "reset": clear all state associated with the test.
func TestBackupRestoreDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	datadriven.Walk(t, "testdata/backup-restore/", func(t *testing.T, path string) {
		var lastCreatedServer string
		ds := newDatadrivenTestState()
		defer ds.cleanup(ctx)
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "reset":
				ds.cleanup(ctx)
				ds = newDatadrivenTestState()
				return ""
			case "new-server":
				var name, shareDirWith, iodir string
				var allowImplicitAccess bool
				d.ScanArgs(t, "name", &name)
				if d.HasArg("share-io-dir") {
					d.ScanArgs(t, "share-io-dir", &shareDirWith)
				}
				if shareDirWith != "" {
					iodir = ds.getIODir(t, shareDirWith)
				}
				if d.HasArg("allow-implicit-access") {
					allowImplicitAccess = true
				}
				lastCreatedServer = name
				ds.addServer(t, name, iodir, allowImplicitAccess)
				return ""
			case "exec-sql":
				server := lastCreatedServer
				user := "root"
				if d.HasArg("server") {
					d.ScanArgs(t, "server", &server)
				}
				if d.HasArg("user") {
					d.ScanArgs(t, "user", &user)
				}
				_, err := ds.getSQLDB(t, server, user).Exec(d.Input)
				if err == nil {
					return ""
				}
				return err.Error()
			case "query-sql":
				server := lastCreatedServer
				user := "root"
				if d.HasArg("server") {
					d.ScanArgs(t, "server", &server)
				}
				if d.HasArg("user") {
					d.ScanArgs(t, "user", &user)
				}
				rows, err := ds.getSQLDB(t, server, user).Query(d.Input)
				if err != nil {
					return err.Error()
				}
				// Find out how many output columns there are.
				cols, err := rows.Columns()
				if err != nil {
					t.Fatal(err)
				}
				// Allocate a buffer of *interface{} to write results into.
				elemsI := make([]interface{}, len(cols))
				for i := range elemsI {
					elemsI[i] = new(interface{})
				}
				elems := make([]string, len(cols))

				// Build string output of the row data.
				var output strings.Builder
				for rows.Next() {
					if err := rows.Scan(elemsI...); err != nil {
						t.Fatal(err)
					}
					for i, elem := range elemsI {
						val := *(elem.(*interface{}))
						switch t := val.(type) {
						case []byte:
							// The postgres wire protocol does not distinguish between
							// strings and byte arrays, but our tests do. In order to do
							// The Right Thing™, we replace byte arrays which are valid
							// UTF-8 with strings. This allows byte arrays which are not
							// valid UTF-8 to print as a list of bytes (e.g. `[124 107]`)
							// while printing valid strings naturally.
							if str := string(t); utf8.ValidString(str) {
								elems[i] = str
							}
						default:
							elems[i] = fmt.Sprintf("%v", val)
						}
					}
					output.WriteString(strings.Join(elems, " "))
					output.WriteString("\n")
				}
				return output.String()
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}

func TestBackupRestoreStatementResult(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, _, sqlDB, dir, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	if err := verifyBackupRestoreStatementResult(
		t, sqlDB, "BACKUP DATABASE data TO $1", LocalFoo,
	); err != nil {
		t.Fatal(err)
	}
	// The GZipBackupManifest subtest is to verify that BackupManifest objects
	// have been stored in the GZip compressed format.
	t.Run("GZipBackupManifest", func(t *testing.T) {
		backupDir := fmt.Sprintf("%s/foo", dir)
		backupManifestFile := backupDir + "/" + backupManifestName
		backupManifestBytes, err := ioutil.ReadFile(backupManifestFile)
		if err != nil {
			t.Fatal(err)
		}
		fileType := http.DetectContentType(backupManifestBytes)
		require.Equal(t, ZipType, fileType)
	})

	sqlDB.Exec(t, "CREATE DATABASE data2")

	if err := verifyBackupRestoreStatementResult(
		t, sqlDB, "RESTORE data.* FROM $1 WITH OPTIONS (into_db='data2')", LocalFoo,
	); err != nil {
		t.Fatal(err)
	}
}

func TestBackupRestoreSingleUserfile(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1000
	ctx, tc, _, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	backupAndRestore(ctx, t, tc, []string{"userfile:///a"}, []string{"userfile:///a"}, numAccounts)
}

func TestBackupRestoreSingleNodeLocal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1000
	ctx, tc, _, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	backupAndRestore(ctx, t, tc, []string{LocalFoo}, []string{LocalFoo}, numAccounts)
}

func TestBackupRestoreMultiNodeLocal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1000
	ctx, tc, _, _, cleanupFn := BackupRestoreTestSetup(t, MultiNode, numAccounts, InitNone)
	defer cleanupFn()

	backupAndRestore(ctx, t, tc, []string{LocalFoo}, []string{LocalFoo}, numAccounts)
}

func TestBackupRestoreMultiNodeRemote(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1000
	ctx, tc, _, _, cleanupFn := BackupRestoreTestSetup(t, MultiNode, numAccounts, InitNone)
	defer cleanupFn()
	// Backing up to node2's local file system
	remoteFoo := "nodelocal://2/foo"

	backupAndRestore(ctx, t, tc, []string{remoteFoo}, []string{LocalFoo}, numAccounts)
}

func TestBackupRestorePartitioned(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1000
	ctx, tc, sqlDB, dir, cleanupFn := BackupRestoreTestSetup(t, MultiNode, numAccounts, InitNone)
	defer cleanupFn()

	// Ensure that each node has at least one leaseholder. (These splits were
	// made in BackupRestoreTestSetup.) These are wrapped with SucceedsSoon()
	// because EXPERIMENTAL_RELOCATE can fail if there are other replication
	// changes happening.
	for _, stmt := range []string{
		`ALTER TABLE data.bank EXPERIMENTAL_RELOCATE VALUES (ARRAY[1], 0)`,
		`ALTER TABLE data.bank EXPERIMENTAL_RELOCATE VALUES (ARRAY[2], 100)`,
		`ALTER TABLE data.bank EXPERIMENTAL_RELOCATE VALUES (ARRAY[3], 200)`,
	} {
		testutils.SucceedsSoon(t, func() error {
			_, err := sqlDB.DB.ExecContext(ctx, stmt)
			return err
		})
	}
	const localFoo1 = LocalFoo + "/1"
	const localFoo2 = LocalFoo + "/2"
	const localFoo3 = LocalFoo + "/3"
	backupURIs := []string{
		fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", localFoo1, url.QueryEscape("default")),
		fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", localFoo2, url.QueryEscape("dc=dc1")),
		fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", localFoo3, url.QueryEscape("dc=dc2")),
	}
	restoreURIs := []string{
		localFoo1,
		localFoo2,
		localFoo3,
	}
	backupAndRestore(ctx, t, tc, backupURIs, restoreURIs, numAccounts)

	// Verify that at least one SST exists in each backup destination.
	sstMatcher := regexp.MustCompile(`\d+\.sst`)
	for i := 1; i <= 3; i++ {
		subDir := fmt.Sprintf("%s/foo/%d", dir, i)
		files, err := ioutil.ReadDir(subDir)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, f := range files {
			if sstMatcher.MatchString(f.Name()) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("no SSTs found in %s", subDir)
		}
	}
	// The PartitionGZip subtest is to verify that partition descriptor files
	// are in the GZip compressed format.
	t.Run("PartitionGZip", func(t *testing.T) {
		partitionMatcher := regexp.MustCompile(`^BACKUP_PART_`)
		for i := 1; i <= 3; i++ {
			subDir := fmt.Sprintf("%s/foo/%d", dir, i)
			files, err := ioutil.ReadDir(subDir)
			if err != nil {
				t.Fatal(err)
			}
			for _, f := range files {
				fName := f.Name()
				if partitionMatcher.MatchString(fName) {
					backupPartitionFile := subDir + "/" + fName
					backupPartitionBytes, err := ioutil.ReadFile(backupPartitionFile)
					if err != nil {
						t.Fatal(err)
					}
					fileType := http.DetectContentType(backupPartitionBytes)
					require.Equal(t, ZipType, fileType)
				}
			}
		}
	})
}

func TestBackupRestoreAppend(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// TODO(adityamaru): Unskip once #53727 is merged.
	skip.WithIssue(t, 54039, "flaky test")
	defer log.Scope(t).Close(t)

	const numAccounts = 1000
	ctx, tc, sqlDB, tmpDir, cleanupFn := BackupRestoreTestSetup(t, MultiNode, numAccounts, InitNone)
	defer cleanupFn()

	// Ensure that each node has at least one leaseholder. (These splits were
	// made in BackupRestoreTestSetup.) These are wrapped with SucceedsSoon()
	// because EXPERIMENTAL_RELOCATE can fail if there are other replication
	// changes happening.
	for _, stmt := range []string{
		`ALTER TABLE data.bank EXPERIMENTAL_RELOCATE VALUES (ARRAY[1], 0)`,
		`ALTER TABLE data.bank EXPERIMENTAL_RELOCATE VALUES (ARRAY[2], 100)`,
		`ALTER TABLE data.bank EXPERIMENTAL_RELOCATE VALUES (ARRAY[3], 200)`,
	} {
		testutils.SucceedsSoon(t, func() error {
			_, err := sqlDB.DB.ExecContext(ctx, stmt)
			return err
		})
	}
	const localFoo1, localFoo2, localFoo3 = LocalFoo + "/1", LocalFoo + "/2", LocalFoo + "/3"
	const userfileFoo1, userfileFoo2, userfileFoo3 = `userfile:///bar/1`, `userfile:///bar/2`,
		`userfile:///bar/3`
	makeBackups := func(b1, b2, b3 string) []interface{} {
		return []interface{}{
			fmt.Sprintf("%s?COCKROACH_LOCALITY=%s&AUTH=implicit", b1, url.QueryEscape("default")),
			fmt.Sprintf("%s?COCKROACH_LOCALITY=%s&AUTH=implicit", b2, url.QueryEscape("dc=dc1")),
			fmt.Sprintf("%s?COCKROACH_LOCALITY=%s&AUTH=implicit", b3, url.QueryEscape("dc=dc2"))}
	}
	makeCollections := func(c1, c2, c3 string) []interface{} {
		return []interface{}{
			fmt.Sprintf("%s?COCKROACH_LOCALITY=%s&AUTH=implicit", c1, url.QueryEscape("default")),
			fmt.Sprintf("%s?COCKROACH_LOCALITY=%s&AUTH=implicit", c2, url.QueryEscape("dc=dc1")),
			fmt.Sprintf("%s?COCKROACH_LOCALITY=%s&AUTH=implicit", c3, url.QueryEscape("dc=dc2"))}
	}

	makeCollectionsWithSubdir := func(c1, c2, c3 string) []interface{} {
		return []interface{}{
			fmt.Sprintf("%s/%s?COCKROACH_LOCALITY=%s&AUTH=implicit", c1, "foo", url.QueryEscape("default")),
			fmt.Sprintf("%s/%s?COCKROACH_LOCALITY=%s&AUTH=implicit", c2, "foo", url.QueryEscape("dc=dc1")),
			fmt.Sprintf("%s/%s?COCKROACH_LOCALITY=%s&AUTH=implicit", c3, "foo", url.QueryEscape("dc=dc2"))}
	}

	// for testing backup *into* with specified subdirectory.
	const specifiedSubdir, newSpecifiedSubdir = `subdir`, `subdir2`

	var full1, full2, subdirFull1, subdirFull2 string

	for _, test := range []struct {
		name                  string
		backups               []interface{}
		collections           []interface{}
		collectionsWithSubdir []interface{}
	}{
		{
			"nodelocal",
			makeBackups(localFoo1, localFoo2, localFoo3),
			// for testing backup *into* collection, pick collection shards on each
			// node.
			makeCollections(`nodelocal://0/`, `nodelocal://1/`, `nodelocal://2/`),
			makeCollectionsWithSubdir(`nodelocal://0`, `nodelocal://1`, `nodelocal://2`),
		},
		{
			"userfile",
			makeBackups(userfileFoo1, userfileFoo2, userfileFoo3),
			makeCollections(`userfile:///0`, `userfile:///1`, `userfile:///2`),
			makeCollectionsWithSubdir(`userfile:///0`, `userfile:///1`, `userfile:///2`),
		},
	} {
		var tsBefore, ts1, ts1again, ts2 string
		sqlDB.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&tsBefore)
		sqlDB.Exec(t, "BACKUP TO ($1, $2, $3) AS OF SYSTEM TIME "+tsBefore,
			test.backups...)
		sqlDB.Exec(t, "BACKUP INTO ($1, $2, $3) AS OF SYSTEM TIME "+tsBefore, test.collections...)
		sqlDB.Exec(t, "BACKUP INTO $4 IN ($1, $2, $3) AS OF SYSTEM TIME "+tsBefore,
			append(test.collectionsWithSubdir, specifiedSubdir)...)

		sqlDB.QueryRow(t, "UPDATE data.bank SET balance = 100 RETURNING cluster_logical_timestamp()").Scan(&ts1)
		sqlDB.Exec(t, "BACKUP TO ($1, $2, $3) AS OF SYSTEM TIME "+ts1, test.backups...)
		sqlDB.Exec(t, "BACKUP INTO LATEST IN ($1, $2, $3) AS OF SYSTEM TIME "+ts1, test.collections...)
		// This should be an incremental as we already have a manifest in specifiedSubdir.
		sqlDB.Exec(t, "BACKUP INTO $4 IN ($1, $2, $3) AS OF SYSTEM TIME "+ts1,
			append(test.collectionsWithSubdir, specifiedSubdir)...)

		// Append to latest again, just to prove we can append to an appended one and
		// that appended didn't e.g. mess up LATEST.
		sqlDB.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&ts1again)
		sqlDB.Exec(t, "BACKUP INTO LATEST IN ($1, $2, $3) AS OF SYSTEM TIME "+ts1again, test.collections...)
		// Ensure that LATEST was created (and can be resolved) even when you backed
		// up into a specified subdir to begin with.
		sqlDB.Exec(t, "BACKUP INTO LATEST IN ($1, $2, $3) AS OF SYSTEM TIME "+ts1again,
			test.collectionsWithSubdir...)

		sqlDB.QueryRow(t, "UPDATE data.bank SET balance = 200 RETURNING cluster_logical_timestamp()").Scan(&ts2)
		rowsTS2 := sqlDB.QueryStr(t, "SELECT * from data.bank ORDER BY id")
		sqlDB.Exec(t, "BACKUP TO ($1, $2, $3) AS OF SYSTEM TIME "+ts2, test.backups...)
		// Start a new full-backup in the collection version.
		sqlDB.Exec(t, "BACKUP INTO ($1, $2, $3) AS OF SYSTEM TIME "+ts2, test.collections...)
		// Write to a new subdirectory thereby triggering a full-backup.
		sqlDB.Exec(t, "BACKUP INTO $4 IN ($1, $2, $3) AS OF SYSTEM TIME "+ts2,
			append(test.collectionsWithSubdir, newSpecifiedSubdir)...)

		sqlDB.Exec(t, "ALTER TABLE data.bank RENAME TO data.renamed")
		sqlDB.Exec(t, "BACKUP TO ($1, $2, $3)", test.backups...)
		sqlDB.Exec(t, "BACKUP INTO LATEST IN ($1, $2, $3)", test.collections...)
		sqlDB.Exec(t, "BACKUP INTO $4 IN ($1, $2, $3)", append(test.collectionsWithSubdir,
			newSpecifiedSubdir)...)

		sqlDB.ExpectErr(t, "cannot append a backup of specific", "BACKUP system.users TO ($1, $2, "+
			"$3)", test.backups...)
		//TODO(dt): prevent backing up different targets to same collection?

		sqlDB.Exec(t, "DROP DATABASE data CASCADE")
		sqlDB.Exec(t, "RESTORE DATABASE data FROM ($1, $2, $3)", test.backups...)
		sqlDB.ExpectErr(t, "relation \"data.bank\" does not exist", "SELECT * FROM data.bank ORDER BY id")
		sqlDB.CheckQueryResults(t, "SELECT * from data.renamed ORDER BY id", rowsTS2)

		findFullBackupPaths := func(baseDir, glob string) (string, string) {
			matches, err := filepath.Glob(glob)
			require.NoError(t, err)
			require.Equal(t, 2, len(matches))
			for i := range matches {
				matches[i] = strings.TrimPrefix(filepath.Dir(matches[i]), baseDir)
			}
			return matches[0], matches[1]
		}

		runRestores := func(collections []interface{}, fullBackup1, fullBackup2 string) {
			sqlDB.Exec(t, "DROP DATABASE data CASCADE")
			sqlDB.Exec(t, "RESTORE DATABASE data FROM $4 IN ($1, $2, $3) AS OF SYSTEM TIME "+tsBefore,
				append(collections, fullBackup1)...)

			sqlDB.Exec(t, "DROP DATABASE data CASCADE")
			sqlDB.Exec(t, "RESTORE DATABASE data FROM $4 IN ($1, $2, $3) AS OF SYSTEM TIME "+ts1,
				append(collections, fullBackup1)...)

			sqlDB.Exec(t, "DROP DATABASE data CASCADE")
			sqlDB.Exec(t, "RESTORE DATABASE data FROM $4 IN ($1, $2, $3) AS OF SYSTEM TIME "+ts1again,
				append(collections, fullBackup1)...)

			sqlDB.Exec(t, "DROP DATABASE data CASCADE")
			sqlDB.Exec(t, "RESTORE DATABASE data FROM $4 IN ($1, $2, $3) AS OF SYSTEM TIME "+ts2, append(collections, fullBackup2)...)
		}

		if test.name == "userfile" {
			// Find the backup times in the collection and try RESTORE'ing to each, and
			// within each also check if we can restore to individual times captured with
			// incremental backups that were appended to that backup.
			store, err := cloudimpl.ExternalStorageFromURI(ctx, "userfile:///0",
				base.ExternalIODirConfig{},
				tc.Servers[0].ClusterSettings(),
				blobs.TestEmptyBlobClientFactory, "root",
				tc.Servers[0].InternalExecutor().(*sql.InternalExecutor), tc.Servers[0].DB())
			require.NoError(t, err)
			defer store.Close()
			files, err := store.ListFiles(ctx, "*/*/*/"+backupManifestName)
			require.NoError(t, err)
			full1 = strings.TrimSuffix(files[0], backupManifestName)
			full2 = strings.TrimSuffix(files[1], backupManifestName)

			// Find the full-backups written to the specified subdirectories, and within
			// each also check if we can restore to individual times captured with
			// incremental backups that were appended to that backup.
			subdirFiles, err := store.ListFiles(ctx, path.Join("foo", fmt.Sprintf("%s*",
				specifiedSubdir), backupManifestName))
			require.NoError(t, err)
			subdirFull1 = strings.TrimSuffix(strings.TrimPrefix(subdirFiles[0], "foo"),
				backupManifestName)
			subdirFull2 = strings.TrimSuffix(strings.TrimPrefix(subdirFiles[1], "foo"),
				backupManifestName)
		} else {
			// Find the backup times in the collection and try RESTORE'ing to each, and
			// within each also check if we can restore to individual times captured with
			// incremental backups that were appended to that backup.
			full1, full2 = findFullBackupPaths(tmpDir, path.Join(tmpDir, "*/*/*/"+backupManifestName))

			// Find the full-backups written to the specified subdirectories, and within
			// each also check if we can restore to individual times captured with
			// incremental backups that were appended to that backup.
			subdirFull1, subdirFull2 = findFullBackupPaths(path.Join(tmpDir, "foo"),
				path.Join(tmpDir, "foo", fmt.Sprintf("%s*", specifiedSubdir), backupManifestName))
		}
		runRestores(test.collections, full1, full2)
		runRestores(test.collectionsWithSubdir, subdirFull1, subdirFull2)

		// TODO(dt): test restoring to other backups via AOST.
	}

}

func TestBackupAndRestoreJobDescription(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, _, sqlDB, tmpDir, cleanupFn := BackupRestoreTestSetup(t, MultiNode, numAccounts, InitNone)
	defer cleanupFn()

	const c1, c2, c3 = `nodelocal://0/`, `nodelocal://1/`, `nodelocal://2/`

	const localFoo1, localFoo2, localFoo3 = LocalFoo + "/1", LocalFoo + "/2", LocalFoo + "/3"
	backups := []interface{}{
		fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", localFoo1, url.QueryEscape("default")),
		fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", localFoo2, url.QueryEscape("dc=dc1")),
		fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", localFoo3, url.QueryEscape("dc=dc2")),
	}

	collections := []interface{}{
		fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", c1, url.QueryEscape("default")),
		fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", c2, url.QueryEscape("dc=dc1")),
		fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", c3, url.QueryEscape("dc=dc2")),
	}

	sqlDB.Exec(t, "BACKUP TO ($1, $2, $3)", backups...)
	sqlDB.Exec(t, "BACKUP INTO ($1, $2, $3)", collections...)
	sqlDB.Exec(t, "BACKUP INTO LATEST IN ($1, $2, $3)", collections...)
	sqlDB.Exec(t, "BACKUP INTO $4 IN ($1, $2, $3)", append(collections, "subdir")...)

	// Find the subdirectory created by the full BACKUP INTO statement.
	matches, err := filepath.Glob(path.Join(tmpDir, "*/*/*/"+backupManifestName))
	require.NoError(t, err)
	require.Equal(t, 1, len(matches))
	for i := range matches {
		matches[i] = strings.TrimPrefix(filepath.Dir(matches[i]), tmpDir)
	}
	full1 := matches[0]
	sqlDB.CheckQueryResults(
		t, "SELECT description FROM [SHOW JOBS]",
		[][]string{
			{fmt.Sprintf("BACKUP TO ('%s', '%s', '%s')", backups[0].(string), backups[1].(string),
				backups[2].(string))},
			{fmt.Sprintf("BACKUP INTO '%s' IN ('%s', '%s', '%s')", full1, collections[0],
				collections[1], collections[2])},
			{fmt.Sprintf("BACKUP INTO '%s' IN ('%s', '%s', '%s')", strings.TrimPrefix(full1, "/"),
				collections[0], collections[1], collections[2])},
			{fmt.Sprintf("BACKUP INTO '%s' IN ('%s', '%s', '%s')", "/subdir",
				collections[0], collections[1], collections[2])},
		},
	)

	sqlDB.Exec(t, "DROP DATABASE data CASCADE")
	sqlDB.Exec(t, "RESTORE DATABASE data FROM ($1, $2, $3)", backups...)

	sqlDB.Exec(t, "DROP DATABASE data CASCADE")
	sqlDB.Exec(t, "RESTORE DATABASE data FROM $4 IN ($1, $2, $3)", append(collections, full1)...)

	sqlDB.Exec(t, "DROP DATABASE data CASCADE")
	sqlDB.Exec(t, "RESTORE DATABASE data FROM $4 IN ($1, $2, $3)", append(collections, "subdir")...)

	// The flavors of BACKUP and RESTORE which automatically resolve the right
	// directory to read/write data to, have URIs with the resolved path written
	// to the job description.
	getResolvedCollectionURIs := func(subdir string) []string {
		resolvedCollectionURIs := make([]string, len(collections))
		for i, collection := range collections {
			parsed, err := url.Parse(collection.(string))
			require.NoError(t, err)
			parsed.Path = path.Join(parsed.Path, subdir)
			resolvedCollectionURIs[i] = parsed.String()
		}

		return resolvedCollectionURIs
	}

	resolvedCollectionURIs := getResolvedCollectionURIs(full1)
	resolvedSubdirURIs := getResolvedCollectionURIs("subdir")

	sqlDB.CheckQueryResults(
		t, "SELECT description FROM [SHOW JOBS] WHERE job_type='RESTORE'",
		[][]string{
			{fmt.Sprintf("RESTORE DATABASE data FROM ('%s', '%s', '%s')",
				backups[0].(string), backups[1].(string), backups[2].(string))},
			{fmt.Sprintf("RESTORE DATABASE data FROM ('%s', '%s', '%s')",
				resolvedCollectionURIs[0], resolvedCollectionURIs[1],
				resolvedCollectionURIs[2])},
			{fmt.Sprintf("RESTORE DATABASE data FROM ('%s', '%s', '%s')",
				resolvedSubdirURIs[0], resolvedSubdirURIs[1],
				resolvedSubdirURIs[2])},
		},
	)
}

func TestBackupRestorePartitionedMergeDirectories(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1000
	ctx, tc, _, _, cleanupFn := BackupRestoreTestSetup(t, MultiNode, numAccounts, InitNone)
	defer cleanupFn()

	// TODO (lucy): This test writes a partitioned backup where all files are
	// written to the same directory, which is similar to the case where a backup
	// is created and then all files are consolidated into the same directory, but
	// we should still have a separate test where the files are actually moved.
	const localFoo1 = LocalFoo + "/1"
	backupURIs := []string{
		fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", localFoo1, url.QueryEscape("default")),
		fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", localFoo1, url.QueryEscape("dc=dc1")),
		fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", localFoo1, url.QueryEscape("dc=dc2")),
	}
	restoreURIs := []string{
		localFoo1,
	}
	backupAndRestore(ctx, t, tc, backupURIs, restoreURIs, numAccounts)
}

func TestBackupRestoreEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 0
	ctx, tc, _, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	backupAndRestore(ctx, t, tc, []string{LocalFoo}, []string{LocalFoo}, numAccounts)
}

// Regression test for #16008. In short, the way RESTORE constructed split keys
// for tables with negative primary key data caused AdminSplit to fail.
func TestBackupRestoreNegativePrimaryKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1000

	ctx, tc, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, MultiNode, numAccounts, InitNone)
	defer cleanupFn()

	// Give half the accounts negative primary keys.
	sqlDB.Exec(t, `UPDATE data.bank SET id = $1 - id WHERE id > $1`, numAccounts/2)

	// Resplit that half of the table space.
	sqlDB.Exec(t,
		`ALTER TABLE data.bank SPLIT AT SELECT generate_series($1, 0, $2)`,
		-numAccounts/2, numAccounts/backupRestoreDefaultRanges/2,
	)

	backupAndRestore(ctx, t, tc, []string{LocalFoo}, []string{LocalFoo}, numAccounts)

	sqlDB.Exec(t, `CREATE UNIQUE INDEX id2 ON data.bank (id)`)
	sqlDB.Exec(t, `ALTER TABLE data.bank ALTER PRIMARY KEY USING COLUMNS(id)`)

	var unused string
	var exportedRows, exportedIndexEntries int
	sqlDB.QueryRow(t, `BACKUP DATABASE data TO $1`, LocalFoo+"/alteredPK").Scan(
		&unused, &unused, &unused, &exportedRows, &exportedIndexEntries, &unused,
	)
	if exportedRows != numAccounts {
		t.Fatalf("expected %d rows, got %d", numAccounts, exportedRows)
	}
	expectedIndexEntries := numAccounts * 3 // old PK, new and old secondary idx
	if exportedIndexEntries != expectedIndexEntries {
		t.Fatalf("expected %d index entries, got %d", expectedIndexEntries, exportedIndexEntries)
	}

}

func backupAndRestore(
	ctx context.Context,
	t *testing.T,
	tc *testcluster.TestCluster,
	backupURIs []string,
	restoreURIs []string,
	numAccounts int,
) {
	// uriFmtStringAndArgs returns format strings like "$1" or "($1, $2, $3)" and
	// an []interface{} of URIs for the BACKUP/RESTORE queries.
	uriFmtStringAndArgs := func(uris []string) (string, []interface{}) {
		urisForFormat := make([]interface{}, len(uris))
		var fmtString strings.Builder
		if len(uris) > 1 {
			fmtString.WriteString("(")
		}
		for i, uri := range uris {
			if i > 0 {
				fmtString.WriteString(", ")
			}
			fmtString.WriteString(fmt.Sprintf("$%d", i+1))
			urisForFormat[i] = uri
		}
		if len(uris) > 1 {
			fmtString.WriteString(")")
		}
		return fmtString.String(), urisForFormat
	}

	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)
	{
		sqlDB.Exec(t, `CREATE INDEX balance_idx ON data.bank (balance)`)
		testutils.SucceedsSoon(t, func() error {
			var unused string
			var createTable string
			sqlDB.QueryRow(t, `SHOW CREATE TABLE data.bank`).Scan(&unused, &createTable)
			if !strings.Contains(createTable, "balance_idx") {
				return errors.New("expected a balance_idx index")
			}
			return nil
		})

		var unused string
		var exported struct {
			rows, idx, bytes int64
		}

		backupURIFmtString, backupURIArgs := uriFmtStringAndArgs(backupURIs)
		backupQuery := fmt.Sprintf("BACKUP DATABASE data TO %s", backupURIFmtString)
		sqlDB.QueryRow(t, backupQuery, backupURIArgs...).Scan(
			&unused, &unused, &unused, &exported.rows, &exported.idx, &exported.bytes,
		)
		// When numAccounts == 0, our approxBytes formula breaks down because
		// backups of no data still contain the system.users and system.descriptor
		// tables. Just skip the check in this case.
		if numAccounts > 0 {
			approxBytes := int64(backupRestoreRowPayloadSize * numAccounts)
			if max := approxBytes * 3; exported.bytes < approxBytes || exported.bytes > max {
				t.Errorf("expected data size in [%d,%d] but was %d", approxBytes, max, exported.bytes)
			}
		}
		if expected := int64(numAccounts * 1); exported.rows != expected {
			t.Fatalf("expected %d rows for %d accounts, got %d", expected, numAccounts, exported.rows)
		}

		found := false
		const stmt = "SELECT payload FROM system.jobs ORDER BY created DESC LIMIT 10"
		for rows := sqlDB.Query(t, stmt); rows.Next(); {
			var payloadBytes []byte
			if err := rows.Scan(&payloadBytes); err != nil {
				t.Fatal(err)
			}

			payload := &jobspb.Payload{}
			if err := protoutil.Unmarshal(payloadBytes, payload); err != nil {
				t.Fatal("cannot unmarshal job payload from system.jobs")
			}

			backupManifest := &BackupManifest{}
			backupPayload, ok := payload.Details.(*jobspb.Payload_Backup)
			if !ok {
				t.Logf("job %T is not a backup: %v", payload.Details, payload.Details)
				continue
			}
			backupDetails := backupPayload.Backup
			found = true
			if err := protoutil.Unmarshal(backupDetails.BackupManifest, backupManifest); err != nil {
				t.Fatal("cannot unmarshal backup descriptor from job payload from system.jobs")
			}
			if backupManifest.DeprecatedStatistics != nil {
				t.Fatal("expected statistics field of backup descriptor payload to be nil")
			}
		}
		if !found {
			t.Fatal("scanned job rows did not contain a backup!")
		}
	}

	uri, err := url.Parse(backupURIs[0])
	require.NoError(t, err)
	if uri.Scheme == "userfile" {
		sqlDB.Exec(t, `CREATE DATABASE foo`)
		sqlDB.Exec(t, `USE foo`)
		sqlDB.Exec(t, `DROP DATABASE data CASCADE`)
		restoreURIFmtString, restoreURIArgs := uriFmtStringAndArgs(restoreURIs)
		restoreQuery := fmt.Sprintf("RESTORE DATABASE DATA FROM %s", restoreURIFmtString)
		verifyRestoreData(t, sqlDB, restoreQuery, restoreURIArgs, numAccounts)
	} else {
		// Start a new cluster to restore into.
		// If the backup is on nodelocal, we need to determine which node it's on.
		// Othewise, default to 0.
		backupNodeID := 0
		if err != nil {
			t.Fatal(err)
		}
		if uri.Scheme == "nodelocal" && uri.Host != "" {
			// If the backup is on nodelocal and has specified a host, expect it to
			// be an integer.
			var err error
			backupNodeID, err = strconv.Atoi(uri.Host)
			if err != nil {
				t.Fatal(err)
			}
		}
		args := base.TestServerArgs{ExternalIODir: tc.Servers[backupNodeID].ClusterSettings().ExternalIODir}
		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tcRestore.Stopper().Stop(ctx)
		sqlDBRestore := sqlutils.MakeSQLRunner(tcRestore.Conns[0])

		// Create some other descriptors to change up IDs
		sqlDBRestore.Exec(t, `CREATE DATABASE other`)
		// Force the ID of the restored bank table to be different.
		sqlDBRestore.Exec(t, `CREATE TABLE other.empty (a INT PRIMARY KEY)`)

		restoreURIFmtString, restoreURIArgs := uriFmtStringAndArgs(restoreURIs)
		restoreQuery := fmt.Sprintf("RESTORE DATABASE DATA FROM %s", restoreURIFmtString)
		verifyRestoreData(t, sqlDBRestore, restoreQuery, restoreURIArgs, numAccounts)
	}
}

func verifyRestoreData(
	t *testing.T,
	sqlDB *sqlutils.SQLRunner,
	restoreQuery string,
	restoreURIArgs []interface{},
	numAccounts int,
) {
	var unused string
	var restored struct {
		rows, idx, bytes int64
	}
	sqlDB.QueryRow(t, restoreQuery, restoreURIArgs...).Scan(
		&unused, &unused, &unused, &restored.rows, &restored.idx, &restored.bytes,
	)

	approxBytes := int64(backupRestoreRowPayloadSize * numAccounts)
	if max := approxBytes * 3; restored.bytes < approxBytes || restored.bytes > max {
		t.Errorf("expected data size in [%d,%d] but was %d", approxBytes, max, restored.bytes)
	}
	if expected := int64(numAccounts); restored.rows != expected {
		t.Fatalf("expected %d rows for %d accounts, got %d", expected, numAccounts, restored.rows)
	}
	if expected := int64(numAccounts); restored.idx != expected {
		t.Fatalf("expected %d idx rows for %d accounts, got %d", expected, numAccounts, restored.idx)
	}

	var rowCount int64
	sqlDB.QueryRow(t, `SELECT count(*) FROM data.bank`).Scan(&rowCount)
	if rowCount != int64(numAccounts) {
		t.Fatalf("expected %d rows but found %d", numAccounts, rowCount)
	}

	sqlDB.QueryRow(t, `SELECT count(*) FROM data.bank@balance_idx`).Scan(&rowCount)
	if rowCount != int64(numAccounts) {
		t.Fatalf("expected %d rows but found %d", numAccounts, rowCount)
	}

	// Verify there's no /Table/51 - /Table/51/1 empty span.
	{
		var count int
		sqlDB.QueryRow(t, `
			SELECT count(*) FROM crdb_internal.ranges
			WHERE start_pretty = (
				('/Table/' ||
				(SELECT table_id FROM crdb_internal.tables
					WHERE database_name = $1 AND name = $2
				)::STRING) ||
				'/1'
			)
		`, "data", "bank").Scan(&count)
		if count != 0 {
			t.Fatal("unexpected span start at primary index")
		}
	}
}

func TestBackupRestoreSystemTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 0
	ctx, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, MultiNode, numAccounts, InitNone)
	conn := sqlDB.DB.(*gosql.DB)
	defer cleanupFn()

	// At the time this test was written, these were the only system tables that
	// were reasonable for a user to backup and restore into another cluster.
	tables := []string{"locations", "role_members", "users", "zones"}
	tableSpec := "system." + strings.Join(tables, ", system.")

	// Take a consistent fingerprint of the original tables.
	var backupAsOf string
	expectedFingerprints := map[string][][]string{}
	err := crdb.ExecuteTx(ctx, conn, nil /* txopts */, func(tx *gosql.Tx) error {
		for _, table := range tables {
			rows, err := conn.Query("SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE system." + table)
			if err != nil {
				return err
			}
			defer rows.Close()
			expectedFingerprints[table], err = sqlutils.RowsToStrMatrix(rows)
			if err != nil {
				return err
			}
		}
		// Record the transaction's timestamp so we can take a backup at the
		// same time.
		return conn.QueryRow("SELECT cluster_logical_timestamp()").Scan(&backupAsOf)
	})
	if err != nil {
		t.Fatal(err)
	}

	// Backup and restore the tables into a new database.
	sqlDB.Exec(t, `CREATE DATABASE system_new`)
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP %s TO '%s' AS OF SYSTEM TIME %s`, tableSpec, LocalFoo, backupAsOf))
	sqlDB.Exec(t, fmt.Sprintf(`RESTORE %s FROM '%s' WITH into_db='system_new'`, tableSpec, LocalFoo))

	// Verify the fingerprints match.
	for _, table := range tables {
		a := sqlDB.QueryStr(t, "SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE system_new."+table)
		if e := expectedFingerprints[table]; !reflect.DeepEqual(e, a) {
			t.Fatalf("fingerprints between system.%[1]s and system_new.%[1]s did not match:%s\n",
				table, strings.Join(pretty.Diff(e, a), "\n"))
		}
	}

	// Verify we can't shoot ourselves in the foot by accidentally restoring
	// directly over the existing system tables.
	sqlDB.ExpectErr(
		t, `relation ".+" already exists`,
		fmt.Sprintf(`RESTORE %s FROM '%s'`, tableSpec, LocalFoo),
	)
}

func TestBackupRestoreSystemJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 0
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, MultiNode, numAccounts, InitNone)
	conn := sqlDB.DB.(*gosql.DB)
	defer cleanupFn()

	sanitizedIncDir := LocalFoo + "/inc?AWS_SESSION_TOKEN="
	incDir := sanitizedIncDir + "secretCredentialsHere"

	sanitizedFullDir := LocalFoo + "/full?AWS_SESSION_TOKEN="
	fullDir := sanitizedFullDir + "moarSecretsHere"

	backupDatabaseID := sqlutils.QueryDatabaseID(t, conn, "data")
	backupTableID := sqlutils.QueryTableID(t, conn, "data", "public", "bank")

	sqlDB.Exec(t, `CREATE DATABASE restoredb`)
	restoreDatabaseID := sqlutils.QueryDatabaseID(t, conn, "restoredb")

	// We create a full backup so that, below, we can test that incremental
	// backups sanitize credentials in "INCREMENTAL FROM" URLs.
	//
	// NB: We don't bother making assertions about this full backup since there
	// are no meaningful differences in how full and incremental backups report
	// status to the system.jobs table. Since the incremental BACKUP syntax is a
	// superset of the full BACKUP syntax, we'll cover everything by verifying the
	// incremental backup below.
	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, fullDir)
	sqlDB.Exec(t, `SET DATABASE = data`)

	sqlDB.Exec(t, `BACKUP TABLE bank TO $1 INCREMENTAL FROM $2`, incDir, fullDir)
	if err := jobutils.VerifySystemJob(t, sqlDB, 1, jobspb.TypeBackup, jobs.StatusSucceeded, jobs.Record{
		Username: security.RootUser,
		Description: fmt.Sprintf(
			`BACKUP TABLE bank TO '%s' INCREMENTAL FROM '%s'`,
			sanitizedIncDir+"redacted", sanitizedFullDir+"redacted",
		),
		DescriptorIDs: descpb.IDs{
			descpb.ID(backupDatabaseID),
			descpb.ID(backupTableID),
		},
	}); err != nil {
		t.Fatal(err)
	}

	sqlDB.Exec(t, `RESTORE TABLE bank FROM $1, $2 WITH OPTIONS (into_db='restoredb')`, fullDir, incDir)
	if err := jobutils.VerifySystemJob(t, sqlDB, 0, jobspb.TypeRestore, jobs.StatusSucceeded, jobs.Record{
		Username: security.RootUser,
		Description: fmt.Sprintf(
			`RESTORE TABLE bank FROM '%s', '%s' WITH into_db='restoredb'`,
			sanitizedFullDir+"redacted", sanitizedIncDir+"redacted",
		),
		DescriptorIDs: descpb.IDs{
			descpb.ID(restoreDatabaseID + 1),
		},
	}); err != nil {
		t.Fatal(err)
	}
}

func redactTestKMSURI(path string) (string, error) {
	var redactedQueryParams = map[string]struct{}{
		cloudimpl.AWSSecretParam: {},
	}

	uri, err := url.Parse(path)
	if err != nil {
		return "", err
	}
	params := uri.Query()
	for param := range params {
		if _, ok := redactedQueryParams[param]; ok {
			params.Set(param, "redacted")
		}
	}
	uri.Path = "/redacted"
	uri.RawQuery = params.Encode()
	return uri.String(), nil
}

// TestEncryptedBackupRestoreSystemJobs ensures that the system jobs entry for encrypted BACKUPs
// have the passphrase or the KMS URI sanitized.
func TestEncryptedBackupRestoreSystemJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	regionEnvVariable := "AWS_KMS_REGION_A"
	keyIDEnvVariable := "AWS_KMS_KEY_ARN_A"

	for _, tc := range []struct {
		name   string
		useKMS bool
	}{
		{
			"encrypted-with-kms",
			true,
		},
		{
			"encrypted-with-passphrase",
			false,
		},
	} {
		var encryptionOption string
		var sanitizedEncryptionOption string
		if tc.useKMS {
			correctKMSURI, _ := getAWSKMSURI(t, regionEnvVariable, keyIDEnvVariable)
			encryptionOption = fmt.Sprintf("kms='%s'", correctKMSURI)
			sanitizedURI, err := redactTestKMSURI(correctKMSURI)
			require.NoError(t, err)
			sanitizedEncryptionOption = fmt.Sprintf("kms='%s'", sanitizedURI)
		} else {
			encryptionOption = "encryption_passphrase='abcdefg'"
			sanitizedEncryptionOption = fmt.Sprintf("encryption_passphrase='redacted'")
		}

		t.Run(tc.name, func(t *testing.T) {
			_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, MultiNode, 3, InitNone)
			conn := sqlDB.DB.(*gosql.DB)
			defer cleanupFn()
			backupLoc1 := LocalFoo + "/x"

			sqlDB.Exec(t, `CREATE DATABASE restoredb`)
			backupDatabaseID := sqlutils.QueryDatabaseID(t, conn, "data")
			backupTableID := sqlutils.QueryTableID(t, conn, "data", "public", "bank")
			restoreDatabaseID := sqlutils.QueryDatabaseID(t, conn, "restoredb")

			// Take an encrypted BACKUP.
			sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE data TO $1 WITH %s`, encryptionOption),
				backupLoc1)

			// Verify the BACKUP job description is sanitized.
			if err := jobutils.VerifySystemJob(t, sqlDB, 0, jobspb.TypeBackup, jobs.StatusSucceeded,
				jobs.Record{
					Username: security.RootUser,
					Description: fmt.Sprintf(
						`BACKUP DATABASE data TO '%s' WITH %s`,
						backupLoc1, sanitizedEncryptionOption),
					DescriptorIDs: descpb.IDs{
						descpb.ID(backupDatabaseID),
						descpb.ID(backupTableID),
					},
				}); err != nil {
				t.Fatal(err)
			}

			// Perform an encrypted RESTORE.
			sqlDB.Exec(t, fmt.Sprintf(`RESTORE TABLE data.bank FROM $1 WITH OPTIONS (
into_db='restoredb', %s)`, encryptionOption), backupLoc1)

			// Verify the RESTORE job description is sanitized.
			if err := jobutils.VerifySystemJob(t, sqlDB, 0, jobspb.TypeRestore, jobs.StatusSucceeded, jobs.Record{
				Username: security.RootUser,
				Description: fmt.Sprintf(
					`RESTORE TABLE data.bank FROM '%s' WITH %s, into_db='restoredb'`,
					backupLoc1, sanitizedEncryptionOption,
				),
				DescriptorIDs: descpb.IDs{
					descpb.ID(restoreDatabaseID + 1),
				},
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type inProgressChecker func(context context.Context, ip inProgressState) error

// inProgressState holds state about an in-progress backup or restore
// for use in inProgressCheckers.
type inProgressState struct {
	*gosql.DB
	backupTableID uint32
	dir, name     string
}

func (ip inProgressState) latestJobID() (int64, error) {
	var id int64
	if err := ip.QueryRow(
		`SELECT job_id FROM crdb_internal.jobs ORDER BY created DESC LIMIT 1`,
	).Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

// checkInProgressBackupRestore will run a backup and restore, pausing each
// approximately halfway through to run either `checkBackup` or `checkRestore`.
func checkInProgressBackupRestore(
	t testing.TB, checkBackup inProgressChecker, checkRestore inProgressChecker,
) {
	// To test incremental progress updates, we install a store response filter,
	// which runs immediately before a KV command returns its response, in our
	// test cluster. Whenever we see an Export or Import response, we do a
	// blocking read on the allowResponse channel to give the test a chance to
	// assert the progress of the job.
	var allowResponse chan struct{}
	params := base.TestClusterArgs{}
	params.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingResponseFilter: func(ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse) *roachpb.Error {
			for _, ru := range br.Responses {
				switch ru.GetInner().(type) {
				case *roachpb.ExportResponse, *roachpb.ImportResponse:
					<-allowResponse
				}
			}
			return nil
		},
	}

	const numAccounts = 1000
	const totalExpectedResponses = backupRestoreDefaultRanges

	ctx, _, sqlDB, dir, cleanup := backupRestoreTestSetupWithParams(t, MultiNode, numAccounts, InitNone, params)
	conn := sqlDB.DB.(*gosql.DB)
	defer cleanup()

	sqlDB.Exec(t, `CREATE DATABASE restoredb`)

	backupTableID := sqlutils.QueryTableID(t, conn, "data", "public", "bank")

	do := func(query string, check inProgressChecker) {
		jobDone := make(chan error)
		allowResponse = make(chan struct{}, totalExpectedResponses)

		go func() {
			_, err := conn.Exec(query, LocalFoo)
			jobDone <- err
		}()

		// Allow half the total expected responses to proceed.
		for i := 0; i < totalExpectedResponses/2; i++ {
			allowResponse <- struct{}{}
		}

		err := retry.ForDuration(testutils.DefaultSucceedsSoonDuration, func() error {
			return check(ctx, inProgressState{
				DB:            conn,
				backupTableID: backupTableID,
				dir:           dir,
				name:          "foo",
			})
		})

		// Close the channel to allow all remaining responses to proceed. We do this
		// even if the above retry.ForDuration failed, otherwise the test will hang
		// forever.
		close(allowResponse)

		if err := <-jobDone; err != nil {
			t.Fatalf("%q: %+v", query, err)
		}

		if err != nil {
			t.Fatal(err)
		}
	}

	do(`BACKUP DATABASE data TO $1`, checkBackup)
	do(`RESTORE data.* FROM $1 WITH OPTIONS (into_db='restoredb')`, checkRestore)
}

func TestBackupRestoreSystemJobsProgress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer jobs.TestingSetProgressThresholds()()

	checkFraction := func(ctx context.Context, ip inProgressState) error {
		jobID, err := ip.latestJobID()
		if err != nil {
			return err
		}
		var fractionCompleted float32
		if err := ip.QueryRow(
			`SELECT fraction_completed FROM crdb_internal.jobs WHERE job_id = $1`,
			jobID,
		).Scan(&fractionCompleted); err != nil {
			return err
		}
		if fractionCompleted < 0.25 || fractionCompleted > 0.75 {
			return errors.Errorf(
				"expected progress to be in range [0.25, 0.75] but got %f",
				fractionCompleted,
			)
		}
		return nil
	}

	checkInProgressBackupRestore(t, checkFraction, checkFraction)
}

func TestBackupRestoreCheckpointing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.WithIssue(t, 33357)

	defer func(oldInterval time.Duration) {
		BackupCheckpointInterval = oldInterval
	}(BackupCheckpointInterval)
	BackupCheckpointInterval = 0

	var checkpointPath string

	checkBackup := func(ctx context.Context, ip inProgressState) error {
		checkpointPath = filepath.Join(ip.dir, ip.name, backupManifestCheckpointName)
		checkpointDescBytes, err := ioutil.ReadFile(checkpointPath)
		if err != nil {
			return errors.Errorf("%+v", err)
		}
		var checkpointDesc BackupManifest
		if err := protoutil.Unmarshal(checkpointDescBytes, &checkpointDesc); err != nil {
			return errors.Errorf("%+v", err)
		}
		if len(checkpointDesc.Files) == 0 {
			return errors.Errorf("empty backup checkpoint descriptor")
		}
		return nil
	}

	checkRestore := func(ctx context.Context, ip inProgressState) error {
		jobID, err := ip.latestJobID()
		if err != nil {
			return err
		}
		highWaterMark, err := getHighWaterMark(jobID, ip.DB)
		if err != nil {
			return err
		}
		low := keys.SystemSQLCodec.TablePrefix(ip.backupTableID)
		high := keys.SystemSQLCodec.TablePrefix(ip.backupTableID + 1)
		if bytes.Compare(highWaterMark, low) <= 0 || bytes.Compare(highWaterMark, high) >= 0 {
			return errors.Errorf("expected high-water mark %v to be between %v and %v",
				highWaterMark, low, high)
		}
		return nil
	}

	checkInProgressBackupRestore(t, checkBackup, checkRestore)

	if _, err := os.Stat(checkpointPath); err == nil {
		t.Fatalf("backup checkpoint descriptor at %s not cleaned up", checkpointPath)
	} else if !os.IsNotExist(err) {
		t.Fatal(err)
	}
}

func createAndWaitForJob(
	t *testing.T,
	db *sqlutils.SQLRunner,
	descriptorIDs []descpb.ID,
	details jobspb.Details,
	progress jobspb.ProgressDetails,
) {
	t.Helper()
	now := timeutil.ToUnixMicros(timeutil.Now())
	payload, err := protoutil.Marshal(&jobspb.Payload{
		Username:      security.RootUser,
		DescriptorIDs: descriptorIDs,
		StartedMicros: now,
		Details:       jobspb.WrapPayloadDetails(details),
		Lease:         &jobspb.Lease{NodeID: 1},
	})
	if err != nil {
		t.Fatal(err)
	}

	progressBytes, err := protoutil.Marshal(&jobspb.Progress{
		ModifiedMicros: now,
		Details:        jobspb.WrapProgressDetails(progress),
	})
	if err != nil {
		t.Fatal(err)
	}

	var jobID int64
	db.QueryRow(
		t, `INSERT INTO system.jobs (created, status, payload, progress) VALUES ($1, $2, $3, $4) RETURNING id`,
		timeutil.FromUnixMicros(now), jobs.StatusRunning, payload, progressBytes,
	).Scan(&jobID)
	jobutils.WaitForJob(t, db, jobID)
}

// TestBackupRestoreResume tests whether backup and restore jobs are properly
// resumed after a coordinator failure. It synthesizes a partially-complete
// backup job and a partially-complete restore job, both with expired leases, by
// writing checkpoints directly to system.jobs, then verifies they are resumed
// and successfully completed within a few seconds. The test additionally
// verifies that backup and restore do not re-perform work the checkpoint claims
// to have completed.
func TestBackupRestoreResume(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer jobs.TestingSetAdoptAndCancelIntervals(100*time.Millisecond, 100*time.Millisecond)()

	ctx := context.Background()

	const numAccounts = 1000
	_, tc, outerDB, dir, cleanupFn := BackupRestoreTestSetup(t, MultiNode, numAccounts, InitNone)
	defer cleanupFn()

	backupTableDesc := catalogkv.TestingGetTableDescriptor(tc.Servers[0].DB(), keys.SystemSQLCodec, "data", "bank")

	t.Run("backup", func(t *testing.T) {
		sqlDB := sqlutils.MakeSQLRunner(outerDB.DB)
		backupStartKey := backupTableDesc.PrimaryIndexSpan(keys.SystemSQLCodec).Key
		backupEndKey, err := rowenc.TestingMakePrimaryIndexKey(backupTableDesc, numAccounts/2)
		if err != nil {
			t.Fatal(err)
		}
		backupCompletedSpan := roachpb.Span{Key: backupStartKey, EndKey: backupEndKey}
		mockManifest, err := protoutil.Marshal(&BackupManifest{
			ClusterID: tc.Servers[0].ClusterID(),
			Files: []BackupManifest_File{
				{Path: "garbage-checkpoint", Span: backupCompletedSpan},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		backupDir := dir + "/backup"
		if err := os.MkdirAll(backupDir, 0755); err != nil {
			t.Fatal(err)
		}
		checkpointFile := backupDir + "/" + backupManifestCheckpointName
		if err := ioutil.WriteFile(checkpointFile, mockManifest, 0644); err != nil {
			t.Fatal(err)
		}
		createAndWaitForJob(
			t, sqlDB, []descpb.ID{backupTableDesc.ID},
			jobspb.BackupDetails{
				EndTime:        tc.Servers[0].Clock().Now(),
				URI:            "nodelocal://0/backup",
				BackupManifest: mockManifest,
			},
			jobspb.BackupProgress{},
		)

		// If the backup properly took the (incorrect) checkpoint into account, it
		// won't have tried to re-export any keys within backupCompletedSpan.
		backupManifestFile := backupDir + "/" + backupManifestName
		backupManifestBytes, err := ioutil.ReadFile(backupManifestFile)
		if err != nil {
			t.Fatal(err)
		}
		fileType := http.DetectContentType(backupManifestBytes)
		if fileType == ZipType {
			backupManifestBytes, err = decompressData(backupManifestBytes)
			require.NoError(t, err)
		}
		var backupManifest BackupManifest
		if err := protoutil.Unmarshal(backupManifestBytes, &backupManifest); err != nil {
			t.Fatal(err)
		}
		for _, file := range backupManifest.Files {
			if file.Span.Overlaps(backupCompletedSpan) && file.Path != "garbage-checkpoint" {
				t.Fatalf("backup re-exported checkpointed span %s", file.Span)
			}
		}
	})

	t.Run("restore", func(t *testing.T) {
		sqlDB := sqlutils.MakeSQLRunner(outerDB.DB)
		restoreDir := "nodelocal://0/restore"
		sqlDB.Exec(t, `BACKUP DATABASE DATA TO $1`, restoreDir)
		sqlDB.Exec(t, `CREATE DATABASE restoredb`)
		restoreDatabaseID := sqlutils.QueryDatabaseID(t, sqlDB.DB, "restoredb")
		restoreTableID, err := catalogkv.GenerateUniqueDescID(ctx, tc.Servers[0].DB(), keys.SystemSQLCodec)
		if err != nil {
			t.Fatal(err)
		}
		restoreHighWaterMark, err := rowenc.TestingMakePrimaryIndexKey(backupTableDesc, numAccounts/2)
		if err != nil {
			t.Fatal(err)
		}
		createAndWaitForJob(
			t, sqlDB, []descpb.ID{restoreTableID},
			jobspb.RestoreDetails{
				DescriptorRewrites: map[descpb.ID]*jobspb.RestoreDetails_DescriptorRewrite{
					backupTableDesc.ID: {
						ParentID: descpb.ID(restoreDatabaseID),
						ID:       restoreTableID,
					},
				},
				URIs: []string{restoreDir},
			},
			jobspb.RestoreProgress{
				HighWater: restoreHighWaterMark,
			},
		)
		// If the restore properly took the (incorrect) low-water mark into account,
		// the first half of the table will be missing.
		var restoredCount int64
		sqlDB.QueryRow(t, `SELECT count(*) FROM restoredb.bank`).Scan(&restoredCount)
		if e, a := int64(numAccounts)/2, restoredCount; e != a {
			t.Fatalf("expected %d restored rows, but got %d\n", e, a)
		}
		sqlDB.Exec(t, `DELETE FROM data.bank WHERE id < $1`, numAccounts/2)
		sqlDB.CheckQueryResults(t,
			`SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE restoredb.bank`,
			sqlDB.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE data.bank`),
		)
	})
}

func getHighWaterMark(jobID int64, sqlDB *gosql.DB) (roachpb.Key, error) {
	var progressBytes []byte
	if err := sqlDB.QueryRow(
		`SELECT progress FROM system.jobs WHERE id = $1`, jobID,
	).Scan(&progressBytes); err != nil {
		return nil, err
	}
	var payload jobspb.Progress
	if err := protoutil.Unmarshal(progressBytes, &payload); err != nil {
		return nil, err
	}
	switch d := payload.Details.(type) {
	case *jobspb.Progress_Restore:
		return d.Restore.HighWater, nil
	default:
		return nil, errors.Errorf("unexpected job details type %T", d)
	}
}

// TestBackupRestoreControlJob tests that PAUSE JOB, RESUME JOB, and CANCEL JOB
// work as intended on backup and restore jobs.
func TestBackupRestoreControlJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.WithIssue(t, 24136)

	// force every call to update
	defer jobs.TestingSetProgressThresholds()()
	defer jobs.TestingSetAdoptAndCancelIntervals(100*time.Millisecond, 100*time.Millisecond)()

	serverArgs := base.TestServerArgs{}
	// Disable external processing of mutations so that the final check of
	// crdb_internal.tables is guaranteed to not be cleaned up. Although this
	// was never observed by a stress test, it is here for safety.
	serverArgs.Knobs.SQLSchemaChanger = &sql.SchemaChangerTestingKnobs{
		// TODO (lucy): if/when this test gets reinstated, figure out what knobs are
		// needed.
	}

	// PAUSE JOB and CANCEL JOB are racy in that it's hard to guarantee that the
	// job is still running when executing a PAUSE or CANCEL--or that the job has
	// even started running. To synchronize, we install a store response filter
	// which does a blocking receive whenever it encounters an export or import
	// response. Below, when we want to guarantee the job is in progress, we do
	// exactly one blocking send. When this send completes, we know the job has
	// started, as we've seen one export or import response. We also know the job
	// has not finished, because we're blocking all future export and import
	// responses until we close the channel, and our backup or restore is large
	// enough that it will generate more than one export or import response.
	var allowResponse chan struct{}
	params := base.TestClusterArgs{ServerArgs: serverArgs}
	params.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingResponseFilter: jobutils.BulkOpResponseFilter(&allowResponse),
	}

	// We need lots of ranges to see what happens when they get chunked. Rather
	// than make a huge table, dial down the zone config for the bank table.
	init := func(tc *testcluster.TestCluster) {
		config.TestingSetupZoneConfigHook(tc.Stopper())
		v, err := tc.Servers[0].DB().Get(context.Background(), keys.SystemSQLCodec.DescIDSequenceKey())
		if err != nil {
			t.Fatal(err)
		}
		last := config.SystemTenantObjectID(v.ValueInt())
		zoneConfig := zonepb.DefaultZoneConfig()
		zoneConfig.RangeMaxBytes = proto.Int64(5000)
		config.TestingSetZoneConfig(last+1, zoneConfig)
	}
	const numAccounts = 1000
	_, _, outerDB, _, cleanup := backupRestoreTestSetupWithParams(t, MultiNode, numAccounts, init, params)
	defer cleanup()

	sqlDB := sqlutils.MakeSQLRunner(outerDB.DB)

	t.Run("foreign", func(t *testing.T) {
		foreignDir := "nodelocal://0/foreign"
		sqlDB.Exec(t, `CREATE DATABASE orig_fkdb`)
		sqlDB.Exec(t, `CREATE DATABASE restore_fkdb`)
		sqlDB.Exec(t, `CREATE TABLE orig_fkdb.fk (i INT REFERENCES data.bank)`)
		// Generate some FK data with splits so backup/restore block correctly.
		for i := 0; i < 10; i++ {
			sqlDB.Exec(t, `INSERT INTO orig_fkdb.fk (i) VALUES ($1)`, i)
			sqlDB.Exec(t, `ALTER TABLE orig_fkdb.fk SPLIT AT VALUES ($1)`, i)
		}

		for i, query := range []string{
			`BACKUP TABLE orig_fkdb.fk TO $1`,
			`RESTORE TABLE orig_fkdb.fk FROM $1 WITH OPTIONS (skip_missing_foreign_keys, into_db='restore_fkdb')`,
		} {
			jobID, err := jobutils.RunJob(t, sqlDB, &allowResponse, []string{"PAUSE"}, query, foreignDir)
			if !testutils.IsError(err, "job paused") {
				t.Fatalf("%d: expected 'job paused' error, but got %+v", i, err)
			}
			sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, jobID))
			jobutils.WaitForJob(t, sqlDB, jobID)
		}

		sqlDB.CheckQueryResults(t,
			`SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE orig_fkdb.fk`,
			sqlDB.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE restore_fkdb.fk`),
		)
	})

	t.Run("pause", func(t *testing.T) {
		pauseDir := "nodelocal://0/pause"
		noOfflineDir := "nodelocal://0/no-offline"
		sqlDB.Exec(t, `CREATE DATABASE pause`)

		for i, query := range []string{
			`BACKUP DATABASE data TO $1`,
			`RESTORE TABLE data.* FROM $1 WITH OPTIONS (into_db='pause')`,
		} {
			ops := []string{"PAUSE", "RESUME", "PAUSE"}
			jobID, err := jobutils.RunJob(t, sqlDB, &allowResponse, ops, query, pauseDir)
			if !testutils.IsError(err, "job paused") {
				t.Fatalf("%d: expected 'job paused' error, but got %+v", i, err)
			}
			if i > 0 {
				sqlDB.CheckQueryResults(t,
					`SELECT name FROM crdb_internal.tables WHERE database_name = 'pause' AND state = 'OFFLINE'`,
					[][]string{{"bank"}},
				)
				// Ensure that OFFLINE tables can be accessed to set zone configs.
				sqlDB.Exec(t, `ALTER TABLE pause.bank CONFIGURE ZONE USING constraints='[+dc=dc1]'`)
				// Ensure that OFFLINE tables are not included in a BACKUP.
				sqlDB.ExpectErr(t, `table "pause.public.bank" does not exist`, `BACKUP pause.bank TO $1`, noOfflineDir)
				sqlDB.Exec(t, `BACKUP pause.* TO $1`, noOfflineDir)
				sqlDB.CheckQueryResults(t, fmt.Sprintf("SHOW BACKUP '%s'", noOfflineDir), [][]string{})
			}
			sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, jobID))
			jobutils.WaitForJob(t, sqlDB, jobID)
		}
		sqlDB.CheckQueryResults(t,
			`SELECT count(*) FROM pause.bank`,
			sqlDB.QueryStr(t, `SELECT count(*) FROM data.bank`),
		)

		sqlDB.CheckQueryResults(t,
			`SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE pause.bank`,
			sqlDB.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE data.bank`),
		)
	})

	t.Run("pause-cancel", func(t *testing.T) {
		backupDir := "nodelocal://0/backup"

		backupJobID, err := jobutils.RunJob(t, sqlDB, &allowResponse, nil, "BACKUP DATABASE data TO $1", backupDir)
		if err != nil {
			t.Fatalf("error while running backup %+v", err)
		}
		jobutils.WaitForJob(t, sqlDB, backupJobID)

		sqlDB.Exec(t, `DROP DATABASE data`)

		query := `RESTORE DATABASE data FROM $1`
		ops := []string{"PAUSE"}
		jobID, err := jobutils.RunJob(t, sqlDB, &allowResponse, ops, query, backupDir)
		if !testutils.IsError(err, "job paused") {
			t.Fatalf("expected 'job paused' error, but got %+v", err)
		}

		// Create a table while the RESTORE is in progress on the database that was
		// created by the restore.
		sqlDB.Exec(t, `CREATE TABLE data.new_table (a int)`)

		// Do things while the job is paused.
		sqlDB.Exec(t, `CANCEL JOB $1`, jobID)

		// Ensure that the tables created by the user, during the RESTORE are
		// still present. Also ensure that the table that was being restored (bank)
		// is not.
		sqlDB.Exec(t, `USE data;`)
		sqlDB.CheckQueryResults(t, `SHOW TABLES;`, [][]string{{"public", "new_table", "table"}})
	})

	t.Run("cancel", func(t *testing.T) {
		cancelDir := "nodelocal://0/cancel"
		sqlDB.Exec(t, `CREATE DATABASE cancel`)

		for i, query := range []string{
			`BACKUP DATABASE data TO $1`,
			`RESTORE TABLE data.* FROM $1 WITH OPTIONS (into_db='cancel')`,
		} {
			if _, err := jobutils.RunJob(
				t, sqlDB, &allowResponse, []string{"cancel"}, query, cancelDir,
			); !testutils.IsError(err, "job canceled") {
				t.Fatalf("%d: expected 'job canceled' error, but got %+v", i, err)
			}
			// Check that executing the same backup or restore succeeds. This won't
			// work if the first backup or restore was not successfully canceled.
			sqlDB.Exec(t, query, cancelDir)
		}
		// Verify the canceled RESTORE added some DROP tables.
		sqlDB.CheckQueryResults(t,
			`SELECT name FROM crdb_internal.tables WHERE database_name = 'cancel' AND state = 'DROP'`,
			[][]string{{"bank"}},
		)
	})
}

func TestRestoreFailCleansUpTypeBackReferences(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	_, _, sqlDB, dir, cleanup := BackupRestoreTestSetup(t, singleNode, 0, InitNone)
	defer cleanup()

	dir = dir + "/foo"

	// Create a database with a type and table.
	sqlDB.Exec(t, `
CREATE DATABASE d;
CREATE TYPE d.ty AS ENUM ('hello');
CREATE TABLE d.tb (x d.ty);
INSERT INTO d.tb VALUES ('hello'), ('hello');
`)

	// Backup d.tb.
	sqlDB.Exec(t, `BACKUP TABLE d.tb TO $1`, LocalFoo)

	// Drop d.tb so that it can be restored.
	sqlDB.Exec(t, `DROP TABLE d.tb`)

	// Bugger the backup by removing the SST files.
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			t.Fatal(err)
		}
		if info.Name() == backupManifestName || !strings.HasSuffix(path, ".sst") {
			return nil
		}
		return os.Remove(path)
	}); err != nil {
		t.Fatal(err)
	}

	// We should get an error when restoring the table.
	sqlDB.ExpectErr(t, "sst: no such file", `RESTORE d.tb FROM $1`, LocalFoo)

	// The failed restore should clean up type back references so that we are able
	// to drop d.ty.
	sqlDB.Exec(t, `DROP TYPE d.ty`)
}

// TestRestoreFailCleanup tests that a failed RESTORE is cleaned up.
func TestRestoreFailCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params := base.TestServerArgs{}
	// Disable GC job so that the final check of crdb_internal.tables is
	// guaranteed to not be cleaned up. Although this was never observed by a
	// stress test, it is here for safety.
	blockGC := make(chan struct{})
	params.Knobs.GCJob = &sql.GCJobTestingKnobs{
		RunBeforeResume: func(_ int64) error {
			<-blockGC
			return nil
		},
	}

	const numAccounts = 1000
	_, tc, sqlDB, dir, cleanup := backupRestoreTestSetupWithParams(t, singleNode, numAccounts,
		InitNone, base.TestClusterArgs{ServerArgs: params})
	defer cleanup()
	kvDB := tc.Server(0).DB()

	dir = dir + "/foo"

	sqlDB.Exec(t, `CREATE DATABASE restore`)

	// Create a user defined type and check that it is cleaned up after the
	// failed restore.
	sqlDB.Exec(t, `CREATE TYPE data.myenum AS ENUM ('hello')`)
	// Do the same with a user defined schema.
	sqlDB.Exec(t, `USE data; CREATE SCHEMA myschema`)

	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, LocalFoo)
	// Bugger the backup by removing the SST files.
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			t.Fatal(err)
		}
		if info.Name() == backupManifestName || !strings.HasSuffix(path, ".sst") {
			return nil
		}
		return os.Remove(path)
	}); err != nil {
		t.Fatal(err)
	}
	sqlDB.ExpectErr(
		t, "sst: no such file",
		`RESTORE data.* FROM $1 WITH OPTIONS (into_db='restore')`, LocalFoo,
	)
	// Verify the failed RESTORE added some DROP tables.
	sqlDB.CheckQueryResults(t,
		`SELECT name FROM crdb_internal.tables WHERE database_name = 'restore' AND state = 'DROP'`,
		[][]string{{"bank"}},
	)

	// Verify that `myenum` was cleaned out from the failed restore. There should
	// only be one namespace entry (data.myenum).
	sqlDB.CheckQueryResults(t, `SELECT count(*) FROM system.namespace WHERE name = 'myenum'`, [][]string{{"1"}})
	// Check the same for data.myschema.
	sqlDB.CheckQueryResults(t, `SELECT count(*) FROM system.namespace WHERE name = 'myschema'`, [][]string{{"1"}})

	// Verify that the schema doesn't show up in the database's schema map.
	dbDesc := catalogkv.TestingGetDatabaseDescriptor(kvDB, keys.SystemSQLCodec, "restore")
	require.Empty(t, dbDesc.Schemas, "unexpected schema map entries %v", dbDesc.Schemas)
}

// TestRestoreFailDatabaseCleanup tests that a failed RESTORE is cleaned up
// when restoring an entire database.
func TestRestoreFailDatabaseCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params := base.TestServerArgs{}
	const numAccounts = 1000
	_, _, sqlDB, dir, cleanup := backupRestoreTestSetupWithParams(t, singleNode, numAccounts,
		InitNone, base.TestClusterArgs{ServerArgs: params})
	defer cleanup()

	dir = dir + "/foo"

	// Create a user defined type and check that it is cleaned up after the
	// failed restore.
	sqlDB.Exec(t, `CREATE TYPE data.myenum AS ENUM ('hello')`)
	// Do the same with a user defined schema.
	sqlDB.Exec(t, `USE data; CREATE SCHEMA myschema`)

	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, LocalFoo)
	// Bugger the backup by removing the SST files.
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			t.Fatal(err)
		}
		if info.Name() == backupManifestName || !strings.HasSuffix(path, ".sst") {
			return nil
		}
		return os.Remove(path)
	}); err != nil {
		t.Fatal(err)
	}
	sqlDB.Exec(t, `DROP DATABASE data`)
	sqlDB.ExpectErr(
		t, "sst: no such file",
		`RESTORE DATABASE data FROM $1`, LocalFoo,
	)
	sqlDB.ExpectErr(
		t, `database "data" does not exist`,
		`DROP DATABASE data`,
	)
}

func TestBackupRestoreUserDefinedSchemas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test takes a full backup and an incremental backup with revision
	// history at certain timestamps, then restores to each of the timestamps to
	// ensure that the types restored are correct.
	t.Run("revision-history", func(t *testing.T) {
		_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, 0, InitNone)
		defer cleanupFn()

		var ts1, ts2, ts3, ts4, ts5, ts6 string
		sqlDB.Exec(t, `
CREATE DATABASE d;
USE d;

CREATE SCHEMA sc;
CREATE SCHEMA sc2;
CREATE TABLE d.sc.t1 (x int);
CREATE TABLE d.sc2.t1 (x bool);
`)
		sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts1)

		sqlDB.Exec(t, `
ALTER SCHEMA sc RENAME TO sc3;
ALTER SCHEMA sc2 RENAME TO sc;
`)
		sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts2)

		sqlDB.Exec(t, `
DROP TABLE sc.t1;
DROP TABLE sc3.t1;
DROP SCHEMA sc;
DROP SCHEMA sc3;
`)
		sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts3)

		sqlDB.Exec(t, `
 CREATE SCHEMA sc;
 CREATE TABLE sc.t1 (a STRING);
`)
		sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts4)
		sqlDB.Exec(t, `BACKUP DATABASE d TO 'nodelocal://0/rev-history-backup' WITH revision_history`)

		sqlDB.Exec(t, `
DROP TABLE sc.t1;
DROP SCHEMA sc;
`)
		sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts5)

		sqlDB.Exec(t, `
CREATE SCHEMA sc;
CREATE TABLE sc.t1 (a FLOAT);
`)
		sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts6)
		sqlDB.Exec(t, `BACKUP DATABASE d TO 'nodelocal://0/rev-history-backup' WITH revision_history`)

		t.Run("ts1", func(t *testing.T) {
			sqlDB.Exec(t, "DROP DATABASE d;")
			sqlDB.Exec(t, "RESTORE DATABASE d FROM 'nodelocal://0/rev-history-backup' AS OF SYSTEM TIME "+ts1)
			sqlDB.Exec(t, "INSERT INTO d.sc.t1 VALUES (1)")
			sqlDB.Exec(t, "INSERT INTO d.sc2.t1 VALUES (true)")
			sqlDB.Exec(t, "USE d; CREATE SCHEMA unused;")
		})
		t.Run("ts2", func(t *testing.T) {
			sqlDB.Exec(t, "DROP DATABASE d;")
			sqlDB.Exec(t, "RESTORE DATABASE d FROM 'nodelocal://0/rev-history-backup' AS OF SYSTEM TIME "+ts2)
			sqlDB.Exec(t, "INSERT INTO d.sc3.t1 VALUES (1)")
			sqlDB.Exec(t, "INSERT INTO d.sc.t1 VALUES (true)")
		})
		t.Run("ts3", func(t *testing.T) {
			sqlDB.Exec(t, "DROP DATABASE d;")
			sqlDB.Exec(t, "RESTORE DATABASE d FROM 'nodelocal://0/rev-history-backup' AS OF SYSTEM TIME "+ts3)
			sqlDB.Exec(t, "USE d")
			sqlDB.Exec(t, "CREATE SCHEMA sc")
			sqlDB.Exec(t, "CREATE SCHEMA sc3;")
		})
		t.Run("ts4", func(t *testing.T) {
			sqlDB.Exec(t, "DROP DATABASE d;")
			sqlDB.Exec(t, "RESTORE DATABASE d FROM 'nodelocal://0/rev-history-backup' AS OF SYSTEM TIME "+ts4)
			sqlDB.Exec(t, "INSERT INTO d.sc.t1 VALUES ('hello')")
		})
		t.Run("ts5", func(t *testing.T) {
			sqlDB.Exec(t, "DROP DATABASE d;")
			sqlDB.Exec(t, "RESTORE DATABASE d FROM 'nodelocal://0/rev-history-backup' AS OF SYSTEM TIME "+ts5)
			sqlDB.Exec(t, "USE d")
			sqlDB.Exec(t, "CREATE SCHEMA sc")
		})
		t.Run("ts6", func(t *testing.T) {
			sqlDB.Exec(t, "DROP DATABASE d;")
			sqlDB.Exec(t, "RESTORE DATABASE d FROM 'nodelocal://0/rev-history-backup' AS OF SYSTEM TIME "+ts6)
			sqlDB.Exec(t, `INSERT INTO d.sc.t1 VALUES (123.123)`)
		})
	})

	// Tests full cluster backup/restore with user defined schemas.
	t.Run("full-cluster", func(t *testing.T) {
		_, _, sqlDB, dataDir, cleanupFn := BackupRestoreTestSetup(t, singleNode, 0, InitNone)
		defer cleanupFn()
		sqlDB.Exec(t, `
CREATE DATABASE d;
USE d;
CREATE SCHEMA unused;
CREATE SCHEMA sc;
CREATE TABLE sc.tb1 (x INT);
INSERT INTO sc.tb1 VALUES (1);
CREATE TYPE sc.typ1 AS ENUM ('hello');
CREATE TABLE sc.tb2 (x sc.typ1);
INSERT INTO sc.tb2 VALUES ('hello');
`)
		// Now backup the full cluster.
		sqlDB.Exec(t, `BACKUP TO 'nodelocal://0/test/'`)
		// Start a new server that shares the data directory.
		_, _, sqlDBRestore, cleanupRestore := backupRestoreTestSetupEmpty(t, singleNode, dataDir, InitNone)
		defer cleanupRestore()

		// Restore into the new cluster.
		sqlDBRestore.Exec(t, `RESTORE FROM 'nodelocal://0/test/'`)

		// Check that we can resolve all names through the user defined schema.
		sqlDBRestore.CheckQueryResults(t, `SELECT * FROM d.sc.tb1`, [][]string{{"1"}})
		sqlDBRestore.CheckQueryResults(t, `SELECT * FROM d.sc.tb2`, [][]string{{"hello"}})
		sqlDBRestore.CheckQueryResults(t, `SELECT 'hello'::d.sc.typ1`, [][]string{{"hello"}})

		// We shouldn't be able to create a new schema with the same name.
		sqlDBRestore.ExpectErr(t, `pq: schema "sc" already exists`, `USE d; CREATE SCHEMA sc`)
		sqlDBRestore.ExpectErr(t, `pq: schema "unused" already exists`, `USE d; CREATE SCHEMA unused`)
	})

	// Tests restoring databases with user defined schemas.
	t.Run("database", func(t *testing.T) {
		_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, 0, InitNone)
		defer cleanupFn()

		sqlDB.Exec(t, `
CREATE DATABASE d;
USE d;
CREATE SCHEMA sc;
CREATE SCHEMA unused;
CREATE TABLE sc.tb1 (x INT);
INSERT INTO sc.tb1 VALUES (1);
CREATE TYPE sc.typ1 AS ENUM ('hello');
CREATE TABLE sc.tb2 (x sc.typ1);
INSERT INTO sc.tb2 VALUES ('hello');
`)
		// Backup the database.
		sqlDB.Exec(t, `BACKUP DATABASE d TO 'nodelocal://0/test/'`)

		// Drop the database and restore into it.
		sqlDB.Exec(t, `DROP DATABASE d`)
		sqlDB.Exec(t, `RESTORE DATABASE d FROM 'nodelocal://0/test/'`)

		// Check that we can resolve all names through the user defined schema.
		sqlDB.CheckQueryResults(t, `SELECT * FROM d.sc.tb1`, [][]string{{"1"}})
		sqlDB.CheckQueryResults(t, `SELECT * FROM d.sc.tb2`, [][]string{{"hello"}})
		sqlDB.CheckQueryResults(t, `SELECT 'hello'::d.sc.typ1`, [][]string{{"hello"}})

		// We shouldn't be able to create a new schema with the same name.
		sqlDB.ExpectErr(t, `pq: schema "sc" already exists`, `USE d; CREATE SCHEMA sc`)
		sqlDB.ExpectErr(t, `pq: schema "unused" already exists`, `USE d; CREATE SCHEMA unused`)
	})

	// Test restoring tables with user defined schemas when restore schemas are
	// not being remapped.
	t.Run("no-remap", func(t *testing.T) {
		_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, 0, InitNone)
		defer cleanupFn()

		sqlDB.Exec(t, `
CREATE DATABASE d;
USE d;
CREATE SCHEMA sc;
CREATE TYPE sc.typ1 AS ENUM ('hello');
CREATE TABLE sc.tb1 (x sc.typ1);
INSERT INTO sc.tb1 VALUES ('hello');
CREATE TABLE sc.tb2 (x INT);
INSERT INTO sc.tb2 VALUES (1);
`)
		{
			// We have to qualify the table correctly to back it up. d.tb1 resolves
			// to d.public.tb1.
			sqlDB.ExpectErr(t, `pq: failed to resolve targets specified in the BACKUP stmt: table "d.tb1" does not exist`, `BACKUP TABLE d.tb1 TO 'nodelocal://0/test/'`)
			// Backup tb1.
			sqlDB.Exec(t, `BACKUP TABLE d.sc.tb1 TO 'nodelocal://0/test/'`)
			// Create a new database to restore into. This restore should restore the
			// schema sc into the new database.
			sqlDB.Exec(t, `CREATE DATABASE d2`)

			// We must properly qualify the table name when restoring as well.
			sqlDB.ExpectErr(t, `pq: failed to resolve targets in the BACKUP location specified by the RESTORE stmt, use SHOW BACKUP to find correct targets: table "d.tb1" does not exist`, `RESTORE TABLE d.tb1 FROM 'nodelocal://0/test/' WITH into_db = 'd2'`)

			sqlDB.Exec(t, `RESTORE TABLE d.sc.tb1 FROM 'nodelocal://0/test/' WITH into_db = 'd2'`)

			// Check that we can resolve all names through the user defined schema.
			sqlDB.CheckQueryResults(t, `SELECT * FROM d2.sc.tb1`, [][]string{{"hello"}})
			sqlDB.CheckQueryResults(t, `SELECT 'hello'::d2.sc.typ1`, [][]string{{"hello"}})

			// We shouldn't be able to create a new schema with the same name.
			sqlDB.ExpectErr(t, `pq: schema "sc" already exists`, `USE d2; CREATE SCHEMA sc`)
		}

		{
			// Test that we can * expand schema prefixed names. Create a new backup
			// with all the tables in d.sc.
			sqlDB.Exec(t, `BACKUP TABLE d.sc.* TO 'nodelocal://0/test2/'`)
			// Create a new database to restore into.
			sqlDB.Exec(t, `CREATE DATABASE d3`)
			sqlDB.Exec(t, `RESTORE TABLE d.sc.* FROM 'nodelocal://0/test2/' WITH into_db = 'd3'`)

			// Check that we can resolve all names through the user defined schema.
			sqlDB.CheckQueryResults(t, `SELECT * FROM d3.sc.tb1`, [][]string{{"hello"}})
			sqlDB.CheckQueryResults(t, `SELECT * FROM d3.sc.tb2`, [][]string{{"1"}})
			sqlDB.CheckQueryResults(t, `SELECT 'hello'::d3.sc.typ1`, [][]string{{"hello"}})

			// We shouldn't be able to create a new schema with the same name.
			sqlDB.ExpectErr(t, `pq: schema "sc" already exists`, `USE d3; CREATE SCHEMA sc`)
		}
	})

	// Test restoring tables with user defined schemas when restore schemas are
	// not being remapped. Like no-remap but with more databases and schemas.
	t.Run("multi-schemas", func(t *testing.T) {
		_, tc, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, 0, InitNone)
		defer cleanupFn()
		kvDB := tc.Server(0).DB()

		sqlDB.Exec(t, `
CREATE DATABASE d1;
USE d1;
CREATE SCHEMA sc1;
CREATE TABLE sc1.tb (x INT);
INSERT INTO sc1.tb VALUES (1);
CREATE SCHEMA sc2;
CREATE TABLE sc2.tb (x INT);
INSERT INTO sc2.tb VALUES (2);

CREATE DATABASE d2;
USE d2;
CREATE SCHEMA sc3;
CREATE TABLE sc3.tb (x INT);
INSERT INTO sc3.tb VALUES (3);
CREATE SCHEMA sc4;
CREATE TABLE sc4.tb (x INT);
INSERT INTO sc4.tb VALUES (4);
`)
		{
			// Backup all databases.
			sqlDB.Exec(t, `BACKUP DATABASE d1, d2 TO 'nodelocal://0/test/'`)
			// Create a new database to restore into. This restore should restore the
			// schemas into the new database.
			sqlDB.Exec(t, `CREATE DATABASE newdb`)
			// Create a schema and table in the database to restore into, unrelated to
			// the restore.
			sqlDB.Exec(t, `USE newdb`)
			sqlDB.Exec(t, `CREATE SCHEMA existingschema`)
			sqlDB.Exec(t, `CREATE TABLE existingschema.tb (x INT)`)
			sqlDB.Exec(t, `INSERT INTO existingschema.tb VALUES (0)`)

			sqlDB.Exec(t, `RESTORE TABLE d1.sc1.*, d1.sc2.*, d2.sc3.*, d2.sc4.* FROM 'nodelocal://0/test/' WITH into_db = 'newdb'`)

			// Check that we can resolve all names through the user defined schemas.
			sqlDB.CheckQueryResults(t, `SELECT * FROM newdb.sc1.tb`, [][]string{{"1"}})
			sqlDB.CheckQueryResults(t, `SELECT * FROM newdb.sc2.tb`, [][]string{{"2"}})
			sqlDB.CheckQueryResults(t, `SELECT * FROM newdb.sc3.tb`, [][]string{{"3"}})
			sqlDB.CheckQueryResults(t, `SELECT * FROM newdb.sc4.tb`, [][]string{{"4"}})

			// Check that name resolution still works for the preexisting schema.
			sqlDB.CheckQueryResults(t, `SELECT * FROM newdb.existingschema.tb`, [][]string{{"0"}})
		}

		// Verify that the schemas are in the database's schema map.
		dbDesc := catalogkv.TestingGetDatabaseDescriptor(kvDB, keys.SystemSQLCodec, "newdb")
		require.Contains(t, dbDesc.Schemas, "sc1")
		require.Contains(t, dbDesc.Schemas, "sc2")
		require.Contains(t, dbDesc.Schemas, "sc3")
		require.Contains(t, dbDesc.Schemas, "sc4")
		require.Contains(t, dbDesc.Schemas, "existingschema")
		require.Len(t, dbDesc.Schemas, 5)
	})
	// Test when we remap schemas to existing schemas in the cluster.
	t.Run("remap", func(t *testing.T) {
		_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, 0, InitNone)
		defer cleanupFn()

		sqlDB.Exec(t, `
CREATE DATABASE d;
USE d;
CREATE SCHEMA sc;
CREATE TYPE sc.typ1 AS ENUM ('hello');
CREATE TABLE sc.tb1 (x sc.typ1);
INSERT INTO sc.tb1 VALUES ('hello');
`)
		// Take a backup.
		sqlDB.Exec(t, `BACKUP TABLE d.sc.tb1 TO 'nodelocal://0/test/'`)
		// Now drop the table.
		sqlDB.Exec(t, `DROP TABLE d.sc.tb1`)
		// Restoring the table should restore into d.sc.
		sqlDB.Exec(t, `RESTORE TABLE d.sc.tb1 FROM 'nodelocal://0/test/'`)
		sqlDB.CheckQueryResults(t, `SELECT * FROM d.sc.tb1`, [][]string{{"hello"}})
	})
}

func TestBackupRestoreUserDefinedTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test takes a full backup and an incremental backup with revision
	// history at certain timestamps, then restores to each of the timestamps to
	// ensure that the types restored are correct.
	//
	// ts1: farewell type exists as (bye, cya)
	// ts2: no farewell type exists
	// ts3: farewell type exists as (another)
	// ts4: no farewell type exists
	// ts5: farewell type exists as (third)
	t.Run("revision-history", func(t *testing.T) {
		_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, 0, InitNone)
		defer cleanupFn()

		var ts1, ts2, ts3, ts4, ts5 string
		// Create some types, databases, and tables that use them.
		sqlDB.Exec(t, `
CREATE DATABASE d;
CREATE TYPE d.unused AS ENUM ('lonely');
CREATE TYPE d.farewell AS ENUM ('bye', 'cya');
CREATE TABLE d.t1 (x d.farewell);
INSERT INTO d.t1 VALUES ('bye'), ('cya');
`)
		sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts1)

		sqlDB.Exec(t, `
DROP TABLE d.t1;
DROP TYPE d.farewell;
`)
		sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts2)

		sqlDB.Exec(t, `
CREATE TYPE d.farewell AS ENUM ('another');
CREATE TABLE d.t1 (x d.farewell);
`)
		sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts3)
		// Make a full backup that includes ts1, ts2 and ts3.
		sqlDB.Exec(t, `BACKUP DATABASE d TO 'nodelocal://0/rev-history-backup' WITH revision_history`)

		sqlDB.Exec(t, `
DROP TABLE d.t1;
DROP TYPE d.farewell;
`)
		sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts4)

		sqlDB.Exec(t, `
CREATE TYPE d.farewell AS ENUM ('third');
CREATE TABLE d.t1 (x d.farewell);
`)
		sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts5)
		// Make an incremental backup that includes ts4 and ts5.
		sqlDB.Exec(t, `BACKUP DATABASE d TO 'nodelocal://0/rev-history-backup' WITH revision_history`)

		t.Run("ts1", func(t *testing.T) {
			// Expect the farewell type ('bye', 'cya') to be restored - from the full
			// backup.
			sqlDB.Exec(t, `DROP DATABASE d;`)
			sqlDB.Exec(t,
				fmt.Sprintf(`
RESTORE DATABASE d FROM 'nodelocal://0/rev-history-backup'
  AS OF SYSTEM TIME %s
`, ts1))
			sqlDB.ExpectErr(t, `pq: type "d.public.farewell" already exists`,
				`CREATE TYPE d.farewell AS ENUM ('bye', 'cya')`)
			sqlDB.ExpectErr(t, `pq: type "d.public.unused" already exists`,
				`CREATE TYPE d.unused AS ENUM ('some_enum')`)
			sqlDB.Exec(t, `SELECT 'bye'::d.farewell; SELECT 'cya'::d.public.farewell;`)
			sqlDB.ExpectErr(t, `pq: invalid input value for enum farewell`,
				`SELECT 'another'::d.farewell;`)
			sqlDB.ExpectErr(t, `pq: invalid input value for enum farewell`,
				`SELECT 'third'::d.farewell;`)
		})

		t.Run("ts2", func(t *testing.T) {
			// Expect no farewell type be restored - from the full backup.
			sqlDB.Exec(t, `DROP DATABASE d;`)
			sqlDB.Exec(t,
				fmt.Sprintf(`
		RESTORE DATABASE d FROM 'nodelocal://0/rev-history-backup'
		 AS OF SYSTEM TIME %s
		`, ts2))
			sqlDB.Exec(t, `CREATE TYPE d.farewell AS ENUM ('bye', 'cya')`)
		})

		t.Run("ts3", func(t *testing.T) {
			// Expect the farewell type ('another') to be restored - from the full
			// backup.
			sqlDB.Exec(t, `DROP DATABASE d;`)
			sqlDB.Exec(t,
				fmt.Sprintf(`
		RESTORE DATABASE d FROM 'nodelocal://0/rev-history-backup'
		 AS OF SYSTEM TIME %s
		`, ts3))
			sqlDB.ExpectErr(t, `pq: type "d.public.farewell" already exists`,
				`CREATE TYPE d.farewell AS ENUM ('bye', 'cya')`)
			sqlDB.ExpectErr(t, `pq: invalid input value for enum farewell`,
				`SELECT 'bye'::d.farewell;`)
			sqlDB.ExpectErr(t, `pq: invalid input value for enum farewell`,
				`SELECT 'cya'::d.farewell;`)
			sqlDB.ExpectErr(t, `pq: invalid input value for enum farewell`,
				`SELECT 'third'::d.farewell;`)
			sqlDB.Exec(t, `SELECT 'another'::d.farewell`)
		})

		t.Run("ts4", func(t *testing.T) {
			// Expect no farewell type to be restored - from the incremental backup.
			sqlDB.Exec(t, `DROP DATABASE d;`)
			sqlDB.Exec(t,
				fmt.Sprintf(`
		RESTORE DATABASE d FROM 'nodelocal://0/rev-history-backup'
		 AS OF SYSTEM TIME %s
		`, ts4))
			sqlDB.Exec(t, `CREATE TYPE d.farewell AS ENUM ('bye', 'cya')`)
		})

		t.Run("ts5", func(t *testing.T) {
			// Expect the farewell type ('third') to be restored - from the
			// incremental backup.
			sqlDB.Exec(t, `DROP DATABASE d;`)
			sqlDB.Exec(t,
				fmt.Sprintf(`
		RESTORE DATABASE d FROM 'nodelocal://0/rev-history-backup'
		 AS OF SYSTEM TIME %s
		`, ts5))
			sqlDB.ExpectErr(t, `pq: type "d.public.farewell" already exists`,
				`CREATE TYPE d.farewell AS ENUM ('bye', 'cya')`)
			sqlDB.ExpectErr(t, `pq: invalid input value for enum farewell`,
				`SELECT 'bye'::d.farewell;`)
			sqlDB.ExpectErr(t, `pq: invalid input value for enum farewell`,
				`SELECT 'cya'::d.farewell;`)
			sqlDB.ExpectErr(t, `pq: invalid input value for enum farewell`,
				`SELECT 'another'::d.farewell;`)
			sqlDB.Exec(t, `SELECT 'third'::d.farewell`)
		})
	})

	// Test backup/restore of a single table.
	t.Run("table", func(t *testing.T) {
		_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, 0, InitNone)
		defer cleanupFn()
		sqlDB.Exec(t, `
CREATE DATABASE d;
CREATE TYPE d.greeting AS ENUM ('hello', 'howdy', 'hi');
CREATE TABLE d.t (x d.greeting);
INSERT INTO d.t VALUES ('hello'), ('howdy');
CREATE TABLE d.t2 (x d.greeting[]);
INSERT INTO d.t2 VALUES (ARRAY['hello']);
CREATE TABLE d.t3 (x d.greeting);
INSERT INTO d.t3 VALUES ('hi');
`)
		// Test backups of t.
		{
			// Now backup t.
			sqlDB.Exec(t, `BACKUP TABLE d.t TO 'nodelocal://0/test/'`)
			// Create a new database to restore the table into.
			sqlDB.Exec(t, `CREATE DATABASE d2`)
			// Restore t into d2.
			sqlDB.Exec(t, `RESTORE TABLE d.t FROM 'nodelocal://0/test/' WITH into_db = 'd2'`)
			// Ensure that greeting type has been restored into d2 as well.
			sqlDB.Exec(t, `CREATE TABLE d2.t2 (x d2.greeting, y d2._greeting)`)
			// Check that the table data is as expected.
			sqlDB.CheckQueryResults(t, `SELECT * FROM d2.t ORDER BY x`, [][]string{{"hello"}, {"howdy"}})
		}

		// Test backing up t2. It only references the implicit array type, so we're
		// checking that the base type gets included as well.
		{
			// Now backup t2.
			sqlDB.Exec(t, `BACKUP TABLE d.t2 TO 'nodelocal://0/test2/'`)
			// Create a new database to restore the table into.
			sqlDB.Exec(t, `CREATE DATABASE d3`)
			// Restore t2 into d3.
			sqlDB.Exec(t, `RESTORE TABLE d.t2 FROM 'nodelocal://0/test2/' WITH into_db = 'd3'`)
			// Ensure that the base type and array type have been restored into d3.
			sqlDB.Exec(t, `CREATE TABLE d3.t (x d3.greeting, y d3._greeting)`)
			// Check that the table data is as expected.
			sqlDB.CheckQueryResults(t, `SELECT * FROM d3.t2`, [][]string{{"{hello}"}})
		}

		// Create a backup of all the tables in d.
		{
			// Backup all of the tables.
			sqlDB.Exec(t, `BACKUP d.* TO 'nodelocal://0/test3/'`)
			// Create a new database to restore all of the tables into.
			sqlDB.Exec(t, `CREATE DATABASE d4`)
			// Restore all of the tables.
			sqlDB.Exec(t, `RESTORE TABLE d.* FROM 'nodelocal://0/test3/' WITH into_db = 'd4'`)
			// Check that all of the tables have expected data.
			sqlDB.CheckQueryResults(t, `SELECT * FROM d4.t ORDER BY x`, [][]string{{"hello"}, {"howdy"}})
			sqlDB.CheckQueryResults(t, `SELECT * FROM d4.t2 ORDER BY x`, [][]string{{"{hello}"}})
			sqlDB.CheckQueryResults(t, `SELECT * FROM d4.t3 ORDER BY x`, [][]string{{"hi"}})
			// Ensure that the types have been restored as well.
			sqlDB.Exec(t, `CREATE TABLE d4.t4 (x d4.greeting, y d4._greeting)`)
		}
	})

	// Test cases where we attempt to remap types in the backup to types that
	// already exist in the cluster.
	t.Run("backup-remap", func(t *testing.T) {
		_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, 0, InitNone)
		defer cleanupFn()
		sqlDB.Exec(t, `
CREATE DATABASE d;
CREATE TYPE d.greeting AS ENUM ('hello', 'howdy', 'hi');
CREATE TABLE d.t (x d.greeting);
INSERT INTO d.t VALUES ('hello'), ('howdy');
CREATE TYPE d.farewell AS ENUM ('bye', 'cya');
CREATE TABLE d.t2 (x d.greeting[]);
INSERT INTO d.t2 VALUES (ARRAY['hello']);
`)
		{
			// Backup and restore t.
			sqlDB.Exec(t, `BACKUP TABLE d.t TO 'nodelocal://0/test/'`)
			sqlDB.Exec(t, `DROP TABLE d.t`)
			sqlDB.Exec(t, `RESTORE TABLE d.t FROM 'nodelocal://0/test/'`)

			// Check that the table data is restored correctly and the types aren't touched.
			sqlDB.CheckQueryResults(t, `SELECT 'hello'::d.greeting, ARRAY['hello']::d.greeting[]`, [][]string{{"hello", "{hello}"}})
			sqlDB.CheckQueryResults(t, `SELECT * FROM d.t ORDER BY x`, [][]string{{"hello"}, {"howdy"}})

			// d.t should be added as a back reference to greeting.
			sqlDB.ExpectErr(t, `pq: cannot drop type "greeting" because other objects \(\[d.public.t2 d.public.t\]\) still depend on it`, `DROP TYPE d.greeting`)
		}

		{
			// Test that backing up an restoring a table with just the array type
			// will remap types appropriately.
			sqlDB.Exec(t, `BACKUP TABLE d.t2 TO 'nodelocal://0/test2/'`)
			sqlDB.Exec(t, `DROP TABLE d.t2`)
			sqlDB.Exec(t, `RESTORE TABLE d.t2 FROM 'nodelocal://0/test2/'`)
			sqlDB.CheckQueryResults(t, `SELECT 'hello'::d.greeting, ARRAY['hello']::d.greeting[]`, [][]string{{"hello", "{hello}"}})
			sqlDB.CheckQueryResults(t, `SELECT * FROM d.t2 ORDER BY x`, [][]string{{"{hello}"}})
		}

		{
			// Create another database with compatible types.
			sqlDB.Exec(t, `CREATE DATABASE d2`)
			sqlDB.Exec(t, `CREATE TYPE d2.greeting AS ENUM ('hello', 'howdy', 'hi')`)

			// Now restore t into this database. It should remap d.greeting to d2.greeting.
			sqlDB.Exec(t, `RESTORE TABLE d.t FROM 'nodelocal://0/test/' WITH into_db = 'd2'`)
			sqlDB.CheckQueryResults(t, `SELECT * FROM d2.t ORDER BY x`, [][]string{{"hello"}, {"howdy"}})
			sqlDB.Exec(t, `INSERT INTO d2.t VALUES ('hi'::d2.greeting)`)

			// Restore t2 as well.
			sqlDB.Exec(t, `RESTORE TABLE d.t2 FROM 'nodelocal://0/test2/' WITH into_db = 'd2'`)
			sqlDB.CheckQueryResults(t, `SELECT * FROM d2.t2 ORDER BY x`, [][]string{{"{hello}"}})
			sqlDB.Exec(t, `INSERT INTO d2.t2 VALUES (ARRAY['hi'::d2.greeting])`)

			// d2.t and d2.t2 should both have back references to d2.greeting.
			sqlDB.ExpectErr(t, `pq: cannot drop type "greeting" because other objects \(\[d2.public.t d2.public.t2\]\) still depend on it`, `DROP TYPE d2.greeting`)
		}

		{
			// Test when type remapping isn't possible. Create a type that isn't
			// compatible with d.greeting.
			sqlDB.Exec(t, `CREATE DATABASE d3`)
			sqlDB.Exec(t, `CREATE TYPE d3.greeting AS ENUM ('hello', 'howdy')`)

			// Now restore t into this database. We'll attempt to remap d.greeting to
			// d3.greeting and fail because they aren't compatible.
			sqlDB.ExpectErr(t, `could not find enum value "hi"`, `RESTORE TABLE d.t FROM 'nodelocal://0/test/' WITH into_db = 'd3'`)

			// Test the same case, but with differing internal representations.
			sqlDB.Exec(t, `CREATE DATABASE d4`)
			sqlDB.Exec(t, `CREATE TYPE d4.greeting AS ENUM ('hello', 'howdy', 'hi', 'greetings')`)
			sqlDB.ExpectErr(t, `has differing physical representation`, `RESTORE TABLE d.t FROM 'nodelocal://0/test/' WITH into_db = 'd4'`)
		}

		{
			// Test a case where after restoring, the array type name originally
			// backed up will be different after being remapped to an existing type.
			sqlDB.Exec(t, `CREATE DATABASE d5`)
			// Creates type _typ1 and array type __typ1.
			sqlDB.Exec(t, `CREATE TYPE d5._typ1 AS ENUM ('v1', 'v2')`)
			// Creates type typ1 and array type ___typ1.
			sqlDB.Exec(t, `CREATE TYPE d5.typ1 AS ENUM ('v1', 'v2')`)
			// Create a table using these ___typ1.
			sqlDB.Exec(t, `CREATE TABLE d5.tb1 (x d5.typ1[])`)
			// Backup tb1.
			sqlDB.Exec(t, `BACKUP TABLE d5.tb1 TO 'nodelocal://0/test3/'`)

			// Create another database with a compatible type.
			sqlDB.Exec(t, `CREATE DATABASE d6`)
			sqlDB.Exec(t, `CREATE TYPE d6.typ1 AS ENUM ('v1', 'v2')`)
			// Restoring tb1 into d6 will remap d5.typ1 to d6.typ1 and d5.___typ1
			// to d6._typ1.
			sqlDB.Exec(t, `RESTORE TABLE d5.tb1 FROM 'nodelocal://0/test3/' WITH into_db = 'd6'`)
			sqlDB.Exec(t, `INSERT INTO d6.tb1 VALUES (ARRAY['v1']::d6._typ1)`)
		}

		{
			// Test a case where we restore to an existing enum that is compatible with,
			// but not the same as greeting.
			sqlDB.Exec(t, `CREATE DATABASE d7`)
			sqlDB.Exec(t, `CREATE TYPE d7.greeting AS ENUM ('hello', 'howdy', 'hi')`)
			// Now add a value to greeting -- this will keep the internal representations
			// of the existing enum members the same.
			sqlDB.Exec(t, `ALTER TYPE d7.greeting ADD VALUE 'greetings' BEFORE 'howdy'`)

			// We should be able to restore d.greeting using d7.greeting.
			sqlDB.Exec(t, `RESTORE TABLE d.t FROM 'nodelocal://0/test/' WITH into_db = 'd7'`)
			sqlDB.Exec(t, `INSERT INTO d7.t VALUES ('greetings')`)
			sqlDB.CheckQueryResults(t, `SELECT * FROM d7.t ORDER BY x`, [][]string{{"hello"}, {"greetings"}, {"howdy"}})
			// d7.t should have a back reference from d7.greeting.
			sqlDB.ExpectErr(t, `pq: cannot drop type "greeting" because other objects \(\[d7.public.t\]\) still depend on it`, `DROP TYPE d7.greeting`)
		}
	})

	t.Run("incremental", func(t *testing.T) {
		_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, 0, InitNone)
		defer cleanupFn()
		sqlDB.Exec(t, `
CREATE DATABASE d;
CREATE TYPE d.greeting AS ENUM ('hello', 'howdy', 'hi');
CREATE TABLE d.t (x d.greeting);
INSERT INTO d.t VALUES ('hello'), ('howdy');
`)
		{
			// Start out with backing up t.
			sqlDB.Exec(t, `BACKUP TABLE d.t TO 'nodelocal://0/test/'`)
			// Alter d.greeting.
			sqlDB.Exec(t, `ALTER TYPE d.greeting RENAME TO newname`)
			// Now backup on top of the original back containing d.greeting.
			sqlDB.Exec(t, `BACKUP TABLE d.t TO 'nodelocal://0/test/'`)
			// We should be able to restore this backup, and see that the type's
			// name change is present.
			sqlDB.Exec(t, `CREATE DATABASE d2`)
			sqlDB.Exec(t, `RESTORE TABLE d.t FROM 'nodelocal://0/test/' WITH into_db = 'd2'`)
			sqlDB.CheckQueryResults(t, `SELECT 'hello'::d2.newname`, [][]string{{"hello"}})
		}

		{
			// Create a database with a type, and take a backup.
			sqlDB.Exec(t, `CREATE DATABASE d3`)
			sqlDB.Exec(t, `BACKUP DATABASE d3 TO 'nodelocal://0/test2/'`)

			// Now create a new type in that database.
			sqlDB.Exec(t, `CREATE TYPE d3.farewell AS ENUM ('bye', 'cya')`)

			// Perform an incremental backup, which should pick up the new type.
			sqlDB.Exec(t, `BACKUP DATABASE d3 TO 'nodelocal://0/test2/'`)

			// Until #51362 lands we have to manually clean up this type before the
			// DROP DATABASE statement otherwise we'll leave behind an orphaned desc.
			sqlDB.Exec(t, `DROP TYPE d3.farewell`)

			// Now restore it.
			sqlDB.Exec(t, `DROP DATABASE d3`)
			sqlDB.Exec(t, `RESTORE DATABASE d3 FROM 'nodelocal://0/test2/'`)
			// Check that we are able to use the type.
			sqlDB.Exec(t, `CREATE TABLE d3.t (x d3.farewell)`)
			sqlDB.Exec(t, `DROP TABLE d3.t`)

			// If we drop the type and back up again, it should be gone.
			sqlDB.Exec(t, `DROP TYPE d3.farewell`)

			sqlDB.Exec(t, `BACKUP DATABASE d3 TO 'nodelocal://0/test2/'`)
			sqlDB.Exec(t, `DROP DATABASE d3`)
			sqlDB.Exec(t, `RESTORE DATABASE d3 FROM 'nodelocal://0/test2/'`)
			sqlDB.ExpectErr(t, `pq: type "d3.farewell" does not exist`, `CREATE TABLE d3.t (x d3.farewell)`)
		}
	})
}

func TestBackupRestoreDuringUserDefinedTypeChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Protects typeChangeStarted.
	var mu syncutil.Mutex
	typeChangeStarted := make(chan struct{})
	waitForBackup := make(chan struct{})
	typeChangeFinished := make(chan struct{})
	_, _, sqlDB, _, cleanupFn := backupRestoreTestSetupWithParams(t, singleNode, 0, InitNone, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLTypeSchemaChanger: &sql.TypeSchemaChangerTestingKnobs{
					RunBeforeEnumMemberPromotion: func() {
						mu.Lock()
						defer mu.Unlock()
						if typeChangeStarted != nil {
							close(typeChangeStarted)
							<-waitForBackup
							typeChangeStarted = nil
						}
					},
				},
			},
		},
	})
	defer cleanupFn()

	// Create a database with a type.
	sqlDB.Exec(t, `
CREATE DATABASE d;
CREATE TYPE d.greeting AS ENUM ('hello', 'howdy', 'hi');
`)

	// Start an ALTER TYPE statement that will block.
	go func() {
		// Note we don't use sqlDB.Exec here because we can't Fatal from within a goroutine.
		if _, err := sqlDB.DB.ExecContext(context.Background(), `ALTER TYPE d.greeting ADD VALUE 'cheers'`); err != nil {
			t.Error(err)
		}
		close(typeChangeFinished)
	}()

	// Wait on the type change to start.
	<-typeChangeStarted

	// Now create a backup while the type change job is blocked so that greeting
	// is backed up with 'cheers' in the READ_ONLY state.
	sqlDB.Exec(t, `BACKUP DATABASE d TO 'nodelocal://0/test/'`)

	// Let the type change finish.
	close(waitForBackup)
	<-typeChangeFinished

	// Now drop the database and restore.
	sqlDB.Exec(t, `DROP DATABASE d`)
	sqlDB.Exec(t, `RESTORE DATABASE d FROM 'nodelocal://0/test/'`)

	// The type change job should be scheduled and succeed. Note that we can't use
	// sqlDB.CheckQueryResultsRetry as it Fatal's upon an error. The case below
	// will error until the job completes.
	testutils.SucceedsSoon(t, func() error {
		_, err := sqlDB.DB.ExecContext(context.Background(), `SELECT 'cheers'::d.greeting`)
		return err
	})
	sqlDB.CheckQueryResults(t, `SELECT 'cheers'::d.greeting`, [][]string{{"cheers"}})
}

func TestBackupRestoreInterleaved(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const numAccounts = 20

	_, _, sqlDB, dir, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()
	args := base.TestServerArgs{ExternalIODir: dir}

	totalRows, _ := generateInterleavedData(sqlDB, t, numAccounts)

	var unused string
	var exportedRows int
	sqlDB.QueryRow(t, `BACKUP DATABASE data TO $1`, LocalFoo).Scan(
		&unused, &unused, &unused, &exportedRows, &unused, &unused,
	)
	if exportedRows != totalRows {
		// TODO(dt): fix row-count including interleaved garbarge
		t.Logf("expected %d rows in BACKUP, got %d", totalRows, exportedRows)
	}

	t.Run("all tables in interleave hierarchy", func(t *testing.T) {
		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tcRestore.Stopper().Stop(context.Background())
		sqlDBRestore := sqlutils.MakeSQLRunner(tcRestore.Conns[0])
		// Create a dummy database to verify rekeying is correctly performed.
		sqlDBRestore.Exec(t, `CREATE DATABASE ignored`)
		sqlDBRestore.Exec(t, `CREATE DATABASE data`)

		var importedRows int
		sqlDBRestore.QueryRow(t, `RESTORE data.* FROM $1`, LocalFoo).Scan(
			&unused, &unused, &unused, &importedRows, &unused, &unused,
		)

		if importedRows != totalRows {
			t.Fatalf("expected %d rows, got %d", totalRows, importedRows)
		}

		var rowCount int64
		sqlDBRestore.QueryRow(t, `SELECT count(*) FROM data.bank`).Scan(&rowCount)
		if rowCount != numAccounts {
			t.Errorf("expected %d rows but found %d", numAccounts, rowCount)
		}
		sqlDBRestore.QueryRow(t, `SELECT count(*) FROM data.i0`).Scan(&rowCount)
		if rowCount != 2*numAccounts {
			t.Errorf("expected %d rows but found %d", 2*numAccounts, rowCount)
		}
		sqlDBRestore.QueryRow(t, `SELECT count(*) FROM data.i0_0`).Scan(&rowCount)
		if rowCount != 3*numAccounts {
			t.Errorf("expected %d rows but found %d", 3*numAccounts, rowCount)
		}
		sqlDBRestore.QueryRow(t, `SELECT count(*) FROM data.i1`).Scan(&rowCount)
		if rowCount != 4*numAccounts {
			t.Errorf("expected %d rows but found %d", 4*numAccounts, rowCount)
		}
	})

	t.Run("interleaved table without parent", func(t *testing.T) {
		sqlDB.ExpectErr(t, "without interleave parent", `BACKUP data.i0 TO $1`, LocalFoo)

		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tcRestore.Stopper().Stop(context.Background())
		sqlDBRestore := sqlutils.MakeSQLRunner(tcRestore.Conns[0])
		sqlDBRestore.Exec(t, `CREATE DATABASE data`)
		sqlDBRestore.ExpectErr(
			t, "without interleave parent",
			`RESTORE TABLE data.i0 FROM $1`, LocalFoo,
		)
	})

	t.Run("interleaved table without child", func(t *testing.T) {
		sqlDB.ExpectErr(t, "without interleave child", `BACKUP data.bank TO $1`, LocalFoo)

		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tcRestore.Stopper().Stop(context.Background())
		sqlDBRestore := sqlutils.MakeSQLRunner(tcRestore.Conns[0])
		sqlDBRestore.Exec(t, `CREATE DATABASE data`)
		sqlDBRestore.ExpectErr(t, "without interleave child", `RESTORE TABLE data.bank FROM $1`, LocalFoo)
	})
}

func TestBackupRestoreCrossTableReferences(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 30
	const createStore = "CREATE DATABASE store"
	const createStoreStats = "CREATE DATABASE storestats"

	_, _, origDB, dir, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()
	args := base.TestServerArgs{ExternalIODir: dir}

	// Generate some testdata and back it up.
	{
		origDB.Exec(t, "SET CLUSTER SETTING sql.cross_db_views.enabled = TRUE")
		origDB.Exec(t, createStore)
		origDB.Exec(t, createStoreStats)

		// customers has multiple inbound FKs, to different indexes.
		origDB.Exec(t, `CREATE TABLE store.customers (
			id INT PRIMARY KEY,
			email STRING UNIQUE
		)`)

		// orders has both in and outbound FKs (receipts and customers).
		// the index on placed makes indexIDs non-contiguous.
		origDB.Exec(t, `CREATE TABLE store.orders (
			id INT PRIMARY KEY,
			placed TIMESTAMP,
			INDEX (placed DESC),
			customerid INT REFERENCES store.customers
		)`)

		// unused makes our table IDs non-contiguous.
		origDB.Exec(t, `CREATE TABLE data.unused (id INT PRIMARY KEY)`)

		// receipts is has a self-referential FK.
		origDB.Exec(t, `CREATE TABLE store.receipts (
			id INT PRIMARY KEY,
			reissue INT REFERENCES store.receipts(id),
			dest STRING REFERENCES store.customers(email),
			orderid INT REFERENCES store.orders
		)`)

		// and a few views for good measure.
		origDB.Exec(t, `CREATE VIEW store.early_customers AS SELECT id, email from store.customers WHERE id < 5`)
		origDB.Exec(t, `CREATE VIEW storestats.ordercounts AS
			SELECT c.id, c.email, count(o.id)
			FROM store.customers AS c
			LEFT OUTER JOIN store.orders AS o ON o.customerid = c.id
			GROUP BY c.id, c.email
			ORDER BY c.id, c.email
		`)
		origDB.Exec(t, `CREATE VIEW store.unused_view AS SELECT id from store.customers WHERE FALSE`)
		origDB.Exec(t, `CREATE VIEW store.referencing_early_customers AS SELECT id, email FROM store.early_customers`)

		for i := 0; i < numAccounts; i++ {
			origDB.Exec(t, `INSERT INTO store.customers VALUES ($1, $1::string)`, i)
		}
		// Each even customerID gets 3 orders, with predictable order and receipt IDs.
		for cID := 0; cID < numAccounts; cID += 2 {
			for i := 0; i < 3; i++ {
				oID := cID*100 + i
				rID := oID * 10
				origDB.Exec(t, `INSERT INTO store.orders VALUES ($1, now(), $2)`, oID, cID)
				origDB.Exec(t, `INSERT INTO store.receipts VALUES ($1, NULL, $2, $3)`, rID, cID, oID)
				if i > 1 {
					origDB.Exec(t, `INSERT INTO store.receipts VALUES ($1, $2, $3, $4)`, rID+1, rID, cID, oID)
				}
			}
		}
		_ = origDB.Exec(t, `BACKUP DATABASE store, storestats TO $1`, LocalFoo)
	}

	origCustomers := origDB.QueryStr(t, `SHOW CONSTRAINTS FROM store.customers`)
	origOrders := origDB.QueryStr(t, `SHOW CONSTRAINTS FROM store.orders`)
	origReceipts := origDB.QueryStr(t, `SHOW CONSTRAINTS FROM store.receipts`)

	origEarlyCustomers := origDB.QueryStr(t, `SELECT * from store.early_customers`)
	origOrderCounts := origDB.QueryStr(t, `SELECT * from storestats.ordercounts ORDER BY id`)

	t.Run("restore everything to new cluster", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.Background())
		db := sqlutils.MakeSQLRunner(tc.Conns[0])

		db.Exec(t, createStore)
		db.Exec(t, `RESTORE store.* FROM $1`, LocalFoo)
		// Restore's Validate checks all the tables point to each other correctly.

		db.CheckQueryResults(t, `SHOW CONSTRAINTS FROM store.customers`, origCustomers)
		db.CheckQueryResults(t, `SHOW CONSTRAINTS FROM store.orders`, origOrders)
		db.CheckQueryResults(t, `SHOW CONSTRAINTS FROM store.receipts`, origReceipts)

		// FK validation on customers from receipts is preserved.
		db.ExpectErr(
			t, "update.*violates foreign key constraint \"fk_dest_ref_customers\"",
			`UPDATE store.customers SET email = concat(id::string, 'nope')`,
		)

		// FK validation on customers from orders is preserved.
		db.ExpectErr(
			t, "update.*violates foreign key constraint \"fk_customerid_ref_customers\"",
			`UPDATE store.customers SET id = id * 1000`,
		)

		// FK validation of customer id is preserved.
		db.ExpectErr(
			t, "insert.*violates foreign key constraint \"fk_customerid_ref_customers\"",
			`INSERT INTO store.orders VALUES (999, NULL, 999)`,
		)

		// FK validation of self-FK is preserved.
		db.ExpectErr(
			t, "insert.*violates foreign key constraint \"fk_reissue_ref_receipts\"",
			`INSERT INTO store.receipts VALUES (1, 999, NULL, NULL)`,
		)
	})

	t.Run("restore customers to new cluster", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.Background())
		db := sqlutils.MakeSQLRunner(tc.Conns[0])
		db.Exec(t, createStore)
		db.Exec(t, `RESTORE store.customers, store.orders FROM $1`, LocalFoo)
		// Restore's Validate checks all the tables point to each other correctly.

		// FK validation on customers from orders is preserved.
		db.ExpectErr(
			t, "update.*violates foreign key constraint \"fk_customerid_ref_customers\"",
			`UPDATE store.customers SET id = id*100`,
		)

		// FK validation on customers from receipts is gone.
		db.Exec(t, `UPDATE store.customers SET email = id::string`)
	})

	t.Run("restore orders to new cluster", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.Background())
		db := sqlutils.MakeSQLRunner(tc.Conns[0])
		db.Exec(t, createStore)

		// FK validation of self-FK is preserved.
		db.ExpectErr(
			t, "cannot restore table \"orders\" without referenced table .* \\(or \"skip_missing_foreign_keys\" option\\)",
			`RESTORE store.orders FROM $1`, LocalFoo,
		)

		db.Exec(t, `RESTORE store.orders FROM $1 WITH OPTIONS (skip_missing_foreign_keys)`, LocalFoo)
		// Restore's Validate checks all the tables point to each other correctly.

		// FK validation is gone.
		db.Exec(t, `INSERT INTO store.orders VALUES (999, NULL, 999)`)
		db.Exec(t, `DELETE FROM store.orders`)
	})

	t.Run("restore receipts to new cluster", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.Background())
		db := sqlutils.MakeSQLRunner(tc.Conns[0])
		db.Exec(t, createStore)
		db.Exec(t, `RESTORE store.receipts FROM $1 WITH OPTIONS (skip_missing_foreign_keys)`, LocalFoo)
		// Restore's Validate checks all the tables point to each other correctly.

		// FK validation of orders and customer is gone.
		db.Exec(t, `INSERT INTO store.receipts VALUES (1, NULL, '987', 999)`)

		// FK validation of self-FK is preserved.
		db.ExpectErr(
			t, "insert.*violates foreign key constraint \"fk_reissue_ref_receipts\"",
			`INSERT INTO store.receipts VALUES (-1, 999, NULL, NULL)`,
		)
	})

	t.Run("restore receipts and customers to new cluster", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.Background())
		db := sqlutils.MakeSQLRunner(tc.Conns[0])
		db.Exec(t, createStore)
		db.Exec(t, `RESTORE store.receipts, store.customers FROM $1 WITH OPTIONS (skip_missing_foreign_keys)`, LocalFoo)
		// Restore's Validate checks all the tables point to each other correctly.

		// FK validation of orders is gone.
		db.Exec(t, `INSERT INTO store.receipts VALUES (1, NULL, '0', 999)`)

		// FK validation of customer email is preserved.
		db.ExpectErr(
			t, "nsert.*violates foreign key constraint \"fk_dest_ref_customers\"",
			`INSERT INTO store.receipts VALUES (-1, NULL, '999', 999)`,
		)

		// FK validation on customers from receipts is preserved.
		db.ExpectErr(
			t, "delete.*violates foreign key constraint \"fk_dest_ref_customers\"",
			`DELETE FROM store.customers`,
		)

		// FK validation of self-FK is preserved.
		db.ExpectErr(
			t, "insert.*violates foreign key constraint \"fk_reissue_ref_receipts\"",
			`INSERT INTO store.receipts VALUES (-1, 999, NULL, NULL)`,
		)
	})

	t.Run("restore simple view", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.Background())
		db := sqlutils.MakeSQLRunner(tc.Conns[0])
		db.Exec(t, createStore)
		db.ExpectErr(
			t, `cannot restore view "early_customers" without restoring referenced table`,
			`RESTORE store.early_customers FROM $1`, LocalFoo,
		)
		db.Exec(t, `RESTORE store.early_customers, store.customers, store.orders FROM $1`, LocalFoo)
		db.CheckQueryResults(t, `SELECT * FROM store.early_customers`, origEarlyCustomers)

		// nothing depends on orders so it can be dropped.
		db.Exec(t, `DROP TABLE store.orders`)

		// customers is aware of the view that depends on it.
		db.ExpectErr(
			t, `cannot drop relation "customers" because view "early_customers" depends on it`,
			`DROP TABLE store.customers`,
		)

		// We want to be able to drop columns not used by the view,
		// however the detection thereof is currently broken - #17269.
		//
		// // columns not depended on by the view are unaffected.
		// db.Exec(`ALTER TABLE store.customers DROP COLUMN email`)
		// db.CheckQueryResults(t, `SELECT * FROM store.early_customers`, origEarlyCustomers)

		db.Exec(t, `DROP TABLE store.customers CASCADE`)
	})

	t.Run("restore multi-table view", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.Background())
		db := sqlutils.MakeSQLRunner(tc.Conns[0])

		db.ExpectErr(
			t, `cannot restore view "ordercounts" without restoring referenced table`,
			`RESTORE DATABASE storestats FROM $1`, LocalFoo,
		)

		db.Exec(t, createStore)
		db.Exec(t, createStoreStats)

		db.ExpectErr(
			t, `cannot restore view "ordercounts" without restoring referenced table`,
			`RESTORE storestats.ordercounts, store.customers FROM $1`, LocalFoo,
		)

		db.Exec(t, `RESTORE store.customers, storestats.ordercounts, store.orders FROM $1`, LocalFoo)

		// we want to observe just the view-related errors, not fk errors below.
		db.Exec(t, `ALTER TABLE store.orders DROP CONSTRAINT fk_customerid_ref_customers`)

		// customers is aware of the view that depends on it.
		db.ExpectErr(
			t, `cannot drop relation "customers" because view "storestats.public.ordercounts" depends on it`,
			`DROP TABLE store.customers`,
		)
		db.ExpectErr(
			t, `cannot drop column "email" because view "storestats.public.ordercounts" depends on it`,
			`ALTER TABLE store.customers DROP COLUMN email`,
		)

		// orders is aware of the view that depends on it.
		db.ExpectErr(
			t, `cannot drop relation "orders" because view "storestats.public.ordercounts" depends on it`,
			`DROP TABLE store.orders`,
		)

		db.CheckQueryResults(t, `SELECT * FROM storestats.ordercounts ORDER BY id`, origOrderCounts)

		db.Exec(t, `CREATE DATABASE otherstore`)
		db.Exec(t, `RESTORE store.* FROM $1 WITH into_db = 'otherstore'`, LocalFoo)
		// we want to observe just the view-related errors, not fk errors below.
		db.Exec(t, `ALTER TABLE otherstore.orders DROP CONSTRAINT fk_customerid_ref_customers`)
		db.Exec(t, `DROP TABLE otherstore.receipts`)

		db.ExpectErr(
			t, `cannot drop relation "customers" because view "early_customers" depends on it`,
			`DROP TABLE otherstore.customers`,
		)

		db.ExpectErr(t, `cannot drop column "email" because view "early_customers" depends on it`,
			`ALTER TABLE otherstore.customers DROP COLUMN email`,
		)
		db.Exec(t, `DROP DATABASE store CASCADE`)
		db.CheckQueryResults(t, `SELECT * FROM otherstore.early_customers ORDER BY id`, origEarlyCustomers)

	})

	t.Run("restore and skip missing views", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.Background())
		db := sqlutils.MakeSQLRunner(tc.Conns[0])

		// Test cases where, after filtering out views that can't be restored, there are no other tables to restore

		db.Exec(t, `RESTORE DATABASE storestats from $1 WITH OPTIONS (skip_missing_views)`, LocalFoo)
		db.Exec(t, `RESTORE storestats.ordercounts from $1 WITH OPTIONS (skip_missing_views)`, LocalFoo)
		// Ensure that the views were not restored since they are missing the tables they reference.
		db.CheckQueryResults(t, `USE storestats; SHOW TABLES;`, [][]string{})

		db.Exec(t, `RESTORE store.early_customers, store.referencing_early_customers from $1 WITH OPTIONS (skip_missing_views)`, LocalFoo)
		// Ensure that the views were not restored since they are missing the tables they reference.
		db.CheckQueryResults(t, `SHOW TABLES;`, [][]string{})

		// Test that views with valid dependencies are restored

		db.Exec(t, `RESTORE DATABASE store from $1 WITH OPTIONS (skip_missing_views)`, LocalFoo)
		db.CheckQueryResults(t, `SELECT * FROM store.early_customers`, origEarlyCustomers)
		db.CheckQueryResults(t, `SELECT * FROM store.referencing_early_customers`, origEarlyCustomers)
		// TODO(lucy, jordan): DROP DATABASE CASCADE doesn't work in the mixed 19.1/
		// 19.2 state, which is unrelated to backup/restore. See #39504 for a
		// description of that problem, which is yet to be investigated.
		// db.Exec(t, `DROP DATABASE store CASCADE`)

		// Test when some tables (views) are skipped and others are restored

		// See above comment for why we can't delete store and have to create
		// another database for now....
		// db.Exec(t, createStore)
		// storestats.ordercounts depends also on store.orders, so it can't be restored
		db.Exec(t, `CREATE DATABASE store2`)
		db.Exec(t, `RESTORE storestats.ordercounts, store.customers from $1 WITH OPTIONS (skip_missing_views, into_db='store2')`, LocalFoo)
		db.CheckQueryResults(t, `SHOW CONSTRAINTS FROM store2.customers`, origCustomers)
		db.ExpectErr(t, `relation "storestats.ordercounts" does not exist`, `SELECT * FROM storestats.ordercounts`)
	})
}

func checksumBankPayload(t *testing.T, sqlDB *sqlutils.SQLRunner) uint32 {
	crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	rows := sqlDB.Query(t, `SELECT id, balance, payload FROM data.bank`)
	defer rows.Close()
	var id, balance int
	var payload []byte
	for rows.Next() {
		if err := rows.Scan(&id, &balance, &payload); err != nil {
			t.Fatal(err)
		}
		if _, err := crc.Write(payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return crc.Sum32()
}

func TestBackupRestoreIncremental(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 10
	const numBackups = 4
	windowSize := int(numAccounts / 3)

	_, tc, sqlDB, dir, cleanupFn := BackupRestoreTestSetup(t, singleNode, 0, InitNone)
	defer cleanupFn()
	args := base.TestServerArgs{ExternalIODir: dir}
	rng, _ := randutil.NewPseudoRand()

	var backupDirs []string
	var checksums []uint32
	{
		for backupNum := 0; backupNum < numBackups; backupNum++ {
			// In the following, windowSize is `w` and offset is `o`. The first
			// mutation creates accounts with id [w,3w). Every mutation after
			// that deletes everything less than o, leaves [o, o+w) unchanged,
			// mutates [o+w,o+2w), and inserts [o+2w,o+3w).
			offset := windowSize * backupNum
			var buf bytes.Buffer
			fmt.Fprintf(&buf, `DELETE FROM data.bank WHERE id < %d; `, offset)
			buf.WriteString(`UPSERT INTO data.bank VALUES `)
			for j := 0; j < windowSize*2; j++ {
				if j != 0 {
					buf.WriteRune(',')
				}
				id := offset + windowSize + j
				payload := randutil.RandBytes(rng, backupRestoreRowPayloadSize)
				fmt.Fprintf(&buf, `(%d, %d, '%s')`, id, backupNum, payload)
			}
			sqlDB.Exec(t, buf.String())

			checksums = append(checksums, checksumBankPayload(t, sqlDB))

			backupDir := fmt.Sprintf("nodelocal://0/%d", backupNum)
			var from string
			if backupNum > 0 {
				from = fmt.Sprintf(` INCREMENTAL FROM %s`, strings.Join(backupDirs, `,`))
			}
			sqlDB.Exec(t, fmt.Sprintf(`BACKUP TABLE data.bank TO '%s' %s`, backupDir, from))

			backupDirs = append(backupDirs, fmt.Sprintf(`'%s'`, backupDir))
		}

		// Test a regression in RESTORE where the batch end key was not
		// being set correctly in Import: make an incremental backup such that
		// the greatest key in the diff is less than the previous backups.
		sqlDB.Exec(t, `INSERT INTO data.bank VALUES (0, -1, 'final')`)
		checksums = append(checksums, checksumBankPayload(t, sqlDB))
		sqlDB.Exec(t, fmt.Sprintf(`BACKUP TABLE data.bank TO '%s' %s`,
			"nodelocal://0/final", fmt.Sprintf(` INCREMENTAL FROM %s`, strings.Join(backupDirs, `,`)),
		))
		backupDirs = append(backupDirs, `'nodelocal://0/final'`)
	}

	// Start a new cluster to restore into.
	{
		restoreTC := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer restoreTC.Stopper().Stop(context.Background())
		sqlDBRestore := sqlutils.MakeSQLRunner(restoreTC.Conns[0])

		sqlDBRestore.Exec(t, `CREATE DATABASE data`)
		sqlDBRestore.Exec(t, `CREATE TABLE data.bank (id INT PRIMARY KEY)`)
		// This "data.bank" table isn't actually the same table as the backup at all
		// so we should not allow using a backup of the other in incremental. We
		// usually compare IDs, but those are only meaningful in the context of a
		// single cluster, so we also need to ensure the previous backup was indeed
		// generated by the same cluster.

		sqlDBRestore.ExpectErr(
			t, fmt.Sprintf("belongs to cluster %s", tc.Servers[0].ClusterID()),
			`BACKUP TABLE data.bank TO $1 INCREMENTAL FROM $2`,
			"nodelocal://0/some-other-table", "nodelocal://0/0",
		)

		for i := len(backupDirs); i > 0; i-- {
			sqlDBRestore.Exec(t, `DROP TABLE IF EXISTS data.bank`)
			from := strings.Join(backupDirs[:i], `,`)
			sqlDBRestore.Exec(t, fmt.Sprintf(`RESTORE data.bank FROM %s`, from))

			checksum := checksumBankPayload(t, sqlDBRestore)
			if checksum != checksums[i-1] {
				t.Fatalf("checksum mismatch at index %d: got %d expected %d",
					i-1, checksum, checksums[i])
			}
		}
	}
}

func TestBackupRestorePartitionedIncremental(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 10
	const numBackups = 4
	windowSize := int(numAccounts / 3)

	_, _, sqlDB, dir, cleanupFn := BackupRestoreTestSetup(t, MultiNode, 0, InitNone)
	defer cleanupFn()
	args := base.TestServerArgs{ExternalIODir: dir}
	rng, _ := randutil.NewPseudoRand()

	// Each incremental backup is written to two different subdirectories in
	// defaultDir and dc1Dir, respectively.
	const defaultDir = "nodelocal://0/default"
	const dc1Dir = "nodelocal://0/dc=dc1"
	var defaultBackupDirs []string
	var checksums []uint32
	{
		for backupNum := 0; backupNum < numBackups; backupNum++ {
			// In the following, windowSize is `w` and offset is `o`. The first
			// mutation creates accounts with id [w,3w). Every mutation after
			// that deletes everything less than o, leaves [o, o+w) unchanged,
			// mutates [o+w,o+2w), and inserts [o+2w,o+3w).
			offset := windowSize * backupNum
			var buf bytes.Buffer
			fmt.Fprintf(&buf, `DELETE FROM data.bank WHERE id < %d; `, offset)
			buf.WriteString(`UPSERT INTO data.bank VALUES `)
			for j := 0; j < windowSize*2; j++ {
				if j != 0 {
					buf.WriteRune(',')
				}
				id := offset + windowSize + j
				payload := randutil.RandBytes(rng, backupRestoreRowPayloadSize)
				fmt.Fprintf(&buf, `(%d, %d, '%s')`, id, backupNum, payload)
			}
			sqlDB.Exec(t, buf.String())

			checksums = append(checksums, checksumBankPayload(t, sqlDB))

			defaultBackupDir := fmt.Sprintf("%s/%d", defaultDir, backupNum)
			dc1BackupDir := fmt.Sprintf("%s/%d", dc1Dir, backupNum)
			var from string
			if backupNum > 0 {
				from = fmt.Sprintf(` INCREMENTAL FROM %s`, strings.Join(defaultBackupDirs, `,`))
			}
			sqlDB.Exec(
				t,
				fmt.Sprintf(`BACKUP TABLE data.bank TO ('%s?COCKROACH_LOCALITY=%s', '%s?COCKROACH_LOCALITY=%s') %s`,
					defaultBackupDir, url.QueryEscape("default"),
					dc1BackupDir, url.QueryEscape("dc=dc1"),
					from),
			)

			defaultBackupDirs = append(defaultBackupDirs, fmt.Sprintf(`'%s'`, defaultBackupDir))
		}
	}

	// Start a new cluster to restore into.
	{
		restoreTC := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer restoreTC.Stopper().Stop(context.Background())
		sqlDBRestore := sqlutils.MakeSQLRunner(restoreTC.Conns[0])

		sqlDBRestore.Exec(t, `CREATE DATABASE data`)
		for i := len(defaultBackupDirs); i > 0; i-- {
			sqlDBRestore.Exec(t, `DROP TABLE IF EXISTS data.bank`)
			var from strings.Builder
			for backupNum := range defaultBackupDirs[:i] {
				if backupNum > 0 {
					from.WriteString(", ")
				}
				from.WriteString(fmt.Sprintf("('%s/%d', '%s/%d')", defaultDir, backupNum, dc1Dir, backupNum))
			}
			sqlDBRestore.Exec(t, fmt.Sprintf(`RESTORE data.bank FROM %s`, from.String()))

			checksum := checksumBankPayload(t, sqlDBRestore)
			if checksum != checksums[i-1] {
				t.Fatalf("checksum mismatch at index %d: got %d expected %d",
					i-1, checksum, checksums[i])
			}
		}
	}
}

// a bg worker is intended to write to the bank table concurrent with other
// operations (writes, backups, restores), mutating the payload on rows-maxID.
// it notified the `wake` channel (to allow ensuring bg activity has occurred)
// and can be informed when errors are allowable (e.g. when the bank table is
// unavailable between a drop and restore) via the atomic "bool" allowErrors.
func startBackgroundWrites(
	stopper *stop.Stopper, sqlDB *gosql.DB, maxID int, wake chan<- struct{}, allowErrors *int32,
) error {
	rng, _ := randutil.NewPseudoRand()

	for {
		select {
		case <-stopper.ShouldQuiesce():
			return nil // All done.
		default:
			// Keep going.
		}

		id := rand.Intn(maxID)
		payload := randutil.RandBytes(rng, backupRestoreRowPayloadSize)

		updateFn := func() error {
			select {
			case <-stopper.ShouldQuiesce():
				return nil // All done.
			default:
				// Keep going.
			}
			_, err := sqlDB.Exec(`UPDATE data.bank SET payload = $1 WHERE id = $2`, payload, id)
			if atomic.LoadInt32(allowErrors) == 1 {
				return nil
			}
			return err
		}
		if err := retry.ForDuration(testutils.DefaultSucceedsSoonDuration, updateFn); err != nil {
			return err
		}
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

func TestBackupRestoreWithConcurrentWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const rows = 10
	const numBackgroundTasks = MultiNode

	_, tc, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, MultiNode, rows, InitNone)
	defer cleanupFn()

	bgActivity := make(chan struct{})
	// allowErrors is used as an atomic bool to tell bg workers when to allow
	// errors, between dropping and restoring the table they are using.
	var allowErrors int32
	for task := 0; task < numBackgroundTasks; task++ {
		taskNum := task
		tc.Stopper().RunWorker(context.Background(), func(context.Context) {
			conn := tc.Conns[taskNum%len(tc.Conns)]
			// Use different sql gateways to make sure leasing is right.
			if err := startBackgroundWrites(tc.Stopper(), conn, rows, bgActivity, &allowErrors); err != nil {
				t.Error(err)
			}
		})
	}

	// Use the data.bank table as a key (id), value (balance) table with a
	// payload.The background tasks are mutating the table concurrently while we
	// backup and restore.
	<-bgActivity

	// Set, break, then reset the id=balance invariant -- while doing concurrent
	// writes -- to get multiple MVCC revisions as well as txn conflicts.
	sqlDB.Exec(t, `UPDATE data.bank SET balance = id`)
	<-bgActivity
	sqlDB.Exec(t, `UPDATE data.bank SET balance = -1`)
	<-bgActivity
	sqlDB.Exec(t, `UPDATE data.bank SET balance = id`)
	<-bgActivity

	// Backup DB while concurrent writes continue.
	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, LocalFoo)

	// Drop the table and restore from backup and check our invariant.
	atomic.StoreInt32(&allowErrors, 1)
	sqlDB.Exec(t, `DROP TABLE data.bank`)
	sqlDB.Exec(t, `RESTORE data.* FROM $1`, LocalFoo)
	atomic.StoreInt32(&allowErrors, 0)

	bad := sqlDB.QueryStr(t, `SELECT id, balance, payload FROM data.bank WHERE id != balance`)
	for _, r := range bad {
		t.Errorf("bad row ID %s = bal %s (payload: %q)", r[0], r[1], r[2])
	}
}

func TestConcurrentBackupRestores(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 10
	const concurrency, numIterations = 2, 3
	ctx, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, MultiNode, numAccounts, InitNone)
	defer cleanupFn()

	g, gCtx := errgroup.WithContext(ctx)
	for i := 0; i < concurrency; i++ {
		table := fmt.Sprintf("bank_%d", i)
		sqlDB.Exec(t, fmt.Sprintf(
			`CREATE TABLE data.%s AS (SELECT * FROM data.bank WHERE id > %d ORDER BY id)`,
			table, i,
		))
		g.Go(func() error {
			for j := 0; j < numIterations; j++ {
				dbName := fmt.Sprintf("%s_%d", table, j)
				backupDir := fmt.Sprintf("nodelocal://0/%s", dbName)
				backupQ := fmt.Sprintf(`BACKUP data.%s TO $1`, table)
				if _, err := sqlDB.DB.ExecContext(gCtx, backupQ, backupDir); err != nil {
					return err
				}
				if _, err := sqlDB.DB.ExecContext(gCtx, fmt.Sprintf(`CREATE DATABASE %s`, dbName)); err != nil {
					return err
				}
				restoreQ := fmt.Sprintf(`RESTORE data.%s FROM $1 WITH OPTIONS (into_db='%s')`, table, dbName)
				if _, err := sqlDB.DB.ExecContext(gCtx, restoreQ, backupDir); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatalf("%+v", err)
	}

	for i := 0; i < concurrency; i++ {
		orig := sqlDB.QueryStr(t, `SELECT * FROM data.bank WHERE id > $1 ORDER BY id`, i)
		for j := 0; j < numIterations; j++ {
			selectQ := fmt.Sprintf(`SELECT * FROM bank_%d_%d.bank_%d ORDER BY id`, i, j, i)
			sqlDB.CheckQueryResults(t, selectQ, orig)
		}
	}
}

func TestBackupTenantsWithRevisionHistory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	ctx, tc, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	_, _, err := tc.Servers[0].StartTenant(base.TestTenantArgs{TenantID: roachpb.MakeTenantID(10)})
	require.NoError(t, err)

	const msg = "can not backup tenants with revision history"

	_, err = sqlDB.DB.ExecContext(ctx, `BACKUP TENANT 10 TO 'nodelocal://0/' WITH revision_history`)
	require.Contains(t, fmt.Sprint(err), msg)

	_, err = sqlDB.DB.ExecContext(ctx, `BACKUP TO 'nodelocal://0/' WITH revision_history`)
	require.Contains(t, fmt.Sprint(err), msg)
}

func TestBackupAsOfSystemTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1000

	ctx, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	var beforeTs, equalTs string
	var rowCount int

	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&beforeTs)

	err := crdb.ExecuteTx(ctx, sqlDB.DB.(*gosql.DB), nil /* txopts */, func(tx *gosql.Tx) error {
		_, err := tx.Exec(`DELETE FROM data.bank WHERE id % 4 = 1`)
		if err != nil {
			return err
		}
		return tx.QueryRow(`SELECT cluster_logical_timestamp()`).Scan(&equalTs)
	})
	if err != nil {
		t.Fatal(err)
	}

	sqlDB.QueryRow(t, `SELECT count(*) FROM data.bank`).Scan(&rowCount)
	if expected := numAccounts * 3 / 4; rowCount != expected {
		t.Fatalf("expected %d rows but found %d", expected, rowCount)
	}

	beforeDir := LocalFoo + `/beforeTs`
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE data TO '%s' AS OF SYSTEM TIME %s`, beforeDir, beforeTs))
	equalDir := LocalFoo + `/equalTs`
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE data TO '%s' AS OF SYSTEM TIME %s`, equalDir, equalTs))

	sqlDB.Exec(t, `DROP TABLE data.bank`)
	sqlDB.Exec(t, `RESTORE data.* FROM $1`, beforeDir)
	sqlDB.QueryRow(t, `SELECT count(*) FROM data.bank`).Scan(&rowCount)
	if expected := numAccounts; rowCount != expected {
		t.Fatalf("expected %d rows but found %d", expected, rowCount)
	}

	sqlDB.Exec(t, `DROP TABLE data.bank`)
	sqlDB.Exec(t, `RESTORE data.* FROM $1`, equalDir)
	sqlDB.QueryRow(t, `SELECT count(*) FROM data.bank`).Scan(&rowCount)
	if expected := numAccounts * 3 / 4; rowCount != expected {
		t.Fatalf("expected %d rows but found %d", expected, rowCount)
	}
}

func TestRestoreAsOfSystemTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 10
	ctx, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()
	const dir = "nodelocal://0/"

	ts := make([]string, 9)

	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[0])

	sqlDB.Exec(t, `UPDATE data.bank SET balance = 1`)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[1])

	// Change the data in the tabe.
	sqlDB.Exec(t, `CREATE TABLE data.teller (id INT PRIMARY KEY, name STRING)`)
	sqlDB.Exec(t, `INSERT INTO data.teller VALUES (1, 'alice'), (7, 'bob'), (3, 'eve')`)

	err := crdb.ExecuteTx(ctx, sqlDB.DB.(*gosql.DB), nil /* txopts */, func(tx *gosql.Tx) error {
		_, err := tx.Exec(`UPDATE data.bank SET balance = 2`)
		if err != nil {
			return err
		}
		return tx.QueryRow(`SELECT cluster_logical_timestamp()`).Scan(&ts[2])
	})
	if err != nil {
		t.Fatal(err)
	}

	fullBackup, latestBackup := dir+"/full", dir+"/latest"
	incBackup, incLatestBackup := dir+"/inc", dir+"/inc-latest"
	inc2Backup, inc2LatestBackup := incBackup+".2", incLatestBackup+".2"

	sqlDB.Exec(t,
		fmt.Sprintf(`BACKUP DATABASE data TO $1 AS OF SYSTEM TIME %s WITH revision_history`, ts[2]),
		fullBackup,
	)
	sqlDB.Exec(t,
		fmt.Sprintf(`BACKUP DATABASE data TO $1 AS OF SYSTEM TIME %s`, ts[2]),
		latestBackup,
	)

	fullTableBackup := dir + "/tbl"
	sqlDB.Exec(t,
		fmt.Sprintf(`BACKUP data.bank TO $1 AS OF SYSTEM TIME %s WITH revision_history`, ts[2]),
		fullTableBackup,
	)

	sqlDB.Exec(t, `UPDATE data.bank SET balance = 3`)

	// Create a table in some other DB -- this won't be in this backup (yet).
	sqlDB.Exec(t, `CREATE DATABASE other`)
	sqlDB.Exec(t, `CREATE TABLE other.sometable (id INT PRIMARY KEY, somevalue INT)`)
	sqlDB.Exec(t, `INSERT INTO other.sometable VALUES (1, 2), (7, 5), (3, 3)`)

	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[3])

	sqlDB.Exec(t, `DELETE FROM data.bank WHERE id >= $1 / 2`, numAccounts)
	sqlDB.Exec(t, `ALTER TABLE other.sometable RENAME TO data.sometable`)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[4])

	sqlDB.Exec(t, `INSERT INTO data.sometable VALUES (2, 2), (4, 5), (6, 3)`)
	sqlDB.Exec(t, `ALTER TABLE data.bank ADD COLUMN points_balance INT DEFAULT 50`)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[5])

	sqlDB.Exec(t, `TRUNCATE TABLE data.bank`)
	sqlDB.Exec(t, `TRUNCATE TABLE data.bank`)
	sqlDB.Exec(t, `TRUNCATE TABLE data.bank`)
	sqlDB.Exec(t, `ALTER TABLE data.sometable RENAME TO other.sometable`)
	sqlDB.Exec(t, `CREATE INDEX ON data.teller (name)`)
	sqlDB.Exec(t, `INSERT INTO data.bank VALUES (2, 2), (4, 4)`)
	sqlDB.Exec(t, `INSERT INTO data.teller VALUES (2, 'craig')`)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[6])

	sqlDB.Exec(t, `TRUNCATE TABLE data.bank`)
	sqlDB.Exec(t, `INSERT INTO data.bank VALUES (2, 2), (4, 4)`)
	sqlDB.Exec(t, `DROP TABLE other.sometable`)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[7])

	sqlDB.Exec(t, `UPSERT INTO data.bank (id, balance)
	           SELECT i, 4 FROM generate_series(0, $1 - 1) AS g(i)`, numAccounts)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[8])

	sqlDB.Exec(t,
		fmt.Sprintf(`BACKUP DATABASE data TO $1 AS OF SYSTEM TIME %s INCREMENTAL FROM $2 WITH revision_history`, ts[5]),
		incBackup, fullBackup,
	)
	sqlDB.Exec(t,
		`BACKUP DATABASE data TO $1 INCREMENTAL FROM $2, $3 WITH revision_history`,
		inc2Backup, fullBackup, incBackup,
	)

	sqlDB.Exec(t,
		fmt.Sprintf(`BACKUP DATABASE data TO $1	AS OF SYSTEM TIME %s INCREMENTAL FROM $2`, ts[5]),
		incLatestBackup, latestBackup,
	)
	sqlDB.Exec(t,
		`BACKUP DATABASE data TO $1 INCREMENTAL FROM $2, $3`,
		inc2LatestBackup, latestBackup, incLatestBackup,
	)

	incTableBackup := dir + "/inctbl"
	sqlDB.Exec(t,
		`BACKUP data.bank TO $1 INCREMENTAL FROM $2 WITH revision_history`,
		incTableBackup, fullTableBackup,
	)

	var after string
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&after)

	for i, timestamp := range ts {
		name := fmt.Sprintf("ts%d", i)
		t.Run(name, func(t *testing.T) {
			sqlDB = sqlutils.MakeSQLRunner(sqlDB.DB)
			// Create new DBs into which we'll restore our copies without conflicting
			// with the existing, original table.
			sqlDB.Exec(t, fmt.Sprintf(`CREATE DATABASE %s`, name))
			sqlDB.Exec(t, fmt.Sprintf(`CREATE DATABASE %stbl`, name))
			// Restore the bank table from the full DB MVCC backup to time x, into a
			// separate DB so that we can later compare it to the original table via
			// time-travel.
			sqlDB.Exec(t,
				fmt.Sprintf(
					`RESTORE data.* FROM $1, $2, $3 AS OF SYSTEM TIME %s WITH into_db='%s'`,
					timestamp, name,
				),
				fullBackup, incBackup, inc2Backup,
			)
			// Similarly restore the since-table backup -- since full DB and single table
			// backups sometimes behave differently.
			sqlDB.Exec(t,
				fmt.Sprintf(
					`RESTORE data.bank FROM $1, $2 AS OF SYSTEM TIME %s WITH into_db='%stbl'`,
					timestamp, name,
				),
				fullTableBackup, incTableBackup,
			)

			// Use time-travel on the existing bank table to determine what RESTORE
			// with AS OF should have produced.
			expected := sqlDB.QueryStr(
				t, fmt.Sprintf(`SELECT * FROM data.bank AS OF SYSTEM TIME %s ORDER BY id`, timestamp),
			)
			// Confirm reading (with no as-of) from the as-of restored table matches.
			sqlDB.CheckQueryResults(t, fmt.Sprintf(`SELECT * FROM %s.bank ORDER BY id`, name), expected)
			sqlDB.CheckQueryResults(t, fmt.Sprintf(`SELECT * FROM %stbl.bank ORDER BY id`, name), expected)

			// `sometable` moved in to data between after ts 3 and removed before 5.
			if i == 4 || i == 5 {
				sqlDB.CheckQueryResults(t,
					fmt.Sprintf(`SELECT * FROM %s.sometable ORDER BY id`, name),
					sqlDB.QueryStr(t, fmt.Sprintf(`SELECT * FROM data.sometable AS OF SYSTEM TIME %s ORDER BY id`, timestamp)),
				)
			}
			// teller was created after ts 2.
			if i > 2 {
				sqlDB.CheckQueryResults(t,
					fmt.Sprintf(`SELECT * FROM %s.teller ORDER BY id`, name),
					sqlDB.QueryStr(t, fmt.Sprintf(`SELECT * FROM data.teller AS OF SYSTEM TIME %s ORDER BY id`, timestamp)),
				)
			}
		})
	}

	t.Run("latest", func(t *testing.T) {
		sqlDB = sqlutils.MakeSQLRunner(sqlDB.DB)
		// The "latest" backup didn't specify ALL mvcc values, so we can't restore
		// to times in the middle.
		sqlDB.Exec(t, `CREATE DATABASE err`)

		// fullBackup covers up to ts[2], inc to ts[5], inc2 to > ts[8].
		sqlDB.ExpectErr(
			t, "invalid RESTORE timestamp",
			fmt.Sprintf(`RESTORE data.* FROM $1 AS OF SYSTEM TIME %s WITH into_db='err'`, ts[3]),
			fullBackup,
		)

		for _, i := range ts {

			if i == ts[2] {
				// latestBackup is _at_ ts2 so that is the time, and the only time, at
				// which restoring it is allowed.
				sqlDB.Exec(
					t, fmt.Sprintf(`RESTORE data.* FROM $1 AS OF SYSTEM TIME %s WITH into_db='err'`, i),
					latestBackup,
				)
				sqlDB.Exec(t, `DROP DATABASE err; CREATE DATABASE err`)
			} else {
				sqlDB.ExpectErr(
					t, "invalid RESTORE timestamp",
					fmt.Sprintf(`RESTORE data.* FROM $1 AS OF SYSTEM TIME %s WITH into_db='err'`, i),
					latestBackup,
				)
			}

			if i == ts[2] || i == ts[5] {
				// latestBackup is _at_ ts2 and incLatestBackup is at ts5, so either of
				// those are valid for the chain (latest,incLatest,inc2Latest). In fact
				// there's a third time -- that of inc2Latest, that is valid as well but
				// it isn't fixed when created above so we know it / test for it.
				sqlDB.Exec(
					t, fmt.Sprintf(`RESTORE data.* FROM $1, $2, $3 AS OF SYSTEM TIME %s WITH into_db='err'`, i),
					latestBackup, incLatestBackup, inc2LatestBackup,
				)
				sqlDB.Exec(t, `DROP DATABASE err; CREATE DATABASE err`)
			} else {
				sqlDB.ExpectErr(
					t, "invalid RESTORE timestamp",
					fmt.Sprintf(`RESTORE data.* FROM $1, $2, $3 AS OF SYSTEM TIME %s WITH into_db='err'`, i),
					latestBackup, incLatestBackup, inc2LatestBackup,
				)
			}
		}

		sqlDB.ExpectErr(
			t, "invalid RESTORE timestamp",
			fmt.Sprintf(`RESTORE data.* FROM $1 AS OF SYSTEM TIME %s WITH into_db='err'`, after),
			latestBackup,
		)
	})

	t.Run("create-backup-drop-backup", func(t *testing.T) {
		var tsBefore string
		backupPath := "nodelocal://0/drop_table_db"

		sqlDB.Exec(t, "CREATE DATABASE drop_table_db")
		sqlDB.Exec(t, "CREATE DATABASE drop_table_db_restore")
		sqlDB.Exec(t, "CREATE TABLE drop_table_db.a (k int, v string)")
		sqlDB.Exec(t, `BACKUP DATABASE drop_table_db TO $1 WITH revision_history`, backupPath)
		sqlDB.Exec(t, "INSERT INTO drop_table_db.a VALUES (1, 'foo')")
		sqlDB.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&tsBefore)
		sqlDB.Exec(t, "DROP TABLE drop_table_db.a")
		sqlDB.Exec(t, `BACKUP DATABASE drop_table_db TO $1 WITH revision_history`, backupPath)
		restoreQuery := fmt.Sprintf(
			"RESTORE drop_table_db.* FROM $1 AS OF SYSTEM TIME %s WITH into_db='drop_table_db_restore'", tsBefore)
		sqlDB.Exec(t, restoreQuery, backupPath)

		restoredTableQuery := "SELECT * FROM drop_table_db_restore.a"
		backedUpTableQuery := fmt.Sprintf("SELECT * FROM drop_table_db.a AS OF SYSTEM TIME %s", tsBefore)
		sqlDB.CheckQueryResults(t, backedUpTableQuery, sqlDB.QueryStr(t, restoredTableQuery))
	})

	t.Run("backup-create-drop-backup", func(t *testing.T) {
		var tsBefore string
		backupPath := "nodelocal://0/create_and_drop"

		sqlDB.Exec(t, "CREATE DATABASE create_and_drop")
		sqlDB.Exec(t, "CREATE DATABASE create_and_drop_restore")
		sqlDB.Exec(t, `BACKUP DATABASE create_and_drop TO $1 WITH revision_history`, backupPath)
		sqlDB.Exec(t, "CREATE TABLE create_and_drop.a (k int, v string)")
		sqlDB.Exec(t, "INSERT INTO create_and_drop.a VALUES (1, 'foo')")
		sqlDB.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&tsBefore)
		sqlDB.Exec(t, "DROP TABLE create_and_drop.a")
		sqlDB.Exec(t, `BACKUP DATABASE create_and_drop TO $1 WITH revision_history`, backupPath)
		restoreQuery := fmt.Sprintf(
			"RESTORE create_and_drop.* FROM $1 AS OF SYSTEM TIME %s WITH into_db='create_and_drop_restore'", tsBefore)
		sqlDB.Exec(t, restoreQuery, backupPath)

		restoredTableQuery := "SELECT * FROM create_and_drop_restore.a"
		backedUpTableQuery := fmt.Sprintf("SELECT * FROM create_and_drop.a AS OF SYSTEM TIME %s", tsBefore)
		sqlDB.CheckQueryResults(t, backedUpTableQuery, sqlDB.QueryStr(t, restoredTableQuery))
	})

	// This is a regression test for #49707.
	t.Run("ignore-dropped-table", func(t *testing.T) {
		backupPath := "nodelocal://0/ignore_dropped_table"

		sqlDB.Exec(t, "CREATE DATABASE ignore_dropped_table")
		sqlDB.Exec(t, "CREATE TABLE ignore_dropped_table.a (k int, v string)")
		sqlDB.Exec(t, "CREATE TABLE ignore_dropped_table.b (k int, v string)")
		sqlDB.Exec(t, "DROP TABLE ignore_dropped_table.a")
		sqlDB.Exec(t, `BACKUP DATABASE ignore_dropped_table TO $1 WITH revision_history`, backupPath)
		// Make a backup without any changes to the schema. This ensures that table
		// "a" is not included in the span for this incremental backup.
		sqlDB.Exec(t, `BACKUP DATABASE ignore_dropped_table TO $1 WITH revision_history`, backupPath)
		// Edit the schemas to back up to ensure there are revisions generated.
		// Table a should not be considered part of the span of the next backup.
		sqlDB.Exec(t, "CREATE TABLE ignore_dropped_table.c (k int, v string)")
		sqlDB.Exec(t, `BACKUP DATABASE ignore_dropped_table TO $1 WITH revision_history`, backupPath)

		// Ensure it can be restored.
		sqlDB.Exec(t, "DROP DATABASE ignore_dropped_table")
		sqlDB.Exec(t, "RESTORE DATABASE ignore_dropped_table FROM $1", backupPath)
	})
}

func TestRestoreAsOfSystemTimeGCBounds(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 10
	ctx, tc, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()
	const dir = "nodelocal://0/"
	preGC := tree.TimestampToDecimalDatum(tc.Server(0).Clock().Now()).String()

	gcr := roachpb.GCRequest{
		// Bogus span to make it a valid request.
		RequestHeader: roachpb.RequestHeader{
			Key:    keys.SystemSQLCodec.TablePrefix(keys.MinUserDescID),
			EndKey: keys.MaxKey,
		},
		Threshold: tc.Server(0).Clock().Now(),
	}
	if _, err := kv.SendWrapped(
		ctx, tc.Server(0).DistSenderI().(*kvcoord.DistSender), &gcr,
	); err != nil {
		t.Fatal(err)
	}

	postGC := tree.TimestampToDecimalDatum(tc.Server(0).Clock().Now()).String()

	lateFullTableBackup := dir + "/tbl-after-gc"
	sqlDB.Exec(t, `BACKUP data.bank TO $1 WITH revision_history`, lateFullTableBackup)
	sqlDB.Exec(t, `DROP TABLE data.bank`)
	sqlDB.ExpectErr(
		t, `BACKUP for requested time only has revision history from`,
		fmt.Sprintf(`RESTORE data.bank FROM $1 AS OF SYSTEM TIME %s`, preGC),
		lateFullTableBackup,
	)
	sqlDB.Exec(
		t, fmt.Sprintf(`RESTORE data.bank FROM $1 AS OF SYSTEM TIME %s`, postGC), lateFullTableBackup,
	)
}

func TestAsOfSystemTimeOnRestoredData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 10
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()
	sqlDB.Exec(t, `BACKUP data.* To $1`, LocalFoo)

	sqlDB.Exec(t, `DROP TABLE data.bank`)

	var beforeTs string
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&beforeTs)
	sqlDB.Exec(t, `RESTORE data.* FROM $1`, LocalFoo)
	var afterTs string
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&afterTs)

	var rowCount int
	const q = `SELECT count(*) FROM data.bank AS OF SYSTEM TIME '%s'`
	// Before the RESTORE, the table doesn't exist, so an AS OF query should fail.
	sqlDB.ExpectErr(
		t, `relation "data.bank" does not exist`,
		fmt.Sprintf(q, beforeTs),
	)
	// After the RESTORE, an AS OF query should work.
	sqlDB.QueryRow(t, fmt.Sprintf(q, afterTs)).Scan(&rowCount)
	if expected := numAccounts; rowCount != expected {
		t.Fatalf("expected %d rows but found %d", expected, rowCount)
	}
}

func TestBackupRestoreChecksum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1000
	_, _, sqlDB, dir, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()
	dir = filepath.Join(dir, "foo")

	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, LocalFoo)

	var backupManifest BackupManifest
	{
		backupManifestBytes, err := ioutil.ReadFile(filepath.Join(dir, backupManifestName))
		if err != nil {
			t.Fatalf("%+v", err)
		}
		fileType := http.DetectContentType(backupManifestBytes)
		if fileType == ZipType {
			backupManifestBytes, err = decompressData(backupManifestBytes)
			require.NoError(t, err)
		}
		if err := protoutil.Unmarshal(backupManifestBytes, &backupManifest); err != nil {
			t.Fatalf("%+v", err)
		}
	}

	// Corrupt one of the files in the backup.
	f, err := os.OpenFile(filepath.Join(dir, backupManifest.Files[1].Path), os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	defer f.Close()
	// The last eight bytes of an SST file store a nonzero magic number. We can
	// blindly null out those bytes and guarantee that the checksum will change.
	if _, err := f.Seek(-8, io.SeekEnd); err != nil {
		t.Fatalf("%+v", err)
	}
	if _, err := f.Write(make([]byte, 8)); err != nil {
		t.Fatalf("%+v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("%+v", err)
	}

	sqlDB.Exec(t, `DROP TABLE data.bank`)
	sqlDB.ExpectErr(t, "checksum mismatch", `RESTORE data.* FROM $1`, LocalFoo)
}

func TestTimestampMismatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const numAccounts = 1

	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	sqlDB.Exec(t, `CREATE TABLE data.t2 (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO data.t2 VALUES (1)`)

	fullBackup := LocalFoo + "/0"
	incrementalT1FromFull := LocalFoo + "/1"
	incrementalT2FromT1 := LocalFoo + "/2"
	incrementalT3FromT1OneTable := LocalFoo + "/3"

	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`,
		fullBackup)
	sqlDB.Exec(t, `BACKUP DATABASE data TO $1 INCREMENTAL FROM $2`,
		incrementalT1FromFull, fullBackup)
	sqlDB.Exec(t, `BACKUP TABLE data.bank TO $1 INCREMENTAL FROM $2`,
		incrementalT3FromT1OneTable, fullBackup)
	sqlDB.Exec(t, `BACKUP DATABASE data TO $1 INCREMENTAL FROM $2, $3`,
		incrementalT2FromT1, fullBackup, incrementalT1FromFull)

	t.Run("Backup", func(t *testing.T) {
		// Missing the initial full backup.
		sqlDB.ExpectErr(
			t, "backups listed out of order",
			`BACKUP DATABASE data TO $1 INCREMENTAL FROM $2`,
			LocalFoo, incrementalT1FromFull,
		)

		// Missing an intermediate incremental backup.
		sqlDB.ExpectErr(
			t, "backups listed out of order",
			`BACKUP DATABASE data TO $1 INCREMENTAL FROM $2, $3`,
			LocalFoo, fullBackup, incrementalT2FromT1,
		)

		// Backups specified out of order.
		sqlDB.ExpectErr(
			t, "out of order",
			`BACKUP DATABASE data TO $1 INCREMENTAL FROM $2, $3`,
			LocalFoo, incrementalT1FromFull, fullBackup,
		)

		// Missing data for one table in the most recent backup.
		sqlDB.ExpectErr(
			t, "previous backup does not contain table",
			`BACKUP DATABASE data TO $1 INCREMENTAL FROM $2, $3`,
			LocalFoo, fullBackup, incrementalT3FromT1OneTable,
		)
	})

	sqlDB.Exec(t, `DROP TABLE data.bank`)
	sqlDB.Exec(t, `DROP TABLE data.t2`)
	t.Run("Restore", func(t *testing.T) {
		// Missing the initial full backup.
		sqlDB.ExpectErr(t, "no backup covers time", `RESTORE data.* FROM $1`, incrementalT1FromFull)

		// Missing an intermediate incremental backup.
		sqlDB.ExpectErr(
			t, "no backup covers time",
			`RESTORE data.* FROM $1, $2`, fullBackup, incrementalT2FromT1,
		)

		// Backups specified out of order.
		sqlDB.ExpectErr(
			t, "out of order",
			`RESTORE data.* FROM $1, $2`, incrementalT1FromFull, fullBackup,
		)

		// Missing data for one table in the most recent backup.
		sqlDB.ExpectErr(
			t, "table \"data.t2\" does not exist",
			`RESTORE data.bank, data.t2 FROM $1, $2`, fullBackup, incrementalT3FromT1OneTable,
		)
	})
}

func TestBackupLevelDB(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	_, _, sqlDB, rawDir, cleanupFn := BackupRestoreTestSetup(t, singleNode, 1, InitNone)
	defer cleanupFn()

	_ = sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, LocalFoo)
	// Verify that the sstables are in LevelDB format by checking the trailer
	// magic.
	var magic = []byte("\x57\xfb\x80\x8b\x24\x75\x47\xdb")
	foundSSTs := 0
	if err := filepath.Walk(rawDir, func(path string, info os.FileInfo, err error) error {
		if filepath.Ext(path) == ".sst" {
			foundSSTs++
			data, err := ioutil.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.HasSuffix(data, magic) {
				t.Fatalf("trailer magic is not LevelDB sstable: %s", path)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("%+v", err)
	}
	if foundSSTs == 0 {
		t.Fatal("found no sstables")
	}
}

func setupBackupEncryptedTest(ctx context.Context, t *testing.T, sqlDB *sqlutils.SQLRunner) {
	// Create a table with a name and content that we never see in cleartext in a
	// backup. And while the content and name are user data and metadata, by also
	// partitioning the table at the sentinel value, we can ensure it also appears
	// in the *backup* metadata as well (since partion = range boundary = backup
	// file boundary that is recorded in metadata).
	sqlDB.Exec(t, `CREATE DATABASE neverappears`)
	sqlDB.Exec(t, `CREATE TABLE neverappears.neverappears (
			neverappears STRING PRIMARY KEY, other string, INDEX neverappears (other)
		)  PARTITION BY LIST (neverappears) (
			PARTITION neverappears2 VALUES IN ('neverappears2'), PARTITION default VALUES IN (default)
		)`)

	// Move a partition to n2 to ensure we get multiple writers during BACKUP and
	// by partitioning *at* the sentinel we also ensure it is in a range boundary.
	sqlDB.Exec(t, `ALTER PARTITION neverappears2 OF TABLE neverappears.neverappears
		CONFIGURE ZONE USING constraints='[+dc=dc2]'`)
	testutils.SucceedsSoon(t, func() error {
		_, err := sqlDB.DB.ExecContext(ctx, `ALTER TABLE neverappears.neverappears
			EXPERIMENTAL_RELOCATE VALUES (ARRAY[2], 'neverappears2')`)
		return err
	})

	// Add the actual content with our sentinel too.
	sqlDB.Exec(t, `INSERT INTO neverappears.neverappears values
		('neverappears1', 'neverappears1-v'),
		('neverappears2', 'neverappears2-v'),
		('neverappears3', 'neverappears3-v')`)

	// Let's throw it in some other cluster metadata too for fun.
	sqlDB.Exec(t, `CREATE USER neverappears`)
	sqlDB.Exec(t, `SET CLUSTER SETTING cluster.organization = 'neverappears'`)
	sqlDB.Exec(t, `CREATE STATISTICS foo_stats FROM neverappears.neverappears`)
}

func checkBackupStatsEncrypted(t *testing.T, rawDir string) {
	partitionMatcher := regexp.MustCompile(`BACKUP-STATISTICS`)
	subDir := path.Join(rawDir, "foo")
	err := filepath.Walk(subDir, func(fName string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if partitionMatcher.MatchString(fName) {
			statsBytes, err := ioutil.ReadFile(fName)
			if err != nil {
				return err
			}
			if strings.Contains(fName, "foo/cleartext") {
				assert.False(t, storageccl.AppearsEncrypted(statsBytes))
			} else {
				assert.True(t, storageccl.AppearsEncrypted(statsBytes))
			}
		}
		return nil
	})
	require.NoError(t, err)
}

func checkBackupFilesEncrypted(t *testing.T, rawDir string) {
	checkedFiles := 0
	if err := filepath.Walk(rawDir, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() && !strings.Contains(path, "foo/cleartext") {
			data, err := ioutil.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(data, []byte("neverappears")) {
				t.Errorf("found cleartext occurrence of sentinel string in %s", path)
			}
			checkedFiles++
		}
		return nil
	}); err != nil {
		t.Fatalf("%+v", err)
	}
	if checkedFiles == 0 {
		t.Fatal("test didn't didn't check any files")
	}
}

func getAWSKMSURI(t *testing.T, regionEnvVariable, keyIDEnvVariable string) (string, string) {
	// If environment credentials are not present, we want to
	// skip all AWS KMS tests, including auth-implicit, even though
	// it is not used in auth-implicit.
	_, err := credentials.NewEnvCredentials().Get()
	if err != nil {
		skip.IgnoreLint(t, "Test only works with AWS credentials")
	}

	q := make(url.Values)
	expect := map[string]string{
		"AWS_ACCESS_KEY_ID":     cloudimpl.AWSAccessKeyParam,
		"AWS_SECRET_ACCESS_KEY": cloudimpl.AWSSecretParam,
		regionEnvVariable:       cloudimpl.KMSRegionParam,
	}
	for env, param := range expect {
		v := os.Getenv(env)
		if v == "" {
			skip.IgnoreLintf(t, "%s env var must be set", env)
		}
		q.Add(param, v)
	}

	// Get AWS Key ARN from env variable.
	// TODO(adityamaru): Check if there is a way to specify this in the default
	// role and if we can derive it from there instead?
	keyARN := os.Getenv(keyIDEnvVariable)
	if keyARN == "" {
		skip.IgnoreLint(t, fmt.Sprintf("%s env var must be set", keyIDEnvVariable))
	}

	// Set AUTH to implicit
	q.Set(cloudimpl.AuthParam, cloudimpl.AuthParamSpecified)
	correctURI := fmt.Sprintf("aws:///%s?%s", keyARN, q.Encode())
	incorrectURI := fmt.Sprintf("aws:///%s?%s", "gibberish", q.Encode())

	return correctURI, incorrectURI
}

func TestEncryptedBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	regionEnvVariable := "AWS_KMS_REGION_A"
	keyIDEnvVariable := "AWS_KMS_KEY_ARN_A"

	for _, tc := range []struct {
		name   string
		useKMS bool
	}{
		{
			"encrypted-with-kms",
			true,
		},
		{
			"encrypted-with-passphrase",
			false,
		},
	} {
		var encryptionOption string
		var incorrectEncryptionOption string
		if tc.useKMS {
			correctKMSURI, incorrectKeyARNURI := getAWSKMSURI(t, regionEnvVariable, keyIDEnvVariable)
			encryptionOption = fmt.Sprintf("kms='%s'", correctKMSURI)
			incorrectEncryptionOption = fmt.Sprintf("kms='%s'", incorrectKeyARNURI)
		} else {
			encryptionOption = "encryption_passphrase='abcdefg'"
			incorrectEncryptionOption = "encryption_passphrase='wrongpassphrase'"
		}

		t.Run(tc.name, func(t *testing.T) {
			ctx, _, sqlDB, rawDir, cleanupFn := BackupRestoreTestSetup(t, MultiNode, 3, InitNone)
			defer cleanupFn()

			setupBackupEncryptedTest(ctx, t, sqlDB)

			// Full cluster-backup to capture all possible metadata.
			backupLoc1 := LocalFoo + "/x?COCKROACH_LOCALITY=default"
			backupLoc2 := LocalFoo + "/x2?COCKROACH_LOCALITY=" + url.QueryEscape("dc=dc1")
			backupLoc1inc := LocalFoo + "/inc1/x?COCKROACH_LOCALITY=default"
			backupLoc2inc := LocalFoo + "/inc1/x2?COCKROACH_LOCALITY=" + url.QueryEscape("dc=dc1")

			plainBackupLoc1 := LocalFoo + "/cleartext?COCKROACH_LOCALITY=default"
			plainBackupLoc2 := LocalFoo + "/cleartext?COCKROACH_LOCALITY=" + url.QueryEscape("dc=dc1")

			sqlDB.Exec(t, `BACKUP TO ($1, $2)`, plainBackupLoc1, plainBackupLoc2)

			sqlDB.Exec(t, fmt.Sprintf(`BACKUP TO ($1, $2) WITH %s`, encryptionOption), backupLoc1,
				backupLoc2)
			// Add the actual content with our sentinel too.
			sqlDB.Exec(t, `UPDATE neverappears.neverappears SET other = 'neverappears'`)
			sqlDB.Exec(t, fmt.Sprintf(`BACKUP TO ($1, $2) INCREMENTAL FROM $3 WITH %s`,
				encryptionOption), backupLoc1inc, backupLoc2inc, backupLoc1)

			t.Run("check-stats-encrypted", func(t *testing.T) {
				checkBackupStatsEncrypted(t, rawDir)
			})

			before := sqlDB.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE neverappears.neverappears`)

			checkBackupFilesEncrypted(t, rawDir)

			sqlDB.Exec(t, `DROP DATABASE neverappears CASCADE`)

			sqlDB.Exec(t, fmt.Sprintf(`SHOW BACKUP $1 WITH %s`, encryptionOption), backupLoc1)

			var expectedShowError string
			if tc.useKMS {
				expectedShowError = `one of the provided URIs was not used when encrypting the base BACKUP`
			} else {
				expectedShowError = `failed to decrypt — maybe incorrect key: cipher: message authentication failed`
			}
			sqlDB.ExpectErr(t, expectedShowError,
				fmt.Sprintf(`SHOW BACKUP $1 WITH %s`, incorrectEncryptionOption), backupLoc1)
			sqlDB.ExpectErr(t,
				`file appears encrypted -- try specifying one of "encryption_passphrase" or "kms"`,
				`SHOW BACKUP $1`, backupLoc1)
			sqlDB.ExpectErr(t, `could not find or read encryption information`,
				fmt.Sprintf(`SHOW BACKUP $1 WITH %s`, encryptionOption), plainBackupLoc1)

			sqlDB.Exec(t, fmt.Sprintf(`RESTORE DATABASE neverappears FROM ($1, $2), ($3, $4) WITH %s`,
				encryptionOption), backupLoc1, backupLoc2, backupLoc1inc, backupLoc2inc)

			sqlDB.CheckQueryResults(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE neverappears.neverappears`, before)
		})
	}
}

func concatMultiRegionKMSURIs(uris []string) string {
	var multiRegionKMSURIs string
	for i, uri := range uris {
		if i == 0 {
			multiRegionKMSURIs = "KMS=("
		}

		multiRegionKMSURIs += fmt.Sprintf("'%s'", uri)
		if i != len(uris)-1 {
			multiRegionKMSURIs += ", "
		}
	}
	multiRegionKMSURIs += ")"

	return multiRegionKMSURIs
}

// This test performs an encrypted BACKUP using a set of regional AWS KMSs and
// then attempts to RESTORE the BACKUP using each one of the regional AWS KMSs
// separately.
func TestRegionalKMSEncryptedBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	regionEnvVariables := []string{"AWS_KMS_REGION_A", "AWS_KMS_REGION_B"}
	keyIDEnvVariables := []string{"AWS_KMS_KEY_ARN_A", "AWS_KMS_KEY_ARN_B"}

	var multiRegionKMSURIs []string
	for i := range regionEnvVariables {
		kmsURI, _ := getAWSKMSURI(t, regionEnvVariables[i], keyIDEnvVariables[i])
		multiRegionKMSURIs = append(multiRegionKMSURIs, kmsURI)
	}

	t.Run("multi-region-kms", func(t *testing.T) {
		ctx, _, sqlDB, rawDir, cleanupFn := BackupRestoreTestSetup(t, MultiNode, 3, InitNone)
		defer cleanupFn()

		setupBackupEncryptedTest(ctx, t, sqlDB)

		// Full cluster-backup to capture all possible metadata.
		backupLoc1 := LocalFoo + "/x?COCKROACH_LOCALITY=default"
		backupLoc2 := LocalFoo + "/x2?COCKROACH_LOCALITY=" + url.QueryEscape("dc=dc1")

		sqlDB.Exec(t, fmt.Sprintf(`BACKUP TO ($1, $2) WITH %s`,
			concatMultiRegionKMSURIs(multiRegionKMSURIs)), backupLoc1,
			backupLoc2)

		t.Run("check-stats-encrypted", func(t *testing.T) {
			checkBackupStatsEncrypted(t, rawDir)
		})

		before := sqlDB.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE neverappears.neverappears`)

		checkBackupFilesEncrypted(t, rawDir)

		sqlDB.Exec(t, fmt.Sprintf(`SHOW BACKUP $1 WITH KMS='%s'`, multiRegionKMSURIs[0]),
			backupLoc1)

		// Attempt to RESTORE using each of the regional KMSs independently.
		for _, uri := range multiRegionKMSURIs {
			sqlDB.Exec(t, `DROP DATABASE neverappears CASCADE`)

			sqlDB.Exec(t, fmt.Sprintf(`RESTORE DATABASE neverappears FROM ($1, $2) WITH %s`,
				concatMultiRegionKMSURIs([]string{uri})), backupLoc1, backupLoc2)

			sqlDB.CheckQueryResults(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE neverappears.neverappears`, before)
		}
	})
}

type testKMSEnv struct {
	settings         *cluster.Settings
	externalIOConfig *base.ExternalIODirConfig
}

var _ cloud.KMSEnv = &testKMSEnv{}

func (e *testKMSEnv) ClusterSettings() *cluster.Settings {
	return e.settings
}

func (e *testKMSEnv) KMSConfig() *base.ExternalIODirConfig {
	return e.externalIOConfig
}

type testKMS struct {
	uri string
}

var _ cloud.KMS = &testKMS{}

func (k *testKMS) MasterKeyID() (string, error) {
	kmsURL, err := url.ParseRequestURI(k.uri)
	if err != nil {
		return "", err
	}

	return strings.TrimPrefix(kmsURL.Path, "/"), nil
}

// Encrypt appends the KMS URI master key ID to data.
func (k *testKMS) Encrypt(ctx context.Context, data []byte) ([]byte, error) {
	kmsURL, err := url.ParseRequestURI(k.uri)
	if err != nil {
		return nil, err
	}
	return []byte(string(data) + strings.TrimPrefix(kmsURL.Path, "/")), nil
}

// Decrypt strips the KMS URI master key ID from data.
func (k *testKMS) Decrypt(ctx context.Context, data []byte) ([]byte, error) {
	kmsURL, err := url.ParseRequestURI(k.uri)
	if err != nil {
		return nil, err
	}
	return []byte(strings.TrimSuffix(string(data), strings.TrimPrefix(kmsURL.Path, "/"))), nil
}

func (k *testKMS) Close() error {
	return nil
}

func MakeTestKMS(uri string, _ cloud.KMSEnv) (cloud.KMS, error) {
	return &testKMS{uri}, nil
}

func constructMockKMSURIsWithKeyID(keyIDs []string) []string {
	q := make(url.Values)
	q.Add(cloudimpl.AuthParam, cloudimpl.AuthParamImplicit)
	q.Add(cloudimpl.KMSRegionParam, "blah")

	var uris []string
	for _, keyID := range keyIDs {
		uris = append(uris, fmt.Sprintf("testkms:///%s?%s", keyID, q.Encode()))
	}

	return uris
}

// TestValidateKMSURIsAgainstFullBackup tests validateKMSURIsAgainstFullBackup()
// which ensures that the KMS URIs provided to an incremental BACKUP are a
// subset of those used during the full BACKUP.
func TestValidateKMSURIsAgainstFullBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		name                  string
		fullBackupURIs        []string
		incrementalBackupURIs []string
		expectError           bool
	}{
		{
			name:                  "inc-full-matching-set",
			fullBackupURIs:        constructMockKMSURIsWithKeyID([]string{"abc", "def"}),
			incrementalBackupURIs: constructMockKMSURIsWithKeyID([]string{"def", "abc"}),
			expectError:           false,
		},
		{
			name:                  "inc-subset-of-full",
			fullBackupURIs:        constructMockKMSURIsWithKeyID([]string{"abc", "def"}),
			incrementalBackupURIs: constructMockKMSURIsWithKeyID([]string{"abc"}),
			expectError:           false,
		},
		{
			name:                  "inc-expands-set-of-full",
			fullBackupURIs:        constructMockKMSURIsWithKeyID([]string{"abc", "def"}),
			incrementalBackupURIs: constructMockKMSURIsWithKeyID([]string{"abc", "def", "ghi"}),
			expectError:           true,
		},
		{
			name:                  "inc-has-mismatch",
			fullBackupURIs:        constructMockKMSURIsWithKeyID([]string{"abc", "def"}),
			incrementalBackupURIs: constructMockKMSURIsWithKeyID([]string{"abc", "ghi"}),
			expectError:           true,
		},
	} {
		masterKeyIDToDataKey := newEncryptedDataKeyMap()

		var defaultEncryptedDataKey []byte
		for _, uri := range tc.fullBackupURIs {
			url, err := url.ParseRequestURI(uri)
			require.NoError(t, err)
			keyID := strings.TrimPrefix(url.Path, "/")

			masterKeyIDToDataKey.addEncryptedDataKey(plaintextMasterKeyID(keyID),
				[]byte("efgh-"+tc.name))

			if defaultEncryptedDataKey == nil {
				defaultEncryptedDataKey = []byte("efgh-" + tc.name)
			}
		}

		kmsInfo, err := validateKMSURIsAgainstFullBackup(
			tc.incrementalBackupURIs, masterKeyIDToDataKey,
			&testKMSEnv{cluster.NoSettings, &base.ExternalIODirConfig{}})
		if tc.expectError {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, tc.incrementalBackupURIs[0], kmsInfo.Uri)
			require.True(t, bytes.Equal(defaultEncryptedDataKey, kmsInfo.EncryptedDataKey))
		}
	}
}

// TestGetEncryptedDataKeyByKMSMasterKeyID tests
// getEncryptedDataKeyByKMSMasterKeyID() which constructs a mapping
// {MasterKeyID : EncryptedDataKey} for each KMS URI.
// It also returns the default KMSInfo to be used for encryption/decryption
// thereafter, which defaults to the first URI.
func TestGetEncryptedDataKeyByKMSMasterKeyID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	plaintextDataKey := []byte("supersecret")
	for _, tc := range []struct {
		name           string
		fullBackupURIs []string
	}{
		{
			name:           "single-uri",
			fullBackupURIs: constructMockKMSURIsWithKeyID([]string{"abc"}),
		},
		{
			name:           "multiple-unique-uris",
			fullBackupURIs: constructMockKMSURIsWithKeyID([]string{"abc", "def"}),
		},
	} {
		expectedMap := newEncryptedDataKeyMap()
		var defaultKMSInfo *jobspb.BackupEncryptionOptions_KMSInfo
		for _, uri := range tc.fullBackupURIs {
			testKMS, err := MakeTestKMS(uri, nil)
			require.NoError(t, err)

			masterKeyID, err := testKMS.MasterKeyID()
			require.NoError(t, err)

			encryptedDataKey, err := testKMS.Encrypt(ctx, plaintextDataKey)
			require.NoError(t, err)

			if defaultKMSInfo == nil {
				defaultKMSInfo = &jobspb.BackupEncryptionOptions_KMSInfo{
					Uri:              uri,
					EncryptedDataKey: encryptedDataKey,
				}
			}

			expectedMap.addEncryptedDataKey(plaintextMasterKeyID(masterKeyID), encryptedDataKey)
		}

		gotMap, gotDefaultKMSInfo, err := getEncryptedDataKeyByKMSMasterKeyID(ctx, tc.fullBackupURIs,
			plaintextDataKey, nil)
		require.NoError(t, err)
		require.Equal(t, *expectedMap, *gotMap)
		require.Equal(t, defaultKMSInfo.Uri, gotDefaultKMSInfo.Uri)
		require.True(t, bytes.Equal(defaultKMSInfo.EncryptedDataKey,
			gotDefaultKMSInfo.EncryptedDataKey))
	}
}

func TestRestoredPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, _, sqlDB, dir, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()
	args := base.TestServerArgs{ExternalIODir: dir}

	rootOnly := sqlDB.QueryStr(t, `SHOW GRANTS ON data.bank`)

	sqlDB.Exec(t, `CREATE USER someone`)
	sqlDB.Exec(t, `GRANT SELECT, INSERT, UPDATE, DELETE ON data.bank TO someone`)

	sqlDB.Exec(t, `CREATE DATABASE data2`)
	// Explicitly don't restore grants when just restoring a database since we
	// cannot ensure that the same users exist in the restoring cluster.
	data2Grants := sqlDB.QueryStr(t, `SHOW GRANTS ON DATABASE data2`)
	sqlDB.Exec(t, `GRANT SELECT, INSERT, UPDATE, DELETE ON DATABASE data2 TO someone`)

	withGrants := sqlDB.QueryStr(t, `SHOW GRANTS ON data.bank`)

	sqlDB.Exec(t, `BACKUP DATABASE data, data2 TO $1`, LocalFoo)
	sqlDB.Exec(t, `DROP TABLE data.bank`)

	t.Run("into fresh db", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.Background())
		sqlDBRestore := sqlutils.MakeSQLRunner(tc.Conns[0])
		sqlDBRestore.Exec(t, `CREATE DATABASE data`)
		sqlDBRestore.Exec(t, `RESTORE data.bank FROM $1`, LocalFoo)
		sqlDBRestore.CheckQueryResults(t, `SHOW GRANTS ON data.bank`, rootOnly)
	})

	t.Run("into db with added grants", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.Background())
		sqlDBRestore := sqlutils.MakeSQLRunner(tc.Conns[0])
		sqlDBRestore.Exec(t, `CREATE DATABASE data`)
		sqlDBRestore.Exec(t, `CREATE USER someone`)
		sqlDBRestore.Exec(t, `GRANT SELECT, INSERT, UPDATE, DELETE ON DATABASE data TO someone`)
		sqlDBRestore.Exec(t, `RESTORE data.bank FROM $1`, LocalFoo)
		sqlDBRestore.CheckQueryResults(t, `SHOW GRANTS ON data.bank`, withGrants)
	})

	t.Run("into db on db grants", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.Background())
		sqlDBRestore := sqlutils.MakeSQLRunner(tc.Conns[0])
		sqlDBRestore.Exec(t, `CREATE USER someone`)
		sqlDBRestore.Exec(t, `RESTORE DATABASE data2 FROM $1`, LocalFoo)
		sqlDBRestore.CheckQueryResults(t, `SHOW GRANTS ON DATABASE data2`, data2Grants)
	})
}

func TestRestoreInto(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, LocalFoo)

	restoreStmt := fmt.Sprintf(`RESTORE data.bank FROM '%s' WITH into_db = 'data 2'`, LocalFoo)

	sqlDB.ExpectErr(t, "a database named \"data 2\" needs to exist", restoreStmt)

	sqlDB.Exec(t, `CREATE DATABASE "data 2"`)
	sqlDB.Exec(t, restoreStmt)

	expected := sqlDB.QueryStr(t, `SELECT * FROM data.bank`)
	sqlDB.CheckQueryResults(t, `SELECT * FROM "data 2".bank`, expected)
}

func TestBackupRestorePermissions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, tc, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	sqlDB.Exec(t, `CREATE USER testuser`)
	pgURL, cleanupFunc := sqlutils.PGUrl(
		t, tc.Server(0).ServingSQLAddr(), "TestBackupRestorePermissions-testuser", url.User("testuser"),
	)
	defer cleanupFunc()
	testuser, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer testuser.Close()

	backupStmt := fmt.Sprintf(`BACKUP DATABASE data TO '%s'`, LocalFoo)

	t.Run("root-only", func(t *testing.T) {
		if _, err := testuser.Exec(backupStmt); !testutils.IsError(
			err, "only users with the admin role are allowed to BACKUP",
		) {
			t.Fatal(err)
		}
	})

	t.Run("privs-required", func(t *testing.T) {
		sqlDB.Exec(t, backupStmt)
		// Root doesn't have CREATE on `system` DB, so that should fail. Still need
		// a valid `dir` though, since descriptors are always loaded first.
		sqlDB.ExpectErr(
			t, "user root does not have CREATE privilege",
			`RESTORE data.bank FROM $1 WITH OPTIONS (into_db='system')`, LocalFoo,
		)
	})

	// Ensure that non-root users with the admin role can backup and restore.
	t.Run("non-root-admin", func(t *testing.T) {
		sqlDB.Exec(t, "GRANT admin TO testuser")

		t.Run("backup-table", func(t *testing.T) {
			testLocalFoo := fmt.Sprintf("nodelocal://0/%s", t.Name())
			testLocalBackupStmt := fmt.Sprintf(`BACKUP data.bank TO '%s'`, testLocalFoo)
			if _, err := testuser.Exec(testLocalBackupStmt); err != nil {
				t.Fatal(err)
			}
			sqlDB.Exec(t, `CREATE DATABASE data2`)
			if _, err := testuser.Exec(`RESTORE data.bank FROM $1 WITH OPTIONS (into_db='data2')`, testLocalFoo); err != nil {
				t.Fatal(err)
			}
		})

		t.Run("backup-database", func(t *testing.T) {
			testLocalFoo := fmt.Sprintf("nodelocal://0/%s", t.Name())
			testLocalBackupStmt := fmt.Sprintf(`BACKUP DATABASE data TO '%s'`, testLocalFoo)
			if _, err := testuser.Exec(testLocalBackupStmt); err != nil {
				t.Fatal(err)
			}
			sqlDB.Exec(t, "DROP DATABASE data")
			if _, err := testuser.Exec(`RESTORE DATABASE data FROM $1`, testLocalFoo); err != nil {
				t.Fatal(err)
			}
		})
	})
}

func TestRestoreDatabaseVersusTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, tc, origDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()
	args := base.TestServerArgs{ExternalIODir: tc.Servers[0].ClusterSettings().ExternalIODir}

	for _, q := range []string{
		`CREATE DATABASE d2`,
		`CREATE DATABASE d3`,
		`CREATE TABLE d3.foo (a INT)`,
		`CREATE DATABASE d4`,
		`CREATE TABLE d4.foo (a INT)`,
		`CREATE TABLE d4.bar (a INT)`,
	} {
		origDB.Exec(t, q)
	}

	d4foo := "nodelocal://0/d4foo"
	d4foobar := "nodelocal://0/d4foobar"
	d4star := "nodelocal://0/d4star"

	origDB.Exec(t, `BACKUP DATABASE data, d2, d3, d4 TO $1`, LocalFoo)
	origDB.Exec(t, `BACKUP d4.foo TO $1`, d4foo)
	origDB.Exec(t, `BACKUP d4.foo, d4.bar TO $1`, d4foobar)
	origDB.Exec(t, `BACKUP d4.* TO $1`, d4star)

	t.Run("incomplete-db", func(t *testing.T) {
		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tcRestore.Stopper().Stop(context.Background())
		sqlDB := sqlutils.MakeSQLRunner(tcRestore.Conns[0])

		sqlDB.Exec(t, `create database d5`)

		sqlDB.ExpectErr(
			t, "cannot RESTORE DATABASE from a backup of individual tables",
			`RESTORE database d4 FROM $1`, d4foo,
		)

		sqlDB.ExpectErr(
			t, "cannot RESTORE <database>.* from a backup of individual tables",
			`RESTORE d4.* FROM $1 WITH into_db = 'd5'`, d4foo,
		)

		sqlDB.ExpectErr(
			t, "cannot RESTORE DATABASE from a backup of individual tables",
			`RESTORE database d4 FROM $1`, d4foobar,
		)

		sqlDB.ExpectErr(
			t, "cannot RESTORE <database>.* from a backup of individual tables",
			`RESTORE d4.* FROM $1 WITH into_db = 'd5'`, d4foobar,
		)

		sqlDB.ExpectErr(
			t, "cannot RESTORE DATABASE from a backup of individual tables",
			`RESTORE database d4 FROM $1`, d4foo,
		)

		sqlDB.Exec(t, `RESTORE database d4 FROM $1`, d4star)

	})

	t.Run("db", func(t *testing.T) {
		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tcRestore.Stopper().Stop(context.Background())
		sqlDB := sqlutils.MakeSQLRunner(tcRestore.Conns[0])
		sqlDB.Exec(t, `RESTORE DATABASE data, d2, d3 FROM $1`, LocalFoo)
	})

	t.Run("db-exists", func(t *testing.T) {
		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tcRestore.Stopper().Stop(context.Background())
		sqlDB := sqlutils.MakeSQLRunner(tcRestore.Conns[0])

		sqlDB.Exec(t, `CREATE DATABASE data`)
		sqlDB.ExpectErr(t, "already exists", `RESTORE DATABASE data FROM $1`, LocalFoo)
	})

	t.Run("tables", func(t *testing.T) {
		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tcRestore.Stopper().Stop(context.Background())
		sqlDB := sqlutils.MakeSQLRunner(tcRestore.Conns[0])

		sqlDB.Exec(t, `CREATE DATABASE data`)
		sqlDB.Exec(t, `RESTORE data.* FROM $1`, LocalFoo)
	})

	t.Run("tables-needs-db", func(t *testing.T) {
		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tcRestore.Stopper().Stop(context.Background())
		sqlDB := sqlutils.MakeSQLRunner(tcRestore.Conns[0])

		sqlDB.ExpectErr(t, "needs to exist", `RESTORE data.*, d4.* FROM $1`, LocalFoo)
	})

	t.Run("into_db", func(t *testing.T) {
		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tcRestore.Stopper().Stop(context.Background())
		sqlDB := sqlutils.MakeSQLRunner(tcRestore.Conns[0])

		sqlDB.ExpectErr(
			t, `cannot use "into_db"`,
			`RESTORE DATABASE data FROM $1 WITH into_db = 'other'`, LocalFoo,
		)
	})
}

func TestBackupAzureAccountName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	values := url.Values{}
	values.Set("AZURE_ACCOUNT_KEY", "password")
	values.Set("AZURE_ACCOUNT_NAME", "\n")

	url := &url.URL{
		Scheme:   "azure",
		Host:     "host",
		Path:     "/backup",
		RawQuery: values.Encode(),
	}

	// Verify newlines in the account name cause an error.
	sqlDB.ExpectErr(t, "azure: account name is not valid", `backup database data to $1`, url.String())
}

// If an operator issues a bad query or if a deploy contains a bug that corrupts
// data, it should be possible to return to a previous point in time before the
// badness. For cases when the last good timestamp is within the gc threshold,
// see the subtests for two ways this can work.
func TestPointInTimeRecovery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1000
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	fullBackupDir := LocalFoo + "/full"
	sqlDB.Exec(t, `BACKUP data.* TO $1`, fullBackupDir)

	sqlDB.Exec(t, `UPDATE data.bank SET balance = 2`)

	incBackupDir := LocalFoo + "/inc"
	sqlDB.Exec(t, `BACKUP data.* TO $1 INCREMENTAL FROM $2`, incBackupDir, fullBackupDir)

	var beforeBadThingTs string
	sqlDB.Exec(t, `UPDATE data.bank SET balance = 3`)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&beforeBadThingTs)

	// Something bad happens.
	sqlDB.Exec(t, `UPDATE data.bank SET balance = 4`)

	beforeBadThingData := sqlDB.QueryStr(t,
		fmt.Sprintf(`SELECT * FROM data.bank AS OF SYSTEM TIME '%s' ORDER BY id`, beforeBadThingTs),
	)

	// If no previous BACKUPs have been taken, a new one can be taken using `AS
	// OF SYSTEM TIME` with a timestamp before the badness started. This can
	// then be RESTORE'd into a temporary database. The operator can manually
	// reconcile the current data with the restored data before finally
	// RENAME-ing the table into the final location.
	t.Run("recovery=new-backup", func(t *testing.T) {
		sqlDB = sqlutils.MakeSQLRunner(sqlDB.DB)
		recoveryDir := LocalFoo + "/new-backup"
		sqlDB.Exec(t,
			fmt.Sprintf(`BACKUP data.* TO $1 AS OF SYSTEM TIME '%s'`, beforeBadThingTs),
			recoveryDir,
		)
		sqlDB.Exec(t, `CREATE DATABASE newbackup`)
		sqlDB.Exec(t, `RESTORE data.* FROM $1 WITH into_db=newbackup`, recoveryDir)

		// Some manual reconciliation of the data in data.bank and
		// newbackup.bank could be done here by the operator.

		sqlDB.Exec(t, `DROP TABLE data.bank`)
		sqlDB.Exec(t, `ALTER TABLE newbackup.bank RENAME TO data.bank`)
		sqlDB.Exec(t, `DROP DATABASE newbackup`)
		sqlDB.CheckQueryResults(t, `SELECT * FROM data.bank ORDER BY id`, beforeBadThingData)
	})

	// If there is a recent BACKUP (either full or incremental), then it will
	// likely be faster to make a BACKUP that is incremental from it and RESTORE
	// using that. Everything else works the same as above.
	t.Run("recovery=inc-backup", func(t *testing.T) {
		sqlDB = sqlutils.MakeSQLRunner(sqlDB.DB)
		recoveryDir := LocalFoo + "/inc-backup"
		sqlDB.Exec(t,
			fmt.Sprintf(`BACKUP data.* TO $1 AS OF SYSTEM TIME '%s' INCREMENTAL FROM $2, $3`, beforeBadThingTs),
			recoveryDir, fullBackupDir, incBackupDir,
		)
		sqlDB.Exec(t, `CREATE DATABASE incbackup`)
		sqlDB.Exec(t,
			`RESTORE data.* FROM $1, $2, $3 WITH into_db=incbackup`,
			fullBackupDir, incBackupDir, recoveryDir,
		)

		// Some manual reconciliation of the data in data.bank and
		// incbackup.bank could be done here by the operator.

		sqlDB.Exec(t, `DROP TABLE data.bank`)
		sqlDB.Exec(t, `ALTER TABLE incbackup.bank RENAME TO data.bank`)
		sqlDB.Exec(t, `DROP DATABASE incbackup`)
		sqlDB.CheckQueryResults(t, `SELECT * FROM data.bank ORDER BY id`, beforeBadThingData)
	})
}

func TestBackupRestoreDropDB(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	sqlDB.Exec(t, `DROP DATABASE data`)
	sqlDB.Exec(t, `CREATE DATABASE data`)
	sqlDB.Exec(t, `CREATE TABLE data.bank (i int)`)
	sqlDB.Exec(t, `INSERT INTO data.bank VALUES (1)`)

	sqlDB.Exec(t, "BACKUP DATABASE data TO $1", LocalFoo)
	sqlDB.Exec(t, "CREATE DATABASE data2")
	sqlDB.Exec(t, "RESTORE data.* FROM $1 WITH OPTIONS (into_db='data2')", LocalFoo)

	expected := sqlDB.QueryStr(t, `SELECT * FROM data.bank`)
	sqlDB.CheckQueryResults(t, `SELECT * FROM data2.bank`, expected)
}

func TestBackupRestoreDropTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	sqlDB.Exec(t, `DROP TABLE data.bank`)
	sqlDB.Exec(t, `
		CREATE TABLE data.bank (i int);
		INSERT INTO data.bank VALUES (1);
	`)

	sqlDB.Exec(t, "BACKUP DATABASE data TO $1", LocalFoo)
	sqlDB.Exec(t, "CREATE DATABASE data2")
	sqlDB.Exec(t, "RESTORE data.* FROM $1 WITH OPTIONS (into_db='data2')", LocalFoo)

	expected := sqlDB.QueryStr(t, `SELECT * FROM data.bank`)
	sqlDB.CheckQueryResults(t, `SELECT * FROM data2.bank`, expected)
}

func TestBackupRestoreIncrementalAddTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()
	sqlDB.Exec(t, `CREATE DATABASE data2`)
	sqlDB.Exec(t, `CREATE TABLE data.t (s string PRIMARY KEY)`)
	full, inc := LocalFoo+"/full", LocalFoo+"/inc"

	sqlDB.Exec(t, `INSERT INTO data.t VALUES ('before')`)
	sqlDB.Exec(t, `BACKUP data.*, data2.* TO $1`, full)
	sqlDB.Exec(t, `UPDATE data.t SET s = 'after'`)

	sqlDB.Exec(t, `CREATE TABLE data2.t2 (i int)`)
	sqlDB.Exec(t, "BACKUP data.*, data2.* TO $1 INCREMENTAL FROM $2", inc, full)
}

func TestBackupRestoreIncrementalAddTableMissing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()
	sqlDB.Exec(t, `CREATE DATABASE data2`)
	sqlDB.Exec(t, `CREATE TABLE data.t (s string PRIMARY KEY)`)
	full, inc := LocalFoo+"/full", LocalFoo+"/inc"

	sqlDB.Exec(t, `INSERT INTO data.t VALUES ('before')`)
	sqlDB.Exec(t, `BACKUP data.* TO $1`, full)
	sqlDB.Exec(t, `UPDATE data.t SET s = 'after'`)

	sqlDB.Exec(t, `CREATE TABLE data2.t2 (i int)`)
	sqlDB.ExpectErr(
		t, "previous backup does not contain table",
		"BACKUP data.*, data2.* TO $1 INCREMENTAL FROM $2", inc, full,
	)
}

func TestBackupRestoreIncrementalTrucateTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()
	sqlDB.Exec(t, `CREATE TABLE data.t (s string PRIMARY KEY)`)
	full, inc := LocalFoo+"/full", LocalFoo+"/inc"

	sqlDB.Exec(t, `INSERT INTO data.t VALUES ('before')`)
	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, full)
	sqlDB.Exec(t, `UPDATE data.t SET s = 'after'`)
	sqlDB.Exec(t, `TRUNCATE data.t`)

	sqlDB.Exec(t, "BACKUP DATABASE data TO $1 INCREMENTAL FROM $2", inc, full)
}

func TestBackupRestoreIncrementalDropTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()
	sqlDB.Exec(t, `CREATE TABLE data.t (s string PRIMARY KEY)`)
	full, inc := LocalFoo+"/full", LocalFoo+"/inc"

	sqlDB.Exec(t, `INSERT INTO data.t VALUES ('before')`)
	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, full)
	sqlDB.Exec(t, `UPDATE data.t SET s = 'after'`)
	sqlDB.Exec(t, `DROP TABLE data.t`)

	sqlDB.Exec(t, "BACKUP DATABASE data TO $1 INCREMENTAL FROM $2", inc, full)
	sqlDB.Exec(t, `DROP DATABASE data`)

	// Restoring to backup before DROP restores t.
	sqlDB.Exec(t, `RESTORE DATABASE data FROM $1`, full)
	sqlDB.Exec(t, `SELECT 1 FROM data.t LIMIT 0`)
	sqlDB.Exec(t, `DROP DATABASE data`)

	// Restoring to backup after DROP does not restore t.
	sqlDB.Exec(t, `RESTORE DATABASE data FROM $1, $2`, full, inc)
	sqlDB.ExpectErr(t, "relation \"data.t\" does not exist", `SELECT 1 FROM data.t LIMIT 0`)
}

func TestFileIOLimits(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 11
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	elsewhere := "nodelocal://0/../../blah"

	sqlDB.Exec(t, `BACKUP data.bank TO $1`, LocalFoo)
	sqlDB.ExpectErr(
		t, "local file access to paths outside of external-io-dir is not allowed",
		`BACKUP data.bank TO $1`, elsewhere,
	)

	sqlDB.Exec(t, `DROP TABLE data.bank`)

	sqlDB.Exec(t, `RESTORE data.bank FROM $1`, LocalFoo)
	sqlDB.ExpectErr(
		t, "local file access to paths outside of external-io-dir is not allowed",
		`RESTORE data.bank FROM $1`, elsewhere,
	)
}

func waitForSuccessfulJob(t *testing.T, tc *testcluster.TestCluster, id int64) {
	// Force newly created job to be adopted and verify it succeeds.
	tc.Server(0).JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
	testutils.SucceedsSoon(t, func() error {
		var unused int64
		return tc.ServerConn(0).QueryRow(
			"SELECT job_id FROM [SHOW JOBS] WHERE job_id = $1 AND status = $2",
			id, jobs.StatusSucceeded).Scan(&unused)
	})
}

func TestDetachedBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, tc, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	db := sqlDB.DB.(*gosql.DB)

	// running backup under transaction requires DETACHED.
	var jobID int64
	tx, err := db.Begin()
	require.NoError(t, err)
	err = tx.QueryRow(`BACKUP DATABASE data TO $1`, LocalFoo).Scan(&jobID)
	require.True(t, testutils.IsError(err,
		"BACKUP cannot be used inside a transaction without DETACHED option"))
	require.NoError(t, tx.Rollback())

	// Okay to run DETACHED backup, even w/out explicit transaction.
	sqlDB.QueryRow(t, `BACKUP DATABASE data TO $1 WITH DETACHED`, LocalFoo).Scan(&jobID)
	waitForSuccessfulJob(t, tc, jobID)

	// Backup again, under explicit transaction.
	tx, err = db.Begin()
	require.NoError(t, err)
	err = tx.QueryRow(`BACKUP DATABASE data TO $1 WITH DETACHED`, LocalFoo+"/1").Scan(&jobID)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	waitForSuccessfulJob(t, tc, jobID)

	// Backup again under transaction, but this time abort the transaction.
	// No new jobs should have been created.
	allJobsQuery := "SELECT job_id FROM [SHOW JOBS]"
	allJobs := sqlDB.QueryStr(t, allJobsQuery)
	tx, err = db.Begin()
	require.NoError(t, err)
	err = tx.QueryRow(`BACKUP DATABASE data TO $1 WITH DETACHED`, LocalFoo+"/2").Scan(&jobID)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	sqlDB.CheckQueryResults(t, allJobsQuery, allJobs)

	// Ensure that we can backup again to the same location as the backup that was
	// rolledback.
	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, LocalFoo+"/2")
}

func TestDetachedRestore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, tc, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	db := sqlDB.DB.(*gosql.DB)

	// Run a BACKUP.
	sqlDB.Exec(t, `CREATE TABLE data.t (id INT, name STRING)`)
	sqlDB.Exec(t, `INSERT INTO data.t VALUES (1, 'foo'), (2, 'bar')`)
	sqlDB.Exec(t, `BACKUP TABLE data.t TO $1`, LocalFoo)
	sqlDB.Exec(t, `CREATE DATABASE test`)

	// Running RESTORE under transaction requires DETACHED.
	var jobID int64
	tx, err := db.Begin()
	require.NoError(t, err)
	err = tx.QueryRow(`RESTORE TABLE t FROM $1 WITH INTO_DB=test`, LocalFoo).Scan(&jobID)
	require.True(t, testutils.IsError(err,
		"RESTORE cannot be used inside a transaction without DETACHED option"))
	require.NoError(t, tx.Rollback())

	// Okay to run DETACHED RESTORE, even w/out explicit transaction.
	sqlDB.QueryRow(t, `RESTORE TABLE t FROM $1 WITH DETACHED, INTO_DB=test`,
		LocalFoo).Scan(&jobID)
	waitForSuccessfulJob(t, tc, jobID)
	sqlDB.Exec(t, `DROP TABLE test.t`)

	// RESTORE again, under explicit transaction.
	tx, err = db.Begin()
	require.NoError(t, err)
	err = tx.QueryRow(`RESTORE TABLE t FROM $1 WITH DETACHED, INTO_DB=test`, LocalFoo).Scan(&jobID)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	waitForSuccessfulJob(t, tc, jobID)
	sqlDB.Exec(t, `DROP TABLE test.t`)

	// RESTORE again under transaction, but this time abort the transaction.
	// No new jobs should have been created.
	allJobsQuery := "SELECT job_id FROM [SHOW JOBS]"
	allJobs := sqlDB.QueryStr(t, allJobsQuery)
	tx, err = db.Begin()
	require.NoError(t, err)
	err = tx.QueryRow(`RESTORE TABLE t FROM $1 WITH DETACHED, INTO_DB=test`, LocalFoo).Scan(&jobID)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	sqlDB.CheckQueryResults(t, allJobsQuery, allJobs)
}

func TestBackupRestoreSequence(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const numAccounts = 1
	_, _, origDB, dir, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()
	args := base.TestServerArgs{ExternalIODir: dir}

	backupLoc := LocalFoo

	origDB.Exec(t, `CREATE SEQUENCE data.t_id_seq`)
	origDB.Exec(t, `CREATE TABLE data.t (id INT PRIMARY KEY DEFAULT nextval('data.t_id_seq'), v text)`)
	origDB.Exec(t, `INSERT INTO data.t (v) VALUES ('foo'), ('bar'), ('baz')`)

	origDB.Exec(t, `BACKUP DATABASE data TO $1`, backupLoc)

	t.Run("restore both table & sequence to a new cluster", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.Background())
		newDB := sqlutils.MakeSQLRunner(tc.Conns[0])

		newDB.Exec(t, `RESTORE DATABASE data FROM $1`, backupLoc)
		newDB.Exec(t, `USE data`)

		// Verify that the db was restored correctly.
		newDB.CheckQueryResults(t, `SELECT * FROM t`, [][]string{
			{"1", "foo"},
			{"2", "bar"},
			{"3", "baz"},
		})
		newDB.CheckQueryResults(t, `SELECT last_value FROM t_id_seq`, [][]string{
			{"3"},
		})

		// Verify that we can kkeep inserting into the table, without violating a uniqueness constraint.
		newDB.Exec(t, `INSERT INTO data.t (v) VALUES ('bar')`)

		// Verify that sequence <=> table dependencies are still in place.
		newDB.ExpectErr(
			t, "pq: cannot drop sequence t_id_seq because other objects depend on it",
			`DROP SEQUENCE t_id_seq`,
		)
	})

	t.Run("restore just the table to a new cluster", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.Background())
		newDB := sqlutils.MakeSQLRunner(tc.Conns[0])

		newDB.Exec(t, `CREATE DATABASE data`)
		newDB.Exec(t, `USE data`)

		newDB.ExpectErr(
			t, "pq: cannot restore table \"t\" without referenced sequence 54 \\(or \"skip_missing_sequences\" option\\)",
			`RESTORE TABLE t FROM $1`, LocalFoo,
		)

		newDB.Exec(t, `RESTORE TABLE t FROM $1 WITH OPTIONS (skip_missing_sequences)`, LocalFoo)

		// Verify that the table was restored correctly.
		newDB.CheckQueryResults(t, `SELECT * FROM data.t`, [][]string{
			{"1", "foo"},
			{"2", "bar"},
			{"3", "baz"},
		})

		// Test that insertion without specifying the id column doesn't work, since
		// the DEFAULT expression has been removed.
		newDB.ExpectErr(
			t, `pq: missing \"id\" primary key column`,
			`INSERT INTO t (v) VALUES ('bloop')`,
		)

		// Test that inserting with a value specified works.
		newDB.Exec(t, `INSERT INTO t (id, v) VALUES (4, 'bloop')`)
	})

	t.Run("restore just the sequence to a new cluster", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.Background())
		newDB := sqlutils.MakeSQLRunner(tc.Conns[0])

		newDB.Exec(t, `CREATE DATABASE data`)
		newDB.Exec(t, `USE data`)
		// TODO(vilterp): create `RESTORE SEQUENCE` instead of `RESTORE TABLE`, and force
		// people to use that?
		newDB.Exec(t, `RESTORE TABLE t_id_seq FROM $1`, backupLoc)

		// Verify that the sequence value was restored.
		newDB.CheckQueryResults(t, `SELECT last_value FROM data.t_id_seq`, [][]string{
			{"3"},
		})

		// Verify that the reference to the table that used it was removed, and
		// it can be dropped.
		newDB.Exec(t, `DROP SEQUENCE t_id_seq`)
	})
}

func TestBackupRestoreSequenceOwnership(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, _, origDB, dir, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()
	args := base.TestServerArgs{ExternalIODir: dir}

	// Setup for sequence ownership backup/restore tests in the same database.
	backupLoc := LocalFoo + `/d`
	origDB.Exec(t, "SET CLUSTER SETTING sql.cross_db_sequence_owners.enabled = TRUE")
	origDB.Exec(t, `CREATE DATABASE d`)
	origDB.Exec(t, `CREATE TABLE d.t(a int)`)
	origDB.Exec(t, `CREATE SEQUENCE d.seq OWNED BY d.t.a`)
	origDB.Exec(t, `BACKUP DATABASE d TO $1`, backupLoc)

	// When restoring a database which has a owning table and an owned sequence,
	// the ownership relationship should be preserved and remapped post restore.
	t.Run("test restoring database should preserve ownership dependency", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.Background())

		newDB := sqlutils.MakeSQLRunner(tc.Conns[0])
		kvDB := tc.Server(0).DB()

		newDB.Exec(t, `RESTORE DATABASE d FROM $1`, backupLoc)

		tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "d", "t")
		seqDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "d", "seq")

		require.True(t, seqDesc.SequenceOpts.HasOwner(), "no sequence owner after restore")
		require.Equal(t, tableDesc.ID, seqDesc.SequenceOpts.SequenceOwner.OwnerTableID,
			"unexpected table is sequence owner after restore",
		)
		require.Equal(t, tableDesc.GetColumns()[0].ID, seqDesc.SequenceOpts.SequenceOwner.OwnerColumnID,
			"unexpected column is sequence owner after restore",
		)
		require.Equal(t, 1, len(tableDesc.GetColumns()[0].OwnsSequenceIds),
			"unexpected number of sequences owned by d.t after restore",
		)
		require.Equal(t, seqDesc.ID, tableDesc.GetColumns()[0].OwnsSequenceIds[0],
			"unexpected ID of sequence owned by table d.t after restore",
		)
	})

	// When restoring a sequence that is owned by a table, but the owning table
	// does not exist, the user must specify the `skip_missing_sequence_owners`
	// flag. When supplied, the sequence should be restored without an owner.
	t.Run("test restoring sequence when table does not exist", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.Background())

		newDB := sqlutils.MakeSQLRunner(tc.Conns[0])
		kvDB := tc.Server(0).DB()
		newDB.Exec(t, `CREATE DATABASE d`)
		newDB.Exec(t, `USE d`)
		newDB.ExpectErr(t, `pq: cannot restore sequence "seq" without referenced owner`,
			`RESTORE TABLE seq FROM $1`, backupLoc)

		newDB.Exec(t, `RESTORE TABLE seq FROM $1 WITH skip_missing_sequence_owners`, backupLoc)
		seqDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "d", "seq")
		require.False(t, seqDesc.SequenceOpts.HasOwner(), "unexpected owner of restored sequence.")
	})

	// When just the table is restored by itself, the ownership dependency is
	// removed as the sequence doesn't exist. When the sequence is restored
	// after that, it requires the `skip_missing_sequence_owners` flag as
	// the table isn't being restored with it, and when provided, the sequence
	// shouldn't have an owner.
	t.Run("test restoring table then sequence should remove ownership dependency",
		func(t *testing.T) {
			tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
			defer tc.Stopper().Stop(context.Background())

			newDB := sqlutils.MakeSQLRunner(tc.Conns[0])
			kvDB := tc.Server(0).DB()
			newDB.Exec(t, `CREATE DATABASE d`)
			newDB.Exec(t, `USE d`)
			newDB.ExpectErr(t, `pq: cannot restore sequence "seq" without referenced owner table`,
				`RESTORE TABLE seq FROM $1`, backupLoc)

			newDB.ExpectErr(t, `pq: cannot restore table "t" without referenced sequence`,
				`RESTORE TABLE t FROM $1`, backupLoc)
			newDB.Exec(t, `RESTORE TABLE t FROM $1 WITH skip_missing_sequence_owners`, backupLoc)

			tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "d", "t")

			require.Equal(t, 0, len(tableDesc.GetColumns()[0].OwnsSequenceIds),
				"expected restored table to own 0 sequences",
			)

			newDB.ExpectErr(t, `pq: cannot restore sequence "seq" without referenced owner table`,
				`RESTORE TABLE seq FROM $1`, backupLoc)
			newDB.Exec(t, `RESTORE TABLE seq FROM $1 WITH skip_missing_sequence_owners`, backupLoc)

			seqDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "d", "seq")
			require.False(t, seqDesc.SequenceOpts.HasOwner(), "unexpected sequence owner after restore")
		})

	// Ownership dependencies should be preserved and remapped when restoring
	// both the owned sequence and owning table into a different database.
	t.Run("test restoring all tables into a different database", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.Background())

		newDB := sqlutils.MakeSQLRunner(tc.Conns[0])
		kvDB := tc.Server(0).DB()

		newDB.Exec(t, `CREATE DATABASE restore_db`)
		newDB.Exec(t, `RESTORE d.* FROM $1 WITH into_db='restore_db'`, backupLoc)

		tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "restore_db", "t")
		seqDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "restore_db", "seq")

		require.True(t, seqDesc.SequenceOpts.HasOwner(), "no sequence owner after restore")
		require.Equal(t, tableDesc.ID, seqDesc.SequenceOpts.SequenceOwner.OwnerTableID,
			"unexpected table is sequence owner after restore",
		)
		require.Equal(t, tableDesc.GetColumns()[0].ID, seqDesc.SequenceOpts.SequenceOwner.OwnerColumnID,
			"unexpected column is sequence owner after restore",
		)
		require.Equal(t, 1, len(tableDesc.GetColumns()[0].OwnsSequenceIds),
			"unexpected number of sequences owned by d.t after restore",
		)
		require.Equal(t, seqDesc.ID, tableDesc.GetColumns()[0].OwnsSequenceIds[0],
			"unexpected ID of sequence owned by table d.t after restore",
		)
	})

	// Setup for cross-database ownership backup-restore tests.
	backupLocD2D3 := LocalFoo + `/d2d3`

	origDB.Exec(t, `CREATE DATABASE d2`)
	origDB.Exec(t, `CREATE TABLE d2.t(a int)`)

	origDB.Exec(t, `CREATE DATABASE d3`)
	origDB.Exec(t, `CREATE TABLE d3.t(a int)`)

	origDB.Exec(t, `CREATE SEQUENCE d2.seq OWNED BY d3.t.a`)

	origDB.Exec(t, `CREATE SEQUENCE d3.seq OWNED BY d2.t.a`)
	origDB.Exec(t, `CREATE SEQUENCE d3.seq2 OWNED BY d3.t.a`)

	origDB.Exec(t, `BACKUP DATABASE d2, d3 TO $1`, backupLocD2D3)

	// When restoring a database that has a sequence which is owned by a table
	// in another database, the user must supply the
	// `skip_missing_sequence_owners` flag. When supplied, the cross-database
	// ownership dependency should be removed.
	t.Run("test restoring two databases removes cross-database ownership dependency",
		func(t *testing.T) {
			tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
			defer tc.Stopper().Stop(context.Background())

			newDB := sqlutils.MakeSQLRunner(tc.Conns[0])
			kvDB := tc.Server(0).DB()

			newDB.ExpectErr(t, "pq: cannot restore sequence \"seq\" without referenced owner|"+
				"pq: cannot restore table \"t\" without referenced sequence",
				`RESTORE DATABASE d2 FROM $1`, backupLocD2D3)
			newDB.Exec(t, `RESTORE DATABASE d2 FROM $1 WITH skip_missing_sequence_owners`, backupLocD2D3)

			tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "d2", "t")
			require.Equal(t, 0, len(tableDesc.GetColumns()[0].OwnsSequenceIds),
				"expected restored table to own no sequences.",
			)

			newDB.ExpectErr(t, "pq: cannot restore sequence \"seq\" without referenced owner|"+
				"pq: cannot restore table \"t\" without referenced sequence",
				`RESTORE DATABASE d3 FROM $1`, backupLocD2D3)
			newDB.Exec(t, `RESTORE DATABASE d3 FROM $1 WITH skip_missing_sequence_owners`, backupLocD2D3)

			seqDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "d3", "seq")
			require.False(t, seqDesc.SequenceOpts.HasOwner(), "unexpected sequence owner after restore")

			// Sequence dependencies inside the database should still be preserved.
			sd := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "d3", "seq2")
			td := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "d3", "t")

			require.True(t, sd.SequenceOpts.HasOwner(), "no owner found for seq2")
			require.Equal(t, td.ID, sd.SequenceOpts.SequenceOwner.OwnerTableID,
				"unexpected table owner for sequence seq2 after restore",
			)
			require.Equal(t, td.GetColumns()[0].ID, sd.SequenceOpts.SequenceOwner.OwnerColumnID,
				"unexpected column owner for sequence seq2 after restore")
			require.Equal(t, 1, len(td.GetColumns()[0].OwnsSequenceIds),
				"unexpected number of sequences owned by d3.t after restore",
			)
			require.Equal(t, sd.ID, td.GetColumns()[0].OwnsSequenceIds[0],
				"unexpected ID of sequences owned by d3.t",
			)
		})

	// When restoring both the databases that contain a cross database ownership
	// dependency, we should preserve and remap the ownership dependencies.
	t.Run("test restoring both databases at the same time", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.Background())

		newDB := sqlutils.MakeSQLRunner(tc.Conns[0])
		kvDB := tc.Server(0).DB()

		newDB.Exec(t, `RESTORE DATABASE d2, d3 FROM $1`, backupLocD2D3)

		// d2.t owns d3.seq should be preserved.
		tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "d2", "t")
		seqDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "d3", "seq")

		require.True(t, seqDesc.SequenceOpts.HasOwner(), "no sequence owner after restore")
		require.Equal(t, tableDesc.ID, seqDesc.SequenceOpts.SequenceOwner.OwnerTableID,
			"unexpected table is sequence owner after restore",
		)
		require.Equal(t, tableDesc.GetColumns()[0].ID, seqDesc.SequenceOpts.SequenceOwner.OwnerColumnID,
			"unexpected column is sequence owner after restore",
		)
		require.Equal(t, 1, len(tableDesc.GetColumns()[0].OwnsSequenceIds),
			"unexpected number of sequences owned by d.t after restore",
		)
		require.Equal(t, seqDesc.ID, tableDesc.GetColumns()[0].OwnsSequenceIds[0],
			"unexpected ID of sequence owned by table d.t after restore",
		)

		// d3.t owns d2.seq and d3.seq2 should be preserved.
		td := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "d3", "t")
		sd := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "d2", "seq")
		sdSeq2 := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "d3", "seq2")

		require.True(t, sd.SequenceOpts.HasOwner(), "no sequence owner after restore")
		require.True(t, sdSeq2.SequenceOpts.HasOwner(), "no sequence owner after restore")

		require.Equal(t, td.ID, sd.SequenceOpts.SequenceOwner.OwnerTableID,
			"unexpected table is sequence owner of d3.seq after restore",
		)
		require.Equal(t, td.ID, sdSeq2.SequenceOpts.SequenceOwner.OwnerTableID,
			"unexpected table is sequence owner of d3.seq2 after restore",
		)

		require.Equal(t, td.GetColumns()[0].ID, sd.SequenceOpts.SequenceOwner.OwnerColumnID,
			"unexpected column is sequence owner of d2.seq after restore",
		)
		require.Equal(t, td.GetColumns()[0].ID, sdSeq2.SequenceOpts.SequenceOwner.OwnerColumnID,
			"unexpected column is sequence owner of d3.seq2 after restore",
		)

		require.Equal(t, 2, len(td.GetColumns()[0].OwnsSequenceIds),
			"unexpected number of sequences owned by d3.t after restore",
		)
		require.Equal(t, sd.ID, td.GetColumns()[0].OwnsSequenceIds[0],
			"unexpected ID of sequence owned by table d3.t after restore",
		)
		require.Equal(t, sdSeq2.ID, td.GetColumns()[0].OwnsSequenceIds[1],
			"unexpected ID of sequence owned by table d3.t after restore",
		)
	})
}

func TestBackupRestoreShowJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	sqlDB.Exec(t, `BACKUP DATABASE data TO $1 WITH revision_history`, LocalFoo)
	sqlDB.Exec(t, `CREATE DATABASE "data 2"`)

	sqlDB.Exec(t, `RESTORE data.bank FROM $1 WITH skip_missing_foreign_keys, into_db = $2`, LocalFoo, "data 2")
	// The "updating privileges" clause in the SELECT statement is for excluding jobs
	// run by an unrelated startup migration.
	// TODO (lucy): Update this if/when we decide to change how these jobs queued by
	// the startup migration are handled.
	sqlDB.CheckQueryResults(
		t, "SELECT description FROM [SHOW JOBS] WHERE description != 'updating privileges' ORDER BY description",
		[][]string{
			{"BACKUP DATABASE data TO 'nodelocal://0/foo' WITH revision_history"},
			{"RESTORE TABLE data.bank FROM 'nodelocal://0/foo' WITH into_db='data 2', skip_missing_foreign_keys"},
		},
	)
}

func TestBackupCreatedStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	sqlDB.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled=false`)

	sqlDB.Exec(t, `CREATE TABLE data.foo (a INT PRIMARY KEY)`)
	injectStats(t, sqlDB, "data.bank", "id")
	injectStats(t, sqlDB, "data.foo", "a")

	sqlDB.Exec(t, `BACKUP data.bank, data.foo TO $1 WITH revision_history`, LocalFoo)
	sqlDB.Exec(t, `CREATE DATABASE "data 2"`)
	sqlDB.Exec(t, `RESTORE data.bank, data.foo FROM $1 WITH skip_missing_foreign_keys, into_db = $2`,
		LocalFoo, "data 2")

	sqlDB.CheckQueryResults(t, getStatsQuery(`"data 2".bank`),
		sqlDB.QueryStr(t, getStatsQuery("data.bank")))
	sqlDB.CheckQueryResults(t, getStatsQuery(`"data 2".foo`),
		sqlDB.QueryStr(t, getStatsQuery("data.foo")))
}

// Ensure that backing up and restoring an empty database succeeds.
func TestBackupRestoreEmptyDB(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	sqlDB.Exec(t, `CREATE DATABASE empty`)
	sqlDB.Exec(t, `BACKUP DATABASE empty TO $1`, LocalFoo)
	sqlDB.Exec(t, `DROP DATABASE empty`)
	sqlDB.Exec(t, `RESTORE DATABASE empty FROM $1`, LocalFoo)
	sqlDB.CheckQueryResults(t, `USE empty; SHOW TABLES;`, [][]string{})
}

func TestBackupRestoreSubsetCreatedStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	sqlDB.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled=false`)

	sqlDB.Exec(t, `CREATE TABLE data.foo (a INT)`)
	bankStats := injectStats(t, sqlDB, "data.bank", "id")
	injectStats(t, sqlDB, "data.foo", "a")

	sqlDB.Exec(t, `BACKUP data.bank, data.foo TO $1 WITH revision_history`, LocalFoo)
	// Clear the stats.
	sqlDB.Exec(t, `DELETE FROM system.table_statistics WHERE true`)
	sqlDB.Exec(t, `CREATE DATABASE "data 2"`)
	sqlDB.Exec(t, `CREATE TABLE "data 2".foo (a INT)`)
	sqlDB.Exec(t, `RESTORE data.bank FROM $1 WITH skip_missing_foreign_keys, into_db = $2`,
		LocalFoo, "data 2")

	// Ensure that bank's stats have been restored, but foo's have not.
	sqlDB.CheckQueryResults(t, getStatsQuery(`"data 2".bank`), bankStats)
	sqlDB.CheckQueryResults(t, getStatsQuery(`"data 2".foo`), [][]string{})
}

// Ensure that statistics are restored from correct backup.
func TestBackupCreatedStatsFromIncrementalBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const incremental1Foo = "nodelocal://0/incremental1foo"
	const incremental2Foo = "nodelocal://0/incremental2foo"
	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()
	var beforeTs string

	sqlDB.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled=false`)

	// Create the 1st backup, with stats estimating 50 rows.
	injectStatsWithRowCount(t, sqlDB, "data.bank", "id", 50 /* rowCount */)
	sqlDB.Exec(t, `BACKUP data.bank TO $1 WITH revision_history`, LocalFoo)

	// Create the 2nd backup, with stats estimating 100 rows.
	injectStatsWithRowCount(t, sqlDB, "data.bank", "id", 100 /* rowCount */)
	statsBackup2 := sqlDB.QueryStr(t, getStatsQuery("data.bank"))
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&beforeTs) // Save time to restore to this point.
	sqlDB.Exec(t, `BACKUP data.bank TO $1 INCREMENTAL FROM $2 WITH revision_history`, incremental1Foo, LocalFoo)

	// Create the 3rd backup, with stats estimating 500 rows.
	injectStatsWithRowCount(t, sqlDB, "data.bank", "id", 500 /* rowCount */)
	sqlDB.Exec(t, `BACKUP data.bank TO $1 INCREMENTAL FROM $2, $3 WITH revision_history`, incremental2Foo, LocalFoo, incremental1Foo)

	// Restore the 2nd backup.
	sqlDB.Exec(t, `CREATE DATABASE "data 2"`)
	sqlDB.Exec(t, fmt.Sprintf(`RESTORE data.bank FROM "%s", "%s", "%s" AS OF SYSTEM TIME %s WITH skip_missing_foreign_keys, into_db = "%s"`,
		LocalFoo, incremental1Foo, incremental2Foo, beforeTs, "data 2"))

	// Expect the stats look as they did in the second backup.
	sqlDB.CheckQueryResults(t, getStatsQuery(`"data 2".bank`), statsBackup2)
}

// TestProtectedTimestampsDuringBackup ensures that the timestamp at which a
// table is taken offline is protected during a BACKUP job to ensure that if
// data can be read for a period longer than the default GC interval.
func TestProtectedTimestampsDuringBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// A sketch of the test is as follows:
	//
	//  * Create a table foo to backup.
	//  * Create an initial BACKUP of foo.
	//  * Set a 1 second gcttl for foo.
	//  * Start a BACKUP incremental from that base backup which blocks after
	//	  setup (after time of backup is decided), until it is signaled.
	//  * Manually enqueue the ranges for GC and ensure that at least one
	//    range ran the GC.
	//  * Unblock the backup.
	//  * Ensure the backup has succeeded.

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	allowRequest := make(chan struct{})
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	params := base.TestClusterArgs{}
	params.ServerArgs.ExternalIODir = dir
	params.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingRequestFilter: func(ctx context.Context, ba roachpb.BatchRequest) *roachpb.Error {
			for _, ru := range ba.Requests {
				switch ru.GetInner().(type) {
				case *roachpb.ExportRequest:
					<-allowRequest
				}
			}
			return nil
		},
	}
	tc := testcluster.StartTestCluster(t, 3, params)
	defer tc.Stopper().Stop(ctx)

	tc.WaitForNodeLiveness(t)
	require.NoError(t, tc.WaitForFullReplication())

	conn := tc.ServerConn(0)
	runner := sqlutils.MakeSQLRunner(conn)
	runner.Exec(t, "CREATE TABLE foo (k INT PRIMARY KEY, v BYTES)")
	close(allowRequest)

	for _, testrun := range []struct {
		name      string
		runBackup func(t *testing.T, query string, sqlDB *sqlutils.SQLRunner)
	}{
		{
			"backup-normal",
			func(t *testing.T, query string, sqlDB *sqlutils.SQLRunner) {
				sqlDB.Exec(t, query)
			},
		},
		{
			"backup-detached",
			func(t *testing.T, query string, sqlDB *sqlutils.SQLRunner) {
				backupWithDetachedOption := query + ` WITH DETACHED`
				db := sqlDB.DB.(*gosql.DB)
				var jobID int64
				tx, err := db.Begin()
				require.NoError(t, err)
				err = tx.QueryRow(backupWithDetachedOption).Scan(&jobID)
				require.NoError(t, err)
				require.NoError(t, tx.Commit())
				waitForSuccessfulJob(t, tc, jobID)
			},
		},
	} {
		baseBackupURI := "nodelocal://0/foo" + testrun.name
		testrun.runBackup(t, fmt.Sprintf(`BACKUP TABLE FOO TO '%s'`, baseBackupURI), runner) // create a base backup.
		allowRequest = make(chan struct{})
		runner.Exec(t, "SET CLUSTER SETTING kv.protectedts.poll_interval = '100ms';")
		runner.Exec(t, "ALTER TABLE foo CONFIGURE ZONE USING gc.ttlseconds = 1;")
		rRand, _ := randutil.NewPseudoRand()
		writeGarbage := func(from, to int) {
			for i := from; i < to; i++ {
				runner.Exec(t, "UPSERT INTO foo VALUES ($1, $2)", i, randutil.RandBytes(rRand, 1<<10))
			}
		}
		writeGarbage(3, 10)
		rowCount := runner.QueryStr(t, "SELECT * FROM foo")

		g, _ := errgroup.WithContext(ctx)
		g.Go(func() error {
			// If BACKUP does not protect the timestamp, the ExportRequest will
			// throw an error and fail the backup.
			incURI := "nodelocal://0/foo-inc" + testrun.name
			testrun.runBackup(t, fmt.Sprintf(`BACKUP TABLE FOO TO '%s' INCREMENTAL FROM '%s'`, incURI, baseBackupURI), runner)
			return nil
		})

		var jobID string
		testutils.SucceedsSoon(t, func() error {
			row := conn.QueryRow("SELECT job_id FROM [SHOW JOBS] ORDER BY created DESC LIMIT 1")
			return row.Scan(&jobID)
		})

		time.Sleep(3 * time.Second) // Wait for the data to definitely be expired and GC to run.
		gcTable := func(skipShouldQueue bool) (traceStr string) {
			rows := runner.Query(t, "SELECT start_key"+
				" FROM crdb_internal.ranges_no_leases"+
				" WHERE table_name = $1"+
				" AND database_name = current_database()"+
				" ORDER BY start_key ASC", "foo")
			var traceBuf strings.Builder
			for rows.Next() {
				var startKey roachpb.Key
				require.NoError(t, rows.Scan(&startKey))
				r := tc.LookupRangeOrFatal(t, startKey)
				l, _, err := tc.FindRangeLease(r, nil)
				require.NoError(t, err)
				lhServer := tc.Server(int(l.Replica.NodeID) - 1)
				s, repl := getFirstStoreReplica(t, lhServer, startKey)
				trace, _, err := s.ManuallyEnqueue(ctx, "gc", repl, skipShouldQueue)
				require.NoError(t, err)
				fmt.Fprintf(&traceBuf, "%s\n", trace.String())
			}
			require.NoError(t, rows.Err())
			return traceBuf.String()
		}

		// We should have refused to GC over the timestamp which we needed to protect.
		gcTable(true /* skipShouldQueue */)

		// Unblock the blocked backup request.
		close(allowRequest)

		runner.CheckQueryResultsRetry(t, "SELECT * FROM foo", rowCount)

		// Wait for the ranges to learn about the removed record and ensure that we
		// can GC from the range soon.
		// This regex matches when all float priorities other than 0.00000. It does
		// this by matching either a float >= 1 (e.g. 1230.012) or a float < 1 (e.g.
		// 0.000123).
		matchNonZero := "[1-9]\\d*\\.\\d+|0\\.\\d*[1-9]\\d*"
		nonZeroProgressRE := regexp.MustCompile(fmt.Sprintf("priority=(%s)", matchNonZero))
		testutils.SucceedsSoon(t, func() error {
			writeGarbage(3, 10)
			if trace := gcTable(false /* skipShouldQueue */); !nonZeroProgressRE.MatchString(trace) {
				return fmt.Errorf("expected %v in trace: %v", nonZeroProgressRE, trace)
			}
			return nil
		})
		require.NoError(t, g.Wait())
	}

}

func getFirstStoreReplica(
	t *testing.T, s serverutils.TestServerInterface, key roachpb.Key,
) (*kvserver.Store, *kvserver.Replica) {
	t.Helper()
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)
	var repl *kvserver.Replica
	testutils.SucceedsSoon(t, func() error {
		repl = store.LookupReplica(roachpb.RKey(key))
		if repl == nil {
			return errors.New(`could not find replica`)
		}
		return nil
	})
	return store, repl
}

// TestRestoreJobErrorPropagates ensures that errors from creating the job
// record propagate correctly.
func TestRestoreErrorPropagates(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	params := base.TestClusterArgs{}
	params.ServerArgs.ExternalIODir = dir
	jobsTableKey := keys.SystemSQLCodec.TablePrefix(keys.JobsTableID)
	var shouldFail, failures int64
	params.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingRequestFilter: func(ctx context.Context, ba roachpb.BatchRequest) *roachpb.Error {
			// Intercept Put and ConditionalPut requests to the jobs table
			// and, if shouldFail is positive, increment failures and return an
			// injected error.
			if !ba.IsWrite() {
				return nil
			}
			for _, ru := range ba.Requests {
				r := ru.GetInner()
				switch r.(type) {
				case *roachpb.ConditionalPutRequest, *roachpb.PutRequest:
					key := r.Header().Key
					if bytes.HasPrefix(key, jobsTableKey) && atomic.LoadInt64(&shouldFail) > 0 {
						return roachpb.NewError(errors.Errorf("boom %d", atomic.AddInt64(&failures, 1)))
					}
				}
			}
			return nil
		},
	}
	tc := testcluster.StartTestCluster(t, 3, params)
	defer tc.Stopper().Stop(ctx)
	db := tc.ServerConn(0)
	runner := sqlutils.MakeSQLRunner(db)
	runner.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false`)
	runner.Exec(t, "CREATE TABLE foo ()")
	runner.Exec(t, "CREATE DATABASE into_db")
	url := `nodelocal://0/foo`
	runner.Exec(t, `BACKUP TABLE foo to '`+url+`'`)
	atomic.StoreInt64(&shouldFail, 1)
	_, err := db.Exec(`RESTORE TABLE foo FROM '` + url + `' WITH into_db = 'into_db'`)
	// Expect to see the first job write failure.
	require.Regexp(t, "boom 1", err)
}

// TestProtectedTimestampsFailDueToLimits ensures that when creating a protected
// timestamp record fails, we return the correct error.
func TestProtectedTimestampsFailDueToLimits(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	params := base.TestClusterArgs{}
	params.ServerArgs.ExternalIODir = dir
	tc := testcluster.StartTestCluster(t, 1, params)
	defer tc.Stopper().Stop(ctx)
	db := tc.ServerConn(0)
	runner := sqlutils.MakeSQLRunner(db)
	runner.Exec(t, "CREATE TABLE foo (k INT PRIMARY KEY, v BYTES)")
	runner.Exec(t, "CREATE TABLE bar (k INT PRIMARY KEY, v BYTES)")
	runner.Exec(t, "SET CLUSTER SETTING kv.protectedts.max_spans = 1")

	// Creating the protected timestamp record should fail because there are too
	// many spans. Ensure that we get the appropriate error.
	_, err := db.Exec(`BACKUP TABLE foo, bar TO 'nodelocal://0/foo'`)
	require.EqualError(t, err, "pq: protectedts: limit exceeded: 0+2 > 1 spans")
}

// Ensure that backing up and restoring tenants succeeds.
func TestBackupRestoreTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1
	ctx, tc, systemDB, dir, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()
	srv := tc.Server(0)

	// NB: tenant certs for 10, 11, 20 are embedded. See:
	_ = security.EmbeddedTenantIDs()

	// Setup a few tenants, each with a different table.
	conn10 := serverutils.StartTenant(t, srv, base.TestTenantArgs{TenantID: roachpb.MakeTenantID(10)})
	defer conn10.Close()
	tenant10 := sqlutils.MakeSQLRunner(conn10)
	tenant10.Exec(t, `CREATE DATABASE foo; CREATE TABLE foo.bar(i int primary key); INSERT INTO foo.bar VALUES (110), (210)`)

	conn11 := serverutils.StartTenant(t, srv, base.TestTenantArgs{TenantID: roachpb.MakeTenantID(11)})
	defer conn11.Close()
	tenant11 := sqlutils.MakeSQLRunner(conn11)
	tenant11.Exec(t, `CREATE DATABASE foo; CREATE TABLE foo.baz(i int primary key); INSERT INTO foo.baz VALUES (111), (211)`)

	conn20 := serverutils.StartTenant(t, srv, base.TestTenantArgs{TenantID: roachpb.MakeTenantID(20)})
	defer conn20.Close()
	tenant20 := sqlutils.MakeSQLRunner(conn20)
	tenant20.Exec(t, `CREATE DATABASE foo; CREATE TABLE foo.qux(i int primary key); INSERT INTO foo.qux VALUES (120), (220)`)

	var ts1, ts2 string
	systemDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts1)
	tenant10.Exec(t, `UPDATE foo.bar SET i = i + 10000`)
	tenant10.Exec(t, `CREATE TABLE foo.bar2(i int primary key); INSERT INTO foo.bar2 VALUES (1010), (2010)`)
	systemDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts2)

	// BACKUP tenant 10 at ts1, before they created bar2.
	systemDB.Exec(t, `BACKUP TENANT 10 TO 'nodelocal://1/t10' AS OF SYSTEM TIME `+ts1)
	// Also create a full cluster backup. It should contain the tenant.
	systemDB.Exec(t, `BACKUP TO 'nodelocal://1/clusterwide' AS OF SYSTEM TIME `+ts1)

	// Incrementally backup tenant 10 again, capturing up to ts2.
	systemDB.Exec(t, `BACKUP TENANT 10 TO 'nodelocal://1/t10' AS OF SYSTEM TIME `+ts2)
	// Run full cluster backup incrementally to ts2 as well.
	systemDB.Exec(t, `BACKUP TO 'nodelocal://1/clusterwide' AS OF SYSTEM TIME `+ts2)

	systemDB.Exec(t, `BACKUP TENANT 11 TO 'nodelocal://1/t11'`)
	systemDB.Exec(t, `BACKUP TENANT 20 TO 'nodelocal://1/t20'`)

	// TODO(dt): test destroying a tenant and RESTORE'ing over a destroyed tenant
	// once tenant destruction actually clears their key-space. See #48775.

	t.Run("non-existent", func(t *testing.T) {
		systemDB.ExpectErr(t, "tenant 123 does not exist", `BACKUP TENANT 123 TO 'nodelocal://1/t1'`)
		systemDB.ExpectErr(t, "tenant 21 does not exist", `BACKUP TENANT 21 TO 'nodelocal://1/t20'`)
		systemDB.ExpectErr(t, "tenant 21 not in backup", `RESTORE TENANT 21 FROM 'nodelocal://1/t20'`)
		systemDB.ExpectErr(t, "file does not exist", `RESTORE TENANT 21 FROM 'nodelocal://1/t21'`)
	})

	t.Run("invalid", func(t *testing.T) {
		systemDB.ExpectErr(t, "invalid tenant ID", `BACKUP TENANT 0 TO 'nodelocal://1/z'`)
		systemDB.ExpectErr(t, "tenant 123 does not exist", `BACKUP TENANT 123 TO 'nodelocal://1/z'`)
		systemDB.ExpectErr(t, "syntax error", `BACKUP TENANT system TO 'nodelocal://1/z'`)
	})

	t.Run("restore-tenant10-to-latest", func(t *testing.T) {
		restoreTC := testcluster.StartTestCluster(
			t, singleNode, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}},
		)
		defer restoreTC.Stopper().Stop(ctx)
		restoreDB := sqlutils.MakeSQLRunner(restoreTC.Conns[0])

		restoreDB.CheckQueryResults(t, `select * from system.tenants`, [][]string{})
		restoreDB.Exec(t, `RESTORE TENANT 10 FROM 'nodelocal://1/t10'`)
		restoreDB.CheckQueryResults(t,
			`select id, active, crdb_internal.pb_to_json('cockroach.sql.sqlbase.TenantInfo', info) from system.tenants`,
			[][]string{{`10`, `true`, `{"id": "10", "state": "ACTIVE"}`}},
		)

		restoreConn10 := serverutils.StartTenant(
			t, restoreTC.Server(0), base.TestTenantArgs{TenantID: roachpb.MakeTenantID(10), Existing: true},
		)
		defer restoreConn10.Close()
		restoreTenant10 := sqlutils.MakeSQLRunner(restoreConn10)

		restoreTenant10.CheckQueryResults(t, `select * from foo.bar`, tenant10.QueryStr(t, `select * from foo.bar`))
		restoreTenant10.CheckQueryResults(t, `select * from foo.bar2`, tenant10.QueryStr(t, `select * from foo.bar2`))
	})

	t.Run("restore-t10-from-cluster-backup", func(t *testing.T) {
		restoreTC := testcluster.StartTestCluster(
			t, singleNode, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}},
		)
		defer restoreTC.Stopper().Stop(ctx)
		restoreDB := sqlutils.MakeSQLRunner(restoreTC.Conns[0])

		restoreDB.CheckQueryResults(t, `select * from system.tenants`, [][]string{})
		restoreDB.Exec(t, `RESTORE TENANT 10 FROM 'nodelocal://1/clusterwide'`)
		restoreDB.CheckQueryResults(t,
			`select id, active, crdb_internal.pb_to_json('cockroach.sql.sqlbase.TenantInfo', info) from system.tenants`,
			[][]string{{`10`, `true`, `{"id": "10", "state": "ACTIVE"}`}},
		)

		restoreConn10 := serverutils.StartTenant(
			t, restoreTC.Server(0), base.TestTenantArgs{TenantID: roachpb.MakeTenantID(10), Existing: true},
		)
		defer restoreConn10.Close()
		restoreTenant10 := sqlutils.MakeSQLRunner(restoreConn10)

		restoreTenant10.CheckQueryResults(t, `select * from foo.bar`, tenant10.QueryStr(t, `select * from foo.bar`))
		restoreTenant10.CheckQueryResults(t, `select * from foo.bar2`, tenant10.QueryStr(t, `select * from foo.bar2`))
	})

	t.Run("restore-all-from-cluster-backup", func(t *testing.T) {
		restoreTC := testcluster.StartTestCluster(
			t, singleNode, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}},
		)

		defer restoreTC.Stopper().Stop(ctx)
		restoreDB := sqlutils.MakeSQLRunner(restoreTC.Conns[0])

		restoreDB.CheckQueryResults(t, `select * from system.tenants`, [][]string{})
		restoreDB.Exec(t, `RESTORE FROM 'nodelocal://1/clusterwide'`)
		restoreDB.CheckQueryResults(t,
			`select id, active, crdb_internal.pb_to_json('cockroach.sql.sqlbase.TenantInfo', info) from system.tenants`,
			[][]string{
				{`10`, `true`, `{"id": "10", "state": "ACTIVE"}`},
				{`11`, `true`, `{"id": "11", "state": "ACTIVE"}`},
				{`20`, `true`, `{"id": "20", "state": "ACTIVE"}`},
			},
		)

		restoreConn10 := serverutils.StartTenant(
			t, restoreTC.Server(0), base.TestTenantArgs{TenantID: roachpb.MakeTenantID(10), Existing: true},
		)
		defer restoreConn10.Close()
		restoreTenant10 := sqlutils.MakeSQLRunner(restoreConn10)

		restoreTenant10.CheckQueryResults(t, `select * from foo.bar`, tenant10.QueryStr(t, `select * from foo.bar`))
		restoreTenant10.CheckQueryResults(t, `select * from foo.bar2`, tenant10.QueryStr(t, `select * from foo.bar2`))

		restoreConn11 := serverutils.StartTenant(
			t, restoreTC.Server(0), base.TestTenantArgs{TenantID: roachpb.MakeTenantID(11), Existing: true},
		)
		defer restoreConn11.Close()
		restoreTenant11 := sqlutils.MakeSQLRunner(restoreConn11)

		restoreTenant11.CheckQueryResults(t, `select * from foo.baz`, tenant11.QueryStr(t, `select * from foo.baz`))
	})

	t.Run("restore-tenant10-to-ts1", func(t *testing.T) {
		restoreTC := testcluster.StartTestCluster(
			t, singleNode, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}},
		)
		defer restoreTC.Stopper().Stop(ctx)
		restoreDB := sqlutils.MakeSQLRunner(restoreTC.Conns[0])

		restoreDB.Exec(t, `RESTORE TENANT 10 FROM 'nodelocal://1/t10' AS OF SYSTEM TIME `+ts1)

		restoreConn10 := serverutils.StartTenant(
			t, restoreTC.Server(0), base.TestTenantArgs{TenantID: roachpb.MakeTenantID(10), Existing: true},
		)
		defer restoreConn10.Close()
		restoreTenant10 := sqlutils.MakeSQLRunner(restoreConn10)

		restoreTenant10.CheckQueryResults(t, `select * from foo.bar`, tenant10.QueryStr(t, `select * from foo.bar AS OF SYSTEM TIME `+ts1))
	})

	t.Run("restore-tenant20-to-latest", func(t *testing.T) {
		restoreTC := testcluster.StartTestCluster(
			t, singleNode, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}},
		)
		defer restoreTC.Stopper().Stop(ctx)
		restoreDB := sqlutils.MakeSQLRunner(restoreTC.Conns[0])

		restoreDB.Exec(t, `RESTORE TENANT 20 FROM 'nodelocal://1/t20'`)

		restoreConn20 := serverutils.StartTenant(
			t, restoreTC.Server(0), base.TestTenantArgs{TenantID: roachpb.MakeTenantID(20), Existing: true},
		)
		defer restoreConn20.Close()
		restoreTenant20 := sqlutils.MakeSQLRunner(restoreConn20)

		restoreTenant20.CheckQueryResults(t, `select * from foo.qux`, tenant20.QueryStr(t, `select * from foo.qux`))
	})
}

// TestClientDisconnect ensures that an backup job can complete even if
// the client connection which started it closes.
func TestClientDisconnect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const restoreDB = "restoredb"

	testCases := []struct {
		jobType    string
		jobCommand string
	}{
		{
			jobType:    "BACKUP",
			jobCommand: fmt.Sprintf("BACKUP TO '%s'", LocalFoo),
		},
		{
			jobType:    "RESTORE",
			jobCommand: fmt.Sprintf("RESTORE data.* FROM '%s' WITH into_db='%s'", LocalFoo, restoreDB),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.jobType, func(t *testing.T) {
			// When completing an export request, signal the a request has been sent and
			// then wait to be signaled.
			allowResponse := make(chan struct{})
			gotRequest := make(chan struct{}, 1)
			args := base.TestClusterArgs{}
			args.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
				TestingResponseFilter: func(ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse) *roachpb.Error {
					for _, ru := range br.Responses {
						switch ru.GetInner().(type) {
						case *roachpb.ExportResponse, *roachpb.ImportResponse:
							select {
							case gotRequest <- struct{}{}:
							default:
							}
							select {
							case <-allowResponse:
							case <-ctx.Done(): // Deal with test failures.
							}
						}
					}
					return nil
				},
			}
			ctx, tc, sqlDB, _, cleanup := backupRestoreTestSetupWithParams(t, MultiNode, 1 /* numAccounts */, InitNone, args)
			defer cleanup()
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			conn := tc.ServerConn(0)
			sqlDB.Exec(t, "SET CLUSTER SETTING kv.protectedts.poll_interval = '100ms';")

			// If we're testing restore, we first create a backup file to restore.
			if testCase.jobType == "RESTORE" {
				close(allowResponse)
				sqlDB.Exec(t, fmt.Sprintf("CREATE DATABASE %s", restoreDB))
				sqlDB.Exec(t, "BACKUP TO $1", LocalFoo)
				// Reset the channels. There will be a request on the gotRequest channel
				// due to the backup.
				allowResponse = make(chan struct{})
				<-gotRequest
			}

			// Make credentials for the new connection.
			sqlDB.Exec(t, `CREATE USER testuser`)
			sqlDB.Exec(t, `GRANT admin TO testuser`)
			pgURL, cleanup := sqlutils.PGUrl(t, tc.Server(0).ServingSQLAddr(),
				"TestClientDisconnect-testuser", url.User("testuser"))
			defer cleanup()

			// Kick off the job on a new connection which we're going to close.
			done := make(chan struct{})
			ctxToCancel, cancel := context.WithCancel(ctx)
			defer cancel()
			go func() {
				defer close(done)
				connCfg, err := pgx.ParseConnectionString(pgURL.String())
				assert.NoError(t, err)
				db, err := pgx.Connect(connCfg)
				assert.NoError(t, err)
				defer func() { _ = db.Close() }()
				_, err = db.ExecEx(ctxToCancel, testCase.jobCommand, nil /* options */)
				assert.Equal(t, context.Canceled, err)
			}()

			// Wait for the job to start.
			var jobID string
			testutils.SucceedsSoon(t, func() error {
				row := conn.QueryRow(
					"SELECT job_id FROM [SHOW JOBS] WHERE job_type = $1 ORDER BY created DESC LIMIT 1",
					testCase.jobType,
				)
				return row.Scan(&jobID)
			})

			// Wait for it to actually start.
			<-gotRequest

			// Cancel the job's context and wait for the goroutine to exit.
			cancel()
			<-done

			// Allow the job to proceed.
			close(allowResponse)

			// Wait for the job to get marked as succeeded.
			testutils.SucceedsSoon(t, func() error {
				var status string
				if err := conn.QueryRow("SELECT status FROM [SHOW JOB " + jobID + "]").Scan(&status); err != nil {
					return err
				}
				const succeeded = "succeeded"
				if status != succeeded {
					return errors.Errorf("expected %s, got %v", succeeded, status)
				}
				return nil
			})
		})
	}
}

func TestBackupDoesNotHangOnIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 10
	ctx, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	sqlDB.Exec(t, "SET CLUSTER SETTING bulkio.backup.read_with_priority_after = '100ms'")
	sqlDB.Exec(t, "SET CLUSTER SETTING bulkio.backup.read_retry_delay = '10ms'")

	// start a txn that we'll hold open while we try to backup.
	tx, err := sqlDB.DB.(*gosql.DB).BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// observe commit time to ensure it sees the client restart error below when
	// the backup aborts it.
	if _, err := tx.Exec("SELECT cluster_logical_timestamp()"); err != nil {
		t.Fatal(err)
	}

	// lay down an intent that out backup will hit.
	if _, err := tx.Exec("UPDATE data.bank SET balance = 0 WHERE id = 5 OR id = 8"); err != nil {
		t.Fatal(err)
	}

	// backup the table in which we have our intent.
	_, err = sqlDB.DB.ExecContext(ctx, "BACKUP data.bank TO 'nodelocal://0/intent'")
	require.NoError(t, err)

	// observe that the backup aborted our txn.
	require.Error(t, tx.Commit())
}

// TestRestoreResetsDescriptorVersions tests that new descriptors created while
// restoring have their versions reset. Descriptors end up at version 2 after
// the job is finished, since they are updated once at the end of the job to
// make them public.
func TestRestoreResetsDescriptorVersions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	_, tc, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, 0, InitNone)
	defer cleanupFn()
	kvDB := tc.Server(0).DB()

	// Create some descriptors and do some schema changes to bump their versions.
	sqlDB.Exec(t, `
CREATE DATABASE d_old;
ALTER DATABASE d_old RENAME TO d;

USE d;

CREATE SCHEMA sc_old;
ALTER SCHEMA sc_old RENAME TO sc;

CREATE TABLE sc.tb (x INT);
ALTER TABLE sc.tb ADD COLUMN a INT;

CREATE TYPE sc.typ AS ENUM ('hello');
ALTER TYPE sc.typ ADD VALUE 'hi';
`)
	// Back up the database.
	sqlDB.Exec(t, `BACKUP DATABASE d TO 'nodelocal://0/test/'`)

	// Drop the database and restore into it.
	sqlDB.Exec(t, `DROP DATABASE d`)
	sqlDB.Exec(t, `RESTORE DATABASE d FROM 'nodelocal://0/test/'`)

	dbDesc := catalogkv.TestingGetDatabaseDescriptor(kvDB, keys.SystemSQLCodec, "d")
	require.EqualValues(t, 2, dbDesc.Version)

	schemaDesc := catalogkv.TestingGetSchemaDescriptor(kvDB, keys.SystemSQLCodec, dbDesc.GetID(), "sc")
	require.EqualValues(t, 2, schemaDesc.Version)

	tableDesc := catalogkv.TestingGetTableDescriptorFromSchema(kvDB, keys.SystemSQLCodec, "d", "sc", "tb")
	require.EqualValues(t, 2, tableDesc.Version)

	typeDesc := catalogkv.TestingGetTypeDescriptorFromSchema(kvDB, keys.SystemSQLCodec, "d", "sc", "typ")
	require.EqualValues(t, 2, typeDesc.Version)
}

func TestOfflineDescriptorsDuringRestore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var state = struct {
		mu syncutil.Mutex
		// Closed when the restore job has reached the point right before publishing
		// descriptors.
		beforePublishingNotification chan struct{}
		// Closed when we're ready to resume with the restore.
		continueNotification chan struct{}
	}{}
	initBackfillNotification := func() (chan struct{}, chan struct{}) {
		state.mu.Lock()
		defer state.mu.Unlock()
		state.beforePublishingNotification = make(chan struct{})
		state.continueNotification = make(chan struct{})
		return state.beforePublishingNotification, state.continueNotification
	}
	notifyBackfill := func(ctx context.Context) {
		state.mu.Lock()
		defer state.mu.Unlock()
		if state.beforePublishingNotification != nil {
			close(state.beforePublishingNotification)
			state.beforePublishingNotification = nil
		}
		select {
		case <-state.continueNotification:
			return
		case <-ctx.Done():
			return
		}
	}

	t.Run("restore-database", func(t *testing.T) {
		ctx, tc, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, 0, InitNone)
		defer cleanupFn()
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		kvDB := tc.Server(0).DB()

		for _, server := range tc.Servers {
			registry := server.JobRegistry().(*jobs.Registry)
			registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
				jobspb.TypeRestore: func(raw jobs.Resumer) jobs.Resumer {
					r := raw.(*restoreResumer)
					r.testingKnobs.beforePublishingDescriptors = func() error {
						notifyBackfill(ctx)
						return nil
					}
					return r
				},
			}
		}

		sqlDB.Exec(t, `
CREATE DATABASE d;
USE d;
CREATE SCHEMA sc;
CREATE TABLE sc.tb (x INT);
CREATE TYPE sc.typ AS ENUM ('hello');
`)

		// Back up the database.
		sqlDB.Exec(t, `BACKUP DATABASE d TO 'nodelocal://0/test/'`)

		// Drop the database and restore into it.
		sqlDB.Exec(t, `DROP DATABASE d`)

		beforePublishingNotif, continueNotif := initBackfillNotification()
		g := ctxgroup.WithContext(ctx)
		g.GoCtx(func(ctx context.Context) error {
			if _, err := sqlDB.DB.ExecContext(ctx, `RESTORE DATABASE d FROM 'nodelocal://0/test/'`); err != nil {
				t.Fatal(err)
			}
			return nil
		})

		<-beforePublishingNotif

		// Verify that the descriptors are offline.

		dbDesc := catalogkv.TestingGetDatabaseDescriptor(kvDB, keys.SystemSQLCodec, "d")
		require.Equal(t, descpb.DescriptorState_OFFLINE, dbDesc.State)

		schemaDesc := catalogkv.TestingGetSchemaDescriptor(kvDB, keys.SystemSQLCodec, dbDesc.GetID(), "sc")
		require.Equal(t, descpb.DescriptorState_OFFLINE, schemaDesc.State)

		tableDesc := catalogkv.TestingGetTableDescriptorFromSchema(kvDB, keys.SystemSQLCodec, "d", "sc", "tb")
		require.Equal(t, descpb.DescriptorState_OFFLINE, tableDesc.State)

		typeDesc := catalogkv.TestingGetTypeDescriptorFromSchema(kvDB, keys.SystemSQLCodec, "d", "sc", "typ")
		require.Equal(t, descpb.DescriptorState_OFFLINE, typeDesc.State)

		// Verify that the descriptors are not visible.
		// TODO (lucy): Arguably there should be a SQL test where we manually create
		// the offline descriptors. This part doesn't have much to do with RESTORE
		// per se.

		// Sometimes name resolution doesn't result in an "offline" error because
		// the lookups are performed in planner.LookupObject(), which sets the
		// Required flag to false so that callers can decide what to do with a
		// negative result, but also means that we never generate the error in the
		// first place. Right now we just settle for having some error reported, even
		// if it's not the ideal error.

		sqlDB.CheckQueryResults(t, `SHOW DATABASES`, [][]string{
			{"data", security.RootUser}, {"defaultdb", security.RootUser},
			{"postgres", security.RootUser}, {"system", security.NodeUser}})

		sqlDB.ExpectErr(t, `database "d" is offline: restoring`, `USE d`)

		sqlDB.ExpectErr(t, `target database or schema does not exist`, `SHOW TABLES FROM d`)
		sqlDB.ExpectErr(t, `target database or schema does not exist`, `SHOW TABLES FROM d.sc`)

		sqlDB.ExpectErr(t, `relation "d.sc.tb" does not exist`, `SELECT * FROM d.sc.tb`)
		sqlDB.ExpectErr(t, `relation "d.sc.tb" does not exist`, `ALTER TABLE d.sc.tb ADD COLUMN b INT`)

		sqlDB.ExpectErr(t, `type "d.sc.typ" does not exist`, `ALTER TYPE d.sc.typ RENAME TO typ2`)

		sqlDB.ExpectErr(t, `cannot create "d.sc.other" because the target database or schema does not exist`, `CREATE TABLE d.sc.other()`)

		close(continueNotif)
		require.NoError(t, g.Wait())
	})

	t.Run("restore-into-existing-database", func(t *testing.T) {
		ctx, tc, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, 0, InitNone)
		defer cleanupFn()
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		kvDB := tc.Server(0).DB()

		for _, server := range tc.Servers {
			registry := server.JobRegistry().(*jobs.Registry)
			registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
				jobspb.TypeRestore: func(raw jobs.Resumer) jobs.Resumer {
					r := raw.(*restoreResumer)
					r.testingKnobs.beforePublishingDescriptors = func() error {
						notifyBackfill(ctx)
						return nil
					}
					return r
				},
			}
		}

		sqlDB.Exec(t, `
CREATE DATABASE d;
USE d;
CREATE SCHEMA sc;
CREATE TABLE sc.tb (x INT);
CREATE TYPE sc.typ AS ENUM ('hello');
`)

		// Back up the database.
		sqlDB.Exec(t, `BACKUP DATABASE d TO 'nodelocal://0/test/'`)

		// Drop the database.
		sqlDB.Exec(t, `DROP DATABASE d`)

		// Create a new database and restore into it.
		sqlDB.Exec(t, `CREATE DATABASE newdb`)

		beforePublishingNotif, continueNotif := initBackfillNotification()
		g := ctxgroup.WithContext(ctx)
		g.GoCtx(func(ctx context.Context) error {
			if _, err := sqlDB.DB.ExecContext(ctx, `RESTORE d.* FROM 'nodelocal://0/test/' WITH into_db='newdb'`); err != nil {
				t.Fatal(err)
			}
			return nil
		})

		<-beforePublishingNotif

		// Verify that the descriptors are offline.

		dbDesc := catalogkv.TestingGetDatabaseDescriptor(kvDB, keys.SystemSQLCodec, "newdb")
		schemaDesc := catalogkv.TestingGetSchemaDescriptor(kvDB, keys.SystemSQLCodec, dbDesc.GetID(), "sc")
		require.Equal(t, descpb.DescriptorState_OFFLINE, schemaDesc.State)

		tableDesc := catalogkv.TestingGetTableDescriptorFromSchema(kvDB, keys.SystemSQLCodec, "newdb", "sc", "tb")
		require.Equal(t, descpb.DescriptorState_OFFLINE, tableDesc.State)

		typeDesc := catalogkv.TestingGetTypeDescriptorFromSchema(kvDB, keys.SystemSQLCodec, "newdb", "sc", "typ")
		require.Equal(t, descpb.DescriptorState_OFFLINE, typeDesc.State)

		// Verify that the descriptors are not visible.
		// TODO (lucy): Arguably there should be a SQL test where we manually create
		// the offline descriptors. This part doesn't have much to do with RESTORE
		// per se.

		// Sometimes name resolution doesn't result in an "offline" error because
		// the lookups are performed in planner.LookupObject(), which sets the
		// Required flag to false so that callers can decide what to do with a
		// negative result, but also means that we never generate the error in the
		// first place. Right now we just settle for having some error reported, even
		// if it's not the ideal error.

		sqlDB.Exec(t, `USE newdb`)

		sqlDB.CheckQueryResults(t, `SHOW TABLES`, [][]string{})
		sqlDB.CheckQueryResults(t, `SHOW TYPES`, [][]string{})
		sqlDB.CheckQueryResults(t, `SHOW SCHEMAS`, [][]string{
			{"crdb_internal", "NULL"}, {"information_schema", "NULL"}, {"pg_catalog", "NULL"}, {"pg_extension", "NULL"},
			{"public", security.AdminRole},
		})

		sqlDB.ExpectErr(t, `target database or schema does not exist`, `SHOW TABLES FROM newdb.sc`)

		sqlDB.ExpectErr(t, `schema "sc" is offline: restoring`, `SELECT * FROM newdb.sc.tb`)
		sqlDB.ExpectErr(t, `schema "sc" is offline: restoring`, `SELECT * FROM sc.tb`)
		sqlDB.ExpectErr(t, `schema "sc" is offline: restoring`, `ALTER TABLE newdb.sc.tb ADD COLUMN b INT`)
		sqlDB.ExpectErr(t, `schema "sc" is offline: restoring`, `ALTER TABLE sc.tb ADD COLUMN b INT`)

		sqlDB.ExpectErr(t, `schema "sc" is offline: restoring`, `ALTER TYPE newdb.sc.typ RENAME TO typ2`)
		sqlDB.ExpectErr(t, `schema "sc" is offline: restoring`, `ALTER TYPE sc.typ RENAME TO typ2`)

		sqlDB.ExpectErr(t, `schema "sc" is offline: restoring`, `ALTER SCHEMA sc RENAME TO sc2`)

		close(continueNotif)
		require.NoError(t, g.Wait())

	})
}

// TestManifestBitFlip tests that we can detect a corrupt manifest when a bit
// was flipped on disk for both an unencrypted and an encrypted manifest.
func TestManifestBitFlip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	_, _, sqlDB, rawDir, cleanupFn := BackupRestoreTestSetup(t, singleNode, 1, InitNone)
	defer cleanupFn()
	sqlDB.Exec(t, `CREATE DATABASE r1; CREATE DATABASE r2; CREATE DATABASE r3;`)
	const checksumError = "checksum mismatch"
	t.Run("unencrypted", func(t *testing.T) {
		sqlDB.Exec(t, `BACKUP DATABASE data TO 'nodelocal://0/bit_flip_unencrypted'`)
		flipBitInManifests(t, rawDir)
		sqlDB.ExpectErr(t, checksumError,
			`RESTORE data.* FROM 'nodelocal://0/bit_flip_unencrypted' WITH into_db='r1'`)
	})

	t.Run("encrypted", func(t *testing.T) {
		sqlDB.Exec(t, `BACKUP DATABASE data TO 'nodelocal://0/bit_flip_encrypted' WITH encryption_passphrase='abc'`)
		flipBitInManifests(t, rawDir)
		sqlDB.ExpectErr(t, checksumError,
			`RESTORE data.* FROM 'nodelocal://0/bit_flip_encrypted' WITH encryption_passphrase='abc', into_db='r3'`)
	})
}

// flipBitInManifests flips a bit in every backup manifest it sees. If
// afterDecompression is set to true, then the bit is contents are decompressed,
// a bit is flipped, and the corrupt data is re-compressed. This simulates a
// corruption in the data that will not be caught during the decompression
// layer.
func flipBitInManifests(t *testing.T, rawDir string) {
	foundManifest := false
	err := filepath.Walk(rawDir, func(path string, info os.FileInfo, err error) error {
		log.Infof(context.Background(), "visiting %s", path)
		if filepath.Base(path) == backupManifestName {
			foundManifest = true
			data, err := ioutil.ReadFile(path)
			require.NoError(t, err)
			data[20] ^= 1
			if err := ioutil.WriteFile(path, data, 0644 /* perm */); err != nil {
				t.Fatal(err)
			}
		}
		return nil
	})
	require.NoError(t, err)
	if !foundManifest {
		t.Fatal("found no manifest")
	}
}
