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
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
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

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/multitenantccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/partitionccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/amazon"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl" // register cloud storage providers
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descidgen"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/logtags"
	"github.com/gogo/protobuf/proto"
	pgx "github.com/jackc/pgx/v4"
	"github.com/kr/pretty"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func init() {
	cloud.RegisterKMSFromURIFactory(MakeTestKMS, "testkms")
}

func makeTableSpan(tableID uint32) roachpb.Span {
	k := keys.SystemSQLCodec.TablePrefix(tableID)
	return roachpb.Span{Key: k, EndKey: k.PrefixEnd()}
}

type sqlDBKey struct {
	server string
	user   string
}

type datadrivenTestState struct {
	servers      map[string]serverutils.TestServerInterface
	dataDirs     map[string]string
	sqlDBs       map[sqlDBKey]*gosql.DB
	noticeBuffer []string
	cleanupFns   []func()
}

var localityCfgs = map[string]roachpb.Locality{
	"us-east-1": {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east-1"},
			{Key: "availability-zone", Value: "us-east1"},
		},
	},
	"us-west-1": {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-west-1"},
			{Key: "availability-zone", Value: "us-west1"},
		},
	},
	"eu-central-1": {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "eu-central-1"},
			{Key: "availability-zone", Value: "eu-central-1"},
		},
	},
	"eu-north-1": {
		Tiers: []roachpb.Tier{
			{Key: "region", Value: "eu-north-1"},
			{Key: "availability-zone", Value: "eu-north-1"},
		},
	},
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
	d.noticeBuffer = nil
}

func (d *datadrivenTestState) addServer(
	t *testing.T,
	name, iodir, tempCleanupFrequency string,
	ioConf base.ExternalIODirConfig,
	localities string,
) error {
	var tc serverutils.TestClusterInterface
	var cleanup func()
	params := base.TestClusterArgs{}
	params.ServerArgs.ExternalIODirConfig = ioConf
	if tempCleanupFrequency != "" {
		duration, err := time.ParseDuration(tempCleanupFrequency)
		if err != nil {
			return errors.New("unable to parse tempCleanupFrequency during server creation")
		}
		settings := cluster.MakeTestingClusterSettings()
		sql.TempObjectCleanupInterval.Override(context.Background(), &settings.SV, duration)
		sql.TempObjectWaitInterval.Override(context.Background(), &settings.SV, time.Millisecond)
		params.ServerArgs.Settings = settings
	}

	clusterSize := singleNode

	if localities != "" {
		cfgs := strings.Split(localities, ",")
		clusterSize = len(cfgs)
		serverArgsPerNode := make(map[int]base.TestServerArgs)
		for i, cfg := range cfgs {
			param := params.ServerArgs
			param.Locality = localityCfgs[cfg]
			serverArgsPerNode[i] = param
		}
		params.ServerArgsPerNode = serverArgsPerNode
	}
	if iodir == "" {
		tc, _, iodir, cleanup = backupRestoreTestSetupWithParams(t, clusterSize, 0, InitManualReplication, params)
	} else {
		tc, _, cleanup = backupRestoreTestSetupEmptyWithParams(t, clusterSize, iodir, InitManualReplication, params)
	}
	d.servers[name] = tc.Server(0)
	d.dataDirs[name] = iodir
	d.cleanupFns = append(d.cleanupFns, cleanup)

	return nil
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

	base, err := pq.NewConnector(pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	connector := pq.ConnectorWithNoticeHandler(base, func(notice *pq.Error) {
		d.noticeBuffer = append(d.noticeBuffer, notice.Severity+": "+notice.Message)
		if notice.Detail != "" {
			d.noticeBuffer = append(d.noticeBuffer, "DETAIL: "+notice.Detail)
		}
		if notice.Hint != "" {
			d.noticeBuffer = append(d.noticeBuffer, "HINT: "+notice.Hint)
		}
	})
	d.sqlDBs[key] = gosql.OpenDB(connector)
	return d.sqlDBs[key]
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
	skip.UnderRace(t, "takes >1 min under race")
	skip.UnderDeadlock(t, "assertion failure under deadlock")

	// This test uses this mock HTTP server to pass the backup files between tenants.
	httpAddr, httpServerCleanup := makeInsecureHTTPServer(t)
	defer httpServerCleanup()

	ctx := context.Background()
	datadriven.Walk(t, testutils.TestDataPath(t, "backup-restore", ""), func(t *testing.T, path string) {
		var lastCreatedServer string
		ds := newDatadrivenTestState()
		defer ds.cleanup(ctx)
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "sleep":
				var sleepDuration string
				d.ScanArgs(t, "time", &sleepDuration)
				duration, err := time.ParseDuration(sleepDuration)
				if err != nil {
					return err.Error()
				}
				time.Sleep(duration)
				return ""
			case "reset":
				ds.cleanup(ctx)
				ds = newDatadrivenTestState()
				return ""
			case "new-server":
				var name, shareDirWith, iodir, tempCleanupFrequency, localities string
				var io base.ExternalIODirConfig
				d.ScanArgs(t, "name", &name)
				if d.HasArg("share-io-dir") {
					d.ScanArgs(t, "share-io-dir", &shareDirWith)
				}
				if shareDirWith != "" {
					iodir = ds.getIODir(t, shareDirWith)
				}
				if d.HasArg("allow-implicit-access") {
					io.EnableNonAdminImplicitAndArbitraryOutbound = true
				}
				if d.HasArg("disable-http") {
					io.DisableHTTP = true
				}
				if d.HasArg("temp-cleanup-freq") {
					d.ScanArgs(t, "temp-cleanup-freq", &tempCleanupFrequency)
				}
				if d.HasArg("localities") {
					d.ScanArgs(t, "localities", &localities)
				}
				lastCreatedServer = name
				err := ds.addServer(t, name, iodir, tempCleanupFrequency, io, localities)
				if err != nil {
					return err.Error()
				}
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
				ds.noticeBuffer = nil
				d.Input = strings.ReplaceAll(d.Input, "http://COCKROACH_TEST_HTTP_SERVER/", httpAddr)
				_, err := ds.getSQLDB(t, server, user).Exec(d.Input)
				ret := ds.noticeBuffer
				if err != nil {
					// pausepoint errors have the job ID in them, and datadriven tests
					// don't seem to support regex matching. Clean the error up to not
					// include the job ID.
					if i := strings.Index(err.Error(), "paused before it completed with reason"); i != -1 {
						ret = append(ds.noticeBuffer, err.Error()[i:])
					} else if pqErr := (*pq.Error)(nil); errors.As(err, &pqErr) {
						ret = append(ds.noticeBuffer, err.Error())
						if pqErr.Detail != "" {
							ret = append(ret, "DETAIL: "+pqErr.Detail)
						}
						if pqErr.Hint != "" {
							ret = append(ret, "HINT: "+pqErr.Hint)
						}
					} else {
						t.Errorf("failed to execute stmt %s due to %s", d.Input, err.Error())
					}
				}
				return strings.Join(ret, "\n")
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
				output, err := sqlutils.RowsToDataDrivenOutput(rows)
				require.NoError(t, err)
				return output
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
	_, sqlDB, dir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	if err := verifyBackupRestoreStatementResult(
		t, sqlDB, "BACKUP DATABASE data TO $1", localFoo,
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
		require.True(t, isGZipped(backupManifestBytes))
	})

	sqlDB.Exec(t, "CREATE DATABASE data2")

	if err := verifyBackupRestoreStatementResult(
		t, sqlDB, "RESTORE data.* FROM $1 WITH OPTIONS (into_db='data2')", localFoo,
	); err != nil {
		t.Fatal(err)
	}
}

func TestBackupRestoreSingleUserfile(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1000
	ctx := context.Background()
	tc, _, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	backupAndRestore(ctx, t, tc, []string{"userfile:///a"}, []string{"userfile:///a"}, numAccounts)
}

func TestBackupRestoreSingleNodeLocal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1000
	ctx := context.Background()
	tc, _, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	backupAndRestore(ctx, t, tc, []string{localFoo}, []string{localFoo}, numAccounts)
}

func TestBackupRestoreMultiNodeLocal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1000
	ctx := context.Background()
	tc, _, _, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	backupAndRestore(ctx, t, tc, []string{localFoo}, []string{localFoo}, numAccounts)
}

func TestBackupRestoreMultiNodeRemote(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1000
	ctx := context.Background()
	tc, _, _, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts, InitManualReplication)
	defer cleanupFn()
	// Backing up to node2's local file system
	remoteFoo := "nodelocal://2/foo"

	backupAndRestore(ctx, t, tc, []string{remoteFoo}, []string{localFoo}, numAccounts)
}

func TestBackupRestorePartitioned(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1000

	args := base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "west"},
					// NB: This has the same value as an az in the east region
					// on purpose.
					{Key: "az", Value: "az1"},
					{Key: "dc", Value: "dc1"},
				}},
			},
			1: {
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "east"},
					// NB: This has the same value as an az in the west region
					// on purpose.
					{Key: "az", Value: "az1"},
					{Key: "dc", Value: "dc2"},
				}},
			},
			2: {
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "east"},
					{Key: "az", Value: "az2"},
					{Key: "dc", Value: "dc3"},
				}},
			},
		},
	}

	ctx := context.Background()
	_, sqlDB, dir, cleanupFn := backupRestoreTestSetupWithParams(t, 3 /* nodes */, numAccounts, InitManualReplication, args)
	defer cleanupFn()

	// locationToDir converts backup URIs based on localFoo to the temporary
	// file it represents on disk.
	locationToDir := func(location string) string {
		return strings.Replace(location, localFoo, filepath.Join(dir, "foo"), 1)
	}

	hasSSTs := func(t *testing.T, location string) bool {
		sstMatcher := regexp.MustCompile(`\d+\.sst`)
		subDir := filepath.Join(locationToDir(location), "data")
		files, err := ioutil.ReadDir(subDir)
		if err != nil {
			if oserror.IsNotExist(err) {
				return false
			}

			t.Fatal(err)
		}
		found := false
		for _, f := range files {
			if sstMatcher.MatchString(f.Name()) {
				found = true
				break
			}
		}
		return found
	}

	requireHasSSTs := func(t *testing.T, locations ...string) {
		for _, location := range locations {
			require.True(t, hasSSTs(t, location))
		}
	}

	requireHasNoSSTs := func(t *testing.T, locations ...string) {
		for _, location := range locations {
			require.False(t, hasSSTs(t, location))
		}
	}

	requireCompressedManifest := func(t *testing.T, locations ...string) {
		partitionMatcher := regexp.MustCompile(`^BACKUP_PART_`)
		for _, location := range locations {
			subDir := locationToDir(location)
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
					require.True(t, isGZipped(backupPartitionBytes))
				}
			}
		}
	}

	runBackupRestore := func(t *testing.T, sqlDB *sqlutils.SQLRunner, backupURIs []string) {
		locationFmtString, locationURIArgs := uriFmtStringAndArgs(backupURIs)
		backupQuery := fmt.Sprintf("BACKUP DATABASE data TO %s", locationFmtString)
		sqlDB.Exec(t, backupQuery, locationURIArgs...)

		sqlDB.Exec(t, `DROP DATABASE data;`)
		restoreQuery := fmt.Sprintf("RESTORE DATABASE data FROM %s", locationFmtString)
		sqlDB.Exec(t, restoreQuery, locationURIArgs...)
	}

	// Ensure that each node has at least one leaseholder. These are wrapped with
	// SucceedsSoon() because EXPERIMENTAL_RELOCATE can fail if there are other
	// replication changes happening.
	ensureLeaseholder := func(t *testing.T, sqlDB *sqlutils.SQLRunner) {
		for _, stmt := range []string{
			`ALTER TABLE data.bank SPLIT AT VALUES (0)`,
			`ALTER TABLE data.bank SPLIT AT VALUES (100)`,
			`ALTER TABLE data.bank SPLIT AT VALUES (200)`,
			`ALTER TABLE data.bank EXPERIMENTAL_RELOCATE VALUES (ARRAY[1], 0)`,
			`ALTER TABLE data.bank EXPERIMENTAL_RELOCATE VALUES (ARRAY[2], 100)`,
			`ALTER TABLE data.bank EXPERIMENTAL_RELOCATE VALUES (ARRAY[3], 200)`,
		} {
			testutils.SucceedsSoon(t, func() error {
				_, err := sqlDB.DB.ExecContext(ctx, stmt)
				return err
			})
		}
	}

	t.Run("partition-by-unique-key", func(t *testing.T) {
		ensureLeaseholder(t, sqlDB)
		testSubDir := t.Name()
		locations := []string{
			localFoo + "/" + testSubDir + "/1",
			localFoo + "/" + testSubDir + "/2",
			localFoo + "/" + testSubDir + "/3",
		}
		backupURIs := []string{
			// The first location will contain data from node 3 with config
			// dc=dc3.
			fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", locations[0], url.QueryEscape("default")),
			fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", locations[1], url.QueryEscape("dc=dc1")),
			fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", locations[2], url.QueryEscape("dc=dc2")),
		}
		runBackupRestore(t, sqlDB, backupURIs)

		// Verify that at least one SST exists in each backup destination.
		requireHasSSTs(t, locations...)

		// Verify that all of the partition manifests are compressed.
		requireCompressedManifest(t, locations...)
	})

	// Test that we're selecting the most specific locality tier for a location.
	t.Run("partition-by-different-tiers", func(t *testing.T) {
		ensureLeaseholder(t, sqlDB)
		testSubDir := t.Name()
		locations := []string{
			localFoo + "/" + testSubDir + "/1",
			localFoo + "/" + testSubDir + "/2",
			localFoo + "/" + testSubDir + "/3",
			localFoo + "/" + testSubDir + "/4",
		}
		backupURIs := []string{
			fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", locations[0], url.QueryEscape("default")),
			fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", locations[1], url.QueryEscape("region=east")),
			fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", locations[2], url.QueryEscape("az=az1")),
			fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", locations[3], url.QueryEscape("az=az2")),
		}

		runBackupRestore(t, sqlDB, backupURIs)

		// All data should be covered by az=az1 or az=az2, so expect all the
		// data on those locations.
		requireHasNoSSTs(t, locations[0], locations[1])
		requireHasSSTs(t, locations[2], locations[3])
	})

	t.Run("partition-by-several-keys", func(t *testing.T) {
		ensureLeaseholder(t, sqlDB)
		testSubDir := t.Name()
		locations := []string{
			localFoo + "/" + testSubDir + "/1",
			localFoo + "/" + testSubDir + "/2",
			localFoo + "/" + testSubDir + "/3",
			localFoo + "/" + testSubDir + "/4",
		}
		backupURIs := []string{
			fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", locations[0], url.QueryEscape("default")),
			fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", locations[1], url.QueryEscape("region=east,az=az1")),
			fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", locations[2], url.QueryEscape("region=east,az=az2")),
			fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", locations[3], url.QueryEscape("region=west,az=az1")),
		}

		// Specifying multiple tiers is not supported.
		locationFmtString, locationURIArgs := uriFmtStringAndArgs(backupURIs)
		backupQuery := fmt.Sprintf("BACKUP DATABASE data TO %s", locationFmtString)
		sqlDB.ExpectErr(t, `tier must be in the form "key=value" not "region=east,az=az1"`, backupQuery, locationURIArgs...)
	})
}

func TestBackupRestoreAppend(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.WithIssue(t, 54599, "flaky test")
	skip.UnderRace(t, "flaky test. Issues #50984, #54599")
	defer log.Scope(t).Close(t)

	const numAccounts = 1000
	ctx := context.Background()
	tc, sqlDB, tmpDir, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	// Ensure that each node has at least one leaseholder. (These splits were
	// made in backupRestoreTestSetup.) These are wrapped with SucceedsSoon()
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
	const localFoo1, localFoo2, localFoo3 = localFoo + "/1", localFoo + "/2", localFoo + "/3"
	const userfileFoo1, userfileFoo2, userfileFoo3 = `userfile:///bar/1`, `userfile:///bar/2`,
		`userfile:///bar/3`
	makeBackups := func(b1, b2, b3 string) []interface{} {
		return []interface{}{
			fmt.Sprintf("%s?COCKROACH_LOCALITY=%s&AUTH=implicit", b1, url.QueryEscape("default")),
			fmt.Sprintf("%s?COCKROACH_LOCALITY=%s&AUTH=implicit", b2, url.QueryEscape("dc=dc1")),
			fmt.Sprintf("%s?COCKROACH_LOCALITY=%s&AUTH=implicit", b3, url.QueryEscape("dc=dc2")),
		}
	}
	makeCollections := func(c1, c2, c3 string) []interface{} {
		return []interface{}{
			fmt.Sprintf("%s?COCKROACH_LOCALITY=%s&AUTH=implicit", c1, url.QueryEscape("default")),
			fmt.Sprintf("%s?COCKROACH_LOCALITY=%s&AUTH=implicit", c2, url.QueryEscape("dc=dc1")),
			fmt.Sprintf("%s?COCKROACH_LOCALITY=%s&AUTH=implicit", c3, url.QueryEscape("dc=dc2")),
		}
	}

	makeCollectionsWithSubdir := func(c1, c2, c3 string) []interface{} {
		return []interface{}{
			fmt.Sprintf("%s/%s?COCKROACH_LOCALITY=%s&AUTH=implicit", c1, "foo", url.QueryEscape("default")),
			fmt.Sprintf("%s/%s?COCKROACH_LOCALITY=%s&AUTH=implicit", c2, "foo", url.QueryEscape("dc=dc1")),
			fmt.Sprintf("%s/%s?COCKROACH_LOCALITY=%s&AUTH=implicit", c3, "foo", url.QueryEscape("dc=dc2")),
		}
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
		// TODO(dt): prevent backing up different targets to same collection?

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

			if test.name != "userfile" {
				// Cluster restores from userfile are not supported yet since the
				// restoring cluster needs to be empty, which means it can't contain any
				// userfile tables.

				_, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, multiNode, tmpDir, InitManualReplication, base.TestClusterArgs{})
				defer cleanupEmptyCluster()
				sqlDBRestore.Exec(t, "RESTORE FROM $4 IN ($1, $2, $3) AS OF SYSTEM TIME "+ts2, append(collections, fullBackup2)...)
			}
		}

		if test.name == "userfile" {
			// Find the backup times in the collection and try RESTORE'ing to each, and
			// within each also check if we can restore to individual times captured with
			// incremental backups that were appended to that backup.
			store, err := cloud.ExternalStorageFromURI(ctx, "userfile:///0",
				base.ExternalIODirConfig{},
				tc.Servers[0].ClusterSettings(),
				blobs.TestEmptyBlobClientFactory,
				security.RootUserName(),
				tc.Servers[0].InternalExecutor().(*sql.InternalExecutor), tc.Servers[0].DB())
			require.NoError(t, err)
			defer store.Close()
			var files []string
			require.NoError(t, store.List(ctx, "/", "", func(f string) error {
				ok, err := path.Match("*/*/*/"+backupManifestName, f)
				if ok {
					files = append(files, f)
				}
				return err
			}))
			full1 = strings.TrimSuffix(files[0], backupManifestName)
			full2 = strings.TrimSuffix(files[1], backupManifestName)

			// Find the full-backups written to the specified subdirectories, and within
			// each also check if we can restore to individual times captured with
			// incremental backups that were appended to that backup.
			var subdirFiles []string
			require.NoError(t, store.List(ctx, "foo/", "", func(f string) error {
				ok, err := path.Match(specifiedSubdir+"*/"+backupManifestName, f)
				if ok {
					subdirFiles = append(subdirFiles, f)
				}
				return err
			}))
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
	_, sqlDB, tmpDir, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	const c1, c2, c3 = `nodelocal://0/full/`, `nodelocal://1/full/`, `nodelocal://2/full/`
	const i1, i2, i3 = `nodelocal://0/inc/`, `nodelocal://1/inc/`, `nodelocal://2/inc/`

	const localFoo1, localFoo2, localFoo3 = localFoo + "/1", localFoo + "/2", localFoo + "/3"
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

	incrementals := []interface{}{
		fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", i1, url.QueryEscape("default")),
		fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", i2, url.QueryEscape("dc=dc1")),
		fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", i3, url.QueryEscape("dc=dc2")),
	}

	sqlDB.Exec(t, "BACKUP TO ($1, $2, $3)", backups...)
	sqlDB.Exec(t, "BACKUP INTO ($1, $2, $3)", collections...)
	sqlDB.Exec(t, "BACKUP INTO LATEST IN ($1, $2, $3)", collections...)
	sqlDB.Exec(t, "BACKUP INTO $4 IN ($1, $2, $3)", append(collections, "subdir")...)
	sqlDB.Exec(t, "BACKUP INTO LATEST IN $4 WITH incremental_location = ($1, $2, $3)",
		append(incrementals, collections[0])...)

	// Find the subdirectory created by the full BACKUP INTO statement.
	matches, err := filepath.Glob(path.Join(tmpDir, "full/*/*/*/"+backupManifestName))
	require.NoError(t, err)
	require.Equal(t, 1, len(matches))
	for i := range matches {
		matches[i] = strings.TrimPrefix(filepath.Dir(matches[i]), tmpDir)
	}
	full1 := strings.TrimPrefix(matches[0], "/full")
	sqlDB.CheckQueryResults(
		t, "SELECT description FROM [SHOW JOBS]",
		[][]string{
			{fmt.Sprintf("BACKUP TO ('%s', '%s', '%s')", backups[0].(string), backups[1].(string),
				backups[2].(string))},
			{fmt.Sprintf("BACKUP INTO '%s' IN ('%s', '%s', '%s')", full1, collections[0],
				collections[1], collections[2])},
			{fmt.Sprintf("BACKUP INTO '%s' IN ('%s', '%s', '%s')", full1,
				collections[0], collections[1], collections[2])},
			{fmt.Sprintf("BACKUP INTO '%s' IN ('%s', '%s', '%s')", "/subdir",
				collections[0], collections[1], collections[2])},
			{fmt.Sprintf("BACKUP INTO '%s' IN '%s' WITH incremental_location = ('%s', '%s', '%s')",
				"/subdir", collections[0], incrementals[0],
				incrementals[1], incrementals[2])},
		},
	)

	sqlDB.Exec(t, "DROP DATABASE data CASCADE")
	sqlDB.Exec(t, "RESTORE DATABASE data FROM ($1, $2, $3)", backups...)

	sqlDB.Exec(t, "DROP DATABASE data CASCADE")
	sqlDB.Exec(t, "RESTORE DATABASE data FROM $4 IN ($1, $2, $3)", append(collections, full1)...)

	sqlDB.Exec(t, "DROP DATABASE data CASCADE")
	sqlDB.Exec(t, "RESTORE DATABASE data FROM $4 IN ($1, $2, $3)", append(collections, "subdir")...)

	sqlDB.Exec(t, "DROP DATABASE data CASCADE")
	sqlDB.Exec(t, "RESTORE DATABASE data FROM LATEST IN ($1, $2, $3)", collections...)

	sqlDB.Exec(t, "DROP DATABASE data CASCADE")
	sqlDB.Exec(t, "RESTORE DATABASE data FROM LATEST IN ($1, $2, "+
		"$3) WITH incremental_location = ($4, $5, $6)",
		append(collections, incrementals[0], incrementals[1], incrementals[2])...)

	// The flavors of BACKUP and RESTORE which automatically resolve the right
	// directory to read/write data to, have URIs with the resolved path written
	// to the job description.
	getResolvedCollectionURIs := func(prefixes []interface{}, subdir string) []string {
		resolvedCollectionURIs := make([]string, len(prefixes))
		for i, collection := range prefixes {
			parsed, err := url.Parse(collection.(string))
			require.NoError(t, err)
			parsed.Path = path.Join(parsed.Path, subdir)
			resolvedCollectionURIs[i] = parsed.String()
		}

		return resolvedCollectionURIs
	}

	resolvedCollectionURIs := getResolvedCollectionURIs(collections, full1)
	resolvedSubdirURIs := getResolvedCollectionURIs(collections, "subdir")
	resolvedIncURIs := getResolvedCollectionURIs(incrementals, "subdir")

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
			// and again from LATEST IN...
			{fmt.Sprintf("RESTORE DATABASE data FROM ('%s', '%s', '%s')",
				resolvedSubdirURIs[0], resolvedSubdirURIs[1],
				resolvedSubdirURIs[2])},
			{fmt.Sprintf("RESTORE DATABASE data FROM ('%s', '%s', '%s') WITH incremental_location = ('%s', '%s', '%s')",
				resolvedSubdirURIs[0], resolvedSubdirURIs[1], resolvedSubdirURIs[2],
				resolvedIncURIs[0], resolvedIncURIs[1], resolvedIncURIs[2])},
		},
	)
}

func TestBackupRestorePartitionedMergeDirectories(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1000
	ctx := context.Background()
	tc, _, _, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	// TODO (lucy): This test writes a partitioned backup where all files are
	// written to the same directory, which is similar to the case where a backup
	// is created and then all files are consolidated into the same directory, but
	// we should still have a separate test where the files are actually moved.
	const localFoo1 = localFoo + "/1"
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
	ctx := context.Background()
	tc, _, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	backupAndRestore(ctx, t, tc, []string{localFoo}, []string{localFoo}, numAccounts)
}

// Regression test for #16008. In short, the way RESTORE constructed split keys
// for tables with negative primary key data caused AdminSplit to fail.
func TestBackupRestoreNegativePrimaryKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.WithIssue(t, 68127, "flaky test")
	defer log.Scope(t).Close(t)

	const numAccounts = 1000

	ctx := context.Background()
	tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	// Give half the accounts negative primary keys.
	sqlDB.Exec(t, `UPDATE data.bank SET id = $1 - id WHERE id > $1`, numAccounts/2)

	// Resplit that half of the table space.
	sqlDB.Exec(t,
		`ALTER TABLE data.bank SPLIT AT SELECT generate_series($1, 0, $2)`,
		-numAccounts/2, numAccounts/backupRestoreDefaultRanges/2,
	)

	backupAndRestore(ctx, t, tc, []string{localFoo}, []string{localFoo}, numAccounts)

	sqlDB.Exec(t, `CREATE UNIQUE INDEX id2 ON data.bank (id)`)

	var unused string
	var exportedRows, exportedIndexEntries int
	sqlDB.QueryRow(t, `BACKUP DATABASE data TO $1`, localFoo+"/alteredPK").Scan(
		&unused, &unused, &unused, &exportedRows, &exportedIndexEntries, &unused,
	)
	if exportedRows != numAccounts {
		t.Fatalf("expected %d rows, got %d", numAccounts, exportedRows)
	}
	expectedIndexEntries := numAccounts * 2 // Indexes id2 and balance_idx
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
	ctx = logtags.AddTag(ctx, "backup-client", nil)
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
		rows := sqlDB.Query(t, stmt)
		for rows.Next() {
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
			if backupDetails.DeprecatedBackupManifest != nil {
				t.Fatal("expected backup_manifest field of backup descriptor payload to be nil")
			}
			if backupManifest.DeprecatedStatistics != nil {
				t.Fatal("expected statistics field of backup descriptor payload to be nil")
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("unexpected error querying jobs: %s", err.Error())
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
	ctx := context.Background()
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts, InitManualReplication)
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
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP %s TO '%s' AS OF SYSTEM TIME %s`, tableSpec, localFoo, backupAsOf))
	sqlDB.Exec(t, fmt.Sprintf(`RESTORE %s FROM '%s' WITH into_db='system_new'`, tableSpec, localFoo))

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
		fmt.Sprintf(`RESTORE %s FROM '%s'`, tableSpec, localFoo),
	)
}

func TestBackupRestoreSystemJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 0
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts, InitManualReplication)
	conn := sqlDB.DB.(*gosql.DB)
	defer cleanupFn()

	sanitizedIncDir := localFoo + "/inc?AWS_SESSION_TOKEN="
	incDir := sanitizedIncDir + "secretCredentialsHere"

	sanitizedFullDir := localFoo + "/full?AWS_SESSION_TOKEN="
	fullDir := sanitizedFullDir + "moarSecretsHere"

	backupDatabaseID := sqlutils.QueryDatabaseID(t, conn, "data")
	backupSchemaID := sqlutils.QuerySchemaID(t, conn, "data", "public")
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
		Username: security.RootUserName(),
		Description: fmt.Sprintf(
			`BACKUP TABLE bank TO '%s' INCREMENTAL FROM '%s'`,
			sanitizedIncDir+"redacted", sanitizedFullDir+"redacted",
		),
		DescriptorIDs: descpb.IDs{
			descpb.ID(backupDatabaseID),
			descpb.ID(backupSchemaID),
			descpb.ID(backupTableID),
		},
	}); err != nil {
		t.Fatal(err)
	}

	sqlDB.Exec(t, `RESTORE TABLE bank FROM $1, $2 WITH OPTIONS (into_db='restoredb')`, fullDir, incDir)
	if err := jobutils.VerifySystemJob(t, sqlDB, 0, jobspb.TypeRestore, jobs.StatusSucceeded, jobs.Record{
		Username: security.RootUserName(),
		Description: fmt.Sprintf(
			`RESTORE TABLE bank FROM '%s', '%s' WITH into_db = 'restoredb'`,
			sanitizedFullDir+"redacted", sanitizedIncDir+"redacted",
		),
		DescriptorIDs: descpb.IDs{
			descpb.ID(restoreDatabaseID + 1),
			descpb.ID(restoreDatabaseID + 2),
		},
	}); err != nil {
		t.Fatal(err)
	}
}

func redactTestKMSURI(path string) (string, error) {
	redactedQueryParams := map[string]struct{}{
		amazon.AWSSecretParam: {},
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
		t.Run(tc.name, func(t *testing.T) {
			var encryptionOption string
			var sanitizedEncryptionOption1 string
			var sanitizedEncryptionOption2 string
			if tc.useKMS {
				correctKMSURI, _ := getAWSKMSURI(t, regionEnvVariable, keyIDEnvVariable)
				encryptionOption = fmt.Sprintf("kms = '%s'", correctKMSURI)
				sanitizedURI, err := redactTestKMSURI(correctKMSURI)
				require.NoError(t, err)
				sanitizedEncryptionOption1 = fmt.Sprintf("kms = '%s'", sanitizedURI)
				sanitizedEncryptionOption2 = sanitizedEncryptionOption1
			} else {
				encryptionOption = "encryption_passphrase = 'abcdefg'"
				sanitizedEncryptionOption1 = "encryption_passphrase = '*****'"
				sanitizedEncryptionOption2 = "encryption_passphrase = 'redacted'"
			}
			_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, multiNode, 3, InitManualReplication)
			conn := sqlDB.DB.(*gosql.DB)
			defer cleanupFn()
			backupLoc1 := localFoo + "/x"

			sqlDB.Exec(t, `CREATE DATABASE restoredb`)
			backupDatabaseID := sqlutils.QueryDatabaseID(t, conn, "data")
			backupTableID := sqlutils.QueryTableID(t, conn, "data", "public", "bank")
			backupSchemaID := sqlutils.QuerySchemaID(t, conn, "data", "public")
			restoreDatabaseID := sqlutils.QueryDatabaseID(t, conn, "restoredb")

			// Take an encrypted BACKUP.
			sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE data TO $1 WITH %s`, encryptionOption),
				backupLoc1)

			// Verify the BACKUP job description is sanitized.
			if err := jobutils.VerifySystemJob(t, sqlDB, 0, jobspb.TypeBackup, jobs.StatusSucceeded,
				jobs.Record{
					Username: security.RootUserName(),
					Description: fmt.Sprintf(
						`BACKUP DATABASE data TO '%s' WITH %s`,
						backupLoc1, sanitizedEncryptionOption1),
					DescriptorIDs: descpb.IDs{
						descpb.ID(backupDatabaseID),
						descpb.ID(backupSchemaID),
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
				Username: security.RootUserName(),
				Description: fmt.Sprintf(
					`RESTORE TABLE data.bank FROM '%s' WITH %s, into_db = 'restoredb'`,
					backupLoc1, sanitizedEncryptionOption2,
				),
				DescriptorIDs: descpb.IDs{
					descpb.ID(restoreDatabaseID + 1),
					descpb.ID(restoreDatabaseID + 2),
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

func (ip inProgressState) latestJobID() (jobspb.JobID, error) {
	var id jobspb.JobID
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
	var allowResponse chan struct{}
	var exportSpanCompleteCh chan struct{}
	params := base.TestClusterArgs{}
	knobs := base.TestingKnobs{
		DistSQL: &execinfra.TestingKnobs{
			BackupRestoreTestingKnobs: &sql.BackupRestoreTestingKnobs{
				RunAfterExportingSpanEntry: func(_ context.Context, res *roachpb.ExportResponse) {
					<-allowResponse
					// If ResumeSpan is set to nil, it means that we have completed
					// exporting a span and the job will update its fraction progressed.
					if res.ResumeSpan == nil {
						<-exportSpanCompleteCh
					}
				},
				RunAfterProcessingRestoreSpanEntry: func(_ context.Context) {
					<-allowResponse
				},
			},
		},
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}
	params.ServerArgs.Knobs = knobs

	const numAccounts = 100

	ctx := context.Background()
	_, sqlDB, dir, cleanup := backupRestoreTestSetupWithParams(t, multiNode, numAccounts,
		InitManualReplication, params)
	conn := sqlDB.DB.(*gosql.DB)
	defer cleanup()

	sqlDB.Exec(t, `CREATE DATABASE restoredb`)
	// the small test-case will get entirely buffered/merged by small-file merging
	// and not report any progress in the meantime unless it is disabled.
	sqlDB.Exec(t, `SET CLUSTER SETTING bulkio.backup.file_size = '1'`)
	sqlDB.Exec(t, `SET CLUSTER SETTING bulkio.backup.merge_file_buffer_size = '1'`)

	// Ensure that each node has at least one leaseholder. (These splits were
	// made in backupRestoreTestSetup.) These are wrapped with SucceedsSoon()
	// because EXPERIMENTAL_RELOCATE can fail if there are other replication
	// changes happening.
	for _, stmt := range []string{
		`ALTER TABLE data.bank EXPERIMENTAL_RELOCATE VALUES (ARRAY[1], 0)`,
		`ALTER TABLE data.bank EXPERIMENTAL_RELOCATE VALUES (ARRAY[2], 30)`,
		`ALTER TABLE data.bank EXPERIMENTAL_RELOCATE VALUES (ARRAY[3], 80)`,
	} {
		testutils.SucceedsSoon(t, func() error {
			_, err := sqlDB.DB.ExecContext(ctx, stmt)
			return err
		})
	}

	var totalExpectedBackupRequests int
	// mergedRangeQuery calculates the number of spans we expect PartitionSpans to
	// produce. It merges contiguous ranges on the same node.
	// It sorts ranges by start_key and counts the number of times the
	// lease_holder changes by comparing against the previous row's lease_holder.
	mergedRangeQuery := `
WITH
	ranges
		AS (
			SELECT
				start_key,
				lag(lease_holder) OVER (ORDER BY start_key)
					AS prev_lease_holder,
				lease_holder
			FROM
				[SHOW RANGES FROM TABLE data.bank]
		)
SELECT
	count(*)
FROM
	ranges
WHERE
	lease_holder != prev_lease_holder
	OR prev_lease_holder IS NULL;
`

	sqlDB.QueryRow(t, mergedRangeQuery).Scan(&totalExpectedBackupRequests)

	backupTableID := sqlutils.QueryTableID(t, conn, "data", "public", "bank")

	do := func(query string, check inProgressChecker) {
		t.Logf("checking query %q", query)

		var totalExpectedResponses int
		if strings.Contains(query, "BACKUP") {
			exportSpanCompleteCh = make(chan struct{})
			// totalExpectedBackupRequests takes into account the merging that backup
			// does of co-located ranges. It is the expected number of ExportRequests
			// backup issues. DistSender will still split those requests to different
			// ranges on the same node. Each range will write a file, so the number of
			// SST files this backup will write is `backupRestoreDefaultRanges` .
			totalExpectedResponses = totalExpectedBackupRequests
		} else if strings.Contains(query, "RESTORE") {
			// We expect restore to process each file in the backup individually.
			// SST files are written per-range in the backup. So we expect the
			// restore to process #(ranges) that made up the original table.
			totalExpectedResponses = backupRestoreDefaultRanges
		} else {
			t.Fatal("expected query to be either a backup or restore")
		}
		jobDone := make(chan error)
		allowResponse = make(chan struct{}, totalExpectedResponses)

		go func() {
			_, err := conn.Exec(query, localFoo)
			jobDone <- err
		}()

		// Allow half the total expected responses to proceed.
		for i := 0; i < totalExpectedResponses/2; i++ {
			allowResponse <- struct{}{}
		}

		// Due to ExportRequest pagination, in the case of backup, we want to wait
		// until an entire span has been exported before checking job progress.
		if strings.Contains(query, "BACKUP") {
			exportSpanCompleteCh <- struct{}{}
			close(exportSpanCompleteCh)
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
	skip.WithIssue(t, 68571, "flaky test")
	defer log.Scope(t).Close(t)
	defer jobs.TestingSetProgressThresholds()()

	skip.UnderStressRace(t, "test takes too long to run under stressrace")

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
		if fractionCompleted < 0.01 || fractionCompleted > 0.99 {
			return errors.Errorf(
				"expected progress to be in range [0.01, 0.99] but got %f",
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
		checkpointPath = filepath.Join(ip.dir, ip.name, backupProgressDirectory+"/"+backupManifestCheckpointName)
		checkpointDescBytes, err := ioutil.ReadFile(checkpointPath)
		if err != nil {
			return errors.Wrap(err, "error while reading checkpoint")
		}
		var checkpointDesc BackupManifest
		if err := protoutil.Unmarshal(checkpointDescBytes, &checkpointDesc); err != nil {
			return errors.Wrap(err, "error while unmarshalling checkpoint")
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
	} else if !oserror.IsNotExist(err) {
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
		UsernameProto: security.RootUserName().EncodeProto(),
		DescriptorIDs: descriptorIDs,
		StartedMicros: now,
		Details:       jobspb.WrapPayloadDetails(details),
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

	var jobID jobspb.JobID
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

	ctx := context.Background()

	params := base.TestClusterArgs{ServerArgs: base.TestServerArgs{
		Knobs: base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()},
	}}

	const numAccounts = 1000
	tc, outerDB, dir, cleanupFn := backupRestoreTestSetupWithParams(t, multiNode, numAccounts, InitManualReplication, params)
	defer cleanupFn()

	backupTableDesc := desctestutils.TestingGetPublicTableDescriptor(tc.Servers[0].DB(), keys.SystemSQLCodec, "data", "bank")

	t.Run("backup", func(t *testing.T) {
		sqlDB := sqlutils.MakeSQLRunner(outerDB.DB)
		backupStartKey := backupTableDesc.PrimaryIndexSpan(keys.SystemSQLCodec).Key
		backupEndKey, err := randgen.TestingMakePrimaryIndexKey(backupTableDesc, numAccounts/2)
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
		if err := os.MkdirAll(backupDir+"/"+backupProgressDirectory, 0755); err != nil {
			t.Fatal(err)
		}
		checkpointFile := backupDir + "/" + backupProgressDirectory + "/" + backupManifestCheckpointName
		if err := ioutil.WriteFile(checkpointFile, mockManifest, 0644); err != nil {
			t.Fatal(err)
		}
		createAndWaitForJob(
			t, sqlDB, []descpb.ID{backupTableDesc.GetID()},
			jobspb.BackupDetails{
				EndTime: tc.Servers[0].Clock().Now(),
				URI:     "nodelocal://0/backup",
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
		if isGZipped(backupManifestBytes) {
			backupManifestBytes, err = decompressData(ctx, nil, backupManifestBytes)
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
		restoreTableID, err := descidgen.GenerateUniqueDescID(ctx, tc.Servers[0].DB(), keys.SystemSQLCodec)
		if err != nil {
			t.Fatal(err)
		}
		restoreHighWaterMark, err := randgen.TestingMakePrimaryIndexKey(backupTableDesc, numAccounts/2)
		if err != nil {
			t.Fatal(err)
		}
		createAndWaitForJob(
			t, sqlDB, []descpb.ID{restoreTableID},
			jobspb.RestoreDetails{
				DescriptorRewrites: map[descpb.ID]*jobspb.DescriptorRewrite{
					backupTableDesc.GetID(): {
						ParentID:       descpb.ID(restoreDatabaseID),
						ParentSchemaID: descpb.ID(restoreDatabaseID + 1),
						ID:             restoreTableID,
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

func getHighWaterMark(jobID jobspb.JobID, sqlDB *gosql.DB) (roachpb.Key, error) {
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

	serverArgs := base.TestServerArgs{
		DisableSpanConfigs: true,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	}
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
	_, outerDB, _, cleanup := backupRestoreTestSetupWithParams(t, multiNode, numAccounts, init, params)
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

func TestRestoreFailCleansUpTTLSchedules(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	injectedErr := errors.New("injected error")
	testCases := []struct {
		desc                        string
		beforePublishingDescriptors func() error
		afterPublishingDescriptors  func() error
	}{
		{
			desc: "error before publishing descriptors",
			beforePublishingDescriptors: func() error {
				return injectedErr
			},
		},
		{
			desc: "error after publishing descriptors",
			afterPublishingDescriptors: func() error {
				return injectedErr
			},
		},
	}

	ctx := context.Background()

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			c, sqlDB, _, cleanup := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
			defer cleanup()
			for _, server := range c.Servers {
				registry := server.JobRegistry().(*jobs.Registry)
				registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
					jobspb.TypeRestore: func(raw jobs.Resumer) jobs.Resumer {
						r := raw.(*restoreResumer)
						r.testingKnobs.beforePublishingDescriptors = tc.beforePublishingDescriptors
						r.testingKnobs.afterPublishingDescriptors = tc.afterPublishingDescriptors
						return r
					},
				}
			}

			// Create a database with a TTL table.
			sqlDB.Exec(t, `
CREATE DATABASE d;
CREATE TABLE d.tb (id INT PRIMARY KEY) WITH (ttl_expire_after = '10 minutes')
`)

			// Backup d.tb.
			sqlDB.Exec(t, `BACKUP DATABASE d TO $1`, localFoo)

			// Drop d so that it can be restored.
			sqlDB.Exec(t, `DROP DATABASE d`)

			// Attempt the restore and check it fails.
			_, err := sqlDB.DB.ExecContext(ctx, `RESTORE DATABASE d FROM $1`, localFoo)
			require.Error(t, err)
			require.Regexp(t, injectedErr.Error(), err.Error())

			var count int
			sqlDB.QueryRow(t, `SELECT count(1) FROM [SHOW SCHEDULES] WHERE label LIKE 'row-level-ttl-%'`).Scan(&count)
			require.Equal(t, 0, count)
		})
	}
}

func TestRestoreFailCleansUpTypeBackReferences(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	_, sqlDB, dir, cleanup := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
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
	sqlDB.Exec(t, `BACKUP TABLE d.tb TO $1`, localFoo)

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
	sqlDB.ExpectErr(t, "sst: no such file", `RESTORE d.tb FROM $1`, localFoo)

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
		RunBeforeResume: func(_ jobspb.JobID) error {
			<-blockGC
			return nil
		},
	}

	const numAccounts = 1000
	tc, sqlDB, dir, cleanup := backupRestoreTestSetupWithParams(t, singleNode, numAccounts,
		InitManualReplication, base.TestClusterArgs{ServerArgs: params})
	defer cleanup()
	kvDB := tc.Server(0).DB()

	dir = dir + "/foo"

	sqlDB.Exec(t, `CREATE DATABASE restore`)

	// Create a user defined type and check that it is cleaned up after the
	// failed restore.
	sqlDB.Exec(t, `CREATE TYPE data.myenum AS ENUM ('hello')`)
	// Do the same with a user defined schema.
	sqlDB.Exec(t, `USE data; CREATE SCHEMA myschema`)

	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, localFoo)
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
		`RESTORE data.* FROM $1 WITH OPTIONS (into_db='restore')`, localFoo,
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

	// Verify that the only schema that appears is the public schema
	dbDesc := desctestutils.TestingGetDatabaseDescriptor(kvDB, keys.SystemSQLCodec, "restore")
	require.Equal(t, len(dbDesc.DatabaseDesc().Schemas), 1)
	if _, found := dbDesc.DatabaseDesc().Schemas[tree.PublicSchema]; !found {
		t.Error("public schema not found")
	}
}

// TestRestoreFailDatabaseCleanup tests that a failed RESTORE is cleaned up
// when restoring an entire database.
func TestRestoreFailDatabaseCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params := base.TestServerArgs{}
	const numAccounts = 1000
	_, sqlDB, dir, cleanup := backupRestoreTestSetupWithParams(t, singleNode, numAccounts,
		InitManualReplication, base.TestClusterArgs{ServerArgs: params})
	defer cleanup()

	dir = dir + "/foo"

	// Create a user defined type and check that it is cleaned up after the
	// failed restore.
	sqlDB.Exec(t, `CREATE TYPE data.myenum AS ENUM ('hello')`)
	// Do the same with a user defined schema.
	sqlDB.Exec(t, `USE data; CREATE SCHEMA myschema`)

	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, localFoo)
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
		`RESTORE DATABASE data FROM $1`, localFoo,
	)
	sqlDB.ExpectErr(
		t, `database "data" does not exist`,
		`DROP DATABASE data`,
	)
}

func TestRestoreFailCleansUpTempSystemDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	_, sqlDB, dir, cleanup := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
	defer cleanup()

	// Create a database with a type and table.
	sqlDB.Exec(t, `
CREATE DATABASE d;
CREATE TYPE d.ty AS ENUM ('hello');
CREATE TABLE d.tb (x d.ty);
INSERT INTO d.tb VALUES ('hello'), ('hello');
`)

	// Cluster BACKUP.
	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)

	// Bugger the backup by removing the SST files.
	if err := filepath.Walk(dir+"/foo", func(path string, info os.FileInfo, err error) error {
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

	_, sqlDBRestore, cleanupRestore := backupRestoreTestSetupEmpty(t, singleNode, dir, InitManualReplication,
		base.TestClusterArgs{})
	defer cleanupRestore()
	var defaultDBID int
	sqlDBRestore.QueryRow(
		t, "SELECT id FROM system.namespace WHERE name = 'defaultdb'",
	).Scan(&defaultDBID)

	// We should get an error when restoring the table.
	sqlDBRestore.ExpectErr(t, "sst: no such file", `RESTORE FROM $1`, localFoo)

	// Make sure the temp system db is not present.
	row := sqlDBRestore.QueryStr(t, fmt.Sprintf(`SELECT * FROM [SHOW DATABASES] WHERE database_name = '%s'`, restoreTempSystemDB))
	require.Equal(t, 0, len(row))

	// Make sure defaultdb and postgres are recreated with new IDs.
	sqlDBRestore.CheckQueryResults(t,
		fmt.Sprintf(`
SELECT name 
  FROM system.namespace
 WHERE "parentID" = 0 AND id > %d`, defaultDBID,
		), [][]string{{"defaultdb"}, {"postgres"}})
}

func TestBackupRestoreUserDefinedSchemas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test takes a full backup and an incremental backup with revision
	// history at certain timestamps, then restores to each of the timestamps to
	// ensure that the types restored are correct.
	t.Run("revision-history", func(t *testing.T) {
		_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
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
		_, sqlDB, dataDir, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
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
		_, sqlDBRestore, cleanupRestore := backupRestoreTestSetupEmpty(t, singleNode, dataDir, InitManualReplication, base.TestClusterArgs{})
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
		_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
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

	// Tests backing up and restoring all tables in requested user defined
	// schemas.
	t.Run("all-tables-in-requested-schema", func(t *testing.T) {
		_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
		defer cleanupFn()

		sqlDB.Exec(t, `
CREATE TABLE table_in_data (x INT);

CREATE SCHEMA data;
CREATE TABLE data.tb1 (x INT);

CREATE DATABASE foo;
USE foo;
CREATE SCHEMA schema_in_foo;
CREATE TABLE schema_in_foo.tb1 (x INT);

CREATE SCHEMA schema_in_foo2;
CREATE TABLE schema_in_foo2.tb1 (x INT);

CREATE SCHEMA foo;
CREATE TABLE foo.tb1 (x INT);

CREATE TABLE tb2 (y INT);
`)

		for _, tc := range []struct {
			name                   string
			target                 string
			expectedTablesInBackup [][]string
		}{
			{
				name:                   "fully-qualified-target",
				target:                 "foo.schema_in_foo.*",
				expectedTablesInBackup: [][]string{{"schema_in_foo", "tb1"}},
			},
			{
				name:                   "schema-qualified-target",
				target:                 "schema_in_foo.*",
				expectedTablesInBackup: [][]string{{"schema_in_foo", "tb1"}},
			},
			{
				name:                   "schema-qualified-target-with-identical-name-as-curdb",
				target:                 "foo.*",
				expectedTablesInBackup: [][]string{{"foo", "tb1"}},
			},
			{
				name:                   "curdb-public-schema-target",
				target:                 "*",
				expectedTablesInBackup: [][]string{{"public", "tb2"}},
			},
			{
				name:                   "cross-db-qualified-target",
				target:                 "data.*",
				expectedTablesInBackup: [][]string{{"data", "tb1"}, {"public", "bank"}, {"public", "table_in_data"}},
			},
		} {
			sqlDB.Exec(t, fmt.Sprintf(`BACKUP TABLE %s TO 'nodelocal://0/%s'`, tc.target, tc.name))
			sqlDB.Exec(t, `CREATE DATABASE restore`)
			sqlDB.Exec(t, fmt.Sprintf(`RESTORE TABLE %s FROM 'nodelocal://0/%s' WITH into_db='restore'`, tc.target, tc.name))
			sqlDB.CheckQueryResults(t, `SELECT schema_name,
table_name from [SHOW TABLES FROM restore] ORDER BY schema_name, table_name`, tc.expectedTablesInBackup)
			sqlDB.Exec(t, `DROP DATABASE restore CASCADE`)
		}
	})

	// Test restoring tables with user defined schemas when restore schemas are
	// not being remapped.
	t.Run("no-remap", func(t *testing.T) {
		_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
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
		tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
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
		dbDesc := desctestutils.TestingGetDatabaseDescriptor(kvDB, keys.SystemSQLCodec, "newdb")
		require.Contains(t, dbDesc.DatabaseDesc().Schemas, "sc1")
		require.Contains(t, dbDesc.DatabaseDesc().Schemas, "sc2")
		require.Contains(t, dbDesc.DatabaseDesc().Schemas, "sc3")
		require.Contains(t, dbDesc.DatabaseDesc().Schemas, "sc4")
		require.Contains(t, dbDesc.DatabaseDesc().Schemas, "existingschema")
		require.Len(t, dbDesc.DatabaseDesc().Schemas, 6)
	})
	// Test when we remap schemas to existing schemas in the cluster.
	t.Run("remap", func(t *testing.T) {
		_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
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
		_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
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
		_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
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
			sqlDB.Exec(t, `BACKUP d.* TO 'nodelocal://0/test_all_tables/'`)
			// Create a new database to restore all of the tables into.
			sqlDB.Exec(t, `CREATE DATABASE d4`)
			// Restore all of the tables.
			sqlDB.Exec(t, `RESTORE TABLE d.* FROM 'nodelocal://0/test_all_tables/' WITH into_db = 'd4'`)
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
		_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
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
			sqlDB.Exec(t, `BACKUP TABLE d5.tb1 TO 'nodelocal://0/testd5/'`)

			// Create another database with a compatible type.
			sqlDB.Exec(t, `CREATE DATABASE d6`)
			sqlDB.Exec(t, `CREATE TYPE d6.typ1 AS ENUM ('v1', 'v2')`)
			// Restoring tb1 into d6 will remap d5.typ1 to d6.typ1 and d5.___typ1
			// to d6._typ1.
			sqlDB.Exec(t, `RESTORE TABLE d5.tb1 FROM 'nodelocal://0/testd5/' WITH into_db = 'd6'`)
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

	// Test cases where we attempt to remap types in the backup to types that
	// already exist in the cluster with user defined schema.
	t.Run("backup-remap-uds", func(t *testing.T) {
		_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
		defer cleanupFn()
		sqlDB.Exec(t, `
CREATE DATABASE d;
CREATE SCHEMA d.s;
CREATE TYPE d.s.greeting AS ENUM ('hello', 'howdy', 'hi');
CREATE TABLE d.s.t (x d.s.greeting);
INSERT INTO d.s.t VALUES ('hello'), ('howdy');
CREATE TYPE d.s.farewell AS ENUM ('bye', 'cya');
CREATE TABLE d.s.t2 (x d.s.greeting[]);
INSERT INTO d.s.t2 VALUES (ARRAY['hello']);
`)
		{
			// Backup and restore t.
			sqlDB.Exec(t, `BACKUP TABLE d.s.t TO $1`, localFoo+"/1")
			sqlDB.Exec(t, `DROP TABLE d.s.t`)
			sqlDB.Exec(t, `RESTORE TABLE d.s.t FROM $1`, localFoo+"/1")

			// Check that the table data is restored correctly and the types aren't touched.
			sqlDB.CheckQueryResults(t, `SELECT 'hello'::d.s.greeting, ARRAY['hello']::d.s.greeting[]`, [][]string{{"hello", "{hello}"}})
			sqlDB.CheckQueryResults(t, `SELECT * FROM d.s.t ORDER BY x`, [][]string{{"hello"}, {"howdy"}})

			// d.t should be added as a back reference to greeting.
			sqlDB.ExpectErr(t, `pq: cannot drop type "greeting" because other objects \(.*\) still depend on it`, `DROP TYPE d.s.greeting`)
		}

		{
			// Test that backing up and restoring a table with just the array type
			// will remap types appropriately.
			sqlDB.Exec(t, `BACKUP TABLE d.s.t2 TO $1`, localFoo+"/2")
			sqlDB.Exec(t, `DROP TABLE d.s.t2`)
			sqlDB.Exec(t, `RESTORE TABLE d.s.t2 FROM $1`, localFoo+"/2")
			sqlDB.CheckQueryResults(t, `SELECT 'hello'::d.s.greeting, ARRAY['hello']::d.s.greeting[]`, [][]string{{"hello", "{hello}"}})
			sqlDB.CheckQueryResults(t, `SELECT * FROM d.s.t2 ORDER BY x`, [][]string{{"{hello}"}})
		}

		{
			// Create another database with compatible types.
			sqlDB.Exec(t, `CREATE DATABASE d2`)
			sqlDB.Exec(t, `CREATE SCHEMA d2.s`)
			sqlDB.Exec(t, `CREATE TYPE d2.s.greeting AS ENUM ('hello', 'howdy', 'hi')`)

			// Now restore t into this database. It should remap d.greeting to d2.greeting.
			sqlDB.Exec(t, `RESTORE TABLE d.s.t FROM $1 WITH into_db = 'd2'`, localFoo+"/1")
			// d.t should be added as a back reference to greeting.
			sqlDB.ExpectErr(t, `pq: cannot drop type "greeting" because other objects \(.*\) still depend on it`, `DROP TYPE d2.s.greeting`)
		}
	})

	t.Run("incremental", func(t *testing.T) {
		_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
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
			sqlDB.Exec(t, `BACKUP DATABASE d3 TO 'nodelocal://0/testd3/'`)

			// Now create a new type in that database.
			sqlDB.Exec(t, `CREATE TYPE d3.farewell AS ENUM ('bye', 'cya')`)

			// Perform an incremental backup, which should pick up the new type.
			sqlDB.Exec(t, `BACKUP DATABASE d3 TO 'nodelocal://0/testd3/'`)

			// Until #51362 lands we have to manually clean up this type before the
			// DROP DATABASE statement otherwise we'll leave behind an orphaned desc.
			sqlDB.Exec(t, `DROP TYPE d3.farewell`)

			// Now restore it.
			sqlDB.Exec(t, `DROP DATABASE d3`)
			sqlDB.Exec(t, `RESTORE DATABASE d3 FROM 'nodelocal://0/testd3/'`)

			// Check that we are able to use the type.
			sqlDB.Exec(t, `CREATE TABLE d3.t (x d3.farewell)`)
			sqlDB.Exec(t, `DROP TABLE d3.t`)

			// If we drop the type and back up again, it should be gone.
			sqlDB.Exec(t, `DROP TYPE d3.farewell`)

			sqlDB.Exec(t, `BACKUP DATABASE d3 TO 'nodelocal://0/testd3_2/'`)
			sqlDB.Exec(t, `DROP DATABASE d3`)
			sqlDB.Exec(t, `RESTORE DATABASE d3 FROM 'nodelocal://0/testd3_2/'`)
			sqlDB.ExpectErr(t, `pq: type "d3.farewell" does not exist`, `CREATE TABLE d3.t (x d3.farewell)`)
		}
	})
}

func TestBackupRestoreDuringUserDefinedTypeChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name            string
		queries         []string
		succeedAfter    []string
		expectedSuccess []string
		errorAfter      []string
		expectedError   []string
	}{
		{
			"AddInStatement",
			[]string{"ALTER TYPE d.greeting ADD VALUE 'cheers'"},
			[]string{"SELECT 'cheers'::d.greeting"},
			[]string{"cheers"},
			nil,
			nil,
		},
		{
			"AddDropInSeparateStatements",
			[]string{
				"ALTER TYPE d.greeting DROP VALUE 'hi';",
				"ALTER TYPE d.greeting ADD VALUE 'cheers'",
			},
			[]string{"SELECT 'cheers'::d.greeting"},
			[]string{"cheers"},
			[]string{"SELECT 'hi'::d.greeting"},
			[]string{`invalid input value for enum greeting: "hi"`},
		},
		{
			"AddDropInStatementsAndTransaction",
			[]string{
				"BEGIN; ALTER TYPE d.greeting DROP VALUE 'hi'; ALTER TYPE d.greeting ADD VALUE 'cheers'; COMMIT;",
				"ALTER TYPE d.greeting ADD VALUE 'sup'",
				"ALTER TYPE d.greeting DROP VALUE 'hello'",
			},
			[]string{"SELECT 'cheers'::d.greeting", "SELECT 'sup'::d.greeting"},
			[]string{"cheers", "sup"},
			[]string{"SELECT 'hi'::d.greeting", "SELECT 'hello'::d.greeting"},
			[]string{
				`invalid input value for enum greeting: "hi"`,
				`invalid input value for enum greeting: "hello"`,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Protects numTypeChangesStarted and numTypeChangesFinished.
			var mu syncutil.Mutex
			numTypeChangesStarted := 0
			numTypeChangesFinished := 0
			typeChangesStarted := make(chan struct{})
			waitForBackup := make(chan struct{})
			typeChangesFinished := make(chan struct{})
			_, sqlDB, _, cleanupFn := backupRestoreTestSetupWithParams(t, singleNode, 0, InitManualReplication, base.TestClusterArgs{
				ServerArgs: base.TestServerArgs{
					Knobs: base.TestingKnobs{
						SQLTypeSchemaChanger: &sql.TypeSchemaChangerTestingKnobs{
							RunBeforeEnumMemberPromotion: func(context.Context) error {
								mu.Lock()
								if numTypeChangesStarted < len(tc.queries) {
									numTypeChangesStarted++
									if numTypeChangesStarted == len(tc.queries) {
										close(typeChangesStarted)
									}
									mu.Unlock()
									<-waitForBackup
								} else {
									mu.Unlock()
								}
								return nil
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

			// Start ALTER TYPE statement(s) that will block.
			for _, query := range tc.queries {
				go func(query string) {
					// Note we don't use sqlDB.Exec here because we can't Fatal from within a goroutine.
					if _, err := sqlDB.DB.ExecContext(context.Background(), query); err != nil {
						t.Error(err)
					}
					mu.Lock()
					numTypeChangesFinished++
					if numTypeChangesFinished == len(tc.queries) {
						close(typeChangesFinished)
					}
					mu.Unlock()
				}(query)
			}

			// Wait on the type changes to start.
			<-typeChangesStarted

			// Now create a backup while the type change job is blocked so that
			// greeting is backed up with some enum members in READ_ONLY state.
			sqlDB.Exec(t, `BACKUP DATABASE d TO 'nodelocal://0/test/'`)

			// Let the type change finish.
			close(waitForBackup)
			<-typeChangesFinished

			// Now drop the database and restore.
			sqlDB.Exec(t, `DROP DATABASE d`)
			sqlDB.Exec(t, `RESTORE DATABASE d FROM 'nodelocal://0/test/'`)

			// The type change job should be scheduled and finish. Note that we can't use
			// sqlDB.CheckQueryResultsRetry as it Fatal's upon an error. The case below
			// will error until the job completes.
			for i, query := range tc.succeedAfter {
				testutils.SucceedsSoon(t, func() error {
					_, err := sqlDB.DB.ExecContext(context.Background(), query)
					return err
				})
				sqlDB.CheckQueryResults(t, query, [][]string{{tc.expectedSuccess[i]}})
			}

			for i, query := range tc.errorAfter {
				testutils.SucceedsSoon(t, func() error {
					_, err := sqlDB.DB.ExecContext(context.Background(), query)
					if err == nil {
						return errors.New("expected error, found none")
					}
					if !testutils.IsError(err, tc.expectedError[i]) {
						return errors.Newf("expected error %q, found %v", tc.expectedError[i], pgerror.FullError(err))
					}
					return nil
				})
			}
		})
	}
}

func TestBackupRestoreCrossTableReferences(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 30
	const createStore = "CREATE DATABASE store"
	const createStoreStats = "CREATE DATABASE storestats"

	_, origDB, dir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
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
		_ = origDB.Exec(t, `BACKUP DATABASE store, storestats TO $1`, localFoo)
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
		db.Exec(t, `RESTORE store.* FROM $1`, localFoo)
		// Restore's Validate checks all the tables point to each other correctly.

		db.CheckQueryResults(t, `SHOW CONSTRAINTS FROM store.customers`, origCustomers)
		db.CheckQueryResults(t, `SHOW CONSTRAINTS FROM store.orders`, origOrders)
		db.CheckQueryResults(t, `SHOW CONSTRAINTS FROM store.receipts`, origReceipts)

		// FK validation on customers from receipts is preserved.
		db.ExpectErr(
			t, "update.*violates foreign key constraint \"receipts_dest_fkey\"",
			`UPDATE store.customers SET email = concat(id::string, 'nope')`,
		)

		// FK validation on customers from orders is preserved.
		db.ExpectErr(
			t, "update.*violates foreign key constraint \"orders_customerid_fkey\"",
			`UPDATE store.customers SET id = id * 1000`,
		)

		// FK validation of customer id is preserved.
		db.ExpectErr(
			t, "insert.*violates foreign key constraint \"orders_customerid_fkey\"",
			`INSERT INTO store.orders VALUES (999, NULL, 999)`,
		)

		// FK validation of self-FK is preserved.
		db.ExpectErr(
			t, "insert.*violates foreign key constraint \"receipts_reissue_fkey\"",
			`INSERT INTO store.receipts VALUES (1, 999, NULL, NULL)`,
		)
	})

	t.Run("restore customers to new cluster", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.Background())
		db := sqlutils.MakeSQLRunner(tc.Conns[0])
		db.Exec(t, createStore)
		db.Exec(t, `RESTORE store.customers, store.orders FROM $1`, localFoo)
		// Restore's Validate checks all the tables point to each other correctly.

		// FK validation on customers from orders is preserved.
		db.ExpectErr(
			t, "update.*violates foreign key constraint \"orders_customerid_fkey\"",
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
			`RESTORE store.orders FROM $1`, localFoo,
		)

		db.Exec(t, `RESTORE store.orders FROM $1 WITH OPTIONS (skip_missing_foreign_keys)`, localFoo)
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
		db.Exec(t, `RESTORE store.receipts FROM $1 WITH OPTIONS (skip_missing_foreign_keys)`, localFoo)
		// Restore's Validate checks all the tables point to each other correctly.

		// FK validation of orders and customer is gone.
		db.Exec(t, `INSERT INTO store.receipts VALUES (1, NULL, '987', 999)`)

		// FK validation of self-FK is preserved.
		db.ExpectErr(
			t, "insert.*violates foreign key constraint \"receipts_reissue_fkey\"",
			`INSERT INTO store.receipts VALUES (-1, 999, NULL, NULL)`,
		)
	})

	t.Run("restore receipts and customers to new cluster", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.Background())
		db := sqlutils.MakeSQLRunner(tc.Conns[0])
		db.Exec(t, createStore)
		db.Exec(t, `RESTORE store.receipts, store.customers FROM $1 WITH OPTIONS (skip_missing_foreign_keys)`, localFoo)
		// Restore's Validate checks all the tables point to each other correctly.

		// FK validation of orders is gone.
		db.Exec(t, `INSERT INTO store.receipts VALUES (1, NULL, '0', 999)`)

		// FK validation of customer email is preserved.
		db.ExpectErr(
			t, "nsert.*violates foreign key constraint \"receipts_dest_fkey\"",
			`INSERT INTO store.receipts VALUES (-1, NULL, '999', 999)`,
		)

		// FK validation on customers from receipts is preserved.
		db.ExpectErr(
			t, "delete.*violates foreign key constraint \"receipts_dest_fkey\"",
			`DELETE FROM store.customers`,
		)

		// FK validation of self-FK is preserved.
		db.ExpectErr(
			t, "insert.*violates foreign key constraint \"receipts_reissue_fkey\"",
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
			`RESTORE store.early_customers FROM $1`, localFoo,
		)
		db.Exec(t, `RESTORE store.early_customers, store.customers, store.orders FROM $1`, localFoo)
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
			`RESTORE DATABASE storestats FROM $1`, localFoo,
		)

		db.Exec(t, createStore)
		db.Exec(t, createStoreStats)

		db.ExpectErr(
			t, `cannot restore view "ordercounts" without restoring referenced table`,
			`RESTORE storestats.ordercounts, store.customers FROM $1`, localFoo,
		)

		db.Exec(t, `RESTORE store.customers, storestats.ordercounts, store.orders FROM $1`, localFoo)

		// we want to observe just the view-related errors, not fk errors below.
		db.Exec(t, `ALTER TABLE store.orders DROP CONSTRAINT orders_customerid_fkey`)

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
		db.Exec(t, `RESTORE store.* FROM $1 WITH into_db = 'otherstore'`, localFoo)
		// we want to observe just the view-related errors, not fk errors below.
		db.Exec(t, `ALTER TABLE otherstore.orders DROP CONSTRAINT orders_customerid_fkey`)
		db.Exec(t, `DROP TABLE otherstore.receipts`)

		db.ExpectErr(
			t, `cannot drop relation "customers" because view "unused_view" depends on it`,
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

		db.Exec(t, `RESTORE DATABASE storestats from $1 WITH OPTIONS (skip_missing_views)`, localFoo)
		db.Exec(t, `RESTORE storestats.ordercounts from $1 WITH OPTIONS (skip_missing_views)`, localFoo)
		// Ensure that the views were not restored since they are missing the tables they reference.
		db.CheckQueryResults(t, `USE storestats; SHOW TABLES;`, [][]string{})

		// Need to specify into_db otherwise the restore gives error:
		//  a database named "store" needs to exist to restore schema "public".
		db.Exec(t, `RESTORE store.early_customers, store.referencing_early_customers from $1 WITH OPTIONS (skip_missing_views, into_db='storestats')`, localFoo)
		// Ensure that the views were not restored since they are missing the tables they reference.
		db.CheckQueryResults(t, `SHOW TABLES;`, [][]string{})

		// Test that views with valid dependencies are restored

		db.Exec(t, `RESTORE DATABASE store from $1 WITH OPTIONS (skip_missing_views)`, localFoo)
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
		db.Exec(t, `RESTORE storestats.ordercounts, store.customers from $1 WITH OPTIONS (skip_missing_views, into_db='store2')`, localFoo)
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

	tc, sqlDB, dir, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
	defer cleanupFn()
	args := base.TestServerArgs{ExternalIODir: dir}
	rng, _ := randutil.NewTestRand()

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

	_, sqlDB, dir, cleanupFn := backupRestoreTestSetup(t, multiNode, 0, InitManualReplication)
	defer cleanupFn()
	args := base.TestServerArgs{ExternalIODir: dir}
	rng, _ := randutil.NewTestRand()

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
	rng, _ := randutil.NewTestRand()

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
	skip.WithIssue(t, 58211, "flaky test")
	defer log.Scope(t).Close(t)

	const rows = 10
	const numBackgroundTasks = multiNode

	ctx := context.Background()
	tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, multiNode, rows, InitManualReplication)
	defer cleanupFn()

	bgActivity := make(chan struct{})
	// allowErrors is used as an atomic bool to tell bg workers when to allow
	// errors, between dropping and restoring the table they are using.
	var allowErrors int32
	for task := 0; task < numBackgroundTasks; task++ {
		taskNum := task
		_ = tc.Stopper().RunAsyncTask(ctx, "bg-task", func(context.Context) {
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
	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, localFoo)

	// Drop the table and restore from backup and check our invariant.
	atomic.StoreInt32(&allowErrors, 1)
	sqlDB.Exec(t, `DROP TABLE data.bank`)
	sqlDB.Exec(t, `RESTORE data.* FROM $1`, localFoo)
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
	ctx := context.Background()
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts, InitManualReplication)
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
	ctx := context.Background()
	tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	_, err := tc.Servers[0].StartTenant(ctx, base.TestTenantArgs{TenantID: roachpb.MakeTenantID(10)})
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

	ctx := context.Background()
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
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

	beforeDir := localFoo + `/beforeTs`
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE data TO '%s' AS OF SYSTEM TIME %s`, beforeDir, beforeTs))
	equalDir := localFoo + `/equalTs`
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
	ctx := context.Background()
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
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
	sqlDB.Exec(t, `CREATE TABLE data.sometable AS SELECT * FROM other.sometable`)
	sqlDB.Exec(t, `DROP TABLE other.sometable`)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[4])

	sqlDB.Exec(t, `INSERT INTO data.sometable VALUES (2, 2), (4, 5), (6, 3)`)
	sqlDB.Exec(t, `ALTER TABLE data.bank ADD COLUMN points_balance INT DEFAULT 50`)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[5])

	sqlDB.Exec(t, `TRUNCATE TABLE data.bank`)
	sqlDB.Exec(t, `TRUNCATE TABLE data.bank`)
	sqlDB.Exec(t, `TRUNCATE TABLE data.bank`)
	sqlDB.Exec(t, `CREATE TABLE other.sometable AS SELECT * FROM data.sometable`)
	sqlDB.Exec(t, `DROP TABLE data.sometable`)
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
	ctx := context.Background()
	tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()
	const dir = "nodelocal://0/"
	preGC := tree.TimestampToDecimalDatum(tc.Server(0).Clock().Now()).String()

	gcr := roachpb.GCRequest{
		// Bogus span to make it a valid request.
		RequestHeader: roachpb.RequestHeader{
			Key:    keys.SystemSQLCodec.TablePrefix(bootstrap.TestingUserDescID(0)),
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

	t.Run("restore-pre-gc-aost", func(t *testing.T) {
		backupPath := dir + "/tbl-before-gc"
		_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
		defer cleanupFn()

		sqlDB.Exec(t, "CREATE DATABASE db")
		sqlDB.Exec(t, "CREATE TABLE db.a (k int, v string)")
		sqlDB.Exec(t, `BACKUP DATABASE db TO $1 WITH revision_history`, backupPath)

		sqlDB.Exec(t, "DROP TABLE db.a")
		sqlDB.ExpectErr(
			t, `pq: failed to resolve targets in the BACKUP location specified by the RESTORE stmt, use SHOW BACKUP to find correct targets: table "db.a" does not exist, or invalid RESTORE timestamp: supplied backups do not cover requested time`,
			fmt.Sprintf(`RESTORE TABLE db.a FROM $1 AS OF SYSTEM TIME %s`, preGC),
			backupPath,
		)

		sqlDB.Exec(t, "DROP DATABASE db")
		sqlDB.ExpectErr(
			t, `pq: failed to resolve targets in the BACKUP location specified by the RESTORE stmt, use SHOW BACKUP to find correct targets: database "db" does not exist, or invalid RESTORE timestamp: supplied backups do not cover requested time`,
			fmt.Sprintf(`RESTORE DATABASE db FROM $1 AS OF SYSTEM TIME %s`, preGC),
			backupPath,
		)
	})
}

func TestAsOfSystemTimeOnRestoredData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 10
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()
	sqlDB.Exec(t, `BACKUP data.* To $1`, localFoo)

	sqlDB.Exec(t, `DROP TABLE data.bank`)

	var beforeTs string
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&beforeTs)
	sqlDB.Exec(t, `RESTORE data.* FROM $1`, localFoo)
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
	_, sqlDB, dir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()
	dir = filepath.Join(dir, "foo")

	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, localFoo)

	var backupManifest BackupManifest
	{
		backupManifestBytes, err := ioutil.ReadFile(filepath.Join(dir, backupManifestName))
		if err != nil {
			t.Fatalf("%+v", err)
		}
		if isGZipped(backupManifestBytes) {
			backupManifestBytes, err = decompressData(context.Background(), nil, backupManifestBytes)
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
	// mess with some bytes.
	if _, err := f.Seek(-65, io.SeekEnd); err != nil {
		t.Fatalf("%+v", err)
	}
	if _, err := f.Write([]byte{'1', '2', '3'}); err != nil {
		t.Fatalf("%+v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("%+v", err)
	}

	sqlDB.Exec(t, `DROP TABLE data.bank`)
	sqlDB.ExpectErr(t, "checksum mismatch", `RESTORE data.* FROM $1`, localFoo)
}

func TestTimestampMismatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const numAccounts = 1

	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	sqlDB.Exec(t, `CREATE TABLE data.t2 (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO data.t2 VALUES (1)`)

	fullBackup := localFoo + "/0"
	incrementalT1FromFull := localFoo + "/1"
	incrementalT2FromT1 := localFoo + "/2"
	incrementalT3FromT1OneTable := localFoo + "/3"

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
			localFoo, incrementalT1FromFull,
		)

		// Missing an intermediate incremental backup.
		sqlDB.ExpectErr(
			t, "backups listed out of order",
			`BACKUP DATABASE data TO $1 INCREMENTAL FROM $2, $3`,
			localFoo, fullBackup, incrementalT2FromT1,
		)

		// Backups specified out of order.
		sqlDB.ExpectErr(
			t, "out of order",
			`BACKUP DATABASE data TO $1 INCREMENTAL FROM $2, $3`,
			localFoo, incrementalT1FromFull, fullBackup,
		)

		// Missing data for one table in the most recent backup.
		sqlDB.ExpectErr(
			t, "previous backup does not contain table",
			`BACKUP DATABASE data TO $1 INCREMENTAL FROM $2, $3`,
			localFoo, fullBackup, incrementalT3FromT1OneTable,
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
		"AWS_ACCESS_KEY_ID":     amazon.AWSAccessKeyParam,
		"AWS_SECRET_ACCESS_KEY": amazon.AWSSecretParam,
		regionEnvVariable:       amazon.KMSRegionParam,
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
	q.Set(cloud.AuthParam, cloud.AuthParamSpecified)
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
		t.Run(tc.name, func(t *testing.T) {
			var encryptionOption string
			var incorrectEncryptionOption string
			if tc.useKMS {
				correctKMSURI, incorrectKeyARNURI := getAWSKMSURI(t, regionEnvVariable, keyIDEnvVariable)
				encryptionOption = fmt.Sprintf("kms='%s'", correctKMSURI)
				incorrectEncryptionOption = fmt.Sprintf("kms='%s'", incorrectKeyARNURI)
			} else {
				encryptionOption = "encryption_passphrase = 'abcdefg'"
				incorrectEncryptionOption = "encryption_passphrase = 'wrongpassphrase'"
			}
			ctx := context.Background()
			_, sqlDB, rawDir, cleanupFn := backupRestoreTestSetup(t, multiNode, 3, InitManualReplication)
			defer cleanupFn()

			setupBackupEncryptedTest(ctx, t, sqlDB)

			// Full cluster-backup to capture all possible metadata.
			backupLoc1 := localFoo + "/x?COCKROACH_LOCALITY=default"
			backupLoc2 := localFoo + "/x2?COCKROACH_LOCALITY=" + url.QueryEscape("dc=dc1")
			backupLoc1inc := localFoo + "/inc1/x?COCKROACH_LOCALITY=default"
			backupLoc2inc := localFoo + "/inc1/x2?COCKROACH_LOCALITY=" + url.QueryEscape("dc=dc1")

			plainBackupLoc1 := localFoo + "/cleartext?COCKROACH_LOCALITY=default"
			plainBackupLoc2 := localFoo + "/cleartext?COCKROACH_LOCALITY=" + url.QueryEscape("dc=dc1")

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
		ctx := context.Background()
		_, sqlDB, rawDir, cleanupFn := backupRestoreTestSetup(t, multiNode, 3, InitManualReplication)
		defer cleanupFn()

		setupBackupEncryptedTest(ctx, t, sqlDB)

		// Full cluster-backup to capture all possible metadata.
		backupLoc1 := localFoo + "/x?COCKROACH_LOCALITY=default"
		backupLoc2 := localFoo + "/x2?COCKROACH_LOCALITY=" + url.QueryEscape("dc=dc1")

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
	q.Add(cloud.AuthParam, cloud.AuthParamImplicit)
	q.Add(amazon.KMSRegionParam, "blah")

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
	_, sqlDB, dir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()
	args := base.TestServerArgs{ExternalIODir: dir}

	rootOnly := sqlDB.QueryStr(t, `SHOW GRANTS ON data.bank`)

	sqlDB.Exec(t, `CREATE USER someone`)
	sqlDB.Exec(t, `GRANT SELECT, INSERT, UPDATE, DELETE ON data.bank TO someone`)

	sqlDB.Exec(t, `CREATE DATABASE data2`)
	// Explicitly don't restore grants when just restoring a database since we
	// cannot ensure that the same users exist in the restoring cluster.
	data2Grants := sqlDB.QueryStr(t, `SHOW GRANTS ON DATABASE data2`)
	sqlDB.Exec(t, `GRANT CONNECT, CREATE, DROP, GRANT, ZONECONFIG ON DATABASE data2 TO someone`)

	withGrants := sqlDB.QueryStr(t, `SHOW GRANTS ON data.bank`)

	sqlDB.Exec(t, `BACKUP DATABASE data, data2 TO $1`, localFoo)
	sqlDB.Exec(t, `DROP TABLE data.bank`)

	t.Run("into fresh db", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.Background())
		sqlDBRestore := sqlutils.MakeSQLRunner(tc.Conns[0])
		sqlDBRestore.Exec(t, `CREATE DATABASE data`)
		sqlDBRestore.Exec(t, `RESTORE data.bank FROM $1`, localFoo)
		sqlDBRestore.CheckQueryResults(t, `SHOW GRANTS ON data.bank`, rootOnly)
	})

	t.Run("into db with added grants", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.Background())
		sqlDBRestore := sqlutils.MakeSQLRunner(tc.Conns[0])
		sqlDBRestore.Exec(t, `CREATE DATABASE data`)
		sqlDBRestore.Exec(t, `CREATE USER someone`)
		sqlDBRestore.Exec(t, `USE data`)
		sqlDBRestore.Exec(t, `ALTER DEFAULT PRIVILEGES GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO someone`)
		sqlDBRestore.Exec(t, `RESTORE data.bank FROM $1`, localFoo)
		sqlDBRestore.CheckQueryResults(t, `SHOW GRANTS ON data.bank`, withGrants)
	})

	t.Run("into db on db grants", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.Background())
		sqlDBRestore := sqlutils.MakeSQLRunner(tc.Conns[0])
		sqlDBRestore.Exec(t, `CREATE USER someone`)
		sqlDBRestore.Exec(t, `RESTORE DATABASE data2 FROM $1`, localFoo)
		sqlDBRestore.CheckQueryResults(t, `SHOW GRANTS ON DATABASE data2`, data2Grants)
	})
}

func TestRestoreInto(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, localFoo)

	restoreStmt := fmt.Sprintf(`RESTORE data.bank FROM '%s' WITH into_db = 'data 2'`, localFoo)

	sqlDB.ExpectErr(t, "a database named \"data 2\" needs to exist", restoreStmt)

	sqlDB.Exec(t, `CREATE DATABASE "data 2"`)
	sqlDB.Exec(t, restoreStmt)

	expected := sqlDB.QueryStr(t, `SELECT * FROM data.bank`)
	sqlDB.CheckQueryResults(t, `SELECT * FROM "data 2".bank`, expected)
}

func TestRestoreDatabaseVersusTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	tc, origDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
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

	origDB.Exec(t, `BACKUP DATABASE data, d2, d3, d4 TO $1`, localFoo)
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
		sqlDB.Exec(t, `RESTORE DATABASE data, d2, d3 FROM $1`, localFoo)
	})

	t.Run("db-exists", func(t *testing.T) {
		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tcRestore.Stopper().Stop(context.Background())
		sqlDB := sqlutils.MakeSQLRunner(tcRestore.Conns[0])

		sqlDB.Exec(t, `CREATE DATABASE data`)
		sqlDB.ExpectErr(t, "already exists", `RESTORE DATABASE data FROM $1`, localFoo)
	})

	t.Run("tables", func(t *testing.T) {
		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tcRestore.Stopper().Stop(context.Background())
		sqlDB := sqlutils.MakeSQLRunner(tcRestore.Conns[0])

		sqlDB.Exec(t, `CREATE DATABASE data`)
		sqlDB.Exec(t, `RESTORE data.* FROM $1`, localFoo)
	})

	t.Run("tables-needs-db", func(t *testing.T) {
		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tcRestore.Stopper().Stop(context.Background())
		sqlDB := sqlutils.MakeSQLRunner(tcRestore.Conns[0])

		sqlDB.ExpectErr(t, "needs to exist", `RESTORE data.*, d4.* FROM $1`, localFoo)
	})

	t.Run("into_db", func(t *testing.T) {
		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tcRestore.Stopper().Stop(context.Background())
		sqlDB := sqlutils.MakeSQLRunner(tcRestore.Conns[0])

		sqlDB.ExpectErr(
			t, `cannot use "into_db"`,
			`RESTORE DATABASE data FROM $1 WITH into_db = 'other'`, localFoo,
		)
	})
}

func TestBackupAzureAccountName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
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
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	fullBackupDir := localFoo + "/full"
	sqlDB.Exec(t, `BACKUP data.* TO $1`, fullBackupDir)

	sqlDB.Exec(t, `UPDATE data.bank SET balance = 2`)

	incBackupDir := localFoo + "/inc"
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
		recoveryDir := localFoo + "/new-backup"
		sqlDB.Exec(t,
			fmt.Sprintf(`BACKUP data.* TO $1 AS OF SYSTEM TIME '%s'`, beforeBadThingTs),
			recoveryDir,
		)
		sqlDB.Exec(t, `CREATE DATABASE newbackup`)
		sqlDB.Exec(t, `RESTORE data.* FROM $1 WITH into_db=newbackup`, recoveryDir)

		// Some manual reconciliation of the data in data.bank and
		// newbackup.bank could be done here by the operator.

		sqlDB.Exec(t, `DROP TABLE data.bank`)
		sqlDB.Exec(t, `CREATE TABLE data.bank AS SELECT * FROM newbackup.bank`)
		sqlDB.Exec(t, `DROP DATABASE newbackup`)
		sqlDB.CheckQueryResults(t, `SELECT * FROM data.bank ORDER BY id`, beforeBadThingData)
	})

	// If there is a recent BACKUP (either full or incremental), then it will
	// likely be faster to make a BACKUP that is incremental from it and RESTORE
	// using that. Everything else works the same as above.
	t.Run("recovery=inc-backup", func(t *testing.T) {
		sqlDB = sqlutils.MakeSQLRunner(sqlDB.DB)
		recoveryDir := localFoo + "/inc-backup"
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
		sqlDB.Exec(t, `CREATE TABLE data.bank AS SELECT * FROM incbackup.bank`)
		sqlDB.Exec(t, `DROP DATABASE incbackup`)
		sqlDB.CheckQueryResults(t, `SELECT * FROM data.bank ORDER BY id`, beforeBadThingData)
	})
}

func TestBackupRestoreDropDB(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	sqlDB.Exec(t, `DROP DATABASE data`)
	sqlDB.Exec(t, `CREATE DATABASE data`)
	sqlDB.Exec(t, `CREATE TABLE data.bank (i int)`)
	sqlDB.Exec(t, `INSERT INTO data.bank VALUES (1)`)

	sqlDB.Exec(t, "BACKUP DATABASE data TO $1", localFoo)
	sqlDB.Exec(t, "CREATE DATABASE data2")
	sqlDB.Exec(t, "RESTORE data.* FROM $1 WITH OPTIONS (into_db='data2')", localFoo)

	expected := sqlDB.QueryStr(t, `SELECT * FROM data.bank`)
	sqlDB.CheckQueryResults(t, `SELECT * FROM data2.bank`, expected)
}

func TestBackupRestoreDropTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	sqlDB.Exec(t, `DROP TABLE data.bank`)
	sqlDB.Exec(t, `
		CREATE TABLE data.bank (i int);
		INSERT INTO data.bank VALUES (1);
	`)

	sqlDB.Exec(t, "BACKUP DATABASE data TO $1", localFoo)
	sqlDB.Exec(t, "CREATE DATABASE data2")
	sqlDB.Exec(t, "RESTORE data.* FROM $1 WITH OPTIONS (into_db='data2')", localFoo)

	expected := sqlDB.QueryStr(t, `SELECT * FROM data.bank`)
	sqlDB.CheckQueryResults(t, `SELECT * FROM data2.bank`, expected)
}

func TestBackupRestoreIncrementalAddTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()
	sqlDB.Exec(t, `CREATE DATABASE data2`)
	sqlDB.Exec(t, `CREATE TABLE data.t (s string PRIMARY KEY)`)
	full, inc := localFoo+"/full", localFoo+"/inc"

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
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()
	sqlDB.Exec(t, `CREATE DATABASE data2`)
	sqlDB.Exec(t, `CREATE TABLE data.t (s string PRIMARY KEY)`)
	full, inc := localFoo+"/full", localFoo+"/inc"

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
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()
	sqlDB.Exec(t, `CREATE TABLE data.t (s string PRIMARY KEY)`)
	full, inc := localFoo+"/full", localFoo+"/inc"

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
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()
	sqlDB.Exec(t, `CREATE TABLE data.t (s string PRIMARY KEY)`)
	full, inc := localFoo+"/full", localFoo+"/inc"

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
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	elsewhere := "nodelocal://0/../../blah"

	sqlDB.Exec(t, `BACKUP data.bank TO $1`, localFoo)
	sqlDB.ExpectErr(
		t, "local file access to paths outside of external-io-dir is not allowed",
		`BACKUP data.bank TO $1`, elsewhere,
	)

	sqlDB.Exec(t, `DROP TABLE data.bank`)

	sqlDB.Exec(t, `RESTORE data.bank FROM $1`, localFoo)
	sqlDB.ExpectErr(
		t, "local file access to paths outside of external-io-dir is not allowed",
		`RESTORE data.bank FROM $1`, elsewhere,
	)
}

func waitForSuccessfulJob(t *testing.T, tc *testcluster.TestCluster, id jobspb.JobID) {
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
	ctx := context.Background()
	tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	db := sqlDB.DB.(*gosql.DB)

	// running backup under transaction requires DETACHED.
	var jobID jobspb.JobID
	err := crdb.ExecuteTx(ctx, db, nil /* txopts */, func(tx *gosql.Tx) error {
		return tx.QueryRow(`BACKUP DATABASE data TO $1`, localFoo).Scan(&jobID)
	})
	require.True(t, testutils.IsError(err,
		"BACKUP cannot be used inside a transaction without DETACHED option"))

	// Okay to run DETACHED backup, even w/out explicit transaction.
	sqlDB.QueryRow(t, `BACKUP DATABASE data TO $1 WITH DETACHED`, localFoo).Scan(&jobID)
	waitForSuccessfulJob(t, tc, jobID)

	// Backup again, under explicit transaction.
	err = crdb.ExecuteTx(ctx, db, nil /* txopts */, func(tx *gosql.Tx) error {
		return tx.QueryRow(`BACKUP DATABASE data TO $1 WITH DETACHED`, localFoo+"/1").Scan(&jobID)
	})
	require.NoError(t, err)
	waitForSuccessfulJob(t, tc, jobID)

	// Backup again under transaction, but this time abort the transaction.
	// No new jobs should have been created.
	allJobsQuery := "SELECT job_id FROM [SHOW JOBS]"
	allJobs := sqlDB.QueryStr(t, allJobsQuery)
	tx, err := db.Begin()
	require.NoError(t, err)
	err = crdb.Execute(func() error {
		return tx.QueryRow(`BACKUP DATABASE data TO $1 WITH DETACHED`, localFoo+"/2").Scan(&jobID)
	})
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	sqlDB.CheckQueryResults(t, allJobsQuery, allJobs)

	// Ensure that we can backup again to the same location as the backup that was
	// rolledback.
	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, localFoo+"/2")
}

func TestDetachedRestore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	ctx := context.Background()
	tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	db := sqlDB.DB.(*gosql.DB)

	// Run a BACKUP.
	sqlDB.Exec(t, `CREATE TABLE data.t (id INT, name STRING)`)
	sqlDB.Exec(t, `INSERT INTO data.t VALUES (1, 'foo'), (2, 'bar')`)
	sqlDB.Exec(t, `BACKUP TABLE data.t TO $1`, localFoo)
	sqlDB.Exec(t, `CREATE DATABASE test`)

	// Running RESTORE under transaction requires DETACHED.
	var jobID jobspb.JobID
	err := crdb.ExecuteTx(ctx, db, nil /* txopts */, func(tx *gosql.Tx) error {
		return tx.QueryRow(`RESTORE TABLE t FROM $1 WITH INTO_DB=test`, localFoo).Scan(&jobID)
	})
	require.True(t, testutils.IsError(err,
		"RESTORE cannot be used inside a transaction without DETACHED option"))

	// Okay to run DETACHED RESTORE, even w/out explicit transaction.
	sqlDB.QueryRow(t, `RESTORE TABLE t FROM $1 WITH DETACHED, INTO_DB=test`,
		localFoo).Scan(&jobID)
	waitForSuccessfulJob(t, tc, jobID)
	sqlDB.Exec(t, `DROP TABLE test.t`)

	// RESTORE again, under explicit transaction.
	err = crdb.ExecuteTx(ctx, db, nil /* txopts */, func(tx *gosql.Tx) error {
		return tx.QueryRow(`RESTORE TABLE t FROM $1 WITH DETACHED, INTO_DB=test`, localFoo).Scan(&jobID)
	})
	require.NoError(t, err)
	waitForSuccessfulJob(t, tc, jobID)
	sqlDB.Exec(t, `DROP TABLE test.t`)

	// RESTORE again under transaction, but this time abort the transaction.
	// No new jobs should have been created.
	allJobsQuery := "SELECT job_id FROM [SHOW JOBS]"
	allJobs := sqlDB.QueryStr(t, allJobsQuery)
	tx, err := db.Begin()
	require.NoError(t, err)
	err = crdb.Execute(func() error {
		return tx.QueryRow(`RESTORE TABLE t FROM $1 WITH DETACHED, INTO_DB=test`, localFoo).Scan(&jobID)
	})
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	sqlDB.CheckQueryResults(t, allJobsQuery, allJobs)
}

func TestBackupRestoreSequence(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const numAccounts = 1
	_, origDB, dir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()
	args := base.TestServerArgs{ExternalIODir: dir}

	backupLoc := localFoo

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

		// Verify that we can keep inserting into the table, without violating a uniqueness constraint.
		newDB.Exec(t, `INSERT INTO data.t (v) VALUES ('bar')`)

		// Verify that sequence <=> table dependencies are still in place.
		newDB.ExpectErr(
			t, "pq: cannot drop sequence t_id_seq because other objects depend on it",
			`DROP SEQUENCE t_id_seq`,
		)

		// Check that we can rename the sequence, and the table is fine.
		newDB.Exec(t, `ALTER SEQUENCE t_id_seq RENAME TO t_id_seq2`)
		newDB.Exec(t, `INSERT INTO data.t (v) VALUES ('qux')`)
		newDB.CheckQueryResults(t, `SELECT last_value FROM t_id_seq2`, [][]string{{"5"}})
		newDB.CheckQueryResults(t, `SELECT * FROM t`, [][]string{
			{"1", "foo"},
			{"2", "bar"},
			{"3", "baz"},
			{"4", "bar"},
			{"5", "qux"},
		})

		// Verify that sequence <=> table dependencies are still in place.
		newDB.ExpectErr(
			t, "pq: cannot drop sequence t_id_seq2 because other objects depend on it",
			`DROP SEQUENCE t_id_seq2`,
		)
	})

	t.Run("restore just the table to a new cluster", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(context.Background())
		newDB := sqlutils.MakeSQLRunner(tc.Conns[0])

		newDB.Exec(t, `CREATE DATABASE data`)
		newDB.Exec(t, `USE data`)

		newDB.ExpectErr(
			t, "pq: cannot restore table \"t\" without referenced sequence \\d+ \\(or \"skip_missing_sequences\" option\\)",
			`RESTORE TABLE t FROM $1`, localFoo,
		)

		newDB.Exec(t, `RESTORE TABLE t FROM $1 WITH OPTIONS (skip_missing_sequences)`, localFoo)

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

func TestBackupRestoreSequencesInViews(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Test backing up and restoring a database with views referencing sequences.
	t.Run("database", func(t *testing.T) {
		_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
		defer cleanupFn()

		sqlDB.Exec(t, `CREATE DATABASE d`)
		sqlDB.Exec(t, `USE d`)
		sqlDB.Exec(t, `CREATE SEQUENCE s`)
		sqlDB.Exec(t, `CREATE VIEW v AS SELECT k FROM (SELECT nextval('s') AS k)`)

		// Backup the database.
		sqlDB.Exec(t, `BACKUP DATABASE d TO 'nodelocal://0/test/'`)

		// Drop the database and restore into it.
		sqlDB.Exec(t, `DROP DATABASE d`)
		sqlDB.Exec(t, `RESTORE DATABASE d FROM 'nodelocal://0/test/'`)

		// Check that the view is not corrupted.
		sqlDB.CheckQueryResults(t, `SELECT * FROM d.v`, [][]string{{"1"}})

		// Check that the sequence can still be renamed.
		sqlDB.Exec(t, `ALTER SEQUENCE d.s RENAME TO d.s2`)

		// Check that after renaming, the view is still fine, and reflects the rename.
		sqlDB.CheckQueryResults(t, `SELECT * FROM d.v`, [][]string{{"2"}})
		sqlDB.CheckQueryResults(t, `SHOW CREATE VIEW d.v`, [][]string{{
			"d.public.v", "CREATE VIEW public.v (\n\tk\n) AS " +
				"SELECT k FROM (SELECT nextval('public.s2'::REGCLASS) AS k)",
		}})

		// Test that references are still tracked.
		sqlDB.ExpectErr(t, `pq: cannot drop sequence s2 because other objects depend on it`, `DROP SEQUENCE d.s2`)
	})

	// Test backing up and restoring both view and sequence.
	t.Run("restore view and sequence", func(t *testing.T) {
		_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
		defer cleanupFn()

		sqlDB.Exec(t, `CREATE DATABASE d`)
		sqlDB.Exec(t, `USE d`)
		sqlDB.Exec(t, `CREATE SEQUENCE s`)
		sqlDB.Exec(t, `CREATE VIEW v AS (SELECT k FROM (SELECT nextval('s') AS k))`)

		// Backup v and s.
		sqlDB.Exec(t, `BACKUP TABLE v, s TO 'nodelocal://0/test/'`)
		// Drop v and s.
		sqlDB.Exec(t, `DROP VIEW v`)
		sqlDB.Exec(t, `DROP SEQUENCE s`)
		// Restore v and s.
		sqlDB.Exec(t, `RESTORE TABLE s, v FROM 'nodelocal://0/test/'`)
		sqlDB.CheckQueryResults(t, `SHOW CREATE VIEW d.v`, [][]string{{
			"d.public.v", "CREATE VIEW public.v (\n\tk\n) AS " +
				"(SELECT k FROM (SELECT nextval('public.s'::REGCLASS) AS k))",
		}})

		// Check that v is not corrupted.
		sqlDB.CheckQueryResults(t, `SELECT * FROM v`, [][]string{{"1"}})

		// Check that s can be renamed and v reflects the change.
		sqlDB.Exec(t, `ALTER SEQUENCE s RENAME TO s2`)
		sqlDB.CheckQueryResults(t, `SHOW CREATE VIEW d.v`, [][]string{{
			"d.public.v", "CREATE VIEW public.v (\n\tk\n) AS " +
				"(SELECT k FROM (SELECT nextval('public.s2'::REGCLASS) AS k))",
		}})
		sqlDB.CheckQueryResults(t, `SELECT * FROM v`, [][]string{{"2"}})

		// Test that references are still tracked.
		sqlDB.ExpectErr(t, `pq: cannot drop sequence s2 because other objects depend on it`, `DROP SEQUENCE s2`)
	})

	// Test backing up and restoring just the view.
	t.Run("restore just the view", func(t *testing.T) {
		_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
		defer cleanupFn()

		sqlDB.Exec(t, `CREATE DATABASE d`)
		sqlDB.Exec(t, `USE d`)
		sqlDB.Exec(t, `CREATE SEQUENCE s`)
		sqlDB.Exec(t, `CREATE VIEW v AS (SELECT k FROM (SELECT nextval('s') AS k))`)

		// Backup v and drop.
		sqlDB.Exec(t, `BACKUP TABLE v TO 'nodelocal://0/test/'`)
		sqlDB.Exec(t, `DROP VIEW v`)
		// Restore v.
		sqlDB.ExpectErr(
			t, "pq: cannot restore view \"v\" without restoring referenced table \\(or \"skip_missing_views\" option\\)",
			`RESTORE TABLE v FROM 'nodelocal://0/test/'`,
		)
	})
}

func TestBackupRestoreSequenceOwnership(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, origDB, dir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()
	args := base.TestServerArgs{ExternalIODir: dir}

	// Setup for sequence ownership backup/restore tests in the same database.
	backupLoc := localFoo + `/d`
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

		tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "d", "t")
		seqDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "d", "seq")

		require.True(t, seqDesc.GetSequenceOpts().HasOwner(), "no sequence owner after restore")
		require.Equal(t, tableDesc.GetID(), seqDesc.GetSequenceOpts().SequenceOwner.OwnerTableID,
			"unexpected table is sequence owner after restore",
		)
		require.Equal(t, tableDesc.PublicColumns()[0].GetID(), seqDesc.GetSequenceOpts().SequenceOwner.OwnerColumnID,
			"unexpected column is sequence owner after restore",
		)
		require.Equal(t, 1, tableDesc.PublicColumns()[0].NumOwnsSequences(),
			"unexpected number of sequences owned by d.t after restore",
		)
		require.Equal(t, seqDesc.GetID(), tableDesc.PublicColumns()[0].GetOwnsSequenceID(0),
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
		seqDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "d", "seq")
		require.False(t, seqDesc.GetSequenceOpts().HasOwner(), "unexpected owner of restored sequence.")
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

			tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "d", "t")

			require.Equal(t, 0, tableDesc.PublicColumns()[0].NumOwnsSequences(),
				"expected restored table to own 0 sequences",
			)

			newDB.ExpectErr(t, `pq: cannot restore sequence "seq" without referenced owner table`,
				`RESTORE TABLE seq FROM $1`, backupLoc)
			newDB.Exec(t, `RESTORE TABLE seq FROM $1 WITH skip_missing_sequence_owners`, backupLoc)

			seqDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "d", "seq")
			require.False(t, seqDesc.GetSequenceOpts().HasOwner(), "unexpected sequence owner after restore")
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

		tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "restore_db", "t")
		seqDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "restore_db", "seq")

		require.True(t, seqDesc.GetSequenceOpts().HasOwner(), "no sequence owner after restore")
		require.Equal(t, tableDesc.GetID(), seqDesc.GetSequenceOpts().SequenceOwner.OwnerTableID,
			"unexpected table is sequence owner after restore",
		)
		require.Equal(t, tableDesc.PublicColumns()[0].GetID(), seqDesc.GetSequenceOpts().SequenceOwner.OwnerColumnID,
			"unexpected column is sequence owner after restore",
		)
		require.Equal(t, 1, tableDesc.PublicColumns()[0].NumOwnsSequences(),
			"unexpected number of sequences owned by d.t after restore",
		)
		require.Equal(t, seqDesc.GetID(), tableDesc.PublicColumns()[0].GetOwnsSequenceID(0),
			"unexpected ID of sequence owned by table d.t after restore",
		)
	})

	// Setup for cross-database ownership backup-restore tests.
	backupLocD2D3 := localFoo + `/d2d3`

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

			tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "d2", "t")
			require.Equal(t, 0, tableDesc.PublicColumns()[0].NumOwnsSequences(),
				"expected restored table to own no sequences.",
			)

			newDB.ExpectErr(t, "pq: cannot restore sequence \"seq\" without referenced owner|"+
				"pq: cannot restore table \"t\" without referenced sequence",
				`RESTORE DATABASE d3 FROM $1`, backupLocD2D3)
			newDB.Exec(t, `RESTORE DATABASE d3 FROM $1 WITH skip_missing_sequence_owners`, backupLocD2D3)

			seqDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "d3", "seq")
			require.False(t, seqDesc.GetSequenceOpts().HasOwner(), "unexpected sequence owner after restore")

			// Sequence dependencies inside the database should still be preserved.
			sd := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "d3", "seq2")
			td := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "d3", "t")

			require.True(t, sd.GetSequenceOpts().HasOwner(), "no owner found for seq2")
			require.Equal(t, td.GetID(), sd.GetSequenceOpts().SequenceOwner.OwnerTableID,
				"unexpected table owner for sequence seq2 after restore",
			)
			require.Equal(t, td.PublicColumns()[0].GetID(), sd.GetSequenceOpts().SequenceOwner.OwnerColumnID,
				"unexpected column owner for sequence seq2 after restore")
			require.Equal(t, 1, td.PublicColumns()[0].NumOwnsSequences(),
				"unexpected number of sequences owned by d3.t after restore",
			)
			require.Equal(t, sd.GetID(), td.PublicColumns()[0].GetOwnsSequenceID(0),
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
		tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "d2", "t")
		seqDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "d3", "seq")

		require.True(t, seqDesc.GetSequenceOpts().HasOwner(), "no sequence owner after restore")
		require.Equal(t, tableDesc.GetID(), seqDesc.GetSequenceOpts().SequenceOwner.OwnerTableID,
			"unexpected table is sequence owner after restore",
		)
		require.Equal(t, tableDesc.PublicColumns()[0].GetID(), seqDesc.GetSequenceOpts().SequenceOwner.OwnerColumnID,
			"unexpected column is sequence owner after restore",
		)
		require.Equal(t, 1, tableDesc.PublicColumns()[0].NumOwnsSequences(),
			"unexpected number of sequences owned by d.t after restore",
		)
		require.Equal(t, seqDesc.GetID(), tableDesc.PublicColumns()[0].GetOwnsSequenceID(0),
			"unexpected ID of sequence owned by table d.t after restore",
		)

		// d3.t owns d2.seq and d3.seq2 should be preserved.
		td := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "d3", "t")
		sd := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "d2", "seq")
		sdSeq2 := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "d3", "seq2")

		require.True(t, sd.GetSequenceOpts().HasOwner(), "no sequence owner after restore")
		require.True(t, sdSeq2.GetSequenceOpts().HasOwner(), "no sequence owner after restore")

		require.Equal(t, td.GetID(), sd.GetSequenceOpts().SequenceOwner.OwnerTableID,
			"unexpected table is sequence owner of d3.seq after restore",
		)
		require.Equal(t, td.GetID(), sdSeq2.GetSequenceOpts().SequenceOwner.OwnerTableID,
			"unexpected table is sequence owner of d3.seq2 after restore",
		)

		require.Equal(t, td.PublicColumns()[0].GetID(), sd.GetSequenceOpts().SequenceOwner.OwnerColumnID,
			"unexpected column is sequence owner of d2.seq after restore",
		)
		require.Equal(t, td.PublicColumns()[0].GetID(), sdSeq2.GetSequenceOpts().SequenceOwner.OwnerColumnID,
			"unexpected column is sequence owner of d3.seq2 after restore",
		)

		require.Equal(t, 2, td.PublicColumns()[0].NumOwnsSequences(),
			"unexpected number of sequences owned by d3.t after restore",
		)
		require.Equal(t, sd.GetID(), td.PublicColumns()[0].GetOwnsSequenceID(0),
			"unexpected ID of sequence owned by table d3.t after restore",
		)
		require.Equal(t, sdSeq2.GetID(), td.PublicColumns()[0].GetOwnsSequenceID(1),
			"unexpected ID of sequence owned by table d3.t after restore",
		)
	})
}

func TestBackupRestoreShowJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	sqlDB.Exec(t, `BACKUP DATABASE data TO $1 WITH revision_history`, localFoo)
	sqlDB.Exec(t, `CREATE DATABASE "data 2"`)

	sqlDB.Exec(t, `RESTORE data.bank FROM $1 WITH skip_missing_foreign_keys, into_db = $2`, localFoo, "data 2")
	// The "updating privileges" clause in the SELECT statement is for excluding jobs
	// run by an unrelated startup migration.
	// TODO (lucy): Update this if/when we decide to change how these jobs queued by
	// the startup migration are handled.
	sqlDB.CheckQueryResults(
		t, "SELECT description FROM [SHOW JOBS] WHERE description != 'updating privileges' ORDER BY description",
		[][]string{
			{"BACKUP DATABASE data TO 'nodelocal://0/foo' WITH revision_history"},
			{"RESTORE TABLE data.bank FROM 'nodelocal://0/foo' WITH into_db = 'data 2', skip_missing_foreign_keys"},
		},
	)
}

func TestBackupCreatedStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	sqlDB.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled=false`)

	sqlDB.Exec(t, `CREATE TABLE data.foo (a INT PRIMARY KEY)`)
	injectStats(t, sqlDB, "data.bank", "id")
	injectStats(t, sqlDB, "data.foo", "a")

	sqlDB.Exec(t, `BACKUP data.bank, data.foo TO $1 WITH revision_history`, localFoo)
	sqlDB.Exec(t, `CREATE DATABASE "data 2"`)
	sqlDB.Exec(t, `RESTORE data.bank, data.foo FROM $1 WITH skip_missing_foreign_keys, into_db = $2`,
		localFoo, "data 2")

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
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	sqlDB.Exec(t, `CREATE DATABASE empty`)
	sqlDB.Exec(t, `BACKUP DATABASE empty TO $1`, localFoo)
	sqlDB.Exec(t, `DROP DATABASE empty`)
	sqlDB.Exec(t, `RESTORE DATABASE empty FROM $1`, localFoo)
	sqlDB.CheckQueryResults(t, `USE empty; SHOW TABLES;`, [][]string{})
}

func TestBackupRestoreSubsetCreatedStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	sqlDB.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled=false`)

	sqlDB.Exec(t, `CREATE TABLE data.foo (a INT)`)
	bankStats := injectStats(t, sqlDB, "data.bank", "id")
	injectStats(t, sqlDB, "data.foo", "a")

	sqlDB.Exec(t, `BACKUP data.bank, data.foo TO $1 WITH revision_history`, localFoo)
	// Clear the stats.
	sqlDB.Exec(t, `DELETE FROM system.table_statistics WHERE true`)
	sqlDB.Exec(t, `CREATE DATABASE "data 2"`)
	sqlDB.Exec(t, `CREATE TABLE "data 2".foo (a INT)`)
	sqlDB.Exec(t, `RESTORE data.bank FROM $1 WITH skip_missing_foreign_keys, into_db = $2`,
		localFoo, "data 2")

	// Ensure that bank's stats have been restored, but foo's have not.
	sqlDB.CheckQueryResults(t, getStatsQuery(`"data 2".bank`), bankStats)
	sqlDB.CheckQueryResults(t, getStatsQuery(`"data 2".foo`), [][]string{})
}

// This test is a reproduction of a scenario that caused backup jobs to fail
// during stats collection because the stats cache would attempt to resolve a
// descriptor that had been dropped. Stats collection was made more resilient to
// such errors in #61222.
func TestBackupHandlesDroppedTypeStatsCollection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const dest = "userfile:///basefoo"
	const numAccounts = 1
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	sqlDB.Exec(t, `CREATE TYPE greeting as ENUM ('hello')`)
	sqlDB.Exec(t, `CREATE TABLE foo (t greeting)`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES ('hello')`)
	sqlDB.Exec(t, `SET sql_safe_updates=false`)
	sqlDB.Exec(t, `ALTER TABLE foo RENAME COLUMN t to t_old`)
	sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN t VARCHAR`)
	sqlDB.Exec(t, `ALTER TABLE foo DROP COLUMN t_old`)
	sqlDB.Exec(t, `DROP TYPE greeting`)

	// Prior to the fix mentioned above, the stats cache would attempt to resolve
	// the dropped type when computing the stats for foo at the end of the backup
	// job. This would result in a `descriptor not found` error.

	// Ensure a full backup completes successfully.
	sqlDB.Exec(t, `BACKUP foo TO $1`, dest)

	// Ensure an incremental backup completes successfully.
	sqlDB.Exec(t, `BACKUP foo TO $1`, dest)
}

// TestBatchedInsertStats is a test for the `insertStats` method used in a
// cluster restore to restore backed up statistics.
func TestBatchedInsertStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts,
		InitManualReplication)
	defer cleanupFn()
	ctx := context.Background()
	s := tc.Server(0)
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	registry := s.JobRegistry().(*jobs.Registry)
	mkJob := func(t *testing.T) *jobs.Job {
		id := registry.MakeJobID()
		job, err := registry.CreateJobWithTxn(ctx, jobs.Record{
			// Job does not accept an empty Details field, so arbitrarily provide
			// RestoreDetails.
			Details:  jobspb.RestoreDetails{},
			Progress: jobspb.RestoreProgress{},
		}, id, nil /* txn */)
		require.NoError(t, err)
		return job
	}

	generateTableStatistics := func(numStats int) []*stats.TableStatisticProto {
		tableStats := make([]*stats.TableStatisticProto, 0, numStats)
		for i := 0; i < numStats; i++ {
			sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE foo%d (id INT)`, i))
			var tableID descpb.ID
			sqlDB.QueryRow(t, fmt.Sprintf(
				`SELECT id FROM system.namespace WHERE name = 'foo%d'`, i)).Scan(&tableID)
			tableStats = append(tableStats, &stats.TableStatisticProto{
				TableID:       tableID,
				ColumnIDs:     []descpb.ColumnID{1},
				RowCount:      10,
				DistinctCount: 0,
				NullCount:     0,
			})
		}
		return tableStats
	}

	for i, test := range []struct {
		name          string
		numTableStats int
	}{
		{
			name:          "empty-stats",
			numTableStats: 0,
		},
		{
			name:          "less-than-batch-size",
			numTableStats: 5,
		},
		{
			name:          "equal-to-batch-size",
			numTableStats: 10,
		},
		{
			name:          "greater-than-batch-size",
			numTableStats: 15,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dbName := fmt.Sprintf("foo%d", i)
			defer sqlDB.Exec(t, fmt.Sprintf(`DROP DATABASE %s`, dbName))
			sqlDB.Exec(t, fmt.Sprintf("CREATE DATABASE %s", dbName))
			sqlDB.Exec(t, fmt.Sprintf("USE %s", dbName))
			stats := generateTableStatistics(test.numTableStats)

			// Clear the stats.
			sqlDB.Exec(t, `DELETE FROM system.table_statistics WHERE true`)
			job := mkJob(t)
			require.NoError(t, insertStats(ctx, job, &execCfg, stats))
			// If there are no stats to insert, we exit early without updating the
			// job.
			if test.numTableStats != 0 {
				require.True(t, job.Details().(jobspb.RestoreDetails).StatsInserted)
			}
			res := sqlDB.QueryStr(t, `SELECT * FROM system.table_statistics`)
			require.Len(t, res, test.numTableStats)
		})
	}
}

func TestBackupRestoreCorruptedStatsIgnored(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const dest = "userfile:///basefoo"
	const numAccounts = 1
	tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts,
		InitManualReplication)
	defer cleanupFn()

	var tableID int
	sqlDB.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'bank'`).Scan(&tableID)
	sqlDB.Exec(t, `BACKUP data.bank TO $1`, dest)

	// Overwrite the stats file with some invalid data.
	ctx := context.Background()
	execCfg := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig)
	store, err := execCfg.DistSQLSrv.ExternalStorageFromURI(ctx, dest,
		security.RootUserName())
	require.NoError(t, err)
	statsTable := StatsTable{
		Statistics: []*stats.TableStatisticProto{{TableID: descpb.ID(tableID + 1), Name: "notbank"}},
	}
	require.NoError(t, writeTableStatistics(ctx, store, backupStatisticsFileName,
		nil /* encryption */, &statsTable))

	sqlDB.Exec(t, `CREATE DATABASE "data 2"`)
	sqlDB.Exec(t, fmt.Sprintf(`RESTORE data.bank FROM "%s" WITH skip_missing_foreign_keys, into_db = "%s"`,
		dest, "data 2"))

	// Delete the stats file to ensure a restore can succeed even if statistics do
	// not exist.
	require.NoError(t, store.Delete(ctx, backupStatisticsFileName))
	sqlDB.Exec(t, `CREATE DATABASE "data 3"`)
	sqlDB.Exec(t, fmt.Sprintf(`RESTORE data.bank FROM "%s" WITH skip_missing_foreign_keys, into_db = "%s"`,
		dest, "data 3"))
}

// Ensure that statistics are restored from correct backup.
func TestBackupCreatedStatsFromIncrementalBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const incremental1Foo = "nodelocal://0/incremental1foo"
	const incremental2Foo = "nodelocal://0/incremental2foo"
	const numAccounts = 1
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()
	var beforeTs string

	sqlDB.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled=false`)

	// Create the 1st backup, with stats estimating 50 rows.
	injectStatsWithRowCount(t, sqlDB, "data.bank", "id", 50 /* rowCount */)
	sqlDB.Exec(t, `BACKUP data.bank TO $1 WITH revision_history`, localFoo)

	// Create the 2nd backup, with stats estimating 100 rows.
	injectStatsWithRowCount(t, sqlDB, "data.bank", "id", 100 /* rowCount */)
	statsBackup2 := sqlDB.QueryStr(t, getStatsQuery("data.bank"))
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&beforeTs) // Save time to restore to this point.
	sqlDB.Exec(t, `BACKUP data.bank TO $1 INCREMENTAL FROM $2 WITH revision_history`, incremental1Foo, localFoo)

	// Create the 3rd backup, with stats estimating 500 rows.
	injectStatsWithRowCount(t, sqlDB, "data.bank", "id", 500 /* rowCount */)
	sqlDB.Exec(t, `BACKUP data.bank TO $1 INCREMENTAL FROM $2, $3 WITH revision_history`, incremental2Foo, localFoo, incremental1Foo)

	// Restore the 2nd backup.
	sqlDB.Exec(t, `CREATE DATABASE "data 2"`)
	sqlDB.Exec(t, fmt.Sprintf(`RESTORE data.bank FROM "%s", "%s", "%s" AS OF SYSTEM TIME %s WITH skip_missing_foreign_keys, into_db = "%s"`,
		localFoo, incremental1Foo, incremental2Foo, beforeTs, "data 2"))

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
				var jobID jobspb.JobID
				err := crdb.ExecuteTx(ctx, db, nil /* txopts */, func(tx *gosql.Tx) error {
					return tx.QueryRow(backupWithDetachedOption).Scan(&jobID)
				})
				require.NoError(t, err)
				waitForSuccessfulJob(t, tc, jobID)
			},
		},
	} {
		baseBackupURI := "nodelocal://0/foo" + testrun.name
		testrun.runBackup(t, fmt.Sprintf(`BACKUP TABLE FOO TO '%s'`, baseBackupURI), runner) // create a base backup.
		allowRequest = make(chan struct{})
		runner.Exec(t, "SET CLUSTER SETTING kv.protectedts.poll_interval = '100ms';")
		runner.Exec(t, "ALTER TABLE foo CONFIGURE ZONE USING gc.ttlseconds = 1;")
		rRand, _ := randutil.NewTestRand()
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
				trace, _, err := s.ManuallyEnqueue(ctx, "mvccGC", repl, skipShouldQueue)
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

func getTableID(db *kv.DB, dbName, tableName string) descpb.ID {
	desc := desctestutils.TestingGetPublicTableDescriptor(db, keys.SystemSQLCodec, dbName, tableName)
	return desc.GetID()
}

// TestSpanSelectionDuringBackup tests the method spansForAllTableIndexes which
// is used to resolve the spans which will be backed up, and spans for which
// protected ts records will be created.
func TestProtectedTimestampSpanSelectionDuringBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.WithIssue(t, 63209, "flaky test")
	defer log.Scope(t).Close(t)

	skip.UnderStressRace(t,
		"not worth starting/stopping the server for each subtest as they all rely on the shared"+
			" variable `actualResolvedSpan`")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	params := base.TestClusterArgs{}
	params.ServerArgs.ExternalIODir = dir
	var actualResolvedSpans []string
	params.ServerArgs.Knobs.BackupRestore = &sql.BackupRestoreTestingKnobs{
		CaptureResolvedTableDescSpans: func(mergedSpans []roachpb.Span) {
			for _, span := range mergedSpans {
				actualResolvedSpans = append(actualResolvedSpans, span.String())
			}
		},
	}
	tc := testcluster.StartTestCluster(t, 3, params)
	defer tc.Stopper().Stop(ctx)

	tc.WaitForNodeLiveness(t)
	require.NoError(t, tc.WaitForFullReplication())

	conn := tc.ServerConn(0)
	runner := sqlutils.MakeSQLRunner(conn)
	db := tc.Server(0).DB()
	baseBackupURI := "nodelocal://0/foo/"

	t.Run("contiguous-span-merge", func(t *testing.T) {
		runner.Exec(t, "CREATE DATABASE test; USE test;")
		runner.Exec(t, "CREATE TABLE foo (k INT PRIMARY KEY, v BYTES, name STRING, "+
			"INDEX baz(name), INDEX bar (v))")

		runner.Exec(t, fmt.Sprintf(`BACKUP DATABASE test INTO '%s'`, baseBackupURI+t.Name()))
		tableID := getTableID(db, "test", "foo")
		require.Equal(t, []string{fmt.Sprintf("/Table/%d/{1-4}", tableID)}, actualResolvedSpans)
		runner.Exec(t, "DROP DATABASE test;")
		actualResolvedSpans = nil
	})

	t.Run("drop-index-span-merge", func(t *testing.T) {
		runner.Exec(t, "CREATE DATABASE test; USE test;")
		runner.Exec(t, "CREATE TABLE foo (k INT PRIMARY KEY, v BYTES, name STRING, "+
			"INDEX baz(name), INDEX bar (v))")
		runner.Exec(t, "INSERT INTO foo VALUES (1, NULL, 'testuser')")
		runner.Exec(t, "ALTER TABLE foo CONFIGURE ZONE USING gc.ttlseconds=60")
		runner.Exec(t, "DROP INDEX foo@baz")

		runner.Exec(t, fmt.Sprintf(`BACKUP DATABASE test INTO '%s'`, baseBackupURI+t.Name()))
		tableID := getTableID(db, "test", "foo")
		require.Equal(t, []string{
			fmt.Sprintf("/Table/%d/{1-2}", tableID),
			fmt.Sprintf("/Table/%d/{3-4}", tableID),
		}, actualResolvedSpans)
		runner.Exec(t, "DROP DATABASE test;")
		actualResolvedSpans = nil
	})

	t.Run("drop-index-gced-span-merge", func(t *testing.T) {
		runner.Exec(t, "CREATE DATABASE test; USE test;")
		runner.Exec(t, "CREATE TABLE foo (k INT PRIMARY KEY, v BYTES, name STRING, "+
			"INDEX baz(name), INDEX bar (v))")
		runner.Exec(t, "INSERT INTO foo VALUES (1, NULL, 'testuser')")
		runner.Exec(t, "ALTER TABLE foo CONFIGURE ZONE USING gc.ttlseconds=1")
		runner.Exec(t, "DROP INDEX foo@baz")
		time.Sleep(time.Second * 2)

		runner.Exec(t, fmt.Sprintf(`BACKUP DATABASE test INTO '%s'`, baseBackupURI+t.Name()))
		tableID := getTableID(db, "test", "foo")
		require.Equal(t, []string{
			fmt.Sprintf("/Table/%d/{1-2}", tableID),
			fmt.Sprintf("/Table/%d/{3-4}", tableID),
		}, actualResolvedSpans)
		runner.Exec(t, "DROP DATABASE test;")
		actualResolvedSpans = nil
	})

	t.Run("revs-span-merge", func(t *testing.T) {
		runner.Exec(t, "CREATE DATABASE test; USE test;")
		runner.Exec(t, "CREATE TABLE foo (k INT PRIMARY KEY, v BYTES, name STRING, "+
			"INDEX baz(name), INDEX bar (v))")
		runner.Exec(t, "INSERT INTO foo VALUES (1, NULL, 'testuser')")
		runner.Exec(t, "ALTER TABLE foo CONFIGURE ZONE USING gc.ttlseconds=60")
		runner.Exec(t, "DROP INDEX foo@baz")

		runner.Exec(t, `BACKUP DATABASE test TO 'nodelocal://0/fooz' WITH revision_history`)

		// The BACKUP with revision history will pickup the dropped index baz as
		// well because it existed in a non-drop state at some point in the interval
		// covered by this BACKUP.
		tableID := getTableID(db, "test", "foo")
		require.Equal(t, []string{fmt.Sprintf("/Table/%d/{1-4}", tableID)}, actualResolvedSpans)
		actualResolvedSpans = nil
		runner.Exec(t, "DROP TABLE foo")

		runner.Exec(t, "CREATE TABLE foo2 (k INT PRIMARY KEY, v BYTES, name STRING, "+
			"INDEX baz(name), INDEX bar (v))")
		runner.Exec(t, "INSERT INTO foo2 VALUES (1, NULL, 'testuser')")
		runner.Exec(t, "ALTER TABLE foo2 CONFIGURE ZONE USING gc.ttlseconds=60")
		runner.Exec(t, "DROP INDEX foo2@baz")

		runner.Exec(t, `BACKUP DATABASE test TO 'nodelocal://0/fooz' WITH revision_history`)
		// We expect to see only the non-drop indexes of table foo in this
		// incremental backup with revision history. We also expect to see both drop
		// and non-drop indexes of table foo2 as all the indexes were live at some
		// point in the interval covered by this BACKUP.
		tableID2 := getTableID(db, "test", "foo2")
		require.Equal(t, []string{
			fmt.Sprintf("/Table/%d/{1-2}", tableID),
			fmt.Sprintf("/Table/%d/{3-4}", tableID), fmt.Sprintf("/Table/%d/{1-4}", tableID2),
		},
			actualResolvedSpans)
		runner.Exec(t, "DROP DATABASE test;")
		actualResolvedSpans = nil
	})

	t.Run("last-index-dropped", func(t *testing.T) {
		runner.Exec(t, "CREATE DATABASE test; USE test;")
		runner.Exec(t, "CREATE TABLE foo (k INT PRIMARY KEY, v BYTES, name STRING, INDEX baz(name))")
		runner.Exec(t, "CREATE TABLE foo2 (k INT PRIMARY KEY, v BYTES, name STRING, INDEX baz(name))")
		runner.Exec(t, "ALTER TABLE foo CONFIGURE ZONE USING gc.ttlseconds=60")
		runner.Exec(t, "DROP INDEX foo@baz")

		runner.Exec(t, fmt.Sprintf(`BACKUP DATABASE test INTO '%s'`, baseBackupURI+t.Name()))
		tableID := getTableID(db, "test", "foo")
		tableID2 := getTableID(db, "test", "foo2")
		require.Equal(t, []string{
			fmt.Sprintf("/Table/%d/{1-2}", tableID),
			fmt.Sprintf("/Table/%d/{1-3}", tableID2),
		}, actualResolvedSpans)
		runner.Exec(t, "DROP DATABASE test;")
		actualResolvedSpans = nil
	})

	t.Run("last-index-gced", func(t *testing.T) {
		runner.Exec(t, "CREATE DATABASE test; USE test;")
		runner.Exec(t, "CREATE TABLE foo (k INT PRIMARY KEY, v BYTES, name STRING, INDEX baz(name))")
		runner.Exec(t, "INSERT INTO foo VALUES (1, NULL, 'test')")
		runner.Exec(t, "CREATE TABLE foo2 (k INT PRIMARY KEY, v BYTES, name STRING, INDEX baz(name))")
		runner.Exec(t, "ALTER TABLE foo CONFIGURE ZONE USING gc.ttlseconds=1")
		runner.Exec(t, "DROP INDEX foo@baz")
		time.Sleep(time.Second * 2)
		runner.Exec(t, "ALTER TABLE foo CONFIGURE ZONE USING gc.ttlseconds=60")

		runner.Exec(t, fmt.Sprintf(`BACKUP DATABASE test INTO '%s'`, baseBackupURI+t.Name()))
		tableID := getTableID(db, "test", "foo")
		tableID2 := getTableID(db, "test", "foo2")
		require.Equal(t, []string{
			fmt.Sprintf("/Table/%d/{1-2}", tableID),
			fmt.Sprintf("/Table/%d/{1-3}", tableID2),
		}, actualResolvedSpans)
		runner.Exec(t, "DROP DATABASE test;")
		actualResolvedSpans = nil
	})
}

func getMockIndexDesc(indexID descpb.IndexID) descpb.IndexDescriptor {
	mockIndexDescriptor := descpb.IndexDescriptor{ID: indexID}
	return mockIndexDescriptor
}

func getMockTableDesc(
	tableID descpb.ID,
	pkIndex descpb.IndexDescriptor,
	indexes []descpb.IndexDescriptor,
	addingIndexes []descpb.IndexDescriptor,
	droppingIndexes []descpb.IndexDescriptor,
) catalog.TableDescriptor {
	mockTableDescriptor := descpb.TableDescriptor{
		ID:           tableID,
		PrimaryIndex: pkIndex,
		Indexes:      indexes,
	}
	mutationID := descpb.MutationID(0)
	for _, addingIndex := range addingIndexes {
		mutationID++
		mockTableDescriptor.Mutations = append(mockTableDescriptor.Mutations, descpb.DescriptorMutation{
			State:       descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
			Direction:   descpb.DescriptorMutation_ADD,
			Descriptor_: &descpb.DescriptorMutation_Index{Index: &addingIndex},
			MutationID:  mutationID,
		})
	}
	for _, droppingIndex := range droppingIndexes {
		mutationID++
		mockTableDescriptor.Mutations = append(mockTableDescriptor.Mutations, descpb.DescriptorMutation{
			State:       descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
			Direction:   descpb.DescriptorMutation_DROP,
			Descriptor_: &descpb.DescriptorMutation_Index{Index: &droppingIndex},
			MutationID:  mutationID,
		})
	}
	return tabledesc.NewBuilder(&mockTableDescriptor).BuildImmutableTable()
}

// Unit tests for the spansForAllTableIndexes and getPublicIndexTableSpans()
// methods.
func TestPublicIndexTableSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	codec := keys.TODOSQLCodec
	execCfg := &sql.ExecutorConfig{
		Codec: codec,
	}
	unusedMap := make(map[tableAndIndex]bool)
	testCases := []struct {
		name                string
		tableID             descpb.ID
		pkIndex             descpb.IndexDescriptor
		indexes             []descpb.IndexDescriptor
		addingIndexes       []descpb.IndexDescriptor
		droppingIndexes     []descpb.IndexDescriptor
		expectedSpans       []string
		expectedMergedSpans []string
	}{
		{
			name:                "contiguous-spans",
			tableID:             55,
			pkIndex:             getMockIndexDesc(1),
			indexes:             []descpb.IndexDescriptor{getMockIndexDesc(1), getMockIndexDesc(2)},
			expectedSpans:       []string{"/Table/55/{1-2}", "/Table/55/{2-3}"},
			expectedMergedSpans: []string{"/Table/55/{1-3}"},
		},
		{
			name:                "dropped-span-between-two-spans",
			tableID:             56,
			pkIndex:             getMockIndexDesc(1),
			indexes:             []descpb.IndexDescriptor{getMockIndexDesc(1), getMockIndexDesc(3)},
			droppingIndexes:     []descpb.IndexDescriptor{getMockIndexDesc(2)},
			expectedSpans:       []string{"/Table/56/{1-2}", "/Table/56/{3-4}"},
			expectedMergedSpans: []string{"/Table/56/{1-2}", "/Table/56/{3-4}"},
		},
		{
			name:                "gced-span-between-two-spans",
			tableID:             57,
			pkIndex:             getMockIndexDesc(1),
			indexes:             []descpb.IndexDescriptor{getMockIndexDesc(1), getMockIndexDesc(3)},
			expectedSpans:       []string{"/Table/57/{1-2}", "/Table/57/{3-4}"},
			expectedMergedSpans: []string{"/Table/57/{1-2}", "/Table/57/{3-4}"},
		},
		{
			name:    "alternate-spans-dropped",
			tableID: 58,
			pkIndex: getMockIndexDesc(1),
			indexes: []descpb.IndexDescriptor{
				getMockIndexDesc(1), getMockIndexDesc(3),
				getMockIndexDesc(5),
			},
			droppingIndexes:     []descpb.IndexDescriptor{getMockIndexDesc(2), getMockIndexDesc(4)},
			expectedSpans:       []string{"/Table/58/{1-2}", "/Table/58/{3-4}", "/Table/58/{5-6}"},
			expectedMergedSpans: []string{"/Table/58/{1-2}", "/Table/58/{3-4}", "/Table/58/{5-6}"},
		},
		{
			name:    "alternate-spans-gced",
			tableID: 59,
			pkIndex: getMockIndexDesc(1),
			indexes: []descpb.IndexDescriptor{
				getMockIndexDesc(1), getMockIndexDesc(3),
				getMockIndexDesc(5),
			},
			expectedSpans:       []string{"/Table/59/{1-2}", "/Table/59/{3-4}", "/Table/59/{5-6}"},
			expectedMergedSpans: []string{"/Table/59/{1-2}", "/Table/59/{3-4}", "/Table/59/{5-6}"},
		},
		{
			name:    "one-drop-one-gc",
			tableID: 60,
			pkIndex: getMockIndexDesc(1),
			indexes: []descpb.IndexDescriptor{
				getMockIndexDesc(1), getMockIndexDesc(3),
				getMockIndexDesc(5),
			},
			droppingIndexes:     []descpb.IndexDescriptor{getMockIndexDesc(2)},
			expectedSpans:       []string{"/Table/60/{1-2}", "/Table/60/{3-4}", "/Table/60/{5-6}"},
			expectedMergedSpans: []string{"/Table/60/{1-2}", "/Table/60/{3-4}", "/Table/60/{5-6}"},
		},
		{
			// Although there are no keys on index 2, we should not include its
			// span since it holds an adding index.
			name:    "empty-adding-index",
			tableID: 61,
			pkIndex: getMockIndexDesc(1),
			indexes: []descpb.IndexDescriptor{
				getMockIndexDesc(1), getMockIndexDesc(3),
				getMockIndexDesc(4),
			},
			addingIndexes:       []descpb.IndexDescriptor{getMockIndexDesc(2)},
			expectedSpans:       []string{"/Table/61/{1-2}", "/Table/61/{3-4}", "/Table/61/{4-5}"},
			expectedMergedSpans: []string{"/Table/61/{1-2}", "/Table/61/{3-5}"},
		},
	}

	for _, test := range testCases {
		tableDesc := getMockTableDesc(test.tableID, test.pkIndex,
			test.indexes, test.addingIndexes, test.droppingIndexes)
		t.Run(fmt.Sprintf("%s:%s", "getPublicIndexTableSpans", test.name), func(t *testing.T) {
			spans, err := getPublicIndexTableSpans(tableDesc, unusedMap, codec)
			require.NoError(t, err)
			var unmergedSpans []string
			for _, span := range spans {
				unmergedSpans = append(unmergedSpans, span.String())
			}
			require.Equal(t, test.expectedSpans, unmergedSpans)
		})

		t.Run(fmt.Sprintf("%s:%s", "spansForAllTableIndexes", test.name), func(t *testing.T) {
			mergedSpans, err := spansForAllTableIndexes(execCfg, []catalog.TableDescriptor{tableDesc}, nil /* revs */)
			require.NoError(t, err)
			var mergedSpanStrings []string
			for _, mSpan := range mergedSpans {
				mergedSpanStrings = append(mergedSpanStrings, mSpan.String())
			}
			require.Equal(t, test.expectedMergedSpans, mergedSpanStrings)
		})
	}
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
	jobsTableKey := keys.SystemSQLCodec.TablePrefix(uint32(systemschema.JobsTable.GetID()))
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
//
// TODO(adityamaru): Remove in 22.2 once no records protect spans.
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

func TestPaginatedBackupTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type exportResumePoint struct {
		roachpb.Span
		timestamp hlc.Timestamp
	}

	withTS := hlc.Timestamp{WallTime: 1}
	withoutTS := hlc.Timestamp{}

	const numAccounts = 1
	serverArgs := base.TestServerArgs{Knobs: base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()}}
	params := base.TestClusterArgs{ServerArgs: serverArgs}
	var numExportRequests int

	mu := struct {
		syncutil.Mutex
		exportRequestSpansSet map[string]struct{}
		exportRequestSpans    []string
	}{}
	mu.exportRequestSpansSet = make(map[string]struct{})
	mu.exportRequestSpans = make([]string, 0)

	requestSpanStr := func(span roachpb.Span, timestamp hlc.Timestamp) string {
		spanStr := ""
		if !timestamp.IsEmpty() {
			spanStr = ":with_ts"
		}
		return fmt.Sprintf("%v%s", span.String(), spanStr)
	}

	// Check if export request is from a lease for a descriptor to avoid picking
	// up on wrong export requests
	isLeasingExportRequest := func(r *roachpb.ExportRequest) bool {
		_, tenantID, _ := keys.DecodeTenantPrefix(r.Key)
		codec := keys.MakeSQLCodec(tenantID)
		return bytes.HasPrefix(r.Key, codec.DescMetadataPrefix()) &&
			r.EndKey.Equal(r.Key.PrefixEnd())
	}
	params.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingRequestFilter: func(ctx context.Context, request roachpb.BatchRequest) *roachpb.Error {
			for _, ru := range request.Requests {
				if exportRequest, ok := ru.GetInner().(*roachpb.ExportRequest); ok &&
					!isLeasingExportRequest(exportRequest) {

					mu.Lock()
					defer mu.Unlock()
					req := requestSpanStr(roachpb.Span{Key: exportRequest.Key, EndKey: exportRequest.EndKey}, exportRequest.ResumeKeyTS)
					if _, found := mu.exportRequestSpansSet[req]; found {
						return nil // nothing to do
					}
					mu.exportRequestSpansSet[req] = struct{}{}
					mu.exportRequestSpans = append(mu.exportRequestSpans, req)
					numExportRequests++
				}
			}
			return nil
		},
		TestingResponseFilter: func(ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse) *roachpb.Error {
			for i, ru := range br.Responses {
				if exportRequest, ok := ba.Requests[i].GetInner().(*roachpb.ExportRequest); ok &&
					!isLeasingExportRequest(exportRequest) {
					exportResponse := ru.GetInner().(*roachpb.ExportResponse)
					// Every ExportResponse should have a single SST when running backup
					// within a tenant.
					require.Equal(t, 1, len(exportResponse.Files))
				}
			}
			return nil
		},
	}
	tc, systemDB, _, cleanupFn := backupRestoreTestSetupWithParams(t, singleNode, numAccounts,
		InitManualReplication, params)
	defer cleanupFn()
	srv := tc.Server(0)

	_ = security.EmbeddedTenantIDs()

	resetStateVars := func() {
		mu.Lock()
		defer mu.Unlock()

		numExportRequests = 0
		mu.exportRequestSpansSet = make(map[string]struct{})
		mu.exportRequestSpans = mu.exportRequestSpans[:0]
	}

	_, conn10 := serverutils.StartTenant(t, srv,
		base.TestTenantArgs{TenantID: roachpb.MakeTenantID(10)})
	defer conn10.Close()
	tenant10 := sqlutils.MakeSQLRunner(conn10)
	tenant10.Exec(t, `
CREATE DATABASE foo;
CREATE TABLE foo.bar(i int primary key);
INSERT INTO foo.bar VALUES (110), (210), (310), (410), (510)`)
	var id1 int
	tenant10.QueryRow(t, "SELECT 'foo.bar'::regclass::int").Scan(&id1)

	// The total size in bytes of the data to be backed up is 63b.

	// Single ExportRequest with no resume span.
	systemDB.Exec(t, `SET CLUSTER SETTING kv.bulk_sst.target_size='63b'`)
	systemDB.Exec(t, `SET CLUSTER SETTING kv.bulk_sst.max_allowed_overage='0b'`)
	idRE := regexp.MustCompile(":id")
	mkKey := func(id int, k string) roachpb.Key {
		return roachpb.Key(idRE.ReplaceAllString(k, fmt.Sprint(id)))
	}
	mkSpan := func(id int, start, end string) roachpb.Span {
		return roachpb.Span{Key: mkKey(id, start), EndKey: mkKey(id, end)}
	}

	tenant10.Exec(t, `BACKUP DATABASE foo TO 'userfile://defaultdb.myfililes/test'`)
	startingSpan := mkSpan(id1, "/Tenant/10/Table/:id/1", "/Tenant/10/Table/:id/2")
	mu.Lock()
	require.Equal(t, []string{startingSpan.String()}, mu.exportRequestSpans)
	mu.Unlock()
	resetStateVars()

	// Two ExportRequests with one resume span.
	systemDB.Exec(t, `SET CLUSTER SETTING kv.bulk_sst.target_size='50b'`)
	tenant10.Exec(t, `BACKUP DATABASE foo TO 'userfile://defaultdb.myfililes/test2'`)
	startingSpan = mkSpan(id1, "/Tenant/10/Table/:id/1", "/Tenant/10/Table/:id/2")
	resumeSpan := mkSpan(id1, "/Tenant/10/Table/:id/1/510/0", "/Tenant/10/Table/:id/2")
	mu.Lock()
	require.Equal(t, []string{startingSpan.String(), resumeSpan.String()}, mu.exportRequestSpans)
	mu.Unlock()
	resetStateVars()

	// One ExportRequest for every KV.
	systemDB.Exec(t, `SET CLUSTER SETTING kv.bulk_sst.target_size='10b'`)
	tenant10.Exec(t, `BACKUP DATABASE foo TO 'userfile://defaultdb.myfililes/test3'`)
	var expected []string
	for _, resume := range []exportResumePoint{
		{mkSpan(id1, "/Tenant/10/Table/:id/1", "/Tenant/10/Table/:id/2"), withoutTS},
		{mkSpan(id1, "/Tenant/10/Table/:id/1/210/0", "/Tenant/10/Table/:id/2"), withoutTS},
		{mkSpan(id1, "/Tenant/10/Table/:id/1/310/0", "/Tenant/10/Table/:id/2"), withoutTS},
		{mkSpan(id1, "/Tenant/10/Table/:id/1/410/0", "/Tenant/10/Table/:id/2"), withoutTS},
		{mkSpan(id1, "/Tenant/10/Table/:id/1/510/0", "/Tenant/10/Table/:id/2"), withoutTS},
	} {
		expected = append(expected, requestSpanStr(resume.Span, resume.timestamp))
	}
	mu.Lock()
	require.Equal(t, expected, mu.exportRequestSpans)
	mu.Unlock()
	resetStateVars()

	tenant10.Exec(t, `
CREATE DATABASE baz; 
CREATE TABLE baz.bar(i int primary key, v string); 
INSERT INTO baz.bar VALUES (110, 'a'), (210, 'b'), (310, 'c'), (410, 'd'), (510, 'e')`)
	var id2 int
	tenant10.QueryRow(t, "SELECT 'baz.bar'::regclass::int").Scan(&id2)
	// The total size in bytes of the data to be backed up is 63b.

	// Single ExportRequest with no resume span.
	systemDB.Exec(t, `SET CLUSTER SETTING kv.bulk_sst.target_size='10b'`)
	systemDB.Exec(t, `SET CLUSTER SETTING kv.bulk_sst.max_allowed_overage='10b'`)

	// Test mid key breaks for the tenant to verify timestamps on resume.
	tenant10.Exec(t, `UPDATE baz.bar SET v = 'z' WHERE i = 210`)
	tenant10.Exec(t, `BACKUP DATABASE baz TO 'userfile://defaultdb.myfililes/test4' with revision_history`)
	expected = nil
	for _, resume := range []exportResumePoint{
		{mkSpan(id2, "/Tenant/10/Table/3", "/Tenant/10/Table/4"), withoutTS},
		{mkSpan(id2, "/Tenant/10/Table/:id/1", "/Tenant/10/Table/:id/2"), withoutTS},
		{mkSpan(id2, "/Tenant/10/Table/:id/1/210/0", "/Tenant/10/Table/:id/2"), withoutTS},
		// We have two entries for 210 because of history and super small table size
		{mkSpan(id2, "/Tenant/10/Table/:id/1/210/0", "/Tenant/10/Table/:id/2"), withTS},
		{mkSpan(id2, "/Tenant/10/Table/:id/1/310/0", "/Tenant/10/Table/:id/2"), withoutTS},
		{mkSpan(id2, "/Tenant/10/Table/:id/1/410/0", "/Tenant/10/Table/:id/2"), withoutTS},
		{mkSpan(id2, "/Tenant/10/Table/:id/1/510/0", "/Tenant/10/Table/:id/2"), withoutTS},
	} {
		expected = append(expected, requestSpanStr(resume.Span, resume.timestamp))
	}
	mu.Lock()
	require.Equal(t, expected, mu.exportRequestSpans)
	mu.Unlock()
	resetStateVars()

	// TODO(adityamaru): Add a RESTORE inside tenant once it is supported.
}

func TestBackupRestoreInsideTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const numAccounts = 1

	makeTenant := func(srv serverutils.TestServerInterface, tenant uint64) (*sqlutils.SQLRunner, func()) {
		_, conn := serverutils.StartTenant(t, srv, base.TestTenantArgs{TenantID: roachpb.MakeTenantID(tenant)})
		cleanup := func() { conn.Close() }
		return sqlutils.MakeSQLRunner(conn), cleanup
	}
	tc, systemDB, dir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	_, _ = tc, systemDB
	defer cleanupFn()
	srv := tc.Server(0)

	// NB: tenant certs for 10, 11, and 20 are embedded. See:
	_ = security.EmbeddedTenantIDs()

	tenant10, cleanupT10 := makeTenant(srv, 10)
	defer cleanupT10()
	tenant10.Exec(t, `CREATE DATABASE foo; CREATE TABLE foo.bar(i int primary key); INSERT INTO foo.bar VALUES (110), (210)`)

	tenant11, cleanupT11 := makeTenant(srv, 11)
	defer cleanupT11()

	// Create another server.
	tc2, systemDB2, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, dir, InitManualReplication, base.TestClusterArgs{})
	srv2 := tc2.Server(0)
	defer cleanupEmptyCluster()

	tenant10C2, cleanupT10C2 := makeTenant(srv2, 10)
	defer cleanupT10C2()

	tenant11C2, cleanupT11C2 := makeTenant(srv2, 11)
	defer cleanupT11C2()

	t.Run("tenant-backup", func(t *testing.T) {
		// This test uses this mock HTTP server to pass the backup files between tenants.
		httpAddr, httpServerCleanup := makeInsecureHTTPServer(t)
		defer httpServerCleanup()

		tenant10.Exec(t, `BACKUP TO $1`, httpAddr)

		t.Run("cluster-restore", func(t *testing.T) {
			t.Run("into-same-tenant-id", func(t *testing.T) {
				tenant10C2.Exec(t, `RESTORE FROM $1`, httpAddr)
				tenant10C2.CheckQueryResults(t, `SELECT * FROM foo.bar`, tenant10.QueryStr(t, `SELECT * FROM foo.bar`))
			})
			t.Run("into-different-tenant-id", func(t *testing.T) {
				tenant11C2.ExpectErr(t, `cannot cluster RESTORE backups taken from different tenant: 10`,
					`RESTORE FROM $1`, httpAddr)
			})
			t.Run("into-system-tenant-id", func(t *testing.T) {
				systemDB2.ExpectErr(t, `cannot cluster RESTORE backups taken from different tenant: 10`,
					`RESTORE FROM $1`, httpAddr)
			})
		})

		t.Run("database-restore", func(t *testing.T) {
			t.Run("into-same-tenant-id", func(t *testing.T) {
				tenant10.Exec(t, `CREATE DATABASE foo2`)
				tenant10.Exec(t, `RESTORE foo.bar FROM $1 WITH into_db='foo2'`, httpAddr)
				tenant10.CheckQueryResults(t, `SELECT * FROM foo2.bar`, tenant10.QueryStr(t, `SELECT * FROM foo.bar`))
			})
			t.Run("into-different-tenant-id", func(t *testing.T) {
				tenant11.Exec(t, `CREATE DATABASE foo`)
				tenant11.Exec(t, `RESTORE foo.bar FROM $1`, httpAddr)
				tenant11.CheckQueryResults(t, `SELECT * FROM foo.bar`, tenant10.QueryStr(t, `SELECT * FROM foo.bar`))
			})
			t.Run("into-system-tenant-id", func(t *testing.T) {
				systemDB.Exec(t, `CREATE DATABASE foo2`)
				systemDB.Exec(t, `RESTORE foo.bar FROM $1 WITH into_db='foo2'`, httpAddr)
				systemDB.CheckQueryResults(t, `SELECT * FROM foo2.bar`, tenant10.QueryStr(t, `SELECT * FROM foo.bar`))
			})
		})
	})

	t.Run("system-backup", func(t *testing.T) {
		// This test uses this mock HTTP server to pass the backup files between tenants.
		httpAddr, httpServerCleanup := makeInsecureHTTPServer(t)
		defer httpServerCleanup()

		systemDB.Exec(t, `BACKUP TO $1`, httpAddr)

		tenant20C2, cleanupT20C2 := makeTenant(srv2, 20)
		defer cleanupT20C2()

		t.Run("cluster-restore", func(t *testing.T) {
			t.Run("with-tenant", func(t *testing.T) {
				// This is disallowed because the cluster restore includes other
				// tenants, which can't be restored inside a tenant.
				tenant20C2.ExpectErr(t, `only the system tenant can restore other tenants`,
					`RESTORE FROM $1`, httpAddr)
			})

			t.Run("with-no-tenant", func(t *testing.T) {
				// Now restore a cluster backup taken from a system tenant that
				// hasn't created any tenants.
				httpAddrEmpty, cleanupEmptyHTTPServer := makeInsecureHTTPServer(t)
				defer cleanupEmptyHTTPServer()

				_, emptySystemDB, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode,
					dir, InitManualReplication, base.TestClusterArgs{})
				defer cleanupEmptyCluster()

				emptySystemDB.Exec(t, `BACKUP TO $1`, httpAddrEmpty)
				tenant20C2.ExpectErr(t, `cannot cluster RESTORE backups taken from different tenant: system`,
					`RESTORE FROM $1`, httpAddrEmpty)
			})
		})

		t.Run("database-restore-into-tenant", func(t *testing.T) {
			tenant10.Exec(t, `CREATE DATABASE data`)
			tenant10.Exec(t, `RESTORE data.bank FROM $1`, httpAddr)
			systemDB.CheckQueryResults(t, `SELECT * FROM data.bank`, tenant10.QueryStr(t, `SELECT * FROM data.bank`))
		})

	})
}

// TestBackupRestoreInsideMultiPodTenant verifies that backup and restore work
// inside tenants with multiple SQL pods. Currently, verification that restore
// and backup are distributed to all pods in the multi-pod tests must be done
// manually and by enabling logging and checking the log for messages containing
// "starting restore data" or "starting backup data" for nsql1 and nsql2.
// TODO(harding): Verify that backup and restore are distributed in test.
func TestBackupRestoreInsideMultiPodTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "may time out due to multiple servers")

	const numAccounts = 1
	const npods = 2

	makeTenant := func(srv serverutils.TestServerInterface, tenant uint64, existing bool) (*sqlutils.SQLRunner, func()) {
		_, conn := serverutils.StartTenant(t, srv, base.TestTenantArgs{TenantID: roachpb.MakeTenantID(tenant), Existing: existing})
		cleanup := func() { conn.Close() }
		return sqlutils.MakeSQLRunner(conn), cleanup
	}

	tc, systemDB, dir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	_, _ = tc, systemDB
	defer cleanupFn()
	srv := tc.Server(0)

	// NB: tenant certs for 10, 11, and 20 are embedded. See:
	_ = security.EmbeddedTenantIDs()

	// Create another server.
	tc2, systemDB2, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, dir, InitManualReplication, base.TestClusterArgs{})
	srv2 := tc2.Server(0)
	defer cleanupEmptyCluster()

	tenant10 := make([]*sqlutils.SQLRunner, npods)
	cleanupT10 := make([]func(), npods)
	tenant11 := make([]*sqlutils.SQLRunner, npods)
	cleanupT11 := make([]func(), npods)
	tenant10C2 := make([]*sqlutils.SQLRunner, npods)
	cleanupT10C2 := make([]func(), npods)
	for i := 0; i < npods; i++ {
		tenant10[i], cleanupT10[i] = makeTenant(srv, 10, i != 0)
		defer cleanupT10[i]()
		tenant11[i], cleanupT11[i] = makeTenant(srv, 11, i != 0)
		defer cleanupT11[i]()
		tenant10C2[i], cleanupT10C2[i] = makeTenant(srv2, 10, i != 0)
		defer cleanupT10C2[i]()
	}

	tenant11C2, cleanupT11C2 := makeTenant(srv2, 11, false)
	defer cleanupT11C2()

	tenant10[0].Exec(t, `CREATE DATABASE foo; CREATE TABLE foo.bar(i int primary key); INSERT INTO foo.bar VALUES (110), (210), (310)`)

	t.Run("tenant-backup", func(t *testing.T) {
		// This test uses this mock HTTP server to pass the backup files between tenants.
		httpAddr, httpServerCleanup := makeInsecureHTTPServer(t)
		defer httpServerCleanup()

		tenant10[0].Exec(t, `BACKUP TO $1`, httpAddr)

		t.Run("cluster-restore", func(t *testing.T) {
			t.Run("into-same-tenant-id", func(t *testing.T) {
				tenant10C2[0].Exec(t, `RESTORE FROM $1`, httpAddr)
				tenant10C2[0].CheckQueryResults(t, `SELECT * FROM foo.bar`, tenant10[0].QueryStr(t, `SELECT * FROM foo.bar`))
			})
			t.Run("into-different-tenant-id", func(t *testing.T) {
				tenant11C2.ExpectErr(t, `cannot cluster RESTORE backups taken from different tenant: 10`,
					`RESTORE FROM $1`, httpAddr)
			})
			t.Run("into-system-tenant-id", func(t *testing.T) {
				systemDB2.ExpectErr(t, `cannot cluster RESTORE backups taken from different tenant: 10`,
					`RESTORE FROM $1`, httpAddr)
			})
		})

		t.Run("database-restore", func(t *testing.T) {
			t.Run("into-same-tenant-id", func(t *testing.T) {
				tenant10[0].Exec(t, `CREATE DATABASE foo2`)
				tenant10[0].Exec(t, `RESTORE foo.bar FROM $1 WITH into_db='foo2'`, httpAddr)
				tenant10[0].CheckQueryResults(t, `SELECT * FROM foo2.bar`, tenant10[0].QueryStr(t, `SELECT * FROM foo.bar`))
			})
			t.Run("into-different-tenant-id", func(t *testing.T) {
				tenant11[0].Exec(t, `CREATE DATABASE foo`)
				tenant11[0].Exec(t, `RESTORE foo.bar FROM $1`, httpAddr)
				tenant11[0].CheckQueryResults(t, `SELECT * FROM foo.bar`, tenant10[0].QueryStr(t, `SELECT * FROM foo.bar`))
			})
			t.Run("into-system-tenant-id", func(t *testing.T) {
				systemDB.Exec(t, `CREATE DATABASE foo2`)
				systemDB.Exec(t, `RESTORE foo.bar FROM $1 WITH into_db='foo2'`, httpAddr)
				systemDB.CheckQueryResults(t, `SELECT * FROM foo2.bar`, tenant10[0].QueryStr(t, `SELECT * FROM foo.bar`))
			})
		})
	})

	t.Run("system-backup", func(t *testing.T) {
		// This test uses this mock HTTP server to pass the backup files between tenants.
		httpAddr, httpServerCleanup := makeInsecureHTTPServer(t)
		defer httpServerCleanup()

		systemDB.Exec(t, `BACKUP TO $1`, httpAddr)

		tenant20C2, cleanupT20C2 := makeTenant(srv2, 20, false)
		defer cleanupT20C2()

		t.Run("cluster-restore", func(t *testing.T) {
			t.Run("with-tenant", func(t *testing.T) {
				// This is disallowed because the cluster restore includes other
				// tenants, which can't be restored inside a tenant.
				tenant20C2.ExpectErr(t, `only the system tenant can restore other tenants`,
					`RESTORE FROM $1`, httpAddr)
			})

			t.Run("with-no-tenant", func(t *testing.T) {
				// Now restore a cluster backup taken from a system tenant that
				// hasn't created any tenants.
				httpAddrEmpty, cleanupEmptyHTTPServer := makeInsecureHTTPServer(t)
				defer cleanupEmptyHTTPServer()

				_, emptySystemDB, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode,
					dir, InitManualReplication, base.TestClusterArgs{})
				defer cleanupEmptyCluster()

				emptySystemDB.Exec(t, `BACKUP TO $1`, httpAddrEmpty)
				tenant20C2.ExpectErr(t, `cannot cluster RESTORE backups taken from different tenant: system`,
					`RESTORE FROM $1`, httpAddrEmpty)
			})
		})

		t.Run("database-restore-into-tenant", func(t *testing.T) {
			tenant10[0].Exec(t, `CREATE DATABASE data`)
			tenant10[0].Exec(t, `RESTORE data.bank FROM $1`, httpAddr)
			systemDB.CheckQueryResults(t, `SELECT * FROM data.bank`, tenant10[0].QueryStr(t, `SELECT * FROM data.bank`))
		})
	})
}

// Ensure that backing up and restoring tenants succeeds.
func TestBackupRestoreTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params := base.TestClusterArgs{ServerArgs: base.TestServerArgs{
		Knobs: base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()},
	}}

	const numAccounts = 1
	ctx := context.Background()
	tc, systemDB, dir, cleanupFn := backupRestoreTestSetupWithParams(
		t, singleNode, numAccounts, InitManualReplication, params,
	)
	_, _ = tc, systemDB
	defer cleanupFn()
	srv := tc.Server(0)

	// NB: tenant certs for 10, 11, 20 are embedded. See:
	_ = security.EmbeddedTenantIDs()

	// Setup a few tenants, each with a different table.
	_, conn10 := serverutils.StartTenant(t, srv, base.TestTenantArgs{TenantID: roachpb.MakeTenantID(10)})
	defer conn10.Close()
	tenant10 := sqlutils.MakeSQLRunner(conn10)
	tenant10.Exec(t, `CREATE DATABASE foo; CREATE TABLE foo.bar(i int primary key); INSERT INTO foo.bar VALUES (110), (210)`)

	// Wait until tenant 10 usage data is initialized - we will later check that
	// it is restored.
	testutils.SucceedsSoon(t, func() error {
		res := systemDB.QueryStr(t, `SELECT 1 FROM system.tenant_usage WHERE tenant_id = 10`)
		if len(res) == 0 {
			return errors.Errorf("no tenant_usage data for tenant 10")
		}
		return nil
	})

	// Set an all-tenant override and a tenant-specific override.
	// TODO(radu): use ALTER TENANT when available.
	systemDB.Exec(t, `INSERT INTO system.tenant_settings (tenant_id, name, value, value_type) VALUES
		(0, 'tenant_cost_model.kv_read_cost_per_megabyte', '123', 'f'),
		(10, 'tenant_cost_model.kv_write_cost_per_megabyte', '456', 'f')`)

	systemDB.Exec(t, `BACKUP system.users TO 'nodelocal://1/users'`)
	systemDB.CheckQueryResults(t, `SELECT manifest->>'tenants' FROM [SHOW BACKUP 'nodelocal://1/users' WITH as_json]`, [][]string{{"[]"}})

	_, conn11 := serverutils.StartTenant(t, srv, base.TestTenantArgs{TenantID: roachpb.MakeTenantID(11)})
	defer conn11.Close()
	tenant11 := sqlutils.MakeSQLRunner(conn11)
	tenant11.Exec(t, `CREATE DATABASE foo; CREATE TABLE foo.baz(i int primary key); INSERT INTO foo.baz VALUES (111), (211)`)

	_, conn20 := serverutils.StartTenant(t, srv, base.TestTenantArgs{TenantID: roachpb.MakeTenantID(20)})
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
			t, singleNode, base.TestClusterArgs{ServerArgs: base.TestServerArgs{
				ExternalIODir: dir,
				Knobs:         base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()},
			}},
		)
		defer restoreTC.Stopper().Stop(ctx)
		restoreDB := sqlutils.MakeSQLRunner(restoreTC.Conns[0])

		restoreDB.CheckQueryResults(t, `select * from system.tenants`, [][]string{})
		restoreDB.Exec(t, `RESTORE TENANT 10 FROM 'nodelocal://1/t10'`)
		restoreDB.CheckQueryResults(t,
			`SELECT id, active, crdb_internal.pb_to_json('cockroach.sql.sqlbase.TenantInfo', info, true) FROM system.tenants`,
			[][]string{{`10`, `true`, `{"id": "10", "state": "ACTIVE"}`}},
		)
		restoreDB.CheckQueryResults(t,
			`SELECT ru_refill_rate, instance_id, next_instance_id, current_share_sum
			 FROM system.tenant_usage WHERE tenant_id = 10`,
			[][]string{{`100`, `0`, `0`, `0`}},
		)

		ten10Stopper := stop.NewStopper()
		_, restoreConn10 := serverutils.StartTenant(
			t, restoreTC.Server(0), base.TestTenantArgs{
				TenantID: roachpb.MakeTenantID(10), Existing: true, Stopper: ten10Stopper,
			},
		)
		restoreTenant10 := sqlutils.MakeSQLRunner(restoreConn10)
		restoreTenant10.CheckQueryResults(t, `select * from foo.bar`, tenant10.QueryStr(t, `select * from foo.bar`))
		restoreTenant10.CheckQueryResults(t, `select * from foo.bar2`, tenant10.QueryStr(t, `select * from foo.bar2`))

		// Stop the tenant process before destroying the tenant.
		restoreConn10.Close()
		ten10Stopper.Stop(ctx)
		restoreConn10 = nil

		// Mark tenant as DROP.
		restoreDB.Exec(t, `SELECT crdb_internal.destroy_tenant(10)`)
		restoreDB.CheckQueryResults(t,
			`select id, active, crdb_internal.pb_to_json('cockroach.sql.sqlbase.TenantInfo', info) from system.tenants`,
			[][]string{{`10`, `false`, `{"id": "10", "state": "DROP"}`}},
		)

		// Make GC job scheduled by destroy_tenant run in 1 second.
		restoreDB.Exec(t, "SET CLUSTER SETTING kv.range_merge.queue_enabled = false")
		restoreDB.Exec(t, "ALTER RANGE tenants CONFIGURE ZONE USING gc.ttlseconds = 1;")
		// Wait for tenant GC job to complete.
		restoreDB.CheckQueryResultsRetry(
			t,
			"SELECT status FROM [SHOW JOBS] WHERE description = 'GC for tenant 10'",
			[][]string{{"succeeded"}},
		)

		ten10Prefix := keys.MakeTenantPrefix(roachpb.MakeTenantID(10))
		ten10PrefixEnd := ten10Prefix.PrefixEnd()
		rows, err := restoreTC.Server(0).DB().Scan(ctx, ten10Prefix, ten10PrefixEnd, 0 /* maxRows */)
		require.NoError(t, err)
		require.Equal(t, []kv.KeyValue{}, rows)

		restoreDB.Exec(t, `RESTORE TENANT 10 FROM 'nodelocal://1/t10'`)
		restoreDB.CheckQueryResults(t,
			`select id, active, crdb_internal.pb_to_json('cockroach.sql.sqlbase.TenantInfo', info, true) from system.tenants`,
			[][]string{{`10`, `true`, `{"id": "10", "state": "ACTIVE"}`}},
		)

		_, restoreConn10 = serverutils.StartTenant(
			t, restoreTC.Server(0), base.TestTenantArgs{TenantID: roachpb.MakeTenantID(10), Existing: true},
		)
		defer restoreConn10.Close()
		restoreTenant10 = sqlutils.MakeSQLRunner(restoreConn10)

		restoreTenant10.CheckQueryResults(t, `select * from foo.bar`, tenant10.QueryStr(t, `select * from foo.bar`))
		restoreTenant10.CheckQueryResults(t, `select * from foo.bar2`, tenant10.QueryStr(t, `select * from foo.bar2`))

		restoreDB.Exec(t, `SELECT crdb_internal.destroy_tenant(10)`)
		// Wait for tenant GC job to complete.
		restoreDB.CheckQueryResultsRetry(
			t,
			"SELECT status FROM [SHOW JOBS] WHERE description = 'GC for tenant 10'",
			[][]string{{"succeeded"}, {"succeeded"}},
		)

		restoreDB.CheckQueryResults(t, `select * from system.tenants`, [][]string{})
		restoreDB.Exec(t, `RESTORE TENANT 10 FROM 'nodelocal://1/t10'`)
		restoreDB.CheckQueryResults(t,
			`select id, active, crdb_internal.pb_to_json('cockroach.sql.sqlbase.TenantInfo', info, true) from system.tenants`,
			[][]string{{`10`, `true`, `{"id": "10", "state": "ACTIVE"}`}},
		)
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
			`select id, active, crdb_internal.pb_to_json('cockroach.sql.sqlbase.TenantInfo', info, true) from system.tenants`,
			[][]string{{`10`, `true`, `{"id": "10", "state": "ACTIVE"}`}},
		)

		_, restoreConn10 := serverutils.StartTenant(
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
			`select id, active, crdb_internal.pb_to_json('cockroach.sql.sqlbase.TenantInfo', info, true) from system.tenants`,
			[][]string{
				{`10`, `true`, `{"id": "10", "state": "ACTIVE"}`},
				{`11`, `true`, `{"id": "11", "state": "ACTIVE"}`},
				{`20`, `true`, `{"id": "20", "state": "ACTIVE"}`},
			},
		)

		_, restoreConn10 := serverutils.StartTenant(
			t, restoreTC.Server(0), base.TestTenantArgs{TenantID: roachpb.MakeTenantID(10), Existing: true},
		)
		defer restoreConn10.Close()
		restoreTenant10 := sqlutils.MakeSQLRunner(restoreConn10)

		restoreTenant10.CheckQueryResults(t, `select * from foo.bar`, tenant10.QueryStr(t, `select * from foo.bar`))
		restoreTenant10.CheckQueryResults(t, `select * from foo.bar2`, tenant10.QueryStr(t, `select * from foo.bar2`))

		// Verify cluster overrides.
		restoreTenant10.CheckQueryResults(t, `SHOW CLUSTER SETTING tenant_cost_model.kv_read_cost_per_megabyte`, [][]string{{"123"}})
		restoreTenant10.CheckQueryResults(t, `SHOW CLUSTER SETTING tenant_cost_model.kv_write_cost_per_megabyte`, [][]string{{"456"}})

		_, restoreConn11 := serverutils.StartTenant(
			t, restoreTC.Server(0), base.TestTenantArgs{TenantID: roachpb.MakeTenantID(11), Existing: true},
		)
		defer restoreConn11.Close()
		restoreTenant11 := sqlutils.MakeSQLRunner(restoreConn11)

		restoreTenant11.CheckQueryResults(t, `select * from foo.baz`, tenant11.QueryStr(t, `select * from foo.baz`))

		// Check the all-tenant override.
		restoreTenant11.CheckQueryResults(t, `SHOW CLUSTER SETTING tenant_cost_model.kv_read_cost_per_megabyte`, [][]string{{"123"}})

		restoreDB.Exec(t, `SELECT crdb_internal.destroy_tenant(20, true)`)

		restoreDB.Exec(t, `RESTORE TENANT 11 FROM 'nodelocal://1/clusterwide' WITH tenant = '20'`)
		_, restoreConn20 := serverutils.StartTenant(
			t, restoreTC.Server(0), base.TestTenantArgs{TenantID: roachpb.MakeTenantID(20), Existing: true},
		)
		defer restoreConn20.Close()
		restoreTenant20 := sqlutils.MakeSQLRunner(restoreConn20)

		// Tenant 20 gets results that matched the backed up tenant 11.
		restoreTenant20.CheckQueryResults(t, `select * from foo.baz`, tenant11.QueryStr(t, `select * from foo.baz`))
		// Check the all-tenant override.
		restoreTenant20.CheckQueryResults(t, `SHOW CLUSTER SETTING tenant_cost_model.kv_read_cost_per_megabyte`, [][]string{{"123"}})

		// Remove tenant 11, then confirm restoring 11 over 10 fails.
		restoreDB.Exec(t, `SELECT crdb_internal.destroy_tenant(11, true)`)
		restoreDB.ExpectErr(t, `exists`, `RESTORE TENANT 11 FROM 'nodelocal://1/clusterwide' WITH tenant = '10'`)

		// Verify tenant 20 is still unaffected.
		restoreTenant20.CheckQueryResults(t, `select * from foo.baz`, tenant11.QueryStr(t, `select * from foo.baz`))
	})

	t.Run("restore-tenant10-to-ts1", func(t *testing.T) {
		restoreTC := testcluster.StartTestCluster(
			t, singleNode, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}},
		)
		defer restoreTC.Stopper().Stop(ctx)
		restoreDB := sqlutils.MakeSQLRunner(restoreTC.Conns[0])

		restoreDB.Exec(t, `RESTORE TENANT 10 FROM 'nodelocal://1/t10' AS OF SYSTEM TIME `+ts1)

		_, restoreConn10 := serverutils.StartTenant(
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

		_, restoreConn20 := serverutils.StartTenant(
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
			jobCommand: fmt.Sprintf("BACKUP TO '%s'", localFoo),
		},
		{
			jobType:    "RESTORE",
			jobCommand: fmt.Sprintf("RESTORE data.* FROM '%s' WITH into_db='%s'", localFoo, restoreDB),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.jobType, func(t *testing.T) {
			// When completing an export request, signal the a request has been sent and
			// then wait to be signaled.
			allowResponse := make(chan struct{})
			gotRequest := make(chan struct{}, 1)

			blockBackupOrRestore := func(ctx context.Context) {
				select {
				case gotRequest <- struct{}{}:
				default:
				}
				select {
				case <-allowResponse:
				case <-ctx.Done(): // Deal with test failures.
				}
			}

			args := base.TestClusterArgs{}
			knobs := base.TestingKnobs{
				DistSQL: &execinfra.TestingKnobs{BackupRestoreTestingKnobs: &sql.BackupRestoreTestingKnobs{RunAfterProcessingRestoreSpanEntry: func(ctx context.Context) {
					blockBackupOrRestore(ctx)
				}}},
				Store: &kvserver.StoreTestingKnobs{
					TestingResponseFilter: func(ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse) *roachpb.Error {
						for _, ru := range br.Responses {
							switch ru.GetInner().(type) {
							case *roachpb.ExportResponse:
								blockBackupOrRestore(ctx)
							}
						}
						return nil
					},
				},
			}
			args.ServerArgs.Knobs = knobs
			tc, sqlDB, _, cleanup := backupRestoreTestSetupWithParams(t, multiNode, 1 /* numAccounts */, InitManualReplication, args)
			defer cleanup()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			conn := tc.ServerConn(0)
			sqlDB.Exec(t, "SET CLUSTER SETTING kv.protectedts.poll_interval = '100ms';")

			// If we're testing restore, we first create a backup file to restore.
			if testCase.jobType == "RESTORE" {
				close(allowResponse)
				sqlDB.Exec(t, fmt.Sprintf("CREATE DATABASE %s", restoreDB))
				sqlDB.Exec(t, "BACKUP TO $1", localFoo)
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
				connCfg, err := pgx.ParseConfig(pgURL.String())
				assert.NoError(t, err)
				db, err := pgx.ConnectConfig(ctx, connCfg)
				assert.NoError(t, err)
				defer func() { assert.NoError(t, db.Close(ctx)) }()
				_, err = db.Exec(ctxToCancel, testCase.jobCommand)
				assert.Equal(t, context.Canceled, errors.Unwrap(err))
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

func TestBackupExportRequestTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	allowRequest := make(chan struct{})
	defer close(allowRequest)
	params := base.TestClusterArgs{}
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	params.ServerArgs.ExternalIODir = dir
	const numAccounts = 10
	ctx := context.Background()
	tc, sqlDB, _, cleanupFn := backupRestoreTestSetupWithParams(t, 2 /* nodes */, numAccounts,
		InitManualReplication, params)
	defer cleanupFn()

	sqlSessions := []*sqlutils.SQLRunner{}
	for i := 0; i < 2; i++ {
		newConn, err := tc.ServerConn(i).Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		sqlSessions = append(sqlSessions, sqlutils.MakeSQLRunner(newConn))
	}

	sqlDB.Exec(t, "SET CLUSTER SETTING bulkio.backup.read_timeout = '3s'")
	sqlDB.Exec(t, "SET CLUSTER SETTING bulkio.backup.read_with_priority_after = '100ms'")
	sqlDB.Exec(t, "SET CLUSTER SETTING bulkio.backup.read_retry_delay = '10ms'")

	// Start a high priority txn that lays down an intent.
	sqlSessions[0].Exec(t, `BEGIN PRIORITY HIGH; UPDATE data.bank SET balance = 0 WHERE id = 5 OR id = 8;`)

	// Backup should go through the motions of attempting to run a high priority
	// export request but since the intent was laid by a high priority txn it
	// should hang. The timeout should save us in this case.
	_, err := sqlSessions[1].DB.ExecContext(ctx, "BACKUP data.bank TO 'nodelocal://0/timeout'")
	require.True(t, testutils.IsError(err,
		`timeout: operation "ExportRequest for span /Table/\d+/.*\" timed out after 3s`))
}

func TestBackupDoesNotHangOnIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 10
	ctx := context.Background()
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
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

func TestRestoreTypeDescriptorsRollBack(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
	defer cleanupFn()

	for _, server := range tc.Servers {
		registry := server.JobRegistry().(*jobs.Registry)
		registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
			jobspb.TypeRestore: func(raw jobs.Resumer) jobs.Resumer {
				r := raw.(*restoreResumer)
				r.testingKnobs.beforePublishingDescriptors = func() error {
					return errors.New("boom")
				}
				return r
			},
		}
	}

	sqlDB.Exec(t, `
CREATE DATABASE db;
CREATE SCHEMA db.s;
CREATE TYPE db.typ AS ENUM();
CREATE TABLE db.table (k INT PRIMARY KEY, v db.typ);
`)

	// Back up the database, drop it, and restore into it.
	sqlDB.Exec(t, `BACKUP DATABASE db TO 'nodelocal://0/test/1'`)
	sqlDB.Exec(t, `DROP DATABASE db`)
	sqlDB.ExpectErr(t, "boom", `RESTORE DATABASE db FROM 'nodelocal://0/test/1'`)
	sqlDB.CheckQueryResults(t, `SELECT count(*) FROM system.namespace WHERE name = 'typ'`, [][]string{{"0"}})

	// Back up database with user defined schema.
	sqlDB.Exec(t, `
CREATE DATABASE db;
CREATE SCHEMA db.s;
CREATE TYPE db.s.typ AS ENUM();
CREATE TABLE db.s.table (k INT PRIMARY KEY, v db.s.typ);
`)

	// Back up the database, drop it, and restore into it.
	sqlDB.Exec(t, `BACKUP DATABASE db TO 'nodelocal://0/test/2'`)
	sqlDB.Exec(t, `DROP DATABASE db`)
	sqlDB.ExpectErr(t, "boom", `RESTORE DATABASE db FROM 'nodelocal://0/test/2'`)
	sqlDB.CheckQueryResults(t, `SELECT count(*) FROM system.namespace WHERE name = 'typ'`, [][]string{{"0"}})
}

// TestRestoreResetsDescriptorVersions tests that new descriptors created while
// restoring have their versions reset. Descriptors end up at version 2 after
// the job is finished, since they are updated once at the end of the job to
// make them public.
func TestRestoreResetsDescriptorVersions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
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

	dbDesc := desctestutils.TestingGetDatabaseDescriptor(kvDB, keys.SystemSQLCodec, "d")
	require.EqualValues(t, 2, dbDesc.GetVersion())

	schemaDesc := desctestutils.TestingGetSchemaDescriptor(kvDB, keys.SystemSQLCodec, dbDesc.GetID(), "sc")
	require.EqualValues(t, 2, schemaDesc.GetVersion())

	tableDesc := desctestutils.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "d", "sc", "tb")
	require.EqualValues(t, 2, tableDesc.GetVersion())

	typeDesc := desctestutils.TestingGetTypeDescriptor(kvDB, keys.SystemSQLCodec, "d", "sc", "typ")
	require.EqualValues(t, 2, typeDesc.GetVersion())
}

func TestOfflineDescriptorsDuringRestore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	state := struct {
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
		ctx := context.Background()
		tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
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

		dbDesc := desctestutils.TestingGetDatabaseDescriptor(kvDB, keys.SystemSQLCodec, "d")
		require.Equal(t, descpb.DescriptorState_OFFLINE, dbDesc.DatabaseDesc().State)

		schemaDesc := desctestutils.TestingGetSchemaDescriptor(kvDB, keys.SystemSQLCodec, dbDesc.GetID(), "sc")
		require.Equal(t, descpb.DescriptorState_OFFLINE, schemaDesc.SchemaDesc().State)

		tableDesc := desctestutils.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "d", "sc", "tb")
		require.Equal(t, descpb.DescriptorState_OFFLINE, tableDesc.GetState())

		typeDesc := desctestutils.TestingGetTypeDescriptor(kvDB, keys.SystemSQLCodec, "d", "sc", "typ")
		require.Equal(t, descpb.DescriptorState_OFFLINE, typeDesc.TypeDesc().State)

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

		sqlDB.CheckQueryResults(t, `SELECT database_name, owner FROM [SHOW DATABASES]`, [][]string{
			{"data", security.RootUser},
			{"defaultdb", security.RootUser},
			{"postgres", security.RootUser},
			{"system", security.NodeUser},
		})

		sqlDB.ExpectErr(t, `database "d" is offline: restoring`, `USE d`)

		sqlDB.ExpectErr(t, `database "d" is offline: restoring`, `SHOW TABLES FROM d`)
		sqlDB.ExpectErr(t, `database "d" is offline: restoring`, `SHOW TABLES FROM d.sc`)

		sqlDB.ExpectErr(t, `database "d" is offline: restoring`, `SELECT * FROM d.sc.tb`)
		sqlDB.ExpectErr(t, `database "d" is offline: restoring`, `ALTER TABLE d.sc.tb ADD COLUMN b INT`)

		sqlDB.ExpectErr(t, `database "d" is offline: restoring`, `ALTER TYPE d.sc.typ RENAME TO typ2`)

		sqlDB.ExpectErr(t, `database "d" is offline: restoring`, `CREATE TABLE d.sc.other()`)

		close(continueNotif)
		require.NoError(t, g.Wait())
	})

	t.Run("restore-into-existing-database", func(t *testing.T) {
		ctx := context.Background()
		tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
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
CREATE TABLE tb (x INT);
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

		dbDesc := desctestutils.TestingGetDatabaseDescriptor(kvDB, keys.SystemSQLCodec, "newdb")
		schemaDesc := desctestutils.TestingGetSchemaDescriptor(kvDB, keys.SystemSQLCodec, dbDesc.GetID(), "sc")
		require.Equal(t, descpb.DescriptorState_OFFLINE, schemaDesc.SchemaDesc().State)

		publicTableDesc := desctestutils.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "newdb", "public", "tb")
		require.Equal(t, descpb.DescriptorState_OFFLINE, publicTableDesc.GetState())

		scTableDesc := desctestutils.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "newdb", "sc", "tb")
		require.Equal(t, descpb.DescriptorState_OFFLINE, scTableDesc.GetState())

		typeDesc := desctestutils.TestingGetTypeDescriptor(kvDB, keys.SystemSQLCodec, "newdb", "sc", "typ")
		require.Equal(t, descpb.DescriptorState_OFFLINE, typeDesc.TypeDesc().State)

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
			{"crdb_internal", "NULL"},
			{"information_schema", "NULL"},
			{"pg_catalog", "NULL"},
			{"pg_extension", "NULL"},
			{"public", security.AdminRole},
		})

		sqlDB.ExpectErr(t, `schema "sc" is offline: restoring`, `SHOW TABLES FROM newdb.sc`)

		sqlDB.ExpectErr(t, `relation "tb" is offline: restoring`, `SELECT * FROM newdb.tb`)
		sqlDB.ExpectErr(t, `relation "tb" is offline: restoring`, `SELECT * FROM newdb.public.tb`)

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

// TestCleanupDoesNotDeleteParentsWithChildObjects tests that if the job fails
// after the new descriptors have been published, and any new databases or
// schemas have new child objects/schemas added to them, we don't drop those
// databases or schemas (to avoid orphaning the new child objects/schemas).
func TestCleanupDoesNotDeleteParentsWithChildObjects(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	state := struct {
		mu syncutil.Mutex
		// Closed when the restore job has reached the point right after publishing
		// public descriptors.
		afterPublishingNotification chan struct{}
		// Closed when we're ready to resume with the restore.
		continueNotification chan struct{}
	}{}
	notifyAfterPublishing := func() (chan struct{}, chan struct{}) {
		state.mu.Lock()
		defer state.mu.Unlock()
		state.afterPublishingNotification = make(chan struct{})
		state.continueNotification = make(chan struct{})
		return state.afterPublishingNotification, state.continueNotification
	}
	notifyContinue := func(ctx context.Context) {
		state.mu.Lock()
		defer state.mu.Unlock()
		if state.afterPublishingNotification != nil {
			close(state.afterPublishingNotification)
			state.afterPublishingNotification = nil
		}
		select {
		case <-state.continueNotification:
			return
		case <-ctx.Done():
			return
		}
	}

	t.Run("clean-up-database-with-schema", func(t *testing.T) {
		ctx := context.Background()
		tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
		defer cleanupFn()
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		for _, server := range tc.Servers {
			registry := server.JobRegistry().(*jobs.Registry)
			registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
				jobspb.TypeRestore: func(raw jobs.Resumer) jobs.Resumer {
					r := raw.(*restoreResumer)
					r.testingKnobs.afterPublishingDescriptors = func() error {
						notifyContinue(ctx)
						return errors.New("injected error")
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

		afterPublishNotif, continueNotif := notifyAfterPublishing()
		g := ctxgroup.WithContext(ctx)
		g.GoCtx(func(ctx context.Context) error {
			_, err := sqlDB.DB.ExecContext(ctx, `RESTORE DATABASE d FROM 'nodelocal://0/test/'`)
			require.Regexp(t, "injected error", err)
			return nil
		})

		<-afterPublishNotif
		// Create a schema in the database we just made public for which the RESTORE
		// job isn't actually finished.
		sqlDB.Exec(t, `
			USE d;
			CREATE SCHEMA new_schema;
		`)
		close(continueNotif)
		require.NoError(t, g.Wait())

		// Check that the restored database still exists, but only contains the new
		// schema we added.
		sqlDB.CheckQueryResults(t, `SELECT schema_name FROM [SHOW SCHEMAS FROM d] ORDER BY 1`, [][]string{
			{"crdb_internal"}, {"information_schema"}, {"new_schema"}, {"pg_catalog"}, {"pg_extension"}, {"public"},
		})
		sqlDB.CheckQueryResults(t, `SHOW TABLES FROM d`, [][]string{})
		sqlDB.CheckQueryResults(t, `SHOW TYPES`, [][]string{})
	})

	t.Run("clean-up-database-with-table", func(t *testing.T) {
		ctx := context.Background()
		tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
		defer cleanupFn()
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		for _, server := range tc.Servers {
			registry := server.JobRegistry().(*jobs.Registry)
			registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
				jobspb.TypeRestore: func(raw jobs.Resumer) jobs.Resumer {
					r := raw.(*restoreResumer)
					r.testingKnobs.afterPublishingDescriptors = func() error {
						notifyContinue(ctx)
						return errors.New("injected error")
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

		afterPublishNotif, continueNotif := notifyAfterPublishing()
		g := ctxgroup.WithContext(ctx)
		g.GoCtx(func(ctx context.Context) error {
			_, err := sqlDB.DB.ExecContext(ctx, `RESTORE DATABASE d FROM 'nodelocal://0/test/'`)
			require.Regexp(t, "injected error", err)
			return nil
		})

		<-afterPublishNotif
		// Create a table in the database we just made public for which the RESTORE
		// job isn't actually finished.
		sqlDB.Exec(t, `CREATE TABLE d.public.new_table()`)
		close(continueNotif)
		require.NoError(t, g.Wait())

		// Check that the restored database still exists, but only contains the new
		// table we added.
		sqlDB.CheckQueryResults(t, `SELECT schema_name FROM [SHOW SCHEMAS FROM d] ORDER BY 1`, [][]string{
			{"crdb_internal"}, {"information_schema"}, {"pg_catalog"}, {"pg_extension"}, {"public"},
		})
		sqlDB.CheckQueryResults(t, `SELECT schema_name, table_name FROM [SHOW TABLES FROM d]`, [][]string{
			{"public", "new_table"},
		})
		sqlDB.CheckQueryResults(t, `SHOW TYPES`, [][]string{})
	})

	t.Run("clean-up-schema-with-table", func(t *testing.T) {
		ctx := context.Background()
		tc, _, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
		defer cleanupFn()
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		for _, server := range tc.Servers {
			registry := server.JobRegistry().(*jobs.Registry)
			registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
				jobspb.TypeRestore: func(raw jobs.Resumer) jobs.Resumer {
					r := raw.(*restoreResumer)
					r.testingKnobs.afterPublishingDescriptors = func() error {
						notifyContinue(ctx)
						return errors.New("injected error")
					}
					return r
				},
			}
		}

		// Use the same connection throughout (except for the concurrent RESTORE) to
		// preserve session variables.
		conn, err := tc.Conns[0].Conn(ctx)
		require.NoError(t, err)
		sqlDB := sqlutils.MakeSQLRunner(conn)

		sqlDB.Exec(t, `
			CREATE DATABASE olddb;
			USE olddb;
			CREATE SCHEMA sc;
			CREATE TABLE sc.tb (x INT);
		`)

		// Back up the database.
		sqlDB.Exec(t, `BACKUP DATABASE olddb TO 'nodelocal://0/test/'`)

		// Drop the database.
		sqlDB.Exec(t, `DROP DATABASE olddb`)

		// Create a new database and restore into it.
		sqlDB.Exec(t, `CREATE DATABASE newdb`)

		afterPublishNotif, continueNotif := notifyAfterPublishing()
		g := ctxgroup.WithContext(ctx)
		g.GoCtx(func(ctx context.Context) error {
			conn, err := tc.Conns[0].Conn(ctx)
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, `RESTORE olddb.* FROM 'nodelocal://0/test/' WITH into_db='newdb'`)
			require.Regexp(t, "injected error", err)
			return nil
		})

		<-afterPublishNotif
		// Create a table in the schema we just made public for which the RESTORE
		// job isn't actually finished.
		sqlDB.Exec(t, `CREATE TABLE newdb.sc.new_table()`)
		close(continueNotif)
		require.NoError(t, g.Wait())

		sqlDB.Exec(t, `USE newdb`)
		sqlDB.CheckQueryResults(t, `SELECT schema_name from [SHOW SCHEMAS FROM newdb] ORDER BY 1`, [][]string{
			{"crdb_internal"}, {"information_schema"}, {"pg_catalog"}, {"pg_extension"}, {"public"}, {"sc"},
		})
		sqlDB.CheckQueryResults(t, `SELECT schema_name, table_name FROM [SHOW TABLES FROM sc]`, [][]string{
			{"sc", "new_table"},
		})
	})
}

func TestManifestTooNew(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	_, sqlDB, rawDir, cleanupFn := backupRestoreTestSetup(t, singleNode, 1, InitManualReplication)
	defer cleanupFn()
	sqlDB.Exec(t, `CREATE DATABASE r1`)
	sqlDB.Exec(t, `BACKUP DATABASE r1 TO 'nodelocal://0/too_new'`)
	sqlDB.Exec(t, `DROP DATABASE r1`)
	// Prove we can restore.
	sqlDB.Exec(t, `RESTORE DATABASE r1 FROM 'nodelocal://0/too_new'`)
	sqlDB.Exec(t, `DROP DATABASE r1`)

	// Load/deserialize the manifest so we can mess with it.
	manifestPath := filepath.Join(rawDir, "too_new", backupManifestName)
	manifestData, err := ioutil.ReadFile(manifestPath)
	require.NoError(t, err)
	manifestData, err = decompressData(context.Background(), nil, manifestData)
	require.NoError(t, err)
	var backupManifest BackupManifest
	require.NoError(t, protoutil.Unmarshal(manifestData, &backupManifest))

	// Bump the version and write it back out to make it look newer.
	backupManifest.ClusterVersion = roachpb.Version{Major: 99, Minor: 1}
	manifestData, err = protoutil.Marshal(&backupManifest)
	require.NoError(t, err)
	require.NoError(t, ioutil.WriteFile(manifestPath, manifestData, 0644 /* perm */))
	// Also write the checksum file to match the new manifest.
	checksum, err := getChecksum(manifestData)
	require.NoError(t, err)
	require.NoError(t, ioutil.WriteFile(manifestPath+backupManifestChecksumSuffix, checksum, 0644 /* perm */))

	// Verify we reject it.
	sqlDB.ExpectErr(t, "backup from version 99.1 is newer than current version", `RESTORE DATABASE r1 FROM 'nodelocal://0/too_new'`)

	// Bump the version down and write it back out to make it look older.
	backupManifest.ClusterVersion = roachpb.Version{Major: 20, Minor: 2, Internal: 2}
	manifestData, err = protoutil.Marshal(&backupManifest)
	require.NoError(t, err)
	require.NoError(t, ioutil.WriteFile(manifestPath, manifestData, 0644 /* perm */))
	// Also write the checksum file to match the new manifest.
	checksum, err = getChecksum(manifestData)
	require.NoError(t, err)
	require.NoError(t, ioutil.WriteFile(manifestPath+backupManifestChecksumSuffix, checksum, 0644 /* perm */))

	// Prove we can restore again.
	sqlDB.Exec(t, `RESTORE DATABASE r1 FROM 'nodelocal://0/too_new'`)
	sqlDB.Exec(t, `DROP DATABASE r1`)

	// Nil out the version to match an old backup that lacked it.
	backupManifest.ClusterVersion = roachpb.Version{}
	manifestData, err = protoutil.Marshal(&backupManifest)
	require.NoError(t, err)
	require.NoError(t, ioutil.WriteFile(manifestPath, manifestData, 0644 /* perm */))
	// Also write the checksum file to match the new manifest.
	checksum, err = getChecksum(manifestData)
	require.NoError(t, err)
	require.NoError(t, ioutil.WriteFile(manifestPath+backupManifestChecksumSuffix, checksum, 0644 /* perm */))
	// Prove we can restore again.
	sqlDB.Exec(t, `RESTORE DATABASE r1 FROM 'nodelocal://0/too_new'`)
	sqlDB.Exec(t, `DROP DATABASE r1`)
}

// TestManifestBitFlip tests that we can detect a corrupt manifest when a bit
// was flipped on disk for both an unencrypted and an encrypted manifest.
func TestManifestBitFlip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	_, sqlDB, rawDir, cleanupFn := backupRestoreTestSetup(t, singleNode, 1, InitManualReplication)
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
		sqlDB.Exec(t, `BACKUP DATABASE data TO 'nodelocal://0/bit_flip_encrypted' WITH encryption_passphrase = 'abc'`)
		flipBitInManifests(t, rawDir)
		sqlDB.ExpectErr(t, checksumError,
			`RESTORE data.* FROM 'nodelocal://0/bit_flip_encrypted' WITH encryption_passphrase = 'abc', into_db = 'r3'`)
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

func TestFullClusterTemporaryBackupAndRestore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "times out under race cause it starts up two test servers")

	numNodes := 4
	// Start a new server that shares the data directory.
	settings := cluster.MakeTestingClusterSettings()
	sql.TempObjectWaitInterval.Override(context.Background(), &settings.SV, time.Microsecond*0)
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	params := base.TestClusterArgs{}
	params.ServerArgs.ExternalIODir = dir
	params.ServerArgs.UseDatabase = "defaultdb"
	params.ServerArgs.Settings = settings
	knobs := base.TestingKnobs{
		SQLExecutor: &sql.ExecutorTestingKnobs{
			DisableTempObjectsCleanupOnSessionExit: true,
		},
	}
	params.ServerArgs.Knobs = knobs
	tc := serverutils.StartNewTestCluster(
		t, numNodes, params,
	)
	defer tc.Stopper().Stop(context.Background())

	// Start two temporary schemas and create a table in each. This table will
	// have different pg_temp schemas but will be created in the same defaultdb.
	comment := "never see this"
	for _, connID := range []int{0, 1} {
		conn := tc.ServerConn(connID)
		sqlDB := sqlutils.MakeSQLRunner(conn)
		sqlDB.Exec(t, `SET experimental_enable_temp_tables=true`)
		sqlDB.Exec(t, `CREATE TEMP TABLE t (x INT)`)
		sqlDB.Exec(t, fmt.Sprintf(`COMMENT ON TABLE t IS '%s'`, comment))
		require.NoError(t, conn.Close())
	}

	// Create a third session where we have two temp tables which will be in the
	// same pg_temp schema with the same name but in different DBs.
	diffDBConn := tc.ServerConn(2)
	diffDB := sqlutils.MakeSQLRunner(diffDBConn)
	diffDB.Exec(t, `SET experimental_enable_temp_tables=true`)
	diffDB.Exec(t, `CREATE DATABASE d1`)
	diffDB.Exec(t, `USE d1`)
	diffDB.Exec(t, `CREATE TEMP TABLE t (x INT)`)
	diffDB.Exec(t, `CREATE DATABASE d2`)
	diffDB.Exec(t, `USE d2`)
	diffDB.Exec(t, `CREATE TEMP TABLE t (x INT)`)
	require.NoError(t, diffDBConn.Close())

	backupDBConn := tc.ServerConn(3)
	backupDB := sqlutils.MakeSQLRunner(backupDBConn)
	backupDB.Exec(t, `BACKUP TO 'nodelocal://0/full_cluster_backup'`)
	require.NoError(t, backupDBConn.Close())

	params = base.TestClusterArgs{}
	ch := make(chan time.Time)
	finishedCh := make(chan struct{})
	knobs = base.TestingKnobs{
		SQLExecutor: &sql.ExecutorTestingKnobs{
			OnTempObjectsCleanupDone: func() {
				finishedCh <- struct{}{}
			},
			TempObjectsCleanupCh: ch,
		},
	}
	params.ServerArgs.Knobs = knobs
	params.ServerArgs.Settings = settings
	_, sqlDBRestore, cleanupRestore := backupRestoreTestSetupEmpty(t, singleNode, dir, InitManualReplication,
		params)
	defer cleanupRestore()
	sqlDBRestore.Exec(t, `RESTORE FROM 'nodelocal://0/full_cluster_backup'`)

	// Before the reconciliation job runs we should be able to see the following:
	// - 2 synthesized pg_temp sessions in defaultdb and 1 each in db1 and db2.
	// We synthesize a new temp schema for each unique backed-up schemaID
	// of a temporary table descriptor.
	// - All temp tables remapped to belong to the associated synthesized temp
	// schema in the original db.
	checkSchemasQuery := `SELECT count(*) FROM [SHOW SCHEMAS] WHERE schema_name LIKE 'pg_temp_%'`
	sqlDBRestore.CheckQueryResults(t, checkSchemasQuery, [][]string{{"2"}})

	checkTempTablesQuery := `SELECT table_name FROM [SHOW TABLES] ORDER BY table_name`
	sqlDBRestore.CheckQueryResults(t, checkTempTablesQuery, [][]string{{"t"}, {"t"}})

	// Sanity check that the databases the temporary tables originally belonged to
	// are restored.
	sqlDBRestore.CheckQueryResults(t,
		`SELECT database_name FROM [SHOW DATABASES] ORDER BY database_name`,
		[][]string{{"d1"}, {"d2"}, {"defaultdb"}, {"postgres"}, {"system"}})

	// Check that we can see the comment on the temporary tables before the
	// reconciliation job runs.
	checkCommentQuery := fmt.Sprintf(`SELECT count(comment) FROM system.comments WHERE comment='%s'`,
		comment)
	var commentCount int
	sqlDBRestore.QueryRow(t, checkCommentQuery).Scan(&commentCount)
	require.Equal(t, commentCount, 2)

	// Check that show tables in one of the restored DBs returns the temporary
	// table.
	sqlDBRestore.Exec(t, "USE d1")
	sqlDBRestore.CheckQueryResults(t, checkTempTablesQuery, [][]string{
		{"t"},
	})
	sqlDBRestore.CheckQueryResults(t, checkSchemasQuery, [][]string{{"1"}})

	sqlDBRestore.Exec(t, "USE d2")
	sqlDBRestore.CheckQueryResults(t, checkTempTablesQuery, [][]string{
		{"t"},
	})
	sqlDBRestore.CheckQueryResults(t, checkSchemasQuery, [][]string{{"1"}})

	testutils.SucceedsSoon(t, func() error {
		ch <- timeutil.Now()
		<-finishedCh

		for _, database := range []string{"defaultdb", "d1", "d2"} {
			sqlDBRestore.Exec(t, fmt.Sprintf("USE %s", database))
			// Check that all the synthesized temp schemas have been wiped.
			sqlDBRestore.CheckQueryResults(t, checkSchemasQuery, [][]string{{"0"}})

			// Check that all the temp tables have been wiped.
			sqlDBRestore.CheckQueryResults(t, checkTempTablesQuery, [][]string{})

			// Check that all the temp table comments have been wiped.
			sqlDBRestore.QueryRow(t, checkCommentQuery).Scan(&commentCount)
			require.Equal(t, commentCount, 0)
		}
		return nil
	})
}

func TestRestoreJobEventLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.ScopeWithoutShowLogs(t).Close(t)

	defer jobs.TestingSetProgressThresholds()()

	baseDir := "testdata"
	args := base.TestServerArgs{ExternalIODir: baseDir, Knobs: base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()}}
	params := base.TestClusterArgs{ServerArgs: args}
	tc, sqlDB, _, cleanupFn := backupRestoreTestSetupWithParams(t, singleNode, 1,
		InitManualReplication, params)
	defer cleanupFn()

	var forceFailure bool
	for i := range tc.Servers {
		tc.Servers[i].JobRegistry().(*jobs.Registry).TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
			jobspb.TypeRestore: func(raw jobs.Resumer) jobs.Resumer {
				r := raw.(*restoreResumer)
				r.testingKnobs.beforePublishingDescriptors = func() error {
					if forceFailure {
						return errors.New("testing injected failure")
					}
					return nil
				}
				return r
			},
		}
	}

	sqlDB.Exec(t, `CREATE DATABASE r1`)
	sqlDB.Exec(t, `CREATE TABLE r1.foo (id INT)`)
	sqlDB.Exec(t, `BACKUP DATABASE r1 TO 'nodelocal://0/eventlogging'`)
	sqlDB.Exec(t, `DROP DATABASE r1`)

	beforeRestore := timeutil.Now()
	restoreQuery := `RESTORE DATABASE r1 FROM 'nodelocal://0/eventlogging'`

	var jobID int64
	var unused interface{}
	sqlDB.QueryRow(t, restoreQuery).Scan(&jobID, &unused, &unused, &unused, &unused,
		&unused)

	expectedStatus := []string{string(jobs.StatusSucceeded), string(jobs.StatusRunning)}
	jobstest.CheckEmittedEvents(t, expectedStatus, beforeRestore.UnixNano(), jobID, "restore",
		"RESTORE")

	sqlDB.Exec(t, `DROP DATABASE r1`)

	// Now let's test the events that are emitted when a job fails.
	forceFailure = true
	beforeSecondRestore := timeutil.Now()
	sqlDB.ExpectErrSucceedsSoon(t, "testing injected failure", restoreQuery)

	row := sqlDB.QueryRow(t, "SELECT job_id FROM [SHOW JOBS] WHERE status = 'failed'")
	row.Scan(&jobID)

	expectedStatus = []string{
		string(jobs.StatusFailed), string(jobs.StatusReverting),
		string(jobs.StatusRunning),
	}
	jobstest.CheckEmittedEvents(t, expectedStatus, beforeSecondRestore.UnixNano(), jobID,
		"restore", "RESTORE")
}

func TestBackupOnlyPublicIndexes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "likely slow under race")

	numAccounts := 1000
	chunkSize := int64(100)

	// Create 2 blockers that block the backfill after 3 and 6 chunks have been
	// processed respectively.
	// Expect there to be 10 chunks in an index backfill:
	//  numAccounts / chunkSize = 1000 / 100 = 10 chunks.
	backfillBlockers := []thresholdBlocker{
		makeThresholdBlocker(3),
		makeThresholdBlocker(6),
	}

	// Separately, make a coule of blockers that block all schema change jobs.
	blockBackfills := make(chan struct{})
	// By default allow backfills to proceed.
	close(blockBackfills)

	var chunkCount int32
	serverArgs := base.TestServerArgs{}
	serverArgs.Knobs = base.TestingKnobs{
		// Configure knobs to block the index backfills.
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: chunkSize,
		},
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				curChunk := int(atomic.LoadInt32(&chunkCount))
				for _, blocker := range backfillBlockers {
					blocker.maybeBlock(curChunk)
				}
				atomic.AddInt32(&chunkCount, 1)

				// Separately, block backfills.
				<-blockBackfills

				return nil
			},
			// Flush every chunk during the backfills.
			BulkAdderFlushesEveryBatch: true,
		},
	}
	params := base.TestClusterArgs{ServerArgs: serverArgs}

	ctx := context.Background()
	tc, sqlDB, rawDir, cleanupFn := backupRestoreTestSetupWithParams(
		t, singleNode, numAccounts, InitManualReplication, params,
	)
	defer cleanupFn()
	kvDB := tc.Server(0).DB()

	locationToDir := func(location string) string {
		return strings.Replace(location, localFoo, filepath.Join(rawDir, "foo"), 1)
	}

	// Test timeline:
	//  1. Full backup
	//  2. Backfill started
	//  3. Inc 1
	//  4. Inc 2
	//  5. Backfill completeed
	//  6. Inc 3
	//  7. Drop index
	//  8. Inc 4

	// First take a full backup.
	fullBackup := localFoo + "/full"
	sqlDB.Exec(t, `BACKUP DATABASE data TO $1 WITH revision_history`, fullBackup)
	var dataBankTableID descpb.ID
	sqlDB.QueryRow(t, `SELECT 'data.bank'::regclass::int`).
		Scan(&dataBankTableID)

	fullBackupSpans := getSpansFromManifest(ctx, t, locationToDir(fullBackup))
	require.Equal(t, 1, len(fullBackupSpans))
	require.Equal(t, fmt.Sprintf("/Table/%d/{1-2}", dataBankTableID), fullBackupSpans[0].String())

	// Now we're going to add an index. We should only see the index
	// appear in the backup once it is PUBLIC.
	var g errgroup.Group
	g.Go(func() error {
		// We use the underlying DB since the goroutine should not call t.Fatal.
		_, err := sqlDB.DB.ExecContext(ctx,
			`CREATE INDEX new_balance_idx ON data.bank(balance)`)
		return errors.Wrap(err, "creating index")
	})

	inc1Loc := localFoo + "/inc1"
	inc2Loc := localFoo + "/inc2"

	g.Go(func() error {
		defer backfillBlockers[0].allowToProceed()
		backfillBlockers[0].waitUntilBlocked()

		// Take an incremental backup and assert that it doesn't contain any
		// data. The only added data was from the backfill, which should not
		// be included because they are historical writes.
		_, err := sqlDB.DB.ExecContext(ctx, `BACKUP DATABASE data TO $1 INCREMENTAL FROM $2 WITH revision_history`,
			inc1Loc, fullBackup)
		return errors.Wrap(err, "running inc 1 backup")
	})

	g.Go(func() error {
		defer backfillBlockers[1].allowToProceed()
		backfillBlockers[1].waitUntilBlocked()

		// Take an incremental backup and assert that it doesn't contain any
		// data. The only added data was from the backfill, which should not be
		// included because they are historical writes.
		_, err := sqlDB.DB.ExecContext(ctx, `BACKUP DATABASE data TO $1 INCREMENTAL FROM $2, $3 WITH revision_history`,
			inc2Loc, fullBackup, inc1Loc)
		return errors.Wrap(err, "running inc 2 backup")
	})

	// Wait for the backfill and incremental backup to complete.
	require.NoError(t, g.Wait())

	inc1Spans := getSpansFromManifest(ctx, t, locationToDir(inc1Loc))
	require.Equalf(t, 0, len(inc1Spans), "expected inc1 to not have any data, found %v", inc1Spans)

	inc2Spans := getSpansFromManifest(ctx, t, locationToDir(inc2Loc))
	require.Equalf(t, 0, len(inc2Spans), "expected inc2 to not have any data, found %v", inc2Spans)

	// Take another incremental backup that should only contain the newly added
	// index.
	inc3Loc := localFoo + "/inc3"
	sqlDB.Exec(t, `BACKUP DATABASE data TO $1 INCREMENTAL FROM $2, $3, $4 WITH revision_history`,
		inc3Loc, fullBackup, inc1Loc, inc2Loc)
	inc3Spans := getSpansFromManifest(ctx, t, locationToDir(inc3Loc))
	require.Equal(t, 1, len(inc3Spans))
	require.Equal(t, fmt.Sprintf("/Table/%d/{2-3}", dataBankTableID), inc3Spans[0].String())

	// Drop the index.
	sqlDB.Exec(t, `DROP INDEX new_balance_idx`)

	// Take another incremental backup.
	inc4Loc := localFoo + "/inc4"
	sqlDB.Exec(t, `BACKUP DATABASE data TO $1 INCREMENTAL FROM $2, $3, $4, $5 WITH revision_history`,
		inc4Loc, fullBackup, inc1Loc, inc2Loc, inc3Loc)

	numAccountsStr := strconv.Itoa(numAccounts)

	// Restore the entire chain and check that we got the full indexes.
	{
		sqlDB.Exec(t, `CREATE DATABASE restoredb;`)
		sqlDB.Exec(t, `RESTORE data.bank FROM $1, $2, $3, $4 WITH into_db='restoredb'`,
			fullBackup, inc1Loc, inc2Loc, inc3Loc)
		sqlDB.CheckQueryResults(t, `SELECT count(*) FROM restoredb.bank@[1]`, [][]string{{numAccountsStr}})
		sqlDB.CheckQueryResults(t, `SELECT count(*) FROM restoredb.bank@[2]`, [][]string{{numAccountsStr}})
		kvCount, err := getKVCount(ctx, kvDB, "restoredb", "bank")
		require.NoError(t, err)
		require.Equal(t, 2*numAccounts, kvCount)

		// Cleanup.
		sqlDB.Exec(t, `DROP DATABASE restoredb CASCADE;`)
	}

	// Restore to a time where the index was being added and check that the
	// second index was regenerated entirely.
	{
		blockBackfills = make(chan struct{}) // block the synthesized schema change job
		sqlDB.Exec(t, `CREATE DATABASE restoredb;`)
		sqlDB.Exec(t, `RESTORE data.bank FROM $1, $2, $3 WITH into_db='restoredb';`,
			fullBackup, inc1Loc, inc2Loc)
		sqlDB.CheckQueryResults(t, `SELECT count(*) FROM restoredb.bank@[1]`, [][]string{{numAccountsStr}})
		sqlDB.ExpectErr(t, "index .* not found", `SELECT count(*) FROM restoredb.bank@[2]`)

		// Allow backfills to proceed.
		close(blockBackfills)

		// Wait for the synthesized schema change to finish, and assert that it
		// finishes correctly.
		scQueryRes := sqlDB.QueryStr(t, `SELECT job_id FROM [SHOW JOBS]
		WHERE job_type = 'SCHEMA CHANGE' AND description LIKE 'RESTORING:%'`)
		require.Equal(t, 1, len(scQueryRes),
			`expected only 1 schema change to be generated by the restore`)
		require.Equal(t, 1, len(scQueryRes[0]),
			`expected only 1 column to be returned from query`)
		scJobID, err := strconv.Atoi(scQueryRes[0][0])
		require.NoError(t, err)
		waitForSuccessfulJob(t, tc, jobspb.JobID(scJobID))

		// The synthesized index addition has completed.
		sqlDB.CheckQueryResults(t, `SELECT count(*) FROM restoredb.bank@[2]`, [][]string{{numAccountsStr}})
		kvCount, err := getKVCount(ctx, kvDB, "restoredb", "bank")
		require.NoError(t, err)
		require.Equal(t, 2*numAccounts, kvCount)

		// Cleanup.
		sqlDB.Exec(t, `DROP DATABASE restoredb CASCADE;`)
	}

	// Restore to a time after the index was dropped and double check that we
	// didn't bring back any keys from the dropped index.
	{
		blockBackfills = make(chan struct{}) // block the synthesized schema change job
		sqlDB.Exec(t, `CREATE DATABASE restoredb;`)
		sqlDB.Exec(t, `RESTORE data.bank FROM $1, $2, $3, $4, $5 WITH into_db='restoredb';`,
			fullBackup, inc1Loc, inc2Loc, inc3Loc, inc4Loc)
		sqlDB.CheckQueryResults(t, `SELECT count(*) FROM restoredb.bank@[1]`, [][]string{{numAccountsStr}})
		sqlDB.ExpectErr(t, "index .* not found", `SELECT count(*) FROM restoredb.bank@[2]`)

		// Allow backfills to proceed.
		close(blockBackfills)
		kvCount, err := getKVCount(ctx, kvDB, "restoredb", "bank")
		require.NoError(t, err)
		require.Equal(t, numAccounts, kvCount)

		// Cleanup.
		sqlDB.Exec(t, `DROP DATABASE restoredb CASCADE;`)
	}
}

func TestBackupWorkerFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.WithIssue(t, 64773, "flaky test")
	defer log.Scope(t).Close(t)

	skip.UnderStress(t, "under stress the test unexpectedly surfaces non-retryable errors on"+
		" backup failure")

	allowResponse := make(chan struct{})
	params := base.TestClusterArgs{}
	params.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingResponseFilter: jobutils.BulkOpResponseFilter(&allowResponse),
	}
	params.ServerArgs.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()

	const numAccounts = 100

	tc, _, _, cleanup := backupRestoreTestSetupWithParams(t, multiNode, numAccounts,
		InitManualReplication, params)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)
	defer cleanup()

	var expectedCount int
	sqlDB.QueryRow(t, `SELECT count(*) FROM data.bank`).Scan(&expectedCount)
	query := `BACKUP DATABASE data TO 'userfile:///worker-failure'`
	errCh := make(chan error)
	go func() {
		_, err := conn.Exec(query)
		errCh <- err
	}()
	select {
	case allowResponse <- struct{}{}:
	case err := <-errCh:
		t.Fatalf("%s: query returned before expected: %s", err, query)
	}
	var jobID jobspb.JobID
	sqlDB.QueryRow(t, `SELECT id FROM system.jobs ORDER BY created DESC LIMIT 1`).Scan(&jobID)

	// Shut down a node.
	tc.StopServer(1)

	close(allowResponse)
	// We expect the statement to retry since it should have encountered a
	// retryable error.
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}

	// But the job should be restarted and succeed eventually.
	jobutils.WaitForJob(t, sqlDB, jobID)

	// Drop database and restore to ensure that the backup was successful.
	sqlDB.Exec(t, `DROP DATABASE data`)
	sqlDB.Exec(t, `RESTORE DATABASE data FROM 'userfile:///worker-failure'`)
	var actualCount int
	sqlDB.QueryRow(t, `SELECT count(*) FROM data.bank`).Scan(&actualCount)
	require.Equal(t, expectedCount, actualCount)
}

// Regression test for #66797 ensuring that the span merging optimization
// doesn't produce an error when there are span-merging opportunities on
// descriptor revisions from before the GC threshold of the table.
func TestSpanMergingBeforeGCThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
	defer cleanupFn()
	kvDB := tc.Server(0).DB()

	sqlDB.Exec(t, `
ALTER RANGE default CONFIGURE ZONE USING gc.ttlseconds = 1;
CREATE DATABASE test; USE test;
`)

	// Produce a table with the following indexes over time:
	//
	//       |             |
	//   t@1 |     xxxxxxxx|xxxxxxxxxxxxxxxxxxxxxxx
	//   t@2 |     xxxxxxxx|xxxxxxxxx
	//   t@3 |             |
	//   t@4 |     xxxxxxxx|xxxxxxxxx
	//       ----------------------------------------
	//             t1    gc_tresh    t2            t3
	//
	// The span-merging optimization will first look at t3, find only 1 index and
	// continue It will then look at t1 and see a span-merging opportunity over
	// t@3, but a read at this timestamp should fail as it's older than the GC
	// TTL.
	startTime := timeutil.Now()
	sqlDB.Exec(t, `
CREATE TABLE t (a INT PRIMARY KEY, b INT, c INT, INDEX idx_2 (b), INDEX idx_3 (c), INDEX idx_4 (b, c));
DROP INDEX idx_3;
`)

	clearHistoricalTableVersions := func() {
		// Save the latest value of the descriptor.
		table := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")
		mutTable := tabledesc.NewBuilder(table.TableDesc()).BuildExistingMutableTable()
		mutTable.Version = 0

		// Reset the descriptor table to clear the revisions of the initial table
		// descriptor.
		var b kv.Batch
		descriptorTableSpan := makeTableSpan(keys.DescriptorTableID)
		b.AddRawRequest(&roachpb.RevertRangeRequest{
			RequestHeader: roachpb.RequestHeader{
				Key:    descriptorTableSpan.Key,
				EndKey: descriptorTableSpan.EndKey,
			},
			TargetTime: hlc.Timestamp{WallTime: startTime.UnixNano()},
		})
		b.Header.MaxSpanRequestKeys = sql.RevertTableDefaultBatchSize
		require.NoError(t, kvDB.Run(context.Background(), &b))

		// Re-insert the latest value of the table descriptor.
		desc, err := protoutil.Marshal(mutTable.DescriptorProto())
		require.NoError(t, err)
		insertQ := fmt.Sprintf("SELECT crdb_internal.unsafe_upsert_descriptor(%d, decode('%s', 'hex'), true);",
			table.GetID(), hex.EncodeToString(desc))
		sqlDB.Exec(t, insertQ)
	}

	// Clear previous MVCC versions of the table descriptor so that the oldest
	// version of the table descriptor (which has an MVCC timestamp outside the GC
	// window) will trigger a scan during the span-merging phase of backup
	// planning.
	clearHistoricalTableVersions()

	// Drop all of the secondary indexes since backup first checks the current
	// schema.
	sqlDB.Exec(t, `DROP INDEX idx_2, idx_4`)

	// Wait for the old schema to exceed the GC window.
	time.Sleep(2 * time.Second)

	sqlDB.Exec(t, `BACKUP test.t TO 'nodelocal://1/backup_test' WITH revision_history`)
}

func waitForStatus(t *testing.T, db *sqlutils.SQLRunner, jobID int64, status jobs.Status) error {
	return testutils.SucceedsSoonError(func() error {
		var jobStatus string
		db.QueryRow(t, `SELECT status FROM system.jobs WHERE id = $1`, jobID).Scan(&jobStatus)

		if status != jobs.Status(jobStatus) {
			return errors.Newf("expected jobID %d to have status %, got %s", jobID, status, jobStatus)
		}
		return nil
	})
}

// Test to verify that RESTORE jobs self pause on error when given the
// DEBUG_PAUSE_ON = 'error' option.
func TestRestorePauseOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.ScopeWithoutShowLogs(t).Close(t)

	defer jobs.TestingSetProgressThresholds()()

	baseDir := "testdata"
	args := base.TestServerArgs{ExternalIODir: baseDir, Knobs: base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()}}
	params := base.TestClusterArgs{ServerArgs: args}
	tc, sqlDB, _, cleanupFn := backupRestoreTestSetupWithParams(t, singleNode, 1,
		InitManualReplication, params)
	defer cleanupFn()

	var forceFailure bool
	for i := range tc.Servers {
		tc.Servers[i].JobRegistry().(*jobs.Registry).TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
			jobspb.TypeRestore: func(raw jobs.Resumer) jobs.Resumer {
				r := raw.(*restoreResumer)
				r.testingKnobs.beforePublishingDescriptors = func() error {
					if forceFailure {
						return errors.New("testing injected failure")
					}
					return nil
				}
				return r
			},
		}
	}

	sqlDB.Exec(t, `CREATE DATABASE r1`)
	sqlDB.Exec(t, `CREATE TABLE r1.foo (id INT)`)
	sqlDB.Exec(t, `BACKUP DATABASE r1 TO 'nodelocal://0/eventlogging'`)
	sqlDB.Exec(t, `DROP DATABASE r1`)

	restoreQuery := `RESTORE DATABASE r1 FROM 'nodelocal://0/eventlogging' WITH DEBUG_PAUSE_ON = 'error'`
	findJobQuery := `SELECT job_id FROM [SHOW JOBS] WHERE description LIKE '%RESTORE DATABASE%' ORDER BY created DESC`

	// Verify that a RESTORE job will self pause on an error, but can be resumed
	// after the source of error is fixed.
	{
		var jobID int64
		forceFailure = true

		sqlDB.QueryRow(t, restoreQuery)

		sqlDB.QueryRow(t, findJobQuery).Scan(&jobID)
		if err := waitForStatus(t, sqlDB, jobID, jobs.StatusPaused); err != nil {
			t.Fatal(err)
		}

		forceFailure = false
		sqlDB.Exec(t, "RESUME JOB $1", jobID)

		if err := waitForStatus(t, sqlDB, jobID, jobs.StatusSucceeded); err != nil {
			t.Fatal(err)
		}
	}

	// Verify that a RESTORE job will self pause on an error and can be canceled.
	{
		var jobID int64
		forceFailure = true
		sqlDB.Exec(t, `DROP DATABASE r1`)
		sqlDB.QueryRow(t, restoreQuery)
		sqlDB.QueryRow(t, findJobQuery).Scan(&jobID)
		if err := waitForStatus(t, sqlDB, jobID, jobs.StatusPaused); err != nil {
			t.Fatal(err)
		}

		sqlDB.Exec(t, "CANCEL JOB $1", jobID)

		if err := waitForStatus(t, sqlDB, jobID, jobs.StatusCanceled); err != nil {
			t.Fatal(err)
		}
	}
}

// TestDroppedDescriptorRevisionAndSystemDBIDClash is a regression test for a
// discrepancy in the descriptor resolution logic during restore planning and
// execution.
//
// While the resolution logic in restore planning filtered out descriptor
// revisions in the dropped state, the logic in execution did not do this. As a
// a result of this, the restore job would process additional descriptors (the
// dropped revisions). In the case of full cluster restores, the planning phase
// picks an id higher than all restored desc ids, for the tempSystemDB. The
// additional dropped descriptor revisions during execution could have the same
// id as the tempSystemDB. This id clash would cause issues when processing
// descriptor rewrites which are keyed on the descriptor id.
//
// Table and database restores are not affected by this bug since we filter the
// descriptors during execution based on the descriptor rewrites we allocated in
// planning. Since no additional entries for system tables are added to the
// rewrites, we expect to filter out all dropped revisions since there will be
// no rewrites allocated for them in the first place.
func TestDroppedDescriptorRevisionAndSystemDBIDClash(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	_, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, 1, InitManualReplication)
	defer cleanupFn()

	sqlDB.Exec(t, `
CREATE TABLE foo (id INT);
BACKUP TO 'nodelocal://0/foo' WITH revision_history;
DROP TABLE foo;
`)

	var aost string
	sqlDB.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&aost)
	sqlDB.Exec(t, `BACKUP TO $1 WITH revision_history`, localFoo)

	_, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir,
		InitManualReplication, base.TestClusterArgs{})
	defer cleanupEmptyCluster()
	sqlDBRestore.Exec(t, "RESTORE FROM $1 AS OF SYSTEM TIME "+aost, localFoo)
}

// TestRestoreNewDatabaseName tests the new_db_name optional feature for single database
// restores, which allows the user to rename the database they intend to restore.
func TestRestoreNewDatabaseName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	sqlDB.Exec(t, `CREATE DATABASE fkdb`)
	sqlDB.Exec(t, `CREATE TABLE fkdb.fk (ind INT)`)

	for i := 0; i < 10; i++ {
		sqlDB.Exec(t, `INSERT INTO fkdb.fk (ind) VALUES ($1)`, i)
	}
	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)

	// Ensure restore fails with new_db_name on cluster, table, and multiple database restores
	t.Run("new_db_name syntax checks", func(t *testing.T) {
		expectedErr := "new_db_name can only be used for RESTORE DATABASE with a single target database"

		sqlDB.ExpectErr(t, expectedErr, "RESTORE FROM $1 with new_db_name = 'new_fkdb'", localFoo)

		sqlDB.ExpectErr(t, expectedErr, "RESTORE DATABASE fkdb, "+
			"data FROM $1 with new_db_name = 'new_fkdb'", localFoo)

		sqlDB.ExpectErr(t, expectedErr, "RESTORE TABLE fkdb.fk FROM $1 with new_db_name = 'new_fkdb'",
			localFoo)
	})

	// Should fail because 'fkbd' database is still in cluster
	sqlDB.ExpectErr(t, `database "fkdb" already exists`,
		"RESTORE DATABASE fkdb FROM $1", localFoo)

	// Should pass because 'new_fkdb' is not in cluster
	sqlDB.Exec(t, "RESTORE DATABASE fkdb FROM $1 WITH new_db_name = 'new_fkdb'", localFoo)

	// Verify restored database is in cluster with new name
	sqlDB.CheckQueryResults(t,
		`SELECT database_name FROM [SHOW DATABASES] WHERE database_name = 'new_fkdb'`,
		[][]string{{"new_fkdb"}})

	// Verify table was properly restored
	sqlDB.CheckQueryResults(t, `SELECT count(*) FROM new_fkdb.fk`,
		[][]string{{"10"}})

	// Should fail because we just restored new_fkbd into cluster
	sqlDB.ExpectErr(t, `database "new_fkdb" already exists`,
		"RESTORE DATABASE fkdb FROM $1 WITH new_db_name = 'new_fkdb'", localFoo)
}

// TestRestoreRemappingOfExistingUDTInColExpr is a regression test for a nil
// pointer exception when restoring tables that point to existing types. When
// updating the back references of the existing types we would index into a map
// keyed on pre-rewrite IDs using a post rewrite ID.
func TestRestoreRemappingOfExistingUDTInColExpr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	sqlDB.Exec(t, `
CREATE TYPE status AS ENUM ('open', 'closed', 'inactive');
CREATE TABLE foo (id INT PRIMARY KEY, what status default 'open');
BACKUP DATABASE data to 'nodelocal://0/foo';
DROP TABLE foo CASCADE;
DROP TYPE status;
CREATE TYPE status AS ENUM ('open', 'closed', 'inactive');
RESTORE TABLE foo FROM 'nodelocal://0/foo';
`)
}

// TestGCDropIndexSpanExpansion is a regression test for
// https://github.com/cockroachdb/cockroach/issues/72263.
func TestGCDropIndexSpanExpansion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "takes >1 min under race")

	aboutToGC := make(chan struct{})
	allowGC := make(chan struct{})
	var gcJobID jobspb.JobID
	baseDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{
		ExternalIODir: baseDir,
		Knobs: base.TestingKnobs{
			GCJob: &sql.GCJobTestingKnobs{RunBeforePerformGC: func(id jobspb.JobID) error {
				gcJobID = id
				aboutToGC <- struct{}{}
				<-allowGC
				return nil
			}},
		},
	}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlRunner := sqlutils.MakeSQLRunner(conn)
	sqlRunner.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`) // speeds up the test

	sqlRunner.Exec(t, `
CREATE DATABASE test; USE test;
CREATE TABLE foo (id INT PRIMARY KEY, id2 INT, id3 INT, INDEX bar (id2), INDEX baz(id3));
ALTER INDEX foo@bar CONFIGURE ZONE USING gc.ttlseconds = '1';
INSERT INTO foo VALUES (1, 2, 3);
DROP INDEX foo@bar;
`)

	// Wait until the index is about to get gc'ed.
	<-aboutToGC

	// Take a full backup with revision history so it includes the PUBLIC version
	// of index `bar` in the backed up spans.
	sqlRunner.Exec(t, `BACKUP INTO 'nodelocal://0/foo' WITH revision_history`)

	// Take an incremental backup with revision history. This backup will not
	// include the dropped index span.
	sqlRunner.Exec(t, `BACKUP INTO LATEST IN 'nodelocal://0/foo' WITH revision_history`)

	// Allow the GC to complete.
	close(allowGC)

	// Wait for the GC to complete.
	jobutils.WaitForJob(t, sqlRunner, gcJobID)
	waitForTableSplit(t, conn, "foo", "test")

	// This backup should succeed since the spans being backed up have a default
	// GC TTL of 25 hours.
	sqlRunner.Exec(t, `BACKUP INTO LATEST IN 'nodelocal://0/foo' WITH revision_history`)
}

// TestRestoreSchemaDescriptorsRollBack is a regression test that ensures that a
// failed restore properly cleans up the database, schema and the database
// descriptors Schemas slice. Prior to the fix, the order in which we performed
// the cleanup would leave a dangling entry in the system.descriptor table for
// the cleaned up database. This caused the subsequent backup to fail when
// resolving the targets to backup with a "duplicate database" error.
func TestRestoreSchemaDescriptorsRollBack(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
	defer cleanupFn()

	for _, server := range tc.Servers {
		registry := server.JobRegistry().(*jobs.Registry)
		registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
			jobspb.TypeRestore: func(raw jobs.Resumer) jobs.Resumer {
				r := raw.(*restoreResumer)
				r.testingKnobs.beforePublishingDescriptors = func() error {
					return errors.New("boom")
				}
				return r
			},
		}
	}

	sqlDB.Exec(t, `
CREATE DATABASE db;
CREATE SCHEMA db.s;
`)

	sqlDB.Exec(t, `BACKUP DATABASE db TO 'nodelocal://0/test/1'`)
	sqlDB.Exec(t, `DROP DATABASE db`)
	sqlDB.ExpectErr(t, "boom", `RESTORE DATABASE db FROM 'nodelocal://0/test/1'`)

	sqlDB.Exec(t, `
CREATE DATABASE db;
CREATE SCHEMA db.s;
`)

	sqlDB.Exec(t, `BACKUP DATABASE db TO 'nodelocal://0/test/2'`)
}

func TestBackupMemMonitorSSTSinkQueueSize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	memoryMonitor := mon.NewMonitor(
		"test-mem",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		cluster.MakeTestingClusterSettings(),
	)
	ctx := context.Background()
	byteLimit := 14 << 20 // 14 MiB
	memoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(int64(byteLimit)))
	defer memoryMonitor.Stop(ctx)
	params := base.TestClusterArgs{}
	knobs := base.TestingKnobs{
		DistSQL: &execinfra.TestingKnobs{
			BackupRestoreTestingKnobs: &sql.BackupRestoreTestingKnobs{
				BackupMemMonitor: memoryMonitor,
			},
		},
	}
	params.ServerArgs.Knobs = knobs

	const numAccounts = 100

	_, sqlDB, _, cleanup := backupRestoreTestSetupWithParams(t, singleNode, numAccounts,
		InitManualReplication, params)
	defer cleanup()

	// Run a backup and expect the Grow() for the sstSink to return a memory error
	// since the default queue byte size is 16MiB.
	sqlDB.ExpectErr(t, "failed to reserve memory for sstSink queue", `BACKUP INTO 'nodelocal://0/foo'`)

	// Reduce the queue byte size cluster setting.
	sqlDB.Exec(t, `SET CLUSTER SETTING bulkio.backup.merge_file_buffer_size = '13MiB'`)

	// Now the backup should succeed because it is below the `byteLimit`.
	sqlDB.Exec(t, `BACKUP INTO 'nodelocal://0/bar'`)
}

// TestBackupRestoreSeperateIncrementalPrefix tests that a backup/restore round
// trip using the 'incremental_location' parameter restores the same db as a BR
// round trip without the parameter.
func TestBackupRestoreSeparateIncrementalPrefix(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	const c1, c2, c3 = `nodelocal://0/full/`, `nodelocal://1/full/`, `nodelocal://2/full/`
	const i1, i2, i3 = `nodelocal://0/inc/`, `nodelocal://1/inc/`, `nodelocal://2/inc/`

	collections := []string{
		fmt.Sprintf("'%s?COCKROACH_LOCALITY=%s'", c1, url.QueryEscape("default")),
		fmt.Sprintf("'%s?COCKROACH_LOCALITY=%s'", c2, url.QueryEscape("dc=dc1")),
		fmt.Sprintf("'%s?COCKROACH_LOCALITY=%s'", c3, url.QueryEscape("dc=dc2")),
	}

	incrementals := []string{
		fmt.Sprintf("'%s?COCKROACH_LOCALITY=%s'", i1, url.QueryEscape("default")),
		fmt.Sprintf("'%s?COCKROACH_LOCALITY=%s'", i2, url.QueryEscape("dc=dc1")),
		fmt.Sprintf("'%s?COCKROACH_LOCALITY=%s'", i3, url.QueryEscape("dc=dc2")),
	}
	tests := []struct {
		dest []string
		inc  []string
	}{
		{dest: []string{collections[0]}, inc: []string{incrementals[0]}},
		{dest: collections, inc: incrementals},
	}

	for _, br := range tests {

		dest := strings.Join(br.dest, ", ")
		inc := strings.Join(br.inc, ", ")

		if len(br.dest) > 1 {
			dest = "(" + dest + ")"
			inc = "(" + inc + ")"
		}
		// create db
		sqlDB.Exec(t, `CREATE DATABASE fkdb`)
		sqlDB.Exec(t, `CREATE TABLE fkdb.fk (ind INT)`)

		for i := 0; i < 10; i++ {
			sqlDB.Exec(t, `INSERT INTO fkdb.fk (ind) VALUES ($1)`, i)
		}
		fb := fmt.Sprintf("BACKUP DATABASE fkdb INTO %s", dest)
		sqlDB.Exec(t, fb)

		sqlDB.Exec(t, `INSERT INTO fkdb.fk (ind) VALUES ($1)`, 200)

		sib := fmt.Sprintf("BACKUP DATABASE fkdb INTO LATEST IN %s WITH incremental_location = %s", dest, inc)
		sqlDB.Exec(t, sib)
		sir := fmt.Sprintf("RESTORE DATABASE fkdb FROM LATEST IN %s WITH new_db_name = 'inc_fkdb', incremental_location = %s", dest, inc)
		sqlDB.Exec(t, sir)

		ib := fmt.Sprintf("BACKUP DATABASE fkdb INTO LATEST IN %s", dest)
		sqlDB.Exec(t, ib)
		ir := fmt.Sprintf("RESTORE DATABASE fkdb FROM LATEST IN %s WITH new_db_name = 'trad_fkdb'", dest)
		sqlDB.Exec(t, ir)

		sqlDB.CheckQueryResults(t, `SELECT * FROM trad_fkdb.fk`, sqlDB.QueryStr(t, `SELECT * FROM inc_fkdb.fk`))

		sqlDB.Exec(t, "DROP DATABASE fkdb")
		sqlDB.Exec(t, "DROP DATABASE trad_fkdb;")
		sqlDB.Exec(t, "DROP DATABASE inc_fkdb;")
	}
}

func TestExcludeDataFromBackupAndRestore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc, sqlDB, iodir, cleanupFn := backupRestoreTestSetupWithParams(t, singleNode, 10,
		InitManualReplication, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(), // speeds up test
					SpanConfig: &spanconfig.TestingKnobs{
						SQLWatcherCheckpointNoopsEveryDurationOverride: 100 * time.Millisecond,
					},
				},
			},
		})
	defer cleanupFn()

	_, restoreDB, cleanup := backupRestoreTestSetupEmpty(t, singleNode, iodir, InitManualReplication,
		base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(), // speeds up test
				},
			},
		})
	defer cleanup()

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`)
	conn := tc.Conns[0]

	sqlDB.Exec(t, `CREATE TABLE data.foo (id INT, INDEX bar(id))`)
	sqlDB.Exec(t, `INSERT INTO data.foo select * from generate_series(1,10)`)

	// Create another table.
	sqlDB.Exec(t, `CREATE TABLE data.bar (id INT, INDEX bar(id))`)
	sqlDB.Exec(t, `INSERT INTO data.bar select * from generate_series(1,10)`)

	// Set foo to exclude_data_from_backup and back it up. The ExportRequest
	// should be a noop and backup no data.
	sqlDB.Exec(t, `ALTER TABLE data.foo SET (exclude_data_from_backup = true)`)
	waitForTableSplit(t, conn, "foo", "data")
	waitForReplicaFieldToBeSet(t, tc, conn, "foo", "data", func(r *kvserver.Replica) (bool, error) {
		if !r.ExcludeDataFromBackup() {
			return false, errors.New("waiting for exclude_data_from_backup to be applied")
		}
		return true, nil
	})
	waitForTableSplit(t, conn, "bar", "data")
	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, localFoo)

	restoreDB.Exec(t, `RESTORE DATABASE data FROM $1`, localFoo)
	require.Len(t, restoreDB.QueryStr(t, `SELECT * FROM data.foo`), 0)
	require.Len(t, restoreDB.QueryStr(t, `SELECT * FROM data.bar`), 10)
}

// TestExportRequestBelowGCThresholdOnDataExcludedFromBackup tests that a
// `BatchTimestampBeforeGCError` on an ExportRequest targeting a table that has
// been marked as excluded from backup, does not cause the backup to fail.
func TestExportRequestBelowGCThresholdOnDataExcludedFromBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStressRace(t, "test is too slow to run under race")

	ctx := context.Background()
	localExternalDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	args := base.TestClusterArgs{}
	args.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		DisableGCQueue:            true,
		DisableLastProcessedCheck: true,
	}
	args.ServerArgs.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
	args.ServerArgs.ExternalIODir = localExternalDir
	tc := testcluster.StartTestCluster(t, 3, args)
	defer tc.Stopper().Stop(ctx)

	tc.WaitForNodeLiveness(t)
	require.NoError(t, tc.WaitForFullReplication())

	for _, server := range tc.Servers {
		registry := server.JobRegistry().(*jobs.Registry)
		registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
			jobspb.TypeBackup: func(raw jobs.Resumer) jobs.Resumer {
				r := raw.(*backupResumer)
				r.testingKnobs.ignoreProtectedTimestamps = true
				return r
			},
		}
	}
	conn := tc.ServerConn(0)
	_, err := conn.Exec("CREATE TABLE foo (k INT PRIMARY KEY, v BYTES)")
	require.NoError(t, err)

	_, err = conn.Exec("SET CLUSTER SETTING kv.protectedts.poll_interval = '10ms';")
	require.NoError(t, err)

	_, err = conn.Exec("SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'") // speeds up the test
	require.NoError(t, err)

	const tableRangeMaxBytes = 1 << 18
	_, err = conn.Exec("ALTER TABLE foo CONFIGURE ZONE USING "+
		"gc.ttlseconds = 1, range_max_bytes = $1, range_min_bytes = 1<<10;", tableRangeMaxBytes)
	require.NoError(t, err)

	rRand, _ := randutil.NewTestRand()
	upsertUntilBackpressure := func() {
		for {
			_, err := conn.Exec("UPSERT INTO foo VALUES (1, $1)",
				randutil.RandBytes(rRand, 1<<15))
			if testutils.IsError(err, "backpressure") {
				break
			}
			require.NoError(t, err)
		}
	}
	const processedPattern = `(?s)shouldQueue=true.*processing replica.*GC score after GC`
	processedRegexp := regexp.MustCompile(processedPattern)

	gcSoon := func() {
		testutils.SucceedsSoon(t, func() error {
			upsertUntilBackpressure()
			s, repl := getStoreAndReplica(t, tc, conn, "foo", "defaultdb")
			trace, _, err := s.ManuallyEnqueue(ctx, "mvccGC", repl, false)
			require.NoError(t, err)
			if !processedRegexp.MatchString(trace.String()) {
				return errors.Errorf("%q does not match %q", trace.String(), processedRegexp)
			}
			return nil
		})
	}

	waitForTableSplit(t, conn, "foo", "defaultdb")
	waitForReplicaFieldToBeSet(t, tc, conn, "foo", "defaultdb", func(r *kvserver.Replica) (bool, error) {
		if r.GetMaxBytes() != tableRangeMaxBytes {
			return false, errors.New("waiting for range_max_bytes to be applied")
		}
		return true, nil
	})

	var tsBefore string
	require.NoError(t, conn.QueryRow("SELECT cluster_logical_timestamp()").Scan(&tsBefore))
	gcSoon()

	_, err = conn.Exec(fmt.Sprintf("BACKUP TABLE foo TO $1 AS OF SYSTEM TIME '%s'", tsBefore), localFoo)
	testutils.IsError(err, "must be after replica GC threshold")

	_, err = conn.Exec(`ALTER TABLE foo SET (exclude_data_from_backup = true)`)
	require.NoError(t, err)
	waitForReplicaFieldToBeSet(t, tc, conn, "foo", "defaultdb", func(r *kvserver.Replica) (bool, error) {
		if !r.ExcludeDataFromBackup() {
			return false, errors.New("waiting for exclude_data_from_backup to be applied")
		}
		return true, nil
	})

	_, err = conn.Exec(fmt.Sprintf("BACKUP TABLE foo TO $1 AS OF SYSTEM TIME '%s'", tsBefore), localFoo)
	require.NoError(t, err)
}

// TestBackupRestoreSystemUsers tests RESTORE SYSTEM USERS feature which allows user to
// restore users from a backup into current cluster and regrant roles.
func TestBackupRestoreSystemUsers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	sqlDB, tempDir, cleanupFn := createEmptyCluster(t, singleNode)
	_, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{})
	defer cleanupFn()
	defer cleanupEmptyCluster()

	sqlDB.Exec(t, `CREATE USER app; CREATE USER test`)
	sqlDB.Exec(t, `CREATE ROLE app_role; CREATE ROLE test_role`)
	sqlDB.Exec(t, `GRANT app_role TO test_role;`) // 'test_role' is a member of 'app_role'
	sqlDB.Exec(t, `GRANT admin, app_role TO app; GRANT test_role TO test`)
	sqlDB.Exec(t, `CREATE DATABASE db; CREATE TABLE db.foo (ind INT)`)
	sqlDB.Exec(t, `BACKUP TO $1`, localFoo+"/1")
	sqlDB.Exec(t, `BACKUP DATABASE db TO $1`, localFoo+"/2")
	sqlDB.Exec(t, `BACKUP TABLE system.users TO $1`, localFoo+"/3")

	// User 'test' exists in both clusters but 'app' only exists in the backup
	sqlDBRestore.Exec(t, `CREATE USER test`)
	sqlDBRestore.Exec(t, `CREATE DATABASE db`)
	// Create multiple databases to make max descriptor ID be larger than max descriptor ID
	// in the backup to test if we correctly generate new descriptor IDs
	sqlDBRestore.Exec(t, `CREATE DATABASE db1; CREATE DATABASE db2; CREATE DATABASE db3`)

	t.Run("system users", func(t *testing.T) {
		sqlDBRestore.Exec(t, "RESTORE SYSTEM USERS FROM $1", localFoo+"/1")

		// Role 'app_role' and user 'app' will be added, and 'app' is granted with 'app_role'
		// User test will remain untouched with no role granted
		sqlDBRestore.CheckQueryResults(t, "SELECT * FROM system.users", [][]string{
			{"admin", "", "true"},
			{"app", "NULL", "false"},
			{"app_role", "NULL", "true"},
			{"root", "", "false"},
			{"test", "NULL", "false"},
			{"test_role", "NULL", "true"},
		})
		sqlDBRestore.CheckQueryResults(t, "SELECT * FROM system.role_members", [][]string{
			{"admin", "app", "false"},
			{"admin", "root", "true"},
			{"app_role", "app", "false"},
			{"app_role", "test_role", "false"},
		})
		sqlDBRestore.CheckQueryResults(t, "SHOW USERS", [][]string{
			{"admin", "", "{}"},
			{"app", "", "{admin,app_role}"},
			{"app_role", "", "{}"},
			{"root", "", "{admin}"},
			{"test", "", "{}"},
			{"test_role", "", "{app_role}"},
		})
	})

	t.Run("restore-from-backup-with-no-system-users", func(t *testing.T) {
		sqlDBRestore.ExpectErr(t, "cannot restore system users as no system.users table in the backup",
			"RESTORE SYSTEM USERS FROM $1", localFoo+"/2")
	})

	_, sqlDBRestore1, cleanupEmptyCluster1 := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{})
	defer cleanupEmptyCluster1()
	t.Run("restore-from-backup-with-no-system-role-members", func(t *testing.T) {
		sqlDBRestore1.Exec(t, "RESTORE SYSTEM USERS FROM $1", localFoo+"/3")

		sqlDBRestore1.CheckQueryResults(t, "SELECT * FROM system.users", [][]string{
			{"admin", "", "true"},
			{"app", "NULL", "false"},
			{"app_role", "NULL", "true"},
			{"root", "", "false"},
			{"test", "NULL", "false"},
			{"test_role", "NULL", "true"},
		})
		sqlDBRestore1.CheckQueryResults(t, "SELECT * FROM system.role_members", [][]string{
			{"admin", "root", "true"},
		})
		sqlDBRestore1.CheckQueryResults(t, "SHOW USERS", [][]string{
			{"admin", "", "{}"},
			{"app", "", "{}"},
			{"app_role", "", "{}"},
			{"root", "", "{admin}"},
			{"test", "", "{}"},
			{"test_role", "", "{}"},
		})
	})
}
