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

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
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
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestBackupRestoreStatementResult(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
		backupManifestFile := backupDir + "/" + BackupManifestName
		backupManifestBytes, err := ioutil.ReadFile(backupManifestFile)
		if err != nil {
			t.Fatal(err)
		}
		fileType := http.DetectContentType(backupManifestBytes)
		require.Equal(t, ZipType, fileType)
	})

	sqlDB.Exec(t, "CREATE DATABASE data2")

	if err := verifyBackupRestoreStatementResult(
		t, sqlDB, "RESTORE data.* FROM $1 WITH OPTIONS ('into_db'='data2')", LocalFoo,
	); err != nil {
		t.Fatal(err)
	}
}

func TestBackupRestoreSingleNodeLocal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1000
	ctx, tc, _, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	backupAndRestore(ctx, t, tc, []string{LocalFoo}, []string{LocalFoo}, numAccounts)
}

func TestBackupRestoreMultiNodeLocal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1000
	ctx, tc, _, _, cleanupFn := BackupRestoreTestSetup(t, MultiNode, numAccounts, InitNone)
	defer cleanupFn()

	backupAndRestore(ctx, t, tc, []string{LocalFoo}, []string{LocalFoo}, numAccounts)
}

func TestBackupRestoreMultiNodeRemote(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1000
	ctx, tc, _, _, cleanupFn := BackupRestoreTestSetup(t, MultiNode, numAccounts, InitNone)
	defer cleanupFn()
	// Backing up to node2's local file system
	remoteFoo := "nodelocal://2/foo"

	backupAndRestore(ctx, t, tc, []string{remoteFoo}, []string{LocalFoo}, numAccounts)
}

func TestBackupRestorePartitioned(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

	const numAccounts = 1000
	ctx, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, MultiNode, numAccounts, InitNone)
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

	backups := []interface{}{
		fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", localFoo1, url.QueryEscape("default")),
		fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", localFoo2, url.QueryEscape("dc=dc1")),
		fmt.Sprintf("%s?COCKROACH_LOCALITY=%s", localFoo3, url.QueryEscape("dc=dc2")),
	}

	var tsBefore, ts1, ts2 string
	sqlDB.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&tsBefore)

	sqlDB.Exec(t, "BACKUP TO ($1, $2, $3) AS OF SYSTEM TIME "+tsBefore, backups...)

	sqlDB.QueryRow(t, "UPDATE data.bank SET balance = 100 RETURNING cluster_logical_timestamp()").Scan(&ts1)
	sqlDB.Exec(t, "BACKUP TO ($1, $2, $3) AS OF SYSTEM TIME "+ts1, backups...)

	sqlDB.QueryRow(t, "UPDATE data.bank SET balance = 200 RETURNING cluster_logical_timestamp()").Scan(&ts2)
	rowsTS2 := sqlDB.QueryStr(t, "SELECT * from data.bank ORDER BY id")
	sqlDB.Exec(t, "BACKUP TO ($1, $2, $3) AS OF SYSTEM TIME "+ts2, backups...)

	sqlDB.Exec(t, "ALTER TABLE data.bank RENAME TO data.renamed")
	sqlDB.Exec(t, "BACKUP TO ($1, $2, $3)", backups...)

	sqlDB.ExpectErr(t, "cannot append a backup of specific", "BACKUP system.users TO ($1, $2, $3)", backups...)

	sqlDB.Exec(t, "DROP DATABASE data CASCADE")
	sqlDB.Exec(t, "RESTORE DATABASE data FROM ($1, $2, $3)", backups...)
	sqlDB.ExpectErr(t, "relation \"data.bank\" does not exist", "SELECT * FROM data.bank ORDER BY id")
	sqlDB.CheckQueryResults(t, "SELECT * from data.renamed ORDER BY id", rowsTS2)

	// TODO(dt): test restoring to other backups via AOST.
}

func TestBackupRestorePartitionedMergeDirectories(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

	const numAccounts = 0
	ctx, tc, _, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	backupAndRestore(ctx, t, tc, []string{LocalFoo}, []string{LocalFoo}, numAccounts)
}

// Regression test for #16008. In short, the way RESTORE constructed split keys
// for tables with negative primary key data caused AdminSplit to fail.
func TestBackupRestoreNegativePrimaryKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

	// Start a new cluster to restore into.
	{
		// If the backup is on nodelocal, we need to determine which node it's on.
		// Othewise, default to 0.
		backupNodeID := 0
		uri, err := url.Parse(backupURIs[0])
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

		var unused string
		var restored struct {
			rows, idx, bytes int64
		}

		restoreURIFmtString, restoreURIArgs := uriFmtStringAndArgs(restoreURIs)
		restoreQuery := fmt.Sprintf("RESTORE DATABASE DATA FROM %s", restoreURIFmtString)
		sqlDBRestore.QueryRow(t, restoreQuery, restoreURIArgs...).Scan(
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
		sqlDBRestore.QueryRow(t, `SELECT count(*) FROM data.bank`).Scan(&rowCount)
		if rowCount != int64(numAccounts) {
			t.Fatalf("expected %d rows but found %d", numAccounts, rowCount)
		}

		sqlDBRestore.QueryRow(t, `SELECT count(*) FROM data.bank@balance_idx`).Scan(&rowCount)
		if rowCount != int64(numAccounts) {
			t.Fatalf("expected %d rows but found %d", numAccounts, rowCount)
		}

		// Verify there's no /Table/51 - /Table/51/1 empty span.
		{
			var count int
			sqlDBRestore.QueryRow(t, `
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
}

func TestBackupRestoreSystemTables(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
		DescriptorIDs: sqlbase.IDs{
			sqlbase.ID(backupDatabaseID),
			sqlbase.ID(backupTableID),
		},
	}); err != nil {
		t.Fatal(err)
	}

	sqlDB.Exec(t, `RESTORE TABLE bank FROM $1, $2 WITH OPTIONS ('into_db'='restoredb')`, fullDir, incDir)
	if err := jobutils.VerifySystemJob(t, sqlDB, 0, jobspb.TypeRestore, jobs.StatusSucceeded, jobs.Record{
		Username: security.RootUser,
		Description: fmt.Sprintf(
			`RESTORE TABLE bank FROM '%s', '%s' WITH into_db = 'restoredb'`,
			sanitizedFullDir+"redacted", sanitizedIncDir+"redacted",
		),
		DescriptorIDs: sqlbase.IDs{
			sqlbase.ID(restoreDatabaseID + 1),
		},
	}); err != nil {
		t.Fatal(err)
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
	do(`RESTORE data.* FROM $1 WITH OPTIONS ('into_db'='restoredb')`, checkRestore)
}

func TestBackupRestoreSystemJobsProgress(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

	t.Skip("https://github.com/cockroachdb/cockroach/issues/33357")

	defer func(oldInterval time.Duration) {
		BackupCheckpointInterval = oldInterval
	}(BackupCheckpointInterval)
	BackupCheckpointInterval = 0

	var checkpointPath string

	checkBackup := func(ctx context.Context, ip inProgressState) error {
		checkpointPath = filepath.Join(ip.dir, ip.name, BackupManifestCheckpointName)
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
	descriptorIDs []sqlbase.ID,
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

	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond

	ctx := context.Background()

	const numAccounts = 1000
	_, tc, outerDB, dir, cleanupFn := BackupRestoreTestSetup(t, MultiNode, numAccounts, InitNone)
	defer cleanupFn()

	backupTableDesc := sqlbase.TestingGetTableDescriptor(tc.Servers[0].DB(), keys.SystemSQLCodec, "data", "bank")

	t.Run("backup", func(t *testing.T) {
		sqlDB := sqlutils.MakeSQLRunner(outerDB.DB)
		backupStartKey := backupTableDesc.PrimaryIndexSpan(keys.SystemSQLCodec).Key
		backupEndKey, err := sqlbase.TestingMakePrimaryIndexKey(backupTableDesc, numAccounts/2)
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
		checkpointFile := backupDir + "/" + BackupManifestCheckpointName
		if err := ioutil.WriteFile(checkpointFile, mockManifest, 0644); err != nil {
			t.Fatal(err)
		}
		createAndWaitForJob(
			t, sqlDB, []sqlbase.ID{backupTableDesc.ID},
			jobspb.BackupDetails{
				EndTime:        tc.Servers[0].Clock().Now(),
				URI:            "nodelocal://0/backup",
				BackupManifest: mockManifest,
			},
			jobspb.BackupProgress{},
		)

		// If the backup properly took the (incorrect) checkpoint into account, it
		// won't have tried to re-export any keys within backupCompletedSpan.
		backupManifestFile := backupDir + "/" + BackupManifestName
		backupManifestBytes, err := ioutil.ReadFile(backupManifestFile)
		if err != nil {
			t.Fatal(err)
		}
		fileType := http.DetectContentType(backupManifestBytes)
		if fileType == ZipType {
			backupManifestBytes, err = DecompressData(backupManifestBytes)
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
		restoreHighWaterMark, err := sqlbase.TestingMakePrimaryIndexKey(backupTableDesc, numAccounts/2)
		if err != nil {
			t.Fatal(err)
		}
		createAndWaitForJob(
			t, sqlDB, []sqlbase.ID{restoreTableID},
			jobspb.RestoreDetails{
				DescriptorRewrites: map[sqlbase.ID]*jobspb.RestoreDetails_DescriptorRewrite{
					backupTableDesc.ID: {
						ParentID: sqlbase.ID(restoreDatabaseID),
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
	t.Skip("#24136")

	// force every call to update
	defer jobs.TestingSetProgressThresholds()()

	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond

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
			`RESTORE TABLE orig_fkdb.fk FROM $1 WITH OPTIONS ('skip_missing_foreign_keys', 'into_db'='restore_fkdb')`,
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
			`RESTORE TABLE data.* FROM $1 WITH OPTIONS ('into_db'='pause')`,
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
			`RESTORE TABLE data.* FROM $1 WITH OPTIONS ('into_db'='cancel')`,
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

// TestRestoreFailCleanup tests that a failed RESTORE is cleaned up.
func TestRestoreFailCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
	_, _, sqlDB, dir, cleanup := backupRestoreTestSetupWithParams(t, singleNode, numAccounts,
		InitNone, base.TestClusterArgs{ServerArgs: params})
	defer cleanup()

	dir = dir + "/foo"

	sqlDB.Exec(t, `CREATE DATABASE restore`)
	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, LocalFoo)
	// Bugger the backup by removing the SST files.
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			t.Fatal(err)
		}
		if info.Name() == BackupManifestName || !strings.HasSuffix(path, ".sst") {
			return nil
		}
		return os.Remove(path)
	}); err != nil {
		t.Fatal(err)
	}
	sqlDB.ExpectErr(
		t, "sst: no such file",
		`RESTORE data.* FROM $1 WITH OPTIONS ('into_db'='restore')`, LocalFoo,
	)
	// Verify the failed RESTORE added some DROP tables.
	sqlDB.CheckQueryResults(t,
		`SELECT name FROM crdb_internal.tables WHERE database_name = 'restore' AND state = 'DROP'`,
		[][]string{{"bank"}},
	)
}

// TestRestoreFailDatabaseCleanup tests that a failed RESTORE is cleaned up
// when restoring an entire database.
func TestRestoreFailDatabaseCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params := base.TestServerArgs{}
	// Disable external processing of mutations so that the final check of
	// crdb_internal.tables is guaranteed to not be cleaned up. Although this
	// was never observed by a stress test, it is here for safety.
	blockGC := make(chan struct{})
	params.Knobs.GCJob = &sql.GCJobTestingKnobs{RunBeforeResume: func(_ int64) error { <-blockGC; return nil }}

	const numAccounts = 1000
	_, _, sqlDB, dir, cleanup := backupRestoreTestSetupWithParams(t, singleNode, numAccounts,
		InitNone, base.TestClusterArgs{ServerArgs: params})
	defer cleanup()

	dir = dir + "/foo"

	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, LocalFoo)
	// Bugger the backup by removing the SST files.
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			t.Fatal(err)
		}
		if info.Name() == BackupManifestName || !strings.HasSuffix(path, ".sst") {
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
	close(blockGC)
}

func TestBackupRestoreUserDefinedTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// TODO (rohany): Add tests for backup/restore with revision history and
	//  incremental backups once types can change.

	// Test full cluster backup/restore.
	t.Run("full-cluster", func(t *testing.T) {
		_, _, sqlDB, dataDir, cleanupFn := BackupRestoreTestSetup(t, singleNode, 0, InitNone)
		defer cleanupFn()
		// Create some types, databases, and tables that use them.
		sqlDB.Exec(t, `
SET experimental_enable_enums = true;

CREATE DATABASE d;
CREATE TYPE d.greeting AS ENUM ('hello', 'howdy', 'hi');
CREATE TABLE d.t1 (x d.greeting);
INSERT INTO d.t1 VALUES ('hello'), ('howdy');
CREATE TABLE d.t2 (x d.greeting[]);
INSERT INTO d.t2 VALUES (ARRAY['howdy']), (ARRAY['hi']);

CREATE DATABASE d2;
CREATE TYPE d2.farewell AS ENUM ('bye', 'cya');
CREATE TABLE d2.t1 (x d2.farewell);
INSERT INTO d2.t1 VALUES ('bye'), ('cya');
CREATE TABLE d2.t2 (x d2.farewell[]);
INSERT INTO d2.t2 VALUES (ARRAY['bye']), (ARRAY['cya']);
`)
		// Backup the cluster.
		sqlDB.Exec(t, `BACKUP TO 'nodelocal://0/test/'`)
		// Start a new server that shares the data directory.
		_, _, sqlDBRestore, cleanupRestore := backupRestoreTestSetupEmpty(t, singleNode, dataDir, InitNone)
		defer cleanupRestore()

		// Restore into the new cluster.
		sqlDBRestore.Exec(t, `RESTORE FROM 'nodelocal://0/test/'`)

		// Check all of the tables have the right data.
		sqlDBRestore.CheckQueryResults(t, `SELECT * FROM d.t1 ORDER BY x`, [][]string{{"hello"}, {"howdy"}})
		sqlDBRestore.CheckQueryResults(t, `SELECT * FROM d.t2 ORDER BY x`, [][]string{{"{howdy}"}, {"{hi}"}})
		sqlDBRestore.CheckQueryResults(t, `SELECT * FROM d2.t1 ORDER BY x`, [][]string{{"bye"}, {"cya"}})
		sqlDBRestore.CheckQueryResults(t, `SELECT * FROM d2.t2 ORDER BY x`, [][]string{{"{bye}"}, {"{cya}"}})

		// We should be able to resolve each restored type. Test this by inserting
		// into each of the restored tables.
		sqlDBRestore.Exec(t, `INSERT INTO d.t1 VALUES ('hi')`)
		sqlDBRestore.Exec(t, `INSERT INTO d.t2 VALUES (ARRAY['hello'])`)
		sqlDBRestore.Exec(t, `INSERT INTO d2.t1 VALUES ('cya')`)
		sqlDBRestore.Exec(t, `INSERT INTO d2.t2 VALUES (ARRAY['cya'])`)

		// Each of the restored types should have namespace entries. Test this by
		// trying to create types that would cause namespace conflicts.
		sqlDBRestore.Exec(t, `SET experimental_enable_enums = true`)
		sqlDBRestore.ExpectErr(t, `pq: type "d.public.greeting" already exists`, `CREATE TYPE d.greeting AS ENUM ('hello', 'hiya')`)
		sqlDBRestore.ExpectErr(t, `pq: type "d.public._greeting" already exists`, `CREATE TYPE d._greeting AS ENUM ('hello', 'hiya')`)
		sqlDBRestore.ExpectErr(t, `pq: type "d2.public.farewell" already exists`, `CREATE TYPE d2.farewell AS ENUM ('go', 'away')`)
		sqlDBRestore.ExpectErr(t, `pq: type "d2.public._farewell" already exists`, `CREATE TYPE d2._farewell AS ENUM ('go', 'away')`)
	})

	// Test backup/restore of a database.
	t.Run("database", func(t *testing.T) {
		_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, 0, InitNone)
		defer cleanupFn()
		// Create a database with some types and tables. We include a table with
		// different expressions within to test that the types within get remapped.
		sqlDB.Exec(t, `
SET experimental_enable_enums = true;
CREATE DATABASE d;
CREATE TYPE d.greeting AS ENUM ('hello', 'howdy', 'hi');
CREATE TYPE d.farewell AS ENUM ('bye', 'cya');
CREATE TABLE d.t1 (x d.greeting);
INSERT INTO d.t1 VALUES ('hello'), ('howdy');
CREATE TABLE d.t2 (x d.greeting[]);
INSERT INTO d.t2 VALUES (ARRAY['howdy']), (ARRAY['hi']);
CREATE TABLE d.expr (
	x d.greeting,
  y d.greeting DEFAULT 'hello',
	z bool AS (y = 'howdy') STORED,
  CHECK (x < 'hi'),
	CHECK (x = ANY enum_range(y, 'hi'))
);
`)
		// Now backup the database.
		sqlDB.Exec(t, `BACKUP DATABASE d TO 'nodelocal://0/test/'`)
		// Drop the database and restore into it.
		sqlDB.Exec(t, `DROP DATABASE d`)
		sqlDB.Exec(t, `RESTORE DATABASE d FROM 'nodelocal://0/test/'`)

		// All of the tables should have the values we expect.
		sqlDB.CheckQueryResults(t, `SELECT * FROM d.t1 ORDER BY x`, [][]string{{"hello"}, {"howdy"}})
		sqlDB.CheckQueryResults(t, `SELECT * FROM d.t2 ORDER BY x`, [][]string{{"{howdy}"}, {"{hi}"}})

		// Insert a row into the expr table so that all of the expressions are
		// evaluated and checked.
		sqlDB.Exec(t, `INSERT INTO d.expr VALUES ('howdy')`)
		sqlDB.CheckQueryResults(t, `SELECT * FROM d.expr`, [][]string{{"howdy", "hello", "false"}})
		sqlDB.ExpectErr(t, `pq: failed to satisfy CHECK constraint`, `INSERT INTO d.expr VALUES ('hi')`)

		// We should be able to use the restored types to create new tables.
		sqlDB.Exec(t, `CREATE TABLE d.t3 (x d.greeting, y d.farewell)`)
		// We should detect name conflicts trying to overwrite existing type names.
		sqlDB.ExpectErr(t, `pq: type "d.public.greeting" already exists`, `CREATE TYPE d.greeting AS ENUM ('hello', 'hiya')`)
		sqlDB.ExpectErr(t, `pq: type "d.public._greeting" already exists`, `CREATE TYPE d._greeting AS ENUM ('hello', 'hiya')`)
		sqlDB.ExpectErr(t, `pq: type "d.public.farewell" already exists`, `CREATE TYPE d.farewell AS ENUM ('hello', 'hiya')`)
		sqlDB.ExpectErr(t, `pq: type "d.public._farewell" already exists`, `CREATE TYPE d._farewell AS ENUM ('hello', 'hiya')`)
	})

	// Test backup/restore of a single table.
	t.Run("table", func(t *testing.T) {
		_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, 0, InitNone)
		defer cleanupFn()
		sqlDB.Exec(t, `
SET experimental_enable_enums = true;
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
		// TODO (rohany): Add a test for remapping to enums that are compatible
		//  but not the same once ALTER TYPE is possibe.
		_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, 0, InitNone)
		defer cleanupFn()
		sqlDB.Exec(t, `
SET experimental_enable_enums = true;
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
	})
}

func TestBackupRestoreInterleaved(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

	const numAccounts = 30
	const createStore = "CREATE DATABASE store"
	const createStoreStats = "CREATE DATABASE storestats"

	_, _, origDB, dir, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()
	args := base.TestServerArgs{ExternalIODir: dir}

	// Generate some testdata and back it up.
	{
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

		db.Exec(t, `RESTORE store.orders FROM $1 WITH OPTIONS ('skip_missing_foreign_keys')`, LocalFoo)
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
		db.Exec(t, `RESTORE store.receipts FROM $1 WITH OPTIONS ('skip_missing_foreign_keys')`, LocalFoo)
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
		db.Exec(t, `RESTORE store.receipts, store.customers FROM $1 WITH OPTIONS ('skip_missing_foreign_keys')`, LocalFoo)
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

		db.Exec(t, `RESTORE DATABASE storestats from $1 WITH OPTIONS ('skip_missing_views')`, LocalFoo)
		db.Exec(t, `RESTORE storestats.ordercounts from $1 WITH OPTIONS ('skip_missing_views')`, LocalFoo)
		// Ensure that the views were not restored since they are missing the tables they reference.
		db.CheckQueryResults(t, `USE storestats; SHOW TABLES;`, [][]string{})

		db.Exec(t, `RESTORE store.early_customers, store.referencing_early_customers from $1 WITH OPTIONS ('skip_missing_views')`, LocalFoo)
		// Ensure that the views were not restored since they are missing the tables they reference.
		db.CheckQueryResults(t, `SHOW TABLES;`, [][]string{})

		// Test that views with valid dependencies are restored

		db.Exec(t, `RESTORE DATABASE store from $1 WITH OPTIONS ('skip_missing_views')`, LocalFoo)
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
		db.Exec(t, `RESTORE storestats.ordercounts, store.customers from $1 WITH OPTIONS ('skip_missing_views', 'into_db'='store2')`, LocalFoo)
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
				restoreQ := fmt.Sprintf(`RESTORE data.%s FROM $1 WITH OPTIONS ('into_db'='%s')`, table, dbName)
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

func TestBackupAsOfSystemTime(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

	const numAccounts = 10
	ctx, tc, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()
	const dir = "nodelocal://0/"
	preGC := tree.TimestampToDecimal(tc.Server(0).Clock().Now()).String()

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

	postGC := tree.TimestampToDecimal(tc.Server(0).Clock().Now()).String()

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

	const numAccounts = 1000
	_, _, sqlDB, dir, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()
	dir = filepath.Join(dir, "foo")

	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, LocalFoo)

	var backupManifest BackupManifest
	{
		backupManifestBytes, err := ioutil.ReadFile(filepath.Join(dir, BackupManifestName))
		if err != nil {
			t.Fatalf("%+v", err)
		}
		fileType := http.DetectContentType(backupManifestBytes)
		if fileType == ZipType {
			backupManifestBytes, err = DecompressData(backupManifestBytes)
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

func TestBackupEncrypted(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, _, sqlDB, rawDir, cleanupFn := BackupRestoreTestSetup(t, MultiNode, 3, InitNone)
	defer cleanupFn()

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

	// Full cluster-backup to capture all possible metadata.
	backupLoc1 := LocalFoo + "/x?COCKROACH_LOCALITY=default"
	backupLoc2 := LocalFoo + "/x2?COCKROACH_LOCALITY=" + url.QueryEscape("dc=dc1")
	backupLoc1inc := LocalFoo + "/inc1/x?COCKROACH_LOCALITY=default"
	backupLoc2inc := LocalFoo + "/inc1/x2?COCKROACH_LOCALITY=" + url.QueryEscape("dc=dc1")

	plainBackupLoc1 := LocalFoo + "/cleartext?COCKROACH_LOCALITY=default"
	plainBackupLoc2 := LocalFoo + "/cleartext?COCKROACH_LOCALITY=" + url.QueryEscape("dc=dc1")

	sqlDB.Exec(t, `BACKUP TO ($1, $2)`, plainBackupLoc1, plainBackupLoc2)

	sqlDB.Exec(t, `BACKUP TO ($1, $2) WITH encryption_passphrase='abcdefg'`, backupLoc1, backupLoc2)
	// Add the actual content with our sentinel too.
	sqlDB.Exec(t, `UPDATE neverappears.neverappears SET other = 'neverappears'`)
	sqlDB.Exec(t, `BACKUP TO ($1, $2) INCREMENTAL FROM $3 WITH encryption_passphrase='abcdefg'`,
		backupLoc1inc, backupLoc2inc, backupLoc1)

	t.Run("check-stats-encrypted", func(t *testing.T) {
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
	})

	before := sqlDB.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE neverappears.neverappears`)

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

	sqlDB.Exec(t, `DROP DATABASE neverappears CASCADE`)

	sqlDB.Exec(t, `SHOW BACKUP $1 WITH encryption_passphrase='abcdefg'`, backupLoc1)
	sqlDB.ExpectErr(t, `cipher: message authentication failed`, `SHOW BACKUP $1 WITH encryption_passphrase='wronngpassword'`, backupLoc1)
	sqlDB.ExpectErr(t, `file appears encrypted -- try specifying "encryption_passphrase"`, `SHOW BACKUP $1`, backupLoc1)
	sqlDB.ExpectErr(t, `could not find or read encryption information`, `SHOW BACKUP $1 WITH encryption_passphrase='wronngpassword'`, plainBackupLoc1)

	sqlDB.Exec(t, `RESTORE DATABASE neverappears FROM ($1, $2), ($3, $4) WITH encryption_passphrase='abcdefg'`,
		backupLoc1, backupLoc2, backupLoc1inc, backupLoc2inc)

	sqlDB.CheckQueryResults(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE neverappears.neverappears`, before)
}

func TestRestoredPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
		if _, err := testuser.Exec(`RESTORE blah FROM 'blah'`); !testutils.IsError(
			err, "only users with the admin role are allowed to RESTORE",
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
			`RESTORE data.bank FROM $1 WITH OPTIONS ('into_db'='system')`, LocalFoo,
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
			if _, err := testuser.Exec(`RESTORE data.bank FROM $1 WITH OPTIONS ('into_db'='data2')`, testLocalFoo); err != nil {
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

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	sqlDB.Exec(t, `DROP DATABASE data`)
	sqlDB.Exec(t, `CREATE DATABASE data`)
	sqlDB.Exec(t, `CREATE TABLE data.bank (i int)`)
	sqlDB.Exec(t, `INSERT INTO data.bank VALUES (1)`)

	sqlDB.Exec(t, "BACKUP DATABASE data TO $1", LocalFoo)
	sqlDB.Exec(t, "CREATE DATABASE data2")
	sqlDB.Exec(t, "RESTORE data.* FROM $1 WITH OPTIONS ('into_db'='data2')", LocalFoo)

	expected := sqlDB.QueryStr(t, `SELECT * FROM data.bank`)
	sqlDB.CheckQueryResults(t, `SELECT * FROM data2.bank`, expected)
}

func TestBackupRestoreDropTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
	sqlDB.Exec(t, "RESTORE data.* FROM $1 WITH OPTIONS ('into_db'='data2')", LocalFoo)

	expected := sqlDB.QueryStr(t, `SELECT * FROM data.bank`)
	sqlDB.CheckQueryResults(t, `SELECT * FROM data2.bank`, expected)
}

func TestBackupRestoreIncrementalAddTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

func TestBackupRestoreNotInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	db := sqlDB.DB.(*gosql.DB)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`BACKUP DATABASE data TO 'blah'`); !testutils.IsError(err, "cannot be used inside a transaction") {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	sqlDB.Exec(t, `BACKUP DATABASE data TO $1`, LocalFoo)
	sqlDB.Exec(t, `DROP DATABASE data`)
	sqlDB.Exec(t, `RESTORE DATABASE data FROM $1`, LocalFoo)

	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`RESTORE DATABASE data FROM 'blah'`); !testutils.IsError(err, "cannot be used inside a transaction") {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// TODO(dt): move to importccl.
	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`IMPORT TABLE t (id INT PRIMARY KEY) CSV DATA ('blah')`); !testutils.IsError(err, "cannot be used inside a transaction") {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestBackupRestoreSequence(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

		newDB.Exec(t, `RESTORE TABLE t FROM $1 WITH OPTIONS ('skip_missing_sequences')`, LocalFoo)

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

func TestBackupRestoreShowJob(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
			{"RESTORE TABLE data.bank FROM 'nodelocal://0/foo' WITH into_db = 'data 2', skip_missing_foreign_keys"},
		},
	)
}

func TestBackupCreatedStats(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	sqlDB.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled=false`)

	sqlDB.Exec(t, `CREATE TABLE data.foo (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `CREATE STATISTICS foo_stats FROM data.foo`)
	sqlDB.Exec(t, `CREATE STATISTICS bank_stats FROM data.bank`)
	sqlDB.Exec(t, `BACKUP data.bank, data.foo TO $1 WITH revision_history`, LocalFoo)
	sqlDB.Exec(t, `CREATE DATABASE "data 2"`)
	sqlDB.Exec(t, `RESTORE data.bank, data.foo FROM $1 WITH skip_missing_foreign_keys, into_db = $2`,
		LocalFoo, "data 2")

	sqlDB.CheckQueryResults(t,
		`SELECT statistics_name, column_names, row_count, distinct_count, null_count
	FROM [SHOW STATISTICS FOR TABLE "data 2".bank] WHERE statistics_name='bank_stats'`,
		[][]string{
			{"bank_stats", "{id}", "1", "1", "0"},
			{"bank_stats", "{balance}", "1", "1", "0"},
			{"bank_stats", "{payload}", "1", "1", "0"},
		})
	sqlDB.CheckQueryResults(t,
		`SELECT statistics_name, column_names, row_count, distinct_count, null_count
	FROM [SHOW STATISTICS FOR TABLE "data 2".foo] WHERE statistics_name='foo_stats'`,
		[][]string{
			{"foo_stats", "{a}", "0", "0", "0"},
		})
}

// Ensure that backing up and restoring an empty database succeeds.
func TestBackupRestoreEmptyDB(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	sqlDB.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled=false`)

	sqlDB.Exec(t, `CREATE TABLE data.foo (a INT)`)
	sqlDB.Exec(t, `CREATE STATISTICS foo_stats FROM data.foo`)
	sqlDB.Exec(t, `CREATE STATISTICS bank_stats FROM data.bank`)

	sqlDB.Exec(t, `BACKUP data.bank, data.foo TO $1 WITH revision_history`, LocalFoo)
	sqlDB.Exec(t, `DELETE FROM system.table_statistics WHERE name = 'foo_stats' OR name = 'bank_stats'`)
	sqlDB.Exec(t, `CREATE DATABASE "data 2"`)
	sqlDB.Exec(t, `RESTORE data.bank FROM $1 WITH skip_missing_foreign_keys, into_db = $2`,
		LocalFoo, "data 2")

	// Ensure that the bank_stats have been restored, but foo_stats have not.
	sqlDB.CheckQueryResults(t,
		`SELECT name, "columnIDs", "rowCount", "distinctCount", "nullCount" FROM system.table_statistics`,
		[][]string{
			{"bank_stats", "{1}", "1", "1", "0"}, // id column
			{"bank_stats", "{2}", "1", "1", "0"}, // balance column
			{"bank_stats", "{3}", "1", "1", "0"}, // payload column
		})
}

// Ensure that statistics are restored from correct backup.
func TestBackupCreatedStatsFromIncrementalBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const incremental1Foo = "nodelocal://0/incremental1foo"
	const incremental2Foo = "nodelocal://0/incremental2foo"
	const numAccounts = 1
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()
	var beforeTs string

	sqlDB.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled=false`)

	// Create the 1st backup, where data.bank has 1 account.
	sqlDB.Exec(t, `CREATE STATISTICS bank_stats FROM data.bank`)
	sqlDB.Exec(t, `BACKUP data.bank TO $1 WITH revision_history`, LocalFoo)

	// Create the 2nd backup, where data.bank has 3 accounts.
	sqlDB.Exec(t, `INSERT INTO data.bank VALUES (2, 2), (4, 4)`)
	sqlDB.Exec(t, `CREATE STATISTICS bank_stats FROM data.bank`)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&beforeTs) // Save time to restore to this point.
	sqlDB.Exec(t, `BACKUP data.bank TO $1 INCREMENTAL FROM $2 WITH revision_history`, incremental1Foo, LocalFoo)

	// Create the 3rd backup, where data.bank has 5 accounts.
	sqlDB.Exec(t, `INSERT INTO data.bank VALUES (3, 3), (5, 2)`)
	sqlDB.Exec(t, `CREATE STATISTICS bank_stats FROM data.bank`)
	sqlDB.Exec(t, `BACKUP data.bank TO $1 INCREMENTAL FROM $2, $3 WITH revision_history`, incremental2Foo, LocalFoo, incremental1Foo)

	// Restore the 2nd backup.
	sqlDB.Exec(t, `CREATE DATABASE "data 2"`)
	sqlDB.Exec(t, fmt.Sprintf(`RESTORE data.bank FROM "%s", "%s", "%s" AS OF SYSTEM TIME %s WITH skip_missing_foreign_keys, into_db = "%s"`,
		LocalFoo, incremental1Foo, incremental2Foo, beforeTs, "data 2"))

	// Expect the values in row_count and distinct_count to be 3. The values
	// would be 1 if the stats from the full backup were restored and 5 if
	// the stats from the latest incremental backup were restored.
	sqlDB.CheckQueryResults(t,
		`SELECT statistics_name, column_names, row_count, distinct_count, null_count
	FROM [SHOW STATISTICS FOR TABLE "data 2".bank] WHERE statistics_name='bank_stats'`,
		[][]string{
			{"bank_stats", "{id}", "3", "3", "0"},
			{"bank_stats", "{balance}", "3", "3", "0"},
			{"bank_stats", "{payload}", "3", "2", "2"},
		})
}

// TestProtectedTimestampsDuringBackup ensures that the timestamp at which a
// table is taken offline is protected during a BACKUP job to ensure that if
// data can be read for a period longer than the default GC interval.
func TestProtectedTimestampsDuringBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// A sketch of the test is as follows:
	//
	//  * Create a table foo to backup.
	//  * Set a 1 second gcttl for foo.
	//  * Start a BACKUP which blocks after setup (after time of backup is
	//    decided), until it is signaled.
	//  * Manually enqueue the ranges for GC and ensure that at least one
	//    range ran the GC.
	//  * Unblock the backup.
	//  * Ensure the backup has succeeded.

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	allowResponse := make(chan struct{})
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	params := base.TestClusterArgs{}
	params.ServerArgs.ExternalIODir = dir
	params.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingResponseFilter: func(
			ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse,
		) *roachpb.Error {
			for _, ru := range br.Responses {
				switch ru.GetInner().(type) {
				case *roachpb.ExportResponse, *roachpb.ImportResponse:
					<-allowResponse
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

	go func() {
		// N.B. We use the conn rather than the runner here since the test may
		// finish before the job finishes. The test will finish as soon as the
		// timestamp is no longer protected. If the test starts tearing down the
		// cluster before the backup job is done, the test may still fail when the
		// backup fails. This test does not particularly care if the BACKUP
		// completes with a success or failure, as long as the timestamp is released
		// shortly after the BACKUP is unblocked.
		_, _ = conn.Exec(`BACKUP TABLE FOO TO 'nodelocal://1/foo'`) // ignore error.
	}()

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
	close(allowResponse)

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
