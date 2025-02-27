// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/backup/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestBackupCompaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tempDir, tempDirCleanup := testutils.TempDir(t)
	defer tempDirCleanup()
	st := cluster.MakeTestingClusterSettings()
	backupinfo.WriteMetadataWithExternalSSTsEnabled.Override(ctx, &st.SV, true)
	tc, db, cleanupDB := backupRestoreTestSetupEmpty(
		t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Settings: st,
			},
		},
	)
	defer cleanupDB()

	startAndAwaitCompaction := func(start, end string) {
		compactionBuiltin := "SELECT crdb_internal.backup_compaction(ARRAY['nodelocal://1/backup'], 'LATEST', ''::BYTES, '%s'::DECIMAL, '%s'::DECIMAL)"
		row := db.QueryRow(t, fmt.Sprintf(compactionBuiltin, start, end))
		var jobID jobspb.JobID
		row.Scan(&jobID)
		waitForSuccessfulJob(t, tc, jobID)
	}
	fullBackupAostCmd := "BACKUP INTO 'nodelocal://1/backup' AS OF SYSTEM TIME '%s'"
	incBackupCmd := "BACKUP INTO LATEST IN 'nodelocal://1/backup'"
	incBackupAostCmd := "BACKUP INTO LATEST IN 'nodelocal://1/backup' AS OF SYSTEM TIME '%s'"
	t.Run("basic operations insert, update, and delete", func(t *testing.T) {
		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
		}()
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		start := getTime()
		db.Exec(t, fmt.Sprintf(fullBackupAostCmd, start))
		var backupPath string
		db.QueryRow(t, "SHOW BACKUPS IN 'nodelocal://1/backup'").Scan(&backupPath)

		// Run twice to test compaction on top of compaction.
		for i := 0; i < 2; i++ {
			db.Exec(t, "INSERT INTO foo VALUES (2, 2), (3, 3)")
			db.Exec(t, incBackupCmd)
			db.Exec(t, "UPDATE foo SET b = b + 1 WHERE a = 2")
			db.Exec(t, incBackupCmd)
			db.Exec(t, "DELETE FROM foo WHERE a = 3")
			end := getTime()
			db.Exec(
				t,
				fmt.Sprintf(incBackupAostCmd, end),
			)
			startAndAwaitCompaction(start, end)
			validateCompactedBackupForTables(t, db, []string{"foo"}, "'nodelocal://1/backup'")
			start = end
		}

		// Ensure that additional backups were created.
		var numBackups int
		db.QueryRow(
			t,
			"SELECT count(DISTINCT (start_time, end_time)) FROM "+
				"[SHOW BACKUP FROM $1 IN 'nodelocal://1/backup']",
			backupPath,
		).Scan(&numBackups)
		require.Equal(t, 9, numBackups)
	})

	t.Run("create and drop tables", func(t *testing.T) {
		defer func() {
			db.Exec(t, "DROP TABLE IF EXISTS foo, bar, baz")
		}()
		db.Exec(t, "CREATE TABLE foo (a INT PRIMARY KEY, b INT)")
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		start := getTime()
		db.Exec(t, fmt.Sprintf(fullBackupAostCmd, start))

		db.Exec(t, "CREATE TABLE bar (a INT, b INT)")
		db.Exec(t, "INSERT INTO bar VALUES (1, 1)")
		db.Exec(t, incBackupCmd)

		db.Exec(t, "INSERT INTO bar VALUES (2, 2)")
		db.Exec(t, incBackupCmd)

		db.Exec(t, "CREATE TABLE baz (a INT, b INT)")
		db.Exec(t, "INSERT INTO baz VALUES (3, 3)")
		end := getTime()
		db.Exec(
			t,
			fmt.Sprintf(incBackupAostCmd, end),
		)
		startAndAwaitCompaction(start, end)
		validateCompactedBackupForTables(
			t, db,
			[]string{"foo", "bar", "baz"},
			"'nodelocal://1/backup'",
		)

		db.Exec(t, "DROP TABLE bar")
		end = getTime()
		db.Exec(
			t,
			fmt.Sprintf(incBackupAostCmd, end),
		)
		startAndAwaitCompaction(start, end)

		db.Exec(t, "DROP TABLE foo, baz")
		db.Exec(t, "RESTORE FROM LATEST IN 'nodelocal://1/backup'")
		rows := db.QueryStr(t, "SELECT * FROM [SHOW TABLES] WHERE table_name = 'bar'")
		require.Empty(t, rows)
	})

	t.Run("create indexes", func(t *testing.T) {
		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
		}()
		db.Exec(t, "INSERT INTO foo VALUES (1, 1), (2, 2), (3, 3)")
		start := getTime()
		db.Exec(t, fmt.Sprintf(fullBackupAostCmd, start))

		db.Exec(t, "CREATE INDEX bar ON foo (a)")
		db.Exec(t, "CREATE INDEX baz ON foo (a)")
		db.Exec(t, incBackupCmd)

		db.Exec(t, "CREATE INDEX qux ON foo (b)")
		db.Exec(t, "DROP INDEX foo@bar")
		end := getTime()
		db.Exec(
			t,
			fmt.Sprintf(incBackupAostCmd, end),
		)
		startAndAwaitCompaction(start, end)

		var numIndexes, restoredNumIndexes int
		db.QueryRow(t, "SELECT count(*) FROM [SHOW INDEXES FROM foo]").Scan(&numIndexes)
		db.Exec(t, "DROP TABLE foo")
		db.Exec(t, "RESTORE TABLE foo FROM LATEST IN 'nodelocal://1/backup'")
		db.QueryRow(t, "SELECT count(*) FROM [SHOW INDEXES FROM foo]").Scan(&restoredNumIndexes)
		require.Equal(t, numIndexes, restoredNumIndexes)
	})

	t.Run("compact middle of backup chain", func(t *testing.T) {
		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
		}()
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		db.Exec(t, "BACKUP INTO 'nodelocal://1/backup'")

		db.Exec(t, "INSERT INTO foo VALUES (2, 2)")
		start := getTime()
		db.Exec(
			t,
			fmt.Sprintf(incBackupAostCmd, start),
		)

		db.Exec(t, "INSERT INTO foo VALUES (3, 3)")
		db.Exec(t, incBackupCmd)

		db.Exec(t, "INSERT INTO foo VALUES (4, 4)")
		db.Exec(t, incBackupCmd)

		db.Exec(t, "INSERT INTO foo VALUES (5, 5)")
		end := getTime()
		db.Exec(
			t,
			fmt.Sprintf(incBackupAostCmd, end),
		)

		db.Exec(t, "INSERT INTO foo VALUES (6, 6)")
		db.Exec(t, incBackupCmd)

		startAndAwaitCompaction(start, end)
		validateCompactedBackupForTables(t, db, []string{"foo"}, "'nodelocal://1/backup'")
	})

	t.Run("table-level backups", func(t *testing.T) {
		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
		}()
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		start := getTime()
		db.Exec(t, fmt.Sprintf(
			fullBackupAostCmd, start,
		))

		db.Exec(t, "INSERT INTO foo VALUES (2, 2)")
		db.Exec(t, incBackupCmd)
		db.Exec(t, "UPDATE foo SET b = b + 1 WHERE a = 2")
		db.Exec(t, "DELETE FROM foo WHERE a = 1")
		end := getTime()
		db.Exec(
			t,
			fmt.Sprintf(
				incBackupAostCmd,
				end,
			),
		)

		startAndAwaitCompaction(start, end)
		validateCompactedBackupForTables(t, db, []string{"foo"}, "'nodelocal://1/backup'")
	})

	t.Run("encrypted backups", func(t *testing.T) {
		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
		}()
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		opts := "encryption_passphrase = 'correct-horse-battery-staple'"
		start := getTime()
		db.Exec(t, fmt.Sprintf(
			fullBackupAostCmd+" WITH %s", start, opts,
		))
		db.Exec(t, "INSERT INTO foo VALUES (2, 2)")
		db.Exec(
			t,
			fmt.Sprintf(incBackupCmd+" WITH %s", opts),
		)
		db.Exec(t, "UPDATE foo SET b = b + 1 WHERE a = 2")
		end := getTime()
		db.Exec(
			t,
			fmt.Sprintf(incBackupAostCmd+" WITH %s", end, opts),
		)
		db.Exec(
			t,
			fmt.Sprintf(
				`SELECT crdb_internal.backup_compaction(
ARRAY['nodelocal://1/backup'], 
'LATEST',
crdb_internal.json_to_pb(
'cockroach.sql.jobs.jobspb.BackupEncryptionOptions',
'{"mode": 0, "raw_passphrase": "correct-horse-battery-staple"}'
), '%s', '%s')`,
				start, end,
			),
		)
		validateCompactedBackupForTablesWithOpts(
			t, db, []string{"foo"}, "'nodelocal://1/backup'", opts,
		)
	})
	// TODO (kev-cao): Once range keys are supported by the compaction
	// iterator, add tests for dropped tables/indexes.
}

func TestBackupCompactionLocalityAware(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderDuress(t, "node startup is slow")

	tempDir, tempDirCleanup := testutils.TempDir(t)
	defer tempDirCleanup()
	tc, db, cleanupDB := backupRestoreTestSetupEmpty(
		t, multiNode, tempDir, InitManualReplication,
		base.TestClusterArgs{
			ServerArgsPerNode: map[int]base.TestServerArgs{
				0: {
					Locality: roachpb.Locality{Tiers: []roachpb.Tier{
						{Key: "region", Value: "west"},
						{Key: "az", Value: "az1"},
						{Key: "dc", Value: "dc1"},
					}},
				},
				1: {
					Locality: roachpb.Locality{Tiers: []roachpb.Tier{
						{Key: "region", Value: "east"},
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
		},
	)
	defer cleanupDB()
	collectionURIs := strings.Join([]string{
		fmt.Sprintf(
			"'nodelocal://1/backup?COCKROACH_LOCALITY=%s'",
			url.QueryEscape("default"),
		),
		fmt.Sprintf(
			"'nodelocal://2/backup?COCKROACH_LOCALITY=%s'",
			url.QueryEscape("dc=dc2"),
		),
		fmt.Sprintf(
			"'nodelocal://3/backup?COCKROACH_LOCALITY=%s'",
			url.QueryEscape("region=west"),
		),
	}, ", ")
	db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
	db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
	start := getTime()
	db.Exec(
		t,
		fmt.Sprintf("BACKUP INTO (%s) AS OF SYSTEM TIME '%s'", collectionURIs, start),
	)

	db.Exec(t, "INSERT INTO foo VALUES (2, 2)")
	db.Exec(
		t,
		fmt.Sprintf("BACKUP INTO LATEST IN (%s)", collectionURIs),
	)

	db.Exec(t, "INSERT INTO foo VALUES (3, 3)")
	end := getTime()
	db.Exec(
		t,
		fmt.Sprintf("BACKUP INTO LATEST IN (%s) AS OF SYSTEM TIME '%s'", collectionURIs, end),
	)
	compactionBuiltin := "SELECT crdb_internal.backup_compaction(ARRAY[%s], 'LATEST', '', '%s', '%s')"
	row := db.QueryRow(t, fmt.Sprintf(compactionBuiltin, collectionURIs, start, end))
	var jobID jobspb.JobID
	row.Scan(&jobID)
	waitForSuccessfulJob(t, tc, jobID)
	validateCompactedBackupForTables(t, db, []string{"foo"}, collectionURIs)
}

func validateCompactedBackupForTables(
	t *testing.T, db *sqlutils.SQLRunner, tables []string, collectionURIs string,
) {
	t.Helper()
	validateCompactedBackupForTablesWithOpts(t, db, tables, collectionURIs, "")
}

func validateCompactedBackupForTablesWithOpts(
	t *testing.T, db *sqlutils.SQLRunner, tables []string, collectionURIs string, opts string,
) {
	t.Helper()
	rows := make(map[string][][]string)
	for _, table := range tables {
		rows[table] = db.QueryStr(t, "SELECT * FROM "+table)
	}
	tablesList := strings.Join(tables, ", ")
	db.Exec(t, "DROP TABLE "+tablesList)
	restoreQuery := fmt.Sprintf("RESTORE TABLE %s FROM LATEST IN (%s)", tablesList, collectionURIs)
	if opts != "" {
		restoreQuery += " WITH " + opts
	}
	db.Exec(t, restoreQuery)
	for table, originalRows := range rows {
		restoredRows := db.QueryStr(t, "SELECT * FROM "+table)
		require.Equal(t, originalRows, restoredRows, "table %s", table)
	}
}

func getTime() string {
	return hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}.AsOfSystemTime()
}
