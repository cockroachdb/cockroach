// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestBackupCompaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tempDir, tempDirCleanup := testutils.TempDir(t)
	defer tempDirCleanup()
	_, db, cleanupDB := backupRestoreTestSetupEmpty(
		t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{},
	)
	defer cleanupDB()
	fullBackupStmt := "BACKUP INTO 'nodelocal://1/backup'"

	t.Run("compaction creates a new backup", func(t *testing.T) {
		numExplicitBackups := 3
		db.Exec(t, fullBackupStmt)
		var backupPath string
		db.QueryRow(t, "SHOW BACKUPS IN 'nodelocal://1/backup'").Scan(&backupPath)
		for i := 1; i < numExplicitBackups; i++ {
			doIncrementalBackup(t, db, "'nodelocal://1/backup'", i == numExplicitBackups-1)
		}
		var numBackups int
		db.QueryRow(
			t,
			"SELECT count(DISTINCT (start_time, end_time)) FROM "+
				"[SHOW BACKUP FROM $1 IN 'nodelocal://1/backup']",
			backupPath,
		).Scan(&numBackups)
		require.Equal(t, numExplicitBackups+1, numBackups)
	})

	t.Run("basic operations (insert/update/delete)", func(t *testing.T) {
		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
		}()
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		db.Exec(t, fullBackupStmt)
		// Run twice to test compaction on top of compaction.
		for i := 0; i < 2; i++ {
			db.Exec(t, "INSERT INTO foo VALUES (2, 2), (3, 3)")
			doIncrementalBackup(t, db, "'nodelocal://1/backup'", false)
			db.Exec(t, "UPDATE foo SET b = b + 1 WHERE a = 2")
			doIncrementalBackup(t, db, "'nodelocal://1/backup'", false)
			db.Exec(t, "DELETE FROM foo WHERE a = 3")
			doIncrementalBackup(t, db, "'nodelocal://1/backup'", true)
			validateCompactedBackupForTables(t, db, []string{"foo"}, "'nodelocal://1/backup'")
		}
	})

	t.Run("create and drop tables", func(t *testing.T) {
		defer func() {
			db.Exec(t, "DROP TABLE IF EXISTS foo, bar, baz")
		}()
		db.Exec(t, "CREATE TABLE foo (a INT PRIMARY KEY, b INT)")
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		db.Exec(t, fullBackupStmt)
		db.Exec(t, "CREATE TABLE bar (a INT, b INT)")
		db.Exec(t, "INSERT INTO bar VALUES (1, 1)")
		doIncrementalBackup(t, db, "'nodelocal://1/backup'", false)
		db.Exec(t, "INSERT INTO bar VALUES (2, 2)")
		doIncrementalBackup(t, db, "'nodelocal://1/backup'", true)
		db.Exec(t, "CREATE TABLE baz (a INT, b INT)")
		db.Exec(t, "INSERT INTO baz VALUES (3, 3)")
		doIncrementalBackup(t, db, "'nodelocal://1/backup'", true)
		validateCompactedBackupForTables(
			t, db,
			[]string{"foo", "bar", "baz"},
			"'nodelocal://1/backup'",
		)

		db.Exec(t, "DROP TABLE bar")
		doIncrementalBackup(t, db, "'nodelocal://1/backup'", true)
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
		db.Exec(t, fullBackupStmt)
		db.Exec(t, "CREATE INDEX ON foo (a)")
		doIncrementalBackup(t, db, "'nodelocal://1/backup'", false)
		db.Exec(t, "CREATE INDEX ON foo (b)")
		doIncrementalBackup(t, db, "'nodelocal://1/backup'", true)

		var numIndexes, restoredNumIndexes int
		db.QueryRow(t, "SELECT count(*) FROM [SHOW INDEXES FROM foo]").Scan(&numIndexes)
		db.Exec(t, "DROP TABLE foo")
		db.Exec(t, "RESTORE TABLE foo FROM LATEST IN 'nodelocal://1/backup'")
		db.QueryRow(t, "SELECT count(*) FROM [SHOW INDEXES FROM foo]").Scan(&restoredNumIndexes)
		require.Equal(t, numIndexes, restoredNumIndexes)
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
	_, db, cleanupDB := backupRestoreTestSetupEmpty(
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
	db.Exec(t, fmt.Sprintf("BACKUP INTO (%s)", collectionURIs))
	db.Exec(t, "INSERT INTO foo VALUES (2, 2)")
	doIncrementalBackup(t, db, collectionURIs, false)
	db.Exec(t, "INSERT INTO foo VALUES (3, 3)")
	doIncrementalBackup(t, db, collectionURIs, true)
	validateCompactedBackupForTables(t, db, []string{"foo"}, collectionURIs)
}

// doIncrementalBackup performs an incremental backup, and if compact is set to true, performs a
// compaction afterward.
// TODO (kev-cao): Remove once doCompaction bool is removed and builtin for compaction is made.
func doIncrementalBackup(
	t *testing.T, db *sqlutils.SQLRunner, collectionURIs string, compact bool,
) {
	t.Helper()
	if compact {
		defer testutils.HookGlobal(&doCompaction, true)()
	}
	db.Exec(t, fmt.Sprintf("BACKUP INTO LATEST IN (%s)", collectionURIs))
}

func validateCompactedBackupForTables(
	t *testing.T, db *sqlutils.SQLRunner, tables []string, collectionURIs string,
) {
	t.Helper()
	rows := make(map[string][][]string)
	for _, table := range tables {
		rows[table] = db.QueryStr(t, "SELECT * FROM "+table)
	}
	tablesList := strings.Join(tables, ", ")
	db.Exec(t, "DROP TABLE "+tablesList)
	db.Exec(
		t, fmt.Sprintf("RESTORE TABLE %s FROM LATEST IN (%s)", tablesList, collectionURIs),
	)
	for table, originalRows := range rows {
		restoredRows := db.QueryStr(t, "SELECT * FROM "+table)
		require.Equal(t, originalRows, restoredRows, "table %s", table)
	}
}
