// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"fmt"
	"math/rand/v2"
	"net/url"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
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
	insertRows := func(nRows int, table string) []string {
		queries := make([]string, 0, nRows)
		for i := 0; i < nRows; i++ {
			a, b := rand.IntN(1000), rand.IntN(100)
			queries = append(
				queries,
				fmt.Sprintf("INSERT INTO %s VALUES (%d, %d)", table, a, b),
			)
		}
		return queries
	}
	updateRows := func(nRows int, table string) []string {
		return []string{
			fmt.Sprintf("UPDATE %s SET b = b + 1 ORDER BY random() LIMIT %d", table, nRows),
		}
	}
	deleteRows := func(nRows int, table string) []string {
		return []string{
			fmt.Sprintf("DELETE FROM %s ORDER BY random() LIMIT %d", table, nRows),
		}
	}
	fullBackupStmt := "BACKUP INTO 'nodelocal://1/backup'"

	t.Run("compaction creates a new backup", func(t *testing.T) {
		numExplicitBackups := 5
		db.Exec(t, fullBackupStmt)
		var backupPath string
		db.QueryRow(t, "SHOW BACKUPS IN 'nodelocal://1/backup'").Scan(&backupPath)
		countBackups := func() (count int) {
			db.QueryRow(
				t,
				"SELECT count(DISTINCT (start_time, end_time)) FROM "+
					"[SHOW BACKUP FROM $1 IN 'nodelocal://1/backup']",
				backupPath,
			).Scan(&count)
			return
		}
		for i := 1; i < numExplicitBackups; i++ {
			runQueriesAndBackup(
				t, db, "'nodelocal://1/backup'",
				[]string{}, []string{}, i == numExplicitBackups-1,
			)
		}
		require.Equal(t, numExplicitBackups+1, countBackups())
	})

	t.Run("basic operations (insert/update/delete)", func(t *testing.T) {
		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
		}()
		for _, stmt := range insertRows(10, "foo") {
			db.Exec(t, stmt)
		}
		db.Exec(t, fullBackupStmt)
		// Run twice to test compaction on top of compaction.
		for i := 0; i < 2; i++ {
			runQueriesAndBackup(
				t, db, "'nodelocal://1/backup'",
				insertRows(20, "foo"), []string{}, false,
			)
			runQueriesAndBackup(
				t, db, "'nodelocal://1/backup'",
				updateRows(10, "foo"), []string{}, false,
			)
			runQueriesAndBackup(
				t, db, "'nodelocal://1/backup'",
				deleteRows(5, "foo"), []string{}, true,
			)
		}
	})

	t.Run("create tables", func(t *testing.T) {
		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo, bar, baz")
		}()
		for _, stmt := range insertRows(10, "foo") {
			db.Exec(t, stmt)
		}
		db.Exec(t, fullBackupStmt)
		runQueriesAndBackup(
			t, db, "'nodelocal://1/backup'",
			insertRows(20, "foo"), []string{}, false,
		)
		runQueriesAndBackup(
			t, db, "'nodelocal://1/backup'",
			insertRows(20, "foo"), []string{}, false,
		)
		runQueriesAndBackup(
			t, db, "'nodelocal://1/backup'",
			append(
				[]string{"CREATE TABLE bar (a INT, b INT)"},
				insertRows(10, "bar")...,
			), []string{"foo", "bar"}, true,
		)
		// Create another table to test compaction on compaction.
		runQueriesAndBackup(
			t, db, "'nodelocal://1/backup'",
			insertRows(10, "bar"), []string{}, false,
		)
		runQueriesAndBackup(
			t, db, "'nodelocal://1/backup'",
			insertRows(10, "bar"), []string{}, false,
		)
		runQueriesAndBackup(
			t, db, "'nodelocal://1/backup'",
			append(
				[]string{"CREATE TABLE baz (a INT, b INT)"},
				insertRows(10, "baz")...,
			), []string{"foo", "bar", "baz"}, true,
		)
	})

	t.Run("create indexes", func(t *testing.T) {
		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
		}()
		for _, stmt := range insertRows(10, "foo") {
			db.Exec(t, stmt)
		}
		db.Exec(t, fullBackupStmt)
		runQueriesAndBackup(
			t, db, "'nodelocal://1/backup'",
			insertRows(20, "foo"), []string{}, false,
		)
		runQueriesAndBackup(
			t, db, "'nodelocal://1/backup'",
			insertRows(10, "foo"), []string{}, false,
		)
		runQueriesAndBackup(
			t, db, "'nodelocal://1/backup'",
			[]string{"CREATE INDEX ON foo (a)"}, []string{}, true,
		)
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

func TestBackupCompactionMultiRegion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
	db.Exec(t, fmt.Sprintf("BACKUP INTO (%s)", collectionURIs))
	createInsertQueries := func(count int) []string {
		insertQueries := make([]string, 0, count)
		for i := 0; i < count; i++ {
			a, b := rand.IntN(1000), rand.IntN(100)
			insertQueries = append(insertQueries, fmt.Sprintf("INSERT INTO %s VALUES (%d, %d)", "foo", a, b))
		}
		return insertQueries
	}
	runQueriesAndBackup(t, db, collectionURIs, createInsertQueries(10), []string{}, false)
	runQueriesAndBackup(t, db, collectionURIs, createInsertQueries(10), []string{}, false)
	runQueriesAndBackup(t, db, collectionURIs, createInsertQueries(10), []string{"foo"}, false)
	runQueriesAndBackup(
		t, db, collectionURIs, []string{"UPDATE foo SET b = b + 1 ORDER BY random() LIMIT 5"},
		[]string{"foo"}, false,
	)
	runQueriesAndBackup(
		t, db, collectionURIs, []string{"DELETE FROM foo ORDER BY random() LIMIT 5"},
		[]string{"foo"}, true,
	)
}

// runQueriesAndBackup runs the given queries and then executes an incremental backup. If compaction
// is set to true, it will also compact the backups after the backup is taken. It then
// validates the compaction on the given tables. The URIs should be a comma separated list of URIs.
// It assumes that a full backup exists at the collection URIs.
func runQueriesAndBackup(
	t *testing.T,
	db *sqlutils.SQLRunner,
	collectionURIs string,
	queries []string,
	tablesToValidate []string,
	compact bool,
) {
	t.Helper()
	for _, query := range queries {
		db.Exec(t, query)
	}
	if compact {
		defer testutils.HookGlobal(&doCompaction, true)()
	}
	db.Exec(t, fmt.Sprintf("BACKUP INTO LATEST IN (%s)", collectionURIs))
	if compact {
		validateCompactedBackupForTables(t, db, tablesToValidate, collectionURIs)
	}
}

func validateCompactedBackupForTables(
	t *testing.T, db *sqlutils.SQLRunner, tables []string, collectionURIs string,
) {
	t.Helper()
	for _, table := range tables {
		originalRows := db.QueryStr(t, "SELECT * FROM "+table)
		db.Exec(t, "DROP TABLE "+table)
		db.Exec(
			t, fmt.Sprintf("RESTORE TABLE %s FROM LATEST IN (%s)", table, collectionURIs),
		)
		restoredRows := db.QueryStr(t, "SELECT * FROM "+table)
		require.Equal(t, originalRows, restoredRows, "table %s", table)
	}
}
