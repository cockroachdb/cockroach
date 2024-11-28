// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backuprand

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/backup/backuptestutils"
	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/fingerprintutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBackupRestoreRandomDataRoundtrips conducts backup/restore roundtrips on
// randomly generated tables and verifies their data and schema are preserved.
// It tests that full database backup as well as all subsets of per-table backup
// roundtrip properly. 50% of the time, the test runs the restore with the
// schema_only parameter, which does not restore any rows from user tables.
func TestBackupRestoreRandomDataRoundtrips(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "takes >1 min under race")
	rng, _ := randutil.NewPseudoRand()
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	defer nodelocal.ReplaceNodeLocalForTesting(t.TempDir())()
	params := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// Fails with the default test tenant due to span limits. Tracked
			// with #76378.
			DefaultTestTenant: base.TODOTestTenantDisabled,
			UseDatabase:       "rand",
			ExternalIODir:     dir,
		},
	}
	const localFoo = "nodelocal://1/foo/"

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, params)
	defer tc.Stopper().Stop(ctx)
	tc.ToggleReplicateQueues(false)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, "CREATE DATABASE rand")

	setup := sqlsmith.Setups[sqlsmith.RandTableSetupName](rng)
	for _, stmt := range setup {
		if _, err := tc.Conns[0].Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	numInserts := 20

	runSchemaOnlyExtension := ""
	if rng.Intn(10)%2 == 0 {
		runSchemaOnlyExtension = ", schema_only"
	}

	tables := sqlDB.Query(t, `SELECT name FROM crdb_internal.tables WHERE 
database_name = 'rand' AND schema_name = 'public'`)
	var tableNames []string
	for tables.Next() {
		var tableName string
		if err := tables.Scan(&tableName); err != nil {
			t.Fatal(err)
		}
		// Note: we do not care how many rows successfully populate
		// the given table
		if _, err := randgen.PopulateTableWithRandData(rng, tc.Conns[0], tableName,
			numInserts, nil); err != nil {
			t.Fatal(err)
		}
		tableNames = append(tableNames, tableName)
	}

	expectedCreateTableStmt := make(map[string]string)
	expectedData := make(map[string]int64)
	for _, tableName := range tableNames {
		expectedCreateTableStmt[tableName] = sqlDB.QueryStr(t,
			fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE TABLE %s]`, tree.NameString(tableName)))[0][0]
		if runSchemaOnlyExtension == "" {
			var err error
			tableID := sqlutils.QueryTableID(t, sqlDB.DB, "rand", "public", tableName)
			expectedData[tableName], err = fingerprintutils.FingerprintTable(ctx, tc.Conns[0], tableID,
				fingerprintutils.Stripped())
			require.NoError(t, err)
		}
	}

	// Now that we've created our random tables, backup and restore the whole DB
	// and compare all table descriptors for equality.

	dbBackup := localFoo + "wholedb"
	tablesBackup := localFoo + "alltables"
	dbBackups := []string{dbBackup, tablesBackup}
	if err := backuptestutils.VerifyBackupRestoreStatementResult(
		t, sqlDB, "BACKUP DATABASE rand INTO $1", dbBackup,
	); err != nil {
		t.Fatal(err)
	}
	if err := backuptestutils.VerifyBackupRestoreStatementResult(
		t, sqlDB, "BACKUP TABLE rand.* INTO $1", tablesBackup,
	); err != nil {
		t.Fatal(err)
	}

	// verifyTables asserts that the list of input tables in the restored
	// database, restoredb, contains the same schema as the original randomly
	// generated tables.
	verifyTables := func(t *testing.T, tableNames []string, restoreCmd string) {

		if strings.Contains(restoreCmd, "experimental deferred copy") {
			var downloadJobID jobspb.JobID
			sqlDB.QueryRow(t, `SELECT job_id FROM [SHOW JOBS] WHERE description LIKE '%Background Data Download%' ORDER BY created LIMIT 1`).Scan(&downloadJobID)
			jobutils.WaitForJobToSucceed(t, sqlDB, downloadJobID)
		}

		for _, tableName := range tableNames {
			t.Logf("Verifying table %q", tableName)
			restoreTable := "restoredb." + tree.NameString(tableName)
			createStmt := sqlDB.QueryStr(t,
				fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE TABLE %s]`, restoreTable))[0][0]
			if !strings.Contains(restoreCmd, "experimental deferred copy") {
				// TODO(msbutler): Remove this condition after
				// https://github.com/cockroachdb/cockroach/pull/119955 merges.
				assert.Equal(t, expectedCreateTableStmt[tableName], createStmt,
					"SHOW CREATE %s not equal after RESTORE", tableName)
			}

			if runSchemaOnlyExtension == "" {
				tableID := sqlutils.QueryTableID(t, sqlDB.DB, "restoredb", "public", tableName)
				fingerpint, err := fingerprintutils.FingerprintTable(ctx, tc.Conns[0], tableID,
					fingerprintutils.Stripped())
				require.NoError(t, err)
				require.Equal(t, expectedData[tableName], fingerpint)
			} else {
				sqlDB.CheckQueryResults(t, fmt.Sprintf(`SELECT count(*) FROM %s`, restoreTable),
					[][]string{{"0"}})
			}
		}
	}

	withOnlineRestore := func() string {
		onlineRestoreExtension := ""
		if rng.Intn(2) != 0 {
			onlineRestoreExtension = ", experimental deferred copy"
		}
		return onlineRestoreExtension
	}

	// This loop tests that two kinds of table restores (full database restore
	// and per-table restores) work properly with two kinds of table backups
	// (full database backups and per-table backups).
	for _, backup := range dbBackups {
		sqlDB.Exec(t, "DROP DATABASE IF EXISTS restoredb")
		sqlDB.Exec(t, "CREATE DATABASE restoredb")

		online := withOnlineRestore()
		tableQuery := fmt.Sprintf("RESTORE rand.* FROM LATEST IN $1 WITH OPTIONS (into_db='restoredb'%s%s)", runSchemaOnlyExtension, online)
		if err := backuptestutils.VerifyBackupRestoreStatementResult(
			t, sqlDB, tableQuery, backup,
		); err != nil && online == "" {
			// Only fail on error for non-online restores as verification checks output for regular restores
			t.Fatal(err)
		}
		verifyTables(t, tableNames, tableQuery)
		sqlDB.Exec(t, "DROP DATABASE IF EXISTS restoredb")

		online = withOnlineRestore()
		dbQuery := fmt.Sprintf("RESTORE DATABASE rand FROM LATEST IN $1 WITH OPTIONS (new_db_name='restoredb'%s%s)", runSchemaOnlyExtension, online)
		if err := backuptestutils.VerifyBackupRestoreStatementResult(
			t, sqlDB, dbQuery, backup,
		); err != nil && online == "" {
			t.Fatal(err)
		}
		verifyTables(t, tableNames, dbQuery)
	}

	tableNameCombos := powerset(tableNames)

	for i, combo := range tableNameCombos {
		sqlDB.Exec(t, "DROP DATABASE IF EXISTS restoredb")
		sqlDB.Exec(t, "CREATE DATABASE restoredb")
		backupTarget := fmt.Sprintf("%s%d", localFoo, i)
		if len(combo) == 0 {
			continue
		}
		var buf strings.Builder
		comma := ""
		for _, t := range combo {
			buf.WriteString(comma)
			buf.WriteString(tree.NameString(t))
			comma = ", "
		}
		tables := buf.String()
		t.Logf("Testing subset backup/restore %s", tables)
		sqlDB.Exec(t, fmt.Sprintf(`BACKUP TABLE %s INTO $1`, tables), backupTarget)
		comboQuery := fmt.Sprintf("RESTORE TABLE %s FROM LATEST IN $1 WITH OPTIONS (into_db='restoredb' %s%s)", tables, runSchemaOnlyExtension, withOnlineRestore())
		_, err := tc.Conns[0].Exec(comboQuery, backupTarget)
		if err != nil {
			if strings.Contains(err.Error(), "skip_missing_foreign_keys") {
				// Ignore subset, since we can't restore subsets that don't include the
				// full foreign key graph for any of the contained tables.
				continue
			}
			t.Fatal(err)
		}
		verifyTables(t, combo, comboQuery)
		t.Log("combo", i, combo)
	}
}

// powerset returns the powerset of the input slice of strings - all subsets,
// including the empty subset.
func powerset(input []string) [][]string {
	return powersetHelper(input, []string{})
}

func powersetHelper(ps, new []string) [][]string {
	if len(ps) == 0 {
		return [][]string{new}
	}
	res := powersetHelper(ps[1:], new[:len(new):len(new)])
	return append(res, powersetHelper(ps[1:], append(new, ps[0]))...)
}
