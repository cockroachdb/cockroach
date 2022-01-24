// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/assert"
)

// TestBackupRestoreRandomDataRoundtrips creates random tables using SQLSmith
// and backs up and restores them, ensuring that the schema is properly
// preserved across the roundtrip. It tests that full database backup as well
// as all subsets of per-table backup roundtrip properly.
func TestBackupRestoreRandomDataRoundtrips(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "takes >1 min under race")
	rng, _ := randutil.NewPseudoRand()
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	params := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			UseDatabase:   "rand",
			ExternalIODir: dir,
		},
	}
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, singleNode, params)
	defer tc.Stopper().Stop(ctx)
	InitManualReplication(tc)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, "CREATE DATABASE rand")

	setup := sqlsmith.Setups[sqlsmith.RandTableSetupName](rng)
	if _, err := tc.Conns[0].Exec(setup); err != nil {
		t.Fatal(err)
	}

	tables := sqlDB.Query(t, `SELECT name FROM crdb_internal.tables WHERE 
database_name = 'rand' AND schema_name = 'public'`)
	var tableNames []string
	for tables.Next() {
		var tableName string
		if err := tables.Scan(&tableName); err != nil {
			t.Fatal(err)
		}
		tableNames = append(tableNames, tableName)
	}

	getCreateStatementForTable := func(tableName string) string {
		var ignored, createStatement string
		table := sqlDB.QueryRow(t, fmt.Sprintf("SHOW CREATE TABLE %s", tableName))
		table.Scan(&ignored, &createStatement)
		return createStatement
	}

	expectedCreateTableStmt := make(map[string]string)
	for _, tableName := range tableNames {
		createStatement := getCreateStatementForTable(fmt.Sprintf("rand.%s", tableName))
		expectedCreateTableStmt[tableName] = createStatement
	}

	// TODO(jordan): we should insert random data using SQLSmith mutation
	// statements here.

	// Now that we've created our random tables, backup and restore the whole DB
	// and compare all table descriptors for equality.

	dbBackup := localFoo + "wholedb"
	tablesBackup := localFoo + "alltables"
	dbBackups := []string{dbBackup, tablesBackup}

	if err := verifyBackupRestoreStatementResult(
		t, sqlDB, "BACKUP DATABASE rand TO $1", dbBackup,
	); err != nil {
		t.Fatal(err)
	}
	if err := verifyBackupRestoreStatementResult(
		t, sqlDB, "BACKUP TABLE rand.* TO $1", tablesBackup,
	); err != nil {
		t.Fatal(err)
	}

	// verifyTables asserts that the list of input tables in the restored
	// database, restoredb, contains the same schema as the original randomly
	// generated tables.
	verifyTables := func(t *testing.T, tableNames []string) {
		for _, tableName := range tableNames {
			t.Logf("Verifying table %s", tableName)
			createStatement := getCreateStatementForTable("restoredb." + tableName)
			assert.Equal(t, expectedCreateTableStmt[tableName], createStatement, "SHOW CREATE %s not equal after RESTORE", tableName)
		}
	}

	// This loop tests that two kinds of table restores (full database restore
	// and per-table restores) work properly with two kinds of table backups
	// (full database backups and per-table backups).
	for _, backup := range dbBackups {
		sqlDB.Exec(t, "DROP DATABASE IF EXISTS restoredb; CREATE DATABASE restoredb")
		if err := verifyBackupRestoreStatementResult(
			t, sqlDB, "RESTORE rand.* FROM $1 WITH OPTIONS (into_db='restoredb')", backup,
		); err != nil {
			t.Fatal(err)
		}
		verifyTables(t, tableNames)
		sqlDB.Exec(t, "DROP DATABASE IF EXISTS restoredb")

		if err := verifyBackupRestoreStatementResult(
			t, sqlDB, "RESTORE DATABASE rand FROM $1 WITH OPTIONS (new_db_name='restoredb')", backup,
		); err != nil {
			t.Fatal(err)
		}
		verifyTables(t, tableNames)
	}

	tableNameCombos := powerset(tableNames)

	for i, combo := range tableNameCombos {
		sqlDB.Exec(t, "DROP DATABASE IF EXISTS restoredb; CREATE DATABASE restoredb")
		backupTarget := fmt.Sprintf("%s%d", localFoo, i)
		if len(combo) == 0 {
			continue
		}
		tables := strings.Join(combo, ", ")
		t.Logf("Testing subset backup/restore %s", tables)
		sqlDB.Exec(t, fmt.Sprintf(`BACKUP TABLE %s TO $1`, tables), backupTarget)
		_, err := tc.Conns[0].Exec(fmt.Sprintf("RESTORE TABLE %s FROM $1 WITH OPTIONS (into_db='restoredb')", tables),
			backupTarget)
		if err != nil {
			if strings.Contains(err.Error(), "skip_missing_foreign_keys") {
				// Ignore subset, since we can't restore subsets that don't include the
				// full foreign key graph for any of the contained tables.
				continue
			}
			t.Fatal(err)
		}
		verifyTables(t, combo)
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
