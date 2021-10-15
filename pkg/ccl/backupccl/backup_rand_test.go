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

	// TODO(jordan): we should insert random data using SQLSmith mutation
	// statements here.

	// Now that we've created our random tables, backup and restore the whole DB
	// and compare all table descriptors for equality.

	if err := verifyBackupRestoreStatementResult(
		t, sqlDB, "BACKUP DATABASE rand TO $1", LocalFoo,
	); err != nil {
		t.Fatal(err)
	}
	sqlDB.Exec(t, "CREATE DATABASE rand2")

	if err := verifyBackupRestoreStatementResult(
		t, sqlDB, "RESTORE rand.* FROM $1 WITH OPTIONS (into_db='rand2')", LocalFoo,
	); err != nil {
		t.Fatal(err)
	}

	expectedCreateTableStmt := make(map[string]string)
	for _, tableName := range tableNames {
		t.Logf("Verifying table %s", tableName)
		var ignored string
		var createStatement1, createStatement2 string
		table1 := sqlDB.QueryRow(t, fmt.Sprintf("SHOW CREATE TABLE rand.%s", tableName))
		table1.Scan(&ignored, &createStatement1)
		expectedCreateTableStmt[tableName] = createStatement1

		table2 := sqlDB.QueryRow(t, fmt.Sprintf("SHOW CREATE TABLE rand2.%s", tableName))
		table2.Scan(&ignored, &createStatement2)

		assert.Equal(t, createStatement1, createStatement2, "SHOW CREATE %s not equal after RESTORE", tableName)
	}

	tableNameCombos := powerset(tableNames)

	for i, combo := range tableNameCombos {
		sqlDB.Exec(t, "DROP DATABASE IF EXISTS restoredb; CREATE DATABASE restoredb")
		backupTarget := fmt.Sprintf("%s%d", LocalFoo, i)
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

		for _, tableName := range combo {
			t.Logf("Verifying table %s", tableName)
			var ignored, createStatement string
			table := sqlDB.QueryRow(t, fmt.Sprintf("SHOW CREATE TABLE restoredb.%s", tableName))
			table.Scan(&ignored, &createStatement)

			assert.Equal(t, createStatement, expectedCreateTableStmt[tableName], "SHOW CREATE %s not equal after RESTORE",
				tableName)
		}
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
	res := powersetHelper(ps[1:], new)
	return append(res, powersetHelper(ps[1:], append(new, ps[0]))...)
}
