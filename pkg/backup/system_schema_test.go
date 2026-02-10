// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestAllSystemTablesHaveBackupConfig is a test that enumerates all system
// table names and ensures that a config is specified for each tables.
func TestAllSystemTablesHaveBackupConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				DefaultTestTenant: base.TestControlsTenantsExplicitly,
			}})
	defer tc.Stopper().Stop(ctx)
	systemSQL := sqlutils.MakeSQLRunner(tc.Conns[0])

	_, tSQL := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{
		TenantID:     roachpb.MustMakeTenantID(10),
		TestingKnobs: base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()},
	})
	defer tSQL.Close()

	secondaryTenantSQL := sqlutils.MakeSQLRunner(tSQL)

	verifySystemTables := func(tableNames [][]string) {
		for _, systemTableNameRow := range tableNames {
			systemTableName := systemTableNameRow[0]
			if systemTableBackupConfiguration[systemTableName].shouldIncludeInClusterBackup == invalidBackupInclusion {
				t.Fatalf("cluster backup inclusion not specified for system table %s", systemTableName)
			}
		}
	}
	tableNamesQuery := `USE system; SELECT table_name FROM [SHOW TABLES];`

	verifySystemTables(systemSQL.QueryStr(t, tableNamesQuery))
	verifySystemTables(secondaryTenantSQL.QueryStr(t, tableNamesQuery))

}

func TestConfigurationDetailsOnlySetForIncludedTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for systemTable, configuration := range systemTableBackupConfiguration {
		if configuration.customRestoreFunc != nil {
			// If some restore options were specified, we probably want to also
			// include in in the set of system tables that are looked at by cluster
			// backup/restore.
			if optInToClusterBackup != configuration.shouldIncludeInClusterBackup {
				t.Fatalf("custom restore function specified for table %q, but it's not included in cluster backups",
					systemTable)
			}
		}
	}
}

type rekeyPair struct {
	oldID descpb.ID
	newID descpb.ID
}

func rekey(oldID, newID descpb.ID) rekeyPair {
	return rekeyPair{oldID: oldID, newID: newID}
}

func rekeyMap(pairs ...rekeyPair) jobspb.DescRewriteMap {
	m := make(jobspb.DescRewriteMap)
	for _, p := range pairs {
		m[p.oldID] = &jobspb.DescriptorRewrite{ID: p.newID}
	}
	return m
}

type testRow struct {
	id descpb.ID
}

func row(id descpb.ID) testRow {
	return testRow{id: id}
}

type rekeyTestCase struct {
	name         string
	colName      string
	initialRows  []testRow
	rekeys       jobspb.DescRewriteMap
	expectedRows []descpb.ID
}

func rekeyTestHelper(
	ctx context.Context,
	t *testing.T,
	s serverutils.TestServerInterface,
	tableName string,
	tc rekeyTestCase,
	rekeyFunc func(string) func(context.Context, isql.Txn, string, jobspb.DescRewriteMap) error,
) {
	sqlDB := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("defaultdb")))
	ie := s.InternalDB().(isql.DB)

	// Delete existing data from previous test iteration.
	sqlDB.Exec(t, fmt.Sprintf("DELETE FROM %s", tableName))

	for _, r := range tc.initialRows {
		sqlDB.Exec(t, fmt.Sprintf("INSERT INTO %s (%s) VALUES ($1)", tableName, tc.colName), r.id)
	}

	err := ie.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return rekeyFunc(tc.colName)(ctx, txn, tableName, tc.rekeys)
	})
	require.NoError(t, err)

	rows := sqlDB.Query(t, fmt.Sprintf("SELECT %s FROM %s ORDER BY %s", tc.colName, tableName, tc.colName))
	defer rows.Close()

	actualRows := make([]descpb.ID, 0)
	for rows.Next() {
		var id descpb.ID
		require.NoError(t, rows.Scan(&id))
		actualRows = append(actualRows, id)
	}

	// Sort expected rows for comparison.
	expected := make([]descpb.ID, len(tc.expectedRows))
	copy(expected, tc.expectedRows)
	sort.Slice(expected, func(i, j int) bool { return expected[i] < expected[j] })

	require.Equal(t, expected, actualRows)
}

func TestRekeySystemTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("defaultdb")))
	const tableName = "defaultdb.public.test_rekey_table"
	sqlDB.Exec(t, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY)", tableName))

	testcases := []rekeyTestCase{
		{
			name:         "basic single remapping",
			colName:      "id",
			initialRows:  []testRow{row(100)},
			rekeys:       rekeyMap(rekey(100, 1001)),
			expectedRows: []descpb.ID{1001},
		},
		{
			name:         "multiple remappings",
			colName:      "id",
			initialRows:  []testRow{row(100), row(200), row(300)},
			rekeys:       rekeyMap(rekey(100, 1001), rekey(200, 2002), rekey(300, 3003)),
			expectedRows: []descpb.ID{1001, 2002, 3003},
		},
		{
			name:         "below 50 is preserved even without rekey",
			colName:      "id",
			initialRows:  []testRow{row(10)},
			rekeys:       rekeyMap(rekey(100, 1001)),
			expectedRows: []descpb.ID{10},
		},
		{
			name:         "above 50 removed without rekey",
			colName:      "id",
			initialRows:  []testRow{row(150)},
			rekeys:       rekeyMap(rekey(100, 1001)),
			expectedRows: []descpb.ID{},
		},
		{
			name:         "overlapping IDs where old ID matches another new ID",
			colName:      "id",
			initialRows:  []testRow{row(100), row(200)},
			rekeys:       rekeyMap(rekey(100, 200), rekey(200, 300)),
			expectedRows: []descpb.ID{200, 300},
		},
		{
			name:         "empty table",
			colName:      "id",
			initialRows:  []testRow{},
			rekeys:       rekeyMap(rekey(100, 1001)),
			expectedRows: []descpb.ID{},
		},
		{
			name:         "ID 50 remapped",
			colName:      "id",
			initialRows:  []testRow{row(50)},
			rekeys:       rekeyMap(rekey(50, 5000)),
			expectedRows: []descpb.ID{5000},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			rekeyTestHelper(ctx, t, s, tableName, tc, rekeySystemTable)
		})
	}
}

func TestNonClusterRekeySystemTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("defaultdb")))
	const tableName = "defaultdb.public.test_rekey_table"
	sqlDB.Exec(t, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY)", tableName))

	testcases := []rekeyTestCase{
		{
			name:         "basic single remapping",
			colName:      "id",
			initialRows:  []testRow{row(100)},
			rekeys:       rekeyMap(rekey(100, 1001)),
			expectedRows: []descpb.ID{1001},
		},
		{
			name:         "filters out rows not in rekey map",
			colName:      "id",
			initialRows:  []testRow{row(100), row(200)},
			rekeys:       rekeyMap(rekey(100, 1001)),
			expectedRows: []descpb.ID{1001},
		},
		{
			name:         "filters out system table IDs under 50 not in rekey map",
			colName:      "id",
			initialRows:  []testRow{row(10), row(100)},
			rekeys:       rekeyMap(rekey(100, 1001)),
			expectedRows: []descpb.ID{1001},
		},
		{
			name:         "overlapping IDs",
			colName:      "id",
			initialRows:  []testRow{row(100), row(200)},
			rekeys:       rekeyMap(rekey(100, 200), rekey(200, 300)),
			expectedRows: []descpb.ID{200, 300},
		},
		{
			name:         "empty table",
			colName:      "id",
			initialRows:  []testRow{},
			rekeys:       rekeyMap(rekey(100, 1001)),
			expectedRows: []descpb.ID{},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			rekeyTestHelper(ctx, t, s, tableName, tc, nonClusterRekeySystemTable)
		})
	}
}
