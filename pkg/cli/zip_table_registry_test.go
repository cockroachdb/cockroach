// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryForTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	reg := DebugZipTableRegistry{
		"table_with_sensitive_cols": {
			nonSensitiveCols: NonSensitiveColumns{"x", "y", "z"},
		},
		"table_with_empty_sensitive_cols": {
			nonSensitiveCols: NonSensitiveColumns{},
		},
		"table_with_custom_queries": {
			customQueryUnredacted: "SELECT * FROM table_with_custom_queries",
			customQueryRedacted:   "SELECT a, b, c FROM table_with_custom_queries",
		},
		"table_with_non_sensitive_cols_and_custom_unredacted_query": {
			nonSensitiveCols:      NonSensitiveColumns{"x", "crdb_internal.pretty_key(y, 0) as y", "z"},
			customQueryUnredacted: "SELECT x, crdb_internal.pretty_key(y, 0) as y, z FROM table_with_non_sensitive_cols_and_custom_unredacted_query",
		},
		"table_with_non_sensitive_cols_and_custom_unredacted_query_with_fallback": {
			nonSensitiveCols:              NonSensitiveColumns{"x", "crdb_internal.pretty_key(y, 0) as y", "z"},
			customQueryUnredacted:         "SELECT x, crdb_internal.pretty_key(y, 0) as y, z FROM table_with_non_sensitive_cols_and_custom_unredacted_query_with_fallback",
			customQueryUnredactedFallback: "SELECT x FROM table_with_non_sensitive_cols_and_custom_unredacted_query_with_fallback",
		},
	}

	t.Run("errors if no table config present in registry", func(t *testing.T) {
		actual, err := reg.QueryForTable("does_not_exist", false /* redact */)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no entry found")
		assert.Empty(t, actual)
	})

	t.Run("produces `TABLE` query when unredacted with no custom query", func(t *testing.T) {
		table := "table_with_sensitive_cols"
		expected := TableQuery{query: "TABLE table_with_sensitive_cols"}
		actual, err := reg.QueryForTable(table, false /* redact */)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})

	t.Run("produces custom query when unredacted and custom query supplied", func(t *testing.T) {
		table := "table_with_custom_queries"
		expected := TableQuery{query: "SELECT * FROM table_with_custom_queries"}
		actual, err := reg.QueryForTable(table, false /* redact */)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})

	t.Run("produces query with only non-sensitive columns when redacted and no custom query", func(t *testing.T) {
		table := "table_with_sensitive_cols"
		expected := TableQuery{query: `SELECT x, y, z FROM table_with_sensitive_cols`}
		actual, err := reg.QueryForTable(table, true /* redact */)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})

	t.Run("produces custom when redacted and custom query supplied", func(t *testing.T) {
		table := "table_with_custom_queries"
		expected := TableQuery{query: "SELECT a, b, c FROM table_with_custom_queries"}
		actual, err := reg.QueryForTable(table, true /* redact */)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})

	t.Run("returns error when no custom queries and no non-sensitive columns supplied", func(t *testing.T) {
		table := "table_with_empty_sensitive_cols"
		actual, err := reg.QueryForTable(table, true /* redact */)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no non-sensitive columns defined")
		assert.Empty(t, actual)
	})

	t.Run("produces query when a combination of nonSensitiveCols and customQueryUnredacted is supplied", func(t *testing.T) {
		table := "table_with_non_sensitive_cols_and_custom_unredacted_query"
		expected := TableQuery{query: "SELECT x, crdb_internal.pretty_key(y, 0) as y, z FROM table_with_non_sensitive_cols_and_custom_unredacted_query"}

		t.Run("with redact flag", func(t *testing.T) {
			actual, err := reg.QueryForTable(table, true /* redact */)
			assert.NoError(t, err)
			assert.Equal(t, expected, actual)
		})

		t.Run("without redact flag", func(t *testing.T) {
			actual, err := reg.QueryForTable(table, false /* redact */)
			assert.NoError(t, err)
			assert.Equal(t, expected, actual)
		})
	})

	t.Run("with fallback query", func(t *testing.T) {
		table := "table_with_non_sensitive_cols_and_custom_unredacted_query_with_fallback"
		expected := TableQuery{
			query:    "SELECT x, crdb_internal.pretty_key(y, 0) as y, z FROM table_with_non_sensitive_cols_and_custom_unredacted_query_with_fallback",
			fallback: "SELECT x FROM table_with_non_sensitive_cols_and_custom_unredacted_query_with_fallback",
		}
		actual, err := reg.QueryForTable(table, false /* redact */)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})
}

func TestZipContainsAllSystemTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	rows := sqlDB.Query(t, `SELECT table_name FROM [SHOW TABLES FROM system] WHERE type = 'table' ORDER BY table_name`)
	defer rows.Close()

	var allSystemTables []string
	for rows.Next() {
		var tableName string
		require.NoError(t, rows.Scan(&tableName))
		allSystemTables = append(allSystemTables, "system."+tableName)
	}
	require.NoError(t, rows.Err())

	// Verify that every system table is either in zipSystemTables or in the
	// disabled list.
	var missingTables []string
	for _, fullTableName := range allSystemTables {
		if _, tbd := toBeTriaged[fullTableName]; tbd {
			// TODO(yuzefovich): remove this.
			continue
		}
		_, inRegistry := zipSystemTables[fullTableName]
		_, inDisabled := disabledSystemTables[fullTableName]
		if !inRegistry && !inDisabled {
			missingTables = append(missingTables, fullTableName)
		}
		// Ensure tables are not in both lists (would be redundant).
		require.Falsef(t, inRegistry && inDisabled, "system table %q is in both zipSystemTables and disabledSystemTables registries", fullTableName)
	}
	require.Falsef(t, len(missingTables) > 0, "the following system tables are neither in zipSystemTables nor in disabledSystemTables registries:\n%s", strings.Join(missingTables, "\n"))

	// Verify that disabled tables are indeed not in the registry.
	for disabledTable := range disabledSystemTables {
		tableQuery, err := zipSystemTables.QueryForTable(disabledTable, false /* redact */)
		require.Equal(t, "", tableQuery.query)
		require.Error(t, err)
	}
}

func TestTableRegistryConfigs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	validate := func(table string, regConfig TableRegistryConfig) {
		if regConfig.customQueryRedacted == "" && len(regConfig.nonSensitiveCols) == 0 {
			t.Fatalf("table %q contains no redacted query configuration", table)
		}
		if regConfig.customQueryRedacted != "" && len(regConfig.nonSensitiveCols) > 0 {
			t.Fatalf(
				"table %q has both customQueryRedacted and nonSensitiveCols. These fields are mutually exclusive.",
				table)
		}
	}

	for table, regConfig := range zipInternalTablesPerCluster {
		validate(table, regConfig)
	}
	for table, regConfig := range zipInternalTablesPerNode {
		validate(table, regConfig)
	}
	for table, regConfig := range zipSystemTables {
		validate(table, regConfig)
	}
}

func executeAllCustomQuerys(
	t *testing.T, sqlDB *sqlutils.SQLRunner, tableRegistry DebugZipTableRegistry,
) {
	for table, regConfig := range tableRegistry {
		if regConfig.customQueryRedacted != "" {
			rows := sqlDB.Query(t, regConfig.customQueryRedacted)
			require.NoError(t, rows.Err(), "failed to select for table %s redacted", table)
		}

		if regConfig.customQueryUnredacted != "" {
			rows := sqlDB.Query(t, regConfig.customQueryUnredacted)
			require.NoError(t, rows.Err(), "failed to select for table %s unredacted", table)
		}
	}
}

func TestCustomQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cluster := serverutils.StartCluster(t, 1 /* numNodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// The zip queries include queries that are only meant to work
			// in a system tenant. These would fail if pointed to a
			// secondary tenant.
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
	})
	defer cluster.Stopper().Stop(context.Background())
	testConn := cluster.ServerConn(0 /* idx */)
	sqlDB := sqlutils.MakeSQLRunner(testConn)

	executeAllCustomQuerys(t, sqlDB, zipInternalTablesPerCluster)
	executeAllCustomQuerys(t, sqlDB, zipInternalTablesPerNode)
	executeAllCustomQuerys(t, sqlDB, zipSystemTables)
}

func executeSelectOnNonSensitiveColumns(
	t *testing.T, sqlDB *sqlutils.SQLRunner, tableRegistry DebugZipTableRegistry,
) {

	for table, regConfig := range tableRegistry {
		if len(regConfig.nonSensitiveCols) != 0 {
			columns := strings.Join(regConfig.nonSensitiveCols[:], ",")
			rows := sqlDB.Query(t, fmt.Sprintf("SELECT %s FROM %s", columns, table))
			require.NoError(t, rows.Err(), "failed to select non sensitive columns on table %s", table)
		}
	}
}

func TestNonSensitiveColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cluster := serverutils.StartCluster(t, 1 /* numNodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// The zip queries include queries that are only meant to work
			// in a system tenant. These would fail if pointed to a
			// secondary tenant.
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
			//We are seeing certificate issue in CI test job. Hence,we are
			//running cluster in insecure mode.
			Insecure: true,
		},
	})
	defer cluster.Stopper().Stop(context.Background())
	testConn := cluster.ServerConn(0 /* idx */)
	sqlDB := sqlutils.MakeSQLRunner(testConn)

	executeSelectOnNonSensitiveColumns(t, sqlDB, zipInternalTablesPerCluster)
	executeSelectOnNonSensitiveColumns(t, sqlDB, zipInternalTablesPerNode)
	executeSelectOnNonSensitiveColumns(t, sqlDB, zipSystemTables)
}
