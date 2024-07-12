// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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

func TestNoForbiddenSystemTablesInDebugZip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	forbiddenSysTables := []string{
		"system.users",
		"system.web_sessions",
		"system.join_tokens",
		"system.comments",
		"system.ui",
		"system.zones",
		"system.statement_bundle_chunks",
		"system.statement_statistics",
		"system.transaction_statistics",
		"system.statement_activity",
		"system.transaction_activity",
	}
	for _, forbiddenTable := range forbiddenSysTables {
		tableQuery, err := zipSystemTables.QueryForTable(forbiddenTable, false /* redact */)
		assert.Equal(t, "", tableQuery.query)
		assert.Error(t, err)
		assert.Equal(t, fmt.Sprintf("no entry found in table registry for: %s", forbiddenTable), err.Error())
	}
}

func TestNoNonSensitiveColsAndCustomRedactedQueries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	errFmtString := `FAILURE: The debug zip TableRegistryConfig for table %q
contains both a custom redacted query (customQueryRedacted) AND a list of
non sensitive columns (nonSensitiveCols). customQueryRedacted will ALWAYS
be used in place of nonSensitiveCols if defined, so please remove the
nonSensitiveCols. PLEASE be sure that NONE of the columns outside of those
listed in nonSensitiveCols have leaked into your customQueryRedacted, as 
this would be a PCI leak. If any columns in your customQueryRedacted were 
NOT already listed in nonSensitiveCols, you MUST confirm with the compliance 
team that these columns are acceptable to reveal in an unredacted manner, or
you must redact them at the SQL level.`
	for table, regConfig := range zipInternalTablesPerCluster {
		if regConfig.customQueryRedacted != "" && len(regConfig.nonSensitiveCols) > 0 {
			t.Fatalf(errFmtString, table)
		}
	}

	for table, regConfig := range zipInternalTablesPerNode {
		if regConfig.customQueryRedacted != "" && len(regConfig.nonSensitiveCols) > 0 {
			t.Fatalf(errFmtString, table)
		}
	}

	for table, regConfig := range zipSystemTables {
		if regConfig.customQueryRedacted != "" && len(regConfig.nonSensitiveCols) > 0 {
			t.Fatalf(errFmtString, table)
		}
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
	ctx := context.Background()

	params, _ := tests.CreateTestServerParams()
	cluster := serverutils.StartNewTestCluster(t, 1 /* numNodes */, base.TestClusterArgs{
		ServerArgs: params,
	})
	testConn := cluster.ServerConn(0 /* idx */)
	sqlDB := sqlutils.MakeSQLRunner(testConn)
	defer cluster.Stopper().Stop(ctx)

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

	params, _ := tests.CreateTestServerParams()
	cluster := serverutils.StartNewTestCluster(t, 1 /* numNodes */, base.TestClusterArgs{
		ServerArgs: params,
	})
	defer cluster.Stopper().Stop(context.Background())
	testConn := cluster.ServerConn(0 /* idx */)
	sqlDB := sqlutils.MakeSQLRunner(testConn)

	executeSelectOnNonSensitiveColumns(t, sqlDB, zipInternalTablesPerCluster)
	executeSelectOnNonSensitiveColumns(t, sqlDB, zipInternalTablesPerNode)
	executeSelectOnNonSensitiveColumns(t, sqlDB, zipSystemTables)
}
