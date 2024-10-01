// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/kvclientutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestCreateAsVTable verifies that all vtables can be used as the source of
// CREATE TABLE AS and CREATE MATERIALIZED VIEW AS.
func TestCreateAsVTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlRunner := sqlutils.MakeSQLRunner(db)
	var p parser.Parser

	i := 0
	for _, vSchema := range virtualSchemas {
		for _, vSchemaDef := range vSchema.tableDefs {
			if vSchemaDef.isUnimplemented() {
				continue
			}

			var name tree.TableName
			var ctasColumns []string
			schema := vSchemaDef.getSchema()
			statements, err := p.Parse(schema)
			require.NoErrorf(t, err, schema)
			require.Lenf(t, statements, 1, schema)
			switch stmt := statements[0].AST.(type) {
			case *tree.CreateTable:
				name = stmt.Table
				for _, def := range stmt.Defs {
					if colDef, ok := def.(*tree.ColumnTableDef); ok {
						if colDef.Hidden {
							continue
						}
						// Filter out vector columns to prevent error in CTAS:
						// "VECTOR column types are unsupported".
						if colDef.Type.(*types.T).Identical(types.Int2Vector) ||
							colDef.Type.(*types.T).Identical(types.OidVector) {
							continue
						}
						ctasColumns = append(ctasColumns, colDef.Name.String())
					}
				}
			case *tree.CreateView:
				name = stmt.Name
				ctasColumns = []string{"*"}
			default:
				require.Failf(t, "missing case", "unexpected type %T for schema %s", stmt, schema)
			}

			fqName := name.FQString()
			if s.TenantController().StartedDefaultTestTenant() {
				// Some of the virtual tables are currently only available in
				// the system tenant.
				// TODO(yuzefovich): update this list when #54252 is addressed.
				onlySystemTenant := map[string]struct{}{
					`"".crdb_internal.gossip_alerts`:                  {},
					`"".crdb_internal.gossip_liveness`:                {},
					`"".crdb_internal.gossip_nodes`:                   {},
					`"".crdb_internal.kv_flow_controller`:             {},
					`"".crdb_internal.kv_flow_control_handles`:        {},
					`"".crdb_internal.kv_flow_token_deductions`:       {},
					`"".crdb_internal.kv_node_status`:                 {},
					`"".crdb_internal.kv_node_liveness`:               {},
					`"".crdb_internal.kv_store_status`:                {},
					`"".crdb_internal.node_tenant_capabilities_cache`: {},
					`"".crdb_internal.tenant_usage_details`:           {},
				}
				if _, ok := onlySystemTenant[fqName]; ok {
					continue
				}
			}
			// Filter by trace_id to prevent error when selecting from
			// crdb_internal.cluster_inflight_traces:
			// "pq: a trace_id value needs to be specified".
			var where string
			if fqName == `"".crdb_internal.cluster_inflight_traces` {
				where = " WHERE trace_id = 1"
			}

			createTableStmt := fmt.Sprintf(
				"CREATE TABLE test_table_%d AS SELECT %s FROM %s%s",
				i, strings.Join(ctasColumns, ", "), fqName, where,
			)
			sqlRunner.Exec(t, createTableStmt)
			createViewStmt := fmt.Sprintf(
				"CREATE MATERIALIZED VIEW test_view_%d AS SELECT * FROM %s%s",
				i, fqName, where,
			)
			sqlRunner.Exec(t, createViewStmt)
			i++
		}
	}

	waitForJobsSuccess(t, sqlRunner)
}

// TestCreateAsVTable verifies that SHOW commands can be used as the source of
// CREATE TABLE AS and CREATE MATERIALIZED VIEW AS.
func TestCreateAsShow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		sql   string
		setup string
	}{
		{
			sql: "SHOW CLUSTER SETTINGS",
		},
		{
			sql:   "SHOW CLUSTER SETTINGS FOR TENANT [2]",
			setup: "SELECT crdb_internal.create_tenant(2)",
		},
		{
			sql: "SHOW DATABASES",
		},
		{
			sql:   "SHOW ENUMS",
			setup: "CREATE TYPE e AS ENUM ('a', 'b')",
		},
		{
			sql:   "SHOW TYPES",
			setup: "CREATE TYPE p AS (x int, y int)",
		},
		{
			sql: "SHOW CREATE DATABASE defaultdb",
		},
		{
			sql: "SHOW CREATE ALL SCHEMAS",
		},
		{
			sql: "SHOW CREATE ALL TABLES",
		},
		{
			sql:   "SHOW CREATE TABLE show_create_tbl",
			setup: "CREATE TABLE show_create_tbl (id int PRIMARY KEY)",
		},
		{
			sql:   "SHOW CREATE FUNCTION show_create_fn",
			setup: "CREATE FUNCTION show_create_fn(i int) RETURNS INT AS 'SELECT i' LANGUAGE SQL",
		},
		{
			sql: "SHOW CREATE ALL TYPES",
		},
		{
			sql: "SHOW INDEXES FROM DATABASE defaultdb",
		},
		{
			sql:   "SHOW INDEXES FROM show_indexes_tbl",
			setup: "CREATE TABLE show_indexes_tbl (id int PRIMARY KEY)",
		},
		{
			sql:   "SHOW COLUMNS FROM show_columns_tbl",
			setup: "CREATE TABLE show_columns_tbl (id int PRIMARY KEY)",
		},
		{
			sql:   "SHOW CONSTRAINTS FROM show_constraints_tbl",
			setup: "CREATE TABLE show_constraints_tbl (id int PRIMARY KEY)",
		},
		{
			sql: "SHOW PARTITIONS FROM DATABASE defaultdb",
		},
		{
			sql:   "SHOW PARTITIONS FROM TABLE show_partitions_tbl",
			setup: "CREATE TABLE show_partitions_tbl (id int PRIMARY KEY)",
		},
		{
			sql:   "SHOW PARTITIONS FROM INDEX show_partitions_idx_tbl@show_partitions_idx_tbl_pkey",
			setup: "CREATE TABLE show_partitions_idx_tbl (id int PRIMARY KEY)",
		},
		{
			sql: "SHOW GRANTS",
		},
		{
			sql: "SHOW JOBS",
		},
		{
			sql: "SHOW CHANGEFEED JOBS",
		},
		{
			sql: "SHOW ALL CLUSTER STATEMENTS",
		},
		{
			sql: "SHOW ALL LOCAL STATEMENTS",
		},
		{
			sql: "SHOW ALL LOCAL STATEMENTS",
		},
		{
			sql: "SHOW RANGES WITH DETAILS, KEYS, TABLES",
		},
		{
			sql:   "SHOW RANGE FROM TABLE show_ranges_tbl FOR ROW (0)",
			setup: "CREATE TABLE show_ranges_tbl (id int PRIMARY KEY)",
		},
		{
			sql: "SHOW SURVIVAL GOAL FROM DATABASE",
		},
		{
			sql: "SHOW REGIONS FROM DATABASE",
		},
		{
			sql: "SHOW GRANTS ON ROLE",
		},
		{
			sql: "SHOW ROLES",
		},
		{
			sql: "SHOW SCHEMAS",
		},
		{
			sql:   "SHOW SEQUENCES",
			setup: "CREATE SEQUENCE seq",
		},
		{
			sql: "SHOW ALL SESSIONS",
		},
		{
			sql: "SHOW CLUSTER SESSIONS",
		},
		{
			sql: "SHOW SYNTAX 'SELECT 1'",
		},
		{
			sql:   "SHOW FUNCTIONS",
			setup: "CREATE FUNCTION show_functions_fn(i int) RETURNS INT AS 'SELECT i' LANGUAGE SQL",
		},
		{
			sql: "SHOW TABLES",
		},
		{
			sql: "SHOW ALL TRANSACTIONS",
		},
		{
			sql: "SHOW CLUSTER TRANSACTIONS",
		},
		{
			sql: "SHOW USERS",
		},
		{
			sql: "SHOW ALL",
		},
		{
			sql: "SHOW ZONE CONFIGURATIONS",
		},
		{
			sql: "SHOW SCHEDULES",
		},
		{
			sql: "SHOW JOBS FOR SCHEDULES SELECT id FROM [SHOW SCHEDULES]",
		},
		{
			sql: "SHOW FULL TABLE SCANS",
		},
		{
			sql: "SHOW DEFAULT PRIVILEGES",
		},
	}

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlRunner := sqlutils.MakeSQLRunner(db)

	for i, testCase := range testCases {
		t.Run(testCase.sql, func(t *testing.T) {
			if testCase.setup != "" {
				if s.TenantController().StartedDefaultTestTenant() && strings.Contains(testCase.setup, "create_tenant") {
					// Only the system tenant has the ability to create other
					// tenants.
					return
				}
				sqlRunner.Exec(t, testCase.setup)
			}
			createTableStmt := fmt.Sprintf(
				"CREATE TABLE test_table_%d AS SELECT * FROM [%s]",
				i, testCase.sql,
			)
			sqlRunner.Exec(t, createTableStmt)
			createViewStmt := fmt.Sprintf(
				"CREATE MATERIALIZED VIEW test_view_%d AS SELECT * FROM [%s]",
				i, testCase.sql,
			)
			sqlRunner.Exec(t, createViewStmt)
			i++
		})
	}

	waitForJobsSuccess(t, sqlRunner)
}

func waitForJobsSuccess(t *testing.T, sqlRunner *sqlutils.SQLRunner) {
	query := `SELECT job_id, status, error, description 
FROM [SHOW JOBS] 
WHERE job_type IN ('SCHEMA CHANGE', 'NEW SCHEMA CHANGE')
AND status != 'succeeded'`
	sqlRunner.CheckQueryResultsRetry(t, query, [][]string{})
}

// TestFormat verifies the statement in the schema change job description.
func TestFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		sql            string
		setup          string
		expectedFormat string
	}{
		{
			sql:            "CREATE TABLE ctas_implicit_columns_tbl AS SELECT * FROM ctas_implicit_columns_source_tbl",
			setup:          "CREATE TABLE ctas_implicit_columns_source_tbl (id int PRIMARY KEY)",
			expectedFormat: "CREATE TABLE defaultdb.public.ctas_implicit_columns_tbl (id) AS SELECT * FROM defaultdb.public.ctas_implicit_columns_source_tbl",
		},
		{
			sql:            "CREATE TABLE ctas_explicit_columns_tbl (id) AS SELECT * FROM ctas_explicit_columns_source_tbl",
			setup:          "CREATE TABLE ctas_explicit_columns_source_tbl (id int PRIMARY KEY)",
			expectedFormat: "CREATE TABLE defaultdb.public.ctas_explicit_columns_tbl (id) AS SELECT * FROM defaultdb.public.ctas_explicit_columns_source_tbl",
		},
		{
			sql:            "CREATE MATERIALIZED VIEW cmvas_implicit_columns_tbl AS SELECT * FROM cmvas_implicit_columns_source_tbl",
			setup:          "CREATE TABLE cmvas_implicit_columns_source_tbl (id int PRIMARY KEY)",
			expectedFormat: "CREATE MATERIALIZED VIEW defaultdb.public.cmvas_implicit_columns_tbl AS SELECT cmvas_implicit_columns_source_tbl.id FROM defaultdb.public.cmvas_implicit_columns_source_tbl WITH DATA",
		},
		{
			sql:            "CREATE MATERIALIZED VIEW cmvas_explicit_columns_tbl (id2) AS SELECT * FROM cmvas_explicit_columns_source_tbl",
			setup:          "CREATE TABLE cmvas_explicit_columns_source_tbl (id int PRIMARY KEY)",
			expectedFormat: "CREATE MATERIALIZED VIEW defaultdb.public.cmvas_explicit_columns_tbl (id2) AS SELECT cmvas_explicit_columns_source_tbl.id FROM defaultdb.public.cmvas_explicit_columns_source_tbl WITH DATA",
		},
	}

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlRunner := sqlutils.MakeSQLRunner(db)
	var p parser.Parser

	for _, tc := range testCases {
		t.Run(tc.sql, func(t *testing.T) {
			sqlRunner.Exec(t, tc.setup)
			sqlRunner.Exec(t, tc.sql)

			statements, err := p.Parse(tc.sql)
			require.NoError(t, err)
			require.Len(t, statements, 1)
			var name string
			switch stmt := statements[0].AST.(type) {
			case *tree.CreateTable:
				name = stmt.Table.Table()
			case *tree.CreateView:
				name = stmt.Name.Table()
			default:
				require.Failf(t, "missing case", "unexpected type %T", stmt)
			}
			// Filter description starting with CREATE to filter out CMVAS
			// "updating view reference" job.
			query := fmt.Sprintf(
				`SELECT description
FROM [SHOW JOBS SELECT id FROM system.jobs]
WHERE job_type IN ('SCHEMA CHANGE', 'NEW SCHEMA CHANGE')
AND description LIKE 'CREATE%%%s%%'`,
				name,
			)
			sqlRunner.CheckQueryResults(t, query, [][]string{{tc.expectedFormat}})
		})
	}
}

// TestTransactionRetryError tests that the schema changer succeeds if there is
// a retryable transaction error.
func TestTransactionRetryError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		desc          string
		setup         string
		query         string
		verifyResults func(sqlutils.Fataler, *sqlutils.SQLRunner)
	}{
		{
			desc:  "CREATE TABLE AS",
			setup: "CREATE SEQUENCE seq",
			query: "CREATE TABLE t AS SELECT nextval('seq')",
			verifyResults: func(t sqlutils.Fataler, sqlRunner *sqlutils.SQLRunner) {
				// Result should be 2 but is 3 because of this bug
				// https://github.com/cockroachdb/cockroach/issues/78457.
				sqlRunner.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"3"}})
			},
		},
		{
			desc:  "CREATE MATERIALIZED VIEW AS",
			setup: "CREATE SEQUENCE seq",
			query: "CREATE MATERIALIZED VIEW v AS SELECT nextval('seq')",
			verifyResults: func(t sqlutils.Fataler, sqlRunner *sqlutils.SQLRunner) {
				sqlRunner.CheckQueryResults(t, "SELECT * FROM v", [][]string{{"2"}})
			},
		},
	}

	ctx := context.Background()
	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			filterFunc, verifyFunc := kvclientutils.PrefixTransactionRetryFilter(t, schemaChangerBackfillTxnDebugName, 1)
			s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
				Knobs: base.TestingKnobs{
					KVClient: &kvcoord.ClientTestingKnobs{
						TransactionRetryFilter: filterFunc,
					},
				},
			})
			defer s.Stopper().Stop(ctx)
			sqlRunner := sqlutils.MakeSQLRunner(db)
			sqlRunner.Exec(t, testCase.setup)
			sqlRunner.Exec(t, testCase.query)
			verifyFunc()
			testCase.verifyResults(t, sqlRunner)
		})
	}
}
