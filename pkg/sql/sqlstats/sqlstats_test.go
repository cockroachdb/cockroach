// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlstats_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestDataDrivenTest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLStatsKnobs: &sqlstats.TestingKnobs{
					SynchronousSQLStats: true,
				},
			},
		})

		defer s.Stopper().Stop(context.Background())
		statsRunner := sqlutils.MakeSQLRunner(conn)
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "exec-sql":
				sqlConn := s.SQLConn(t)
				defer sqlConn.Close()
				execSqlRunner := sqlutils.MakeSQLRunner(sqlConn)
				var appName, dbName string
				if d.HasArg("db") {
					d.ScanArgs(t, "db", &dbName)
					execSqlRunner.Exec(t, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName))
					execSqlRunner.Exec(t, fmt.Sprintf("USE %s", dbName))
				} else {
					t.Fatalf("db arg is required")
				}
				if d.HasArg("app-name") {
					d.ScanArgs(t, "app-name", &appName)
					execSqlRunner.Exec(t, "SET APPLICATION_NAME = $1", appName)
				}
				// Set tracing on to ensure kv exec stats are always captured
				execSqlRunner.Exec(t, "SET tracing = on")
				execSqlRunner.Exec(t, d.Input)
				return ""
			case "show-stats":
				var appName, dbName string
				if d.HasArg("app-name") {
					d.ScanArgs(t, "app-name", &appName)
				}
				if d.HasArg("db") {
					d.ScanArgs(t, "db", &dbName)
				}
				return GetStats(t, statsRunner, appName, dbName)
			case "show-txn-stats":
				var appName, dbName string
				if d.HasArg("app-name") {
					d.ScanArgs(t, "app-name", &appName)
				}
				if d.HasArg("db") {
					d.ScanArgs(t, "db", &dbName)
				}
				return GetTxnStats(t, statsRunner, appName, dbName)
			default:
				t.Fatalf("unexpected cmd %s", d.Cmd)
			}
			return ""
		})
	})
}

// TestStmtStatsQueryColumns verifies that the query, query_summary,
// and database columns are populated in crdb_internal.cluster_statement_statistics
// and in the persisted views that join with system.statements.
func TestStmtStatsQueryColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLStatsKnobs: &sqlstats.TestingKnobs{
				SynchronousSQLStats: true,
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	appName := "TestStmtStatsQueryColumns"

	// Execute a statement with a known app name and database.
	execConn := s.SQLConn(t)
	defer execConn.Close()
	execDB := sqlutils.MakeSQLRunner(execConn)
	execDB.Exec(t, "SET APPLICATION_NAME = $1", appName)
	execDB.Exec(t, "CREATE DATABASE testdb")
	execDB.Exec(t, "USE testdb")
	execDB.Exec(t, "SELECT 1")

	obsDB := sqlutils.MakeSQLRunner(conn)

	// Verify in-memory cluster_statement_statistics has the new columns populated.
	var query, querySummary, database string
	obsDB.QueryRow(t,
		`SELECT query, query_summary, database
		 FROM crdb_internal.cluster_statement_statistics
		 WHERE app_name = $1 AND query LIKE '%SELECT _'`, appName,
	).Scan(&query, &querySummary, &database)

	require.NotEmpty(t, query, "query column should be populated")
	require.Contains(t, query, "SELECT")
	require.NotEmpty(t, querySummary, "query_summary column should be populated")
	require.Equal(t, "testdb", database, "database column should match the database used")

	// Flush stats to disk so the persisted views have data.
	s.SQLServer().(*sql.Server).GetSQLStatsProvider().MaybeFlush(ctx, s.AppStopper())

	// Verify statement_statistics_persisted populates query/query_summary/database
	// via the JOIN with system.statements. The statementstore writer is
	// asynchronous, so wait for the row to appear.
	var pQuery, pQuerySummary, pDatabase string
	testutils.SucceedsSoon(t, func() error {
		row := obsDB.DB.QueryRowContext(ctx,
			`SELECT query, query_summary, database
			 FROM crdb_internal.statement_statistics_persisted
			 WHERE app_name = $1 AND query LIKE '%SELECT _'`, appName)
		if err := row.Scan(&pQuery, &pQuerySummary, &pDatabase); err != nil {
			return err
		}
		if pQuery == "" {
			return errors.New("query column not yet populated by statementstore")
		}
		return nil
	})

	require.Contains(t, pQuery, "SELECT")
	require.NotEmpty(t, pQuerySummary, "persisted: query_summary column should be populated")
	require.Equal(t, "testdb", pDatabase, "persisted: database column should match")

	// Verify the merged statement_statistics view also has the columns.
	var mQuery, mQuerySummary, mDatabase string
	obsDB.QueryRow(t,
		`SELECT query, query_summary, database
		 FROM crdb_internal.statement_statistics
		 WHERE app_name = $1 AND query LIKE '%SELECT _'`, appName,
	).Scan(&mQuery, &mQuerySummary, &mDatabase)

	require.NotEmpty(t, mQuery, "merged view: query column should be populated")
	require.NotEmpty(t, mQuerySummary, "merged view: query_summary column should be populated")
	require.Equal(t, "testdb", mDatabase, "merged view: database column should match")
}

func GetStats(t *testing.T, conn *sqlutils.SQLRunner, appName string, dbName string) string {
	t.Helper()
	query := ` SELECT row_to_json(s)  FROM (
SELECT 
	encode(fingerprint_id, 'hex') AS fingerprint_id,
	encode(transaction_fingerprint_id, 'hex') AS transaction_fingerprint_id,
	plan_hash != '\x0000000000000000' AS plan_hash_set,
	metadata ->> 'query' AS query,
	metadata ->> 'querySummary' AS summary,
	metadata -> 'distsql' AS plan_distributed,
	metadata -> 'vec' AS plan_vectorized,
	metadata -> 'fullScan' AS plan_full_scan,
	statistics -> 'statistics'->> 'cnt' AS count,
	statistics -> 'statistics'-> 'nodes' AS nodes,
	statistics -> 'statistics' -> 'sqlType' AS sql_type,
	(statistics -> 'statistics' -> 'parseLat' -> 'mean')::FLOAT > 0 AS parse_lat_not_zero,
	(statistics -> 'statistics' -> 'planLat' -> 'mean')::FLOAT > 0 AS plan_lat_not_zero,
	(statistics -> 'statistics' -> 'runLat' -> 'mean')::FLOAT > 0 AS run_lat_not_zero,
	(statistics -> 'statistics' -> 'svcLat' -> 'mean')::FLOAT > 0 AS svc_lat_not_zero
FROM crdb_internal.statement_statistics
WHERE app_name = $1
AND metadata->>'db' = $2
ORDER BY fingerprint_id) s
`
	res := conn.QueryStr(t, query, appName, dbName)
	rows := make([]string, len(res))
	for rowIdx := range res {
		rows[rowIdx] = strings.Join(res[rowIdx], ",")
	}
	return strings.Join(rows, "\n")
}

func GetTxnStats(t *testing.T, conn *sqlutils.SQLRunner, appName string, dbName string) string {
	query := ` SELECT row_to_json(s) FROM (
SELECT 
	encode(fingerprint_id, 'hex') AS txn_fingerprint_id,
	metadata->'stmtFingerprintIDs' AS statement_fingerprint_ids,
  statistics -> 'statistics'->> 'cnt' AS count,
	(statistics -> 'statistics' -> 'commitLat' -> 'mean')::FLOAT > 0 AS commit_lat_not_zero,
	(statistics -> 'statistics' -> 'svcLat' -> 'mean')::FLOAT > 0 AS svc_lat_not_zero
FROM crdb_internal.transaction_statistics ts
WHERE ts.fingerprint_id in (
    SELECT DISTINCT ss.transaction_fingerprint_id
    FROM crdb_internal.statement_statistics ss
    WHERE app_name = $1
		AND metadata->>'db' = $2
)
ORDER BY fingerprint_id) s
`
	res := conn.QueryStr(t, query, appName, dbName)
	rows := make([]string, len(res))
	for rowIdx := range res {
		rows[rowIdx] = strings.Join(res[rowIdx], ",")
	}
	return strings.Join(rows, "\n")
}
