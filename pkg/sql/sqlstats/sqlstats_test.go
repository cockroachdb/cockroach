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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
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
	metadata -> 'implicitTxn' AS plan_implicit_txn,
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
