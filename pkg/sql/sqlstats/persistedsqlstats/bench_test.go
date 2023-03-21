// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package persistedsqlstats_test

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func BenchmarkConcurrentSelect1(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	for _, numOfConcurrentConn := range []int{24, 48, 64} {
		b.Run(fmt.Sprintf("concurrentConn=%d", numOfConcurrentConn),
			func(b *testing.B) {
				s, db, _ := serverutils.StartServer(b, base.TestServerArgs{})
				sqlServer := s.SQLServer().(*sql.Server)
				defer s.Stopper().Stop(ctx)

				starter := make(chan struct{})
				latencyChan := make(chan float64, numOfConcurrentConn)
				defer close(latencyChan)

				var wg sync.WaitGroup
				for connIdx := 0; connIdx < numOfConcurrentConn; connIdx++ {
					sqlConn, err := db.Conn(ctx)
					if err != nil {
						b.Fatalf("unexpected error creating db conn: %s", err)
					}
					wg.Add(1)

					go func(conn *gosql.Conn, idx int) {
						defer wg.Done()
						runner := sqlutils.MakeSQLRunner(conn)
						<-starter

						start := timeutil.Now()
						for i := 0; i < b.N; i++ {
							runner.Exec(b, "SELECT 1")
						}
						duration := timeutil.Since(start)
						latencyChan <- float64(duration.Milliseconds()) / float64(b.N)
					}(sqlConn, connIdx)
				}

				close(starter)
				wg.Wait()

				var totalLat float64
				for i := 0; i < numOfConcurrentConn; i++ {
					totalLat += <-latencyChan
				}
				b.ReportMetric(
					sqlServer.ServerMetrics.
						StatsMetrics.
						SQLTxnStatsCollectionOverhead.
						Mean(),
					"overhead(ns/op)")
			})
	}
}

// runBenchmarkPersistedSqlStatsFlush benchmarks the persisted stats
func runBenchmarkPersistedSqlStatsFlush(
	b *testing.B, tc *testcluster.TestCluster, db *sqlutils.SQLRunner, ctx context.Context,
) {
	rng := randutil.NewTestRandWithSeed(0)
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Select 20000 rows
		for j := 0; j < 20000; j++ {
			appName := randutil.RandString(rng, 5, charset)
			db.Exec(b, fmt.Sprintf("SET application_name='%s'", appName))
			db.Exec(b, "SELECT id FROM bench.t1 LIMIT 5")
		}
		b.StartTimer()
		tc.Server(0).SQLServer().(*sql.Server).GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats).Flush(ctx)
		b.StopTimer()
	}
}

// runBenchmarkPersistedSqlStatsSelects benchmarks select statements
func runBenchmarkPersistedSqlStatsSelects(b *testing.B, db *sqlutils.SQLRunner, query string) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StartTimer()
		rows := db.Query(b, query)
		if err := rows.Err(); err != nil {
			b.Fatal(err)
		}
		rows.Close()
		b.StopTimer()
	}
}

// BenchmarkSqlStatsPersisted tests measures the performance of persisted
// statistics.
func BenchmarkSqlStatsPersisted(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)

	type clusterCreationFn func() (*sqlutils.SQLRunner, *testcluster.TestCluster)
	type clusterSpec struct {
		name   string
		create clusterCreationFn
	}
	for _, cluster := range []clusterSpec{
		{
			name: "3node",
			create: func() (*sqlutils.SQLRunner, *testcluster.TestCluster) {
				tc := testcluster.StartTestCluster(b, 3,
					base.TestClusterArgs{
						ReplicationMode: base.ReplicationAuto,
						ServerArgs: base.TestServerArgs{
							UseDatabase:       "bench",
							SQLMemoryPoolSize: 512 << 20,
						},
					})
				sqlRunner := sqlutils.MakeRoundRobinSQLRunner(tc.Conns[0],
					tc.Conns[1], tc.Conns[2])
				return sqlRunner, tc
			},
		},
		{
			name: "6node",
			create: func() (*sqlutils.SQLRunner, *testcluster.TestCluster) {
				tc := testcluster.StartTestCluster(b, 6,
					base.TestClusterArgs{
						ReplicationMode: base.ReplicationAuto,
						ServerArgs: base.TestServerArgs{
							UseDatabase:       "bench",
							SQLMemoryPoolSize: 512 << 20,
						},
					})
				sqlRunner := sqlutils.MakeRoundRobinSQLRunner(tc.Conns[0],
					tc.Conns[1], tc.Conns[2], tc.Conns[3],
					tc.Conns[4], tc.Conns[5])
				return sqlRunner, tc
			},
		},
	} {
		b.Run(cluster.name, func(b *testing.B) {
			type testSpec struct {
				// array of computed columns
				computedColumns []string
			}
			for _, test := range []testSpec{
				{computedColumns: []string{"execution_count", "service_latency",
					"cpu_sql_nanos", "contention_time",
					"total_estimated_execution_time", "p99_latency"}},
				{computedColumns: []string{"execution_count", "service_latency",
					"cpu_sql_nanos", "contention_time", "total_estimated_execution_time"}},
				{computedColumns: []string{"execution_count", "service_latency",
					"cpu_sql_nanos", "contention_time"}},
				{computedColumns: []string{"execution_count", "service_latency",
					"cpu_sql_nanos"}},
				{computedColumns: []string{"execution_count", "service_latency"}},
				{computedColumns: []string{"execution_count"}},
				{computedColumns: []string{}},
			} {

				// compute expressions
				executionCount :=
					"((statistics->'statistics'->'cnt')::INT8)"
				serviceLatency :=
					"((statistics->'statistics'->'svcLat'->'mean')::FLOAT)"
				cpuSqlNanos :=
					"((statistics->'execution_statistics'->'cpuSQLNanos'->'mean')::FLOAT)"
				contentionTime :=
					"((statistics->'execution_statistics'->'contentionTime'->'mean')::FLOAT)"
				totalEstimatedExecutionTime :=
					"(statistics->'statistics'->>'cnt')::FLOAT * (statistics->'statistics'->'svcLat'->>'mean')::FLOAT"
				p99Latency :=
					"((statistics->'statistics'->'latencyInfo'->'p99')::FLOAT)"
				computeExpressions := []string{executionCount, serviceLatency,
					cpuSqlNanos, contentionTime, totalEstimatedExecutionTime, p99Latency}

				// Indexes
				executionCountIdx := "execution_count_idx"
				serviceLatencyIdx := "service_latency_idx"
				sqlCpuNanosIdx := "cpu_sql_nanos_idx"
				contentionTimeIdx := "contention_time_idx"
				p99LatencyIdx := "p99_latency_idx"
				totalEstimatedExecutionTimeIdx := "total_estimated_execution_time_idx"

				// DROP INDEX queries
				dropExecutionCountIdx :=
					fmt.Sprintf("DROP INDEX system.transaction_statistics@%s; "+
						"DROP INDEX system.statement_statistics@%s;", executionCountIdx,
						executionCountIdx)
				dropServiceLatencyIdx :=
					fmt.Sprintf("DROP INDEX system.transaction_statistics@%s; "+
						"DROP INDEX system.statement_statistics@%s;", serviceLatencyIdx,
						serviceLatencyIdx)
				dropSqlCpuNanosIdx :=
					fmt.Sprintf("DROP INDEX system.transaction_statistics@%s; "+
						"DROP INDEX system.statement_statistics@%s;", sqlCpuNanosIdx,
						sqlCpuNanosIdx)
				dropContentionTimeIdx :=
					fmt.Sprintf("DROP INDEX system.transaction_statistics@%s; "+
						"DROP INDEX system.statement_statistics@%s;", contentionTimeIdx,
						contentionTimeIdx)
				dropTotalEstimatedExecutionTimeIdx :=
					fmt.Sprintf("DROP INDEX system.transaction_statistics@%s; "+
						"DROP INDEX system.statement_statistics@%s;",
						totalEstimatedExecutionTimeIdx, totalEstimatedExecutionTimeIdx)
				dropP99LatencyIdx :=
					fmt.Sprintf("DROP INDEX system.transaction_statistics@%s; "+
						"DROP INDEX system.statement_statistics@%s;", p99LatencyIdx,
						p99LatencyIdx)
				dropQueries := []string{dropExecutionCountIdx, dropServiceLatencyIdx,
					dropSqlCpuNanosIdx, dropContentionTimeIdx,
					dropTotalEstimatedExecutionTimeIdx, dropP99LatencyIdx}

				var name strings.Builder
				if test.computedColumns == nil {
					name.WriteString("computedColumns=none")
				} else {
					name.WriteString(fmt.Sprintf("computedColumns=%d",
						len(test.computedColumns)))
				}
				for i, val := range test.computedColumns {
					if i > len(dropQueries) {
						dropQueries = []string{}
					} else {
						dropQueries = dropQueries[1:]
					}
					computeExpressions[i] = val
				}
				var selectQueries []string
				for i := range computeExpressions {
					orderClause := fmt.Sprintf(" ORDER BY %s DESC LIMIT 500",
						computeExpressions[i])
					baseQuery := fmt.Sprintf(`SELECT
										app_name,
										aggregated_ts,
										fingerprint_id,
										%s,
										%s,
										%s,
										%s,
										%s,
										%s,
										metadata,
										statistics
									FROM system.transaction_statistics
									WHERE app_name NOT LIKE '$ internal%%' AND 
										aggregated_ts > (now() - INTERVAL '1 hour') %s`,
						computeExpressions[0],
						computeExpressions[1],
						computeExpressions[2],
						computeExpressions[3],
						computeExpressions[4],
						computeExpressions[5],
						orderClause)
					selectQueries = append(selectQueries, baseQuery)
				}
				txnStatsQuery := fmt.Sprintf(`SELECT * FROM 
								((%s) UNION (%s) UNION (%s) UNION (%s) UNION (%s) UNION (%s))`,
					selectQueries[0], selectQueries[1], selectQueries[2],
					selectQueries[3], selectQueries[4], selectQueries[5])

				selectQueries = []string{}
				for i := range computeExpressions {
					orderClause := fmt.Sprintf(" ORDER BY %s DESC LIMIT 500",
						computeExpressions[i])
					baseQuery := fmt.Sprintf(`SELECT
										app_name,
										aggregated_ts,
										fingerprint_id,
										transaction_fingerprint_id,
										%s,
										%s,
										%s,
										%s,
										%s,
										%s,
										metadata,
										statistics
									FROM system.statement_statistics
									WHERE app_name NOT LIKE '$ internal%%' AND 
										aggregated_ts > (now() - INTERVAL '1 hour') %s`,
						computeExpressions[0],
						computeExpressions[1],
						computeExpressions[2],
						computeExpressions[3],
						computeExpressions[4],
						computeExpressions[5],
						orderClause)
					selectQueries = append(selectQueries, baseQuery)
				}
				stmtStatsQuery := fmt.Sprintf(`SELECT * FROM
								((%s) UNION (%s) UNION (%s) UNION (%s) UNION (%s) UNION (%s))`,
					selectQueries[0], selectQueries[1], selectQueries[2],
					selectQueries[3], selectQueries[4], selectQueries[5])
				b.Run(name.String(), func(b *testing.B) {
					ctx := context.Background()
					sqlRunner, tc := cluster.create()
					defer tc.Stopper().Stop(ctx)
					sqlRunner.Exec(b, `INSERT INTO system.users VALUES ('node', NULL, 
							true, 3)`)
					sqlRunner.Exec(b, `GRANT node TO root`)
					sqlRunner.Exec(b, `CREATE DATABASE IF NOT EXISTS bench`)
					for _, query := range dropQueries {
						sqlRunner.Exec(b, query)
					}
					sqlRunner.Exec(b, "CREATE TABLE bench.t1 ("+
						"id UUID PRIMARY KEY NOT NULL DEFAULT gen_random_uuid())")

					// Insert 10000 rows
					var buf bytes.Buffer
					buf.WriteString("INSERT INTO bench.t1 (id) VALUES (DEFAULT)")
					for j := 0; j < 10000; j++ {
						if j > 0 {
							buf.WriteString(", (DEFAULT)")
						}
					}
					sqlRunner.Exec(b, buf.String())
					buf.Reset()

					b.ReportAllocs()
					// Run flush benchmark first to initialize stats tables
					b.Run("BenchmarkPersistedSqlStatsFlush", func(b *testing.B) {
						runBenchmarkPersistedSqlStatsFlush(b, tc, sqlRunner, ctx)
					})

					// Run flush benchmark first to initialize stats tables
					b.Run("BenchmarkPersistedSqlStatsSelectStatements",
						func(b *testing.B) {
							runBenchmarkPersistedSqlStatsSelects(b, sqlRunner, stmtStatsQuery)
						})

					// Run flush benchmark first to initialize stats tables
					b.Run("BenchmarkPersistedSqlStatsSelectTransactions",
						func(b *testing.B) {
							runBenchmarkPersistedSqlStatsSelects(b, sqlRunner, txnStatsQuery)
						})
				})
			}
		})
	}
}
