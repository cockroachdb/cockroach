// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scannedspanstats_test

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// runBenchmarkScans benchmarks select statements
func runBenchmarkSelects(b *testing.B, db *sqlutils.SQLRunner, tables []string) {

	var buf bytes.Buffer
	var selectQueries []string
	joins := 0

	for i := 0; i < len(tables); i++ {
		if buf.Len() == 0 {
			buf.WriteString(fmt.Sprintf("SELECT * FROM %s", tables[i]))
		} else {
			fmt.Fprintf(&buf, ", %s", tables[i])
			joins++
		}
		selectQuery := buf.String()
		selectQueries = append(selectQueries, selectQuery+" LIMIT 50")
		if joins == 2 {
			buf.Reset()
			joins = 0
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, val := range selectQueries {
			rows := db.Query(b, val)
			if err := rows.Err(); err != nil {
				b.Fatal(err)
			}
			rows.Close()
		}
	}
	b.StopTimer()
}

// BenchmarkScannedSpanStats measures the overhead of collecting statistics on planned span scans.
func BenchmarkScannedSpanStats(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)

	type clusterCreationFn func() (*sqlutils.SQLRunner, *stop.Stopper)
	type clusterSpec struct {
		name   string
		create clusterCreationFn
	}
	for _, cluster := range []clusterSpec{
		{
			name: "1node",
			create: func() (*sqlutils.SQLRunner, *stop.Stopper) {
				tc := testcluster.StartTestCluster(b, 1,
					base.TestClusterArgs{
						ReplicationMode: base.ReplicationAuto,
						ServerArgs: base.TestServerArgs{
							UseDatabase: "bench",
						},
					})
				sqlRunner := sqlutils.MakeSQLRunner(tc.Conns[0])
				return sqlRunner, tc.Stopper()
			},
		},
		{
			name: "3node",
			create: func() (*sqlutils.SQLRunner, *stop.Stopper) {
				tc := testcluster.StartTestCluster(b, 3,
					base.TestClusterArgs{
						ReplicationMode: base.ReplicationAuto,
						ServerArgs: base.TestServerArgs{
							UseDatabase: "bench",
						},
					})
				sqlRunner := sqlutils.MakeRoundRobinSQLRunner(tc.Conns[0], tc.Conns[1], tc.Conns[2])
				return sqlRunner, tc.Stopper()
			},
		},
		{
			name: "6node",
			create: func() (*sqlutils.SQLRunner, *stop.Stopper) {
				tc := testcluster.StartTestCluster(b, 6,
					base.TestClusterArgs{
						ReplicationMode: base.ReplicationAuto,
						ServerArgs: base.TestServerArgs{
							UseDatabase: "bench",
						},
					})
				sqlRunner := sqlutils.MakeRoundRobinSQLRunner(tc.Conns[0], tc.Conns[1], tc.Conns[2], tc.Conns[3], tc.Conns[4], tc.Conns[5])
				return sqlRunner, tc.Stopper()
			},
		},
	} {
		b.Run(cluster.name, func(b *testing.B) {
			ctx := context.Background()
			for _, n := range []int{10, 100, 200} {
				for _, m := range []int{100, 500} {
					b.Run(fmt.Sprintf("scanning %d tables with %d rows", n, m), func(b *testing.B) {
						type testSpec struct {
							// turn on span stat collection
							collectSpanStats bool
							// control the limit of table stats stored in the cache
							maxScannedSpanStats int
						}
						for _, test := range []testSpec{
							{maxScannedSpanStats: 50},
							{maxScannedSpanStats: 100},
							{maxScannedSpanStats: 500},
						} {
							var name strings.Builder
							name.WriteString(fmt.Sprintf("MaxMemScannedSpanStats=%s", strconv.Itoa(test.maxScannedSpanStats)))
							b.Run(name.String(), func(b *testing.B) {
								sqlRunner, stop := cluster.create()
								defer stop.Stop(ctx)

								sqlRunner.Exec(b, `CREATE DATABASE IF NOT EXISTS bench`)
								sqlRunner.Exec(b, `SET CLUSTER SETTING sql.metrics.statement_details.max_mem_scanned_span_stats = $1`, test.maxScannedSpanStats)
								var tableNames []string

								// Create n number of tables
								for i := 0; i < n; i++ {
									tableName := fmt.Sprintf("bench.t%s", strconv.Itoa(i))
									colName := fmt.Sprintf("c%s", strconv.Itoa(i))
									createQuery := fmt.Sprintf("CREATE TABLE %s (id UUID PRIMARY KEY NOT NULL DEFAULT gen_random_uuid(), %s INT)", tableName, colName)
									sqlRunner.Exec(b, createQuery)
									tableNames = append(tableNames, tableName)

									// Insert m number of rows
									var buf bytes.Buffer
									buf.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES ", tableName, colName))
									for j := 0; j < m; j++ {
										if j > 0 {
											buf.WriteString(", ")
										}
										fmt.Fprintf(&buf, "(%d)", j)
									}
									sqlRunner.Exec(b, buf.String())
								}

								b.ReportAllocs()
								runBenchmarkSelects(b, sqlRunner, tableNames)
							})
						}
					})
				}
			}
		})
	}
}
