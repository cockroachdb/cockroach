// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)
// Author: Andrei Matei (andrei@cockroachlabs.com)

package sql_test

import (
	gosql "database/sql"
	"fmt"
	"sort"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestSessionTrace(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s := log.Scope(t)
	defer s.Close(t)

	testData := []struct {
		name     string
		getRows  func(t *testing.T, sqlDB *gosql.DB) (*gosql.Rows, error)
		expSpans []string
	}{
		{"SessionTrace", func(t *testing.T, sqlDB *gosql.DB) (*gosql.Rows, error) {
			// Start session tracing.
			if _, err := sqlDB.Exec("SET TRACE = ON"); err != nil {
				t.Fatal(err)
			}

			// Run some query
			rows, err := sqlDB.Query(`SELECT * FROM test.foo`)
			if err != nil {
				t.Fatal(err)
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}

			// Stop tracing and extract the trace
			if _, err := sqlDB.Exec("SET TRACE = OFF"); err != nil {
				t.Fatal(err)
			}

			return sqlDB.Query(
				"SELECT DISTINCT(operation) op FROM crdb_internal.session_trace " +
					"WHERE operation IS NOT NULL ORDER BY op")
		},
			[]string{
				"sql txn implicit",
				"grpcTransport SendNext",
				"/cockroach.roachpb.Internal/Batch",
			},
		},
		{"ShowTraceFor", func(_ *testing.T, sqlDB *gosql.DB) (*gosql.Rows, error) {
			return sqlDB.Query(
				"SELECT DISTINCT(operation) op FROM [SHOW TRACE FOR SELECT * FROM test.foo] " +
					"WHERE operation IS NOT NULL ORDER BY op")
		},
			[]string{
				"sql txn implicit",
				"starting plan",
				"consuming rows",
				"grpcTransport SendNext",
				"/cockroach.roachpb.Internal/Batch",
			},
		},
	}

	for _, test := range testData {
		sort.Strings(test.expSpans)

		t.Run(test.name, func(t *testing.T) {
			// Session tracing needs to work regardless of whether tracing is enabled, so
			// we're goint to test both cases.
			//
			// We'll also check traces from all nodes. The point is to be sure that we
			// test a node that is different than the leaseholder for the range, so that
			// the trace contains remote spans.
			for _, enableTr := range []bool{false, true} {
				name := "TracingOff"
				if enableTr {
					name = "TracingOn"
				}
				t.Run(name, func(t *testing.T) {
					// Create a cluster. We'll run sub-tests using each node of this cluster.
					const numNodes = 3
					cluster := serverutils.StartTestCluster(t, numNodes, base.TestClusterArgs{})
					defer cluster.Stopper().Stop(context.TODO())

					clusterDB := cluster.ServerConn(0)
					if _, err := clusterDB.Exec(`
				CREATE DATABASE test;
				CREATE TABLE test.foo (id INT PRIMARY KEY);
			`); err != nil {
						t.Fatal(err)
					}
					for i := 0; i < numNodes; i++ {
						t.Run(fmt.Sprintf("node-%d", i), func(t *testing.T) {
							sqlDB := cluster.ServerConn(i)
							sqlDB.SetMaxOpenConns(1)

							// Run a non-traced read to acquire a lease on the table, so that the
							// traced read below doesn't need to take a lease. Tracing a lease
							// acquisition incurs some spans that are too fragile to test here.
							if _, err := sqlDB.Exec(`SELECT * FROM test.foo LIMIT 1`); err != nil {
								t.Fatal(err)
							}

							if _, err := cluster.ServerConn(0).Exec(
								fmt.Sprintf(`SET CLUSTER SETTING trace.debug.enable = %t`, enableTr),
							); err != nil {
								t.Fatal(err)
							}

							// DistSQL doesn't support snowball tracing at the moment.
							if _, err := sqlDB.Exec("SET DISTSQL = OFF"); err != nil {
								t.Fatal(err)
							}

							// Sanity check that new sessions don't have trace info on them.
							if !enableTr {
								row := sqlDB.QueryRow("SELECT COUNT(1) FROM crdb_internal.session_trace")
								var count int
								if err := row.Scan(&count); err != nil {
									t.Fatal(err)
								}
								if count != 0 {
									t.Fatalf("expected crdb_internal.session_trace to be empty "+
										"at the beginning of a session, but it wasn't. Count: %d.", count)
								}
							}

							rows, err := test.getRows(t, sqlDB)
							if err != nil {
								t.Fatal(err)
							}
							defer rows.Close()

							r := 0
							for rows.Next() {
								var op string
								if err := rows.Scan(&op); err != nil {
									t.Fatal(err)
								}
								if r >= len(test.expSpans) {
									t.Fatalf("extra span: %s", op)
								}
								if op != test.expSpans[r] {
									t.Fatalf("expected span: %q, got: %q", test.expSpans[r], op)
								}
								r++
							}
							if r < len(test.expSpans) {
								t.Fatalf("missing expected spans: %s", test.expSpans[r:])
							}
						})
					}

				})
			}
		})
	}
}
