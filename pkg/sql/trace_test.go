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

package sql_test

import (
	"bytes"
	gosql "database/sql"
	"fmt"
	"sort"
	"testing"
	"text/tabwriter"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func rowsToStrings(rows *gosql.Rows) [][]string {
	cols, err := rows.Columns()
	if err != nil {
		panic(err)
	}
	pretty := [][]string{cols}
	results := make([]interface{}, len(cols))
	for i := range results {
		results[i] = new(interface{})
	}
	for rows.Next() {
		if err := rows.Scan(results[:]...); err != nil {
			panic(err)
		}
		cur := make([]string, len(cols))
		for i := range results {
			val := *results[i].(*interface{})
			var str string
			if val == nil {
				str = "NULL"
			} else {
				switch v := val.(type) {
				case []byte:
					str = string(v)
				default:
					str = fmt.Sprintf("%v", v)
				}
			}
			cur[i] = str
		}
		pretty = append(pretty, cur)
	}
	return pretty
}

func prettyPrint(m [][]string) string {
	var buf bytes.Buffer
	tw := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)
	for i := range m {
		for j := range m[i] {
			_, _ = tw.Write([]byte(m[i][j]))
			if j == len(m[i])-1 {
				continue
			}
			_, _ = tw.Write([]byte{'\t'})
		}
		_, _ = tw.Write([]byte{'\n'})
	}
	_ = tw.Flush()
	return buf.String()
}

func TestExplainTrace(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// SHOW TRACE FOR needs to work regardless of whether tracing is enabled. Test both cases.
	for _, enableTr := range []bool{false, true} {
		name := "TracingOff"
		if enableTr {
			name = "TracingOn"
		}
		t.Run(name, func(t *testing.T) {
			const numNodes = 4
			cluster := serverutils.StartTestCluster(t, numNodes, base.TestClusterArgs{})
			defer cluster.Stopper().Stop(context.TODO())

			if _, err := cluster.ServerConn(0).Exec(
				fmt.Sprintf(`SET CLUSTER SETTING trace.debug.enable = %t`, enableTr),
			); err != nil {
				t.Fatal(err)
			}

			if _, err := cluster.ServerConn(0).Exec(`
				CREATE DATABASE test;
				CREATE TABLE test.foo (id INT PRIMARY KEY);
				SET DISTSQL = OFF;
			`); err != nil {
				t.Fatal(err)
			}
			// Check SHOW TRACE from all nodes. The point is to test from a node
			// that is different than the leaseholder for the range.
			for n := 0; n < numNodes; n++ {
				rows, err := cluster.ServerConn(n).Query(`SHOW TRACE FOR SELECT * FROM test.foo`)
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				expParts := []string{"explain trace", "grpcTransport SendNext", "/cockroach.roachpb.Internal/Batch"}
				var parts []string

				pretty := rowsToStrings(rows)
				for _, row := range pretty[1:] {
					part := row[3] // Operation
					if ind := sort.SearchStrings(parts, part); ind == len(parts) || parts[ind] != part {
						parts = append(parts, part)
						sort.Strings(parts)
					}
				}
				sort.Strings(expParts)
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				for _, exp := range expParts {
					found := false
					for _, part := range parts {
						if part == exp {
							found = true
							break
						}
					}
					if !found {
						t.Fatalf(
							"expected at least %v, got %v\n\nResults:\n%v", expParts, parts, prettyPrint(pretty),
						)
					}
				}
			}
		})
	}
}

func TestSessionTrace(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

					// Start session tracing.
					if _, err := sqlDB.Exec("SET TRACE = ON"); err != nil {
						t.Fatal(err)
					}

					rows, err := sqlDB.Query(`SELECT * FROM test.foo`)
					if err != nil {
						t.Fatal(err)
					}
					if err := rows.Close(); err != nil {
						t.Fatal(err)
					}

					if _, err := sqlDB.Exec("SET TRACE = OFF"); err != nil {
						t.Fatal(err)
					}

					rows, err = sqlDB.Query(
						"SELECT DISTINCT(operation) op FROM crdb_internal.session_trace " +
							"WHERE operation IS NOT NULL ORDER BY op")
					if err != nil {
						t.Fatal(err)
					}
					defer rows.Close()
					expSpans := []string{
						"sql txn implicit",
						"grpcTransport SendNext",
						"/cockroach.roachpb.Internal/Batch",
					}
					sort.Strings(expSpans)
					r := 0
					for rows.Next() {
						var op string
						if err := rows.Scan(&op); err != nil {
							t.Fatal(err)
						}
						if r >= len(expSpans) {
							t.Fatalf("extra span: %s", op)
						}
						if op != expSpans[r] {
							t.Fatalf("expected span: %q, got: %q", expSpans[r], op)
						}
						r++
					}
					if r < len(expSpans) {
						t.Fatalf("missing expected spans: %s", expSpans[r:])
					}
				})
			}

		})
	}
}
