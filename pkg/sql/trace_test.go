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
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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

	// EXPLAIN (TRACE) needs to work regardless of whether tracing is enabled. Test both cases.
	for _, enableTr := range []bool{false, true} {
		name := "TracingOff"
		if enableTr {
			name = "TracingOn"
		}
		t.Run(name, func(t *testing.T) {
			defer tracing.SetEnabled(enableTr)()

			const numNodes = 4
			cluster := serverutils.StartTestCluster(t, numNodes, base.TestClusterArgs{})
			defer cluster.Stopper().Stop(context.TODO())

			if _, err := cluster.ServerConn(0).Exec(`
				CREATE DATABASE test;
				CREATE TABLE test.foo (id INT PRIMARY KEY);
				SET DISTSQL = OFF;
			`); err != nil {
				t.Fatal(err)
			}
			// Check EXPLAIN (TRACE) from all nodes. The point is to test from a node
			// that is different than the leaseholder for the range.
			for n := 0; n < numNodes; n++ {
				rows, err := cluster.ServerConn(n).Query(`EXPLAIN (TRACE) SELECT * FROM test.foo`)
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				expParts := []string{"explain trace", "grpcTransport SendNext", "node.Batch"}
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
