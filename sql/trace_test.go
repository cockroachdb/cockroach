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
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func rowsToStrings(rows *sql.Rows) [][]string {
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
	defer config.TestingDisableTableSplits()()

	s, sqlDB, _ := setup(t)
	defer cleanup(s, sqlDB)

	if _, err := sqlDB.Exec(`CREATE DATABASE test; CREATE TABLE test.foo (id INT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	rows, err := sqlDB.Query(`EXPLAIN (TRACE) INSERT INTO test.foo VALUES (1)`)
	if err != nil {
		t.Fatal(err)
	}
	expParts := []string{"coordinator", "node 1"}
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
	if !reflect.DeepEqual(expParts, parts) {
		t.Fatalf("expected %v, got %v\n\nResults:\n%v", expParts, parts, prettyPrint(pretty))
	}
}
