// Copyright 2018 The Cockroach Authors.
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

package optbuilder

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
)

// NB: The tests in this file have been adapted from
// sql/sem/tree/name_resolution_test.go.

func newTestingScope() *scope {
	return &scope{
		cols: []scopeColumn{
			{
				name:  tree.Name("table_name"),
				table: tree.MakeTableNameWithSchema("", "crdb_internal", "tables"),
				id:    1,
			},
			{name: tree.Name("x"), table: tree.MakeTableName("db1", "foo"), id: 2},
			{name: tree.Name("x"), table: tree.MakeTableName("db2", "foo"), id: 3},
			{name: tree.Name("x"), table: tree.MakeUnqualifiedTableName("bar"), id: 4},
			{name: tree.Name("k"), table: tree.MakeTableName("db1", "kv"), id: 5},
			{name: tree.Name("v"), table: tree.MakeTableName("db1", "kv"), id: 6},
		},
	}
}

func TestResolveQualifiedStar(t *testing.T) {
	testCases := []struct {
		in    string
		tnout string
		csout string
		err   string
	}{
		{`a.*`, ``, ``, `no data source matches pattern: a.*`},
		{`foo.*`, ``, ``, `ambiguous source name: "foo"`},
		{`db1.public.foo.*`, `db1.public.foo`, `x`, ``},
		{`db1.foo.*`, `db1.public.foo`, `x`, ``},
		{`dbx.foo.*`, ``, ``, `no data source matches pattern: dbx.foo.*`},
		{`kv.*`, `db1.public.kv`, `k, v`, ``},
	}
	testingScope := newTestingScope()
	for _, tc := range testCases {
		t.Run(tc.in, func(t *testing.T) {
			tnout, csout, err := func() (string, string, error) {
				stmt, err := parser.ParseOne(fmt.Sprintf("SELECT %s", tc.in))
				if err != nil {
					return "", "", err
				}
				v := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(tree.VarName)
				c, err := v.NormalizeVarName()
				if err != nil {
					return "", "", err
				}
				acs, ok := c.(*tree.AllColumnsSelector)
				if !ok {
					return "", "", fmt.Errorf("var name %s (%T) did not resolve to AllColumnsSelector, found %T instead",
						v, v, c)
				}
				tn, res, err := acs.Resolve(context.Background(), testingScope)
				if err != nil {
					return "", "", err
				}
				s, ok := res.(*scope)
				if !ok {
					return "", "", fmt.Errorf("resolver did not return *scope, found %T instead", res)
				}
				nl := make(tree.NameList, 0, len(s.cols))
				for i := range s.cols {
					col := s.cols[i]
					if col.table == *tn && !col.hidden {
						nl = append(nl, col.name)
					}
				}
				return tn.String(), nl.String(), nil
			}()
			if !testutils.IsError(err, tc.err) {
				t.Fatalf("%s: expected %s, but found %v", tc.in, tc.err, err)
			}
			if tc.err != "" {
				return
			}

			if tc.tnout != tnout {
				t.Fatalf("%s: expected tn %s, but found %s", tc.in, tc.tnout, tnout)
			}
			if tc.csout != csout {
				t.Fatalf("%s: expected cs %s, but found %s", tc.in, tc.csout, csout)
			}
		})
	}
}

func TestResolveColumnItem(t *testing.T) {
	testCases := []struct {
		in  string
		out string
		err string
	}{
		{`a`, ``, `column name "a" not found`},
		{`x`, ``, `column reference "x" is ambiguous \(candidates: db1.public.foo.x, db2.public.foo.x, bar.x\)`},
		{`k`, `db1.public.kv.k(5)`, ``},
		{`v`, `db1.public.kv.v(6)`, ``},
		{`table_name`, `"".crdb_internal.tables.table_name(1)`, ``},

		{`blix.x`, ``, `no data source matches prefix: blix`},
		{`"".x`, ``, `invalid column name: ""\.x`},
		{`foo.x`, ``, `ambiguous source name`},
		{`kv.k`, `db1.public.kv.k(5)`, ``},
		{`bar.x`, `bar.x(4)`, ``},
		{`tables.table_name`, `"".crdb_internal.tables.table_name(1)`, ``},

		{`a.b.x`, ``, `no data source matches prefix: a\.b`},
		{`crdb_internal.tables.table_name`, `"".crdb_internal.tables.table_name(1)`, ``},
		{`public.foo.x`, ``, `ambiguous source name`},
		{`public.kv.k`, `db1.public.kv.k(5)`, ``},

		// CockroachDB extension: d.t.x -> d.public.t.x
		{`db1.foo.x`, `db1.public.foo.x(2)`, ``},
		{`db2.foo.x`, `db2.public.foo.x(3)`, ``},

		{`a.b.c.x`, ``, `no data source matches prefix: a\.b\.c`},
		{`"".crdb_internal.tables.table_name`, `"".crdb_internal.tables.table_name(1)`, ``},
		{`db1.public.foo.x`, `db1.public.foo.x(2)`, ``},
		{`db2.public.foo.x`, `db2.public.foo.x(3)`, ``},
		{`db1.public.kv.v`, `db1.public.kv.v(6)`, ``},
	}

	testingScope := newTestingScope()
	for _, tc := range testCases {
		t.Run(tc.in, func(t *testing.T) {
			out, err := func() (string, error) {
				stmt, err := parser.ParseOne(fmt.Sprintf("SELECT %s", tc.in))
				if err != nil {
					return "", err
				}
				v := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(tree.VarName)
				c, err := v.NormalizeVarName()
				if err != nil {
					return "", err
				}
				ci, ok := c.(*tree.ColumnItem)
				if !ok {
					return "", fmt.Errorf("var name %s (%T) did not resolve to ColumnItem, found %T instead",
						v, v, c)
				}
				res, err := ci.Resolve(context.Background(), testingScope)
				if err != nil {
					return "", err
				}
				col, ok := res.(*scopeColumn)
				if !ok {
					return "", fmt.Errorf("resolver did not return *scopeColumn, found %T instead", res)
				}
				return fmt.Sprintf("%s.%s(%d)", col.table.String(), col.name, col.id), nil
			}()
			if !testutils.IsError(err, tc.err) {
				t.Fatalf("%s: expected %s, but found %v", tc.in, tc.err, err)
			}
			if tc.err != "" {
				return
			}

			if tc.out != out {
				t.Fatalf("%s: expected %s, but found %s", tc.in, tc.out, out)
			}
		})
	}
}
