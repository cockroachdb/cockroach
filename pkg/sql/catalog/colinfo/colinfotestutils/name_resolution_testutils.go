// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colinfotestutils

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
)

// ColumnItemResolverTester is an interface that should be implemented by any
// struct that also implements tree.ColumnItemResolver. It is used to test that
// the implementation of tree.ColumnItemResolver is correct.
type ColumnItemResolverTester interface {
	// GetColumnItemResolver returns the tree.ColumnItemResolver. Since any
	// struct implementing ColumnItemResolverTester should also implement
	// tree.ColumnItemResolver, this is basically an identity function.
	GetColumnItemResolver() colinfo.ColumnItemResolver

	// AddTable adds a table with the given column names to the
	// tree.ColumnItemResolver.
	AddTable(tabName tree.TableName, colNames []tree.Name)

	// ResolveQualifiedStarTestResults returns the results of running
	// RunResolveQualifiedStarTest on the tree.ColumnItemResolver.
	ResolveQualifiedStarTestResults(
		srcName *tree.TableName, srcMeta colinfo.ColumnSourceMeta,
	) (string, string, error)

	// ResolveColumnItemTestResults returns the results of running
	// RunResolveColumnItemTest on the tree.ColumnItemResolver.
	ResolveColumnItemTestResults(colRes colinfo.ColumnResolutionResult) (string, error)
}

func initColumnItemResolverTester(t *testing.T, ct ColumnItemResolverTester) {
	ct.AddTable(tree.MakeTableNameWithSchema("", "crdb_internal", "tables"), []tree.Name{"table_name"})
	ct.AddTable(tree.MakeTableNameWithSchema("db1", tree.PublicSchemaName, "foo"), []tree.Name{"x"})
	ct.AddTable(tree.MakeTableNameWithSchema("db2", tree.PublicSchemaName, "foo"), []tree.Name{"x"})
	ct.AddTable(tree.MakeUnqualifiedTableName("bar"), []tree.Name{"x"})
	ct.AddTable(tree.MakeTableNameWithSchema("db1", tree.PublicSchemaName, "kv"), []tree.Name{"k", "v"})
}

// RunResolveQualifiedStarTest tests that the given ColumnItemResolverTester
// correctly resolves names of the form "<tableName>.*".
func RunResolveQualifiedStarTest(t *testing.T, ct ColumnItemResolverTester) {
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

	initColumnItemResolverTester(t, ct)
	resolver := ct.GetColumnItemResolver()
	for _, tc := range testCases {
		t.Run(tc.in, func(t *testing.T) {
			tnout, csout, err := func() (string, string, error) {
				stmt, err := parser.ParseOne(fmt.Sprintf("SELECT %s", tc.in))
				if err != nil {
					return "", "", err
				}
				v := stmt.AST.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(tree.VarName)
				c, err := v.NormalizeVarName()
				if err != nil {
					return "", "", err
				}
				acs, ok := c.(*tree.AllColumnsSelector)
				if !ok {
					return "", "", fmt.Errorf("var name %s (%T) did not resolve to AllColumnsSelector, found %T instead",
						v, v, c)
				}
				tn, res, err := colinfo.ResolveAllColumnsSelector(context.Background(), resolver, acs)
				if err != nil {
					return "", "", err
				}
				return ct.ResolveQualifiedStarTestResults(tn, res)
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

// RunResolveColumnItemTest tests that the given ColumnItemResolverTester
// correctly resolves column names.
func RunResolveColumnItemTest(t *testing.T, ct ColumnItemResolverTester) {
	testCases := []struct {
		in  string
		out string
		err string
	}{
		{`a`, ``, `column "a" does not exist`},
		{`x`, ``, `column reference "x" is ambiguous \(candidates: db1.public.foo.x, db2.public.foo.x, bar.x\)`},
		{`k`, `db1.public.kv.k`, ``},
		{`v`, `db1.public.kv.v`, ``},
		{`table_name`, `"".crdb_internal.tables.table_name`, ``},

		{`blix.x`, ``, `no data source matches prefix: blix`},
		{`"".x`, ``, `invalid column name: ""\.x`},
		{`foo.x`, ``, `ambiguous source name`},
		{`kv.k`, `db1.public.kv.k`, ``},
		{`bar.x`, `bar.x`, ``},
		{`tables.table_name`, `"".crdb_internal.tables.table_name`, ``},

		{`a.b.x`, ``, `no data source matches prefix: a\.b`},
		{`crdb_internal.tables.table_name`, `"".crdb_internal.tables.table_name`, ``},
		{`public.foo.x`, ``, `ambiguous source name`},
		{`public.kv.k`, `db1.public.kv.k`, ``},

		// CockroachDB extension: d.t.x -> d.public.t.x
		{`db1.foo.x`, `db1.public.foo.x`, ``},
		{`db2.foo.x`, `db2.public.foo.x`, ``},

		{`a.b.c.x`, ``, `no data source matches prefix: a\.b\.c`},
		{`"".crdb_internal.tables.table_name`, `"".crdb_internal.tables.table_name`, ``},
		{`db1.public.foo.x`, `db1.public.foo.x`, ``},
		{`db2.public.foo.x`, `db2.public.foo.x`, ``},
		{`db1.public.kv.v`, `db1.public.kv.v`, ``},
	}

	initColumnItemResolverTester(t, ct)
	resolver := ct.GetColumnItemResolver()
	for _, tc := range testCases {
		t.Run(tc.in, func(t *testing.T) {
			out, err := func() (string, error) {
				stmt, err := parser.ParseOne(fmt.Sprintf("SELECT %s", tc.in))
				if err != nil {
					return "", err
				}
				v := stmt.AST.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(tree.VarName)
				c, err := v.NormalizeVarName()
				if err != nil {
					return "", err
				}
				ci, ok := c.(*tree.ColumnItem)
				if !ok {
					return "", fmt.Errorf("var name %s (%T) did not resolve to ColumnItem, found %T instead",
						v, v, c)
				}
				res, err := colinfo.ResolveColumnItem(context.Background(), resolver, ci)
				if err != nil {
					return "", err
				}
				return ct.ResolveColumnItemTestResults(res)
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
