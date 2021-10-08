// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree_test

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestClassifyTablePattern(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCases := []struct {
		in, out  string
		expanded string
		err      string
	}{
		{`a`, `a`, `""."".a`, ``},
		{`a.b`, `a.b`, `"".a.b`, ``},
		{`a.b.c`, `a.b.c`, `a.b.c`, ``},
		{`a.b.c.d`, ``, ``, `at or near "\.": syntax error`},
		{`a.""`, ``, ``, `invalid table name: a\.""`},
		{`a.b.""`, ``, ``, `invalid table name: a\.b\.""`},
		{`a.b.c.""`, ``, ``, `at or near "\.": syntax error`},
		{`a."".c`, ``, ``, `invalid table name: a\.""\.c`},
		// CockroachDB extension: empty catalog name.
		{`"".b.c`, `"".b.c`, `"".b.c`, ``},

		// Check keywords: disallowed in first position, ok afterwards.
		{`user.x.y`, ``, ``, `syntax error`},
		{`"user".x.y`, `"user".x.y`, `"user".x.y`, ``},
		{`x.user.y`, `x."user".y`, `x."user".y`, ``},
		{`x.user`, `x."user"`, `"".x."user"`, ``},

		{`*`, `*`, `""."".*`, ``},
		{`a.*`, `a.*`, `"".a.*`, ``},
		{`a.b.*`, `a.b.*`, `a.b.*`, ``},
		{`a.b.c.*`, ``, ``, `at or near "\.": syntax error`},
		{`a.b.*.c`, ``, ``, `at or near "\.": syntax error`},
		{`a.*.b`, ``, ``, `at or near "\.": syntax error`},
		{`*.b`, ``, ``, `at or near "\.": syntax error`},
		{`"".*`, ``, ``, `invalid table name: "".\*`},
		{`a."".*`, ``, ``, `invalid table name: a\.""\.\*`},
		{`a.b."".*`, ``, ``, `invalid table name: a.b.""`},
		// CockroachDB extension: empty catalog name.
		{`"".b.*`, `"".b.*`, `"".b.*`, ``},

		// Check keywords: disallowed in first position, ok afterwards.
		{`user.x.*`, ``, ``, `syntax error`},
		{`"user".x.*`, `"user".x.*`, `"user".x.*`, ``},
		{`x.user.*`, `x."user".*`, `x."user".*`, ``},

		{`foo@bar`, ``, ``, `at or near "@": syntax error`},
	}

	for _, tc := range testCases {
		t.Run(tc.in, func(t *testing.T) {
			tp, err := func() (tree.TablePattern, error) {
				stmt, err := parser.ParseOne(fmt.Sprintf("GRANT SELECT ON %s TO foo", tc.in))
				if err != nil {
					return nil, err
				}
				tp, err := stmt.AST.(*tree.Grant).Targets.Tables[0].NormalizeTablePattern()
				if err != nil {
					return nil, err
				}
				return tp, nil
			}()
			if !testutils.IsError(err, tc.err) {
				t.Fatalf("%s: expected %s, but found %v", tc.in, tc.err, err)
			}
			if tc.err != "" {
				return
			}
			if out := tp.String(); tc.out != out {
				t.Fatalf("%s: expected %s, but found %s", tc.in, tc.out, out)
			}
			switch tpv := tp.(type) {
			case *tree.AllTablesSelector:
				tpv.ExplicitSchema = true
				tpv.ExplicitCatalog = true
			case *tree.TableName:
				tpv.ExplicitSchema = true
				tpv.ExplicitCatalog = true
			default:
				t.Fatalf("%s: unknown pattern type: %T", tc.in, tp)
			}

			if out := tp.String(); tc.expanded != out {
				t.Fatalf("%s: expected full %s, but found %s", tc.in, tc.expanded, out)
			}
		})
	}
}

func TestClassifyColumnName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCases := []struct {
		in, out string
		err     string
	}{
		{`a`, `a`, ``},
		{`a.b`, `a.b`, ``},
		{`a.b.c`, `a.b.c`, ``},
		{`a.b.c.d`, `a.b.c.d`, ``},
		{`a.b.c.d.e`, ``, `at or near "\.": syntax error`},
		{`""`, ``, `invalid column name: ""`},
		{`a.""`, ``, `invalid column name: a\.""`},
		{`a.b.""`, ``, `invalid column name: a\.b\.""`},
		{`a.b.c.""`, ``, `invalid column name: a\.b\.c\.""`},
		{`a.b.c.d.""`, ``, `at or near "\.": syntax error`},
		{`"".a`, ``, `invalid column name: ""\.a`},
		{`"".a.b`, ``, `invalid column name: ""\.a\.b`},
		// CockroachDB extension: empty catalog name.
		{`"".a.b.c`, `"".a.b.c`, ``},

		{`a.b."".d`, ``, `invalid column name: a\.b\.""\.d`},
		{`a."".c.d`, ``, `invalid column name: a\.""\.c\.d`},

		// Check keywords: disallowed in first position, ok afterwards.
		{`user.x.y`, ``, `syntax error`},
		{`"user".x.y`, `"user".x.y`, ``},
		{`x.user.y`, `x.user.y`, ``},
		{`x.user`, `x."user"`, ``},

		{`*`, `*`, ``},
		{`a.*`, `a.*`, ``},
		{`a.b.*`, `a.b.*`, ``},
		{`a.b.c.*`, `a.b.c.*`, ``},
		{`a.b.c.d.*`, ``, `at or near "\.": syntax error`},
		{`a.b.*.c`, ``, `at or near "\.": syntax error`},
		{`a.*.b`, ``, `at or near "\.": syntax error`},
		{`*.b`, ``, `at or near "\.": syntax error`},
		{`"".*`, ``, `invalid column name: "".\*`},
		{`a."".*`, ``, `invalid column name: a\.""\.\*`},
		{`a.b."".*`, ``, `invalid column name: a\.b\.""\.\*`},
		{`a.b.c."".*`, ``, `at or near "\.": syntax error`},

		{`"".a.*`, ``, `invalid column name: ""\.a.*`},
		// CockroachDB extension: empty catalog name.
		{`"".a.b.*`, `"".a.b.*`, ``},

		{`a."".c.*`, ``, `invalid column name: a\.""\.c\.*`},

		// Check keywords: disallowed in first position, ok afterwards.
		{`user.x.*`, ``, `syntax error`},
		{`"user".x.*`, `"user".x.*`, ``},
		{`x.user.*`, `x.user.*`, ``},

		{`foo@bar`, ``, `at or near "@": syntax error`},
	}

	for _, tc := range testCases {
		t.Run(tc.in, func(t *testing.T) {
			v, err := func() (tree.VarName, error) {
				stmt, err := parser.ParseOne(fmt.Sprintf("SELECT %s", tc.in))
				if err != nil {
					return nil, err
				}
				v := stmt.AST.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(tree.VarName)
				return v.NormalizeVarName()
			}()
			if !testutils.IsError(err, tc.err) {
				t.Fatalf("%s: expected %s, but found %v", tc.in, tc.err, err)
			}
			if tc.err != "" {
				return
			}
			if out := v.String(); tc.out != out {
				t.Fatalf("%s: expected %s, but found %s", tc.in, tc.out, out)
			}
			switch v.(type) {
			case *tree.AllColumnsSelector:
			case tree.UnqualifiedStar:
			case *tree.ColumnItem:
			default:
				t.Fatalf("%s: unknown var type: %T", tc.in, v)
			}
		})
	}
}
