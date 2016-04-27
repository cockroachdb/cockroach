// Copyright 2015 The Cockroach Authors.
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
// Author: Marc Berhault (marc@cockroachlabs.com)

package parser

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/testutils"
)

// TestQualifiedNameString tests the string representation of QualifiedName.
func TestQualifiedNameString(t *testing.T) {
	testCases := []struct {
		in, out string
	}{
		{"*", `"*"`},
		// Non-reserved keyword.
		{"DATABASE", `DATABASE`},
		{"dAtAbAse", `dAtAbAse`},
		// Reserved keyword.
		{"SELECT", `"SELECT"`},
		{"sElEcT", `"sElEcT"`},
		// Ident format: starts with [a-zA-Z_] or extended ascii,
		// and is then followed by [a-zA-Z0-9$_] or extended ascii.
		{"foo$09", "foo$09"},
		{"_Ab10", "_Ab10"},
		// Everything else quotes the string and escapes double quotes.
		{".foobar", `".foobar"`},
		{`".foobar"`, `""".foobar"""`},
		{`\".foobar\"`, `"\"".foobar\"""`},
	}

	for _, tc := range testCases {
		q := &QualifiedName{Base: Name(tc.in)}
		if q.String() != tc.out {
			t.Errorf("expected q.String() == %q, got %q", tc.out, q.String())
		}
	}
}

func TestNormalizeTableName(t *testing.T) {
	testCases := []struct {
		in, out string
		db      string
		err     string
	}{
		{`foo`, `test.foo`, `test`, ``},
		{`test.foo`, `test.foo`, ``, ``},
		{`bar.foo`, `bar.foo`, `test`, ``},
		{`foo@bar`, `test.foo@bar`, `test`, ``},
		{`foo@{FORCE_INDEX=bar}`, `test.foo@bar`, `test`, ``},
		{`foo@{NO_INDEX_JOIN}`, `test.foo@{NO_INDEX_JOIN}`, `test`, ``},
		{`foo@{FORCE_INDEX=bar,NO_INDEX_JOIN}`, `test.foo@{FORCE_INDEX=bar,NO_INDEX_JOIN}`,
			`test`, ``},
		{`test.foo@bar`, `test.foo@bar`, ``, ``},

		{`""`, ``, ``, `empty table name`},
		{`foo`, ``, ``, `no database specified`},
		{`foo@bar`, ``, ``, `no database specified`},
		{`test.foo.bar`, ``, ``, `invalid table name: test.foo.bar`},
		{`test.foo[bar]`, ``, ``, `invalid table name: test.foo\[bar\]`},
		{`test.foo.bar[blah]`, ``, ``, `invalid table name: test.foo.bar\[blah\]`},
	}

	for _, tc := range testCases {
		stmt, err := ParseOneTraditional(fmt.Sprintf("SELECT * FROM %s", tc.in))
		if err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		ate := stmt.(*Select).Select.(*SelectClause).From[0].(*AliasedTableExpr)
		err = ate.Expr.(*QualifiedName).NormalizeTableName(tc.db)
		if tc.err != "" {
			if !testutils.IsError(err, tc.err) {
				t.Fatalf("%s: expected %s, but found %s", tc.in, tc.err, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: expected success, but found %v", tc.in, err)
		}
		ate.Expr.(*QualifiedName).ClearString()
		if out := ate.String(); tc.out != out {
			t.Errorf("%s: expected %s, but found %s", tc.in, tc.out, out)
		}
	}
}

func TestNormalizeColumnName(t *testing.T) {
	testCases := []struct {
		in, out string
		err     string
	}{
		{`foo`, `"".foo`, ``},
		{`"".foo`, `"".foo`, ``},
		{`*`, `"".*`, ``},
		{`"".*`, `"".*`, ``},
		{`foo.bar`, `foo.bar`, ``},
		{`foo.*`, `foo.*`, ``},
		{`foo.bar[blah]`, `foo.bar[blah]`, ``},
		{`foo[bar]`, `"".foo[bar]`, ``},

		{`""`, ``, `empty column name`},
		{`test.foo.bar`, ``, `invalid column name: test.foo.bar`},
		{`test.foo.*`, ``, `invalid column name: test.foo.*`},
	}

	for _, tc := range testCases {
		q, err := ParseExprTraditional(tc.in)
		if err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		err = q.(*QualifiedName).NormalizeColumnName()
		if tc.err != "" {
			if !testutils.IsError(err, tc.err) {
				t.Fatalf("%s: expected %s, but found %s", tc.in, tc.err, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: expected success, but found %v", tc.in, err)
		}
		q.(*QualifiedName).ClearString()
		if out := q.String(); tc.out != out {
			t.Errorf("%s: expected %s, but found %s", tc.in, tc.out, out)
		}
	}
}

// TestExprString verifies that converting an expression to a string and back
// doesn't change the (normalized) expression.
func TestExprString(t *testing.T) {
	testExprs := []string{
		`a AND b`,
		`a AND b OR c`,
		`(a AND b) OR c`,
		`a AND (b OR c)`,
		`a AND NOT ((b OR c) AND (d AND e))`,
		`~-a`,
		`~-a`,
		`-2*(a+3)*b-2/c`,
		`a&b<<(b+c)&d AND (b&d)+c>>(d&a)`,
		`a&(b<<b+c)&d AND b&(d+c>>d)&a`,
		`a = b|c`,
		`a != b|c`,
		`NOT a AND b`,
		`NOT (a AND b)`,
		`(NOT a) AND b`,
		`NOT (a = NOT b = c)`,
		`NOT NOT a = b`,
		`NOT NOT (a = b)`,
		`NOT (NOT a) < b`,
		`NOT (NOT a = b)`,
		`(NOT NOT a) >= b`,
		`(a OR (b BETWEEN (c+d) AND (e+f))) AND b`,
		`(1 >= 2) IS OF (BOOL)`,
		`(1 >= 2) = (2 IS OF (BOOL))`,
	}
	for _, exprStr := range testExprs {
		expr, err := ParseExprTraditional(exprStr)
		if err != nil {
			t.Fatalf("%s: %v", exprStr, err)
		}
		if expr, err = TypeConstants(expr); err != nil {
			t.Fatalf("%s: %v", expr, err)
		}
		// str may differ than exprStr (we may be adding some parens).
		str := expr.String()
		expr2, err := ParseExprTraditional(str)
		if err != nil {
			t.Fatalf("%s: %v", exprStr, err)
		}
		if expr2, err = TypeConstants(expr2); err != nil {
			t.Fatalf("%s: %v", expr2, err)
		}
		// Verify that when we stringify the expression again, the string is the
		// same. This is important because we don't want cycles of parsing and
		// printing an expression to keep adding parens.
		if str2 := expr2.String(); str != str2 {
			t.Errorf("Print/parse/print cycle changes the string: `%s` vs `%s`", str, str2)
		}
		// Compare the normalized expressions.
		normalized, err := defaultContext.NormalizeExpr(expr.(TypedExpr))
		if err != nil {
			t.Fatalf("%s: %v", exprStr, err)
		}
		normalized2, err := defaultContext.NormalizeExpr(expr2.(TypedExpr))
		if err != nil {
			t.Fatalf("%s: %v", exprStr, err)
		}
		if !reflect.DeepEqual(normalized, normalized2) {
			t.Errorf("normalized expressions differ\n"+
				"original:     %s\n"+
				"intermediate: %s\n"+
				"before: %#v\n"+
				"after:  %#v", exprStr, str, normalized, normalized2)
		}
	}
}
