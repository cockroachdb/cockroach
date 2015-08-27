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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"testing"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func parseAndNormalizeExpr(t *testing.T, sql string) parser.Expr {
	q, err := parser.ParseTraditional("SELECT " + sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	expr := q[0].(*parser.Select).Exprs[0].Expr
	r, err := parser.NormalizeExpr(expr)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}

	// Perform qualified name resolution because {analyze,simplify}Expr want
	// expressions containing qvalues.
	s := &scanNode{}
	s.desc = &TableDescriptor{
		Columns: []ColumnDescriptor{
			{Name: "a", ID: 1},
			{Name: "b", ID: 2},
			{Name: "c", ID: 3},
			{Name: "d", ID: 4},
			{Name: "e", ID: 5},
		},
	}
	s.visibleCols = s.desc.Columns

	r, err = s.resolveQNames(r)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return r
}

func TestSplitOrExpr(t *testing.T) {
	defer leaktest.AfterTest(t)

	testData := []struct {
		expr     string
		expected string
	}{
		{`a`, `a`},
		{`a AND b`, `a AND b`},
		{`a OR b`, `a, b`},
		{`(a OR b) OR (c OR (d OR e))`, `a, b, c, d, e`},
	}
	for _, d := range testData {
		expr := parseAndNormalizeExpr(t, d.expr)
		exprs := splitOrExpr(expr, nil)
		if s := exprs.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
	}
}

func TestSplitAndExpr(t *testing.T) {
	defer leaktest.AfterTest(t)

	testData := []struct {
		expr     string
		expected string
	}{
		{`a`, `a`},
		{`a AND b`, `a, b`},
		{`a OR b`, `a OR b`},
		{`(a AND b) AND (c AND (d AND e))`, `a, b, c, d, e`},
	}
	for _, d := range testData {
		expr := parseAndNormalizeExpr(t, d.expr)
		exprs := splitAndExpr(expr, nil)
		if s := exprs.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
	}
}

func TestSimplifyExpr(t *testing.T) {
	defer leaktest.AfterTest(t)

	testData := []struct {
		expr     string
		expected string
	}{
		{`a`, `a`},
		{`a AND b`, `a AND b`},
		{`a OR b`, `a OR b`},
		{`(a AND b) AND (c AND d)`, `a AND b AND c AND d`},
		{`(a OR b) OR (c OR d)`, `a OR b OR c OR d`},
		{`a < lower('FOO')`, `a < 'foo'`},
		{`a < 1 AND a < 2 AND a < 3 AND a < 4 AND a < 5`, `a < 1`},
		{`a < 1 OR a < 2 OR a < 3 OR a < 4 OR a < 5`, `a < 5`},
		{`(a < 1 OR a > 1) AND a >= 1`, `a > 1`},
		{`a < 1 AND (a > 2 AND a < 1)`, `false`},
		{`a < 1 OR (a > 1 OR a < 2)`, `true`},
		{`a < 1 AND abs(a) > 0`, `a < 1`},
		{`a < 1 OR abs(a) > 0`, `true`},

		{`a LIKE '%foo'`, `true`},
		{`a LIKE 'foo'`, `a = 'foo'`},
		{`a LIKE 'foo%'`, `a >= 'foo' AND a < 'fop'`},
		{`a LIKE 'foo_'`, `a >= 'foo' AND a < 'fop'`},
		{`a LIKE 'bar_foo%'`, `a >= 'bar' AND a < 'bas'`},
		{`a SIMILAR TO '.*'`, `true`},
		{`a SIMILAR TO 'foo'`, `a = 'foo'`},
		{`a SIMILAR TO 'foo.*'`, `a >= 'foo' AND a < 'fop'`},
		{`a SIMILAR TO '(foo|foobar).*'`, `a >= 'foo' AND a < 'fop'`},
	}
	for _, d := range testData {
		expr := parseAndNormalizeExpr(t, d.expr)
		expr = simplifyExpr(expr)
		if s := expr.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
	}
}

func TestSimplifyNotExpr(t *testing.T) {
	defer leaktest.AfterTest(t)

	testData := []struct {
		expr     string
		expected string
	}{
		{`NOT a = 1`, `a != 1`},
		{`NOT a != 1`, `a = 1`},
		{`NOT a > 1`, `a < 1`},
		{`NOT a >= 1`, `a <= 1`},
		{`NOT a < 1`, `a > 1`},
		{`NOT a <= 1`, `a >= 1`},
		{`NOT a LIKE 'foo'`, `true`},
		{`NOT a NOT LIKE 'foo'`, `a = 'foo'`},
		{`NOT a SIMILAR TO 'foo'`, `true`},
		{`NOT a NOT SIMILAR TO 'foo'`, `a = 'foo'`},
		{`NOT (a != 1 AND b != 1)`, `a = 1 OR b = 1`},
		{`NOT (a != 1 OR a <= 1)`, `a = 1`},
	}
	for _, d := range testData {
		expr := parseAndNormalizeExpr(t, d.expr)
		expr = simplifyExpr(expr)
		if s := expr.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
	}
}

func TestSimplifyAndExpr(t *testing.T) {
	defer leaktest.AfterTest(t)

	testData := []struct {
		expr     string
		expected string
	}{
		{`a < 1 AND b < 1 AND a < 2 AND b < 2`, `a < 1 AND b < 1`},
		{`(a > 1 AND a < 2) AND (a > 0 AND a < 3)`, `a > 1 AND a < 2`},

		{`a = 1 AND a = true`, `false`},
		{`a = 1 AND a = 1.1`, `false`},
		{`a = 1 AND a = '1'`, `false`},
		{`a = 1 AND a = (1, 2)`, `false`},
		{`a = 1 AND a = NULL`, `false`},
		{`a = 1 AND a != NULL`, `a = 1 AND a != NULL`},
		{`a = 1 AND b = 1`, `a = 1 AND b = 1`},

		{`a = 1 AND a = 1`, `a = 1`},
		{`a = 1 AND a = 2`, `false`},
		{`a = 2 AND a = 1`, `false`},
		{`a = 1 AND a != 1`, `false`},
		{`a = 1 AND a != 2`, `a = 1`},
		{`a = 2 AND a != 1`, `a = 2`},
		{`a = 1 AND a > 1`, `false`},
		{`a = 1 AND a > 2`, `false`},
		{`a = 2 AND a > 1`, `a = 2`},
		{`a = 1 AND a >= 1`, `a = 1`},
		{`a = 1 AND a >= 2`, `false`},
		{`a = 2 AND a >= 1`, `a = 2`},
		{`a = 1 AND a < 1`, `false`},
		{`a = 1 AND a < 2`, `a = 1`},
		{`a = 2 AND a < 1`, `false`},
		{`a = 1 AND a <= 1`, `a = 1`},
		{`a = 1 AND a <= 2`, `a = 1`},
		{`a = 2 AND a <= 1`, `false`},

		{`a != 1 AND a = 1`, `false`},
		{`a != 1 AND a = 2`, `a = 2`},
		{`a != 2 AND a = 1`, `a = 1`},
		{`a != 1 AND a != 1`, `a != 1`},
		{`a != 1 AND a != 2`, `a != 1 AND a != 2`},
		{`a != 2 AND a != 1`, `a != 2 AND a != 1`},
		{`a != 1 AND a > 1`, `a > 1`},
		{`a != 1 AND a > 2`, `a > 2`},
		{`a != 2 AND a > 1`, `a > 1`},
		{`a != 1 AND a >= 1`, `a > 1`},
		{`a != 1 AND a >= 2`, `a >= 2`},
		{`a != 2 AND a >= 1`, `a >= 1`},
		{`a != 1 AND a < 1`, `a < 1`},
		{`a != 1 AND a < 2`, `a < 2`},
		{`a != 2 AND a < 1`, `a < 1`},
		{`a != 1 AND a <= 1`, `a < 1`},
		{`a != 1 AND a <= 2`, `a <= 2`},
		{`a != 2 AND a <= 1`, `a <= 1`},

		{`a > 1 AND a = 1`, `false`},
		{`a > 1 AND a = 2`, `a = 2`},
		{`a > 2 AND a = 1`, `false`},
		{`a > 1 AND a != 1`, `a > 1`},
		{`a > 1 AND a != 2`, `a > 1`},
		{`a > 2 AND a != 1`, `a > 2`},
		{`a > 1 AND a > 1`, `a > 1`},
		{`a > 1 AND a > 2`, `a > 2`},
		{`a > 2 AND a > 1`, `a > 2`},
		{`a > 1 AND a >= 1`, `a > 1`},
		{`a > 1 AND a >= 2`, `a >= 2`},
		{`a > 2 AND a >= 1`, `a > 2`},
		{`a > 1 AND a < 1`, `false`},
		{`a > 1 AND a < 2`, `a > 1 AND a < 2`},
		{`a > 2 AND a < 1`, `false`},
		{`a > 1 AND a <= 1`, `false`},
		{`a > 1 AND a <= 2`, `a > 1 AND a <= 2`},
		{`a > 2 AND a <= 1`, `false`},

		{`a >= 1 AND a = 1`, `a = 1`},
		{`a >= 1 AND a = 2`, `a = 2`},
		{`a >= 2 AND a = 1`, `false`},
		{`a >= 1 AND a != 1`, `a > 1`},
		{`a >= 1 AND a != 2`, `a >= 1`},
		{`a >= 2 AND a != 1`, `a >= 2`},
		{`a >= 1 AND a > 1`, `a > 1`},
		{`a >= 1 AND a > 2`, `a > 2`},
		{`a >= 2 AND a > 1`, `a >= 2`},
		{`a >= 1 AND a >= 1`, `a >= 1`},
		{`a >= 1 AND a >= 2`, `a >= 2`},
		{`a >= 2 AND a >= 1`, `a >= 2`},
		{`a >= 1 AND a < 1`, `false`},
		{`a >= 1 AND a < 2`, `a >= 1 AND a < 2`},
		{`a >= 2 AND a < 1`, `false`},
		{`a >= 1 AND a <= 1`, `a = 1`},
		{`a >= 1 AND a <= 2`, `a >= 1 AND a <= 2`},
		{`a >= 2 AND a <= 1`, `false`},

		{`a < 1 AND a = 1`, `false`},
		{`a < 1 AND a = 2`, `false`},
		{`a < 2 AND a = 1`, `a = 1`},
		{`a < 1 AND a != 1`, `a < 1`},
		{`a < 1 AND a != 2`, `a < 1`},
		{`a < 2 AND a != 1`, `a < 2`},
		{`a < 1 AND a > 1`, `false`},
		{`a < 1 AND a > 2`, `false`},
		{`a < 2 AND a > 1`, `a < 2 AND a > 1`},
		{`a < 1 AND a >= 1`, `false`},
		{`a < 1 AND a >= 2`, `false`},
		{`a < 2 AND a >= 1`, `a < 2 AND a >= 1`},
		{`a < 1 AND a < 1`, `a < 1`},
		{`a < 1 AND a < 2`, `a < 1`},
		{`a < 2 AND a < 1`, `a < 1`},
		{`a < 1 AND a <= 1`, `a < 1`},
		{`a < 1 AND a <= 2`, `a < 1`},
		{`a < 2 AND a <= 1`, `a <= 1`},

		{`a <= 1 AND a = 1`, `a = 1`},
		{`a <= 1 AND a = 2`, `false`},
		{`a <= 2 AND a = 1`, `a = 1`},
		{`a <= 1 AND a != 1`, `a < 1`},
		{`a <= 1 AND a != 2`, `a <= 1`},
		{`a <= 2 AND a != 1`, `a <= 2`},
		{`a <= 1 AND a > 1`, `false`},
		{`a <= 1 AND a > 2`, `false`},
		{`a <= 2 AND a > 1`, `a <= 2 AND a > 1`},
		{`a <= 1 AND a >= 1`, `a = 1`},
		{`a <= 1 AND a >= 2`, `false`},
		{`a <= 2 AND a >= 1`, `a <= 2 AND a >= 1`},
		{`a <= 1 AND a < 1`, `a < 1`},
		{`a <= 1 AND a < 2`, `a <= 1`},
		{`a <= 2 AND a < 1`, `a < 1`},
		{`a <= 1 AND a <= 1`, `a <= 1`},
		{`a <= 1 AND a <= 2`, `a <= 1`},
		{`a <= 2 AND a <= 1`, `a <= 1`},
	}
	for _, d := range testData {
		expr1 := parseAndNormalizeExpr(t, d.expr)
		expr2 := simplifyExpr(expr1)
		if s := expr2.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
		if _, ok := expr2.(*parser.AndExpr); !ok {
			// The result was not an AND expression. Verify that the analysis is
			// commutative.
			andExpr := expr1.(*parser.AndExpr)
			andExpr.Left, andExpr.Right = andExpr.Right, andExpr.Left
			expr3 := simplifyExpr(andExpr)
			if s := expr3.String(); d.expected != s {
				t.Errorf("%s: expected %s, but found %s", expr1, d.expected, s)
			}
		}
	}
}

func TestSimplifyOrExpr(t *testing.T) {
	defer leaktest.AfterTest(t)

	testData := []struct {
		expr     string
		expected string
	}{
		{`a < 1 OR b < 1 OR a < 2 OR b < 2`, `a < 2 OR b < 2`},
		{`(a > 2 OR a < 1) OR (a > 3 OR a < 0)`, `a > 2 OR a < 1`},

		{`a = 1 OR a = true`, `a = 1 OR a = true`},
		{`a = 1 OR a = 1.1`, `a = 1 OR a = 1.1`},
		{`a = 1 OR a = '1'`, `a = 1 OR a = '1'`},
		{`a = 1 OR a = (1, 2)`, `a = 1 OR a = (1, 2)`},
		{`a = 1 OR a = NULL`, `a = 1 OR a = NULL`},
		{`a = 1 OR a != NULL`, `a = 1 OR a != NULL`},
		{`a = 1 OR b = 1`, `a = 1 OR b = 1`},

		{`a = 1 OR a = 1`, `a = 1`},
		{`a = 1 OR a = 2`, `a = 1 OR a = 2`},
		{`a = 2 OR a = 1`, `a = 2 OR a = 1`},
		{`a = 1 OR a != 1`, `true`},
		{`a = 1 OR a != 2`, `a != 2`},
		{`a = 2 OR a != 1`, `a != 1`},
		{`a = 1 OR a > 1`, `a >= 1`},
		{`a = 1 OR a > 2`, `a = 1 OR a > 2`},
		{`a = 2 OR a > 1`, `a > 1`},
		{`a = 1 OR a >= 1`, `a >= 1`},
		{`a = 1 OR a >= 2`, `a = 1 OR a >= 2`},
		{`a = 2 OR a >= 1`, `a >= 1`},
		{`a = 1 OR a < 1`, `a <= 1`},
		{`a = 1 OR a < 2`, `a < 2`},
		{`a = 2 OR a < 1`, `a = 2 OR a < 1`},
		{`a = 1 OR a <= 1`, `a <= 1`},
		{`a = 1 OR a <= 2`, `a <= 2`},
		{`a = 2 OR a <= 1`, `a = 2 OR a <= 1`},

		{`a != 1 OR a = 1`, `true`},
		{`a != 1 OR a = 2`, `a != 1`},
		{`a != 2 OR a = 1`, `a != 2`},
		{`a != 1 OR a != 1`, `a != 1`},
		{`a != 1 OR a != 2`, `true`},
		{`a != 2 OR a != 1`, `true`},
		{`a != 1 OR a > 1`, `a != 1`},
		{`a != 1 OR a > 2`, `a != 1`},
		{`a != 2 OR a > 1`, `true`},
		{`a != 1 OR a >= 1`, `true`},
		{`a != 1 OR a >= 2`, `a != 1`},
		{`a != 2 OR a >= 1`, `true`},
		{`a != 1 OR a < 1`, `a != 1`},
		{`a != 1 OR a < 2`, `true`},
		{`a != 2 OR a < 1`, `a != 2`},
		{`a != 1 OR a <= 1`, `true`},
		{`a != 1 OR a <= 2`, `true`},
		{`a != 2 OR a <= 1`, `a != 2`},

		{`a > 1 OR a = 1`, `a >= 1`},
		{`a > 1 OR a = 2`, `a > 1`},
		{`a > 2 OR a = 1`, `a > 2 OR a = 1`},
		{`a > 1 OR a != 1`, `a != 1`},
		{`a > 1 OR a != 2`, `true`},
		{`a > 2 OR a != 1`, `a != 1`},
		{`a > 1 OR a > 1`, `a > 1`},
		{`a > 1 OR a > 2`, `a > 1`},
		{`a > 2 OR a > 1`, `a > 1`},
		{`a > 1 OR a >= 1`, `a >= 1`},
		{`a > 1 OR a >= 2`, `a > 1`},
		{`a > 2 OR a >= 1`, `a >= 1`},
		{`a > 1 OR a < 1`, `a != 1`},
		{`a > 1 OR a < 2`, `true`},
		{`a > 2 OR a < 1`, `a > 2 OR a < 1`},
		{`a > 1 OR a <= 1`, `true`},
		{`a > 1 OR a <= 2`, `true`},
		{`a > 2 OR a <= 1`, `a > 2 OR a <= 1`},

		{`a >= 1 OR a = 1`, `a >= 1`},
		{`a >= 1 OR a = 2`, `a >= 1`},
		{`a >= 2 OR a = 1`, `a >= 2 OR a = 1`},
		{`a >= 1 OR a != 1`, `true`},
		{`a >= 1 OR a != 2`, `true`},
		{`a >= 2 OR a != 1`, `a != 1`},
		{`a >= 1 OR a > 1`, `a >= 1`},
		{`a >= 1 OR a > 2`, `a >= 1`},
		{`a >= 2 OR a > 1`, `a > 1`},
		{`a >= 1 OR a >= 1`, `a >= 1`},
		{`a >= 1 OR a >= 2`, `a >= 1`},
		{`a >= 2 OR a >= 1`, `a >= 1`},
		{`a >= 1 OR a < 1`, `true`},
		{`a >= 1 OR a < 2`, `true`},
		{`a >= 2 OR a < 1`, `a >= 2 OR a < 1`},
		{`a >= 1 OR a <= 1`, `true`},
		{`a >= 1 OR a <= 2`, `true`},
		{`a >= 2 OR a <= 1`, `a >= 2 OR a <= 1`},

		{`a < 1 OR a = 1`, `a <= 1`},
		{`a < 1 OR a = 2`, `a < 1 OR a = 2`},
		{`a < 2 OR a = 1`, `a < 2`},
		{`a < 1 OR a != 1`, `a != 1`},
		{`a < 1 OR a != 2`, `a != 2`},
		{`a < 2 OR a != 1`, `true`},
		{`a < 1 OR a > 1`, `a != 1`},
		{`a < 1 OR a > 2`, `a < 1 OR a > 2`},
		{`a < 2 OR a > 1`, `true`},
		{`a < 1 OR a >= 1`, `true`},
		{`a < 1 OR a >= 2`, `a < 1 OR a >= 2`},
		{`a < 2 OR a >= 1`, `true`},
		{`a < 1 OR a < 1`, `a < 1`},
		{`a < 1 OR a < 2`, `a < 2`},
		{`a < 2 OR a < 1`, `a < 2`},
		{`a < 1 OR a <= 1`, `a <= 1`},
		{`a < 1 OR a <= 2`, `a <= 2`},
		{`a < 2 OR a <= 1`, `a < 2`},

		{`a <= 1 OR a = 1`, `a <= 1`},
		{`a <= 1 OR a = 2`, `a <= 1 OR a = 2`},
		{`a <= 2 OR a = 1`, `a <= 2`},
		{`a <= 1 OR a != 1`, `true`},
		{`a <= 1 OR a != 2`, `a != 2`},
		{`a <= 2 OR a != 1`, `true`},
		{`a <= 1 OR a > 1`, `true`},
		{`a <= 1 OR a > 2`, `a <= 1 OR a > 2`},
		{`a <= 2 OR a > 1`, `true`},
		{`a <= 1 OR a >= 1`, `true`},
		{`a <= 1 OR a >= 2`, `a <= 1 OR a >= 2`},
		{`a <= 2 OR a >= 1`, `true`},
		{`a <= 1 OR a < 1`, `a <= 1`},
		{`a <= 1 OR a < 2`, `a < 2`},
		{`a <= 2 OR a < 1`, `a <= 2`},
		{`a <= 1 OR a <= 1`, `a <= 1`},
		{`a <= 1 OR a <= 2`, `a <= 2`},
		{`a <= 2 OR a <= 1`, `a <= 2`},
	}
	for _, d := range testData {
		expr1 := parseAndNormalizeExpr(t, d.expr)
		expr2 := simplifyExpr(expr1)
		if s := expr2.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
		if _, ok := expr2.(*parser.OrExpr); !ok {
			// The result was not an OR expression. Verify that the analysis is
			// commutative.
			orExpr := expr1.(*parser.OrExpr)
			orExpr.Left, orExpr.Right = orExpr.Right, orExpr.Left
			expr3 := simplifyExpr(orExpr)
			if s := expr3.String(); d.expected != s {
				t.Errorf("%s: expected %s, but found %s", expr1, d.expected, s)
			}
		}
	}
}
