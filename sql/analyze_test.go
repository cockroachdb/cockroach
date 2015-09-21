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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func testTableDesc() *TableDescriptor {
	return &TableDescriptor{
		Name:     "test",
		ID:       1001,
		ParentID: 1000,
		Columns: []ColumnDescriptor{
			{Name: "a", Type: ColumnType{Kind: ColumnType_INT}},
			{Name: "b", Type: ColumnType{Kind: ColumnType_INT}},
			{Name: "c", Type: ColumnType{Kind: ColumnType_BOOL}},
			{Name: "d", Type: ColumnType{Kind: ColumnType_BOOL}},
			{Name: "e", Type: ColumnType{Kind: ColumnType_BOOL}},
			{Name: "f", Type: ColumnType{Kind: ColumnType_BOOL}},
			{Name: "g", Type: ColumnType{Kind: ColumnType_BOOL}},
			{Name: "h", Type: ColumnType{Kind: ColumnType_FLOAT}},
			{Name: "i", Type: ColumnType{Kind: ColumnType_STRING}},
		},
		PrimaryIndex: IndexDescriptor{
			Name: "primary", Unique: true, ColumnNames: []string{"a"},
		},
		Privileges: NewDefaultPrivilegeDescriptor(),
	}
}

func parseAndNormalizeExpr(t *testing.T, sql string) (parser.Expr, qvalMap) {
	q, err := parser.ParseTraditional("SELECT " + sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	expr := q[0].(*parser.Select).Exprs[0].Expr
	expr, err = parser.NormalizeExpr(expr)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}

	// Perform qualified name resolution because {analyze,simplify}Expr want
	// expressions containing qvalues.
	s := &scanNode{}
	s.desc = testTableDesc()
	s.visibleCols = s.desc.Columns

	if err := s.desc.AllocateIDs(); err != nil {
		t.Fatal(err)
	}

	expr, err = s.resolveQNames(expr)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if _, err := parser.TypeCheckExpr(expr); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return expr, s.qvals
}

func checkEquivExpr(a, b parser.Expr, qvals qvalMap) error {
	// The expressions above only use the values 1 and 2. Verify that the
	// simplified expressions evaluate to the same value as the original
	// expression for interesting values.
	for _, v := range []parser.DInt{0, 1, 2, 3} {
		for _, q := range qvals {
			q.datum = v
		}
		da, err := parser.EvalExpr(a)
		if err != nil {
			return fmt.Errorf("%s: %v", a, err)
		}
		db, err := parser.EvalExpr(b)
		if err != nil {
			return fmt.Errorf("%s: %v", b, err)
		}
		if da != db {
			return fmt.Errorf("%s: %d: expected %s, but found %s", a, v, da, db)
		}
	}
	return nil
}

func TestSplitOrExpr(t *testing.T) {
	defer leaktest.AfterTest(t)

	testData := []struct {
		expr     string
		expected string
	}{
		{`f`, `f`},
		{`f AND g`, `f AND g`},
		{`f OR g`, `f, g`},
		{`(f OR g) OR (c OR (d OR e))`, `f, g, c, d, e`},
	}
	for _, d := range testData {
		expr, _ := parseAndNormalizeExpr(t, d.expr)
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
		{`f`, `f`},
		{`f AND g`, `f, g`},
		{`f OR g`, `f OR g`},
		{`(f AND g) AND (c AND (d AND e))`, `f, g, c, d, e`},
	}
	for _, d := range testData {
		expr, _ := parseAndNormalizeExpr(t, d.expr)
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
		{`true`, `true`},
		{`false`, `false`},

		{`f`, `f`},
		{`f AND g`, `f AND g`},
		{`f OR g`, `f OR g`},
		{`(f AND g) AND (c AND d)`, `f AND g AND c AND d`},
		{`(f OR g) OR (c OR d)`, `f OR g OR c OR d`},
		{`i < lower('FOO')`, `i < 'foo'`},
		{`a < 1 AND a < 2 AND a < 3 AND a < 4 AND a < 5`, `a < 1`},
		{`a < 1 OR a < 2 OR a < 3 OR a < 4 OR a < 5`, `a < 5`},
		{`(a < 1 OR a > 1) AND a >= 1`, `a > 1`},
		{`a < 1 AND (a > 2 AND a < 1)`, `false`},
		{`a < 1 OR (a > 1 OR a < 2)`, `true`},
		{`a < 1 AND length(i) > 0`, `a < 1`},
		{`a < 1 OR length(i) > 0`, `true`},

		{`a = NULL`, `false`},
		{`a != NULL`, `false`},
		{`a > NULL`, `false`},
		{`a >= NULL`, `false`},
		{`a < NULL`, `false`},
		{`a <= NULL`, `false`},
		{`a IN (NULL)`, `false`},

		{`f < false`, `false`},
		{`f < true`, `f < true`},
		{`f > false`, `f > false`},
		{`f > true`, `false`},
		{`a < -9223372036854775808`, `false`},
		{`a < 9223372036854775807`, `a < 9223372036854775807`},
		{`a > -9223372036854775808`, `a > -9223372036854775808`},
		{`a > 9223372036854775807`, `false`},
		{`h < -1.7976931348623157e+308`, `false`},
		{`h < 1.7976931348623157e+308`, `h < 1.7976931348623157e+308`},
		{`h > -1.7976931348623157e+308`, `h > -1.7976931348623157e+308`},
		{`h > 1.7976931348623157e+308`, `false`},
		{`i < ''`, `false`},
		{`i > ''`, `i > ''`},

		{`a IN (1, 1)`, `a IN (1)`},
		{`a IN (2, 3, 1)`, `a IN (1, 2, 3)`},
		{`a IN (1, NULL, 2, NULL)`, `a IN (1, 2)`},
		{`a IN (1, NULL) OR a IN (2, NULL)`, `a IN (1, 2)`},

		{`(a, b) IN ((1, 2))`, `(a, b) IN ((1, 2))`},
		{`(a, b) IN ((1, 2), (1, 2))`, `(a, b) IN ((1, 2))`},
		{`(a, b) IN ((1, 2)) OR (a, b) IN ((3, 4))`, `(a, b) IN ((1, 2), (3, 4))`},
		{`(a, b) = (1, 2) OR (a, b) = (3, 4)`, `(a, b) IN ((1, 2), (3, 4))`},
		{`(a, b) IN ((2, 1), (1, 2), (1, 2), (2, 1))`, `(a, b) IN ((1, 2), (2, 1))`},

		{`i LIKE '%foo'`, `true`},
		{`i LIKE 'foo'`, `i = 'foo'`},
		{`i LIKE 'foo%'`, `i >= 'foo' AND i < 'fop'`},
		{`i LIKE 'foo_'`, `i >= 'foo' AND i < 'fop'`},
		{`i LIKE 'bar_foo%'`, `i >= 'bar' AND i < 'bas'`},
		{`i SIMILAR TO '%'`, `true`},
		{`i SIMILAR TO 'foo'`, `i = 'foo'`},
		{`i SIMILAR TO 'foo%'`, `i >= 'foo' AND i < 'fop'`},
		{`i SIMILAR TO '(foo|foobar)%'`, `i >= 'foo' AND i < 'fop'`},
	}
	for _, d := range testData {
		expr, _ := parseAndNormalizeExpr(t, d.expr)
		expr = simplifyExpr(expr)
		if s := expr.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
	}
}

func TestSimplifyNotExpr(t *testing.T) {
	defer leaktest.AfterTest(t)

	testData := []struct {
		expr       string
		expected   string
		checkEquiv bool
	}{
		{`NOT a = 1`, `a != 1`, true},
		{`NOT a != 1`, `a = 1`, true},
		{`NOT a > 1`, `a <= 1`, true},
		{`NOT a >= 1`, `a < 1`, true},
		{`NOT a < 1`, `a >= 1`, true},
		{`NOT a <= 1`, `a > 1`, true},
		{`NOT a IN (1, 2)`, `a NOT IN (1, 2)`, true},
		{`NOT a NOT IN (1, 2)`, `a IN (1, 2)`, true},
		{`NOT i LIKE 'foo'`, `true`, false},
		{`NOT i NOT LIKE 'foo'`, `i = 'foo'`, false},
		{`NOT i SIMILAR TO 'foo'`, `true`, false},
		{`NOT i NOT SIMILAR TO 'foo'`, `i = 'foo'`, false},
		{`NOT (a != 1 AND b != 1)`, `a = 1 OR b = 1`, false},
		{`NOT (a != 1 OR a < 1)`, `a = 1`, false},
	}
	for _, d := range testData {
		expr1, qvals := parseAndNormalizeExpr(t, d.expr)
		expr2 := simplifyExpr(expr1)
		if s := expr2.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
		if d.checkEquiv {
			if err := checkEquivExpr(expr1, expr2, qvals); err != nil {
				t.Error(err)
				continue
			}
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

		{`a = 1 AND a = 2`, `false`},
		{`a = 1 AND a = NULL`, `false`},
		{`a = 1 AND a != NULL`, `false`},
		{`a = 1 AND b = 1`, `a = 1 AND b = 1`},
	}
	for _, d := range testData {
		expr1, _ := parseAndNormalizeExpr(t, d.expr)
		expr2 := simplifyExpr(expr1)
		if s := expr2.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
	}
}

func TestSimplifyAndExprCheck(t *testing.T) {
	defer leaktest.AfterTest(t)

	testData := []struct {
		expr       string
		expected   string
		checkEquiv bool
	}{
		{`a = 1 AND a = 1`, `a = 1`, true},
		{`a = 1 AND a = 2`, `false`, true},
		{`a = 2 AND a = 1`, `false`, true},
		{`a = 1 AND a != 1`, `false`, true},
		{`a = 1 AND a != 2`, `a = 1`, true},
		{`a = 2 AND a != 1`, `a = 2`, true},
		{`a = 1 AND a > 1`, `false`, true},
		{`a = 1 AND a > 2`, `false`, true},
		{`a = 2 AND a > 1`, `a = 2`, true},
		{`a = 1 AND a >= 1`, `a = 1`, true},
		{`a = 1 AND a >= 2`, `false`, true},
		{`a = 2 AND a >= 1`, `a = 2`, true},
		{`a = 1 AND a < 1`, `false`, true},
		{`a = 1 AND a < 2`, `a = 1`, true},
		{`a = 2 AND a < 1`, `false`, true},
		{`a = 1 AND a <= 1`, `a = 1`, true},
		{`a = 1 AND a <= 2`, `a = 1`, true},
		{`a = 2 AND a <= 1`, `false`, true},
		{`a = 1 AND a IN (1)`, `a = 1`, true},
		{`a = 1 AND a IN (2)`, `false`, true},
		{`a = 2 AND a IN (1)`, `false`, true},
		{`a = 2 AND a IN (0, 1, 2, 3, 4)`, `a = 2`, true},

		{`a != 1 AND a = 1`, `false`, true},
		{`a != 1 AND a = 2`, `a = 2`, true},
		{`a != 2 AND a = 1`, `a = 1`, true},
		{`a != 1 AND a != 1`, `a != 1`, true},
		{`a != 1 AND a != 2`, `a != 1 AND a != 2`, true},
		{`a != 2 AND a != 1`, `a != 2 AND a != 1`, true},
		{`a != 1 AND a > 1`, `a > 1`, true},
		{`a != 1 AND a > 2`, `a > 2`, true},
		{`a != 2 AND a > 1`, `a > 1`, false},
		{`a != 1 AND a >= 1`, `a > 1`, true},
		{`a != 1 AND a >= 2`, `a >= 2`, true},
		{`a != 2 AND a >= 1`, `a >= 1`, false},
		{`a != 1 AND a < 1`, `a < 1`, true},
		{`a != 1 AND a < 2`, `a < 2`, false},
		{`a != 2 AND a < 1`, `a < 1`, true},
		{`a != 1 AND a <= 1`, `a < 1`, true},
		{`a != 1 AND a <= 2`, `a <= 2`, false},
		{`a != 2 AND a <= 1`, `a <= 1`, true},

		{`a > 1 AND a = 1`, `false`, true},
		{`a > 1 AND a = 2`, `a = 2`, true},
		{`a > 2 AND a = 1`, `false`, true},
		{`a > 1 AND a != 1`, `a > 1`, true},
		{`a > 1 AND a != 2`, `a > 1`, false},
		{`a > 2 AND a != 1`, `a > 2`, true},
		{`a > 1 AND a > 1`, `a > 1`, true},
		{`a > 1 AND a > 2`, `a > 2`, true},
		{`a > 2 AND a > 1`, `a > 2`, true},
		{`a > 1 AND a >= 1`, `a > 1`, true},
		{`a > 1 AND a >= 2`, `a >= 2`, true},
		{`a > 2 AND a >= 1`, `a > 2`, true},
		{`a > 1 AND a < 1`, `false`, true},
		{`a > 1 AND a < 2`, `a > 1 AND a < 2`, true},
		{`a > 2 AND a < 1`, `false`, true},
		{`a > 1 AND a <= 1`, `false`, true},
		{`a > 1 AND a <= 2`, `a > 1 AND a <= 2`, true},
		{`a > 2 AND a <= 1`, `false`, true},
		{`a > 1 AND a IN (1)`, `false`, true},
		{`a > 1 AND a IN (2)`, `a IN (2)`, true},
		{`a > 2 AND a IN (1)`, `false`, true},
		{`a > 2 AND a IN (0, 1, 2, 3, 4)`, `a IN (3, 4)`, true},

		{`a >= 1 AND a = 1`, `a = 1`, true},
		{`a >= 1 AND a = 2`, `a = 2`, true},
		{`a >= 2 AND a = 1`, `false`, true},
		{`a >= 1 AND a != 1`, `a > 1`, true},
		{`a >= 1 AND a != 2`, `a >= 1`, false},
		{`a >= 2 AND a != 1`, `a >= 2`, true},
		{`a >= 1 AND a > 1`, `a > 1`, true},
		{`a >= 1 AND a > 2`, `a > 2`, true},
		{`a >= 2 AND a > 1`, `a >= 2`, true},
		{`a >= 1 AND a >= 1`, `a >= 1`, true},
		{`a >= 1 AND a >= 2`, `a >= 2`, true},
		{`a >= 2 AND a >= 1`, `a >= 2`, true},
		{`a >= 1 AND a < 1`, `false`, true},
		{`a >= 1 AND a < 2`, `a >= 1 AND a < 2`, true},
		{`a >= 2 AND a < 1`, `false`, true},
		{`a >= 1 AND a <= 1`, `a = 1`, true},
		{`a >= 1 AND a <= 2`, `a >= 1 AND a <= 2`, true},
		{`a >= 2 AND a <= 1`, `false`, true},
		{`a >= 1 AND a IN (1)`, `a IN (1)`, true},
		{`a >= 1 AND a IN (2)`, `a IN (2)`, true},
		{`a >= 2 AND a IN (1)`, `false`, true},
		{`a >= 2 AND a IN (0, 1, 2, 3, 4)`, `a IN (2, 3, 4)`, true},

		{`a < 1 AND a = 1`, `false`, true},
		{`a < 1 AND a = 2`, `false`, true},
		{`a < 2 AND a = 1`, `a = 1`, true},
		{`a < 1 AND a != 1`, `a < 1`, true},
		{`a < 1 AND a != 2`, `a < 1`, true},
		{`a < 2 AND a != 1`, `a < 2`, false},
		{`a < 1 AND a > 1`, `false`, true},
		{`a < 1 AND a > 2`, `false`, true},
		{`a < 2 AND a > 1`, `a < 2 AND a > 1`, true},
		{`a < 1 AND a >= 1`, `false`, true},
		{`a < 1 AND a >= 2`, `false`, true},
		{`a < 2 AND a >= 1`, `a < 2 AND a >= 1`, true},
		{`a < 1 AND a < 1`, `a < 1`, true},
		{`a < 1 AND a < 2`, `a < 1`, true},
		{`a < 2 AND a < 1`, `a < 1`, true},
		{`a < 1 AND a <= 1`, `a < 1`, true},
		{`a < 1 AND a <= 2`, `a < 1`, true},
		{`a < 2 AND a <= 1`, `a <= 1`, true},
		{`a < 1 AND a IN (1)`, `false`, true},
		{`a < 1 AND a IN (2)`, `false`, true},
		{`a < 2 AND a IN (1)`, `a IN (1)`, true},
		{`a < 2 AND a IN (0, 1, 2, 3, 4)`, `a IN (0, 1)`, true},

		{`a <= 1 AND a = 1`, `a = 1`, true},
		{`a <= 1 AND a = 2`, `false`, true},
		{`a <= 2 AND a = 1`, `a = 1`, true},
		{`a <= 1 AND a != 1`, `a < 1`, true},
		{`a <= 1 AND a != 2`, `a <= 1`, true},
		{`a <= 2 AND a != 1`, `a <= 2`, false},
		{`a <= 1 AND a > 1`, `false`, true},
		{`a <= 1 AND a > 2`, `false`, true},
		{`a <= 2 AND a > 1`, `a <= 2 AND a > 1`, true},
		{`a <= 1 AND a >= 1`, `a = 1`, true},
		{`a <= 1 AND a >= 2`, `false`, true},
		{`a <= 2 AND a >= 1`, `a <= 2 AND a >= 1`, true},
		{`a <= 1 AND a < 1`, `a < 1`, true},
		{`a <= 1 AND a < 2`, `a <= 1`, true},
		{`a <= 2 AND a < 1`, `a < 1`, true},
		{`a <= 1 AND a <= 1`, `a <= 1`, true},
		{`a <= 1 AND a <= 2`, `a <= 1`, true},
		{`a <= 2 AND a <= 1`, `a <= 1`, true},
		{`a <= 1 AND a IN (1)`, `a IN (1)`, true},
		{`a <= 1 AND a IN (2)`, `false`, true},
		{`a <= 2 AND a IN (1)`, `a IN (1)`, true},
		{`a <= 2 AND a IN (0, 1, 2, 3, 4)`, `a IN (0, 1, 2)`, true},

		{`a IN (1) AND a IN (1)`, `a IN (1)`, true},
		{`a IN (1) AND a IN (2)`, `false`, true},
		{`a IN (2) AND a IN (1)`, `false`, true},
		{`a IN (1) AND a IN (1, 2, 3, 4, 5)`, `a IN (1)`, true},
		{`a IN (2, 4) AND a IN (1, 2, 3, 4, 5)`, `a IN (2, 4)`, true},
		{`a IN (4, 2) AND a IN (5, 4, 3, 2, 1)`, `a IN (2, 4)`, true},
	}
	for _, d := range testData {
		expr1, qvals := parseAndNormalizeExpr(t, d.expr)
		expr2 := simplifyExpr(expr1)
		if s := expr2.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}

		if d.checkEquiv {
			if err := checkEquivExpr(expr1, expr2, qvals); err != nil {
				t.Error(err)
				continue
			}
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
		{`a = 1 OR a = 2`, `a IN (1, 2)`},
		{`a = 1 OR a = NULL`, `a = 1`},
		{`a = 1 OR a != NULL`, `a = 1`},
		{`a = 1 OR b = 1`, `a = 1 OR b = 1`},
	}
	for _, d := range testData {
		expr1, _ := parseAndNormalizeExpr(t, d.expr)
		expr2 := simplifyExpr(expr1)
		if s := expr2.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
	}
}

func TestSimplifyOrExprCheck(t *testing.T) {
	defer leaktest.AfterTest(t)

	testData := []struct {
		expr     string
		expected string
	}{
		{`a = 1 OR a = 2 OR a = 3 or a = 4`, `a IN (1, 2, 3, 4)`},
		{`a = 1 OR a IN (1, 2, 3) OR a = 2 OR a = 3`, `a IN (1, 2, 3)`},
		{`a > 1 OR a IN (2, 3)`, `a > 1`},

		{`a < 1 OR b < 1 OR a < 2 OR b < 2`, `a < 2 OR b < 2`},
		{`(a > 2 OR a < 1) OR (a > 3 OR a < 0)`, `a > 2 OR a < 1`},

		{`a = 1 OR a = 1`, `a = 1`},
		{`a = 1 OR a = 2`, `a IN (1, 2)`},
		{`a = 2 OR a = 1`, `a IN (1, 2)`},
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
		{`a = 1 OR a IN (1)`, `a IN (1)`},
		{`a = 1 OR a IN (2)`, `a IN (1, 2)`},
		{`a = 2 OR a IN (1)`, `a IN (1, 2)`},

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
		{`a > 1 OR a IN (1)`, `a >= 1`},
		{`a > 1 OR a IN (2)`, `a > 1`},
		{`a > 2 OR a IN (1)`, `a > 2 OR a IN (1)`},

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
		{`a >= 1 OR a IN (1)`, `a >= 1`},
		{`a >= 1 OR a IN (2)`, `a >= 1`},
		{`a >= 2 OR a IN (1)`, `a >= 2 OR a IN (1)`},

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
		{`a < 1 OR a IN (1)`, `a <= 1`},
		{`a < 1 OR a IN (2)`, `a < 1 OR a IN (2)`},
		{`a < 2 OR a IN (1)`, `a < 2`},

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
		{`a <= 1 OR a IN (1)`, `a <= 1`},
		{`a <= 1 OR a IN (2)`, `a <= 1 OR a IN (2)`},
		{`a <= 2 OR a IN (1)`, `a <= 2`},
	}
	for _, d := range testData {
		expr1, qvals := parseAndNormalizeExpr(t, d.expr)
		expr2 := simplifyExpr(expr1)
		if s := expr2.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}

		if err := checkEquivExpr(expr1, expr2, qvals); err != nil {
			t.Error(err)
			continue
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
