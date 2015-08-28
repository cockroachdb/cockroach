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

var testTableDesc = &TableDescriptor{
	Name: "test",
	ID:   1000,
	Columns: []ColumnDescriptor{
		{Name: "a", Type: ColumnType{Kind: ColumnType_INT}},
		{Name: "b", Type: ColumnType{Kind: ColumnType_INT}},
		{Name: "c", Type: ColumnType{Kind: ColumnType_INT}},
		{Name: "d", Type: ColumnType{Kind: ColumnType_INT}},
		{Name: "e", Type: ColumnType{Kind: ColumnType_INT}},
	},
	PrimaryIndex: IndexDescriptor{
		Name: "primary", Unique: true, ColumnNames: []string{"a"},
	},
	Privileges: NewDefaultPrivilegeDescriptor(),
}

func init() {
	if err := testTableDesc.AllocateIDs(); err != nil {
		panic(err)
	}
}

func parseAndNormalizeExpr(t *testing.T, sql string) (parser.Expr, qvalMap) {
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
	s.desc = testTableDesc
	s.visibleCols = s.desc.Columns

	r, err = s.resolveQNames(r)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return r, s.qvals
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
		{`a`, `a`},
		{`a AND b`, `a AND b`},
		{`a OR b`, `a, b`},
		{`(a OR b) OR (c OR (d OR e))`, `a, b, c, d, e`},
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
		{`a`, `a`},
		{`a AND b`, `a, b`},
		{`a OR b`, `a OR b`},
		{`(a AND b) AND (c AND (d AND e))`, `a, b, c, d, e`},
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

		{`a IN (1, 1)`, `a IN (1)`},
		{`a IN (2, 3, 1)`, `a IN (1, 2, 3)`},
		{`a IN (1, true)`, `true`},
		{`a IN (1, NULL)`, `a IN (NULL, 1)`}, // TODO(pmattis): `a IN (1)`

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
		{`NOT a LIKE 'foo'`, `true`, false},
		{`NOT a NOT LIKE 'foo'`, `a = 'foo'`, false},
		{`NOT a SIMILAR TO 'foo'`, `true`, false},
		{`NOT a NOT SIMILAR TO 'foo'`, `a = 'foo'`, false},
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

		{`a = 1 AND a = true`, `false`},
		{`a = 1 AND a = 1.1`, `false`},
		{`a = 1 AND a = '1'`, `false`},
		{`a = 1 AND a = (1, 2)`, `false`},
		{`a = 1 AND a = NULL`, `false`},
		{`a = 1 AND a != NULL`, `a = 1 AND a != NULL`},
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
		{`a = 1 OR a = true`, `a = 1 OR a = true`},
		{`a = 1 OR a = 1.1`, `a = 1 OR a = 1.1`},
		{`a = 1 OR a = '1'`, `a = 1 OR a = '1'`},
		{`a = 1 OR a = (1, 2)`, `a = 1 OR a = (1, 2)`},
		{`a = 1 OR a = NULL`, `a = 1 OR a = NULL`},
		{`a = 1 OR a != NULL`, `a = 1 OR a != NULL`},
		{`a = 1 OR b = 1`, `a = 1 OR b = 1`},
	}
	for _, d := range testData {
		expr1, _ := parseAndNormalizeExpr(t, d.expr)
		expr2 := simplifyExpr(expr1)
		if s := expr2.String(); d.expected != s {
			t.Fatalf("%s: expected %s, but found %s", d.expr, d.expected, s)
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
		{`a > 1 OR a IN (2, 3)`, `a > 1 OR a IN (2, 3)`},
		// {`(a, b) IN ((2, 1), (1, 2), (1, 2), (2, 1))`, `(a, b) IN ((1, 2), (2, 1)`},

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
		expr1, qvals := parseAndNormalizeExpr(t, d.expr)
		expr2 := simplifyExpr(expr1)
		if s := expr2.String(); d.expected != s {
			t.Fatalf("%s: expected %s, but found %s", d.expr, d.expected, s)
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
