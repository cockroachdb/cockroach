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
			{Name: "j", Type: ColumnType{Kind: ColumnType_INT}},
		},
		PrimaryIndex: IndexDescriptor{
			Name: "primary", Unique: true, ColumnNames: []string{"a"},
			ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
		},
		Privileges:    NewDefaultPrivilegeDescriptor(),
		FormatVersion: BaseFormatVersion,
	}
}

func parseAndNormalizeExpr(t *testing.T, sql string) (parser.Expr, qvalMap) {
	expr, err := parser.ParseExprTraditional(sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	expr, err = (parser.EvalContext{}).NormalizeExpr(expr)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}

	// Perform qualified name resolution because {analyze,simplify}Expr want
	// expressions containing qvalues.
	desc := testTableDesc()
	sel := testInitDummySelectNode(desc)
	if err := desc.AllocateIDs(); err != nil {
		t.Fatal(err)
	}
	expr, nErr := sel.resolveQNames(expr)
	if nErr != nil {
		t.Fatalf("%s: %v", sql, nErr)
	}
	if _, err := expr.TypeCheck(nil); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return expr, sel.qvals
}

func checkEquivExpr(a, b parser.Expr, qvals qvalMap) error {
	// The expressions above only use the values 1 and 2. Verify that the
	// simplified expressions evaluate to the same value as the original
	// expression for interesting values.
	zero := parser.DInt(0)
	for _, v := range []parser.Datum{zero, zero + 1, zero + 2, zero + 3, parser.DNull} {
		for _, q := range qvals {
			q.datum = v
		}
		da, err := a.Eval(parser.EvalContext{})
		if err != nil {
			return fmt.Errorf("%s: %v", a, err)
		}
		db, err := b.Eval(parser.EvalContext{})
		if err != nil {
			return fmt.Errorf("%s: %v", b, err)
		}
		// This is tricky: we don't require the expressions to produce identical
		// results, but to either both return true or both return not true (either
		// false or NULL).
		if (da == parser.DBool(true)) != (db == parser.DBool(true)) {
			return fmt.Errorf("%s: %s: expected %s, but found %s", a, v, da, db)
		}
	}
	return nil
}

func TestSplitOrExpr(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
	defer leaktest.AfterTest(t)()

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
	defer leaktest.AfterTest(t)()

	testData := []struct {
		expr     string
		expected string
		isEquiv  bool
	}{
		{`true`, `true`, true},
		{`false`, `false`, true},

		{`f`, `f`, true},
		{`f AND g`, `f AND g`, true},
		{`f OR g`, `f OR g`, true},
		{`(f AND g) AND (c AND d)`, `f AND g AND c AND d`, true},
		{`(f OR g) OR (c OR d)`, `f OR g OR c OR d`, true},
		{`i < lower('FOO')`, `i < 'foo'`, true},
		{`a < 1 AND a < 2 AND a < 3 AND a < 4 AND a < 5`, `a < 1`, true},
		{`a < 1 OR a < 2 OR a < 3 OR a < 4 OR a < 5`, `a < 5`, true},
		{`(a < 1 OR a > 1) AND a >= 1`, `a > 1`, true},
		{`a < 1 AND (a > 2 AND a < 1)`, `false`, true},
		{`a < 1 OR (a > 1 OR a < 2)`, `a < 1 OR a IS NOT NULL`, true},
		{`a < 1 AND length(i) > 0`, `a < 1`, false},
		{`a < 1 OR length(i) > 0`, `true`, false},
		{`a <= 5 AND a IN (4, 5, 6)`, `a IN (4, 5)`, true},

		{`a = NULL`, `false`, true},
		{`a != NULL`, `false`, true},
		{`a > NULL`, `false`, true},
		{`a >= NULL`, `false`, true},
		{`a < NULL`, `false`, true},
		{`a <= NULL`, `false`, true},
		{`a IN (NULL)`, `false`, true},

		{`f < false`, `false`, true},
		{`f < true`, `f < true`, true},
		{`f > false`, `f > false`, true},
		{`f > true`, `false`, true},
		{`a < -9223372036854775808`, `false`, true},
		{`a < 9223372036854775807`, `a < 9223372036854775807`, true},
		{`a > -9223372036854775808`, `a > -9223372036854775808`, true},
		{`a > 9223372036854775807`, `false`, true},
		{`h < -1.7976931348623157e+308`, `false`, true},
		{`h < 1.7976931348623157e+308`, `h < 1.7976931348623157e+308`, true},
		{`h > -1.7976931348623157e+308`, `h > -1.7976931348623157e+308`, true},
		{`h > 1.7976931348623157e+308`, `false`, true},
		{`i < ''`, `false`, true},
		{`i > ''`, `i > ''`, true},

		{`a IN (1, 1)`, `a IN (1)`, true},
		{`a IN (2, 3, 1)`, `a IN (1, 2, 3)`, true},
		{`a IN (1, NULL, 2, NULL)`, `a IN (1, 2)`, true},
		{`a IN (1, NULL) OR a IN (2, NULL)`, `a IN (1, 2)`, true},

		{`(a, b) IN ((1, 2))`, `(a, b) IN ((1, 2))`, true},
		{`(a, b) IN ((1, 2), (1, 2))`, `(a, b) IN ((1, 2))`, true},
		{`(a, b) IN ((1, 2)) OR (a, b) IN ((3, 4))`, `(a, b) IN ((1, 2), (3, 4))`, true},
		{`(a, b) = (1, 2)`, `(a, b) IN ((1, 2))`, true},
		{`(a, b) = (1, 2) OR (a, b) = (3, 4)`, `(a, b) IN ((1, 2), (3, 4))`, true},
		{`(a, b) IN ((2, 1), (1, 2), (1, 2), (2, 1))`, `(a, b) IN ((1, 2), (2, 1))`, true},

		// Expressions that don't simplify as of Dec 2015, although they could:
		// {`a <= 5 AND (a, b) IN ((1, 2))`, `(a, b) IN ((1, 2))`, true},
		// {`a <= 5 AND b >= 6 AND (a, b) IN ((1, 2))`, `false`, true},
		// {`(a, b) IN ((1, 2)) AND a = 1`, `(a, b) IN ((1, 2))`, true},

		{`i LIKE '%foo'`, `true`, false},
		{`i LIKE 'foo'`, `i = 'foo'`, false},
		{`i LIKE 'foo%'`, `i >= 'foo' AND i < 'fop'`, false},
		{`i LIKE 'foo_'`, `i >= 'foo' AND i < 'fop'`, false},
		{`i LIKE 'bar_foo%'`, `i >= 'bar' AND i < 'bas'`, false},
		{`i SIMILAR TO '%'`, `true`, false},
		{`i SIMILAR TO 'foo'`, `i = 'foo'`, false},
		{`i SIMILAR TO 'foo%'`, `i >= 'foo' AND i < 'fop'`, false},
		{`i SIMILAR TO '(foo|foobar)%'`, `i >= 'foo' AND i < 'fop'`, false},

		{`c IS NULL`, `c IS NULL`, true},
		{`c IS NOT NULL`, `c IS NOT NULL`, true},
		{`c IS TRUE`, `true`, false},
		{`c IS NOT TRUE`, `true`, false},
		{`c IS FALSE`, `true`, false},
		{`c IS NOT FALSE`, `true`, false},
		{`c IS UNKNOWN`, `c IS NULL`, true},
		{`c IS NOT UNKNOWN`, `c IS NOT NULL`, true},
		{`a IS DISTINCT FROM NULL`, `a IS NOT NULL`, true},
		{`a IS NOT DISTINCT FROM NULL`, `a IS NULL`, true},
		{`c IS NOT NULL AND c IS NULL`, `false`, true},

		// From a logic-test expression that we previously failed to simplify.
		{`((a <= 0 AND h > 1.0) OR (a >= 6 AND a <= 3)) AND a >= 5`, `false`, true},

		// From logic-test expessions that generated nil branches for AND/OR
		// expressions.
		{`((a < 0) AND (a < 0 AND b > 0)) OR (a > 1 AND a < 0)`,
			`a < 0 AND b > 0`, true},
		{`((a < 0) OR (a < 0 OR b > 0)) AND (a > 0 OR a < 1)`,
			`a < 0 OR b > 0 AND a IS NOT NULL`, true},
	}
	for _, d := range testData {
		expr, _ := parseAndNormalizeExpr(t, d.expr)
		expr, equiv := simplifyExpr(expr)
		if s := expr.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
		if d.isEquiv != equiv {
			t.Fatalf("%s: expected %v, but found %v", d.expr, d.isEquiv, equiv)
		}
	}
}

func TestSimplifyNotExpr(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		expr       string
		expected   string
		isEquiv    bool
		checkEquiv bool
	}{
		{`NOT a = 1`, `a != 1`, true, true},
		{`NOT a != 1`, `a = 1`, true, true},
		{`NOT a > 1`, `a <= 1`, true, true},
		{`NOT a >= 1`, `a < 1`, true, true},
		{`NOT a < 1`, `a >= 1`, true, true},
		{`NOT a <= 1`, `a > 1`, true, true},
		{`NOT a IN (1, 2)`, `a NOT IN (1, 2)`, true, true},
		{`NOT a NOT IN (1, 2)`, `a IN (1, 2)`, true, true},
		{`NOT i LIKE 'foo'`, `true`, false, false},
		{`NOT i NOT LIKE 'foo'`, `i = 'foo'`, false, false},
		{`NOT i SIMILAR TO 'foo'`, `true`, false, false},
		{`NOT i NOT SIMILAR TO 'foo'`, `i = 'foo'`, false, false},
		{`NOT (a != 1 AND b != 1)`, `a = 1 OR b = 1`, true, false},
		{`NOT (a != 1 OR a < 1)`, `a = 1`, true, false},
	}
	for _, d := range testData {
		expr1, qvals := parseAndNormalizeExpr(t, d.expr)
		expr2, equiv := simplifyExpr(expr1)
		if s := expr2.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
		if d.isEquiv != equiv {
			t.Errorf("%s: expected %v, but found %v", d.expr, d.isEquiv, equiv)
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
	defer leaktest.AfterTest(t)()

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
		expr2, equiv := simplifyExpr(expr1)
		if s := expr2.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
		if !equiv {
			t.Errorf("%s: expected equivalent, but found %v", d.expr, equiv)
		}
	}
}

func TestSimplifyAndExprCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
		{`a = 1 AND a IS NULL`, `false`, true},
		{`a IS NULL AND a = 1`, `false`, true},
		{`a = 1 AND a IS NOT NULL`, `a = 1`, true},
		{`a IS NOT NULL AND a = 1`, `a = 1`, true},
		{`a = 1 AND a = 1.0`, `a = 1`, true},

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
		{`a != 1 AND a IN (1)`, `false`, true},
		{`a != 1 AND a IN (2)`, `a IN (2)`, true},
		{`a != 1 AND a IN (1, 2)`, `a IN (2)`, true},
		{`a != 1 AND a IS NULL`, `false`, true},
		{`a IS NULL AND a != 1`, `false`, true},
		{`a != 1 AND a IS NOT NULL`, `a != 1 AND a IS NOT NULL`, true},
		{`a IS NOT NULL AND a != 1`, `a IS NOT NULL AND a != 1`, true},
		{`a != 1 AND a = 1.0`, `false`, true},
		{`a != 1 AND a != 1.0`, `a != 1`, true},

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
		{`a > 1 AND a IS NULL`, `false`, true},
		{`a IS NULL AND a > 1`, `false`, true},
		{`a > 1 AND a IS NOT NULL`, `a > 1`, true},
		{`a IS NOT NULL AND a > 1`, `a > 1`, true},
		{`a > 1.0 AND a = 2`, `a = 2`, true},
		{`a > 1 AND a = 2.0`, `a = 2.0`, true},

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
		{`a >= 1 AND a IS NULL`, `false`, true},
		{`a IS NULL AND a >= 1`, `false`, true},
		{`a >= 1 AND a IS NOT NULL`, `a >= 1`, true},
		{`a IS NOT NULL AND a >= 1`, `a >= 1`, true},

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
		{`a < 1 AND a IS NULL`, `false`, true},
		{`a IS NULL AND a < 1`, `false`, true},
		{`a < 1 AND a IS NOT NULL`, `a < 1`, true},
		{`a IS NOT NULL AND a < 1`, `a < 1`, true},

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
		{`a <= 1 AND a IS NULL`, `false`, true},
		{`a IS NULL AND a <= 1`, `false`, true},
		{`a <= 1 AND a IS NOT NULL`, `a <= 1`, true},
		{`a IS NOT NULL AND a <= 1`, `a <= 1`, true},

		{`a IN (1) AND a IN (1)`, `a IN (1)`, true},
		{`a IN (1) AND a IN (2)`, `false`, true},
		{`a IN (1) AND a IN (1, 2, 3, 4, 5)`, `a IN (1)`, true},
		{`a IN (2, 4) AND a IN (1, 2, 3, 4, 5)`, `a IN (2, 4)`, true},
		{`a IN (4, 2) AND a IN (5, 4, 3, 2, 1)`, `a IN (2, 4)`, true},
		{`a IN (1) AND a IS NULL`, `false`, true},
		{`a IS NULL AND a IN (1)`, `false`, true},
		{`a IN (1) AND a IS NOT NULL`, `a IN (1)`, true},
		{`a IS NOT NULL AND a IN (1)`, `a IN (1)`, true},

		{`a IS NULL AND a IS NULL`, `a IS NULL`, true},
		{`a IS NOT NULL AND a IS NOT NULL`, `a IS NOT NULL`, true},
		{`a IS NULL AND a IS NOT NULL`, `false`, true},
	}
	for _, d := range testData {
		expr1, qvals := parseAndNormalizeExpr(t, d.expr)
		expr2, equiv := simplifyExpr(expr1)
		if s := expr2.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
		if d.checkEquiv != equiv {
			t.Errorf("%s: expected %v, but found %v", d.expr, d.checkEquiv, equiv)
		}
		err := checkEquivExpr(expr1, expr2, qvals)
		if d.checkEquiv && err != nil {
			t.Error(err)
			continue
		} else if !d.checkEquiv && err == nil {
			t.Errorf("%s: expected not equivalent, but found equivalent", d.expr)
			continue
		}

		if _, ok := expr2.(*parser.AndExpr); !ok {
			// The result was not an AND expression. Re-parse to re-resolve QNames
			// and verify that the analysis is commutative.
			expr1, _ = parseAndNormalizeExpr(t, d.expr)
			andExpr := expr1.(*parser.AndExpr)
			andExpr.Left, andExpr.Right = andExpr.Right, andExpr.Left
			expr3, equiv := simplifyExpr(andExpr)
			if s := expr3.String(); d.expected != s {
				t.Errorf("%s: expected %s, but found %s", expr1, d.expected, s)
			}
			if d.checkEquiv != equiv {
				t.Errorf("%s: expected %v, but found %v", d.expr, d.checkEquiv, equiv)
			}
		}
	}
}

func TestSimplifyOrExpr(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
		expr2, _ := simplifyExpr(expr1)
		if s := expr2.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
	}
}

func TestSimplifyOrExprCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
		{`a = 1 OR a != 1`, `a IS NOT NULL`},
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
		{`a = 1 OR a = 1.0`, `a = 1`},

		{`a != 1 OR a = 1`, `a IS NOT NULL`},
		{`a != 1 OR a = 2`, `a != 1`},
		{`a != 2 OR a = 1`, `a != 2`},
		{`a != 1 OR a != 1`, `a != 1`},
		{`a != 1 OR a != 2`, `a IS NOT NULL`},
		{`a != 2 OR a != 1`, `a IS NOT NULL`},
		{`a != 1 OR a > 1`, `a != 1`},
		{`a != 1 OR a > 2`, `a != 1`},
		{`a != 2 OR a > 1`, `a IS NOT NULL`},
		{`a != 1 OR a >= 1`, `a IS NOT NULL`},
		{`a != 1 OR a >= 2`, `a != 1`},
		{`a != 2 OR a >= 1`, `a IS NOT NULL`},
		{`a != 1 OR a < 1`, `a != 1`},
		{`a != 1 OR a < 2`, `a IS NOT NULL`},
		{`a != 2 OR a < 1`, `a != 2`},
		{`a != 1 OR a <= 1`, `a IS NOT NULL`},
		{`a != 1 OR a <= 2`, `a IS NOT NULL`},
		{`a != 2 OR a <= 1`, `a != 2`},
		{`a != 1 OR a IN (1)`, `a IS NOT NULL`},
		{`a != 1 OR a IN (2)`, `a != 1`},
		{`a != 2 OR a IN (1, 2)`, `a IS NOT NULL`},
		{`a != 1 OR a = 1.0`, `a IS NOT NULL`},
		{`a != 1 OR a != 1.0`, `a != 1`},

		{`a > 1 OR a = 1`, `a >= 1`},
		{`a > 1 OR a = 2`, `a > 1`},
		{`a > 2 OR a = 1`, `a > 2 OR a = 1`},
		{`a > 1 OR a != 1`, `a != 1`},
		{`a > 1 OR a != 2`, `a IS NOT NULL`},
		{`a > 2 OR a != 1`, `a != 1`},
		{`a > 1 OR a > 1`, `a > 1`},
		{`a > 1 OR a > 2`, `a > 1`},
		{`a > 2 OR a > 1`, `a > 1`},
		{`a > 1 OR a >= 1`, `a >= 1`},
		{`a > 1 OR a >= 2`, `a > 1`},
		{`a > 2 OR a >= 1`, `a >= 1`},
		{`a > 1 OR a < 1`, `a != 1`},
		{`a > 1 OR a < 2`, `a IS NOT NULL`},
		{`a > 2 OR a < 1`, `a > 2 OR a < 1`},
		{`a > 1 OR a <= 1`, `a IS NOT NULL`},
		{`a > 1 OR a <= 2`, `a IS NOT NULL`},
		{`a > 2 OR a <= 1`, `a > 2 OR a <= 1`},
		{`a > 1 OR a IN (1)`, `a >= 1`},
		{`a > 1 OR a IN (2)`, `a > 1`},
		{`a > 2 OR a IN (1)`, `a > 2 OR a IN (1)`},
		{`a > 1.0 OR a = 1`, `a >= 1`},
		{`a > 1 OR a = 1.0`, `a >= 1`},

		{`a >= 1 OR a = 1`, `a >= 1`},
		{`a >= 1 OR a = 2`, `a >= 1`},
		{`a >= 2 OR a = 1`, `a >= 2 OR a = 1`},
		{`a >= 1 OR a != 1`, `a IS NOT NULL`},
		{`a >= 1 OR a != 2`, `a IS NOT NULL`},
		{`a >= 2 OR a != 1`, `a != 1`},
		{`a >= 1 OR a > 1`, `a >= 1`},
		{`a >= 1 OR a > 2`, `a >= 1`},
		{`a >= 2 OR a > 1`, `a > 1`},
		{`a >= 1 OR a >= 1`, `a >= 1`},
		{`a >= 1 OR a >= 2`, `a >= 1`},
		{`a >= 2 OR a >= 1`, `a >= 1`},
		{`a >= 1 OR a < 1`, `a IS NOT NULL`},
		{`a >= 1 OR a < 2`, `a IS NOT NULL`},
		{`a >= 2 OR a < 1`, `a >= 2 OR a < 1`},
		{`a >= 1 OR a <= 1`, `a IS NOT NULL`},
		{`a >= 1 OR a <= 2`, `a IS NOT NULL`},
		{`a >= 2 OR a <= 1`, `a >= 2 OR a <= 1`},
		{`a >= 1 OR a IN (1)`, `a >= 1`},
		{`a >= 1 OR a IN (2)`, `a >= 1`},
		{`a >= 2 OR a IN (1)`, `a >= 2 OR a IN (1)`},

		{`a < 1 OR a = 1`, `a <= 1`},
		{`a < 1 OR a = 2`, `a < 1 OR a = 2`},
		{`a < 2 OR a = 1`, `a < 2`},
		{`a < 1 OR a != 1`, `a != 1`},
		{`a < 1 OR a != 2`, `a != 2`},
		{`a < 2 OR a != 1`, `a IS NOT NULL`},
		{`a < 1 OR a > 1`, `a != 1`},
		{`a < 1 OR a > 2`, `a < 1 OR a > 2`},
		{`a < 2 OR a > 1`, `a IS NOT NULL`},
		{`a < 1 OR a >= 1`, `a IS NOT NULL`},
		{`a < 1 OR a >= 2`, `a < 1 OR a >= 2`},
		{`a < 2 OR a >= 1`, `a IS NOT NULL`},
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
		{`a <= 1 OR a != 1`, `a IS NOT NULL`},
		{`a <= 1 OR a != 2`, `a != 2`},
		{`a <= 2 OR a != 1`, `a IS NOT NULL`},
		{`a <= 1 OR a > 1`, `a IS NOT NULL`},
		{`a <= 1 OR a > 2`, `a <= 1 OR a > 2`},
		{`a <= 2 OR a > 1`, `a IS NOT NULL`},
		{`a <= 1 OR a >= 1`, `a IS NOT NULL`},
		{`a <= 1 OR a >= 2`, `a <= 1 OR a >= 2`},
		{`a <= 2 OR a >= 1`, `a IS NOT NULL`},
		{`a <= 1 OR a < 1`, `a <= 1`},
		{`a <= 1 OR a < 2`, `a < 2`},
		{`a <= 2 OR a < 1`, `a <= 2`},
		{`a <= 1 OR a <= 1`, `a <= 1`},
		{`a <= 1 OR a <= 2`, `a <= 2`},
		{`a <= 2 OR a <= 1`, `a <= 2`},
		{`a <= 1 OR a IN (1)`, `a <= 1`},
		{`a <= 1 OR a IN (2)`, `a <= 1 OR a IN (2)`},
		{`a <= 2 OR a IN (1)`, `a <= 2`},

		{`a IN (1) OR a IN (1)`, `a IN (1)`},
		{`a IN (1) OR a IN (2)`, `a IN (1, 2)`},
		{`a IN (1) OR a IN (1, 2, 3, 4, 5)`, `a IN (1, 2, 3, 4, 5)`},
		{`a IN (4, 2) OR a IN (5, 4, 3, 2, 1)`, `a IN (1, 2, 3, 4, 5)`},
		{`a IN (1) OR a IS NULL`, `a IN (1) OR a IS NULL`},

		{`a IS NULL OR a IS NULL`, `a IS NULL`},
		{`a IS NOT NULL OR a IS NOT NULL`, `a IS NOT NULL`},
		{`a IS NULL OR a IS NOT NULL`, `true`},
	}
	for _, d := range testData {
		expr1, qvals := parseAndNormalizeExpr(t, d.expr)
		expr2, equiv := simplifyExpr(expr1)
		if s := expr2.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
		if !equiv {
			t.Errorf("%s: expected equivalent, but found %v", d.expr, equiv)
		}

		if err := checkEquivExpr(expr1, expr2, qvals); err != nil {
			t.Error(err)
			continue
		}

		if _, ok := expr2.(*parser.OrExpr); !ok {
			// The result was not an OR expression. Re-parse to re-resolve QNames
			// and verify that the analysis is commutative.
			expr1, _ = parseAndNormalizeExpr(t, d.expr)
			orExpr := expr1.(*parser.OrExpr)
			orExpr.Left, orExpr.Right = orExpr.Right, orExpr.Left
			expr3, equiv := simplifyExpr(orExpr)
			if s := expr3.String(); d.expected != s {
				t.Errorf("%s: expected %s, but found %s", expr1, d.expected, s)
			}
			if !equiv {
				t.Errorf("%s: expected equivalent, but found %v", d.expr, equiv)
			}
		}
	}
}
