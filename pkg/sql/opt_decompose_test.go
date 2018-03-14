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

package sql

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func testTableDesc() *sqlbase.TableDescriptor {
	return &sqlbase.TableDescriptor{
		Name:     "test",
		ID:       1001,
		ParentID: 1000,
		Columns: []sqlbase.ColumnDescriptor{
			{Name: "a", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}},
			{Name: "b", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}},
			{Name: "c", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BOOL}},
			{Name: "d", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BOOL}},
			{Name: "e", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BOOL}},
			{Name: "f", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BOOL}},
			{Name: "g", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BOOL}},
			{Name: "h", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_FLOAT}},
			{Name: "i", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_STRING}},
			{Name: "j", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}},
			{Name: "k", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES}},
			{Name: "l", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_DECIMAL}},
			{Name: "m", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_DECIMAL}},
			{Name: "n", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_DATE}},
			{Name: "o", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_TIMESTAMP}},
			{Name: "p", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_TIMESTAMPTZ}},
			{Name: "q", Type: sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}, Nullable: true},
		},
		PrimaryIndex: sqlbase.IndexDescriptor{
			Name: "primary", Unique: true, ColumnNames: []string{"a"},
			ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
		},
		Privileges:    sqlbase.NewDefaultPrivilegeDescriptor(),
		FormatVersion: sqlbase.FamilyFormatVersion,
	}
}

func makeSelectNode(t *testing.T, p *planner) *renderNode {
	desc := testTableDesc()
	sel := testInitDummySelectNode(t, p, desc)
	if err := desc.AllocateIDs(); err != nil {
		t.Fatal(err)
	}
	numColumns := len(sel.sourceInfo[0].SourceColumns)
	sel.ivarHelper = tree.MakeIndexedVarHelper(sel, numColumns)
	p.extendedEvalCtx.IVarContainer = sel
	sel.run.curSourceRow = make(tree.Datums, numColumns)
	return sel
}

func parseAndNormalizeExpr(t *testing.T, p *planner, sql string, sel *renderNode) tree.TypedExpr {
	expr, err := parser.ParseExpr(sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}

	// Perform name resolution because {decompose,simplify}Expr want
	// expressions containing IndexedVars.
	if expr, _, _, err = p.resolveNamesForRender(expr, sel); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	p.semaCtx.IVarContainer = p.extendedEvalCtx.IVarContainer
	typedExpr, err := tree.TypeCheck(expr, &p.semaCtx, types.Any)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if typedExpr, err = p.extendedEvalCtx.NormalizeExpr(typedExpr); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return typedExpr
}

func checkEquivExpr(evalCtx *tree.EvalContext, a, b tree.TypedExpr, sel *renderNode) error {
	// The expressions above only use the values 1 and 2. Verify that the
	// simplified expressions evaluate to the same value as the original
	// expression for interesting values.
	for _, v := range []tree.Datum{
		tree.NewDInt(0),
		tree.NewDInt(1),
		tree.NewDInt(2),
		tree.NewDInt(3),
		tree.DNull,
	} {
		for i := range sel.run.curSourceRow {
			sel.run.curSourceRow[i] = v
		}
		da, err := a.Eval(evalCtx)
		if err != nil {
			return fmt.Errorf("%s: %v", a, err)
		}
		db, err := b.Eval(evalCtx)
		if err != nil {
			return fmt.Errorf("%s: %v", b, err)
		}
		// This is tricky: we don't require the expressions to produce identical
		// results, but to either both return true or both return not true (either
		// false or NULL).
		if (da == tree.DBoolTrue) != (db == tree.DBoolTrue) {
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
	p := makeTestPlanner()
	for _, d := range testData {
		t.Run(d.expr+"~"+d.expected, func(t *testing.T) {
			p.extendedEvalCtx = makeTestingExtendedEvalContext(cluster.MakeTestingClusterSettings())
			defer p.extendedEvalCtx.Stop(context.Background())
			sel := makeSelectNode(t, p)
			expr := parseAndNormalizeExpr(t, p, d.expr, sel)
			exprs := splitOrExpr(p.EvalContext(), expr, nil /* exprs */)
			if s := exprs.String(); d.expected != s {
				t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
			}
		})
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
	p := makeTestPlanner()
	for _, d := range testData {
		t.Run(d.expr+"~"+d.expected, func(t *testing.T) {
			p.extendedEvalCtx = makeTestingExtendedEvalContext(cluster.MakeTestingClusterSettings())
			defer p.extendedEvalCtx.Stop(context.Background())
			sel := makeSelectNode(t, p)
			expr := parseAndNormalizeExpr(t, p, d.expr, sel)
			exprs := splitAndExpr(p.EvalContext(), expr, nil /* exprs */)
			if s := exprs.String(); d.expected != s {
				t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
			}
		})
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

		{`f`, `f = true`, true},
		{`f AND g`, `(f = true) AND (g = true)`, true},
		{`f OR g`, `(f = true) OR (g = true)`, true},
		{`(f AND g) AND (c AND d)`, `(f = true) AND ((g = true) AND ((c = true) AND (d = true)))`, true},
		{`(f OR g) OR (c OR d)`, `(f = true) OR ((g = true) OR ((c = true) OR (d = true)))`, true},
		{`i < lower('FOO')`, `i < 'foo'`, true},
		{`a < 1 AND a < 2 AND a < 3 AND a < 4 AND a < 5`, `a < 1`, true},
		{`a < 1 OR a < 2 OR a < 3 OR a < 4 OR a < 5`, `a < 5`, true},
		{`(a < 1 OR a > 1) AND a >= 1`, `a > 1`, true},
		{`a < 1 AND (a > 2 AND a < 1)`, `false`, true},
		{`a < 1 OR (a > 1 OR a < 2)`, `(a < 1) OR (a IS NOT NULL)`, true},
		{`a < 1 AND length(i) > 0`, `a < 1`, false},
		{`a < 1 OR length(i) > 0`, `true`, false},
		{`a <= 5 AND a IN (4, 5, 6)`, `a IN (4, 5)`, true},

		{`a = NULL`, `NULL`, true},
		{`a != NULL`, `NULL`, true},
		{`a > NULL`, `NULL`, true},
		{`a >= NULL`, `NULL`, true},
		{`a < NULL`, `NULL`, true},
		{`a <= NULL`, `NULL`, true},
		{`a IN (NULL)`, `NULL`, true},

		{`f != false`, `f > false`, true},
		{`f != true`, `f < true`, true},
		{`f < false`, `false`, true},
		{`f < true`, `f < true`, true},
		{`f > false`, `f > false`, true},
		{`f > true`, `false`, true},
		{`a != -9223372036854775808`, `a > -9223372036854775808`, true},
		{`a != 9223372036854775807`, `a < 9223372036854775807`, true},
		{`a < -9223372036854775808`, `false`, true},
		{`a < 9223372036854775807`, `a < 9223372036854775807`, true},
		{`a > -9223372036854775808`, `a > -9223372036854775808`, true},
		{`a > 9223372036854775807`, `false`, true},
		{`h < 'NaN'`, `false`, true},
		{`h < (-1:::float/0:::float)`, `h < -Inf`, true},
		{`h < -1.7976931348623157e+308`, `h < -1.7976931348623157e+308`, true},
		{`h < 1.7976931348623157e+308`, `h < 1.7976931348623157e+308`, true},
		{`h > -1.7976931348623157e+308`, `h > -1.7976931348623157e+308`, true},
		{`h > 1.7976931348623157e+308`, `h > 1.7976931348623157e+308`, true},
		{`h > (1:::float/0:::float)`, `false`, true},
		{`i < ''`, `false`, true},
		{`i > ''`, `i > ''`, true},

		{`a IN (1, 1)`, `a IN (1)`, true},
		{`a IN (2, 3, 1)`, `a IN (1, 2, 3)`, true},
		{`a IN (1, NULL, 2, NULL)`, `a IN (NULL, 1, 2)`, true},
		{`a IN (1, 2) OR a IN (2, 3)`, `a IN (1, 2, 3)`, true},

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
		{`i LIKE 'foo%'`, `(i >= 'foo') AND (i < 'fop')`, false},
		{`i LIKE 'foo_'`, `(i >= 'foo') AND (i < 'fop')`, false},
		{`i LIKE 'bar_foo%'`, `(i >= 'bar') AND (i < 'bas')`, false},
		{`i SIMILAR TO '%'`, `true`, false},
		{`i SIMILAR TO 'foo'`, `i = 'foo'`, false},
		{`i SIMILAR TO 'foo%'`, `(i >= 'foo') AND (i < 'fop')`, false},
		{`i SIMILAR TO '(foo|foobar)%'`, `(i >= 'foo') AND (i < 'fop')`, false},

		{`c IS NULL`, `c IS NULL`, true},
		{`c IS NOT NULL`, `c IS NOT NULL`, true},
		{`c ISNULL`, `c IS NULL`, true},
		{`c NOTNULL`, `c IS NOT NULL`, true},
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
			`(a < 0) AND (b > 0)`, true},
		{`((a < 0) OR (a < 0 OR b > 0)) AND (a > 0 OR a < 1)`,
			`((a < 0) OR (b > 0)) AND (a IS NOT NULL)`, true},

		// Contains mixed date-type comparisons.
		{`n >= DATE '1997-01-01' AND n < (DATE '1997-01-01' + INTERVAL '1' year)`,
			`(n >= '1997-01-01') AND (n < '1998-01-01 00:00:00+00:00')`, true},
		{`o >= DATE '1997-01-01' AND o < (DATE '1997-01-01' + INTERVAL '1' year)`,
			`(o >= '1997-01-01') AND (o < '1998-01-01 00:00:00+00:00')`, true},
		{`p >= DATE '1997-01-01' AND p < (DATE '1997-01-01' + INTERVAL '1' year)`,
			`(p >= '1997-01-01') AND (p < '1998-01-01 00:00:00+00:00')`, true},
	}
	p := makeTestPlanner()
	for _, d := range testData {
		t.Run(d.expr+"~"+d.expected, func(t *testing.T) {
			p.extendedEvalCtx = makeTestingExtendedEvalContext(cluster.MakeTestingClusterSettings())
			defer p.extendedEvalCtx.Stop(context.Background())
			sel := makeSelectNode(t, p)
			// We need to manually close this memory account because we're doing the
			// evals ourselves here.
			defer p.extendedEvalCtx.ActiveMemAcc.Close(context.Background())
			p.extendedEvalCtx.IVarContainer = sel
			expr := parseAndNormalizeExpr(t, p, d.expr, sel)
			expr, equiv := SimplifyExpr(p.EvalContext(), expr)
			if s := expr.String(); d.expected != s {
				t.Errorf("%s: structure: expected %s, but found %s", d.expr, d.expected, s)
			}
			if d.isEquiv != equiv {
				t.Fatalf("%s: equivalence: expected %v, but found %v", d.expr, d.isEquiv, equiv)
			}
		})
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
		{`NOT c`, `c < true`, true, false},
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
		{`NOT i ~ 'foo'`, `true`, false, false},
		{`NOT i !~ 'foo'`, `true`, false, false},
		{`NOT i ~* 'foo'`, `true`, false, false},
		{`NOT i !~* 'foo'`, `true`, false, false},
		{`NOT a IS NULL`, `a IS NOT NULL`, true, true},
		{`NOT a IS NOT NULL`, `a IS NULL`, true, true},
		{`NOT (a != 1 AND b != 1)`, `(a = 1) OR (b = 1)`, true, true},
		{`NOT (a != 1 OR a < 1)`, `a = 1`, true, true},
		{`NOT NOT a = 1`, `a = 1`, true, true},
		{`NOT NOT NOT a = 1`, `a != 1`, true, true},
		{`NOT NOT NOT NOT a = 1`, `a = 1`, true, true},
	}
	p := makeTestPlanner()
	for _, d := range testData {
		t.Run(d.expr+"~"+d.expected, func(t *testing.T) {
			p.extendedEvalCtx = makeTestingExtendedEvalContext(cluster.MakeTestingClusterSettings())
			defer p.extendedEvalCtx.Stop(context.Background())
			sel := makeSelectNode(t, p)
			expr1 := parseAndNormalizeExpr(t, p, d.expr, sel)
			expr2, equiv := SimplifyExpr(p.EvalContext(), expr1)
			if s := expr2.String(); d.expected != s {
				t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
			}
			if d.isEquiv != equiv {
				t.Errorf("%s: expected %v, but found %v", d.expr, d.isEquiv, equiv)
			}
			if d.checkEquiv {
				if err := checkEquivExpr(p.EvalContext(), expr1, expr2, sel); err != nil {
					t.Error(err)
				}
			}
		})
	}
}

func TestSimplifyAndExpr(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		expr     string
		expected string
	}{
		{`a < 1 AND b < 1 AND a < 2 AND b < 2`, `(a < 1) AND (b < 1)`},
		{`(a > 1 AND a < 2) AND (a > 0 AND a < 3)`, `(a > 1) AND (a < 2)`},

		{`a = 1 AND a = 2`, `false`},
		{`a = 1 AND a = NULL`, `false`},
		{`a = 1 AND a != NULL`, `false`},
		{`a = 1 AND b = 1`, `(a = 1) AND (b = 1)`},
	}
	p := makeTestPlanner()
	for _, d := range testData {
		t.Run(d.expr+"~"+d.expected, func(t *testing.T) {
			p.extendedEvalCtx = makeTestingExtendedEvalContext(cluster.MakeTestingClusterSettings())
			defer p.extendedEvalCtx.Stop(context.Background())
			sel := makeSelectNode(t, p)
			expr1 := parseAndNormalizeExpr(t, p, d.expr, sel)
			expr2, equiv := SimplifyExpr(p.EvalContext(), expr1)
			if s := expr2.String(); d.expected != s {
				t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
			}
			if !equiv {
				t.Errorf("%s: expected equivalent, but found %v", d.expr, equiv)
			}
		})
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
		{`a != 1 AND a != 2`, `(a != 1) AND (a != 2)`, true},
		{`a != 2 AND a != 1`, `(a != 2) AND (a != 1)`, true},
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
		{`a != 1 AND a IS NOT NULL`, `(a != 1) AND (a IS NOT NULL)`, true},
		{`a IS NOT NULL AND a != 1`, `(a IS NOT NULL) AND (a != 1)`, true},
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
		{`a > 1 AND a < 2`, `(a > 1) AND (a < 2)`, true},
		{`a > 2 AND a < 1`, `false`, true},
		{`a > 1 AND a <= 1`, `false`, true},
		{`a > 1 AND a <= 2`, `(a > 1) AND (a <= 2)`, true},
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
		{`a > 1 AND a = 2.1`, `a = 2.1`, true},

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
		{`a >= 1 AND a < 2`, `(a >= 1) AND (a < 2)`, true},
		{`a >= 2 AND a < 1`, `false`, true},
		{`a >= 1 AND a <= 1`, `a = 1`, true},
		{`a >= 1 AND a <= 2`, `(a >= 1) AND (a <= 2)`, true},
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
		{`a < 2 AND a > 1`, `(a < 2) AND (a > 1)`, true},
		{`a < 1 AND a >= 1`, `false`, true},
		{`a < 1 AND a >= 2`, `false`, true},
		{`a < 2 AND a >= 1`, `(a < 2) AND (a >= 1)`, true},
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
		{`a <= 2 AND a > 1`, `(a <= 2) AND (a > 1)`, true},
		{`a <= 1 AND a >= 1`, `a = 1`, true},
		{`a <= 1 AND a >= 2`, `false`, true},
		{`a <= 2 AND a >= 1`, `(a <= 2) AND (a >= 1)`, true},
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
	p := makeTestPlanner()
	for _, d := range testData {
		t.Run(d.expr+"~"+d.expected, func(t *testing.T) {
			p.extendedEvalCtx = makeTestingExtendedEvalContext(cluster.MakeTestingClusterSettings())
			defer p.extendedEvalCtx.Stop(context.Background())
			sel := makeSelectNode(t, p)
			expr1 := parseAndNormalizeExpr(t, p, d.expr, sel)
			expr2, equiv := SimplifyExpr(p.EvalContext(), expr1)
			if s := expr2.String(); d.expected != s {
				t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
			}
			if d.checkEquiv != equiv {
				t.Errorf("%s: expected %v, but found %v", d.expr, d.checkEquiv, equiv)
			}
			err := checkEquivExpr(p.EvalContext(), expr1, expr2, sel)
			if d.checkEquiv && err != nil {
				t.Error(err)
				return
			} else if !d.checkEquiv && err == nil {
				t.Errorf("%s: expected not equivalent, but found equivalent", d.expr)
				return
			}

			if _, ok := expr2.(*tree.AndExpr); !ok {
				// The result was not an AND expression. Re-parse to re-resolve names
				// and verify that the analysis is commutative.
				expr1 = parseAndNormalizeExpr(t, p, d.expr, sel)
				andExpr := expr1.(*tree.AndExpr)
				andExpr.Left, andExpr.Right = andExpr.Right, andExpr.Left
				expr3, equiv := SimplifyExpr(p.EvalContext(), andExpr)
				if s := expr3.String(); d.expected != s {
					t.Errorf("%s: expected %s, but found %s", expr1, d.expected, s)
				}
				if d.checkEquiv != equiv {
					t.Errorf("%s: expected %v, but found %v", d.expr, d.checkEquiv, equiv)
				}
			}
		})
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
		{`a = 1 OR b = 1`, `(a = 1) OR (b = 1)`},
	}
	p := makeTestPlanner()
	for _, d := range testData {
		t.Run(d.expr+"~"+d.expected, func(t *testing.T) {
			p.extendedEvalCtx = makeTestingExtendedEvalContext(cluster.MakeTestingClusterSettings())
			defer p.extendedEvalCtx.Stop(context.Background())
			sel := makeSelectNode(t, p)
			expr1 := parseAndNormalizeExpr(t, p, d.expr, sel)
			expr2, _ := SimplifyExpr(p.EvalContext(), expr1)
			if s := expr2.String(); d.expected != s {
				t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
			}
		})
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

		{`a < 1 OR b < 1 OR a < 2 OR b < 2`, `(a < 2) OR (b < 2)`},
		{`(a > 2 OR a < 1) OR (a > 3 OR a < 0)`, `(a > 2) OR (a < 1)`},

		{`a = 1 OR a = 1`, `a = 1`},
		{`a = 1 OR a = 2`, `a IN (1, 2)`},
		{`a = 2 OR a = 1`, `a IN (1, 2)`},
		{`a = 1 OR a != 1`, `a IS NOT NULL`},
		{`a = 1 OR a != 2`, `a != 2`},
		{`a = 2 OR a != 1`, `a != 1`},
		{`a = 1 OR a > 1`, `a >= 1`},
		{`a = 1 OR a > 2`, `(a = 1) OR (a > 2)`},
		{`a = 2 OR a > 1`, `a > 1`},
		{`a = 1 OR a >= 1`, `a >= 1`},
		{`a = 1 OR a >= 2`, `(a = 1) OR (a >= 2)`},
		{`a = 2 OR a >= 1`, `a >= 1`},
		{`a = 1 OR a < 1`, `a <= 1`},
		{`a = 1 OR a < 2`, `a < 2`},
		{`a = 2 OR a < 1`, `(a = 2) OR (a < 1)`},
		{`a = 1 OR a <= 1`, `a <= 1`},
		{`a = 1 OR a <= 2`, `a <= 2`},
		{`a = 2 OR a <= 1`, `(a = 2) OR (a <= 1)`},
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
		{`a > 2 OR a = 1`, `(a > 2) OR (a = 1)`},
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
		{`a > 2 OR a < 1`, `(a > 2) OR (a < 1)`},
		{`a > 1 OR a <= 1`, `a IS NOT NULL`},
		{`a > 1 OR a <= 2`, `a IS NOT NULL`},
		{`a > 2 OR a <= 1`, `(a > 2) OR (a <= 1)`},
		{`a > 1 OR a IN (1)`, `a >= 1`},
		{`a > 1 OR a IN (2)`, `a > 1`},
		{`a > 2 OR a IN (1)`, `(a > 2) OR (a IN (1))`},
		{`a > 1.0 OR a = 1`, `a >= 1`},
		{`a > 1 OR a = 1.0`, `a >= 1`},

		{`a >= 1 OR a = 1`, `a >= 1`},
		{`a >= 1 OR a = 2`, `a >= 1`},
		{`a >= 2 OR a = 1`, `(a >= 2) OR (a = 1)`},
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
		{`a >= 2 OR a < 1`, `(a >= 2) OR (a < 1)`},
		{`a >= 1 OR a <= 1`, `a IS NOT NULL`},
		{`a >= 1 OR a <= 2`, `a IS NOT NULL`},
		{`a >= 2 OR a <= 1`, `(a >= 2) OR (a <= 1)`},
		{`a >= 1 OR a IN (1)`, `a >= 1`},
		{`a >= 1 OR a IN (2)`, `a >= 1`},
		{`a >= 2 OR a IN (1)`, `(a >= 2) OR (a IN (1))`},

		{`a < 1 OR a = 1`, `a <= 1`},
		{`a < 1 OR a = 2`, `(a < 1) OR (a = 2)`},
		{`a < 2 OR a = 1`, `a < 2`},
		{`a < 1 OR a != 1`, `a != 1`},
		{`a < 1 OR a != 2`, `a != 2`},
		{`a < 2 OR a != 1`, `a IS NOT NULL`},
		{`a < 1 OR a > 1`, `a != 1`},
		{`a < 1 OR a > 2`, `(a < 1) OR (a > 2)`},
		{`a < 2 OR a > 1`, `a IS NOT NULL`},
		{`a < 1 OR a >= 1`, `a IS NOT NULL`},
		{`a < 1 OR a >= 2`, `(a < 1) OR (a >= 2)`},
		{`a < 2 OR a >= 1`, `a IS NOT NULL`},
		{`a < 1 OR a < 1`, `a < 1`},
		{`a < 1 OR a < 2`, `a < 2`},
		{`a < 2 OR a < 1`, `a < 2`},
		{`a < 1 OR a <= 1`, `a <= 1`},
		{`a < 1 OR a <= 2`, `a <= 2`},
		{`a < 2 OR a <= 1`, `a < 2`},
		{`a < 1 OR a IN (1)`, `a <= 1`},
		{`a < 1 OR a IN (2)`, `(a < 1) OR (a IN (2))`},
		{`a < 2 OR a IN (1)`, `a < 2`},

		{`a <= 1 OR a = 1`, `a <= 1`},
		{`a <= 1 OR a = 2`, `(a <= 1) OR (a = 2)`},
		{`a <= 2 OR a = 1`, `a <= 2`},
		{`a <= 1 OR a != 1`, `a IS NOT NULL`},
		{`a <= 1 OR a != 2`, `a != 2`},
		{`a <= 2 OR a != 1`, `a IS NOT NULL`},
		{`a <= 1 OR a > 1`, `a IS NOT NULL`},
		{`a <= 1 OR a > 2`, `(a <= 1) OR (a > 2)`},
		{`a <= 2 OR a > 1`, `a IS NOT NULL`},
		{`a <= 1 OR a >= 1`, `a IS NOT NULL`},
		{`a <= 1 OR a >= 2`, `(a <= 1) OR (a >= 2)`},
		{`a <= 2 OR a >= 1`, `a IS NOT NULL`},
		{`a <= 1 OR a < 1`, `a <= 1`},
		{`a <= 1 OR a < 2`, `a < 2`},
		{`a <= 2 OR a < 1`, `a <= 2`},
		{`a <= 1 OR a <= 1`, `a <= 1`},
		{`a <= 1 OR a <= 2`, `a <= 2`},
		{`a <= 2 OR a <= 1`, `a <= 2`},
		{`a <= 1 OR a IN (1)`, `a <= 1`},
		{`a <= 1 OR a IN (2)`, `(a <= 1) OR (a IN (2))`},
		{`a <= 2 OR a IN (1)`, `a <= 2`},

		{`a IN (1) OR a IN (1)`, `a IN (1)`},
		{`a IN (1) OR a IN (2)`, `a IN (1, 2)`},
		{`a IN (1) OR a IN (1, 2, 3, 4, 5)`, `a IN (1, 2, 3, 4, 5)`},
		{`a IN (4, 2) OR a IN (5, 4, 3, 2, 1)`, `a IN (1, 2, 3, 4, 5)`},
		{`a IN (1) OR a IS NULL`, `(a IN (1)) OR (a IS NULL)`},

		{`a IS NULL OR a IS NULL`, `a IS NULL`},
		{`a IS NOT NULL OR a IS NOT NULL`, `a IS NOT NULL`},
		{`a IS NULL OR a IS NOT NULL`, `true`},
	}
	p := makeTestPlanner()
	for _, d := range testData {
		t.Run(d.expr+"~"+d.expected, func(t *testing.T) {
			p.extendedEvalCtx = makeTestingExtendedEvalContext(cluster.MakeTestingClusterSettings())
			defer p.extendedEvalCtx.Stop(context.Background())
			sel := makeSelectNode(t, p)
			expr1 := parseAndNormalizeExpr(t, p, d.expr, sel)
			expr2, equiv := SimplifyExpr(p.EvalContext(), expr1)
			if s := expr2.String(); d.expected != s {
				t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
			}
			if !equiv {
				t.Errorf("%s: expected equivalent, but found %v", d.expr, equiv)
			}

			if err := checkEquivExpr(p.EvalContext(), expr1, expr2, sel); err != nil {
				t.Error(err)
				return
			}

			if _, ok := expr2.(*tree.OrExpr); !ok {
				// The result was not an OR expression. Re-parse to re-resolve names
				// and verify that the analysis is commutative.
				expr1 = parseAndNormalizeExpr(t, p, d.expr, sel)
				orExpr := expr1.(*tree.OrExpr)
				orExpr.Left, orExpr.Right = orExpr.Right, orExpr.Left
				expr3, equiv := SimplifyExpr(p.EvalContext(), orExpr)
				if s := expr3.String(); d.expected != s {
					t.Errorf("%s: expected %s, but found %s", expr1, d.expected, s)
				}
				if !equiv {
					t.Errorf("%s: expected equivalent, but found %v", d.expr, equiv)
				}
			}
		})
	}
}
