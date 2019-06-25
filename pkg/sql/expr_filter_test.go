// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

type countVarsVisitor struct {
	numNames, numValues int
}

func (v *countVarsVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	switch expr.(type) {
	case *tree.IndexedVar:
		v.numValues++
	case *tree.ColumnItem:
		v.numNames++
	}

	return true, expr
}

func (*countVarsVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }

// countVars counts how many *ColumnItems and *IndexedVar nodes are in an expression.
func countVars(expr tree.Expr) (numNames, numValues int) {
	v := countVarsVisitor{}
	if expr != nil {
		tree.WalkExprConst(&v, expr)
	}
	return v.numNames, v.numValues
}

func TestSplitFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// In each testcase, we are splitting the filter in expr according to the set of variables in
	// vars.
	//
	// The "restricted" expression is an expression that is true whenever the filter expression is
	// true (for any setting of variables) but only references variables in the vars set.
	//
	// The "remainder" expression must ensure that the conjunction (AND) between itself and the
	// restricted expression is equivalent to the original expr filter.
	testData := []struct {
		expr        string
		vars        []string
		expectedRes string
		expectedRem string
	}{
		{`a = 1`, []string{""}, `<nil>`, `a = 1`},
		{`a = 1`, []string{"a"}, `a = 1`, `<nil>`},

		{`NOT a = 1`, []string{""}, `<nil>`, `NOT (a = 1)`},
		{`NOT a = 1`, []string{"a"}, `NOT (a = 1)`, `<nil>`},

		{`a = 1 AND b = 5`, []string{""}, `<nil>`, `(a = 1) AND (b = 5)`},
		{`a = 1 AND b = 5`, []string{"a"}, `a = 1`, `b = 5`},
		{`a = 1 AND b = 5`, []string{"a", "b"}, `(a = 1) AND (b = 5)`, `<nil>`},

		{`NOT (a = 1 AND b = 5)`, []string{""}, `<nil>`, `NOT ((a = 1) AND (b = 5))`},
		{`NOT (a = 1 AND b = 5)`, []string{"a"}, `<nil>`, `NOT ((a = 1) AND (b = 5))`},
		{`NOT (a = 1 AND b = 5)`, []string{"a", "b"}, `NOT ((a = 1) AND (b = 5))`, `<nil>`},

		{`a = 1 OR b = 5`, []string{""}, `<nil>`, `(a = 1) OR (b = 5)`},
		{`a = 1 OR b = 5`, []string{"a"}, `<nil>`, `(a = 1) OR (b = 5)`},
		{`a = 1 OR b = 5`, []string{"a", "b"}, `(a = 1) OR (b = 5)`, `<nil>`},

		{`NOT (a = 1 OR b = 5)`, []string{""}, `<nil>`, `NOT ((a = 1) OR (b = 5))`},
		{`NOT (a = 1 OR b = 5)`, []string{"a"}, `NOT (a = 1)`, `NOT (b = 5)`},
		{`NOT (a = 1 OR b = 5)`, []string{"a", "b"}, `NOT ((a = 1) OR (b = 5))`, `<nil>`},

		{`(a = 1 AND b = 5) OR (a = 2 AND b = 6)`,
			[]string{"a"},
			`(a = 1) OR (a = 2)`,
			`((a = 1) AND (b = 5)) OR ((a = 2) AND (b = 6))`,
		},
		{`(NOT (a = 1 AND b = 5)) OR (a = 2 AND b = 6)`,
			[]string{"a"},
			`<nil>`,
			`(NOT ((a = 1) AND (b = 5))) OR ((a = 2) AND (b = 6))`,
		},
		{`NOT ((a = 1 AND b = 5) OR (a = 2 AND b = 6))`,
			[]string{"a", "b"},
			`NOT (((a = 1) AND (b = 5)) OR ((a = 2) AND (b = 6)))`,
			`<nil>`,
		},

		{`a < b`, []string{"a"}, `<nil>`, `a < b`},
		{`a < b`, []string{"a", "a", "b"}, `a < b`, `<nil>`},

		{`a < b AND a < j`, []string{"a", "b"}, `a < b`, `a < j`},
		{`a + b = 1 AND a + j = 2`, []string{"a", "b"}, `(a + b) = 1`, `(a + j) = 2`},

		{`a IN (1, b) AND c`, []string{"a"}, `<nil>`, `(a IN (1, b)) AND c`},
		{`a IN (1, b) AND c`, []string{"a", "b"}, `a IN (1, b)`, `c`},
		{`a IN (1, b) AND c`, []string{"c"}, `c`, `a IN (1, b)`},

		{`(a, b, j) = (1, 2, 3)`, []string{"b"}, `b = 2`, `(a, b, j) = (1, 2, 3)`},
		{`(a, b, j) = (1, 2, 3)`, []string{"a", "j"}, `(a, j) = (1, 3)`, `(a, b, j) = (1, 2, 3)`},

		{`(a, b, j) != (1, 2, 3)`, []string{"b"}, `<nil>`, `(a, b, j) != (1, 2, 3)`},
		{`(a, b, j) != (1, 2, 3)`, []string{"a", "j"}, `<nil>`, `(a, b, j) != (1, 2, 3)`},

		{`(a, b, j) < (1, 2, 3)`, []string{"a"}, `a <= 1`, `(a, b, j) < (1, 2, 3)`},
		{`(a, b, j) < (1, 2, 3)`, []string{"a", "b"}, `(a, b) <= (1, 2)`, `(a, b, j) < (1, 2, 3)`},
		{`(a, b, j) < (1, 2, 3)`, []string{"b"}, `<nil>`, `(a, b, j) < (1, 2, 3)`},

		{`(a, b, j) <= (1, 2, 3)`, []string{"a"}, `a <= 1`, `(a, b, j) <= (1, 2, 3)`},
		{`(a, b, j) <= (1, 2, 3)`, []string{"a", "b"}, `(a, b) <= (1, 2)`, `(a, b, j) <= (1, 2, 3)`},
		{`(a, b, j) <= (1, 2, 3)`, []string{"b"}, `<nil>`, `(a, b, j) <= (1, 2, 3)`},

		{`(a, b, j) > (1, 2, 3)`, []string{"a"}, `a >= 1`, `(a, b, j) > (1, 2, 3)`},
		{`(a, b, j) > (1, 2, 3)`, []string{"a", "b"}, `(a, b) >= (1, 2)`, `(a, b, j) > (1, 2, 3)`},
		{`(a, b, j) > (1, 2, 3)`, []string{"b"}, `<nil>`, `(a, b, j) > (1, 2, 3)`},

		{`(a, b, j) >= (1, 2, 3)`, []string{"a"}, `a >= 1`, `(a, b, j) >= (1, 2, 3)`},
		{`(a, b, j) >= (1, 2, 3)`, []string{"a", "b"}, `(a, b) >= (1, 2)`, `(a, b, j) >= (1, 2, 3)`},
		{`(a, b, j) >= (1, 2, 3)`, []string{"b"}, `<nil>`, `(a, b, j) >= (1, 2, 3)`},

		{`NOT ((a, b, j) = (1, 2, 3))`, []string{"b"}, `<nil>`, `NOT ((a, b, j) = (1, 2, 3))`},
		{`NOT ((a, b, j) = (1, 2, 3))`, []string{"a", "j"}, `<nil>`, `NOT ((a, b, j) = (1, 2, 3))`},

		{`NOT ((a, b, j) != (1, 2, 3))`, []string{"b"}, `NOT (b != 2)`, `NOT ((a, b, j) != (1, 2, 3))`},
		{`NOT ((a, b, j) != (1, 2, 3))`, []string{"a", "j"}, `NOT ((a, j) != (1, 3))`, `NOT ((a, b, j) != (1, 2, 3))`},

		{`NOT ((a, b, j) < (1, 2, 3))`, []string{"a"}, `NOT (a < 1)`, `NOT ((a, b, j) < (1, 2, 3))`},
		{`NOT ((a, b, j) < (1, 2, 3))`, []string{"a", "b"}, `NOT ((a, b) < (1, 2))`, `NOT ((a, b, j) < (1, 2, 3))`},
		{`NOT ((a, b, j) < (1, 2, 3))`, []string{"b"}, `<nil>`, `NOT ((a, b, j) < (1, 2, 3))`},

		{`NOT ((a, b, j) <= (1, 2, 3))`, []string{"a"}, `NOT (a < 1)`, `NOT ((a, b, j) <= (1, 2, 3))`},
		{`NOT ((a, b, j) <= (1, 2, 3))`, []string{"a", "b"}, `NOT ((a, b) < (1, 2))`, `NOT ((a, b, j) <= (1, 2, 3))`},
		{`NOT ((a, b, j) <= (1, 2, 3))`, []string{"b"}, `<nil>`, `NOT ((a, b, j) <= (1, 2, 3))`},

		{`NOT ((a, b, j) > (1, 2, 3))`, []string{"a"}, `NOT (a > 1)`, `NOT ((a, b, j) > (1, 2, 3))`},
		{`NOT ((a, b, j) > (1, 2, 3))`, []string{"a", "b"}, `NOT ((a, b) > (1, 2))`, `NOT ((a, b, j) > (1, 2, 3))`},
		{`NOT ((a, b, j) > (1, 2, 3))`, []string{"b"}, `<nil>`, `NOT ((a, b, j) > (1, 2, 3))`},

		{`NOT ((a, b, j) >= (1, 2, 3))`, []string{"a"}, `NOT (a > 1)`, `NOT ((a, b, j) >= (1, 2, 3))`},
		{`NOT ((a, b, j) >= (1, 2, 3))`, []string{"a", "b"}, `NOT ((a, b) > (1, 2))`, `NOT ((a, b, j) >= (1, 2, 3))`},
		{`NOT ((a, b, j) >= (1, 2, 3))`, []string{"b"}, `<nil>`, `NOT ((a, b, j) >= (1, 2, 3))`},

		{`(a, b, j, h) < (1, 2, 3, 4.0)`, []string{"a"}, `a <= 1`, `(a, b, j, h) < (1, 2, 3, 4.0)`},
		{`(a, b, j, h) < (1, 2, 3, 4.0)`, []string{"a", "j"}, `a <= 1`, `(a, b, j, h) < (1, 2, 3, 4.0)`},
		{`(a, b, j, h) < (1, 2, 3, 4.0)`, []string{"a", "b", "j"}, `(a, b, j) <= (1, 2, 3)`, `(a, b, j, h) < (1, 2, 3, 4.0)`},
		{`(a, b, j, h) < (1, 2, 3, 4.0)`, []string{"a", "b", "h"}, `(a, b) <= (1, 2)`, `(a, b, j, h) < (1, 2, 3, 4.0)`},

		// Regression tests for #21243.
		{
			`((a, b, j) = (1, 2, 6)) AND (h > 8.0)`,
			[]string{"a", "b"},
			`(a, b) = (1, 2)`,
			`((a, b, j) = (1, 2, 6)) AND (h > 8.0)`,
		},
		{
			`(((a, b) > (1, 2)) OR (((a, b) = (1, 2)) AND (j < 6))) OR (((a, b, j) = (1, 2, 6)) AND (h > 8.0))`,
			[]string{"a", "b"},
			`(((a, b) > (1, 2)) OR ((a, b) = (1, 2))) OR ((a, b) = (1, 2))`,
			`(((a, b) > (1, 2)) OR (((a, b) = (1, 2)) AND (j < 6))) OR (((a, b, j) = (1, 2, 6)) AND (h > 8.0))`,
		},
	}

	p := makeTestPlanner()
	for _, d := range testData {
		t.Run(fmt.Sprintf("%s~(%s, %s)", d.expr, d.expectedRes, d.expectedRem), func(t *testing.T) {
			p.extendedEvalCtx = makeTestingExtendedEvalContext(cluster.MakeTestingClusterSettings())
			defer p.extendedEvalCtx.Stop(context.Background())
			sel := makeSelectNode(t, p)
			// A function that "converts" only vars in the list.
			conv := func(expr tree.VariableExpr) (bool, tree.Expr) {
				iv := expr.(*tree.IndexedVar)
				colName := iv.String()
				for _, col := range d.vars {
					if colName == col {
						// Convert to a VarName (to check that conversion happens correctly). It
						// will print the same.
						return true, &tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{colName}}
					}
				}
				return false, nil
			}
			expr := parseAndNormalizeExpr(t, p, d.expr, sel)
			exprStr := expr.String()
			res, rem := splitFilter(expr, conv)
			// We use Sprint to handle the 'nil' case correctly.
			resStr := fmt.Sprint(res)
			remStr := fmt.Sprint(rem)
			if resStr != d.expectedRes || remStr != d.expectedRem {
				t.Errorf("`%s` split along (%s): expected:\n   `%s`,`%s`\ngot:\n   `%s`,`%s`",
					d.expr, strings.Join(d.vars, ","), d.expectedRes, d.expectedRem, resStr, remStr)
			}
			_, numVals := countVars(res)
			if numVals != 0 {
				t.Errorf("`%s` split along (%s): resulting expression `%s` has unconverted IndexedVars!",
					d.expr, strings.Join(d.vars, ","), resStr)
			}
			numNames, _ := countVars(rem)
			if numNames != 0 {
				t.Errorf("`%s` split along (%s): remainder expressions `%s` has converted IndexedVars!",
					d.expr, strings.Join(d.vars, ","), remStr)
			}
			// Verify the original expression didn't change.
			if exprStr != expr.String() {
				t.Errorf("Expression changed after splitFilter; before: `%s` after: `%s`",
					exprStr, expr.String())
			}
		})
	}
}

func TestExtractNotNullConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Note: see testTableDesc for the variable schema.
	testCases := []struct {
		expr        string
		notNullVars []int
	}{
		{`c`, []int{2}},
		{`NOT c`, []int{2}},
		{`a = 1`, []int{0}},
		{`a IS NULL`, []int{}},
		{`a IS NOT NULL`, []int{0}},
		{`NOT (a = 1)`, []int{0}},
		{`NOT (a IS NULL)`, []int{}}, // We could do better here.
		{`NOT (a IS NOT NULL)`, []int{}},
		{`(a IS NOT NULL) AND (b IS NOT NULL)`, []int{0, 1}},
		{`(a IS NOT NULL) OR (b IS NOT NULL)`, []int{}},
		{`(a IS NOT NULL) OR (a IS NULL)`, []int{}},
		{`a > b`, []int{0, 1}},
		{`a = b`, []int{0, 1}},
		{`a + b > 1`, []int{0, 1}},
		{`c OR d`, []int{}}, // `NULL OR true` and `true OR NULL` are true.
		{`a = 1 AND b = 2`, []int{0, 1}},
		{`a = 1 OR b = 2`, []int{}},
		{`(a = 1 AND b = 2) OR (b = 3 AND j = 4)`, []int{1}},
		{`(a, b) = (1, 2)`, []int{0, 1}},
		{`(a, b) > (1, 2)`, []int{}},
		{`a IN (b,j)`, []int{0}},
		{`(a, b) IN ((1,j))`, []int{0, 1}},
		{`a NOT IN (b,j)`, []int{0}},
		{`(a, b) NOT IN ((1,j))`, []int{0, 1}},
		{`(a + b, 1 + j) IN ((1, 2), (3, 4))`, []int{0, 1, 9}},
	}

	p := makeTestPlanner()
	for _, tc := range testCases {
		t.Run(tc.expr, func(t *testing.T) {
			p.extendedEvalCtx = makeTestingExtendedEvalContext(cluster.MakeTestingClusterSettings())
			defer p.extendedEvalCtx.Stop(context.Background())

			sel := makeSelectNode(t, p)
			expr := parseAndNormalizeExpr(t, p, tc.expr, sel)
			result := extractNotNullConstraints(expr)
			var expected util.FastIntSet
			for _, v := range tc.notNullVars {
				expected.Add(v)
			}
			if !result.Equals(expected) {
				t.Errorf("expected %s, got %s", &expected, result)
			}
		})
	}
}
