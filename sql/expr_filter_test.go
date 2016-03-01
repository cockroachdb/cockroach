// Copyright 2016 The Cockroach Authors.
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
// Author: Radu Berinde (radu@cockroachlabs.com)

package sql

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

type countVarsVisitor struct {
	numQNames, numQValues int
}

func (v *countVarsVisitor) VisitPre(expr parser.Expr) (recurse bool, newExpr parser.Expr) {
	switch expr.(type) {
	case *qvalue:
		v.numQValues++
	case *parser.QualifiedName:
		v.numQNames++
	}

	return true, expr
}

func (*countVarsVisitor) VisitPost(expr parser.Expr) parser.Expr { return expr }

// countVars counts how many *QualifiedName and *qvalue nodes are in an expression.
func countVars(expr parser.Expr) (numQNames, numQValues int) {
	v := countVarsVisitor{}
	if expr != nil {
		parser.WalkExprConst(&v, expr)
	}
	return v.numQNames, v.numQValues
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

		{`NOT a = 1`, []string{""}, `<nil>`, `NOT a = 1`},
		{`NOT a = 1`, []string{"a"}, `NOT a = 1`, `<nil>`},

		{`a = 1 AND b = 5`, []string{""}, `<nil>`, `a = 1 AND b = 5`},
		{`a = 1 AND b = 5`, []string{"a"}, `a = 1`, `b = 5`},
		{`a = 1 AND b = 5`, []string{"a", "b"}, `a = 1 AND b = 5`, `<nil>`},

		{`NOT (a = 1 AND b = 5)`, []string{""}, `<nil>`, `NOT a = 1 AND b = 5`},
		{`NOT (a = 1 AND b = 5)`, []string{"a"}, `<nil>`, `NOT a = 1 AND b = 5`},
		{`NOT (a = 1 AND b = 5)`, []string{"a", "b"}, `NOT a = 1 AND b = 5`, `<nil>`},

		{`a = 1 OR b = 5`, []string{""}, `<nil>`, `a = 1 OR b = 5`},
		{`a = 1 OR b = 5`, []string{"a"}, `<nil>`, `a = 1 OR b = 5`},
		{`a = 1 OR b = 5`, []string{"a", "b"}, `a = 1 OR b = 5`, `<nil>`},

		{`NOT (a = 1 OR b = 5)`, []string{""}, `<nil>`, `NOT a = 1 OR b = 5`},
		{`NOT (a = 1 OR b = 5)`, []string{"a"}, `NOT a = 1`, `NOT b = 5`},
		{`NOT (a = 1 OR b = 5)`, []string{"a", "b"}, `NOT a = 1 OR b = 5`, `<nil>`},

		{`(a = 1 AND b = 5) OR (a = 2 AND b = 6)`,
			[]string{"a"},
			`a = 1 OR a = 2`,
			`a = 1 AND b = 5 OR a = 2 AND b = 6`,
		},
		{`(NOT (a = 1 AND b = 5)) OR (a = 2 AND b = 6)`,
			[]string{"a"},
			`<nil>`,
			`NOT a = 1 AND b = 5 OR a = 2 AND b = 6`,
		},
		{`NOT ((a = 1 AND b = 5) OR (a = 2 AND b = 6))`,
			[]string{"a", "b"},
			`NOT a = 1 AND b = 5 OR a = 2 AND b = 6`,
			`<nil>`,
		},

		{`a < b`, []string{"a"}, `<nil>`, `a < b`},
		{`a < b`, []string{"a", "a", "b"}, `a < b`, `<nil>`},

		{`a < b AND a < j`, []string{"a", "b"}, `a < b`, `a < j`},
		{`a + b = 1 AND a + j = 2`, []string{"a", "b"}, `a + b = 1`, `a + j = 2`},

		{`a IN (1, b) AND c`, []string{"a"}, `<nil>`, `a IN (1, b) AND c`},
		{`a IN (1, b) AND c`, []string{"a", "b"}, `a IN (1, b)`, `c`},
		{`a IN (1, b) AND c`, []string{"c"}, `c`, `a IN (1, b)`},
	}

	for _, d := range testData {
		// A function that "converts" only vars in the list.
		conv := func(expr parser.VariableExpr) (bool, parser.VariableExpr) {
			q := expr.(*qvalue)
			colName := q.colRef.get().Name
			for _, col := range d.vars {
				if colName == col {
					// Convert to a QualifiedName (to check that conversion happens correctly). It
					// will print the same.
					return true, &parser.QualifiedName{Base: parser.Name(colName)}
				}
			}
			return false, nil
		}
		expr, _ := parseAndNormalizeExpr(t, d.expr)
		exprStr := expr.String()
		res, rem := splitFilter(expr, conv)
		// We use Sprint to handle the 'nil' case correctly.
		resStr := fmt.Sprint(res)
		remStr := fmt.Sprint(rem)
		if testing.Verbose() {
			fmt.Printf("Expr `%s` split along (%s): `%s`,`%s`\n",
				expr, strings.Join(d.vars, ","), resStr, remStr)
		}
		if resStr != d.expectedRes || remStr != d.expectedRem {
			t.Errorf("`%s` split along (%s): expected:\n   `%s`,`%s`\ngot:\n   `%s`,`%s`",
				d.expr, strings.Join(d.vars, ","), d.expectedRes, d.expectedRem, resStr, remStr)
		}
		_, numQVals := countVars(res)
		if numQVals != 0 {
			t.Errorf("`%s` split along (%s): resulting expression `%s` has unconverted qvalues!",
				d.expr, strings.Join(d.vars, ","), resStr)
		}
		numQNames, _ := countVars(rem)
		if numQNames != 0 {
			t.Errorf("`%s` split along (%s): remainder expressions `%s` has converted qvalues!",
				d.expr, strings.Join(d.vars, ","), remStr)
		}
		// Verify the original expression didn't change.
		if exprStr != expr.String() {
			t.Errorf("Expression changed after splitFilter; before: `%s` after: `%s`",
				exprStr, expr.String())
		}
	}
}
