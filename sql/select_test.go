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

func makeTestIndex(t *testing.T, columns []string) (*TableDescriptor, *IndexDescriptor) {
	desc := testTableDesc()
	desc.Indexes = append(desc.Indexes, IndexDescriptor{
		Name:        "foo",
		ColumnNames: columns,
	})
	if err := desc.AllocateIDs(); err != nil {
		t.Fatal(err)
	}
	return desc, &desc.Indexes[len(desc.Indexes)-1]
}

func makeConstraints(t *testing.T, sql string, desc *TableDescriptor,
	index *IndexDescriptor) (indexConstraints, parser.Expr) {
	expr, _ := parseAndNormalizeExpr(t, sql)
	exprs, equiv := analyzeExpr(expr)

	c := &indexInfo{
		desc:     desc,
		index:    index,
		covering: true,
	}
	c.analyzeExprs(exprs)
	if equiv && len(exprs) == 1 {
		expr = joinAndExprs(exprs[0])
	}
	return c.constraints, expr
}

func TestMakeConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)

	testData := []struct {
		expr     string
		columns  []string
		expected string
	}{
		{`a = 1`, []string{"b"}, `[]`},
		{`a = 1`, []string{"a"}, `[a = 1]`},
		{`a != 1`, []string{"a"}, `[a != 1]`},
		{`a > 1`, []string{"a"}, `[a >= 2]`},
		{`a >= 1`, []string{"a"}, `[a >= 1]`},
		{`a < 1`, []string{"a"}, `[a < 1]`},
		{`a <= 1`, []string{"a"}, `[a <= 1]`},

		{`a IN (1,2,3)`, []string{"a"}, `[a IN (1, 2, 3)]`},
		{`a IN (1,2,3) AND b = 1`, []string{"a", "b"}, `[a IN (1, 2, 3), b = 1]`},
		{`a = 1 AND b IN (1,2,3)`, []string{"a", "b"}, `[a = 1, b IN (1, 2, 3)]`},

		// Prefer EQ over IN.
		//
		// TODO(pmattis): We could conceivably propagate the "a = 1" down to the IN
		// expression and simplify. Doesn't seem worth it at this time.
		{`a = 1 AND (a, b) IN ((1, 2))`, []string{"a", "b"}, `[a = 1]`},
		{`(a, b) IN ((1, 2)) AND a = 1`, []string{"a", "b"}, `[a = 1]`},

		{`a = 1 AND b = 1`, []string{"a", "b"}, `[a = 1, b = 1]`},
		{`a = 1 AND b != 1`, []string{"a", "b"}, `[a = 1, b != 1]`},
		{`a = 1 AND b > 1`, []string{"a", "b"}, `[a = 1, b >= 2]`},
		{`a = 1 AND b >= 1`, []string{"a", "b"}, `[a = 1, b >= 1]`},
		{`a = 1 AND b < 1`, []string{"a", "b"}, `[a = 1, b < 1]`},
		{`a = 1 AND b <= 1`, []string{"a", "b"}, `[a = 1, b <= 1]`},

		{`a != 1 AND b = 1`, []string{"a", "b"}, `[a != 1, b = 1]`},
		{`a != 1 AND b != 1`, []string{"a", "b"}, `[a != 1, b != 1]`},
		{`a != 1 AND b > 1`, []string{"a", "b"}, `[a != 1, b >= 2]`},
		{`a != 1 AND b >= 1`, []string{"a", "b"}, `[a != 1, b >= 1]`},
		{`a != 1 AND b < 1`, []string{"a", "b"}, `[a != 1]`},
		{`a != 1 AND b <= 1`, []string{"a", "b"}, `[a != 1]`},

		{`a > 1 AND b = 1`, []string{"a", "b"}, `[a >= 2, b = 1]`},
		{`a > 1 AND b != 1`, []string{"a", "b"}, `[a >= 2, b != 1]`},
		{`a > 1 AND b > 1`, []string{"a", "b"}, `[a >= 2, b >= 2]`},
		{`a > 1 AND b >= 1`, []string{"a", "b"}, `[a >= 2, b >= 1]`},
		{`a > 1 AND b < 1`, []string{"a", "b"}, `[a >= 2]`},
		{`a > 1 AND b <= 1`, []string{"a", "b"}, `[a >= 2]`},

		{`a >= 1 AND b = 1`, []string{"a", "b"}, `[a >= 1, b = 1]`},
		{`a >= 1 AND b != 1`, []string{"a", "b"}, `[a >= 1, b != 1]`},
		{`a >= 1 AND b > 1`, []string{"a", "b"}, `[a >= 1, b >= 2]`},
		{`a >= 1 AND b >= 1`, []string{"a", "b"}, `[a >= 1, b >= 1]`},
		{`a >= 1 AND b < 1`, []string{"a", "b"}, `[a >= 1]`},
		{`a >= 1 AND b <= 1`, []string{"a", "b"}, `[a >= 1]`},

		{`a < 1 AND b = 1`, []string{"a", "b"}, `[a < 1]`},
		{`a < 1 AND b != 1`, []string{"a", "b"}, `[a < 1]`},
		{`a < 1 AND b > 1`, []string{"a", "b"}, `[a < 1]`},
		{`a < 1 AND b >= 1`, []string{"a", "b"}, `[a < 1]`},
		{`a < 1 AND b < 1`, []string{"a", "b"}, `[a < 1]`},
		{`a < 1 AND b <= 1`, []string{"a", "b"}, `[a < 1]`},

		{`a <= 1 AND b = 1`, []string{"a", "b"}, `[a <= 1, b = 1]`},
		{`a <= 1 AND b != 1`, []string{"a", "b"}, `[a <= 1]`},
		{`a <= 1 AND b > 1`, []string{"a", "b"}, `[a <= 1]`},
		{`a <= 1 AND b >= 1`, []string{"a", "b"}, `[a <= 1]`},
		{`a <= 1 AND b < 1`, []string{"a", "b"}, `[a <= 1, b < 1]`},
		{`a <= 1 AND b <= 1`, []string{"a", "b"}, `[a <= 1, b <= 1]`},

		{`a IN (1) AND b = 1`, []string{"a", "b"}, `[a IN (1), b = 1]`},
		{`a IN (1) AND b != 1`, []string{"a", "b"}, `[a IN (1), b != 1]`},
		{`a IN (1) AND b > 1`, []string{"a", "b"}, `[a IN (1), b >= 2]`},
		{`a IN (1) AND b >= 1`, []string{"a", "b"}, `[a IN (1), b >= 1]`},
		{`a IN (1) AND b < 1`, []string{"a", "b"}, `[a IN (1), b < 1]`},
		{`a IN (1) AND b <= 1`, []string{"a", "b"}, `[a IN (1), b <= 1]`},

		{`(a, b) IN ((1, 2))`, []string{"a", "b"}, `[(a, b) IN ((1, 2))]`},
		{`(b, a) IN ((1, 2))`, []string{"a", "b"}, `[(b, a) IN ((1, 2))]`},
		{`(b, a) IN ((1, 2))`, []string{"a"}, `[(b, a) IN ((1, 2))]`},

		{`a IS NULL`, []string{"a"}, `[a IS NULL]`},
		{`a IS NOT NULL`, []string{"a"}, `[a IS NOT NULL]`},
	}
	for _, d := range testData {
		desc, index := makeTestIndex(t, d.columns)
		constraints, _ := makeConstraints(t, d.expr, desc, index)
		if s := constraints.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
	}
}

func TestMakeSpans(t *testing.T) {
	defer leaktest.AfterTest(t)

	testData := []struct {
		expr     string
		columns  []string
		expected string
	}{
		{`a = 1`, []string{"a"}, `/1-/2`},
		{`a != 1`, []string{"a"}, `/#-`},
		{`a > 1`, []string{"a"}, `/2-`},
		{`a >= 1`, []string{"a"}, `/1-`},
		{`a < 1`, []string{"a"}, `/#-/1`},
		{`a <= 1`, []string{"a"}, `/#-/2`},
		{`a IS NULL`, []string{"a"}, `-/#`},
		{`a IS NOT NULL`, []string{"a"}, `/#-`},

		{`a IN (1,2,3)`, []string{"a"}, `/1-/2 /2-/3 /3-/4`},
		{`a IN (1,2,3) AND b = 1`, []string{"a", "b"}, `/1/1-/1/2 /2/1-/2/2 /3/1-/3/2`},
		{`a = 1 AND b IN (1,2,3)`, []string{"a", "b"}, `/1/1-/1/2 /1/2-/1/3 /1/3-/1/4`},
		{`a >= 1 AND b IN (1,2,3)`, []string{"a", "b"}, `/1-`},
		{`a <= 1 AND b IN (1,2,3)`, []string{"a", "b"}, `/#-/2`},
		{`(a, b) IN ((1, 2), (3, 4))`, []string{"a", "b"}, `/1/2-/1/3 /3/4-/3/5`},
		{`(b, a) IN ((1, 2), (3, 4))`, []string{"a", "b"}, `/2/1-/2/2 /4/3-/4/4`},
		{`(a, b) IN ((1, 2), (3, 4))`, []string{"b"}, `/2-/3 /4-/5`},

		{`a = 1 AND b = 1`, []string{"a", "b"}, `/1/1-/1/2`},
		{`a = 1 AND b != 1`, []string{"a", "b"}, `/1/#-/2`},
		{`a = 1 AND b > 1`, []string{"a", "b"}, `/1/2-/2`},
		{`a = 1 AND b >= 1`, []string{"a", "b"}, `/1/1-/2`},
		{`a = 1 AND b < 1`, []string{"a", "b"}, `/1/#-/1/1`},
		{`a = 1 AND b <= 1`, []string{"a", "b"}, `/1/#-/1/2`},
		{`a = 1 AND b IS NULL`, []string{"a", "b"}, `/1-/1/#`},
		{`a = 1 AND b IS NOT NULL`, []string{"a", "b"}, `/1/#-/2`},

		{`a != 1 AND b = 1`, []string{"a", "b"}, `/#/1-`},
		{`a != 1 AND b != 1`, []string{"a", "b"}, `/#/#-`},
		{`a != 1 AND b > 1`, []string{"a", "b"}, `/#/2-`},
		{`a != 1 AND b >= 1`, []string{"a", "b"}, `/#/1-`},
		{`a != 1 AND b < 1`, []string{"a", "b"}, `/#-`},
		{`a != 1 AND b <= 1`, []string{"a", "b"}, `/#-`},
		{`a != 1 AND b IS NULL`, []string{"a", "b"}, `/#-`},
		{`a != 1 AND b IS NOT NULL`, []string{"a", "b"}, `/#/#-`},

		{`a > 1 AND b = 1`, []string{"a", "b"}, `/2/1-`},
		{`a > 1 AND b != 1`, []string{"a", "b"}, `/2/#-`},
		{`a > 1 AND b > 1`, []string{"a", "b"}, `/2/2-`},
		{`a > 1 AND b >= 1`, []string{"a", "b"}, `/2/1-`},
		{`a > 1 AND b < 1`, []string{"a", "b"}, `/2-`},
		{`a > 1 AND b <= 1`, []string{"a", "b"}, `/2-`},
		{`a > 1 AND b IS NULL`, []string{"a", "b"}, `/2-`},
		{`a > 1 AND b IS NOT NULL`, []string{"a", "b"}, `/2/#-`},

		{`a >= 1 AND b = 1`, []string{"a", "b"}, `/1/1-`},
		{`a >= 1 AND b != 1`, []string{"a", "b"}, `/1/#-`},
		{`a >= 1 AND b > 1`, []string{"a", "b"}, `/1/2-`},
		{`a >= 1 AND b >= 1`, []string{"a", "b"}, `/1/1-`},
		{`a >= 1 AND b < 1`, []string{"a", "b"}, `/1-`},
		{`a >= 1 AND b <= 1`, []string{"a", "b"}, `/1-`},
		{`a >= 1 AND b IS NULL`, []string{"a", "b"}, `/1-`},
		{`a >= 1 AND b IS NOT NULL`, []string{"a", "b"}, `/1/#-`},

		{`a < 1 AND b = 1`, []string{"a", "b"}, `/#-/1`},
		{`a < 1 AND b != 1`, []string{"a", "b"}, `/#-/1`},
		{`a < 1 AND b > 1`, []string{"a", "b"}, `/#-/1`},
		{`a < 1 AND b >= 1`, []string{"a", "b"}, `/#-/1`},
		{`a < 1 AND b < 1`, []string{"a", "b"}, `/#-/1`},
		{`a < 1 AND b <= 1`, []string{"a", "b"}, `/#-/1`},
		{`a < 1 AND b IS NULL`, []string{"a", "b"}, `/#-/1`},
		{`a < 1 AND b IS NOT NULL`, []string{"a", "b"}, `/#-/1`},

		{`a <= 1 AND b = 1`, []string{"a", "b"}, `/#-/1/2`},
		{`a <= 1 AND b != 1`, []string{"a", "b"}, `/#-/2`},
		{`a <= 1 AND b > 1`, []string{"a", "b"}, `/#-/2`},
		{`a <= 1 AND b >= 1`, []string{"a", "b"}, `/#-/2`},
		{`a <= 1 AND b < 1`, []string{"a", "b"}, `/#-/1/1`},
		{`a <= 1 AND b <= 1`, []string{"a", "b"}, `/#-/1/2`},
		{`a <= 1 AND b IS NULL`, []string{"a", "b"}, `/#-/1/#`},
		{`a <= 1 AND b IS NOT NULL`, []string{"a", "b"}, `/#-/2`},

		{`a IN (1) AND b = 1`, []string{"a", "b"}, `/1/1-/1/2`},
		{`a IN (1) AND b != 1`, []string{"a", "b"}, `/1/#-/2`},
		{`a IN (1) AND b > 1`, []string{"a", "b"}, `/1/2-/2`},
		{`a IN (1) AND b >= 1`, []string{"a", "b"}, `/1/1-/2`},
		{`a IN (1) AND b < 1`, []string{"a", "b"}, `/1/#-/1/1`},
		{`a IN (1) AND b <= 1`, []string{"a", "b"}, `/1/#-/1/2`},
		{`a IN (1) AND b IS NULL`, []string{"a", "b"}, `/1-/1/#`},
		{`a IN (1) AND b IS NOT NULL`, []string{"a", "b"}, `/1/#-/2`},
	}
	for _, d := range testData {
		desc, index := makeTestIndex(t, d.columns)
		constraints, _ := makeConstraints(t, d.expr, desc, index)
		spans := makeSpans(constraints, desc.ID, index.ID)
		if s := prettySpans(spans, 2); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
	}
}

func TestExactPrefix(t *testing.T) {
	defer leaktest.AfterTest(t)

	testData := []struct {
		expr     string
		columns  []string
		expected int
	}{
		{`a = 1`, []string{"a"}, 1},
		{`a != 1`, []string{"a"}, 0},
		{`a IN (1)`, []string{"a"}, 1},
		{`a = 1 AND b = 1`, []string{"a", "b"}, 2},
		{`(a, b) IN ((1, 2))`, []string{"a", "b"}, 2},
		{`(a, b) IN ((1, 2))`, []string{"a"}, 1},
		{`(a, b) IN ((1, 2))`, []string{"b"}, 1},
		{`(a, b) IN ((1, 2)) AND c = true`, []string{"a", "b", "c"}, 3},
		{`a = 1 AND (b, c) IN ((2, true))`, []string{"a", "b", "c"}, 3},
	}
	for _, d := range testData {
		desc, index := makeTestIndex(t, d.columns)
		constraints, _ := makeConstraints(t, d.expr, desc, index)
		prefix := exactPrefix(constraints)
		if d.expected != prefix {
			t.Errorf("%s: expected %d, but found %d", d.expr, d.expected, prefix)
		}
	}
}

func TestApplyConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)

	testData := []struct {
		expr     string
		columns  []string
		expected string
	}{
		{`a = 1`, []string{"a"}, `<nil>`},
		{`a = 1 AND b = 1`, []string{"a", "b"}, `<nil>`},
		{`a = 1 AND b = 1`, []string{"a"}, `b = 1`},
		{`a = 1 AND b = 1`, []string{"b"}, `a = 1`},
		{`a = 1 AND b > 1`, []string{"a", "b"}, `b > 1`},
		{`a > 1 AND b = 1`, []string{"a", "b"}, `a > 1 AND b = 1`},
		{`a IN (1)`, []string{"a"}, `<nil>`},
		{`a IN (1) OR a IN (2)`, []string{"a"}, `<nil>`},
		{`a = 1 OR a = 2`, []string{"a"}, `<nil>`},
		{`a = 1 OR b = 2`, []string{"a"}, `a = 1 OR b = 2`},
		{`NOT (a != 1)`, []string{"a"}, `<nil>`},
		{`a != 1`, []string{"a"}, `a != 1`},
		{`a IS NOT NULL`, []string{"a"}, `<nil>`},
		{`a = 1 AND b IS NOT NULL`, []string{"a", "b"}, `<nil>`},
	}
	for _, d := range testData {
		desc, index := makeTestIndex(t, d.columns)
		constraints, expr := makeConstraints(t, d.expr, desc, index)
		expr2 := applyConstraints(expr, constraints)
		if s := fmt.Sprint(expr2); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
	}
}
