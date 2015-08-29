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
	index *IndexDescriptor) indexConstraints {
	expr, _ := parseAndNormalizeExpr(t, sql)
	exprs := analyzeExpr(expr)

	c := &indexInfo{
		desc:     desc,
		index:    index,
		covering: true,
	}
	c.analyzeRanges(exprs)
	return c.constraints
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
		{`a != 1`, []string{"a"}, `[]`},
		{`a > 1`, []string{"a"}, `[a >= 2]`},
		{`a >= 1`, []string{"a"}, `[a >= 1]`},
		{`a < 1`, []string{"a"}, `[a < 1]`},
		{`a <= 1`, []string{"a"}, `[a <= 1]`},

		{`a IN (1,2,3)`, []string{"a"}, `[a IN (1, 2, 3)]`},
		{`a IN (1,2,3) AND b = 1`, []string{"a", "b"}, `[a IN (1, 2, 3), b = 1]`},
		{`a = 1 AND b IN (1,2,3)`, []string{"a", "b"}, `[a = 1, b IN (1, 2, 3)]`},

		{`a = 1 AND b = 1`, []string{"a", "b"}, `[a = 1, b = 1]`},
		{`a = 1 AND b != 1`, []string{"a", "b"}, `[a = 1]`},
		{`a = 1 AND b > 1`, []string{"a", "b"}, `[a = 1, b >= 2]`},
		{`a = 1 AND b >= 1`, []string{"a", "b"}, `[a = 1, b >= 1]`},
		{`a = 1 AND b < 1`, []string{"a", "b"}, `[a = 1, b < 1]`},
		{`a = 1 AND b <= 1`, []string{"a", "b"}, `[a = 1, b <= 1]`},

		{`a != 1 AND b = 1`, []string{"a", "b"}, `[]`},
		{`a != 1 AND b != 1`, []string{"a", "b"}, `[]`},
		{`a != 1 AND b > 1`, []string{"a", "b"}, `[]`},
		{`a != 1 AND b >= 1`, []string{"a", "b"}, `[]`},
		{`a != 1 AND b < 1`, []string{"a", "b"}, `[]`},
		{`a != 1 AND b <= 1`, []string{"a", "b"}, `[]`},

		{`a > 1 AND b = 1`, []string{"a", "b"}, `[a >= 2, b = 1]`},
		{`a > 1 AND b != 1`, []string{"a", "b"}, `[a >= 2]`},
		{`a > 1 AND b > 1`, []string{"a", "b"}, `[a >= 2, b >= 2]`},
		{`a > 1 AND b >= 1`, []string{"a", "b"}, `[a >= 2, b >= 1]`},
		{`a > 1 AND b < 1`, []string{"a", "b"}, `[a >= 2]`},
		{`a > 1 AND b <= 1`, []string{"a", "b"}, `[a >= 2]`},

		{`a >= 1 AND b = 1`, []string{"a", "b"}, `[a >= 1, b = 1]`},
		{`a >= 1 AND b != 1`, []string{"a", "b"}, `[a >= 1]`},
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
		{`a IN (1) AND b != 1`, []string{"a", "b"}, `[a IN (1)]`},
		{`a IN (1) AND b > 1`, []string{"a", "b"}, `[a IN (1), b >= 2]`},
		{`a IN (1) AND b >= 1`, []string{"a", "b"}, `[a IN (1), b >= 1]`},
		{`a IN (1) AND b < 1`, []string{"a", "b"}, `[a IN (1), b < 1]`},
		{`a IN (1) AND b <= 1`, []string{"a", "b"}, `[a IN (1), b <= 1]`},
	}
	for _, d := range testData {
		desc, index := makeTestIndex(t, d.columns)
		constraints := makeConstraints(t, d.expr, desc, index)
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
		{`a != 1`, []string{"a"}, `-`},
		{`a > 1`, []string{"a"}, `/2-`},
		{`a >= 1`, []string{"a"}, `/1-`},
		{`a < 1`, []string{"a"}, `-/1`},
		{`a <= 1`, []string{"a"}, `-/2`},

		{`a IN (1,2,3)`, []string{"a"}, `/1-/2 /2-/3 /3-/4`},
		{`a IN (1,2,3) AND b = 1`, []string{"a", "b"}, `/1/1-/1/2 /2/1-/2/2 /3/1-/3/2`},
		{`a = 1 AND b IN (1,2,3)`, []string{"a", "b"}, `/1/1-/1/2 /1/2-/1/3 /1/3-/1/4`},

		{`a = 1 AND b = 1`, []string{"a", "b"}, `/1/1-/1/2`},
		{`a = 1 AND b != 1`, []string{"a", "b"}, `/1-/2`},
		{`a = 1 AND b > 1`, []string{"a", "b"}, `/1/2-/2`},
		{`a = 1 AND b >= 1`, []string{"a", "b"}, `/1/1-/2`},
		{`a = 1 AND b < 1`, []string{"a", "b"}, `/1-/1/1`},
		{`a = 1 AND b <= 1`, []string{"a", "b"}, `/1-/1/2`},

		{`a != 1 AND b = 1`, []string{"a", "b"}, `-`},
		{`a != 1 AND b != 1`, []string{"a", "b"}, `-`},
		{`a != 1 AND b > 1`, []string{"a", "b"}, `-`},
		{`a != 1 AND b >= 1`, []string{"a", "b"}, `-`},
		{`a != 1 AND b < 1`, []string{"a", "b"}, `-`},
		{`a != 1 AND b <= 1`, []string{"a", "b"}, `-`},

		{`a > 1 AND b = 1`, []string{"a", "b"}, `/2/1-`},
		{`a > 1 AND b != 1`, []string{"a", "b"}, `/2-`},
		{`a > 1 AND b > 1`, []string{"a", "b"}, `/2/2-`},
		{`a > 1 AND b >= 1`, []string{"a", "b"}, `/2/1-`},
		{`a > 1 AND b < 1`, []string{"a", "b"}, `/2-`},
		{`a > 1 AND b <= 1`, []string{"a", "b"}, `/2-`},

		{`a >= 1 AND b = 1`, []string{"a", "b"}, `/1/1-`},
		{`a >= 1 AND b != 1`, []string{"a", "b"}, `/1-`},
		{`a >= 1 AND b > 1`, []string{"a", "b"}, `/1/2-`},
		{`a >= 1 AND b >= 1`, []string{"a", "b"}, `/1/1-`},
		{`a >= 1 AND b < 1`, []string{"a", "b"}, `/1-`},
		{`a >= 1 AND b <= 1`, []string{"a", "b"}, `/1-`},

		{`a < 1 AND b = 1`, []string{"a", "b"}, `-/1`},
		{`a < 1 AND b != 1`, []string{"a", "b"}, `-/1`},
		{`a < 1 AND b > 1`, []string{"a", "b"}, `-/1`},
		{`a < 1 AND b >= 1`, []string{"a", "b"}, `-/1`},
		{`a < 1 AND b < 1`, []string{"a", "b"}, `-/1`},
		{`a < 1 AND b <= 1`, []string{"a", "b"}, `-/1`},

		{`a <= 1 AND b = 1`, []string{"a", "b"}, `-/1/2`},
		{`a <= 1 AND b != 1`, []string{"a", "b"}, `-/2`},
		{`a <= 1 AND b > 1`, []string{"a", "b"}, `-/2`},
		{`a <= 1 AND b >= 1`, []string{"a", "b"}, `-/2`},
		{`a <= 1 AND b < 1`, []string{"a", "b"}, `-/1/1`},
		{`a <= 1 AND b <= 1`, []string{"a", "b"}, `-/1/2`},

		{`a IN (1) AND b = 1`, []string{"a", "b"}, `/1/1-/1/2`},
		{`a IN (1) AND b != 1`, []string{"a", "b"}, `/1-/2`},
		{`a IN (1) AND b > 1`, []string{"a", "b"}, `/1/2-/2`},
		{`a IN (1) AND b >= 1`, []string{"a", "b"}, `/1/1-/2`},
		{`a IN (1) AND b < 1`, []string{"a", "b"}, `/1-/1/1`},
		{`a IN (1) AND b <= 1`, []string{"a", "b"}, `/1-/1/2`},
	}
	for _, d := range testData {
		desc, index := makeTestIndex(t, d.columns)
		constraints := makeConstraints(t, d.expr, desc, index)
		spans := makeSpans(constraints, desc.ID, index.ID)
		if s := prettySpans(spans, desc, index); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
	}
}
