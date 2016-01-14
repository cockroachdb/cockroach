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
	//"fmt"
	//"math"
	//"strings"
	"testing"

	//"github.com/cockroachdb/cockroach/keys"
	//"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/encoding"
	//"github.com/cockroachdb/cockroach/util/leaktest"
)

func makeTestIndex(t *testing.T, columns []string, dirs []encoding.Direction) (
	*TableDescriptor, *IndexDescriptor) {
	desc := testTableDesc()
	desc.Indexes = append(desc.Indexes, IndexDescriptor{
		Name:        "foo",
		ColumnNames: columns,
	})
	idx := &desc.Indexes[len(desc.Indexes)-1]
	// Fill in the directions for the columns.
	for i := range columns {
		var dir IndexDescriptor_Direction
		if dirs[i] == encoding.Ascending {
			dir = IndexDescriptor_ASC
		} else {
			dir = IndexDescriptor_DESC
		}
		idx.ColumnDirections = append(idx.ColumnDirections, dir)
	}

	if err := desc.AllocateIDs(); err != nil {
		t.Fatal(err)
	}
	return desc, idx
}

/* MEH
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
		{`a != 1`, []string{"a"}, `[a IS NOT NULL]`},
		{`a > 1`, []string{"a"}, `[a >= 2]`},
		{`a >= 1`, []string{"a"}, `[a >= 1]`},
		{`a < 1`, []string{"a"}, `[a IS NOT NULL, a <= 0]`},
		{`a <= 1`, []string{"a"}, `[a IS NOT NULL, a <= 1]`},

		{`a IN (1,2,3)`, []string{"a"}, `[a IN (1, 2, 3)]`},
		{`a IN (1,2,3) AND b = 1`, []string{"a", "b"}, `[a IN (1, 2, 3), b = 1]`},
		{`a = 1 AND b IN (1,2,3)`, []string{"a", "b"}, `[a = 1, b IN (1, 2, 3)]`},

		// Prefer EQ over IN.
		{`a IN (1) AND a = 1`, []string{"a"}, `[a = 1]`},
		// TODO(pmattis): We could conceivably propagate the "a = 1" down to the IN
		// expression and simplify. Doesn't seem worth it at this time. Issue #3472.
		{`a = 1 AND (a, b) IN ((1, 2))`, []string{"a", "b"}, `[a = 1]`},
		{`(a, b) IN ((1, 2)) AND a = 1`, []string{"a", "b"}, `[a = 1]`},

		{`a = 1 AND b = 1`, []string{"a", "b"}, `[a = 1, b = 1]`},
		{`a = 1 AND b != 1`, []string{"a", "b"}, `[a = 1, b IS NOT NULL]`},
		{`a = 1 AND b > 1`, []string{"a", "b"}, `[a = 1, b >= 2]`},
		{`a = 1 AND b >= 1`, []string{"a", "b"}, `[a = 1, b >= 1]`},
		{`a = 1 AND b < 1`, []string{"a", "b"}, `[a = 1, b IS NOT NULL, b <= 0]`},
		{`a = 1 AND b <= 1`, []string{"a", "b"}, `[a = 1, b IS NOT NULL, b <= 1]`},

		{`a != 1 AND b = 1`, []string{"a", "b"}, `[a IS NOT NULL]`},
		{`a != 1 AND b != 1`, []string{"a", "b"}, `[a IS NOT NULL]`},
		{`a != 1 AND b > 1`, []string{"a", "b"}, `[a IS NOT NULL]`},
		{`a != 1 AND b >= 1`, []string{"a", "b"}, `[a IS NOT NULL]`},
		{`a != 1 AND b < 1`, []string{"a", "b"}, `[a IS NOT NULL]`},
		{`a != 1 AND b <= 1`, []string{"a", "b"}, `[a IS NOT NULL]`},

		{`a > 1 AND b = 1`, []string{"a", "b"}, `[a >= 2, b = 1]`},
		{`a > 1 AND b != 1`, []string{"a", "b"}, `[a >= 2, b IS NOT NULL]`},
		{`a > 1 AND b > 1`, []string{"a", "b"}, `[a >= 2, b >= 2]`},
		{`a > 1 AND b >= 1`, []string{"a", "b"}, `[a >= 2, b >= 1]`},
		{`a > 1 AND b < 1`, []string{"a", "b"}, `[a >= 2]`},
		{`a > 1 AND b <= 1`, []string{"a", "b"}, `[a >= 2]`},

		{`a >= 1 AND b = 1`, []string{"a", "b"}, `[a >= 1, b = 1]`},
		{`a >= 1 AND b != 1`, []string{"a", "b"}, `[a >= 1, b IS NOT NULL]`},
		{`a >= 1 AND b > 1`, []string{"a", "b"}, `[a >= 1, b >= 2]`},
		{`a >= 1 AND b >= 1`, []string{"a", "b"}, `[a >= 1, b >= 1]`},
		{`a >= 1 AND b < 1`, []string{"a", "b"}, `[a >= 1]`},
		{`a >= 1 AND b <= 1`, []string{"a", "b"}, `[a >= 1]`},

		{`a < 1 AND b = 1`, []string{"a", "b"}, `[a IS NOT NULL, a <= 0, b = 1]`},
		{`a < 1 AND b != 1`, []string{"a", "b"}, `[a IS NOT NULL, a <= 0]`},
		{`a < 1 AND b > 1`, []string{"a", "b"}, `[a IS NOT NULL, a <= 0]`},
		{`a < 1 AND b >= 1`, []string{"a", "b"}, `[a IS NOT NULL, a <= 0]`},
		{`a < 1 AND b < 1`, []string{"a", "b"}, `[a IS NOT NULL, a <= 0, b <= 0]`},
		{`a < 1 AND b <= 1`, []string{"a", "b"}, `[a IS NOT NULL, a <= 0, b <= 1]`},

		{`a <= 1 AND b = 1`, []string{"a", "b"}, `[a IS NOT NULL, a <= 1, b = 1]`},
		{`a <= 1 AND b != 1`, []string{"a", "b"}, `[a IS NOT NULL, a <= 1]`},
		{`a <= 1 AND b > 1`, []string{"a", "b"}, `[a IS NOT NULL, a <= 1]`},
		{`a <= 1 AND b >= 1`, []string{"a", "b"}, `[a IS NOT NULL, a <= 1]`},
		{`a <= 1 AND b < 1`, []string{"a", "b"}, `[a IS NOT NULL, a <= 1, b <= 0]`},
		{`a <= 1 AND b <= 1`, []string{"a", "b"}, `[a IS NOT NULL, a <= 1, b <= 1]`},

		{`a IN (1) AND b = 1`, []string{"a", "b"}, `[a IN (1), b = 1]`},
		{`a IN (1) AND b != 1`, []string{"a", "b"}, `[a IN (1), b IS NOT NULL]`},
		{`a IN (1) AND b > 1`, []string{"a", "b"}, `[a IN (1), b >= 2]`},
		{`a IN (1) AND b >= 1`, []string{"a", "b"}, `[a IN (1), b >= 1]`},
		{`a IN (1) AND b < 1`, []string{"a", "b"}, `[a IN (1), b IS NOT NULL, b <= 0]`},
		{`a IN (1) AND b <= 1`, []string{"a", "b"}, `[a IN (1), b IS NOT NULL, b <= 1]`},

		{`(a, b) IN ((1, 2))`, []string{"a", "b"}, `[(a, b) IN ((1, 2))]`},
		{`(b, a) IN ((1, 2))`, []string{"a", "b"}, `[(b, a) IN ((1, 2))]`},
		{`(b, a) IN ((1, 2))`, []string{"a"}, `[(b, a) IN ((1, 2))]`},

		{`a <= 5 AND b >= 6 AND (a, b) IN ((1, 2))`, []string{"a", "b"}, `[(a, b) IN ((1, 2))]`},

		{`a IS NULL`, []string{"a"}, `[a IS NULL]`},
		{`a IS NOT NULL`, []string{"a"}, `[a IS NOT NULL]`},
	}
	for _, d := range testData {
		dirs := make([]encoding.Direction, 0, len(d.columns))
		for range d.columns {
			dirs = append(dirs, encoding.Ascending)
		}
		desc, index := makeTestIndex(t, d.columns, dirs)
		constraints, _ := makeConstraints(t, d.expr, desc, index)
		if s := constraints.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
	}
}

func indexToDirs(index *IndexDescriptor) []encoding.Direction {
	var dirs []encoding.Direction
	for _, dir := range index.ColumnDirections {
		d, err := dir.toEncodingDirection()
		if err != nil {
			panic(err)
		}
		dirs = append(dirs, d)
	}
	return dirs
}

func TestMakeSpans(t *testing.T) {
	defer leaktest.AfterTest(t)

	testData := []struct {
		expr         string
		columns      []string
		expectedAsc  string
		expectedDesc string
	}{
		{`a = 1`, []string{"a"}, `/1-/2`, `/1-/0`},
		{`a != 1`, []string{"a"}, `/#-`, `-/#`},
		{`a > 1`, []string{"a"}, `/2-`, `-/1`},
		{`a >= 1`, []string{"a"}, `/1-`, `-/0`},
		{`a < 1`, []string{"a"}, `/#-/1`, `/0-/#`},
		{`a <= 1`, []string{"a"}, `/#-/2`, `/1-/#`},
		{`a IS NULL`, []string{"a"}, `-/#`, `/NULL-`},
		{`a IS NOT NULL`, []string{"a"}, `/#-`, `-/#`},

		{`a IN (1,2,3)`, []string{"a"}, `/1-/2 /2-/3 /3-/4`, `/3-/2 /2-/1 /1-/0`},
		{`a IN (1,2,3) AND b = 1`, []string{"a", "b"},
			`/1/1-/1/2 /2/1-/2/2 /3/1-/3/2`, `/3/1-/3/0 /2/1-/2/0 /1/1-/1/0`},
		{`a = 1 AND b IN (1,2,3)`, []string{"a", "b"},
			`/1/1-/1/2 /1/2-/1/3 /1/3-/1/4`, `/1/3-/1/2 /1/2-/1/1 /1/1-/1/0`},
		{`a >= 1 AND b IN (1,2,3)`, []string{"a", "b"}, `/1-`, `-/0`},
		{`a <= 1 AND b IN (1,2,3)`, []string{"a", "b"}, `/#-/2`, `/1-/#`},
		{`(a, b) IN ((1, 2), (3, 4))`, []string{"a", "b"},
			`/1/2-/1/3 /3/4-/3/5`, `/3/4-/3/3 /1/2-/1/1`},
		{`(b, a) IN ((1, 2), (3, 4))`, []string{"a", "b"},
			`/2/1-/2/2 /4/3-/4/4`, `/4/3-/4/2 /2/1-/2/0`},
		{`(a, b) IN ((1, 2), (3, 4))`, []string{"b"}, `/2-/3 /4-/5`, `/4-/3 /2-/1`},

		{`a = 1 AND b = 1`, []string{"a", "b"}, `/1/1-/1/2`, `/1/1-/1/0`},
		{`a = 1 AND b != 1`, []string{"a", "b"}, `/1/#-/2`, `/1-/1/#`},
		{`a = 1 AND b > 1`, []string{"a", "b"}, `/1/2-/2`, `/1-/1/1`},
		{`a = 1 AND b >= 1`, []string{"a", "b"}, `/1/1-/2`, `/1-/1/0`},
		{`a = 1 AND b < 1`, []string{"a", "b"}, `/1/#-/1/1`, `/1/0-/1/#`},
		{`a = 1 AND b <= 1`, []string{"a", "b"}, `/1/#-/1/2`, `/1/1-/1/#`},
		{`a = 1 AND b IS NULL`, []string{"a", "b"}, `/1-/1/#`, `/1/NULL-/0`},
		{`a = 1 AND b IS NOT NULL`, []string{"a", "b"}, `/1/#-/2`, `/1-/1/#`},

		{`a != 1 AND b = 1`, []string{"a", "b"}, `/#-`, `-/#`},
		{`a != 1 AND b != 1`, []string{"a", "b"}, `/#-`, `-/#`},
		{`a != 1 AND b > 1`, []string{"a", "b"}, `/#-`, `-/#`},
		{`a != 1 AND b >= 1`, []string{"a", "b"}, `/#-`, `-/#`},
		{`a != 1 AND b < 1`, []string{"a", "b"}, `/#-`, `-/#`},
		{`a != 1 AND b <= 1`, []string{"a", "b"}, `/#-`, `-/#`},
		{`a != 1 AND b IS NULL`, []string{"a", "b"}, `/#-`, `-/#`},
		{`a != 1 AND b IS NOT NULL`, []string{"a", "b"}, `/#-`, `-/#`},

		{`a > 1 AND b = 1`, []string{"a", "b"}, `/2/1-`, `-/2/0`},
		{`a > 1 AND b != 1`, []string{"a", "b"}, `/2/#-`, `-/2/#`},
		{`a > 1 AND b > 1`, []string{"a", "b"}, `/2/2-`, `-/2/1`},
		{`a > 1 AND b >= 1`, []string{"a", "b"}, `/2/1-`, `-/2/0`},
		{`a > 1 AND b < 1`, []string{"a", "b"}, `/2-`, `-/1`},
		{`a > 1 AND b <= 1`, []string{"a", "b"}, `/2-`, `-/1`},
		{`a > 1 AND b IS NULL`, []string{"a", "b"}, `/2-`, `-/1`},
		{`a > 1 AND b IS NOT NULL`, []string{"a", "b"}, `/2/#-`, `-/2/#`},

		{`a >= 1 AND b = 1`, []string{"a", "b"}, `/1/1-`, `-/1/0`},
		{`a >= 1 AND b != 1`, []string{"a", "b"}, `/1/#-`, `-/1/#`},
		{`a >= 1 AND b > 1`, []string{"a", "b"}, `/1/2-`, `-/1/1`},
		{`a >= 1 AND b >= 1`, []string{"a", "b"}, `/1/1-`, `-/1/0`},
		{`a >= 1 AND b < 1`, []string{"a", "b"}, `/1-`, `-/0`},
		{`a >= 1 AND b <= 1`, []string{"a", "b"}, `/1-`, `-/0`},
		{`a >= 1 AND b IS NULL`, []string{"a", "b"}, `/1-`, `-/0`},
		{`a >= 1 AND b IS NOT NULL`, []string{"a", "b"}, `/1/#-`, `-/1/#`},

		{`a < 1 AND b = 1`, []string{"a", "b"}, `/#-/0/2`, `/0/1-/#`},
		{`a < 1 AND b != 1`, []string{"a", "b"}, `/#-/1`, `/0-/#`},
		{`a < 1 AND b > 1`, []string{"a", "b"}, `/#-/1`, `/0-/#`},
		{`a < 1 AND b >= 1`, []string{"a", "b"}, `/#-/1`, `/0-/#`},
		{`a < 1 AND b < 1`, []string{"a", "b"}, `/#-/0/1`, `/0/0-/#`},
		{`a < 1 AND b <= 1`, []string{"a", "b"}, `/#-/0/2`, `/0/1-/#`},
		{`a < 1 AND b IS NULL`, []string{"a", "b"}, `/#-/0/#`, `/0/NULL-/#`},
		{`a < 1 AND b IS NOT NULL`, []string{"a", "b"}, `/#-/1`, `/0-/#`},

		{`a <= 1 AND b = 1`, []string{"a", "b"}, `/#-/1/2`, `/1/1-/#`},
		{`a <= 1 AND b != 1`, []string{"a", "b"}, `/#-/2`, `/1-/#`},
		{`a <= 1 AND b > 1`, []string{"a", "b"}, `/#-/2`, `/1-/#`},
		{`a <= 1 AND b >= 1`, []string{"a", "b"}, `/#-/2`, `/1-/#`},
		{`a <= 1 AND b < 1`, []string{"a", "b"}, `/#-/1/1`, `/1/0-/#`},
		{`a <= 1 AND b <= 1`, []string{"a", "b"}, `/#-/1/2`, `/1/1-/#`},
		{`a <= 1 AND b IS NULL`, []string{"a", "b"}, `/#-/1/#`, `/1/NULL-/#`},
		{`a <= 1 AND b IS NOT NULL`, []string{"a", "b"}, `/#-/2`, `/1-/#`},

		{`a IN (1) AND b = 1`, []string{"a", "b"}, `/1/1-/1/2`, `/1/1-/1/0`},
		{`a IN (1) AND b != 1`, []string{"a", "b"}, `/1/#-/2`, `/1-/1/#`},
		{`a IN (1) AND b > 1`, []string{"a", "b"}, `/1/2-/2`, `/1-/1/1`},
		{`a IN (1) AND b >= 1`, []string{"a", "b"}, `/1/1-/2`, `/1-/1/0`},
		{`a IN (1) AND b < 1`, []string{"a", "b"}, `/1/#-/1/1`, `/1/0-/1/#`},
		{`a IN (1) AND b <= 1`, []string{"a", "b"}, `/1/#-/1/2`, `/1/1-/1/#`},
		{`a IN (1) AND b IS NULL`, []string{"a", "b"}, `/1-/1/#`, `/1/NULL-/0`},
		{`a IN (1) AND b IS NOT NULL`, []string{"a", "b"}, `/1/#-/2`, `/1-/1/#`},

		{`(a, b) = (1, 2)`, []string{"a"}, `/1-/2`, `/1-/0`},
		{`(a, b) = (1, 2)`, []string{"a", "b"}, `/1/2-/1/3`, `/1/2-/1/1`},

		// When encoding an end constraint for a maximal datum, we use
		// bytes.PrefixEnd() to go beyond the normal encodings of that datatype.
		{fmt.Sprintf(`a = %d`, math.MaxInt64), []string{"a"},
			`/9223372036854775807-/<util/encoding/encoding.go: ` +
				`varint 9223372036854775808 overflows int64>`, `/9223372036854775807-/9223372036854775806`},
		{fmt.Sprintf(`a = %d`, math.MinInt64), []string{"a"},
			`/-9223372036854775808-/-9223372036854775807`,
			`/-9223372036854775808-/<util/encoding/encoding.go: varint 9223372036854775808 overflows int64>`},
	}
	for _, d := range testData {
		for i := range []int{0, 1} {
			dir := encoding.Ascending
			if i == 1 {
				dir = encoding.Descending
			}
			dirs := make([]encoding.Direction, 0, len(d.columns))
			for range d.columns {
				dirs = append(dirs, dir)
			}
			desc, index := makeTestIndex(t, d.columns, dirs)
			constraints, _ := makeConstraints(t, d.expr, desc, index)
			spans := makeSpans(constraints, desc.ID, index)
			s := prettySpans(spans, 2)
			var expected string
			if dir == encoding.Ascending {
				expected = d.expectedAsc
			} else {
				expected = d.expectedDesc
			}
			s = keys.MassagePrettyPrintedSpanForTest(s, indexToDirs(index))
			if expected != s {
				t.Errorf("[index direction: %d] %s: expected %s, but found %s", dir, d.expr, expected, s)
			}
		}
	}

	type Col struct {
		col string
		dir encoding.Direction
	}

	// Test indexes with mixed-directions (some cols Asc, some cols Desc) and other edge cases.
	testData2 := []struct {
		expr     string
		columns  []Col
		expected string
	}{
		{`a = 1 AND b = 5`,
			[]Col{{"a", encoding.Ascending}, {"b", encoding.Descending}, {"c", encoding.Ascending}},
			`/1/5-/1/4`},
		{`a = 7 AND b IN (1,2,3) AND c = false`,
			[]Col{{"a", encoding.Ascending}, {"b", encoding.Descending}, {"c", encoding.Ascending}},
			`/7/3/0-/7/3/1 /7/2/0-/7/2/1 /7/1/0-/7/1/1`},
		// Test different directions for te columns inside a tuple.
		{`(a,b,j) IN ((1,2,3), (4,5,6))`,
			[]Col{{"a", encoding.Descending}, {"b", encoding.Ascending}, {"j", encoding.Descending}},
			`/4/5/6-/4/5/5 /1/2/3-/1/2/2`},
		{`i = E'\xff'`,
			[]Col{{"i", encoding.Ascending}},
			`/"\xff"-/"\xff\x00"`},
		// Test that limits on bytes work correctly: when encoding a descending limit for bytes,
		// we need to go outside the bytes encoding.
		// "\xaa" is encoded as [bytesDescMarker, ^0xaa, <term escape sequence>]
		{`i = E'\xaa'`,
			[]Col{{"i", encoding.Descending}},
			fmt.Sprintf("raw:%c%c\xff\xfe-%c%c\xff\xff",
				encoding.BytesDescMarker, ^byte(0xaa), encoding.BytesDescMarker, ^byte(0xaa))},
	}
	for _, d := range testData2 {
		cols := make([]string, 0, len(d.columns))
		dirs := make([]encoding.Direction, 0, len(d.columns))
		for _, col := range d.columns {
			cols = append(cols, col.col)
			dirs = append(dirs, col.dir)
		}
		desc, index := makeTestIndex(t, cols, dirs)
		constraints, _ := makeConstraints(t, d.expr, desc, index)
		spans := makeSpans(constraints, desc.ID, index)
		var got string
		raw := false
		if strings.HasPrefix(d.expected, "raw:") {
			raw = true
			span := spans[0]
			d.expected = d.expected[4:]
			// Trim the index prefix from the span.
			got = strings.TrimPrefix(string(span.start), string(MakeIndexKeyPrefix(desc.ID, index.ID))) +
				"-" + strings.TrimPrefix(string(span.end), string(MakeIndexKeyPrefix(desc.ID, index.ID)))
		} else {
			got = keys.MassagePrettyPrintedSpanForTest(prettySpans(spans, 2), indexToDirs(index))
		}
		if d.expected != got {
			if !raw {
				t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, got)
			} else {
				t.Errorf("%s: expected %# x, but found %# x", d.expr, []byte(d.expected), got)
			}
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
		dirs := make([]encoding.Direction, 0, len(d.columns))
		for range d.columns {
			dirs = append(dirs, encoding.Ascending)
		}
		desc, index := makeTestIndex(t, d.columns, dirs)
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
		{`a >= 1 AND b = 2`, []string{"a", "b"}, `a >= 1 AND b = 2`},
		{`a >= 1 AND a <= 3 AND b = 2`, []string{"a", "b"}, `a >= 1 AND a <= 3 AND b = 2`},
		{`(a, b) = (1, 2) AND c IS NOT NULL`, []string{"a", "b", "c"}, `<nil>`},
		{`a IN (1, 2) AND b = 3`, []string{"a", "b"}, `b = 3`},
		{`a <= 5 AND b >= 6 AND (a, b) IN ((1, 2))`, []string{"a", "b"}, `a <= 5 AND b >= 6`},
		{`a IN (1) AND a = 1`, []string{"a"}, `<nil>`},
		{`(a, b) = (1, 2)`, []string{"a"}, `(a, b) IN ((1, 2))`},
		// Filters that are not trimmed as of Dec 2015, although they could be.
		// Issue #3473.
		// {`a > 1`, []string{"a"}, `<nil>`},
		// {`a < 1`, []string{"a"}, `<nil>`},
	}
	for _, d := range testData {
		dirs := make([]encoding.Direction, 0, len(d.columns))
		for range d.columns {
			dirs = append(dirs, encoding.Ascending)
		}
		desc, index := makeTestIndex(t, d.columns, dirs)
		constraints, expr := makeConstraints(t, d.expr, desc, index)
		expr2 := applyConstraints(expr, constraints)
		if s := fmt.Sprint(expr2); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
	}
}
*/
