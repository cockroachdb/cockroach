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
	"math"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestMergeAndSortSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Each testcase is a list of ranges; each range gets converted into a span.
	testCases := [][][2]int{
		{{1, 10}},
		{{1, 2}, {2, 3}},
		{{1, 2}, {2, 3}, {2, 4}, {2, 5}, {3, 4}, {3, 4}, {3, 5}},
		{{2, 4}, {1, 3}},
		{{1, 2}, {2, 3}, {2, 4}, {3, 6}, {4, 6}, {5, 6}},
		{{3, 4}, {1, 2}},
		{{3, 5}, {1, 2}, {4, 7}},
		{{1, 50}, {1, 2}, {1, 3}, {3, 5}, {3, 10}, {9, 30}, {30, 49}},
		{{10, 15}, {5, 9}, {20, 30}, {40, 50}, {35, 36}},
		{{10, 15}, {5, 9}, {20, 30}, {40, 50}, {35, 36}, {36, 40}},
		{{10, 15}, {5, 9}, {20, 30}, {9, 10}, {40, 50}, {35, 36}, {36, 40}},
		{{14, 21}, {10, 15}, {5, 9}, {20, 30}, {9, 10}, {40, 50}, {35, 36}, {36, 40}},
		{{14, 21}, {10, 15}, {5, 9}, {20, 30}, {9, 10}, {40, 50}, {35, 36}, {36, 40}, {30, 35}},
	}

	for _, tc := range testCases {
		// We use a bitmap on the keyspace to verify the results:
		//  - we set the bits for all areas covered by the ranges;
		//  - we verify that merged spans are ordered, non-overlapping, and
		//    contain only covered areas of the bitmap;
		//  - we verify that after we unset all areas covered by the merged
		//    spans, there are no bits that remain set.
		bitmap := make([]bool, 100)
		var s roachpb.Spans
		for _, v := range tc {
			start := v[0]
			end := v[1]
			for j := start; j < end; j++ {
				bitmap[j] = true
			}
			s = append(s, roachpb.Span{Key: []byte{byte(start)}, EndKey: []byte{byte(end)}})
		}

		printSpans := func(s roachpb.Spans, title string) {
			fmt.Printf("%s:", title)
			for _, span := range s {
				fmt.Printf(" %d-%d", span.Key[0], span.EndKey[0])
			}
			fmt.Printf("\n")
		}

		if testing.Verbose() || log.V(1) {
			printSpans(s, "Input spans ")
		}

		s = mergeAndSortSpans(s)

		if testing.Verbose() || log.V(1) {
			printSpans(s, "Output spans")
		}

		last := -1
		for i := range s {
			start := int(s[i].Key[0])
			end := int(s[i].EndKey[0])
			if start >= end {
				t.Fatalf("invalid span %d-%d", start, end)
			}
			if start <= last {
				t.Fatalf("span %d-%d starts before previous span ends", start, end)
			}
			last = end
			for j := start; j < end; j++ {
				if !bitmap[j] {
					t.Fatalf("span %d-%d incorrectly contains %d", start, end, j)
				}
				bitmap[j] = false
			}
			if start != 0 && bitmap[start-1] {
				t.Fatalf("span %d-%d should begin earlier", start, end)
			}
			if bitmap[end] {
				t.Fatalf("span %d-%d should end later", start, end)
			}
		}
		for i, val := range bitmap {
			if val {
				t.Fatalf("key %d not covered by any spans", i)
			}
		}
	}
}

func makeTestIndex(
	t *testing.T, columns []string, dirs []encoding.Direction,
) (*sqlbase.TableDescriptor, *sqlbase.IndexDescriptor) {
	desc := testTableDesc()
	desc.Indexes = append(desc.Indexes, sqlbase.IndexDescriptor{
		Name:        "foo",
		ColumnNames: columns,
	})
	idx := &desc.Indexes[len(desc.Indexes)-1]
	// Fill in the directions for the columns.
	for i := range columns {
		var dir sqlbase.IndexDescriptor_Direction
		if dirs[i] == encoding.Ascending {
			dir = sqlbase.IndexDescriptor_ASC
		} else {
			dir = sqlbase.IndexDescriptor_DESC
		}
		idx.ColumnDirections = append(idx.ColumnDirections, dir)
	}

	if err := desc.AllocateIDs(); err != nil {
		t.Fatal(err)
	}
	return desc, idx
}

// makeTestIndexFromStr creates a test index from a string that enumerates the
// columns, separated by commas. Each column has an optional '-' at the end if
// it is descending.
func makeTestIndexFromStr(
	t *testing.T, columnsStr string,
) (*sqlbase.TableDescriptor, *sqlbase.IndexDescriptor) {
	columns := strings.Split(columnsStr, ",")
	dirs := make([]encoding.Direction, len(columns))
	for i, c := range columns {
		if c[len(c)-1] == '-' {
			dirs[i] = encoding.Descending
			columns[i] = columns[i][:len(c)-1]
		} else {
			dirs[i] = encoding.Ascending
		}
	}
	return makeTestIndex(t, columns, dirs)
}

func makeConstraints(
	t *testing.T,
	p *planner,
	sql string,
	desc *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
	sel *renderNode,
) (orIndexConstraints, tree.TypedExpr) {
	expr := parseAndNormalizeExpr(t, p, sql, sel)
	exprs, equiv := decomposeExpr(p.EvalContext(), expr)

	c := &indexInfo{
		desc:     desc,
		index:    index,
		covering: true,
	}
	c.analyzeExprs(p.EvalContext(), exprs)
	if equiv && len(exprs) == 1 {
		expr = joinAndExprs(exprs[0])
	}
	return c.constraints, expr
}

func TestMakeConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		expr     string
		columns  string
		expected string
	}{
		{`c`, `c`, `[c = true]`},
		{`c = true`, `c`, `[c = true]`},
		{`c = false`, `c`, `[c = false]`},
		{`c != true`, `c`, `[c IS NOT NULL, c <= false]`},
		{`c != false`, `c`, `[c >= true]`},
		{`NOT c`, `c`, `[c IS NOT NULL, c <= false]`},
		{`c IS TRUE`, `c`, `[c = true]`},
		{`c IS NOT TRUE`, `c`, `[c IS NOT NULL, c <= false] OR [c IS NULL]`},
		{`c IS FALSE`, `c`, `[c = false]`},
		{`c IS NOT FALSE`, `c`, `[c >= true] OR [c IS NULL]`},
		{`c IS NOT DISTINCT FROM TRUE`, `c`, `[c = true]`},
		{`c IS DISTINCT FROM TRUE`, `c`, `[c IS NOT NULL, c <= false] OR [c IS NULL]`},
		{`c IS NOT DISTINCT FROM FALSE`, `c`, `[c = false]`},
		{`c IS DISTINCT FROM FALSE`, `c`, `[c >= true] OR [c IS NULL]`},

		{`a = 1`, `b`, ``},
		{`a = 1`, `a`, `[a = 1]`},
		{`a != 1`, `a`, `[a IS NOT NULL]`},
		{`a > 1`, `a`, `[a >= 2]`},
		{`a >= 1`, `a`, `[a >= 1]`},
		{`a < 1`, `a`, `[a IS NOT NULL, a <= 0]`},
		{`a <= 1`, `a`, `[a IS NOT NULL, a <= 1]`},

		{`a IN (1,2,3)`, `a`, `[a IN (1, 2, 3)]`},
		{`a IN (1,2,3) AND b = 1`, `a,b`, `[a IN (1, 2, 3), b = 1]`},
		{`a = 1 AND b IN (1,2,3)`, `a,b`, `[a = 1, b IN (1, 2, 3)]`},

		// Prefer EQ over IN.
		{`a IN (1) AND a = 1`, `a`, `[a = 1]`},
		// TODO(pmattis): We could conceivably propagate the `a = 1` down to the IN
		// expression and simplify. Doesn't seem worth it at this time. Issue #3472.
		{`a = 1 AND (a, b) IN ((1, 2))`, `a,b`, `[a = 1]`},
		{`(a, b) IN ((1, 2)) AND a = 1`, `a,b`, `[a = 1]`},

		{`a = 1 AND b = 1`, `a,b`, `[a = 1, b = 1]`},
		{`a = 1 AND b != 1`, `a,b`, `[a = 1, b IS NOT NULL]`},
		{`a = 1 AND b > 1`, `a,b`, `[a = 1, b >= 2]`},
		{`a = 1 AND b >= 1`, `a,b`, `[a = 1, b >= 1]`},
		{`a = 1 AND b < 1`, `a,b`, `[a = 1, b IS NOT NULL, b <= 0]`},
		{`a = 1 AND b <= 1`, `a,b`, `[a = 1, b IS NOT NULL, b <= 1]`},

		{`a != 1 AND b = 1`, `a,b`, `[a IS NOT NULL]`},
		{`a != 1 AND b != 1`, `a,b`, `[a IS NOT NULL]`},
		{`a != 1 AND b > 1`, `a,b`, `[a IS NOT NULL]`},
		{`a != 1 AND b >= 1`, `a,b`, `[a IS NOT NULL]`},
		{`a != 1 AND b < 1`, `a,b`, `[a IS NOT NULL]`},
		{`a != 1 AND b <= 1`, `a,b`, `[a IS NOT NULL]`},

		{`a > 1 AND b = 1`, `a,b`, `[a >= 2, b = 1]`},
		{`a > 1 AND b != 1`, `a,b`, `[a >= 2, b IS NOT NULL]`},
		{`a > 1 AND b > 1`, `a,b`, `[a >= 2, b >= 2]`},
		{`a > 1 AND b >= 1`, `a,b`, `[a >= 2, b >= 1]`},
		{`a > 1 AND b < 1`, `a,b`, `[a >= 2]`},
		{`a > 1 AND b <= 1`, `a,b`, `[a >= 2]`},

		{`a >= 1 AND b = 1`, `a,b`, `[a >= 1, b = 1]`},
		{`a >= 1 AND b != 1`, `a,b`, `[a >= 1, b IS NOT NULL]`},
		{`a >= 1 AND b > 1`, `a,b`, `[a >= 1, b >= 2]`},
		{`a >= 1 AND b >= 1`, `a,b`, `[a >= 1, b >= 1]`},
		{`a >= 1 AND b < 1`, `a,b`, `[a >= 1]`},
		{`a >= 1 AND b <= 1`, `a,b`, `[a >= 1]`},

		{`a < 1 AND b = 1`, `a,b`, `[a IS NOT NULL, a <= 0, b = 1]`},
		{`a < 1 AND b != 1`, `a,b`, `[a IS NOT NULL, a <= 0]`},
		{`a < 1 AND b > 1`, `a,b`, `[a IS NOT NULL, a <= 0]`},
		{`a < 1 AND b >= 1`, `a,b`, `[a IS NOT NULL, a <= 0]`},
		{`a < 1 AND b < 1`, `a,b`, `[a IS NOT NULL, a <= 0, b <= 0]`},
		{`a < 1 AND b <= 1`, `a,b`, `[a IS NOT NULL, a <= 0, b <= 1]`},

		{`a <= 1 AND b = 1`, `a,b`, `[a IS NOT NULL, a <= 1, b = 1]`},
		{`a <= 1 AND b != 1`, `a,b`, `[a IS NOT NULL, a <= 1]`},
		{`a <= 1 AND b > 1`, `a,b`, `[a IS NOT NULL, a <= 1]`},
		{`a <= 1 AND b >= 1`, `a,b`, `[a IS NOT NULL, a <= 1]`},
		{`a <= 1 AND b < 1`, `a,b`, `[a IS NOT NULL, a <= 1, b <= 0]`},
		{`a <= 1 AND b <= 1`, `a,b`, `[a IS NOT NULL, a <= 1, b <= 1]`},

		{`a IN (1) AND b = 1`, `a,b`, `[a IN (1), b = 1]`},
		{`a IN (1) AND b != 1`, `a,b`, `[a IN (1), b IS NOT NULL]`},
		{`a IN (1) AND b > 1`, `a,b`, `[a IN (1), b >= 2]`},
		{`a IN (1) AND b >= 1`, `a,b`, `[a IN (1), b >= 1]`},
		{`a IN (1) AND b < 1`, `a,b`, `[a IN (1), b IS NOT NULL, b <= 0]`},
		{`a IN (1) AND b <= 1`, `a,b`, `[a IN (1), b IS NOT NULL, b <= 1]`},

		{`(a, b) IN ((1, 2))`, `a,b`, `[(a, b) IN ((1, 2))]`},
		{`(b, a) IN ((1, 2))`, `a,b`, `[(b, a) IN ((1, 2))]`},
		{`(b, a) IN ((1, 2))`, `a`, `[(b, a) IN ((1, 2))]`},

		{`(a, b) = (1, 2)`, `a,b`, `[(a, b) IN ((1, 2))]`},
		{`(b, a) = (1, 2)`, `a,b`, `[(b, a) IN ((1, 2))]`},
		{`(b, a) = (1, 2)`, `a`, `[(b, a) IN ((1, 2))]`},

		{`(a, b) != (1, 2)`, `a,b`, ``},

		{`a <= 5 AND b >= 6 AND (a, b) IN ((1, 2))`, `a,b`, `[(a, b) IN ((1, 2))]`},

		{`a IS NULL`, `a`, `[a IS NULL]`},
		{`a IS NOT NULL`, `a`, `[a IS NOT NULL]`},
		{`a IS NOT DISTINCT FROM NULL`, `a`, `[a IS NULL]`},
		{`a IS DISTINCT FROM NULL`, `a`, `[a IS NOT NULL]`},

		{`a = 1 OR a = 3`, `a`, `[a IN (1, 3)]`},
		{`a <= 1 OR a >= 8`, `a`, `[a IS NOT NULL, a <= 1] OR [a >= 8]`},
		{`a < 1 OR a > 2`, `a`, `[a IS NOT NULL, a <= 0] OR [a >= 3]`},
		{`a < 1 OR a = 3 OR a > 5`, `a`, `[a IS NOT NULL, a <= 0] OR [a = 3] OR [a >= 6]`},

		{`a = 1 OR b = 3`, `a,b`, ``},
		{`a = 1 OR b > 3`, `a,b`, ``},
		{`a > 1 OR b > 3`, `a,b`, ``},

		{`(a > 1 AND a < 10) OR (a = 15)`, `a`, `[a >= 2, a <= 9] OR [a = 15]`},
		{`(a >= 1 AND a <= 10) OR (a >= 20 AND a <= 30)`, `a`,
			`[a >= 1, a <= 10] OR [a >= 20, a <= 30]`},
		{`(a > 1 AND a < 10) OR (a > 20 AND a < 30)`, `a`,
			`[a >= 2, a <= 9] OR [a >= 21, a <= 29]`},

		{`a = 1 OR (a = 3 AND b = 2)`, `a`, `[a = 1] OR [a = 3]`},
		{`a = 1 OR (a = 3 AND b = 2)`, `b`, ``},
		{`a = 1 OR (a = 3 AND b = 2)`, `a,b`, `[a = 1] OR [a = 3, b = 2]`},
		{`a < 2 OR (a > 5 AND b > 2)`, `a`,
			`[a IS NOT NULL, a <= 1] OR [a >= 6]`},
		{`a < 2 OR (a > 5 AND b > 2)`, `b`, ``},
		{`a < 2 OR (a > 5 AND b > 2)`, `a,b`,
			`[a IS NOT NULL, a <= 1] OR [a >= 6, b >= 3]`},

		{`(a = 1 AND b >= 10 AND b <= 20) OR (a = 2 AND b >= 1 AND b <= 9)`,
			`a`, `[a = 1] OR [a = 2]`},
		{`(a = 1 AND b >= 10 AND b <= 20) OR (a = 2 AND b >= 1 AND b <= 9)`,
			`b`, `[b >= 10, b <= 20] OR [b >= 1, b <= 9]`},
		{`(a = 1 AND b >= 10 AND b <= 20) OR (a = 2 AND b >= 1 AND b <= 9)`,
			`a,b`, `[a = 1, b >= 10, b <= 20] OR [a = 2, b >= 1, b <= 9]`},

		{`(a, b) >= (1, 4)`, `a,b`, `[(a, b) >= (1, 4)]`},
		{`(a, b) >= (1, 4)`, `a`, ``},
		{`(a, b) >= (1, 4)`, `b`, ``},
		{`(b, a) >= (1, 4)`, `a,b`, ``},
	}
	p := makeTestPlanner()
	for _, d := range testData {
		t.Run(d.expr+"~"+d.expected, func(t *testing.T) {
			p.extendedEvalCtx = makeTestingExtendedEvalContext()
			defer p.extendedEvalCtx.Stop(context.Background())
			sel := makeSelectNode(t, p)
			desc, index := makeTestIndexFromStr(t, d.columns)
			constraints, _ := makeConstraints(t, p, d.expr, desc, index, sel)
			if s := constraints.String(); d.expected != s {
				t.Errorf("%s, columns: %s: expected %s, but found %s", d.expr, d.columns, d.expected, s)
			}
		})
	}
}

func TestMakeSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		expr         string
		columns      string
		expectedAsc  string
		expectedDesc string
	}{
		{`c`, `c`, `/1-/2`, `/1-/0`},
		{`c = true`, `c`, `/1-/2`, `/1-/0`},
		{`c = false`, `c`, `/0-/1`, `/0-/-1`},
		{`c != true`, `c`, `/!NULL-/1`, `/0-/NULL`},
		{`c != false`, `c`, `/1-`, `-/0`},
		{`NOT c`, `c`, `/!NULL-/1`, `/0-/NULL`},
		{`c IS TRUE`, `c`, `/1-/2`, `/1-/0`},
		{`c IS NOT TRUE`, `c`, `/NULL-/1`, `/0-`},
		{`c IS FALSE`, `c`, `/0-/1`, `/0-/-1`},
		{`c IS NOT FALSE`, `c`, `/NULL-/!NULL /1-`, `-/0 /NULL-`},
		{`c IS NOT DISTINCT FROM TRUE`, `c`, `/1-/2`, `/1-/0`},
		{`c IS DISTINCT FROM TRUE`, `c`, `/NULL-/1`, `/0-`},
		{`c IS NOT DISTINCT FROM FALSE`, `c`, `/0-/1`, `/0-/-1`},
		{`c IS DISTINCT FROM FALSE`, `c`, `/NULL-/!NULL /1-`, `-/0 /NULL-`},

		{`a = 1`, `a`, `/1-/2`, `/1-/0`},
		{`a != 1`, `a`, `/!NULL-`, `-/NULL`},
		{`a > 1`, `a`, `/2-`, `-/1`},
		{`a >= 1`, `a`, `/1-`, `-/0`},
		{`a < 1`, `a`, `/!NULL-/1`, `/0-/NULL`},
		{`a <= 1`, `a`, `/!NULL-/2`, `/1-/NULL`},
		{`a IS NULL`, `a`, `/NULL-/!NULL`, `/NULL-`},
		{`a IS NOT NULL`, `a`, `/!NULL-`, `-/NULL`},
		{`a IS NOT DISTINCT FROM NULL`, `a`, `/NULL-/!NULL`, `/NULL-`},
		{`a IS DISTINCT FROM NULL`, `a`, `/!NULL-`, `-/NULL`},

		{`a IN (1,2,3)`, `a`, `/1-/4`, `/3-/0`},
		{`a IN (1,3,5)`, `a`, `/1-/2 /3-/4 /5-/6`, `/5-/4 /3-/2 /1-/0`},
		{`a IN (1,2,3) AND b = 1`, `a,b`,
			`/1/1-/1/2 /2/1-/2/2 /3/1-/3/2`, `/3/1-/3/0 /2/1-/2/0 /1/1-/1/0`},
		{`a = 1 AND b IN (1,2,3)`, `a,b`,
			`/1/1-/1/4`, `/1/3-/1/0`},
		{`a = 1 AND b IN (1,3,5)`, `a,b`,
			`/1/1-/1/2 /1/3-/1/4 /1/5-/1/6`, `/1/5-/1/4 /1/3-/1/2 /1/1-/1/0`},
		{`a >= 1 AND b IN (1,2,3)`, `a,b`, `/1-`, `-/0`},
		{`a <= 1 AND b IN (1,2,3)`, `a,b`, `/!NULL-/2`, `/1-/NULL`},
		{`(a, b) IN ((1, 2), (3, 4))`, `a,b`,
			`/1/2-/1/3 /3/4-/3/5`, `/3/4-/3/3 /1/2-/1/1`},
		{`(b, a) IN ((1, 2), (3, 4))`, `a,b`,
			`/2/1-/2/2 /4/3-/4/4`, `/4/3-/4/2 /2/1-/2/0`},
		{`(a, b) IN ((1, 2), (3, 4))`, `b`, `/2-/3 /4-/5`, `/4-/3 /2-/1`},

		{`a = 1 AND b = 1`, `a,b`, `/1/1-/1/2`, `/1/1-/1/0`},
		{`a = 1 AND b != 1`, `a,b`, `/1/!NULL-/2`, `/1-/1/NULL`},
		{`a = 1 AND b > 1`, `a,b`, `/1/2-/2`, `/1-/1/1`},
		{`a = 1 AND b >= 1`, `a,b`, `/1/1-/2`, `/1-/1/0`},
		{`a = 1 AND b < 1`, `a,b`, `/1/!NULL-/1/1`, `/1/0-/1/NULL`},
		{`a = 1 AND b <= 1`, `a,b`, `/1/!NULL-/1/2`, `/1/1-/1/NULL`},
		{`a = 1 AND b IS NULL`, `a,b`, `/1/NULL-/1/!NULL`, `/1/NULL-/0`},
		{`a = 1 AND b IS NOT NULL`, `a,b`, `/1/!NULL-/2`, `/1-/1/NULL`},
		{`a = 1 AND b IS NOT DISTINCT FROM NULL`, `a,b`, `/1/NULL-/1/!NULL`, `/1/NULL-/0`},
		{`a = 1 AND b IS DISTINCT FROM NULL`, `a,b`, `/1/!NULL-/2`, `/1-/1/NULL`},

		{`a != 1 AND b = 1`, `a,b`, `/!NULL-`, `-/NULL`},
		{`a != 1 AND b != 1`, `a,b`, `/!NULL-`, `-/NULL`},
		{`a != 1 AND b > 1`, `a,b`, `/!NULL-`, `-/NULL`},
		{`a != 1 AND b >= 1`, `a,b`, `/!NULL-`, `-/NULL`},
		{`a != 1 AND b < 1`, `a,b`, `/!NULL-`, `-/NULL`},
		{`a != 1 AND b <= 1`, `a,b`, `/!NULL-`, `-/NULL`},
		{`a != 1 AND b IS NULL`, `a,b`, `/!NULL-`, `-/NULL`},
		{`a != 1 AND b IS NOT NULL`, `a,b`, `/!NULL-`, `-/NULL`},
		{`a != 1 AND b IS NOT DISTINCT FROM NULL`, `a,b`, `/!NULL-`, `-/NULL`},
		{`a != 1 AND b IS DISTINCT FROM NULL`, `a,b`, `/!NULL-`, `-/NULL`},

		{`a > 1 AND b = 1`, `a,b`, `/2/1-`, `-/2/0`},
		{`a > 1 AND b != 1`, `a,b`, `/2/!NULL-`, `-/2/NULL`},
		{`a > 1 AND b > 1`, `a,b`, `/2/2-`, `-/2/1`},
		{`a > 1 AND b >= 1`, `a,b`, `/2/1-`, `-/2/0`},
		{`a > 1 AND b < 1`, `a,b`, `/2-`, `-/1`},
		{`a > 1 AND b <= 1`, `a,b`, `/2-`, `-/1`},
		{`a > 1 AND b IS NULL`, `a,b`, `/2/NULL-`, `-/1`},
		{`a > 1 AND b IS NOT NULL`, `a,b`, `/2/!NULL-`, `-/2/NULL`},
		{`a > 1 AND b IS NOT DISTINCT FROM NULL`, `a,b`, `/2/NULL-`, `-/1`},
		{`a > 1 AND b IS DISTINCT FROM NULL`, `a,b`, `/2/!NULL-`, `-/2/NULL`},

		{`a >= 1 AND b = 1`, `a,b`, `/1/1-`, `-/1/0`},
		{`a >= 1 AND b != 1`, `a,b`, `/1/!NULL-`, `-/1/NULL`},
		{`a >= 1 AND b > 1`, `a,b`, `/1/2-`, `-/1/1`},
		{`a >= 1 AND b >= 1`, `a,b`, `/1/1-`, `-/1/0`},
		{`a >= 1 AND b < 1`, `a,b`, `/1-`, `-/0`},
		{`a >= 1 AND b <= 1`, `a,b`, `/1-`, `-/0`},
		{`a >= 1 AND b IS NULL`, `a,b`, `/1/NULL-`, `-/0`},
		{`a >= 1 AND b IS NOT NULL`, `a,b`, `/1/!NULL-`, `-/1/NULL`},
		{`a >= 1 AND b IS NOT DISTINCT FROM NULL`, `a,b`, `/1/NULL-`, `-/0`},
		{`a >= 1 AND b IS DISTINCT FROM NULL`, `a,b`, `/1/!NULL-`, `-/1/NULL`},

		{`a < 1 AND b = 1`, `a,b`, `/!NULL-/0/2`, `/0/1-/NULL`},
		{`a < 1 AND b != 1`, `a,b`, `/!NULL-/1`, `/0-/NULL`},
		{`a < 1 AND b > 1`, `a,b`, `/!NULL-/1`, `/0-/NULL`},
		{`a < 1 AND b >= 1`, `a,b`, `/!NULL-/1`, `/0-/NULL`},
		{`a < 1 AND b < 1`, `a,b`, `/!NULL-/0/1`, `/0/0-/NULL`},
		{`a < 1 AND b <= 1`, `a,b`, `/!NULL-/0/2`, `/0/1-/NULL`},
		{`a < 1 AND b IS NULL`, `a,b`, `/!NULL-/0/!NULL`, `/0/NULL-/NULL`},
		{`a < 1 AND b IS NOT NULL`, `a,b`, `/!NULL-/1`, `/0-/NULL`},
		{`a < 1 AND b IS NOT DISTINCT FROM NULL`, `a,b`, `/!NULL-/0/!NULL`, `/0/NULL-/NULL`},
		{`a < 1 AND b IS DISTINCT FROM NULL`, `a,b`, `/!NULL-/1`, `/0-/NULL`},

		{`a <= 1 AND b = 1`, `a,b`, `/!NULL-/1/2`, `/1/1-/NULL`},
		{`a <= 1 AND b != 1`, `a,b`, `/!NULL-/2`, `/1-/NULL`},
		{`a <= 1 AND b > 1`, `a,b`, `/!NULL-/2`, `/1-/NULL`},
		{`a <= 1 AND b >= 1`, `a,b`, `/!NULL-/2`, `/1-/NULL`},
		{`a <= 1 AND b < 1`, `a,b`, `/!NULL-/1/1`, `/1/0-/NULL`},
		{`a <= 1 AND b <= 1`, `a,b`, `/!NULL-/1/2`, `/1/1-/NULL`},
		{`a <= 1 AND b IS NULL`, `a,b`, `/!NULL-/1/!NULL`, `/1/NULL-/NULL`},
		{`a <= 1 AND b IS NOT NULL`, `a,b`, `/!NULL-/2`, `/1-/NULL`},
		{`a <= 1 AND b IS NOT DISTINCT FROM NULL`, `a,b`, `/!NULL-/1/!NULL`, `/1/NULL-/NULL`},
		{`a <= 1 AND b IS DISTINCT FROM NULL`, `a,b`, `/!NULL-/2`, `/1-/NULL`},

		{`a IN (1) AND b = 1`, `a,b`, `/1/1-/1/2`, `/1/1-/1/0`},
		{`a IN (1) AND b != 1`, `a,b`, `/1/!NULL-/2`, `/1-/1/NULL`},
		{`a IN (1) AND b > 1`, `a,b`, `/1/2-/2`, `/1-/1/1`},
		{`a IN (1) AND b >= 1`, `a,b`, `/1/1-/2`, `/1-/1/0`},
		{`a IN (1) AND b < 1`, `a,b`, `/1/!NULL-/1/1`, `/1/0-/1/NULL`},
		{`a IN (1) AND b <= 1`, `a,b`, `/1/!NULL-/1/2`, `/1/1-/1/NULL`},
		{`a IN (1) AND b IS NULL`, `a,b`, `/1/NULL-/1/!NULL`, `/1/NULL-/0`},
		{`a IN (1) AND b IS NOT NULL`, `a,b`, `/1/!NULL-/2`, `/1-/1/NULL`},
		{`a IN (1) AND b IS NOT DISTINCT FROM NULL`, `a,b`, `/1/NULL-/1/!NULL`, `/1/NULL-/0`},
		{`a IN (1) AND b IS DISTINCT FROM NULL`, `a,b`, `/1/!NULL-/2`, `/1-/1/NULL`},

		{`(a, b) = (1, 2)`, `a`, `/1-/2`, `/1-/0`},
		{`(a, b) = (1, 2)`, `a,b`, `/1/2-/1/3`, `/1/2-/1/1`},

		{`a > 1 OR a >= 5`, `a`, `/2-`, `-/1`},
		{`a < 5 OR a >= 1`, `a`, `/!NULL-`, `-/NULL`},
		{`a < 1 OR a >= 5`, `a`, `/!NULL-/1 /5-`, `-/4 /0-/NULL`},
		{`a = 1 OR a > 8`, `a`, `/1-/2 /9-`, `-/8 /1-/0`},
		{`a = 8 OR a > 1`, `a`, `/2-`, `-/1`},
		{`a < 1 OR a = 5 OR a > 8`, `a`, `/!NULL-/1 /5-/6 /9-`, `-/8 /5-/4 /0-/NULL`},
		{`a < 8 OR a = 8 OR a > 8`, `a`, `/!NULL-`, `-/NULL`},

		{`(a = 1 AND b = 5) OR (a = 3 AND b = 7)`, `a`, `/1-/2 /3-/4`, `/3-/2 /1-/0`},
		{`(a = 1 AND b = 5) OR (a = 3 AND b = 7)`, `b`, `/5-/6 /7-/8`, `/7-/6 /5-/4`},
		{`(a = 1 AND b = 5) OR (a = 3 AND b = 7)`, `a,b`,
			`/1/5-/1/6 /3/7-/3/8`, `/3/7-/3/6 /1/5-/1/4`},

		{`(a = 1 AND b < 5) OR (a = 3 AND b > 7)`, `a`, `/1-/2 /3-/4`, `/3-/2 /1-/0`},
		{`(a = 1 AND b < 5) OR (a = 3 AND b > 7)`, `b`, `/!NULL-/5 /8-`, `-/7 /4-/NULL`},
		{`(a = 1 AND b < 5) OR (a = 3 AND b > 7)`, `a,b`,
			`/1/!NULL-/1/5 /3/8-/4`, `/3-/3/7 /1/4-/1/NULL`},

		{`(a = 1 AND b > 5) OR (a = 3 AND b > 7)`, `a`, `/1-/2 /3-/4`, `/3-/2 /1-/0`},
		{`(a = 1 AND b > 5) OR (a = 3 AND b > 7)`, `b`, `/6-`, `-/5`},
		{`(a = 1 AND b > 5) OR (a = 3 AND b > 7)`, `a,b`,
			`/1/6-/2 /3/8-/4`, `/3-/3/7 /1-/1/5`},

		{`(a = 1 AND b > 5) OR (a = 3 AND b < 7)`, `a`, `/1-/2 /3-/4`, `/3-/2 /1-/0`},
		{`(a = 1 AND b > 5) OR (a = 3 AND b < 7)`, `b`, `/!NULL-`, `-/NULL`},
		{`(a = 1 AND b > 5) OR (a = 3 AND b < 7)`, `a,b`,
			`/1/6-/2 /3/!NULL-/3/7`, `/3/6-/3/NULL /1-/1/5`},

		{`(a < 1 AND b < 5) OR (a > 3 AND b > 7)`, `a`, `/!NULL-/1 /4-`, `-/3 /0-/NULL`},
		{`(a < 1 AND b < 5) OR (a > 3 AND b > 7)`, `b`, `/!NULL-/5 /8-`, `-/7 /4-/NULL`},
		{`(a < 1 AND b < 5) OR (a > 3 AND b > 7)`, `a,b`,
			`/!NULL-/0/5 /4/8-`, `-/4/7 /0/4-/NULL`},

		{`(a > 3 AND b < 5) OR (a < 1 AND b > 7)`, `a`, `/!NULL-/1 /4-`, `-/3 /0-/NULL`},
		{`(a > 3 AND b < 5) OR (a < 1 AND b > 7)`, `b`, `/!NULL-/5 /8-`, `-/7 /4-/NULL`},
		{`(a > 3 AND b < 5) OR (a < 1 AND b > 7)`, `a,b`,
			`/!NULL-/1 /4-`, `-/3 /0-/NULL`},

		{`(a > 1 AND b < 5) OR (a < 3 AND b > 7)`, `a`, `/!NULL-`, `-/NULL`},
		{`(a > 1 AND b < 5) OR (a < 3 AND b > 7)`, `b`, `/!NULL-/5 /8-`, `-/7 /4-/NULL`},
		{`(a > 1 AND b < 5) OR (a < 3 AND b > 7)`, `a,b`, `/!NULL-`, `-/NULL`},

		{`(a = 5) OR (a, b) IN ((1, 1), (3, 3))`, `a`, `/1-/2 /3-/4 /5-/6`, `/5-/4 /3-/2 /1-/0`},
		{`(a = 5) OR (a, b) IN ((1, 1), (3, 3))`, `b`, `-`, `-`},
		{`(a = 5) OR (a, b) IN ((1, 1), (3, 3))`, `a,b`,
			`/1/1-/1/2 /3/3-/3/4 /5-/6`, `/5-/4 /3/3-/3/2 /1/1-/1/0`},

		{fmt.Sprintf(`a = %d`, math.MaxInt64), `a`,
			`/9223372036854775807-/9223372036854775807/PrefixEnd`,
			`/9223372036854775807-/9223372036854775806`},
		{fmt.Sprintf(`a = %d`, math.MinInt64), `a`,
			`/-9223372036854775808-/-9223372036854775807`,
			`/-9223372036854775808-/-9223372036854775808/PrefixEnd`},

		{`(a, b) >= (1, 4)`, `a,b`, `/1/4-`, `-/1/3`},
		{`(a, b) > (1, 4)`, `a,b`, `/1/5-`, `-/1/4`},
		{`(a, b) < (1, 4)`, `a,b`, `/!NULL-/1/4`, `/1/3-/NULL`},
		{`(a, b) <= (1, 4)`, `a,b`, `/!NULL-/1/5`, `/1/4-/NULL`},
		{`(a, b) = (1, 4)`, `a,b`, `/1/4-/1/5`, `/1/4-/1/3`},
		{`(a, b) != (1, 4)`, `a,b`, `-`, `-`},
	}
	p := makeTestPlanner()
	for _, d := range testData {
		t.Run(d.expr, func(t *testing.T) {
			for _, dir := range []encoding.Direction{encoding.Ascending, encoding.Descending} {
				var expected string
				if dir == encoding.Ascending {
					expected = d.expectedAsc
				} else {
					expected = d.expectedDesc
				}
				p.extendedEvalCtx = makeTestingExtendedEvalContext()
				defer p.extendedEvalCtx.Stop(context.Background())
				sel := makeSelectNode(t, p)
				columns := strings.Split(d.columns, ",")
				dirs := make([]encoding.Direction, 0, len(columns))
				for range columns {
					dirs = append(dirs, dir)
				}
				desc, index := makeTestIndex(t, columns, dirs)
				constraints, _ := makeConstraints(t, p, d.expr, desc, index, sel)
				spans, err := makeSpans(p.EvalContext(), constraints, desc, index)
				if err != nil {
					t.Fatal(err)
				}
				s := sqlbase.PrettySpans(index, spans, 2)
				if expected != s {
					t.Errorf("[index direction: %d] %s: expected %s, but found %s", dir, d.expr, expected, s)
					for _, sp := range spans {
						t.Errorf("Start: %x   End: %x", sp.Key, sp.EndKey)
					}
				}
			}
		})
	}

	// Test indexes with mixed-directions (some cols Asc, some cols Desc) and other edge cases.
	testData2 := []struct {
		expr     string
		columns  string
		expected string
	}{
		{`a = 1 AND b = 5`, `a,b-,c`, `/1/5-/1/4`},
		{`a = 7 AND b IN (1,2,3) AND c = false`, `a,b-,c`,
			`/7/3/0-/7/3/1 /7/2/0-/7/2/1 /7/1/0-/7/1/1`},
		// Test different directions for te columns inside a tuple.
		{`(a,b,j) IN ((1,2,3), (4,5,6))`, `a-,b,j-`, `/4/5/6-/4/5/5 /1/2/3-/1/2/2`},
		{`k = b'\xff'`, `k`, `/"\xff"-/"\xff"/PrefixEnd`},
		// Test that limits on bytes work correctly: when encoding a descending limit for bytes,
		// we need to go outside the bytes encoding.
		// "\xaa" is encoded as [bytesDescMarker, ^0xaa, <term escape sequence>]
		{`k = b'\xaa'`, `k-`,
			fmt.Sprintf("raw:%c%c\xff\xfe-%c%c\xff\xff",
				encoding.BytesDescMarker, ^byte(0xaa), encoding.BytesDescMarker, ^byte(0xaa))},

		// Ensure tuples with differing index directions aren't constrained.
		// TODO(mjibson): fix this, see #6346
		{`(a, b) >= (1, 4)`, `a-,b`, `-`},
		{`(a, b) >= (1, 4)`, `a,b-`, `-`},
	}

	for _, d := range testData2 {
		t.Run(d.expr+"~"+d.expected, func(t *testing.T) {
			p.extendedEvalCtx = makeTestingExtendedEvalContext()
			defer p.extendedEvalCtx.Stop(context.Background())
			sel := makeSelectNode(t, p)
			desc, index := makeTestIndexFromStr(t, d.columns)
			constraints, _ := makeConstraints(t, p, d.expr, desc, index, sel)
			spans, err := makeSpans(p.EvalContext(), constraints, desc, index)
			if err != nil {
				t.Fatal(err)
			}
			var got string
			raw := false
			if strings.HasPrefix(d.expected, "raw:") {
				raw = true
				span := spans[0]
				d.expected = d.expected[4:]
				// Trim the index prefix from the span.
				prefix := string(sqlbase.MakeIndexKeyPrefix(desc, index.ID))
				got = strings.TrimPrefix(string(span.Key), prefix) + "-" +
					strings.TrimPrefix(string(span.EndKey), prefix)
			} else {
				got = sqlbase.PrettySpans(index, spans, 2)
			}
			if d.expected != got {
				if !raw {
					t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, got)
				} else {
					t.Errorf("%s: expected %# x, but found %# x", d.expr, []byte(d.expected), got)
				}
			}
		})
	}
}

func TestExactPrefix(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		expr     string
		columns  string
		expected int
	}{
		{`c`, `c`, 1},
		{`c = true`, `c`, 1},
		{`c = false`, `c`, 1},
		{`c != true`, `c`, 0},
		{`c != false`, `c`, 0},
		{`NOT c`, `c`, 0},
		{`c IS TRUE`, `c`, 1},
		{`c IS NOT TRUE`, `c`, 0},
		{`c IS FALSE`, `c`, 1},
		{`c IS NOT FALSE`, `c`, 0},
		{`c IS NOT DISTINCT FROM TRUE`, `c`, 1},
		{`c IS DISTINCT FROM TRUE`, `c`, 0},
		{`c IS NOT DISTINCT FROM FALSE`, `c`, 1},
		{`c IS DISTINCT FROM FALSE`, `c`, 0},

		{`a = 1`, `a`, 1},
		{`a != 1`, `a`, 0},
		{`a IN (1)`, `a`, 1},
		{`a = 1 AND b = 1`, `a,b`, 2},
		{`(a, b) IN ((1, 2))`, `a,b`, 2},
		{`(a, b) IN ((1, 2))`, `a`, 1},
		{`(a, b) IN ((1, 2))`, `b`, 1},
		{`(a, b) IN ((1, 2)) AND c = true`, `a,b,c`, 3},
		{`a = 1 AND (b, c) IN ((2, true))`, `a,b,c`, 3},

		{`(a, b) = (1, 2) OR (a, b, c) = (1, 3, true)`, `a,b`, 1},
		{`(a, b) = (1, 2) OR (a, b, c) = (3, 4, true)`, `a,b`, 0},
		{`(a, b) = (1, 2) OR a = 1`, `a,b`, 1},
		{`(a, b) = (1, 2) OR a = 2`, `a,b`, 0},

		{`a = 1 OR (a = 1 AND b = 2)`, `a`, 1},
		{`a = 1 OR (a = 1 AND b = 2)`, `b`, 0},
		{`a = 1 OR (a = 1 AND b = 2)`, `a,b`, 1},
		{`a = 1 OR (a = 2 AND b = 2)`, `a`, 0},

		{`(a = 1 AND b = 2) OR (a = 1 AND b = 2)`, `a`, 1},
		{`(a = 1 AND b = 2) OR (a = 1 AND b = 2)`, `b`, 1},
		{`(a = 1 AND b = 2) OR (a = 1 AND b = 2)`, `b,a`, 2},
		{`(a = 1 AND b = 2) OR (a = 1 AND b = 2)`, `a,b`, 2},
		{`(a = 1 AND b = 1) OR (a = 1 AND b = 2)`, `a`, 1},
		{`(a = 1 AND b = 1) OR (a = 1 AND b = 2)`, `b`, 0},
		{`(a = 1 AND b = 1) OR (a = 1 AND b = 2)`, `a,b`, 1},
		{`(a = 1 AND b = 1) OR (a = 1 AND b = 2)`, `b,a`, 0},
		{`(a = 1 AND b = 1) OR (a = 2 AND b = 2)`, `a`, 0},
		{`(a = 1 AND b = 1) OR (a = 2 AND b = 2)`, `b`, 0},
		{`(a = 1 AND b = 1) OR (a = 2 AND b = 2)`, `a,b`, 0},

		{`(a = 1 AND b > 4) OR (a = 1 AND b < 1)`, `a`, 1},
		{`(a = 1 AND b > 4) OR (a = 1 AND b < 1)`, `b`, 0},
		{`(a = 1 AND b > 4) OR (a = 1 AND b < 1)`, `a,b`, 1},
	}
	p := makeTestPlanner()
	for _, d := range testData {
		t.Run(fmt.Sprintf("%s~%d", d.expr, d.expected), func(t *testing.T) {
			p.extendedEvalCtx = makeTestingExtendedEvalContext()
			defer p.extendedEvalCtx.Stop(context.Background())
			sel := makeSelectNode(t, p)
			desc, index := makeTestIndexFromStr(t, d.columns)
			constraints, _ := makeConstraints(t, p, d.expr, desc, index, sel)
			prefix := constraints.exactPrefix(p.EvalContext())
			if d.expected != prefix {
				t.Errorf("%s: expected %d, but found %d", d.expr, d.expected, prefix)
			}
		})
	}
}

func TestApplyConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		expr     string
		columns  string
		expected string
	}{
		{`a = 1`, `a`, `<nil>`},
		{`a = 1 AND b = 1`, `a,b`, `<nil>`},
		{`a = 1 AND b = 1`, `a`, `b = 1`},
		{`a = 1 AND b = 1`, `b`, `a = 1`},
		{`a = 1 AND b > 1`, `a,b`, `<nil>`},
		{`a > 1 AND b = 1`, `a,b`, `b = 1`},
		{`a IN (1)`, `a`, `<nil>`},
		{`a IN (1) OR a IN (2)`, `a`, `<nil>`},
		{`a = 1 OR a = 2`, `a`, `<nil>`},
		{`a = 1 OR b = 2`, `a`, `(a = 1) OR (b = 2)`},
		{`NOT (a != 1)`, `a`, `<nil>`},
		{`a != 1`, `a`, `a != 1`},
		{`a IS NOT NULL`, `a`, `<nil>`},
		{`a IS DISTINCT FROM NULL`, `a`, `<nil>`},
		{`a = 1 AND b IS NOT NULL`, `a,b`, `<nil>`},
		{`a = 1 AND b IS DISTINCT FROM NULL`, `a,b`, `<nil>`},
		{`a >= 1 AND b = 2`, `a,b`, `b = 2`},
		{`a >= 1 AND a <= 3 AND b = 2`, `a,b`, `b = 2`},
		{`(a, b) = (1, 2) AND c IS NOT NULL`, `a,b,c`, `<nil>`},
		{`(a, b) = (1, 2) AND c IS DISTINCT FROM NULL`, `a,b,c`, `<nil>`},
		{`a IN (1, 2) AND b = 3`, `a,b`, `b = 3`},
		{`a <= 5 AND b >= 6 AND (a, b) IN ((1, 2))`, `a,b`, `false`},
		{`a IN (1) AND a = 1`, `a`, `<nil>`},
		{`(a, b) = (1, 2)`, `a`, `b = 2`},
		{`(a, b) != (1, 2)`, `a,b`, `(a, b) != (1, 2)`},
		{`a > 1`, `a`, `<nil>`},
		{`a < 1`, `a`, `<nil>`},
		// The constraint (l, m) < (123, 456) must be treated as implying
		// l <= 123. This means that l < 123 definitely cannot be
		// simplified.
		// Note 1: we use DECIMAL columns so that constraint extraction
		// cannot change the < constraint on the left <= by applying
		// Next(). Any column type without a Next() would do.
		// Note 2: we use a COALESCE expression to make the
		// sub-expression "l < 123" invisible to constraint analysis; any
		// function that returns an opaque boolean based on a boolean
		// argument would do.
		{`(l, m) < (123, 456) AND COALESCE(l < 123, true)`, `l,m`,
			`((l, m) < (123, 456)) AND COALESCE(l < 123, true)`},
		// Same for the other direction.
		{`(l, m) > (123, 456) AND COALESCE(l > 123, true)`, `l,m`,
			`((l, m) > (123, 456)) AND COALESCE(l > 123, true)`},
		// The constraint a <= 1 implies that a != 2 is true.
		// Note: we use COALESCE so that the sub-expression a != 2 is not
		// elided during constraint analysis before constraint
		// propagation.
		{`a <= 1 AND COALESCE(a != 2, true)`, `a`, `COALESCE(true, true)`},
		// Regression tests: #13707
		// The following tests must achieve a constraint on `a` and an
		// expression to simplify that contains `a` in a previously
		// unhandled comparison operator. We use OR so that analyzeExpr
		// doesn't decompose further.
		{`a < 3 AND (b < 2 OR a IN (0,1,2))`, `a`, `(b < 2) OR (a IN (0, 1, 2))`},
		{`a < 3 AND (b < 2 OR a NOT IN (0,1,2))`, `a`, `(b < 2) OR (a NOT IN (0, 1, 2))`},
		{`a < 3 AND (b < 2 OR a = ANY ARRAY[0,1,2])`, `a`, `(b < 2) OR (a = ANY ARRAY[0,1,2])`},
		{`a IN (0, 2, 3) AND (b < 2 OR a <= 4)`, `a`, `(b < 2) OR (a <= 4)`},
		{`a IN (0, 2, 3) AND (b < 2 OR a = 2)`, `a`, `(b < 2) OR (a = 2)`},
	}
	p := makeTestPlanner()
	for _, d := range testData {
		t.Run(d.expr+"~"+d.expected, func(t *testing.T) {
			p.extendedEvalCtx = makeTestingExtendedEvalContext()
			defer p.extendedEvalCtx.Stop(context.Background())
			sel := makeSelectNode(t, p)
			desc, index := makeTestIndexFromStr(t, d.columns)
			constraints, expr := makeConstraints(t, p, d.expr, desc, index, sel)
			expr2 := applyIndexConstraints(p.EvalContext(), expr, constraints)
			if s := fmt.Sprint(expr2); d.expected != s {
				t.Errorf("%s: expected %s, but found %s (constraints %s)", d.expr, d.expected, s, constraints)
			}
		})
	}
}
