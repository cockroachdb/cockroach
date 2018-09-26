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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

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

func makeSpans(
	t *testing.T,
	p *planner,
	sql string,
	desc *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
	sel *renderNode,
) (_ *constraint.Constraint, spans roachpb.Spans) {
	expr := parseAndNormalizeExpr(t, p, sql, sel)

	c := &indexInfo{
		desc:  desc,
		index: index,
	}
	var o xform.Optimizer
	o.Init(p.EvalContext())
	for _, c := range desc.Columns {
		o.Memo().Metadata().AddColumn(c.Name, c.Type.ToDatumType())
	}
	semaCtx := tree.MakeSemaContext(false /* privileged */)
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	bld := optbuilder.NewScalar(context.Background(), &semaCtx, &evalCtx, o.Factory())
	bld.AllowUnsupportedExpr = true
	err := bld.Build(expr)
	if err != nil {
		t.Fatal(err)
	}
	filterExpr := o.Memo().Root()
	err = c.makeIndexConstraints(&o, filterExpr, p.EvalContext())
	if err != nil {
		t.Fatal(err)
	}

	spans, err = spansFromConstraint(desc, index, c.ic.Constraint(), exec.ColumnOrdinalSet{})
	if err != nil {
		t.Fatal(err)
	}
	return c.ic.Constraint(), spans
}

func TestMakeSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Note: these tests are not intended to be exhaustive tests for index
	// constraint generation (opt.IndexConstraints have their own tests).
	// The purpose is to verify the end-to-end process of generating the Spans.

	testData := []struct {
		expr         string
		columns      string
		expectedAsc  string
		expectedDesc string
	}{
		{`c`, `c`, `/1-/2`, `/1-/0`},
		{`a = 1`, `a`, `/1-/2`, `/1-/0`},
		{`q = 1`, `q`, `/1-/2`, `/1-/0`},
		{`a < 1`, `a`, `-/1`, `/0-`},
		{`q < 1`, `q`, `/!NULL-/1`, `/0-/NULL`},
		{`a IS NULL`, `a`, ``, ``},
		{`q IS NULL`, `q`, `/NULL-/!NULL`, `/NULL-`},
		{`a IS NOT NULL`, `a`, `-`, `-`},
		{`q IS NOT NULL`, `q`, `/!NULL-`, `-/NULL`},

		{`a IN (1,2,3)`, `a`, `/1-/4`, `/3-/0`},
		{`a IN (1,2,3) AND b = 1`, `a,b`,
			`/1/1-/1/2 /2/1-/2/2 /3/1-/3/2`, `/3/1-/3/0 /2/1-/2/0 /1/1-/1/0`},
		{`a = 1 AND b IN (1,3,5)`, `a,b`,
			`/1/1-/1/2 /1/3-/1/4 /1/5-/1/6`, `/1/5-/1/4 /1/3-/1/2 /1/1-/1/0`},
		{`a >= 1 AND b IN (1,2,3)`, `a,b`, `/1/1-`, `-/1/0`},
		{`a <= 1 AND b IN (1,2,3)`, `a,b`, `-/1/4`, `/1/3-`},

		{`a = 1 AND b = 1`, `a,b`, `/1/1-/1/2`, `/1/1-/1/0`},
		{`a = 1 AND b != 1`, `a,b`, `/1-/1/1 /1/2-/2`, `/1-/1/1 /1/0-/0`},
		{`a = 1 AND b > 1`, `a,b`, `/1/2-/2`, `/1-/1/1`},
		{`a = 1 AND b >= 1`, `a,b`, `/1/1-/2`, `/1-/1/0`},
		{`a = 1 AND b < 1`, `a,b`, `/1-/1/1`, `/1/0-/0`},
		{`a = 1 AND b <= 1`, `a,b`, `/1-/1/2`, `/1/1-/0`},
		{`a = 1 AND q = 1`, `a,q`, `/1/1-/1/2`, `/1/1-/1/0`},
		{`a = 1 AND q IS NOT NULL`, `a,q`, `/1/!NULL-/2`, `/1-/1/NULL`},

		{`(a, b) = (1, 2)`, `a`, `/1-/2`, `/1-/0`},
		{`(a, b) = (1, 2)`, `a,b`, `/1/2-/1/3`, `/1/2-/1/1`},

		{`a > 1 OR a >= 5`, `a`, `/2-`, `-/1`},
		{`a < 5 OR a >= 1`, `a`, `-`, `-`},
		{`a < 1 OR a >= 5`, `a`, `-/1 /5-`, `-/4 /0-`},
		{`a = 1 OR a > 8`, `a`, `/1-/2 /9-`, `-/8 /1-/0`},
		{`a = 8 OR a > 1`, `a`, `/2-`, `-/1`},
		{`a < 1 OR a = 5 OR a > 8`, `a`, `-/1 /5-/6 /9-`, `-/8 /5-/4 /0-`},

		{fmt.Sprintf(`a = %d`, math.MaxInt64), `a`,
			`/9223372036854775807-/9223372036854775807/PrefixEnd`,
			`/9223372036854775807-/9223372036854775806`},
		{fmt.Sprintf(`a = %d`, math.MinInt64), `a`,
			`/-9223372036854775808-/-9223372036854775807`,
			`/-9223372036854775808-/-9223372036854775808/PrefixEnd`},

		{`(a, b) >= (1, 4)`, `a,b`, `/1/4-`, `-/1/3`},
		{`(a, b) > (1, 4)`, `a,b`, `/1/5-`, `-/1/4`},
		{`(a, b) < (1, 4)`, `a,b`, `-/1/4`, `/1/3-`},
		{`(a, b) <= (1, 4)`, `a,b`, `-/1/5`, `/1/4-`},
		{`(a, b) = (1, 4)`, `a,b`, `/1/4-/1/5`, `/1/4-/1/3`},
		{`(a, b) != (1, 4)`, `a,b`, `-/1/4 /1/5-`, `-/1/4 /1/3-`},
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
				p.extendedEvalCtx = makeTestingExtendedEvalContext(cluster.MakeTestingClusterSettings())
				defer p.extendedEvalCtx.Stop(context.Background())
				sel := makeSelectNode(t, p)
				columns := strings.Split(d.columns, ",")
				dirs := make([]encoding.Direction, 0, len(columns))
				for range columns {
					dirs = append(dirs, dir)
				}
				desc, index := makeTestIndex(t, columns, dirs)
				_, spans := makeSpans(t, p, d.expr, desc, index, sel)

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

		// Tuples with differing index directions.
		{`(a, b) >= (1, 4)`, `a-,b`, `-/0`},
		{`(a, b) >= (1, 4)`, `a,b-`, `/1-`},
	}

	for _, d := range testData2 {
		t.Run(d.expr+"~"+d.expected, func(t *testing.T) {
			p.extendedEvalCtx = makeTestingExtendedEvalContext(cluster.MakeTestingClusterSettings())
			defer p.extendedEvalCtx.Stop(context.Background())
			sel := makeSelectNode(t, p)
			desc, index := makeTestIndexFromStr(t, d.columns)
			_, spans := makeSpans(t, p, d.expr, desc, index, sel)
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
		{`NOT c`, `c`, 1},
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
		{`(a, b) IN ((1, 2))`, `b`, 2}, // 2 because a is the primary key.
		{`(q, b) IN ((1, 2))`, `b`, 1},
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
		{`(a = 1 AND b = 2) OR (a = 1 AND b = 2)`, `b`, 2}, // 2 because a is the primary key.
		{`(q = 1 AND b = 2) OR (q = 1 AND b = 2)`, `b`, 1},
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
			p.extendedEvalCtx = makeTestingExtendedEvalContext(cluster.MakeTestingClusterSettings())
			defer p.extendedEvalCtx.Stop(context.Background())
			sel := makeSelectNode(t, p)
			desc, index := makeTestIndexFromStr(t, d.columns)
			c, _ := makeSpans(t, p, d.expr, desc, index, sel)
			prefix := c.ExactPrefix(p.EvalContext())
			if d.expected != prefix {
				t.Errorf("%s: expected %d, but found %d", d.expr, d.expected, prefix)
			}
		})
	}
}
