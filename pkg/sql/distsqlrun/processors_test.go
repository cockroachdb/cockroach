// Copyright 2017 The Cockroach Authors.
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

package distsqlrun

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestPostProcess(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := [10]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(intType, tree.NewDInt(tree.DInt(i)))
	}

	// We run the same input rows through various PostProcessSpecs.
	input := sqlbase.EncDatumRows{
		{v[0], v[1], v[2]},
		{v[0], v[1], v[3]},
		{v[0], v[1], v[4]},
		{v[0], v[2], v[3]},
		{v[0], v[2], v[4]},
		{v[0], v[3], v[4]},
		{v[1], v[2], v[3]},
		{v[1], v[2], v[4]},
		{v[1], v[3], v[4]},
		{v[2], v[3], v[4]},
	}

	testCases := []struct {
		post          PostProcessSpec
		outputTypes   []sqlbase.ColumnType
		expNeededCols []int
		expected      string
	}{
		{
			post:          PostProcessSpec{},
			outputTypes:   threeIntCols,
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 1 2] [0 1 3] [0 1 4] [0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4] [2 3 4]]",
		},

		// Filter.
		{
			post: PostProcessSpec{
				Filter: Expression{Expr: "@1 = 1"},
			},
			outputTypes:   threeIntCols,
			expNeededCols: []int{0, 1, 2},
			expected:      "[[1 2 3] [1 2 4] [1 3 4]]",
		},

		// Projection.
		{
			post: PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 2},
			},
			outputTypes:   twoIntCols,
			expNeededCols: []int{0, 2},
			expected:      "[[0 2] [0 3] [0 4] [0 3] [0 4] [0 4] [1 3] [1 4] [1 4] [2 4]]",
		},

		// Filter and projection; filter only refers to projected column.
		{
			post: PostProcessSpec{
				Filter:        Expression{Expr: "@1 = 1"},
				Projection:    true,
				OutputColumns: []uint32{0, 2},
			},
			outputTypes:   twoIntCols,
			expNeededCols: []int{0, 2},
			expected:      "[[1 3] [1 4] [1 4]]",
		},

		// Filter and projection; filter refers to non-projected column.
		{
			post: PostProcessSpec{
				Filter:        Expression{Expr: "@2 = 2"},
				Projection:    true,
				OutputColumns: []uint32{0, 2},
			},
			outputTypes:   twoIntCols,
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 3] [0 4] [1 3] [1 4]]",
		},

		// Rendering.
		{
			post: PostProcessSpec{
				RenderExprs: []Expression{{Expr: "@1"}, {Expr: "@2"}, {Expr: "@1 + @2"}},
			},
			outputTypes:   threeIntCols,
			expNeededCols: []int{0, 1},
			expected:      "[[0 1 1] [0 1 1] [0 1 1] [0 2 2] [0 2 2] [0 3 3] [1 2 3] [1 2 3] [1 3 4] [2 3 5]]",
		},

		// Rendering and filtering; filter refers to column used in rendering.
		{
			post: PostProcessSpec{
				Filter:      Expression{Expr: "@2 = 2"},
				RenderExprs: []Expression{{Expr: "@1"}, {Expr: "@2"}, {Expr: "@1 + @2"}},
			},
			outputTypes:   threeIntCols,
			expNeededCols: []int{0, 1},
			expected:      "[[0 2 2] [0 2 2] [1 2 3] [1 2 3]]",
		},

		// Rendering and filtering; filter refers to column not used in rendering.
		{
			post: PostProcessSpec{
				Filter:      Expression{Expr: "@3 = 4"},
				RenderExprs: []Expression{{Expr: "@1"}, {Expr: "@2"}, {Expr: "@1 + @2"}},
			},
			outputTypes:   threeIntCols,
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 1 1] [0 2 2] [0 3 3] [1 2 3] [1 3 4] [2 3 5]]",
		},

		// More complex rendering expressions.
		{
			post: PostProcessSpec{
				RenderExprs: []Expression{
					{Expr: "@1 - @2"},
					{Expr: "@1 + @2 * @3"},
					{Expr: "@1 >= 2"},
					{Expr: "((@1 = @2 - 1))"},
					{Expr: "@1 = @2 - 1 OR @1 = @3 - 2"},
					{Expr: "@1 = @2 - 1 AND @1 = @3 - 2"},
				},
			},
			outputTypes:   []sqlbase.ColumnType{intType, intType, boolType, boolType, boolType, boolType},
			expNeededCols: []int{0, 1, 2},
			expected: "[" + strings.Join([]string{
				/* 0 1 2 */ "[-1 2 false true true true]",
				/* 0 1 3 */ "[-1 3 false true true false]",
				/* 0 1 4 */ "[-1 4 false true true false]",
				/* 0 2 3 */ "[-2 6 false false false false]",
				/* 0 2 4 */ "[-2 8 false false false false]",
				/* 0 3 4 */ "[-3 12 false false false false]",
				/* 1 2 3 */ "[-1 7 false true true true]",
				/* 1 2 4 */ "[-1 9 false true true false]",
				/* 1 3 4 */ "[-2 13 false false false false]",
				/* 2 3 4 */ "[-1 14 true true true true]",
			}, " ") + "]",
		},

		// Offset.
		{
			post:          PostProcessSpec{Offset: 3},
			outputTypes:   threeIntCols,
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4] [2 3 4]]",
		},

		// Limit.
		{
			post:          PostProcessSpec{Limit: 3},
			outputTypes:   threeIntCols,
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 1 2] [0 1 3] [0 1 4]]",
		},
		{
			post:          PostProcessSpec{Limit: 9},
			outputTypes:   threeIntCols,
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 1 2] [0 1 3] [0 1 4] [0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4]]",
		},
		{
			post:          PostProcessSpec{Limit: 10},
			outputTypes:   threeIntCols,
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 1 2] [0 1 3] [0 1 4] [0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4] [2 3 4]]",
		},
		{
			post:          PostProcessSpec{Limit: 11},
			outputTypes:   threeIntCols,
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 1 2] [0 1 3] [0 1 4] [0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4] [2 3 4]]",
		},

		// Offset + limit.
		{
			post:          PostProcessSpec{Offset: 3, Limit: 2},
			outputTypes:   threeIntCols,
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 2 3] [0 2 4]]",
		},
		{
			post:          PostProcessSpec{Offset: 3, Limit: 6},
			outputTypes:   threeIntCols,
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4]]",
		},
		{
			post:          PostProcessSpec{Offset: 3, Limit: 7},
			outputTypes:   threeIntCols,
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4] [2 3 4]]",
		},
		{
			post:          PostProcessSpec{Offset: 3, Limit: 8},
			outputTypes:   threeIntCols,
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4] [2 3 4]]",
		},

		// Filter + offset.
		{
			post: PostProcessSpec{
				Filter: Expression{Expr: "@1 = 1"},
				Offset: 1,
			},
			outputTypes:   threeIntCols,
			expNeededCols: []int{0, 1, 2},
			expected:      "[[1 2 4] [1 3 4]]",
		},

		// Filter + limit.
		{
			post: PostProcessSpec{
				Filter: Expression{Expr: "@1 = 1"},
				Limit:  2,
			},
			outputTypes:   threeIntCols,
			expNeededCols: []int{0, 1, 2},
			expected:      "[[1 2 3] [1 2 4]]",
		},
	}

	for tcIdx, tc := range testCases {
		t.Run(strconv.Itoa(tcIdx), func(t *testing.T) {
			inBuf := NewRowBuffer(threeIntCols, input, RowBufferArgs{})
			outBuf := &RowBuffer{}

			var out ProcOutputHelper
			evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
			defer evalCtx.Stop(context.Background())
			if err := out.Init(&tc.post, inBuf.OutputTypes(), evalCtx, outBuf); err != nil {
				t.Fatal(err)
			}

			// Verify neededColumns().
			count := 0
			neededCols := out.neededColumns()
			neededCols.ForEach(func(_ int) {
				count++
			})
			if count != len(tc.expNeededCols) {
				t.Fatalf("invalid neededCols length %d, expected %d", count, len(tc.expNeededCols))
			}
			for _, col := range tc.expNeededCols {
				if !neededCols.Contains(col) {
					t.Errorf("column %d not found in neededCols", col)
				}
			}
			// Run the rows through the helper.
			for i := range input {
				status, err := out.EmitRow(context.TODO(), input[i])
				if err != nil {
					t.Fatal(err)
				}
				if status != NeedMoreRows {
					out.Close()
					break
				}
			}
			var res sqlbase.EncDatumRows
			for {
				row := outBuf.NextNoMeta(t)
				if row == nil {
					break
				}
				res = append(res, row)
			}

			if str := res.String(tc.outputTypes); str != tc.expected {
				t.Errorf("expected output:\n    %s\ngot:\n    %s\n", tc.expected, str)
			}
		})
	}
}

func TestAggregatorSpecAggregationEquals(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Used for FilterColIdx *uint32.
	colIdx1 := uint32(0)
	colIdx2 := uint32(1)

	for i, tc := range []struct {
		a, b     AggregatorSpec_Aggregation
		expected bool
	}{
		// Func tests.
		{
			a:        AggregatorSpec_Aggregation{Func: AggregatorSpec_IDENT},
			b:        AggregatorSpec_Aggregation{Func: AggregatorSpec_IDENT},
			expected: true,
		},
		{
			a:        AggregatorSpec_Aggregation{Func: AggregatorSpec_IDENT},
			b:        AggregatorSpec_Aggregation{Func: AggregatorSpec_AVG},
			expected: false,
		},

		// ColIdx tests.
		{
			a:        AggregatorSpec_Aggregation{Func: AggregatorSpec_IDENT, ColIdx: []uint32{1, 2}},
			b:        AggregatorSpec_Aggregation{Func: AggregatorSpec_IDENT, ColIdx: []uint32{1, 2}},
			expected: true,
		},
		{
			a:        AggregatorSpec_Aggregation{Func: AggregatorSpec_IDENT, ColIdx: []uint32{1}},
			b:        AggregatorSpec_Aggregation{Func: AggregatorSpec_IDENT, ColIdx: []uint32{1, 3}},
			expected: false,
		},
		{
			a:        AggregatorSpec_Aggregation{Func: AggregatorSpec_IDENT, ColIdx: []uint32{1, 2}},
			b:        AggregatorSpec_Aggregation{Func: AggregatorSpec_IDENT, ColIdx: []uint32{1, 3}},
			expected: false,
		},

		// FilterColIdx tests.
		{
			a:        AggregatorSpec_Aggregation{Func: AggregatorSpec_IDENT, FilterColIdx: &colIdx1},
			b:        AggregatorSpec_Aggregation{Func: AggregatorSpec_IDENT, FilterColIdx: &colIdx1},
			expected: true,
		},
		{
			a:        AggregatorSpec_Aggregation{Func: AggregatorSpec_IDENT, FilterColIdx: &colIdx1},
			b:        AggregatorSpec_Aggregation{Func: AggregatorSpec_IDENT},
			expected: false,
		},
		{
			a:        AggregatorSpec_Aggregation{Func: AggregatorSpec_IDENT, FilterColIdx: &colIdx1},
			b:        AggregatorSpec_Aggregation{Func: AggregatorSpec_IDENT, FilterColIdx: &colIdx2},
			expected: false,
		},

		// Distinct tests.
		{
			a:        AggregatorSpec_Aggregation{Func: AggregatorSpec_IDENT, Distinct: true},
			b:        AggregatorSpec_Aggregation{Func: AggregatorSpec_IDENT, Distinct: true},
			expected: true,
		},
		{
			a:        AggregatorSpec_Aggregation{Func: AggregatorSpec_IDENT, Distinct: false},
			b:        AggregatorSpec_Aggregation{Func: AggregatorSpec_IDENT, Distinct: false},
			expected: true,
		},
		{
			a:        AggregatorSpec_Aggregation{Func: AggregatorSpec_IDENT, Distinct: false},
			b:        AggregatorSpec_Aggregation{Func: AggregatorSpec_IDENT},
			expected: true,
		},
		{
			a:        AggregatorSpec_Aggregation{Func: AggregatorSpec_IDENT, Distinct: true},
			b:        AggregatorSpec_Aggregation{Func: AggregatorSpec_IDENT},
			expected: false,
		},
	} {
		if actual := tc.a.Equals(tc.b); tc.expected != actual {
			t.Fatalf("case %d: incorrect result from %#v.Equals(%#v), expected %t, actual %t", i, tc.a, tc.b, tc.expected, actual)
		}

		// Reflexive case.
		if actual := tc.b.Equals(tc.a); tc.expected != actual {
			t.Fatalf("case %d: incorrect result from %#v.Equals(%#v), expected %t, actual %t", i, tc.b, tc.a, tc.expected, actual)
		}
	}
}

func TestProcessorBaseContext(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	runTest := func(t *testing.T, f func(noop *noopProcessor)) {
		flowCtx := &FlowCtx{
			Ctx:      ctx,
			Settings: st,
			EvalCtx:  tree.MakeTestingEvalContext(st),
		}
		defer flowCtx.EvalCtx.Stop(ctx)

		input := NewRepeatableRowSource(oneIntCol, makeIntRows(10, 1))
		noop, err := newNoopProcessor(flowCtx, input, &PostProcessSpec{}, &RowDisposer{})
		if err != nil {
			t.Fatal(err)
		}

		// The context should be valid before Next is called in case ConsumerDone
		// or ConsumerClosed are called without calling Next.j
		if noop.ctx != ctx {
			t.Fatalf("processorBase.ctx not initialized")
		}
		f(noop)
		// The context should be reset after ConsumerClosed is called so that any
		// subsequent logging calls will not operate on closed spans.
		if noop.ctx != ctx {
			t.Fatalf("processorBase.ctx not reset on close")
		}
	}

	t.Run("next-close", func(t *testing.T) {
		runTest(t, func(noop *noopProcessor) {
			// The normal case: a call to Next followed by the processor being closed.
			noop.Next()
			noop.ConsumerClosed()
		})
	})

	t.Run("close-without-next", func(t *testing.T) {
		runTest(t, func(noop *noopProcessor) {
			// A processor can be closed without Next ever being called.
			noop.ConsumerClosed()
		})
	})

	t.Run("close-next", func(t *testing.T) {
		runTest(t, func(noop *noopProcessor) {
			// After the processor is closed, it can't be opened via a call to Next.
			noop.ConsumerClosed()
			noop.Next()
		})
	})

	t.Run("next-close-next", func(t *testing.T) {
		runTest(t, func(noop *noopProcessor) {
			// A spurious call to Next after the processor is closed.
			noop.Next()
			noop.ConsumerClosed()
			noop.Next()
		})
	})

	t.Run("next-close-close", func(t *testing.T) {
		runTest(t, func(noop *noopProcessor) {
			// Close should be idempotent.
			noop.Next()
			noop.ConsumerClosed()
			noop.ConsumerClosed()
		})
	})
}
