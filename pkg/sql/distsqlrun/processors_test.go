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
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package distsqlrun

import (
	"strconv"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestPostProcess(t *testing.T) {
	defer leaktest.AfterTest(t)()

	columnTypeInt := sqlbase.ColumnType{Kind: sqlbase.ColumnType_INT}
	v := [10]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(columnTypeInt, parser.NewDInt(parser.DInt(i)))
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
		expNeededCols []int
		expected      string
	}{
		{
			post:          PostProcessSpec{},
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 1 2] [0 1 3] [0 1 4] [0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4] [2 3 4]]",
		},

		// Filter.
		{
			post: PostProcessSpec{
				Filter: Expression{Expr: "@1 = 1"},
			},
			expNeededCols: []int{0, 1, 2},
			expected:      "[[1 2 3] [1 2 4] [1 3 4]]",
		},

		// Projection.
		{
			post: PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 2},
			},
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
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 3] [0 4] [1 3] [1 4]]",
		},

		// Rendering.
		{
			post: PostProcessSpec{
				RenderExprs: []Expression{{Expr: "@1"}, {Expr: "@2"}, {Expr: "@1 + @2"}},
			},
			expNeededCols: []int{0, 1},
			expected:      "[[0 1 1] [0 1 1] [0 1 1] [0 2 2] [0 2 2] [0 3 3] [1 2 3] [1 2 3] [1 3 4] [2 3 5]]",
		},

		// Rendering and filtering; filter refers to column used in rendering.
		{
			post: PostProcessSpec{
				Filter:      Expression{Expr: "@2 = 2"},
				RenderExprs: []Expression{{Expr: "@1"}, {Expr: "@2"}, {Expr: "@1 + @2"}},
			},
			expNeededCols: []int{0, 1},
			expected:      "[[0 2 2] [0 2 2] [1 2 3] [1 2 3]]",
		},

		// Rendering and filtering; filter refers to column not used in rendering.
		{
			post: PostProcessSpec{
				Filter:      Expression{Expr: "@3 = 4"},
				RenderExprs: []Expression{{Expr: "@1"}, {Expr: "@2"}, {Expr: "@1 + @2"}},
			},
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
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4] [2 3 4]]",
		},

		// Limit.
		{
			post:          PostProcessSpec{Limit: 3},
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 1 2] [0 1 3] [0 1 4]]",
		},
		{
			post:          PostProcessSpec{Limit: 9},
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 1 2] [0 1 3] [0 1 4] [0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4]]",
		},
		{
			post:          PostProcessSpec{Limit: 10},
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 1 2] [0 1 3] [0 1 4] [0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4] [2 3 4]]",
		},
		{
			post:          PostProcessSpec{Limit: 11},
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 1 2] [0 1 3] [0 1 4] [0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4] [2 3 4]]",
		},

		// Offset + limit.
		{
			post:          PostProcessSpec{Offset: 3, Limit: 2},
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 2 3] [0 2 4]]",
		},
		{
			post:          PostProcessSpec{Offset: 3, Limit: 6},
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4]]",
		},
		{
			post:          PostProcessSpec{Offset: 3, Limit: 7},
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4] [2 3 4]]",
		},
		{
			post:          PostProcessSpec{Offset: 3, Limit: 8},
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4] [2 3 4]]",
		},

		// Filter + offset.
		{
			post: PostProcessSpec{
				Filter: Expression{Expr: "@1 = 1"},
				Offset: 1,
			},
			expNeededCols: []int{0, 1, 2},
			expected:      "[[1 2 4] [1 3 4]]",
		},

		// Filter + limit.
		{
			post: PostProcessSpec{
				Filter: Expression{Expr: "@1 = 1"},
				Limit:  2,
			},
			expNeededCols: []int{0, 1, 2},
			expected:      "[[1 2 3] [1 2 4]]",
		},
	}

	for tcIdx, tc := range testCases {
		t.Run(strconv.Itoa(tcIdx), func(t *testing.T) {
			inBuf := NewRowBuffer(nil /* types */, input, RowBufferArgs{})
			outBuf := &RowBuffer{}

			var out procOutputHelper
			if err := out.init(&tc.post, inBuf.Types(), &parser.EvalContext{}, outBuf); err != nil {
				t.Fatal(err)
			}

			// Verify neededColumns().
			neededCols := out.neededColumns()
			if len(neededCols) != len(input[0]) {
				t.Fatalf("invalid neededCols length %d, expected %d", len(neededCols), len(input[0]))
			}
			expNeeded := make([]bool, len(input[0]))
			for _, col := range tc.expNeededCols {
				expNeeded[col] = true
			}
			for i := range neededCols {
				if neededCols[i] != expNeeded[i] {
					t.Errorf("column %d needed %t, expected %t", i, neededCols[i], expNeeded[i])
				}
			}
			// Run the rows through the helper.
			for i := range input {
				status, err := out.emitRow(context.TODO(), input[i])
				if err != nil {
					t.Fatal(err)
				}
				if status != NeedMoreRows {
					out.close()
					break
				}
			}
			var res sqlbase.EncDatumRows
			for {
				row, meta := outBuf.Next()
				if !meta.Empty() {
					t.Fatalf("unexpected metadata: %v", meta)
				}
				if row == nil {
					break
				}
				res = append(res, row)
			}

			if str := res.String(); str != tc.expected {
				t.Errorf("expected output:\n    %s\ngot:\n    %s\n", tc.expected, str)
			}
		})
	}
}
