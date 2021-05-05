// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecagg"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestDefaultAggregateFunc(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCases := []aggregatorTestCase{
		{
			name: "StringAgg",
			typs: []*types.T{types.Int, types.String, types.String},
			input: colexectestutils.Tuples{
				{nil, "a", "1"},
				{nil, "b", "2"},
				{0, "c", "3"},
				{0, "d", "4"},
				{0, "e", "5"},
				{1, "f", "6"},
				{1, "g", "7"},
			},
			groupCols: []uint32{0},
			aggCols:   [][]uint32{{0}, {1, 2}},
			aggFns: []execinfrapb.AggregatorSpec_Func{
				execinfrapb.AnyNotNull,
				execinfrapb.StringAgg,
			},
			expected: colexectestutils.Tuples{
				{nil, "a2b"},
				{0, "c4d5e"},
				{1, "f7g"},
			},
		},
		{
			name: "StringAggWithConstDelimiter",
			typs: []*types.T{types.Int, types.String},
			input: colexectestutils.Tuples{
				{nil, "a"},
				{nil, "b"},
				{0, "c"},
				{0, "d"},
				{0, "e"},
				{1, "f"},
				{1, "g"},
			},
			groupCols: []uint32{0},
			aggCols:   [][]uint32{{0}, {1}},
			aggFns: []execinfrapb.AggregatorSpec_Func{
				execinfrapb.AnyNotNull,
				execinfrapb.StringAgg,
			},
			expected: colexectestutils.Tuples{
				{nil, "a_b"},
				{0, "c_d_e"},
				{1, "f_g"},
			},
			constArguments: [][]execinfrapb.Expression{nil, {{Expr: "'_'"}}},
		},
		{
			name: "JsonAggWithStringAgg",
			typs: []*types.T{types.Int, types.Jsonb, types.String},
			input: colexectestutils.Tuples{
				{nil, `{"id": 1}`, "a"},
				{nil, `{"id": 2}`, "b"},
				{0, `{"id": 1}`, "c"},
				{0, `{"id": 2}`, "d"},
				{0, `{"id": 2}`, "e"},
				{1, `{"id": 3}`, "f"},
			},
			groupCols: []uint32{0},
			aggCols:   [][]uint32{{0}, {1}, {2}, {2}},
			aggFns: []execinfrapb.AggregatorSpec_Func{
				execinfrapb.AnyNotNull,
				execinfrapb.JSONAgg,
				execinfrapb.JSONAgg,
				execinfrapb.StringAgg,
			},
			expected: colexectestutils.Tuples{
				{nil, `[{"id": 1}, {"id": 2}]`, `["a", "b"]`, "a_b"},
				{0, `[{"id": 1}, {"id": 2}, {"id": 2}]`, `["c", "d", "e"]`, "c_d_e"},
				{1, `[{"id": 3}]`, `["f"]`, "f"},
			},
			constArguments: [][]execinfrapb.Expression{nil, nil, nil, {{Expr: "'_'"}}},
		},
		{
			name: "XorAgg",
			typs: types.TwoIntCols,
			input: colexectestutils.Tuples{
				{nil, 3},
				{nil, 1},
				{0, -5},
				{0, -1},
				{0, 0},
			},
			groupCols: []uint32{0},
			aggCols:   [][]uint32{{0}, {1}},
			aggFns: []execinfrapb.AggregatorSpec_Func{
				execinfrapb.AnyNotNull,
				execinfrapb.XorAgg,
			},
			expected: colexectestutils.Tuples{
				{nil, 2},
				{0, 4},
			},
		},
	}

	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	aggMemAcc := evalCtx.Mon.MakeBoundAccount()
	defer aggMemAcc.Close(context.Background())
	evalCtx.SingleDatumAggMemAccount = &aggMemAcc
	semaCtx := tree.MakeSemaContext()
	for _, agg := range aggTypes {
		for _, tc := range testCases {
			t.Run(fmt.Sprintf("%s/%s", agg.name, tc.name), func(t *testing.T) {
				if err := tc.init(); err != nil {
					t.Fatal(err)
				}
				constructors, constArguments, outputTypes, err := colexecagg.ProcessAggregations(
					&evalCtx, &semaCtx, tc.spec.Aggregations, tc.typs,
				)
				require.NoError(t, err)
				colexectestutils.RunTestsWithTyps(t, testAllocator, []colexectestutils.Tuples{tc.input}, [][]*types.T{tc.typs}, tc.expected, colexectestutils.UnorderedVerifier,
					func(input []colexecop.Operator) (colexecop.Operator, error) {
						return agg.new(&colexecagg.NewAggregatorArgs{
							Allocator:      testAllocator,
							MemAccount:     testMemAcc,
							Input:          input[0],
							InputTypes:     tc.typs,
							Spec:           tc.spec,
							EvalCtx:        &evalCtx,
							Constructors:   constructors,
							ConstArguments: constArguments,
							OutputTypes:    outputTypes,
						})
					})
			})
		}
	}
}

func BenchmarkDefaultAggregateFunction(b *testing.B) {
	aggFn := execinfrapb.StringAgg
	for _, agg := range aggTypes {
		for _, numInputRows := range []int{32, 32 * coldata.BatchSize()} {
			for _, groupSize := range []int{1, 2, 32, 128, coldata.BatchSize()} {
				benchmarkAggregateFunction(
					b, agg, aggFn, []*types.T{types.String, types.String}, groupSize,
					0 /* distinctProb */, numInputRows,
				)
			}
		}
	}
}
