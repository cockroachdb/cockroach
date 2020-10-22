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
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
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
			aggFns: []execinfrapb.AggregatorSpec_Func{
				execinfrapb.AggregatorSpec_ANY_NOT_NULL,
				execinfrapb.AggregatorSpec_STRING_AGG,
			},
			input: tuples{
				{nil, "a", "1"},
				{nil, "b", "2"},
				{0, "c", "3"},
				{0, "d", "4"},
				{0, "e", "5"},
				{1, "f", "6"},
				{1, "g", "7"},
			},
			expected: tuples{
				{nil, "a2b"},
				{0, "c4d5e"},
				{1, "f7g"},
			},
			typs:      []*types.T{types.Int, types.String, types.String},
			name:      "StringAgg",
			groupCols: []uint32{0},
			aggCols:   [][]uint32{{0}, {1, 2}},
		},
		{
			aggFns: []execinfrapb.AggregatorSpec_Func{
				execinfrapb.AggregatorSpec_ANY_NOT_NULL,
				execinfrapb.AggregatorSpec_STRING_AGG,
			},
			input: tuples{
				{nil, "a"},
				{nil, "b"},
				{0, "c"},
				{0, "d"},
				{0, "e"},
				{1, "f"},
				{1, "g"},
			},
			constArguments: [][]execinfrapb.Expression{nil, {{Expr: "'_'"}}},
			expected: tuples{
				{nil, "a_b"},
				{0, "c_d_e"},
				{1, "f_g"},
			},
			typs:      []*types.T{types.Int, types.String},
			name:      "StringAggWithConstDelimiter",
			groupCols: []uint32{0},
			aggCols:   [][]uint32{{0}, {1}},
		},
		{
			aggFns: []execinfrapb.AggregatorSpec_Func{
				execinfrapb.AggregatorSpec_ANY_NOT_NULL,
				execinfrapb.AggregatorSpec_JSON_AGG,
				execinfrapb.AggregatorSpec_JSON_AGG,
				execinfrapb.AggregatorSpec_STRING_AGG,
			},
			typs: []*types.T{types.Int, types.Jsonb, types.String},
			input: tuples{
				{nil, `'{"id": 1}'`, "a"},
				{nil, `'{"id": 2}'`, "b"},
				{0, `'{"id": 1}'`, "c"},
				{0, `'{"id": 2}'`, "d"},
				{0, `'{"id": 2}'`, "e"},
				{1, `'{"id": 3}'`, "f"},
			},
			constArguments: [][]execinfrapb.Expression{nil, nil, nil, {{Expr: "'_'"}}},
			expected: tuples{
				{nil, `'[{"id": 1}, {"id": 2}]'`, `'["a", "b"]'`, "a_b"},
				{0, `'[{"id": 1}, {"id": 2}, {"id": 2}]'`, `'["c", "d", "e"]'`, "c_d_e"},
				{1, `'[{"id": 3}]'`, `'["f"]'`, "f"},
			},
			name:      "JsonAggWithStringAgg",
			groupCols: []uint32{0},
			aggCols:   [][]uint32{{0}, {1}, {2}, {2}},
		},
		{
			aggFns: []execinfrapb.AggregatorSpec_Func{
				execinfrapb.AggregatorSpec_ANY_NOT_NULL,
				execinfrapb.AggregatorSpec_XOR_AGG,
			},
			input: tuples{
				{nil, 3},
				{nil, 1},
				{0, -5},
				{0, -1},
				{0, 0},
			},
			expected: tuples{
				{nil, 2},
				{0, 4},
			},
			typs:      []*types.T{types.Int, types.Int},
			name:      "XorAgg",
			groupCols: []uint32{0},
			aggCols:   [][]uint32{{0}, {1}},
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
				runTestsWithTyps(t, []tuples{tc.input}, [][]*types.T{tc.typs}, tc.expected, unorderedVerifier,
					func(input []colexecbase.Operator) (colexecbase.Operator, error) {
						return agg.new(
							testAllocator, testMemAcc, input[0], tc.typs, tc.spec, &evalCtx,
							constructors, constArguments, outputTypes, false, /* isScalar */
						)
					})
			})
		}
	}
}

func BenchmarkDefaultAggregateFunction(b *testing.B) {
	const numInputBatches = 64
	aggFn := execinfrapb.AggregatorSpec_STRING_AGG
	for _, agg := range aggTypes {
		for _, groupSize := range []int{1, 2, 32, 128, coldata.BatchSize() / 2, coldata.BatchSize()} {
			for _, nullProb := range []float64{0.0, nullProbability} {
				benchmarkAggregateFunction(
					b, agg, aggFn, []*types.T{types.String, types.String}, groupSize,
					0 /* distinctProb */, nullProb, numInputBatches,
				)
			}
		}
	}
}
