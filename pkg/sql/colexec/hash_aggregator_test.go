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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecagg"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/stretchr/testify/require"
)

var hashAggregatorTestCases = []aggregatorTestCase{
	{
		name: "carryBetweenBatches",
		typs: types.TwoIntCols,
		input: colexectestutils.Tuples{
			{0, 1},
			{1, 5},
			{0, 4},
			{0, 2},
			{2, 6},
			{0, 3},
			{0, 7},
		},
		groupCols: []uint32{0},
		aggCols:   [][]uint32{{0}, {1}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.SumInt,
		},
		expected: colexectestutils.Tuples{
			{1, 5},
			{2, 6},
			{0, 17},
		},
		unorderedInput: true,
	},
	{
		name: "bucketCollision",
		typs: types.TwoIntCols,
		input: colexectestutils.Tuples{
			{0, 3},
			{0, 4},
			{coldata.BatchSize(), 6},
			{0, 5},
			{coldata.BatchSize(), 7},
		},
		groupCols: []uint32{0},
		aggCols:   [][]uint32{{0}, {1}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.SumInt,
		},
		expected: colexectestutils.Tuples{
			{0, 12},
			{coldata.BatchSize(), 13},
		},
	},
	{
		name: "decimalSums",
		typs: []*types.T{types.Int, types.Int, types.Decimal},
		input: colexectestutils.Tuples{
			{0, 1, 1.3},
			{0, 1, 1.6},
			{0, 1, 0.5},
			{1, 1, 1.2},
		},
		groupCols: []uint32{0, 1},
		aggCols:   [][]uint32{{0}, {1}, {2}, {1}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.AnyNotNull,
			execinfrapb.Sum,
			execinfrapb.SumInt,
		},
		expected: colexectestutils.Tuples{
			{0, 1, 3.4, 3},
			{1, 1, 1.2, 1},
		},
		convToDecimal: true,
	},
}

func init() {
	for i := range hashAggregatorTestCases {
		if err := hashAggregatorTestCases[i].init(); err != nil {
			colexecerror.InternalError(err)
		}
	}
}

func TestHashAggregator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	for _, tc := range hashAggregatorTestCases {
		constructors, constArguments, outputTypes, err := colexecagg.ProcessAggregations(
			&evalCtx, nil /* semaCtx */, tc.spec.Aggregations, tc.typs,
		)
		require.NoError(t, err)
		verifier := colexectestutils.OrderedVerifier
		if tc.unorderedInput {
			verifier = colexectestutils.UnorderedVerifier
		}
		colexectestutils.RunTests(t, testAllocator, []colexectestutils.Tuples{tc.input}, tc.expected, verifier, func(sources []colexecop.Operator) (colexecop.Operator, error) {
			return NewHashAggregator(&colexecagg.NewAggregatorArgs{
				Allocator:      testAllocator,
				MemAccount:     testMemAcc,
				Input:          sources[0],
				InputTypes:     tc.typs,
				Spec:           tc.spec,
				EvalCtx:        &evalCtx,
				Constructors:   constructors,
				ConstArguments: constArguments,
				OutputTypes:    outputTypes,
			},
				nil, /* newSpillingQueueArgs */
			)
		})
	}
}

func BenchmarkHashAggregatorInputTuplesTracking(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(b, false /* inMem */)
	defer cleanup()
	queueCfg.CacheMode = colcontainer.DiskQueueCacheModeReuseCache
	queueCfg.SetDefaultBufferSizeBytesForCacheMode()

	aggFn := execinfrapb.Min
	numRows := []int{1, 32, coldata.BatchSize(), 32 * coldata.BatchSize(), 1024 * coldata.BatchSize()}
	groupSizes := []int{1, 2, 32, 128, coldata.BatchSize()}
	if testing.Short() {
		numRows = []int{32, 32 * coldata.BatchSize()}
		groupSizes = []int{1, coldata.BatchSize()}
	}
	var memAccounts []*mon.BoundAccount
	for _, numInputRows := range numRows {
		for _, groupSize := range groupSizes {
			for _, agg := range []aggType{
				{
					new: func(args *colexecagg.NewAggregatorArgs) (colexecop.ResettableOperator, error) {
						return NewHashAggregator(args, nil /* newSpillingQueueArgs */)
					},
					name: "tracking=false",
				},
				{
					new: func(args *colexecagg.NewAggregatorArgs) (colexecop.ResettableOperator, error) {
						spillingQueueMemAcc := testMemMonitor.MakeBoundAccount()
						memAccounts = append(memAccounts, &spillingQueueMemAcc)
						return NewHashAggregator(args, &colexecutils.NewSpillingQueueArgs{
							UnlimitedAllocator: colmem.NewAllocator(ctx, &spillingQueueMemAcc, testColumnFactory),
							Types:              args.InputTypes,
							MemoryLimit:        execinfra.DefaultMemoryLimit,
							DiskQueueCfg:       queueCfg,
							FDSemaphore:        &colexecop.TestingSemaphore{},
							DiskAcc:            testDiskAcc,
						})
					},
					name: "tracking=true",
				},
			} {
				benchmarkAggregateFunction(
					b, agg, aggFn, []*types.T{types.Int}, groupSize,
					0 /* distinctProb */, numInputRows,
				)
			}
		}
	}

	for _, account := range memAccounts {
		account.Close(ctx)
	}
}
