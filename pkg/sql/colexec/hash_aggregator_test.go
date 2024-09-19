// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecagg"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
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
	{
		name: "oneOrderedCol",
		typs: []*types.T{types.Int, types.Int, types.Int},
		input: colexectestutils.Tuples{
			{0, 1, 1},
			{1, 0, 1},
			{2, 1, 1},
			{2, 1, 1},
			{2, 0, 1},
			{3, 1, 1},
			{3, 0, 1},
			{3, 0, 1},
			{3, 1, 1},
			{4, 0, 1},
			{4, 0, 1},
			{5, 1, 1},
			{5, 0, 1},
			{5, 1, 1},
		},
		groupCols: []uint32{0, 1},
		aggCols:   [][]uint32{{0}, {1}, {2}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.AnyNotNull,
			execinfrapb.SumInt,
		},
		expected: colexectestutils.Tuples{
			{0, 1, 1},
			{1, 0, 1},
			{2, 1, 2},
			{2, 0, 1},
			{3, 1, 2},
			{3, 0, 2},
			{4, 0, 2},
			{5, 1, 2},
			{5, 0, 1},
		},
		unorderedInput: false,
		orderedCols:    []uint32{0},
	},
	{
		name: "twoOrderedCol",
		typs: []*types.T{types.Int, types.Int, types.Int},
		input: colexectestutils.Tuples{
			{0, 1, 1},
			{1, 0, 1},
			{2, 0, 1},
			{2, 1, 1},
			{2, 1, 1},
			{3, 0, 1},
			{3, 0, 1},
			{3, 1, 1},
			{3, 1, 1},
			{4, 0, 1},
			{4, 0, 1},
			{5, 0, 1},
			{5, 1, 1},
			{5, 1, 1},
		},
		groupCols: []uint32{0, 1, 2},
		aggCols:   [][]uint32{{0}, {1}, {2}, {2}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.AnyNotNull,
			execinfrapb.AnyNotNull,
			execinfrapb.SumInt,
		},
		expected: colexectestutils.Tuples{
			{0, 1, 1, 1},
			{1, 0, 1, 1},
			{2, 0, 1, 1},
			{2, 1, 1, 2},
			{3, 0, 1, 2},
			{3, 1, 1, 2},
			{4, 0, 1, 2},
			{5, 0, 1, 1},
			{5, 1, 1, 2},
		},
		unorderedInput: false,
		orderedCols:    []uint32{0, 1},
	},
	{
		name: "PartialOrderWithNulls",
		typs: []*types.T{types.Int, types.Int, types.Int},
		input: colexectestutils.Tuples{
			{nil, 1, 1},
			{nil, 1, 1},
			{nil, 1, 1},
			{nil, 1, 1},
			{nil, 0, 1},
			{2, 1, 1},
			{2, 0, 1},
			{3, 1, 1},
			{3, 0, 1},
			{3, 0, 1},
		},
		groupCols: []uint32{0, 1},
		aggCols:   [][]uint32{{0}, {1}, {2}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.AnyNotNull,
			execinfrapb.SumInt,
		},
		expected: colexectestutils.Tuples{
			{nil, 1, 4},
			{nil, 0, 1},
			{2, 1, 1},
			{2, 0, 1},
			{3, 1, 1},
			{3, 0, 2},
		},
		unorderedInput: false,
		orderedCols:    []uint32{0},
	},
	{
		name: "orderedColSpansMultMaxBufferBoundary",
		typs: []*types.T{types.Int, types.Int, types.Int},
		input: colexectestutils.Tuples{
			{0, 1, 1},
			{0, 1, 1},
			{0, 3, 1},
			{0, 2, 1},
			{0, 3, 1},
			{0, 3, 1},
			{0, 3, 1},
			{0, 2, 1},
			{0, 1, 1},
			{0, 3, 1},
			{0, 1, 1},
			{0, 1, 1},
			{0, 3, 1},
			{0, 1, 1},
			{0, 3, 1},
			{0, 1, 1},
			{0, 1, 1},
			{0, 2, 1},
			{0, 2, 1},
			{0, 2, 1},
			{0, 3, 1},
			{0, 1, 1},
			{0, 1, 1},
			{0, 1, 1},
			{0, 2, 1},
			{1, 1, 1},
		},
		groupCols: []uint32{0, 1},
		aggCols:   [][]uint32{{0}, {1}, {2}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.AnyNotNull,
			execinfrapb.SumInt,
		},
		expected: colexectestutils.Tuples{
			{0, 1, 11},
			{0, 3, 8},
			{0, 2, 6},
			{1, 1, 1},
		},
		unorderedInput: false,
		orderedCols:    []uint32{0},
	},
	{
		name: "orderedColUnique",
		typs: []*types.T{types.Int, types.Int, types.Int},
		input: colexectestutils.Tuples{
			{0, 1, 2},
			{1, 1, 3},
			{2, 3, 1},
			{3, 2, 1},
			{4, 3, 0},
			{5, 3, 5},
			{6, 3, 1},
			{7, 3, 1},
			{8, 1, 1},
			{9, 4, 8},
			{10, 3, 2},
			{11, 3, 1},
			{12, 2, 3},
			{13, 1, 3},
			{14, 1, 2},
		},
		groupCols: []uint32{0, 1},
		aggCols:   [][]uint32{{0}, {1}, {2}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.AnyNotNull,
			execinfrapb.SumInt,
		},
		expected: colexectestutils.Tuples{
			{0, 1, 2},
			{1, 1, 3},
			{2, 3, 1},
			{3, 2, 1},
			{4, 3, 0},
			{5, 3, 5},
			{6, 3, 1},
			{7, 3, 1},
			{8, 1, 1},
			{9, 4, 8},
			{10, 3, 2},
			{11, 3, 1},
			{12, 2, 3},
			{13, 1, 3},
			{14, 1, 2},
		},
		unorderedInput: false,
		orderedCols:    []uint32{0},
	},
	{
		name: "spilloverAfterEmit",
		typs: []*types.T{types.Int, types.Int, types.Int},
		input: colexectestutils.Tuples{
			{1, 0, 1},
			{1, 0, 1},
			{1, 0, 1},
			{1, 0, 1},
			{1, 0, 1},
			{3, 0, 1},
			{3, 1, 1},
			{3, 2, 1},
			{3, 3, 1},
			{3, 4, 1},
			{3, 5, 1},
			{3, 6, 1},
			{3, 7, 1},
			{3, 8, 1},
			{3, 9, 1},
			{3, 10, 1},
			{3, 11, 1},
			{3, 12, 1},
			{3, 13, 1},
			{3, 14, 1},
			{3, 15, 1},
			{3, 16, 1},
			{3, 17, 1},
			{3, 18, 1},
			{3, 19, 1},
			{3, 20, 1},
			{3, 21, 1},
			{3, 22, 1},
			{3, 23, 1},
			{3, 24, 1},
			{3, 25, 1},
			{3, 26, 1},
			{3, 27, 1},
			{3, 28, 1},
			{3, 29, 1},
			{4, 0, 1},
		},
		groupCols: []uint32{0, 1},
		aggCols:   [][]uint32{{0}, {1}, {2}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.AnyNotNull,
			execinfrapb.SumInt,
		},
		expected: colexectestutils.Tuples{
			{1, 0, 5},
			{3, 0, 1},
			{3, 1, 1},
			{3, 2, 1},
			{3, 3, 1},
			{3, 4, 1},
			{3, 5, 1},
			{3, 6, 1},
			{3, 7, 1},
			{3, 8, 1},
			{3, 9, 1},
			{3, 10, 1},
			{3, 11, 1},
			{3, 12, 1},
			{3, 13, 1},
			{3, 14, 1},
			{3, 15, 1},
			{3, 16, 1},
			{3, 17, 1},
			{3, 18, 1},
			{3, 19, 1},
			{3, 20, 1},
			{3, 21, 1},
			{3, 22, 1},
			{3, 23, 1},
			{3, 24, 1},
			{3, 25, 1},
			{3, 26, 1},
			{3, 27, 1},
			{3, 28, 1},
			{3, 29, 1},
			{4, 0, 1},
		},
		unorderedInput: false,
		orderedCols:    []uint32{0},
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

	evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	rng, _ := randutil.NewTestRand()
	for _, tc := range hashAggregatorTestCases {
		log.Infof(context.Background(), "%s", tc.name)
		constructors, constArguments, outputTypes, err := colexecagg.ProcessAggregations(
			context.Background(), &evalCtx, nil /* semaCtx */, tc.spec.Aggregations, tc.typs,
		)
		require.NoError(t, err)
		var typs [][]*types.T
		typs = append(typs, tc.typs)
		verifier := colexectestutils.OrderedVerifier
		if tc.unorderedInput {
			verifier = colexectestutils.UnorderedVerifier
		} else if len(tc.orderedCols) > 0 {
			verifier = colexectestutils.PartialOrderedVerifier
		}
		colexectestutils.RunTestsWithOrderedCols(t, testAllocator, []colexectestutils.Tuples{tc.input}, typs, tc.expected, verifier, tc.orderedCols,
			func(sources []colexecop.Operator) (colexecop.Operator, error) {
				args := &colexecagg.NewAggregatorArgs{
					Allocator:      testAllocator,
					Input:          sources[0],
					InputTypes:     tc.typs,
					Spec:           tc.spec,
					EvalCtx:        &evalCtx,
					Constructors:   constructors,
					ConstArguments: constArguments,
					OutputTypes:    outputTypes,
				}
				args.TestingKnobs.HashTableNumBuckets = uint32(1 + rng.Intn(7))
				return NewHashAggregator(
					context.Background(),
					&colexecagg.NewHashAggregatorArgs{
						NewAggregatorArgs:        args,
						HashTableAllocator:       testAllocator,
						OutputUnlimitedAllocator: testAllocator,
						MaxOutputBatchMemSize:    math.MaxInt64,
					},
					nil, /* newSpillingQueueArgs */
				), nil
			})
	}
}

func BenchmarkHashAggregatorInputTuplesTracking(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(b, false /* inMem */)
	defer cleanup()

	numRows := []int{1, 32, coldata.BatchSize(), 32 * coldata.BatchSize(), 1024 * coldata.BatchSize()}
	groupSizes := []int{1, 2, 32, 128, coldata.BatchSize()}
	if testing.Short() {
		numRows = []int{32, 32 * coldata.BatchSize()}
		groupSizes = []int{1, coldata.BatchSize()}
	}
	var memAccounts []*mon.BoundAccount
	// We choose any_not_null aggregate function because it is the simplest
	// possible and, thus, its Compute function call will have the least
	// impact when benchmarking the aggregator logic.
	aggFn := execinfrapb.AnyNotNull
	for _, numInputRows := range numRows {
		for _, groupSize := range groupSizes {
			for _, agg := range []aggType{
				{
					new: func(ctx context.Context, args *colexecagg.NewAggregatorArgs) colexecop.ResettableOperator {
						return NewHashAggregator(
							ctx,
							&colexecagg.NewHashAggregatorArgs{
								NewAggregatorArgs:        args,
								HashTableAllocator:       testAllocator,
								OutputUnlimitedAllocator: testAllocator,
								MaxOutputBatchMemSize:    math.MaxInt64,
							},
							nil, /* newSpillingQueueArgs */
						)
					},
					name:  "tracking=false",
					order: unordered,
				},
				{
					new: func(ctx context.Context, args *colexecagg.NewAggregatorArgs) colexecop.ResettableOperator {
						spillingQueueMemAcc := testMemMonitor.MakeBoundAccount()
						memAccounts = append(memAccounts, &spillingQueueMemAcc)
						return NewHashAggregator(
							ctx,
							&colexecagg.NewHashAggregatorArgs{
								NewAggregatorArgs:        args,
								HashTableAllocator:       testAllocator,
								OutputUnlimitedAllocator: testAllocator,
								MaxOutputBatchMemSize:    math.MaxInt64,
							},
							&colexecutils.NewSpillingQueueArgs{
								UnlimitedAllocator: colmem.NewAllocator(ctx, &spillingQueueMemAcc, testColumnFactory),
								Types:              args.InputTypes,
								MemoryLimit:        execinfra.DefaultMemoryLimit,
								DiskQueueCfg:       queueCfg,
								FDSemaphore:        &colexecop.TestingSemaphore{},
								DiskAcc:            testDiskAcc,
								DiskQueueMemAcc:    testMemAcc,
							},
						)
					},
					name:  "tracking=true",
					order: unordered,
				},
			} {
				benchmarkAggregateFunction(
					b, agg, aggFn, []*types.T{types.Int}, 1 /* numGroupCol */, groupSize,
					0 /* distinctProb */, numInputRows, 0, /* chunkSize */
					0 /* limit */, 0, /* numSameAggs */
				)
			}
		}
	}

	for _, account := range memAccounts {
		account.Close(ctx)
	}
}

// BenchmarkHashAggregatorPartialOrder compares the performance of the in-memory
// hash aggregator with one of two grouping columns ordered against both
// grouping columns unordered.
func BenchmarkHashAggregatorPartialOrder(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(b, false /* inMem */)
	defer cleanup()

	numRows := []int{1, 32, coldata.BatchSize(), 32 * coldata.BatchSize(), 1024 * coldata.BatchSize()}
	// chunkSizes is the number of distinct values in the ordered group column.
	chunkSizes := []int{1, 32, coldata.BatchSize() + 1}
	// groupSizes is the total number of groups.
	groupSizes := []int{1, 32, 128, coldata.BatchSize() + 1}
	limits := []int{1, 32 * coldata.BatchSize()}
	if testing.Short() {
		numRows = []int{32, 32 * coldata.BatchSize()}
		chunkSizes = []int{1, coldata.BatchSize() + 1}
		groupSizes = []int{1, coldata.BatchSize()}
	}
	var memAccounts []*mon.BoundAccount
	f := func(ctx context.Context, args *colexecagg.NewAggregatorArgs) colexecop.ResettableOperator {
		spillingQueueMemAcc := testMemMonitor.MakeBoundAccount()
		memAccounts = append(memAccounts, &spillingQueueMemAcc)
		return NewHashAggregator(
			ctx,
			&colexecagg.NewHashAggregatorArgs{
				NewAggregatorArgs:        args,
				HashTableAllocator:       testAllocator,
				OutputUnlimitedAllocator: testAllocator,
				MaxOutputBatchMemSize:    math.MaxInt64,
			},
			&colexecutils.NewSpillingQueueArgs{
				UnlimitedAllocator: colmem.NewAllocator(ctx, &spillingQueueMemAcc, testColumnFactory),
				Types:              args.InputTypes,
				MemoryLimit:        execinfra.DefaultMemoryLimit,
				DiskQueueCfg:       queueCfg,
				FDSemaphore:        &colexecop.TestingSemaphore{},
				DiskAcc:            testDiskAcc,
				DiskQueueMemAcc:    testMemAcc,
			},
		)
	}
	// We choose any_not_null aggregate function because it is the simplest
	// possible and, thus, its Compute function call will have the least impact
	// when benchmarking the aggregator logic.
	aggFn := execinfrapb.AnyNotNull
	for _, numInputRows := range numRows {
		for _, limit := range limits {
			if limit > numInputRows {
				continue
			}
			for _, groupSize := range groupSizes {
				for _, chunkSize := range chunkSizes {
					if groupSize > chunkSize || chunkSize > numInputRows {
						continue
					}
					for _, agg := range []aggType{
						{
							new:   f,
							name:  fmt.Sprintf("hash-unordered/limit=%d/chunkSize=%d", limit, chunkSize),
							order: unordered,
						},
						{
							new:   f,
							name:  fmt.Sprintf("hash-partial-order/limit=%d/chunkSize=%d", limit, chunkSize),
							order: partial,
						},
					} {
						if agg.order == unordered && chunkSize != chunkSizes[0] {
							// Chunk size isn't a factor for the unordered hash aggregator,
							// so we can skip all but one case.
							continue
						}
						benchmarkAggregateFunction(
							b, agg, aggFn, []*types.T{types.Int}, 2 /* numGroupCol */, groupSize,
							0 /* distinctProb */, numInputRows, chunkSize, limit, 0, /* numSameAggs */
						)
					}
				}
			}
		}
	}
	for _, account := range memAccounts {
		account.Close(ctx)
	}
}
