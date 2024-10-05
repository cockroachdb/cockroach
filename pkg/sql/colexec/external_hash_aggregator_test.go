// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecagg"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/marusama/semaphore"
	"github.com/stretchr/testify/require"
)

func TestExternalHashAggregator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Mon:     evalCtx.TestingMon,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
		DiskMonitor: testDiskMonitor,
	}

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()
	var monitorRegistry colexecargs.MonitorRegistry
	defer monitorRegistry.Close(ctx)

	rng, _ := randutil.NewTestRand()
	numForcedRepartitions := rng.Intn(5)
	for _, cfg := range []struct {
		diskSpillingEnabled bool
		spillForced         bool
		memoryLimitBytes    int64
	}{
		{
			diskSpillingEnabled: true,
			spillForced:         true,
		},
		{
			diskSpillingEnabled: true,
			spillForced:         false,
		},
		{
			diskSpillingEnabled: false,
		},
		{
			diskSpillingEnabled: true,
			spillForced:         false,
			memoryLimitBytes:    hashAggregatorAllocSize * sizeOfAggBucket,
		},
		{
			diskSpillingEnabled: true,
			spillForced:         false,
			memoryLimitBytes:    hashAggregatorAllocSize * sizeOfAggBucket * 2,
		},
	} {
		HashAggregationDiskSpillingEnabled.Override(ctx, &flowCtx.Cfg.Settings.SV, cfg.diskSpillingEnabled)
		flowCtx.Cfg.TestingKnobs.ForceDiskSpill = cfg.spillForced
		flowCtx.Cfg.TestingKnobs.MemoryLimitBytes = cfg.memoryLimitBytes
		for _, tc := range append(aggregatorsTestCases, hashAggregatorTestCases...) {
			if len(tc.groupCols) == 0 {
				// If there are no grouping columns, then the ordered
				// aggregator is planned.
				continue
			}
			if tc.aggFilter != nil {
				// Filtering aggregation is not supported with the ordered
				// aggregation which is required for the external hash
				// aggregator in the fallback strategy.
				continue
			}
			log.Infof(ctx, "diskSpillingEnabled=%t/spillForced=%t/memoryLimitBytes=%d/numRepartitions=%d/%s", cfg.diskSpillingEnabled, cfg.spillForced, cfg.memoryLimitBytes, numForcedRepartitions, tc.name)
			constructors, constArguments, outputTypes, err := colexecagg.ProcessAggregations(
				ctx, &evalCtx, nil /* semaCtx */, tc.spec.Aggregations, tc.typs,
			)
			require.NoError(t, err)
			verifier := colexectestutils.OrderedVerifier
			if tc.unorderedInput {
				verifier = colexectestutils.UnorderedVerifier
			} else if len(tc.orderedCols) > 0 {
				verifier = colexectestutils.PartialOrderedVerifier
			}
			var numExpectedClosers int
			if cfg.diskSpillingEnabled {
				// The external sorter (accounting for two closers), the disk
				// spiller, and the external hash aggregator should be added as
				// Closers.
				numExpectedClosers = 4
				if len(tc.spec.OutputOrdering.Columns) > 0 {
					// When the output ordering is required, we also plan
					// another external sort which accounts for two closers.
					numExpectedClosers += 2
				}
			} else {
				// Only the in-memory hash aggregator should be added.
				numExpectedClosers = 1
			}
			var semsToCheck []semaphore.Semaphore
			colexectestutils.RunTestsWithTyps(t, testAllocator, []colexectestutils.Tuples{tc.input}, [][]*types.T{tc.typs}, tc.expected, verifier, func(input []colexecop.Operator) (colexecop.Operator, error) {
				// ehaNumRequiredFDs is the minimum number of file descriptors
				// that are needed for the machinery of the external aggregator
				// (plus 1 is needed for the in-memory hash aggregator in order
				// to track tuples in a spilling queue).
				ehaNumRequiredFDs := 1 + colexecop.ExternalSorterMinPartitions
				sem := colexecop.NewTestingSemaphore(ehaNumRequiredFDs)
				semsToCheck = append(semsToCheck, sem)
				op, closers, err := createExternalHashAggregator(
					ctx, flowCtx, &colexecagg.NewAggregatorArgs{
						Allocator:      testAllocator,
						MemAccount:     testMemAcc,
						Input:          input[0],
						InputTypes:     tc.typs,
						Spec:           tc.spec,
						EvalCtx:        &evalCtx,
						Constructors:   constructors,
						ConstArguments: constArguments,
						OutputTypes:    outputTypes,
					},
					queueCfg, sem, numForcedRepartitions, &monitorRegistry,
				)
				require.Equal(t, numExpectedClosers, len(closers))
				if !cfg.diskSpillingEnabled {
					// Sanity check that indeed only the in-memory hash
					// aggregator was created.
					_, isHashAgg := MaybeUnwrapInvariantsChecker(op).(*hashAggregator)
					require.True(t, isHashAgg)
				}
				return op, err
			})
			for i, sem := range semsToCheck {
				require.Equal(t, 0, sem.GetCount(), "sem still reports open FDs at index %d", i)
			}
		}
	}
}

func BenchmarkExternalHashAggregator(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Mon:     evalCtx.TestingMon,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
		DiskMonitor: testDiskMonitor,
	}

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(b, false /* inMem */)
	defer cleanup()
	var monitorRegistry colexecargs.MonitorRegistry
	defer monitorRegistry.Close(ctx)

	numRows := []int{coldata.BatchSize(), 64 * coldata.BatchSize(), 4096 * coldata.BatchSize()}
	groupSizes := []int{1, 2, 32, 128, coldata.BatchSize()}
	if testing.Short() {
		numRows = []int{64 * coldata.BatchSize()}
		groupSizes = []int{1, coldata.BatchSize()}
	}
	// We choose any_not_null aggregate function because it is the simplest
	// possible and, thus, its Compute function call will have the least impact
	// when benchmarking the aggregator logic.
	aggFn := execinfrapb.AnyNotNull
	for _, spillForced := range []bool{false, true} {
		flowCtx.Cfg.TestingKnobs.ForceDiskSpill = spillForced
		for _, numInputRows := range numRows {
			for _, groupSize := range groupSizes {
				benchmarkAggregateFunction(
					b, aggType{
						new: func(ctx context.Context, args *colexecagg.NewAggregatorArgs) colexecop.ResettableOperator {
							op, _, err := createExternalHashAggregator(
								ctx, flowCtx, args, queueCfg, &colexecop.TestingSemaphore{},
								0 /* numForcedRepartitions */, &monitorRegistry,
							)
							require.NoError(b, err)
							// The hash-based partitioner is not a
							// ResettableOperator, so in order to not change the
							// signatures of the aggregator constructors, we
							// wrap it with a noop operator. It is ok for the
							// purposes of this benchmark.
							return colexecop.NewNoop(op)
						},
						name:  fmt.Sprintf("spilled=%t", spillForced),
						order: unordered,
					},
					aggFn, []*types.T{types.Int}, 1 /* numGroupCol */, groupSize,
					0 /* distinctProb */, numInputRows, 0, /* chunkSize */
					0 /* limit */, 0, /* numSameAggs */
				)
			}
		}
	}
}

// createExternalHashAggregator is a helper function that instantiates a
// disk-backed hash aggregator.
func createExternalHashAggregator(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	newAggArgs *colexecagg.NewAggregatorArgs,
	diskQueueCfg colcontainer.DiskQueueCfg,
	testingSemaphore semaphore.Semaphore,
	numForcedRepartitions int,
	monitorRegistry *colexecargs.MonitorRegistry,
) (colexecop.Operator, []colexecop.Closer, error) {
	spec := &execinfrapb.ProcessorSpec{
		Input: []execinfrapb.InputSyncSpec{{ColumnTypes: newAggArgs.InputTypes}},
		Core: execinfrapb.ProcessorCoreUnion{
			Aggregator: newAggArgs.Spec,
		},
		Post:        execinfrapb.PostProcessSpec{},
		ResultTypes: newAggArgs.OutputTypes,
	}
	args := &colexecargs.NewColOperatorArgs{
		Spec:                spec,
		Inputs:              []colexecargs.OpWithMetaInfo{{Root: newAggArgs.Input}},
		StreamingMemAccount: testMemAcc,
		DiskQueueCfg:        diskQueueCfg,
		FDSemaphore:         testingSemaphore,
		MonitorRegistry:     monitorRegistry,
	}
	args.TestingKnobs.NumForcedRepartitions = numForcedRepartitions
	result, err := colexecargs.TestNewColOperator(ctx, flowCtx, args)
	return result.Root, result.ToClose, err
}
