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
	var closerRegistry colexecargs.CloserRegistry
	defer closerRegistry.Close(ctx)

	var diskSpillingState = struct {
		numRuns                    int
		numRunsWithExpectedClosers int
	}{}

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
			var semsToCheck []semaphore.Semaphore
			var numRuns int
			colexectestutils.RunTestsWithTyps(t, testAllocator, []colexectestutils.Tuples{tc.input}, [][]*types.T{tc.typs}, tc.expected, verifier, func(input []colexecop.Operator) (colexecop.Operator, error) {
				// ehaNumRequiredFDs is the minimum number of file descriptors
				// that are needed for the machinery of the external aggregator
				// (plus 1 is needed for the in-memory hash aggregator in order
				// to track tuples in a spilling queue).
				ehaNumRequiredFDs := 1 + colexecop.ExternalSorterMinPartitions
				sem := colexecop.NewTestingSemaphore(ehaNumRequiredFDs)
				semsToCheck = append(semsToCheck, sem)
				numRuns++
				op, err := createExternalHashAggregator(
					ctx, flowCtx, &colexecagg.NewAggregatorArgs{
						Allocator:      testAllocator,
						Input:          input[0],
						InputTypes:     tc.typs,
						Spec:           tc.spec,
						EvalCtx:        &evalCtx,
						Constructors:   constructors,
						ConstArguments: constArguments,
						OutputTypes:    outputTypes,
					},
					queueCfg, sem, numForcedRepartitions, &monitorRegistry, &closerRegistry,
				)
				if !cfg.diskSpillingEnabled {
					// Sanity check that indeed only the in-memory hash
					// aggregator was created.
					_, isHashAgg := MaybeUnwrapInvariantsChecker(op).(*hashAggregator)
					require.True(t, isHashAgg)
				}
				return op, err
			})
			if cfg.diskSpillingEnabled {
				diskSpillingState.numRuns++
				// We always have the root diskSpiller (1) added to the closers.
				// Then, when it spills to disk, _most commonly_ we will see the
				// following:
				// - the hash-based partitioner (2) for the external hash
				// aggregator
				// - the diskSpiller (3) and the external sort (4) that we use
				// in the fallback strategy of the external hash aggregator
				// - the diskSpiller (5) and the external sort (6) that we plan
				// on top of the hash-based partitioner to maintain the output
				// ordering.
				expectedNumClosersPerRun := 6
				if numForcedRepartitions == 0 {
					// In this case we won't create the external sort (4) in the
					// fallback strategy.
					expectedNumClosersPerRun--
				}
				if len(tc.spec.OutputOrdering.Columns) == 0 {
					// In this case we won't create the diskSpiller (5) and the
					// external sort (6).
					expectedNumClosersPerRun -= 2
				}

				if expectedNumClosersPerRun*numRuns == closerRegistry.NumClosers() {
					diskSpillingState.numRunsWithExpectedClosers++
				}
			}
			// Close all closers manually (in production this is done on the
			// flow cleanup).
			closerRegistry.Close(ctx)
			closerRegistry.Reset()
			for i, sem := range semsToCheck {
				require.Equal(t, 0, sem.GetCount(), "sem still reports open FDs at index %d", i)
			}
		}
	}
	// We have this sanity check to ensure that all expected closers in the
	// _most common_ scenario were added as closers. Coming up with an exact
	// formula for expected number of closers for each case proved quite
	// difficult.
	lowerBound := 0.5
	if numForcedRepartitions == 1 || numForcedRepartitions == 2 {
		// In these scenarios more internal operations might not spill to disk,
		// so lower the bound.
		lowerBound = 0.3
	}
	require.Less(t, lowerBound, float64(diskSpillingState.numRunsWithExpectedClosers)/float64(diskSpillingState.numRuns))
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
	var closerRegistry colexecargs.CloserRegistry
	afterEachRun := func() {
		closerRegistry.BenchmarkReset(ctx)
		monitorRegistry.BenchmarkReset(ctx)
	}

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
							op, err := createExternalHashAggregator(
								ctx, flowCtx, args, queueCfg, &colexecop.TestingSemaphore{},
								0 /* numForcedRepartitions */, &monitorRegistry, &closerRegistry,
							)
							require.NoError(b, err)
							// The hash-based partitioner is not a
							// ResettableOperator, so in order to not change the
							// signatures of the aggregator constructors, we
							// wrap it with a noop operator. It is ok for the
							// purposes of this benchmark.
							return colexecop.NewNoop(op)
						},
						afterEachRun: afterEachRun,
						name:         fmt.Sprintf("spilled=%t", spillForced),
						order:        unordered,
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
	closerRegistry *colexecargs.CloserRegistry,
) (colexecop.Operator, error) {
	spec := &execinfrapb.ProcessorSpec{
		Input: []execinfrapb.InputSyncSpec{{ColumnTypes: newAggArgs.InputTypes}},
		Core: execinfrapb.ProcessorCoreUnion{
			Aggregator: newAggArgs.Spec,
		},
		Post:        execinfrapb.PostProcessSpec{},
		ResultTypes: newAggArgs.OutputTypes,
	}
	args := &colexecargs.NewColOperatorArgs{
		Spec:            spec,
		Inputs:          []colexecargs.OpWithMetaInfo{{Root: newAggArgs.Input}},
		DiskQueueCfg:    diskQueueCfg,
		FDSemaphore:     testingSemaphore,
		MonitorRegistry: monitorRegistry,
		CloserRegistry:  closerRegistry,
	}
	args.TestingKnobs.NumForcedRepartitions = numForcedRepartitions
	result, err := colexecargs.TestNewColOperator(ctx, flowCtx, args)
	return result.Root, err
}
