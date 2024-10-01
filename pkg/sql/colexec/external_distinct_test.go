// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
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
	"github.com/marusama/semaphore"
	"github.com/stretchr/testify/require"
)

func TestExternalDistinct(t *testing.T) {
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
	// Test the case in which the default memory is used as well as the case in
	// which the distinct spills to disk.
	for _, spillForced := range []bool{false, true} {
		flowCtx.Cfg.TestingKnobs.ForceDiskSpill = spillForced
		for tcIdx, tc := range distinctTestCases {
			log.Infof(context.Background(), "spillForced=%t/%d", spillForced, tcIdx)
			var semsToCheck []semaphore.Semaphore
			var outputOrdering execinfrapb.Ordering
			verifier := colexectestutils.UnorderedVerifier
			// Check that the disk spiller, the external distinct, and the
			// disk-backed sort (which includes both the disk spiller and the
			// sort) were added as Closers.
			numExpectedClosers := 4
			if tc.isOrderedOnDistinctCols {
				outputOrdering = convertDistinctColsToOrdering(tc.distinctCols)
				verifier = colexectestutils.OrderedVerifier
				// The disk spiller and the sort included in the final
				// disk-backed sort must also be added as Closers.
				numExpectedClosers += 2
			}
			tc.runTests(t, verifier, func(input []colexecop.Operator) (colexecop.Operator, error) {
				// A sorter should never exceed ExternalSorterMinPartitions, even
				// during repartitioning. A panic will happen if a sorter requests
				// more than this number of file descriptors.
				sem := colexecop.NewTestingSemaphore(colexecop.ExternalSorterMinPartitions)
				semsToCheck = append(semsToCheck, sem)
				distinct, closers, err := createExternalDistinct(
					ctx, flowCtx, input, tc.typs, tc.distinctCols, tc.nullsAreDistinct, tc.errorOnDup,
					outputOrdering, queueCfg, sem, nil /* spillingCallbackFn */, numForcedRepartitions,
					&monitorRegistry,
				)
				require.Equal(t, numExpectedClosers, len(closers))
				return distinct, err
			})
			for i, sem := range semsToCheck {
				require.Equal(t, 0, sem.GetCount(), "sem still reports open FDs at index %d", i)
			}
		}
	}
}

// TestExternalDistinctSpilling verifies that the external distinct correctly
// handles the scenario when spilling to disk occurs after some tuples have
// been emitted in the output by the in-memory unordered distinct.
func TestExternalDistinctSpilling(t *testing.T) {
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
	nCols := 1 + rng.Intn(3)
	typs := make([]*types.T, nCols)
	distinctCols := make([]uint32, nCols)
	for i := range typs {
		typs[i] = types.Int
		distinctCols[i] = uint32(i)
	}

	batchMemEstimate := colmem.EstimateBatchSizeBytes(typs, coldata.BatchSize())
	// Set the memory limit in such a manner that at least 2 batches of distinct
	// tuples are emitted by the in-memory unordered distinct before the
	// spilling occurs.
	nBatchesOutputByInMemoryOp := 2 + rng.Int63n(2)
	memoryLimitBytes := nBatchesOutputByInMemoryOp * batchMemEstimate
	if memoryLimitBytes < mon.DefaultPoolAllocationSize {
		memoryLimitBytes = mon.DefaultPoolAllocationSize
		nBatchesOutputByInMemoryOp = memoryLimitBytes / batchMemEstimate
	}
	flowCtx.Cfg.TestingKnobs.MemoryLimitBytes = memoryLimitBytes

	// Calculate the total number of distinct batches at least twice as large
	// as for the in-memory operator in order to make sure that the external
	// distinct has enough work to do.
	nDistinctBatches := int(nBatchesOutputByInMemoryOp * (2 + rng.Int63n(2)))
	newTupleProbability := rng.Float64()
	nTuples := int(float64(nDistinctBatches*coldata.BatchSize()) / newTupleProbability)
	const maxNumTuples = 25000
	spillingMightNotHappen := false
	if nTuples > maxNumTuples {
		// If we happen to set a large value for coldata.BatchSize() and a small
		// value for newTupleProbability, we might end up with huge number of
		// tuples. Then, when RunTests test harness uses small batch size, the
		// test might take a while, so we'll limit the number of tuples.
		nTuples = maxNumTuples
		// Since we have limited the number of tuples, it is possible that the
		// spilling will not occur because we have given too large of a memory
		// limit to the in-memory distinct. In such (relatively rare) scenario
		// we cannot check that we spilled every time, yet we might as well run
		// the correctness check.
		spillingMightNotHappen = true
	}
	tups, expected := generateRandomDataForUnorderedDistinct(rng, nTuples, nCols, newTupleProbability)

	var numRuns, numSpills int
	var semsToCheck []semaphore.Semaphore
	numForcedRepartitions := rng.Intn(5)
	colexectestutils.RunTestsWithoutAllNullsInjection(
		t,
		testAllocator,
		[]colexectestutils.Tuples{tups},
		[][]*types.T{typs},
		expected,
		// tups and expected are in an arbitrary order, so we use an unordered
		// verifier.
		colexectestutils.UnorderedVerifier,
		func(input []colexecop.Operator) (colexecop.Operator, error) {
			// Since we're giving very low memory limit to the operator, in
			// order to make the test run faster, we'll use an unlimited number
			// of file descriptors.
			sem := colexecop.NewTestingSemaphore(0 /* limit */)
			semsToCheck = append(semsToCheck, sem)
			var outputOrdering execinfrapb.Ordering
			distinct, closers, err := createExternalDistinct(
				ctx, flowCtx, input, typs, distinctCols, false /* nullsAreDistinct */, "", /* errorOnDup */
				outputOrdering, queueCfg, sem, func() { numSpills++ }, numForcedRepartitions,
				&monitorRegistry,
			)
			require.NoError(t, err)
			// Check that the disk spiller, the external distinct, and the
			// disk-backed sort (which accounts for two) were added as Closers.
			numExpectedClosers := 4
			require.Equal(t, numExpectedClosers, len(closers))
			numRuns++
			return distinct, nil
		},
	)
	for i, sem := range semsToCheck {
		require.Equal(t, 0, sem.GetCount(), "sem still reports open FDs at index %d", i)
	}
	if !spillingMightNotHappen {
		// The "randomNullsInjection" subtest might not spill to disk when a
		// large portion of rows is made NULL, so we allow two cases:
		// - numSpills == numRuns
		// - numSpills == numRuns - 1.
		require.GreaterOrEqual(t, numSpills, numRuns-1, "the spilling didn't occur in all cases")
	}
}

// generateRandomDataForDistinct is a utility function that generates data to be
// used in randomized unit test of an unordered distinct operation. Note that
// tups and expected can be in an arbitrary order (meaning the former is
// shuffled whereas the latter is not).
func generateRandomDataForUnorderedDistinct(
	rng *rand.Rand, nTups, nDistinctCols int, newTupleProbability float64,
) (tups, expected colexectestutils.Tuples) {
	tups = make(colexectestutils.Tuples, nTups)
	expected = make(colexectestutils.Tuples, 1, nTups)
	tups[0] = make(colexectestutils.Tuple, nDistinctCols)
	for j := 0; j < nDistinctCols; j++ {
		tups[0][j] = 0
	}
	expected[0] = tups[0]

	// We will construct the data in an ordered manner, and we'll shuffle it so
	// that duplicate tuples are distributed randomly and not consequently.
	newValueProbability := getNewValueProbabilityForDistinct(newTupleProbability, nDistinctCols)
	for i := 1; i < nTups; i++ {
		tups[i] = make(colexectestutils.Tuple, nDistinctCols)
		isDuplicate := true
		for j := range tups[i] {
			tups[i][j] = tups[i-1][j].(int)
			if rng.Float64() < newValueProbability {
				tups[i][j] = tups[i][j].(int) + 1
				isDuplicate = false
			}
		}
		if !isDuplicate {
			expected = append(expected, tups[i])
		}
	}

	rand.Shuffle(nTups, func(i, j int) { tups[i], tups[j] = tups[j], tups[i] })
	return tups, expected
}

func convertDistinctColsToOrdering(distinctCols []uint32) execinfrapb.Ordering {
	var ordering execinfrapb.Ordering
	for _, colIdx := range distinctCols {
		ordering.Columns = append(ordering.Columns, execinfrapb.Ordering_Column{ColIdx: colIdx})
	}
	return ordering
}

func BenchmarkExternalDistinct(b *testing.B) {
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

	for _, spillForced := range []bool{false, true} {
		for _, maintainOrdering := range []bool{false, true} {
			if !spillForced && maintainOrdering {
				// The in-memory unordered distinct maintains the input ordering
				// by design, so it's not an interesting case to test it with
				// both options for 'maintainOrdering' parameter, and we skip
				// one.
				continue
			}
			flowCtx.Cfg.TestingKnobs.ForceDiskSpill = spillForced
			name := fmt.Sprintf("spilled=%t/ordering=%t/shuffled", spillForced, maintainOrdering)
			runDistinctBenchmarks(
				ctx,
				b,
				func(allocator *colmem.Allocator, input colexecop.Operator, distinctCols []uint32, numOrderedCols int, typs []*types.T) (colexecop.Operator, error) {
					var outputOrdering execinfrapb.Ordering
					if maintainOrdering {
						outputOrdering = convertDistinctColsToOrdering(distinctCols)
					}
					op, _, err := createExternalDistinct(
						ctx, flowCtx, []colexecop.Operator{input}, typs,
						distinctCols, false /* nullsAreDistinct */, "", /* errorOnDup */
						outputOrdering, queueCfg, &colexecop.TestingSemaphore{},
						nil /* spillingCallbackFn */, 0, /* numForcedRepartitions */
						&monitorRegistry,
					)
					return op, err
				},
				func(nCols int) int {
					return 0
				},
				name,
				true, /* isExternal */
				true, /* shuffleInput */
			)
		}
	}
}

// createExternalDistinct is a helper function that instantiates a disk-backed
// distinct operator.
func createExternalDistinct(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	sources []colexecop.Operator,
	typs []*types.T,
	distinctCols []uint32,
	nullsAreDistinct bool,
	errorOnDup string,
	outputOrdering execinfrapb.Ordering,
	diskQueueCfg colcontainer.DiskQueueCfg,
	testingSemaphore semaphore.Semaphore,
	spillingCallbackFn func(),
	numForcedRepartitions int,
	monitorRegistry *colexecargs.MonitorRegistry,
) (colexecop.Operator, []colexecop.Closer, error) {
	distinctSpec := &execinfrapb.DistinctSpec{
		DistinctColumns:  distinctCols,
		NullsAreDistinct: nullsAreDistinct,
		ErrorOnDup:       errorOnDup,
		OutputOrdering:   outputOrdering,
	}
	spec := &execinfrapb.ProcessorSpec{
		Input: []execinfrapb.InputSyncSpec{{ColumnTypes: typs}},
		Core: execinfrapb.ProcessorCoreUnion{
			Distinct: distinctSpec,
		},
		Post:        execinfrapb.PostProcessSpec{},
		ResultTypes: typs,
	}
	args := &colexecargs.NewColOperatorArgs{
		Spec:                spec,
		Inputs:              colexectestutils.MakeInputs(sources),
		StreamingMemAccount: testMemAcc,
		DiskQueueCfg:        diskQueueCfg,
		FDSemaphore:         testingSemaphore,
		MonitorRegistry:     monitorRegistry,
	}
	args.TestingKnobs.SpillingCallbackFn = spillingCallbackFn
	args.TestingKnobs.NumForcedRepartitions = numForcedRepartitions
	result, err := colexecargs.TestNewColOperator(ctx, flowCtx, args)
	return result.Root, result.ToClose, err
}
