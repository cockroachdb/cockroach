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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
		DiskMonitor: testDiskMonitor,
	}

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()

	var (
		accounts []*mon.BoundAccount
		monitors []*mon.BytesMonitor
	)
	rng, _ := randutil.NewPseudoRand()
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
			// Check that the external distinct and the disk-backed sort
			// were added as Closers.
			numExpectedClosers := 2
			if tc.isOrderedOnDistinctCols {
				outputOrdering = convertDistinctColsToOrdering(tc.distinctCols)
				verifier = colexectestutils.OrderedVerifier
				// The final disk-backed sort must also be added as a
				// Closer.
				numExpectedClosers++
			}
			tc.runTests(t, verifier, func(input []colexecop.Operator) (colexecop.Operator, error) {
				// A sorter should never exceed ExternalSorterMinPartitions, even
				// during repartitioning. A panic will happen if a sorter requests
				// more than this number of file descriptors.
				sem := colexecop.NewTestingSemaphore(colexecop.ExternalSorterMinPartitions)
				semsToCheck = append(semsToCheck, sem)
				distinct, newAccounts, newMonitors, closers, err := createExternalDistinct(
					ctx, flowCtx, input, tc.typs, tc.distinctCols, tc.nullsAreDistinct, tc.errorOnDup,
					outputOrdering, queueCfg, sem, nil /* spillingCallbackFn */, numForcedRepartitions,
				)
				require.Equal(t, numExpectedClosers, len(closers))
				accounts = append(accounts, newAccounts...)
				monitors = append(monitors, newMonitors...)
				return distinct, err
			})
			if tc.errorOnDup == "" || tc.noError {
				// We don't check that all FDs were released if an error is
				// expected to be returned because our utility closeIfCloser()
				// doesn't handle multiple closers (which is always the case for
				// the external distinct).
				for i, sem := range semsToCheck {
					require.Equal(t, 0, sem.GetCount(), "sem still reports open FDs at index %d", i)
				}
			}
		}
	}
	for _, acc := range accounts {
		acc.Close(ctx)
	}
	for _, mon := range monitors {
		mon.Stop(ctx)
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
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
		DiskMonitor: testDiskMonitor,
	}

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()

	var (
		accounts []*mon.BoundAccount
		monitors []*mon.BytesMonitor
	)

	rng, _ := randutil.NewPseudoRand()
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
	nBatchesOutputByInMemoryOp := 2 + rng.Intn(2)
	memoryLimitBytes := int64(nBatchesOutputByInMemoryOp * batchMemEstimate)
	if memoryLimitBytes < mon.DefaultPoolAllocationSize {
		memoryLimitBytes = mon.DefaultPoolAllocationSize
		nBatchesOutputByInMemoryOp = int(memoryLimitBytes) / batchMemEstimate
	}
	flowCtx.Cfg.TestingKnobs.MemoryLimitBytes = memoryLimitBytes

	// Calculate the total number of distinct batches at least twice as large
	// as for the in-memory operator in order to make sure that the external
	// distinct has enough work to do.
	nDistinctBatches := nBatchesOutputByInMemoryOp * (2 + rng.Intn(2))
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
			distinct, newAccounts, newMonitors, closers, err := createExternalDistinct(
				ctx, flowCtx, input, typs, distinctCols, false /* nullsAreDistinct */, "", /* errorOnDup */
				outputOrdering, queueCfg, sem, func() { numSpills++ }, numForcedRepartitions,
			)
			require.NoError(t, err)
			// Check that the external distinct and the disk-backed sort
			// were added as Closers.
			numExpectedClosers := 2
			require.Equal(t, numExpectedClosers, len(closers))
			accounts = append(accounts, newAccounts...)
			monitors = append(monitors, newMonitors...)
			numRuns++
			return distinct, nil
		},
	)
	for i, sem := range semsToCheck {
		require.Equal(t, 0, sem.GetCount(), "sem still reports open FDs at index %d", i)
	}
	if !spillingMightNotHappen {
		require.Equal(t, numRuns, numSpills, "the spilling didn't occur in all cases")
	}

	for _, acc := range accounts {
		acc.Close(ctx)
	}
	for _, mon := range monitors {
		mon.Stop(ctx)
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
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
		DiskMonitor: testDiskMonitor,
	}
	var (
		memAccounts []*mon.BoundAccount
		memMonitors []*mon.BytesMonitor
	)

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(b, false /* inMem */)
	defer cleanup()

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
			name := fmt.Sprintf("spilled=%t/ordering=%t", spillForced, maintainOrdering)
			runDistinctBenchmarks(
				ctx,
				b,
				func(allocator *colmem.Allocator, input colexecop.Operator, distinctCols []uint32, numOrderedCols int, typs []*types.T) (colexecop.Operator, error) {
					var outputOrdering execinfrapb.Ordering
					if maintainOrdering {
						outputOrdering = convertDistinctColsToOrdering(distinctCols)
					}
					op, accs, mons, _, err := createExternalDistinct(
						ctx, flowCtx, []colexecop.Operator{input}, typs,
						distinctCols, false /* nullsAreDistinct */, "", /* errorOnDup */
						outputOrdering, queueCfg, &colexecop.TestingSemaphore{},
						nil /* spillingCallbackFn */, 0, /* numForcedRepartitions */
					)
					memAccounts = append(memAccounts, accs...)
					memMonitors = append(memMonitors, mons...)
					return op, err
				},
				func(nCols int) int {
					return 0
				},
				name,
				true, /* isExternal */
			)
		}
	}
	for _, account := range memAccounts {
		account.Close(ctx)
	}
	for _, monitor := range memMonitors {
		monitor.Stop(ctx)
	}
}

// createExternalDistinct is a helper function that instantiates a disk-backed
// distinct operator. It returns an operator and an error as well as memory
// monitors and memory accounts that will need to be closed once the caller is
// done with the operator.
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
) (colexecop.Operator, []*mon.BoundAccount, []*mon.BytesMonitor, []colexecop.Closer, error) {
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
	}
	args.TestingKnobs.SpillingCallbackFn = spillingCallbackFn
	args.TestingKnobs.NumForcedRepartitions = numForcedRepartitions
	result, err := colexecargs.TestNewColOperator(ctx, flowCtx, args)
	return result.Root, result.OpAccounts, result.OpMonitors, result.ToClose, err
}
