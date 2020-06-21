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
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/marusama/semaphore"
	"github.com/stretchr/testify/require"
)

func TestExternalHashJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings:    st,
			DiskMonitor: testDiskMonitor,
		},
	}

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()

	var (
		accounts []*mon.BoundAccount
		monitors []*mon.BytesMonitor
	)
	rng, _ := randutil.NewPseudoRand()
	// Test the case in which the default memory is used as well as the case in
	// which the joiner spills to disk.
	for _, spillForced := range []bool{false, true} {
		flowCtx.Cfg.TestingKnobs.ForceDiskSpill = spillForced
		for _, tcs := range [][]*joinTestCase{hjTestCases, mjTestCases} {
			for _, tc := range tcs {
				delegateFDAcquisitions := rng.Float64() < 0.5
				t.Run(fmt.Sprintf("spillForced=%t/%s/delegateFDAcquisitions=%t", spillForced, tc.description, delegateFDAcquisitions), func(t *testing.T) {
					var semsToCheck []semaphore.Semaphore
					if !tc.onExpr.Empty() {
						// When we have ON expression, there might be other operators (like
						// selections) on top of the external hash joiner in
						// diskSpiller.diskBackedOp chain. This will not allow for Close()
						// call to propagate to the external hash joiner, so we will skip
						// allNullsInjection test for now.
						defer func(oldValue bool) {
							tc.skipAllNullsInjection = oldValue
						}(tc.skipAllNullsInjection)
						tc.skipAllNullsInjection = true
					}
					runHashJoinTestCase(t, tc, func(sources []colexecbase.Operator) (colexecbase.Operator, error) {
						sem := colexecbase.NewTestingSemaphore(externalHJMinPartitions)
						semsToCheck = append(semsToCheck, sem)
						spec := createSpecForHashJoiner(tc)
						// TODO(asubiotto): Pass in the testing.T of the caller to this
						//  function and do substring matching on the test name to
						//  conditionally explicitly call Close() on the hash joiner
						//  (through result.ToClose) in cases where it is known the sorter
						//  will not be drained.
						hjOp, newAccounts, newMonitors, closers, err := createDiskBackedHashJoiner(
							ctx, flowCtx, spec, sources, func() {}, queueCfg,
							2 /* numForcedPartitions */, delegateFDAcquisitions, sem,
						)
						// Expect three closers. These are the external hash joiner, and
						// one external sorter for each input.
						// TODO(asubiotto): Explicitly Close when testing.T is passed into
						//  this constructor and we do a substring match.
						require.Equal(t, 3, len(closers))
						accounts = append(accounts, newAccounts...)
						monitors = append(monitors, newMonitors...)
						return hjOp, err
					})
					for i, sem := range semsToCheck {
						require.Equal(t, 0, sem.GetCount(), "sem still reports open FDs at index %d", i)
					}
				})
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

// TestExternalHashJoinerFallbackToSortMergeJoin tests that the external hash
// joiner falls back to using sort + merge join when repartitioning doesn't
// decrease the size of the partition. We instantiate two sources that contain
// the same tuple many times.
func TestExternalHashJoinerFallbackToSortMergeJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
			TestingKnobs: execinfra.TestingKnobs{
				ForceDiskSpill:   true,
				MemoryLimitBytes: 1,
			},
			DiskMonitor: testDiskMonitor,
		},
	}
	sourceTypes := []*types.T{types.Int}
	batch := testAllocator.NewMemBatch(sourceTypes)
	// We don't need to set the data since zero values in the columns work.
	batch.SetLength(coldata.BatchSize())
	nBatches := 2
	leftSource := newFiniteBatchSource(batch, sourceTypes, nBatches)
	rightSource := newFiniteBatchSource(batch, sourceTypes, nBatches)
	tc := &joinTestCase{
		joinType:     sqlbase.InnerJoin,
		leftTypes:    sourceTypes,
		leftOutCols:  []uint32{0},
		leftEqCols:   []uint32{0},
		rightTypes:   sourceTypes,
		rightOutCols: []uint32{0},
		rightEqCols:  []uint32{0},
	}
	tc.init()
	spec := createSpecForHashJoiner(tc)
	var spilled bool
	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()
	sem := colexecbase.NewTestingSemaphore(externalHJMinPartitions)
	// Ignore closers since the sorter should close itself when it is drained of
	// all tuples. We assert this by checking that the semaphore reports a count
	// of 0.
	hj, accounts, monitors, _, err := createDiskBackedHashJoiner(
		ctx, flowCtx, spec, []colexecbase.Operator{leftSource, rightSource},
		func() { spilled = true }, queueCfg, 0 /* numForcedRepartitions */, true, /* delegateFDAcquisitions */
		sem,
	)
	defer func() {
		for _, acc := range accounts {
			acc.Close(ctx)
		}
		for _, mon := range monitors {
			mon.Stop(ctx)
		}
	}()
	require.NoError(t, err)
	hj.Init()
	// We have a full cross-product, so we should get the number of tuples
	// squared in the output.
	expectedTuplesCount := nBatches * nBatches * coldata.BatchSize() * coldata.BatchSize()
	actualTuplesCount := 0
	for b := hj.Next(ctx); b.Length() > 0; b = hj.Next(ctx) {
		actualTuplesCount += b.Length()
	}
	require.True(t, spilled)
	require.Equal(t, expectedTuplesCount, actualTuplesCount)
	require.Equal(t, 0, sem.GetCount())
}

func BenchmarkExternalHashJoiner(b *testing.B) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings:    st,
			DiskMonitor: testDiskMonitor,
		},
	}
	nCols := 4
	sourceTypes := make([]*types.T, nCols)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		sourceTypes[colIdx] = types.Int
	}

	batch := testAllocator.NewMemBatch(sourceTypes)
	for colIdx := 0; colIdx < nCols; colIdx++ {
		col := batch.ColVec(colIdx).Int64()
		for i := 0; i < coldata.BatchSize(); i++ {
			col[i] = int64(i)
		}
	}
	batch.SetLength(coldata.BatchSize())

	var (
		memAccounts []*mon.BoundAccount
		memMonitors []*mon.BytesMonitor
	)
	for _, hasNulls := range []bool{false, true} {
		if hasNulls {
			for colIdx := 0; colIdx < nCols; colIdx++ {
				vec := batch.ColVec(colIdx)
				vec.Nulls().SetNull(0)
			}
		} else {
			for colIdx := 0; colIdx < nCols; colIdx++ {
				vec := batch.ColVec(colIdx)
				vec.Nulls().UnsetNulls()
			}
		}
		queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(b, false /* inMem */)
		defer cleanup()
		leftSource := newFiniteBatchSource(batch, sourceTypes, 0)
		rightSource := newFiniteBatchSource(batch, sourceTypes, 0)
		for _, fullOuter := range []bool{false, true} {
			for _, nBatches := range []int{1 << 2, 1 << 7} {
				for _, spillForced := range []bool{false, true} {
					flowCtx.Cfg.TestingKnobs.ForceDiskSpill = spillForced
					name := fmt.Sprintf(
						"nulls=%t/fullOuter=%t/batches=%d/spillForced=%t",
						hasNulls, fullOuter, nBatches, spillForced)
					joinType := sqlbase.InnerJoin
					if fullOuter {
						joinType = sqlbase.FullOuterJoin
					}
					tc := &joinTestCase{
						joinType:     joinType,
						leftTypes:    sourceTypes,
						leftOutCols:  []uint32{0, 1},
						leftEqCols:   []uint32{0, 2},
						rightTypes:   sourceTypes,
						rightOutCols: []uint32{2, 3},
						rightEqCols:  []uint32{0, 1},
					}
					tc.init()
					spec := createSpecForHashJoiner(tc)
					b.Run(name, func(b *testing.B) {
						// 8 (bytes / int64) * nBatches (number of batches) * col.BatchSize() (rows /
						// batch) * nCols (number of columns / row) * 2 (number of sources).
						b.SetBytes(int64(8 * nBatches * coldata.BatchSize() * nCols * 2))
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							leftSource.reset(nBatches)
							rightSource.reset(nBatches)
							hj, accounts, monitors, _, err := createDiskBackedHashJoiner(
								ctx, flowCtx, spec, []colexecbase.Operator{leftSource, rightSource},
								func() {}, queueCfg, 0 /* numForcedRepartitions */, false, /* delegateFDAcquisitions */
								colexecbase.NewTestingSemaphore(VecMaxOpenFDsLimit),
							)
							memAccounts = append(memAccounts, accounts...)
							memMonitors = append(memMonitors, monitors...)
							require.NoError(b, err)
							hj.Init()
							for b := hj.Next(ctx); b.Length() > 0; b = hj.Next(ctx) {
							}
						}
					})
				}
			}
		}
	}
	for _, memAccount := range memAccounts {
		memAccount.Close(ctx)
	}
	for _, memMonitor := range memMonitors {
		memMonitor.Stop(ctx)
	}
}

// createDiskBackedHashJoiner is a helper function that instantiates a
// disk-backed hash join operator. The desired memory limit must have been
// already set on flowCtx. It returns an operator and an error as well as
// memory monitors and memory accounts that will need to be closed once the
// caller is done with the operator.
func createDiskBackedHashJoiner(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.ProcessorSpec,
	inputs []colexecbase.Operator,
	spillingCallbackFn func(),
	diskQueueCfg colcontainer.DiskQueueCfg,
	numForcedRepartitions int,
	delegateFDAcquisitions bool,
	testingSemaphore semaphore.Semaphore,
) (colexecbase.Operator, []*mon.BoundAccount, []*mon.BytesMonitor, []IdempotentCloser, error) {
	args := NewColOperatorArgs{
		Spec:                spec,
		Inputs:              inputs,
		StreamingMemAccount: testMemAcc,
		DiskQueueCfg:        diskQueueCfg,
		FDSemaphore:         testingSemaphore,
	}
	// We will not use streaming memory account for the external hash join so
	// that the in-memory hash join operator could hit the memory limit set on
	// flowCtx.
	args.TestingKnobs.SpillingCallbackFn = spillingCallbackFn
	args.TestingKnobs.NumForcedRepartitions = numForcedRepartitions
	args.TestingKnobs.DelegateFDAcquisitions = delegateFDAcquisitions
	result, err := TestNewColOperator(ctx, flowCtx, args)
	return result.Op, result.OpAccounts, result.OpMonitors, result.ToClose, err
}
