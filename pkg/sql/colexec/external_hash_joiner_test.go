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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
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
		Cfg:     &execinfra.ServerConfig{Settings: st},
	}

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()

	var (
		memAccounts []*mon.BoundAccount
		memMonitors []*mon.BytesMonitor
	)
	// External hash joiner needs at least two partitions per side.
	const maxNumberPartitions = 4
	// Test the case in which the default memory is used as well as the case in
	// which the joiner spills to disk.
	for _, spillForced := range []bool{false, true} {
		flowCtx.Cfg.TestingKnobs.ForceDiskSpill = spillForced
		for _, tcs := range [][]joinTestCase{hjTestCases, mjTestCases} {
			for _, tc := range tcs {
				t.Run(fmt.Sprintf("spillForced=%t/%s", spillForced, tc.description), func(t *testing.T) {
					// Unfortunately, there is currently no better way to check that the
					// external hash joiner does not have leftover file descriptors other
					// than appending each semaphore used to this slice on construction.
					// This is because some tests don't fully drain the input, making
					// intercepting the Close() method not a useful option, since it is
					// impossible to check between an expected case where more than 0 FDs
					// are open (e.g. in allNullsInjection, where the joiner is not fully
					// drained so Close must be called explicitly) and an unexpected one.
					// These cases happen during normal execution when a limit is
					// satisfied, but flows will call Close explicitly on Cleanup.
					// TODO(yuzefovich): not implemented yet, currently we rely on the
					// flow tracking open FDs and releasing any leftovers.
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
					runHashJoinTestCase(t, tc, func(sources []Operator) (Operator, error) {
						sem := NewTestingSemaphore(maxNumberPartitions)
						semsToCheck = append(semsToCheck, sem)
						spec := createSpecForHashJoiner(tc)
						hjOp, accounts, monitors, err := createDiskBackedHashJoiner(
							ctx, flowCtx, spec, sources, func() {}, queueCfg,
							2 /* numForcedPartitions */, sem,
						)
						memAccounts = append(memAccounts, accounts...)
						memMonitors = append(memMonitors, monitors...)
						return hjOp, err
					})
					for i, sem := range semsToCheck {
						require.Equal(t, 0, sem.GetCount(), "sem still reports open FDs at index %d", i)
					}
				})
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
		},
	}
	sourceTypes := []coltypes.T{coltypes.Int64}
	batch := testAllocator.NewMemBatch(sourceTypes)
	// We don't need to set the data since zero values in the columns work.
	batch.SetLength(coldata.BatchSize())
	nBatches := 2
	leftSource := newFiniteBatchSource(batch, nBatches)
	rightSource := newFiniteBatchSource(batch, nBatches)
	spec := createSpecForHashJoiner(joinTestCase{
		joinType:     sqlbase.JoinType_INNER,
		leftTypes:    sourceTypes,
		leftOutCols:  []uint32{0},
		leftEqCols:   []uint32{0},
		rightTypes:   sourceTypes,
		rightOutCols: []uint32{0},
		rightEqCols:  []uint32{0},
	})
	var spilled bool
	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()
	// External hash joiner needs at least two partitions per side.
	const maxNumberPartitions = 4
	hj, accounts, monitors, err := createDiskBackedHashJoiner(
		ctx, flowCtx, spec, []Operator{leftSource, rightSource},
		func() { spilled = true }, queueCfg, 0, /* numForcedRepartitions */
		NewTestingSemaphore(maxNumberPartitions),
	)
	defer func() {
		for _, memAccount := range accounts {
			memAccount.Close(ctx)
		}
		for _, memMonitor := range monitors {
			memMonitor.Stop(ctx)
		}
	}()
	require.NoError(t, err)
	hj.Init()
	err = execerror.CatchVectorizedRuntimeError(func() {
		for b := hj.Next(ctx); b.Length() > 0; b = hj.Next(ctx) {
		}
	})
	require.True(t, spilled)
	// Currently, we don't have the fallback in place, so we expect an error.
	// TODO(yuzefovich): change this once we have the fallback.
	require.NotNil(t, err)
	require.True(t, strings.Contains(err.Error(), externalHJFallbackToSortMergeJoinMsg))
}

func BenchmarkExternalHashJoiner(b *testing.B) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg:     &execinfra.ServerConfig{Settings: st},
	}
	nCols := 4
	sourceTypes := make([]coltypes.T, nCols)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		sourceTypes[colIdx] = coltypes.Int64
	}

	batch := testAllocator.NewMemBatch(sourceTypes)
	for colIdx := 0; colIdx < nCols; colIdx++ {
		col := batch.ColVec(colIdx).Int64()
		for i := 0; i < int(coldata.BatchSize()); i++ {
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
		leftSource := newFiniteBatchSource(batch, 0)
		rightSource := newFiniteBatchSource(batch, 0)
		for _, fullOuter := range []bool{false, true} {
			for _, nBatches := range []int{1 << 2, 1 << 7} {
				for _, spillForced := range []bool{false, true} {
					flowCtx.Cfg.TestingKnobs.ForceDiskSpill = spillForced
					name := fmt.Sprintf(
						"nulls=%t/fullOuter=%t/batches=%d/spillForced=%t",
						hasNulls, fullOuter, nBatches, spillForced)
					joinType := sqlbase.JoinType_INNER
					if fullOuter {
						joinType = sqlbase.JoinType_FULL_OUTER
					}
					spec := createSpecForHashJoiner(joinTestCase{
						joinType:     joinType,
						leftTypes:    sourceTypes,
						leftOutCols:  []uint32{0, 1},
						leftEqCols:   []uint32{0, 2},
						rightTypes:   sourceTypes,
						rightOutCols: []uint32{2, 3},
						rightEqCols:  []uint32{0, 1},
					})
					b.Run(name, func(b *testing.B) {
						// 8 (bytes / int64) * nBatches (number of batches) * col.BatchSize() (rows /
						// batch) * nCols (number of columns / row) * 2 (number of sources).
						b.SetBytes(int64(8 * nBatches * int(coldata.BatchSize()) * nCols * 2))
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							leftSource.reset(nBatches)
							rightSource.reset(nBatches)
							hj, accounts, monitors, err := createDiskBackedHashJoiner(
								ctx, flowCtx, spec, []Operator{leftSource, rightSource},
								func() {}, queueCfg, 0, /* numForcedRepartitions */
								NewTestingSemaphore(VecMaxOpenFDsLimit),
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
	inputs []Operator,
	spillingCallbackFn func(),
	diskQueueCfg colcontainer.DiskQueueCfg,
	numForcedRepartitions int,
	testingSemaphore semaphore.Semaphore,
) (Operator, []*mon.BoundAccount, []*mon.BytesMonitor, error) {
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
	result, err := NewColOperator(ctx, flowCtx, args)
	return result.Op, result.BufferingOpMemAccounts, result.BufferingOpMemMonitors, err
}
