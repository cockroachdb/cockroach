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
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestExternalSort(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}

	var (
		memAccounts []*mon.BoundAccount
		memMonitors []*mon.BytesMonitor
	)
	const maxNumberPartitions = 2
	// Test the case in which the default memory is used as well as the case in
	// which the joiner spills to disk.
	for _, spillForced := range []bool{false, true} {
		flowCtx.Cfg.TestingKnobs.ForceDiskSpill = spillForced
		if spillForced {
			// In order to increase test coverage of recursive merging, we have the
			// lowest possible memory limit - this will force creating partitions
			// consisting of a single batch.
			flowCtx.Cfg.TestingKnobs.MemoryLimitBytes = 1
		} else {
			flowCtx.Cfg.TestingKnobs.MemoryLimitBytes = 0
		}
		for _, tcs := range [][]sortTestCase{sortAllTestCases, topKSortTestCases, sortChunksTestCases} {
			for _, tc := range tcs {
				t.Run(fmt.Sprintf("spillForced=%t/%s", spillForced, tc.description), func(t *testing.T) {
					runTests(
						t,
						[]tuples{tc.tuples},
						tc.expected,
						orderedVerifier,
						func(input []Operator) (Operator, error) {
							sorter, accounts, monitors, err := createDiskBackedSorter(
								ctx, flowCtx, input, tc.logTypes, tc.ordCols, tc.matchLen, tc.k, func() {},
								maxNumberPartitions,
							)
							memAccounts = append(memAccounts, accounts...)
							memMonitors = append(memMonitors, monitors...)
							return sorter, err
						})
				})
			}
		}
	}
	for _, account := range memAccounts {
		account.Close(ctx)
	}
	for _, monitor := range memMonitors {
		monitor.Stop(ctx)
	}
}

func TestExternalSortRandomized(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("https://github.com/cockroachdb/cockroach/issues/45322")
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}
	rng, _ := randutil.NewPseudoRand()
	nTups := int(coldata.BatchSize()*4 + 1)
	maxCols := 2
	// TODO(yuzefovich): randomize types as well.
	logTypes := make([]types.T, maxCols)
	for i := range logTypes {
		logTypes[i] = *types.Int
	}

	var (
		memAccounts []*mon.BoundAccount
		memMonitors []*mon.BytesMonitor
	)
	const maxNumberPartitions = 2
	// Interesting disk spilling scenarios:
	// 1) The sorter is forced to spill to disk as soon as possible.
	// 2) The memory limit is set to mon.DefaultPoolAllocationSize, this will
	//    allow the in-memory sorter to spool several batches before hitting the
	//    memory limit.
	for _, tk := range []execinfra.TestingKnobs{{ForceDiskSpill: true}, {MemoryLimitBytes: mon.DefaultPoolAllocationSize}} {
		flowCtx.Cfg.TestingKnobs = tk
		for nCols := 1; nCols <= maxCols; nCols++ {
			for nOrderingCols := 1; nOrderingCols <= nCols; nOrderingCols++ {
				namePrefix := "MemoryLimit=" + humanizeutil.IBytes(tk.MemoryLimitBytes)
				if tk.ForceDiskSpill {
					namePrefix = "ForceDiskSpill=true"
				}
				name := fmt.Sprintf("%s/nCols=%d/nOrderingCols=%d", namePrefix, nCols, nOrderingCols)
				t.Run(name, func(t *testing.T) {
					tups, expected, ordCols := generateRandomDataForTestSort(rng, nTups, nCols, nOrderingCols)
					runTests(
						t,
						[]tuples{tups},
						expected,
						orderedVerifier,
						func(input []Operator) (Operator, error) {
							sorter, accounts, monitors, err := createDiskBackedSorter(
								ctx, flowCtx, input, logTypes[:nCols], ordCols,
								0 /* matchLen */, 0 /* k */, func() {},
								maxNumberPartitions,
							)
							memAccounts = append(memAccounts, accounts...)
							memMonitors = append(memMonitors, monitors...)
							return sorter, err
						})
				})
			}
		}
	}
	for _, account := range memAccounts {
		account.Close(ctx)
	}
	for _, monitor := range memMonitors {
		monitor.Stop(ctx)
	}
}

func BenchmarkExternalSort(b *testing.B) {
	defer leaktest.AfterTest(b)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}
	rng, _ := randutil.NewPseudoRand()
	var (
		memAccounts []*mon.BoundAccount
		memMonitors []*mon.BytesMonitor
	)

	for _, nBatches := range []int{1 << 1, 1 << 4, 1 << 8} {
		for _, nCols := range []int{1, 2, 4} {
			for _, spillForced := range []bool{false, true} {
				flowCtx.Cfg.TestingKnobs.ForceDiskSpill = spillForced
				name := fmt.Sprintf("rows=%d/cols=%d/spilled=%t", nBatches*int(coldata.BatchSize()), nCols, spillForced)
				b.Run(name, func(b *testing.B) {
					// 8 (bytes / int64) * nBatches (number of batches) * coldata.BatchSize() (rows /
					// batch) * nCols (number of columns / row).
					b.SetBytes(int64(8 * nBatches * int(coldata.BatchSize()) * nCols))
					logTypes := make([]types.T, nCols)
					for i := range logTypes {
						logTypes[i] = *types.Int
					}
					physTypes, err := typeconv.FromColumnTypes(logTypes)
					require.NoError(b, err)
					batch := testAllocator.NewMemBatch(physTypes)
					batch.SetLength(coldata.BatchSize())
					ordCols := make([]execinfrapb.Ordering_Column, nCols)
					for i := range ordCols {
						ordCols[i].ColIdx = uint32(i)
						ordCols[i].Direction = execinfrapb.Ordering_Column_Direction(rng.Int() % 2)
						col := batch.ColVec(i).Int64()
						for j := 0; j < int(coldata.BatchSize()); j++ {
							col[j] = rng.Int63() % int64((i*1024)+1)
						}
					}
					b.ResetTimer()
					for n := 0; n < b.N; n++ {
						source := newFiniteBatchSource(batch, nBatches)
						var spilled bool
						// TODO(yuzefovich): do not specify maxNumberPartitions (let the
						// external sorter figure out that number itself) once we pass in
						// filled-in disk queue config.
						sorter, accounts, monitors, err := createDiskBackedSorter(
							ctx, flowCtx, []Operator{source}, logTypes, ordCols,
							0 /* matchLen */, 0 /* k */, func() { spilled = true },
							64, /* maxNumberPartitions */
						)
						memAccounts = append(memAccounts, accounts...)
						memMonitors = append(memMonitors, monitors...)
						if err != nil {
							b.Fatal(err)
						}
						sorter.Init()
						for out := sorter.Next(ctx); out.Length() != 0; out = sorter.Next(ctx) {
						}
						require.Equal(b, spillForced, spilled, fmt.Sprintf(
							"expected: spilled=%t\tactual: spilled=%t", spillForced, spilled,
						))
					}
				})
			}
		}
	}
	for _, account := range memAccounts {
		account.Close(ctx)
	}
	for _, monitor := range memMonitors {
		monitor.Stop(ctx)
	}
}

// createDiskBackedSorter is a helper function that instantiates a disk-backed
// sort operator. The desired memory limit must have been already set on
// flowCtx. It returns an operator and an error as well as memory monitors and
// memory accounts that will need to be closed once the caller is done with the
// operator.
func createDiskBackedSorter(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	input []Operator,
	logTypes []types.T,
	ordCols []execinfrapb.Ordering_Column,
	matchLen int,
	k uint16,
	spillingCallbackFn func(),
	maxNumberPartitions int,
) (Operator, []*mon.BoundAccount, []*mon.BytesMonitor, error) {
	sorterSpec := &execinfrapb.SorterSpec{
		OutputOrdering:   execinfrapb.Ordering{Columns: ordCols},
		OrderingMatchLen: uint32(matchLen),
	}
	spec := &execinfrapb.ProcessorSpec{
		Input: []execinfrapb.InputSyncSpec{{ColumnTypes: logTypes}},
		Core: execinfrapb.ProcessorCoreUnion{
			Sorter: sorterSpec,
		},
		Post: execinfrapb.PostProcessSpec{
			Limit: uint64(k),
		},
	}
	args := NewColOperatorArgs{
		Spec:                spec,
		Inputs:              input,
		StreamingMemAccount: testMemAcc,
	}
	// External sorter relies on different memory accounts to
	// understand when to start a new partition, so we will not use
	// the streaming memory account.
	args.TestingKnobs.SpillingCallbackFn = spillingCallbackFn
	args.TestingKnobs.MaxNumberPartitions = maxNumberPartitions
	result, err := NewColOperator(ctx, flowCtx, args)
	return result.Op, result.BufferingOpMemAccounts, result.BufferingOpMemMonitors, err
}
