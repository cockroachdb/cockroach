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
	// Interesting memory limits:
	// 0 - the default 64MiB value is used, so we exercise that the disk spilling
	//     machinery doesn't affect the correctness.
	// 1 - this will force the in-memory sorter to hit the memory limit right
	//     after it spools the first batch (i.e. it will buffer up a single batch
	//     and then will hit OOM) which will trigger the external sort.
	for _, memoryLimit := range []int64{0, 1} {
		flowCtx.Cfg.TestingKnobs.MemoryLimitBytes = memoryLimit
		t.Run(fmt.Sprintf("MemoryLimit=%d", memoryLimit), func(t *testing.T) {
			for _, tc := range sortTestCases {
				runTests(
					t,
					[]tuples{tc.tuples},
					tc.expected,
					orderedVerifier,
					func(input []Operator) (Operator, error) {
						sorter, accounts, monitors, err := createDiskBackedSorter(
							ctx, flowCtx, input, tc.logTypes, tc.ordCols, func() {},
						)
						memAccounts = append(memAccounts, accounts...)
						memMonitors = append(memMonitors, monitors...)
						return sorter, err
					})
			}
		})
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
	maxCols := 3
	// TODO(yuzefovich): randomize types as well.
	logTypes := make([]types.T, maxCols)
	for i := range logTypes {
		logTypes[i] = *types.Int
	}

	var (
		memAccounts []*mon.BoundAccount
		memMonitors []*mon.BytesMonitor
	)
	// Interesting memory limits:
	// 1 - this will force the in-memory sorter to hit the memory limit right
	//     after it spools the first batch (i.e. it will buffer up a single batch
	//     and then will hit OOM) which will trigger the external sort.
	// 10240 (mon.DefaultPoolAllocationSize) - this will allow the in-memory sorter
	//     to spool several batches before hitting the memory limit.
	for _, memoryLimit := range []int64{1, mon.DefaultPoolAllocationSize} {
		flowCtx.Cfg.TestingKnobs.MemoryLimitBytes = memoryLimit
		for nCols := 1; nCols < maxCols; nCols++ {
			for nOrderingCols := 1; nOrderingCols <= nCols; nOrderingCols++ {
				name := fmt.Sprintf("MemoryLimit=%d/nCols=%d/nOrderingCols=%d", memoryLimit, nCols, nOrderingCols)
				t.Run(name, func(t *testing.T) {
					tups, expected, ordCols := generateRandomDataForTestSort(rng, nTups, nCols, nOrderingCols)
					runTests(
						t,
						[]tuples{tups},
						expected,
						orderedVerifier,
						func(input []Operator) (Operator, error) {
							sorter, accounts, monitors, err := createDiskBackedSorter(
								ctx, flowCtx, input, logTypes[:nCols], ordCols, func() {},
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
			for _, memoryLimit := range []int64{0, 1} {
				flowCtx.Cfg.TestingKnobs.MemoryLimitBytes = memoryLimit
				shouldSpill := memoryLimit > 0
				name := fmt.Sprintf("rows=%d/cols=%d/spilled=%t", nBatches*int(coldata.BatchSize()), nCols, shouldSpill)
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
						var (
							resultBatches int
							spilled       bool
						)
						sorter, accounts, monitors, err := createDiskBackedSorter(
							ctx, flowCtx, []Operator{source}, logTypes, ordCols, func() { spilled = true },
						)
						memAccounts = append(memAccounts, accounts...)
						memMonitors = append(memMonitors, monitors...)
						if err != nil {
							b.Fatal(err)
						}
						resultBatches = nBatches
						sorter.Init()
						for i := 0; i < resultBatches; i++ {
							out := sorter.Next(ctx)
							if out.Length() == 0 {
								b.Fail()
							}
						}
						require.Equal(b, shouldSpill, spilled, fmt.Sprintf(
							"expected: spilled=%t\tactual: spilled=%t", shouldSpill, spilled,
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
	spillingCallbackFn func(),
) (Operator, []*mon.BoundAccount, []*mon.BytesMonitor, error) {
	sorterSpec := &execinfrapb.SorterSpec{}
	sorterSpec.OutputOrdering.Columns = ordCols
	spec := &execinfrapb.ProcessorSpec{
		Input: []execinfrapb.InputSyncSpec{{ColumnTypes: logTypes}},
		Core: execinfrapb.ProcessorCoreUnion{
			Sorter: sorterSpec,
		},
		Post: execinfrapb.PostProcessSpec{},
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
	result, err := NewColOperator(ctx, flowCtx, args)
	return result.Op, result.BufferingOpMemAccounts, result.BufferingOpMemMonitors, err
}
