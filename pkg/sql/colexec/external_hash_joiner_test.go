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
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
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

	var (
		memAccounts []*mon.BoundAccount
		memMonitors []*mon.BytesMonitor
	)
	// Test the case in which the default memory is used as well as the case in
	// which the joiner spills to disk.
	for _, spillForced := range []bool{false, true} {
		flowCtx.Cfg.TestingKnobs.ForceDiskSpill = spillForced
		t.Run(fmt.Sprintf("spillForced=%t", spillForced), func(t *testing.T) {
			for _, tcs := range [][]joinTestCase{hjTestCases, mjTestCases} {
				for _, tc := range tcs {
					runHashJoinTestCase(t, tc, func(sources []Operator) (Operator, error) {
						spec := createSpecForHashJoiner(tc)
						hjOp, accounts, monitors, err := createDiskBackedHashJoiner(
							ctx, flowCtx, spec, sources, func() {},
						)
						memAccounts = append(memAccounts, accounts...)
						memMonitors = append(memMonitors, monitors...)
						return hjOp, err
					})
				}
			}
		})
	}
	for _, memAccount := range memAccounts {
		memAccount.Close(ctx)
	}
	for _, memMonitor := range memMonitors {
		memMonitor.Stop(ctx)
	}
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
								ctx, flowCtx, spec, []Operator{leftSource, rightSource}, func() {},
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
) (Operator, []*mon.BoundAccount, []*mon.BytesMonitor, error) {
	args := NewColOperatorArgs{
		Spec:                spec,
		Inputs:              inputs,
		StreamingMemAccount: testMemAcc,
	}
	// We will not use streaming memory account for the external hash join so
	// that the in-memory hash join operator could hit the memory limit set on
	// flowCtx.
	args.TestingKnobs.SpillingCallbackFn = spillingCallbackFn
	result, err := NewColOperator(ctx, flowCtx, args)
	return result.Op, result.BufferingOpMemAccounts, result.BufferingOpMemMonitors, err
}
