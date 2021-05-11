// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colflow_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/stretchr/testify/require"
)

func TestVectorizeInternalMemorySpaceError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := &execinfra.FlowCtx{
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
		DiskMonitor: testDiskMonitor,
		EvalCtx:     &evalCtx,
	}

	oneInput := []execinfrapb.InputSyncSpec{
		{ColumnTypes: []*types.T{types.Int}},
	}

	testCases := []struct {
		desc string
		spec *execinfrapb.ProcessorSpec
	}{
		{
			desc: "CASE",
			spec: &execinfrapb.ProcessorSpec{
				Input: oneInput,
				Core: execinfrapb.ProcessorCoreUnion{
					Noop: &execinfrapb.NoopCoreSpec{},
				},
				Post: execinfrapb.PostProcessSpec{
					RenderExprs: []execinfrapb.Expression{{Expr: "CASE WHEN @1 = 1 THEN 1 ELSE 2 END"}},
				},
				ResultTypes: types.OneIntCol,
			},
		},
	}

	for _, tc := range testCases {
		for _, success := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s-success-expected-%t", tc.desc, success), func(t *testing.T) {
				sources := []colexecop.Operator{colexecutils.NewFixedNumTuplesNoInputOp(testAllocator, 0 /* numTuples */, nil /* opToInitialize */)}
				if len(tc.spec.Input) > 1 {
					sources = append(sources, colexecutils.NewFixedNumTuplesNoInputOp(testAllocator, 0 /* numTuples */, nil /* opToInitialize */))
				}
				memMon := mon.NewMonitor("MemoryMonitor", mon.MemoryResource, nil, nil, 0, math.MaxInt64, st)
				if success {
					memMon.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
				} else {
					memMon.Start(ctx, nil, mon.MakeStandaloneBudget(1))
				}
				defer memMon.Stop(ctx)
				acc := memMon.MakeBoundAccount()
				defer acc.Close(ctx)
				args := &colexecargs.NewColOperatorArgs{
					Spec:                tc.spec,
					Inputs:              colexectestutils.MakeInputs(sources),
					StreamingMemAccount: &acc,
				}
				var setupErr error
				err := colexecerror.CatchVectorizedRuntimeError(func() {
					_, setupErr = colbuilder.NewColOperator(ctx, flowCtx, args)
				})
				if setupErr != nil {
					t.Fatal(setupErr)
				}
				if success {
					require.NoError(t, err, "expected success, found: ", err)
				} else {
					require.Error(t, err, "expected memory error, found nothing")
				}
			})
		}
	}
}

func TestVectorizeAllocatorSpaceError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := &execinfra.FlowCtx{
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
		DiskMonitor: testDiskMonitor,
		EvalCtx:     &evalCtx,
	}

	oneInput := []execinfrapb.InputSyncSpec{
		{ColumnTypes: []*types.T{types.Int}},
	}
	twoInputs := []execinfrapb.InputSyncSpec{
		{ColumnTypes: []*types.T{types.Int}},
		{ColumnTypes: []*types.T{types.Int}},
	}

	testCases := []struct {
		desc string
		spec *execinfrapb.ProcessorSpec
		// spillingSupported, if set to true, indicates that disk spilling for the
		// operator is supported and we expect success only.
		spillingSupported bool
	}{
		{
			desc: "SORTER",
			spec: &execinfrapb.ProcessorSpec{
				Input: oneInput,
				Core: execinfrapb.ProcessorCoreUnion{
					Sorter: &execinfrapb.SorterSpec{
						OutputOrdering: execinfrapb.Ordering{
							Columns: []execinfrapb.Ordering_Column{
								{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC},
							},
						},
					},
				},
				ResultTypes: oneInput[0].ColumnTypes,
			},
			spillingSupported: true,
		},
		{
			desc: "HASH AGGREGATOR",
			spec: &execinfrapb.ProcessorSpec{
				Input: oneInput,
				Core: execinfrapb.ProcessorCoreUnion{
					Aggregator: &execinfrapb.AggregatorSpec{
						Type: execinfrapb.AggregatorSpec_SCALAR,
						Aggregations: []execinfrapb.AggregatorSpec_Aggregation{
							{
								Func:   execinfrapb.Max,
								ColIdx: []uint32{0},
							},
						},
					},
				},
				ResultTypes: oneInput[0].ColumnTypes,
			},
		},
		{
			desc: "HASH JOINER",
			spec: &execinfrapb.ProcessorSpec{
				Input: twoInputs,
				Core: execinfrapb.ProcessorCoreUnion{
					HashJoiner: &execinfrapb.HashJoinerSpec{
						LeftEqColumns:  []uint32{0},
						RightEqColumns: []uint32{0},
					},
				},
				ResultTypes: append(twoInputs[0].ColumnTypes, twoInputs[1].ColumnTypes...),
			},
			spillingSupported: true,
		},
	}

	typs := []*types.T{types.Int}
	batch := testAllocator.NewMemBatchWithFixedCapacity(typs, 1 /* size */)
	for _, tc := range testCases {
		for _, success := range []bool{true, false} {
			expectNoMemoryError := success || tc.spillingSupported
			t.Run(fmt.Sprintf("%s-success-expected-%t", tc.desc, expectNoMemoryError), func(t *testing.T) {
				sources := []colexecop.Operator{colexecop.NewRepeatableBatchSource(testAllocator, batch, typs)}
				if len(tc.spec.Input) > 1 {
					sources = append(sources, colexecop.NewRepeatableBatchSource(testAllocator, batch, typs))
				}
				memMon := mon.NewMonitor("MemoryMonitor", mon.MemoryResource, nil, nil, 0, math.MaxInt64, st)
				flowCtx.Cfg.TestingKnobs = execinfra.TestingKnobs{}
				if expectNoMemoryError {
					memMon.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
					if !success {
						// These are the cases that we expect in-memory operators to hit a
						// memory error. To enable testing this case, force disk spills. We
						// do this in this if branch to allow the external algorithms to use
						// an unlimited monitor.
						flowCtx.Cfg.TestingKnobs.ForceDiskSpill = true
					}
				} else {
					memMon.Start(ctx, nil, mon.MakeStandaloneBudget(1))
					flowCtx.Cfg.TestingKnobs.ForceDiskSpill = true
				}
				defer memMon.Stop(ctx)
				acc := memMon.MakeBoundAccount()
				defer acc.Close(ctx)
				args := &colexecargs.NewColOperatorArgs{
					Spec:                tc.spec,
					Inputs:              colexectestutils.MakeInputs(sources),
					StreamingMemAccount: &acc,
					FDSemaphore:         colexecop.NewTestingSemaphore(256),
				}
				// The disk spilling infrastructure relies on different memory
				// accounts, so if the spilling is supported, we do *not* want to use
				// streaming memory account.
				args.TestingKnobs.UseStreamingMemAccountForBuffering = !tc.spillingSupported
				var (
					result *colexecargs.NewColOperatorResult
					err    error
				)
				// The memory error can occur either during planning or during
				// execution, and we want to actually execute the "query" only
				// if there was no error during planning. That is why we have
				// two separate panic-catchers.
				if err = colexecerror.CatchVectorizedRuntimeError(func() {
					result, err = colbuilder.NewColOperator(ctx, flowCtx, args)
					require.NoError(t, err)
				}); err == nil {
					err = colexecerror.CatchVectorizedRuntimeError(func() {
						result.Root.Init(ctx)
						result.Root.Next()
						result.Root.Next()
					})
				}
				if result != nil {
					for _, memAccount := range result.OpAccounts {
						memAccount.Close(ctx)
					}
					for _, memMonitor := range result.OpMonitors {
						memMonitor.Stop(ctx)
					}
				}
				if expectNoMemoryError {
					require.NoError(t, err, "expected success, found: ", err)
				} else {
					require.Error(t, err, "expected memory error, found nothing")
				}
			})
		}
	}
}
