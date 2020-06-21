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
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
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
			Settings:    st,
			DiskMonitor: testDiskMonitor,
		},
		EvalCtx: &evalCtx,
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
			},
		},
		{
			desc: "MERGE JOIN",
			spec: &execinfrapb.ProcessorSpec{
				Input: twoInputs,
				Core: execinfrapb.ProcessorCoreUnion{
					MergeJoiner: &execinfrapb.MergeJoinerSpec{},
				},
			},
		},
	}

	for _, tc := range testCases {
		for _, success := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s-success-expected-%t", tc.desc, success), func(t *testing.T) {
				inputs := []colexecbase.Operator{colexec.NewZeroOpNoInput()}
				if len(tc.spec.Input) > 1 {
					inputs = append(inputs, colexec.NewZeroOpNoInput())
				}
				memMon := mon.MakeMonitor("MemoryMonitor", mon.MemoryResource, nil, nil, 0, math.MaxInt64, st)
				if success {
					memMon.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
				} else {
					memMon.Start(ctx, nil, mon.MakeStandaloneBudget(1))
				}
				defer memMon.Stop(ctx)
				acc := memMon.MakeBoundAccount()
				defer acc.Close(ctx)
				args := colexec.NewColOperatorArgs{
					Spec:                tc.spec,
					Inputs:              inputs,
					StreamingMemAccount: &acc,
				}
				args.TestingKnobs.UseStreamingMemAccountForBuffering = true
				result, err := colbuilder.NewColOperator(ctx, flowCtx, args)
				if err != nil {
					t.Fatal(err)
				}
				err = acc.Grow(ctx, int64(result.InternalMemUsage))
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
			Settings:    st,
			DiskMonitor: testDiskMonitor,
		},
		EvalCtx: &evalCtx,
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
								Func:   execinfrapb.AggregatorSpec_MAX,
								ColIdx: []uint32{0},
							},
						},
					},
				},
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
			},
			spillingSupported: true,
		},
	}

	typs := []*types.T{types.Int}
	batch := testAllocator.NewMemBatchWithSize(typs, 1 /* size */)
	for _, tc := range testCases {
		for _, success := range []bool{true, false} {
			expectNoMemoryError := success || tc.spillingSupported
			t.Run(fmt.Sprintf("%s-success-expected-%t", tc.desc, expectNoMemoryError), func(t *testing.T) {
				inputs := []colexecbase.Operator{colexecbase.NewRepeatableBatchSource(testAllocator, batch, typs)}
				if len(tc.spec.Input) > 1 {
					inputs = append(inputs, colexecbase.NewRepeatableBatchSource(testAllocator, batch, typs))
				}
				memMon := mon.MakeMonitor("MemoryMonitor", mon.MemoryResource, nil, nil, 0, math.MaxInt64, st)
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
					flowCtx.Cfg.TestingKnobs.MemoryLimitBytes = 1
				}
				defer memMon.Stop(ctx)
				acc := memMon.MakeBoundAccount()
				defer acc.Close(ctx)
				args := colexec.NewColOperatorArgs{
					Spec:                tc.spec,
					Inputs:              inputs,
					StreamingMemAccount: &acc,
					FDSemaphore:         colexecbase.NewTestingSemaphore(256),
				}
				// The disk spilling infrastructure relies on different memory
				// accounts, so if the spilling is supported, we do *not* want to use
				// streaming memory account.
				args.TestingKnobs.UseStreamingMemAccountForBuffering = !tc.spillingSupported
				var (
					result colexec.NewColOperatorResult
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
						result.Op.Init()
						result.Op.Next(ctx)
						result.Op.Next(ctx)
					})
				}
				for _, memAccount := range result.OpAccounts {
					memAccount.Close(ctx)
				}
				for _, memMonitor := range result.OpMonitors {
					memMonitor.Stop(ctx)
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
