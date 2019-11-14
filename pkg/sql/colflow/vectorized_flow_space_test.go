// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colflow

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

func TestVectorizeSpaceError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := &execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}

	oneInput := []execinfrapb.InputSyncSpec{
		{ColumnTypes: []types.T{*types.Int}},
	}
	twoInputs := []execinfrapb.InputSyncSpec{
		{ColumnTypes: []types.T{*types.Int}},
		{ColumnTypes: []types.T{*types.Int}},
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
		for _, succ := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s-success-expected-%t", tc.desc, succ), func(t *testing.T) {
				inputs := []colexec.Operator{colexec.NewZeroOp(nil)}
				if len(tc.spec.Input) > 1 {
					inputs = append(inputs, colexec.NewZeroOp(nil))
				}
				memMon := mon.MakeMonitor("MemoryMonitor", mon.MemoryResource, nil, nil, 0, math.MaxInt64, st)
				if succ {
					memMon.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
				} else {
					memMon.Start(ctx, nil, mon.MakeStandaloneBudget(1))
				}
				acc := memMon.MakeBoundAccount()
				result, err := colexec.NewColOperator(ctx, flowCtx, tc.spec, inputs, &mon.BoundAccount{})
				if err != nil {
					t.Fatal(err)
				}
				err = acc.Grow(ctx, int64(result.StaticMemUsage))
				if succ && err != nil {
					t.Fatal("Expected success, found:", err)
				}
				if !succ && err == nil {
					t.Fatal("Expected memory error, found nothing.")
				}
			})
		}
	}
}
