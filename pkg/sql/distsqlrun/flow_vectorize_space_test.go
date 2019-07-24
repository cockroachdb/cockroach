// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsqlrun

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
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

	flowCtx := &FlowCtx{
		Settings: st,
		EvalCtx:  &evalCtx,
	}

	// Without a limit, the default sorter creates a vectorized operater
	// that we don't know memory usage of statically.
	sorterCore := &distsqlpb.SorterSpec{
		OutputOrdering: distsqlpb.Ordering{
			Columns: []distsqlpb.Ordering_Column{
				{
					ColIdx:    0,
					Direction: distsqlpb.Ordering_Column_ASC,
				},
			},
		},
	}

	aggregatorCore := &distsqlpb.AggregatorSpec{
		Type: distsqlpb.AggregatorSpec_SCALAR,
		Aggregations: []distsqlpb.AggregatorSpec_Aggregation{
			{
				Func:   distsqlpb.AggregatorSpec_MAX,
				ColIdx: []uint32{0},
			},
		},
	}

	input := []distsqlpb.InputSyncSpec{{
		ColumnTypes: []types.T{*types.Int},
	}}

	testCases := []struct {
		desc string
		spec *distsqlpb.ProcessorSpec
	}{
		{
			desc: "topk",
			spec: &distsqlpb.ProcessorSpec{
				Input: input,
				Core: distsqlpb.ProcessorCoreUnion{
					Sorter: sorterCore,
				},
				Post: distsqlpb.PostProcessSpec{
					Limit: 5,
				},
			},
		},
		{
			desc: "projection",
			spec: &distsqlpb.ProcessorSpec{
				Input: input,
				Core: distsqlpb.ProcessorCoreUnion{
					Sorter: sorterCore,
				},
				Post: distsqlpb.PostProcessSpec{
					RenderExprs: []distsqlpb.Expression{{Expr: "@1 + 1"}},
				},
			},
		},
		{
			desc: "in_projection",
			spec: &distsqlpb.ProcessorSpec{
				Input: input,
				Core: distsqlpb.ProcessorCoreUnion{
					Sorter: sorterCore,
				},
				Post: distsqlpb.PostProcessSpec{
					RenderExprs: []distsqlpb.Expression{{Expr: "@1 IN (1, 2)"}},
				},
			},
		},
		{
			desc: "aggregation",
			spec: &distsqlpb.ProcessorSpec{
				Input: input,
				Core: distsqlpb.ProcessorCoreUnion{
					Aggregator: aggregatorCore,
				},
			},
		},
	}

	for _, tc := range testCases {
		for _, succ := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s-success-expected-%t", tc.desc, succ), func(t *testing.T) {
				inputs := []exec.Operator{exec.NewZeroOp(nil)}
				memMon := mon.MakeMonitor("MemoryMonitor", mon.MemoryResource, nil, nil, 0, math.MaxInt64, st)
				if succ {
					memMon.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
				} else {
					memMon.Start(ctx, nil, mon.MakeStandaloneBudget(1))
				}
				acc := memMon.MakeBoundAccount()
				result, err := newColOperator(ctx, flowCtx, tc.spec, inputs)
				if err != nil {
					t.Fatal(err)
				}
				err = acc.Grow(ctx, int64(result.memUsage))
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
