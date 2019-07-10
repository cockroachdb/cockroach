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
			desc: "Test construct sorttopk",
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
			desc: "Test simple projection op",
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
			desc: "Test in projection op",
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
			desc: "Test aggregations",
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
				var memMon mon.BytesMonitor
				if succ {
					memMon = mon.MakeUnlimitedMonitor(
						ctx, "Unlimited Monitor", mon.MemoryResource, nil, nil, math.MaxInt64, st)
				} else {
					memMon = mon.MakeMonitorWithLimit(
						"Limited Monitor", mon.MemoryResource, 1, nil, nil, 1, math.MaxInt64, st)
				}
				acc := memMon.MakeBoundAccount()
				_, _, err := newColOperator(ctx, flowCtx, tc.spec, inputs, &acc)
				if succ && err != nil {
					t.Fatal("Expected success, found: ", err)
				}
				if !succ && err == nil {
					t.Fatal("Expected memory error, found nothing.")
				}
			})
		}
	}
}
