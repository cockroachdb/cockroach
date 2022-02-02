// Copyright 2018 The Cockroach Authors.
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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/stretchr/testify/require"
)

var topKSortTestCases []sortTestCase

func init() {
	topKSortTestCases = []sortTestCase{
		{
			description: "k < input length",
			tuples:      colexectestutils.Tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			expected:    colexectestutils.Tuples{{1}, {2}, {3}},
			typs:        []*types.T{types.Int},
			ordCols:     []execinfrapb.Ordering_Column{{ColIdx: 0}},
			k:           3,
		},
		{
			description: "k > input length",
			tuples:      colexectestutils.Tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			expected:    colexectestutils.Tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			typs:        []*types.T{types.Int},
			ordCols:     []execinfrapb.Ordering_Column{{ColIdx: 0}},
			k:           10,
		},
		{
			description: "nulls",
			tuples:      colexectestutils.Tuples{{1}, {2}, {nil}, {3}, {4}, {5}, {6}, {7}, {nil}},
			expected:    colexectestutils.Tuples{{nil}, {nil}, {1}},
			typs:        []*types.T{types.Int},
			ordCols:     []execinfrapb.Ordering_Column{{ColIdx: 0}},
			k:           3,
		},
		{
			description: "descending",
			tuples:      colexectestutils.Tuples{{0, 1}, {0, 2}, {0, 3}, {0, 4}, {0, 5}, {1, 5}},
			expected:    colexectestutils.Tuples{{0, 5}, {1, 5}, {0, 4}},
			typs:        []*types.T{types.Int, types.Int},
			ordCols: []execinfrapb.Ordering_Column{
				{ColIdx: 1, Direction: execinfrapb.Ordering_Column_DESC},
				{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC},
			},
			k: 3,
		},
	}
}

func TestTopKSorter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range topKSortTestCases {
		log.Infof(context.Background(), "%s", tc.description)
		colexectestutils.RunTests(t, testAllocator, []colexectestutils.Tuples{tc.tuples}, tc.expected, colexectestutils.OrderedVerifier, func(input []colexecop.Operator) (colexecop.Operator, error) {
			return NewTopKSorter(testAllocator, input[0], tc.typs, tc.ordCols, tc.k, execinfra.DefaultMemoryLimit), nil
		})
	}
}

// TestTopKPlanning verifies that the top K sorter is planned only in case when
// it is beneficial to do so.
func TestTopKPlanning(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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

	createSorter := func(
		typs []*types.T,
		k uint64,
	) (colexecop.Operator, []*mon.BoundAccount, []*mon.BytesMonitor, []colexecop.Closer, error) {
		spec := &execinfrapb.ProcessorSpec{
			Input: []execinfrapb.InputSyncSpec{{ColumnTypes: typs}},
			Core: execinfrapb.ProcessorCoreUnion{
				Sorter: &execinfrapb.SorterSpec{
					OutputOrdering: execinfrapb.Ordering{
						Columns: []execinfrapb.Ordering_Column{{ColIdx: 0}},
					},
				},
			},
			Post: execinfrapb.PostProcessSpec{
				Limit: k,
			},
			ResultTypes: typs,
		}
		args := &colexecargs.NewColOperatorArgs{
			Spec:                spec,
			Inputs:              colexectestutils.MakeInputs([]colexecop.Operator{nil}),
			StreamingMemAccount: testMemAcc,
		}
		args.TestingKnobs.DiskSpillingDisabled = true
		result, err := colexecargs.TestNewColOperator(ctx, flowCtx, args)
		return result.Root, result.OpAccounts, result.OpMonitors, result.ToClose, err
	}

	for _, tc := range []struct {
		typs     []*types.T
		k        uint64
		topKUsed bool
	}{
		// No Bytes-like type, value of K doesn't matter.
		{typs: []*types.T{types.Int}, k: 2 * maxKWithBytesLikeType, topKUsed: true},
		// Bytes-like type with too large of a value of K.
		{typs: []*types.T{types.Bytes}, k: 2 * maxKWithBytesLikeType, topKUsed: false},
		// Bytes-like type with reasonably small value of K.
		{typs: []*types.T{types.Bytes}, k: maxKWithBytesLikeType, topKUsed: true},
	} {
		sorter, accounts, monitors, _, err := createSorter(tc.typs, tc.k)
		require.NoError(t, err)
		// We always plan a limitOp, which is redundant when the top K sorter is
		// used.
		_, isTopK := sorter.(*limitOp).Input.(*topKSorter)
		require.Equal(t, tc.topKUsed, isTopK)
		for _, a := range accounts {
			a.Close(ctx)
		}
		for _, m := range monitors {
			m.Stop(ctx)
		}
	}
}
