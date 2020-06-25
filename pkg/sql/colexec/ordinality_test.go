// Copyright 2019 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestOrdinality(t *testing.T) {
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
	tcs := []struct {
		tuples     []tuple
		expected   []tuple
		inputTypes []*types.T
	}{
		{
			tuples:     tuples{{1}},
			expected:   tuples{{1, 1}},
			inputTypes: []*types.T{types.Int},
		},
		{
			tuples:     tuples{{}, {}, {}, {}, {}},
			expected:   tuples{{1}, {2}, {3}, {4}, {5}},
			inputTypes: []*types.T{},
		},
		{
			tuples:     tuples{{5}, {6}, {7}, {8}},
			expected:   tuples{{5, 1}, {6, 2}, {7, 3}, {8, 4}},
			inputTypes: []*types.T{types.Int},
		},
		{
			tuples:     tuples{{5, 'a'}, {6, 'b'}, {7, 'c'}, {8, 'd'}},
			expected:   tuples{{5, 'a', 1}, {6, 'b', 2}, {7, 'c', 3}, {8, 'd', 4}},
			inputTypes: []*types.T{types.Int, types.String},
		},
	}

	for _, tc := range tcs {
		runTests(t, []tuples{tc.tuples}, tc.expected, orderedVerifier,
			func(input []colexecbase.Operator) (colexecbase.Operator, error) {
				return createTestOrdinalityOperator(ctx, flowCtx, input[0], tc.inputTypes)
			})
	}
}

func BenchmarkOrdinality(b *testing.B) {
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

	typs := []*types.T{types.Int, types.Int, types.Int}
	batch := testAllocator.NewMemBatch(typs)
	batch.SetLength(coldata.BatchSize())
	source := colexecbase.NewRepeatableBatchSource(testAllocator, batch, typs)
	ordinality, err := createTestOrdinalityOperator(ctx, flowCtx, source, []*types.T{types.Int, types.Int, types.Int})
	require.NoError(b, err)
	ordinality.Init()

	b.SetBytes(int64(8 * coldata.BatchSize()))
	for i := 0; i < b.N; i++ {
		ordinality.Next(ctx)
	}
}

func createTestOrdinalityOperator(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	input colexecbase.Operator,
	inputTypes []*types.T,
) (colexecbase.Operator, error) {
	spec := &execinfrapb.ProcessorSpec{
		Input: []execinfrapb.InputSyncSpec{{ColumnTypes: inputTypes}},
		Core: execinfrapb.ProcessorCoreUnion{
			Ordinality: &execinfrapb.OrdinalitySpec{},
		},
	}
	args := NewColOperatorArgs{
		Spec:                spec,
		Inputs:              []colexecbase.Operator{input},
		StreamingMemAccount: testMemAcc,
	}
	args.TestingKnobs.UseStreamingMemAccountForBuffering = true
	result, err := TestNewColOperator(ctx, flowCtx, args)
	if err != nil {
		return nil, err
	}
	return result.Op, nil
}
