// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecbase_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
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
	"github.com/stretchr/testify/require"
)

func TestOrdinality(t *testing.T) {
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
	tcs := []struct {
		tuples     []colexectestutils.Tuple
		expected   []colexectestutils.Tuple
		inputTypes []*types.T
	}{
		{
			tuples:     colexectestutils.Tuples{{1}},
			expected:   colexectestutils.Tuples{{1, 1}},
			inputTypes: []*types.T{types.Int},
		},
		{
			tuples:     colexectestutils.Tuples{{}, {}, {}, {}, {}},
			expected:   colexectestutils.Tuples{{1}, {2}, {3}, {4}, {5}},
			inputTypes: []*types.T{},
		},
		{
			tuples:     colexectestutils.Tuples{{5}, {6}, {7}, {8}},
			expected:   colexectestutils.Tuples{{5, 1}, {6, 2}, {7, 3}, {8, 4}},
			inputTypes: []*types.T{types.Int},
		},
		{
			tuples:     colexectestutils.Tuples{{5, 'a'}, {6, 'b'}, {7, 'c'}, {8, 'd'}},
			expected:   colexectestutils.Tuples{{5, 'a', 1}, {6, 'b', 2}, {7, 'c', 3}, {8, 'd', 4}},
			inputTypes: []*types.T{types.Int, types.String},
		},
	}

	for _, tc := range tcs {
		colexectestutils.RunTests(t, testAllocator, []colexectestutils.Tuples{tc.tuples}, tc.expected, colexectestutils.OrderedVerifier,
			func(input []colexecop.Operator) (colexecop.Operator, error) {
				return createTestOrdinalityOperator(ctx, flowCtx, input[0], tc.inputTypes)
			})
	}
}

func BenchmarkOrdinality(b *testing.B) {
	defer log.Scope(b).Close(b)
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
	batch := testAllocator.NewMemBatchWithMaxCapacity(typs)
	batch.SetLength(coldata.BatchSize())
	source := colexecop.NewRepeatableBatchSource(testAllocator, batch, typs)
	ordinality, err := createTestOrdinalityOperator(ctx, flowCtx, source, []*types.T{types.Int, types.Int, types.Int})
	require.NoError(b, err)
	ordinality.Init(ctx)

	b.SetBytes(int64(8 * coldata.BatchSize()))
	for i := 0; i < b.N; i++ {
		ordinality.Next()
	}
}

func createTestOrdinalityOperator(
	ctx context.Context, flowCtx *execinfra.FlowCtx, input colexecop.Operator, inputTypes []*types.T,
) (colexecop.Operator, error) {
	spec := &execinfrapb.ProcessorSpec{
		Input: []execinfrapb.InputSyncSpec{{ColumnTypes: inputTypes}},
		Core: execinfrapb.ProcessorCoreUnion{
			Ordinality: &execinfrapb.OrdinalitySpec{},
		},
		ResultTypes: append(inputTypes, types.Int),
	}
	args := &colexecargs.NewColOperatorArgs{
		Spec:                spec,
		Inputs:              []colexecargs.OpWithMetaInfo{{Root: input}},
		StreamingMemAccount: testMemAcc,
	}
	result, err := colexecargs.TestNewColOperator(ctx, flowCtx, args)
	if err != nil {
		return nil, err
	}
	return result.Root, nil
}
