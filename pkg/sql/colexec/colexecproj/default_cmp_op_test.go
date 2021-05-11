// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecproj

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestDefaultCmpProjOps(t *testing.T) {
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
	testCases := []struct {
		cmpExpr      string
		inputTypes   []*types.T
		inputTuples  colexectestutils.Tuples
		outputTuples colexectestutils.Tuples
	}{
		{
			cmpExpr:    "@1 ILIKE @2",
			inputTypes: []*types.T{types.String, types.String},
			inputTuples: colexectestutils.Tuples{
				{"abc", "ABC"},
				{"a42", "A%"},
				{nil, "%A"},
				{"abc", "A%b"},
			},
			outputTuples: colexectestutils.Tuples{
				{"abc", "ABC", true},
				{"a42", "A%", true},
				{nil, "%A", nil},
				{"abc", "A%b", false},
			},
		},
		{
			cmpExpr:    "@1 ILIKE 'A%'",
			inputTypes: []*types.T{types.String},
			inputTuples: colexectestutils.Tuples{
				{"abc"},
				{"a42"},
				{nil},
				{"def"},
			},
			outputTuples: colexectestutils.Tuples{
				{"abc", true},
				{"a42", true},
				{nil, nil},
				{"def", false},
			},
		},
		{
			cmpExpr:    "@1 IS DISTINCT FROM @2",
			inputTypes: []*types.T{types.String, types.String},
			inputTuples: colexectestutils.Tuples{
				{"abc", "abc"},
				{nil, nil},
				{"abc", "ab"},
			},
			outputTuples: colexectestutils.Tuples{
				{"abc", "abc", false},
				{nil, nil, false},
				{"abc", "ab", true},
			},
		},
		{
			cmpExpr:    "(1, 2) IS DISTINCT FROM @1",
			inputTypes: []*types.T{types.MakeTuple([]*types.T{types.Int, types.Int})},
			inputTuples: colexectestutils.Tuples{
				{"(1, NULL)"},
				{nil},
				{"(1, 2)"},
			},
			outputTuples: colexectestutils.Tuples{
				{"(1, NULL)", true},
				{nil, true},
				{"(1, 2)", false},
			},
		},
	}
	for _, c := range testCases {
		t.Run(c.cmpExpr, func(t *testing.T) {
			colexectestutils.RunTestsWithTyps(t, testAllocator, []colexectestutils.Tuples{c.inputTuples}, [][]*types.T{c.inputTypes}, c.outputTuples, colexectestutils.OrderedVerifier,
				func(input []colexecop.Operator) (colexecop.Operator, error) {
					return colexectestutils.CreateTestProjectingOperator(
						ctx, flowCtx, input[0], c.inputTypes,
						c.cmpExpr, false /* canFallbackToRowexec */, testMemAcc,
					)
				})
		})
	}
}

func BenchmarkDefaultCmpProjOp(b *testing.B) {
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
	for _, useSel := range []bool{false, true} {
		for _, hasNulls := range []bool{false, true} {
			inputTypes := []*types.T{types.String, types.String}
			name := fmt.Sprintf("IS DISTINCT FROM/useSel=%t/hasNulls=%t", useSel, hasNulls)
			benchmarkProjOp(b, name, func(source *colexecop.RepeatableBatchSource) (colexecop.Operator, error) {
				return colexectestutils.CreateTestProjectingOperator(
					ctx, flowCtx, source, inputTypes,
					"@1 IS DISTINCT FROM @2", false /* canFallbackToRowexec */, testMemAcc,
				)
			}, inputTypes, useSel, hasNulls)
		}
	}
}
