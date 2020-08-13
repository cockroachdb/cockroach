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

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
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
		inputTuples  tuples
		outputTuples tuples
	}{
		{
			cmpExpr:    "@1 ILIKE @2",
			inputTypes: []*types.T{types.String, types.String},
			inputTuples: tuples{
				{"abc", "ABC"},
				{"a42", "A%"},
				{nil, "%A"},
				{"abc", "A%b"},
			},
			outputTuples: tuples{
				{"abc", "ABC", true},
				{"a42", "A%", true},
				{nil, "%A", nil},
				{"abc", "A%b", false},
			},
		},
		{
			cmpExpr:    "@1 ILIKE 'A%'",
			inputTypes: []*types.T{types.String},
			inputTuples: tuples{
				{"abc"},
				{"a42"},
				{nil},
				{"def"},
			},
			outputTuples: tuples{
				{"abc", true},
				{"a42", true},
				{nil, nil},
				{"def", false},
			},
		},
		{
			cmpExpr:    "@1 IS DISTINCT FROM @2",
			inputTypes: []*types.T{types.String, types.String},
			inputTuples: tuples{
				{"abc", "abc"},
				{nil, nil},
				{"abc", "ab"},
			},
			outputTuples: tuples{
				{"abc", "abc", false},
				{nil, nil, false},
				{"abc", "ab", true},
			},
		},
		{
			cmpExpr:    "(1, 2) IS DISTINCT FROM @1",
			inputTypes: []*types.T{types.MakeTuple([]*types.T{types.Int, types.Int})},
			inputTuples: tuples{
				{"(1, NULL)"},
				{nil},
				{"(1, 2)"},
			},
			outputTuples: tuples{
				{"(1, NULL)", true},
				{nil, true},
				{"(1, 2)", false},
			},
		},
	}
	for _, c := range testCases {
		t.Run(c.cmpExpr, func(t *testing.T) {
			runTestsWithTyps(t, []tuples{c.inputTuples}, [][]*types.T{c.inputTypes}, c.outputTuples, orderedVerifier,
				func(input []colexecbase.Operator) (colexecbase.Operator, error) {
					return createTestProjectingOperator(
						ctx, flowCtx, input[0], c.inputTypes,
						c.cmpExpr, false, /* canFallbackToRowexec */
					)
				})
		})
	}
}

func BenchmarkDefaultCmpProjOp(b *testing.B) {
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
			benchmarkProjOp(b, name, func(source *colexecbase.RepeatableBatchSource) (colexecbase.Operator, error) {
				return createTestProjectingOperator(
					ctx, flowCtx, source, inputTypes,
					"@1 IS DISTINCT FROM @2", false, /* canFallbackToRowexec */
				)
			}, inputTypes, useSel, hasNulls)
		}
	}
}
