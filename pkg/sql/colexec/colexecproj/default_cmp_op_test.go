// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecproj

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestDefaultCmpProjOps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Mon:     evalCtx.TestingMon,
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
						ctx, flowCtx, input[0], c.inputTypes, c.cmpExpr, testMemAcc,
					)
				})
		})
	}
}

func BenchmarkDefaultCmpProjOp(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Mon:     evalCtx.TestingMon,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}
	var sb strings.Builder
	sb.WriteString("@1 = ANY (")
	for i := 0; i < 500; i++ {
		if i > 1 {
			sb.WriteByte(',')
		}
		sb.WriteString("'abc")
		sb.WriteString(strconv.Itoa(i))
		sb.WriteByte('\'')
	}
	sb.WriteByte(')')
	eqAny := sb.String()
	benchCases := []struct {
		name string
		expr string
		typs []*types.T
	}{
		{
			name: "IS DISTINCT FROM",
			expr: "@1 IS DISTINCT FROM @2",
			typs: []*types.T{types.String, types.String},
		},
		{
			name: "eq ANY const",
			expr: eqAny,
			typs: []*types.T{types.String},
		},
	}
	for _, benchCase := range benchCases {
		for _, useSel := range []bool{false, true} {
			for _, hasNulls := range []bool{false, true} {
				name := fmt.Sprintf("%s/useSel=%t/hasNulls=%t", benchCase.name, useSel, hasNulls)
				benchmarkProjOp(b, name, func(source *colexecop.RepeatableBatchSource) (colexecop.Operator, error) {
					return colexectestutils.CreateTestProjectingOperator(
						ctx, flowCtx, source, benchCase.typs,
						benchCase.expr, testMemAcc,
					)
				}, benchCase.typs, useSel, hasNulls)
			}
		}
	}
}
