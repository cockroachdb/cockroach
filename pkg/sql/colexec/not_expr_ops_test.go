// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestNotExprProjOp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	testCases := []struct {
		desc         string
		inputTuples  colexectestutils.Tuples
		outputTuples colexectestutils.Tuples
		projExpr     string
	}{
		{
			desc:         "SELECT NOT c FROM t -- NOT expr with no NULL",
			inputTuples:  colexectestutils.Tuples{{true}, {false}, {true}, {false}},
			outputTuples: colexectestutils.Tuples{{true, false}, {false, true}, {true, false}, {false, true}},
			projExpr:     "NOT",
		},
		{
			desc:         "SELECT NOT c FROM t -- NOT expr with only TRUE",
			inputTuples:  colexectestutils.Tuples{{true}, {true}, {true}, {true}},
			outputTuples: colexectestutils.Tuples{{true, false}, {true, false}, {true, false}, {true, false}},
			projExpr:     "NOT",
		},
		{
			desc:         "SELECT NOT c FROM t -- NOT expr with only FALSE",
			inputTuples:  colexectestutils.Tuples{{false}, {false}, {false}, {false}},
			outputTuples: colexectestutils.Tuples{{false, true}, {false, true}, {false, true}, {false, true}},
			projExpr:     "NOT",
		},
		{
			desc:         "SELECT NOT c FROM t -- NOT expr with only NULL",
			inputTuples:  colexectestutils.Tuples{{nil}, {nil}, {nil}, {nil}},
			outputTuples: colexectestutils.Tuples{{nil, nil}, {nil, nil}, {nil, nil}, {nil, nil}},
			projExpr:     "NOT",
		},
		{
			desc:         "SELECT NOT c FROM t -- NOT expr with NULL and only FALSE",
			inputTuples:  colexectestutils.Tuples{{nil}, {false}, {nil}, {false}},
			outputTuples: colexectestutils.Tuples{{nil, nil}, {false, true}, {nil, nil}, {false, true}},
			projExpr:     "NOT",
		},
		{
			desc:         "SELECT NOT c FROM t -- NOT expr with NULL and only TRUE",
			inputTuples:  colexectestutils.Tuples{{nil}, {true}, {nil}, {true}},
			outputTuples: colexectestutils.Tuples{{nil, nil}, {true, false}, {nil, nil}, {true, false}},
			projExpr:     "NOT",
		},
		{
			desc:         "SELECT NOT c FROM t -- NOT expr with NULL and both BOOL",
			inputTuples:  colexectestutils.Tuples{{nil}, {true}, {nil}, {false}},
			outputTuples: colexectestutils.Tuples{{nil, nil}, {true, false}, {nil, nil}, {false, true}},
			projExpr:     "NOT",
		},
	}

	for _, c := range testCases {
		log.Infof(ctx, "%s", c.desc)
		opConstructor := func(input []colexecop.Operator) (colexecop.Operator, error) {
			return NewNotExprProjOp(types.BoolFamily, testAllocator, input[0], 0, 1)
		}
		colexectestutils.RunTestsWithoutAllNullsInjection(t, testAllocator, []colexectestutils.Tuples{c.inputTuples}, [][]*types.T{{types.Bool}}, c.outputTuples, colexectestutils.OrderedVerifier, opConstructor)
	}
}

func TestNotExprSelOp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	testCases := []struct {
		desc         string
		inputTuples  colexectestutils.Tuples
		outputTuples colexectestutils.Tuples
		selExpr      string
	}{
		{
			desc:         "SELECT c FROM t WHERE NOT c -- NOT expr with no NULL",
			inputTuples:  colexectestutils.Tuples{{true}, {false}, {true}, {false}},
			outputTuples: colexectestutils.Tuples{{false}, {false}},
			selExpr:      "NOT",
		},
		{
			desc:         "SELECT c FROM t WHERE NOT c -- NOT expr with only FALSE",
			inputTuples:  colexectestutils.Tuples{{false}, {false}, {false}, {false}},
			outputTuples: colexectestutils.Tuples{{false}, {false}, {false}, {false}},
			selExpr:      "NOT",
		},
		{
			desc:         "SELECT c FROM t WHERE NOT c -- NOT expr with only TRUE",
			inputTuples:  colexectestutils.Tuples{{true}, {true}, {true}, {true}},
			outputTuples: colexectestutils.Tuples{},
			selExpr:      "NOT",
		},
		{
			desc:         "SELECT c FROM t WHERE NOT c -- NOT expr with only one FALSE and rest TRUE",
			inputTuples:  colexectestutils.Tuples{{true}, {false}, {true}, {true}},
			outputTuples: colexectestutils.Tuples{{false}},
			selExpr:      "NOT",
		},
		{
			desc:         "SELECT c FROM t WHERE NOT c -- NOT expr with only one TRUE and rest FALSE",
			inputTuples:  colexectestutils.Tuples{{false}, {true}, {false}, {false}},
			outputTuples: colexectestutils.Tuples{{false}, {false}, {false}},
			selExpr:      "NOT",
		},
		{
			desc:         "SELECT c FROM t WHERE NOT c -- NOT expr with FALSE and NULL",
			inputTuples:  colexectestutils.Tuples{{nil}, {nil}, {false}, {nil}},
			outputTuples: colexectestutils.Tuples{{false}},
			selExpr:      "NOT",
		},
		{
			desc:         "SELECT c FROM t WHERE NOT c -- NOT expr with TRUE, FALSE and NULL",
			inputTuples:  colexectestutils.Tuples{{false}, {true}, {false}, {nil}},
			outputTuples: colexectestutils.Tuples{{false}, {false}},
			selExpr:      "NOT",
		},
		{
			desc:         "SELECT c FROM t WHERE NOT c -- NOT expr with only NULL",
			inputTuples:  colexectestutils.Tuples{{nil}, {nil}, {nil}, {nil}},
			outputTuples: colexectestutils.Tuples{},
			selExpr:      "NOT",
		},
	}

	for _, c := range testCases {
		log.Infof(ctx, "%s", c.desc)
		opConstructor := func(sources []colexecop.Operator) (colexecop.Operator, error) {
			return NewNotExprSelOp(types.BoolFamily, sources[0], 0)
		}
		colexectestutils.RunTestsWithoutAllNullsInjection(t, testAllocator, []colexectestutils.Tuples{c.inputTuples}, [][]*types.T{{types.Bool}}, c.outputTuples, colexectestutils.OrderedVerifier, opConstructor)
	}
}

func TestNotNullProjOp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	testCases := []struct {
		desc         string
		inputTuples  colexectestutils.Tuples
		outputTuples colexectestutils.Tuples
		projExpr     string
	}{
		{
			desc:         "SELECT NOT c FROM t -- NOT expr with only NULL",
			inputTuples:  colexectestutils.Tuples{{nil}, {nil}, {nil}, {nil}},
			outputTuples: colexectestutils.Tuples{{nil, nil}, {nil, nil}, {nil, nil}, {nil, nil}},
			projExpr:     "NOT",
		},
	}

	for _, c := range testCases {
		log.Infof(ctx, "%s", c.desc)
		opConstructor := func(input []colexecop.Operator) (colexecop.Operator, error) {
			return newNotNullProjOp(testAllocator, input[0], 0, 1), nil
		}
		colexectestutils.RunTestsWithoutAllNullsInjection(t, testAllocator, []colexectestutils.Tuples{c.inputTuples}, [][]*types.T{{types.Bool}}, c.outputTuples, colexectestutils.OrderedVerifier, opConstructor)
	}
}
