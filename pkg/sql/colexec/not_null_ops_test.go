// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestNotNullProjOp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
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
			return NewNotNullProjOp(testAllocator, input[0], 0, 1), nil
		}
		colexectestutils.RunTestsWithoutAllNullsInjection(t, testAllocator, []colexectestutils.Tuples{c.inputTuples}, [][]*types.T{{types.Bool}}, c.outputTuples, colexectestutils.OrderedVerifier, opConstructor)
	}
}
