// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execagg"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func createWindowFnSpec(
	ctx context.Context,
	planCtx *PlanningCtx,
	plan *PhysicalPlan,
	funcInProgress *windowFuncHolder,
	ordCols []execinfrapb.Ordering_Column,
) (execinfrapb.WindowerSpec_WindowFn, *types.T, error) {
	for _, argIdx := range funcInProgress.argsIdxs {
		if argIdx >= uint32(len(plan.GetResultTypes())) {
			return execinfrapb.WindowerSpec_WindowFn{}, nil, errors.Errorf("ColIdx out of range (%d)", argIdx)
		}
	}
	// Figure out which built-in to compute.
	funcSpec, err := rowexec.CreateWindowerSpecFunc(funcInProgress.expr.Func.String())
	if err != nil {
		return execinfrapb.WindowerSpec_WindowFn{}, nil, err
	}
	argTypes := make([]*types.T, len(funcInProgress.argsIdxs))
	for i, argIdx := range funcInProgress.argsIdxs {
		argTypes[i] = plan.GetResultTypes()[argIdx]
	}
	_, outputType, err := execagg.GetWindowFunctionInfo(funcSpec, argTypes...)
	if err != nil {
		return execinfrapb.WindowerSpec_WindowFn{}, outputType, err
	}
	funcInProgressSpec := execinfrapb.WindowerSpec_WindowFn{
		Func:         funcSpec,
		ArgsIdxs:     funcInProgress.argsIdxs,
		Ordering:     execinfrapb.Ordering{Columns: ordCols},
		FilterColIdx: int32(funcInProgress.filterColIdx),
		OutputColIdx: uint32(funcInProgress.outputColIdx),
	}
	if funcInProgress.frame != nil {
		// funcInProgress has a custom window frame.
		frameSpec := execinfrapb.WindowerSpec_Frame{}
		if err := frameSpec.InitFromAST(ctx, funcInProgress.frame, planCtx.EvalContext()); err != nil {
			return execinfrapb.WindowerSpec_WindowFn{}, outputType, err
		}
		funcInProgressSpec.Frame = &frameSpec
	}

	return funcInProgressSpec, outputType, nil
}
