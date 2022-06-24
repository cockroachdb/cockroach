// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func (p *planner) EvalRoutine(
	ctx context.Context, expr *tree.Routine, args tree.RoutineArgs,
) (result tree.Datum, err error) {
	plan, err := expr.PlanFn(newExecFactory(p), args.Values)
	if err != nil {
		return nil, err
	}

	typs := []*types.T{expr.ResolvedType()}

	var rch rowContainerHelper
	rch.Init(typs, p.ExtendedEvalContext(), "udf" /* opName */)
	rowResultWriter := NewRowResultWriter(&rch)

	rightRowsIterator := newRowContainerIterator(ctx, rch, typs)

	if err := runPlanInsidePlan(p.RunParams(ctx), plan.(*planComponents), rowResultWriter); err != nil {
		return nil, err
	}

	res, err := rightRowsIterator.Next()
	if err != nil {
		return nil, err
	}

	return res[0], nil
}
