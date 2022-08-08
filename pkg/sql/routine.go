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

// EvalRoutineExpr returns the result of evaluating the routine. It calls the
// routine's PlanFn to generate a plan for each statement in the routine, then
// runs the plans. The resulting value of the last statement in the routine is
// returned.
// TODO(mgartner): Support executing multi-statement routines.
func (p *planner) EvalRoutineExpr(
	ctx context.Context, expr *tree.RoutineExpr,
) (result tree.Datum, err error) {
	typs := []*types.T{expr.ResolvedType()}

	// The result of the routine is the result of the last statement. The result
	// of any preceding statements is ignored. We set up a rowResultWriter that
	// can store the results of the final statement here.
	var rch rowContainerHelper
	rch.Init(typs, p.ExtendedEvalContext(), "routine" /* opName */)
	defer rch.Close(ctx)
	rrw := NewRowResultWriter(&rch)

	// Execute each statement in the routine sequentially.
	for i := 0; i < expr.NumStmts; i++ {
		// Generate a plan for executing the ith statement.
		plan, err := expr.PlanFn(ctx, newExecFactory(p), i)
		if err != nil {
			return nil, err
		}

		// If this is the last statement, use the rowResultWriter created above.
		// Otherwise, use a rowResultWriter that drops all rows added to it.
		var w rowResultWriter
		if i == expr.NumStmts-1 {
			w = rrw
		} else {
			w = &droppingResultWriter{}
		}

		// TODO(mgartner): Add a new tracing.ChildSpan to the context for better
		// tracing of UDFs, like we do with apply-joins.
		err = runPlanInsidePlan(ctx, p.RunParams(ctx), plan.(*planComponents), w)
		if err != nil {
			return nil, err
		}
	}

	// Fetch the first row from the row container and return the first
	// datum.
	// TODO(mgartner): Consider adding an assertion error if more than one
	// row exists in the row container. This would require the optimizer to
	// automatically add LIMIT 1 expressions on the last statement in a
	// routine to avoid errors when a statement returns more than one row.
	// Adding the limit would be valid because any other rows after the
	// first can simply be ignored. The limit could also be beneficial
	// because it could allow additional query plan optimizations.
	rightRowsIterator := newRowContainerIterator(ctx, rch, typs)
	defer rightRowsIterator.Close()
	res, err := rightRowsIterator.Next()
	if err != nil {
		return nil, err
	}
	return res[0], nil
}

// droppingResultWriter drops all rows that are added to it. It only tracks
// errors with the SetError and Err functions.
type droppingResultWriter struct {
	err error
}

// AddRow is part of the rowResultWriter interface.
func (d *droppingResultWriter) AddRow(ctx context.Context, row tree.Datums) error {
	return nil
}

// IncrementRowsAffected is part of the rowResultWriter interface.
func (d *droppingResultWriter) IncrementRowsAffected(ctx context.Context, n int) {}

// SetError is part of the rowResultWriter interface.
func (d *droppingResultWriter) SetError(err error) {
	d.err = err
}

// Err is part of the rowResultWriter interface.
func (d *droppingResultWriter) Err() error {
	return d.err
}
