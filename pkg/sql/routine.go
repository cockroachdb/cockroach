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
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// EvalRoutineExpr returns the result of evaluating the routine. It calls the
// routine's PlanFn to generate a plan for each statement in the routine, then
// runs the plans. The resulting value of the last statement in the routine is
// returned.
func (p *planner) EvalRoutineExpr(
	ctx context.Context, expr *tree.RoutineExpr, args tree.Datums,
) (result tree.Datum, err error) {
	// Return the cached result if it exists.
	if expr.CachedResult != nil {
		return expr.CachedResult, nil
	}

	// The result of the routine is the result of the last statement. The result
	// of any preceding statements is ignored. We set up a rowResultWriter that
	// can store the results of the final statement here.
	var rch rowContainerHelper
	retTypes := []*types.T{expr.ResolvedType()}
	rch.Init(ctx, retTypes, p.ExtendedEvalContext(), "routine" /* opName */)
	defer rch.Close(ctx)
	rrw := NewRowResultWriter(&rch)

	// Configure stepping for volatile routines so that mutations made by the
	// invoking statement are visible to the routine.
	txn := p.Txn()
	if expr.EnableStepping {
		prevSteppingMode := txn.ConfigureStepping(ctx, kv.SteppingEnabled)
		prevSeqNum := txn.GetLeafTxnInputState(ctx).ReadSeqNum
		defer func() {
			// If the routine errored, the transaction should be aborted, so
			// there is no need to reconfigure stepping or revert to the
			// original sequence number.
			if err == nil {
				_ = p.Txn().ConfigureStepping(ctx, prevSteppingMode)
				err = txn.SetReadSeqNum(prevSeqNum)
			}
		}()
	}

	// Execute each statement in the routine sequentially.
	stmtIdx := 0
	ef := newExecFactory(ctx, p)
	err = expr.ForEachPlan(ctx, ef, args, func(plan tree.RoutinePlan, isFinalPlan bool) error {
		stmtIdx++
		opName := "udf-stmt-" + expr.Name + "-" + strconv.Itoa(stmtIdx)
		ctx, sp := tracing.ChildSpan(ctx, opName)
		defer sp.Finish()

		// If this is the last statement, use the rowResultWriter created above.
		// Otherwise, use a rowResultWriter that drops all rows added to it.
		var w rowResultWriter
		if isFinalPlan {
			w = rrw
		} else {
			w = &droppingResultWriter{}
		}

		// Place a sequence point before each statement in the routine for
		// volatile functions.
		if expr.EnableStepping {
			if err := txn.Step(ctx); err != nil {
				return err
			}
		}

		// Run the plan.
		err = runPlanInsidePlan(ctx, p.RunParams(ctx), plan.(*planComponents), w)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	// Fetch the first row from the row container and return the first
	// datum.
	rightRowsIterator := newRowContainerIterator(ctx, rch)
	defer rightRowsIterator.Close()
	row, err := rightRowsIterator.Next()
	if err != nil {
		return nil, err
	}
	var res tree.Datum
	if row == nil {
		// The result is NULL if no rows were returned by the last statement.
		res = tree.DNull
	} else {
		// The result is the first and only column in the row returned by the
		// last statement.
		res = row[0]
	}
	if len(expr.Args) == 0 && !expr.EnableStepping {
		// Cache the result if there are zero arguments and stepping is
		// disabled.
		expr.CachedResult = res
	}
	return res, nil
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
