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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// EvalRoutineExpr returns the result of evaluating the routine. It calls the
// routine's ForEachPlan closure to generate a plan for each statement in the
// routine, then runs the plans. The resulting value of the last statement in
// the routine is returned.
func (p *planner) EvalRoutineExpr(
	ctx context.Context, expr *tree.RoutineExpr, args tree.Datums,
) (result tree.Datum, err error) {
	// Return the cached result if it exists.
	if expr.CachedResult != nil {
		return expr.CachedResult, nil
	}

	var g routineGenerator
	g.init(ctx, p, expr, args)
	defer g.Close(ctx)
	err = g.Start(ctx, p.Txn())
	if err != nil {
		return nil, err
	}

	hasNext, err := g.Next(ctx)
	if err != nil {
		return nil, err
	}

	var res tree.Datum
	if !hasNext {
		// The result is NULL if no rows were returned by the last statement in
		// the routine.
		res = tree.DNull
	} else {
		// The result is the first and only column in the row returned by the
		// last statement in the routine.
		row, err := g.Values()
		if err != nil {
			return nil, err
		}
		res = row[0]
	}
	if len(expr.Args) == 0 && !expr.EnableStepping {
		// Cache the result if there are zero arguments and stepping is
		// disabled.
		expr.CachedResult = res
	}
	return res, nil
}

// routineGenerator is an eval.ValueGenerator that produces the result of a
// routine.
type routineGenerator struct {
	p        *planner
	expr     *tree.RoutineExpr
	args     tree.Datums
	rch      rowContainerHelper
	rci      *rowContainerIterator
	currVals tree.Datums
}

var _ eval.ValueGenerator = &routineGenerator{}

// init initializes a routineGenerator.
func (g *routineGenerator) init(
	ctx context.Context, p *planner, expr *tree.RoutineExpr, args tree.Datums,
) {
	*g = routineGenerator{
		p:    p,
		expr: expr,
		args: args,
	}
	retTypes := []*types.T{expr.ResolvedType()}
	g.rch.Init(ctx, retTypes, p.ExtendedEvalContext(), "routine" /* opName */)
	g.rci = newRowContainerIterator(ctx, g.rch, retTypes)
}

// ResolvedType is part of the ValueGenerator interface.
func (g *routineGenerator) ResolvedType() *types.T {
	return g.expr.ResolvedType()
}

// Start is part of the ValueGenerator interface.
func (g *routineGenerator) Start(ctx context.Context, txn *kv.Txn) (err error) {
	// Configure stepping for volatile routines so that mutations made by the
	// invoking statement are visible to the routine.
	if g.expr.EnableStepping {
		prevSteppingMode := txn.ConfigureStepping(ctx, kv.SteppingEnabled)
		prevSeqNum := txn.GetLeafTxnInputState(ctx).ReadSeqNum
		defer func() {
			// If the routine errored, the transaction should be aborted, so
			// there is no need to reconfigure stepping or revert to the
			// original sequence number.
			if err == nil {
				_ = txn.ConfigureStepping(ctx, prevSteppingMode)
				err = txn.SetReadSeqNum(prevSeqNum)
			}
		}()
	}

	// Execute each statement in the routine sequentially.
	stmtIdx := 0
	ef := newExecFactory(ctx, g.p)
	rrw := NewRowResultWriter(&g.rch)
	return g.expr.ForEachPlan(ctx, ef, g.args, func(plan tree.RoutinePlan, isFinalPlan bool) error {
		stmtIdx++
		opName := "udf-stmt-" + g.expr.Name + "-" + strconv.Itoa(stmtIdx)
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
		if g.expr.EnableStepping {
			if err := txn.Step(ctx); err != nil {
				return err
			}
		}

		// Run the plan.
		err = runPlanInsidePlan(ctx, g.p.RunParams(ctx), plan.(*planComponents), w)
		if err != nil {
			return err
		}

		return nil
	})
}

// Next is part of the ValueGenerator interface.
func (g *routineGenerator) Next(ctx context.Context) (bool, error) {
	var err error
	g.currVals, err = g.rci.Next()
	if err != nil {
		return false, err
	}
	return g.currVals != nil, nil
}

// Values is part of the ValueGenerator interface.
func (g *routineGenerator) Values() (tree.Datums, error) {
	return g.currVals, nil
}

// Close is part of the ValueGenerator interface.
func (g *routineGenerator) Close(ctx context.Context) {
	if g.rci != nil {
		g.rci.Close()
		g.rci = nil
	}
	g.rch.Close(ctx)
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
