// Copyright 2019 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/execbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// applyJoinNode implements apply join: the execution component of correlated
// subqueries. Note that only correlated subqueries that the optimizer's
// tranformations couldn't decorrelate get planned using apply joins.
// The node reads rows from the left planDataSource, and for each
// row, re-plans the right side of the join after replacing its outer columns
// with the corresponding values from the current row on the left. The new right
// plan is then executed and joined with the left row according to normal join
// semantics. This node doesn't support right or full outer joins, or set
// operations.
type applyJoinNode struct {
	joinType sqlbase.JoinType

	optimizer xform.Optimizer

	// The memo for the right side of the join - the one with the outer columns.
	rightMemo *memo.Memo

	// The data source with no outer columns.
	input planDataSource

	// leftBoundColMap maps an outer opt column id, bound by this apply join, to
	// the position in the left plan's row that contains the binding.
	leftBoundColMap opt.ColMap

	// pred represents the join predicate.
	pred *joinPredicate

	// columns contains the metadata for the results of this node.
	columns sqlbase.ResultColumns

	// rightCols contains the metadata for the result of the right side of this
	// apply join, as built in the optimization phase. Later on, every re-planning
	// of the right side will emit these same columns.
	rightCols sqlbase.ResultColumns

	// rightProps are the required physical properties that the optimizer has
	// imposed for the right side of this apply join, specifically containing
	// the required presentation (order of output columns) for the right side
	// of the join. Since the optimizer will re-plan the right expression every
	// time a new left row is seen, it's possible that some values of the left row
	// would cause a right plan that was different from that in `rightCols` above.
	// This would cause issues, so we pin the right side's output columns with
	// these properties.
	rightProps *physical.Required

	// right is the optimizer expression that represents the right side of the
	// join. It still contains outer columns and VariableExprs that we will be
	// replacing on every new left row, and as such isn't yet suitable for
	// direct use.
	right memo.RelExpr

	run struct {
		// emptyRight is a cached, all-NULL slice that's used for left outer joins
		// in the case of finding no match on the left.
		emptyRight tree.Datums
		// leftRow is the current left row being processed.
		leftRow tree.Datums
		// leftRowFoundAMatch is set to true when a left row found any match at all,
		// so that left outer joins and antijoins can know to output a row.
		leftRowFoundAMatch bool
		// rightRows will be populated with the result of the right side of the join
		// each time it's run.
		rightRows *rowcontainer.RowContainer
		// curRightRow is the index into rightRows of the current right row being
		// processed.
		curRightRow int
		// out is the full result row, populated on each call to Next.
		out tree.Datums
		// done is true if the left side has been exhausted.
		done bool
	}
}

// Set to true to enable ultra verbose debug logging.
func newApplyJoinNode(
	joinType sqlbase.JoinType,
	left planDataSource,
	leftBoundColMap opt.ColMap,
	rightProps *physical.Required,
	rightCols sqlbase.ResultColumns,
	right memo.RelExpr,
	pred *joinPredicate,
	memo *memo.Memo,
) (planNode, error) {
	switch joinType {
	case sqlbase.JoinType_RIGHT_OUTER, sqlbase.JoinType_FULL_OUTER:
		return nil, errors.AssertionFailedf("unsupported right outer apply join: %d", log.Safe(joinType))
	case sqlbase.JoinType_EXCEPT_ALL, sqlbase.JoinType_INTERSECT_ALL:
		return nil, errors.AssertionFailedf("unsupported apply set op: %d", log.Safe(joinType))
	}

	return &applyJoinNode{
		joinType:        joinType,
		input:           left,
		leftBoundColMap: leftBoundColMap,
		pred:            pred,
		rightMemo:       memo,
		rightProps:      rightProps,
		rightCols:       rightCols,
		right:           right,
		columns:         pred.cols,
	}, nil
}

func (a *applyJoinNode) startExec(params runParams) error {
	// If needed, pre-allocate a left row of NULL tuples for when the
	// join predicate fails to match.
	if a.joinType == sqlbase.LeftOuterJoin {
		a.run.emptyRight = make(tree.Datums, a.right.Relational().OutputCols.Len())
		for i := range a.run.emptyRight {
			a.run.emptyRight[i] = tree.DNull
		}
	}
	a.run.out = make(tree.Datums, len(a.columns))
	ci := sqlbase.ColTypeInfoFromResCols(a.rightCols)
	acc := params.EvalContext().Mon.MakeBoundAccount()
	a.run.rightRows = rowcontainer.NewRowContainer(acc, ci, 0 /* rowCapacity */)
	return nil
}

func (a *applyJoinNode) Next(params runParams) (bool, error) {
	if a.run.done {
		return false, nil
	}

	for {
		for a.run.curRightRow < a.run.rightRows.Len() {
			// We have right rows set up - check the next one for a match.
			var rrow tree.Datums
			if len(a.rightCols) != 0 {
				rrow = a.run.rightRows.At(a.run.curRightRow)
			}
			a.run.curRightRow++
			// Compute join.
			predMatched, err := a.pred.eval(params.EvalContext(), a.run.leftRow, rrow)
			if err != nil {
				return false, err
			}
			if !predMatched {
				// Didn't match? Try with the next right-side row.
				continue
			}

			a.run.leftRowFoundAMatch = true
			if a.joinType == sqlbase.JoinType_LEFT_ANTI ||
				a.joinType == sqlbase.JoinType_LEFT_SEMI {
				// We found a match, but we're doing an anti or semi join, so we're
				// done with this left row.
				break
			}
			// We're doing an ordinary join, so prep the row and emit it.
			a.pred.prepareRow(a.run.out, a.run.leftRow, rrow)
			return true, nil
		}
		// We're out of right side rows. Clear them, and reset the match state for
		// next time.
		a.run.rightRows.Clear(params.ctx)
		foundAMatch := a.run.leftRowFoundAMatch
		a.run.leftRowFoundAMatch = false

		if a.run.leftRow != nil {
			// If we have a left row already, we have to check to see if we need to
			// emit rows for semi, outer, or anti joins.
			if foundAMatch {
				if a.joinType == sqlbase.JoinType_LEFT_SEMI {
					// We found a match, and we're doing an semi-join, so we're done
					// with this left row after we output it.
					a.pred.prepareRow(a.run.out, a.run.leftRow, nil)
					a.run.leftRow = nil
					return true, nil
				}
			} else {
				// We found no match. Output LEFT OUTER or ANTI match if necessary.
				switch a.joinType {
				case sqlbase.JoinType_LEFT_OUTER:
					a.pred.prepareRow(a.run.out, a.run.leftRow, a.run.emptyRight)
					a.run.leftRow = nil
					return true, nil
				case sqlbase.JoinType_LEFT_ANTI:
					a.pred.prepareRow(a.run.out, a.run.leftRow, nil)
					a.run.leftRow = nil
					return true, nil
				}
			}
		}

		// We need a new row on the left.
		ok, err := a.input.plan.Next(params)
		if err != nil {
			return false, err
		}
		if !ok {
			// No more rows on the left. Goodbye!
			a.run.done = true
			return false, nil
		}

		// Extract the values of the outer columns of the other side of the apply
		// from the latest input row.
		row := a.input.plan.Values()
		a.run.leftRow = row

		// At this point, it's time to do the major lift of apply join: re-planning
		// the right side of the join using the optimizer, with all outer columns
		// in the right side replaced by the bindings that were defined by the most
		// recently read left row.
		//
		// We have a cached optimizer here that we'll use to re-plan that right
		// side. Now, we'll essentially go through an entire recursive planning and
		// execution phase. Initialize the optimizer, replace the outer columns,
		// run the optimizer on the replaced right side, transform the optimized
		// expression into logical planNodes, perform physical planning on the
		// logical planNodes, and finally(!) run the physical plan. That will
		// produce n new rows from the right side of the join, which we'll join to
		// the current left row using the ordinary join semantics defined by the
		// type of join we've been instructed to do (inner, left outer, semi, or
		// anti).

		a.optimizer.Init(params.p.EvalContext(), &params.p.optPlanningCtx.catalog)

		bindings := make(map[opt.ColumnID]tree.Datum, a.leftBoundColMap.Len())
		a.leftBoundColMap.ForEach(func(k, v int) {
			bindings[opt.ColumnID(k)] = row[v]
		})

		// Replace the outer VariableExprs that this applyJoin node is responsible
		// for with the constant values in the latest left row.
		factory := a.optimizer.Factory()
		execbuilder.ReplaceVars(factory, a.right, a.rightProps, bindings)

		newRightSide, err := a.optimizer.Optimize()
		if err != nil {
			return false, err
		}

		execFactory := makeExecFactory(params.p)
		eb := execbuilder.New(&execFactory, factory.Memo(), nil /* catalog */, newRightSide, params.EvalContext())
		eb.DisableTelemetry()
		p, err := eb.Build()
		if err != nil {
			if errors.IsAssertionFailure(err) {
				// Enhance the error with the EXPLAIN (OPT, VERBOSE) of the inner
				// expression.
				fmtFlags := memo.ExprFmtHideQualifications | memo.ExprFmtHideScalars | memo.ExprFmtHideTypes
				explainOpt := memo.FormatExpr(newRightSide, fmtFlags, nil /* catalog */)
				err = errors.WithDetailf(err, "newRightSide:\n%s", explainOpt)
			}
			return false, err
		}
		plan := p.(*planTop)

		if err := a.runRightSidePlan(params, plan); err != nil {
			return false, err
		}

		// We've got fresh right rows. Continue along in the loop, which will deal
		// with joining the right plan's output with our left row.
	}
}

// runRightSidePlan runs a planTop that's been generated based on the
// re-optimized right hand side of the apply join, stashing the result in
// a.run.rightRows, ready for retrieval. An error indicates that something went
// wrong during execution of the right hand side of the join, and that we should
// completely give up on the outer join.
func (a *applyJoinNode) runRightSidePlan(params runParams, plan *planTop) error {
	a.run.curRightRow = 0
	a.run.rightRows.Clear(params.ctx)
	return runPlanInsidePlan(params, plan, a.run.rightRows)
}

// runPlanInsidePlan is used to run a plan and gather the results in a row
// container, as part of the execution of an "outer" plan.
func runPlanInsidePlan(
	params runParams, plan *planTop, rowContainer *rowcontainer.RowContainer,
) error {
	rowResultWriter := NewRowResultWriter(rowContainer)
	recv := MakeDistSQLReceiver(
		params.ctx, rowResultWriter, tree.Rows,
		params.extendedEvalCtx.ExecCfg.RangeDescriptorCache,
		params.extendedEvalCtx.ExecCfg.LeaseHolderCache,
		params.p.Txn(),
		func(ts hlc.Timestamp) {
			_ = params.extendedEvalCtx.ExecCfg.Clock.Update(ts)
		},
		params.p.extendedEvalCtx.Tracing,
	)
	defer recv.Release()

	if !params.p.extendedEvalCtx.ExecCfg.DistSQLPlanner.PlanAndRunSubqueries(
		params.ctx,
		params.p,
		params.extendedEvalCtx.copy,
		plan.subqueryPlans,
		recv,
		true,
	) {
		if err := rowResultWriter.Err(); err != nil {
			return err
		}
		return recv.commErr
	}

	// Make a copy of the EvalContext so it can be safely modified.
	evalCtx := params.p.ExtendedEvalContextCopy()
	planCtx := params.p.extendedEvalCtx.ExecCfg.DistSQLPlanner.newLocalPlanningCtx(params.ctx, evalCtx)
	// Always plan local.
	planCtx.isLocal = true
	plannerCopy := *params.p
	planCtx.planner = &plannerCopy
	planCtx.planner.curPlan = *plan
	planCtx.ExtendedEvalCtx.Planner = &plannerCopy
	planCtx.stmtType = recv.stmtType

	params.p.extendedEvalCtx.ExecCfg.DistSQLPlanner.PlanAndRun(
		params.ctx, evalCtx, planCtx, params.p.Txn(), plan.plan, recv,
	)()
	if recv.commErr != nil {
		return recv.commErr
	}
	return rowResultWriter.err
}

func (a *applyJoinNode) Values() tree.Datums {
	return a.run.out
}

func (a *applyJoinNode) Close(ctx context.Context) {
	a.input.plan.Close(ctx)
	if a.run.rightRows != nil {
		a.run.rightRows.Close(ctx)
	}
}
