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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
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
	joinType descpb.JoinType

	// The data source with no outer columns.
	input planDataSource

	// pred represents the join predicate.
	pred *joinPredicate

	// columns contains the metadata for the results of this node.
	columns colinfo.ResultColumns

	// rightTypes is the schema of the rows produced by the right side of the
	// join, as built in the optimization phase. Later on, every re-planning of
	// the right side will emit these same columns.
	rightTypes []*types.T

	planRightSideFn exec.ApplyJoinPlanRightSideFn

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
		rightRows rowContainerHelper
		// rightRowsIterator, if non-nil, is the iterator into rightRows.
		rightRowsIterator *rowContainerIterator
		// out is the full result row, populated on each call to Next.
		out tree.Datums
		// done is true if the left side has been exhausted.
		done bool
	}
}

func newApplyJoinNode(
	joinType descpb.JoinType,
	left planDataSource,
	rightCols colinfo.ResultColumns,
	pred *joinPredicate,
	planRightSideFn exec.ApplyJoinPlanRightSideFn,
) (planNode, error) {
	switch joinType {
	case descpb.RightOuterJoin, descpb.FullOuterJoin:
		return nil, errors.AssertionFailedf("unsupported right outer apply join: %d", log.Safe(joinType))
	case descpb.ExceptAllJoin, descpb.IntersectAllJoin:
		return nil, errors.AssertionFailedf("unsupported apply set op: %d", log.Safe(joinType))
	case descpb.RightSemiJoin, descpb.RightAntiJoin:
		return nil, errors.AssertionFailedf("unsupported right semi/anti apply join: %d", log.Safe(joinType))
	}

	return &applyJoinNode{
		joinType:        joinType,
		input:           left,
		pred:            pred,
		rightTypes:      getTypesFromResultColumns(rightCols),
		planRightSideFn: planRightSideFn,
		columns:         pred.cols,
	}, nil
}

func (a *applyJoinNode) startExec(params runParams) error {
	// If needed, pre-allocate a right row of NULL tuples for when the
	// join predicate fails to match.
	if a.joinType == descpb.LeftOuterJoin {
		a.run.emptyRight = make(tree.Datums, len(a.rightTypes))
		for i := range a.run.emptyRight {
			a.run.emptyRight[i] = tree.DNull
		}
	}
	a.run.out = make(tree.Datums, len(a.columns))
	a.run.rightRows.init(a.rightTypes, params.extendedEvalCtx, "apply-join" /* opName */)
	return nil
}

func (a *applyJoinNode) Next(params runParams) (bool, error) {
	if a.run.done {
		return false, nil
	}

	for {
		if a.run.rightRowsIterator != nil {
			// We have right rows set up - check the next one for a match.
			for {
				// Note that if a.rightTypes has zero length, non-nil rrow is
				// returned the correct number of times.
				rrow, err := a.run.rightRowsIterator.next()
				if err != nil {
					return false, err
				}
				if rrow == nil {
					// We have exhausted all rows from the right side.
					break
				}
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
				if a.joinType == descpb.LeftAntiJoin ||
					a.joinType == descpb.LeftSemiJoin {
					// We found a match, but we're doing an anti or semi join,
					// so we're done with this left row.
					break
				}
				// We're doing an ordinary join, so prep the row and emit it.
				a.pred.prepareRow(a.run.out, a.run.leftRow, rrow)
				return true, nil
			}

			// We're either out of right side rows or we broke out of the loop
			// before consuming all right rows because we found a match for an
			// anti or semi join. Clear the right rows to prepare them for the
			// next left row.
			if err := a.clearRightRows(params); err != nil {
				return false, err
			}
		}
		// We're out of right side rows. Reset the match state for next time.
		foundAMatch := a.run.leftRowFoundAMatch
		a.run.leftRowFoundAMatch = false

		if a.run.leftRow != nil {
			// If we have a left row already, we have to check to see if we need to
			// emit rows for semi, outer, or anti joins.
			if foundAMatch {
				if a.joinType == descpb.LeftSemiJoin {
					// We found a match, and we're doing an semi-join, so we're done
					// with this left row after we output it.
					a.pred.prepareRow(a.run.out, a.run.leftRow, nil)
					a.run.leftRow = nil
					return true, nil
				}
			} else {
				// We found no match. Output LEFT OUTER or ANTI match if necessary.
				switch a.joinType {
				case descpb.LeftOuterJoin:
					a.pred.prepareRow(a.run.out, a.run.leftRow, a.run.emptyRight)
					a.run.leftRow = nil
					return true, nil
				case descpb.LeftAntiJoin:
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
		leftRow := a.input.plan.Values()
		a.run.leftRow = leftRow

		// At this point, it's time to do the major lift of apply join: re-planning
		// the right side of the join using the optimizer, with all outer columns
		// in the right side replaced by the bindings that were defined by the most
		// recently read left row.
		p, err := a.planRightSideFn(newExecFactory(params.p), leftRow)
		if err != nil {
			return false, err
		}
		plan := p.(*planComponents)

		if err := a.runRightSidePlan(params, plan); err != nil {
			return false, err
		}

		// We've got fresh right rows. Continue along in the loop, which will deal
		// with joining the right plan's output with our left row.
	}
}

// clearRightRows clears rightRows and resets rightRowsIterator. This function
// must be called before reusing rightRows and rightRowIterator.
func (a *applyJoinNode) clearRightRows(params runParams) error {
	if err := a.run.rightRows.clear(params.ctx); err != nil {
		return err
	}
	a.run.rightRowsIterator.close()
	a.run.rightRowsIterator = nil
	return nil
}

// runRightSidePlan runs a planTop that's been generated based on the
// re-optimized right hand side of the apply join, stashing the result in
// a.run.rightRows, ready for retrieval. An error indicates that something went
// wrong during execution of the right hand side of the join, and that we should
// completely give up on the outer join.
func (a *applyJoinNode) runRightSidePlan(params runParams, plan *planComponents) error {
	if err := runPlanInsidePlan(params, plan, &a.run.rightRows); err != nil {
		return err
	}
	a.run.rightRowsIterator = newRowContainerIterator(params.ctx, a.run.rightRows, a.rightTypes)
	return nil
}

// runPlanInsidePlan is used to run a plan and gather the results in a row
// container, as part of the execution of an "outer" plan.
func runPlanInsidePlan(
	params runParams, plan *planComponents, rowContainer *rowContainerHelper,
) error {
	rowResultWriter := NewRowResultWriter(rowContainer)
	recv := MakeDistSQLReceiver(
		params.ctx, rowResultWriter, tree.Rows,
		params.ExecCfg().RangeDescriptorCache,
		params.p.Txn(),
		params.ExecCfg().Clock,
		params.p.extendedEvalCtx.Tracing,
		params.p.ExecCfg().ContentionRegistry,
		nil, /* testingPushCallback */
	)
	defer recv.Release()

	if len(plan.subqueryPlans) != 0 {
		// We currently don't support cases when both the "inner" and the
		// "outer" plans have subqueries due to limitations of how we're
		// propagating the results of the subqueries.
		if len(params.p.curPlan.subqueryPlans) != 0 {
			return unimplemented.NewWithIssue(66447, `apply joins with subqueries in the "inner" and "outer" contexts are not supported`)
		}
		// Right now curPlan.subqueryPlans are the subqueries from the "outer"
		// plan (and we know there are none given the check above). If parts of
		// the "inner" plan refer to the subqueries, we know that they must
		// refer to the "inner" subqueries. To allow for that to happen we have
		// to manually replace the subqueries on the planner's curPlan and
		// restore the original state before exiting.
		oldSubqueries := params.p.curPlan.subqueryPlans
		params.p.curPlan.subqueryPlans = plan.subqueryPlans
		defer func() {
			params.p.curPlan.subqueryPlans = oldSubqueries
		}()
		// Create a separate memory account for the results of the subqueries.
		// Note that we intentionally defer the closure of the account until we
		// return from this method (after the main query is executed).
		subqueryResultMemAcc := params.p.EvalContext().Mon.MakeBoundAccount()
		defer subqueryResultMemAcc.Close(params.ctx)
		if !params.p.extendedEvalCtx.ExecCfg.DistSQLPlanner.PlanAndRunSubqueries(
			params.ctx,
			params.p,
			params.extendedEvalCtx.copy,
			plan.subqueryPlans,
			recv,
			&subqueryResultMemAcc,
		) {
			return rowResultWriter.Err()
		}
	}

	// Make a copy of the EvalContext so it can be safely modified.
	evalCtx := params.p.ExtendedEvalContextCopy()
	plannerCopy := *params.p
	distributePlan := getPlanDistribution(
		params.ctx, &plannerCopy, plannerCopy.execCfg.NodeID, plannerCopy.SessionData().DistSQLMode, plan.main,
	)
	planCtx := params.p.extendedEvalCtx.ExecCfg.DistSQLPlanner.NewPlanningCtx(
		params.ctx, evalCtx, &plannerCopy, params.p.txn, distributePlan.WillDistribute(),
	)
	planCtx.planner.curPlan.planComponents = *plan
	planCtx.ExtendedEvalCtx.Planner = &plannerCopy
	planCtx.stmtType = recv.stmtType

	params.p.extendedEvalCtx.ExecCfg.DistSQLPlanner.PlanAndRun(
		params.ctx, evalCtx, planCtx, params.p.Txn(), plan.main, recv,
	)()
	return rowResultWriter.Err()
}

func (a *applyJoinNode) Values() tree.Datums {
	return a.run.out
}

func (a *applyJoinNode) Close(ctx context.Context) {
	a.input.plan.Close(ctx)
	a.run.rightRows.close(ctx)
	if a.run.rightRowsIterator != nil {
		a.run.rightRowsIterator.close()
		a.run.rightRowsIterator = nil
	}
}
