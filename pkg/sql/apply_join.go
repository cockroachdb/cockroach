// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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
	singleInputPlanNode

	// pred represents the join predicate.
	pred *joinPredicate

	// columns contains the metadata for the results of this node.
	columns colinfo.ResultColumns

	// rightTypes is the schema of the rows produced by the right side of the
	// join, as built in the optimization phase. Later on, every re-planning of
	// the right side will emit these same columns.
	rightTypes []*types.T

	planRightSideFn exec.ApplyJoinPlanRightSideFn
	// iterationCount tracks the number of times planRightSideFn has been
	// invoked (in other words, the number of left rows for which we have
	// performed the apply join so far).
	iterationCount int

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
	left planNode,
	rightCols colinfo.ResultColumns,
	pred *joinPredicate,
	planRightSideFn exec.ApplyJoinPlanRightSideFn,
) (planNode, error) {
	switch joinType {
	case descpb.RightOuterJoin, descpb.FullOuterJoin:
		return nil, errors.AssertionFailedf("unsupported right outer apply join: %d", redact.Safe(joinType))
	case descpb.ExceptAllJoin, descpb.IntersectAllJoin:
		return nil, errors.AssertionFailedf("unsupported apply set op: %d", redact.Safe(joinType))
	case descpb.RightSemiJoin, descpb.RightAntiJoin:
		return nil, errors.AssertionFailedf("unsupported right semi/anti apply join: %d", redact.Safe(joinType))
	}

	return &applyJoinNode{
		joinType:            joinType,
		singleInputPlanNode: singleInputPlanNode{left},
		pred:                pred,
		rightTypes:          getTypesFromResultColumns(rightCols),
		planRightSideFn:     planRightSideFn,
		columns:             pred.cols,
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
	a.run.rightRows.Init(params.ctx, a.rightTypes, params.extendedEvalCtx, "apply-join" /* opName */)
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
				rrow, err := a.run.rightRowsIterator.Next()
				if err != nil {
					return false, err
				}
				if rrow == nil {
					// We have exhausted all rows from the right side.
					break
				}
				// Compute join.
				predMatched, err := a.pred.eval(params.ctx, params.EvalContext(), a.run.leftRow, rrow)
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
		ok, err := a.input.Next(params)
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
		leftRow := a.input.Values()
		a.run.leftRow = leftRow
		a.iterationCount++

		// At this point, it's time to do the major lift of apply join: re-planning
		// the right side of the join using the optimizer, with all outer columns
		// in the right side replaced by the bindings that were defined by the most
		// recently read left row.
		if err := a.runNextRightSideIteration(params, leftRow); err != nil {
			return false, err
		}

		// We've got fresh right rows. Continue along in the loop, which will deal
		// with joining the right plan's output with our left row.
	}
}

// clearRightRows clears rightRows and resets rightRowsIterator. This function
// must be called before reusing rightRows and rightRowsIterator.
func (a *applyJoinNode) clearRightRows(params runParams) error {
	if err := a.run.rightRows.Clear(params.ctx); err != nil {
		return err
	}
	a.run.rightRowsIterator.Close()
	a.run.rightRowsIterator = nil
	return nil
}

// runNextRightSideIteration generates a planTop based on the re-optimized right
// hand side of the apply join given the next left row and runs the plan to
// completion, stashing the result in a.run.rightRows, ready for retrieval. An
// error indicates that something went wrong during execution of the right hand
// side of the join, and that we should completely give up on the outer join.
func (a *applyJoinNode) runNextRightSideIteration(params runParams, leftRow tree.Datums) error {
	opName := "apply-join-iteration-" + strconv.Itoa(a.iterationCount)
	ctx, sp := tracing.ChildSpan(params.ctx, opName)
	defer sp.Finish()
	p, err := a.planRightSideFn(ctx, newExecFactory(ctx, params.p), leftRow)
	if err != nil {
		return err
	}
	plan := p.(*planComponents)
	rowResultWriter := NewRowResultWriter(&a.run.rightRows)
	if err := runPlanInsidePlan(
		ctx, params, plan, rowResultWriter,
		nil /* deferredRoutineSender */, "", /* stmtForDistSQLDiagram */
	); err != nil {
		return err
	}
	a.run.rightRowsIterator = newRowContainerIterator(ctx, a.run.rightRows)
	return nil
}

// runPlanInsidePlan is used to run a plan and gather the results in the
// resultWriter, as part of the execution of an "outer" plan.
func runPlanInsidePlan(
	ctx context.Context,
	params runParams,
	plan *planComponents,
	resultWriter rowResultWriter,
	deferredRoutineSender eval.DeferredRoutineSender,
	stmtForDistSQLDiagram string,
) error {
	defer plan.close(ctx)
	execCfg := params.ExecCfg()
	recv := MakeDistSQLReceiver(
		ctx, resultWriter, tree.Rows,
		execCfg.RangeDescriptorCache,
		params.p.Txn(),
		execCfg.Clock,
		params.p.extendedEvalCtx.Tracing,
	)
	defer recv.Release()

	plannerCopy := *params.p
	plannerCopy.curPlan.planComponents = *plan
	// "Pausable portal" execution model is only applicable to the outer
	// statement since we actually need to execute all inner plans to completion
	// before we can produce any "outer" rows to be returned to the client, so
	// we make sure to unset pausablePortal field on the planner.
	plannerCopy.pausablePortal = nil

	// planner object embeds the extended eval context, so we will modify that
	// (which won't affect the outer planner's extended eval context), and we'll
	// use it as the golden version going forward.
	plannerCopy.extendedEvalCtx.Planner = &plannerCopy
	plannerCopy.extendedEvalCtx.StreamManagerFactory = &plannerCopy
	plannerCopy.extendedEvalCtx.RoutineSender = deferredRoutineSender
	evalCtxFactory := plannerCopy.ExtendedEvalContextCopy

	if len(plan.subqueryPlans) != 0 {
		// Create a separate memory account for the results of the subqueries.
		// Note that we intentionally defer the closure of the account until we
		// return from this method (after the main query is executed).
		subqueryResultMemAcc := params.p.Mon().MakeBoundAccount()
		defer subqueryResultMemAcc.Close(ctx)
		if !execCfg.DistSQLPlanner.PlanAndRunSubqueries(
			ctx,
			&plannerCopy,
			evalCtxFactory,
			plan.subqueryPlans,
			recv,
			&subqueryResultMemAcc,
			false, /* skipDistSQLDiagramGeneration */
			params.p.mustUseLeafTxn(),
		) {
			return resultWriter.Err()
		}
	}

	distributePlan, distSQLProhibitedErr := getPlanDistribution(
		ctx, plannerCopy.Descriptors().HasUncommittedTypes(),
		plannerCopy.SessionData(), plan.main, &plannerCopy.distSQLVisitor,
	)
	distributeType := DistributionType(LocalDistribution)
	if distributePlan.WillDistribute() {
		distributeType = FullDistribution
	}
	evalCtx := evalCtxFactory()
	planCtx := execCfg.DistSQLPlanner.NewPlanningCtx(ctx, evalCtx, &plannerCopy, plannerCopy.txn, distributeType)
	planCtx.distSQLProhibitedErr = distSQLProhibitedErr
	planCtx.stmtType = recv.stmtType
	planCtx.mustUseLeafTxn = params.p.mustUseLeafTxn()
	planCtx.stmtForDistSQLDiagram = stmtForDistSQLDiagram

	// Wrap PlanAndRun in a function call so that we clean up immediately.
	func() {
		finishedSetupFn, cleanup := getFinishedSetupFn(&plannerCopy)
		defer cleanup()
		execCfg.DistSQLPlanner.PlanAndRun(
			ctx, evalCtx, planCtx, plannerCopy.Txn(), plan.main, recv, finishedSetupFn,
		)
	}()

	// Check if there was an error interacting with the resultWriter.
	if recv.commErr != nil {
		return recv.commErr
	}
	if resultWriter.Err() != nil {
		return resultWriter.Err()
	}

	plannerCopy.autoCommit = false
	execCfg.DistSQLPlanner.PlanAndRunPostQueries(
		ctx,
		&plannerCopy,
		func(usedConcurrently bool) *extendedEvalContext {
			return evalCtxFactory()
		},
		&plannerCopy.curPlan.planComponents,
		recv,
	)
	// We might have appended some cascades or checks to the plannerCopy, so we
	// need to update the plan for cleanup purposes before proceeding.
	*plan = plannerCopy.curPlan.planComponents
	if recv.commErr != nil {
		return recv.commErr
	}

	return resultWriter.Err()
}

func (a *applyJoinNode) Values() tree.Datums {
	return a.run.out
}

func (a *applyJoinNode) Close(ctx context.Context) {
	a.input.Close(ctx)
	a.run.rightRows.Close(ctx)
	if a.run.rightRowsIterator != nil {
		a.run.rightRowsIterator.Close()
		a.run.rightRowsIterator = nil
	}
}
