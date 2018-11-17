// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/execbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

type planMaker interface {
	// newPlan starts preparing the query plan for a single SQL
	// statement.
	//
	// It performs as many early checks as possible on the structure of
	// the SQL statement, including verifying permissions and type
	// checking.  The returned plan object is not ready to execute; the
	// optimizePlan() method must be called first. See makePlan()
	// below.
	//
	// This method should not be used directly; instead prefer makePlan()
	// or prepare() below.
	newPlan(
		ctx context.Context, stmt tree.Statement, desiredTypes []types.T,
	) (planNode, error)

	// makePlan prepares the query plan for a single SQL statement.  it
	// calls newPlan() then optimizePlan() on the result.
	// The logical plan is stored in the planner's curPlan field.
	//
	// Execution must start by calling curPlan.start() first and then
	// iterating using curPlan.plan.Next() and curPlan.plan.Values() in
	// order to retrieve matching rows. Finally, the plan must be closed
	// with curPlan.close().
	makePlan(ctx context.Context, stmt Statement) error

	// prepare does the same checks as makePlan but skips building some
	// data structures necessary for execution, based on the assumption
	// that the plan will never be run. A planNode built with prepare()
	// will do just enough work to check the structural validity of the
	// SQL statement and determine types for placeholders. However it is
	// not appropriate to call optimizePlan(), Next() or Values() on a plan
	// object created with prepare().
	// The plan should still be closed with p.curPlan.close() though.
	prepare(ctx context.Context, stmt tree.Statement) error
}

var _ planMaker = &planner{}

// runParams is a struct containing all parameters passed to planNode.Next() and
// startPlan.
type runParams struct {
	// context.Context for this method call.
	ctx context.Context

	// extendedEvalCtx groups fields useful for this execution.
	// Used during local execution and distsql physical planning.
	extendedEvalCtx *extendedEvalContext

	// planner associated with this execution. Only used during local
	// execution.
	p *planner
}

// EvalContext() gives convenient access to the runParam's EvalContext().
func (r *runParams) EvalContext() *tree.EvalContext {
	return &r.extendedEvalCtx.EvalContext
}

// SessionData gives convenient access to the runParam's SessionData.
func (r *runParams) SessionData() *sessiondata.SessionData {
	return r.extendedEvalCtx.SessionData
}

// planNode defines the interface for executing a query or portion of a query.
//
// The following methods apply to planNodes and contain special cases
// for each type; they thus need to be extended when adding/removing
// planNode instances:
// - planMaker.newPlan()
// - planMaker.doPrepare()
// - planMaker.setNeededColumns()  (needed_columns.go)
// - planMaker.expandPlan()        (expand_plan.go)
// - planVisitor.visit()           (walk.go)
// - planNodeNames                 (walk.go)
// - planMaker.optimizeFilters()   (filter_opt.go)
// - setLimitHint()                (limit_hint.go)
// - collectSpans()                (plan_spans.go)
// - planOrdering()                (plan_ordering.go)
// - planColumns()                 (plan_columns.go)
//
// Also, there are optional interfaces that new nodes may want to implement:
// - execStartable
// - autoCommitNode
//
type planNode interface {
	// Next performs one unit of work, returning false if an error is
	// encountered or if there is no more work to do. For statements
	// that return a result set, the Values() method will return one row
	// of results each time that Next() returns true.
	// See executor.go: forEachRow() for an example.
	//
	// Available after startPlan(). It is illegal to call Next() after it returns
	// false. It is legal to call Next() even if the node implements
	// planNodeFastPath and the FastPathResults() method returns true.
	Next(params runParams) (bool, error)

	// Values returns the values at the current row. The result is only valid
	// until the next call to Next().
	//
	// Available after Next().
	Values() tree.Datums

	// Close terminates the planNode execution and releases its resources.
	// This method should be called if the node has been used in any way (any
	// methods on it have been called) after it was constructed. Note that this
	// doesn't imply that startPlan() has been necessarily called.
	Close(ctx context.Context)
}

// PlanNode is the exported name for planNode. Useful for CCL hooks.
type PlanNode = planNode

// planNodeFastPath is implemented by nodes that can perform all their
// work during startPlan(), possibly affecting even multiple rows. For
// example, DELETE can do this.
type planNodeFastPath interface {
	// FastPathResults returns the affected row count and true if the
	// node has no result set and has already executed when startPlan() completes.
	// Note that Next() must still be valid even if this method returns
	// true, although it may have nothing left to do.
	FastPathResults() (int, bool)
}

var _ planNode = &alterIndexNode{}
var _ planNode = &alterSequenceNode{}
var _ planNode = &alterTableNode{}
var _ planNode = &createDatabaseNode{}
var _ planNode = &createIndexNode{}
var _ planNode = &createSequenceNode{}
var _ planNode = &createStatsNode{}
var _ planNode = &createTableNode{}
var _ planNode = &CreateUserNode{}
var _ planNode = &createViewNode{}
var _ planNode = &delayedNode{}
var _ planNode = &deleteNode{}
var _ planNode = &distinctNode{}
var _ planNode = &dropDatabaseNode{}
var _ planNode = &dropIndexNode{}
var _ planNode = &dropSequenceNode{}
var _ planNode = &dropTableNode{}
var _ planNode = &DropUserNode{}
var _ planNode = &dropViewNode{}
var _ planNode = &explainDistSQLNode{}
var _ planNode = &explainPlanNode{}
var _ planNode = &filterNode{}
var _ planNode = &groupNode{}
var _ planNode = &hookFnNode{}
var _ planNode = &indexJoinNode{}
var _ planNode = &insertNode{}
var _ planNode = &joinNode{}
var _ planNode = &limitNode{}
var _ planNode = &ordinalityNode{}
var _ planNode = &projectSetNode{}
var _ planNode = &relocateNode{}
var _ planNode = &renameColumnNode{}
var _ planNode = &renameDatabaseNode{}
var _ planNode = &renameIndexNode{}
var _ planNode = &renameTableNode{}
var _ planNode = &renderNode{}
var _ planNode = &rowCountNode{}
var _ planNode = &scanNode{}
var _ planNode = &scatterNode{}
var _ planNode = &serializeNode{}
var _ planNode = &showFingerprintsNode{}
var _ planNode = &showRangesNode{}
var _ planNode = &showTraceNode{}
var _ planNode = &sortNode{}
var _ planNode = &splitNode{}
var _ planNode = &truncateNode{}
var _ planNode = &unaryNode{}
var _ planNode = &unionNode{}
var _ planNode = &updateNode{}
var _ planNode = &upsertNode{}
var _ planNode = &valuesNode{}
var _ planNode = &virtualTableNode{}
var _ planNode = &windowNode{}
var _ planNode = &zeroNode{}

var _ planNodeFastPath = &CreateUserNode{}
var _ planNodeFastPath = &DropUserNode{}
var _ planNodeFastPath = &alterUserSetPasswordNode{}
var _ planNodeFastPath = &createTableNode{}
var _ planNodeFastPath = &deleteNode{}
var _ planNodeFastPath = &rowCountNode{}
var _ planNodeFastPath = &serializeNode{}
var _ planNodeFastPath = &setZoneConfigNode{}
var _ planNodeFastPath = &controlJobsNode{}

// planNodeRequireSpool serves as marker for nodes whose parent must
// ensure that the node is fully run to completion (and the results
// spooled) during the start phase. This is currently implemented by
// all mutation statements except for upsert.
type planNodeRequireSpool interface {
	requireSpool()
}

var _ planNodeRequireSpool = &serializeNode{}

// planNodeSpool serves as marker for nodes that can perform all their
// execution during the start phase. This is different from the "fast
// path" interface because a node that performs all its execution
// during the start phase might still have some result rows and thus
// not implement the fast path.
//
// This interface exists for the following optimization: nodes
// that require spooling but are the children of a spooled node
// do not require the introduction of an explicit spool.
type planNodeSpooled interface {
	spooled()
}

var _ planNodeSpooled = &spoolNode{}

// planTop is the struct that collects the properties
// of an entire plan.
// Note: some additional per-statement state is also stored in
// semaCtx (placeholders).
// TODO(jordan): investigate whether/how per-plan state like
// placeholder data can be concentrated in a single struct.
type planTop struct {
	// AST is the syntax tree for the current statement.
	AST tree.Statement

	// plan is the top-level node of the logical plan.
	plan planNode

	// deps, if non-nil, collects the table/view dependencies for this query.
	// Any planNode constructors that resolves a table name or reference in the query
	// to a descriptor must register this descriptor into planDeps.
	// This is (currently) used by CREATE VIEW.
	// TODO(knz): Remove this in favor of a better encapsulated mechanism.
	deps planDependencies

	// cteNameEnvironment collects the mapping from common table expression alias
	// to the planNodes that represent their source.
	cteNameEnvironment cteNameEnvironment

	// hasStar collects whether any star expansion has occurred during
	// logical plan construction. This is used by CREATE VIEW until
	// #10028 is addressed.
	hasStar bool

	// isCorrelated collects whether the query was found to be correlated.
	// Used to produce better error messages.
	isCorrelated bool

	// subqueryPlans contains all the sub-query plans.
	subqueryPlans []subquery

	// auditEvents becomes non-nil if any of the descriptors used by
	// current statement is causing an auditing event. See exec_log.go.
	auditEvents []auditEvent
}

// makePlan implements the Planner interface. It populates the
// planner's curPlan field.
//
// The caller is responsible for populating the placeholders
// beforehand (currently in semaCtx.Placeholders).
//
// After makePlan(), the caller should be careful to also call
// p.curPlan.Close().
func (p *planner) makePlan(ctx context.Context, stmt Statement) error {
	// Reinitialize.
	p.curPlan = planTop{AST: stmt.AST}

	log.VEvent(ctx, 1, "heuristic planner starts")

	var err error
	p.curPlan.plan, err = p.newPlan(ctx, stmt.AST, nil /*desiredTypes*/)
	if err != nil {
		return err
	}
	cols := planColumns(p.curPlan.plan)
	if stmt.ExpectedTypes != nil {
		if !stmt.ExpectedTypes.TypesEqual(cols) {
			return pgerror.NewError(pgerror.CodeFeatureNotSupportedError,
				"cached plan must not change result type")
		}
	}
	if err := p.semaCtx.Placeholders.AssertAllAssigned(); err != nil {
		// We need to close in case there were any subqueries created.
		p.curPlan.close(ctx)
		return err
	}

	// Ensure that any hidden result column is effectively hidden.
	// We do this before optimization below so that the needed
	// column optimization kills the hidden columns.
	p.curPlan.plan, err = p.hideHiddenColumns(ctx, p.curPlan.plan, cols)
	if err != nil {
		p.curPlan.close(ctx)
		return err
	}

	log.VEvent(ctx, 1, "heuristic planner optimizes plan")

	needed := allColumns(p.curPlan.plan)
	p.curPlan.plan, err = p.optimizePlan(ctx, p.curPlan.plan, needed)
	if err != nil {
		p.curPlan.close(ctx)
		return err
	}

	log.VEvent(ctx, 1, "heuristic planner optimizes subqueries")

	// Now do the same work for all sub-queries.
	for i := range p.curPlan.subqueryPlans {
		if err := p.optimizeSubquery(ctx, &p.curPlan.subqueryPlans[i]); err != nil {
			p.curPlan.close(ctx)
			return err
		}
	}

	if log.V(3) {
		log.Infof(ctx, "statement %s compiled to:\n%s", stmt,
			planToString(ctx, p.curPlan.plan, p.curPlan.subqueryPlans))
	}

	return nil
}

// makeOptimizerPlan is an alternative to makePlan which uses the cost-based
// optimizer.
func (p *planner) makeOptimizerPlan(ctx context.Context, stmt Statement) error {
	// Ensure that p.curPlan is populated in case an error occurs early,
	// so that maybeLogStatement in the error case does not find an empty AST.
	p.curPlan = planTop{AST: stmt.AST}

	// Start with fast check to see if top-level statement is supported.
	switch stmt.AST.(type) {
	case *tree.ParenSelect, *tree.Select, *tree.SelectClause,
		*tree.UnionClause, *tree.ValuesClause, *tree.Explain:

	default:
		return pgerror.Unimplemented("statement", fmt.Sprintf("unsupported statement: %T", stmt.AST))
	}

	var catalog optCatalog
	catalog.init(p.execCfg.TableStatsCache, p)

	p.optimizer.Init(p.EvalContext())
	f := p.optimizer.Factory()

	// If the statement includes a PreparedStatement, then separate planning into
	// two distinct phases:
	//
	//   PREPARE - Build the Memo (optbuild) and apply normalization rules to it.
	//             If the query contains placeholders, values are not assigned
	//             during this phase, as that only happens during the EXECUTE
	//             phase. If the query does not contain placeholders, then also
	//             apply exploration rules to the Memo so that there's even less
	//             to do during the EXECUTE phase.
	//
	//   EXECUTE - Before the query can be executed, first any placeholders must
	//             be assigned values. This can trigger additional normalization
	//             rules, such as with this example:
	//
	//               SELECT * FROM abc WHERE b = $1 - 5
	//
	//             Without folding the Sub expression, any index on the "b" column
	//             won't be found. This also means that after placeholders are
	//             assigned, exploration rules must be applied (vs. applying them
	//             during PREPARE when there are no placeholders).
	//
	//             Whether there were placeholders or not, after exploration the
	//             plan tree must be built (execbuild). This tree is set as the
	//             planner.curPlan, and the EXECUTE phase of planning is complete.
	//
	var prepStmt *PreparedStatement
	inPreparePhase := p.EvalContext().PrepareOnly
	if stmt.Prepared != nil {
		// Only use memo if it was actually prepared. It may not have been in case
		// of fallback to the heuristic planner.
		if inPreparePhase || stmt.Prepared.Memo != nil {
			prepStmt = stmt.Prepared
		}
	}

	// If this is the prepare phase, or if a prepared memo:
	//   1. doesn't yet exist, or
	//   2. it's been invalidated by schema or other changes
	//
	// Then entirely rebuild the memo from the AST.
	if inPreparePhase || prepStmt == nil || prepStmt.Memo.IsStale(ctx, p.EvalContext(), &catalog) {
		bld := optbuilder.New(ctx, &p.semaCtx, p.EvalContext(), &catalog, f, stmt.AST)
		bld.KeepPlaceholders = prepStmt != nil
		err := bld.Build()
		if err != nil {
			// isCorrelated is used in the fallback case to create a better error.
			p.curPlan.isCorrelated = bld.IsCorrelated
			return err
		}

		if prepStmt != nil {
			// If the memo doesn't have placeholders, then fully optimize it, since
			// it can be reused without further changes to build the execution tree.
			if !f.Memo().HasPlaceholders() {
				p.optimizer.Optimize()
			}

			// Detach the prepared memo from the factory and transfer its ownership
			// to the prepared statement. DetachMemo will re-initialize the optimizer
			// to an empty memo.
			prepStmt.Memo = p.optimizer.DetachMemo()
		}
	}

	// If in the PREPARE phase, construct a dummy plan that has correct output
	// columns. Only output columns and placeholder types are needed.
	if inPreparePhase {
		md := prepStmt.Memo.Metadata()
		physical := prepStmt.Memo.RootProps()
		resultCols := make(sqlbase.ResultColumns, len(physical.Presentation))
		for i, col := range physical.Presentation {
			resultCols[i].Name = col.Label
			resultCols[i].Typ = md.ColumnType(col.ID)
		}
		p.curPlan.plan = &zeroNode{columns: resultCols}
		return nil
	}

	// This is the EXECUTE phase, so finish optimization by assigning any
	// remaining placeholders and applying exploration rules.
	var execMemo *memo.Memo
	if prepStmt == nil {
		p.optimizer.Optimize()
		execMemo = f.Memo()
	} else {
		if prepStmt.Memo.HasPlaceholders() {
			// Reinitialize the optimizer and construct a new memo that is copied
			// from the prepared memo, but with placeholders assigned.
			if err := p.optimizer.Factory().AssignPlaceholders(prepStmt.Memo); err != nil {
				return err
			}
			p.optimizer.Optimize()
			execMemo = f.Memo()
		} else {
			execMemo = prepStmt.Memo
		}
	}

	// Build the plan tree and store it in planner.curPlan.
	root := execMemo.RootExpr()
	execFactory := makeExecFactory(p)
	plan, err := execbuilder.New(&execFactory, execMemo, root, p.EvalContext()).Build()
	if err != nil {
		return err
	}

	p.curPlan = *plan.(*planTop)
	// Since the assignment above just cleared the AST, we need to set it again.
	p.curPlan.AST = stmt.AST

	cols := planColumns(p.curPlan.plan)
	if stmt.ExpectedTypes != nil {
		if !stmt.ExpectedTypes.TypesEqual(cols) {
			return pgerror.NewError(pgerror.CodeFeatureNotSupportedError,
				"cached plan must not change result type")
		}
	}

	return nil
}

// hideHiddenColumn ensures that if the plan is returning some hidden
// column(s), it is wrapped into a renderNode which only renders the
// visible columns.
func (p *planner) hideHiddenColumns(
	ctx context.Context, plan planNode, cols sqlbase.ResultColumns,
) (planNode, error) {
	hasHidden := false
	for i := range cols {
		if cols[i].Hidden {
			hasHidden = true
			break
		}
	}
	if !hasHidden {
		// Nothing to do.
		return plan, nil
	}

	var tn tree.TableName
	newPlan, err := p.insertRender(ctx, plan, &tn)
	if err != nil {
		// Don't return a nil plan on error -- the caller must be able to
		// Close() it even if the replacement fails.
		return plan, err
	}

	return newPlan, nil
}

// close ensures that the plan's resources have been deallocated.
func (p *planTop) close(ctx context.Context) {
	if p.plan != nil {
		p.plan.Close(ctx)
		p.plan = nil
	}

	for i := range p.subqueryPlans {
		// Once a subquery plan has been evaluated, it already closes its
		// plan.
		if p.subqueryPlans[i].plan != nil {
			p.subqueryPlans[i].plan.Close(ctx)
			p.subqueryPlans[i].plan = nil
		}
	}
}

// start starts the plan.
func (p *planTop) start(params runParams) error {
	if err := p.evalSubqueries(params); err != nil {
		return err
	}
	return startPlan(params, p.plan)
}

// columns retrieves the plan's columns.
func (p *planTop) columns() sqlbase.ResultColumns {
	return planColumns(p.plan)
}

func (p *planTop) collectSpans(params runParams) (readSpans, writeSpans roachpb.Spans, err error) {
	readSpans, writeSpans, err = collectSpans(params, p.plan)
	if err != nil {
		return nil, nil, err
	}
	for i := range params.p.curPlan.subqueryPlans {
		reads, writes, err := collectSpans(params, params.p.curPlan.subqueryPlans[i].plan)
		if err != nil {
			return nil, nil, err
		}
		readSpans = append(readSpans, reads...)
		writeSpans = append(writeSpans, writes...)
	}
	return readSpans, writeSpans, nil
}

// startPlan starts the given plan and all its sub-query nodes.
func startPlan(params runParams, plan planNode) error {
	// Now start execution.
	if err := startExec(params, plan); err != nil {
		return err
	}

	// Finally, trigger limit propagation through the plan.  The actual
	// LIMIT values will have been evaluated by startExec().
	params.p.setUnlimited(plan)

	return nil
}

// execStartable is implemented by planNodes that have an initial
// execution step.
type execStartable interface {
	startExec(params runParams) error
}

// autoCommitNode is implemented by planNodes that might be able to commit the
// KV txn in which they operate. Some nodes might want to do this to take
// advantage of the 1PC optimization in case they're running as an implicit
// transaction.
// Only the top-level node in a plan is allowed to auto-commit. A node that
// choses to do so has to be cognizant of all its children: it needs to only
// auto-commit after all the children have finished performing KV operations
// and, more generally, after the plan is guaranteed to not produce any
// execution errors (in case of an error anywhere in the query, we do not want
// to commit the txn).
type autoCommitNode interface {
	// enableAutoCommit is called on the root planNode (if it implements this
	// interface).
	enableAutoCommit()
}

var _ autoCommitNode = &createTableNode{}
var _ autoCommitNode = &delayedNode{}
var _ autoCommitNode = &deleteNode{}
var _ autoCommitNode = &insertNode{}
var _ autoCommitNode = &updateNode{}
var _ autoCommitNode = &upsertNode{}

// startExec calls startExec() on each planNode that supports
// execStartable using a depth-first, post-order traversal.
// The subqueries, if any, are also started.
//
// Reminder: walkPlan() ensures that subqueries and sub-plans are
// started before startExec() is called.
func startExec(params runParams, plan planNode) error {
	o := planObserver{
		enterNode: func(ctx context.Context, _ string, p planNode) (bool, error) {
			switch p.(type) {
			case *explainPlanNode, *explainDistSQLNode:
				// Do not recurse: we're not starting the plan if we just show its structure with EXPLAIN.
				return false, nil
			case *showTraceNode:
				// showTrace needs to override the params struct, and does so in its startExec() method.
				return false, nil
			case *createStatsNode:
				return false, errors.Errorf("statistics can only be created via DistSQL")
			}
			return true, nil
		},
		leaveNode: func(_ string, n planNode) error {
			if s, ok := n.(execStartable); ok {
				return s.startExec(params)
			}
			return nil
		},
	}
	return walkPlan(params.ctx, plan, o)
}

func (p *planner) maybePlanHook(ctx context.Context, stmt tree.Statement) (planNode, error) {
	// TODO(dan): This iteration makes the plan dispatch no longer constant
	// time. We could fix that with a map of `reflect.Type` but including
	// reflection in such a primary codepath is unfortunate. Instead, the
	// upcoming IR work will provide unique numeric type tags, which will
	// elegantly solve this.
	for _, planHook := range planHooks {
		if fn, header, subplans, err := planHook(ctx, stmt, p); err != nil {
			return nil, err
		} else if fn != nil {
			return &hookFnNode{f: fn, header: header, subplans: subplans}, nil
		}
	}
	for _, planHook := range wrappedPlanHooks {
		if node, err := planHook(ctx, stmt, p); err != nil {
			return nil, err
		} else if node != nil {
			return node, err
		}
	}

	return nil, nil
}

// delegateQuery creates a plan for a given SQL query.
// In addition, the caller can specify an additional validation
// function (initialCheck) that will be ran and checked for errors
// during plan optimization. This is meant for checks that cannot be
// run during a SQL prepare operation.
func (p *planner) delegateQuery(
	ctx context.Context,
	name string,
	sql string,
	initialCheck func(ctx context.Context) error,
	desiredTypes []types.T,
) (planNode, error) {
	log.VEventf(ctx, 2, "delegated query: %q", sql)

	// Prepare the sub-plan.
	stmt, err := parser.ParseOne(sql)
	if err != nil {
		return nil, err
	}
	plan, err := p.newPlan(ctx, stmt, desiredTypes)
	if err != nil {
		return nil, err
	}

	if initialCheck == nil {
		return plan, nil
	}

	// To enable late calling into initialCheck, we use a delayedNode.
	return &delayedNode{
		name: name,

		// The columns attribute cannot be a straight-up reference to the sub-plan's
		// own columns, because they can be modified in-place by setNeededColumns().
		columns: append(sqlbase.ResultColumns(nil), planColumns(plan)...),

		// The delayed constructor's only responsibility is to call
		// initialCheck() - the plan is already constructed.
		constructor: func(ctx context.Context, _ *planner) (planNode, error) {
			if err := initialCheck(ctx); err != nil {
				return nil, err
			}
			return plan, nil
		},

		// Breaking with the common usage pattern of delayedNode, where
		// the plan attribute is initially nil (the constructor creates
		// it), here we prepopulate the field with the sub-plan created
		// above. We do this instead of simply returning the newly created
		// sub-plan in a constructor closure, to ensure the sub-plan is
		// properly Close()d if the delayedNode is discarded before its
		// constructor is called.
		plan: plan,
	}, nil
}

// newPlan constructs a planNode from a statement. This is used
// recursively by the various node constructors.
func (p *planner) newPlan(
	ctx context.Context, stmt tree.Statement, desiredTypes []types.T,
) (planNode, error) {
	tracing.AnnotateTrace()

	// This will set the system DB trigger for transactions containing
	// schema-modifying statements that have no effect, such as
	// `BEGIN; INSERT INTO ...; CREATE TABLE IF NOT EXISTS ...; COMMIT;`
	// where the table already exists. This will generate some false
	// refreshes, but that's expected to be quite rare in practice.
	canModifySchema := tree.CanModifySchema(stmt)
	if canModifySchema {
		if err := p.txn.SetSystemConfigTrigger(); err != nil {
			return nil, errors.Wrap(err,
				"schema change statement cannot follow a statement that has written in the same transaction")
		}
	}

	if p.EvalContext().TxnReadOnly {
		if canModifySchema || tree.CanWriteData(stmt) {
			return nil, pgerror.NewErrorf(pgerror.CodeReadOnlySQLTransactionError,
				"cannot execute %s in a read-only transaction", stmt.StatementTag())
		}
	}

	if plan, err := p.maybePlanHook(ctx, stmt); plan != nil || err != nil {
		return plan, err
	}

	switch n := stmt.(type) {
	case *tree.AlterIndex:
		return p.AlterIndex(ctx, n)
	case *tree.AlterTable:
		return p.AlterTable(ctx, n)
	case *tree.AlterSequence:
		return p.AlterSequence(ctx, n)
	case *tree.AlterUserSetPassword:
		return p.AlterUserSetPassword(ctx, n)
	case *tree.CancelQueries:
		return p.CancelQueries(ctx, n)
	case *tree.CancelSessions:
		return p.CancelSessions(ctx, n)
	case *tree.CommentOnTable:
		return p.CommentOnTable(ctx, n)
	case *tree.ControlJobs:
		return p.ControlJobs(ctx, n)
	case *tree.Scrub:
		return p.Scrub(ctx, n)
	case *tree.CreateDatabase:
		return p.CreateDatabase(ctx, n)
	case *tree.CreateIndex:
		return p.CreateIndex(ctx, n)
	case *tree.CreateTable:
		return p.CreateTable(ctx, n)
	case *tree.CreateUser:
		return p.CreateUser(ctx, n)
	case *tree.CreateView:
		return p.CreateView(ctx, n)
	case *tree.CreateSequence:
		return p.CreateSequence(ctx, n)
	case *tree.CreateStats:
		return p.CreateStatistics(ctx, n)
	case *tree.Deallocate:
		return p.Deallocate(ctx, n)
	case *tree.Delete:
		return p.Delete(ctx, n, desiredTypes)
	case *tree.Discard:
		return p.Discard(ctx, n)
	case *tree.DropDatabase:
		return p.DropDatabase(ctx, n)
	case *tree.DropIndex:
		return p.DropIndex(ctx, n)
	case *tree.DropTable:
		return p.DropTable(ctx, n)
	case *tree.DropView:
		return p.DropView(ctx, n)
	case *tree.DropSequence:
		return p.DropSequence(ctx, n)
	case *tree.DropUser:
		return p.DropUser(ctx, n)
	case *tree.Explain:
		return p.Explain(ctx, n)
	case *tree.Grant:
		return p.Grant(ctx, n)
	case *tree.Insert:
		return p.Insert(ctx, n, desiredTypes)
	case *tree.ParenSelect:
		return p.newPlan(ctx, n.Select, desiredTypes)
	case *tree.Relocate:
		return p.Relocate(ctx, n)
	case *tree.RenameColumn:
		return p.RenameColumn(ctx, n)
	case *tree.RenameDatabase:
		return p.RenameDatabase(ctx, n)
	case *tree.RenameIndex:
		return p.RenameIndex(ctx, n)
	case *tree.RenameTable:
		return p.RenameTable(ctx, n)
	case *tree.Revoke:
		return p.Revoke(ctx, n)
	case *tree.Scatter:
		return p.Scatter(ctx, n)
	case *tree.Select:
		return p.Select(ctx, n, desiredTypes)
	case *tree.SelectClause:
		return p.SelectClause(ctx, n, nil /* orderBy */, nil /* limit */, nil, /* with */
			desiredTypes, publicColumns)
	case *tree.SetClusterSetting:
		return p.SetClusterSetting(ctx, n)
	case *tree.SetZoneConfig:
		return p.SetZoneConfig(ctx, n)
	case *tree.SetVar:
		return p.SetVar(ctx, n)
	case *tree.SetTransaction:
		return p.SetTransaction(n)
	case *tree.SetSessionCharacteristics:
		return p.SetSessionCharacteristics(n)
	case *tree.ShowClusterSetting:
		return p.ShowClusterSetting(ctx, n)
	case *tree.ShowVar:
		return p.ShowVar(ctx, n)
	case *tree.ShowColumns:
		return p.ShowColumns(ctx, n)
	case *tree.ShowConstraints:
		return p.ShowConstraints(ctx, n)
	case *tree.ShowCreate:
		return p.ShowCreate(ctx, n)
	case *tree.ShowDatabases:
		return p.ShowDatabases(ctx, n)
	case *tree.ShowGrants:
		return p.ShowGrants(ctx, n)
	case *tree.ShowHistogram:
		return p.ShowHistogram(ctx, n)
	case *tree.ShowIndex:
		return p.ShowIndex(ctx, n)
	case *tree.ShowQueries:
		return p.ShowQueries(ctx, n)
	case *tree.ShowJobs:
		return p.ShowJobs(ctx, n)
	case *tree.ShowRoleGrants:
		return p.ShowRoleGrants(ctx, n)
	case *tree.ShowRoles:
		return p.ShowRoles(ctx, n)
	case *tree.ShowSessions:
		return p.ShowSessions(ctx, n)
	case *tree.ShowTableStats:
		return p.ShowTableStats(ctx, n)
	case *tree.ShowSyntax:
		return p.ShowSyntax(ctx, n)
	case *tree.ShowTables:
		return p.ShowTables(ctx, n)
	case *tree.ShowSchemas:
		return p.ShowSchemas(ctx, n)
	case *tree.ShowTraceForSession:
		return p.ShowTrace(ctx, n)
	case *tree.ShowTransactionStatus:
		return p.ShowTransactionStatus(ctx)
	case *tree.ShowUsers:
		return p.ShowUsers(ctx, n)
	case *tree.ShowZoneConfig:
		return p.ShowZoneConfig(ctx, n)
	case *tree.ShowRanges:
		return p.ShowRanges(ctx, n)
	case *tree.ShowFingerprints:
		return p.ShowFingerprints(ctx, n)
	case *tree.Split:
		return p.Split(ctx, n)
	case *tree.Truncate:
		return p.Truncate(ctx, n)
	case *tree.UnionClause:
		return p.Union(ctx, n, desiredTypes)
	case *tree.Update:
		return p.Update(ctx, n, desiredTypes)
	case *tree.ValuesClause:
		return p.Values(ctx, n, desiredTypes)
	case *tree.ValuesClauseWithNames:
		return p.Values(ctx, n, desiredTypes)
	default:
		return nil, errors.Errorf("unknown statement type: %T", stmt)
	}
}

// prepare constructs the logical plan for the statement.  This is
// needed both to type placeholders and to inform pgwire of the types
// of the result columns. All statements that either support
// placeholders or have result columns must be handled here.
// The resulting plan is stored in p.curPlan.
func (p *planner) prepare(ctx context.Context, stmt tree.Statement) error {
	// Reinitialize.
	p.curPlan = planTop{AST: stmt}

	// Prepare the plan.
	plan, err := p.doPrepare(ctx, stmt)
	if err != nil {
		return err
	}

	// Store the plan for later use.
	p.curPlan.plan = plan

	return nil
}

func (p *planner) doPrepare(ctx context.Context, stmt tree.Statement) (planNode, error) {
	if plan, err := p.maybePlanHook(ctx, stmt); plan != nil || err != nil {
		return plan, err
	}
	p.isPreparing = true

	switch n := stmt.(type) {
	case *tree.AlterUserSetPassword:
		return p.AlterUserSetPassword(ctx, n)
	case *tree.CancelQueries:
		return p.CancelQueries(ctx, n)
	case *tree.CancelSessions:
		return p.CancelSessions(ctx, n)
	case *tree.ControlJobs:
		return p.ControlJobs(ctx, n)
	case *tree.CreateUser:
		return p.CreateUser(ctx, n)
	case *tree.CreateTable:
		return p.CreateTable(ctx, n)
	case *tree.Delete:
		return p.Delete(ctx, n, nil)
	case *tree.DropUser:
		return p.DropUser(ctx, n)
	case *tree.Explain:
		return p.Explain(ctx, n)
	case *tree.Insert:
		return p.Insert(ctx, n, nil)
	case *tree.Scrub:
		return p.Scrub(ctx, n)
	case *tree.Select:
		return p.Select(ctx, n, nil)
	case *tree.SelectClause:
		return p.SelectClause(ctx, n, nil /* orderBy */, nil /* limit */, nil, /* with */
			nil /* desiredTypes */, publicColumns)
	case *tree.SetClusterSetting:
		return p.SetClusterSetting(ctx, n)
	case *tree.SetVar:
		return p.SetVar(ctx, n)
	case *tree.SetZoneConfig:
		return p.SetZoneConfig(ctx, n)
	case *tree.ShowClusterSetting:
		return p.ShowClusterSetting(ctx, n)
	case *tree.ShowVar:
		return p.ShowVar(ctx, n)
	case *tree.ShowCreate:
		return p.ShowCreate(ctx, n)
	case *tree.ShowColumns:
		return p.ShowColumns(ctx, n)
	case *tree.ShowDatabases:
		return p.ShowDatabases(ctx, n)
	case *tree.ShowGrants:
		return p.ShowGrants(ctx, n)
	case *tree.ShowIndex:
		return p.ShowIndex(ctx, n)
	case *tree.ShowConstraints:
		return p.ShowConstraints(ctx, n)
	case *tree.ShowQueries:
		return p.ShowQueries(ctx, n)
	case *tree.ShowJobs:
		return p.ShowJobs(ctx, n)
	case *tree.ShowRoleGrants:
		return p.ShowRoleGrants(ctx, n)
	case *tree.ShowRoles:
		return p.ShowRoles(ctx, n)
	case *tree.ShowSessions:
		return p.ShowSessions(ctx, n)
	case *tree.ShowTables:
		return p.ShowTables(ctx, n)
	case *tree.ShowSchemas:
		return p.ShowSchemas(ctx, n)
	case *tree.ShowTraceForSession:
		return p.ShowTrace(ctx, n)
	case *tree.ShowUsers:
		return p.ShowUsers(ctx, n)
	case *tree.ShowTransactionStatus:
		return p.ShowTransactionStatus(ctx)
	case *tree.ShowRanges:
		return p.ShowRanges(ctx, n)
	case *tree.Split:
		return p.Split(ctx, n)
	case *tree.Truncate:
		return p.Truncate(ctx, n)
	case *tree.Relocate:
		return p.Relocate(ctx, n)
	case *tree.Scatter:
		return p.Scatter(ctx, n)
	case *tree.Update:
		return p.Update(ctx, n, nil)
	default:
		// Other statement types do not have result columns and do not
		// support placeholders so there is no need for any special
		// handling here.
		return nil, nil
	}
}

// Mark transaction as operating on the system DB if the descriptor id
// is within the SystemConfig range.
func (p *planner) maybeSetSystemConfig(id sqlbase.ID) error {
	if !sqlbase.IsSystemConfigID(id) {
		return nil
	}
	// Mark transaction as operating on the system DB.
	return p.txn.SetSystemConfigTrigger()
}
