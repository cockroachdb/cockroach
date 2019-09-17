// Copyright 2015 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/delegate"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

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

// ExecCfg gives convenient access to the runParam's ExecutorConfig.
func (r *runParams) ExecCfg() *ExecutorConfig {
	return r.extendedEvalCtx.ExecCfg
}

// Ann is a shortcut for the Annotations from the eval context.
func (r *runParams) Ann() *tree.Annotations {
	return r.extendedEvalCtx.EvalContext.Annotations
}

// createTimeForNewTableDescriptor consults the cluster version to determine
// whether the CommitTimestamp() needs to be observed when creating a new
// TableDescriptor. See TableDescriptor.ModificationTime.
//
// TODO(ajwerner): remove in 20.1.
func (r *runParams) creationTimeForNewTableDescriptor() hlc.Timestamp {
	// Before 19.2 we needed to observe the transaction CommitTimestamp to ensure
	// that CreateAsOfTime and ModificationTime reflected the timestamp at which the
	// creating transaction committed. Starting in 19.2 we use a zero-valued
	// CreateAsOfTime and ModificationTime when creating a table descriptor and then
	// upon reading use the MVCC timestamp to populate the values.
	var ts hlc.Timestamp
	if !r.ExecCfg().Settings.Version.IsActive(cluster.VersionTableDescModificationTimeFromMVCC) {
		ts = r.p.txn.CommitTimestamp()
	}
	return ts
}

// planNode defines the interface for executing a query or portion of a query.
//
// The following methods apply to planNodes and contain special cases
// for each type; they thus need to be extended when adding/removing
// planNode instances:
// - planVisitor.visit()           (walk.go)
// - planNodeNames                 (walk.go)
// - planMaker.optimizeFilters()   (filter_opt.go)
// - setLimitHint()                (limit_hint.go)
// - planOrdering()                (plan_ordering.go)
// - planColumns()                 (plan_columns.go)
//
type planNode interface {
	startExec(params runParams) error

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
	// doesn't imply that startExec() has been necessarily called.
	//
	// This method must not be called during execution - the planNode
	// tree must remain "live" and readable via walk() even after
	// execution completes.
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
var _ planNode = &bufferNode{}
var _ planNode = &cancelQueriesNode{}
var _ planNode = &cancelSessionsNode{}
var _ planNode = &changePrivilegesNode{}
var _ planNode = &createDatabaseNode{}
var _ planNode = &createIndexNode{}
var _ planNode = &createSequenceNode{}
var _ planNode = &createStatsNode{}
var _ planNode = &createTableNode{}
var _ planNode = &CreateUserNode{}
var _ planNode = &createViewNode{}
var _ planNode = &delayedNode{}
var _ planNode = &deleteNode{}
var _ planNode = &deleteRangeNode{}
var _ planNode = &distinctNode{}
var _ planNode = &dropDatabaseNode{}
var _ planNode = &dropIndexNode{}
var _ planNode = &dropSequenceNode{}
var _ planNode = &dropTableNode{}
var _ planNode = &DropUserNode{}
var _ planNode = &dropViewNode{}
var _ planNode = &errorIfRowsNode{}
var _ planNode = &explainDistSQLNode{}
var _ planNode = &explainPlanNode{}
var _ planNode = &explainVecNode{}
var _ planNode = &filterNode{}
var _ planNode = &groupNode{}
var _ planNode = &hookFnNode{}
var _ planNode = &indexJoinNode{}
var _ planNode = &insertNode{}
var _ planNode = &joinNode{}
var _ planNode = &limitNode{}
var _ planNode = &max1RowNode{}
var _ planNode = &ordinalityNode{}
var _ planNode = &projectSetNode{}
var _ planNode = &relocateNode{}
var _ planNode = &renameColumnNode{}
var _ planNode = &renameDatabaseNode{}
var _ planNode = &renameIndexNode{}
var _ planNode = &renameTableNode{}
var _ planNode = &renderNode{}
var _ planNode = &rowCountNode{}
var _ planNode = &scanBufferNode{}
var _ planNode = &scanNode{}
var _ planNode = &scatterNode{}
var _ planNode = &serializeNode{}
var _ planNode = &sequenceSelectNode{}
var _ planNode = &showFingerprintsNode{}
var _ planNode = &showTraceNode{}
var _ planNode = &sortNode{}
var _ planNode = &splitNode{}
var _ planNode = &unsplitNode{}
var _ planNode = &unsplitAllNode{}
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
var _ planNodeFastPath = &deleteRangeNode{}
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

	// subqueryPlans contains all the sub-query plans.
	subqueryPlans []subquery

	// postqueryPlans contains all the plans for subqueries that are to be
	// executed after the main query (for example, foreign key checks).
	postqueryPlans []postquery

	// auditEvents becomes non-nil if any of the descriptors used by
	// current statement is causing an auditing event. See exec_log.go.
	auditEvents []auditEvent

	// flags is populated during planning and execution.
	flags planFlags

	// execErr retains the last execution error, if any.
	execErr error

	// maybeSavePlan, if defined, is called during close() to
	// conditionally save the logical plan to savedPlanForStats.
	maybeSavePlan func(context.Context) *roachpb.ExplainTreePlanNode

	// savedPlanForStats is conditionally populated at the end of
	// statement execution, for registration in statement statistics.
	savedPlanForStats *roachpb.ExplainTreePlanNode

	// avoidBuffering, when set, causes the execution to avoid buffering
	// results.
	avoidBuffering bool
}

// postquery is a query tree that is executed after the main one. It can only
// return an error (for example, foreign key violation).
type postquery struct {
	plan planNode
}

// close ensures that the plan's resources have been deallocated.
func (p *planTop) close(ctx context.Context) {
	if p.plan != nil {
		if p.maybeSavePlan != nil && p.flags.IsSet(planFlagExecDone) {
			p.savedPlanForStats = p.maybeSavePlan(ctx)
		}
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

	for i := range p.postqueryPlans {
		if p.postqueryPlans[i].plan != nil {
			p.postqueryPlans[i].plan.Close(ctx)
			p.postqueryPlans[i].plan = nil
		}
	}
}

// startExec calls startExec() on each planNode using a depth-first, post-order
// traversal.  The subqueries, if any, are also started.
//
// Reminder: walkPlan() ensures that subqueries and sub-plans are
// started before startExec() is called.
func startExec(params runParams, plan planNode) error {
	o := planObserver{
		enterNode: func(ctx context.Context, _ string, p planNode) (bool, error) {
			switch p.(type) {
			case *explainPlanNode, *explainDistSQLNode, *explainVecNode:
				// Do not recurse: we're not starting the plan if we just show its structure with EXPLAIN.
				return false, nil
			case *showTraceNode:
				// showTrace needs to override the params struct, and does so in its startExec() method.
				return false, nil
			}
			return true, nil
		},
		leaveNode: func(_ string, n planNode) error {
			return n.startExec(params)
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
		if fn, header, subplans, avoidBuffering, err := planHook(ctx, stmt, p); err != nil {
			return nil, err
		} else if fn != nil {
			if avoidBuffering {
				p.curPlan.avoidBuffering = true
			}
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

// newPlan constructs a planNode from a statement. This is used
// recursively by the various node constructors.
func (p *planner) newPlan(
	ctx context.Context, stmt tree.Statement, desiredTypes []*types.T,
) (planNode, error) {
	tracing.AnnotateTrace()

	// This will set the system DB trigger for transactions containing
	// schema-modifying statements that have no effect, such as
	// `BEGIN; INSERT INTO ...; CREATE TABLE IF NOT EXISTS ...; COMMIT;`
	// where the table already exists. This will generate some false
	// schema cache refreshes, but that's expected to be quite rare in practice.
	canModifySchema := tree.CanModifySchema(stmt)
	if canModifySchema {
		if err := p.txn.SetSystemConfigTrigger(); err != nil {
			return nil, unimplemented.NewWithIssuef(26508,
				"schema change statement cannot follow a statement that has written in the same transaction: %v", err)
		}
	}

	if p.EvalContext().TxnReadOnly {
		if canModifySchema || tree.CanWriteData(stmt) {
			return nil, pgerror.Newf(pgcode.ReadOnlySQLTransaction,
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
	case *tree.CommentOnColumn:
		return p.CommentOnColumn(ctx, n)
	case *tree.CommentOnDatabase:
		return p.CommentOnDatabase(ctx, n)
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
	case *tree.SetSessionAuthorizationDefault:
		return p.SetSessionAuthorizationDefault()
	case *tree.SetSessionCharacteristics:
		return p.SetSessionCharacteristics(n)
	case *tree.ShowClusterSetting:
		return p.ShowClusterSetting(ctx, n)
	case *tree.ShowHistogram:
		return p.ShowHistogram(ctx, n)
	case *tree.ShowTableStats:
		return p.ShowTableStats(ctx, n)
	case *tree.ShowTraceForSession:
		return p.ShowTrace(ctx, n)
	case *tree.ShowZoneConfig:
		return p.ShowZoneConfig(ctx, n)
	case *tree.ShowFingerprints:
		return p.ShowFingerprints(ctx, n)
	case *tree.Split:
		return p.Split(ctx, n)
	case *tree.Unsplit:
		return p.Unsplit(ctx, n)
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
	case tree.CCLOnlyStatement:
		return nil, pgerror.Newf(pgcode.CCLRequired,
			"a CCL binary is required to use this statement type: %T", stmt)
	default:
		var catalog optCatalog
		catalog.init(p)
		catalog.reset()
		newStmt, err := delegate.TryDelegate(ctx, &catalog, p.EvalContext(), stmt)
		if err != nil {
			return nil, err
		}
		if newStmt != nil {
			return p.newPlan(ctx, newStmt, nil /* desiredTypes */)
		}
		return nil, errors.AssertionFailedf("unknown statement type: %T", stmt)
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

// planFlags is used throughout the planning code to keep track of various
// events or decisions along the way.
type planFlags uint32

const (
	// planFlagOptUsed is set if the optimizer was used to create the plan.
	planFlagOptUsed planFlags = (1 << iota)

	// planFlagOptCacheHit is set if a plan from the query plan cache was used (and
	// re-optimized).
	planFlagOptCacheHit

	// planFlagOptCacheMiss is set if we looked for a plan in the query plan cache but
	// did not find one.
	planFlagOptCacheMiss

	// planFlagDistributed is set if the plan is for the DistSQL engine, in
	// distributed mode.
	planFlagDistributed

	// planFlagDistSQLLocal is set if the plan is for the DistSQL engine,
	// but in local mode.
	planFlagDistSQLLocal

	// planFlagExecDone marks that execution has been completed.
	planFlagExecDone

	// planFlagImplicitTxn marks that the plan was run inside of an implicit
	// transaction.
	planFlagImplicitTxn
)

func (pf planFlags) IsSet(flag planFlags) bool {
	return (pf & flag) != 0
}

func (pf *planFlags) Set(flag planFlags) {
	*pf |= flag
}
