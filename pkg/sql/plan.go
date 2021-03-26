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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
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

// planNode defines the interface for executing a query or portion of a query.
//
// The following methods apply to planNodes and contain special cases
// for each type; they thus need to be extended when adding/removing
// planNode instances:
// - planVisitor.visit()           (walk.go)
// - planNodeNames                 (walk.go)
// - setLimitHint()                (limit_hint.go)
// - planColumns()                 (plan_columns.go)
//
type planNode interface {
	startExec(params runParams) error

	// Next performs one unit of work, returning false if an error is
	// encountered or if there is no more work to do. For statements
	// that return a result set, the Values() method will return one row
	// of results each time that Next() returns true.
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
	//
	// The node must not be used again after this method is called. Some nodes put
	// themselves back into memory pools on Close.
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

// planNodeReadingOwnWrites can be implemented by planNodes which do
// not use the standard SQL principle of reading at the snapshot
// established at the start of the transaction. It requests that
// the top-level (shared) `startExec` function disable stepping
// mode for the duration of the node's `startExec()` call.
//
// This done e.g. for most DDL statements that perform multiple KV
// operations on descriptors, expecting to read their own writes.
//
// Note that only `startExec()` runs with the modified stepping mode,
// not the `Next()` methods. This interface (and the idea of
// temporarily disabling stepping mode) is neither sensical nor
// applicable to planNodes whose execution is interleaved with
// that of others.
type planNodeReadingOwnWrites interface {
	// ReadingOwnWrites is a marker interface.
	ReadingOwnWrites()
}

var _ planNode = &alterIndexNode{}
var _ planNode = &alterSchemaNode{}
var _ planNode = &alterSequenceNode{}
var _ planNode = &alterTableNode{}
var _ planNode = &alterTableOwnerNode{}
var _ planNode = &alterTableSetSchemaNode{}
var _ planNode = &alterTypeNode{}
var _ planNode = &bufferNode{}
var _ planNode = &cancelQueriesNode{}
var _ planNode = &cancelSessionsNode{}
var _ planNode = &changePrivilegesNode{}
var _ planNode = &createDatabaseNode{}
var _ planNode = &createIndexNode{}
var _ planNode = &createSequenceNode{}
var _ planNode = &createStatsNode{}
var _ planNode = &createTableNode{}
var _ planNode = &createTypeNode{}
var _ planNode = &CreateRoleNode{}
var _ planNode = &createViewNode{}
var _ planNode = &delayedNode{}
var _ planNode = &deleteNode{}
var _ planNode = &deleteRangeNode{}
var _ planNode = &distinctNode{}
var _ planNode = &dropDatabaseNode{}
var _ planNode = &dropIndexNode{}
var _ planNode = &dropSchemaNode{}
var _ planNode = &dropSequenceNode{}
var _ planNode = &dropTableNode{}
var _ planNode = &dropTypeNode{}
var _ planNode = &DropRoleNode{}
var _ planNode = &dropViewNode{}
var _ planNode = &errorIfRowsNode{}
var _ planNode = &explainVecNode{}
var _ planNode = &filterNode{}
var _ planNode = &GrantRoleNode{}
var _ planNode = &groupNode{}
var _ planNode = &hookFnNode{}
var _ planNode = &indexJoinNode{}
var _ planNode = &insertNode{}
var _ planNode = &insertFastPathNode{}
var _ planNode = &joinNode{}
var _ planNode = &limitNode{}
var _ planNode = &max1RowNode{}
var _ planNode = &ordinalityNode{}
var _ planNode = &projectSetNode{}
var _ planNode = &reassignOwnedByNode{}
var _ planNode = &refreshMaterializedViewNode{}
var _ planNode = &recursiveCTENode{}
var _ planNode = &relocateNode{}
var _ planNode = &renameColumnNode{}
var _ planNode = &renameDatabaseNode{}
var _ planNode = &renameIndexNode{}
var _ planNode = &renameTableNode{}
var _ planNode = &reparentDatabaseNode{}
var _ planNode = &renderNode{}
var _ planNode = &RevokeRoleNode{}
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

var _ planNodeFastPath = &deleteRangeNode{}
var _ planNodeFastPath = &rowCountNode{}
var _ planNodeFastPath = &serializeNode{}
var _ planNodeFastPath = &setZoneConfigNode{}
var _ planNodeFastPath = &controlJobsNode{}
var _ planNodeFastPath = &controlSchedulesNode{}

var _ planNodeReadingOwnWrites = &alterIndexNode{}
var _ planNodeReadingOwnWrites = &alterSchemaNode{}
var _ planNodeReadingOwnWrites = &alterSequenceNode{}
var _ planNodeReadingOwnWrites = &alterTableNode{}
var _ planNodeReadingOwnWrites = &alterTypeNode{}
var _ planNodeReadingOwnWrites = &createIndexNode{}
var _ planNodeReadingOwnWrites = &createSequenceNode{}
var _ planNodeReadingOwnWrites = &createDatabaseNode{}
var _ planNodeReadingOwnWrites = &createTableNode{}
var _ planNodeReadingOwnWrites = &createTypeNode{}
var _ planNodeReadingOwnWrites = &createViewNode{}
var _ planNodeReadingOwnWrites = &changePrivilegesNode{}
var _ planNodeReadingOwnWrites = &dropSchemaNode{}
var _ planNodeReadingOwnWrites = &dropTypeNode{}
var _ planNodeReadingOwnWrites = &refreshMaterializedViewNode{}
var _ planNodeReadingOwnWrites = &reparentDatabaseNode{}
var _ planNodeReadingOwnWrites = &setZoneConfigNode{}

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

type flowInfo struct {
	typ     planComponentType
	diagram execinfrapb.FlowDiagram
	// explainVec and explainVecVerbose are only populated when collecting a
	// statement bundle when the plan was vectorized.
	explainVec        []string
	explainVecVerbose []string
	// flowsMetadata stores metadata from flows that will be used by
	// execstats.TraceAnalyzer.
	flowsMetadata *execstats.FlowsMetadata
}

// planTop is the struct that collects the properties
// of an entire plan.
// Note: some additional per-statement state is also stored in
// semaCtx (placeholders).
// TODO(jordan): investigate whether/how per-plan state like
// placeholder data can be concentrated in a single struct.
type planTop struct {
	// stmt is a reference to the current statement (AST and other metadata).
	stmt *Statement

	planComponents

	// mem/catalog retains the memo and catalog that were used to create the
	// plan. Only set if needed by instrumentation (see ShouldSaveMemo).
	mem     *memo.Memo
	catalog *optCatalog

	// auditEvents becomes non-nil if any of the descriptors used by
	// current statement is causing an auditing event. See exec_log.go.
	auditEvents []auditEvent

	// flags is populated during planning and execution.
	flags planFlags

	// avoidBuffering, when set, causes the execution to avoid buffering
	// results.
	avoidBuffering bool

	// If we are collecting query diagnostics, flow information, including
	// diagrams, are saved here.
	distSQLFlowInfos []flowInfo

	instrumentation *instrumentationHelper
}

// physicalPlanTop is a utility wrapper around PhysicalPlan that allows for
// storing planNodes that "power" the processors in the physical plan.
type physicalPlanTop struct {
	// PhysicalPlan contains the physical plan that has not yet been finalized.
	*PhysicalPlan
	// planNodesToClose contains the planNodes that are a part of the physical
	// plan (via planNodeToRowSource wrapping). These planNodes need to be
	// closed explicitly since we don't have a planNode tree that performs the
	// closure.
	planNodesToClose []planNode
}

func (p *physicalPlanTop) Close(ctx context.Context) {
	for _, plan := range p.planNodesToClose {
		plan.Close(ctx)
	}
	p.planNodesToClose = nil
}

// planMaybePhysical is a utility struct representing a plan. It can currently
// use either planNode or DistSQL spec representation, but eventually will be
// replaced by the latter representation directly.
type planMaybePhysical struct {
	planNode planNode
	// physPlan (when non-nil) contains the physical plan that has not yet
	// been finalized.
	physPlan *physicalPlanTop
}

func makePlanMaybePhysical(physPlan *PhysicalPlan, planNodesToClose []planNode) planMaybePhysical {
	return planMaybePhysical{
		physPlan: &physicalPlanTop{
			PhysicalPlan:     physPlan,
			planNodesToClose: planNodesToClose,
		},
	}
}

func (p *planMaybePhysical) isPhysicalPlan() bool {
	return p.physPlan != nil
}

func (p *planMaybePhysical) planColumns() colinfo.ResultColumns {
	if p.isPhysicalPlan() {
		return p.physPlan.ResultColumns
	}
	return planColumns(p.planNode)
}

// Close closes the pieces of the plan that haven't been yet closed. Note that
// it also resets the corresponding fields.
func (p *planMaybePhysical) Close(ctx context.Context) {
	if p.planNode != nil {
		p.planNode.Close(ctx)
		p.planNode = nil
	}
	if p.physPlan != nil {
		p.physPlan.Close(ctx)
		p.physPlan = nil
	}
}

type planComponentType int

const (
	planComponentTypeUnknown = iota
	planComponentTypeMainQuery
	planComponentTypeSubquery
	planComponentTypePostquery
)

func (t planComponentType) String() string {
	switch t {
	case planComponentTypeMainQuery:
		return "main-query"
	case planComponentTypeSubquery:
		return "subquery"
	case planComponentTypePostquery:
		return "postquery"
	default:
		return "unknownquerytype"
	}
}

// planComponents groups together the various components of the entire query
// plan.
type planComponents struct {
	// subqueryPlans contains all the sub-query plans.
	subqueryPlans []subquery

	// plan for the main query.
	main planMaybePhysical

	// cascades contains metadata for all cascades.
	cascades []cascadeMetadata

	// checkPlans contains all the plans for queries that are to be executed after
	// the main query (for example, foreign key checks).
	checkPlans []checkPlan
}

type cascadeMetadata struct {
	exec.Cascade
	// plan for the cascade. This plan is not populated upfront; it is created
	// only when it needs to run, after the main query (and previous cascades).
	plan planMaybePhysical
}

// checkPlan is a query tree that is executed after the main one. It can only
// return an error (for example, foreign key violation).
type checkPlan struct {
	plan planMaybePhysical
}

// close calls Close on all plan trees.
func (p *planComponents) close(ctx context.Context) {
	p.main.Close(ctx)
	for i := range p.subqueryPlans {
		p.subqueryPlans[i].plan.Close(ctx)
	}
	for i := range p.cascades {
		p.cascades[i].plan.Close(ctx)
	}
	for i := range p.checkPlans {
		p.checkPlans[i].plan.Close(ctx)
	}
}

// init resets planTop to point to a given statement; used at the start of the
// planning process.
func (p *planTop) init(stmt *Statement, instrumentation *instrumentationHelper) {
	*p = planTop{
		stmt:            stmt,
		instrumentation: instrumentation,
	}
}

// close ensures that the plan's resources have been deallocated.
func (p *planTop) close(ctx context.Context) {
	if p.flags.IsSet(planFlagExecDone) {
		p.savePlanInfo(ctx)
	}
	p.planComponents.close(ctx)
}

// savePlanInfo uses p.explainPlan to populate the plan string and/or tree.
func (p *planTop) savePlanInfo(ctx context.Context) {
	vectorized := p.flags.IsSet(planFlagVectorized)
	distribution := physicalplan.LocalPlan
	if p.flags.IsSet(planFlagFullyDistributed) {
		distribution = physicalplan.FullyDistributedPlan
	} else if p.flags.IsSet(planFlagPartiallyDistributed) {
		distribution = physicalplan.PartiallyDistributedPlan
	}
	p.instrumentation.RecordPlanInfo(distribution, vectorized)
}

// startExec calls startExec() on each planNode using a depth-first, post-order
// traversal.  The subqueries, if any, are also started.
//
// If the planNode also implements the nodeReadingOwnWrites interface,
// the txn is temporarily reconfigured to use read-your-own-writes for
// the duration of the call to startExec. This is used e.g. by
// DDL statements.
//
// Reminder: walkPlan() ensures that subqueries and sub-plans are
// started before startExec() is called.
func startExec(params runParams, plan planNode) error {
	o := planObserver{
		enterNode: func(ctx context.Context, _ string, p planNode) (bool, error) {
			switch p.(type) {
			case *explainVecNode, *explainDDLNode:
				// Do not recurse: we're not starting the plan if we just show its structure with EXPLAIN.
				return false, nil
			case *showTraceNode:
				// showTrace needs to override the params struct, and does so in its startExec() method.
				return false, nil
			}
			return true, nil
		},
		leaveNode: func(_ string, n planNode) (err error) {
			if _, ok := n.(planNodeReadingOwnWrites); ok {
				prevMode := params.p.Txn().ConfigureStepping(params.ctx, kv.SteppingDisabled)
				defer func() { _ = params.p.Txn().ConfigureStepping(params.ctx, prevMode) }()
			}
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
	return nil, nil
}

// Mark transaction as operating on the system DB if the descriptor id
// is within the SystemConfig range.
func (p *planner) maybeSetSystemConfig(id descpb.ID) error {
	if !descpb.IsSystemConfigID(id) {
		return nil
	}
	// Mark transaction as operating on the system DB.
	// Only the system tenant marks the SystemConfigTrigger.
	return p.txn.SetSystemConfigTrigger(p.execCfg.Codec.ForSystemTenant())
}

// planFlags is used throughout the planning code to keep track of various
// events or decisions along the way.
type planFlags uint32

const (
	// planFlagOptCacheHit is set if a plan from the query plan cache was used (and
	// re-optimized).
	planFlagOptCacheHit = (1 << iota)

	// planFlagOptCacheMiss is set if we looked for a plan in the query plan cache but
	// did not find one.
	planFlagOptCacheMiss

	// planFlagFullyDistributed is set if the query execution is is fully
	// distributed.
	planFlagFullyDistributed

	// planFlagPartiallyDistributed is set if the query execution is is partially
	// distributed (see physicalplan.PartiallyDistributedPlan).
	planFlagPartiallyDistributed

	// planFlagNotDistributed is set if the query execution is not distributed.
	planFlagNotDistributed

	// planFlagExecDone marks that execution has been completed.
	planFlagExecDone

	// planFlagImplicitTxn marks that the plan was run inside of an implicit
	// transaction.
	planFlagImplicitTxn

	// planFlagIsDDL marks that the plan contains DDL.
	planFlagIsDDL

	// planFlagVectorized is set if the plan is executed via the vectorized
	// engine.
	planFlagVectorized

	// planFlagTenant is set if the plan is executed on behalf of a tenant.
	planFlagTenant

	// planFlagContainsFullTableScan is set if the plan involves an unconstrained
	// scan on (the primary key of) a table.
	planFlagContainsFullTableScan

	// planFlagContainsFullIndexScan is set if the plan involves an unconstrained
	// secondary index scan.
	planFlagContainsFullIndexScan
)

func (pf planFlags) IsSet(flag planFlags) bool {
	return (pf & flag) != 0
}

func (pf *planFlags) Set(flag planFlags) {
	*pf |= flag
}

// IsDistributed returns true if either the fully or the partially distributed
// flags is set.
func (pf planFlags) IsDistributed() bool {
	return pf.IsSet(planFlagFullyDistributed) || pf.IsSet(planFlagPartiallyDistributed)
}
