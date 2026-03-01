// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/auditlogging"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
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
func (r *runParams) EvalContext() *eval.Context {
	return &r.extendedEvalCtx.Context
}

// SessionData gives convenient access to the runParam's SessionData.
func (r *runParams) SessionData() *sessiondata.SessionData {
	return r.extendedEvalCtx.SessionData()
}

// ExecCfg gives convenient access to the runParam's ExecutorConfig.
func (r *runParams) ExecCfg() *ExecutorConfig {
	return r.extendedEvalCtx.ExecCfg
}

// Ann is a shortcut for the Annotations from the eval context.
func (r *runParams) Ann() *tree.Annotations {
	return r.extendedEvalCtx.Context.Annotations
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
type planNode interface {
	startExec(params runParams) error

	// Next performs one unit of work, returning false if an error is
	// encountered or if there is no more work to do. For statements
	// that return a result set, the Values() method will return one row
	// of results each time that Next() returns true.
	//
	// Available after startPlan(). It is illegal to call Next() after it returns
	// false.
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

	InputCount() int
	Input(i int) (planNode, error)
	SetInput(i int, p planNode) error
}

// zeroInputPlanNode is embedded in planNode implementations that have no input
// planNode. It implements the InputCount, Input, and SetInput methods of
// planNode.
type zeroInputPlanNode struct{}

func (zeroInputPlanNode) InputCount() int { return 0 }

func (zeroInputPlanNode) Input(i int) (planNode, error) {
	return nil, errors.AssertionFailedf("node has no inputs")
}

func (zeroInputPlanNode) SetInput(i int, p planNode) error {
	return errors.AssertionFailedf("node has no inputs")
}

// singleInputPlanNode is embedded in planNode implementations that have a
// single input planNode. It implements the InputCount, Input, and SetInput
// methods of planNode.
type singleInputPlanNode struct {
	input planNode
}

func (n *singleInputPlanNode) InputCount() int { return 1 }

func (n *singleInputPlanNode) Input(i int) (planNode, error) {
	if i == 0 {
		return n.input, nil
	}
	return nil, errors.AssertionFailedf("input index %d is out of range", i)
}

func (n *singleInputPlanNode) SetInput(i int, p planNode) error {
	if i == 0 {
		n.input = p
		return nil
	}
	return errors.AssertionFailedf("input index %d is out of range", i)
}

// mutationPlanNode is a specification of planNode for mutations operations
// (those that insert/update/detele/etc rows).
type mutationPlanNode interface {
	planNode

	// rowsWritten returns the number of table rows modified by this planNode.
	// It does not include rows written to secondary indexes. It should only be
	// called once Next returns false.
	rowsWritten() int64

	// indexRowsWritten returns the number of primary and secondary index rows
	// modified by this planNode. It is always >= rowsWritten. It should only be
	// called once Next returns false.
	indexRowsWritten() int64

	// indexBytesWritten returns the number of primary and secondary index bytes
	// modified by this planNode. It should only be called once Next returns
	// false.
	indexBytesWritten() int64

	// returnsRowsAffected indicates that the planNode returns the number of
	// rows affected by the mutation, rather than the rows themselves.
	returnsRowsAffected() bool

	// kvCPUTime returns the cumulative CPU time (in nanoseconds) that KV reported
	// in BatchResponse headers during the execution of this mutation. It should
	// only be called once Next returns false.
	kvCPUTime() int64
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
var _ planNode = &alterIndexVisibleNode{}
var _ planNode = &alterSchemaNode{}
var _ planNode = &alterSequenceNode{}
var _ planNode = &alterTableNode{}
var _ planNode = &alterTableOwnerNode{}
var _ planNode = &alterTableSetSchemaNode{}
var _ planNode = &alterTypeNode{}
var _ planNode = &bufferNode{}
var _ planNode = &cancelQueriesNode{}
var _ planNode = &cancelSessionsNode{}
var _ planNode = &changeDescriptorBackedPrivilegesNode{}
var _ planNode = &completionsNode{}
var _ planNode = &createDatabaseNode{}
var _ planNode = &createFunctionNode{}
var _ planNode = &createIndexNode{}
var _ planNode = &createSequenceNode{}
var _ planNode = &createStatsNode{}
var _ planNode = &createTableNode{}
var _ planNode = &createTypeNode{}
var _ planNode = &CreateRoleNode{}
var _ planNode = &createViewNode{}
var _ planNode = &delayedNode{}
var _ planNode = &deleteNode{}
var _ planNode = &deleteSwapNode{}
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
var _ planNode = &endPreparedTxnNode{}
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
var _ planNode = &relocateRange{}
var _ planNode = &renameColumnNode{}
var _ planNode = &renameDatabaseNode{}
var _ planNode = &renameIndexNode{}
var _ planNode = &renameTableNode{}
var _ planNode = &renderNode{}
var _ planNode = &RevokeRoleNode{}
var _ planNode = &scanBufferNode{}
var _ planNode = &scanNode{}
var _ planNode = &scatterNode{}
var _ planNode = &sequenceSelectNode{}
var _ planNode = &showFingerprintsNode{}
var _ planNode = &showTraceNode{}
var _ planNode = &sortNode{}
var _ planNode = &splitNode{}
var _ planNode = &topKNode{}
var _ planNode = &unsplitNode{}
var _ planNode = &unsplitAllNode{}
var _ planNode = &truncateNode{}
var _ planNode = &unaryNode{}
var _ planNode = &unionNode{}
var _ planNode = &updateNode{}
var _ planNode = &updateSwapNode{}
var _ planNode = &upsertNode{}
var _ planNode = &valuesNode{}
var _ planNode = &vectorMutationSearchNode{}
var _ planNode = &vectorSearchNode{}
var _ planNode = &virtualTableNode{}
var _ planNode = &windowNode{}
var _ planNode = &zeroNode{}

var _ planNodeReadingOwnWrites = &alterIndexNode{}
var _ planNodeReadingOwnWrites = &alterSchemaNode{}
var _ planNodeReadingOwnWrites = &alterSequenceNode{}
var _ planNodeReadingOwnWrites = &alterTableNode{}
var _ planNodeReadingOwnWrites = &alterTypeNode{}
var _ planNodeReadingOwnWrites = &createFunctionNode{}
var _ planNodeReadingOwnWrites = &createIndexNode{}
var _ planNodeReadingOwnWrites = &createSequenceNode{}
var _ planNodeReadingOwnWrites = &createDatabaseNode{}
var _ planNodeReadingOwnWrites = &createTableNode{}
var _ planNodeReadingOwnWrites = &createTypeNode{}
var _ planNodeReadingOwnWrites = &createViewNode{}
var _ planNodeReadingOwnWrites = &changeDescriptorBackedPrivilegesNode{}
var _ planNodeReadingOwnWrites = &dropSchemaNode{}
var _ planNodeReadingOwnWrites = &dropTypeNode{}
var _ planNodeReadingOwnWrites = &refreshMaterializedViewNode{}
var _ planNodeReadingOwnWrites = &setZoneConfigNode{}

type flowInfo struct {
	typ planComponentType
	// diagram is only populated when instrumentationHelper.shouldSaveDiagrams()
	// returns true. (Even in that case we've seen a sentry report #149987 where
	// it was nil.)
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
	// plan. Set unconditionally but used only by instrumentation (in order to
	// build the stmt bundle).
	mem     *memo.Memo
	catalog optPlanningCatalog

	// auditEventBuilders becomes non-nil if the current statement
	// is eligible for auditing (see sql/audit_logging.go)
	auditEventBuilders []auditlogging.AuditEventBuilder

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
	// onClose, if non-nil, will be called when closing this object.
	onClose func()
}

func (p *physicalPlanTop) Close(ctx context.Context) {
	for _, plan := range p.planNodesToClose {
		plan.Close(ctx)
	}
	p.planNodesToClose = nil
	if p.onClose != nil {
		p.onClose()
		p.onClose = nil
	}
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
	planComponentTypeInner
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

	// flags is populated during planning and execution.
	flags planFlags

	// plan for the main query.
	main planMaybePhysical

	// mainRowCount is the estimated number of rows that the main query will
	// return, negative if the stats weren't available to make a good estimate.
	mainRowCount int64

	// cascades contains metadata for all cascades.
	cascades []postQueryMetadata

	// checkPlans contains all the plans for queries that are to be executed after
	// the main query (for example, foreign key checks).
	checkPlans []checkPlan

	// triggers contains metadata for all triggers.
	triggers []postQueryMetadata
}

type postQueryMetadata struct {
	exec.PostQuery
	// plan for the cascade/triggers. This plan is not populated upfront; it is
	// created only when it needs to run, after the main query.
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
	for i := range p.triggers {
		p.triggers[i].plan.Close(ctx)
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

// savePlanInfo updates the instrumentationHelper with information about how the
// plan was executed.
// NB: should only be called _after_ the execution of the plan has completed.
func (p *planTop) savePlanInfo() {
	vectorized := p.flags.IsSet(planFlagVectorized)
	distribution := physicalplan.LocalPlan
	if p.flags.IsSet(planFlagDistributedExecution) {
		// Only show that the plan was distributed if we actually had
		// distributed execution. This matches the logic from explainPlanNode
		// where we use the actual physical plan's distribution rather than the
		// physical planning heuristic.
		if p.flags.IsSet(planFlagFullyDistributed) {
			distribution = physicalplan.FullyDistributedPlan
		} else if p.flags.IsSet(planFlagPartiallyDistributed) {
			distribution = physicalplan.PartiallyDistributedPlan
		}
	}
	containsMutation := p.flags.IsSet(planFlagContainsMutation)
	generic := p.flags.IsSet(planFlagGeneric)
	optimized := p.flags.IsSet(planFlagOptimized)
	p.instrumentation.RecordPlanInfo(
		distribution, vectorized, containsMutation, generic, optimized,
	)
}

// startExec calls startExec() on each planNode using a depth-first, post-order
// traversal. The subqueries, if any, are also started.
//
// If the planNode also implements the nodeReadingOwnWrites interface,
// the txn is temporarily reconfigured to use read-your-own-writes for
// the duration of the call to startExec. This is used e.g. by
// DDL statements.
func startExec(params runParams, plan planNode) error {
	switch plan.(type) {
	case *explainVecNode, *explainDDLNode:
		// Do not recurse: we're not starting the plan if we just show its
		// structure with EXPLAIN.
	case *showTraceNode:
		// showTrace needs to override the params struct, and does so in its
		// startExec() method.
	default:
		// Start children nodes first. This ensures that subqueries and
		// sub-plans are started before startExec() is called.
		for i, n := 0, plan.InputCount(); i < n; i++ {
			child, err := plan.Input(i)
			if err != nil {
				return err
			}
			if err := startExec(params, child); err != nil {
				return err
			}
		}
	}
	if _, ok := plan.(planNodeReadingOwnWrites); ok {
		prevMode := params.p.Txn().ConfigureStepping(params.ctx, kv.SteppingDisabled)
		defer func() { _ = params.p.Txn().ConfigureStepping(params.ctx, prevMode) }()
	}
	return plan.startExec(params)
}

func (p *planner) maybePlanHook(ctx context.Context, stmt tree.Statement) (planNode, error) {
	// TODO(dan): This iteration makes the plan dispatch no longer constant
	// time. We could fix that with a map of `reflect.Type` but including
	// reflection in such a primary codepath is unfortunate. Instead, the
	// upcoming IR work will provide unique numeric type tags, which will
	// elegantly solve this.
	for _, planHook := range planHooks {

		// If we don't have placeholder, we know we're just doing prepare and we
		// should type check instead of doing the actual planning.
		if !p.EvalContext().HasPlaceholders() {
			matched, header, err := planHook.typeCheck(ctx, stmt, p)
			if err != nil {
				return nil, err
			}
			if !matched {
				continue
			}
			return newHookFnNode(planHook.name, func(ctx context.Context, datums chan<- tree.Datums) error {
				return errors.AssertionFailedf(
					"cannot execute prepared %v statement",
					planHook.name,
				)
			}, header, p.execCfg.Stopper), nil
		}

		if fn, header, avoidBuffering, err := planHook.fn(ctx, stmt, p); err != nil {
			return nil, err
		} else if fn != nil {
			if avoidBuffering {
				p.curPlan.avoidBuffering = true
			}
			return newHookFnNode(planHook.name, fn, header, p.execCfg.Stopper), nil
		}
	}
	return nil, nil
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

	// planFlagFullyDistributed is set if the query is planned to use full
	// distribution. This flag indicates that the query is such that it can be
	// distributed, and we think it's worth doing so; however, due to range
	// placement and other physical planning decisions, the plan might still end
	// up being local. See planFlagDistributedExecution if interested in whether
	// the physical plan actually ends up being distributed.
	planFlagFullyDistributed

	// planFlagPartiallyDistributed is set if the query is planned to use
	// partial distribution (see physicalplan.PartiallyDistributedPlan). Same
	// caveats apply as for planFlagFullyDistributed.
	planFlagPartiallyDistributed

	// planFlagImplicitTxn marks that the plan was run inside of an implicit
	// transaction.
	planFlagImplicitTxn

	// planFlagIsDDL marks that the plan contains DDL.
	planFlagIsDDL

	// planFlagVectorized is set if the plan is executed via the vectorized
	// engine.
	planFlagVectorized

	// planFlagContainsFullTableScan is set if the plan involves an unconstrained
	// scan on (the primary key of) a table. This could be an unconstrained scan
	// of any cardinality. Full scans of virtual tables are ignored.
	planFlagContainsFullTableScan

	// planFlagContainsFullIndexScan is set if the plan involves an unconstrained
	// non-partial secondary index scan. This could be an unconstrainted scan of
	// any cardinality. Full scans of virtual tables are ignored.
	planFlagContainsFullIndexScan

	// planFlagContainsLargeFullTableScan is set if the plan involves an
	// unconstrained scan on (the primary key of) a table estimated to read more
	// than large_full_scan_rows (or without available stats). Large scans of
	// virtual tables are ignored.
	planFlagContainsLargeFullTableScan

	// planFlagContainsLargeFullIndexScan is set if the plan involves an
	// unconstrained non-partial secondary index scan estimated to read more than
	// large_full_scan_rows (or without available stats). Large scans of virtual
	// tables are ignored.
	planFlagContainsLargeFullIndexScan

	// planFlagContainsMutation is set if the plan has any mutations.
	// TODO(yuzefovich): in addition to DELETE, INSERT, UPDATE, and UPSERT this
	// flag is also set for different ALTER and CREATE statements. Audit usages
	// of this flag to see whether it's desirable.
	planFlagContainsMutation

	// planFlagContainsLocking is set if the plan has a node with locking.
	planFlagContainsLocking

	// planFlagCheckContainsLocking is set if at least one check plan has a node
	// with locking.
	planFlagCheckContainsLocking

	// planFlagSessionMigration is set if the plan is being created during
	// a session migration.
	planFlagSessionMigration

	// planFlagGeneric is set if a generic query plan was used. A generic query
	// plan is a plan that is fully-optimized once and can be reused without
	// being re-optimized.
	planFlagGeneric

	// planFlagOptimized is set if optimization was performed during the
	// current execution of the query.
	planFlagOptimized

	// planFlagDistributedExecution is set if execution of any part of the plan
	// was distributed.
	planFlagDistributedExecution

	// These flags indicate whether at least one DELETE, INSERT, UPDATE, or
	// UPSERT stmt was found in the whole plan.
	planFlagContainsDelete
	planFlagContainsInsert
	planFlagContainsUpdate
	planFlagContainsUpsert

	// planFlagUsesRLS is set if the plan applies row-level security policies.
	planFlagUsesRLS
)

// IsSet returns true if the receiver has all of the given flags set.
func (pf planFlags) IsSet(flags planFlags) bool {
	return (pf & flags) == flags
}

// Set sets all of the given flags in the receiver.
func (pf *planFlags) Set(flags planFlags) {
	*pf |= flags
}

// Unset unsets all of the given flags in the receiver.
func (pf *planFlags) Unset(flags planFlags) {
	*pf &^= flags
}

// ShouldBeDistributed returns true if either fully distributed or partially
// distributed flag is set. In other words, it returns whether the plan should
// be distributed (we might end up not distributing it though due to range
// placement or moving the single flow to the gateway).
func (pf planFlags) ShouldBeDistributed() bool {
	return pf&(planFlagFullyDistributed|planFlagPartiallyDistributed) != 0
}
