// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

type distSQLSpecExecFactory struct {
	planner *planner
	dsp     *DistSQLPlanner
	// planContexts is a utility struct that stores already instantiated
	// planning contexts. It should not be accessed directly, use getPlanCtx()
	// instead. The struct allows for lazy instantiation of the planning
	// contexts which are then reused between different calls to Construct*
	// methods. We need to keep both because every stage of processors can
	// either be distributed or local regardless of the distribution of the
	// previous stages.
	planContexts struct {
		distPlanCtx *PlanningCtx
		// localPlanCtx stores the local planning context of the gateway.
		localPlanCtx *PlanningCtx
	}
	singleTenant bool
}

var _ exec.Factory = &distSQLSpecExecFactory{}

func newDistSQLSpecExecFactory(p *planner) exec.Factory {
	return &distSQLSpecExecFactory{
		planner:      p,
		dsp:          p.extendedEvalCtx.DistSQLPlanner,
		singleTenant: p.execCfg.Codec.ForSystemTenant(),
	}
}

func (e *distSQLSpecExecFactory) getPlanCtx(recommendation distRecommendation) *PlanningCtx {
	distribute := false
	if e.singleTenant {
		distribute = shouldDistributeGivenRecAndMode(recommendation, e.planner.extendedEvalCtx.SessionData.DistSQLMode)
	}
	if distribute {
		if e.planContexts.distPlanCtx == nil {
			evalCtx := e.planner.ExtendedEvalContext()
			e.planContexts.distPlanCtx = e.dsp.NewPlanningCtx(evalCtx.Context, evalCtx, e.planner.txn, distribute)
		}
		return e.planContexts.distPlanCtx
	}
	if e.planContexts.localPlanCtx == nil {
		evalCtx := e.planner.ExtendedEvalContext()
		e.planContexts.localPlanCtx = e.dsp.NewPlanningCtx(evalCtx.Context, evalCtx, e.planner.txn, distribute)
	}
	return e.planContexts.localPlanCtx
}

func (e *distSQLSpecExecFactory) ConstructValues(
	rows [][]tree.TypedExpr, cols sqlbase.ResultColumns,
) (exec.Node, error) {
	// TODO(yuzefovich): make sure to not distribute when rows == 0.
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

// ConstructScan implements exec.Factory interface by combining the logic that
// performs scanNode creation of execFactory.ConstructScan and physical
// planning of table readers of DistSQLPlanner.createTableReaders.
func (e *distSQLSpecExecFactory) ConstructScan(
	table cat.Table,
	index cat.Index,
	needed exec.TableColumnOrdinalSet,
	indexConstraint *constraint.Constraint,
	hardLimit int64,
	softLimit int64,
	reverse bool,
	maxResults uint64,
	reqOrdering exec.OutputOrdering,
	rowCount float64,
	locking *tree.LockingItem,
) (exec.Node, error) {
	if table.IsVirtualTable() {
		return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
	}

	var p PhysicalPlan
	// Although we don't yet recommend distributing plans where soft limits
	// propagate to scan nodes because we don't have infrastructure to only
	// plan for a few ranges at a time, the propagation of the soft limits
	// to scan nodes has been added in 20.1 release, so to keep the
	// previous behavior we continue to ignore the soft limits for now.
	// TODO(yuzefovich): pay attention to the soft limits.
	recommendation := canDistribute

	// Phase 1: set up all necessary infrastructure for table reader planning
	// below. This phase is equivalent to what execFactory.ConstructScan does.
	tabDesc := table.(*optTable).desc
	indexDesc := index.(*optIndex).desc
	colCfg := makeScanColumnsConfig(table, needed)
	sb := span.MakeBuilder(e.planner.ExecCfg().Codec, tabDesc.TableDesc(), indexDesc)

	// Note that initColsForScan and setting ResultColumns below are equivalent
	// to what scan.initTable call does in execFactory.ConstructScan.
	cols, err := initColsForScan(tabDesc, colCfg)
	if err != nil {
		return nil, err
	}
	p.ResultColumns = sqlbase.ResultColumnsFromColDescs(tabDesc.GetID(), cols)

	if indexConstraint != nil && indexConstraint.IsContradiction() {
		// TODO(yuzefovich): once ConstructValues is implemented, consider
		// calling it here.
		physPlan, err := e.dsp.createValuesPlan(
			getTypesFromResultColumns(p.ResultColumns), 0 /* numRows */, nil, /* rawBytes */
		)
		return planMaybePhysical{physPlan: physPlan}, err
	}

	// TODO(yuzefovich): scanNode adds "parallel" attribute in walk.go when
	// scanNode.canParallelize() returns true. We should plumb that info from
	// here somehow as well.
	var spans roachpb.Spans
	spans, err = sb.SpansFromConstraint(indexConstraint, needed, false /* forDelete */)
	if err != nil {
		return nil, err
	}
	isFullTableScan := len(spans) == 1 && spans[0].EqualValue(
		tabDesc.IndexSpan(e.planner.ExecCfg().Codec, indexDesc.ID),
	)
	if err = colCfg.assertValidReqOrdering(reqOrdering); err != nil {
		return nil, err
	}

	// Check if we are doing a full scan.
	if isFullTableScan {
		recommendation = recommendation.compose(shouldDistribute)
	}

	// Phase 2: perform the table reader planning. This phase is equivalent to
	// what DistSQLPlanner.createTableReaders does.
	colsToTableOrdinalMap := toTableOrdinals(cols, tabDesc, colCfg.visibility)
	trSpec := physicalplan.NewTableReaderSpec()
	*trSpec = execinfrapb.TableReaderSpec{
		Table:      *tabDesc.TableDesc(),
		Reverse:    reverse,
		IsCheck:    false,
		Visibility: colCfg.visibility,
		// Retain the capacity of the spans slice.
		Spans: trSpec.Spans[:0],
	}
	trSpec.IndexIdx, err = getIndexIdx(indexDesc, tabDesc)
	if err != nil {
		return nil, err
	}
	if locking != nil {
		trSpec.LockingStrength = sqlbase.ToScanLockingStrength(locking.Strength)
		trSpec.LockingWaitPolicy = sqlbase.ToScanLockingWaitPolicy(locking.WaitPolicy)
		if trSpec.LockingStrength != sqlbase.ScanLockingStrength_FOR_NONE {
			// Scans that are performing row-level locking cannot currently be
			// distributed because their locks would not be propagated back to
			// the root transaction coordinator.
			// TODO(nvanbenschoten): lift this restriction.
			recommendation = cannotDistribute
		}
	}

	// Note that we don't do anything about the possible filter here since we
	// don't know yet whether we will have it. ConstructFilter is responsible
	// for pushing the filter down into the post-processing stage of this scan.
	post := execinfrapb.PostProcessSpec{}
	if hardLimit != 0 {
		post.Limit = uint64(hardLimit)
	} else if softLimit != 0 {
		trSpec.LimitHint = softLimit
	}

	err = e.dsp.planTableReaders(
		e.getPlanCtx(recommendation),
		&p,
		&tableReaderPlanningInfo{
			spec:                   trSpec,
			post:                   post,
			desc:                   tabDesc,
			spans:                  spans,
			reverse:                reverse,
			scanVisibility:         colCfg.visibility,
			maxResults:             maxResults,
			estimatedRowCount:      uint64(rowCount),
			reqOrdering:            ReqOrdering(reqOrdering),
			cols:                   cols,
			colsToTableOrdrinalMap: colsToTableOrdinalMap,
		},
	)

	return planMaybePhysical{physPlan: &p}, err
}

func (e *distSQLSpecExecFactory) ConstructFilter(
	n exec.Node, filter tree.TypedExpr, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	plan := n.(planMaybePhysical)
	physPlan := plan.physPlan
	physPlan.SetMergeOrdering(e.dsp.convertOrdering(ReqOrdering(reqOrdering), physPlan.PlanToStreamColMap))
	// We recommend for filter's distribution to be the same as the last stage
	// of processors.
	recommendation := shouldNotDistribute
	if physPlan.IsLastStageDistributed() {
		recommendation = shouldDistribute
	}
	gatewayNodeID := roachpb.NodeID(e.planner.execCfg.NodeID.SQLInstanceID())
	if err := checkExpr(filter); err != nil {
		recommendation = cannotDistribute
		if physPlan.IsLastStageDistributed() {
			// The last stage has been distributed, but filter expression
			// cannot be distributed, so we need to merge the streams on a
			// single node. We could do so on one of the nodes that streams
			// originate from, but for now we choose the gateway.
			physPlan.AddSingleGroupStage(
				gatewayNodeID,
				execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
				execinfrapb.PostProcessSpec{},
				physPlan.ResultTypes,
				gatewayNodeID,
			)
		}
	}
	// AddFilter will attempt to push the filter into the last stage of
	// processors.
	if err := physPlan.AddFilter(filter, e.getPlanCtx(recommendation), physPlan.PlanToStreamColMap, gatewayNodeID); err != nil {
		return nil, err
	}
	return plan, nil
}

func (e *distSQLSpecExecFactory) ConstructSimpleProject(
	n exec.Node, cols []exec.NodeColumnOrdinal, colNames []string, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructRender(
	n exec.Node,
	columns sqlbase.ResultColumns,
	exprs tree.TypedExprs,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructApplyJoin(
	joinType sqlbase.JoinType,
	left exec.Node,
	rightColumns sqlbase.ResultColumns,
	onCond tree.TypedExpr,
	planRightSideFn exec.ApplyJoinPlanRightSideFn,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructHashJoin(
	joinType sqlbase.JoinType,
	left, right exec.Node,
	leftEqCols, rightEqCols []exec.NodeColumnOrdinal,
	leftEqColsAreKey, rightEqColsAreKey bool,
	extraOnCond tree.TypedExpr,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructMergeJoin(
	joinType sqlbase.JoinType,
	left, right exec.Node,
	onCond tree.TypedExpr,
	leftOrdering, rightOrdering sqlbase.ColumnOrdering,
	reqOrdering exec.OutputOrdering,
	leftEqColsAreKey, rightEqColsAreKey bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructGroupBy(
	input exec.Node,
	groupCols []exec.NodeColumnOrdinal,
	groupColOrdering sqlbase.ColumnOrdering,
	aggregations []exec.AggInfo,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructScalarGroupBy(
	input exec.Node, aggregations []exec.AggInfo,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructDistinct(
	input exec.Node,
	distinctCols, orderedCols exec.NodeColumnOrdinalSet,
	reqOrdering exec.OutputOrdering,
	nullsAreDistinct bool,
	errorOnDup string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructSetOp(
	typ tree.UnionType, all bool, left, right exec.Node,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructSort(
	input exec.Node, ordering sqlbase.ColumnOrdering, alreadyOrderedPrefix int,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructOrdinality(
	input exec.Node, colName string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructIndexJoin(
	input exec.Node,
	table cat.Table,
	keyCols []exec.NodeColumnOrdinal,
	tableCols exec.TableColumnOrdinalSet,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructLookupJoin(
	joinType sqlbase.JoinType,
	input exec.Node,
	table cat.Table,
	index cat.Index,
	eqCols []exec.NodeColumnOrdinal,
	eqColsAreKey bool,
	lookupCols exec.TableColumnOrdinalSet,
	onCond tree.TypedExpr,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructGeoLookupJoin(
	joinType sqlbase.JoinType,
	geoRelationshipType geoindex.RelationshipType,
	input exec.Node,
	table cat.Table,
	index cat.Index,
	geoCol exec.NodeColumnOrdinal,
	lookupCols exec.TableColumnOrdinalSet,
	onCond tree.TypedExpr,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructZigzagJoin(
	leftTable cat.Table,
	leftIndex cat.Index,
	rightTable cat.Table,
	rightIndex cat.Index,
	leftEqCols []exec.NodeColumnOrdinal,
	rightEqCols []exec.NodeColumnOrdinal,
	leftCols exec.NodeColumnOrdinalSet,
	rightCols exec.NodeColumnOrdinalSet,
	onCond tree.TypedExpr,
	fixedVals []exec.Node,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructLimit(
	input exec.Node, limit, offset tree.TypedExpr,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructMax1Row(
	input exec.Node, errorText string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructProjectSet(
	n exec.Node, exprs tree.TypedExprs, zipCols sqlbase.ResultColumns, numColsPerGen []int,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructWindow(
	input exec.Node, window exec.WindowInfo,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) RenameColumns(
	input exec.Node, colNames []string,
) (exec.Node, error) {
	var inputCols sqlbase.ResultColumns
	// distSQLSpecExecFactory still constructs some of the planNodes (for
	// example, some variants of EXPLAIN), and we need to be able to rename
	// the columns on them.
	switch plan := input.(type) {
	case planMaybePhysical:
		inputCols = plan.physPlan.ResultColumns
	case planNode:
		inputCols = planMutableColumns(plan)
	default:
		panic("unexpected node")
	}
	for i := range inputCols {
		inputCols[i].Name = colNames[i]
	}
	return input, nil
}

func (e *distSQLSpecExecFactory) ConstructPlan(
	root exec.Node, subqueries []exec.Subquery, cascades []exec.Cascade, checks []exec.Node,
) (exec.Plan, error) {
	return constructPlan(e.planner, root, subqueries, cascades, checks)
}

func (e *distSQLSpecExecFactory) ConstructExplainOpt(
	plan string, envOpts exec.ExplainEnvData,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructExplain(
	options *tree.ExplainOptions, stmtType tree.StatementType, plan exec.Plan,
) (exec.Node, error) {
	// TODO(yuzefovich): make sure to return the same nice error in some
	// variants of EXPLAIN when subqueries are present as we do in the old path.
	// TODO(yuzefovich): make sure that local plan nodes that create
	// distributed jobs are shown as "distributed". See distSQLExplainable.
	return constructExplainPlanNode(options, stmtType, plan.(*planTop), e.planner)
}

func (e *distSQLSpecExecFactory) ConstructShowTrace(
	typ tree.ShowTraceType, compact bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructInsert(
	input exec.Node,
	table cat.Table,
	insertCols exec.TableColumnOrdinalSet,
	returnCols exec.TableColumnOrdinalSet,
	checkCols exec.CheckOrdinalSet,
	allowAutoCommit bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructInsertFastPath(
	rows [][]tree.TypedExpr,
	table cat.Table,
	insertCols exec.TableColumnOrdinalSet,
	returnCols exec.TableColumnOrdinalSet,
	checkCols exec.CheckOrdinalSet,
	fkChecks []exec.InsertFastPathFKCheck,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructUpdate(
	input exec.Node,
	table cat.Table,
	fetchCols exec.TableColumnOrdinalSet,
	updateCols exec.TableColumnOrdinalSet,
	returnCols exec.TableColumnOrdinalSet,
	checks exec.CheckOrdinalSet,
	passthrough sqlbase.ResultColumns,
	allowAutoCommit bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructUpsert(
	input exec.Node,
	table cat.Table,
	canaryCol exec.NodeColumnOrdinal,
	insertCols exec.TableColumnOrdinalSet,
	fetchCols exec.TableColumnOrdinalSet,
	updateCols exec.TableColumnOrdinalSet,
	returnCols exec.TableColumnOrdinalSet,
	checks exec.CheckOrdinalSet,
	allowAutoCommit bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructDelete(
	input exec.Node,
	table cat.Table,
	fetchCols exec.TableColumnOrdinalSet,
	returnCols exec.TableColumnOrdinalSet,
	allowAutoCommit bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructDeleteRange(
	table cat.Table,
	needed exec.TableColumnOrdinalSet,
	indexConstraint *constraint.Constraint,
	interleavedTables []cat.Table,
	maxReturnedKeys int,
	allowAutoCommit bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructCreateTable(
	input exec.Node, schema cat.Schema, ct *tree.CreateTable,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructCreateView(
	schema cat.Schema,
	viewName string,
	ifNotExists bool,
	replace bool,
	temporary bool,
	viewQuery string,
	columns sqlbase.ResultColumns,
	deps opt.ViewDeps,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructSequenceSelect(sequence cat.Sequence) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructSaveTable(
	input exec.Node, table *cat.DataSourceName, colNames []string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructErrorIfRows(
	input exec.Node, mkErr func(tree.Datums) error,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructOpaque(metadata opt.OpaqueMetadata) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructAlterTableSplit(
	index cat.Index, input exec.Node, expiration tree.TypedExpr,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructAlterTableUnsplit(
	index cat.Index, input exec.Node,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructAlterTableUnsplitAll(index cat.Index) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructAlterTableRelocate(
	index cat.Index, input exec.Node, relocateLease bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructBuffer(
	input exec.Node, label string,
) (exec.BufferNode, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructScanBuffer(
	ref exec.BufferNode, label string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructRecursiveCTE(
	initial exec.Node, fn exec.RecursiveCTEIterationFn, label string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructControlJobs(
	command tree.JobCommand, input exec.Node,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructCancelQueries(
	input exec.Node, ifExists bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructCancelSessions(
	input exec.Node, ifExists bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}

func (e *distSQLSpecExecFactory) ConstructExport(
	input exec.Node, fileName tree.TypedExpr, fileFormat string, options []exec.KVOption,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning")
}
