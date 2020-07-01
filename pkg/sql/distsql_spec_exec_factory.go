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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
	singleTenant  bool
	gatewayNodeID roachpb.NodeID
}

var _ exec.Factory = &distSQLSpecExecFactory{}

func newDistSQLSpecExecFactory(p *planner) exec.Factory {
	return &distSQLSpecExecFactory{
		planner:       p,
		dsp:           p.extendedEvalCtx.DistSQLPlanner,
		singleTenant:  p.execCfg.Codec.ForSystemTenant(),
		gatewayNodeID: p.extendedEvalCtx.DistSQLPlanner.gatewayNodeID,
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

// TODO(yuzefovich): consider adding machinery that would confirm that
// assumptions that the execution makes about the plans coming from the
// optimizer are satisfied. A couple of examples are making sure that the
// constants in comparison expressions are always on the right and that the
// tuples in IN clause are sorted.

func (e *distSQLSpecExecFactory) ConstructValues(
	rows [][]tree.TypedExpr, cols sqlbase.ResultColumns,
) (exec.Node, error) {
	if (len(cols) == 0 && len(rows) == 1) || len(rows) == 0 {
		physPlan, err := e.dsp.createValuesPlan(
			getTypesFromResultColumns(cols), len(rows), nil, /* rawBytes */
		)
		if err != nil {
			return nil, err
		}
		physPlan.ResultColumns = cols
		return planMaybePhysical{physPlan: &physicalPlanTop{PhysicalPlan: physPlan}}, nil
	}
	recommendation := shouldDistribute
	for _, exprs := range rows {
		recommendation = recommendation.compose(
			e.checkExprsAndMaybeMergeLastStage(exprs, nil /* physPlan */),
		)
		if recommendation == cannotDistribute {
			break
		}
	}

	var (
		physPlan         *PhysicalPlan
		err              error
		planNodesToClose []planNode
	)
	planCtx := e.getPlanCtx(recommendation)
	if mustWrapValuesNode(planCtx, true /* specifiedInQuery */) {
		// The valuesNode must be wrapped into the physical plan, so we cannot
		// avoid creating it. See mustWrapValuesNode for more details.
		v := &valuesNode{
			columns:          cols,
			tuples:           rows,
			specifiedInQuery: true,
		}
		planNodesToClose = []planNode{v}
		physPlan, err = e.dsp.wrapPlan(planCtx, v)
	} else {
		// We can create a spec for the values processor, so we don't create a
		// valuesNode.
		physPlan, err = e.dsp.createPhysPlanForTuples(planCtx, rows, cols)
	}
	if err != nil {
		return nil, err
	}
	physPlan.ResultColumns = cols
	return planMaybePhysical{physPlan: &physicalPlanTop{
		PhysicalPlan:     physPlan,
		planNodesToClose: planNodesToClose,
	}}, nil
}

// ConstructScan implements exec.Factory interface by combining the logic that
// performs scanNode creation of execFactory.ConstructScan and physical
// planning of table readers of DistSQLPlanner.createTableReaders.
func (e *distSQLSpecExecFactory) ConstructScan(
	table cat.Table,
	index cat.Index,
	needed exec.TableColumnOrdinalSet,
	indexConstraint *constraint.Constraint,
	invertedConstraint invertedexpr.InvertedSpans,
	hardLimit int64,
	softLimit int64,
	reverse bool,
	maxResults uint64,
	reqOrdering exec.OutputOrdering,
	rowCount float64,
	locking *tree.LockingItem,
) (exec.Node, error) {
	if table.IsVirtualTable() {
		return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: virtual table scan")
	}

	p := MakePhysicalPlan(e.gatewayNodeID)
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
	p.ResultColumns = sqlbase.ResultColumnsFromColDescPtrs(tabDesc.GetID(), cols)

	if indexConstraint != nil && indexConstraint.IsContradiction() {
		// Note that empty rows argument is handled by ConstructValues first -
		// it will always create an appropriate values processor spec, so there
		// will be no planNodes created (which is what we want in this case).
		return e.ConstructValues([][]tree.TypedExpr{} /* rows */, p.ResultColumns)
	}

	// TODO(yuzefovich): scanNode adds "parallel" attribute in walk.go when
	// scanNode.canParallelize() returns true. We should plumb that info from
	// here somehow as well.
	var spans roachpb.Spans
	if invertedConstraint != nil {
		spans, err = GenerateInvertedSpans(invertedConstraint, sb)
	} else {
		spans, err = sb.SpansFromConstraint(indexConstraint, needed, false /* forDelete */)
	}
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

	return planMaybePhysical{physPlan: &physicalPlanTop{PhysicalPlan: &p}}, err
}

// checkExprsAndMaybeMergeLastStage is a helper method that returns a
// recommendation about exprs' distribution. If one of the expressions cannot
// be distributed, then all expressions cannot be distributed either. In such
// case if the last stage is distributed, this method takes care of merging the
// streams on the gateway node. physPlan may be nil.
func (e *distSQLSpecExecFactory) checkExprsAndMaybeMergeLastStage(
	exprs tree.TypedExprs, physPlan *PhysicalPlan,
) distRecommendation {
	// We recommend for exprs' distribution to be the same as the last stage
	// of processors (if there is such).
	recommendation := shouldDistribute
	if physPlan != nil && !physPlan.IsLastStageDistributed() {
		recommendation = shouldNotDistribute
	}
	for _, expr := range exprs {
		if err := checkExpr(expr); err != nil {
			recommendation = cannotDistribute
			if physPlan != nil {
				// The filter expression cannot be distributed, so we need to
				// make sure that there is a single stream on a node. We could
				// do so on one of the nodes that streams originate from, but
				// for now we choose the gateway.
				physPlan.EnsureSingleStreamOnGateway()
			}
			break
		}
	}
	return recommendation
}

func (e *distSQLSpecExecFactory) ConstructFilter(
	n exec.Node, filter tree.TypedExpr, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	physPlan, plan := getPhysPlan(n)
	recommendation := e.checkExprsAndMaybeMergeLastStage([]tree.TypedExpr{filter}, physPlan)
	// AddFilter will attempt to push the filter into the last stage of
	// processors.
	if err := physPlan.AddFilter(filter, e.getPlanCtx(recommendation), physPlan.PlanToStreamColMap); err != nil {
		return nil, err
	}
	physPlan.SetMergeOrdering(e.dsp.convertOrdering(ReqOrdering(reqOrdering), physPlan.PlanToStreamColMap))
	return plan, nil
}

// ConstructInvertedFilter is part of the exec.Factory interface.
func (e *distSQLSpecExecFactory) ConstructInvertedFilter(
	n exec.Node, invFilter *invertedexpr.SpanExpression, invColumn exec.NodeColumnOrdinal,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(
		47473, "experimental opt-driven distsql planning: inverted filter")
}

func (e *distSQLSpecExecFactory) ConstructSimpleProject(
	n exec.Node, cols []exec.NodeColumnOrdinal, colNames []string, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	// distSQLSpecExecFactory still constructs some of the planNodes (for
	// example, some variants of EXPLAIN), and we need to be able to add a
	// simple projection on top of them.
	if p, ok := n.(planNode); ok {
		return constructSimpleProjectForPlanNode(p, cols, colNames, reqOrdering)
	}
	physPlan, plan := getPhysPlan(n)
	projection := make([]uint32, len(cols))
	for i := range cols {
		projection[i] = uint32(cols[i])
	}
	physPlan.AddProjection(projection)
	physPlan.ResultColumns = getResultColumnsForSimpleProject(cols, colNames, physPlan.ResultTypes, physPlan.ResultColumns)
	physPlan.PlanToStreamColMap = identityMap(physPlan.PlanToStreamColMap, len(cols))
	if reqOrdering == nil {
		// When reqOrdering is nil, we're adding a top-level (i.e. "final")
		// projection. In such scenario we need to be careful to not simply
		// reset the merge ordering that is currently set on the plan - we do
		// so by merging the streams on the gateway node.
		physPlan.EnsureSingleStreamOnGateway()
	}
	physPlan.SetMergeOrdering(e.dsp.convertOrdering(ReqOrdering(reqOrdering), physPlan.PlanToStreamColMap))
	return plan, nil
}

func (e *distSQLSpecExecFactory) ConstructRender(
	n exec.Node,
	columns sqlbase.ResultColumns,
	exprs tree.TypedExprs,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	physPlan, plan := getPhysPlan(n)
	recommendation := e.checkExprsAndMaybeMergeLastStage(exprs, physPlan)
	if err := physPlan.AddRendering(
		exprs, e.getPlanCtx(recommendation), physPlan.PlanToStreamColMap, getTypesFromResultColumns(columns),
	); err != nil {
		return nil, err
	}
	physPlan.ResultColumns = columns
	physPlan.PlanToStreamColMap = identityMap(physPlan.PlanToStreamColMap, len(exprs))
	physPlan.SetMergeOrdering(e.dsp.convertOrdering(ReqOrdering(reqOrdering), physPlan.PlanToStreamColMap))
	return plan, nil
}

func (e *distSQLSpecExecFactory) ConstructApplyJoin(
	joinType sqlbase.JoinType,
	left exec.Node,
	rightColumns sqlbase.ResultColumns,
	onCond tree.TypedExpr,
	planRightSideFn exec.ApplyJoinPlanRightSideFn,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: apply join")
}

// TODO(yuzefovich): move the decision whether to use an interleaved join from
// the physical planner into the execbuilder.

func (e *distSQLSpecExecFactory) ConstructHashJoin(
	joinType sqlbase.JoinType,
	left, right exec.Node,
	leftEqCols, rightEqCols []exec.NodeColumnOrdinal,
	leftEqColsAreKey, rightEqColsAreKey bool,
	extraOnCond tree.TypedExpr,
) (exec.Node, error) {
	return e.constructHashOrMergeJoin(
		joinType, left, right, extraOnCond, leftEqCols, rightEqCols,
		leftEqColsAreKey, rightEqColsAreKey,
		ReqOrdering{} /* mergeJoinOrdering */, exec.OutputOrdering{}, /* reqOrdering */
	)
}

func (e *distSQLSpecExecFactory) ConstructMergeJoin(
	joinType sqlbase.JoinType,
	left, right exec.Node,
	onCond tree.TypedExpr,
	leftOrdering, rightOrdering sqlbase.ColumnOrdering,
	reqOrdering exec.OutputOrdering,
	leftEqColsAreKey, rightEqColsAreKey bool,
) (exec.Node, error) {
	leftEqCols, rightEqCols, mergeJoinOrdering, err := getEqualityIndicesAndMergeJoinOrdering(leftOrdering, rightOrdering)
	if err != nil {
		return nil, err
	}
	return e.constructHashOrMergeJoin(
		joinType, left, right, onCond, leftEqCols, rightEqCols,
		leftEqColsAreKey, rightEqColsAreKey, mergeJoinOrdering, reqOrdering,
	)
}

func populateAggFuncSpec(
	spec *execinfrapb.AggregatorSpec_Aggregation,
	funcName string,
	distinct bool,
	argCols []exec.NodeColumnOrdinal,
	constArgs []tree.Datum,
	filter exec.NodeColumnOrdinal,
	planCtx *PlanningCtx,
	physPlan *PhysicalPlan,
) (argumentsColumnTypes []*types.T, err error) {
	funcIdx, err := execinfrapb.GetAggregateFuncIdx(funcName)
	if err != nil {
		return nil, err
	}
	spec.Func = execinfrapb.AggregatorSpec_Func(funcIdx)
	spec.Distinct = distinct
	spec.ColIdx = make([]uint32, len(argCols))
	for i, col := range argCols {
		spec.ColIdx[i] = uint32(col)
	}
	if filter != tree.NoColumnIdx {
		filterColIdx := uint32(physPlan.PlanToStreamColMap[filter])
		spec.FilterColIdx = &filterColIdx
	}
	if len(constArgs) > 0 {
		spec.Arguments = make([]execinfrapb.Expression, len(constArgs))
		argumentsColumnTypes = make([]*types.T, len(constArgs))
		for k, argument := range constArgs {
			var err error
			spec.Arguments[k], err = physicalplan.MakeExpression(argument, planCtx, nil)
			if err != nil {
				return nil, err
			}
			argumentsColumnTypes[k] = argument.ResolvedType()
		}
	}
	return argumentsColumnTypes, nil
}

func (e *distSQLSpecExecFactory) constructAggregators(
	input exec.Node,
	groupCols []exec.NodeColumnOrdinal,
	groupColOrdering sqlbase.ColumnOrdering,
	aggregations []exec.AggInfo,
	reqOrdering exec.OutputOrdering,
	isScalar bool,
) (exec.Node, error) {
	physPlan, plan := getPhysPlan(input)
	// planAggregators() itself decides whether to distribute the aggregation.
	planCtx := e.getPlanCtx(shouldDistribute)
	aggregationSpecs := make([]execinfrapb.AggregatorSpec_Aggregation, len(groupCols)+len(aggregations))
	argumentsColumnTypes := make([][]*types.T, len(groupCols)+len(aggregations))
	var err error
	if len(groupCols) > 0 {
		argColsScratch := []exec.NodeColumnOrdinal{0}
		noFilter := exec.NodeColumnOrdinal(tree.NoColumnIdx)
		for i, col := range groupCols {
			spec := &aggregationSpecs[i]
			argColsScratch[0] = col
			_, err = populateAggFuncSpec(
				spec, builtins.AnyNotNull, false /* distinct*/, argColsScratch,
				nil /* constArgs */, noFilter, planCtx, physPlan,
			)
			if err != nil {
				return nil, err
			}
		}
	}
	for j := range aggregations {
		i := len(groupCols) + j
		spec := &aggregationSpecs[i]
		agg := &aggregations[j]
		argumentsColumnTypes[i], err = populateAggFuncSpec(
			spec, agg.FuncName, agg.Distinct, agg.ArgCols,
			agg.ConstArgs, agg.Filter, planCtx, physPlan,
		)
		if err != nil {
			return nil, err
		}
	}
	if err := e.dsp.planAggregators(
		planCtx,
		physPlan,
		&aggregatorPlanningInfo{
			aggregations:         aggregationSpecs,
			argumentsColumnTypes: argumentsColumnTypes,
			isScalar:             isScalar,
			groupCols:            convertOrdinalsToInts(groupCols),
			groupColOrdering:     groupColOrdering,
			inputMergeOrdering:   physPlan.MergeOrdering,
			reqOrdering:          ReqOrdering(reqOrdering),
		},
	); err != nil {
		return nil, err
	}
	physPlan.ResultColumns = getResultColumnsForGroupBy(physPlan.ResultColumns, groupCols, aggregations)
	return plan, nil
}

func (e *distSQLSpecExecFactory) ConstructGroupBy(
	input exec.Node,
	groupCols []exec.NodeColumnOrdinal,
	groupColOrdering sqlbase.ColumnOrdering,
	aggregations []exec.AggInfo,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return e.constructAggregators(
		input,
		groupCols,
		groupColOrdering,
		aggregations,
		reqOrdering,
		false, /* isScalar */
	)
}

func (e *distSQLSpecExecFactory) ConstructScalarGroupBy(
	input exec.Node, aggregations []exec.AggInfo,
) (exec.Node, error) {
	return e.constructAggregators(
		input,
		nil, /* groupCols */
		nil, /* groupColOrdering */
		aggregations,
		exec.OutputOrdering{}, /* reqOrdering */
		true,                  /* isScalar */
	)
}

func (e *distSQLSpecExecFactory) ConstructDistinct(
	input exec.Node,
	distinctCols, orderedCols exec.NodeColumnOrdinalSet,
	reqOrdering exec.OutputOrdering,
	nullsAreDistinct bool,
	errorOnDup string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: distinct")
}

func (e *distSQLSpecExecFactory) ConstructSetOp(
	typ tree.UnionType, all bool, left, right exec.Node,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: set op")
}

func (e *distSQLSpecExecFactory) ConstructSort(
	input exec.Node, ordering sqlbase.ColumnOrdering, alreadyOrderedPrefix int,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: sort")
}

func (e *distSQLSpecExecFactory) ConstructOrdinality(
	input exec.Node, colName string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: ordinality")
}

func (e *distSQLSpecExecFactory) ConstructIndexJoin(
	input exec.Node,
	table cat.Table,
	keyCols []exec.NodeColumnOrdinal,
	tableCols exec.TableColumnOrdinalSet,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: index join")
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
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: lookup join")
}

func (e *distSQLSpecExecFactory) ConstructInvertedJoin(
	joinType sqlbase.JoinType,
	invertedExpr tree.TypedExpr,
	input exec.Node,
	table cat.Table,
	index cat.Index,
	inputCol exec.NodeColumnOrdinal,
	lookupCols exec.TableColumnOrdinalSet,
	onCond tree.TypedExpr,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: geo lookup join")
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
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: zigzag join")
}

func (e *distSQLSpecExecFactory) ConstructLimit(
	input exec.Node, limitExpr, offsetExpr tree.TypedExpr,
) (exec.Node, error) {
	physPlan, plan := getPhysPlan(input)
	// Note that we pass in nil slice for exprs because we will evaluate both
	// expressions below, locally.
	recommendation := e.checkExprsAndMaybeMergeLastStage(nil /* exprs */, physPlan)
	count, offset, err := evalLimit(e.planner.EvalContext(), limitExpr, offsetExpr)
	if err != nil {
		return nil, err
	}
	if err = physPlan.AddLimit(count, offset, e.getPlanCtx(recommendation)); err != nil {
		return nil, err
	}
	// Since addition of limit and/or offset doesn't change any properties of
	// the physical plan, we don't need to update any of those (like
	// PlanToStreamColMap, etc).
	return plan, nil
}

func (e *distSQLSpecExecFactory) ConstructMax1Row(
	input exec.Node, errorText string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: max1row")
}

func (e *distSQLSpecExecFactory) ConstructProjectSet(
	n exec.Node, exprs tree.TypedExprs, zipCols sqlbase.ResultColumns, numColsPerGen []int,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: project set")
}

func (e *distSQLSpecExecFactory) ConstructWindow(
	input exec.Node, window exec.WindowInfo,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: window")
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
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: explain opt")
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
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: show trace")
}

func (e *distSQLSpecExecFactory) ConstructInsert(
	input exec.Node,
	table cat.Table,
	insertCols exec.TableColumnOrdinalSet,
	returnCols exec.TableColumnOrdinalSet,
	checkCols exec.CheckOrdinalSet,
	allowAutoCommit bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: insert")
}

func (e *distSQLSpecExecFactory) ConstructInsertFastPath(
	rows [][]tree.TypedExpr,
	table cat.Table,
	insertCols exec.TableColumnOrdinalSet,
	returnCols exec.TableColumnOrdinalSet,
	checkCols exec.CheckOrdinalSet,
	fkChecks []exec.InsertFastPathFKCheck,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: insert fast path")
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
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: update")
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
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: upsert")
}

func (e *distSQLSpecExecFactory) ConstructDelete(
	input exec.Node,
	table cat.Table,
	fetchCols exec.TableColumnOrdinalSet,
	returnCols exec.TableColumnOrdinalSet,
	allowAutoCommit bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: delete")
}

func (e *distSQLSpecExecFactory) ConstructDeleteRange(
	table cat.Table,
	needed exec.TableColumnOrdinalSet,
	indexConstraint *constraint.Constraint,
	interleavedTables []cat.Table,
	maxReturnedKeys int,
	allowAutoCommit bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: delete range")
}

func (e *distSQLSpecExecFactory) ConstructCreateTable(
	input exec.Node, schema cat.Schema, ct *tree.CreateTable,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: create table")
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
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: create view")
}

func (e *distSQLSpecExecFactory) ConstructSequenceSelect(sequence cat.Sequence) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: sequence select")
}

func (e *distSQLSpecExecFactory) ConstructSaveTable(
	input exec.Node, table *cat.DataSourceName, colNames []string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: save table")
}

func (e *distSQLSpecExecFactory) ConstructErrorIfRows(
	input exec.Node, mkErr func(tree.Datums) error,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: error if rows")
}

func (e *distSQLSpecExecFactory) ConstructOpaque(metadata opt.OpaqueMetadata) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: opaque")
}

func (e *distSQLSpecExecFactory) ConstructAlterTableSplit(
	index cat.Index, input exec.Node, expiration tree.TypedExpr,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: alter table split")
}

func (e *distSQLSpecExecFactory) ConstructAlterTableUnsplit(
	index cat.Index, input exec.Node,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: alter table unsplit")
}

func (e *distSQLSpecExecFactory) ConstructAlterTableUnsplitAll(index cat.Index) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: alter table unsplit all")
}

func (e *distSQLSpecExecFactory) ConstructAlterTableRelocate(
	index cat.Index, input exec.Node, relocateLease bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: alter table relocate")
}

func (e *distSQLSpecExecFactory) ConstructBuffer(
	input exec.Node, label string,
) (exec.BufferNode, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: buffer")
}

func (e *distSQLSpecExecFactory) ConstructScanBuffer(
	ref exec.BufferNode, label string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: scan buffer")
}

func (e *distSQLSpecExecFactory) ConstructRecursiveCTE(
	initial exec.Node, fn exec.RecursiveCTEIterationFn, label string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: recursive CTE")
}

func (e *distSQLSpecExecFactory) ConstructControlJobs(
	command tree.JobCommand, input exec.Node,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: control jobs")
}

func (e *distSQLSpecExecFactory) ConstructCancelQueries(
	input exec.Node, ifExists bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: cancel queries")
}

func (e *distSQLSpecExecFactory) ConstructCancelSessions(
	input exec.Node, ifExists bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: cancel sessions")
}

func (e *distSQLSpecExecFactory) ConstructExport(
	input exec.Node, fileName tree.TypedExpr, fileFormat string, options []exec.KVOption,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: export")
}

func getPhysPlan(n exec.Node) (*PhysicalPlan, planMaybePhysical) {
	plan := n.(planMaybePhysical)
	return plan.physPlan.PhysicalPlan, plan
}

func (e *distSQLSpecExecFactory) constructHashOrMergeJoin(
	joinType sqlbase.JoinType,
	left, right exec.Node,
	onCond tree.TypedExpr,
	leftEqCols, rightEqCols []exec.NodeColumnOrdinal,
	leftEqColsAreKey, rightEqColsAreKey bool,
	mergeJoinOrdering sqlbase.ColumnOrdering,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	leftPhysPlan, leftPlan := getPhysPlan(left)
	rightPhysPlan, rightPlan := getPhysPlan(right)
	resultColumns := getJoinResultColumns(joinType, leftPhysPlan.ResultColumns, rightPhysPlan.ResultColumns)
	leftMap, rightMap := leftPhysPlan.PlanToStreamColMap, rightPhysPlan.PlanToStreamColMap
	helper := &joinPlanningHelper{
		numLeftCols:             len(leftPhysPlan.ResultColumns),
		numRightCols:            len(rightPhysPlan.ResultColumns),
		leftPlanToStreamColMap:  leftMap,
		rightPlanToStreamColMap: rightMap,
	}
	post, joinToStreamColMap := helper.joinOutColumns(joinType, resultColumns)
	// We always try to distribute the join, but planJoiners() itself might
	// decide not to.
	onExpr, err := helper.remapOnExpr(e.getPlanCtx(shouldDistribute), onCond)
	if err != nil {
		return nil, err
	}

	leftEqColsRemapped := eqCols(leftEqCols, leftMap)
	rightEqColsRemapped := eqCols(rightEqCols, rightMap)
	p := e.dsp.planJoiners(&joinPlanningInfo{
		leftPlan:              leftPhysPlan,
		rightPlan:             rightPhysPlan,
		joinType:              joinType,
		joinResultTypes:       getTypesFromResultColumns(resultColumns),
		onExpr:                onExpr,
		post:                  post,
		joinToStreamColMap:    joinToStreamColMap,
		leftEqCols:            leftEqColsRemapped,
		rightEqCols:           rightEqColsRemapped,
		leftEqColsAreKey:      leftEqColsAreKey,
		rightEqColsAreKey:     rightEqColsAreKey,
		leftMergeOrd:          distsqlOrdering(mergeJoinOrdering, leftEqColsRemapped),
		rightMergeOrd:         distsqlOrdering(mergeJoinOrdering, rightEqColsRemapped),
		leftPlanDistribution:  leftPhysPlan.Distribution,
		rightPlanDistribution: rightPhysPlan.Distribution,
	}, ReqOrdering(reqOrdering))
	p.ResultColumns = resultColumns
	return planMaybePhysical{physPlan: &physicalPlanTop{
		PhysicalPlan:     p,
		planNodesToClose: append(leftPlan.physPlan.planNodesToClose, rightPlan.physPlan.planNodesToClose...),
	}}, nil
}
