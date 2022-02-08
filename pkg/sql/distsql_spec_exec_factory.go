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
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

type distSQLSpecExecFactory struct {
	planner *planner
	dsp     *DistSQLPlanner
	// planCtx should not be used directly - getPlanCtx() should be used instead.
	planCtx              *PlanningCtx
	singleTenant         bool
	planningMode         distSQLPlanningMode
	gatewaySQLInstanceID base.SQLInstanceID
}

var _ exec.Factory = &distSQLSpecExecFactory{}

// distSQLPlanningMode indicates the planning mode in which
// distSQLSpecExecFactory is operating.
type distSQLPlanningMode int

const (
	// distSQLDefaultPlanning is the default planning mode in which the factory
	// can create a physical plan with any plan distribution (local, partially
	// distributed, or fully distributed).
	distSQLDefaultPlanning distSQLPlanningMode = iota
	// distSQLLocalOnlyPlanning is the planning mode in which the factory
	// only creates local physical plans.
	distSQLLocalOnlyPlanning
)

func newDistSQLSpecExecFactory(p *planner, planningMode distSQLPlanningMode) exec.Factory {
	e := &distSQLSpecExecFactory{
		planner:              p,
		dsp:                  p.extendedEvalCtx.DistSQLPlanner,
		singleTenant:         p.execCfg.Codec.ForSystemTenant(),
		planningMode:         planningMode,
		gatewaySQLInstanceID: p.extendedEvalCtx.DistSQLPlanner.gatewaySQLInstanceID,
	}
	distribute := DistributionType(DistributionTypeNone)
	if e.planningMode != distSQLLocalOnlyPlanning {
		distribute = DistributionTypeSystemTenantOnly
	}
	evalCtx := p.ExtendedEvalContext()
	e.planCtx = e.dsp.NewPlanningCtx(evalCtx.Context, evalCtx, e.planner,
		e.planner.txn, distribute)
	return e
}

func (e *distSQLSpecExecFactory) getPlanCtx(recommendation distRecommendation) *PlanningCtx {
	distribute := false
	if e.singleTenant && e.planningMode != distSQLLocalOnlyPlanning {
		distribute = shouldDistributeGivenRecAndMode(
			recommendation, e.planner.extendedEvalCtx.SessionData().DistSQLMode,
		)
	}
	e.planCtx.isLocal = !distribute
	return e.planCtx
}

// TODO(yuzefovich): consider adding machinery that would confirm that
// assumptions that the execution makes about the plans coming from the
// optimizer are satisfied. A couple of examples are making sure that the
// constants in comparison expressions are always on the right and that the
// tuples in IN clause are sorted.

func (e *distSQLSpecExecFactory) ConstructValues(
	rows [][]tree.TypedExpr, cols colinfo.ResultColumns,
) (exec.Node, error) {
	if (len(cols) == 0 && len(rows) == 1) || len(rows) == 0 {
		planCtx := e.getPlanCtx(canDistribute)
		colTypes := getTypesFromResultColumns(cols)
		spec := e.dsp.createValuesSpec(planCtx, colTypes, len(rows), nil /* rawBytes */)
		physPlan, err := e.dsp.createValuesPlan(planCtx, spec, colTypes)
		if err != nil {
			return nil, err
		}
		physPlan.ResultColumns = cols
		return makePlanMaybePhysical(physPlan, nil /* planNodesToClose */), nil
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
		physPlan, err = e.dsp.wrapPlan(planCtx, v, e.planningMode != distSQLLocalOnlyPlanning)
	} else {
		// We can create a spec for the values processor, so we don't create a
		// valuesNode.
		colTypes := getTypesFromResultColumns(cols)
		var spec *execinfrapb.ValuesCoreSpec
		spec, err = e.dsp.createValuesSpecFromTuples(planCtx, rows, colTypes)
		if err != nil {
			return nil, err
		}
		physPlan, err = e.dsp.createValuesPlan(planCtx, spec, colTypes)
	}
	if err != nil {
		return nil, err
	}
	physPlan.ResultColumns = cols
	return makePlanMaybePhysical(physPlan, planNodesToClose), nil
}

// ConstructScan implements exec.Factory interface by combining the logic that
// performs scanNode creation of execFactory.ConstructScan and physical
// planning of table readers of DistSQLPlanner.createTableReaders.
func (e *distSQLSpecExecFactory) ConstructScan(
	table cat.Table, index cat.Index, params exec.ScanParams, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	if table.IsVirtualTable() {
		return constructVirtualScan(
			e, e.planner, table, index, params, reqOrdering,
			func(d *delayedNode) (exec.Node, error) {
				physPlan, err := e.dsp.wrapPlan(e.getPlanCtx(cannotDistribute), d, e.planningMode != distSQLLocalOnlyPlanning)
				if err != nil {
					return nil, err
				}
				physPlan.ResultColumns = d.columns
				return makePlanMaybePhysical(physPlan, []planNode{d}), nil
			},
		)
	}

	// Although we don't yet recommend distributing plans where soft limits
	// propagate to scan nodes because we don't have infrastructure to only
	// plan for a few ranges at a time, the propagation of the soft limits
	// to scan nodes has been added in 20.1 release, so to keep the
	// previous behavior we continue to ignore the soft limits for now.
	// TODO(yuzefovich): pay attention to the soft limits.
	recommendation := canDistribute
	if params.LocalityOptimized {
		recommendation = recommendation.compose(cannotDistribute)
	}
	planCtx := e.getPlanCtx(recommendation)
	p := planCtx.NewPhysicalPlan()

	// Phase 1: set up all necessary infrastructure for table reader planning
	// below. This phase is equivalent to what execFactory.ConstructScan does.
	tabDesc := table.(*optTable).desc
	idx := index.(*optIndex).idx
	colCfg := makeScanColumnsConfig(table, params.NeededCols)

	var sb span.Builder
	sb.Init(e.planner.EvalContext(), e.planner.ExecCfg().Codec, tabDesc, idx)

	cols := make([]catalog.Column, 0, params.NeededCols.Len())
	allCols := tabDesc.AllColumns()
	for ord, ok := params.NeededCols.Next(0); ok; ord, ok = params.NeededCols.Next(ord + 1) {
		cols = append(cols, allCols[ord])
	}
	columnIDs := make([]descpb.ColumnID, len(cols))
	for i := range cols {
		columnIDs[i] = cols[i].GetID()
	}

	p.ResultColumns = colinfo.ResultColumnsFromColumns(tabDesc.GetID(), cols)

	if params.IndexConstraint != nil && params.IndexConstraint.IsContradiction() {
		// Note that empty rows argument is handled by ConstructValues first -
		// it will always create an appropriate values processor spec, so there
		// will be no planNodes created (which is what we want in this case).
		return e.ConstructValues([][]tree.TypedExpr{} /* rows */, p.ResultColumns)
	}

	var spans roachpb.Spans
	var err error
	if params.InvertedConstraint != nil {
		spans, err = sb.SpansFromInvertedSpans(params.InvertedConstraint, params.IndexConstraint, nil /* scratch */)
	} else {
		splitter := span.MakeSplitter(tabDesc, idx, params.NeededCols)
		spans, err = sb.SpansFromConstraint(params.IndexConstraint, splitter)
	}
	if err != nil {
		return nil, err
	}

	isFullTableOrIndexScan := len(spans) == 1 && spans[0].EqualValue(
		tabDesc.IndexSpan(e.planner.ExecCfg().Codec, idx.GetID()),
	)
	if err = colCfg.assertValidReqOrdering(reqOrdering); err != nil {
		return nil, err
	}

	// Check if we are doing a full scan.
	if isFullTableOrIndexScan {
		recommendation = recommendation.compose(shouldDistribute)
	}

	// Phase 2: perform the table reader planning. This phase is equivalent to
	// what DistSQLPlanner.createTableReaders does.
	trSpec := physicalplan.NewTableReaderSpec()
	*trSpec = execinfrapb.TableReaderSpec{
		Reverse:                         params.Reverse,
		TableDescriptorModificationTime: tabDesc.GetModificationTime(),
	}
	if err := rowenc.InitIndexFetchSpec(&trSpec.FetchSpec, e.planner.ExecCfg().Codec, tabDesc, idx, columnIDs); err != nil {
		return nil, err
	}
	if params.Locking != nil {
		trSpec.LockingStrength = descpb.ToScanLockingStrength(params.Locking.Strength)
		trSpec.LockingWaitPolicy = descpb.ToScanLockingWaitPolicy(params.Locking.WaitPolicy)
		if trSpec.LockingStrength != descpb.ScanLockingStrength_FOR_NONE {
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
	if params.HardLimit != 0 {
		post.Limit = uint64(params.HardLimit)
	} else if params.SoftLimit != 0 {
		trSpec.LimitHint = params.SoftLimit
	}

	err = e.dsp.planTableReaders(
		e.getPlanCtx(recommendation),
		p,
		&tableReaderPlanningInfo{
			spec:              trSpec,
			post:              post,
			desc:              tabDesc,
			spans:             spans,
			reverse:           params.Reverse,
			parallelize:       params.Parallelize,
			estimatedRowCount: uint64(params.EstimatedRowCount),
			reqOrdering:       ReqOrdering(reqOrdering),
		},
	)

	return makePlanMaybePhysical(p, nil /* planNodesToClose */), err
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
	n exec.Node,
	invFilter *inverted.SpanExpression,
	preFiltererExpr tree.TypedExpr,
	preFiltererType *types.T,
	invColumn exec.NodeColumnOrdinal,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(
		47473, "experimental opt-driven distsql planning: inverted filter")
}

func (e *distSQLSpecExecFactory) ConstructSimpleProject(
	n exec.Node, cols []exec.NodeColumnOrdinal, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	physPlan, plan := getPhysPlan(n)
	projection := make([]uint32, len(cols))
	for i := range cols {
		projection[i] = uint32(cols[physPlan.PlanToStreamColMap[i]])
	}
	newColMap := identityMap(physPlan.PlanToStreamColMap, len(cols))
	physPlan.AddProjection(
		projection,
		e.dsp.convertOrdering(ReqOrdering(reqOrdering), newColMap),
	)
	physPlan.ResultColumns = getResultColumnsForSimpleProject(
		cols, nil /* colNames */, physPlan.GetResultTypes(), physPlan.ResultColumns,
	)
	physPlan.PlanToStreamColMap = newColMap
	return plan, nil
}

func (e *distSQLSpecExecFactory) ConstructSerializingProject(
	n exec.Node, cols []exec.NodeColumnOrdinal, colNames []string,
) (exec.Node, error) {
	physPlan, plan := getPhysPlan(n)
	physPlan.EnsureSingleStreamOnGateway()
	projection := make([]uint32, len(cols))
	for i := range cols {
		projection[i] = uint32(cols[physPlan.PlanToStreamColMap[i]])
	}
	physPlan.AddProjection(projection, execinfrapb.Ordering{})
	physPlan.ResultColumns = getResultColumnsForSimpleProject(cols, colNames, physPlan.GetResultTypes(), physPlan.ResultColumns)
	physPlan.PlanToStreamColMap = identityMap(physPlan.PlanToStreamColMap, len(cols))
	return plan, nil
}

func (e *distSQLSpecExecFactory) ConstructRender(
	n exec.Node,
	columns colinfo.ResultColumns,
	exprs tree.TypedExprs,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	physPlan, plan := getPhysPlan(n)
	recommendation := e.checkExprsAndMaybeMergeLastStage(exprs, physPlan)

	newColMap := identityMap(physPlan.PlanToStreamColMap, len(exprs))
	if err := physPlan.AddRendering(
		exprs, e.getPlanCtx(recommendation), physPlan.PlanToStreamColMap, getTypesFromResultColumns(columns),
		e.dsp.convertOrdering(ReqOrdering(reqOrdering), newColMap),
	); err != nil {
		return nil, err
	}
	physPlan.ResultColumns = columns
	physPlan.PlanToStreamColMap = newColMap
	return plan, nil
}

func (e *distSQLSpecExecFactory) ConstructApplyJoin(
	joinType descpb.JoinType,
	left exec.Node,
	rightColumns colinfo.ResultColumns,
	onCond tree.TypedExpr,
	planRightSideFn exec.ApplyJoinPlanRightSideFn,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: apply join")
}

func (e *distSQLSpecExecFactory) ConstructHashJoin(
	joinType descpb.JoinType,
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
	joinType descpb.JoinType,
	left, right exec.Node,
	onCond tree.TypedExpr,
	leftOrdering, rightOrdering colinfo.ColumnOrdering,
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
	groupColOrdering colinfo.ColumnOrdering,
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
			groupCols:            convertNodeOrdinalsToInts(groupCols),
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
	groupColOrdering colinfo.ColumnOrdering,
	aggregations []exec.AggInfo,
	reqOrdering exec.OutputOrdering,
	groupingOrderType exec.GroupingOrderType,
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
	physPlan, plan := getPhysPlan(input)
	spec := e.dsp.createDistinctSpec(
		convertFastIntSetToUint32Slice(distinctCols),
		convertFastIntSetToUint32Slice(orderedCols),
		nullsAreDistinct,
		errorOnDup,
		e.dsp.convertOrdering(ReqOrdering(reqOrdering), physPlan.PlanToStreamColMap),
	)
	e.dsp.addDistinctProcessors(physPlan, spec)
	// Since addition of distinct processors doesn't change any properties of
	// the physical plan, we don't need to update any of those.
	return plan, nil
}

// ConstructHashSetOp is part of the exec.Factory interface.
func (e *distSQLSpecExecFactory) ConstructHashSetOp(
	typ tree.UnionType, all bool, left, right exec.Node,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: hash set op")
}

// ConstructStreamingSetOp is part of the exec.Factory interface.
func (e *distSQLSpecExecFactory) ConstructStreamingSetOp(
	typ tree.UnionType,
	all bool,
	left, right exec.Node,
	streamingOrdering colinfo.ColumnOrdering,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: streaming set op")
}

// ConstructUnionAll is part of the exec.Factory interface.
func (e *distSQLSpecExecFactory) ConstructUnionAll(
	left, right exec.Node, reqOrdering exec.OutputOrdering, hardLimit uint64,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: union all")
}

func (e *distSQLSpecExecFactory) ConstructSort(
	input exec.Node, ordering exec.OutputOrdering, alreadyOrderedPrefix int,
) (exec.Node, error) {
	physPlan, plan := getPhysPlan(input)
	e.dsp.addSorters(physPlan, colinfo.ColumnOrdering(ordering), alreadyOrderedPrefix, 0 /* limit */)
	// Since addition of sorters doesn't change any properties of the physical
	// plan, we don't need to update any of those.
	return plan, nil
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
	joinType descpb.JoinType,
	input exec.Node,
	table cat.Table,
	index cat.Index,
	eqCols []exec.NodeColumnOrdinal,
	eqColsAreKey bool,
	lookupExpr tree.TypedExpr,
	remoteLookupExpr tree.TypedExpr,
	lookupCols exec.TableColumnOrdinalSet,
	onCond tree.TypedExpr,
	isFirstJoinInPairedJoiner bool,
	isSecondJoinInPairedJoiner bool,
	reqOrdering exec.OutputOrdering,
	locking *tree.LockingItem,
) (exec.Node, error) {
	// TODO (rohany): Implement production of system columns by the underlying scan here.
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: lookup join")
}

func (e *distSQLSpecExecFactory) ConstructInvertedJoin(
	joinType descpb.JoinType,
	invertedExpr tree.TypedExpr,
	input exec.Node,
	table cat.Table,
	index cat.Index,
	prefixEqCols []exec.NodeColumnOrdinal,
	lookupCols exec.TableColumnOrdinalSet,
	onCond tree.TypedExpr,
	isFirstJoinInPairedJoiner bool,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: inverted join")
}

func (e *distSQLSpecExecFactory) constructZigzagJoinSide(
	planCtx *PlanningCtx,
	table cat.Table,
	index cat.Index,
	wantedCols exec.TableColumnOrdinalSet,
	fixedVals []tree.TypedExpr,
	eqCols []exec.TableColumnOrdinal,
) (zigzagPlanningSide, error) {
	desc := table.(*optTable).desc
	colCfg := scanColumnsConfig{wantedColumns: make([]tree.ColumnID, 0, wantedCols.Len())}
	for c, ok := wantedCols.Next(0); ok; c, ok = wantedCols.Next(c + 1) {
		colCfg.wantedColumns = append(colCfg.wantedColumns, desc.PublicColumns()[c].GetID())
	}
	cols, err := initColsForScan(desc, colCfg)
	if err != nil {
		return zigzagPlanningSide{}, err
	}
	typs := make([]*types.T, len(fixedVals))
	for i := range typs {
		typs[i] = fixedVals[i].ResolvedType()
	}
	valuesSpec, err := e.dsp.createValuesSpecFromTuples(planCtx, [][]tree.TypedExpr{fixedVals}, typs)
	if err != nil {
		return zigzagPlanningSide{}, err
	}

	// TODO (cucaroach): update indexUsageStats.

	return zigzagPlanningSide{
		desc:        desc,
		index:       index.(*optIndex).idx,
		cols:        cols,
		eqCols:      convertTableOrdinalsToInts(eqCols),
		fixedValues: valuesSpec,
	}, nil
}

func (e *distSQLSpecExecFactory) ConstructZigzagJoin(
	leftTable cat.Table,
	leftIndex cat.Index,
	leftCols exec.TableColumnOrdinalSet,
	leftFixedVals []tree.TypedExpr,
	leftEqCols []exec.TableColumnOrdinal,
	rightTable cat.Table,
	rightIndex cat.Index,
	rightCols exec.TableColumnOrdinalSet,
	rightFixedVals []tree.TypedExpr,
	rightEqCols []exec.TableColumnOrdinal,
	onCond tree.TypedExpr,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	// Because we cannot distribute we don't need to check the onCond and fixedValue exprs
	// with checkExpr but we would need to if we ever try to distribute ZZ joins.
	planCtx := e.getPlanCtx(cannotDistribute)

	sides := make([]zigzagPlanningSide, 2)
	var err error
	sides[0], err = e.constructZigzagJoinSide(planCtx, leftTable, leftIndex, leftCols, leftFixedVals, leftEqCols)
	if err != nil {
		return nil, err
	}
	sides[1], err = e.constructZigzagJoinSide(planCtx, rightTable, rightIndex, rightCols, rightFixedVals, rightEqCols)
	if err != nil {
		return nil, err
	}

	leftResultColumns := colinfo.ResultColumnsFromColumns(sides[0].desc.GetID(), sides[0].cols)
	rightResultColumns := colinfo.ResultColumnsFromColumns(sides[1].desc.GetID(), sides[1].cols)
	resultColumns := make(colinfo.ResultColumns, 0, len(leftResultColumns)+len(rightResultColumns))
	resultColumns = append(resultColumns, leftResultColumns...)
	resultColumns = append(resultColumns, rightResultColumns...)
	p, err := e.dsp.planZigzagJoin(planCtx, zigzagPlanningInfo{
		sides:       sides,
		columns:     resultColumns,
		onCond:      onCond,
		reqOrdering: ReqOrdering(reqOrdering),
	})
	if err != nil {
		return nil, err
	}
	p.ResultColumns = resultColumns
	return makePlanMaybePhysical(p, nil /* planNodesToClose */), nil
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

func (e *distSQLSpecExecFactory) ConstructTopK(
	input exec.Node, k int64, ordering exec.OutputOrdering, alreadyOrderedPrefix int,
) (exec.Node, error) {
	physPlan, plan := getPhysPlan(input)
	if k <= 0 {
		return nil, errors.New("negative or zero value for LIMIT")
	}
	// No already ordered prefix.
	e.dsp.addSorters(physPlan, colinfo.ColumnOrdering(ordering), alreadyOrderedPrefix, k)
	// Since addition of topk doesn't change any properties of
	// the physical plan, we don't need to update any of those.
	return plan, nil
}

func (e *distSQLSpecExecFactory) ConstructMax1Row(
	input exec.Node, errorText string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: max1row")
}

func (e *distSQLSpecExecFactory) ConstructProjectSet(
	n exec.Node, exprs tree.TypedExprs, zipCols colinfo.ResultColumns, numColsPerGen []int,
) (exec.Node, error) {
	physPlan, plan := getPhysPlan(n)
	cols := append(plan.physPlan.ResultColumns, zipCols...)
	err := e.dsp.addProjectSet(
		physPlan,
		// Currently, projectSetProcessors are always planned as a "grouping"
		// stage (meaning a single processor on the gateway), so we use
		// cannotDistribute as the recommendation.
		e.getPlanCtx(cannotDistribute),
		&projectSetPlanningInfo{
			columns:         cols,
			numColsInSource: len(plan.physPlan.ResultColumns),
			exprs:           exprs,
			numColsPerGen:   numColsPerGen,
		},
	)
	if err != nil {
		return nil, err
	}
	physPlan.ResultColumns = cols
	return plan, nil
}

func (e *distSQLSpecExecFactory) ConstructWindow(
	input exec.Node, window exec.WindowInfo,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: window")
}

func (e *distSQLSpecExecFactory) ConstructPlan(
	root exec.Node,
	subqueries []exec.Subquery,
	cascades []exec.Cascade,
	checks []exec.Node,
	rootRowCount int64,
) (exec.Plan, error) {
	if len(subqueries) != 0 {
		return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: subqueries")
	}
	if len(cascades) != 0 {
		return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: cascades")
	}
	if len(checks) != 0 {
		return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: checks")
	}
	return constructPlan(e.planner, root, subqueries, cascades, checks, rootRowCount)
}

func (e *distSQLSpecExecFactory) ConstructExplainOpt(
	plan string, envOpts exec.ExplainEnvData,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: explain opt")
}

func (e *distSQLSpecExecFactory) ConstructExplain(
	options *tree.ExplainOptions,
	stmtType tree.StatementReturnType,
	buildFn exec.BuildPlanForExplainFn,
) (exec.Node, error) {
	if options.Flags[tree.ExplainFlagEnv] {
		return nil, errors.New("ENV only supported with (OPT) option")
	}

	// We cannot create the explained plan in the same PlanInfrastructure with the
	// "outer" plan. Create a separate factory.
	newFactory := newDistSQLSpecExecFactory(e.planner, e.planningMode)
	plan, err := buildFn(newFactory)
	// Release the resources acquired during the physical planning right away.
	newFactory.(*distSQLSpecExecFactory).planCtx.getCleanupFunc()()
	if err != nil {
		return nil, err
	}

	p := plan.(*explain.Plan).WrappedPlan.(*planComponents)
	var explainNode planNode
	if options.Mode == tree.ExplainVec {
		explainNode = &explainVecNode{
			options: options,
			plan:    *p,
		}
	} else if options.Mode == tree.ExplainDDL {
		explainNode = &explainDDLNode{
			options: options,
			plan:    *p,
		}
	} else {
		explainNode = &explainPlanNode{
			options: options,
			flags:   explain.MakeFlags(options),
			plan:    plan.(*explain.Plan),
		}
	}

	physPlan, err := e.dsp.wrapPlan(e.getPlanCtx(cannotDistribute), explainNode, e.planningMode != distSQLLocalOnlyPlanning)
	if err != nil {
		return nil, err
	}
	physPlan.ResultColumns = planColumns(explainNode)
	// Plan distribution of an explain node is considered to be the same as of
	// the query being explained.
	// TODO(yuzefovich): we might also need to look at the distribution of
	// subqueries and postqueries.
	physPlan.Distribution = p.main.physPlan.Distribution
	return makePlanMaybePhysical(physPlan, []planNode{explainNode}), nil
}

func (e *distSQLSpecExecFactory) ConstructShowTrace(
	typ tree.ShowTraceType, compact bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: show trace")
}

func (e *distSQLSpecExecFactory) ConstructInsert(
	input exec.Node,
	table cat.Table,
	arbiterIndexes cat.IndexOrdinals,
	arbiterConstraints cat.UniqueOrdinals,
	insertCols exec.TableColumnOrdinalSet,
	returnCols exec.TableColumnOrdinalSet,
	checkCols exec.CheckOrdinalSet,
	autoCommit bool,
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
	autoCommit bool,
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
	passthrough colinfo.ResultColumns,
	autoCommit bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: update")
}

func (e *distSQLSpecExecFactory) ConstructUpsert(
	input exec.Node,
	table cat.Table,
	arbiterIndexes cat.IndexOrdinals,
	arbiterConstraints cat.UniqueOrdinals,
	canaryCol exec.NodeColumnOrdinal,
	insertCols exec.TableColumnOrdinalSet,
	fetchCols exec.TableColumnOrdinalSet,
	updateCols exec.TableColumnOrdinalSet,
	returnCols exec.TableColumnOrdinalSet,
	checks exec.CheckOrdinalSet,
	autoCommit bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: upsert")
}

func (e *distSQLSpecExecFactory) ConstructDelete(
	input exec.Node,
	table cat.Table,
	fetchCols exec.TableColumnOrdinalSet,
	returnCols exec.TableColumnOrdinalSet,
	autoCommit bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: delete")
}

func (e *distSQLSpecExecFactory) ConstructDeleteRange(
	table cat.Table,
	needed exec.TableColumnOrdinalSet,
	indexConstraint *constraint.Constraint,
	autoCommit bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: delete range")
}

func (e *distSQLSpecExecFactory) ConstructCreateTable(
	schema cat.Schema, ct *tree.CreateTable,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: create table")
}

func (e *distSQLSpecExecFactory) ConstructCreateTableAs(
	input exec.Node, schema cat.Schema, ct *tree.CreateTable,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: create table")
}

func (e *distSQLSpecExecFactory) ConstructCreateView(
	schema cat.Schema,
	viewName *cat.DataSourceName,
	ifNotExists bool,
	replace bool,
	persistence tree.Persistence,
	materialized bool,
	viewQuery string,
	columns colinfo.ResultColumns,
	deps opt.ViewDeps,
	typeDeps opt.ViewTypeDeps,
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
	input exec.Node, mkErr exec.MkErrFn,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: error if rows")
}

func (e *distSQLSpecExecFactory) ConstructOpaque(metadata opt.OpaqueMetadata) (exec.Node, error) {
	plan, err := constructOpaque(metadata)
	if err != nil {
		return nil, err
	}
	physPlan, err := e.dsp.wrapPlan(e.getPlanCtx(cannotDistribute), plan, e.planningMode != distSQLLocalOnlyPlanning)
	if err != nil {
		return nil, err
	}
	physPlan.ResultColumns = planColumns(plan)
	return makePlanMaybePhysical(physPlan, []planNode{plan}), nil
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
	index cat.Index, input exec.Node, relocateSubject tree.RelocateSubject,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: alter table relocate")
}

func (e *distSQLSpecExecFactory) ConstructAlterRangeRelocate(
	input exec.Node,
	relocateSubject tree.RelocateSubject,
	toStoreID tree.TypedExpr,
	fromStoreID tree.TypedExpr,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: alter range relocate")
}

func (e *distSQLSpecExecFactory) ConstructBuffer(input exec.Node, label string) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: buffer")
}

func (e *distSQLSpecExecFactory) ConstructScanBuffer(
	ref exec.Node, label string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: scan buffer")
}

func (e *distSQLSpecExecFactory) ConstructRecursiveCTE(
	initial exec.Node, fn exec.RecursiveCTEIterationFn, label string, deduplicate bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: recursive CTE")
}

func (e *distSQLSpecExecFactory) ConstructControlJobs(
	command tree.JobCommand, input exec.Node, reason tree.TypedExpr,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: control jobs")
}

func (e *distSQLSpecExecFactory) ConstructControlSchedules(
	command tree.ScheduleCommand, input exec.Node,
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

func (e *distSQLSpecExecFactory) ConstructCreateStatistics(
	cs *tree.CreateStats,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: create statistics")
}

func (e *distSQLSpecExecFactory) ConstructExport(
	input exec.Node,
	fileName tree.TypedExpr,
	fileFormat string,
	options []exec.KVOption,
	notNullColsSet exec.NodeColumnOrdinalSet,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: export")
}

func getPhysPlan(n exec.Node) (*PhysicalPlan, planMaybePhysical) {
	plan := n.(planMaybePhysical)
	return plan.physPlan.PhysicalPlan, plan
}

func (e *distSQLSpecExecFactory) constructHashOrMergeJoin(
	joinType descpb.JoinType,
	left, right exec.Node,
	onCond tree.TypedExpr,
	leftEqCols, rightEqCols []exec.NodeColumnOrdinal,
	leftEqColsAreKey, rightEqColsAreKey bool,
	mergeJoinOrdering colinfo.ColumnOrdering,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	leftPhysPlan, leftPlan := getPhysPlan(left)
	rightPhysPlan, rightPlan := getPhysPlan(right)
	resultColumns := getJoinResultColumns(joinType, leftPhysPlan.ResultColumns, rightPhysPlan.ResultColumns)
	leftMap, rightMap := leftPhysPlan.PlanToStreamColMap, rightPhysPlan.PlanToStreamColMap
	helper := &joinPlanningHelper{
		numLeftOutCols:          len(leftPhysPlan.GetResultTypes()),
		numRightOutCols:         len(rightPhysPlan.GetResultTypes()),
		numAllLeftCols:          len(leftPhysPlan.GetResultTypes()),
		leftPlanToStreamColMap:  leftMap,
		rightPlanToStreamColMap: rightMap,
	}
	post, joinToStreamColMap := helper.joinOutColumns(joinType, resultColumns)
	// We always try to distribute the join, but planJoiners() itself might
	// decide not to.
	planCtx := e.getPlanCtx(shouldDistribute)
	onExpr, err := helper.remapOnExpr(planCtx, onCond)
	if err != nil {
		return nil, err
	}

	leftEqColsRemapped := eqCols(leftEqCols, leftMap)
	rightEqColsRemapped := eqCols(rightEqCols, rightMap)
	info := joinPlanningInfo{
		leftPlan:                 leftPhysPlan,
		rightPlan:                rightPhysPlan,
		joinType:                 joinType,
		joinResultTypes:          getTypesFromResultColumns(resultColumns),
		onExpr:                   onExpr,
		post:                     post,
		joinToStreamColMap:       joinToStreamColMap,
		leftEqCols:               leftEqColsRemapped,
		rightEqCols:              rightEqColsRemapped,
		leftEqColsAreKey:         leftEqColsAreKey,
		rightEqColsAreKey:        rightEqColsAreKey,
		leftMergeOrd:             distsqlOrdering(mergeJoinOrdering, leftEqColsRemapped),
		rightMergeOrd:            distsqlOrdering(mergeJoinOrdering, rightEqColsRemapped),
		leftPlanDistribution:     leftPhysPlan.Distribution,
		rightPlanDistribution:    rightPhysPlan.Distribution,
		allowPartialDistribution: e.planningMode != distSQLLocalOnlyPlanning,
	}
	p := e.dsp.planJoiners(planCtx, &info, ReqOrdering(reqOrdering))
	p.ResultColumns = resultColumns
	return makePlanMaybePhysical(p, append(leftPlan.physPlan.planNodesToClose, rightPlan.physPlan.planNodesToClose...)), nil
}
