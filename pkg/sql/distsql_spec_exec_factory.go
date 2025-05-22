// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

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
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type distSQLSpecExecFactory struct {
	ctx     context.Context
	planner *planner
	dsp     *DistSQLPlanner
	// planCtx should not be used directly - getPlanCtx() should be used instead.
	planCtx              *PlanningCtx
	singleTenant         bool
	planningMode         distSQLPlanningMode
	gatewaySQLInstanceID base.SQLInstanceID
	distSQLVisitor       distSQLExprCheckVisitor
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

func newDistSQLSpecExecFactory(
	ctx context.Context, p *planner, planningMode distSQLPlanningMode,
) exec.Factory {
	e := &distSQLSpecExecFactory{
		ctx:                  ctx,
		planner:              p,
		dsp:                  p.extendedEvalCtx.DistSQLPlanner,
		singleTenant:         p.execCfg.Codec.ForSystemTenant(),
		planningMode:         planningMode,
		gatewaySQLInstanceID: p.extendedEvalCtx.DistSQLPlanner.gatewaySQLInstanceID,
	}
	distribute := DistributionType(LocalDistribution)
	if e.planningMode != distSQLLocalOnlyPlanning {
		distribute = FullDistribution
	}
	evalCtx := p.ExtendedEvalContext()
	e.planCtx = e.dsp.NewPlanningCtx(ctx, evalCtx, e.planner, e.planner.txn, distribute)
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
		return e.makeTrivialValues(cols, len(rows))
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
	planCtx := e.getPlanCtx(recommendation)

	// If we decided not to distribute, create a valuesNode and wrap it into the
	// physical plan. It's possible that the Values node has expressions that
	// cannot be serialized, but even if not, we avoid serializing when the plan
	// is local as an optimization.
	//
	// See also wrapValuesNode().
	wrapPlan := planCtx.isLocal
	return e.constructValues(planCtx, rows, nil /* literalRows */, cols, wrapPlan)
}

func (e *distSQLSpecExecFactory) ConstructLiteralValues(
	rows tree.ExprContainer, cols colinfo.ResultColumns,
) (exec.Node, error) {
	if (len(cols) == 0 && rows.NumRows() == 1) || rows.NumRows() == 0 {
		return e.makeTrivialValues(cols, rows.NumRows())
	}

	var wrapPlan bool
	switch rows.(type) {
	case tree.VectorRows:
		// The vectorized literal Values operator must always be planned as a
		// DistSQL processor, since the planNode implementation does not support
		// it.
		wrapPlan = false
	case *rowcontainer.RowContainer:
		// The row container literal Values operator must always be planned as a
		// wrapped planNode, since it cannot be serialized.
		wrapPlan = true
	default:
		return nil, errors.AssertionFailedf("unexpected ExprContainer type %T", rows)
	}
	// Literal Values operators are always local.
	planCtx := e.getPlanCtx(cannotDistribute)
	return e.constructValues(planCtx, nil /* rows */, rows, cols, wrapPlan)
}

// makeTrivialValues constructs a plan for a VALUES operator with no columns or
// no rows. It is used as an optimization in ConstructValues and
// ConstructLiteralValues.
func (e *distSQLSpecExecFactory) makeTrivialValues(
	cols colinfo.ResultColumns, rowCount int,
) (exec.Node, error) {
	planCtx := e.getPlanCtx(canDistribute)
	colTypes := getTypesFromResultColumns(cols)
	spec := e.dsp.createValuesSpec(planCtx, colTypes, rowCount, nil /* rawBytes */)
	physPlan, _, err := e.dsp.createValuesPlan(planCtx, spec, colTypes, nil /* finalizeLastStageCb */)
	if err != nil {
		return nil, err
	}
	physPlan.ResultColumns = cols
	return makePlanMaybePhysical(physPlan, nil /* planNodesToClose */), nil
}

// constructValues extracts the common logic between building Values and
// LiteralValues operators. Only one of rows and literalRows should be set.
func (e *distSQLSpecExecFactory) constructValues(
	planCtx *PlanningCtx,
	rows [][]tree.TypedExpr,
	literalRows tree.ExprContainer,
	cols colinfo.ResultColumns,
	wrapPlan bool,
) (exec.Node, error) {
	if wrapPlan {
		// The valuesNode must be wrapped into the physical plan, so we cannot
		// avoid creating it. See wrapValuesNode for more details.
		v := &valuesNode{
			columns:          cols,
			tuples:           rows,
			specifiedInQuery: true,
		}
		if literalRows != nil {
			v.valuesRun = valuesRun{rows: literalRows.(*rowcontainer.RowContainer)}
		}
		planNodesToClose := []planNode{v}
		physPlan, err := e.dsp.wrapPlan(e.ctx, planCtx, v, e.planningMode != distSQLLocalOnlyPlanning)
		if err != nil {
			return nil, err
		}
		physPlan.ResultColumns = cols
		return makePlanMaybePhysical(physPlan, planNodesToClose), nil
	}
	// We can create a spec for the values processor, so we don't create a
	// valuesNode.
	colTypes := getTypesFromResultColumns(cols)
	spec, err := e.dsp.createValuesSpecFromTuples(e.ctx, planCtx, rows, colTypes)
	if err != nil {
		return nil, err
	}
	physPlan, pIdx, err := e.dsp.createValuesPlan(planCtx, spec, colTypes, nil /* finalizeLastStageCb */)
	if err != nil {
		return nil, err
	}
	if vectorRows, ok := literalRows.(tree.VectorRows); ok {
		if physPlan.LocalVectorSources == nil {
			physPlan.LocalVectorSources = make(map[int32]any)
		}
		physPlan.LocalVectorSources[int32(pIdx)] = vectorRows.Batch
	}
	physPlan.ResultColumns = cols
	return makePlanMaybePhysical(physPlan, nil), nil
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
				planCtx := e.getPlanCtx(cannotDistribute)
				physPlan, err := e.dsp.wrapPlan(e.ctx, planCtx, d, e.planningMode != distSQLLocalOnlyPlanning)
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
	sb.InitAllowingExternalRowData(e.planner.EvalContext(), e.planner.ExecCfg().Codec, tabDesc, idx)

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
		spans, err = sb.SpansFromInvertedSpans(e.ctx, params.InvertedConstraint, params.IndexConstraint, nil /* scratch */)
	} else {
		var splitter span.Splitter
		if params.Locking.MustLockAllRequestedColumnFamilies() {
			splitter = span.MakeSplitterForSideEffect(tabDesc, idx, params.NeededCols)
		} else {
			splitter = span.MakeSplitter(tabDesc, idx, params.NeededCols)
		}
		spans, err = sb.SpansFromConstraint(params.IndexConstraint, splitter)
	}
	if err != nil {
		return nil, err
	}

	isFullTableOrIndexScan := len(spans) == 1 && spans[0].EqualValue(
		tabDesc.IndexSpanAllowingExternalRowData(e.planner.ExecCfg().Codec, idx.GetID()),
	)
	if err = colCfg.assertValidReqOrdering(reqOrdering); err != nil {
		return nil, err
	}

	// Check if we are doing a full scan.
	// TODO(yuzefovich): add better heuristics here so that we always distribute
	// "large" scans, as controlled by a session variable.
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
	trSpec.LockingStrength = descpb.ToScanLockingStrength(params.Locking.Strength)
	trSpec.LockingWaitPolicy = descpb.ToScanLockingWaitPolicy(params.Locking.WaitPolicy)
	trSpec.LockingDurability = descpb.ToScanLockingDurability(params.Locking.Durability)
	if trSpec.LockingStrength != descpb.ScanLockingStrength_FOR_NONE {
		// Scans that are performing row-level locking cannot currently be
		// distributed because their locks would not be propagated back to
		// the root transaction coordinator.
		// TODO(nvanbenschoten): lift this restriction.
		recommendation = cannotDistribute
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
		e.ctx,
		e.getPlanCtx(recommendation),
		p,
		&tableReaderPlanningInfo{
			spec:              trSpec,
			post:              post,
			desc:              tabDesc,
			spans:             spans,
			reverse:           params.Reverse,
			parallelize:       params.Parallelize,
			estimatedRowCount: params.EstimatedRowCount,
			reqOrdering:       ReqOrdering(reqOrdering),
		},
	)

	return makePlanMaybePhysical(p, nil /* planNodesToClose */), err
}

// Ctx implements the Factory interface.
func (e *distSQLSpecExecFactory) Ctx() context.Context {
	return e.ctx
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
		recommendation = cannotDistribute
	}
	for _, expr := range exprs {
		if err := checkExprForDistSQL(expr, &e.distSQLVisitor); err != nil {
			recommendation = cannotDistribute
			if physPlan != nil {
				// The expression cannot be distributed, so we need to make sure that
				// there is a single stream on a node. We could do so on one of the
				// nodes that streams originate from, but for now we choose the gateway.
				physPlan.EnsureSingleStreamOnGateway(e.ctx, nil /* finalizeLastStageCb */)
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
	if err := physPlan.AddFilter(e.ctx, filter, e.getPlanCtx(recommendation), physPlan.PlanToStreamColMap, nil /* finalizeLastStageCb */); err != nil {
		return nil, err
	}
	physPlan.SetMergeOrdering(e.dsp.convertOrdering(ReqOrdering(reqOrdering), physPlan.PlanToStreamColMap))
	return plan, nil
}

// ConstructInvertedFilter is part of the exec.Factory interface.
func (e *distSQLSpecExecFactory) ConstructInvertedFilter(
	input exec.Node,
	invFilter *inverted.SpanExpression,
	preFiltererExpr tree.TypedExpr,
	preFiltererType *types.T,
	invColumn exec.NodeColumnOrdinal,
) (exec.Node, error) {
	physPlan, _ := getPhysPlan(input)
	recommendation := e.checkExprsAndMaybeMergeLastStage([]tree.TypedExpr{preFiltererExpr}, physPlan)
	if invFilter.Left != nil || invFilter.Right != nil {
		// When filtering is a union of inverted spans, it is distributable: place
		// an inverted filterer on each node, which produce the primary keys in
		// arbitrary order, and de-duplicate the PKs at the next stage.
		// The expression is a union of inverted spans iff all the spans have been
		// promoted to FactoredUnionSpans, in which case the Left and Right
		// inverted.Expressions are nil.
		// See also checkSupportForInvertedFilterNode.
		recommendation = cannotDistribute
		physPlan.EnsureSingleStreamOnGateway(e.ctx, nil /* finalizeLastStageCb */)
	} else {
		// TODO(yuzefovich): we might want to be smarter about this and don't force
		// distribution with small inputs.
		log.VEventf(e.ctx, 2, "inverted filter (union of inverted spans) recommends plan distribution")
		recommendation = recommendation.compose(shouldDistribute)
	}
	planCtx := e.getPlanCtx(recommendation)
	planInfo := &invertedFilterPlanningInfo{
		expression:      invFilter,
		preFiltererExpr: preFiltererExpr,
		preFiltererType: preFiltererType,
		invColumn:       int(invColumn),
	}
	if err := e.dsp.planInvertedFilter(e.ctx, planCtx, planInfo, physPlan); err != nil {
		return nil, err
	}
	return physPlan, nil
}

func (e *distSQLSpecExecFactory) ConstructSimpleProject(
	n exec.Node, cols []exec.NodeColumnOrdinal, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	physPlan, plan := getPhysPlan(n)
	projection := make([]uint32, len(cols))
	for i, col := range cols {
		projection[i] = uint32(physPlan.PlanToStreamColMap[col])
	}
	newColMap := identityMap(physPlan.PlanToStreamColMap, len(cols))
	physPlan.AddProjection(
		projection,
		e.dsp.convertOrdering(ReqOrdering(reqOrdering), newColMap),
		nil, /* finalizeLastStageCb */
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
	physPlan.EnsureSingleStreamOnGateway(e.ctx, nil /* finalizeLastStageCb */)
	projection := make([]uint32, len(cols))
	for i, col := range cols {
		projection[i] = uint32(physPlan.PlanToStreamColMap[col])
	}
	physPlan.AddProjection(projection, execinfrapb.Ordering{}, nil /* finalizeLastStageCb */)
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
		e.ctx, exprs, e.getPlanCtx(recommendation), physPlan.PlanToStreamColMap, getTypesFromResultColumns(columns),
		e.dsp.convertOrdering(ReqOrdering(reqOrdering), newColMap), nil, /* finalizeLastStageCb */
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
	estimatedLeftRowCount, estimatedRightRowCount uint64,
) (exec.Node, error) {
	return e.constructHashOrMergeJoin(
		joinType, left, right, extraOnCond, leftEqCols, rightEqCols,
		leftEqColsAreKey, rightEqColsAreKey,
		ReqOrdering{} /* mergeJoinOrdering */, exec.OutputOrdering{}, /* reqOrdering */
		estimatedLeftRowCount, estimatedRightRowCount,
	)
}

func (e *distSQLSpecExecFactory) ConstructMergeJoin(
	joinType descpb.JoinType,
	left, right exec.Node,
	onCond tree.TypedExpr,
	leftOrdering, rightOrdering colinfo.ColumnOrdering,
	reqOrdering exec.OutputOrdering,
	leftEqColsAreKey, rightEqColsAreKey bool,
	estimatedLeftRowCount, estimatedRightRowCount uint64,
) (exec.Node, error) {
	leftEqCols, rightEqCols, mergeJoinOrdering, err := getEqualityIndicesAndMergeJoinOrdering(leftOrdering, rightOrdering)
	if err != nil {
		return nil, err
	}
	return e.constructHashOrMergeJoin(
		joinType, left, right, onCond, leftEqCols, rightEqCols,
		leftEqColsAreKey, rightEqColsAreKey, mergeJoinOrdering, reqOrdering,
		estimatedLeftRowCount, estimatedRightRowCount,
	)
}

func populateAggFuncSpec(
	ctx context.Context,
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
		var ef physicalplan.ExprFactory
		ef.Init(ctx, planCtx, nil /* indexVarMap */)
		for k, argument := range constArgs {
			var err error
			spec.Arguments[k], err = ef.Make(argument)
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
	estimatedRowCount uint64,
	estimatedInputRowCount uint64,
) (exec.Node, error) {
	physPlan, plan := getPhysPlan(input)
	// planAggregators() itself decides whether to distribute the aggregation.
	aggRec := shouldDistribute
	if estimatedInputRowCount != 0 && e.planner.SessionData().DistributeGroupByRowCountThreshold > estimatedInputRowCount {
		// Don't force distribution if we expect to process small number of
		// rows.
		aggRec = canDistribute
	}
	planCtx := e.getPlanCtx(aggRec)
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
				e.ctx, spec, builtins.AnyNotNull, false /* distinct*/, argColsScratch,
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
			e.ctx, spec, agg.FuncName, agg.Distinct, agg.ArgCols,
			agg.ConstArgs, agg.Filter, planCtx, physPlan,
		)
		if err != nil {
			return nil, err
		}
	}
	if err := e.dsp.planAggregators(
		e.ctx,
		planCtx,
		physPlan,
		&aggregatorPlanningInfo{
			aggregations:         aggregationSpecs,
			argumentsColumnTypes: argumentsColumnTypes,
			isScalar:             isScalar,
			groupCols:            groupCols,
			groupColOrdering:     groupColOrdering,
			inputMergeOrdering:   physPlan.MergeOrdering,
			reqOrdering:          ReqOrdering(reqOrdering),
			estimatedRowCount:    estimatedRowCount,
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
	estimatedRowCount uint64,
	estimatedInputRowCount uint64,
) (exec.Node, error) {
	return e.constructAggregators(
		input,
		groupCols,
		groupColOrdering,
		aggregations,
		reqOrdering,
		false, /* isScalar */
		estimatedRowCount,
		estimatedInputRowCount,
	)
}

func (e *distSQLSpecExecFactory) ConstructScalarGroupBy(
	input exec.Node, aggregations []exec.AggInfo, estimatedInputRowCount uint64,
) (exec.Node, error) {
	return e.constructAggregators(
		input,
		nil, /* groupCols */
		nil, /* groupColOrdering */
		aggregations,
		exec.OutputOrdering{}, /* reqOrdering */
		true,                  /* isScalar */
		1,                     /* estimatedRowCount */
		estimatedInputRowCount,
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
	e.dsp.addDistinctProcessors(e.ctx, physPlan, spec, nil /* finalizeLastStageCb */)
	// Since addition of distinct processors doesn't change any properties of
	// the physical plan, we don't need to update any of those.
	return plan, nil
}

// ConstructHashSetOp is part of the exec.Factory interface.
func (e *distSQLSpecExecFactory) ConstructHashSetOp(
	typ tree.UnionType, all bool, left, right exec.Node,
) (exec.Node, error) {
	return e.constructSetOp(left, right, setOpPlanningInfo{
		unionType: typ,
		all:       all,
	})
}

// ConstructStreamingSetOp is part of the exec.Factory interface.
func (e *distSQLSpecExecFactory) ConstructStreamingSetOp(
	typ tree.UnionType,
	all bool,
	left, right exec.Node,
	leftOrdering, rightOrdering, streamingOrdering colinfo.ColumnOrdering,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return e.constructSetOp(left, right, setOpPlanningInfo{
		unionType:         typ,
		all:               all,
		leftOrdering:      leftOrdering,
		rightOrdering:     rightOrdering,
		streamingOrdering: streamingOrdering,
		reqOrdering:       ReqOrdering(reqOrdering),
	})
}

// ConstructUnionAll is part of the exec.Factory interface.
func (e *distSQLSpecExecFactory) ConstructUnionAll(
	left, right exec.Node,
	leftOrdering, rightOrdering colinfo.ColumnOrdering,
	reqOrdering exec.OutputOrdering,
	hardLimit uint64,
	enforceHomeRegion bool,
) (exec.Node, error) {
	const (
		typ = tree.UnionOp
		all = true
	)
	return e.constructSetOp(left, right, setOpPlanningInfo{
		unionType:         typ,
		all:               all,
		enforceHomeRegion: enforceHomeRegion,
		leftOrdering:      leftOrdering,
		rightOrdering:     rightOrdering,
		streamingOrdering: colinfo.ColumnOrdering(reqOrdering),
		reqOrdering:       ReqOrdering(reqOrdering),
		hardLimit:         hardLimit,
	})
}

func (e *distSQLSpecExecFactory) constructSetOp(
	left, right exec.Node, planInfo setOpPlanningInfo,
) (exec.Node, error) {
	leftPhysPlan, _ := getPhysPlan(left)
	rightPhysPlan, _ := getPhysPlan(right)
	resultCols, err := getSetOpResultColumns(
		planInfo.unionType, leftPhysPlan.ResultColumns, rightPhysPlan.ResultColumns,
	)
	if err != nil {
		return nil, err
	}
	planCtx := e.getPlanCtx(canDistribute)
	p := planCtx.NewPhysicalPlan()
	if err := e.dsp.planSetOp(e.ctx, p, leftPhysPlan, rightPhysPlan, planInfo); err != nil {
		return nil, err
	}
	p.ResultColumns = resultCols
	return makePlanMaybePhysical(p, nil /* planNodesToClose */), nil
}

// ConstructSort is part of the exec.Factory interface.
func (e *distSQLSpecExecFactory) ConstructSort(
	input exec.Node,
	ordering exec.OutputOrdering,
	alreadyOrderedPrefix int,
	estimatedInputRowCount uint64,
) (exec.Node, error) {
	physPlan, plan := getPhysPlan(input)
	// TODO(yuzefovich): add better heuristics here so that we always distribute
	// "large" sorts, as controlled by a session variable.
	e.dsp.addSorters(e.ctx, physPlan, colinfo.ColumnOrdering(ordering), alreadyOrderedPrefix, 0 /* limit */, nil /* finalizeLastStageCb */)
	// Since addition of sorters doesn't change any properties of the physical
	// plan, we don't need to update any of those.
	return plan, nil
}

// ConstructOrdinality is part of the exec.Factory interface.
func (e *distSQLSpecExecFactory) ConstructOrdinality(
	input exec.Node, colName string,
) (exec.Node, error) {
	physPlan, plan := getPhysPlan(input)
	ordinalitySpec := execinfrapb.ProcessorCoreUnion{
		Ordinality: &execinfrapb.OrdinalitySpec{},
	}

	physPlan.PlanToStreamColMap = append(physPlan.PlanToStreamColMap, len(physPlan.GetResultTypes()))
	outputTypes := append(physPlan.GetResultTypes(), types.Int)

	// WITH ORDINALITY never gets distributed so that the gateway node can
	// always number each row in order.
	physPlan.AddSingleGroupStage(
		e.ctx, e.gatewaySQLInstanceID, ordinalitySpec,
		execinfrapb.PostProcessSpec{}, outputTypes, nil, /* finalizeLastStageCb */
	)

	physPlan.ResultColumns = append(physPlan.ResultColumns, colinfo.ResultColumn{
		Name: colName,
		Typ:  types.Int,
	})

	return plan, nil
}

func (e *distSQLSpecExecFactory) ConstructIndexJoin(
	input exec.Node,
	table cat.Table,
	keyCols []exec.NodeColumnOrdinal,
	tableCols exec.TableColumnOrdinalSet,
	reqOrdering exec.OutputOrdering,
	locking opt.Locking,
	limitHint int64,
) (exec.Node, error) {
	physPlan, plan := getPhysPlan(input)
	tabDesc := table.(*optTable).desc
	colCfg := makeScanColumnsConfig(table, tableCols)

	var fetch fetchPlanningInfo
	if err := fetch.initDescDefaults(tabDesc, colCfg); err != nil {
		return nil, err
	}

	idx := tabDesc.GetPrimaryIndex()
	fetch.index = idx
	fetch.lockingStrength = descpb.ToScanLockingStrength(locking.Strength)
	fetch.lockingWaitPolicy = descpb.ToScanLockingWaitPolicy(locking.WaitPolicy)
	fetch.lockingDurability = descpb.ToScanLockingDurability(locking.Durability)

	// TODO(drewk): in an EXPLAIN context, record the index usage.
	planInfo := &indexJoinPlanningInfo{
		fetch:       fetch,
		keyCols:     keyCols,
		reqOrdering: ReqOrdering(reqOrdering),
		limitHint:   limitHint,
	}

	recommendation := canDistribute
	if locking.Strength != tree.ForNone {
		// Index joins that are performing row-level locking cannot currently be
		// distributed because their locks would not be propagated back to the root
		// transaction coordinator.
		// TODO(nvanbenschoten): lift this restriction.
		recommendation = cannotDistribute
		physPlan.EnsureSingleStreamOnGateway(e.ctx, nil /* finalizeLastStageCb */)
	}
	planCtx := e.getPlanCtx(recommendation)
	if err := e.dsp.planIndexJoin(e.ctx, planCtx, planInfo, physPlan); err != nil {
		return nil, err
	}
	physPlan.ResultColumns = fetch.columns
	return plan, nil
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
	locking opt.Locking,
	limitHint int64,
	remoteOnlyLookups bool,
	reverseScans bool,
) (exec.Node, error) {
	physPlan, plan := getPhysPlan(input)
	var planNodesToClose []planNode
	if table.IsVirtualTable() {
		planCtx := e.getPlanCtx(cannotDistribute)
		physPlan.EnsureSingleStreamOnGateway(e.ctx, nil /* finalizeLastStageCb */)
		vTableLookupJoin, err := constructVirtualTableLookupJoin(
			e.planner, joinType, plan, table, index, eqCols, lookupCols, onCond,
		)
		if err != nil {
			return nil, err
		}
		planNodesToClose = []planNode{vTableLookupJoin}
		allowPartialDistribution := e.planningMode != distSQLLocalOnlyPlanning
		physPlan, err = e.dsp.wrapPlan(e.ctx, planCtx, vTableLookupJoin, allowPartialDistribution)
		if err != nil {
			return nil, err
		}
		physPlan.ResultColumns = planColumns(vTableLookupJoin)
	} else {
		tabDesc := table.(*optTable).desc
		idx := index.(*optIndex).idx
		colCfg := makeScanColumnsConfig(table, lookupCols)

		var fetch fetchPlanningInfo
		if err := fetch.initDescDefaults(tabDesc, colCfg); err != nil {
			return nil, err
		}

		fetch.index = idx
		fetch.lockingStrength = descpb.ToScanLockingStrength(locking.Strength)
		fetch.lockingWaitPolicy = descpb.ToScanLockingWaitPolicy(locking.WaitPolicy)
		fetch.lockingDurability = descpb.ToScanLockingDurability(locking.Durability)

		// TODO(drewk): if in an EXPLAIN context, record the index usage.
		planInfo := &lookupJoinPlanningInfo{
			fetch:                      fetch,
			joinType:                   joinType,
			eqCols:                     eqCols,
			eqColsAreKey:               eqColsAreKey,
			lookupExpr:                 lookupExpr,
			remoteLookupExpr:           remoteLookupExpr,
			isFirstJoinInPairedJoiner:  isFirstJoinInPairedJoiner,
			isSecondJoinInPairedJoiner: isSecondJoinInPairedJoiner,
			reqOrdering:                ReqOrdering(reqOrdering),
			limitHint:                  limitHint,
			remoteOnlyLookups:          remoteOnlyLookups,
			reverseScans:               reverseScans,
		}
		if onCond != tree.DBoolTrue {
			planInfo.onCond = onCond
		}

		recommendation := e.checkExprsAndMaybeMergeLastStage([]tree.TypedExpr{lookupExpr, onCond}, physPlan)
		if locking.Strength != tree.ForNone {
			// Lookup joins that are performing row-level locking cannot currently be
			// distributed because their locks would not be propagated back to the root
			// transaction coordinator.
			// TODO(nvanbenschoten): lift this restriction.
			recommendation = cannotDistribute
			physPlan.EnsureSingleStreamOnGateway(e.ctx, nil /* finalizeLastStageCb */)
		} else if remoteLookupExpr != nil || remoteOnlyLookups {
			// Do not distribute locality-optimized joins, since it would defeat the
			// purpose of the optimization.
			recommendation = cannotDistribute
			physPlan.EnsureSingleStreamOnGateway(e.ctx, nil /* finalizeLastStageCb */)
		}
		planCtx := e.getPlanCtx(recommendation)
		if err := e.dsp.planLookupJoin(e.ctx, planCtx, planInfo, physPlan); err != nil {
			return nil, err
		}

		rightCols := colinfo.ResultColumnsFromColumns(tabDesc.GetID(), makeColList(table, lookupCols))
		physPlan.ResultColumns = getJoinResultColumns(joinType, physPlan.ResultColumns, rightCols)
	}

	return makePlanMaybePhysical(physPlan, planNodesToClose), nil
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
	locking opt.Locking,
) (exec.Node, error) {
	physPlan, _ := getPhysPlan(input)

	tabDesc := table.(*optTable).desc
	idx := index.(*optIndex).idx
	colCfg := makeScanColumnsConfig(table, lookupCols)

	var fetch fetchPlanningInfo
	if err := fetch.initDescDefaults(tabDesc, colCfg); err != nil {
		return nil, err
	}
	fetch.index = idx
	fetch.lockingStrength = descpb.ToScanLockingStrength(locking.Strength)
	fetch.lockingWaitPolicy = descpb.ToScanLockingWaitPolicy(locking.WaitPolicy)
	fetch.lockingDurability = descpb.ToScanLockingDurability(locking.Durability)

	// TODO(drewk): if we're in an EXPLAIN context, record the index usage.
	planInfo := &invertedJoinPlanningInfo{
		fetch:                     fetch,
		joinType:                  joinType,
		prefixEqCols:              prefixEqCols,
		invertedExpr:              invertedExpr,
		isFirstJoinInPairedJoiner: isFirstJoinInPairedJoiner,
		reqOrdering:               ReqOrdering(reqOrdering),
	}
	if onCond != nil && onCond != tree.DBoolTrue {
		planInfo.onExpr = onCond
	}

	recommendation := e.checkExprsAndMaybeMergeLastStage([]tree.TypedExpr{onCond}, physPlan)
	if locking.Strength != tree.ForNone {
		// Lookup joins that are performing row-level locking cannot currently be
		// distributed because their locks would not be propagated back to the root
		// transaction coordinator.
		// TODO(nvanbenschoten): lift this restriction.
		recommendation = cannotDistribute
		physPlan.EnsureSingleStreamOnGateway(e.ctx, nil /* finalizeLastStageCb */)
	}
	planCtx := e.getPlanCtx(recommendation)
	if err := e.dsp.planInvertedJoin(e.ctx, planCtx, planInfo, physPlan); err != nil {
		return nil, err
	}
	physPlan.ResultColumns = invertedJoinResultCols(
		joinType, physPlan.ResultColumns, fetch.columns, isFirstJoinInPairedJoiner,
	)
	return makePlanMaybePhysical(physPlan, nil), nil
}

func (e *distSQLSpecExecFactory) constructZigzagJoinSide(
	planCtx *PlanningCtx,
	table cat.Table,
	index cat.Index,
	wantedCols exec.TableColumnOrdinalSet,
	fixedVals []tree.TypedExpr,
	eqCols []exec.TableColumnOrdinal,
	locking opt.Locking,
) (zigzagPlanningSide, error) {
	desc := table.(*optTable).desc
	colCfg := makeScanColumnsConfig(table, wantedCols)

	eqColOrdinals, err := tableToScanOrdinals(wantedCols, eqCols)
	if err != nil {
		return zigzagPlanningSide{}, err
	}

	cols, err := initColsForScan(desc, colCfg)
	if err != nil {
		return zigzagPlanningSide{}, err
	}
	typs := make([]*types.T, len(fixedVals))
	for i := range typs {
		typs[i] = fixedVals[i].ResolvedType()
	}
	valuesSpec, err := e.dsp.createValuesSpecFromTuples(e.ctx, planCtx, [][]tree.TypedExpr{fixedVals}, typs)
	if err != nil {
		return zigzagPlanningSide{}, err
	}

	// TODO (cucaroach): update indexUsageStats.

	return zigzagPlanningSide{
		desc:              desc,
		index:             index.(*optIndex).idx,
		cols:              cols,
		eqCols:            eqColOrdinals,
		fixedValues:       valuesSpec,
		lockingStrength:   descpb.ToScanLockingStrength(locking.Strength),
		lockingWaitPolicy: descpb.ToScanLockingWaitPolicy(locking.WaitPolicy),
		lockingDurability: descpb.ToScanLockingDurability(locking.Durability),
	}, nil
}

func (e *distSQLSpecExecFactory) ConstructZigzagJoin(
	leftTable cat.Table,
	leftIndex cat.Index,
	leftCols exec.TableColumnOrdinalSet,
	leftFixedVals []tree.TypedExpr,
	leftEqCols []exec.TableColumnOrdinal,
	leftLocking opt.Locking,
	rightTable cat.Table,
	rightIndex cat.Index,
	rightCols exec.TableColumnOrdinalSet,
	rightFixedVals []tree.TypedExpr,
	rightEqCols []exec.TableColumnOrdinal,
	rightLocking opt.Locking,
	onCond tree.TypedExpr,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	// TODO(yuzefovich): we might want to be smarter about this and don't
	// force distribution with small inputs.
	recommendation := e.checkExprsAndMaybeMergeLastStage([]tree.TypedExpr{onCond}, nil /* physPlan */)
	if leftLocking.Strength != tree.ForNone || rightLocking.Strength != tree.ForNone {
		// ZigZag joins that are performing row-level locking cannot currently be
		// distributed because their locks would not be propagated back to the root
		// transaction coordinator.
		// TODO(nvanbenschoten): lift this restriction.
		recommendation = cannotDistribute
	}
	planCtx := e.getPlanCtx(recommendation)

	sides := make([]zigzagPlanningSide, 2)
	var err error
	sides[0], err = e.constructZigzagJoinSide(planCtx, leftTable, leftIndex, leftCols, leftFixedVals, leftEqCols, leftLocking)
	if err != nil {
		return nil, err
	}
	sides[1], err = e.constructZigzagJoinSide(planCtx, rightTable, rightIndex, rightCols, rightFixedVals, rightEqCols, rightLocking)
	if err != nil {
		return nil, err
	}

	leftResultColumns := colinfo.ResultColumnsFromColumns(sides[0].desc.GetID(), sides[0].cols)
	rightResultColumns := colinfo.ResultColumnsFromColumns(sides[1].desc.GetID(), sides[1].cols)
	resultColumns := make(colinfo.ResultColumns, 0, len(leftResultColumns)+len(rightResultColumns))
	resultColumns = append(resultColumns, leftResultColumns...)
	resultColumns = append(resultColumns, rightResultColumns...)
	p, err := e.dsp.planZigzagJoin(e.ctx, planCtx, zigzagPlanningInfo{
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
	count, offset, err := evalLimit(e.ctx, e.planner.EvalContext(), limitExpr, offsetExpr)
	if err != nil {
		return nil, err
	}
	if err = physPlan.AddLimit(e.ctx, count, offset, e.getPlanCtx(recommendation), nil /* finalizeLastStageCb */); err != nil {
		return nil, err
	}
	// Since addition of limit and/or offset doesn't change any properties of
	// the physical plan, we don't need to update any of those (like
	// PlanToStreamColMap, etc).
	return plan, nil
}

func (e *distSQLSpecExecFactory) ConstructTopK(
	input exec.Node,
	k int64,
	ordering exec.OutputOrdering,
	alreadyOrderedPrefix int,
	estimatedInputRowCount uint64,
) (exec.Node, error) {
	physPlan, plan := getPhysPlan(input)
	if k <= 0 {
		return nil, errors.New("negative or zero value for LIMIT")
	}
	// TODO(yuzefovich): add better heuristics here so that we always distribute
	// "large" sorts, as controlled by a session variable.
	e.dsp.addSorters(e.ctx, physPlan, colinfo.ColumnOrdering(ordering), alreadyOrderedPrefix, k, nil /* finalizeLastStageCb */)
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
		e.ctx,
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
		nil, /* finalizeLastStageCb */
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
	physPlan, plan := getPhysPlan(input)

	if len(window.Exprs) == 0 {
		// If we don't have any window functions to compute, then all input
		// columns are simply passed-through, so we don't need to plan the
		// windower. This shouldn't really happen since the optimizer should
		// eliminate such a window node, but if some of the optimizer's rules
		// are disabled (in tests), it could happen.
		return plan, nil
	}

	partitionIdxs := make([]uint32, len(window.Partition))
	for i := range partitionIdxs {
		partitionIdxs[i] = uint32(window.Partition[i])
	}

	planInfo := windowPlanningInfo{
		funcs:          make([]*windowFuncHolder, len(window.Exprs)),
		partitionIdxs:  partitionIdxs,
		columnOrdering: window.Ordering,
	}
	for windowFnSpecIdx := range window.Exprs {
		argsIdxs := make([]uint32, len(window.ArgIdxs[windowFnSpecIdx]))
		for i := range argsIdxs {
			argsIdxs[i] = uint32(window.ArgIdxs[windowFnSpecIdx][i])
		}
		planInfo.funcs[windowFnSpecIdx] = &windowFuncHolder{
			expr:         window.Exprs[windowFnSpecIdx],
			args:         window.Exprs[windowFnSpecIdx].Exprs,
			argsIdxs:     argsIdxs,
			filterColIdx: window.FilterIdxs[windowFnSpecIdx],
			outputColIdx: window.OutputIdxs[windowFnSpecIdx],
			frame:        window.Exprs[windowFnSpecIdx].WindowDef.Frame,
		}
	}

	recommendation := canDistribute
	if len(partitionIdxs) > 0 {
		// If the window has a PARTITION BY clause, then we should distribute the
		// execution.
		// TODO(yuzefovich): we might want to be smarter about this and don't force
		// distribution with small inputs.
		log.VEventf(e.ctx, 2, "window with PARTITION BY recommends plan distribution")
		recommendation = shouldDistribute
	}
	planCtx := e.getPlanCtx(recommendation)
	if err := e.dsp.planWindow(e.ctx, planCtx, &planInfo, physPlan); err != nil {
		return nil, err
	}

	physPlan.ResultColumns = window.Cols
	return plan, nil
}

func (e *distSQLSpecExecFactory) ConstructPlan(
	root exec.Node,
	subqueries []exec.Subquery,
	cascades, triggers []exec.PostQuery,
	checks []exec.Node,
	rootRowCount int64,
	flags exec.PlanFlags,
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
	if len(triggers) != 0 {
		return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: triggers")
	}
	if p, ok := root.(planMaybePhysical); !ok {
		return nil, errors.AssertionFailedf("unexpected type for root: %T", root)
	} else {
		p.physPlan.onClose = e.planCtx.getCleanupFunc()
	}
	return constructPlan(e.planner, root, subqueries, cascades, triggers, checks, rootRowCount, flags)
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
) (_ exec.Node, retErr error) {
	if options.Flags[tree.ExplainFlagEnv] {
		return nil, errors.New("ENV only supported with (OPT) option")
	}

	// We cannot create the explained plan in the same PlanInfrastructure with the
	// "outer" plan. Create a separate factory.
	newFactory := newDistSQLSpecExecFactory(e.ctx, e.planner, e.planningMode)
	plan, err := buildFn(newFactory)
	// Make sure to release the resources of the new factory if we encounter an
	// error (if we don't, then the cleanup will be performed when closing the
	// plan).
	defer func() {
		if retErr != nil {
			newFactory.(*distSQLSpecExecFactory).planCtx.getCleanupFunc()()
		}
	}()
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

	planCtx := e.getPlanCtx(cannotDistribute)
	physPlan, err := e.dsp.wrapPlan(e.ctx, planCtx, explainNode, e.planningMode != distSQLLocalOnlyPlanning)
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
	uniqueWithTombstoneIndexes cat.IndexOrdinals,
	autoCommit bool,
	vectorInsert bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: insert")
}

func (e *distSQLSpecExecFactory) ConstructInsertFastPath(
	rows [][]tree.TypedExpr,
	table cat.Table,
	insertCols exec.TableColumnOrdinalSet,
	returnCols exec.TableColumnOrdinalSet,
	checkCols exec.CheckOrdinalSet,
	fkChecks []exec.InsertFastPathCheck,
	uniqChecks []exec.InsertFastPathCheck,
	uniqueWithTombstoneIndexes cat.IndexOrdinals,
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
	uniqueWithTombstoneIndexes cat.IndexOrdinals,
	lockedIndexes cat.IndexOrdinals,
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
	uniqueWithTombstoneIndexes cat.IndexOrdinals,
	lockedIndexes cat.IndexOrdinals,
	autoCommit bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: upsert")
}

func (e *distSQLSpecExecFactory) ConstructDelete(
	input exec.Node,
	table cat.Table,
	fetchCols exec.TableColumnOrdinalSet,
	returnCols exec.TableColumnOrdinalSet,
	passthrough colinfo.ResultColumns,
	lockedIndexes cat.IndexOrdinals,
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

func (e *distSQLSpecExecFactory) ConstructVectorSearch(
	table cat.Table,
	index cat.Index,
	outCols exec.TableColumnOrdinalSet,
	prefixConstraint *constraint.Constraint,
	queryVector tree.TypedExpr,
	targetNeighborCount uint64,
) (exec.Node, error) {
	tabDesc := table.(*optTable).desc
	indexDesc := index.(*optIndex).idx
	cols := makeColList(table, outCols)
	resultCols := colinfo.ResultColumnsFromColumns(tabDesc.GetID(), cols)

	// Encode the prefix constraint as a list of roachpb.Keys.
	var sb span.Builder
	sb.Init(e.planner.EvalContext(), e.planner.ExecCfg().Codec, tabDesc, indexDesc)
	prefixKeys, err := sb.KeysFromVectorPrefixConstraint(e.ctx, prefixConstraint)
	if err != nil {
		return nil, err
	}
	planInfo := &vectorSearchPlanningInfo{
		table:               tabDesc,
		index:               indexDesc,
		prefixKeys:          prefixKeys,
		queryVector:         queryVector,
		targetNeighborCount: targetNeighborCount,
		cols:                cols,
		columns:             resultCols,
	}
	// Don't allow distribution for vector search operators, for now.
	planCtx := e.getPlanCtx(cannotDistribute)
	physPlan := planCtx.NewPhysicalPlan()
	if err = e.dsp.planVectorSearch(planCtx, planInfo, physPlan); err != nil {
		return nil, err
	}
	physPlan.ResultColumns = resultCols
	return makePlanMaybePhysical(physPlan, nil /* planNodesToClose */), nil
}

func (e *distSQLSpecExecFactory) ConstructVectorMutationSearch(
	input exec.Node,
	table cat.Table,
	index cat.Index,
	prefixKeyCols []exec.NodeColumnOrdinal,
	queryVectorCol exec.NodeColumnOrdinal,
	suffixKeyCols []exec.NodeColumnOrdinal,
	isIndexPut bool,
) (exec.Node, error) {
	physPlan, plan := getPhysPlan(input)

	// Pass through the input columns, and project the partition key column and
	// optionally the quantized vectors.
	cols := make(colinfo.ResultColumns, len(physPlan.ResultColumns), len(physPlan.ResultColumns)+2)
	copy(cols, physPlan.ResultColumns)
	cols = append(cols, colinfo.ResultColumn{Name: "partition-key", Typ: types.Int})
	if isIndexPut {
		cols = append(cols, colinfo.ResultColumn{Name: "quantized-vector", Typ: types.Bytes})
	}
	planInfo := &vectorMutationSearchPlanningInfo{
		table:          table.(*optTable).desc,
		index:          index.(*optIndex).idx,
		prefixKeyCols:  prefixKeyCols,
		queryVectorCol: queryVectorCol,
		suffixKeyCols:  suffixKeyCols,
		isIndexPut:     isIndexPut,
	}
	// Don't allow distribution for vector search operators, for now.
	planCtx := e.getPlanCtx(cannotDistribute)
	if err := e.dsp.planVectorMutationSearch(planCtx, planInfo, physPlan); err != nil {
		return nil, err
	}
	physPlan.ResultColumns = cols
	return plan, nil
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
	createView *tree.CreateView,
	schema cat.Schema,
	viewQuery string,
	columns colinfo.ResultColumns,
	deps opt.SchemaDeps,
	typeDeps opt.SchemaTypeDeps,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: create view")
}

func (e *distSQLSpecExecFactory) ConstructCreateFunction(
	schema cat.Schema,
	cf *tree.CreateRoutine,
	deps opt.SchemaDeps,
	typeDeps opt.SchemaTypeDeps,
	functionDeps opt.SchemaFunctionDeps,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: create function")
}

func (e *distSQLSpecExecFactory) ConstructCreateTrigger(_ *tree.CreateTrigger) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: create trigger")
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
	planCtx := e.getPlanCtx(cannotDistribute)
	physPlan, err := e.dsp.wrapPlan(e.ctx, planCtx, plan, e.planningMode != distSQLLocalOnlyPlanning)
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

func (e *distSQLSpecExecFactory) ConstructShowCompletions(
	input *tree.ShowCompletions,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: show completions")
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
	physPlan, plan := getPhysPlan(input)
	planInfo, err := buildExportPlanningInfo(
		e.ctx, e.planner, physPlan.ResultColumns, fileName, fileFormat, options, notNullColsSet,
	)
	if err != nil {
		return nil, err
	}
	planCtx := e.getPlanCtx(canDistribute)
	if err = e.dsp.planExport(e.ctx, planCtx, planInfo, physPlan); err != nil {
		return nil, err
	}
	physPlan.ResultColumns = colinfo.ExportColumns
	return plan, nil
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
	estimatedLeftRowCount, estimatedRightRowCount uint64,
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
	rec := canDistribute
	if len(leftEqCols) > 0 {
		// We can partition both streams on the equality columns.
		if estimatedLeftRowCount == 0 && estimatedRightRowCount == 0 {
			// In the absence of stats for both inputs, fall back to
			// distributing.
			rec = shouldDistribute
		} else if estimatedLeftRowCount+estimatedRightRowCount >= e.planner.SessionData().DistributeGroupByRowCountThreshold {
			// If we have stats on at least one input, then distribute only if
			// the join appears to be "large".
			rec = shouldDistribute
		}
	}
	planCtx := e.getPlanCtx(rec)
	onExpr, err := helper.remapOnExpr(e.ctx, planCtx, onCond)
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
	p := e.dsp.planJoiners(e.ctx, planCtx, &info, ReqOrdering(reqOrdering))
	p.ResultColumns = resultColumns
	return makePlanMaybePhysical(p, append(leftPlan.physPlan.planNodesToClose, rightPlan.physPlan.planNodesToClose...)), nil
}

func (e *distSQLSpecExecFactory) ConstructCall(proc *tree.RoutineExpr) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "experimental opt-driven distsql planning: call")
}
