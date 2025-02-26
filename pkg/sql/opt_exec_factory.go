// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treewindow"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

type execFactory struct {
	ctx     context.Context
	planner *planner
	// alloc is allocated lazily the first time it is needed and shared among
	// all mutation planNodes created by the factory. It should not be accessed
	// directly - use getDatumAlloc() instead.
	alloc *tree.DatumAlloc
	// isExplain is true if this factory is used to build a statement inside
	// EXPLAIN or EXPLAIN ANALYZE.
	isExplain bool
}

var _ exec.Factory = &execFactory{}

func newExecFactory(ctx context.Context, p *planner) *execFactory {
	return &execFactory{
		ctx:     ctx,
		planner: p,
	}
}

// Ctx implements the Factory interface.
func (ef *execFactory) Ctx() context.Context {
	return ef.ctx
}

func (ef *execFactory) getDatumAlloc() *tree.DatumAlloc {
	if ef.alloc == nil {
		ef.alloc = &tree.DatumAlloc{}
	}
	return ef.alloc
}

// ConstructValues is part of the exec.Factory interface.
func (ef *execFactory) ConstructValues(
	rows [][]tree.TypedExpr, cols colinfo.ResultColumns,
) (exec.Node, error) {
	if len(cols) == 0 && len(rows) == 1 {
		return &unaryNode{}, nil
	}
	if len(rows) == 0 {
		return &zeroNode{columns: cols}, nil
	}
	return &valuesNode{
		columns:          cols,
		tuples:           rows,
		specifiedInQuery: true,
	}, nil
}

// ConstructLiteralValues is part of the exec.Factory interface.
func (ef *execFactory) ConstructLiteralValues(
	rows tree.ExprContainer, cols colinfo.ResultColumns,
) (exec.Node, error) {
	if len(cols) == 0 && rows.NumRows() == 1 {
		return &unaryNode{}, nil
	}
	if rows.NumRows() == 0 {
		return &zeroNode{columns: cols}, nil
	}
	switch t := rows.(type) {
	case *rowcontainer.RowContainer:
		return &valuesNode{
			columns:                  cols,
			specifiedInQuery:         true,
			externallyOwnedContainer: true,
			valuesRun:                valuesRun{rows: t},
		}, nil
	case *tree.VectorRows:
		return &valuesNode{
			columns:                  cols,
			specifiedInQuery:         true,
			externallyOwnedContainer: true,
			coldataBatch:             t.Batch,
		}, nil
	default:
		return nil, errors.AssertionFailedf("unexpected rows type %T in ConstructLiteralValues", rows)
	}
}

// ConstructScan is part of the exec.Factory interface.
func (ef *execFactory) ConstructScan(
	table cat.Table, index cat.Index, params exec.ScanParams, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	if table.IsVirtualTable() {
		return ef.constructVirtualScan(table, index, params, reqOrdering)
	}

	tabDesc := table.(*optTable).desc
	idx := index.(*optIndex).idx
	// Create a scanNode.
	scan := ef.planner.Scan()
	colCfg := makeScanColumnsConfig(table, params.NeededCols)

	if err := scan.initTable(ef.ctx, ef.planner, tabDesc, colCfg); err != nil {
		return nil, err
	}

	if params.IndexConstraint != nil && params.IndexConstraint.IsContradiction() {
		return newZeroNode(scan.resultColumns), nil
	}

	scan.index = idx
	scan.hardLimit = params.HardLimit
	scan.softLimit = params.SoftLimit

	scan.reverse = params.Reverse
	scan.parallelize = params.Parallelize
	var err error
	scan.spans, err = generateScanSpans(ef.ctx, ef.planner.EvalContext(), ef.planner.ExecCfg().Codec, tabDesc, idx, params)
	if err != nil {
		return nil, err
	}

	scan.isFull = len(scan.spans) == 1 && scan.spans[0].EqualValue(
		scan.desc.IndexSpanAllowingExternalRowData(ef.planner.ExecCfg().Codec, scan.index.GetID()),
	)
	if err = colCfg.assertValidReqOrdering(reqOrdering); err != nil {
		return nil, err
	}
	scan.reqOrdering = ReqOrdering(reqOrdering)
	scan.estimatedRowCount = params.EstimatedRowCount
	scan.lockingStrength = descpb.ToScanLockingStrength(params.Locking.Strength)
	scan.lockingWaitPolicy = descpb.ToScanLockingWaitPolicy(params.Locking.WaitPolicy)
	scan.lockingDurability = descpb.ToScanLockingDurability(params.Locking.Durability)
	scan.localityOptimized = params.LocalityOptimized
	if !ef.isExplain && !ef.planner.SessionData().Internal {
		idxUsageKey := roachpb.IndexUsageKey{
			TableID: roachpb.TableID(tabDesc.GetID()),
			IndexID: roachpb.IndexID(idx.GetID()),
		}
		ef.planner.extendedEvalCtx.indexUsageStats.RecordRead(idxUsageKey)
	}

	return scan, nil
}

func generateScanSpans(
	ctx context.Context,
	evalCtx *eval.Context,
	codec keys.SQLCodec,
	tabDesc catalog.TableDescriptor,
	index catalog.Index,
	params exec.ScanParams,
) (roachpb.Spans, error) {
	var sb span.Builder
	sb.InitAllowingExternalRowData(evalCtx, codec, tabDesc, index)
	if params.InvertedConstraint != nil {
		return sb.SpansFromInvertedSpans(ctx, params.InvertedConstraint, params.IndexConstraint, nil /* scratch */)
	}
	splitter := span.MakeSplitter(tabDesc, index, params.NeededCols)
	return sb.SpansFromConstraint(params.IndexConstraint, splitter)
}

func (ef *execFactory) constructVirtualScan(
	table cat.Table, index cat.Index, params exec.ScanParams, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return constructVirtualScan(
		ef, ef.planner, table, index, params, reqOrdering,
		func(d *delayedNode) (exec.Node, error) { return d, nil },
	)
}

// ConstructFilter is part of the exec.Factory interface.
func (ef *execFactory) ConstructFilter(
	n exec.Node, filter tree.TypedExpr, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	p := n.(planNode)
	f := &filterNode{
		singleInputPlanNode: singleInputPlanNode{p},
		columns:             planColumns(p),
	}
	f.filter = filter
	f.reqOrdering = ReqOrdering(reqOrdering)

	// If there's a spool, pull it up.
	if spool, ok := f.input.(*spoolNode); ok {
		f.input = spool.input
		spool.input = f
		return spool, nil
	}
	return f, nil
}

// ConstructInvertedFilter is part of the exec.Factory interface.
func (ef *execFactory) ConstructInvertedFilter(
	n exec.Node,
	invFilter *inverted.SpanExpression,
	preFiltererExpr tree.TypedExpr,
	preFiltererType *types.T,
	invColumn exec.NodeColumnOrdinal,
) (exec.Node, error) {
	inputCols := planColumns(n.(planNode))
	columns := make(colinfo.ResultColumns, len(inputCols))
	copy(columns, inputCols)
	n = &invertedFilterNode{
		singleInputPlanNode: singleInputPlanNode{n.(planNode)},
		expression:          invFilter,
		preFiltererExpr:     preFiltererExpr,
		preFiltererType:     preFiltererType,
		invColumn:           int(invColumn),
		resultColumns:       columns,
	}
	return n, nil
}

// ConstructSimpleProject is part of the exec.Factory interface.
func (ef *execFactory) ConstructSimpleProject(
	n exec.Node, cols []exec.NodeColumnOrdinal, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return constructSimpleProjectForPlanNode(n.(planNode), cols, nil /* colNames */, reqOrdering)
}

func constructSimpleProjectForPlanNode(
	n planNode, cols []exec.NodeColumnOrdinal, colNames []string, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	// If the top node is already a renderNode, we can just rearrange the columns
	// as an optimization if each render expression is projected exactly once.
	if r, ok := n.(*renderNode); ok && canRearrangeRenders(cols, r.render) {
		oldCols, oldRenders := r.columns, r.render
		r.columns = make(colinfo.ResultColumns, len(cols))
		r.render = make([]tree.TypedExpr, len(cols))
		for i, ord := range cols {
			r.columns[i] = oldCols[ord]
			if colNames != nil {
				r.columns[i].Name = colNames[i]
			}
			r.render[i] = oldRenders[ord]
		}
		r.reqOrdering = ReqOrdering(reqOrdering)
		return r, nil
	}
	inputCols := planColumns(n)
	var rb renderBuilder
	rb.init(n, reqOrdering)

	// TODO(mgartner): With an indexed var helper we are potentially allocating
	// more indexed variables than we need. We only need len(cols) indexed vars
	// but we have to allocate len(inputCols) to make sure there is an indexed
	// variable for all possible input ordinals.
	ivh := tree.MakeIndexedVarHelper(rb.r, len(inputCols))
	exprs := make(tree.TypedExprs, len(cols))
	for i, col := range cols {
		exprs[i] = ivh.IndexedVar(int(col))
	}
	var resultTypes []*types.T
	if colNames != nil {
		// We will need updated result types.
		resultTypes = make([]*types.T, len(cols))
		for i := range exprs {
			resultTypes[i] = exprs[i].ResolvedType()
		}
	}
	resultCols := getResultColumnsForSimpleProject(cols, colNames, resultTypes, inputCols)
	rb.setOutput(exprs, resultCols)
	return rb.res, nil
}

// canRearrangeRenders returns true if the renderNode with the given columns and
// render expressions can be combined with a parent renderNode. This is possible
// if there are no duplicates in the columns, and every render expression is
// referenced at least once. In other words, it is possible when every render
// expression is projected exactly once. This ensures that no side effects are
// lost or duplicated, even if the result of an expression isn't needed (or is
// needed more than once).
func canRearrangeRenders(cols []exec.NodeColumnOrdinal, render tree.TypedExprs) bool {
	// Check whether each render expression is projected at least once, if
	// that's not the case, then we must add another processor in order for
	// each render expression to be evaluated (this is needed for edge cases
	// like the render expressions resulting in errors).
	//
	// See also PhysicalPlan.AddProjection for a similar case.
	if len(cols) < len(render) {
		// There is no way for each of the render expressions to be referenced.
		return false
	}
	var colsSeen intsets.Fast
	renderUsed := make([]bool, len(render))
	for _, c := range cols {
		if colsSeen.Contains(int(c)) {
			return false
		}
		colsSeen.Add(int(c))
		renderUsed[c] = true
	}
	for _, used := range renderUsed {
		// Need to add a new renderNode if at least one render is not projected.
		if !used {
			return false
		}
	}
	return true
}

// ConstructSerializingProject is part of the exec.Factory interface.
func (ef *execFactory) ConstructSerializingProject(
	n exec.Node, cols []exec.NodeColumnOrdinal, colNames []string,
) (exec.Node, error) {
	node := n.(planNode)
	// If we are just renaming columns, we can do that in place.
	if len(cols) == len(planColumns(node)) {
		identity := true
		for i := range cols {
			if cols[i] != exec.NodeColumnOrdinal(i) {
				identity = false
				break
			}
		}
		if identity {
			inputCols := planMutableColumns(node)
			for i := range inputCols {
				inputCols[i].Name = colNames[i]
			}
			// TODO(yuzefovich): if n is not a renderNode, we won't serialize
			// it, but this is breaking the contract of
			// ConstructSerializingProject. We should clean this up, but in the
			// mean time it seems acceptable given that the method is called
			// only for the root node.
			if r, ok := n.(*renderNode); ok {
				r.serialize = true
			}
			return n, nil
		}
	}
	res, err := constructSimpleProjectForPlanNode(node, cols, colNames, nil /* reqOrdering */)
	if err != nil {
		return nil, err
	}
	switch r := res.(type) {
	case *renderNode:
		r.serialize = true
	case *spoolNode:
		// If we pulled up a spoolNode, we don't need to materialize the
		// ordering (because all mutations are currently not distributed).
		// TODO(yuzefovich): evaluate whether we still need to push renderings
		// through the spoolNode.
	default:
		return nil, errors.AssertionFailedf("unexpected planNode type %T in ConstructSerializingProject", res)
	}
	return res, nil
}

// ConstructRender is part of the exec.Factory interface.
// N.B.: The input exprs will be modified.
func (ef *execFactory) ConstructRender(
	n exec.Node,
	columns colinfo.ResultColumns,
	exprs tree.TypedExprs,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	var rb renderBuilder
	rb.init(n, reqOrdering)
	rb.setOutput(exprs, columns)
	return rb.res, nil
}

// ConstructHashJoin is part of the exec.Factory interface.
func (ef *execFactory) ConstructHashJoin(
	joinType descpb.JoinType,
	left, right exec.Node,
	leftEqCols, rightEqCols []exec.NodeColumnOrdinal,
	leftEqColsAreKey, rightEqColsAreKey bool,
	extraOnCond tree.TypedExpr,
	estimatedLeftRowCount, estimatedRightRowCount uint64,
) (exec.Node, error) {
	p := ef.planner
	leftPlan := left.(planNode)
	rightPlan := right.(planNode)
	leftCols := planColumns(leftPlan)
	rightCols := planColumns(rightPlan)
	pred := makePredicate(joinType, leftCols, rightCols, extraOnCond)

	numEqCols := len(leftEqCols)
	pred.leftEqualityIndices = leftEqCols
	pred.rightEqualityIndices = rightEqCols
	nameBuf := make(tree.NameList, 2*numEqCols)
	pred.leftColNames = nameBuf[:numEqCols:numEqCols]
	pred.rightColNames = nameBuf[numEqCols:]

	for i := range leftEqCols {
		pred.leftColNames[i] = tree.Name(leftCols[leftEqCols[i]].Name)
		pred.rightColNames[i] = tree.Name(rightCols[rightEqCols[i]].Name)
	}
	pred.leftEqKey = leftEqColsAreKey
	pred.rightEqKey = rightEqColsAreKey

	return p.makeJoinNode(leftPlan, rightPlan, pred, estimatedLeftRowCount, estimatedRightRowCount), nil
}

// ConstructApplyJoin is part of the exec.Factory interface.
func (ef *execFactory) ConstructApplyJoin(
	joinType descpb.JoinType,
	left exec.Node,
	rightColumns colinfo.ResultColumns,
	onCond tree.TypedExpr,
	planRightSideFn exec.ApplyJoinPlanRightSideFn,
) (exec.Node, error) {
	l := left.(planNode)
	leftCols := planColumns(l)
	pred := makePredicate(joinType, leftCols, rightColumns, onCond)
	return newApplyJoinNode(joinType, l, rightColumns, pred, planRightSideFn)
}

// ConstructMergeJoin is part of the exec.Factory interface.
func (ef *execFactory) ConstructMergeJoin(
	joinType descpb.JoinType,
	left, right exec.Node,
	onCond tree.TypedExpr,
	leftOrdering, rightOrdering colinfo.ColumnOrdering,
	reqOrdering exec.OutputOrdering,
	leftEqColsAreKey, rightEqColsAreKey bool,
	estimatedLeftRowCount, estimatedRightRowCount uint64,
) (exec.Node, error) {
	var err error
	p := ef.planner
	leftPlan := left.(planNode)
	rightPlan := right.(planNode)
	leftCols := planColumns(leftPlan)
	rightCols := planColumns(rightPlan)
	pred := makePredicate(joinType, leftCols, rightCols, onCond)
	node := p.makeJoinNode(leftPlan, rightPlan, pred, estimatedLeftRowCount, estimatedRightRowCount)
	pred.leftEqKey = leftEqColsAreKey
	pred.rightEqKey = rightEqColsAreKey

	pred.leftEqualityIndices, pred.rightEqualityIndices, node.mergeJoinOrdering, err = getEqualityIndicesAndMergeJoinOrdering(leftOrdering, rightOrdering)
	if err != nil {
		return nil, err
	}
	n := len(leftOrdering)
	pred.leftColNames = make(tree.NameList, n)
	pred.rightColNames = make(tree.NameList, n)
	for i := 0; i < n; i++ {
		leftColIdx, rightColIdx := leftOrdering[i].ColIdx, rightOrdering[i].ColIdx
		pred.leftColNames[i] = tree.Name(leftCols[leftColIdx].Name)
		pred.rightColNames[i] = tree.Name(rightCols[rightColIdx].Name)
	}

	// Set up node.props, which tells the distsql planner to maintain the
	// resulting ordering (if needed).
	node.reqOrdering = ReqOrdering(reqOrdering)

	return node, nil
}

// ConstructScalarGroupBy is part of the exec.Factory interface.
func (ef *execFactory) ConstructScalarGroupBy(
	input exec.Node, aggregations []exec.AggInfo, estimatedInputRowCount uint64,
) (exec.Node, error) {
	// There are no grouping columns with scalar GroupBy, so we create empty
	// arguments upfront to be passed into getResultColumnsForGroupBy call
	// below.
	var inputCols colinfo.ResultColumns
	var groupCols []exec.NodeColumnOrdinal
	n := &groupNode{
		singleInputPlanNode:    singleInputPlanNode{input.(planNode)},
		funcs:                  make([]*aggregateFuncHolder, 0, len(aggregations)),
		columns:                getResultColumnsForGroupBy(inputCols, groupCols, aggregations),
		isScalar:               true,
		estimatedRowCount:      1,
		estimatedInputRowCount: estimatedInputRowCount,
	}
	if err := ef.addAggregations(n, aggregations); err != nil {
		return nil, err
	}
	return n, nil
}

// ConstructGroupBy is part of the exec.Factory interface.
func (ef *execFactory) ConstructGroupBy(
	input exec.Node,
	groupCols []exec.NodeColumnOrdinal,
	groupColOrdering colinfo.ColumnOrdering,
	aggregations []exec.AggInfo,
	reqOrdering exec.OutputOrdering,
	groupingOrderType exec.GroupingOrderType,
	estimatedRowCount uint64,
	estimatedInputRowCount uint64,
) (exec.Node, error) {
	inputPlan := input.(planNode)
	inputCols := planColumns(inputPlan)
	// TODO(harding): Use groupingOrder to determine when to use a hash
	// aggregator.
	n := &groupNode{
		singleInputPlanNode:    singleInputPlanNode{inputPlan},
		funcs:                  make([]*aggregateFuncHolder, 0, len(groupCols)+len(aggregations)),
		columns:                getResultColumnsForGroupBy(inputCols, groupCols, aggregations),
		groupCols:              groupCols,
		groupColOrdering:       groupColOrdering,
		isScalar:               false,
		reqOrdering:            ReqOrdering(reqOrdering),
		estimatedRowCount:      estimatedRowCount,
		estimatedInputRowCount: estimatedInputRowCount,
	}
	for _, col := range n.groupCols {
		// TODO(radu): only generate the grouping columns we actually need.
		f := newAggregateFuncHolder(
			builtins.AnyNotNull,
			[]exec.NodeColumnOrdinal{col},
			nil,   /* arguments */
			false, /* isDistinct */
			false, /* distsqlBlocklist */
		)
		n.funcs = append(n.funcs, f)
	}
	if err := ef.addAggregations(n, aggregations); err != nil {
		return nil, err
	}
	return n, nil
}

func (ef *execFactory) addAggregations(n *groupNode, aggregations []exec.AggInfo) error {
	for i := range aggregations {
		agg := &aggregations[i]

		f := newAggregateFuncHolder(
			agg.FuncName,
			agg.ArgCols,
			agg.ConstArgs,
			agg.Distinct,
			agg.DistsqlBlocklist,
		)
		f.filterRenderIdx = int(agg.Filter)

		n.funcs = append(n.funcs, f)
	}
	return nil
}

// ConstructDistinct is part of the exec.Factory interface.
func (ef *execFactory) ConstructDistinct(
	input exec.Node,
	distinctCols, orderedCols exec.NodeColumnOrdinalSet,
	reqOrdering exec.OutputOrdering,
	nullsAreDistinct bool,
	errorOnDup string,
) (exec.Node, error) {
	return &distinctNode{
		singleInputPlanNode: singleInputPlanNode{input.(planNode)},
		distinctOnColIdxs:   distinctCols,
		columnsInOrder:      orderedCols,
		reqOrdering:         ReqOrdering(reqOrdering),
		nullsAreDistinct:    nullsAreDistinct,
		errorOnDup:          errorOnDup,
	}, nil
}

// ConstructHashSetOp is part of the exec.Factory interface.
func (ef *execFactory) ConstructHashSetOp(
	typ tree.UnionType, all bool, left, right exec.Node,
) (exec.Node, error) {
	return ef.planner.newUnionNode(
		typ, all, left.(planNode), right.(planNode), nil, nil, 0, /* hardLimit */
		false, /* enforceHomeRegion */
	)
}

// ConstructStreamingSetOp is part of the exec.Factory interface.
func (ef *execFactory) ConstructStreamingSetOp(
	typ tree.UnionType,
	all bool,
	left, right exec.Node,
	streamingOrdering colinfo.ColumnOrdering,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return ef.planner.newUnionNode(
		typ,
		all,
		left.(planNode),
		right.(planNode),
		streamingOrdering,
		ReqOrdering(reqOrdering),
		0,     /* hardLimit */
		false, /* enforceHomeRegion */
	)
}

// ConstructUnionAll is part of the exec.Factory interface.
func (ef *execFactory) ConstructUnionAll(
	left, right exec.Node, reqOrdering exec.OutputOrdering, hardLimit uint64, enforceHomeRegion bool,
) (exec.Node, error) {
	return ef.planner.newUnionNode(
		tree.UnionOp,
		true, /* all */
		left.(planNode),
		right.(planNode),
		colinfo.ColumnOrdering(reqOrdering),
		ReqOrdering(reqOrdering),
		hardLimit,
		enforceHomeRegion,
	)
}

// ConstructSort is part of the exec.Factory interface.
func (ef *execFactory) ConstructSort(
	input exec.Node,
	ordering exec.OutputOrdering,
	alreadyOrderedPrefix int,
	estimatedInputRowCount uint64,
) (exec.Node, error) {
	return &sortNode{
		singleInputPlanNode:    singleInputPlanNode{input.(planNode)},
		ordering:               colinfo.ColumnOrdering(ordering),
		alreadyOrderedPrefix:   alreadyOrderedPrefix,
		estimatedInputRowCount: estimatedInputRowCount,
	}, nil
}

// ConstructOrdinality is part of the exec.Factory interface.
func (ef *execFactory) ConstructOrdinality(input exec.Node, colName string) (exec.Node, error) {
	plan := input.(planNode)
	inputColumns := planColumns(plan)
	cols := make(colinfo.ResultColumns, len(inputColumns)+1)
	copy(cols, inputColumns)
	cols[len(cols)-1] = colinfo.ResultColumn{
		Name: colName,
		Typ:  types.Int,
	}
	return &ordinalityNode{
		singleInputPlanNode: singleInputPlanNode{plan},
		columns:             cols,
	}, nil
}

// ConstructIndexJoin is part of the exec.Factory interface.
func (ef *execFactory) ConstructIndexJoin(
	input exec.Node,
	table cat.Table,
	keyCols []exec.NodeColumnOrdinal,
	tableCols exec.TableColumnOrdinalSet,
	reqOrdering exec.OutputOrdering,
	locking opt.Locking,
	limitHint int64,
) (exec.Node, error) {
	tabDesc := table.(*optTable).desc
	colCfg := makeScanColumnsConfig(table, tableCols)
	cols := makeColList(table, tableCols)

	tableScan := ef.planner.Scan()

	if err := tableScan.initTable(ef.ctx, ef.planner, tabDesc, colCfg); err != nil {
		return nil, err
	}

	idx := tabDesc.GetPrimaryIndex()
	tableScan.index = idx
	tableScan.disableBatchLimit()
	tableScan.lockingStrength = descpb.ToScanLockingStrength(locking.Strength)
	tableScan.lockingWaitPolicy = descpb.ToScanLockingWaitPolicy(locking.WaitPolicy)
	tableScan.lockingDurability = descpb.ToScanLockingDurability(locking.Durability)

	if !ef.isExplain && !ef.planner.SessionData().Internal {
		idxUsageKey := roachpb.IndexUsageKey{
			TableID: roachpb.TableID(tabDesc.GetID()),
			IndexID: roachpb.IndexID(idx.GetID()),
		}
		ef.planner.extendedEvalCtx.indexUsageStats.RecordRead(idxUsageKey)
	}

	n := &indexJoinNode{
		singleInputPlanNode: singleInputPlanNode{input.(planNode)},
		table:               tableScan,
		cols:                cols,
		resultColumns:       colinfo.ResultColumnsFromColumns(tabDesc.GetID(), cols),
		reqOrdering:         ReqOrdering(reqOrdering),
		limitHint:           limitHint,
	}

	n.keyCols = make([]int, len(keyCols))
	for i, c := range keyCols {
		n.keyCols[i] = int(c)
	}

	return n, nil
}

// ConstructLookupJoin is part of the exec.Factory interface.
func (ef *execFactory) ConstructLookupJoin(
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
) (exec.Node, error) {
	if table.IsVirtualTable() {
		return ef.constructVirtualTableLookupJoin(joinType, input, table, index, eqCols, lookupCols, onCond)
	}
	tabDesc := table.(*optTable).desc
	idx := index.(*optIndex).idx
	colCfg := makeScanColumnsConfig(table, lookupCols)
	tableScan := ef.planner.Scan()

	if err := tableScan.initTable(ef.ctx, ef.planner, tabDesc, colCfg); err != nil {
		return nil, err
	}

	tableScan.index = idx
	tableScan.lockingStrength = descpb.ToScanLockingStrength(locking.Strength)
	tableScan.lockingWaitPolicy = descpb.ToScanLockingWaitPolicy(locking.WaitPolicy)
	tableScan.lockingDurability = descpb.ToScanLockingDurability(locking.Durability)

	if !ef.isExplain && !ef.planner.SessionData().Internal {
		idxUsageKey := roachpb.IndexUsageKey{
			TableID: roachpb.TableID(tabDesc.GetID()),
			IndexID: roachpb.IndexID(idx.GetID()),
		}
		ef.planner.extendedEvalCtx.indexUsageStats.RecordRead(idxUsageKey)
	}

	n := &lookupJoinNode{
		singleInputPlanNode:        singleInputPlanNode{input.(planNode)},
		table:                      tableScan,
		joinType:                   joinType,
		eqColsAreKey:               eqColsAreKey,
		isFirstJoinInPairedJoiner:  isFirstJoinInPairedJoiner,
		isSecondJoinInPairedJoiner: isSecondJoinInPairedJoiner,
		reqOrdering:                ReqOrdering(reqOrdering),
		limitHint:                  limitHint,
		remoteOnlyLookups:          remoteOnlyLookups,
	}
	n.eqCols = make([]int, len(eqCols))
	for i, c := range eqCols {
		n.eqCols[i] = int(c)
	}
	n.columns = getJoinResultColumns(joinType, planColumns(input.(planNode)), planColumns(tableScan))
	n.lookupExpr = lookupExpr
	n.remoteLookupExpr = remoteLookupExpr
	if onCond != tree.DBoolTrue {
		n.onCond = onCond
	}
	if isFirstJoinInPairedJoiner {
		n.columns = append(n.columns, colinfo.ResultColumn{Name: "cont", Typ: types.Bool})
	}

	return n, nil
}

func (ef *execFactory) constructVirtualTableLookupJoin(
	joinType descpb.JoinType,
	input exec.Node,
	table cat.Table,
	index cat.Index,
	eqCols []exec.NodeColumnOrdinal,
	lookupCols exec.TableColumnOrdinalSet,
	onCond tree.TypedExpr,
) (exec.Node, error) {
	tn := &table.(*optVirtualTable).name
	virtual, err := ef.planner.getVirtualTabler().getVirtualTableEntry(tn, ef.planner)
	if err != nil {
		return nil, err
	}
	if !canQueryVirtualTable(ef.planner.EvalContext(), virtual) {
		return nil, newUnimplementedVirtualTableError(tn.Schema(), tn.Table())
	}
	if len(eqCols) > 1 {
		return nil, errors.AssertionFailedf("vtable indexes with more than one column aren't supported yet")
	}
	// Check for explicit use of the dummy column.
	if lookupCols.Contains(0) {
		return nil, errors.Errorf("use of %s column not allowed.", table.Column(0).ColName())
	}
	idx := index.(*optVirtualIndex).idx
	tableDesc := table.(*optVirtualTable).desc
	// Build the result columns.
	inputCols := planColumns(input.(planNode))

	if onCond == tree.DBoolTrue {
		onCond = nil
	}

	var tableScan scanNode
	// Set up a scanNode that we won't actually use, just to get the needed
	// column analysis.
	colCfg := makeScanColumnsConfig(table, lookupCols)
	if err := tableScan.initTable(ef.ctx, ef.planner, tableDesc, colCfg); err != nil {
		return nil, err
	}
	tableScan.index = idx
	vtableCols := colinfo.ResultColumnsFromColumns(tableDesc.GetID(), tableDesc.PublicColumns())
	projectedVtableCols := planColumns(&tableScan)
	var outputCols colinfo.ResultColumns
	switch joinType {
	case descpb.InnerJoin, descpb.LeftOuterJoin:
		outputCols = make(colinfo.ResultColumns, 0, len(inputCols)+len(projectedVtableCols))
		outputCols = append(outputCols, inputCols...)
		outputCols = append(outputCols, projectedVtableCols...)
	case descpb.LeftSemiJoin, descpb.LeftAntiJoin:
		outputCols = make(colinfo.ResultColumns, 0, len(inputCols))
		outputCols = append(outputCols, inputCols...)
	default:
		return nil, errors.AssertionFailedf("unexpected join type for virtual lookup join: %s", joinType.String())
	}
	pred := makePredicate(joinType, inputCols, projectedVtableCols, onCond)
	n := &vTableLookupJoinNode{
		singleInputPlanNode: singleInputPlanNode{input.(planNode)},
		joinType:            joinType,
		virtualTableEntry:   virtual,
		dbName:              tn.Catalog(),
		table:               tableDesc,
		index:               idx,
		eqCol:               int(eqCols[0]),
		inputCols:           inputCols,
		vtableCols:          vtableCols,
		lookupCols:          lookupCols,
		columns:             outputCols,
		pred:                pred,
	}
	return n, nil
}

func (ef *execFactory) ConstructInvertedJoin(
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
	tabDesc := table.(*optTable).desc
	idx := index.(*optIndex).idx
	colCfg := makeScanColumnsConfig(table, lookupCols)
	tableScan := ef.planner.Scan()

	if err := tableScan.initTable(ef.ctx, ef.planner, tabDesc, colCfg); err != nil {
		return nil, err
	}
	tableScan.index = idx
	tableScan.lockingStrength = descpb.ToScanLockingStrength(locking.Strength)
	tableScan.lockingWaitPolicy = descpb.ToScanLockingWaitPolicy(locking.WaitPolicy)
	tableScan.lockingDurability = descpb.ToScanLockingDurability(locking.Durability)

	if !ef.isExplain && !ef.planner.SessionData().Internal {
		idxUsageKey := roachpb.IndexUsageKey{
			TableID: roachpb.TableID(tabDesc.GetID()),
			IndexID: roachpb.IndexID(idx.GetID()),
		}
		ef.planner.extendedEvalCtx.indexUsageStats.RecordRead(idxUsageKey)
	}

	n := &invertedJoinNode{
		singleInputPlanNode:       singleInputPlanNode{input.(planNode)},
		table:                     tableScan,
		joinType:                  joinType,
		invertedExpr:              invertedExpr,
		isFirstJoinInPairedJoiner: isFirstJoinInPairedJoiner,
		reqOrdering:               ReqOrdering(reqOrdering),
	}
	if len(prefixEqCols) > 0 {
		n.prefixEqCols = make([]int, len(prefixEqCols))
		for i, c := range prefixEqCols {
			n.prefixEqCols[i] = int(c)
		}
	}
	if onCond != nil && onCond != tree.DBoolTrue {
		n.onExpr = onCond
	}
	// Build the result columns.
	inputCols := planColumns(input.(planNode))
	var scanCols colinfo.ResultColumns
	if joinType.ShouldIncludeRightColsInOutput() {
		scanCols = planColumns(tableScan)
	}
	numCols := len(inputCols) + len(scanCols)
	if isFirstJoinInPairedJoiner {
		numCols++
	}
	n.columns = make(colinfo.ResultColumns, 0, numCols)
	n.columns = append(n.columns, inputCols...)
	n.columns = append(n.columns, scanCols...)
	if isFirstJoinInPairedJoiner {
		n.columns = append(n.columns, colinfo.ResultColumn{Name: "cont", Typ: types.Bool})
	}
	return n, nil
}

// Helper function to create a scanNode from just a table / index descriptor
// and requested cols.
func (ef *execFactory) constructScanForZigzag(
	table cat.Table,
	index cat.Index,
	cols exec.TableColumnOrdinalSet,
	eqCols []exec.TableColumnOrdinal,
	locking opt.Locking,
) (_ *scanNode, eqColOrdinals []int, _ error) {
	colCfg := makeScanColumnsConfig(table, cols)

	var err error
	eqColOrdinals, err = tableToScanOrdinals(cols, eqCols)
	if err != nil {
		return nil, nil, err
	}

	tableDesc := table.(*optTable).desc
	idxDesc := index.(*optIndex).idx
	scan := ef.planner.Scan()
	if err := scan.initTable(ef.ctx, ef.planner, tableDesc, colCfg); err != nil {
		return nil, nil, err
	}

	if !ef.isExplain && !ef.planner.SessionData().Internal {
		idxUsageKey := roachpb.IndexUsageKey{
			TableID: roachpb.TableID(tableDesc.GetID()),
			IndexID: roachpb.IndexID(idxDesc.GetID()),
		}
		ef.planner.extendedEvalCtx.indexUsageStats.RecordRead(idxUsageKey)
	}

	scan.index = idxDesc
	scan.lockingStrength = descpb.ToScanLockingStrength(locking.Strength)
	scan.lockingWaitPolicy = descpb.ToScanLockingWaitPolicy(locking.WaitPolicy)
	scan.lockingDurability = descpb.ToScanLockingDurability(locking.Durability)

	return scan, eqColOrdinals, nil
}

// ConstructZigzagJoin is part of the exec.Factory interface.
func (ef *execFactory) ConstructZigzagJoin(
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
	if len(leftEqCols) != len(rightEqCols) {
		return nil, errors.AssertionFailedf("creating zigzag join with unequal number of equated cols")
	}

	n := &zigzagJoinNode{
		sides:       make([]zigzagJoinSide, 2),
		reqOrdering: ReqOrdering(reqOrdering),
	}
	var err error
	n.sides[0].scan, n.sides[0].eqCols, err = ef.constructScanForZigzag(leftTable, leftIndex, leftCols, leftEqCols, leftLocking)
	if err != nil {
		return nil, err
	}
	n.sides[1].scan, n.sides[1].eqCols, err = ef.constructScanForZigzag(rightTable, rightIndex, rightCols, rightEqCols, rightLocking)
	if err != nil {
		return nil, err
	}

	if onCond != nil && onCond != tree.DBoolTrue {
		n.onCond = onCond
	}

	// The resultant columns are identical to those from individual index scans; so
	// reuse the resultColumns generated in the scanNodes.
	n.columns = make(
		colinfo.ResultColumns,
		0,
		len(n.sides[0].scan.resultColumns)+len(n.sides[1].scan.resultColumns),
	)
	n.columns = append(n.columns, n.sides[0].scan.resultColumns...)
	n.columns = append(n.columns, n.sides[1].scan.resultColumns...)

	// Fixed values are the values fixed for a prefix of each side's index columns.
	// See the comment in pkg/sql/rowexec/zigzagjoiner.go for how they are used.

	// mkFixedVals creates a values node that contains a single row with values
	// for a prefix of the index columns.
	// TODO(radu): using a valuesNode to represent a single tuple is dubious.
	mkFixedVals := func(fixedVals []tree.TypedExpr, index cat.Index) *valuesNode {
		cols := make(colinfo.ResultColumns, len(fixedVals))
		for i := range cols {
			col := index.Column(i)
			cols[i].Name = string(col.ColName())
			cols[i].Typ = col.DatumType()
		}
		return &valuesNode{
			columns:          cols,
			tuples:           [][]tree.TypedExpr{fixedVals},
			specifiedInQuery: true,
		}
	}
	n.sides[0].fixedVals = mkFixedVals(leftFixedVals, leftIndex)
	n.sides[1].fixedVals = mkFixedVals(rightFixedVals, rightIndex)
	return n, nil
}

// ConstructLimit is part of the exec.Factory interface.
func (ef *execFactory) ConstructLimit(
	input exec.Node, limit, offset tree.TypedExpr,
) (exec.Node, error) {
	plan := input.(planNode)
	// If the input plan is also a limitNode that has just an offset, and we are
	// only applying a limit, update the existing node. This is useful because
	// Limit and Offset are separate operators which result in separate calls to
	// this function.
	if l, ok := plan.(*limitNode); ok && l.countExpr == nil && offset == nil {
		l.countExpr = limit
		return l, nil
	}
	// If the input plan is a spoolNode, then propagate any constant limit to it.
	if spool, ok := plan.(*spoolNode); ok {
		if val, ok := limit.(*tree.DInt); ok {
			spool.hardLimit = int64(*val)
		}
	}
	return &limitNode{
		singleInputPlanNode: singleInputPlanNode{input.(planNode)},
		countExpr:           limit,
		offsetExpr:          offset,
	}, nil
}

// ConstructTopK is part of the exec.Factory interface.
func (ef *execFactory) ConstructTopK(
	input exec.Node,
	k int64,
	ordering exec.OutputOrdering,
	alreadyOrderedPrefix int,
	estimatedInputRowCount uint64,
) (exec.Node, error) {
	return &topKNode{
		singleInputPlanNode:    singleInputPlanNode{input.(planNode)},
		k:                      k,
		ordering:               colinfo.ColumnOrdering(ordering),
		alreadyOrderedPrefix:   alreadyOrderedPrefix,
		estimatedInputRowCount: estimatedInputRowCount,
	}, nil
}

// ConstructMax1Row is part of the exec.Factory interface.
func (ef *execFactory) ConstructMax1Row(input exec.Node, errorText string) (exec.Node, error) {
	return &max1RowNode{
		singleInputPlanNode: singleInputPlanNode{input.(planNode)},
		errorText:           errorText,
	}, nil
}

// ConstructBuffer is part of the exec.Factory interface.
func (ef *execFactory) ConstructBuffer(input exec.Node, label string) (exec.Node, error) {
	return &bufferNode{
		singleInputPlanNode: singleInputPlanNode{input.(planNode)},
		label:               label,
	}, nil
}

// ConstructScanBuffer is part of the exec.Factory interface.
func (ef *execFactory) ConstructScanBuffer(ref exec.Node, label string) (exec.Node, error) {
	if n, ok := ref.(*explain.Node); ok {
		// This can happen if we used explain on the main query but we construct the
		// scan buffer inside a separate plan (e.g. recursive CTEs).
		ref = n.WrappedNode()
	}
	return &scanBufferNode{
		buffer: ref.(*bufferNode),
		label:  label,
	}, nil
}

// ConstructRecursiveCTE is part of the exec.Factory interface.
func (ef *execFactory) ConstructRecursiveCTE(
	initial exec.Node, fn exec.RecursiveCTEIterationFn, label string, deduplicate bool,
) (exec.Node, error) {
	return &recursiveCTENode{
		singleInputPlanNode: singleInputPlanNode{initial.(planNode)},
		genIterationFn:      fn,
		label:               label,
		deduplicate:         deduplicate,
	}, nil
}

// ConstructProjectSet is part of the exec.Factory interface.
func (ef *execFactory) ConstructProjectSet(
	n exec.Node, exprs tree.TypedExprs, zipCols colinfo.ResultColumns, numColsPerGen []int,
) (exec.Node, error) {
	p := n.(planNode)
	cols := planColumns(p)
	allCols := append(cols, zipCols...)
	return &projectSetNode{
		singleInputPlanNode: singleInputPlanNode{p},
		projectSetPlanningInfo: projectSetPlanningInfo{
			columns:         allCols,
			numColsInSource: len(cols),
			exprs:           exprs,
			numColsPerGen:   numColsPerGen,
		},
	}, nil
}

// ConstructWindow is part of the exec.Factory interface.
func (ef *execFactory) ConstructWindow(root exec.Node, wi exec.WindowInfo) (exec.Node, error) {
	p := &windowNode{
		singleInputPlanNode: singleInputPlanNode{root.(planNode)},
		columns:             wi.Cols,
	}

	partitionIdxs := make([]int, len(wi.Partition))
	for i, idx := range wi.Partition {
		partitionIdxs[i] = int(idx)
	}

	p.funcs = make([]*windowFuncHolder, len(wi.Exprs))
	for i := range wi.Exprs {
		argsIdxs := make([]uint32, len(wi.ArgIdxs[i]))
		for j := range argsIdxs {
			argsIdxs[j] = uint32(wi.ArgIdxs[i][j])
		}

		p.funcs[i] = &windowFuncHolder{
			expr:           wi.Exprs[i],
			args:           wi.Exprs[i].Exprs,
			argsIdxs:       argsIdxs,
			filterColIdx:   wi.FilterIdxs[i],
			outputColIdx:   wi.OutputIdxs[i],
			partitionIdxs:  partitionIdxs,
			columnOrdering: wi.Ordering,
			frame:          wi.Exprs[i].WindowDef.Frame,
		}
		if len(wi.Ordering) == 0 {
			frame := p.funcs[i].frame
			if frame.Mode == treewindow.RANGE && frame.Bounds.HasOffset() {
				// Execution requires a single column to order by when there is
				// a RANGE mode frame with at least one 'offset' bound.
				return nil, errors.AssertionFailedf("a RANGE mode frame with an offset bound must have an ORDER BY column")
			}
		}
	}

	return p, nil
}

// ConstructPlan is part of the exec.Factory interface.
func (ef *execFactory) ConstructPlan(
	root exec.Node,
	subqueries []exec.Subquery,
	cascades, triggers []exec.PostQuery,
	checks []exec.Node,
	rootRowCount int64,
	flags exec.PlanFlags,
) (exec.Plan, error) {
	// No need to spool at the root.
	if spool, ok := root.(*spoolNode); ok {
		root = spool.input
	}
	return constructPlan(ef.planner, root, subqueries, cascades, triggers, checks, rootRowCount, flags)
}

// urlOutputter handles writing strings into an encoded URL for EXPLAIN (OPT,
// ENV). It also ensures that (in the text that is encoded by the URL) each
// entry gets its own line and there's exactly one blank line between entries.
type urlOutputter struct {
	buf bytes.Buffer
}

func (e *urlOutputter) writef(format string, args ...interface{}) {
	if e.buf.Len() > 0 {
		e.buf.WriteString("\n")
	}
	fmt.Fprintf(&e.buf, format, args...)
}

func (e *urlOutputter) finish() (url.URL, error) {
	// Generate a URL that encodes all the text.
	var compressed bytes.Buffer
	encoder := base64.NewEncoder(base64.URLEncoding, &compressed)
	compressor := zlib.NewWriter(encoder)
	if _, err := e.buf.WriteTo(compressor); err != nil {
		return url.URL{}, err
	}
	if err := compressor.Close(); err != nil {
		return url.URL{}, err
	}
	if err := encoder.Close(); err != nil {
		return url.URL{}, err
	}
	return url.URL{
		Scheme:   "https",
		Host:     "cockroachdb.github.io",
		Path:     "text/decode.html",
		Fragment: compressed.String(),
	}, nil
}

// showEnv implements EXPLAIN (opt, env). It returns a node which displays
// the environment a query was run in.
func (ef *execFactory) showEnv(plan string, envOpts exec.ExplainEnvData) (exec.Node, error) {
	var out urlOutputter

	ie := ef.planner.extendedEvalCtx.ExecCfg.InternalDB.NewInternalExecutor(
		ef.planner.SessionData(),
	)
	c := makeStmtEnvCollector(ef.ctx, ef.planner, ie.(*InternalExecutor))

	// Show the version of Cockroach running.
	if err := c.PrintVersion(&out.buf); err != nil {
		return nil, err
	}
	out.writef("")
	// Show the values of all non-default session variables and session
	// settings.
	if err := c.PrintSessionSettings(&out.buf, &ef.planner.extendedEvalCtx.Settings.SV, false /* all */); err != nil {
		return nil, err
	}
	out.writef("")
	if err := c.PrintClusterSettings(&out.buf, false /* all */); err != nil {
		return nil, err
	}

	// Show the definition of each referenced catalog object.
	for i := range envOpts.Sequences {
		out.writef("")
		if err := c.PrintCreateSequence(&out.buf, &envOpts.Sequences[i]); err != nil {
			return nil, err
		}
	}

	for i := range envOpts.Tables {
		out.writef("")
		if err := c.PrintCreateTable(
			&out.buf, &envOpts.Tables[i], false, /* redactValues */
		); err != nil {
			return nil, err
		}
		out.writef("")

		// In addition to the schema, it's important to know what the table
		// statistics on each table are.

		// NOTE: We don't include the histograms because they take up a ton of
		// vertical space. Unfortunately this means that in some cases we won't be
		// able to reproduce a particular plan.
		err := c.PrintTableStats(&out.buf, &envOpts.Tables[i], true /* hideHistograms */)
		if err != nil {
			return nil, err
		}
	}
	// PrintCreateTable above omitted the FK constraints from the schema, so we
	// need to add them separately.
	for _, addFK := range envOpts.AddFKs {
		fmt.Fprintf(&out.buf, "%s;\n", addFK)
	}

	for i := range envOpts.Views {
		out.writef("")
		if err := c.PrintCreateView(&out.buf, &envOpts.Views[i], false /* redactValues */); err != nil {
			return nil, err
		}
	}

	// Show the query running. Note that this is the *entire* query, including
	// the "EXPLAIN (opt, env)" preamble.
	out.writef("%s;\n----\n%s", ef.planner.stmt.AST.String(), plan)

	url, err := out.finish()
	if err != nil {
		return nil, err
	}
	return &valuesNode{
		columns:          append(colinfo.ResultColumns(nil), colinfo.ExplainPlanColumns...),
		tuples:           [][]tree.TypedExpr{{tree.NewDString(url.String())}},
		specifiedInQuery: true,
	}, nil
}

// ConstructExplainOpt is part of the exec.Factory interface.
func (ef *execFactory) ConstructExplainOpt(
	planText string, envOpts exec.ExplainEnvData,
) (exec.Node, error) {
	// If this was an EXPLAIN (opt, env), we need to run a bunch of auxiliary
	// queries to fetch the environment info.
	if envOpts.ShowEnv {
		return ef.showEnv(planText, envOpts)
	}

	var rows [][]tree.TypedExpr
	ss := strings.Split(strings.Trim(planText, "\n"), "\n")
	for _, line := range ss {
		rows = append(rows, []tree.TypedExpr{tree.NewDString(line)})
	}

	return &valuesNode{
		columns:          append(colinfo.ResultColumns(nil), colinfo.ExplainPlanColumns...),
		tuples:           rows,
		specifiedInQuery: true,
	}, nil
}

// ConstructShowTrace is part of the exec.Factory interface.
func (ef *execFactory) ConstructShowTrace(typ tree.ShowTraceType, compact bool) (exec.Node, error) {
	var node planNode = ef.planner.makeShowTraceNode(compact, typ == tree.ShowTraceKV)

	// Ensure the messages are sorted in age order, so that the user
	// does not get confused.
	ageColIdx := colinfo.GetTraceAgeColumnIdx(compact)
	node = &sortNode{
		singleInputPlanNode: singleInputPlanNode{node},
		ordering: colinfo.ColumnOrdering{
			colinfo.ColumnOrderInfo{ColIdx: ageColIdx, Direction: encoding.Ascending},
		},
	}

	if typ == tree.ShowTraceReplica {
		node = &showTraceReplicaNode{
			singleInputPlanNode: singleInputPlanNode{node},
		}
	}
	return node, nil
}

func ordinalsToIndexes(table cat.Table, ords cat.IndexOrdinals) []catalog.Index {
	if ords == nil {
		return nil
	}

	retval := make([]catalog.Index, len(ords))
	for i, idx := range ords {
		retval[i] = table.Index(idx).(*optIndex).idx
	}
	return retval
}

func (ef *execFactory) ConstructInsert(
	input exec.Node,
	table cat.Table,
	arbiterIndexes cat.IndexOrdinals,
	arbiterConstraints cat.UniqueOrdinals,
	insertColOrdSet exec.TableColumnOrdinalSet,
	returnColOrdSet exec.TableColumnOrdinalSet,
	checkOrdSet exec.CheckOrdinalSet,
	uniqueWithTombstoneIndexes cat.IndexOrdinals,
	autoCommit bool,
) (exec.Node, error) {
	// Derive insert table and column descriptors.
	rowsNeeded := !returnColOrdSet.Empty()
	tabDesc := table.(*optTable).desc
	cols := makeColList(table, insertColOrdSet)

	// Create the table inserter, which does the bulk of the work.
	internal := ef.planner.SessionData().Internal
	ri, err := row.MakeInserter(
		ef.ctx,
		ef.planner.txn,
		ef.planner.ExecCfg().Codec,
		tabDesc,
		ordinalsToIndexes(table, uniqueWithTombstoneIndexes),
		cols,
		ef.getDatumAlloc(),
		&ef.planner.ExecCfg().Settings.SV,
		internal,
		ef.planner.ExecCfg().GetRowMetrics(internal),
	)
	if err != nil {
		return nil, err
	}

	// Regular path for INSERT.
	ins := insertNodePool.Get().(*insertNode)
	*ins = insertNode{
		singleInputPlanNode: singleInputPlanNode{input.(planNode)},
		run: insertRun{
			ti:         tableInserter{ri: ri},
			checkOrds:  checkOrdSet,
			insertCols: ri.InsertCols,
		},
	}

	ins.run.regionLocalInfo.setupEnforceHomeRegion(ef.planner, table, cols, ins.run.ti.ri.InsertColIDtoRowIndex)

	// If rows are not needed, no columns are returned.
	if rowsNeeded {
		returnCols := makeColList(table, returnColOrdSet)
		ins.columns = colinfo.ResultColumnsFromColumns(tabDesc.GetID(), returnCols)

		// Set the tabColIdxToRetIdx for the mutation. Insert always returns
		// non-mutation columns in the same order they are defined in the table.
		ins.run.tabColIdxToRetIdx = makePublicToReturnColumnIndexMapping(tabDesc, returnCols)
		ins.run.rowsNeeded = true
	}

	if autoCommit {
		ins.enableAutoCommit()
	}

	// serialize the data-modifying plan to ensure that no data is
	// observed that hasn't been validated first. See the comments
	// on BatchedNext() in plan_batch.go.
	if rowsNeeded {
		return &spoolNode{
			singleInputPlanNode: singleInputPlanNode{&serializeNode{source: ins}},
		}, nil
	}

	// We could use serializeNode here, but using rowCountNode is an
	// optimization that saves on calls to Next() by the caller.
	return &rowCountNode{source: ins}, nil
}

func (ef *execFactory) ConstructInsertFastPath(
	rows [][]tree.TypedExpr,
	table cat.Table,
	insertColOrdSet exec.TableColumnOrdinalSet,
	returnColOrdSet exec.TableColumnOrdinalSet,
	checkOrdSet exec.CheckOrdinalSet,
	fkChecks []exec.InsertFastPathCheck,
	uniqChecks []exec.InsertFastPathCheck,
	uniqueWithTombstoneIndexes cat.IndexOrdinals,
	autoCommit bool,
) (exec.Node, error) {
	// Derive insert table and column descriptors.
	rowsNeeded := !returnColOrdSet.Empty()
	tabDesc := table.(*optTable).desc
	cols := makeColList(table, insertColOrdSet)

	// Create the table inserter, which does the bulk of the work.
	internal := ef.planner.SessionData().Internal
	ri, err := row.MakeInserter(
		ef.ctx,
		ef.planner.txn,
		ef.planner.ExecCfg().Codec,
		tabDesc,
		ordinalsToIndexes(table, uniqueWithTombstoneIndexes),
		cols,
		ef.getDatumAlloc(),
		&ef.planner.ExecCfg().Settings.SV,
		internal,
		ef.planner.ExecCfg().GetRowMetrics(internal),
	)
	if err != nil {
		return nil, err
	}

	// Regular path for INSERT.
	ins := insertFastPathNodePool.Get().(*insertFastPathNode)
	*ins = insertFastPathNode{
		input: rows,
		run: insertFastPathRun{
			insertRun: insertRun{
				ti:         tableInserter{ri: ri},
				checkOrds:  checkOrdSet,
				insertCols: ri.InsertCols,
			},
		},
	}

	ins.run.regionLocalInfo.setupEnforceHomeRegion(ef.planner, table, cols, ins.run.ti.ri.InsertColIDtoRowIndex)

	if len(uniqChecks) > 0 {
		ins.run.uniqChecks = make([]insertFastPathCheck, len(uniqChecks))
		for i := range uniqChecks {
			ins.run.uniqChecks[i].InsertFastPathCheck = uniqChecks[i]
		}
	}

	if len(fkChecks) > 0 {
		ins.run.fkChecks = make([]insertFastPathCheck, len(fkChecks))
		for i := range fkChecks {
			ins.run.fkChecks[i].InsertFastPathCheck = fkChecks[i]
		}
	}

	// If rows are not needed, no columns are returned.
	if rowsNeeded {
		returnCols := makeColList(table, returnColOrdSet)
		ins.columns = colinfo.ResultColumnsFromColumns(tabDesc.GetID(), returnCols)

		// Set the tabColIdxToRetIdx for the mutation. Insert always returns
		// non-mutation columns in the same order they are defined in the table.
		ins.run.tabColIdxToRetIdx = makePublicToReturnColumnIndexMapping(tabDesc, returnCols)
		ins.run.rowsNeeded = true
	}

	if len(rows) == 0 {
		return &zeroNode{columns: ins.columns}, nil
	}

	if autoCommit {
		ins.enableAutoCommit()
	}

	// serialize the data-modifying plan to ensure that no data is
	// observed that hasn't been validated first. See the comments
	// on BatchedNext() in plan_batch.go.
	if rowsNeeded {
		return &spoolNode{
			singleInputPlanNode: singleInputPlanNode{&serializeNode{source: ins}},
		}, nil
	}

	// We could use serializeNode here, but using rowCountNode is an
	// optimization that saves on calls to Next() by the caller.
	return &rowCountNode{source: ins}, nil
}

func (ef *execFactory) ConstructUpdate(
	input exec.Node,
	table cat.Table,
	fetchColOrdSet exec.TableColumnOrdinalSet,
	updateColOrdSet exec.TableColumnOrdinalSet,
	returnColOrdSet exec.TableColumnOrdinalSet,
	checks exec.CheckOrdinalSet,
	passthrough colinfo.ResultColumns,
	uniqueWithTombstoneIndexes cat.IndexOrdinals,
	autoCommit bool,
) (exec.Node, error) {
	// TODO(radu): the execution code has an annoying limitation that the fetch
	// columns must be a superset of the update columns, even when the "old" value
	// of a column is not necessary. The optimizer code for pruning columns is
	// aware of this limitation.
	if !updateColOrdSet.SubsetOf(fetchColOrdSet) {
		return nil, errors.AssertionFailedf("execution requires all update columns have a fetch column")
	}

	// Derive table and column descriptors.
	rowsNeeded := !returnColOrdSet.Empty()
	tabDesc := table.(*optTable).desc
	fetchCols := makeColList(table, fetchColOrdSet)
	updateCols := makeColList(table, updateColOrdSet)

	// Create the table updater, which does the bulk of the work.
	internal := ef.planner.SessionData().Internal
	ru, err := row.MakeUpdater(
		ef.ctx,
		ef.planner.txn,
		ef.planner.ExecCfg().Codec,
		tabDesc,
		ordinalsToIndexes(table, uniqueWithTombstoneIndexes),
		updateCols,
		fetchCols,
		row.UpdaterDefault,
		ef.getDatumAlloc(),
		&ef.planner.ExecCfg().Settings.SV,
		internal,
		ef.planner.ExecCfg().GetRowMetrics(internal),
	)
	if err != nil {
		return nil, err
	}

	upd := updateNodePool.Get().(*updateNode)
	*upd = updateNode{
		singleInputPlanNode: singleInputPlanNode{input.(planNode)},
		run: updateRun{
			tu:             tableUpdater{ru: ru},
			checkOrds:      checks,
			numPassthrough: len(passthrough),
		},
	}

	upd.run.regionLocalInfo.setupEnforceHomeRegion(ef.planner, table, ru.UpdateCols,
		upd.run.tu.ru.UpdateColIDtoRowIndex)

	// If rows are not needed, no columns are returned.
	if rowsNeeded {
		returnCols := makeColList(table, returnColOrdSet)

		upd.columns = colinfo.ResultColumnsFromColumns(tabDesc.GetID(), returnCols)
		// Add the passthrough columns to the returning columns.
		upd.columns = append(upd.columns, passthrough...)

		// Set the rowIdxToRetIdx for the mutation. Update returns the non-mutation
		// columns specified, in the same order they are defined in the table.
		//
		// The Updater derives/stores the fetch columns of the mutation and
		// since the return columns are always a subset of the fetch columns,
		// we can use use the fetch columns to generate the mapping for the
		// returned rows.
		upd.run.rowIdxToRetIdx = row.ColMapping(ru.FetchCols, returnCols)
		upd.run.rowsNeeded = true
	}

	if autoCommit {
		upd.enableAutoCommit()
	}

	// Serialize the data-modifying plan to ensure that no data is observed that
	// hasn't been validated first. See the comments on BatchedNext() in
	// plan_batch.go.
	if rowsNeeded {
		return &spoolNode{
			singleInputPlanNode: singleInputPlanNode{&serializeNode{source: upd}},
		}, nil
	}

	// We could use serializeNode here, but using rowCountNode is an
	// optimization that saves on calls to Next() by the caller.
	return &rowCountNode{source: upd}, nil
}

func (ef *execFactory) ConstructUpsert(
	input exec.Node,
	table cat.Table,
	arbiterIndexes cat.IndexOrdinals,
	arbiterConstraints cat.UniqueOrdinals,
	canaryCol exec.NodeColumnOrdinal,
	insertColOrdSet exec.TableColumnOrdinalSet,
	fetchColOrdSet exec.TableColumnOrdinalSet,
	updateColOrdSet exec.TableColumnOrdinalSet,
	returnColOrdSet exec.TableColumnOrdinalSet,
	checks exec.CheckOrdinalSet,
	uniqueWithTombstoneIndexes cat.IndexOrdinals,
	autoCommit bool,
) (exec.Node, error) {
	// Derive table and column descriptors.
	rowsNeeded := !returnColOrdSet.Empty()
	tabDesc := table.(*optTable).desc
	insertCols := makeColList(table, insertColOrdSet)
	fetchCols := makeColList(table, fetchColOrdSet)
	updateCols := makeColList(table, updateColOrdSet)

	// Create the table inserter, which does the bulk of the insert-related work.
	internal := ef.planner.SessionData().Internal
	ri, err := row.MakeInserter(
		ef.ctx,
		ef.planner.txn,
		ef.planner.ExecCfg().Codec,
		tabDesc,
		ordinalsToIndexes(table, uniqueWithTombstoneIndexes),
		insertCols,
		ef.getDatumAlloc(),
		&ef.planner.ExecCfg().Settings.SV,
		internal,
		ef.planner.ExecCfg().GetRowMetrics(internal),
	)
	if err != nil {
		return nil, err
	}

	// Create the table updater, which does the bulk of the update-related work.
	ru, err := row.MakeUpdater(
		ef.ctx,
		ef.planner.txn,
		ef.planner.ExecCfg().Codec,
		tabDesc,
		ordinalsToIndexes(table, uniqueWithTombstoneIndexes),
		updateCols,
		fetchCols,
		row.UpdaterDefault,
		ef.getDatumAlloc(),
		&ef.planner.ExecCfg().Settings.SV,
		internal,
		ef.planner.ExecCfg().GetRowMetrics(internal),
	)
	if err != nil {
		return nil, err
	}

	// Instantiate the upsert node.
	ups := upsertNodePool.Get().(*upsertNode)
	*ups = upsertNode{
		singleInputPlanNode: singleInputPlanNode{input.(planNode)},
		run: upsertRun{
			checkOrds:  checks,
			insertCols: ri.InsertCols,
			tw: tableUpserter{
				ri:            ri,
				canaryOrdinal: int(canaryCol),
				fetchCols:     fetchCols,
				updateCols:    updateCols,
				ru:            ru,
			},
		},
	}

	// If rows are not needed, no columns are returned.
	if rowsNeeded {
		returnCols := makeColList(table, returnColOrdSet)
		ups.columns = colinfo.ResultColumnsFromColumns(tabDesc.GetID(), returnCols)

		// Update the tabColIdxToRetIdx for the mutation. Upsert returns
		// non-mutation columns specified, in the same order they are defined
		// in the table.
		ups.run.tw.tabColIdxToRetIdx = makePublicToReturnColumnIndexMapping(tabDesc, returnCols)
		ups.run.tw.returnCols = returnCols
		ups.run.tw.rowsNeeded = true
	}

	if autoCommit {
		ups.enableAutoCommit()
	}

	// Serialize the data-modifying plan to ensure that no data is observed that
	// hasn't been validated first. See the comments on BatchedNext() in
	// plan_batch.go.
	if rowsNeeded {
		return &spoolNode{
			singleInputPlanNode: singleInputPlanNode{&serializeNode{source: ups}},
		}, nil
	}

	// We could use serializeNode here, but using rowCountNode is an
	// optimization that saves on calls to Next() by the caller.
	return &rowCountNode{source: ups}, nil
}

func (ef *execFactory) ConstructDelete(
	input exec.Node,
	table cat.Table,
	fetchColOrdSet exec.TableColumnOrdinalSet,
	returnColOrdSet exec.TableColumnOrdinalSet,
	passthrough colinfo.ResultColumns,
	autoCommit bool,
) (exec.Node, error) {
	// Derive table and column descriptors.
	rowsNeeded := !returnColOrdSet.Empty()
	tabDesc := table.(*optTable).desc
	fetchCols := makeColList(table, fetchColOrdSet)

	// Create the table deleter, which does the bulk of the work.
	internal := ef.planner.SessionData().Internal
	rd := row.MakeDeleter(
		ef.planner.ExecCfg().Codec,
		tabDesc,
		fetchCols,
		&ef.planner.ExecCfg().Settings.SV,
		internal,
		ef.planner.ExecCfg().GetRowMetrics(internal),
	)

	// Now make a delete node. We use a pool.
	del := deleteNodePool.Get().(*deleteNode)
	*del = deleteNode{
		singleInputPlanNode: singleInputPlanNode{input.(planNode)},
		run: deleteRun{
			td:             tableDeleter{rd: rd, alloc: ef.getDatumAlloc()},
			numPassthrough: len(passthrough),
		},
	}

	// If rows are not needed, no columns are returned.
	if rowsNeeded {
		returnCols := makeColList(table, returnColOrdSet)
		// Delete returns the non-mutation columns specified, in the same
		// order they are defined in the table.
		del.columns = colinfo.ResultColumnsFromColumns(tabDesc.GetID(), returnCols)

		// Add the passthrough columns to the returning columns.
		del.columns = append(del.columns, passthrough...)

		del.run.rowIdxToRetIdx = row.ColMapping(rd.FetchCols, returnCols)
		del.run.rowsNeeded = true
	}

	if autoCommit {
		del.enableAutoCommit()
	}

	// Serialize the data-modifying plan to ensure that no data is observed that
	// hasn't been validated first. See the comments on BatchedNext() in
	// plan_batch.go.
	if rowsNeeded {
		return &spoolNode{
			singleInputPlanNode: singleInputPlanNode{&serializeNode{source: del}},
		}, nil
	}

	// We could use serializeNode here, but using rowCountNode is an
	// optimization that saves on calls to Next() by the caller.
	return &rowCountNode{source: del}, nil
}

func (ef *execFactory) ConstructDeleteRange(
	table cat.Table,
	needed exec.TableColumnOrdinalSet,
	indexConstraint *constraint.Constraint,
	autoCommit bool,
) (exec.Node, error) {
	tabDesc := table.(*optTable).desc
	var sb span.Builder
	sb.Init(ef.planner.EvalContext(), ef.planner.ExecCfg().Codec, tabDesc, tabDesc.GetPrimaryIndex())

	splitter := span.MakeSplitterForDelete(tabDesc, tabDesc.GetPrimaryIndex(), needed)
	spans, err := sb.SpansFromConstraint(indexConstraint, splitter)
	if err != nil {
		return nil, err
	}

	dr := &deleteRangeNode{
		spans:             spans,
		desc:              tabDesc,
		autoCommitEnabled: autoCommit,
	}

	return dr, nil
}

// ConstructVectorSearch is part of the exec.Factory interface.
func (ef *execFactory) ConstructVectorSearch(
	table cat.Table,
	index cat.Index,
	outCols exec.TableColumnOrdinalSet,
	prefixKey constraint.Key,
	queryVector tree.TypedExpr,
	targetNeighborCount uint64,
) (exec.Node, error) {
	tabDesc := table.(*optTable).desc
	indexDesc := index.(*optIndex).idx
	cols := makeColList(table, outCols)
	resultCols := colinfo.ResultColumnsFromColumns(tabDesc.GetID(), cols)

	// Encode the prefix values as a roachpb.Key.
	var sb span.Builder
	sb.Init(ef.planner.EvalContext(), ef.planner.ExecCfg().Codec, tabDesc, indexDesc)
	encPrefixKey, _, err := sb.EncodeConstraintKey(prefixKey)
	if err != nil {
		return nil, err
	}
	return &vectorSearchNode{
		table:               tabDesc,
		index:               indexDesc,
		prefixKey:           encPrefixKey,
		queryVector:         queryVector,
		targetNeighborCount: targetNeighborCount,
		cols:                cols,
		resultCols:          resultCols,
	}, nil
}

// ConstructVectorMutationSearch is part of the exec.Factory interface.
func (ef *execFactory) ConstructVectorMutationSearch(
	input exec.Node,
	table cat.Table,
	index cat.Index,
	prefixKeyCols []exec.NodeColumnOrdinal,
	queryVectorCol exec.NodeColumnOrdinal,
	suffixKeyCols []exec.NodeColumnOrdinal,
	isIndexPut bool,
) (exec.Node, error) {
	// Pass through the input columns, and project the partition key column and
	// optionally the quantized vectors.
	inputPlan := input.(planNode)
	inputColumns := planColumns(inputPlan)
	cols := make(colinfo.ResultColumns, len(inputColumns), len(inputColumns)+2)
	copy(cols, inputColumns)
	cols = append(cols, colinfo.ResultColumn{Name: "partition-key", Typ: types.Int})
	if isIndexPut {
		cols = append(cols, colinfo.ResultColumn{Name: "quantized-vector", Typ: types.Bytes})
	}

	return &vectorMutationSearchNode{
		singleInputPlanNode: singleInputPlanNode{input: inputPlan},
		table:               table.(*optTable).desc,
		index:               index.(*optIndex).idx,
		prefixKeyCols:       prefixKeyCols,
		queryVectorCol:      queryVectorCol,
		suffixKeyCols:       suffixKeyCols,
		isIndexPut:          isIndexPut,
		columns:             cols,
	}, nil
}

// ConstructCreateTable is part of the exec.Factory interface.
func (ef *execFactory) ConstructCreateTable(
	schema cat.Schema, ct *tree.CreateTable,
) (exec.Node, error) {
	if err := checkSchemaChangeEnabled(
		ef.ctx,
		ef.planner.ExecCfg(),
		"CREATE TABLE",
	); err != nil {
		return nil, err
	}
	return &createTableNode{
		n:      ct,
		dbDesc: schema.(*optSchema).database,
	}, nil
}

// ConstructCreateTableAs is part of the exec.Factory interface.
func (ef *execFactory) ConstructCreateTableAs(
	input exec.Node, schema cat.Schema, ct *tree.CreateTable,
) (exec.Node, error) {
	if err := checkSchemaChangeEnabled(
		ef.ctx,
		ef.planner.ExecCfg(),
		"CREATE TABLE",
	); err != nil {
		return nil, err
	}

	return &createTableNode{
		n:      ct,
		dbDesc: schema.(*optSchema).database,
		input:  input.(planNode),
	}, nil
}

// ConstructCreateView is part of the exec.Factory interface.
func (ef *execFactory) ConstructCreateView(
	createView *tree.CreateView,
	schema cat.Schema,
	viewQuery string,
	columns colinfo.ResultColumns,
	deps opt.SchemaDeps,
	typeDeps opt.SchemaTypeDeps,
) (exec.Node, error) {

	if err := checkSchemaChangeEnabled(
		ef.ctx,
		ef.planner.ExecCfg(),
		"CREATE VIEW",
	); err != nil {
		return nil, err
	}

	planDeps, typeDepSet, _, err := toPlanDependencies(deps, typeDeps, intsets.Fast{} /* funcDeps */)
	if err != nil {
		return nil, err
	}

	return &createViewNode{
		createView: createView,
		viewQuery:  viewQuery,
		dbDesc:     schema.(*optSchema).database,
		columns:    columns,
		planDeps:   planDeps,
		typeDeps:   typeDepSet,
	}, nil
}

// ConstructCreateFunction is part of the exec.Factory interface.
func (ef *execFactory) ConstructCreateFunction(
	schema cat.Schema,
	cf *tree.CreateRoutine,
	deps opt.SchemaDeps,
	typeDeps opt.SchemaTypeDeps,
	functionDeps opt.SchemaFunctionDeps,
) (exec.Node, error) {

	if err := checkSchemaChangeEnabled(
		ef.ctx,
		ef.planner.ExecCfg(),
		"CREATE FUNCTION",
	); err != nil {
		return nil, err
	}

	plan, err := ef.planner.SchemaChange(ef.ctx, cf)
	if err != nil {
		return nil, err
	}
	if plan != nil {
		return plan, nil
	}

	planDeps, typeDepSet, funcDepList, err := toPlanDependencies(deps, typeDeps, functionDeps)
	if err != nil {
		return nil, err
	}

	return &createFunctionNode{
		cf:           cf,
		dbDesc:       schema.(*optSchema).database,
		scDesc:       schema.(*optSchema).schema,
		planDeps:     planDeps,
		typeDeps:     typeDepSet,
		functionDeps: funcDepList,
	}, nil
}

// ConstructCreateTrigger is part of the exec.Factory interface.
func (ef *execFactory) ConstructCreateTrigger(ct *tree.CreateTrigger) (exec.Node, error) {
	if err := checkSchemaChangeEnabled(
		ef.ctx,
		ef.planner.ExecCfg(),
		"CREATE TRIGGER",
	); err != nil {
		return nil, err
	}
	plan, err := ef.planner.SchemaChange(ef.ctx, ct)
	if err != nil {
		return nil, err
	}
	if plan == nil {
		return nil, pgerror.New(pgcode.FeatureNotSupported,
			"CREATE TRIGGER is only implemented in the declarative schema changer")
	}
	return plan, nil
}

func toPlanDependencies(
	deps opt.SchemaDeps, typeDeps opt.SchemaTypeDeps, funcDeps opt.SchemaFunctionDeps,
) (planDependencies, typeDependencies, functionDependencies, error) {
	planDeps := make(planDependencies, len(deps))
	for _, d := range deps {
		desc, err := getDescForDataSource(d.DataSource)
		if err != nil {
			return nil, nil, nil, err
		}
		var ref descpb.TableDescriptor_Reference
		if d.SpecificIndex {
			idx := d.DataSource.(cat.Table).Index(d.Index)
			ref.IndexID = idx.(*optIndex).idx.GetID()
		}
		if !d.ColumnOrdinals.Empty() {
			ref.ColumnIDs = make([]descpb.ColumnID, 0, d.ColumnOrdinals.Len())
			d.ColumnOrdinals.ForEach(func(ord int) {
				ref.ColumnIDs = append(ref.ColumnIDs, desc.AllColumns()[ord].GetID())
			})
		}
		entry := planDeps[desc.GetID()]
		entry.desc = desc
		entry.deps = append(entry.deps, ref)
		planDeps[desc.GetID()] = entry
	}

	typeDepSet := make(typeDependencies, typeDeps.Len())
	typeDeps.ForEach(func(id int) {
		typeDepSet[descpb.ID(id)] = struct{}{}
	})

	funcDepList := make(functionDependencies, funcDeps.Len())
	funcDeps.ForEach(func(i int) {
		descID := funcdesc.UserDefinedFunctionOIDToID(oid.Oid(i))
		funcDepList[descID] = struct{}{}
	})
	return planDeps, typeDepSet, funcDepList, nil
}

// ConstructSequenceSelect is part of the exec.Factory interface.
func (ef *execFactory) ConstructSequenceSelect(sequence cat.Sequence) (exec.Node, error) {
	return ef.planner.SequenceSelectNode(sequence.(*optSequence).desc)
}

// ConstructSaveTable is part of the exec.Factory interface.
func (ef *execFactory) ConstructSaveTable(
	input exec.Node, table *cat.DataSourceName, colNames []string,
) (exec.Node, error) {
	return ef.planner.makeSaveTable(input.(planNode), table, colNames), nil
}

// ConstructErrorIfRows is part of the exec.Factory interface.
func (ef *execFactory) ConstructErrorIfRows(
	input exec.Node, mkErr exec.MkErrFn,
) (exec.Node, error) {
	return &errorIfRowsNode{
		singleInputPlanNode: singleInputPlanNode{input.(planNode)},
		mkErr:               mkErr,
	}, nil
}

// ConstructOpaque is part of the exec.Factory interface.
func (ef *execFactory) ConstructOpaque(metadata opt.OpaqueMetadata) (exec.Node, error) {
	return constructOpaque(metadata)
}

// ConstructAlterTableSplit is part of the exec.Factory interface.
func (ef *execFactory) ConstructAlterTableSplit(
	index cat.Index, input exec.Node, expiration tree.TypedExpr,
) (exec.Node, error) {

	execCfg := ef.planner.ExecCfg()
	if err := checkSchemaChangeEnabled(
		ef.ctx,
		execCfg,
		"ALTER TABLE/INDEX SPLIT AT",
	); err != nil {
		return nil, err
	}

	if err := sqlclustersettings.RequireSystemTenantOrClusterSetting(execCfg.Codec, execCfg.Settings, SecondaryTenantSplitAtEnabled); err != nil {
		return nil, err
	}

	expirationTime, err := parseExpirationTime(ef.ctx, ef.planner.EvalContext(), expiration)
	if err != nil {
		return nil, err
	}

	return &splitNode{
		tableDesc:           index.Table().(*optTable).desc,
		index:               index.(*optIndex).idx,
		singleInputPlanNode: singleInputPlanNode{input.(planNode)},
		expirationTime:      expirationTime,
	}, nil
}

// ConstructAlterTableUnsplit is part of the exec.Factory interface.
func (ef *execFactory) ConstructAlterTableUnsplit(
	index cat.Index, input exec.Node,
) (exec.Node, error) {
	if err := checkSchemaChangeEnabled(
		ef.ctx,
		ef.planner.ExecCfg(),
		"ALTER TABLE/INDEX UNSPLIT AT",
	); err != nil {
		return nil, err
	}
	return &unsplitNode{
		tableDesc:           index.Table().(*optTable).desc,
		index:               index.(*optIndex).idx,
		singleInputPlanNode: singleInputPlanNode{input.(planNode)},
	}, nil
}

// ConstructAlterTableUnsplitAll is part of the exec.Factory interface.
func (ef *execFactory) ConstructAlterTableUnsplitAll(index cat.Index) (exec.Node, error) {
	if err := checkSchemaChangeEnabled(
		ef.ctx,
		ef.planner.ExecCfg(),
		"ALTER TABLE/INDEX UNSPLIT ALL",
	); err != nil {
		return nil, err
	}
	return &unsplitAllNode{
		tableDesc: index.Table().(*optTable).desc,
		index:     index.(*optIndex).idx,
	}, nil
}

// ConstructAlterTableRelocate is part of the exec.Factory interface.
func (ef *execFactory) ConstructAlterTableRelocate(
	index cat.Index, input exec.Node, relocateSubject tree.RelocateSubject,
) (exec.Node, error) {
	return &relocateNode{
		subjectReplicas:     relocateSubject,
		tableDesc:           index.Table().(*optTable).desc,
		index:               index.(*optIndex).idx,
		singleInputPlanNode: singleInputPlanNode{input.(planNode)},
	}, nil
}

// ConstructAlterRangeRelocate is part of the exec.Factory interface.
func (ef *execFactory) ConstructAlterRangeRelocate(
	input exec.Node,
	relocateSubject tree.RelocateSubject,
	toStoreID tree.TypedExpr,
	fromStoreID tree.TypedExpr,
) (exec.Node, error) {
	return &relocateRange{
		singleInputPlanNode: singleInputPlanNode{input.(planNode)},
		subjectReplicas:     relocateSubject,
		toStoreID:           toStoreID,
		fromStoreID:         fromStoreID,
	}, nil
}

// ConstructControlJobs is part of the exec.Factory interface.
func (ef *execFactory) ConstructControlJobs(
	command tree.JobCommand, input exec.Node, reason tree.TypedExpr,
) (exec.Node, error) {
	reasonDatum, err := eval.Expr(ef.ctx, ef.planner.EvalContext(), reason)
	if err != nil {
		return nil, err
	}

	var reasonStr string
	if reasonDatum != tree.DNull {
		reasonStrDatum, ok := reasonDatum.(*tree.DString)
		if !ok {
			return nil, errors.Errorf("expected string value for the reason")
		}
		reasonStr = string(*reasonStrDatum)
	}

	return &controlJobsNode{
		singleInputPlanNode: singleInputPlanNode{input.(planNode)},
		desiredStatus:       jobCommandToDesiredStatus[command],
		reason:              reasonStr,
	}, nil
}

// ConstructControlJobs is part of the exec.Factory interface.
func (ef *execFactory) ConstructControlSchedules(
	command tree.ScheduleCommand, input exec.Node,
) (exec.Node, error) {
	return &controlSchedulesNode{
		singleInputPlanNode: singleInputPlanNode{input.(planNode)},
		command:             command,
	}, nil
}

// ConstructShowCompletions is part of the exec.Factory interface.
func (ef *execFactory) ConstructShowCompletions(command *tree.ShowCompletions) (exec.Node, error) {
	return &completionsNode{
		n: command,
	}, nil
}

// ConstructCancelQueries is part of the exec.Factory interface.
func (ef *execFactory) ConstructCancelQueries(input exec.Node, ifExists bool) (exec.Node, error) {
	return &cancelQueriesNode{
		singleInputPlanNode: singleInputPlanNode{input.(planNode)},
		ifExists:            ifExists,
	}, nil
}

// ConstructCancelSessions is part of the exec.Factory interface.
func (ef *execFactory) ConstructCancelSessions(input exec.Node, ifExists bool) (exec.Node, error) {
	return &cancelSessionsNode{
		singleInputPlanNode: singleInputPlanNode{input.(planNode)},
		ifExists:            ifExists,
	}, nil
}

// ConstructCreateStatistics is part of the exec.Factory interface.
func (ef *execFactory) ConstructCreateStatistics(cs *tree.CreateStats) (exec.Node, error) {
	if err := featureflag.CheckEnabled(
		ef.ctx,
		ef.planner.ExecCfg(),
		featureStatsEnabled,
		"ANALYZE/CREATE STATISTICS",
	); err != nil {
		return nil, err
	}
	// Don't run as a job if we are inside an EXPLAIN / EXPLAIN ANALYZE. That will
	// allow us to get insight into the actual execution.
	runAsJob := !ef.isExplain && ef.planner.instrumentation.ShouldUseJobForCreateStats()

	return &createStatsNode{
		CreateStats: *cs,
		p:           ef.planner,
		runAsJob:    runAsJob,
	}, nil
}

// ConstructExplain is part of the exec.Factory interface.
func (ef *execFactory) ConstructExplain(
	options *tree.ExplainOptions,
	stmtType tree.StatementReturnType,
	buildFn exec.BuildPlanForExplainFn,
) (exec.Node, error) {
	if options.Flags[tree.ExplainFlagEnv] {
		return nil, errors.New("ENV only supported with (OPT) option")
	}

	plan, err := buildFn(&execFactory{
		ctx:       ef.ctx,
		planner:   ef.planner,
		isExplain: true,
	})
	if err != nil {
		return nil, err
	}
	if options.Mode == tree.ExplainVec {
		wrappedPlan := plan.(*explain.Plan).WrappedPlan.(*planComponents)
		return &explainVecNode{
			options: options,
			plan:    *wrappedPlan,
		}, nil
	}
	if options.Mode == tree.ExplainDDL {
		wrappedPlan := plan.(*explain.Plan).WrappedPlan.(*planComponents)
		return &explainDDLNode{
			options: options,
			plan:    *wrappedPlan,
		}, nil
	}
	flags := explain.MakeFlags(options)
	if ef.planner.execCfg.TestingKnobs.DeterministicExplain {
		flags.Deflake = explain.DeflakeVolatile
	}
	n := &explainPlanNode{
		options: options,
		flags:   flags,
		plan:    plan.(*explain.Plan),
	}
	return n, nil
}

// ConstructCall is part of the exec.Factory interface.
func (e *execFactory) ConstructCall(proc *tree.RoutineExpr) (exec.Node, error) {
	return &callNode{proc: proc}, nil
}

// renderBuilder encapsulates the code to build a renderNode.
type renderBuilder struct {
	r   *renderNode
	res planNode
}

// init initializes the renderNode with render expressions.
func (rb *renderBuilder) init(n exec.Node, reqOrdering exec.OutputOrdering) {
	p := n.(planNode)
	rb.r = &renderNode{
		singleInputPlanNode: singleInputPlanNode{p},
		columns:             planColumns(p),
	}
	rb.r.reqOrdering = ReqOrdering(reqOrdering)

	// If there's a spool, pull it up.
	if spool, ok := rb.r.input.(*spoolNode); ok {
		rb.r.input = spool.input
		spool.input = rb.r
		rb.res = spool
	} else {
		rb.res = rb.r
	}
}

// setOutput sets the output of the renderNode. exprs is the list of render
// expressions, and columns is the list of information about the expressions,
// including their names, types, and so on. They must be the same length.
func (rb *renderBuilder) setOutput(exprs tree.TypedExprs, columns colinfo.ResultColumns) {
	rb.r.render = exprs
	rb.r.columns = columns
}

// makeColList returns a list of table column interfaces. Columns are
// included if their ordinal position in the table schema is in the cols set.
func makeColList(table cat.Table, cols exec.TableColumnOrdinalSet) []catalog.Column {
	tab := table.(optCatalogTableInterface)
	ret := make([]catalog.Column, 0, cols.Len())
	for i, n := 0, table.ColumnCount(); i < n; i++ {
		if !cols.Contains(i) {
			continue
		}
		ret = append(ret, tab.getCol(i))
	}
	return ret
}

// makePublicToReturnColumnIndexMapping returns a map from the ordinals
// of the table's public columns to ordinals in the returnColDescs slice.
//
//	More precisely, for 0 <= i < len(tableDesc.PublicColumns()):
//	 result[i] = j such that returnColDescs[j].ID is the ID of
//	                 the i'th public column, or
//	            -1 if the i'th public column is not found in returnColDescs.
func makePublicToReturnColumnIndexMapping(
	tableDesc catalog.TableDescriptor, returnCols []catalog.Column,
) []int {
	return row.ColMapping(tableDesc.PublicColumns(), returnCols)
}
