// Copyright 2018 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/errors"
)

type execFactory struct {
	planner *planner
	// isExplain is true if this factory is used to build a statement inside
	// EXPLAIN or EXPLAIN ANALYZE.
	isExplain bool
}

var _ exec.Factory = &execFactory{}

func newExecFactory(p *planner) *execFactory {
	return &execFactory{
		planner: p,
	}
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

// ConstructScan is part of the exec.Factory interface.
func (ef *execFactory) ConstructScan(
	table cat.Table, index cat.Index, params exec.ScanParams, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	if table.IsVirtualTable() {
		return ef.constructVirtualScan(table, index, params, reqOrdering)
	}

	tabDesc := table.(*optTable).desc
	indexDesc := index.(*optIndex).desc
	// Create a scanNode.
	scan := ef.planner.Scan()
	colCfg := makeScanColumnsConfig(table, params.NeededCols)

	// initTable checks that the current user has the correct privilege to access
	// the table. However, the privilege has already been checked in optbuilder,
	// and does not need to be rechecked. In fact, it's an error to check the
	// privilege if the table was originally part of a view, since lower privilege
	// users might be able to access a view that uses a higher privilege table.
	ef.planner.skipSelectPrivilegeChecks = true
	defer func() { ef.planner.skipSelectPrivilegeChecks = false }()
	if err := scan.initTable(context.TODO(), ef.planner, tabDesc, nil, colCfg); err != nil {
		return nil, err
	}

	if params.IndexConstraint != nil && params.IndexConstraint.IsContradiction() {
		return newZeroNode(scan.resultColumns), nil
	}

	scan.index = indexDesc
	scan.hardLimit = params.HardLimit
	scan.softLimit = params.SoftLimit

	scan.reverse = params.Reverse
	scan.parallelize = params.Parallelize
	var err error
	scan.spans, err = generateScanSpans(ef.planner.EvalContext(), ef.planner.ExecCfg().Codec, tabDesc, indexDesc, params)
	if err != nil {
		return nil, err
	}

	scan.isFull = len(scan.spans) == 1 && scan.spans[0].EqualValue(
		scan.desc.IndexSpan(ef.planner.ExecCfg().Codec, scan.index.ID),
	)
	if err = colCfg.assertValidReqOrdering(reqOrdering); err != nil {
		return nil, err
	}
	scan.reqOrdering = ReqOrdering(reqOrdering)
	scan.estimatedRowCount = uint64(params.EstimatedRowCount)
	if params.Locking != nil {
		scan.lockingStrength = descpb.ToScanLockingStrength(params.Locking.Strength)
		scan.lockingWaitPolicy = descpb.ToScanLockingWaitPolicy(params.Locking.WaitPolicy)
	}
	return scan, nil
}

func generateScanSpans(
	evalCtx *tree.EvalContext,
	codec keys.SQLCodec,
	tabDesc *tabledesc.Immutable,
	indexDesc *descpb.IndexDescriptor,
	params exec.ScanParams,
) (roachpb.Spans, error) {
	sb := span.MakeBuilder(evalCtx, codec, tabDesc, indexDesc)
	if params.InvertedConstraint != nil {
		return sb.SpansFromInvertedSpans(params.InvertedConstraint, params.IndexConstraint)
	}
	return sb.SpansFromConstraint(params.IndexConstraint, params.NeededCols, false /* forDelete */)
}

func (ef *execFactory) constructVirtualScan(
	table cat.Table, index cat.Index, params exec.ScanParams, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return constructVirtualScan(
		ef, ef.planner, table, index, params, reqOrdering,
		func(d *delayedNode) (exec.Node, error) { return d, nil },
	)
}

func asDataSource(n exec.Node) planDataSource {
	plan := n.(planNode)
	return planDataSource{
		columns: planColumns(plan),
		plan:    plan,
	}
}

// ConstructFilter is part of the exec.Factory interface.
func (ef *execFactory) ConstructFilter(
	n exec.Node, filter tree.TypedExpr, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	// Create a filterNode.
	src := asDataSource(n)
	f := &filterNode{
		source: src,
	}
	f.ivarHelper = tree.MakeIndexedVarHelper(f, len(src.columns))
	f.filter = f.ivarHelper.Rebind(filter)
	if f.filter == nil {
		// Filter statically evaluates to true. Just return the input plan.
		return n, nil
	}
	f.reqOrdering = ReqOrdering(reqOrdering)

	// If there's a spool, pull it up.
	if spool, ok := f.source.plan.(*spoolNode); ok {
		f.source.plan = spool.source
		spool.source = f
		return spool, nil
	}
	return f, nil
}

// ConstructInvertedFilter is part of the exec.Factory interface.
func (ef *execFactory) ConstructInvertedFilter(
	n exec.Node,
	invFilter *invertedexpr.SpanExpression,
	preFiltererExpr tree.TypedExpr,
	preFiltererType *types.T,
	invColumn exec.NodeColumnOrdinal,
) (exec.Node, error) {
	inputCols := planColumns(n.(planNode))
	columns := make(colinfo.ResultColumns, len(inputCols))
	copy(columns, inputCols)
	n = &invertedFilterNode{
		input:           n.(planNode),
		expression:      invFilter,
		preFiltererExpr: preFiltererExpr,
		preFiltererType: preFiltererType,
		invColumn:       int(invColumn),
		resultColumns:   columns,
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
	// If the top node is already a renderNode, just rearrange the columns. But
	// we don't want to duplicate a rendering expression (in case it is expensive
	// to compute or has side-effects); so if we have duplicates we avoid this
	// optimization (and add a new renderNode).
	if r, ok := n.(*renderNode); ok && !hasDuplicates(cols) {
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
	var inputCols colinfo.ResultColumns
	if colNames == nil {
		// We will need the names of the input columns.
		inputCols = planColumns(n.(planNode))
	}

	var rb renderBuilder
	rb.init(n, reqOrdering)

	exprs := make(tree.TypedExprs, len(cols))
	for i, col := range cols {
		exprs[i] = rb.r.ivarHelper.IndexedVar(int(col))
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

func hasDuplicates(cols []exec.NodeColumnOrdinal) bool {
	var set util.FastIntSet
	for _, c := range cols {
		if set.Contains(int(c)) {
			return true
		}
		set.Add(int(c))
	}
	return false
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
			return n, nil
		}
	}
	return constructSimpleProjectForPlanNode(node, cols, colNames, nil /* reqOrdering */)
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
	for i, expr := range exprs {
		exprs[i] = rb.r.ivarHelper.Rebind(expr)
	}
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
) (exec.Node, error) {
	p := ef.planner
	leftSrc := asDataSource(left)
	rightSrc := asDataSource(right)
	pred := makePredicate(joinType, leftSrc.columns, rightSrc.columns)

	numEqCols := len(leftEqCols)
	pred.leftEqualityIndices = leftEqCols
	pred.rightEqualityIndices = rightEqCols
	nameBuf := make(tree.NameList, 2*numEqCols)
	pred.leftColNames = nameBuf[:numEqCols:numEqCols]
	pred.rightColNames = nameBuf[numEqCols:]

	for i := range leftEqCols {
		pred.leftColNames[i] = tree.Name(leftSrc.columns[leftEqCols[i]].Name)
		pred.rightColNames[i] = tree.Name(rightSrc.columns[rightEqCols[i]].Name)
	}
	pred.leftEqKey = leftEqColsAreKey
	pred.rightEqKey = rightEqColsAreKey

	pred.onCond = pred.iVarHelper.Rebind(extraOnCond)

	return p.makeJoinNode(leftSrc, rightSrc, pred), nil
}

// ConstructApplyJoin is part of the exec.Factory interface.
func (ef *execFactory) ConstructApplyJoin(
	joinType descpb.JoinType,
	left exec.Node,
	rightColumns colinfo.ResultColumns,
	onCond tree.TypedExpr,
	planRightSideFn exec.ApplyJoinPlanRightSideFn,
) (exec.Node, error) {
	leftSrc := asDataSource(left)
	pred := makePredicate(joinType, leftSrc.columns, rightColumns)
	pred.onCond = pred.iVarHelper.Rebind(onCond)
	return newApplyJoinNode(joinType, leftSrc, rightColumns, pred, planRightSideFn)
}

// ConstructMergeJoin is part of the exec.Factory interface.
func (ef *execFactory) ConstructMergeJoin(
	joinType descpb.JoinType,
	left, right exec.Node,
	onCond tree.TypedExpr,
	leftOrdering, rightOrdering colinfo.ColumnOrdering,
	reqOrdering exec.OutputOrdering,
	leftEqColsAreKey, rightEqColsAreKey bool,
) (exec.Node, error) {
	var err error
	p := ef.planner
	leftSrc := asDataSource(left)
	rightSrc := asDataSource(right)
	pred := makePredicate(joinType, leftSrc.columns, rightSrc.columns)
	pred.onCond = pred.iVarHelper.Rebind(onCond)
	node := p.makeJoinNode(leftSrc, rightSrc, pred)
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
		pred.leftColNames[i] = tree.Name(leftSrc.columns[leftColIdx].Name)
		pred.rightColNames[i] = tree.Name(rightSrc.columns[rightColIdx].Name)
	}

	// Set up node.props, which tells the distsql planner to maintain the
	// resulting ordering (if needed).
	node.reqOrdering = ReqOrdering(reqOrdering)

	return node, nil
}

// ConstructScalarGroupBy is part of the exec.Factory interface.
func (ef *execFactory) ConstructScalarGroupBy(
	input exec.Node, aggregations []exec.AggInfo,
) (exec.Node, error) {
	// There are no grouping columns with scalar GroupBy, so we create empty
	// arguments upfront to be passed into getResultColumnsForGroupBy call
	// below.
	var inputCols colinfo.ResultColumns
	var groupCols []exec.NodeColumnOrdinal
	n := &groupNode{
		plan:     input.(planNode),
		funcs:    make([]*aggregateFuncHolder, 0, len(aggregations)),
		columns:  getResultColumnsForGroupBy(inputCols, groupCols, aggregations),
		isScalar: true,
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
) (exec.Node, error) {
	inputPlan := input.(planNode)
	inputCols := planColumns(inputPlan)
	n := &groupNode{
		plan:             inputPlan,
		funcs:            make([]*aggregateFuncHolder, 0, len(groupCols)+len(aggregations)),
		columns:          getResultColumnsForGroupBy(inputCols, groupCols, aggregations),
		groupCols:        convertOrdinalsToInts(groupCols),
		groupColOrdering: groupColOrdering,
		isScalar:         false,
		reqOrdering:      ReqOrdering(reqOrdering),
	}
	for _, col := range n.groupCols {
		// TODO(radu): only generate the grouping columns we actually need.
		f := newAggregateFuncHolder(
			builtins.AnyNotNull,
			[]int{col},
			nil,   /* arguments */
			false, /* isDistinct */
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
		renderIdxs := convertOrdinalsToInts(agg.ArgCols)

		f := newAggregateFuncHolder(
			agg.FuncName,
			renderIdxs,
			agg.ConstArgs,
			agg.Distinct,
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
		plan:              input.(planNode),
		distinctOnColIdxs: distinctCols,
		columnsInOrder:    orderedCols,
		reqOrdering:       ReqOrdering(reqOrdering),
		nullsAreDistinct:  nullsAreDistinct,
		errorOnDup:        errorOnDup,
	}, nil
}

// ConstructSetOp is part of the exec.Factory interface.
func (ef *execFactory) ConstructSetOp(
	typ tree.UnionType, all bool, left, right exec.Node,
) (exec.Node, error) {
	return ef.planner.newUnionNode(typ, all, left.(planNode), right.(planNode))
}

// ConstructSort is part of the exec.Factory interface.
func (ef *execFactory) ConstructSort(
	input exec.Node, ordering exec.OutputOrdering, alreadyOrderedPrefix int,
) (exec.Node, error) {
	return &sortNode{
		plan:                 input.(planNode),
		ordering:             colinfo.ColumnOrdering(ordering),
		alreadyOrderedPrefix: alreadyOrderedPrefix,
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
		source:  plan,
		columns: cols,
	}, nil
}

// ConstructIndexJoin is part of the exec.Factory interface.
func (ef *execFactory) ConstructIndexJoin(
	input exec.Node,
	table cat.Table,
	keyCols []exec.NodeColumnOrdinal,
	tableCols exec.TableColumnOrdinalSet,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	tabDesc := table.(*optTable).desc
	colCfg := makeScanColumnsConfig(table, tableCols)
	colDescs := makeColDescList(table, tableCols)

	tableScan := ef.planner.Scan()

	if err := tableScan.initTable(context.TODO(), ef.planner, tabDesc, nil, colCfg); err != nil {
		return nil, err
	}

	primaryIndex := tabDesc.GetPrimaryIndex()
	tableScan.index = primaryIndex.IndexDesc()
	tableScan.disableBatchLimit()

	n := &indexJoinNode{
		input:         input.(planNode),
		table:         tableScan,
		cols:          colDescs,
		resultColumns: colinfo.ResultColumnsFromColDescs(tabDesc.GetID(), colDescs),
		reqOrdering:   ReqOrdering(reqOrdering),
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
	lookupCols exec.TableColumnOrdinalSet,
	onCond tree.TypedExpr,
	isSecondJoinInPairedJoiner bool,
	reqOrdering exec.OutputOrdering,
	locking *tree.LockingItem,
) (exec.Node, error) {
	if table.IsVirtualTable() {
		return ef.constructVirtualTableLookupJoin(joinType, input, table, index, eqCols, lookupCols, onCond)
	}
	tabDesc := table.(*optTable).desc
	indexDesc := index.(*optIndex).desc
	colCfg := makeScanColumnsConfig(table, lookupCols)
	tableScan := ef.planner.Scan()

	if err := tableScan.initTable(context.TODO(), ef.planner, tabDesc, nil, colCfg); err != nil {
		return nil, err
	}

	tableScan.index = indexDesc
	if locking != nil {
		tableScan.lockingStrength = descpb.ToScanLockingStrength(locking.Strength)
		tableScan.lockingWaitPolicy = descpb.ToScanLockingWaitPolicy(locking.WaitPolicy)
	}

	n := &lookupJoinNode{
		input:                      input.(planNode),
		table:                      tableScan,
		joinType:                   joinType,
		eqColsAreKey:               eqColsAreKey,
		isSecondJoinInPairedJoiner: isSecondJoinInPairedJoiner,
		reqOrdering:                ReqOrdering(reqOrdering),
	}
	n.eqCols = make([]int, len(eqCols))
	for i, c := range eqCols {
		n.eqCols[i] = int(c)
	}
	pred := makePredicate(joinType, planColumns(input.(planNode)), planColumns(tableScan))
	if onCond != nil && onCond != tree.DBoolTrue {
		n.onCond = pred.iVarHelper.Rebind(onCond)
	}
	n.columns = pred.cols

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
	virtual, err := ef.planner.getVirtualTabler().getVirtualTableEntry(tn)
	if err != nil {
		return nil, err
	}
	if len(eqCols) > 1 {
		return nil, errors.AssertionFailedf("vtable indexes with more than one column aren't supported yet")
	}
	// Check for explicit use of the dummy column.
	if lookupCols.Contains(0) {
		return nil, errors.Errorf("use of %s column not allowed.", table.Column(0).ColName())
	}
	indexDesc := index.(*optVirtualIndex).desc
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
	if err := tableScan.initTable(context.TODO(), ef.planner, tableDesc, nil, colCfg); err != nil {
		return nil, err
	}
	tableScan.index = indexDesc
	vtableCols := colinfo.ResultColumnsFromColDescs(tableDesc.ID, tableDesc.Columns)
	projectedVtableCols := planColumns(&tableScan)
	outputCols := make(colinfo.ResultColumns, 0, len(inputCols)+len(projectedVtableCols))
	outputCols = append(outputCols, inputCols...)
	outputCols = append(outputCols, projectedVtableCols...)
	// joinType is either INNER or LEFT_OUTER.
	pred := makePredicate(joinType, inputCols, projectedVtableCols)
	pred.onCond = pred.iVarHelper.Rebind(onCond)
	n := &vTableLookupJoinNode{
		input:             input.(planNode),
		joinType:          joinType,
		virtualTableEntry: virtual,
		dbName:            tn.Catalog(),
		table:             tableDesc,
		index:             indexDesc,
		eqCol:             int(eqCols[0]),
		inputCols:         inputCols,
		vtableCols:        vtableCols,
		lookupCols:        lookupCols,
		columns:           outputCols,
		pred:              pred,
	}
	return n, nil
}

func (ef *execFactory) ConstructInvertedJoin(
	joinType descpb.JoinType,
	invertedExpr tree.TypedExpr,
	input exec.Node,
	table cat.Table,
	index cat.Index,
	lookupCols exec.TableColumnOrdinalSet,
	onCond tree.TypedExpr,
	isFirstJoinInPairedJoiner bool,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	tabDesc := table.(*optTable).desc
	indexDesc := index.(*optIndex).desc
	// NB: lookupCols does not include the inverted column, which is only a partial
	// representation of the original table column. This scan configuration does not
	// affect what the invertedJoiner implementation retrieves from the inverted
	// index (which includes the inverted column). This scan configuration is used
	// later for computing the output from the inverted join.
	colCfg := makeScanColumnsConfig(table, lookupCols)
	tableScan := ef.planner.Scan()

	if err := tableScan.initTable(context.TODO(), ef.planner, tabDesc, nil, colCfg); err != nil {
		return nil, err
	}
	tableScan.index = indexDesc

	n := &invertedJoinNode{
		input:                     input.(planNode),
		table:                     tableScan,
		joinType:                  joinType,
		invertedExpr:              invertedExpr,
		isFirstJoinInPairedJoiner: isFirstJoinInPairedJoiner,
		reqOrdering:               ReqOrdering(reqOrdering),
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
	indexDesc *descpb.IndexDescriptor,
	tableDesc *tabledesc.Immutable,
	cols exec.TableColumnOrdinalSet,
) (*scanNode, error) {

	colCfg := scanColumnsConfig{
		wantedColumns: make([]tree.ColumnID, 0, cols.Len()),
	}

	for c, ok := cols.Next(0); ok; c, ok = cols.Next(c + 1) {
		colCfg.wantedColumns = append(colCfg.wantedColumns, tree.ColumnID(tableDesc.Columns[c].ID))
	}

	scan := ef.planner.Scan()
	if err := scan.initTable(context.TODO(), ef.planner, tableDesc, nil, colCfg); err != nil {
		return nil, err
	}

	scan.index = indexDesc

	return scan, nil
}

// ConstructZigzagJoin is part of the exec.Factory interface.
func (ef *execFactory) ConstructZigzagJoin(
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
	leftIndexDesc := leftIndex.(*optIndex).desc
	leftTabDesc := leftTable.(*optTable).desc
	rightIndexDesc := rightIndex.(*optIndex).desc
	rightTabDesc := rightTable.(*optTable).desc

	leftScan, err := ef.constructScanForZigzag(leftIndexDesc, leftTabDesc, leftCols)
	if err != nil {
		return nil, err
	}
	rightScan, err := ef.constructScanForZigzag(rightIndexDesc, rightTabDesc, rightCols)
	if err != nil {
		return nil, err
	}

	n := &zigzagJoinNode{
		reqOrdering: ReqOrdering(reqOrdering),
	}
	if onCond != nil && onCond != tree.DBoolTrue {
		n.onCond = onCond
	}
	n.sides = make([]zigzagJoinSide, 2)
	n.sides[0].scan = leftScan
	n.sides[1].scan = rightScan
	n.sides[0].eqCols = make([]int, len(leftEqCols))
	n.sides[1].eqCols = make([]int, len(rightEqCols))

	if len(leftEqCols) != len(rightEqCols) {
		panic("creating zigzag join with unequal number of equated cols")
	}

	for i, c := range leftEqCols {
		n.sides[0].eqCols[i] = int(c)
		n.sides[1].eqCols[i] = int(rightEqCols[i])
	}
	// The resultant columns are identical to those from individual index scans; so
	// reuse the resultColumns generated in the scanNodes.
	n.columns = make(
		colinfo.ResultColumns,
		0,
		len(leftScan.resultColumns)+len(rightScan.resultColumns),
	)
	n.columns = append(n.columns, leftScan.resultColumns...)
	n.columns = append(n.columns, rightScan.resultColumns...)

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
			// TODO(rytaft): Remove this once the optimizer encodes the fixed values.
			if col.Kind() == cat.VirtualInverted {
				cols[i].Typ = index.Table().Column(col.InvertedSourceColumnOrdinal()).DatumType()
			} else {
				cols[i].Typ = col.DatumType()
			}
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
		plan:       plan,
		countExpr:  limit,
		offsetExpr: offset,
	}, nil
}

// ConstructMax1Row is part of the exec.Factory interface.
func (ef *execFactory) ConstructMax1Row(input exec.Node, errorText string) (exec.Node, error) {
	plan := input.(planNode)
	return &max1RowNode{
		plan:      plan,
		errorText: errorText,
	}, nil
}

// ConstructBuffer is part of the exec.Factory interface.
func (ef *execFactory) ConstructBuffer(input exec.Node, label string) (exec.Node, error) {
	return &bufferNode{
		plan:  input.(planNode),
		label: label,
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
	initial exec.Node, fn exec.RecursiveCTEIterationFn, label string,
) (exec.Node, error) {
	return &recursiveCTENode{
		initial:        initial.(planNode),
		genIterationFn: fn,
		label:          label,
	}, nil
}

// ConstructProjectSet is part of the exec.Factory interface.
func (ef *execFactory) ConstructProjectSet(
	n exec.Node, exprs tree.TypedExprs, zipCols colinfo.ResultColumns, numColsPerGen []int,
) (exec.Node, error) {
	src := asDataSource(n)
	cols := append(src.columns, zipCols...)
	return &projectSetNode{
		source: src.plan,
		projectSetPlanningInfo: projectSetPlanningInfo{
			columns:         cols,
			numColsInSource: len(src.columns),
			exprs:           exprs,
			numColsPerGen:   numColsPerGen,
		},
	}, nil
}

// ConstructWindow is part of the exec.Factory interface.
func (ef *execFactory) ConstructWindow(root exec.Node, wi exec.WindowInfo) (exec.Node, error) {
	p := &windowNode{
		plan:         root.(planNode),
		columns:      wi.Cols,
		windowRender: make([]tree.TypedExpr, len(wi.Cols)),
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
			window:         p,
			filterColIdx:   wi.FilterIdxs[i],
			outputColIdx:   wi.OutputIdxs[i],
			partitionIdxs:  partitionIdxs,
			columnOrdering: wi.Ordering,
			frame:          wi.Exprs[i].WindowDef.Frame,
		}
		if len(wi.Ordering) == 0 {
			frame := p.funcs[i].frame
			if frame.Mode == tree.RANGE && frame.Bounds.HasOffset() {
				// Execution requires a single column to order by when there is
				// a RANGE mode frame with at least one 'offset' bound.
				return nil, errors.AssertionFailedf("a RANGE mode frame with an offset bound must have an ORDER BY column")
			}
		}

		p.windowRender[wi.OutputIdxs[i]] = p.funcs[i]
	}

	return p, nil
}

// ConstructPlan is part of the exec.Factory interface.
func (ef *execFactory) ConstructPlan(
	root exec.Node, subqueries []exec.Subquery, cascades []exec.Cascade, checks []exec.Node,
) (exec.Plan, error) {
	// No need to spool at the root.
	if spool, ok := root.(*spoolNode); ok {
		root = spool.source
	}
	return constructPlan(ef.planner, root, subqueries, cascades, checks)
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

	c := makeStmtEnvCollector(
		ef.planner.EvalContext().Context,
		ef.planner.extendedEvalCtx.InternalExecutor.(*InternalExecutor),
	)

	// Show the version of Cockroach running.
	if err := c.PrintVersion(&out.buf); err != nil {
		return nil, err
	}
	out.writef("")
	// Show the values of any non-default session variables that can impact
	// planning decisions.
	if err := c.PrintSettings(&out.buf); err != nil {
		return nil, err
	}

	// Show the definition of each referenced catalog object.
	for i := range envOpts.Sequences {
		out.writef("")
		if err := c.PrintCreateSequence(&out.buf, &envOpts.Sequences[i]); err != nil {
			return nil, err
		}
	}

	// TODO(justin): it might also be relevant in some cases to print the create
	// statements for tables referenced via FKs in these tables.
	for i := range envOpts.Tables {
		out.writef("")
		if err := c.PrintCreateTable(&out.buf, &envOpts.Tables[i]); err != nil {
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

	for i := range envOpts.Views {
		out.writef("")
		if err := c.PrintCreateView(&out.buf, &envOpts.Views[i]); err != nil {
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
		plan: node,
		ordering: colinfo.ColumnOrdering{
			colinfo.ColumnOrderInfo{ColIdx: ageColIdx, Direction: encoding.Ascending},
		},
	}

	if typ == tree.ShowTraceReplica {
		node = &showTraceReplicaNode{plan: node}
	}
	return node, nil
}

func (ef *execFactory) ConstructInsert(
	input exec.Node,
	table cat.Table,
	arbiters cat.IndexOrdinals,
	insertColOrdSet exec.TableColumnOrdinalSet,
	returnColOrdSet exec.TableColumnOrdinalSet,
	checkOrdSet exec.CheckOrdinalSet,
	autoCommit bool,
) (exec.Node, error) {
	ctx := ef.planner.extendedEvalCtx.Context

	// Derive insert table and column descriptors.
	rowsNeeded := !returnColOrdSet.Empty()
	tabDesc := table.(*optTable).desc
	colDescs := makeColDescList(table, insertColOrdSet)

	if err := ef.planner.maybeSetSystemConfig(tabDesc.GetID()); err != nil {
		return nil, err
	}

	// Create the table inserter, which does the bulk of the work.
	ri, err := row.MakeInserter(
		ctx, ef.planner.txn, ef.planner.ExecCfg().Codec, tabDesc, colDescs, ef.planner.alloc,
	)
	if err != nil {
		return nil, err
	}

	// Regular path for INSERT.
	ins := insertNodePool.Get().(*insertNode)
	*ins = insertNode{
		source: input.(planNode),
		run: insertRun{
			ti:         tableInserter{ri: ri},
			checkOrds:  checkOrdSet,
			insertCols: ri.InsertCols,
		},
	}

	// If rows are not needed, no columns are returned.
	if rowsNeeded {
		returnColDescs := makeColDescList(table, returnColOrdSet)
		ins.columns = colinfo.ResultColumnsFromColDescs(tabDesc.GetID(), returnColDescs)

		// Set the tabColIdxToRetIdx for the mutation. Insert always returns
		// non-mutation columns in the same order they are defined in the table.
		ins.run.tabColIdxToRetIdx = row.ColMapping(tabDesc.Columns, returnColDescs)
		ins.run.rowsNeeded = true
	}

	if autoCommit {
		ins.enableAutoCommit()
	}

	// serialize the data-modifying plan to ensure that no data is
	// observed that hasn't been validated first. See the comments
	// on BatchedNext() in plan_batch.go.
	if rowsNeeded {
		return &spoolNode{source: &serializeNode{source: ins}}, nil
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
	fkChecks []exec.InsertFastPathFKCheck,
	autoCommit bool,
) (exec.Node, error) {
	ctx := ef.planner.extendedEvalCtx.Context

	// Derive insert table and column descriptors.
	rowsNeeded := !returnColOrdSet.Empty()
	tabDesc := table.(*optTable).desc
	colDescs := makeColDescList(table, insertColOrdSet)

	if err := ef.planner.maybeSetSystemConfig(tabDesc.GetID()); err != nil {
		return nil, err
	}

	// Create the table inserter, which does the bulk of the work.
	ri, err := row.MakeInserter(
		ctx, ef.planner.txn, ef.planner.ExecCfg().Codec, tabDesc, colDescs, ef.planner.alloc,
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

	if len(fkChecks) > 0 {
		ins.run.fkChecks = make([]insertFastPathFKCheck, len(fkChecks))
		for i := range fkChecks {
			ins.run.fkChecks[i].InsertFastPathFKCheck = fkChecks[i]
		}
	}

	// If rows are not needed, no columns are returned.
	if rowsNeeded {
		returnColDescs := makeColDescList(table, returnColOrdSet)
		ins.columns = colinfo.ResultColumnsFromColDescs(tabDesc.GetID(), returnColDescs)

		// Set the tabColIdxToRetIdx for the mutation. Insert always returns
		// non-mutation columns in the same order they are defined in the table.
		ins.run.tabColIdxToRetIdx = row.ColMapping(tabDesc.Columns, returnColDescs)
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
		return &spoolNode{source: &serializeNode{source: ins}}, nil
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
	autoCommit bool,
) (exec.Node, error) {
	ctx := ef.planner.extendedEvalCtx.Context

	// Derive table and column descriptors.
	rowsNeeded := !returnColOrdSet.Empty()
	tabDesc := table.(*optTable).desc
	fetchColDescs := makeColDescList(table, fetchColOrdSet)

	if err := ef.planner.maybeSetSystemConfig(tabDesc.GetID()); err != nil {
		return nil, err
	}

	// Add each column to update as a sourceSlot. The CBO only uses scalarSlot,
	// since it compiles tuples and subqueries into a simple sequence of target
	// columns.
	updateColDescs := makeColDescList(table, updateColOrdSet)
	sourceSlots := make([]sourceSlot, len(updateColDescs))
	for i := range sourceSlots {
		sourceSlots[i] = scalarSlot{column: updateColDescs[i], sourceIndex: len(fetchColDescs) + i}
	}

	// Create the table updater, which does the bulk of the work.
	ru, err := row.MakeUpdater(
		ctx,
		ef.planner.txn,
		ef.planner.ExecCfg().Codec,
		tabDesc,
		updateColDescs,
		fetchColDescs,
		row.UpdaterDefault,
		ef.planner.alloc,
	)
	if err != nil {
		return nil, err
	}

	// updateColsIdx inverts the mapping of UpdateCols to FetchCols. See
	// the explanatory comments in updateRun.
	var updateColsIdx catalog.TableColMap
	for i := range ru.UpdateCols {
		id := ru.UpdateCols[i].ID
		updateColsIdx.Set(id, i)
	}

	upd := updateNodePool.Get().(*updateNode)
	*upd = updateNode{
		source: input.(planNode),
		run: updateRun{
			tu:        tableUpdater{ru: ru},
			checkOrds: checks,
			iVarContainerForComputedCols: schemaexpr.RowIndexedVarContainer{
				CurSourceRow: make(tree.Datums, len(ru.FetchCols)),
				Cols:         ru.FetchCols,
				Mapping:      ru.FetchColIDtoRowIndex,
			},
			sourceSlots:    sourceSlots,
			updateValues:   make(tree.Datums, len(ru.UpdateCols)),
			updateColsIdx:  updateColsIdx,
			numPassthrough: len(passthrough),
		},
	}

	// If rows are not needed, no columns are returned.
	if rowsNeeded {
		returnColDescs := makeColDescList(table, returnColOrdSet)

		upd.columns = colinfo.ResultColumnsFromColDescs(tabDesc.GetID(), returnColDescs)
		// Add the passthrough columns to the returning columns.
		upd.columns = append(upd.columns, passthrough...)

		// Set the rowIdxToRetIdx for the mutation. Update returns the non-mutation
		// columns specified, in the same order they are defined in the table.
		//
		// The Updater derives/stores the fetch columns of the mutation and
		// since the return columns are always a subset of the fetch columns,
		// we can use use the fetch columns to generate the mapping for the
		// returned rows.
		upd.run.rowIdxToRetIdx = row.ColMapping(ru.FetchCols, returnColDescs)
		upd.run.rowsNeeded = true
	}

	if autoCommit {
		upd.enableAutoCommit()
	}

	// Serialize the data-modifying plan to ensure that no data is observed that
	// hasn't been validated first. See the comments on BatchedNext() in
	// plan_batch.go.
	if rowsNeeded {
		return &spoolNode{source: &serializeNode{source: upd}}, nil
	}

	// We could use serializeNode here, but using rowCountNode is an
	// optimization that saves on calls to Next() by the caller.
	return &rowCountNode{source: upd}, nil
}

func (ef *execFactory) ConstructUpsert(
	input exec.Node,
	table cat.Table,
	arbiters cat.IndexOrdinals,
	canaryCol exec.NodeColumnOrdinal,
	insertColOrdSet exec.TableColumnOrdinalSet,
	fetchColOrdSet exec.TableColumnOrdinalSet,
	updateColOrdSet exec.TableColumnOrdinalSet,
	returnColOrdSet exec.TableColumnOrdinalSet,
	checks exec.CheckOrdinalSet,
	autoCommit bool,
) (exec.Node, error) {
	ctx := ef.planner.extendedEvalCtx.Context

	// Derive table and column descriptors.
	rowsNeeded := !returnColOrdSet.Empty()
	tabDesc := table.(*optTable).desc
	insertColDescs := makeColDescList(table, insertColOrdSet)
	fetchColDescs := makeColDescList(table, fetchColOrdSet)
	updateColDescs := makeColDescList(table, updateColOrdSet)

	if err := ef.planner.maybeSetSystemConfig(tabDesc.GetID()); err != nil {
		return nil, err
	}

	// Create the table inserter, which does the bulk of the insert-related work.
	ri, err := row.MakeInserter(
		ctx,
		ef.planner.txn,
		ef.planner.ExecCfg().Codec,
		tabDesc,
		insertColDescs,
		ef.planner.alloc,
	)
	if err != nil {
		return nil, err
	}

	// Create the table updater, which does the bulk of the update-related work.
	ru, err := row.MakeUpdater(
		ctx,
		ef.planner.txn,
		ef.planner.ExecCfg().Codec,
		tabDesc,
		updateColDescs,
		fetchColDescs,
		row.UpdaterDefault,
		ef.planner.alloc,
	)
	if err != nil {
		return nil, err
	}

	// Instantiate the upsert node.
	ups := upsertNodePool.Get().(*upsertNode)
	*ups = upsertNode{
		source: input.(planNode),
		run: upsertRun{
			checkOrds:  checks,
			insertCols: ri.InsertCols,
			tw: optTableUpserter{
				ri:            ri,
				canaryOrdinal: int(canaryCol),
				fetchCols:     fetchColDescs,
				updateCols:    updateColDescs,
				ru:            ru,
			},
		},
	}

	// If rows are not needed, no columns are returned.
	if rowsNeeded {
		returnColDescs := makeColDescList(table, returnColOrdSet)
		ups.columns = colinfo.ResultColumnsFromColDescs(tabDesc.GetID(), returnColDescs)

		// Update the tabColIdxToRetIdx for the mutation. Upsert returns
		// non-mutation columns specified, in the same order they are defined
		// in the table.
		ups.run.tw.tabColIdxToRetIdx = row.ColMapping(tabDesc.Columns, returnColDescs)
		ups.run.tw.returnCols = returnColDescs
		ups.run.tw.rowsNeeded = true
	}

	if autoCommit {
		ups.enableAutoCommit()
	}

	// Serialize the data-modifying plan to ensure that no data is observed that
	// hasn't been validated first. See the comments on BatchedNext() in
	// plan_batch.go.
	if rowsNeeded {
		return &spoolNode{source: &serializeNode{source: ups}}, nil
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
	autoCommit bool,
) (exec.Node, error) {
	// Derive table and column descriptors.
	rowsNeeded := !returnColOrdSet.Empty()
	tabDesc := table.(*optTable).desc
	fetchColDescs := makeColDescList(table, fetchColOrdSet)

	if err := ef.planner.maybeSetSystemConfig(tabDesc.GetID()); err != nil {
		return nil, err
	}

	// Create the table deleter, which does the bulk of the work. In the HP,
	// the deleter derives the columns that need to be fetched. By contrast, the
	// CBO will have already determined the set of fetch columns, and passes
	// those sets into the deleter (which will basically be a no-op).
	rd := row.MakeDeleter(ef.planner.ExecCfg().Codec, tabDesc, fetchColDescs)

	// Now make a delete node. We use a pool.
	del := deleteNodePool.Get().(*deleteNode)
	*del = deleteNode{
		source: input.(planNode),
		run: deleteRun{
			td:                        tableDeleter{rd: rd, alloc: ef.planner.alloc},
			partialIndexDelValsOffset: len(rd.FetchCols),
		},
	}

	// If rows are not needed, no columns are returned.
	if rowsNeeded {
		returnColDescs := makeColDescList(table, returnColOrdSet)
		// Delete returns the non-mutation columns specified, in the same
		// order they are defined in the table.
		del.columns = colinfo.ResultColumnsFromColDescs(tabDesc.GetID(), returnColDescs)

		del.run.rowIdxToRetIdx = row.ColMapping(rd.FetchCols, returnColDescs)
		del.run.rowsNeeded = true
	}

	if autoCommit {
		del.enableAutoCommit()
	}

	// Serialize the data-modifying plan to ensure that no data is observed that
	// hasn't been validated first. See the comments on BatchedNext() in
	// plan_batch.go.
	if rowsNeeded {
		return &spoolNode{source: &serializeNode{source: del}}, nil
	}

	// We could use serializeNode here, but using rowCountNode is an
	// optimization that saves on calls to Next() by the caller.
	return &rowCountNode{source: del}, nil
}

func (ef *execFactory) ConstructDeleteRange(
	table cat.Table,
	needed exec.TableColumnOrdinalSet,
	indexConstraint *constraint.Constraint,
	interleavedTables []cat.Table,
	autoCommit bool,
) (exec.Node, error) {
	tabDesc := table.(*optTable).desc
	indexDesc := tabDesc.GetPrimaryIndex().IndexDesc()
	sb := span.MakeBuilder(ef.planner.EvalContext(), ef.planner.ExecCfg().Codec, tabDesc, indexDesc)

	if err := ef.planner.maybeSetSystemConfig(tabDesc.GetID()); err != nil {
		return nil, err
	}

	// Setting the "forDelete" flag includes all column families in case where a
	// single record is deleted.
	spans, err := sb.SpansFromConstraint(indexConstraint, needed, true /* forDelete */)
	if err != nil {
		return nil, err
	}

	dr := &deleteRangeNode{
		interleavedFastPath: false,
		spans:               spans,
		desc:                tabDesc,
		autoCommitEnabled:   autoCommit,
	}

	if len(interleavedTables) > 0 {
		dr.interleavedFastPath = true
		dr.interleavedDesc = make([]*tabledesc.Immutable, len(interleavedTables))
		for i := range dr.interleavedDesc {
			dr.interleavedDesc[i] = interleavedTables[i].(*optTable).desc
		}
	}
	return dr, nil
}

// ConstructCreateTable is part of the exec.Factory interface.
func (ef *execFactory) ConstructCreateTable(
	schema cat.Schema, ct *tree.CreateTable,
) (exec.Node, error) {
	if err := checkSchemaChangeEnabled(
		ef.planner.EvalContext().Context,
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
		ef.planner.EvalContext().Context,
		ef.planner.ExecCfg(),
		"CREATE TABLE",
	); err != nil {
		return nil, err
	}

	return &createTableNode{
		n:          ct,
		dbDesc:     schema.(*optSchema).database,
		sourcePlan: input.(planNode),
	}, nil
}

// ConstructCreateView is part of the exec.Factory interface.
func (ef *execFactory) ConstructCreateView(
	schema cat.Schema,
	viewName *cat.DataSourceName,
	ifNotExists bool,
	replace bool,
	persistence tree.Persistence,
	materialized bool,
	viewQuery string,
	columns colinfo.ResultColumns,
	deps opt.ViewDeps,
) (exec.Node, error) {

	if err := checkSchemaChangeEnabled(
		ef.planner.EvalContext().Context,
		ef.planner.ExecCfg(),
		"CREATE VIEW",
	); err != nil {
		return nil, err
	}

	planDeps := make(planDependencies, len(deps))
	for _, d := range deps {
		desc, err := getDescForDataSource(d.DataSource)
		if err != nil {
			return nil, err
		}
		var ref descpb.TableDescriptor_Reference
		if d.SpecificIndex {
			idx := d.DataSource.(cat.Table).Index(d.Index)
			ref.IndexID = idx.(*optIndex).desc.ID
		}
		if !d.ColumnOrdinals.Empty() {
			ref.ColumnIDs = make([]descpb.ColumnID, 0, d.ColumnOrdinals.Len())
			d.ColumnOrdinals.ForEach(func(ord int) {
				ref.ColumnIDs = append(ref.ColumnIDs, desc.Columns[ord].ID)
			})
		}
		entry := planDeps[desc.ID]
		entry.desc = desc
		entry.deps = append(entry.deps, ref)
		planDeps[desc.ID] = entry
	}

	return &createViewNode{
		viewName:     viewName,
		ifNotExists:  ifNotExists,
		replace:      replace,
		materialized: materialized,
		persistence:  persistence,
		viewQuery:    viewQuery,
		dbDesc:       schema.(*optSchema).database,
		columns:      columns,
		planDeps:     planDeps,
	}, nil
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
		plan:  input.(planNode),
		mkErr: mkErr,
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
	if err := checkSchemaChangeEnabled(
		ef.planner.EvalContext().Context,
		ef.planner.ExecCfg(),
		"ALTER TABLE/INDEX SPLIT AT",
	); err != nil {
		return nil, err
	}

	if !ef.planner.ExecCfg().Codec.ForSystemTenant() {
		return nil, errorutil.UnsupportedWithMultiTenancy(54254)
	}

	expirationTime, err := parseExpirationTime(ef.planner.EvalContext(), expiration)
	if err != nil {
		return nil, err
	}

	return &splitNode{
		tableDesc:      index.Table().(*optTable).desc,
		index:          index.(*optIndex).desc,
		rows:           input.(planNode),
		expirationTime: expirationTime,
	}, nil
}

// ConstructAlterTableUnsplit is part of the exec.Factory interface.
func (ef *execFactory) ConstructAlterTableUnsplit(
	index cat.Index, input exec.Node,
) (exec.Node, error) {
	if err := checkSchemaChangeEnabled(
		ef.planner.EvalContext().Context,
		ef.planner.ExecCfg(),
		"ALTER TABLE/INDEX UNSPLIT AT",
	); err != nil {
		return nil, err
	}

	if !ef.planner.ExecCfg().Codec.ForSystemTenant() {
		return nil, errorutil.UnsupportedWithMultiTenancy(54254)
	}

	return &unsplitNode{
		tableDesc: index.Table().(*optTable).desc,
		index:     index.(*optIndex).desc,
		rows:      input.(planNode),
	}, nil
}

// ConstructAlterTableUnsplitAll is part of the exec.Factory interface.
func (ef *execFactory) ConstructAlterTableUnsplitAll(index cat.Index) (exec.Node, error) {
	if err := checkSchemaChangeEnabled(
		ef.planner.EvalContext().Context,
		ef.planner.ExecCfg(),
		"ALTER TABLE/INDEX UNSPLIT ALL",
	); err != nil {
		return nil, err
	}

	if !ef.planner.ExecCfg().Codec.ForSystemTenant() {
		return nil, errorutil.UnsupportedWithMultiTenancy(54254)
	}

	return &unsplitAllNode{
		tableDesc: index.Table().(*optTable).desc,
		index:     index.(*optIndex).desc,
	}, nil
}

// ConstructAlterTableRelocate is part of the exec.Factory interface.
func (ef *execFactory) ConstructAlterTableRelocate(
	index cat.Index, input exec.Node, relocateLease bool,
) (exec.Node, error) {
	if !ef.planner.ExecCfg().Codec.ForSystemTenant() {
		return nil, errorutil.UnsupportedWithMultiTenancy(54250)
	}

	return &relocateNode{
		relocateLease: relocateLease,
		tableDesc:     index.Table().(*optTable).desc,
		index:         index.(*optIndex).desc,
		rows:          input.(planNode),
	}, nil
}

// ConstructControlJobs is part of the exec.Factory interface.
func (ef *execFactory) ConstructControlJobs(
	command tree.JobCommand, input exec.Node,
) (exec.Node, error) {
	return &controlJobsNode{
		rows:          input.(planNode),
		desiredStatus: jobCommandToDesiredStatus[command],
	}, nil
}

// ConstructControlJobs is part of the exec.Factory interface.
func (ef *execFactory) ConstructControlSchedules(
	command tree.ScheduleCommand, input exec.Node,
) (exec.Node, error) {
	return &controlSchedulesNode{
		rows:    input.(planNode),
		command: command,
	}, nil
}

// ConstructCancelQueries is part of the exec.Factory interface.
func (ef *execFactory) ConstructCancelQueries(input exec.Node, ifExists bool) (exec.Node, error) {
	return &cancelQueriesNode{
		rows:     input.(planNode),
		ifExists: ifExists,
	}, nil
}

// ConstructCancelSessions is part of the exec.Factory interface.
func (ef *execFactory) ConstructCancelSessions(input exec.Node, ifExists bool) (exec.Node, error) {
	return &cancelSessionsNode{
		rows:     input.(planNode),
		ifExists: ifExists,
	}, nil
}

// ConstructCreateStatistics is part of the exec.Factory interface.
func (ef *execFactory) ConstructCreateStatistics(cs *tree.CreateStats) (exec.Node, error) {
	ctx := ef.planner.extendedEvalCtx.Context
	if err := featureflag.CheckEnabled(
		ctx,
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
	options *tree.ExplainOptions, stmtType tree.StatementType, buildFn exec.BuildPlanForExplainFn,
) (exec.Node, error) {
	if options.Flags[tree.ExplainFlagEnv] {
		return nil, errors.New("ENV only supported with (OPT) option")
	}

	explainFactory := explain.NewFactory(&execFactory{
		planner:   ef.planner,
		isExplain: true,
	})
	plan, err := buildFn(explainFactory)
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
	flags := explain.MakeFlags(options)
	n := &explainPlanNode{
		options: options,
		flags:   flags,
		plan:    plan.(*explain.Plan),
	}
	return n, nil
}

// renderBuilder encapsulates the code to build a renderNode.
type renderBuilder struct {
	r   *renderNode
	res planNode
}

// init initializes the renderNode with render expressions.
func (rb *renderBuilder) init(n exec.Node, reqOrdering exec.OutputOrdering) {
	src := asDataSource(n)
	rb.r = &renderNode{
		source: src,
	}
	rb.r.ivarHelper = tree.MakeIndexedVarHelper(rb.r, len(src.columns))
	rb.r.reqOrdering = ReqOrdering(reqOrdering)

	// If there's a spool, pull it up.
	if spool, ok := rb.r.source.plan.(*spoolNode); ok {
		rb.r.source.plan = spool.source
		spool.source = rb.r
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

// makeColDescList returns a list of table column descriptors. Columns are
// included if their ordinal position in the table schema is in the cols set.
func makeColDescList(table cat.Table, cols exec.TableColumnOrdinalSet) []descpb.ColumnDescriptor {
	tab := table.(optCatalogTableInterface)
	colDescs := make([]descpb.ColumnDescriptor, 0, cols.Len())
	for i, n := 0, table.ColumnCount(); i < n; i++ {
		if !cols.Contains(i) {
			continue
		}
		colDescs = append(colDescs, *tab.getColDesc(i))
	}
	return colDescs
}
