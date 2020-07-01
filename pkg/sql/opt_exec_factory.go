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

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

type execFactory struct {
	planner         *planner
	allowAutoCommit bool
}

var _ exec.Factory = &execFactory{}

func newExecFactory(p *planner) *execFactory {
	return &execFactory{
		planner:         p,
		allowAutoCommit: p.autoCommit,
	}
}

func (ef *execFactory) disableAutoCommit() {
	ef.allowAutoCommit = false
}

// ConstructValues is part of the exec.Factory interface.
func (ef *execFactory) ConstructValues(
	rows [][]tree.TypedExpr, cols sqlbase.ResultColumns,
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
		return ef.constructVirtualScan(
			table, index, needed, indexConstraint, hardLimit, softLimit, reverse, maxResults,
			reqOrdering, rowCount, locking,
		)
	}

	tabDesc := table.(*optTable).desc
	indexDesc := index.(*optIndex).desc
	// Create a scanNode.
	scan := ef.planner.Scan()
	colCfg := makeScanColumnsConfig(table, needed)

	sb := span.MakeBuilder(ef.planner.ExecCfg().Codec, tabDesc.TableDesc(), indexDesc)

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

	if indexConstraint != nil && indexConstraint.IsContradiction() {
		return newZeroNode(scan.resultColumns), nil
	}

	scan.index = indexDesc
	scan.hardLimit = hardLimit
	scan.softLimit = softLimit

	scan.reverse = reverse
	scan.maxResults = maxResults
	scan.parallelScansEnabled = sqlbase.ParallelScans.Get(&ef.planner.extendedEvalCtx.Settings.SV)
	var err error
	if invertedConstraint != nil {
		scan.spans, err = GenerateInvertedSpans(invertedConstraint, sb)
	} else {
		scan.spans, err = sb.SpansFromConstraint(indexConstraint, needed, false /* forDelete */)
	}
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
	scan.estimatedRowCount = uint64(rowCount)
	if locking != nil {
		scan.lockingStrength = sqlbase.ToScanLockingStrength(locking.Strength)
		scan.lockingWaitPolicy = sqlbase.ToScanLockingWaitPolicy(locking.WaitPolicy)
	}
	return scan, nil
}

func (ef *execFactory) constructVirtualScan(
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
	tn := &table.(*optVirtualTable).name
	virtual, err := ef.planner.getVirtualTabler().getVirtualTableEntry(tn)
	if err != nil {
		return nil, err
	}
	indexDesc := index.(*optVirtualIndex).desc
	columns, constructor := virtual.getPlanInfo(
		table.(*optVirtualTable).desc.TableDesc(),
		indexDesc, indexConstraint)

	var n exec.Node
	n = &delayedNode{
		name:            fmt.Sprintf("%s@%s", table.Name(), index.Name()),
		columns:         columns,
		indexConstraint: indexConstraint,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			return constructor(ctx, p, tn.Catalog())
		},
	}

	// Check for explicit use of the dummy column.
	if needed.Contains(0) {
		return nil, errors.Errorf("use of %s column not allowed.", table.Column(0).ColName())
	}
	if locking != nil {
		// We shouldn't have allowed SELECT FOR UPDATE for a virtual table.
		return nil, errors.AssertionFailedf("locking cannot be used with virtual table")
	}
	if needed.Len() != len(columns) {
		// We are selecting a subset of columns; we need a projection.
		cols := make([]exec.NodeColumnOrdinal, 0, needed.Len())
		colNames := make([]string, len(cols))
		for ord, ok := needed.Next(0); ok; ord, ok = needed.Next(ord + 1) {
			cols = append(cols, exec.NodeColumnOrdinal(ord-1))
			colNames = append(colNames, columns[ord-1].Name)
		}
		n, err = ef.ConstructSimpleProject(n, cols, colNames, nil /* reqOrdering */)
		if err != nil {
			return nil, err
		}
	}
	if hardLimit != 0 {
		n, err = ef.ConstructLimit(n, tree.NewDInt(tree.DInt(hardLimit)), nil /* offset */)
		if err != nil {
			return nil, err
		}
	}
	// reqOrdering will be set if the optimizer expects that the output of the
	// exec.Node that we're returning will actually have a legitimate ordering.
	// Virtual indexes never provide a legitimate ordering, so we have to make
	// sure to sort if we have a required ordering.
	if len(reqOrdering) != 0 {
		n, err = ef.ConstructSort(n, sqlbase.ColumnOrdering(reqOrdering), 0)
		if err != nil {
			return nil, err
		}
	}
	return n, nil
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
	// Push the filter into the scanNode. We cannot do this if the scanNode has a
	// limit (it would make the limit apply AFTER the filter).
	if s, ok := n.(*scanNode); ok && s.filter == nil && s.hardLimit == 0 {
		s.filter = s.filterVars.Rebind(filter)
		// Note: if the filter statically evaluates to true, s.filter stays nil.
		s.reqOrdering = ReqOrdering(reqOrdering)
		return s, nil
	}
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
	n exec.Node, invFilter *invertedexpr.SpanExpression, invColumn exec.NodeColumnOrdinal,
) (exec.Node, error) {
	inputCols := planColumns(n.(planNode))
	columns := make(sqlbase.ResultColumns, len(inputCols))
	copy(columns, inputCols)
	n = &invertedFilterNode{
		input:         n.(planNode),
		expression:    invFilter,
		invColumn:     int(invColumn),
		resultColumns: columns,
	}
	return n, nil
}

// ConstructSimpleProject is part of the exec.Factory interface.
func (ef *execFactory) ConstructSimpleProject(
	n exec.Node, cols []exec.NodeColumnOrdinal, colNames []string, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return constructSimpleProjectForPlanNode(n.(planNode), cols, colNames, reqOrdering)
}

// ConstructRender is part of the exec.Factory interface.
// N.B.: The input exprs will be modified.
func (ef *execFactory) ConstructRender(
	n exec.Node,
	columns sqlbase.ResultColumns,
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

// RenameColumns is part of the exec.Factory interface.
func (ef *execFactory) RenameColumns(n exec.Node, colNames []string) (exec.Node, error) {
	inputCols := planMutableColumns(n.(planNode))
	for i := range inputCols {
		inputCols[i].Name = colNames[i]
	}
	return n, nil
}

// ConstructHashJoin is part of the exec.Factory interface.
func (ef *execFactory) ConstructHashJoin(
	joinType sqlbase.JoinType,
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
	joinType sqlbase.JoinType,
	left exec.Node,
	rightColumns sqlbase.ResultColumns,
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
	joinType sqlbase.JoinType,
	left, right exec.Node,
	onCond tree.TypedExpr,
	leftOrdering, rightOrdering sqlbase.ColumnOrdering,
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
	var inputCols sqlbase.ResultColumns
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
	groupColOrdering sqlbase.ColumnOrdering,
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
		f := n.newAggregateFuncHolder(
			builtins.AnyNotNull,
			[]int{col},
			nil, /* arguments */
			ef.planner.EvalContext().Mon.MakeBoundAccount(),
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

		f := n.newAggregateFuncHolder(
			agg.FuncName,
			renderIdxs,
			agg.ConstArgs,
			ef.planner.EvalContext().Mon.MakeBoundAccount(),
		)
		if agg.Distinct {
			f.setDistinct()
		}
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
	input exec.Node, ordering sqlbase.ColumnOrdering, alreadyOrderedPrefix int,
) (exec.Node, error) {
	return &sortNode{
		plan:                 input.(planNode),
		ordering:             ordering,
		alreadyOrderedPrefix: alreadyOrderedPrefix,
	}, nil
}

// ConstructOrdinality is part of the exec.Factory interface.
func (ef *execFactory) ConstructOrdinality(input exec.Node, colName string) (exec.Node, error) {
	plan := input.(planNode)
	inputColumns := planColumns(plan)
	cols := make(sqlbase.ResultColumns, len(inputColumns)+1)
	copy(cols, inputColumns)
	cols[len(cols)-1] = sqlbase.ResultColumn{
		Name: colName,
		Typ:  types.Int,
	}
	return &ordinalityNode{
		source:  plan,
		columns: cols,
		run: ordinalityRun{
			row:    make(tree.Datums, len(cols)),
			curCnt: 1,
		},
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
	tableScan.index = &primaryIndex
	tableScan.disableBatchLimit()

	n := &indexJoinNode{
		input:         input.(planNode),
		table:         tableScan,
		cols:          colDescs,
		resultColumns: sqlbase.ResultColumnsFromColDescs(tabDesc.GetID(), colDescs),
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

	n := &lookupJoinNode{
		input:        input.(planNode),
		table:        tableScan,
		joinType:     joinType,
		eqColsAreKey: eqColsAreKey,
		reqOrdering:  ReqOrdering(reqOrdering),
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
	joinType sqlbase.JoinType,
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
	vtableCols := sqlbase.ResultColumnsFromColDescs(tableDesc.ID, tableDesc.Columns)
	projectedVtableCols := planColumns(&tableScan)
	outputCols := make(sqlbase.ResultColumns, 0, len(inputCols)+len(projectedVtableCols))
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
		table:             tableDesc.TableDesc(),
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
	joinType sqlbase.JoinType,
	invertedExpr tree.TypedExpr,
	input exec.Node,
	table cat.Table,
	index cat.Index,
	inputCol exec.NodeColumnOrdinal,
	lookupCols exec.TableColumnOrdinalSet,
	onCond tree.TypedExpr,
	// The OutputOrdering is ignored since the inverted join always maintains
	// the ordering of the input rows.
	_ exec.OutputOrdering,
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
		input:        input.(planNode),
		table:        tableScan,
		joinType:     joinType,
		invertedExpr: invertedExpr,
		inputCol:     int32(inputCol),
	}
	if onCond != nil && onCond != tree.DBoolTrue {
		n.onExpr = onCond
	}
	// Build the result columns.
	inputCols := planColumns(input.(planNode))
	var scanCols sqlbase.ResultColumns
	if joinType != sqlbase.LeftSemiJoin && joinType != sqlbase.LeftAntiJoin {
		scanCols = planColumns(tableScan)
	}
	n.columns = make(sqlbase.ResultColumns, 0, len(inputCols)+len(scanCols))
	n.columns = append(n.columns, inputCols...)
	n.columns = append(n.columns, scanCols...)
	return n, nil
}

// Helper function to create a scanNode from just a table / index descriptor
// and requested cols.
func (ef *execFactory) constructScanForZigzag(
	indexDesc *sqlbase.IndexDescriptor,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	cols exec.NodeColumnOrdinalSet,
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
		sqlbase.ResultColumns,
		0,
		len(leftScan.resultColumns)+len(rightScan.resultColumns),
	)
	n.columns = append(n.columns, leftScan.resultColumns...)
	n.columns = append(n.columns, rightScan.resultColumns...)

	// Fixed values are the values fixed for a prefix of each side's index columns.
	// See the comment in pkg/sql/rowexec/zigzagjoiner.go for how they are used.
	for i := range fixedVals {
		valNode, ok := fixedVals[i].(*valuesNode)
		if !ok {
			panic("non-values node passed as fixed value to zigzag join")
		}
		if i >= len(n.sides) {
			panic("more fixed values passed than zigzag join sides")
		}
		n.sides[i].fixedVals = valNode
	}
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
func (ef *execFactory) ConstructBuffer(input exec.Node, label string) (exec.BufferNode, error) {
	return &bufferNode{
		plan:  input.(planNode),
		label: label,
	}, nil
}

// ConstructScanBuffer is part of the exec.Factory interface.
func (ef *execFactory) ConstructScanBuffer(ref exec.BufferNode, label string) (exec.Node, error) {
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
	n exec.Node, exprs tree.TypedExprs, zipCols sqlbase.ResultColumns, numColsPerGen []int,
) (exec.Node, error) {
	src := asDataSource(n)
	cols := append(src.columns, zipCols...)
	p := &projectSetNode{
		source:          src.plan,
		sourceCols:      src.columns,
		columns:         cols,
		numColsInSource: len(src.columns),
		exprs:           exprs,
		funcs:           make([]*tree.FuncExpr, len(exprs)),
		numColsPerGen:   numColsPerGen,
		run: projectSetRun{
			gens:      make([]tree.ValueGenerator, len(exprs)),
			done:      make([]bool, len(exprs)),
			rowBuffer: make(tree.Datums, len(cols)),
		},
	}

	for i, expr := range exprs {
		if tFunc, ok := expr.(*tree.FuncExpr); ok && tFunc.IsGeneratorApplication() {
			// Set-generating functions: generate_series() etc.
			p.funcs[i] = tFunc
		}
	}

	return p, nil
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
				// We have an empty ordering, but RANGE mode when at least one bound
				// has 'offset' requires a single column in ORDER BY. We have optimized
				// it out, but the execution still needs information about which column
				// it was, so we reconstruct the "original" ordering (note that the
				// direction of the ordering doesn't actually matter, so we leave it
				// with the default value).
				p.funcs[i].columnOrdering = sqlbase.ColumnOrdering{
					sqlbase.ColumnOrderInfo{
						ColIdx: int(wi.RangeOffsetColumn),
					},
				}
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
		columns:          sqlbase.ExplainOptColumns,
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
		columns:          sqlbase.ExplainOptColumns,
		tuples:           rows,
		specifiedInQuery: true,
	}, nil
}

// ConstructExplain is part of the exec.Factory interface.
func (ef *execFactory) ConstructExplain(
	options *tree.ExplainOptions, stmtType tree.StatementType, plan exec.Plan,
) (exec.Node, error) {
	return constructExplainPlanNode(options, stmtType, plan.(*planTop), ef.planner)
}

// ConstructShowTrace is part of the exec.Factory interface.
func (ef *execFactory) ConstructShowTrace(typ tree.ShowTraceType, compact bool) (exec.Node, error) {
	var node planNode = ef.planner.makeShowTraceNode(compact, typ == tree.ShowTraceKV)

	// Ensure the messages are sorted in age order, so that the user
	// does not get confused.
	ageColIdx := sqlbase.GetTraceAgeColumnIdx(compact)
	node = &sortNode{
		plan: node,
		ordering: sqlbase.ColumnOrdering{
			sqlbase.ColumnOrderInfo{ColIdx: ageColIdx, Direction: encoding.Ascending},
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
	insertColOrdSet exec.TableColumnOrdinalSet,
	returnColOrdSet exec.TableColumnOrdinalSet,
	checkOrdSet exec.CheckOrdinalSet,
	allowAutoCommit bool,
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
		ins.columns = sqlbase.ResultColumnsFromColDescs(tabDesc.GetID(), returnColDescs)

		// Set the tabColIdxToRetIdx for the mutation. Insert always returns
		// non-mutation columns in the same order they are defined in the table.
		ins.run.tabColIdxToRetIdx = row.ColMapping(tabDesc.Columns, returnColDescs)
		ins.run.rowsNeeded = true
	}

	if allowAutoCommit && ef.allowAutoCommit {
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
		ins.columns = sqlbase.ResultColumnsFromColDescs(tabDesc.GetID(), returnColDescs)

		// Set the tabColIdxToRetIdx for the mutation. Insert always returns
		// non-mutation columns in the same order they are defined in the table.
		ins.run.tabColIdxToRetIdx = row.ColMapping(tabDesc.Columns, returnColDescs)
		ins.run.rowsNeeded = true
	}

	if len(rows) == 0 {
		return &zeroNode{columns: ins.columns}, nil
	}

	if ef.allowAutoCommit {
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
	passthrough sqlbase.ResultColumns,
	allowAutoCommit bool,
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

	// Truncate any FetchCols added by MakeUpdater. The optimizer has already
	// computed a correct set that can sometimes be smaller.
	ru.FetchCols = ru.FetchCols[:len(fetchColDescs)]

	// updateColsIdx inverts the mapping of UpdateCols to FetchCols. See
	// the explanatory comments in updateRun.
	updateColsIdx := make(map[sqlbase.ColumnID]int, len(ru.UpdateCols))
	for i := range ru.UpdateCols {
		id := ru.UpdateCols[i].ID
		updateColsIdx[id] = i
	}

	upd := updateNodePool.Get().(*updateNode)
	*upd = updateNode{
		source: input.(planNode),
		run: updateRun{
			tu:        tableUpdater{ru: ru},
			checkOrds: checks,
			iVarContainerForComputedCols: sqlbase.RowIndexedVarContainer{
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

		upd.columns = sqlbase.ResultColumnsFromColDescs(tabDesc.GetID(), returnColDescs)
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

	if allowAutoCommit && ef.allowAutoCommit {
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
	canaryCol exec.NodeColumnOrdinal,
	insertColOrdSet exec.TableColumnOrdinalSet,
	fetchColOrdSet exec.TableColumnOrdinalSet,
	updateColOrdSet exec.TableColumnOrdinalSet,
	returnColOrdSet exec.TableColumnOrdinalSet,
	checks exec.CheckOrdinalSet,
	allowAutoCommit bool,
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

	// Truncate any FetchCols added by MakeUpdater. The optimizer has already
	// computed a correct set that can sometimes be smaller.
	ru.FetchCols = ru.FetchCols[:len(fetchColDescs)]

	// updateColsIdx inverts the mapping of UpdateCols to FetchCols. See
	// the explanatory comments in updateRun.
	updateColsIdx := make(map[sqlbase.ColumnID]int, len(ru.UpdateCols))
	for i := range ru.UpdateCols {
		id := ru.UpdateCols[i].ID
		updateColsIdx[id] = i
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
				alloc:         ef.planner.alloc,
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
		ups.columns = sqlbase.ResultColumnsFromColDescs(tabDesc.GetID(), returnColDescs)

		// Update the tabColIdxToRetIdx for the mutation. Upsert returns
		// non-mutation columns specified, in the same order they are defined
		// in the table.
		ups.run.tw.tabColIdxToRetIdx = row.ColMapping(tabDesc.Columns, returnColDescs)
		ups.run.tw.returnCols = returnColDescs
		ups.run.tw.collectRows = true
	}

	if allowAutoCommit && ef.allowAutoCommit {
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
	allowAutoCommit bool,
) (exec.Node, error) {
	ctx := ef.planner.extendedEvalCtx.Context

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
	rd, err := row.MakeDeleter(
		ctx,
		ef.planner.txn,
		ef.planner.ExecCfg().Codec,
		tabDesc,
		fetchColDescs,
		ef.planner.alloc,
	)
	if err != nil {
		return nil, err
	}

	// Truncate any FetchCols added by MakeUpdater. The optimizer has already
	// computed a correct set that can sometimes be smaller.
	rd.FetchCols = rd.FetchCols[:len(fetchColDescs)]

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
		del.columns = sqlbase.ResultColumnsFromColDescs(tabDesc.GetID(), returnColDescs)

		del.run.rowIdxToRetIdx = row.ColMapping(rd.FetchCols, returnColDescs)
		del.run.rowsNeeded = true
	}

	if allowAutoCommit && ef.allowAutoCommit {
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
	maxReturnedKeys int,
	allowAutoCommit bool,
) (exec.Node, error) {
	tabDesc := table.(*optTable).desc
	indexDesc := &tabDesc.PrimaryIndex
	sb := span.MakeBuilder(ef.planner.ExecCfg().Codec, tabDesc.TableDesc(), indexDesc)

	if err := ef.planner.maybeSetSystemConfig(tabDesc.GetID()); err != nil {
		return nil, err
	}

	// Setting the "forDelete" flag includes all column families in case where a
	// single record is deleted.
	spans, err := sb.SpansFromConstraint(indexConstraint, needed, true /* forDelete */)
	if err != nil {
		return nil, err
	}

	// Permitting autocommit in DeleteRange is very important, because DeleteRange
	// is used for simple deletes from primary indexes like
	// DELETE FROM t WHERE key = 1000
	// When possible, we need to make this a 1pc transaction for performance
	// reasons. At the same time, we have to be careful, because DeleteRange
	// returns all of the keys that it deleted - so we have to set a limit on the
	// DeleteRange request. But, trying to set autocommit and a limit on the
	// request doesn't work properly if the limit is hit. So, we permit autocommit
	// here if we can guarantee that the number of returned keys is finite and
	// relatively small.
	autoCommitEnabled := allowAutoCommit && ef.allowAutoCommit
	// If maxReturnedKeys is 0, it indicates that we weren't able to determine
	// the maximum number of returned keys, so we'll give up and not permit
	// autocommit.
	if maxReturnedKeys == 0 || maxReturnedKeys > TableTruncateChunkSize {
		autoCommitEnabled = false
	}

	dr := &deleteRangeNode{
		interleavedFastPath: false,
		spans:               spans,
		desc:                tabDesc,
		autoCommitEnabled:   autoCommitEnabled,
	}

	if len(interleavedTables) > 0 {
		dr.interleavedFastPath = true
		dr.interleavedDesc = make([]*sqlbase.ImmutableTableDescriptor, len(interleavedTables))
		for i := range dr.interleavedDesc {
			dr.interleavedDesc[i] = interleavedTables[i].(*optTable).desc
		}
	}
	return dr, nil
}

// ConstructCreateTable is part of the exec.Factory interface.
func (ef *execFactory) ConstructCreateTable(
	input exec.Node, schema cat.Schema, ct *tree.CreateTable,
) (exec.Node, error) {
	nd := &createTableNode{n: ct, dbDesc: schema.(*optSchema).desc}
	if input != nil {
		nd.sourcePlan = input.(planNode)
	}
	return nd, nil
}

// ConstructCreateView is part of the exec.Factory interface.
func (ef *execFactory) ConstructCreateView(
	schema cat.Schema,
	viewName string,
	ifNotExists bool,
	replace bool,
	temporary bool,
	viewQuery string,
	columns sqlbase.ResultColumns,
	deps opt.ViewDeps,
) (exec.Node, error) {

	planDeps := make(planDependencies, len(deps))
	for _, d := range deps {
		desc, err := getDescForDataSource(d.DataSource)
		if err != nil {
			return nil, err
		}
		var ref sqlbase.TableDescriptor_Reference
		if d.SpecificIndex {
			idx := d.DataSource.(cat.Table).Index(d.Index)
			ref.IndexID = idx.(*optIndex).desc.ID
		}
		if !d.ColumnOrdinals.Empty() {
			ref.ColumnIDs = make([]sqlbase.ColumnID, 0, d.ColumnOrdinals.Len())
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
		viewName:    tree.Name(viewName),
		ifNotExists: ifNotExists,
		replace:     replace,
		temporary:   temporary,
		viewQuery:   viewQuery,
		dbDesc:      schema.(*optSchema).desc,
		columns:     columns,
		planDeps:    planDeps,
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
	input exec.Node, mkErr func(tree.Datums) error,
) (exec.Node, error) {
	return &errorIfRowsNode{
		plan:  input.(planNode),
		mkErr: mkErr,
	}, nil
}

// ConstructOpaque is part of the exec.Factory interface.
func (ef *execFactory) ConstructOpaque(metadata opt.OpaqueMetadata) (exec.Node, error) {
	o, ok := metadata.(*opaqueMetadata)
	if !ok {
		return nil, errors.AssertionFailedf("unexpected OpaqueMetadata object type %T", metadata)
	}
	return o.plan, nil
}

// ConstructAlterTableSplit is part of the exec.Factory interface.
func (ef *execFactory) ConstructAlterTableSplit(
	index cat.Index, input exec.Node, expiration tree.TypedExpr,
) (exec.Node, error) {
	expirationTime, err := parseExpirationTime(ef.planner.EvalContext(), expiration)
	if err != nil {
		return nil, err
	}

	return &splitNode{
		tableDesc:      &index.Table().(*optTable).desc.TableDescriptor,
		index:          index.(*optIndex).desc,
		rows:           input.(planNode),
		expirationTime: expirationTime,
	}, nil
}

// ConstructAlterTableUnsplit is part of the exec.Factory interface.
func (ef *execFactory) ConstructAlterTableUnsplit(
	index cat.Index, input exec.Node,
) (exec.Node, error) {
	return &unsplitNode{
		tableDesc: &index.Table().(*optTable).desc.TableDescriptor,
		index:     index.(*optIndex).desc,
		rows:      input.(planNode),
	}, nil
}

// ConstructAlterTableUnsplitAll is part of the exec.Factory interface.
func (ef *execFactory) ConstructAlterTableUnsplitAll(index cat.Index) (exec.Node, error) {
	return &unsplitAllNode{
		tableDesc: &index.Table().(*optTable).desc.TableDescriptor,
		index:     index.(*optIndex).desc,
	}, nil
}

// ConstructAlterTableRelocate is part of the exec.Factory interface.
func (ef *execFactory) ConstructAlterTableRelocate(
	index cat.Index, input exec.Node, relocateLease bool,
) (exec.Node, error) {
	return &relocateNode{
		relocateLease: relocateLease,
		tableDesc:     &index.Table().(*optTable).desc.TableDescriptor,
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
func (rb *renderBuilder) setOutput(exprs tree.TypedExprs, columns sqlbase.ResultColumns) {
	rb.r.render = exprs
	rb.r.columns = columns
}

// makeColDescList returns a list of table column descriptors. Columns are
// included if their ordinal position in the table schema is in the cols set.
func makeColDescList(table cat.Table, cols exec.TableColumnOrdinalSet) []sqlbase.ColumnDescriptor {
	colDescs := make([]sqlbase.ColumnDescriptor, 0, cols.Len())
	for i, n := 0, table.DeletableColumnCount(); i < n; i++ {
		if !cols.Contains(i) {
			continue
		}
		colDescs = append(colDescs, *table.Column(i).(*sqlbase.ColumnDescriptor))
	}
	return colDescs
}
