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
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

type execFactory struct {
	planner *planner
}

var _ exec.Factory = &execFactory{}

func makeExecFactory(p *planner) execFactory {
	return execFactory{planner: p}
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
	needed exec.ColumnOrdinalSet,
	indexConstraint *constraint.Constraint,
	hardLimit int64,
	reverse bool,
	maxResults uint64,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	tabDesc := table.(*optTable).desc
	indexDesc := index.(*optIndex).desc
	// Create a scanNode.
	scan := ef.planner.Scan()
	colCfg := makeScanColumnsConfig(table, needed)

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
	scan.isSecondaryIndex = (indexDesc != &tabDesc.PrimaryIndex)
	scan.hardLimit = hardLimit
	scan.reverse = reverse
	scan.maxResults = maxResults
	scan.parallelScansEnabled = sqlbase.ParallelScans.Get(&ef.planner.extendedEvalCtx.Settings.SV)
	var err error
	scan.spans, err = spansFromConstraint(
		tabDesc,
		indexDesc,
		indexConstraint,
		needed,
		false, /* forDelete */
	)
	if err != nil {
		return nil, err
	}
	for i := range reqOrdering {
		if reqOrdering[i].ColIdx >= len(colCfg.wantedColumns) {
			return nil, errors.Errorf("invalid reqOrdering: %v", reqOrdering)
		}
	}
	scan.props.ordering = sqlbase.ColumnOrdering(reqOrdering)
	scan.createdByOpt = true
	return scan, nil
}

// ConstructVirtualScan is part of the exec.Factory interface.
func (ef *execFactory) ConstructVirtualScan(table cat.Table) (exec.Node, error) {
	tn := &table.(*optVirtualTable).name
	virtual, err := ef.planner.getVirtualTabler().getVirtualTableEntry(tn)
	if err != nil {
		return nil, err
	}
	columns, constructor := virtual.getPlanInfo()

	return &delayedNode{
		columns: columns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			return constructor(ctx, p, tn.Catalog())
		},
	}, nil
}

func asDataSource(n exec.Node) planDataSource {
	plan := n.(planNode)
	return planDataSource{
		info: &sqlbase.DataSourceInfo{SourceColumns: planColumns(plan)},
		plan: plan,
	}
}

// ConstructFilter is part of the exec.Factory interface.
func (ef *execFactory) ConstructFilter(
	n exec.Node, filter tree.TypedExpr, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	// Push the filter into the scanNode. We cannot do this if the scanNode has a
	// limit (it would make the limit apply AFTER the filter).
	if s, ok := n.(*scanNode); ok && s.filter == nil && s.hardLimit == 0 {
		s.filter = s.filterVars.Rebind(filter, true /* alsoReset */, false /* normalizeToNonNil */)
		s.props.ordering = sqlbase.ColumnOrdering(reqOrdering)
		return s, nil
	}
	// Create a filterNode.
	src := asDataSource(n)
	f := &filterNode{
		source: src,
	}
	f.ivarHelper = tree.MakeIndexedVarHelper(f, len(src.info.SourceColumns))
	f.filter = f.ivarHelper.Rebind(filter, true /* alsoReset */, false /* normalizeToNonNil */)
	f.props.ordering = sqlbase.ColumnOrdering(reqOrdering)

	// If there's a spool, pull it up.
	if spool, ok := f.source.plan.(*spoolNode); ok {
		f.source.plan = spool.source
		spool.source = f
		return spool, nil
	}
	return f, nil
}

// ConstructSimpleProject is part of the exec.Factory interface.
func (ef *execFactory) ConstructSimpleProject(
	n exec.Node, cols []exec.ColumnOrdinal, colNames []string, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	// If the top node is already a renderNode, just rearrange the columns. But
	// we don't want to duplicate a rendering expression (in case it is expensive
	// to compute or has side-effects); so if we have duplicates we avoid this
	// optimization (and add a new renderNode).
	if r, ok := n.(*renderNode); ok && !hasDuplicates(cols) {
		oldCols, oldRenders := r.columns, r.render
		r.columns = make(sqlbase.ResultColumns, len(cols))
		r.render = make([]tree.TypedExpr, len(cols))
		for i, ord := range cols {
			r.columns[i] = oldCols[ord]
			if colNames != nil {
				r.columns[i].Name = colNames[i]
			}
			r.render[i] = oldRenders[ord]
		}
		r.props.ordering = sqlbase.ColumnOrdering(reqOrdering)
		return r, nil
	}
	var inputCols sqlbase.ResultColumns
	if colNames == nil {
		// We will need the names of the input columns.
		inputCols = planColumns(n.(planNode))
	}

	var rb renderBuilder
	rb.init(n, reqOrdering, len(cols))
	for i, col := range cols {
		v := rb.r.ivarHelper.IndexedVar(int(col))
		if colNames == nil {
			rb.addExpr(v, inputCols[col].Name)
		} else {
			rb.addExpr(v, colNames[i])
		}
	}
	return rb.res, nil
}

func hasDuplicates(cols []exec.ColumnOrdinal) bool {
	var set util.FastIntSet
	for _, c := range cols {
		if set.Contains(int(c)) {
			return true
		}
		set.Add(int(c))
	}
	return false
}

// ConstructRender is part of the exec.Factory interface.
func (ef *execFactory) ConstructRender(
	n exec.Node, exprs tree.TypedExprs, colNames []string, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	var rb renderBuilder
	rb.init(n, reqOrdering, len(exprs))
	for i, expr := range exprs {
		expr = rb.r.ivarHelper.Rebind(expr, false /* alsoReset */, true /* normalizeToNonNil */)
		rb.addExpr(expr, colNames[i])
	}
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
	leftEqCols, rightEqCols []exec.ColumnOrdinal,
	leftEqColsAreKey, rightEqColsAreKey bool,
	extraOnCond tree.TypedExpr,
) (exec.Node, error) {
	p := ef.planner
	leftSrc := asDataSource(left)
	rightSrc := asDataSource(right)
	pred, _, err := p.makeJoinPredicate(
		context.TODO(), leftSrc.info, rightSrc.info, joinType, nil, /* cond */
	)
	if err != nil {
		return nil, err
	}

	numEqCols := len(leftEqCols)
	// Save some allocations by putting both sides in the same slice.
	intBuf := make([]int, 2*numEqCols)
	pred.leftEqualityIndices = intBuf[:numEqCols:numEqCols]
	pred.rightEqualityIndices = intBuf[numEqCols:]
	nameBuf := make(tree.NameList, 2*numEqCols)
	pred.leftColNames = nameBuf[:numEqCols:numEqCols]
	pred.rightColNames = nameBuf[numEqCols:]

	for i := range leftEqCols {
		pred.leftEqualityIndices[i] = int(leftEqCols[i])
		pred.rightEqualityIndices[i] = int(rightEqCols[i])
		pred.leftColNames[i] = tree.Name(leftSrc.info.SourceColumns[leftEqCols[i]].Name)
		pred.rightColNames[i] = tree.Name(rightSrc.info.SourceColumns[rightEqCols[i]].Name)
	}
	pred.leftEqKey = leftEqColsAreKey
	pred.rightEqKey = rightEqColsAreKey

	pred.onCond = pred.iVarHelper.Rebind(
		extraOnCond, false /* alsoReset */, false, /* normalizeToNonNil */
	)

	return p.makeJoinNode(leftSrc, rightSrc, pred), nil
}

// ConstructApplyJoin is part of the exec.Factory interface.
func (ef *execFactory) ConstructApplyJoin(
	joinType sqlbase.JoinType,
	left exec.Node,
	leftBoundColMap opt.ColMap,
	memo *memo.Memo,
	rightProps *physical.Required,
	fakeRight exec.Node,
	right memo.RelExpr,
	onCond tree.TypedExpr,
) (exec.Node, error) {
	leftSrc := asDataSource(left)
	rightSrc := asDataSource(fakeRight)
	rightSrc.plan.Close(context.TODO())
	p := ef.planner
	pred, _, err := p.makeJoinPredicate(
		context.TODO(), leftSrc.info, rightSrc.info, joinType, nil, /* cond */
	)
	if err != nil {
		return nil, err
	}
	pred.onCond = pred.iVarHelper.Rebind(
		onCond, false /* alsoReset */, false, /* normalizeToNonNil */
	)
	rightCols := rightSrc.info.SourceColumns
	return newApplyJoinNode(joinType, asDataSource(left), leftBoundColMap, rightProps, rightCols, right, pred, memo)
}

// ConstructMergeJoin is part of the exec.Factory interface.
func (ef *execFactory) ConstructMergeJoin(
	joinType sqlbase.JoinType,
	left, right exec.Node,
	onCond tree.TypedExpr,
	leftOrdering, rightOrdering sqlbase.ColumnOrdering,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	p := ef.planner
	leftSrc := asDataSource(left)
	rightSrc := asDataSource(right)
	pred, _, err := p.makeJoinPredicate(
		context.TODO(), leftSrc.info, rightSrc.info, joinType, nil, /* cond */
	)
	if err != nil {
		return nil, err
	}
	pred.onCond = pred.iVarHelper.Rebind(
		onCond, false /* alsoReset */, false, /* normalizeToNonNil */
	)

	n := len(leftOrdering)
	if n == 0 || len(rightOrdering) != n {
		return nil, errors.Errorf("orderings from the left and right side must be the same non-zero length")
	}
	pred.leftEqualityIndices = make([]int, n)
	pred.rightEqualityIndices = make([]int, n)
	pred.leftColNames = make(tree.NameList, n)
	pred.rightColNames = make(tree.NameList, n)
	for i := 0; i < n; i++ {
		leftColIdx, rightColIdx := leftOrdering[i].ColIdx, rightOrdering[i].ColIdx
		pred.leftEqualityIndices[i] = leftColIdx
		pred.rightEqualityIndices[i] = rightColIdx
		pred.leftColNames[i] = tree.Name(leftSrc.info.SourceColumns[leftColIdx].Name)
		pred.rightColNames[i] = tree.Name(rightSrc.info.SourceColumns[rightColIdx].Name)
	}

	node := p.makeJoinNode(leftSrc, rightSrc, pred)
	node.mergeJoinOrdering = make(sqlbase.ColumnOrdering, n)
	for i := 0; i < n; i++ {
		// The mergeJoinOrdering "columns" are equality column indices.  Because of
		// the way we constructed the equality indices, the ordering will always be
		// 0,1,2,3..
		node.mergeJoinOrdering[i].ColIdx = i
		node.mergeJoinOrdering[i].Direction = leftOrdering[i].Direction
	}

	// Set up node.props, which tells the distsql planner to maintain the
	// resulting ordering (if needed).
	node.props.ordering = sqlbase.ColumnOrdering(reqOrdering)

	return node, nil
}

// ConstructScalarGroupBy is part of the exec.Factory interface.
func (ef *execFactory) ConstructScalarGroupBy(
	input exec.Node, aggregations []exec.AggInfo,
) (exec.Node, error) {
	n := &groupNode{
		plan:     input.(planNode),
		funcs:    make([]*aggregateFuncHolder, 0, len(aggregations)),
		columns:  make(sqlbase.ResultColumns, 0, len(aggregations)),
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
	groupCols []exec.ColumnOrdinal,
	groupColOrdering sqlbase.ColumnOrdering,
	aggregations []exec.AggInfo,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	n := &groupNode{
		plan:             input.(planNode),
		funcs:            make([]*aggregateFuncHolder, 0, len(groupCols)+len(aggregations)),
		columns:          make(sqlbase.ResultColumns, 0, len(groupCols)+len(aggregations)),
		groupCols:        make([]int, len(groupCols)),
		groupColOrdering: groupColOrdering,
		isScalar:         false,
		props: physicalProps{
			ordering: sqlbase.ColumnOrdering(reqOrdering),
		},
	}
	inputCols := planColumns(n.plan)
	for i := range groupCols {
		col := int(groupCols[i])
		n.groupCols[i] = col

		// TODO(radu): only generate the grouping columns we actually need.
		f := n.newAggregateFuncHolder(
			builtins.AnyNotNull,
			inputCols[col].Typ,
			col,
			builtins.NewAnyNotNullAggregate,
			nil, /* arguments */
			ef.planner.EvalContext().Mon.MakeBoundAccount(),
		)
		n.funcs = append(n.funcs, f)
		n.columns = append(n.columns, inputCols[col])
	}
	if err := ef.addAggregations(n, aggregations); err != nil {
		return nil, err
	}
	return n, nil
}

func (ef *execFactory) addAggregations(n *groupNode, aggregations []exec.AggInfo) error {
	inputCols := planColumns(n.plan)
	for i := range aggregations {
		agg := &aggregations[i]
		builtin := agg.Builtin
		var renderIdx int
		var aggFn func(*tree.EvalContext, tree.Datums) tree.AggregateFunc

		switch len(agg.ArgCols) {
		case 0:
			renderIdx = noRenderIdx
			aggFn = func(evalCtx *tree.EvalContext, arguments tree.Datums) tree.AggregateFunc {
				return builtin.AggregateFunc([]*types.T{}, evalCtx, arguments)
			}

		case 1:
			renderIdx = int(agg.ArgCols[0])
			aggFn = func(evalCtx *tree.EvalContext, arguments tree.Datums) tree.AggregateFunc {
				return builtin.AggregateFunc([]*types.T{inputCols[renderIdx].Typ}, evalCtx, arguments)
			}

		default:
			return unimplemented.NewWithIssue(28417,
				"aggregate functions with multiple non-constant expressions are not supported",
			)
		}

		f := n.newAggregateFuncHolder(
			agg.FuncName,
			agg.ResultType,
			renderIdx,
			aggFn,
			agg.ConstArgs,
			ef.planner.EvalContext().Mon.MakeBoundAccount(),
		)
		if agg.Distinct {
			f.setDistinct()
		}

		if agg.Filter == -1 {
			// A value of -1 means the aggregate had no filter.
			f.filterRenderIdx = noRenderIdx
		} else {
			f.filterRenderIdx = int(agg.Filter)
		}

		n.funcs = append(n.funcs, f)
		n.columns = append(n.columns, sqlbase.ResultColumn{
			Name: agg.FuncName,
			Typ:  agg.ResultType,
		})
	}
	return nil
}

// ConstructDistinct is part of the exec.Factory interface.
func (ef *execFactory) ConstructDistinct(
	input exec.Node, distinctCols, orderedCols exec.ColumnOrdinalSet,
) (exec.Node, error) {
	return &distinctNode{
		plan:              input.(planNode),
		distinctOnColIdxs: distinctCols,
		columnsInOrder:    orderedCols,
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
	input exec.Node, ordering sqlbase.ColumnOrdering,
) (exec.Node, error) {
	plan := input.(planNode)
	inputColumns := planColumns(plan)
	return &sortNode{
		plan:     plan,
		columns:  inputColumns,
		ordering: ordering,
		needSort: true,
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
	input exec.Node, table cat.Table, cols exec.ColumnOrdinalSet, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	tabDesc := table.(*optTable).desc
	colCfg := makeScanColumnsConfig(table, cols)
	colDescs := makeColDescList(table, cols)

	// TODO(rytaft): saveTableNode is not yet supported as input to an index
	// join, so discard the saveTableNode.
	if saveTable, ok := input.(*saveTableNode); ok {
		input = saveTable.source
	}

	// TODO(justin): this would be something besides a scanNode in the general
	// case of a lookup join.
	var scan *scanNode
	switch t := input.(type) {
	case *scanNode:
		scan = t
	case *zeroNode:
		// zeroNode is possible when the scanNode had a contradiction constraint.
		return newZeroNode(sqlbase.ResultColumnsFromColDescs(colDescs)), nil
	default:
		return nil, fmt.Errorf("%T not supported as input to lookup join", t)
	}

	tableScan := ef.planner.Scan()

	if err := tableScan.initTable(context.TODO(), ef.planner, tabDesc, nil, colCfg); err != nil {
		return nil, err
	}

	primaryIndex := tabDesc.GetPrimaryIndex()
	tableScan.index = &primaryIndex
	tableScan.isSecondaryIndex = false
	tableScan.disableBatchLimit()

	primaryKeyColumns, colIDtoRowIndex := processIndexJoinColumns(tableScan, scan)
	primaryKeyPrefix := roachpb.Key(sqlbase.MakeIndexKeyPrefix(tabDesc.TableDesc(), tableScan.index.ID))

	return &indexJoinNode{
		index:             scan,
		table:             tableScan,
		primaryKeyColumns: primaryKeyColumns,
		cols:              colDescs,
		resultColumns:     sqlbase.ResultColumnsFromColDescs(colDescs),
		run: indexJoinRun{
			primaryKeyPrefix: primaryKeyPrefix,
			colIDtoRowIndex:  colIDtoRowIndex,
		},
		props: physicalProps{
			ordering: sqlbase.ColumnOrdering(reqOrdering),
		},
	}, nil
}

// ConstructLookupJoin is part of the exec.Factory interface.
func (ef *execFactory) ConstructLookupJoin(
	joinType sqlbase.JoinType,
	input exec.Node,
	table cat.Table,
	index cat.Index,
	eqCols []exec.ColumnOrdinal,
	eqColsAreKey bool,
	lookupCols exec.ColumnOrdinalSet,
	onCond tree.TypedExpr,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	tabDesc := table.(*optTable).desc
	indexDesc := index.(*optIndex).desc
	colCfg := makeScanColumnsConfig(table, lookupCols)
	tableScan := ef.planner.Scan()

	if err := tableScan.initTable(context.TODO(), ef.planner, tabDesc, nil, colCfg); err != nil {
		return nil, err
	}

	tableScan.index = indexDesc
	tableScan.isSecondaryIndex = (indexDesc != &tabDesc.PrimaryIndex)

	n := &lookupJoinNode{
		input:        input.(planNode),
		table:        tableScan,
		joinType:     joinType,
		eqColsAreKey: eqColsAreKey,
		props: physicalProps{
			ordering: sqlbase.ColumnOrdering(reqOrdering),
		},
	}
	if onCond != nil && onCond != tree.DBoolTrue {
		n.onCond = onCond
	}
	n.eqCols = make([]int, len(eqCols))
	for i, c := range eqCols {
		n.eqCols[i] = int(c)
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
	cols exec.ColumnOrdinalSet,
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
	scan.isSecondaryIndex = (indexDesc.ID != tableDesc.PrimaryIndex.ID)

	return scan, nil
}

// ConstructZigzagJoin is part of the exec.Factory interface.
func (ef *execFactory) ConstructZigzagJoin(
	leftTable cat.Table,
	leftIndex cat.Index,
	rightTable cat.Table,
	rightIndex cat.Index,
	leftEqCols []exec.ColumnOrdinal,
	rightEqCols []exec.ColumnOrdinal,
	leftCols exec.ColumnOrdinalSet,
	rightCols exec.ColumnOrdinalSet,
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
		props: physicalProps{
			ordering: sqlbase.ColumnOrdering(reqOrdering),
		},
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
	// See the comment in pkg/sql/distsqlrun/zigzagjoiner.go for how they are used.
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
func (ef *execFactory) ConstructMax1Row(input exec.Node) (exec.Node, error) {
	plan := input.(planNode)
	return &max1RowNode{
		plan: plan,
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
	return &scanBufferNode{
		buffer: ref.(*bufferNode),
		label:  label,
	}, nil
}

// ConstructProjectSet is part of the exec.Factory interface.
func (ef *execFactory) ConstructProjectSet(
	n exec.Node, exprs tree.TypedExprs, zipCols sqlbase.ResultColumns, numColsPerGen []int,
) (exec.Node, error) {
	src := asDataSource(n)
	cols := append(src.info.SourceColumns, zipCols...)
	p := &projectSetNode{
		source:          src.plan,
		sourceInfo:      src.info,
		columns:         cols,
		numColsInSource: len(src.info.SourceColumns),
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

		p.windowRender[wi.OutputIdxs[i]] = p.funcs[i]
	}

	return p, nil
}

// ConstructPlan is part of the exec.Factory interface.
func (ef *execFactory) ConstructPlan(
	root exec.Node, subqueries []exec.Subquery, postqueries []exec.Node,
) (exec.Plan, error) {
	// Enable auto-commit if the planner setting allows it, and there are no
	// postqueries.
	if ef.planner.autoCommit && len(postqueries) == 0 {
		if ac, ok := root.(autoCommitNode); ok {
			ac.enableAutoCommit()
		}
	}
	// No need to spool at the root.
	if spool, ok := root.(*spoolNode); ok {
		root = spool.source
	}
	res := &planTop{
		plan:        root.(planNode),
		auditEvents: ef.planner.curPlan.auditEvents,
	}
	if len(subqueries) > 0 {
		res.subqueryPlans = make([]subquery, len(subqueries))
		for i := range subqueries {
			in := &subqueries[i]
			out := &res.subqueryPlans[i]
			out.subquery = in.ExprNode
			switch in.Mode {
			case exec.SubqueryExists:
				out.execMode = distsqlrun.SubqueryExecModeExists
			case exec.SubqueryOneRow:
				out.execMode = distsqlrun.SubqueryExecModeOneRow
			case exec.SubqueryAnyRows:
				out.execMode = distsqlrun.SubqueryExecModeAllRowsNormalized
			case exec.SubqueryAllRows:
				out.execMode = distsqlrun.SubqueryExecModeAllRows
			default:
				return nil, errors.Errorf("invalid SubqueryMode %d", in.Mode)
			}
			out.expanded = true
			out.plan = in.Root.(planNode)
		}
	}
	if len(postqueries) > 0 {
		res.postqueryPlans = make([]postquery, len(postqueries))
		for i := range res.postqueryPlans {
			res.postqueryPlans[i].plan = postqueries[i].(planNode)
		}
	}

	return res, nil
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

// environmentQuery is a helper to run a query to build up the output of
// showEnv. It expects a query that returns a single string column.
func (ef *execFactory) environmentQuery(query string) (string, error) {
	r, err := ef.planner.extendedEvalCtx.InternalExecutor.QueryRow(
		ef.planner.EvalContext().Context,
		"EXPLAIN (env)",
		ef.planner.Txn(),
		query,
	)
	if err != nil {
		return "", err
	}

	if len(r) != 1 {
		return "", errors.AssertionFailedf(
			"expected env query %q to return a single column, returned %d",
			query,
			len(r),
		)
	}

	s, ok := r[0].(*tree.DString)
	if !ok {
		return "", errors.AssertionFailedf(
			"expected env query %q to return a DString, returned %T",
			query,
			r[0],
		)
	}

	return string(*s), nil
}

var testingOverrideExplainEnvVersion string

// TestingOverrideExplainEnvVersion overrides the version reported by
// EXPLAIN (OPT, ENV). Used for testing.
func TestingOverrideExplainEnvVersion(ver string) func() {
	prev := testingOverrideExplainEnvVersion
	testingOverrideExplainEnvVersion = ver
	return func() { testingOverrideExplainEnvVersion = prev }
}

// showEnv implements EXPLAIN (opt, env). It returns a node which displays
// the environment a query was run in.
func (ef *execFactory) showEnv(plan string, envOpts exec.ExplainEnvData) (exec.Node, error) {
	var out urlOutputter

	// Show the version of Cockroach running.
	version, err := ef.environmentQuery("SELECT version()")
	if err != nil {
		return nil, err
	}
	if testingOverrideExplainEnvVersion != "" {
		version = testingOverrideExplainEnvVersion
	}
	out.writef("Version: %s\n", version)

	// Show the definition of each referenced catalog object.
	for _, tn := range envOpts.Sequences {
		createStatement, err := ef.environmentQuery(
			fmt.Sprintf("SELECT create_statement FROM [SHOW CREATE SEQUENCE %s]", tn.String()),
		)
		if err != nil {
			return nil, err
		}

		out.writef("%s;\n", createStatement)
	}

	// TODO(justin): it might also be relevant in some cases to print the create
	// statements for tables referenced via FKs in these tables.
	for _, tn := range envOpts.Tables {
		createStatement, err := ef.environmentQuery(
			fmt.Sprintf("SELECT create_statement FROM [SHOW CREATE TABLE %s]", tn.String()),
		)
		if err != nil {
			return nil, err
		}

		out.writef("%s;\n", createStatement)

		// In addition to the schema, it's important to know what the table
		// statistics on each table are.

		// NOTE: The histogram buckets take up a ton of vertical space and we
		// don't use them in planning, so don't include them.
		// TODO(justin): Revisit this once we use histograms in planning.
		stats, err := ef.environmentQuery(
			fmt.Sprintf(
				`
SELECT
	jsonb_pretty(COALESCE(json_agg(stat), '[]'))
FROM
	(
		SELECT
			json_array_elements(statistics) - 'histo_buckets' AS stat
		FROM
			[SHOW STATISTICS USING JSON FOR TABLE %s]
	)
`,
				tn.String(),
			),
		)
		if err != nil {
			return nil, err
		}

		out.writef("ALTER TABLE %s INJECT STATISTICS '%s';\n", tn.String(), stats)
	}

	for _, tn := range envOpts.Views {
		createStatement, err := ef.environmentQuery(
			fmt.Sprintf("SELECT create_statement FROM [SHOW CREATE VIEW %s]", tn.String()),
		)
		if err != nil {
			return nil, err
		}

		out.writef("%s;\n", createStatement)
	}

	// Show the values of any non-default session variables that can impact
	// planning decisions.

	value, err := ef.environmentQuery(fmt.Sprintf("SHOW reorder_joins_limit"))
	if err != nil {
		return nil, err
	}
	if value != strconv.FormatInt(opt.DefaultJoinOrderLimit, 10) {
		out.writef("SET reorder_joins_limit = %s;\n", value)
	}

	for _, param := range []string{
		"experimental_enable_zigzag_join",
	} {
		value, err := ef.environmentQuery(fmt.Sprintf("SHOW %s", param))
		if err != nil {
			return nil, err
		}
		defaultVal := varGen[param].GlobalDefault(nil)
		if value != defaultVal {
			out.writef("SET %s = %s;\n", param, value)
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
	p := plan.(*planTop)

	analyzeSet := options.Flags.Contains(tree.ExplainFlagAnalyze)

	if options.Flags.Contains(tree.ExplainFlagEnv) {
		return nil, errors.New("ENV only supported with (OPT) option")
	}

	switch options.Mode {
	case tree.ExplainDistSQL:
		return &explainDistSQLNode{
			plan:          p.plan,
			subqueryPlans: p.subqueryPlans,
			analyze:       analyzeSet,
			stmtType:      stmtType,
		}, nil

	case tree.ExplainPlan:
		if analyzeSet {
			return nil, errors.New("EXPLAIN ANALYZE only supported with (DISTSQL) option")
		}
		// NOEXPAND and NOOPTIMIZE must always be set when using the optimizer to
		// prevent the plans from being modified.
		opts := *options
		opts.Flags.Add(tree.ExplainFlagNoExpand)
		opts.Flags.Add(tree.ExplainFlagNoOptimize)
		return ef.planner.makeExplainPlanNodeWithPlan(
			context.TODO(),
			&opts,
			false, /* optimizeSubqueries */
			p.plan,
			p.subqueryPlans,
			p.postqueryPlans,
		)

	default:
		panic(fmt.Sprintf("unsupported explain mode %v", options.Mode))
	}
}

// ConstructShowTrace is part of the exec.Factory interface.
func (ef *execFactory) ConstructShowTrace(typ tree.ShowTraceType, compact bool) (exec.Node, error) {
	var node planNode = ef.planner.makeShowTraceNode(compact, typ == tree.ShowTraceKV)

	// Ensure the messages are sorted in age order, so that the user
	// does not get confused.
	ageColIdx := sqlbase.GetTraceAgeColumnIdx(compact)
	node = &sortNode{
		plan:    node,
		columns: planColumns(node),
		ordering: sqlbase.ColumnOrdering{
			sqlbase.ColumnOrderInfo{ColIdx: ageColIdx, Direction: encoding.Ascending},
		},
		needSort: true,
	}

	if typ == tree.ShowTraceReplica {
		node = &showTraceReplicaNode{plan: node}
	}
	return node, nil
}

// mutationRowIdxToReturnIdx returns the mapping from the origColDescs to the
// returnColDescs (where returnColDescs is a subset of the origColDescs).
// -1 is used for columns not part of the returnColDescs.
// It is the responsibility of the caller to ensure a mapping is possible.
func mutationRowIdxToReturnIdx(origColDescs, returnColDescs []sqlbase.ColumnDescriptor) []int {
	// Create a ColumnID to index map.
	colIDToRetIndex := row.ColIDtoRowIndexFromCols(origColDescs)

	// Initialize the rowIdxToTabColIdx array.
	rowIdxToRetIdx := make([]int, len(origColDescs))
	for i := range rowIdxToRetIdx {
		// -1 value indicates that this column is not being returned.
		rowIdxToRetIdx[i] = -1
	}

	// Set the appropriate index values for the returning columns.
	for i := range returnColDescs {
		if idx, ok := colIDToRetIndex[returnColDescs[i].ID]; ok {
			rowIdxToRetIdx[idx] = i
		}
	}

	return rowIdxToRetIdx
}

func (ef *execFactory) ConstructInsert(
	input exec.Node,
	table cat.Table,
	insertColOrdSet exec.ColumnOrdinalSet,
	returnColOrdSet exec.ColumnOrdinalSet,
	checkOrdSet exec.CheckOrdinalSet,
	skipFKChecks bool,
) (exec.Node, error) {
	// Derive insert table and column descriptors.
	rowsNeeded := !returnColOrdSet.Empty()
	tabDesc := table.(*optTable).desc
	colDescs := makeColDescList(table, insertColOrdSet)

	// Construct the check helper if there are any check constraints.
	checkHelper := sqlbase.NewInputCheckHelper(checkOrdSet, tabDesc)

	// Determine the foreign key tables involved in the update.
	fkTables, err := ef.makeFkMetadata(tabDesc, row.CheckInserts, checkHelper)
	if err != nil {
		return nil, err
	}

	// Create the table insert, which does the bulk of the work.
	checkFKs := row.CheckFKs
	if skipFKChecks {
		checkFKs = row.SkipFKs
	}
	ri, err := row.MakeInserter(ef.planner.txn, tabDesc, fkTables, colDescs,
		checkFKs, &ef.planner.alloc)
	if err != nil {
		return nil, err
	}

	// Determine the relational type of the generated insert node.
	// If rows are not needed, no columns are returned.
	var returnCols sqlbase.ResultColumns
	var tabColIdxToRetIdx []int
	if rowsNeeded {
		returnColDescs := makeColDescList(table, returnColOrdSet)

		// Only return the columns that are part of the table descriptor.
		// This is important when columns are added and being back-filled
		// as part of the same transaction when the delete runs.
		// In such cases, the newly added columns shouldn't be returned.
		// See regression logic tests for #29494.
		if len(tabDesc.Columns) < len(returnColDescs) {
			returnColDescs = returnColDescs[:len(tabDesc.Columns)]
		}

		returnCols = sqlbase.ResultColumnsFromColDescs(returnColDescs)

		// Update the tabColIdxToRetIdx for the mutation. Insert always
		// returns non-mutation columns in the same order they are defined in
		// the table.
		tabColIdxToRetIdx = mutationRowIdxToReturnIdx(tabDesc.Columns, returnColDescs)
	}

	// Regular path for INSERT.
	ins := insertNodePool.Get().(*insertNode)
	*ins = insertNode{
		source:  input.(planNode),
		columns: returnCols,
		run: insertRun{
			ti:          tableInserter{ri: ri},
			checkHelper: checkHelper,
			rowsNeeded:  rowsNeeded,
			iVarContainerForComputedCols: sqlbase.RowIndexedVarContainer{
				Cols:    tabDesc.Columns,
				Mapping: ri.InsertColIDtoRowIndex,
			},
			insertCols:        ri.InsertCols,
			tabColIdxToRetIdx: tabColIdxToRetIdx,
		},
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
	fetchColOrdSet exec.ColumnOrdinalSet,
	updateColOrdSet exec.ColumnOrdinalSet,
	returnColOrdSet exec.ColumnOrdinalSet,
	checks exec.CheckOrdinalSet,
) (exec.Node, error) {
	// Derive table and column descriptors.
	rowsNeeded := !returnColOrdSet.Empty()
	tabDesc := table.(*optTable).desc
	fetchColDescs := makeColDescList(table, fetchColOrdSet)

	// Add each column to update as a sourceSlot. The CBO only uses scalarSlot,
	// since it compiles tuples and subqueries into a simple sequence of target
	// columns.
	updateColDescs := makeColDescList(table, updateColOrdSet)
	sourceSlots := make([]sourceSlot, len(updateColDescs))
	for i := range sourceSlots {
		sourceSlots[i] = scalarSlot{column: updateColDescs[i], sourceIndex: len(fetchColDescs) + i}
	}

	// Construct the check helper if there are any check constraints.
	checkHelper := sqlbase.NewInputCheckHelper(checks, tabDesc)

	// Determine the foreign key tables involved in the update.
	fkTables, err := row.MakeFkMetadata(
		ef.planner.extendedEvalCtx.Context,
		tabDesc,
		row.CheckUpdates,
		ef.planner.LookupTableByID,
		ef.planner.CheckPrivilege,
		ef.planner.analyzeExpr,
		checkHelper,
	)
	if err != nil {
		return nil, err
	}

	// Create the table updater, which does the bulk of the work. In the HP,
	// the updater derives the columns that need to be fetched. By contrast, the
	// CBO will have already determined the set of fetch and update columns, and
	// passes those sets into the updater (which will basically be a no-op).
	ru, err := row.MakeUpdater(
		ef.planner.txn,
		tabDesc,
		fkTables,
		updateColDescs,
		fetchColDescs,
		row.UpdaterDefault,
		ef.planner.EvalContext(),
		&ef.planner.alloc,
	)
	if err != nil {
		return nil, err
	}

	// Truncate any FetchCols added by MakeUpdater. The optimizer has already
	// computed a correct set that can sometimes be smaller.
	ru.FetchCols = ru.FetchCols[:len(fetchColDescs)]

	// Determine the relational type of the generated update node.
	// If rows are not needed, no columns are returned.
	var returnCols sqlbase.ResultColumns
	var rowIdxToRetIdx []int
	if rowsNeeded {
		returnColDescs := makeColDescList(table, returnColOrdSet)

		// Only return the columns that are part of the table descriptor.
		// This is important when columns are added and being back-filled
		// as part of the same transaction when the update runs.
		// In such cases, the newly added columns shouldn't be returned.
		// See regression logic tests for #29494.
		if len(tabDesc.Columns) < len(returnColDescs) {
			returnColDescs = returnColDescs[:len(tabDesc.Columns)]
		}

		returnCols = sqlbase.ResultColumnsFromColDescs(returnColDescs)

		// Update the rowIdxToRetIdx for the mutation. Update returns
		// the non-mutation columns specified, in the same order they are
		// defined in the table.
		//
		// The Updater derives/stores the fetch columns of the mutation and
		// since the return columns are always a subset of the fetch columns,
		// we can use use the fetch columns to generate the mapping for the
		// returned rows.
		rowIdxToRetIdx = mutationRowIdxToReturnIdx(ru.FetchCols, returnColDescs)
	}

	// updateColsIdx inverts the mapping of UpdateCols to FetchCols. See
	// the explanatory comments in updateRun.
	updateColsIdx := make(map[sqlbase.ColumnID]int, len(ru.UpdateCols))
	for i := range ru.UpdateCols {
		id := ru.UpdateCols[i].ID
		updateColsIdx[id] = i
	}

	upd := updateNodePool.Get().(*updateNode)
	*upd = updateNode{
		source:  input.(planNode),
		columns: returnCols,
		run: updateRun{
			tu:          tableUpdater{ru: ru},
			checkHelper: checkHelper,
			rowsNeeded:  rowsNeeded,
			iVarContainerForComputedCols: sqlbase.RowIndexedVarContainer{
				CurSourceRow: make(tree.Datums, len(ru.FetchCols)),
				Cols:         ru.FetchCols,
				Mapping:      ru.FetchColIDtoRowIndex,
			},
			sourceSlots:    sourceSlots,
			updateValues:   make(tree.Datums, len(ru.UpdateCols)),
			updateColsIdx:  updateColsIdx,
			rowIdxToRetIdx: rowIdxToRetIdx,
		},
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

func (ef *execFactory) makeFkMetadata(
	tabDesc *sqlbase.ImmutableTableDescriptor,
	fkCheckType row.FKCheckType,
	checkHelper *sqlbase.CheckHelper,
) (row.FkTableMetadata, error) {
	// Determine the foreign key tables involved in the upsert.
	return row.MakeFkMetadata(
		ef.planner.extendedEvalCtx.Context,
		tabDesc,
		fkCheckType,
		ef.planner.LookupTableByID,
		ef.planner.CheckPrivilege,
		ef.planner.analyzeExpr,
		checkHelper,
	)
}

func (ef *execFactory) ConstructUpsert(
	input exec.Node,
	table cat.Table,
	canaryCol exec.ColumnOrdinal,
	insertColOrdSet exec.ColumnOrdinalSet,
	fetchColOrdSet exec.ColumnOrdinalSet,
	updateColOrdSet exec.ColumnOrdinalSet,
	returnColOrdSet exec.ColumnOrdinalSet,
	checks exec.CheckOrdinalSet,
) (exec.Node, error) {
	// Derive table and column descriptors.
	rowsNeeded := !returnColOrdSet.Empty()
	tabDesc := table.(*optTable).desc
	insertColDescs := makeColDescList(table, insertColOrdSet)
	fetchColDescs := makeColDescList(table, fetchColOrdSet)
	updateColDescs := makeColDescList(table, updateColOrdSet)

	// Construct the check helper if there are any check constraints.
	checkHelper := sqlbase.NewInputCheckHelper(checks, tabDesc)

	// Determine the foreign key tables involved in the upsert.
	fkTables, err := ef.makeFkMetadata(tabDesc, row.CheckUpdates, checkHelper)
	if err != nil {
		return nil, err
	}

	// Create the table inserter, which does the bulk of the insert-related work.
	ri, err := row.MakeInserter(ef.planner.txn, tabDesc, fkTables, insertColDescs,
		row.CheckFKs, &ef.planner.alloc)
	if err != nil {
		return nil, err
	}

	// Create the table updater, which does the bulk of the update-related work.
	// In the HP, the updater derives the columns that need to be fetched. By
	// contrast, the CBO will have already determined the set of fetch and update
	// columns, and passes those sets into the updater (which will basically be a
	// no-op).
	ru, err := row.MakeUpdater(
		ef.planner.txn,
		tabDesc,
		fkTables,
		updateColDescs,
		fetchColDescs,
		row.UpdaterDefault,
		ef.planner.EvalContext(),
		&ef.planner.alloc,
	)
	if err != nil {
		return nil, err
	}

	// Truncate any FetchCols added by MakeUpdater. The optimizer has already
	// computed a correct set that can sometimes be smaller.
	ru.FetchCols = ru.FetchCols[:len(fetchColDescs)]

	// Determine the relational type of the generated upsert node.
	// If rows are not needed, no columns are returned.
	var returnCols sqlbase.ResultColumns
	var returnColDescs []sqlbase.ColumnDescriptor
	var tabColIdxToRetIdx []int
	if rowsNeeded {
		returnColDescs = makeColDescList(table, returnColOrdSet)

		// Only return the columns that are part of the table descriptor.
		// This is important when columns are added and being back-filled
		// as part of the same transaction when the delete runs.
		// In such cases, the newly added columns shouldn't be returned.
		// See regression logic tests for #29494.
		if len(tabDesc.Columns) < len(returnColDescs) {
			returnColDescs = returnColDescs[:len(tabDesc.Columns)]
		}

		returnCols = sqlbase.ResultColumnsFromColDescs(returnColDescs)

		// Update the tabColIdxToRetIdx for the mutation. Upsert returns
		// non-mutation columns specified, in the same order they are defined
		// in the table.
		tabColIdxToRetIdx = mutationRowIdxToReturnIdx(tabDesc.Columns, returnColDescs)
	}

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
		source:  input.(planNode),
		columns: returnCols,
		run: upsertRun{
			checkHelper: checkHelper,
			insertCols:  ri.InsertCols,
			iVarContainerForComputedCols: sqlbase.RowIndexedVarContainer{
				Cols:    tabDesc.Columns,
				Mapping: ri.InsertColIDtoRowIndex,
			},
			tw: &optTableUpserter{
				tableUpserterBase: tableUpserterBase{
					ri:          ri,
					alloc:       &ef.planner.alloc,
					collectRows: rowsNeeded,
				},
				canaryOrdinal:     int(canaryCol),
				fkTables:          fkTables,
				fetchCols:         fetchColDescs,
				updateCols:        updateColDescs,
				returnCols:        returnColDescs,
				ru:                ru,
				tabColIdxToRetIdx: tabColIdxToRetIdx,
			},
		},
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

// colsRequiredForDelete returns all the columns required to perform a delete
// of a row on the table. This will include the returnColDescs columns that
// are referenced in the RETURNING clause of the delete mutation. This
// is different from the fetch columns of the delete mutation as the
// fetch columns includes more columns. Specifically, the fetch columns also
// include columns that are not part of index keys or the RETURNING columns
// (columns, for example, referenced in the WHERE clause).
func colsRequiredForDelete(
	table cat.Table, tableColDescs, returnColDescs []sqlbase.ColumnDescriptor,
) []sqlbase.ColumnDescriptor {
	// Find all the columns that are part of the rows returned by the delete.
	deleteDescs := make([]sqlbase.ColumnDescriptor, 0, len(tableColDescs))
	var deleteCols util.FastIntSet
	for i := 0; i < table.IndexCount(); i++ {
		index := table.Index(i)
		for j := 0; j < index.KeyColumnCount(); j++ {
			col := *index.Column(j).Column.(*sqlbase.ColumnDescriptor)
			if deleteCols.Contains(int(col.ID)) {
				continue
			}

			deleteDescs = append(deleteDescs, col)
			deleteCols.Add(int(col.ID))
		}
	}

	// Add columns specified in the RETURNING clause.
	for _, col := range returnColDescs {
		if deleteCols.Contains(int(col.ID)) {
			continue
		}

		deleteDescs = append(deleteDescs, col)
		deleteCols.Add(int(col.ID))
	}

	// The order of the columns processed by the delete must be in the order they
	// are present in the table.
	tabDescs := make([]sqlbase.ColumnDescriptor, 0, len(deleteDescs))
	for i := 0; i < len(tableColDescs); i++ {
		col := tableColDescs[i]
		if deleteCols.Contains(int(col.ID)) {
			tabDescs = append(tabDescs, col)
		}
	}

	return tabDescs
}

func (ef *execFactory) ConstructDelete(
	input exec.Node,
	table cat.Table,
	fetchColOrdSet exec.ColumnOrdinalSet,
	returnColOrdSet exec.ColumnOrdinalSet,
	skipFKChecks bool,
) (exec.Node, error) {
	// Derive table and column descriptors.
	rowsNeeded := !returnColOrdSet.Empty()
	tabDesc := table.(*optTable).desc
	fetchColDescs := makeColDescList(table, fetchColOrdSet)

	// Determine the foreign key tables involved in the update.
	fkTables, err := ef.makeFkMetadata(tabDesc, row.CheckDeletes, nil /* checkHelper */)
	if err != nil {
		return nil, err
	}

	fastPathInterleaved := canDeleteFastInterleaved(tabDesc, fkTables)
	if fastPathNode, ok := maybeCreateDeleteFastNode(
		context.TODO(), input.(planNode), tabDesc, fastPathInterleaved, rowsNeeded); ok {
		return fastPathNode, nil
	}

	checkFKs := row.CheckFKs
	if skipFKChecks {
		checkFKs = row.SkipFKs
	}
	// Create the table deleter, which does the bulk of the work. In the HP,
	// the deleter derives the columns that need to be fetched. By contrast, the
	// CBO will have already determined the set of fetch columns, and passes
	// those sets into the deleter (which will basically be a no-op).
	rd, err := row.MakeDeleter(
		ef.planner.txn,
		tabDesc,
		fkTables,
		fetchColDescs,
		checkFKs,
		ef.planner.EvalContext(),
		&ef.planner.alloc,
	)
	if err != nil {
		return nil, err
	}

	// Truncate any FetchCols added by MakeUpdater. The optimizer has already
	// computed a correct set that can sometimes be smaller.
	rd.FetchCols = rd.FetchCols[:len(fetchColDescs)]

	// Determine the relational type of the generated delete node.
	// If rows are not needed, no columns are returned.
	var returnCols sqlbase.ResultColumns
	var rowIdxToRetIdx []int
	if rowsNeeded {
		returnColDescs := makeColDescList(table, returnColOrdSet)

		// Only return the columns that are part of the table descriptor.
		// This is important when columns are added and being back-filled
		// as part of the same transaction when the delete runs.
		// In such cases, the newly added columns shouldn't be returned.
		// See regression logic tests for #29494.
		if len(tabDesc.Columns) < len(returnColDescs) {
			returnColDescs = returnColDescs[:len(tabDesc.Columns)]
		}

		// Delete returns the non-mutation columns specified, in the same
		// order they are defined in the table.
		returnCols = sqlbase.ResultColumnsFromColDescs(returnColDescs)

		// Find all the columns that the deleteNode receives. The returning
		// columns of the mutation are a subset of this column set.
		requiredDeleteColumns := colsRequiredForDelete(table, tabDesc.Columns, returnColDescs)

		// Update the rowIdxToReturnIdx for the mutation.
		rowIdxToRetIdx = mutationRowIdxToReturnIdx(requiredDeleteColumns, returnColDescs)
	}

	// Now make a delete node. We use a pool.
	del := deleteNodePool.Get().(*deleteNode)
	*del = deleteNode{
		source:  input.(planNode),
		columns: returnCols,
		run: deleteRun{
			td:             tableDeleter{rd: rd, alloc: &ef.planner.alloc},
			rowsNeeded:     rowsNeeded,
			rowIdxToRetIdx: rowIdxToRetIdx,
		},
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
	table cat.Table, needed exec.ColumnOrdinalSet, indexConstraint *constraint.Constraint,
) (exec.Node, error) {
	tabDesc := table.(*optTable).desc
	indexDesc := &tabDesc.PrimaryIndex

	// Setting the "forDelete" flag includes all column families in case where a
	// single record is deleted.
	spans, err := spansFromConstraint(
		tabDesc,
		indexDesc,
		indexConstraint,
		needed,
		true, /* forDelete */
	)
	if err != nil {
		return nil, err
	}

	return &deleteRangeNode{
		interleavedFastPath: false,
		spans:               spans,
		desc:                tabDesc,
	}, nil
}

func (ef *execFactory) ConstructCreateTable(
	input exec.Node, schema cat.Schema, ct *tree.CreateTable,
) (exec.Node, error) {
	nd := &createTableNode{n: ct, dbDesc: schema.(*optSchema).desc}
	if input != nil {
		nd.sourcePlan = input.(planNode)
	}
	return nd, nil
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
		force:          ef.planner.SessionData().ForceSplitAt,
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
func (rb *renderBuilder) init(n exec.Node, reqOrdering exec.OutputOrdering, cap int) {
	src := asDataSource(n)
	rb.r = &renderNode{
		source:     src,
		sourceInfo: sqlbase.MultiSourceInfo{src.info},
		render:     make([]tree.TypedExpr, 0, cap),
		columns:    make([]sqlbase.ResultColumn, 0, cap),
	}
	rb.r.ivarHelper = tree.MakeIndexedVarHelper(rb.r, len(src.info.SourceColumns))
	rb.r.props.ordering = sqlbase.ColumnOrdering(reqOrdering)

	// If there's a spool, pull it up.
	if spool, ok := rb.r.source.plan.(*spoolNode); ok {
		rb.r.source.plan = spool.source
		spool.source = rb.r
		rb.res = spool
	} else {
		rb.res = rb.r
	}
}

// addExpr adds a new render expression with the given name.
func (rb *renderBuilder) addExpr(expr tree.TypedExpr, colName string) {
	rb.r.render = append(rb.r.render, expr)
	rb.r.columns = append(rb.r.columns, sqlbase.ResultColumn{Name: colName, Typ: expr.ResolvedType()})
}

// makeColDescList returns a list of table column descriptors. Columns are
// included if their ordinal position in the table schema is in the cols set.
func makeColDescList(table cat.Table, cols exec.ColumnOrdinalSet) []sqlbase.ColumnDescriptor {
	colDescs := make([]sqlbase.ColumnDescriptor, 0, cols.Len())
	for i, n := 0, table.DeletableColumnCount(); i < n; i++ {
		if !cols.Contains(i) {
			continue
		}
		colDescs = append(colDescs, *table.Column(i).(*sqlbase.ColumnDescriptor))
	}
	return colDescs
}

// makeScanColumnsConfig builds a scanColumnsConfig struct by constructing a
// list of descriptor IDs for columns in the given cols set. Columns are
// identified by their ordinal position in the table schema.
func makeScanColumnsConfig(table cat.Table, cols exec.ColumnOrdinalSet) scanColumnsConfig {
	// Set visibility=publicAndNonPublicColumns, since all columns in the "cols"
	// set should be projected, regardless of whether they're public or non-
	// public. The caller decides which columns to include (or not include). Note
	// that when wantedColumns is non-empty, the visibility flag will never
	// trigger the addition of more columns.
	colCfg := scanColumnsConfig{
		wantedColumns: make([]tree.ColumnID, 0, cols.Len()),
		visibility:    publicAndNonPublicColumns,
	}
	for c, ok := cols.Next(0); ok; c, ok = cols.Next(c + 1) {
		desc := table.Column(c).(*sqlbase.ColumnDescriptor)
		colCfg.wantedColumns = append(colCfg.wantedColumns, tree.ColumnID(desc.ID))
	}
	return colCfg
}
