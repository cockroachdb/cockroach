// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
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
	table opt.Table,
	index opt.Index,
	cols exec.ColumnOrdinalSet,
	indexConstraint *constraint.Constraint,
	hardLimit int64,
	reverse bool,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	tabDesc := table.(*optTable).desc
	indexDesc := index.(*optIndex).desc
	// Create a scanNode.
	scan := ef.planner.Scan()
	colCfg := scanColumnsConfig{
		wantedColumns: make([]tree.ColumnID, 0, cols.Len()),
	}
	for c, ok := cols.Next(0); ok; c, ok = cols.Next(c + 1) {
		colCfg.wantedColumns = append(colCfg.wantedColumns, tree.ColumnID(tabDesc.Columns[c].ID))
	}

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
	scan.run.isSecondaryIndex = (indexDesc != &tabDesc.PrimaryIndex)
	scan.hardLimit = hardLimit
	scan.reverse = reverse
	var err error
	scan.spans, err = spansFromConstraint(tabDesc, indexDesc, indexConstraint, cols)
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
func (ef *execFactory) ConstructVirtualScan(table opt.Table) (exec.Node, error) {
	tn := table.Name()
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
func (ef *execFactory) ConstructFilter(n exec.Node, filter tree.TypedExpr) (exec.Node, error) {
	// Push the filter into the scanNode. We cannot do this if the scanNode has a
	// limit (it would make the limit apply AFTER the filter).
	if s, ok := n.(*scanNode); ok && s.filter == nil && s.hardLimit == 0 {
		s.filter = s.filterVars.Rebind(filter, true /* alsoReset */, false /* normalizeToNonNil */)
		return s, nil
	}
	// Create a filterNode.
	src := asDataSource(n)
	f := &filterNode{
		source: src,
	}
	f.ivarHelper = tree.MakeIndexedVarHelper(f, len(src.info.SourceColumns))
	f.filter = f.ivarHelper.Rebind(filter, true /* alsoReset */, false /* normalizeToNonNil */)
	return f, nil
}

// ConstructSimpleProject is part of the exec.Factory interface.
func (ef *execFactory) ConstructSimpleProject(
	n exec.Node, cols []exec.ColumnOrdinal, colNames []string,
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
		return r, nil
	}
	var inputCols sqlbase.ResultColumns
	if colNames == nil {
		// We will need the names of the input columns.
		inputCols = planColumns(n.(planNode))
	}
	src := asDataSource(n)
	r := &renderNode{
		source:     src,
		sourceInfo: sqlbase.MultiSourceInfo{src.info},
		render:     make([]tree.TypedExpr, len(cols)),
		columns:    make([]sqlbase.ResultColumn, len(cols)),
	}
	r.ivarHelper = tree.MakeIndexedVarHelper(r, len(src.info.SourceColumns))
	for i, col := range cols {
		v := r.ivarHelper.IndexedVar(int(col))
		r.render[i] = v
		if colNames == nil {
			r.columns[i].Name = inputCols[col].Name
		} else {
			r.columns[i].Name = colNames[i]
		}
		r.columns[i].Typ = v.ResolvedType()
	}
	return r, nil
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
	n exec.Node, exprs tree.TypedExprs, colNames []string,
) (exec.Node, error) {
	src := asDataSource(n)
	r := &renderNode{
		source:     src,
		sourceInfo: sqlbase.MultiSourceInfo{src.info},
		render:     make([]tree.TypedExpr, len(exprs)),
		columns:    make([]sqlbase.ResultColumn, len(exprs)),
	}
	r.ivarHelper = tree.MakeIndexedVarHelper(r, len(src.info.SourceColumns))
	for i, expr := range exprs {
		expr = r.ivarHelper.Rebind(expr, false /* alsoReset */, true /* normalizeToNonNil */)
		r.render[i] = expr
		r.columns[i] = sqlbase.ResultColumn{Name: colNames[i], Typ: expr.ResolvedType()}
	}
	return r, nil
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
	joinType sqlbase.JoinType, left, right exec.Node, onCond tree.TypedExpr,
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
	onCond = pred.iVarHelper.Rebind(
		onCond, false /* alsoReset */, false, /* normalizeToNonNil */
	)
	// Try to harvest equality columns from the ON expression.
	onAndExprs := splitAndExpr(p.EvalContext(), onCond, nil /* exprs */)
	for _, e := range onAndExprs {
		if e != tree.DBoolTrue && !pred.tryAddEqualityFilter(e, leftSrc.info, rightSrc.info) {
			pred.onCond = mergeConj(pred.onCond, e)
		}
	}

	return p.makeJoinNode(leftSrc, rightSrc, pred), nil
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
	orderedGroupCols exec.ColumnOrdinalSet,
	aggregations []exec.AggInfo,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	n := &groupNode{
		plan:             input.(planNode),
		funcs:            make([]*aggregateFuncHolder, 0, len(groupCols)+len(aggregations)),
		columns:          make(sqlbase.ResultColumns, 0, len(groupCols)+len(aggregations)),
		groupCols:        make([]int, len(groupCols)),
		orderedGroupCols: make([]int, 0, orderedGroupCols.Len()),
		isScalar:         false,
		props: physicalProps{
			ordering: sqlbase.ColumnOrdering(reqOrdering),
		},
	}
	inputCols := planColumns(n.plan)
	for i := range groupCols {
		col := int(groupCols[i])
		n.groupCols[i] = col
		if orderedGroupCols.Contains(col) {
			n.orderedGroupCols = append(n.orderedGroupCols, col)
		}

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
				return builtin.AggregateFunc([]types.T{}, evalCtx, arguments)
			}

		case 1:
			renderIdx = int(agg.ArgCols[0])
			aggFn = func(evalCtx *tree.EvalContext, arguments tree.Datums) tree.AggregateFunc {
				return builtin.AggregateFunc([]types.T{inputCols[renderIdx].Typ}, evalCtx, arguments)
			}

		default:
			return pgerror.UnimplementedWithIssueError(28417,
				"aggregate functions with multiple non-constant expressions are not supported",
			)
		}

		f := n.newAggregateFuncHolder(
			agg.FuncName,
			agg.ResultType,
			renderIdx,
			aggFn,
			nil, /* arguments */
			ef.planner.EvalContext().Mon.MakeBoundAccount(),
		)
		if agg.Distinct {
			f.setDistinct()
		}
		n.funcs = append(n.funcs, f)
		n.columns = append(n.columns, sqlbase.ResultColumn{
			Name: fmt.Sprintf("agg%d", i),
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
	input exec.Node, table opt.Table, cols exec.ColumnOrdinalSet, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	tabDesc := table.(*optTable).desc
	colCfg := scanColumnsConfig{
		wantedColumns: make([]tree.ColumnID, 0, cols.Len()),
	}

	colDescs := make([]sqlbase.ColumnDescriptor, 0, cols.Len())
	for c, ok := cols.Next(0); ok; c, ok = cols.Next(c + 1) {
		desc := tabDesc.Columns[c]
		colDescs = append(colDescs, desc)
		colCfg.wantedColumns = append(colCfg.wantedColumns, tree.ColumnID(desc.ID))
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
	tableScan.run.isSecondaryIndex = false
	tableScan.disableBatchLimit()

	primaryKeyColumns, colIDtoRowIndex := processIndexJoinColumns(tableScan, scan)
	primaryKeyPrefix := roachpb.Key(sqlbase.MakeIndexKeyPrefix(tabDesc, tableScan.index.ID))

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
	table opt.Table,
	index opt.Index,
	keyCols []exec.ColumnOrdinal,
	lookupCols exec.ColumnOrdinalSet,
	onCond tree.TypedExpr,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	tabDesc := table.(*optTable).desc
	indexDesc := index.(*optIndex).desc
	colCfg := scanColumnsConfig{
		wantedColumns: make([]tree.ColumnID, 0, lookupCols.Len()),
	}

	for c, ok := lookupCols.Next(0); ok; c, ok = lookupCols.Next(c + 1) {
		colCfg.wantedColumns = append(colCfg.wantedColumns, tree.ColumnID(tabDesc.Columns[c].ID))
	}

	tableScan := ef.planner.Scan()

	if err := tableScan.initTable(context.TODO(), ef.planner, tabDesc, nil, colCfg); err != nil {
		return nil, err
	}

	tableScan.index = indexDesc
	tableScan.run.isSecondaryIndex = (indexDesc != &tabDesc.PrimaryIndex)

	n := &lookupJoinNode{
		input:    input.(planNode),
		table:    tableScan,
		joinType: joinType,
		props: physicalProps{
			ordering: sqlbase.ColumnOrdering(reqOrdering),
		},
	}
	if onCond != nil && onCond != tree.DBoolTrue {
		n.onCond = onCond
	}
	n.keyCols = make([]int, len(keyCols))
	for i, c := range keyCols {
		n.keyCols[i] = int(c)
	}
	inputCols := planColumns(input.(planNode))
	scanCols := planColumns(tableScan)
	n.columns = make(sqlbase.ResultColumns, 0, len(inputCols)+len(scanCols))
	n.columns = append(n.columns, inputCols...)
	n.columns = append(n.columns, scanCols...)
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
	return &limitNode{
		plan:       plan,
		countExpr:  limit,
		offsetExpr: offset,
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

// ConstructPlan is part of the exec.Factory interface.
func (ef *execFactory) ConstructPlan(
	root exec.Node, subqueries []exec.Subquery,
) (exec.Plan, error) {
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
			default:
				return nil, errors.Errorf("invalid SubqueryMode %d", in.Mode)
			}
			out.expanded = true
			out.plan = in.Root.(planNode)
		}
	}
	return res, nil
}

// ConstructExplain is part of the exec.Factory interface.
func (ef *execFactory) ConstructExplain(
	options *tree.ExplainOptions, plan exec.Plan,
) (exec.Node, error) {
	p := plan.(*planTop)

	analyzeSet := options.Flags.Contains(tree.ExplainFlagAnalyze)

	switch options.Mode {
	case tree.ExplainDistSQL:
		if len(p.subqueryPlans) > 0 {
			return nil, fmt.Errorf("subqueries not supported yet")
		}
		return &explainDistSQLNode{
			plan:    p.plan,
			analyze: analyzeSet,
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
