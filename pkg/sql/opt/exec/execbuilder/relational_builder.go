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

package execbuilder

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

type execPlan struct {
	root exec.Node

	// outputCols is a map from opt.ColumnID to exec.ColumnOrdinal. It maps
	// columns in the output set of a relational expression to indices in the
	// result columns of the exec.Node.
	//
	// The reason we need to keep track of this (instead of using just the
	// relational properties) is that the relational properties don't force a
	// single "schema": any ordering of the output columns is possible. We choose
	// the schema that is most convenient: for scans, we use the table's column
	// ordering. Consider:
	//   SELECT a, b FROM t WHERE a = b
	// and the following two cases:
	//   1. The table is defined as (k INT PRIMARY KEY, a INT, b INT). The scan will
	//      return (k, a, b).
	//   2. The table is defined as (k INT PRIMARY KEY, b INT, a INT). The scan will
	//      return (k, b, a).
	// In these two cases, the relational properties are effectively the same.
	//
	// An alternative to this would be to always use a "canonical" schema, for
	// example the output columns in increasing index order. However, this would
	// require a lot of otherwise unnecessary projections.
	//
	// The number of entries set in the map is always the same with the number of
	// columns emitted by Node.
	//
	// Note: conceptually, this could be a ColList; however, the map is more
	// convenient when converting VariableOps to IndexedVars.
	outputCols opt.ColMap
}

// makeBuildScalarCtx returns a buildScalarCtx that can be used with expressions
// that refer the output columns of this plan.
func (ep *execPlan) makeBuildScalarCtx() buildScalarCtx {
	return buildScalarCtx{
		ivh:     tree.MakeIndexedVarHelper(nil /* container */, ep.outputCols.Len()),
		ivarMap: ep.outputCols,
	}
}

// getColumnOrdinal takes a column that is known to be produced by the execPlan
// and returns the ordinal index of that column in the result columns of the
// node.
func (ep *execPlan) getColumnOrdinal(col opt.ColumnID) exec.ColumnOrdinal {
	ord, ok := ep.outputCols.Get(int(col))
	if !ok {
		panic(fmt.Sprintf("column %d not in input", col))
	}
	return exec.ColumnOrdinal(ord)
}

func (ep *execPlan) getColumnOrdinalSet(cols opt.ColSet) exec.ColumnOrdinalSet {
	var res exec.ColumnOrdinalSet
	cols.ForEach(func(colID int) {
		res.Add(int(ep.getColumnOrdinal(opt.ColumnID(colID))))
	})
	return res
}

func (b *Builder) buildRelational(e memo.RelExpr) (execPlan, error) {
	var ep execPlan
	var err error
	switch t := e.(type) {
	case *memo.ValuesExpr:
		ep, err = b.buildValues(t)

	case *memo.ScanExpr:
		ep, err = b.buildScan(t)

	case *memo.VirtualScanExpr:
		ep, err = b.buildVirtualScan(t)

	case *memo.SelectExpr:
		ep, err = b.buildSelect(t)

	case *memo.ProjectExpr:
		ep, err = b.buildProject(t)

	case *memo.GroupByExpr, *memo.ScalarGroupByExpr:
		ep, err = b.buildGroupBy(e)

	case *memo.DistinctOnExpr:
		ep, err = b.buildDistinct(t)

	case *memo.LimitExpr, *memo.OffsetExpr:
		ep, err = b.buildLimitOffset(e)

	case *memo.SortExpr:
		ep, err = b.buildSort(t)

	case *memo.IndexJoinExpr:
		ep, err = b.buildIndexJoin(t)

	case *memo.LookupJoinExpr:
		ep, err = b.buildLookupJoin(t)

	case *memo.ExplainExpr:
		ep, err = b.buildExplain(t)

	case *memo.ShowTraceForSessionExpr:
		ep, err = b.buildShowTrace(t)

	case *memo.RowNumberExpr:
		ep, err = b.buildRowNumber(t)

	case *memo.MergeJoinExpr:
		ep, err = b.buildMergeJoin(t)

	case *memo.ZipExpr:
		ep, err = b.buildZip(t)

	default:
		if opt.IsSetOp(e) {
			ep, err = b.buildSetOp(e)
			break
		}
		if opt.IsJoinNonApplyOp(e) {
			ep, err = b.buildHashJoin(e)
			break
		}
		if opt.IsJoinApplyOp(e) {
			if e.Child(1).Op() == opt.ZipOp {
				ep, err = b.buildProjectSet(e)
				break
			}
			return execPlan{}, b.decorrelationError()
		}
		return execPlan{}, errors.Errorf("unsupported relational op %s", e.Op())
	}
	if err != nil {
		return execPlan{}, err
	}
	if p := e.Physical().Presentation; !p.Any() {
		ep, err = b.applyPresentation(ep, p)
	}
	return ep, err
}

func (b *Builder) buildValues(values *memo.ValuesExpr) (execPlan, error) {
	numCols := len(values.Cols)

	rows := make([][]tree.TypedExpr, len(values.Rows))
	rowBuf := make([]tree.TypedExpr, len(rows)*numCols)
	scalarCtx := buildScalarCtx{}
	for i := range rows {
		tup := values.Rows[i].(*memo.TupleExpr)
		if len(tup.Elems) != numCols {
			return execPlan{}, fmt.Errorf("inconsistent row length %d vs %d", len(tup.Elems), numCols)
		}
		// Chop off prefix of rowBuf and limit its capacity.
		rows[i] = rowBuf[:numCols:numCols]
		rowBuf = rowBuf[numCols:]
		var err error
		for j := 0; j < numCols; j++ {
			rows[i][j], err = b.buildScalar(&scalarCtx, tup.Elems[j])
			if err != nil {
				return execPlan{}, err
			}
		}
	}
	return b.constructValues(rows, values.Cols)
}

func (b *Builder) constructValues(rows [][]tree.TypedExpr, cols opt.ColList) (execPlan, error) {
	md := b.mem.Metadata()
	resultCols := make(sqlbase.ResultColumns, len(cols))
	for i, col := range cols {
		resultCols[i].Name = md.ColumnLabel(col)
		resultCols[i].Typ = md.ColumnType(col)
	}
	node, err := b.factory.ConstructValues(rows, resultCols)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{root: node}
	for i, col := range cols {
		ep.outputCols.Set(int(col), i)
	}

	return ep, nil
}

// getColumns returns the set of column ordinals in the table for the set of
// column IDs, along with a mapping from the column IDs to output ordinals
// (starting with outputOrdinalStart).
func (b *Builder) getColumns(
	cols opt.ColSet, tableID opt.TableID,
) (exec.ColumnOrdinalSet, opt.ColMap) {
	needed := exec.ColumnOrdinalSet{}
	output := opt.ColMap{}

	columnCount := b.mem.Metadata().Table(tableID).ColumnCount()
	n := 0
	for i := 0; i < columnCount; i++ {
		colID := tableID.ColumnID(i)
		if cols.Contains(int(colID)) {
			needed.Add(i)
			output.Set(int(colID), n)
			n++
		}
	}

	return needed, output
}

func (b *Builder) makeSQLOrderingFromChoice(
	plan execPlan, ordering *props.OrderingChoice,
) sqlbase.ColumnOrdering {
	if ordering.Any() {
		return nil
	}

	colOrder := make(sqlbase.ColumnOrdering, len(ordering.Columns))
	for i := range colOrder {
		colOrder[i].ColIdx = int(plan.getColumnOrdinal(ordering.Columns[i].AnyID()))
		if ordering.Columns[i].Descending {
			colOrder[i].Direction = encoding.Descending
		} else {
			colOrder[i].Direction = encoding.Ascending
		}
	}

	return colOrder
}

func (b *Builder) makeSQLOrdering(plan execPlan, ordering opt.Ordering) sqlbase.ColumnOrdering {
	colOrder := make(sqlbase.ColumnOrdering, len(ordering))
	for i := range ordering {
		colOrder[i].ColIdx = int(plan.getColumnOrdinal(ordering[i].ID()))
		if ordering[i].Descending() {
			colOrder[i].Direction = encoding.Descending
		} else {
			colOrder[i].Direction = encoding.Ascending
		}
	}

	return colOrder
}

func (b *Builder) buildScan(scan *memo.ScanExpr) (execPlan, error) {
	md := b.mem.Metadata()
	tab := md.Table(scan.Table)

	// Check if we tried to force a specific index but there was no Scan with that
	// index in the memo.
	if scan.Flags.ForceIndex && scan.Flags.Index != scan.Index {
		idx := tab.Index(scan.Flags.Index)
		var err error
		if idx.IsInverted() {
			err = fmt.Errorf("index \"%s\" is inverted and cannot be used for this query", idx.IdxName())
		} else {
			// This should never happen.
			err = fmt.Errorf("index \"%s\" cannot be used for this query", idx.IdxName())
		}
		return execPlan{}, err
	}

	needed, output := b.getColumns(scan.Cols, scan.Table)
	res := execPlan{outputCols: output}

	reqOrdering := b.makeSQLOrderingFromChoice(res, &scan.Physical().Ordering)

	_, reverse := scan.CanProvideOrdering(md, &scan.Physical().Ordering)

	root, err := b.factory.ConstructScan(
		tab,
		tab.Index(scan.Index),
		needed,
		scan.Constraint,
		scan.HardLimit.RowCount(),
		// def.HardLimit.Reverse() was taken into account by CanProvideOrdering.
		reverse,
		exec.OutputOrdering(reqOrdering),
	)
	if err != nil {
		return execPlan{}, err
	}
	res.root = root
	return res, nil
}

func (b *Builder) buildVirtualScan(scan *memo.VirtualScanExpr) (execPlan, error) {
	md := b.mem.Metadata()
	tab := md.Table(scan.Table)

	_, output := b.getColumns(scan.Cols, scan.Table)
	res := execPlan{outputCols: output}

	root, err := b.factory.ConstructVirtualScan(tab)
	if err != nil {
		return execPlan{}, err
	}
	res.root = root
	return res, nil
}

func (b *Builder) buildSelect(sel *memo.SelectExpr) (execPlan, error) {
	input, err := b.buildRelational(sel.Input)
	if err != nil {
		return execPlan{}, err
	}
	ctx := input.makeBuildScalarCtx()
	filter, err := b.buildScalar(&ctx, &sel.Filters)
	if err != nil {
		return execPlan{}, err
	}
	node, err := b.factory.ConstructFilter(input.root, filter)
	if err != nil {
		return execPlan{}, err
	}
	return execPlan{
		root: node,
		// A filtering node does not modify the schema.
		outputCols: input.outputCols,
	}, nil
}

// applySimpleProject adds a simple projection on top of an existing plan.
func (b *Builder) applySimpleProject(input execPlan, cols opt.ColSet) (execPlan, error) {
	// We have only pass-through columns.
	colList := make([]exec.ColumnOrdinal, 0, cols.Len())
	var outputCols opt.ColMap
	cols.ForEach(func(i int) {
		outputCols.Set(i, len(colList))
		colList = append(colList, input.getColumnOrdinal(opt.ColumnID(i)))
	})
	node, err := b.factory.ConstructSimpleProject(input.root, colList, nil /* colNames */)
	if err != nil {
		return execPlan{}, err
	}
	return execPlan{root: node, outputCols: outputCols}, nil
}

func (b *Builder) buildProject(prj *memo.ProjectExpr) (execPlan, error) {
	md := b.mem.Metadata()
	input, err := b.buildRelational(prj.Input)
	if err != nil {
		return execPlan{}, err
	}
	projections := prj.Projections
	if len(projections) == 0 {
		// We have only pass-through columns.
		return b.applySimpleProject(input, prj.Passthrough)
	}

	var outputCols opt.ColMap
	exprs := make(tree.TypedExprs, 0, len(projections)+prj.Passthrough.Len())
	colNames := make([]string, 0, len(exprs))
	ctx := input.makeBuildScalarCtx()
	for i := range projections {
		item := &projections[i]
		expr, err := b.buildScalar(&ctx, item.Element)
		if err != nil {
			return execPlan{}, err
		}
		outputCols.Set(int(item.Col), i)
		exprs = append(exprs, expr)
		colNames = append(colNames, md.ColumnLabel(item.Col))
	}
	prj.Passthrough.ForEach(func(i int) {
		colID := opt.ColumnID(i)
		outputCols.Set(i, len(exprs))
		exprs = append(exprs, b.indexedVar(&ctx, md, colID))
		colNames = append(colNames, md.ColumnLabel(colID))
	})
	node, err := b.factory.ConstructRender(input.root, exprs, colNames)
	if err != nil {
		return execPlan{}, err
	}
	return execPlan{root: node, outputCols: outputCols}, nil
}

func (b *Builder) buildHashJoin(join memo.RelExpr) (execPlan, error) {
	joinType := joinOpToJoinType(join.Op())
	left, right, onExpr, outputCols, err := b.initJoinBuild(
		join.Child(0).(memo.RelExpr),
		join.Child(1).(memo.RelExpr),
		join.Child(2).(opt.ScalarExpr),
		joinType,
	)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{outputCols: outputCols}
	ep.root, err = b.factory.ConstructHashJoin(joinType, left.root, right.root, onExpr)
	if err != nil {
		return execPlan{}, err
	}
	return ep, nil
}

func (b *Builder) buildMergeJoin(join *memo.MergeJoinExpr) (execPlan, error) {
	joinType := joinOpToJoinType(join.JoinType)

	left, right, onExpr, outputCols, err := b.initJoinBuild(
		join.Left, join.Right, &join.On, joinType,
	)
	if err != nil {
		return execPlan{}, err
	}
	leftOrd := b.makeSQLOrdering(left, join.LeftEq)
	rightOrd := b.makeSQLOrdering(right, join.RightEq)
	ep := execPlan{outputCols: outputCols}
	reqOrd := b.makeSQLOrderingFromChoice(ep, &join.Physical().Ordering)
	ep.root, err = b.factory.ConstructMergeJoin(
		joinType, left.root, right.root, onExpr, leftOrd, rightOrd, exec.OutputOrdering(reqOrd),
	)
	if err != nil {
		return execPlan{}, err
	}
	return ep, nil
}

// initJoinBuild builds the inputs to the join as well as the ON expression.
func (b *Builder) initJoinBuild(
	leftChild memo.RelExpr, rightChild memo.RelExpr, onCond opt.ScalarExpr, joinType sqlbase.JoinType,
) (leftPlan, rightPlan execPlan, onExpr tree.TypedExpr, outputCols opt.ColMap, _ error) {
	leftPlan, err := b.buildRelational(leftChild)
	if err != nil {
		return execPlan{}, execPlan{}, nil, opt.ColMap{}, err
	}
	rightPlan, err = b.buildRelational(rightChild)
	if err != nil {
		return execPlan{}, execPlan{}, nil, opt.ColMap{}, err
	}

	allCols := joinOutputMap(leftPlan.outputCols, rightPlan.outputCols)

	ctx := buildScalarCtx{
		ivh:     tree.MakeIndexedVarHelper(nil /* container */, allCols.Len()),
		ivarMap: allCols,
	}

	if onCond.Op() != opt.UnknownOp {
		onExpr, err = b.buildScalar(&ctx, onCond)
		if err != nil {
			return execPlan{}, execPlan{}, nil, opt.ColMap{}, err
		}
	}

	if joinType == sqlbase.LeftSemiJoin || joinType == sqlbase.LeftAntiJoin {
		// For semi and anti join, only the left columns are output.
		return leftPlan, rightPlan, onExpr, leftPlan.outputCols, nil
	}
	return leftPlan, rightPlan, onExpr, allCols, nil
}

// joinOutputMap determines the outputCols map for a (non-semi/anti) join, given
// the outputCols maps for its inputs.
func joinOutputMap(left, right opt.ColMap) opt.ColMap {
	numLeftCols := left.Len()
	res := left.Copy()
	right.ForEach(func(colIdx, rightIdx int) {
		res.Set(colIdx, rightIdx+numLeftCols)
	})
	return res
}

func joinOpToJoinType(op opt.Operator) sqlbase.JoinType {
	switch op {
	case opt.InnerJoinOp:
		return sqlbase.InnerJoin

	case opt.LeftJoinOp:
		return sqlbase.LeftOuterJoin

	case opt.RightJoinOp:
		return sqlbase.RightOuterJoin

	case opt.FullJoinOp:
		return sqlbase.FullOuterJoin

	case opt.SemiJoinOp:
		return sqlbase.LeftSemiJoin

	case opt.AntiJoinOp:
		return sqlbase.LeftAntiJoin

	default:
		panic(fmt.Sprintf("not a join op %s", op))
	}
}

func (b *Builder) buildGroupBy(groupBy memo.RelExpr) (execPlan, error) {
	input, err := b.buildGroupByInput(groupBy)
	if err != nil {
		return execPlan{}, err
	}

	var ep execPlan
	groupingCols := groupBy.Private().(*memo.GroupingPrivate).GroupingCols
	groupingColIdx := make([]exec.ColumnOrdinal, 0, groupingCols.Len())
	for i, ok := groupingCols.Next(0); ok; i, ok = groupingCols.Next(i + 1) {
		ep.outputCols.Set(i, len(groupingColIdx))
		groupingColIdx = append(groupingColIdx, input.getColumnOrdinal(opt.ColumnID(i)))
	}

	aggregations := *groupBy.Child(1).(*memo.AggregationsExpr)
	aggInfos := make([]exec.AggInfo, len(aggregations))
	for i := range aggregations {
		item := &aggregations[i]
		name, overload := memo.FindAggregateOverload(item.Agg)

		distinct := false
		argIdx := make([]exec.ColumnOrdinal, item.Agg.ChildCount())
		for j := range argIdx {
			child := item.Agg.Child(j)

			if aggDistinct, ok := child.(*memo.AggDistinctExpr); ok {
				distinct = true
				child = aggDistinct.Input
			}
			v, ok := child.(*memo.VariableExpr)
			if !ok {
				return execPlan{}, errors.Errorf("only VariableOp args supported")
			}
			argIdx[j] = input.getColumnOrdinal(v.Col)
		}

		aggInfos[i] = exec.AggInfo{
			FuncName:   name,
			Builtin:    overload,
			Distinct:   distinct,
			ResultType: item.Agg.DataType(),
			ArgCols:    argIdx,
		}
		ep.outputCols.Set(int(item.Col), len(groupingColIdx)+i)
	}

	if groupBy.Op() == opt.ScalarGroupByOp {
		ep.root, err = b.factory.ConstructScalarGroupBy(input.root, aggInfos)
	} else {
		groupByInput := groupBy.Child(0).(memo.RelExpr)
		orderedInputCols := input.getColumnOrdinalSet(aggOrderedCols(groupByInput, groupingCols))
		reqOrdering := b.makeSQLOrderingFromChoice(ep, &groupBy.Physical().Ordering)
		ep.root, err = b.factory.ConstructGroupBy(
			input.root, groupingColIdx, orderedInputCols, aggInfos, exec.OutputOrdering(reqOrdering),
		)
	}
	if err != nil {
		return execPlan{}, err
	}
	return ep, nil
}

func (b *Builder) buildDistinct(distinct *memo.DistinctOnExpr) (execPlan, error) {
	input, err := b.buildGroupByInput(distinct)
	if err != nil {
		return execPlan{}, err
	}

	// The DistinctOn operator can effectively project away columns if they don't
	// have a corresponding aggregation. Introduce that project before the
	// distinct.
	aggs := distinct.Aggregations
	if n := distinct.GroupingCols.Len() + aggs.ChildCount(); n != input.outputCols.Len() {
		cols := make(opt.ColList, 0, n)
		for i, ok := distinct.GroupingCols.Next(0); ok; i, ok = distinct.GroupingCols.Next(i + 1) {
			cols = append(cols, opt.ColumnID(i))
		}
		cols = append(cols, aggs.Private().(opt.ColList)...)
		input.root, err = b.ensureColumns(input, cols)
		if err != nil {
			return execPlan{}, err
		}
		input.outputCols = opt.ColMap{}
		for i, col := range cols {
			input.outputCols.Set(int(col), i)
		}
	}

	distinctCols := input.getColumnOrdinalSet(distinct.GroupingCols)
	orderedCols := input.getColumnOrdinalSet(aggOrderedCols(distinct.Input, distinct.GroupingCols))
	node, err := b.factory.ConstructDistinct(input.root, distinctCols, orderedCols)
	if err != nil {
		return execPlan{}, err
	}
	return execPlan{root: node, outputCols: input.outputCols}, nil
}

func (b *Builder) buildGroupByInput(groupBy memo.RelExpr) (execPlan, error) {
	groupByInput := groupBy.Child(0).(memo.RelExpr)
	input, err := b.buildRelational(groupByInput)
	if err != nil {
		return execPlan{}, err
	}

	// TODO(radu): this is a one-off fix for an otherwise bigger gap: we should
	// have a more general mechanism (through physical properties or otherwise) to
	// figure out unneeded columns and project them away as necessary. The
	// optimizer doesn't guarantee that it adds ProjectOps everywhere.
	//
	// We address just the GroupBy case for now because there is a particularly
	// important case with COUNT(*) where we can remove all input columns, which
	// leads to significant speedup.
	private := groupBy.Private().(*memo.GroupingPrivate)
	neededCols := private.GroupingCols.Copy()
	aggs := *groupBy.Child(1).(*memo.AggregationsExpr)
	for i := range aggs {
		neededCols.UnionWith(memo.ExtractAggInputColumns(aggs[i].Agg))
	}

	if neededCols.Equals(groupByInput.Relational().OutputCols) {
		// All columns produced by the input are used.
		return input, nil
	}

	// The input is producing columns that are not useful; set up a projection.
	cols := make([]exec.ColumnOrdinal, 0, input.outputCols.Len())
	var newOutputCols opt.ColMap
	input.outputCols.ForEach(func(colID, ordinal int) {
		if neededCols.Contains(colID) {
			newOutputCols.Set(colID, len(cols))
			cols = append(cols, exec.ColumnOrdinal(ordinal))
		}
	})

	input.root, err = b.factory.ConstructSimpleProject(input.root, cols, nil /* colNames */)
	if err != nil {
		return execPlan{}, err
	}
	input.outputCols = newOutputCols
	return input, nil
}

// aggOrderedCols returns (as ordinals) the set of columns in the input of an
// aggregation operator on which there is an ordering.
func aggOrderedCols(inputExpr memo.RelExpr, groupingCols opt.ColSet) opt.ColSet {
	// Use the ordering that we require on the child (this is the more restrictive
	// between GroupByDef.Ordering and the ordering required on the aggregation
	// operator itself).
	ordering := inputExpr.Physical().Ordering
	var res opt.ColSet
	for i := range ordering.Columns {
		g := ordering.Columns[i].Group
		g = g.Intersection(groupingCols)
		if !g.Intersects(groupingCols) {
			// This group refers to a column that is not a grouping column.
			// The rest of the ordering is not useful.
			break
		}
		res.UnionWith(g)
	}
	res.IntersectionWith(groupingCols)
	return res
}

func (b *Builder) buildSetOp(set memo.RelExpr) (execPlan, error) {
	left, err := b.buildRelational(set.Child(0).(memo.RelExpr))
	if err != nil {
		return execPlan{}, err
	}
	right, err := b.buildRelational(set.Child(1).(memo.RelExpr))
	if err != nil {
		return execPlan{}, err
	}

	private := set.Private().(*memo.SetPrivate)

	// We need to make sure that the two sides render the columns in the same
	// order; otherwise we add projections.
	//
	// In most cases the projection is needed only to reorder the columns, but not
	// always. For example:
	//  (SELECT a, a, b FROM ab) UNION (SELECT x, y, z FROM xyz)
	// The left input could be just a scan that produces two columns.
	//
	// TODO(radu): we don't have to respect the exact order in the two ColLists;
	// if one side has the right columns but in a different permutation, we could
	// set up a matching projection on the other side. For example:
	//   (SELECT b, c, a FROM abc) UNION (SELECT z, y, x FROM xyz)
	// The expression for this could be a UnionOp on top of two ScanOps (any
	// internal projections could be removed by normalization rules).
	// The scans produce columns `a, b, c` and `x, y, z` respectively. We could
	// leave `a, b, c` as is and project the other side to `x, z, y`.
	// Note that (unless this is part of a larger query) the presentation property
	// will ensure that the columns are presented correctly in the output (i.e. in
	// the order `b, c, a`).
	leftNode, err := b.ensureColumns(left, private.LeftCols)
	if err != nil {
		return execPlan{}, err
	}
	rightNode, err := b.ensureColumns(right, private.RightCols)
	if err != nil {
		return execPlan{}, err
	}

	var typ tree.UnionType
	var all bool
	switch set.Op() {
	case opt.UnionOp:
		typ, all = tree.UnionOp, false
	case opt.UnionAllOp:
		typ, all = tree.UnionOp, true
	case opt.IntersectOp:
		typ, all = tree.IntersectOp, false
	case opt.IntersectAllOp:
		typ, all = tree.IntersectOp, true
	case opt.ExceptOp:
		typ, all = tree.ExceptOp, false
	case opt.ExceptAllOp:
		typ, all = tree.ExceptOp, true
	default:
		panic(fmt.Sprintf("invalid operator %s", set.Op()))
	}

	node, err := b.factory.ConstructSetOp(typ, all, leftNode, rightNode)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{root: node}
	for i, col := range private.OutCols {
		ep.outputCols.Set(int(col), i)
	}
	return ep, nil
}

// buildLimitOffset builds a plan for a LimitOp or OffsetOp
func (b *Builder) buildLimitOffset(e memo.RelExpr) (execPlan, error) {
	input, err := b.buildRelational(e.Child(0).(memo.RelExpr))
	if err != nil {
		return execPlan{}, err
	}
	// LIMIT/OFFSET expression should never need buildScalarContext, because it
	// can't refer to the input expression.
	expr, err := b.buildScalar(nil, e.Child(1).(opt.ScalarExpr))
	if err != nil {
		return execPlan{}, err
	}
	var node exec.Node
	if e.Op() == opt.LimitOp {
		node, err = b.factory.ConstructLimit(input.root, expr, nil)
	} else {
		node, err = b.factory.ConstructLimit(input.root, nil, expr)
	}
	if err != nil {
		return execPlan{}, err
	}
	return execPlan{root: node, outputCols: input.outputCols}, nil
}

func (b *Builder) buildSort(sort *memo.SortExpr) (execPlan, error) {
	input, err := b.buildRelational(sort.Input)
	if err != nil {
		return execPlan{}, err
	}
	return b.buildSortedInput(input, &sort.Physical().Ordering)
}

func (b *Builder) buildRowNumber(rowNum *memo.RowNumberExpr) (execPlan, error) {
	input, err := b.buildRelational(rowNum.Input)
	if err != nil {
		return execPlan{}, err
	}

	colName := b.mem.Metadata().ColumnLabel(rowNum.ColID)

	node, err := b.factory.ConstructOrdinality(input.root, colName)
	if err != nil {
		return execPlan{}, err
	}

	// We have one additional ordinality column, which is ordered at the end of
	// the list.
	outputCols := input.outputCols.Copy()
	outputCols.Set(int(rowNum.ColID), outputCols.Len())

	return execPlan{root: node, outputCols: outputCols}, nil
}

func (b *Builder) buildIndexJoin(join *memo.IndexJoinExpr) (execPlan, error) {
	var err error
	// If the index join child is a sort operator then flip the order so that the
	// sort is on top of the index join.
	// TODO(radu): Remove this code once we have support for a more general
	// lookup join execution path.
	var ordering *props.OrderingChoice
	child := join.Input
	if child.Op() == opt.SortOp {
		ordering = &child.Physical().Ordering
		child = child.Child(0).(memo.RelExpr)
	}

	input, err := b.buildRelational(child)
	if err != nil {
		return execPlan{}, err
	}

	md := b.mem.Metadata()

	cols := join.Cols
	needed, output := b.getColumns(cols, join.Table)
	res := execPlan{outputCols: output}

	// Get sort *result column* ordinals. Don't confuse these with *table column*
	// ordinals, which are used by the needed set. The sort columns should already
	// be in the needed set, so no need to add anything further to that.
	var reqOrdering sqlbase.ColumnOrdering
	if ordering == nil {
		reqOrdering = b.makeSQLOrderingFromChoice(res, &join.Physical().Ordering)
	}

	res.root, err = b.factory.ConstructIndexJoin(
		input.root, md.Table(join.Table), needed, exec.OutputOrdering(reqOrdering),
	)
	if err != nil {
		return execPlan{}, err
	}
	if ordering != nil {
		res, err = b.buildSortedInput(res, ordering)
		if err != nil {
			return execPlan{}, err
		}
	}

	return res, nil
}

func (b *Builder) buildLookupJoin(join *memo.LookupJoinExpr) (execPlan, error) {
	input, err := b.buildRelational(join.Input)
	if err != nil {
		return execPlan{}, err
	}

	md := b.mem.Metadata()

	keyCols := make([]exec.ColumnOrdinal, len(join.KeyCols))
	for i, c := range join.KeyCols {
		keyCols[i] = input.getColumnOrdinal(c)
	}

	inputCols := join.Input.Relational().OutputCols
	lookupCols := join.Cols.Difference(inputCols)

	lookupOrdinals, lookupColMap := b.getColumns(lookupCols, join.Table)
	allCols := joinOutputMap(input.outputCols, lookupColMap)

	res := execPlan{outputCols: allCols}

	// Get sort *result column* ordinals. Don't confuse these with *table column*
	// ordinals, which are used by the needed set. The sort columns should already
	// be in the needed set, so no need to add anything further to that.
	reqOrdering := b.makeSQLOrderingFromChoice(res, &join.Physical().Ordering)

	ctx := buildScalarCtx{
		ivh:     tree.MakeIndexedVarHelper(nil /* container */, allCols.Len()),
		ivarMap: allCols,
	}
	onExpr, err := b.buildScalar(&ctx, &join.On)
	if err != nil {
		return execPlan{}, err
	}

	tab := md.Table(join.Table)
	res.root, err = b.factory.ConstructLookupJoin(
		joinOpToJoinType(join.JoinType),
		input.root,
		tab,
		tab.Index(join.Index),
		keyCols,
		lookupOrdinals,
		onExpr,
		exec.OutputOrdering(reqOrdering),
	)
	if err != nil {
		return execPlan{}, err
	}

	// Apply a post-projection if Cols doesn't contain all input columns.
	if !inputCols.SubsetOf(join.Cols) {
		return b.applySimpleProject(res, join.Cols)
	}
	return res, nil
}

// initZipBuild builds the expressions in a Zip operation and initializes the
// data structures needed to build a projectSetNode.
// Note: this function modifies outputCols.
func (b *Builder) initZipBuild(
	zip *memo.ZipExpr, outputCols opt.ColMap, scalarCtx buildScalarCtx,
) (tree.TypedExprs, sqlbase.ResultColumns, []int, opt.ColMap, error) {
	exprs := make(tree.TypedExprs, len(zip.Funcs))
	numColsPerGen := make([]int, len(exprs))
	var err error
	for i, child := range zip.Funcs {
		exprs[i], err = b.buildScalar(&scalarCtx, child)
		if err != nil {
			return nil, nil, nil, opt.ColMap{}, err
		}

		if fn, ok := child.(*memo.FunctionExpr); ok && fn.Properties.Class == tree.GeneratorClass {
			numColsPerGen[i] = len(fn.Properties.ReturnLabels)
		} else {
			numColsPerGen[i] = 1
		}
	}

	md := b.mem.Metadata()
	resultCols := make(sqlbase.ResultColumns, len(zip.Cols))
	for i, col := range zip.Cols {
		resultCols[i].Name = md.ColumnLabel(col)
		resultCols[i].Typ = md.ColumnType(col)
	}

	numInputCols := outputCols.Len()
	for i, col := range zip.Cols {
		outputCols.Set(int(col), i+numInputCols)
	}

	return exprs, resultCols, numColsPerGen, outputCols, nil
}

func (b *Builder) buildZip(zip *memo.ZipExpr) (execPlan, error) {
	exprs, resultCols, numColsPerGen, outputCols, err := b.initZipBuild(
		zip, opt.ColMap{}, buildScalarCtx{},
	)
	if err != nil {
		return execPlan{}, err
	}

	// This is an uncorrelated Zip, so the input to the ProjectSet node is empty.
	input, err := b.factory.ConstructValues([][]tree.TypedExpr{{}}, nil)
	if err != nil {
		return execPlan{}, err
	}

	node, err := b.factory.ConstructProjectSet(input, exprs, resultCols, numColsPerGen)
	if err != nil {
		return execPlan{}, err
	}

	ep := execPlan{root: node}
	ep.outputCols = outputCols
	return ep, nil
}

func (b *Builder) buildProjectSet(join memo.RelExpr) (execPlan, error) {
	input, err := b.buildRelational(join.Child(0).(memo.RelExpr))
	if err != nil {
		return execPlan{}, err
	}

	ctx := input.makeBuildScalarCtx()
	exprs, resultCols, numColsPerGen, outputCols, err := b.initZipBuild(
		join.Child(1).(*memo.ZipExpr), input.outputCols, ctx,
	)
	if err != nil {
		return execPlan{}, err
	}

	node, err := b.factory.ConstructProjectSet(input.root, exprs, resultCols, numColsPerGen)
	if err != nil {
		return execPlan{}, err
	}

	ep := execPlan{root: node}
	ep.outputCols = outputCols
	return ep, nil
}

// needProjection figures out what projection is needed on top of the input plan
// to produce the given list of columns. If the input plan already produces
// the columns (in the same order), returns needProj=false.
func (b *Builder) needProjection(
	input execPlan, colList opt.ColList,
) (_ []exec.ColumnOrdinal, needProj bool) {
	if input.outputCols.Len() == len(colList) {
		identity := true
		for i, col := range colList {
			if ord, ok := input.outputCols.Get(int(col)); !ok || ord != i {
				identity = false
				break
			}
		}
		if identity {
			return nil, false
		}
	}
	cols := make([]exec.ColumnOrdinal, len(colList))
	for i, col := range colList {
		cols[i] = input.getColumnOrdinal(col)
	}
	return cols, true
}

// ensureColumns applies a projection as necessary to make the output match the
// given list of columns.
func (b *Builder) ensureColumns(input execPlan, colList opt.ColList) (exec.Node, error) {
	cols, needProj := b.needProjection(input, colList)
	if !needProj {
		// No projection necessary.
		return input.root, nil
	}
	return b.factory.ConstructSimpleProject(input.root, cols, nil /* colNames */)
}

// applyPresentation adds a projection to a plan to satisfy a required
// Presentation property.
func (b *Builder) applyPresentation(input execPlan, p props.Presentation) (execPlan, error) {
	colList := make(opt.ColList, len(p))
	colNames := make([]string, len(p))
	for i := range p {
		colList[i] = p[i].ID
		colNames[i] = p[i].Label
	}

	cols, needProj := b.needProjection(input, colList)
	if !needProj {
		node, err := b.factory.RenameColumns(input.root, colNames)
		return execPlan{root: node, outputCols: input.outputCols}, err
	}

	node, err := b.factory.ConstructSimpleProject(input.root, cols, colNames)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{root: node}
	for i := range p {
		ep.outputCols.Set(int(p[i].ID), i)
	}
	return ep, nil
}

func (b *Builder) buildExplain(explain *memo.ExplainExpr) (execPlan, error) {
	if explain.Options.Mode == tree.ExplainOpt {
		// Special case: EXPLAIN (OPT). Put the formatted expression in
		// a valuesNode.
		textRows := strings.Split(strings.Trim(explain.Input.String(), "\n"), "\n")
		rows := make([][]tree.TypedExpr, len(textRows))
		for i := range textRows {
			rows[i] = []tree.TypedExpr{tree.NewDString(textRows[i])}
		}
		return b.constructValues(rows, explain.ColList)
	}

	input, err := b.buildRelational(explain.Input)
	if err != nil {
		return execPlan{}, err
	}

	plan, err := b.factory.ConstructPlan(input.root, b.subqueries)
	if err != nil {
		return execPlan{}, err
	}
	node, err := b.factory.ConstructExplain(&explain.Options, plan)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{root: node}
	for i := range explain.ColList {
		ep.outputCols.Set(int(explain.ColList[i]), i)
	}
	// The subqueries are now owned by the explain node; remove them so they don't
	// also show up in the final plan.
	b.subqueries = b.subqueries[:0]
	return ep, nil
}

func (b *Builder) buildShowTrace(show *memo.ShowTraceForSessionExpr) (execPlan, error) {
	node, err := b.factory.ConstructShowTrace(show.TraceType, show.Compact)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{root: node}
	for i := range show.ColList {
		ep.outputCols.Set(int(show.ColList[i]), i)
	}
	// The subqueries are now owned by the explain node; remove them so they don't
	// also show up in the final plan.
	return ep, nil
}

// buildSortedInput is a helper method that can be reused to sort any input plan
// by the given ordering.
func (b *Builder) buildSortedInput(
	input execPlan, ordering *props.OrderingChoice,
) (execPlan, error) {
	colOrd := b.makeSQLOrderingFromChoice(input, ordering)
	node, err := b.factory.ConstructSort(input.root, colOrd)
	if err != nil {
		return execPlan{}, err
	}
	return execPlan{root: node, outputCols: input.outputCols}, nil
}
