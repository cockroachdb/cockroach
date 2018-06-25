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

func (b *Builder) buildRelational(ev memo.ExprView) (execPlan, error) {
	var ep execPlan
	var err error
	switch ev.Operator() {
	case opt.ValuesOp:
		ep, err = b.buildValues(ev)

	case opt.ScanOp:
		ep, err = b.buildScan(ev)

	case opt.SelectOp:
		ep, err = b.buildSelect(ev)

	case opt.ProjectOp:
		ep, err = b.buildProject(ev)

	case opt.InnerJoinOp:
		ep, err = b.buildJoin(ev, sqlbase.InnerJoin)

	case opt.LeftJoinOp:
		ep, err = b.buildJoin(ev, sqlbase.LeftOuterJoin)

	case opt.RightJoinOp:
		ep, err = b.buildJoin(ev, sqlbase.RightOuterJoin)

	case opt.FullJoinOp:
		ep, err = b.buildJoin(ev, sqlbase.FullOuterJoin)

	case opt.SemiJoinOp:
		ep, err = b.buildJoin(ev, sqlbase.LeftSemiJoin)

	case opt.AntiJoinOp:
		ep, err = b.buildJoin(ev, sqlbase.LeftAntiJoin)

	case opt.GroupByOp:
		ep, err = b.buildGroupBy(ev)

	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp,
		opt.UnionAllOp, opt.IntersectAllOp, opt.ExceptAllOp:
		ep, err = b.buildSetOp(ev)

	case opt.LimitOp, opt.OffsetOp:
		ep, err = b.buildLimitOffset(ev)

	case opt.SortOp:
		ep, err = b.buildSort(ev)

	case opt.LookupJoinOp:
		ep, err = b.buildLookupJoin(ev)

	case opt.ExplainOp:
		ep, err = b.buildExplain(ev)

	case opt.ShowTraceForSessionOp:
		ep, err = b.buildShowTrace(ev)

	case opt.RowNumberOp:
		ep, err = b.buildRowNumber(ev)

	default:
		return execPlan{}, errors.Errorf("unsupported relational op %s", ev.Operator())
	}
	if err != nil {
		return execPlan{}, err
	}
	if p := ev.Physical().Presentation; p.Defined() {
		ep, err = b.applyPresentation(ep, ev.Metadata(), p)
	}
	return ep, err
}

func (b *Builder) buildValues(ev memo.ExprView) (execPlan, error) {
	md := ev.Metadata()
	cols := ev.Private().(opt.ColList)
	numCols := len(cols)

	rows := make([][]tree.TypedExpr, ev.ChildCount())
	rowBuf := make([]tree.TypedExpr, len(rows)*numCols)
	scalarCtx := buildScalarCtx{}
	for i := range rows {
		row := ev.Child(i)
		if row.ChildCount() != numCols {
			return execPlan{}, fmt.Errorf("inconsistent row length %d vs %d", ev.ChildCount(), numCols)
		}
		// Chop off prefix of rowBuf and limit its capacity.
		rows[i] = rowBuf[:numCols:numCols]
		rowBuf = rowBuf[numCols:]
		var err error
		for j := 0; j < numCols; j++ {
			rows[i][j], err = b.buildScalar(&scalarCtx, row.Child(j))
			if err != nil {
				return execPlan{}, err
			}
		}
	}
	return b.constructValues(md, rows, cols)
}

func (b *Builder) constructValues(
	md *opt.Metadata, rows [][]tree.TypedExpr, cols opt.ColList,
) (execPlan, error) {
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
// column IDs, along with a mapping from the column IDs to output ordinals.
func (*Builder) getColumns(
	md *opt.Metadata, cols opt.ColSet, tableID opt.TableID,
) (exec.ColumnOrdinalSet, opt.ColMap) {
	needed := exec.ColumnOrdinalSet{}
	output := opt.ColMap{}

	columnCount := md.Table(tableID).ColumnCount()
	n := 0
	for i := 0; i < columnCount; i++ {
		colID := md.TableColumn(tableID, i)
		if cols.Contains(int(colID)) {
			needed.Add(i)
			output.Set(int(colID), n)
			n++
		}
	}

	return needed, output
}

func (b *Builder) makeSQLOrdering(res execPlan, ev memo.ExprView) sqlbase.ColumnOrdering {
	var reqOrder sqlbase.ColumnOrdering
	if reqProps := ev.Physical(); len(reqProps.Ordering) > 0 {
		reqOrder = make(sqlbase.ColumnOrdering, len(reqProps.Ordering))
		for i := range reqOrder {
			reqOrder[i].ColIdx = int(res.getColumnOrdinal(reqProps.Ordering[i].ID()))
			if reqProps.Ordering[i].Ascending() {
				reqOrder[i].Direction = encoding.Ascending
			} else {
				reqOrder[i].Direction = encoding.Descending
			}
		}
	}

	return reqOrder
}

func (b *Builder) buildScan(ev memo.ExprView) (execPlan, error) {
	def := ev.Private().(*memo.ScanOpDef)
	md := ev.Metadata()
	tab := md.Table(def.Table)

	needed, output := b.getColumns(md, def.Cols, def.Table)
	res := execPlan{outputCols: output}

	reqOrder := b.makeSQLOrdering(res, ev)

	root, err := b.factory.ConstructScan(
		tab,
		tab.Index(def.Index),
		needed,
		def.Constraint,
		def.HardLimit,
		reqOrder,
	)
	if err != nil {
		return execPlan{}, err
	}
	res.root = root
	return res, nil
}

func (b *Builder) buildSelect(ev memo.ExprView) (execPlan, error) {
	input, err := b.buildRelational(ev.Child(0))
	if err != nil {
		return execPlan{}, err
	}
	ctx := input.makeBuildScalarCtx()
	filter, err := b.buildScalar(&ctx, ev.Child(1))
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

func (b *Builder) buildProject(ev memo.ExprView) (execPlan, error) {
	input, err := b.buildRelational(ev.Child(0))
	if err != nil {
		return execPlan{}, err
	}
	var outputCols opt.ColMap
	projections := ev.Child(1)
	def := projections.Private().(*memo.ProjectionsOpDef)
	if len(def.SynthesizedCols) == 0 {
		// We have only pass-through columns.
		cols := make([]exec.ColumnOrdinal, 0, def.PassthroughCols.Len())
		def.PassthroughCols.ForEach(func(i int) {
			outputCols.Set(i, len(cols))
			cols = append(cols, input.getColumnOrdinal(opt.ColumnID(i)))
		})
		node, err := b.factory.ConstructSimpleProject(input.root, cols, nil /* colNames */)
		if err != nil {
			return execPlan{}, err
		}
		return execPlan{root: node, outputCols: outputCols}, nil
	}

	exprs := make(tree.TypedExprs, 0, len(def.SynthesizedCols)+def.PassthroughCols.Len())
	colNames := make([]string, 0, len(exprs))
	ctx := input.makeBuildScalarCtx()
	for i, col := range def.SynthesizedCols {
		expr, err := b.buildScalar(&ctx, projections.Child(i))
		if err != nil {
			return execPlan{}, err
		}
		outputCols.Set(int(col), i)
		exprs = append(exprs, expr)
		colNames = append(colNames, ev.Metadata().ColumnLabel(col))
	}
	def.PassthroughCols.ForEach(func(i int) {
		colID := opt.ColumnID(i)
		outputCols.Set(i, len(exprs))
		exprs = append(exprs, b.indexedVar(&ctx, ev.Metadata(), colID))
		colNames = append(colNames, ev.Metadata().ColumnLabel(colID))
	})
	node, err := b.factory.ConstructRender(input.root, exprs, colNames)
	if err != nil {
		return execPlan{}, err
	}
	return execPlan{root: node, outputCols: outputCols}, nil
}

func (b *Builder) buildJoin(ev memo.ExprView, joinType sqlbase.JoinType) (execPlan, error) {
	left, err := b.buildRelational(ev.Child(0))
	if err != nil {
		return execPlan{}, err
	}
	right, err := b.buildRelational(ev.Child(1))
	if err != nil {
		return execPlan{}, err
	}

	// Calculate the outputCols map for the join plan: the first numLeftCols
	// correspond to the columns from the left, the rest correspond to columns
	// from the right (except for Anti and Semi joins).
	numLeftCols := left.outputCols.Len()
	inputCols := left.outputCols.Copy()
	right.outputCols.ForEach(func(colIdx, rightIdx int) {
		inputCols.Set(colIdx, rightIdx+numLeftCols)
	})

	ctx := buildScalarCtx{
		ivh:     tree.MakeIndexedVarHelper(nil /* container */, inputCols.Len()),
		ivarMap: inputCols,
	}
	onExpr, err := b.buildScalar(&ctx, ev.Child(2))
	if err != nil {
		return execPlan{}, err
	}

	var ep execPlan
	if joinType == sqlbase.LeftSemiJoin || joinType == sqlbase.LeftAntiJoin {
		// For semi and anti join, only the left columns are output.
		ep.outputCols = left.outputCols
	} else {
		ep.outputCols = inputCols
	}
	ep.root, err = b.factory.ConstructJoin(joinType, left.root, right.root, onExpr)
	if err != nil {
		return execPlan{}, err
	}
	return ep, nil
}

func (b *Builder) buildGroupBy(ev memo.ExprView) (execPlan, error) {
	input, err := b.buildRelational(ev.Child(0))
	if err != nil {
		return execPlan{}, err
	}
	aggregations := ev.Child(1)
	numAgg := aggregations.ChildCount()
	groupingCols := ev.Private().(*memo.GroupByDef).GroupingCols

	groupingColIdx := make([]exec.ColumnOrdinal, 0, groupingCols.Len())
	var ep execPlan
	for i, ok := groupingCols.Next(0); ok; i, ok = groupingCols.Next(i + 1) {
		ep.outputCols.Set(i, len(groupingColIdx))
		groupingColIdx = append(groupingColIdx, input.getColumnOrdinal(opt.ColumnID(i)))
	}

	aggColList := aggregations.Private().(opt.ColList)
	aggInfos := make([]exec.AggInfo, numAgg)
	for i := 0; i < numAgg; i++ {
		fn := aggregations.Child(i)
		name, overload := memo.FindAggregateOverload(fn)

		argIdx := make([]exec.ColumnOrdinal, fn.ChildCount())
		for j := range argIdx {
			child := fn.Child(j)
			if child.Operator() != opt.VariableOp {
				return execPlan{}, errors.Errorf("only VariableOp args supported")
			}
			col := child.Private().(opt.ColumnID)
			argIdx[j] = input.getColumnOrdinal(col)
		}

		aggInfos[i] = exec.AggInfo{
			FuncName:   name,
			Builtin:    overload,
			ResultType: fn.Logical().Scalar.Type,
			ArgCols:    argIdx,
		}
		ep.outputCols.Set(int(aggColList[i]), len(groupingColIdx)+i)
	}

	ep.root, err = b.factory.ConstructGroupBy(input.root, groupingColIdx, aggInfos)
	if err != nil {
		return execPlan{}, err
	}
	return ep, nil
}

func (b *Builder) buildSetOp(ev memo.ExprView) (execPlan, error) {
	left, err := b.buildRelational(ev.Child(0))
	if err != nil {
		return execPlan{}, err
	}
	right, err := b.buildRelational(ev.Child(1))
	if err != nil {
		return execPlan{}, err
	}

	colMap := *ev.Private().(*memo.SetOpColMap)

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
	leftNode, err := b.ensureColumns(left, colMap.Left)
	if err != nil {
		return execPlan{}, err
	}
	rightNode, err := b.ensureColumns(right, colMap.Right)
	if err != nil {
		return execPlan{}, err
	}

	var typ tree.UnionType
	var all bool
	switch ev.Operator() {
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
		panic(fmt.Sprintf("invalid operator %s", ev.Operator()))
	}

	node, err := b.factory.ConstructSetOp(typ, all, leftNode, rightNode)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{root: node}
	for i, col := range colMap.Out {
		ep.outputCols.Set(int(col), i)
	}
	return ep, nil
}

// buildLimitOffset builds a plan for a LimitOp or OffsetOp
func (b *Builder) buildLimitOffset(ev memo.ExprView) (execPlan, error) {
	input, err := b.buildRelational(ev.Child(0))
	if err != nil {
		return execPlan{}, err
	}
	// LIMIT/OFFSET expression should never need buildScalarContext, because it
	// can't refer to the input expression.
	expr, err := b.buildScalar(nil, ev.Child(1))
	if err != nil {
		return execPlan{}, err
	}
	var node exec.Node
	if ev.Operator() == opt.LimitOp {
		node, err = b.factory.ConstructLimit(input.root, expr, nil)
	} else {
		node, err = b.factory.ConstructLimit(input.root, nil, expr)
	}
	if err != nil {
		return execPlan{}, err
	}
	return execPlan{root: node, outputCols: input.outputCols}, nil
}

func (b *Builder) buildSort(ev memo.ExprView) (execPlan, error) {
	input, err := b.buildRelational(ev.Child(0))
	if err != nil {
		return execPlan{}, err
	}
	return b.buildSortedInput(input, ev.Physical().Ordering)
}

func (b *Builder) buildRowNumber(ev memo.ExprView) (execPlan, error) {
	input, err := b.buildRelational(ev.Child(0))
	if err != nil {
		return execPlan{}, err
	}

	def := ev.Private().(*memo.RowNumberDef)
	colName := ev.Metadata().ColumnLabel(def.ColID)

	node, err := b.factory.ConstructOrdinality(input.root, colName)
	if err != nil {
		return execPlan{}, err
	}

	// We have one additional ordinality column, which is ordered at the end of
	// the list.
	outputCols := input.outputCols.Copy()
	outputCols.Set(int(def.ColID), outputCols.Len())

	return execPlan{root: node, outputCols: outputCols}, nil
}

func (b *Builder) buildLookupJoin(ev memo.ExprView) (execPlan, error) {
	var err error
	// If lookup join child is a limit and/or sort operator then flip the order so
	// that the sort/limit is on top of the lookup join.
	// TODO(radu): Remove this code once we have support for a more general
	// lookup join operator.
	var limit tree.TypedExpr
	var ordering opt.Ordering
	child := ev.Child(0)
	if child.Operator() == opt.LimitOp {
		limit, err = b.buildScalar(nil, child.Child(1))
		if err != nil {
			return execPlan{}, err
		}
		child = child.Child(0)
	}
	if child.Operator() == opt.SortOp {
		ordering = child.Physical().Ordering
		child = child.Child(0)
	}

	input, err := b.buildRelational(child)
	if err != nil {
		return execPlan{}, err
	}

	md := ev.Metadata()
	def := ev.Private().(*memo.LookupJoinDef)

	cols := ev.Child(0).Logical().Relational.OutputCols.Union(def.LookupCols)
	needed, output := b.getColumns(md, cols, def.Table)
	res := execPlan{outputCols: output}

	// Get sort *result column* ordinals. Don't confuse these with *table column*
	// ordinals, which are used by the needed set. The sort columns should already
	// be in the needed set, so no need to add anything further to that.
	var reqOrder sqlbase.ColumnOrdering
	if ordering == nil {
		reqOrder = b.makeSQLOrdering(res, ev)
	}

	res.root, err = b.factory.ConstructIndexJoin(input.root, md.Table(def.Table), needed, reqOrder)
	if err != nil {
		return execPlan{}, err
	}
	if ordering != nil {
		res, err = b.buildSortedInput(res, ordering)
		if err != nil {
			return execPlan{}, err
		}
	}

	if limit != nil {
		var err error
		res.root, err = b.factory.ConstructLimit(res.root, limit, nil)
		if err != nil {
			return execPlan{}, err
		}
	}
	return res, nil
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
func (b *Builder) applyPresentation(
	input execPlan, md *opt.Metadata, p props.Presentation,
) (execPlan, error) {
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

func (b *Builder) buildExplain(ev memo.ExprView) (execPlan, error) {
	def := ev.Private().(*memo.ExplainOpDef)
	if def.Options.Mode == tree.ExplainOpt {
		// Special case: EXPLAIN (OPT). Put the formatted expression in
		// a valuesNode.
		textRows := strings.Split(strings.Trim(ev.Child(0).String(), "\n"), "\n")
		rows := make([][]tree.TypedExpr, len(textRows))
		for i := range textRows {
			rows[i] = []tree.TypedExpr{tree.NewDString(textRows[i])}
		}
		return b.constructValues(ev.Metadata(), rows, def.ColList)
	}

	input, err := b.buildRelational(ev.Child(0))
	if err != nil {
		return execPlan{}, err
	}

	plan, err := b.factory.ConstructPlan(input.root, b.subqueries)
	if err != nil {
		return execPlan{}, err
	}
	node, err := b.factory.ConstructExplain(&def.Options, plan)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{root: node}
	for i := range def.ColList {
		ep.outputCols.Set(int(def.ColList[i]), i)
	}
	// The subqueries are now owned by the explain node; remove them so they don't
	// also show up in the final plan.
	b.subqueries = b.subqueries[:0]
	return ep, nil
}

func (b *Builder) buildShowTrace(ev memo.ExprView) (execPlan, error) {
	def := ev.Private().(*memo.ShowTraceOpDef)
	node, err := b.factory.ConstructShowTrace(def.Type, def.Compact)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{root: node}
	for i := range def.ColList {
		ep.outputCols.Set(int(def.ColList[i]), i)
	}
	// The subqueries are now owned by the explain node; remove them so they don't
	// also show up in the final plan.
	return ep, nil
}

// buildSortedInput is a helper method that can be reused to sort any input plan
// by the given ordering.
func (b *Builder) buildSortedInput(input execPlan, ordering opt.Ordering) (execPlan, error) {
	colOrd := make(sqlbase.ColumnOrdering, len(ordering))
	for i, col := range ordering {
		ord := input.getColumnOrdinal(col.ID())
		colOrd[i].ColIdx = int(ord)
		if col.Descending() {
			colOrd[i].Direction = encoding.Descending
		} else {
			colOrd[i].Direction = encoding.Ascending
		}
	}
	node, err := b.factory.ConstructSort(input.root, colOrd)
	if err != nil {
		return execPlan{}, err
	}
	return execPlan{root: node, outputCols: input.outputCols}, nil
}
