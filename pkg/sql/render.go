// Copyright 2015 The Cockroach Authors.
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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// renderNode encapsulates the render logic of a select statement:
// expressing new values using expressions over source values.
type renderNode struct {
	planner *planner

	// source describes where the data is coming from.
	// populated initially by initFrom().
	// potentially modified by index selection.
	source planDataSource

	// sourceInfo contains the reference to the dataSourceInfo in the
	// source planDataSource that is needed for name resolution.
	// We keep one instance of multiSourceInfo cached here so as to avoid
	// re-creating it every time analyzeExpr() is called in computeRender().
	sourceInfo multiSourceInfo

	// Helper for indexed vars. This holds the actual instances of
	// IndexedVars replaced in Exprs. The indexed vars contain indices
	// to the array of source columns.
	ivarHelper parser.IndexedVarHelper

	// Rendering expressions for rows and corresponding output columns.
	// populated by addOrMergeRenders()
	// as invoked initially by initTargets() and initWhere().
	// sortNode peeks into the render array defined by initTargets() as an optimization.
	// sortNode adds extra renderNode renders for sort columns not requested as select targets.
	// groupNode copies/extends the render array defined by initTargets() and
	// will add extra renderNode renders for the aggregation sources.
	// windowNode also adds additional renders for the window functions.
	render  []parser.TypedExpr
	columns ResultColumns

	// A piece of metadata to indicate whether a star expression was expanded
	// during rendering.
	isStar bool

	// The number of initial columns - before adding any internal render
	// targets for grouping, filtering or ordering. The original columns
	// are columns[:numOriginalCols], the internally added ones are
	// columns[numOriginalCols:].
	// populated by initTargets(), which thus must be obviously vcalled before initWhere()
	// and the other initializations that may add render columns.
	numOriginalCols int

	// ordering indicates the order of returned rows.
	// initially suggested by the GROUP BY and ORDER BY clauses;
	// modified by index selection.
	ordering orderingInfo

	// explain supports EXPLAIN(DEBUG).
	explain explainMode

	// The current source row, with one value per source column.
	// populated by Next(), used by renderRow().
	curSourceRow parser.Datums

	// The rendered row, with one value for each render expression.
	// populated by Next().
	row parser.Datums

	// This struct must be allocated on the heap and its location stay
	// stable after construction because it implements
	// IndexedVarContainer and the IndexedVar objects in sub-expressions
	// will link to it by reference after checkRenderStar / analyzeExpr.
	// Enforce this using NoCopy.
	noCopy util.NoCopy
}

func (s *renderNode) Columns() ResultColumns {
	return s.columns
}

func (s *renderNode) Ordering() orderingInfo {
	return s.ordering
}

func (s *renderNode) Values() parser.Datums {
	return s.row
}

func (s *renderNode) MarkDebug(mode explainMode) {
	if mode != explainDebug {
		panic(fmt.Sprintf("unknown debug mode %d", mode))
	}
	s.explain = mode
	s.source.plan.MarkDebug(mode)
}

func (s *renderNode) DebugValues() debugValues {
	return s.source.plan.DebugValues()
}

func (s *renderNode) Start() error { return s.source.plan.Start() }

func (s *renderNode) Next() (bool, error) {
	if next, err := s.source.plan.Next(); !next {
		return false, err
	}

	if s.explain == explainDebug && s.source.plan.DebugValues().output != debugValueRow {
		// Pass through non-row debug values.
		return true, nil
	}

	row := s.source.plan.Values()
	s.curSourceRow = row

	err := s.renderRow()
	return err == nil, err
}

func (s *renderNode) Close() {
	s.source.plan.Close()
}

// IndexedVarEval implements the parser.IndexedVarContainer interface.
func (s *renderNode) IndexedVarEval(idx int, ctx *parser.EvalContext) (parser.Datum, error) {
	return s.curSourceRow[idx].Eval(ctx)
}

// IndexedVarResolvedType implements the parser.IndexedVarContainer interface.
func (s *renderNode) IndexedVarResolvedType(idx int) parser.Type {
	return s.sourceInfo[0].sourceColumns[idx].Typ
}

// IndexedVarString implements the parser.IndexedVarContainer interface.
func (s *renderNode) IndexedVarFormat(buf *bytes.Buffer, f parser.FmtFlags, idx int) {
	s.sourceInfo[0].FormatVar(buf, f, idx)
}

// Select selects rows from a SELECT/UNION/VALUES, ordering and/or limiting them.
func (p *planner) Select(
	n *parser.Select, desiredTypes []parser.Type, autoCommit bool,
) (planNode, error) {
	wrapped := n.Select
	limit := n.Limit
	orderBy := n.OrderBy

	for s, ok := wrapped.(*parser.ParenSelect); ok; s, ok = wrapped.(*parser.ParenSelect) {
		wrapped = s.Select.Select
		if s.Select.OrderBy != nil {
			if orderBy != nil {
				return nil, fmt.Errorf("multiple ORDER BY clauses not allowed")
			}
			orderBy = s.Select.OrderBy
		}
		if s.Select.Limit != nil {
			if limit != nil {
				return nil, fmt.Errorf("multiple LIMIT clauses not allowed")
			}
			limit = s.Select.Limit
		}
	}

	switch s := wrapped.(type) {
	case *parser.SelectClause:
		// Select can potentially optimize index selection if it's being ordered,
		// so we allow it to do its own sorting.
		return p.SelectClause(s, orderBy, limit, desiredTypes, publicColumns)

	// TODO(dan): Union can also do optimizations when it has an ORDER BY, but
	// currently expects the ordering to be done externally, so we let it fall
	// through. Instead of continuing this special casing, it may be worth
	// investigating a general mechanism for passing some context down during
	// plan node construction.
	default:
		plan, err := p.newPlan(s, desiredTypes, autoCommit)
		if err != nil {
			return nil, err
		}
		sort, err := p.orderBy(orderBy, plan)
		if err != nil {
			return nil, err
		}
		if sort != nil {
			sort.plan = plan
			plan = sort
		}
		limit, err := p.Limit(limit)
		if err != nil {
			return nil, err
		}
		if limit != nil {
			limit.plan = plan
			plan = limit
		}
		return plan, nil
	}
}

// SelectClause selects rows from a single table. Select is the workhorse of the
// SQL statements. In the slowest and most general case, select must perform
// full table scans across multiple tables and sort and join the resulting rows
// on arbitrary columns. Full table scans can be avoided when indexes can be
// used to satisfy the where-clause. scanVisibility controls which columns are
// visible to the select.
//
// NB: This is passed directly to planNode only when there is no ORDER BY,
// LIMIT, or parenthesis in the parsed SELECT. See `sql/parser.Select` and
// `sql/parser.SelectStatement`.
//
// Privileges: SELECT on table
//   Notes: postgres requires SELECT. Also requires UPDATE on "FOR UPDATE".
//          mysql requires SELECT.
func (p *planner) SelectClause(
	parsed *parser.SelectClause,
	orderBy parser.OrderBy,
	limit *parser.Limit,
	desiredTypes []parser.Type,
	scanVisibility scanVisibility,
) (planNode, error) {
	s := &renderNode{planner: p}

	if err := s.initFrom(parsed, scanVisibility); err != nil {
		return nil, err
	}

	where, err := s.initWhere(parsed.Where)
	if err != nil {
		return nil, err
	}

	s.ivarHelper = parser.MakeIndexedVarHelper(s, len(s.sourceInfo[0].sourceColumns))

	if err := s.initTargets(parsed.Exprs, desiredTypes); err != nil {
		return nil, err
	}

	// NB: orderBy, window, and groupBy are passed and can modify the renderNode,
	// but must do so in that order.
	sort, err := p.orderBy(orderBy, s)
	if err != nil {
		return nil, err
	}
	window, err := p.window(parsed, s)
	if err != nil {
		return nil, err
	}
	group, err := p.groupBy(parsed, s)
	if err != nil {
		return nil, err
	}

	if where != nil && where.filter != nil && group != nil {
		// Allow the group-by to add an implicit "IS NOT NULL" filter.
		where.filter = where.ivarHelper.Rebind(group.isNotNullFilter(where.filter), false, false)
	}

	limitPlan, err := p.Limit(limit)
	if err != nil {
		return nil, err
	}
	distinctPlan := p.Distinct(parsed)

	result := planNode(s)
	if group != nil {
		group.plan = result
		result = group
	}
	if window != nil {
		window.plan = result
		result = window
	}
	if sort != nil {
		sort.plan = result
		result = sort
	}
	if distinctPlan != nil {
		distinctPlan.plan = result
		result = distinctPlan
	}
	if limitPlan != nil {
		limitPlan.plan = result
		result = limitPlan
	}
	return result, nil
}

// initFrom initializes the table node, given the parsed select expression
func (s *renderNode) initFrom(parsed *parser.SelectClause, scanVisibility scanVisibility) error {
	// AS OF expressions should be handled by the executor.
	if parsed.From.AsOf.Expr != nil && !s.planner.avoidCachedDescriptors {
		return fmt.Errorf("unexpected AS OF SYSTEM TIME")
	}
	src, err := s.planner.getSources(parsed.From.Tables, scanVisibility)
	if err != nil {
		return err
	}
	s.source = src
	s.sourceInfo = multiSourceInfo{s.source.info}
	return nil
}

func (s *renderNode) initTargets(targets parser.SelectExprs, desiredTypes []parser.Type) error {
	// Loop over the select expressions and expand them into the expressions
	// we're going to use to generate the returned column set and the names for
	// those columns.
	for i, target := range targets {
		desiredType := parser.TypeAny
		if len(desiredTypes) > i {
			desiredType = desiredTypes[i]
		}
		cols, exprs, hasStar, err := s.planner.computeRender(target, desiredType,
			s.source.info, s.ivarHelper, true)
		if err != nil {
			return err
		}

		// If the current expression is a set-returning function, we need to move
		// it up to the sources list as a cross join and add a render for the
		// function's column in the join.
		if e := extractSetReturningFunction(exprs); e != nil {
			cols, exprs, hasStar, err = s.transformToCrossJoin(e, desiredType)
			if err != nil {
				return err
			}
		}

		s.isStar = s.isStar || hasStar
		_ = s.addOrMergeRenders(cols, exprs, false)
	}
	// `groupBy` or `orderBy` may internally add additional columns which we
	// do not want to include in validation of e.g. `GROUP BY 2`. We record the
	// current (initial) number of columns.
	s.numOriginalCols = len(s.columns)
	if len(s.render) != len(s.columns) {
		panic(fmt.Sprintf("%d renders but %d columns!", len(s.render), len(s.columns)))
	}
	return nil
}

// extractSetReturningFunction checks if the first expression in the list is a
// FuncExpr that returns a TypeTable, returning it if so.
func extractSetReturningFunction(exprs []parser.TypedExpr) *parser.FuncExpr {
	if len(exprs) == 1 && exprs[0].ResolvedType().FamilyEqual(parser.TypeTable) {
		switch e := exprs[0].(type) {
		case *parser.FuncExpr:
			return e
		}
	}
	return nil
}

// transformToCrossJoin moves a would-be render expression into a data source
// cross-joined with the renderNode's existing data sources, returning a
// render expression that points at the new data source.
func (s *renderNode) transformToCrossJoin(
	e *parser.FuncExpr, desiredType parser.Type,
) (columns ResultColumns, exprs []parser.TypedExpr, hasStar bool, err error) {
	src, err := s.planner.getDataSource(e, nil, publicColumns)
	if err != nil {
		return nil, nil, false, err
	}
	src, err = s.planner.makeJoin("CROSS JOIN", s.source, src, nil)
	if err != nil {
		return nil, nil, false, err
	}
	s.source = src
	s.sourceInfo = multiSourceInfo{s.source.info}
	// We must regenerate the var helper at this point since we changed
	// the source list.
	s.ivarHelper = parser.MakeIndexedVarHelper(s, len(s.sourceInfo[0].sourceColumns))

	newTarget := parser.SelectExpr{
		Expr: s.ivarHelper.IndexedVar(s.ivarHelper.NumVars() - 1),
	}
	return s.planner.computeRender(newTarget, desiredType,
		s.source.info, s.ivarHelper, true)
}

func (s *renderNode) initWhere(where *parser.Where) (*filterNode, error) {
	if where == nil {
		return nil, nil
	}

	f := &filterNode{p: s.planner, source: s.source}
	f.ivarHelper = parser.MakeIndexedVarHelper(f, len(s.sourceInfo[0].sourceColumns))

	var err error
	f.filter, err = s.planner.analyzeExpr(where.Expr, s.sourceInfo, f.ivarHelper,
		parser.TypeBool, true, "WHERE")
	if err != nil {
		return nil, err
	}

	// Make sure there are no aggregation/window functions in the filter
	// (after subqueries have been expanded).
	if err := s.planner.parser.AssertNoAggregationOrWindowing(
		f.filter, "WHERE", s.planner.session.SearchPath,
	); err != nil {
		return nil, err
	}

	// Insert the newly created filterNode between the renderNode and
	// its original FROM source.
	f.source = s.source
	s.source.plan = f

	return f, nil
}

// getRenderColName returns the output column name for a render expression.
func getRenderColName(target parser.SelectExpr) string {
	if target.As != "" {
		return string(target.As)
	}

	// If the expression designates a column, try to reuse that column's name
	// as render name.
	if c, ok := target.Expr.(*parser.ColumnItem); ok {
		// We only shorten the name of the result column to become the
		// unqualified column part of this expr name if there is
		// no additional subscript on the column.
		if len(c.Selector) == 0 {
			return c.Column()
		}
	}
	return target.Expr.String()
}

// appendRenderColumn adds a new render expression at the end of the current list.
// The expression must be normalized already.
func (s *renderNode) addRenderColumn(expr parser.TypedExpr, col ResultColumn) {
	s.render = append(s.render, expr)
	s.columns = append(s.columns, col)
}

// resetRenderColumns resets all the render expressions. This is used e.g. by
// aggregation and windowing (see group.go / window.go). The method also
// asserts that both the render and columns array have the same size.
func (s *renderNode) resetRenderColumns(exprs []parser.TypedExpr, cols ResultColumns) {
	if len(exprs) != len(cols) {
		panic(fmt.Sprintf("resetRenderColumns used with arrays of different sizes: %d != %d", len(exprs), len(cols)))
	}
	s.render = exprs
	s.columns = cols
}

// renderRow renders the row by evaluating the render expressions.
func (s *renderNode) renderRow() error {
	if s.row == nil {
		s.row = make([]parser.Datum, len(s.render))
	}
	for i, e := range s.render {
		var err error
		s.row[i], err = e.Eval(&s.planner.evalCtx)
		if err != nil {
			return err
		}
	}
	return nil
}

// Searches for a render target that matches the given column reference.
func (s *renderNode) findRenderIndexForCol(colIdx int) (idx int, ok bool) {
	for i, r := range s.render {
		if ivar, ok := r.(*parser.IndexedVar); ok && ivar.Idx == colIdx {
			return i, true
		}
	}
	return invalidColIdx, false
}

// Computes ordering information for the render node, given ordering information for the "from"
// node.
//
//    SELECT a, b FROM t@abc ...
//      the ordering is: first by column 0 (a), then by column 1 (b)
//
//    SELECT a, b FROM t@abc WHERE a = 1 ...
//      the ordering is: exact match column (a), ordered by column 1 (b)
//
//    SELECT b, a FROM t@abc ...
//      the ordering is: first by column 1 (a), then by column 0 (a)
//
//    SELECT a, c FROM t@abc ...
//      the ordering is: just by column 0 (a). Here we don't have b as a render target so we
//      cannot possibly use (or even express) the second-rank order by b (which makes any lower
//      ranks unusable as well).
//
//      Note that for queries like
//         SELECT a, c FROM t@abc ORDER by a,b,c
//      we internally add b as a render target. The same holds for any targets required for
//      grouping.
func (s *renderNode) computeOrdering(fromOrder orderingInfo) orderingInfo {
	var ordering orderingInfo

	// See if any of the "exact match" columns have render targets. We can ignore any columns that
	// don't have render targets. For example, assume we are using an ascending index on (k, v) with
	// the query:
	//
	//   SELECT v FROM t WHERE k = 1
	//
	// The rows from the index are ordered by k then by v, but since k is an exact match
	// column the results are also ordered just by v.
	for colIdx := range fromOrder.exactMatchCols {
		if renderIdx, ok := s.findRenderIndexForCol(colIdx); ok {
			ordering.addExactMatchColumn(renderIdx)
		}
	}
	// Find the longest prefix of columns that have render targets. Once we find a column that is
	// not part of the output, the rest of the ordered columns aren't useful.
	//
	// For example, assume we are using an ascending index on (k, v) with the query:
	//
	//   SELECT v FROM t WHERE k > 1
	//
	// The rows from the index are ordered by k then by v. We cannot make any use of this
	// ordering as an ordering on v.
	for _, colOrder := range fromOrder.ordering {
		renderIdx, ok := s.findRenderIndexForCol(colOrder.ColIdx)
		if !ok {
			return ordering
		}
		ordering.addColumn(renderIdx, colOrder.Direction)
	}
	// We added all columns in fromOrder; we can copy the distinct flag.
	ordering.unique = fromOrder.unique
	return ordering
}
