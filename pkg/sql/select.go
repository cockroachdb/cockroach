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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// selectNode encapsulates the core logic of a select statement: retrieving filtered results from
// the sources. Grouping, sorting, distinct and limits are implemented on top of this node (as
// wrappers), though these are taken into consideration at the selectNode level for optimization
// (e.g. index selection).
type selectNode struct {
	planner *planner

	// top refers to the surrounding selectTopNode.
	top *selectTopNode

	// source describes where the data is coming from.
	// populated initially by initFrom().
	// potentially modified by index selection.
	source planDataSource

	// sourceInfo contains the reference to the dataSourceInfo in the
	// source planDataSource that is needed for name resolution.
	// We keep one instance of multiSourceInfo cached here so as to avoid
	// re-creating it every time analyzeExpr() is called in addRender().
	sourceInfo multiSourceInfo

	// Helper for indexed vars. This holds the actual instances of
	// IndexedVars replaced in Exprs. The indexed vars contain indices
	// to the array of source columns.
	ivarHelper parser.IndexedVarHelper

	// Rendering expressions for rows and corresponding output columns.
	// populated by addRender()
	// as invoked initially by initTargets() and initWhere().
	// sortNode peeks into the render array defined by initTargets() as an optimization.
	// sortNode adds extra selectNode renders for sort columns not requested as select targets.
	// groupNode copies/extends the render array defined by initTargets()
	// will add extra selectNode renders for the aggregation sources.
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

	// Filtering expression for rows.
	// populated initially by initWhere().
	// modified by index selection (split between scan filter and post-indexjoin filter).
	filter parser.TypedExpr

	// ordering indicates the order of returned rows.
	// initially suggested by the GROUP BY and ORDER BY clauses;
	// modified by index selection.
	ordering orderingInfo

	// support attributes for EXPLAIN(DEBUG)
	explain   explainMode
	debugVals debugValues

	// The current source row, with one value per source column.
	// populated by Next(), used by renderRow().
	curSourceRow parser.DTuple

	// The rendered row, with one value for each render expression.
	// populated by Next().
	row parser.DTuple

	// This struct must be allocated on the heap and its location stay
	// stable after construction because it implements
	// IndexedVarContainer and the IndexedVar objects in sub-expressions
	// will link to it by reference after checkRenderStar / analyzeExpr.
	// Enforce this using NoCopy.
	noCopy util.NoCopy
}

func (s *selectNode) Columns() ResultColumns {
	return s.columns
}

func (s *selectNode) Ordering() orderingInfo {
	return s.ordering
}

func (s *selectNode) Values() parser.DTuple {
	return s.row
}

func (s *selectNode) MarkDebug(mode explainMode) {
	if mode != explainDebug {
		panic(fmt.Sprintf("unknown debug mode %d", mode))
	}
	s.explain = mode
	s.source.plan.MarkDebug(mode)
}

func (s *selectNode) DebugValues() debugValues {
	if s.explain != explainDebug {
		panic(fmt.Sprintf("node not in debug mode (mode %d)", s.explain))
	}
	return s.debugVals
}

func (s *selectNode) Start() error {
	if err := s.source.plan.Start(); err != nil {
		return err
	}

	for _, e := range s.render {
		if err := s.planner.startSubqueryPlans(e); err != nil {
			return err
		}
	}
	return s.planner.startSubqueryPlans(s.filter)
}

func (s *selectNode) Next() (bool, error) {
	for {
		if next, err := s.source.plan.Next(); !next {
			return false, err
		}

		if s.explain == explainDebug {
			s.debugVals = s.source.plan.DebugValues()

			if s.debugVals.output != debugValueRow {
				// Let the debug values pass through.
				return true, nil
			}
		}
		row := s.source.plan.Values()
		s.curSourceRow = row
		passesFilter, err := sqlbase.RunFilter(s.filter, &s.planner.evalCtx)
		if err != nil {
			return false, err
		}

		if passesFilter {
			err := s.renderRow()
			return err == nil, err
		} else if s.explain == explainDebug {
			// Mark the row as filtered out.
			s.debugVals.output = debugValueFiltered
			return true, nil
		}
		// Row was filtered out; grab the next row.
	}
}

func (s *selectNode) ExplainTypes(regTypes func(string, string)) {
	if s.filter != nil {
		regTypes("filter", parser.AsStringWithFlags(s.filter, parser.FmtShowTypes))
	}
	for i, rexpr := range s.render {
		regTypes(fmt.Sprintf("render %d", i), parser.AsStringWithFlags(rexpr, parser.FmtShowTypes))
	}
}

func (s *selectNode) ExplainPlan(v bool) (name, description string, children []planNode) {
	subplans := []planNode{s.source.plan}

	subplans = s.planner.collectSubqueryPlans(s.filter, subplans)

	for _, e := range s.render {
		subplans = s.planner.collectSubqueryPlans(e, subplans)
	}

	if len(subplans) == 1 && !v {
		return s.source.plan.ExplainPlan(v)
	}

	var buf bytes.Buffer

	buf.WriteString("from (")
	for i, col := range s.source.info.sourceColumns {
		if i > 0 {
			buf.WriteString(", ")
		}
		if col.hidden {
			buf.WriteByte('*')
		}
		alias := s.source.info.findTableAlias(i)
		parser.FormatNode(&buf, parser.FmtSimple, &alias)
		buf.WriteByte('.')
		parser.FormatNode(&buf, parser.FmtSimple, parser.Name(col.Name))
	}
	buf.WriteByte(')')

	name = "render/filter"
	if s.explain != explainNone {
		name = fmt.Sprintf("%s(%s)", name, explainStrings[s.explain])
	}

	return name, buf.String(), subplans
}

func (s *selectNode) SetLimitHint(numRows int64, soft bool) {
	s.source.plan.SetLimitHint(numRows, soft || s.filter != nil)
}

func (s *selectNode) Close() {
	s.source.plan.Close()
}

// IndexedVarEval implements the parser.IndexedVarContainer interface.
func (s *selectNode) IndexedVarEval(idx int, ctx *parser.EvalContext) (parser.Datum, error) {
	return s.curSourceRow[idx].Eval(ctx)
}

// IndexedVarResolvedType implements the parser.IndexedVarContainer interface.
func (s *selectNode) IndexedVarResolvedType(idx int) parser.Type {
	return s.sourceInfo[0].sourceColumns[idx].Typ
}

// IndexedVarString implements the parser.IndexedVarContainer interface.
func (s *selectNode) IndexedVarFormat(buf *bytes.Buffer, f parser.FmtFlags, idx int) {
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
		limit, err := p.Limit(limit)
		if err != nil {
			return nil, err
		}
		result := &selectTopNode{source: plan, sort: sort, limit: limit}
		limit.setTop(result)
		return result, nil
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
	s := &selectNode{planner: p}

	if err := s.initFrom(parsed, scanVisibility); err != nil {
		return nil, err
	}

	s.ivarHelper = parser.MakeIndexedVarHelper(s, len(s.sourceInfo[0].sourceColumns))

	if err := s.initTargets(parsed.Exprs, desiredTypes); err != nil {
		return nil, err
	}

	if err := s.initWhere(parsed.Where); err != nil {
		return nil, err
	}

	// NB: orderBy, window, and groupBy are passed and can modify the selectNode,
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

	if s.filter != nil && group != nil {
		// Allow the group-by to add an implicit "IS NOT NULL" filter.
		s.filter = group.isNotNullFilter(s.filter)
	}

	limitPlan, err := p.Limit(limit)
	if err != nil {
		return nil, err
	}
	distinctPlan := p.Distinct(parsed)

	result := &selectTopNode{
		source:   s,
		group:    group,
		window:   window,
		sort:     sort,
		distinct: distinctPlan,
		limit:    limitPlan,
	}
	s.top = result
	limitPlan.setTop(result)
	distinctPlan.setTop(result)

	return result, nil
}

func (s *selectNode) expandPlan() error {
	// Get the ordering for index selection (if any).
	var ordering sqlbase.ColumnOrdering
	var grouping bool

	if s.top.group != nil {
		ordering = s.top.group.desiredOrdering
		grouping = true
	} else if s.top.sort != nil {
		ordering = s.top.sort.Ordering().ordering
	}

	// Estimate the limit parameters. We can't full eval them just yet,
	// because evaluation requires running potential sub-queries, which
	// cannot occur during expandPlan.
	limitCount, limitOffset := s.top.limit.estimateLimit()

	if scan, ok := s.source.plan.(*scanNode); ok {
		// Find the set of columns that we actually need values for. This is an
		// optimization to avoid unmarshaling unnecessary values and is also
		// used for index selection.
		neededCols := make([]bool, len(s.source.info.sourceColumns))
		for i := range neededCols {
			neededCols[i] = s.ivarHelper.IndexedVarUsed(i)
		}
		scan.setNeededColumns(neededCols)

		// Compute a filter expression for the scan node.
		convFunc := func(expr parser.VariableExpr) (bool, parser.VariableExpr) {
			ivar := expr.(*parser.IndexedVar)
			s.ivarHelper.AssertSameContainer(ivar)
			return true, scan.filterVars.IndexedVar(ivar.Idx)
		}

		scan.filter, s.filter = splitFilter(s.filter, convFunc)
		if s.filter != nil {
			// Right now we support only one table, so the entire expression
			// should be converted.
			panic(fmt.Sprintf("residual filter `%s` (scan filter `%s`)", s.filter, scan.filter))
		}

		var analyzeOrdering analyzeOrderingFn
		if ordering != nil {
			analyzeOrdering = func(indexOrdering orderingInfo) (matchingCols, totalCols int) {
				selOrder := s.computeOrdering(indexOrdering)
				return computeOrderingMatch(ordering, selOrder, false), len(ordering)
			}
		}

		// If we have a reasonable limit, prefer an order matching index even if
		// it is not covering - unless we are grouping, in which case the limit
		// applies to the grouping results and not to the rows we scan.
		var preferOrderMatchingIndex bool
		if !grouping && len(ordering) > 0 && limitCount <= 1000-limitOffset {
			preferOrderMatchingIndex = true
		}

		plan, err := selectIndex(scan, analyzeOrdering, preferOrderMatchingIndex)
		if err != nil {
			return err
		}

		// Update s.source.info with the new plan.
		s.source.plan = plan
	}

	// Expand the source node. We need to do this before computing the
	// ordering, since expansion may modify the ordering.
	if err := s.source.plan.expandPlan(); err != nil {
		return err
	}

	s.ordering = s.computeOrdering(s.source.plan.Ordering())

	// Expand the sub-query plans in the local sub-expressions, if any.
	// This must be done for filters after index selection and splitting
	// the filter, since part of the filter may have landed in the source
	// scanNode and will be expanded there.
	if err := s.planner.expandSubqueryPlans(s.filter); err != nil {
		return err
	}
	for _, e := range s.render {
		if err := s.planner.expandSubqueryPlans(e); err != nil {
			return err
		}
	}

	return nil
}

// initFrom initializes the table node, given the parsed select expression
func (s *selectNode) initFrom(parsed *parser.SelectClause, scanVisibility scanVisibility) error {
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

func (s *selectNode) initTargets(targets parser.SelectExprs, desiredTypes []parser.Type) error {
	// Loop over the select expressions and expand them into the expressions
	// we're going to use to generate the returned column set and the names for
	// those columns.
	for i, target := range targets {
		var desiredType parser.Type
		if len(desiredTypes) > i {
			desiredType = desiredTypes[i]
		}
		if err := s.addRender(target, desiredType); err != nil {
			return err
		}
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

func (s *selectNode) initWhere(where *parser.Where) error {
	if where == nil {
		return nil
	}

	var err error
	s.filter, err = s.planner.analyzeExpr(where.Expr, s.sourceInfo, s.ivarHelper,
		parser.TypeBool, true, "WHERE")
	if err != nil {
		return err
	}

	// Make sure there are no aggregation/window functions in the filter
	// (after subqueries have been expanded).
	if err := s.planner.parser.AssertNoAggregationOrWindowing(
		s.filter, "WHERE", s.planner.session.SearchPath,
	); err != nil {
		return err
	}

	return nil
}

// checkRenderStar checks if the SelectExpr is either an
// UnqualifiedStar or an AllColumnsSelector. If so, we match the
// prefix of the name to one of the tables in the query and then
// expand the "*" into a list of columns. A ResultColumns and Expr
// pair is returned for each column.
func checkRenderStar(
	target parser.SelectExpr, src *dataSourceInfo, ivarHelper parser.IndexedVarHelper,
) (isStar bool, columns ResultColumns, exprs []parser.TypedExpr, err error) {
	v, ok := target.Expr.(parser.VarName)
	if !ok {
		return false, nil, nil, nil
	}
	switch v.(type) {
	case parser.UnqualifiedStar, *parser.AllColumnsSelector:
	default:
		return false, nil, nil, nil
	}

	if target.As != "" {
		return false, nil, nil, fmt.Errorf("\"%s\" cannot be aliased", v)
	}

	columns, exprs, err = src.expandStar(v, ivarHelper)
	return true, columns, exprs, err
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

func (s *selectNode) addRender(target parser.SelectExpr, desiredType parser.Type) error {
	// Pre-normalize any VarName so the work is not done twice below.
	if err := target.NormalizeTopLevelVarName(); err != nil {
		return err
	}

	if isStar, cols, typedExprs, err := checkRenderStar(target, s.source.info, s.ivarHelper); err != nil {
		return err
	} else if isStar {
		s.columns = append(s.columns, cols...)
		s.render = append(s.render, typedExprs...)
		s.isStar = true
		return nil
	}

	// When generating an output column name it should exactly match the original
	// expression, so determine the output column name before we perform any
	// manipulations to the expression.
	outputName := getRenderColName(target)

	normalized, err := s.planner.analyzeExpr(target.Expr, s.sourceInfo, s.ivarHelper, desiredType, false, "")
	if err != nil {
		return err
	}
	s.render = append(s.render, normalized)

	s.columns = append(s.columns, ResultColumn{Name: outputName, Typ: normalized.ResolvedType()})
	return nil
}

// renderRow renders the row by evaluating the render expressions.
func (s *selectNode) renderRow() error {
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
func (s *selectNode) findRenderIndexForCol(colIdx int) (idx int, ok bool) {
	for i, r := range s.render {
		if ivar, ok := r.(*parser.IndexedVar); ok && ivar.Idx == colIdx {
			return i, true
		}
	}
	return invalidColIdx, false
}

// Computes ordering information for the select node, given ordering information for the "from"
// node.
//
//    SELECT a, b FROM t@abc ...
//    	the ordering is: first by column 0 (a), then by column 1 (b)
//
//    SELECT a, b FROM t@abc WHERE a = 1 ...
//    	the ordering is: exact match column (a), ordered by column 1 (b)
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
func (s *selectNode) computeOrdering(fromOrder orderingInfo) orderingInfo {
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
