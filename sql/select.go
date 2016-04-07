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
	"fmt"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
)

// tableInfo contains the information for table used by a select statement. It can be an actual
// table, or a "virtual table" which is the result of a subquery.
type tableInfo struct {
	// node which can be used to retrieve the data (normally a scanNode). For performance purposes,
	// this node can be aware of the filters, grouping etc.
	node planNode

	// alias (if no alias is given and the source is a table, this is the table name)
	alias string

	// resultColumns which match the node.Columns() 1-to-1. However the column names might be
	// different if the statement renames them using AS.
	columns []ResultColumn
}

// selectNode encapsulates the core logic of a select statement: retrieving filtered results from
// the sources. Grouping, sorting, distinct and limits are implemented on top of this node (as
// wrappers), though these are taken into consideration at the selectNode level for optimization
// (e.g. index selection).
type selectNode struct {
	planner *planner

	table tableInfo

	// Map of qvalues encountered in expressions.
	qvals qvalMap

	pErr *roachpb.Error

	// Rendering expressions for rows and corresponding output columns.
	render  []parser.Expr
	columns []ResultColumn

	// The rendered row, with one value for each render expression.
	row parser.DTuple

	// Filtering expression for rows.
	filter parser.Expr

	// The number of initial columns - before adding any internal render targets for grouping or
	// ordering. The original columns are columns[:numOriginalCols], the internally added ones are
	// columns[numOriginalCols:].
	numOriginalCols int

	explain   explainMode
	debugVals debugValues

	ordering orderingInfo
}

func (s *selectNode) Columns() []ResultColumn {
	if s.explain == explainDebug {
		return debugColumns
	}
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
	s.table.node.MarkDebug(mode)
}

func (s *selectNode) DebugValues() debugValues {
	if s.explain != explainDebug {
		panic(fmt.Sprintf("node not in debug mode (mode %d)", s.explain))
	}
	return s.debugVals
}

func (s *selectNode) Next() bool {
	for {
		if !s.table.node.Next() {
			return false
		}

		if s.explain == explainDebug {
			s.debugVals = s.table.node.DebugValues()

			if s.debugVals.output != debugValueRow {
				// Let the debug values pass through.
				return true
			}
		}
		row := s.table.node.Values()
		s.qvals.populateQVals(row)
		passesFilter, err := runFilter(s.filter, s.planner.evalCtx)
		if err != nil {
			s.pErr = roachpb.NewError(err)
			return false
		}

		if passesFilter {
			s.renderRow()
			return true
		} else if s.explain == explainDebug {
			// Mark the row as filtered out.
			s.debugVals.output = debugValueFiltered
			return true
		}
		// Row was filtered out; grab the next row.
	}
}

func (s *selectNode) PErr() *roachpb.Error {
	if s.pErr != nil {
		return s.pErr
	}
	return s.table.node.PErr()
}

func (s *selectNode) ExplainPlan() (name, description string, children []planNode) {
	return s.table.node.ExplainPlan()
}

func (s *selectNode) SetLimitHint(numRows int64, soft bool) {
	s.table.node.SetLimitHint(numRows, soft || s.filter != nil)
}

// Select selects rows from a SELECT/UNION/VALUES, ordering and/or limiting them.
func (p *planner) Select(n *parser.Select, autoCommit bool) (planNode, *roachpb.Error) {
	wrapped := n.Select
	limit := n.Limit
	orderBy := n.OrderBy

	for s, ok := wrapped.(*parser.ParenSelect); ok; s, ok = wrapped.(*parser.ParenSelect) {
		wrapped = s.Select.Select
		if s.Select.OrderBy != nil {
			if orderBy != nil {
				return nil, roachpb.NewUErrorf("multiple ORDER BY clauses not allowed")
			}
			orderBy = s.Select.OrderBy
		}
		if s.Select.Limit != nil {
			if limit != nil {
				return nil, roachpb.NewUErrorf("multiple LIMIT clauses not allowed")
			}
			limit = s.Select.Limit
		}
	}

	switch s := wrapped.(type) {
	case *parser.SelectClause:
		// Select can potentially optimize index selection if it's being ordered,
		// so we allow it to do its own sorting.
		node := &selectNode{planner: p}
		return p.initSelect(node, s, orderBy, limit)
	// TODO(dan): Union can also do optimizations when it has an ORDER BY, but
	// currently expects the ordering to be done externally, so we let it fall
	// through. Instead of continuing this special casing, it may be worth
	// investigating a general mechanism for passing some context down during
	// plan node construction.
	default:
		plan, pErr := p.makePlan(s, autoCommit)
		if pErr != nil {
			return nil, pErr
		}
		sort, pErr := p.orderBy(orderBy, plan)
		if pErr != nil {
			return nil, pErr
		}
		count, offset, err := p.evalLimit(limit)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		return p.limit(count, offset, sort.wrap(plan)), nil
	}
}

// SelectClause selects rows from a single table. Select is the workhorse of the
// SQL statements. In the slowest and most general case, select must perform
// full table scans across multiple tables and sort and join the resulting rows
// on arbitrary columns. Full table scans can be avoided when indexes can be
// used to satisfy the where-clause.
//
// NB: This is passed directly to planNode only when there is no ORDER BY,
// LIMIT, or parenthesis in the parsed SELECT. See `sql/parser.Select` and
// `sql/parser.SelectStatement`.
//
// Privileges: SELECT on table
//   Notes: postgres requires SELECT. Also requires UPDATE on "FOR UPDATE".
//          mysql requires SELECT.
func (p *planner) SelectClause(parsed *parser.SelectClause) (planNode, *roachpb.Error) {
	node := &selectNode{planner: p}
	return p.initSelect(node, parsed, nil, nil)
}

func (p *planner) initSelect(
	s *selectNode, parsed *parser.SelectClause, orderBy parser.OrderBy, limit *parser.Limit,
) (planNode, *roachpb.Error) {

	s.qvals = make(qvalMap)

	if pErr := s.initFrom(p, parsed); pErr != nil {
		return nil, pErr
	}

	if pErr := s.initTargets(parsed.Exprs); pErr != nil {
		return nil, pErr
	}

	if pErr := s.initWhere(parsed.Where); pErr != nil {
		return nil, pErr
	}

	// NB: both orderBy and groupBy are passed and can modify the selectNode but orderBy must do so first.
	sort, pErr := p.orderBy(orderBy, s)
	if pErr != nil {
		return nil, pErr
	}
	group, pErr := p.groupBy(parsed, s)
	if pErr != nil {
		return nil, pErr
	}

	if s.filter != nil && group != nil {
		// Allow the group-by to add an implicit "IS NOT NULL" filter.
		s.filter = group.isNotNullFilter(s.filter)
	}

	// Get the ordering for index selection (if any).
	var ordering columnOrdering
	var grouping bool

	if group != nil {
		ordering = group.desiredOrdering
		grouping = true
	} else if sort != nil {
		ordering = sort.Ordering().ordering
	}

	limitCount, limitOffset, err := p.evalLimit(limit)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	if scan, ok := s.table.node.(*scanNode); ok {
		// Find the set of columns that we actually need values for. This is an
		// optimization to avoid unmarshaling unnecessary values and is also
		// used for index selection.
		neededCols := make([]bool, len(s.table.columns))
		for i := range neededCols {
			_, ok := s.qvals[columnRef{&s.table, i}]
			neededCols[i] = ok
		}
		scan.setNeededColumns(neededCols)

		// If we are only preparing, the filter expression can contain
		// unexpanded subqueries which are not supported by splitFilter.
		if !p.evalCtx.PrepareOnly {
			// Compute a filter expression for the scan node.
			convFunc := func(expr parser.VariableExpr) (bool, parser.VariableExpr) {
				qval := expr.(*qvalue)
				if qval.colRef.table != &s.table {
					// TODO(radu): when we will support multiple tables, this
					// will be a valid case.
					panic("scan qvalue refers to unknown table")
				}
				return true, scan.getQValue(qval.colRef.colIdx)
			}

			scan.filter, s.filter = splitFilter(s.filter, convFunc)
			if s.filter != nil {
				// Right now we support only one table, so the entire expression
				// should be converted.
				panic(fmt.Sprintf("residual filter `%s` (scan filter `%s`)", s.filter, scan.filter))
			}
		}

		var analyzeOrdering analyzeOrderingFn
		if grouping && len(ordering) == 1 && s.filter == nil {
			// If grouping has a desired single-column order and the index
			// matches that order, we can limit the scan to a single key.
			analyzeOrdering =
				func(indexOrdering orderingInfo) (matchingCols, totalCols int, singleKey bool) {
					selOrder := s.computeOrdering(indexOrdering)
					matchingCols = computeOrderingMatch(ordering, selOrder, false)
					return matchingCols, 1, matchingCols == 1
				}
		} else if ordering != nil {
			analyzeOrdering =
				func(indexOrdering orderingInfo) (matchingCols, totalCols int, singleKey bool) {
					selOrder := s.computeOrdering(indexOrdering)
					return computeOrderingMatch(ordering, selOrder, false), len(ordering), false
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
			return nil, roachpb.NewError(err)
		}

		// Update s.table with the new plan.
		s.table.node = plan
	}

	s.ordering = s.computeOrdering(s.table.node.Ordering())

	// Wrap this node as necessary.
	return p.limit(limitCount, limitOffset, p.distinct(parsed, sort.wrap(group.wrap(s)))), nil
}

// initFrom initializes the table node, given the parsed select expression
func (s *selectNode) initFrom(p *planner, parsed *parser.SelectClause) *roachpb.Error {
	from := parsed.From
	var colAlias parser.NameList
	switch len(from) {
	case 0:
		s.table.node = &emptyNode{results: true}

	case 1:
		ate, ok := from[0].(*parser.AliasedTableExpr)
		if !ok {
			return roachpb.NewErrorf("TODO(pmattis): unsupported FROM: %s", from)
		}

		switch expr := ate.Expr.(type) {
		case *parser.QualifiedName:
			// Usual case: a table.
			scan := &scanNode{planner: p, txn: p.txn}
			s.table.alias, s.pErr = scan.initTable(p, expr, ate.Hints)
			if s.pErr != nil {
				return s.pErr
			}
			s.table.node = scan

		case *parser.Subquery:
			// We have a subquery (this includes a simple "VALUES").
			if ate.As.Alias == "" {
				return roachpb.NewErrorf("subquery in FROM must have an alias")
			}

			s.table.node, s.pErr = p.makePlan(expr.Select, false)
			if s.pErr != nil {
				return s.pErr
			}

		default:
			return roachpb.NewErrorf("TODO(pmattis): unsupported FROM: %s", from)
		}

		if ate.As.Alias != "" {
			// If an alias was specified, use that.
			s.table.alias = string(ate.As.Alias)
		}
		colAlias = ate.As.Cols
	default:
		s.pErr = roachpb.NewErrorf("TODO(pmattis): unsupported FROM: %s", from)
		return s.pErr
	}

	s.table.columns = s.table.node.Columns()
	if len(colAlias) > 0 {
		// Make a copy of the slice since we are about to modify the contents.
		s.table.columns = append([]ResultColumn(nil), s.table.columns...)

		// The column aliases can only refer to explicit columns.
		for colIdx, aliasIdx := 0, 0; aliasIdx < len(colAlias); colIdx++ {
			if colIdx >= len(s.table.columns) {
				return roachpb.NewErrorf(
					"table \"%s\" has %d columns available but %d columns specified",
					s.table.alias, aliasIdx, len(colAlias))
			}
			if s.table.columns[colIdx].hidden {
				continue
			}
			s.table.columns[colIdx].Name = string(colAlias[aliasIdx])
			aliasIdx++
		}
	}
	return nil
}

func (s *selectNode) initTargets(targets parser.SelectExprs) *roachpb.Error {
	// Loop over the select expressions and expand them into the expressions
	// we're going to use to generate the returned column set and the names for
	// those columns.
	for _, target := range targets {
		if s.pErr = s.addRender(target); s.pErr != nil {
			return s.pErr
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

func (s *selectNode) initWhere(where *parser.Where) *roachpb.Error {
	if where == nil {
		return nil
	}
	var err error
	s.filter, err = s.resolveQNames(where.Expr)
	s.pErr = roachpb.NewError(err)
	if s.pErr != nil {
		return s.pErr
	}

	whereType, err := s.filter.TypeCheck(s.planner.evalCtx.Args)
	if err != nil {
		s.pErr = roachpb.NewError(err)
		return s.pErr
	}
	if !(whereType.TypeEqual(parser.DummyBool) || whereType == parser.DNull) {
		s.pErr = roachpb.NewUErrorf("argument of WHERE must be type %s, not type %s",
			parser.DummyBool.Type(), whereType.Type())
		return s.pErr
	}

	// Normalize the expression (this will also evaluate any branches that are
	// constant).
	s.filter, err = s.planner.parser.NormalizeExpr(s.planner.evalCtx, s.filter)
	if err != nil {
		s.pErr = roachpb.NewError(err)
		return s.pErr
	}
	s.filter, s.pErr = s.planner.expandSubqueries(s.filter, 1)
	if s.pErr != nil {
		return s.pErr
	}

	// Make sure there are no aggregation functions in the filter (after subqueries have been
	// expanded).
	if s.planner.aggregateInExpr(s.filter) {
		s.pErr = roachpb.NewUErrorf("aggregate functions are not allowed in WHERE")
		return s.pErr
	}

	return nil
}

// checkRenderStar checks if the SelectExpr is a QualifiedName with a StarIndirection suffix. If so,
// we match the prefix of the qualified name to one of the tables in the query and then expand the
// "*" into a list of columns. The qvalMap is updated to include all the relevant columns. A
// ResultColumns and Expr pair is returned for each column.
func checkRenderStar(
	target parser.SelectExpr, table *tableInfo, qvals qvalMap,
) (isStar bool, columns []ResultColumn, exprs []parser.Expr, err error) {
	qname, ok := target.Expr.(*parser.QualifiedName)
	if !ok {
		return false, nil, nil, nil
	}
	if err := qname.NormalizeColumnName(); err != nil {
		return false, nil, nil, err
	}
	if !qname.IsStar() {
		return false, nil, nil, nil
	}

	if table.alias == "" {
		return false, nil, nil, fmt.Errorf("\"%s\" with no tables specified is not valid", qname)
	}
	if target.As != "" {
		return false, nil, nil, fmt.Errorf("\"%s\" cannot be aliased", qname)
	}

	// TODO(radu): support multiple FROMs, consolidate with logic in findColumn
	if tableName := qname.Table(); tableName != "" && !equalName(table.alias, tableName) {
		return false, nil, nil, fmt.Errorf("table \"%s\" not found", tableName)
	}

	for idx, col := range table.columns {
		if col.hidden {
			continue
		}
		qval := qvals.getQVal(columnRef{table, idx})
		columns = append(columns, ResultColumn{Name: col.Name, Typ: qval.datum})
		exprs = append(exprs, qval)
	}
	return true, columns, exprs, nil
}

// getRenderColName returns the output column name for a render expression.
// The expression cannot be a star.
func getRenderColName(target parser.SelectExpr) string {
	if target.As != "" {
		return string(target.As)
	}
	if qname, ok := target.Expr.(*parser.QualifiedName); ok {
		return qname.Column()
	}
	return target.Expr.String()
}

func (s *selectNode) addRender(target parser.SelectExpr) *roachpb.Error {
	// outputName will be empty if the target is not aliased.
	outputName := string(target.As)

	if isStar, cols, exprs, err := checkRenderStar(target, &s.table, s.qvals); err != nil {
		s.pErr = roachpb.NewError(err)
		return s.pErr
	} else if isStar {
		s.columns = append(s.columns, cols...)
		s.render = append(s.render, exprs...)
		return nil
	}

	// When generating an output column name it should exactly match the original
	// expression, so determine the output column name before we perform any
	// manipulations to the expression.
	outputName = getRenderColName(target)

	// Resolve qualified names. This has the side-effect of normalizing any
	// qualified name found.
	var resolved parser.Expr
	var err error
	if resolved, err = s.resolveQNames(target.Expr); err != nil {
		s.pErr = roachpb.NewError(err)
		return s.pErr
	}
	if resolved, s.pErr = s.planner.expandSubqueries(resolved, 1); s.pErr != nil {
		return s.pErr
	}
	var typ parser.Datum
	typ, err = resolved.TypeCheck(s.planner.evalCtx.Args)
	s.pErr = roachpb.NewError(err)
	if s.pErr != nil {
		return s.pErr
	}
	var normalized parser.Expr
	normalized, err = s.planner.parser.NormalizeExpr(s.planner.evalCtx, resolved)
	s.pErr = roachpb.NewError(err)
	if s.pErr != nil {
		return s.pErr
	}
	s.render = append(s.render, normalized)

	if target.As == "" {
		switch t := target.Expr.(type) {
		case *parser.QualifiedName:
			// If the expression is a qualified name, use the column name, not the
			// full qualification as the column name to return.
			outputName = t.Column()
		}
	}
	s.columns = append(s.columns, ResultColumn{Name: outputName, Typ: typ})
	return nil
}

// renderRow renders the row by evaluating the render expressions. Assumes the qvals have been
// populated with the current row. May set n.pErr if an error occurs during expression evaluation.
func (s *selectNode) renderRow() {
	if s.row == nil {
		s.row = make([]parser.Datum, len(s.render))
	}
	for i, e := range s.render {
		var err error
		s.row[i], err = e.Eval(s.planner.evalCtx)
		s.pErr = roachpb.NewError(err)
		if s.pErr != nil {
			return
		}
	}
}

// Searches for a render target that matches the given column reference.
func (s *selectNode) findRenderIndexForCol(colRef columnRef) (idx int, ok bool) {
	for i, r := range s.render {
		if qval, ok := r.(*qvalue); ok && qval.colRef == colRef {
			return i, true
		}
	}
	return -1, false
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
		colRef := columnRef{&s.table, colIdx}
		if renderIdx, ok := s.findRenderIndexForCol(colRef); ok {
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
		colRef := columnRef{&s.table, colOrder.colIdx}
		renderIdx, ok := s.findRenderIndexForCol(colRef)
		if !ok {
			return ordering
		}
		ordering.addColumn(renderIdx, colOrder.direction)
	}
	// We added all columns in fromOrder; we can copy the distinct flag.
	ordering.unique = fromOrder.unique
	return ordering
}
