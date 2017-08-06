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

package sql

import (
	"bytes"
	"errors"
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	// populated by addOrReuseRenders()
	// as invoked initially by initTargets() and initWhere().
	// sortNode peeks into the render array defined by initTargets() as an optimization.
	// sortNode adds extra renderNode renders for sort columns not requested as select targets.
	// groupNode copies/extends the render array defined by initTargets() and
	// will add extra renderNode renders for the aggregation sources.
	// windowNode also adds additional renders for the window functions.
	render  []parser.TypedExpr
	columns sqlbase.ResultColumns

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

func (r *renderNode) Values() parser.Datums {
	return r.row
}

func (r *renderNode) Start(params runParams) error {
	return r.source.plan.Start(params)
}

func (r *renderNode) Next(params runParams) (bool, error) {
	if next, err := r.source.plan.Next(params); !next {
		return false, err
	}

	r.curSourceRow = r.source.plan.Values()

	err := r.renderRow()
	return err == nil, err
}

func (r *renderNode) Close(ctx context.Context) {
	r.source.plan.Close(ctx)
}

// IndexedVarEval implements the parser.IndexedVarContainer interface.
func (r *renderNode) IndexedVarEval(idx int, ctx *parser.EvalContext) (parser.Datum, error) {
	return r.curSourceRow[idx].Eval(ctx)
}

// IndexedVarResolvedType implements the parser.IndexedVarContainer interface.
func (r *renderNode) IndexedVarResolvedType(idx int) parser.Type {
	return r.sourceInfo[0].sourceColumns[idx].Typ
}

// IndexedVarString implements the parser.IndexedVarContainer interface.
func (r *renderNode) IndexedVarFormat(buf *bytes.Buffer, f parser.FmtFlags, idx int) {
	r.sourceInfo[0].FormatVar(buf, f, idx)
}

// Select selects rows from a SELECT/UNION/VALUES, ordering and/or limiting them.
func (p *planner) Select(
	ctx context.Context, n *parser.Select, desiredTypes []parser.Type,
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
		return p.SelectClause(ctx, s, orderBy, limit, desiredTypes, publicColumns)

	// TODO(dan): Union can also do optimizations when it has an ORDER BY, but
	// currently expects the ordering to be done externally, so we let it fall
	// through. Instead of continuing this special casing, it may be worth
	// investigating a general mechanism for passing some context down during
	// plan node construction.
	default:
		plan, err := p.newPlan(ctx, s, desiredTypes)
		if err != nil {
			return nil, err
		}
		sort, err := p.orderBy(ctx, orderBy, plan)
		if err != nil {
			return nil, err
		}
		if sort != nil {
			sort.plan = plan
			plan = sort
		}
		limit, err := p.Limit(ctx, limit)
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
	ctx context.Context,
	parsed *parser.SelectClause,
	orderBy parser.OrderBy,
	limit *parser.Limit,
	desiredTypes []parser.Type,
	scanVisibility scanVisibility,
) (planNode, error) {
	r := &renderNode{planner: p}

	if err := r.initFrom(ctx, parsed, scanVisibility); err != nil {
		return nil, err
	}

	var where *filterNode
	if parsed.Where != nil {
		var err error
		where, err = r.initWhere(ctx, parsed.Where.Expr)
		if err != nil {
			return nil, err
		}
	}

	r.ivarHelper = parser.MakeIndexedVarHelper(r, len(r.sourceInfo[0].sourceColumns))

	if err := r.initTargets(ctx, parsed.Exprs, desiredTypes); err != nil {
		return nil, err
	}

	// NB: orderBy, window, and groupBy are passed and can modify the renderNode,
	// but must do so in that order.
	sort, err := p.orderBy(ctx, orderBy, r)
	if err != nil {
		return nil, err
	}
	window, err := p.window(ctx, parsed, r)
	if err != nil {
		return nil, err
	}
	groupComplex, group, err := p.groupBy(ctx, parsed, r)
	if err != nil {
		return nil, err
	}

	if group != nil && group.requiresIsNotNullFilter() {
		if where == nil {
			var err error
			where, err = r.initWhere(ctx, nil)
			if err != nil {
				return nil, err
			}
		}
		group.addIsNotNullFilter(where, r)
	}

	limitPlan, err := p.Limit(ctx, limit)
	if err != nil {
		return nil, err
	}
	distinctPlan := p.Distinct(parsed)

	result := planNode(r)
	if groupComplex != nil {
		// group.plan is already r.
		result = groupComplex
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
func (r *renderNode) initFrom(
	ctx context.Context, parsed *parser.SelectClause, scanVisibility scanVisibility,
) error {
	// AS OF expressions should be handled by the executor.
	if parsed.From.AsOf.Expr != nil && !r.planner.avoidCachedDescriptors {
		return fmt.Errorf("unexpected AS OF SYSTEM TIME")
	}
	src, err := r.planner.getSources(ctx, parsed.From.Tables, scanVisibility)
	if err != nil {
		return err
	}
	r.source = src
	r.sourceInfo = multiSourceInfo{r.source.info}
	return nil
}

func (r *renderNode) initTargets(
	ctx context.Context, targets parser.SelectExprs, desiredTypes []parser.Type,
) error {
	// Loop over the select expressions and expand them into the expressions
	// we're going to use to generate the returned column set and the names for
	// those columns.
	for i, target := range targets {
		desiredType := parser.TypeAny
		if len(desiredTypes) > i {
			desiredType = desiredTypes[i]
		}

		// Output column names should exactly match the original expression, so we
		// have to determine the output column name before we rewrite SRFs below.
		outputName, err := getRenderColName(r.planner.session.SearchPath, target)
		if err != nil {
			return err
		}

		// If the current expression contains a set-returning function, we need to
		// move it up to the sources list as a cross join and add a render for the
		// function's column in the join.
		newTarget, err := r.rewriteSRFs(ctx, target)
		if err != nil {
			return err
		}

		cols, exprs, hasStar, err := r.planner.computeRenderAllowingStars(ctx, newTarget, desiredType,
			r.sourceInfo, r.ivarHelper, outputName)
		if err != nil {
			return err
		}

		r.isStar = r.isStar || hasStar
		_ = r.addOrReuseRenders(cols, exprs, false)
	}
	// `groupBy` or `orderBy` may internally add additional columns which we
	// do not want to include in validation of e.g. `GROUP BY 2`. We record the
	// current (initial) number of columns.
	r.numOriginalCols = len(r.columns)
	if len(r.render) != len(r.columns) {
		panic(fmt.Sprintf("%d renders but %d columns!", len(r.render), len(r.columns)))
	}
	return nil
}

// srfExtractionVisitor replaces the innermost set-returning function in an
// expression with an IndexedVar that points at a new index at the end of the
// ivarHelper. The extracted SRF is retained in the srf field.
//
// This visitor is intentionally limited to extracting only one SRF, because we
// don't support lateral correlated subqueries.
type srfExtractionVisitor struct {
	err        error
	srf        *parser.FuncExpr
	ivarHelper *parser.IndexedVarHelper
	searchPath parser.SearchPath
}

var _ parser.Visitor = &srfExtractionVisitor{}

func (v *srfExtractionVisitor) VisitPre(expr parser.Expr) (recurse bool, newNode parser.Expr) {
	_, isSubquery := expr.(*parser.Subquery)
	return !isSubquery, expr
}

func (v *srfExtractionVisitor) VisitPost(expr parser.Expr) parser.Expr {
	switch t := expr.(type) {
	case *parser.FuncExpr:
		fd, err := t.Func.Resolve(v.searchPath)
		if err != nil {
			v.err = err
			return expr
		}
		if _, ok := parser.Generators[fd.Name]; ok {
			if v.srf != nil {
				v.err = errors.New("cannot specify two set-returning functions in the same SELECT expression")
				return expr
			}
			v.srf = t
			return v.ivarHelper.IndexedVar(v.ivarHelper.AppendSlot())
		}
	}
	return expr
}

// rewriteSRFs creates data sources for any set-returning functions in the
// provided render expression, cross-joins these data sources with the
// renderNode's existing data sources, and returns a new render expression with
// the set-returning function replaced by an IndexedVar that points at the new
// data source.
//
// Expressions with more than one SRF require lateral correlated subqueries,
// which are not yet supported. For now, this function returns an error if more
// than one SRF is present in the render expression.
func (r *renderNode) rewriteSRFs(
	ctx context.Context, target parser.SelectExpr,
) (parser.SelectExpr, error) {
	// Walk the render expression looking for SRFs.
	v := &r.planner.srfExtractionVisitor
	*v = srfExtractionVisitor{
		err:        nil,
		srf:        nil,
		ivarHelper: &r.ivarHelper,
		searchPath: r.planner.session.SearchPath,
	}
	expr, _ := parser.WalkExpr(v, target.Expr)
	if v.err != nil {
		return target, v.err
	}

	// Return the original render expression unchanged if the srfExtractionVisitor
	// didn't find any SRFs.
	if v.srf == nil {
		return target, nil
	}

	// We rewrote exactly one SRF; cross-join it with our sources and return the
	// new render expression.
	src, err := r.planner.getDataSource(ctx, v.srf, nil, publicColumns)
	if err != nil {
		return target, err
	}
	src, err = r.planner.makeJoin(ctx, "CROSS JOIN", r.source, src, nil)
	if err != nil {
		return target, err
	}
	r.source = src
	r.sourceInfo = multiSourceInfo{r.source.info}

	return parser.SelectExpr{Expr: expr}, nil
}

func (r *renderNode) initWhere(ctx context.Context, whereExpr parser.Expr) (*filterNode, error) {
	f := &filterNode{source: r.source}
	f.ivarHelper = parser.MakeIndexedVarHelper(f, len(r.sourceInfo[0].sourceColumns))

	if whereExpr != nil {
		var err error
		f.filter, err = r.planner.analyzeExpr(ctx, whereExpr, r.sourceInfo, f.ivarHelper,
			parser.TypeBool, true, "WHERE")
		if err != nil {
			return nil, err
		}

		// Make sure there are no aggregation/window functions in the filter
		// (after subqueries have been expanded).
		if err := r.planner.parser.AssertNoAggregationOrWindowing(
			f.filter, "WHERE", r.planner.session.SearchPath,
		); err != nil {
			return nil, err
		}
	}

	// Insert the newly created filterNode between the renderNode and
	// its original FROM source.
	f.source = r.source
	r.source.plan = f

	return f, nil
}

// getRenderColName returns the output column name for a render expression.
func getRenderColName(searchPath parser.SearchPath, target parser.SelectExpr) (string, error) {
	if target.As != "" {
		return string(target.As), nil
	}

	// If the expression designates a column, try to reuse that column's name
	// as render name.
	if err := target.NormalizeTopLevelVarName(); err != nil {
		return "", err
	}

	// If target.Expr is a funcExpr, resolving the function within will normalize
	// target.Expr's string representation. We want the output column name to be
	// unnormalized, so we compute target.Expr's string representation now, even
	// though we may choose to return something other than exprStr in the switch
	// below.
	exprStr := target.Expr.String()

	switch t := target.Expr.(type) {
	case *parser.ColumnItem:
		// We only shorten the name of the result column to become the
		// unqualified column part of this expr name if there is
		// no additional subscript on the column.
		if len(t.Selector) == 0 {
			return t.Column(), nil
		}

	// For compatibility with Postgres, a render expression rooted by a
	// set-returning function is named after that SRF.
	case *parser.FuncExpr:
		fd, err := t.Func.Resolve(searchPath)
		if err != nil {
			return "", err
		}
		if _, ok := parser.Generators[fd.Name]; ok {
			return fd.Name, nil
		}
	}

	return exprStr, nil
}

// appendRenderColumn adds a new render expression at the end of the current list.
// The expression must be normalized already.
func (r *renderNode) addRenderColumn(expr parser.TypedExpr, col sqlbase.ResultColumn) {
	r.render = append(r.render, expr)
	r.columns = append(r.columns, col)
}

// resetRenderColumns resets all the render expressions. This is used e.g. by
// aggregation and windowing (see group.go / window.go). The method also
// asserts that both the render and columns array have the same size.
func (r *renderNode) resetRenderColumns(exprs []parser.TypedExpr, cols sqlbase.ResultColumns) {
	if len(exprs) != len(cols) {
		panic(fmt.Sprintf("resetRenderColumns used with arrays of different sizes: %d != %d", len(exprs), len(cols)))
	}
	r.render = exprs
	r.columns = cols
}

// renderRow renders the row by evaluating the render expressions.
func (r *renderNode) renderRow() error {
	if r.row == nil {
		r.row = make([]parser.Datum, len(r.render))
	}
	for i, e := range r.render {
		var err error
		r.row[i], err = e.Eval(&r.planner.evalCtx)
		if err != nil {
			return err
		}
	}
	return nil
}

// Searches for a render target that matches the given column reference.
func (r *renderNode) findRenderIndexForCol(colIdx int) (idx int, ok bool) {
	for i, r := range r.render {
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
func (r *renderNode) computeOrdering(fromOrder orderingInfo) {
	r.ordering = orderingInfo{}

	// Detect constants.
	for col, expr := range r.render {
		_, hasRowDependentValues, err := r.resolveNames(expr)
		if err != nil {
			// If we get an error here, the expression must contain an unresolved name
			// or invalid indexed var; ignore.
			continue
		}
		if !hasRowDependentValues && !r.columns[col].Omitted {
			r.ordering.addConstantColumn(col)
		}
	}

	// See if any of the constant columns have render targets. We can ignore any columns that
	// don't have render targets. For example, assume we are using an ascending index on (k, v) with
	// the query:
	//
	//   SELECT v FROM t WHERE k = 1
	//
	// The rows from the index are ordered by k then by v, but since k is a
	// constant column the results are also ordered just by v.
	if !fromOrder.constantCols.Empty() {
		fromOrder.constantCols.ForEach(func(colIdx uint32) {
			if renderIdx, ok := r.findRenderIndexForCol(int(colIdx)); ok {
				r.ordering.addConstantColumn(renderIdx)
			}
		})
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
	for _, group := range fromOrder.ordering {
		var colsWithTargets util.FastIntSet
		for col, ok := group.cols.Next(0); ok; col, ok = group.cols.Next(col + 1) {
			if renderIdx, ok := r.findRenderIndexForCol(int(col)); ok {
				colsWithTargets.Add(uint32(renderIdx))
			}
		}
		if colsWithTargets.Empty() {
			// This group has no output columns we can refer to in the ordering; we
			// have to stop here.
			return
		}
		r.ordering.addColumnGroup(colsWithTargets, group.dir)
	}
	// We added all columns in fromOrder; we can copy the unique flag.
	r.ordering.isKey = fromOrder.isKey
}
