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
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// renderNode encapsulates the render logic of a select statement:
// expressing new values using expressions over source values.
type renderNode struct {
	// source describes where the data is coming from.
	// populated initially by initFrom().
	// potentially modified by index selection.
	source planDataSource

	// sourceInfo contains the reference to the DataSourceInfo in the
	// source planDataSource that is needed for name resolution.
	// We keep one instance of multiSourceInfo cached here so as to avoid
	// re-creating it every time analyzeExpr() is called in computeRender().
	sourceInfo sqlbase.MultiSourceInfo

	// Helper for indexed vars. This holds the actual instances of
	// IndexedVars replaced in Exprs. The indexed vars contain indices
	// to the array of source columns.
	ivarHelper tree.IndexedVarHelper

	// Rendering expressions for rows and corresponding output columns.
	// populated by addOrReuseRenders()
	// as invoked initially by initTargets() and initWhere().
	// sortNode peeks into the render array defined by initTargets() as an optimization.
	// sortNode adds extra renderNode renders for sort columns not requested as select targets.
	// groupNode copies/extends the render array defined by initTargets() and
	// will add extra renderNode renders for the aggregation sources.
	// windowNode also adds additional renders for the window functions.
	render []tree.TypedExpr

	// renderStrings stores the symbolic representations of the expressions in
	// render, in the same order. It's used to prevent recomputation of the
	// symbolic representations.
	renderStrings []string

	// columns is the set of result columns.
	columns sqlbase.ResultColumns

	// The number of initial columns - before adding any internal render
	// targets for grouping, filtering or ordering. The original columns
	// are columns[:numOriginalCols], the internally added ones are
	// columns[numOriginalCols:].
	// populated by initTargets(), which thus must be obviously called before initWhere()
	// and the other initializations that may add render columns.
	numOriginalCols int

	// ordering indicates the order of returned rows.
	// initially suggested by the GROUP BY and ORDER BY clauses;
	// modified by index selection.
	props physicalProps

	run renderRun

	// This struct must be allocated on the heap and its location stay
	// stable after construction because it implements
	// IndexedVarContainer and the IndexedVar objects in sub-expressions
	// will link to it by reference after checkRenderStar / analyzeExpr.
	// Enforce this using NoCopy.
	noCopy util.NoCopy
}

// Select selects rows from a SELECT/UNION/VALUES, ordering and/or limiting them.
func (p *planner) Select(
	ctx context.Context, n *tree.Select, desiredTypes []types.T,
) (planNode, error) {
	wrapped := n.Select
	limit := n.Limit
	orderBy := n.OrderBy
	with := n.With

	for s, ok := wrapped.(*tree.ParenSelect); ok; s, ok = wrapped.(*tree.ParenSelect) {
		wrapped = s.Select.Select
		if s.Select.OrderBy != nil {
			if orderBy != nil {
				return nil, pgerror.NewErrorf(
					pgerror.CodeSyntaxError, "multiple ORDER BY clauses not allowed",
				)
			}
			orderBy = s.Select.OrderBy
		}
		if s.Select.Limit != nil {
			if limit != nil {
				return nil, pgerror.NewErrorf(
					pgerror.CodeSyntaxError, "multiple LIMIT clauses not allowed",
				)
			}
			limit = s.Select.Limit
		}
	}

	switch s := wrapped.(type) {
	case *tree.SelectClause:
		// Select can potentially optimize index selection if it's being ordered,
		// so we allow it to do its own sorting.
		return p.SelectClause(ctx, s, orderBy, limit, with, desiredTypes, publicColumns)

	// TODO(dan): Union can also do optimizations when it has an ORDER BY, but
	// currently expects the ordering to be done externally, so we let it fall
	// through. Instead of continuing this special casing, it may be worth
	// investigating a general mechanism for passing some context down during
	// plan node construction.
	// TODO(jordan): this limitation also applies to CTEs, which do not yet
	// propagate into VALUES and UNION clauses
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
// LIMIT, or parenthesis in the parsed SELECT. See `sql/tree.Select` and
// `sql/tree.SelectStatement`.
//
// Privileges: SELECT on table
//   Notes: postgres requires SELECT. Also requires UPDATE on "FOR UPDATE".
//          mysql requires SELECT.
func (p *planner) SelectClause(
	ctx context.Context,
	parsed *tree.SelectClause,
	orderBy tree.OrderBy,
	limit *tree.Limit,
	with *tree.With,
	desiredTypes []types.T,
	scanVisibility scanVisibility,
) (planNode, error) {
	r := &renderNode{}

	resetter, err := p.initWith(ctx, with)
	if err != nil {
		return nil, err
	}
	if resetter != nil {
		defer resetter(p)
	}

	if err := p.initFrom(ctx, r, parsed, scanVisibility); err != nil {
		return nil, err
	}

	var where *filterNode
	if parsed.Where != nil {
		var err error
		where, err = p.initWhere(ctx, r, parsed.Where.Expr)
		if err != nil {
			return nil, err
		}
	}

	if err := p.initTargets(ctx, r, parsed.Exprs, desiredTypes); err != nil {
		return nil, err
	}

	// NB: orderBy, window, and groupBy are passed and can modify the
	// renderNode, but must do so in that order.
	// Note that this order is exactly the reverse of the order of how the
	// plan is constructed i.e. a logical plan tree might look like
	// (renderNodes omitted)
	//  distinctNode
	//       |
	//       |
	//       v
	//    sortNode
	//       |
	//       |
	//       v
	//   windowNode
	//       |
	//       |
	//       v
	//   groupNode
	//       |
	//       |
	//       v
	distinctComplex, distinct, err := p.distinct(ctx, parsed, r)
	if err != nil {
		return nil, err
	}
	sort, err := p.orderBy(ctx, orderBy, r)
	if err != nil {
		return nil, err
	}

	// For DISTINCT ON expressions either one of the following must be
	// satisfied:
	//    - DISTINCT ON expressions is a subset of a prefix of the ORDER BY
	//	expressions.
	//	    e.g.  SELECT DISTINCT ON (b, a) ... ORDER BY a, b, c
	//    - DISTINCT ON expressions includes all ORDER BY expressions.
	//	    e.g.  SELECT DISTINCT ON (b, a) ... ORDER BY a
	if distinct != nil && sort != nil && !distinct.distinctOnColIdxs.Empty() {
		numDistinctExprs := distinct.distinctOnColIdxs.Len()
		for i, order := range sort.ordering {
			// DISTINCT ON contains all ORDER BY expressions.
			// Continue.
			if i >= numDistinctExprs {
				break
			}

			if !distinct.distinctOnColIdxs.Contains(order.ColIdx) {
				return nil, pgerror.NewErrorf(
					pgerror.CodeSyntaxError,
					"SELECT DISTINCT ON expressions must be a prefix of or include all ORDER BY expressions",
				)
			}
		}
	}

	window, err := p.window(ctx, parsed, r)
	if err != nil {
		return nil, err
	}
	groupComplex, group, err := p.groupBy(ctx, parsed, r)
	if err != nil {
		return nil, err
	}

	if group != nil && group.requiresIsDistinctFromNullFilter() {
		if where == nil {
			var err error
			where, err = p.initWhere(ctx, r, nil)
			if err != nil {
				return nil, err
			}
		}
		group.addIsDistinctFromNullFilter(where, r)
	}

	limitPlan, err := p.Limit(ctx, limit)
	if err != nil {
		return nil, err
	}

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
	if distinctComplex != nil && distinct != nil {
		distinct.plan = result
		result = distinctComplex
	}
	if limitPlan != nil {
		limitPlan.plan = result
		result = limitPlan
	}
	return result, nil
}

// IndexedVarEval implements the tree.IndexedVarContainer interface.
func (r *renderNode) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	return r.run.curSourceRow[idx].Eval(ctx)
}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (r *renderNode) IndexedVarResolvedType(idx int) types.T {
	return r.sourceInfo[0].SourceColumns[idx].Typ
}

// IndexedVarNodeFormatter implements the tree.IndexedVarContainer interface.
func (r *renderNode) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return r.sourceInfo[0].NodeFormatter(idx)
}

// renderRun contains the run-time state of renderNode during local execution.
type renderRun struct {
	// The current source row, with one value per source column.
	// populated by Next(), used by renderRow().
	curSourceRow tree.Datums

	// The rendered row, with one value for each render expression.
	// populated by Next().
	row tree.Datums
}

func (r *renderNode) Next(params runParams) (bool, error) {
	if next, err := r.source.plan.Next(params); !next {
		return false, err
	}

	r.run.curSourceRow = r.source.plan.Values()

	err := r.renderRow(params.EvalContext())
	return err == nil, err
}

func (r *renderNode) Values() tree.Datums       { return r.run.row }
func (r *renderNode) Close(ctx context.Context) { r.source.plan.Close(ctx) }

// initFrom initializes the table node, given the parsed select expression
func (p *planner) initFrom(
	ctx context.Context, r *renderNode, parsed *tree.SelectClause, scanVisibility scanVisibility,
) error {
	_, _, err := p.getTimestamp(parsed.From.AsOf)
	if err != nil {
		return err
	}

	src, err := p.getSources(ctx, parsed.From.Tables, scanVisibility)
	if err != nil {
		return err
	}
	r.source = src
	r.sourceInfo = sqlbase.MultiSourceInfo{r.source.info}
	return nil
}

func (p *planner) initTargets(
	ctx context.Context, r *renderNode, targets tree.SelectExprs, desiredTypes []types.T,
) error {
	// We need to rewrite the SRFs first thing, because the ivarHelper
	// initialized below needs to know the final shape of the FROM clause,
	// after SRF rewrites has been processed.
	var err error
	targets, err = p.rewriteSRFs(ctx, r, targets)
	if err != nil {
		return err
	}

	r.ivarHelper = tree.MakeIndexedVarHelper(r, len(r.source.info.SourceColumns))

	// Loop over the select expressions and expand them into the expressions
	// we're going to use to generate the returned column set and the names for
	// those columns.
	for i, target := range targets {
		desiredType := types.Any
		if len(desiredTypes) > i {
			desiredType = desiredTypes[i]
		}

		// Output column names should exactly match the original expression, so we
		// have to determine the output column name before we rewrite SRFs below.
		outputName, err := getRenderColName(p.SessionData().SearchPath, target, &r.ivarHelper)
		if err != nil {
			return err
		}

		cols, exprs, hasStar, err := p.computeRenderAllowingStars(ctx, target, desiredType,
			r.sourceInfo, r.ivarHelper, outputName)
		if err != nil {
			return err
		}

		p.curPlan.hasStar = p.curPlan.hasStar || hasStar
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

// insertRender creates a new renderNode that renders exactly its
// source plan.
func (p *planner) insertRender(
	ctx context.Context, plan planNode, tn *tree.TableName,
) (*renderNode, error) {
	src := planDataSource{
		info: sqlbase.NewSourceInfoForSingleTable(*tn, planColumns(plan)),
		plan: plan,
	}
	render := &renderNode{
		source:     src,
		sourceInfo: sqlbase.MultiSourceInfo{src.info},
	}
	if err := p.initTargets(ctx, render,
		tree.SelectExprs{tree.SelectExpr{Expr: tree.StarExpr()}},
		nil /*desiredTypes*/); err != nil {
		return nil, err
	}
	return render, nil
}

// getTimestamp will get the timestamp for an AS OF clause. It will also
// verify the timestamp against the transaction. If AS OF SYSTEM TIME is
// specified in any part of the query, then it must be consistent with
// what is known to the Executor. If the AsOfClause contains a
// timestamp, then true will be returned.
func (p *planner) getTimestamp(asOf tree.AsOfClause) (hlc.Timestamp, bool, error) {
	if asOf.Expr != nil {
		// At this point, the executor only knows how to recognize AS OF
		// SYSTEM TIME at the top level. When it finds it there,
		// p.asOfSystemTime is set. If AS OF SYSTEM TIME wasn't found
		// there, we cannot accept it anywhere else either.
		// TODO(anyone): this restriction might be lifted if we support
		// table readers at arbitrary timestamps, and each FROM clause
		// can have its own timestamp. In that case, the timestamp
		// would not be set globally for the entire txn.
		if !p.asOfSystemTime {
			return hlc.MaxTimestamp, false,
				fmt.Errorf("AS OF SYSTEM TIME must be provided on a top-level statement")
		}

		// The Executor found an AS OF SYSTEM TIME clause at the top
		// level. We accept AS OF SYSTEM TIME in multiple places (e.g. in
		// subqueries or view queries) but they must all point to the same
		// timestamp.
		ts, err := EvalAsOfTimestamp(p.EvalContext(), asOf, hlc.MaxTimestamp)
		if err != nil {
			return hlc.MaxTimestamp, false, err
		}
		if ts != p.txn.OrigTimestamp() {
			return hlc.MaxTimestamp, false,
				fmt.Errorf("cannot specify AS OF SYSTEM TIME with different timestamps")
		}
		return ts, true, nil
	}
	return hlc.MaxTimestamp, false, nil
}

// srfExtractionVisitor replaces the inner set-returning function(s) in an
// expression with an expression that accesses the relevant
// columns in the FROM clause.
//
// This visitor is intentionally limited to extracting only one SRF, because we
// don't support lateral correlated subqueries.
type srfExtractionVisitor struct {
	err        error
	ctx        context.Context
	p          *planner
	r          *renderNode
	searchPath sessiondata.SearchPath
	seenSRF    bool
}

var _ tree.Visitor = &srfExtractionVisitor{}

func (v *srfExtractionVisitor) VisitPre(expr tree.Expr) (recurse bool, newNode tree.Expr) {
	if v.err != nil {
		return false, expr
	}
	// If a scalar sub-expression is a subquery, this will be handled
	// separately in the latest step by computeRenderAllowingStar(). We
	// do not want to recurse and replace SRFs here, because that work
	// will be done for the subquery later.
	_, isSubquery := expr.(*tree.Subquery)
	if isSubquery {
		return false, expr
	}

	switch t := expr.(type) {
	case *tree.ColumnAccessExpr:
		fe, ok := t.Expr.(*tree.FuncExpr)
		if !ok {
			// If there is no function inside the column access expression, then it
			// will be dealt with elsewhere.
			return false, expr
		}
		newFe := *fe
		newFe.EscapeSRF = true
		newCAE := *t
		newCAE.Expr = &newFe
		expr = &newCAE
	}
	return true, expr
}

func (v *srfExtractionVisitor) VisitPost(expr tree.Expr) tree.Expr {
	if v.err != nil {
		return expr
	}
	switch t := expr.(type) {
	case *tree.ColumnAccessExpr:
		// TODO(knz): support arbitrary composite expressions.
		fe, ok := t.Expr.(*tree.FuncExpr)
		if !ok || !fe.EscapeSRF {
			v.err = pgerror.NewErrorf(pgerror.CodeInternalError,
				"programmer error: invalid Walk recursion on ColumnAccessExpr")
			return expr
		}
		foundSRF, srfName, err := v.lookupSRF(fe)
		if err != nil {
			v.err = err
			return expr
		}
		if !foundSRF {
			v.err = pgerror.UnimplementedWithIssueErrorf(24866,
				"composite-returning scalar functions: %q", tree.ErrString(t.Expr))
			return expr
		}
		newExpr, err := v.transformSRF(srfName, fe, t)
		if err != nil {
			v.err = err
			return expr
		}
		return newExpr
	case *tree.FuncExpr:
		if t.EscapeSRF {
			return expr
		}
		foundSRF, srfName, err := v.lookupSRF(t)
		if err != nil {
			v.err = err
			return expr
		}
		if foundSRF {
			newExpr, err := v.transformSRF(srfName, t, nil /* columnAccess */)
			if err != nil {
				v.err = err
				return expr
			}
			return newExpr
		}
	}
	return expr
}

func (v *srfExtractionVisitor) lookupSRF(t *tree.FuncExpr) (bool, string, error) {
	fd, err := t.Func.Resolve(v.searchPath)
	if err != nil {
		return false, "", err
	}
	if _, ok := builtins.Generators[fd.Name]; ok {
		return true, fd.Name, nil
	}
	return false, "", nil
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
func (p *planner) rewriteSRFs(
	ctx context.Context, r *renderNode, targets tree.SelectExprs,
) (tree.SelectExprs, error) {
	// Walk the render expression looking for SRFs.
	v := &p.srfExtractionVisitor

	// Ensure that the previous visitor state is preserved during the
	// analysis below. This is especially important when recursing into
	// scalar subqueries, which may run this analysis too.
	defer func(p *planner, prevV srfExtractionVisitor) { p.srfExtractionVisitor = prevV }(p, *v)
	*v = srfExtractionVisitor{
		err:        nil,
		ctx:        ctx,
		p:          p,
		r:          r,
		seenSRF:    false,
		searchPath: p.SessionData().SearchPath,
	}

	// Rewrite the SRFs, if any. We want to avoid re-allocating a targets
	// array unless necessary.
	var newTargets tree.SelectExprs
	for i, target := range targets {
		newExpr, changed := tree.WalkExpr(v, target.Expr)
		if v.err != nil {
			return targets, v.err
		}
		if !changed {
			if newTargets != nil {
				newTargets[i] = target
			}
			continue
		}

		if newTargets == nil {
			newTargets = make(tree.SelectExprs, len(targets))
			copy(newTargets, targets[:i])
		}
		newTargets[i].Expr = newExpr
		newTargets[i].As = target.As
	}
	if newTargets != nil {
		return newTargets, nil
	}
	return targets, nil
}

func (v *srfExtractionVisitor) transformSRF(
	srfName string, srf *tree.FuncExpr, columnAccessExpr *tree.ColumnAccessExpr,
) (tree.Expr, error) {
	if v.seenSRF {
		// TODO(knz/jordan): multiple SRFs in the same scalar expression
		// should be migrated to a _single_ join operand in the FROM
		// clause, that zip the values togethers into a single table
		// (filling with NULLs where some SRFs produce fewer rows than
		// others).
		return nil, pgerror.UnimplementedWithIssueErrorf(20511,
			"cannot use multiple set-returning functions in a single SELECT clause: %q",
			tree.ErrString(srf))
	}
	v.seenSRF = true

	// Transform the SRF. This must be done in any case, whether the SRF
	// was used as scalar or whether a specific column was requested.
	tblName := tree.MakeUnqualifiedTableName(tree.Name(srfName))
	src, err := v.p.getGeneratorPlan(v.ctx, srf, tblName)
	if err != nil {
		return nil, err
	}

	// Save the SRF data source for later.
	srfSource := src

	// Do we already have something in the FROM clause?
	if !isUnarySource(v.r.source) {
		// The FROM clause specifies something. Replace with a cross-join (which is
		// an inner join without a condition).
		src, err = v.p.makeJoin(v.ctx, sqlbase.InnerJoin, v.r.source, src, nil)
		if err != nil {
			return nil, err
		}
	}
	// Keep the remaining relational expression.
	v.r.source = src
	v.r.sourceInfo = sqlbase.MultiSourceInfo{src.info}

	// Now decide what to render.
	if columnAccessExpr != nil {
		// Syntax: (tbl).colname or (tbl).*
		if columnAccessExpr.Star {
			// (tbl).*
			// We want all the columns as separate renders. Let the star
			// expansion that occurs later do the job.
			return &tree.AllColumnsSelector{TableName: tree.MakeUnresolvedName(srfName)}, nil
		}
		// (tbl).colname
		// We just want one column. Render that.
		return tree.NewColumnItem(&tblName, tree.Name(columnAccessExpr.ColName)), nil
	}

	// Just the SRF name. Check whether we have just one column or more.
	if len(srfSource.info.SourceColumns) == 1 {
		// Just one column. Render that column exactly.
		return tree.NewColumnItem(&tblName, tree.Name(srfSource.info.SourceColumns[0].Name)), nil
	}
	// More than one column.
	// We want all the columns as a tuple. Render that.
	// TODO(knz): This should be returned as a composite expression instead
	// (tuple with labels). See #24866.
	tExpr := &tree.Tuple{
		Exprs: make(tree.Exprs, len(srfSource.info.SourceColumns)),
	}
	for i, c := range srfSource.info.SourceColumns {
		tExpr.Exprs[i] = tree.NewColumnItem(&tblName, tree.Name(c.Name))
	}
	return tExpr, nil
}

// A unary source is the special source used with empty FROM clauses:
// a pseudo-table with zero columns and exactly one row.
func isUnarySource(src planDataSource) bool {
	_, ok := src.plan.(*unaryNode)
	return ok && len(src.info.SourceColumns) == 0
}

func (p *planner) initWhere(
	ctx context.Context, r *renderNode, whereExpr tree.Expr,
) (*filterNode, error) {
	f := &filterNode{source: r.source}
	f.ivarHelper = tree.MakeIndexedVarHelper(f, len(r.sourceInfo[0].SourceColumns))

	if whereExpr != nil {
		var err error
		f.filter, err = p.analyzeExpr(ctx, whereExpr, r.sourceInfo, f.ivarHelper,
			types.Bool, true, "WHERE")
		if err != nil {
			return nil, err
		}

		// Make sure there are no aggregation/window functions in the filter
		// (after subqueries have been expanded).
		if err := p.txCtx.AssertNoAggregationOrWindowing(
			f.filter, "WHERE", p.SessionData().SearchPath,
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
func getRenderColName(
	searchPath sessiondata.SearchPath, target tree.SelectExpr, helper *tree.IndexedVarHelper,
) (string, error) {
	if target.As != "" {
		return string(target.As), nil
	}

	// If the expression designates a column, try to reuse that column's name
	// as render name.
	if err := target.NormalizeTopLevelVarName(); err != nil {
		return "", err
	}

	switch t := target.Expr.(type) {
	case *tree.ColumnItem:
		return t.Column(), nil

	case *tree.FuncExpr:
		// Special case for rendering builtin functions: the column name for an
		// otherwise un-named builtin output column is just the name of the builtin.
		fd, err := t.Func.Resolve(searchPath)
		if err != nil {
			return "", err
		}
		return fd.Name, nil

	case *tree.ColumnAccessExpr:
		// Special case for rending a column accessor.
		if t.Star {
			// TODO(bram): remove this, this case should never happen once (srf).* is
			// correctly implemented.
			return t.Expr.String(), nil
		}
		return t.ColName, nil
	}

	return target.Expr.String(), nil
}

// appendRenderColumn adds a new render expression at the end of the current list.
// The expression must be normalized already.
func (r *renderNode) addRenderColumn(
	expr tree.TypedExpr, exprStr string, col sqlbase.ResultColumn,
) {
	r.render = append(r.render, expr)
	r.renderStrings = append(r.renderStrings, exprStr)
	r.columns = append(r.columns, col)
}

// resetRenderColumns resets all the render expressions. This is used e.g. by
// aggregation and windowing (see group.go / window.go). The method also
// asserts that both the render and columns array have the same size.
func (r *renderNode) resetRenderColumns(exprs []tree.TypedExpr, cols sqlbase.ResultColumns) {
	if len(exprs) != len(cols) {
		panic(fmt.Sprintf("resetRenderColumns used with arrays of different sizes: %d != %d", len(exprs), len(cols)))
	}
	r.render = exprs
	// This clears all of the cached render strings. They'll get created again
	// when necessary.
	r.renderStrings = make([]string, len(cols))
	r.columns = cols
}

// renderRow renders the row by evaluating the render expressions.
func (r *renderNode) renderRow(evalCtx *tree.EvalContext) error {
	if r.run.row == nil {
		r.run.row = make([]tree.Datum, len(r.render))
	}
	for i, e := range r.render {
		var err error
		evalCtx.IVarContainer = r
		r.run.row[i], err = e.Eval(evalCtx)
		if err != nil {
			return err
		}
	}
	return nil
}

// computePhysicalPropsForRender computes ordering information for the
// render node, given ordering information for the "from" node.
//
//    SELECT a, b FROM t@abc ...
//      the ordering is: first by column 0 (a), then by column 1 (b).
//
//    SELECT a, b FROM t@abc WHERE a = 1 ...
//      the ordering is: exact match column (a), ordered by column 1 (b).
//
//    SELECT b, a FROM t@abc ...
//      the ordering is: first by column 1 (a), then by column 0 (a).
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
//
//    SELECT a, b, a FROM t@abc ...
//      we have an equivalency group between columns 0 and 2 and the ordering is
//      first by column 0 (a), then by column 1.
//
// The planner is necessary to perform name resolution while detecting constant columns.
func (p *planner) computePhysicalPropsForRender(r *renderNode, fromOrder physicalProps) {
	// See physicalProps.project for a description of the projection map.
	projMap := make([]int, len(r.render))
	for i, expr := range r.render {
		if ivar, ok := expr.(*tree.IndexedVar); ok {
			// Column ivar.Idx of the source becomes column i of the render node.
			projMap[i] = ivar.Idx
		} else {
			projMap[i] = -1
		}
	}
	r.props = fromOrder.project(projMap)

	// Detect constants.
	for col, expr := range r.render {
		_, hasRowDependentValues, _, err := p.resolveNamesForRender(expr, r)
		if err != nil {
			// If we get an error here, the expression must contain an unresolved name
			// or invalid indexed var; ignore.
			continue
		}
		if !hasRowDependentValues && !r.columns[col].Omitted {
			r.props.addConstantColumn(col)
		}
	}
}

// colIdxByRenderAlias returns the corresponding index in columns of an expression
// that may refer to a column alias.
// If there are no aliases in columns that expr refers to, then -1 is returned.
// This method is pertinent to ORDER BY and DISTINCT ON clauses that may refer
// to a column alias.
func (r *renderNode) colIdxByRenderAlias(
	expr tree.Expr, columns sqlbase.ResultColumns, op string,
) (int, error) {
	index := -1

	if vBase, ok := expr.(tree.VarName); ok {
		v, err := vBase.NormalizeVarName()
		if err != nil {
			return 0, err
		}

		if c, ok := v.(*tree.ColumnItem); ok && c.TableName.Parts[0] == "" {
			// Look for an output column that matches the name. This
			// handles cases like:
			//
			//   SELECT a AS b FROM t ORDER BY b
			//   SELECT DISTINCT ON (b) a AS b FROM t
			target := string(c.ColumnName)
			for j, col := range columns {
				if col.Name == target {
					if index != -1 {
						// There is more than one render alias that matches the clause. Here,
						// SQL92 is specific as to what should be done: if the underlying
						// expression is known (we're on a renderNode) and it is equivalent,
						// then just accept that and ignore the ambiguity.
						// This plays nice with `SELECT b, * FROM t ORDER BY b`. Otherwise,
						// reject with an ambiguity error.
						if r == nil || !r.equivalentRenders(j, index) {
							return 0, pgerror.NewErrorf(
								pgerror.CodeAmbiguousAliasError,
								"%s \"%s\" is ambiguous", op, target,
							)
						}
						// Note that in this case we want to use the index of the first matching
						// column. This is because renderNode.computePhysicalProps also prefers
						// the first column, and we want the orderings to match as much as
						// possible.
						continue
					}
					index = j
				}
			}
		}
	}

	return index, nil
}
