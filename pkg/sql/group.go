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
	"strings"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// groupBy constructs a groupNode according to grouping functions or clauses. This may adjust the
// render targets in the renderNode as necessary.
func (p *planner) groupBy(
	ctx context.Context, n *parser.SelectClause, s *renderNode,
) (*groupNode, error) {
	// Determine if aggregation is being performed. This check is done on the raw
	// Select expressions as simplification might have removed aggregation
	// functions (e.g. `SELECT MIN(1)` -> `SELECT 1`).
	if isAggregate := p.parser.IsAggregate(n, p.session.SearchPath); !isAggregate {
		return nil, nil
	}

	groupByExprs := make([]parser.Expr, len(n.GroupBy))

	// A SELECT clause is valid if all its expressions are valid as well, one
	// of the criteria for a valid expression is if it appears verbatim in one
	// of the GROUP BY clauses.
	// NB: "verbatim" above is defined using a string-equality comparison
	// as an approximation of a recursive tree-equality comparison.
	//
	// For example:
	// Valid:   `SELECT UPPER(k), SUM(v) FROM kv GROUP BY UPPER(k)`
	// - `UPPER(k)` appears in GROUP BY.
	// Invalid: `SELECT k, SUM(v) FROM kv GROUP BY UPPER(k)`
	// - `k` does not appear in GROUP BY; UPPER(k) does nothing to help here.
	//
	// In the construction of the outer renderNode, when renders are
	// processed (via computeRender()), the expressions are
	// normalized. In order to compare these normalized render
	// expressions to GROUP BY expressions, we need to normalize the
	// GROUP BY expressions as well.
	// This is done before determining if aggregation is being performed, because
	// that determination is made during validation, which will require matching
	// expressions.
	for i, expr := range n.GroupBy {
		expr = parser.StripParens(expr)

		// Check whether the GROUP BY clause refers to a rendered column
		// (specified in the original query) by index, e.g. `SELECT a, SUM(b)
		// FROM y GROUP BY 1`.
		col, err := p.colIndex(s.numOriginalCols, expr, "GROUP BY")
		if err != nil {
			return nil, err
		}

		if col != -1 {
			groupByExprs[i] = s.render[col]
			expr = n.Exprs[col].Expr
		} else {
			// We do not need to fully analyze the GROUP BY expression here
			// (as per analyzeExpr) because this is taken care of by computeRender
			// below.
			resolvedExpr, _, err := p.resolveNames(expr, s.sourceInfo, s.ivarHelper)
			if err != nil {
				return nil, err
			}
			groupByExprs[i] = resolvedExpr
		}

		if err := p.parser.AssertNoAggregationOrWindowing(
			expr, "GROUP BY", p.session.SearchPath,
		); err != nil {
			return nil, err
		}
	}

	// Normalize and check the HAVING expression too if it exists.
	var typedHaving parser.TypedExpr
	if n.Having != nil {
		if p.parser.WindowFuncInExpr(n.Having.Expr) {
			return nil, sqlbase.NewWindowingError("HAVING")
		}
		var err error
		typedHaving, err = p.analyzeExpr(ctx, n.Having.Expr, s.sourceInfo, s.ivarHelper,
			parser.TypeBool, true, "HAVING")
		if err != nil {
			return nil, err
		}
	}

	group := &groupNode{
		planner:            p,
		values:             valuesNode{columns: s.columns},
		render:             s.render,
		filterToRenderIdxs: make(map[int]int),
	}

	aggVisitor := extractAggregatesVisitor{
		n:           group,
		groupStrs:   make(map[string]struct{}, len(groupByExprs)),
		groupedCopy: new(extractAggregatesVisitor),
	}

	checkVisitor := checkQueryVisitor{
		groupStrs:   make(map[string]struct{}, len(groupByExprs)),
		groupedCopy: new(checkQueryVisitor),
	}

	for _, e := range groupByExprs {
		aggVisitor.groupStrs[e.String()] = struct{}{}
		checkVisitor.groupStrs[e.String()] = struct{}{}
	}

	// A copy of the visitor that is used when a subtree appears in the GROUP BY.
	// One copy is allocated up-front, rather than potentially several on-the-fly,
	// to reduce allocations.
	*aggVisitor.groupedCopy = aggVisitor
	aggVisitor.groupedCopy.groupedCopy = nil

	*checkVisitor.groupedCopy = checkVisitor
	checkVisitor.groupedCopy.groupedCopy = nil

	// Loop over the render expressions and check for any malformed queries.
	for _, r := range group.render {
		if err := checkVisitor.check(r); err != nil {
			return nil, err
		}
	}

	if typedHaving != nil {
		if err := checkVisitor.check(typedHaving); err != nil {
			return nil, err
		}
		group.having = typedHaving
	}

	// Loop over the render expressions and extract any aggregate functions --
	// IndexedVars are also replaced (with identAggregates, which just return the last
	// value added to them for a bucket) to provide grouped-by values for each bucket.
	// After extraction, group.render will be entirely rendered from aggregateFuncHolders,
	// and group.funcs will contain all the functions which need to be fed values.
	for i, r := range group.render {
		group.render[i] = aggVisitor.extract(r)
	}

	if typedHaving != nil {
		group.having = aggVisitor.extract(typedHaving)
	}

	// Queries like `SELECT MAX(n) FROM t` expect a row of NULLs if nothing was aggregated.
	group.addNullBucketIfEmpty = len(groupByExprs) == 0

	group.buckets = make(map[string]struct{})

	if log.V(2) {
		strs := make([]string, 0, len(group.funcs))
		for _, f := range group.funcs {
			strs = append(strs, f.String())
		}
		log.Infof(ctx, "Group: %s", strings.Join(strs, ", "))
	}

	// Replace the render expressions in the scanNode with expressions that
	// compute only the arguments to the aggregate expressions.
	newRenders := make([]parser.TypedExpr, len(group.funcs))
	newColumns := make(sqlbase.ResultColumns, len(group.funcs))
	for i, f := range group.funcs {
		// Note: we do not need to normalize the expressions again because
		// they were normalized by renderNode's initFrom() before we
		// extracted aggregation functions above.
		newRenders[i] = f.arg
		newColumns[i] = sqlbase.ResultColumn{
			Name: f.arg.String(),
			Typ:  f.arg.ResolvedType(),
		}
	}
	// TODO(radu): we should not add duplicate renders; add a mapping between
	// group.funcs and s.renders (instead of requiring 1-to-1 correspondence) and
	// use addOrMergeRender
	s.resetRenderColumns(newRenders, newColumns)

	// Add the group-by expressions so they are available for bucketing.
	group.groupByIdx = make([]int, 0, len(groupByExprs))
	for _, g := range groupByExprs {
		cols, exprs, hasStar, err := s.planner.computeRenderAllowingStars(
			ctx, parser.SelectExpr{Expr: g}, parser.TypeAny, s.sourceInfo, s.ivarHelper,
			autoGenerateRenderOutputName)
		if err != nil {
			return nil, err
		}
		s.isStar = s.isStar || hasStar
		colIdxs := s.addOrMergeRenders(cols, exprs, true /* reuseExistingRender */)
		group.groupByIdx = append(group.groupByIdx, colIdxs...)
	}

	// Add the filter expressions, so they are available when deciding what rows
	// to feed to the aggregation functions.
	for i, f := range group.funcs {
		if f.filter == nil {
			continue
		}

		if err := p.parser.AssertNoAggregationOrWindowing(
			f.filter, "FILTER", p.session.SearchPath,
		); err != nil {
			return nil, err
		}

		cols, exprs, hasStar, err := s.planner.computeRenderAllowingStars(
			ctx, parser.SelectExpr{Expr: f.filter}, parser.TypeAny,
			s.sourceInfo, s.ivarHelper, autoGenerateRenderOutputName)
		if err != nil {
			return nil, err
		}
		if hasStar {
			panic("star found in filter; this should not have passed type checking")
		}
		colIdxs := s.addOrMergeRenders(cols, exprs, true /* reuseExistingRender */)
		if len(colIdxs) != 1 {
			panic("multiple columns rendered for filter")
		}
		group.filterToRenderIdxs[i] = colIdxs[0]
	}

	group.desiredOrdering = group.desiredAggregateOrdering()
	return group, nil
}

// A groupNode implements the planNode interface and handles the grouping logic.
// It "wraps" a planNode which is used to retrieve the ungrouped results.
type groupNode struct {
	planner *planner

	// The "wrapped" node (which returns ungrouped results).
	plan planNode

	// render contains the expressions that produce the output values. The
	// aggregate functions in them are going to be replaced by
	// aggregateFuncHolders, which accumulate the data to be aggregated bucketed
	// by the group-by values.
	render []parser.TypedExpr
	having parser.TypedExpr

	// funcs are the aggregation functions that the renders use.
	funcs []*aggregateFuncHolder
	// Indices (in the wrapped plan's columns) for the group by columns.
	groupByIdx []int
	// Map of index of aggregation function (from funcs) to the render for the
	// corresponding filtering expression in the wrapped plan.
	// The map is only populated for functions that have a filter.
	filterToRenderIdxs map[int]int
	// The set of bucket keys.
	buckets map[string]struct{}

	addNullBucketIfEmpty bool

	values    valuesNode
	populated bool

	// During rendering, aggregateFuncHolders compute their result for group.currentBucket.
	currentBucket string

	// desiredOrdering is set only if we are aggregating around a single MIN/MAX
	// function and we can compute the final result using a single row, assuming
	// a specific ordering of the underlying plan.
	desiredOrdering sqlbase.ColumnOrdering
	needOnlyOneRow  bool
	gotOneRow       bool

	explain explainMode
}

func (n *groupNode) Columns() sqlbase.ResultColumns {
	return n.values.Columns()
}

func (n *groupNode) Ordering() orderingInfo {
	// TODO(dt): aggregate buckets are returned un-ordered for now.
	return orderingInfo{}
}

func (n *groupNode) Values() parser.Datums {
	return n.values.Values()
}

func (n *groupNode) MarkDebug(mode explainMode) {
	if mode != explainDebug {
		panic(fmt.Sprintf("unknown debug mode %d", mode))
	}
	n.explain = mode
	n.plan.MarkDebug(mode)
}

func (n *groupNode) DebugValues() debugValues {
	if n.populated {
		return n.values.DebugValues()
	}

	// We are emitting a "buffered" row.
	vals := n.plan.DebugValues()
	if vals.output == debugValueRow {
		vals.output = debugValueBuffered
	}
	return vals
}

func (n *groupNode) Spans(ctx context.Context) (_, _ roachpb.Spans, _ error) {
	return n.plan.Spans(ctx)
}

func (n *groupNode) Start(ctx context.Context) error {
	return n.plan.Start(ctx)
}

func (n *groupNode) Next(ctx context.Context) (bool, error) {
	var scratch []byte
	// We're going to consume n.plan until it's exhausted, feed all the rows to
	// n.funcs, and then call n.computeAggregated to generate all the results.
	// Subsequent calls to next will skip the first part and just return a result.
	for !n.populated {
		next := false
		if !(n.needOnlyOneRow && n.gotOneRow) {
			var err error
			next, err = n.plan.Next(ctx)
			if err != nil {
				return false, err
			}
		}
		if !next {
			n.populated = true
			if err := n.computeAggregates(ctx); err != nil {
				return false, err
			}
			break
		}
		if n.explain == explainDebug && n.plan.DebugValues().output != debugValueRow {
			// Pass through non-row debug values.
			return true, nil
		}

		// Add row to bucket.

		values := n.plan.Values()
		valuesToAccumulate := values[:len(n.funcs)]

		// TODO(dt): optimization: skip buckets when underlying plan is ordered by grouped values.

		bucket := scratch
		for _, idx := range n.groupByIdx {
			var err error
			bucket, err = sqlbase.EncodeDatum(bucket, values[idx])
			if err != nil {
				return false, err
			}
		}

		n.buckets[string(bucket)] = struct{}{}

		// Feed the aggregateFuncHolders for this bucket the non-grouped values.
		for i, value := range valuesToAccumulate {
			var filterVal parser.Datum = parser.DBoolTrue
			if renderIdx, ok := n.filterToRenderIdxs[i]; ok {
				filterVal = values[renderIdx]
			}
			if filterVal != parser.DBoolTrue {
				continue
			}

			if err := n.funcs[i].add(ctx, n.planner.session, bucket, value); err != nil {
				return false, err
			}
		}
		scratch = bucket[:0]

		n.gotOneRow = true

		if n.explain == explainDebug {
			// Emit a "buffered" row.
			return true, nil
		}
	}

	return n.values.Next(ctx)
}

func (n *groupNode) computeAggregates(ctx context.Context) error {
	if len(n.buckets) < 1 && n.addNullBucketIfEmpty {
		n.buckets[""] = struct{}{}
	}

	// Render the results.
	n.values.rows = sqlbase.NewRowContainer(
		n.planner.session.TxnState.makeBoundAccount(),
		sqlbase.ColTypeInfoFromResCols(n.values.Columns()),
		len(n.buckets),
	)
	row := make(parser.Datums, len(n.render))
	for k := range n.buckets {
		n.currentBucket = k

		if n.having != nil {
			res, err := n.having.Eval(&n.planner.evalCtx)
			if err != nil {
				return err
			}
			if val, err := parser.GetBool(res); err != nil {
				return err
			} else if !val {
				continue
			}
		}
		for i, r := range n.render {
			var err error
			row[i], err = r.Eval(&n.planner.evalCtx)
			if err != nil {
				return err
			}
		}

		if _, err := n.values.rows.AddRow(ctx, row); err != nil {
			return err
		}
	}
	return nil
}

func (n *groupNode) Close(ctx context.Context) {
	n.plan.Close(ctx)
	for _, f := range n.funcs {
		f.close(ctx, n.planner.session)
	}
	n.values.Close(ctx)
	n.buckets = nil
}

// requiresIsNotNullFilter returns whether a "col IS NOT NULL" constraint must
// be added. This is the case when we have a single MIN/MAX aggregation
// function.
func (n *groupNode) requiresIsNotNullFilter() bool {
	return len(n.desiredOrdering) == 1
}

// isNotNullFilter adds as a "col IS NOT NULL" constraint to the expression if
// the groupNode has a desired ordering on col (see
// desiredAggregateOrdering). A desired ordering will only be present if there
// is a single MIN/MAX aggregation function.
func (n *groupNode) isNotNullFilter(expr parser.TypedExpr) parser.TypedExpr {
	if !n.requiresIsNotNullFilter() {
		panic("IS NOT NULL filter not required")
	}
	i := n.desiredOrdering[0].ColIdx
	f := n.funcs[i]
	isNotNull := parser.NewTypedComparisonExpr(
		parser.IsNot,
		f.arg,
		parser.DNull,
	)
	if expr == nil {
		return isNotNull
	}
	return parser.NewTypedAndExpr(
		expr,
		isNotNull,
	)
}

// desiredAggregateOrdering computes the desired output ordering from the
// scan.
//
// We only have a desired ordering if we have a single MIN or MAX aggregation
// with a simple column argument and there is no GROUP BY.
func (n *groupNode) desiredAggregateOrdering() sqlbase.ColumnOrdering {
	if len(n.groupByIdx) > 0 {
		return nil
	}

	if len(n.funcs) != 1 {
		return nil
	}
	f := n.funcs[0]
	impl := f.create(&n.planner.evalCtx)
	switch impl.(type) {
	case *parser.MaxAggregate, *parser.MinAggregate:
		if f.arg == nil {
			return nil
		}
		direction := encoding.Ascending
		switch f.arg.(type) {
		case *parser.IndexedVar:
			if _, ok := impl.(*parser.MaxAggregate); ok {
				direction = encoding.Descending
			}
			return sqlbase.ColumnOrdering{{ColIdx: 0, Direction: direction}}
		}
	}
	return nil
}

type extractAggregatesVisitor struct {
	n         *groupNode
	groupStrs map[string]struct{}

	// groupedCopy is nil when visitor is in an Expr subtree that appears in the GROUP BY clause.
	groupedCopy *extractAggregatesVisitor
}

var _ parser.Visitor = &extractAggregatesVisitor{}

func (v *extractAggregatesVisitor) VisitPre(expr parser.Expr) (recurse bool, newExpr parser.Expr) {
	// This expression is in the GROUP BY - switch to the visitor that will accept
	// IndexedVars for this and any subtrees.
	if _, ok := v.groupStrs[expr.String()]; ok && v.groupedCopy != nil && v != v.groupedCopy {
		expr, _ = parser.WalkExpr(v.groupedCopy, expr)
		return false, expr
	}

	switch t := expr.(type) {
	case *parser.FuncExpr:
		if agg := t.GetAggregateConstructor(); agg != nil {
			if len(t.Exprs) != 1 {
				// Type checking has already run on these expressions thus
				// if an aggregate function of the wrong arity gets here,
				// something has gone really wrong. Additionally the query
				// checker is not functioning correctly as it should have
				// panicked there.
				panic("query checker did not detect multiple arguments for aggregator")
			}
			argExpr := t.Exprs[0]

			var filterExpr parser.TypedExpr
			if t.Filter != nil {
				filterExpr = t.Filter.(parser.TypedExpr)
			}
			f := v.n.newAggregateFuncHolder(t, argExpr.(parser.TypedExpr), filterExpr, agg)
			if t.Type == parser.DistinctFuncType {
				f.seen = make(map[string]struct{})
			}
			v.n.funcs = append(v.n.funcs, f)
			return false, f
		}
	case *parser.IndexedVar:
		if v.groupedCopy != nil {
			panic("query checker did not detect column not appearing in GROUP BY clauses or aggregation function")
		}
		f := v.n.newAggregateFuncHolder(t, t, nil /* filter */, parser.NewIdentAggregate)
		v.n.funcs = append(v.n.funcs, f)
		return false, f
	}

	return true, expr
}

func (*extractAggregatesVisitor) VisitPost(expr parser.Expr) parser.Expr { return expr }

// extract aggregateFuncHolders from exprs that use aggregation and add them to
// the groupNode.
func (v extractAggregatesVisitor) extract(typedExpr parser.TypedExpr) parser.TypedExpr {
	expr, _ := parser.WalkExpr(&v, typedExpr)
	return expr.(parser.TypedExpr)
}

type checkQueryVisitor struct {
	groupStrs map[string]struct{}

	// groupedCopy is nil when visitor is in an Expr subtree that appears in the GROUP BY clause.
	groupedCopy   *checkQueryVisitor
	subAggVisitor parser.IsAggregateVisitor
	err           error
}

var _ parser.Visitor = &checkQueryVisitor{}

func (v *checkQueryVisitor) VisitPre(expr parser.Expr) (recurse bool, newExpr parser.Expr) {
	if v.err != nil {
		return false, expr
	}

	// This expression is in the GROUP BY - switch to the visitor that will accept
	// IndexedVars for this and any subtrees.
	if _, ok := v.groupStrs[expr.String()]; ok && v.groupedCopy != nil && v != v.groupedCopy {
		expr, _ = parser.WalkExpr(v.groupedCopy, expr)
		return false, expr
	}

	switch t := expr.(type) {
	case *parser.FuncExpr:
		if agg := t.GetAggregateConstructor(); agg != nil {
			if len(t.Exprs) != 1 {
				// Type checking has already run on these expressions thus
				// if an aggregate function of the wrong arity gets here,
				// something has gone really wrong.
				panic(fmt.Sprintf("%q has %d arguments (expected 1)", t.Func, len(t.Exprs)))
			}

			argExpr := t.Exprs[0]

			defer v.subAggVisitor.Reset()
			if parser.WalkExprConst(&v.subAggVisitor, argExpr); v.subAggVisitor.Aggregated {
				v.err = fmt.Errorf("aggregate function calls cannot be nested under %s()", t.Func)
			}
			return false, expr
		}
	case *parser.IndexedVar:
		if v.groupedCopy != nil {
			v.err = fmt.Errorf("column \"%s\" must appear in the GROUP BY clause or be used in an aggregate function",
				t.String())
			return true, expr
		}
		return false, expr
	}
	return true, expr
}

func (*checkQueryVisitor) VisitPost(expr parser.Expr) parser.Expr { return expr }

// Check if expr provided is valid.
// An expression is valid if:
// - it is an aggregate expression, or
// - it appears verbatim in groupBy, or
// - it is not an IndexedVar, and all of its subexpressions (as defined by
// its Walk implementation) are valid
// NB: "verbatim" above is defined using a string-equality comparison
// as an approximation of a recursive tree-equality comparison.
//
// For example:
// Invalid: `SELECT k, SUM(v) FROM kv`
// - `k` is unaggregated and does not appear in the (missing) GROUP BY.
// Valid:      `SELECT k, SUM(v) FROM kv GROUP BY k`
// Also valid: `SELECT UPPER(k), SUM(v) FROM kv GROUP BY UPPER(k)`
// - `UPPER(k)` appears in GROUP BY.
// Also valid: `SELECT UPPER(k), SUM(v) FROM kv GROUP BY k`
// - `k` appears in GROUP BY, so `UPPER(k)` is OK, but...
// Invalid:    `SELECT k, SUM(v) FROM kv GROUP BY UPPER(k)`
// - `k` does not appear in GROUP BY; UPPER(k) does nothing to help here.
func (v checkQueryVisitor) check(typedExpr parser.TypedExpr) error {
	parser.WalkExpr(&v, typedExpr)
	return v.err
}

var _ parser.TypedExpr = &aggregateFuncHolder{}
var _ parser.VariableExpr = &aggregateFuncHolder{}

type aggregateFuncHolder struct {
	// expr must either contain an aggregation function (SUM, COUNT, etc.) or an
	// expression that also appears as one of the GROUP BY expressions (v+w in
	// SELECT v+w FROM kvw GROUP BY v+w).
	expr          parser.TypedExpr
	arg           parser.TypedExpr
	filter        parser.TypedExpr
	create        func(*parser.EvalContext) parser.AggregateFunc
	group         *groupNode
	buckets       map[string]parser.AggregateFunc
	bucketsMemAcc WrappableMemoryAccount
	seen          map[string]struct{}
}

func (n *groupNode) newAggregateFuncHolder(
	expr, arg, filter parser.TypedExpr, create func(*parser.EvalContext) parser.AggregateFunc,
) *aggregateFuncHolder {
	res := &aggregateFuncHolder{
		expr:          expr,
		arg:           arg,
		filter:        filter,
		create:        create,
		group:         n,
		buckets:       make(map[string]parser.AggregateFunc),
		bucketsMemAcc: n.planner.session.TxnState.OpenAccount(),
	}
	return res
}

func (a *aggregateFuncHolder) close(ctx context.Context, s *Session) {
	for _, aggFunc := range a.buckets {
		aggFunc.Close(ctx)
	}

	a.buckets = nil
	a.seen = nil
	a.group = nil

	a.bucketsMemAcc.Wtxn(s).Close(ctx)
}

// add accumulates one more value for a particular bucket into an aggregation
// function.
func (a *aggregateFuncHolder) add(
	ctx context.Context, s *Session, bucket []byte, d parser.Datum,
) error {
	// NB: the compiler *should* optimize `myMap[string(myBytes)]`. See:
	// https://github.com/golang/go/commit/f5f5a8b6209f84961687d993b93ea0d397f5d5bf

	if a.seen != nil {
		encoded, err := sqlbase.EncodeDatum(bucket, d)
		if err != nil {
			return err
		}
		if _, ok := a.seen[string(encoded)]; ok {
			// skip
			return nil
		}
		if err := a.bucketsMemAcc.Wtxn(s).Grow(ctx, int64(len(encoded))); err != nil {
			return err
		}
		a.seen[string(encoded)] = struct{}{}
	}

	impl, ok := a.buckets[string(bucket)]
	if !ok {
		impl = a.create(&a.group.planner.evalCtx)
		a.buckets[string(bucket)] = impl
	}

	return impl.Add(ctx, d)
}

func (*aggregateFuncHolder) Variable() {}

func (a *aggregateFuncHolder) Format(buf *bytes.Buffer, f parser.FmtFlags) {
	a.expr.Format(buf, f)
}
func (a *aggregateFuncHolder) String() string { return parser.AsString(a) }

func (a *aggregateFuncHolder) Walk(v parser.Visitor) parser.Expr { return a }

func (a *aggregateFuncHolder) TypeCheck(
	_ *parser.SemaContext, desired parser.Type,
) (parser.TypedExpr, error) {
	return a, nil
}

func (a *aggregateFuncHolder) Eval(ctx *parser.EvalContext) (parser.Datum, error) {
	found, ok := a.buckets[a.group.currentBucket]
	if !ok {
		found = a.create(ctx)
	}

	result, err := found.Result()
	if err != nil {
		return nil, err
	}

	if result == nil {
		if parser.IsIdentAggregate(found) {
			// Identity functions return their argument, even if no
			// aggregation inputs were seen.
			return a.arg.Eval(ctx)
		}
		// Otherwise, we can't be here: all aggregation functions
		// should return a valid value or DNull if there are no rows.
		panic("aggregation function returned nil")
	}

	return result, nil
}

func (a *aggregateFuncHolder) ResolvedType() parser.Type {
	return a.expr.ResolvedType()
}
