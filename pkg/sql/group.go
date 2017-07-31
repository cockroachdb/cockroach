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
	"fmt"
	"strings"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// groupByStrMap maps each GROUP BY expression string to the index of the column
// in the underlying renderNode that renders this expression.
// For stars (GROUP BY k.*) the special value -1 is used.
type groupByStrMap map[string]int

// groupBy constructs a planNode "complex" consisting of a groupNode and other
// post-processing nodes according to grouping functions or clauses.
//
// The complex always includes a groupNode which initially uses the given
// renderNode as its own data source; the data source can be changed later with
// an equivalent one if the renderNode is optimized out.
//
// The visible node from the consumer-side is a renderNode which renders
// post-aggregation expressions:
//
//          renderNode (post-agg rendering)
//              |
//              |
//              v
//          groupNode
//              |
//              |
//              v
//          renderNode (pre-agg rendering)
//
// If HAVING is used, there is also a filterNode in-between the renderNode and
// the groupNode:
//
//          renderNode (post-agg rendering)
//              |
//              |
//              v
//          filterNode
//              |
//              |
//              v
//           groupNode
//              |
//              |
//              v
//          renderNode (pre-agg rendering)
//
// This function returns both the consumer-side planNode and the main groupNode; if there
// is no grouping, both are nil.
func (p *planner) groupBy(
	ctx context.Context, n *parser.SelectClause, r *renderNode,
) (planNode, *groupNode, error) {
	// Determine if aggregation is being performed. This check is done on the raw
	// Select expressions as simplification might have removed aggregation
	// functions (e.g. `SELECT MIN(1)` -> `SELECT 1`).
	if isAggregate := p.parser.IsAggregate(n, p.session.SearchPath); !isAggregate {
		return nil, nil, nil
	}

	groupByExprs := make([]parser.Expr, len(n.GroupBy))

	// In the construction of the renderNode, when renders are processed (via
	// computeRender()), the expressions are normalized. In order to compare these
	// normalized render expressions to GROUP BY expressions, we need to normalize
	// the GROUP BY expressions as well. This is done before determining if
	// aggregation is being performed, because that determination is made during
	// validation, which will require matching expressions.
	for i, expr := range n.GroupBy {
		expr = parser.StripParens(expr)

		// Check whether the GROUP BY clause refers to a rendered column
		// (specified in the original query) by index, e.g. `SELECT a, SUM(b)
		// FROM y GROUP BY 1`.
		col, err := p.colIndex(r.numOriginalCols, expr, "GROUP BY")
		if err != nil {
			return nil, nil, err
		}

		if col != -1 {
			groupByExprs[i] = r.render[col]
			expr = n.Exprs[col].Expr
		} else {
			// We do not need to fully analyze the GROUP BY expression here
			// (as per analyzeExpr) because this is taken care of by computeRender
			// below. We do however need to resolveNames so the
			// AssertNoAggregationOrWindowing call below can find resolved
			// FunctionDefs in the AST (instead of UnresolvedNames).
			resolvedExpr, _, err := p.resolveNames(expr, r.sourceInfo, r.ivarHelper)
			if err != nil {
				return nil, nil, err
			}
			groupByExprs[i] = resolvedExpr
		}

		if err := p.parser.AssertNoAggregationOrWindowing(
			expr, "GROUP BY", p.session.SearchPath,
		); err != nil {
			return nil, nil, err
		}
	}

	// Normalize and check the HAVING expression too if it exists.
	var typedHaving parser.TypedExpr
	if n.Having != nil {
		if p.parser.WindowFuncInExpr(n.Having.Expr) {
			return nil, nil, sqlbase.NewWindowingError("HAVING")
		}
		var err error
		typedHaving, err = p.analyzeExpr(ctx, n.Having.Expr, r.sourceInfo, r.ivarHelper,
			parser.TypeBool, true, "HAVING")
		if err != nil {
			return nil, nil, err
		}
	}

	group := &groupNode{
		planner: p,
		plan:    r,
	}

	// We replace the columns in the underlying renderNode with what the
	// groupNode needs as input:
	//  - group by expressions
	//  - arguments to the aggregate expressions
	//  - having expressions
	origRenders := r.render
	origColumns := r.columns
	r.resetRenderColumns(nil, nil)

	// Add the group-by expressions.

	// groupStrs maps a GROUP BY expression string to the index of the column in
	// the underlying renderNode.
	groupStrs := make(groupByStrMap, len(groupByExprs))
	for _, g := range groupByExprs {
		cols, exprs, hasStar, err := p.computeRenderAllowingStars(
			ctx, parser.SelectExpr{Expr: g}, parser.TypeAny, r.sourceInfo, r.ivarHelper,
			autoGenerateRenderOutputName)
		if err != nil {
			return nil, nil, err
		}
		r.isStar = r.isStar || hasStar
		colIdxs := r.addOrReuseRenders(cols, exprs, true /* reuseExistingRender */)
		if !hasStar {
			groupStrs[symbolicExprStr(g)] = colIdxs[0]
		} else {
			// We use a special value to indicate a star (e.g. GROUP BY t.*).
			groupStrs[symbolicExprStr(g)] = -1
		}
	}
	group.numGroupCols = len(r.render)

	var havingNode *filterNode
	plan := planNode(group)

	// Extract any aggregate functions from having expressions, adding renders to
	// r as needed.
	//
	// "Grouping expressions" - expressions that also show up under GROUP BY - are
	// also treated as aggregate expressions (with identAggregate).
	if typedHaving != nil {
		havingNode = &filterNode{
			source: planDataSource{plan: plan, info: &dataSourceInfo{}},
		}
		plan = havingNode
		havingNode.ivarHelper = parser.MakeIndexedVarHelper(havingNode, 0)

		aggVisitor := extractAggregatesVisitor{
			ctx:        ctx,
			planner:    p,
			groupNode:  group,
			preRender:  r,
			ivarHelper: &havingNode.ivarHelper,
			groupStrs:  groupStrs,
		}
		// havingExpr is the HAVING expression, where any aggregates or grouping
		// expressions have been replaced with havingNode IndexedVars.
		havingExpr, err := aggVisitor.extract(typedHaving)
		if err != nil {
			return nil, nil, err
		}
		// The group.columns have been updated; the sourceColumns in the node must
		// also be updated (for IndexedVarResolvedType to work).
		havingNode.source.info = newSourceInfoForSingleTable(anonymousTable, group.columns)

		havingNode.filter, err = r.planner.analyzeExpr(
			ctx, havingExpr, nil /* no source info */, havingNode.ivarHelper,
			parser.TypeBool, true /* require type */, "HAVING",
		)
		if err != nil {
			return nil, nil, err
		}
	}

	postRender := &renderNode{
		planner: r.planner,
		source:  planDataSource{plan: plan},
	}
	plan = postRender

	// The filterNode and the post renderNode operate on the same schema; append
	// to the IndexedVars that the filter node created.
	postRender.ivarHelper = parser.MakeIndexedVarHelper(postRender, len(group.funcs))

	// Extract any aggregate functions from the select expressions, adding renders
	// to r as needed.
	aggVisitor := extractAggregatesVisitor{
		ctx:        ctx,
		planner:    p,
		groupNode:  group,
		preRender:  r,
		ivarHelper: &postRender.ivarHelper,
		groupStrs:  groupStrs,
	}
	for i, r := range origRenders {
		renderExpr, err := aggVisitor.extract(r)
		if err != nil {
			return nil, nil, err
		}
		postRender.addRenderColumn(renderExpr, origColumns[i])
	}

	postRender.source.info = newSourceInfoForSingleTable(anonymousTable, group.columns)
	postRender.sourceInfo = multiSourceInfo{postRender.source.info}

	// Queries like `SELECT MAX(n) FROM t` expect a row of NULLs if nothing was aggregated.
	group.addNullBucketIfEmpty = len(groupByExprs) == 0

	group.buckets = make(map[string]struct{})

	if log.V(2) {
		strs := make([]string, 0, len(group.funcs))
		for _, f := range group.funcs {
			strs = append(strs, f.expr.String())
		}
		log.Infof(ctx, "Group: %s", strings.Join(strs, ", "))
	}

	group.desiredOrdering = group.desiredAggregateOrdering()

	return plan, group, nil
}

// A groupNode implements the planNode interface and handles the grouping logic.
// It "wraps" a planNode which is used to retrieve the ungrouped results.
type groupNode struct {
	planner *planner

	// The source node (which returns values that feed into the aggregation).
	plan planNode

	// The group-by columns are the first numGroupCols columns of
	// the source plan.
	numGroupCols int

	// funcs are the aggregation functions that the renders use.
	funcs []*aggregateFuncHolder
	// The set of bucket keys. We add buckets as we are processing input rows, and
	// we remove them as we are outputting results.
	buckets   map[string]struct{}
	populated bool

	addNullBucketIfEmpty bool

	columns sqlbase.ResultColumns
	values  parser.Datums

	// desiredOrdering is set only if we are aggregating around a single MIN/MAX
	// function and we can compute the final result using a single row, assuming
	// a specific ordering of the underlying plan.
	desiredOrdering sqlbase.ColumnOrdering
	needOnlyOneRow  bool
	gotOneRow       bool
}

func (n *groupNode) Values() parser.Datums {
	return n.values
}

func (n *groupNode) Start(params runParams) error {
	return n.plan.Start(params)
}

func (n *groupNode) Next(params runParams) (bool, error) {
	var scratch []byte
	// We're going to consume n.plan until it's exhausted (feeding all the rows to
	// n.funcs), and then call n.setupOutput.
	// Subsequent calls to next will skip the first part and just return a result.
	for !n.populated {
		next := false
		if err := params.p.cancelChecker.Check(); err != nil {
			return false, err
		}
		if !(n.needOnlyOneRow && n.gotOneRow) {
			var err error
			next, err = n.plan.Next(params)
			if err != nil {
				return false, err
			}
		}
		if !next {
			n.populated = true
			n.setupOutput()
			break
		}

		// Add row to bucket.

		values := n.plan.Values()

		// TODO(dt): optimization: skip buckets when underlying plan is ordered by grouped values.

		bucket := scratch
		for idx := 0; idx < n.numGroupCols; idx++ {
			var err error
			bucket, err = sqlbase.EncodeDatum(bucket, values[idx])
			if err != nil {
				return false, err
			}
		}

		n.buckets[string(bucket)] = struct{}{}

		// Feed the aggregateFuncHolders for this bucket the non-grouped values.
		for _, f := range n.funcs {
			if f.hasFilter && values[f.filterRenderIdx] != parser.DBoolTrue {
				continue
			}

			var value parser.Datum
			if f.argRenderIdx != noRenderIdx {
				value = values[f.argRenderIdx]
			}

			if err := f.add(params.ctx, n.planner.session, bucket, value); err != nil {
				return false, err
			}
		}
		scratch = bucket[:0]

		n.gotOneRow = true
	}

	if len(n.buckets) == 0 {
		return false, nil
	}
	var bucket string
	// Pick an arbitrary bucket.
	for bucket = range n.buckets {
		break
	}
	delete(n.buckets, bucket)
	for i, f := range n.funcs {
		aggregateFunc, ok := f.buckets[bucket]
		if !ok {
			// No input for this bucket (possible if f has a FILTER).
			// In most cases the result is NULL but there are exceptions
			// (like COUNT).
			aggregateFunc = f.create(&n.planner.evalCtx)
		}
		var err error
		n.values[i], err = aggregateFunc.Result()
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

// setupOutput runs once after all the input rows have been processed. It sets
// up the necessary state to start iterating through the buckets in Next().
func (n *groupNode) setupOutput() {
	if len(n.buckets) < 1 && n.addNullBucketIfEmpty {
		n.buckets[""] = struct{}{}
	}
	n.values = make(parser.Datums, len(n.funcs))
}

func (n *groupNode) Close(ctx context.Context) {
	n.plan.Close(ctx)
	for _, f := range n.funcs {
		f.close(ctx, n.planner.session)
	}
	n.buckets = nil
}

// requiresIsNotNullFilter returns whether a "col IS NOT NULL" constraint must
// be added. This is the case when we have a single MIN/MAX aggregation
// function.
func (n *groupNode) requiresIsNotNullFilter() bool {
	return len(n.desiredOrdering) == 1
}

// isNotNullFilter adds as a "col IS NOT NULL" constraint to the filterNode
// (which is under the renderNode).
func (n *groupNode) addIsNotNullFilter(where *filterNode, render *renderNode) {
	if !n.requiresIsNotNullFilter() {
		panic("IS NOT NULL filter not required")
	}
	isNotNull := parser.NewTypedComparisonExpr(
		parser.IsNot,
		where.ivarHelper.Rebind(
			render.render[n.desiredOrdering[0].ColIdx],
			false, // alsoReset
			true,  // normalizeToNonNil
		),
		parser.DNull,
	)
	if where.filter == nil {
		where.filter = isNotNull
	} else {
		where.filter = parser.NewTypedAndExpr(where.filter, isNotNull)
	}
}

// desiredAggregateOrdering computes the desired output ordering from the
// scan.
//
// We only have a desired ordering if we have a single MIN or MAX aggregation
// with a simple column argument and there is no GROUP BY.
func (n *groupNode) desiredAggregateOrdering() sqlbase.ColumnOrdering {
	if n.numGroupCols > 0 {
		return nil
	}

	if len(n.funcs) != 1 {
		return nil
	}
	f := n.funcs[0]
	impl := f.create(&n.planner.evalCtx)
	switch impl.(type) {
	case *parser.MinAggregate:
		return sqlbase.ColumnOrdering{{ColIdx: f.argRenderIdx, Direction: encoding.Ascending}}
	case *parser.MaxAggregate:
		return sqlbase.ColumnOrdering{{ColIdx: f.argRenderIdx, Direction: encoding.Descending}}
	}
	return nil
}

// extractAggregatesVisitor extracts arguments to aggregate functions and adds
// them to the preRender renderNode. It returns new expression where arguments
// to aggregate functions (as well as expressions that also appear in a GROUP
// BY) are replaced with IndexedVars suitable for a node that has the groupNode
// as a data source - namely a renderNode or a filterNode (for HAVING).
type extractAggregatesVisitor struct {
	ctx       context.Context
	planner   *planner
	groupNode *groupNode
	// preRender is the render node that feeds its output into the groupNode.
	preRender *renderNode
	// ivarHelper is associated with a node above the groupNode, either a
	// filterNode (for HAVING) or a renderNode.
	ivarHelper *parser.IndexedVarHelper
	groupStrs  groupByStrMap
	err        error
}

var _ parser.Visitor = &extractAggregatesVisitor{}

// addAggregation adds an aggregateFuncHolder to the groupNode funcs and returns
// an IndexedVar that refers to the index of the function.
func (v *extractAggregatesVisitor) addAggregation(f *aggregateFuncHolder) *parser.IndexedVar {
	// TODO(radu): we could check for duplicate aggregations here and reuse
	// them; useful for cases like
	//   SELECT SUM(x), y FROM t GROUP BY y HAVING SUM(x) > 0
	v.groupNode.funcs = append(v.groupNode.funcs, f)

	renderIdx := v.ivarHelper.AppendSlot()
	if renderIdx != len(v.groupNode.funcs)-1 {
		panic(fmt.Sprintf(
			"no 1-1 correspondence between funcs %v and %d indexed vars",
			v.groupNode.funcs, renderIdx,
		))
	}
	// We care about the name of the groupNode columns as an optimization: we want
	// them to match the post-render node's columns if the post-render expressions
	// are trivial (so the renderNode can be elided).
	colName, err := getRenderColName(v.planner.session.SearchPath, parser.SelectExpr{Expr: f.expr})
	if err != nil {
		colName = fmt.Sprintf("agg%d", renderIdx)
	} else if strings.ToLower(colName) == "count_rows()" {
		// Special case: count(*) expressions are converted to count_rows(); since
		// typical usage is count(*), use that column name instead of count_rows(),
		// allowing elision of the renderNode.
		// TODO(radu): remove this if #16535 is resolved.
		colName = "count(*)"
	}
	v.groupNode.columns = append(v.groupNode.columns, sqlbase.ResultColumn{
		Name: colName,
		Typ:  f.expr.ResolvedType(),
	})
	return v.ivarHelper.IndexedVar(renderIdx)
}

func (v *extractAggregatesVisitor) VisitPre(expr parser.Expr) (recurse bool, newExpr parser.Expr) {
	if v.err != nil {
		return false, expr
	}

	if groupIdx, ok := v.groupStrs[symbolicExprStr(expr)]; ok {
		// This expression is in the GROUP BY; it is already being rendered by the
		// renderNode.
		if groupIdx == -1 {
			// We use this special value to indicate a star GROUP BY.
			// TODO(radu): to support this we need to store all the render indices in
			// groupStrs, and create a tuple of IndexedVars. Also see #15750.
			v.err = errors.New("star expressions not supported with grouping")
			return false, expr
		}
		f := v.groupNode.newAggregateFuncHolder(
			v.preRender.render[groupIdx], groupIdx, true /* ident */, parser.NewIdentAggregate,
		)

		return false, v.addAggregation(f)
	}

	switch t := expr.(type) {
	case *parser.FuncExpr:
		if agg := t.GetAggregateConstructor(); agg != nil {
			var f *aggregateFuncHolder
			switch len(t.Exprs) {
			case 0:
				// COUNT_ROWS has no arguments.
				f = v.groupNode.newAggregateFuncHolder(t, noRenderIdx, false /* not ident */, agg)

			case 1:
				argExpr := t.Exprs[0].(parser.TypedExpr)

				if err := v.planner.parser.AssertNoAggregationOrWindowing(
					argExpr,
					fmt.Sprintf("the argument of %s()", t.Func),
					v.planner.session.SearchPath,
				); err != nil {
					v.err = err
					return false, expr
				}

				// Add a render for the argument.
				col := sqlbase.ResultColumn{
					Name: argExpr.String(),
					Typ:  argExpr.ResolvedType(),
				}

				argRenderIdx := v.preRender.addOrReuseRender(col, argExpr, true /* reuse */)

				f = v.groupNode.newAggregateFuncHolder(t, argRenderIdx, false /* not ident */, agg)

			default:
				// TODO: #10495
				v.err = pgerror.UnimplementedWithIssueErrorf(10495, "aggregate functions with multiple arguments are not supported yet")
				return false, expr
			}

			if t.Type == parser.DistinctFuncType {
				f.setDistinct()
			}

			if t.Filter != nil {
				filterExpr := t.Filter.(parser.TypedExpr)

				if err := v.planner.parser.AssertNoAggregationOrWindowing(
					filterExpr, "FILTER", v.planner.session.SearchPath,
				); err != nil {
					v.err = err
					return false, expr
				}

				col, renderExpr, err := v.planner.computeRender(
					v.ctx,
					parser.SelectExpr{Expr: filterExpr},
					parser.TypeBool,
					v.preRender.sourceInfo,
					v.preRender.ivarHelper,
					autoGenerateRenderOutputName,
				)
				if err != nil {
					v.err = err
					return false, expr
				}

				filterRenderIdx := v.preRender.addOrReuseRender(col, renderExpr, true /* reuse */)
				f.setFilter(filterRenderIdx)
			}
			return false, v.addAggregation(f)
		}

	case *parser.IndexedVar:
		v.err = errors.Errorf(
			"column \"%s\" must appear in the GROUP BY clause or be used in an aggregate function", t,
		)
		return false, expr
	}

	return true, expr
}

func (*extractAggregatesVisitor) VisitPost(expr parser.Expr) parser.Expr { return expr }

// extract aggregateFuncHolders from exprs that use aggregation and add them to
// the groupNode.
func (v extractAggregatesVisitor) extract(typedExpr parser.TypedExpr) (parser.TypedExpr, error) {
	v.err = nil
	expr, _ := parser.WalkExpr(&v, typedExpr)
	return expr.(parser.TypedExpr), v.err
}

type aggregateFuncHolder struct {
	// expr must either contain an aggregation function (SUM, COUNT, etc.) or an
	// expression that also appears as one of the GROUP BY expressions (v+w in
	// SELECT v+w FROM kvw GROUP BY v+w).
	expr parser.TypedExpr

	// The argument of the function is a single value produced by the renderNode
	// underneath.
	argRenderIdx int
	hasFilter    bool
	// If there is a filter, the result is a single value produced by the
	// renderNode underneath.
	filterRenderIdx int

	identAggregate bool

	create        func(*parser.EvalContext) parser.AggregateFunc
	group         *groupNode
	buckets       map[string]parser.AggregateFunc
	bucketsMemAcc WrappableMemoryAccount
	seen          map[string]struct{}
}

const noRenderIdx = -1

func (n *groupNode) newAggregateFuncHolder(
	expr parser.TypedExpr,
	argRenderIdx int,
	identAggregate bool,
	create func(*parser.EvalContext) parser.AggregateFunc,
) *aggregateFuncHolder {
	res := &aggregateFuncHolder{
		expr:           expr,
		argRenderIdx:   argRenderIdx,
		create:         create,
		group:          n,
		identAggregate: identAggregate,
		buckets:        make(map[string]parser.AggregateFunc),
		bucketsMemAcc:  n.planner.session.TxnState.OpenAccount(),
	}
	return res
}

func (a *aggregateFuncHolder) setFilter(filterRenderIdx int) {
	a.hasFilter = true
	a.filterRenderIdx = filterRenderIdx
}

// setDistinct causes a to ignore duplicate values of the argument.
func (a *aggregateFuncHolder) setDistinct() {
	a.seen = make(map[string]struct{})
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
