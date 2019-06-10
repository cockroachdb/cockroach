// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// A groupNode implements the planNode interface and handles the grouping logic.
// It "wraps" a planNode which is used to retrieve the ungrouped results.
type groupNode struct {
	// The schema for this groupNode.
	columns sqlbase.ResultColumns

	// desiredOrdering is set only if we are aggregating around a single MIN/MAX
	// function and we can compute the final result using a single row, assuming
	// a specific ordering of the underlying plan.
	desiredOrdering sqlbase.ColumnOrdering

	// needOnlyOneRow determines whether aggregation should stop as soon
	// as one result row can be computed.
	needOnlyOneRow bool

	// The source node (which returns values that feed into the aggregation).
	plan planNode

	// Indices of the group by columns in the source plan.
	groupCols []int

	// Set when we have an input ordering on (a subset of) grouping columns. Only
	// column indices in groupCols can appear in this ordering.
	groupColOrdering sqlbase.ColumnOrdering

	// isScalar is set for "scalar groupby", where we want a result
	// even if there are no input rows, e.g. SELECT MIN(x) FROM t.
	isScalar bool

	// funcs are the aggregation functions that the renders use.
	funcs []*aggregateFuncHolder

	props physicalProps
}

// groupBy constructs a planNode "complex" consisting of a groupNode and other
// post-processing nodes according to grouping functions or clauses.
//
// The complex always includes a groupNode which initially uses the given
// renderNode as its own data source; the data source can be changed later with
// an equivalent one if the renderNode is optimized out.
//
// Note that this function clobbers the planner's semaCtx. The caller is responsible
// for saving and restoring the Properties field.
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
	ctx context.Context, n *tree.SelectClause, r *renderNode,
) (planNode, *groupNode, error) {
	// Determine if aggregation is being performed. This check is done on the raw
	// Select expressions as simplification might have removed aggregation
	// functions (e.g. `SELECT MIN(1)` -> `SELECT 1`).
	if n.Having == nil && len(n.GroupBy) == 0 && !r.renderProps.SeenAggregate {
		return nil, nil, nil
	}
	if n.Having != nil && len(n.From.Tables) == 0 {
		return nil, nil, pgerror.UnimplementedWithIssue(26349, "HAVING clause without FROM")
	}

	groupByExprs := make([]tree.Expr, len(n.GroupBy))

	// In the construction of the renderNode, when renders are processed (via
	// computeRender()), the expressions are normalized. In order to compare these
	// normalized render expressions to GROUP BY expressions, we need to normalize
	// the GROUP BY expressions as well. This is done before determining if
	// aggregation is being performed, because that determination is made during
	// validation, which will require matching expressions.
	for i, expr := range n.GroupBy {
		expr = tree.StripParens(expr)

		// Check whether the GROUP BY clause refers to a rendered column
		// (specified in the original query) by index, e.g. `SELECT a, SUM(b)
		// FROM y GROUP BY 1`.
		col, err := p.colIndex(r.numOriginalCols, expr, "GROUP BY")
		if err != nil {
			return nil, nil, err
		}

		if col != -1 {
			groupByExprs[i] = n.Exprs[col].Expr
		} else {
			groupByExprs[i] = expr
		}
	}

	group := &groupNode{
		plan: r,
	}

	// We replace the columns in the underlying renderNode with what the
	// groupNode needs as input:
	//  - group by expressions
	//  - arguments to the aggregate expressions
	//  - having expressions
	origRenders := r.render
	origColumns := r.columns
	r.resetRenderColumns(nil, nil)
	r.renderProps.Clear()

	// Add the group-by expressions.

	// groupStrs maps a GROUP BY expression string to the index of the column in
	// the underlying renderNode. This is used as an optimization when analyzing
	// the arguments of aggregate functions: if an argument is already grouped,
	// and thus rendered, the rendered expression can be used as argument to
	// the aggregate function directly; there is no need to add a render. See
	// extractAggregatesVisitor below.
	groupStrs := make(groupByStrMap, len(groupByExprs))

	// We need to ensure there are no special function in the clause.
	p.semaCtx.Properties.Require("GROUP BY", tree.RejectSpecial)

	for _, g := range groupByExprs {
		cols, exprs, hasStar, err := p.computeRenderAllowingStars(
			ctx, tree.SelectExpr{Expr: g}, types.Any, r.sourceInfo, r.ivarHelper,
			autoGenerateRenderOutputName)
		if err != nil {
			return nil, nil, err
		}
		p.curPlan.hasStar = p.curPlan.hasStar || hasStar

		// GROUP BY (a, b) -> GROUP BY a, b
		cols, exprs = flattenTuples(cols, exprs, &r.ivarHelper)

		colIdxs := r.addOrReuseRenders(cols, exprs, true /* reuseExistingRender */)
		if len(colIdxs) == 1 {
			// We only remember the render if there is a 1:1 correspondence with
			// the expression written after GROUP BY and the computed renders.
			// This may not be true e.g. when there is a star expansion like
			// GROUP BY kv.*.
			groupStrs[symbolicExprStr(g)] = colIdxs[0]
		}
		// Also remember all the rendered sub-expressions, if there was an
		// expansion. This enables reuse of all the actual grouping expressions.
		for i, e := range exprs {
			groupStrs[symbolicExprStr(e)] = colIdxs[i]
		}
	}
	// Because of the way we set up the pre-projection above, the grouping columns
	// are always the first columns.
	group.groupCols = make([]int, len(r.render))
	for i := range group.groupCols {
		group.groupCols[i] = i
	}

	var havingNode *filterNode
	plan := planNode(group)

	// Extract any aggregate functions from having expressions, adding renders to
	// r as needed.
	//
	// "Grouping expressions" - expressions that also show up under GROUP BY - are
	// also treated as aggregate expressions (any_not_null aggregation).
	// Normalize and check the HAVING expression too if it exists.
	if n.Having != nil {
		havingNode = &filterNode{
			source: planDataSource{
				plan: plan,
				info: r.source.info, // Note: info is re-populated below.
			},
		}
		havingNode.ivarHelper = tree.MakeIndexedVarHelper(havingNode, len(r.source.info.SourceColumns))
		plan = havingNode

		// We allow aggregates in HAVING clause, but not other special functions.
		p.semaCtx.Properties.Require("HAVING", tree.RejectWindowApplications|tree.RejectGenerators)

		// Semantically analyze the HAVING expression.
		var err error
		havingNode.filter, err = p.analyzeExpr(
			ctx, n.Having.Expr, r.sourceInfo, havingNode.ivarHelper,
			types.Bool, true /* require type */, "HAVING",
		)
		if err != nil {
			return nil, nil, err
		}

		havingNode.ivarHelper = tree.MakeIndexedVarHelper(havingNode, 0)
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
		havingNode.filter, err = aggVisitor.extract(havingNode.filter)
		if err != nil {
			return nil, nil, err
		}

		// The having node is being plugged to the groupNode as source.
		// We reuse the group node's column names and types.
		havingNode.source.info = sqlbase.NewSourceInfoForSingleTable(
			sqlbase.AnonymousTable, group.columns,
		)
	}

	postRender := &renderNode{
		source: planDataSource{plan: plan},
	}
	plan = postRender

	// The filterNode and the post renderNode operate on the same schema; append
	// to the IndexedVars that the filter node created.
	postRender.ivarHelper = tree.MakeIndexedVarHelper(postRender, len(group.funcs))

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
		postRender.addRenderColumn(renderExpr, symbolicExprStr(renderExpr), origColumns[i])
	}

	postRender.source.info = sqlbase.NewSourceInfoForSingleTable(
		sqlbase.AnonymousTable, group.columns,
	)
	postRender.sourceInfo = sqlbase.MultiSourceInfo{postRender.source.info}

	// Queries like `SELECT MAX(n) FROM t` expect a row of NULLs if nothing was aggregated.
	group.isScalar = len(groupByExprs) == 0

	if log.V(2) {
		strs := make([]string, 0, len(group.funcs))
		for _, f := range group.funcs {
			strs = append(strs, f.funcName)
		}
		log.Infof(ctx, "Group: %s", strings.Join(strs, ", "))
	}

	group.desiredOrdering = group.desiredAggregateOrdering(p.EvalContext())

	return plan, group, nil
}

func (n *groupNode) startExec(params runParams) error {
	panic("groupNode cannot be run in local mode")
}

func (n *groupNode) Next(params runParams) (bool, error) {
	panic("groupNode cannot be run in local mode")
}

func (n *groupNode) Values() tree.Datums {
	panic("groupNode cannot be run in local mode")
}

func (n *groupNode) Close(ctx context.Context) {
	n.plan.Close(ctx)
	for _, f := range n.funcs {
		f.close(ctx)
	}
}

// requiresIsDistinctFromNullFilter returns whether a
// "col IS DISTINCT FROM NULL" constraint must be added. This is the case when
// we have a single MIN/MAX aggregation function.
func (n *groupNode) requiresIsDistinctFromNullFilter() bool {
	return len(n.desiredOrdering) == 1
}

// isNotNullFilter adds as a "col IS DISTINCT FROM NULL" constraint to the filterNode
// (which is under the renderNode).
func (n *groupNode) addIsDistinctFromNullFilter(where *filterNode, render *renderNode) {
	if !n.requiresIsDistinctFromNullFilter() {
		panic("IS DISTINCT FROM NULL filter not required")
	}
	isDistinctFromNull := tree.NewTypedComparisonExpr(
		tree.IsDistinctFrom,
		where.ivarHelper.Rebind(
			render.render[n.desiredOrdering[0].ColIdx],
			false, // alsoReset
			true,  // normalizeToNonNil
		),
		tree.DNull,
	)
	if where.filter == nil {
		where.filter = isDistinctFromNull
	} else {
		where.filter = tree.NewTypedAndExpr(where.filter, isDistinctFromNull)
	}
}

// desiredAggregateOrdering computes the desired output ordering from the
// scan.
//
// We only have a desired ordering if we have a single MIN or MAX aggregation
// with a simple column argument and there is no GROUP BY.
//
// TODO(knz/radu): it's expensive to instantiate the aggregate
// function here just to determine whether it's a min or max. We could
// instead have another variable (e.g. from the AST) tell us what type
// of aggregation we're dealing with, and test that here.
func (n *groupNode) desiredAggregateOrdering(evalCtx *tree.EvalContext) sqlbase.ColumnOrdering {
	if len(n.groupCols) > 0 {
		return nil
	}

	if len(n.funcs) != 1 {
		return nil
	}
	f := n.funcs[0]
	impl := f.create(evalCtx, nil /* arguments */)
	switch impl.(type) {
	case *builtins.MinAggregate:
		return sqlbase.ColumnOrdering{{ColIdx: f.argRenderIdx, Direction: encoding.Ascending}}
	case *builtins.MaxAggregate:
		return sqlbase.ColumnOrdering{{ColIdx: f.argRenderIdx, Direction: encoding.Descending}}
	}
	return nil
}

// aggIsGroupingColumn returns true if the given output aggregation is an
// any_not_null aggregation for a grouping column. The grouping column
// index is also returned.
func (n *groupNode) aggIsGroupingColumn(aggIdx int) (colIdx int, ok bool) {
	if holder := n.funcs[aggIdx]; holder.funcName == builtins.AnyNotNull {
		for _, c := range n.groupCols {
			if c == holder.argRenderIdx {
				return c, true
			}
		}
	}
	return -1, false
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
	ivarHelper *tree.IndexedVarHelper
	groupStrs  groupByStrMap
	err        error
}

// groupByStrMap maps each GROUP BY expression string to the index of the column
// in the underlying renderNode that renders this expression.
// For stars (GROUP BY k.*) the special value -1 is used.
type groupByStrMap map[string]int

var _ tree.Visitor = &extractAggregatesVisitor{}

// addAggregation adds an aggregateFuncHolder to the groupNode funcs and returns
// an IndexedVar that refers to the index of the function.
func (v *extractAggregatesVisitor) addAggregation(f *aggregateFuncHolder) *tree.IndexedVar {
	for i, g := range v.groupNode.funcs {
		if aggregateFuncsEqual(f, g) {
			return v.ivarHelper.IndexedVarWithType(i, f.resultType)
		}
	}
	v.groupNode.funcs = append(v.groupNode.funcs, f)

	renderIdx := v.ivarHelper.AppendSlot()
	if renderIdx != len(v.groupNode.funcs)-1 {
		panic(fmt.Sprintf(
			"no 1-1 correspondence between funcs %v and %d indexed vars",
			v.groupNode.funcs, renderIdx,
		))
	}
	// Make up a column name. If we need a particular name to elide a renderNode,
	// the column will be renamed.
	v.groupNode.columns = append(v.groupNode.columns, sqlbase.ResultColumn{
		Name: fmt.Sprintf("agg%d", renderIdx),
		Typ:  f.resultType,
	})
	return v.ivarHelper.IndexedVarWithType(renderIdx, f.resultType)
}

func (v *extractAggregatesVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if v.err != nil {
		return false, expr
	}

	if groupIdx, ok := v.groupStrs[symbolicExprStr(expr)]; ok {
		// This expression is in the GROUP BY; it is already being rendered by the
		// renderNode. Set up an any_not_null aggregation.
		f := v.groupNode.newAggregateFuncHolder(
			builtins.AnyNotNull,
			v.preRender.render[groupIdx].ResolvedType(),
			groupIdx,
			builtins.NewAnyNotNullAggregate,
			nil,
			v.planner.EvalContext().Mon.MakeBoundAccount(),
		)

		return false, v.addAggregation(f)
	}

	switch t := expr.(type) {
	case *tree.FuncExpr:
		if agg := t.GetAggregateConstructor(); agg != nil {
			var f *aggregateFuncHolder
			if len(t.Exprs) == 0 {
				// COUNT_ROWS has no arguments.
				f = v.groupNode.newAggregateFuncHolder(
					t.Func.String(),
					t.ResolvedType(),
					noRenderIdx,
					agg,
					nil,
					v.planner.EvalContext().Mon.MakeBoundAccount(),
				)
			} else {
				// Only the first argument can be an expression, all the following ones
				// must be consts. So before we proceed, they must be checked.
				arguments := make(tree.Datums, len(t.Exprs)-1)
				if len(t.Exprs) > 1 {
					evalContext := v.planner.EvalContext()
					for i := 1; i < len(t.Exprs); i++ {
						if !tree.IsConst(evalContext, t.Exprs[i]) {
							v.err = pgerror.UnimplementedWithIssue(28417, "aggregate functions with multiple non-constant expressions are not supported")
							return false, expr
						}
						var err error
						arguments[i-1], err = t.Exprs[i].(tree.TypedExpr).Eval(evalContext)
						if err != nil {
							v.err = errors.AssertionFailedf("can't evaluate %s - %v", t.Exprs[i].String(), err)
							return false, expr
						}
					}
				}

				argExpr := t.Exprs[0].(tree.TypedExpr)

				// TODO(knz): it's really a shame that we need to recurse
				// through the sub-tree to determine whether the arguments
				// don't contain invalid functions. This really would want to
				// be checked on the return path of the recursion.
				// See issue #26425.
				if v.planner.txCtx.WindowFuncInExpr(argExpr) {
					v.err = sqlbase.NewWindowInAggError()
					return false, expr
				} else if v.planner.txCtx.AggregateInExpr(argExpr, v.planner.SessionData().SearchPath) {
					v.err = sqlbase.NewAggInAggError()
					return false, expr
				}

				// Add a pre-rendering for the argument.
				col := sqlbase.ResultColumn{
					Name: argExpr.String(),
					Typ:  argExpr.ResolvedType(),
				}

				argRenderIdx := v.preRender.addOrReuseRender(col, argExpr, true /* reuse */)

				f = v.groupNode.newAggregateFuncHolder(
					t.Func.String(),
					t.ResolvedType(),
					argRenderIdx,
					agg,
					arguments,
					v.planner.EvalContext().Mon.MakeBoundAccount(),
				)
			}

			if t.Type == tree.DistinctFuncType {
				f.setDistinct()
			}

			if t.Filter != nil {
				filterExpr := t.Filter.(tree.TypedExpr)

				// We need to save and restore the previous value of the field in
				// semaCtx in case we are recursively called within a subquery
				// context.
				scalarProps := &v.planner.semaCtx.Properties
				defer scalarProps.Restore(*scalarProps)
				scalarProps.Require("FILTER", tree.RejectSpecial)

				col, renderExpr, err := v.planner.computeRender(
					v.ctx,
					tree.SelectExpr{Expr: filterExpr},
					types.Bool,
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

	case *tree.IndexedVar:
		v.err = pgerror.Newf(pgerror.CodeGroupingError,
			"column \"%s\" must appear in the GROUP BY clause or be used in an aggregate function",
			t)
		return false, expr
	}

	return true, expr
}

func (*extractAggregatesVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }

// extract aggregateFuncHolders from exprs that use aggregation and add them to
// the groupNode.
func (v extractAggregatesVisitor) extract(typedExpr tree.TypedExpr) (tree.TypedExpr, error) {
	v.err = nil
	expr, _ := tree.WalkExpr(&v, typedExpr)
	return expr.(tree.TypedExpr), v.err
}

type aggregateFuncHolder struct {
	// Name of the aggregate function. Empty if this column reproduces a bucket
	// key unchanged.
	funcName string

	resultType *types.T

	// The argument of the function is a single value produced by the renderNode
	// underneath. If the function has no argument (COUNT_ROWS), it is set to
	// noRenderIdx.
	argRenderIdx int
	// If there is a filter, the result is a single value produced by the
	// renderNode underneath. If there is no filter, it is set to noRenderIdx.
	filterRenderIdx int

	// create instantiates the built-in execution context for the
	// aggregation function.
	create func(*tree.EvalContext, tree.Datums) tree.AggregateFunc

	// arguments are constant expressions that can be optionally passed into an
	// aggregator.
	arguments tree.Datums

	run aggregateFuncRun
}

// aggregateFuncRun contains the run-time state for one aggregation function
// during local execution.
type aggregateFuncRun struct {
	buckets       map[string]tree.AggregateFunc
	bucketsMemAcc mon.BoundAccount
	seen          map[string]struct{}
}

const noRenderIdx = -1

// newAggregateFuncHolder creates an aggregateFuncHolder.
//
// If function is nil, this is an "ident" aggregation (meaning that the input is
// a group-by column and the "aggregation" returns its value)
//
// If the aggregation function takes no arguments (e.g. COUNT_ROWS),
// argRenderIdx is noRenderIdx.
func (n *groupNode) newAggregateFuncHolder(
	funcName string,
	resultType *types.T,
	argRenderIdx int,
	create func(*tree.EvalContext, tree.Datums) tree.AggregateFunc,
	arguments tree.Datums,
	acc mon.BoundAccount,
) *aggregateFuncHolder {
	res := &aggregateFuncHolder{
		funcName:        funcName,
		resultType:      resultType,
		argRenderIdx:    argRenderIdx,
		filterRenderIdx: noRenderIdx,
		create:          create,
		arguments:       arguments,
		run: aggregateFuncRun{
			buckets:       make(map[string]tree.AggregateFunc),
			bucketsMemAcc: acc,
		},
	}
	return res
}

func (a *aggregateFuncHolder) setFilter(filterRenderIdx int) {
	a.filterRenderIdx = filterRenderIdx
}

func (a *aggregateFuncHolder) hasFilter() bool {
	return a.filterRenderIdx != noRenderIdx
}

// setDistinct causes a to ignore duplicate values of the argument.
func (a *aggregateFuncHolder) setDistinct() {
	a.run.seen = make(map[string]struct{})
}

// isDistinct returns true if only distinct values are aggregated,
// e.g. SUM(DISTINCT x).
func (a *aggregateFuncHolder) isDistinct() bool {
	return a.run.seen != nil
}

func aggregateFuncsEqual(a, b *aggregateFuncHolder) bool {
	return a.funcName == b.funcName && a.resultType == b.resultType &&
		a.argRenderIdx == b.argRenderIdx && a.filterRenderIdx == b.filterRenderIdx
}

func (a *aggregateFuncHolder) close(ctx context.Context) {
	for _, aggFunc := range a.run.buckets {
		aggFunc.Close(ctx)
	}

	a.run.buckets = nil
	a.run.seen = nil

	a.run.bucketsMemAcc.Close(ctx)
}
