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

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
)

// groupBy constructs a groupNode according to grouping functions or clauses. This may adjust the
// render targets in the selectNode as necessary.
func (p *planner) groupBy(n *parser.SelectClause, s *selectNode) (*groupNode, error) {
	// Determine if aggregation is being performed. This check is done on the raw
	// Select expressions as simplification might have removed aggregation
	// functions (e.g. `SELECT MIN(1)` -> `SELECT 1`).
	if isAggregate := p.parser.IsAggregate(n); !isAggregate {
		return nil, nil
	}

	groupBy := append([]parser.Expr(nil), n.GroupBy...)

	// Start by normalizing the GROUP BY expressions (to match what has been done to
	// the SELECT expressions in addRender) so that we can compare to them later.
	// This is done before determining if aggregation is being performed, because
	// that determination is made during validation, which will require matching
	// expressions.
	for i := range groupBy {
		if p.parser.AggregateInExpr(groupBy[i]) {
			return nil, fmt.Errorf("aggregate functions are not allowed in GROUP BY")
		}

		// We do not need to fully analyze the GROUP BY expression here
		// (as per analyzeExpr) because this is taken care of by addRender
		// below.
		resolved, err := resolveQNames(groupBy[i], s.sourceInfo, s.qvals, &p.qnameVisitor)
		if err != nil {
			return nil, err
		}

		// If a col index is specified, replace it with that expression first.
		// NB: This is not a deep copy, and thus when extractAggregatesVisitor runs
		// on s.render, the GroupBy expressions can contain wrapped qvalues.
		// aggregateFuncHolder's Eval() method handles being called during grouping.
		if col, err := colIndex(s.numOriginalCols, resolved); err != nil {
			return nil, err
		} else if col >= 0 {
			groupBy[i] = s.render[col]
		} else {
			groupBy[i] = resolved
		}
	}

	// Normalize and check the HAVING expression too if it exists.
	var typedHaving parser.TypedExpr
	var err error
	if n.Having != nil {
		typedHaving, err = p.analyzeExpr(n.Having.Expr, s.sourceInfo, s.qvals,
			parser.TypeBool, true, "HAVING")
		if err != nil {
			return nil, err
		}
		n.Having.Expr = typedHaving
	}

	group := &groupNode{
		planner: p,
		values:  valuesNode{columns: s.columns},
		render:  s.render,
	}

	visitor := extractAggregatesVisitor{
		n:           group,
		groupStrs:   make(map[string]struct{}, len(groupBy)),
		groupedCopy: new(extractAggregatesVisitor),
	}

	for _, e := range groupBy {
		visitor.groupStrs[e.String()] = struct{}{}
	}

	// A copy of the visitor that is used when a subtree appears in the GROUP BY.
	// One copy is allocated up-front, rather than potentially several on-the-fly,
	// to reduce allocations.
	*visitor.groupedCopy = visitor
	visitor.groupedCopy.groupedCopy = nil

	// Loop over the render expressions and extract any aggregate functions --
	// qvalues are also replaced (with identAggregates, which just return the last
	// value added to them for a bucket) to provide grouped-by values for each bucket.
	// After extraction, group.render will be entirely rendered from aggregateFuncHolders,
	// and group.funcs will contain all the functions which need to be fed values.
	for i := range group.render {
		typedExpr, err := visitor.extract(group.render[i])
		if err != nil {
			return nil, err
		}
		group.render[i] = typedExpr
	}

	if typedHaving != nil {
		var err error
		typedHaving, err = visitor.extract(typedHaving)
		if err != nil {
			return nil, err
		}
		group.having = typedHaving
	}

	// Queries like `SELECT MAX(n) FROM t` expect a row of NULLs if nothing was aggregated.
	group.addNullBucketIfEmpty = len(groupBy) == 0

	group.buckets = make(map[string]struct{})

	if log.V(2) {
		strs := make([]string, 0, len(group.funcs))
		for _, f := range group.funcs {
			strs = append(strs, f.String())
		}
		log.Infof(p.ctx(), "Group: %s", strings.Join(strs, ", "))
	}

	// Replace the render expressions in the scanNode with expressions that
	// compute only the arguments to the aggregate expressions.
	s.render = make([]parser.TypedExpr, len(group.funcs))
	for i, f := range group.funcs {
		s.render[i] = f.arg
	}

	// Add the group-by expressions so they are available for bucketing.
	for _, g := range groupBy {
		if err := s.addRender(parser.SelectExpr{Expr: g}, nil); err != nil {
			return nil, err
		}
	}

	group.desiredOrdering = desiredAggregateOrdering(group.funcs)
	return group, nil
}

// A groupNode implements the planNode interface and handles the grouping logic.
// It "wraps" a planNode which is used to retrieve the ungrouped results.
type groupNode struct {
	planner *planner

	// The "wrapped" node (which returns ungrouped results).
	plan planNode

	render []parser.TypedExpr
	having parser.TypedExpr

	funcs []*aggregateFuncHolder
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

func (n *groupNode) Columns() []ResultColumn {
	return n.values.Columns()
}

func (n *groupNode) Ordering() orderingInfo {
	// TODO(dt): aggregate buckets are returned un-ordered for now.
	return orderingInfo{}
}

func (n *groupNode) Values() parser.DTuple {
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

func (n *groupNode) expandPlan() error {
	// We do not need to recurse into the child node here; selectTopNode
	// does this for us.

	for _, e := range n.render {
		if err := n.planner.expandSubqueryPlans(e); err != nil {
			return err
		}
	}

	if err := n.planner.expandSubqueryPlans(n.having); err != nil {
		return err
	}

	if len(n.desiredOrdering) > 0 {
		match := computeOrderingMatch(n.desiredOrdering, n.plan.Ordering(), false)
		if match == len(n.desiredOrdering) {
			// We have a single MIN/MAX function and the underlying plan's
			// ordering matches the function. We only need to retrieve one row.
			n.plan.SetLimitHint(1, false /* !soft */)
			n.needOnlyOneRow = true
		}
	}
	return nil
}

func (n *groupNode) Start() error {
	if err := n.plan.Start(); err != nil {
		return err
	}

	for _, e := range n.render {
		if err := n.planner.startSubqueryPlans(e); err != nil {
			return err
		}
	}

	return n.planner.startSubqueryPlans(n.having)
}

func (n *groupNode) Next() (bool, error) {
	var scratch []byte
	for !n.populated {
		next := false
		if !(n.needOnlyOneRow && n.gotOneRow) {
			var err error
			next, err = n.plan.Next()
			if err != nil {
				return false, err
			}
		}
		if !next {
			if err := n.computeAggregates(); err != nil {
				return false, err
			}
			n.populated = true
			break
		}
		if n.explain == explainDebug && n.plan.DebugValues().output != debugValueRow {
			// Pass through non-row debug values.
			return true, nil
		}

		// Add row to bucket.

		values := n.plan.Values()
		aggregatedValues, groupedValues := values[:len(n.funcs)], values[len(n.funcs):]

		// TODO(dt): optimization: skip buckets when underlying plan is ordered by grouped values.

		encoded, err := sqlbase.EncodeDTuple(scratch, groupedValues)
		if err != nil {
			return false, err
		}

		n.buckets[string(encoded)] = struct{}{}

		// Feed the aggregateFuncHolders for this bucket the non-grouped values.
		for i, value := range aggregatedValues {
			if err := n.funcs[i].add(encoded, value); err != nil {
				return false, err
			}
		}
		scratch = encoded[:0]

		n.gotOneRow = true

		if n.explain == explainDebug {
			// Emit a "buffered" row.
			return true, nil
		}
	}

	return n.values.Next()
}

func (n *groupNode) computeAggregates() error {
	if len(n.buckets) < 1 && n.addNullBucketIfEmpty {
		n.buckets[""] = struct{}{}
	}

	// Since this controls Eval behavior of aggregateFuncHolder, it is not set until init is complete.
	n.populated = true

	// Render the results.
	n.values.rows = make([]parser.DTuple, 0, len(n.buckets))
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

		row := make(parser.DTuple, 0, len(n.render))
		for _, r := range n.render {
			res, err := r.Eval(&n.planner.evalCtx)
			if err != nil {
				return err
			}
			row = append(row, res)
		}

		n.values.rows = append(n.values.rows, row)
	}
	return nil
}

func (n *groupNode) ExplainPlan(_ bool) (name, description string, children []planNode) {
	name = "group"
	var buf bytes.Buffer
	for i, f := range n.funcs {
		if i > 0 {
			buf.WriteString(", ")
		}
		f.Format(&buf, parser.FmtSimple)
	}

	subplans := []planNode{n.plan}
	for _, e := range n.render {
		subplans = n.planner.collectSubqueryPlans(e, subplans)
	}
	if n.having != nil {
		buf.WriteString(" HAVING ")
		n.having.Format(&buf, parser.FmtSimple)
		subplans = n.planner.collectSubqueryPlans(n.having, subplans)
	}
	return name, buf.String(), subplans
}

func (n *groupNode) ExplainTypes(regTypes func(string, string)) {
	if n.having != nil {
		regTypes("having", parser.AsStringWithFlags(n.having, parser.FmtShowTypes))
	}
	cols := n.Columns()
	for i, rexpr := range n.render {
		regTypes(fmt.Sprintf("render %s", cols[i].Name), parser.AsStringWithFlags(rexpr, parser.FmtShowTypes))
	}
}

func (*groupNode) SetLimitHint(_ int64, _ bool) {}

// wrap the supplied planNode with the groupNode if grouping/aggregation is required.
func (n *groupNode) wrap(plan planNode) planNode {
	if n == nil {
		return plan
	}
	n.plan = plan
	return n
}

// isNotNullFilter adds as a "col IS NOT NULL" constraint to the expression if
// the groupNode has a desired ordering on col (see
// desiredAggregateOrdering). A desired ordering will only be present if there
// is a single MIN/MAX aggregation function.
func (n *groupNode) isNotNullFilter(expr parser.TypedExpr) parser.TypedExpr {
	if len(n.desiredOrdering) != 1 {
		return expr
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
// scan. It looks for an output column index containing a simple MIN/MAX
// aggregation. If zero or multiple MIN/MAX aggregations are requested then no
// ordering will be requested. A negative index indicates a MAX aggregation was
// requested for the output column.
func desiredAggregateOrdering(funcs []*aggregateFuncHolder) sqlbase.ColumnOrdering {
	limit := -1
	direction := encoding.Ascending
	for i, f := range funcs {
		impl := f.create()
		switch impl.(type) {
		case *parser.MaxAggregate, *parser.MinAggregate:
			if limit != -1 || f.arg == nil {
				return nil
			}
			switch f.arg.(type) {
			case *qvalue:
				limit = i
				if _, ok := impl.(*parser.MaxAggregate); ok {
					direction = encoding.Descending
				}
			default:
				return nil
			}

		default:
			return nil
		}
	}
	if limit == -1 {
		return nil
	}
	return sqlbase.ColumnOrdering{{ColIdx: limit, Direction: direction}}
}

type extractAggregatesVisitor struct {
	n         *groupNode
	groupStrs map[string]struct{}

	// groupedCopy is nil when visitor is in an Expr subtree that appears in the GROUP BY clause.
	groupedCopy         *extractAggregatesVisitor
	subAggregateVisitor parser.IsAggregateVisitor
	err                 error
}

var _ parser.Visitor = &extractAggregatesVisitor{}

func (v *extractAggregatesVisitor) VisitPre(expr parser.Expr) (recurse bool, newExpr parser.Expr) {
	if v.err != nil {
		return false, expr
	}

	// This expression is in the GROUP BY - switch to the visitor that will accept
	// qvalues for this and any subtrees.
	if _, ok := v.groupStrs[expr.String()]; ok && v.groupedCopy != nil && v != v.groupedCopy {
		expr, _ = parser.WalkExpr(v.groupedCopy, expr)
		return false, expr
	}

	switch t := expr.(type) {
	case *parser.FuncExpr:
		if len(t.Name.Indirect) > 0 {
			break
		}
		if impl, ok := parser.Aggregates[strings.ToLower(string(t.Name.Base))]; ok {
			if len(t.Exprs) != 1 {
				// Type checking has already run on these expressions thus
				// if an aggregate function of the wrong arity gets here,
				// something has gone really wrong.
				panic(fmt.Sprintf("%s has %d arguments (expected 1)", t.Name.Base, len(t.Exprs)))
			}

			defer v.subAggregateVisitor.Reset()
			parser.WalkExprConst(&v.subAggregateVisitor, t.Exprs[0])
			if v.subAggregateVisitor.Aggregated {
				v.err = fmt.Errorf("aggregate function calls cannot be nested under %s", t.Name)
				return false, expr
			}

			f := &aggregateFuncHolder{
				expr:    t,
				arg:     t.Exprs[0].(parser.TypedExpr),
				create:  impl[0].AggregateFunc,
				group:   v.n,
				buckets: make(map[string]parser.AggregateFunc),
			}
			if t.Type == parser.Distinct {
				f.seen = make(map[string]struct{})
			}
			v.n.funcs = append(v.n.funcs, f)
			return false, f
		}
	case *qvalue:
		if v.groupedCopy != nil {
			v.err = fmt.Errorf("column \"%s\" must appear in the GROUP BY clause or be used in an aggregate function",
				t.colRef.get().Name)
			return true, expr
		}
		f := &aggregateFuncHolder{
			expr:    t,
			arg:     t,
			create:  parser.NewIdentAggregate,
			group:   v.n,
			buckets: make(map[string]parser.AggregateFunc),
		}
		v.n.funcs = append(v.n.funcs, f)
		return false, f
	}
	return true, expr
}

func (*extractAggregatesVisitor) VisitPost(expr parser.Expr) parser.Expr { return expr }

// Extract aggregateFuncHolders from exprs that use aggregation and check if they are valid.
// An expression is valid if:
// - it is an aggregate expression, or
// - it appears verbatim in groupBy, or
// - it is not a qvalue, and all of its subexpressions (as defined by
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
func (v extractAggregatesVisitor) extract(typedExpr parser.TypedExpr) (parser.TypedExpr, error) {
	expr, _ := parser.WalkExpr(&v, typedExpr)
	if v.err != nil {
		return nil, v.err
	}
	return expr.(parser.TypedExpr), nil
}

var _ parser.TypedExpr = &aggregateFuncHolder{}
var _ parser.VariableExpr = &aggregateFuncHolder{}

type aggregateFuncHolder struct {
	expr    parser.TypedExpr
	arg     parser.TypedExpr
	create  func() parser.AggregateFunc
	group   *groupNode
	buckets map[string]parser.AggregateFunc
	seen    map[string]struct{}
}

func (a *aggregateFuncHolder) add(bucket []byte, d parser.Datum) error {
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
		a.seen[string(encoded)] = struct{}{}
	}

	impl, ok := a.buckets[string(bucket)]
	if !ok {
		impl = a.create()
		a.buckets[string(bucket)] = impl
	}

	return impl.Add(d)
}

func (*aggregateFuncHolder) Variable() {}

func (a *aggregateFuncHolder) Format(buf *bytes.Buffer, f parser.FmtFlags) {
	a.expr.Format(buf, f)
}
func (a *aggregateFuncHolder) String() string { return parser.AsString(a) }

func (a *aggregateFuncHolder) Walk(v parser.Visitor) parser.Expr { return a }

func (a *aggregateFuncHolder) TypeCheck(_ *parser.SemaContext, desired parser.Datum) (parser.TypedExpr, error) {
	return a, nil
}

func (a *aggregateFuncHolder) Eval(ctx *parser.EvalContext) (parser.Datum, error) {
	// During init of the group buckets, grouped expressions (i.e. wrapped
	// qvalues) are Eval()'ed to determine the bucket for a row, so pass these
	// calls through to the underlying `arg` expr Eval until init is done.
	if !a.group.populated {
		return a.arg.Eval(ctx)
	}

	found, ok := a.buckets[a.group.currentBucket]
	if !ok {
		found = a.create()
	}

	datum, err := found.Result()
	if err != nil {
		return nil, err
	}

	// This is almost certainly the identity. Oh well.
	return datum.Eval(ctx)
}

func (a *aggregateFuncHolder) ReturnType() parser.Datum {
	return a.expr.ReturnType()
}
