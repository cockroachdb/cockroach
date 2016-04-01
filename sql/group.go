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
	"math"
	"strings"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/decimal"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
)

var aggregates = map[string]func() aggregateImpl{
	"avg":      newAvgAggregate,
	"count":    newCountAggregate,
	"max":      newMaxAggregate,
	"min":      newMinAggregate,
	"sum":      newSumAggregate,
	"stddev":   newStddevAggregate,
	"variance": newVarianceAggregate,
}

// groupBy constructs a groupNode according to grouping functions or clauses. This may adjust the
// render targets in the selectNode as necessary.
func (p *planner) groupBy(n *parser.SelectClause, s *selectNode) (*groupNode, *roachpb.Error) {
	// Determine if aggregation is being performed. This check is done on the raw
	// Select expressions as simplification might have removed aggregation
	// functions (e.g. `SELECT MIN(1)` -> `SELECT 1`).
	if isAggregate := p.isAggregate(n); !isAggregate {
		return nil, nil
	}

	groupBy := append([]parser.Expr(nil), n.GroupBy...)

	// Start by normalizing the GROUP BY expressions (to match what has been done to
	// the SELECT expressions in addRender) so that we can compare to them later.
	// This is done before determining if aggregation is being performed, because
	// that determination is made during validation, which will require matching
	// expressions.
	for i := range groupBy {
		resolved, err := s.resolveQNames(groupBy[i])
		if err != nil {
			return nil, roachpb.NewError(err)
		}

		// We could potentially skip this, since it will be checked in addRender,
		// but checking now allows early err return.
		if _, err := resolved.TypeCheck(p.evalCtx.Args); err != nil {
			return nil, roachpb.NewError(err)
		}

		norm, err := p.parser.NormalizeExpr(p.evalCtx, resolved)
		if err != nil {
			return nil, roachpb.NewError(err)
		}

		// If a col index is specified, replace it with that expression first.
		// NB: This is not a deep copy, and thus when extractAggregateFuncs runs
		// on s.render, the GroupBy expressions can contain wrapped qvalues.
		// aggregateFunc's Eval() method handles being called during grouping.
		if col, err := colIndex(s.numOriginalCols, norm); err != nil {
			return nil, roachpb.NewError(err)
		} else if col >= 0 {
			groupBy[i] = s.render[col]
		} else {
			groupBy[i] = norm
		}
	}

	// Normalize and check the HAVING expression too if it exists.
	if n.Having != nil {
		having, err := s.resolveQNames(n.Having.Expr)
		if err != nil {
			return nil, roachpb.NewError(err)
		}

		having, err = p.parser.NormalizeExpr(p.evalCtx, having)
		if err != nil {
			return nil, roachpb.NewError(err)
		}

		havingType, err := having.TypeCheck(p.evalCtx.Args)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		if !(havingType.TypeEqual(parser.DummyBool) || havingType == parser.DNull) {
			return nil, roachpb.NewUErrorf("argument of HAVING must be type %s, not type %s", parser.DummyBool.Type(), havingType.Type())
		}
		n.Having.Expr = having
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
	// After extraction, group.render will be entirely rendered from aggregateFuncs,
	// and group.funcs will contain all the functions which need to be fed values.
	for i := range group.render {
		expr, err := visitor.extract(group.render[i])
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		group.render[i] = expr
	}

	if n.Having != nil {
		having, err := visitor.extract(n.Having.Expr)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		group.having = having
	}

	// Queries like `SELECT MAX(n) FROM t` expect a row of NULLs if nothing was aggregated.
	group.addNullBucketIfEmpty = len(groupBy) == 0

	group.buckets = make(map[string]struct{})

	if log.V(2) {
		strs := make([]string, 0, len(group.funcs))
		for _, f := range group.funcs {
			strs = append(strs, f.String())
		}
		log.Infof("Group: %s", strings.Join(strs, ", "))
	}

	// Replace the render expressions in the scanNode with expressions that
	// compute only the arguments to the aggregate expressions.
	s.render = make([]parser.Expr, len(group.funcs))
	for i, f := range group.funcs {
		s.render[i] = f.arg
	}

	// Add the group-by expressions so they are available for bucketing.
	for _, g := range groupBy {
		if err := s.addRender(parser.SelectExpr{Expr: g}); err != nil {
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

	render []parser.Expr
	having parser.Expr

	funcs []*aggregateFunc
	// The set of bucket keys.
	buckets map[string]struct{}

	addNullBucketIfEmpty bool

	values    valuesNode
	populated bool

	// During rendering, aggregateFuncs compute their result for group.currentBucket.
	currentBucket string

	desiredOrdering columnOrdering
	pErr            *roachpb.Error

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

func (n *groupNode) Next() bool {
	var scratch []byte

	if n.pErr != nil {
		return false
	}

	for !n.populated {
		if !n.plan.Next() {
			n.pErr = n.plan.PErr()
			if n.pErr != nil {
				return false
			}
			n.computeAggregates()
			n.populated = true
			break
		}
		if n.explain == explainDebug && n.plan.DebugValues().output != debugValueRow {
			// Pass through non-row debug values.
			return true
		}

		// Add row to bucket.

		values := n.plan.Values()
		aggregatedValues, groupedValues := values[:len(n.funcs)], values[len(n.funcs):]

		// TODO(dt): optimization: skip buckets when underlying plan is ordered by grouped values.

		encoded, err := encodeDTuple(scratch, groupedValues)
		if err != nil {
			n.pErr = roachpb.NewError(err)
			return false
		}

		n.buckets[string(encoded)] = struct{}{}

		// Feed the aggregateFuncs for this bucket the non-grouped values.
		for i, value := range aggregatedValues {
			if err := n.funcs[i].add(encoded, value); err != nil {
				n.pErr = roachpb.NewError(err)
				return false
			}
		}
		scratch = encoded[:0]

		if n.explain == explainDebug {
			// Emit a "buffered" row.
			return true
		}
	}

	return n.values.Next()
}

func (n *groupNode) computeAggregates() {
	if len(n.buckets) < 1 && n.addNullBucketIfEmpty {
		n.buckets[""] = struct{}{}
	}

	// Since this controls Eval behavior of aggregateFunc, it is not set until init is complete.
	n.populated = true

	// Render the results.
	n.values.rows = make([]parser.DTuple, 0, len(n.buckets))
	for k := range n.buckets {
		n.currentBucket = k

		if n.having != nil {
			res, err := n.having.Eval(n.planner.evalCtx)
			if err != nil {
				n.pErr = roachpb.NewError(err)
				return
			}
			if res, err := parser.GetBool(res); err != nil {
				n.pErr = roachpb.NewError(err)
				return
			} else if !res {
				continue
			}
		}

		row := make(parser.DTuple, 0, len(n.render))
		for _, r := range n.render {
			res, err := r.Eval(n.planner.evalCtx)
			if err != nil {
				n.pErr = roachpb.NewError(err)
				return
			}
			row = append(row, res)
		}

		n.values.rows = append(n.values.rows, row)
	}

}

func (n *groupNode) PErr() *roachpb.Error {
	return n.pErr
}

func (n *groupNode) ExplainPlan() (name, description string, children []planNode) {
	name = "group"
	strs := make([]string, 0, len(n.funcs))
	for _, f := range n.funcs {
		strs = append(strs, f.String())
	}
	description = strings.Join(strs, ", ")
	return name, description, []planNode{n.plan}
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
func (n *groupNode) isNotNullFilter(expr parser.Expr) parser.Expr {
	if len(n.desiredOrdering) != 1 {
		return expr
	}
	i := n.desiredOrdering[0].colIdx
	f := n.funcs[i]
	isNotNull := &parser.ComparisonExpr{
		Operator: parser.IsNot,
		Left:     f.arg,
		Right:    parser.DNull,
	}
	if expr == nil {
		return isNotNull
	}
	return &parser.AndExpr{
		Left:  expr,
		Right: isNotNull,
	}
}

// desiredAggregateOrdering computes the desired output ordering from the
// scan. It looks for an output column index containing a simple MIN/MAX
// aggregation. If zero or multiple MIN/MAX aggregations are requested then no
// ordering will be requested. A negative index indicates a MAX aggregation was
// requested for the output column.
func desiredAggregateOrdering(funcs []*aggregateFunc) columnOrdering {
	limit := -1
	direction := encoding.Ascending
	for i, f := range funcs {
		impl := f.create()
		switch impl.(type) {
		case *maxAggregate, *minAggregate:
			if limit != -1 || f.arg == nil {
				return nil
			}
			switch f.arg.(type) {
			case *qvalue:
				limit = i
				if _, ok := impl.(*maxAggregate); ok {
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
	return columnOrdering{{limit, direction}}
}

type extractAggregatesVisitor struct {
	n         *groupNode
	groupStrs map[string]struct{}

	// groupedCopy is nil when visitor is in an Expr subtree that appears in the GROUP BY clause.
	groupedCopy         *extractAggregatesVisitor
	subAggregateVisitor isAggregateVisitor
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
		if impl, ok := aggregates[strings.ToLower(string(t.Name.Base))]; ok {
			if len(t.Exprs) != 1 {
				// Type checking has already run on these expressions thus
				// if an aggregate function of the wrong arity gets here,
				// something has gone really wrong.
				panic(fmt.Sprintf("%s has %d arguments (expected 1)", t.Name.Base, len(t.Exprs)))
			}

			defer v.subAggregateVisitor.reset()
			parser.WalkExprConst(&v.subAggregateVisitor, t.Exprs[0])
			if v.subAggregateVisitor.aggregated {
				v.err = fmt.Errorf("aggregate function calls cannot be nested under %s", t.Name)
				return false, expr
			}

			f := &aggregateFunc{
				expr:    t,
				arg:     t.Exprs[0],
				create:  impl,
				group:   v.n,
				buckets: make(map[string]aggregateImpl),
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
		f := &aggregateFunc{
			expr:    t,
			arg:     t,
			create:  newIdentAggregate,
			group:   v.n,
			buckets: make(map[string]aggregateImpl),
		}
		v.n.funcs = append(v.n.funcs, f)
		return false, f
	}
	return true, expr
}

func (*extractAggregatesVisitor) VisitPost(expr parser.Expr) parser.Expr { return expr }

// Extract aggregateFuncs from exprs that use aggregation and check if they are valid.
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
func (v extractAggregatesVisitor) extract(expr parser.Expr) (parser.Expr, error) {
	expr, _ = parser.WalkExpr(&v, expr)
	return expr, v.err
}

var _ parser.Visitor = &isAggregateVisitor{}

type isAggregateVisitor struct {
	aggregated bool
}

func (v *isAggregateVisitor) VisitPre(expr parser.Expr) (recurse bool, newExpr parser.Expr) {
	switch t := expr.(type) {
	case *parser.FuncExpr:
		if _, ok := aggregates[strings.ToLower(string(t.Name.Base))]; ok {
			v.aggregated = true
			return false, expr
		}
	case *parser.Subquery:
		return false, expr
	}

	return true, expr
}

func (*isAggregateVisitor) VisitPost(expr parser.Expr) parser.Expr { return expr }

func (v *isAggregateVisitor) reset() {
	v.aggregated = false
}

func (p *planner) aggregateInExpr(expr parser.Expr) bool {
	if expr != nil {
		defer p.isAggregateVisitor.reset()
		parser.WalkExprConst(&p.isAggregateVisitor, expr)
		if p.isAggregateVisitor.aggregated {
			return true
		}
	}
	return false
}

func (p *planner) isAggregate(n *parser.SelectClause) bool {
	if n.Having != nil || len(n.GroupBy) > 0 {
		return true
	}

	defer p.isAggregateVisitor.reset()
	for _, target := range n.Exprs {
		parser.WalkExprConst(&p.isAggregateVisitor, target.Expr)
		if p.isAggregateVisitor.aggregated {
			return true
		}
	}
	return false
}

var _ parser.VariableExpr = &aggregateFunc{}

type aggregateFunc struct {
	expr    parser.Expr
	arg     parser.Expr
	create  func() aggregateImpl
	group   *groupNode
	buckets map[string]aggregateImpl
	seen    map[string]struct{}
}

func (a *aggregateFunc) add(bucket []byte, d parser.Datum) error {
	// NB: the compiler *should* optimize `myMap[string(myBytes)]`. See:
	// https://github.com/golang/go/commit/f5f5a8b6209f84961687d993b93ea0d397f5d5bf

	if a.seen != nil {
		encoded, err := encodeDatum(bucket, d)
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

	return impl.add(d)
}

func (*aggregateFunc) Variable() {}

func (a *aggregateFunc) String() string {
	return a.expr.String()
}

func (a *aggregateFunc) Walk(v parser.Visitor) parser.Expr { return a }

func (a *aggregateFunc) TypeCheck(args parser.MapArgs) (parser.Datum, error) {
	return a.expr.TypeCheck(args)
}

func (a *aggregateFunc) Eval(ctx parser.EvalContext) (parser.Datum, error) {
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

	datum, err := found.result()
	if err != nil {
		return nil, err
	}

	// This is almost certainly the identity. Oh well.
	return datum.Eval(ctx)
}

type aggregateImpl interface {
	add(parser.Datum) error
	result() (parser.Datum, error)
}

var _ aggregateImpl = &avgAggregate{}
var _ aggregateImpl = &countAggregate{}
var _ aggregateImpl = &maxAggregate{}
var _ aggregateImpl = &minAggregate{}
var _ aggregateImpl = &sumAggregate{}
var _ aggregateImpl = &stddevAggregate{}
var _ aggregateImpl = &varianceAggregate{}
var _ aggregateImpl = &floatVarianceAggregate{}
var _ aggregateImpl = &decimalVarianceAggregate{}
var _ aggregateImpl = &identAggregate{}

// In order to render the unaggregated (i.e. grouped) fields, during aggregation,
// the values for those fields have to be stored for each bucket.
// The `identAggregate` provides an "aggregate" function that actually
// just returns the last value passed to `add`, unchanged. For accumulating
// and rendering though it behaves like the other aggregate functions,
// allowing both those steps to avoid special-casing grouped vs aggregated fields.
type identAggregate struct {
	val parser.Datum
}

func newIdentAggregate() aggregateImpl {
	return &identAggregate{}
}

func (a *identAggregate) add(datum parser.Datum) error {
	a.val = datum
	return nil
}

func (a *identAggregate) result() (parser.Datum, error) {
	return a.val, nil
}

type avgAggregate struct {
	sumAggregate
	count int
}

func newAvgAggregate() aggregateImpl {
	return &avgAggregate{}
}

func (a *avgAggregate) add(datum parser.Datum) error {
	if datum == parser.DNull {
		return nil
	}
	if err := a.sumAggregate.add(datum); err != nil {
		return err
	}
	a.count++
	return nil
}

func (a *avgAggregate) result() (parser.Datum, error) {
	sum, err := a.sumAggregate.result()
	if err != nil {
		return parser.DNull, err
	}
	if sum == parser.DNull {
		return sum, nil
	}
	switch t := sum.(type) {
	case parser.DInt:
		// TODO(nvanbenschoten) decimal: this should be a numeric, once
		// better type coercion semantics are defined.
		return parser.DFloat(t) / parser.DFloat(a.count), nil
	case parser.DFloat:
		return t / parser.DFloat(a.count), nil
	case *parser.DDecimal:
		count := inf.NewDec(int64(a.count), 0)
		t.QuoRound(&t.Dec, count, decimal.Precision, inf.RoundHalfUp)
		return t, nil
	default:
		return parser.DNull, util.Errorf("unexpected SUM result type: %s", t.Type())
	}
}

type countAggregate struct {
	count int
}

func newCountAggregate() aggregateImpl {
	return &countAggregate{}
}

func (a *countAggregate) add(datum parser.Datum) error {
	if datum == parser.DNull {
		return nil
	}
	switch t := datum.(type) {
	case parser.DTuple:
		for _, d := range t {
			if d != parser.DNull {
				a.count++
				break
			}
		}
	default:
		a.count++
	}
	return nil
}

func (a *countAggregate) result() (parser.Datum, error) {
	return parser.DInt(a.count), nil
}

type maxAggregate struct {
	max parser.Datum
}

func newMaxAggregate() aggregateImpl {
	return &maxAggregate{}
}

func (a *maxAggregate) add(datum parser.Datum) error {
	if datum == parser.DNull {
		return nil
	}
	if a.max == nil {
		a.max = datum
		return nil
	}
	c := a.max.Compare(datum)
	if c < 0 {
		a.max = datum
	}
	return nil
}

func (a *maxAggregate) result() (parser.Datum, error) {
	if a.max == nil {
		return parser.DNull, nil
	}
	return a.max, nil
}

type minAggregate struct {
	min parser.Datum
}

func newMinAggregate() aggregateImpl {
	return &minAggregate{}
}

func (a *minAggregate) add(datum parser.Datum) error {
	if datum == parser.DNull {
		return nil
	}
	if a.min == nil {
		a.min = datum
		return nil
	}
	c := a.min.Compare(datum)
	if c > 0 {
		a.min = datum
	}
	return nil
}

func (a *minAggregate) result() (parser.Datum, error) {
	if a.min == nil {
		return parser.DNull, nil
	}
	return a.min, nil
}

type sumAggregate struct {
	sum parser.Datum
}

func newSumAggregate() aggregateImpl {
	return &sumAggregate{}
}

func (a *sumAggregate) add(datum parser.Datum) error {
	if datum == parser.DNull {
		return nil
	}
	if a.sum == nil {
		switch t := datum.(type) {
		case *parser.DDecimal:
			// Make copy of decimal to allow for modification later.
			dd := &parser.DDecimal{}
			dd.Set(&t.Dec)
			datum = dd
		}
		a.sum = datum
		return nil
	}

	switch t := datum.(type) {
	case parser.DInt:
		if v, ok := a.sum.(parser.DInt); ok {
			a.sum = v + t
			return nil
		}

	case parser.DFloat:
		if v, ok := a.sum.(parser.DFloat); ok {
			a.sum = v + t
			return nil
		}

	case *parser.DDecimal:
		if v, ok := a.sum.(*parser.DDecimal); ok {
			v.Add(&v.Dec, &t.Dec)
			a.sum = v
			return nil
		}
	}

	return util.Errorf("unexpected SUM argument type: %s", datum.Type())
}

func (a *sumAggregate) result() (parser.Datum, error) {
	if a.sum == nil {
		return parser.DNull, nil
	}
	switch t := a.sum.(type) {
	case *parser.DDecimal:
		dd := &parser.DDecimal{}
		dd.Set(&t.Dec)
		return dd, nil
	default:
		return a.sum, nil
	}
}

type varianceAggregate struct {
	typedAggregate aggregateImpl
	// Used for passing int64s as *inf.Dec values.
	tmpDec parser.DDecimal
}

func newVarianceAggregate() aggregateImpl {
	return &varianceAggregate{}
}

func (a *varianceAggregate) add(datum parser.Datum) error {
	if datum == parser.DNull {
		return nil
	}

	const unexpectedErrFormat = "unexpected VARIANCE argument type: %s"
	switch t := datum.(type) {
	case parser.DFloat:
		if a.typedAggregate == nil {
			a.typedAggregate = newFloatVarianceAggregate()
		} else {
			switch a.typedAggregate.(type) {
			case *floatVarianceAggregate:
			default:
				return util.Errorf(unexpectedErrFormat, datum.Type())
			}
		}
		return a.typedAggregate.add(t)
	case parser.DInt:
		if a.typedAggregate == nil {
			a.typedAggregate = newDecimalVarianceAggregate()
		} else {
			switch a.typedAggregate.(type) {
			case *decimalVarianceAggregate:
			default:
				return util.Errorf(unexpectedErrFormat, datum.Type())
			}
		}
		a.tmpDec.SetUnscaled(int64(t))
		return a.typedAggregate.add(&a.tmpDec)
	case *parser.DDecimal:
		if a.typedAggregate == nil {
			a.typedAggregate = newDecimalVarianceAggregate()
		} else {
			switch a.typedAggregate.(type) {
			case *decimalVarianceAggregate:
			default:
				return util.Errorf(unexpectedErrFormat, datum.Type())
			}
		}
		return a.typedAggregate.add(t)
	default:
		return util.Errorf(unexpectedErrFormat, datum.Type())
	}
}

func (a *varianceAggregate) result() (parser.Datum, error) {
	if a.typedAggregate == nil {
		return parser.DNull, nil
	}
	return a.typedAggregate.result()
}

type floatVarianceAggregate struct {
	count   int
	mean    float64
	sqrDiff float64
}

func newFloatVarianceAggregate() aggregateImpl {
	return &floatVarianceAggregate{}
}

func (a *floatVarianceAggregate) add(datum parser.Datum) error {
	f := float64(datum.(parser.DFloat))

	// Uses the Knuth/Welford method for accurately computing variance online in a
	// single pass. See http://www.johndcook.com/blog/standard_deviation/ and
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm.
	a.count++
	delta := f - a.mean
	a.mean += delta / float64(a.count)
	a.sqrDiff += delta * (f - a.mean)
	return nil
}

func (a *floatVarianceAggregate) result() (parser.Datum, error) {
	if a.count < 2 {
		return parser.DNull, nil
	}
	return parser.DFloat(a.sqrDiff / (float64(a.count) - 1)), nil
}

type decimalVarianceAggregate struct {
	// Variables used across iterations.
	count   inf.Dec
	mean    inf.Dec
	sqrDiff inf.Dec

	// Variables used as scratch space within iterations.
	delta inf.Dec
	tmp   inf.Dec
}

func newDecimalVarianceAggregate() aggregateImpl {
	return &decimalVarianceAggregate{}
}

// Read-only constants used for compuation.
var (
	decimalOne = inf.NewDec(1, 0)
	decimalTwo = inf.NewDec(2, 0)
)

func (a *decimalVarianceAggregate) add(datum parser.Datum) error {
	d := datum.(*parser.DDecimal).Dec

	// Uses the Knuth/Welford method for accurately computing variance online in a
	// single pass. See http://www.johndcook.com/blog/standard_deviation/ and
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm.
	a.count.Add(&a.count, decimalOne)
	a.delta.Sub(&d, &a.mean)
	a.tmp.QuoRound(&a.delta, &a.count, decimal.Precision, inf.RoundHalfUp)
	a.mean.Add(&a.mean, &a.tmp)
	a.tmp.Sub(&d, &a.mean)
	a.sqrDiff.Add(&a.sqrDiff, a.delta.Mul(&a.delta, &a.tmp))
	return nil
}

func (a *decimalVarianceAggregate) result() (parser.Datum, error) {
	if a.count.Cmp(decimalTwo) < 0 {
		return parser.DNull, nil
	}
	a.tmp.Sub(&a.count, decimalOne)
	dd := &parser.DDecimal{}
	dd.QuoRound(&a.sqrDiff, &a.tmp, decimal.Precision, inf.RoundHalfUp)
	return dd, nil
}

type stddevAggregate struct {
	varianceAggregate
}

func newStddevAggregate() aggregateImpl {
	return &stddevAggregate{varianceAggregate: *newVarianceAggregate().(*varianceAggregate)}
}

func (a *stddevAggregate) result() (parser.Datum, error) {
	variance, err := a.varianceAggregate.result()
	if err != nil || variance == parser.DNull {
		return variance, err
	}
	switch t := variance.(type) {
	case parser.DFloat:
		return parser.DFloat(math.Sqrt(float64(t))), nil
	case *parser.DDecimal:
		decimal.Sqrt(&t.Dec, &t.Dec, decimal.Precision)
		return t, nil
	}
	return nil, util.Errorf("unexpected variance result type: %s", variance.Type())
}
