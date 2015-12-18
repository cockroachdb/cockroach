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
	"strings"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/log"
)

var aggregates = map[string]func() aggregateImpl{
	"avg":   newAvgAggregate,
	"count": newCountAggregate,
	"max":   newMaxAggregate,
	"min":   newMinAggregate,
	"sum":   newSumAggregate,
}

func (p *planner) groupBy(n *parser.Select, s *scanNode) (*groupNode, error) {
	// Start by normalizing the GROUP BY expressions (to match what has been done to
	// the SELECT expressions in addRenderer) so that we can compare them later.
	// This is done before determining if aggregation is being performed, because
	// that determination is made during validation, which will require matching
	// expressions.
	for i := range n.GroupBy {
		norm, err := s.resolveQNames(n.GroupBy[i])
		if err != nil {
			return nil, err
		}
		norm, err = p.parser.NormalizeExpr(p.evalCtx, norm)
		if err != nil {
			return nil, err
		}
		n.GroupBy[i] = norm
	}

	// Determine if aggregation is being performed and, if so, if it is valid.
	// Check each render expressions and verify that the only qvalues mentioned
	// in it are either aggregated or appear in GROUP BY expressions.
	if aggregation, err := checkAggregateExprs(n.GroupBy, s.render); !aggregation {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	// TODO(pmattis): This only handles aggregate functions, not GROUP BY.

	// Loop over the render expressions and extract any aggregate functions.
	var funcs []*aggregateFunc
	for i, r := range s.render {
		r, f, err := p.extractAggregateFuncs(r)
		if err != nil {
			return nil, err
		}
		s.render[i] = r
		funcs = append(funcs, f...)
	}
	if len(funcs) == 0 {
		return nil, nil
	}

	if log.V(2) {
		strs := make([]string, 0, len(funcs))
		for _, f := range funcs {
			strs = append(strs, f.val.String())
		}
		log.Infof("Group: %s", strings.Join(strs, ", "))
	}

	group := &groupNode{
		planner: p,
		columns: s.columns,
		render:  s.render,
		funcs:   funcs,
	}

	// Replace the render expressions in the scanNode with expressions that
	// compute only the arguments to the aggregate expressions.
	s.columns = make([]column, 0, len(funcs))
	s.render = make([]parser.Expr, 0, len(funcs))
	for _, f := range funcs {
		if len(f.val.expr.Exprs) != 1 {
			panic(fmt.Sprintf("%s has %d arguments (expected 1)", f.val.expr.Name, len(f.val.expr.Exprs)))
		}
		s.columns = append(s.columns, column{name: f.val.String(), typ: f.val.datum})
		s.render = append(s.render, f.val.expr.Exprs[0])
	}

	group.desiredOrdering = desiredAggregateOrdering(group.funcs)
	return group, nil
}

type groupNode struct {
	planner         *planner
	plan            planNode
	columns         []column
	row             parser.DTuple
	render          []parser.Expr
	funcs           []*aggregateFunc
	desiredOrdering []int
	needGroup       bool
	err             error
}

func (n *groupNode) Columns() []column {
	return n.columns
}

func (n *groupNode) Ordering() ([]int, int) {
	return n.plan.Ordering()
}

func (n *groupNode) Values() parser.DTuple {
	return n.row
}

func (n *groupNode) Next() bool {
	if !n.needGroup || n.err != nil {
		return false
	}
	n.needGroup = false

	// Loop over the rows passing the values into the corresponding aggregation
	// functions.
	for n.plan.Next() {
		values := n.plan.Values()
		for i, f := range n.funcs {
			if n.err = f.add(values[i]); n.err != nil {
				return false
			}
		}
	}

	n.err = n.plan.Err()
	if n.err != nil {
		return false
	}

	// Fill in the aggregate function result value.
	for _, f := range n.funcs {
		if f.val.datum, n.err = f.impl.result(); n.err != nil {
			return false
		}
	}

	// Render the results.
	n.row = make([]parser.Datum, len(n.render))
	for i, r := range n.render {
		n.row[i], n.err = r.Eval(n.planner.evalCtx)
		if n.err != nil {
			return false
		}
	}

	return n.err == nil
}

func (n *groupNode) Err() error {
	return n.err
}

func (n *groupNode) ExplainPlan() (name, description string, children []planNode) {
	name = "group"
	strs := make([]string, 0, len(n.funcs))
	for _, f := range n.funcs {
		strs = append(strs, f.val.String())
	}
	description = strings.Join(strs, ", ")
	return name, description, []planNode{n.plan}
}

// wrap the supplied planNode with the groupNode if grouping/aggregation is required.
func (n *groupNode) wrap(plan planNode) planNode {
	if n == nil {
		return plan
	}
	n.plan = plan
	n.needGroup = true
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
	i := n.desiredOrdering[0]
	if i < 0 {
		i = -i
	}
	f := n.funcs[i-1]
	isNotNull := &parser.ComparisonExpr{
		Operator: parser.IsNot,
		Left:     f.val.expr.Exprs[0],
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
func desiredAggregateOrdering(funcs []*aggregateFunc) []int {
	var limit int
	for i, f := range funcs {
		switch f.impl.(type) {
		case *maxAggregate, *minAggregate:
			if limit != 0 || len(f.val.expr.Exprs) != 1 {
				return nil
			}
			switch f.val.expr.Exprs[0].(type) {
			case *qvalue:
				limit = i + 1
				if _, ok := f.impl.(*maxAggregate); ok {
					limit = -limit
				}
			default:
				return nil
			}

		default:
			return nil
		}
	}
	if limit == 0 {
		return nil
	}
	return []int{limit}
}

type extractAggregatesVisitor struct {
	funcs []*aggregateFunc
	err   error
}

var _ parser.Visitor = &extractAggregatesVisitor{}

func (v *extractAggregatesVisitor) Visit(expr parser.Expr, pre bool) (parser.Visitor, parser.Expr) {
	if !pre || v.err != nil {
		return nil, expr
	}
	switch t := expr.(type) {
	case *parser.FuncExpr:
		if len(t.Name.Indirect) > 0 {
			break
		}
		if impl, ok := aggregates[strings.ToLower(string(t.Name.Base))]; ok {
			f := &aggregateFunc{
				val: aggregateValue{
					expr: t,
				},
				impl: impl(),
			}
			if t.Distinct {
				f.seen = make(map[string]struct{})
			}
			v.funcs = append(v.funcs, f)
			return nil, &f.val
		}
	}
	return v, expr
}

func (v *extractAggregatesVisitor) run(expr parser.Expr) (parser.Expr, []*aggregateFunc, error) {
	*v = extractAggregatesVisitor{}
	expr = parser.WalkExpr(v, expr)
	return expr, v.funcs, v.err
}

func (p *planner) extractAggregateFuncs(expr parser.Expr) (parser.Expr, []*aggregateFunc, error) {
	return p.extractAggregatesVisitor.run(expr)
}

type checkAggregateVisitor struct {
	groupStrs  map[string]struct{}
	aggregated bool
	err        error
}

var _ parser.Visitor = &checkAggregateVisitor{}

func (v *checkAggregateVisitor) Visit(expr parser.Expr, pre bool) (parser.Visitor, parser.Expr) {
	if !pre || v.err != nil {
		return nil, expr
	}

	if t, ok := expr.(*parser.FuncExpr); ok {
		if _, ok := aggregates[strings.ToLower(string(t.Name.Base))]; ok {
			v.aggregated = true
			return nil, expr
		}
	}

	if t, ok := expr.(*qvalue); ok {
		if _, ok := v.groupStrs[t.String()]; ok {
			return nil, expr
		}
		v.err = fmt.Errorf("column \"%s\" must appear in the GROUP BY clause or be used in an aggregate function", t.col.Name)
		return v, expr
	}

	if _, ok := v.groupStrs[expr.String()]; ok {
		return nil, expr
	}
	return v, expr
}

// Check if expressions use aggregation and, if so, if they are valid.
// "Valid" expressions must either contain no unaggregated qvalues
// or must appear, verbatim, in the group-by clause. Expressions are
// string-compared to the group-by clauses (as an approximation of) a
// recursive expression-tree equality check.
func checkAggregateExprs(group parser.GroupBy, exprs []parser.Expr) (bool, error) {
	aggregated := len(group) > 0

	//TODO(davidt): remove when group by is implemented
	if aggregated {
		return aggregated, fmt.Errorf("GROUP BY not supported yet")
	}

	v := checkAggregateVisitor{}

	v.groupStrs = make(map[string]struct{}, len(group))
	for i := range group {
		v.groupStrs[group[i].String()] = struct{}{}
	}

	for _, expr := range exprs {
		_ = parser.WalkExpr(&v, expr)
		if v.aggregated {
			aggregated = true
		}
		if v.err != nil {
			return aggregated, v.err
		}
	}
	return aggregated, nil
}

type aggregateValue struct {
	datum parser.Datum
	expr  *parser.FuncExpr
}

var _ parser.VariableExpr = &aggregateValue{}

func (*aggregateValue) Variable() {}

func (av *aggregateValue) String() string {
	return av.expr.String()
}

func (av *aggregateValue) Walk(v parser.Visitor) {
	// I expected to implement:
	// av.datum = parser.WalkExpr(v, av.datum).(parser.Datum)
	// But it seems `av.datum` is sometimes nil.
}

func (av *aggregateValue) TypeCheck(args parser.MapArgs) (parser.Datum, error) {
	return av.expr.TypeCheck(args)
}

func (av *aggregateValue) Eval(ctx parser.EvalContext) (parser.Datum, error) {
	return av.datum.Eval(ctx)
}

type aggregateFunc struct {
	val  aggregateValue
	impl aggregateImpl
	seen map[string]struct{}
}

func (a *aggregateFunc) add(d parser.Datum) error {
	if a.seen != nil {
		encoded, err := encodeDatum(nil, d)
		if err != nil {
			return err
		}
		e := string(encoded)
		if _, ok := a.seen[e]; ok {
			// skip
			return nil
		}
		a.seen[e] = struct{}{}
	}
	return a.impl.add(d)
}

func encodeDatum(b []byte, d parser.Datum) ([]byte, error) {
	if values, ok := d.(parser.DTuple); ok {
		return encodeDTuple(b, values)
	}
	return encodeTableKey(b, d)
}

func encodeDTuple(b []byte, d parser.DTuple) ([]byte, error) {
	for _, val := range d {
		var err error
		b, err = encodeDatum(b, val)
		if err != nil {
			return nil, err
		}
	}
	return b, nil
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
		return parser.DFloat(t) / parser.DFloat(a.count), nil
	case parser.DFloat:
		return t / parser.DFloat(a.count), nil
	default:
		return parser.DNull, fmt.Errorf("unexpected SUM result type: %s", t.Type())
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
	}

	return fmt.Errorf("unexpected SUM argument type: %s", datum.Type())
}

func (a *sumAggregate) result() (parser.Datum, error) {
	if a.sum == nil {
		return parser.DNull, nil
	}
	return a.sum, nil
}
