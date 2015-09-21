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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/log"
)

var aggregates = map[string]aggregateImpl{
	"avg":   &avgAggregate{},
	"count": &countAggregate{},
	"max":   &maxAggregate{},
	"min":   &minAggregate{},
	"sum":   &sumAggregate{},
}

func (p *planner) groupBy(n *parser.Select, s *scanNode) (*groupNode, error) {
	// TODO(pmattis): This only handles aggregate functions, not GROUP BY.

	// Loop over the render expressions and extract any aggregate functions.
	var funcs []*aggregateFunc
	for i, r := range s.render {
		r, f, err := extractAggregateFuncs(r)
		if err != nil {
			return nil, err
		}
		s.render[i] = r
		funcs = append(funcs, f...)
	}
	if len(funcs) == 0 {
		return nil, nil
	}

	// Aggregation is being performed. Loop over the render expressions again and
	// verify that the only qvalues mentioned by GROUP BY expressions are allowed
	// outside of the aggregate function arguments. For example, the following is
	// illegal because k is used outside of the aggregate function and not part
	// of a GROUP BY expression.
	//
	//   SELECT COUNT(k), k FROM kv GROUP BY v
	for _, r := range s.render {
		if err := checkAggregateExpr(r); err != nil {
			return nil, err
		}
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
	s.columns = make([]string, 0, len(funcs))
	s.render = make([]parser.Expr, 0, len(funcs))
	for _, f := range funcs {
		if len(f.val.Exprs) != 1 {
			panic(fmt.Sprintf("%s has %d arguments (expected 1)", f.val.Name, len(f.val.Exprs)))
		}
		s.columns = append(s.columns, f.val.String())
		s.render = append(s.render, f.val.Exprs[0])
	}

	return group, nil
}

type groupNode struct {
	planner   *planner
	plan      planNode
	columns   []string
	row       parser.DTuple
	render    []parser.Expr
	funcs     []*aggregateFunc
	needGroup bool
	err       error
}

func (n *groupNode) Columns() []string {
	return n.columns
}

func (n *groupNode) Ordering() []int {
	return n.plan.Ordering()
}

func (n *groupNode) Values() parser.DTuple {
	return n.row
}

func (n *groupNode) Next() bool {
	if !n.needGroup {
		return false
	}
	n.needGroup = false

	// Loop over the rows passing the values into the corresponding aggregation
	// functions.
	for n.plan.Next() {
		values := n.plan.Values()
		for i, f := range n.funcs {
			if n.err = f.impl.Add(values[i]); n.err != nil {
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
		if f.val.datum, n.err = f.impl.Result(); n.err != nil {
			return false
		}
	}

	// Render the results.
	n.row = make([]parser.Datum, len(n.render))
	for i, r := range n.render {
		n.row[i], n.err = n.planner.evalCtx.EvalExpr(r)
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
					FuncExpr: t,
				},
				impl: impl.New(),
			}
			v.funcs = append(v.funcs, f)
			return nil, &f.val
		}
	}
	return v, expr
}

func extractAggregateFuncs(expr parser.Expr) (parser.Expr, []*aggregateFunc, error) {
	v := extractAggregatesVisitor{}
	expr = parser.WalkExpr(&v, expr)
	return expr, v.funcs, v.err
}

type checkAggregateVisitor struct {
	err error
}

var _ parser.Visitor = &checkAggregateVisitor{}

func (v *checkAggregateVisitor) Visit(expr parser.Expr, pre bool) (parser.Visitor, parser.Expr) {
	if !pre || v.err != nil {
		return nil, expr
	}
	switch t := expr.(type) {
	case *qvalue:
		// TODO(pmattis): Check that the qvalue is part of a GROUP BY expression.
		v.err = fmt.Errorf("column \"%s\" must appear in the GROUP BY clause or be used in an aggregate function", t.col.Name)
		return nil, expr
	}
	return v, expr
}

func checkAggregateExpr(expr parser.Expr) error {
	v := checkAggregateVisitor{}
	_ = parser.WalkExpr(&v, expr)
	return v.err
}

type aggregateValue struct {
	datum parser.Datum
	// Tricky: we embed a parser.FuncExpr so that aggregateValue implements
	// parser.expr()! Note that we can't just implement aggregateValue.expr() as
	// that interface method is defined in the wrong package.
	*parser.FuncExpr
}

var _ parser.DReference = &aggregateValue{}

func (v *aggregateValue) Datum() parser.Datum {
	return v.datum
}

type aggregateFunc struct {
	val  aggregateValue
	impl aggregateImpl
}

type aggregateImpl interface {
	New() aggregateImpl
	Add(parser.Datum) error
	Result() (parser.Datum, error)
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

func (a *avgAggregate) New() aggregateImpl {
	return &avgAggregate{}
}

func (a *avgAggregate) Add(datum parser.Datum) error {
	if datum == parser.DNull {
		return nil
	}
	if err := a.sumAggregate.Add(datum); err != nil {
		return err
	}
	a.count++
	return nil
}

func (a *avgAggregate) Result() (parser.Datum, error) {
	sum, err := a.sumAggregate.Result()
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

func (a *countAggregate) New() aggregateImpl {
	return &countAggregate{}
}

func (a *countAggregate) Add(datum parser.Datum) error {
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

func (a *countAggregate) Result() (parser.Datum, error) {
	return parser.DInt(a.count), nil
}

type maxAggregate struct {
	max parser.Datum
}

func (a *maxAggregate) New() aggregateImpl {
	return &maxAggregate{}
}

func (a *maxAggregate) Add(datum parser.Datum) error {
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

func (a *maxAggregate) Result() (parser.Datum, error) {
	if a.max == nil {
		return parser.DNull, nil
	}
	return a.max, nil
}

type minAggregate struct {
	min parser.Datum
}

func (a *minAggregate) New() aggregateImpl {
	return &minAggregate{}
}

func (a *minAggregate) Add(datum parser.Datum) error {
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

func (a *minAggregate) Result() (parser.Datum, error) {
	if a.min == nil {
		return parser.DNull, nil
	}
	return a.min, nil
}

type sumAggregate struct {
	sum parser.Datum
}

func (a *sumAggregate) New() aggregateImpl {
	return &sumAggregate{}
}

func (a *sumAggregate) Add(datum parser.Datum) error {
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

func (a *sumAggregate) Result() (parser.Datum, error) {
	if a.sum == nil {
		return parser.DNull, nil
	}
	return a.sum, nil
}
