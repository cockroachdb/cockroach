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

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// subquery represents a subquery expression in an expression tree
// after it has been converted to a query plan. It is carried
// in the expression tree from the point type checking occurs to
// the point the query starts execution / evaluation.
type subquery struct {
	planner  *planner
	typ      parser.Type
	subquery *parser.Subquery
	execMode subqueryExecMode
	expanded bool
	started  bool
	plan     planNode
	result   parser.Datum
}

type subqueryExecMode int

const (
	// Sub-query is argument to EXISTS. Only 0 or 1 row is expected.
	// Result type is Bool.
	execModeExists subqueryExecMode = iota
	// Sub-query is argument to IN. Any number of rows expected. Result
	// type is tuple of rows. As a special case, if there is only one
	// column selected, the result is a tuple of the selected values
	// (instead of a tuple of 1-tuples).
	execModeAllRowsNormalized
	// Sub-query is argument to an ARRAY constructor. Any number of rows
	// expected, and exactly one column is expected. Result type is tuple
	// of selected values.
	execModeAllRows
	// Sub-query is argument to another function. Exactly 1 row
	// expected. Result type is tuple of columns, unless there is
	// exactly 1 column in which case the result type is that column's
	// type.
	execModeOneRow
)

var _ parser.TypedExpr = &subquery{}
var _ parser.VariableExpr = &subquery{}

func (s *subquery) Format(buf *bytes.Buffer, f parser.FmtFlags) {
	if s.execMode == execModeExists {
		buf.WriteString("EXISTS ")
	}
	if f == parser.FmtShowTypes {
		// TODO(knz/nvanbenschoten) It is not possible to extract types
		// from the subquery using Format, because type checking does not
		// replace the sub-expressions of a SelectClause node in-place.
		f = parser.FmtSimple
	}
	s.subquery.Format(buf, f)
}

func (s *subquery) String() string { return parser.AsString(s) }

func (s *subquery) Walk(v parser.Visitor) parser.Expr {
	return s
}

func (s *subquery) Variable() {}

func (s *subquery) TypeCheck(_ *parser.SemaContext, desired parser.Type) (parser.TypedExpr, error) {
	// TODO(knz): if/when type checking can be extracted from the
	// newPlan recursion, we can propagate the desired type to the
	// sub-query. For now, the type is simply derived during the subquery node
	// creation by looking at the result column types.

	// TODO(nvanbenschoten) Type checking for the comparison operator(s)
	// should take this new node into account. In particular it should
	// check that the tuple types match pairwise.
	return s, nil
}

func (s *subquery) ResolvedType() parser.Type { return s.typ }

func (s *subquery) Eval(_ *parser.EvalContext) (parser.Datum, error) {
	if s.result == nil {
		panic("subquery was not pre-evaluated properly")
	}
	return s.result, nil
}

func (s *subquery) doEval(ctx context.Context) (result parser.Datum, err error) {
	// After evaluation, there is no plan remaining.
	defer func() { s.plan.Close(ctx); s.plan = nil }()

	switch s.execMode {
	case execModeExists:
		// For EXISTS expressions, all we want to know is if there is at least one
		// result.
		next, err := s.plan.Next(ctx)
		if err != nil {
			return result, err
		}
		if next {
			result = parser.MakeDBool(true)
		}
		if result == nil {
			result = parser.MakeDBool(false)
		}

	case execModeAllRows, execModeAllRowsNormalized:
		var rows parser.DTuple
		next, err := s.plan.Next(ctx)
		for ; next; next, err = s.plan.Next(ctx) {
			values := s.plan.Values()
			switch len(values) {
			case 1:
				// This seems hokey, but if we don't do this then the subquery expands
				// to a tuple of tuples instead of a tuple of values and an expression
				// like "k IN (SELECT foo FROM bar)" will fail because we're comparing
				// a single value against a tuple.
				rows.D = append(rows.D, values[0])
			default:
				// The result from plan.Values() is only valid until the next call to
				// plan.Next(), so make a copy.
				valuesCopy := parser.NewDTupleWithLen(len(values))
				copy(valuesCopy.D, values)
				rows.D = append(rows.D, valuesCopy)
			}
		}
		if err != nil {
			return result, err
		}

		if ok, dir := s.subqueryTupleOrdering(); ok {
			if dir == encoding.Descending {
				rows.D.Reverse()
			}
			rows.SetSorted()
		}
		if s.execMode == execModeAllRowsNormalized {
			rows.Normalize(&s.planner.evalCtx)
		}
		result = &rows

	case execModeOneRow:
		result = parser.DNull
		hasRow, err := s.plan.Next(ctx)
		if err != nil {
			return result, err
		}
		if hasRow {
			values := s.plan.Values()
			switch len(values) {
			case 1:
				result = values[0]
			default:
				valuesCopy := parser.NewDTupleWithLen(len(values))
				copy(valuesCopy.D, values)
				result = valuesCopy
			}
			another, err := s.plan.Next(ctx)
			if err != nil {
				return result, err
			}
			if another {
				return result, fmt.Errorf("more than one row returned by a subquery used as an expression")
			}
		}
	}

	return result, nil
}

// subqueryTupleOrdering returns whether the rows of the subquery are ordered
// such that the resulting subquery tuple can be considered fully sorted.
// For this to happen, the columns in the subquery must be sorted in the same
// direction and with the same order of precedence that the tuple will have. The
// method will return a boolean specifying whether the result is in sorted order,
// and if so, will specify which direction it is sorted in.
//
// TODO(knz): This will not work for subquery renders that are not row dependent
// like
//   SELECT 1 IN (SELECT 1 ORDER BY 1)
// because even if they are included in an ORDER BY clause, they will not be part
// of the plan.Ordering().
func (s *subquery) subqueryTupleOrdering() (bool, encoding.Direction) {
	// Columns must be sorted in the order that they appear in the render
	// and which they will later appear in the resulting tuple.
	desired := make(sqlbase.ColumnOrdering, len(s.plan.Columns()))
	for i := range desired {
		desired[i] = sqlbase.ColumnOrderInfo{
			ColIdx:    i,
			Direction: encoding.Ascending,
		}
	}

	// Check Ascending direction.
	order := s.plan.Ordering()
	match := order.computeMatch(desired)
	if match == len(desired) {
		return true, encoding.Ascending
	}
	// Check Descending direction.
	match = order.reverse().computeMatch(desired)
	if match == len(desired) {
		return true, encoding.Descending
	}
	return false, 0
}

// subqueryPlanVisitor is responsible for acting on the query plan
// that implements the sub-query, after it has been populated by
// subqueryVisitor. This visitor supports both starting
// and evaluating the sub-plans in one recursion.
type subqueryPlanVisitor struct {
	p *planner
}

func (v *subqueryPlanVisitor) subqueryNode(ctx context.Context, sq *subquery) error {
	if !sq.expanded {
		panic("subquery was not expanded properly")
	}
	if !sq.started {
		if err := v.p.startPlan(ctx, sq.plan); err != nil {
			return err
		}
		sq.started = true
		res, err := sq.doEval(ctx)
		if err != nil {
			return err
		}
		sq.result = res
	}
	return nil
}

func (v *subqueryPlanVisitor) enterNode(_ context.Context, _ string, n planNode) bool {
	if _, ok := n.(*explainPlanNode); ok {
		// EXPLAIN doesn't start/substitute sub-queries.
		return false
	}
	return true
}

func (p *planner) startSubqueryPlans(ctx context.Context, plan planNode) error {
	// We also run and pre-evaluate the subqueries during start,
	// so as to avoid re-running the sub-query for every row
	// in the results of the surrounding planNode.
	p.subqueryPlanVisitor = subqueryPlanVisitor{p: p}
	return walkPlan(ctx, plan, planObserver{
		subqueryNode: p.subqueryPlanVisitor.subqueryNode,
		enterNode:    p.subqueryPlanVisitor.enterNode,
	})
}

// subquerySpanCollector is responsible for collecting all read spans that
// subqueries in a query plan may touch. Subqueries should never be performing
// any write operations, so only the read spans are collected.
// FOR REVIEW: this assumption is correct, right?
type subquerySpanCollector struct {
	reads roachpb.Spans
}

func (v *subquerySpanCollector) subqueryNode(ctx context.Context, sq *subquery) error {
	reads, writes, err := sq.plan.Spans(ctx)
	if err != nil {
		return err
	}
	if len(writes) > 0 {
		return errors.Errorf("unexpected span writes in subquery: %v", writes)
	}
	v.reads = append(v.reads, reads...)
	return nil
}

func collectSubquerySpans(ctx context.Context, plan planNode) (roachpb.Spans, error) {
	var v subquerySpanCollector
	po := planObserver{subqueryNode: v.subqueryNode}
	if err := walkPlan(ctx, plan, po); err != nil {
		return nil, err
	}
	return v.reads, nil
}

// subqueryVisitor replaces parser.Subquery syntax nodes by a
// sql.subquery node and an initial query plan for running the
// sub-query.
type subqueryVisitor struct {
	*planner
	columns int
	path    []parser.Expr // parent expressions
	pathBuf [4]parser.Expr
	err     error

	// TODO(andrei): plumb the context through the parser.Visitor.
	ctx context.Context
}

var _ parser.Visitor = &subqueryVisitor{}

func (v *subqueryVisitor) VisitPre(expr parser.Expr) (recurse bool, newExpr parser.Expr) {
	if v.err != nil {
		return false, expr
	}

	if _, ok := expr.(*subquery); ok {
		// We already replaced this one; do nothing.
		return false, expr
	}

	v.path = append(v.path, expr)

	var exists *parser.ExistsExpr
	sq, ok := expr.(*parser.Subquery)
	if !ok {
		exists, ok = expr.(*parser.ExistsExpr)
		if !ok {
			return true, expr
		}
		sq, ok = exists.Subquery.(*parser.Subquery)
		if !ok {
			return true, expr
		}
	}

	// Calling newPlan() might recursively invoke expandSubqueries, so we need to preserve
	// the state of the visitor across the call to newPlan().
	visitorCopy := v.planner.subqueryVisitor
	plan, err := v.planner.newPlan(v.ctx, sq.Select, nil, false)
	v.planner.subqueryVisitor = visitorCopy
	if err != nil {
		v.err = err
		return false, expr
	}

	result := &subquery{planner: v.planner, subquery: sq, plan: plan}

	if exists != nil {
		result.execMode = execModeExists
		result.typ = parser.TypeBool
	} else {
		wantedNumColumns, execMode := v.getSubqueryContext()
		result.execMode = execMode

		// First check that the number of columns match.
		cols := plan.Columns()
		if numColumns := len(cols); wantedNumColumns != numColumns {
			switch wantedNumColumns {
			case 1:
				v.err = fmt.Errorf("subquery must return only one column, found %d",
					numColumns)
			default:
				v.err = fmt.Errorf("subquery must return %d columns, found %d",
					wantedNumColumns, numColumns)
			}
			return false, expr
		}

		if wantedNumColumns == 1 && execMode != execModeAllRowsNormalized {
			// This seems hokey, but if we don't do this then the subquery expands
			// to a tuple of tuples instead of a tuple of values and an expression
			// like "k IN (SELECT foo FROM bar)" will fail because we're comparing
			// a single value against a tuple.
			result.typ = cols[0].Typ
		} else {
			colTypes := make(parser.TTuple, wantedNumColumns)
			for i, col := range cols {
				colTypes[i] = col.Typ
			}
			result.typ = colTypes
		}
	}

	return false, result
}

func (v *subqueryVisitor) VisitPost(expr parser.Expr) parser.Expr {
	if v.err == nil {
		v.path = v.path[:len(v.path)-1]
	}
	return expr
}

func (p *planner) replaceSubqueries(
	ctx context.Context, expr parser.Expr, columns int,
) (parser.Expr, error) {
	p.subqueryVisitor = subqueryVisitor{planner: p, columns: columns, ctx: ctx}
	p.subqueryVisitor.path = p.subqueryVisitor.pathBuf[:0]
	expr, _ = parser.WalkExpr(&p.subqueryVisitor, expr)
	return expr, p.subqueryVisitor.err
}

// getSubqueryContext returns:
// - the desired number of columns;
// - the mode in which the sub-query should be executed.
func (v *subqueryVisitor) getSubqueryContext() (columns int, execMode subqueryExecMode) {
	for i := len(v.path) - 1; i >= 0; i-- {
		switch e := v.path[i].(type) {
		case *parser.ExistsExpr:
			continue
		case *parser.Subquery:
			continue
		case *parser.ParenExpr:
			continue

		case *parser.ArrayFlatten:
			// If the subquery is inside of an ARRAY constructor, it must return a
			// single-column, multi-row result.
			return 1, execModeAllRows

		case *parser.ComparisonExpr:
			// The subquery must occur on the right hand side of the comparison.
			//
			// TODO(pmattis): Figure out a way to lift this restriction so that we
			// can support:
			//
			//   SELECT (SELECT 1, 2) = (SELECT 1, 2)
			columns = 1
			switch t := e.Left.(type) {
			case *parser.Tuple:
				columns = len(t.Exprs)
			case *parser.DTuple:
				columns = len(t.D)
			}

			execMode = execModeOneRow
			switch e.Operator {
			case parser.In, parser.NotIn:
				execMode = execModeAllRowsNormalized
			}

			return columns, execMode

		default:
			// Any other expr that has this sub-query as operand
			// is expecting a single value.
			return 1, execModeOneRow
		}
	}

	// We have not encountered any non-paren, non-IN expression so far,
	// so the outer context is informing us of the desired number of
	// columns.
	return v.columns, execModeOneRow
}
