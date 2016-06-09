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

	"github.com/cockroachdb/cockroach/sql/parser"
)

// subquery represents a subquery expression in an expression tree
// after it has been converted to a query plan. It is carried
// in the expression tree from the point type checking occurs to
// the point the query starts execution / evaluation.
type subquery struct {
	typ            parser.Datum
	subquery       *parser.Subquery
	execMode       subqueryExecMode
	wantNormalized bool
	expanded       bool
	started        bool
	plan           planNode
	result         parser.Datum
	err            error
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

func (s *subquery) TypeCheck(_ *parser.SemaContext, desired parser.Datum) (parser.TypedExpr, error) {
	// TODO(knz): if/when type checking can be extracted from the
	// newPlan recursion, we can propagate the desired type to the
	// sub-query. For now, the type is simply derived during the subquery node
	// creation by looking at the result column types.

	// TODO(nvanbenschoten) Type checking for the comparison operator(s)
	// should take this new node into account. In particular it should
	// check that the tuple types match pairwise.
	return s, nil
}

func (s *subquery) ReturnType() parser.Datum { return s.typ }

func (s *subquery) Eval(_ *parser.EvalContext) (parser.Datum, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.result == nil {
		panic("subquery was not pre-evaluated properly")
	}
	return s.result, s.err
}

func (s *subquery) doEval() (parser.Datum, error) {
	var result parser.Datum
	switch s.execMode {
	case execModeExists:
		// For EXISTS expressions, all we want to know is if there is at least one
		// result.
		next, err := s.plan.Next()
		if s.err = err; err != nil {
			return result, err
		}
		if next {
			result = parser.MakeDBool(true)
		}
		if result == nil {
			result = parser.MakeDBool(false)
		}

	case execModeAllRows:
		var rows parser.DTuple
		next, err := s.plan.Next()
		for ; next; next, err = s.plan.Next() {
			values := s.plan.Values()
			switch len(values) {
			case 1:
				// This seems hokey, but if we don't do this then the subquery expands
				// to a tuple of tuples instead of a tuple of values and an expression
				// like "k IN (SELECT foo FROM bar)" will fail because we're comparing
				// a single value against a tuple.
				rows = append(rows, values[0])
			default:
				// The result from plan.Values() is only valid until the next call to
				// plan.Next(), so make a copy.
				valuesCopy := make(parser.DTuple, len(values))
				copy(valuesCopy, values)
				rows = append(rows, &valuesCopy)
			}
		}
		if s.err = err; err != nil {
			return result, err
		}
		if s.wantNormalized {
			rows.Normalize()
		}
		result = &rows

	case execModeOneRow:
		result = parser.DNull
		next, err := s.plan.Next()
		if s.err = err; err != nil {
			return result, err
		}
		if next {
			values := s.plan.Values()
			switch len(values) {
			case 1:
				result = values[0]
			default:
				valuesCopy := make(parser.DTuple, len(values))
				copy(valuesCopy, values)
				result = &valuesCopy
			}
			another, err := s.plan.Next()
			if s.err = err; err != nil {
				return result, err
			}
			if another {
				s.err = fmt.Errorf("more than one row returned by a subquery used as an expression")
				return result, s.err
			}
		}
	}

	return result, nil
}

// subqueryPlanVisitor is responsible for acting on the query plan
// that implements the sub-query, after it has been populated by
// subqueryVisitor.  This visitor supports both expanding, starting
// and evaluating the sub-plans in one recursion.
type subqueryPlanVisitor struct {
	doExpand bool
	doStart  bool
	doEval   bool
	err      error
}

var _ parser.Visitor = &subqueryPlanVisitor{}

func (v *subqueryPlanVisitor) VisitPre(expr parser.Expr) (recurse bool, newExpr parser.Expr) {
	if v.err != nil {
		return false, expr
	}
	if sq, ok := expr.(*subquery); ok {
		if v.doExpand && !sq.expanded {
			v.err = sq.plan.expandPlan()
			sq.expanded = true
		}
		if v.err == nil && v.doStart && !sq.started {
			if !sq.expanded {
				panic("subquery was not expanded properly")
			}
			v.err = sq.plan.Start()
			sq.started = true
		}
		if v.err == nil && v.doEval && sq.result == nil {
			if !sq.expanded || !sq.started {
				panic("subquery was not expanded or prepared properly")
			}
			sq.result, sq.err = sq.doEval()
			if sq.err != nil {
				v.err = sq.err
			}
		}
		return false, expr
	}
	return true, expr
}

func (v *subqueryPlanVisitor) VisitPost(expr parser.Expr) parser.Expr { return expr }

func (p *planner) expandSubqueryPlans(expr parser.Expr) error {
	if expr == nil {
		return nil
	}
	p.subqueryPlanVisitor = subqueryPlanVisitor{doExpand: true}
	_, _ = parser.WalkExpr(&p.subqueryPlanVisitor, expr)
	return p.subqueryPlanVisitor.err
}

func (p *planner) startSubqueryPlans(expr parser.Expr) error {
	if expr == nil {
		return nil
	}
	// We also run and pre-evaluate the subqueries during start,
	// so as to avoid re-running the sub-query for every row
	// in the results of the surrounding planNode.
	p.subqueryPlanVisitor = subqueryPlanVisitor{doStart: true, doEval: true}
	_, _ = parser.WalkExpr(&p.subqueryPlanVisitor, expr)
	return p.subqueryPlanVisitor.err
}

// collectSubqueryPlansVisitor gathers all the planNodes implementing
// sub-queries in a given expression. This is used by EXPLAIN to show
// the sub-plans.
type collectSubqueryPlansVisitor struct {
	plans []planNode
}

var _ parser.Visitor = &collectSubqueryPlansVisitor{}

func (v *collectSubqueryPlansVisitor) VisitPre(expr parser.Expr) (
	recurse bool, newExpr parser.Expr) {
	if sq, ok := expr.(*subquery); ok {
		if sq.plan == nil {
			panic("cannot collect the sub-plans before they were expanded")
		}
		v.plans = append(v.plans, sq.plan)
		return false, expr
	}
	return true, expr
}

func (v *collectSubqueryPlansVisitor) VisitPost(expr parser.Expr) parser.Expr { return expr }

func (p *planner) collectSubqueryPlans(expr parser.Expr, result []planNode) []planNode {
	if expr == nil {
		return result
	}
	p.collectSubqueryPlansVisitor = collectSubqueryPlansVisitor{plans: result}
	_, _ = parser.WalkExpr(&p.collectSubqueryPlansVisitor, expr)
	return p.collectSubqueryPlansVisitor.plans
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

	planMaker := *v.planner
	plan, err := planMaker.newPlan(sq.Select, nil, false)
	if err != nil {
		v.err = err
		return false, expr
	}

	result := &subquery{subquery: sq, plan: plan}

	if exists != nil {
		result.execMode = execModeExists
		result.typ = parser.TypeBool
	} else {
		wantedNumColumns, ctxIsInExpr := v.getSubqueryContext()

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

		// Decide how the sub-query will be evaluated.
		if ctxIsInExpr {
			result.execMode = execModeAllRows
			result.wantNormalized = true
		} else {
			result.execMode = execModeOneRow
		}

		if wantedNumColumns == 1 && !ctxIsInExpr {
			// This seems hokey, but if we don't do this then the subquery expands
			// to a tuple of tuples instead of a tuple of values and an expression
			// like "k IN (SELECT foo FROM bar)" will fail because we're comparing
			// a single value against a tuple.
			result.typ = cols[0].Typ
		} else {
			colTypes := make(parser.DTuple, wantedNumColumns)
			for i, col := range cols {
				colTypes[i] = col.Typ
			}
			result.typ = &colTypes
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

func (p *planner) replaceSubqueries(expr parser.Expr, columns int) (parser.Expr, error) {
	p.subqueryVisitor = subqueryVisitor{planner: p, columns: columns}
	p.subqueryVisitor.path = p.subqueryVisitor.pathBuf[:0]
	expr, _ = parser.WalkExpr(&p.subqueryVisitor, expr)
	return expr, p.subqueryVisitor.err
}

// getSubqueryContext returns:
// - the desired number of columns;
// - whether the sub-query is operand of a IN/NOT IN expression.
func (v *subqueryVisitor) getSubqueryContext() (columns int, ctxIsInExpr bool) {
	for i := len(v.path) - 1; i >= 0; i-- {
		switch e := v.path[i].(type) {
		case *parser.ExistsExpr:
			continue
		case *parser.Subquery:
			continue
		case *parser.ParenExpr:
			continue

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
				columns = len(*t)
			}

			ctxIsInExpr = false
			switch e.Operator {
			case parser.In, parser.NotIn:
				ctxIsInExpr = true
			}

			return columns, ctxIsInExpr

		default:
			// Any other expr that has this sub-query as operand
			// is expecting a single value.
			return 1, false
		}
	}

	// We have not encountered any non-paren, non-IN expression so far,
	// so the outer context is informing us of the desired number of
	// columns.
	return v.columns, false
}
