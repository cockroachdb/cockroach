// Copyright 2016 The Cockroach Authors.
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
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package sql

import "github.com/cockroachdb/cockroach/pkg/sql/parser"

// optimizePlan transforms the query plan into its final form.  This
// includes calling expandPlan().  The SQL "prepare" phase, as well as
// the EXPLAIN statement, should merely build the plan node(s) and
// call optimizePlan(). This is called automatically by makePlan().
func (p *planner) optimizePlan(plan planNode, needed []bool) (planNode, error) {
	// We propagate the needed columns a first time. This will remove
	// any unused renders, which in turn may simplify expansion (remove
	// sub-expressions).
	setNeededColumns(plan, needed)

	// Perform plan expansion; this does index selection, sort
	// optimization etc.
	newPlan, err := p.expandPlan(plan)
	if err != nil {
		return nil, err
	}
	plan = newPlan

	// We now propagate the needed columns again. This will ensure that
	// the needed columns are properly computed for newly expanded nodes.
	setNeededColumns(plan, needed)

	// Now do the same work for all sub-queries.
	i := subqueryInitializer{p: p}
	v := planVisitor{p: p, observer: &i}
	v.visit(plan)
	return plan, i.err
}

// subqueryInitializer ensures that initNeededColumns() and
// optimizeFilters() is called on the planNodes of all sub-query
// expressions.
type subqueryInitializer struct {
	p   *planner
	err error
}

var _ planObserver = &subqueryInitializer{}
var _ parser.Visitor = &subqueryInitializer{}

// expr implements the planObserver interface.
func (i *subqueryInitializer) expr(_, _ string, _ int, expr parser.Expr) {
	if expr == nil || i.err != nil {
		return
	}
	parser.WalkExprConst(i, expr)
}

func (i *subqueryInitializer) enterNode(_ string, _ planNode) bool { return true }
func (i *subqueryInitializer) leaveNode(_ string)                  {}
func (i *subqueryInitializer) attr(_, _, _ string)                 {}

// VisitPre implements the parser.Visitor interface.
func (i *subqueryInitializer) VisitPre(expr parser.Expr) (recurse bool, newExpr parser.Expr) {
	if i.err != nil {
		return false, expr
	}

	if sq, ok := expr.(*subquery); ok && sq.plan != nil && !sq.expanded {
		needed := make([]bool, len(sq.plan.Columns()))
		if sq.execMode != execModeExists {
			// EXISTS does not need values; the rest does.
			for i := range needed {
				needed[i] = true
			}
		}
		var err error
		sq.plan, err = i.p.optimizePlan(sq.plan, needed)
		if err != nil {
			i.err = err
		}
		sq.expanded = true
		return false, expr
	}
	return true, expr
}

// VisitPost implements the parser.Visitor interface.
func (i *subqueryInitializer) VisitPost(expr parser.Expr) parser.Expr { return expr }
