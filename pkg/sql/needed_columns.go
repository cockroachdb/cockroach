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

import (
	"fmt"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

// initNeededColumns triggers needed column detection and propagation
// throughout the planNode. Sub-queries, if any and doSubqueries is
// true, are also processed.
func (p *planner) initNeededColumns(plan planNode, needed []bool, doSubqueries bool) {
	setNeededColumns(plan, needed)
	if doSubqueries {
		v := planVisitor{p: p, observer: subqueryInitializer{p: p}}
		v.visit(plan)
	}
}

// setNeededColumns informs the node about which columns are
// effectively needed by the consumer of its result rows. The needed
// array can be nil, which means that the current non-omitted columns
// in the result set are needed, i.e. preserve and propagate the
// existing settings.
func setNeededColumns(plan planNode, needed []bool) {
	if needed == nil {
		// Keep the current configuration from the node's ResultColumns.
		cols := plan.Columns()
		needed = make([]bool, len(cols))
		for i := range cols {
			needed[i] = !cols[i].omitted
		}
	}

	switch n := plan.(type) {
	case *createTableNode:
		if n.n.As() {
			setNeededColumns(n.sourcePlan, nil)
		}

	case *createViewNode:
		setNeededColumns(n.sourcePlan, nil)

	case *explainDebugNode:
		setNeededColumns(n.plan, nil)

	case *explainTraceNode:
		setNeededColumns(n.plan, nil)

	case *explainPlanNode:
		// EXPLAIN(PLAN) takes care of its needed column initialization
		// itself, based on the presence of the NOOPTIMIZE option.

	case *limitNode:
		setNeededColumns(n.plan, needed)

	case *indexJoinNode:
		setNeededColumns(n.index, n.valNeededIndex)
		setNeededColumns(n.table, needed)

	case *unionNode:
		if !n.emitAll {
			// only if we are not computing uniqueness can
			// we skip loading values in the source.
			needed = nil
		}
		setNeededColumns(n.left, needed)
		setNeededColumns(n.right, needed)

	case *joinNode:
		markOmitted(n.columns, needed)
		leftNeeded, rightNeeded := n.pred.getNeededColumns(needed)
		setNeededColumns(n.left.plan, leftNeeded)
		setNeededColumns(n.right.plan, rightNeeded)

	case *ordinalityNode:
		markOmitted(n.columns[:len(needed)-1], needed[:len(needed)-1])
		setNeededColumns(n.source, needed[:len(needed)-1])

	case *valuesNode:
		markOmitted(n.columns, needed)

	case *scanNode:
		copy(n.valNeededForCol, needed)

		for i := range needed {
			// All the values involved in the filter expression are needed too.
			if n.filterVars.IndexedVarUsed(i) {
				n.valNeededForCol[i] = true
			}
		}

		markOmitted(n.resultColumns, n.valNeededForCol)

	case *distinctNode:
		sourceNeeded := make([]bool, len(n.plan.Columns()))
		copy(sourceNeeded, needed)
		// All the sorting columns are also needed.
		for i, o := range n.columnsInOrder {
			sourceNeeded[i] = sourceNeeded[i] || o
		}
		setNeededColumns(n.plan, sourceNeeded)

	case *selectNode:
		markOmitted(n.columns, needed)

		// Optimization: remove all the render expressions that are not
		// needed. While doing so, some indexed vars may disappear
		// entirely, which may enable omission of more columns from the
		// source. To detect this, we need to reset the IndexedVarHelper
		// and re-bind all the expressions.
		n.ivarHelper.Reset()
		for i, val := range needed {
			if !val {
				// This render is not used, so reduce its expression to NULL.
				n.render[i] = parser.DNull
				continue
			}
			n.render[i] = n.ivarHelper.Rebind(n.render[i])
		}
		n.filter = n.ivarHelper.Rebind(n.filter)

		// Now detect which columns from the source are still needed.
		neededFromSource := make([]bool, len(n.source.info.sourceColumns))
		for i := range neededFromSource {
			neededFromSource[i] = n.ivarHelper.IndexedVarUsed(i)
		}
		setNeededColumns(n.source.plan, neededFromSource)

	case *selectTopNode:
		if n.plan == nil {
			// Not yet expanded: we don't know what the relationship is
			// between the sub-nodes, so the best we can do is to trigger
			// the optimization in the source node as if all columns were
			// needed.
			setNeededColumns(n.source, nil)
		} else {
			setNeededColumns(n.plan, needed)
		}

	case *sortNode:
		sourceNeeded := make([]bool, len(n.plan.Columns()))
		copy(sourceNeeded[:len(needed)], needed)

		// All the ordering columns are also needed.
		for _, o := range n.ordering {
			sourceNeeded[o.ColIdx] = true
		}

		markOmitted(n.columns, sourceNeeded[:len(n.columns)])
		setNeededColumns(n.plan, sourceNeeded)

	case *groupNode:
		// TODO(knz) This can be optimized by removing the aggregation
		// results that are not needed, then removing additional renders
		// from the source that would otherwise only be needed for the
		// omitted aggregation results.
		setNeededColumns(n.plan, nil)

	case *windowNode:
		// TODO(knz) This can be optimized by removing the window function
		// definitions that are not needed, then removing additional
		// renders from the source that would otherwise only be needed for
		// the omitted window definitions.
		setNeededColumns(n.plan, nil)

	case *deleteNode:
		// TODO(knz) This can be optimized by omitting the columns that
		// are not part of the primary key, do not participate in
		// foreign key relations and that are not needed for RETURNING.
		setNeededColumns(n.run.rows, nil)

	case *updateNode:
		// TODO(knz) This can be optimized by omitting the columns that
		// are not part of the primary key, do not participate in
		// foreign key relations and that are not needed for RETURNING.
		setNeededColumns(n.run.rows, nil)

	case *insertNode:
		// TODO(knz) This can be optimized by omitting the columns that
		// are not part of the primary key, do not participate in
		// foreign key relations and that are not needed for RETURNING.
		setNeededColumns(n.run.rows, nil)

	default:
		if !nodeTypesWithoutQuery[reflect.TypeOf(plan)] {
			panic(fmt.Sprintf("unhandled node type: %T", plan))
		}
	}
}

// markOmitted propagates the information from the needed array back
// to the ResultColumns array.
func markOmitted(cols ResultColumns, needed []bool) {
	for i, val := range needed {
		cols[i].omitted = !val
	}
}

// subqueryInitializer ensures that initNeededColumns() is called on
// the planNodes of all sub-query expressions.
type subqueryInitializer struct {
	p *planner
}

var _ planObserver = subqueryInitializer{}
var _ parser.Visitor = subqueryInitializer{}

// expr implements the planObserver interface.
func (i subqueryInitializer) expr(_, _ string, _ int, expr parser.Expr) {
	if expr == nil {
		return
	}
	parser.WalkExprConst(i, expr)
}

func (i subqueryInitializer) enterNode(_ string, _ planNode) bool { return true }
func (i subqueryInitializer) leaveNode(_ string)                  {}
func (i subqueryInitializer) attr(_, _, _ string)                 {}

// VisitPre implements the parser.Visitor interface.
func (i subqueryInitializer) VisitPre(expr parser.Expr) (recurse bool, newExpr parser.Expr) {
	if sq, ok := expr.(*subquery); ok && sq.plan != nil {
		var needed []bool
		if sq.execMode == execModeExists {
			// Mark all columns as not needed -- EXISTS does not need values.
			needed = make([]bool, len(sq.plan.Columns()))
		}
		i.p.initNeededColumns(sq.plan, needed, true)
		return false, expr
	}
	return true, expr
}

// VisitPost implements the parser.Visitor interface.
func (i subqueryInitializer) VisitPost(expr parser.Expr) parser.Expr { return expr }

// nodeTypesWithoutQuery indicates which nodes contain no sub-plans
// that need recursing into. We need this to check that the pattern
// match in setNeededColumns() is not incomplete.
var nodeTypesWithoutQuery = map[reflect.Type]bool{
	reflect.TypeOf(&alterTableNode{}):     true,
	reflect.TypeOf(&copyNode{}):           true,
	reflect.TypeOf(&createDatabaseNode{}): true,
	reflect.TypeOf(&createIndexNode{}):    true,
	reflect.TypeOf(&createUserNode{}):     true,
	reflect.TypeOf(&delayedNode{}):        true,
	reflect.TypeOf(&dropDatabaseNode{}):   true,
	reflect.TypeOf(&dropIndexNode{}):      true,
	reflect.TypeOf(&dropTableNode{}):      true,
	reflect.TypeOf(&dropViewNode{}):       true,
	reflect.TypeOf(&emptyNode{}):          true,
	reflect.TypeOf(&hookFnNode{}):         true,
	reflect.TypeOf(&splitNode{}):          true,
	reflect.TypeOf(&valueGenerator{}):     true,
	reflect.TypeOf(&valuesNode{}):         true,
}
