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
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

// optimizePlan transforms the query plan into its final form.  This
// includes calling expandPlan(). The SQL "prepare" phase, as well as
// the EXPLAIN statement, should merely build the plan node(s) and
// call optimizePlan(). This is called automatically by makePlan().
func (p *planner) optimizePlan(
	ctx context.Context, plan planNode, needed []bool,
) (planNode, error) {
	// We propagate the needed columns a first time. This will remove
	// any unused renders, which in turn may simplify expansion (remove
	// sub-expressions).
	setNeededColumns(plan, needed)

	newPlan, err := p.triggerFilterPropagation(ctx, plan)
	if err != nil {
		return plan, err
	}

	// Perform plan expansion; this does index selection, sort
	// optimization etc.
	newPlan, err = p.expandPlan(ctx, newPlan)
	if err != nil {
		return plan, err
	}

	// We now propagate the needed columns again. This will ensure that
	// the needed columns are properly computed for newly expanded nodes.
	setNeededColumns(newPlan, needed)

	// Now do the same work for all sub-queries.
	i := &subqueryInitializer{p: p}
	observer := planObserver{
		subqueryNode: i.subqueryNode,
		enterNode:    i.enterNode,
	}
	if err := walkPlan(ctx, newPlan, observer); err != nil {
		return plan, err
	}
	return newPlan, nil
}

// subqueryInitializer ensures that initNeededColumns() and
// optimizeFilters() is called on the planNodes of all sub-query
// expressions.
type subqueryInitializer struct {
	p *planner
}

// subqueryNode implements the planObserver interface.
func (i *subqueryInitializer) subqueryNode(ctx context.Context, sq *subquery) error {
	if sq.plan != nil && !sq.expanded {
		if sq.execMode == execModeExists || sq.execMode == execModeOneRow {
			numRows := parser.DInt(1)
			if sq.execMode == execModeOneRow {
				// When using a sub-query in a scalar context, we must
				// appropriately reject sub-queries that return more than 1
				// row.
				numRows = 2
			}

			sq.plan = &limitNode{p: i.p, plan: sq.plan, countExpr: parser.NewDInt(numRows)}
		}

		needed := make([]bool, len(sq.plan.Columns()))
		if sq.execMode != execModeExists {
			// EXISTS does not need values; the rest does.
			for i := range needed {
				needed[i] = true
			}
		}

		var err error
		sq.plan, err = i.p.optimizePlan(ctx, sq.plan, needed)
		if err != nil {
			return err
		}
		sq.expanded = true
	}
	return nil
}

func (i *subqueryInitializer) enterNode(_ context.Context, _ string, _ planNode) bool {
	return true
}
