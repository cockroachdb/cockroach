// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// optimizePlan transforms the query plan into its final form.  This
// includes calling expandPlan(). The SQL "prepare" phase, as well as
// the EXPLAIN statement, should merely build the plan node(s) and
// call optimizePlan(). This is called automatically by makePlan().
//
// The plan returned by optimizePlan *must* be Close()d, even in case
// of error, because it may contain memory-registered data structures
// and other things that need clean up.
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

	return newPlan, nil
}

// optimizeSubquery ensures plan optimization has been perfomed on the given subquery.
func (p *planner) optimizeSubquery(ctx context.Context, sq *subquery) error {
	if sq.expanded {
		// Already processed. Nothing to do.
		return nil
	}

	if log.V(2) {
		log.Infof(ctx, "optimizing subquery %d (%q)", sq.subquery.Idx, sq.subquery)
	}

	needed := make([]bool, len(planColumns(sq.plan)))
	if sq.execMode != distsqlrun.SubqueryExecModeExists {
		// EXISTS does not need values; the rest does.
		for i := range needed {
			needed[i] = true
		}
	}

	var err error
	sq.plan, err = p.optimizePlan(ctx, sq.plan, needed)
	if err != nil {
		return err
	}
	sq.expanded = true
	return nil
}
