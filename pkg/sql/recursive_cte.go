// Copyright 2019 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// recursiveCTENode implements the logic for a recursive CTE:
//  1. Evaluate the initial query; emit the results and also save them in
//     a "working" table.
//  2. So long as the working table is not empty:
//     * evaluate the recursive query, substituting the current contents of
//       the working table for the recursive self-reference;
//     * emit all resulting rows, and save them as the next iteration's
//       working table.
// The recursive query tree is regenerated each time using a callback
// (implemented by the execbuilder).
type recursiveCTENode struct {
	initial planNode

	genIterationFn exec.RecursiveCTEIterationFn

	label string

	recursiveCTERun
}

type recursiveCTERun struct {
	// typs is the schema of the rows produced by this CTE.
	typs []*types.T
	// workingRows contains the rows produced by the current iteration (aka the
	// "working" table).
	workingRows rowContainerHelper
	iterator    *rowContainerIterator
	currentRow  tree.Datums

	initialDone bool
	done        bool
}

func (n *recursiveCTENode) startExec(params runParams) error {
	n.typs = planTypes(n.initial)
	n.workingRows.init(n.typs, params.extendedEvalCtx, "cte" /* opName */)
	return nil
}

func (n *recursiveCTENode) Next(params runParams) (bool, error) {
	if err := params.p.cancelChecker.Check(); err != nil {
		return false, err
	}

	if !n.initialDone {
		// Fully consume the initial rows (we could have read the initial rows
		// one at a time and return it in the same fashion, but that would
		// require special-case behavior).
		for {
			ok, err := n.initial.Next(params)
			if err != nil {
				return false, err
			}
			if !ok {
				break
			}
			if err = n.workingRows.addRow(params.ctx, n.initial.Values()); err != nil {
				return false, err
			}
		}
		n.iterator = newRowContainerIterator(params.ctx, n.workingRows, n.typs)
		n.initialDone = true
	}

	if n.done {
		return false, nil
	}

	if n.workingRows.len() == 0 {
		// Last iteration returned no rows.
		n.done = true
		return false, nil
	}

	var err error
	n.currentRow, err = n.iterator.next()
	if err != nil {
		return false, err
	}
	if n.currentRow != nil {
		// There are more rows to return from the last iteration.
		return true, nil
	}

	// Let's run another iteration.

	n.iterator.close()
	n.iterator = nil
	lastWorkingRows := n.workingRows
	defer lastWorkingRows.close(params.ctx)

	n.workingRows = rowContainerHelper{}
	n.workingRows.init(n.typs, params.extendedEvalCtx, "cte" /* opName */)

	// Set up a bufferNode that can be used as a reference for a scanBufferNode.
	buf := &bufferNode{
		// The plan here is only useful for planColumns, so it's ok to always use
		// the initial plan.
		plan:  n.initial,
		typs:  n.typs,
		rows:  lastWorkingRows,
		label: n.label,
	}
	newPlan, err := n.genIterationFn(newExecFactory(params.p), buf)
	if err != nil {
		return false, err
	}

	if err := runPlanInsidePlan(params, newPlan.(*planComponents), &n.workingRows); err != nil {
		return false, err
	}

	n.iterator = newRowContainerIterator(params.ctx, n.workingRows, n.typs)
	n.currentRow, err = n.iterator.next()
	if err != nil {
		return false, err
	}
	return n.currentRow != nil, nil
}

func (n *recursiveCTENode) Values() tree.Datums {
	return n.currentRow
}

func (n *recursiveCTENode) Close(ctx context.Context) {
	n.initial.Close(ctx)
	n.workingRows.close(ctx)
	if n.iterator != nil {
		n.iterator.close()
		n.iterator = nil
	}
}
