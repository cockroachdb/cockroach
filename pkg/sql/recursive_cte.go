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
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	// workingRows contains the rows produced by the current iteration (aka the
	// "working" table).
	workingRows *rowcontainer.RowContainer
	// nextRowIdx is the index inside workingRows of the next row to be returned
	// by the operator.
	nextRowIdx int

	initialDone bool
	done        bool
}

func (n *recursiveCTENode) startExec(params runParams) error {
	n.workingRows = rowcontainer.NewRowContainer(
		params.EvalContext().Mon.MakeBoundAccount(),
		sqlbase.ColTypeInfoFromResCols(getPlanColumns(n.initial, false /* mut */)),
		0, /* rowCapacity */
	)
	n.nextRowIdx = 0
	return nil
}

func (n *recursiveCTENode) Next(params runParams) (bool, error) {
	if err := params.p.cancelChecker.Check(); err != nil {
		return false, err
	}

	n.nextRowIdx++

	if !n.initialDone {
		ok, err := n.initial.Next(params)
		if err != nil {
			return false, err
		}
		if ok {
			if _, err = n.workingRows.AddRow(params.ctx, n.initial.Values()); err != nil {
				return false, err
			}
			return true, nil
		}
		n.initialDone = true
	}

	if n.done {
		return false, nil
	}

	if n.workingRows.Len() == 0 {
		// Last iteration returned no rows.
		n.done = true
		return false, nil
	}

	// There are more rows to return from the last iteration.
	if n.nextRowIdx <= n.workingRows.Len() {
		return true, nil
	}

	// Let's run another iteration.

	lastWorkingRows := n.workingRows
	defer lastWorkingRows.Close(params.ctx)

	n.workingRows = rowcontainer.NewRowContainer(
		params.EvalContext().Mon.MakeBoundAccount(),
		sqlbase.ColTypeInfoFromResCols(getPlanColumns(n.initial, false /* mut */)),
		0, /* rowCapacity */
	)

	// Set up a bufferNode that can be used as a reference for a scanBufferNode.
	buf := &bufferNode{
		// The plan here is only useful for planColumns, so it's ok to always use
		// the initial plan.
		plan:         n.initial,
		bufferedRows: lastWorkingRows,
		label:        n.label,
	}
	newPlan, err := n.genIterationFn(buf)
	if err != nil {
		return false, err
	}

	if err := runPlanInsidePlan(params, newPlan.(*planTop), n.workingRows); err != nil {
		return false, err
	}
	n.nextRowIdx = 1
	return n.workingRows.Len() > 0, nil
}

func (n *recursiveCTENode) Values() tree.Datums {
	return n.workingRows.At(n.nextRowIdx - 1)
}

func (n *recursiveCTENode) Close(ctx context.Context) {
	n.initial.Close(ctx)
	n.workingRows.Close(ctx)
}
