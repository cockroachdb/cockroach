// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// recursiveCTENode implements the logic for a recursive CTE:
//  1. Evaluate the initial query; emit the results and also save them in
//     a "working" table.
//  2. So long as the working table is not empty:
//     * evaluate the recursive query, substituting the current contents of
//     the working table for the recursive self-reference;
//     * emit all resulting rows, and save them as the next iteration's
//     working table.
//
// The recursive query tree is regenerated each time using a callback
// (implemented by the execbuilder).
type recursiveCTENode struct {
	// The input plan node is for the initial query.
	singleInputPlanNode

	genIterationFn exec.RecursiveCTEIterationFn
	// iterationCount tracks the number of invocations of genIterationFn.
	iterationCount int

	label string

	// If true, all rows must be deduplicated against previous rows.
	deduplicate bool

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

	// allRows contains all distinct rows produced (in all iterations); only used
	// if deduplicating.
	allRows rowContainerHelper

	initialDone bool
	done        bool

	// err is only used to implement rowResultWriter.
	err error
}

func (n *recursiveCTENode) startExec(params runParams) error {
	n.typs = planTypes(n.input)
	n.workingRows.Init(params.ctx, n.typs, params.extendedEvalCtx, "cte" /* opName */)
	if n.deduplicate {
		n.allRows.InitWithDedup(params.ctx, n.typs, params.extendedEvalCtx, "cte-all" /* opName */)
	}
	return nil
}

func (n *recursiveCTENode) Next(params runParams) (bool, error) {
	if err := params.p.cancelChecker.Check(); err != nil {
		return false, err
	}

	if !n.initialDone {
		// Fully consume the initial rows (we could have read the initial rows one
		// at a time and returned them in the same fashion, but that would require
		// special-case behavior).
		for {
			ok, err := n.input.Next(params)
			if err != nil {
				return false, err
			}
			if !ok {
				break
			}
			if err := n.AddRow(params.ctx, n.input.Values()); err != nil {
				return false, err
			}
		}
		n.iterator = newRowContainerIterator(params.ctx, n.workingRows)
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

	var err error
	n.currentRow, err = n.iterator.Next()
	if err != nil {
		return false, err
	}
	if n.currentRow != nil {
		// There are more rows to return from the last iteration.
		return true, nil
	}

	// Let's run another iteration.

	n.iterator.Close()
	n.iterator = nil
	lastWorkingRows := n.workingRows
	defer lastWorkingRows.Close(params.ctx)

	n.workingRows = rowContainerHelper{}
	n.workingRows.Init(params.ctx, n.typs, params.extendedEvalCtx, "cte" /* opName */)

	// Set up a bufferNode that can be used as a reference for a scanBufferNode.
	buf := &bufferNode{
		// The plan here is only useful for planColumns, so it's ok to always use
		// the initial plan.
		singleInputPlanNode: singleInputPlanNode{n.input},
		typs:                n.typs,
		rows:                lastWorkingRows,
		label:               n.label,
	}
	newPlan, err := n.genIterationFn(newExecFactory(params.ctx, params.p), buf)
	if err != nil {
		return false, err
	}

	n.iterationCount++
	opName := "recursive-cte-iteration-" + strconv.Itoa(n.iterationCount)
	ctx, sp := tracing.ChildSpan(params.ctx, opName)
	defer sp.Finish()
	if err := runPlanInsidePlan(
		ctx, params, newPlan.(*planComponents), rowResultWriter(n),
		nil /* deferredRoutineSender */, "", /* stmtForDistSQLDiagram */
	); err != nil {
		return false, err
	}

	n.iterator = newRowContainerIterator(params.ctx, n.workingRows)
	n.currentRow, err = n.iterator.Next()
	if err != nil {
		return false, err
	}
	return n.currentRow != nil, nil
}

func (n *recursiveCTENode) Values() tree.Datums {
	return n.currentRow
}

func (n *recursiveCTENode) Close(ctx context.Context) {
	n.input.Close(ctx)
	if n.deduplicate {
		n.allRows.Close(ctx)
	}
	n.workingRows.Close(ctx)
	if n.iterator != nil {
		n.iterator.Close()
		n.iterator = nil
	}
}

// recursiveCTENode implements rowResultWriter and is used as the result writer
// for each iteration.
var _ rowResultWriter = (*recursiveCTENode)(nil)

// AddRow is part of the rowResultWriter interface.
//
// If we are not deduplicating, the rows are added to the workingRows container.
//
// If we are deduplicating, each row is either discarded if it has a duplicate
// in the allRows container or added to both allRows and workingRows otherwise.
func (n *recursiveCTENode) AddRow(ctx context.Context, row tree.Datums) error {
	if n.deduplicate {
		if ok, err := n.allRows.AddRowWithDedup(ctx, row); err != nil {
			return err
		} else if !ok {
			// Duplicate row; don't add to the resulting rows.
			return nil
		}
	}
	return n.workingRows.AddRow(ctx, row)
}

// SetRowsAffected is part of the rowResultWriter interface.
func (n *recursiveCTENode) SetRowsAffected(context.Context, int) {
}

// SetError is part of the rowResultWriter interface.
func (n *recursiveCTENode) SetError(err error) {
	n.err = err
}

// Error is part of the rowResultWriter interface.
func (n *recursiveCTENode) Err() error {
	return n.err
}
