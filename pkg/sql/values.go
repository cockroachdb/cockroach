// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

type valuesNode struct {
	zeroInputPlanNode

	// Note: the columns can be renamed in place (see planMutableColumns).
	columns colinfo.ResultColumns
	tuples  [][]tree.TypedExpr

	// specifiedInQuery is set if the valuesNode represents a literal
	// relational expression that was present in the original SQL text,
	// as opposed to e.g. a valuesNode resulting from the expansion of
	// a vtable value generator. This changes distsql physical planning.
	specifiedInQuery bool

	// externallyOwnedContainer allows an external entity to control its
	// lifetime so we don't call Close.  Used by copy to reuse the container.
	externallyOwnedContainer bool

	valuesRun

	// Allow passing a coldata.Batch through a valuesNode.
	coldataBatch coldata.Batch
}

func (p *planner) newContainerValuesNode(columns colinfo.ResultColumns, capacity int) *valuesNode {
	return &valuesNode{
		columns: columns,
		valuesRun: valuesRun{
			rows: rowcontainer.NewRowContainerWithCapacity(
				p.Mon().MakeBoundAccount(),
				colinfo.ColTypeInfoFromResCols(columns),
				capacity,
			),
		},
	}
}

// valuesRun is the run-time state of a valuesNode during local execution.
type valuesRun struct {
	rows    *rowcontainer.RowContainer
	nextRow int // The index of the next row.
}

func (n *valuesNode) startExec(params runParams) error {
	if n.coldataBatch != nil {
		return errors.AssertionFailedf("planning error: valuesNode started with coldata.Batch")
	}

	if n.rows != nil {
		// n.rows was already created in newContainerValuesNode.
		// Nothing to do here.
		return nil
	}

	// This node is coming from a SQL query (as opposed to sortNode and
	// others that create a valuesNode internally for storing results
	// from other planNodes), so its expressions need evaluating.
	// This may run subqueries.
	n.rows = rowcontainer.NewRowContainerWithCapacity(
		params.p.Mon().MakeBoundAccount(),
		colinfo.ColTypeInfoFromResCols(n.columns),
		len(n.tuples),
	)

	row := make([]tree.Datum, len(n.columns))
	for _, tupleRow := range n.tuples {
		for i, typedExpr := range tupleRow {
			var err error
			row[i], err = eval.Expr(params.ctx, params.EvalContext(), typedExpr)
			if err != nil {
				return err
			}
		}
		if _, err := n.rows.AddRow(params.ctx, row); err != nil {
			return err
		}
	}

	return nil
}

func (n *valuesNode) Next(runParams) (bool, error) {
	if n.nextRow >= n.rows.Len() {
		return false, nil
	}
	n.nextRow++
	return true, nil
}

func (n *valuesNode) Values() tree.Datums {
	return n.rows.At(n.nextRow - 1)
}

func (n *valuesNode) Close(ctx context.Context) {
	if n.rows != nil && !n.externallyOwnedContainer {
		n.rows.Close(ctx)
		n.rows = nil
	}
}
