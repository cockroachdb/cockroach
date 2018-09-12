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

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type virtualTableValuesNode struct {
	columns sqlbase.ResultColumns
	tuples  [][]tree.TypedExpr
	// isConst is set if the virtualTableValuesNode only contains constant expressions (no
	// subqueries). In this case, rows will be evaluated during the first call
	// to planNode.Start and memoized for future consumption. A virtualTableValuesNode with
	// isConst = true can serve its virtualTableValues multiple times. See virtualTableValuesNode.Reset.
	isConst bool

	// specifiedInQuery is set if the virtualTableValuesNode represents a literal
	// relational expression that was present in the original SQL text,
	// as opposed to e.g. a virtualTableValuesNode resulting from the expansion of
	// a vtable value generator. This changes distsql physical planning.
	specifiedInQuery bool

	virtualTableValuesRun
}

func (p *planner) newContainerVirtualTableValuesNode(
	columns sqlbase.ResultColumns, capacity int, next func() (tree.Datums, bool, error),
) *virtualTableValuesNode {
	return &virtualTableValuesNode{
		columns: columns,
		isConst: true,
		virtualTableValuesRun: virtualTableValuesRun{
			rows: sqlbase.NewRowContainer(
				p.EvalContext().Mon.MakeBoundAccount(), sqlbase.ColTypeInfoFromResCols(columns), capacity,
			),
			next: next,
		},
	}
}

// virtualTableValuesRun is the run-time state of a virtualTableValuesNode during local execution.
type virtualTableValuesRun struct {
	rows    *sqlbase.RowContainer
	next    func() (tree.Datums, bool, error)
	nextRow int
}

func (n *virtualTableValuesNode) startExec(params runParams) error {
	if n.rows != nil {
		if !n.isConst {
			log.Fatalf(params.ctx, "virtualTableValuesNode evaluated twice")
		}
		return nil
	}

	// This node is coming from a SQL query (as opposed to sortNode and
	// others that create a virtualTableValuesNode internally for storing results
	// from other planNodes), so its expressions need evaluating.
	// This may run subqueries.
	n.rows = sqlbase.NewRowContainer(
		params.extendedEvalCtx.Mon.MakeBoundAccount(),
		sqlbase.ColTypeInfoFromResCols(n.columns),
		len(n.tuples),
	)

	row := make([]tree.Datum, len(n.columns))
	for _, tupleRow := range n.tuples {
		for i, typedExpr := range tupleRow {
			if n.columns[i].Omitted {
				row[i] = tree.DNull
			} else {
				var err error
				row[i], err = typedExpr.Eval(params.EvalContext())
				if err != nil {
					return err
				}
			}
		}
		if _, err := n.rows.AddRow(params.ctx, row); err != nil {
			return err
		}
	}

	return nil
}

// Reset resets the virtualTableValuesNode processing state without requiring recomputation
// of the virtualTableValues tuples if the virtualTableValuesNode is processed again. Reset can only
// be called if virtualTableValuesNode.isConst.
func (n *virtualTableValuesNode) Reset(ctx context.Context) {
	if !n.isConst {
		log.Fatalf(ctx, "virtualTableValuesNode.Reset can only be called on constant virtualTableValuesNodes")
	}
	n.nextRow = 0
}

func (n *virtualTableValuesNode) Next(params runParams) (bool, error) {
	if n.nextRow >= n.rows.Len() {
		row, hasNext, err := n.next()
		if !hasNext {
			return false, err
		}
		n.rows.AddRow(params.ctx, row)
	}
	n.nextRow++
	return true, nil
}

func (n *virtualTableValuesNode) Values() tree.Datums {
	return n.rows.At(n.nextRow - 1)
}

func (n *virtualTableValuesNode) Close(ctx context.Context) {
	if n.rows != nil {
		n.rows.Close(ctx)
		n.rows = nil
	}
}
