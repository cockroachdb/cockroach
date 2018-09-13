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
)

type virtualTableValuesNode struct {
	virtualTableValuesRun
}

func (p *planner) newContainerVirtualTableValuesNode(
	columns sqlbase.ResultColumns, capacity int, next func() (tree.Datums, bool, error),
) *virtualTableValuesNode {
	return &virtualTableValuesNode{
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

// Reset resets the virtualTableValuesNode processing state without requiring recomputation
// of the virtualTableValues tuples if the virtualTableValuesNode is processed again. Reset can only
// be called if virtualTableValuesNode.isConst.
func (n *virtualTableValuesNode) Reset(ctx context.Context) {
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
