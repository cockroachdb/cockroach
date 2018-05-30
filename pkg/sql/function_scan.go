// Copyright 2018 The Cockroach Authors.
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

// funcScanNode zips through one or more SRFs.
type funcScanNode struct {
	sources []planNode
	columns sqlbase.ResultColumns

	run applySRFRun
}

func (p *planner) RowsFrom(ctx context.Context, node *tree.RowsFromExpr) (planNode, error) {
	switch len(node.Items) {
	case 0:
		// This should never happen during parsing, but is semantically valid.
		return &zeroNode{}, nil
	case 1:
		// Shortcut when there is just one generator.
		return p.makeGenerator(ctx, node.Items[0])
	}

	n := funcScanNode{
		sources: make([]planNode, len(node.Items)),
		columns: make(sqlbase.ResultColumns, 0, len(node.Items)),
		run: applySRFRun{
			numColsPerSource: make([]int, len(node.Items)),
			done:             make([]bool, len(node.Items)),
		},
	}

	for i, expr := range node.Items {
		plan, err := p.makeGenerator(ctx, expr)
		if err != nil {
			return nil, err
		}
		srfCols := planColumns(plan)
		n.columns = append(n.columns, srfCols...)
		n.sources[i] = plan
		n.run.numColsPerSource[i] = len(srfCols)
	}

	n.run.rowBuffer = make(tree.Datums, len(n.columns))
	return &n, nil
}

type applySRFRun struct {
	rowBuffer        tree.Datums
	numColsPerSource []int
	done             []bool
}

func (n *funcScanNode) Next(params runParams) (bool, error) {
	colIdx := 0
	newValAvail := false
	for i, src := range n.sources {
		numCols := n.run.numColsPerSource[i]

		if !n.run.done[i] {
			// Check whether this source still has some values available.
			hasVals, err := src.Next(params)
			if err != nil {
				return false, err
			}
			n.run.done[i] = !hasVals

			if hasVals {
				// This source has values, use them.
				copy(n.run.rowBuffer[colIdx:colIdx+numCols], src.Values())
				newValAvail = true
			} else {
				// No values left. Fill the buffer with NULLs for future
				// results.
				for j := 0; j < numCols; j++ {
					n.run.rowBuffer[colIdx+j] = tree.DNull
				}
			}
		}

		// Advance to the next column group.
		colIdx += numCols
	}

	return newValAvail, nil
}

func (n *funcScanNode) Values() tree.Datums { return n.run.rowBuffer }

func (n *funcScanNode) Close(ctx context.Context) {
	for _, p := range n.sources {
		p.Close(ctx)
	}
}
