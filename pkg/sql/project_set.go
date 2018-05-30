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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// projectSetNode zips through SRFs for every row of the table source.
type projectSetNode struct {
	source planNode

	numColsInSource int
	funcs           []*tree.FuncExpr
	columns         sqlbase.ResultColumns

	run projectSetRun
}

type projectSetRun struct {
	rowBuffer tree.Datums

	// inputRowReady is set when there was a row of input data available.
	inputRowReady bool

	gens          []tree.ValueGenerator
	numColsPerGen []int
	genDone       []bool
}

func (n *projectSetNode) Next(params runParams) (bool, error) {
	for {
		if !n.run.inputRowReady {
			hasRow, err := n.source.Next(params)
			if err != nil || !hasRow {
				return false, err
			}

			// Keep the values for later.
			copy(n.run.rowBuffer, n.source.Values())

			// Initialize a round of SRF generators.
			for i, fn := range n.funcs {
				expr, err := fn.Eval(params.EvalContext())
				if err != nil {
					return false, err
				}
				var tb *tree.DTable
				if expr == tree.DNull {
					tb = builtins.EmptyDTable()
				} else {
					tb = expr.(*tree.DTable)
				}

				gen := tb.ValueGenerator
				if err := gen.Start(); err != nil {
					return false, err
				}
				n.run.gens[i] = gen
			}

			// Mark the row ready for further iterations.
			n.run.inputRowReady = true
		}

		// Try to find some data on the generator side.
		colIdx := n.numColsInSource
		newValAvail := false
		for i, gen := range n.run.gens {
			numCols := n.run.numColsPerGen[i]

			if !n.run.genDone[i] {
				// Check whether this source still has some values available.
				hasVals, err := gen.Next()
				if err != nil {
					return false, err
				}
				n.run.genDone[i] = !hasVals

				if hasVals {
					// This source has values, use them.
					copy(n.run.rowBuffer[colIdx:colIdx+numCols], gen.Values())
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

		if newValAvail {
			return true, nil
		}

		// The current batch of SRF values was exhausted. Advance
		// to the next input row.
		n.run.inputRowReady = false
	}
}

func (n *projectSetNode) Values() tree.Datums { return n.run.rowBuffer }

func (n *projectSetNode) Close(ctx context.Context) {
	n.source.Close(ctx)
	for _, gen := range n.run.gens {
		gen.Close()
	}
}
