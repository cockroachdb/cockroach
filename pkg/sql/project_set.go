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

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// projectSetNode zips through a list of generators for every row of
// the table source. It can also operate on non-generator
// expressions, in which case a single row with that expression's
// results is produced.
type projectSetNode struct {
	source  planNode
	columns sqlbase.ResultColumns

	run projectSetRun
}

// ProjectSet wraps a plan in a projectSetNode.
func (p *planner) ProjectSet(
	ctx context.Context, source planNode, errCtx string, exprs ...tree.Expr,
) (planNode, error) {
	if len(exprs) == 0 {
		return nil, pgerror.NewErrorf(pgerror.CodeInternalError,
			"programming error: ProjectSet invoked with no projected expression")
	}

	srcCols := planColumns(source)
	n := projectSetNode{
		source:  source,
		columns: make(sqlbase.ResultColumns, 0, len(srcCols)+len(exprs)),
		run: projectSetRun{
			numColsInSource: len(srcCols),
			exprs:           make(tree.TypedExprs, len(exprs)),
			funcs:           make([]*tree.FuncExpr, len(exprs)),
			gens:            make([]tree.ValueGenerator, len(exprs)),
			numColsPerGen:   make([]int, len(exprs)),
			done:            make([]bool, len(exprs)),
		},
	}

	// The resulting plans produces at least every column of the
	// input. They appear first in the input so that any indexed vars
	// that refered to the original source stay valid.
	copy(n.columns, srcCols)

	// Analyze the provided expressions.
	for i, expr := range exprs {
		if err := p.txCtx.AssertNoAggregationOrWindowing(
			expr, errCtx, p.SessionData().SearchPath); err != nil {
			return nil, err
		}

		normalized, err := p.analyzeExpr(
			ctx, expr, sqlbase.MultiSourceInfo{}, tree.IndexedVarHelper{}, types.Any, false, errCtx)
		if err != nil {
			return nil, err
		}

		// Store it for later.
		n.run.exprs[i] = normalized

		// Now we need to set up the execution and the result columns
		// separately for SRF invocations and "simple" scalar expressions.

		if tFunc, ok := normalized.(*tree.FuncExpr); ok && tFunc.IsGeneratorApplication() {
			// Set-generating functions: generate_series() etc.
			tType := normalized.ResolvedType().(types.TTuple)
			n.run.funcs[i] = tFunc
			n.run.numColsPerGen[i] = len(tType.Types)

			// Prepare the result columns. Use the tuple labels in the SRF's
			// return type as column labels.
			for j := range tType.Types {
				n.columns = append(n.columns, sqlbase.ResultColumn{
					Name: tType.Labels[j],
					Typ:  tType.Types[j],
				})
			}
		} else {
			// A simple non-generator expression.
			n.run.numColsPerGen[i] = 1

			// There is just one result column.
			// TODO(knz): until #26236 is resolved, make a best effort at guessing
			// suitable column names.
			var colName string
			if origFunc, ok := expr.(*tree.FuncExpr); ok {
				colName = origFunc.Func.String()
			} else {
				colName = expr.String()
			}
			n.columns = append(n.columns, sqlbase.ResultColumn{
				Name: colName,
				Typ:  normalized.ResolvedType(),
			})
		}
	}

	// Pre-allocate the result buffer to conserve memory.
	n.run.rowBuffer = make(tree.Datums, len(n.columns))

	return &n, nil
}

type projectSetRun struct {
	// inputRowReady is set when there was a row of input data available
	// from the source.
	inputRowReady bool

	// rowBuffer will contain the current row of results.
	rowBuffer tree.Datums

	// numColsInSource is the number of columns in the source plan, i.e.
	// the number of columns at the beginning of rowBuffer that do not
	// contain SRF results.
	numColsInSource int

	// exprs are the constant-folded, type checked expressions specified
	// in the ROWS FROM syntax. This can contain many kinds of expressions
	// (anything that is "function-like" including COALESCE, NULLIF) not just
	// SRFs.
	exprs tree.TypedExprs

	// funcs contains a valid pointer to a SRF FuncExpr for every entry
	// in `exprs` that is actually a SRF function application.
	// The size of the slice is the same as `exprs` though.
	funcs []*tree.FuncExpr

	// gens contains the current "active" ValueGenerators for each entry
	// in `funcs`. They are initialized anew for every new row in the source.
	gens []tree.ValueGenerator

	// numColsPerGen indicates how many columns in rowBuffer are populated by
	// each entry in `gens`.
	numColsPerGen []int

	// done indicates for each `expr` whether the values produced by
	// either the SRF or the scalar expresions are fully consumed and
	// thus also whether NULLs should be emitted instead.
	done []bool
}

func (n *projectSetNode) Next(params runParams) (bool, error) {
	for {
		// If there's a cancellation request or a timeout, process it here.
		if err := params.p.cancelChecker.Check(); err != nil {
			return false, err
		}

		// Start of a new row of input?
		if !n.run.inputRowReady {
			// Read the row from the source.
			hasRow, err := n.source.Next(params)
			if err != nil || !hasRow {
				return false, err
			}

			// Keep the values for later.
			copy(n.run.rowBuffer, n.source.Values())

			// Initialize a round of SRF generators or scalar values.
			colIdx := n.run.numColsInSource
			for i := range n.run.exprs {
				fn := n.run.funcs[i]
				if fn == nil {
					// This is a regular scalar expression. Evaluate it just once, store
					// the result in rowBuffer. The result will be produced multiple times.
					n.run.rowBuffer[colIdx], err = n.run.exprs[i].Eval(params.EvalContext())
					if err != nil {
						return false, err
					}
				} else {
					// A set-generating function. Prepare its ValueGenerator.
					gen, err := fn.EvalArgsAndGetGenerator(params.EvalContext())
					if err != nil {
						return false, err
					}
					if gen == nil {
						gen = builtins.EmptyGenerator()
					}
					if err := gen.Start(); err != nil {
						return false, err
					}
					n.run.gens[i] = gen
				}
				n.run.done[i] = false
				colIdx += n.run.numColsPerGen[i]
			}

			// Mark the row ready for further iterations.
			n.run.inputRowReady = true
		}

		// Try to find some data on the generator side.
		colIdx := n.run.numColsInSource
		newValAvail := false
		for i := range n.run.exprs {
			numCols := n.run.numColsPerGen[i]

			// Do we have a SRF?
			if gen := n.run.gens[i]; gen != nil {
				// Yes. Is there still work to do for the current row?
				if !n.run.done[i] {
					// Yes; heck whether this source still has some values available.
					hasVals, err := gen.Next()
					if err != nil {
						return false, err
					}
					if hasVals {
						// This source has values, use them.
						copy(n.run.rowBuffer[colIdx:colIdx+numCols], gen.Values())
						newValAvail = true
					} else {
						n.run.done[i] = true
						// No values left. Fill the buffer with NULLs for future
						// results.
						for j := 0; j < numCols; j++ {
							n.run.rowBuffer[colIdx+j] = tree.DNull
						}
					}
				}
			} else {
				// A simple scalar result.
				// Do we still need to produce the scalar value? (first row)
				if !n.run.done[i] {
					// Yes. Produce it once, then indicate it's "done".
					newValAvail = true
					n.run.done[i] = true
				} else {
					// Ensure that every row after the first returns a NULL value.
					n.run.rowBuffer[colIdx] = tree.DNull
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
		if gen != nil {
			gen.Close()
		}
	}
}
