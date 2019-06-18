// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// projectSetNode zips through a list of generators for every row of
// the table source.
//
// Reminder, for context: the functional zip over iterators a,b,c
// returns tuples of values from a,b,c picked "simultaneously". NULLs
// are used when an iterator is "shorter" than another. For example:
//
//    zip([1,2,3], ['a','b']) = [(1,'a'), (2,'b'), (3, null)]
//
// In this context, projectSetNode corresponds to a relational
// operator project(R, a, b, c, ...) which, for each row in R,
// produces all the rows produced by zip(a, b, c, ...) with the values
// of R prefixed. Formally, this performs a lateral cross join of R
// with zip(a,b,c).
type projectSetNode struct {
	source     planNode
	sourceInfo *sqlbase.DataSourceInfo

	// ivarHelper is used to resolve names in correlated SRFs.
	ivarHelper tree.IndexedVarHelper

	// columns contains all the columns from the source, and then
	// the columns from the generators.
	columns sqlbase.ResultColumns

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

	// numColsPerGen indicates how many columns are produced by
	// each entry in `exprs`.
	numColsPerGen []int

	// props are the ordering, key props etc.
	props physicalProps

	run projectSetRun
}

// ProjectSet wraps a plan in a projectSetNode.
func (p *planner) ProjectSet(
	ctx context.Context,
	source planNode,
	sourceInfo *sqlbase.DataSourceInfo,
	errCtx string,
	sourceNames tree.TableNames,
	exprs ...tree.Expr,
) (planDataSource, error) {
	if len(exprs) == 0 {
		return planDataSource{}, errors.AssertionFailedf("ProjectSet invoked with no projected expression")
	}

	srcCols := sourceInfo.SourceColumns
	n := &projectSetNode{
		source:          source,
		sourceInfo:      sourceInfo,
		columns:         make(sqlbase.ResultColumns, 0, len(srcCols)+len(exprs)),
		numColsInSource: len(srcCols),
		exprs:           make(tree.TypedExprs, len(exprs)),
		funcs:           make([]*tree.FuncExpr, len(exprs)),
		numColsPerGen:   make([]int, len(exprs)),
		run: projectSetRun{
			gens: make([]tree.ValueGenerator, len(exprs)),
			done: make([]bool, len(exprs)),
		},
	}

	// The resulting plans produces at least every column of the
	// input. They appear first in the input so that any indexed vars
	// that referred to the original source stay valid.
	n.columns = append(n.columns, srcCols...)

	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer p.semaCtx.Properties.Restore(p.semaCtx.Properties)

	// Ensure there are no aggregate or window functions in the clause.
	p.semaCtx.Properties.Require("FROM",
		tree.RejectAggregates|tree.RejectWindowApplications|tree.RejectNestedGenerators)

	// Analyze the provided expressions.
	n.ivarHelper = tree.MakeIndexedVarHelper(n, len(srcCols))
	for i, expr := range exprs {
		normalized, err := p.analyzeExpr(
			ctx, expr, sqlbase.MultiSourceInfo{sourceInfo}, n.ivarHelper, types.Any, false, errCtx)
		if err != nil {
			return planDataSource{}, err
		}

		// Store it for later.
		n.exprs[i] = normalized

		// Now we need to set up the execution and the result columns
		// separately for SRF invocations and "simple" scalar expressions.

		if tFunc, ok := normalized.(*tree.FuncExpr); ok && tFunc.IsGeneratorApplication() {
			// Set-generating functions: generate_series() etc.
			fd, err := tFunc.Func.Resolve(p.semaCtx.SearchPath)
			if err != nil {
				return planDataSource{}, err
			}

			n.funcs[i] = tFunc
			n.numColsPerGen[i] = len(fd.ReturnLabels)

			typ := normalized.ResolvedType()
			if n.numColsPerGen[i] == 1 {
				// Single-column return type.
				n.columns = append(n.columns, sqlbase.ResultColumn{
					Name: fd.ReturnLabels[0],
					Typ:  typ,
				})
			} else {
				// Prepare the result columns. Use the tuple labels in the SRF's
				// return type as column labels.
				for j := range typ.TupleContents() {
					n.columns = append(n.columns, sqlbase.ResultColumn{
						Name: typ.TupleLabels()[j],
						Typ:  &typ.TupleContents()[j],
					})
				}
			}
		} else {
			// A simple non-generator expression.
			n.numColsPerGen[i] = 1

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

	var info *sqlbase.DataSourceInfo
	if sourceNames == nil {
		// No source names specified: we have the ROWS FROM syntax. There is no
		// source operand, so we can ignore the sourceInfo really.
		info = sqlbase.NewSourceInfoForSingleTable(sqlbase.AnonymousTable, n.columns)
	} else {
		// Some sources specified. We must keep the source info and also
		// add new source aliases for each column group.
		numAliasesInSource := len(sourceInfo.SourceAliases)
		info = &sqlbase.DataSourceInfo{
			SourceColumns: n.columns,
			SourceAliases: make(sqlbase.SourceAliases, numAliasesInSource+len(n.exprs)),
		}
		copy(info.SourceAliases, sourceInfo.SourceAliases)
		colIdx := n.numColsInSource
		for i := range n.exprs {
			nextFirstCol := colIdx + n.numColsPerGen[i]
			info.SourceAliases[numAliasesInSource+i] = sqlbase.SourceAlias{
				Name:      sourceNames[i],
				ColumnSet: sqlbase.FillColumnRange(colIdx, nextFirstCol-1),
			}
			colIdx = nextFirstCol
		}
	}

	return planDataSource{info: info, plan: n}, nil
}

func (n *projectSetNode) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	return n.run.rowBuffer[idx].Eval(ctx)
}

func (n *projectSetNode) IndexedVarResolvedType(idx int) *types.T {
	return n.columns[idx].Typ
}

func (n *projectSetNode) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return n.sourceInfo.NodeFormatter(idx)
}

type projectSetRun struct {
	// inputRowReady is set when there was a row of input data available
	// from the source.
	inputRowReady bool

	// rowBuffer will contain the current row of results.
	rowBuffer tree.Datums

	// gens contains the current "active" ValueGenerators for each entry
	// in `funcs`. They are initialized anew for every new row in the source.
	gens []tree.ValueGenerator

	// done indicates for each `expr` whether the values produced by
	// either the SRF or the scalar expressions are fully consumed and
	// thus also whether NULLs should be emitted instead.
	done []bool
}

func (n *projectSetNode) startExec(runParams) error {
	return nil
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
			colIdx := n.numColsInSource
			evalCtx := params.EvalContext()
			evalCtx.IVarContainer = n
			for i := range n.exprs {
				if fn := n.funcs[i]; fn != nil {
					// A set-generating function. Prepare its ValueGenerator.
					gen, err := fn.EvalArgsAndGetGenerator(evalCtx)
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
				colIdx += n.numColsPerGen[i]
			}

			// Mark the row ready for further iterations.
			n.run.inputRowReady = true
		}

		// Try to find some data on the generator side.
		colIdx := n.numColsInSource
		newValAvail := false
		for i := range n.exprs {
			numCols := n.numColsPerGen[i]

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
					var err error
					n.run.rowBuffer[colIdx], err = n.exprs[i].Eval(params.EvalContext())
					if err != nil {
						return false, err
					}
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

func (n *projectSetNode) computePhysicalProps() {
	// We can pass through properties because projectSetNode preserves
	// all input columns, and they come first.
	n.props = planPhysicalProps(n.source)
	// However any key in the source is destroyed because rows may repeat
	// multiple times.
	n.props.weakKeys = nil
}
