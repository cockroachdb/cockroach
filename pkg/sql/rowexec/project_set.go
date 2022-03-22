// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/errors"
)

// projectSetProcessor is the physical processor implementation of
// projectSetNode.
type projectSetProcessor struct {
	execinfra.ProcessorBase

	input execinfra.RowSource
	spec  *execinfrapb.ProjectSetSpec

	// exprHelpers are the constant-folded, type checked expressions specified
	// in the ROWS FROM syntax. This can contain many kinds of expressions
	// (anything that is "function-like" including COALESCE, NULLIF) not just
	// SRFs.
	exprHelpers []*execinfrapb.ExprHelper

	// funcs contains a valid pointer to a SRF FuncExpr for every entry
	// in `exprHelpers` that is actually a SRF function application.
	// The size of the slice is the same as `exprHelpers` though.
	funcs []*tree.FuncExpr

	// inputRowReady is set when there was a row of input data available
	// from the source.
	inputRowReady bool

	// RowBuffer will contain the current row of results.
	rowBuffer rowenc.EncDatumRow

	// gens contains the current "active" ValueGenerators for each entry
	// in `funcs`. They are initialized anew for every new row in the source.
	gens []tree.ValueGenerator

	// done indicates for each `Expr` whether the values produced by
	// either the SRF or the scalar expressions are fully consumed and
	// thus also whether NULLs should be emitted instead.
	done []bool

	cancelChecker cancelchecker.CancelChecker
}

var _ execinfra.Processor = &projectSetProcessor{}
var _ execinfra.RowSource = &projectSetProcessor{}
var _ execinfra.OpNode = &projectSetProcessor{}

const projectSetProcName = "projectSet"

func newProjectSetProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.ProjectSetSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*projectSetProcessor, error) {
	outputTypes := append(input.OutputTypes(), spec.GeneratedColumns...)
	ps := &projectSetProcessor{
		input:       input,
		spec:        spec,
		exprHelpers: make([]*execinfrapb.ExprHelper, len(spec.Exprs)),
		funcs:       make([]*tree.FuncExpr, len(spec.Exprs)),
		rowBuffer:   make(rowenc.EncDatumRow, len(outputTypes)),
		gens:        make([]tree.ValueGenerator, len(spec.Exprs)),
		done:        make([]bool, len(spec.Exprs)),
	}
	if err := ps.Init(
		ps,
		post,
		outputTypes,
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{ps.input},
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				ps.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}

	// Initialize exprHelpers.
	semaCtx := ps.FlowCtx.NewSemaContext(ps.EvalCtx.Txn)
	for i, expr := range ps.spec.Exprs {
		var helper execinfrapb.ExprHelper
		err := helper.Init(expr, ps.input.OutputTypes(), semaCtx, ps.EvalCtx)
		if err != nil {
			return nil, err
		}
		if tFunc, ok := helper.Expr.(*tree.FuncExpr); ok && tFunc.IsGeneratorApplication() {
			// Expr is a set-generating function.
			ps.funcs[i] = tFunc
		}
		ps.exprHelpers[i] = &helper
	}
	return ps, nil
}

// Start is part of the RowSource interface.
func (ps *projectSetProcessor) Start(ctx context.Context) {
	ctx = ps.StartInternal(ctx, projectSetProcName)
	ps.input.Start(ctx)
	ps.cancelChecker.Reset(ctx)
}

// nextInputRow returns the next row or metadata from ps.input. It also
// initializes the value generators for that row.
func (ps *projectSetProcessor) nextInputRow() (
	rowenc.EncDatumRow,
	*execinfrapb.ProducerMetadata,
	error,
) {
	row, meta := ps.input.Next()
	if row == nil {
		return nil, meta, nil
	}

	// Initialize a round of SRF generators or scalar values.
	for i := range ps.exprHelpers {
		if fn := ps.funcs[i]; fn != nil {
			// A set-generating function. Prepare its ValueGenerator.

			// Set ExprHelper.row so that we can use it as an IndexedVarContainer.
			ps.exprHelpers[i].Row = row

			ps.EvalCtx.IVarContainer = ps.exprHelpers[i]
			gen, err := fn.EvalArgsAndGetGenerator(ps.EvalCtx)
			if err != nil {
				return nil, nil, err
			}
			if gen == nil {
				gen = builtins.EmptyGenerator()
			}
			if err := gen.Start(ps.Ctx, ps.FlowCtx.Txn); err != nil {
				return nil, nil, err
			}
			ps.gens[i] = gen
		}
		ps.done[i] = false
	}

	return row, nil, nil
}

// nextGeneratorValues populates the row buffer with the next set of generated
// values. It returns true if any of the generators produce new values.
func (ps *projectSetProcessor) nextGeneratorValues() (newValAvail bool, err error) {
	colIdx := len(ps.input.OutputTypes())
	for i := range ps.exprHelpers {
		// Do we have a SRF?
		if gen := ps.gens[i]; gen != nil {
			// Yes. Is there still work to do for the current row?
			numCols := int(ps.spec.NumColsPerGen[i])
			if !ps.done[i] {
				// Yes; check whether this source still has some values available.
				hasVals, err := gen.Next(ps.Ctx)
				if err != nil {
					return false, err
				}
				if hasVals {
					// This source has values, use them.
					values, err := gen.Values()
					if err != nil {
						return false, err
					}
					for _, value := range values {
						ps.rowBuffer[colIdx] = ps.toEncDatum(value, colIdx)
						colIdx++
					}
					newValAvail = true
				} else {
					ps.done[i] = true
					// No values left. Fill the buffer with NULLs for future results.
					for j := 0; j < numCols; j++ {
						ps.rowBuffer[colIdx] = ps.toEncDatum(tree.DNull, colIdx)
						colIdx++
					}
				}
			} else {
				// Already done. Increment colIdx.
				colIdx += numCols
			}
		} else {
			// A simple scalar result.
			// Do we still need to produce the scalar value? (first row)
			if !ps.done[i] {
				// Yes. Produce it once, then indicate it's "done".
				value, err := ps.exprHelpers[i].Eval(ps.rowBuffer)
				if err != nil {
					return false, err
				}
				ps.rowBuffer[colIdx] = ps.toEncDatum(value, colIdx)
				colIdx++
				newValAvail = true
				ps.done[i] = true
			} else {
				// Ensure that every row after the first returns a NULL value.
				ps.rowBuffer[colIdx] = ps.toEncDatum(tree.DNull, colIdx)
				colIdx++
			}
		}
	}
	return newValAvail, nil
}

// Next is part of the RowSource interface.
func (ps *projectSetProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for ps.State == execinfra.StateRunning {
		if err := ps.cancelChecker.Check(); err != nil {
			ps.MoveToDraining(err)
			return nil, ps.DrainHelper()
		}

		// Start of a new row of input?
		if !ps.inputRowReady {
			// Read the row from the source.
			row, meta, err := ps.nextInputRow()
			if meta != nil {
				if meta.Err != nil {
					ps.MoveToDraining(nil /* err */)
				}
				return nil, meta
			}
			if err != nil {
				ps.MoveToDraining(err)
				return nil, ps.DrainHelper()
			}
			if row == nil {
				ps.MoveToDraining(nil /* err */)
				return nil, ps.DrainHelper()
			}

			// Keep the values for later.
			copy(ps.rowBuffer, row)
			ps.inputRowReady = true
		}

		// Try to find some data on the generator side.
		newValAvail, err := ps.nextGeneratorValues()
		if err != nil {
			ps.MoveToDraining(err)
			return nil, ps.DrainHelper()
		}
		if newValAvail {
			if outRow := ps.ProcessRowHelper(ps.rowBuffer); outRow != nil {
				return outRow, nil
			}
		} else {
			// The current batch of SRF values was exhausted. Advance
			// to the next input row.
			ps.inputRowReady = false
		}
	}
	return nil, ps.DrainHelper()
}

func (ps *projectSetProcessor) toEncDatum(d tree.Datum, colIdx int) rowenc.EncDatum {
	generatedColIdx := colIdx - len(ps.input.OutputTypes())
	ctyp := ps.spec.GeneratedColumns[generatedColIdx]
	return rowenc.DatumToEncDatum(ctyp, d)
}

func (ps *projectSetProcessor) close() {
	ps.InternalCloseEx(func() {
		for _, gen := range ps.gens {
			if gen != nil {
				gen.Close(ps.Ctx)
			}
		}
	})
}

// ConsumerClosed is part of the RowSource interface.
func (ps *projectSetProcessor) ConsumerClosed() {
	ps.close()
}

// ChildCount is part of the execinfra.OpNode interface.
func (ps *projectSetProcessor) ChildCount(verbose bool) int {
	if _, ok := ps.input.(execinfra.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (ps *projectSetProcessor) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		if n, ok := ps.input.(execinfra.OpNode); ok {
			return n
		}
		panic("input to projectSetProcessor is not an execinfra.OpNode")

	}
	panic(errors.AssertionFailedf("invalid index %d", nth))
}
