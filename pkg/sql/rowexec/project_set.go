// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/errors"
)

// projectSetProcessor is the physical processor implementation of
// projectSetNode.
type projectSetProcessor struct {
	execinfra.ProcessorBase

	evalCtx *eval.Context
	input   execinfra.RowSource
	spec    *execinfrapb.ProjectSetSpec

	// eh contains the type-checked expression specified in the ROWS FROM
	// syntax. It can contain many kinds of expressions (anything that is
	// "function-like" including COALESCE, NULLIF), not just SRFs.
	eh execinfrapb.MultiExprHelper

	// funcs contains a valid pointer to a SRF FuncExpr for every entry
	// in `exprHelpers` that is actually a SRF function application.
	// The size of the slice is the same as `exprHelpers` though.
	funcs []tree.TypedExpr

	// mustBeStreaming indicates whether at least one function in funcs is of
	// "streaming" nature.
	mustBeStreaming bool

	// inputRowReady is set when there was a row of input data available
	// from the source.
	inputRowReady bool

	// RowBuffer will contain the current row of results.
	rowBuffer rowenc.EncDatumRow

	// gens contains the current "active" eval.ValueGenerators for each entry
	// in `funcs`. They are initialized anew for every new row in the source.
	gens []eval.ValueGenerator

	// done indicates for each `Expr` whether the values produced by
	// either the SRF or the scalar expressions are fully consumed and
	// thus also whether NULLs should be emitted instead.
	done []bool

	cancelChecker cancelchecker.CancelChecker
}

var _ execinfra.Processor = &projectSetProcessor{}
var _ execinfra.RowSource = &projectSetProcessor{}
var _ execopnode.OpNode = &projectSetProcessor{}

const projectSetProcName = "projectSet"

func newProjectSetProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.ProjectSetSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
) (*projectSetProcessor, error) {
	outputTypes := append(input.OutputTypes(), spec.GeneratedColumns...)
	ps := &projectSetProcessor{
		// We'll be mutating the eval context, so we always need a copy.
		evalCtx:   flowCtx.NewEvalCtx(),
		input:     input,
		spec:      spec,
		funcs:     make([]tree.TypedExpr, len(spec.Exprs)),
		rowBuffer: make(rowenc.EncDatumRow, len(outputTypes)),
		gens:      make([]eval.ValueGenerator, len(spec.Exprs)),
		done:      make([]bool, len(spec.Exprs)),
	}
	if err := ps.InitWithEvalCtx(
		ctx,
		ps,
		post,
		outputTypes,
		flowCtx,
		ps.evalCtx,
		processorID,
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

	// Initialize exprHelper.
	semaCtx := ps.FlowCtx.NewSemaContext(ps.FlowCtx.Txn)
	err := ps.eh.Init(ctx, len(ps.spec.Exprs), ps.input.OutputTypes(), semaCtx, ps.evalCtx)
	if err != nil {
		return nil, err
	}
	for i, expr := range ps.spec.Exprs {
		if err = ps.eh.AddExpr(ctx, expr, i); err != nil {
			return nil, err
		}
		texpr := ps.eh.Expr(i)
		if tFunc, ok := texpr.(*tree.FuncExpr); ok && tFunc.IsGeneratorClass() {
			// Expr is a set-generating function.
			ps.funcs[i] = tFunc
			ps.mustBeStreaming = ps.mustBeStreaming || tFunc.IsVectorizeStreaming()
		}
		if tRoutine, ok := texpr.(*tree.RoutineExpr); ok {
			// A routine in the context of a project-set is a set-returning
			// routine.
			ps.funcs[i] = tRoutine
		}
	}
	return ps, nil
}

// MustBeStreaming implements the execinfra.Processor interface.
func (ps *projectSetProcessor) MustBeStreaming() bool {
	return ps.mustBeStreaming
}

// Start is part of the RowSource interface.
func (ps *projectSetProcessor) Start(ctx context.Context) {
	ctx = ps.StartInternal(ctx, projectSetProcName)
	ps.input.Start(ctx)
	ps.cancelChecker.Reset(ctx, rowinfra.RowExecCancelCheckInterval)
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
	// Set expression helper row so that we can use it as an
	// IndexedVarContainer.
	ps.eh.SetRow(row)
	ps.evalCtx.IVarContainer = ps.eh.IVarContainer()

	// Initialize a round of SRF generators or scalar values.
	for i, n := 0, ps.eh.ExprCount(); i < n; i++ {
		if fn := ps.funcs[i]; fn != nil {
			// A set-generating function. Prepare its eval.ValueGenerator.

			// First, make sure to close its eval.ValueGenerator from the
			// previous input row (if it exists).
			if ps.gens[i] != nil {
				ps.gens[i].Close(ps.Ctx())
				ps.gens[i] = nil
			}

			var gen eval.ValueGenerator
			var err error
			switch t := fn.(type) {
			case *tree.FuncExpr:
				gen, err = eval.GetFuncGenerator(ps.Ctx(), ps.evalCtx, t)
			case *tree.RoutineExpr:
				gen, err = eval.GetRoutineGenerator(ps.Ctx(), ps.evalCtx, t)
				if err == nil && gen == nil && t.MultiColOutput && !t.Generator {
					// If the routine will return multiple output columns, we expect the
					// routine to return nulls for each column type instead of no rows, so
					// we can't use the empty generator. Set-returning routines
					// (i.e., Generators), have different behavior and are handled
					// separately.
					gen, err = builtins.NullGenerator(t.ResolvedType())
				}
			default:
				return nil, nil, errors.AssertionFailedf("unexpected expression in project-set: %T", fn)
			}
			if err != nil {
				return nil, nil, err
			}
			if gen == nil {
				gen = builtins.EmptyGenerator()
			}
			if aliasSetter, ok := gen.(eval.AliasAwareValueGenerator); ok {
				if err := aliasSetter.SetAlias(ps.spec.GeneratedColumns, ps.spec.GeneratedColumnLabels); err != nil {
					return nil, nil, err
				}
			}
			// Store the generator before Start so that it'll be closed even if
			// Start returns an error.
			ps.gens[i] = gen
			if err := gen.Start(ps.Ctx(), ps.FlowCtx.Txn); err != nil {
				return nil, nil, err
			}
		}
		ps.done[i] = false
	}

	return row, nil, nil
}

// nextGeneratorValues populates the row buffer with the next set of generated
// values. It returns true if any of the generators produce new values.
func (ps *projectSetProcessor) nextGeneratorValues() (newValAvail bool, err error) {
	colIdx := len(ps.input.OutputTypes())
	for i, n := 0, ps.eh.ExprCount(); i < n; i++ {
		// Do we have a SRF?
		if gen := ps.gens[i]; gen != nil {
			// Yes. Is there still work to do for the current row?
			numCols := int(ps.spec.NumColsPerGen[i])
			if !ps.done[i] {
				// Yes; check whether this source still has some values available.
				hasVals, err := gen.Next(ps.Ctx())
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
						ps.rowBuffer[colIdx], err = ps.toEncDatum(value, colIdx)
						if err != nil {
							return false, err
						}
						colIdx++
					}
					newValAvail = true
				} else {
					ps.done[i] = true
					// No values left. Fill the buffer with NULLs for future results.
					for j := 0; j < numCols; j++ {
						ps.rowBuffer[colIdx], err = ps.toEncDatum(tree.DNull, colIdx)
						if err != nil {
							return false, err
						}
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
				value, err := ps.eh.EvalExpr(ps.Ctx(), i, ps.rowBuffer)
				if err != nil {
					return false, err
				}
				ps.rowBuffer[colIdx], err = ps.toEncDatum(value, colIdx)
				if err != nil {
					return false, err
				}
				colIdx++
				newValAvail = true
				ps.done[i] = true
			} else {
				// Ensure that every row after the first returns a NULL value.
				ps.rowBuffer[colIdx], err = ps.toEncDatum(tree.DNull, colIdx)
				if err != nil {
					return false, err
				}
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

func (ps *projectSetProcessor) toEncDatum(d tree.Datum, colIdx int) (rowenc.EncDatum, error) {
	generatedColIdx := colIdx - len(ps.input.OutputTypes())
	ctyp := ps.spec.GeneratedColumns[generatedColIdx]
	return rowenc.DatumToEncDatumEx(ctyp, d)
}

func (ps *projectSetProcessor) close() {
	if ps.Closed {
		return
	}
	// Close all generator functions before the context is replaced in
	// InternalClose().
	for i, gen := range ps.gens {
		if gen != nil {
			gen.Close(ps.Ctx())
			ps.gens[i] = nil
		}
	}
	ps.InternalClose()
}

// ConsumerClosed is part of the RowSource interface.
func (ps *projectSetProcessor) ConsumerClosed() {
	ps.close()
}

// ChildCount is part of the execopnode.OpNode interface.
func (ps *projectSetProcessor) ChildCount(verbose bool) int {
	if _, ok := ps.input.(execopnode.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execopnode.OpNode interface.
func (ps *projectSetProcessor) Child(nth int, verbose bool) execopnode.OpNode {
	if nth == 0 {
		if n, ok := ps.input.(execopnode.OpNode); ok {
			return n
		}
		panic("input to projectSetProcessor is not an execopnode.OpNode")

	}
	panic(errors.AssertionFailedf("invalid index %d", nth))
}
