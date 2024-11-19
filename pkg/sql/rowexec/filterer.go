// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/errors"
)

// filtererProcessor is a processor that filters input rows according to a
// boolean expression.
type filtererProcessor struct {
	execinfra.ProcessorBase
	evalCtx *eval.Context
	input   execinfra.RowSource
	filter  execinfrapb.ExprHelper
}

var _ execinfra.Processor = &filtererProcessor{}
var _ execinfra.RowSource = &filtererProcessor{}
var _ execopnode.OpNode = &filtererProcessor{}

const filtererProcName = "filterer"

func newFiltererProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.FiltererSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
) (*filtererProcessor, error) {
	f := &filtererProcessor{
		// Make a copy of the eval context since we're going to pass it to the
		// ExprHelper later (which might modify it).
		evalCtx: flowCtx.NewEvalCtx(),
		input:   input,
	}
	types := input.OutputTypes()
	if err := f.InitWithEvalCtx(
		ctx,
		f,
		post,
		types,
		flowCtx,
		f.evalCtx,
		processorID,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{InputsToDrain: []execinfra.RowSource{f.input}},
	); err != nil {
		return nil, err
	}

	if err := f.filter.Init(ctx, spec.Filter, types, &f.SemaCtx, f.evalCtx); err != nil {
		return nil, err
	}

	if execstats.ShouldCollectStats(ctx, flowCtx.CollectStats) {
		f.input = newInputStatCollector(f.input)
		f.ExecStatsForTrace = f.execStatsForTrace
	}
	return f, nil
}

// Start is part of the RowSource interface.
func (f *filtererProcessor) Start(ctx context.Context) {
	ctx = f.StartInternal(ctx, filtererProcName)
	f.input.Start(ctx)
}

// Next is part of the RowSource interface.
func (f *filtererProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for f.State == execinfra.StateRunning {
		row, meta := f.input.Next()

		if meta != nil {
			if meta.Err != nil {
				f.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			f.MoveToDraining(nil /* err */)
			break
		}

		// Perform the actual filtering.
		passes, err := f.filter.EvalFilter(f.Ctx(), row)
		if err != nil {
			f.MoveToDraining(err)
			break
		}
		if passes {
			if outRow := f.ProcessRowHelper(row); outRow != nil {
				return outRow, nil
			}
		}
	}
	return nil, f.DrainHelper()
}

// execStatsForTrace implements ProcessorBase.ExecStatsForTrace.
func (f *filtererProcessor) execStatsForTrace() *execinfrapb.ComponentStats {
	is, ok := getInputStats(f.input)
	if !ok {
		return nil
	}
	return &execinfrapb.ComponentStats{
		Inputs: []execinfrapb.InputStats{is},
		Output: f.OutputHelper.Stats(),
	}
}

// ChildCount is part of the execopnode.OpNode interface.
func (f *filtererProcessor) ChildCount(bool) int {
	if _, ok := f.input.(execopnode.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execopnode.OpNode interface.
func (f *filtererProcessor) Child(nth int, _ bool) execopnode.OpNode {
	if nth == 0 {
		if n, ok := f.input.(execopnode.OpNode); ok {
			return n
		}
		panic("input to filterer is not an execopnode.OpNode")
	}
	panic(errors.AssertionFailedf("invalid index %d", nth))
}
