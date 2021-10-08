// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/errors"
)

// filtererProcessor is a processor that filters input rows according to a
// boolean expression.
type filtererProcessor struct {
	execinfra.ProcessorBase
	input  execinfra.RowSource
	filter *execinfrapb.ExprHelper
}

var _ execinfra.Processor = &filtererProcessor{}
var _ execinfra.RowSource = &filtererProcessor{}
var _ execinfra.OpNode = &filtererProcessor{}

const filtererProcName = "filterer"

func newFiltererProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.FiltererSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*filtererProcessor, error) {
	f := &filtererProcessor{input: input}
	types := input.OutputTypes()
	if err := f.Init(
		f,
		post,
		types,
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{InputsToDrain: []execinfra.RowSource{f.input}},
	); err != nil {
		return nil, err
	}

	f.filter = &execinfrapb.ExprHelper{}
	if err := f.filter.Init(spec.Filter, types, &f.SemaCtx, f.EvalCtx); err != nil {
		return nil, err
	}

	ctx := flowCtx.EvalCtx.Ctx()
	if execinfra.ShouldCollectStats(ctx, flowCtx) {
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
		passes, err := f.filter.EvalFilter(row)
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

// ChildCount is part of the execinfra.OpNode interface.
func (f *filtererProcessor) ChildCount(bool) int {
	if _, ok := f.input.(execinfra.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (f *filtererProcessor) Child(nth int, _ bool) execinfra.OpNode {
	if nth == 0 {
		if n, ok := f.input.(execinfra.OpNode); ok {
			return n
		}
		panic("input to filterer is not an execinfra.OpNode")
	}
	panic(errors.AssertionFailedf("invalid index %d", nth))
}
