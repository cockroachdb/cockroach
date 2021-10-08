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
	"github.com/cockroachdb/errors"
)

// noopProcessor is a processor that simply passes rows through from the
// synchronizer to the post-processing stage. It can be useful for its
// post-processing or in the last stage of a computation, where we may only
// need the synchronizer to join streams.
type noopProcessor struct {
	execinfra.ProcessorBase
	input execinfra.RowSource
}

var _ execinfra.Processor = &noopProcessor{}
var _ execinfra.RowSource = &noopProcessor{}
var _ execinfra.OpNode = &noopProcessor{}

const noopProcName = "noop"

func newNoopProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*noopProcessor, error) {
	n := &noopProcessor{input: input}
	if err := n.Init(
		n,
		post,
		input.OutputTypes(),
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{InputsToDrain: []execinfra.RowSource{n.input}},
	); err != nil {
		return nil, err
	}
	ctx := flowCtx.EvalCtx.Ctx()
	if execinfra.ShouldCollectStats(ctx, flowCtx) {
		n.input = newInputStatCollector(n.input)
		n.ExecStatsForTrace = n.execStatsForTrace
	}
	return n, nil
}

// Start is part of the RowSource interface.
func (n *noopProcessor) Start(ctx context.Context) {
	ctx = n.StartInternal(ctx, noopProcName)
	n.input.Start(ctx)
}

// Next is part of the RowSource interface.
func (n *noopProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for n.State == execinfra.StateRunning {
		row, meta := n.input.Next()

		if meta != nil {
			if meta.Err != nil {
				n.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			n.MoveToDraining(nil /* err */)
			break
		}

		if outRow := n.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, n.DrainHelper()
}

// execStatsForTrace implements ProcessorBase.ExecStatsForTrace.
func (n *noopProcessor) execStatsForTrace() *execinfrapb.ComponentStats {
	is, ok := getInputStats(n.input)
	if !ok {
		return nil
	}
	return &execinfrapb.ComponentStats{
		Inputs: []execinfrapb.InputStats{is},
		Output: n.OutputHelper.Stats(),
	}
}

// ChildCount is part of the execinfra.OpNode interface.
func (n *noopProcessor) ChildCount(bool) int {
	if _, ok := n.input.(execinfra.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (n *noopProcessor) Child(nth int, _ bool) execinfra.OpNode {
	if nth == 0 {
		if n, ok := n.input.(execinfra.OpNode); ok {
			return n
		}
		panic("input to noop is not an execinfra.OpNode")
	}
	panic(errors.AssertionFailedf("invalid index %d", nth))
}
