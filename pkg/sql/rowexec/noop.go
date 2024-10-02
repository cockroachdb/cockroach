// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execreleasable"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
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
var _ execreleasable.Releasable = &noopProcessor{}
var _ execopnode.OpNode = &noopProcessor{}

const noopProcName = "noop"

var noopPool = sync.Pool{
	New: func() interface{} {
		return &noopProcessor{}
	},
}

func newNoopProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
) (*noopProcessor, error) {
	n := noopPool.Get().(*noopProcessor)
	n.input = input
	if err := n.Init(
		ctx,
		n,
		post,
		input.OutputTypes(),
		flowCtx,
		processorID,
		nil, /* memMonitor */
		// We append input to inputs to drain below in order to reuse the same
		// underlying slice from the pooled noopProcessor.
		execinfra.ProcStateOpts{},
	); err != nil {
		return nil, err
	}
	n.AddInputToDrain(n.input)
	if execstats.ShouldCollectStats(ctx, flowCtx.CollectStats) {
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

// Release releases this noopProcessor back to the pool.
func (n *noopProcessor) Release() {
	n.ProcessorBase.Reset()
	*n = noopProcessor{ProcessorBase: n.ProcessorBase}
	noopPool.Put(n)
}

// ChildCount is part of the execopnode.OpNode interface.
func (n *noopProcessor) ChildCount(bool) int {
	if _, ok := n.input.(execopnode.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execopnode.OpNode interface.
func (n *noopProcessor) Child(nth int, _ bool) execopnode.OpNode {
	if nth == 0 {
		if n, ok := n.input.(execopnode.OpNode); ok {
			return n
		}
		panic("input to noop is not an execopnode.OpNode")
	}
	panic(errors.AssertionFailedf("invalid index %d", nth))
}
