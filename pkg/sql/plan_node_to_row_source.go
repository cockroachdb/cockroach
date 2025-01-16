// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execreleasable"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/optional"
	"github.com/cockroachdb/errors"
)

type metadataForwarder interface {
	forwardMetadata(metadata *execinfrapb.ProducerMetadata)
}

type planNodeToRowSource struct {
	execinfra.ProcessorBase

	input execinfra.RowSource

	fastPath bool

	node        planNode
	params      runParams
	outputTypes []*types.T

	firstNotWrapped planNode

	// run time state machine values
	row rowenc.EncDatumRow

	contentionEventsListener  execstats.ContentionEventsListener
	tenantConsumptionListener execstats.TenantConsumptionListener
}

var _ execinfra.LocalProcessor = &planNodeToRowSource{}
var _ execreleasable.Releasable = &planNodeToRowSource{}
var _ execopnode.OpNode = &planNodeToRowSource{}

var planNodeToRowSourcePool = sync.Pool{
	New: func() interface{} {
		return &planNodeToRowSource{}
	},
}

func newPlanNodeToRowSource(
	source planNode, params runParams, fastPath bool, firstNotWrapped planNode,
) *planNodeToRowSource {
	p := planNodeToRowSourcePool.Get().(*planNodeToRowSource)
	*p = planNodeToRowSource{
		ProcessorBase:   p.ProcessorBase,
		fastPath:        fastPath,
		node:            source,
		params:          params,
		firstNotWrapped: firstNotWrapped,
		row:             p.row,
	}
	if fastPath {
		// If our node is a "fast path node", it means that we're set up to
		// just return a row count meaning we'll output a single row with a
		// single INT column.
		p.outputTypes = []*types.T{types.Int}
	} else {
		p.outputTypes = getTypesFromResultColumns(planColumns(source))
	}
	if p.row != nil && cap(p.row) >= len(p.outputTypes) {
		// In some cases we might have no output columns, so nil row would have
		// sufficient width, yet nil row is a special value, so we can only
		// reuse the old row if it's non-nil.
		p.row = p.row[:len(p.outputTypes)]
	} else {
		p.row = make(rowenc.EncDatumRow, len(p.outputTypes))
	}
	return p
}

// MustBeStreaming implements the execinfra.Processor interface.
func (p *planNodeToRowSource) MustBeStreaming() bool {
	switch p.node.(type) {
	case *hookFnNode, *cdcValuesNode:
		// hookFnNode is special because it might be blocked forever if we decide to
		// buffer its output.
		// cdcValuesNode is a node used by CDC that must stream data row-by-row, and
		// it may also block forever if the input is buffered.
		return true
	default:
		return false
	}
}

// Init implements the execinfra.LocalProcessor interface.
func (p *planNodeToRowSource) Init(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	post *execinfrapb.PostProcessSpec,
) error {
	if err := p.InitWithEvalCtx(
		ctx,
		p,
		post,
		p.outputTypes,
		flowCtx,
		flowCtx.EvalCtx,
		processorID,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{
			// Input to drain is added in SetInput.
			TrailingMetaCallback: p.trailingMetaCallback,
		},
	); err != nil {
		return err
	}
	if execstats.ShouldCollectStats(ctx, flowCtx.CollectStats) {
		p.ExecStatsForTrace = p.execStatsForTrace
	}
	return nil
}

// SetInput implements the LocalProcessor interface.
// input is the first upstream RowSource. When we're done executing, we need to
// drain this row source of its metadata in case the planNode tree we're
// wrapping returned an error, since planNodes don't know how to drain trailing
// metadata.
func (p *planNodeToRowSource) SetInput(ctx context.Context, input execinfra.RowSource) error {
	if p.firstNotWrapped == nil {
		// Short-circuit if we never set firstNotWrapped - indicating this planNode
		// tree had no DistSQL-plannable subtrees.
		return nil
	}
	p.input = input
	// Adding the input to drain ensures that the input will be properly closed
	// by this planNodeToRowSource. This is important since the
	// rowSourceToPlanNode created below is not responsible for that.
	p.AddInputToDrain(input)
	// Search the plan we're wrapping for firstNotWrapped, which is the planNode
	// that DistSQL planning resumed in. Replace that planNode with input,
	// wrapped as a planNode.
	return walkPlan(ctx, p.node, planObserver{
		replaceNode: func(ctx context.Context, nodeName string, plan planNode) (planNode, error) {
			if plan == p.firstNotWrapped {
				return newRowSourceToPlanNode(input, p, planColumns(p.firstNotWrapped), p.firstNotWrapped), nil
			}
			return nil, nil
		},
	})
}

func (p *planNodeToRowSource) Start(ctx context.Context) {
	ctx = p.StartInternal(ctx, nodeName(p.node), &p.contentionEventsListener, &p.tenantConsumptionListener)
	p.params.ctx = ctx
	// This starts all of the nodes below this node.
	if err := startExec(p.params, p.node); err != nil {
		p.MoveToDraining(err)
	}
}

func init() {
	colexec.IsFastPathNode = func(rs execinfra.RowSource) bool {
		p, ok := rs.(*planNodeToRowSource)
		return ok && p.fastPath
	}
}

func (p *planNodeToRowSource) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if p.State == execinfra.StateRunning && p.fastPath {
		var count int
		// If our node is a "fast path node", it means that we're set up to just
		// return a row count. So trigger the fast path and return the row count as
		// a row with a single column.
		fastPath, ok := p.node.(planNodeFastPath)

		if ok {
			var res bool
			if count, res = fastPath.FastPathResults(); res {
				if p.params.extendedEvalCtx.Tracing.Enabled() {
					log.VEvent(p.params.ctx, 2, "fast path completed")
				}
			} else {
				// Fall back to counting the rows.
				count = 0
				ok = false
			}
		}

		if !ok {
			// If we have no fast path to trigger, fall back to counting the rows
			// by Nexting our source until exhaustion.
			next, err := p.node.Next(p.params)
			for ; next; next, err = p.node.Next(p.params) {
				count++
			}
			if err != nil {
				p.MoveToDraining(err)
				return nil, p.DrainHelper()
			}
		}
		p.MoveToDraining(nil /* err */)
		// Return the row count the only way we can: as a single-column row with
		// the count inside.
		return rowenc.EncDatumRow{rowenc.EncDatum{Datum: tree.NewDInt(tree.DInt(count))}}, nil
	}

	for p.State == execinfra.StateRunning {
		valid, err := p.node.Next(p.params)
		if err != nil || !valid {
			p.MoveToDraining(err)
			return nil, p.DrainHelper()
		}

		for i, datum := range p.node.Values() {
			if datum != nil {
				p.row[i] = rowenc.DatumToEncDatum(p.outputTypes[i], datum)
			}
		}
		// ProcessRow here is required to deal with projections, which won't be
		// pushed into the wrapped plan.
		if outRow := p.ProcessRowHelper(p.row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, p.DrainHelper()
}

// forwardMetadata will be called by any upstream rowSourceToPlanNode processors
// that need to forward metadata to the end of the flow. They can't pass
// metadata through local processors, so they instead add the metadata to our
// trailing metadata and expect us to forward it further.
func (p *planNodeToRowSource) forwardMetadata(metadata *execinfrapb.ProducerMetadata) {
	p.ProcessorBase.AppendTrailingMeta(*metadata)
}

func (p *planNodeToRowSource) trailingMetaCallback() []execinfrapb.ProducerMetadata {
	var meta []execinfrapb.ProducerMetadata
	if p.InternalClose() {
		// Check if we're wrapping a mutation and emit the rows written metric
		// if so.
		if m, ok := p.node.(mutationPlanNode); ok {
			metrics := execinfrapb.GetMetricsMeta()
			metrics.RowsWritten = m.rowsWritten()
			meta = []execinfrapb.ProducerMetadata{{Metrics: metrics}}
		}
	}
	return meta
}

// execStatsForTrace implements ProcessorBase.ExecStatsForTrace.
func (p *planNodeToRowSource) execStatsForTrace() *execinfrapb.ComponentStats {
	// Propagate contention time and RUs from IO requests.
	if p.contentionEventsListener.GetContentionTime() == 0 && p.tenantConsumptionListener.GetConsumedRU() == 0 {
		return nil
	}
	return &execinfrapb.ComponentStats{
		KV: execinfrapb.KVStats{
			ContentionTime: optional.MakeTimeValue(p.contentionEventsListener.GetContentionTime()),
		},
		Exec: execinfrapb.ExecStats{
			ConsumedRU: optional.MakeUint(p.tenantConsumptionListener.GetConsumedRU()),
		},
	}
}

// Release releases this planNodeToRowSource back to the pool.
func (p *planNodeToRowSource) Release() {
	p.ProcessorBase.Reset()
	// Deeply reset the row.
	for i := range p.row {
		p.row[i] = rowenc.EncDatum{}
	}
	// Note that we don't reuse the outputTypes slice because it is exposed to
	// the outer physical planning code.
	*p = planNodeToRowSource{
		ProcessorBase: p.ProcessorBase,
		row:           p.row[:0],
	}
	planNodeToRowSourcePool.Put(p)
}

// ChildCount is part of the execopnode.OpNode interface.
func (p *planNodeToRowSource) ChildCount(verbose bool) int {
	if _, ok := p.input.(execopnode.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execopnode.OpNode interface.
func (p *planNodeToRowSource) Child(nth int, verbose bool) execopnode.OpNode {
	switch nth {
	case 0:
		if n, ok := p.input.(execopnode.OpNode); ok {
			return n
		}
		panic("input to planNodeToRowSource is not an execopnode.OpNode")
	default:
		panic(errors.AssertionFailedf("invalid index %d", nth))
	}
}
