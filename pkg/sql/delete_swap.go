// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

var deleteSwapNodePool = sync.Pool{
	New: func() interface{} {
		return &deleteSwapNode{}
	},
}

type deleteSwapNode struct {
	// Unlike insertFastPathNode, deleteSwapNode reads from input in order to
	// support projections, which are used by some DELETE statements.
	singleInputPlanNode

	// columns is set if this DELETE is returning any rows, to be
	// consumed by a renderNode upstream. This occurs when there is a
	// RETURNING clause with some scalar expressions.
	columns colinfo.ResultColumns

	run deleteRun
}

var _ mutationPlanNode = &deleteSwapNode{}

func (d *deleteSwapNode) startExec(params runParams) error {
	panic("deleteSwapNode cannot be run in local mode")
}

// Next implements the planNode interface.
func (d *deleteSwapNode) Next(_ runParams) (bool, error) {
	panic("deleteSwapNode cannot be run in local mode")
}

// Values implements the planNode interface.
func (d *deleteSwapNode) Values() tree.Datums {
	panic("deleteSwapNode cannot be run in local mode")
}

func (d *deleteSwapNode) Close(ctx context.Context) {
	d.input.Close(ctx)
	d.run.close(ctx)
	*d = deleteSwapNode{}
	deleteSwapNodePool.Put(d)
}

func (d *deleteSwapNode) rowsWritten() int64 {
	return d.run.rowsAffected()
}

func (d *deleteSwapNode) indexRowsWritten() int64 {
	return d.run.td.indexRowsWritten
}

func (d *deleteSwapNode) indexBytesWritten() int64 {
	// No bytes counted as written for a deletion.
	return 0
}

func (d *deleteSwapNode) returnsRowsAffected() bool {
	return !d.run.rowsNeeded
}

func (d *deleteSwapNode) kvCPUTime() int64 {
	return d.run.td.kvCPUTime
}

func (d *deleteSwapNode) enableAutoCommit() {
	d.run.td.enableAutoCommit()
}

// deleteSwapProcessor is a LocalProcessor that wraps deleteSwapNode execution logic.
type deleteSwapProcessor struct {
	execinfra.ProcessorBase

	input execinfra.RowSource
	node  *deleteSwapNode

	outputTypes []*types.T

	cancelChecker cancelchecker.CancelChecker
}

var _ execinfra.LocalProcessor = &deleteSwapProcessor{}
var _ execopnode.OpNode = &deleteSwapProcessor{}

// Init initializes the deleteSwapProcessor.
func (d *deleteSwapProcessor) Init(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	post *execinfrapb.PostProcessSpec,
) error {
	if execstats.ShouldCollectStats(ctx, flowCtx.CollectStats) {
		if flowCtx.Txn != nil {
			d.node.run.contentionEventsListener.Init(flowCtx.Txn.ID())
		}
		d.ExecStatsForTrace = d.execStatsForTrace
	}
	memMonitor := execinfra.NewMonitor(ctx, flowCtx.Mon, mon.MakeName("delete-swap-mem"))
	return d.InitWithEvalCtx(
		ctx, d, post, d.outputTypes, flowCtx, flowCtx.EvalCtx, processorID, memMonitor,
		execinfra.ProcStateOpts{
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				// IndexBytesWritten only tracks inserted bytes, so leave it out.
				metrics := execinfrapb.GetMetricsMeta()
				metrics.RowsWritten = d.node.run.rowsAffected()
				metrics.IndexRowsWritten = d.node.run.td.indexRowsWritten
				metrics.KVCPUTime = d.node.run.td.kvCPUTime
				meta := []execinfrapb.ProducerMetadata{{Metrics: metrics}}
				d.close()
				return meta
			},
		},
	)
}

// SetInput sets the input RowSource for the deleteSwapProcessor.
func (d *deleteSwapProcessor) SetInput(ctx context.Context, input execinfra.RowSource) error {
	if execstats.ShouldCollectStats(ctx, d.FlowCtx.CollectStats) {
		input = rowexec.NewInputStatCollector(input)
	}
	d.input = input
	d.AddInputToDrain(input)
	return nil
}

// Start begins execution of the deleteSwapProcessor.
func (d *deleteSwapProcessor) Start(ctx context.Context) {
	d.StartInternal(ctx, "deleteSwapProcessor",
		&d.node.run.contentionEventsListener, &d.node.run.tenantConsumptionListener,
	)
	d.cancelChecker.Reset(ctx, rowinfra.RowExecCancelCheckInterval)
	d.input.Start(ctx)
	d.node.run.traceKV = d.FlowCtx.TraceKV
	d.node.run.mustValidateOldPKValues = true
	d.node.run.init(d.Ctx(), d.FlowCtx.EvalCtx, d.FlowCtx.Mon, d.node.columns)
	if err := d.node.run.td.init(d.Ctx(), d.FlowCtx.Txn, d.FlowCtx.EvalCtx); err != nil {
		d.MoveToDraining(err)
		return
	}

	// Run the mutation to completion. DeleteSwap only processes one row, so no
	// need to loop.
	if err := d.processBatch(); err != nil {
		d.MoveToDraining(err)
	}
}

func (d *deleteSwapProcessor) processBatch() error {
	// Delete swap does everything in one batch. There should only be a single row
	// of input, to ensure the savepoint rollback below has the correct SQL
	// semantics.
	if err := d.cancelChecker.Check(); err != nil {
		return err
	}

	// Advance one individual row from input RowSource.
	inputRow, meta := d.input.Next()
	if meta != nil {
		if meta.Err != nil {
			return meta.Err
		}
		return nil
	}

	if inputRow != nil {
		// Convert EncDatumRow to tree.Datums.
		datumRow := make(tree.Datums, len(inputRow))
		err := rowenc.EncDatumRowToDatums(d.input.OutputTypes(), datumRow, inputRow, nil)
		if err != nil {
			return err
		}

		// Process the deletion of the current input row.
		if err := d.node.run.processSourceRow(d.Ctx(), datumRow); err != nil {
			return err
		}

		// Verify that there was only a single row of input.
		inputRow, meta = d.input.Next()
		if meta != nil && meta.Err != nil {
			return meta.Err
		}
		if inputRow != nil {
			return errors.AssertionFailedf("expected only 1 row as input to delete swap")
		}
	}

	// Delete swap works by optimistically modifying every index in the same
	// batch. If the row does not actually exist, the write to the primary index
	// will fail with ConditionFailedError, but writes to some secondary indexes
	// might succeed. We use a savepoint here to undo those writes.
	sp, err := d.node.run.td.createSavepoint(d.Ctx())
	if err != nil {
		return err
	}

	d.node.run.td.setRowsWrittenLimit(d.FlowCtx.EvalCtx.SessionData())
	if err := d.node.run.td.finalize(d.Ctx()); err != nil {
		// If this was a ConditionFailedError, it means the row did not exist in the
		// primary index. We must roll back to the savepoint above to undo writes to
		// all secondary indexes.
		if condErr := (*kvpb.ConditionFailedError)(nil); errors.As(err, &condErr) {
			if err := d.node.run.td.rollbackToSavepoint(d.Ctx(), sp); err != nil {
				return err
			}
			return nil
		}
		return err
	}

	// Possibly initiate a run of CREATE STATISTICS.
	d.FlowCtx.Cfg.StatsRefresher.NotifyMutation(d.Ctx(), d.node.run.td.tableDesc(), int(d.node.run.rowsAffected()))

	return nil
}

// Next implements the RowSource interface.
func (d *deleteSwapProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if d.State != execinfra.StateRunning {
		return nil, d.DrainHelper()
	}

	// Return next row from accumulated results.
	var err error
	for d.node.run.next() {
		datumRow := d.node.run.values()
		encRow := make(rowenc.EncDatumRow, len(datumRow))
		for i, datum := range datumRow {
			encRow[i], err = rowenc.DatumToEncDatum(d.outputTypes[i], datum)
			if err != nil {
				d.MoveToDraining(err)
				return nil, d.DrainHelper()
			}
		}
		if outRow := d.ProcessRowHelper(encRow); outRow != nil {
			return outRow, nil
		}
	}

	// No more rows to return.
	d.MoveToDraining(nil)
	return nil, d.DrainHelper()
}

func (d *deleteSwapProcessor) close() {
	if d.InternalClose() {
		d.node.run.close(d.Ctx())
		d.MemMonitor.Stop(d.Ctx())
	}
}

// ConsumerClosed implements the RowSource interface.
func (d *deleteSwapProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	d.close()
}

// ChildCount is part of the execopnode.OpNode interface.
func (d *deleteSwapProcessor) ChildCount(verbose bool) int {
	if _, ok := d.input.(execopnode.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execopnode.OpNode interface.
func (d *deleteSwapProcessor) Child(nth int, verbose bool) execopnode.OpNode {
	if nth == 0 {
		if n, ok := d.input.(execopnode.OpNode); ok {
			return n
		}
		panic("input to deleteSwapProcessor is not an execopnode.OpNode")
	}
	panic(errors.AssertionFailedf("invalid index %d", nth))
}

// execStatsForTrace implements ProcessorBase.ExecStatsForTrace.
func (d *deleteSwapProcessor) execStatsForTrace() *execinfrapb.ComponentStats {
	is, ok := rowexec.GetInputStats(d.input)
	if !ok {
		return nil
	}
	ret := &execinfrapb.ComponentStats{
		Inputs: []execinfrapb.InputStats{is},
		Output: d.OutputHelper.Stats(),
	}
	d.node.run.populateExecStatsForTrace(ret)
	return ret
}
