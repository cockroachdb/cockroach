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

var updateSwapNodePool = sync.Pool{
	New: func() interface{} {
		return &updateSwapNode{}
	},
}

type updateSwapNode struct {
	// Unlike insertFastPathNode, updateSwapNode reads from input in order to
	// support projections, which are used by most UPDATE statements.
	singleInputPlanNode

	// columns is set if this UPDATE is returning any rows, to be
	// consumed by a renderNode upstream. This occurs when there is a
	// RETURNING clause with some scalar expressions.
	columns colinfo.ResultColumns

	run updateRun
}

var _ mutationPlanNode = &updateSwapNode{}

func (u *updateSwapNode) startExec(params runParams) error {
	panic("updateSwapNode cannot be run in local mode")
}

// Next implements the planNode interface.
func (u *updateSwapNode) Next(_ runParams) (bool, error) {
	panic("updateSwapNode cannot be run in local mode")
}

// Values implements the planNode interface.
func (u *updateSwapNode) Values() tree.Datums {
	panic("updateSwapNode cannot be run in local mode")
}

func (u *updateSwapNode) Close(ctx context.Context) {
	u.input.Close(ctx)
	u.run.close(ctx)
	*u = updateSwapNode{}
	updateSwapNodePool.Put(u)
}

func (u *updateSwapNode) rowsWritten() int64 {
	return u.run.rowsAffected()
}

func (u *updateSwapNode) indexRowsWritten() int64 {
	return u.run.tu.indexRowsWritten
}

func (u *updateSwapNode) indexBytesWritten() int64 {
	return u.run.tu.indexBytesWritten
}

func (u *updateSwapNode) returnsRowsAffected() bool {
	return !u.run.rowsNeeded
}

func (u *updateSwapNode) kvCPUTime() int64 {
	return u.run.tu.kvCPUTime
}

func (u *updateSwapNode) enableAutoCommit() {
	u.run.tu.enableAutoCommit()
}

// updateSwapProcessor is a LocalProcessor that wraps updateSwapNode execution logic.
type updateSwapProcessor struct {
	execinfra.ProcessorBase

	input execinfra.RowSource
	node  *updateSwapNode

	outputTypes []*types.T

	cancelChecker cancelchecker.CancelChecker
}

var _ execinfra.LocalProcessor = &updateSwapProcessor{}
var _ execopnode.OpNode = &updateSwapProcessor{}

// Init initializes the updateSwapProcessor.
func (u *updateSwapProcessor) Init(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	post *execinfrapb.PostProcessSpec,
) error {
	if execstats.ShouldCollectStats(ctx, flowCtx.CollectStats) {
		if flowCtx.Txn != nil {
			u.node.run.contentionEventsListener.Init(flowCtx.Txn.ID())
		}
		u.ExecStatsForTrace = u.execStatsForTrace
	}
	memMonitor := execinfra.NewMonitor(ctx, flowCtx.Mon, mon.MakeName("update-swap-mem"))
	return u.InitWithEvalCtx(
		ctx, u, post, u.outputTypes, flowCtx, flowCtx.EvalCtx, processorID, memMonitor,
		execinfra.ProcStateOpts{
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				metrics := execinfrapb.GetMetricsMeta()
				metrics.RowsWritten = u.node.run.rowsAffected()
				metrics.IndexRowsWritten = u.node.run.tu.indexRowsWritten
				metrics.IndexBytesWritten = u.node.run.tu.indexBytesWritten
				metrics.KVCPUTime = u.node.run.tu.kvCPUTime
				meta := []execinfrapb.ProducerMetadata{{Metrics: metrics}}
				u.close()
				return meta
			},
		},
	)
}

// SetInput sets the input RowSource for the updateSwapProcessor.
func (u *updateSwapProcessor) SetInput(ctx context.Context, input execinfra.RowSource) error {
	if execstats.ShouldCollectStats(ctx, u.FlowCtx.CollectStats) {
		input = rowexec.NewInputStatCollector(input)
	}
	u.input = input
	u.AddInputToDrain(input)
	return nil
}

// Start begins execution of the updateSwapProcessor.
func (u *updateSwapProcessor) Start(ctx context.Context) {
	u.StartInternal(ctx, "updateSwapProcessor",
		&u.node.run.contentionEventsListener, &u.node.run.tenantConsumptionListener,
	)
	u.cancelChecker.Reset(ctx, rowinfra.RowExecCancelCheckInterval)
	u.input.Start(ctx)
	u.node.run.traceKV = u.FlowCtx.TraceKV
	u.node.run.mustValidateOldPKValues = true
	u.node.run.init(u.FlowCtx.EvalCtx, u.FlowCtx.Mon, u.node.columns)
	if err := u.node.run.tu.init(u.Ctx(), u.FlowCtx.Txn, u.FlowCtx.EvalCtx); err != nil {
		u.MoveToDraining(err)
		return
	}

	// Run the mutation to completion. UpdateSwap only processes one row, so no
	// need to loop.
	if err := u.processBatch(); err != nil {
		u.MoveToDraining(err)
	}
}

// processBatch implements the batch processing logic moved from updateSwapNode.processBatch.
func (u *updateSwapProcessor) processBatch() error {
	// Update-swap does everything in one batch. There should only be a single row
	// of input, to ensure the savepoint rollback below has the correct SQL
	// semantics.
	if err := u.cancelChecker.Check(); err != nil {
		return err
	}

	// Advance one individual row from input RowSource.
	inputRow, meta := u.input.Next()
	if meta != nil {
		if meta.Err != nil {
			return meta.Err
		}
		return nil
	}

	if inputRow != nil {
		// Convert EncDatumRow to tree.Datums.
		datumRow := make(tree.Datums, len(inputRow))
		err := rowenc.EncDatumRowToDatums(u.input.OutputTypes(), datumRow, inputRow, nil)
		if err != nil {
			return err
		}

		// Process the update of the current input row.
		if err := u.node.run.processSourceRow(u.Ctx(), u.FlowCtx.EvalCtx, &u.SemaCtx, u.FlowCtx.EvalCtx.SessionData(), datumRow); err != nil {
			return err
		}

		// Verify that there was only a single row of input.
		inputRow, meta = u.input.Next()
		if meta != nil && meta.Err != nil {
			return meta.Err
		}
		if inputRow != nil {
			return errors.AssertionFailedf("expected only 1 row as input to update swap")
		}
	}

	// Update swap works by optimistically modifying every index in the same
	// batch. If the row does not actually exist, the write to the primary index
	// will fail with ConditionFailedError, but writes to some secondary indexes
	// might succeed. We use a savepoint here to undo those writes.
	sp, err := u.node.run.tu.createSavepoint(u.Ctx())
	if err != nil {
		return err
	}

	u.node.run.tu.setRowsWrittenLimit(u.FlowCtx.EvalCtx.SessionData())
	if err := u.node.run.tu.finalize(u.Ctx()); err != nil {
		// If this was a ConditionFailedError, it means the row did not exist in the
		// primary index. We must roll back to the savepoint above to undo writes to
		// all secondary indexes.
		if condErr := (*kvpb.ConditionFailedError)(nil); errors.As(err, &condErr) {
			if err := u.node.run.tu.rollbackToSavepoint(u.Ctx(), sp); err != nil {
				return err
			}
			return nil
		}
		return err
	}

	// Possibly initiate a run of CREATE STATISTICS.
	u.FlowCtx.Cfg.StatsRefresher.NotifyMutation(u.Ctx(), u.node.run.tu.tableDesc(), int(u.node.run.rowsAffected()))

	return nil
}

// Next implements the RowSource interface.
func (u *updateSwapProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if u.State != execinfra.StateRunning {
		return nil, u.DrainHelper()
	}

	// Return next row from accumulated results.
	var err error
	for u.node.run.next() {
		datumRow := u.node.run.values()
		encRow := make(rowenc.EncDatumRow, len(datumRow))
		for i, datum := range datumRow {
			encRow[i], err = rowenc.DatumToEncDatum(u.outputTypes[i], datum)
			if err != nil {
				u.MoveToDraining(err)
				return nil, u.DrainHelper()
			}
		}
		if outRow := u.ProcessRowHelper(encRow); outRow != nil {
			return outRow, nil
		}
	}

	u.MoveToDraining(nil)
	return nil, u.DrainHelper()
}

func (u *updateSwapProcessor) close() {
	if u.InternalClose() {
		u.node.run.close(u.Ctx())
		u.MemMonitor.Stop(u.Ctx())
	}
}

// ConsumerClosed implements the RowSource interface.
func (u *updateSwapProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	u.close()
}

// ChildCount is part of the execopnode.OpNode interface.
func (u *updateSwapProcessor) ChildCount(verbose bool) int {
	if _, ok := u.input.(execopnode.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execopnode.OpNode interface.
func (u *updateSwapProcessor) Child(nth int, verbose bool) execopnode.OpNode {
	if nth == 0 {
		if n, ok := u.input.(execopnode.OpNode); ok {
			return n
		}
		panic("input to updateSwapProcessor is not an execopnode.OpNode")
	}
	panic(errors.AssertionFailedf("invalid index %d", nth))
}

// execStatsForTrace implements ProcessorBase.ExecStatsForTrace.
func (u *updateSwapProcessor) execStatsForTrace() *execinfrapb.ComponentStats {
	is, ok := rowexec.GetInputStats(u.input)
	if !ok {
		return nil
	}
	ret := &execinfrapb.ComponentStats{
		Inputs: []execinfrapb.InputStats{is},
		Output: u.OutputHelper.Stats(),
	}
	u.node.run.populateExecStatsForTrace(ret)
	return ret
}
