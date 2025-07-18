// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// deleteRun contains the run-time state of deleteNode during local execution.
type deleteRun struct {
	mutationOutputHelper
	td tableDeleter

	// rowsNeeded is set to true if the mutation operator needs to return the rows
	// that were affected by the mutation.
	rowsNeeded bool

	// resultRowBuffer is used to prepare a result row for accumulation
	// into the row container above, when rowsNeeded is set.
	resultRowBuffer tree.Datums

	// traceKV caches the current KV tracing flag.
	traceKV bool

	// rowIdxToRetIdx is the mapping from the columns returned by the deleter
	// to the columns in the resultRowBuffer. A value of -1 is used to indicate
	// that the column at that index is not part of the resultRowBuffer
	// of the mutation. Otherwise, the value at the i-th index refers to the
	// index of the resultRowBuffer where the i-th column is to be returned.
	rowIdxToRetIdx []int

	// numPassthrough is the number of columns in addition to the set of columns
	// of the target table being returned, that must be passed through from the
	// input node.
	numPassthrough int

	mustValidateOldPKValues bool

	originTimestampCPutHelper row.OriginTimestampCPutHelper
}

func (r *deleteRun) init(
	_ context.Context, evalCtx *eval.Context, mon *mon.BytesMonitor, columns colinfo.ResultColumns,
) {
	if ots := evalCtx.SessionData().OriginTimestampForLogicalDataReplication; ots.IsSet() {
		r.originTimestampCPutHelper.OriginTimestamp = ots
	}

	if !r.rowsNeeded {
		return
	}

	r.rows = rowcontainer.NewRowContainer(
		mon.MakeBoundAccount(),
		colinfo.ColTypeInfoFromResCols(columns),
	)
	r.resultRowBuffer = make([]tree.Datum, len(columns))
	for i := range r.resultRowBuffer {
		r.resultRowBuffer[i] = tree.DNull
	}
}

// processSourceRow processes one row from the source for deletion and, if
// result rows are needed, saves it in the result row container
func (r *deleteRun) processSourceRow(ctx context.Context, sourceVals tree.Datums) error {
	// Remove extra columns for partial index predicate values and AFTER triggers.
	deleteVals := sourceVals[:len(r.td.rd.FetchCols)+r.numPassthrough]
	sourceVals = sourceVals[len(deleteVals):]

	// Create a set of partial index IDs to not delete from. Indexes should not
	// be deleted from when they are partial indexes and the row does not
	// satisfy the predicate and therefore do not exist in the partial index.
	// This set is passed as a argument to tableDeleter.row below.
	var pm row.PartialIndexUpdateHelper
	if n := len(r.td.tableDesc().PartialIndexes()); n > 0 {
		err := pm.Init(nil /* partialIndexPutVals */, sourceVals[:n], r.td.tableDesc())
		if err != nil {
			return err
		}
		sourceVals = sourceVals[n:]
	}

	// Keep track of the vector index partitions to update. This information is
	// passed to tableInserter.row below.
	var vh row.VectorIndexUpdateHelper
	if n := len(r.td.tableDesc().VectorIndexes()); n > 0 {
		vh.InitForDel(sourceVals[:n], r.td.tableDesc())
	}

	// Queue the deletion in the KV batch.
	if err := r.td.row(
		ctx, deleteVals, pm, vh, r.originTimestampCPutHelper, r.mustValidateOldPKValues, r.traceKV,
	); err != nil {
		return err
	}
	r.onModifiedRow()
	if !r.rowsNeeded {
		return nil
	}

	// Result rows must be accumulated.
	//
	// The new values can include all columns, so the values may contain
	// additional columns for every newly dropped column not visible. We do not
	// want them to be available for RETURNING.
	//
	// r.rows.NumCols() is guaranteed to only contain the requested
	// public columns.
	largestRetIdx := -1
	for i := range r.rowIdxToRetIdx {
		retIdx := r.rowIdxToRetIdx[i]
		if retIdx >= 0 {
			if retIdx >= largestRetIdx {
				largestRetIdx = retIdx
			}
			r.resultRowBuffer[retIdx] = deleteVals[i]
		}
	}

	// At this point we've extracted all the RETURNING values that are part
	// of the target table. We must now extract the columns in the RETURNING
	// clause that refer to other tables (from the USING clause of the delete).
	if r.numPassthrough > 0 {
		passthroughBegin := len(r.td.rd.FetchCols)
		passthroughEnd := passthroughBegin + r.numPassthrough
		passthroughValues := deleteVals[passthroughBegin:passthroughEnd]

		for i := 0; i < r.numPassthrough; i++ {
			largestRetIdx++
			r.resultRowBuffer[largestRetIdx] = passthroughValues[i]
		}

	}
	return r.addRow(ctx, r.resultRowBuffer)
}

var deleteNodePool = sync.Pool{
	New: func() interface{} {
		return &deleteNode{}
	},
}

type deleteNode struct {
	singleInputPlanNode

	// columns is set if this DELETE is returning any rows, to be
	// consumed by a renderNode upstream. This occurs when there is a
	// RETURNING clause with some scalar expressions.
	columns colinfo.ResultColumns

	run deleteRun
}

var _ mutationPlanNode = &deleteNode{}

func (d *deleteNode) startExec(params runParams) error {
	panic("deleteNode cannot be run in local mode")
}

// Next implements the planNode interface.
func (d *deleteNode) Next(_ runParams) (bool, error) {
	panic("deleteNode cannot be run in local mode")
}

// Values implements the planNode interface.
func (d *deleteNode) Values() tree.Datums {
	panic("deleteNode cannot be run in local mode")
}

func (d *deleteNode) Close(ctx context.Context) {
	d.input.Close(ctx)
	d.run.close(ctx)
	*d = deleteNode{}
	deleteNodePool.Put(d)
}

func (d *deleteNode) rowsWritten() int64 {
	return d.run.rowsAffected()
}

func (d *deleteNode) indexRowsWritten() int64 {
	return d.run.td.indexRowsWritten
}

func (d *deleteNode) indexBytesWritten() int64 {
	// No bytes counted as written for a deletion.
	return 0
}

func (d *deleteNode) returnsRowsAffected() bool {
	return !d.run.rowsNeeded
}

func (d *deleteNode) kvCPUTime() int64 {
	return d.run.td.kvCPUTime
}

func (d *deleteNode) enableAutoCommit() {
	d.run.td.enableAutoCommit()
}

// deleteProcessor is a LocalProcessor that wraps deleteNode execution logic.
type deleteProcessor struct {
	execinfra.ProcessorBase

	input execinfra.RowSource
	node  *deleteNode

	outputTypes []*types.T

	datumAlloc      tree.DatumAlloc
	datumScratch    tree.Datums
	encDatumScratch rowenc.EncDatumRow

	cancelChecker cancelchecker.CancelChecker
}

var _ execinfra.LocalProcessor = &deleteProcessor{}
var _ execopnode.OpNode = &deleteProcessor{}

// Init initializes the deleteProcessor.
func (d *deleteProcessor) Init(
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
	memMonitor := execinfra.NewMonitor(ctx, flowCtx.Mon, mon.MakeName("delete-mem"))
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

// SetInput sets the input RowSource for the deleteProcessor.
func (d *deleteProcessor) SetInput(ctx context.Context, input execinfra.RowSource) error {
	if execstats.ShouldCollectStats(ctx, d.FlowCtx.CollectStats) {
		input = rowexec.NewInputStatCollector(input)
	}
	d.input = input
	d.AddInputToDrain(input)
	return nil
}

// Start begins execution of the deleteProcessor.
func (d *deleteProcessor) Start(ctx context.Context) {
	d.StartInternal(ctx, "deleteProcessor",
		&d.node.run.contentionEventsListener, &d.node.run.tenantConsumptionListener,
	)
	d.cancelChecker.Reset(ctx, rowinfra.RowExecCancelCheckInterval)
	d.input.Start(ctx)
	d.node.run.traceKV = d.FlowCtx.TraceKV
	d.node.run.init(d.Ctx(), d.FlowCtx.EvalCtx, d.MemMonitor, d.node.columns)
	if err := d.node.run.td.init(d.Ctx(), d.FlowCtx.Txn, d.FlowCtx.EvalCtx); err != nil {
		d.MoveToDraining(err)
		return
	}

	// Run the mutation to completion.
	for {
		lastBatch, err := d.processBatch()
		if err != nil {
			d.MoveToDraining(err)
			return
		}
		if lastBatch {
			return
		}
	}
}

// processBatch buffers a batch of rows from the input and flushes them for
// deletion.
func (d *deleteProcessor) processBatch() (lastBatch bool, err error) {
	// Consume/accumulate the rows for this batch.
	lastBatch = false
	for {
		if err = d.cancelChecker.Check(); err != nil {
			return false, err
		}

		// Advance one individual row from input RowSource.
		inputRow, meta := d.input.Next()
		if meta != nil {
			if meta.Err != nil {
				return false, meta.Err
			}
			continue
		}
		if inputRow == nil {
			lastBatch = true
			break
		}

		// Convert EncDatumRow to tree.Datums.
		if cap(d.datumScratch) < len(inputRow) {
			d.datumScratch = make(tree.Datums, len(inputRow))
		}
		datumRow := d.datumScratch[:len(inputRow)]
		err := rowenc.EncDatumRowToDatums(d.input.OutputTypes(), datumRow, inputRow, &d.datumAlloc)
		if err != nil {
			return false, err
		}

		// Process the deletion of the current input row.
		if err = d.node.run.processSourceRow(d.Ctx(), datumRow); err != nil {
			return false, err
		}

		// Are we done yet with the current SQL-level batch?
		if d.node.run.td.currentBatchSize >= d.node.run.td.maxBatchSize ||
			d.node.run.td.b.ApproximateMutationBytes() >= d.node.run.td.maxBatchByteSize {
			break
		}
	}

	if d.node.run.td.currentBatchSize > 0 {
		if !lastBatch {
			// We only run/commit the batch if there were some rows processed
			// in this batch.
			if err = d.node.run.td.flushAndStartNewBatch(d.Ctx()); err != nil {
				return false, err
			}
		}
	}

	if lastBatch {
		d.node.run.td.setRowsWrittenLimit(d.FlowCtx.EvalCtx.SessionData())
		if err = d.node.run.td.finalize(d.Ctx()); err != nil {
			return false, err
		}
		// Possibly initiate a run of CREATE STATISTICS.
		d.FlowCtx.Cfg.StatsRefresher.NotifyMutation(d.Ctx(), d.node.run.td.tableDesc(), int(d.node.run.rowsAffected()))
	}
	return lastBatch, nil
}

// Next implements the RowSource interface.
func (d *deleteProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if d.State != execinfra.StateRunning {
		return nil, d.DrainHelper()
	}

	// Return next row from accumulated results.
	var err error
	for d.node.run.next() {
		datumRow := d.node.run.values()
		if cap(d.encDatumScratch) < len(datumRow) {
			d.encDatumScratch = make(rowenc.EncDatumRow, len(datumRow))
		}
		encRow := d.encDatumScratch[:len(datumRow)]
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

func (d *deleteProcessor) close() {
	if d.InternalClose() {
		d.node.run.close(d.Ctx())
		d.MemMonitor.Stop(d.Ctx())
	}
}

// ConsumerClosed implements the RowSource interface.
func (d *deleteProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	d.close()
}

// ChildCount is part of the execopnode.OpNode interface.
func (d *deleteProcessor) ChildCount(verbose bool) int {
	if _, ok := d.input.(execopnode.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execopnode.OpNode interface.
func (d *deleteProcessor) Child(nth int, verbose bool) execopnode.OpNode {
	if nth == 0 {
		if n, ok := d.input.(execopnode.OpNode); ok {
			return n
		}
		panic("input to deleteProcessor is not an execopnode.OpNode")
	}
	panic(errors.AssertionFailedf("invalid index %d", nth))
}

// execStatsForTrace implements ProcessorBase.ExecStatsForTrace.
func (d *deleteProcessor) execStatsForTrace() *execinfrapb.ComponentStats {
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
