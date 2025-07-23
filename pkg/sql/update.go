// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
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
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

var updateNodePool = sync.Pool{
	New: func() interface{} {
		return &updateNode{}
	},
}

type updateNode struct {
	singleInputPlanNode

	// columns is set if this UPDATE is returning any rows, to be
	// consumed by a renderNode upstream. This occurs when there is a
	// RETURNING clause with some scalar expressions.
	columns colinfo.ResultColumns

	run updateRun
}

var _ mutationPlanNode = &updateNode{}

// updateRun contains the run-time state of updateNode during local execution.
type updateRun struct {
	mutationOutputHelper
	tu tableUpdater

	checkOrds checkSet

	// rowsNeeded is set to true if the mutation operator needs to return the rows
	// that were affected by the mutation.
	rowsNeeded bool

	// resultRowBuffer is used to prepare a result row for accumulation
	// into the row container above, when rowsNeeded is set.
	resultRowBuffer tree.Datums

	// traceKV caches the current KV tracing flag.
	traceKV bool

	// rowIdxToRetIdx is the mapping from the columns in ru.FetchCols to the
	// columns in the resultRowBuffer. A value of -1 is used to indicate
	// that the column at that index is not part of the resultRowBuffer
	// of the mutation. Otherwise, the value at the i-th index refers to the
	// index of the resultRowBuffer where the i-th column is to be returned.
	rowIdxToRetIdx []int

	// numPassthrough is the number of columns in addition to the set of
	// columns of the target table being returned, that we must pass through
	// from the input node.
	numPassthrough int

	// regionLocalInfo handles erroring out the UPDATE when the
	// enforce_home_region setting is on.
	regionLocalInfo regionLocalInfoType

	mustValidateOldPKValues bool

	originTimestampCPutHelper row.OriginTimestampCPutHelper
}

func (r *updateRun) init(
	evalCtx *eval.Context, mon *mon.BytesMonitor, columns colinfo.ResultColumns,
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

func (u *updateNode) startExec(params runParams) error {
	panic("updateNode cannot be run in local mode")
}

// Next implements the planNode interface.
func (u *updateNode) Next(_ runParams) (bool, error) {
	panic("updateNode cannot be run in local mode")
}

// Values implements the planNode interface.
func (u *updateNode) Values() tree.Datums {
	panic("updateNode cannot be run in local mode")
}

// processSourceRow processes one row from the source for update and, if
// result rows are needed, saves it in the result row container.
func (r *updateRun) processSourceRow(
	ctx context.Context,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
	sessionData *sessiondata.SessionData,
	sourceVals tree.Datums,
) error {
	// sourceVals contains values for the columns from the table, in the order of the
	// table descriptor. (One per column in u.tw.ru.FetchCols)
	//
	// And then after that, all the extra expressions potentially added via
	// a renderNode for the RHS of the assignments.

	// oldValues is the prefix of sourceVals that corresponds to real
	// stored columns in the table, that is, excluding the RHS assignment
	// expressions.
	oldValues := sourceVals[:len(r.tu.ru.FetchCols)]
	sourceVals = sourceVals[len(oldValues):]

	// The update values follow the fetch values and their order corresponds to the order of ru.UpdateCols.
	updateValues := sourceVals[:len(r.tu.ru.UpdateCols)]
	sourceVals = sourceVals[len(updateValues):]

	// The passthrough values follow the update values.
	passthroughValues := sourceVals[:r.numPassthrough]
	sourceVals = sourceVals[len(passthroughValues):]

	// Verify the schema constraints. For consistency with INSERT/UPSERT
	// and compatibility with PostgreSQL, we must do this before
	// processing the CHECK constraints.
	if err := enforceNotNullConstraints(updateValues, r.tu.ru.UpdateCols); err != nil {
		return err
	}

	// Run the CHECK constraints, if any. CheckHelper will either evaluate the
	// constraints itself, or else inspect boolean columns from the input that
	// contain the results of evaluation.
	if !r.checkOrds.Empty() {
		if err := checkMutationInput(
			ctx, evalCtx, semaCtx, sessionData,
			r.tu.tableDesc(), r.checkOrds, sourceVals[:r.checkOrds.Len()],
		); err != nil {
			return err
		}
		sourceVals = sourceVals[r.checkOrds.Len():]
	}

	// Create a set of partial index IDs to not add entries or remove entries
	// from. Put values are followed by del values.
	var pm row.PartialIndexUpdateHelper
	if n := len(r.tu.tableDesc().PartialIndexes()); n > 0 {
		err := pm.Init(sourceVals[:n], sourceVals[n:n*2], r.tu.tableDesc())
		if err != nil {
			return err
		}
		sourceVals = sourceVals[n*2:]
	}

	// Keep track of the vector index partitions to update, as well as the
	// quantized vectors. This information is passed to tableInserter.row below.
	// Order of column values is put partitions, quantized vectors, followed by
	// del partitions
	var vh row.VectorIndexUpdateHelper
	if n := len(r.tu.tableDesc().VectorIndexes()); n > 0 {
		vh.InitForPut(sourceVals[:n], sourceVals[n:n*2], r.tu.tableDesc())
		vh.InitForDel(sourceVals[n*2:n*3], r.tu.tableDesc())
	}

	// Error out the update if the enforce_home_region session setting is on and
	// the row's locality doesn't match the gateway region.
	if err := r.regionLocalInfo.checkHomeRegion(updateValues); err != nil {
		return err
	}

	// Queue the insert in the KV batch.
	newValues, err := r.tu.rowForUpdate(
		ctx, oldValues, updateValues, pm, vh, r.originTimestampCPutHelper, r.mustValidateOldPKValues, r.traceKV,
	)
	if err != nil {
		return err
	}
	r.onModifiedRow()
	if !r.rowsNeeded {
		return nil
	}

	// Result rows must be accumulated.
	//
	// The new values can include all columns,  so the values may contain
	// additional columns for every newly added column not yet visible. We do
	// not want them to be available for RETURNING.
	//
	// MakeUpdater guarantees that the first columns of the new values
	// are those specified u.columns.
	largestRetIdx := -1
	for i := range r.rowIdxToRetIdx {
		retIdx := r.rowIdxToRetIdx[i]
		if retIdx >= 0 {
			if retIdx >= largestRetIdx {
				largestRetIdx = retIdx
			}
			r.resultRowBuffer[retIdx] = newValues[i]
		}
	}

	// At this point we've extracted all the RETURNING values that are part
	// of the target table. We must now extract the columns in the RETURNING
	// clause that refer to other tables (from the FROM clause of the update).
	for i := 0; i < r.numPassthrough; i++ {
		largestRetIdx++
		r.resultRowBuffer[largestRetIdx] = passthroughValues[i]
	}

	return r.addRow(ctx, r.resultRowBuffer)
}

func (u *updateNode) Close(ctx context.Context) {
	u.input.Close(ctx)
	u.run.close(ctx)
	*u = updateNode{}
	updateNodePool.Put(u)
}

func (u *updateNode) rowsWritten() int64 {
	return u.run.rowsAffected()
}

func (u *updateNode) indexRowsWritten() int64 {
	return u.run.tu.indexRowsWritten
}

func (u *updateNode) indexBytesWritten() int64 {
	return u.run.tu.indexBytesWritten
}

func (u *updateNode) returnsRowsAffected() bool {
	return !u.run.rowsNeeded
}

func (u *updateNode) kvCPUTime() int64 {
	return u.run.tu.kvCPUTime
}

func (u *updateNode) enableAutoCommit() {
	u.run.tu.enableAutoCommit()
}

// updateProcessor is a LocalProcessor that wraps updateNode execution logic.
type updateProcessor struct {
	execinfra.ProcessorBase

	input execinfra.RowSource
	node  *updateNode

	outputTypes []*types.T

	datumAlloc      tree.DatumAlloc
	datumScratch    tree.Datums
	encDatumScratch rowenc.EncDatumRow

	cancelChecker cancelchecker.CancelChecker
}

var _ execinfra.LocalProcessor = &updateProcessor{}
var _ execopnode.OpNode = &updateProcessor{}

// Init initializes the updateProcessor.
func (u *updateProcessor) Init(
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
	memMonitor := execinfra.NewMonitor(ctx, flowCtx.Mon, mon.MakeName("update-mem"))
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

// SetInput sets the input RowSource for the updateProcessor.
func (u *updateProcessor) SetInput(ctx context.Context, input execinfra.RowSource) error {
	if execstats.ShouldCollectStats(ctx, u.FlowCtx.CollectStats) {
		input = rowexec.NewInputStatCollector(input)
	}
	u.input = input
	u.AddInputToDrain(input)
	return nil
}

// Start begins execution of the updateProcessor.
func (u *updateProcessor) Start(ctx context.Context) {
	u.StartInternal(ctx, "updateProcessor",
		&u.node.run.contentionEventsListener, &u.node.run.tenantConsumptionListener,
	)
	u.cancelChecker.Reset(ctx, rowinfra.RowExecCancelCheckInterval)
	u.input.Start(ctx)
	u.node.run.traceKV = u.FlowCtx.TraceKV
	u.node.run.init(u.FlowCtx.EvalCtx, u.MemMonitor, u.node.columns)
	if err := u.node.run.tu.init(u.Ctx(), u.FlowCtx.Txn, u.FlowCtx.EvalCtx); err != nil {
		u.MoveToDraining(err)
		return
	}

	// Run the mutation to completion.
	for {
		lastBatch, err := u.processBatch()
		if err != nil {
			u.MoveToDraining(err)
			return
		}
		if lastBatch {
			return
		}
	}
}

// processBatch implements the batch processing logic moved from updateNode.processBatch.
func (u *updateProcessor) processBatch() (lastBatch bool, err error) {
	// Consume/accumulate the rows for this batch.
	lastBatch = false
	for {
		if err = u.cancelChecker.Check(); err != nil {
			return false, err
		}

		// Advance one individual row from input RowSource.
		inputRow, meta := u.input.Next()
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
		if cap(u.datumScratch) < len(inputRow) {
			u.datumScratch = make(tree.Datums, len(inputRow))
		}
		datumRow := u.datumScratch[:len(inputRow)]
		err = rowenc.EncDatumRowToDatums(u.input.OutputTypes(), datumRow, inputRow, &u.datumAlloc)
		if err != nil {
			return false, err
		}

		// Process the update of the current input row.
		if err = u.node.run.processSourceRow(u.Ctx(), u.FlowCtx.EvalCtx, &u.SemaCtx, u.FlowCtx.EvalCtx.SessionData(), datumRow); err != nil {
			return false, err
		}

		// Are we done yet with the current SQL-level batch?
		if u.node.run.tu.currentBatchSize >= u.node.run.tu.maxBatchSize ||
			u.node.run.tu.b.ApproximateMutationBytes() >= u.node.run.tu.maxBatchByteSize {
			break
		}
	}

	if u.node.run.tu.currentBatchSize > 0 {
		if !lastBatch {
			// We only run/commit the batch if there were some rows processed
			// in this batch.
			if err = u.node.run.tu.flushAndStartNewBatch(u.Ctx()); err != nil {
				return false, err
			}
		}
	}

	if lastBatch {
		u.node.run.tu.setRowsWrittenLimit(u.FlowCtx.EvalCtx.SessionData())
		if err = u.node.run.tu.finalize(u.Ctx()); err != nil {
			return false, err
		}
		// Possibly initiate a run of CREATE STATISTICS.
		u.FlowCtx.Cfg.StatsRefresher.NotifyMutation(u.Ctx(), u.node.run.tu.tableDesc(), int(u.node.run.rowsAffected()))
	}
	return lastBatch, nil
}

// Next implements the RowSource interface.
func (u *updateProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if u.State != execinfra.StateRunning {
		return nil, u.DrainHelper()
	}

	// Return next row from accumulated results.
	var err error
	for u.node.run.next() {
		datumRow := u.node.run.values()
		if cap(u.encDatumScratch) < len(datumRow) {
			u.encDatumScratch = make(rowenc.EncDatumRow, len(datumRow))
		}
		encRow := u.encDatumScratch[:len(datumRow)]
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

	// No more rows, move to draining.
	u.MoveToDraining(nil)
	return nil, u.DrainHelper()
}

func (u *updateProcessor) close() {
	if u.InternalClose() {
		u.node.run.close(u.Ctx())
		u.MemMonitor.Stop(u.Ctx())
	}
}

// ConsumerClosed implements the RowSource interface.
func (u *updateProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	u.close()
}

// enforceNotNullConstraints enforces NOT NULL column constraints. row and cols
// must have the same length.
func enforceNotNullConstraints(row tree.Datums, cols []catalog.Column) error {
	if len(row) != len(cols) {
		return errors.AssertionFailedf("expected length of row (%d) and columns (%d) to match",
			len(row), len(cols))
	}
	for i, col := range cols {
		if !col.IsNullable() && row[i] == tree.DNull {
			return sqlerrors.NewNonNullViolationError(col.GetName())
		}
	}
	return nil
}

// ChildCount is part of the execopnode.OpNode interface.
func (u *updateProcessor) ChildCount(verbose bool) int {
	if _, ok := u.input.(execopnode.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execopnode.OpNode interface.
func (u *updateProcessor) Child(nth int, verbose bool) execopnode.OpNode {
	if nth == 0 {
		if n, ok := u.input.(execopnode.OpNode); ok {
			return n
		}
		panic("input to updateProcessor is not an execopnode.OpNode")
	}
	panic(errors.AssertionFailedf("invalid index %d", nth))
}

// execStatsForTrace implements ProcessorBase.ExecStatsForTrace.
func (u *updateProcessor) execStatsForTrace() *execinfrapb.ComponentStats {
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
