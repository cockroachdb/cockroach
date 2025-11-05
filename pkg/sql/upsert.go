// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

var upsertNodePool = sync.Pool{
	New: func() interface{} {
		return &upsertNode{}
	},
}

type upsertNode struct {
	singleInputPlanNode

	// columns is set if this UPDATE is returning any rows, to be
	// consumed by a renderNode upstream. This occurs when there is a
	// RETURNING clause with some scalar expressions.
	columns colinfo.ResultColumns

	run upsertRun
}

var _ mutationPlanNode = &upsertNode{}

// upsertRun contains the run-time state of upsertNode during local execution.
type upsertRun struct {
	tw        tableUpserter
	checkOrds checkSet

	// insertCols are the columns being inserted/upserted into.
	insertCols []catalog.Column

	// traceKV caches the current KV tracing flag.
	traceKV bool

	originTimestampCPutHelper row.OriginTimestampCPutHelper
}

func (r *upsertRun) init(ctx context.Context, evalCtx *eval.Context, txn *kv.Txn) error {
	if ots := evalCtx.SessionData().OriginTimestampForLogicalDataReplication; ots.IsSet() {
		r.originTimestampCPutHelper.OriginTimestamp = ots
	}
	return r.tw.init(ctx, txn, evalCtx)
}

func (n *upsertNode) startExec(params runParams) error {
	panic("upsertNode cannot be run in local mode")
}

// Next implements the planNode interface.
func (n *upsertNode) Next(_ runParams) (bool, error) {
	panic("upsertNode cannot be run in local mode")
}

// Values implements the planNode interface.
func (n *upsertNode) Values() tree.Datums {
	panic("upsertNode cannot be run in local mode")
}

// processSourceRow processes one row from the source for upsertion.
// The table writer is in charge of accumulating the result rows.
func (r *upsertRun) processSourceRow(
	ctx context.Context,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
	sessionData *sessiondata.SessionData,
	execCfg *ExecutorConfig,
	rowVals tree.Datums,
) error {
	// Check for NOT NULL constraint violations.
	if r.tw.canaryOrdinal != -1 && rowVals[r.tw.canaryOrdinal] != tree.DNull {
		// When there is a canary column and its value is not NULL, then an
		// existing row is being updated, so check only the update columns for
		// NOT NULL constraint violations.
		offset := len(r.insertCols) + len(r.tw.fetchCols)
		vals := rowVals[offset : offset+len(r.tw.updateCols)]
		if err := enforceNotNullConstraints(vals, r.tw.updateCols); err != nil {
			return err
		}
	} else {
		// Otherwise, there is no canary column (i.e., canaryOrdinal is -1,
		// which is the case for "blind" upsert which overwrites existing rows
		// without performing a read) or it is NULL, indicating that a new row
		// is being inserted. In this case, check the insert columns for a NOT
		// NULL constraint violation.
		vals := rowVals[:len(r.insertCols)]
		if err := enforceNotNullConstraints(vals, r.insertCols); err != nil {
			return err
		}
	}

	lastUpsertCol := len(r.insertCols) + len(r.tw.fetchCols) + len(r.tw.updateCols)
	if r.tw.canaryOrdinal != -1 {
		lastUpsertCol++
	}
	upsertVals := rowVals[:lastUpsertCol]
	rowVals = rowVals[lastUpsertCol:]

	// Verify the CHECK constraints by inspecting boolean columns from the input that
	// contain the results of evaluation.
	if !r.checkOrds.Empty() {
		if err := checkMutationInput(
			ctx, evalCtx, semaCtx, sessionData,
			r.tw.tableDesc(), r.checkOrds, rowVals[:r.checkOrds.Len()],
		); err != nil {
			return err
		}
		rowVals = rowVals[r.checkOrds.Len():]
	}

	// Create a set of partial index IDs to not add or remove entries from. Order is puts
	// then deletes.
	var pm row.PartialIndexUpdateHelper
	if n := len(r.tw.tableDesc().PartialIndexes()); n > 0 {
		err := pm.Init(rowVals[:n], rowVals[n:n*2], r.tw.tableDesc())
		if err != nil {
			return err
		}
		rowVals = rowVals[n*2:]
	}

	// Keep track of the vector index partitions to update, as well as the
	// quantized vectors for puts. This information is passed to tableInserter.row
	// below.
	var vh row.VectorIndexUpdateHelper
	if n := len(r.tw.tableDesc().VectorIndexes()); n > 0 {
		vh.InitForPut(rowVals[:n], rowVals[n:n*2], r.tw.tableDesc())
		vh.InitForDel(rowVals[n*2:n*3], r.tw.tableDesc())
	}

	if buildutil.CrdbTestBuild {
		// This testing knob allows us to suspend execution to force a race condition.
		if fn := execCfg.TestingKnobs.AfterArbiterRead; fn != nil {
			fn(evalCtx.Planner.(*planner).stmt.SQL)
		}
	}

	// Process the row. This is also where the tableWriter will accumulate
	// the row for later.
	return r.tw.row(ctx, upsertVals, pm, vh, r.originTimestampCPutHelper, r.traceKV)
}

func (n *upsertNode) Close(ctx context.Context) {
	n.input.Close(ctx)
	n.run.tw.close(ctx)
	*n = upsertNode{}
	upsertNodePool.Put(n)
}

func (n *upsertNode) returnsRowsAffected() bool {
	return !n.run.tw.rowsNeeded
}

func (n *upsertNode) enableAutoCommit() {
	n.run.tw.enableAutoCommit()
}

// upsertProcessor is a LocalProcessor that wraps upsertNode execution logic.
type upsertProcessor struct {
	execinfra.ProcessorBase

	input execinfra.RowSource
	node  *upsertNode

	outputTypes []*types.T

	datumAlloc      tree.DatumAlloc
	datumScratch    tree.Datums
	encDatumScratch rowenc.EncDatumRow

	cancelChecker cancelchecker.CancelChecker
}

var _ execinfra.LocalProcessor = &upsertProcessor{}
var _ execopnode.OpNode = &upsertProcessor{}

// Init initializes the upsertProcessor.
func (u *upsertProcessor) Init(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	post *execinfrapb.PostProcessSpec,
) error {
	if execstats.ShouldCollectStats(ctx, flowCtx.CollectStats) {
		if flowCtx.Txn != nil {
			u.node.run.tw.contentionEventsListener.Init(flowCtx.Txn.ID())
		}
		u.ExecStatsForTrace = u.execStatsForTrace
	}
	memMonitor := execinfra.NewMonitor(ctx, flowCtx.Mon, mon.MakeName("upsert-mem"))
	return u.InitWithEvalCtx(
		ctx, u, post, u.outputTypes, flowCtx, flowCtx.EvalCtx, processorID, memMonitor,
		execinfra.ProcStateOpts{
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				metrics := execinfrapb.GetMetricsMeta()
				metrics.RowsWritten = u.node.run.tw.rowsAffected()
				metrics.IndexRowsWritten = u.node.run.tw.indexRowsWritten
				metrics.IndexBytesWritten = u.node.run.tw.indexBytesWritten
				metrics.KVCPUTime = u.node.run.tw.kvCPUTime
				meta := []execinfrapb.ProducerMetadata{{Metrics: metrics}}
				u.close()
				return meta
			},
		},
	)
}

// SetInput sets the input RowSource for the upsertProcessor.
func (u *upsertProcessor) SetInput(ctx context.Context, input execinfra.RowSource) error {
	if execstats.ShouldCollectStats(ctx, u.FlowCtx.CollectStats) {
		input = rowexec.NewInputStatCollector(input)
	}
	u.input = input
	u.AddInputToDrain(input)
	return nil
}

// Start begins execution of the upsertProcessor.
func (u *upsertProcessor) Start(ctx context.Context) {
	u.StartInternal(ctx, "upsertProcessor",
		&u.node.run.tw.contentionEventsListener, &u.node.run.tw.tenantConsumptionListener,
	)
	u.cancelChecker.Reset(ctx, rowinfra.RowExecCancelCheckInterval)
	u.input.Start(ctx)
	u.node.run.traceKV = u.FlowCtx.TraceKV
	if err := u.node.run.init(u.Ctx(), u.FlowCtx.EvalCtx, u.FlowCtx.Txn); err != nil {
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

// processBatch implements the batch processing logic moved from upsertNode.processBatch.
func (u *upsertProcessor) processBatch() (lastBatch bool, err error) {
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
		err := rowenc.EncDatumRowToDatums(u.input.OutputTypes(), datumRow, inputRow, &u.datumAlloc)
		if err != nil {
			return false, err
		}

		// Process the upsert of the current input row.
		execCfg := u.FlowCtx.Cfg.ExecutorConfig.(*ExecutorConfig)
		if err = u.node.run.processSourceRow(u.Ctx(), u.FlowCtx.EvalCtx, &u.SemaCtx, u.FlowCtx.EvalCtx.SessionData(), execCfg, datumRow); err != nil {
			return false, err
		}

		// Are we done yet with the current SQL-level batch?
		if u.node.run.tw.currentBatchSize >= u.node.run.tw.maxBatchSize ||
			u.node.run.tw.b.ApproximateMutationBytes() >= u.node.run.tw.maxBatchByteSize {
			break
		}
	}

	if u.node.run.tw.currentBatchSize > 0 {
		if !lastBatch {
			// We only run/commit the batch if there were some rows processed
			// in this batch.
			if err = u.node.run.tw.flushAndStartNewBatch(u.Ctx()); err != nil {
				return false, err
			}
		}
	}

	if lastBatch {
		u.node.run.tw.setRowsWrittenLimit(u.FlowCtx.EvalCtx.SessionData())
		if err = u.node.run.tw.finalize(u.Ctx()); err != nil {
			return false, err
		}
		// Possibly initiate a run of CREATE STATISTICS.
		u.FlowCtx.Cfg.StatsRefresher.NotifyMutation(u.Ctx(), u.node.run.tw.tableDesc(), int(u.node.run.tw.rowsAffected()))
	}
	return lastBatch, nil
}

// Next implements the RowSource interface.
func (u *upsertProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if u.State != execinfra.StateRunning {
		return nil, u.DrainHelper()
	}

	// Return next row from accumulated results.
	var err error
	for u.node.run.tw.next() {
		datumRow := u.node.run.tw.values()
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

	// No more rows to return.
	u.MoveToDraining(nil)
	return nil, u.DrainHelper()
}

func (u *upsertProcessor) close() {
	if u.InternalClose() {
		u.node.run.tw.close(u.Ctx())
		u.MemMonitor.Stop(u.Ctx())
	}
}

// ConsumerClosed implements the RowSource interface.
func (u *upsertProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	u.close()
}

// ChildCount is part of the execopnode.OpNode interface.
func (u *upsertProcessor) ChildCount(verbose bool) int {
	if _, ok := u.input.(execopnode.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execopnode.OpNode interface.
func (u *upsertProcessor) Child(nth int, verbose bool) execopnode.OpNode {
	if nth == 0 {
		if n, ok := u.input.(execopnode.OpNode); ok {
			return n
		}
		panic("input to upsertProcessor is not an execopnode.OpNode")
	}
	panic(errors.AssertionFailedf("invalid index %d", nth))
}

// execStatsForTrace implements ProcessorBase.ExecStatsForTrace.
func (u *upsertProcessor) execStatsForTrace() *execinfrapb.ComponentStats {
	is, ok := rowexec.GetInputStats(u.input)
	if !ok {
		return nil
	}
	ret := &execinfrapb.ComponentStats{
		Inputs: []execinfrapb.InputStats{is},
		Output: u.OutputHelper.Stats(),
	}
	u.node.run.tw.populateExecStatsForTrace(ret)
	return ret
}
