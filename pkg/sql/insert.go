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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
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
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

var insertNodePool = sync.Pool{
	New: func() interface{} {
		return &insertNode{}
	},
}

var tableInserterPool = sync.Pool{
	New: func() interface{} {
		return &tableInserter{}
	},
}

type insertNode struct {
	singleInputPlanNode

	// columns is set if this INSERT is returning any rows, to be
	// consumed by a renderNode upstream. This occurs when there is a
	// RETURNING clause with some scalar expressions.
	columns colinfo.ResultColumns

	// vectorInsert is set if this INSERT should be executed via a specialized
	// implementation in the vectorized engine. Currently only set for inserts
	// executed on behalf of COPY statements.
	vectorInsert bool

	run insertRun
}

var _ mutationPlanNode = &insertNode{}

// insertRun contains the run-time state of insertNode during local execution.
type insertRun struct {
	mutationOutputHelper
	ti tableInserter

	checkOrds checkSet

	// insertCols are the columns being inserted into.
	insertCols []catalog.Column

	// rowsNeeded is set to true if the mutation operator needs to return the rows
	// that were affected by the mutation.
	rowsNeeded bool

	// resultRowBuffer is used to prepare a result row for accumulation
	// into the row container above, when rowsNeeded is set.
	resultRowBuffer tree.Datums

	// rowIdxToTabColIdx is the mapping from the ordering of rows in
	// insertCols to the ordering in the rows in the table, used when
	// rowsNeeded is set to populate resultRowBuffer and the row
	// container. The return index is -1 if the column for the row
	// index is not public. This is used in conjunction with tabIdxToRetIdx
	// to populate the resultRowBuffer.
	rowIdxToTabColIdx []int

	// tabColIdxToRetIdx is the mapping from the columns in the table to the
	// columns in the resultRowBuffer. A value of -1 is used to indicate
	// that the table column at that index is not part of the resultRowBuffer
	// of the mutation. Otherwise, the value at the i-th index refers to the
	// index of the resultRowBuffer where the i-th column of the table is
	// to be returned.
	tabColIdxToRetIdx []int

	// traceKV caches the current KV tracing flag.
	traceKV bool

	// regionLocalInfo handles erroring out the INSERT when the
	// enforce_home_region setting is on.
	regionLocalInfo regionLocalInfoType

	originTimestampCPutHelper row.OriginTimestampCPutHelper
}

// regionLocalInfoType contains common items needed for determining the home region
// of an insert or update operation.
type regionLocalInfoType struct {

	// regionMustBeLocalColID indicates the id of the column which must use a
	// home region matching the gateway region in a REGIONAL BY ROW table.
	regionMustBeLocalColID descpb.ColumnID

	// gatewayRegion is the string representation of the gateway region when
	// regionMustBeLocalColID is non-zero.
	gatewayRegion string

	// colIDtoRowIndex is the map to use to decode regionMustBeLocalColID into
	// an index into the Datums slice corresponding with the crdb_region column.
	colIDtoRowIndex catalog.TableColMap
}

// setupEnforceHomeRegion sets regionMustBeLocalColID and the gatewayRegion name
// if we're enforcing a home region for this insert run. The colIDtoRowIndex map
// to use to when translating column id to row index is also set up.
func (r *regionLocalInfoType) setupEnforceHomeRegion(
	p *planner, table cat.Table, cols []catalog.Column, colIDtoRowIndex catalog.TableColMap,
) {
	if p.EnforceHomeRegion() {
		if gatewayRegion, ok := p.EvalContext().Locality.Find("region"); ok {
			if homeRegionColName, ok := table.HomeRegionColName(); ok {
				for _, col := range cols {
					if col.ColName() == tree.Name(homeRegionColName) {
						r.regionMustBeLocalColID = col.GetID()
						r.gatewayRegion = gatewayRegion
						r.colIDtoRowIndex = colIDtoRowIndex
						break
					}
				}
			}
		}
	}
}

// checkHomeRegion errors out the insert or update if the enforce_home_region session setting is on and
// the row's locality doesn't match the gateway region.
func (r *regionLocalInfoType) checkHomeRegion(row tree.Datums) error {
	if r.regionMustBeLocalColID != 0 {
		if regionColIdx, ok := r.colIDtoRowIndex.Get(r.regionMustBeLocalColID); ok {
			if regionEnum, regionColIsEnum := row[regionColIdx].(*tree.DEnum); regionColIsEnum {
				if regionEnum.LogicalRep != r.gatewayRegion {
					return pgerror.Newf(pgcode.QueryHasNoHomeRegion,
						`Query has no home region. Try running the query from region '%s'. %s`,
						regionEnum.LogicalRep,
						sqlerrors.EnforceHomeRegionFurtherInfo,
					)
				}
			} else {
				return errors.AssertionFailedf(
					`expected REGIONAL BY ROW AS column id %d to be an enum but found: %v`,
					r.regionMustBeLocalColID, row[regionColIdx].ResolvedType(),
				)
			}
		}
	}
	return nil
}

func (r *insertRun) init(
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

	// In some cases (e.g. `INSERT INTO t (a) ...`) the data source
	// does not provide all the table columns. However we do need to
	// produce result rows that contain values for all the table
	// columns, in the correct order.  This will be done by
	// re-ordering the data into resultRowBuffer.
	//
	// Also we need to re-order the values in the source, ordered by
	// insertCols, when writing them to resultRowBuffer, according to
	// the rowIdxToTabColIdx mapping.

	r.resultRowBuffer = make(tree.Datums, len(columns))
	for i := range r.resultRowBuffer {
		r.resultRowBuffer[i] = tree.DNull
	}

	colIDToRetIndex := catalog.ColumnIDToOrdinalMap(r.ti.tableDesc().PublicColumns())
	r.rowIdxToTabColIdx = make([]int, len(r.insertCols))
	for i, col := range r.insertCols {
		if idx, ok := colIDToRetIndex.Get(col.GetID()); !ok {
			// Column must be write only and not public.
			r.rowIdxToTabColIdx[i] = -1
		} else {
			r.rowIdxToTabColIdx[i] = idx
		}
	}
}

// processSourceRow processes one row from the source for insertion and, if
// result rows are needed, saves it in the result row container.
func (r *insertRun) processSourceRow(
	ctx context.Context,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
	sessionData *sessiondata.SessionData,
	rowVals tree.Datums,
) error {
	insertVals := rowVals[:len(r.insertCols)]
	if err := enforceNotNullConstraints(insertVals, r.insertCols); err != nil {
		return err
	}

	rowVals = rowVals[len(insertVals):]

	// Verify the CHECK constraint results, if any.
	if n := r.checkOrds.Len(); n > 0 {
		if err := checkMutationInput(
			ctx, evalCtx, semaCtx, sessionData,
			r.ti.tableDesc(), r.checkOrds, rowVals[:n],
		); err != nil {
			return err
		}
		rowVals = rowVals[n:]
	}

	// Create a set of partial index IDs to not write to. Indexes should not be
	// written to when they are partial indexes and the row does not satisfy the
	// predicate. This set is passed as a parameter to tableInserter.row below.
	var pm row.PartialIndexUpdateHelper
	if n := len(r.ti.tableDesc().PartialIndexes()); n > 0 {
		err := pm.Init(rowVals[:n], nil /* partialIndexDelVals */, r.ti.tableDesc())
		if err != nil {
			return err
		}
		rowVals = rowVals[n:]
	}

	// Keep track of the vector index partitions to update, as well as the
	// quantized vectors. This information is passed to tableInserter.row below.
	// Input is one partition key per vector index followed by one quantized vector
	// per index.
	var vh row.VectorIndexUpdateHelper
	if n := len(r.ti.tableDesc().VectorIndexes()); n > 0 {
		vh.InitForPut(rowVals[:n], rowVals[n:n*2], r.ti.tableDesc())
	}

	// Error out the insert if the enforce_home_region session setting is on and
	// the row's locality doesn't match the gateway region.
	if err := r.regionLocalInfo.checkHomeRegion(insertVals); err != nil {
		return err
	}

	// Queue the insert in the KV batch.
	if err := r.ti.row(ctx, insertVals, pm, vh, r.originTimestampCPutHelper, r.traceKV); err != nil {
		return err
	}
	r.onModifiedRow()
	if !r.rowsNeeded {
		return nil
	}

	// Result rows must be accumulated.
	for i, val := range insertVals {
		// The downstream consumer will want the rows in the order of
		// the table descriptor, not that of insertCols. Reorder them
		// and ignore non-public columns.
		if tabIdx := r.rowIdxToTabColIdx[i]; tabIdx >= 0 {
			if retIdx := r.tabColIdxToRetIdx[tabIdx]; retIdx >= 0 {
				r.resultRowBuffer[retIdx] = val
			}
		}
	}
	return r.addRow(ctx, r.resultRowBuffer)
}

func (n *insertNode) startExec(params runParams) error {
	panic("insertNode cannot be run in local mode")
}

// Next implements the planNode interface.
func (n *insertNode) Next(_ runParams) (bool, error) {
	panic("insertNode cannot be run in local mode")
}

// Values implements the planNode interface.
func (n *insertNode) Values() tree.Datums {
	panic("insertNode cannot be run in local mode")
}

func (n *insertNode) Close(ctx context.Context) {
	n.input.Close(ctx)
	n.run.close(ctx)
	*n = insertNode{}
	insertNodePool.Put(n)
}

// See planner.autoCommit.
func (n *insertNode) enableAutoCommit() {
	n.run.ti.enableAutoCommit()
}

func (n *insertNode) returnsRowsAffected() bool {
	return !n.run.rowsNeeded
}

// insertProcessor is a LocalProcessor that wraps insertNode execution logic.
type insertProcessor struct {
	execinfra.ProcessorBase

	input execinfra.RowSource
	node  *insertNode

	outputTypes []*types.T

	datumAlloc      tree.DatumAlloc
	datumScratch    tree.Datums
	encDatumScratch rowenc.EncDatumRow

	cancelChecker cancelchecker.CancelChecker
}

var _ execinfra.LocalProcessor = &insertProcessor{}
var _ execopnode.OpNode = &insertProcessor{}

// Init initializes the insertProcessor.
func (i *insertProcessor) Init(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	post *execinfrapb.PostProcessSpec,
) error {
	if execstats.ShouldCollectStats(ctx, flowCtx.CollectStats) {
		if flowCtx.Txn != nil {
			i.node.run.contentionEventsListener.Init(flowCtx.Txn.ID())
		}
		i.ExecStatsForTrace = i.execStatsForTrace
	}
	memMonitor := execinfra.NewMonitor(ctx, flowCtx.Mon, mon.MakeName("insert-mem"))
	return i.InitWithEvalCtx(
		ctx, i, post, i.outputTypes, flowCtx, flowCtx.EvalCtx, processorID, memMonitor,
		execinfra.ProcStateOpts{
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				metrics := execinfrapb.GetMetricsMeta()
				metrics.RowsWritten = i.node.run.rowsAffected()
				metrics.IndexRowsWritten = i.node.run.ti.indexRowsWritten
				metrics.IndexBytesWritten = i.node.run.ti.indexBytesWritten
				metrics.KVCPUTime = i.node.run.ti.kvCPUTime
				meta := []execinfrapb.ProducerMetadata{{Metrics: metrics}}
				i.close()
				return meta
			},
		},
	)
}

// SetInput sets the input RowSource for the insertProcessor.
func (i *insertProcessor) SetInput(ctx context.Context, input execinfra.RowSource) error {
	if execstats.ShouldCollectStats(ctx, i.FlowCtx.CollectStats) {
		input = rowexec.NewInputStatCollector(input)
	}
	i.input = input
	i.AddInputToDrain(input)
	return nil
}

// Start begins execution of the insertProcessor.
func (i *insertProcessor) Start(ctx context.Context) {
	i.StartInternal(ctx, "insertProcessor",
		&i.node.run.contentionEventsListener, &i.node.run.tenantConsumptionListener,
	)
	i.cancelChecker.Reset(ctx, rowinfra.RowExecCancelCheckInterval)
	i.input.Start(ctx)
	i.node.run.traceKV = i.FlowCtx.TraceKV
	i.node.run.init(i.FlowCtx.EvalCtx, i.MemMonitor, i.node.columns)
	if err := i.node.run.ti.init(i.Ctx(), i.FlowCtx.Txn, i.FlowCtx.EvalCtx); err != nil {
		i.MoveToDraining(err)
		return
	}

	// Run the mutation to completion.
	for {
		lastBatch, err := i.processBatch()
		if err != nil {
			i.MoveToDraining(err)
			return
		}
		if lastBatch {
			return
		}
	}
}

// processBatch implements the batch processing logic moved from insertNode.processBatch.
func (i *insertProcessor) processBatch() (lastBatch bool, err error) {
	// Consume/accumulate the rows for this batch.
	lastBatch = false
	for {
		if err = i.cancelChecker.Check(); err != nil {
			return false, err
		}

		// Advance one individual row from input RowSource.
		inputRow, meta := i.input.Next()
		if meta != nil {
			if meta.Err != nil {
				// Intercept parse error due to ALTER COLUMN TYPE schema change.
				err := interceptAlterColumnTypeParseError(i.node.run.insertCols, -1, meta.Err)
				return false, err
			}
			continue
		}
		if inputRow == nil {
			lastBatch = true
			break
		}

		if buildutil.CrdbTestBuild {
			// This testing knob allows us to suspend execution to force a race condition.
			execCfg := i.FlowCtx.Cfg.ExecutorConfig.(*ExecutorConfig)
			if fn := execCfg.TestingKnobs.AfterArbiterRead; fn != nil {
				fn(i.FlowCtx.EvalCtx.Planner.(*planner).stmt.SQL)
			}
		}

		// Convert EncDatumRow to tree.Datums.
		if cap(i.datumScratch) < len(inputRow) {
			i.datumScratch = make(tree.Datums, len(inputRow))
		}
		datumRow := i.datumScratch[:len(inputRow)]
		err := rowenc.EncDatumRowToDatums(i.input.OutputTypes(), datumRow, inputRow, &i.datumAlloc)
		if err != nil {
			return false, err
		}

		// Process the insertion of the current input row.
		if err = i.node.run.processSourceRow(i.Ctx(), i.FlowCtx.EvalCtx, &i.SemaCtx, i.FlowCtx.EvalCtx.SessionData(), datumRow); err != nil {
			return false, err
		}

		// Are we done yet with the current SQL-level batch?
		if i.node.run.ti.currentBatchSize >= i.node.run.ti.maxBatchSize ||
			i.node.run.ti.b.ApproximateMutationBytes() >= i.node.run.ti.maxBatchByteSize {
			break
		}
	}

	if i.node.run.ti.currentBatchSize > 0 {
		if !lastBatch {
			// We only run/commit the batch if there were some rows processed.
			// in this batch.
			if err = i.node.run.ti.flushAndStartNewBatch(i.Ctx()); err != nil {
				return false, err
			}
		}
	}

	if lastBatch {
		i.node.run.ti.setRowsWrittenLimit(i.FlowCtx.EvalCtx.SessionData())
		if err = i.node.run.ti.finalize(i.Ctx()); err != nil {
			return false, err
		}
		// Possibly initiate a run of CREATE STATISTICS.
		i.FlowCtx.Cfg.StatsRefresher.NotifyMutation(i.Ctx(), i.node.run.ti.tableDesc(), int(i.node.run.rowsAffected()))
	}
	return lastBatch, nil
}

// Next implements the RowSource interface.
func (i *insertProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if i.State != execinfra.StateRunning {
		return nil, i.DrainHelper()
	}

	// Return next row from accumulated results.
	var err error
	for i.node.run.next() {
		datumRow := i.node.run.values()
		if cap(i.encDatumScratch) < len(datumRow) {
			i.encDatumScratch = make(rowenc.EncDatumRow, len(datumRow))
		}
		encRow := i.encDatumScratch[:len(datumRow)]
		for j, datum := range datumRow {
			encRow[j], err = rowenc.DatumToEncDatum(i.outputTypes[j], datum)
			if err != nil {
				i.MoveToDraining(err)
				return nil, i.DrainHelper()
			}
		}
		if outRow := i.ProcessRowHelper(encRow); outRow != nil {
			return outRow, nil
		}
	}

	// No more rows to return.
	i.MoveToDraining(nil)
	return nil, i.DrainHelper()
}

func (i *insertProcessor) close() {
	if i.InternalClose() {
		i.node.run.close(i.Ctx())
		i.MemMonitor.Stop(i.Ctx())
	}
}

// ConsumerClosed implements the RowSource interface.
func (i *insertProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	i.close()
}

// ChildCount is part of the execopnode.OpNode interface.
func (i *insertProcessor) ChildCount(verbose bool) int {
	if _, ok := i.input.(execopnode.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execopnode.OpNode interface.
func (i *insertProcessor) Child(nth int, verbose bool) execopnode.OpNode {
	if nth == 0 {
		if n, ok := i.input.(execopnode.OpNode); ok {
			return n
		}
		panic("input to insertProcessor is not an execopnode.OpNode")
	}
	panic(errors.AssertionFailedf("invalid index %d", nth))
}

// execStatsForTrace implements ProcessorBase.ExecStatsForTrace.
func (i *insertProcessor) execStatsForTrace() *execinfrapb.ComponentStats {
	is, ok := rowexec.GetInputStats(i.input)
	if !ok {
		return nil
	}
	ret := &execinfrapb.ComponentStats{
		Inputs: []execinfrapb.InputStats{is},
		Output: i.OutputHelper.Stats(),
	}
	i.node.run.populateExecStatsForTrace(ret)
	return ret
}
