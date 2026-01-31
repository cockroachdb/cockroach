// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

var insertFastPathNodePool = sync.Pool{
	New: func() interface{} {
		return &insertFastPathNode{}
	},
}

// insertFastPathNode is a faster implementation of inserting values in a table
// and performing FK checks. It is used when all the foreign key checks can be
// performed via a direct lookup in an index, and when the input is VALUES of
// limited size (at most mutations.MaxBatchSize).
type insertFastPathNode struct {
	zeroInputPlanNode
	// input values, similar to a valuesNode.
	input [][]tree.TypedExpr

	// columns is set if this INSERT is returning any rows, to be
	// consumed by a renderNode upstream. This occurs when there is a
	// RETURNING clause with some scalar expressions.
	columns colinfo.ResultColumns

	run insertFastPathRun
}

var _ mutationPlanNode = &insertFastPathNode{}

type insertFastPathRun struct {
	insertRun

	uniqChecks []insertFastPathCheck

	fkChecks []insertFastPathCheck

	numInputCols int

	// inputBuf stores the evaluation result of the input rows, linearized into a
	// single slice; see inputRow(). Unfortunately we can't do everything one row
	// at a time, because we need the datums for generating error messages in case
	// an FK check fails.
	inputBuf tree.Datums

	// uniqBatch accumulates the uniqueness checks.
	uniqBatch kvpb.BatchRequest
	// uniqSpanInfo keeps track of information for each uniqBatch.Request entry.
	uniqSpanInfo []insertFastPathFKUniqSpanInfo

	// fkBatch accumulates the FK existence checks.
	fkBatch kvpb.BatchRequest
	// fkSpanInfo keeps track of information for each fkBatch.Request entry.
	fkSpanInfo []insertFastPathFKUniqSpanInfo

	// fkSpanMap is used to de-duplicate FK existence checks. Only used if there
	// is more than one input row.
	fkSpanMap map[string]struct{}

	// kvCPUTime tracks the cumulative CPU time (in nanoseconds) that KV reported
	// in BatchResponse headers for FK and uniqueness check batches. The KVCPUTime
	// performed by the INSERT is tracked by the table inserter (insertRun.ti.kvCPUTime).
	kvCPUTime int64
}

// insertFastPathFKUniqSpanInfo records information about each Request in the
// fkBatch or uniqBatch, associating it with a specific check and row index.
type insertFastPathFKUniqSpanInfo struct {
	check  *insertFastPathCheck
	rowIdx int
}

// insertFastPathCheck extends exec.InsertFastPathCheck with
// metadata that is computed once and can be reused across rows.
type insertFastPathCheck struct {
	exec.InsertFastPathCheck

	tabDesc      catalog.TableDescriptor
	idx          catalog.Index
	keyPrefix    []byte
	colMap       catalog.TableColMap
	spanBuilder  span.Builder
	spanSplitter span.Splitter
}

func (c *insertFastPathCheck) init(evalCtx *eval.Context, execCfg *ExecutorConfig) error {
	idx := c.ReferencedIndex.(*optIndex)
	c.tabDesc = c.ReferencedTable.(*optTable).desc
	c.idx = idx.idx

	codec := execCfg.Codec
	c.keyPrefix = rowenc.MakeIndexKeyPrefix(codec, c.tabDesc.GetID(), c.idx.GetID())
	c.spanBuilder.InitAllowingExternalRowData(evalCtx, codec, c.tabDesc, c.idx)

	if c.Locking.MustLockAllRequestedColumnFamilies() {
		c.spanSplitter = span.MakeSplitterForSideEffect(
			c.tabDesc, c.idx, intsets.Fast{}, /* neededColOrdinals */
		)
	} else {
		c.spanSplitter = span.MakeSplitter(c.tabDesc, c.idx, intsets.Fast{} /* neededColOrdinals */)
	}

	if len(c.InsertCols) > idx.numLaxKeyCols {
		return errors.AssertionFailedf(
			"%d FK cols, only %d cols in index", len(c.InsertCols), idx.numLaxKeyCols,
		)
	}
	for i, ord := range c.InsertCols {
		var colID descpb.ColumnID
		if i < c.idx.NumKeyColumns() {
			colID = c.idx.GetKeyColumnID(i)
		} else {
			colID = c.idx.GetKeySuffixColumnID(i - c.idx.NumKeyColumns())
		}

		c.colMap.Set(colID, int(ord))
	}
	return nil
}

// generateSpan returns the span that we need to look up to confirm existence of
// the referenced row.
func (c *insertFastPathCheck) generateSpan(inputRow tree.Datums) (roachpb.Span, error) {
	return row.FKUniqCheckSpan(&c.spanBuilder, c.spanSplitter, inputRow, c.colMap, len(c.InsertCols))
}

// errorForRow returns an error indicating failure of this FK or uniqueness
// check for the given row.
func (c *insertFastPathCheck) errorForRow(inputRow tree.Datums) error {
	values := make(tree.Datums, len(c.InsertCols))
	for i, ord := range c.InsertCols {
		values[i] = inputRow[ord]
	}
	return c.MkErr(values)
}

func (r *insertFastPathRun) inputRow(rowIdx int) tree.Datums {
	start := rowIdx * r.numInputCols
	end := start + r.numInputCols
	return r.inputBuf[start:end:end]
}

// addUniqChecks adds Requests to uniqBatch and entries in uniqSpanInfo as
// needed for checking uniqueness for the given row.
func (r *insertFastPathRun) addUniqChecks(
	ctx context.Context, rowIdx int, inputRow tree.Datums, forTesting bool,
) (combinedRows []tree.Datums, err error) {
	for i := range r.uniqChecks {
		c := &r.uniqChecks[i]

		// See if we have any nulls.
		hasNulls := false
		for _, ord := range c.InsertCols {
			if inputRow[ord] == tree.DNull {
				hasNulls = true
				break
			}
		}
		if hasNulls {
			// We have a row with at least one NULL. NULLs are treated as distinct
			// and we currently don't support the NULLS NOT DISTINCT clause, so this
			// row is always distinct with respect to this particular uniqueness
			// check.
			continue
		}

		// DatumsFromConstraint contains constant values from the WHERE clause
		// constraint which are part of the index key to look up, while inputRow
		// contains the remaining values which are part of the key to look up.
		// Combine them together to get the final index prefix key to search.
		var combinedRow tree.Datums
		if forTesting {
			combinedRows = make([]tree.Datums, len(c.DatumsFromConstraint))
		}
		for templateRowNum, templateRow := range c.DatumsFromConstraint {
			// We can't build the combined row in-place in the template row because
			// the template row will be reused for the next insert row, and
			// overwriting the nil entries would mean we could no longer tell which
			// values must be populated from the input row.
			if templateRowNum == 0 || forTesting {
				combinedRow = make(tree.Datums, len(templateRow))
			}
			copy(combinedRow, templateRow)
			for j := 0; j < len(c.InsertCols); j++ {
				// Datums from single-table constraints are already present in
				// DatumsFromConstraint. Fill in other values from the input row.
				if combinedRow[c.InsertCols[j]] == nil {
					combinedRow[c.InsertCols[j]] = inputRow[c.InsertCols[j]]
				}
			}
			if !forTesting {
				span, err := c.generateSpan(combinedRow)
				if err != nil {
					return nil, err
				}
				reqIdx := len(r.uniqBatch.Requests)
				if r.traceKV {
					log.VEventf(ctx, 2, "UniqScan %s", span)
				}
				r.uniqBatch.Requests = append(r.uniqBatch.Requests, kvpb.RequestUnion{})
				// TODO(msirek): Batch-allocate the kvpb.ScanRequests outside the loop.
				r.uniqBatch.Requests[reqIdx].MustSetInner(&kvpb.ScanRequest{
					RequestHeader: kvpb.RequestHeaderFromSpan(span),
				})
				r.uniqSpanInfo = append(r.uniqSpanInfo, insertFastPathFKUniqSpanInfo{
					check:  c,
					rowIdx: rowIdx,
				})
			} else {
				combinedRows[templateRowNum] = combinedRow
			}
		}
	}
	return combinedRows, nil
}

// addFKChecks adds Requests to fkBatch and entries in fkSpanInfo / fkSpanMap as
// needed for checking foreign keys for the given row.
func (r *insertFastPathRun) addFKChecks(
	ctx context.Context, rowIdx int, inputRow tree.Datums,
) error {
	for i := range r.fkChecks {
		c := &r.fkChecks[i]

		// See if we have any nulls.
		numNulls := 0
		for _, ord := range c.InsertCols {
			if inputRow[ord] == tree.DNull {
				numNulls++
			}
		}
		if numNulls > 0 {
			if c.MatchMethod == tree.MatchFull && numNulls != len(c.InsertCols) {
				return c.errorForRow(inputRow)
			}
			// We have a row with only NULLS, or a row with some NULLs and match
			// method PARTIAL. We can skip this FK check for this row.
			continue
		}

		span, err := c.generateSpan(inputRow)
		if err != nil {
			return err
		}
		if r.fkSpanMap != nil {
			_, exists := r.fkSpanMap[string(span.Key)]
			if exists {
				// Duplicate span.
				continue
			}
			r.fkSpanMap[string(span.Key)] = struct{}{}
		}
		if r.traceKV {
			log.VEventf(ctx, 2, "FKScan %s", span)
		}
		lockStrength := row.GetKeyLockingStrength(descpb.ToScanLockingStrength(c.Locking.Strength))
		lockWaitPolicy := row.GetWaitPolicy(descpb.ToScanLockingWaitPolicy(c.Locking.WaitPolicy))
		lockDurability := row.GetKeyLockingDurability(descpb.ToScanLockingDurability(c.Locking.Durability))
		if r.fkBatch.Header.WaitPolicy != lockWaitPolicy {
			return errors.AssertionFailedf(
				"FK check lock wait policy %s did not match %s",
				lockWaitPolicy, r.fkBatch.Header.WaitPolicy,
			)
		}
		reqIdx := len(r.fkBatch.Requests)
		r.fkBatch.Requests = append(r.fkBatch.Requests, kvpb.RequestUnion{})
		// TODO(msirek): Batch-allocate the kvpb.ScanRequests outside the loop.
		r.fkBatch.Requests[reqIdx].MustSetInner(&kvpb.ScanRequest{
			RequestHeader:        kvpb.RequestHeaderFromSpan(span),
			KeyLockingStrength:   lockStrength,
			KeyLockingDurability: lockDurability,
		})
		r.fkSpanInfo = append(r.fkSpanInfo, insertFastPathFKUniqSpanInfo{
			check:  c,
			rowIdx: rowIdx,
		})
	}
	return nil
}

func (n *insertFastPathNode) startExec(params runParams) error {
	panic("insertFastPathNode cannot be run in local mode")
}

// Next implements the planNode interface.
func (n *insertFastPathNode) Next(_ runParams) (bool, error) {
	panic("insertFastPathNode cannot be run in local mode")
}

// Values implements the planNode interface.
func (n *insertFastPathNode) Values() tree.Datums {
	panic("insertFastPathNode cannot be run in local mode")
}

func (n *insertFastPathNode) Close(ctx context.Context) {
	n.run.close(ctx)
	*n = insertFastPathNode{}
	insertFastPathNodePool.Put(n)
}

func (n *insertFastPathNode) returnsRowsAffected() bool {
	return !n.run.rowsNeeded
}

// See planner.autoCommit.
func (n *insertFastPathNode) enableAutoCommit() {
	n.run.ti.enableAutoCommit()
}

// insertFastPathProcessor is a LocalProcessor that wraps insertFastPathNode execution logic.
type insertFastPathProcessor struct {
	execinfra.ProcessorBase

	node *insertFastPathNode

	outputTypes []*types.T

	encDatumScratch rowenc.EncDatumRow

	cancelChecker cancelchecker.CancelChecker
}

var _ execinfra.LocalProcessor = &insertFastPathProcessor{}
var _ execopnode.OpNode = &insertFastPathProcessor{}

// Init initializes the insertFastPathProcessor.
func (i *insertFastPathProcessor) Init(
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
	memMonitor := execinfra.NewMonitor(ctx, flowCtx.Mon, mon.MakeName("insert-fast-path-mem"))
	return i.InitWithEvalCtx(
		ctx, i, post, i.outputTypes, flowCtx, flowCtx.EvalCtx, processorID, memMonitor,
		execinfra.ProcStateOpts{
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				metrics := execinfrapb.GetMetricsMeta()
				metrics.RowsWritten = i.node.run.rowsAffected()
				metrics.IndexRowsWritten = i.node.run.ti.indexRowsWritten
				metrics.IndexBytesWritten = i.node.run.ti.indexBytesWritten
				metrics.KVCPUTime = i.node.run.kvCPUTime
				meta := []execinfrapb.ProducerMetadata{{Metrics: metrics}}
				i.close()
				return meta
			},
		},
	)
}

// SetInput sets the input RowSource for the insertFastPathProcessor.
func (i *insertFastPathProcessor) SetInput(ctx context.Context, input execinfra.RowSource) error {
	panic(errors.AssertionFailedf("insertFastPathProcessor does not have an input RowSource"))
}

// Start begins execution of the insertFastPathProcessor.
func (i *insertFastPathProcessor) Start(ctx context.Context) {
	i.StartInternal(ctx, "insertFastPathProcessor",
		&i.node.run.contentionEventsListener, &i.node.run.tenantConsumptionListener,
	)
	i.cancelChecker.Reset(ctx, rowinfra.RowExecCancelCheckInterval)
	i.node.run.traceKV = i.FlowCtx.TraceKV
	i.node.run.init(i.FlowCtx.EvalCtx, i.MemMonitor, i.node.columns)

	i.node.run.numInputCols = len(i.node.input[0])
	i.node.run.inputBuf = make(tree.Datums, len(i.node.input)*i.node.run.numInputCols)

	if len(i.node.input) > 1 {
		i.node.run.fkSpanMap = make(map[string]struct{})
	}

	if len(i.node.run.fkChecks) > 0 {
		for j := range i.node.run.fkChecks {
			if err := i.node.run.fkChecks[j].init(i.FlowCtx.EvalCtx, i.FlowCtx.Cfg.ExecutorConfig.(*ExecutorConfig)); err != nil {
				i.MoveToDraining(err)
				return
			}
		}
		maxSpans := len(i.node.run.fkChecks) * len(i.node.input)
		// Any FK checks using locking should have lock wait policy BLOCK.
		i.node.run.fkBatch.Header.WaitPolicy = lock.WaitPolicy_Block
		i.node.run.fkBatch.Requests = make([]kvpb.RequestUnion, 0, maxSpans)
		i.node.run.fkSpanInfo = make([]insertFastPathFKUniqSpanInfo, 0, maxSpans)
		if len(i.node.input) > 1 {
			i.node.run.fkSpanMap = make(map[string]struct{}, maxSpans)
		}
	}

	if len(i.node.run.uniqChecks) > 0 {
		numChecksPerInputRow := 0
		for j := range i.node.run.uniqChecks {
			if err := i.node.run.uniqChecks[j].init(i.FlowCtx.EvalCtx, i.FlowCtx.Cfg.ExecutorConfig.(*ExecutorConfig)); err != nil {
				i.MoveToDraining(err)
				return
			}
			// Each row inserted may result in multiple KV requests to perform the
			// uniqueness checks (1 KV request per entry in DatumsFromConstraint).
			numChecksPerInputRow += len(i.node.run.uniqChecks[j].DatumsFromConstraint)
		}
		maxSpans := len(i.node.input) * numChecksPerInputRow
		i.node.run.uniqBatch.Requests = make([]kvpb.RequestUnion, 0, maxSpans)
		i.node.run.uniqSpanInfo = make([]insertFastPathFKUniqSpanInfo, 0, maxSpans)
	}

	if err := i.node.run.ti.init(i.Ctx(), i.FlowCtx.Txn, i.FlowCtx.EvalCtx); err != nil {
		i.MoveToDraining(err)
		return
	}

	// Run the mutation to completion. InsertFastPath does everything in one
	// batch, so no need to loop.
	if err := i.processBatch(); err != nil {
		i.MoveToDraining(err)
	}
}

// processBatch implements the batch processing logic moved from insertFastPathNode.processBatch.
func (i *insertFastPathProcessor) processBatch() error {
	// The fast path node does everything in one batch.
	for rowIdx, tupleRow := range i.node.input {
		if err := i.cancelChecker.Check(); err != nil {
			return err
		}
		inputRow := i.node.run.inputRow(rowIdx)
		for col, typedExpr := range tupleRow {
			var err error
			inputRow[col], err = eval.Expr(i.Ctx(), i.FlowCtx.EvalCtx, typedExpr)
			if err != nil {
				err = interceptAlterColumnTypeParseError(i.node.run.insertCols, col, err)
				return err
			}
		}

		if buildutil.CrdbTestBuild {
			// This testing knob allows us to suspend execution to force a race condition.
			execCfg := i.FlowCtx.Cfg.ExecutorConfig.(*ExecutorConfig)
			if fn := execCfg.TestingKnobs.AfterArbiterRead; fn != nil {
				fn(i.FlowCtx.EvalCtx.Planner.(*planner).stmt.SQL)
			}
		}

		// Process the insertion for the current source row, potentially.
		// accumulating the result row for later.
		if err := i.node.run.processSourceRow(i.Ctx(), i.FlowCtx.EvalCtx, &i.SemaCtx, i.FlowCtx.EvalCtx.SessionData(), inputRow); err != nil {
			return err
		}

		// Add uniqueness checks.
		if len(i.node.run.uniqChecks) > 0 {
			if _, err := i.node.run.addUniqChecks(i.Ctx(), rowIdx, inputRow, false /* forTesting */); err != nil {
				return err
			}
		}

		// Add FK existence checks.
		if len(i.node.run.fkChecks) > 0 {
			if err := i.node.run.addFKChecks(i.Ctx(), rowIdx, inputRow); err != nil {
				return err
			}
		}
	}

	if len(i.node.run.fkBatch.Requests) > 0 && len(i.node.run.uniqBatch.Requests) > 0 {
		// Perform the foreign key and uniqueness checks in a single batch.
		if err := i.runFKUniqChecks(); err != nil {
			return err
		}
	} else {
		// Perform the uniqueness checks.
		if err := i.runUniqChecks(); err != nil {
			return err
		}

		// Perform the FK checks.
		// TODO(radu): we could run the FK batch in parallel with the main batch (if
		// we aren't auto-committing).
		if err := i.runFKChecks(); err != nil {
			return err
		}
	}
	i.node.run.ti.setRowsWrittenLimit(i.FlowCtx.EvalCtx.SessionData())
	if err := i.node.run.ti.finalize(i.Ctx()); err != nil {
		return err
	}

	// Possibly initiate a run of CREATE STATISTICS.
	i.FlowCtx.Cfg.StatsRefresher.NotifyMutation(i.Ctx(), i.node.run.ti.ri.Helper.TableDesc, len(i.node.input))

	return nil
}

// runUniqChecks runs the uniqBatch and checks that no spans return rows.
func (i *insertFastPathProcessor) runUniqChecks() error {
	if len(i.node.run.uniqBatch.Requests) == 0 {
		return nil
	}
	defer i.node.run.uniqBatch.Reset()

	// Run the uniqueness checks batch.
	ba := i.node.run.uniqBatch.ShallowCopy()
	log.VEventf(i.Ctx(), 2, "uniqueness check: sending a batch with %d requests", len(ba.Requests))
	br, err := i.FlowCtx.Txn.Send(i.Ctx(), ba)
	if err != nil {
		return err.GoError()
	}

	// Accumulate CPU time from the BatchResponse.
	if br.CPUTime > 0 {
		i.node.run.kvCPUTime += br.CPUTime
	}

	for j := range br.Responses {
		resp := br.Responses[j].GetInner().(*kvpb.ScanResponse)
		if len(resp.Rows) > 0 {
			// Found results for lookup; generate the uniqueness violation error.
			info := i.node.run.uniqSpanInfo[j]
			return info.check.errorForRow(i.node.run.inputRow(info.rowIdx))
		}
	}

	return nil
}

// runFKChecks runs the fkBatch and checks that all spans return at least one key.
func (i *insertFastPathProcessor) runFKChecks() error {
	if len(i.node.run.fkBatch.Requests) == 0 {
		return nil
	}
	defer i.node.run.fkBatch.Reset()

	// Run the FK checks batch.
	ba := i.node.run.fkBatch.ShallowCopy()
	log.VEventf(i.Ctx(), 2, "fk check: sending a batch with %d requests", len(ba.Requests))
	br, err := i.FlowCtx.Txn.Send(i.Ctx(), ba)
	if err != nil {
		return err.GoError()
	}

	// Accumulate CPU time from the BatchResponse.
	if br.CPUTime > 0 {
		i.node.run.kvCPUTime += br.CPUTime
	}

	for j := range br.Responses {
		resp := br.Responses[j].GetInner().(*kvpb.ScanResponse)
		if len(resp.Rows) == 0 {
			// No results for lookup; generate the violation error.
			info := i.node.run.fkSpanInfo[j]
			return info.check.errorForRow(i.node.run.inputRow(info.rowIdx))
		}
	}

	return nil
}

// runFKUniqChecks combines the fkBatch and uniqBatch into a single batch and
// then sends the combined batch, so that the requests may be processed in
// parallel. Responses are processed to generate appropriate errors, similar to
// what's done in runFKChecks and runUniqChecks.
func (i *insertFastPathProcessor) runFKUniqChecks() error {
	defer i.node.run.uniqBatch.Reset()
	defer i.node.run.fkBatch.Reset()

	i.node.run.uniqBatch.Requests = append(i.node.run.uniqBatch.Requests, i.node.run.fkBatch.Requests...)

	// Run the combined uniqueness and FK checks batch.
	ba := i.node.run.uniqBatch.ShallowCopy()
	log.VEventf(i.Ctx(), 2, "fk / uniqueness check: sending a batch with %d requests", len(ba.Requests))
	br, err := i.FlowCtx.Txn.Send(i.Ctx(), ba)
	if err != nil {
		return err.GoError()
	}

	// Accumulate CPU time from the BatchResponse.
	if br.CPUTime > 0 {
		i.node.run.kvCPUTime += br.CPUTime
	}

	for j := range br.Responses {
		resp := br.Responses[j].GetInner().(*kvpb.ScanResponse)
		if j < len(i.node.run.uniqSpanInfo) {
			if len(resp.Rows) > 0 {
				// Found results for lookup; generate the uniqueness violation error.
				info := i.node.run.uniqSpanInfo[j]
				return info.check.errorForRow(i.node.run.inputRow(info.rowIdx))
			}
		} else {
			if len(resp.Rows) == 0 {
				// No results for lookup; generate the FK violation error.
				info := i.node.run.fkSpanInfo[j-len(i.node.run.uniqSpanInfo)]
				return info.check.errorForRow(i.node.run.inputRow(info.rowIdx))
			}
		}
	}

	return nil
}

// Next implements the RowSource interface.
func (i *insertFastPathProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
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

func (i *insertFastPathProcessor) close() {
	if i.InternalClose() {
		i.node.run.close(i.Ctx())
		i.MemMonitor.Stop(i.Ctx())
	}
}

// ConsumerClosed implements the RowSource interface.
func (i *insertFastPathProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	i.close()
}

// interceptAlterColumnTypeParseError wraps a type parsing error with a warning
// about the column undergoing an ALTER COLUMN TYPE schema change.
// If colNum is not -1, only the colNum'th column in insertCols will be checked
// for AlterColumnTypeInProgress, otherwise every column in insertCols will
// be checked.
func interceptAlterColumnTypeParseError(insertCols []catalog.Column, colNum int, err error) error {
	// Only intercept the error if the column being inserted into
	// is an actual column. This is to avoid checking on values that don't
	// correspond to an actual column, for example a check constraint.
	if colNum >= len(insertCols) {
		return err
	}
	var insertCol catalog.Column

	// wrapParseError is a helper function that checks if an insertCol has the
	// AlterColumnTypeInProgress flag and wraps the parse error msg stating
	// that the error may be because the column is being altered.
	// Returns if the error msg has been wrapped and the wrapped error msg.
	wrapParseError := func(insertCol catalog.Column, colNum int, err error) (bool, error) {
		if insertCol.ColumnDesc().AlterColumnTypeInProgress {
			code := pgerror.GetPGCode(err)
			if code == pgcode.InvalidTextRepresentation {
				if colNum != -1 {
					// If a column is specified, we can ensure the parse error
					// is happening because the column is undergoing an alter column type
					// schema change.
					return true, errors.Wrapf(err,
						"This table is still undergoing the ALTER COLUMN TYPE schema change, "+
							"this insert is not supported until the schema change is finalized")
				}
				// If no column is specified, the error message is slightly changed to say
				// that the error MAY be because a column is undergoing an alter column type
				// schema change.
				return true, errors.Wrap(err,
					"This table is still undergoing the ALTER COLUMN TYPE schema change, "+
						"this insert may not be supported until the schema change is finalized")
			}
		}
		return false, err
	}

	// If a colNum is specified, we just check the one column for
	// AlterColumnTypeInProgress and return the error whether it's wrapped or not.
	if colNum != -1 {
		insertCol = insertCols[colNum]
		_, err = wrapParseError(insertCol, colNum, err)
		return err
	}

	// If the colNum is -1, we check every insertCol for AlterColumnTypeInProgress.
	for _, insertCol = range insertCols {
		var changed bool
		changed, err = wrapParseError(insertCol, colNum, err)
		if changed {
			return err
		}
	}

	return err
}

// ChildCount is part of the execopnode.OpNode interface.
func (i *insertFastPathProcessor) ChildCount(verbose bool) int {
	return 0
}

// Child is part of the execopnode.OpNode interface.
func (i *insertFastPathProcessor) Child(nth int, verbose bool) execopnode.OpNode {
	panic(errors.AssertionFailedf("invalid index %d", nth))
}

// execStatsForTrace implements ProcessorBase.ExecStatsForTrace.
func (i *insertFastPathProcessor) execStatsForTrace() *execinfrapb.ComponentStats {
	ret := &execinfrapb.ComponentStats{Output: i.OutputHelper.Stats()}
	i.node.run.populateExecStatsForTrace(ret)
	return ret
}
