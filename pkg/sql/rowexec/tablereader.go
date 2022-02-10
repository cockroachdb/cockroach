// Copyright 2016 The Cockroach Authors.
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
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/optional"
	"github.com/cockroachdb/errors"
)

// tableReader is the start of a computation flow; it performs KV operations to
// retrieve rows for a table, runs a filter expression, and passes rows with the
// desired column values to an output RowReceiver.
// See docs/RFCS/distributed_sql.md
type tableReader struct {
	execinfra.ProcessorBase
	execinfra.SpansWithCopy

	limitHint       rowinfra.RowLimit
	parallelize     bool
	batchBytesLimit rowinfra.BytesLimit

	scanStarted bool

	// See TableReaderSpec.MaxTimestampAgeNanos.
	maxTimestampAge time.Duration

	ignoreMisplannedRanges bool

	// fetcher wraps a row.Fetcher, allowing the tableReader to add a stat
	// collection layer.
	fetcher rowFetcher
	alloc   tree.DatumAlloc

	scanStats execinfra.ScanStats

	// rowsRead is the number of rows read and is tracked unconditionally.
	rowsRead int64
}

var _ execinfra.Processor = &tableReader{}
var _ execinfra.RowSource = &tableReader{}
var _ execinfra.Releasable = &tableReader{}
var _ execinfra.OpNode = &tableReader{}

const tableReaderProcName = "table reader"

var trPool = sync.Pool{
	New: func() interface{} {
		return &tableReader{}
	},
}

// newTableReader creates a tableReader.
func newTableReader(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.TableReaderSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*tableReader, error) {
	// NB: we hit this with a zero NodeID (but !ok) with multi-tenancy.
	if nodeID, ok := flowCtx.NodeID.OptionalNodeID(); ok && nodeID == 0 {
		return nil, errors.Errorf("attempting to create a tableReader with uninitialized NodeID")
	}

	if spec.LimitHint > 0 || spec.BatchBytesLimit > 0 {
		// Parallelize shouldn't be set when there's a limit hint, but double-check
		// just in case.
		spec.Parallelize = false
	}
	var batchBytesLimit rowinfra.BytesLimit
	if !spec.Parallelize {
		batchBytesLimit = rowinfra.BytesLimit(spec.BatchBytesLimit)
		if batchBytesLimit == 0 {
			batchBytesLimit = rowinfra.DefaultBatchBytesLimit
		}
	}

	tr := trPool.Get().(*tableReader)

	tr.limitHint = rowinfra.RowLimit(execinfra.LimitHint(spec.LimitHint, post))
	tr.parallelize = spec.Parallelize
	tr.batchBytesLimit = batchBytesLimit
	tr.maxTimestampAge = time.Duration(spec.MaxTimestampAgeNanos)

	resultTypes := make([]*types.T, len(spec.FetchSpec.FetchedColumns))
	for i := range resultTypes {
		resultTypes[i] = spec.FetchSpec.FetchedColumns[i].Type
	}

	tr.ignoreMisplannedRanges = flowCtx.Local
	if err := tr.Init(
		tr,
		post,
		resultTypes,
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{
			// We don't pass tr.input as an inputToDrain; tr.input is just an adapter
			// on top of a Fetcher; draining doesn't apply to it. Moreover, Andrei
			// doesn't trust that the adapter will do the right thing on a Next() call
			// after it had previously returned an error.
			InputsToDrain:        nil,
			TrailingMetaCallback: tr.generateTrailingMeta,
		},
	); err != nil {
		return nil, err
	}

	var fetcher row.Fetcher
	if err := fetcher.Init(
		flowCtx.EvalCtx.Context,
		spec.Reverse,
		spec.LockingStrength,
		spec.LockingWaitPolicy,
		flowCtx.EvalCtx.SessionData().LockTimeout,
		&tr.alloc,
		flowCtx.EvalCtx.Mon,
		&spec.FetchSpec,
	); err != nil {
		return nil, err
	}

	tr.Spans = spec.Spans
	if !tr.ignoreMisplannedRanges {
		// Make a copy of the spans so that we could get the misplanned ranges
		// info.
		tr.MakeSpansCopy()
	}

	if execinfra.ShouldCollectStats(flowCtx.EvalCtx.Ctx(), flowCtx) {
		tr.fetcher = newRowFetcherStatCollector(&fetcher)
		tr.ExecStatsForTrace = tr.execStatsForTrace
	} else {
		tr.fetcher = &fetcher
	}

	return tr, nil
}

func (tr *tableReader) generateTrailingMeta() []execinfrapb.ProducerMetadata {
	// We need to generate metadata before closing the processor because
	// InternalClose() updates tr.Ctx to the "original" context.
	trailingMeta := tr.generateMeta()
	tr.close()
	return trailingMeta
}

// Start is part of the RowSource interface.
func (tr *tableReader) Start(ctx context.Context) {
	if tr.FlowCtx.Txn == nil {
		log.Fatalf(ctx, "tableReader outside of txn")
	}

	// Keep ctx assignment so we remember StartInternal can make a new one.
	ctx = tr.StartInternal(ctx, tableReaderProcName)
	// Appease the linter.
	_ = ctx
}

func (tr *tableReader) startScan(ctx context.Context) error {
	limitBatches := !tr.parallelize
	var bytesLimit rowinfra.BytesLimit
	if !limitBatches {
		bytesLimit = rowinfra.NoBytesLimit
	} else {
		bytesLimit = tr.batchBytesLimit
	}
	log.VEventf(ctx, 1, "starting scan with limitBatches %t", limitBatches)
	var err error
	if tr.maxTimestampAge == 0 {
		err = tr.fetcher.StartScan(
			ctx, tr.FlowCtx.Txn, tr.Spans, bytesLimit, tr.limitHint,
			tr.FlowCtx.TraceKV,
			tr.EvalCtx.TestingKnobs.ForceProductionBatchSizes,
		)
	} else {
		initialTS := tr.FlowCtx.Txn.ReadTimestamp()
		err = tr.fetcher.StartInconsistentScan(
			ctx, tr.FlowCtx.Cfg.DB, initialTS, tr.maxTimestampAge, tr.Spans,
			bytesLimit, tr.limitHint, tr.FlowCtx.TraceKV,
			tr.EvalCtx.TestingKnobs.ForceProductionBatchSizes,
			tr.EvalCtx.QualityOfService(),
		)
	}
	tr.scanStarted = true
	return err
}

// Release releases this tableReader back to the pool.
func (tr *tableReader) Release() {
	tr.ProcessorBase.Reset()
	tr.fetcher.Reset()
	// Deeply reset the spans so that we don't hold onto the keys of the spans.
	tr.SpansWithCopy.Reset()
	*tr = tableReader{
		ProcessorBase: tr.ProcessorBase,
		SpansWithCopy: tr.SpansWithCopy,
		fetcher:       tr.fetcher,
		rowsRead:      0,
	}
	trPool.Put(tr)
}

var tableReaderProgressFrequency int64 = 5000

// TestingSetScannedRowProgressFrequency changes the frequency at which
// row-scanned progress metadata is emitted by table readers.
func TestingSetScannedRowProgressFrequency(val int64) func() {
	oldVal := tableReaderProgressFrequency
	tableReaderProgressFrequency = val
	return func() { tableReaderProgressFrequency = oldVal }
}

// Next is part of the RowSource interface.
func (tr *tableReader) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for tr.State == execinfra.StateRunning {
		if !tr.scanStarted {
			err := tr.startScan(tr.Ctx)
			if err != nil {
				tr.MoveToDraining(err)
				break
			}
		}
		// Check if it is time to emit a progress update.
		if tr.rowsRead >= tableReaderProgressFrequency {
			meta := execinfrapb.GetProducerMeta()
			meta.Metrics = execinfrapb.GetMetricsMeta()
			meta.Metrics.RowsRead = tr.rowsRead
			tr.rowsRead = 0
			return nil, meta
		}

		row, err := tr.fetcher.NextRow(tr.Ctx)
		if row == nil || err != nil {
			tr.MoveToDraining(err)
			break
		}

		// When tracing is enabled, number of rows read is tracked twice (once
		// here, and once through InputStats). This is done so that non-tracing
		// case can avoid tracking of the stall time which gives a noticeable
		// performance hit.
		tr.rowsRead++
		if outRow := tr.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, tr.DrainHelper()
}

func (tr *tableReader) close() {
	if tr.InternalClose() {
		if tr.fetcher != nil {
			tr.fetcher.Close(tr.Ctx)
		}
	}
}

// ConsumerClosed is part of the RowSource interface.
func (tr *tableReader) ConsumerClosed() {
	tr.close()
}

// execStatsForTrace implements ProcessorBase.ExecStatsForTrace.
func (tr *tableReader) execStatsForTrace() *execinfrapb.ComponentStats {
	is, ok := getFetcherInputStats(tr.fetcher)
	if !ok {
		return nil
	}
	tr.scanStats = execinfra.GetScanStats(tr.Ctx)
	ret := &execinfrapb.ComponentStats{
		KV: execinfrapb.KVStats{
			BytesRead:      optional.MakeUint(uint64(tr.fetcher.GetBytesRead())),
			TuplesRead:     is.NumTuples,
			KVTime:         is.WaitTime,
			ContentionTime: optional.MakeTimeValue(execinfra.GetCumulativeContentionTime(tr.Ctx)),
		},
		Output: tr.OutputHelper.Stats(),
	}
	execinfra.PopulateKVMVCCStats(&ret.KV, &tr.scanStats)
	return ret
}

func (tr *tableReader) generateMeta() []execinfrapb.ProducerMetadata {
	var trailingMeta []execinfrapb.ProducerMetadata
	if !tr.ignoreMisplannedRanges {
		nodeID, ok := tr.FlowCtx.NodeID.OptionalNodeID()
		if ok {
			ranges := execinfra.MisplannedRanges(tr.Ctx, tr.SpansCopy, nodeID, tr.FlowCtx.Cfg.RangeCache)
			if ranges != nil {
				trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{Ranges: ranges})
			}
		}
	}
	if tfs := execinfra.GetLeafTxnFinalState(tr.Ctx, tr.FlowCtx.Txn); tfs != nil {
		trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{LeafTxnFinalState: tfs})
	}

	meta := execinfrapb.GetProducerMeta()
	meta.Metrics = execinfrapb.GetMetricsMeta()
	meta.Metrics.BytesRead = tr.fetcher.GetBytesRead()
	meta.Metrics.RowsRead = tr.rowsRead
	return append(trailingMeta, *meta)
}

// ChildCount is part of the execinfra.OpNode interface.
func (tr *tableReader) ChildCount(bool) int {
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (tr *tableReader) Child(nth int, _ bool) execinfra.OpNode {
	panic(errors.AssertionFailedf("invalid index %d", nth))
}
