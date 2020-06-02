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
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
)

// tableReader is the start of a computation flow; it performs KV operations to
// retrieve rows for a table, runs a filter expression, and passes rows with the
// desired column values to an output RowReceiver.
// See docs/RFCS/distributed_sql.md
type tableReader struct {
	execinfra.ProcessorBase

	spans     roachpb.Spans
	limitHint int64

	// maxResults is non-zero if there is a limit on the total number of rows
	// that the tableReader will read.
	maxResults uint64

	// See TableReaderSpec.MaxTimestampAgeNanos.
	maxTimestampAge time.Duration

	ignoreMisplannedRanges bool

	// fetcher wraps a row.Fetcher, allowing the tableReader to add a stat
	// collection layer.
	fetcher rowFetcher
	alloc   sqlbase.DatumAlloc

	// rowsRead is the number of rows read and is tracked unconditionally.
	rowsRead int64
}

var _ execinfra.Processor = &tableReader{}
var _ execinfra.RowSource = &tableReader{}
var _ execinfrapb.MetadataSource = &tableReader{}
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

	tr := trPool.Get().(*tableReader)

	tr.limitHint = execinfra.LimitHint(spec.LimitHint, post)
	tr.maxResults = spec.MaxResults
	tr.maxTimestampAge = time.Duration(spec.MaxTimestampAgeNanos)

	returnMutations := spec.Visibility == execinfra.ScanVisibilityPublicAndNotPublic
	types := spec.Table.ColumnTypesWithMutations(returnMutations)
	tr.ignoreMisplannedRanges = flowCtx.Local
	if err := tr.Init(
		tr,
		post,
		types,
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

	neededColumns := tr.Out.NeededColumns()

	var fetcher row.Fetcher
	columnIdxMap := spec.Table.ColumnIdxMapWithMutations(returnMutations)
	if _, _, err := initRowFetcher(
		flowCtx, &fetcher, &spec.Table, int(spec.IndexIdx), columnIdxMap, spec.Reverse,
		neededColumns, spec.IsCheck, &tr.alloc, spec.Visibility, spec.LockingStrength,
	); err != nil {
		return nil, err
	}

	nSpans := len(spec.Spans)
	if cap(tr.spans) >= nSpans {
		tr.spans = tr.spans[:nSpans]
	} else {
		tr.spans = make(roachpb.Spans, nSpans)
	}
	for i, s := range spec.Spans {
		tr.spans[i] = s.Span
	}

	if sp := opentracing.SpanFromContext(flowCtx.EvalCtx.Ctx()); sp != nil && tracing.IsRecording(sp) {
		tr.fetcher = newRowFetcherStatCollector(&fetcher)
		tr.FinishTrace = tr.outputStatsToTrace
	} else {
		tr.fetcher = &fetcher
	}

	return tr, nil
}

func (tr *tableReader) generateTrailingMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	trailingMeta := tr.generateMeta(ctx)
	tr.InternalClose()
	return trailingMeta
}

// Start is part of the RowSource interface.
func (tr *tableReader) Start(ctx context.Context) context.Context {
	if tr.FlowCtx.Txn == nil {
		log.Fatalf(ctx, "tableReader outside of txn")
	}

	ctx = tr.StartInternal(ctx, tableReaderProcName)

	limitBatches := execinfra.ScanShouldLimitBatches(tr.maxResults, tr.limitHint, tr.FlowCtx)
	log.VEventf(ctx, 1, "starting scan with limitBatches %t", limitBatches)
	var err error
	if tr.maxTimestampAge == 0 {
		err = tr.fetcher.StartScan(
			ctx, tr.FlowCtx.Txn, tr.spans,
			limitBatches, tr.limitHint, tr.FlowCtx.TraceKV,
		)
	} else {
		initialTS := tr.FlowCtx.Txn.ReadTimestamp()
		err = tr.fetcher.StartInconsistentScan(
			ctx, tr.FlowCtx.Cfg.DB, initialTS,
			tr.maxTimestampAge, tr.spans,
			limitBatches, tr.limitHint, tr.FlowCtx.TraceKV,
		)
	}

	if err != nil {
		tr.MoveToDraining(err)
	}
	return ctx
}

// Release releases this tableReader back to the pool.
func (tr *tableReader) Release() {
	tr.ProcessorBase.Reset()
	tr.fetcher.Reset()
	*tr = tableReader{
		ProcessorBase: tr.ProcessorBase,
		fetcher:       tr.fetcher,
		spans:         tr.spans[:0],
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
func (tr *tableReader) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for tr.State == execinfra.StateRunning {
		// Check if it is time to emit a progress update.
		if tr.rowsRead >= tableReaderProgressFrequency {
			meta := execinfrapb.GetProducerMeta()
			meta.Metrics = execinfrapb.GetMetricsMeta()
			meta.Metrics.RowsRead = tr.rowsRead
			tr.rowsRead = 0
			return nil, meta
		}

		row, _, _, err := tr.fetcher.NextRow(tr.Ctx)
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

// ConsumerClosed is part of the RowSource interface.
func (tr *tableReader) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	tr.InternalClose()
}

var _ execinfrapb.DistSQLSpanStats = &TableReaderStats{}

const tableReaderTagPrefix = "tablereader."

// Stats implements the SpanStats interface.
func (trs *TableReaderStats) Stats() map[string]string {
	inputStatsMap := trs.InputStats.Stats(tableReaderTagPrefix)
	inputStatsMap[tableReaderTagPrefix+bytesReadTagSuffix] = humanizeutil.IBytes(trs.BytesRead)
	return inputStatsMap
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (trs *TableReaderStats) StatsForQueryPlan() []string {
	return append(
		trs.InputStats.StatsForQueryPlan("" /* prefix */),
		fmt.Sprintf("%s: %s", bytesReadQueryPlanSuffix, humanizeutil.IBytes(trs.BytesRead)),
	)
}

// outputStatsToTrace outputs the collected tableReader stats to the trace. Will
// fail silently if the tableReader is not collecting stats.
func (tr *tableReader) outputStatsToTrace() {
	is, ok := getFetcherInputStats(tr.FlowCtx, tr.fetcher)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(tr.Ctx); sp != nil {
		tracing.SetSpanStats(sp, &TableReaderStats{
			InputStats: is,
			BytesRead:  tr.fetcher.GetBytesRead(),
		})
	}
}

func (tr *tableReader) generateMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	var trailingMeta []execinfrapb.ProducerMetadata
	if !tr.ignoreMisplannedRanges {
		nodeID, ok := tr.FlowCtx.NodeID.OptionalNodeID()
		if ok {
			ranges := execinfra.MisplannedRanges(ctx, tr.fetcher.GetRangesInfo(), nodeID)
			if ranges != nil {
				trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{Ranges: ranges})
			}
		}
	}
	if tfs := execinfra.GetLeafTxnFinalState(ctx, tr.FlowCtx.Txn); tfs != nil {
		trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{LeafTxnFinalState: tfs})
	}

	meta := execinfrapb.GetProducerMeta()
	meta.Metrics = execinfrapb.GetMetricsMeta()
	meta.Metrics.BytesRead, meta.Metrics.RowsRead = tr.fetcher.GetBytesRead(), tr.rowsRead
	trailingMeta = append(trailingMeta, *meta)
	return trailingMeta
}

// DrainMeta is part of the MetadataSource interface.
func (tr *tableReader) DrainMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	return tr.generateMeta(ctx)
}

// ChildCount is part of the execinfra.OpNode interface.
func (tr *tableReader) ChildCount(bool) int {
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (tr *tableReader) Child(nth int, _ bool) execinfra.OpNode {
	panic(fmt.Sprintf("invalid index %d", nth))
}
