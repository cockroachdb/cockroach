// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsqlrun

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

// ParallelScanResultThreshold is the number of results up to which, if the
// maximum number of results returned by a scan is known, the table reader
// disables batch limits in the dist sender. This results in the parallelization
// of these scans.
const ParallelScanResultThreshold = 10000

// tableReader is the start of a computation flow; it performs KV operations to
// retrieve rows for a table, runs a filter expression, and passes rows with the
// desired column values to an output RowReceiver.
// See docs/RFCS/distributed_sql.md
type tableReader struct {
	ProcessorBase

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

var _ Processor = &tableReader{}
var _ RowSource = &tableReader{}
var _ distsqlpb.MetadataSource = &tableReader{}

const tableReaderProcName = "table reader"

var trPool = sync.Pool{
	New: func() interface{} {
		return &tableReader{}
	},
}

// newTableReader creates a tableReader.
func newTableReader(
	flowCtx *FlowCtx,
	processorID int32,
	spec *distsqlpb.TableReaderSpec,
	post *distsqlpb.PostProcessSpec,
	output RowReceiver,
) (*tableReader, error) {
	if flowCtx.nodeID == 0 {
		return nil, errors.Errorf("attempting to create a tableReader with uninitialized NodeID")
	}

	tr := trPool.Get().(*tableReader)

	tr.limitHint = limitHint(spec.LimitHint, post)
	tr.maxResults = spec.MaxResults
	tr.maxTimestampAge = time.Duration(spec.MaxTimestampAgeNanos)

	returnMutations := spec.Visibility == distsqlpb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC
	types := spec.Table.ColumnTypesWithMutations(returnMutations)
	tr.ignoreMisplannedRanges = flowCtx.local
	if err := tr.Init(
		tr,
		post,
		types,
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		ProcStateOpts{
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

	neededColumns := tr.out.neededColumns()

	var fetcher row.Fetcher
	columnIdxMap := spec.Table.ColumnIdxMapWithMutations(returnMutations)
	if _, _, err := initRowFetcher(
		&fetcher, &spec.Table, int(spec.IndexIdx), columnIdxMap, spec.Reverse,
		neededColumns, spec.IsCheck, &tr.alloc, spec.Visibility,
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
		tr.finishTrace = tr.outputStatsToTrace
	} else {
		tr.fetcher = &rowFetcherWrapper{Fetcher: &fetcher}
	}

	return tr, nil
}

func initRowFetcher(
	fetcher *row.Fetcher,
	desc *sqlbase.TableDescriptor,
	indexIdx int,
	colIdxMap map[sqlbase.ColumnID]int,
	reverseScan bool,
	valNeededForCol util.FastIntSet,
	isCheck bool,
	alloc *sqlbase.DatumAlloc,
	scanVisibility distsqlpb.ScanVisibility,
) (index *sqlbase.IndexDescriptor, isSecondaryIndex bool, err error) {
	immutDesc := sqlbase.NewImmutableTableDescriptor(*desc)
	index, isSecondaryIndex, err = immutDesc.FindIndexByIndexIdx(indexIdx)
	if err != nil {
		return nil, false, err
	}

	cols := immutDesc.Columns
	if scanVisibility == distsqlpb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC {
		cols = immutDesc.ReadableColumns
	}
	tableArgs := row.FetcherTableArgs{
		Desc:             immutDesc,
		Index:            index,
		ColIdxMap:        colIdxMap,
		IsSecondaryIndex: isSecondaryIndex,
		Cols:             cols,
		ValNeededForCol:  valNeededForCol,
	}
	if err := fetcher.Init(
		reverseScan, true /* returnRangeInfo */, isCheck, alloc, tableArgs,
	); err != nil {
		return nil, false, err
	}

	return index, isSecondaryIndex, nil
}

func (tr *tableReader) generateTrailingMeta(ctx context.Context) []distsqlpb.ProducerMetadata {
	trailingMeta := tr.generateMeta(ctx)
	tr.InternalClose()
	return trailingMeta
}

// Start is part of the RowSource interface.
func (tr *tableReader) Start(ctx context.Context) context.Context {
	if tr.flowCtx.txn == nil {
		log.Fatalf(ctx, "tableReader outside of txn")
	}

	// Like every processor, the tableReader will have a context with a log tag
	// and a span. The underlying fetcher inherits the proc's span, but not the
	// log tag.
	fetcherCtx := ctx
	ctx = tr.StartInternal(ctx, tableReaderProcName)
	if procSpan := opentracing.SpanFromContext(ctx); procSpan != nil {
		fetcherCtx = opentracing.ContextWithSpan(fetcherCtx, procSpan)
	}

	// This call doesn't do much; the real "starting" is below.
	tr.fetcher.Start(fetcherCtx)

	limitBatches := true
	// We turn off limited batches if we know we have no limit and if the
	// tableReader spans will return less than the ParallelScanResultThreshold.
	// This enables distsender parallelism - if limitBatches is true, distsender
	// does *not* parallelize multi-range scan requests.
	if tr.maxResults != 0 &&
		tr.maxResults < ParallelScanResultThreshold &&
		tr.limitHint == 0 &&
		sqlbase.ParallelScans.Get(&tr.flowCtx.Settings.SV) {
		limitBatches = false
	}
	log.VEventf(ctx, 1, "starting scan with limitBatches %t", limitBatches)
	var err error
	if tr.maxTimestampAge == 0 {
		err = tr.fetcher.StartScan(
			fetcherCtx, tr.flowCtx.txn, tr.spans,
			limitBatches, tr.limitHint, tr.flowCtx.traceKV,
		)
	} else {
		initialTS := tr.flowCtx.txn.GetTxnCoordMeta(ctx).Txn.OrigTimestamp
		err = tr.fetcher.StartInconsistentScan(
			fetcherCtx, tr.flowCtx.ClientDB, initialTS,
			tr.maxTimestampAge, tr.spans,
			limitBatches, tr.limitHint, tr.flowCtx.traceKV,
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

// Next is part of the RowSource interface.
func (tr *tableReader) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	for tr.State == StateRunning {
		row, meta := tr.fetcher.Next()

		if meta != nil {
			if meta.Err != nil {
				tr.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			tr.MoveToDraining(nil /* err */)
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

var _ distsqlpb.DistSQLSpanStats = &TableReaderStats{}

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
	is, ok := getFetcherInputStats(tr.flowCtx, tr.fetcher)
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

func (tr *tableReader) generateMeta(ctx context.Context) []distsqlpb.ProducerMetadata {
	var trailingMeta []distsqlpb.ProducerMetadata
	if !tr.ignoreMisplannedRanges {
		ranges := misplannedRanges(ctx, tr.fetcher.GetRangesInfo(), tr.flowCtx.nodeID)
		if ranges != nil {
			trailingMeta = append(trailingMeta, distsqlpb.ProducerMetadata{Ranges: ranges})
		}
	}
	if meta := getTxnCoordMeta(ctx, tr.flowCtx.txn); meta != nil {
		trailingMeta = append(trailingMeta, distsqlpb.ProducerMetadata{TxnCoordMeta: meta})
	}

	meta := distsqlpb.GetProducerMeta()
	meta.Metrics = distsqlpb.GetMetricsMeta()
	meta.Metrics.BytesRead, meta.Metrics.RowsRead = tr.fetcher.GetBytesRead(), tr.rowsRead
	trailingMeta = append(trailingMeta, *meta)
	return trailingMeta
}

// DrainMeta is part of the MetadataSource interface.
func (tr *tableReader) DrainMeta(ctx context.Context) []distsqlpb.ProducerMetadata {
	return tr.generateMeta(ctx)
}
