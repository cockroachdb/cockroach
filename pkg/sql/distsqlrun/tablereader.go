// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package distsqlrun

import (
	"context"
	"fmt"
	"sync"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// tableReader is the start of a computation flow; it performs KV operations to
// retrieve rows for a table, runs a filter expression, and passes rows with the
// desired column values to an output RowReceiver.
// See docs/RFCS/distributed_sql.md
type tableReader struct {
	processorBase

	tableDesc sqlbase.TableDescriptor
	spans     roachpb.Spans
	limitHint int64

	// input is really the fetcher below, possibly wrapped in a stats generator.
	input RowSource
	// fetcher is the underlying RowFetcher, should only be used for
	// initialization, call input.Next() to retrieve rows once initialized.
	fetcher sqlbase.RowFetcher
	alloc   sqlbase.DatumAlloc
}

var _ Processor = &tableReader{}
var _ RowSource = &tableReader{}

const tableReaderProcName = "table reader"

// newTableReader creates a tableReader.
func newTableReader(
	flowCtx *FlowCtx,
	processorID int32,
	spec *TableReaderSpec,
	post *PostProcessSpec,
	output RowReceiver,
) (*tableReader, error) {
	if flowCtx.nodeID == 0 {
		return nil, errors.Errorf("attempting to create a tableReader with uninitialized NodeID")
	}

	tr := &tableReader{
		tableDesc: spec.Table,
	}

	tr.limitHint = limitHint(spec.LimitHint, post)

	types := make([]sqlbase.ColumnType, len(spec.Table.Columns))
	for i := range types {
		types[i] = spec.Table.Columns[i].Type
	}
	if err := tr.init(
		post,
		types,
		flowCtx,
		processorID,
		output,
		procStateOpts{
			// We don't pass tr.input as an inputToDrain; tr.input is just an adapter
			// on top of a RowFetcher; draining doesn't apply to it. Moreover, Andrei
			// doesn't trust that the adapter will do the right thing on a Next() call
			// after it had previously returned an error.
			inputsToDrain:        nil,
			trailingMetaCallback: tr.generateTrailingMeta,
		},
	); err != nil {
		return nil, err
	}

	neededColumns := tr.out.neededColumns()

	if _, _, err := initRowFetcher(
		&tr.fetcher, &tr.tableDesc, int(spec.IndexIdx), tr.tableDesc.ColumnIdxMap(), spec.Reverse,
		neededColumns, spec.IsCheck, &tr.alloc,
	); err != nil {
		return nil, err
	}

	tr.spans = make(roachpb.Spans, len(spec.Spans))
	for i, s := range spec.Spans {
		tr.spans[i] = s.Span
	}
	tr.input = &rowFetcherWrapper{RowFetcher: &tr.fetcher}
	return tr, nil
}

// rowFetcherWrapper is used only by a tableReader to wrap calls to
// RowFetcher.NextRow() in a RowSource implementation.
type rowFetcherWrapper struct {
	ctx context.Context
	*sqlbase.RowFetcher
}

var _ RowSource = &rowFetcherWrapper{}

// Start is part of the RowSource interface.
func (w *rowFetcherWrapper) Start(ctx context.Context) context.Context {
	w.ctx = ctx
	return ctx
}

// Next() calls NextRow() on the underlying RowFetcher. If the returned
// ProducerMetadata is not nil, only its Err field will be set.
func (w *rowFetcherWrapper) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	row, _, _, err := w.NextRow(w.ctx)
	if err != nil {
		return row, &ProducerMetadata{Err: err}
	}
	return row, nil
}

func (w rowFetcherWrapper) OutputTypes() []sqlbase.ColumnType { return nil }
func (w rowFetcherWrapper) ConsumerDone()                     {}
func (w rowFetcherWrapper) ConsumerClosed()                   {}

func initRowFetcher(
	fetcher *sqlbase.RowFetcher,
	desc *sqlbase.TableDescriptor,
	indexIdx int,
	colIdxMap map[sqlbase.ColumnID]int,
	reverseScan bool,
	valNeededForCol util.FastIntSet,
	isCheck bool,
	alloc *sqlbase.DatumAlloc,
) (index *sqlbase.IndexDescriptor, isSecondaryIndex bool, err error) {
	index, isSecondaryIndex, err = desc.FindIndexByIndexIdx(indexIdx)
	if err != nil {
		return nil, false, err
	}

	tableArgs := sqlbase.RowFetcherTableArgs{
		Desc:             desc,
		Index:            index,
		ColIdxMap:        colIdxMap,
		IsSecondaryIndex: isSecondaryIndex,
		Cols:             desc.Columns,
		ValNeededForCol:  valNeededForCol,
	}
	if err := fetcher.Init(
		reverseScan, true /* returnRangeInfo */, isCheck, alloc, tableArgs,
	); err != nil {
		return nil, false, err
	}

	return index, isSecondaryIndex, nil
}

// Run is part of the processor interface.
func (tr *tableReader) Run(ctx context.Context, wg *sync.WaitGroup) {
	if tr.out.output == nil {
		panic("tableReader output not initialized for emitting rows")
	}
	ctx = tr.Start(ctx)
	Run(ctx, tr, tr.out.output)
	if wg != nil {
		wg.Done()
	}
}

func (tr *tableReader) generateTrailingMeta() []ProducerMetadata {
	var trailingMeta []ProducerMetadata
	ranges := misplannedRanges(tr.ctx, tr.fetcher.GetRangeInfo(), tr.flowCtx.nodeID)
	if ranges != nil {
		trailingMeta = append(trailingMeta, ProducerMetadata{Ranges: ranges})
	}

	if tr.flowCtx.txn.Type() == client.LeafTxn {
		txnMeta := tr.flowCtx.txn.GetTxnCoordMeta()
		if txnMeta.Txn.ID != (uuid.UUID{}) {
			trailingMeta = append(trailingMeta, ProducerMetadata{TxnMeta: &txnMeta})
		}
	}
	tr.internalClose()
	return trailingMeta
}

// Start is part of the RowSource interface.
func (tr *tableReader) Start(ctx context.Context) context.Context {
	if tr.flowCtx.txn == nil {
		log.Fatalf(ctx, "tableReader outside of txn")
	}

	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		tr.input = NewInputStatCollector(tr.input)
		tr.finishTrace = tr.outputStatsToTrace
	}

	// Like every processor, the tableReader will have a context with a log tag
	// and a span. The underlying fetcher inherits the proc's span, but not the
	// log tag.
	fetcherCtx := ctx
	ctx = tr.startInternal(ctx, tableReaderProcName)
	if procSpan := opentracing.SpanFromContext(ctx); procSpan != nil {
		fetcherCtx = opentracing.ContextWithSpan(fetcherCtx, procSpan)
	}

	// This call doesn't do much; the real "starting" is below.
	tr.input.Start(fetcherCtx)
	// TODO(radu,andrei,knz): set the traceKV flag when requested by the session.
	if err := tr.fetcher.StartScan(
		fetcherCtx, tr.flowCtx.txn, tr.spans,
		true /* limit batches */, tr.limitHint, false, /* traceKV */
	); err != nil {
		tr.moveToDraining(err)
	}
	return ctx
}

// Next is part of the RowSource interface.
func (tr *tableReader) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	for tr.state == stateRunning {
		row, meta := tr.input.Next()

		if meta != nil {
			return nil, meta
		}
		if row == nil {
			tr.moveToDraining(nil /* err */)
			break
		}

		if outRow := tr.processRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, tr.drainHelper()
}

// ConsumerDone is part of the RowSource interface.
func (tr *tableReader) ConsumerDone() {
	tr.moveToDraining(nil /* err */)
}

// ConsumerClosed is part of the RowSource interface.
func (tr *tableReader) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	tr.internalClose()
}

var _ tracing.SpanStats = &TableReaderStats{}

// Stats implements the SpanStats interface.
func (trs *TableReaderStats) Stats() map[string]string {
	return map[string]string{
		"tablereader.input.rows": fmt.Sprintf("%d", trs.InputStats.NumRows),
	}
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (trs *TableReaderStats) StatsForQueryPlan() []string {
	return []string{fmt.Sprintf("rows read: %d", trs.InputStats.NumRows)}
}

// outputStatsToTrace outputs the collected tableReader stats to the trace. Will
// fail silently if the tableReader is not collecting stats.
func (tr *tableReader) outputStatsToTrace() {
	isc, ok := tr.input.(*InputStatCollector)
	if !ok {
		return
	}
	sp := opentracing.SpanFromContext(tr.ctx)
	if sp == nil {
		return
	}
	tracing.SetSpanStats(sp, &TableReaderStats{InputStats: isc.InputStats})
}
