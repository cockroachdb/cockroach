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
	"sync"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// tableReader is the start of a computation flow; it performs KV operations to
// retrieve rows for a table, runs a filter expression, and passes rows with the
// desired column values to an output RowReceiver.
// See docs/RFCS/distributed_sql.md
type tableReader struct {
	processorBase
	rowSourceBase

	tableDesc sqlbase.TableDescriptor
	spans     roachpb.Spans
	limitHint int64

	input RowSource
	// fetcher is the underlying RowFetcher, should only be used for
	// initialization, call input.Next() to retrieve rows once initialized.
	fetcher sqlbase.RowFetcher
	alloc   sqlbase.DatumAlloc

	// trailingMetadata contains producer metadata that is sent once the consumer
	// status is not NeedMoreRows.
	trailingMetadata []ProducerMetadata
}

var _ Processor = &tableReader{}
var _ RowSource = &tableReader{}

// newTableReader creates a tableReader.
func newTableReader(
	flowCtx *FlowCtx, spec *TableReaderSpec, post *PostProcessSpec, output RowReceiver,
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
	if err := tr.init(post, types, flowCtx, nil /* evalCtx */, output); err != nil {
		return nil, err
	}

	neededColumns := tr.out.neededColumns()

	if _, _, err := initRowFetcher(
		&tr.fetcher, &tr.tableDesc, int(spec.IndexIdx), spec.Reverse,
		neededColumns, spec.IsCheck, &tr.alloc,
	); err != nil {
		return nil, err
	}

	tr.spans = make(roachpb.Spans, len(spec.Spans))
	for i, s := range spec.Spans {
		tr.spans[i] = s.Span
	}

	return tr, nil
}

// rowFetcherWrapper is used only by a tableReader to wrap calls to
// RowFetcher.NextRow() in a RowSource implementation.
type rowFetcherWrapper struct {
	ctx context.Context
	*sqlbase.RowFetcher
}

var _ RowSource = rowFetcherWrapper{}

// Next() calls NextRow() on the underlying RowFetcher. If the returned
// ProducerMetadata is not nil, only its Err field will be set.
func (w rowFetcherWrapper) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
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
	reverseScan bool,
	valNeededForCol util.FastIntSet,
	isCheck bool,
	alloc *sqlbase.DatumAlloc,
) (index *sqlbase.IndexDescriptor, isSecondaryIndex bool, err error) {
	index, isSecondaryIndex, err = desc.FindIndexByIndexIdx(indexIdx)
	if err != nil {
		return nil, false, err
	}

	colIdxMap := make(map[sqlbase.ColumnID]int, len(desc.Columns))
	for i, c := range desc.Columns {
		colIdxMap[c.ID] = i
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
func (tr *tableReader) Run(wg *sync.WaitGroup) {
	if tr.out.output == nil {
		panic("tableReader output not initialized for emitting rows")
	}
	Run(tr.flowCtx.Ctx, tr, tr.out.output)
	if wg != nil {
		wg.Done()
	}
}

// close the tableReader and finish any tracing. Any subsequent calls to Next()
// will return empty data.
func (tr *tableReader) close() {
	tr.internalClose()
}

// producerMeta constructs the ProducerMetadata after consumption of rows has
// terminated, either due to being indicated by the consumer, or because the
// processor ran out of rows or encountered an error. It is ok for err to be
// nil indicating that we're done producing rows even though no error occurred.
func (tr *tableReader) producerMeta(err error) *ProducerMetadata {
	if !tr.closed {
		if err != nil {
			tr.trailingMetadata = append(tr.trailingMetadata, ProducerMetadata{Err: err})
		}
		ranges := misplannedRanges(tr.ctx, tr.fetcher.GetRangeInfo(), tr.flowCtx.nodeID)
		if ranges != nil {
			tr.trailingMetadata = append(tr.trailingMetadata, ProducerMetadata{Ranges: ranges})
		}
		traceData := getTraceData(tr.ctx)
		if traceData != nil {
			tr.trailingMetadata = append(tr.trailingMetadata, ProducerMetadata{TraceData: traceData})
		}
		if tr.flowCtx.txn.Type() == client.LeafTxn {
			txnMeta := tr.flowCtx.txn.GetTxnCoordMeta()
			if txnMeta.Txn.ID != (uuid.UUID{}) {
				tr.trailingMetadata = append(tr.trailingMetadata, ProducerMetadata{TxnMeta: &txnMeta})
			}
		}
		tr.close()
	}
	if len(tr.trailingMetadata) > 0 {
		meta := &tr.trailingMetadata[0]
		tr.trailingMetadata = tr.trailingMetadata[1:]
		return meta
	}
	return nil
}

// Next is part of the RowSource interface.
func (tr *tableReader) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	if tr.maybeStart("table reader", "TableReader") {
		if tr.flowCtx.txn == nil {
			log.Fatalf(tr.ctx, "tableReader outside of txn")
		}
		log.VEventf(tr.ctx, 1, "starting")

		// TODO(radu,andrei,knz): set the traceKV flag when requested by the session.
		if err := tr.fetcher.StartScan(
			tr.ctx, tr.flowCtx.txn, tr.spans,
			true /* limit batches */, tr.limitHint, false, /* traceKV */
		); err != nil {
			log.Eventf(tr.ctx, "scan error: %s", err)
			return nil, tr.producerMeta(err)
		}

		tr.input = rowFetcherWrapper{ctx: tr.ctx, RowFetcher: &tr.fetcher}
	}

	if tr.closed || tr.consumerStatus != NeedMoreRows {
		return nil, tr.producerMeta(nil /* err */)
	}

	for {
		row, meta := tr.input.Next()
		var err error
		if meta != nil {
			err = meta.Err
		}
		if row == nil || err != nil {
			// This was the last-row or an error was encountered, annotate the
			// metadata with misplanned ranges and trace data.
			return nil, tr.producerMeta(scrub.UnwrapScrubError(err))
		}

		outRow, status, err := tr.out.ProcessRow(tr.ctx, row)
		if outRow != nil {
			return outRow, nil
		}
		if outRow == nil && err == nil && status == NeedMoreRows {
			continue
		}
		return nil, tr.producerMeta(err)
	}
}

// ConsumerDone is part of the RowSource interface.
func (tr *tableReader) ConsumerDone() {
	tr.consumerDone()
}

// ConsumerClosed is part of the RowSource interface.
func (tr *tableReader) ConsumerClosed() {
	tr.consumerClosed("tableReader")
	// The consumer is done, Next() will not be called again.
	tr.close()
}
