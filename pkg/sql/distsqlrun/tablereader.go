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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

// tableReader is the start of a computation flow; it performs KV operations to
// retrieve rows for a table, runs a filter expression, and passes rows with the
// desired column values to an output RowReceiver.
// See docs/RFCS/distributed_sql.md
type tableReader struct {
	ProcessorBase

	spans     roachpb.Spans
	limitHint int64

	ignoreMisplannedRanges bool

	// input is really the fetcher below, possibly wrapped in a stats generator.
	input RowSource
	// fetcher is the underlying Fetcher, should only be used for
	// initialization, call input.Next() to retrieve rows once initialized.
	fetcher row.Fetcher
	alloc   sqlbase.DatumAlloc
}

var _ Processor = &tableReader{}
var _ RowSource = &tableReader{}

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

	numCols := len(spec.Table.Columns)
	returnMutations := spec.Visibility == distsqlpb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC
	if returnMutations {
		for i := range spec.Table.Mutations {
			if spec.Table.Mutations[i].GetColumn() != nil {
				numCols++
			}
		}
	}
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

	columnIdxMap := spec.Table.ColumnIdxMapWithMutations(returnMutations)
	if _, _, err := initRowFetcher(
		&tr.fetcher, &spec.Table, int(spec.IndexIdx), columnIdxMap, spec.Reverse,
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
	tr.input = &rowFetcherWrapper{Fetcher: &tr.fetcher}

	if sp := opentracing.SpanFromContext(flowCtx.EvalCtx.Ctx()); sp != nil && tracing.IsRecording(sp) {
		tr.input = NewInputStatCollector(tr.input)
		tr.finishTrace = tr.outputStatsToTrace
	}

	return tr, nil
}

// rowFetcherWrapper is used only by a tableReader to wrap calls to
// Fetcher.NextRow() in a RowSource implementation.
type rowFetcherWrapper struct {
	ctx context.Context
	*row.Fetcher
}

var _ RowSource = &rowFetcherWrapper{}

// Start is part of the RowSource interface.
func (w *rowFetcherWrapper) Start(ctx context.Context) context.Context {
	w.ctx = ctx
	return ctx
}

// Next() calls NextRow() on the underlying Fetcher. If the returned
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
	index, isSecondaryIndex, err = desc.FindIndexByIndexIdx(indexIdx)
	if err != nil {
		return nil, false, err
	}

	cols := desc.Columns
	if scanVisibility == distsqlpb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC {
		if len(desc.Mutations) > 0 {
			cols = make([]sqlbase.ColumnDescriptor, 0, len(desc.Columns)+len(desc.Mutations))
			cols = append(cols, desc.Columns...)
			for _, mutation := range desc.Mutations {
				if c := mutation.GetColumn(); c != nil {
					col := *c
					// Even if the column is non-nullable it can be null in the
					// middle of a schema change.
					col.Nullable = true
					cols = append(cols, col)
				}
			}
		}
	}
	tableArgs := row.FetcherTableArgs{
		Desc:             sqlbase.NewImmutableTableDescriptor(*desc),
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

func (tr *tableReader) generateTrailingMeta(ctx context.Context) []ProducerMetadata {
	var trailingMeta []ProducerMetadata
	if !tr.ignoreMisplannedRanges {
		ranges := misplannedRanges(tr.Ctx, tr.fetcher.GetRangeInfo(), tr.flowCtx.nodeID)
		if ranges != nil {
			trailingMeta = append(trailingMeta, ProducerMetadata{Ranges: ranges})
		}
	}
	if meta := getTxnCoordMeta(ctx, tr.flowCtx.txn); meta != nil {
		trailingMeta = append(trailingMeta, ProducerMetadata{TxnCoordMeta: meta})
	}
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
	tr.input.Start(fetcherCtx)
	if err := tr.fetcher.StartScan(
		fetcherCtx, tr.flowCtx.txn, tr.spans,
		true /* limit batches */, tr.limitHint, tr.flowCtx.traceKV,
	); err != nil {
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
	}
	trPool.Put(tr)
}

// Next is part of the RowSource interface.
func (tr *tableReader) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	for tr.State == StateRunning {
		row, meta := tr.input.Next()

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
	return trs.InputStats.Stats(tableReaderTagPrefix)
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (trs *TableReaderStats) StatsForQueryPlan() []string {
	return trs.InputStats.StatsForQueryPlan("" /* prefix */)
}

// outputStatsToTrace outputs the collected tableReader stats to the trace. Will
// fail silently if the tableReader is not collecting stats.
func (tr *tableReader) outputStatsToTrace() {
	is, ok := getInputStats(tr.flowCtx, tr.input)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(tr.Ctx); sp != nil {
		tracing.SetSpanStats(sp, &TableReaderStats{InputStats: is})
	}
}
