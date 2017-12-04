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
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// tableReader is the start of a computation flow; it performs KV operations to
// retrieve rows for a table, runs a filter expression, and passes rows with the
// desired column values to an output RowReceiver.
// See docs/RFCS/distributed_sql.md
type tableReader struct {
	processorBase

	flowCtx *FlowCtx

	tableID   sqlbase.ID
	spans     roachpb.Spans
	limitHint int64

	fetcher sqlbase.MultiRowFetcher
	alloc   sqlbase.DatumAlloc

	isCheck bool
}

var _ Processor = &tableReader{}

// newTableReader creates a tableReader.
func newTableReader(
	flowCtx *FlowCtx, spec *TableReaderSpec, post *PostProcessSpec, output RowReceiver,
) (*tableReader, error) {
	if flowCtx.nodeID == 0 {
		return nil, errors.Errorf("attempting to create a tableReader with uninitialized NodeID")
	}
	tr := &tableReader{
		flowCtx: flowCtx,
		tableID: spec.Table.ID,
		isCheck: spec.IsCheck,
	}

	tr.limitHint = limitHint(spec.LimitHint, post)

	types := make([]sqlbase.ColumnType, len(spec.Table.Columns))
	for i := range types {
		types[i] = spec.Table.Columns[i].Type
	}
	if err := tr.init(post, types, flowCtx, output); err != nil {
		return nil, err
	}

	desc := spec.Table
	if _, _, err := initRowFetcher(
		&tr.fetcher, &desc, int(spec.IndexIdx), spec.Reverse,
		tr.out.neededColumns(), spec.IsCheck, &tr.alloc,
	); err != nil {
		return nil, err
	}

	tr.spans = make(roachpb.Spans, len(spec.Spans))
	for i, s := range spec.Spans {
		tr.spans[i] = s.Span
	}

	return tr, nil
}

func initRowFetcher(
	fetcher *sqlbase.MultiRowFetcher,
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

	tableArgs := sqlbase.MultiRowFetcherTableArgs{
		Desc:             desc,
		Index:            index,
		ColIdxMap:        colIdxMap,
		IsSecondaryIndex: isSecondaryIndex,
		Cols:             desc.Columns,
		ValNeededForCol:  valNeededForCol,
	}
	if err := fetcher.Init(
		reverseScan, true /* returnRangeInfo */, false /* isCheck */, alloc, tableArgs,
	); err != nil {
		return nil, false, err
	}

	return index, isSecondaryIndex, nil
}

// sendMisplannedRangesMetadata sends information about the non-local ranges
// that were read by this tableReader. This should be called after the fetcher
// was used to read everything this tableReader was supposed to read.
func (tr *tableReader) sendMisplannedRangesMetadata(ctx context.Context) {
	misplannedRanges := misplannedRanges(ctx, tr.fetcher.GetRangeInfo(), tr.flowCtx.nodeID)

	if len(misplannedRanges) != 0 {
		tr.out.output.Push(nil /* row */, ProducerMetadata{Ranges: misplannedRanges})
	}
}

// Run is part of the processor interface.
func (tr *tableReader) Run(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx = log.WithLogTagInt(ctx, "TableReader", int(tr.tableID))
	ctx, span := processorSpan(ctx, "table reader")
	defer tracing.FinishSpan(span)

	txn := tr.flowCtx.txn
	if txn == nil {
		log.Fatalf(ctx, "tableReader outside of txn")
	}

	log.VEventf(ctx, 1, "starting")
	if log.V(1) {
		defer log.Infof(ctx, "exiting")
	}

	// TODO(radu,andrei,knz): set the traceKV flag when requested by the session.
	if err := tr.fetcher.StartScan(
		ctx, txn, tr.spans, true /* limit batches */, tr.limitHint, false, /* traceKV */
	); err != nil {
		log.Errorf(ctx, "scan error: %s", err)
		tr.out.output.Push(nil /* row */, ProducerMetadata{Err: err})
		tr.out.Close()
		return
	}

	for {
		row, _, _, err := tr.fetcher.NextRow(ctx)
		if err != nil || row == nil {
			if err != nil {
				if scrub.IsScrubError(err) {
					err = scrub.UnwrapScrubError(err)
				}
				tr.out.output.Push(nil /* row */, ProducerMetadata{Err: err})
			}
			break
		}
		// Emit the row; stop if no more rows are needed.
		consumerStatus, err := tr.out.EmitRow(ctx, row)
		if err != nil || consumerStatus != NeedMoreRows {
			if err != nil {
				tr.out.output.Push(nil /* row */, ProducerMetadata{Err: err})
			}
			break
		}
	}
	tr.sendMisplannedRangesMetadata(ctx)
	sendTraceData(ctx, tr.out.output)
	tr.out.Close()
}
