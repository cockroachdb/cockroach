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
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package distsqlrun

import (
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

// tableReader is the start of a computation flow; it performs KV operations to
// retrieve rows for a table, runs a filter expression, and passes rows with the
// desired column values to an output RowReceiver.
// See docs/RFCS/distributed_sql.md
type tableReader struct {
	flowCtx *FlowCtx
	ctx     context.Context

	spans     roachpb.Spans
	limitHint int64

	fetcher sqlbase.RowFetcher

	out procOutputHelper
}

var _ processor = &tableReader{}

// newTableReader creates a tableReader.
func newTableReader(
	flowCtx *FlowCtx, spec *TableReaderSpec, post *PostProcessSpec, output RowReceiver,
) (*tableReader, error) {
	tr := &tableReader{
		flowCtx: flowCtx,
	}

	// We ignore any limits that are higher than this value to avoid any
	// overflows.
	const overflowProtection = 1000000000
	if post.Limit != 0 && post.Limit <= overflowProtection {
		// In this case the procOutputHelper will tell us to stop once we emit
		// enough rows.
		tr.limitHint = int64(post.Limit)
	} else if spec.LimitHint != 0 && spec.LimitHint <= overflowProtection {
		// Even if the receiver needs only a certain number of rows, we will be able
		// to push more rows into a RowChannel before we block; so increase the
		// estimate by the size of the RowChannel buffer.
		//
		// TODO(radu): if there are multiple processors and RowChannels between us
		// and whatever processor only needs a certain number of rows, we could emit
		// enough rows to fill all those channels. Experiment with this and perhaps
		// find a better mechanism for limits.
		tr.limitHint = spec.LimitHint + rowChannelBufSize
	}

	if post.Filter.Expr != "" {
		// We have a filter so we will likely need to read more rows.
		tr.limitHint *= 2
	}

	types := make([]sqlbase.ColumnType, len(spec.Table.Columns))
	for i := range types {
		types[i] = spec.Table.Columns[i].Type
	}
	if err := tr.out.init(post, types, flowCtx.evalCtx, output); err != nil {
		return nil, err
	}

	desc := spec.Table
	if _, _, err := initRowFetcher(
		&tr.fetcher, &desc, int(spec.IndexIdx), spec.Reverse, tr.out.neededColumns(),
	); err != nil {
		return nil, err
	}

	tr.ctx = log.WithLogTagInt(tr.flowCtx.Context, "TableReader", int(spec.Table.ID))

	tr.spans = make(roachpb.Spans, len(spec.Spans))
	for i, s := range spec.Spans {
		tr.spans[i] = s.Span
	}

	return tr, nil
}

func initRowFetcher(
	fetcher *sqlbase.RowFetcher,
	desc *sqlbase.TableDescriptor,
	indexIdx int,
	reverseScan bool,
	valNeededForCol []bool,
) (index *sqlbase.IndexDescriptor, isSecondaryIndex bool, err error) {
	// indexIdx is 0 for the primary index, or 1 to <num-indexes> for a
	// secondary index.
	if indexIdx < 0 || indexIdx > len(desc.Indexes) {
		return nil, false, errors.Errorf("invalid indexIdx %d", indexIdx)
	}

	if indexIdx > 0 {
		index = &desc.Indexes[indexIdx-1]
		isSecondaryIndex = true
	} else {
		index = &desc.PrimaryIndex
	}

	colIdxMap := make(map[sqlbase.ColumnID]int, len(desc.Columns))
	for i, c := range desc.Columns {
		colIdxMap[c.ID] = i
	}
	if err := fetcher.Init(
		desc, colIdxMap, index, reverseScan, isSecondaryIndex,
		desc.Columns, valNeededForCol,
	); err != nil {
		return nil, false, err
	}
	return index, isSecondaryIndex, nil
}

// Run is part of the processor interface.
func (tr *tableReader) Run(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx, span := tracing.ChildSpan(tr.ctx, "table reader")
	defer tracing.FinishSpan(span)

	txn := tr.flowCtx.setupTxn(ctx)

	log.VEventf(ctx, 1, "starting")
	if log.V(1) {
		defer log.Infof(ctx, "exiting")
	}

	if err := tr.fetcher.StartScan(
		txn, tr.spans, true /* limit batches */, tr.limitHint,
	); err != nil {
		log.Errorf(ctx, "scan error: %s", err)
		tr.out.close(err)
		return
	}

	for {
		fetcherRow, err := tr.fetcher.NextRow()
		if err != nil || fetcherRow == nil {
			tr.out.close(err)
			return
		}
		// Emit the row; stop if no more rows are needed.
		if !tr.out.emitRow(ctx, fetcherRow) {
			tr.out.close(nil)
			return
		}
		/*
			 * TODO(radu): support limit in procOutputHelper
			rowIdx++
			if tr.hardLimit != 0 && rowIdx == tr.hardLimit {
				// We sent tr.hardLimit rows.
				tr.output.Close(nil)
				return
			}
		*/
	}
}
