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

package distsql

import (
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

// tableReader is the start of a computation flow; it performs KV operations to
// retrieve rows for a table, runs a filter expression, and passes rows with the
// desired column values to an output RowReceiver.
// See docs/RFCS/distributed_sql.md
type tableReader struct {
	readerBase

	ctx context.Context

	spans                roachpb.Spans
	hardLimit, softLimit int64

	output RowReceiver
}

var _ processor = &tableReader{}

// newTableReader creates a tableReader.
func newTableReader(
	flowCtx *FlowCtx, spec *TableReaderSpec, output RowReceiver,
) (*tableReader, error) {
	tr := &tableReader{
		output:    output,
		hardLimit: spec.HardLimit,
		softLimit: spec.SoftLimit,
	}

	if tr.hardLimit != 0 && tr.hardLimit < tr.softLimit {
		return nil, errors.Errorf("soft limit %d larger than hard limit %d", tr.softLimit,
			tr.hardLimit)
	}

	err := tr.readerBase.init(flowCtx, &spec.Table, int(spec.IndexIdx), spec.Filter,
		spec.OutputColumns, spec.Reverse)
	if err != nil {
		return nil, err
	}

	tr.ctx = log.WithLogTagInt(tr.flowCtx.Context, "TableReader", int(tr.desc.ID))

	tr.spans = make(roachpb.Spans, len(spec.Spans))
	for i, s := range spec.Spans {
		tr.spans[i] = s.Span
	}

	return tr, nil
}

// getLimitHint calculates the row limit hint for the row fetcher.
func (tr *tableReader) getLimitHint() int64 {
	softLimit := tr.softLimit
	if tr.hardLimit != 0 {
		if tr.filter.expr == nil {
			return tr.hardLimit
		}
		// If we have a filter, we don't know how many rows will pass the filter
		// so the hard limit is actually a "soft" limit at the row fetcher.
		if softLimit == 0 {
			softLimit = tr.hardLimit
		}
	}
	// If the limit is soft, we request a multiple of the limit.
	// If the limit is 0 (no limit), we must return 0.
	return softLimit * 2
}

// Run is part of the processor interface.
func (tr *tableReader) Run(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx, span := tracing.ChildSpan(tr.ctx, "table reader")
	defer tracing.FinishSpan(span)

	log.VEventf(ctx, 1, "starting (filter: %s)", &tr.filter)
	if log.V(1) {
		defer log.Infof(ctx, "exiting")
	}

	if err := tr.fetcher.StartScan(
		tr.flowCtx.txn, tr.spans, true /* limit batches */, tr.getLimitHint(),
	); err != nil {
		log.Errorf(ctx, "scan error: %s", err)
		tr.output.Close(err)
		return
	}
	var rowIdx int64
	for {
		outRow, err := tr.nextRow()
		if err != nil || outRow == nil {
			tr.output.Close(err)
			return
		}
		if log.V(3) {
			log.Infof(ctx, "pushing row %s\n", outRow)
		}
		// Push the row to the output RowReceiver; stop if they don't need more
		// rows.
		if !tr.output.PushRow(outRow) {
			log.VEventf(ctx, 1, "no more rows required")
			tr.output.Close(nil)
			return
		}
		rowIdx++
		if tr.hardLimit != 0 && rowIdx == tr.hardLimit {
			// We sent tr.hardLimit rows.
			tr.output.Close(nil)
			return
		}
	}
}
