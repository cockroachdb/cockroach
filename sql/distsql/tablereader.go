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

	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/pkg/errors"
)

// readerBase implements basic code shared by tableReader and joinReader.
type readerBase struct {
	flowCtx *FlowCtx
	desc    sqlbase.TableDescriptor

	index            *sqlbase.IndexDescriptor
	isSecondaryIndex bool

	fetcher sqlbase.RowFetcher

	filter exprHelper

	// colIdxMap maps ColumnIDs to indices into desc.Columns
	colIdxMap map[sqlbase.ColumnID]int

	outputCols []int

	// Last row returned by the rowFetcher; it has one entry per table column.
	row      sqlbase.EncDatumRow
	rowAlloc sqlbase.EncDatumRowAlloc
}

func (rb *readerBase) init(
	flowCtx *FlowCtx,
	desc *sqlbase.TableDescriptor,
	indexIdx int,
	filter Expression,
	outputCols []uint32,
	reverseScan bool,
) error {
	rb.flowCtx = flowCtx
	rb.desc = *desc

	rb.outputCols = make([]int, len(outputCols))
	for i, v := range outputCols {
		rb.outputCols[i] = int(v)
	}

	numCols := len(rb.desc.Columns)

	// Figure out which columns we need: the output columns plus any other
	// columns used by the filter expression.
	valNeededForCol := make([]bool, numCols)
	for _, c := range rb.outputCols {
		if c < 0 || c >= numCols {
			return errors.Errorf("invalid column index %d", c)
		}
		valNeededForCol[c] = true
	}

	if filter.Expr != "" {
		types := make([]sqlbase.ColumnType_Kind, numCols)
		for i := range types {
			types[i] = rb.desc.Columns[i].Type.Kind
		}
		if err := rb.filter.init(filter, types, flowCtx.evalCtx); err != nil {
			return err
		}
		for c := 0; c < numCols; c++ {
			valNeededForCol[c] = valNeededForCol[c] || rb.filter.vars.IndexedVarUsed(c)
		}
	}

	// indexIdx is 0 for the primary index, or 1 to <num-indexes> for a
	// secondary index.
	if indexIdx < 0 || indexIdx > len(rb.desc.Indexes) {
		return errors.Errorf("invalid indexIdx %d", indexIdx)
	}

	if indexIdx > 0 {
		rb.index = &rb.desc.Indexes[indexIdx-1]
		rb.isSecondaryIndex = true
	} else {
		rb.index = &rb.desc.PrimaryIndex
	}

	rb.colIdxMap = make(map[sqlbase.ColumnID]int, len(rb.desc.Columns))
	for i, c := range rb.desc.Columns {
		rb.colIdxMap[c.ID] = i
	}
	err := rb.fetcher.Init(&rb.desc, rb.colIdxMap, rb.index, reverseScan, rb.isSecondaryIndex,
		rb.desc.Columns, valNeededForCol)
	if err != nil {
		return err
	}
	rb.row = make(sqlbase.EncDatumRow, len(rb.desc.Columns))
	return nil
}

// nextRow processes table rows until it finds a row that passes the filter.
// Returns a nil row when there are no more rows.
func (rb *readerBase) nextRow() (sqlbase.EncDatumRow, error) {
	for {
		fetcherRow, err := rb.fetcher.NextRow()
		if err != nil || fetcherRow == nil {
			return nil, err
		}

		// TODO(radu): we are defeating the purpose of EncDatum here - we
		// should modify RowFetcher to return EncDatums directly and avoid
		// the cost of decoding/reencoding.
		for i := range fetcherRow {
			if fetcherRow[i] != nil {
				rb.row[i].SetDatum(rb.desc.Columns[i].Type.Kind, fetcherRow[i])
			}
		}
		passesFilter, err := rb.filter.evalFilter(rb.row)
		if err != nil {
			return nil, err
		}
		if passesFilter {
			break
		}
	}
	outRow := rb.rowAlloc.AllocRow(len(rb.outputCols))
	for i, col := range rb.outputCols {
		outRow[i] = rb.row[col]
	}
	return outRow, nil
}

// tableReader is the start of a computation flow; it performs KV operations to
// retrieve rows for a table, runs a filter expression, and passes rows with the
// desired column values to an output RowReceiver.
// See docs/RFCS/distributed_sql.md
type tableReader struct {
	readerBase

	ctx context.Context

	spans                sqlbase.Spans
	hardLimit, softLimit int64

	output RowReceiver
}

var _ processor = &tableReader{}

// newTableReader creates a tableReader.
func newTableReader(
	flowCtx *FlowCtx, spec *TableReaderSpec, output RowReceiver) (*tableReader, error,
) {
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

	tr.spans = make(sqlbase.Spans, len(spec.Spans))
	for i, s := range spec.Spans {
		tr.spans[i] = sqlbase.Span{Start: s.Span.Key, End: s.Span.EndKey}
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

	if log.V(2) {
		log.Infof(tr.ctx, "starting (filter: %s)", tr.filter)
		defer log.Infof(tr.ctx, "exiting")
	}

	if err := tr.fetcher.StartScan(tr.flowCtx.txn, tr.spans, tr.getLimitHint()); err != nil {
		log.Errorf(tr.ctx, "scan error: %s", err)
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
			log.Infof(tr.ctx, "pushing row %s\n", outRow)
		}
		// Push the row to the output RowReceiver; stop if they don't need more
		// rows.
		if !tr.output.PushRow(outRow) {
			if log.V(2) {
				log.Infof(tr.ctx, "no more rows required")
			}
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
