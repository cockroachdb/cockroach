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

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// tableReader is the start of a computation flow; it performs KV operations to
// retrieve rows for a table, runs a filter expression, and passes rows with the
// desired column values to an output rowReceiver.
// See docs/RFCS/distributed_sql.md
type tableReader struct {
	desc                 sqlbase.TableDescriptor
	spans                sqlbase.Spans
	outputCols           []int
	hardLimit, softLimit int64

	output rowReceiver

	filter exprHelper

	txn     *client.Txn
	fetcher sqlbase.RowFetcher
	// Last row returned by the rowFetcher; it has one entry per table column.
	row      sqlbase.EncDatumRow
	rowAlloc sqlbase.EncDatumRowAlloc
}

var _ processor = &tableReader{}

// newTableReader creates a tableReader.
func newTableReader(
	spec *TableReaderSpec, txn *client.Txn, output rowReceiver, evalCtx *parser.EvalContext,
) (*tableReader, error) {
	tr := &tableReader{
		desc:      spec.Table,
		txn:       txn,
		output:    output,
		hardLimit: spec.HardLimit,
		softLimit: spec.SoftLimit,
	}

	if tr.hardLimit != 0 && tr.hardLimit < tr.softLimit {
		return nil, util.Errorf("soft limit %d larger than hard limit %d", tr.softLimit,
			tr.hardLimit)
	}

	numCols := len(tr.desc.Columns)

	tr.outputCols = make([]int, len(spec.OutputColumns))
	for i, v := range spec.OutputColumns {
		tr.outputCols[i] = int(v)
	}

	// Figure out which columns we need: the output columns plus any other
	// columns used by the filter expression.
	valNeededForCol := make([]bool, numCols)
	for _, c := range tr.outputCols {
		if c < 0 || c >= numCols {
			return nil, util.Errorf("invalid column index %d", c)
		}
		valNeededForCol[c] = true
	}

	if spec.Filter.Expr != "" {
		types := make([]sqlbase.ColumnType_Kind, numCols)
		for i := range types {
			types[i] = tr.desc.Columns[i].Type.Kind
		}
		if err := tr.filter.init(spec.Filter, types, evalCtx); err != nil {
			return nil, err
		}
		for c := 0; c < numCols; c++ {
			valNeededForCol[c] = valNeededForCol[c] || tr.filter.vars.IndexedVarUsed(c)
		}
	}

	if spec.IndexIdx > uint32(len(tr.desc.Indexes)) {
		return nil, util.Errorf("invalid IndexIdx %d", spec.IndexIdx)
	}

	var index *sqlbase.IndexDescriptor
	var isSecondaryIndex bool

	if spec.IndexIdx > 0 {
		index = &tr.desc.Indexes[spec.IndexIdx-1]
		isSecondaryIndex = true
	} else {
		index = &tr.desc.PrimaryIndex
	}

	colIdxMap := make(map[sqlbase.ColumnID]int, len(tr.desc.Columns))
	for i, c := range tr.desc.Columns {
		colIdxMap[c.ID] = i
	}
	err := tr.fetcher.Init(&tr.desc, colIdxMap, index, spec.Reverse, isSecondaryIndex,
		valNeededForCol)
	if err != nil {
		return nil, err
	}

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

// nextRow processes table rows until it finds a row that passes the filter.
// Returns a nil row when there are no more rows.
func (tr *tableReader) nextRow() (sqlbase.EncDatumRow, error) {
	for {
		fetcherRow, err := tr.fetcher.NextRow()
		if err != nil || fetcherRow == nil {
			return nil, err
		}

		// TODO(radu): we are defeating the purpose of EncDatum here - we
		// should modify RowFetcher to return EncDatums directly and avoid
		// the cost of decoding/reencoding.
		for i := range fetcherRow {
			if fetcherRow[i] != nil {
				tr.row[i].SetDatum(tr.desc.Columns[i].Type.Kind, fetcherRow[i])
			}
		}
		passesFilter, err := tr.filter.evalFilter(tr.row)
		if err != nil {
			return nil, err
		}
		if passesFilter {
			break
		}
	}
	outRow := tr.rowAlloc.AllocRow(len(tr.outputCols))
	for i, col := range tr.outputCols {
		outRow[i] = tr.row[col]
	}
	return outRow, nil
}

// Run is part of the processor interface.
func (tr *tableReader) Run(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	if log.V(1) {
		log.Infof("TableReader filter: %s\n", tr.filter.expr)
	}

	if err := tr.fetcher.StartScan(tr.txn, tr.spans, tr.getLimitHint()); err != nil {
		tr.output.Close(err)
		return
	}
	tr.row = make(sqlbase.EncDatumRow, len(tr.desc.Columns))
	var rowIdx int64
	for {
		outRow, err := tr.nextRow()
		if err != nil || outRow == nil {
			tr.output.Close(err)
			return
		}
		// Push the row to the output rowReceiver; stop if they don't need more
		// rows.
		if !tr.output.PushRow(outRow) {
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
