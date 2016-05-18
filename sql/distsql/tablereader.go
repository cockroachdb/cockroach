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
	desc       sqlbase.TableDescriptor
	spans      sqlbase.Spans
	outputCols []int

	output rowReceiver

	filter parser.TypedExpr
	// filterVars is used to generate IndexedVars that are "backed" by the
	// values in the row tuple.
	filterVars parser.IndexedVarHelper

	evalCtx parser.EvalContext
	txn     *client.Txn
	fetcher sqlbase.RowFetcher
	// Last row returned by the rowFetcher; it has one entry per table column.
	row   row
	alloc sqlbase.DatumAlloc
}

// tableReader implements parser.IndexedVarContainer.
var _ parser.IndexedVarContainer = &tableReader{}

// IndexedVarReturnType is part of the parser.IndexedVarContainer interface.
func (tr *tableReader) IndexedVarReturnType(idx int) parser.Datum {
	return tr.desc.Columns[idx].Type.ToDatumType()
}

// IndexedVarEval is part of the parser.IndexedVarContainer interface.
func (tr *tableReader) IndexedVarEval(idx int, ctx parser.EvalContext) (parser.Datum, error) {
	err := tr.row[idx].Decode(&tr.alloc)
	if err != nil {
		return nil, err
	}
	return tr.row[idx].Datum.Eval(ctx)
}

// IndexedVarString is part of the parser.IndexedVarContainer interface.
func (tr *tableReader) IndexedVarString(idx int) string {
	return string(tr.desc.Columns[idx].Name)
}

// newTableReader creates a tableReader.
func newTableReader(
	spec *TableReaderSpec, txn *client.Txn, output rowReceiver, evalCtx parser.EvalContext,
) (*tableReader, error) {
	tr := &tableReader{
		desc:    spec.Table,
		txn:     txn,
		output:  output,
		evalCtx: evalCtx,
	}

	numCols := len(tr.desc.Columns)

	tr.outputCols = make([]int, len(spec.OutputColumns))
	for i, v := range spec.OutputColumns {
		tr.outputCols[i] = int(v)
	}

	tr.filterVars = parser.MakeIndexedVarHelper(tr, numCols)
	var err error
	tr.filter, err = processExpression(spec.Filter, &tr.filterVars)
	if err != nil {
		return nil, err
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
	for c := 0; c < numCols; c++ {
		valNeededForCol[c] = valNeededForCol[c] || tr.filterVars.IndexedVarUsed(c)
	}

	var index *sqlbase.IndexDescriptor
	var isSecondaryIndex bool
	switch {
	case spec.IndexIdx == 0:
		index = &tr.desc.PrimaryIndex
	case spec.IndexIdx <= uint32(len(tr.desc.Indexes)):
		index = &tr.desc.Indexes[spec.IndexIdx-1]
		isSecondaryIndex = true
	default:
		return nil, util.Errorf("Invalid indexIdx %d", spec.IndexIdx)
	}

	colIdxMap := make(map[sqlbase.ColumnID]int, len(tr.desc.Columns))
	for i, c := range tr.desc.Columns {
		colIdxMap[c.ID] = i
	}
	err = tr.fetcher.Init(&tr.desc, colIdxMap, index, spec.Reverse, isSecondaryIndex,
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

// nextRow processes table rows until it finds a row that passes the filter.
// Returns a nil row when there are no more rows.
func (tr *tableReader) nextRow() (row, error) {
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
		passesFilter, err := sqlbase.RunFilter(tr.filter, tr.evalCtx)
		if err != nil {
			return nil, err
		}
		if passesFilter {
			break
		}
	}
	// TODO(radu): investigate removing this allocation. We can't reuse the
	// same slice because it is being read asynchronously on the other side
	// of the channel. Perhaps streamMsg can store a few preallocated
	// elements to avoid allocation in most cases.
	outRow := make(row, len(tr.outputCols))
	for i, col := range tr.outputCols {
		outRow[i] = tr.row[col]
	}
	return outRow, nil
}

// run is the "main loop".
func (tr *tableReader) run() {
	if log.V(1) {
		log.Infof("TableReader filter: %s\n", tr.filter)
	}
	if err := tr.fetcher.StartScan(tr.txn, tr.spans, 0); err != nil {
		tr.output.Close(err)
		return
	}
	tr.row = make(row, len(tr.desc.Columns))
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
	}
}
