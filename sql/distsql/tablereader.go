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
	"fmt"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// TableReader is the start of a computation flow; it performs KV operations to
// retrieve rows for a table and outputs the desired columns of the rows that
// pass a filter expression.
// See docs/RFCS/distributed_sql.md
type TableReader struct {
	desc       sqlbase.TableDescriptor
	spans      sqlbase.Spans
	outputCols []int

	filter parser.TypedExpr
	// filterVars is used to generate IndexedVars that are "backed" by the
	// values in the row tuple.
	filterVars parser.IndexedVarHelper

	evalCtx parser.EvalContext
	txn     *client.Txn
	fetcher sqlbase.RowFetcher
	// Last row returned by the rowFetcher; it has one entry per table column.
	row parser.DTuple
}

// TableReader implements parser.IndexedVarContainer.
var _ parser.IndexedVarContainer = &TableReader{}

// IndexedVarReturnType is part of the parser.IndexedVarContaine interface.
func (tr *TableReader) IndexedVarReturnType(idx int) parser.Datum {
	return tr.desc.Columns[idx].Type.ToDatumType()
}

// IndexedVarEval is part of the parser.IndexedVarContaine interface.
func (tr *TableReader) IndexedVarEval(idx int, ctx parser.EvalContext) (parser.Datum, error) {
	return tr.row[idx].Eval(ctx)
}

// IndexedVarString is part of the parser.IndexedVarContaine interface.
func (tr *TableReader) IndexedVarString(idx int) string {
	return string(tr.desc.Columns[idx].Name)
}

// NewTableReader creates a TableReader. Exported for testing purposes.
func NewTableReader(spec *TableReaderSpec, txn *client.Txn, evalCtx parser.EvalContext) (
	*TableReader, error,
) {
	tr := &TableReader{
		desc:    spec.Table,
		txn:     txn,
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
		return nil, fmt.Errorf("Invalid indexIdx %d", spec.IndexIdx)
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

// Run is the "main loop".
func (tr *TableReader) Run() error {
	if log.V(1) {
		log.Infof("TableReader filter: %s\n", tr.filter)
	}
	err := tr.fetcher.StartScan(tr.txn, tr.spans, 0)
	if err != nil {
		return err
	}
	for {
		tr.row, err = tr.fetcher.NextRow()
		if err != nil {
			return err
		}
		if tr.row == nil {
			// No more rows.
			break
		}
		passesFilter, err := sqlbase.RunFilter(tr.filter, tr.evalCtx)
		if err != nil {
			return err
		}
		if passesFilter {
			// TODO(radu): these Printfs are temporary and only serve as a way
			// to manually verify this is working as intended. They will be
			// removed once we actually output the data to a stream.
			fmt.Printf("RESULT:")
			for _, d := range tr.row {
				if d != nil {
					fmt.Printf(" %s", d)
				} else {
					fmt.Printf(" <skipped>")
				}
			}
			fmt.Printf("\n")
		}
	}
	return nil
}
