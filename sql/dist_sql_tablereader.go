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

package sql

import (
	"fmt"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// TableReader is the start of a computation flow; it performs KV operations to
// retrieve rows for a table and outputs the desired columns of the rows that
// pass a filter expression.
// See docs/RFCS/distributed_sql.md
type TableReader struct {
	desc       TableDescriptor
	spans      spans
	outputCols []int

	filter parser.Expr
	// filterVars is used to generate IndexedVars that are "backed" by the
	// values in the row tuple.
	filterVars parser.IndexedVarHelper

	evalCtx parser.EvalContext
	txn     *client.Txn
	fetcher rowFetcher
	// Last row returned by the rowFetcher; it has one entry per table column.
	row parser.DTuple
}

// TableReader implements parser.IndexedVarContainer.
var _ parser.IndexedVarContainer = &TableReader{}

// IndexedVarTypeCheck is part of the parser.IndexedVarContainer interface.
func (tr *TableReader) IndexedVarTypeCheck(idx int, args parser.MapArgs) (parser.Datum, error) {
	return tr.desc.Columns[idx].Type.toDatum(), nil
}

// IndexedVarEval is part of the parser.IndexedVarContainer interface.
func (tr *TableReader) IndexedVarEval(idx int, ctx parser.EvalContext) (parser.Datum, error) {
	return tr.row[idx].Eval(ctx)
}

// IndexedVarString is part of the parser.IndexedVarContainer interface.
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
	tr.filter, err = processSQLExpression(spec.Filter, &tr.filterVars)
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

	var index *IndexDescriptor
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

	colIdxMap := make(map[ColumnID]int, len(tr.desc.Columns))
	for i, c := range tr.desc.Columns {
		colIdxMap[c.ID] = i
	}
	err = tr.fetcher.init(&tr.desc, colIdxMap, index, spec.Reverse, isSecondaryIndex,
		valNeededForCol)
	if err != nil {
		return nil, err
	}

	tr.spans = make(spans, len(spec.Spans))
	for i, s := range spec.Spans {
		tr.spans[i] = span{start: s.Span.Key, end: s.Span.EndKey}
	}

	return tr, nil
}

// Run is the "main loop".
func (tr *TableReader) Run() *roachpb.Error {
	if log.V(1) {
		log.Infof("TableReader filter: %s\n", tr.filter)
	}
	pErr := tr.fetcher.startScan(tr.txn, tr.spans, 0)
	if pErr != nil {
		return pErr
	}
	for {
		tr.row, pErr = tr.fetcher.nextRow()
		if pErr != nil {
			return pErr
		}
		if tr.row == nil {
			// No more rows.
			break
		}
		passesFilter, err := runFilter(tr.filter, tr.evalCtx)
		if err != nil {
			return roachpb.NewError(err)
		}
		if passesFilter {
			// TODO These Printfs are temporary and only serve as a way to manually
			// verify this is working as intended. They will be removed once we
			// actually output the data to a stream.
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
