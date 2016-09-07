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
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)

package distsql

import (
	"github.com/cockroachdb/cockroach/sql/sqlbase"
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
