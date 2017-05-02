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

package distsqlrun

import (
	"container/heap"
	"sort"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// sorterValues is the internal wrapper around the collection of rows added to
// a sorter strategy, it is at this level that the rows to be sorted are stored.
type sorterValues struct {
	sqlbase.RowContainer
	types         []sqlbase.ColumnType
	invertSorting bool // Inverts the sorting predicate.
	ordering      sqlbase.ColumnOrdering
	scratchRow    parser.Datums
	scratchEncRow sqlbase.EncDatumRow

	evalCtx *parser.EvalContext

	datumAlloc sqlbase.DatumAlloc
}

var _ heap.Interface = &sorterValues{}

func makeSorterValues(
	ordering sqlbase.ColumnOrdering, types []sqlbase.ColumnType, evalCtx *parser.EvalContext,
) sorterValues {
	acc := evalCtx.Mon.MakeBoundAccount()
	return sorterValues{
		RowContainer:  sqlbase.MakeRowContainer(acc, sqlbase.ColTypeInfoFromColTypes(types), 0),
		types:         types,
		ordering:      ordering,
		scratchRow:    make(parser.Datums, len(types)),
		scratchEncRow: make(sqlbase.EncDatumRow, len(types)),
		evalCtx:       evalCtx,
	}
}

// Less is part of heap.Interface and is only meant to be used internally.
func (sv *sorterValues) Less(i, j int) bool {
	cmp := sqlbase.CompareDatums(sv.ordering, sv.evalCtx, sv.At(i), sv.At(j))
	if sv.invertSorting {
		cmp = -cmp
	}
	return cmp < 0
}

// EncRow returns the idx-th row as an EncDatumRow. The slice itself is reused
// so it is only valid until the next call to EncRow.
func (sv *sorterValues) EncRow(idx int) sqlbase.EncDatumRow {
	datums := sv.At(idx)
	for i, d := range datums {
		sv.scratchEncRow[i] = sqlbase.DatumToEncDatum(sv.types[i], d)
	}
	return sv.scratchEncRow
}

// AddRow adds a row to the container.
func (sv *sorterValues) AddRow(ctx context.Context, row sqlbase.EncDatumRow) error {
	if len(row) != len(sv.types) {
		log.Fatalf(ctx, "invalid row length %d, expected %d", len(row), len(sv.types))
	}
	for i := range row {
		err := row[i].EnsureDecoded(&sv.datumAlloc)
		if err != nil {
			return err
		}
		sv.scratchRow[i] = row[i].Datum
	}
	_, err := sv.RowContainer.AddRow(ctx, sv.scratchRow)
	return err
}

func (sv *sorterValues) Sort() {
	sv.invertSorting = false
	sort.Sort(sv)
}

// Push is part of heap.Interface.
func (sv *sorterValues) Push(_ interface{}) { panic("unimplemented") }

// Pop is part of heap.Interface.
func (sv *sorterValues) Pop() interface{} { panic("unimplemented") }

// MaybeReplaceMax replaces the maximum element with the given row, if it is smaller.
// Assumes InitMaxHeap was called.
func (sv *sorterValues) MaybeReplaceMax(row sqlbase.EncDatumRow) error {
	max := sv.At(0)
	cmp, err := row.CompareToDatums(&sv.datumAlloc, sv.ordering, sv.evalCtx, max)
	if err != nil {
		return err
	}
	if cmp < 0 {
		// row is smaller than the max; replace.
		for i := range row {
			if err := row[i].EnsureDecoded(&sv.datumAlloc); err != nil {
				return err
			}
			max[i] = row[i].Datum
		}
		heap.Fix(sv, 0)
	}
	return nil
}

// Initializes the rows contained within sorterValues as a MaxHeap.
func (sv *sorterValues) InitMaxHeap() {
	sv.invertSorting = true
	heap.Init(sv)
}
