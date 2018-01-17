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
	"container/heap"
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// sortableRowContainer is a container used to store rows and optionally sort
// these.
type sortableRowContainer interface {
	AddRow(context.Context, sqlbase.EncDatumRow) error
	// Sort sorts the rows according to an ordering specified at initialization.
	Sort(context.Context)
	// NewIterator returns a rowIterator that can be used to iterate over
	// the rows.
	NewIterator(context.Context) rowIterator

	// Close frees up resources held by the sortableRowContainer.
	Close(context.Context)
}

// rowIterator is a simple iterator used to iterate over sqlbase.EncDatumRows.
// Example use:
// 	var i rowIterator
// 	for i.Rewind(); ; i.Next() {
// 		if ok, err := i.Valid(); err != nil {
// 			// Handle error.
// 		} else if !ok {
//			break
// 		}
//		row, err := i.Row()
//		if err != nil {
//			// Handle error.
//		}
//		// Do something.
// 	}
//
type rowIterator interface {
	// Rewind seeks to the first row.
	Rewind()
	// Valid must be called after any call to Rewind() or Next(). It returns
	// (true, nil) if the iterator points to a valid row and (false, nil) if the
	// iterator has moved past the last row.
	// If an error has occurred, the returned bool is invalid.
	Valid() (bool, error)
	// Next advances the iterator to the next row in the iteration.
	Next()
	// Row returns the current row. The returned row is only valid until the
	// next call to Rewind() or Next().
	Row() (sqlbase.EncDatumRow, error)

	// Close frees up resources held by the iterator.
	Close()
}

// memRowContainer is the wrapper around sqlbase.RowContainer that provides more
// functionality, especially around converting to/from EncDatumRows and
// facilitating sorting.
type memRowContainer struct {
	sqlbase.RowContainer
	types         []sqlbase.ColumnType
	invertSorting bool // Inverts the sorting predicate.
	ordering      sqlbase.ColumnOrdering
	scratchRow    tree.Datums
	scratchEncRow sqlbase.EncDatumRow

	evalCtx *tree.EvalContext

	datumAlloc sqlbase.DatumAlloc
}

var _ heap.Interface = &memRowContainer{}
var _ sortableRowContainer = &memRowContainer{}

// init initializes the memRowContainer. The memRowContainer uses evalCtx.Mon
// to track memory usage.
func (mc *memRowContainer) init(
	ordering sqlbase.ColumnOrdering, types []sqlbase.ColumnType, evalCtx *tree.EvalContext,
) {
	mc.initWithMon(ordering, types, evalCtx, evalCtx.Mon)
}

// initWithMon initializes the memRowContainer with an explicit monitor. Only
// use this if the default memRowContainer.init() function is insufficient.
func (mc *memRowContainer) initWithMon(
	ordering sqlbase.ColumnOrdering,
	types []sqlbase.ColumnType,
	evalCtx *tree.EvalContext,
	mon *mon.BytesMonitor,
) {
	acc := mon.MakeBoundAccount()
	mc.RowContainer.Init(acc, sqlbase.ColTypeInfoFromColTypes(types), 0)
	mc.types = types
	mc.ordering = ordering
	mc.scratchRow = make(tree.Datums, len(types))
	mc.scratchEncRow = make(sqlbase.EncDatumRow, len(types))
	mc.evalCtx = evalCtx
}

// Less is part of heap.Interface and is only meant to be used internally.
func (mc *memRowContainer) Less(i, j int) bool {
	cmp := sqlbase.CompareDatums(mc.ordering, mc.evalCtx, mc.At(i), mc.At(j))
	if mc.invertSorting {
		cmp = -cmp
	}
	return cmp < 0
}

// EncRow returns the idx-th row as an EncDatumRow. The slice itself is reused
// so it is only valid until the next call to EncRow.
func (mc *memRowContainer) EncRow(idx int) sqlbase.EncDatumRow {
	datums := mc.At(idx)
	for i, d := range datums {
		mc.scratchEncRow[i] = sqlbase.DatumToEncDatum(mc.types[i], d)
	}
	return mc.scratchEncRow
}

// AddRow adds a row to the container.
func (mc *memRowContainer) AddRow(ctx context.Context, row sqlbase.EncDatumRow) error {
	if len(row) != len(mc.types) {
		log.Fatalf(ctx, "invalid row length %d, expected %d", len(row), len(mc.types))
	}
	for i := range row {
		err := row[i].EnsureDecoded(&mc.types[i], &mc.datumAlloc)
		if err != nil {
			return err
		}
		mc.scratchRow[i] = row[i].Datum
	}
	_, err := mc.RowContainer.AddRow(ctx, mc.scratchRow)
	return err
}

func (mc *memRowContainer) Sort(ctx context.Context) {
	mc.invertSorting = false
	cancelChecker := sqlbase.NewCancelChecker(ctx)
	sqlbase.Sort(mc, cancelChecker)
}

// Push is part of heap.Interface.
func (mc *memRowContainer) Push(_ interface{}) { panic("unimplemented") }

// Pop is part of heap.Interface.
func (mc *memRowContainer) Pop() interface{} { panic("unimplemented") }

// MaybeReplaceMax replaces the maximum element with the given row, if it is smaller.
// Assumes InitMaxHeap was called.
func (mc *memRowContainer) MaybeReplaceMax(ctx context.Context, row sqlbase.EncDatumRow) error {
	max := mc.At(0)
	cmp, err := row.CompareToDatums(mc.types, &mc.datumAlloc, mc.ordering, mc.evalCtx, max)
	if err != nil {
		return err
	}
	if cmp < 0 {
		// row is smaller than the max; replace.
		for i := range row {
			if err := row[i].EnsureDecoded(&mc.types[i], &mc.datumAlloc); err != nil {
				return err
			}
			mc.scratchRow[i] = row[i].Datum
		}
		if err := mc.Replace(ctx, 0, mc.scratchRow); err != nil {
			return err
		}
		heap.Fix(mc, 0)
	}
	return nil
}

// InitMaxHeap rearranges the rows in the rowContainer into a Max-Heap.
func (mc *memRowContainer) InitMaxHeap() {
	mc.invertSorting = true
	heap.Init(mc)
}

// memRowIterator is a rowIterator that iterates over a memRowContainer. This
// iterator doesn't iterate over a snapshot of memRowContainer and deletes rows
// as soon as they are iterated over to free up memory eagerly.
type memRowIterator struct {
	*memRowContainer
}

var _ rowIterator = memRowIterator{}

// NewIterator returns an iterator that can be used to iterate over a
// memRowContainer. Note that this iterator doesn't iterate over a snapshot
// of memRowContainer and that it deletes rows as soon as they are iterated
// over.
func (mc *memRowContainer) NewIterator(_ context.Context) rowIterator {
	return memRowIterator{memRowContainer: mc}
}

// Rewind implements the rowIterator interface.
func (i memRowIterator) Rewind() {}

// Valid implements the rowIterator interface.
func (i memRowIterator) Valid() (bool, error) {
	return i.Len() > 0, nil
}

// Next implements the rowIterator interface.
func (i memRowIterator) Next() {
	i.PopFirst()
}

// Row implements the rowIterator interface.
func (i memRowIterator) Row() (sqlbase.EncDatumRow, error) {
	return i.EncRow(0), nil
}

// Close implements the rowIterator interface.
func (i memRowIterator) Close() {}
