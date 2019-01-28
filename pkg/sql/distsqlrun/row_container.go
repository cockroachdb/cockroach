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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/diskmap"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/ring"
	"github.com/pkg/errors"
)

// sortableRowContainer is a container used to store rows and optionally sort
// these.
type sortableRowContainer interface {
	Len() int
	AddRow(context.Context, sqlbase.EncDatumRow) error
	// Sort sorts the rows according to the current ordering.
	Sort(context.Context)
	// ChangeOrdering changes the ordering on which rows will be sorted. In order
	// for the ordering to take effect, Sort() must be called. It returns
	// (possibly new) row container and an error if occurred.
	ChangeOrdering(context.Context, sqlbase.ColumnOrdering) (sortableRowContainer, error)
	// NewIterator returns a rowIterator that can be used to iterate over
	// the rows.
	NewIterator(context.Context) rowIterator
	// NewFinalIterator returns a rowIterator that can be used to iterate over the
	// rows, possibly freeing resources along the way. Subsequent calls to
	// NewIterator or NewFinalIterator are not guaranteed to return any rows.
	NewFinalIterator(context.Context) rowIterator

	// UnsafeReset resets the container, allowing for reuse. It renders all
	// previously allocated rows unsafe.
	UnsafeReset(context.Context) error

	// InitTopK enables optimizations in cases where the caller cares only about
	// the top k rows where k is the size of the sortableRowContainer when
	// InitTopK is called. Once InitTopK is called, callers should not call
	// AddRow. Iterators created after calling InitTopK are guaranteed to read the
	// top k rows only.
	InitTopK()
	// MaybeReplaceMax checks whether the given row belongs in the top k rows,
	// potentially evicting a row in favor of the given row.
	MaybeReplaceMax(context.Context, sqlbase.EncDatumRow) error

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
	mc.initWithMon(ordering, types, evalCtx, evalCtx.Mon, 0 /* rowCapacity */)
}

// initWithMon initializes the memRowContainer with an explicit monitor. Only
// use this if the default memRowContainer.init() function is insufficient.
func (mc *memRowContainer) initWithMon(
	ordering sqlbase.ColumnOrdering,
	types []sqlbase.ColumnType,
	evalCtx *tree.EvalContext,
	mon *mon.BytesMonitor,
	rowCapacity int,
) {
	acc := mon.MakeBoundAccount()
	mc.RowContainer.Init(acc, sqlbase.ColTypeInfoFromColTypes(types), rowCapacity)
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

// Sort implements sortableRowContainer.
func (mc *memRowContainer) Sort(ctx context.Context) {
	mc.invertSorting = false
	cancelChecker := sqlbase.NewCancelChecker(ctx)
	sqlbase.Sort(mc, cancelChecker)
}

// ChangeOrdering implements sortableRowContainer. We don't need to create
// a new memRowContainer and can just change the ordering on-the-fly.
func (mc *memRowContainer) ChangeOrdering(
	_ context.Context, ordering sqlbase.ColumnOrdering,
) (sortableRowContainer, error) {
	mc.ordering = ordering
	return mc, nil
}

// Push is part of heap.Interface.
func (mc *memRowContainer) Push(_ interface{}) { panic("unimplemented") }

// Pop is part of heap.Interface.
func (mc *memRowContainer) Pop() interface{} { panic("unimplemented") }

// MaybeReplaceMax replaces the maximum element with the given row, if it is
// smaller. Assumes InitTopK was called.
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

// InitTopK rearranges the rows in the memRowContainer into a Max-Heap.
func (mc *memRowContainer) InitTopK() {
	mc.invertSorting = true
	heap.Init(mc)
}

// memRowIterator is a rowIterator that iterates over a memRowContainer. This
// iterator doesn't iterate over a snapshot of memRowContainer.
type memRowIterator struct {
	*memRowContainer
	curIdx int
}

var _ rowIterator = &memRowIterator{}

// NewIterator returns an iterator that can be used to iterate over a
// memRowContainer. Note that this iterator doesn't iterate over a snapshot
// of memRowContainer.
func (mc *memRowContainer) NewIterator(_ context.Context) rowIterator {
	return &memRowIterator{memRowContainer: mc}
}

// Rewind implements the rowIterator interface.
func (i *memRowIterator) Rewind() {
	i.curIdx = 0
}

// Valid implements the rowIterator interface.
func (i *memRowIterator) Valid() (bool, error) {
	return i.curIdx < i.Len(), nil
}

// Next implements the rowIterator interface.
func (i *memRowIterator) Next() {
	i.curIdx++
}

// Row implements the rowIterator interface.
func (i *memRowIterator) Row() (sqlbase.EncDatumRow, error) {
	return i.EncRow(i.curIdx), nil
}

// Close implements the rowIterator interface.
func (i *memRowIterator) Close() {}

// memRowFinalIterator is a rowIterator that iterates over a memRowContainer.
// This iterator doesn't iterate over a snapshot of memRowContainer and deletes
// rows as soon as they are iterated over to free up memory eagerly.
type memRowFinalIterator struct {
	*memRowContainer
}

// NewFinalIterator returns an iterator that can be used to iterate over a
// memRowContainer. Note that this iterator doesn't iterate over a snapshot
// of memRowContainer and that it deletes rows as soon as they are iterated
// over.
func (mc *memRowContainer) NewFinalIterator(_ context.Context) rowIterator {
	return memRowFinalIterator{memRowContainer: mc}
}

var _ rowIterator = memRowFinalIterator{}

// Rewind implements the rowIterator interface.
func (i memRowFinalIterator) Rewind() {}

// Valid implements the rowIterator interface.
func (i memRowFinalIterator) Valid() (bool, error) {
	return i.Len() > 0, nil
}

// Next implements the rowIterator interface.
func (i memRowFinalIterator) Next() {
	i.PopFirst()
}

// Row implements the rowIterator interface.
func (i memRowFinalIterator) Row() (sqlbase.EncDatumRow, error) {
	return i.EncRow(0), nil
}

// Close implements the rowIterator interface.
func (i memRowFinalIterator) Close() {}

// diskBackedRowContainer is a sortableRowContainer that uses a memRowContainer
// to store rows and spills back to disk automatically if memory usage exceeds a
// given budget.
type diskBackedRowContainer struct {
	// src is the current sortableRowContainer that is being used to store rows.
	// All the sortableRowContainer methods are redefined rather than delegating
	// to an embedded struct because of how defer works:
	// 	rc.init(...)
	//	defer rc.Close(ctx)
	// The Close will call memRowContainer.Close(ctx) even after spilling to disk.
	src sortableRowContainer

	mrc *memRowContainer
	drc *diskRowContainer

	spilled bool

	// The following fields are used to create a diskRowContainer when spilling
	// to disk.
	engine      diskmap.Factory
	diskMonitor *mon.BytesMonitor
}

var _ sortableRowContainer = &diskBackedRowContainer{}

// init initializes a diskBackedRowContainer.
// Arguments:
//  - ordering is the output ordering; the order in which rows should be sorted.
//  - types is the schema of rows that will be added to this container.
//  - evalCtx defines the context in which to evaluate comparisons, only used
//    when storing rows in memory.
//  - engine is the store used for rows when spilling to disk.
//  - memoryMonitor is used to monitor the diskBackedRowContainer's memory usage.
//    If this monitor denies an allocation, the diskBackedRowContainer will
//    spill to disk.
//  - diskMonitor is used to monitor the diskBackedRowContainer's disk usage if
//    and when it spills to disk.
//  - shouldUseDiskFromStart tells the container to use diskRowContainer from
//    the initialization.
func (f *diskBackedRowContainer) init(
	ordering sqlbase.ColumnOrdering,
	types []sqlbase.ColumnType,
	evalCtx *tree.EvalContext,
	engine diskmap.Factory,
	memoryMonitor *mon.BytesMonitor,
	diskMonitor *mon.BytesMonitor,
	rowCapacity int,
) {
	if shouldUseDiskFromStart {
		drc := makeDiskRowContainer(diskMonitor, types, ordering, engine)
		f.src = &drc
		f.drc = &drc
		f.spilled = true
		return
	}
	mrc := memRowContainer{}
	mrc.initWithMon(ordering, types, evalCtx, memoryMonitor, rowCapacity)
	f.mrc = &mrc
	f.src = &mrc
	f.engine = engine
	f.diskMonitor = diskMonitor
}

func (f *diskBackedRowContainer) Len() int {
	return f.src.Len()
}

func (f *diskBackedRowContainer) AddRow(ctx context.Context, row sqlbase.EncDatumRow) error {
	if err := f.src.AddRow(ctx, row); err != nil {
		if spilled, spillErr := f.spillIfMemErr(ctx, err); !spilled && spillErr == nil {
			// The error was not an out of memory error.
			return err
		} else if spillErr != nil {
			// A disk spill was attempted but there was an error in doing so.
			return spillErr
		}
		// Add the row that caused the memory error.
		return f.src.AddRow(ctx, row)
	}
	return nil
}

// Sort implements sortableRowContainer.
func (f *diskBackedRowContainer) Sort(ctx context.Context) {
	f.src.Sort(ctx)
}

// ChangeOrdering implements sortableRowContainer.
func (f *diskBackedRowContainer) ChangeOrdering(
	ctx context.Context, ordering sqlbase.ColumnOrdering,
) (sortableRowContainer, error) {
	if newSrc, err := f.src.ChangeOrdering(ctx, ordering); err != nil {
		return nil, err
	} else {
		f.src = newSrc
		switch c := newSrc.(type) {
		case *memRowContainer:
			f.mrc = c
		case *diskRowContainer:
			f.drc = c
		default:
			return nil, errors.Errorf("unexpected row container type")
		}
	}
	return f, nil
}

func (f *diskBackedRowContainer) InitTopK() {
	f.src.InitTopK()
}

func (f *diskBackedRowContainer) MaybeReplaceMax(
	ctx context.Context, row sqlbase.EncDatumRow,
) error {
	return f.src.MaybeReplaceMax(ctx, row)
}

func (f *diskBackedRowContainer) NewIterator(ctx context.Context) rowIterator {
	return f.src.NewIterator(ctx)
}

func (f *diskBackedRowContainer) NewFinalIterator(ctx context.Context) rowIterator {
	return f.src.NewFinalIterator(ctx)
}

// UnsafeReset resets the container for reuse. The diskBackedRowContainer will
// reset to using memory if it is using disk.
func (f *diskBackedRowContainer) UnsafeReset(ctx context.Context) error {
	if f.drc != nil {
		f.drc.Close(ctx)
		f.src = f.mrc
		f.drc = nil
		return nil
	}
	return f.mrc.UnsafeReset(ctx)
}

func (f *diskBackedRowContainer) Close(ctx context.Context) {
	if f.drc != nil {
		f.drc.Close(ctx)
	}
	if f.mrc != nil {
		f.mrc.Close(ctx)
	}
}

// Spilled returns whether or not the diskBackedRowContainer spilled to disk
// in its lifetime.
func (f *diskBackedRowContainer) Spilled() bool {
	return f.spilled
}

// UsingDisk returns whether or not the diskBackedRowContainer is currently
// using disk.
func (f *diskBackedRowContainer) UsingDisk() bool {
	return f.drc != nil
}

// spillIfMemErr checks err and calls spillToDisk if the given err is an out of
// memory error. Returns whether the diskBackedRowContainer spilled to disk and
// an error if one occurred while doing so.
func (f *diskBackedRowContainer) spillIfMemErr(ctx context.Context, err error) (bool, error) {
	if pgErr, ok := pgerror.GetPGCause(err); !(ok && pgErr.Code == pgerror.CodeOutOfMemoryError) {
		return false, nil
	}
	if spillErr := f.spillToDisk(ctx); spillErr != nil {
		return false, spillErr
	}
	log.VEventf(ctx, 2, "spilled to disk: %v", err)
	return true, nil
}

func (f *diskBackedRowContainer) spillToDisk(ctx context.Context) error {
	if f.UsingDisk() {
		return errors.New("already using disk")
	}
	drc := makeDiskRowContainer(f.diskMonitor, f.mrc.types, f.mrc.ordering, f.engine)
	i := f.mrc.NewFinalIterator(ctx)
	defer i.Close()
	for i.Rewind(); ; i.Next() {
		if ok, err := i.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}
		memRow, err := i.Row()
		if err != nil {
			return err
		}
		if err := drc.AddRow(ctx, memRow); err != nil {
			return err
		}
	}
	f.mrc.Clear(ctx)

	f.src = &drc
	f.drc = &drc
	f.spilled = true
	return nil
}

// diskBackedIndexedRowContainer is a wrapper around diskBackedRowContainer
// that adds an index to each row added in the order of addition of those rows
// by storing an extra int column at the end of each row.
type diskBackedIndexedRowContainer struct {
	*diskBackedRowContainer

	ctx           context.Context
	scratchEncRow sqlbase.EncDatumRow
	storedTypes   []sqlbase.ColumnType
	datumAlloc    sqlbase.DatumAlloc
	rowAlloc      sqlbase.EncDatumRowAlloc
	idx           uint64 // the index of the next row to be added into the container

	// These fields are for optimizations when container spilled to disk.
	diskRowIter rowIterator
	idxRowIter  int
	// nextPosToCache is the index of the row to be cached next. If it is greater
	// than 0, the cache contains all rows with position in the range
	// [max(0, cache.Len()-maxIndexedRowsCacheSize), nextPosToCache-1].
	nextPosToCache   int
	indexedRowsCache ring.Buffer // the cache of up to maxIndexedRowsCacheSize contiguous rows
}

func makeDiskBackedIndexedRowContainer(
	ordering sqlbase.ColumnOrdering,
	types []sqlbase.ColumnType,
	evalCtx *tree.EvalContext,
	engine diskmap.Factory,
	memoryMonitor *mon.BytesMonitor,
	diskMonitor *mon.BytesMonitor,
	rowCapacity int,
) *diskBackedIndexedRowContainer {
	d := diskBackedIndexedRowContainer{}

	// We will be storing an index of each row as the last INT column.
	d.storedTypes = make([]sqlbase.ColumnType, len(types)+1)
	copy(d.storedTypes, types)
	d.storedTypes[len(d.storedTypes)-1] = sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
	d.scratchEncRow = make(sqlbase.EncDatumRow, len(d.storedTypes))
	d.diskBackedRowContainer = &diskBackedRowContainer{}
	d.diskBackedRowContainer.init(ordering, d.storedTypes, evalCtx, engine, memoryMonitor, diskMonitor, rowCapacity)
	return &d
}

// AddRow implements sortableRowContainer.
func (f *diskBackedIndexedRowContainer) AddRow(ctx context.Context, row sqlbase.EncDatumRow) error {
	f.ctx = ctx
	copy(f.scratchEncRow, row)
	f.scratchEncRow[len(f.scratchEncRow)-1] = sqlbase.DatumToEncDatum(
		sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT},
		tree.NewDInt(tree.DInt(f.idx)),
	)
	f.idx++
	return f.diskBackedRowContainer.AddRow(ctx, f.scratchEncRow)
}

// ChangeOrdering implements sortableRowContainer.
func (f *diskBackedIndexedRowContainer) ChangeOrdering(
	ctx context.Context, ordering sqlbase.ColumnOrdering,
) (sortableRowContainer, error) {
	if newSrc, err := f.diskBackedRowContainer.ChangeOrdering(ctx, ordering); err != nil {
		return nil, err
	} else {
		f.diskBackedRowContainer = newSrc.(*diskBackedRowContainer)
	}
	f.resetCache()
	f.resetIterator()
	return f, nil
}

// resetCache resets cache-related fields allowing for reusing the underlying
// already allocated memory.
func (f *diskBackedIndexedRowContainer) resetCache() {
	f.nextPosToCache = 0
	f.indexedRowsCache.Reset()
}

func (f *diskBackedIndexedRowContainer) resetIterator() {
	if f.diskRowIter != nil {
		f.diskRowIter.Close()
		f.diskRowIter = nil
		f.idxRowIter = 0
	}
}

// UnsafeReset resets the underlying container (if it is using disk, it will be
// reset to using memory).
func (f *diskBackedIndexedRowContainer) UnsafeReset(ctx context.Context) error {
	f.resetCache()
	f.resetIterator()
	f.idx = 0
	return f.diskBackedRowContainer.UnsafeReset(ctx)
}

// Close implements sortableRowContainer.
func (f *diskBackedIndexedRowContainer) Close(ctx context.Context) {
	if f.diskRowIter != nil {
		f.diskRowIter.Close()
	}
	f.diskBackedRowContainer.Close(ctx)
}

const maxIndexedRowsCacheSize = 4096

// GetRow implements IndexedRows.
//
// Getting a row by index is fast from an in-memory row container but is a lot
// slower from a disk-backed one. In order to mitigate the impact we add
// optimizations of maintaining a cache of tree.IndexedRow's and storing a disk
// iterator along with the index of the row it currently points at.
func (f *diskBackedIndexedRowContainer) GetRow(pos int) tree.IndexedRow {
	var rowWithIdx sqlbase.EncDatumRow
	var err error
	if f.UsingDisk() {
		// The cache contains all contiguous rows up to the biggest pos requested
		// so far (even if the rows were not requested explicitly). For example,
		// if the cache is empty and the request comes for a row at pos 3, the
		// cache will contain 4 rows at positions 0, 1, 2, and 3.
		if f.indexedRowsCache.Len() > 0 {
			firstCachedPos, lastCachedPos := f.indexedRowsCache.Len()-maxIndexedRowsCacheSize, f.nextPosToCache-1
			if firstCachedPos < 0 {
				firstCachedPos = 0
			}
			if pos >= firstCachedPos && pos <= lastCachedPos {
				requestedRowCachePos := pos - firstCachedPos
				return f.indexedRowsCache.Get(requestedRowCachePos).(tree.IndexedRow)
			}
		}
		if f.diskRowIter == nil {
			f.diskRowIter = f.diskBackedRowContainer.drc.NewIterator(f.ctx)
			f.diskRowIter.Rewind()
		}
		if f.idxRowIter > pos {
			// The iterator has been advanced further than we need, so we need to
			// start iterating from the beginning.
			firstCachedPos, lastCachedPos := f.indexedRowsCache.Len()-maxIndexedRowsCacheSize, f.nextPosToCache-1
			if firstCachedPos < 0 {
				firstCachedPos = 0
			}
			log.Infof(f.ctx, "rewinding: cache contains indices [%d, %d] but index %d requested", firstCachedPos, lastCachedPos, pos)
			f.idxRowIter = 0
			f.diskRowIter.Rewind()
			f.resetCache()
			if pos-maxIndexedRowsCacheSize > f.nextPosToCache {
				f.nextPosToCache = pos - maxIndexedRowsCacheSize
			}
		}
		for ; ; f.diskRowIter.Next() {
			if ok, err := f.diskRowIter.Valid(); err != nil {
				panic(err)
			} else if !ok {
				panic(fmt.Sprintf("row at pos %d not found", pos))
			}
			if f.idxRowIter == f.nextPosToCache {
				rowWithIdx, err = f.diskRowIter.Row()
				if err != nil {
					panic(err)
				}
				for i := range rowWithIdx {
					// TODO(yuzefovich): account for the extra memory.
					if err := rowWithIdx[i].EnsureDecoded(&f.storedTypes[i], &f.datumAlloc); err != nil {
						panic(err)
					}
				}
				if rowIdx, ok := rowWithIdx[len(rowWithIdx)-1].Datum.(*tree.DInt); ok {
					// We actually need to copy the row into memory.
					ir := indexedRow{int(*rowIdx), f.rowAlloc.CopyRow(rowWithIdx[:len(rowWithIdx)-1])}
					if f.indexedRowsCache.Len() == maxIndexedRowsCacheSize {
						// The cache size is capped at maxIndexedRowsCacheSize, so we first
						// remove the row with the smallest pos.
						f.indexedRowsCache.RemoveFirst()
					}
					f.indexedRowsCache.AddLast(ir)
					f.nextPosToCache++
				} else {
					panic("unexpected last column: should be integer pos")
				}
				if f.idxRowIter == pos {
					return f.indexedRowsCache.GetLast().(tree.IndexedRow)
				}
			}
			f.idxRowIter++
		}
	}
	rowWithIdx = f.diskBackedRowContainer.mrc.EncRow(pos)
	if rowIdx, ok := rowWithIdx[len(rowWithIdx)-1].Datum.(*tree.DInt); ok {
		return indexedRow{int(*rowIdx), rowWithIdx[:len(rowWithIdx)-1]}
	}
	panic("unexpected last column: should be integer pos")
}
