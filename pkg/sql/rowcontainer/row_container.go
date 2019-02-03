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

package rowcontainer

import (
	"container/heap"
	"context"
	"fmt"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/diskmap"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/ring"
	"github.com/pkg/errors"
)

// SortableRowContainer is a container used to store rows and optionally sort
// these.
type SortableRowContainer interface {
	Len() int
	AddRow(context.Context, sqlbase.EncDatumRow) error
	// Sort sorts the rows according to the current ordering (the one set either
	// at initialization or by the last call of Reorder() - if the container is
	// ReorderableRowContainer).
	Sort(context.Context)
	// NewIterator returns a RowIterator that can be used to iterate over
	// the rows.
	NewIterator(context.Context) RowIterator
	// NewFinalIterator returns a RowIterator that can be used to iterate over the
	// rows, possibly freeing resources along the way. Subsequent calls to
	// NewIterator or NewFinalIterator are not guaranteed to return any rows.
	NewFinalIterator(context.Context) RowIterator

	// UnsafeReset resets the container, allowing for reuse. It renders all
	// previously allocated rows unsafe.
	UnsafeReset(context.Context) error

	// InitTopK enables optimizations in cases where the caller cares only about
	// the top k rows where k is the size of the SortableRowContainer when
	// InitTopK is called. Once InitTopK is called, callers should not call
	// AddRow. Iterators created after calling InitTopK are guaranteed to read the
	// top k rows only.
	InitTopK()
	// MaybeReplaceMax checks whether the given row belongs in the top k rows,
	// potentially evicting a row in favor of the given row.
	MaybeReplaceMax(context.Context, sqlbase.EncDatumRow) error

	// Close frees up resources held by the SortableRowContainer.
	Close(context.Context)
}

// ReorderableRowContainer is a SortableRowContainer that can change the
// ordering on which the rows are sorted.
type ReorderableRowContainer interface {
	SortableRowContainer

	// Reorder changes the ordering on which the rows are sorted. In order for
	// new ordering to take effect, Sort() must be called. It returns an error if
	// it occurs.
	Reorder(context.Context, sqlbase.ColumnOrdering) error
}

// RowIterator is a simple iterator used to iterate over sqlbase.EncDatumRows.
// Example use:
// 	var i RowIterator
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
type RowIterator interface {
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

// MemRowContainer is the wrapper around rowcontainer.RowContainer that
// provides more functionality, especially around converting to/from
// EncDatumRows and facilitating sorting.
type MemRowContainer struct {
	RowContainer
	types         []sqlbase.ColumnType
	invertSorting bool // Inverts the sorting predicate.
	ordering      sqlbase.ColumnOrdering
	scratchRow    tree.Datums
	scratchEncRow sqlbase.EncDatumRow

	evalCtx *tree.EvalContext

	datumAlloc sqlbase.DatumAlloc
}

var _ heap.Interface = &MemRowContainer{}
var _ SortableRowContainer = &MemRowContainer{}

// Init initializes the MemRowContainer. The MemRowContainer uses evalCtx.Mon
// to track memory usage.
func (mc *MemRowContainer) Init(
	ordering sqlbase.ColumnOrdering, types []sqlbase.ColumnType, evalCtx *tree.EvalContext,
) {
	mc.InitWithMon(ordering, types, evalCtx, evalCtx.Mon, 0 /* rowCapacity */)
}

// InitWithMon initializes the MemRowContainer with an explicit monitor. Only
// use this if the default MemRowContainer.Init() function is insufficient.
func (mc *MemRowContainer) InitWithMon(
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

// Types returns the MemRowContainer's types.
func (mc *MemRowContainer) Types() []sqlbase.ColumnType {
	return mc.types
}

// Less is part of heap.Interface and is only meant to be used internally.
func (mc *MemRowContainer) Less(i, j int) bool {
	cmp := sqlbase.CompareDatums(mc.ordering, mc.evalCtx, mc.At(i), mc.At(j))
	if mc.invertSorting {
		cmp = -cmp
	}
	return cmp < 0
}

// EncRow returns the idx-th row as an EncDatumRow. The slice itself is reused
// so it is only valid until the next call to EncRow.
func (mc *MemRowContainer) EncRow(idx int) sqlbase.EncDatumRow {
	datums := mc.At(idx)
	for i, d := range datums {
		mc.scratchEncRow[i] = sqlbase.DatumToEncDatum(mc.types[i], d)
	}
	return mc.scratchEncRow
}

// AddRow adds a row to the container.
func (mc *MemRowContainer) AddRow(ctx context.Context, row sqlbase.EncDatumRow) error {
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

// Sort is part of the SortableRowContainer interface.
func (mc *MemRowContainer) Sort(ctx context.Context) {
	mc.invertSorting = false
	cancelChecker := sqlbase.NewCancelChecker(ctx)
	sqlbase.Sort(mc, cancelChecker)
}

// Reorder implements ReorderableRowContainer. We don't need to create a new
// MemRowContainer and can just change the ordering on-the-fly.
func (mc *MemRowContainer) Reorder(_ context.Context, ordering sqlbase.ColumnOrdering) error {
	mc.ordering = ordering
	return nil
}

// Push is part of heap.Interface.
func (mc *MemRowContainer) Push(_ interface{}) { panic("unimplemented") }

// Pop is part of heap.Interface.
func (mc *MemRowContainer) Pop() interface{} { panic("unimplemented") }

// MaybeReplaceMax replaces the maximum element with the given row, if it is
// smaller. Assumes InitTopK was called.
func (mc *MemRowContainer) MaybeReplaceMax(ctx context.Context, row sqlbase.EncDatumRow) error {
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

// InitTopK rearranges the rows in the MemRowContainer into a Max-Heap.
func (mc *MemRowContainer) InitTopK() {
	mc.invertSorting = true
	heap.Init(mc)
}

// memRowIterator is a RowIterator that iterates over a MemRowContainer. This
// iterator doesn't iterate over a snapshot of MemRowContainer.
type memRowIterator struct {
	*MemRowContainer
	curIdx int
}

var _ RowIterator = &memRowIterator{}

// NewIterator returns an iterator that can be used to iterate over a
// MemRowContainer. Note that this iterator doesn't iterate over a snapshot
// of MemRowContainer.
func (mc *MemRowContainer) NewIterator(_ context.Context) RowIterator {
	return &memRowIterator{MemRowContainer: mc}
}

// Rewind implements the RowIterator interface.
func (i *memRowIterator) Rewind() {
	i.curIdx = 0
}

// Valid implements the RowIterator interface.
func (i *memRowIterator) Valid() (bool, error) {
	return i.curIdx < i.Len(), nil
}

// Next implements the RowIterator interface.
func (i *memRowIterator) Next() {
	i.curIdx++
}

// Row implements the RowIterator interface.
func (i *memRowIterator) Row() (sqlbase.EncDatumRow, error) {
	return i.EncRow(i.curIdx), nil
}

// Close implements the RowIterator interface.
func (i *memRowIterator) Close() {}

// memRowFinalIterator is a RowIterator that iterates over a MemRowContainer.
// This iterator doesn't iterate over a snapshot of MemRowContainer and deletes
// rows as soon as they are iterated over to free up memory eagerly.
type memRowFinalIterator struct {
	*MemRowContainer
}

// NewFinalIterator returns an iterator that can be used to iterate over a
// MemRowContainer. Note that this iterator doesn't iterate over a snapshot
// of MemRowContainer and that it deletes rows as soon as they are iterated
// over.
func (mc *MemRowContainer) NewFinalIterator(_ context.Context) RowIterator {
	return memRowFinalIterator{MemRowContainer: mc}
}

var _ RowIterator = memRowFinalIterator{}

// Rewind implements the RowIterator interface.
func (i memRowFinalIterator) Rewind() {}

// Valid implements the RowIterator interface.
func (i memRowFinalIterator) Valid() (bool, error) {
	return i.Len() > 0, nil
}

// Next implements the RowIterator interface.
func (i memRowFinalIterator) Next() {
	i.PopFirst()
}

// Row implements the RowIterator interface.
func (i memRowFinalIterator) Row() (sqlbase.EncDatumRow, error) {
	return i.EncRow(0), nil
}

// Close implements the RowIterator interface.
func (i memRowFinalIterator) Close() {}

// DiskBackedRowContainer is a ReorderableRowContainer that uses a
// MemRowContainer to store rows and spills back to disk automatically if
// memory usage exceeds a given budget.
type DiskBackedRowContainer struct {
	// src is the current ReorderableRowContainer that is being used to store
	// rows. All the ReorderableRowContainer methods are redefined rather than
	// delegated to an embedded struct because of how defer works:
	// 	rc.Init(...)
	//	defer rc.Close(ctx)
	// The Close will call MemRowContainer.Close(ctx) even after spilling to disk.
	src ReorderableRowContainer

	mrc *MemRowContainer
	drc *DiskRowContainer

	spilled bool

	// The following fields are used to create a DiskRowContainer when spilling
	// to disk.
	engine      diskmap.Factory
	diskMonitor *mon.BytesMonitor
}

var _ SortableRowContainer = &DiskBackedRowContainer{}

// Init initializes a DiskBackedRowContainer.
// Arguments:
//  - ordering is the output ordering; the order in which rows should be sorted.
//  - types is the schema of rows that will be added to this container.
//  - evalCtx defines the context in which to evaluate comparisons, only used
//    when storing rows in memory.
//  - engine is the store used for rows when spilling to disk.
//  - memoryMonitor is used to monitor the DiskBackedRowContainer's memory usage.
//    If this monitor denies an allocation, the DiskBackedRowContainer will
//    spill to disk.
//  - diskMonitor is used to monitor the DiskBackedRowContainer's disk usage if
//    and when it spills to disk.
//  - rowCapacity (if not 0) indicates the number of rows that the underlying
//    in-memory container should be preallocated for.
func (f *DiskBackedRowContainer) Init(
	ordering sqlbase.ColumnOrdering,
	types []sqlbase.ColumnType,
	evalCtx *tree.EvalContext,
	engine diskmap.Factory,
	memoryMonitor *mon.BytesMonitor,
	diskMonitor *mon.BytesMonitor,
	rowCapacity int,
) {
	mrc := MemRowContainer{}
	mrc.InitWithMon(ordering, types, evalCtx, memoryMonitor, rowCapacity)
	f.mrc = &mrc
	f.src = &mrc
	f.engine = engine
	f.diskMonitor = diskMonitor
}

// Len is part of the SortableRowContainer interface.
func (f *DiskBackedRowContainer) Len() int {
	return f.src.Len()
}

// AddRow is part of the SortableRowContainer interface.
func (f *DiskBackedRowContainer) AddRow(ctx context.Context, row sqlbase.EncDatumRow) error {
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

// Sort is part of the SortableRowContainer interface.
func (f *DiskBackedRowContainer) Sort(ctx context.Context) {
	f.src.Sort(ctx)
}

// Reorder implements ReorderableRowContainer.
func (f *DiskBackedRowContainer) Reorder(
	ctx context.Context, ordering sqlbase.ColumnOrdering,
) error {
	if err := f.src.Reorder(ctx, ordering); err != nil {
		return err
	}
	switch c := f.src.(type) {
	case *MemRowContainer:
		f.mrc = c
	case *DiskRowContainer:
		f.drc = c
	default:
		return errors.Errorf("unexpected row container type")
	}
	return nil
}

// InitTopK is part of the SortableRowContainer interface.
func (f *DiskBackedRowContainer) InitTopK() {
	f.src.InitTopK()
}

// MaybeReplaceMax is part of the SortableRowContainer interface.
func (f *DiskBackedRowContainer) MaybeReplaceMax(
	ctx context.Context, row sqlbase.EncDatumRow,
) error {
	return f.src.MaybeReplaceMax(ctx, row)
}

// NewIterator is part of the SortableRowContainer interface.
func (f *DiskBackedRowContainer) NewIterator(ctx context.Context) RowIterator {
	return f.src.NewIterator(ctx)
}

// NewFinalIterator is part of the SortableRowContainer interface.
func (f *DiskBackedRowContainer) NewFinalIterator(ctx context.Context) RowIterator {
	return f.src.NewFinalIterator(ctx)
}

// UnsafeReset resets the container for reuse. The DiskBackedRowContainer will
// reset to use memory if it is using disk.
func (f *DiskBackedRowContainer) UnsafeReset(ctx context.Context) error {
	if f.drc != nil {
		f.drc.Close(ctx)
		f.src = f.mrc
		f.drc = nil
		return nil
	}
	return f.mrc.UnsafeReset(ctx)
}

// Close is part of the SortableRowContainer interface.
func (f *DiskBackedRowContainer) Close(ctx context.Context) {
	if f.drc != nil {
		f.drc.Close(ctx)
	}
	f.mrc.Close(ctx)
}

// Spilled returns whether or not the DiskBackedRowContainer spilled to disk
// in its lifetime.
func (f *DiskBackedRowContainer) Spilled() bool {
	return f.spilled
}

// UsingDisk returns whether or not the DiskBackedRowContainer is currently
// using disk.
func (f *DiskBackedRowContainer) UsingDisk() bool {
	return f.drc != nil
}

// spillIfMemErr checks err and calls spillToDisk if the given err is an out of
// memory error. Returns whether the DiskBackedRowContainer spilled to disk and
// an error if one occurred while doing so.
func (f *DiskBackedRowContainer) spillIfMemErr(ctx context.Context, err error) (bool, error) {
	if pgErr, ok := pgerror.GetPGCause(err); !(ok && pgErr.Code == pgerror.CodeOutOfMemoryError) {
		return false, nil
	}
	if spillErr := f.spillToDisk(ctx); spillErr != nil {
		return false, spillErr
	}
	log.VEventf(ctx, 2, "spilled to disk: %v", err)
	return true, nil
}

func (f *DiskBackedRowContainer) spillToDisk(ctx context.Context) error {
	if f.UsingDisk() {
		return errors.New("already using disk")
	}
	drc := MakeDiskRowContainer(f.diskMonitor, f.mrc.types, f.mrc.ordering, f.engine)
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

// DiskBackedIndexedRowContainer is a wrapper around DiskBackedRowContainer
// that adds an index to each row added in the order of addition of those rows
// by storing an extra int column at the end of each row. These indices can be
// thought of as ordinals of the rows.
//
// Note: although DiskRowContainer appends unique rowIDs to the keys that the
// rows are put at, MemRowContainer doesn't do something like that, so the code
// that utilizes internal rowIDs of DiskRowContainer ends up being worse than
// having this specialized container.
type DiskBackedIndexedRowContainer struct {
	*DiskBackedRowContainer

	ctx           context.Context
	scratchEncRow sqlbase.EncDatumRow
	storedTypes   []sqlbase.ColumnType
	datumAlloc    sqlbase.DatumAlloc
	rowAlloc      sqlbase.EncDatumRowAlloc
	idx           uint64 // the index of the next row to be added into the container

	// These fields are for optimizations when container spilled to disk.
	diskRowIter RowIterator
	idxRowIter  int
	// nextPosToCache is the index of the row to be cached next. If it is greater
	// than 0, the cache contains all rows with position in the range
	// [firstCachedRowPos, nextPosToCache).
	firstCachedRowPos int
	nextPosToCache    int
	indexedRowsCache  ring.Buffer // the cache of up to maxIndexedRowsCacheSize contiguous rows
	cacheMemAcc       mon.BoundAccount
}

// MakeDiskBackedIndexedRowContainer creates a DiskBackedIndexedRowContainer
// with the given engine as the underlying store that rows are stored on when
// it spills to disk.
// Arguments:
//  - ordering is the output ordering; the order in which rows should be sorted.
//  - types is the schema of rows that will be added to this container.
//  - evalCtx defines the context in which to evaluate comparisons, only used
//    when storing rows in memory.
//  - engine is the underlying store that rows are stored on when the container
//    spills to disk.
//  - memoryMonitor is used to monitor this container's memory usage.
//  - diskMonitor is used to monitor this container's disk usage.
//  - rowCapacity (if not 0) specifies the number of rows in-memory container
//    should be preallocated for.
func MakeDiskBackedIndexedRowContainer(
	ordering sqlbase.ColumnOrdering,
	types []sqlbase.ColumnType,
	evalCtx *tree.EvalContext,
	engine diskmap.Factory,
	memoryMonitor *mon.BytesMonitor,
	diskMonitor *mon.BytesMonitor,
	rowCapacity int,
) *DiskBackedIndexedRowContainer {
	d := DiskBackedIndexedRowContainer{}

	// We will be storing an index of each row as the last INT column.
	d.storedTypes = make([]sqlbase.ColumnType, len(types)+1)
	copy(d.storedTypes, types)
	d.storedTypes[len(d.storedTypes)-1] = sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
	d.scratchEncRow = make(sqlbase.EncDatumRow, len(d.storedTypes))
	d.DiskBackedRowContainer = &DiskBackedRowContainer{}
	d.DiskBackedRowContainer.Init(ordering, d.storedTypes, evalCtx, engine, memoryMonitor, diskMonitor, rowCapacity)
	d.cacheMemAcc = memoryMonitor.MakeBoundAccount()
	return &d
}

// AddRow implements SortableRowContainer.
func (f *DiskBackedIndexedRowContainer) AddRow(ctx context.Context, row sqlbase.EncDatumRow) error {
	// TODO(yuzefovich): I'm storing here the context which I need in GetRow().
	// Should I instead modify the interface to take in a context as an argument?
	f.ctx = ctx
	copy(f.scratchEncRow, row)
	f.scratchEncRow[len(f.scratchEncRow)-1] = sqlbase.DatumToEncDatum(
		sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT},
		tree.NewDInt(tree.DInt(f.idx)),
	)
	f.idx++
	return f.DiskBackedRowContainer.AddRow(ctx, f.scratchEncRow)
}

// Reorder implements ReorderableRowContainer.
func (f *DiskBackedIndexedRowContainer) Reorder(
	ctx context.Context, ordering sqlbase.ColumnOrdering,
) error {
	if err := f.DiskBackedRowContainer.Reorder(ctx, ordering); err != nil {
		return err
	}
	f.resetCache()
	f.resetIterator()
	return nil
}

// resetCache resets cache-related fields allowing for reusing the underlying
// already allocated memory. Since all rows in the cache are flushed, it also
// clears the corresponding memory account.
func (f *DiskBackedIndexedRowContainer) resetCache() {
	f.firstCachedRowPos = 0
	f.nextPosToCache = 0
	f.indexedRowsCache.Reset()
	f.cacheMemAcc.Clear(f.ctx)
}

func (f *DiskBackedIndexedRowContainer) resetIterator() {
	if f.diskRowIter != nil {
		f.diskRowIter.Close()
		f.diskRowIter = nil
		f.idxRowIter = 0
	}
}

// UnsafeReset resets the underlying container (if it is using disk, it will be
// reset to using memory).
func (f *DiskBackedIndexedRowContainer) UnsafeReset(ctx context.Context) error {
	f.resetCache()
	f.resetIterator()
	f.idx = 0
	return f.DiskBackedRowContainer.UnsafeReset(ctx)
}

// Close implements SortableRowContainer.
func (f *DiskBackedIndexedRowContainer) Close(ctx context.Context) {
	if f.diskRowIter != nil {
		f.diskRowIter.Close()
	}
	f.cacheMemAcc.Close(f.ctx)
	f.DiskBackedRowContainer.Close(ctx)
}

const maxIndexedRowsCacheSize = 4096

// GetRow implements tree.IndexedRows.
//
// Getting a row by index is fast from an in-memory row container but is a lot
// slower from a disk-backed one. In order to mitigate the impact we add
// optimizations of maintaining a cache of tree.IndexedRow's and storing a disk
// iterator along with the index of the row it currently points at.
func (f *DiskBackedIndexedRowContainer) GetRow(pos int) tree.IndexedRow {
	var rowWithIdx sqlbase.EncDatumRow
	var err error
	if f.UsingDisk() {
		// The cache contains all contiguous rows up to the biggest pos requested
		// so far (even if the rows were not requested explicitly). For example,
		// if the cache is empty and the request comes for a row at pos 3, the
		// cache will contain 4 rows at positions 0, 1, 2, and 3.
		if pos >= f.firstCachedRowPos && pos < f.nextPosToCache {
			requestedRowCachePos := pos - f.firstCachedRowPos
			return f.indexedRowsCache.Get(requestedRowCachePos).(tree.IndexedRow)
		}
		if f.diskRowIter == nil {
			f.diskRowIter = f.DiskBackedRowContainer.drc.NewIterator(f.ctx)
			f.diskRowIter.Rewind()
		}
		if f.idxRowIter > pos {
			// The iterator has been advanced further than we need, so we need to
			// start iterating from the beginning.
			log.Infof(f.ctx, "rewinding: cache contains indices [%d, %d) but index %d requested", f.firstCachedRowPos, f.nextPosToCache, pos)
			f.idxRowIter = 0
			f.diskRowIter.Rewind()
			f.resetCache()
			if pos-maxIndexedRowsCacheSize > f.nextPosToCache {
				// The requested pos is further away from the beginning of the
				// container for the cache to hold all the rows up to pos, so we need
				// to skip exactly pos-maxIndexedRowsCacheSize of them.
				f.nextPosToCache = pos - maxIndexedRowsCacheSize
				f.firstCachedRowPos = f.nextPosToCache
			}
		}
		for ; ; f.diskRowIter.Next() {
			if ok, err := f.diskRowIter.Valid(); err != nil {
				// TODO(yuzefovich): return an error when interface is modified (and
				// below).
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
					if err := rowWithIdx[i].EnsureDecoded(&f.storedTypes[i], &f.datumAlloc); err != nil {
						panic(err)
					}
				}
				row := rowWithIdx[:len(rowWithIdx)-1]
				if rowIdx, ok := rowWithIdx[len(rowWithIdx)-1].Datum.(*tree.DInt); ok {
					if f.indexedRowsCache.Len() == maxIndexedRowsCacheSize {
						// The cache size is capped at maxIndexedRowsCacheSize, so we first
						// remove the row with the smallest pos and advance
						// f.firstCachedRowPos.
						usage := sizeOfInt + int64(f.indexedRowsCache.GetFirst().(IndexedRow).Row.Size())
						f.indexedRowsCache.RemoveFirst()
						f.cacheMemAcc.Shrink(f.ctx, usage)
						f.firstCachedRowPos++
					}
					// We choose to ignore minor details like IndexedRow overhead and
					// the cache overhead.
					usage := sizeOfInt + int64(row.Size())
					if err := f.cacheMemAcc.Grow(f.ctx, usage); err != nil {
						panic(err)
					}
					// We actually need to copy the row into memory.
					ir := IndexedRow{int(*rowIdx), f.rowAlloc.CopyRow(row)}
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
	rowWithIdx = f.DiskBackedRowContainer.mrc.EncRow(pos)
	if rowIdx, ok := rowWithIdx[len(rowWithIdx)-1].Datum.(*tree.DInt); ok {
		return IndexedRow{int(*rowIdx), rowWithIdx[:len(rowWithIdx)-1]}
	}
	panic("unexpected last column: should be integer pos")
}

// IndexedRow is a row with a corresponding index.
type IndexedRow struct {
	Idx int
	Row sqlbase.EncDatumRow
}

// GetIdx implements tree.IndexedRow interface.
func (ir IndexedRow) GetIdx() int {
	return ir.Idx
}

// GetDatum implements tree.IndexedRow interface.
func (ir IndexedRow) GetDatum(colIdx int) tree.Datum {
	return ir.Row[colIdx].Datum
}

// GetDatums implements tree.IndexedRow interface.
func (ir IndexedRow) GetDatums(startColIdx, endColIdx int) tree.Datums {
	datums := make(tree.Datums, 0, endColIdx-startColIdx)
	for idx := startColIdx; idx < endColIdx; idx++ {
		datums = append(datums, ir.Row[idx].Datum)
	}
	return datums
}

const sizeOfInt = int64(unsafe.Sizeof(int(0)))
