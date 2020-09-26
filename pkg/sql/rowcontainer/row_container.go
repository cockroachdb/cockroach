// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowcontainer

import (
	"container/heap"
	"context"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/diskmap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/ring"
	"github.com/cockroachdb/cockroach/pkg/util/sort"
	"github.com/cockroachdb/errors"
)

// SortableRowContainer is a container used to store rows and optionally sort
// these.
type SortableRowContainer interface {
	Len() int
	// AddRow adds a row to the container. If an error is returned, then the
	// row wasn't actually added.
	AddRow(context.Context, rowenc.EncDatumRow) error
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
	MaybeReplaceMax(context.Context, rowenc.EncDatumRow) error

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
	Reorder(context.Context, colinfo.ColumnOrdering) error
}

// IndexedRowContainer is a ReorderableRowContainer which also implements
// tree.IndexedRows. It allows retrieving a row at a particular index.
type IndexedRowContainer interface {
	ReorderableRowContainer

	// GetRow returns a row at the given index or an error.
	GetRow(ctx context.Context, idx int) (tree.IndexedRow, error)
}

// DeDupingRowContainer is a container that de-duplicates rows added to the
// container, and assigns them a dense index starting from 0, representing
// when that row was first added. It only supports a configuration where all
// the columns are encoded into the key -- relaxing this is not hard, but is
// not worth adding the code without a use for it.
type DeDupingRowContainer interface {
	// AddRowWithDeDup adds the given row if not already present in the
	// container. It returns the dense number of when the row is first
	// added.
	AddRowWithDeDup(context.Context, rowenc.EncDatumRow) (int, error)
	// UnsafeReset resets the container, allowing for reuse. It renders all
	// previously allocated rows unsafe.
	UnsafeReset(context.Context) error
	// Close frees up resources held by the container.
	Close(context.Context)
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
	Row() (rowenc.EncDatumRow, error)

	// Close frees up resources held by the iterator.
	Close()
}

// MemRowContainer is the wrapper around rowcontainer.RowContainer that
// provides more functionality, especially around converting to/from
// EncDatumRows and facilitating sorting.
type MemRowContainer struct {
	RowContainer
	types         []*types.T
	invertSorting bool // Inverts the sorting predicate.
	ordering      colinfo.ColumnOrdering
	scratchRow    tree.Datums
	scratchEncRow rowenc.EncDatumRow

	evalCtx *tree.EvalContext

	datumAlloc rowenc.DatumAlloc
}

var _ heap.Interface = &MemRowContainer{}
var _ IndexedRowContainer = &MemRowContainer{}

// Init initializes the MemRowContainer. The MemRowContainer uses evalCtx.Mon
// to track memory usage.
func (mc *MemRowContainer) Init(
	ordering colinfo.ColumnOrdering, types []*types.T, evalCtx *tree.EvalContext,
) {
	mc.InitWithMon(ordering, types, evalCtx, evalCtx.Mon)
}

// InitWithMon initializes the MemRowContainer with an explicit monitor. Only
// use this if the default MemRowContainer.Init() function is insufficient.
func (mc *MemRowContainer) InitWithMon(
	ordering colinfo.ColumnOrdering,
	types []*types.T,
	evalCtx *tree.EvalContext,
	mon *mon.BytesMonitor,
) {
	acc := mon.MakeBoundAccount()
	mc.RowContainer.Init(acc, colinfo.ColTypeInfoFromColTypes(types), 0 /* rowCapacity */)
	mc.types = types
	mc.ordering = ordering
	mc.scratchRow = make(tree.Datums, len(types))
	mc.scratchEncRow = make(rowenc.EncDatumRow, len(types))
	mc.evalCtx = evalCtx
}

// Less is part of heap.Interface and is only meant to be used internally.
func (mc *MemRowContainer) Less(i, j int) bool {
	cmp := colinfo.CompareDatums(mc.ordering, mc.evalCtx, mc.At(i), mc.At(j))
	if mc.invertSorting {
		cmp = -cmp
	}
	return cmp < 0
}

// EncRow returns the idx-th row as an EncDatumRow. The slice itself is reused
// so it is only valid until the next call to EncRow.
func (mc *MemRowContainer) EncRow(idx int) rowenc.EncDatumRow {
	datums := mc.At(idx)
	for i, d := range datums {
		mc.scratchEncRow[i] = rowenc.DatumToEncDatum(mc.types[i], d)
	}
	return mc.scratchEncRow
}

// AddRow adds a row to the container.
func (mc *MemRowContainer) AddRow(ctx context.Context, row rowenc.EncDatumRow) error {
	if len(row) != len(mc.types) {
		log.Fatalf(ctx, "invalid row length %d, expected %d", len(row), len(mc.types))
	}
	for i := range row {
		err := row[i].EnsureDecoded(mc.types[i], &mc.datumAlloc)
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
	cancelChecker := cancelchecker.NewCancelChecker(ctx)
	sort.Sort(mc, cancelChecker)
}

// Reorder implements ReorderableRowContainer. We don't need to create a new
// MemRowContainer and can just change the ordering on-the-fly.
func (mc *MemRowContainer) Reorder(_ context.Context, ordering colinfo.ColumnOrdering) error {
	mc.ordering = ordering
	return nil
}

// Push is part of heap.Interface.
func (mc *MemRowContainer) Push(_ interface{}) { panic("unimplemented") }

// Pop is part of heap.Interface.
func (mc *MemRowContainer) Pop() interface{} { panic("unimplemented") }

// MaybeReplaceMax replaces the maximum element with the given row, if it is
// smaller. Assumes InitTopK was called.
func (mc *MemRowContainer) MaybeReplaceMax(ctx context.Context, row rowenc.EncDatumRow) error {
	max := mc.At(0)
	cmp, err := row.CompareToDatums(mc.types, &mc.datumAlloc, mc.ordering, mc.evalCtx, max)
	if err != nil {
		return err
	}
	if cmp < 0 {
		// row is smaller than the max; replace.
		for i := range row {
			if err := row[i].EnsureDecoded(mc.types[i], &mc.datumAlloc); err != nil {
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
func (i *memRowIterator) Row() (rowenc.EncDatumRow, error) {
	return i.EncRow(i.curIdx), nil
}

// Close implements the RowIterator interface.
func (i *memRowIterator) Close() {}

// memRowFinalIterator is a RowIterator that iterates over a MemRowContainer.
// This iterator doesn't iterate over a snapshot of MemRowContainer and deletes
// rows as soon as they are iterated over to free up memory eagerly.
type memRowFinalIterator struct {
	*MemRowContainer

	ctx context.Context
}

// NewFinalIterator returns an iterator that can be used to iterate over a
// MemRowContainer. Note that this iterator doesn't iterate over a snapshot
// of MemRowContainer and that it deletes rows as soon as they are iterated
// over.
func (mc *MemRowContainer) NewFinalIterator(ctx context.Context) RowIterator {
	return memRowFinalIterator{MemRowContainer: mc, ctx: ctx}
}

// GetRow implements IndexedRowContainer.
func (mc *MemRowContainer) GetRow(ctx context.Context, pos int) (tree.IndexedRow, error) {
	return IndexedRow{Idx: pos, Row: mc.EncRow(pos)}, nil
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
	i.PopFirst(i.ctx)
}

// Row implements the RowIterator interface.
func (i memRowFinalIterator) Row() (rowenc.EncDatumRow, error) {
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

	// See comment in DoDeDuplicate().
	deDuplicate bool
	keyToIndex  map[string]int
	// Encoding helpers for de-duplication:
	// encodings keeps around the DatumEncoding equivalents of the encoding
	// directions in ordering to avoid conversions in hot paths.
	encodings  []descpb.DatumEncoding
	datumAlloc rowenc.DatumAlloc
	scratchKey []byte

	spilled bool

	// The following fields are used to create a DiskRowContainer when spilling
	// to disk.
	engine      diskmap.Factory
	diskMonitor *mon.BytesMonitor
}

var _ ReorderableRowContainer = &DiskBackedRowContainer{}
var _ DeDupingRowContainer = &DiskBackedRowContainer{}

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
func (f *DiskBackedRowContainer) Init(
	ordering colinfo.ColumnOrdering,
	types []*types.T,
	evalCtx *tree.EvalContext,
	engine diskmap.Factory,
	memoryMonitor *mon.BytesMonitor,
	diskMonitor *mon.BytesMonitor,
) {
	mrc := MemRowContainer{}
	mrc.InitWithMon(ordering, types, evalCtx, memoryMonitor)
	f.mrc = &mrc
	f.src = &mrc
	f.engine = engine
	f.diskMonitor = diskMonitor
	f.encodings = make([]descpb.DatumEncoding, len(ordering))
	for i, orderInfo := range ordering {
		f.encodings[i] = rowenc.EncodingDirToDatumEncoding(orderInfo.Direction)
	}
}

// DoDeDuplicate causes DiskBackedRowContainer to behave as an implementation
// of DeDupingRowContainer. It should not be mixed with calls to AddRow(). It
// de-duplicates the keys such that only the first row with the given key will
// be stored. The index returned in AddRowWithDedup() is a dense index
// starting from 0, representing when that key was first added. This feature
// does not combine with Sort(), Reorder() etc., and only to be used for
// assignment of these dense indexes. The main reason to add this to
// DiskBackedRowContainer is to avoid significant code duplication in
// constructing another row container.
func (f *DiskBackedRowContainer) DoDeDuplicate() {
	f.deDuplicate = true
	f.keyToIndex = make(map[string]int)
}

// Len is part of the SortableRowContainer interface.
func (f *DiskBackedRowContainer) Len() int {
	return f.src.Len()
}

// AddRow is part of the SortableRowContainer interface.
func (f *DiskBackedRowContainer) AddRow(ctx context.Context, row rowenc.EncDatumRow) error {
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

// AddRowWithDeDup is part of the DeDupingRowContainer interface.
func (f *DiskBackedRowContainer) AddRowWithDeDup(
	ctx context.Context, row rowenc.EncDatumRow,
) (int, error) {
	if !f.UsingDisk() {
		if err := f.encodeKey(ctx, row); err != nil {
			return 0, err
		}
		encodedStr := string(f.scratchKey)
		idx, ok := f.keyToIndex[encodedStr]
		if ok {
			return idx, nil
		}
		idx = f.Len()
		if err := f.AddRow(ctx, row); err != nil {
			return 0, err
		}
		// AddRow may have spilled and deleted the map.
		if !f.UsingDisk() {
			f.keyToIndex[encodedStr] = idx
		}
		return idx, nil
	}
	// Using disk.
	return f.drc.AddRowWithDeDup(ctx, row)
}

func (f *DiskBackedRowContainer) encodeKey(ctx context.Context, row rowenc.EncDatumRow) error {
	if len(row) != len(f.mrc.types) {
		log.Fatalf(ctx, "invalid row length %d, expected %d", len(row), len(f.mrc.types))
	}
	f.scratchKey = f.scratchKey[:0]
	for i, orderInfo := range f.mrc.ordering {
		col := orderInfo.ColIdx
		var err error
		f.scratchKey, err = row[col].Encode(f.mrc.types[col], &f.datumAlloc, f.encodings[i], f.scratchKey)
		if err != nil {
			return err
		}
	}
	return nil
}

// Sort is part of the SortableRowContainer interface.
func (f *DiskBackedRowContainer) Sort(ctx context.Context) {
	f.src.Sort(ctx)
}

// Reorder implements ReorderableRowContainer.
func (f *DiskBackedRowContainer) Reorder(
	ctx context.Context, ordering colinfo.ColumnOrdering,
) error {
	return f.src.Reorder(ctx, ordering)
}

// InitTopK is part of the SortableRowContainer interface.
func (f *DiskBackedRowContainer) InitTopK() {
	f.src.InitTopK()
}

// MaybeReplaceMax is part of the SortableRowContainer interface.
func (f *DiskBackedRowContainer) MaybeReplaceMax(
	ctx context.Context, row rowenc.EncDatumRow,
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
	if f.deDuplicate {
		f.keyToIndex = make(map[string]int)
	}
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
	if f.deDuplicate {
		f.keyToIndex = nil
	}
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

// spillIfMemErr checks err and calls SpillToDisk if the given err is an out of
// memory error. Returns whether the DiskBackedRowContainer spilled to disk and
// an error if one occurred while doing so.
func (f *DiskBackedRowContainer) spillIfMemErr(ctx context.Context, err error) (bool, error) {
	if !sqlerrors.IsOutOfMemoryError(err) {
		return false, nil
	}
	if spillErr := f.SpillToDisk(ctx); spillErr != nil {
		return false, spillErr
	}
	log.VEventf(ctx, 2, "spilled to disk: %v", err)
	return true, nil
}

// SpillToDisk creates a disk row container, injects all the data from the
// in-memory container into it, and clears the in-memory one afterwards.
func (f *DiskBackedRowContainer) SpillToDisk(ctx context.Context) error {
	if f.UsingDisk() {
		return errors.New("already using disk")
	}
	drc := MakeDiskRowContainer(f.diskMonitor, f.mrc.types, f.mrc.ordering, f.engine)
	if f.deDuplicate {
		drc.DoDeDuplicate()
		// After spilling to disk we don't need this map to de-duplicate. The
		// DiskRowContainer will do the de-duplication. Calling AddRow() below
		// is correct since these rows are already de-duplicated.
		f.keyToIndex = nil
	}
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

	scratchEncRow rowenc.EncDatumRow
	storedTypes   []*types.T
	datumAlloc    rowenc.DatumAlloc
	rowAlloc      rowenc.EncDatumRowAlloc
	idx           uint64 // the index of the next row to be added into the container

	// These fields are for optimizations when container spilled to disk.
	diskRowIter RowIterator
	idxRowIter  int
	// nextPosToCache is the index of the row to be cached next. If it is greater
	// than 0, the cache contains all rows with position in the range
	// [firstCachedRowPos, nextPosToCache).
	firstCachedRowPos int
	nextPosToCache    int
	// indexedRowsCache is the cache of up to maxCacheSize contiguous rows.
	indexedRowsCache ring.Buffer
	// maxCacheSize indicates the maximum number of rows to be cached. It is
	// initialized to maxIndexedRowsCacheSize and dynamically adjusted if OOM
	// error is encountered.
	maxCacheSize int
	cacheMemAcc  mon.BoundAccount
	hitCount     int
	missCount    int

	// DisableCache is intended for testing only. It can be set to true to
	// disable reading and writing from the row cache.
	DisableCache bool
}

var _ IndexedRowContainer = &DiskBackedIndexedRowContainer{}

// NewDiskBackedIndexedRowContainer creates a DiskBackedIndexedRowContainer
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
func NewDiskBackedIndexedRowContainer(
	ordering colinfo.ColumnOrdering,
	typs []*types.T,
	evalCtx *tree.EvalContext,
	engine diskmap.Factory,
	memoryMonitor *mon.BytesMonitor,
	diskMonitor *mon.BytesMonitor,
) *DiskBackedIndexedRowContainer {
	d := DiskBackedIndexedRowContainer{}

	// We will be storing an index of each row as the last INT column.
	d.storedTypes = make([]*types.T, len(typs)+1)
	copy(d.storedTypes, typs)
	d.storedTypes[len(d.storedTypes)-1] = types.Int
	d.scratchEncRow = make(rowenc.EncDatumRow, len(d.storedTypes))
	d.DiskBackedRowContainer = &DiskBackedRowContainer{}
	d.DiskBackedRowContainer.Init(ordering, d.storedTypes, evalCtx, engine, memoryMonitor, diskMonitor)
	d.maxCacheSize = maxIndexedRowsCacheSize
	d.cacheMemAcc = memoryMonitor.MakeBoundAccount()
	return &d
}

// AddRow implements SortableRowContainer.
func (f *DiskBackedIndexedRowContainer) AddRow(ctx context.Context, row rowenc.EncDatumRow) error {
	copy(f.scratchEncRow, row)
	f.scratchEncRow[len(f.scratchEncRow)-1] = rowenc.DatumToEncDatum(
		types.Int,
		tree.NewDInt(tree.DInt(f.idx)),
	)
	f.idx++
	return f.DiskBackedRowContainer.AddRow(ctx, f.scratchEncRow)
}

// Reorder implements ReorderableRowContainer.
func (f *DiskBackedIndexedRowContainer) Reorder(
	ctx context.Context, ordering colinfo.ColumnOrdering,
) error {
	if err := f.DiskBackedRowContainer.Reorder(ctx, ordering); err != nil {
		return err
	}
	f.resetCache(ctx)
	f.resetIterator()
	return nil
}

// resetCache resets cache-related fields allowing for reusing the underlying
// already allocated memory. Since all rows in the cache are flushed, it also
// clears the corresponding memory account.
func (f *DiskBackedIndexedRowContainer) resetCache(ctx context.Context) {
	f.firstCachedRowPos = 0
	f.nextPosToCache = 0
	f.indexedRowsCache.Reset()
	f.cacheMemAcc.Clear(ctx)
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
	f.resetCache(ctx)
	f.resetIterator()
	f.idx = 0
	return f.DiskBackedRowContainer.UnsafeReset(ctx)
}

// Close implements SortableRowContainer.
func (f *DiskBackedIndexedRowContainer) Close(ctx context.Context) {
	if f.diskRowIter != nil {
		f.diskRowIter.Close()
	}
	f.cacheMemAcc.Close(ctx)
	f.DiskBackedRowContainer.Close(ctx)
}

const maxIndexedRowsCacheSize = 4096

// GetRow implements tree.IndexedRows.
//
// Getting a row by index is fast from an in-memory row container but is a lot
// slower from a disk-backed one. In order to mitigate the impact we add
// optimizations of maintaining a cache of tree.IndexedRow's and storing a disk
// iterator along with the index of the row it currently points at.
func (f *DiskBackedIndexedRowContainer) GetRow(
	ctx context.Context, pos int,
) (tree.IndexedRow, error) {
	var rowWithIdx rowenc.EncDatumRow
	var err error
	if f.UsingDisk() {
		if f.DisableCache {
			return f.getRowWithoutCache(ctx, pos), nil
		}
		// The cache contains all contiguous rows up to the biggest pos requested
		// so far (even if the rows were not requested explicitly). For example,
		// if the cache is empty and the request comes for a row at pos 3, the
		// cache will contain 4 rows at positions 0, 1, 2, and 3.
		if pos >= f.firstCachedRowPos && pos < f.nextPosToCache {
			requestedRowCachePos := pos - f.firstCachedRowPos
			f.hitCount++
			return f.indexedRowsCache.Get(requestedRowCachePos).(tree.IndexedRow), nil
		}
		f.missCount++
		if f.diskRowIter == nil {
			f.diskRowIter = f.DiskBackedRowContainer.drc.NewIterator(ctx)
			f.diskRowIter.Rewind()
		}
		if f.idxRowIter > pos {
			// The iterator has been advanced further than we need, so we need to
			// start iterating from the beginning.
			log.VEventf(ctx, 1, "rewinding: cache contains indices [%d, %d) but index %d requested", f.firstCachedRowPos, f.nextPosToCache, pos)
			f.idxRowIter = 0
			f.diskRowIter.Rewind()
			f.resetCache(ctx)
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
				return nil, err
			} else if !ok {
				return nil, errors.Errorf("row at pos %d not found", pos)
			}
			if f.idxRowIter == f.nextPosToCache {
				rowWithIdx, err = f.diskRowIter.Row()
				if err != nil {
					return nil, err
				}
				for i := range rowWithIdx {
					if err := rowWithIdx[i].EnsureDecoded(f.storedTypes[i], &f.datumAlloc); err != nil {
						return nil, err
					}
				}
				row, rowIdx := rowWithIdx[:len(rowWithIdx)-1], rowWithIdx[len(rowWithIdx)-1].Datum
				if idx, ok := rowIdx.(*tree.DInt); ok {
					if f.indexedRowsCache.Len() == f.maxCacheSize {
						// The cache size is capped at f.maxCacheSize, so we reuse the row
						// with the smallest pos, put it as the last row, and advance
						// f.firstCachedRowPos.
						if err := f.reuseFirstRowInCache(ctx, int(*idx), row); err != nil {
							return nil, err
						}
					} else {
						// We choose to ignore minor details like IndexedRow overhead and
						// the cache overhead.
						usage := sizeOfInt + int64(row.Size())
						if err := f.cacheMemAcc.Grow(ctx, usage); err != nil {
							if sqlerrors.IsOutOfMemoryError(err) {
								// We hit the memory limit, so we need to cap the cache size
								// and reuse the memory underlying first row in the cache.
								if f.indexedRowsCache.Len() == 0 {
									// The cache is empty, so there is no memory to be reused.
									return nil, err
								}
								f.maxCacheSize = f.indexedRowsCache.Len()
								if err := f.reuseFirstRowInCache(ctx, int(*idx), row); err != nil {
									return nil, err
								}
							} else {
								return nil, err
							}
						} else {
							// We actually need to copy the row into memory.
							ir := IndexedRow{int(*idx), f.rowAlloc.CopyRow(row)}
							f.indexedRowsCache.AddLast(ir)
						}
					}
					f.nextPosToCache++
				} else {
					return nil, errors.Errorf("unexpected last column type: should be DInt but found %T", idx)
				}
				if f.idxRowIter == pos {
					return f.indexedRowsCache.GetLast().(tree.IndexedRow), nil
				}
			}
			f.idxRowIter++
		}
	}
	rowWithIdx = f.DiskBackedRowContainer.mrc.EncRow(pos)
	row, rowIdx := rowWithIdx[:len(rowWithIdx)-1], rowWithIdx[len(rowWithIdx)-1].Datum
	if idx, ok := rowIdx.(*tree.DInt); ok {
		return IndexedRow{int(*idx), row}, nil
	}
	return nil, errors.Errorf("unexpected last column type: should be DInt but found %T", rowIdx)
}

// reuseFirstRowInCache reuses the underlying memory of the first row in the
// cache to store 'row' and puts it as the last one in the cache. It adjusts
// the memory account accordingly and, if necessary, removes some first rows.
func (f *DiskBackedIndexedRowContainer) reuseFirstRowInCache(
	ctx context.Context, idx int, row rowenc.EncDatumRow,
) error {
	newRowSize := row.Size()
	for {
		if f.indexedRowsCache.Len() == 0 {
			return errors.Errorf("unexpectedly the cache of DiskBackedIndexedRowContainer contains zero rows")
		}
		indexedRowToReuse := f.indexedRowsCache.GetFirst().(IndexedRow)
		oldRowSize := indexedRowToReuse.Row.Size()
		delta := int64(newRowSize - oldRowSize)
		if delta > 0 {
			// New row takes up more memory than the old one.
			if err := f.cacheMemAcc.Grow(ctx, delta); err != nil {
				if sqlerrors.IsOutOfMemoryError(err) {
					// We need to actually reduce the cache size, so we remove the first
					// row and adjust the memory account, maxCacheSize, and
					// f.firstCachedRowPos accordingly.
					f.indexedRowsCache.RemoveFirst()
					f.cacheMemAcc.Shrink(ctx, int64(oldRowSize))
					f.maxCacheSize--
					f.firstCachedRowPos++
					if f.indexedRowsCache.Len() == 0 {
						return err
					}
					continue
				}
				return err
			}
		} else if delta < 0 {
			f.cacheMemAcc.Shrink(ctx, -delta)
		}
		indexedRowToReuse.Idx = idx
		copy(indexedRowToReuse.Row, row)
		f.indexedRowsCache.RemoveFirst()
		f.indexedRowsCache.AddLast(indexedRowToReuse)
		f.firstCachedRowPos++
		return nil
	}
}

// getRowWithoutCache returns the row at requested position without using the
// cache. It utilizes the same disk row iterator along multiple consequent
// calls and rewinds the iterator only when it has been advanced further than
// the position requested.
//
// NOTE: this method should only be used for testing purposes.
func (f *DiskBackedIndexedRowContainer) getRowWithoutCache(
	ctx context.Context, pos int,
) tree.IndexedRow {
	if !f.UsingDisk() {
		panic(errors.Errorf("getRowWithoutCache is called when the container is using memory"))
	}
	if f.diskRowIter == nil {
		f.diskRowIter = f.DiskBackedRowContainer.drc.NewIterator(ctx)
		f.diskRowIter.Rewind()
	}
	if f.idxRowIter > pos {
		// The iterator has been advanced further than we need, so we need to
		// start iterating from the beginning.
		f.idxRowIter = 0
		f.diskRowIter.Rewind()
	}
	for ; ; f.diskRowIter.Next() {
		if ok, err := f.diskRowIter.Valid(); err != nil {
			panic(err)
		} else if !ok {
			panic(errors.AssertionFailedf("row at pos %d not found", pos))
		}
		if f.idxRowIter == pos {
			rowWithIdx, err := f.diskRowIter.Row()
			if err != nil {
				panic(err)
			}
			for i := range rowWithIdx {
				if err := rowWithIdx[i].EnsureDecoded(f.storedTypes[i], &f.datumAlloc); err != nil {
					panic(err)
				}
			}
			row, rowIdx := rowWithIdx[:len(rowWithIdx)-1], rowWithIdx[len(rowWithIdx)-1].Datum
			if idx, ok := rowIdx.(*tree.DInt); ok {
				return IndexedRow{int(*idx), f.rowAlloc.CopyRow(row)}
			}
			panic(errors.Errorf("unexpected last column type: should be DInt but found %T", rowIdx))
		}
		f.idxRowIter++
	}
}

// IndexedRow is a row with a corresponding index.
type IndexedRow struct {
	Idx int
	Row rowenc.EncDatumRow
}

// GetIdx implements tree.IndexedRow interface.
func (ir IndexedRow) GetIdx() int {
	return ir.Idx
}

// GetDatum implements tree.IndexedRow interface.
func (ir IndexedRow) GetDatum(colIdx int) (tree.Datum, error) {
	return ir.Row[colIdx].Datum, nil
}

// GetDatums implements tree.IndexedRow interface.
func (ir IndexedRow) GetDatums(startColIdx, endColIdx int) (tree.Datums, error) {
	datums := make(tree.Datums, 0, endColIdx-startColIdx)
	for idx := startColIdx; idx < endColIdx; idx++ {
		datums = append(datums, ir.Row[idx].Datum)
	}
	return datums, nil
}

const sizeOfInt = int64(unsafe.Sizeof(int(0)))
