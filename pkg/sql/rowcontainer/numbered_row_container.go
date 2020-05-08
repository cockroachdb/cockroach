// Copyright 2020 The Cockroach Authors.
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
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/diskmap"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// DiskBackedNumberedRowContainer that stores a map from idx => row, where idx is a
// 0-based dense numbering. Optionally, if deDup is true, it can de-duplicate the
// rows before assigning a number. It spills to disk if needed.
type DiskBackedNumberedRowContainer struct {
	deDup bool
	rc    *DiskBackedRowContainer

	deduper DeDupingRowContainer

	storedTypes []*types.T
	datumAlloc  sqlbase.DatumAlloc
	rowAlloc    sqlbase.EncDatumRowAlloc
	idx         uint64 // the index of the next row to be added into the container

	rowIter       *numberedDiskRowIterator
	rowIterMemAcc mon.BoundAccount
	DisableCache  bool
}

// NewDiskBackedNumberedRowContainer creates a DiskBackedNumberedRowContainer.
//
// Arguments:
//  - deDup is true if it should de-duplicate.
//  - types is the schema of rows that will be added to this container.
//  - evalCtx defines the context.
//  - engine is the underlying store that rows are stored on when the container
//    spills to disk.
//  - memoryMonitor is used to monitor this container's memory usage.
//  - diskMonitor is used to monitor this container's disk usage.
//  - rowCapacity (if not 0) specifies the number of rows in-memory container
//    should be preallocated for.
func NewDiskBackedNumberedRowContainer(
	deDup bool,
	types []*types.T,
	evalCtx *tree.EvalContext,
	engine diskmap.Factory,
	memoryMonitor *mon.BytesMonitor,
	diskMonitor *mon.BytesMonitor,
	rowCapacity int,
) *DiskBackedNumberedRowContainer {
	d := &DiskBackedNumberedRowContainer{
		deDup:         deDup,
		storedTypes:   types,
		rowIterMemAcc: memoryMonitor.MakeBoundAccount(),
	}
	d.rc = &DiskBackedRowContainer{}
	d.rc.Init(nil /*ordering*/, types, evalCtx, engine, memoryMonitor, diskMonitor, rowCapacity)
	if deDup {
		ordering := make(sqlbase.ColumnOrdering, len(types))
		for i := range types {
			ordering[i].ColIdx = i
			ordering[i].Direction = encoding.Ascending
		}
		deduper := &DiskBackedRowContainer{}
		deduper.Init(ordering, types, evalCtx, engine, memoryMonitor, diskMonitor, rowCapacity)
		deduper.DoDeDuplicate()
		d.deduper = deduper
	}
	return d
}

// Len returns the number of rows in this container.
func (d *DiskBackedNumberedRowContainer) Len() int {
	return int(d.idx)
}

// Spilled returns whether the primary container is spilled.
func (d *DiskBackedNumberedRowContainer) Spilled() bool {
	return d.rc.Spilled()
}

// testingSpillToDisk is for tests to spill the container(s)
// to disk.
func (d *DiskBackedNumberedRowContainer) testingSpillToDisk(ctx context.Context) {
	if !d.rc.UsingDisk() {
		d.rc.SpillToDisk(ctx)
	}
	if d.deDup && !d.deduper.(*DiskBackedRowContainer).UsingDisk() {
		d.deduper.(*DiskBackedRowContainer).SpillToDisk(ctx)
	}
}

// AddRow tries to add a row. It returns the position of the
// row in the container.
func (d *DiskBackedNumberedRowContainer) AddRow(
	ctx context.Context, row sqlbase.EncDatumRow,
) (uint64, error) {
	if d.deDup {
		assignedIdx, err := d.deduper.AddRowWithDeDup(ctx, row)
		if err != nil {
			return 0, err
		}
		if assignedIdx < d.idx {
			// Existing row.
			return assignedIdx, nil
		} else if assignedIdx != d.idx {
			panic(fmt.Sprintf("DiskBackedNumberedRowContainer bug: assignedIdx %d != d.idx %d",
				assignedIdx, d.idx))
		}
		// Else assignedIdx == d.idx, so a new row.
	}
	idx := d.idx
	// An error in AddRow() will cause the two row containers
	// to no longer be in-step with each other wrt the numbering
	// but that is not a concern since the caller will not
	// continue using d after an error.
	d.idx++
	return idx, d.rc.AddRow(ctx, row)
}

// SetupForRead must be called before calling GetRow(). No more AddRow() calls
// are permitted (before UnsafeReset()). See the comment for
// NumberedDiskRowIterator for how we use the future accesses.
func (d *DiskBackedNumberedRowContainer) SetupForRead(ctx context.Context, accesses [][]int) {
	if !d.rc.UsingDisk() {
		return
	}
	rowIter := d.rc.drc.newNumberedIterator(ctx)
	meanBytesPerRow := d.rc.drc.MeanEncodedRowBytes()
	if meanBytesPerRow == 0 {
		meanBytesPerRow = 100 // arbitrary
	}
	const bytesPerSSBlock = 64 * 1024
	meanRowsPerSSBlock := bytesPerSSBlock / meanBytesPerRow
	const maxCacheSize = 4096
	cacheSize := maxCacheSize
	if d.DisableCache {
		// This is not an efficient way to disable the cache, but ok for tests.
		cacheSize = 0
	}
	d.rowIter = newNumberedDiskRowIterator(
		ctx, rowIter, accesses, meanRowsPerSSBlock, cacheSize, &d.rowIterMemAcc)
}

// GetRow returns a row with the given index. If skip is true the row is not
// actually read and just indicates a read that is being skipped. It is used
// to maintain synchronization with the future, since the caller can skip
// accesses for semi-joins and anti-joins.
func (d *DiskBackedNumberedRowContainer) GetRow(
	ctx context.Context, idx uint64, skip bool,
) (sqlbase.EncDatumRow, error) {
	if !d.rc.UsingDisk() {
		if skip {
			return nil, nil
		}
		return d.rc.mrc.EncRow(int(idx)), nil
	}
	return d.rowIter.getRow(ctx, idx, skip)
}

// UnsafeResets resets this container to be reused.
func (d *DiskBackedNumberedRowContainer) UnsafeReset(ctx context.Context) error {
	if d.rowIter != nil {
		d.rowIter.close(ctx)
		d.rowIter = nil
	}
	d.idx = 0
	if err := d.rc.UnsafeReset(ctx); err != nil {
		return err
	}
	if d.deduper != nil {
		if err := d.deduper.UnsafeReset(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Close closes the container.
func (d *DiskBackedNumberedRowContainer) Close(ctx context.Context) {
	if d.rowIter != nil {
		d.rowIter.close(ctx)
	}
	d.rowIterMemAcc.Close(ctx)
	d.rc.Close(ctx)
	if d.deduper != nil {
		d.deduper.Close(ctx)
	}
}

// numberedDiskRowIterator wraps a numberedRowIterator and adds two pieces
// of functionality:
// - decides between seek and next when positioning the iterator.
// - maintains a cache.
//
// Cache:
//
// The callers of GetRow() know the full pattern of future accesses,
// represented as a [][]int where the int is the row number. Within a slice
// they access in increasing order of index (since forward iteration is
// cheaper), but across different slices there is no such constraint. Caching
// policies like LRU work without knowing the future, and use "recency" of row
// R (number of other distinct rows accessed since last access to row R) as a
// proxy for "reuse distance" of row R (number of distinct rows that will be
// accessed from one access of R to the next access of R). More sophisticated
// policies try to use the recently observed reuse distance as a predictor for
// the future reuse distance. In our case we know the exact reuse distance
// from each access of row R to its next access, so one can construct an
// optimal cache policy for a certain cache size: keep track of the current
// reuse distance for each element of the cache and evict the one with the
// highest reuse distance. A cache miss causes the retrieved entry to be added
// to a full cache only if its reuse distance to the next access is less than
// the highest reuse distance currently in the cache. This optimality requires
// some book-keeping overhead:
//
// - A map with O(R) entries where R is the number of unique rows that will be
//   accessed and an overall size proportional to the total number of accesses.
//   Overall this is within a constant factor of [][]int, but the constant could
//   be high. Note that we need this map because when doing Next() on the iterator
//   we encounter entries different from the ones that caused this cache miss
//   and we need to decide whether to cache them -- if we had a random access
//   iterator such that sequential access was the same cost as random
//   access, then a single []int with the next reuse position for each access
//   would have sufficed.
// - A heap containing the rows in the cache that is updated on each cache hit,
//   and whenever a row is evicted or added to the cache. This is O(log N) where
//   N is the number of entries in the cache.
//
// Overall, this may be too much memory and cpu overhead for not enough
// benefit, but it will put an upper bound on what we can achieve with a
// cache. And for inverted index queries involving intersection it is possible
// that the row container contains far more rows than the number of unique
// rows that will be accessed, so a small cache which knows the future could
// be very beneficial. One motivation for this approach was that #48118
// mentioned low observed cache hit rates with a simpler approach. And since
// we turn off ssblock caching for these underlying storage engine, the
// cost of a cache miss is high.
//
// TODO(sumeer):
// - Before integrating with joinReader, try some realistic benchmarks.
// - Use some realistic inverted index workloads (including geospatial) to
//   measure the effect of this cache.
type numberedDiskRowIterator struct {
	rowIter      *numberedRowIterator
	isPositioned bool
	// The current index the rowIter is positioned at, when isPositioned == true.
	idxRowIter uint64
	// The mean number of rows per ssblock.
	meanRowsPerSSBlock int
	// The maximum number of rows in the cache. This can be shrunk under memory
	// pressure.
	maxCacheSize int
	memAcc       *mon.BoundAccount

	// The cache. It contains an entry for all the rows that will be accessed,
	// and not just the ones for which we currently have a cached EncDatumRow.
	cache map[uint64]*cacheElement
	// The current access index in the sequence of all the accesses. This is
	// used to know where we are in the known future.
	accessIdx int
	// A max heap containing only the rows for which we have a cached
	// EncDatumRow. The top element has the highest nextAccess and is the
	// best candidate to evict.
	cacheHeap  cacheMaxNextAccessHeap
	datumAlloc sqlbase.DatumAlloc
	rowAlloc   sqlbase.EncDatumRowAlloc

	hitCount  int
	missCount int
}

type cacheElement struct {
	// The future accesses for this row, expressed as the accessIdx when it will
	// happen. We update this slice to remove the first entry whenever an access
	// happens, so when non-empty, accesses[0] represents the next access, and
	// when empty there are no more accesses left.
	accesses []int
	// row is non-nil for a cached row.
	row sqlbase.EncDatumRow
	// When row is non-nil, this is the element in the heap.
	heapElement cacheRowHeapElement
}

type cacheRowHeapElement struct {
	// The index of this cached row.
	rowIdx uint64
	// The next access of this cached row.
	nextAccess int
	// The index in the heap.
	heapIdx int
}

type cacheMaxNextAccessHeap []*cacheRowHeapElement

func (h cacheMaxNextAccessHeap) Len() int { return len(h) }
func (h cacheMaxNextAccessHeap) Less(i, j int) bool {
	return h[i].nextAccess > h[j].nextAccess
}
func (h cacheMaxNextAccessHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIdx = i
	h[j].heapIdx = j
}
func (h *cacheMaxNextAccessHeap) Push(x interface{}) {
	n := len(*h)
	elem := x.(*cacheRowHeapElement)
	elem.heapIdx = n
	*h = append(*h, elem)
}
func (h *cacheMaxNextAccessHeap) Pop() interface{} {
	old := *h
	n := len(old)
	elem := old[n-1]
	elem.heapIdx = -1
	*h = old[0 : n-1]
	return elem
}

// TODO(sumeer): fix the inconsistency in using int and uint64
// for row indexes. int should be sufficient.
// TODO(sumeer): memory accounting for map and heap.
// TODO(sumeer): proper error strings.
func newNumberedDiskRowIterator(
	_ context.Context,
	rowIter *numberedRowIterator,
	accesses [][]int,
	meanRowsPerSSBlock int,
	maxCacheSize int,
	memAcc *mon.BoundAccount,
) *numberedDiskRowIterator {
	n := &numberedDiskRowIterator{
		rowIter:            rowIter,
		meanRowsPerSSBlock: meanRowsPerSSBlock,
		maxCacheSize:       maxCacheSize,
		memAcc:             memAcc,
		cache:              make(map[uint64]*cacheElement, maxCacheSize),
	}
	var accessIdx int
	for _, accSlice := range accesses {
		for _, rowIdx := range accSlice {
			elem := n.cache[uint64(rowIdx)]
			if elem == nil {
				elem = &cacheElement{}
				elem.heapElement.rowIdx = uint64(rowIdx)
				n.cache[uint64(rowIdx)] = elem
			}
			elem.accesses = append(elem.accesses, accessIdx)
			accessIdx++
		}
	}
	return n
}

func (n *numberedDiskRowIterator) close(ctx context.Context) {
	n.memAcc.Clear(ctx)
	n.rowIter.Close()
}

func (n *numberedDiskRowIterator) getRow(
	ctx context.Context, idx uint64, skip bool,
) (sqlbase.EncDatumRow, error) {
	thisAccessIdx := n.accessIdx
	n.accessIdx++
	elem, ok := n.cache[idx]
	if !ok {
		return nil, errors.Errorf("caller is accessing a row that was not specified up front")
	}
	if len(elem.accesses) == 0 || elem.accesses[0] != thisAccessIdx {
		// TODO(sumeer): should we relax this, since perfect synchronization is not
		// a requirement for correctness.
		return nil, errors.Errorf("caller is no longer synchronized with future accesses")
	}
	elem.accesses = elem.accesses[1:]
	var nextAccess int
	if len(elem.accesses) > 0 {
		nextAccess = elem.accesses[0]
	} else {
		nextAccess = math.MaxInt32
	}

	// Check for cache hit. This also updates the heap position,
	// which we need to do even for skip == true.
	if elem.row != nil {
		n.hitCount++
		elem.heapElement.nextAccess = nextAccess
		heap.Fix(&n.cacheHeap, elem.heapElement.heapIdx)
		if skip {
			return nil, nil
		}
		return elem.row, nil
	}

	// Cache miss.
	n.missCount++
	// If skip, we can just return.
	if skip {
		return nil, nil
	}

	// Need to position the rowIter. We could add Prev(), since the engine supports
	// it, if benchmarks indicate it would help. For now we just Seek() for that
	// case.
	if n.isPositioned && idx >= n.idxRowIter && (int(idx-n.idxRowIter) <= n.meanRowsPerSSBlock) {
		// Need to move forward, possibly within the same ssblock, so use Next().
		// It is possible we are already positioned at the right place.
		for i := int(idx - n.idxRowIter); i > 0; i-- {
			n.rowIter.Next()
			if valid, err := n.rowIter.Valid(); err != nil || !valid {
				if err != nil {
					return nil, err
				}
				return nil, errors.Errorf("caller violated contract")
			}
			n.idxRowIter++
			if i == 0 {
				break
			}
			// i > 0. This is before the row we want to return, but it may
			// be worthwhile to cache it.
			preElem, ok := n.cache[n.idxRowIter]
			if !ok {
				// This is a row that is never accessed.
				continue
			}
			if preElem.row != nil {
				// Already in cache.
				continue
			}
			if len(preElem.accesses) == 0 {
				// No accesses left.
				continue
			}
			if err := n.tryAddCache(ctx, n.idxRowIter, preElem); err != nil {
				return nil, err
			}
		}
		// Try adding to cache
		return n.tryAddCacheAndReturnRow(ctx, idx, elem)
	} else {
		n.rowIter.seekToIndex(idx)
		n.isPositioned = true
		n.idxRowIter = idx
		if valid, err := n.rowIter.Valid(); err != nil || !valid {
			if err != nil {
				return nil, err
			}
			return nil, errors.Errorf("caller violated contract")
		}
		// Try adding to cache
		return n.tryAddCacheAndReturnRow(ctx, idx, elem)
	}
}

func (n *numberedDiskRowIterator) ensureDecodedAndCopy(
	row sqlbase.EncDatumRow,
) (sqlbase.EncDatumRow, error) {
	for i := range row {
		if err := row[i].EnsureDecoded(n.rowIter.rowContainer.types[i], &n.datumAlloc); err != nil {
			return nil, err
		}
	}
	return n.rowAlloc.CopyRow(row), nil
}

func (n *numberedDiskRowIterator) tryAddCacheAndReturnRow(
	ctx context.Context, idx uint64, elem *cacheElement,
) (sqlbase.EncDatumRow, error) {
	r, err := n.rowIter.Row()
	if err != nil {
		return nil, err
	}
	row, err := n.ensureDecodedAndCopy(r)
	if err != nil {
		return nil, err
	}
	return row, n.tryAddCacheHelper(ctx, elem, row, true)
}

func (n *numberedDiskRowIterator) tryAddCache(
	ctx context.Context, idx uint64, elem *cacheElement,
) error {
	// We don't want to pay the cost of rowIter.Row() if the row will not be
	// added to the cache. But to do correct memory accounting, which is needed
	// for the precise caching decision, we do need the EncDatumRow. So we do a
	// cheap check that is a good predictor of whether the row will be cached,
	// and then call rowIter.Row().
	cacheSize := len(n.cacheHeap)
	if cacheSize == n.maxCacheSize && (cacheSize == 0 || n.cacheHeap[0].nextAccess <= elem.accesses[0]) {
		return nil
	}
	row, err := n.rowIter.Row()
	if err != nil {
		return err
	}
	return n.tryAddCacheHelper(ctx, elem, row, false)
}

func (n *numberedDiskRowIterator) tryAddCacheHelper(
	ctx context.Context, elem *cacheElement, row sqlbase.EncDatumRow, alreadyDecodedAndCopied bool,
) error {
	if len(elem.accesses) == 0 {
		return nil
	}
	nextAccess := elem.accesses[0]
	evict := func() error {
		heapElem := n.cacheHeap[0]
		heap.Pop(&n.cacheHeap)
		evictElem, ok := n.cache[heapElem.rowIdx]
		if !ok {
			return errors.Errorf("bug: element not in cache map")
		}
		bytes := evictElem.row.Size()
		n.memAcc.Shrink(ctx, int64(bytes))
		evictElem.row = nil
		return nil
	}
	rowBytesUsage := -1
	for {
		if n.maxCacheSize == 0 {
			return nil
		}
		if len(n.cacheHeap) == n.maxCacheSize && n.cacheHeap[0].nextAccess <= nextAccess {
			return nil
		}
		if len(n.cacheHeap) >= n.maxCacheSize {
			if err := evict(); err != nil {
				return err
			}
			continue
		}

		// We shrink maxCacheSize such that it is a good current indicator of how
		// many rows memAcc will allow us to place in the cache. So it is likely
		// that this row can be added. Decode the row to get the correct
		// rowBytesUsage.
		if !alreadyDecodedAndCopied {
			var err error
			row, err = n.ensureDecodedAndCopy(row)
			if err != nil {
				return err
			}
			alreadyDecodedAndCopied = true
		}
		if rowBytesUsage == -1 {
			rowBytesUsage = int(row.Size())
		}
		if err := n.memAcc.Grow(ctx, int64(rowBytesUsage)); err != nil {
			if sqlbase.IsOutOfMemoryError(err) {
				// Could not grow the memory to handle this row, so reduce the
				// maxCacheSize (max count of entries), to the current number of
				// entries in the cache. The assumption here is that rows in the cache
				// are of similar size. Using maxCacheSize to make eviction decisions
				// is cheaper than calling Grow().
				n.maxCacheSize = len(n.cacheHeap)
				continue
			} else {
				return err
			}
		} else {
			// There is room in the cache.
			break
		}
	}
	// Add to cache.
	elem.heapElement.nextAccess = nextAccess
	elem.row = row
	heap.Push(&n.cacheHeap, &elem.heapElement)
	return nil
}
