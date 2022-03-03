// Copyright 2017 Andy Kimball
// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tscache

import (
	"bytes"
	"container/list"
	"context"
	"fmt"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/andy-kimball/arenaskl"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// rangeOptions are passed to AddRange to indicate the bounds of the range. By
// default, the "from" and "to" keys are inclusive. Setting these bit flags
// indicates that one or both is exclusive instead.
type rangeOptions int

const (
	// excludeFrom indicates that the range does not include the starting key.
	excludeFrom = rangeOptions(1 << iota)

	// excludeTo indicates that the range does not include the ending key.
	excludeTo
)

// nodeOptions are meta tags on skiplist nodes that indicate the status and role
// of that node in the intervalSkl. The options are bit flags that can be
// independently added and removed.
//
// Each node in the intervalSkl holds a key and, optionally, the latest read
// timestamp for that key. In addition, the node optionally holds the latest
// read timestamp for the range of keys between itself and the next key that is
// present in the skiplist. This space between keys is called the "gap", and the
// timestamp for that range is called the "gap timestamp". Here is a simplified
// representation that would result after these ranges were added to an empty
// intervalSkl:
//   ["apple", "orange") = 200
//   ["kiwi", "raspberry"] = 100
//
//   "apple"    "orange"   "raspberry"
//   keyts=200  keyts=100  keyts=100
//   gapts=200  gapts=100  gapts=0
//
// That is, the range from apple (inclusive) to orange (exclusive) has a read
// timestamp of 200. The range from orange (inclusive) to raspberry (inclusive)
// has a read timestamp of 100. All other keys have a read timestamp of 0.
type nodeOptions int

const (
	// initialized indicates that the node has been created and fully
	// initialized. Key and gap values are final, and can now be used.
	initialized = 1 << iota

	// cantInit indicates that the node should never be allowed to initialize.
	// This is set on nodes which were unable to ratchet their values at some
	// point because of a full arena. In this case, the node's values should
	// never become final and any goroutines trying to initialize it it will be
	// forced to create it again in a new page when they notice this flag.
	cantInit

	// hasKey indicates that the node has an associated key value. If this is
	// not set, then the key timestamp is assumed to be zero and the key is
	// assumed to not have a corresponding txnID.
	hasKey

	// hasGap indicates that the node has an associated gap value. If this is
	// not set, then the gap timestamp is assumed to be zero and the gap is
	// assumed to not have a corresponding txnID.
	hasGap
)

const (
	encodedValSize = int(unsafe.Sizeof(cacheValue{}))

	// initialSklPageSize is the initial size of each page in the sklImpl's
	// intervalSkl. The pages start small to limit the memory footprint of
	// the data structure for short-lived tests. Reducing this size can hurt
	// performance but it decreases the risk of OOM failures when many tests
	// are running concurrently.
	initialSklPageSize = 128 << 10 // 128 KB
	// maximumSklPageSize is the maximum size of each page in the sklImpl's
	// intervalSkl. A long-running server is expected to settle on pages of
	// this size under steady-state load.
	maximumSklPageSize = 32 << 20 // 32 MB

	defaultMinSklPages = 2
)

// initialSklAllocSize is the amount of space in its arena that an empty
// arenaskl.Skiplist consumes.
var initialSklAllocSize = func() int {
	a := arenaskl.NewArena(1000)
	_ = arenaskl.NewSkiplist(a)
	return int(a.Size())
}()

// intervalSkl efficiently tracks the latest logical time at which any key or
// range of keys has been accessed. Keys are binary values of any length, and
// times are represented as hybrid logical timestamps (see hlc package). The
// data structure guarantees that the read timestamp of any given key or range
// will never decrease. In other words, if a lookup returns timestamp A and
// repeating the same lookup returns timestamp B, then B >= A.
//
// Add and lookup operations do not block or interfere with one another, which
// enables predictable operation latencies. Also, the impact of the structure on
// the GC is virtually nothing, even when the structure is very large. These
// properties are enabled by employing a lock-free skiplist implementation that
// uses an arena allocator. Skiplist nodes refer to one another by offset into
// the arena rather than by pointer, so the GC has very few objects to track.
//
//
// The data structure can conceptually be thought of as being parameterized over
// a key and a value type, such that the key implements a Comparable interface
// (see interval.Comparable) and the value implements a Ratchetable interface:
//
//   type Ratchetable interface {
//     Ratchet(other Ratchetable) (changed bool)
//   }
//
// In other words, if Go supported zero-cost abstractions, this type might look
// like:
//
//   type intervalSkl<K: Comparable, V: Ratchetable>
//
type intervalSkl struct {
	// rotMutex synchronizes page rotation with all other operations. The read
	// lock is acquired by the Add and Lookup operations. The write lock is
	// acquired only when the pages are rotated. Since that is very rare, the
	// vast majority of operations can proceed without blocking.
	rotMutex syncutil.RWMutex

	// The following fields are used to enforce a minimum retention window on
	// all timestamp intervals. intervalSkl promises to retain all timestamp
	// intervals until they are at least this old before allowing the floor
	// timestamp to ratchet and subsume them. If clock is nil then no minimum
	// retention policy will be employed.
	clock  *hlc.Clock
	minRet time.Duration

	// The size of the last allocated page in the data structure, in bytes. When
	// a page fills, a new page will be allocate, the pages will be rotated, and
	// older entries will be discarded. Page sizes grow exponentially as pages
	// are allocated up to a maximum of maximumSklPageSize. The value will never
	// regress over the lifetime of an intervalSkl instance.
	//
	// The entire data structure is typically bound to a maximum a size of
	// maximumSklPageSize*minPages. However, this limit can be violated if the
	// intervalSkl needs to grow larger to enforce a minimum retention policy.
	pageSize      uint32
	pageSizeFixed bool // testing only

	// The linked list maintains fixed-size skiplist pages, ordered by creation
	// time such that the first page is the one most recently created. When the
	// first page fills, a new empty page is prepended to the front of the list
	// and all others are pushed back. This first page is the only sklPage that
	// is written to, all others are immutable after they have left the front of
	// the list. However, earlier pages are accessed whenever necessary during
	// lookups. Pages are evicted when they become too old, subject to a minimum
	// retention policy described above.
	pages    list.List // List<*sklPage>
	minPages int

	// In order to ensure that timestamps never decrease, intervalSkl maintains
	// a floor timestamp, which is the minimum timestamp that can be returned by
	// the lookup operations. When the earliest page is discarded, its current
	// maximum timestamp becomes the new floor timestamp for the overall
	// intervalSkl.
	floorTS hlc.Timestamp

	metrics sklMetrics
}

// newIntervalSkl creates a new interval skiplist with the given minimum
// retention duration and the maximum size.
func newIntervalSkl(clock *hlc.Clock, minRet time.Duration, metrics sklMetrics) *intervalSkl {
	s := intervalSkl{
		clock:    clock,
		minRet:   minRet,
		pageSize: initialSklPageSize / 2, // doubled in pushNewPage
		minPages: defaultMinSklPages,
		metrics:  metrics,
	}
	s.pushNewPage(0 /* maxTime */, nil /* arena */)
	s.metrics.Pages.Update(1)
	return &s
}

// Add marks the a single key as having been read at the given timestamp. Once
// Add completes, future lookups of this key are guaranteed to return an equal
// or greater timestamp.
func (s *intervalSkl) Add(key []byte, val cacheValue) {
	s.AddRange(nil, key, 0, val)
}

// AddRange marks the given range of keys [from, to] as having been read at the
// given timestamp. The starting and ending points of the range are inclusive by
// default, but can be excluded by passing the applicable range options. nil can
// be passed as the "from" key, in which case only the end key will be added.
// nil can also be passed as the "to" key, in which case an open range will be
// added spanning [from, infinity). However, it is illegal to pass nil for both
// "from" and "to". It is also illegal for "from" > "to", which would be an
// inverted range.
//
// intervalSkl defines the domain of possible keys to span ["", nil). A range
// with a starting key of []byte("") is treated as a closed range beginning at
// the minimum key. A range with an ending key of []byte(nil) is treated as an
// open range extending to infinity (as such, excludeTo has not effect on it). A
// range starting at []byte("") and ending at []byte(nil) will span all keys.
//
// If some or all of the range was previously read at a higher timestamp, then
// the range is split into sub-ranges that are each marked with the maximum read
// timestamp for that sub-range. Once AddRange completes, future lookups at any
// point in the range are guaranteed to return an equal or greater timestamp.
func (s *intervalSkl) AddRange(from, to []byte, opt rangeOptions, val cacheValue) {
	if from == nil && to == nil {
		panic("from and to keys cannot be nil")
	}
	if encodedRangeSize(from, to, opt) > int(s.maximumPageSize())-initialSklAllocSize {
		// Without this check, we could fall into an infinite page rotation loop
		// if a range would take up more space than available in an empty page.
		panic("key range too large to fit in any page")
	}

	if to != nil {
		cmp := 0
		if from != nil {
			cmp = bytes.Compare(from, to)
		}

		switch {
		case cmp > 0:
			// Starting key is after ending key. This shouldn't happen. Determine
			// the index where the keys diverged and panic.
			d := 0
			for d < len(from) && d < len(to) {
				if from[d] != to[d] {
					break
				}
				d++
			}
			msg := fmt.Sprintf("inverted range (issue #32149): key lens = [%d,%d), diff @ index %d",
				len(from), len(to), d)
			log.Errorf(context.Background(), "%s, [%s,%s)", msg, from, to)
			panic(redact.Safe(msg))
		case cmp == 0:
			// Starting key is same as ending key, so just add single node.
			if opt == (excludeFrom | excludeTo) {
				// Both from and to keys are excluded, so range is zero length.
				return
			}

			// Just add the ending key.
			from = nil
			opt = 0
		}
	}

	for {
		// Try to add the range to the later page.
		filledPage := s.addRange(from, to, opt, val)
		if filledPage == nil {
			break
		}

		// The page was filled up, so rotate the pages and then try again.
		s.rotatePages(filledPage)
	}
}

// addRange marks the given range of keys [from, to] as having been read at the
// given timestamp. The key range and the rangeOptions observe the same behavior
// as is specified for AddRange above. Notably, addRange treats nil "from" and
// "to" arguments in accordance with AddRange's contract. It returns nil if the
// operation was successful, or a pointer to an sklPage if the operation failed
// because that page was full.
func (s *intervalSkl) addRange(from, to []byte, opt rangeOptions, val cacheValue) *sklPage {
	// Acquire the rotation mutex read lock so that the page will not be rotated
	// while add or lookup operations are in progress.
	s.rotMutex.RLock()
	defer s.rotMutex.RUnlock()

	// If floor ts is greater than the requested timestamp, then no need to
	// perform a search or add any records. We don't return early when the
	// timestamps are equal, because their flags may differ.
	if val.ts.Less(s.floorTS) {
		return nil
	}

	fp := s.frontPage()

	var it arenaskl.Iterator
	it.Init(fp.list)

	// Start by ensuring that the ending node has been created (unless "to" is
	// nil, in which case the range extends indefinitely). Do this before creating
	// the start node, so that the range won't extend past the end point during
	// the period between creating the two endpoints. Since we need the ending node
	// to be initialized before creating the starting node, we pass mustInit = true.
	var err error
	if to != nil {
		if (opt & excludeTo) == 0 {
			err = fp.addNode(&it, to, val, hasKey, true /* mustInit */)
		} else {
			err = fp.addNode(&it, to, val, 0, true /* mustInit */)
		}

		if errors.Is(err, arenaskl.ErrArenaFull) {
			return fp
		}
	}

	// If from is nil, then the "range" is just a single key. We already
	// asserted above that if from == nil then to != nil.
	if from == nil {
		return nil
	}

	// Ensure that the starting node has been created.
	if (opt & excludeFrom) == 0 {
		err = fp.addNode(&it, from, val, hasKey|hasGap, false /* mustInit */)
	} else {
		err = fp.addNode(&it, from, val, hasGap, false /* mustInit */)
	}

	if errors.Is(err, arenaskl.ErrArenaFull) {
		return fp
	}

	// Seek to the node immediately after the "from" node.
	//
	// If there are no nodes after the "from" node (only possible if to == nil),
	// then ensureFloorValue below will be a no-op because no other nodes need
	// to be adjusted.
	if !it.Valid() || !bytes.Equal(it.Key(), from) {
		// We will only reach this state if we didn't need to add a node at
		// "from" due to the previous gap value being larger than val. The fast
		// path for this case is in sklPage.addNode. For all other times, adding
		// the new node will have positioned the iterator at "from".
		//
		// If Seek returns false then we're already at the following node, so
		// there's no need to call Next.
		if it.Seek(from) {
			it.Next()
		}
	} else {
		it.Next()
	}

	// Now iterate forwards and ensure that all nodes between the start and
	// end (exclusive) have timestamps that are >= the range timestamp. end
	// is exclusive because we already added a node at that key.
	if !fp.ensureFloorValue(&it, to, val) {
		// Page is filled up, so rotate pages and try again.
		return fp
	}

	return nil
}

// frontPage returns the front page of the intervalSkl.
func (s *intervalSkl) frontPage() *sklPage {
	return s.pages.Front().Value.(*sklPage)
}

// pushNewPage prepends a new empty page to the front of the pages list. It
// accepts an optional arena argument to facilitate re-use.
func (s *intervalSkl) pushNewPage(maxTime ratchetingTime, arena *arenaskl.Arena) {
	size := s.nextPageSize()
	if arena != nil && arena.Cap() == size {
		// Re-use the provided arena, if possible.
		arena.Reset()
	} else {
		// Otherwise, construct new memory arena.
		arena = arenaskl.NewArena(size)
	}
	p := newSklPage(arena)
	p.maxTime = maxTime
	s.pages.PushFront(p)
}

// nextPageSize returns the size that the next allocated page should use.
func (s *intervalSkl) nextPageSize() uint32 {
	if s.pageSizeFixed || s.pageSize == maximumSklPageSize {
		return s.pageSize
	}
	s.pageSize *= 2
	if s.pageSize > maximumSklPageSize {
		s.pageSize = maximumSklPageSize
	}
	return s.pageSize
}

// maximumPageSize returns the maximum page size that this instance of the
// intervalSkl will be able to accommodate. The method takes into consideration
// whether the page size is fixed or dynamic.
func (s *intervalSkl) maximumPageSize() uint32 {
	if s.pageSizeFixed {
		return s.pageSize
	}
	return maximumSklPageSize
}

// rotatePages makes the later page the earlier page, and then discards the
// earlier page. The max timestamp of the earlier page becomes the new floor
// timestamp, in order to guarantee that timestamp lookups never return decreasing
// values.
func (s *intervalSkl) rotatePages(filledPage *sklPage) {
	// Acquire the rotation mutex write lock to lock the entire intervalSkl.
	s.rotMutex.Lock()
	defer s.rotMutex.Unlock()

	fp := s.frontPage()
	if filledPage != fp {
		// Another thread already rotated the pages, so don't do anything more.
		return
	}

	// Determine the minimum timestamp a page must contain to be within the
	// minimum retention window. If clock is nil, we have no minimum retention
	// window.
	minTSToRetain := hlc.MaxTimestamp
	if s.clock != nil {
		minTSToRetain = s.clock.Now().Add(-s.minRet.Nanoseconds(), 0)
	}

	// Iterate over the pages in reverse, evicting pages that are no longer
	// needed and ratcheting up the floor timestamp in the process.
	//
	// If possible, keep a reference to an evicted page's arena so that we can
	// re-use it. This is safe because we're holding the rotation mutex write
	// lock, so there cannot be concurrent readers and no reader will ever
	// access evicted pages once we unlock.
	back := s.pages.Back()
	var oldArena *arenaskl.Arena
	for s.pages.Len() >= s.minPages {
		bp := back.Value.(*sklPage)
		bpMaxTS := bp.getMaxTimestamp()
		if minTSToRetain.LessEq(bpMaxTS) {
			// The back page's maximum timestamp is within the time
			// window we've promised to retain, so we can't evict it.
			break
		}

		// Max timestamp of the back page becomes the new floor timestamp.
		s.floorTS.Forward(bpMaxTS)

		// Evict the page.
		oldArena = bp.list.Arena()
		evict := back
		back = back.Prev()
		s.pages.Remove(evict)
	}

	// Push a new empty page on the front of the pages list. We give this page
	// the maxTime of the old front page. This assures that the maxTime for a
	// page is always equal to or greater than that for all earlier pages. In
	// other words, it assures that the maxTime for a page is not only the
	// maximum timestamp for all values it contains, but also for all values any
	// earlier pages contain.
	s.pushNewPage(fp.maxTime, oldArena)

	// Update metrics.
	s.metrics.Pages.Update(int64(s.pages.Len()))
	s.metrics.PageRotations.Inc(1)
}

// LookupTimestamp returns the latest timestamp value at which the given key was
// read. If this operation is repeated with the same key, it will always result
// in an equal or greater timestamp.
func (s *intervalSkl) LookupTimestamp(key []byte) cacheValue {
	return s.LookupTimestampRange(nil, key, 0)
}

// LookupTimestampRange returns the latest timestamp value of any key within the
// specified range. If this operation is repeated with the same range, it will
// always result in an equal or greater timestamp.
func (s *intervalSkl) LookupTimestampRange(from, to []byte, opt rangeOptions) cacheValue {
	if from == nil && to == nil {
		panic("from and to keys cannot be nil")
	}

	// Acquire the rotation mutex read lock so that the page will not be rotated
	// while add or lookup operations are in progress.
	s.rotMutex.RLock()
	defer s.rotMutex.RUnlock()

	// Iterate over the pages, performing the lookup on each and remembering the
	// maximum value we've seen so far.
	var val cacheValue
	for e := s.pages.Front(); e != nil; e = e.Next() {
		p := e.Value.(*sklPage)

		// If the maximum value's timestamp is greater than the max timestamp in
		// the current page, then there's no need to do the lookup in this page.
		// There's also no reason to do the lookup in any earlier pages either,
		// because rotatePages assures that a page will never have a max
		// timestamp smaller than that of any page earlier than it.
		//
		// NB: if the max timestamp of the current page is equal to the maximum
		// value's timestamp, then we still need to perform the lookup. This is
		// because the current page's max timestamp _may_ (if the hlc.Timestamp
		// ceil operation in sklPage.ratchetMaxTimestamp was a no-op) correspond
		// to a real range's timestamp, and this range _may_ overlap with our
		// lookup range. If that is the case and that other range has a
		// different txnID than our current cacheValue result (val), then we
		// need to remove the txnID from our result, per the ratcheting policy
		// for cacheValues. This is tested in TestIntervalSklMaxPageTS.
		maxTS := p.getMaxTimestamp()
		if maxTS.Less(val.ts) {
			break
		}

		val2 := p.lookupTimestampRange(from, to, opt)
		val, _ = ratchetValue(val, val2)
	}

	// Return the higher value from the page lookups and the floor
	// timestamp.
	floorVal := cacheValue{ts: s.floorTS, txnID: noTxnID}
	val, _ = ratchetValue(val, floorVal)

	return val
}

// FloorTS returns the receiver's floor timestamp.
func (s *intervalSkl) FloorTS() hlc.Timestamp {
	s.rotMutex.RLock()
	defer s.rotMutex.RUnlock()
	return s.floorTS
}

// sklPage maintains a skiplist based on a fixed-size arena. When the arena has
// filled up, it returns arenaskl.ErrArenaFull. At that point, a new fixed page
// must be allocated and used instead.
type sklPage struct {
	list    *arenaskl.Skiplist
	maxTime ratchetingTime // accessed atomically
	isFull  int32          // accessed atomically
}

func newSklPage(arena *arenaskl.Arena) *sklPage {
	return &sklPage{list: arenaskl.NewSkiplist(arena)}
}

func (p *sklPage) lookupTimestampRange(from, to []byte, opt rangeOptions) cacheValue {
	if to != nil {
		cmp := 0
		if from != nil {
			cmp = bytes.Compare(from, to)
		}

		if cmp > 0 {
			// Starting key is after ending key, so range is zero length.
			return cacheValue{}
		}
		if cmp == 0 {
			// Starting key is same as ending key.
			if opt == (excludeFrom | excludeTo) {
				// Both from and to keys are excluded, so range is zero length.
				return cacheValue{}
			}

			// Scan over a single key.
			from = to
			opt = 0
		}
	}

	var it arenaskl.Iterator
	it.Init(p.list)
	it.SeekForPrev(from)

	return p.maxInRange(&it, from, to, opt)
}

// addNode adds a new node at key with the provided value if one does not exist.
// If one does exist, it ratchets the existing node's value instead.
//
// If the mustInit flag is set, the function will ensure that the node is
// initialized by the time the method returns, even if a different goroutine
// created the node. If the flag is not set and a different goroutine created
// the node, the method won't try to help.
func (p *sklPage) addNode(
	it *arenaskl.Iterator, key []byte, val cacheValue, opt nodeOptions, mustInit bool,
) error {
	// Array with constant size will remain on the stack.
	var arr [encodedValSize * 2]byte
	var keyVal, gapVal cacheValue

	if (opt & hasKey) != 0 {
		keyVal = val
	}

	if (opt & hasGap) != 0 {
		gapVal = val
	}

	if !it.SeekForPrev(key) {
		// The key was not found. Scan for the previous gap value.
		prevGapVal := p.incomingGapVal(it, key)

		var err error
		if it.Valid() && bytes.Equal(it.Key(), key) {
			// Another thread raced and added a node at key while we were
			// scanning backwards. Ratchet the new node.
			err = arenaskl.ErrRecordExists
		} else {
			// There is still no node at key. If the previous node has a gap
			// value that would not be updated with the new value, then there is
			// no need to add another node, since its timestamp would be the
			// same as the gap timestamp and its txnID would be the same as the
			// gap txnID.
			if _, update := ratchetValue(prevGapVal, val); !update {
				return nil
			}

			// Ratchet max timestamp before adding the node.
			p.ratchetMaxTimestamp(val.ts)

			// Ensure that a new node is created. It needs to stay in the
			// initializing state until the gap value of its preceding node
			// has been found and used to ratchet this node's value. During
			// the search for the gap value, this node acts as a sentinel
			// for other ongoing operations - when they see this node they're
			// forced to stop and ratchet its value before they can continue.
			b, meta := encodeValueSet(arr[:0], keyVal, gapVal)
			err = it.Add(key, b, meta)
		}

		switch {
		case errors.Is(err, arenaskl.ErrArenaFull):
			atomic.StoreInt32(&p.isFull, 1)
			return err
		case errors.Is(err, arenaskl.ErrRecordExists):
			// Another thread raced and added the node, so just ratchet its
			// values instead (down below).
		case err == nil:
			// Add was successful, so finish initialization by scanning for gap
			// value and using it to ratchet the new nodes' values.
			return p.ensureInitialized(it, key)
		default:
			panic(fmt.Sprintf("unexpected error: %v", err))
		}
	}

	// If mustInit is set to true then we're promising that the node will be
	// initialized by the time this method returns. Ensure this by helping out
	// the goroutine that created the node.
	if (it.Meta()&initialized) == 0 && mustInit {
		if err := p.ensureInitialized(it, key); err != nil {
			return err
		}
	}

	// Ratchet up the timestamps on the existing node, but don't set the
	// initialized bit. If mustInit is set then we already made sure the node
	// was initialized. If mustInit is not set then we don't require it to be
	// initialized.
	if opt == 0 {
		// Don't need to set either key or gap value, so done.
		return nil
	}
	return p.ratchetValueSet(it, always, keyVal, gapVal, false /* setInit */)
}

// ensureInitialized ensures that the node at the specified key is initialized.
// It does so by first scanning backwards to the first initialized node and
// using its gap value as the initial "previous gap value". It then scans
// forward until it reaches the desired key, ratcheting any uninitialized nodes
// it encounters (but not initializing them), and updating the candidate
// "previous gap value" as it goes. Finally, it initializes the node with the
// "previous gap value".
//
// Iterating backwards and then forwards solves potential race conditions with
// other threads. During backwards iteration, other nodes can be inserting new
// nodes between the previous node and the lookup node, which could change the
// choice for the "previous gap value". The solution is two-fold:
//
// 1. Add new nodes in two phases - initializing and then initialized. Nodes in
//    the initializing state act as a synchronization point between goroutines
//    that are adding a particular node and goroutines that are scanning for gap
//    values. Scanning goroutines encounter the initializing nodes and are
//    forced to ratchet them before continuing. If they fail to ratchet them
//    because an arena is full, the nodes must never be initialized so they are
//    set to cantInit. This is critical for correctness, because if one of these
//    initializing nodes was not ratcheted when encountered during a forward
//    scan and later initialized, we could see a ratchet inversion. For example,
//    the inversion would occur if:
//    - 1: a goroutine is scanning forwards after finding a previous gap value
//         from node A in which it plans to initialize node C.
//    - 2: node B is created and initialized between node A and node C with a
//         larger value than either.
//    - 1: the iterator scanning forwards to node C is already past node B when
//         it is created.
//    - 3: a lookup for the timestamp of node C comes in. Since it's not
//         initialized, it uses node B's gap value.
//    - 1: the iterator reaches node C and initializes it with node A's gap
//         value, which is smaller than node B's.
//    - 4: another lookup for the timestamp of node C comes it. It returns the
//         nodes newly initialized value, which is smaller than the one it
//         reported before.
//    Ratcheting initializing nodes when encountered with the current gap value
//    avoids this race.
//
//    However, only a goroutine that saw a node in an uninitialized state before
//    scanning backwards can switch it from initializing to initialized. This
//    enforces a "happens-before" relationship between the creation of a node
//    and the discovery of the gap value that is used when initializing it. If
//    any goroutine was able to initialize a node, then this relationship would
//    not exist and we could experience races where a newly inserted node A's
//    call to ensureFloorValue could come before the insertion of a node B, but
//    node B could be initialized with a gap value discovered before the
//    insertion of node A. For more on this, see the discussion in #19672.
//
// 2. After the gap value of the first initialized node with a key less than or
//    equal to the desired key has been found, the scanning goroutine will scan
//    forwards until it reaches the original key. It will ratchet any
//    uninitialized nodes along the way and inherit the gap value from them as
//    it goes. By the time it reaches the original key, it has a valid gap
//    value, which we have called the "previous gap value". At this point, if
//    the node at key is uninitialized, the node can be initialized with the
//    "previous gap value".
//
// It is an error to call ensureInitialized on a key without a node. When
// finished, the iterator will be positioned the same as if it.Seek(key) had
// been called.
func (p *sklPage) ensureInitialized(it *arenaskl.Iterator, key []byte) error {
	// Determine the incoming gap value.
	prevGapVal := p.incomingGapVal(it, key)

	// Make sure we're on the right key again.
	if util.RaceEnabled && !bytes.Equal(it.Key(), key) {
		panic("no node found")
	}

	// If the node isn't initialized, initialize it.
	return p.ratchetValueSet(it, onlyIfUninitialized, prevGapVal, prevGapVal, true /* setInit */)
}

// ensureFloorValue scans from the current position of the iterator to the
// provided key, ratcheting all initialized or uninitialized nodes as it goes
// with the provided value. It returns a boolean indicating whether it was
// successful (true) or whether it saw an ErrArenaFull while ratcheting (false).
func (p *sklPage) ensureFloorValue(it *arenaskl.Iterator, to []byte, val cacheValue) bool {
	for it.Valid() {
		util.RacePreempt()

		// If "to" is not nil (open range) then it is treated as an exclusive
		// bound.
		if to != nil && bytes.Compare(it.Key(), to) >= 0 {
			break
		}

		if atomic.LoadInt32(&p.isFull) == 1 {
			// Page is full, so stop iterating. The caller will then be able to
			// release the read lock and rotate the pages. Not doing this could
			// result in forcing all other operations to wait for this thread to
			// completely finish iteration. That could take a long time if this
			// range is very large.
			return false
		}

		// Don't clear the initialization bit, since we don't have the gap
		// timestamp from the previous node, and don't need an initialized node
		// for this operation anyway.
		err := p.ratchetValueSet(it, always, val, val, false /* setInit */)
		switch {
		case err == nil:
			// Continue scanning.
		case errors.Is(err, arenaskl.ErrArenaFull):
			// Page is too full to ratchet value, so stop iterating.
			return false
		default:
			panic(fmt.Sprintf("unexpected error: %v", err))
		}

		it.Next()
	}

	return true
}

func (p *sklPage) ratchetMaxTimestamp(ts hlc.Timestamp) {
	new := makeRatchetingTime(ts)
	for {
		old := ratchetingTime(atomic.LoadInt64((*int64)(&p.maxTime)))
		if new <= old {
			break
		}

		if atomic.CompareAndSwapInt64((*int64)(&p.maxTime), int64(old), int64(new)) {
			break
		}
	}
}

func (p *sklPage) getMaxTimestamp() hlc.Timestamp {
	return ratchetingTime(atomic.LoadInt64((*int64)(&p.maxTime))).get()
}

// ratchetingTime is a compressed representation of an hlc.Timestamp, reduced
// down to 64 bits to support atomic access.
//
// ratchetingTime implements compression such that any loss of information when
// passing through the type results in the resulting Timestamp being ratcheted
// to a larger value. This provides the guarantee that the following relation
// holds, regardless of the value of x:
//
//   x.LessEq(makeRatchetingTime(x).get())
//
// It also provides the guarantee that if the synthetic flag is set on the
// initial timestamp, then this flag is set on the resulting Timestamp. So the
// following relation is guaranteed to hold, regardless of the value of x:
//
//   x.IsFlagSet(SYNTHETIC) == makeRatchetingTime(x).get().IsFlagSet(SYNTHETIC)
//
// Compressed ratchetingTime values compare such that taking the maximum of any
// two ratchetingTime values and converting that back to a Timestamp is always
// equal to or larger than the equivalent call through the Timestamp.Forward
// method. So the following relation is guaranteed to hold, regardless of the
// value of x or y:
//
//   z := max(makeRatchetingTime(x), makeRatchetingTime(y)).get()
//   x.Forward(y).LessEq(z)
//
// Bit layout (LSB to MSB):
//  bits 0:      inverted synthetic flag
//  bits 1 - 63: upper 63 bits of wall time
type ratchetingTime int64

func makeRatchetingTime(ts hlc.Timestamp) ratchetingTime {
	// Cheat and just use the max wall time portion of the timestamp, since it's
	// fine for the max timestamp to be a bit too large. This is the case
	// because it's always safe to increase the timestamp in a range. It's also
	// always safe to remove the transaction ID from a range. Either of these
	// changes may force a transaction to lose "ownership" over a range of keys,
	// but they'll never allow a transaction to gain "ownership" over a range of
	// keys that it wouldn't otherwise have. In other words, it's ok for the
	// intervalSkl to produce false negatives but never ok for it to produce
	// false positives.
	//
	// We could use an atomic.Value to store a "MaxValue" cacheValue for a given
	// page, but this would be more expensive and it's not clear that it would
	// be worth it.
	rt := ratchetingTime(ts.WallTime)
	if ts.Logical > 0 {
		rt++
	}

	// Similarly, cheat and use the last bit in the wall time to indicate
	// whether the timestamp is synthetic or not. Do so by first rounding up the
	// last bit of the wall time so that it is empty. This is safe for the same
	// reason that rounding up the logical portion of the timestamp in the wall
	// time is safe (see above).
	//
	// We use the last bit to indicate that the flag is NOT set. This ensures
	// that if two timestamps have the same ordering but different values for
	// the synthetic flag, the timestamp without the synthetic flag has a larger
	// ratchetingTime value. This follows how Timestamp.Forward treats the flag.
	if rt&1 == 1 {
		rt++
	}
	if !ts.Synthetic {
		rt |= 1
	}

	return rt
}

func (rt ratchetingTime) get() hlc.Timestamp {
	var ts hlc.Timestamp
	ts.WallTime = int64(rt &^ 1)
	if rt&1 == 0 {
		ts.Synthetic = true
	}
	return ts
}

// ratchetPolicy defines the behavior a ratcheting attempt should take when
// trying to ratchet a node. Certain operations require nodes to be ratcheted
// regardless of whether they're already initialized or not. Other operations
// only want nodes that are uninitialized to be ratcheted.
type ratchetPolicy bool

const (
	// always is a policy to ratchet a node regardless of whether it is already
	// initialized or not.
	always ratchetPolicy = false
	// onlyIfUninitialized is a policy to only ratchet a node if it has not been
	// initialized yet.
	onlyIfUninitialized ratchetPolicy = true
)

// ratchetValueSet will update the current node's key and gap values to the
// maximum of their current values or the given values. If setInit is true, then
// the initialized bit will be set, indicating that the node is now fully
// initialized and its values can now be relied upon.
//
// The method will return ErrArenaFull if the arena was too full to ratchet the
// node's value set. In that case, the node will be marked with the "cantInit"
// flag because its values should never be trusted in isolation.
func (p *sklPage) ratchetValueSet(
	it *arenaskl.Iterator, policy ratchetPolicy, keyVal, gapVal cacheValue, setInit bool,
) error {
	// Array with constant size will remain on the stack.
	var arr [encodedValSize * 2]byte

	for {
		util.RacePreempt()

		meta := it.Meta()
		inited := (meta & initialized) != 0
		if inited && policy == onlyIfUninitialized {
			// If the node is already initialized and the policy is
			// onlyIfUninitialized, return. If this isn't the first ratcheting
			// attempt then we must have raced with node initialization before.
			return nil
		}
		if (meta & cantInit) != 0 {
			// If the meta has the cantInit flag set to true, we fail with an
			// ErrArenaFull error to force the current goroutine to retry on a
			// new page.
			return arenaskl.ErrArenaFull
		}

		newMeta := meta
		updateInit := setInit && !inited
		if updateInit {
			newMeta |= initialized
		}

		var keyValUpdate, gapValUpdate bool
		oldKeyVal, oldGapVal := decodeValueSet(it.Value(), meta)
		keyVal, keyValUpdate = ratchetValue(oldKeyVal, keyVal)
		gapVal, gapValUpdate = ratchetValue(oldGapVal, gapVal)
		updateVals := keyValUpdate || gapValUpdate

		if updateVals {
			// If we're updating the values (and maybe the init flag) then we
			// need to call it.Set. This can return an ErrArenaFull, which we
			// must handle with care.

			// Ratchet the max timestamp.
			maxTs := keyVal.ts
			maxTs.Forward(gapVal.ts)
			p.ratchetMaxTimestamp(maxTs)

			// Remove the hasKey and hasGap flags from the meta. These will be
			// replaced below.
			newMeta &^= (hasKey | hasGap)

			// Update the values, possibly preserving the init bit.
			b, valMeta := encodeValueSet(arr[:0], keyVal, gapVal)
			newMeta |= valMeta

			err := it.Set(b, newMeta)
			switch {
			case err == nil:
				// Success.
				return nil
			case errors.Is(err, arenaskl.ErrRecordUpdated):
				// Record was updated by another thread, so restart ratchet attempt.
				continue
			case errors.Is(err, arenaskl.ErrArenaFull):
				// The arena was full which means that we were unable to ratchet
				// the value of this node. Mark the page as full and make sure
				// that the node is moved to the "cantInit" state if it hasn't
				// been initialized yet. This is critical because if the node
				// was initialized after this, its value set would be relied
				// upon to stand on its own even though it would be missing the
				// ratcheting we tried to perform here.
				atomic.StoreInt32(&p.isFull, 1)

				if !inited && (meta&cantInit) == 0 {
					err := it.SetMeta(meta | cantInit)
					switch {
					case errors.Is(err, arenaskl.ErrRecordUpdated):
						// Record was updated by another thread, so restart
						// ratchet attempt.
						continue
					case errors.Is(err, arenaskl.ErrArenaFull):
						panic(fmt.Sprintf("SetMeta with larger meta should not return %v", err))
					}
				}
				return arenaskl.ErrArenaFull
			default:
				panic(fmt.Sprintf("unexpected error: %v", err))
			}
		} else if updateInit {
			// If we're only updating the init flag and not the values, we can
			// use it.SetMeta instead of it.Set, which avoids allocating new
			// chunks in the arena.
			err := it.SetMeta(newMeta)
			switch {
			case err == nil:
				// Success.
				return nil
			case errors.Is(err, arenaskl.ErrRecordUpdated):
				// Record was updated by another thread, so restart ratchet attempt.
				continue
			case errors.Is(err, arenaskl.ErrArenaFull):
				panic(fmt.Sprintf("SetMeta with larger meta should not return %v", err))
			default:
				panic(fmt.Sprintf("unexpected error: %v", err))
			}
		} else {
			return nil
		}
	}
}

// maxInRange scans the range of keys between from and to and returns the
// maximum (initialized or uninitialized) value found. When finished, the
// iterator will be positioned the same as if it.Seek(to) had been called.
func (p *sklPage) maxInRange(it *arenaskl.Iterator, from, to []byte, opt rangeOptions) cacheValue {
	// Determine the previous gap value. This will move the iterator to the
	// first node >= from.
	prevGapVal := p.incomingGapVal(it, from)

	if !it.Valid() {
		// No more nodes.
		return prevGapVal
	} else if bytes.Equal(it.Key(), from) {
		// Found a node at from.
		if (it.Meta() & initialized) != 0 {
			// The node was initialized. Ignore the previous gap value.
			prevGapVal = cacheValue{}
		}
	} else {
		// No node at from. Remove excludeFrom option.
		opt &^= excludeFrom
	}

	// Scan the rest of the way. Notice that we provide the previous gap value.
	// This is important for two reasons:
	// 1. it will be counted towards the maxVal result.
	// 2. it will be used to ratchet uninitialized nodes that the scan sees
	//    before any initialized nodes.
	_, maxVal := p.scanTo(it, to, opt, prevGapVal)
	return maxVal
}

// incomingGapVal determines the gap value active at the specified key by first
// scanning backwards to the first initialized node and then scanning forwards
// to the specified key. If there is already a node at key then the previous gap
// value will be returned. When finished, the iterator will be positioned the
// same as if it.Seek(key) had been called.
//
// During forward iteration, if another goroutine inserts a new gap node in the
// interval between the previous node and the original key, then either:
//
// 1. The forward iteration finds it and looks up its gap value. That node's gap
//    value now becomes the new "previous gap value", and iteration continues.
//
// 2. The new node is created after the iterator has move past its position. As
//    part of node creation, the creator had to scan backwards to find the gap
//    value of the previous node. It is guaranteed to find a gap value that is
//    >= the gap value found by the original goroutine.
//
// This means that no matter what gets inserted, or when it gets inserted, the
// scanning goroutine is guaranteed to end up with a value that will never
// decrease on future lookups, which is the critical invariant.
func (p *sklPage) incomingGapVal(it *arenaskl.Iterator, key []byte) cacheValue {
	// Iterate backwards to the nearest initialized node.
	prevInitNode(it)

	// Iterate forwards to key, remembering the last gap value.
	prevGapVal, _ := p.scanTo(it, key, 0, cacheValue{})
	return prevGapVal
}

// scanTo scans from the current iterator position until the key "to". While
// scanning, any uninitialized values are ratcheted with the current gap value,
// which is essential to avoiding ratchet inversions (see the comment on
// ensureInitialized).
//
// The function then returns the maximum value seen along with the gap value at
// the end of the scan. If the iterator is positioned at a key > "to", the
// function will return zero values. The function takes an optional initial gap
// value argument, which is used to initialize the running maximum and gap
// values. When finished, the iterator will be positioned the same as if
// it.Seek(to) had been called.
func (p *sklPage) scanTo(
	it *arenaskl.Iterator, to []byte, opt rangeOptions, initGapVal cacheValue,
) (prevGapVal, maxVal cacheValue) {
	prevGapVal, maxVal = initGapVal, initGapVal
	first := true
	for {
		util.RacePreempt()

		if !it.Valid() {
			// No more nodes, which can happen for open ranges.
			return
		}

		toCmp := bytes.Compare(it.Key(), to)
		if to == nil {
			// to == nil means open range, so toCmp will always be -1.
			toCmp = -1
		}
		if toCmp > 0 || (toCmp == 0 && (opt&excludeTo) != 0) {
			// Past the end key or we don't want to consider the end key.
			return
		}

		// Ratchet uninitialized nodes. We pass onlyIfUninitialized, so if
		// the node is already initialized then this is a no-op.
		ratchetErr := p.ratchetValueSet(it, onlyIfUninitialized,
			prevGapVal, prevGapVal, false /* setInit */)

		// Decode the current node's value set.
		keyVal, gapVal := decodeValueSet(it.Value(), it.Meta())
		if errors.Is(ratchetErr, arenaskl.ErrArenaFull) {
			// If we failed to ratchet an uninitialized node above, the desired
			// ratcheting won't be reflected in the decoded values. Perform the
			// ratcheting manually.
			keyVal, _ = ratchetValue(keyVal, prevGapVal)
			gapVal, _ = ratchetValue(gapVal, prevGapVal)
		}

		if !(first && (opt&excludeFrom) != 0) {
			// As long as this isn't the first key and opt says to exclude the
			// first key, we ratchet the maxVal.
			maxVal, _ = ratchetValue(maxVal, keyVal)
		}

		if toCmp == 0 {
			// We're on the scan's end key, so return the max value seen.
			return
		}

		// Ratchet the maxVal by the current gapVal.
		maxVal, _ = ratchetValue(maxVal, gapVal)

		// Haven't yet reached the scan's end key, so keep iterating.
		prevGapVal = gapVal
		first = false
		it.Next()
	}
}

// prevInitNode moves the iterator backwards to the nearest initialized node. If
// the iterator is already positioned on an initialized node then this function
// is a no-op.
func prevInitNode(it *arenaskl.Iterator) {
	for {
		util.RacePreempt()

		if !it.Valid() {
			// No more previous nodes, so use the zero value.
			it.SeekToFirst()
			break
		}

		if (it.Meta() & initialized) != 0 {
			// Found an initialized node.
			break
		}

		// Haven't yet reached an initialized node, so keep iterating.
		it.Prev()
	}
}

func decodeValueSet(b []byte, meta uint16) (keyVal, gapVal cacheValue) {
	if (meta & hasKey) != 0 {
		b, keyVal = decodeValue(b)
	}

	if (meta & hasGap) != 0 {
		_, gapVal = decodeValue(b)
	}

	return
}

func encodeValueSet(b []byte, keyVal, gapVal cacheValue) (ret []byte, meta uint16) {
	if !keyVal.ts.IsEmpty() {
		b = encodeValue(b, keyVal)
		meta |= hasKey
	}

	if !gapVal.ts.IsEmpty() {
		b = encodeValue(b, gapVal)
		meta |= hasGap
	}

	ret = b
	return
}

func decodeValue(b []byte) (ret []byte, val cacheValue) {
	// Copy and interpret the byte slice as a cacheValue.
	valPtr := (*[encodedValSize]byte)(unsafe.Pointer(&val))
	copy(valPtr[:], b)
	ret = b[encodedValSize:]
	return ret, val
}

func encodeValue(b []byte, val cacheValue) []byte {
	// Interpret the cacheValue as a byte slice and copy.
	prev := len(b)
	b = b[:prev+encodedValSize]
	valPtr := (*[encodedValSize]byte)(unsafe.Pointer(&val))
	copy(b[prev:], valPtr[:])
	return b
}

func encodedRangeSize(from, to []byte, opt rangeOptions) int {
	vals := 1
	if (opt & excludeTo) == 0 {
		vals++
	}
	if (opt & excludeFrom) == 0 {
		vals++
	}
	// This will be an overestimate because nodes will almost
	// always be smaller than arenaskl.MaxNodeSize.
	return len(from) + len(to) + (vals * encodedValSize) + (2 * arenaskl.MaxNodeSize)
}
