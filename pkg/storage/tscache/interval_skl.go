// Copyright (C) 2017 Andy Kimball
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tscache

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/andy-kimball/arenaskl"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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

	// hasKey indicates that the node has an associated key value. If this is
	// not set, then the key timestamp is assumed to be zero and the key is
	// assumed to not have a corresponding txnID.
	hasKey

	// hasGap indicates that the node has an associated gap value. If this is
	// not set, then the gap timestamp is assumed to be zero and the gep is
	// assumed to not have a corresponding txnID.
	hasGap

	// useMaxVal indicates that the intervalSkl's max value should be used in
	// place of the node's gap and key timestamps. This is used when the
	// structure has filled up and the value can no longer be set (but meta
	// can).
	useMaxVal
)

const (
	encodedTsSize      = int(unsafe.Sizeof(hlc.Timestamp{}))
	encodedTxnIDSize   = int(unsafe.Sizeof(uuid.UUID{}))
	encodedValSize     = encodedTsSize + encodedTxnIDSize
	defaultMinSklPages = 2
)

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

	// The size of each page in the data structure, in bytes. When a page fills,
	// the pages will be rotated and older entries will be discarded. The entire
	// data structure will usually have a size limit of pageSize*minPages.
	// However, this limit can be violated if the intervalSkl needs to grow
	// larger to enforce a minimum retention policy.
	pageSize uint32

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
}

// newIntervalSkl creates a new interval skiplist with the given minimum
// retention duration and the maximum size.
func newIntervalSkl(clock *hlc.Clock, minRet time.Duration, pageSize uint32) *intervalSkl {
	s := intervalSkl{
		clock:    clock,
		minRet:   minRet,
		pageSize: pageSize,
		minPages: defaultMinSklPages,
	}
	s.pushNewPage(0)
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
// If some or all of the range was previously read at a higher timestamp, then
// the range is split into sub-ranges that are each marked with the maximum read
// timestamp for that sub-range.  Once AddRange completes, future lookups at any
// point in the range are guaranteed to return an equal or greater timestamp.
func (s *intervalSkl) AddRange(from, to []byte, opt rangeOptions, val cacheValue) {
	if from == nil && to == nil {
		panic("from and to keys cannot be nil")
	}

	if to != nil {
		cmp := 0
		if from != nil {
			cmp = bytes.Compare(from, to)
		}

		switch {
		case cmp > 0:
			// Starting key is after ending key. This shouldn't happen.
			panic(interval.ErrInvertedRange)
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

	// If floor ts is >= requested timestamp, then no need to perform a search
	// or add any records.
	if !s.floorTS.Less(val.ts) {
		return nil
	}

	fp := s.frontPage()

	var it arenaskl.Iterator
	it.Init(fp.list)

	// Start by ensuring that the ending node has been created (unless "to" is
	// nil, in which case the range extends indefinitely). Do this before creating
	// the start node, so that the range won't extend past the end point during
	// the period between creating the two endpoints.
	var err error
	if to != nil {
		if (opt & excludeTo) == 0 {
			err = fp.addNode(&it, to, val, hasKey)
		} else {
			err = fp.addNode(&it, to, val, 0)
		}

		if err == arenaskl.ErrArenaFull {
			return fp
		}
	}

	// If from is nil, then the "range" is just a single key. We already
	// asserted above that if from == nil then to != nil.
	if from == nil {
		return nil
	}

	// Ensure that the starting node has been created.
	if (opt&excludeFrom) == 0 || len(from) == 0 {
		err = fp.addNode(&it, from, val, hasKey|hasGap)
	} else {
		err = fp.addNode(&it, from, val, hasGap)
	}

	if err == arenaskl.ErrArenaFull {
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

// pushNewPage prepends a new empty page to the front of the pages list.
func (s *intervalSkl) pushNewPage(maxWallTime int64) {
	p := newSklPage(s.pageSize)
	p.maxWallTime = maxWallTime
	s.pages.PushFront(p)
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

	// Push a new empty page on the front of the pages list. We give this page
	// the maxWallTime of the old front page. This assures that the maxWallTime
	// for a page is always equal to or greater than that for all earlier pages.
	// In other words, it assures that the maxWallTime for a page is not only
	// the maximum timestamp for all values it contains, but also for all values
	// any earlier pages contain.
	s.pushNewPage(atomic.LoadInt64(&fp.maxWallTime))

	// Determine the minimum timestamp a page must contain to be within the
	// minimum retention window. If clock is nil, we have no minimum retention
	// window.
	minTSToRetain := hlc.MaxTimestamp
	if s.clock != nil {
		minTSToRetain = s.clock.Now()
		minTSToRetain.WallTime -= s.minRet.Nanoseconds()
	}

	// Iterate over the pages in reverse, evicting pages that are no longer
	// needed and ratcheting up the floor timestamp in the process.
	back := s.pages.Back()
	for s.pages.Len() > s.minPages {
		bp := back.Value.(*sklPage)
		bpMaxTS := hlc.Timestamp{WallTime: atomic.LoadInt64(&bp.maxWallTime)}
		if !bpMaxTS.Less(minTSToRetain) {
			// The back page's maximum timestamp is within the time
			// window we've promised to retain, so we can't evict it.
			break
		}

		// Max timestamp of the back page becomes the new floor timestamp.
		s.floorTS.Forward(bpMaxTS)

		// Evict the page.
		evict := back
		back = back.Prev()
		s.pages.Remove(evict)
	}
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
		maxTS := hlc.Timestamp{WallTime: atomic.LoadInt64(&p.maxWallTime)}
		if maxTS.Less(val.ts) {
			break
		}

		val2 := p.lookupTimestampRange(from, to, opt)
		val, _ = ratchetValue(val, val2)
	}

	// Return the higher value from the the page lookups and the floor
	// timestamp.
	floorVal := cacheValue{ts: s.floorTS, txnID: noTxnID}
	val, _ = ratchetValue(val, floorVal)

	return val
}

// sklPage maintains a skiplist based on a fixed-size arena. When the arena has
// filled up, it returns arenaskl.ErrArenaFull. At that point, a new fixed page
// must be allocated and used instead.
type sklPage struct {
	list        *arenaskl.Skiplist
	maxWallTime int64 // accessed atomically
	isFull      int32 // accessed atomically
}

func newSklPage(size uint32) *sklPage {
	return &sklPage{list: arenaskl.NewSkiplist(arenaskl.NewArena(size))}
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

	return p.scanForMaxValue(&it, from, to, opt, false /* initFrom */)
}

func (p *sklPage) addNode(
	it *arenaskl.Iterator, key []byte, val cacheValue, opt nodeOptions,
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
		util.RacePreempt()

		// The key was not found. If the previous node has a gap value that
		// would not be updated with the new value, then there is no need to add
		// another node, since its timestamp would be the same as the gap
		// timestamp and its txnID would be the same as the gap txnID.
		//
		// NB: We pass excludeTo here because another thread may race and add a
		// node at the key while we're scanning for the previous value. We don't
		// want to consider this new key here because even if it has a larger
		// key value that indicates that we can take this fast path, it may have
		// a smaller gap value that we need to ratchet below. Now, if a node
		// races and is added at key while we're scanning for the previous
		// value, we'll get an empty previous value, which won't trigger the
		// optimization and we'll ratchet the node's value below.
		prevVal := p.scanForMaxValue(it, key, key, excludeTo, false /* initFrom */)
		if _, update := ratchetValue(prevVal, val); !update {
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
		b, meta := p.encodeValueSet(arr[:0], keyVal, gapVal)

		util.RacePreempt()
		err := it.Add(key, b, meta)
		switch err {
		case arenaskl.ErrArenaFull:
			atomic.StoreInt32(&p.isFull, 1)
			return err
		case arenaskl.ErrRecordExists:
			// Another thread raced and added the node, so just ratchet its
			// values instead (down below).
		case nil:
			// Add was successful, so finish initialization by scanning for gap
			// value and using it to ratchet the new nodes' values.
			p.scanForMaxValue(it, key, key, 0, true /* initFrom */)
			return nil
		default:
			panic(fmt.Sprintf("unexpected error: %v", err))
		}
	} else {
		// The key exists already.
		if opt == 0 {
			// Don't need to set either key or gap value, so done.
			return nil
		}
	}

	// Ratchet up the timestamps on the existing node, but don't set the
	// initialized bit, since we don't have the gap value from the previous
	// node. Leave finishing initialization to the goroutine that added the
	// node.
	p.ratchetValueSet(it, keyVal, gapVal, false /* setInit */)

	return nil
}

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
		p.ratchetValueSet(it, val, val, false /* setInit */)

		it.Next()
	}

	return true
}

func (p *sklPage) ratchetMaxTimestamp(ts hlc.Timestamp) {
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
	new := ts.WallTime
	if ts.Logical > 0 {
		new++
	}

	for {
		old := atomic.LoadInt64(&p.maxWallTime)
		if new <= old {
			break
		}

		if atomic.CompareAndSwapInt64(&p.maxWallTime, old, new) {
			break
		}
	}
}

// ratchetValueSet will update the current node's key and gap values to the
// maximum of their current values or the given values. If setInit is true,
// then the initialized bit will be set, indicating that the node is now
// fully initialized and its values can now be relied upon.
func (p *sklPage) ratchetValueSet(it *arenaskl.Iterator, keyVal, gapVal cacheValue, setInit bool) {
	// Array with constant size will remain on the stack.
	var arr [encodedValSize * 2]byte

	for {
		util.RacePreempt()

		meta := it.Meta()
		oldKeyVal, oldGapVal := p.decodeValueSet(it.Value(), meta)

		var keyValUpdate, gapValUpdate bool
		keyVal, keyValUpdate = ratchetValue(oldKeyVal, keyVal)
		gapVal, gapValUpdate = ratchetValue(oldGapVal, gapVal)
		update := keyValUpdate || gapValUpdate

		var initMeta uint16
		if setInit {
			// Always set the initialized bit.
			initMeta = initialized
		} else {
			// Preserve the current value of the initialized bit.
			initMeta = meta & initialized
		}

		// Check whether it's necessary to make an update.
		var err error
		if !update {
			newMeta := meta | initMeta

			if newMeta == meta {
				// New meta value is same as old, so no update necessary.
				return
			}

			// Set the initialized bit, but no need to update the values.
			err = it.SetMeta(newMeta)
		} else {
			// Ratchet the max timestamp.
			keyTs, gapTs := keyVal.ts, gapVal.ts
			if gapTs.Less(keyTs) {
				p.ratchetMaxTimestamp(keyTs)
			} else {
				p.ratchetMaxTimestamp(gapTs)
			}

			// Update the values, possibly preserving the init bit.
			b, newMeta := p.encodeValueSet(arr[:0], keyVal, gapVal)
			err = it.Set(b, newMeta|initMeta)
		}

		switch err {
		case nil:
			return

		case arenaskl.ErrArenaFull:
			atomic.StoreInt32(&p.isFull, 1)

			// Arena full, so ratchet the values to the max value.
			err = it.SetMeta(uint16(useMaxVal) | initMeta)
			if err == arenaskl.ErrRecordUpdated {
				continue
			}

			return

		case arenaskl.ErrRecordUpdated:
			// Record was updated by another thread, so restart ratchet attempt.
			continue

		default:
			panic(fmt.Sprintf("unexpected error: %v", err))
		}
	}
}

// scanForMaxValue scans for the maximum value in the specified range. It does
// so by first scanning backwards to the first initialized node and using its
// gap value as the initial "previous gap value". It then scans forward until it
// reaches the "from" key, ratcheting any uninitialized nodes it encounters (but
// not initializing them), and updating the candidate "previous gap value" as it
// goes. Once the scan enters the specified range, it begins keeping track of
// the maximum value that it has seen, which is initially set to the "previous
// gap value". It ratchets this maximum value with each new value that it
// encounters while iterating between "from" and "to", returning the final
// maximum when the iteration reaches the end of the range.
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
//    forced to ratchet them before continuing.
//
//    However, only the goroutine that inserted a node can switch it from
//    initializing to initialized. This enforces a "happens-before" relationship
//    between the creation of a node and the discovery of the gap value that is
//    used when initializing it. If any goroutine was able to initialize a node,
//    then this relationship would not exist and we could experience races where
//    a newly inserted node A's call to ensureFloorValue could come before the
//    insertion of a node B, but node B could be initialized with a gap value
//    discovered before the insertion of node A. For more on this, see the
//    discussion in #19672.
//
// 2. After the gap value of the first initialized node with a key less than or
//    equal to "from" has been found, the scanning goroutine will scan forwards
//    until it reaches the original "from" key. It will ratchet any
//    uninitialized nodes along the way and inherit the gap value from them as
//    it goes. By the time it reaches the original key, it has a valid gap
//    value, which we have called the "previous gap value". At this point, if
//    the node at "from" is uninitialized and initFrom == true, the node can be
//    initialized with the "previous gap value".
//
//    With the gap value for the first node in the range bounds determined, the
//    scan can then continue over the rest of the range, keeping track of the
//    maximum value seen between "from" and "to".
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
//
// The method takes arguments to specify a range to scan over. It is not legal
// to pass nil as the "from" argument or for "from" > "to". It also takes an
// "initFrom" flag, which specifies whether an uninitialized node at key "from"
// should be initialized, if one is discovered. The iterator is expected be
// positioned at "from" if a node exists at that key in the skiplist. If not, it
// is expected to be positioned on the preceding node.
func (p *sklPage) scanForMaxValue(
	it *arenaskl.Iterator, from, to []byte, opt rangeOptions, initFrom bool,
) cacheValue {
	clone := *it

	// Iterate backwards, looking for an already initialized node which will
	// supply the initial "previous gap value". If the iterator is positioned on
	// "from" and from is already initialized, the this will break immediately.
	for {
		util.RacePreempt()

		if !clone.Valid() {
			// No more previous nodes, so use the zero value and begin forward
			// iteration from the first node.
			clone.SeekToFirst()
			break
		}

		meta := clone.Meta()
		if (meta & initialized) != 0 {
			// Found an initialized node. We can begin the forward scan.
			break
		}

		// Haven't yet reached an initialized node, so keep iterating.
		clone.Prev()
	}

	// Now iterate forwards until "to" is reached. Keep track of the current gap
	// value and the maximum value seen between "from" and "to", respecting the
	// rangeOptions.
	var gapVal, maxVal cacheValue
	for {
		util.RacePreempt()

		if !clone.Valid() {
			// No more nodes, which can happen for open ranges. Ratchet maxVal
			// with the current gapVal and return.
			maxVal, _ = ratchetValue(maxVal, gapVal)
			return maxVal
		}

		itKey := clone.Key()
		fromCmp := bytes.Compare(itKey, from)
		toCmp := bytes.Compare(itKey, to)
		if to == nil {
			// to == nil means open range, so toCmp will always be -1.
			toCmp = -1
		}

		if (clone.Meta() & initialized) == 0 {
			// The node is uninitialized, so ratchet it with the current gap
			// value. Note that this ratcheting will be reflected in the call to
			// decodeValueSet below.
			//
			// If this is the scan's start key and initFrom == true, we also
			// initialize the node.
			setInit := fromCmp == 0 && initFrom
			p.ratchetValueSet(&clone, gapVal, gapVal, setInit)
		}

		if fromCmp > 0 {
			// Past the scan's start key so ratchet max with gapVal. Remember
			// that we may have scanned backwards above to get the initial
			// gapVal and we could be racing with other threads that are
			// inserting nodes, so it's not guaranteed that we'll reach this
			// case during the first few iterations.
			maxVal, _ = ratchetValue(maxVal, gapVal)
		}

		if toCmp > 0 || (toCmp == 0 && (opt&excludeTo) != 0) {
			// Past the scan's end key. Ratchet max with the current gapVal and
			// return.
			return maxVal
		}

		var keyVal cacheValue
		keyVal, gapVal = p.decodeValueSet(clone.Value(), clone.Meta())

		if fromCmp > 0 || (fromCmp == 0 && (opt&excludeFrom) == 0) {
			// This key is part of the desired range so ratchet max with its
			// value. Like the case above, we may not meet the condition for
			// this case if we are not yet in the range. However, the check
			// above guarantees that we'll never get here if we've already
			// passed the end of the range.
			maxVal, _ = ratchetValue(maxVal, keyVal)
		}

		if toCmp == 0 {
			// On the scan's end key, so return the max value seen.
			return maxVal
		}

		// Haven't yet reached the scan's end key, so keep iterating.
		clone.Next()
	}
}

func (p *sklPage) decodeValueSet(b []byte, meta uint16) (keyVal, gapVal cacheValue) {
	if (meta & useMaxVal) != 0 {
		ts := hlc.Timestamp{WallTime: atomic.LoadInt64(&p.maxWallTime)}
		maxVal := cacheValue{ts: ts, txnID: noTxnID}
		return maxVal, maxVal
	}

	if (meta & hasKey) != 0 {
		b, keyVal = decodeValue(b)
	}

	if (meta & hasGap) != 0 {
		_, gapVal = decodeValue(b)
	}

	return
}

func (p *sklPage) encodeValueSet(b []byte, keyVal, gapVal cacheValue) (ret []byte, meta uint16) {
	if keyVal.ts.WallTime != 0 || keyVal.ts.Logical != 0 {
		b = encodeValue(b, keyVal)
		meta |= hasKey
	}

	if gapVal.ts.WallTime != 0 || gapVal.ts.Logical != 0 {
		b = encodeValue(b, gapVal)
		meta |= hasGap
	}

	ret = b
	return
}

func decodeValue(b []byte) (ret []byte, val cacheValue) {
	val.ts.WallTime = int64(binary.BigEndian.Uint64(b))
	val.ts.Logical = int32(binary.BigEndian.Uint32(b[8:]))
	var err error
	if val.txnID, err = uuid.FromBytes(b[encodedTsSize:encodedValSize]); err != nil {
		panic(err)
	}
	ret = b[encodedValSize:]
	return
}

func encodeValue(b []byte, val cacheValue) []byte {
	l := len(b)
	b = b[:l+encodedValSize]
	binary.BigEndian.PutUint64(b[l:], uint64(val.ts.WallTime))
	binary.BigEndian.PutUint32(b[l+8:], uint32(val.ts.Logical))
	if _, err := val.txnID.MarshalTo(b[l+encodedTsSize:]); err != nil {
		panic(err)
	}
	return b
}
