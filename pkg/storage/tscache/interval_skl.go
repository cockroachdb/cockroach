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
	"encoding/binary"
	"fmt"
	"sync/atomic"

	"github.com/andy-kimball/arenaskl"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// rangeOptions are passed to AddRange to indicate the bounds of the range. By
// default, the "from" and "to" keys are inclusive. Setting these bit flags
// indicates that one or both is exclusive instead.
type rangeOptions int

const (
	// excludeFrom indicates that the range does not include the starting key.
	excludeFrom = 1 << iota

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
	// initializing indicates that the node has been created, but is still in a
	// provisional state. Any key and gap timestamps are not final values, and
	// should not be used until initialization is complete.
	initializing = 1 << iota

	// hasKey indicates that the node has an associated key timestamp. If this
	// is not set, then the key timestamp is assumed to be zero.
	hasKey

	// hasGap indicates that the node has an associated gap timestamp. If this
	// is not set, then the gap timestamp is assumed to be zero.
	hasGap

	// useMaxTs indicates that the intervalSkl's max value should be used in
	// place of the node's gap and key timestamps. This is used when the
	// structure has filled up and the value can no longer be set (but meta
	// can).
	useMaxTs
)

const (
	encodedTsSize = 12
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
	// Maximum size of the data structure in bytes. When the data structure
	// fills, older entries are discarded.
	size uint32

	// rotMutex synchronizes page rotation with all other operations. The read
	// lock is acquired by the Add and Lookup operations. The write lock is
	// acquired only when the pages are rotated. Since that is very rare, the
	// vast majority of operations can proceed without blocking.
	rotMutex syncutil.RWMutex

	// The structure maintains two fixed-size skiplist pages. When the later
	// page fills, it becomes the earlier page, and the previous earlier page is
	// discarded. In order to ensure that timestamps never decrease, intervalSkl
	// maintains a floor timestamp, which is the minimum read timestamp that can
	// be returned by the lookup operations. When the earlier page is discarded,
	// its current maximum read timestamp becomes the new floor timestamp for
	// the overall intervalSkl.
	later   *sklPage
	earlier *sklPage
	floorTs hlc.Timestamp
}

// newIntervalSkl creates a new interval skiplist with the given maximum size.
func newIntervalSkl(size uint32) *intervalSkl {
	// The earlier and later fixed pages are each 1/2 the size of the larger
	// page.
	return &intervalSkl{size: size, later: newSklPage(size / 2)}
}

// Add marks the a single key as having been read at the given timestamp. Once
// Add completes, future lookups of this key are guaranteed to return an equal
// or greater timestamp.
func (s *intervalSkl) Add(key []byte, ts hlc.Timestamp) {
	s.AddRange(key, key, 0, ts)
}

// AddRange marks the given range of keys (from, to) as having been read at the
// given timestamp. If some or all of the range was previously read at a higher
// timestamp, then the range is split into sub-ranges that are each marked with
// the maximum read timestamp for that sub-range. The starting and ending points
// of the range are inclusive by default, but can be excluded by passing the
// applicable range options. Once AddRange completes, future lookups at any point
// in the range are guaranteed to return an equal or greater timestamp.
func (s *intervalSkl) AddRange(from, to []byte, opt rangeOptions, ts hlc.Timestamp) {
	if from == nil {
		panic("from key cannot be nil")
	}

	if to != nil {
		cmp := bytes.Compare(from, to)
		if cmp > 0 {
			// Starting key is after ending key, so range is zero length.
			return
		}

		if cmp == 0 {
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
		filledPage := s.addRange(from, to, opt, ts)
		if filledPage == nil {
			break
		}

		// The page was filled up, so rotate the pages and then try again.
		s.rotatePages(filledPage)
	}
}

// addRange marks the given range of keys (from, to) as having been read at the
// given timestamp. It returns nil if the operation was successful, or a pointer
// to an sklPage if the operation failed because that page was full.
func (s *intervalSkl) addRange(from, to []byte, opt rangeOptions, ts hlc.Timestamp) *sklPage {
	// Acquire the rotation mutex read lock so that the page will not be rotated
	// while add or lookup operations are in progress.
	s.rotMutex.RLock()
	defer s.rotMutex.RUnlock()

	// If floor ts is >= requested timestamp, then no need to perform a search
	// or add any records.
	if !s.floorTs.Less(ts) {
		return nil
	}

	var it arenaskl.Iterator
	it.Init(s.later.list)

	// Start by ensuring that the ending node has been created (unless "to" is
	// nil, in which case the range extends indefinitely). Do this before creating
	// the start node, so that the range won't extend past the end point during
	// the period between creating the two endpoints.
	var err error
	if to != nil {
		if (opt & excludeTo) == 0 {
			err = s.later.addNode(&it, to, ts, hasKey)
		} else {
			err = s.later.addNode(&it, to, ts, 0)
		}

		if err == arenaskl.ErrArenaFull {
			return s.later
		}
	}

	// If from is nil, then the "range" is just a single key.
	if from == nil {
		return nil
	}

	// Ensure that the starting node has been created.
	if (opt & excludeFrom) == 0 {
		err = s.later.addNode(&it, from, ts, hasKey|hasGap)
	} else {
		err = s.later.addNode(&it, from, ts, hasGap)
	}

	if err == arenaskl.ErrArenaFull {
		return s.later
	}

	// Seek to the node immediately after the "from" node.
	if !it.Valid() || !bytes.Equal(it.Key(), from) {
		if it.Seek(from) {
			it.Next()
		}
	} else {
		it.Next()
	}

	// Now iterate forwards and ensure that all nodes between the start and
	// end (exclusive) have timestamps that are >= the range timestamp.
	if !s.later.ensureFloorTs(&it, to, ts) {
		// Page is filled up, so rotate pages and try again.
		return s.later
	}

	return nil
}

// rotatePages makes the later page the earlier page, and then discards the
// earlier page. The max timestamp of the earlier page becomes the new floor
// timestamp, in order to guarantee that timestamp lookups never return decreasing
// values.
func (s *intervalSkl) rotatePages(filledPage *sklPage) {
	// Acquire the rotation mutex write lock to lock the entire intervalSkl.
	s.rotMutex.Lock()
	defer s.rotMutex.Unlock()

	if filledPage != s.later {
		// Another thread already rotated the pages, so don't do anything more.
		return
	}

	// Max timestamp of the earlier page becomes the new floor timestamp.
	if s.earlier != nil {
		newFloorTs := hlc.Timestamp{WallTime: atomic.LoadInt64(&s.earlier.maxWallTime)}
		s.floorTs.Forward(newFloorTs)
	}

	// Make the later page the earlier page.
	s.earlier = s.later
	s.later = newSklPage(s.size / 2)
}

// LookupTimestamp returns the latest timestamp at which the given key was read.
// If this operation is repeated with the same key, it will always result in an
// equal or greater timestamp.
func (s *intervalSkl) LookupTimestamp(key []byte) hlc.Timestamp {
	// Acquire the rotation mutex read lock so that the page will not be rotated
	// while add or lookup operations are in progress.
	s.rotMutex.RLock()
	defer s.rotMutex.RUnlock()

	// First perform lookup on the later page.
	ts := s.later.lookupTimestamp(key)

	// Now perform same lookup on the earlier page.
	if s.earlier != nil {
		// If later page timestamp is greater than the max timestamp in the
		// earlier page, then no need to do lookup at all.
		maxTs := hlc.Timestamp{WallTime: atomic.LoadInt64(&s.earlier.maxWallTime)}
		if ts.Less(maxTs) {
			ts2 := s.earlier.lookupTimestamp(key)
			if ts.Less(ts2) {
				ts = ts2
			}
		}
	}

	// Return the higher timestamp from the two lookups.
	if ts.Less(s.floorTs) {
		ts = s.floorTs
	}

	return ts
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

func (p *sklPage) lookupTimestamp(key []byte) hlc.Timestamp {
	var it arenaskl.Iterator
	it.Init(p.list)

	if !it.SeekForPrev(key) {
		// Key not found, so scan previous nodes to find the gap timestamp.
		return p.scanForTimestamp(&it, key, false)
	}

	if (it.Meta() & initializing) != 0 {
		// Node is not yet initialized, so scan previous nodes to find the
		// gap timestamp needed to initialize this node.
		return p.scanForTimestamp(&it, key, true)
	}

	keyTs, _ := p.decodeTimestampSet(it.Value(), it.Meta())
	return keyTs
}

func (p *sklPage) addNode(
	it *arenaskl.Iterator, key []byte, ts hlc.Timestamp, opt nodeOptions,
) error {
	var arr [encodedTsSize * 2]byte
	var keyTs, gapTs hlc.Timestamp

	if (opt & hasKey) != 0 {
		keyTs = ts
	}

	if (opt & hasGap) != 0 {
		gapTs = ts
	}

	if !it.SeekForPrev(key) {
		// If the previous node has a gap timestamp that is >= than the new
		// timestamp, then there is no need to add another node, since its
		// timestamp would be the same as the gap timestamp.
		prevTs := p.scanForTimestamp(it, key, false)
		if !prevTs.Less(ts) {
			return nil
		}

		// Ratchet max timestamp before adding the node.
		p.ratchetMaxTimestamp(ts)

		// Ensure that a new node is created. It needs to stay in the
		// initializing state until the gap timestamp of its preceding node
		// has been found and used to ratchet this node's timestamps. During
		// the search for the gap timestamp, this node acts as a sentinel
		// for other ongoing operations - when they see this node they're
		// forced to stop and help complete its initialization before they
		// can continue.
		b, meta := p.encodeTimestampSet(arr[:0], keyTs, gapTs)
		err := it.Add(key, b, meta|initializing)
		if err == arenaskl.ErrArenaFull {
			atomic.StoreInt32(&p.isFull, 1)
			return err
		}

		if err == nil {
			// Add was successful, so finish initialization by scanning for
			// gap timestamp and using it to ratchet the new nodes' timestamps.
			p.scanForTimestamp(it, key, true)
			return nil
		}

		// Another thread raced and added the node, so just ratchet its
		// timestamps instead.
	} else {
		if opt == 0 {
			// Don't need to set either key or gap ts, so done.
			return nil
		}
	}

	// Ratchet up the timestamps on the existing node, but don't clear the
	// initializing bit, since we don't have the gap timestamp from the previous
	// node. Leave finishing initialization to the thread that added the node, or
	// to a lookup thread that requires it.
	p.ratchetTimestampSet(it, keyTs, gapTs, false)

	return nil
}

func (p *sklPage) ensureFloorTs(it *arenaskl.Iterator, to []byte, ts hlc.Timestamp) bool {
	for it.Valid() {
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
		p.ratchetTimestampSet(it, ts, ts, false)

		it.Next()
	}

	return true
}

func (p *sklPage) ratchetMaxTimestamp(ts hlc.Timestamp) {
	// Cheat and just use the max wall time portion of the timestamp, since it's
	// fine for the max timestamp to be a bit too large.
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

// ratchetTimestampSet will update the current node's key and gap timestamps to
// the maximum of their current values or the given values. If clearInit is true,
// then the initializing bit will be cleared, indicating that the node is now
// fully initialized and its timestamps can now be relied upon.
func (p *sklPage) ratchetTimestampSet(
	it *arenaskl.Iterator, keyTs, gapTs hlc.Timestamp, clearInit bool,
) {
	var arr [encodedTsSize * 2]byte

	for {
		meta := it.Meta()
		oldKeyTs, oldGapTs := p.decodeTimestampSet(it.Value(), meta)

		greater := false
		if oldKeyTs.Less(keyTs) {
			greater = true
		} else {
			keyTs = oldKeyTs
		}

		if oldGapTs.Less(gapTs) {
			greater = true
		} else {
			gapTs = oldGapTs
		}

		var initMeta uint16
		if clearInit {
			initMeta = 0
		} else {
			initMeta = meta & initializing
		}

		// Check whether it's necessary to make an update.
		var err error
		if !greater {
			if !clearInit || (meta&initializing) == 0 {
				// No update necessary because the init bit doesn't need to be
				// cleared or it's already cleared.
				return
			}

			// Clear the initializing bit, but no need to update the timestamps.
			err = it.SetMeta(meta & ^uint16(initializing))
		} else {
			// Ratchet the max timestamp.
			if gapTs.Less(keyTs) {
				p.ratchetMaxTimestamp(keyTs)
			} else {
				p.ratchetMaxTimestamp(gapTs)
			}

			// Update the timestamps, possibly preserving the init bit.
			b, newMeta := p.encodeTimestampSet(arr[:0], keyTs, gapTs)
			err = it.Set(b, newMeta|initMeta)
		}

		switch err {
		case nil:
			return

		case arenaskl.ErrArenaFull:
			atomic.StoreInt32(&p.isFull, 1)

			// Arena full, so ratchet the timestamps to the max timestamp.
			err = it.SetMeta(uint16(useMaxTs) | initMeta)
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

// scanForTimestamp scans backwards for the first initialized node and uses its
// gap timestamp as the initial candidate. It then scans forwards until it
// reaches the termination key, ratcheting any uninitialized nodes it encounters,
// and updating the candidate gap timestamp as it goes. The timestamp of the
// termination key is returned.
//
// Iterating backwards and then forwards solves potential race conditions with
// other threads. During iteration backwards, other nodes can be inserting new
// nodes between the previous node and the lookup node, which could change the
// correct value of the gap timestamp. The solution is two-fold:
//
// 1. Add new nodes in two phases - initializing and then initialized. Nodes in
//    the initializing state act as a synchronization point between goroutines
//    that are adding a particular node and goroutines that are scanning for gap
//    timestamps. Scanning goroutines encounter the initializing nodes and are
//    forced to deal with them before continuing.
//
// 2. After the gap timestamp of the previous node has been found, the scanning
//    goroutine will scan forwards until it reaches the original key. It will
//    complete initialization of any nodes along the way and inherit the gap
//    timestamp of initialized nodes as it goes. By the time it reaches the
//    original key, it has a valid gap timestamp value.
//
// During forward iteration, if another goroutine inserts a new gap node in the
// interval between the previous node and the original key, then either:
//
// 1. The forward iteration finds it and looks up its gap timestamp. That node
//    now becomes the new "previous node", and iteration continues.
//
// 2. The new node is created after the iterator has move past its position. As
//    part of node creation, the creator had to scan backwards to find the gap
//    timestamp of the previous node. It is guaranteed to find a gap timestamp
//    that is >= the gap timestamp found by the original goroutine.
//
// This means that no matter what gets inserted, or when it gets inserted, the
// scanning goroutine is guaranteed to end up with a timestamp value that will
// never decrease on future lookups, which is the critical invariant.
func (p *sklPage) scanForTimestamp(it *arenaskl.Iterator, key []byte, onKey bool) hlc.Timestamp {
	clone := *it

	if onKey {
		// The iterator is currently positioned on the key node, so need to
		// iterate backwards from there in order to find the gap timestamp.
		clone.Prev()
	}

	// First iterate backwards, looking for an already initialized node which
	// will supply the initial candidate gap timestamp.
	var gapTs hlc.Timestamp
	for {
		if !clone.Valid() {
			// No more previous nodes, so use the zero timestamp and begin
			// forward iteration from the first node.
			clone.SeekToFirst()
			break
		}

		meta := clone.Meta()
		if (meta & initializing) == 0 {
			// Found the gap timestamp for an initialized node.
			_, gapTs = p.decodeTimestampSet(clone.Value(), meta)
			clone.Next()
			break
		}

		clone.Prev()
	}

	// Now iterate forwards until "key" is reached, update any uninitialized
	// nodes along the way, and update the gap timestamp.
	for {
		if !clone.Valid() {
			return gapTs
		}

		if (clone.Meta() & initializing) != 0 {
			// Finish initializing the node with the gap timestamp.
			p.ratchetTimestampSet(&clone, gapTs, gapTs, true)
		}

		cmp := bytes.Compare(clone.Key(), key)
		if cmp > 0 {
			// Past the lookup key, so use the gap timestamp.
			return gapTs
		}

		var keyTs hlc.Timestamp
		keyTs, gapTs = p.decodeTimestampSet(clone.Value(), clone.Meta())

		if cmp == 0 {
			// On the lookup key, so use the key timestamp.
			return keyTs
		}

		// Haven't yet reached the lookup key, so keep iterating.
		clone.Next()
	}
}

func (p *sklPage) decodeTimestampSet(b []byte, meta uint16) (keyTs, gapTs hlc.Timestamp) {
	if (meta & useMaxTs) != 0 {
		ts := hlc.Timestamp{WallTime: atomic.LoadInt64(&p.maxWallTime)}
		return ts, ts
	}

	if (meta & hasKey) != 0 {
		b, keyTs = decodeTimestamp(b)
	}

	if (meta & hasGap) != 0 {
		_, gapTs = decodeTimestamp(b)
	}

	return
}

func (p *sklPage) encodeTimestampSet(
	b []byte, keyTs, gapTs hlc.Timestamp,
) (ret []byte, meta uint16) {
	if keyTs.WallTime != 0 || keyTs.Logical != 0 {
		b = encodeTimestamp(b, keyTs)
		meta |= hasKey
	}

	if gapTs.WallTime != 0 || gapTs.Logical != 0 {
		b = encodeTimestamp(b, gapTs)
		meta |= hasGap
	}

	ret = b
	return
}

func decodeTimestamp(b []byte) (ret []byte, ts hlc.Timestamp) {
	wallTime := binary.BigEndian.Uint64(b)
	logical := binary.BigEndian.Uint32(b[8:])
	ts = hlc.Timestamp{WallTime: int64(wallTime), Logical: int32(logical)}
	ret = b[encodedTsSize:]
	return
}

func encodeTimestamp(b []byte, ts hlc.Timestamp) []byte {
	l := len(b)
	b = b[:l+encodedTsSize]
	binary.BigEndian.PutUint64(b[l:], uint64(ts.WallTime))
	binary.BigEndian.PutUint32(b[l+8:], uint32(ts.Logical))
	return b
}
