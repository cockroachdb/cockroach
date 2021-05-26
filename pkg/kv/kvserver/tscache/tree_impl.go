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
	"fmt"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const (
	// defaultTreeImplSize is the default size in bytes for a treeImpl timestamp
	// cache. Note that the timestamp cache can use more memory than this
	// because it holds on to all entries that are younger than
	// MinRetentionWindow.
	defaultTreeImplSize = 64 << 20 // 64 MB
)

func makeCacheEntry(key cache.IntervalKey, value cacheValue) *cache.Entry {
	alloc := struct {
		key   cache.IntervalKey
		value cacheValue
		entry cache.Entry
	}{
		key:   key,
		value: value,
	}
	alloc.entry.Key = &alloc.key
	alloc.entry.Value = &alloc.value
	return &alloc.entry
}

var cacheEntryOverhead = uint64(unsafe.Sizeof(cache.IntervalKey{}) +
	unsafe.Sizeof(cacheValue{}) + unsafe.Sizeof(cache.Entry{}))

func cacheEntrySize(start, end interval.Comparable) uint64 {
	n := uint64(cap(start))
	if end != nil && len(start) > 0 && len(end) > 0 && &end[0] != &start[0] {
		// If the end key exists and is not sharing memory with the start key,
		// account for its memory usage.
		n += uint64(cap(end))
	}
	n += cacheEntryOverhead
	return n
}

// treeImpl implements the Cache interface. It maintains an interval tree FIFO
// cache of keys or key ranges and the timestamps at which they were most
// recently read or written. If a timestamp was read or written by a
// transaction, the txn ID is stored with the timestamp to avoid advancing
// timestamps on successive requests from the same transaction.
type treeImpl struct {
	syncutil.RWMutex

	cache            *cache.IntervalCache
	lowWater, latest hlc.Timestamp

	bytes    uint64
	maxBytes uint64
	metrics  Metrics
}

var _ Cache = &treeImpl{}

// newTreeImpl returns a new treeImpl with the supplied hybrid clock.
func newTreeImpl(clock *hlc.Clock) *treeImpl {
	tc := &treeImpl{
		cache:    cache.NewIntervalCache(cache.Config{Policy: cache.CacheFIFO}),
		maxBytes: uint64(defaultTreeImplSize),
		metrics:  makeMetrics(),
	}
	tc.clear(clock.Now())
	tc.cache.Config.ShouldEvict = tc.shouldEvict
	tc.cache.Config.OnEvicted = tc.onEvicted
	return tc
}

// clear clears the cache and resets the low-water mark.
func (tc *treeImpl) clear(lowWater hlc.Timestamp) {
	tc.Lock()
	defer tc.Unlock()
	tc.cache.Clear()
	tc.lowWater = lowWater
	tc.latest = tc.lowWater
}

// len returns the total number of read and write intervals in the cache.
func (tc *treeImpl) len() int {
	tc.RLock()
	defer tc.RUnlock()
	return tc.cache.Len()
}

// Add implements the Cache interface.
func (tc *treeImpl) Add(start, end roachpb.Key, ts hlc.Timestamp, txnID uuid.UUID) {
	// This gives us a memory-efficient end key if end is empty.
	if len(end) == 0 {
		end = start.Next()
		start = end[:len(start)]
	}

	tc.Lock()
	defer tc.Unlock()
	tc.latest.Forward(ts)

	// Only add to the cache if the timestamp is more recent than the
	// low water mark.
	if tc.lowWater.Less(ts) {

		addRange := func(r interval.Range) {
			value := cacheValue{ts: ts, txnID: txnID}
			key := tc.cache.MakeKey(r.Start, r.End)
			entry := makeCacheEntry(key, value)
			tc.bytes += cacheEntrySize(r.Start, r.End)
			tc.cache.AddEntry(entry)
		}
		addEntryAfter := func(entry, after *cache.Entry) {
			ck := entry.Key.(*cache.IntervalKey)
			tc.bytes += cacheEntrySize(ck.Start, ck.End)
			tc.cache.AddEntryAfter(entry, after)
		}

		r := interval.Range{
			Start: interval.Comparable(start),
			End:   interval.Comparable(end),
		}

		// Check existing, overlapping entries and truncate/split/remove if
		// superseded and in the past. If existing entries are in the future,
		// subtract from the range/ranges that need to be added to cache.
		for _, entry := range tc.cache.GetOverlaps(r.Start, r.End) {
			cv := entry.Value.(*cacheValue)
			key := entry.Key.(*cache.IntervalKey)
			sCmp := r.Start.Compare(key.Start)
			eCmp := r.End.Compare(key.End)
			// Some of the cases below adjust cv and key in-place (in a manner that
			// maintains the IntervalCache invariants). These in-place modifications
			// change the size of the entry. To capture all of these modifications we
			// compute the current size of the entry and then use the new size at the
			// end of this iteration to update Cache.bytes.
			oldSize := cacheEntrySize(key.Start, key.End)
			if cv.ts.Less(ts) {
				// The existing interval has a timestamp less than the new
				// interval. Compare interval ranges to determine how to
				// modify existing interval.
				switch {
				case sCmp == 0 && eCmp == 0:
					// New and old are equal; replace old with new and avoid the need to insert new.
					//
					// New: ------------
					// Old: ------------
					//
					// New: ------------
					// Old:
					*cv = cacheValue{ts: ts, txnID: txnID}
					tc.cache.MoveToEnd(entry)
					return
				case sCmp <= 0 && eCmp >= 0:
					// New contains or is equal to old; delete old.
					//
					// New: ------------      ------------      ------------
					// Old:   --------    or    ----------  or  ----------
					//
					// New: ------------      ------------      ------------
					// Old:
					tc.cache.DelEntry(entry)
					continue // DelEntry adjusted tc.bytes, don't do it again
				case sCmp > 0 && eCmp < 0:
					// Old contains new; split up old into two.
					//
					// New:     ----
					// Old: ------------
					//
					// New:     ----
					// Old: ----    ----
					oldEnd := key.End
					key.End = r.Start

					newKey := tc.cache.MakeKey(r.End, oldEnd)
					newEntry := makeCacheEntry(newKey, *cv)
					addEntryAfter(newEntry, entry)
				case eCmp >= 0:
					// Left partial overlap; truncate old end.
					//
					// New:     --------          --------
					// Old: --------      or  ------------
					//
					// New:     --------          --------
					// Old: ----              ----
					key.End = r.Start
				case sCmp <= 0:
					// Right partial overlap; truncate old start.
					//
					// New: --------          --------
					// Old:     --------  or  ------------
					//
					// New: --------          --------
					// Old:         ----              ----
					key.Start = r.End
				default:
					panic(fmt.Sprintf("no overlap between %v and %v", key.Range, r))
				}
			} else if ts.Less(cv.ts) {
				// The existing interval has a timestamp greater than the new interval.
				// Compare interval ranges to determine how to modify new interval before
				// adding it to the timestamp cache.
				switch {
				case sCmp >= 0 && eCmp <= 0:
					// Old contains or is equal to new; no need to add.
					//
					// Old: -----------      -----------      -----------      -----------
					// New:    -----     or  -----------  or  --------     or     --------
					//
					// Old: -----------      -----------      -----------      -----------
					// New:
					return
				case sCmp < 0 && eCmp > 0:
					// New contains old; split up old into two. We can add the left piece
					// immediately because it is guaranteed to be before the rest of the
					// overlaps.
					//
					// Old:    ------
					// New: ------------
					//
					// Old:    ------
					// New: ---      ---
					lr := interval.Range{Start: r.Start, End: key.Start}
					addRange(lr)

					r.Start = key.End
				case eCmp > 0:
					// Left partial overlap; truncate new start.
					//
					// Old: --------          --------
					// New:     --------  or  ------------
					//
					// Old: --------          --------
					// New:         ----              ----
					r.Start = key.End
				case sCmp < 0:
					// Right partial overlap; truncate new end.
					//
					// Old:     --------          --------
					// New: --------      or  ------------
					//
					// Old:     --------          --------
					// New: ----              ----
					r.End = key.Start
				default:
					panic(fmt.Sprintf("no overlap between %v and %v", key.Range, r))
				}
			} else if cv.txnID == txnID {
				// The existing interval has a timestamp equal to the new
				// interval, and the same transaction ID.
				switch {
				case sCmp >= 0 && eCmp <= 0:
					// Old contains or is equal to new; no need to add.
					//
					// New:    -----     or  -----------  or  --------     or     --------
					// Old: -----------      -----------      -----------      -----------
					//
					// New:
					// Old: -----------      -----------      -----------      -----------
					return
				case sCmp <= 0 && eCmp >= 0:
					// New contains old; delete old.
					//
					// New: ------------      ------------      ------------
					// Old:   --------    or    ----------  or  ----------
					//
					// New: ------------      ------------      ------------
					// Old:
					tc.cache.DelEntry(entry)
					continue // DelEntry adjusted tc.bytes, don't do it again
				case eCmp >= 0:
					// Left partial overlap; truncate old end.
					//
					// New:     --------          --------
					// Old: --------      or  ------------
					//
					// New:     --------          --------
					// Old: ----              ----
					key.End = r.Start
				case sCmp <= 0:
					// Right partial overlap; truncate old start.
					//
					// New: --------          --------
					// Old:     --------  or  ------------
					//
					// New: --------          --------
					// Old:         ----              ----
					key.Start = r.End
				default:
					panic(fmt.Sprintf("no overlap between %v and %v", key.Range, r))
				}
			} else {
				// The existing interval has a timestamp equal to the new
				// interval and a different transaction ID.
				switch {
				case sCmp == 0 && eCmp == 0:
					// New and old are equal. Segment is no longer owned by any
					// transaction.
					//
					// New: ------------
					// Old: ------------
					//
					// New:
					// Nil: ============
					// Old:
					cv.txnID = noTxnID
					tc.bytes += cacheEntrySize(key.Start, key.End) - oldSize
					return
				case sCmp == 0 && eCmp > 0:
					// New contains old, left-aligned. Clear ownership of the
					// existing segment and truncate new.
					//
					// New: ------------
					// Old: ----------
					//
					// New:           --
					// Nil: ==========
					// Old:
					cv.txnID = noTxnID
					r.Start = key.End
				case sCmp < 0 && eCmp == 0:
					// New contains old, right-aligned. Clear ownership of the
					// existing segment and truncate new.
					//
					// New: ------------
					// Old:   ----------
					//
					// New: --
					// Nil:   ==========
					// Old:
					cv.txnID = noTxnID
					r.End = key.Start
				case sCmp < 0 && eCmp > 0:
					// New contains old; split into three segments with the
					// overlap owned by no txn.
					//
					// New: ------------
					// Old:   --------
					//
					// New: --        --
					// Nil:   ========
					// Old:
					cv.txnID = noTxnID

					newKey := tc.cache.MakeKey(r.Start, key.Start)
					newEntry := makeCacheEntry(newKey, cacheValue{ts: ts, txnID: txnID})
					addEntryAfter(newEntry, entry)
					r.Start = key.End
				case sCmp > 0 && eCmp < 0:
					// Old contains new; split up old into two. New segment is
					// owned by no txn.
					//
					// New:     ----
					// Old: ------------
					//
					// New:
					// Nil:     ====
					// Old: ----    ----
					txnID = noTxnID
					oldEnd := key.End
					key.End = r.Start

					newKey := tc.cache.MakeKey(r.End, oldEnd)
					newEntry := makeCacheEntry(newKey, *cv)
					addEntryAfter(newEntry, entry)
				case eCmp == 0:
					// Old contains new, right-aligned; truncate old end and clear
					// ownership of new segment.
					//
					// New:     --------
					// Old: ------------
					//
					// New:
					// Nil:     ========
					// Old: ----
					txnID = noTxnID
					key.End = r.Start
				case sCmp == 0:
					// Old contains new, left-aligned; truncate old start and
					// clear ownership of new segment.
					// New: --------
					// Old: ------------
					//
					// New:
					// Nil: ========
					// Old:         ----
					txnID = noTxnID
					key.Start = r.End
				case eCmp > 0:
					// Left partial overlap; truncate old end and split new into
					// segments owned by no txn (the overlap) and the new txn.
					//
					// New:     --------
					// Old: --------
					//
					// New:         ----
					// Nil:     ====
					// Old: ----
					key.End, r.Start = r.Start, key.End

					newKey := tc.cache.MakeKey(key.End, r.Start)
					newCV := cacheValue{ts: cv.ts}
					newEntry := makeCacheEntry(newKey, newCV)
					addEntryAfter(newEntry, entry)
				case sCmp < 0:
					// Right partial overlap; truncate old start and split new into
					// segments owned by no txn (the overlap) and the new txn.
					//
					// New: --------
					// Old:     --------
					//
					// New: ----
					// Nil:     ====
					// Old:         ----
					key.Start, r.End = r.End, key.Start

					newKey := tc.cache.MakeKey(r.End, key.Start)
					newCV := cacheValue{ts: cv.ts}
					newEntry := makeCacheEntry(newKey, newCV)
					addEntryAfter(newEntry, entry)
				default:
					panic(fmt.Sprintf("no overlap between %v and %v", key.Range, r))
				}
			}
			tc.bytes += cacheEntrySize(key.Start, key.End) - oldSize
		}
		addRange(r)
	}
}

// getLowWater implements the Cache interface.
func (tc *treeImpl) getLowWater() hlc.Timestamp {
	tc.RLock()
	defer tc.RUnlock()
	return tc.lowWater
}

// GetMax implements the Cache interface.
func (tc *treeImpl) GetMax(start, end roachpb.Key) (hlc.Timestamp, uuid.UUID) {
	return tc.getMax(start, end)
}

func (tc *treeImpl) getMax(start, end roachpb.Key) (hlc.Timestamp, uuid.UUID) {
	tc.Lock()
	defer tc.Unlock()
	if len(end) == 0 {
		end = start.Next()
	}
	maxTS := tc.lowWater
	maxTxnID := noTxnID
	for _, o := range tc.cache.GetOverlaps(start, end) {
		ce := o.Value.(*cacheValue)
		if maxTS.Less(ce.ts) {
			maxTS = ce.ts
			maxTxnID = ce.txnID
		} else if maxTS == ce.ts && maxTxnID != ce.txnID {
			maxTxnID = noTxnID
		}
	}
	return maxTS, maxTxnID
}

// shouldEvict returns true if the cache entry's timestamp is no
// longer within the MinRetentionWindow.
func (tc *treeImpl) shouldEvict(size int, key, value interface{}) bool {
	if tc.bytes <= tc.maxBytes {
		return false
	}
	ce := value.(*cacheValue)
	// In case low water mark was set higher, evict any entries
	// which occurred before it.
	if ce.ts.Less(tc.lowWater) {
		return true
	}
	// Compute the edge of the cache window.
	edge := tc.latest.Add(-MinRetentionWindow.Nanoseconds(), 0)
	// We evict and update the low water mark if the proposed evictee's
	// timestamp is <= than the edge of the window.
	if ce.ts.LessEq(edge) {
		tc.lowWater = ce.ts
		return true
	}
	return false
}

// onEvicted is called when an entry is evicted from the cache.
func (tc *treeImpl) onEvicted(k, v interface{}) {
	ck := k.(*cache.IntervalKey)
	reqSize := cacheEntrySize(ck.Start, ck.End)
	if tc.bytes < reqSize {
		panic(fmt.Sprintf("bad reqSize: %d < %d", tc.bytes, reqSize))
	}
	tc.bytes -= reqSize
}

// Metrics implements the Cache interface.
func (tc *treeImpl) Metrics() Metrics {
	return tc.metrics
}
