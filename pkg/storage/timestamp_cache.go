// Copyright 2014 The Cockroach Authors.
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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"fmt"
	"time"
	"unsafe"

	"github.com/google/btree"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const (
	// MinTSCacheWindow specifies the minimum duration to hold entries in the
	// cache before allowing eviction. After this window expires, transactions
	// writing to this node with timestamps lagging by more than MinTSCacheWindow
	// will necessarily have to advance their commit timestamp.
	MinTSCacheWindow = 10 * time.Second

	// defaultTimestampCacheSize is the default size in bytes for a store's
	// timestamp cache. Note that the timestamp cache can use more memory than
	// this because it holds on to all entries that are younger than
	// MinTSCacheWindow.
	defaultTimestampCacheSize = 64 << 20 // 64 MB

	// Max entries in each btree node.
	// TODO(peter): Not yet tuned.
	btreeDegree = 64
)

// cacheRequest holds the timestamp cache data from a single batch request. The
// requests are stored in a btree keyed by the timestamp and are "expanded" to
// populate the read/write interval caches if a potential conflict is detected
// due to an earlier request (based on timestamp) arriving.
type cacheRequest struct {
	span      roachpb.RSpan
	reads     []roachpb.Span
	writes    []roachpb.Span
	txn       roachpb.Span
	txnID     *uuid.UUID
	timestamp hlc.Timestamp
	// Used to distinguish requests with identical timestamps. For actual
	// requests, the uniqueID value is >0. When probing the btree for requests
	// later than a particular timestamp a value of 0 is used.
	uniqueID int64
}

// Less implements the btree.Item interface.
func (cr *cacheRequest) Less(other btree.Item) bool {
	otherReq := other.(*cacheRequest)
	if cr.timestamp.Less(otherReq.timestamp) {
		return true
	}
	if otherReq.timestamp.Less(cr.timestamp) {
		return false
	}
	// Fallback to comparison of the uniqueID as a tie-breaker. This allows
	// multiple requests with the same timestamp to exist in the requests btree.
	return cr.uniqueID < otherReq.uniqueID
}

// numSpans returns the number of spans the request will expand into.
func (cr *cacheRequest) numSpans() int {
	n := len(cr.reads) + len(cr.writes)
	if cr.txn.Key != nil {
		n++
	}
	return n
}

func (cr *cacheRequest) size() uint64 {
	var n uint64
	for _, s := range cr.reads {
		n += cacheEntrySize(interval.Comparable(s.Key), interval.Comparable(s.EndKey), cr.txnID)
	}
	for _, s := range cr.writes {
		n += cacheEntrySize(interval.Comparable(s.Key), interval.Comparable(s.EndKey), cr.txnID)
	}
	if cr.txn.Key != nil {
		n += cacheEntrySize(interval.Comparable(cr.txn.Key), nil, nil)
	}
	return n
}

var cacheEntryOverhead = uint64(unsafe.Sizeof(cache.IntervalKey{}) +
	unsafe.Sizeof(cacheValue{}) + unsafe.Sizeof(cache.Entry{}))

func cacheEntrySize(start, end interval.Comparable, txnID *uuid.UUID) uint64 {
	n := uint64(cap(start))
	if end != nil && len(start) > 0 && len(end) > 0 && &end[0] != &start[0] {
		// If the end key exists and is not sharing memory with the start key,
		// account for its memory usage.
		n += uint64(cap(end))
	}
	if txnID != nil {
		// Every entry which references the txn ID is charged for its memory. This
		// results in counting that memory multiple times with the result that the
		// cache might not be using as much memory as we've configured it for.
		n += uint64(unsafe.Sizeof(*txnID))
	}
	n += cacheEntryOverhead
	return n
}

// A TimestampCache maintains an interval tree FIFO cache of keys or
// key ranges and the timestamps at which they were most recently read
// or written. If a timestamp was read or written by a transaction,
// the txn ID is stored with the timestamp to avoid advancing
// timestamps on successive requests from the same transaction.
//
// The cache also maintains a low-water mark which is the most
// recently evicted entry's timestamp. This value always ratchets
// with monotonic increases. The low water mark is initialized to
// the current system time plus the maximum clock offset.
type timestampCache struct {
	rCache, wCache   *cache.IntervalCache
	lowWater, latest hlc.Timestamp

	// The requests tree contains cacheRequest entries keyed by timestamp. A
	// request is "expanded" (i.e. the read/write spans are added to the
	// read/write interval caches) when the timestamp cache is accessed on behalf
	// of an earlier request.
	requests   *btree.BTree
	tmpReq     cacheRequest
	reqIDAlloc int64
	reqSpans   int

	bytes    uint64
	maxBytes uint64
}

// lowWaterTxnIDMarker is a special txn ID that identifies a cache entry as a
// low water mark. It is specified when a lease is acquired to clear the
// timestamp cache for a range. Also see timestampCache.getMax where this txn
// ID is checked in order to return whether the max read/write timestamp came
// from a regular entry or one of these low water mark entries.
var lowWaterTxnIDMarker = func() *uuid.UUID {
	// The specific txn ID used here isn't important. We use something that is a)
	// non-zero and b) obvious.
	u, err := uuid.FromString("11111111-1111-1111-1111-111111111111")
	if err != nil {
		log.Fatal(context.Background(), err)
	}
	return &u
}()

// A cacheValue combines the timestamp with an optional txn ID.
type cacheValue struct {
	timestamp hlc.Timestamp
	txnID     *uuid.UUID // Nil for no transaction
}

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

// newTimestampCache returns a new timestamp cache with supplied
// hybrid clock.
func newTimestampCache(clock *hlc.Clock) *timestampCache {
	tc := &timestampCache{
		rCache:   cache.NewIntervalCache(cache.Config{Policy: cache.CacheFIFO}),
		wCache:   cache.NewIntervalCache(cache.Config{Policy: cache.CacheFIFO}),
		maxBytes: uint64(defaultTimestampCacheSize),
	}
	tc.Clear(clock.Now())
	tc.rCache.Config.ShouldEvict = tc.shouldEvict
	tc.wCache.Config.ShouldEvict = tc.shouldEvict

	onEvicted := func(k, v interface{}) {
		ck := k.(*cache.IntervalKey)
		cv := v.(*cacheValue)
		reqSize := cacheEntrySize(ck.Start, ck.End, cv.txnID)
		if tc.bytes < reqSize {
			panic(fmt.Sprintf("bad reqSize: %d < %d", tc.bytes, reqSize))
		}
		tc.bytes -= reqSize
	}
	tc.rCache.Config.OnEvicted = onEvicted
	tc.wCache.Config.OnEvicted = onEvicted
	return tc
}

// Clear clears the cache and resets the low-water mark.
func (tc *timestampCache) Clear(lowWater hlc.Timestamp) {
	tc.requests = btree.New(btreeDegree)
	tc.rCache.Clear()
	tc.wCache.Clear()
	tc.lowWater = lowWater
	tc.latest = tc.lowWater
}

// len returns the total number of read and write intervals in the
// TimestampCache.
func (tc *timestampCache) len() int {
	return tc.rCache.Len() + tc.wCache.Len() + tc.reqSpans
}

// add the specified timestamp to the cache as covering the range of
// keys from start to end. If end is nil, the range covers the start
// key only. txnID is nil for no transaction. readTSCache specifies
// whether the command adding this timestamp should update the read
// timestamp; false to update the write timestamp cache.
func (tc *timestampCache) add(
	start, end roachpb.Key, timestamp hlc.Timestamp, txnID *uuid.UUID, readTSCache bool,
) {
	// This gives us a memory-efficient end key if end is empty.
	if len(end) == 0 {
		end = start.Next()
		start = end[:len(start)]
	}
	tc.latest.Forward(timestamp)
	// Only add to the cache if the timestamp is more recent than the
	// low water mark.
	if tc.lowWater.Less(timestamp) {
		tcache := tc.wCache
		if readTSCache {
			tcache = tc.rCache
		}

		addRange := func(r interval.Range) {
			value := cacheValue{timestamp: timestamp, txnID: txnID}
			key := tcache.MakeKey(r.Start, r.End)
			entry := makeCacheEntry(key, value)
			tc.bytes += cacheEntrySize(r.Start, r.End, txnID)
			tcache.AddEntry(entry)
		}
		addEntryAfter := func(entry, after *cache.Entry) {
			ck := entry.Key.(*cache.IntervalKey)
			cv := entry.Value.(*cacheValue)
			tc.bytes += cacheEntrySize(ck.Start, ck.End, cv.txnID)
			tcache.AddEntryAfter(entry, after)
		}

		r := interval.Range{
			Start: interval.Comparable(start),
			End:   interval.Comparable(end),
		}

		// Check existing, overlapping entries and truncate/split/remove if
		// superseded and in the past. If existing entries are in the future,
		// subtract from the range/ranges that need to be added to cache.
		for _, entry := range tcache.GetOverlaps(r.Start, r.End) {
			cv := entry.Value.(*cacheValue)
			key := entry.Key.(*cache.IntervalKey)
			sCmp := r.Start.Compare(key.Start)
			eCmp := r.End.Compare(key.End)
			// Some of the cases below adjust cv and key in-place (in a manner that
			// maintains the IntervalCache invariants). These in-place modifications
			// change the size of the entry. To capture all of these modifications we
			// compute the current size of the entry and then use the new size at the
			// end of this iteration to update TimestampCache.bytes.
			oldSize := cacheEntrySize(key.Start, key.End, cv.txnID)
			if cv.timestamp.Less(timestamp) {
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
					*cv = cacheValue{timestamp: timestamp, txnID: txnID}
					tcache.MoveToEnd(entry)
					return
				case sCmp <= 0 && eCmp >= 0:
					// New contains or is equal to old; delete old.
					//
					// New: ------------      ------------      ------------
					// Old:   --------    or    ----------  or  ----------
					//
					// New: ------------      ------------      ------------
					// Old:
					tcache.DelEntry(entry)
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

					newKey := tcache.MakeKey(r.End, oldEnd)
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
			} else if timestamp.Less(cv.timestamp) {
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
			} else if (cv.txnID == nil && txnID == nil) ||
				(cv.txnID != nil && txnID != nil && *cv.txnID == *txnID) {
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
					tcache.DelEntry(entry)
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
					cv.txnID = nil
					tc.bytes += cacheEntrySize(key.Start, key.End, cv.txnID) - oldSize
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
					cv.txnID = nil
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
					cv.txnID = nil
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
					cv.txnID = nil

					newKey := tcache.MakeKey(r.Start, key.Start)
					newEntry := makeCacheEntry(newKey, cacheValue{timestamp: timestamp, txnID: txnID})
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
					txnID = nil
					oldEnd := key.End
					key.End = r.Start

					newKey := tcache.MakeKey(r.End, oldEnd)
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
					txnID = nil
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
					txnID = nil
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

					newKey := tcache.MakeKey(key.End, r.Start)
					newCV := cacheValue{timestamp: cv.timestamp, txnID: nil}
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

					newKey := tcache.MakeKey(r.End, key.Start)
					newCV := cacheValue{timestamp: cv.timestamp, txnID: nil}
					newEntry := makeCacheEntry(newKey, newCV)
					addEntryAfter(newEntry, entry)
				default:
					panic(fmt.Sprintf("no overlap between %v and %v", key.Range, r))
				}
			}
			tc.bytes += cacheEntrySize(key.Start, key.End, cv.txnID) - oldSize
		}
		addRange(r)
	}
}

// AddRequest adds the specified request to the cache in an unexpanded state.
func (tc *timestampCache) AddRequest(req cacheRequest) {
	if len(req.reads) == 0 && len(req.writes) == 0 && req.txn.Key == nil {
		// The request didn't contain any spans for the timestamp cache.
		return
	}

	if !tc.lowWater.Less(req.timestamp) {
		// Request too old to be added.
		return
	}

	tc.reqIDAlloc++
	req.uniqueID = tc.reqIDAlloc
	tc.requests.ReplaceOrInsert(&req)
	tc.reqSpans += req.numSpans()
	tc.bytes += req.size()

	// Bump the latest timestamp and evict any requests that are now too old.
	tc.latest.Forward(req.timestamp)
	edge := tc.latest
	edge.WallTime -= MinTSCacheWindow.Nanoseconds()

	// Evict requests as long as the number of cached spans (both in the requests
	// queue and the interval caches) is larger than the eviction threshold.
	for tc.bytes > tc.maxBytes {
		// TODO(peter): It might be more efficient to gather up the requests to
		// delete using BTree.AscendLessThan rather than calling Min
		// repeatedly. Maybe.
		minItem := tc.requests.Min()
		if minItem == nil {
			break
		}
		minReq := minItem.(*cacheRequest)
		if edge.Less(minReq.timestamp) {
			break
		}
		tc.lowWater = minReq.timestamp
		tc.requests.DeleteMin()
		if tc.reqSpans < minReq.numSpans() {
			panic(fmt.Sprintf("bad reqSpans: %d < %d", tc.reqSpans, minReq.numSpans()))
		}
		tc.reqSpans -= minReq.numSpans()
		minReqSize := minReq.size()
		if tc.bytes < minReqSize {
			panic(fmt.Sprintf("bad reqSize: %d < %d", tc.bytes, minReqSize))
		}
		tc.bytes -= minReqSize
	}
}

// ExpandRequests expands any request that is newer than the specified
// timestamp and which overlaps the specified span.
func (tc *timestampCache) ExpandRequests(timestamp hlc.Timestamp, span roachpb.RSpan) {
	// Find all of the requests that have a timestamp greater than or equal to
	// the specified timestamp. Note that we can't delete the requests during the
	// btree iteration.
	var reqs []*cacheRequest
	tc.tmpReq.timestamp = timestamp
	tc.requests.AscendGreaterOrEqual(&tc.tmpReq, func(i btree.Item) bool {
		cr := i.(*cacheRequest)
		if cr.span.Overlaps(span) {
			reqs = append(reqs, cr)
		}
		return true
	})

	// Expand the requests, inserting the spans into either the read or write
	// interval caches.
	for _, req := range reqs {
		tc.requests.Delete(req)
		if tc.reqSpans < req.numSpans() {
			panic(fmt.Sprintf("bad reqSpans: %d < %d", tc.reqSpans, req.numSpans()))
		}
		tc.reqSpans -= req.numSpans()
		reqSize := req.size()
		if tc.bytes < reqSize {
			panic(fmt.Sprintf("bad reqSize: %d < %d", tc.bytes, reqSize))
		}
		tc.bytes -= reqSize
		for _, sp := range req.reads {
			tc.add(sp.Key, sp.EndKey, req.timestamp, req.txnID, true /* readTSCache */)
		}
		for _, sp := range req.writes {
			tc.add(sp.Key, sp.EndKey, req.timestamp, req.txnID, false /* !readTSCache */)
		}
		if req.txn.Key != nil {
			// Make the transaction key from the request key. We're guaranteed
			// req.txnID != nil because we only hit this code path for
			// EndTransactionRequests.
			key := keys.TransactionKey(req.txn.Key, *req.txnID)
			// We set txnID=nil because we want hits for same txn ID.
			tc.add(key, nil, req.timestamp, nil, false /* !readTSCache */)
		}
	}
}

// GetMaxRead returns the maximum read timestamp which overlaps the
// interval spanning from start to end. If that timestamp belongs to a
// single transaction, that transaction's ID is returned. If no part
// of the specified range is overlapped by timestamps from different
// transactions in the cache, the low water timestamp is returned for
// the read timestamps. Also returns an "ok" bool, indicating whether
// an explicit match of the interval was found in the cache (as
// opposed to using the low-water mark).
func (tc *timestampCache) GetMaxRead(start, end roachpb.Key) (hlc.Timestamp, *uuid.UUID, bool) {
	return tc.getMax(start, end, true)
}

// GetMaxWrite returns the maximum write timestamp which overlaps the
// interval spanning from start to end. If that timestamp belongs to a
// single transaction, that transaction's ID is returned. If no part
// of the specified range is overlapped by timestamps from different
// transactions in the cache, the low water timestamp is returned for
// the write timestamps. Also returns an "ok" bool, indicating whether
// an explicit match of the interval was found in the cache (as
// opposed to using the low-water mark).
func (tc *timestampCache) GetMaxWrite(start, end roachpb.Key) (hlc.Timestamp, *uuid.UUID, bool) {
	return tc.getMax(start, end, false)
}

func (tc *timestampCache) getMax(
	start, end roachpb.Key, readTSCache bool,
) (hlc.Timestamp, *uuid.UUID, bool) {
	if len(end) == 0 {
		end = start.Next()
	}
	var ok bool
	maxTS := tc.lowWater
	var maxTxnID *uuid.UUID
	cache := tc.wCache
	if readTSCache {
		cache = tc.rCache
	}
	for _, o := range cache.GetOverlaps(start, end) {
		ce := o.Value.(*cacheValue)
		if maxTS.Less(ce.timestamp) {
			ok = true
			maxTS = ce.timestamp
			maxTxnID = ce.txnID
		} else if maxTS == ce.timestamp && maxTxnID != nil &&
			(ce.txnID == nil || *maxTxnID != *ce.txnID) {
			maxTxnID = nil
		}
	}
	if maxTxnID == lowWaterTxnIDMarker {
		ok = false
	}
	return maxTS, maxTxnID, ok
}

// shouldEvict returns true if the cache entry's timestamp is no
// longer within the MinTSCacheWindow.
func (tc *timestampCache) shouldEvict(size int, key, value interface{}) bool {
	if tc.bytes <= tc.maxBytes {
		return false
	}
	ce := value.(*cacheValue)
	// In case low water mark was set higher, evict any entries
	// which occurred before it.
	if ce.timestamp.Less(tc.lowWater) {
		return true
	}
	// Compute the edge of the cache window.
	edge := tc.latest
	edge.WallTime -= MinTSCacheWindow.Nanoseconds()
	// We evict and update the low water mark if the proposed evictee's
	// timestamp is <= than the edge of the window.
	if !edge.Less(ce.timestamp) {
		tc.lowWater = ce.timestamp
		return true
	}
	return false
}
