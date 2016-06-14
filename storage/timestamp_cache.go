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

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/cache"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/interval"
	"github.com/cockroachdb/cockroach/util/uuid"
	"github.com/google/btree"
)

const (
	// MinTSCacheWindow specifies the minimum duration to hold entries in
	// the cache before allowing eviction. After this window expires,
	// transactions writing to this node with timestamps lagging by more
	// than minCacheWindow will necessarily have to advance their commit
	// timestamp.
	MinTSCacheWindow = 10 * time.Second

	defaultEvictionSizeThreshold = 512

	// Max entries in each btree node.
	// TODO(peter): Not yet tuned.
	btreeDegree = 64
)

// cacheRequest holds the timestamp cache data from a single batch request. The
// requests are stored in a btree keyed by the timestamp and are "expanded" to
// populate the read/write interval caches if a potential conflict is detected
// due to an earlier request (based on timestamp) arriving.
type cacheRequest struct {
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

	// evictionSizeThreshold allows old entries to stay in the TimestampCache
	// indefinitely as long as the number of intervals in the cache doesn't
	// exceed this value. Once the cache grows beyond it, intervals are
	// evicted according to the time window. This threshold is intended to
	// permit transactions to take longer than the eviction window duration
	// if the size of the cache is not a concern, such as when a user
	// is using an interactive SQL shell.
	evictionSizeThreshold int
}

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
		rCache:                cache.NewIntervalCache(cache.Config{Policy: cache.CacheFIFO}),
		wCache:                cache.NewIntervalCache(cache.Config{Policy: cache.CacheFIFO}),
		requests:              btree.New(btreeDegree),
		evictionSizeThreshold: defaultEvictionSizeThreshold,
	}
	tc.Clear(clock)
	tc.rCache.Config.ShouldEvict = tc.shouldEvict
	tc.wCache.Config.ShouldEvict = tc.shouldEvict
	return tc
}

// Clear clears the cache and resets the low water mark to the
// current time plus the maximum clock offset.
func (tc *timestampCache) Clear(clock *hlc.Clock) {
	tc.rCache.Clear()
	tc.wCache.Clear()
	tc.lowWater = clock.Now()
	// TODO(tschottdorf): It's dangerous to inject timestamps (which will make
	// it into the HLC) like that.
	tc.lowWater.WallTime += clock.MaxOffset().Nanoseconds()
	tc.latest = tc.lowWater
}

// len returns the total number of read and write intervals in the
// TimestampCache.
func (tc *timestampCache) len() int {
	return tc.rCache.Len() + tc.wCache.Len() + tc.reqSpans
}

// SetLowWater sets the cache's low water mark, which is the minimum
// value the cache will return from calls to GetMax().
func (tc *timestampCache) SetLowWater(lowWater hlc.Timestamp) {
	if tc.lowWater.Less(lowWater) {
		tc.lowWater = lowWater
	}
}

// add the specified timestamp to the cache as covering the range of
// keys from start to end. If end is nil, the range covers the start
// key only. txnID is nil for no transaction. readTSCache specifies
// whether the command adding this timestamp should update the read
// timestamp; false to update the write timestamp cache.
func (tc *timestampCache) add(
	start, end roachpb.Key,
	timestamp hlc.Timestamp,
	txnID *uuid.UUID,
	readTSCache bool,
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
			tcache.AddEntry(entry)
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
			if !timestamp.Less(cv.timestamp) {
				// The existing interval has a timestamp less than or equal to the new interval.
				// Compare interval ranges to determine how to modify existing interval.
				switch {
				case sCmp == 0 && eCmp == 0:
					// New and old are equal; replace old with new and avoid the need to insert new.
					//
					// New: ------------
					// Old: ------------
					//
					// New: ------------
					*cv = cacheValue{timestamp: timestamp, txnID: txnID}
					tcache.MoveToEnd(entry)
					return
				case sCmp <= 0 && eCmp >= 0:
					// New contains or is equal to old; delete old.
					//
					// New: ------------      ------------      ------------
					// Old:   --------    or    ----------  or  ----------
					//
					// Old:
					tcache.DelEntry(entry)
				case sCmp > 0 && eCmp < 0:
					// Old contains new; split up old into two.
					//
					// New:     ----
					// Old: ------------
					//
					// Old: ----    ----
					oldEnd := key.End
					key.End = r.Start

					key := tcache.MakeKey(r.End, oldEnd)
					newEntry := makeCacheEntry(key, *cv)
					tcache.AddEntryAfter(newEntry, entry)
				case eCmp >= 0:
					// Left partial overlap; truncate old end.
					//
					// New:     --------          --------
					// Old: --------      or  ------------
					//
					// Old: ----              ----
					key.End = r.Start
				case sCmp <= 0:
					// Right partial overlap; truncate old start.
					//
					// New: --------          --------
					// Old:     --------  or  ------------
					//
					// Old:         ----              ----
					key.Start = r.End
				default:
					panic(fmt.Sprintf("no overlap between %v and %v", key.Range, r))
				}
			} else {
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
					// New:         ----              ----
					r.Start = key.End
				case sCmp < 0:
					// Right partial overlap; truncate new end.
					//
					// Old:     --------          --------
					// New: --------      or  ------------
					//
					// New: ----              ----
					r.End = key.Start
				default:
					panic(fmt.Sprintf("no overlap between %v and %v", key.Range, r))
				}
			}
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

	// Bump the latest timestamp and evict any requests that are now too old.
	tc.latest.Forward(req.timestamp)
	edge := tc.latest
	edge.WallTime -= MinTSCacheWindow.Nanoseconds()

	// Evict requests as long as the number of cached spans (both in the requests
	// queue and the interval caches) is larger than the eviction threshold.
	for tc.len() > tc.evictionSizeThreshold {
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
	}
}

// ExpandRequests expands any request that is newer than the specified
// timestamp.
func (tc *timestampCache) ExpandRequests(timestamp hlc.Timestamp) {
	// Find all of the requests that have a timestamp greater than or equal to
	// the specified timestamp. Note that we can't delete the requests during the
	// btree iteration.
	var reqs []*cacheRequest
	tc.tmpReq.timestamp = timestamp
	tc.requests.AscendGreaterOrEqual(&tc.tmpReq, func(i btree.Item) bool {
		// TODO(peter): We could be more intelligent about not expanding a request
		// if there is no possibility of overlap. For example, in workloads where
		// there are concurrent bulk inserts for completely distinct ranges.
		reqs = append(reqs, i.(*cacheRequest))
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
		for _, sp := range req.reads {
			tc.add(sp.Key, sp.EndKey, req.timestamp, req.txnID, true /* readTSCache */)
		}
		for _, sp := range req.writes {
			tc.add(sp.Key, sp.EndKey, req.timestamp, req.txnID, false /* !readTSCache */)
		}
		if req.txn.Key != nil {
			// We set txnID=nil because we want hits for same txn ID.
			tc.add(req.txn.Key, req.txn.EndKey, req.timestamp, nil, false /* !readTSCache */)
		}
	}
}

// GetMaxRead returns the maximum read timestamp which overlaps the
// interval spanning from start to end. Cached timestamps matching the
// specified txnID are not considered. If no part of the specified
// range is overlapped by timestamps from different transactions in
// the cache, the low water timestamp is returned for the read
// timestamps. Also returns an "ok" bool, indicating whether an
// explicit match of the interval was found in the cache.
func (tc *timestampCache) GetMaxRead(start, end roachpb.Key, txnID *uuid.UUID) (hlc.Timestamp, bool) {
	return tc.getMax(start, end, txnID, true)
}

// GetMaxWrite returns the maximum write timestamp which overlaps the
// interval spanning from start to end. Cached timestamps matching the
// specified txnID are not considered. If no part of the specified
// range is overlapped by timestamps from different transactions in
// the cache, the low water timestamp is returned for the write
// timestamps. Also returns an "ok" bool, indicating whether an
// explicit match of the interval was found in the cache.
//
// The txn ID prevents restarts with a pattern like: read("a"),
// write("a"). The read adds a timestamp for "a". Then the write (for
// the same transaction) would get that as the max timestamp and be
// forced to increment it. This allows timestamps from the same txn
// to be ignored because the write would instead get the low water
// timestamp.
func (tc *timestampCache) GetMaxWrite(start, end roachpb.Key, txnID *uuid.UUID) (hlc.Timestamp, bool) {
	return tc.getMax(start, end, txnID, false)
}

func (tc *timestampCache) getMax(start, end roachpb.Key, txnID *uuid.UUID, readTSCache bool) (hlc.Timestamp, bool) {
	if len(end) == 0 {
		end = start.Next()
	}
	var ok bool
	max := tc.lowWater
	cache := tc.wCache
	if readTSCache {
		cache = tc.rCache
	}
	for _, o := range cache.GetOverlaps(start, end) {
		ce := o.Value.(*cacheValue)
		if ce.txnID == nil || txnID == nil || !roachpb.TxnIDEqual(txnID, ce.txnID) {
			if max.Less(ce.timestamp) {
				ok = true
				max = ce.timestamp
			}
		}
	}
	return max, ok
}

// MergeInto merges all entries from this timestamp cache into the
// dest timestamp cache. The clear parameter, if true, copies the
// values of lowWater and latest and clears the destination cache
// before merging in the source.
func (tc *timestampCache) MergeInto(dest *timestampCache, clear bool) {
	if clear {
		dest.rCache.Clear()
		dest.wCache.Clear()
		dest.lowWater = tc.lowWater
		dest.latest = tc.latest
		dest.requests = btree.New(btreeDegree)
		dest.reqIDAlloc = 0

		// Because we just cleared the destination cache, we can directly
		// insert entries from this cache.
		hardMerge := func(srcCache, destCache *cache.IntervalCache) {
			srcCache.Do(func(k, v interface{}) {
				// Cache entries are mutable (see Add), so we give each cache its own
				// unique copy.
				entry := makeCacheEntry(*k.(*cache.IntervalKey), *v.(*cacheValue))
				destCache.AddEntry(entry)
			})
		}
		hardMerge(tc.rCache, dest.rCache)
		hardMerge(tc.wCache, dest.wCache)
	} else {
		dest.lowWater.Forward(tc.lowWater)
		dest.latest.Forward(tc.latest)

		// The cache was not cleared before, so we can't just insert entries because
		// intervals may need to be adjusted or removed to maintain the non-overlapping
		// guarantee.
		softMerge := func(srcCache *cache.IntervalCache, readTSCache bool) {
			srcCache.Do(func(k, v interface{}) {
				key, val := *k.(*cache.IntervalKey), *v.(*cacheValue)
				dest.add(roachpb.Key(key.Start), roachpb.Key(key.End), val.timestamp, val.txnID, readTSCache)
			})
		}
		softMerge(tc.rCache, true)
		softMerge(tc.wCache, false)
	}

	// Copy the requests.
	tc.requests.Ascend(func(i btree.Item) bool {
		req := *(i.(*cacheRequest))
		dest.reqIDAlloc++
		req.uniqueID = dest.reqIDAlloc
		dest.requests.ReplaceOrInsert(&req)
		dest.reqSpans += req.numSpans()
		return true
	})
}

// shouldEvict returns true if the cache entry's timestamp is no
// longer within the MinTSCacheWindow.
func (tc *timestampCache) shouldEvict(size int, key, value interface{}) bool {
	if tc.len() <= tc.evictionSizeThreshold {
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
