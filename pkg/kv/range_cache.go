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
// Author: Matt Tracy (matt.r.tracy@gmail.com)

package kv

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/biogo/store/llrb"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// rangeCacheKey is the key type used to store and sort values in the
// RangeCache.
type rangeCacheKey roachpb.RKey

func (a rangeCacheKey) String() string {
	return roachpb.Key(a).String()
}

// Compare implements the llrb.Comparable interface for rangeCacheKey, so that
// it can be used as a key for util.OrderedCache.
func (a rangeCacheKey) Compare(b llrb.Comparable) int {
	return bytes.Compare(a, b.(rangeCacheKey))
}

func meta(k roachpb.RKey) (roachpb.RKey, error) {
	return keys.Addr(keys.RangeMetaKey(k))
}

// RangeDescriptorDB is a type which can query range descriptors from an
// underlying datastore. This interface is used by rangeDescriptorCache to
// initially retrieve information which will be cached.
type RangeDescriptorDB interface {
	// RangeLookup takes a meta key to look up descriptors for and a descriptor
	// of the range believed to hold the meta key. Two slices of range
	// descriptors are returned. The first of these slices holds descriptors
	// which contain the given key (possibly from intents), and the second holds
	// prefetched adjacent descriptors.
	RangeLookup(
		ctx context.Context,
		key roachpb.RKey,
		desc *roachpb.RangeDescriptor,
		useReverseScan bool,
	) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, *roachpb.Error)
	// FirstRange returns the descriptor for the first Range. This is the
	// Range containing all \x00\x00meta1 entries.
	FirstRange() (*roachpb.RangeDescriptor, error)
}

// rangeDescriptorCache is used to retrieve range descriptors for
// arbitrary keys. Descriptors are initially queried from storage
// using a RangeDescriptorDB, but is cached for subsequent lookups.
type rangeDescriptorCache struct {
	ctx context.Context
	// RangeDescriptorDB is used to retrieve range descriptors from the
	// database, which will be cached by this structure.
	db RangeDescriptorDB
	// rangeCache caches replica metadata for key ranges. The cache is
	// filled while servicing read and write requests to the key value
	// store.
	rangeCache struct {
		syncutil.RWMutex
		cache *cache.OrderedCache
	}
	// lookupRequests stores all inflight requests retrieving range
	// descriptors from the database. It allows multiple RangeDescriptorDB
	// lookup requests for the same inferred range descriptor to be
	// multiplexed onto the same database lookup. See makeLookupRequestKey
	// for details on this inference.
	lookupRequests struct {
		syncutil.Mutex
		inflight map[lookupRequestKey]lookupRequest
	}
}

type lookupRequest struct {
	// observers stores the cache lookups that have joined onto the existing
	// lookupRequest and wish to be notified of its database lookup's results
	// instead of performing the database lookup themselves.
	observers []chan<- lookupResult
}

type lookupResult struct {
	desc       *roachpb.RangeDescriptor
	evictToken *EvictionToken
	err        error
}

type lookupRequestKey struct {
	key            string
	useReverseScan bool
}

// makeLookupRequestKey constructs a lookupRequestKey with the goal of
// mapping all requests which are inferred to be looking for the same
// descriptor onto the same request key to establish request coalescing.
//
// If the prevDesc is not nil and we had a cache miss, there are three possible
// events that may have happened. For each of these, we try to coalesce all
// requests that will end up on the same range post-event together.
// - Split:  for a split, only the right half of the split will attempt to evict
//           the stale descriptor because only the right half will be sending to
//           the wrong range. Once this stale descriptor is evicted, keys from
//           both halves of the split will miss the cache. Because both sides of
//           the split will now map to the same lookupResult, it is important to
//           use EvictAndReplace if possible to insert one of the two new descriptors.
//           This way, no requests to that descriptor will ever miss the cache and
//           risk being coalesced into the other request. If this is not possible,
//           the lookup will still work, but it will require multiple lookups, which
//           will be launched in series when requests find that their desired key
//           is outside of the returned descriptor.
// - Merges: for a merge, the left half of the merge will never notice. The right
//           half of the merge will suddenly find its descriptor to be stale, so
//           it will evict and lookup the new descriptor. We set the key to hash
//           to the start of the stale descriptor for lookup requests to the right
//           half of the merge so that all requests will be coalesced to the same
//           lookupRequest.
// - Rebal:  for a rebalance, the entire descriptor will suddenly go stale and
//           requests to it will evict the descriptor. We set the key to hash to
//           the start of the stale descriptor for lookup requests to the rebalanced
//           descriptor so that all requests will be coalesced to the same lookupRequest.
//
// Note that the above description assumes that useReverseScan is false for simplicity.
// If useReverseScan is true, we need to use the end key of the stale descriptor instead.
func makeLookupRequestKey(
	key roachpb.RKey, evictToken *EvictionToken, useReverseScan bool,
) lookupRequestKey {
	if evictToken != nil {
		if useReverseScan {
			key = evictToken.prevDesc.EndKey
		} else {
			key = evictToken.prevDesc.StartKey
		}
	}
	return lookupRequestKey{
		key:            string(key),
		useReverseScan: useReverseScan,
	}
}

// newRangeDescriptorCache returns a new RangeDescriptorCache which
// uses the given RangeDescriptorDB as the underlying source of range
// descriptors.
func newRangeDescriptorCache(
	ctx context.Context, db RangeDescriptorDB, size int,
) *rangeDescriptorCache {
	rdc := &rangeDescriptorCache{ctx: ctx, db: db}
	rdc.rangeCache.cache = cache.NewOrderedCache(cache.Config{
		Policy: cache.CacheLRU,
		ShouldEvict: func(n int, _, _ interface{}) bool {
			return n > size
		},
	})
	rdc.lookupRequests.inflight = make(map[lookupRequestKey]lookupRequest)
	return rdc
}

func (rdc *rangeDescriptorCache) String() string {
	rdc.rangeCache.RLock()
	defer rdc.rangeCache.RUnlock()
	return rdc.stringLocked()
}

func (rdc *rangeDescriptorCache) stringLocked() string {
	var buf bytes.Buffer
	rdc.rangeCache.cache.Do(func(k, v interface{}) {
		fmt.Fprintf(&buf, "key=%s desc=%+v\n", roachpb.Key(k.(rangeCacheKey)), v)
	})
	return buf.String()
}

// EvictionToken holds eviction state between calls to LookupRangeDescriptor.
type EvictionToken struct {
	prevDesc *roachpb.RangeDescriptor

	doOnce    sync.Once                                 // assures that do and doReplace are run up to once.
	doLocker  sync.Locker                               // protects do and doReplace.
	do        func() error                              // called on eviction.
	doReplace func(rs ...roachpb.RangeDescriptor) error // called after eviction on EvictAndReplace.
}

func (rdc *rangeDescriptorCache) makeEvictionToken(
	prevDesc *roachpb.RangeDescriptor, evict func() error,
) *EvictionToken {
	return &EvictionToken{
		prevDesc:  prevDesc,
		do:        evict,
		doReplace: rdc.insertRangeDescriptorsLocked,
		doLocker:  &rdc.rangeCache,
	}
}

// Evict instructs the EvictionToken to evict the RangeDescriptor it was created
// with from the rangeDescriptorCache.
func (et *EvictionToken) Evict(ctx context.Context) error {
	return et.EvictAndReplace(ctx)
}

// EvictAndReplace instructs the EvictionToken to evict the RangeDescriptor it was
// created with from the rangeDescriptorCache. It also allows the user to provide
// new RangeDescriptors to insert into the cache, all atomically. When called without
// arguments, EvictAndReplace will behave the same as Evict.
func (et *EvictionToken) EvictAndReplace(
	ctx context.Context, newDescs ...roachpb.RangeDescriptor,
) error {
	var err error
	et.doOnce.Do(func() {
		et.doLocker.Lock()
		defer et.doLocker.Unlock()
		err = et.do()
		if err == nil {
			if len(newDescs) > 0 {
				err = et.doReplace(newDescs...)
				log.Eventf(ctx, "evicting cached range descriptor with %d replacements", len(newDescs))
			} else {
				log.Event(ctx, "evicting cached range descriptor")
			}
		}
	})
	return err
}

// LookupRangeDescriptor attempts to locate a descriptor for the range
// containing the given Key. This is done by querying the two-level
// lookup table of range descriptors which cockroach maintains. The
// function should be provided with an EvictionToken if one was
// acquired from this function on a previous lookup. If not, an
// empty EvictionToken can be provided.
//
// This method first looks up the specified key in the first level of
// range metadata, which returns the location of the key within the
// second level of range metadata. This second level location is then
// queried to retrieve a descriptor for the range where the key's
// value resides. Range descriptors retrieved during each search are
// cached for subsequent lookups.
//
// This method returns the RangeDescriptor for the range containing
// the key's data and a token to manage evicting the RangeDescriptor
// if it is found to be stale, or an error if any occurred.
func (rdc *rangeDescriptorCache) LookupRangeDescriptor(
	ctx context.Context, key roachpb.RKey, evictToken *EvictionToken, useReverseScan bool,
) (*roachpb.RangeDescriptor, *EvictionToken, error) {
	return rdc.lookupRangeDescriptorInternal(ctx, key, evictToken, useReverseScan, nil)
}

// lookupRangeDescriptorInternal is called from LookupRangeDescriptor or from tests.
//
// If a WaitGroup is supplied, it is signaled when the request is
// added to the inflight request map (with or without merging) or the
// function finishes. Used for testing.
func (rdc *rangeDescriptorCache) lookupRangeDescriptorInternal(
	ctx context.Context,
	key roachpb.RKey,
	evictToken *EvictionToken,
	useReverseScan bool,
	wg *sync.WaitGroup,
) (*roachpb.RangeDescriptor, *EvictionToken, error) {
	rdc.rangeCache.RLock()
	doneWg := func() {
		if wg != nil {
			wg.Done()
		}
		wg = nil
	}
	defer doneWg()

	if _, desc, err := rdc.getCachedRangeDescriptorLocked(key, useReverseScan); err != nil {
		rdc.rangeCache.RUnlock()
		return nil, nil, err
	} else if desc != nil {
		rdc.rangeCache.RUnlock()
		returnToken := rdc.makeEvictionToken(desc, func() error {
			return rdc.evictCachedRangeDescriptorLocked(key, desc, useReverseScan)
		})
		log.Event(ctx, "looked up range descriptor from cache")
		return desc, returnToken, nil
	}

	if log.V(3) {
		log.Infof(ctx, "lookup range descriptor: key=%s\n%s", key, rdc.stringLocked())
	} else if log.V(2) {
		log.Infof(ctx, "lookup range descriptor: key=%s", key)
	}

	var res lookupResult
	requestKey := makeLookupRequestKey(key, evictToken, useReverseScan)
	rdc.lookupRequests.Lock()
	if req, inflight := rdc.lookupRequests.inflight[requestKey]; inflight {
		resC := make(chan lookupResult, 1)
		req.observers = append(req.observers, resC)
		rdc.lookupRequests.inflight[requestKey] = req
		rdc.lookupRequests.Unlock()
		rdc.rangeCache.RUnlock()
		doneWg()

		res = <-resC
		log.Event(ctx, "looked up range descriptor with shared request")
	} else {
		rdc.lookupRequests.inflight[requestKey] = req
		rdc.lookupRequests.Unlock()
		rdc.rangeCache.RUnlock()
		doneWg()

		rs, preRs, err := rdc.performRangeLookup(ctx, key, useReverseScan)
		if err != nil {
			res = lookupResult{err: err}
		} else {
			switch len(rs) {
			case 0:
				res = lookupResult{err: fmt.Errorf("no range descriptors returned for %s", key)}
			case 1:
				desc := &rs[0]
				res = lookupResult{
					desc: desc,
					evictToken: rdc.makeEvictionToken(desc, func() error {
						return rdc.evictCachedRangeDescriptorLocked(key, desc, useReverseScan)
					}),
				}
			case 2:
				desc := &rs[0]
				nextDesc := rs[1]
				res = lookupResult{
					desc: desc,
					evictToken: rdc.makeEvictionToken(desc, func() error {
						return rdc.insertRangeDescriptorsLocked(nextDesc)
					}),
				}
			default:
				panic(fmt.Sprintf("more than 2 matching range descriptors returned for %s: %v", key, rs))
			}
		}

		// We want to be assured that all goroutines which experienced a cache miss
		// have joined our in-flight request, and all others will experience a
		// cache hit. This requires atomicity across cache population and
		// notification, hence this exclusive lock.
		rdc.rangeCache.Lock()
		if res.err == nil {
			// These need to be separate because we need to preserve the pointer to rs[0]
			// so that the seenDesc logic works correctly in EvictCachedRangeDescriptor. An
			// append could cause a copy, which would change the address of rs[0]. We insert
			// the prefetched descriptors first to avoid any unintended overwriting.
			if err := rdc.insertRangeDescriptorsLocked(preRs...); err != nil {
				log.Warningf(ctx, "range cache inserting prefetched descriptors failed: %v", err)
			}
			if err := rdc.insertRangeDescriptorsLocked(rs...); err != nil {
				res = lookupResult{err: err}
			}
		}

		// rdc.lookupRequests does not need to be locked here because we hold an exclusive
		// write lock on rdc.rangeCache. However, we do anyway for clarity and future proofing.
		rdc.lookupRequests.Lock()
		for _, observer := range rdc.lookupRequests.inflight[requestKey].observers {
			observer <- res
		}
		delete(rdc.lookupRequests.inflight, requestKey)
		rdc.lookupRequests.Unlock()
		rdc.rangeCache.Unlock()
		log.Event(ctx, "looked up range descriptor")
	}

	// It rarely may be possible that we got grouped in with the wrong
	// RangeLookup (eg. from a double split), so if we did, return an error with
	// an unmodified eviction token.
	if desc := res.desc; desc != nil {
		containsFn := (*roachpb.RangeDescriptor).ContainsKey
		if useReverseScan {
			containsFn = (*roachpb.RangeDescriptor).ContainsExclusiveEndKey
		}
		if !containsFn(desc, key) {
			return nil, evictToken, errors.Errorf("key %q not contained in range lookup's resulting descriptor %v", key, desc)
		}
	}
	return res.desc, res.evictToken, res.err
}

// performRangeLookup handles delegating the range lookup to the cache's
// RangeDescriptorDB.
func (rdc *rangeDescriptorCache) performRangeLookup(
	ctx context.Context, key roachpb.RKey, useReverseScan bool,
) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error) {
	// metadataKey is sent to RangeLookup to find the RangeDescriptor
	// which contains key.
	metadataKey, err := meta(key)
	if err != nil {
		return nil, nil, err
	}

	// desc is the RangeDescriptor for the range which contains metadataKey.
	var desc *roachpb.RangeDescriptor
	switch {
	case bytes.Equal(metadataKey, roachpb.RKeyMin):
		// In this case, the requested key is stored in the cluster's first
		// range. Return the first range, which is always gossiped and not
		// queried from the datastore.
		var err error
		if desc, err = rdc.db.FirstRange(); err != nil {
			return nil, nil, err
		}
		return []roachpb.RangeDescriptor{*desc}, nil, nil
	case bytes.HasPrefix(metadataKey, keys.Meta1Prefix):
		// In this case, desc is the cluster's first range.
		var err error
		if desc, err = rdc.db.FirstRange(); err != nil {
			return nil, nil, err
		}
	default:
		// Look up desc from the cache, which will recursively call into
		// this function if it is not cached.
		var err error
		if desc, _, err = rdc.LookupRangeDescriptor(
			ctx, metadataKey, nil, useReverseScan,
		); err != nil {
			return nil, nil, err
		}
	}
	// Tag inner operations.
	ctx = log.WithLogTag(ctx, "range-lookup", nil)
	descs, prefetched, pErr := rdc.db.RangeLookup(ctx, metadataKey, desc, useReverseScan)
	return descs, prefetched, pErr.GoError()
}

// EvictCachedRangeDescriptor will evict any cached range descriptors
// for the given key. It is intended that this method be called from a
// consumer of rangeDescriptorCache if the returned range descriptor is
// discovered to be stale.
// seenDesc should always be passed in and is used as the basis of a
// compare-and-evict (as pointers); if it is nil, eviction is unconditional
// but a warning will be logged.
func (rdc *rangeDescriptorCache) EvictCachedRangeDescriptor(
	descKey roachpb.RKey, seenDesc *roachpb.RangeDescriptor, inclusive bool,
) error {
	rdc.rangeCache.Lock()
	defer rdc.rangeCache.Unlock()
	return rdc.evictCachedRangeDescriptorLocked(descKey, seenDesc, inclusive)
}

func (rdc *rangeDescriptorCache) evictCachedRangeDescriptorLocked(
	descKey roachpb.RKey, seenDesc *roachpb.RangeDescriptor, inclusive bool,
) error {
	rngKey, cachedDesc, err := rdc.getCachedRangeDescriptorLocked(descKey, inclusive)
	if err != nil {
		return err
	}
	// Note that we're doing a "compare-and-erase": If seenDesc is not nil,
	// we want to clean the cache only if it equals the cached range
	// descriptor as a pointer. If not, then likely some other caller
	// already evicted previously, and we can save work by not doing it
	// again (which would prompt another expensive lookup).
	if seenDesc != nil && seenDesc != cachedDesc {
		return nil
	}

	for {
		if log.V(3) {
			log.Infof(rdc.ctx, "evict cached descriptor: key=%s desc=%s\n%s",
				descKey, cachedDesc, rdc.stringLocked())
		} else if log.V(2) {
			log.Infof(rdc.ctx, "evict cached descriptor: key=%s desc=%s", descKey, cachedDesc)
		}
		rdc.rangeCache.cache.Del(rngKey)

		// Retrieve the metadata range key for the next level of metadata, and
		// evict that key as well. This loop ends after the meta1 range, which
		// returns KeyMin as its metadata key.
		descKey, err = meta(descKey)
		if err != nil {
			return err
		}
		rngKey, cachedDesc, err = rdc.getCachedRangeDescriptorLocked(descKey, inclusive)
		if err != nil {
			return err
		}
		// TODO(tschottdorf): write a test that verifies that the first descriptor
		// can also be evicted. This is necessary since the initial range
		// [KeyMin,KeyMax) may turn into [KeyMin, "something"), after which
		// larger ranges don't fit into it any more.
		if bytes.Equal(descKey, roachpb.RKeyMin) {
			break
		}
	}
	return nil
}

// getCachedRangeDescriptor is a helper function to retrieve the descriptor of
// the range which contains the given key, if present in the cache. It
// acquires a read lock on rdc.rangeCache before delegating to
// getCachedRangeDescriptorLocked.
// `inclusive` determines the behaviour at the range boundary: If set to true
// and `key` is the EndKey and StartKey of two adjacent ranges, the first range
// is returned instead of the second (which technically contains the given key).
func (rdc *rangeDescriptorCache) getCachedRangeDescriptor(
	key roachpb.RKey, inclusive bool,
) (rangeCacheKey, *roachpb.RangeDescriptor, error) {
	rdc.rangeCache.RLock()
	defer rdc.rangeCache.RUnlock()
	return rdc.getCachedRangeDescriptorLocked(key, inclusive)
}

// getCachedRangeDescriptorLocked is a helper function to retrieve the
// descriptor of the range which contains the given key, if present in the
// cache. It is assumed that the caller holds a read lock on rdc.rangeCache.
func (rdc *rangeDescriptorCache) getCachedRangeDescriptorLocked(
	key roachpb.RKey, inclusive bool,
) (rangeCacheKey, *roachpb.RangeDescriptor, error) {
	// The cache is indexed using the end-key of the range, but the
	// end-key is non-inclusive by default.
	var metaKey roachpb.RKey
	var err error
	if !inclusive {
		metaKey, err = meta(key.Next())
	} else {
		metaKey, err = meta(key)
	}
	if err != nil {
		return nil, nil, err
	}

	k, v, ok := rdc.rangeCache.cache.Ceil(rangeCacheKey(metaKey))
	if !ok {
		return nil, nil, nil
	}
	metaEndKey := k.(rangeCacheKey)
	rd := v.(*roachpb.RangeDescriptor)

	containsFn := (*roachpb.RangeDescriptor).ContainsKey
	if inclusive {
		containsFn = (*roachpb.RangeDescriptor).ContainsExclusiveEndKey
	}

	// Return nil if the key does not belong to the range.
	if !containsFn(rd, key) {
		return nil, nil, nil
	}
	return metaEndKey, rd, nil
}

// insertRangeDescriptorsLocked is a helper function to insert the provided
// range descriptors into the rangeDescriptorCache. It is assumed that the
// caller holds a write lock on rdc.rangeCache.
func (rdc *rangeDescriptorCache) insertRangeDescriptorsLocked(rs ...roachpb.RangeDescriptor) error {
	for i := range rs {
		// Note: we append the end key of each range to meta records
		// so that calls to rdc.rangeCache.cache.Ceil() for a key will return
		// the correct range.

		// Before adding a new descriptor, make sure we clear out any
		// pre-existing, overlapping descriptor which might have been
		// re-inserted due to concurrent range lookups.
		if err := rdc.clearOverlappingCachedRangeDescriptors(&rs[i]); err != nil {
			return err
		}
		rangeKey, err := meta(rs[i].EndKey)
		if err != nil {
			return err
		}
		if log.V(2) {
			log.Infof(rdc.ctx, "adding descriptor: key=%s desc=%s", rangeKey, &rs[i])
		}
		rdc.rangeCache.cache.Add(rangeCacheKey(rangeKey), &rs[i])
	}
	return nil
}

// clearOverlappingCachedRangeDescriptors looks up and clears any
// cache entries which overlap the specified descriptor.
func (rdc *rangeDescriptorCache) clearOverlappingCachedRangeDescriptors(
	desc *roachpb.RangeDescriptor,
) error {
	key := desc.EndKey
	metaKey, err := meta(key)
	if err != nil {
		return err
	}

	// Clear out any descriptors which subsume the key which we're going
	// to cache. For example, if an existing KeyMin->KeyMax descriptor
	// should be cleared out in favor of a KeyMin->"m" descriptor.
	k, v, ok := rdc.rangeCache.cache.Ceil(rangeCacheKey(metaKey))
	if ok {
		descriptor := v.(*roachpb.RangeDescriptor)
		if descriptor.StartKey.Less(key) && !descriptor.EndKey.Less(key) {
			if log.V(2) {
				log.Infof(rdc.ctx, "clearing overlapping descriptor: key=%s desc=%s", k, descriptor)
			}
			rdc.rangeCache.cache.Del(k.(rangeCacheKey))
		}
	}

	startMeta, err := meta(desc.StartKey)
	if err != nil {
		return err
	}
	endMeta, err := meta(desc.EndKey)
	if err != nil {
		return err
	}

	// Also clear any descriptors which are subsumed by the one we're
	// going to cache. This could happen on a merge (and also happens
	// when there's a lot of concurrency). Iterate from the range meta key
	// after RangeMetaKey(desc.StartKey) to the range meta key for desc.EndKey.
	var keys []rangeCacheKey
	rdc.rangeCache.cache.DoRange(func(k, v interface{}) bool {
		if log.V(2) {
			log.Infof(rdc.ctx, "clearing subsumed descriptor: key=%s desc=%s",
				k, v.(*roachpb.RangeDescriptor))
		}
		keys = append(keys, k.(rangeCacheKey))

		return false
	}, rangeCacheKey(startMeta.Next()), rangeCacheKey(endMeta))

	for _, key := range keys {
		rdc.rangeCache.cache.Del(key)
	}
	return nil
}
