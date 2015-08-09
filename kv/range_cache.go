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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Matt Tracy (matt.r.tracy@gmail.com)

package kv

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/biogo/store/llrb"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/cache"
	"github.com/cockroachdb/cockroach/util/log"
)

// rangeCacheKey is the key type used to store and sort values in the
// RangeCache.
type rangeCacheKey proto.Key

func (a rangeCacheKey) String() string {
	return proto.Key(a).String()
}

// Compare implements the llrb.Comparable interface for rangeCacheKey, so that
// it can be used as a key for util.OrderedCache.
func (a rangeCacheKey) Compare(b llrb.Comparable) int {
	return bytes.Compare(a, b.(rangeCacheKey))
}

// rangeDescriptorDB is a type which can query range descriptors from an
// underlying datastore. This interface is used by rangeDescriptorCache to
// initially retrieve information which will be cached.
type rangeDescriptorDB interface {
	// getRangeDescriptors returns a sorted slice of RangeDescriptors for a set
	// of consecutive ranges, the first of which must contain the requested key.
	// The additional RangeDescriptors are returned with the intent of pre-
	// caching subsequent ranges which are likely to be requested soon by the
	// current workload.
	getRangeDescriptors(proto.Key, lookupOptions) ([]proto.RangeDescriptor, error)
}

// rangeDescriptorCache is used to retrieve range descriptors for
// arbitrary keys. Descriptors are initially queried from storage
// using a rangeDescriptorDB, but is cached for subsequent lookups.
type rangeDescriptorCache struct {
	// rangeDescriptorDB is used to retrieve range descriptors from the
	// database, which will be cached by this structure.
	db rangeDescriptorDB
	// rangeCache caches replica metadata for key ranges. The cache is
	// filled while servicing read and write requests to the key value
	// store.
	rangeCache *cache.OrderedCache
	// rangeCacheMu protects rangeCache for concurrent access
	rangeCacheMu sync.RWMutex
}

// newRangeDescriptorCache returns a new RangeDescriptorCache which
// uses the given rangeDescriptorDB as the underlying source of range
// descriptors.
func newRangeDescriptorCache(db rangeDescriptorDB, size int) *rangeDescriptorCache {
	return &rangeDescriptorCache{
		db: db,
		rangeCache: cache.NewOrderedCache(cache.Config{
			Policy: cache.CacheLRU,
			ShouldEvict: func(n int, k, v interface{}) bool {
				return n > size
			},
		}),
	}
}

func (rdc *rangeDescriptorCache) String() string {
	rdc.rangeCacheMu.RLock()
	defer rdc.rangeCacheMu.RUnlock()
	return rdc.stringLocked()
}

func (rdc *rangeDescriptorCache) stringLocked() string {
	var buf bytes.Buffer
	rdc.rangeCache.Do(func(k, v interface{}) {
		fmt.Fprintf(&buf, "key=%s desc=%+v\n", proto.Key(k.(rangeCacheKey)), v)
	})
	return buf.String()
}

// LookupRangeDescriptor attempts to locate a descriptor for the range
// containing the given Key. This is done by querying the two-level
// lookup table of range descriptors which cockroach maintains.
//
// This method first looks up the specified key in the first level of
// range metadata, which returns the location of the key within the
// second level of range metadata. This second level location is then
// queried to retrieve a descriptor for the range where the key's
// value resides. Range descriptors retrieved during each search are
// cached for subsequent lookups.
//
// This method returns the RangeDescriptor for the range containing
// the key's data, or an error if any occurred.
func (rdc *rangeDescriptorCache) LookupRangeDescriptor(key proto.Key,
	options lookupOptions) (*proto.RangeDescriptor, error) {
	if _, r := rdc.getCachedRangeDescriptor(key, options.useReverseScan); r != nil {
		return r, nil
	}

	if log.V(2) {
		log.Infof("lookup range descriptor: key=%s\n%s", key, rdc)
	} else if log.V(1) {
		log.Infof("lookup range descriptor: key=%s", key)
	}
	rs, err := rdc.db.getRangeDescriptors(key, options)
	if err != nil {
		return nil, err
	}
	// TODO(tamird): there is a race here; multiple readers may experience cache
	// misses and concurrently attempt to refresh the cache, duplicating work.
	// Locking over the getRangeDescriptors call is even worse though, because
	// that blocks the cache completely for the duration of a slow query to the
	// cluster.
	rdc.rangeCacheMu.Lock()
	for i := range rs {
		// Note: we append the end key of each range to meta[12] records
		// so that calls to rdc.rangeCache.Ceil() for a key will return
		// the correct range. Using the start key would require using
		// Floor() which is a possibility for our llrb-based OrderedCache
		// but not possible for RocksDB.

		// Before adding a new descriptor, make sure we clear out any
		// pre-existing, overlapping descriptor which might have been
		// re-inserted due to concurrent range lookups.
		rangeKey := keys.RangeMetaKey(rs[i].EndKey)
		if log.V(1) {
			log.Infof("adding descriptor: key=%s desc=%s", rangeKey, &rs[i])
		}
		rdc.clearOverlappingCachedRangeDescriptors(rs[i].EndKey, rangeKey, &rs[i])
		rdc.rangeCache.Add(rangeCacheKey(rangeKey), &rs[i])
	}
	if len(rs) == 0 {
		log.Fatalf("no range descriptors returned for %s", key)
	}
	rdc.rangeCacheMu.Unlock()
	return &rs[0], nil
}

// EvictCachedRangeDescriptor will evict any cached range descriptors
// for the given key. It is intended that this method be called from a
// consumer of rangeDescriptorCache if the returned range descriptor is
// discovered to be stale.
// seenDesc should always be passed in and is used as the basis of a
// compare-and-evict (as pointers); if it is nil, eviction is unconditional
// but a warning will be logged.
func (rdc *rangeDescriptorCache) EvictCachedRangeDescriptor(descKey proto.Key,
	seenDesc *proto.RangeDescriptor, isReverse bool) {
	if seenDesc == nil {
		log.Warningf("compare-and-evict for key %s with nil descriptor; clearing unconditionally", descKey)
	}

	rdc.rangeCacheMu.Lock()
	defer rdc.rangeCacheMu.Unlock()

	rngKey, cachedDesc := rdc.getCachedRangeDescriptorLocked(descKey, isReverse)
	// Note that we're doing a "compare-and-erase": If seenDesc is not nil,
	// we want to clean the cache only if it equals the cached range
	// descriptor as a pointer. If not, then likely some other caller
	// already evicted previously, and we can save work by not doing it
	// again (which would prompt another expensive lookup).
	if seenDesc != nil && seenDesc != cachedDesc {
		return
	}

	for !bytes.Equal(descKey, proto.KeyMin) {
		if log.V(2) {
			log.Infof("evict cached descriptor: key=%s desc=%s\n%s", descKey, cachedDesc, rdc.stringLocked())
		} else if log.V(1) {
			log.Infof("evict cached descriptor: key=%s desc=%s", descKey, cachedDesc)
		}
		rdc.rangeCache.Del(rngKey)

		// Retrieve the metadata range key for the next level of metadata, and
		// evict that key as well. This loop ends after the meta1 range, which
		// returns KeyMin as its metadata key.
		descKey = keys.RangeMetaKey(descKey)
		rngKey, cachedDesc = rdc.getCachedRangeDescriptorLocked(descKey, isReverse)
	}
}

// getCachedRangeDescriptor is a helper function to retrieve the descriptor of
// the range which contains the given key, if present in the cache. It
// acquires a read lock on rdc.rangeCacheMu before delegating to
// getCachedRangeDescriptorLocked.
func (rdc *rangeDescriptorCache) getCachedRangeDescriptor(key proto.Key, isReverse bool) (
	rangeCacheKey, *proto.RangeDescriptor) {
	rdc.rangeCacheMu.RLock()
	defer rdc.rangeCacheMu.RUnlock()
	return rdc.getCachedRangeDescriptorLocked(key, isReverse)
}

// getCachedRangeDescriptorLocked is a helper function to retrieve the
// descriptor of the range which contains the given key, if present in the
// cache. It is assumed that the caller holds a read lock on rdc.rangeCacheMu.
func (rdc *rangeDescriptorCache) getCachedRangeDescriptorLocked(key proto.Key, isReverse bool) (
	rangeCacheKey, *proto.RangeDescriptor) {
	// The cache is indexed using the end-key of the range, but the
	// end-key is non-inclusive.
	var metaKey proto.Key
	if !isReverse {
		// If it is not reverse scan, we access the cache using key.Next().
		metaKey = keys.RangeMetaKey(key.Next())
	} else {
		// Because reverse scan request is begining at end key(exclusive),so we
		// access the cache using key directly.
		metaKey = keys.RangeMetaKey(key)
	}

	k, v, ok := rdc.rangeCache.Ceil(rangeCacheKey(metaKey))
	if !ok {
		return nil, nil
	}
	metaEndKey := k.(rangeCacheKey)
	rd := v.(*proto.RangeDescriptor)

	// Check that key actually belongs to the range.
	if !rd.ContainsKey(keys.KeyAddress(key)) {
		// The key is the EndKey of the range in reverse scan, just return the range descriptor.
		if isReverse && key.Equal(rd.EndKey) {
			return metaEndKey, rd
		}
		return nil, nil
	}

	// The key is the StartKey of the range in reverse scan. We need to return the previous range
	// descriptor, but it is not in the cache yet.
	if isReverse && key.Equal(rd.StartKey) {
		return nil, nil
	}
	return metaEndKey, rd
}

// clearOverlappingCachedRangeDescriptors looks up and clears any
// cache entries which overlap the specified key or descriptor.
func (rdc *rangeDescriptorCache) clearOverlappingCachedRangeDescriptors(key, metaKey proto.Key, desc *proto.RangeDescriptor) {
	if desc.StartKey.Equal(desc.EndKey) { // True for some unittests.
		return
	}
	// Clear out any descriptors which subsume the key which we're going
	// to cache. For example, if an existing KeyMin->KeyMax descriptor
	// should be cleared out in favor of a KeyMin->"m" descriptor.
	k, v, ok := rdc.rangeCache.Ceil(rangeCacheKey(metaKey))
	if ok {
		descriptor := v.(*proto.RangeDescriptor)
		addrKey := keys.KeyAddress(key)
		if !addrKey.Less(descriptor.StartKey) && !descriptor.EndKey.Less(addrKey) {
			if log.V(1) {
				log.Infof("clearing overlapping descriptor: key=%s desc=%s", k, descriptor)
			}
			rdc.rangeCache.Del(k.(rangeCacheKey))
		}
	}
	// Also clear any descriptors which are subsumed by the one we're
	// going to cache. This could happen on a merge (and also happens
	// when there's a lot of concurrency). Iterate from the range meta key
	// after RangeMetaKey(desc.StartKey) to the range meta key for desc.EndKey.
	rdc.rangeCache.DoRange(func(k, v interface{}) {
		if log.V(1) {
			log.Infof("clearing subsumed descriptor: key=%s desc=%s", k, v.(*proto.RangeDescriptor))
		}
		rdc.rangeCache.Del(k.(rangeCacheKey))
	}, rangeCacheKey(keys.RangeMetaKey(desc.StartKey).Next()),
		rangeCacheKey(keys.RangeMetaKey(desc.EndKey)))
}
