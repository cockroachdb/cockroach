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
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/cache"
	"github.com/cockroachdb/cockroach/util/log"
)

// rangeCacheKey is the key type used to store and sort values in the
// RangeCache.
type rangeCacheKey proto.Key

// Compare implements the llrb.Comparable interface for rangeCacheKey, so that
// it can be used as a key for util.OrderedCache.
func (a rangeCacheKey) Compare(b llrb.Comparable) int {
	return bytes.Compare(a, b.(rangeCacheKey))
}

// rangeDescriptorDB is a type which can query range descriptors from an
// underlying datastore. This interface is used by rangeDescriptorCache to
// initially retrieve information which will be cached.
type rangeDescriptorDB interface {
	// getRangeDescriptor retrieves a descriptor for the range
	// containing the given key from storage. This function returns a
	// sorted slice of RangeDescriptors for a set of consecutive ranges,
	// the first which must contain the requested key. The additional
	// RangeDescriptors are returned with the intent of pre-caching
	// subsequent ranges which are likely to be requested soon by the
	// current workload.
	getRangeDescriptor(proto.Key, lookupOptions) ([]proto.RangeDescriptor, error)
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

func (rmc *rangeDescriptorCache) String() string {
	var buf bytes.Buffer
	rmc.rangeCacheMu.Lock()
	rmc.rangeCache.Do(func(k, v interface{}) {
		fmt.Fprintf(&buf, "key=%s desc=%+v\n", proto.Key(k.(rangeCacheKey)), v)
	})
	rmc.rangeCacheMu.Unlock()
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
func (rmc *rangeDescriptorCache) LookupRangeDescriptor(key proto.Key,
	options lookupOptions) (*proto.RangeDescriptor, error) {
	_, r := rmc.getCachedRangeDescriptor(key)
	if log.V(1) {
		log.Infof("lookup range descriptor: key=%s desc=%+v\n%s", key, r, rmc)
	}
	if r != nil {
		return r, nil
	}

	rs, err := rmc.db.getRangeDescriptor(key, options)
	if err != nil {
		return nil, err
	}
	rmc.rangeCacheMu.Lock()
	for i := range rs {
		// Note: we append the end key of each range to meta[12] records
		// so that calls to rmc.rangeCache.Ceil() for a key will return
		// the correct range. Using the start key would require using
		// Floor() which is a possibility for our llrb-based OrderedCache
		// but not possible for RocksDB.
		rmc.rangeCache.Add(rangeCacheKey(engine.RangeMetaKey(rs[i].EndKey)), &rs[i])
	}
	if len(rs) == 0 {
		log.Fatalf("no range descriptors returned for %s", key)
	}
	rmc.rangeCacheMu.Unlock()
	return &rs[0], nil
}

// EvictCachedRangeDescriptor will evict any cached range descriptors
// for the given key. It is intended that this method be called from a
// consumer of rangeDescriptorCache if the returned range descriptor is
// discovered to be stale.
func (rmc *rangeDescriptorCache) EvictCachedRangeDescriptor(key proto.Key) {
	for {
		k, rd := rmc.getCachedRangeDescriptor(key)
		if k != nil {
			rmc.rangeCacheMu.Lock()
			rmc.rangeCache.Del(k)
			rmc.rangeCacheMu.Unlock()
			if log.V(1) {
				log.Infof("evict cached descriptor: key=%s desc=%+v\n%s", key, rd, rmc)
			}
		}
		// Retrieve the metadata range key for the next level of metadata, and
		// evict that key as well. This loop ends after the meta1 range, which
		// returns KeyMin as its metadata key.
		key = engine.RangeMetaKey(key)
		if len(key) == 0 {
			break
		}
	}
}

// getCachedRangeDescriptor is a helper function to retrieve the
// descriptor of the range which contains the given key, if present in
// the cache.
func (rmc *rangeDescriptorCache) getCachedRangeDescriptor(key proto.Key) (
	rangeCacheKey, *proto.RangeDescriptor) {
	// We want to look up the range descriptor for key. The cache is
	// indexed using the end-key of the range, but the end-key is
	// non-inclusive. So we access the cache using key.Next().
	metaKey := engine.RangeMetaKey(key.Next())
	rmc.rangeCacheMu.RLock()
	defer rmc.rangeCacheMu.RUnlock()

	k, v, ok := rmc.rangeCache.Ceil(rangeCacheKey(metaKey))
	if !ok {
		return nil, nil
	}
	metaEndKey := k.(rangeCacheKey)
	rd := v.(*proto.RangeDescriptor)

	// Check that key actually belongs to range
	if !rd.ContainsKey(engine.KeyAddress(key)) {
		return nil, nil
	}
	return metaEndKey, rd
}
