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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Matt Tracy (matt.r.tracy@gmail.com)

package kv

import (
	"bytes"
	"sync"

	"code.google.com/p/biogo.store/llrb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
)

const (
	// rangeCacheSize is the number of entries held in the range cache.
	// TODO(mrtracy): This value should be a command-line option.
	rangeCacheSize = 1 << 20
)

// rangeCacheKey is the key type used to store and sort values in the
// RangeCache.
type rangeCacheKey storage.Key

// Compare implements the llrb.Comparable interface for rangeCacheKey, so that
// it can be used as a key for util.OrderedCache.
func (a rangeCacheKey) Compare(b llrb.Comparable) int {
	return bytes.Compare(a, b.(rangeCacheKey))
}

// rangeCacheShouldEvict is a function that determines when range cache
// entries are evicted.
func rangeCacheShouldEvict(size int, k, v interface{}) bool {
	return size > rangeCacheSize
}

// RangeMetadataDB is a type which can query metadata range information from an
// underlying datastore. This interface is used by RangeMetadataCache to
// initially retrieve information which will be cached. Unlike RangeMetataCache,
// this interface directly exposes access to metadata ranges at both the first
// and second level.
type RangeMetadataDB interface {
	GetLevelOneMetadata(storage.Key) (storage.Key, *storage.RangeDescriptor, error)
	GetLevelTwoMetadata(storage.Key, *storage.RangeDescriptor) (storage.Key, *storage.RangeDescriptor, error)
}

// RangeMetadataCache is used to retrieve range metadata for arbitrary keys.
// Metadata is initially queried from storage using a RangeMetadataDB, but is
// cached for subsequent lookups.
type RangeMetadataCache struct {
	// RangeMetadataDB is used to retrieve metadata range information from the
	// database, which will be cached by this structure.
	RangeMetadataDB
	// rangeCache caches replica metadata for key ranges. The cache is
	// filled while servicing read and write requests to the key value
	// store.
	rangeCache *util.OrderedCache
	// rangeCacheMu protects rangeCache for concurrent access
	rangeCacheMu sync.RWMutex
}

// NewRangeMetadataCache returns a new RangeMetadataCache which uses the given
// RangeMetadataDB as the underlying source of range metadata.
func NewRangeMetadataCache(db RangeMetadataDB) *RangeMetadataCache {
	return &RangeMetadataCache{
		RangeMetadataDB: db,
		rangeCache: util.NewOrderedCache(util.CacheConfig{
			Policy:      util.CacheLRU,
			ShouldEvict: rangeCacheShouldEvict,
		}),
	}
}

// LookupRangeMetadata attempts to locate metadata for the range containing the
// given Key. This is done by querying the two-level lookup table of range
// metadata which cockroach maintains.
//
// This method first looks up the specified key in the first level of range
// metadata, which returns the location of the key within the second level of
// range metadata. This second level location is then queried to retrieve
// metadata for the range where the key's value resides. Range metadata
// descriptors retrieved during each search are cached for subsequent lookups.
//
// This method returns the RangeDescriptor for the range containing the key's
// data, or an error if any occurred.
func (rmc *RangeMetadataCache) LookupRangeMetadata(key storage.Key) (*storage.RangeDescriptor, error) {
	// Check cache for second-level metadata, which may already be present
	_, meta2range := rmc.getCachedRangeMetadata(storage.KeyMeta2Prefix, key)
	if meta2range != nil {
		return meta2range, nil
	}

	// If not present, retrieve first-level metadata range, consulting the cache
	// before the server.
	_, meta1range := rmc.getCachedRangeMetadata(storage.KeyMeta1Prefix, key)
	if meta1range == nil {
		k, v, err := rmc.GetLevelOneMetadata(key)
		if err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(k, storage.KeyMeta1Prefix) {
			return nil, util.Errorf("got invalid meta1 key from lookup: %s",
				string(k))
		}
		rmc.rangeCacheMu.Lock()
		rmc.rangeCache.Add(rangeCacheKey(k), v)
		rmc.rangeCacheMu.Unlock()
		meta1range = v
	}

	// Retrieve second-level metadata, cache and return.
	k, meta2range, err := rmc.GetLevelTwoMetadata(key, meta1range)
	if err != nil {
		return nil, err
	}
	if !bytes.HasPrefix(k, storage.KeyMeta2Prefix) {
		return nil, util.Errorf("got invalid meta2 key from lookup: %s",
			string(k))
	}
	rmc.rangeCacheMu.Lock()
	rmc.rangeCache.Add(rangeCacheKey(k), meta2range)
	rmc.rangeCacheMu.Unlock()
	return meta2range, nil
}

// EvictCachedRangeMetadata will evict any cached metadata range descriptors for
// the given key. It is intended that this method be called from a consumer of
// RangeMetadataCache when the returned range metadata is discovered to be
// stale.
func (rmc *RangeMetadataCache) EvictCachedRangeMetadata(key storage.Key) {
	k, _ := rmc.getCachedRangeMetadata(storage.KeyMeta1Prefix, key)
	if k != nil {
		rmc.rangeCacheMu.Lock()
		rmc.rangeCache.Del(k)
		rmc.rangeCacheMu.Unlock()
	}
	k, _ = rmc.getCachedRangeMetadata(storage.KeyMeta2Prefix, key)
	if k != nil {
		rmc.rangeCacheMu.Lock()
		rmc.rangeCache.Del(k)
		rmc.rangeCacheMu.Unlock()
	}
}

// getCachedRangeMetadata is a helper function to retrieve a cached metadata
// range which contains the given key, if present in the cache.
func (rmc *RangeMetadataCache) getCachedRangeMetadata(prefix storage.Key, key storage.Key) (
	rangeCacheKey, *storage.RangeDescriptor) {
	rmc.rangeCacheMu.RLock()
	defer rmc.rangeCacheMu.RUnlock()

	cacheKey := rangeCacheKey(storage.MakeKey(prefix, key))
	k, v, ok := rmc.rangeCache.Ceil(cacheKey)
	if !ok {
		return nil, nil
	}
	endkey := k.(rangeCacheKey)
	val := v.(*storage.RangeDescriptor)

	// Candidate key should have same metadata prefix
	if !bytes.HasPrefix(endkey, prefix) {
		return nil, nil
	}
	// Check that key actually belongs to range:
	if cacheKey.Compare(rangeCacheKey(val.StartKey)) < 0 {
		return nil, nil
	}
	return endkey, val
}
