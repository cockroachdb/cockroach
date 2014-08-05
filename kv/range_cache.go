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
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
)

const (
	// rangeCacheSize is the number of entries held in the range cache.
	// TODO(mrtracy): This value should be a command-line option.
	rangeCacheSize = 1 << 20
)

// rangeCacheKey is the key type used to store and sort values in the
// RangeCache.
type rangeCacheKey engine.Key

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
// initially retrieve information which will be cached.
type RangeMetadataDB interface {
	// Lookup metadata for the range containing the given key.  This method
	// should return the RangeDescriptor containing the key, along with the
	// Metadata key at which the RangeDescriptor's value was stored.
	LookupRangeMetadata(engine.Key) (engine.Key, *storage.RangeDescriptor, error)
}

// RangeMetadataCache is used to retrieve range metadata for arbitrary keys.
// Metadata is initially queried from storage using a RangeMetadataDB, but is
// cached for subsequent lookups.
type RangeMetadataCache struct {
	// RangeMetadataDB is used to retrieve metadata range information from the
	// database, which will be cached by this structure.
	db RangeMetadataDB
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
		db: db,
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
func (rmc *RangeMetadataCache) LookupRangeMetadata(key engine.Key) (*storage.RangeDescriptor, error) {
	metadataKey := engine.RangeMetaKey(key)
	if len(metadataKey) == 0 {
		// The description of the first range is gossiped, and thus does not
		// need to be cached.  Just ask the underlying db, which will return
		// immediately.
		_, r, err := rmc.db.LookupRangeMetadata(key)
		return r, err
	}

	_, r := rmc.getCachedRangeMetadata(key)
	if r != nil {
		return r, nil
	}

	k, r, err := rmc.db.LookupRangeMetadata(key)
	if err != nil {
		return nil, err
	}
	rmc.rangeCacheMu.Lock()
	rmc.rangeCache.Add(rangeCacheKey(k), r)
	rmc.rangeCacheMu.Unlock()
	return r, nil
}

// EvictCachedRangeMetadata will evict any cached metadata range descriptors for
// the given key. It is intended that this method be called from a consumer of
// RangeMetadataCache when the returned range metadata is discovered to be
// stale.
func (rmc *RangeMetadataCache) EvictCachedRangeMetadata(key engine.Key) {
	for {
		k, _ := rmc.getCachedRangeMetadata(key)
		if k != nil {
			rmc.rangeCacheMu.Lock()
			rmc.rangeCache.Del(k)
			rmc.rangeCacheMu.Unlock()
		}
		// Retrieve the metadata range key for the next level of metadata, and
		// evict that key as well.  This loop ends after the meta1 range, which
		// returns KeyMin as its metadata key.
		key = engine.RangeMetaKey(key)
		if len(key) == 0 {
			break
		}
	}
}

// getCachedRangeMetadata is a helper function to retrieve the metadata
// range which contains the given key, if present in the cache.
func (rmc *RangeMetadataCache) getCachedRangeMetadata(key engine.Key) (
	rangeCacheKey, *storage.RangeDescriptor) {
	metaKey := engine.RangeMetaKey(key)
	rmc.rangeCacheMu.RLock()
	defer rmc.rangeCacheMu.RUnlock()

	k, v, ok := rmc.rangeCache.Ceil(rangeCacheKey(metaKey))
	if !ok {
		return nil, nil
	}
	metaEndKey := k.(rangeCacheKey)
	rd := v.(*storage.RangeDescriptor)

	// Check that key actually belongs to range
	if !rd.ContainsKey(key) {
		return nil, nil
	}
	return metaEndKey, rd
}
