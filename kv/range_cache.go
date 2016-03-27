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
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/cache"
	"github.com/cockroachdb/cockroach/util/log"
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

func mustMeta(k roachpb.RKey) roachpb.RKey {
	m, err := meta(k)
	if err != nil {
		panic(err)
	}
	return m
}

// RangeDescriptorDB is a type which can query range descriptors from an
// underlying datastore. This interface is used by rangeDescriptorCache to
// initially retrieve information which will be cached.
type RangeDescriptorDB interface {
	// rangeLookup takes a meta key to look up descriptors for,
	// for example \x00\x00meta1aa or \x00\x00meta2f.
	// The two booleans are considerIntents and useReverseScan respectively.
	RangeLookup(roachpb.RKey, *roachpb.RangeDescriptor, bool, bool) ([]roachpb.RangeDescriptor, *roachpb.Error)
	// FirstRange returns the descriptor for the first Range. This is the
	// Range containing all \x00\x00meta1 entries.
	FirstRange() (*roachpb.RangeDescriptor, *roachpb.Error)
}

// rangeDescriptorCache is used to retrieve range descriptors for
// arbitrary keys. Descriptors are initially queried from storage
// using a RangeDescriptorDB, but is cached for subsequent lookups.
type rangeDescriptorCache struct {
	// RangeDescriptorDB is used to retrieve range descriptors from the
	// database, which will be cached by this structure.
	db RangeDescriptorDB
	// rangeCache caches replica metadata for key ranges. The cache is
	// filled while servicing read and write requests to the key value
	// store.
	rangeCache *cache.OrderedCache
	// rangeCacheMu protects rangeCache for concurrent access
	rangeCacheMu sync.RWMutex
}

// newRangeDescriptorCache returns a new RangeDescriptorCache which
// uses the given RangeDescriptorDB as the underlying source of range
// descriptors.
func newRangeDescriptorCache(db RangeDescriptorDB, size int) *rangeDescriptorCache {
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
		fmt.Fprintf(&buf, "key=%s desc=%+v\n", roachpb.Key(k.(rangeCacheKey)), v)
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
func (rdc *rangeDescriptorCache) LookupRangeDescriptor(key roachpb.RKey,
	considerIntents, useReverseScan bool) (*roachpb.RangeDescriptor, *roachpb.Error) {
	if _, r, err := rdc.getCachedRangeDescriptor(key, useReverseScan); err != nil {
		return nil, roachpb.NewError(err)
	} else if r != nil {
		return r, nil
	}

	if log.V(2) {
		log.Infof("lookup range descriptor: key=%s\n%s", key, rdc)
	} else if log.V(1) {
		log.Infof("lookup range descriptor: key=%s", key)
	}
	rs, pErr := func(key roachpb.RKey, considerIntents, useReverseScan bool) ([]roachpb.RangeDescriptor, *roachpb.Error) {
		// metadataKey is sent to rangeLookup to find the
		// RangeDescriptor which contains key.
		metadataKey, err := meta(key)
		if err != nil {
			return nil, roachpb.NewError(err)
		}

		var (
			// desc is the RangeDescriptor for the range which contains
			// metadataKey.
			desc *roachpb.RangeDescriptor
			pErr *roachpb.Error
		)

		if bytes.Equal(metadataKey, roachpb.RKeyMin) {
			// In this case, the requested key is stored in the cluster's first
			// range. Return the first range, which is always gossiped and not
			// queried from the datastore.
			desc, pErr = rdc.db.FirstRange()
			if pErr != nil {
				return nil, pErr
			}
			return []roachpb.RangeDescriptor{*desc}, nil
		}
		if bytes.HasPrefix(metadataKey, keys.Meta1Prefix) {
			// In this case, desc is the cluster's first range.
			if desc, pErr = rdc.db.FirstRange(); pErr != nil {
				return nil, pErr
			}
		} else {
			// Look up desc from the cache, which will recursively call into
			// this function if it is not cached.
			desc, pErr = rdc.LookupRangeDescriptor(metadataKey, considerIntents, useReverseScan)
			if pErr != nil {
				return nil, pErr
			}
		}
		return rdc.db.RangeLookup(metadataKey, desc, considerIntents, useReverseScan)
	}(key, considerIntents, useReverseScan)
	if pErr != nil {
		return nil, pErr
	}
	if len(rs) == 0 {
		panic(fmt.Sprintf("no range descriptors returned for %s", key))
	}
	// TODO(tamird): there is a race here; multiple readers may experience cache
	// misses and concurrently attempt to refresh the cache, duplicating work.
	// Locking over the getRangeDescriptors call is even worse though, because
	// that blocks the cache completely for the duration of a slow query to the
	// cluster.
	rdc.rangeCacheMu.Lock()
	for i := range rs {
		// Note: we append the end key of each range to meta records
		// so that calls to rdc.rangeCache.Ceil() for a key will return
		// the correct range. Using the start key would require using
		// Floor() which is a possibility for our llrb-based OrderedCache
		// but not possible for RocksDB.

		// Before adding a new descriptor, make sure we clear out any
		// pre-existing, overlapping descriptor which might have been
		// re-inserted due to concurrent range lookups.
		rangeKey, err := meta(rs[i].EndKey)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		if log.V(1) {
			log.Infof("adding descriptor: key=%s desc=%s", rangeKey, &rs[i])
		}
		if err := rdc.clearOverlappingCachedRangeDescriptors(&rs[i]); err != nil {
			return nil, roachpb.NewError(err)
		}
		rdc.rangeCache.Add(rangeCacheKey(rangeKey), &rs[i])
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
func (rdc *rangeDescriptorCache) EvictCachedRangeDescriptor(descKey roachpb.RKey,
	seenDesc *roachpb.RangeDescriptor, inclusive bool) error {
	if seenDesc == nil {
		log.Warningf("compare-and-evict for key %s with nil descriptor; clearing unconditionally", descKey)
	}

	rdc.rangeCacheMu.Lock()
	defer rdc.rangeCacheMu.Unlock()

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
		if log.V(2) {
			log.Infof("evict cached descriptor: key=%s desc=%s\n%s", descKey, cachedDesc, rdc.stringLocked())
		} else if log.V(1) {
			log.Infof("evict cached descriptor: key=%s desc=%s", descKey, cachedDesc)
		}
		rdc.rangeCache.Del(rngKey)

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
// acquires a read lock on rdc.rangeCacheMu before delegating to
// getCachedRangeDescriptorLocked.
// `inclusive` determines the behaviour at the range boundary: If set to true
// and `key` is the EndKey and StartKey of two adjacent ranges, the first range
// is returned instead of the second (which technically contains the given key).
func (rdc *rangeDescriptorCache) getCachedRangeDescriptor(key roachpb.RKey, inclusive bool) (
	rangeCacheKey, *roachpb.RangeDescriptor, error) {
	rdc.rangeCacheMu.RLock()
	defer rdc.rangeCacheMu.RUnlock()
	return rdc.getCachedRangeDescriptorLocked(key, inclusive)
}

// getCachedRangeDescriptorLocked is a helper function to retrieve the
// descriptor of the range which contains the given key, if present in the
// cache. It is assumed that the caller holds a read lock on rdc.rangeCacheMu.
func (rdc *rangeDescriptorCache) getCachedRangeDescriptorLocked(key roachpb.RKey, inclusive bool) (
	rangeCacheKey, *roachpb.RangeDescriptor, error) {
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

	k, v, ok := rdc.rangeCache.Ceil(rangeCacheKey(metaKey))
	if !ok {
		return nil, nil, nil
	}
	metaEndKey := k.(rangeCacheKey)
	rd := v.(*roachpb.RangeDescriptor)

	// Check that key actually belongs to the range.
	if !rd.ContainsKey(key) {
		// The key is the EndKey and we're inclusive, so just return the range descriptor.
		if inclusive && key.Equal(rd.EndKey) {
			return metaEndKey, rd, nil
		}
		return nil, nil, nil
	}

	// The key is the StartKey, but we're inclusive and thus need to return the
	// previous range descriptor, but it is not in the cache yet.
	if inclusive && key.Equal(rd.StartKey) {
		return nil, nil, nil
	}
	return metaEndKey, rd, nil
}

// clearOverlappingCachedRangeDescriptors looks up and clears any
// cache entries which overlap the specified descriptor.
func (rdc *rangeDescriptorCache) clearOverlappingCachedRangeDescriptors(desc *roachpb.RangeDescriptor) error {
	key := desc.EndKey
	metaKey, err := meta(key)
	if err != nil {
		return err
	}

	// Clear out any descriptors which subsume the key which we're going
	// to cache. For example, if an existing KeyMin->KeyMax descriptor
	// should be cleared out in favor of a KeyMin->"m" descriptor.
	k, v, ok := rdc.rangeCache.Ceil(rangeCacheKey(metaKey))
	if ok {
		descriptor := v.(*roachpb.RangeDescriptor)
		if descriptor.StartKey.Less(key) && !descriptor.EndKey.Less(key) {
			if log.V(1) {
				log.Infof("clearing overlapping descriptor: key=%s desc=%s", k, descriptor)
			}
			rdc.rangeCache.Del(k.(rangeCacheKey))
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
	rdc.rangeCache.DoRange(func(k, v interface{}) {
		if log.V(1) {
			log.Infof("clearing subsumed descriptor: key=%s desc=%s", k, v.(*roachpb.RangeDescriptor))
		}
		rdc.rangeCache.Del(k.(rangeCacheKey))
	}, rangeCacheKey(startMeta.Next()), rangeCacheKey(endMeta))
	return nil
}
