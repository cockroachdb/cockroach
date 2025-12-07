// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metric

import (
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// LabelSliceCacheKey is the hash key type for the cache.
type LabelSliceCacheKey uint64

// LabelSliceCacheValue is the value stored in the cache.
type LabelSliceCacheValue struct {
	LabelValues []string
	Counter     atomic.Int64
}

// LabelSliceCache is a thread-safe cache mapping hash keys to label value/counter pairs.
type LabelSliceCache struct {
	mu struct {
		syncutil.Mutex
		cache *cache.UnorderedCache
	}
}

// NewLabelSliceCache creates a new LabelSliceCache.
func NewLabelSliceCache() *LabelSliceCache {
	labelSliceCache := &LabelSliceCache{}
	labelSliceCache.mu.cache = cache.NewUnorderedCache(cache.Config{})
	return labelSliceCache
}

// Get returns the value for the given key, or nil if not present.
func (lsc *LabelSliceCache) Get(key LabelSliceCacheKey) (*LabelSliceCacheValue, bool) {
	lsc.mu.Lock()
	defer lsc.mu.Unlock()
	val, ok := lsc.mu.cache.Get(key)
	if !ok {
		return nil, false
	}
	return val.(*LabelSliceCacheValue), true
}

// Upsert adds or updates the value for the given key with reference counting.
// This method implements the "add reference" part of the reference counting mechanism.
// When a new metric with a specific label combination is created, this method performs
// either:
//  1. Add a new entry to the cache with a reference count of 1 (if the label combination
//     is being used for the first time)
//  2. Increment the existing reference count by 1 (if the label combination is already
//     cached and being used by other metrics)
//
// This ensures that the cache tracks how many metrics are currently using each
// label combination, enabling proper cleanup via DecrementAndDeleteIfZero when
// metrics are no longer needed.
func (lsc *LabelSliceCache) Upsert(key LabelSliceCacheKey, value *LabelSliceCacheValue) {
	lsc.mu.Lock()
	defer lsc.mu.Unlock()
	val, ok := lsc.mu.cache.Get(key)

	if !ok {
		value.Counter.Store(1)
		lsc.mu.cache.Add(key, value)
	} else {
		existingValue := val.(*LabelSliceCacheValue)
		existingValue.Counter.Add(1)
		lsc.mu.cache.Add(key, existingValue)
	}
}

// Delete removes the value for the given key.
func (lsc *LabelSliceCache) Delete(key LabelSliceCacheKey) {
	lsc.mu.Lock()
	defer lsc.mu.Unlock()
	lsc.mu.cache.Del(key)
}

// DecrementAndDeleteIfZero decrements the reference counter for the given key by 1.
// This method implements reference counting for cached label value combinations.
// When metrics with specific label combinations are created, the cache counter
// is incremented via Upsert. When those metrics are no longer needed or go out
// of scope, this method should be called to decrement the reference count.
//
// The automatic deletion when the counter reaches zero is crucial for preventing
// memory leaks in long-running processes where metrics with dynamic label values
// might be created and destroyed frequently. Without this cleanup mechanism,
// the cache would accumulate unused label combinations indefinitely.
//
// Returns true if the entry was deleted due to the counter reaching zero,
// false if the entry still has references or didn't exist.
func (lsc *LabelSliceCache) DecrementAndDeleteIfZero(key LabelSliceCacheKey) bool {
	lsc.mu.Lock()
	defer lsc.mu.Unlock()

	val, ok := lsc.mu.cache.Get(key)
	if !ok {
		// Key doesn't exist, return 0 and false
		return false
	}

	existingValue := val.(*LabelSliceCacheValue)
	newCount := existingValue.Counter.Add(-1)

	if newCount <= 0 {
		// Remove the entry when counter reaches 0 or below
		lsc.mu.cache.Del(key)
		return true
	}

	return false
}
