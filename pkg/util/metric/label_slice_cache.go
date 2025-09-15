package metric

import (
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/cache"
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
	mu    sync.RWMutex
	cache *cache.UnorderedCache
}

// NewLabelSliceCache creates a new LabelSliceCache.
func NewLabelSliceCache() *LabelSliceCache {
	return &LabelSliceCache{
		cache: cache.NewUnorderedCache(cache.Config{}),
	}
}

// Get returns the value for the given key, or nil if not present.
func (lsc *LabelSliceCache) Get(key LabelSliceCacheKey) (*LabelSliceCacheValue, bool) {
	lsc.mu.RLock()
	defer lsc.mu.RUnlock()
	val, ok := lsc.cache.Get(key)
	if !ok {
		return nil, false
	}
	return val.(*LabelSliceCacheValue), true
}

// Upsert adds or updates the value for the given key. If the key already exists,
// it increments the counter by 1. If it does not exist, it initializes the counter to 1.
func (lsc *LabelSliceCache) Upsert(key LabelSliceCacheKey, value *LabelSliceCacheValue) {
	lsc.mu.Lock()
	defer lsc.mu.Unlock()
	val, ok := lsc.cache.Get(key)

	if !ok {
		value.Counter.Store(1)
		lsc.cache.Add(key, value)
	} else {
		existingValue := val.(*LabelSliceCacheValue)
		existingValue.Counter.Add(1)
		lsc.cache.Add(key, existingValue)
	}
}

// Delete removes the value for the given key.
func (lsc *LabelSliceCache) Delete(key LabelSliceCacheKey) {
	lsc.mu.Lock()
	defer lsc.mu.Unlock()
	lsc.cache.Del(key)
}

// DecrementAndDeleteIfZero decrements the counter for the given key by 1.
// If the counter reaches 0, the entry is removed from the cache.
// Returns the new counter value and true if the entry was deleted.
func (lsc *LabelSliceCache) DecrementAndDeleteIfZero(key LabelSliceCacheKey) bool {
	lsc.mu.Lock()
	defer lsc.mu.Unlock()

	val, ok := lsc.cache.Get(key)
	if !ok {
		// Key doesn't exist, return 0 and false
		return false
	}

	existingValue := val.(*LabelSliceCacheValue)
	newCount := existingValue.Counter.Add(-1)

	if newCount <= 0 {
		// Remove the entry when counter reaches 0 or below
		lsc.cache.Del(key)
		return true
	}

	return false
}
