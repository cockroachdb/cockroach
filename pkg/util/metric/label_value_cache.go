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
func (lvc *LabelSliceCache) Get(key LabelSliceCacheKey) (*LabelSliceCacheValue, bool) {
	lvc.mu.RLock()
	defer lvc.mu.RUnlock()
	val, ok := lvc.cache.Get(key)
	if !ok {
		return nil, false
	}
	return val.(*LabelSliceCacheValue), true
}

// Upsert adds or updates the value for the given key. If the key already exists,
// it increments the counter by 1. If it does not exist, it initializes the counter to 1.
func (lvc *LabelSliceCache) Upsert(key LabelSliceCacheKey, value *LabelSliceCacheValue) {
	lvc.mu.Lock()
	defer lvc.mu.Unlock()
	val, ok := lvc.cache.Get(key)

	if !ok {
		value.Counter.Store(1)
		lvc.cache.Add(key, value)
	} else {
		existingValue := val.(*LabelSliceCacheValue)
		existingValue.Counter.Add(1)
		lvc.cache.Add(key, existingValue)
	}
}

// Delete removes the value for the given key.
func (lvc *LabelSliceCache) Delete(key LabelSliceCacheKey) {
	lvc.mu.Lock()
	defer lvc.mu.Unlock()
	lvc.cache.Del(key)
}
