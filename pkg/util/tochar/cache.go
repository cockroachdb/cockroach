// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tochar

import (
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

const maxCacheKeySize = 100

// FormatCache is a cache used to store parsing info used for to_char.
// It is thread safe, and is safe to use by `nil` caches.
type FormatCache struct {
	// mu must be a Mutex, not a RWMutex because Get can modify the LRU cache.
	mu struct {
		syncutil.Mutex
		cache *cache.UnorderedCache
	}
}

// NewFormatCache returns a new FormatCache.
func NewFormatCache(size int) *FormatCache {
	ret := &FormatCache{}
	ret.mu.cache = cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheLRU,
		ShouldEvict: func(s int, key, value interface{}) bool {
			return s > size
		},
	})
	return ret
}

// Cache key prefixes to avoid collisions between datetime (DCH) and
// numeric (NUM) format strings in the shared cache.
const (
	dchCacheKeyPrefix = "dch:"
	numCacheKeyPrefix = "num:"
)

// numCacheEntry stores the parsed result of a numeric format string.
type numCacheEntry struct {
	nodes []numFormatNode
	desc  numDesc
}

// lookupNum retrieves or parses a numeric format string. It uses a "num:"
// prefix for cache keys to avoid collisions with datetime formats.
func (pc *FormatCache) lookupNum(fmtString string) ([]numFormatNode, numDesc, error) {
	cacheKey := numCacheKeyPrefix + fmtString
	if pc != nil && len(cacheKey) <= maxCacheKeySize {
		if entry, ok := func() (numCacheEntry, bool) {
			pc.mu.Lock()
			defer pc.mu.Unlock()
			ret, ok := pc.mu.cache.Get(cacheKey)
			if ok {
				return ret.(numCacheEntry), true
			}
			return numCacheEntry{}, false
		}(); ok {
			return entry.nodes, entry.desc, nil
		}

		nodes, desc, err := parseNumFormat(fmtString)
		if err != nil {
			return nil, numDesc{}, err
		}
		entry := numCacheEntry{nodes: nodes, desc: desc}
		pc.mu.Lock()
		defer pc.mu.Unlock()
		pc.mu.cache.Add(cacheKey, entry)
		return entry.nodes, entry.desc, nil
	}
	return parseNumFormat(fmtString)
}

// lookupDCH retrieves or parses a datetime (DCH) format string. It uses
// a "dch:" prefix for cache keys to avoid collisions with numeric formats.
func (pc *FormatCache) lookupDCH(fmtString string) []formatNode {
	cacheKey := dchCacheKeyPrefix + fmtString
	if pc != nil && len(cacheKey) <= maxCacheKeySize {
		if ret, ok := func() ([]formatNode, bool) {
			pc.mu.Lock()
			defer pc.mu.Unlock()
			ret, ok := pc.mu.cache.Get(cacheKey)
			if ok {
				return ret.([]formatNode), true
			}
			return nil, false
		}(); ok {
			return ret
		}

		r := parseFormat(fmtString)
		pc.mu.Lock()
		defer pc.mu.Unlock()
		pc.mu.cache.Add(cacheKey, r)
		return r
	}
	return parseFormat(fmtString)
}
