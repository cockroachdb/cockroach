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

func (pc *FormatCache) lookup(fmtString string) []formatNode {
	if pc != nil && len(fmtString) <= maxCacheKeySize {
		if ret, ok := func() ([]formatNode, bool) {
			pc.mu.Lock()
			defer pc.mu.Unlock()
			ret, ok := pc.mu.cache.Get(fmtString)
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
		pc.mu.cache.Add(fmtString, r)
		return r
	}
	return parseFormat(fmtString)
}
