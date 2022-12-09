// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tochar

import (
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

const maxCacheKeySize = 100

// FormatCache is a cache used to store parsing info used for to_char.
// It is thread safe, and is safe to use by `nil` caches.
type FormatCache struct {
	mu struct {
		syncutil.RWMutex
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
		pc.mu.RLock()
		ret, ok := pc.mu.cache.Get(fmtString)
		pc.mu.RUnlock()
		if ok {
			return ret.([]formatNode)
		}

		pc.mu.Lock()
		defer pc.mu.Unlock()
		r := parseFormat(fmtString)
		pc.mu.cache.Add(fmtString, r)
		return r
	}
	return parseFormat(fmtString)
}
