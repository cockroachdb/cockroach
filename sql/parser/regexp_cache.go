// Copyright 2015 The Cockroach Authors.
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
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package parser

import (
	"regexp"
	"sync"

	"github.com/cockroachdb/cockroach/util/cache"
)

// regexpCacheKey allows cache keys to take the form of different types,
// as long as they are comparable and can produce a pattern when needed
// for regexp compilation. The pattern method will not be called until
// after a cache lookup is performed and the result is a miss.
type regexpCacheKey interface {
	pattern() (string, error)
}

// A RegexpCache is a cache used to store compiled regular expressions.
// The cache is safe for concurrent use by multiple goroutines. It is also
// safe to use the cache through a nil reference, where it will act like a valid
// cache with no capacity.
type RegexpCache struct {
	mu    sync.Mutex
	cache *cache.UnorderedCache
}

// NewRegexpCache creates a new RegexpCache of the given size.
// The underlying cache internally uses a hash map, so lookups
// are cheap.
func NewRegexpCache(size int) *RegexpCache {
	return &RegexpCache{
		cache: cache.NewUnorderedCache(cache.Config{
			Policy: cache.CacheLRU,
			ShouldEvict: func(s int, key, value interface{}) bool {
				return s > size
			},
		}),
	}
}

// GetRegexp consults the cache for the regular expressions stored for
// the given key, compiling the key's pattern if it is not already
// in the cache.
func (rc *RegexpCache) GetRegexp(key regexpCacheKey) (*regexp.Regexp, error) {
	if rc != nil {
		re := rc.lookup(key)
		if re != nil {
			return re, nil
		}
	}

	pattern, err := key.pattern()
	if err != nil {
		return nil, err
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	if rc != nil {
		rc.update(key, re)
	}
	return re, nil
}

// lookup checks for the regular expression in the cache in a
// synchronized manner, returning it if it exists.
func (rc *RegexpCache) lookup(key regexpCacheKey) *regexp.Regexp {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	v, ok := rc.cache.Get(key)
	if !ok {
		return nil
	}
	return v.(*regexp.Regexp)
}

// update invalidates the regular expression for the given pattern.
// If a new regular expression is passed in, it is inserted into the cache.
func (rc *RegexpCache) update(key regexpCacheKey, re *regexp.Regexp) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.cache.Del(key)
	if re != nil {
		rc.cache.Add(key, re)
	}
}

// Len returns the number of compiled regular expressions in the cache.
func (rc *RegexpCache) Len() int {
	if rc == nil {
		return 0
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.cache.Len()
}
