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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package kv

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// A LeaseHolderCache is a cache of replica descriptors keyed by range ID.
type LeaseHolderCache struct {
	mu    syncutil.Mutex
	cache *cache.UnorderedCache
}

// NewLeaseHolderCache creates a new leaseHolderCache of the given size.
// The underlying cache internally uses a hash map, so lookups
// are cheap.
func NewLeaseHolderCache(size int) *LeaseHolderCache {
	return &LeaseHolderCache{
		cache: cache.NewUnorderedCache(cache.Config{
			Policy: cache.CacheLRU,
			ShouldEvict: func(s int, key, value interface{}) bool {
				return s > size
			},
		}),
	}
}

// Lookup returns the cached leader of the given range ID.
func (lc *LeaseHolderCache) Lookup(
	ctx context.Context, rangeID roachpb.RangeID,
) (roachpb.ReplicaDescriptor, bool) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if v, ok := lc.cache.Get(rangeID); ok {
		if log.V(2) {
			log.Infof(ctx, "lookup lease holder for r%d: %s", rangeID, v)
		}
		return v.(roachpb.ReplicaDescriptor), true
	}
	if log.V(2) {
		log.Infof(ctx, "lookup lease holder for r%d: not found", rangeID)
	}
	return roachpb.ReplicaDescriptor{}, false
}

// Update invalidates the cached leader for the given range ID. If an empty
// replica descriptor is passed, the cached leader is evicted. Otherwise, the
// passed-in replica descriptor is cached.
func (lc *LeaseHolderCache) Update(
	ctx context.Context, rangeID roachpb.RangeID, repDesc roachpb.ReplicaDescriptor,
) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if (repDesc == roachpb.ReplicaDescriptor{}) {
		if log.V(2) {
			log.Infof(ctx, "evicting lease holder for r%d", rangeID)
		}
		lc.cache.Del(rangeID)
	} else {
		if log.V(2) {
			log.Infof(ctx, "updating lease holder for r%d: %s", rangeID, repDesc)
		}
		lc.cache.Add(rangeID, repDesc)
	}
}
