// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cacheutil

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/errors"
)

// Cache is a shared cache for hashed passwords and other information used
// during user authentication and session initialization.
type Cache[K comparable, V any] struct {
	syncutil.Mutex
	boundAccount       mon.BoundAccount
	tableVersions      []descpb.DescriptorVersion
	cache              map[K]V
	populateCacheGroup *singleflight.Group
	stopper            *stop.Stopper
}

// NewCache initializes a new Cache.
// numSystemTables is the number of system tables backing the cache.
// We use it to initialize the tableVersions slice to 0 for each table.
func NewCache[K comparable, V any](
	account mon.BoundAccount, stopper *stop.Stopper, numSystemTables int,
) *Cache[K, V] {
	tableVersions := make([]descpb.DescriptorVersion, numSystemTables)
	return &Cache[K, V]{
		tableVersions:      tableVersions,
		boundAccount:       account,
		populateCacheGroup: singleflight.NewGroup("load-value", "key"),
		stopper:            stopper,
	}
}

// GetValueLocked returns the value and if the key is found in the cache.
// The cache lock must be held while calling this.
func (c *Cache[K, V]) GetValueLocked(key K) (V, bool) {
	val, ok := c.cache[key]
	return val, ok
}

// LoadValueOutsideOfCacheSingleFlight loads the value for the given requestKey using the provided
// function. It ensures that there is only at most one in-flight request for
// each key at any time.
func (c *Cache[K, V]) LoadValueOutsideOfCacheSingleFlight(
	ctx context.Context, requestKey string, fn func(loadCtx context.Context) (interface{}, error),
) (*V, error) {
	future, _ := c.populateCacheGroup.DoChan(ctx,
		requestKey,
		singleflight.DoOpts{
			Stop:               c.stopper,
			InheritCancelation: false,
		},
		fn,
	)
	res := future.WaitForResult(ctx)
	if res.Err != nil {
		return nil, res.Err
	}
	val := res.Val.(*V)
	return val, nil
}

// MaybeWriteBackToCache tries to put the key, value into the
// cache, and returns true if it succeeded. If the underlying system
// tables have been modified since they were read, the cache is not
// updated. The entrySize should be the size in bytes of the key and value.
// Note that reading from system tables may give us data from a newer table
// version than the one we pass in here, that is okay since the cache will
// be invalidated upon the next read.
func (c *Cache[K, V]) MaybeWriteBackToCache(
	ctx context.Context, tableVersions []descpb.DescriptorVersion, key K, value V, entrySize int64,
) bool {
	c.Lock()
	defer c.Unlock()
	// Table versions have changed while we were looking: don't cache the data.
	if len(c.tableVersions) != len(tableVersions) {
		panic(errors.AssertionFailedf("cache.tableVersions slice must be the same len as tableVersions, c.tableVersions: %v, tableVersions: %v", c.tableVersions, tableVersions))
	}
	for i := 0; i < len(c.tableVersions); i++ {
		if c.tableVersions[i] != tableVersions[i] {
			return false
		}
	}
	// Table version remains the same: update map, unlock, return.
	if err := c.boundAccount.Grow(ctx, entrySize); err != nil {
		// If there is no memory available to cache the entry, we can still
		// proceed with authentication so that users are not locked out of
		// the database.
		log.Ops.Warningf(ctx, "no memory available to cache info: %v", err)
		return false
	}
	c.cache[key] = value
	return true
}

// ClearCacheIfStaleLocked compares the cached table versions to the current table
// versions. If the cached versions are older, the cache is cleared. If the
// cached versions are newer, then false is returned to indicate that the
// cached data should not be used.
// The cache must be locked while this is called.
func (c *Cache[K, V]) ClearCacheIfStaleLocked(
	ctx context.Context, tableVersions []descpb.DescriptorVersion,
) (isEligibleForCache bool) {
	if len(c.tableVersions) != len(tableVersions) {
		panic(errors.AssertionFailedf("cache.tableVersions slice must be the same len as tableVersions, c.tableVersions: %v, tableVersions: %v", c.tableVersions, tableVersions))
	}
	for i := 0; i < len(c.tableVersions); i++ {
		// If any table is out of date, clear the cache.
		if c.tableVersions[i] < tableVersions[i] {
			// If the cache is based on old table versions,
			// then update versions and drop the map.
			c.tableVersions = tableVersions
			c.cache = make(map[K]V)
			c.boundAccount.Empty(ctx)
			return false
		}
	}
	for i := 0; i < len(c.tableVersions); i++ {
		// If the cache is based on a newer table version, then
		// don't use the cache for this transaction.
		if c.tableVersions[i] > tableVersions[i] {
			return false
		}
	}
	return true
}
