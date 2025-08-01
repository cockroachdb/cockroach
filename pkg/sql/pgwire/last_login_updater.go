// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgwire

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// lastLoginUpdater handles updating the last login time for SQL users with
// deduplication via singleflight to reduce concurrent updates. It also uses
// an LRU cache so that a user never is updated more than once every 10 minutes.
type lastLoginUpdater struct {
	// group ensures that there is at most one last login time update
	// in-flight at any given time.
	group *singleflight.Group
	// execCfg is the executor configuration used for running update operations.
	execCfg *sql.ExecutorConfig

	mu struct {
		syncutil.Mutex
		// pendingUsers tracks users that need their last login time updated
		// in the next singleflight operation.
		pendingUsers map[username.SQLUsername]struct{}
		// lastUpdateCache tracks the last time each user's login time was updated
		// to avoid redundant database updates.
		lastUpdateCache *cache.UnorderedCache
	}
}

const (
	// lastLoginCacheSize is the maximum number of users to track in the LRU
	// cache.
	lastLoginCacheSize = 100
	// lastLoginEvictionTime is how long to keep entries in the cache before
	// evicting them.
	lastLoginEvictionTime = 10 * time.Minute
)

// newLastLoginUpdater creates a new lastLoginUpdater instance.
func newLastLoginUpdater(execCfg *sql.ExecutorConfig) *lastLoginUpdater {
	// Create cache with LRU eviction, limiting to lastLoginCacheSize users and entries older than lastLoginEvictionTime.
	cacheConfig := cache.Config{
		Policy: cache.CacheLRU,
		ShouldEvict: func(size int, key, value interface{}) bool {
			// Evict if we have more than lastLoginCacheSize entries or if the entry
			// is older than lastLoginEvictionTime.
			if size > lastLoginCacheSize {
				return true
			}
			lastUpdate, ok := value.(time.Time)
			if !ok {
				// Evict invalid entries also.
				return true
			}
			return timeutil.Since(lastUpdate) > lastLoginEvictionTime
		},
	}

	u := &lastLoginUpdater{
		group:   singleflight.NewGroup("update last login time", singleflight.NoTags),
		execCfg: execCfg,
	}
	u.mu.pendingUsers = make(map[username.SQLUsername]struct{})
	u.mu.lastUpdateCache = cache.NewUnorderedCache(cacheConfig)
	return u
}

// updateLastLoginTime updates the last login time for the SQL user
// asynchronously. This is not guaranteed to succeed and we log any errors
// obtained from the update transaction to the DEV channel.
func (u *lastLoginUpdater) updateLastLoginTime(ctx context.Context, dbUser username.SQLUsername) {
	// Avoid updating if we're in a read-only tenant.
	if u.execCfg.TenantReadOnly {
		return
	}

	// Check if we recently updated this user's login time.
	var recentlyUpdated bool
	func() {
		u.mu.Lock()
		defer u.mu.Unlock()
		if lastUpdate, ok := u.mu.lastUpdateCache.Get(dbUser); ok {
			if lastUpdateTime, ok := lastUpdate.(time.Time); ok {
				// Only update if it's been more than lastLoginEvictionTime since the last update.
				if timeutil.Since(lastUpdateTime) < lastLoginEvictionTime {
					recentlyUpdated = true
				}
			}
		}
	}()
	if recentlyUpdated {
		return
	}

	// Use singleflight to ensure at most one last login time update batch
	// is in-flight at any given time.
	future, leader := u.group.DoChan(ctx, "UpdateLastLoginTime",
		singleflight.DoOpts{
			Stop:               u.execCfg.Stopper,
			InheritCancelation: false,
		},
		func(ctx context.Context) (interface{}, error) {
			// The leader adds itself to pending users, and processes all
			// pending updates.
			func() {
				u.mu.Lock()
				defer u.mu.Unlock()
				u.mu.pendingUsers[dbUser] = struct{}{}
			}()
			return nil, u.processPendingUpdates(ctx)
		})

	// Add this user to the pending set if not the leader. This causes the user
	// to be processed the next time a singleflight begins, which means the
	// recorded estimated_last_login_time will be approximate.
	if !leader {
		func() {
			u.mu.Lock()
			defer u.mu.Unlock()
			u.mu.pendingUsers[dbUser] = struct{}{}
		}()
	} else {
		// Leader waits for the result in an async task to avoid blocking authentication
		if err := u.execCfg.Stopper.RunAsyncTask(ctx, "wait_last_login_update", func(ctx context.Context) {
			result := future.WaitForResult(ctx)
			if result.Err != nil {
				log.Warningf(ctx, "could not update last login times: %v", result.Err)
			}
		}); err != nil {
			log.Warningf(ctx, "could not create async task to wait for last login update: %v", err)
		}
	}
}

// processPendingUpdates processes all users in the pending set and clears it.
func (u *lastLoginUpdater) processPendingUpdates(ctx context.Context) error {
	var users []username.SQLUsername
	func() {
		u.mu.Lock()
		defer u.mu.Unlock()
		users = make([]username.SQLUsername, 0, len(u.mu.pendingUsers))
		for user := range u.mu.pendingUsers {
			users = append(users, user)
		}
		// Clear the pending users set.
		u.mu.pendingUsers = make(map[username.SQLUsername]struct{})
	}()

	if len(users) == 0 {
		return nil
	}

	// Update last login time for all pending users in a single query.
	err := sql.UpdateLastLoginTime(ctx, u.execCfg, users)
	if err != nil {
		return err
	}

	// Update the cache with the current time for all successfully updated users.
	now := timeutil.Now()
	func() {
		u.mu.Lock()
		defer u.mu.Unlock()
		for _, user := range users {
			u.mu.lastUpdateCache.Add(user, now)
		}
	}()

	return nil
}
