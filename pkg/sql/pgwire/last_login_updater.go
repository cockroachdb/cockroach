// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgwire

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
)

// lastLoginUpdater handles updating the last login time for SQL users with
// deduplication via singleflight to reduce concurrent updates.
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
	}
}

// newLastLoginUpdater creates a new lastLoginUpdater instance.
func newLastLoginUpdater(execCfg *sql.ExecutorConfig) *lastLoginUpdater {
	u := &lastLoginUpdater{
		group:   singleflight.NewGroup("update last login time", ""),
		execCfg: execCfg,
	}
	u.mu.pendingUsers = make(map[username.SQLUsername]struct{})
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
			u.mu.Lock()
			u.mu.pendingUsers[dbUser] = struct{}{}
			u.mu.Unlock()
			return nil, u.processPendingUpdates(ctx)
		})

	// Add this user to the pending set if not the leader,
	if !leader {
		u.mu.Lock()
		u.mu.pendingUsers[dbUser] = struct{}{}
		u.mu.Unlock()
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
	u.mu.Lock()
	users := make([]username.SQLUsername, 0, len(u.mu.pendingUsers))
	for user := range u.mu.pendingUsers {
		users = append(users, user)
	}
	// Clear the pending users set.
	u.mu.pendingUsers = make(map[username.SQLUsername]struct{})
	u.mu.Unlock()

	// Update last login time for all pending users in a single query.
	err := sql.UpdateLastLoginTime(ctx, u.execCfg, users)
	if err != nil {
		return err
	}
	return nil
}
