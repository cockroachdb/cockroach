// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package authentication

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// CacheEnabled is a cluster setting that determines if the
// AuthInfoCache and associated logic is enabled.
var CacheEnabled = settings.RegisterBoolSetting(
	"server.authentication_cache.enabled",
	`enables a cache used during authentication to avoid lookups to system tables
when retrieving per-user authentication-related information`,
	true,
).WithPublic()

// AuthInfoCache is a shared cache for hashed passwords and other
// information used during user authentication.
type AuthInfoCache struct {
	syncutil.Mutex
	usersTableVersion       descpb.DescriptorVersion
	roleOptionsTableVersion descpb.DescriptorVersion
	// cache is a mapping from username to AuthInfo.
	cache map[security.SQLUsername]AuthInfo
}

// AuthInfo contains data that is used to perform an authentication attempt.
type AuthInfo struct {
	// UserExists is set to true if the user has a row in system.users.
	UserExists bool
	// CanLogin is set to false if the user has the NOLOGIN role option.
	CanLogin bool
	// HashedPassword is the hashed password and can be nil.
	HashedPassword []byte
	// ValidUntil is the VALID UNTIL role option.
	ValidUntil *tree.DTimestamp
}

// Get consults the AuthInfoCache and returns the AuthInfo for the provided
// normalizedUsername. If the information is not in the cache, or if the
// underlying tables have changed since the cache was populated, then the
// readFromStore callback is used to load new data.
func (a *AuthInfoCache) Get(
	ctx context.Context,
	settings *cluster.Settings,
	leaseMgr *lease.Manager,
	ie sqlutil.InternalExecutor,
	db *kv.DB,
	normalizedUsername security.SQLUsername,
	readFromStore func(
		ctx context.Context,
		txn *kv.Txn,
		ie sqlutil.InternalExecutor,
		normalizedUsername security.SQLUsername,
	) (AuthInfo, error),
) (aInfo AuthInfo, err error) {
	if !CacheEnabled.Get(&settings.SV) {
		return readFromStore(ctx, nil /* txn */, ie, normalizedUsername)
	}
	err = descs.Txn(ctx, settings, leaseMgr, ie, db,
		func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) (err error) {
			_, usersTableDesc, err := descriptors.GetImmutableTableByName(
				ctx,
				txn,
				UsersTableName,
				tree.ObjectLookupFlagsWithRequired(),
			)
			if err != nil {
				return err
			}
			_, roleOptionsTableDesc, err := descriptors.GetImmutableTableByName(
				ctx,
				txn,
				RoleOptionsTableName,
				tree.ObjectLookupFlagsWithRequired(),
			)
			if err != nil {
				return err
			}
			if usersTableDesc.IsUncommittedVersion() || roleOptionsTableDesc.IsUncommittedVersion() {
				aInfo, err = readFromStore(ctx, txn, ie, normalizedUsername)
				if err != nil {
					return err
				}
			}
			usersTableVersion := usersTableDesc.GetVersion()
			roleOptionsTableVersion := roleOptionsTableDesc.GetVersion()
			// We loop in case the table version changes while looking up
			// password or role options.

			for {
				// Check version and maybe clear cache while holding the mutex.
				var found bool
				aInfo, found = func() (AuthInfo, bool) {
					a.Lock()
					defer a.Unlock()
					if a.usersTableVersion != usersTableVersion {
						// Update users table version and drop the map.
						a.usersTableVersion = usersTableVersion
						a.cache = make(map[security.SQLUsername]AuthInfo)
					}
					if a.roleOptionsTableVersion != roleOptionsTableVersion {
						// Update role_optiosn table version and drop the map.
						a.roleOptionsTableVersion = roleOptionsTableVersion
						a.cache = make(map[security.SQLUsername]AuthInfo)
					}
					aInfo, found = a.cache[normalizedUsername]
					return aInfo, found
				}()

				if found {
					return nil
				}

				// Lookup memberships outside the lock.
				aInfo, err = readFromStore(ctx, txn, ie, normalizedUsername)
				if err != nil {
					return err
				}

				updatedCache := func() bool {
					// Update membership.
					a.Lock()
					defer a.Unlock()
					// Table version has changed while we were looking: unlock and start over.
					if a.usersTableVersion != usersTableVersion {
						usersTableVersion = a.usersTableVersion
						return false
					}
					if a.roleOptionsTableVersion != roleOptionsTableVersion {
						roleOptionsTableVersion = a.roleOptionsTableVersion
						return false
					}
					// Table version remains the same: update map, unlock, return.
					a.cache[normalizedUsername] = aInfo
					return true
				}()
				if updatedCache {
					return nil
				}
			}
		})
	return aInfo, err
}
