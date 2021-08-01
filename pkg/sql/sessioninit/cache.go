// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sessioninit

import (
	"context"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// CacheEnabledSettingName is the name of the CacheEnabled cluster setting.
var CacheEnabledSettingName = "server.authentication_cache.enabled"

// CacheEnabled is a cluster setting that determines if the
// sessioninit.Cache and associated logic is enabled.
var CacheEnabled = settings.RegisterBoolSetting(
	CacheEnabledSettingName,
	"enables a cache used during authentication to avoid lookups to system tables "+
		"when retrieving per-user authentication-related information",
	true,
).WithPublic()

// Cache is a shared cache for hashed passwords and other information used
// during user authentication and session initialization.
type Cache struct {
	syncutil.Mutex
	usersTableVersion          descpb.DescriptorVersion
	roleOptionsTableVersion    descpb.DescriptorVersion
	dbRoleSettingsTableVersion descpb.DescriptorVersion
	boundAccount               mon.BoundAccount
	// authInfoCache is a mapping from username to AuthInfo.
	authInfoCache map[security.SQLUsername]AuthInfo
	// settingsCache is a mapping from (dbID, username) to default settings.
	settingsCache map[SettingsCacheKey][]string
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

// SettingsCacheKey is the key used for the settingsCache.
type SettingsCacheKey struct {
	DatabaseID descpb.ID
	Username   security.SQLUsername
}

// SettingsCacheEntry represents an entry in the settingsCache. It is
// used so that the entries can be returned in a stable order.
type SettingsCacheEntry struct {
	SettingsCacheKey
	Settings []string
}

// NewCache initializes a new sessioninit.Cache.
func NewCache(account mon.BoundAccount) *Cache {
	return &Cache{
		boundAccount: account,
	}
}

// GetAuthInfo consults the sessioninit.Cache and returns the AuthInfo for the
// provided username and databaseName. If the information is not in the cache,
// or if the underlying tables have changed since the cache was populated,
// then the readFromSystemTables callback is used to load new data.
func (a *Cache) GetAuthInfo(
	ctx context.Context,
	settings *cluster.Settings,
	ie sqlutil.InternalExecutor,
	db *kv.DB,
	f *descs.CollectionFactory,
	username security.SQLUsername,
	readFromSystemTables func(
		ctx context.Context,
		txn *kv.Txn,
		ie sqlutil.InternalExecutor,
		username security.SQLUsername,
	) (AuthInfo, error),
) (aInfo AuthInfo, err error) {
	if !CacheEnabled.Get(&settings.SV) {
		return readFromSystemTables(ctx, nil /* txn */, ie, username)
	}
	err = f.Txn(ctx, ie, db, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) (err error) {
		var usersTableDesc, roleOptionsTableDesc catalog.TableDescriptor
		_, usersTableDesc, err = descriptors.GetImmutableTableByName(
			ctx,
			txn,
			UsersTableName,
			tree.ObjectLookupFlagsWithRequired(),
		)
		if err != nil {
			return err
		}
		_, roleOptionsTableDesc, err = descriptors.GetImmutableTableByName(
			ctx,
			txn,
			RoleOptionsTableName,
			tree.ObjectLookupFlagsWithRequired(),
		)
		if err != nil {
			return err
		}

		// If the underlying table versions are not committed, stop and avoid
		// trying to cache anything.
		if usersTableDesc.IsUncommittedVersion() ||
			roleOptionsTableDesc.IsUncommittedVersion() {
			aInfo, err = readFromSystemTables(
				ctx,
				txn,
				ie,
				username,
			)
			return err
		}
		usersTableVersion := usersTableDesc.GetVersion()
		roleOptionsTableVersion := roleOptionsTableDesc.GetVersion()

		// We loop in case the table version changes while looking up
		// password or role options.
		for {
			// Check version and maybe clear cache while holding the mutex.
			var found bool
			aInfo, found = a.readAuthInfoFromCache(ctx, usersTableVersion, roleOptionsTableVersion, username)

			if found {
				return nil
			}

			// Lookup the data outside the lock.
			aInfo, err = readFromSystemTables(
				ctx,
				txn,
				ie,
				username,
			)
			if err != nil {
				return err
			}

			finishedLoop := a.writeAuthInfoBackToCache(
				ctx,
				&usersTableVersion,
				&roleOptionsTableVersion,
				aInfo,
				username,
			)
			if finishedLoop {
				return nil
			}
		}
	})
	return aInfo, err
}

func (a *Cache) readAuthInfoFromCache(
	ctx context.Context,
	usersTableVersion descpb.DescriptorVersion,
	roleOptionsTableVersion descpb.DescriptorVersion,
	username security.SQLUsername,
) (AuthInfo, bool) {
	a.Lock()
	defer a.Unlock()
	// We don't need to check dbRoleSettingsTableVersion here, so pass in the
	// one we already have.
	a.clearCacheIfStale(ctx, usersTableVersion, roleOptionsTableVersion, a.dbRoleSettingsTableVersion)
	ai, foundAuthInfo := a.authInfoCache[username]
	return ai, foundAuthInfo
}

// writeAuthInfoBackToCache tries to put the fetched AuthInfo into the
// authInfoCache, and returns true if it succeeded. If the underlying system
// tables have been modified since they were read, the authInfoCache is not
// updated, and the passed in table descriptor versions are updated.
func (a *Cache) writeAuthInfoBackToCache(
	ctx context.Context,
	usersTableVersion *descpb.DescriptorVersion,
	roleOptionsTableVersion *descpb.DescriptorVersion,
	aInfo AuthInfo,
	username security.SQLUsername,
) bool {
	a.Lock()
	defer a.Unlock()
	// Table versions have changed while we were looking: unlock and start over.
	if a.usersTableVersion != *usersTableVersion || a.roleOptionsTableVersion != *roleOptionsTableVersion {
		*usersTableVersion = a.usersTableVersion
		*roleOptionsTableVersion = a.roleOptionsTableVersion
		return false
	}
	// Table version remains the same: update map, unlock, return.
	const sizeOfUsername = int(unsafe.Sizeof(security.SQLUsername{}))
	const sizeOfAuthInfo = int(unsafe.Sizeof(AuthInfo{}))
	const sizeOfTimestamp = int(unsafe.Sizeof(tree.DTimestamp{}))
	sizeOfEntry := sizeOfUsername + len(username.Normalized()) +
		sizeOfAuthInfo + len(aInfo.HashedPassword) +
		sizeOfTimestamp
	if err := a.boundAccount.Grow(ctx, int64(sizeOfEntry)); err != nil {
		// If there is no memory available to cache the entry, we can still
		// proceed with authentication so that users are not locked out of
		// the database.
		log.Ops.Warningf(ctx, "no memory available to cache authentication info: %v", err)
	} else {
		a.authInfoCache[username] = aInfo
	}
	return true
}

// GetDefaultSettings consults the sessioninit.Cache and returns the list of
// SettingsCacheEntry for the provided username and databaseName. If the
// information is not in the cache, or if the underlying tables have changed
// since the cache was populated, then the readFromSystemTables callback is
// used to load new data.
func (a *Cache) GetDefaultSettings(
	ctx context.Context,
	settings *cluster.Settings,
	ie sqlutil.InternalExecutor,
	db *kv.DB,
	f *descs.CollectionFactory,
	username security.SQLUsername,
	databaseName string,
	readFromSystemTables func(
		ctx context.Context,
		txn *kv.Txn,
		ie sqlutil.InternalExecutor,
		username security.SQLUsername,
		databaseID descpb.ID,
	) ([]SettingsCacheEntry, error),
) (settingsEntries []SettingsCacheEntry, err error) {
	// TODO(rafi): remove this flag in v21.2.
	if !settings.Version.IsActive(ctx, clusterversion.DatabaseRoleSettings) {
		return nil, nil
	}

	err = f.Txn(ctx, ie, db, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) (err error) {
		var dbRoleSettingsTableDesc catalog.TableDescriptor
		_, dbRoleSettingsTableDesc, err = descriptors.GetImmutableTableByName(
			ctx,
			txn,
			DatabaseRoleSettingsTableName,
			tree.ObjectLookupFlagsWithRequired(),
		)
		if err != nil {
			return err
		}
		databaseID := descpb.ID(0)
		if databaseName != "" {
			dbDesc, err := descriptors.GetImmutableDatabaseByName(ctx, txn, databaseName, tree.DatabaseLookupFlags{})
			if err != nil {
				return err
			}
			// If dbDesc is nil, the database name was not valid, but that should
			// not cause a login-preventing error.
			if dbDesc != nil {
				databaseID = dbDesc.GetID()
			}
		}

		// If the underlying table versions are not committed or if the cache is
		// disabled, stop and avoid trying to cache anything.
		// We can't check if the cache is disabled earlier, since we always need to
		// start the `CollectionFactory.Txn()` regardless in order to look up the
		// database descriptor ID.
		if dbRoleSettingsTableDesc.IsUncommittedVersion() || !CacheEnabled.Get(&settings.SV) {
			settingsEntries, err = readFromSystemTables(
				ctx,
				txn,
				ie,
				username,
				databaseID,
			)
			return err
		}
		dbRoleSettingsTableVersion := dbRoleSettingsTableDesc.GetVersion()

		// We loop in case the table version changes while looking up
		// password or role options.
		for {
			// Check version and maybe clear cache while holding the mutex.
			var found bool
			settingsEntries, found = a.readDefaultSettingsFromCache(ctx, dbRoleSettingsTableVersion, username, databaseID)

			if found {
				return nil
			}

			// Lookup the data outside the lock.
			settingsEntries, err = readFromSystemTables(
				ctx,
				txn,
				ie,
				username,
				databaseID,
			)
			if err != nil {
				return err
			}

			finishedLoop := a.writeDefaultSettingsBackToCache(
				ctx,
				&dbRoleSettingsTableVersion,
				settingsEntries,
			)
			if finishedLoop {
				return nil
			}
		}
	})
	return settingsEntries, err
}

func (a *Cache) readDefaultSettingsFromCache(
	ctx context.Context,
	dbRoleSettingsTableVersion descpb.DescriptorVersion,
	username security.SQLUsername,
	databaseID descpb.ID,
) ([]SettingsCacheEntry, bool) {
	a.Lock()
	defer a.Unlock()
	// We don't need to check usersTableVersion or roleOptionsTableVersion here,
	// so pass in the values we already have.
	a.clearCacheIfStale(ctx, a.usersTableVersion, a.roleOptionsTableVersion, dbRoleSettingsTableVersion)
	foundAllDefaultSettings := true
	var sEntries []SettingsCacheEntry
	// Search through the cache for the settings entries we need. Since we look up
	// multiple entries in the cache, the same setting might appear multiple
	// times. Note that GenerateSettingsCacheKeys goes in order of precedence,
	// so the order of the returned []SettingsCacheEntry is important and the
	// caller must take care not to apply a setting if it has already appeared
	// earlier in the list.
	for _, k := range GenerateSettingsCacheKeys(databaseID, username) {
		s, ok := a.settingsCache[k]
		if !ok {
			foundAllDefaultSettings = false
			break
		}
		sEntries = append(sEntries, SettingsCacheEntry{k, s})
	}
	return sEntries, foundAllDefaultSettings
}

// writeDefaultSettingsBackToCache tries to put the fetched SettingsCacheEntry
// list into the settingsCache, and returns true if it succeeded. If the
// underlying system tables have been modified since they were read, the
// settingsCache is not updated, and the passed in table descriptor version is
// updated.
func (a *Cache) writeDefaultSettingsBackToCache(
	ctx context.Context,
	dbRoleSettingsTableVersion *descpb.DescriptorVersion,
	settingsEntries []SettingsCacheEntry,
) bool {
	return func() bool {
		a.Lock()
		defer a.Unlock()
		// Table version has changed while we were looking: unlock and start over.
		if a.dbRoleSettingsTableVersion != *dbRoleSettingsTableVersion {
			*dbRoleSettingsTableVersion = a.dbRoleSettingsTableVersion
			return false
		}

		// Table version remains the same: update map, unlock, return.
		const sizeOfSettingsCacheEntry = int(unsafe.Sizeof(SettingsCacheEntry{}))
		sizeOfSettings := 0
		for _, sEntry := range settingsEntries {
			if _, ok := a.settingsCache[sEntry.SettingsCacheKey]; ok {
				// Avoid double-counting memory if a key is already in the cache.
				continue
			}
			sizeOfSettings += sizeOfSettingsCacheEntry
			sizeOfSettings += len(sEntry.SettingsCacheKey.Username.Normalized())
			for _, s := range sEntry.Settings {
				sizeOfSettings += len(s)
			}
		}
		if err := a.boundAccount.Grow(ctx, int64(sizeOfSettings)); err != nil {
			// If there is no memory available to cache the entry, we can still
			// proceed with authentication so that users are not locked out of
			// the database.
			log.Ops.Warningf(ctx, "no memory available to cache authentication info: %v", err)
		} else {
			for _, sEntry := range settingsEntries {
				// Avoid re-storing an existing key.
				if _, ok := a.settingsCache[sEntry.SettingsCacheKey]; !ok {
					a.settingsCache[sEntry.SettingsCacheKey] = sEntry.Settings
				}
			}
		}
		return true
	}()
}

func (a *Cache) clearCacheIfStale(
	ctx context.Context,
	usersTableVersion descpb.DescriptorVersion,
	roleOptionsTableVersion descpb.DescriptorVersion,
	dbRoleSettingsTableVersion descpb.DescriptorVersion,
) {
	if a.usersTableVersion != usersTableVersion ||
		// Update table versions and drop the maps.
		a.roleOptionsTableVersion != roleOptionsTableVersion ||
		a.dbRoleSettingsTableVersion != dbRoleSettingsTableVersion {
		a.usersTableVersion = usersTableVersion
		a.roleOptionsTableVersion = roleOptionsTableVersion
		a.dbRoleSettingsTableVersion = dbRoleSettingsTableVersion
		a.authInfoCache = make(map[security.SQLUsername]AuthInfo)
		a.settingsCache = make(map[SettingsCacheKey][]string)
		a.boundAccount.Empty(ctx)
	}
}

// GenerateSettingsCacheKeys returns a slice of all the SettingsCacheKey
// that are relevant for the given databaseID and username. The slice is
// ordered in descending order of precedence.
func GenerateSettingsCacheKeys(
	databaseID descpb.ID, username security.SQLUsername,
) []SettingsCacheKey {
	return []SettingsCacheKey{
		{
			DatabaseID: databaseID,
			Username:   username,
		},
		{
			DatabaseID: defaultDatabaseID,
			Username:   username,
		},
		{
			DatabaseID: databaseID,
			Username:   defaultUsername,
		},
		{
			DatabaseID: defaultDatabaseID,
			Username:   defaultUsername,
		},
	}
}
