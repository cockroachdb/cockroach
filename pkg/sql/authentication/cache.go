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
// AuthInfoCache and associated logic is enabled.
var CacheEnabled = settings.RegisterBoolSetting(
	CacheEnabledSettingName,
	"enables a cache used during authentication to avoid lookups to system tables "+
		"when retrieving per-user authentication-related information",
	true,
).WithPublic()

// AuthInfoCache is a shared cache for hashed passwords and other
// information used during user authentication and session initialization.
type AuthInfoCache struct {
	syncutil.Mutex
	usersTableVersion          descpb.DescriptorVersion
	roleOptionsTableVersion    descpb.DescriptorVersion
	dbRoleSettingsTableVersion descpb.DescriptorVersion
	boundAccount               mon.BoundAccount
	// cache is a mapping from username to AuthInfo.
	cache map[security.SQLUsername]AuthInfo
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

// NewCache initializes a new AuthInfoCache.
func NewCache(account mon.BoundAccount) *AuthInfoCache {
	return &AuthInfoCache{
		boundAccount: account,
	}
}

// Get consults the AuthInfoCache and returns the AuthInfo and list of
// SettingsCacheEntry for the provided username and databaseName. If the
// information is not in the cache, or if the underlying tables have changed
// since the cache was populated, then the readFromStore callback is used to
// load new data.
func (a *AuthInfoCache) Get(
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
		fetchDefaultSettings bool,
	) (AuthInfo, []SettingsCacheEntry, error),
) (aInfo AuthInfo, settingsEntries []SettingsCacheEntry, err error) {
	// TODO(rafi): remove this flag in v21.2.
	fetchDefaultSettings := settings.Version.IsActive(ctx, clusterversion.DatabaseRoleSettings)

	err = f.Txn(ctx, ie, db, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) (err error) {
		var usersTableDesc, roleOptionsTableDesc, dbRoleSettingsTableDesc catalog.TableDescriptor
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
		if fetchDefaultSettings {
			_, dbRoleSettingsTableDesc, err = descriptors.GetImmutableTableByName(
				ctx,
				txn,
				DatabaseRoleSettingsTableName,
				tree.ObjectLookupFlagsWithRequired(),
			)
		}
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

		if usersTableDesc.IsUncommittedVersion() ||
			roleOptionsTableDesc.IsUncommittedVersion() ||
			(fetchDefaultSettings && dbRoleSettingsTableDesc.IsUncommittedVersion()) ||
			!CacheEnabled.Get(&settings.SV) {
			aInfo, settingsEntries, err = readFromSystemTables(
				ctx,
				txn,
				ie,
				username,
				databaseID,
				fetchDefaultSettings,
			)
			if err != nil {
				return err
			}
		}
		usersTableVersion := usersTableDesc.GetVersion()
		roleOptionsTableVersion := roleOptionsTableDesc.GetVersion()
		dbRoleSettingsTableVersion := descpb.DescriptorVersion(0)
		if fetchDefaultSettings {
			dbRoleSettingsTableVersion = dbRoleSettingsTableDesc.GetVersion()
		}

		// We loop in case the table version changes while looking up
		// password or role options.
		for {
			// Check version and maybe clear cache while holding the mutex.
			var found bool
			aInfo, settingsEntries, found = a.readFromCache(ctx, usersTableVersion, roleOptionsTableVersion, dbRoleSettingsTableVersion, username, databaseID)

			if found {
				return nil
			}

			// Lookup the data outside the lock.
			aInfo, settingsEntries, err = readFromSystemTables(
				ctx,
				txn,
				ie,
				username,
				databaseID,
				fetchDefaultSettings,
			)
			if err != nil {
				return err
			}

			finishedLoop := a.writeBackToCache(
				ctx,
				&usersTableVersion,
				&roleOptionsTableVersion,
				&dbRoleSettingsTableVersion,
				aInfo,
				settingsEntries,
				username,
			)
			if finishedLoop {
				return nil
			}
		}
	})
	return aInfo, settingsEntries, err
}

func (a *AuthInfoCache) readFromCache(
	ctx context.Context,
	usersTableVersion descpb.DescriptorVersion,
	roleOptionsTableVersion descpb.DescriptorVersion,
	dbRoleSettingsTableVersion descpb.DescriptorVersion,
	username security.SQLUsername,
	databaseID descpb.ID,
) (AuthInfo, []SettingsCacheEntry, bool) {
	a.Lock()
	defer a.Unlock()
	a.checkStaleness(ctx, usersTableVersion, roleOptionsTableVersion, dbRoleSettingsTableVersion)
	ai, foundAuthInfo := a.cache[username]
	foundAllDefaultSettings := true
	var sEntries []SettingsCacheEntry
	// Search through the cache for the settings entries we need. Note
	// that GenerateSettingsCacheKeys goes in order of precedence.
	for _, k := range GenerateSettingsCacheKeys(databaseID, username) {
		s, ok := a.settingsCache[k]
		if !ok {
			foundAllDefaultSettings = false
			break
		}
		sEntries = append(sEntries, SettingsCacheEntry{k, s})
	}
	return ai, sEntries, foundAuthInfo && foundAllDefaultSettings
}

func (a *AuthInfoCache) checkStaleness(
	ctx context.Context,
	usersTableVersion descpb.DescriptorVersion,
	roleOptionsTableVersion descpb.DescriptorVersion,
	dbRoleSettingsTableVersion descpb.DescriptorVersion,
) {
	if a.usersTableVersion != usersTableVersion {
		// Update users table version and drop the map.
		a.usersTableVersion = usersTableVersion
		a.cache = make(map[security.SQLUsername]AuthInfo)
		a.settingsCache = make(map[SettingsCacheKey][]string)
		a.boundAccount.Empty(ctx)
	}
	if a.roleOptionsTableVersion != roleOptionsTableVersion {
		// Update role_options table version and drop the map.
		a.roleOptionsTableVersion = roleOptionsTableVersion
		a.cache = make(map[security.SQLUsername]AuthInfo)
		a.settingsCache = make(map[SettingsCacheKey][]string)
		a.boundAccount.Empty(ctx)
	}
	if a.dbRoleSettingsTableVersion != dbRoleSettingsTableVersion {
		// Update database_role_settings table version and drop the map.
		a.dbRoleSettingsTableVersion = dbRoleSettingsTableVersion
		a.cache = make(map[security.SQLUsername]AuthInfo)
		a.settingsCache = make(map[SettingsCacheKey][]string)
		a.boundAccount.Empty(ctx)
	}
}

// writeBackToCache tries to put the fetched data into the cache, and returns
// true if it succeeded. If the underlying system tables have been modified
// since they were read, the cache is not updated, and the passed in table
// descriptor versions are all updated.
func (a *AuthInfoCache) writeBackToCache(
	ctx context.Context,
	usersTableVersion *descpb.DescriptorVersion,
	roleOptionsTableVersion *descpb.DescriptorVersion,
	dbRoleSettingsTableVersion *descpb.DescriptorVersion,
	aInfo AuthInfo,
	settingsEntries []SettingsCacheEntry,
	username security.SQLUsername,
) bool {
	return func() bool {
		a.Lock()
		defer a.Unlock()
		// Table version has changed while we were looking: unlock and start over.
		if a.usersTableVersion != *usersTableVersion ||
			a.roleOptionsTableVersion != *roleOptionsTableVersion ||
			a.dbRoleSettingsTableVersion != *dbRoleSettingsTableVersion {
			*usersTableVersion = a.usersTableVersion
			*roleOptionsTableVersion = a.roleOptionsTableVersion
			*dbRoleSettingsTableVersion = a.dbRoleSettingsTableVersion
			return false
		}

		// Table version remains the same: update map, unlock, return.
		const sizeOfUsername = int(unsafe.Sizeof(security.SQLUsername{}))
		const sizeOfAuthInfo = int(unsafe.Sizeof(AuthInfo{}))
		const sizeOfTimestamp = int(unsafe.Sizeof(tree.DTimestamp{}))
		const sizeOfSettingsCacheKey = int(unsafe.Sizeof(SettingsCacheKey{}))
		const sizeOfSliceOverHead = int(unsafe.Sizeof([]string{}))
		sizeOfSettings := 0
		for _, sEntry := range settingsEntries {
			if _, ok := a.settingsCache[sEntry.SettingsCacheKey]; ok {
				// Avoid double-counting memory if a key is already in the cache.
				continue
			}
			sizeOfSettings += sizeOfSettingsCacheKey
			sizeOfSettings += sizeOfSliceOverHead
			sizeOfSettings += len(sEntry.SettingsCacheKey.Username.Normalized())
			for _, s := range sEntry.Settings {
				sizeOfSettings += len(s)
			}
		}
		sizeOfEntry := sizeOfUsername + len(username.Normalized()) +
			sizeOfAuthInfo + len(aInfo.HashedPassword) +
			sizeOfTimestamp + sizeOfSettings
		if err := a.boundAccount.Grow(ctx, int64(sizeOfEntry)); err != nil {
			// If there is no memory available to cache the entry, we can still
			// proceed with authentication so that users are not locked out of
			// the database.
			log.Ops.Warningf(ctx, "no memory available to cache authentication info: %v", err)
		} else {
			a.cache[username] = aInfo
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

// defaultDatabaseID is used in the settingsCache for entries that should
// apply to all database.
const defaultDatabaseID = 0

// defaultUsername is used in the settingsCache for entries that should
// apply to all roles.
var defaultUsername = security.MakeSQLUsernameFromPreNormalizedString("")

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
