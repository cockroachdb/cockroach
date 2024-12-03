// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sessioninit

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/password"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/go-ldap/ldap/v3"
)

// CacheEnabledSettingName is the name of the CacheEnabled cluster setting.
const CacheEnabledSettingName = "server.authentication_cache.enabled"

// CacheEnabled is a cluster setting that determines if the
// sessioninit.Cache and associated logic is enabled.
var CacheEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	CacheEnabledSettingName,
	"enables a cache used during authentication to avoid lookups to system tables "+
		"when retrieving per-user authentication-related information",
	true,
	settings.WithPublic)

// Cache is a shared cache for hashed passwords and other information used
// during user authentication and session initialization.
type Cache struct {
	syncutil.Mutex
	usersTableVersion          descpb.DescriptorVersion
	roleOptionsTableVersion    descpb.DescriptorVersion
	dbRoleSettingsTableVersion descpb.DescriptorVersion
	boundAccount               mon.BoundAccount
	// authInfoCache is a mapping from username to AuthInfo.
	authInfoCache map[username.SQLUsername]AuthInfo
	// settingsCache is a mapping from (dbID, username) to default settings.
	settingsCache map[SettingsCacheKey][]string
	// populateCacheGroup is used to ensure that there is at most one in-flight
	// request for populating each cache entry.

	populateCacheGroup *singleflight.Group
	stopper            *stop.Stopper
}

// AuthInfo contains data that is used to perform an authentication attempt.
type AuthInfo struct {
	// UserExists is set to true if the user has a row in system.users.
	UserExists bool
	// CanLoginSQLRoleOpt is set to false if the user has the NOLOGIN or NOSQLLOGIN role option.
	CanLoginSQLRoleOpt bool
	// CanLoginDBConsoleRoleOpt is set to false if the user has NOLOGIN role option.
	CanLoginDBConsoleRoleOpt bool
	// CanUseReplicationRoleOpt is set to true if the user has the REPLICATION role option.
	CanUseReplicationRoleOpt bool
	// HashedPassword is the hashed password and can be nil.
	HashedPassword password.PasswordHash
	// ValidUntil is the VALID UNTIL role option.
	ValidUntil *tree.DTimestamp
	// Subject is the SUBJECT role option. It is used to match the subject
	// distinguished name in a client certificate.
	Subject *ldap.DN
}

// SettingsCacheKey is the key used for the settingsCache.
type SettingsCacheKey struct {
	DatabaseID descpb.ID
	Username   username.SQLUsername
}

// SettingsCacheEntry represents an entry in the settingsCache. It is
// used so that the entries can be returned in a stable order.
type SettingsCacheEntry struct {
	SettingsCacheKey
	Settings []string
}

// NewCache initializes a new sessioninit.Cache.
func NewCache(account mon.BoundAccount, stopper *stop.Stopper) *Cache {
	return &Cache{
		boundAccount:       account,
		populateCacheGroup: singleflight.NewGroup("load-value", "key"),
		stopper:            stopper,
	}
}

// GetAuthInfo consults the sessioninit.Cache and returns the AuthInfo for the
// provided username and databaseName. If the information is not in the cache,
// or if the underlying tables have changed since the cache was populated,
// then the readFromSystemTables callback is used to load new data.
func (a *Cache) GetAuthInfo(
	ctx context.Context,
	settings *cluster.Settings,
	db descs.DB,
	username username.SQLUsername,
	readFromSystemTables func(
		ctx context.Context,
		db descs.DB,
		username username.SQLUsername,
	) (AuthInfo, error),
) (aInfo AuthInfo, err error) {
	if !CacheEnabled.Get(&settings.SV) {
		return readFromSystemTables(ctx, db, username)
	}

	var usersTableDesc catalog.TableDescriptor
	var roleOptionsTableDesc catalog.TableDescriptor
	err = db.DescsTxn(ctx, func(
		ctx context.Context, txn descs.Txn,
	) error {
		// When running on a PCR reader catalog we need to ensure all descriptors
		// have matching timestamps for external row data. Certain system tables
		// like users and role_options are replicated, which can cause mixed
		// external row data timestamps, which can lead to a retryable error.
		// To avoid this we will set a replication safe AOST timestamp when running
		// on a reader catalog.
		if err := txn.Descriptors().MaybeSetReplicationSafeTS(ctx, txn.KV()); err != nil {
			return err
		}
		usersTableDesc, err = txn.Descriptors().ByIDWithLeased(txn.KV()).Get().Table(ctx, keys.UsersTableID)
		if err != nil {
			return err
		}
		roleOptionsTableDesc, err = txn.Descriptors().ByIDWithLeased(txn.KV()).Get().Table(ctx, keys.RoleOptionsTableID)
		return err
	})
	if err != nil {
		return AuthInfo{}, err
	}

	usersTableVersion := usersTableDesc.GetVersion()
	roleOptionsTableVersion := roleOptionsTableDesc.GetVersion()

	// Check version and maybe clear cache while holding the mutex.
	var found bool
	aInfo, found = a.readAuthInfoFromCache(ctx, usersTableVersion, roleOptionsTableVersion, username)

	if found {
		return aInfo, nil
	}

	// Lookup the data outside the lock. There will be at most one
	// request in-flight for each user. The user and role_options table
	// versions are also part of the request key so that we don't read data
	// from an old version of either table.
	val, err := a.loadValueOutsideOfCache(
		ctx, fmt.Sprintf("authinfo-%s-%d-%d", username.Normalized(), usersTableVersion, roleOptionsTableVersion),
		func(loadCtx context.Context) (interface{}, error) {
			authInfo, err := readFromSystemTables(loadCtx, db, username)
			if err != nil {
				return AuthInfo{}, err
			}
			// Write data back to the cache if the table version hasn't changed.
			a.maybeWriteAuthInfoBackToCache(
				ctx,
				usersTableVersion,
				roleOptionsTableVersion,
				authInfo,
				username,
			)
			return authInfo, nil
		},
	)
	if err != nil {
		return AuthInfo{}, err
	}
	aInfo = val.(AuthInfo)
	return aInfo, nil
}

func (a *Cache) readAuthInfoFromCache(
	ctx context.Context,
	usersTableVersion descpb.DescriptorVersion,
	roleOptionsTableVersion descpb.DescriptorVersion,
	username username.SQLUsername,
) (AuthInfo, bool) {
	a.Lock()
	defer a.Unlock()
	// We don't need to check dbRoleSettingsTableVersion here, so pass in the
	// one we already have.
	isEligibleForCache := a.clearCacheIfStale(ctx, usersTableVersion, roleOptionsTableVersion, a.dbRoleSettingsTableVersion)
	if !isEligibleForCache {
		return AuthInfo{}, false
	}
	ai, foundAuthInfo := a.authInfoCache[username]
	return ai, foundAuthInfo
}

// loadValueOutsideOfCache loads the value for the given requestKey using the provided
// function. It ensures that there is only at most one in-flight request for
// each key at any time.
func (a *Cache) loadValueOutsideOfCache(
	ctx context.Context, requestKey string, fn func(loadCtx context.Context) (interface{}, error),
) (interface{}, error) {
	future, _ := a.populateCacheGroup.DoChan(ctx,
		requestKey,
		singleflight.DoOpts{
			Stop:               a.stopper,
			InheritCancelation: false,
		},
		fn,
	)
	res := future.WaitForResult(ctx)
	if res.Err != nil {
		return AuthInfo{}, res.Err
	}
	return res.Val, nil
}

// maybeWriteAuthInfoBackToCache tries to put the fetched AuthInfo into the
// authInfoCache, and returns true if it succeeded. If the underlying system
// tables have been modified since they were read, the authInfoCache is not
// updated.
// Note that reading from system tables may give us data from a newer table
// version than the one we pass in here, that is okay since the cache will
// be invalidated upon the next read.
func (a *Cache) maybeWriteAuthInfoBackToCache(
	ctx context.Context,
	usersTableVersion descpb.DescriptorVersion,
	roleOptionsTableVersion descpb.DescriptorVersion,
	aInfo AuthInfo,
	user username.SQLUsername,
) {
	a.Lock()
	defer a.Unlock()
	// Table versions have changed while we were looking: don't cache the data.
	if a.usersTableVersion != usersTableVersion || a.roleOptionsTableVersion != roleOptionsTableVersion {
		return
	}
	// Table version remains the same: update map, unlock, return.
	const sizeOfUsername = int(unsafe.Sizeof(username.SQLUsername{}))
	const sizeOfAuthInfo = int(unsafe.Sizeof(AuthInfo{}))
	const sizeOfTimestamp = int(unsafe.Sizeof(tree.DTimestamp{}))

	hpSize := 0
	if aInfo.HashedPassword != nil {
		hpSize = aInfo.HashedPassword.Size()
	}
	subjectSize := 0
	if aInfo.Subject != nil {
		for _, rdn := range aInfo.Subject.RDNs {
			for _, attr := range rdn.Attributes {
				subjectSize += len(attr.Type)
				subjectSize += len(attr.Value)
			}
		}
	}

	sizeOfEntry := sizeOfUsername + len(user.Normalized()) +
		sizeOfAuthInfo + hpSize + sizeOfTimestamp + subjectSize
	if err := a.boundAccount.Grow(ctx, int64(sizeOfEntry)); err != nil {
		// If there is no memory available to cache the entry, we can still
		// proceed with authentication so that users are not locked out of
		// the database.
		log.Ops.Warningf(ctx, "no memory available to cache authentication info: %v", err)
		return
	}
	a.authInfoCache[user] = aInfo
}

// GetDefaultSettings consults the sessioninit.Cache and returns the list of
// SettingsCacheEntry for the provided username and databaseName. If the
// information is not in the cache, or if the underlying tables have changed
// since the cache was populated, then the readFromSystemTables callback is
// used to load new data.
func (a *Cache) GetDefaultSettings(
	ctx context.Context,
	settings *cluster.Settings,
	db descs.DB,
	userName username.SQLUsername,
	databaseName string,
	readFromSystemTables func(
		ctx context.Context,
		f descs.DB,
		userName username.SQLUsername,
		databaseID descpb.ID,
	) ([]SettingsCacheEntry, error),
) (settingsEntries []SettingsCacheEntry, err error) {
	var dbRoleSettingsTableDesc catalog.TableDescriptor
	var databaseID descpb.ID
	err = db.DescsTxn(ctx, func(
		ctx context.Context, txn descs.Txn,
	) error {
		dbRoleSettingsTableDesc, err = txn.Descriptors().ByIDWithLeased(txn.KV()).Get().Table(ctx, keys.DatabaseRoleSettingsTableID)
		if err != nil {
			return err
		}
		databaseID = descpb.ID(0)
		if databaseName != "" {
			dbDesc, err := txn.Descriptors().ByNameWithLeased(txn.KV()).MaybeGet().Database(ctx, databaseName)
			if err != nil {
				return err
			}
			// If dbDesc is nil, the database name was not valid, but that should
			// not cause a login-preventing error.
			if dbDesc != nil {
				databaseID = dbDesc.GetID()
			} else {
				log.Sessions.Infof(ctx, "cannot connect to %s; database does not exist", databaseName)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// We can't check if the cache is disabled earlier, since we always need to
	// start the `CollectionFactory.Txn()` regardless in order to look up the
	// database descriptor ID.
	if !CacheEnabled.Get(&settings.SV) {
		settingsEntries, err = readFromSystemTables(
			ctx,
			db,
			userName,
			databaseID,
		)
		return settingsEntries, err
	}

	dbRoleSettingsTableVersion := dbRoleSettingsTableDesc.GetVersion()

	// Check version and maybe clear cache while holding the mutex.
	var found bool
	settingsEntries, found = a.readDefaultSettingsFromCache(ctx, dbRoleSettingsTableVersion, userName, databaseID)

	if found {
		return settingsEntries, nil
	}

	// Lookup the data outside the lock. There will be at most one request
	// in-flight for each user+database. The db_role_settings table version is
	// also part of the request key so that we don't read data from an old
	// version of the table.
	val, err := a.loadValueOutsideOfCache(
		ctx, fmt.Sprintf("defaultsettings-%s-%d-%d", userName.Normalized(), databaseID, dbRoleSettingsTableVersion),
		func(loadCtx context.Context) (interface{}, error) {
			defaultSettings, err := readFromSystemTables(loadCtx, db, userName, databaseID)
			if err != nil {
				return nil, err
			}
			// Write the fetched data back to the cache if the table version hasn't
			// changed.
			a.maybeWriteDefaultSettingsBackToCache(
				ctx,
				dbRoleSettingsTableVersion,
				defaultSettings,
			)
			return defaultSettings, nil
		},
	)
	if err != nil {
		return nil, err
	}
	settingsEntries = val.([]SettingsCacheEntry)
	return settingsEntries, nil
}

func (a *Cache) readDefaultSettingsFromCache(
	ctx context.Context,
	dbRoleSettingsTableVersion descpb.DescriptorVersion,
	userName username.SQLUsername,
	databaseID descpb.ID,
) ([]SettingsCacheEntry, bool) {
	a.Lock()
	defer a.Unlock()
	// We don't need to check usersTableVersion or roleOptionsTableVersion here,
	// so pass in the values we already have.
	isEligibleForCache := a.clearCacheIfStale(
		ctx, a.usersTableVersion, a.roleOptionsTableVersion, dbRoleSettingsTableVersion,
	)
	if !isEligibleForCache {
		return nil, false
	}
	foundAllDefaultSettings := true
	var sEntries []SettingsCacheEntry
	// Search through the cache for the settings entries we need. Since we look up
	// multiple entries in the cache, the same setting might appear multiple
	// times. Note that GenerateSettingsCacheKeys goes in order of precedence,
	// so the order of the returned []SettingsCacheEntry is important and the
	// caller must take care not to apply a setting if it has already appeared
	// earlier in the list.
	for _, k := range GenerateSettingsCacheKeys(databaseID, userName) {
		s, ok := a.settingsCache[k]
		if !ok {
			foundAllDefaultSettings = false
			break
		}
		sEntries = append(sEntries, SettingsCacheEntry{k, s})
	}
	return sEntries, foundAllDefaultSettings
}

// maybeWriteDefaultSettingsBackToCache tries to put the fetched SettingsCacheEntry
// list into the settingsCache, and returns true if it succeeded. If the
// underlying system tables have been modified since they were read, the
// settingsCache is not updated.
// Note that reading from system tables may give us data from a newer table
// version than the one we pass in here, that is okay since the cache will
// be invalidated upon the next read.
func (a *Cache) maybeWriteDefaultSettingsBackToCache(
	ctx context.Context,
	dbRoleSettingsTableVersion descpb.DescriptorVersion,
	settingsEntries []SettingsCacheEntry,
) {
	a.Lock()
	defer a.Unlock()
	// Table version has changed while we were looking: don't cache the data.
	if a.dbRoleSettingsTableVersion > dbRoleSettingsTableVersion {
		return
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
		return
	}
	for _, sEntry := range settingsEntries {
		// Avoid re-storing an existing key.
		if _, ok := a.settingsCache[sEntry.SettingsCacheKey]; !ok {
			a.settingsCache[sEntry.SettingsCacheKey] = sEntry.Settings
		}
	}
}

// clearCacheIfStale compares the cached table versions to the current table
// versions. If the cached versions are older, the cache is cleared. If the
// cached versions are newer, then false is returned to indicate that the
// cached data should not be used.
func (a *Cache) clearCacheIfStale(
	ctx context.Context,
	usersTableVersion descpb.DescriptorVersion,
	roleOptionsTableVersion descpb.DescriptorVersion,
	dbRoleSettingsTableVersion descpb.DescriptorVersion,
) (isEligibleForCache bool) {
	if a.usersTableVersion < usersTableVersion ||
		a.roleOptionsTableVersion < roleOptionsTableVersion ||
		a.dbRoleSettingsTableVersion < dbRoleSettingsTableVersion {
		// If the cache is based on old table versions, then update versions and
		// drop the map.
		a.usersTableVersion = usersTableVersion
		a.roleOptionsTableVersion = roleOptionsTableVersion
		a.dbRoleSettingsTableVersion = dbRoleSettingsTableVersion
		a.authInfoCache = make(map[username.SQLUsername]AuthInfo)
		a.settingsCache = make(map[SettingsCacheKey][]string)
		a.boundAccount.Empty(ctx)
	} else if a.usersTableVersion > usersTableVersion ||
		a.roleOptionsTableVersion > roleOptionsTableVersion ||
		a.dbRoleSettingsTableVersion > dbRoleSettingsTableVersion {
		// If the cache is based on newer table versions, then this transaction
		// should not use the cached data.
		return false
	}
	return true
}

// GenerateSettingsCacheKeys returns a slice of all the SettingsCacheKey
// that are relevant for the given databaseID and username. The slice is
// ordered in descending order of precedence.
func GenerateSettingsCacheKeys(
	databaseID descpb.ID, userName username.SQLUsername,
) []SettingsCacheKey {
	return []SettingsCacheKey{
		{
			DatabaseID: databaseID,
			Username:   userName,
		},
		{
			DatabaseID: defaultDatabaseID,
			Username:   userName,
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
