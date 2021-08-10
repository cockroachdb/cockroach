// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessioninit"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// GetUserSessionInitInfo determines if the given user exists and
// also returns a password retrieval function, other authentication-related
// information, and default session variable settings that are to be applied
// before a SQL session is created.
//
// The caller is responsible for normalizing the username.
// (CockroachDB has case-insensitive usernames, unlike PostgreSQL.)
//
// The function is tolerant of unavailable clusters (or unavailable
// system database) as follows:
//
// - if the user is root, the user is reported to exist immediately
//   without querying system.users at all. The password retrieval
//   is delayed until actually needed by the authentication method.
//   This way, if the client presents a valid TLS certificate
//   the password is not even needed at all. This is useful for e.g.
//   `cockroach node status`.
//
//   If root is forced to use a password (e.g. logging in onto the UI)
//   then a user login timeout greater than 5 seconds is also
//   ignored. This ensures that root has a modicum of comfort
//   logging into an unavailable cluster.
//
//   TODO(knz): this does not yet quite work because even if the pw
//   auth on the UI succeeds writing to system.web_sessions will still
//   stall on an unavailable cluster and prevent root from logging in.
//
// - if the user is another user than root, then the function fails
//   after a timeout instead of blocking. The timeout is configurable
//   via the cluster setting server.user_login.timeout. Note that this
//   is a single timeout for looking up the password, role options, and
//   default session variable settings.
//
// - there is a cache for the the information from system.users,
//   system.role_options, and system.database_role_settings. As long as the
//   lookup succeeded before and there haven't been any CREATE/ALTER/DROP ROLE
//   commands since, then the cache is used without a KV lookup.
func GetUserSessionInitInfo(
	ctx context.Context,
	execCfg *ExecutorConfig,
	ie *InternalExecutor,
	username security.SQLUsername,
	databaseName string,
) (
	exists bool,
	canLogin bool,
	validUntil *tree.DTimestamp,
	defaultSettings []sessioninit.SettingsCacheEntry,
	pwRetrieveFn func(ctx context.Context) (hashedPassword []byte, err error),
	err error,
) {
	if username.IsRootUser() {
		// As explained above, for root we report that the user exists
		// immediately, and delay retrieving the password until strictly
		// necessary.
		rootFn := func(ctx context.Context) ([]byte, error) {
			authInfo, _, err := retrieveSessionInitInfoWithCache(ctx, execCfg, ie, username, databaseName)
			return authInfo.HashedPassword, err
		}

		// Root user cannot have password expiry and must have login.
		// It also never has default settings applied to it.
		return true, true, nil, nil, rootFn, nil
	}

	// Other users must reach for system.users no matter what, because
	// only that contains the truth about whether the user exists.
	authInfo, settingsEntries, err := retrieveSessionInitInfoWithCache(
		ctx, execCfg, ie, username, databaseName,
	)
	return authInfo.UserExists,
		authInfo.CanLogin,
		authInfo.ValidUntil,
		settingsEntries,
		func(ctx context.Context) ([]byte, error) {
			return authInfo.HashedPassword, nil
		},
		err
}

func retrieveSessionInitInfoWithCache(
	ctx context.Context,
	execCfg *ExecutorConfig,
	ie *InternalExecutor,
	username security.SQLUsername,
	databaseName string,
) (aInfo sessioninit.AuthInfo, settingsEntries []sessioninit.SettingsCacheEntry, err error) {
	// We may be operating with a timeout.
	timeout := userLoginTimeout.Get(&ie.s.cfg.Settings.SV)
	// We don't like long timeouts for root.
	// (4.5 seconds to not exceed the default 5s timeout configured in many clients.)
	const maxRootTimeout = 4*time.Second + 500*time.Millisecond
	if username.IsRootUser() && (timeout == 0 || timeout > maxRootTimeout) {
		timeout = maxRootTimeout
	}

	runFn := func(fn func(ctx context.Context) error) error { return fn(ctx) }
	if timeout != 0 {
		runFn = func(fn func(ctx context.Context) error) error {
			return contextutil.RunWithTimeout(ctx, "get-user-timeout", timeout, fn)
		}
	}
	err = runFn(func(ctx context.Context) (retErr error) {
		aInfo, retErr = execCfg.SessionInitCache.GetAuthInfo(
			ctx,
			execCfg.Settings,
			ie,
			execCfg.DB,
			execCfg.CollectionFactory,
			username,
			retrieveAuthInfo,
		)
		if retErr != nil {
			return retErr
		}
		// Avoid looking up default settings for root and non-existent users.
		if username.IsRootUser() || !aInfo.UserExists {
			return nil
		}
		settingsEntries, retErr = execCfg.SessionInitCache.GetDefaultSettings(
			ctx,
			execCfg.Settings,
			ie,
			execCfg.DB,
			execCfg.CollectionFactory,
			username,
			databaseName,
			retrieveDefaultSettings,
		)
		return retErr
	})

	if err != nil {
		// Failed to retrieve the user account. Report in logs for later investigation.
		log.Warningf(ctx, "user lookup for %q failed: %v", username, err)
		err = errors.Wrap(errors.Handled(err), "internal error while retrieving user account")
	}
	return aInfo, settingsEntries, err
}

func retrieveAuthInfo(
	ctx context.Context, txn *kv.Txn, ie sqlutil.InternalExecutor, username security.SQLUsername,
) (aInfo sessioninit.AuthInfo, retErr error) {
	// Use fully qualified table name to avoid looking up "".system.users.
	const getHashedPassword = `SELECT "hashedPassword" FROM system.public.users ` +
		`WHERE username=$1`
	values, err := ie.QueryRowEx(
		ctx, "get-hashed-pwd", txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		getHashedPassword, username)
	if err != nil {
		return sessioninit.AuthInfo{}, errors.Wrapf(err, "error looking up user %s", username)
	}
	if values != nil {
		aInfo.UserExists = true
		if v := values[0]; v != tree.DNull {
			aInfo.HashedPassword = []byte(*(v.(*tree.DBytes)))
		}
	}

	if !aInfo.UserExists {
		return sessioninit.AuthInfo{}, nil
	}

	// None of the rest of the role options are relevant for root.
	if username.IsRootUser() {
		return aInfo, nil
	}

	// Use fully qualified table name to avoid looking up "".system.role_options.
	const getLoginDependencies = `SELECT option, value FROM system.public.role_options ` +
		`WHERE username=$1 AND option IN ('NOLOGIN', 'VALID UNTIL')`

	roleOptsIt, err := ie.QueryIteratorEx(
		ctx, "get-login-dependencies", txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		getLoginDependencies,
		username,
	)
	if err != nil {
		return sessioninit.AuthInfo{}, errors.Wrapf(err, "error looking up user %s", username)
	}
	// We have to make sure to close the iterator since we might return from
	// the for loop early (before Next() returns false).
	defer func() { retErr = errors.CombineErrors(retErr, roleOptsIt.Close()) }()

	// To support users created before 20.1, allow all USERS/ROLES to login
	// if NOLOGIN is not found.
	aInfo.CanLogin = true
	var ok bool
	for ok, err = roleOptsIt.Next(ctx); ok; ok, err = roleOptsIt.Next(ctx) {
		row := roleOptsIt.Cur()
		option := string(tree.MustBeDString(row[0]))

		if option == "NOLOGIN" {
			aInfo.CanLogin = false
		}

		if option == "VALID UNTIL" {
			if tree.DNull.Compare(nil, row[1]) != 0 {
				ts := string(tree.MustBeDString(row[1]))
				// This is okay because the VALID UNTIL is stored as a string
				// representation of a TimestampTZ which has the same underlying
				// representation in the table as a Timestamp (UTC time).
				timeCtx := tree.NewParseTimeContext(timeutil.Now())
				aInfo.ValidUntil, _, err = tree.ParseDTimestamp(timeCtx, ts, time.Microsecond)
				if err != nil {
					return sessioninit.AuthInfo{}, errors.Wrap(err,
						"error trying to parse timestamp while retrieving password valid until value")
				}
			}
		}
	}

	return aInfo, err
}

func retrieveDefaultSettings(
	ctx context.Context,
	txn *kv.Txn,
	ie sqlutil.InternalExecutor,
	username security.SQLUsername,
	databaseID descpb.ID,
) (settingsEntries []sessioninit.SettingsCacheEntry, retErr error) {
	// Add an empty slice for all the keys so that something gets cached and
	// prevents a lookup for the same key from happening later.
	keys := sessioninit.GenerateSettingsCacheKeys(databaseID, username)
	settingsEntries = make([]sessioninit.SettingsCacheEntry, len(keys))
	for i, k := range keys {
		settingsEntries[i] = sessioninit.SettingsCacheEntry{
			SettingsCacheKey: k,
			Settings:         []string{},
		}
	}

	// The default settings are not relevant for root.
	if username.IsRootUser() {
		return settingsEntries, nil
	}

	// Use fully qualified table name to avoid looking up "".system.role_options.
	const getDefaultSettings = `
SELECT
  database_id, role_name, settings
FROM
  system.public.database_role_settings
WHERE
  (database_id = 0 AND role_name = $1)
  OR (database_id = $2 AND role_name = $1)
  OR (database_id = $2 AND role_name = '')
  OR (database_id = 0 AND role_name = '');
`
	defaultSettingsIt, err := ie.QueryIteratorEx(
		ctx, "get-default-settings", txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		getDefaultSettings,
		username,
		databaseID,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "error looking up user %s", username)
	}
	// We have to make sure to close the iterator since we might return from
	// the for loop early (before Next() returns false).
	defer func() { retErr = errors.CombineErrors(retErr, defaultSettingsIt.Close()) }()

	var ok bool
	for ok, err = defaultSettingsIt.Next(ctx); ok; ok, err = defaultSettingsIt.Next(ctx) {
		row := defaultSettingsIt.Cur()
		fetechedDatabaseID := descpb.ID(tree.MustBeDOid(row[0]).DInt)
		fetchedUsername := security.MakeSQLUsernameFromPreNormalizedString(string(tree.MustBeDString(row[1])))
		settingsDatum := tree.MustBeDArray(row[2])
		fetchedSettings := make([]string, settingsDatum.Len())
		for i, s := range settingsDatum.Array {
			fetchedSettings[i] = string(tree.MustBeDString(s))
		}

		thisKey := sessioninit.SettingsCacheKey{
			DatabaseID: fetechedDatabaseID,
			Username:   fetchedUsername,
		}
		// Add the result to the settings list. Note that we don't use a map
		// because the list is in order of precedence.
		for i, s := range settingsEntries {
			if s.SettingsCacheKey == thisKey {
				settingsEntries[i].Settings = fetchedSettings
			}
		}
	}

	return settingsEntries, err
}

var userLoginTimeout = settings.RegisterDurationSetting(
	"server.user_login.timeout",
	"timeout after which client authentication times out if some system range is unavailable (0 = no timeout)",
	10*time.Second,
	settings.NonNegativeDuration,
).WithPublic()

// GetAllRoles returns a "set" (map) of Roles -> true.
func (p *planner) GetAllRoles(ctx context.Context) (map[security.SQLUsername]bool, error) {
	query := `SELECT username FROM system.users`
	it, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryIteratorEx(
		ctx, "read-users", p.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		query)
	if err != nil {
		return nil, err
	}

	users := make(map[security.SQLUsername]bool)
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		username := tree.MustBeDString(it.Cur()[0])
		// The usernames in system.users are already normalized.
		users[security.MakeSQLUsernameFromPreNormalizedString(string(username))] = true
	}
	if err != nil {
		return nil, err
	}
	return users, nil
}

// RoleExists returns true if the role exists.
func RoleExists(
	ctx context.Context, execCfg *ExecutorConfig, txn *kv.Txn, role security.SQLUsername,
) (bool, error) {
	query := `SELECT username FROM system.users WHERE username = $1`
	row, err := execCfg.InternalExecutor.QueryRowEx(
		ctx, "read-users", txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		query, role,
	)
	if err != nil {
		return false, err
	}

	return row != nil, nil
}

var roleMembersTableName = tree.MakeTableNameWithSchema("system", tree.PublicSchemaName, "role_members")

// BumpRoleMembershipTableVersion increases the table version for the
// role membership table.
func (p *planner) BumpRoleMembershipTableVersion(ctx context.Context) error {
	_, tableDesc, err := p.ResolveMutableTableDescriptor(ctx, &roleMembersTableName, true, tree.ResolveAnyTableKind)
	if err != nil {
		return err
	}

	return p.writeSchemaChange(
		ctx, tableDesc, descpb.InvalidMutationID, "updating version for role membership table",
	)
}

// bumpUsersTableVersion increases the table version for the
// users table.
func (p *planner) bumpUsersTableVersion(ctx context.Context) error {
	_, tableDesc, err := p.ResolveMutableTableDescriptor(ctx, sessioninit.UsersTableName, true, tree.ResolveAnyTableKind)
	if err != nil {
		return err
	}

	return p.writeSchemaChange(
		ctx, tableDesc, descpb.InvalidMutationID, "updating version for users table",
	)
}

// bumpRoleOptionsTableVersion increases the table version for the
// role_options table.
func (p *planner) bumpRoleOptionsTableVersion(ctx context.Context) error {
	_, tableDesc, err := p.ResolveMutableTableDescriptor(ctx, sessioninit.RoleOptionsTableName, true, tree.ResolveAnyTableKind)
	if err != nil {
		return err
	}

	return p.writeSchemaChange(
		ctx, tableDesc, descpb.InvalidMutationID, "updating version for role options table",
	)
}

// bumpDatabaseRoleSettingsTableVersion increases the table version for the
// database_role_settings table.
func (p *planner) bumpDatabaseRoleSettingsTableVersion(ctx context.Context) error {
	_, tableDesc, err := p.ResolveMutableTableDescriptor(ctx, sessioninit.DatabaseRoleSettingsTableName, true, tree.ResolveAnyTableKind)
	if err != nil {
		return err
	}

	return p.writeSchemaChange(
		ctx, tableDesc, descpb.InvalidMutationID, "updating version for database_role_settings table",
	)
}
