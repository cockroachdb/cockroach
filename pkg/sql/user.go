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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessioninit"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
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
	canLoginSQL bool,
	canLoginDBConsole bool,
	isSuperuser bool,
	defaultSettings []sessioninit.SettingsCacheEntry,
	pwRetrieveFn func(ctx context.Context) (expired bool, hashedPassword security.PasswordHash, err error),
	err error,
) {
	runFn := getUserInfoRunFn(execCfg, username, "get-user-timeout")

	if username.IsRootUser() {
		// As explained above, for root we report that the user exists
		// immediately, and delay retrieving the password until strictly
		// necessary.
		rootFn := func(ctx context.Context) (expired bool, ret security.PasswordHash, err error) {
			err = runFn(ctx, func(ctx context.Context) error {
				authInfo, _, err := retrieveSessionInitInfoWithCache(ctx, execCfg, ie, username, databaseName)
				if err != nil {
					return err
				}
				ret = authInfo.HashedPassword
				return nil
			})
			if ret == nil {
				ret = security.MissingPasswordHash
			}
			// NB: Root user password does not expire.
			return false /* expired */, ret, err
		}

		// Root user cannot have password expiry and must have login.
		// It also never has default settings applied to it.
		return true, true, true, true, nil, rootFn, nil
	}

	var authInfo sessioninit.AuthInfo
	var settingsEntries []sessioninit.SettingsCacheEntry

	if err = runFn(ctx, func(ctx context.Context) error {
		// Other users must reach for system.users no matter what, because
		// only that contains the truth about whether the user exists.
		authInfo, settingsEntries, err = retrieveSessionInitInfoWithCache(
			ctx, execCfg, ie, username, databaseName,
		)
		if err != nil {
			return err
		}

		// Find whether the user is an admin.
		return execCfg.CollectionFactory.Txn(
			ctx,
			ie,
			execCfg.DB,
			func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
				memberships, err := MemberOfWithAdminOption(
					ctx,
					execCfg,
					ie,
					descsCol,
					txn,
					username,
				)
				if err != nil {
					return err
				}
				_, isSuperuser = memberships[security.AdminRoleName()]
				return nil
			},
		)
	}); err != nil {
		log.Warningf(ctx, "user membership lookup for %q failed: %v", username, err)
		err = errors.Wrap(errors.Handled(err), "internal error while retrieving user account memberships")
	}

	return authInfo.UserExists,
		authInfo.CanLoginSQL,
		authInfo.CanLoginDBConsole,
		isSuperuser,
		settingsEntries,
		func(ctx context.Context) (expired bool, ret security.PasswordHash, err error) {
			ret = authInfo.HashedPassword
			if authInfo.ValidUntil != nil {
				// NB: we compute the expiration as late as possible,
				// to ensure that we determine the expiration relative
				// to the time at which the client presents the password
				// to the server (and not earlier).
				if authInfo.ValidUntil.Time.Sub(timeutil.Now()) < 0 {
					expired = true
					ret = nil
				}
			}
			if ret == nil {
				ret = security.MissingPasswordHash
			}
			return expired, ret, nil
		},
		err
}

func getUserInfoRunFn(
	execCfg *ExecutorConfig, username security.SQLUsername, opName string,
) func(context.Context, func(context.Context) error) error {
	// We may be operating with a timeout.
	timeout := userLoginTimeout.Get(&execCfg.Settings.SV)
	// We don't like long timeouts for root.
	// (4.5 seconds to not exceed the default 5s timeout configured in many clients.)
	const maxRootTimeout = 4*time.Second + 500*time.Millisecond
	if username.IsRootUser() && (timeout == 0 || timeout > maxRootTimeout) {
		timeout = maxRootTimeout
	}

	runFn := func(ctx context.Context, fn func(ctx context.Context) error) error { return fn(ctx) }
	if timeout != 0 {
		runFn = func(ctx context.Context, fn func(ctx context.Context) error) error {
			return contextutil.RunWithTimeout(ctx, opName, timeout, fn)
		}
	}
	return runFn
}

func retrieveSessionInitInfoWithCache(
	ctx context.Context,
	execCfg *ExecutorConfig,
	ie *InternalExecutor,
	username security.SQLUsername,
	databaseName string,
) (aInfo sessioninit.AuthInfo, settingsEntries []sessioninit.SettingsCacheEntry, err error) {
	if err = func() (retErr error) {
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
	}(); err != nil {
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
		return aInfo, errors.Wrapf(err, "error looking up user %s", username)
	}
	var hashedPassword []byte
	if values != nil {
		aInfo.UserExists = true
		if v := values[0]; v != tree.DNull {
			hashedPassword = []byte(*(v.(*tree.DBytes)))
		}
	}
	aInfo.HashedPassword = security.LoadPasswordHash(ctx, hashedPassword)

	if !aInfo.UserExists {
		return aInfo, nil
	}

	// None of the rest of the role options are relevant for root.
	if username.IsRootUser() {
		return aInfo, nil
	}

	// Use fully qualified table name to avoid looking up "".system.role_options.
	const getLoginDependencies = `SELECT option, value FROM system.public.role_options ` +
		`WHERE username=$1 AND option IN ('NOLOGIN', 'VALID UNTIL', 'NOSQLLOGIN')`

	roleOptsIt, err := ie.QueryIteratorEx(
		ctx, "get-login-dependencies", txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		getLoginDependencies,
		username,
	)
	if err != nil {
		return aInfo, errors.Wrapf(err, "error looking up user %s", username)
	}
	// We have to make sure to close the iterator since we might return from
	// the for loop early (before Next() returns false).
	defer func() { retErr = errors.CombineErrors(retErr, roleOptsIt.Close()) }()

	// To support users created before 20.1, allow all USERS/ROLES to login
	// if NOLOGIN is not found.
	aInfo.CanLoginSQL = true
	aInfo.CanLoginDBConsole = true
	var ok bool
	for ok, err = roleOptsIt.Next(ctx); ok; ok, err = roleOptsIt.Next(ctx) {
		row := roleOptsIt.Cur()
		option := string(tree.MustBeDString(row[0]))

		if option == "NOLOGIN" {
			aInfo.CanLoginSQL = false
			aInfo.CanLoginDBConsole = false
		}
		if option == "NOSQLLOGIN" {
			aInfo.CanLoginSQL = false
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
					return aInfo, errors.Wrap(err,
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
	settings.TenantWritable,
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
func (p *planner) RoleExists(ctx context.Context, role security.SQLUsername) (bool, error) {
	return RoleExists(ctx, p.ExecCfg(), p.Txn(), role)
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

func (p *planner) setRole(ctx context.Context, local bool, s security.SQLUsername) error {
	sessionUser := p.SessionData().SessionUser()
	becomeUser := sessionUser
	// Check the role exists - if so, populate becomeUser.
	if !s.IsNoneRole() {
		becomeUser = s

		exists, err := p.RoleExists(ctx, becomeUser)
		if err != nil {
			return err
		}
		if !exists {
			return pgerror.Newf(
				pgcode.InvalidParameterValue,
				"role %s does not exist",
				becomeUser.Normalized(),
			)
		}
	}

	if err := p.checkCanBecomeUser(ctx, becomeUser); err != nil {
		return err
	}

	// Buffer the ParamStatusUpdate. We must *always* send this on an update,
	// so we can't short circuit.
	updateStr := "off"
	willBecomeAdmin, err := p.UserHasAdminRole(ctx, becomeUser)
	if err != nil {
		return err
	}
	if willBecomeAdmin {
		updateStr = "on"
	}

	return p.applyOnSessionDataMutators(
		ctx,
		local,
		func(m sessionDataMutator) error {
			m.data.IsSuperuser = willBecomeAdmin
			m.bufferParamStatusUpdate("is_superuser", updateStr)

			// The "none" user does resets the SessionUserProto in a SET ROLE.
			if becomeUser.IsNoneRole() {
				if m.data.SessionUserProto.Decode().Normalized() != "" {
					m.data.UserProto = m.data.SessionUserProto
					m.data.SessionUserProto = ""
				}
				m.data.SearchPath = m.data.SearchPath.WithUserSchemaName(m.data.User().Normalized())
				return nil
			}

			// Only update session_user when we are transitioning from the current_user
			// being the session_user.
			if m.data.SessionUserProto == "" {
				m.data.SessionUserProto = m.data.UserProto
			}
			m.data.UserProto = becomeUser.EncodeProto()
			m.data.SearchPath = m.data.SearchPath.WithUserSchemaName(m.data.User().Normalized())
			return nil
		},
	)

}

// checkCanBecomeUser returns an error if the SessionUser cannot become the
// becomeUser.
func (p *planner) checkCanBecomeUser(ctx context.Context, becomeUser security.SQLUsername) error {
	sessionUser := p.SessionData().SessionUser()

	// Switching to None can always succeed.
	if becomeUser.IsNoneRole() {
		return nil
	}
	// Root users are able to become anyone.
	if sessionUser.IsRootUser() {
		return nil
	}
	// You can always become yourself.
	if becomeUser.Normalized() == sessionUser.Normalized() {
		return nil
	}
	// Only root can become root.
	// This is a CockroachDB specialization of the superuser case, as we don't want
	// to allow admins to become root in the tenant case, where only system
	// admins can be the root user.
	if becomeUser.IsRootUser() {
		return pgerror.Newf(
			pgcode.InsufficientPrivilege,
			"only root can become root",
		)
	}

	memberships, err := p.MemberOfWithAdminOption(ctx, sessionUser)
	if err != nil {
		return err
	}
	// Superusers can become anyone except root. In CRDB, admins are superusers.
	if _, ok := memberships[security.AdminRoleName()]; ok {
		return nil
	}
	// Otherwise, check the session user is a member of the user they will become.
	if _, ok := memberships[becomeUser]; !ok {
		return pgerror.Newf(
			pgcode.InsufficientPrivilege,
			`permission denied to set role "%s"`,
			becomeUser.Normalized(),
		)
	}
	return nil
}

// MaybeUpgradeStoredPasswordHash attempts to convert a stored hash
// that was encoded using crdb-bcrypt, to the SCRAM-SHA-256 format.
//
// This auto-conversion is a CockroachDB-specific feature, which
// pushes clusters upgraded from a previous version into using
// SCRAM-SHA-256.
//
// The caller is responsible for ensuring this function is only called
// after a successful authentication, that is, the provided cleartext
// password is known to match the previously-encoded prevHash.
func MaybeUpgradeStoredPasswordHash(
	ctx context.Context,
	execCfg *ExecutorConfig,
	username security.SQLUsername,
	cleartext string,
	currentHash security.PasswordHash,
) {
	// This call also checks whether the conversion has been disabled by
	// configuration.
	converted, prevHash, newHash, newMethod, err := security.MaybeUpgradePasswordHash(ctx, &execCfg.Settings.SV, cleartext, currentHash)
	if err != nil {
		// We're not returning an error: clients should not be refused a
		// session just because a password conversion failed.
		//
		// Simply explain what happened in logs for troubleshooting.
		log.Warningf(ctx, "password hash conversion failed: %+v", err)
		return
	} else if !converted {
		// No conversion happening. Nothing to do.
		return
	}

	// The password hash was successfully converted. Store the new hash.
	if err := updateUserPasswordHash(ctx, execCfg, username, prevHash, newHash); err != nil {
		// Again, we don't want to fail with an error, because at this
		// point authentication succeeded.
		//
		// Simply explain what happened in logs for troubleshooting.
		log.Warningf(ctx, "storing the new password hash after conversion failed: %+v", err)
	} else {
		// Inform the security audit log that the hash was upgraded.
		log.StructuredEvent(ctx, &eventpb.PasswordHashConverted{
			RoleName:  username.Normalized(),
			OldMethod: currentHash.Method().String(),
			NewMethod: newMethod,
		})
	}
}

func updateUserPasswordHash(
	ctx context.Context,
	execCfg *ExecutorConfig,
	username security.SQLUsername,
	prevHash, newHash []byte,
) error {
	runFn := getUserInfoRunFn(execCfg, username, "set-hash-timeout")

	return runFn(ctx, func(ctx context.Context) error {
		return DescsTxn(ctx, execCfg, func(ctx context.Context, txn *kv.Txn, d *descs.Collection) error {
			// NB: we cannot use ALTER USER ... WITH PASSWORD here,
			// because it is not guaranteed to recognize the hash in the
			// WITH PASSWORD clause.
			// (The detection could be disabled via cluster setting
			// server.user_login.store_client_pre_hashed_passwords.enabled)
			//
			// So instead we write to system.users and bump the version to
			// invalidate the cache manually.
			//
			// The motivation for the "WHERE hashedPassword = prevHash"
			// clause and the check for rowsAffected is to protect against
			// two hazards:
			//
			// - a race condition where an ALTER USER WITH PASSWORD is
			//   executed by an administrator concurrently with a user
			//   login. Without WHERE, this could mistakenly override the
			//   new password.
			//
			// - multiple concurrent logins by the same user, triggering
			//   the same password upgrade for all of them. Without WHERE,
			//   we'd be writing to system.users for all of them and queue
			//   potentially many schema updates, creating a bottleneck.
			//
			rowsAffected, err := execCfg.InternalExecutor.Exec(
				ctx,
				"set-password-hash",
				txn,
				`UPDATE system.users SET "hashedPassword" = $3 WHERE username = $1 AND "hashedPassword" = $2`,
				username.Normalized(),
				prevHash,
				newHash,
			)
			if err != nil || rowsAffected == 0 {
				// Error, or no update took place.
				return err
			}
			usersTable, err := d.GetMutableTableByID(
				ctx, txn, keys.UsersTableID, tree.ObjectLookupFlagsWithRequired(),
			)
			if err != nil {
				return err
			}
			// WriteDesc will internally bump the version.
			return d.WriteDesc(ctx, false /* kvTrace */, usersTable, txn)
		})
	})
}
