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
	"github.com/cockroachdb/cockroach/pkg/sql/authentication"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// GetUserHashedPassword determines if the given user exists and
// also returns a password retrieval function.
//
// The caller is responsible for normalizing the username.
// (CockroachDB has case-insensitive usernames, unlike PostgreSQL.)
//
// The function is tolerant of unavailable clusters (or unavailable
// system.user) as follows:
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
//   TODO(knz): this does not yet quite work becaus even if the pw
//   auth on the UI succeeds writing to system.web_sessions will still
//   stall on an unavailable cluster and prevent root from logging in.
//
// - if the user is another user than root, then the function fails
//   after a timeout instead of blocking. The timeout is configurable
//   via the cluster setting.
//
// - there is a cache for the the information from system.users and
//   system.role_options. As long as the lookup succeeded before and there
//   haven't been any CREATE/ALTER/DROP ROLE commands since, then the cache is
//   used without a KV lookup.
//
func GetUserHashedPassword(
	ctx context.Context, execCfg *ExecutorConfig, ie *InternalExecutor, username security.SQLUsername,
) (
	exists bool,
	canLogin bool,
	pwRetrieveFn func(ctx context.Context) (hashedPassword []byte, err error),
	validUntilFn func(ctx context.Context) (timestamp *tree.DTimestamp, err error),
	err error,
) {
	isRoot := username.IsRootUser()

	if isRoot {
		// As explained above, for root we report that the user exists
		// immediately, and delay retrieving the password until strictly
		// necessary.
		rootFn := func(ctx context.Context) ([]byte, error) {
			authInfo, err := retrieveUserAndPasswordWithCache(ctx, execCfg, ie, isRoot, username)
			return authInfo.HashedPassword, err
		}

		// Root user cannot have password expiry and must have login.
		validUntilFn := func(ctx context.Context) (*tree.DTimestamp, error) {
			return nil, nil
		}
		return true, true, rootFn, validUntilFn, nil
	}

	// Other users must reach for system.users no matter what, because
	// only that contains the truth about whether the user exists.
	authInfo, err := retrieveUserAndPasswordWithCache(
		ctx, execCfg, ie, isRoot, username,
	)
	return authInfo.UserExists, authInfo.CanLogin,
		func(ctx context.Context) ([]byte, error) { return authInfo.HashedPassword, nil },
		func(ctx context.Context) (*tree.DTimestamp, error) { return authInfo.ValidUntil, nil },
		err
}

func retrieveUserAndPasswordWithCache(
	ctx context.Context,
	execCfg *ExecutorConfig,
	ie *InternalExecutor,
	isRoot bool,
	normalizedUsername security.SQLUsername,
) (aInfo authentication.AuthInfo, err error) {
	// We may be operating with a timeout.
	timeout := userLoginTimeout.Get(&ie.s.cfg.Settings.SV)
	// We don't like long timeouts for root.
	// (4.5 seconds to not exceed the default 5s timeout configured in many clients.)
	const maxRootTimeout = 4*time.Second + 500*time.Millisecond
	if isRoot && (timeout == 0 || timeout > maxRootTimeout) {
		timeout = maxRootTimeout
	}

	runFn := func(fn func(ctx context.Context) error) error { return fn(ctx) }
	if timeout != 0 {
		runFn = func(fn func(ctx context.Context) error) error {
			return contextutil.RunWithTimeout(ctx, "get-user-timeout", timeout, fn)
		}
	}
	err = runFn(func(ctx context.Context) (retErr error) {
		aInfo, retErr = execCfg.AuthenticationInfoCache.Get(
			ctx,
			execCfg.Settings,
			execCfg.LeaseManager,
			ie,
			execCfg.DB,
			normalizedUsername,
			retrieveUserAndPassword,
		)
		return retErr
	})

	if err != nil {
		// Failed to retrieve the user account. Report in logs for later investigation.
		log.Warningf(ctx, "user lookup for %q failed: %v", normalizedUsername, err)
		err = errors.Wrap(errors.Handled(err), "internal error while retrieving user account")
	}
	return aInfo, err
}

func retrieveUserAndPassword(
	ctx context.Context,
	txn *kv.Txn,
	ie sqlutil.InternalExecutor,
	normalizedUsername security.SQLUsername,
) (aInfo authentication.AuthInfo, retErr error) {
	// Use fully qualified table name to avoid looking up "".system.users.
	const getHashedPassword = `SELECT "hashedPassword" FROM system.public.users ` +
		`WHERE username=$1`
	values, err := ie.QueryRowEx(
		ctx, "get-hashed-pwd", txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		getHashedPassword, normalizedUsername)
	if err != nil {
		return authentication.AuthInfo{}, errors.Wrapf(err, "error looking up user %s", normalizedUsername)
	}
	if values != nil {
		aInfo.UserExists = true
		if v := values[0]; v != tree.DNull {
			aInfo.HashedPassword = []byte(*(v.(*tree.DBytes)))
		}
	}

	if !aInfo.UserExists {
		return authentication.AuthInfo{}, nil
	}

	// Use fully qualified table name to avoid looking up "".system.role_options.
	getLoginDependencies := `SELECT option, value FROM system.public.role_options ` +
		`WHERE username=$1 AND option IN ('NOLOGIN', 'VALID UNTIL')`

	it, err := ie.QueryIteratorEx(
		ctx, "get-login-dependencies", txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		getLoginDependencies,
		normalizedUsername,
	)
	if err != nil {
		return authentication.AuthInfo{}, errors.Wrapf(err, "error looking up user %s", normalizedUsername)
	}
	// We have to make sure to close the iterator since we might return from
	// the for loop early (before Next() returns false).
	defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()

	// To support users created before 20.1, allow all USERS/ROLES to login
	// if NOLOGIN is not found.
	aInfo.CanLogin = true
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		row := it.Cur()
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
					return authentication.AuthInfo{}, errors.Wrap(err,
						"error trying to parse timestamp while retrieving password valid until value")
				}
			}
		}
	}

	return aInfo, err
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
	_, tableDesc, err := p.ResolveMutableTableDescriptor(ctx, authentication.UsersTableName, true, tree.ResolveAnyTableKind)
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
	_, tableDesc, err := p.ResolveMutableTableDescriptor(ctx, authentication.RoleOptionsTableName, true, tree.ResolveAnyTableKind)
	if err != nil {
		return err
	}

	return p.writeSchemaChange(
		ctx, tableDesc, descpb.InvalidMutationID, "updating version for role options table",
	)
}
