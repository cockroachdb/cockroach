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

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// GetUserHashedPassword determines if the given user exists and
// also returns a password retrieval function.
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
func GetUserHashedPassword(
	ctx context.Context, ie *InternalExecutor, username string,
) (
	exists bool,
	canLogin bool,
	pwRetrieveFn func(ctx context.Context) (hashedPassword []byte, err error),
	validUntilFn func(ctx context.Context) (timestamp *tree.DTimestamp, err error),
	err error,
) {
	normalizedUsername := tree.Name(username).Normalize()
	isRoot := normalizedUsername == security.RootUser

	if isRoot {
		// As explained above, for root we report that the user exists
		// immediately, and delay retrieving the password until strictly
		// necessary.
		rootFn := func(ctx context.Context) ([]byte, error) {
			_, _, hashedPassword, _, err := retrieveUserAndPassword(ctx, ie, isRoot, normalizedUsername)
			return hashedPassword, err
		}

		// Root user cannot have password expiry and must have login.
		validUntilFn := func(ctx context.Context) (*tree.DTimestamp, error) {
			return nil, nil
		}
		return true, true, rootFn, validUntilFn, nil
	}

	// Other users must reach for system.users no matter what, because
	// only that contains the truth about whether the user exists.
	exists, canLogin, hashedPassword, validUntil, err := retrieveUserAndPassword(ctx, ie, isRoot, normalizedUsername)
	return exists, canLogin,
		func(ctx context.Context) ([]byte, error) { return hashedPassword, nil },
		func(ctx context.Context) (*tree.DTimestamp, error) { return validUntil, nil },
		err
}

func retrieveUserAndPassword(
	ctx context.Context, ie *InternalExecutor, isRoot bool, normalizedUsername string,
) (exists bool, canLogin bool, hashedPassword []byte, validUntil *tree.DTimestamp, err error) {
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

	// Perform the lookup with a timeout.
	err = runFn(func(ctx context.Context) error {
		const getHashedPassword = `SELECT "hashedPassword" FROM system.users ` +
			`WHERE username=$1`
		values, err := ie.QueryRowEx(
			ctx, "get-hashed-pwd", nil, /* txn */
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			getHashedPassword, normalizedUsername)
		if err != nil {
			return errors.Wrapf(err, "error looking up user %s", normalizedUsername)
		}
		if values != nil {
			exists = true
			if v := values[0]; v != tree.DNull {
				hashedPassword = []byte(*(v.(*tree.DBytes)))
			}
		}

		if !exists {
			return nil
		}

		getLoginDependencies := `SELECT option, value FROM system.role_options ` +
			`WHERE username=$1 AND option IN ('NOLOGIN', 'VALID UNTIL')`

		loginDependencies, err := ie.QueryEx(
			ctx, "get-login-dependencies", nil, /* txn */
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			getLoginDependencies,
			normalizedUsername,
		)
		if err != nil {
			return errors.Wrapf(err, "error looking up user %s", normalizedUsername)
		}

		// To support users created before 20.1, allow all USERS/ROLES to login
		// if NOLOGIN is not found.
		canLogin = true
		for _, row := range loginDependencies {
			option := string(tree.MustBeDString(row[0]))

			if option == "NOLOGIN" {
				canLogin = false
			}

			if option == "VALID UNTIL" {
				if tree.DNull.Compare(nil, row[1]) != 0 {
					ts := string(tree.MustBeDString(row[1]))
					// This is okay because the VALID UNTIL is stored as a string
					// representation of a TimestampTZ which has the same underlying
					// representation in the table as a Timestamp (UTC time).
					timeCtx := tree.NewParseTimeContext(timeutil.Now())
					validUntil, _, err = tree.ParseDTimestamp(timeCtx, ts, time.Microsecond)
					if err != nil {
						return errors.Wrap(err,
							"error trying to parse timestamp while retrieving password valid until value")
					}
				}
			}
		}

		return nil
	})

	if err != nil {
		// Failed to retrieve the user account. Report in logs for later investigation.
		log.Warningf(ctx, "user lookup for %q failed: %v", normalizedUsername, err)
		err = errors.HandledWithMessage(err, "internal error while retrieving user account")
	}
	return exists, canLogin, hashedPassword, validUntil, err
}

var userLoginTimeout = settings.RegisterPublicNonNegativeDurationSetting(
	"server.user_login.timeout",
	"timeout after which client authentication times out if some system range is unavailable (0 = no timeout)",
	10*time.Second,
)

// GetAllRoles returns a "set" (map) of Roles -> true.
func (p *planner) GetAllRoles(ctx context.Context) (map[string]bool, error) {
	query := `SELECT username FROM system.users`
	rows, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryEx(
		ctx, "read-users", p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		query)
	if err != nil {
		return nil, err
	}

	users := make(map[string]bool)
	for _, row := range rows {
		username := tree.MustBeDString(row[0])
		users[string(username)] = true
	}
	return users, nil
}

var roleMembersTableName = tree.MakeTableName("system", "role_members")

// BumpRoleMembershipTableVersion increases the table version for the
// role membership table.
func (p *planner) BumpRoleMembershipTableVersion(ctx context.Context) error {
	tableDesc, err := p.ResolveMutableTableDescriptor(ctx, &roleMembersTableName, true, resolver.ResolveAnyDescType)
	if err != nil {
		return err
	}

	return p.writeSchemaChange(
		ctx, tableDesc, sqlbase.InvalidMutationID, "updating version for role membership table",
	)
}
