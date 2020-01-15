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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// GetUserHashedPassword returns the hashedPassword for the given username if
// found in system.users.
//
// The function is tolerant of unavailable clusters (or unavailable
// system.user) as follows:
//
// - if the user is root and system.users is unavailable, the function
//   reports that the user exists but has no password. (Reminder: "no
//   password" means that they cannot use password authentication and
//   thus must use certificates instead, or an existing web session
//   cookie when using the web UI.)
//
//   This special case exists so that root can still log in into an
//   unavailable cluster. This is currently required for proper
//   function of `cockroach node status` which uses SQL.
//   Ideally we would not need SQL to enquire the status of
//   clusters and this special case would not need to exist.
//
// - if the user is another user than root, then the function fails
//   after a timeout instead of blocking. The timeout is configurable
//   via a cluster setting.
//
func GetUserHashedPassword(
	ctx context.Context, ie *InternalExecutor, metrics *MemoryMetrics, username string,
) (exists bool, hashedPassword []byte, err error) {
	normalizedUsername := tree.Name(username).Normalize()
	isRoot := normalizedUsername == security.RootUser
	// Always return no password for the root user, even if someone manually inserts one.
	if isRoot {
		return true, nil, nil
	}

	// We may be operating with a timeout.
	timeout := userLoginTimeout.Get(&ie.s.cfg.Settings.SV)
	runFn := func(fn func(ctx context.Context) error) error { return fn(ctx) }
	if timeout != 0 {
		runFn = func(fn func(ctx context.Context) error) error {
			return contextutil.RunWithTimeout(ctx, "get-hashed-pwd-timeout", timeout, fn)
		}
	}

	// Perform the lookup with a timeout.
	err = runFn(func(ctx context.Context) error {
		const getHashedPassword = `SELECT "hashedPassword" FROM system.users ` +
			`WHERE username=$1 AND "isRole" = false`
		values, err := ie.QueryRow(
			ctx, "get-hashed-pwd", nil /* txn */, getHashedPassword, normalizedUsername)
		if err != nil {
			return errors.Wrapf(err, "error looking up user %s", normalizedUsername)
		}
		if values != nil {
			exists = true
			hashedPassword = []byte(*(values[0].(*tree.DBytes)))
		}
		return nil
	})

	if errors.Is(err, context.DeadlineExceeded) {
		// Failed to retrieve the user account.
		log.Warningf(ctx, "user lookup for %q failed: %v", username, err)
		// As a special case, if root is logging in we know the user
		// account exists; we just report it has no password so that it
		// can only log in using certs or GSS.
		if isRoot {
			return true, nil, nil
		}
		err = errors.HandledWithMessage(err, "timeout while retrieving user account")
	}
	return exists, hashedPassword, err
}

var userLoginTimeout = settings.RegisterPublicNonNegativeDurationSetting(
	"server.user_login.timeout",
	"timeout after which client authentication times out if some system range is unavailable (0 = no timeout)",
	10*time.Second,
)

// The map value is true if the map key is a role, false if it is a user.
func (p *planner) GetAllUsersAndRoles(ctx context.Context) (map[string]bool, error) {
	query := `SELECT username,"isRole"  FROM system.users`
	rows, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Query(
		ctx, "read-users", p.txn, query)
	if err != nil {
		return nil, err
	}

	users := make(map[string]bool)
	for _, row := range rows {
		username := tree.MustBeDString(row[0])
		isRole := row[1].(*tree.DBool)
		users[string(username)] = bool(*isRole)
	}
	return users, nil
}

var roleMembersTableName = tree.MakeTableName("system", "role_members")

// BumpRoleMembershipTableVersion increases the table version for the
// role membership table.
func (p *planner) BumpRoleMembershipTableVersion(ctx context.Context) error {
	tableDesc, err := p.ResolveMutableTableDescriptor(ctx, &roleMembersTableName, true, ResolveAnyDescType)
	if err != nil {
		return err
	}

	return p.writeSchemaChange(ctx, tableDesc, sqlbase.InvalidMutationID)
}
