// Copyright 2017 The Cockroach Authors.
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
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

// CreateUserNode creates entries in the system.users table.
// This is called from CREATE USER and CREATE ROLE.
type CreateUserNode struct {
	ifNotExists bool
	isRole      bool
	userAuthInfo

	run createUserRun
}

var userTableName = tree.NewTableName("system", "users")

// CreateUser creates a user.
// Privileges: INSERT on system.users.
//   notes: postgres allows the creation of users with an empty password. We do
//          as well, but disallow password authentication for these users.
func (p *planner) CreateUser(ctx context.Context, n *tree.CreateUser) (planNode, error) {
	return p.CreateUserNode(ctx, n.Name, n.Password, n.IfNotExists, false /* isRole */, "CREATE USER")
}

// CreateUserNode creates a "create user" plan node. This can be called from CREATE USER or CREATE ROLE.
func (p *planner) CreateUserNode(
	ctx context.Context, nameE, passwordE tree.Expr, ifNotExists bool, isRole bool, opName string,
) (*CreateUserNode, error) {
	tDesc, err := ResolveExistingObject(ctx, p, userTableName, tree.ObjectLookupFlagsWithRequired(), ResolveRequireTableDesc)
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, tDesc, privilege.INSERT); err != nil {
		return nil, err
	}

	ua, err := p.getUserAuthInfo(nameE, passwordE, opName)
	if err != nil {
		return nil, err
	}

	return &CreateUserNode{
		userAuthInfo: ua,
		ifNotExists:  ifNotExists,
		isRole:       isRole,
	}, nil
}

func (n *CreateUserNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeCreate("user"))

	normalizedUsername, hashedPassword, err := n.userAuthInfo.resolve()
	if err != nil {
		return err
	}

	if len(hashedPassword) > 0 && params.extendedEvalCtx.ExecCfg.RPCContext.Insecure {
		// We disallow setting a non-empty password in insecure mode
		// because insecure means an observer may have MITM'ed the change
		// and learned the password.
		//
		// It's valid to clear the password (WITH PASSWORD NULL) however
		// since that forces cert auth when moving back to secure mode,
		// and certs can't be MITM'ed over the insecure SQL connection.
		return pgerror.New(pgcode.InvalidPassword,
			"setting or updating a password is not supported in insecure mode")
	}

	// Reject the "public" role. It does not have an entry in the users table but is reserved.
	if normalizedUsername == sqlbase.PublicRole {
		return pgerror.Newf(pgcode.ReservedName, "role name %q is reserved", sqlbase.PublicRole)
	}

	var opName string
	if n.isRole {
		opName = "create-role"
	} else {
		opName = "create-user"
	}

	// Check if the user/role exists.
	row, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.QueryRowEx(
		params.ctx,
		opName,
		params.p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`select "isRole" from system.users where username = $1`,
		normalizedUsername,
	)
	if err != nil {
		return errors.Wrapf(err, "error looking up user")
	}
	if row != nil {
		isRole := bool(*row[0].(*tree.DBool))
		if isRole == n.isRole && n.ifNotExists {
			// The username exists with the same role setting, and we asked to skip
			// if it exists: no error.
			return nil
		}
		msg := "a user"
		if isRole {
			msg = "a role"
		}
		return pgerror.Newf(pgcode.DuplicateObject,
			"%s named %s already exists",
			msg, normalizedUsername)
	}

	n.run.rowsAffected, err = params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
		params.ctx,
		opName,
		params.p.txn,
		"insert into system.users values ($1, $2, $3)",
		normalizedUsername,
		hashedPassword,
		n.isRole,
	)
	if err != nil {
		return err
	} else if n.run.rowsAffected != 1 {
		return errors.AssertionFailedf("%d rows affected by user creation; expected exactly one row affected",
			n.run.rowsAffected,
		)
	}

	return nil
}

type createUserRun struct {
	rowsAffected int
}

// Next implements the planNode interface.
func (*CreateUserNode) Next(runParams) (bool, error) { return false, nil }

// Values implements the planNode interface.
func (*CreateUserNode) Values() tree.Datums { return tree.Datums{} }

// Close implements the planNode interface.
func (*CreateUserNode) Close(context.Context) {}

// FastPathResults implements the planNodeFastPath interface.
func (n *CreateUserNode) FastPathResults() (int, bool) { return n.run.rowsAffected, true }

const usernameHelp = "usernames are case insensitive, must start with a letter, " +
	"digit or underscore, may contain letters, digits, dashes, or underscores, and must not exceed 63 characters"

var usernameRE = regexp.MustCompile(`^[\p{Ll}0-9_][\p{Ll}0-9_-]{0,62}$`)

var blacklistedUsernames = map[string]struct{}{
	security.NodeUser: {},
}

// NormalizeAndValidateUsername case folds the specified username and verifies
// it validates according to the usernameRE regular expression.
// It rejects reserved user names.
func NormalizeAndValidateUsername(username string) (string, error) {
	username, err := NormalizeAndValidateUsernameNoBlacklist(username)
	if err != nil {
		return "", err
	}
	if _, ok := blacklistedUsernames[username]; ok {
		return "", errors.Errorf("username %q reserved", username)
	}
	return username, nil
}

// NormalizeAndValidateUsernameNoBlacklist case folds the specified username and verifies
// it validates according to the usernameRE regular expression.
func NormalizeAndValidateUsernameNoBlacklist(username string) (string, error) {
	username = tree.Name(username).Normalize()
	if !usernameRE.MatchString(username) {
		return "", errors.Errorf("username %q invalid; %s", username, usernameHelp)
	}
	return username, nil
}

var errNoUserNameSpecified = errors.New("no username specified")

type userAuthInfo struct {
	name     func() (string, error)
	password func() (bool, string, error)
}

func (p *planner) getUserAuthInfo(nameE, passwordE tree.Expr, ctx string) (userAuthInfo, error) {
	name, err := p.TypeAsString(nameE, ctx)
	if err != nil {
		return userAuthInfo{}, err
	}
	var password func() (bool, string, error)
	if passwordE != nil {
		password, err = p.typeAsStringOrNull(passwordE, ctx)
		if err != nil {
			return userAuthInfo{}, err
		}
	}
	return userAuthInfo{name: name, password: password}, nil
}

// resolve returns the actual user name and (hashed) password.  The
// returned hashed password slice is nil iff the user was specified to
// have no password (e.g. CREATE USER without a PASSWORD clause, or
// using PASSWORD NULL). If the password was specified but empty
// (e.g. PASSWORD ''), an error is reported instead.
func (ua *userAuthInfo) resolve() (string, []byte, error) {
	name, err := ua.name()
	if err != nil {
		return "", nil, err
	}
	if name == "" {
		return "", nil, errNoUserNameSpecified
	}
	normalizedUsername, err := NormalizeAndValidateUsername(name)
	if err != nil {
		return "", nil, err
	}

	var hashedPassword []byte
	if ua.password != nil {
		isNull, resolvedPassword, err := ua.password()
		if err != nil {
			return "", nil, err
		}
		if isNull {
			return normalizedUsername, hashedPassword, nil
		}
		if resolvedPassword == "" {
			return "", nil, security.ErrEmptyPassword
		}

		hashedPassword, err = security.HashPassword(resolvedPassword)
		if err != nil {
			return "", nil, err
		}
	}

	return normalizedUsername, hashedPassword, nil
}
