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
	"fmt"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

// CreateRoleNode creates entries in the system.users table.
// This is called from CREATE USER and CREATE ROLE.
type CreateRoleNode struct {
	ifNotExists bool
	isRole      bool
	roleOptions roleoption.List
	userNameInfo
}

var userTableName = tree.NewTableName("system", "users")

// RoleOptionsTableName represents system.role_options.
var RoleOptionsTableName = tree.NewTableName("system", "role_options")

// CreateRole represents a CREATE ROLE statement.
// Privileges: INSERT on system.users.
//   notes: postgres allows the creation of users with an empty password. We do
//          as well, but disallow password authentication for these users.
func (p *planner) CreateRole(ctx context.Context, n *tree.CreateRole) (planNode, error) {
	return p.CreateRoleNode(ctx, n.Name, n.IfNotExists, n.IsRole,
		"CREATE ROLE", n.KVOptions)
}

// CreateRoleNode creates a "create user" plan node.
// This can be called from CREATE USER or CREATE ROLE.
func (p *planner) CreateRoleNode(
	ctx context.Context,
	nameE tree.Expr,
	ifNotExists bool,
	isRole bool,
	opName string,
	kvOptions tree.KVOptions,
) (*CreateRoleNode, error) {
	if err := p.HasRoleOption(ctx, roleoption.CREATEROLE); err != nil {
		return nil, err
	}

	asStringOrNull := func(e tree.Expr, op string) (func() (bool, string, error), error) {
		return p.TypeAsStringOrNull(ctx, e, op)
	}
	roleOptions, err := kvOptions.ToRoleOptions(asStringOrNull, opName)

	// Using CREATE ROLE syntax enables NOLOGIN by default.
	if isRole && !roleOptions.Contains(roleoption.LOGIN) &&
		!roleOptions.Contains(roleoption.NOLOGIN) {
		roleOptions = append(roleOptions,
			roleoption.RoleOption{Option: roleoption.NOLOGIN, HasValue: false})
	}

	if err != nil {
		return nil, err
	}

	if err := roleOptions.CheckRoleOptionConflicts(); err != nil {
		return nil, err
	}

	ua, err := p.getUserAuthInfo(ctx, nameE, opName)
	if err != nil {
		return nil, err
	}

	return &CreateRoleNode{
		userNameInfo: ua,
		ifNotExists:  ifNotExists,
		isRole:       isRole,
		roleOptions:  roleOptions,
	}, nil
}

func (n *CreateRoleNode) startExec(params runParams) error {
	var opName string
	if n.isRole {
		sqltelemetry.IncIAMCreateCounter(sqltelemetry.Role)
		opName = "create-role"
	} else {
		sqltelemetry.IncIAMCreateCounter(sqltelemetry.User)
		opName = "create-user"
	}

	normalizedUsername, err := n.userNameInfo.resolveUsername()
	if err != nil {
		return err
	}

	var hashedPassword []byte
	if n.roleOptions.Contains(roleoption.PASSWORD) {
		hashedPassword, err = n.roleOptions.GetHashedPassword()
		if err != nil {
			return err
		}

		if len(hashedPassword) > 0 && params.extendedEvalCtx.ExecCfg.RPCContext.Config.Insecure {
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
	} else {
		// v20.1 and below crash during authentication if they find a NULL value
		// in system.users.hashedPassword. v20.2 and above handle this correctly,
		// but we need to maintain mixed version compatibility for at least one
		// release.
		// TODO(nvanbenschoten): remove this for v21.1.
		hashedPassword = []byte{}
	}

	// Reject the "public" role. It does not have an entry in the users table but is reserved.
	if normalizedUsername == sqlbase.PublicRole {
		return pgerror.Newf(pgcode.ReservedName, "role name %q is reserved", sqlbase.PublicRole)
	}

	// Check if the user/role exists.
	row, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.QueryRowEx(
		params.ctx,
		opName,
		params.p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		fmt.Sprintf(`select "isRole" from %s where username = $1`, userTableName),
		normalizedUsername,
	)
	if err != nil {
		return errors.Wrapf(err, "error looking up user")
	}
	if row != nil {
		if n.ifNotExists {
			return nil
		}
		return pgerror.Newf(pgcode.DuplicateObject,
			"a role/user named %s already exists", normalizedUsername)
	}

	// TODO(richardjcai): move hashedPassword column to system.role_options.
	rowsAffected, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
		params.ctx,
		opName,
		params.p.txn,
		fmt.Sprintf("insert into %s values ($1, $2, $3)", userTableName),
		normalizedUsername,
		hashedPassword,
		n.isRole,
	)

	if err != nil {
		return err
	} else if rowsAffected != 1 {
		return errors.AssertionFailedf("%d rows affected by user creation; expected exactly one row affected",
			rowsAffected,
		)
	}

	// Get a map of statements to execute for role options and their values.
	stmts, err := n.roleOptions.GetSQLStmts(sqltelemetry.CreateRole)
	if err != nil {
		return err
	}

	for stmt, value := range stmts {
		qargs := []interface{}{normalizedUsername}

		if value != nil {
			isNull, val, err := value()
			if err != nil {
				return err
			}
			if isNull {
				// If the value of the role option is NULL, ensure that nil is passed
				// into the statement placeholder, since val is string type "NULL"
				// will not be interpreted as NULL by the InternalExecutor.
				qargs = append(qargs, nil)
			} else {
				qargs = append(qargs, val)
			}
		}

		_, err = params.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
			params.ctx,
			opName,
			params.p.txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			stmt,
			qargs...,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

// Next implements the planNode interface.
func (*CreateRoleNode) Next(runParams) (bool, error) { return false, nil }

// Values implements the planNode interface.
func (*CreateRoleNode) Values() tree.Datums { return tree.Datums{} }

// Close implements the planNode interface.
func (*CreateRoleNode) Close(context.Context) {}

const usernameHelp = "Usernames are case insensitive, must start with a letter, " +
	"digit or underscore, may contain letters, digits, dashes, periods, or underscores, and must not exceed 63 characters."

var usernameRE = regexp.MustCompile(`^[\p{Ll}0-9_][---\p{Ll}0-9_.]*$`)

var blocklistedUsernames = map[string]struct{}{
	security.NodeUser: {},
}

// NormalizeAndValidateUsername case folds the specified username and verifies
// it validates according to the usernameRE regular expression.
// It rejects reserved user names.
func NormalizeAndValidateUsername(username string) (string, error) {
	username, err := NormalizeAndValidateUsernameNoBlocklist(username)
	if err != nil {
		return "", err
	}
	if _, ok := blocklistedUsernames[username]; ok {
		return "", pgerror.Newf(pgcode.ReservedName, "username %q reserved", username)
	}
	return username, nil
}

// NormalizeAndValidateUsernameNoBlocklist case folds the specified username and verifies
// it validates according to the usernameRE regular expression.
func NormalizeAndValidateUsernameNoBlocklist(username string) (string, error) {
	username = tree.Name(username).Normalize()
	if !usernameRE.MatchString(username) {
		return "", errors.WithHint(pgerror.Newf(pgcode.InvalidName, "username %q invalid", username), usernameHelp)
	}
	if len(username) > 63 {
		return "", errors.WithHint(pgerror.Newf(pgcode.NameTooLong, "username %q is too long", username), usernameHelp)
	}
	return username, nil
}

var errNoUserNameSpecified = errors.New("no username specified")

type userNameInfo struct {
	name func() (string, error)
}

func (p *planner) getUserAuthInfo(
	ctx context.Context, nameE tree.Expr, context string,
) (userNameInfo, error) {
	name, err := p.TypeAsString(ctx, nameE, context)
	if err != nil {
		return userNameInfo{}, err
	}

	return userNameInfo{name: name}, nil
}

// resolveUsername returns the actual user name.
func (ua *userNameInfo) resolveUsername() (string, error) {
	name, err := ua.name()
	if err != nil {
		return "", err
	}
	if name == "" {
		return "", errNoUserNameSpecified
	}
	normalizedUsername, err := NormalizeAndValidateUsername(name)
	if err != nil {
		return "", err
	}

	return normalizedUsername, nil
}
