// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
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
	tDesc, err := getTableDesc(ctx, p.txn, p.getVirtualTabler(), userTableName)
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
	normalizedUsername, hashedPassword, err := n.userAuthInfo.resolve()
	if err != nil {
		return err
	}

	if len(hashedPassword) > 0 && params.extendedEvalCtx.ExecCfg.RPCContext.Insecure {
		return errors.New("cluster in insecure mode; user cannot use password authentication")
	}

	var opName string
	if n.isRole {
		opName = "create-role"
	} else {
		opName = "create-user"
	}

	internalExecutor := InternalExecutor{ExecCfg: params.extendedEvalCtx.ExecCfg}
	n.run.rowsAffected, err = internalExecutor.ExecuteStatementInTransaction(
		params.ctx,
		opName,
		params.p.txn,
		"INSERT INTO system.public.users VALUES ($1, $2, $3);",
		normalizedUsername,
		hashedPassword,
		n.isRole,
	)
	if err != nil {
		if sqlbase.IsUniquenessConstraintViolationError(err) {
			// Entry exists. We need to know if it's a user or role.
			isRole, roleErr := existingUserIsRole(params.ctx, internalExecutor, opName, params.p.txn, normalizedUsername)
			if roleErr != nil {
				return roleErr
			}
			if isRole == n.isRole && n.ifNotExists {
				// The username exists with the same role setting, and we asked to skip
				// if it exists: no error.
				//
				// INSERT only detects error at the end of each batch.  This
				// means perhaps the count by ExecuteStatementInTransactions
				// will have reported updated rows even though an error was
				// encountered.  If the error was due to a duplicate entry, we
				// are not actually inserting anything but are canceling the
				// error, so clear the row count so that the client can learn
				// what's going on.
				n.run.rowsAffected = 0
				return nil
			}

			if isRole {
				err = errors.Errorf("a role named %s already exists", normalizedUsername)
			} else {
				err = errors.Errorf("a user named %s already exists", normalizedUsername)
			}
		}
		return err
	} else if n.run.rowsAffected != 1 {
		return errors.Errorf(
			"%d rows affected by user creation; expected exactly one row affected", n.run.rowsAffected,
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

const usernameHelp = "usernames are case insensitive, must start with a letter " +
	"or underscore, may contain letters, digits or underscores, and must not exceed 63 characters"

var usernameRE = regexp.MustCompile(`^[\p{Ll}_][\p{Ll}0-9_]{0,62}$`)

var blacklistedUsernames = map[string]struct{}{
	security.NodeUser: {},
}

// NormalizeAndValidateUsername case folds the specified username and verifies
// it validates according to the usernameRE regular expression.
func NormalizeAndValidateUsername(username string) (string, error) {
	username = tree.Name(username).Normalize()
	if !usernameRE.MatchString(username) {
		return "", errors.Errorf("username %q invalid; %s", username, usernameHelp)
	}
	if _, ok := blacklistedUsernames[username]; ok {
		return "", errors.Errorf("username %q reserved", username)
	}
	return username, nil
}

var errNoUserNameSpecified = errors.New("no username specified")

type userAuthInfo struct {
	name     func() (string, error)
	password func() (string, error)
}

func (p *planner) getUserAuthInfo(nameE, passwordE tree.Expr, ctx string) (userAuthInfo, error) {
	name, err := p.TypeAsString(nameE, ctx)
	if err != nil {
		return userAuthInfo{}, err
	}
	var password func() (string, error)
	if passwordE != nil {
		password, err = p.TypeAsString(passwordE, ctx)
		if err != nil {
			return userAuthInfo{}, err
		}
	}
	return userAuthInfo{name: name, password: password}, nil
}

// resolve returns the actual user name and (hashed) password.
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
		resolvedPassword, err := ua.password()
		if err != nil {
			return "", nil, err
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
