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

type createUserNode struct {
	ifNotExists bool
	userAuthInfo

	run createUserRun
}

// CreateUser creates a user.
// Privileges: INSERT on system.users.
//   notes: postgres allows the creation of users with an empty password. We do
//          as well, but disallow password authentication for these users.
func (p *planner) CreateUser(ctx context.Context, n *tree.CreateUser) (planNode, error) {
	tDesc, err := getTableDesc(ctx, p.txn, p.getVirtualTabler(), &tree.TableName{DatabaseName: "system", TableName: "users"})
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(tDesc, privilege.INSERT); err != nil {
		return nil, err
	}

	ua, err := p.getUserAuthInfo(n.Name, n.Password, "CREATE USER")
	if err != nil {
		return nil, err
	}

	return &createUserNode{
		userAuthInfo: ua,
		ifNotExists:  n.IfNotExists,
	}, nil
}

func (n *createUserNode) startExec(params runParams) error {
	normalizedUsername, hashedPassword, err := n.userAuthInfo.resolve()
	if err != nil {
		return err
	}

	internalExecutor := InternalExecutor{LeaseManager: params.p.LeaseMgr()}
	n.run.rowsAffected, err = internalExecutor.ExecuteStatementInTransaction(
		params.ctx,
		"create-user",
		params.p.txn,
		"INSERT INTO system.users VALUES ($1, $2, false);",
		normalizedUsername,
		hashedPassword,
	)
	if err != nil {
		if sqlbase.IsUniquenessConstraintViolationError(err) {
			if n.ifNotExists {
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
			err = errors.Errorf("user %s already exists", normalizedUsername)
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

func (*createUserNode) Next(runParams) (bool, error)   { return false, nil }
func (*createUserNode) Values() tree.Datums            { return tree.Datums{} }
func (*createUserNode) Close(context.Context)          {}
func (n *createUserNode) FastPathResults() (int, bool) { return n.run.rowsAffected, true }

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
