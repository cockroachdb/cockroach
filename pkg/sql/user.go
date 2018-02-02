// Copyright 2016 The Cockroach Authors.
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

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// GetUserHashedPassword returns the hashedPassword for the given username if
// found in system.users.
func GetUserHashedPassword(
	ctx context.Context, executor *Executor, metrics *MemoryMetrics, username string,
) (bool, []byte, error) {
	normalizedUsername := tree.Name(username).Normalize()
	// Always return no password for the root user, even if someone manually inserts one.
	if normalizedUsername == security.RootUser {
		return true, nil, nil
	}

	var hashedPassword []byte
	var exists bool
	err := executor.cfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		p, cleanup := newInternalPlanner(
			"get-pwd", txn, security.RootUser, metrics, &executor.cfg)
		defer cleanup()
		const getHashedPassword = `SELECT "hashedPassword" FROM system.public.users ` +
			`WHERE username=$1 AND "isRole" = false`
		values, err := p.QueryRow(ctx, getHashedPassword, normalizedUsername)
		if err != nil {
			return errors.Errorf("error looking up user %s", normalizedUsername)
		}
		if len(values) == 0 {
			return nil
		}
		exists = true
		hashedPassword = []byte(*(values[0].(*tree.DBytes)))
		return nil
	})

	return exists, hashedPassword, err
}

// The map value is true if the map key is a role, false if it is a user.
func (p *planner) GetAllUsersAndRoles(ctx context.Context) (map[string]bool, error) {
	query := `SELECT username,"isRole"  FROM system.public.users`
	newPlanner, cleanup := newInternalPlanner(
		"get-all-users-and-roles", p.txn, security.RootUser, p.extendedEvalCtx.MemMetrics, p.ExecCfg())
	defer cleanup()
	rows, _ /* cols */, err := newPlanner.queryRows(ctx, query)
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

// Returns true is the requested username is a role, false if it is a user.
// Returns error if it does not exist.
func existingUserIsRole(
	ctx context.Context, ie InternalExecutor, opName string, txn *client.Txn, username string,
) (bool, error) {
	values, err := ie.QueryRowInTransaction(
		ctx,
		opName,
		txn,
		`SELECT "isRole" FROM system.public.users WHERE username=$1`,
		username)
	if err != nil {
		return false, errors.Errorf("error looking up user %s", username)
	}
	if len(values) == 0 {
		return false, errors.Errorf("no user or role named %s", username)
	}

	isRole := bool(*(values[0]).(*tree.DBool))
	return isRole, nil
}
