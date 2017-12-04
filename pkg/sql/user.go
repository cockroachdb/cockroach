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
	"github.com/pkg/errors"
	"golang.org/x/net/context"

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
		p := makeInternalPlanner("get-pwd", txn, security.RootUser, metrics)
		defer finishInternalPlanner(p)
		const getHashedPassword = `SELECT "hashedPassword" FROM system.users ` +
			`WHERE username=$1`
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

// GetAllUsers returns all the usernames in system.users.
func GetAllUsers(ctx context.Context, plan *planner) (map[string]bool, error) {
	query := `SELECT username FROM system.users`
	p := makeInternalPlanner("get-all-user", plan.txn, security.RootUser, plan.session.memMetrics)
	defer finishInternalPlanner(p)
	rows, err := p.queryRows(ctx, query)

	users := make(map[string]bool)
	if err == nil {
		for _, row := range rows {
			users[string(tree.MustBeDString(row[0]))] = true
		}
	}
	return users, err
}
