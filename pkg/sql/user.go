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

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// GetUserHashedPassword returns the hashedPassword for the given username if
// found in system.users.
func GetUserHashedPassword(
	ctx context.Context, ie *InternalExecutor, metrics *MemoryMetrics, username string,
) (bool, []byte, error) {
	normalizedUsername := tree.Name(username).Normalize()
	// Always return no password for the root user, even if someone manually inserts one.
	if normalizedUsername == security.RootUser {
		return true, nil, nil
	}

	const getHashedPassword = `SELECT "hashedPassword" FROM system.users ` +
		`WHERE username=$1 AND "isRole" = false`
	values, err := ie.QueryRow(
		ctx, "get-hashed-pwd", nil /* txn */, getHashedPassword, normalizedUsername)
	if err != nil {
		return false, nil, errors.Wrapf(err, "error looking up user %s", normalizedUsername)
	}
	if values == nil {
		return false, nil, nil
	}
	hashedPassword := []byte(*(values[0].(*tree.DBytes)))
	return true, hashedPassword, nil
}

// The map value is true if the map key is a role, false if it is a user.
func (p *planner) GetAllUsersAndRoles(ctx context.Context) (map[string]bool, error) {
	query := `SELECT username,"isRole"  FROM system.users`
	rows, _ /* cols */, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Query(
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
	tableDesc, err := p.ResolveMutableTableDescriptor(ctx, &roleMembersTableName, true, anyDescType)
	if err != nil {
		return err
	}

	return p.writeSchemaChange(ctx, tableDesc, sqlbase.InvalidMutationID)
}
