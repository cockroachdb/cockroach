// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlutil

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// ResolveMemberOfWithAdminOption explores all the memberships of the user.
// It returns a map of memberships, as well as whether that membership
// is an admin role.
func ResolveMemberOfWithAdminOption(
	ctx context.Context, member security.SQLUsername, txn *kv.Txn, ie InternalExecutor,
) (map[security.SQLUsername]bool, error) {
	ret := map[security.SQLUsername]bool{}

	// Keep track of members we looked up.
	visited := map[security.SQLUsername]struct{}{}
	toVisit := []security.SQLUsername{member}
	lookupRolesStmt := `SELECT "role", "isAdmin" FROM system.public.role_members WHERE "member" = $1`

	for len(toVisit) > 0 {
		// Pop first element.
		m := toVisit[0]
		toVisit = toVisit[1:]
		if _, ok := visited[m]; ok {
			continue
		}
		visited[m] = struct{}{}

		it, err := ie.QueryIterator(
			ctx, "expand-roles", txn, lookupRolesStmt, m.Normalized(),
		)
		if err != nil {
			return nil, err
		}

		var ok bool
		for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
			row := it.Cur()
			roleName := tree.MustBeDString(row[0])
			isAdmin := row[1].(*tree.DBool)

			// system.role_members stores pre-normalized usernames.
			role := security.MakeSQLUsernameFromPreNormalizedString(string(roleName))
			ret[role] = bool(*isAdmin)

			// We need to expand this role. Let the "pop" worry about already-visited elements.
			toVisit = append(toVisit, role)
		}
		if err != nil {
			return nil, err
		}
	}

	return ret, nil
}
