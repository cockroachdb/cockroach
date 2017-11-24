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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"golang.org/x/net/context"
)

// AuthorizationAccessor for checking authorization (e.g. desc privileges).
type AuthorizationAccessor interface {
	// CheckPrivilege verifies that the user has `privilege` on `descriptor`.
	CheckPrivilege(
		ctx context.Context, descriptor sqlbase.DescriptorProto, privilege privilege.Kind,
	) error

	// anyPrivilege verifies that the user has any privilege on `descriptor`.
	anyPrivilege(ctx context.Context, descriptor sqlbase.DescriptorProto) error

	// RequiresSuperUser errors if the session user isn't a super-user (i.e. root
	// or node). Includes the named action in the error message.
	RequireSuperUser(ctx context.Context, action string) error
}

var _ AuthorizationAccessor = &planner{}

// CheckPrivilege verifies that `user`` has `privilege` on `descriptor`.
func CheckPrivilege(
	user string, descriptor sqlbase.DescriptorProto, privilege privilege.Kind,
) error {
	if descriptor.GetPrivileges().CheckPrivilege(user, privilege) {
		return nil
	}
	return fmt.Errorf("user %s does not have %s privilege on %s %s",
		user, privilege, descriptor.TypeName(), descriptor.GetName())
}

// CheckPrivilege implements the AuthorizationAccessor interface.
func (p *planner) CheckPrivilege(
	ctx context.Context, descriptor sqlbase.DescriptorProto, privilege privilege.Kind,
) error {
	userErr := CheckPrivilege(p.session.User, descriptor, privilege)
	if userErr == nil {
		// Fast path: the user has direct grants on the object.
		return nil
	}

	// Slow path: lookup role memberships for user.
	roles, err := p.GetRolesForUser(ctx, p.session.User)
	if err != nil {
		return err
	}

	for _, r := range roles {
		if err := CheckPrivilege(r, descriptor, privilege); err != nil {
			continue
		}
		// Success! role has privileges.
		return nil
	}

	// No roles with privileges. Return the user error, it's clearer than
	// "some random role you belong to" doesn't have privileges.
	return userErr
}

// anyPrivilege implements the AuthorizationAccessor interface.
func (p *planner) anyPrivilege(ctx context.Context, descriptor sqlbase.DescriptorProto) error {
	if userCanSeeDescriptor(descriptor, p.session.User) {
		return nil
	}

	// Slow path: lookup role memberships for user.
	roles, err := p.GetRolesForUser(ctx, p.session.User)
	if err != nil {
		return err
	}

	for _, r := range roles {
		if !userCanSeeDescriptor(descriptor, r) {
			continue
		}
		// Success! role has privileges.
		return nil
	}

	return fmt.Errorf("user %s has no privileges on %s %s",
		p.session.User, descriptor.TypeName(), descriptor.GetName())
}

// RequireSuperUser implements the AuthorizationAccessor interface.
func (p *planner) RequireSuperUser(ctx context.Context, action string) error {
	if p.session.User == security.RootUser || p.session.User == security.NodeUser {
		return nil
	}

	// Slow path: lookup role memberships for user.
	roles, err := p.GetRolesForUser(ctx, p.session.User)
	if err != nil {
		return err
	}

	for _, r := range roles {
		if r == security.AdminRole {
			return nil
		}
	}

	return fmt.Errorf("only admin users are allowed to %s", action)
}

func userCanSeeDescriptor(descriptor sqlbase.DescriptorProto, user string) bool {
	return descriptor.GetPrivileges().AnyPrivilege(user) || isVirtualDescriptor(descriptor)
}

// GetRolesForUser finds all the roles this user belongs to, recursively.
// `username` is not included in the result.
// This assumes no cycles is in the membership graph.
func (p *planner) GetRolesForUser(ctx context.Context, username string) ([]string, error) {
	remaining := map[string]struct{}{username: struct{}{}}
	expandedRoles := make(map[string]struct{})

	query := `SELECT "role" FROM system.role_members WHERE member = $1`
	p = makeInternalPlanner("expand-roles", p.txn, security.RootUser, p.session.memMetrics)
	defer finishInternalPlanner(p)

	for len(remaining) > 0 {
		// Pick an element from the remaining list. Order doesn't matter.
		var nextElement string
		for k := range remaining {
			nextElement = k
			break
		}
		delete(remaining, nextElement)

		rows, err := p.queryRows(ctx, query, nextElement)
		if err != nil {
			return nil, err
		}

		for _, row := range rows {
			roleName := string(tree.MustBeDString(row[0]))
			if _, ok := expandedRoles[roleName]; !ok {
				// We have not seen this role before, add it to the roles still to lookup.
				remaining[roleName] = struct{}{}
				// And to the list of expanded roles.
				expandedRoles[roleName] = struct{}{}
			}
		}
	}

	result := make([]string, len(expandedRoles))
	i := 0
	for r := range expandedRoles {
		result[i] = r
	}
	return result, nil
}
