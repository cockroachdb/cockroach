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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// AuthorizationAccessor for checking authorization (e.g. desc privileges).
type AuthorizationAccessor interface {
	// CheckPrivilege verifies that the user has `privilege` on `descriptor`.
	CheckPrivilege(
		ctx context.Context, descriptor sqlbase.DescriptorProto, privilege privilege.Kind,
	) error

	// CheckAnyPrivilege returns nil if user has any privileges at all.
	CheckAnyPrivilege(ctx context.Context, descriptor sqlbase.DescriptorProto) error

	// RequiresSuperUser errors if the session user isn't a super-user (i.e. root
	// or node). Includes the named action in the error message.
	RequireSuperUser(ctx context.Context, action string) error

	// MemberOfWithAdminOption looks up all the roles (direct and indirect) that 'member' is a member
	// of and returns a map of role -> isAdmin.
	MemberOfWithAdminOption(ctx context.Context, member string) (map[string]bool, error)
}

var _ AuthorizationAccessor = &planner{}

// CheckPrivilegeForUser verifies that `user`` has `privilege` on `descriptor`.
// This is not part of the planner as the only caller (ccl/sqlccl/restore.go) does not have one.
func CheckPrivilegeForUser(
	_ context.Context, user string, descriptor sqlbase.DescriptorProto, privilege privilege.Kind,
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
	user := p.SessionData().User
	privs := descriptor.GetPrivileges()

	// Check if 'user' itself has privileges.
	if privs.CheckPrivilege(user, privilege) {
		return nil
	}

	// Expand role memberships.
	memberOf, err := p.MemberOfWithAdminOption(ctx, user)
	if err != nil {
		return err
	}

	// Iterate over the roles that 'user' is a member of. We don't care about the admin option.
	for role := range memberOf {
		if privs.CheckPrivilege(role, privilege) {
			return nil
		}
	}

	return fmt.Errorf("user %s does not have %s privilege on %s %s",
		user, privilege, descriptor.TypeName(), descriptor.GetName())
}

// CheckAnyPrivilege implements the AuthorizationAccessor interface.
func (p *planner) CheckAnyPrivilege(ctx context.Context, descriptor sqlbase.DescriptorProto) error {
	if isVirtualDescriptor(descriptor) {
		return nil
	}

	user := p.SessionData().User
	privs := descriptor.GetPrivileges()

	// Check if 'user' itself has privileges.
	if privs.AnyPrivilege(user) {
		return nil
	}

	// Expand role memberships.
	memberOf, err := p.MemberOfWithAdminOption(ctx, user)
	if err != nil {
		return err
	}

	// Iterate over the roles that 'user' is a member of. We don't care about the admin option.
	for role := range memberOf {
		if privs.AnyPrivilege(role) {
			return nil
		}
	}

	return fmt.Errorf("user %s has no privileges on %s %s",
		p.SessionData().User, descriptor.TypeName(), descriptor.GetName())
}

// RequireSuperUser implements the AuthorizationAccessor interface.
func (p *planner) RequireSuperUser(ctx context.Context, action string) error {
	user := p.SessionData().User

	// Check if user is 'root' or 'node'.
	if user == security.RootUser || user == security.NodeUser {
		return nil
	}

	// Expand role memberships.
	memberOf, err := p.MemberOfWithAdminOption(ctx, user)
	if err != nil {
		return err
	}

	// Check is 'user' is a member of role 'admin'.
	if _, ok := memberOf[sqlbase.AdminRole]; ok {
		return nil
	}

	return fmt.Errorf("only superusers are allowed to %s", action)
}

// MemberOfWithAdminOption looks up all the roles 'member' belongs to (direct and indirect) and
// returns a map of "role" -> "isAdmin".
// The "isAdmin" flag applies to both direct and indirect members.
// TODO(mberhault): this is too expensive: we need some type of caching with change notification.
func (p *planner) MemberOfWithAdminOption(
	ctx context.Context, member string,
) (map[string]bool, error) {
	ret := map[string]bool{}

	// Keep track of members we looked up.
	visited := map[string]struct{}{}

	toVisit := []string{member}

	lookupRolesStmt := `SELECT "role", "isAdmin" FROM system.public.role_members WHERE "member" = $1`

	internalExecutor := InternalExecutor{ExecCfg: p.ExecCfg()}

	for len(toVisit) > 0 {
		// Pop first element.
		m := toVisit[0]
		toVisit = toVisit[1:]
		if _, ok := visited[m]; ok {
			continue
		}
		visited[m] = struct{}{}

		rows, _ /* cols */, err := internalExecutor.QueryRowsInTransaction(
			ctx,
			"expand-roles",
			p.Txn(),
			lookupRolesStmt,
			m,
		)
		if err != nil {
			return nil, err
		}

		for _, row := range rows {
			roleName := tree.MustBeDString(row[0])
			isAdmin := row[1].(*tree.DBool)

			ret[string(roleName)] = bool(*isAdmin)

			// We need to expand this role. Let the "pop" worry about already-visited elements.
			toVisit = append(toVisit, string(roleName))
		}
	}

	return ret, nil
}
