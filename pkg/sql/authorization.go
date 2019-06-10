// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type membershipCache struct {
	syncutil.Mutex
	tableVersion sqlbase.DescriptorVersion
	// userCache is a mapping from username to userRoleMembership.
	userCache map[string]userRoleMembership
}

var roleMembersCache membershipCache

// userRoleMembership is a mapping of "rolename" -> "with admin option".
type userRoleMembership map[string]bool

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
	return pgerror.Newf(pgcode.InsufficientPrivilege,
		"user %s does not have %s privilege on %s %s",
		user, privilege, descriptor.TypeName(), descriptor.GetName())
}

// CheckPrivilege implements the AuthorizationAccessor interface.
func (p *planner) CheckPrivilege(
	ctx context.Context, descriptor sqlbase.DescriptorProto, privilege privilege.Kind,
) error {
	// Test whether the object is being audited, and if so, record an
	// audit event. We place this check here to increase the likelihood
	// it will not be forgotten if features are added that access
	// descriptors (since every use of descriptors presumably need a
	// permission check).
	p.maybeAudit(descriptor, privilege)

	user := p.SessionData().User
	privs := descriptor.GetPrivileges()

	// Check if 'user' itself has privileges.
	if privs.CheckPrivilege(user, privilege) {
		return nil
	}

	// Check if the 'public' pseudo-role has privileges.
	if privs.CheckPrivilege(sqlbase.PublicRole, privilege) {
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

	return pgerror.Newf(pgcode.InsufficientPrivilege,
		"user %s does not have %s privilege on %s %s",
		user, privilege, descriptor.TypeName(), descriptor.GetName())
}

// CheckAnyPrivilege implements the AuthorizationAccessor interface.
func (p *planner) CheckAnyPrivilege(ctx context.Context, descriptor sqlbase.DescriptorProto) error {
	user := p.SessionData().User
	privs := descriptor.GetPrivileges()

	// Check if 'user' itself has privileges.
	if privs.AnyPrivilege(user) {
		return nil
	}

	// Check if 'public' has privileges.
	if privs.AnyPrivilege(sqlbase.PublicRole) {
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

	return pgerror.Newf(pgcode.InsufficientPrivilege,
		"user %s has no privileges on %s %s",
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

	return pgerror.Newf(pgcode.InsufficientPrivilege,
		"only superusers are allowed to %s", action)
}

// MemberOfWithAdminOption looks up all the roles 'member' belongs to (direct and indirect) and
// returns a map of "role" -> "isAdmin".
// The "isAdmin" flag applies to both direct and indirect members.
func (p *planner) MemberOfWithAdminOption(
	ctx context.Context, member string,
) (map[string]bool, error) {
	// Lookup table version.
	objDesc, err := p.PhysicalSchemaAccessor().GetObjectDesc(ctx, p.txn, &roleMembersTableName,
		p.ObjectLookupFlags(true /*required*/, false /*requireMutable*/))
	if err != nil {
		return nil, err
	}
	tableDesc := objDesc.TableDesc()
	tableVersion := tableDesc.Version

	// We loop in case the table version changes while we're looking up memberships.
	for {
		// Check version and maybe clear cache while holding the mutex.
		// We release the lock here instead of using defer as we need to keep
		// going and re-lock if adding the looked-up entry.
		roleMembersCache.Lock()
		if roleMembersCache.tableVersion != tableVersion {
			// Update version and drop the map.
			roleMembersCache.tableVersion = tableDesc.Version
			roleMembersCache.userCache = make(map[string]userRoleMembership)
		}

		userMapping, ok := roleMembersCache.userCache[member]
		roleMembersCache.Unlock()

		if ok {
			// Found: return.
			return userMapping, nil
		}

		// Lookup memberships outside the lock.
		memberships, err := p.resolveMemberOfWithAdminOption(ctx, member)
		if err != nil {
			return nil, err
		}

		// Update membership.
		roleMembersCache.Lock()
		if roleMembersCache.tableVersion != tableVersion {
			// Table version has changed while we were looking, unlock and start over.
			tableVersion = roleMembersCache.tableVersion
			roleMembersCache.Unlock()
			continue
		}

		// Table version remains the same: update map, unlock, return.
		roleMembersCache.userCache[member] = memberships
		roleMembersCache.Unlock()

		return memberships, nil
	}
}

// resolveMemberOfWithAdminOption performs the actual recursive role membership lookup.
// TODO(mberhault): this is the naive way and performs a full lookup for each user,
// we could save detailed memberships (as opposed to fully expanded) and reuse them
// across users. We may then want to lookup more than just this user.
func (p *planner) resolveMemberOfWithAdminOption(
	ctx context.Context, member string,
) (map[string]bool, error) {
	ret := map[string]bool{}

	// Keep track of members we looked up.
	visited := map[string]struct{}{}
	toVisit := []string{member}
	lookupRolesStmt := `SELECT "role", "isAdmin" FROM system.role_members WHERE "member" = $1`

	for len(toVisit) > 0 {
		// Pop first element.
		m := toVisit[0]
		toVisit = toVisit[1:]
		if _, ok := visited[m]; ok {
			continue
		}
		visited[m] = struct{}{}

		rows, err := p.ExecCfg().InternalExecutor.Query(
			ctx, "expand-roles", nil /* txn */, lookupRolesStmt, m,
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
