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

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// MembershipCache is a shared cache for role membership information.
type MembershipCache struct {
	syncutil.Mutex
	tableVersion sqlbase.DescriptorVersion
	// userCache is a mapping from username to userRoleMembership.
	userCache map[string]userRoleMembership
}

// userRoleMembership is a mapping of "rolename" -> "with admin option".
type userRoleMembership map[string]bool

// AuthorizationAccessor for checking authorization (e.g. desc privileges).
type AuthorizationAccessor interface {
	// CheckPrivilege verifies that the user has `privilege` on `descriptor`.
	CheckPrivilege(
		ctx context.Context, descriptor sqlbase.DescriptorInterface, privilege privilege.Kind,
	) error

	// CheckAnyPrivilege returns nil if user has any privileges at all.
	CheckAnyPrivilege(ctx context.Context, descriptor sqlbase.DescriptorInterface) error

	// HasAdminRole returns tuple of bool and error:
	// (true, nil) means that the user has an admin role (i.e. root or node)
	// (false, nil) means that the user has NO admin role
	// (false, err) means that there was an error running the query on
	// the `system.users` table
	HasAdminRole(ctx context.Context) (bool, error)

	// RequireAdminRole is a wrapper on top of HasAdminRole.
	// It errors if HasAdminRole errors or if the user isn't a super-user.
	// Includes the named action in the error message.
	RequireAdminRole(ctx context.Context, action string) error

	// MemberOfWithAdminOption looks up all the roles (direct and indirect) that 'member' is a member
	// of and returns a map of role -> isAdmin.
	MemberOfWithAdminOption(ctx context.Context, member string) (map[string]bool, error)
}

var _ AuthorizationAccessor = &planner{}

// CheckPrivilege implements the AuthorizationAccessor interface.
// Requires a valid transaction to be open.
func (p *planner) CheckPrivilege(
	ctx context.Context, descriptor sqlbase.DescriptorInterface, privilege privilege.Kind,
) error {
	// Verify that the txn is valid in any case, so that
	// we don't get the risk to say "OK" to root requests
	// with an invalid API usage.
	if p.txn == nil || !p.txn.IsOpen() {
		return errors.AssertionFailedf("cannot use CheckPrivilege without a txn")
	}

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
// Requires a valid transaction to be open.
func (p *planner) CheckAnyPrivilege(
	ctx context.Context, descriptor sqlbase.DescriptorInterface,
) error {
	// Verify that the txn is valid in any case, so that
	// we don't get the risk to say "OK" to root requests
	// with an invalid API usage.
	if p.txn == nil || !p.txn.IsOpen() {
		return errors.AssertionFailedf("cannot use CheckAnyPrivilege without a txn")
	}

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

// HasAdminRole implements the AuthorizationAccessor interface.
// Requires a valid transaction to be open.
func (p *planner) HasAdminRole(ctx context.Context) (bool, error) {
	user := p.SessionData().User
	if user == "" {
		return false, errors.AssertionFailedf("empty user")
	}
	// Verify that the txn is valid in any case, so that
	// we don't get the risk to say "OK" to root requests
	// with an invalid API usage.
	if p.txn == nil || !p.txn.IsOpen() {
		return false, errors.AssertionFailedf("cannot use HasAdminRole without a txn")
	}

	// Check if user is 'root' or 'node'.
	// TODO(knz): planner HasAdminRole has no business authorizing
	// the "node" principal - node should not be issuing SQL queries.
	if user == security.RootUser || user == security.NodeUser {
		return true, nil
	}

	// Expand role memberships.
	memberOf, err := p.MemberOfWithAdminOption(ctx, user)
	if err != nil {
		return false, err
	}

	// Check is 'user' is a member of role 'admin'.
	if _, ok := memberOf[sqlbase.AdminRole]; ok {
		return true, nil
	}

	return false, nil
}

// RequireAdminRole implements the AuthorizationAccessor interface.
// Requires a valid transaction to be open.
func (p *planner) RequireAdminRole(ctx context.Context, action string) error {
	ok, err := p.HasAdminRole(ctx)

	if err != nil {
		return err
	}
	if !ok {
		//raise error if user is not a super-user
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"only users with the admin role are allowed to %s", action)
	}
	return nil
}

// MemberOfWithAdminOption looks up all the roles 'member' belongs to (direct and indirect) and
// returns a map of "role" -> "isAdmin".
// The "isAdmin" flag applies to both direct and indirect members.
// Requires a valid transaction to be open.
func (p *planner) MemberOfWithAdminOption(
	ctx context.Context, member string,
) (map[string]bool, error) {
	if p.txn == nil || !p.txn.IsOpen() {
		return nil, errors.AssertionFailedf("cannot use MemberOfWithAdminoption without a txn")
	}

	roleMembersCache := p.execCfg.RoleMemberCache

	// Lookup table version.
	objDesc, err := p.PhysicalSchemaAccessor().GetObjectDesc(
		ctx,
		p.txn,
		p.ExecCfg().Settings,
		p.ExecCfg().Codec,
		roleMembersTableName.Catalog(),
		roleMembersTableName.Schema(),
		roleMembersTableName.Table(),
		p.ObjectLookupFlags(true /*required*/, false /*requireMutable*/),
	)
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
			roleMembersCache.tableVersion = tableVersion
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

// HasRoleOption converts the roleoption to it's SQL column name and
// checks if the user belongs to a role where the roleprivilege has value true.
// Only works on checking the "positive version" of the privilege.
// Requires a valid transaction to be open.
// Example: CREATEROLE instead of NOCREATEROLE.
func (p *planner) HasRoleOption(ctx context.Context, roleOption roleoption.Option) error {
	// Verify that the txn is valid in any case, so that
	// we don't get the risk to say "OK" to root requests
	// with an invalid API usage.
	if p.txn == nil || !p.txn.IsOpen() {
		return errors.AssertionFailedf("cannot use HasRoleOption without a txn")
	}

	user := p.SessionData().User
	if user == security.RootUser || user == security.NodeUser {
		return nil
	}

	normalizedName, err := NormalizeAndValidateUsername(user)
	if err != nil {
		return err
	}

	// Create list of roles for sql WHERE IN clause.
	memberOf, err := p.MemberOfWithAdminOption(ctx, normalizedName)
	if err != nil {
		return err
	}

	var roles = tree.NewDArray(types.String)
	err = roles.Append(tree.NewDString(normalizedName))
	if err != nil {
		return err
	}
	for role := range memberOf {
		err := roles.Append(tree.NewDString(role))
		if err != nil {
			return err
		}
	}

	hasRolePrivilege, err := p.ExecCfg().InternalExecutor.QueryEx(
		ctx, "has-role-option", p.Txn(),
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		fmt.Sprintf(
			`SELECT 1 from %s WHERE option = '%s' AND username = ANY($1) LIMIT 1`,
			RoleOptionsTableName,
			roleOption.String()),
		roles)

	if err != nil {
		return err
	}

	if len(hasRolePrivilege) != 0 {
		return nil
	}

	// User is not a member of a role that has CREATEROLE privilege.
	return pgerror.Newf(pgcode.InsufficientPrivilege,
		"user %s does not have %s privilege", user, roleOption.String())
}

// ConnAuditingClusterSettingName is the name of the cluster setting
// for the cluster setting that enables pgwire-level connection audit
// logs.
//
// This name is defined here because it is needed in the telemetry
// counts in SetClusterSetting() and importing pgwire here would
// create a circular dependency.
const ConnAuditingClusterSettingName = "server.auth_log.sql_connections.enabled"

// AuthAuditingClusterSettingName is the name of the cluster setting
// for the cluster setting that enables pgwire-level authentication audit
// logs.
//
// This name is defined here because it is needed in the telemetry
// counts in SetClusterSetting() and importing pgwire here would
// create a circular dependency.
const AuthAuditingClusterSettingName = "server.auth_log.sql_sessions.enabled"
