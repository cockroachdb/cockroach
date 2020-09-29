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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// MembershipCache is a shared cache for role membership information.
type MembershipCache struct {
	syncutil.Mutex
	tableVersion descpb.DescriptorVersion
	// userCache is a mapping from username to userRoleMembership.
	userCache map[string]userRoleMembership
}

// userRoleMembership is a mapping of "rolename" -> "with admin option".
type userRoleMembership map[string]bool

// AuthorizationAccessor for checking authorization (e.g. desc privileges).
type AuthorizationAccessor interface {
	// CheckPrivilege verifies that the user has `privilege` on `descriptor`.
	CheckPrivilegeForUser(
		ctx context.Context, descriptor catalog.Descriptor, privilege privilege.Kind, user string,
	) error

	// CheckPrivilege verifies that the current user has `privilege` on `descriptor`.
	CheckPrivilege(
		ctx context.Context, descriptor catalog.Descriptor, privilege privilege.Kind,
	) error

	// CheckAnyPrivilege returns nil if user has any privileges at all.
	CheckAnyPrivilege(ctx context.Context, descriptor catalog.Descriptor) error

	// UserHasAdminRole returns tuple of bool and error:
	// (true, nil) means that the user has an admin role (i.e. root or node)
	// (false, nil) means that the user has NO admin role
	// (false, err) means that there was an error running the query on
	// the `system.users` table
	UserHasAdminRole(ctx context.Context, user string) (bool, error)

	// HasAdminRole checks if the current session's user has admin role.
	HasAdminRole(ctx context.Context) (bool, error)

	// RequireAdminRole is a wrapper on top of HasAdminRole.
	// It errors if HasAdminRole errors or if the user isn't a super-user.
	// Includes the named action in the error message.
	RequireAdminRole(ctx context.Context, action string) error

	// MemberOfWithAdminOption looks up all the roles (direct and indirect) that 'member' is a member
	// of and returns a map of role -> isAdmin.
	MemberOfWithAdminOption(ctx context.Context, member string) (map[string]bool, error)

	// HasRoleOption converts the roleoption to its SQL column name and checks if
	// the user belongs to a role where the option has value true. Requires a
	// valid transaction to be open.
	//
	// This check should be done on the version of the privilege that is stored in
	// the role options table. Example: CREATEROLE instead of NOCREATEROLE.
	// NOLOGIN instead of LOGIN.
	HasRoleOption(ctx context.Context, roleOption roleoption.Option) (bool, error)
}

var _ AuthorizationAccessor = &planner{}

// CheckPrivilegeForUser implements the AuthorizationAccessor interface.
// Requires a valid transaction to be open.
func (p *planner) CheckPrivilegeForUser(
	ctx context.Context, descriptor catalog.Descriptor, privilege privilege.Kind, user string,
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

	privs := descriptor.GetPrivileges()

	// Check if the 'public' pseudo-role has privileges.
	if privs.CheckPrivilege(security.PublicRole, privilege) {
		return nil
	}

	hasPriv, err := p.checkRolePredicate(ctx, user, func(role string) bool {
		return IsOwner(descriptor, role) || privs.CheckPrivilege(role, privilege)
	})
	if err != nil {
		return err
	}
	if hasPriv {
		return nil
	}
	return pgerror.Newf(pgcode.InsufficientPrivilege,
		"user %s does not have %s privilege on %s %s",
		user, privilege, descriptor.TypeName(), descriptor.GetName())
}

// CheckPrivilege implements the AuthorizationAccessor interface.
// Requires a valid transaction to be open.
func (p *planner) CheckPrivilege(
	ctx context.Context, descriptor catalog.Descriptor, privilege privilege.Kind,
) error {
	return p.CheckPrivilegeForUser(ctx, descriptor, privilege, p.User())
}

func getOwnerOfDesc(desc catalog.Descriptor) string {
	// Descriptors created prior to 20.2 do not have owners set.
	owner := desc.GetPrivileges().Owner
	if owner == "" {
		// If the descriptor is ownerless and the descriptor is part of the system db,
		// node is the owner.
		if desc.GetID() == keys.SystemDatabaseID || desc.GetParentID() == keys.SystemDatabaseID {
			owner = security.NodeUser
		} else {
			// This check is redundant in this case since admin already has privilege
			// on all non-system objects.
			owner = security.AdminRole
		}
	}
	return owner
}

// IsOwner returns if the role has ownership on the descriptor.
func IsOwner(desc catalog.Descriptor, role string) bool {
	return role == getOwnerOfDesc(desc)
}

// HasOwnership returns if the role or any role the role is a member of
// has ownership privilege of the desc.
// TODO(richardjcai): SUPERUSER has implicit ownership.
// We do not have SUPERUSER privilege yet but should we consider root a superuser?
func (p *planner) HasOwnership(ctx context.Context, descriptor catalog.Descriptor) (bool, error) {
	user := p.SessionData().User

	return p.checkRolePredicate(ctx, user, func(role string) bool {
		return IsOwner(descriptor, role)
	})
}

// checkRolePredicate checks if the predicate is true for the user or
// any roles the user is a member of.
func (p *planner) checkRolePredicate(
	ctx context.Context, user string, predicate func(role string) bool,
) (bool, error) {
	if ok := predicate(user); ok {
		return ok, nil
	}
	memberOf, err := p.MemberOfWithAdminOption(ctx, user)
	if err != nil {
		return false, err
	}
	for role := range memberOf {
		if ok := predicate(role); ok {
			return ok, nil
		}
	}
	return false, nil
}

// CheckAnyPrivilege implements the AuthorizationAccessor interface.
// Requires a valid transaction to be open.
func (p *planner) CheckAnyPrivilege(ctx context.Context, descriptor catalog.Descriptor) error {
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
	if privs.AnyPrivilege(security.PublicRole) {
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

// UserHasAdminRole implements the AuthorizationAccessor interface.
// Requires a valid transaction to be open.
func (p *planner) UserHasAdminRole(ctx context.Context, user string) (bool, error) {
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
	if _, ok := memberOf[security.AdminRole]; ok {
		return true, nil
	}

	return false, nil
}

// HasAdminRole implements the AuthorizationAccessor interface.
// Requires a valid transaction to be open.
func (p *planner) HasAdminRole(ctx context.Context) (bool, error) {
	return p.UserHasAdminRole(ctx, p.User())
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
	tableDesc := objDesc.(catalog.TableDescriptor)
	tableVersion := tableDesc.GetVersion()

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

// HasRoleOption implements the AuthorizationAccessor interface.
func (p *planner) HasRoleOption(ctx context.Context, roleOption roleoption.Option) (bool, error) {
	// Verify that the txn is valid in any case, so that
	// we don't get the risk to say "OK" to root requests
	// with an invalid API usage.
	if p.txn == nil || !p.txn.IsOpen() {
		return false, errors.AssertionFailedf("cannot use HasRoleOption without a txn")
	}

	user := p.SessionData().User
	if user == security.RootUser || user == security.NodeUser {
		return true, nil
	}

	normalizedName, err := NormalizeAndValidateUsername(user)
	if err != nil {
		return false, err
	}

	// Create list of roles for sql WHERE IN clause.
	memberOf, err := p.MemberOfWithAdminOption(ctx, normalizedName)
	if err != nil {
		return false, err
	}

	var roles = tree.NewDArray(types.String)
	err = roles.Append(tree.NewDString(normalizedName))
	if err != nil {
		return false, err
	}
	for role := range memberOf {
		if role == security.AdminRole {
			// Superusers have all role privileges.
			return true, nil
		}
		err := roles.Append(tree.NewDString(role))
		if err != nil {
			return false, err
		}
	}

	hasRolePrivilege, err := p.ExecCfg().InternalExecutor.QueryEx(
		ctx, "has-role-option", p.Txn(),
		sessiondata.InternalExecutorOverride{User: security.RootUser},
		fmt.Sprintf(
			`SELECT 1 from %s WHERE option = '%s' AND username = ANY($1) LIMIT 1`,
			RoleOptionsTableName,
			roleOption.String()),
		roles)

	if err != nil {
		return false, err
	}

	if len(hasRolePrivilege) != 0 {
		return true, nil
	}

	return false, nil
}

// CheckRoleOption checks if the current user has roleOption and returns an
// insufficient privilege error if the user does not have roleOption.
func (p *planner) CheckRoleOption(ctx context.Context, roleOption roleoption.Option) error {
	hasRoleOption, err := p.HasRoleOption(ctx, roleOption)
	if err != nil {
		return err
	}

	if !hasRoleOption {
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"user %s does not have %s privilege", p.User(), roleOption)
	}

	return nil
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

func (p *planner) canCreateOnSchema(ctx context.Context, schemaID descpb.ID, user string) error {
	resolvedSchema, err := p.Descriptors().ResolveSchemaByID(ctx, p.Txn(), schemaID)
	if err != nil {
		return err
	}

	switch resolvedSchema.Kind {
	case catalog.SchemaPublic, catalog.SchemaTemporary:
		// Anyone can CREATE on a public schema, and callers check whether the
		// temporary schema is valid to create in.
		return nil
	case catalog.SchemaVirtual:
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"cannot CREATE on schema %s", resolvedSchema.Name)
	case catalog.SchemaUserDefined:
		return p.CheckPrivilegeForUser(ctx, resolvedSchema.Desc, privilege.CREATE, user)
	default:
		panic(errors.AssertionFailedf("unknown schema kind %d", resolvedSchema.Kind))
	}
}

func (p *planner) canResolveDescUnderSchema(
	ctx context.Context, schemaID descpb.ID, desc catalog.Descriptor,
) error {
	// We can't always resolve temporary schemas by ID (for example in the temporary
	// object cleaner which accesses temporary schemas not in the current session).
	// To avoid an internal error, we just don't check usage on temporary tables.
	if tbl, ok := desc.(catalog.TableDescriptor); ok && tbl.IsTemporary() {
		return nil
	}
	resolvedSchema, err := p.Descriptors().ResolveSchemaByID(ctx, p.Txn(), schemaID)
	if err != nil {
		return err
	}

	switch resolvedSchema.Kind {
	case catalog.SchemaPublic, catalog.SchemaTemporary, catalog.SchemaVirtual:
		// Anyone can resolve under temporary, public or virtual schemas.
		return nil
	case catalog.SchemaUserDefined:
		return p.CheckPrivilegeForUser(ctx, resolvedSchema.Desc, privilege.USAGE, p.User())
	default:
		panic(errors.AssertionFailedf("unknown schema kind %d", resolvedSchema.Kind))
	}
}

// checkCanAlterToNewOwner checks if the new owner exists, the current user
// has privileges to alter the owner of the object and the current user is a
//  member of the new owner role.
func (p *planner) checkCanAlterToNewOwner(
	ctx context.Context,
	desc catalog.MutableDescriptor,
	privs *descpb.PrivilegeDescriptor,
	newOwner string,
	hasOwnership bool,
) error {
	// Make sure the newOwner exists.
	roleExists, err := p.RoleExists(ctx, newOwner)
	if err != nil {
		return err
	}
	if !roleExists {
		return pgerror.Newf(pgcode.UndefinedObject, "role/user %q does not exist", newOwner)
	}

	var objType string
	switch desc.(type) {
	case *typedesc.Mutable:
		objType = "type"
	case *tabledesc.Mutable:
		objType = "table"
	case *schemadesc.Mutable:
		objType = "schema"
	case *dbdesc.Mutable:
		objType = "database"
	default:
		return errors.AssertionFailedf("unknown object descriptor type %v", desc)
	}

	// Make sure the user has ownership on the table
	// and not just create privilege.
	if !hasOwnership {
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"must be owner of %s %s", tree.Name(objType), tree.Name(desc.GetName()))
	}

	// Requirements from PG:
	// To alter the owner, you must also be a direct or indirect member of the
	// new owning role, and that role must have CREATE privilege on the
	// table's schema.
	memberOf, err := p.MemberOfWithAdminOption(ctx, privs.Owner)
	if err != nil {
		return err
	}
	if _, ok := memberOf[newOwner]; !ok {
		return pgerror.Newf(
			pgcode.InsufficientPrivilege, "must be member of role %q", newOwner)
	}

	return nil
}

// HasOwnershipOnSchema checks if the current user has ownership on the schema.
// For schemas, we cannot always use HasOwnership as not every schema has a
// descriptor.
func (p *planner) HasOwnershipOnSchema(ctx context.Context, schemaID descpb.ID) (bool, error) {
	resolvedSchema, err := p.Descriptors().ResolveSchemaByID(
		ctx, p.Txn(), schemaID,
	)
	if err != nil {
		return false, err
	}

	hasOwnership := false
	switch resolvedSchema.Kind {
	case catalog.SchemaPublic:
		// admin is the owner of the public schema.
		hasOwnership, err = p.UserHasAdminRole(ctx, p.User())
		if err != nil {
			return false, err
		}
	case catalog.SchemaVirtual:
		// Cannot drop on virtual schemas.
	case catalog.SchemaTemporary:
		// The user owns all the temporary schemas that they created in the session.
		hasOwnership = p.SessionData() != nil &&
			p.SessionData().IsTemporarySchemaID(uint32(resolvedSchema.ID))
	case catalog.SchemaUserDefined:
		hasOwnership, err = p.HasOwnership(ctx, resolvedSchema.Desc)
		if err != nil {
			return false, err
		}
	default:
		panic(errors.AssertionFailedf("unknown schema kind %d", resolvedSchema.Kind))
	}

	return hasOwnership, nil
}
