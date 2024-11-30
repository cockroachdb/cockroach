// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/errors"
)

// AuthorizationAccessor for checking authorization (e.g. desc privileges).
type AuthorizationAccessor interface {
	// CheckPrivilegeForTableID verifies that the user has `privilege` on the table
	// denoted by `tableID`.
	CheckPrivilegeForTableID(
		ctx context.Context, tableID descpb.ID, privilege privilege.Kind,
	) error

	// HasPrivilege checks if the user has `privilege` on `descriptor`.
	HasPrivilege(ctx context.Context, privilegeObject privilege.Object, privilege privilege.Kind, user username.SQLUsername) (bool, error)

	// HasAnyPrivilege returns true if user has any privileges at all.
	HasAnyPrivilege(ctx context.Context, privilegeObject privilege.Object) (bool, error)

	// CheckPrivilege verifies that the user has `privilege` on `descriptor`.
	CheckPrivilegeForUser(
		ctx context.Context, privilegeObject privilege.Object, privilege privilege.Kind, user username.SQLUsername,
	) error

	// CheckPrivilege verifies that the current user has `privilege` on `descriptor`.
	CheckPrivilege(ctx context.Context, privilegeObject privilege.Object, privilege privilege.Kind) error

	// CheckAnyPrivilege returns nil if user has any privileges at all.
	CheckAnyPrivilege(ctx context.Context, descriptor privilege.Object) error

	// UserHasAdminRole returns tuple of bool and error:
	// (true, nil) means that the user has an admin role (i.e. root or node)
	// (false, nil) means that the user has NO admin role
	// (false, err) means that there was an error running the query on
	// the `system.users` table
	UserHasAdminRole(ctx context.Context, user username.SQLUsername) (bool, error)

	// HasAdminRole checks if the current session's user has admin role.
	HasAdminRole(ctx context.Context) (bool, error)

	// MemberOfWithAdminOption looks up all the roles (direct and indirect) that 'member' is a member
	// of and returns a map of role -> isAdmin.
	MemberOfWithAdminOption(ctx context.Context, member username.SQLUsername) (map[username.SQLUsername]bool, error)

	// UserHasRoleOption converts the roleoption to its SQL column name and checks
	// if the user has that option. Requires a valid transaction to be open.
	//
	// This check should be done on the version of the privilege that is stored in
	// the role options table. Example: CREATEROLE instead of NOCREATEROLE.
	// NOLOGIN instead of LOGIN.
	UserHasRoleOption(ctx context.Context, user username.SQLUsername, roleOption roleoption.Option) (bool, error)

	// HasRoleOption calls UserHasRoleOption with the current user.
	HasRoleOption(ctx context.Context, roleOption roleoption.Option) (bool, error)

	// HasGlobalPrivilegeOrRoleOption returns a bool representing whether the current user
	// has a global privilege or the corresponding legacy role option.
	HasGlobalPrivilegeOrRoleOption(ctx context.Context, privilege privilege.Kind) (bool, error)

	// CheckGlobalPrivilegeOrRoleOption checks if the current user has a global privilege
	// or the corresponding legacy role option, and returns an error if the user does not.
	CheckGlobalPrivilegeOrRoleOption(ctx context.Context, privilege privilege.Kind) error
}

var _ AuthorizationAccessor = &planner{}

// HasPrivilege is part of the AuthorizationAccessor interface.
func (p *planner) HasPrivilege(
	ctx context.Context,
	privilegeObject privilege.Object,
	privilegeKind privilege.Kind,
	user username.SQLUsername,
) (bool, error) {
	// Skip privilege checking if `privilegeKind == 0`. This exists to enable a
	// more cohesive coding style in the caller where CheckPrivilege(priv=0)
	// means "do not perform any privilege check".
	if privilegeKind == 0 {
		return true, nil
	}

	// Verify that the txn is valid in any case, so that
	// we don't get the risk to say "OK" to root requests
	// with an invalid API usage.
	if p.txn == nil {
		return false, errors.AssertionFailedf("cannot use CheckPrivilege without a txn")
	}

	// root, admin and node user should always have privileges, except NOSQLLOGIN.
	// This allows us to short-circuit privilege checks for
	// virtual object such that we don't have to query the system.privileges
	// table. This is especially import for internal executor queries. Right now
	// we only short-circuit non-descriptor backed objects. There are certain
	// descriptor backed objects that we can't short-circuit, ie system tables.
	if (user.IsRootUser() || user.IsAdminRole() || user.IsNodeUser()) &&
		!privilegeObject.GetObjectType().IsDescriptorBacked() &&
		privilegeKind != privilege.NOSQLLOGIN {
		validPrivs, err := privilege.GetValidPrivilegesForObject(privilegeObject.GetObjectType())
		if err != nil {
			return false, err
		}
		if validPrivs.Contains(privilegeKind) {
			return true, nil
		}
		return false, nil
	}

	// Test whether the object is being audited, and if so, record an
	// audit event. We place this check here to increase the likelihood
	// it will not be forgotten if features are added that access
	// descriptors (since every use of descriptors presumably need a
	// permission check).
	p.maybeAuditSensitiveTableAccessEvent(privilegeObject, privilegeKind)

	privs, err := p.getPrivilegeDescriptor(ctx, privilegeObject)
	if err != nil {
		return false, err
	}

	// Check if the 'public' pseudo-role has privileges.
	if privs.CheckPrivilege(username.PublicRoleName(), privilegeKind) {
		// Before returning true, make sure the user actually exists.
		// We only need to check for existence here, since a dropped user will not
		// appear in the role hierarchy, so it cannot pass the privilege check made
		// later in this function. The RoleExists call performs a system table
		// lookup, so it's better not to do it in the general case.
		if user.IsNodeUser() || user.IsRootUser() {
			// Short-circuit for the node and root users to avoid doing an extra
			// lookup in common cases (e.g., internal executor usages).
			return true, nil
		}
		if err := p.CheckRoleExists(ctx, user); err != nil {
			return false, pgerror.Wrapf(err, pgcode.UndefinedObject, "role %s was concurrently dropped", user)
		}
		return true, nil
	}

	hasPriv, err := p.checkRolePredicate(ctx, user, func(role username.SQLUsername) (bool, error) {
		isOwner, err := isOwner(ctx, p, privilegeObject, role)
		return isOwner || privs.CheckPrivilege(role, privilegeKind), err
	})
	if err != nil {
		return false, err
	}
	if hasPriv {
		return true, nil
	}
	return false, nil
}

// HasAnyPrivilege is part of the AuthorizationAccessor interface.
func (p *planner) HasAnyPrivilege(
	ctx context.Context, privilegeObject privilege.Object,
) (bool, error) {
	// Verify that the txn is valid in any case, so that
	// we don't get the risk to say "OK" to root requests
	// with an invalid API usage.
	if p.txn == nil {
		return false, errors.AssertionFailedf("cannot use CheckAnyPrivilege without a txn")
	}

	user := p.SessionData().User()
	if user.IsNodeUser() {
		// User "node" has all privileges.
		return true, nil
	}

	privs, err := p.getPrivilegeDescriptor(ctx, privilegeObject)
	if err != nil {
		return false, err
	}

	// Check if 'user' itself has privileges.
	if privs.AnyPrivilege(user) {
		return true, nil
	}

	// Check if 'public' has privileges.
	if privs.AnyPrivilege(username.PublicRoleName()) {
		return true, nil
	}

	// Expand role memberships.
	memberOf, err := p.MemberOfWithAdminOption(ctx, user)
	if err != nil {
		return false, err
	}

	// Iterate over the roles that 'user' is a member of. We don't care about the admin option.
	for role := range memberOf {
		if privs.AnyPrivilege(role) {
			return true, nil
		}
	}

	return false, nil
}

// CheckPrivilegeForUser implements the AuthorizationAccessor interface.
// Requires a valid transaction to be open.
func (p *planner) CheckPrivilegeForUser(
	ctx context.Context,
	privilegeObject privilege.Object,
	privilegeKind privilege.Kind,
	user username.SQLUsername,
) error {
	hasPriv, err := p.HasPrivilege(ctx, privilegeObject, privilegeKind, user)
	if err != nil {
		return err
	}
	if hasPriv {
		return nil
	}
	// Special case for system tables. The VIEWSYSTEMTABLE system privilege is
	// equivalent to having SELECT on all system tables. This is because it is not
	// possible to dynamically grant SELECT privileges system tables, but in the
	// context of support escalations, we need to be able to grant the ability to
	// view system tables without granting the entire admin role.
	if d, ok := privilegeObject.(catalog.Descriptor); ok {
		if catalog.IsSystemDescriptor(d) && privilegeKind == privilege.SELECT {
			hasViewSystemTablePriv, err := p.HasPrivilege(
				ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.VIEWSYSTEMTABLE, user,
			)
			if err != nil {
				return err
			}
			if hasViewSystemTablePriv {
				return nil
			}
		}
	}
	return insufficientPrivilegeError(user, privilegeKind, privilegeObject)
}

// CheckPrivilege implements the AuthorizationAccessor interface.
// Requires a valid transaction to be open.
// TODO(arul): This CheckPrivileges method name is rather deceptive,
// it should be probably be called CheckPrivilegesOrOwnership and return
// a better error.
func (p *planner) CheckPrivilege(
	ctx context.Context, object privilege.Object, privilege privilege.Kind,
) error {
	return p.CheckPrivilegeForUser(ctx, object, privilege, p.User())
}

// MustCheckGrantOptionsForUser calls PrivilegeDescriptor.CheckGrantOptions, which
// will return an error if a user tries to grant a privilege it does not have
// grant options for. Owners implicitly have all grant options, and also grant
// options are inherited from parent roles.
func (p *planner) MustCheckGrantOptionsForUser(
	ctx context.Context,
	privs *catpb.PrivilegeDescriptor,
	privilegeObject privilege.Object,
	privList privilege.List,
	user username.SQLUsername,
	isGrant bool,
) error {
	if hasPriv, err := p.CheckGrantOptionsForUser(
		ctx, privs, privilegeObject, privList, user,
	); hasPriv || err != nil {
		return err
	}
	code := pgcode.WarningPrivilegeNotGranted
	if !isGrant {
		code = pgcode.WarningPrivilegeNotRevoked
	}
	if privList.Len() > 1 {
		return pgerror.Newf(
			code, "user %s missing WITH GRANT OPTION privilege on one or more of %s",
			user, privList,
		)
	}
	return pgerror.Newf(
		code, "user %s missing WITH GRANT OPTION privilege on %s",
		user, privList,
	)
}

// CheckGrantOptionsForUser is like MustCheckGrantOptionsForUser but does not
// return an error if the check does not succeed due to privileges. It only
// returns an error if there is a runtime problem calculating whether the
// user has the priviliges in question.
func (p *planner) CheckGrantOptionsForUser(
	ctx context.Context,
	privs *catpb.PrivilegeDescriptor,
	privilegeObject privilege.Object,
	privList privilege.List,
	user username.SQLUsername,
) (isGrantable bool, _ error) {
	// Always allow the command to go through if performed by a superuser or the
	// owner of the object
	isAdmin, err := p.UserHasAdminRole(ctx, user)
	if err != nil {
		return false, err
	}
	if isAdmin {
		return true, nil
	}

	// Normally, we check the user and its ancestors. But if
	// enableGrantOptionInheritance is false, then we only check the user.
	runPredicateFn := p.checkRolePredicate
	if !enableGrantOptionInheritance.Get(&p.ExecCfg().Settings.SV) {
		runPredicateFn = func(ctx context.Context, user username.SQLUsername, predicate func(role username.SQLUsername) (bool, error)) (bool, error) {
			return predicate(user)
		}
	}

	return runPredicateFn(ctx, user, func(role username.SQLUsername) (bool, error) {
		if enableGrantOptionForOwner.Get(&p.ExecCfg().Settings.SV) {
			if owned, err := isOwner(ctx, p, privilegeObject, role); err != nil {
				return false, err
			} else if owned {
				// Short-circuit if the role is the owner of the object.
				return true, nil
			}
		}
		return privs.CheckGrantOptions(role, privList), nil
	})
}

func (p *planner) getOwnerOfPrivilegeObject(
	ctx context.Context, privilegeObject privilege.Object,
) (username.SQLUsername, error) {
	// Short-circuit for virtual tables, which are all owned by node. This allows
	// us to avoid fetching the synthetic privileges for virtual tables in a
	// thundering herd while populating a table like pg_class, which has a row for
	// every table, including virtual tables.
	if d, ok := privilegeObject.(catalog.TableDescriptor); ok && d.IsVirtualTable() {
		return username.NodeUserName(), nil
	}
	privDesc, err := p.getPrivilegeDescriptor(ctx, privilegeObject)
	if err != nil {
		return username.SQLUsername{}, err
	}
	// Descriptors created prior to 20.2 do not have owners set.
	owner := privDesc.Owner()
	if owner.Undefined() {
		// If the descriptor is ownerless and the descriptor is part of the system db,
		// node is the owner.
		if d, ok := privilegeObject.(catalog.Descriptor); ok && catalog.IsSystemDescriptor(d) {
			owner = username.NodeUserName()
		} else {
			// This check is redundant in this case since admin already has privilege
			// on all non-system objects.
			owner = username.AdminRoleName()
		}
	}
	return owner, nil
}

// isOwner returns if the role has ownership on the privilege object. The admin
// and root roles implicitly have ownership of all objects that are not
// owned by node, and node implicitly owns all objects.
func isOwner(
	ctx context.Context, p *planner, privilegeObject privilege.Object, role username.SQLUsername,
) (bool, error) {
	owner, err := p.getOwnerOfPrivilegeObject(ctx, privilegeObject)
	if err != nil {
		return false, err
	}
	if role.IsNodeUser() {
		return true, nil
	}
	if !owner.IsNodeUser() {
		if role.IsAdminRole() || role.IsRootUser() {
			return true, nil
		}
	}
	return role == owner, nil
}

// HasOwnership returns if the role or any role the role is a member of
// has ownership privilege of the desc. Admins have ownership of all objects.
// TODO(richardjcai): SUPERUSER has implicit ownership.
// We do not have SUPERUSER privilege yet but should we consider root a superuser?
func (p *planner) HasOwnership(
	ctx context.Context, privilegeObject privilege.Object,
) (bool, error) {
	user := p.SessionData().User()

	return p.checkRolePredicate(ctx, user, func(role username.SQLUsername) (bool, error) {
		return isOwner(ctx, p, privilegeObject, role)
	})
}

// checkRolePredicate checks if the predicate is true for the user or
// any roles the user is a member of.
func (p *planner) checkRolePredicate(
	ctx context.Context,
	user username.SQLUsername,
	predicate func(role username.SQLUsername) (bool, error),
) (bool, error) {
	ok, err := predicate(user)
	if err != nil {
		return false, err
	}
	if ok {
		return ok, nil
	}

	memberOf, err := p.MemberOfWithAdminOption(ctx, user)
	if err != nil {
		return false, err
	}
	for role := range memberOf {
		ok, err := predicate(role)
		if err != nil {
			return false, err
		}
		if ok {
			return ok, nil
		}
	}
	return false, nil
}

// CheckAnyPrivilege implements the AuthorizationAccessor interface.
// Requires a valid transaction to be open.
func (p *planner) CheckAnyPrivilege(ctx context.Context, privilegeObject privilege.Object) error {
	user := p.SessionData().User()
	ok, err := p.HasAnyPrivilege(ctx, privilegeObject)
	if err != nil {
		return err
	}
	if !ok {
		return insufficientPrivilegeError(user, 0 /* kind */, privilegeObject)
	}
	return nil
}

// UserHasAdminRole implements the AuthorizationAccessor interface.
// Requires a valid transaction to be open.
func (p *planner) UserHasAdminRole(ctx context.Context, user username.SQLUsername) (bool, error) {
	if user.Undefined() {
		return false, sqlerrors.NewUndefinedUserError(user)
	}
	// Verify that the txn is valid in any case, so that
	// we don't get the risk to say "OK" to root requests
	// with an invalid API usage.
	if p.txn == nil {
		return false, errors.AssertionFailedf("cannot use HasAdminRole without a txn")
	}

	// Check if user is either 'admin', 'root' or 'node'.
	// TODO(knz): planner HasAdminRole has no business authorizing
	// the "node" principal - node should not be issuing SQL queries.
	if user.IsAdminRole() || user.IsRootUser() || user.IsNodeUser() {
		return true, nil
	}
	if user.IsPublicRole() {
		return false, nil
	}

	// Expand role memberships.
	memberOf, err := p.MemberOfWithAdminOption(ctx, user)
	if err != nil {
		return false, err
	}

	// Check is 'user' is a member of role 'admin'.
	if _, ok := memberOf[username.AdminRoleName()]; ok {
		return true, nil
	}

	return false, nil
}

// HasAdminRole implements the AuthorizationAccessor interface.
// Requires a valid transaction to be open.
func (p *planner) HasAdminRole(ctx context.Context) (bool, error) {
	return p.UserHasAdminRole(ctx, p.User())
}

// MemberOfWithAdminOption is a wrapper around the MemberOfWithAdminOption
// method.
func (p *planner) MemberOfWithAdminOption(
	ctx context.Context, member username.SQLUsername,
) (map[username.SQLUsername]bool, error) {
	return MemberOfWithAdminOption(
		ctx,
		p.execCfg,
		p.InternalSQLTxn(),
		member,
	)
}

// MemberOfWithAdminOption looks up all the roles 'member' belongs to (direct and indirect) and
// returns a map of "role" -> "isAdmin".
// The "isAdmin" flag applies to both direct and indirect members.
// Requires a valid transaction to be open.
func MemberOfWithAdminOption(
	ctx context.Context, execCfg *ExecutorConfig, txn descs.Txn, member username.SQLUsername,
) (_ map[username.SQLUsername]bool, retErr error) {
	return execCfg.RoleMemberCache.GetRolesForMember(ctx, txn, member)
}

// EnsureUserOnlyBelongsToRoles grants all the roles in `roles` to `user` and,
// revokes all other roles. This is intended to be used when there is an
// external source of truth for role membership (e.g. an LDAP server), and we
// need to keep role memberships in sync with that source of truth.
func EnsureUserOnlyBelongsToRoles(
	ctx context.Context,
	execCfg *ExecutorConfig,
	user username.SQLUsername,
	roles []username.SQLUsername,
) error {
	return execCfg.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		currentRoles, err := MemberOfWithAdminOption(ctx, execCfg, txn, user)
		if err != nil {
			return err
		}

		// Compute the differences between the current roles and the desired roles
		// to determine which roles need to granted or revoked. This will ensure
		// that if the actual roles and desired roles already match, then no work
		// will be performed.
		rolesToRevoke := make([]username.SQLUsername, 0, len(currentRoles))
		rolesToGrant := make([]username.SQLUsername, 0, len(roles))
		for role := range currentRoles {
			if !slices.Contains(roles, role) {
				rolesToRevoke = append(rolesToRevoke, role)
			}
		}
		for _, role := range roles {
			if _, ok := currentRoles[role]; !ok {
				rolesToGrant = append(rolesToGrant, role)
			}
		}

		if len(rolesToRevoke) > 0 {
			revokeStmt := strings.Builder{}
			revokeStmt.WriteString("REVOKE ")
			addComma := false
			for _, role := range rolesToRevoke {
				if addComma {
					revokeStmt.WriteString(", ")
				}
				revokeStmt.WriteString(role.SQLIdentifier())
				addComma = true
			}
			revokeStmt.WriteString(" FROM ")
			revokeStmt.WriteString(user.SQLIdentifier())
			if _, err := txn.Exec(
				ctx, "EnsureUserOnlyBelongsToRoles-revoke", txn.KV(), revokeStmt.String(),
			); err != nil {
				return err
			}
		}

		if len(rolesToGrant) > 0 {
			grantStmt := strings.Builder{}
			grantStmt.WriteString("GRANT ")
			addComma := false
			for _, role := range rolesToGrant {
				if roleExists, _ := RoleExists(ctx, txn, role); roleExists {
					if addComma {
						grantStmt.WriteString(", ")
					}
					grantStmt.WriteString(role.SQLIdentifier())
					addComma = true
				}
			}
			grantStmt.WriteString(" TO ")
			grantStmt.WriteString(user.SQLIdentifier())
			if _, err := txn.Exec(
				ctx, "EnsureUserOnlyBelongsToRoles-grant", txn.KV(), grantStmt.String(),
			); err != nil {
				return err
			}
		}

		return nil
	})
}

var enableGrantOptionInheritance = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.auth.grant_option_inheritance.enabled",
	"determines whether the GRANT OPTION for privileges is inherited through role membership",
	true,
	settings.WithPublic,
)

var enableGrantOptionForOwner = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.auth.grant_option_for_owner.enabled",
	"determines whether the GRANT OPTION for privileges is implicitly given to the owner of an object",
	true,
	settings.WithPublic,
)

// UserHasRoleOption implements the AuthorizationAccessor interface.
func (p *planner) UserHasRoleOption(
	ctx context.Context, user username.SQLUsername, roleOption roleoption.Option,
) (bool, error) {
	// Verify that the txn is valid in any case, so that
	// we don't get the risk to say "OK" to root requests
	// with an invalid API usage.
	if p.txn == nil {
		return false, errors.AssertionFailedf("cannot use HasRoleOption without a txn")
	}

	if user.IsRootUser() || user.IsNodeUser() {
		return true, nil
	}

	hasAdmin, err := p.UserHasAdminRole(ctx, user)
	if err != nil {
		return false, err
	}
	if hasAdmin {
		// Superusers have all role privileges.
		return true, nil
	}

	hasRolePrivilege, err := p.InternalSQLTxn().QueryRowEx(
		ctx, "has-role-option", p.Txn(),
		sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf(
			`SELECT 1 from system.public.%s WHERE option = '%s' AND username = $1 LIMIT 1`,
			catconstants.RoleOptionsTableName, roleOption.String()), user.Normalized())
	if err != nil {
		return false, err
	}

	if len(hasRolePrivilege) != 0 {
		return true, nil
	}

	return false, nil
}

// HasRoleOption implements the AuthorizationAccessor interface.
func (p *planner) HasRoleOption(ctx context.Context, roleOption roleoption.Option) (bool, error) {
	return p.UserHasRoleOption(ctx, p.User(), roleOption)
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

// HasGlobalPrivilegeOrRoleOption implements the AuthorizationAccessor interface.
func (p *planner) HasGlobalPrivilegeOrRoleOption(
	ctx context.Context, privilege privilege.Kind,
) (bool, error) {
	ok, err := p.HasPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege, p.User())
	if err != nil {
		return false, err
	}
	if ok {
		return true, nil
	}
	maybeRoleOptionName := string(privilege.DisplayName())
	if roleOption, ok := roleoption.ByName[maybeRoleOptionName]; ok {
		return p.HasRoleOption(ctx, roleOption)
	}
	return false, nil
}

// CheckGlobalPrivilegeOrRoleOption implements the AuthorizationAccessor interface.
func (p *planner) CheckGlobalPrivilegeOrRoleOption(
	ctx context.Context, privilege privilege.Kind,
) error {
	ok, err := p.HasGlobalPrivilegeOrRoleOption(ctx, privilege)
	if err != nil {
		return err
	}
	if !ok {
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"user %s does not have %s privilege", p.User(), privilege)
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

// shouldCheckPublicSchema indicates whether canCreateOnSchema should check
// CREATE privileges for the public schema.
type shouldCheckPublicSchema bool

const (
	checkPublicSchema     shouldCheckPublicSchema = true
	skipCheckPublicSchema shouldCheckPublicSchema = false
)

// canCreateOnSchema returns whether a user has permission to create new objects
// on the specified schema. For `public` schemas, it checks if the user has
// CREATE privileges on the specified dbID. Note that skipCheckPublicSchema may
// be passed to skip this check, since some callers check this separately.
//
// Privileges on temporary schemas are not validated. This is the caller's
// responsibility.
func (p *planner) canCreateOnSchema(
	ctx context.Context,
	schemaID descpb.ID,
	dbID descpb.ID,
	user username.SQLUsername,
	checkPublicSchema shouldCheckPublicSchema,
) error {
	scDesc, err := p.Descriptors().ByIDWithLeased(p.Txn()).WithoutNonPublic().Get().Schema(ctx, schemaID)
	if err != nil {
		return err
	}

	switch kind := scDesc.SchemaKind(); kind {
	case catalog.SchemaPublic:
		// The public schema is valid to create in if the parent database is.
		if !checkPublicSchema {
			// The caller wishes to skip this check.
			return nil
		}
		dbDesc, err := p.Descriptors().ByIDWithLeased(p.Txn()).WithoutNonPublic().Get().Database(ctx, dbID)
		if err != nil {
			return err
		}
		return p.CheckPrivilegeForUser(ctx, dbDesc, privilege.CREATE, user)
	case catalog.SchemaTemporary:
		// Callers must check whether temporary schemas are valid to create in.
		return nil
	case catalog.SchemaVirtual:
		return sqlerrors.NewCannotModifyVirtualSchemaError(scDesc.GetName())
	case catalog.SchemaUserDefined:
		return p.CheckPrivilegeForUser(ctx, scDesc, privilege.CREATE, user)
	default:
		panic(errors.AssertionFailedf("unknown schema kind %d", kind))
	}
}

// checkCanAlterToNewOwner checks that the new owner exists and the current user
// has privileges to alter the owner of the object. If the current user is not
// a superuser, it also checks that they are a member of the new owner role.
func (p *planner) checkCanAlterToNewOwner(
	ctx context.Context, desc catalog.MutableDescriptor, newOwner username.SQLUsername,
) error {
	// Make sure the newOwner exists.
	if err := p.CheckRoleExists(ctx, newOwner); err != nil {
		return err
	}

	// If the user is a superuser, skip privilege checks.
	hasAdmin, err := p.HasAdminRole(ctx)
	if err != nil {
		return err
	}
	if hasAdmin {
		return nil
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
	case *funcdesc.Mutable:
		objType = "function"
	default:
		return errors.AssertionFailedf("unknown object descriptor type %v", desc)
	}

	hasOwnership, err := p.HasOwnership(ctx, desc)
	if err != nil {
		return err
	}
	if !hasOwnership {
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"must be owner of %s %s", tree.Name(objType), tree.Name(desc.GetName()))
	}

	// To alter the owner, you must also be a direct or indirect member of the new
	// owning role.
	if p.User() == newOwner {
		return nil
	}
	memberOf, err := p.MemberOfWithAdminOption(ctx, p.User())
	if err != nil {
		return err
	}
	if _, ok := memberOf[newOwner]; ok {
		return nil
	}
	return pgerror.Newf(pgcode.InsufficientPrivilege, "must be member of role %q", newOwner)
}

// HasOwnershipOnSchema checks if the current user has ownership on the schema.
// For schemas, we cannot always use HasOwnership as not every schema has a
// descriptor.
func (p *planner) HasOwnershipOnSchema(
	ctx context.Context, schemaID descpb.ID, dbID descpb.ID,
) (bool, error) {
	if dbID == keys.SystemDatabaseID {
		// Only the node user has ownership over the system database.
		return p.User().IsNodeUser(), nil
	}
	scDesc, err := p.Descriptors().ByIDWithLeased(p.Txn()).WithoutNonPublic().Get().Schema(ctx, schemaID)
	if err != nil {
		return false, err
	}

	hasOwnership := false
	switch kind := scDesc.SchemaKind(); kind {
	case catalog.SchemaPublic:
		// admin is the owner of the public schema.
		hasOwnership, err = p.UserHasAdminRole(ctx, p.User())
		if err != nil {
			return false, err
		}
	case catalog.SchemaVirtual:
		// Cannot drop on virtual schemas.
	case catalog.SchemaTemporary, catalog.SchemaUserDefined:
		hasOwnership, err = p.HasOwnership(ctx, scDesc)
		if err != nil {
			return false, err
		}
	default:
		panic(errors.AssertionFailedf("unknown schema kind %d", kind))
	}

	return hasOwnership, nil
}

// HasViewActivityOrViewActivityRedactedRole is part of the eval.SessionAccessor interface.
// It returns 2 boolean values - the first indicating if we have either privilege requested,
// and the second indicating  whether or not it was VIEWACTIVITYREDACTED.
// Requires a valid transaction to be open.
func (p *planner) HasViewActivityOrViewActivityRedactedRole(
	ctx context.Context,
) (hasPrivs bool, shouldRedact bool, err error) {
	if hasAdmin, err := p.HasAdminRole(ctx); err != nil {
		return hasAdmin, false, err
	} else if hasAdmin {
		return true, false, nil
	}

	// We check for VIEWACTIVITYREDACTED first as users can have both
	// VIEWACTIVITY and VIEWACTIVITYREDACTED, where VIEWACTIVITYREDACTED
	// takes precedence (i.e. we must redact senstitive values).
	if hasViewRedacted, err := p.HasGlobalPrivilegeOrRoleOption(ctx, privilege.VIEWACTIVITYREDACTED); err != nil {
		return false, false, err
	} else if hasViewRedacted {
		return true, true, nil
	}

	if hasView, err := p.HasGlobalPrivilegeOrRoleOption(ctx, privilege.VIEWACTIVITY); err != nil {
		return false, false, err
	} else if hasView {
		return true, false, nil
	}

	return false, false, nil
}

func insufficientPrivilegeError(
	user username.SQLUsername, kind privilege.Kind, object privilege.Object,
) error {
	// For consistency Postgres, we report the error message as not
	// having a privilege on the object type "relation".
	objTypeStr := object.GetObjectTypeString()
	objType := object.GetObjectType()
	if objType == privilege.VirtualTable || objType == privilege.Table || objType == privilege.Sequence {
		objTypeStr = "relation"
	}

	// If kind is 0 (no-privilege is 0), we return that the user has no privileges.
	if kind == 0 {
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"user %s has no privileges on %s %s",
			user, objTypeStr, object.GetName())
	}

	// Make a slightly different message for the global privilege object so that
	// it uses more understandable user-facing language.
	if object.GetObjectType() == privilege.Global {
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"user %s does not have %s system privilege",
			user, kind)
	}
	return sqlerrors.NewInsufficientPrivilegeOnDescriptorError(user, []privilege.Kind{kind}, objTypeStr, object.GetName())
}

// IsInsufficientPrivilegeError returns true if the error is a pgerror
// with code pgcode.InsufficientPrivilege.
func IsInsufficientPrivilegeError(err error) bool {
	return pgerror.GetPGCode(err) == pgcode.InsufficientPrivilege
}
