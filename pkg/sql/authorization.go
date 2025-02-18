// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/memsize"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessioninit"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/errors"
)

// MembershipCache is a shared cache for role membership information.
type MembershipCache struct {
	syncutil.Mutex
	tableVersion descpb.DescriptorVersion
	boundAccount mon.BoundAccount
	// userCache is a mapping from username to userRoleMembership.
	userCache map[username.SQLUsername]userRoleMembership
	// populateCacheGroup ensures that there is at most one request in-flight
	// for each key.
	populateCacheGroup *singleflight.Group
	// readTS is the timestamp that was used to populate the cache.
	readTS  hlc.Timestamp
	stopper *stop.Stopper
}

// NewMembershipCache initializes a new MembershipCache.
func NewMembershipCache(account mon.BoundAccount, stopper *stop.Stopper) *MembershipCache {
	return &MembershipCache{
		boundAccount:       account,
		populateCacheGroup: singleflight.NewGroup("lookup role membership", "key"),
		stopper:            stopper,
	}
}

// RunAtCacheReadTS runs an operations at a timestamp that is guaranteed to
// be consistent with the data in the cache. txn is used to check if the
// table version matches the cached table version, and if it does, db is
// used to start a separate transaction at the cached timestamp.
func (m *MembershipCache) RunAtCacheReadTS(
	ctx context.Context, db descs.DB, txn descs.Txn, f func(context.Context, descs.Txn) error,
) error {
	tableDesc, err := txn.Descriptors().ByIDWithLeased(txn.KV()).Get().Table(ctx, keys.RoleMembersTableID)
	if err != nil {
		return err
	}

	var readTS hlc.Timestamp
	func() {
		m.Lock()
		defer m.Unlock()
		if tableDesc.IsUncommittedVersion() {
			return
		}
		if tableDesc.GetVersion() > m.tableVersion {
			return
		}
		if tableDesc.GetVersion() < m.tableVersion {
			readTS = tableDesc.GetModificationTime()
			return
		}
		// The cached ts could be from long ago, so use the table modification
		// if it's more recent.
		if m.readTS.Less(tableDesc.GetModificationTime()) {
			readTS = tableDesc.GetModificationTime()
			return
		}
		readTS = m.readTS
	}()

	// If there's no cached read timestamp, then use the existing transaction.
	if readTS.IsEmpty() {
		return f(ctx, txn)
	}

	// If we found a historical timestamp to use, run in a different transaction.
	if err := db.DescsTxn(ctx, func(ctx context.Context, newTxn descs.Txn) error {
		if err := newTxn.KV().SetFixedTimestamp(ctx, readTS); err != nil {
			return err
		}
		return f(ctx, newTxn)
	}); err != nil {
		if errors.HasType(err, (*kvpb.BatchTimestampBeforeGCError)(nil)) {
			// If we picked a timestamp that has already been GC'd, then we modify
			// cache to mark it as stale so it's refreshed on the next access,
			// release the lease, and retry.
			func() {
				m.Lock()
				defer m.Unlock()
				m.tableVersion = 0
			}()
			txn.Descriptors().ReleaseSpecifiedLeases(ctx, []lease.IDVersion{
				{
					Name:    tableDesc.GetName(),
					ID:      tableDesc.GetID(),
					Version: tableDesc.GetVersion(),
				},
			})
			return m.RunAtCacheReadTS(ctx, db, txn, f)
		}
		return err
	}
	return nil
}

// userRoleMembership is a mapping of "rolename" -> "with admin option".
type userRoleMembership map[username.SQLUsername]bool

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
	if txn == nil {
		return nil, errors.AssertionFailedf("cannot use MemberOfWithAdminoption without a txn")
	}

	roleMembersCache := execCfg.RoleMemberCache

	// Lookup table version.
	_, roleMembersTableDesc, err := descs.PrefixAndTable(
		ctx, txn.Descriptors().ByNameWithLeased(txn.KV()).Get(), &roleMembersTableName,
	)
	if err != nil {
		return nil, err
	}

	tableVersion := roleMembersTableDesc.GetVersion()
	if roleMembersTableDesc.IsUncommittedVersion() {
		return resolveMemberOfWithAdminOption(ctx, member, txn)
	}
	if txn.SessionData().AllowRoleMembershipsToChangeDuringTransaction {
		systemUsersTableDesc, err := txn.Descriptors().ByIDWithLeased(txn.KV()).Get().Table(ctx, keys.UsersTableID)
		if err != nil {
			return nil, err
		}

		roleOptionsTableDesc, err := txn.Descriptors().ByIDWithLeased(txn.KV()).Get().Table(ctx, keys.RoleOptionsTableID)
		if err != nil {
			return nil, err
		}

		systemUsersTableVersion := systemUsersTableDesc.GetVersion()
		if systemUsersTableDesc.IsUncommittedVersion() {
			return resolveMemberOfWithAdminOption(ctx, member, txn)
		}

		roleOptionsTableVersion := roleOptionsTableDesc.GetVersion()
		if roleOptionsTableDesc.IsUncommittedVersion() {
			return resolveMemberOfWithAdminOption(ctx, member, txn)
		}

		defer func() {
			if retErr != nil {
				return
			}
			txn.Descriptors().ReleaseSpecifiedLeases(ctx, []lease.IDVersion{
				{
					Name:    roleMembersTableDesc.GetName(),
					ID:      roleMembersTableDesc.GetID(),
					Version: tableVersion,
				},
				{
					Name:    systemUsersTableDesc.GetName(),
					ID:      systemUsersTableDesc.GetID(),
					Version: systemUsersTableVersion,
				},
				{
					Name:    roleOptionsTableDesc.GetName(),
					ID:      roleOptionsTableDesc.GetID(),
					Version: roleOptionsTableVersion,
				},
			})
		}()
	}

	// Check version and maybe clear cache while holding the mutex.
	// We use a closure here so that we release the lock here, then keep
	// going and re-lock if adding the looked-up entry.
	userMapping, found, refreshCache := func() (userRoleMembership, bool, bool) {
		roleMembersCache.Lock()
		defer roleMembersCache.Unlock()
		if roleMembersCache.tableVersion < tableVersion {
			// If the cache is based on an old table version, then update version and
			// drop the map.
			roleMembersCache.tableVersion = tableVersion
			roleMembersCache.userCache = make(map[username.SQLUsername]userRoleMembership)
			roleMembersCache.boundAccount.Empty(ctx)
			return nil, false, true
		} else if roleMembersCache.tableVersion > tableVersion {
			// If the cache is based on a newer table version, then this transaction
			// should not use the cached data.
			return nil, false, true
		}
		userMapping, ok := roleMembersCache.userCache[member]
		return userMapping, ok, len(roleMembersCache.userCache) == 0
	}()

	if !refreshCache {
		// The cache always contains entries for every role that exists, so the
		// only time we need to refresh the cache is if it has been invalidated
		// by the tableVersion changing.
		if found {
			return userMapping, nil
		} else {
			return nil, sqlerrors.NewUndefinedUserError(member)
		}
	}

	// If `txn` is high priority and in a retry, do not launch the singleflight to
	// populate the cache because it can potentially cause a deadlock (see
	// TestConcurrentGrants/concurrent-GRANTs-high-priority for a repro) and
	// instead just issue a read using `txn` to `system.role_members` table and
	// return the result.
	if txn.KV().UserPriority() == roachpb.MaxUserPriority && txn.KV().Epoch() > 0 {
		return resolveMemberOfWithAdminOption(ctx, member, txn)
	}

	// Lookup memberships outside the lock. There will be at most one request
	// in-flight for each user. The role_memberships table version is also part
	// of the request key so that we don't read data from an old version of the
	// table.
	//
	// The singleflight closure uses a fresh transaction to prevent a data race
	// that may occur if the context is cancelled, leading to the outer txn
	// being cleaned up. We set the timestamp of this new transaction to be
	// the same as the outer transaction that already read the descriptor, to
	// ensure that we are reading from the right version of the table.
	newTxnTimestamp := txn.KV().ReadTimestamp()
	future, _ := roleMembersCache.populateCacheGroup.DoChan(ctx,
		fmt.Sprintf("refreshMembershipCache-%d", tableVersion),
		singleflight.DoOpts{
			Stop:               roleMembersCache.stopper,
			InheritCancelation: false,
		},
		func(ctx context.Context) (interface{}, error) {
			var allMemberships map[username.SQLUsername]userRoleMembership
			err = execCfg.InternalDB.Txn(ctx, func(ctx context.Context, newTxn isql.Txn) error {
				// Run the membership read as high-priority, thereby pushing any intents
				// out of its way. This prevents deadlocks in cases where a GRANT/REVOKE
				// txn, which has already laid a write intent on the
				// `system.role_members` table, waits for `newTxn` and `newTxn`, which
				// attempts to read the same system table, is blocked by the
				// GRANT/REVOKE txn.
				if err := newTxn.KV().SetUserPriority(roachpb.MaxUserPriority); err != nil {
					return err
				}
				err := newTxn.KV().SetFixedTimestamp(ctx, newTxnTimestamp)
				if err != nil {
					return err
				}
				allMemberships, err = resolveAllMemberships(ctx, newTxn)
				if err != nil {
					return err
				}
				return err
			})
			if err != nil {
				return nil, err
			}
			func() {
				// Update membership if the table version hasn't changed.
				roleMembersCache.Lock()
				defer roleMembersCache.Unlock()
				if roleMembersCache.tableVersion != tableVersion {
					// Table version has changed while we were looking: don't cache the data.
					return
				}

				// Table version remains the same: update map, unlock, return.
				sizeOfCache := int64(0)
				for _, memberships := range allMemberships {
					sizeOfEntry := int64(len(member.Normalized()))
					for m := range memberships {
						sizeOfEntry += int64(len(m.Normalized()))
						sizeOfEntry += memsize.Bool
					}
					sizeOfCache += sizeOfEntry
				}
				if err := roleMembersCache.boundAccount.Grow(ctx, sizeOfCache); err != nil {
					// If there is no memory available to cache the entry, we can still
					// proceed so that the query has a chance to succeed.
					log.Ops.Warningf(ctx, "no memory available to cache role membership info: %v", err)
				} else {
					roleMembersCache.userCache = allMemberships
					roleMembersCache.readTS = newTxnTimestamp
				}
			}()
			return allMemberships, nil
		})
	var allMemberships map[username.SQLUsername]userRoleMembership
	res := future.WaitForResult(ctx)
	if res.Err != nil {
		return nil, res.Err
	}
	allMemberships = res.Val.(map[username.SQLUsername]userRoleMembership)
	memberships, found := allMemberships[member]
	// The map contains entries for every role that exists, so if it's
	// not present in the map, the role does not exist.
	if !found {
		return nil, sqlerrors.NewUndefinedUserError(member)
	}
	return memberships, nil
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

// resolveMemberOfWithAdminOption performs the actual recursive role membership lookup.
func resolveMemberOfWithAdminOption(
	ctx context.Context, member username.SQLUsername, txn isql.Txn,
) (map[username.SQLUsername]bool, error) {
	allMemberships, err := resolveAllMemberships(ctx, txn)
	if err != nil {
		return nil, err
	}

	// allMemberships will have an entry for every user, so we first verify that
	// the user exists.
	ret, roleExists := allMemberships[member]
	if !roleExists {
		return nil, sqlerrors.NewUndefinedUserError(member)
	}

	return ret, nil
}

// membership represents a parent-child role relationship.
type membership struct {
	parent, child username.SQLUsername
	isAdmin       bool
}

func resolveAllMemberships(
	ctx context.Context, txn isql.Txn,
) (map[username.SQLUsername]userRoleMembership, error) {
	memberToDirectParents := make(map[username.SQLUsername][]membership)
	if err := forEachRoleWithMemberships(
		ctx, txn,
		func(ctx context.Context, role username.SQLUsername, memberships []membership) error {
			memberToDirectParents[role] = memberships
			return nil
		},
	); err != nil {
		return nil, err
	}
	// We need to add entries for the node and public roles, which do not have
	// rows in system.users.
	memberToDirectParents[username.NodeUserName()] = append(
		memberToDirectParents[username.NodeUserName()],
		membership{
			parent:  username.AdminRoleName(),
			child:   username.NodeUserName(),
			isAdmin: true,
		},
	)
	memberToDirectParents[username.PublicRoleName()] = []membership{}

	// Recurse through all roles associated with each user.
	ret := make(map[username.SQLUsername]userRoleMembership)
	for member := range memberToDirectParents {
		memberToAllAncestors := make(map[username.SQLUsername]bool)
		var recurse func(u username.SQLUsername)
		recurse = func(u username.SQLUsername) {
			for _, membership := range memberToDirectParents[u] {
				// If the parent role was seen before, we still might need to update
				// the isAdmin flag for that role, but there's no need to recurse
				// through the role's ancestry again.
				prev, alreadySeen := memberToAllAncestors[membership.parent]
				memberToAllAncestors[membership.parent] = prev || membership.isAdmin
				if !alreadySeen {
					recurse(membership.parent)
				}
			}
		}
		recurse(member)
		ret[member] = memberToAllAncestors
	}
	return ret, nil
}

func forEachRoleWithMemberships(
	ctx context.Context,
	txn isql.Txn,
	fn func(ctx context.Context, role username.SQLUsername, memberships []membership) error,
) (retErr error) {
	const query = `
SELECT
  u.username,
  array_agg(rm.role ORDER BY rank),
  array_agg(rm."isAdmin" ORDER BY rank)
FROM system.users u
LEFT OUTER JOIN (
  SELECT *, rank() OVER (PARTITION BY member ORDER BY role)
  FROM system.role_members
) rm ON u.username = rm.member
GROUP BY u.username;`
	it, err := txn.QueryIteratorEx(ctx, "read-role-with-memberships", txn.KV(),
		sessiondata.NodeUserSessionDataOverride, query)
	if err != nil {
		return err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()

	var ok bool
	var loopErr error
	for ok, loopErr = it.Next(ctx); ok; ok, loopErr = it.Next(ctx) {
		row := it.Cur()
		child := tree.MustBeDString(row[0])
		childName := username.MakeSQLUsernameFromPreNormalizedString(string(child))
		parentNames := tree.MustBeDArray(row[1])
		isAdmins := tree.MustBeDArray(row[2])

		memberships := make([]membership, 0, parentNames.Len())
		for i := 0; i < parentNames.Len(); i++ {
			if parentNames.Array[i] == tree.DNull {
				// A null element means this role has no parents.
				continue
			}
			// The names in the system tables are already normalized.
			parent := tree.MustBeDString(parentNames.Array[i])
			parentName := username.MakeSQLUsernameFromPreNormalizedString(string(parent))
			isAdmin := tree.MustBeDBool(isAdmins.Array[i])
			memberships = append(memberships, membership{
				parent:  parentName,
				child:   childName,
				isAdmin: bool(isAdmin),
			})
		}

		if err := fn(
			ctx,
			childName,
			memberships,
		); err != nil {
			return err
		}
	}
	if loopErr != nil {
		return loopErr
	}
	return nil
}

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
			`SELECT 1 from %s WHERE option = '%s' AND username = $1 LIMIT 1`,
			sessioninit.RoleOptionsTableName, roleOption.String()), user.Normalized())
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
