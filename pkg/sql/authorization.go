// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
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
	"github.com/cockroachdb/cockroach/pkg/util"
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
	stopper            *stop.Stopper
}

// NewMembershipCache initializes a new MembershipCache.
func NewMembershipCache(account mon.BoundAccount, stopper *stop.Stopper) *MembershipCache {
	return &MembershipCache{
		boundAccount:       account,
		populateCacheGroup: singleflight.NewGroup("lookup role membership", "key"),
		stopper:            stopper,
	}
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

	// HasRoleOption converts the roleoption to its SQL column name and checks if
	// the user belongs to a role where the option has value true. Requires a
	// valid transaction to be open.
	//
	// This check should be done on the version of the privilege that is stored in
	// the role options table. Example: CREATEROLE instead of NOCREATEROLE.
	// NOLOGIN instead of LOGIN.
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

// isOwner returns if the role has ownership on the privilege object.
func isOwner(
	ctx context.Context, p *planner, privilegeObject privilege.Object, role username.SQLUsername,
) (bool, error) {
	owner, err := p.getOwnerOfPrivilegeObject(ctx, privilegeObject)
	if err != nil {
		return false, err
	}
	return role == owner, nil
}

// HasOwnership returns if the role or any role the role is a member of
// has ownership privilege of the desc.
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
	_, tableDesc, err := descs.PrefixAndTable(
		ctx, txn.Descriptors().ByNameWithLeased(txn.KV()).Get(), &roleMembersTableName,
	)
	if err != nil {
		return nil, err
	}

	tableVersion := tableDesc.GetVersion()
	if tableDesc.IsUncommittedVersion() {
		return resolveMemberOfWithAdminOption(ctx, member, txn, useSingleQueryForRoleMembershipCache.Get(execCfg.SV()))
	}
	if txn.SessionData().AllowRoleMembershipsToChangeDuringTransaction {
		defer func() {
			if retErr != nil {
				return
			}
			txn.Descriptors().ReleaseSpecifiedLeases(ctx, []lease.IDVersion{
				{
					Name:    tableDesc.GetName(),
					ID:      tableDesc.GetID(),
					Version: tableVersion,
				},
			})
		}()
	}

	// Check version and maybe clear cache while holding the mutex.
	// We use a closure here so that we release the lock here, then keep
	// going and re-lock if adding the looked-up entry.
	userMapping, found := func() (userRoleMembership, bool) {
		roleMembersCache.Lock()
		defer roleMembersCache.Unlock()
		if roleMembersCache.tableVersion < tableVersion {
			// If the cache is based on an old table version, then update version and
			// drop the map.
			roleMembersCache.tableVersion = tableVersion
			roleMembersCache.userCache = make(map[username.SQLUsername]userRoleMembership)
			roleMembersCache.boundAccount.Empty(ctx)
		} else if roleMembersCache.tableVersion > tableVersion {
			// If the cache is based on a newer table version, then this transaction
			// should not use the cached data.
			return nil, false
		}
		userMapping, ok := roleMembersCache.userCache[member]
		return userMapping, ok
	}()

	if found {
		// Found: return.
		return userMapping, nil
	}

	// If `txn` is high priority and in a retry, do not launch the singleflight to
	// populate the cache because it can potentially cause a deadlock (see
	// TestConcurrentGrants/concurrent-GRANTs-high-priority for a repro) and
	// instead just issue a read using `txn` to `system.role_members` table and
	// return the result.
	if txn.KV().UserPriority() == roachpb.MaxUserPriority && txn.KV().Epoch() > 0 {
		return resolveMemberOfWithAdminOption(ctx, member, txn, useSingleQueryForRoleMembershipCache.Get(execCfg.SV()))
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
		fmt.Sprintf("%s-%d", member.Normalized(), tableVersion),
		singleflight.DoOpts{
			Stop:               roleMembersCache.stopper,
			InheritCancelation: false,
		},
		func(ctx context.Context) (interface{}, error) {
			var m map[username.SQLUsername]bool
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
				m, err = resolveMemberOfWithAdminOption(
					ctx, member, newTxn,
					useSingleQueryForRoleMembershipCache.Get(execCfg.SV()),
				)
				if err != nil {
					return err
				}
				return err
			})
			return m, err
		})
	var memberships map[username.SQLUsername]bool
	res := future.WaitForResult(ctx)
	if res.Err != nil {
		return nil, res.Err
	}
	memberships = res.Val.(map[username.SQLUsername]bool)

	func() {
		// Update membership if the table version hasn't changed.
		roleMembersCache.Lock()
		defer roleMembersCache.Unlock()
		if roleMembersCache.tableVersion != tableVersion {
			// Table version has changed while we were looking: don't cache the data.
			return
		}

		// Table version remains the same: update map, unlock, return.
		sizeOfEntry := int64(len(member.Normalized()))
		for m := range memberships {
			sizeOfEntry += int64(len(m.Normalized()))
			sizeOfEntry += memsize.Bool
		}
		if err := roleMembersCache.boundAccount.Grow(ctx, sizeOfEntry); err != nil {
			// If there is no memory available to cache the entry, we can still
			// proceed so that the query has a chance to succeed.
			log.Ops.Warningf(ctx, "no memory available to cache role membership info: %v", err)
		} else {
			roleMembersCache.userCache[member] = memberships
		}
	}()
	return memberships, nil
}

var defaultSingleQueryForRoleMembershipCache = util.ConstantWithMetamorphicTestBool(
	"resolve-membership-single-scan-enabled",
	true, /* defaultValue */
)

var useSingleQueryForRoleMembershipCache = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.auth.resolve_membership_single_scan.enabled",
	"determines whether to populate the role membership cache with a single scan",
	defaultSingleQueryForRoleMembershipCache,
	settings.WithPublic)

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
	ctx context.Context, member username.SQLUsername, txn isql.Txn, singleQuery bool,
) (map[username.SQLUsername]bool, error) {
	roleExists, err := RoleExists(ctx, txn, member)
	if err != nil {
		return nil, err
	} else if !roleExists {
		return nil, sqlerrors.NewUndefinedUserError(member)
	}
	ret := map[username.SQLUsername]bool{}
	if singleQuery {
		type membership struct {
			role    username.SQLUsername
			isAdmin bool
		}
		memberToRoles := make(map[username.SQLUsername][]membership)
		if err := forEachRoleMembership(ctx, txn, func(role, member username.SQLUsername, isAdmin bool) error {
			memberToRoles[member] = append(memberToRoles[member], membership{role, isAdmin})
			return nil
		}); err != nil {
			return nil, err
		}

		// Recurse through all roles associated with the member.
		var recurse func(u username.SQLUsername)
		recurse = func(u username.SQLUsername) {
			for _, membership := range memberToRoles[u] {
				// If the parent role was seen before, we still might need to update
				// the isAdmin flag for that role, but there's no need to recurse
				// through the role's ancestry again.
				prev, alreadySeen := ret[membership.role]
				ret[membership.role] = prev || membership.isAdmin
				if !alreadySeen {
					recurse(membership.role)
				}
			}
		}
		recurse(member)
		return ret, nil
	}

	// Keep track of members we looked up.
	visited := map[username.SQLUsername]struct{}{}
	toVisit := []username.SQLUsername{member}
	lookupRolesStmt := `SELECT "role", "isAdmin" FROM system.role_members WHERE "member" = $1`

	for len(toVisit) > 0 {
		// Pop first element.
		m := toVisit[0]
		toVisit = toVisit[1:]
		if _, ok := visited[m]; ok {
			continue
		}
		visited[m] = struct{}{}

		it, err := txn.QueryIteratorEx(
			ctx, "expand-roles", txn.KV(), sessiondata.InternalExecutorOverride{
				User: username.NodeUserName(),
			}, lookupRolesStmt, m.Normalized(),
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
			role := username.MakeSQLUsernameFromPreNormalizedString(string(roleName))
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

// HasRoleOption implements the AuthorizationAccessor interface.
func (p *planner) HasRoleOption(ctx context.Context, roleOption roleoption.Option) (bool, error) {
	// Verify that the txn is valid in any case, so that
	// we don't get the risk to say "OK" to root requests
	// with an invalid API usage.
	if p.txn == nil {
		return false, errors.AssertionFailedf("cannot use HasRoleOption without a txn")
	}

	user := p.SessionData().User()
	if user.IsRootUser() || user.IsNodeUser() {
		return true, nil
	}

	hasAdmin, err := p.HasAdminRole(ctx)
	if err != nil {
		return false, err
	}
	if hasAdmin {
		// Superusers have all role privileges.
		return true, nil
	}

	hasRolePrivilege, err := p.InternalSQLTxn().QueryRowEx(
		ctx, "has-role-option", p.Txn(),
		sessiondata.RootUserSessionDataOverride,
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
	if hasViewRedacted, err := p.HasViewActivityRedacted(ctx); err != nil {
		return false, false, err
	} else if hasViewRedacted {
		return true, true, nil
	}

	if hasView, err := p.HasViewActivity(ctx); err != nil {
		return false, false, err
	} else if hasView {
		return true, false, nil
	}

	return false, false, nil
}

func (p *planner) HasViewActivityRedacted(ctx context.Context) (bool, error) {
	return p.HasGlobalPrivilegeOrRoleOption(ctx, privilege.VIEWACTIVITYREDACTED)
}

func (p *planner) HasViewActivity(ctx context.Context) (bool, error) {
	return p.HasGlobalPrivilegeOrRoleOption(ctx, privilege.VIEWACTIVITY)
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

	return pgerror.Newf(pgcode.InsufficientPrivilege,
		"user %s does not have %s privilege on %s %s",
		user, kind, objTypeStr, object.GetName())
}

// IsInsufficientPrivilegeError returns true if the error is a pgerror
// with code pgcode.InsufficientPrivilege.
func IsInsufficientPrivilegeError(err error) bool {
	return pgerror.GetPGCode(err) == pgcode.InsufficientPrivilege
}
