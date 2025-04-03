// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rolemembershipcache

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/memsize"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
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
	readTS hlc.Timestamp
	// internalDB is used to run the queries to populate the cache.
	internalDB descs.DB
	stopper    *stop.Stopper
}

// userRoleMembership is a mapping of "rolename" -> "with admin option".
type userRoleMembership map[username.SQLUsername]bool

// NewMembershipCache initializes a new MembershipCache.
func NewMembershipCache(
	account mon.BoundAccount, internalDB descs.DB, stopper *stop.Stopper,
) *MembershipCache {
	return &MembershipCache{
		boundAccount:       account,
		populateCacheGroup: singleflight.NewGroup("lookup role membership", "key"),
		internalDB:         internalDB,
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

// GetRolesForMember looks up all the roles 'member' belongs to (direct and
// indirect) and returns a map of "role" -> "isAdmin".
// The "isAdmin" flag applies to both direct and indirect members.
// Requires a valid transaction to be open.
func (m *MembershipCache) GetRolesForMember(
	ctx context.Context, txn descs.Txn, member username.SQLUsername,
) (_ map[username.SQLUsername]bool, retErr error) {
	if txn == nil {
		return nil, errors.AssertionFailedf("cannot use MembershipCache without a txn")
	}

	// Lookup table versions.
	roleMembersTableDesc, err := txn.Descriptors().ByIDWithLeased(txn.KV()).Get().Table(ctx, keys.RoleMembersTableID)
	if err != nil {
		return nil, err
	}

	tableVersion := roleMembersTableDesc.GetVersion()
	if roleMembersTableDesc.IsUncommittedVersion() {
		return resolveRolesForMember(ctx, txn, member)
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
			return resolveRolesForMember(ctx, txn, member)
		}

		roleOptionsTableVersion := roleOptionsTableDesc.GetVersion()
		if roleOptionsTableDesc.IsUncommittedVersion() {
			return resolveRolesForMember(ctx, txn, member)
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
		m.Lock()
		defer m.Unlock()
		if m.tableVersion < tableVersion {
			// If the cache is based on an old table version, then update version and
			// drop the map.
			m.tableVersion = tableVersion
			m.userCache = make(map[username.SQLUsername]userRoleMembership)
			m.boundAccount.Empty(ctx)
			return nil, false, true
		} else if m.tableVersion > tableVersion {
			// If the cache is based on a newer table version, then this transaction
			// should not use the cached data.
			return nil, false, true
		}
		userMapping, ok := m.userCache[member]
		return userMapping, ok, len(m.userCache) == 0
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
		return resolveRolesForMember(ctx, txn, member)
	}

	// Lookup memberships outside the lock. There will be at most one request
	// in-flight for version of the table. The role_memberships table version is
	// also part of the request key so that we don't read data from an old version
	// of the table.
	//
	// The singleflight closure uses a fresh transaction to prevent a data race
	// that may occur if the context is cancelled, leading to the outer txn
	// being cleaned up. We set the timestamp of this new transaction to be
	// the same as the outer transaction that already read the descriptor, to
	// ensure that we are reading from the right version of the table.
	newTxnTimestamp := txn.KV().ReadTimestamp()
	future, _ := m.populateCacheGroup.DoChan(ctx,
		fmt.Sprintf("refreshMembershipCache-%d", tableVersion),
		singleflight.DoOpts{
			Stop:               m.stopper,
			InheritCancelation: false,
		},
		func(ctx context.Context) (interface{}, error) {
			var allMemberships map[username.SQLUsername]userRoleMembership
			err = m.internalDB.Txn(ctx, func(ctx context.Context, newTxn isql.Txn) error {
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
				m.Lock()
				defer m.Unlock()
				if m.tableVersion != tableVersion {
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
				if err := m.boundAccount.Grow(ctx, sizeOfCache); err != nil {
					// If there is no memory available to cache the entry, we can still
					// proceed so that the query has a chance to succeed.
					log.Ops.Warningf(ctx, "no memory available to cache role membership info: %v", err)
				} else {
					m.userCache = allMemberships
					m.readTS = newTxnTimestamp
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

// resolveRolesForMember performs the actual recursive role membership lookup.
func resolveRolesForMember(
	ctx context.Context, txn isql.Txn, member username.SQLUsername,
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
