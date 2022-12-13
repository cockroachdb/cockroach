// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package syntheticprivilegecache

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/cacheutil"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

// Cache is used to cache synthetic privileges.
type Cache struct {
	settings       *cluster.Settings
	db             *kv.DB
	c              *cacheutil.Cache
	virtualSchemas catalog.VirtualSchemas
	ief            descs.TxnManager
	stopper        *stop.Stopper
}

// New constructs a new Cache.
func New(
	settings *cluster.Settings,
	stopper *stop.Stopper,
	db *kv.DB,
	account mon.BoundAccount,
	virtualSchemas catalog.VirtualSchemas,
	ief descs.TxnManager,
) *Cache {
	return &Cache{
		settings:       settings,
		stopper:        stopper,
		db:             db,
		c:              cacheutil.NewCache(account, stopper, 1),
		virtualSchemas: virtualSchemas,
		ief:            ief,
	}
}

func (c *Cache) Get(
	ctx context.Context, txn *kv.Txn, col *descs.Collection, spo syntheticprivilege.Object,
) (*catpb.PrivilegeDescriptor, error) {
	_, desc, err := col.GetImmutableTableByName(
		ctx,
		txn,
		syntheticprivilege.SystemPrivilegesTableName,
		tree.ObjectLookupFlagsWithRequired(),
	)
	if err != nil {
		return nil, err
	}
	if desc.IsUncommittedVersion() {
		return c.readFromStorage(ctx, txn, spo)
	}
	found, privileges, retErr := c.getFromCache(ctx, desc.GetVersion(), spo.GetPath())
	if found {
		return &privileges, retErr
	}
	val, err := c.c.LoadValueOutsideOfCacheSingleFlight(ctx, fmt.Sprintf("%s-%d", spo.GetPath(), desc.GetVersion()),
		func(loadCtx context.Context) (_ interface{}, retErr error) {
			return c.readFromStorage(ctx, txn, spo)
		})
	if err != nil {
		return nil, err
	}
	privDesc := val.(*catpb.PrivilegeDescriptor)
	// Only write back to the cache if the table version is
	// committed.
	c.c.MaybeWriteBackToCache(ctx, []descpb.DescriptorVersion{desc.GetVersion()}, spo.GetPath(), *privDesc)
	return privDesc, nil
}

func (c *Cache) getFromCache(
	ctx context.Context, version descpb.DescriptorVersion, path string,
) (bool, catpb.PrivilegeDescriptor, error) {
	c.c.Lock()
	defer c.c.Unlock()
	if isEligibleForCache := c.c.ClearCacheIfStaleLocked(
		ctx, []descpb.DescriptorVersion{version},
	); isEligibleForCache {
		val, ok := c.c.GetValueLocked(path)
		if ok {
			return true, val.(catpb.PrivilegeDescriptor), nil
		}
	}
	return false, catpb.PrivilegeDescriptor{}, nil
}

// synthesizePrivilegeDescriptorFromSystemPrivilegesTable reads from the
// system.privileges table to create the PrivilegeDescriptor from the
// corresponding privilege object. This is only used if the we cannot
// resolve the PrivilegeDescriptor from the cache.
func (c *Cache) readFromStorage(
	ctx context.Context, txn *kv.Txn, spo syntheticprivilege.Object,
) (_ *catpb.PrivilegeDescriptor, retErr error) {

	query := fmt.Sprintf(
		`SELECT username, privileges, grant_options FROM system.%s WHERE path = $1`,
		catconstants.SystemPrivilegeTableName,
	)
	// TODO(ajwerner): Use an internal executor bound to the transaction.
	ie := c.ief.MakeInternalExecutorWithoutTxn()
	it, err := ie.QueryIteratorEx(
		ctx, `get-system-privileges`, txn, sessiondata.NodeUserSessionDataOverride, query, spo.GetPath(),
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		retErr = errors.CombineErrors(retErr, it.Close())
	}()

	privAccumulator := newAccumulator(spo.GetObjectType(), spo.GetPath())
	for {
		ok, err := it.Next(ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}

		user := tree.MustBeDString(it.Cur()[0])
		privArr := tree.MustBeDArray(it.Cur()[1])
		grantOptionArr := tree.MustBeDArray(it.Cur()[2])
		if err := privAccumulator.addRow(tree.DString(spo.GetPath()), user, privArr, grantOptionArr); err != nil {
			return nil, err
		}
	}

	privDesc := privAccumulator.finish()
	// To avoid having to insert a row for public for each virtual
	// table into system.privileges, we assume that if there is
	// NO entry for public in the PrivilegeDescriptor, Public has
	// grant. If there is an empty row for Public, then public
	// does not have grant.
	if spo.GetObjectType() == privilege.VirtualTable {
		if _, found := privDesc.FindUser(username.PublicRoleName()); !found {
			privDesc.Grant(username.PublicRoleName(), privilege.List{privilege.SELECT}, false)
		}
	}

	// We use InvalidID to skip checks on the root/admin roles having
	// privileges.
	if err := privDesc.Validate(
		descpb.InvalidID,
		spo.GetObjectType(),
		spo.GetPath(),
		privilege.GetValidPrivilegesForObject(spo.GetObjectType()),
	); err != nil {
		return nil, err
	}
	return privDesc, err
}
