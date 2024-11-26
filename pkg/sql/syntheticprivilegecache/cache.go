// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package syntheticprivilegecache

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/cacheutil"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Cache is used to cache synthetic privileges.
type Cache struct {
	settings       *cluster.Settings
	db             *kv.DB
	c              *cacheutil.Cache[string, catpb.PrivilegeDescriptor]
	virtualSchemas catalog.VirtualSchemas
	ief            descs.DB
	warmed         chan struct{}
	stopper        *stop.Stopper
}

// New constructs a new Cache.
func New(
	settings *cluster.Settings,
	stopper *stop.Stopper,
	db *kv.DB,
	account mon.BoundAccount,
	virtualSchemas catalog.VirtualSchemas,
	ief descs.DB,
) *Cache {
	return &Cache{
		settings:       settings,
		stopper:        stopper,
		db:             db,
		c:              cacheutil.NewCache[string, catpb.PrivilegeDescriptor](account, stopper, 1),
		virtualSchemas: virtualSchemas,
		ief:            ief,
		warmed:         make(chan struct{}),
	}
}

func (c *Cache) Get(
	ctx context.Context, txn isql.Txn, col *descs.Collection, spo syntheticprivilege.Object,
) (*catpb.PrivilegeDescriptor, error) {
	_, desc, err := descs.PrefixAndTable(ctx, col.ByNameWithLeased(txn.KV()).Get(), syntheticprivilege.SystemPrivilegesTableName)
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

	// Before we launch a goroutine to go fetch this descriptor, make sure that
	// the logic to fetch all descriptors at startup has completed.
	if err := c.waitForWarmed(ctx); err != nil {
		return nil, err
	}
	privDesc, err := c.c.LoadValueOutsideOfCacheSingleFlight(ctx, fmt.Sprintf("%s-%d", spo.GetPath(), desc.GetVersion()),
		func(loadCtx context.Context) (_ interface{}, retErr error) {
			privDesc, err := c.readFromStorage(loadCtx, txn, spo)
			if err != nil {
				return nil, err
			}
			entrySize := int64(len(spo.GetPath())) + computePrivDescSize(privDesc)
			// Only write back to the cache if the table version is committed.
			c.c.MaybeWriteBackToCache(ctx, []descpb.DescriptorVersion{desc.GetVersion()}, spo.GetPath(), *privDesc, entrySize)
			return privDesc, nil
		})
	if err != nil {
		return nil, err
	}
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
			return true, val, nil
		}
	}
	return false, catpb.PrivilegeDescriptor{}, nil
}

// synthesizePrivilegeDescriptorFromSystemPrivilegesTable reads from the
// system.privileges table to create the PrivilegeDescriptor from the
// corresponding privilege object. This is only used if the we cannot
// resolve the PrivilegeDescriptor from the cache.
func (c *Cache) readFromStorage(
	ctx context.Context, txn isql.Txn, spo syntheticprivilege.Object,
) (_ *catpb.PrivilegeDescriptor, retErr error) {

	query := fmt.Sprintf(
		`SELECT username, privileges, grant_options FROM system.%s WHERE path = $1`,
		catconstants.SystemPrivilegeTableName,
	)
	// TODO(ajwerner): Use an internal executor bound to the transaction.
	it, err := txn.QueryIteratorEx(
		ctx, `get-system-privileges`, txn.KV(), sessiondata.NodeUserSessionDataOverride, query, spo.GetPath(),
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

	// Admin always has ALL global privileges.
	if spo.GetObjectType() == privilege.Global {
		privDesc.Grant(username.AdminRoleName(), privilege.List{privilege.ALL}, true)
	}

	// We use InvalidID to skip checks on the root/admin roles having
	// privileges.
	validPrivs, err := privilege.GetValidPrivilegesForObject(spo.GetObjectType())
	if err != nil {
		return nil, err
	}
	if err := privDesc.Validate(
		descpb.InvalidID,
		spo.GetObjectType(),
		spo.GetPath(),
		validPrivs,
	); err != nil {
		return nil, err
	}
	return privDesc, nil
}

// Start starts the cache by pre-fetching the synthetic privileges.
func (c *Cache) Start(ctx context.Context) {
	if err := c.stopper.RunAsyncTask(ctx, "syntheticprivilegecache-warm", func(ctx context.Context) {
		defer close(c.warmed)
		start := timeutil.Now()
		if err := c.start(ctx); err != nil {
			log.Warningf(ctx, "failed to warm privileges for virtual tables: %v", err)
		} else {
			log.Infof(ctx, "warmed privileges for virtual tables in %v", timeutil.Since(start))
		}
	}); err != nil {
		close(c.warmed)
	}
}

func (c *Cache) start(ctx context.Context) error {
	var tableVersions []descpb.DescriptorVersion
	vtablePathToPrivilegeAccumulator := make(map[string]*accumulator)
	query := fmt.Sprintf(
		`SELECT path, username, privileges, grant_options FROM system.%s WHERE path LIKE $1`,
		catconstants.SystemPrivilegeTableName,
	)
	if err := c.ief.DescsTxn(ctx, func(
		ctx context.Context, txn descs.Txn,
	) (retErr error) {
		_, systemPrivDesc, err := descs.PrefixAndTable(ctx, txn.Descriptors().ByNameWithLeased(txn.KV()).Get(), syntheticprivilege.SystemPrivilegesTableName)
		if err != nil {
			return err
		}
		if systemPrivDesc.IsUncommittedVersion() {
			// This shouldn't ever happen, but if it does somehow, then we can't pre-warm the cache.
			logcrash.ReportOrPanic(
				ctx, &c.settings.SV,
				"cannot warm cache: %s is at an uncommitted version",
				syntheticprivilege.SystemPrivilegesTableName,
			)
			return errors.AssertionFailedf(
				"%s is at an uncommitted version",
				syntheticprivilege.SystemPrivilegesTableName,
			)
		}
		tableVersions = []descpb.DescriptorVersion{systemPrivDesc.GetVersion()}

		it, err := txn.QueryIteratorEx(
			ctx, `get-vtable-privileges`, txn.KV(), sessiondata.NodeUserSessionDataOverride,
			query, fmt.Sprintf("/%s/%%", syntheticprivilege.VirtualTablePathPrefix),
		)
		if err != nil {
			return err
		}
		defer func() {
			retErr = errors.CombineErrors(retErr, it.Close())
		}()

		for {
			ok, err := it.Next(ctx)
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			path := tree.MustBeDString(it.Cur()[0])
			user := tree.MustBeDString(it.Cur()[1])
			privArr := tree.MustBeDArray(it.Cur()[2])
			grantOptionArr := tree.MustBeDArray(it.Cur()[3])
			accum, ok := vtablePathToPrivilegeAccumulator[string(path)]
			if !ok {
				accum = newAccumulator(privilege.VirtualTable, string(path))
				vtablePathToPrivilegeAccumulator[string(path)] = accum
			}
			if err := accum.addRow(path, user, privArr, grantOptionArr); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	for scName := range catconstants.VirtualSchemaNames {
		sc, _ := c.virtualSchemas.GetVirtualSchema(scName)
		sc.VisitTables(func(object catalog.VirtualObject) {
			vtablePriv := syntheticprivilege.VirtualTablePrivilege{
				SchemaName: scName,
				TableName:  sc.Desc().GetName(),
			}
			privDesc := vtablePriv.GetFallbackPrivileges()
			if accum, ok := vtablePathToPrivilegeAccumulator[vtablePriv.GetPath()]; ok {
				privDesc = accum.finish()
			}
			entrySize := int64(len(vtablePriv.GetPath())) + computePrivDescSize(privDesc)
			c.c.MaybeWriteBackToCache(ctx, tableVersions, vtablePriv.GetPath(), *privDesc, entrySize)
		})
	}
	return nil
}

func (c *Cache) waitForWarmed(ctx context.Context) error {
	select {
	case <-c.warmed:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// computePrivDescSize computes the size in bytes required by the data in this
// descriptor.
func computePrivDescSize(privDesc *catpb.PrivilegeDescriptor) int64 {
	privDescSize := int(unsafe.Sizeof(*privDesc))
	privDescSize += len(privDesc.OwnerProto)
	for _, u := range privDesc.Users {
		privDescSize += int(unsafe.Sizeof(u))
		privDescSize += len(u.UserProto)
	}
	return int64(privDescSize)
}
