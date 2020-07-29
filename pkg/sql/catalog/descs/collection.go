// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package descs provides abstractions for dealing with sets of descriptors.
// It is utilized during schema changes and by catalog.Accessor implementations.
package descs

import (
	"context"
	"sort"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/database"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// UncommittedDatabase is a database that has been created/dropped
// within the current transaction using the Collection. A rename
// is a drop of the old name and creation of the new name.
type UncommittedDatabase struct {
	name    string
	id      sqlbase.ID
	dropped bool
}

// uncommittedDescriptor is a descriptor that has been modified in the current
// transaction.
type uncommittedDescriptor struct {
	mutable   catalog.MutableDescriptor
	immutable catalog.Descriptor
}

// leasedDescriptors holds references to all the descriptors leased in the
// transaction, and supports access by name and by ID.
type leasedDescriptors struct {
	descs []catalog.Descriptor
}

func (ld *leasedDescriptors) add(desc catalog.Descriptor) {
	ld.descs = append(ld.descs, desc)
}

func (ld *leasedDescriptors) releaseAll() (toRelease []catalog.Descriptor) {
	toRelease = append(toRelease, ld.descs...)
	ld.descs = ld.descs[:0]
	return toRelease
}

func (ld *leasedDescriptors) release(ids []sqlbase.ID) (toRelease []catalog.Descriptor) {
	// Sort the descriptors and leases to make it easy to find the leases to release.
	leasedDescs := ld.descs
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	sort.Slice(leasedDescs, func(i, j int) bool {
		return leasedDescs[i].GetID() < leasedDescs[j].GetID()
	})

	filteredLeases := leasedDescs[:0] // will store the remaining leases
	idsToConsider := ids
	shouldRelease := func(id sqlbase.ID) (found bool) {
		for len(idsToConsider) > 0 && idsToConsider[0] < id {
			idsToConsider = idsToConsider[1:]
		}
		return len(idsToConsider) > 0 && idsToConsider[0] == id
	}
	for _, l := range leasedDescs {
		if !shouldRelease(l.GetID()) {
			filteredLeases = append(filteredLeases, l)
		} else {
			toRelease = append(toRelease, l)
		}
	}
	ld.descs = filteredLeases
	return toRelease
}

func (ld *leasedDescriptors) getByID(id sqlbase.ID) catalog.Descriptor {
	for i := range ld.descs {
		desc := ld.descs[i]
		if desc.GetID() == id {
			return desc
		}
	}
	return nil
}

func (ld *leasedDescriptors) getByName(
	dbID sqlbase.ID, schemaID sqlbase.ID, name string,
) catalog.Descriptor {
	for i := range ld.descs {
		desc := ld.descs[i]
		if lease.NameMatchesDescriptor(desc, dbID, schemaID, name) {
			return desc
		}
	}
	return nil
}

func (ld *leasedDescriptors) numDescriptors() int {
	return len(ld.descs)
}

// MakeCollection constructs a Collection.
func MakeCollection(
	leaseMgr *lease.Manager,
	settings *cluster.Settings,
	dbCache *database.Cache,
	dbCacheSubscriber DatabaseCacheSubscriber,
) Collection {
	return Collection{
		leaseMgr:          leaseMgr,
		settings:          settings,
		databaseCache:     dbCache,
		dbCacheSubscriber: dbCacheSubscriber,
	}
}

// NewCollection constructs a new *Collection.
func NewCollection(leaseMgr *lease.Manager, settings *cluster.Settings) *Collection {
	tc := MakeCollection(leaseMgr, settings, nil, nil)
	return &tc
}

// Collection is a collection of descriptors held by a single session that
// serves SQL requests, or a background job using descriptors. The
// collection is cleared using ReleaseAll() which is called at the
// end of each transaction on the session, or on hitting conditions such
// as errors, or retries that result in transaction timestamp changes.
type Collection struct {
	// leaseMgr manages acquiring and releasing per-descriptor leases.
	leaseMgr *lease.Manager
	// A collection of descriptors valid for the timestamp. They are released once
	// the transaction using them is complete. If the transaction gets pushed and
	// the timestamp changes, the descriptors are released.
	// TODO (lucy): Use something other than an unsorted slice for faster lookups.
	leasedDescriptors leasedDescriptors
	// Descriptors modified by the uncommitted transaction affiliated with this
	// Collection. This allows a transaction to see its own modifications while
	// bypassing the descriptor lease mechanism. The lease mechanism will have its
	// own transaction to read the descriptor and will hang waiting for the
	// uncommitted changes to the descriptor. These descriptors are local to this
	// Collection and invisible to other transactions.
	uncommittedDescriptors []uncommittedDescriptor

	// databaseCache is used as a cache for database names.
	// This field is nil when the field is initialized for an internalPlanner.
	// TODO(andrei): get rid of it and replace it with a leasing system for
	// database descriptors.
	databaseCache *database.Cache

	// schemaCache maps {databaseID, schemaName} -> (schemaID, if exists, otherwise nil).
	// TODO(sqlexec): replace with leasing system with custom schemas.
	// This is currently never cleared, because there should only be unique schemas
	// being added for each Collection as only temporary schemas can be
	// made, and you cannot read from other schema caches.
	schemaCache sync.Map

	// DatabaseCacheSubscriber is used to block until the node's database cache has been
	// updated when ReleaseAll is called.
	dbCacheSubscriber DatabaseCacheSubscriber

	// Same as uncommittedDescriptors applying to databases modified within
	// an uncommitted transaction.
	uncommittedDatabases []UncommittedDatabase

	// allDescriptors is a slice of all available descriptors. The descriptors
	// are cached to avoid repeated lookups by users like virtual tables. The
	// cache is purged whenever events would cause a scan of all descriptors to
	// return different values, such as when the txn timestamp changes or when
	// new descriptors are written in the txn.
	//
	// TODO(ajwerner): This cache may be problematic in clusters with very large
	// numbers of descriptors.
	allDescriptors []sqlbase.DescriptorInterface

	// allDatabaseDescriptors is a slice of all available database descriptors.
	// These are purged at the same time as allDescriptors.
	allDatabaseDescriptors []*sqlbase.ImmutableDatabaseDescriptor

	// allSchemasForDatabase maps databaseID -> schemaID -> schemaName.
	// For each databaseID, all schemas visible under the database can be
	// observed.
	// These are purged at the same time as allDescriptors.
	allSchemasForDatabase map[sqlbase.ID]map[sqlbase.ID]string

	// settings are required to correctly resolve system.namespace accesses in
	// mixed version (19.2/20.1) clusters.
	// TODO(solon): This field could maybe be removed in 20.2.
	settings *cluster.Settings
}

// GetMutableTableDescriptor returns a mutable table descriptor.
//
// If flags.required is false, GetMutableTableDescriptor() will gracefully
// return a nil descriptor and no error if the table does not exist.
//
func (tc *Collection) GetMutableTableDescriptor(
	ctx context.Context, txn *kv.Txn, tn *tree.TableName, flags tree.ObjectLookupFlags,
) (*sqlbase.MutableTableDescriptor, error) {
	desc, err := tc.getMutableObjectDescriptor(ctx, txn, tn, flags)
	if err != nil {
		return nil, err
	}
	mutDesc, ok := desc.(*sqlbase.MutableTableDescriptor)
	if !ok {
		return nil, nil
	}
	hydrated, err := tc.hydrateTypesInTableDesc(ctx, txn, mutDesc)
	if err != nil {
		return nil, err
	}
	return hydrated.(*sqlbase.MutableTableDescriptor), nil
}

func (tc *Collection) getMutableObjectDescriptor(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, flags tree.ObjectLookupFlags,
) (catalog.MutableDescriptor, error) {
	if log.V(2) {
		log.Infof(ctx, "reading mutable descriptor on '%s'", name)
	}

	refuseFurtherLookup, dbID, err := tc.GetUncommittedDatabaseID(name.Catalog(), flags.Required)
	if refuseFurtherLookup || err != nil {
		return nil, err
	}

	if dbID == sqlbase.InvalidID && tc.DatabaseCache() != nil {
		// Resolve the database from the database cache when the transaction
		// hasn't modified the database.
		dbID, err = tc.DatabaseCache().GetDatabaseID(ctx, tc.leaseMgr.DB().Txn, name.Catalog(), flags.Required)
		if err != nil || dbID == sqlbase.InvalidID {
			// dbID can still be invalid if required is false and the database is not found.
			return nil, err
		}
	}

	// The following checks only work if the dbID is not invalid.
	if dbID != sqlbase.InvalidID {
		// Resolve the schema to the ID of the schema.
		foundSchema, resolvedSchema, err := tc.ResolveSchema(ctx, txn, dbID, name.Schema())
		if err != nil || !foundSchema {
			return nil, err
		}

		if refuseFurtherLookup, desc, err := tc.getUncommittedDescriptor(
			dbID,
			resolvedSchema.ID,
			name.Object(),
			flags.Required,
		); refuseFurtherLookup || err != nil {
			return nil, err
		} else if mut := desc.mutable; mut != nil {
			log.VEventf(ctx, 2, "found uncommitted descriptor %d", mut.GetID())
			return mut, nil
		}
	}

	phyAccessor := catalogkv.UncachedPhysicalAccessor{}
	obj, err := phyAccessor.GetObjectDesc(
		ctx,
		txn,
		tc.settings,
		tc.codec(),
		name.Catalog(),
		name.Schema(),
		name.Object(),
		flags,
	)
	if err != nil || obj == nil {
		return nil, err
	}
	mutDesc, ok := obj.(catalog.MutableDescriptor)
	if !ok {
		return nil, nil
	}
	return mutDesc, nil
}

// ResolveSchema attempts to lookup the schema from the schemaCache if it exists,
// otherwise falling back to a database lookup.
func (tc *Collection) ResolveSchema(
	ctx context.Context, txn *kv.Txn, dbID sqlbase.ID, schemaName string,
) (bool, sqlbase.ResolvedSchema, error) {
	// Fast path public schema, as it is always found.
	if schemaName == tree.PublicSchema {
		return true, sqlbase.ResolvedSchema{ID: keys.PublicSchemaID, Kind: sqlbase.SchemaPublic}, nil
	}

	type schemaCacheKey struct {
		dbID       sqlbase.ID
		schemaName string
	}

	key := schemaCacheKey{dbID: dbID, schemaName: schemaName}
	// First lookup the cache.
	// TODO (SQLSchema): This should look into the lease manager.
	if val, ok := tc.schemaCache.Load(key); ok {
		return true, val.(sqlbase.ResolvedSchema), nil
	}

	// Next, try lookup the result from KV, storing and returning the value.
	exists, resolved, err := (catalogkv.UncachedPhysicalAccessor{}).GetSchema(ctx, txn, tc.codec(), dbID, schemaName)
	if err != nil || !exists {
		return exists, sqlbase.ResolvedSchema{}, err
	}

	tc.schemaCache.Store(key, resolved)
	return exists, resolved, err
}

// GetTableVersion returns a table descriptor with a version suitable for
// the transaction: table.ModificationTime <= txn.Timestamp < expirationTime.
// The table must be released by calling tc.ReleaseAll().
//
// If flags.required is false, GetTableVersion() will gracefully
// return a nil descriptor and no error if the table does not exist.
//
// It might also add a transaction deadline to the transaction that is
// enforced at the KV layer to ensure that the transaction doesn't violate
// the validity window of the table descriptor version returned.
//
func (tc *Collection) GetTableVersion(
	ctx context.Context, txn *kv.Txn, tn *tree.TableName, flags tree.ObjectLookupFlags,
) (*sqlbase.ImmutableTableDescriptor, error) {
	desc, err := tc.getObjectVersion(ctx, txn, tn, flags)
	if err != nil {
		return nil, err
	}
	table, ok := desc.(*sqlbase.ImmutableTableDescriptor)
	if !ok {
		if flags.Required {
			return nil, sqlbase.NewUndefinedRelationError(tn)
		}
		return nil, nil
	}
	hydrated, err := tc.hydrateTypesInTableDesc(ctx, txn, table)
	if err != nil {
		return nil, err
	}
	return hydrated.(*sqlbase.ImmutableTableDescriptor), nil
}

func (tc *Collection) getObjectVersion(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, flags tree.ObjectLookupFlags,
) (catalog.Descriptor, error) {
	if log.V(2) {
		log.Infof(ctx, "planner acquiring lease on descriptor '%s'", name)
	}

	readObjectFromStore := func() (catalog.Descriptor, error) {
		phyAccessor := catalogkv.UncachedPhysicalAccessor{}
		return phyAccessor.GetObjectDesc(
			ctx,
			txn,
			tc.settings,
			tc.codec(),
			name.Catalog(),
			name.Schema(),
			name.Object(),
			flags,
		)
	}

	refuseFurtherLookup, dbID, err := tc.GetUncommittedDatabaseID(name.Catalog(), flags.Required)
	if refuseFurtherLookup || err != nil {
		return nil, err
	}

	if dbID == sqlbase.InvalidID && tc.DatabaseCache() != nil {
		// Resolve the database from the database cache when the transaction
		// hasn't modified the database.
		dbID, err = tc.DatabaseCache().GetDatabaseID(ctx, tc.leaseMgr.DB().Txn, name.Catalog(), flags.Required)
		if err != nil || dbID == sqlbase.InvalidID {
			// dbID can still be invalid if required is false and the database is not found.
			return nil, err
		}
	}

	// If at this point we have an InvalidID, we should immediately try read from store.
	if dbID == sqlbase.InvalidID {
		return readObjectFromStore()
	}

	// Resolve the schema to the ID of the schema.
	foundSchema, resolvedSchema, err := tc.ResolveSchema(ctx, txn, dbID, name.Schema())
	if err != nil || !foundSchema {
		return nil, err
	}
	schemaID := resolvedSchema.ID

	// TODO(vivek): Ideally we'd avoid caching for only the
	// system.descriptor and system.lease tables, because they are
	// used for acquiring leases, creating a chicken&egg problem.
	// But doing so turned problematic and the tests pass only by also
	// disabling caching of system.eventlog, system.rangelog, and
	// system.users. For now we're sticking to disabling caching of
	// all system descriptors except the role-members-desc.
	avoidCache := flags.AvoidCached || lease.TestingTableLeasesAreDisabled() ||
		(name.Catalog() == sqlbase.SystemDatabaseName && name.Object() != sqlbase.RoleMembersTable.Name)

	if refuseFurtherLookup, desc, err := tc.getUncommittedDescriptor(
		dbID,
		schemaID,
		name.Object(),
		flags.Required,
	); refuseFurtherLookup || err != nil {
		return nil, err
	} else if immut := desc.immutable; immut != nil {
		// If not forcing to resolve using KV, tables being added aren't visible.
		if immut.Adding() && !avoidCache {
			if !flags.Required {
				return nil, nil
			}
			return nil, catalog.FilterDescriptorState(immut)
		}

		log.VEventf(ctx, 2, "found uncommitted descriptor %d", immut.GetID())
		return immut, nil
	}

	if avoidCache {
		return readObjectFromStore()
	}

	// First, look to see if we already have the descriptor.
	// This ensures that, once a SQL transaction resolved name N to id X, it will
	// continue to use N to refer to X even if N is renamed during the
	// transaction.
	if desc := tc.leasedDescriptors.getByName(dbID, schemaID, name.Object()); desc != nil {
		log.VEventf(ctx, 2, "found descriptor in collection for '%s'", name)
		return desc, nil
	}

	readTimestamp := txn.ReadTimestamp()
	desc, expiration, err := tc.leaseMgr.AcquireByName(ctx, readTimestamp, dbID, schemaID, name.Object())
	if err != nil {
		// Read the descriptor from the store in the face of some specific errors
		// because of a known limitation of AcquireByName. See the known
		// limitations of AcquireByName for details.
		if catalog.HasInactiveDescriptorError(err) ||
			errors.Is(err, sqlbase.ErrDescriptorNotFound) {
			return readObjectFromStore()
		}
		// Lease acquisition failed with some other error. This we don't
		// know how to deal with, so propagate the error.
		return nil, err
	}

	if expiration.LessEq(readTimestamp) {
		log.Fatalf(ctx, "bad descriptor for T=%s, expiration=%s", readTimestamp, expiration)
	}

	tc.leasedDescriptors.add(desc)
	log.VEventf(ctx, 2, "added descriptor '%s' to collection", name)

	// If the descriptor we just acquired expires before the txn's deadline,
	// reduce the deadline. We use ReadTimestamp() that doesn't return the commit
	// timestamp, so we need to set a deadline on the transaction to prevent it
	// from committing beyond the version's expiration time.
	txn.UpdateDeadlineMaybe(ctx, expiration)
	return desc, nil
}

// GetTableVersionByID is a by-ID variant of GetTableVersion (i.e. uses same cache).
func (tc *Collection) GetTableVersionByID(
	ctx context.Context, txn *kv.Txn, tableID sqlbase.ID, flags tree.ObjectLookupFlags,
) (*sqlbase.ImmutableTableDescriptor, error) {
	desc, err := tc.getDescriptorVersionByID(ctx, txn, tableID, flags)
	if err != nil {
		if errors.Is(err, sqlbase.ErrDescriptorNotFound) {
			return nil, sqlbase.NewUndefinedRelationError(
				&tree.TableRef{TableID: int64(tableID)})
		}
		return nil, err
	}
	table, ok := desc.(*sqlbase.ImmutableTableDescriptor)
	if !ok {
		return nil, sqlbase.NewUndefinedRelationError(
			&tree.TableRef{TableID: int64(tableID)})
	}
	hydrated, err := tc.hydrateTypesInTableDesc(ctx, txn, table)
	if err != nil {
		return nil, err
	}
	return hydrated.(*sqlbase.ImmutableTableDescriptor), nil
}

func (tc *Collection) getDescriptorVersionByID(
	ctx context.Context, txn *kv.Txn, id sqlbase.ID, flags tree.ObjectLookupFlags,
) (catalog.Descriptor, error) {
	if flags.AvoidCached || lease.TestingTableLeasesAreDisabled() {
		desc, err := catalogkv.GetDescriptorByID(ctx, txn, tc.codec(), id)
		if err != nil {
			return nil, err
		}
		if desc == nil {
			return nil, sqlbase.ErrDescriptorNotFound
		}
		if err := catalog.FilterDescriptorState(desc); err != nil {
			return nil, err
		}
		return desc, nil
	}

	for _, ud := range tc.uncommittedDescriptors {
		if immut := ud.immutable; immut.GetID() == id {
			log.VEventf(ctx, 2, "found uncommitted descriptor %d", id)
			if immut.Dropped() {
				// TODO (lucy): This error is meant to be parallel to the error returned
				// from FilterDescriptorState, but it may be too low-level for getting
				// descriptors from the descriptor collection. In general the errors
				// being returned from this method aren't that consistent.
				return nil, catalog.NewInactiveDescriptorError(catalog.ErrDescriptorDropped)
			}
			return immut, nil
		}
	}

	// First, look to see if we already have the table in the shared cache.
	if desc := tc.leasedDescriptors.getByID(id); desc != nil {
		log.VEventf(ctx, 2, "found descriptor %d in cache", id)
		return desc, nil
	}

	readTimestamp := txn.ReadTimestamp()
	desc, expiration, err := tc.leaseMgr.Acquire(ctx, readTimestamp, id)
	if err != nil {
		return nil, err
	}

	if expiration.LessEq(readTimestamp) {
		log.Fatalf(ctx, "bad descriptor for T=%s, expiration=%s", readTimestamp, expiration)
	}

	tc.leasedDescriptors.add(desc)
	log.VEventf(ctx, 2, "added descriptor %q to collection", desc.GetName())

	// If the descriptor we just acquired expires before the txn's deadline,
	// reduce the deadline. We use ReadTimestamp() that doesn't return the commit
	// timestamp, so we need to set a deadline on the transaction to prevent it
	// from committing beyond the version's expiration time.
	txn.UpdateDeadlineMaybe(ctx, expiration)
	return desc, nil
}

// GetMutableTableVersionByID is a variant of sqlbase.GetTableDescFromID which returns a mutable
// table descriptor of the table modified in the same transaction.
func (tc *Collection) GetMutableTableVersionByID(
	ctx context.Context, tableID sqlbase.ID, txn *kv.Txn,
) (*sqlbase.MutableTableDescriptor, error) {
	desc, err := tc.getMutableDescriptorByID(ctx, tableID, txn)
	if err != nil {
		return nil, err
	}
	table := desc.(*sqlbase.MutableTableDescriptor)
	hydrated, err := tc.hydrateTypesInTableDesc(ctx, txn, table)
	if err != nil {
		return nil, err
	}
	return hydrated.(*sqlbase.MutableTableDescriptor), nil
}

func (tc *Collection) getMutableDescriptorByID(
	ctx context.Context, id sqlbase.ID, txn *kv.Txn,
) (catalog.MutableDescriptor, error) {
	log.VEventf(ctx, 2, "planner getting mutable descriptor for id %d", id)

	if desc := tc.getUncommittedDescriptorByID(id); desc != nil {
		log.VEventf(ctx, 2, "found uncommitted descriptor %d", id)
		return desc, nil
	}
	desc, err := catalogkv.GetMutableDescriptorByID(ctx, txn, tc.codec(), id)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		return nil, sqlbase.ErrDescriptorNotFound
	}
	return desc, nil
}

// hydrateTypesInTableDesc installs user defined type metadata in all types.T
// present in the input TableDescriptor. It always returns the same type of
// TableDescriptor that was passed in. It ensures that ImmutableTableDescriptors
// are not modified during the process of metadata installation.
func (tc *Collection) hydrateTypesInTableDesc(
	ctx context.Context, txn *kv.Txn, desc sqlbase.TableDescriptorInterface,
) (sqlbase.TableDescriptorInterface, error) {
	switch t := desc.(type) {
	case *sqlbase.MutableTableDescriptor:
		// It is safe to hydrate directly into MutableTableDescriptor since it is
		// not shared. When hydrating mutable descriptors, use the mutable access
		// method to access types.
		getType := func(ctx context.Context, id sqlbase.ID) (*tree.TypeName, sqlbase.TypeDescriptorInterface, error) {
			desc, err := tc.GetMutableTypeVersionByID(ctx, txn, id)
			if err != nil {
				return nil, nil, err
			}
			// TODO (lucy): This database access should go through the collection.
			dbDesc, err := sqlbase.GetDatabaseDescFromID(ctx, txn, tc.codec(), desc.ParentID)
			if err != nil {
				return nil, nil, err
			}
			name := tree.MakeNewQualifiedTypeName(dbDesc.Name, tree.PublicSchema, desc.Name)
			return &name, desc, nil
		}

		return desc, sqlbase.HydrateTypesInTableDescriptor(ctx, t.TableDesc(), sqlbase.TypeLookupFunc(getType))
	case *sqlbase.ImmutableTableDescriptor:
		// ImmutableTableDescriptors need to be copied before hydration, because
		// they are potentially read by multiple threads. If there aren't any user
		// defined types in the descriptor, then return early.
		if !t.ContainsUserDefinedTypes() {
			return desc, nil
		}

		// TODO (rohany, ajwerner): Here we would look into the cached set of
		//  hydrated table descriptors and potentially return without having to
		//  make a copy. However, we could avoid hitting the cache if any of the
		//  user defined types have been modified in this transaction.

		getType := func(ctx context.Context, id sqlbase.ID) (*tree.TypeName, sqlbase.TypeDescriptorInterface, error) {
			desc, err := tc.GetTypeVersionByID(ctx, txn, id, tree.ObjectLookupFlagsWithRequired())
			if err != nil {
				return nil, nil, err
			}
			// TODO (lucy): This database access should go through the collection.
			dbDesc, err := sqlbase.GetDatabaseDescFromID(ctx, txn, tc.codec(), desc.ParentID)
			if err != nil {
				return nil, nil, err
			}
			name := tree.MakeNewQualifiedTypeName(dbDesc.Name, tree.PublicSchema, desc.Name)
			return &name, desc, nil
		}

		// Make a copy of the underlying descriptor before hydration.
		descBase := protoutil.Clone(t.TableDesc()).(*sqlbase.TableDescriptor)
		if err := sqlbase.HydrateTypesInTableDescriptor(ctx, descBase, sqlbase.TypeLookupFunc(getType)); err != nil {
			return nil, err
		}
		return sqlbase.NewImmutableTableDescriptor(*descBase), nil
	default:
		return desc, nil
	}
}

// ReleaseSpecifiedLeases releases the leases for the descriptors with ids in
// the passed slice. Errors are logged but ignored.
func (tc *Collection) ReleaseSpecifiedLeases(ctx context.Context, descs []lease.IDVersion) {
	ids := make([]sqlbase.ID, len(descs))
	for i := range descs {
		ids[i] = descs[i].ID
	}
	toRelease := tc.leasedDescriptors.release(ids)
	for _, desc := range toRelease {
		if err := tc.leaseMgr.Release(desc); err != nil {
			log.Warningf(ctx, "%v", err)
		}
	}
}

// ReleaseLeases releases all leases. Errors are logged but ignored.
func (tc *Collection) ReleaseLeases(ctx context.Context) {
	log.VEventf(ctx, 2, "releasing %d descriptors", tc.leasedDescriptors.numDescriptors())
	toRelease := tc.leasedDescriptors.releaseAll()
	for _, desc := range toRelease {
		if err := tc.leaseMgr.Release(desc); err != nil {
			log.Warningf(ctx, "%v", err)
		}
	}
}

// ReleaseAll releases all state currently held by the Collection.
// ReleaseAll calls ReleaseLeases.
func (tc *Collection) ReleaseAll(ctx context.Context) {
	tc.ReleaseLeases(ctx)
	tc.uncommittedDescriptors = nil
	tc.uncommittedDatabases = nil
	tc.releaseAllDescriptors()
}

// WaitForCacheToDropDatabases waits until the database cache has been updated
// to properly reflect all dropped databases, so that future commands on the
// same gateway node observe the dropped databases.
func (tc *Collection) WaitForCacheToDropDatabases(ctx context.Context) {
	for _, uc := range tc.uncommittedDatabases {
		if !uc.dropped {
			continue
		}
		// Wait until the database cache has been updated to properly
		// reflect a dropped database, so that future commands on the
		// same gateway node observe the dropped database.
		tc.dbCacheSubscriber.WaitForCacheState(
			func(dc *database.Cache) bool {
				// Resolve the database name from the database cache.
				dbID, err := dc.GetCachedDatabaseID(uc.name)
				if err != nil || dbID == sqlbase.InvalidID {
					// dbID can still be 0 if required is false and
					// the database is not found. Swallowing error here
					// because it was felt there was no value in returning
					// it to a higher layer only to be swallow there. This
					// entire codepath is only called from one place so
					// it's better to swallow it here.
					return true
				}

				// If the database name still exists but it now references another
				// db with a more recent id, we're good - it means that the database
				// name has been reused.
				return dbID > uc.id
			})
	}
}

// HasUncommittedTables returns true if the Collection contains uncommitted
// tables.
func (tc *Collection) HasUncommittedTables() bool {
	for _, desc := range tc.uncommittedDescriptors {
		if desc.immutable.TableDesc() != nil {
			return true
		}
	}
	return false
}

// HasUncommittedTypes returns true if the Collection contains uncommitted
// types.
func (tc *Collection) HasUncommittedTypes() bool {
	for _, desc := range tc.uncommittedDescriptors {
		if desc.immutable.TypeDesc() != nil {
			return true
		}
	}
	return false
}

// Satisfy the linter.
var _ = (*Collection).HasUncommittedTypes

// AddUncommittedDescriptor adds an uncommitted descriptor modified in the
// transaction to the Collection.
func (tc *Collection) AddUncommittedDescriptor(desc catalog.MutableDescriptor) error {
	if desc.GetVersion() != desc.OriginalVersion()+1 {
		return errors.AssertionFailedf(
			"descriptor version %d not incremented from cluster version %d",
			desc.GetVersion(), desc.OriginalVersion())
	}
	tbl := uncommittedDescriptor{
		mutable:   desc,
		immutable: desc.Immutable(),
	}
	for i, d := range tc.uncommittedDescriptors {
		if d.mutable.GetID() == desc.GetID() {
			tc.uncommittedDescriptors[i] = tbl
			return nil
		}
	}
	tc.uncommittedDescriptors = append(tc.uncommittedDescriptors, tbl)
	tc.releaseAllDescriptors()
	return nil
}

// GetDescriptorsWithNewVersion returns all the IDVersion pairs that have
// undergone a schema change. Returns nil for no schema changes. The version
// returned for each schema change is ClusterVersion - 1, because that's the one
// that will be used when checking for table descriptor two version invariance.
func (tc *Collection) GetDescriptorsWithNewVersion() []lease.IDVersion {
	var descs []lease.IDVersion
	for _, desc := range tc.uncommittedDescriptors {
		if mut := desc.mutable; !mut.IsNew() {
			descs = append(descs, lease.NewIDVersionPrev(mut.OriginalName(), mut.OriginalID(), mut.OriginalVersion()))
		}
	}
	return descs
}

// GetUncommittedTables returns all the tables updated or created in the
// transaction.
func (tc *Collection) GetUncommittedTables() (tables []*sqlbase.ImmutableTableDescriptor) {
	for _, desc := range tc.uncommittedDescriptors {
		if desc.immutable.TableDesc() != nil {
			tables = append(tables, desc.immutable.(*sqlbase.ImmutableTableDescriptor))
		}
	}
	return tables
}

// User defined type accessors.

// GetMutableTypeDescriptor is the equivalent of GetMutableTableDescriptor but
// for accessing types.
func (tc *Collection) GetMutableTypeDescriptor(
	ctx context.Context, txn *kv.Txn, tn *tree.TypeName, flags tree.ObjectLookupFlags,
) (*sqlbase.MutableTypeDescriptor, error) {
	desc, err := tc.getMutableObjectDescriptor(ctx, txn, tn, flags)
	if err != nil {
		return nil, err
	}
	mutDesc, ok := desc.(*sqlbase.MutableTypeDescriptor)
	if !ok {
		if flags.Required {
			return nil, sqlbase.NewUndefinedTypeError(tn)
		}
		return nil, nil
	}
	return mutDesc, nil
}

// GetMutableTypeVersionByID is the equivalent of GetMutableTableDescriptorByID
// but for accessing types.
func (tc *Collection) GetMutableTypeVersionByID(
	ctx context.Context, txn *kv.Txn, typeID sqlbase.ID,
) (*sqlbase.MutableTypeDescriptor, error) {
	desc, err := tc.getMutableDescriptorByID(ctx, typeID, txn)
	if err != nil {
		return nil, err
	}
	return desc.(*sqlbase.MutableTypeDescriptor), nil
}

// GetTypeVersion is the equivalent of GetTableVersion but for accessing types.
func (tc *Collection) GetTypeVersion(
	ctx context.Context, txn *kv.Txn, tn *tree.TypeName, flags tree.ObjectLookupFlags,
) (*sqlbase.ImmutableTypeDescriptor, error) {
	desc, err := tc.getObjectVersion(ctx, txn, tn, flags)
	if err != nil {
		return nil, err
	}
	typ, ok := desc.(*sqlbase.ImmutableTypeDescriptor)
	if !ok {
		if flags.Required {
			return nil, sqlbase.NewUndefinedTypeError(tn)
		}
		return nil, nil
	}
	return typ, nil
}

// GetTypeVersionByID is the equivalent of GetTableVersionByID but for accessing
// types.
func (tc *Collection) GetTypeVersionByID(
	ctx context.Context, txn *kv.Txn, typeID sqlbase.ID, flags tree.ObjectLookupFlags,
) (*sqlbase.ImmutableTypeDescriptor, error) {
	desc, err := tc.getDescriptorVersionByID(ctx, txn, typeID, flags)
	if err != nil {
		if errors.Is(err, sqlbase.ErrDescriptorNotFound) {
			return nil, pgerror.Newf(
				pgcode.UndefinedObject, "type with ID %d does not exist", typeID)
		}
		return nil, err
	}
	typ, ok := desc.(*sqlbase.ImmutableTypeDescriptor)
	if !ok {
		return nil, pgerror.Newf(
			pgcode.UndefinedObject, "type with ID %d does not exist", typeID)
	}
	return typ, nil
}

// DBAction is an operation to an uncommitted database.
type DBAction bool

const (
	// DBCreated notes that the database has been created.
	DBCreated DBAction = false
	// DBDropped notes that the database has been dropped.
	DBDropped DBAction = true
)

// AddUncommittedDatabase stages the database action for the relevant database.
func (tc *Collection) AddUncommittedDatabase(name string, id sqlbase.ID, action DBAction) {
	db := UncommittedDatabase{name: name, id: id, dropped: action == DBDropped}
	tc.uncommittedDatabases = append(tc.uncommittedDatabases, db)
	tc.releaseAllDescriptors()
}

// GetUncommittedDatabaseID returns a database ID for the requested tablename
// if the requested tablename is for a database modified within the transaction
// affiliated with the LeaseCollection.
func (tc *Collection) GetUncommittedDatabaseID(
	requestedDbName string, required bool,
) (c bool, res sqlbase.ID, err error) {
	// Walk latest to earliest so that a DROP DATABASE followed by a
	// CREATE DATABASE with the same name will result in the CREATE DATABASE
	// being seen.
	for i := len(tc.uncommittedDatabases) - 1; i >= 0; i-- {
		db := tc.uncommittedDatabases[i]
		if requestedDbName == db.name {
			if db.dropped {
				if required {
					return true, sqlbase.InvalidID, sqlbase.NewUndefinedDatabaseError(requestedDbName)
				}
				return true, sqlbase.InvalidID, nil
			}
			return false, db.id, nil
		}
	}
	return false, sqlbase.InvalidID, nil
}

// getUncommittedDescriptor returns a descriptor for the requested name
// if the requested name is for a descriptor modified within the transaction
// affiliated with the LeaseCollection.
//
// The first return value "refuseFurtherLookup" is true when there is
// a known deletion of that descriptor, so it would be invalid to miss the
// cache and go to KV (where the descriptor prior to the DROP may
// still exist).
func (tc *Collection) getUncommittedDescriptor(
	dbID sqlbase.ID, schemaID sqlbase.ID, name string, required bool,
) (refuseFurtherLookup bool, desc uncommittedDescriptor, err error) {
	// Walk latest to earliest so that a DROP followed by a CREATE with the same
	// name will result in the CREATE being seen.
	for i := len(tc.uncommittedDescriptors) - 1; i >= 0; i-- {
		desc := tc.uncommittedDescriptors[i]
		mutDesc := desc.mutable
		// If a descriptor has gotten renamed we'd like to disallow using the old
		// names. The renames could have happened in another transaction but it's
		// still okay to disallow the use of the old name in this transaction
		// because the other transaction has already committed and this transaction
		// is seeing the effect of it.
		for _, drain := range mutDesc.GetDrainingNames() {
			if drain.Name == name &&
				drain.ParentID == dbID &&
				drain.ParentSchemaID == schemaID {
				// Name has gone away.
				if required {
					// If it's required here, say it doesn't exist.
					err = sqlbase.NewUndefinedRelationError(tree.NewUnqualifiedTableName(tree.Name(name)))
				}
				// The desc collection knows better; the caller has to avoid
				// going to KV in any case: refuseFurtherLookup = true
				return true, uncommittedDescriptor{}, err
			}
		}

		// Do we know about a descriptor with this name?
		if lease.NameMatchesDescriptor(mutDesc, dbID, schemaID, name) {
			// Right state?
			if err = catalog.FilterDescriptorState(mutDesc); err != nil && !catalog.HasAddingTableError(err) {
				if !required {
					// If it's not required here, we simply say we don't have it.
					err = nil
				}
				// The desc collection knows better; the caller has to avoid
				// going to KV in any case: refuseFurtherLookup = true
				return true, uncommittedDescriptor{}, err
			}

			// Got a descriptor.
			return false, desc, nil
		}
	}
	return false, uncommittedDescriptor{}, nil
}

// GetUncommittedTableByID returns an uncommitted table by its ID.
func (tc *Collection) GetUncommittedTableByID(id sqlbase.ID) *sqlbase.MutableTableDescriptor {
	desc := tc.getUncommittedDescriptorByID(id)
	if desc != nil {
		if table, ok := desc.(*sqlbase.MutableTableDescriptor); ok {
			return table
		}
	}
	return nil
}

func (tc *Collection) getUncommittedDescriptorByID(id sqlbase.ID) catalog.MutableDescriptor {
	for i := range tc.uncommittedDescriptors {
		desc := &tc.uncommittedDescriptors[i]
		if desc.mutable.GetID() == id {
			return desc.mutable
		}
	}
	return nil
}

// GetAllDescriptors returns all descriptors visible by the transaction,
// first checking the Collection's cached descriptors for validity
// before defaulting to a key-value scan, if necessary.
func (tc *Collection) GetAllDescriptors(
	ctx context.Context, txn *kv.Txn,
) ([]sqlbase.DescriptorInterface, error) {
	if tc.allDescriptors == nil {
		descs, err := catalogkv.GetAllDescriptors(ctx, txn, tc.codec())
		if err != nil {
			return nil, err
		}
		// There could be tables with user defined types that need hydrating,
		// so collect the needed information to set up metadata in those types.
		dbDescs := make(map[sqlbase.ID]*sqlbase.ImmutableDatabaseDescriptor)
		typDescs := make(map[sqlbase.ID]*sqlbase.ImmutableTypeDescriptor)
		for _, desc := range descs {
			switch desc := desc.(type) {
			case *sqlbase.ImmutableDatabaseDescriptor:
				dbDescs[desc.GetID()] = desc
			case *sqlbase.ImmutableTypeDescriptor:
				typDescs[desc.GetID()] = desc
			}
		}
		// If we found any type descriptors, that means that some of the tables we
		// scanned might have types that need hydrating.
		if len(typDescs) > 0 {
			// Since we just scanned all the descriptors, we already have everything
			// we need to hydrate our types. Set up an accessor for the type hydration
			// method to look into the scanned set of descriptors.
			typeLookup := func(ctx context.Context, id sqlbase.ID) (*tree.TypeName, sqlbase.TypeDescriptorInterface, error) {
				typDesc := typDescs[id]
				dbDesc := dbDescs[typDesc.ParentID]
				if dbDesc == nil {
					// TODO (rohany): Since DROP TYPE has not been implemented yet
					//  (see #48363), dropped databases do not yet clean up their
					//  orphaned child type descriptors. That could lead to dbDesc being
					//  nil here. Once we support drop type, this check does not need to
					//  be performed.
					return nil, nil, errors.Newf("database id %d not found", typDesc.ParentID)
				}
				schemaName, err := resolver.ResolveSchemaNameByID(ctx, txn, tc.codec(), dbDesc.GetID(), typDesc.ParentSchemaID)
				if err != nil {
					return nil, nil, err
				}
				name := tree.MakeNewQualifiedTypeName(dbDesc.GetName(), schemaName, typDesc.GetName())
				return &name, typDesc, nil
			}
			// Now hydrate all table descriptors.
			for i := range descs {
				desc := descs[i]
				if tblDesc, ok := desc.(*sqlbase.ImmutableTableDescriptor); ok {
					if err := sqlbase.HydrateTypesInTableDescriptor(
						ctx,
						tblDesc.TableDesc(),
						sqlbase.TypeLookupFunc(typeLookup),
					); err != nil {
						// If we ran into an error hydrating the types, that means that we
						// have some sort of corrupted descriptor state. Rather than disable
						// uses of GetAllDescriptors, just log the error.
						log.Errorf(ctx, "%s", err.Error())
					}
				}
			}
		}

		tc.allDescriptors = descs
	}
	return tc.allDescriptors, nil
}

// GetAllDatabaseDescriptors returns all database descriptors visible by the
// transaction, first checking the Collection's cached descriptors for
// validity before scanning system.namespace and looking up the descriptors
// in the database cache, if necessary.
func (tc *Collection) GetAllDatabaseDescriptors(
	ctx context.Context, txn *kv.Txn,
) ([]*sqlbase.ImmutableDatabaseDescriptor, error) {
	if tc.allDatabaseDescriptors == nil {
		dbDescIDs, err := catalogkv.GetAllDatabaseDescriptorIDs(ctx, txn, tc.codec())
		if err != nil {
			return nil, err
		}
		dbDescs, err := catalogkv.GetDatabaseDescriptorsFromIDs(ctx, txn, tc.codec(), dbDescIDs)
		if err != nil {
			return nil, err
		}
		tc.allDatabaseDescriptors = dbDescs
	}
	return tc.allDatabaseDescriptors, nil
}

// GetSchemasForDatabase returns the schemas for a given database
// visible by the transaction. This uses the schema cache locally
// if possible, or else performs a scan on kv.
func (tc *Collection) GetSchemasForDatabase(
	ctx context.Context, txn *kv.Txn, dbID sqlbase.ID,
) (map[sqlbase.ID]string, error) {
	if tc.allSchemasForDatabase == nil {
		tc.allSchemasForDatabase = make(map[sqlbase.ID]map[sqlbase.ID]string)
	}
	if _, ok := tc.allSchemasForDatabase[dbID]; !ok {
		var err error
		tc.allSchemasForDatabase[dbID], err = resolver.GetForDatabase(ctx, txn, tc.codec(), dbID)
		if err != nil {
			return nil, err
		}
	}
	return tc.allSchemasForDatabase[dbID], nil
}

// releaseAllDescriptors releases the cached slice of all descriptors
// held by Collection.
func (tc *Collection) releaseAllDescriptors() {
	tc.allDescriptors = nil
	tc.allDatabaseDescriptors = nil
	tc.allSchemasForDatabase = nil
}

// CopyModifiedObjects copies the modified schema to the table collection. Used
// when initializing an InternalExecutor.
func (tc *Collection) CopyModifiedObjects(to *Collection) {
	if tc == nil {
		return
	}
	to.uncommittedDescriptors = tc.uncommittedDescriptors
	to.uncommittedDatabases = tc.uncommittedDatabases
	// Do not copy the leased descriptors because we do not want
	// the leased descriptors to be released by the "to" Collection.
	// The "to" Collection can re-lease the same descriptors.
}

// ModifiedCollectionCopier is an interface used to copy modified schema elements
// to a new Collection.
type ModifiedCollectionCopier interface {
	CopyModifiedObjects(to *Collection)
}

func (tc *Collection) codec() keys.SQLCodec {
	return tc.leaseMgr.Codec()
}

// LeaseManager returns the lease.Manager.
func (tc *Collection) LeaseManager() *lease.Manager {
	return tc.leaseMgr
}

// DatabaseCache returns the database.Cache.
func (tc *Collection) DatabaseCache() *database.Cache {
	return tc.databaseCache
}

// ResetDatabaseCache resets the table collection's database.Cache.
func (tc *Collection) ResetDatabaseCache(dbCache *database.Cache) {
	tc.databaseCache = dbCache
}

// MigrationSchemaChangeRequiredContext flags a schema change as necessary to
// run even in a mixed-version 19.2/20.1 state where schema changes are normally
// banned, because the schema change is being run in a startup migration. It's
// the caller's responsibility to ensure that the schema change job is safe to
// run in a mixed-version state.
//
// TODO (lucy): Remove this in 20.2.
func MigrationSchemaChangeRequiredContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, migrationSchemaChangeRequiredHint{}, migrationSchemaChangeRequiredHint{})
}

// MigrationSchemaChangeRequiredFromContext returns true if the context
// indicates that a schema change should be run despite a mixed 19.2/20.1
// cluster version.
func MigrationSchemaChangeRequiredFromContext(ctx context.Context) bool {
	return ctx.Value(migrationSchemaChangeRequiredHint{}) == nil
}

type migrationSchemaChangeRequiredHint struct{}

// ErrSchemaChangeDisallowedInMixedState signifies that an attempted schema
// change was disallowed from running in a mixed-version
var ErrSchemaChangeDisallowedInMixedState = errors.New("schema change cannot be initiated in this version until the version upgrade is finalized")

// DatabaseCacheSubscriber allows the connExecutor to wait for a callback.
type DatabaseCacheSubscriber interface {
	// WaitForCacheState takes a callback depending on the cache state and blocks
	// until the callback declares success. The callback is repeatedly called as
	// the cache is updated.
	WaitForCacheState(cond func(*database.Cache) bool)
}
