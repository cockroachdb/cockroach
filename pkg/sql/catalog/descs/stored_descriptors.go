// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descs

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/catkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// storedDescriptorStatus is the status of a stored descriptor.
type storedDescriptorStatus int

const (
	// notValidatedYet designates descriptors which have been read from
	// storage but have not been validated yet. Until they're validated they
	// cannot be used for anything else than validating other descriptors.
	notValidatedYet storedDescriptorStatus = iota

	// notCheckedOutYet designates descriptors which have been properly read from
	// storage and have been validated but have never been checked out yet. This
	// means that the mutable and immutable descriptor protos are known to be the
	// exact same.
	notCheckedOutYet

	// checkedOutAtLeastOnce designates descriptors which have been checked out at
	// least once. Newly-created descriptors are considered to have been checked
	// out as well.
	checkedOutAtLeastOnce
)

// storedDescriptor is a descriptor that has been cached after being read
// and/or being modified in the current transaction.
type storedDescriptor struct {

	// immutable holds the descriptor as it was when this struct was initialized,
	// either after being read from storage or after being checked in.
	immutable catalog.Descriptor

	// mutable is initialized as a mutable copy of immutable when the descriptor
	// is read from storage.
	// This value might be nil in some rare cases where we completely bypass
	// storage for performance reasons because the descriptor is guaranteed to
	// never change. Such is the case of the system database descriptor for
	// instance.
	// This value should not make its way outside the storedDescriptors
	// other than via checkOut.
	mutable catalog.MutableDescriptor

	// storedDescriptorStatus describes the status of the mutable and
	// immutable descriptors
	storedDescriptorStatus
}

// GetName implements the catalog.NameEntry interface.
func (u *storedDescriptor) GetName() string {
	return u.immutable.GetName()
}

// GetParentID implements the catalog.NameEntry interface.
func (u *storedDescriptor) GetParentID() descpb.ID {
	return u.immutable.GetParentID()
}

// GetParentSchemaID implements the catalog.NameEntry interface.
func (u storedDescriptor) GetParentSchemaID() descpb.ID {
	return u.immutable.GetParentSchemaID()
}

// GetID implements the catalog.NameEntry interface.
func (u storedDescriptor) GetID() descpb.ID {
	return u.immutable.GetID()
}

// checkOut is how the mutable descriptor should be accessed.
func (u *storedDescriptor) checkOut() catalog.MutableDescriptor {
	if u.mutable == nil {
		// This special case is allowed for certain system descriptors which
		// for performance reasons are never actually read from storage, instead
		// we use a copy of the descriptor hard-coded in the system schema used for
		// bootstrapping a cluster.
		//
		// This implies that these descriptors never undergo any changes and
		// therefore checking out a mutable descriptor is pointless for the most
		// part. This may nonetheless legitimately happen during migrations
		// which change all descriptors somehow, so we need to support this.
		return u.immutable.NewBuilder().BuildExistingMutable()
	}
	u.storedDescriptorStatus = checkedOutAtLeastOnce
	return u.mutable
}

var _ catalog.NameEntry = (*storedDescriptor)(nil)

// storedDescriptors is the data structure holding all
// storedDescriptor objects for a Collection.
//
// Immutable descriptors can be freely looked up.
// Mutable descriptors can be:
// 1. added into it,
// 2. checked out of it,
// 3. checked back in to it.
//
// An error will be triggered by:
// - checking out a mutable descriptor that hasn't yet been added,
// - checking in a descriptor that has been added but not yet checked out,
// - any checked-out-but-not-checked-in mutable descriptors at commit time.
//
type storedDescriptors struct {
	codec keys.SQLCodec

	// A mirror of the descriptors in storage. These descriptors are either (1)
	// already stored and were read from KV, or (2) have been modified by the
	// uncommitted transaction affiliated with this Collection and should be
	// written to KV upon commit.
	// Source (1) serves as a cache. Source (2) allows a transaction to see its
	// own modifications while bypassing the descriptor lease mechanism. The
	// lease mechanism will have its own transaction to read the descriptor and
	// will hang waiting for the uncommitted changes to the descriptor if this
	// transaction is PRIORITY HIGH. These descriptors are local to this
	// Collection and their state is thus not visible to other transactions.
	descs nstree.Map

	// addedSystemDatabase is used to mark whether the optimization to add the
	// system database to the set of stored descriptors has occurred.
	addedSystemDatabase bool

	// systemNamespace is a cache of system table namespace entries. We assume
	// these are immutable for the life of the process.
	systemNamespace *systemDatabaseNamespaceCache

	// hasAll* indicates previously completed range storage lookups. When set, we
	// know these descriptors are cached in the map.
	hasAllDescriptors         bool
	hasAllDatabaseDescriptors bool

	// allSchemasForDatabase maps databaseID -> schemaID -> schemaName.
	// For each databaseID, all schemas visible under the database can be
	// observed.
	// These are read from store, which means they may not be up to date if
	// modifications have been made. The freshest schemas should be in the map
	// above.
	allSchemasForDatabase map[descpb.ID]map[descpb.ID]string

	// memAcc is the actual account of an injected, upstream monitor
	// to track memory usage of storedDescriptors.
	memAcc mon.BoundAccount
}

func makeStoredDescriptors(
	codec keys.SQLCodec, systemNamespace *systemDatabaseNamespaceCache, monitor *mon.BytesMonitor,
) storedDescriptors {
	return storedDescriptors{
		codec:           codec,
		systemNamespace: systemNamespace,
		memAcc:          monitor.MakeBoundAccount(),
	}
}

func (sd *storedDescriptors) reset(ctx context.Context) {
	sd.descs.Clear()
	sd.addedSystemDatabase = false
	sd.hasAllDescriptors = false
	sd.hasAllDatabaseDescriptors = false
	sd.allSchemasForDatabase = nil
	sd.memAcc.Clear(ctx)
}

// add adds a descriptor to the set of stored descriptors and returns
// an immutable copy of that descriptor.
func (sd *storedDescriptors) add(
	ctx context.Context, mut catalog.MutableDescriptor, status storedDescriptorStatus,
) (catalog.Descriptor, error) {
	uNew, err := makeStoredDescriptor(mut, status)
	if err != nil {
		return nil, err
	}
	if prev, ok := sd.descs.GetByID(mut.GetID()).(*storedDescriptor); ok {
		if prev.mutable.OriginalVersion() != mut.OriginalVersion() {
			return nil, errors.AssertionFailedf(
				"cannot add a version of descriptor with a different original version" +
					" than it was previously added with")
		}
	} else {
		if err := sd.memAcc.Grow(ctx, mut.ByteSize()); err != nil {
			err = errors.Wrap(err, "Memory usage exceeds limit for storedDescriptors.descs")
			return nil, err
		}
	}
	sd.descs.Upsert(uNew)
	return uNew.immutable, err
}

// checkOut checks out a stored mutable descriptor for use in the
// transaction. This descriptor should later be checked in again.
func (sd *storedDescriptors) checkOut(id descpb.ID) (_ catalog.MutableDescriptor, err error) {
	defer func() {
		err = errors.NewAssertionErrorWithWrappedErrf(
			err, "cannot check out stored descriptor with ID %d", id,
		)
	}()
	if id == keys.SystemDatabaseID {
		sd.maybeAddSystemDatabase()
	}
	entry := sd.descs.GetByID(id)
	if entry == nil {
		return nil, errors.New("descriptor hasn't been added yet")
	}
	u := entry.(*storedDescriptor)
	if u.storedDescriptorStatus == notValidatedYet {
		return nil, errors.New("descriptor hasn't been validated yet")
	}
	return u.checkOut(), nil
}

// checkIn checks in a stored mutable descriptor that was previously
// checked out.
func (sd *storedDescriptors) checkIn(mut catalog.MutableDescriptor) error {
	uNew, err := makeStoredDescriptor(mut, checkedOutAtLeastOnce)
	if err != nil {
		return err
	}
	if prev := sd.descs.GetByID(mut.GetID()); prev != nil {
		return errors.AssertionFailedf("cannot check in a descriptor that has not been previously checked out")
	}
	sd.descs.Upsert(uNew)
	return err
}

// upgradeToValidated upgrades a stored descriptor that was previously
// unvalidated to not checked out yet.
func (sd *storedDescriptors) upgradeToValidated(id descpb.ID) (err error) {
	defer func() {
		err = errors.NewAssertionErrorWithWrappedErrf(
			err, "cannot upgrade stored descriptor with ID %d", id,
		)
	}()
	entry := sd.descs.GetByID(id)
	if entry == nil {
		return errors.New("descriptor has not been cached")
	}
	storedDesc := entry.(*storedDescriptor)
	if storedDesc.storedDescriptorStatus == checkedOutAtLeastOnce {
		return errors.New("descriptor has already been checked out")
	}
	storedDesc.storedDescriptorStatus = notCheckedOutYet
	return nil
}

func makeStoredDescriptor(
	desc catalog.MutableDescriptor, status storedDescriptorStatus,
) (*storedDescriptor, error) {
	version := desc.GetVersion()
	origVersion := desc.OriginalVersion()
	if version != origVersion && version != origVersion+1 {
		return nil, errors.AssertionFailedf(
			"descriptor %d version %d not compatible with cluster version %d",
			desc.GetID(), version, origVersion)
	}

	mutable, err := maybeRefreshCachedFieldsOnTypeDescriptor(desc)
	if err != nil {
		return nil, err
	}

	return &storedDescriptor{
		mutable:                mutable,
		immutable:              mutable.ImmutableCopy(),
		storedDescriptorStatus: status,
	}, nil
}

// maybeRefreshCachedFieldsOnTypeDescriptor refreshes the cached fields on a
// Mutable if the given descriptor is a type descriptor and works as a pass
// through for all other descriptors. Mutable type descriptors are refreshed to
// reconstruct enumMetadata. This ensures that tables hydration following a
// type descriptor update (in the same txn) happens using the modified fields.
func maybeRefreshCachedFieldsOnTypeDescriptor(
	desc catalog.MutableDescriptor,
) (catalog.MutableDescriptor, error) {
	typeDesc, ok := desc.(catalog.TypeDescriptor)
	if ok {
		return typedesc.UpdateCachedFieldsOnModifiedMutable(typeDesc)
	}
	return desc, nil
}

// getCachedByID looks up a cached descriptor by ID.
func (sd *storedDescriptors) getCachedByID(
	id descpb.ID,
) (catalog.Descriptor, storedDescriptorStatus) {
	if id == keys.SystemDatabaseID {
		sd.maybeAddSystemDatabase()
	}
	entry := sd.descs.GetByID(id)
	if entry == nil {
		return nil, notValidatedYet
	}
	u := entry.(*storedDescriptor)
	return u.immutable, u.storedDescriptorStatus
}

// getCachedByName looks up a cached descriptor by name.
func (sd *storedDescriptors) getCachedByName(
	dbID descpb.ID, schemaID descpb.ID, name string,
) catalog.Descriptor {
	if dbID == 0 && schemaID == 0 && name == systemschema.SystemDatabaseName {
		sd.maybeAddSystemDatabase()
	}
	// Walk latest to earliest so that a DROP followed by a CREATE with the same
	// name will result in the CREATE being seen.
	if got := sd.descs.GetByName(dbID, schemaID, name); got != nil {
		u := got.(*storedDescriptor)
		if u.storedDescriptorStatus == notValidatedYet {
			return nil
		}
		return u.immutable
	}
	return nil
}

// getUnvalidatedByName looks up an unvalidated descriptor by name.
func (sd *storedDescriptors) getUnvalidatedByName(
	dbID descpb.ID, schemaID descpb.ID, name string,
) catalog.Descriptor {
	if dbID == 0 && schemaID == 0 && name == systemschema.SystemDatabaseName {
		sd.maybeAddSystemDatabase()
	}
	entry := sd.descs.GetByName(dbID, schemaID, name)
	if entry == nil {
		return nil
	}
	u := entry.(*storedDescriptor)
	if u.storedDescriptorStatus != notValidatedYet {
		return nil
	}
	return u.immutable
}

// getAllDescriptors looks up all descriptors. The first call must go to KV.
// Subsequent calls can retrieve descriptors from the cache.
func (sd *storedDescriptors) getAllDescriptors(
	ctx context.Context, txn *kv.Txn, version clusterversion.ClusterVersion,
) (nstree.Catalog, error) {
	if !sd.hasAllDescriptors {
		c, err := catkv.GetCatalogUnvalidated(ctx, sd.codec, txn)
		if err != nil {
			return nstree.Catalog{}, err
		}
		descs := c.OrderedDescriptors()
		ve := c.Validate(ctx, version, catalog.ValidationReadTelemetry, catalog.ValidationLevelCrossReferences, descs...)
		if err := ve.CombinedError(); err != nil {
			return nstree.Catalog{}, err
		}
		if err := c.ForEachDescriptorEntry(func(desc catalog.Descriptor) error {
			if sd.descs.GetByID(desc.GetID()) == nil {
				// Cache this descriptor.
				mut := desc.NewBuilder().BuildExistingMutable()
				_, err := sd.add(ctx, mut, notCheckedOutYet)
				if err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return nstree.Catalog{}, err
		}
		sd.hasAllDescriptors = true
		sd.hasAllDatabaseDescriptors = true
	}
	// TODO(jchan): There's a lot of wasted effort here in reconstructing a
	// catalog and hydrating every descriptor. We could do better by caching the
	// catalog and invalidating it upon descriptor check-out.
	cat := nstree.MutableCatalog{}
	if err := sd.descs.IterateByID(func(entry catalog.NameEntry) error {
		var desc catalog.Descriptor
		sDesc := entry.(*storedDescriptor)
		if sDesc.storedDescriptorStatus == checkedOutAtLeastOnce {
			desc = sDesc.mutable.ImmutableCopy()
		} else {
			desc = sDesc.immutable
		}
		cat.UpsertDescriptorEntry(desc)
		return nil
	}); err != nil {
		return nstree.Catalog{}, err
	}
	// There could be tables with user defined types that need hydrating.
	if err := hydrateCatalog(ctx, cat.Catalog); err != nil {
		// If we ran into an error hydrating the types, that means that we
		// have some sort of corrupted descriptor state. Rather than disable
		// uses of getAllDescriptors, just log the error.
		log.Errorf(ctx, "%s", err.Error())
	}
	return cat.Catalog, nil
}

// getAllDatabaseDescriptors looks up all database descriptors. The first call
// must go to KV, except if getAllDescriptors has been called. Subsequent calls
// can retrieve descriptors from the cache.
func (sd *storedDescriptors) getAllDatabaseDescriptors(
	ctx context.Context,
	version clusterversion.ClusterVersion,
	txn *kv.Txn,
	vd validate.ValidationDereferencer,
) ([]catalog.DatabaseDescriptor, error) {
	if !sd.hasAllDescriptors && !sd.hasAllDatabaseDescriptors {
		c, err := catkv.GetAllDatabaseDescriptorIDs(ctx, txn, sd.codec)
		if err != nil {
			return nil, err
		}
		dbDescs, err := catkv.MustGetDescriptorsByID(ctx, version, sd.codec, txn, vd, c.OrderedDescriptorIDs(), catalog.Database)
		if err != nil {
			return nil, err
		}
		for _, desc := range dbDescs {
			if sd.descs.GetByID(desc.GetID()) == nil {
				mut := desc.NewBuilder().BuildExistingMutable()
				_, err := sd.add(ctx, mut, notCheckedOutYet)
				if err != nil {
					return nil, err
				}
			}
		}
		sd.hasAllDatabaseDescriptors = true
	}
	var allDatabaseDescriptors []catalog.DatabaseDescriptor
	if err := sd.descs.IterateDatabasesByName(func(entry catalog.NameEntry) error {
		sDesc := entry.(*storedDescriptor)
		var dbDesc catalog.DatabaseDescriptor
		if sDesc.storedDescriptorStatus == checkedOutAtLeastOnce {
			dbDesc = sDesc.mutable.ImmutableCopy().(catalog.DatabaseDescriptor)
		} else {
			dbDesc = sDesc.immutable.(catalog.DatabaseDescriptor)
		}
		allDatabaseDescriptors = append(allDatabaseDescriptors, dbDesc)
		return nil
	}); err != nil {
		return nil, err
	}
	return allDatabaseDescriptors, nil
}

func (sd *storedDescriptors) getSchemasForDatabase(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor,
) (map[descpb.ID]string, error) {
	if sd.allSchemasForDatabase == nil {
		sd.allSchemasForDatabase = make(map[descpb.ID]map[descpb.ID]string)
	}
	if _, ok := sd.allSchemasForDatabase[db.GetID()]; !ok {
		var err error
		allSchemas, err := resolver.GetForDatabase(ctx, txn, sd.codec, db)
		if err != nil {
			return nil, err
		}
		sd.allSchemasForDatabase[db.GetID()] = make(map[descpb.ID]string)
		for id, entry := range allSchemas {
			sd.allSchemasForDatabase[db.GetID()][id] = entry.Name
		}
		return sd.allSchemasForDatabase[db.GetID()], nil
	}
	schemasForDatabase := sd.allSchemasForDatabase[db.GetID()]
	if err := sd.descs.IterateSchemasForDatabaseByName(db.GetID(), func(entry catalog.NameEntry) error {
		sDesc := entry.(*storedDescriptor)
		var schemaDesc catalog.Descriptor
		if sDesc.storedDescriptorStatus == checkedOutAtLeastOnce {
			schemaDesc = sDesc.mutable.ImmutableCopy()
		} else {
			schemaDesc = sDesc.immutable
		}
		schemasForDatabase[schemaDesc.GetID()] = schemaDesc.GetName()
		return nil
	}); err != nil {
		return nil, err
	}
	return schemasForDatabase, nil
}

func (sd *storedDescriptors) idDefinitelyDoesNotExist(id descpb.ID) bool {
	if !sd.hasAllDescriptors {
		return false
	}
	return sd.descs.GetByID(id) == nil
}

// lookupName is used when reading a descriptor from the storage layer by name.
// Descriptors are physically keyed by ID, so we need to resolve their ID by
// querying the system.namespace table first, which is what this method does.
// We can avoid having to do this in some special cases:
// - When the descriptor name and ID are hard-coded. This is the case for the
//   system database and for the tables in it.
// - When we're looking up a schema for which we already have the descriptor
//   of the parent database. The schema ID can be looked up in it.
//
func (sd *storedDescriptors) lookupName(
	ctx context.Context,
	txn *kv.Txn,
	maybeDB catalog.DatabaseDescriptor,
	parentID descpb.ID,
	parentSchemaID descpb.ID,
	name string,
) (id descpb.ID, err error) {
	// Handle special cases which might avoid a namespace table query.
	switch parentID {
	case descpb.InvalidID:
		if name == systemschema.SystemDatabaseName {
			// Special case: looking up the system database.
			// The system database's descriptor ID is hard-coded.
			return keys.SystemDatabaseID, nil
		}
	case keys.SystemDatabaseID:
		// Special case: looking up something in the system database.
		// Those namespace table entries are cached.
		id = sd.systemNamespace.lookup(parentSchemaID, name)
		if id != descpb.InvalidID {
			return id, err
		}
		// Make sure to cache the result if we had to look it up.
		defer func() {
			if err == nil && id != descpb.InvalidID {
				sd.systemNamespace.add(descpb.NameInfo{
					ParentID:       keys.SystemDatabaseID,
					ParentSchemaID: parentSchemaID,
					Name:           name,
				}, id)
			}
		}()
	default:
		if parentSchemaID == descpb.InvalidID {
			// At this point we know that parentID is not zero, so a zero
			// parentSchemaID means we're looking up a schema.
			if maybeDB != nil {
				// Special case: looking up a schema, but in a database which we already
				// have the descriptor for. We find the schema ID in there.
				id := maybeDB.GetSchemaID(name)
				return id, nil
			}
		}
	}
	// Fall back to querying the namespace table.
	return catkv.LookupID(
		ctx, txn, sd.codec, parentID, parentSchemaID, name,
	)
}

// getByName reads a descriptor from the storage layer by name.
//
// This is a three-step process:
// 1. resolve the descriptor's ID using the name information,
// 2. actually read the descriptor from storage,
// 3. check that the name in the descriptor is the one we expect; meaning that
//    there is no RENAME underway for instance.
//
func (sd *storedDescriptors) getByName(
	ctx context.Context,
	version clusterversion.ClusterVersion,
	txn *kv.Txn,
	vd validate.ValidationDereferencer,
	maybeDB catalog.DatabaseDescriptor,
	parentID descpb.ID,
	parentSchemaID descpb.ID,
	name string,
) (catalog.Descriptor, error) {
	descID, err := sd.lookupName(ctx, txn, maybeDB, parentID, parentSchemaID, name)
	if err != nil || descID == descpb.InvalidID {
		return nil, err
	}
	descs, err := sd.getByIDs(ctx, version, txn, vd, []descpb.ID{descID})
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			// Having done the namespace lookup, the descriptor must exist.
			return nil, errors.WithAssertionFailure(err)
		}
		return nil, err
	}
	if descs[0].GetName() != name {
		// Immediately after a RENAME an old name still points to the descriptor
		// during the drain phase for the name. Do not return a descriptor during
		// draining.
		//
		// TODO(postamar): remove this after 22.1 is release.
		// At that point, draining names will no longer have to be supported.
		// We can then consider making the descriptor collection aware of
		// uncommitted namespace operations.
		return nil, nil
	}
	return descs[0], nil
}

// getByIDs actually reads a batch of descriptors from the storage layer.
func (sd *storedDescriptors) getByIDs(
	ctx context.Context,
	version clusterversion.ClusterVersion,
	txn *kv.Txn,
	vd validate.ValidationDereferencer,
	ids []descpb.ID,
) ([]catalog.Descriptor, error) {
	ret := make([]catalog.Descriptor, len(ids))
	kvIDs := make([]descpb.ID, 0, len(ids))
	indexes := make([]int, 0, len(ids))
	for i, id := range ids {
		if id == keys.SystemDatabaseID {
			// Special handling for the system database descriptor.
			//
			// This is done for performance reasons, to save ourselves an unnecessary
			// round trip to storage which otherwise quickly compounds.
			//
			// The system database descriptor should never actually be mutated, which is
			// why we return the same hard-coded descriptor every time. It's assumed
			// that callers of this method will check the privileges on the descriptor
			// (like any other database) and return an error.
			ret[i] = dbdesc.NewBuilder(systemschema.SystemDB.DatabaseDesc()).BuildExistingMutable()
		} else {
			kvIDs = append(kvIDs, id)
			indexes = append(indexes, i)
		}
	}
	if len(kvIDs) == 0 {
		return ret, nil
	}
	kvDescs, err := catkv.MustGetDescriptorsByID(ctx, version, sd.codec, txn, vd, kvIDs, catalog.Any)
	if err != nil {
		return nil, err
	}
	for j, desc := range kvDescs {
		ret[indexes[j]] = desc
	}
	return ret, nil
}

func (sd *storedDescriptors) iterateNewVersionByID(
	fn func(originalVersion lease.IDVersion) error,
) error {
	return sd.descs.IterateByID(func(entry catalog.NameEntry) error {
		u := entry.(*storedDescriptor)
		if u.storedDescriptorStatus == notValidatedYet {
			return nil
		}
		mut := u.mutable
		if mut == nil || mut.IsNew() || !mut.IsUncommittedVersion() {
			return nil
		}
		return fn(lease.NewIDVersionPrev(mut.OriginalName(), mut.OriginalID(), mut.OriginalVersion()))
	})
}

func (sd *storedDescriptors) iterateUncommittedByID(fn func(imm catalog.Descriptor) error) error {
	return sd.descs.IterateByID(func(entry catalog.NameEntry) error {
		u := entry.(*storedDescriptor)
		if u.storedDescriptorStatus != checkedOutAtLeastOnce || !u.immutable.IsUncommittedVersion() {
			return nil
		}
		return fn(u.immutable)
	})
}

func (sd *storedDescriptors) getUncommittedTables() (tables []catalog.TableDescriptor) {
	_ = sd.iterateUncommittedByID(func(desc catalog.Descriptor) error {
		if table, ok := desc.(catalog.TableDescriptor); ok {
			tables = append(tables, table)
		}
		return nil
	})
	return tables
}

func (sd *storedDescriptors) getUncommittedDescriptorsForValidation() (descs []catalog.Descriptor) {
	_ = sd.iterateUncommittedByID(func(desc catalog.Descriptor) error {
		descs = append(descs, desc)
		return nil
	})
	return descs
}

func (sd *storedDescriptors) hasUncommittedTables() (has bool) {
	_ = sd.iterateUncommittedByID(func(desc catalog.Descriptor) error {
		if _, has = desc.(catalog.TableDescriptor); has {
			return iterutil.StopIteration()
		}
		return nil
	})
	return has
}

func (sd *storedDescriptors) hasUncommittedTypes() (has bool) {
	_ = sd.iterateUncommittedByID(func(desc catalog.Descriptor) error {
		if _, has = desc.(catalog.TypeDescriptor); has {
			return iterutil.StopIteration()
		}
		return nil
	})
	return has
}

var systemStoredDatabase = &storedDescriptor{
	immutable: dbdesc.NewBuilder(systemschema.SystemDB.DatabaseDesc()).BuildImmutableDatabase(),
	// Note that the mutable field is left as nil. We'll generate a new
	// value lazily when this is needed, which ought to be exceedingly rare.
	mutable:                nil,
	storedDescriptorStatus: notCheckedOutYet,
}

func (sd *storedDescriptors) maybeAddSystemDatabase() {
	if !sd.addedSystemDatabase {
		sd.addedSystemDatabase = true
		sd.descs.Upsert(systemStoredDatabase)
	}
}
