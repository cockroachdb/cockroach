// Copyright 2021 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// uncommittedDescriptor is a descriptor that has been modified in the current
// transaction.
type uncommittedDescriptor struct {
	mutable   catalog.MutableDescriptor
	immutable catalog.Descriptor
}

func (u *uncommittedDescriptor) GetName() string {
	return u.immutable.GetName()
}

func (u *uncommittedDescriptor) GetParentID() descpb.ID {
	return u.immutable.GetParentID()
}

func (u uncommittedDescriptor) GetParentSchemaID() descpb.ID {
	return u.immutable.GetParentSchemaID()
}

func (u uncommittedDescriptor) GetID() descpb.ID {
	return u.immutable.GetID()
}

var _ catalog.NameEntry = (*uncommittedDescriptor)(nil)

type kvDescriptors struct {
	codec keys.SQLCodec

	// Descriptors modified by the uncommitted transaction affiliated with this
	// Collection. This allows a transaction to see its own modifications while
	// bypassing the descriptor lease mechanism. The lease mechanism will have its
	// own transaction to read the descriptor and will hang waiting for the
	// uncommitted changes to the descriptor. These descriptors are local to this
	// Collection and invisible to other transactions.
	uncommittedDescriptors nstree.Map
	// uncommittedDescriptorNames is the set of names which a read or written
	// descriptor took on at some point in its lifetime. Everything added to
	// uncommittedDescriptors is added to uncommittedDescriptorNames as well
	// as all of the known draining names. The idea is that if we find that
	// a name is not in the above map but is in the set, then we can avoid
	// doing a lookup.
	uncommittedDescriptorNames nstree.Set

	// allDescriptors is a slice of all available descriptors. The descriptors
	// are cached to avoid repeated lookups by users like virtual tables. The
	// cache is purged whenever events would cause a scan of all descriptors to
	// return different values, such as when the txn timestamp changes or when
	// new descriptors are written in the txn.
	//
	// TODO(ajwerner): This cache may be problematic in clusters with very large
	// numbers of descriptors.
	allDescriptors allDescriptors

	// allDatabaseDescriptors is a slice of all available database descriptors.
	// These are purged at the same time as allDescriptors.
	allDatabaseDescriptors []catalog.DatabaseDescriptor

	// allSchemasForDatabase maps databaseID -> schemaID -> schemaName.
	// For each databaseID, all schemas visible under the database can be
	// observed.
	// These are purged at the same time as allDescriptors.
	allSchemasForDatabase map[descpb.ID]map[descpb.ID]string
}

// allDescriptors is an abstraction to capture the complete set of descriptors
// read from the store. It is used to accelerate repeated invocations of virtual
// tables which utilize descriptors. It tends to get used to build a
// sql.internalLookupCtx.
//
// TODO(ajwerner): Memory monitoring.
// TODO(ajwerner): Unify this struct with the uncommittedDescriptors set.
// TODO(ajwerner): Unify the sql.internalLookupCtx with the descs.Collection.
type allDescriptors struct {
	descs []catalog.Descriptor
	byID  map[descpb.ID]int
}

func (d *allDescriptors) init(descriptors []catalog.Descriptor) {
	d.descs = descriptors
	d.byID = make(map[descpb.ID]int, len(descriptors))
	for i, desc := range descriptors {
		d.byID[desc.GetID()] = i
	}
}

func (d *allDescriptors) clear() {
	d.descs = nil
	d.byID = nil
}

func (d *allDescriptors) isEmpty() bool {
	return d.descs == nil
}

func (d *allDescriptors) contains(id descpb.ID) bool {
	_, exists := d.byID[id]
	return exists
}

func makeKVDescriptors(codec keys.SQLCodec) kvDescriptors {
	return kvDescriptors{
		codec:                      codec,
		uncommittedDescriptors:     nstree.MakeMap(),
		uncommittedDescriptorNames: nstree.MakeSet(),
	}
}

func (kd *kvDescriptors) reset() {
	kd.releaseAllDescriptors()
	kd.uncommittedDescriptors.Clear()
	kd.uncommittedDescriptorNames.Clear()
}

// releaseAllDescriptors releases the cached slice of all descriptors
// held by Collection.
//
// TODO(ajwerner): Make this unnecessary by ensuring that all writes properly
// interact with this layer.
func (kd *kvDescriptors) releaseAllDescriptors() {
	kd.allDescriptors.clear()
	kd.allDatabaseDescriptors = nil
	kd.allSchemasForDatabase = nil
}

func (kd *kvDescriptors) getByID(
	ctx context.Context, txn *kv.Txn, id descpb.ID, mutable bool,
) (desc catalog.Descriptor, err error) {
	_, desc, err = kd.getDescriptor(ctx, txn, id, mutable, "" /* maybeLookedUpName */)
	return desc, err
}

func (kd *kvDescriptors) lookupName(
	ctx context.Context, txn *kv.Txn, parentID descpb.ID, parentSchemaID descpb.ID, name string,
) (found bool, _ descpb.ID, _ error) {
	// Handle special cases which might avoid a namespace table query.
	switch parentID {
	case descpb.InvalidID:
		if name == systemschema.SystemDatabaseName {
			// Special case: looking up the system database.
			// The system database's descriptor ID is hard-coded.
			return true, keys.SystemDatabaseID, nil
		}
	case keys.SystemDatabaseID:
		// Special case: looking up something in the system database.
		// Those namespace table entries are cached.
		id, err := lookupSystemDatabaseNamespaceCache(ctx, txn, kd.codec, parentSchemaID, name)
		return id != descpb.InvalidID, id, err
	default:
		if parentSchemaID == descpb.InvalidID {
			if ud := kd.getUncommittedByID(parentID); ud != nil {
				// Special case: looking up a schema, but in a database which we already
				// have the descriptor for. We find the schema ID in there.
				id := ud.immutable.(catalog.DatabaseDescriptor).GetSchemaID(name)
				return id != descpb.InvalidID, id, nil
			}
		}
	}
	// Fall back to querying the namespace table.
	found, id, err := catalogkv.LookupObjectID(
		ctx, txn, kd.codec, parentID, parentSchemaID, name,
	)
	if err != nil || !found {
		return found, descpb.InvalidID, err
	}
	return true, id, nil
}

func (kd *kvDescriptors) getByName(
	ctx context.Context,
	txn *kv.Txn,
	parentID descpb.ID,
	parentSchemaID descpb.ID,
	name string,
	mutable bool,
) (found bool, desc catalog.Descriptor, err error) {
	found, descID, err := kd.lookupName(ctx, txn, parentID, parentSchemaID, name)
	if !found || err != nil {
		return found, nil, err
	}
	return kd.getDescriptor(ctx, txn, descID, mutable, name)
}

func (kd *kvDescriptors) getDescriptor(
	ctx context.Context, txn *kv.Txn, id descpb.ID, mutable bool, maybeLookedUpName string,
) (found bool, _ catalog.Descriptor, _ error) {
	withNameLookup := maybeLookedUpName != ""
	if id == keys.SystemDatabaseID {
		// Special handling for the system database descriptor.
		// This database descriptor must be immutable.
		if mutable {
			return false, nil, errors.AssertionFailedf("system database descriptor cannot be mutable", id)
		}
		return true, systemschema.SystemDB, nil
	}
	// Always pick up a mutable copy so it can be cached.
	desc, err := catalogkv.MustGetMutableDescriptorByID(ctx, txn, kd.codec, id)
	if err != nil {
		if withNameLookup && errors.Is(err, catalog.ErrDescriptorNotFound) {
			// Having done the namespace lookup, the descriptor must exist.
			err = errors.HandleAsAssertionFailure(err)
		}
		return false, nil, err
	}
	if withNameLookup {
		// Immediately after a RENAME an old name still points to the descriptor
		// during the drain phase for the name. Do not return a descriptor during
		// draining.
		if desc.GetName() != maybeLookedUpName {
			return false, nil, nil
		}
	}
	ud, err := kd.addUncommittedDescriptor(desc)
	if err != nil {
		return false, nil, err
	}
	if !mutable {
		return true, ud.immutable, nil
	}
	return true, ud.mutable, nil
}

// getUncommittedByName returns a descriptor for the requested name
// if the requested name is for a descriptor modified within the transaction
// affiliated with the Collection.
//
// The first return value "refuseFurtherLookup" is true when there is a known
// rename of that descriptor, so it would be invalid to miss the cache and go to
// KV (where the descriptor prior to the rename may still exist).
func (kd *kvDescriptors) getUncommittedByName(
	dbID descpb.ID, schemaID descpb.ID, name string,
) (refuseFurtherLookup bool, desc *uncommittedDescriptor) {

	// Walk latest to earliest so that a DROP followed by a CREATE with the same
	// name will result in the CREATE being seen.
	if got := kd.uncommittedDescriptors.GetByName(dbID, schemaID, name); got != nil {
		return false, got.(*uncommittedDescriptor)
	}
	return kd.uncommittedDescriptorNames.Contains(descpb.NameInfo{
		ParentID:       dbID,
		ParentSchemaID: schemaID,
		Name:           name,
	}), nil
}

func (kd *kvDescriptors) getAllDescriptors(
	ctx context.Context, txn *kv.Txn,
) ([]catalog.Descriptor, error) {
	if kd.allDescriptors.isEmpty() {
		descs, err := catalogkv.GetAllDescriptors(ctx, txn, kd.codec, true /* shouldRunPostDeserializationChanges */)
		if err != nil {
			return nil, err
		}

		// There could be tables with user defined types that need hydrating.
		if err := HydrateGivenDescriptors(ctx, descs); err != nil {
			// If we ran into an error hydrating the types, that means that we
			// have some sort of corrupted descriptor state. Rather than disable
			// uses of GetAllDescriptors, just log the error.
			log.Errorf(ctx, "%s", err.Error())
		}

		kd.allDescriptors.init(descs)
	}
	return kd.allDescriptors.descs, nil
}

func (kd *kvDescriptors) getAllDatabaseDescriptors(
	ctx context.Context, txn *kv.Txn,
) ([]catalog.DatabaseDescriptor, error) {
	if kd.allDatabaseDescriptors == nil {
		dbDescIDs, err := catalogkv.GetAllDatabaseDescriptorIDs(ctx, txn, kd.codec)
		if err != nil {
			return nil, err
		}
		dbDescs, err := catalogkv.GetDatabaseDescriptorsFromIDs(
			ctx, txn, kd.codec, dbDescIDs,
		)
		if err != nil {
			return nil, err
		}
		kd.allDatabaseDescriptors = dbDescs
	}
	return kd.allDatabaseDescriptors, nil
}

func (kd *kvDescriptors) getSchemasForDatabase(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID,
) (map[descpb.ID]string, error) {
	if kd.allSchemasForDatabase == nil {
		kd.allSchemasForDatabase = make(map[descpb.ID]map[descpb.ID]string)
	}
	if _, ok := kd.allSchemasForDatabase[dbID]; !ok {
		var err error
		kd.allSchemasForDatabase[dbID], err = resolver.GetForDatabase(ctx, txn, kd.codec, dbID)
		if err != nil {
			return nil, err
		}
	}
	return kd.allSchemasForDatabase[dbID], nil
}

func (kd *kvDescriptors) getUncommittedByID(id descpb.ID) *uncommittedDescriptor {
	ud, _ := kd.uncommittedDescriptors.GetByID(id).(*uncommittedDescriptor)
	return ud
}

func (kd *kvDescriptors) getDescriptorsWithNewVersion() []lease.IDVersion {
	var descs []lease.IDVersion
	_ = kd.uncommittedDescriptors.IterateByID(func(entry catalog.NameEntry) error {
		desc := entry.(*uncommittedDescriptor)
		if mut := desc.mutable; !mut.IsNew() && mut.IsUncommittedVersion() {
			orig := mut.ImmutableCopyOfOriginalVersion()
			descs = append(descs, lease.NewIDVersionPrev(orig.GetName(), orig.GetID(), orig.GetVersion()))
		}
		return nil
	})
	return descs
}

func (kd *kvDescriptors) getUncommittedTables() (tables []catalog.TableDescriptor) {
	_ = kd.uncommittedDescriptors.IterateByID(func(entry catalog.NameEntry) error {
		desc := entry.(*uncommittedDescriptor)
		table, ok := desc.immutable.(catalog.TableDescriptor)
		if ok && desc.immutable.IsUncommittedVersion() {
			tables = append(tables, table)
		}
		return nil
	})
	return tables
}

func (kd *kvDescriptors) validateUncommittedDescriptors(ctx context.Context, txn *kv.Txn) error {
	descs := make([]catalog.Descriptor, 0, kd.uncommittedDescriptors.Len())
	_ = kd.uncommittedDescriptors.IterateByID(func(descriptor catalog.NameEntry) error {
		ud := descriptor.(*uncommittedDescriptor)
		newVersion := ud.mutable.ImmutableCopy()
		oldVersion := ud.mutable.ImmutableCopyOfOriginalVersion()
		if !oldVersion.DescriptorProto().Equal(newVersion.DescriptorProto()) {
			// Only validate descriptors which have changed.
			descs = append(descs, ud.immutable)
		}
		return nil
	})
	if len(descs) == 0 {
		return nil
	}
	// TODO(ajwerner): Leverage this cache as the DescGetter.
	bdg := catalogkv.NewOneLevelUncachedDescGetter(txn, kd.codec)
	return catalog.Validate(
		ctx,
		bdg,
		catalog.ValidationWriteTelemetry,
		catalog.ValidationLevelAllPreTxnCommit,
		descs...,
	).CombinedError()
}

func (kd *kvDescriptors) hasUncommittedTables() (has bool) {
	_ = kd.uncommittedDescriptors.IterateByID(func(entry catalog.NameEntry) error {
		if _, has = entry.(*uncommittedDescriptor).immutable.(catalog.TableDescriptor); has {
			return iterutil.StopIteration()
		}
		return nil
	})
	return has
}

func (kd *kvDescriptors) hasUncommittedTypes() (has bool) {
	_ = kd.uncommittedDescriptors.IterateByID(func(entry catalog.NameEntry) error {
		if _, has = entry.(*uncommittedDescriptor).immutable.(catalog.TypeDescriptor); has {
			return iterutil.StopIteration()
		}
		return nil
	})
	return has
}

func (kd *kvDescriptors) addUncommittedDescriptor(
	desc catalog.MutableDescriptor,
) (*uncommittedDescriptor, error) {
	version := desc.GetVersion()
	origVersion := desc.ImmutableCopyOfOriginalVersion().GetVersion()
	if version != origVersion && version != origVersion+1 {
		return nil, errors.AssertionFailedf(
			"descriptor %d version %d not compatible with cluster version %d",
			desc.GetID(), version, origVersion)
	}

	mutable, err := maybeRefreshCachedFieldsOnTypeDescriptor(desc)
	if err != nil {
		return nil, err
	}

	ud := &uncommittedDescriptor{
		mutable:   mutable,
		immutable: desc.ImmutableCopy(),
	}
	for _, n := range desc.GetDrainingNames() {
		kd.uncommittedDescriptorNames.Add(n)
	}
	kd.uncommittedDescriptors.Upsert(ud)
	kd.releaseAllDescriptors()
	return ud, nil
}

func (kd *kvDescriptors) idDefinitelyDoesNotExist(id descpb.ID) bool {
	if kd.allDescriptors.isEmpty() {
		return false
	}
	return !kd.allDescriptors.contains(id)
}
