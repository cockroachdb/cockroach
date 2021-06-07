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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// uncommittedDescriptor is a descriptor that has been modified in the current
// transaction.
type uncommittedDescriptor struct {
	mutable   catalog.MutableDescriptor
	immutable catalog.Descriptor
}

type kvDescriptors struct {
	codec keys.SQLCodec

	// Descriptors modified by the uncommitted transaction affiliated with this
	// Collection. This allows a transaction to see its own modifications while
	// bypassing the descriptor lease mechanism. The lease mechanism will have its
	// own transaction to read the descriptor and will hang waiting for the
	// uncommitted changes to the descriptor. These descriptors are local to this
	// Collection and invisible to other transactions.
	// TODO (lucy): Replace this with a data structure for faster lookups.
	// Currently, the order in which descriptors are inserted matters, since we
	// look at the draining names to account for descriptors being renamed. Any
	// replacement data structure may have to store the name information in some
	// different, more explicit way.
	uncommittedDescriptors []*uncommittedDescriptor

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

func makeReadDescriptors(codec keys.SQLCodec) kvDescriptors {
	return kvDescriptors{
		codec: codec,
	}
}

func (kd *kvDescriptors) reset() {
	kd.releaseAllDescriptors()
	kd.uncommittedDescriptors = nil
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
) (catalog.Descriptor, error) {
	// Always pick up a mutable copy so it can be cached.
	// TODO (lucy): If the descriptor doesn't exist, should we generate our
	// own error here instead of using the one from catalogkv?
	desc, err := catalogkv.MustGetMutableDescriptorByID(ctx, txn, kd.codec, id)
	if err != nil {
		return nil, err
	}
	ud, err := kd.addUncommittedDescriptor(desc)
	if err != nil {
		return nil, err
	}
	if mutable {
		return desc, nil
	}
	return ud.immutable, nil
}

func (kd *kvDescriptors) getByName(
	ctx context.Context,
	txn *kv.Txn,
	parentID descpb.ID,
	parentSchemaID descpb.ID,
	name string,
	mutable bool,
) (found bool, desc catalog.Descriptor, err error) {
	// Bypass the namespace lookup from the store for system tables.
	descID := bootstrap.LookupSystemTableDescriptorID(kd.codec, parentID, name)
	isSystemDescriptor := descID != descpb.InvalidID
	if !isSystemDescriptor {
		var found bool
		var err error
		found, descID, err = catalogkv.LookupObjectID(ctx, txn, kd.codec, parentID, parentSchemaID, name)
		if err != nil || !found {
			return found, nil, err
		}
	}
	// Always pick up a mutable copy so it can be cached.
	desc, err = catalogkv.GetMutableDescriptorByID(ctx, txn, kd.codec, descID)
	if err != nil {
		return false, nil, err
	} else if desc == nil && isSystemDescriptor {
		// This can happen during startup because we're not actually looking up the
		// system descriptor IDs in KV.
		return false, nil, errors.Wrapf(catalog.ErrDescriptorNotFound, "descriptor %d not found", descID)
	} else if desc == nil {
		// Having done the namespace lookup, the descriptor must exist.
		return false, nil, errors.AssertionFailedf("descriptor %d not found", descID)
	}
	// Immediately after a RENAME an old name still points to the descriptor
	// during the drain phase for the name. Do not return a descriptor during
	// draining.
	if desc.GetName() != name {
		return false, nil, nil
	}
	ud, err := kd.addUncommittedDescriptor(desc.(catalog.MutableDescriptor))
	if err != nil {
		return false, nil, err
	}
	if !mutable {
		desc = ud.immutable
	}
	return true, desc, nil
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
	for i := len(kd.uncommittedDescriptors) - 1; i >= 0; i-- {
		desc := kd.uncommittedDescriptors[i]
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
				return true, nil
			}
		}

		// Otherwise, if the name matches, we return it. It's up to the caller to
		// filter descriptors in non-public states.
		// TODO (lucy): Is it possible to return dropped descriptors at this point,
		// after the previous draining names check?
		if lease.NameMatchesDescriptor(mutDesc, dbID, schemaID, name) {
			return false, desc
		}
	}
	return false, nil
}

func (kd *kvDescriptors) getAllDescriptors(
	ctx context.Context, txn *kv.Txn,
) ([]catalog.Descriptor, error) {
	if kd.allDescriptors.isEmpty() {
		descs, err := catalogkv.GetAllDescriptors(ctx, txn, kd.codec)
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
	for _, desc := range kd.uncommittedDescriptors {
		if desc.mutable.GetID() == id {
			return desc
		}
	}
	return nil
}

func (kd *kvDescriptors) getDescriptorsWithNewVersion() []lease.IDVersion {
	var descs []lease.IDVersion
	for _, desc := range kd.uncommittedDescriptors {
		if mut := desc.mutable; !mut.IsNew() && mut.IsUncommittedVersion() {
			descs = append(descs, lease.NewIDVersionPrev(mut.OriginalName(), mut.OriginalID(), mut.OriginalVersion()))
		}
	}
	return descs
}

func (kd *kvDescriptors) getUncommittedTables() (tables []catalog.TableDescriptor) {
	for _, desc := range kd.uncommittedDescriptors {
		table, ok := desc.immutable.(catalog.TableDescriptor)
		if ok && desc.immutable.IsUncommittedVersion() {
			tables = append(tables, table)
		}
	}
	return tables
}

func (kd *kvDescriptors) validateUncommittedDescriptors(ctx context.Context, txn *kv.Txn) error {
	descs := make([]catalog.Descriptor, len(kd.uncommittedDescriptors))
	for i, ud := range kd.uncommittedDescriptors {
		descs[i] = ud.immutable
	}
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

func (kd *kvDescriptors) hasUncommittedTables() bool {
	for _, desc := range kd.uncommittedDescriptors {
		if _, isTable := desc.immutable.(catalog.TableDescriptor); isTable {
			return true
		}
	}
	return false
}

func (kd *kvDescriptors) hasUncommittedTypes() bool {
	for _, desc := range kd.uncommittedDescriptors {
		if _, isType := desc.immutable.(catalog.TypeDescriptor); isType {
			return true
		}
	}
	return false
}

func (kd *kvDescriptors) addUncommittedDescriptor(
	desc catalog.MutableDescriptor,
) (*uncommittedDescriptor, error) {
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

	ud := &uncommittedDescriptor{
		mutable:   mutable,
		immutable: desc.ImmutableCopy(),
	}

	var found bool
	for i, d := range kd.uncommittedDescriptors {
		if d.mutable.GetID() == desc.GetID() {
			kd.uncommittedDescriptors[i], found = ud, true
			break
		}
	}
	if !found {
		kd.uncommittedDescriptors = append(kd.uncommittedDescriptors, ud)
	}
	kd.releaseAllDescriptors()
	return ud, nil
}

func (kd *kvDescriptors) idDefinitelyDoesNotExist(id descpb.ID) bool {
	if kd.allDescriptors.isEmpty() {
		return false
	}
	return !kd.allDescriptors.contains(id)
}
