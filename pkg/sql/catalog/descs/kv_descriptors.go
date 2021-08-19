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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type kvDescriptors struct {
	codec keys.SQLCodec

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
		codec: codec,
	}
}

func (kd *kvDescriptors) reset() {
	kd.releaseAllDescriptors()
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

// lookupName is used when reading a descriptor from the storage layer by name.
// Descriptors are physically keyed by ID, so we need to resolve their ID by
// querying the system.namespace table first, which is what this method does.
// We can avoid having to do this in some special cases:
// - When the descriptor name and ID are hard-coded. This is the case for the
//   system database and for the tables in it.
// - When we're looking up a schema for which we already have the descriptor
//   of the parent database. The schema ID can be looked up in it.
//
// TODO(postamar): add namespace caching to the Collection
// By having the Collection mediate all namespace queries for a transaction
// (i.e. what it's already doing for descriptors) we could prevent more
// unnecessary roundtrips to the storage layer.
func (kd *kvDescriptors) lookupName(
	ctx context.Context,
	txn *kv.Txn,
	maybeDB catalog.DatabaseDescriptor,
	parentID descpb.ID,
	parentSchemaID descpb.ID,
	name string,
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
		id, err := lookupSystemDatabaseNamespaceCache(ctx, kd.codec, parentSchemaID, name)
		return id != descpb.InvalidID, id, err
	default:
		if parentSchemaID == descpb.InvalidID {
			// At this point we know that parentID is not zero, so a zero
			// parentSchemaID means we're looking up a schema.
			if maybeDB != nil {
				// Special case: looking up a schema, but in a database which we already
				// have the descriptor for. We find the schema ID in there.
				id := maybeDB.GetSchemaID(name)
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

// getByName reads a descriptor from the storage layer by name.
//
// This is a three-step process:
// 1. resolve the descriptor's ID using the name information,
// 2. actually read the descriptor from storage,
// 3. check that the name in the descriptor is the one we expect; meaning that
//    there is no RENAME underway for instance.
//
func (kd *kvDescriptors) getByName(
	ctx context.Context,
	txn *kv.Txn,
	maybeDB catalog.DatabaseDescriptor,
	parentID descpb.ID,
	parentSchemaID descpb.ID,
	name string,
) (desc catalog.MutableDescriptor, err error) {
	found, descID, err := kd.lookupName(ctx, txn, maybeDB, parentID, parentSchemaID, name)
	if !found || err != nil {
		return nil, err
	}
	desc, err = kd.getByID(ctx, txn, descID)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			// Having done the namespace lookup, the descriptor must exist.
			return nil, errors.WithAssertionFailure(err)
		}
		return nil, err
	}
	if desc.GetName() != name {
		// Immediately after a RENAME an old name still points to the descriptor
		// during the drain phase for the name. Do not return a descriptor during
		// draining.
		return nil, nil
	}
	return desc, nil
}

// getByID actually reads a descriptor from the storage layer.
func (kd *kvDescriptors) getByID(
	ctx context.Context, txn *kv.Txn, id descpb.ID,
) (_ catalog.MutableDescriptor, _ error) {
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
		return dbdesc.NewBuilder(systemschema.SystemDB.DatabaseDesc()).BuildExistingMutable(), nil
	}

	return catalogkv.MustGetMutableDescriptorByID(ctx, txn, kd.codec, id)
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

func (kd *kvDescriptors) idDefinitelyDoesNotExist(id descpb.ID) bool {
	if kd.allDescriptors.isEmpty() {
		return false
	}
	return !kd.allDescriptors.contains(id)
}
