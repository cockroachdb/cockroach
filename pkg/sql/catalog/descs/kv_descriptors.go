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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/catkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type kvDescriptors struct {
	codec keys.SQLCodec

	// systemNamespace is a cache of system table namespace entries. We assume
	// these are immutable for the life of the process.
	systemNamespace *systemDatabaseNamespaceCache

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
	c nstree.Catalog
}

func (d *allDescriptors) init(c nstree.Catalog) {
	d.c = c
}

func (d *allDescriptors) clear() {
	*d = allDescriptors{}
}

func (d *allDescriptors) isUnset() bool {
	return !d.c.IsInitialized()
}

func (d *allDescriptors) contains(id descpb.ID) bool {
	return d.c.IsInitialized() && d.c.LookupDescriptorEntry(id) != nil
}

func makeKVDescriptors(
	codec keys.SQLCodec, systemNamespace *systemDatabaseNamespaceCache,
) kvDescriptors {
	return kvDescriptors{
		codec:           codec,
		systemNamespace: systemNamespace,
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
func (kd *kvDescriptors) lookupName(
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
		id = kd.systemNamespace.lookup(parentSchemaID, name)
		if id != descpb.InvalidID {
			return id, err
		}
		// Make sure to cache the result if we had to look it up.
		defer func() {
			if err == nil && id != descpb.InvalidID {
				kd.systemNamespace.add(descpb.NameInfo{
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
		ctx, txn, kd.codec, parentID, parentSchemaID, name,
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
func (kd *kvDescriptors) getByName(
	ctx context.Context,
	txn *kv.Txn,
	version clusterversion.ClusterVersion,
	maybeDB catalog.DatabaseDescriptor,
	parentID descpb.ID,
	parentSchemaID descpb.ID,
	name string,
) (catalog.MutableDescriptor, error) {
	descID, err := kd.lookupName(ctx, txn, maybeDB, parentID, parentSchemaID, name)
	if err != nil || descID == descpb.InvalidID {
		return nil, err
	}
	descs, err := kd.getByIDs(ctx, txn, version, []descpb.ID{descID})
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
func (kd *kvDescriptors) getByIDs(
	ctx context.Context, txn *kv.Txn, version clusterversion.ClusterVersion, ids []descpb.ID,
) ([]catalog.MutableDescriptor, error) {
	ret := make([]catalog.MutableDescriptor, len(ids))
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
	kvDescs, err := catkv.MustGetDescriptorsByID(ctx, txn, kd.codec, version, kvIDs, catalog.Any)
	if err != nil {
		return nil, err
	}
	for j, desc := range kvDescs {
		b := desc.NewBuilder()
		b.RunPostDeserializationChanges()
		ret[indexes[j]] = b.BuildExistingMutable()
	}
	return ret, nil
}

func (kd *kvDescriptors) getAllDescriptors(
	ctx context.Context, txn *kv.Txn, version clusterversion.ClusterVersion,
) (nstree.Catalog, error) {
	if kd.allDescriptors.isUnset() {
		c, err := catkv.GetCatalogUnvalidated(ctx, txn, kd.codec)
		if err != nil {
			return nstree.Catalog{}, err
		}

		descs := c.OrderedDescriptors()
		ve := c.Validate(ctx, version, catalog.ValidationReadTelemetry, catalog.ValidationLevelCrossReferences, descs...)
		if err := ve.CombinedError(); err != nil {
			return nstree.Catalog{}, err
		}

		// There could be tables with user defined types that need hydrating.
		if err := HydrateGivenDescriptors(ctx, descs); err != nil {
			// If we ran into an error hydrating the types, that means that we
			// have some sort of corrupted descriptor state. Rather than disable
			// uses of getAllDescriptors, just log the error.
			log.Errorf(ctx, "%s", err.Error())
		}

		kd.allDescriptors.init(c)
	}
	return kd.allDescriptors.c, nil
}

func (kd *kvDescriptors) getAllDatabaseDescriptors(
	ctx context.Context, txn *kv.Txn, version clusterversion.ClusterVersion,
) ([]catalog.DatabaseDescriptor, error) {
	if kd.allDatabaseDescriptors == nil {
		c, err := catkv.GetAllDatabaseDescriptorIDs(ctx, txn, kd.codec)
		if err != nil {
			return nil, err
		}
		dbDescs, err := catkv.MustGetDescriptorsByID(ctx, txn, kd.codec, version, c.OrderedDescriptorIDs(), catalog.Database)
		if err != nil {
			return nil, err
		}
		kd.allDatabaseDescriptors = make([]catalog.DatabaseDescriptor, len(dbDescs))
		for i, dbDesc := range dbDescs {
			kd.allDatabaseDescriptors[i] = dbDesc.(catalog.DatabaseDescriptor)
		}
	}
	return kd.allDatabaseDescriptors, nil
}

func (kd *kvDescriptors) getSchemasForDatabase(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor,
) (map[descpb.ID]string, error) {
	if kd.allSchemasForDatabase == nil {
		kd.allSchemasForDatabase = make(map[descpb.ID]map[descpb.ID]string)
	}
	if _, ok := kd.allSchemasForDatabase[db.GetID()]; !ok {
		var err error
		allSchemas, err := resolver.GetForDatabase(ctx, txn, kd.codec, db)
		if err != nil {
			return nil, err
		}
		kd.allSchemasForDatabase[db.GetID()] = make(map[descpb.ID]string)
		for id, entry := range allSchemas {
			kd.allSchemasForDatabase[db.GetID()][id] = entry.Name
		}
	}
	return kd.allSchemasForDatabase[db.GetID()], nil
}

func (kd *kvDescriptors) idDefinitelyDoesNotExist(id descpb.ID) bool {
	if kd.allDescriptors.isUnset() {
		return false
	}
	return !kd.allDescriptors.contains(id)
}
