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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/catkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// storedDescriptors is the data structure caching the descriptors in storage
// for a Collection. The descriptors are such as they were at the beginning of
// the transaction. Validation is performed lazily.
type storedDescriptors struct {
	// codec used for reading from storage.
	codec keys.SQLCodec

	// cache mirrors the descriptors in storage.
	// This map does not store descriptors by name.
	cache nstree.Map

	// nameIndex is a subset of cache which allows lookups by name.
	nameIndex nstree.Map

	// validationLevels persists the levels to which each descriptor in cache
	// has already been validated.
	validationLevels map[descpb.ID]catalog.ValidationLevel

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
	sd := storedDescriptors{
		codec:           codec,
		systemNamespace: systemNamespace,
		memAcc:          monitor.MakeBoundAccount(),
	}
	return sd
}

// Reset zeroes the object for re-use in a new transaction.
func (sd *storedDescriptors) Reset(ctx context.Context) {
	sd.cache.Clear()
	sd.nameIndex.Clear()
	sd.memAcc.Clear(ctx)
	old := *sd
	*sd = storedDescriptors{
		codec:           old.codec,
		cache:           old.cache,
		nameIndex:       old.nameIndex,
		systemNamespace: old.systemNamespace,
		memAcc:          old.memAcc,
	}
}

// Ensure adds a descriptor to the storedDescriptors layer.
// This should not cause any information loss.
func (sd *storedDescriptors) Ensure(ctx context.Context, desc catalog.Descriptor) error {
	if _, isMutable := desc.(catalog.MutableDescriptor); isMutable {
		return errors.AssertionFailedf("attempted to add mutable descriptor to storedDescriptors")
	}
	if sd.UpdateValidationLevel(desc, catalog.NoValidation) {
		if err := sd.memAcc.Grow(ctx, desc.ByteSize()); err != nil {
			return err
		}
	}
	sd.cache.Upsert(desc, true /* skipNameMap */)
	sd.nameIndex.Upsert(desc, desc.Dropped() || desc.SkipNamespace())
	return nil
}

// GetCachedByID looks up a descriptor by ID.
// The system database descriptor is given special treatment to speed up lookups
// and validations by avoiding an unnecessary round-trip to storage, as this
// descriptor is known to never change.
func (sd *storedDescriptors) GetCachedByID(id descpb.ID) catalog.Descriptor {
	if e := sd.cache.GetByID(id); e != nil {
		return e.(catalog.Descriptor)
	}
	if id == keys.SystemDatabaseID {
		sd.UpdateValidationLevel(systemschema.SystemDB, validate.Write)
		sd.cache.Upsert(systemschema.SystemDB, true /* skipNameMap */)
		sd.nameIndex.Upsert(systemschema.SystemDB, systemschema.SystemDB.SkipNamespace())
		return systemschema.SystemDB
	}
	return nil
}

// GetCachedByName is the by-name equivalent of GetCachedByID.
// Since name collisions can occur in the presence of dropped descriptors, this
// will return the descriptor which has been added last, which should be the one
// which isn't dropped and actually owns the name.
func (sd *storedDescriptors) GetCachedByName(
	dbID descpb.ID, schemaID descpb.ID, name string,
) catalog.Descriptor {
	if e := sd.nameIndex.GetByName(dbID, schemaID, name); e != nil {
		return e.(catalog.Descriptor)
	}
	if dbID == 0 && schemaID == 0 && name == catconstants.SystemDatabaseName {
		return sd.GetCachedByID(keys.SystemDatabaseID)
	}
	return nil
}

// EnsureAllDescriptors ensures that all stored descriptors are cached.
func (sd *storedDescriptors) EnsureAllDescriptors(ctx context.Context, txn *kv.Txn) error {
	if sd.hasAllDescriptors {
		return nil
	}
	c, err := catkv.GetCatalogUnvalidated(ctx, sd.codec, txn)
	if err != nil {
		return err
	}
	if err = c.ForEachDescriptorEntry(func(desc catalog.Descriptor) error {
		return sd.Ensure(ctx, desc)
	}); err != nil {
		return err
	}
	sd.hasAllDescriptors = true
	return nil
}

// EnsureAllDatabaseDescriptors ensures that all stored database descriptors
// are in the cache.
func (sd *storedDescriptors) EnsureAllDatabaseDescriptors(ctx context.Context, txn *kv.Txn) error {
	if sd.hasAllDescriptors || sd.hasAllDatabaseDescriptors {
		return nil
	}
	c, err := catkv.GetAllDatabaseDescriptorIDs(ctx, txn, sd.codec)
	if err != nil {
		return err
	}
	var readIDs catalog.DescriptorIDSet
	_ = c.ForEachNamespaceEntry(func(e catalog.NameEntry) error {
		if id := e.GetID(); sd.GetCachedByID(id) == nil {
			readIDs.Add(id)
		}
		return nil
	})
	if err = sd.EnsureFromStorageByIDs(ctx, txn, readIDs, catalog.Database); err != nil {
		return err
	}
	sd.hasAllDatabaseDescriptors = true
	return nil
}

func (sd *storedDescriptors) ensureAllSchemaIDsAndNamesForDatabase(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor,
) error {
	if sd.allSchemasForDatabase == nil {
		sd.allSchemasForDatabase = make(map[descpb.ID]map[descpb.ID]string)
	}
	if _, ok := sd.allSchemasForDatabase[db.GetID()]; ok {
		return nil
	}
	schemasNamespaceEntries, err := resolver.GetForDatabase(ctx, txn, sd.codec, db)
	if err != nil {
		return err
	}
	m := make(map[descpb.ID]string, len(schemasNamespaceEntries))
	for id, entry := range schemasNamespaceEntries {
		m[id] = entry.Name
	}
	sd.allSchemasForDatabase[db.GetID()] = m
	return nil
}

// GetSchemaIDsAndNamesForDatabase generates a new map containing the ID -> name
// mappings for all schemas in the given database.
func (sd *storedDescriptors) GetSchemaIDsAndNamesForDatabase(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor,
) (map[descpb.ID]string, error) {
	if err := sd.ensureAllSchemaIDsAndNamesForDatabase(ctx, txn, db); err != nil {
		return nil, err
	}
	src := sd.allSchemasForDatabase[db.GetID()]
	ret := make(map[descpb.ID]string, len(src))
	for id, name := range src {
		ret[id] = name
	}
	return ret, nil
}

// IsIDKnownToNotExist returns false iff there definitely is no descriptor
// in storage with that ID.
func (sd *storedDescriptors) IsIDKnownToNotExist(id descpb.ID) bool {
	if !sd.hasAllDescriptors {
		return false
	}
	return sd.GetCachedByID(id) == nil
}

// LookupName is used when reading a descriptor from the storage layer by name.
// Descriptors are physically keyed by ID, so we need to resolve their ID by
// querying the system.namespace table first, which is what this method does.
// We can avoid having to do this in some special cases:
// - When the descriptor name and ID are hard-coded. This is the case for the
//   system database and for the tables in it.
// - When we're looking up a schema for which we already have the descriptor
//   of the parent database. The schema ID can be looked up in it.
//
func (sd *storedDescriptors) LookupName(
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
		if name == catconstants.SystemDatabaseName {
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

// GetByName reads a descriptor from the storage layer by name.
//
// This is a three-step process:
// 1. resolve the descriptor's ID using the name information,
// 2. actually read the descriptor from storage,
// 3. check that the name in the descriptor is the one we expect; meaning that
//    there is no RENAME underway for instance.
//
func (sd *storedDescriptors) GetByName(
	ctx context.Context, txn *kv.Txn, parentID descpb.ID, parentSchemaID descpb.ID, name string,
) (catalog.Descriptor, error) {
	var maybeDB catalog.DatabaseDescriptor
	if parent := sd.GetCachedByID(parentID); parent != nil {
		maybeDB, _ = catalog.AsDatabaseDescriptor(parent)
	}
	id, err := sd.LookupName(ctx, txn, maybeDB, parentID, parentSchemaID, name)
	if err != nil || id == descpb.InvalidID {
		return nil, err
	}
	desc := sd.GetCachedByID(id)
	if desc == nil {
		err = sd.EnsureFromStorageByIDs(ctx, txn, catalog.MakeDescriptorIDSet(id), catalog.Any)
		if err != nil {
			if errors.Is(err, catalog.ErrDescriptorNotFound) {
				// Having done the namespace lookup, the descriptor must exist.
				return nil, errors.WithAssertionFailure(err)
			}
			return nil, err
		}
		desc = sd.GetCachedByID(id)
	}
	if desc.GetName() != name {
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
	return desc, nil
}

// EnsureFromStorageByIDs actually reads a batch of descriptors from storage
// and adds them to the cache. It assumes (without checking) that they are not
// already present in the cache.
func (sd *storedDescriptors) EnsureFromStorageByIDs(
	ctx context.Context,
	txn *kv.Txn,
	ids catalog.DescriptorIDSet,
	descriptorType catalog.DescriptorType,
) error {
	if ids.Empty() {
		return nil
	}
	descs, err := catkv.MustGetDescriptorsByIDUnvalidated(
		ctx, sd.codec, txn, ids.Ordered(), descriptorType,
	)
	if err != nil {
		return err
	}
	for _, desc := range descs {
		if err = sd.Ensure(ctx, desc); err != nil {
			return err
		}
	}
	return nil
}

// IterateCachedByID applies fn to all known descriptors in the cache in
// increasing sequence of IDs.
func (sd *storedDescriptors) IterateCachedByID(fn func(desc catalog.Descriptor) error) error {
	return sd.cache.IterateByID(func(entry catalog.NameEntry) error {
		return fn(entry.(catalog.Descriptor))
	})
}

// IterateDatabasesByName applies fn to all known database descriptors in the
// cache in increasing sequence of names.
func (sd *storedDescriptors) IterateDatabasesByName(
	fn func(desc catalog.DatabaseDescriptor) error,
) error {
	return sd.nameIndex.IterateDatabasesByName(func(entry catalog.NameEntry) error {
		db, err := catalog.AsDatabaseDescriptor(entry.(catalog.Descriptor))
		if err != nil {
			return err
		}
		return fn(db)
	})
}

// GetValidationLevelByID returns the known level of validation for a cached
// descriptor.
func (sd *storedDescriptors) GetValidationLevelByID(id descpb.ID) catalog.ValidationLevel {
	if vl, ok := sd.validationLevels[id]; ok {
		return vl
	}
	if id == keys.SystemDatabaseID {
		return validate.Write
	}
	return catalog.NoValidation
}

// UpdateValidationLevel increases the known level of validation for a cached
// descriptor, if the new level is higher than the previous. In that case it
// returns true.
func (sd *storedDescriptors) UpdateValidationLevel(
	desc catalog.Descriptor, newLevel catalog.ValidationLevel,
) (wasUpdated bool) {
	if sd.validationLevels == nil {
		sd.validationLevels = make(map[descpb.ID]catalog.ValidationLevel)
	}
	if vl, ok := sd.validationLevels[desc.GetID()]; !ok || vl < newLevel {
		sd.validationLevels[desc.GetID()] = newLevel
		return true
	}
	return false
}

// RemoveUncommitted removes a newly-uncommitted descriptor from the name
// index. If this isn't done this layer won't be properly shadowed by the
// uncommitted layer.
func (sd *storedDescriptors) RemoveUncommitted(desc catalog.Descriptor) {
	sd.nameIndex.Remove(desc.GetID())
}
