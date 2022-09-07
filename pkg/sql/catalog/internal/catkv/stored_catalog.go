// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catkv

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// StoredCatalog is the data structure caching the descriptors in storage
// for a Collection. The descriptors are such as they were at the beginning of
// the transaction. Validation is performed lazily.
//
// A StoredCatalog can also be initialized in a bare-bones fashion with just
// a catalogReader and used for direct catalog access, see MakeDirect.
type StoredCatalog struct {
	catalogReader

	// cache mirrors the descriptors in storage.
	// This map does not store descriptors by name.
	cache nstree.IDMap

	// nameIndex is a subset of cache which allows lookups by name.
	nameIndex nstree.NameMap

	// validationLevels persists the levels to which each descriptor in cache
	// has already been validated.
	validationLevels map[descpb.ID]catalog.ValidationLevel

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
	// to track memory usage of StoredCatalog.
	memAcc *mon.BoundAccount
}

// MakeStoredCatalog returns a new instance of StoredCatalog.
func MakeStoredCatalog(
	codec keys.SQLCodec,
	version clusterversion.ClusterVersion,
	systemDatabaseCache *SystemDatabaseCache,
	monitor *mon.BytesMonitor,
) StoredCatalog {
	sd := StoredCatalog{
		catalogReader: catalogReader{
			Codec:               codec,
			Version:             version,
			systemDatabaseCache: systemDatabaseCache,
		},
	}
	if monitor != nil {
		memAcc := monitor.MakeBoundAccount()
		sd.memAcc = &memAcc
	}
	return sd
}

// Reset zeroes the object for re-use in a new transaction.
func (sc *StoredCatalog) Reset(ctx context.Context) {
	sc.cache.Clear()
	sc.nameIndex.Clear()
	if sc.memAcc != nil {
		sc.memAcc.Clear(ctx)
	}
	old := *sc
	*sc = StoredCatalog{
		catalogReader: old.catalogReader,
		cache:         old.cache,
		nameIndex:     old.nameIndex,
		memAcc:        old.memAcc,
	}
}

// ensure adds a descriptor to the StoredCatalog layer.
// This should not cause any information loss.
func (sc *StoredCatalog) ensure(ctx context.Context, desc catalog.Descriptor) error {
	if _, isMutable := desc.(catalog.MutableDescriptor); isMutable {
		return errors.AssertionFailedf("attempted to add mutable descriptor to StoredCatalog")
	}
	if sc.UpdateValidationLevel(desc, catalog.NoValidation) && sc.memAcc != nil {
		if err := sc.memAcc.Grow(ctx, desc.ByteSize()); err != nil {
			return err
		}
	}
	sc.cache.Upsert(desc)
	sc.nameIndex.Upsert(desc, desc.Dropped() || desc.SkipNamespace())
	return nil
}

// GetCachedByID looks up a descriptor by ID.
// The system database descriptor is given special treatment to speed up lookups
// and validations by avoiding an unnecessary round-trip to storage, as this
// descriptor is known to never change.
func (sc *StoredCatalog) GetCachedByID(id descpb.ID) catalog.Descriptor {
	if e := sc.cache.Get(id); e != nil {
		return e.(catalog.Descriptor)
	}
	return nil
}

// getCachedIDByName looks up a descriptor ID by name in the cache.
// Dropped descriptors are not added to the name index, so their IDs can't be
// looked up by name in the cache.
func (sc *StoredCatalog) getCachedIDByName(key descpb.NameInfo) descpb.ID {
	if e := sc.nameIndex.GetByName(key.ParentID, key.ParentSchemaID, key.Name); e != nil {
		return e.GetID()
	}
	// If we're looking up a schema name, find it in the database if we have it.
	if key.ParentID != descpb.InvalidID && key.ParentSchemaID == descpb.InvalidID {
		if parentDesc := sc.GetCachedByID(key.ParentID); parentDesc != nil {
			if db, ok := parentDesc.(catalog.DatabaseDescriptor); ok {
				return db.GetSchemaID(key.Name)
			}
		}
	}
	return descpb.InvalidID
}

// GetCachedByName is the by-name equivalent of GetCachedByID.
func (sc *StoredCatalog) GetCachedByName(
	dbID descpb.ID, schemaID descpb.ID, name string,
) catalog.Descriptor {
	key := descpb.NameInfo{ParentID: dbID, ParentSchemaID: schemaID, Name: name}
	if id := sc.getCachedIDByName(key); id != descpb.InvalidID {
		if desc := sc.GetCachedByID(id); desc != nil && desc.GetName() == name {
			return desc
		}
	}
	return nil
}

// EnsureAllDescriptors ensures that all stored descriptors are cached.
func (sc *StoredCatalog) EnsureAllDescriptors(ctx context.Context, txn *kv.Txn) error {
	if sc.hasAllDescriptors {
		return nil
	}
	c, err := sc.scanAll(ctx, txn)
	if err != nil {
		return err
	}
	if err = c.ForEachDescriptorEntry(func(desc catalog.Descriptor) error {
		return sc.ensure(ctx, desc)
	}); err != nil {
		return err
	}
	sc.hasAllDescriptors = true
	return nil
}

// EnsureAllDatabaseDescriptors ensures that all stored database descriptors
// are in the cache.
func (sc *StoredCatalog) EnsureAllDatabaseDescriptors(ctx context.Context, txn *kv.Txn) error {
	if sc.hasAllDescriptors || sc.hasAllDatabaseDescriptors {
		return nil
	}
	c, err := sc.scanNamespaceForDatabases(ctx, txn)
	if err != nil {
		return err
	}
	var readIDs catalog.DescriptorIDSet
	_ = c.ForEachNamespaceEntry(func(e catalog.NameEntry) error {
		if id := e.GetID(); sc.GetCachedByID(id) == nil {
			readIDs.Add(id)
		}
		return nil
	})
	if err = sc.EnsureFromStorageByIDs(ctx, txn, readIDs, catalog.Database); err != nil {
		return err
	}
	sc.hasAllDatabaseDescriptors = true
	return nil
}

func (sc *StoredCatalog) ensureAllSchemaIDsAndNamesForDatabase(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor,
) error {
	if sc.allSchemasForDatabase == nil {
		sc.allSchemasForDatabase = make(map[descpb.ID]map[descpb.ID]string)
	}
	if _, ok := sc.allSchemasForDatabase[db.GetID()]; ok {
		return nil
	}
	schemasNamespaceEntries, err := resolver.GetForDatabase(ctx, txn, sc.Codec, db)
	if err != nil {
		return err
	}
	m := make(map[descpb.ID]string, len(schemasNamespaceEntries))
	for id, entry := range schemasNamespaceEntries {
		m[id] = entry.Name
	}
	sc.allSchemasForDatabase[db.GetID()] = m
	return nil
}

// GetSchemaIDsAndNamesForDatabase generates a new map containing the ID -> name
// mappings for all schemas in the given database.
func (sc *StoredCatalog) GetSchemaIDsAndNamesForDatabase(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor,
) (map[descpb.ID]string, error) {
	if err := sc.ensureAllSchemaIDsAndNamesForDatabase(ctx, txn, db); err != nil {
		return nil, err
	}
	src := sc.allSchemasForDatabase[db.GetID()]
	ret := make(map[descpb.ID]string, len(src))
	for id, name := range src {
		ret[id] = name
	}
	return ret, nil
}

// IsIDKnownToNotExist returns false iff there definitely is no descriptor
// in storage with that ID.
func (sc *StoredCatalog) IsIDKnownToNotExist(id descpb.ID) bool {
	if !sc.hasAllDescriptors {
		return false
	}
	return sc.GetCachedByID(id) == nil
}

// LookupDescriptorID is used when reading a descriptor from the storage layer
// by name.
// Descriptors are physically keyed by ID, so we need to resolve their ID by
// querying the system.namespace table first, which is what this method does.
// We can avoid having to do this in some special cases:
//   - When the descriptor name and ID are hard-coded. This is the case for the
//     system database and for the tables in it.
//   - When we're looking up a schema for which we already have the descriptor
//     of the parent database. The schema ID can be looked up in it.
func (sc *StoredCatalog) LookupDescriptorID(
	ctx context.Context, txn *kv.Txn, parentID, parentSchemaID descpb.ID, name string,
) (descpb.ID, error) {
	// Look for the descriptor ID in memory first.
	key := descpb.NameInfo{ParentID: parentID, ParentSchemaID: parentSchemaID, Name: name}
	if id := sc.getCachedIDByName(key); id != descpb.InvalidID {
		return id, nil
	}
	// Fall back to querying the namespace table.
	c, err := sc.getNamespaceEntries(ctx, txn, []descpb.NameInfo{key})
	if err != nil {
		return descpb.InvalidID, err
	}
	if ne := c.LookupNamespaceEntry(key); ne != nil {
		return ne.GetID(), nil
	}
	return descpb.InvalidID, nil
}

// GetByName reads a descriptor from the storage layer by name.
//
// This is a three-step process:
//  1. resolve the descriptor's ID using the name information,
//  2. actually read the descriptor from storage,
//  3. check that the name in the descriptor is the one we expect; meaning that
//     there is no RENAME underway for instance.
func (sc *StoredCatalog) GetByName(
	ctx context.Context, txn *kv.Txn, parentID descpb.ID, parentSchemaID descpb.ID, name string,
) (catalog.Descriptor, error) {
	id, err := sc.LookupDescriptorID(ctx, txn, parentID, parentSchemaID, name)
	if err != nil || id == descpb.InvalidID {
		return nil, err
	}
	desc := sc.GetCachedByID(id)
	if desc == nil {
		err = sc.EnsureFromStorageByIDs(ctx, txn, catalog.MakeDescriptorIDSet(id), catalog.Any)
		if err != nil {
			if errors.Is(err, catalog.ErrDescriptorNotFound) {
				// Having done the namespace lookupObjectID, the descriptor must exist.
				return nil, errors.WithAssertionFailure(err)
			}
			return nil, err
		}
		desc = sc.GetCachedByID(id)
	}
	if desc.GetName() != name {
		// TODO(postamar): make StoredCatalog aware of name ops
		return nil, nil
	}
	return desc, nil
}

// EnsureFromStorageByIDs actually reads a batch of descriptors from storage
// and adds them to the cache. It assumes (without checking) that they are not
// already present in the cache.
func (sc *StoredCatalog) EnsureFromStorageByIDs(
	ctx context.Context,
	txn *kv.Txn,
	ids catalog.DescriptorIDSet,
	descriptorType catalog.DescriptorType,
) error {
	if ids.Empty() {
		return nil
	}
	c, err := sc.getDescriptorEntries(ctx, txn, ids.Ordered(), true /* isRequired */, descriptorType)
	if err != nil {
		return err
	}
	return c.ForEachDescriptorEntry(func(desc catalog.Descriptor) error {
		return sc.ensure(ctx, desc)
	})
}

// IterateCachedByID applies fn to all known descriptors in the cache in
// increasing sequence of IDs.
func (sc *StoredCatalog) IterateCachedByID(fn func(desc catalog.Descriptor) error) error {
	return sc.cache.Iterate(func(entry catalog.NameEntry) error {
		return fn(entry.(catalog.Descriptor))
	})
}

// IterateDatabasesByName applies fn to all known database descriptors in the
// cache in increasing sequence of names.
func (sc *StoredCatalog) IterateDatabasesByName(
	fn func(desc catalog.DatabaseDescriptor) error,
) error {
	return sc.nameIndex.IterateDatabasesByName(func(entry catalog.NameEntry) error {
		db, err := catalog.AsDatabaseDescriptor(entry.(catalog.Descriptor))
		if err != nil {
			return err
		}
		return fn(db)
	})
}

// GetValidationLevelByID returns the known level of validation for a cached
// descriptor.
func (sc *StoredCatalog) GetValidationLevelByID(id descpb.ID) catalog.ValidationLevel {
	if vl, ok := sc.validationLevels[id]; ok {
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
func (sc *StoredCatalog) UpdateValidationLevel(
	desc catalog.Descriptor, newLevel catalog.ValidationLevel,
) (wasUpdated bool) {
	if sc.validationLevels == nil {
		sc.validationLevels = make(map[descpb.ID]catalog.ValidationLevel)
	}
	if vl, ok := sc.validationLevels[desc.GetID()]; !ok || vl < newLevel {
		sc.validationLevels[desc.GetID()] = newLevel
		return true
	}
	return false
}

// RemoveFromNameIndex removes a descriptor from the name index.
// This needs to be done if the descriptor is to be promoted to the uncommitted
// layer which shadows this one in the descs.Collection.
func (sc *StoredCatalog) RemoveFromNameIndex(desc catalog.Descriptor) {
	sc.nameIndex.Remove(desc.GetID())
}

// NewValidationDereferencer returns this StoredCatalog object wrapped in a
// validate.ValidationDereferencer implementation. This ensures that any
// descriptors read for the purpose of validating others are also cached.
func (sc *StoredCatalog) NewValidationDereferencer(txn *kv.Txn) validate.ValidationDereferencer {
	return &storedCatalogBackedDereferencer{
		sc:  sc,
		txn: txn,
	}
}

type storedCatalogBackedDereferencer struct {
	sc  *StoredCatalog
	txn *kv.Txn
}

var _ validate.ValidationDereferencer = &storedCatalogBackedDereferencer{}

// DereferenceDescriptors implements the validate.ValidationDereferencer
// interface by leveraging the StoredCatalog's caching.
func (c storedCatalogBackedDereferencer) DereferenceDescriptors(
	ctx context.Context, version clusterversion.ClusterVersion, reqs []descpb.ID,
) (ret []catalog.Descriptor, _ error) {
	ret = make([]catalog.Descriptor, len(reqs))
	fallbackReqs := make([]descpb.ID, 0, len(reqs))
	fallbackRetIndexes := make([]int, 0, len(reqs))
	for i, id := range reqs {
		if cd := c.sc.GetCachedByID(id); cd == nil {
			fallbackReqs = append(fallbackReqs, id)
			fallbackRetIndexes = append(fallbackRetIndexes, i)
		} else {
			ret[i] = cd
		}
	}
	if len(fallbackReqs) > 0 {
		read, err := c.sc.getDescriptorEntries(ctx, c.txn, fallbackReqs, false /* isRequired */, catalog.Any)
		if err != nil {
			return nil, err
		}
		// Add all descriptors to the cache BEFORE validating them.
		err = read.ForEachDescriptorEntry(func(desc catalog.Descriptor) error {
			return c.sc.ensure(ctx, desc)
		})
		if err != nil {
			return nil, err
		}
		for j, id := range fallbackReqs {
			desc := read.LookupDescriptorEntry(id)
			if desc == nil {
				continue
			}
			if err = validate.Self(version, desc); err != nil {
				return nil, err
			}
			c.sc.UpdateValidationLevel(desc, catalog.ValidationLevelSelfOnly)
			ret[fallbackRetIndexes[j]] = desc
		}
	}
	return ret, nil
}

// DereferenceDescriptorIDs implements the validate.ValidationDereferencer
// interface.
func (c storedCatalogBackedDereferencer) DereferenceDescriptorIDs(
	ctx context.Context, reqs []descpb.NameInfo,
) ([]descpb.ID, error) {
	// TODO(postamar): cache namespace entries in StoredCatalog
	read, err := c.sc.getNamespaceEntries(ctx, c.txn, reqs)
	if err != nil {
		return nil, err
	}
	ret := make([]descpb.ID, len(reqs))
	for i, nameInfo := range reqs {
		if ne := read.LookupNamespaceEntry(nameInfo); ne != nil {
			ret[i] = ne.GetID()
		}
	}
	return ret, nil
}
