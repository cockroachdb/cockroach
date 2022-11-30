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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
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
	CatalogReader
	DescriptorValidationModeProvider

	// cache mirrors the descriptors in storage.
	// This map does not store descriptors by name.
	cache nstree.IDMap

	// comments mirrors the comments in storage.
	comments map[catalogkeys.CommentKey]string

	// zoneConfigs mirrors the zone configurations in storage.
	zoneConfigs storedZoneConfigs

	// nameIndex is a subset of cache which allows lookups by name.
	nameIndex nstree.NameMap

	// drainedNames is a set of keys which can't be looked up in nameIndex.
	drainedNames map[descpb.NameInfo]struct{}

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

	// allDescriptorsForDatabase maps databaseID->nstree.Catalog
	allDescriptorsForDatabase map[descpb.ID]nstree.Catalog

	// memAcc is the actual account of an injected, upstream monitor
	// to track memory usage of StoredCatalog.
	memAcc *mon.BoundAccount
}

// DescriptorValidationModeProvider encapsulates the descriptor_validation
// session variable state.
type DescriptorValidationModeProvider interface {

	// ValidateDescriptorsOnRead returns true iff 'on' or 'read_only'.
	ValidateDescriptorsOnRead() bool

	// ValidateDescriptorsOnWrite returns true iff 'on'.
	ValidateDescriptorsOnWrite() bool
}

// MakeStoredCatalog returns a new instance of StoredCatalog.
func MakeStoredCatalog(
	cr CatalogReader, dvmp DescriptorValidationModeProvider, monitor *mon.BytesMonitor,
) StoredCatalog {
	sc := StoredCatalog{
		CatalogReader:                    cr,
		DescriptorValidationModeProvider: dvmp,
		comments:                         make(map[catalogkeys.CommentKey]string),
		zoneConfigs:                      makeStoredZoneConfigs(),
	}
	if monitor != nil {
		memAcc := monitor.MakeBoundAccount()
		sc.memAcc = &memAcc
	}
	return sc
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
		CatalogReader:                    old.CatalogReader,
		DescriptorValidationModeProvider: old.DescriptorValidationModeProvider,
		cache:                            old.cache,
		nameIndex:                        old.nameIndex,
		memAcc:                           old.memAcc,
		comments:                         make(map[catalogkeys.CommentKey]string),
		zoneConfigs:                      makeStoredZoneConfigs(),
	}
}

func (sc *StoredCatalog) ensureAll(ctx context.Context, c nstree.Catalog) error {
	if err := sc.ensure(ctx, c); err != nil {
		return err
	}

	if err := sc.ensureComment(ctx, c); err != nil {
		return err
	}

	return sc.ensureZoneConfig(ctx, c)
}

// ensure adds descriptors and namespace entries to the StoredCatalog layer.
// This should not cause any information loss.
func (sc *StoredCatalog) ensure(ctx context.Context, c nstree.Catalog) error {
	if err := c.ForEachDescriptorEntry(func(desc catalog.Descriptor) error {
		if _, isMutable := desc.(catalog.MutableDescriptor); isMutable {
			return errors.AssertionFailedf("attempted to add mutable descriptor to StoredCatalog")
		}
		if sc.UpdateValidationLevel(desc, catalog.NoValidation) && sc.memAcc != nil {
			if err := sc.memAcc.Grow(ctx, desc.ByteSize()); err != nil {
				return err
			}
		}
		sc.cache.Upsert(desc)
		skipNamespace := desc.Dropped() || desc.SkipNamespace()
		sc.nameIndex.Upsert(desc, skipNamespace)
		if !skipNamespace {
			delete(sc.drainedNames, descpb.NameInfo{
				ParentID:       desc.GetParentID(),
				ParentSchemaID: desc.GetParentSchemaID(),
				Name:           desc.GetName(),
			})
		}
		return nil
	}); err != nil {
		return err
	}
	return c.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
		if sc.nameIndex.GetByID(e.GetID()) == nil &&
			sc.nameIndex.GetByName(e.GetParentID(), e.GetParentSchemaID(), e.GetName()) == nil {
			sc.nameIndex.Upsert(e, false /* skipNameMap */)
		}
		delete(sc.drainedNames, descpb.NameInfo{
			ParentID:       e.GetParentID(),
			ParentSchemaID: e.GetParentSchemaID(),
			Name:           e.GetName(),
		})
		return nil
	})
}

func (sc *StoredCatalog) ensureComment(ctx context.Context, c nstree.Catalog) error {
	return c.ForEachCommentUnordered(func(key catalogkeys.CommentKey, cmt string) error {
		if existing, ok := sc.comments[key]; ok {
			sc.memAcc.Shrink(ctx, int64(len(existing)))
		}
		sc.comments[key] = cmt
		if err := sc.memAcc.Grow(ctx, int64(len(cmt))); err != nil {
			return err
		}
		return nil
	})
}

// markZoneConfigInfoSeen see storedZoneConfigs.markSeen
func (sc *StoredCatalog) markZoneConfigInfoSeen(id descpb.ID) {
	sc.zoneConfigs.markSeen(id)
}

// ensureZoneConfig see upserts all zone configs into the StoredCatalog.
func (sc *StoredCatalog) ensureZoneConfig(ctx context.Context, c nstree.Catalog) error {
	// Zone config should have been loaded together with descriptors in one batch.
	if err := c.ForEachDescriptorEntry(func(desc catalog.Descriptor) error {
		sc.markZoneConfigInfoSeen(desc.GetID())
		return nil
	}); err != nil {
		return err
	}
	return c.ForEachZoneConfigUnordered(func(id descpb.ID, zc catalog.ZoneConfig) error {
		if existing, _ := sc.zoneConfigs.get(id); existing != nil {
			sc.memAcc.Shrink(ctx, int64(existing.Size()))
		}
		sc.zoneConfigs.upsert(id, zc)
		if err := sc.memAcc.Grow(ctx, int64(zc.Size())); err != nil {
			return err
		}
		return nil
	})
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

// GetCachedComment checks if comments of an object is cached and return the
// comment if there is a comment exists for the key.
// 1. False is returned for "cached" if the object is never seen by this cache.
// 2. comment may not exist for a (objID, subID, cmtType) tuple even an object
// has been seen, in this case, "cached" will be true, and "hasCmt" will be
// false.
func (sc *StoredCatalog) GetCachedComment(
	key catalogkeys.CommentKey,
) (cmt string, hasCmt bool, cached bool) {
	if sc.cache.Get(descpb.ID(key.ObjectID)) == nil {
		return "", false, false
	}
	if cmt, ok := sc.comments[key]; ok {
		return cmt, ok, true
	}
	return "", false, true
}

// GetCachedZoneConfig checks if zone config of an object is cached and returns
// the zone config if so.
// 1. False is returned for "cached" if the object is never seen by this cache.
// 2. zone config record may not exist for a seen object. True for "cached" and
// False for "hasZoneConfig" will be return in that case.
func (sc *StoredCatalog) GetCachedZoneConfig(id descpb.ID) (zc catalog.ZoneConfig, cached bool) {
	return sc.zoneConfigs.get(id)
}

// GetCachedIDByName looks up a descriptor ID by name in the cache.
// Dropped descriptors are not added to the name index, so their IDs can't be
// looked up by name in the cache.
func (sc *StoredCatalog) GetCachedIDByName(key catalog.NameKey) descpb.ID {
	if _, found := sc.drainedNames[descpb.NameInfo{
		ParentID:       key.GetParentID(),
		ParentSchemaID: key.GetParentSchemaID(),
		Name:           key.GetName(),
	}]; found {
		return descpb.InvalidID
	}
	if e := sc.nameIndex.GetByName(key.GetParentID(), key.GetParentSchemaID(), key.GetName()); e != nil {
		return e.GetID()
	}
	// If we're looking up a schema name, find it in the database if we have it.
	if key.GetParentID() != descpb.InvalidID && key.GetParentSchemaID() == descpb.InvalidID {
		if parentDesc := sc.GetCachedByID(key.GetParentID()); parentDesc != nil {
			if db, ok := parentDesc.(catalog.DatabaseDescriptor); ok {
				return db.GetSchemaID(key.GetName())
			}
		}
	}
	return descpb.InvalidID
}

// EnsureAllDescriptors ensures that all stored descriptors are cached.
func (sc *StoredCatalog) EnsureAllDescriptors(ctx context.Context, txn *kv.Txn) error {
	if sc.hasAllDescriptors {
		return nil
	}
	c, err := sc.ScanAll(ctx, txn)
	if err != nil {
		return err
	}

	// These ids don't have corresponding descriptors but some of them may have
	// zone configs. So need to explicitly mark them as seen.
	sc.markZoneConfigInfoSeen(keys.RootNamespaceID)
	for _, id := range keys.PseudoTableIDs {
		sc.markZoneConfigInfoSeen(descpb.ID(id))
	}

	if err := sc.ensureAll(ctx, c); err != nil {
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
	c, err := sc.ScanNamespaceForDatabases(ctx, txn)
	if err != nil {
		return err
	}
	if err = sc.ensure(ctx, c); err != nil {
		return err
	}
	var readIDs catalog.DescriptorIDSet
	_ = c.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
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

// GetAllDescriptorNamesForDatabase generates a new map catalog with namespace
// entries for all children of the requested database. It stores the result in
// the cache.
func (sc *StoredCatalog) GetAllDescriptorNamesForDatabase(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor,
) (nstree.Catalog, error) {
	if sc.allDescriptorsForDatabase == nil {
		sc.allDescriptorsForDatabase = make(map[descpb.ID]nstree.Catalog)
	}
	c, ok := sc.allDescriptorsForDatabase[db.GetID()]
	if !ok {
		var err error
		c, err = sc.ScanNamespaceForDatabaseEntries(ctx, txn, db)
		if err != nil {
			return nstree.Catalog{}, err
		}
		if err = sc.ensure(ctx, c); err != nil {
			return nstree.Catalog{}, err
		}
		sc.allDescriptorsForDatabase[db.GetID()] = c
	}
	return c, nil
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
	c, err := sc.ScanNamespaceForDatabaseSchemas(ctx, txn, db)
	if err != nil {
		return err
	}
	if err = sc.ensure(ctx, c); err != nil {
		return err
	}
	m := make(map[descpb.ID]string)
	// This is needed at least for the temp system db during restores.
	if !db.HasPublicSchemaWithDescriptor() {
		m[keys.PublicSchemaIDForBackup] = catconstants.PublicSchemaName
	}
	_ = c.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
		m[e.GetID()] = e.GetName()
		return nil
	})
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
func (sc *StoredCatalog) IsIDKnownToNotExist(id descpb.ID, parentID catid.DescID) bool {
	if !sc.hasAllDescriptors && !sc.hasAllDescriptorForDatabase(parentID) {
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
	if id := sc.GetCachedIDByName(&key); id != descpb.InvalidID {
		return id, nil
	}
	// Fall back to querying the namespace table.
	c, err := sc.GetNamespaceEntries(ctx, txn, []descpb.NameInfo{key})
	if err != nil {
		return descpb.InvalidID, err
	}
	if err = sc.ensure(ctx, c); err != nil {
		return descpb.InvalidID, err
	}
	if ne := c.LookupNamespaceEntry(key); ne != nil {
		return ne.GetID(), nil
	}
	return descpb.InvalidID, nil
}

// EnsureFromStorageByIDs actually reads a batch of descriptors from storage
// and adds them to the cache. It assumes (without checking) that they are not
// already present in the cache. It also caches descriptor metadata (e.g.
// comment) which is read together with from a same kv batch.
func (sc *StoredCatalog) EnsureFromStorageByIDs(
	ctx context.Context,
	txn *kv.Txn,
	ids catalog.DescriptorIDSet,
	descriptorType catalog.DescriptorType,
) error {
	if ids.Empty() {
		return nil
	}
	c, err := sc.GetDescriptorEntries(ctx, txn, ids.Ordered(), true /* isRequired */, descriptorType)
	if err != nil {
		return err
	}

	ids.ForEach(func(id descpb.ID) {
		sc.markZoneConfigInfoSeen(id)
	})
	if err := sc.ensureAll(ctx, c); err != nil {
		return err
	}

	return nil
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
		id := sc.GetCachedIDByName(entry)
		if id == descpb.InvalidID {
			return nil
		}
		db, err := catalog.AsDatabaseDescriptor(sc.GetCachedByID(id))
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

// RemoveFromNameIndex removes an entry from the name index.
// This needs to be done if a descriptor is to be promoted to the uncommitted
// layer which shadows this one in the descs.Collection.
func (sc *StoredCatalog) RemoveFromNameIndex(id catid.DescID) {
	e := sc.nameIndex.GetByID(id)
	if e == nil {
		return
	}
	if sc.drainedNames == nil {
		sc.drainedNames = make(map[descpb.NameInfo]struct{})
	}
	sc.drainedNames[descpb.NameInfo{
		ParentID:       e.GetParentID(),
		ParentSchemaID: e.GetParentSchemaID(),
		Name:           e.GetName(),
	}] = struct{}{}
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

func (sc *StoredCatalog) hasAllDescriptorForDatabase(id catid.DescID) bool {
	if id == 0 {
		return false
	}
	_, ok := sc.allDescriptorsForDatabase[id]
	return ok
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
		read, err := c.sc.GetDescriptorEntries(ctx, c.txn, fallbackReqs, false /* isRequired */, catalog.Any)
		if err != nil {
			return nil, err
		}

		if err := c.sc.ensureAll(ctx, read); err != nil {
			return nil, err
		}

		for j, id := range fallbackReqs {
			desc := read.LookupDescriptorEntry(id)
			if desc == nil {
				continue
			}
			if c.sc.ValidateDescriptorsOnRead() {
				if err = validate.Self(version, desc); err != nil {
					return nil, err
				}
				c.sc.UpdateValidationLevel(desc, catalog.ValidationLevelSelfOnly)
			}
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
	read, err := c.sc.GetNamespaceEntries(ctx, c.txn, reqs)
	if err != nil {
		return nil, err
	}
	if err = c.sc.ensure(ctx, read); err != nil {
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

type storedZoneConfigs struct {
	zcs     map[descpb.ID]catalog.ZoneConfig
	seenIDs catalog.DescriptorIDSet
}

func makeStoredZoneConfigs() storedZoneConfigs {
	return storedZoneConfigs{
		zcs: make(map[descpb.ID]catalog.ZoneConfig),
	}
}

// upsert needs to be called an id has been marked as seen with markSeen,
// otherwise the upsert would be a no-op. It's possible that a table is dropped,
// its zone config stays in system.zones before it's gc'ed. In this case, we
// should just ignore the zone config since we won't see the descriptor anyway.
// Maybe it's fine to put the zone config in cache anyway, but it's good to keep
// data sane.
func (szc *storedZoneConfigs) upsert(id descpb.ID, zc catalog.ZoneConfig) {
	if szc.seenIDs.Contains(id) {
		szc.zcs[id] = zc
	}
}

// markSeen should be called before upsert is called to put a zone config in
// cache.
func (szc *storedZoneConfigs) markSeen(id descpb.ID) {
	szc.seenIDs.Add(id)
}

// get returns a boolean indicating the zone config information is cached or
// not, together with a zone config (which can be nil) when it's cached.
func (szc *storedZoneConfigs) get(id descpb.ID) (zc catalog.ZoneConfig, cached bool) {
	if szc.seenIDs.Contains(id) {
		return szc.zcs[id], true
	}
	return nil, false
}
