// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catkv

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// cachedCatalogReader is a CatalogReader implementation which aggressively
// caches all catalog query results.
type cachedCatalogReader struct {

	// cr is the CatalogReader which is delegated to for cache misses.
	cr CatalogReader

	// cache contains the cached catalog.
	cache nstree.MutableCatalog

	// by*State contain further caches and other state.
	byIDState   map[descpb.ID]byIDStateValue
	byNameState map[descpb.NameInfo]byNameStateValue

	// has* indicates previously completed lookups. When set, we
	// know the corresponding catalog data is in the cache.
	hasScanAll                   bool
	hasScanAllComments           bool
	hasScanNamespaceForDatabases bool

	// memAcc is the actual account of an injected, upstream monitor
	// to track memory usage of cachedCatalogReader.
	memAcc *mon.BoundAccount

	// systemDatabaseCache is a per-node cache of system database catalog
	// information. Its presence is entirely optional and only serves to
	// eliminate superfluous round trips to KV.
	systemDatabaseCache *SystemDatabaseCache

	// version only needs to be set when systemDatabaseCache is set.
	version clusterversion.ClusterVersion
}

type byIDStateValue struct {
	// These flags indicate previously completed lookups scoped to
	// this descriptor ID.
	hasScanNamespaceForDatabaseEntries bool
	hasScanNamespaceForDatabaseSchemas bool
	hasGetDescriptorEntries            bool
}

type byNameStateValue struct {
	// These flags indicate previously completed lookups scoped to
	// this name key.
	hasGetNamespaceEntries bool
}

// NewCatalogReader is the constructor for the default CatalogReader
// implementation, which is cached.
func NewCatalogReader(
	codec keys.SQLCodec,
	version clusterversion.ClusterVersion,
	systemDatabaseCache *SystemDatabaseCache,
	maybeMonitor *mon.BytesMonitor,
) CatalogReader {
	ccr := &cachedCatalogReader{
		cr:                  &catalogReader{codec: codec},
		version:             version,
		systemDatabaseCache: systemDatabaseCache,
	}
	if maybeMonitor != nil {
		memAcc := maybeMonitor.MakeBoundAccount()
		ccr.memAcc = &memAcc
	}
	return ccr
}

var _ CatalogReader = &cachedCatalogReader{}

// Codec is part of the CatalogReader interface.
func (c *cachedCatalogReader) Codec() keys.SQLCodec {
	return c.cr.Codec()
}

// Cache is part of the CatalogReader interface.
func (c *cachedCatalogReader) Cache() nstree.Catalog {
	return c.cache.Catalog
}

// IsIDInCache is part of the CatalogReader interface.
func (c *cachedCatalogReader) IsIDInCache(id descpb.ID) bool {
	return c.byIDState[id].hasGetDescriptorEntries
}

// IsNameInCache is part of the CatalogReader interface.
func (c *cachedCatalogReader) IsNameInCache(key descpb.NameInfo) bool {
	return c.cache.LookupNamespaceEntry(key) != nil
}

// IsDescIDKnownToNotExist is part of the CatalogReader interface.
func (c *cachedCatalogReader) IsDescIDKnownToNotExist(id, maybeParentID descpb.ID) bool {
	if c.cache.LookupDescriptor(id) != nil {
		return false
	}
	if c.hasScanAll {
		return true
	}
	if c.byIDState[maybeParentID].hasScanNamespaceForDatabaseEntries {
		var found bool
		_ = c.cache.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
			if e.GetParentID() == maybeParentID && e.GetID() == id {
				found = true
				return iterutil.StopIteration()
			}
			return nil
		})
		return !found
	}
	return false
}

// Reset is part of the CatalogReader interface.
func (c *cachedCatalogReader) Reset(ctx context.Context) {
	c.cr.Reset(ctx)
	c.cache.Clear()
	if c.memAcc != nil {
		c.memAcc.Clear(ctx)
	}
	old := *c
	*c = cachedCatalogReader{
		cr:                  old.cr,
		memAcc:              old.memAcc,
		systemDatabaseCache: old.systemDatabaseCache,
		version:             old.version,
	}
}

// ScanAllComments is part of the CatalogReader interface.
func (c *cachedCatalogReader) ScanAllComments(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor,
) (nstree.Catalog, error) {
	if c.hasScanAllComments {
		return c.cache.Catalog, nil
	}
	// Scan all catalog comments.
	read, err := c.cr.ScanAllComments(ctx, txn, db)
	if err != nil {
		return nstree.Catalog{}, err
	}
	// We don't wipe out anything we already read when
	// updating the cache. So add the comments in and then
	// add back any descriptors + comments we read earlier.
	mergedCatalog := nstree.MutableCatalog{}
	mergedCatalog.AddAll(read)
	mergedCatalog.AddAll(c.cache.Catalog)
	if err := c.ensure(ctx, mergedCatalog.Catalog); err != nil {
		return nstree.Catalog{}, err
	}
	c.hasScanAllComments = true
	return read, nil
}

// ScanAll is part of the CatalogReader interface.
func (c *cachedCatalogReader) ScanAll(ctx context.Context, txn *kv.Txn) (nstree.Catalog, error) {
	if c.hasScanAll {
		return c.cache.Catalog, nil
	}
	// These ids don't have corresponding descriptors but some of them may have
	// zone configs.
	{
		s := byIDStateValue{hasGetDescriptorEntries: true}
		c.setByIDState(keys.RootNamespaceID, s)
		for _, id := range keys.PseudoTableIDs {
			c.setByIDState(descpb.ID(id), s)
		}
	}
	// Scan all catalog tables.
	read, err := c.cr.ScanAll(ctx, txn)
	if err != nil {
		return nstree.Catalog{}, err
	}
	if err := c.ensure(ctx, read); err != nil {
		return nstree.Catalog{}, err
	}
	c.hasScanAll = true
	c.hasScanNamespaceForDatabases = true
	c.hasScanAllComments = true
	if err := read.ForEachDescriptor(func(desc catalog.Descriptor) error {
		// We must update the byID and byName states for each descriptor that
		// was read.
		var idState byIDStateValue
		var nameState byNameStateValue
		idState.hasScanNamespaceForDatabaseEntries = true
		idState.hasScanNamespaceForDatabaseSchemas = true
		idState.hasGetDescriptorEntries = true
		nameState.hasGetNamespaceEntries = true
		c.setByIDState(desc.GetID(), idState)
		ni := descpb.NameInfo{
			ParentID: desc.GetParentID(),
			Name:     desc.GetName(),
		}
		if typ := desc.DescriptorType(); typ != catalog.Database && typ != catalog.Schema {
			ni.ParentSchemaID = desc.GetParentSchemaID()
		}
		c.setByNameState(ni, nameState)
		return nil
	}); err != nil {
		return nstree.Catalog{}, err
	}
	return read, nil
}

// ScanDescriptorsInSpans is part of the CatalogReader interface.
func (c *cachedCatalogReader) ScanDescriptorsInSpans(
	ctx context.Context, txn *kv.Txn, spans []roachpb.Span,
) (nstree.Catalog, error) {
	// TODO (brian.dillmann@): explore caching these calls.
	// https://github.com/cockroachdb/cockroach/issues/134666
	return c.cr.ScanDescriptorsInSpans(ctx, txn, spans)
}

// ScanNamespaceForDatabases is part of the CatalogReader interface.
func (c *cachedCatalogReader) ScanNamespaceForDatabases(
	ctx context.Context, txn *kv.Txn,
) (nstree.Catalog, error) {
	if c.hasScanNamespaceForDatabases {
		var mc nstree.MutableCatalog
		_ = c.cache.ForEachDatabaseNamespaceEntry(func(e nstree.NamespaceEntry) error {
			mc.UpsertNamespaceEntry(e, e.GetID(), e.GetMVCCTimestamp())
			return nil
		})
		return mc.Catalog, nil
	}
	read, err := c.cr.ScanNamespaceForDatabases(ctx, txn)
	if err != nil {
		return nstree.Catalog{}, err
	}
	if err := c.ensure(ctx, read); err != nil {
		return nstree.Catalog{}, err
	}
	c.hasScanNamespaceForDatabases = true
	return read, nil
}

// ScanNamespaceForDatabaseSchemas is part of the CatalogReader interface.
func (c *cachedCatalogReader) ScanNamespaceForDatabaseSchemas(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor,
) (nstree.Catalog, error) {
	s := c.byIDState[db.GetID()]
	if s.hasScanNamespaceForDatabaseSchemas {
		var mc nstree.MutableCatalog
		_ = c.cache.ForEachSchemaNamespaceEntryInDatabase(db.GetID(), func(e nstree.NamespaceEntry) error {
			mc.UpsertNamespaceEntry(e, e.GetID(), e.GetMVCCTimestamp())
			return nil
		})
		return mc.Catalog, nil
	}
	read, err := c.cr.ScanNamespaceForDatabaseSchemas(ctx, txn, db)
	if err != nil {
		return nstree.Catalog{}, err
	}
	if err := c.ensure(ctx, read); err != nil {
		return nstree.Catalog{}, err
	}
	s.hasScanNamespaceForDatabaseSchemas = true
	c.setByIDState(db.GetID(), s)
	return read, nil
}

// ScanNamespaceForDatabaseSchemasAndObjects is part of the CatalogReader
// interface.
func (c *cachedCatalogReader) ScanNamespaceForDatabaseSchemasAndObjects(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor,
) (nstree.Catalog, error) {
	s := c.byIDState[db.GetID()]
	if s.hasScanNamespaceForDatabaseEntries {
		var mc nstree.MutableCatalog
		_ = c.cache.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
			if e.GetParentID() == db.GetID() {
				mc.UpsertNamespaceEntry(e, e.GetID(), e.GetMVCCTimestamp())
			}
			return nil
		})
		return mc.Catalog, nil
	}
	read, err := c.cr.ScanNamespaceForDatabaseSchemasAndObjects(ctx, txn, db)
	if err != nil {
		return nstree.Catalog{}, err
	}
	if err := c.ensure(ctx, read); err != nil {
		return nstree.Catalog{}, err
	}
	s.hasScanNamespaceForDatabaseEntries = true
	s.hasScanNamespaceForDatabaseSchemas = true
	c.setByIDState(db.GetID(), s)
	return read, nil
}

// ScanNamespaceForSchemaObjects is part of the CatalogReader interface.
func (c *cachedCatalogReader) ScanNamespaceForSchemaObjects(
	ctx context.Context, txn *kv.Txn, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor,
) (nstree.Catalog, error) {
	s := c.byIDState[db.GetID()]
	if s.hasScanNamespaceForDatabaseEntries {
		var mc nstree.MutableCatalog
		_ = c.cache.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
			if e.GetParentID() == db.GetID() && e.GetParentSchemaID() == sc.GetID() {
				mc.UpsertNamespaceEntry(e, e.GetID(), e.GetMVCCTimestamp())
			}
			return nil
		})
		return mc.Catalog, nil
	}
	read, err := c.cr.ScanNamespaceForSchemaObjects(ctx, txn, db, sc)
	if err != nil {
		return nstree.Catalog{}, err
	}
	if err := c.ensure(ctx, read); err != nil {
		return nstree.Catalog{}, err
	}
	return read, nil
}

// GetByIDs is part of the CatalogReader interface.
func (c *cachedCatalogReader) GetByIDs(
	ctx context.Context,
	txn *kv.Txn,
	ids []descpb.ID,
	isDescriptorRequired bool,
	expectedType catalog.DescriptorType,
) (nstree.Catalog, error) {
	numUncached := 0
	// Move any uncached IDs to the front of the slice.
	for i, id := range ids {
		if c.byIDState[id].hasGetDescriptorEntries || c.hasScanAll {
			continue
		}
		if desc := c.systemDatabaseCache.lookupDescriptor(c.version, id); desc != nil {
			c.cache.UpsertDescriptor(desc)
		}
		ids[i], ids[numUncached] = ids[numUncached], id
		numUncached++
	}
	if numUncached > 0 && !(c.hasScanAll && !isDescriptorRequired) {
		uncachedIDs := ids[:numUncached]
		read, err := c.cr.GetByIDs(ctx, txn, uncachedIDs, isDescriptorRequired, expectedType)
		if err != nil {
			return nstree.Catalog{}, err
		}
		if err := c.ensure(ctx, read); err != nil {
			return nstree.Catalog{}, err
		}
		for _, id := range uncachedIDs {
			s := c.byIDState[id]
			s.hasGetDescriptorEntries = true
			c.setByIDState(id, s)
		}
	}
	ret := c.cache.FilterByIDsExclusive(ids)
	if isDescriptorRequired {
		for _, id := range ids[numUncached:] {
			if ret.LookupDescriptor(id) == nil {
				return nstree.Catalog{}, wrapError(expectedType, id, requiredError(expectedType, id))
			}
		}
	}
	return ret, nil
}

// GetByNames is part of the CatalogReader interface.
func (c *cachedCatalogReader) GetByNames(
	ctx context.Context, txn *kv.Txn, nameInfos []descpb.NameInfo,
) (nstree.Catalog, error) {
	numUncached := 0
	// Move any uncached name keys to the front of the slice.
	for i, ni := range nameInfos {
		if c.byNameState[ni].hasGetNamespaceEntries || c.hasScanAll {
			continue
		}
		if id, ts := c.systemDatabaseCache.lookupDescriptorID(c.version, ni); id != descpb.InvalidID {
			c.cache.UpsertNamespaceEntry(ni, id, ts)
			s := c.byNameState[ni]
			s.hasGetNamespaceEntries = true
			c.setByNameState(ni, s)
			continue
		}
		nameInfos[i], nameInfos[numUncached] = nameInfos[numUncached], ni
		numUncached++
	}
	if numUncached > 0 && !c.hasScanAll {
		uncachedNameInfos := nameInfos[:numUncached]
		read, err := c.cr.GetByNames(ctx, txn, uncachedNameInfos)
		if err != nil {
			return nstree.Catalog{}, err
		}
		if err := c.ensure(ctx, read); err != nil {
			return nstree.Catalog{}, err
		}
		for _, ni := range uncachedNameInfos {
			s := c.byNameState[ni]
			s.hasGetNamespaceEntries = true
			c.setByNameState(ni, s)
		}
	}
	return c.cache.FilterByNames(nameInfos), nil
}

// ensure adds descriptors, namespace, comment, zone config entries to the
// cache. This will not cause any information loss.
func (c *cachedCatalogReader) ensure(ctx context.Context, read nstree.Catalog) error {
	oldSize := c.cache.ByteSize()
	c.cache.AddAll(read)
	c.systemDatabaseCache.update(c.version, read)
	_ = read.ForEachDescriptor(func(desc catalog.Descriptor) error {
		if desc.Dropped() {
			return nil
		}
		if !desc.SkipNamespace() {
			// The descriptor is expected to have a matching namespace entry.
			c.cache.UpsertNamespaceEntry(desc, desc.GetID(), desc.GetModificationTime())
		}
		if db, ok := desc.(catalog.DatabaseDescriptor); ok {
			// Database descriptors know the name -> ID mappings of their schemas.
			_ = db.ForEachSchema(func(id descpb.ID, name string) error {
				key := descpb.NameInfo{ParentID: db.GetID(), Name: name}
				c.cache.UpsertNamespaceEntry(&key, id, desc.GetModificationTime())
				return nil
			})
		}
		return nil
	})
	if c.memAcc == nil {
		return nil
	}
	return c.memAcc.Grow(ctx, c.cache.ByteSize()-oldSize)

}

func (c *cachedCatalogReader) setByIDState(id descpb.ID, s byIDStateValue) {
	if c.byIDState == nil {
		c.byIDState = make(map[descpb.ID]byIDStateValue)
	}
	c.byIDState[id] = s
}

func (c *cachedCatalogReader) setByNameState(ni descpb.NameInfo, s byNameStateValue) {
	if c.byNameState == nil {
		c.byNameState = make(map[descpb.NameInfo]byNameStateValue)
	}
	c.byNameState[ni] = s
}
