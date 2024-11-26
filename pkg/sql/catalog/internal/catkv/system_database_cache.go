// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catkv

import (
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// SystemDatabaseCache is used to cache the system descriptor IDs.
// We get to assume that for a given name and a given cluster version, the name
// to ID mapping will never change for the life of process.
// This is helpful because unlike other descriptors, we can't always leverage
// the lease manager to cache all system table IDs.
//
// Note that scoping the cache by active version might not, in most cases, be
// necessary. It only comes into use if we rename or drop a system object during
// a cluster version upgrade migration. This possibly has never happened yet,
// but it's safer to be a bit defensive here to prevent bugs when if one day it
// does happen, especially because said bugs would manifest themselves only in a
// mixed-version cluster, which presently are notably under-tested.
type SystemDatabaseCache struct {
	mu struct {
		syncutil.RWMutex
		m map[roachpb.Version]*nstree.MutableCatalog
	}
}

// NewSystemDatabaseCache constructs a new instance of SystemDatabaseCache.
func NewSystemDatabaseCache(codec keys.SQLCodec, settings *cluster.Settings) *SystemDatabaseCache {
	c := &SystemDatabaseCache{}
	c.mu.m = make(map[roachpb.Version]*nstree.MutableCatalog)
	// Warm the cache for the binary version with namespace entries for IDs in the
	// reserved range from the bootstrapped schema, which are known to be valid.
	ms := bootstrap.MakeMetadataSchema(
		codec,
		zonepb.DefaultZoneConfigRef(),
		zonepb.DefaultSystemZoneConfigRef(),
	)
	var warm nstree.MutableCatalog
	_ = ms.ForEachCatalogDescriptor(func(desc catalog.Descriptor) error {
		if desc.GetID() < keys.MaxReservedDescID && !desc.SkipNamespace() {
			key := descpb.NameInfo{
				ParentID:       desc.GetParentID(),
				ParentSchemaID: desc.GetParentSchemaID(),
				Name:           desc.GetName(),
			}
			warm.UpsertNamespaceEntry(key, desc.GetID(), desc.GetModificationTime())
		}
		return nil
	})
	c.mu.m[settings.Version.LatestVersion()] = &warm
	return c
}

// lookupDescriptor looks for the corresponding descriptor entry in the cache.
func (c *SystemDatabaseCache) lookupDescriptor(
	_ clusterversion.ClusterVersion, id descpb.ID,
) catalog.Descriptor {
	switch id {
	case keys.SystemPublicSchemaID:
		return schemadesc.GetPublicSchema()
	}
	// There are not many descriptors which are known to never change.
	// There is the system database descriptor and its public schema, but these
	// cases are handled above.
	// As of today, we can't assume that there are any others.
	return nil
}

// lookupDescriptorID looks for the corresponding descriptor name -> ID entry in
// the cache.
func (c *SystemDatabaseCache) lookupDescriptorID(
	version clusterversion.ClusterVersion, key descpb.NameInfo,
) (descpb.ID, hlc.Timestamp) {
	if key.ParentID == descpb.InvalidID &&
		key.ParentSchemaID == descpb.InvalidID &&
		key.Name == catconstants.SystemDatabaseName {
		return keys.SystemDatabaseID, hlc.Timestamp{}
	}
	if key.ParentID == keys.SystemDatabaseID &&
		key.ParentSchemaID == descpb.InvalidID &&
		key.Name == catconstants.PublicSchemaName {
		return keys.SystemPublicSchemaID, hlc.Timestamp{}
	}
	if c == nil {
		return descpb.InvalidID, hlc.Timestamp{}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if cached := c.mu.m[version.Version]; cached != nil {
		if e := cached.LookupNamespaceEntry(key); e != nil {
			return e.GetID(), e.GetMVCCTimestamp()
		}
	}
	return descpb.InvalidID, hlc.Timestamp{}
}

// update the cache for the specified version with a collection of descriptors
// and namespace entries.
//
// Anything not pertaining to the system database is ignored. Furthermore,
// descriptors themselves are also completely ignored, see lookupDescriptor as
// to why that is the case. Effectively, we only add system namespace entries to
// the cache.
func (c *SystemDatabaseCache) update(version clusterversion.ClusterVersion, in nstree.Catalog) {
	if c == nil {
		return
	}
	nameCandidates := c.nameCandidatesForUpdate(version, in)
	if len(nameCandidates) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cached := c.mu.m[version.Version]
	if cached == nil {
		cached = &nstree.MutableCatalog{}
		c.mu.m[version.Version] = cached
	}
	for _, e := range nameCandidates {
		cached.UpsertNamespaceEntry(e, e.GetID(), e.GetMVCCTimestamp())
	}
}

// nameCandidatesForUpdate is a helper function for update which returns a
// subset of namespace entries in the given nstree.Catalog which could
// potentially be upserted into the cache.
// The purpose of this function, which only requires a read-lock, is to prevent
// superfluous acquisitions of the write-lock: most of the time we expect there
// to be no updates.
func (c *SystemDatabaseCache) nameCandidatesForUpdate(
	version clusterversion.ClusterVersion, in nstree.Catalog,
) []nstree.NamespaceEntry {
	if c == nil {
		// This should never happen, when c is nil this function should never
		// even be called.
		return nil
	}
	var systemNames []nstree.NamespaceEntry
	_ = in.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
		if e.GetParentID() == keys.SystemDatabaseID {
			systemNames = append(systemNames, e)
		}
		return nil
	})
	// Exit early if the input does not include any system names.
	if len(systemNames) == 0 {
		return nil
	}
	// Return the difference of the set of system names in the input with the set
	// of names presently in the cache.
	c.mu.RLock()
	defer c.mu.RUnlock()
	cached := c.mu.m[version.Version]
	if cached == nil {
		return systemNames
	}
	diff := make([]nstree.NamespaceEntry, 0, len(systemNames))
	for _, e := range systemNames {
		if cached.LookupNamespaceEntry(catalog.MakeNameInfo(e)) == nil {
			diff = append(diff, e)
		}
	}
	return diff
}
