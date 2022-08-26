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
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// SystemDatabaseCache is used to cache the system descriptor IDs.
// We get to assume that for a given name and a given cluster version, the name
// to ID mapping will never change for the life of process.
// This is helpful because unlike other descriptors, we can't always leverage
// the lease manager to cache all system table IDs.
type SystemDatabaseCache struct {
	syncutil.RWMutex
	m map[roachpb.Version]*nstree.MutableCatalog
}

// NewSystemDatabaseCache constructs a new instance of SystemDatabaseCache.
func NewSystemDatabaseCache(codec keys.SQLCodec, settings *cluster.Settings) *SystemDatabaseCache {
	c := &SystemDatabaseCache{
		m: make(map[roachpb.Version]*nstree.MutableCatalog),
	}
	// Warm the cache for the binary version with namespace entries for IDs in the
	// reserved range from the bootstrapped schema, which are known to be valid.
	ms := bootstrap.MakeMetadataSchema(
		codec,
		zonepb.DefaultZoneConfigRef(),
		zonepb.DefaultSystemZoneConfigRef(),
	)
	var warm nstree.MutableCatalog
	_ = ms.ForEachCatalogDescriptor(func(desc catalog.Descriptor) error {
		if desc.GetID() < keys.MaxReservedDescID {
			key := descpb.NameInfo{
				ParentID:       desc.GetParentID(),
				ParentSchemaID: desc.GetParentSchemaID(),
				Name:           desc.GetName(),
			}
			warm.UpsertNamespaceEntry(key, desc.GetID())
		}
		return nil
	})
	c.m[settings.Version.BinaryVersion()] = &warm
	return c
}

// lookupDescriptor looks for the corresponding descriptor entry in the cache.
func (c *SystemDatabaseCache) lookupDescriptor(
	_ clusterversion.ClusterVersion, id descpb.ID,
) catalog.Descriptor {
	if id == keys.SystemDatabaseID {
		return systemschema.SystemDB
	}
	// There are not many descriptors which are known to never change. For a start
	// any of the system tables may be referenced in views, which would add
	// back-references to their descriptor.
	return nil
}

// lookupDescriptorID looks for the corresponding descriptor name -> ID entry in
// the cache.
func (c *SystemDatabaseCache) lookupDescriptorID(
	version clusterversion.ClusterVersion, key catalog.NameKey,
) descpb.ID {
	if key.GetParentID() == descpb.InvalidID &&
		key.GetParentSchemaID() == 0 &&
		key.GetName() == catconstants.SystemDatabaseName {
		return keys.SystemDatabaseID
	}
	if c == nil {
		return descpb.InvalidID
	}
	c.RLock()
	defer c.RUnlock()
	if cat := c.m[version.Version]; cat != nil {
		if e := cat.LookupNamespaceEntry(key); e != nil {
			return e.GetID()
		}
	}
	return descpb.InvalidID
}

// update the cache for the specified version with a collection of descriptors
// and namespace entries.
//
// Anything not pertaining to the system database is ignored.
// Presently, system descriptors are all ignored and only namespace entries are
// used.
func (c *SystemDatabaseCache) update(version clusterversion.ClusterVersion, in nstree.Catalog) {
	if c == nil {
		return
	}
	nameCandidates := c.updateCandidates(version, in)
	c.Lock()
	defer c.Unlock()
	cat, ok := c.m[version.Version]
	if !ok {
		cat = &nstree.MutableCatalog{}
		c.m[version.Version] = cat
	}
	for _, e := range nameCandidates {
		cat.UpsertNamespaceEntry(e, e.GetID())
	}
}

func (c *SystemDatabaseCache) updateCandidates(
	version clusterversion.ClusterVersion, in nstree.Catalog,
) (nameCandidates []catalog.NameEntry) {
	if c == nil {
		return nil
	}
	c.RLock()
	defer c.RUnlock()
	cat := c.m[version.Version]
	_ = in.ForEachNamespaceEntry(func(e catalog.NameEntry) error {
		if e.GetParentID() != keys.SystemDatabaseID {
			return nil
		}
		if cat == nil || cat.LookupNamespaceEntry(e) == nil {
			nameCandidates = append(nameCandidates, e)
		}
		return nil
	})
	return nameCandidates
}
