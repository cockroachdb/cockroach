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
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// systemDatabaseNamespaceCache is used to cache the IDs of system descriptors.
// We get to assume that for a given name, it will never change for the life of
// the process. This is helpful because unlike other descriptors, we can't
// always leverage the lease manager to cache all system table IDs.
type systemDatabaseNamespaceCache struct {
	syncutil.RWMutex
	ns map[descpb.NameInfo]descpb.ID
}

func newSystemDatabaseNamespaceCache(codec keys.SQLCodec) *systemDatabaseNamespaceCache {
	nc := &systemDatabaseNamespaceCache{}
	nc.ns = make(map[descpb.NameInfo]descpb.ID)
	ms := bootstrap.MakeMetadataSchema(
		codec,
		zonepb.DefaultZoneConfigRef(),
		zonepb.DefaultSystemZoneConfigRef(),
	)
	_ = ms.ForEachCatalogDescriptor(func(desc catalog.Descriptor) error {
		if desc.GetID() < keys.MaxReservedDescID {
			nc.ns[descpb.NameInfo{
				ParentID:       desc.GetParentID(),
				ParentSchemaID: desc.GetParentSchemaID(),
				Name:           desc.GetName(),
			}] = desc.GetID()
		}
		return nil
	})
	return nc
}

// lookupSystemDatabaseNamespaceCache looks for the corresponding namespace
// entry in the cache. If the cache is empty, it creates a bootstrap schema
// and populates the cache with the descriptors in it.
func (s *systemDatabaseNamespaceCache) lookup(schemaID descpb.ID, name string) descpb.ID {
	if s == nil {
		return descpb.InvalidID
	}
	s.RLock()
	defer s.RUnlock()
	return s.ns[descpb.NameInfo{
		ParentID:       keys.SystemDatabaseID,
		ParentSchemaID: schemaID,
		Name:           name,
	}]
}

func (s *systemDatabaseNamespaceCache) add(info descpb.NameInfo, id descpb.ID) {
	if s == nil {
		return
	}
	s.Lock()
	defer s.Unlock()
	s.ns[info] = id
}
