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

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type systemDatabaseNamespaceCache struct {
	syncutil.RWMutex
	ns map[descpb.NameInfo]descpb.ID
}

func (nc *systemDatabaseNamespaceCache) optimisticLookup(
	parentSchemaID descpb.ID, name string,
) (wasOptimismWarranted bool, id descpb.ID) {
	nc.RLock()
	defer nc.RUnlock()
	if nc.ns == nil {
		return false, descpb.InvalidID
	}
	return true, nc.ns[descpb.NameInfo{
		ParentID:       keys.SystemDatabaseID,
		ParentSchemaID: parentSchemaID,
		Name:           name,
	}]
}

func (nc *systemDatabaseNamespaceCache) populateAll(
	ctx context.Context, codec keys.SQLCodec,
) error {
	nc.Lock()
	defer nc.Unlock()
	m := make(map[descpb.NameInfo]descpb.ID)
	ms := bootstrap.MakeMetadataSchema(
		codec,
		zonepb.DefaultZoneConfigRef(),
		zonepb.DefaultSystemZoneConfigRef(),
	)
	_ = ms.ForEachCatalogDescriptor(func(desc catalog.Descriptor) error {
		m[descpb.NameInfo{
			ParentID:       desc.GetParentID(),
			ParentSchemaID: desc.GetParentSchemaID(),
			Name:           desc.GetName(),
		}] = desc.GetID()
		return nil
	})
	nc.ns = m
	return nil
}

var systemNamespace, tenantNamespace systemDatabaseNamespaceCache

// lookupSystemDatabaseNamespaceCache looks for the corresponding namespace
// entry in the cache. If the cache is empty, it creates a bootstrap schema
// and populates the cache with the descriptors in it.
func lookupSystemDatabaseNamespaceCache(
	ctx context.Context, codec keys.SQLCodec, parentSchemaID descpb.ID, name string,
) (descpb.ID, error) {
	nc := &tenantNamespace
	if codec.ForSystemTenant() {
		nc = &systemNamespace
	}
	wasOptimismWarranted, id := nc.optimisticLookup(parentSchemaID, name)
	if wasOptimismWarranted {
		return id, nil
	}
	err := nc.populateAll(ctx, codec)
	if err != nil {
		return descpb.InvalidID, err
	}
	_, id = nc.optimisticLookup(parentSchemaID, name)
	return id, nil
}
