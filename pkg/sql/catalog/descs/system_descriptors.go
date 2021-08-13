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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
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
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec,
) error {
	nc.Lock()
	defer nc.Unlock()
	begin := catalogkeys.NewNameKeyComponents(keys.SystemDatabaseID, keys.RootNamespaceID, "")
	end := catalogkeys.NewNameKeyComponents(keys.SystemDatabaseID+1, keys.RootNamespaceID, "")
	const maxRows = 0
	kvs, err := txn.Scan(ctx, catalogkeys.EncodeNameKey(codec, begin), catalogkeys.EncodeNameKey(codec, end), maxRows)
	if err != nil {
		return err
	}
	ids := catalog.DescriptorIDSet{}
	m := make(map[descpb.NameInfo]descpb.ID, len(kvs))
	for _, kv := range kvs {
		k, err := catalogkeys.DecodeNameMetadataKey(codec, kv.Key)
		if err != nil {
			return err
		}
		if k.ParentID == keys.SystemDatabaseID {
			id := descpb.ID(kv.ValueInt())
			m[k] = id
			ids.Add(id)
		}
	}
	// Check that the namespace table is not empty.
	// This can legitimately happen during startup of a bootstrapped cluster.
	// In this case we fall back to the bootstrapped schema.
	if len(m) == 0 {
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
	}
	nc.ns = m
	return nil
}

var systemNamespace, tenantNamespace systemDatabaseNamespaceCache

// lookupSystemDatabaseNamespaceCache looks for the corresponding namespace
// entry in the cache. If the cache is empty, it scans the namespace table for
// the name keys with the system database as parent and populates the cache with
// the results.
// Namespace table entries looked up in this way are therefore read from storage
// at most once per node.
func lookupSystemDatabaseNamespaceCache(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, parentSchemaID descpb.ID, name string,
) (descpb.ID, error) {
	for _, d := range systemschema.UnleasableSystemDescriptors {
		if parentSchemaID == d.GetParentSchemaID() && name == d.GetName() {
			return d.GetID(), nil
		}
	}
	nc := &tenantNamespace
	if codec.ForSystemTenant() {
		nc = &systemNamespace
	}
	wasOptimismWarranted, id := nc.optimisticLookup(parentSchemaID, name)
	if wasOptimismWarranted {
		return id, nil
	}
	err := nc.populateAll(ctx, txn, codec)
	if err != nil {
		return descpb.InvalidID, err
	}
	_, id = nc.optimisticLookup(parentSchemaID, name)
	return id, nil
}
