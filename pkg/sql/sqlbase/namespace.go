// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// This file abstracts all accesses to system.namespace. Entries in
// system.namespace are never modified. We only write new entries or delete
// existing entries.
//
// As of 20.1, the older system.namespace table is marked deprecated. It is
// replaced by a new system.namespace table that has an additional parentSchemaID
// column, which allows support for additional physical schemas. The new
// namespace table is also created outside the system config range, so it is no
// longer gossiped.
//
// To ensure accesses are seamless across mixed version clusters, >= 20.1 clusters,
// and during the upgrade process, the following functions should be used
// for adding/removing entries.
// TODO(solon): The fallback semantics will no longer be required in 20.2.
// This code should be cleaned up then, to only access the new system.namespace
// table.

// Deleting entries from system.namespace:
// Entries are deleted from both the deprecated and newer system.namespace, if
// they exist in them.
// Entries may be in one/both of the tables.
// - In a mixed version (19.2/20.1) cluster, the entry only exists in the older
// system.namespace.
// - In a 20.1 cluster, if the entry was created before upgrade, the entry exists
// in both the tables.
// - In a 20.1 cluster, if the entry was created after upgrade, it exists only
// in the newer system.namespace.
//
// Adding entries to system.namespace:
// Entries are added to either the new system.namespace or the deprecated
// system.namespace, depending on the cluster version. Methods supplied by
// this file only abstract key construction based on the cluster settings.
// It is not safe to construct keys and do removals/lookups using them, as
// this can cause issues in mixed version clusters. Please use the provided
// removal/lookup methods for those cases.

// RemoveObjectNamespaceEntry removes entries from both the deprecated and
// new system.namespace table (if one exists).
func RemoveObjectNamespaceEntry(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	parentID ID,
	parentSchemaID ID,
	name string,
	KVTrace bool,
) error {
	b := txn.NewBatch()
	var toDelete []DescriptorKey
	// The (parentID, name) mapping could be in either the new system.namespace
	// or the deprecated version. Thus we try to remove the mapping from both.
	if parentID == keys.RootNamespaceID {
		toDelete = append(toDelete, NewDatabaseKey(name))
		// TODO(solon): This can be completely removed in 20.2.
		toDelete = append(toDelete, NewDeprecatedDatabaseKey(name))
	} else if parentSchemaID == keys.RootNamespaceID {
		// Schemas were introduced in 20.1.
		toDelete = append(toDelete, NewSchemaKey(parentID, name))
	} else {
		toDelete = append(toDelete, NewTableKey(parentID, parentSchemaID, name))
		// TODO(solon): This can be completely removed in 20.2.
		toDelete = append(toDelete, NewDeprecatedTableKey(parentID, name))
	}
	for _, delKey := range toDelete {
		if KVTrace {
			log.VEventf(ctx, 2, "Del %s", delKey)
		}
		b.Del(delKey.Key(codec))
	}
	return txn.Run(ctx, b)
}

// RemovePublicTableNamespaceEntry is a wrapper around RemoveObjectNamespaceEntry
// for public tables.
func RemovePublicTableNamespaceEntry(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, parentID ID, name string,
) error {
	return RemoveObjectNamespaceEntry(ctx, txn, codec, parentID, keys.PublicSchemaID, name, false /* KVTrace */)
}

// RemoveSchemaNamespaceEntry is a wrapper around RemoveObjectNamespaceEntry
// for schemas.
func RemoveSchemaNamespaceEntry(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, parentID ID, name string,
) error {
	return RemoveObjectNamespaceEntry(ctx, txn, codec, parentID, keys.RootNamespaceID, name, false /* KVTrace */)
}

// RemoveDatabaseNamespaceEntry is a wrapper around RemoveObjectNamespaceEntry
// for databases.
func RemoveDatabaseNamespaceEntry(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, name string, KVTrace bool,
) error {
	return RemoveObjectNamespaceEntry(ctx, txn, codec, keys.RootNamespaceID, keys.RootNamespaceID, name, KVTrace)
}

// MakeObjectNameKey returns a key in the system.namespace table for
// a given parentID and name, based on the cluster version.
// - If cluster version >= 20.1, the key is in the new system.namespace table.
// - If cluster version < 20.1, the key is in the deprecated system.namespace table.
// - The parentSchemaID field is ignored in < 20.1 clusters.
func MakeObjectNameKey(
	ctx context.Context, settings *cluster.Settings, parentID ID, parentSchemaID ID, name string,
) DescriptorKey {
	// TODO(solon): This if condition can be removed in 20.2
	if !settings.Version.IsActive(ctx, clusterversion.VersionNamespaceTableWithSchemas) {
		return NewDeprecatedTableKey(parentID, name)
	}
	var key DescriptorKey
	if parentID == keys.RootNamespaceID {
		key = NewDatabaseKey(name)
	} else if parentSchemaID == keys.RootNamespaceID {
		key = NewSchemaKey(parentID, name)
	} else {
		key = NewTableKey(parentID, parentSchemaID, name)
	}
	return key
}

// MakePublicTableNameKey is a wrapper around MakeObjectNameKey for public tables.
func MakePublicTableNameKey(
	ctx context.Context, settings *cluster.Settings, parentID ID, name string,
) DescriptorKey {
	return MakeObjectNameKey(ctx, settings, parentID, keys.PublicSchemaID, name)
}

// MakeDatabaseNameKey is a wrapper around MakeObjectNameKey for databases.
func MakeDatabaseNameKey(
	ctx context.Context, settings *cluster.Settings, name string,
) DescriptorKey {
	return MakeObjectNameKey(ctx, settings, keys.RootNamespaceID, keys.RootNamespaceID, name)
}

// LookupObjectID returns the ObjectID for the given
// (parentID, parentSchemaID, name) supplied. If cluster version < 20.1,
// the parentSchemaID is ignored.
func LookupObjectID(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	parentID ID,
	parentSchemaID ID,
	name string,
) (bool, ID, error) {
	var key DescriptorKey
	if parentID == keys.RootNamespaceID {
		key = NewDatabaseKey(name)
	} else if parentSchemaID == keys.RootNamespaceID {
		key = NewSchemaKey(parentID, name)
	} else {
		key = NewTableKey(parentID, parentSchemaID, name)
	}
	log.Eventf(ctx, "looking up descriptor ID for name key %q", key.Key(codec))
	res, err := txn.Get(ctx, key.Key(codec))
	if err != nil {
		return false, InvalidID, err
	}
	if res.Exists() {
		return true, ID(res.ValueInt()), nil
	}
	// If the key wasn't found in the new system.namespace table, it may still
	// exist in the deprecated system.namespace in the case of mixed version clusters.
	// TODO(solon): This can be removed in 20.2.

	// This fallback logic is only required if the table is under the public schema
	// or we are resolving a database.
	// Without this check, we can run into the following problem:
	// - Persistent table `t` was created before the cluster upgrade, so it is
	// present in both the old & new system.namespace table.
	// - A session creates a temporary table `u`, which means the session has a
	// valid temporary schema.
	// - If this session explicitly accesses `pg_temp.t`, it should fail -- but
	// without this check, `pg_temp.t` will return the permanent table instead.
	if parentSchemaID != keys.PublicSchemaID && parentSchemaID != keys.RootNamespaceID {
		return false, InvalidID, nil
	}

	var dKey DescriptorKey
	if parentID == keys.RootNamespaceID {
		dKey = NewDeprecatedDatabaseKey(name)
	} else {
		dKey = NewDeprecatedTableKey(parentID, name)
	}
	log.Eventf(ctx, "looking up descriptor ID for name key %q", dKey.Key(codec))
	res, err = txn.Get(ctx, dKey.Key(codec))
	if err != nil {
		return false, InvalidID, err
	}
	if res.Exists() {
		return true, ID(res.ValueInt()), nil
	}
	return false, InvalidID, nil
}

// LookupPublicTableID is a wrapper around LookupObjectID for public tables.
func LookupPublicTableID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, parentID ID, name string,
) (bool, ID, error) {
	return LookupObjectID(ctx, txn, codec, parentID, keys.PublicSchemaID, name)
}

// LookupDatabaseID is  a wrapper around LookupObjectID for databases.
func LookupDatabaseID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, name string,
) (bool, ID, error) {
	return LookupObjectID(ctx, txn, codec, keys.RootNamespaceID, keys.RootNamespaceID, name)
}
