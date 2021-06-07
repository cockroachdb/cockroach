// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalogkv

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// This file abstracts all accesses to system.namespace. Entries in
// system.namespace are never modified. We only write new entries or delete
// existing entries.

// WriteObjectNamespaceEntryRemovalToBatch writes Del operations to b for
// both the deprecated and new system.namespace table (if one exists).
func WriteObjectNamespaceEntryRemovalToBatch(
	ctx context.Context,
	b *kv.Batch,
	codec keys.SQLCodec,
	parentID descpb.ID,
	parentSchemaID descpb.ID,
	name string,
	KVTrace bool,
) {
	delKey := MakeObjectNameKey(parentID, parentSchemaID, name)
	if KVTrace {
		log.VEventf(ctx, 2, "Del %s", delKey)
	}
	b.Del(delKey.Key(codec))
}

// RemoveObjectNamespaceEntry removes entries from both the deprecated and
// new system.namespace table (if one exists).
func RemoveObjectNamespaceEntry(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	parentID descpb.ID,
	parentSchemaID descpb.ID,
	name string,
	KVTrace bool,
) error {
	b := txn.NewBatch()
	WriteObjectNamespaceEntryRemovalToBatch(ctx, b, codec, parentID, parentSchemaID, name, KVTrace)
	return txn.Run(ctx, b)
}

// RemoveSchemaNamespaceEntry is a wrapper around RemoveObjectNamespaceEntry
// for schemas.
func RemoveSchemaNamespaceEntry(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, parentID descpb.ID, name string,
) error {
	return RemoveObjectNamespaceEntry(ctx, txn, codec, parentID, keys.RootNamespaceID, name, false /* KVTrace */)
}

// MakeObjectNameKey returns a key in the system.namespace table for
// a given parentID, parentSchemaID and name.
func MakeObjectNameKey(
	parentID descpb.ID, parentSchemaID descpb.ID, name string,
) catalogkeys.DescriptorKey {
	if parentID == keys.RootNamespaceID {
		return catalogkeys.NewDatabaseKey(name)
	}
	if parentSchemaID == keys.RootNamespaceID {
		return catalogkeys.NewSchemaKey(parentID, name)
	}
	return catalogkeys.NewTableKey(parentID, parentSchemaID, name)
}

// LookupObjectID returns the ObjectID for the given
// (parentID, parentSchemaID, name) supplied.
func LookupObjectID(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	parentID descpb.ID,
	parentSchemaID descpb.ID,
	name string,
) (bool, descpb.ID, error) {
	// Avoid a network round-trip if we accidentally ended up here without a
	// name to look up.
	if name == "" {
		return false, descpb.InvalidID, nil
	}
	key := MakeObjectNameKey(parentID, parentSchemaID, name).Key(codec)
	log.Eventf(ctx, "looking up descriptor ID for name key %q", key)
	res, err := txn.Get(ctx, key)
	if err != nil || !res.Exists() {
		return false, descpb.InvalidID, err
	}
	return true, descpb.ID(res.ValueInt()), nil
}

// LookupDatabaseID is  a wrapper around LookupObjectID for databases.
func LookupDatabaseID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, name string,
) (bool, descpb.ID, error) {
	return LookupObjectID(ctx, txn, codec, keys.RootNamespaceID, keys.RootNamespaceID, name)
}
