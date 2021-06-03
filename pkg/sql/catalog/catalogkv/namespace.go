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
	key := catalogkeys.NewNameKeyComponents(parentID, parentSchemaID, name)
	log.Eventf(ctx, "looking up descriptor ID for name key %q", key)
	res, err := txn.Get(ctx, catalogkeys.EncodeNameKey(codec, key))
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
