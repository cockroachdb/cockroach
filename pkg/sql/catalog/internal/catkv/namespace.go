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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
)

// LookupIDs returns the IDs of the descriptor for the requested namespace table
// row keys.
func LookupIDs(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, nameInfos []descpb.NameInfo,
) ([]descpb.ID, error) {
	return lookupIDs(ctx, txn, catalogQuerier{codec: codec}, nameInfos)
}

// LookupID is like LookupIDs but for one record.
func LookupID(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	parentID descpb.ID,
	parentSchemaID descpb.ID,
	name string,
) (bool, descpb.ID, error) {
	nameInfo := descpb.NameInfo{ParentID: parentID, ParentSchemaID: parentSchemaID, Name: name}
	ids, err := LookupIDs(ctx, txn, codec, []descpb.NameInfo{nameInfo})
	if err != nil || ids[0] == descpb.InvalidID {
		return false, descpb.InvalidID, err
	}
	return true, ids[0], nil
}

// GetAllDatabaseDescriptorIDs looks up and returns all available database
// descriptor IDs.
func GetAllDatabaseDescriptorIDs(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec,
) (nstree.Catalog, error) {
	cq := catalogQuerier{codec: codec}
	return cq.query(ctx, txn, func(codec keys.SQLCodec, b *kv.Batch) {
		b.Header.MaxSpanRequestKeys = 0
		prefix := catalogkeys.MakeDatabaseNameKey(codec, "")
		b.Scan(prefix, prefix.PrefixEnd())
	})
}
