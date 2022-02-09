// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descidgen"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
)

// CreateSystemTable is a function to inject a new system table. If the table
// already exists, ths function is a no-op.
func createSystemTable(
	ctx context.Context, db *kv.DB, codec keys.SQLCodec, desc catalog.TableDescriptor,
) error {
	// We install the table at the KV layer so that we have tighter control over the
	// placement and fewer deep dependencies. Most new system table descriptors will
	// have dynamically allocated IDs. This method supports that, but also continues
	// to support adding fixed ID system table descriptors.
	return db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		tKey := catalogkeys.EncodeNameKey(codec, desc)

		// If this descriptor doesn't have an ID, which happens for dynamic
		// system tables, we need to allocate it an ID.
		if desc.GetID() == descpb.InvalidID {
			// If we're going to allocate an ID, make sure the table does not exist.
			got, err := txn.Get(ctx, tKey)
			if err != nil {
				return err
			}
			if got.Value.IsPresent() {
				return nil
			}
			id, err := descidgen.GenerateUniqueDescID(ctx, db, codec)
			if err != nil {
				return err
			}
			mut := desc.NewBuilder().BuildCreatedMutable().(*tabledesc.Mutable)
			mut.ID = id
			desc = mut
		}

		b := txn.NewBatch()
		b.CPut(tKey, desc.GetID(), nil)
		b.CPut(catalogkeys.MakeDescMetadataKey(codec, desc.GetID()), desc.DescriptorProto(), nil)
		return txn.Run(ctx, b)
	})
}
