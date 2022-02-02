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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descidgen"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/errors"
)

// CreateSystemTable is a function to inject a new system table. If the table
// already exists, ths function is a no-op.
func createSystemTable(
	ctx context.Context, db *kv.DB, codec keys.SQLCodec, desc catalog.TableDescriptor,
) error {
	// We install the table at the KV layer so that we can choose a known ID in
	// the reserved ID space. (The SQL layer doesn't allow this.)
	err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		tKey := catalogkeys.MakePublicObjectNameKey(codec, desc.GetParentID(), desc.GetName())

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
			descProto := desc.DescriptorProto()
			descpb.SetID(descProto, id)
			desc = descbuilder.NewBuilder(descProto).BuildCreatedMutable().(catalog.TableDescriptor)
		}

		b := txn.NewBatch()
		b.CPut(tKey, desc.GetID(), nil)
		b.CPut(catalogkeys.MakeDescMetadataKey(codec, desc.GetID()), desc.DescriptorProto(), nil)
		if err := txn.SetSystemConfigTrigger(codec.ForSystemTenant()); err != nil {
			return err
		}
		return txn.Run(ctx, b)
	})
	// CPuts only provide idempotent inserts if we ignore the errors that arise
	// when the condition isn't met.
	if errors.HasType(err, (*roachpb.ConditionFailedError)(nil)) {
		return nil
	}
	return err
}
