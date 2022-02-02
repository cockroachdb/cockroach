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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descidgen"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// CreateSystemTable is a function to inject a new system table. If the table
// already exists, ths function is a no-op.
func createSystemTable(
	ctx context.Context, db *kv.DB, codec keys.SQLCodec, desc catalog.TableDescriptor,
) error {
	// We install the table at the KV layer so that we can choose a known ID in
	// the reserved ID space. (The SQL layer doesn't allow this.)
	return db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		tKey := catalogkeys.MakePublicObjectNameKey(codec, desc.GetParentID(), desc.GetName())

		// Make sure the table does not exist. Note that this makes the CPuts below
		// unnecessary, and it provides the idempotence. They are just defence in
		// depth.
		got, err := txn.Get(ctx, tKey)
		if err != nil {
			return err
		}
		if got.Value.IsPresent() {
			iv, err := got.Value.GetInt()
			if err != nil {
				return errors.NewAssertionErrorWithWrappedErrf(err,
					"failed to decode pre-existing ID in namespace table for system table %s",
					desc.GetName())
			}
			log.Infof(ctx, "system table %s already exists at ID %v %v", desc.GetName(), iv, err)
			return nil
		}

		// If this descriptor doesn't have an ID, which happens for dynamic
		// system tables, we need to allocate it an ID.
		if desc.GetID() == descpb.InvalidID {
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
}
