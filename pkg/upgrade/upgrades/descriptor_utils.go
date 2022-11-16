// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descidgen"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
)

// createSystemTable is a function to inject a new system table. If the table
// already exists, ths function is a no-op.
func createSystemTable(
	ctx context.Context,
	db *kv.DB,
	settings *cluster.Settings,
	codec keys.SQLCodec,
	desc catalog.TableDescriptor,
) error {
	return db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_, _, err := CreateSystemTableInTxn(ctx, settings, txn, codec, desc)
		return err
	})
}

// CreateSystemTableInTxn is a function to inject a new system table. If the table
// already exists, ths function is a no-op.
func CreateSystemTableInTxn(
	ctx context.Context,
	settings *cluster.Settings,
	txn *kv.Txn,
	codec keys.SQLCodec,
	desc catalog.TableDescriptor,
) (descpb.ID, bool, error) {
	// We install the table at the KV layer so that we have tighter control over the
	// placement and fewer deep dependencies. Most new system table descriptors will
	// have dynamically allocated IDs. This method supports that, but also continues
	// to support adding fixed ID system table descriptors.
	tKey := catalogkeys.EncodeNameKey(codec, desc)
	// If we're going to allocate an ID, make sure the table does not exist.
	got, err := txn.Get(ctx, tKey)
	if err != nil {
		return descpb.InvalidID, false, err
	}
	if got.Value.IsPresent() {
		return descpb.InvalidID, false, nil
	}
	if desc.GetID() == descpb.InvalidID {
		id, err := descidgen.NewTransactionalGenerator(settings, codec, txn).
			GenerateUniqueDescID(ctx)
		if err != nil {
			return descpb.InvalidID, false, err
		}
		mut := desc.NewBuilder().BuildCreatedMutable().(*tabledesc.Mutable)
		mut.ID = id
		desc = mut
	}

	b := txn.NewBatch()
	b.CPut(tKey, desc.GetID(), nil)
	b.CPut(catalogkeys.MakeDescMetadataKey(codec, desc.GetID()), desc.DescriptorProto(), nil)
	if desc.IsSequence() {
		b.InitPut(codec.SequenceKey(uint32(desc.GetID())), desc.GetSequenceOpts().Start, false /* failOnTombstones */)
	}
	if err := txn.Run(ctx, b); err != nil {
		return descpb.InvalidID, false, err
	}
	return desc.GetID(), true, nil
}
