// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// createSystemTable is a function to inject a new system table. If the table
// already exists, ths function is a no-op. If the setGlobalLocality flag is
// true this system table will be setup as a global table on multi-region clusters,
// otherwise the locality needs to be configured by the caller.
func createSystemTable(
	ctx context.Context,
	db descs.DB,
	settings *cluster.Settings,
	codec keys.SQLCodec,
	desc catalog.TableDescriptor,
	tableLocality tree.LocalityLevel,
) error {
	return db.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		dbDesc, err := txn.Descriptors().ByIDWithoutLeased(txn.KV()).Get().Database(ctx, keys.SystemDatabaseID)
		if err != nil {
			return err
		}
		// For locality by row extra work is needed, and we will leave it to the caller.
		if tableLocality == tree.LocalityLevelRow {
			return errors.AssertionFailedf("only global and by region are implemented.")
		}
		if dbDesc.IsMultiRegion() {
			primaryRegion, err := dbDesc.PrimaryRegionName()
			if err != nil {
				return err
			}
			tableDescBuilder := tabledesc.NewBuilder(desc.TableDesc())
			mutableDesc := tableDescBuilder.BuildExistingMutableTable()
			if tableLocality == tree.LocalityLevelGlobal {
				mutableDesc.SetTableLocalityGlobal()
			} else {
				// Locality by table.
				mutableDesc.SetTableLocalityRegionalByTable(tree.Name(primaryRegion))
			}
			desc = mutableDesc
		}
		_, _, err = CreateSystemTableInTxn(ctx, settings, txn.KV(), codec, desc)
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
		b.CPut(codec.SequenceKey(uint32(desc.GetID())), desc.GetSequenceOpts().Start, nil /* expValue */)
	}
	if err := txn.Run(ctx, b); err != nil {
		return descpb.InvalidID, false, err
	}
	return desc.GetID(), true, nil
}
