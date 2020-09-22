// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gcjob

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func updateDescriptorGCMutations(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	tableID descpb.ID,
	garbageCollectedIndexID descpb.IndexID,
) error {
	log.Infof(ctx, "updating GCMutations for table %d after removing index %d",
		tableID, garbageCollectedIndexID)
	// Remove the mutation from the table descriptor.
	return descs.Txn(
		ctx, execCfg.Settings, execCfg.LeaseManager, execCfg.InternalExecutor,
		execCfg.DB, func(
			ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
		) error {
			tbl, err := descsCol.GetMutableTableVersionByID(ctx, tableID, txn)
			if err != nil {
				return err
			}
			for i := 0; i < len(tbl.GCMutations); i++ {
				other := tbl.GCMutations[i]
				if other.IndexID == garbageCollectedIndexID {
					tbl.GCMutations = append(tbl.GCMutations[:i], tbl.GCMutations[i+1:]...)
					break
				}
			}
			b := txn.NewBatch()
			if err := descsCol.WriteDescToBatch(ctx, false /* kvTrace */, tbl, b); err != nil {
				return err
			}
			return txn.Run(ctx, b)
		})
}

// dropTableDesc removes a descriptor from the KV database.
func dropTableDesc(
	ctx context.Context, db *kv.DB, codec keys.SQLCodec, tableDesc *tabledesc.Immutable,
) error {
	log.Infof(ctx, "removing table descriptor for table %d", tableDesc.ID)
	return db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.SetSystemConfigTrigger(codec.ForSystemTenant()); err != nil {
			return err
		}
		b := &kv.Batch{}

		// Delete the descriptor.
		descKey := catalogkeys.MakeDescMetadataKey(codec, tableDesc.ID)
		b.Del(descKey)
		// Delete the zone config entry for this table, if necessary.
		if codec.ForSystemTenant() {
			zoneKeyPrefix := config.MakeZoneKeyPrefix(config.SystemTenantObjectID(tableDesc.ID))
			b.DelRange(zoneKeyPrefix, zoneKeyPrefix.PrefixEnd(), false /* returnKeys */)
		}
		return txn.Run(ctx, b)
	})
}

// deleteDatabaseZoneConfig removes the zone config for a given database ID.
func deleteDatabaseZoneConfig(
	ctx context.Context, db *kv.DB, codec keys.SQLCodec, databaseID descpb.ID,
) error {
	if databaseID == descpb.InvalidID {
		return nil
	}
	if !codec.ForSystemTenant() {
		// Secondary tenants do not have zone configs for individual objects.
		return nil
	}
	return db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.SetSystemConfigTrigger(true /* forSystemTenant */); err != nil {
			return err
		}
		b := &kv.Batch{}

		// Delete the zone config entry for the dropped database associated with the
		// job, if it exists.
		dbZoneKeyPrefix := config.MakeZoneKeyPrefix(config.SystemTenantObjectID(databaseID))
		b.DelRange(dbZoneKeyPrefix, dbZoneKeyPrefix.PrefixEnd(), false /* returnKeys */)
		return txn.Run(ctx, b)
	})
}
