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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func updateDescriptorGCMutations(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	table *sqlbase.TableDescriptor,
	garbageCollectedIndexID sqlbase.IndexID,
) error {
	log.Infof(ctx, "updating GCMutations for table %d after removing index %d", table.ID, garbageCollectedIndexID)
	// Remove the mutation from the table descriptor.
	updateTableMutations := func(tbl *sqlbase.MutableTableDescriptor) error {
		for i := 0; i < len(tbl.GCMutations); i++ {
			other := tbl.GCMutations[i]
			if other.IndexID == garbageCollectedIndexID {
				tbl.GCMutations = append(tbl.GCMutations[:i], tbl.GCMutations[i+1:]...)
				break
			}
		}

		return nil
	}

	_, err := execCfg.LeaseManager.Publish(
		ctx,
		table.ID,
		updateTableMutations,
		nil, /* logEvent */
	)
	if err != nil {
		return err
	}
	return nil
}

// dropTableDesc removes a descriptor from the KV database.
func dropTableDesc(ctx context.Context, db *kv.DB, tableDesc *sqlbase.TableDescriptor) error {
	log.Infof(ctx, "removing table descriptor for table %d", tableDesc.ID)
	descKey := sqlbase.MakeDescMetadataKey(tableDesc.ID)
	zoneKeyPrefix := config.MakeZoneKeyPrefix(uint32(tableDesc.ID))

	return db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		b := &kv.Batch{}
		// Delete the descriptor.
		b.Del(descKey)
		// Delete the zone config entry for this table.
		b.DelRange(zoneKeyPrefix, zoneKeyPrefix.PrefixEnd(), false /* returnKeys */)
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}

		return txn.Run(ctx, b)
	})
}

// deleteDatabaseZoneConfig removes the zone config for a given database ID.
func deleteDatabaseZoneConfig(ctx context.Context, db *kv.DB, databaseID sqlbase.ID) error {
	return db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		b := &kv.Batch{}

		// Delete the zone config entry for the dropped database associated with the
		// job, if it exists.
		if databaseID == sqlbase.InvalidID {
			return nil
		}
		dbZoneKeyPrefix := config.MakeZoneKeyPrefix(uint32(databaseID))
		b.DelRange(dbZoneKeyPrefix, dbZoneKeyPrefix.PrefixEnd(), false /* returnKeys */)
		return txn.Run(ctx, b)
	})
}
