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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// updateDescriptorGCMutations removes the GCMutation for the given
// index ID. We no longer populate this field, but we still search it
// to remove existing entries.
func updateDescriptorGCMutations(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	tableID descpb.ID,
	garbageCollectedIndexID descpb.IndexID,
) error {
	return sql.DescsTxn(ctx, execCfg, func(
		ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
	) error {
		tbl, err := descsCol.GetMutableTableVersionByID(ctx, tableID, txn)
		if err != nil {
			return err
		}
		found := false
		for i := 0; i < len(tbl.GCMutations); i++ {
			other := tbl.GCMutations[i]
			if other.IndexID == garbageCollectedIndexID {
				tbl.GCMutations = append(tbl.GCMutations[:i], tbl.GCMutations[i+1:]...)
				found = true
				break
			}
		}
		if found {
			log.Infof(ctx, "updating GCMutations for table %d after removing index %d",
				tableID, garbageCollectedIndexID)
			// Remove the mutation from the table descriptor.
			b := txn.NewBatch()
			if err := descsCol.WriteDescToBatch(ctx, false /* kvTrace */, tbl, b); err != nil {
				return err
			}
			return txn.Run(ctx, b)
		}
		return nil
	})
}

// deleteDatabaseZoneConfig removes the zone config for a given database ID.
func deleteDatabaseZoneConfig(
	ctx context.Context,
	db *kv.DB,
	codec keys.SQLCodec,
	settings *cluster.Settings,
	databaseID descpb.ID,
) error {
	if databaseID == descpb.InvalidID {
		return nil
	}
	return db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if !settings.Version.IsActive(
			ctx, clusterversion.DisableSystemConfigGossipTrigger,
		) {
			if err := txn.DeprecatedSetSystemConfigTrigger(codec.ForSystemTenant()); err != nil {
				return err
			}
		}
		b := &kv.Batch{}

		// Delete the zone config entry for the dropped database associated with the
		// job, if it exists.
		dbZoneKeyPrefix := config.MakeZoneKeyPrefix(codec, databaseID)
		b.DelRange(dbZoneKeyPrefix, dbZoneKeyPrefix.PrefixEnd(), false /* returnKeys */)
		return txn.Run(ctx, b)
	})
}
