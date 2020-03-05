// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// dropTables drops the table data and descriptor of tables that have an expired
// deadline and updates the job details to mark the work it did.
// Note that the job details still need to be synced back to the job.
func dropTables(
	ctx context.Context,
	db *client.DB,
	distSender *kv.DistSender,
	details *jobspb.WaitingForGCDetails,
) error {
	for _, droppedTable := range details.Tables {
		var table *sqlbase.TableDescriptor
		var err error
		if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			table, err = sqlbase.GetTableDescFromID(ctx, txn, droppedTable.ID)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}

		timeUntilGC := timeutil.Unix(0, droppedTable.Deadline).Sub(timeutil.Now())
		if !table.Dropped() || timeUntilGC > 0 {
			// We shouldn't drop this table yet.
			return nil
		}

		// First, delete all the table data.
		if err := truncateTable(ctx, db, distSender, table); err != nil {
			return err
		}

		// Then, delete the table descriptor.
		if err := dropTableDesc(ctx, db, table); err != nil {
			return err
		}

		// Update the details payload to indicate that the table was dropped.
		if err := completeDropDetails(table.ID, details); err != nil {
			return err
		}
	}
	return nil
}

// completeDropDetails updates the job payload details to indicate the progress
// made.
func completeDropDetails(tableID sqlbase.ID, details *jobspb.WaitingForGCDetails) error {
	for i, tableToDrop := range details.Tables {
		if tableToDrop.ID == tableID {
			// We have dropped this table.
			details.Tables = append(details.Tables[:i], details.Tables[i+1:]...)
		}
	}
	return nil
}

// dropTableDesc removes a descriptor from the KV database.
func dropTableDesc(ctx context.Context, db *client.DB, tableDesc *sqlbase.TableDescriptor) error {
	descKey := sqlbase.MakeDescMetadataKey(tableDesc.ID)
	zoneKeyPrefix := config.MakeZoneKeyPrefix(uint32(tableDesc.ID))

	return db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		// Finished deleting all the table data, now delete the table meta data.
		// Delete table descriptor
		b := &client.Batch{}
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
func deleteDatabaseZoneConfig(ctx context.Context, db *client.DB, databaseID sqlbase.ID) error {
	return db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		// Finished deleting all the table data, now delete the table meta data.
		b := &client.Batch{}

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
