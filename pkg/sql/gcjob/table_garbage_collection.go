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

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// gcTables drops the table data and descriptor of tables that have an expired
// deadline and updates the job details to mark the work it did.
// The job progress is updated in place, but needs to be persisted to the job.
func gcTables(
	ctx context.Context, execCfg *sql.ExecutorConfig, progress *jobspb.SchemaChangeGCProgress,
) (bool, error) {
	didGC := false
	if log.V(2) {
		log.Infof(ctx, "GC is being considered for tables: %+v", progress.Tables)
	}
	for _, droppedTable := range progress.Tables {
		if droppedTable.Status != jobspb.SchemaChangeGCProgress_DELETING {
			// Table is not ready to be dropped, or has already been dropped.
			continue
		}

		var table *sqlbase.ImmutableTableDescriptor
		if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			var err error
			table, err = catalogkv.MustGetTableDescByID(ctx, txn, execCfg.Codec, droppedTable.ID)
			return err
		}); err != nil {
			if errors.Is(err, sqlbase.ErrDescriptorNotFound) {
				// This can happen if another GC job created for the same table got to
				// the table first. See #50344.
				log.Warningf(ctx, "table descriptor %d not found while attempting to GC, skipping", droppedTable.ID)
				// Update the details payload to indicate that the table was dropped.
				markTableGCed(ctx, droppedTable.ID, progress)
				didGC = true
				continue
			}
			return false, errors.Wrapf(err, "fetching table %d", droppedTable.ID)
		}

		if !table.Dropped() {
			// We shouldn't drop this table yet.
			continue
		}

		// First, delete all the table data.
		if err := clearTableData(ctx, execCfg.DB, execCfg.DistSender, execCfg.Codec, table); err != nil {
			return false, errors.Wrapf(err, "clearing data for table %d", table.ID)
		}

		// Finished deleting all the table data, now delete the table meta data.
		if err := dropTableDesc(ctx, execCfg.DB, execCfg.Codec, table); err != nil {
			return false, errors.Wrapf(err, "dropping table descriptor for table %d", table.ID)
		}

		// Update the details payload to indicate that the table was dropped.
		markTableGCed(ctx, table.ID, progress)
		didGC = true
	}
	return didGC, nil
}

// clearTableData deletes all of the data in the specified table.
func clearTableData(
	ctx context.Context,
	db *kv.DB,
	distSender *kvcoord.DistSender,
	codec keys.SQLCodec,
	table *sqlbase.ImmutableTableDescriptor,
) error {
	// If DropTime isn't set, assume this drop request is from a version
	// 1.1 server and invoke legacy code that uses DeleteRange and range GC.
	// TODO(pbardea): Note that we never set the drop time for interleaved tables,
	// but this check was added to be more explicit about it. This should get
	// cleaned up.
	if table.DropTime == 0 || table.IsInterleaved() {
		log.Infof(ctx, "clearing data in chunks for table %d", table.ID)
		return sql.ClearTableDataInChunks(ctx, db, codec, table, false /* traceKV */)
	}
	log.Infof(ctx, "clearing data for table %d", table.ID)

	tableSpan := table.TableSpan(codec)

	log.VEventf(ctx, 2, "ClearRange %s - %s", tableSpan.Key, tableSpan.EndKey)
	var b kv.Batch
	b.AddRawRequest(&roachpb.ClearRangeRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    tableSpan.Key,
			EndKey: tableSpan.EndKey,
		},
	})
	return db.Run(ctx, &b)
}
