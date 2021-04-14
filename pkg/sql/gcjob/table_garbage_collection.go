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
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// gcTables drops the table data and descriptor of tables that have an expired
// deadline and updates the job details to mark the work it did.
// The job progress is updated in place, but needs to be persisted to the job.
func gcTables(
	ctx context.Context, execCfg *sql.ExecutorConfig, progress *jobspb.SchemaChangeGCProgress,
) error {
	if log.V(2) {
		log.Infof(ctx, "GC is being considered for tables: %+v", progress.Tables)
	}
	for _, droppedTable := range progress.Tables {
		if droppedTable.Status != jobspb.SchemaChangeGCProgress_DELETING {
			// Table is not ready to be dropped, or has already been dropped.
			continue
		}

		var table catalog.TableDescriptor
		if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			var err error
			table, err = catalogkv.MustGetTableDescByID(ctx, txn, execCfg.Codec, droppedTable.ID)
			return err
		}); err != nil {
			if errors.Is(err, catalog.ErrDescriptorNotFound) {
				// This can happen if another GC job created for the same table got to
				// the table first. See #50344.
				log.Warningf(ctx, "table descriptor %d not found while attempting to GC, skipping", droppedTable.ID)
				// Update the details payload to indicate that the table was dropped.
				markTableGCed(ctx, droppedTable.ID, progress)
				continue
			}
			return errors.Wrapf(err, "fetching table %d", droppedTable.ID)
		}

		if !table.Dropped() {
			// We shouldn't drop this table yet.
			continue
		}

		// First, delete all the table data.
		if err := ClearTableData(ctx, execCfg.DB, execCfg.DistSender, execCfg.Codec, table); err != nil {
			return errors.Wrapf(err, "clearing data for table %d", table.GetID())
		}

		// Finished deleting all the table data, now delete the table meta data.
		if err := sql.DeleteTableDescAndZoneConfig(ctx, execCfg.DB, execCfg.Codec, table); err != nil {
			return errors.Wrapf(err, "dropping table descriptor for table %d", table.GetID())
		}

		// Update the details payload to indicate that the table was dropped.
		markTableGCed(ctx, table.GetID(), progress)
	}
	return nil
}

// ClearTableData deletes all of the data in the specified table.
func ClearTableData(
	ctx context.Context,
	db *kv.DB,
	distSender *kvcoord.DistSender,
	codec keys.SQLCodec,
	table catalog.TableDescriptor,
) error {
	// If DropTime isn't set, assume this drop request is from a version
	// 1.1 server and invoke legacy code that uses DeleteRange and range GC.
	// TODO(pbardea): Note that we never set the drop time for interleaved tables,
	// but this check was added to be more explicit about it. This should get
	// cleaned up.
	if table.GetDropTime() == 0 || table.IsInterleaved() {
		log.Infof(ctx, "clearing data in chunks for table %d", table.GetID())
		return sql.ClearTableDataInChunks(ctx, db, codec, table, false /* traceKV */)
	}
	log.Infof(ctx, "clearing data for table %d", table.GetID())

	tableKey := roachpb.RKey(codec.TablePrefix(uint32(table.GetID())))
	tableSpan := roachpb.RSpan{Key: tableKey, EndKey: tableKey.PrefixEnd()}
	return clearSpanData(ctx, db, distSender, tableSpan)
}

func clearSpanData(
	ctx context.Context, db *kv.DB, distSender *kvcoord.DistSender, span roachpb.RSpan,
) error {

	// ClearRange requests lays down RocksDB range deletion tombstones that have
	// serious performance implications (#24029). The logic below attempts to
	// bound the number of tombstones in one store by sending the ClearRange
	// requests to each range in the table in small, sequential batches rather
	// than letting DistSender send them all in parallel, to hopefully give the
	// compaction queue time to compact the range tombstones away in between
	// requests.
	//
	// As written, this approach has several deficiencies. It does not actually
	// wait for the compaction queue to compact the tombstones away before
	// sending the next request. It is likely insufficient if multiple DROP
	// TABLEs are in flight at once. It does not save its progress in case the
	// coordinator goes down. These deficiencies could be addressed, but this code
	// was originally a stopgap to avoid the range tombstone performance hit. The
	// RocksDB range tombstone implementation has since been improved and the
	// performance implications of many range tombstones has been reduced
	// dramatically making this simplistic throttling sufficient.

	// These numbers were chosen empirically for the clearrange roachtest and
	// could certainly use more tuning.
	const batchSize = 100
	const waitTime = 500 * time.Millisecond

	var n int
	lastKey := span.Key
	ri := kvcoord.NewRangeIterator(distSender)
	timer := timeutil.NewTimer()
	defer timer.Stop()

	for ri.Seek(ctx, span.Key, kvcoord.Ascending); ; ri.Next(ctx) {
		if !ri.Valid() {
			return ri.Error()
		}

		if n++; n >= batchSize || !ri.NeedAnother(span) {
			endKey := ri.Desc().EndKey
			if span.EndKey.Less(endKey) {
				endKey = span.EndKey
			}
			var b kv.Batch
			b.AddRawRequest(&roachpb.ClearRangeRequest{
				RequestHeader: roachpb.RequestHeader{
					Key:    lastKey.AsRawKey(),
					EndKey: endKey.AsRawKey(),
				},
			})
			log.VEventf(ctx, 2, "ClearRange %s - %s", lastKey, endKey)
			if err := db.Run(ctx, &b); err != nil {
				return errors.Wrapf(err, "clear range %s - %s", lastKey, endKey)
			}
			n = 0
			lastKey = endKey
			timer.Reset(waitTime)
			select {
			case <-timer.C:
				timer.Read = true
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if !ri.NeedAnother(span) {
			break
		}
	}

	return nil
}
