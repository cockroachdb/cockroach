// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gcjob

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
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
		if droppedTable.Status != jobspb.SchemaChangeGCProgress_CLEARING {
			// Table is not ready to be dropped, or has already been dropped.
			continue
		}

		var table catalog.TableDescriptor
		if err := sql.DescsTxn(ctx, execCfg, func(ctx context.Context, txn isql.Txn, col *descs.Collection) (err error) {
			table, err = col.ByIDWithoutLeased(txn.KV()).Get().Table(ctx, droppedTable.ID)
			return err
		}); err != nil {
			if isMissingDescriptorError(err) {
				// This can happen if another GC job created for the same table got to
				// the table first. See #50344.
				log.Warningf(ctx, "table descriptor %d not found while attempting to GC, skipping", droppedTable.ID)
				// Update the details payload to indicate that the table was dropped.
				markTableGCed(ctx, droppedTable.ID, progress, jobspb.SchemaChangeGCProgress_CLEARED)
				continue
			}
			return errors.Wrapf(err, "fetching table %d", droppedTable.ID)
		}

		// TODO(ajwerner): How does this happen?
		if !table.Dropped() {
			// We shouldn't drop this table yet.
			continue
		}

		// First, delete all the table data.
		if err := ClearTableData(
			ctx, execCfg.DB, execCfg.DistSender, execCfg.Codec, &execCfg.Settings.SV, table,
		); err != nil {
			return errors.Wrapf(err, "clearing data for table %d", table.GetID())
		}

		delta, err := spanconfig.Delta(ctx, execCfg.SpanConfigSplitter, table, nil /* uncommitted */)
		if err != nil {
			return err
		}

		// Deduct from system.span_count appropriately.
		if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			_, err := execCfg.SpanConfigLimiter.ShouldLimit(ctx, txn, delta)
			return err
		}); err != nil {
			return errors.Wrapf(err, "deducting span count for table %d", table.GetID())
		}

		// Finished deleting all the table data, now delete the table meta data.
		if err := sql.DeleteTableDescAndZoneConfig(ctx, execCfg, table); err != nil {
			return errors.Wrapf(err, "dropping table descriptor for table %d", table.GetID())
		}

		// Update the details payload to indicate that the table was dropped.
		markTableGCed(ctx, table.GetID(), progress, jobspb.SchemaChangeGCProgress_CLEARED)
	}
	return nil
}

// ClearTableData deletes all of the data in the specified table.
func ClearTableData(
	ctx context.Context,
	db *kv.DB,
	distSender *kvcoord.DistSender,
	codec keys.SQLCodec,
	sv *settings.Values,
	table catalog.TableDescriptor,
) error {
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
	ri := kvcoord.MakeRangeIterator(distSender)
	var timer timeutil.Timer
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
			b.AddRawRequest(&kvpb.ClearRangeRequest{
				RequestHeader: kvpb.RequestHeader{
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

// DeleteAllTableData issues non-transactional MVCC range tombstones over the specified table's
// data.
//
// This function will error, without a resume span, if it encounters an intent
// in the span. The caller should resolve these intents by retrying the
// function. To prevent errors, the caller should only pass a span that will not
// see new writes during this bulk delete operation (e.g. on a span past their
// gc TTL or an offline span a part of an import rollback).
func DeleteAllTableData(
	ctx context.Context,
	db *kv.DB,
	distSender *kvcoord.DistSender,
	codec keys.SQLCodec,
	table catalog.TableDescriptor,
) error {
	log.Infof(ctx, "deleting data for table %d", table.GetID())
	tableKey := roachpb.RKey(codec.TablePrefix(uint32(table.GetID())))
	tableSpan := roachpb.RSpan{Key: tableKey, EndKey: tableKey.PrefixEnd()}
	return deleteAllSpanData(ctx, db, distSender, tableSpan)
}

// deleteAllSpanData issues non-transactional MVCC range tombstones over all
// data in the specified span.
func deleteAllSpanData(
	ctx context.Context, db *kv.DB, distSender *kvcoord.DistSender, span roachpb.RSpan,
) error {

	// When the distSender receives a DeleteRange request, it determines how the
	// request maps to ranges, and automatically parallelizes the sending of
	// sub-batches to the different ranges. If the DeleteRange request spans too
	// many ranges, the distSender may reach its concurrency limit, causing severe
	// latency to foreground traffic (see #85470).
	//
	// To prevent this, partition the provided span into subspans where each
	// contains at most 100 ranges and issue sequential DeleteRangeRequests on
	// each subspan.
	const rangesPerBatch = 100

	var n int
	lastKey := span.Key
	ri := kvcoord.MakeRangeIterator(distSender)

	for ri.Seek(ctx, span.Key, kvcoord.Ascending); ; ri.Next(ctx) {
		if !ri.Valid() {
			return ri.Error()
		}

		if n++; n >= rangesPerBatch || !ri.NeedAnother(span) {
			endKey := ri.Desc().EndKey
			if span.EndKey.Less(endKey) {
				endKey = span.EndKey
			}
			var b kv.Batch
			b.AdmissionHeader = kvpb.AdmissionHeader{
				Priority:                 int32(admissionpb.BulkNormalPri),
				CreateTime:               timeutil.Now().UnixNano(),
				Source:                   kvpb.AdmissionHeader_FROM_SQL,
				NoMemoryReservedAtSource: true,
			}
			b.AddRawRequest(&kvpb.DeleteRangeRequest{
				RequestHeader: kvpb.RequestHeader{
					Key:    lastKey.AsRawKey(),
					EndKey: endKey.AsRawKey(),
				},
				UseRangeTombstone:       true,
				IdempotentTombstone:     true,
				UpdateRangeDeleteGCHint: true,
			})
			log.VEventf(ctx, 2, "delete range %s - %s", lastKey, endKey)
			if err := db.Run(ctx, &b); err != nil {
				log.Errorf(ctx, "delete range %s - %s failed: %s", span.Key, span.EndKey, err.Error())
				return errors.Wrapf(err, "delete range %s - %s", lastKey, endKey)
			}
			n = 0
			lastKey = endKey
		}

		if !ri.NeedAnother(span) {
			break
		}
	}

	return nil
}

func deleteTableDescriptorsAfterGC(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	details *jobspb.SchemaChangeGCDetails,
	progress *jobspb.SchemaChangeGCProgress,
) error {
	checkImmediatelyOnWait := false
	for _, droppedTable := range progress.Tables {
		if droppedTable.Status == jobspb.SchemaChangeGCProgress_CLEARED {
			// Table is not ready to be dropped, or has already been dropped.
			continue
		}

		var table catalog.TableDescriptor
		if err := sql.DescsTxn(ctx, execCfg, func(ctx context.Context, txn isql.Txn, col *descs.Collection) (err error) {
			table, err = col.ByIDWithoutLeased(txn.KV()).Get().Table(ctx, droppedTable.ID)
			return err
		}); err != nil {
			if isMissingDescriptorError(err) {
				// This can happen if another GC job created for the same table got to
				// the table first. See #50344.
				log.Warningf(ctx, "table descriptor %d not found while attempting to GC, skipping", droppedTable.ID)
				// Update the details payload to indicate that the table was dropped.
				markTableGCed(ctx, droppedTable.ID, progress, jobspb.SchemaChangeGCProgress_CLEARED)
				continue
			}
			return errors.Wrapf(err, "fetching table %d", droppedTable.ID)
		}

		// TODO(ajwerner): How does this happen?
		if !table.Dropped() {
			// We shouldn't drop this table yet.
			continue
		}

		// First, delete all the table data.
		if err := waitForEmptyPrefix(
			ctx, execCfg.DB, &execCfg.Settings.SV,
			execCfg.GCJobTestingKnobs.SkipWaitingForMVCCGC,
			checkImmediatelyOnWait,
			execCfg.Codec.TablePrefix(uint32(table.GetID())),
		); err != nil {
			return errors.Wrapf(err, "waiting for empty table %d", table.GetID())
		}
		// Assume that once one of our tables have completed GC, the next table may
		// also have completed GC.
		checkImmediatelyOnWait = true
		delta, err := spanconfig.Delta(ctx, execCfg.SpanConfigSplitter, table, nil /* uncommitted */)
		if err != nil {
			return err
		}

		// Deduct from system.span_count appropriately.
		if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			_, err := execCfg.SpanConfigLimiter.ShouldLimit(ctx, txn, delta)
			return err
		}); err != nil {
			return errors.Wrapf(err, "deducting span count for table %d", table.GetID())
		}

		// Finished deleting all the table data, now delete the table meta data.
		if err := sql.DeleteTableDescAndZoneConfig(ctx, execCfg, table); err != nil {
			return errors.Wrapf(err, "dropping table descriptor for table %d", table.GetID())
		}

		// Update the details payload to indicate that the table was dropped.
		markTableGCed(ctx, table.GetID(), progress, jobspb.SchemaChangeGCProgress_CLEARED)
	}

	// Drop database zone config when all the tables have been GCed.
	if details.ParentID != descpb.InvalidID && isDoneGC(progress) {
		if err := deleteDatabaseZoneConfig(
			ctx,
			execCfg.DB,
			execCfg.Codec,
			execCfg.Settings,
			details.ParentID,
		); err != nil {
			return errors.Wrap(err, "deleting database zone config")
		}
	}
	return nil
}
