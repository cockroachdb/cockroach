// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gcjob

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/regions"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// deleteIndexData is used to issue range deletion tombstones over all indexes
// being gc'd.
func deleteIndexData(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	parentID descpb.ID,
	progress *jobspb.SchemaChangeGCProgress,
) error {
	droppedIndexes := progress.Indexes
	if log.V(2) {
		log.Infof(ctx, "GC is being considered on table %d for indexes indexes: %+v", parentID, droppedIndexes)
	}

	// Before deleting any indexes, ensure that old versions of the table descriptor
	// are no longer in use. This is necessary in the case of truncate, where we
	// schedule a GC Job in the transaction that commits the truncation.
	cachedRegions, err := regions.NewCachedDatabaseRegions(ctx, execCfg.DB, execCfg.LeaseManager)
	if err != nil {
		return err
	}
	parentDesc, err := sql.WaitToUpdateLeases(ctx, execCfg.LeaseManager, cachedRegions, parentID)
	if isMissingDescriptorError(err) {
		handleTableDescriptorDeleted(ctx, parentID, progress)
		return nil
	}
	if err != nil {
		return err
	}

	parentTable, isTable := parentDesc.(catalog.TableDescriptor)
	if !isTable {
		return errors.AssertionFailedf("expected descriptor %d to be a table, not %T", parentID, parentDesc)
	}
	for _, index := range droppedIndexes {
		// TODO(ajwerner): Is there any reason to check on the current status of
		// the index? At time of writing, we don't checkpoint between operating on
		// individual indexes, and we always delete the data before moving on to
		// waiting, so it seems like there's nothing to check for.

		if err := clearIndex(
			ctx, execCfg, parentTable, index.IndexID, deleteAllSpanData,
		); err != nil {
			return errors.Wrapf(err, "deleting index %d from table %d", index.IndexID, parentTable.GetID())
		}
		markIndexGCed(
			ctx, index.IndexID, progress,
			jobspb.SchemaChangeGCProgress_WAITING_FOR_MVCC_GC,
		)
	}
	return nil
}

// gcIndexes find the indexes that need to be GC'd, GC's them, and then updates
// the cleans up the table descriptor, zone configs and job payload to indicate
// the work that it did.
func gcIndexes(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	parentID descpb.ID,
	progress *jobspb.SchemaChangeGCProgress,
) error {
	droppedIndexes := progress.Indexes
	if log.V(2) {
		log.Infof(ctx, "GC is being considered on table %d for indexes indexes: %+v", parentID, droppedIndexes)
	}

	// Before deleting any indexes, ensure that old versions of the table descriptor
	// are no longer in use. This is necessary in the case of truncate, where we
	// schedule a GC Job in the transaction that commits the truncation.
	cachedRegions, err := regions.NewCachedDatabaseRegions(ctx, execCfg.DB, execCfg.LeaseManager)
	if err != nil {
		return err
	}
	parentDesc, err := sql.WaitToUpdateLeases(ctx, execCfg.LeaseManager, cachedRegions, parentID)
	if isMissingDescriptorError(err) {
		handleTableDescriptorDeleted(ctx, parentID, progress)
		return nil
	}
	if err != nil {
		return err
	}

	parentTable, isTable := parentDesc.(catalog.TableDescriptor)
	if !isTable {
		return errors.AssertionFailedf("expected descriptor %d to be a table, not %T", parentID, parentDesc)
	}
	for _, index := range droppedIndexes {
		if index.Status != jobspb.SchemaChangeGCProgress_CLEARING {
			continue
		}

		if err := clearIndex(
			ctx, execCfg, parentTable, index.IndexID, clearSpanData,
		); err != nil {
			return errors.Wrapf(err, "clearing index %d from table %d", index.IndexID, parentTable.GetID())
		}

		// All the data chunks have been removed. Now also removed the
		// zone configs for the dropped indexes, if any.
		removeIndexZoneConfigs := func(
			ctx context.Context, txn descs.Txn,
		) error {
			freshParentTableDesc, err := txn.Descriptors().MutableByID(txn.KV()).Table(ctx, parentID)
			if err != nil {
				return err
			}
			return sql.RemoveIndexZoneConfigs(
				ctx, txn, execCfg, false /* kvTrace */, freshParentTableDesc, []uint32{uint32(index.IndexID)},
			)
		}
		err := execCfg.InternalDB.DescsTxn(ctx, removeIndexZoneConfigs)
		if isMissingDescriptorError(err) {
			handleTableDescriptorDeleted(ctx, parentID, progress)
			return nil
		}
		if err != nil {
			return errors.Wrapf(err, "removing index %d zone configs", index.IndexID)
		}
		markIndexGCed(
			ctx, index.IndexID, progress, jobspb.SchemaChangeGCProgress_CLEARED,
		)
	}
	return nil
}

type clearOrDeleteSpanDataFunc = func(
	ctx context.Context,
	db *kv.DB,
	distSender *kvcoord.DistSender,
	span roachpb.RSpan,
) error

// clearIndexes issues Clear Range requests over all specified indexes.
func clearIndex(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	tableDesc catalog.TableDescriptor,
	indexID descpb.IndexID,
	clearOrDeleteSpanData clearOrDeleteSpanDataFunc,
) error {
	log.Infof(ctx, "clearing index %d from table %d", indexID, tableDesc.GetID())

	sp := tableDesc.IndexSpan(execCfg.Codec, indexID)
	start, err := keys.Addr(sp.Key)
	if err != nil {
		return errors.Wrap(err, "failed to addr index start")
	}
	end, err := keys.Addr(sp.EndKey)
	if err != nil {
		return errors.Wrap(err, "failed to addr index end")
	}
	rSpan := roachpb.RSpan{Key: start, EndKey: end}
	return clearOrDeleteSpanData(ctx, execCfg.DB, execCfg.DistSender, rSpan)
}

func deleteIndexZoneConfigsAfterGC(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	parentID descpb.ID,
	progress *jobspb.SchemaChangeGCProgress,
) error {
	checkImmediatelyOnWait := false
	for _, index := range progress.Indexes {
		if index.Status == jobspb.SchemaChangeGCProgress_CLEARED {
			continue
		}

		if err := waitForEmptyPrefix(
			ctx, execCfg.DB, execCfg.SV(),
			execCfg.GCJobTestingKnobs.SkipWaitingForMVCCGC,
			checkImmediatelyOnWait,
			execCfg.Codec.IndexPrefix(uint32(parentID), uint32(index.IndexID)),
		); err != nil {
			return errors.Wrapf(err, "waiting for gc of index %d from table %d",
				index.IndexID, parentID)
		}
		checkImmediatelyOnWait = true
		// All the data chunks have been removed. Now also removed the
		// zone configs for the dropped indexes, if any.
		removeIndexZoneConfigs := func(
			ctx context.Context, txn descs.Txn,
		) error {
			freshParentTableDesc, err := txn.Descriptors().MutableByID(txn.KV()).Table(ctx, parentID)
			if err != nil {
				return err
			}
			return sql.RemoveIndexZoneConfigs(
				ctx, txn, execCfg, false /* kvTrace */, freshParentTableDesc, []uint32{uint32(index.IndexID)},
			)
		}
		err := execCfg.InternalDB.DescsTxn(ctx, removeIndexZoneConfigs)
		switch {
		case isMissingDescriptorError(err):
			log.Infof(ctx, "removing index %d zone config from table %d failed: %v",
				index.IndexID, parentID, err)
		case err != nil:
			return errors.Wrapf(err, "removing index %d zone configs", index.IndexID)
		}
		markIndexGCed(
			ctx, index.IndexID, progress, jobspb.SchemaChangeGCProgress_CLEARED,
		)
	}
	return nil
}

// handleTableDescriptorDeleted should be called when logic detects that
// a table descriptor has been deleted while attempting to GC an index.
// The function marks in progress that all indexes have been cleared.
func handleTableDescriptorDeleted(
	ctx context.Context, parentID descpb.ID, progress *jobspb.SchemaChangeGCProgress,
) {
	droppedIndexes := progress.Indexes
	// If the descriptor has been removed, then we need to assume that the relevant
	// zone configs and data have been cleaned up by another process.
	log.Infof(ctx, "descriptor %d dropped, assuming another process has handled GC", parentID)
	for _, index := range droppedIndexes {
		markIndexGCed(
			ctx, index.IndexID, progress, jobspb.SchemaChangeGCProgress_CLEARED,
		)
	}
}
