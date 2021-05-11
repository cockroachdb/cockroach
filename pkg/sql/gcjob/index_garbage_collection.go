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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

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
	if err := sql.WaitToUpdateLeases(ctx, execCfg.LeaseManager, parentID); err != nil {
		return err
	}

	var parentTable catalog.TableDescriptor
	if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		parentTable, err = catalogkv.MustGetTableDescByID(ctx, txn, execCfg.Codec, parentID)
		return err
	}); err != nil {
		return errors.Wrapf(err, "fetching parent table %d", parentID)
	}

	for _, index := range droppedIndexes {
		if index.Status != jobspb.SchemaChangeGCProgress_DELETING {
			continue
		}

		if err := clearIndex(ctx, execCfg, parentTable, index.IndexID); err != nil {
			return errors.Wrapf(err, "clearing index %d from table %d", index.IndexID, parentTable.GetID())
		}

		// All the data chunks have been removed. Now also removed the
		// zone configs for the dropped indexes, if any.
		removeIndexZoneConfigs := func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) error {
			freshParentTableDesc, err := descriptors.GetMutableTableByID(
				ctx, txn, parentID, tree.ObjectLookupFlags{
					CommonLookupFlags: tree.CommonLookupFlags{
						AvoidCached:    true,
						Required:       true,
						IncludeDropped: true,
						IncludeOffline: true,
					},
				})
			if err != nil {
				return err
			}
			return sql.RemoveIndexZoneConfigs(
				ctx, txn, execCfg, freshParentTableDesc, []uint32{uint32(index.IndexID)},
			)
		}
		lm, ie, db := execCfg.LeaseManager, execCfg.InternalExecutor, execCfg.DB
		if err := descs.Txn(
			ctx, execCfg.Settings, lm, ie, db, removeIndexZoneConfigs,
		); err != nil {
			return errors.Wrapf(err, "removing index %d zone configs", index.IndexID)
		}

		if err := completeDroppedIndex(
			ctx, execCfg, parentTable, index.IndexID, progress,
		); err != nil {
			return err
		}
	}
	return nil
}

// clearIndexes issues Clear Range requests over all specified indexes.
func clearIndex(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	tableDesc catalog.TableDescriptor,
	indexID descpb.IndexID,
) error {
	log.Infof(ctx, "clearing index %d from table %d", indexID, tableDesc.GetID())

	sp := tableDesc.IndexSpan(execCfg.Codec, indexID)
	start, err := keys.Addr(sp.Key)
	if err != nil {
		return errors.Errorf("failed to addr index start: %v", err)
	}
	end, err := keys.Addr(sp.EndKey)
	if err != nil {
		return errors.Errorf("failed to addr index end: %v", err)
	}
	rSpan := roachpb.RSpan{Key: start, EndKey: end}
	return clearSpanData(ctx, execCfg.DB, execCfg.DistSender, rSpan)
}

// completeDroppedIndexes updates the mutations of the table descriptor to
// indicate that the index was dropped, as well as the job detail payload.
func completeDroppedIndex(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	table catalog.TableDescriptor,
	indexID descpb.IndexID,
	progress *jobspb.SchemaChangeGCProgress,
) error {
	if err := updateDescriptorGCMutations(ctx, execCfg, table.GetID(), indexID); err != nil {
		return errors.Wrapf(err, "updating GC mutations")
	}

	markIndexGCed(ctx, indexID, progress)

	return nil
}
