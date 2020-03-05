// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package waiting_for_gc

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

// gcIndexes find the indexes that need to be GC'd, GC's them, and then updates
// the cleans up the table descriptor, zone configs and job payload to indicate
// the work that it did.
func gcIndexes(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	parentID sqlbase.ID,
	progress *jobspb.WaitingForGCProgress,
) error {
	droppedIndexes := progress.Indexes

	// Find which indexes have expired.
	expiredIndexes := getIndexesToDrop(droppedIndexes)
	expiredIndexDescs := make([]sqlbase.IndexDescriptor, len(expiredIndexes))
	for i, index := range expiredIndexes {
		expiredIndexDescs[i] = sqlbase.IndexDescriptor{ID: index.IndexID}
	}

	parentTable, err := getTableDescriptor(ctx, execCfg, parentID)
	if err != nil {
		return err
	}

	if err := clearIndexes(ctx, execCfg.DB, parentTable, expiredIndexDescs); err != nil {
		return err
	}

	// All the data chunks have been removed. Now also removed the
	// zone configs for the dropped indexes, if any.
	if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		return sql.RemoveIndexZoneConfigs(ctx, txn, execCfg, parentTable.GetID(), expiredIndexDescs)
	}); err != nil {
		return err
	}

	if err := completeDroppedIndexes(ctx, execCfg, parentTable, expiredIndexes, progress); err != nil {
		return err
	}

	return nil
}

// getIndexesToDrop returns all indexes whose GC TTL has expired, sorted in
// order that they should be dropped.
func getIndexesToDrop(
	indexes []jobspb.WaitingForGCProgress_IndexProgress,
) []jobspb.WaitingForGCProgress_IndexProgress {
	var indexesToGC []jobspb.WaitingForGCProgress_IndexProgress

	for _, droppedIndex := range indexes {
		if droppedIndex.Status == jobspb.WaitingForGCProgress_DELETING {
			indexesToGC = append(indexesToGC, droppedIndex)
		}
	}

	return indexesToGC
}

// clearIndexes issues Clear Range requests over all specified indexes.
func clearIndexes(
	ctx context.Context,
	db *client.DB,
	tableDesc *sqlbase.TableDescriptor,
	indexes []sqlbase.IndexDescriptor,
) error {
	for _, index := range indexes {
		if index.IsInterleaved() {
			return errors.Errorf("unexpected interleaved index %d", index.ID)
		}

		sp := tableDesc.IndexSpan(index.ID)

		// ClearRange cannot be run in a transaction, so create a
		// non-transactional batch to send the request.
		b := &client.Batch{}
		b.AddRawRequest(&roachpb.ClearRangeRequest{
			RequestHeader: roachpb.RequestHeader{
				Key:    sp.Key,
				EndKey: sp.EndKey,
			},
		})
		return db.Run(ctx, b)
	}

	return nil
}

// completeDroppedIndexes updates the mutations of the table descriptor to
// indicate that the index was dropped, as well as the job detail payload.
func completeDroppedIndexes(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	table *sqlbase.TableDescriptor,
	GCedIndexes []jobspb.WaitingForGCProgress_IndexProgress,
	progress *jobspb.WaitingForGCProgress,
) error {
	garbageCollectedIndexIDs := make(map[sqlbase.IndexID]struct{})
	for _, index := range GCedIndexes {
		garbageCollectedIndexIDs[index.IndexID] = struct{}{}
	}

	if err := updateDescriptorMutations(ctx, execCfg, table, garbageCollectedIndexIDs); err != nil {
		return err
	}

	markIndexGCed(garbageCollectedIndexIDs, progress)

	return nil
}
