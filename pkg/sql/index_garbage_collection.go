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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

// gcIndexes find the indexes that need to be GC'd, GC's them, and then updates
// the cleans up the table descriptor, zone configs and job payload to indicate
// the work that it did.
func gcIndexes(
	ctx context.Context,
	execCfg *ExecutorConfig,
	parentID sqlbase.ID,
	progress *jobspb.WaitingForGCProgress,
) error {
	droppedIndexes := progress.Indexes

	// Find which indexes have expired.
	expiredIndexes := filterExpiredIndexes(droppedIndexes)
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
		return removeIndexZoneConfigs(ctx, txn, execCfg, parentTable.GetID(), expiredIndexDescs)
	}); err != nil {
		return err
	}

	if err := completeDroppedIndexes(ctx, execCfg, parentTable, expiredIndexes, progress); err != nil {
		return err
	}

	return nil
}

func getTableDescriptor(
	ctx context.Context, execCfg *ExecutorConfig, ID sqlbase.ID,
) (*sqlbase.TableDescriptor, error) {
	var table *sqlbase.TableDescriptor
	if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		table, err = sqlbase.GetTableDescFromID(ctx, txn, ID)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return table, nil
}

// filterExpiredIndexes returns all indexes whose GC TTL has expired, sorted in
// order that they should be dropped.
func filterExpiredIndexes(
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
	execCfg *ExecutorConfig,
	table *sqlbase.TableDescriptor,
	GCedIndexes []jobspb.WaitingForGCProgress_IndexProgress,
	progress *jobspb.WaitingForGCProgress,
) error {
	garbageCollectedIndexIDs := make(map[sqlbase.IndexID]struct{})
	for _, index := range GCedIndexes {
		garbageCollectedIndexIDs[index.IndexID] = struct{}{}
	}

	// Remove the mutation from the table descriptor.
	updateTableMutations := func(tbl *sqlbase.MutableTableDescriptor) error {
		found := false
		for i := 0; i < len(tbl.GCMutations); i++ {
			other := tbl.GCMutations[i]
			_, ok := garbageCollectedIndexIDs[other.IndexID]
			if ok {
				tbl.GCMutations = append(tbl.GCMutations[:i], tbl.GCMutations[i+1:]...)
				found = true
				break
			}
		}
		if !found {
			return errDidntUpdateDescriptor
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

	// Update the job details to remove the dropped indexes.
	for i := range progress.Indexes {
		indexToUpdate := &progress.Indexes[i]
		if _, ok := garbageCollectedIndexIDs[indexToUpdate.IndexID]; ok {
			indexToUpdate.Status = jobspb.WaitingForGCProgress_DELETED
		}
	}

	return nil
}
