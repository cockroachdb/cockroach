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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// filterExpiredIndexes returns all indexes whose GC TTL has expired, sorted by drop
// time.
func filterExpiredIndexes(
	ctx context.Context,
	protectedtsCache protectedts.Cache,
	tableDesc *sqlbase.TableDescriptor,
	indexes []jobspb.WaitingForGCDetails_DroppedIndex,
) []jobspb.WaitingForGCDetails_DroppedIndex {
	indexesToGC := make([]jobspb.WaitingForGCDetails_DroppedIndex, 0)

	// In addition to checking the deadline, check if the keys are protected.
	for _, droppedIndex := range indexes {
		hasExpired := timeutil.Since(timeutil.Unix(0, droppedIndex.Deadline)) > 0
		if hasExpired {
			indexesToGC = append(indexesToGC, droppedIndex)
		}
	}

	sort.Slice(indexesToGC, func(i, j int) bool {
		return indexesToGC[i].Deadline < indexesToGC[j].Deadline
	})

	return indexesToGC
}

func clearIndexes(
	ctx context.Context,
	db *client.DB,
	tableDesc *sqlbase.TableDescriptor,
	indexes []sqlbase.IndexDescriptor,
) error {
	for _, index := range indexes {
		return clearIndex(ctx, db, tableDesc, &index)
	}

	return nil
}

func clearIndex(
	ctx context.Context,
	db *client.DB,
	tableDesc *sqlbase.TableDescriptor,
	idx *sqlbase.IndexDescriptor,
) error {
	if idx.IsInterleaved() {
		return errors.Errorf("unexpected interleaved index %d", idx.ID)
	}

	sp := tableDesc.IndexSpan(idx.ID)

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

// completeDroppedIndexes updates the mutations of the table descriptor to
// indicate that the index was dropped.
func completeDroppedIndexes(
	ctx context.Context,
	execCfg *ExecutorConfig,
	table *sqlbase.TableDescriptor,
	indexes []jobspb.WaitingForGCDetails_DroppedIndex,
	details *jobspb.WaitingForGCDetails,
) error {
	droppedIndexIDs := make(map[sqlbase.IndexID]struct{})
	for _, index := range indexes {
		droppedIndexIDs[index.IndexID] = struct{}{}
	}

	// Remove the mutation from the table descriptor.
	leaseManager := execCfg.LeaseManager
	updateTableMutations := func(tbl *sqlbase.MutableTableDescriptor) error {
		found := false
		for i := 0; i < len(tbl.GCMutations); i++ {
			other := tbl.GCMutations[i]
			_, ok := droppedIndexIDs[other.IndexID]
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

	_, err := leaseManager.Publish(
		ctx,
		table.ID,
		updateTableMutations,
		nil, /* logEvent */
	)
	if err != nil {
		return err
	}

	// Update the job details to remove the dropped indexes.
	for i, indexToDrop := range details.Indexes {
		if _, ok := droppedIndexIDs[indexToDrop.IndexID]; ok {
			details.Indexes = append(details.Indexes[:i], details.Indexes[i+1:]...)
		}
	}
	if len(details.Indexes) == 0 {
		// Clear out the tables if all indexes have been dropped to indicate that
		// the GC is done.
		details.Tables = details.Tables[:0]
	}

	return nil
}

func gcIndexes(
	ctx context.Context,
	execCfg *ExecutorConfig,
	parentTable *TableDescriptor,
	details *jobspb.WaitingForGCDetails,
) error {
	droppedIndexes := details.Indexes

	if len(droppedIndexes) == 0 {
		// All indexes have been dropped.
		return nil
	}

	expiredIndexes := filterExpiredIndexes(ctx, execCfg.ProtectedTimestampProvider, parentTable, droppedIndexes)
	expiredIndexDescs := make([]sqlbase.IndexDescriptor, len(expiredIndexes))
	for i, index := range expiredIndexes {
		expiredIndexDescs[i] = sqlbase.IndexDescriptor{ID: index.IndexID}
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

	if err := completeDroppedIndexes(ctx, execCfg, parentTable, expiredIndexes, details); err != nil {
		return err
	}

	return nil
}
