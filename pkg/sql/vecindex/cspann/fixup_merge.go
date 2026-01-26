// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// mergePartition starts or continues a merge of the given partition. This is
// performed as a series of atomic steps that incrementally update the index.
// After each step, the overall index is in a well-defined state that still
// allows searches, inserts, and deletes. However, depending on their state,
// individual partitions can disallow inserts and deletes.
//
// Here is the flow for merging a partition:
//
//  1. Update the merging partition's state from Ready to Merging. Only do this
//     if there is another partition at the same level of the tree in the Ready
//     state. This ensures that the merging vectors always have a place to go.
//  2. Move each vector in the merging partition to another partition at the
//     same level that has the closest centroid.
//  3. Once the merging partition has no more vectors, remove it from its
//     parent. The partition is no longer visible in the index. However, leave
//     its metadata record behind as a tombstone, to ensure the partition is not
//     re-created by a racing worker still stuck on the creation step of a
//     previous split.
//
// The following diagrams show the partition state machines for the split
// operation:
//
// +----------------+
// |     Ready      |
// +-------+--------+
// .       |
// +-------v--------+
// |    Merging     |
// +----------------+
// .
func (fw *fixupWorker) mergePartition(
	ctx context.Context, parentPartitionKey PartitionKey, partitionKey PartitionKey,
) (err error) {
	defer func() {
		// Suppress fixup aborted error, since this is an expected error when
		// workers are racing.
		if errors.Is(err, errFixupAborted) {
			log.VEventf(ctx, 2, "aborted merge of partition %d: %v", partitionKey, err)
			err = nil
		} else {
			err = errors.Wrapf(err, "merging partition %d", partitionKey)
		}
	}()

	partition, err := fw.getPartition(ctx, partitionKey)
	if err != nil || partition == nil {
		return err
	}

	metadata := *partition.Metadata()
	if metadata.StateDetails.State == ReadyState {
		// Check if partition size has increased while the fixup was waiting to
		// be processed. If it's gotten too large, abort the fixup (the partition
		// is in the Ready state, so the fixup is not yet in progress). Note that
		// even if it's above the min partition size, we still merge, since if it
		// fell below that size, it's likely to do so again, and we don't want to
		// be making expensive getPartition calls.
		threshold := fw.index.options.MinPartitionSize * 3 / 2
		if partition.Count() >= threshold && !fw.singleStep {
			return nil
		}
	} else {
		if !metadata.StateDetails.MaybeMergeStalled(fw.index.options.StalledOpTimeout()) {
			// There's evidence that another worker has been recently processing
			// the partition, so don't process the fixup. This minimizes the
			// possibility of multiple workers on different nodes doing duplicate
			// work (an efficiency issue, not a correctness issue).
			return nil
		}
	}

	// Update partition's state to Merging.
	if metadata.StateDetails.State == ReadyState {
		if err = fw.startMerge(ctx, partitionKey, metadata); err != nil {
			return err
		}

		log.VEventf(ctx, 2, "merging partition %d with %d vectors (parent=%d, state=%s)",
			partitionKey, partition.Count(), parentPartitionKey, metadata.StateDetails.String())

		// Now that the partition is in the merging state, re-fetch its vectors,
		// since more may have been added while updating the state.
		partition, err = fw.getPartition(ctx, partitionKey)
		if err != nil || partition == nil {
			return err
		}
		metadata = *partition.Metadata()
	}

	if metadata.StateDetails.State == MergingState {
		// Get the full vectors for the merging partition.
		var vectors vector.Set
		vectors, err = fw.getFullVectorsForPartition(ctx, partitionKey, partition)
		if err != nil {
			return err
		}

		// Move vectors into new partitions at the same level.
		for i := range vectors.Count {
			childKey := partition.ChildKeys()[i]
			err = fw.index.store.RunTransaction(ctx, func(txn Txn) error {
				fw.tempIndexCtx.Init(txn)
				fw.index.setupInsertContext(&fw.tempIndexCtx, fw.treeKey, metadata.Level)
				fw.tempIndexCtx.query.InitTransformed(vectors.At(i))
				return fw.index.insertHelper(ctx, &fw.tempIndexCtx, fw.treeKey, childKey)
			})
			if err != nil {
				return err
			}

			if err = fw.removeFromPartition(ctx, partitionKey, childKey, metadata); err != nil {
				return err
			}
		}

		// Remove the merged partition from its parent.
		err = fw.removeFromParentPartition(ctx, parentPartitionKey, partitionKey, metadata.Level+1)
		if err != nil {
			return err
		}
	}

	return nil
}

// startMerge updates the state of the given partition from Ready to Merging. It
// also ensures that there is another partition at the same level of the tree
// that's in the Ready state. This ensures that there is always a target
// partition where merging vectors can be moved. Otherwise, the merge could get
// stuck.
func (fw *fixupWorker) startMerge(
	ctx context.Context, partitionKey PartitionKey, metadata PartitionMetadata,
) error {
	// Find candidate partitions at the same level as the input partition.
	err := fw.index.store.RunTransaction(ctx, func(txn Txn) error {
		// Setup the search context. Search for the centroid of the input partition
		// in the parent level. The centroid has already been randomized, but
		// is not yet normalized.
		fw.tempIndexCtx.Init(txn)
		fw.index.setupContext(&fw.tempIndexCtx, fw.treeKey, metadata.Level+1, SearchOptions{
			SkipRerank:              true,
			DisableSplitMergeFixups: !fw.index.options.IsDeterministic,
		})
		fw.tempIndexCtx.query.InitCentroid(fw.index.quantizer.GetDistanceMetric(), metadata.Centroid)

		fw.tempIndexCtx.tempSearchSet.Init()
		fw.tempIndexCtx.tempSearchSet.MaxResults = 4

		err := fw.index.searchHelper(ctx, &fw.tempIndexCtx, &fw.tempIndexCtx.tempSearchSet)
		if err != nil {
			return errors.Wrapf(err, "searching for partition at the same level")
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Check whether any of the candidates is in the Ready state.
	for {
		result := fw.tempIndexCtx.tempSearchSet.PopBestResult()
		if result == nil {
			break
		}
		if result.ChildKey.PartitionKey == partitionKey {
			// Skip over merging partition.
			continue
		}

		expected := metadata
		err = fw.index.store.TryStartMerge(
			ctx, fw.treeKey, partitionKey, expected, result.ChildKey.PartitionKey)
		if err != nil {
			metadata, err = suppressRaceErrors(err)
			if err == nil {
				// Another worker raced to start the merge, so abort.
				return errors.Wrapf(errFixupAborted,
					"starting partition %d merge with partition %d at same level: expected %s, found %s",
					partitionKey, result.ChildKey.PartitionKey,
					metadata.StateDetails.String(), metadata.StateDetails.String())
			}
			return errors.Wrapf(err, "starting partition %d merge with partition %d at same level",
				partitionKey, result.ChildKey.PartitionKey)
		}

		// Partition state was successfully updated to Merging.
		if fw.singleStep {
			return errFixupAborted
		}
		return nil
	}

	return errors.Wrapf(errFixupAborted,
		"could not find a Ready partition at the same level as partition %d", partitionKey)
}
