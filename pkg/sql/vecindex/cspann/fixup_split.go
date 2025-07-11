// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"context"
	"math"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/utils"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// splitPartition starts or continues a split of the given partition. This is
// performed as a series of atomic steps that incrementally update the index.
// After each step, the overall index is in a well-defined state that still
// updateMetadata searches, inserts, and deletes. However, depending on their
// state, individual partitions can disallow inserts and deletes.
//
// Here is the flow for splitting a non-root partition:
//
//  1. Update the splitting partition's state from Ready to Splitting. Allocate
//     keys for two target partitions where vectors will be copied.
//  2. Create an empty left target partition in state Updating, with its source
//     set to the splitting partition. Use K-means to compute its centroid from
//     the subset of vectors it is expected to contain (note that the actual
//     subset may shift if there are concurrent updates).
//  3. Add the left target partition to the splitting partition's parent.
//  4. Similarly, create an empty right target partition.
//  5. Add the right target partition to the parent.
//  6. Update the splitting partition's state from Splitting to
//     DrainingForSplit. Vectors cannot be inserted in this state, but instead
//     are "forwarded" to the closest target partition.
//  7. Reload the splitting partition's vectors and reassign any that are closer
//     to a sibling partition's centroid than they are to either of the target
//     partitions' centroids.
//  8. Search in other partitions at the same tree level for vectors that are
//     closer to a target partition's centroid than they are to their own.
//     Reassign any that are found to one of the target partitions.
//  9. Reload the splitting partition's vectors and copy the "left" subset to
//     the left target partition.
//  10. Copy the "right" subset of vectors to the right target partition. At this
//     point, the splitting vectors are duplicated in the index. Any searches
//     will filter out duplicates.
//  11. Clear all vectors from the splitting partition, so that it's empty.
//     Searches can still find these vectors because they were copied to the
//     left and right target partitions in steps #7 and #8.
//  12. Update the splitting partition's state from DrainingForSplit to
//     DeletingForSplit.
//  13. Update the left target partition's state from Updating to Ready.
//  14. Update the right target partition's state from Updating to Ready.
//  15. Remove the splitting partition from its parent. The partition is no
//     longer visible in the index. However, leave its metadata record behind
//     as a tombstone, to ensure the partition is not re-created by a racing
//     worker still stuck on the creation step of a previous split.
//
// Splitting a root partition changes the above flow in the following ways:
//
//  1. Follow above steps #1 - #11, skipping steps #3, #5, #8, and #9 since the
//     root partition does not have a parent.
//  2. Update the root partition's state from DrainingForSplit to AddingLevel
//     and increase its level by one.
//  3. Add the left target partition as a child of the root partition.
//  4. Update the left target partition's state from Updating to Ready.
//  5. Add the right target partition as a child of the root partition.
//  6. Update the right target partition's state from Updating to Ready.
//  7. Update the root partition's state to Ready.
//
// The following diagrams show the partition state machines for the split
// operation:
//
// Splitting Non-Root        Splitting Root         Target of Split
// +----------------+      +----------------+      +----------------+
// |     Ready      |      |     Ready      |      |     Missing    |
// +-------+--------+      +-------+--------+      +-------+--------+
// .       |                       |                       |
// +-------v--------+      +-------v--------+      +-------v--------+
// |   Splitting    |      |   Splitting    |      |    Updating    |
// +-------+--------+      +-------+--------+      +-------+--------+
// .       |                       |                       |
// +-------v--------+      +-------v--------+      +-------v--------+
// |DrainingForSplit|      |DrainingForSplit|      |     Ready      |
// +-------+--------+      +-------+--------+      +----------------+
// .       |                       |
// +-------v--------+      +-------v--------+
// |   Deleting     |      |  AddingLevel   |
// +----------------+      +-------+--------+
// .                               |
// .                       +-------v--------+
// .                       |     Ready      |
// .                       +----------------+
// .
func (fw *fixupWorker) splitPartition(
	ctx context.Context, parentPartitionKey PartitionKey, partitionKey PartitionKey,
) (err error) {
	defer func() {
		// Suppress fixup aborted error, since this is an expected error when
		// workers are racing.
		if errors.Is(err, errFixupAborted) {
			log.VEventf(ctx, 2, "aborted split of partition %d: %v", partitionKey, err)
			err = nil
		} else {
			err = errors.Wrapf(err, "splitting partition %d", partitionKey)
		}
	}()

	partition, err := fw.getPartition(ctx, partitionKey)
	if err != nil || partition == nil {
		return err
	}

	metadata := *partition.Metadata()
	if metadata.StateDetails.State == ReadyState {
		// Check if partition size has decreased while the fixup was waiting to
		// be processed. If it's gone down too much, abort the fixup (the partition
		// is in the Ready state, so the fixup is not yet in progress). Note that
		// even if it's below the max partition size, we still split, since if it
		// exceeded that size, it's likely to do so again, and we don't want to be
		// making expensive getPartition calls.
		threshold := (fw.index.options.MaxPartitionSize + fw.index.options.MinPartitionSize) / 2
		if partition.Count() <= threshold && !fw.singleStep {
			return nil
		}
	} else {
		if !metadata.StateDetails.MaybeSplitStalled(fw.index.options.StalledOpTimeout()) {
			// There's evidence that another worker has been recently processing
			// the partition, so don't process the fixup. This minimizes the
			// possibility of multiple workers on different nodes doing duplicate
			// work (an efficiency issue, not a correctness issue).
			return nil
		}
	}

	// Update partition's state to Splitting.
	if metadata.StateDetails.State == ReadyState {
		expected := metadata
		metadata.StateDetails.MakeSplitting(
			fw.index.store.MakePartitionKey(), fw.index.store.MakePartitionKey())
		err = fw.updateMetadata(ctx, partitionKey, metadata, expected)
		if err != nil {
			return err
		}
		partition.Metadata().StateDetails = metadata.StateDetails

		log.VEventf(ctx, 2, "splitting partition %d with %d vectors (parent=%d, state=%s)",
			partitionKey, partition.Count(), parentPartitionKey, metadata.StateDetails.String())
	}

	// Create left and right target partitions.
	var vectors vector.Set
	var leftMetadata, rightMetadata PartitionMetadata
	leftPartitionKey := metadata.StateDetails.Target1
	rightPartitionKey := metadata.StateDetails.Target2
	if metadata.StateDetails.State == SplittingState {
		// Get the full vectors for the splitting partition.
		vectors, err = fw.getFullVectorsForPartition(ctx, partitionKey, partition)
		if err != nil {
			return err
		}

		// Compute centroids for new target partitions.
		leftCentroid := make(vector.T, fw.index.quantizer.GetDims())
		rightCentroid := make(vector.T, fw.index.quantizer.GetDims())
		fw.computeSplitCentroids(
			partition, vectors, leftCentroid, rightCentroid, false /* pinLeftCentroid */)

		// Create empty target partitions.
		leftMetadata, err = fw.createSplitSubPartition(
			ctx, parentPartitionKey, partitionKey, metadata, leftPartitionKey, leftCentroid)
		if err != nil {
			return err
		}

		// If another worker has already written a possibly different left centroid,
		// then use that instead of the one that was just computed.
		// NOTE: If underlying slice arrays are the same, then no recomputation is
		// needed because the centroids were not updated by createSplitSubPartition.
		if &leftMetadata.Centroid[0] != &leftCentroid[0] {
			fw.computeSplitCentroids(
				partition, vectors, leftMetadata.Centroid, rightCentroid, true /* pinLeftCentroid */)
		}

		rightMetadata, err = fw.createSplitSubPartition(
			ctx, parentPartitionKey, partitionKey, metadata, rightPartitionKey, rightCentroid)
		if err != nil {
			return err
		}

		// Move to the DrainingForSplit state.
		expected := metadata
		metadata.StateDetails.MakeDrainingForSplit(leftPartitionKey, rightPartitionKey)
		err = fw.updateMetadata(ctx, partitionKey, metadata, expected)
		if err != nil {
			return err
		}
		partition.Metadata().StateDetails = metadata.StateDetails

		// Reload the partition, in case new vectors have been added to it while
		// creating the target partitions. Now that we're in the DrainingForSplit
		// state, no new vectors can be added to this partition going forward.
		partition, err = fw.getPartition(ctx, partitionKey)
		if err != nil || partition == nil {
			return err
		}
		if !metadata.HasSameTimestamp(partition.Metadata()) {
			// Another worker must have updated the metadata, so abort.
			return errors.Wrapf(errFixupAborted, "reloading partition, metadata timestamp changed")
		}
	} else if metadata.StateDetails.State != MissingState {
		// Fetch metadata for already created left and right partitions.
		leftMetadata, err = fw.getPartitionMetadata(ctx, leftPartitionKey)
		if err != nil {
			return err
		}
		rightMetadata, err = fw.getPartitionMetadata(ctx, rightPartitionKey)
		if err != nil {
			return err
		}
	}

	if metadata.StateDetails.State == DrainingForSplitState {
		// Get the full vectors for the splitting partition.
		vectors, err = fw.getFullVectorsForPartition(ctx, partitionKey, partition)
		if err != nil {
			return err
		}

		// While most vectors will be assigned to the new target partitions, some
		// may need to be reassigned to siblings that are closer.
		vectors, err = fw.reassignToSiblings(
			ctx, parentPartitionKey, partitionKey, partition, vectors,
			leftPartitionKey, rightPartitionKey)
		if err != nil {
			return err
		}

		// If still updating the target partitions, then distribute vectors among
		// them.
		leftState := leftMetadata.StateDetails.State
		rightState := rightMetadata.StateDetails.State
		if leftState == UpdatingState && rightState == UpdatingState {
			// Search for vectors at this level that are closer to one of the new
			// target partitions.
			err = fw.reassignFromSameLevel(ctx,
				partitionKey, leftPartitionKey, rightPartitionKey, leftMetadata, rightMetadata)
			if err != nil {
				return err
			}

			err = fw.copyToSplitSubPartitions(ctx, partition, vectors, leftMetadata, rightMetadata)
			if err != nil {
				return err
			}
		}

		// Partition should not be needed after this point.
		partition = nil

		// Clear all vectors from the splitting partition. Note that the vectors
		// have already been copied to the two target partitions, so they're still
		// accessible to splits.
		err = fw.clearPartition(ctx, partitionKey, metadata)
		if err != nil {
			return err
		}

		// Check whether the splitting partition is the root.
		if parentPartitionKey != InvalidKey {
			// This is a non-root partition, so move to DeletingForSplit state.
			expected := metadata
			metadata.StateDetails.MakeDeletingForSplit(leftPartitionKey, rightPartitionKey)
			err = fw.updateMetadata(ctx, partitionKey, metadata, expected)
			if err != nil {
				return err
			}
		} else {
			// This is the root partition, so move to the AddingLevel state and
			// increase its level by one.
			expected := metadata
			metadata.Level++
			metadata.StateDetails.MakeAddingLevel(leftPartitionKey, rightPartitionKey)
			err = fw.updateMetadata(ctx, partitionKey, metadata, expected)
			if err != nil {
				return err
			}
		}
	}

	if metadata.StateDetails.State == DeletingForSplitState {
		// Update target partition states from Updating to Ready.
		if leftMetadata.StateDetails.State == UpdatingState {
			expected := leftMetadata
			leftMetadata.StateDetails.MakeReady()
			err = fw.updateMetadata(ctx, leftPartitionKey, leftMetadata, expected)
			if err != nil {
				return err
			}
		}
		if rightMetadata.StateDetails.State == UpdatingState {
			expected := rightMetadata
			rightMetadata.StateDetails.MakeReady()
			err = fw.updateMetadata(ctx, rightPartitionKey, rightMetadata, expected)
			if err != nil {
				return err
			}
		}

		// Remove the splitting partition from its parent. Note that we don't
		// delete the partition's metadata record, instead leaving it behind as a
		// "tombstone". This prevents other racing workers from resurrecting the
		// partition as a zombie, which could otherwise happen like this:
		//
		//  1. Partition A begins splitting into partition B and partition C.
		//  2. Multiple workers are racing to create empty partitions B and C.
		//  3. Partition B is marked as Ready and immediately gets split into
		//     partitions D and E.
		//  4. Partition B is deleted from the index.
		//  5. Meanwhile, there's still a worker running step #2, and it
		//     re-creates partition B.
		//
		// The reborn partition is empty and in a zombie Updating state that will
		// never be set to Ready. Even worse, vectors can be inserted into the
		// zombie partition, and then the partition can be re-deleted by another
		// racing worker running step #4, which can cause its vectors to disappear
		// forever.
		err = fw.removeFromParentPartition(ctx, parentPartitionKey, partitionKey, metadata.Level+1)
		if err != nil {
			return err
		}

		if fw.fp.onSuccessfulSplit != nil {
			// Notify listener that a partition has been successfully split.
			fw.fp.onSuccessfulSplit()
		}
	}

	if metadata.StateDetails.State == AddingLevelState {
		// Add the left and right target partitions to the root partition.
		err = fw.addTargetPartitionToRoot(ctx, leftPartitionKey, metadata, leftMetadata)
		if err != nil {
			return err
		}
		err = fw.addTargetPartitionToRoot(ctx, rightPartitionKey, metadata, rightMetadata)
		if err != nil {
			return err
		}

		// Move to the Ready state.
		expected := metadata
		metadata.StateDetails.MakeReady()
		err = fw.updateMetadata(ctx, partitionKey, metadata, expected)
		if err != nil {
			return err
		}

		if fw.fp.onSuccessfulSplit != nil {
			// Notify listener that a partition has been successfully split.
			fw.fp.onSuccessfulSplit()
		}
	}

	return nil
}

// reassignToSiblings checks if the vectors in a splitting partition need to be
// assigned to partitions other than the left and right target partitions. If a
// vector is closer to a sibling partition's centroid than it is to the left or
// right partitions' centroids, then it will be added to the sibling partition.
// It is also removed from "sourcePartition" and from "sourceVectors".
//
// reassignToSiblings returns the source vectors, minus any reassigned vectors.
func (fw *fixupWorker) reassignToSiblings(
	ctx context.Context,
	parentPartitionKey, sourcePartitionKey PartitionKey,
	sourcePartition *Partition,
	sourceVectors vector.Set,
	leftPartitionKey, rightPartitionKey PartitionKey,
) (vector.Set, error) {
	// No siblings if this is the root.
	if parentPartitionKey == InvalidKey {
		return sourceVectors, nil
	}

	// Fetch parent partition. If it does not exist, then it could be because
	// another agent completed the split or because the parent was itself split.
	// In either case, abort this split. If it's not yet done, it will be
	// restarted at a later time with the new parent.
	parentPartition, err := fw.getPartition(ctx, parentPartitionKey)
	if err != nil {
		return vector.Set{}, err
	}
	if parentPartition == nil {
		return vector.Set{}, errors.Wrapf(errFixupAborted,
			"parent partition %d of partition %d no longer exists",
			parentPartitionKey, sourcePartitionKey)
	}

	// Remove the splitting partition, since vectors cannot be assigned to it. If
	// it is not a child of the parent, then abort the split. This can happen if
	// another agent has completed the split or if the splitting partition has
	// been re-parented when a new level was added to the tree.
	if !parentPartition.ReplaceWithLastByKey(ChildKey{PartitionKey: sourcePartitionKey}) {
		return vector.Set{}, errors.Wrapf(errFixupAborted,
			"partition %d is no longer a child of parent partition %d",
			sourcePartitionKey, parentPartitionKey)
	}

	// Lazily get sibling metadata only if it's actually needed.
	fw.tempMetadataToGet = fw.tempMetadataToGet[:0]
	getSiblingMetadata := func() ([]PartitionMetadataToGet, error) {
		if len(fw.tempMetadataToGet) == 0 {
			fw.tempMetadataToGet = utils.EnsureSliceLen(fw.tempMetadataToGet, parentPartition.Count())
			for i := range len(fw.tempMetadataToGet) {
				fw.tempMetadataToGet[i].Key = parentPartition.ChildKeys()[i].PartitionKey
			}
			err = fw.index.store.TryGetPartitionMetadata(ctx, fw.treeKey, fw.tempMetadataToGet)
			if err != nil {
				return nil, errors.Wrapf(err,
					"getting partition metadata for %d siblings of partition %d (parent=%d)",
					len(fw.tempMetadataToGet)-1, sourcePartitionKey, parentPartitionKey)
			}
		}
		return fw.tempMetadataToGet, nil
	}

	tempSiblingDistances := fw.workspace.AllocFloats(parentPartition.Count())
	defer fw.workspace.FreeFloats(tempSiblingDistances)
	tempSiblingErrorBounds := fw.workspace.AllocFloats(parentPartition.Count())
	defer fw.workspace.FreeFloats(tempSiblingErrorBounds)

	for i := 0; i < sourceVectors.Count; i++ {
		// Check whether the vector is closer to a sibling centroid than its own
		// new centroid.
		vec := sourceVectors.At(i)
		parentPartition.Quantizer().EstimateDistances(&fw.workspace,
			parentPartition.QuantizedSet(), vec, tempSiblingDistances, tempSiblingErrorBounds)

		var leftDistance, rightDistance float32
		minDistance := float32(math.MaxFloat32)
		for offset, childKey := range parentPartition.ChildKeys() {
			minDistance = min(minDistance, tempSiblingDistances[offset])
			if childKey.PartitionKey == leftPartitionKey {
				leftDistance = tempSiblingDistances[offset]
			} else if childKey.PartitionKey == rightPartitionKey {
				rightDistance = tempSiblingDistances[offset]
			}
		}
		if minDistance >= leftDistance && minDistance >= rightDistance {
			// Could not find a closer sibling, so done with this vector.
			continue
		}

		// Lazily fetch metadata for sibling partitions.
		allSiblingMetadata, err := getSiblingMetadata()
		if err != nil {
			return vector.Set{}, err
		}

		// Find nearest sibling that allows inserts and is closer than either the
		// left or right target partitions.
		siblingOffset := -1
		minDistance = min(leftDistance, rightDistance)
		for offset, distance := range tempSiblingDistances {
			if distance >= minDistance {
				continue
			} else if !fw.tempMetadataToGet[offset].Metadata.StateDetails.State.AllowAdd() {
				continue
			}
			siblingOffset = offset
			minDistance = distance
		}
		if siblingOffset == -1 {
			// No closer sibling could be found, so return.
			continue
		}

		// Attempt to insert into the partition.
		siblingPartitionKey := parentPartition.ChildKeys()[siblingOffset].PartitionKey
		siblingMetadata := allSiblingMetadata[siblingOffset].Metadata
		childKey := sourcePartition.ChildKeys()[i : i+1]
		valueBytes := sourcePartition.ValueBytes()[i : i+1]
		_, err = fw.addToPartition(ctx, siblingPartitionKey, vec.AsSet(),
			childKey, valueBytes, siblingMetadata)
		if err != nil {
			allSiblingMetadata[siblingOffset].Metadata, err = suppressRaceErrors(err)
			if err == nil {
				// Another worker raced to update the metadata, so just skip.
				continue
			}
			return vector.Set{}, errors.Wrapf(err,
				"adding vector from splitting partition %d to partition %d",
				sourcePartitionKey, siblingPartitionKey)
		}

		// Add succeeded, so remove the vector from the splitting partition.
		sourceVectors.ReplaceWithLast(i)
		sourcePartition.ReplaceWithLast(i)
		i--

		log.VEventf(ctx, 3,
			"reassigning vector from splitting partition %d (parent=%d) to sibling partition %d",
			sourcePartitionKey, parentPartitionKey, siblingPartitionKey)
	}

	return sourceVectors, nil
}

// reassignFromSameLevel searches for vectors at the same level as a splitting
// partition (i.e. siblings and "cousins"). If a vector is closer to one of the
// split target partition centroids than it is to its own centroid, then it's
// moved to the target partition.
func (fw *fixupWorker) reassignFromSameLevel(
	ctx context.Context,
	sourcePartitionKey, leftPartitionKey, rightPartitionKey PartitionKey,
	leftMetadata, rightMetadata PartitionMetadata,
) error {
	doReassign := func(partitionKey PartitionKey, metadata PartitionMetadata) error {
		// Start transaction to search for vectors at the same level as the
		// splitting centroid.
		var results []SearchResult
		err := fw.index.store.RunTransaction(ctx, func(txn Txn) error {
			// Cosine and InnerProduct need a spherical centroid (normalized).
			centroid := metadata.Centroid
			if fw.index.quantizer.GetDistanceMetric() != vecpb.L2SquaredDistance {
				tempCentroid := fw.workspace.AllocVector(len(centroid))
				defer fw.workspace.FreeVector(tempCentroid)
				copy(tempCentroid, centroid)
				num32.Normalize(tempCentroid)
				centroid = tempCentroid
			}

			// Setup the search context. Use a larger beam size for the search for
			// a higher chance of finding vectors that need to move.
			fw.tempIndexCtx.Init(txn)
			fw.index.setupContext(&fw.tempIndexCtx, fw.treeKey, SearchOptions{
				BaseBeamSize: fw.index.options.BaseBeamSize * 2,
				SkipRerank:   true,
			}, metadata.Level, centroid, true /* transformed */)

			// Limit the max number of results to something reasonable.
			fw.tempIndexCtx.tempSearchSet.Init()
			maxResults := fw.index.options.MaxPartitionSize / 2
			if maxResults <= 0 {
				return nil
			}
			fw.tempIndexCtx.tempSearchSet.MaxResults = maxResults

			// Include distance of each result from its current centroid.
			fw.tempIndexCtx.tempSearchSet.IncludeCentroidDistances = true

			// Skip past the splitting partition and its two target partitions.
			fw.tempPartitionKeys[0] = sourcePartitionKey
			fw.tempPartitionKeys[1] = leftPartitionKey
			fw.tempPartitionKeys[2] = rightPartitionKey
			fw.tempIndexCtx.tempSearchSet.ExcludedPartitions = fw.tempPartitionKeys[:3]

			// Execute the search.
			err := fw.index.searchHelper(ctx, &fw.tempIndexCtx, &fw.tempIndexCtx.tempSearchSet)
			if err != nil {
				return errors.Wrapf(err,
					"searching for vectors at same level to reassign to target partition %d (level=%d)",
					partitionKey, metadata.Level)
			}

			// Filter out results with vectors that are closer to their own centroid
			// than they are to the splitting partition's centroid.
			results = fw.tempIndexCtx.tempSearchSet.PopResults()
			for i := 0; i < len(results); i++ {
				result := &results[i]

				if result.QueryDistance >= result.CentroidDistance {
					results = utils.ReplaceWithLast(results, i)
					i--
					continue
				}
			}
			if len(results) == 0 {
				return nil
			}

			// Get full vectors to reassign.
			results, err = fw.index.getFullVectors(ctx, &fw.tempIndexCtx, results)
			if err != nil {
				return errors.Wrapf(err, "getting full vectors to reassign to partition %d (level=%d)",
					partitionKey, metadata.Level)
			}

			return nil
		})
		if len(results) == 0 || err != nil {
			return err
		}

		// Move reassigned vectors to the target partition.
		tempVector := fw.workspace.AllocVector(fw.index.quantizer.GetDims())
		defer fw.workspace.FreeVector(tempVector)

		for i := range results {
			result := &results[i]
			log.VEventf(ctx, 3,
				"reassigning vector from partition %d (level=%d) to splitting target partition %d",
				result.ParentPartitionKey, metadata.Level, partitionKey)

			// Vector may need to be randomized and normalized.
			fw.transformFullVector(metadata.Level, result.Vector, tempVector)

			moved, err := fw.index.store.TryMoveVector(ctx,
				fw.treeKey, result.ParentPartitionKey, partitionKey,
				tempVector, result.ChildKey, result.ValueBytes, metadata)
			if err != nil {
				metadata, err = suppressRaceErrors(err)
				if err == nil {
					// Another agent has updated the target partition's state, so
					// stop trying to assign vectors to it.
					return nil
				}
				return errors.Wrapf(err,
					"reassigning vector from partition %d (level=%d) to splitting target partition %d",
					result.ParentPartitionKey, metadata.Level, partitionKey)
			}
			if moved && fw.singleStep {
				return errFixupAborted
			}
		}

		return nil
	}

	if err := doReassign(leftPartitionKey, leftMetadata); err != nil {
		return err
	}

	if err := doReassign(rightPartitionKey, rightMetadata); err != nil {
		return err
	}

	return nil
}

// getPartition returns the partition with the given key, or nil if it does not
// exist.
func (fw *fixupWorker) getPartition(
	ctx context.Context, partitionKey PartitionKey,
) (*Partition, error) {
	partition, err := fw.index.store.TryGetPartition(ctx, fw.treeKey, partitionKey)
	if err != nil {
		if errors.Is(err, ErrPartitionNotFound) {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "getting partition %d", partitionKey)
	}
	return partition, nil
}

// getPartitionMetadata returns the up-to-date metadata of the partition with
// the given key.
func (fw *fixupWorker) getPartitionMetadata(
	ctx context.Context, partitionKey PartitionKey,
) (PartitionMetadata, error) {
	fw.tempMetadataToGet = utils.EnsureSliceLen(fw.tempMetadataToGet, 1)
	fw.tempMetadataToGet[0].Key = partitionKey
	err := fw.index.store.TryGetPartitionMetadata(ctx, fw.treeKey, fw.tempMetadataToGet)
	if err != nil {
		return PartitionMetadata{}, errors.Wrapf(err, "getting metadata for partition %d", partitionKey)
	}
	return fw.tempMetadataToGet[0].Metadata, nil
}

// updateMetadata updates the given partition's metadata record, on the
// condition that its existing value matches the expected value. If not, it
// returns errFixupAborted.
func (fw *fixupWorker) updateMetadata(
	ctx context.Context, partitionKey PartitionKey, metadata, expected PartitionMetadata,
) error {
	err := fw.index.store.TryUpdatePartitionMetadata(
		ctx, fw.treeKey, partitionKey, metadata, expected)
	if err != nil {
		metadata, err = suppressRaceErrors(err)
		if err == nil {
			// Another worker raced to update the metadata, so abort.
			return errors.Wrapf(errFixupAborted, "updating partition %d expected %s, found %s",
				partitionKey, expected.StateDetails.String(), metadata.StateDetails.String())
		}
		return errors.Wrapf(err, "updating metadata for partition %d", partitionKey)
	}

	log.VEventf(ctx, 2, "updated partition %d from %s to %s",
		partitionKey, expected.StateDetails.String(), metadata.StateDetails.String())

	if fw.singleStep {
		return errFixupAborted
	}
	return nil
}

// addTargetPartitionToRoot adds a partition's key (and its centroid) to the
// root partition, on the condition that the root's metadata matches
// "rootMetadata". This is used when the root partition is being split, in the
// AddingLevel state.
func (fw *fixupWorker) addTargetPartitionToRoot(
	ctx context.Context, partitionKey PartitionKey, rootMetadata, metadata PartitionMetadata,
) error {
	if metadata.StateDetails.State == UpdatingState {
		// Add the target partition's key and centroid to the root partition.
		err := fw.addToParentPartition(ctx, RootKey, rootMetadata, metadata.Centroid, partitionKey)
		if err != nil {
			return err
		}

		// Change target partition's state from Updating to Ready.
		expected := metadata
		metadata.StateDetails.MakeReady()
		err = fw.updateMetadata(ctx, partitionKey, metadata, expected)
		if err != nil {
			return err
		}
	}

	return nil
}

// addToPartition adds vectors and associated data to the given partition, on
// the condition that its metadata matches the expected value. If not, it
// returns errFixupAborted. If at least one vector was added to the partition,
// it returns added=true.
func (fw *fixupWorker) addToPartition(
	ctx context.Context,
	partitionKey PartitionKey,
	vectors vector.Set,
	childKeys []ChildKey,
	valueBytes []ValueBytes,
	expected PartitionMetadata,
) (added bool, err error) {
	if !expected.StateDetails.State.AllowAdd() {
		return false, errors.AssertionFailedf(
			"cannot add to partition in state %s that disallows adds", expected.StateDetails.State)
	}
	fw.index.validateVectorsToAdd(expected.Level, vectors)

	added, err = fw.index.store.TryAddToPartition(ctx, fw.treeKey, partitionKey,
		vectors, childKeys, valueBytes, expected)
	if err != nil {
		metadata, err := suppressRaceErrors(err)
		if err == nil {
			// Another worker raced to update the metadata, so abort.
			return false, errors.Wrapf(errFixupAborted,
				"adding %d vectors to partition %d expected %s, found %s", vectors.Count, partitionKey,
				expected.StateDetails.String(), metadata.StateDetails.String())
		}
		return false, errors.Wrap(err, "adding to partition")
	} else if fw.singleStep && added {
		return true, errFixupAborted
	}

	return added, nil
}

// clearPartition removes all vectors from the given partition. This only
// happens if the partition's metadata has not changed. If it has changed,
// clearPartition returns errFixupAborted.
func (fw *fixupWorker) clearPartition(
	ctx context.Context, partitionKey PartitionKey, metadata PartitionMetadata,
) (err error) {
	if metadata.StateDetails.State.AllowAdd() {
		// Something's wrong if partition is being cleared in a state that allows
		// new vectors to be added.
		return errors.AssertionFailedf(
			"cannot add to partition in state %s that allows adds", metadata.StateDetails.State)
	}

	// Remove all children in the partition.
	count, err := fw.index.store.TryClearPartition(ctx, fw.treeKey, partitionKey, metadata)
	if err != nil {
		metadata, err = suppressRaceErrors(err)
		if err == nil {
			// Another worker raced to update the metadata, so abort.
			return errors.Wrapf(errFixupAborted,
				"clearing vectors from partition %d, expected %s, found %s",
				partitionKey, metadata.StateDetails.String(), metadata.StateDetails.String())
		}
		return errors.Wrap(err, "clearing vectors")
	}

	if count > 0 {
		log.VEventf(ctx, 2, "cleared %d vectors from partition %d (state=%s)",
			count, partitionKey, metadata.StateDetails.String())

		if fw.singleStep {
			return errFixupAborted
		}
	}

	return nil
}

// computeSplitCentroids uses K-means to separate vectors in the given partition
// into a left and right subset. It copies the centroids of each subset into
// caller-allocated "leftCentroid" and "rightCentroid". If "pinLeftCentroid" is
// true, then the left centroid is assumed to have already been calculated, and
// only the right centroid is calculated, based on the left centroid's value.
func (fw *fixupWorker) computeSplitCentroids(
	sourcePartition *Partition,
	vectors vector.Set,
	leftCentroid, rightCentroid vector.T,
	pinLeftCentroid bool,
) {
	// If the partition had tons of dangling references that need to be cleaned
	// up, or if Delete operations are racing with the split, we might not have
	// enough vectors to compute K-means centroids.
	// NOTE: It's fine if the centroids are approximate, e.g. in the case where
	// there are concurrent inserts or deletes.
	switch vectors.Count {
	case 0:
		// Use centroid from splitting partition.
		if !pinLeftCentroid {
			copy(leftCentroid, sourcePartition.Centroid())
		}
		copy(rightCentroid, sourcePartition.Centroid())

	case 1:
		// Use last remaining vector as the centroid.
		if !pinLeftCentroid {
			copy(leftCentroid, vectors.At(0))
		}
		copy(rightCentroid, vectors.At(0))

	default:
		// Compute centroids using K-means.
		kmeans := BalancedKmeans{
			Workspace:      &fw.workspace,
			Rand:           fw.rng,
			DistanceMetric: fw.index.quantizer.GetDistanceMetric(),
		}
		kmeans.ComputeCentroids(vectors, leftCentroid, rightCentroid, pinLeftCentroid)
	}
}

// createSplitSubPartition constructs one of the split target partitions, to
// which vectors from the splitting source partition will be copied. The created
// partition is initially empty and in the Updating state. It is added as a
// child of the given parent partition.
func (fw *fixupWorker) createSplitSubPartition(
	ctx context.Context,
	parentPartitionKey PartitionKey,
	sourcePartitionKey PartitionKey,
	sourceMetadata PartitionMetadata,
	partitionKey PartitionKey,
	centroid vector.T,
) (targetMetadata PartitionMetadata, err error) {
	defer func() {
		err = errors.Wrapf(err, "creating split target partition %d (source=%d, parent=%d)",
			partitionKey, sourcePartitionKey, parentPartitionKey)
	}()

	// Create an empty partition in the Updating state.
	targetMetadata = PartitionMetadata{
		Level:    sourceMetadata.Level,
		Centroid: centroid,
	}
	targetMetadata.StateDetails.MakeUpdating(sourcePartitionKey)
	err = fw.index.store.TryCreateEmptyPartition(ctx, fw.treeKey, partitionKey, targetMetadata)
	if err != nil {
		targetMetadata, err = suppressRaceErrors(err)
		centroid = targetMetadata.Centroid
		if err != nil {
			return PartitionMetadata{}, errors.Wrap(err, "creating empty target partition")
		}
	} else {
		log.VEventf(ctx, 2, "created split target partition %d (source=%d, parent=%d)",
			partitionKey, sourcePartitionKey, parentPartitionKey)

		if fw.singleStep {
			return PartitionMetadata{}, errFixupAborted
		}
	}

	// Ensure that the new target partition is linked into a parent partition.
	if targetMetadata.StateDetails.State == UpdatingState && parentPartitionKey != InvalidKey {
		// Load parent metadata to verify that it's in a state that allows inserts.
		parentMetadata, err := fw.getPartitionMetadata(ctx, parentPartitionKey)
		if err != nil {
			return PartitionMetadata{}, err
		}

		parentLevel := sourceMetadata.Level + 1
		if parentMetadata.StateDetails.State != ReadyState || parentMetadata.Level != parentLevel {
			// Only parent partitions in the Ready state at the expected level (level
			// can change after split/merge) allow children to be added.
			// TODO(andyk): Use parent state to identify alternate insert partition.
			return PartitionMetadata{}, errFixupAborted
		}

		err = fw.addToParentPartition(
			ctx, parentPartitionKey, parentMetadata, centroid, partitionKey)
		if err != nil {
			return PartitionMetadata{}, err
		}
	}

	return targetMetadata, nil
}

// addToParentPartition inserts a reference to a child partition into a parent
// partition with the given key and level. The vector inserted into the parent
// is the centroid of the child partition, and the child key is the key of the
// child partition. If the parent is not in a state that allows inserts, or if
// its level does not match the given level, then this fixup is aborted.
func (fw *fixupWorker) addToParentPartition(
	ctx context.Context,
	parentPartitionKey PartitionKey,
	parentMetadata PartitionMetadata,
	centroid vector.T,
	partitionKey PartitionKey,
) error {
	// Cosine and InnerProduct need to normalize centroids before adding them to a
	// partition.
	switch fw.index.quantizer.GetDistanceMetric() {
	case vecpb.CosineDistance, vecpb.InnerProductDistance:
		tempCentroid := fw.workspace.AllocVector(len(centroid))
		defer fw.workspace.FreeVector(tempCentroid)
		copy(tempCentroid, centroid)
		num32.Normalize(tempCentroid)
		centroid = tempCentroid
	}

	// Add the target partition key to the parent partition.
	fw.tempChildKey[0] = ChildKey{PartitionKey: partitionKey}
	fw.tempValueBytes[0] = nil
	added, err := fw.addToPartition(ctx, parentPartitionKey,
		centroid.AsSet(), fw.tempChildKey[:1], fw.tempValueBytes[:1], parentMetadata)
	if added {
		log.VEventf(ctx, 2,
			"added centroid for partition %d to parent partition %d (level=%d, state=%s)",
			partitionKey, parentPartitionKey, parentMetadata.Level, parentMetadata.StateDetails.String())
	}
	if err != nil {
		return errors.Wrapf(err,
			"adding centroid for partition %d to parent partition %d (level=%d, state=%s)",
			partitionKey, parentPartitionKey, parentMetadata.Level, parentMetadata.StateDetails.String())
	}

	return nil
}

// removeFromParentPartition removes any reference to a child partition from the
// parent partition with the given key and level. If the parent is not in a
// state that allows inserts, or if its level does not match the given level,
// then this fixup is aborted.
func (fw *fixupWorker) removeFromParentPartition(
	ctx context.Context, parentPartitionKey, partitionKey PartitionKey, parentLevel Level,
) (err error) {
	var parentMetadata PartitionMetadata

	defer func() {
		err = errors.Wrapf(err, "removing partition %d from parent partition %d (level=%d, state=%s)",
			partitionKey, parentPartitionKey, parentLevel, parentMetadata.StateDetails.String())
	}()

	// Load parent metadata to verify that it's in a state that allows deletes.
	parentMetadata, err = fw.getPartitionMetadata(ctx, parentPartitionKey)
	if err != nil {
		return errors.Wrapf(err, "getting parent partition %d metadata", parentPartitionKey)
	}

	if parentMetadata.StateDetails.State.CanSkipRemove() || parentMetadata.Level != parentLevel {
		// Child should not be removed from the parent that's draining or will be
		// deleted, or if its level has changed (i.e. in root partition case).
		return errFixupAborted
	}

	// Remove the partition from its parent.
	fw.tempChildKey[0] = ChildKey{PartitionKey: partitionKey}
	removed, err := fw.index.store.TryRemoveFromPartition(
		ctx, fw.treeKey, parentPartitionKey, fw.tempChildKey[:1], parentMetadata)
	if err != nil {
		parentMetadata, err = suppressRaceErrors(err)
		if err == nil {
			// Another worker raced and updated the metadata, so abort.
			// TODO(andyk): Use parent state to identify alternate insert partition.
			return errFixupAborted
		}
		return errors.Wrap(err, "removing partition from parent")
	}

	if removed {
		log.VEventf(ctx, 2, "removed partition %d (parent=%d, state=%s)",
			partitionKey, parentPartitionKey, parentMetadata.StateDetails.String())

		if fw.singleStep {
			return errFixupAborted
		}
	}

	return nil
}

// copyToSplitSubPartitions copies the given set of vectors to left and right
// target partitions, based on which centroid they're closer to.
func (fw *fixupWorker) copyToSplitSubPartitions(
	ctx context.Context,
	sourcePartition *Partition,
	vectors vector.Set,
	leftMetadata, rightMetadata PartitionMetadata,
) (err error) {
	var leftCount int
	sourceState := sourcePartition.Metadata().StateDetails

	defer func() {
		err = errors.Wrapf(err,
			"assigning %d vectors to left partition %d and %d vectors to right partition %d",
			leftCount, sourceState.Target1, vectors.Count-leftCount, sourceState.Target2)
	}()

	tempAssignments := fw.workspace.AllocUint64s(vectors.Count)
	defer fw.workspace.FreeUint64s(tempAssignments)

	// Assign vectors to the partition with the nearest centroid.
	kmeans := BalancedKmeans{Workspace: &fw.workspace, Rand: fw.rng}
	leftCount = kmeans.AssignPartitions(
		vectors, leftMetadata.Centroid, rightMetadata.Centroid, tempAssignments)

	// Assign vectors and associated keys and values into contiguous left and right groupings.
	childKeys := slices.Clone(sourcePartition.ChildKeys())
	valueBytes := slices.Clone(sourcePartition.ValueBytes())
	splitPartitionData(&fw.workspace, vectors, childKeys, valueBytes, tempAssignments)
	leftVectors := vectors
	rightVectors := leftVectors.SplitAt(leftCount)
	leftChildKeys := childKeys[:leftCount]
	rightChildKeys := childKeys[leftCount:]
	leftValueBytes := valueBytes[:leftCount]
	rightValueBytes := valueBytes[leftCount:]

	// Add vectors to left and right target partitions. Note that this may not be
	// transactional; if an error occurs, any vectors already added may not be
	// rolled back. This is OK, since the vectors are still present in the
	// source partition.
	leftPartitionKey := sourceState.Target1
	added, err := fw.addToPartition(ctx,
		leftPartitionKey, leftVectors, leftChildKeys, leftValueBytes, leftMetadata)
	if added {
		log.VEventf(ctx, 2, "assigned %d vectors to left partition %d (level=%d, state=%s)",
			leftCount, leftPartitionKey, leftMetadata.Level, leftMetadata.StateDetails.String())
	}
	if err != nil {
		return err
	}

	rightPartitionKey := sourceState.Target2
	added, err = fw.addToPartition(ctx,
		rightPartitionKey, rightVectors, rightChildKeys, rightValueBytes, rightMetadata)
	if added {
		log.VEventf(ctx, 2, "assigned %d vectors to right partition %d (level=%d, state=%s)",
			vectors.Count-leftCount, rightPartitionKey,
			rightMetadata.Level, rightMetadata.StateDetails.String())
	}
	if err != nil {
		return err
	}

	return nil
}

// suppressRaceErrors suppresses two kinds of errors that can result from
// another worker assisting with a fixup operation:
//
//   - ErrPartitionNotFound: This can happen when another worker has deleted a
//     partition as part of a split or merge. This returns partition metadata set
//     to the Missing state.
//   - ConditionFailedError: This can happen when another worker has updated the
//     partition's metadata. This returns the most up-to-date metadata for the
//     partition.
func suppressRaceErrors(err error) (PartitionMetadata, error) {
	var errConditionFailed *ConditionFailedError
	if errors.Is(err, ErrPartitionNotFound) {
		return PartitionMetadata{}, nil
	} else if errors.As(err, &errConditionFailed) {
		return errConditionFailed.Actual, nil
	}
	return PartitionMetadata{}, err
}

// splitPartitionData groups the provided partition data according to the left
// and right offsets. The assignments slice specifies which partition the data
// will be moved into: 0 for left and 1 for right. The internal ordering of
// elements on each side is not defined.
func splitPartitionData(
	w *workspace.T,
	vectors vector.Set,
	childKeys []ChildKey,
	valueBytes []ValueBytes,
	assignments []uint64,
) {
	tempVector := w.AllocFloats(vectors.Dims)
	defer w.FreeFloats(tempVector)

	// Use a two-pointer approach to partition the data. left points to the next
	// position where a left element should go. right points to the next position
	// where a right element should go (from the end).
	left := 0
	right := len(assignments) - 1

	for {
		// Find a misplaced element on the left side (should be 0 but is 1).
		for left < right && assignments[left] == 0 {
			left++
		}

		// Find a misplaced element on the right side (should be 1 but is 0).
		for left < right && assignments[right] == 1 {
			right--
		}

		if left >= right {
			// No more misplaced elements, so break.
			break
		}

		// Swap vectors.
		leftVector := vectors.At(left)
		rightVector := vectors.At(right)
		copy(tempVector, leftVector)
		copy(leftVector, rightVector)
		copy(rightVector, tempVector)

		// Swap child keys.
		childKeys[left], childKeys[right] = childKeys[right], childKeys[left]

		// Swap value bytes.
		valueBytes[left], valueBytes[right] = valueBytes[right], valueBytes[left]

		left++
		right--
	}
}
