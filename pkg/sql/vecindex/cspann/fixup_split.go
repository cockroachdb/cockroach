// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"context"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
// TODO(andyk): This will need to be updated to handle moving vectors to/from
// sibling/cousin partitions.
//
//  1. Update the splitting partition's state from Ready to Splitting. Allocate
//     keys for two target sub-partitions where vectors will be copied.
//  2. Create an empty left sub-partition in state Updating, with its source set
//     to the splitting partition. Use K-means to compute its centroid from the
//     subset of vectors it is expected to contain (note that the actual subset
//     may shift if there are concurrent updates).
//  3. Add the left sub-partition to the splitting partition's parent.
//  4. Similarly, create an empty right sub-partition.
//  5. Add the right sub-partition to the parent.
//  6. Update the splitting partition's state from Splitting to
//     DrainingForSplit. Vectors cannot be inserted or deleted in this state,
//     but instead are "forwarded" to the closest target sub-partition.
//  7. Reload the splitting partition's vectors and copy the "left" subset to
//     the left sub-partition.
//  8. Update the left sub-partition's state from Updating to Ready.
//  9. Copy the "right" subset of vectors to the right sub-partition.
//  10. Update the right sub-partition's state from Updating to Ready. At this
//     point, the splitting vectors are duplicated in the index. Any searches
//     will filter out duplicates.
//  11. Remove the splitting partition from its parent. The duplicates are no
//     longer visible to searches.
//  12. Delete the splitting partition from the index.
//
// Splitting a root partition changes the above flow in the following ways:
//
//	a. The root partition does not have a parent, so step #3 is skipped.
//	b. Similarly, step #5 is skipped.
//	c. The root partition cannot be removed from its parent and is not deleted,
//	   so steps #11 and #12 are skipped.
//	d. Instead, all vectors in the root partition are cleared so that it is
//	   temporarily empty. Searches can still find these vectors because they
//	   were copied to the left and right sub-partitions in steps #7 and #9.
//	   Searches can find the keys of the sub-partitions in the Target fields of
//	   the root partition metadata.
//	e. Update the root partition's state to AddingLevel and increase its level
//	   by one.
//	f. Add the left sub-partition as a child of the root partition.
//	g. Add the right sub-partition as a child of the root partition.
//	h. Update the root partition's state to Ready.
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
// |    Missing     |      |  AddingLevel   |
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

	if metadata.Level != LeafLevel && partition.Count() == 0 {
		if partitionKey != RootKey || metadata.StateDetails.State == ReadyState {
			// Something's terribly wrong, abort and hope that merge can clean this up.
			return errors.AssertionFailedf("non-leaf partition %d (state=%s) should not have 0 vectors",
				partitionKey, metadata.StateDetails.State.String())
		}
	}

	log.VEventf(ctx, 2, "splitting partition %d with %d vectors (parent=%d, state=%s)",
		partitionKey, parentPartitionKey, partition.Count(), metadata.StateDetails.String())

	// Update partition's state to Splitting.
	if metadata.StateDetails.State == ReadyState {
		expected := metadata
		metadata.StateDetails = MakeSplittingDetails(
			fw.index.store.MakePartitionKey(), fw.index.store.MakePartitionKey())
		err = fw.updateMetadata(ctx, partitionKey, metadata, expected)
		if err != nil {
			return err
		}
		partition.Metadata().StateDetails = metadata.StateDetails
	}

	// Create target sub-partitions.
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

		// Compute centroids for new sub-partitions.
		leftCentroid := make(vector.T, fw.index.quantizer.GetDims())
		rightCentroid := make(vector.T, fw.index.quantizer.GetDims())
		fw.computeSplitCentroids(
			partition, vectors, leftCentroid, rightCentroid, false /* pinLeftCentroid */)

		// Create empty sub-partitions.
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
		metadata.StateDetails = MakeDrainingForSplitDetails(leftPartitionKey, rightPartitionKey)
		err = fw.updateMetadata(ctx, partitionKey, metadata, expected)
		if err != nil {
			return err
		}
		partition.Metadata().StateDetails = metadata.StateDetails

		// Reload the partition, in case new vectors have been added to it while
		// creating the sub-partitions. Now that we're in the DrainingForSplit
		// state, no new vectors can be added to this partition going forward.
		partition, err = fw.getPartition(ctx, partitionKey)
		if err != nil || partition == nil {
			return err
		}
	} else if metadata.StateDetails.State == DrainingForSplitState {
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

		// Add vectors to nearest partition.
		err = fw.copyToSplitSubPartitions(ctx, partition, vectors, leftMetadata, rightMetadata)
		if err != nil {
			return err
		}

		// Check whether the splitting partition is the root.
		if parentPartitionKey != InvalidKey {
			// The source partition has been drained, so remove it from its parent
			// and delete it.
			err = fw.deletePartition(ctx, parentPartitionKey, partitionKey)
			if err != nil {
				return err
			}
		} else {
			// This is the root partition, so remove all of its vectors rather than
			// delete the root partition itself. Note that the vectors have already
			// been copied to the two target partitions.
			err = fw.clearPartition(ctx, partitionKey, partition)
			if err != nil {
				return err
			}

			// Increase level by one and move to the AddingLevel state.
			expected := metadata
			metadata.Level++
			metadata.StateDetails = MakeAddingLevelDetails(leftPartitionKey, rightPartitionKey)
			err = fw.updateMetadata(ctx, partitionKey, metadata, expected)
			if err != nil {
				return err
			}
		}
	}

	if metadata.StateDetails.State == AddingLevelState {
		fw.tempChildKey[0] = ChildKey{PartitionKey: leftPartitionKey}
		fw.tempValueBytes[0] = nil
		err = fw.addToPartition(ctx, partitionKey,
			leftMetadata.Centroid.AsSet(), fw.tempChildKey[:1], fw.tempValueBytes[:1], metadata)
		if err != nil {
			return err
		}

		fw.tempChildKey[0] = ChildKey{PartitionKey: rightPartitionKey}
		fw.tempValueBytes[0] = nil
		err = fw.addToPartition(ctx, partitionKey,
			rightMetadata.Centroid.AsSet(), fw.tempChildKey[:1], fw.tempValueBytes[:1], metadata)
		if err != nil {
			return err
		}

		// Move to the Ready state.
		expected := metadata
		metadata.StateDetails = MakeReadyDetails()
		err = fw.updateMetadata(ctx, partitionKey, metadata, expected)
		if err != nil {
			return err
		}
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
	metadata, err := fw.index.store.TryGetPartitionMetadata(ctx, fw.treeKey, partitionKey)
	if err != nil {
		metadata, err = suppressRaceErrors(err)
		if err != nil {
			return PartitionMetadata{}, errors.Wrapf(err, "getting metadata for partition %d", partitionKey)
		}
	}
	return metadata, nil
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
	} else if fw.singleStep {
		return errFixupAborted
	}
	return nil
}

// addToPartition adds vectors and associated data to the given partition, on
// the condition that its metadata matches the expected value. If not, it
// returns errFixupAborted.
func (fw *fixupWorker) addToPartition(
	ctx context.Context,
	partitionKey PartitionKey,
	vectors vector.Set,
	childKeys []ChildKey,
	valueBytes []ValueBytes,
	expected PartitionMetadata,
) error {
	if !expected.StateDetails.State.AllowAddOrRemove() {
		return errors.AssertionFailedf("cannot add to partition in state that disallows adds/removes")
	}

	added, err := fw.index.store.TryAddToPartition(ctx, fw.treeKey, partitionKey,
		vectors, childKeys, valueBytes, expected)
	if err != nil {
		metadata, err := suppressRaceErrors(err)
		if err == nil {
			// Another worker raced to update the metadata, so abort.
			return errors.Wrapf(errFixupAborted,
				"adding %d vectors to partition %d expected %s, found %s", vectors.Count, partitionKey,
				expected.StateDetails.String(), metadata.StateDetails.String())
		}
		return errors.Wrap(err, "adding to partition")
	} else if fw.singleStep && added {
		return errFixupAborted
	}
	return nil
}

// clearPartition removes all vectors and associated data from the given
// partition, leaving it empty, on the condition that the partition's state has
// not changed unexpectedly. If that's the case, it returns errFixupAborted.
func (fw *fixupWorker) clearPartition(
	ctx context.Context, partitionKey PartitionKey, partition *Partition,
) (err error) {
	if partition.Metadata().StateDetails.State.AllowAddOrRemove() {
		return errors.AssertionFailedf("cannot clear partition in state that allows adds/removes")
	}

	// Remove all children in the partition.
	removed, err := fw.index.store.TryRemoveFromPartition(ctx, fw.treeKey,
		partitionKey, partition.ChildKeys(), *partition.Metadata())
	if err != nil {
		metadata, err := suppressRaceErrors(err)
		if err == nil {
			// Another worker raced to update the metadata, so abort.
			return errors.Wrapf(errFixupAborted,
				"clearing % vectors from partition, %d expected %s, found %s", partition.Count(),
				partitionKey, metadata.StateDetails.String(), metadata.StateDetails.String())
		}
		return errors.Wrap(err, "clearing vectors")
	} else if fw.singleStep && removed {
		return errFixupAborted
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
		tempOffsets := fw.workspace.AllocUint64s(vectors.Count)
		defer fw.workspace.FreeUint64s(tempOffsets)
		kmeans := BalancedKmeans{Workspace: &fw.workspace, Rand: fw.rng}
		kmeans.ComputeCentroids(vectors, leftCentroid, rightCentroid, pinLeftCentroid, tempOffsets)
	}
}

// createSplitSubPartition constructs one of the split target sub-partitions, to
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
	const format = "creating split sub-partition %d, with parent %d"

	defer func() {
		err = errors.Wrapf(err, format, partitionKey, parentPartitionKey)
	}()

	log.VEventf(ctx, 2, format, partitionKey, parentPartitionKey)

	// Create an empty partition in the Updating state.
	targetMetadata = PartitionMetadata{
		Level:        sourceMetadata.Level,
		Centroid:     centroid,
		StateDetails: MakeUpdatingDetails(sourcePartitionKey),
	}
	err = fw.index.store.TryCreateEmptyPartition(ctx, fw.treeKey, partitionKey, targetMetadata)
	if err != nil {
		targetMetadata, err = suppressRaceErrors(err)
		if err != nil {
			return PartitionMetadata{}, errors.Wrap(err, "creating empty sub-partition")
		}
	} else if fw.singleStep {
		return PartitionMetadata{}, errFixupAborted
	}

	// Ensure that the new sub-partition is linked into a parent partition.
	if targetMetadata.StateDetails.State == UpdatingState && parentPartitionKey != InvalidKey {
		err = fw.addToParentPartition(
			ctx, parentPartitionKey, partitionKey, sourceMetadata.Level+1, centroid)
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
	parentPartitionKey, partitionKey PartitionKey,
	parentLevel Level,
	centroid vector.T,
) (err error) {
	const format = "adding partition %d to parent partition %d (level=%d, state=%s)"
	var parentMetadata PartitionMetadata

	defer func() {
		err = errors.Wrapf(err, format,
			partitionKey, parentPartitionKey, parentMetadata.Level, parentMetadata.StateDetails.State)
	}()

	// Load parent parentPartition to verify that it's in a state that allows
	// inserts.
	var parentPartition *Partition
	parentPartition, err = fw.getPartition(ctx, parentPartitionKey)
	if err != nil {
		return errors.Wrapf(err, "getting parent partition")
	}
	if parentPartition != nil {
		parentMetadata = *parentPartition.Metadata()
	}

	log.VEventf(ctx, 2, format,
		partitionKey, parentPartitionKey, parentMetadata.Level, parentMetadata.StateDetails.State)

	if parentMetadata.StateDetails.State != ReadyState || parentMetadata.Level != parentLevel {
		// Only parent partitions in the Ready state at the expected level (level
		// can change after split/merge) allow children to be added.
		// TODO(andyk): Use parent state to identify alternate insert partition.
		return errFixupAborted
	}

	// Parent partition is ready, so try to add to it.
	fw.tempChildKey[0] = ChildKey{PartitionKey: partitionKey}
	fw.tempValueBytes[0] = nil
	err = fw.addToPartition(ctx, parentPartitionKey,
		centroid.AsSet(), fw.tempChildKey[:1], fw.tempValueBytes[:1], parentMetadata)
	if err != nil {
		return err
	}

	return nil
}

// deletePartition removes the given partition from its parent and then deletes
// the partition. If the parent is not in a state that allows inserts, then this
// fixup is aborted.
func (fw *fixupWorker) deletePartition(
	ctx context.Context, parentPartitionKey, partitionKey PartitionKey,
) (err error) {
	const format = "deleting partition %d, with parent partition %d (state=%d)"
	var parentMetadata PartitionMetadata

	defer func() {
		err = errors.Wrapf(err, format,
			partitionKey, parentPartitionKey, parentMetadata.StateDetails.State)
	}()

	// Load parent partition to verify that it's in a state that allows inserts.
	var parentPartition *Partition
	parentPartition, err = fw.getPartition(ctx, parentPartitionKey)
	if err != nil {
		return err
	}
	if parentPartition != nil {
		parentMetadata = *parentPartition.Metadata()
	}

	log.VEventf(ctx, 2, format, partitionKey, parentPartitionKey, parentMetadata.StateDetails.State)

	if !parentMetadata.StateDetails.State.AllowAddOrRemove() {
		// Child could not be removed from the parent because it doesn't exist or
		// it no longer allows deletes.
		// TODO(andyk): Use parent state to identify alternate insert partition.
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

	// Delete the partition, ignoring any partition not found error, as it means
	// another worker already deleted the partition. Do not delete the partition
	// unless it was successfully removed from its parent, as otherwise we might
	// delete a partition that has been re-parented, leaving the parent with a
	// dangling reference.
	//
	// TODO(andyk): If the worker crashes or errors after removing this partition
	// from its parent but before deleting it, it won't be cleaned up. This should
	// be extremely rare, and therefore waste a tiny amount of storage, so it's
	// probably not worth addressing.
	if removed {
		err = fw.index.store.TryDeletePartition(ctx, fw.treeKey, partitionKey)
		if err != nil {
			if !errors.Is(err, ErrPartitionNotFound) {
				return errors.Wrapf(err, "deleting partition")
			}
		} else if fw.singleStep {
			return errFixupAborted
		}
	}

	return nil
}

// copyToSplitSubPartitions copies the given set of vectors to left and right
// sub-partitions, based on which centroid they're closer to. It also updates
// the state of each sub-partition from Updating to Ready.
func (fw *fixupWorker) copyToSplitSubPartitions(
	ctx context.Context,
	sourcePartition *Partition,
	vectors vector.Set,
	leftMetadata, rightMetadata PartitionMetadata,
) (err error) {
	const format = "assigning %d vectors to left partition %d and %d vectors to right partition %d"

	var leftOffsets, rightOffsets []uint64
	sourceState := sourcePartition.Metadata().StateDetails

	defer func() {
		err = errors.Wrapf(err, format,
			len(leftOffsets), sourceState.Target1, len(rightOffsets), sourceState.Target2)
	}()

	tempOffsets := fw.workspace.AllocUint64s(vectors.Count)
	defer fw.workspace.FreeUint64s(tempOffsets)

	// Assign vectors to the partition with the nearest centroid.
	kmeans := BalancedKmeans{Workspace: &fw.workspace, Rand: fw.rng}
	leftOffsets, rightOffsets = kmeans.AssignPartitions(
		vectors, leftMetadata.Centroid, rightMetadata.Centroid, tempOffsets)

	// Sort vectors into contiguous left and right groupings.
	sortVectors(&fw.workspace, vectors, leftOffsets, rightOffsets)
	leftVectors := vectors
	rightVectors := leftVectors.SplitAt(len(leftOffsets))

	childKeys := make([]ChildKey, vectors.Count)
	valueBytes := make([]ValueBytes, vectors.Count)
	leftChildKeys := copyByOffsets(
		sourcePartition.ChildKeys(), childKeys[:len(leftOffsets)], leftOffsets)
	rightChildKeys := copyByOffsets(
		sourcePartition.ChildKeys(), childKeys[len(leftOffsets):], rightOffsets)
	leftValueBytes := copyByOffsets(
		sourcePartition.ValueBytes(), valueBytes[:len(leftOffsets)], leftOffsets)
	rightValueBytes := copyByOffsets(
		sourcePartition.ValueBytes(), valueBytes[len(leftOffsets):], rightOffsets)

	log.VEventf(ctx, 2, format,
		len(leftOffsets), sourceState.Target1, len(rightOffsets), sourceState.Target2)

	// Add vectors to left and right sub-partitions. Note that this may not be
	// transactional; if an error occurs, any vectors already added may not be
	// rolled back. This is OK, since the vectors are still present in the
	// source partition.
	if leftMetadata.StateDetails.State == UpdatingState {
		leftPartitionKey := sourceState.Target1
		err = fw.copyVectorsToSubPartition(ctx,
			leftPartitionKey, leftMetadata, leftVectors, leftChildKeys, leftValueBytes)
		if err != nil {
			return err
		}
	}
	if rightMetadata.StateDetails.State == UpdatingState {
		if sourcePartition.Level() != LeafLevel && vectors.Count == 1 {
			// This should have been a merge, not a split, but we're too far into the
			// split operation to back out now, so avoid an empty non-root partition by
			// duplicating the last remaining vector in both partitions.
			rightVectors = leftVectors
			rightChildKeys = leftChildKeys
			rightValueBytes = leftValueBytes
		}

		rightPartitionKey := sourceState.Target2
		err = fw.copyVectorsToSubPartition(ctx,
			rightPartitionKey, rightMetadata, rightVectors, rightChildKeys, rightValueBytes)
		if err != nil {
			return err
		}
	}

	return nil
}

// copyVectorsToSubPartition copies the given set of vectors, along with
// associated keys and values, to a split sub-partition with the given key. The
// vectors will only be added if the partition's metadata matches the expected
// value. It also updates the state of the partition from Updating to Ready.
func (fw *fixupWorker) copyVectorsToSubPartition(
	ctx context.Context,
	partitionKey PartitionKey,
	metadata PartitionMetadata,
	vectors vector.Set,
	childKeys []ChildKey,
	valueBytes []ValueBytes,
) error {
	// Add vectors to sub-partition, as long as metadata matches.
	err := fw.addToPartition(ctx, partitionKey, vectors, childKeys, valueBytes, metadata)
	if err != nil {
		return err
	}

	// Update partition state from Updating to Ready.
	expected := metadata
	metadata.StateDetails = MakeReadyDetails()
	err = fw.updateMetadata(ctx, partitionKey, metadata, expected)
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

// sortVectors sorts the input vectors in-place, according to the provided left
// and right offsets, which reference vectors by position. Vectors at left
// offsets are sorted at the beginning of the slice, followed by vectors at
// right offsets. The internal ordering among left and right vectors is not
// defined.
//
// NOTE: The left and right offsets are modified in-place with the updated
// positions of the vectors.
func sortVectors(w *workspace.T, vectors vector.Set, leftOffsets, rightOffsets []uint64) {
	tempVector := w.AllocFloats(vectors.Dims)
	defer w.FreeFloats(tempVector)

	// Sort left and right offsets.
	slices.Sort(leftOffsets)
	slices.Sort(rightOffsets)

	// Any left offsets that point beyond the end of the left list indicate that
	// a vector needs to be moved from the right half of vectors to the left half.
	// The reverse is true for right offsets. Because the left and right offsets
	// are in sorted order, out-of-bounds offsets must be at the end of the left
	// list and the beginning of the right list. Therefore, the algorithm just
	// needs to iterate over those out-of-bounds offsets and swap the positions
	// of the referenced vectors.
	li := len(leftOffsets) - 1
	ri := 0

	var rightToLeft, leftToRight vector.T
	for li >= 0 {
		left := int(leftOffsets[li])
		if left < len(leftOffsets) {
			break
		}

		right := int(rightOffsets[ri])
		if right >= len(leftOffsets) {
			panic(errors.AssertionFailedf(
				"expected equal number of left and right offsets that need to be swapped"))
		}

		// Swap vectors.
		rightToLeft = vectors.At(left)
		leftToRight = vectors.At(right)
		copy(tempVector, rightToLeft)
		copy(rightToLeft, leftToRight)
		copy(leftToRight, tempVector)

		leftOffsets[li] = uint64(left)
		rightOffsets[ri] = uint64(right)

		li--
		ri++
	}
}

func copyByOffsets[T any](source, target []T, offsets []uint64) []T {
	for i := range offsets {
		target[i] = source[offsets[i]]
	}
	return target
}
