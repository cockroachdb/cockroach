// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import (
	"context"
	"math/rand"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/internal"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecstore"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// fixupWorker is a background worker that processes queued fixups. Multiple
// workers can run at a time in parallel. Once started, a worker continually
// processes fixups until its context is canceled.
type fixupWorker struct {
	// fp points back to the processor that spawned this worker.
	fp *fixupProcessor
	// index points back to the vector index to which fixups are applied.
	index *VectorIndex
	// rng is a random number generator. If nil, then the global random number
	// generator will be used.
	rng *rand.Rand
	// workspace is used to stack-allocate temporary memory.
	workspace internal.Workspace
	// searchCtx is reused to perform index searches and inserts.
	searchCtx searchContext

	// tempVectorsWithKeys is temporary memory for vectors and their keys.
	tempVectorsWithKeys []vecstore.VectorWithKey
}

// NewFixupWorker returns a new worker for the given processor.
func NewFixupWorker(fp *fixupProcessor) *fixupWorker {
	worker := &fixupWorker{
		fp:    fp,
		index: fp.index,
	}
	if fp.seed != 0 {
		// Create a random number generator for the background goroutine.
		worker.rng = rand.New(rand.NewSource(fp.seed))
	}
	return worker
}

// Start continually processes queued fixups until the context is canceled.
func (fw *fixupWorker) Start(ctx context.Context) {
	ctx = internal.WithWorkspace(ctx, &fw.workspace)

	for {
		next, ok := fw.fp.nextFixup(ctx)
		if !ok {
			break
		}

		// Invoke the fixup function. Note that we do not hold the lock while
		// processing the fixup.
		var err error
		switch next.Type {
		case splitOrMergeFixup:
			err = fw.splitOrMergePartition(ctx, next.ParentPartitionKey, next.PartitionKey)
			if err != nil {
				err = errors.Wrapf(err, "splitting partition %d", next.PartitionKey)
			}

		case vectorDeleteFixup:
			err = fw.deleteVector(ctx, next.PartitionKey, next.VectorKey)
			if err != nil {
				err = errors.Wrap(err, "deleting vector")
			}
		}

		if err != nil {
			// This is a background goroutine, so just log error and continue.
			log.Errorf(ctx, "fixup processor error: %v", err)
		}

		// Delete already-processed fixup from its pending map, even if the fixup
		// failed, in order to avoid looping over the same fixup.
		fw.fp.removeFixup(next)
	}
}

// splitOrMergePartition splits or merges the partition with the given key and
// parent key, depending on whether it's over-sized or under-sized. This
// operation runs in its own transaction.
func (fw *fixupWorker) splitOrMergePartition(
	ctx context.Context, parentPartitionKey vecstore.PartitionKey, partitionKey vecstore.PartitionKey,
) (err error) {
	// Run the split or merge within a transaction.
	txn, err := fw.index.store.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = fw.index.store.Commit(ctx, txn)
		} else {
			err = errors.CombineErrors(err, fw.index.store.Abort(ctx, txn))
		}
	}()

	// Get the partition to be split or merged from the store.
	partition, err := txn.GetPartition(ctx, partitionKey)
	if errors.Is(err, vecstore.ErrPartitionNotFound) {
		log.VEventf(ctx, 2, "partition %d no longer exists, do not split or merge", partitionKey)
		return nil
	} else if err != nil {
		return errors.Wrapf(err, "getting partition %d to split or merge", partitionKey)
	}

	// Don't split or merge the partition if its size is within bounds.
	split := partition.Count() > fw.index.options.MaxPartitionSize
	merge := partition.Count() < fw.index.options.MinPartitionSize
	if !split && !merge {
		log.VEventf(ctx, 2, "partition %d size is within bounds, do not split or merge", partitionKey)
		return nil
	}

	// Load the parent of the partition to split or merge.
	var parentPartition *vecstore.Partition
	if parentPartitionKey != vecstore.InvalidKey {
		parentPartition, err = txn.GetPartition(ctx, parentPartitionKey)
		if errors.Is(err, vecstore.ErrPartitionNotFound) {
			log.VEventf(ctx, 2,
				"parent partition %d of partition %d no longer exists, do not split or merge",
				parentPartitionKey, partitionKey)
			return nil
		} else if err != nil {
			return errors.Wrapf(err, "getting parent %d of partition %d to split or merge",
				parentPartitionKey, partitionKey)
		}

		// Don't split or merge the partition if no longer a child of the parent.
		if parentPartition.Find(vecstore.ChildKey{PartitionKey: partitionKey}) == -1 {
			log.VEventf(ctx, 2, "partition %d is no longer child of partition %d, do not split or merge",
				partitionKey, parentPartitionKey)
			return nil
		}
	}

	// Get the full vectors for the splitting or merging partition's children.
	vectors, err := fw.getFullVectorsForPartition(ctx, txn, partitionKey, partition)
	if err != nil {
		return errors.Wrapf(
			err, "getting full vectors for split or merge of partition %d", partitionKey)
	}

	if split {
		return fw.splitPartition(
			ctx, txn, parentPartitionKey, parentPartition, partitionKey, partition, vectors)
	}
	return fw.mergePartition(
		ctx, txn, parentPartitionKey, parentPartition, partitionKey, partition, vectors)
}

// splitPartition splits the given partition by separating its vectors into one
// of two new replacement partitions.
func (fw *fixupWorker) splitPartition(
	ctx context.Context,
	txn vecstore.Txn,
	parentPartitionKey vecstore.PartitionKey,
	parentPartition *vecstore.Partition,
	partitionKey vecstore.PartitionKey,
	partition *vecstore.Partition,
	vectors vector.Set,
) (err error) {
	if vectors.Count < 2 {
		// This could happen if the partition had tons of dangling references that
		// need to be cleaned up.
		// TODO(andyk): We might consider cleaning up references and/or rewriting
		// the partition, or maybe enqueueing a merge fixup.
		log.VEventf(ctx, 2, "partition %d has only %d live vectors, do not split",
			partitionKey, vectors.Count)
		return nil
	}

	// Determine which partition children should go into the left split partition
	// and which should go into the right split partition.
	tempOffsets := fw.workspace.AllocUint64s(vectors.Count)
	defer fw.workspace.FreeUint64s(tempOffsets)
	kmeans := BalancedKmeans{Workspace: &fw.workspace, Rand: fw.rng}
	tempLeftOffsets, tempRightOffsets := kmeans.Compute(&vectors, tempOffsets)

	leftSplit, rightSplit := fw.splitPartitionData(
		ctx, partition, vectors, tempLeftOffsets, tempRightOffsets)

	if parentPartition != nil {
		// De-link the splitting partition from its parent partition. This does
		// not delete data or metadata in the partition.
		childKey := vecstore.ChildKey{PartitionKey: partitionKey}
		if !parentPartition.ReplaceWithLastByKey(childKey) {
			return errors.Wrapf(err, "removing splitting partition %d from its in-memory parent %d",
				partitionKey, parentPartitionKey)
		}

		count, err := fw.index.removeFromPartition(ctx, txn, parentPartitionKey, childKey)
		if err != nil {
			return errors.Wrapf(err, "removing splitting partition %d from its parent %d",
				partitionKey, parentPartitionKey)
		}

		if count != 0 {
			// Move any vectors to sibling partitions that have closer centroids.
			// Lazily get parent vectors only if they're actually needed.
			var parentVectors vector.Set
			getParentVectors := func() (vector.Set, error) {
				if parentVectors.Dims != 0 {
					return parentVectors, nil
				}
				var err error
				parentVectors, err = fw.getFullVectorsForPartition(
					ctx, txn, parentPartitionKey, parentPartition)
				return parentVectors, err
			}

			err = fw.moveVectorsToSiblings(
				ctx, txn, parentPartitionKey, parentPartition, getParentVectors, partitionKey, &leftSplit)
			if err != nil {
				return err
			}
			err = fw.moveVectorsToSiblings(
				ctx, txn, parentPartitionKey, parentPartition, getParentVectors, partitionKey, &rightSplit)
			if err != nil {
				return err
			}

			// Move any vectors at the same level that are closer to the new split
			// centroids than they are to their own centroids.
			if err = fw.linkNearbyVectors(ctx, txn, partitionKey, leftSplit.Partition); err != nil {
				return err
			}
			if err = fw.linkNearbyVectors(ctx, txn, partitionKey, rightSplit.Partition); err != nil {
				return err
			}
		}
	}

	// Insert the two new partitions into the index. This only adds their data
	// (and metadata) for the partition - they're not yet linked into the K-means
	// tree.
	leftPartitionKey, err := txn.InsertPartition(ctx, leftSplit.Partition)
	if err != nil {
		return errors.Wrapf(err, "creating left partition for split of partition %d", partitionKey)
	}
	rightPartitionKey, err := txn.InsertPartition(ctx, rightSplit.Partition)
	if err != nil {
		return errors.Wrapf(err, "creating right partition for split of partition %d", partitionKey)
	}

	log.VEventf(ctx, 2,
		"splitting partition %d (%d vectors) into left partition %d "+
			"(%d vectors) and right partition %d (%d vectors)",
		partitionKey, len(partition.ChildKeys()),
		leftPartitionKey, len(leftSplit.Partition.ChildKeys()),
		rightPartitionKey, len(rightSplit.Partition.ChildKeys()))

	// Now link the new partitions into the K-means tree.
	if parentPartition == nil {
		// Add a new level to the tree by setting a new root partition that points
		// to the two new partitions.
		centroids := vector.MakeSet(fw.index.rootQuantizer.GetRandomDims())
		centroids.EnsureCapacity(2)
		centroids.Add(leftSplit.Partition.Centroid())
		centroids.Add(rightSplit.Partition.Centroid())
		quantizedSet := fw.index.rootQuantizer.Quantize(ctx, &centroids)
		childKeys := []vecstore.ChildKey{
			{PartitionKey: leftPartitionKey},
			{PartitionKey: rightPartitionKey},
		}
		rootPartition := vecstore.NewPartition(
			fw.index.rootQuantizer, quantizedSet, childKeys, partition.Level()+1)
		if err = txn.SetRootPartition(ctx, rootPartition); err != nil {
			return errors.Wrapf(err, "setting new root for split of partition %d", partitionKey)
		}

		log.VEventf(ctx, 2, "created new root level with child partitions %d and %d",
			leftPartitionKey, rightPartitionKey)
	} else {
		// Link the two new partitions into the K-means tree by inserting them
		// into the parent level. This can trigger a further split, this time of
		// the parent level.
		searchCtx := fw.reuseSearchContext(ctx, txn)
		searchCtx.Level = parentPartition.Level() + 1

		searchCtx.Randomized = leftSplit.Partition.Centroid()
		childKey := vecstore.ChildKey{PartitionKey: leftPartitionKey}
		err = fw.index.insertHelper(searchCtx, childKey)
		if err != nil {
			return errors.Wrapf(err, "inserting left partition for split of partition %d", partitionKey)
		}

		searchCtx.Randomized = rightSplit.Partition.Centroid()
		childKey = vecstore.ChildKey{PartitionKey: rightPartitionKey}
		err = fw.index.insertHelper(searchCtx, childKey)
		if err != nil {
			return errors.Wrapf(err, "inserting right partition for split of partition %d", partitionKey)
		}

		// Delete the old partition.
		if err = txn.DeletePartition(ctx, partitionKey); err != nil {
			return errors.Wrapf(err, "deleting partition %d for split", partitionKey)
		}
	}

	return nil
}

// Split the given partition into left and right partitions, according to the
// provided left and right offsets. The offsets are expected to be in sorted
// order and refer to the corresponding vectors and child keys in the splitting
// partition.
// NOTE: The vectors set will be updated in-place, via a partial sort that moves
// vectors in the left partition to the left side of the set. However, the split
// partition is not modified.
func (fw *fixupWorker) splitPartitionData(
	ctx context.Context,
	splitPartition *vecstore.Partition,
	vectors vector.Set,
	leftOffsets, rightOffsets []uint64,
) (leftSplit, rightSplit splitData) {
	// Copy centroid distances and child keys so they can be split.
	centroidDistances := slices.Clone(splitPartition.QuantizedSet().GetCentroidDistances())
	childKeys := slices.Clone(splitPartition.ChildKeys())

	tempVector := fw.workspace.AllocFloats(fw.index.quantizer.GetRandomDims())
	defer fw.workspace.FreeFloats(tempVector)

	// Any left offsets that point beyond the end of the left list indicate that
	// a vector needs to be moved from the right partition to the left partition.
	// The reverse is true for right offsets. Because the left and right offsets
	// are in sorted order, out-of-bounds offsets must be at the end of the left
	// list and the beginning of the right list. Therefore, the algorithm just
	// needs to iterate over those offsets and swap the positions of the
	// referenced vectors.
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

		// Swap centroid distances and child keys.
		centroidDistances[left], centroidDistances[right] =
			centroidDistances[right], centroidDistances[left]
		childKeys[left], childKeys[right] = childKeys[right], childKeys[left]

		li--
		ri++
	}

	leftVectorSet := vectors
	rightVectorSet := leftVectorSet.SplitAt(len(leftOffsets))

	leftCentroidDistances := centroidDistances[:len(leftOffsets):len(leftOffsets)]
	leftChildKeys := childKeys[:len(leftOffsets):len(leftOffsets)]
	leftSplit.Init(ctx, fw.index.quantizer, leftVectorSet,
		leftCentroidDistances, leftChildKeys, splitPartition.Level())

	rightCentroidDistances := centroidDistances[len(leftOffsets):]
	rightChildKeys := childKeys[len(leftOffsets):]
	rightSplit.Init(ctx, fw.index.quantizer, rightVectorSet,
		rightCentroidDistances, rightChildKeys, splitPartition.Level())

	return leftSplit, rightSplit
}

// moveVectorsToSiblings checks each vector in the new split partition to see if
// it's now closer to a sibling partition's centroid than it is to its own
// centroid. If that's true, then move the vector to the sibling partition. Pass
// function to lazily fetch parent vectors, as it's expensive and is only needed
// if vectors actually need to be moved.
func (fw *fixupWorker) moveVectorsToSiblings(
	ctx context.Context,
	txn vecstore.Txn,
	parentPartitionKey vecstore.PartitionKey,
	parentPartition *vecstore.Partition,
	getParentVectors func() (vector.Set, error),
	oldPartitionKey vecstore.PartitionKey,
	split *splitData,
) error {
	for i := 0; i < split.Vectors.Count; i++ {
		if split.Vectors.Count == 1 && split.Partition.Level() != vecstore.LeafLevel {
			// Don't allow so many vectors to be moved that a non-leaf partition
			// ends up empty. This would violate a key constraint that the K-means
			// tree is always fully balanced.
			break
		}

		vector := split.Vectors.At(i)

		// If distance to new centroid is <= distance to old centroid, then skip.
		newCentroidDistance := split.Partition.QuantizedSet().GetCentroidDistances()[i]
		if newCentroidDistance <= split.OldCentroidDistances[i] {
			continue
		}

		// Get the full vectors for the parent partition's children.
		parentVectors, err := getParentVectors()
		if err != nil {
			return err
		}

		// Check whether the vector is closer to a sibling centroid than its own
		// new centroid.
		minDistanceOffset := -1
		for parent := 0; parent < parentVectors.Count; parent++ {
			squaredDistance := num32.L2Distance(parentVectors.At(parent), vector)
			if squaredDistance < newCentroidDistance {
				newCentroidDistance = squaredDistance
				minDistanceOffset = parent
			}
		}
		if minDistanceOffset == -1 {
			continue
		}

		siblingPartitionKey := parentPartition.ChildKeys()[minDistanceOffset].PartitionKey
		log.VEventf(ctx, 3, "moving vector from splitting partition %d to sibling partition %d",
			oldPartitionKey, siblingPartitionKey)

		// Found a sibling child partition that's closer, so insert the vector
		// there instead.
		childKey := split.Partition.ChildKeys()[i]
		_, err = fw.index.addToPartition(ctx, txn, parentPartitionKey, siblingPartitionKey, vector, childKey)
		if err != nil {
			return errors.Wrapf(err, "moving vector to partition %d", siblingPartitionKey)
		}

		// Remove the vector's data from the new partition. The remove operation
		// backfills data at the current index with data from the last index.
		// Therefore, don't increment the iteration index, since the next item
		// is in the same location as the last.
		split.ReplaceWithLast(i)
		i--
	}

	return nil
}

// linkNearbyVectors searches for vectors at the same level that are close to
// the given split partition's centroid. If they are closer than they are to
// their own centroid, then move them to the split partition.
func (fw *fixupWorker) linkNearbyVectors(
	ctx context.Context,
	txn vecstore.Txn,
	oldPartitionKey vecstore.PartitionKey,
	partition *vecstore.Partition,
) error {
	// TODO(andyk): Add way to filter search set in order to skip vectors deeper
	// down in the search rather than afterwards.
	searchCtx := fw.reuseSearchContext(ctx, txn)
	searchCtx.Options = SearchOptions{ReturnVectors: true}
	searchCtx.Level = partition.Level()
	searchCtx.Randomized = partition.Centroid()

	// Don't link more vectors than the number of remaining slots in the split
	// partition, to avoid triggering another split.
	maxResults := fw.index.options.MaxPartitionSize - partition.Count()
	if maxResults < 1 {
		return nil
	}
	searchSet := vecstore.SearchSet{MaxResults: maxResults}
	err := fw.index.searchHelper(searchCtx, &searchSet)
	if err != nil {
		return err
	}

	tempVector := fw.workspace.AllocVector(fw.index.quantizer.GetRandomDims())
	defer fw.workspace.FreeVector(tempVector)

	// Filter the results.
	results := searchSet.PopUnsortedResults()
	for i := range results {
		result := &results[i]

		// Skip vectors that are closer to their own centroid than they are to
		// the split partition's centroid.
		if result.QuerySquaredDistance >= result.CentroidDistance*result.CentroidDistance {
			continue
		}

		log.VEventf(ctx, 3, "linking vector from partition %d to splitting partition %d",
			result.ChildKey.PartitionKey, oldPartitionKey)

		// Leaf vectors from the primary index need to be randomized.
		vector := result.Vector
		if partition.Level() == vecstore.LeafLevel {
			fw.index.quantizer.RandomizeVector(ctx, vector, tempVector, false /* invert */)
			vector = tempVector
		}

		// Remove the vector from the other partition.
		count, err := fw.index.removeFromPartition(ctx, txn, result.ParentPartitionKey, result.ChildKey)
		if err != nil {
			return err
		}
		if count == 0 && partition.Level() > vecstore.LeafLevel {
			// Removing the vector will result in an empty non-leaf partition, which
			// is not allowed, as the K-means tree would not be fully balanced. Add
			// the vector back to the partition. This is a very rare case and that
			// partition is likely to be merged away regardless.
			_, err = txn.AddToPartition(ctx, result.ParentPartitionKey, vector, result.ChildKey)
			if err != nil {
				return err
			}
			continue
		}

		// Add the vector to the split partition.
		partition.Add(ctx, vector, result.ChildKey)
	}

	return nil
}

// mergePartition merges the given partition by moving all of its vectors to
// sibling partitions and then deleting the merged partition.
func (fw *fixupWorker) mergePartition(
	ctx context.Context,
	txn vecstore.Txn,
	parentPartitionKey vecstore.PartitionKey,
	parentPartition *vecstore.Partition,
	partitionKey vecstore.PartitionKey,
	partition *vecstore.Partition,
	vectors vector.Set,
) (err error) {
	if partitionKey == vecstore.RootKey {
		return errors.AssertionFailedf("cannot merge the root partition")
	}

	// This check ensures that the tree always stays fully balanced; removing a
	// level always happens at the root.
	if parentPartition.Count() == 1 && parentPartitionKey != vecstore.RootKey {
		log.VEventf(ctx, 2, "partition %d has no sibling partitions, do not merge", partitionKey)
		return nil
	}

	log.VEventf(ctx, 2, "merging partition %d (%d vectors)",
		partitionKey, len(partition.ChildKeys()))

	// Delete the merging partition from the store. This actually deletes the
	// partition's data and metadata.
	if err = txn.DeletePartition(ctx, partitionKey); err != nil {
		return errors.Wrapf(err, "deleting partition %d", partitionKey)
	}

	// If the merging partition is the last partition in the parent, then the
	// entire level needs to be removed from the tree. This is only allowed if
	// the parent partition is the root partition (the sibling partition check
	// made above ensures that this is the case). Reduce the number of levels in
	// the tree by one by merging vectors in the merging partition into the root
	// partition.
	if parentPartition.Count() == 1 {
		if parentPartitionKey != vecstore.RootKey {
			return errors.AssertionFailedf("only root partition can have zero vectors")
		}
		quantizedSet := fw.index.rootQuantizer.Quantize(ctx, &vectors)
		rootPartition := vecstore.NewPartition(
			fw.index.rootQuantizer, quantizedSet, partition.ChildKeys(), partition.Level())
		if err = txn.SetRootPartition(ctx, rootPartition); err != nil {
			return errors.Wrapf(err, "setting new root for merge of partition %d", partitionKey)
		}

		return nil
	}

	// De-link the merging partition from its parent partition. This does not
	// delete data or metadata in the partition.
	childKey := vecstore.ChildKey{PartitionKey: partitionKey}
	if !parentPartition.ReplaceWithLastByKey(childKey) {
		log.VEventf(ctx, 2, "partition %d is no longer child of partition %d, do not merge",
			partitionKey, parentPartitionKey)
		return nil
	}
	if _, err = fw.index.removeFromPartition(ctx, txn, parentPartitionKey, childKey); err != nil {
		return errors.Wrapf(err, "remove partition %d from parent partition %d",
			partitionKey, parentPartitionKey)
	}

	// Re-insert vectors from deleted partition into remaining partitions at the
	// same level.
	fw.searchCtx = searchContext{
		Ctx:       ctx,
		Workspace: fw.workspace,
		Txn:       txn,
		Level:     parentPartition.Level(),
	}

	childKeys := partition.ChildKeys()
	for i := range childKeys {
		fw.searchCtx.Randomized = vectors.At(i)
		err = fw.index.insertHelper(&fw.searchCtx, childKeys[i])
		if err != nil {
			return errors.Wrapf(err, "inserting vector from merged partition %d", partitionKey)
		}
	}

	return nil
}

// deleteVector deletes a vector from the store that has had its primary key
// deleted in the primary index, but was never deleted from the secondary index.
func (fw *fixupWorker) deleteVector(
	ctx context.Context, partitionKey vecstore.PartitionKey, vectorKey vecstore.PrimaryKey,
) (err error) {
	// Run the deletion within a transaction.
	txn, err := fw.index.store.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = fw.index.store.Commit(ctx, txn)
		} else {
			err = errors.CombineErrors(err, fw.index.store.Abort(ctx, txn))
		}
	}()

	log.VEventf(ctx, 2, "deleting dangling vector from partition %d", partitionKey)

	// Verify that the vector is still missing from the primary index. This guards
	// against a race condition where a row is created and deleted repeatedly with
	// the same primary key.
	childKey := vecstore.ChildKey{PrimaryKey: vectorKey}
	fw.tempVectorsWithKeys = ensureSliceLen(fw.tempVectorsWithKeys, 1)
	fw.tempVectorsWithKeys[0] = vecstore.VectorWithKey{Key: childKey}
	if err = txn.GetFullVectors(ctx, fw.tempVectorsWithKeys); err != nil {
		return errors.Wrap(err, "getting full vector")
	}
	if fw.tempVectorsWithKeys[0].Vector != nil {
		log.VEventf(ctx, 2, "primary key row exists, do not delete vector")
		return nil
	}

	_, err = fw.index.removeFromPartition(ctx, txn, partitionKey, childKey)
	if errors.Is(err, vecstore.ErrPartitionNotFound) {
		log.VEventf(ctx, 2, "partition %d no longer exists, do not delete vector", partitionKey)
		return nil
	}
	return err
}

// getFullVectorsForPartition fetches the full-size vectors (potentially
// randomized by the quantizer) that are quantized by the given partition.
// Discard any dangling vectors in the partition.
func (fw *fixupWorker) getFullVectorsForPartition(
	ctx context.Context,
	txn vecstore.Txn,
	partitionKey vecstore.PartitionKey,
	partition *vecstore.Partition,
) (vector.Set, error) {
	childKeys := partition.ChildKeys()
	fw.tempVectorsWithKeys = ensureSliceLen(fw.tempVectorsWithKeys, len(childKeys))
	for i := range childKeys {
		fw.tempVectorsWithKeys[i] = vecstore.VectorWithKey{Key: childKeys[i]}
	}
	err := txn.GetFullVectors(ctx, fw.tempVectorsWithKeys)
	if err != nil {
		err = errors.Wrapf(err, "getting full vectors of partition %d to split", partitionKey)
		return vector.Set{}, err
	}

	// Remove dangling vector references.
	for i := 0; i < len(fw.tempVectorsWithKeys); i++ {
		if fw.tempVectorsWithKeys[i].Vector != nil {
			continue
		}

		// Move last reference to current location and reduce size of slice.
		count := len(fw.tempVectorsWithKeys) - 1
		fw.tempVectorsWithKeys[i] = fw.tempVectorsWithKeys[count]
		fw.tempVectorsWithKeys = fw.tempVectorsWithKeys[:count]
		partition.ReplaceWithLast(i)
		i--
	}

	vectors := vector.MakeSet(fw.index.quantizer.GetRandomDims())
	vectors.AddUndefined(len(fw.tempVectorsWithKeys))
	for i := range fw.tempVectorsWithKeys {
		// Leaf vectors from the primary index need to be randomized.
		if partition.Level() == vecstore.LeafLevel {
			fw.index.quantizer.RandomizeVector(
				ctx, fw.tempVectorsWithKeys[i].Vector, vectors.At(i), false /* invert */)
		} else {
			copy(vectors.At(i), fw.tempVectorsWithKeys[i].Vector)
		}
	}

	return vectors, nil
}

// reuseSearchContext initializes the reusable search context, including reusing
// its temp slices.
func (fw *fixupWorker) reuseSearchContext(ctx context.Context, txn vecstore.Txn) *searchContext {
	fw.searchCtx = searchContext{
		Ctx:                 ctx,
		Workspace:           fw.workspace,
		Txn:                 txn,
		tempKeys:            fw.searchCtx.tempKeys,
		tempCounts:          fw.searchCtx.tempCounts,
		tempVectorsWithKeys: fw.searchCtx.tempVectorsWithKeys,
	}
	return &fw.searchCtx
}
