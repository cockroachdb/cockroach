// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"context"
	"math/rand"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// # Split and Merge Fixups
//
// Splits and merges are performed as a series of atomic steps that
// incrementally mutate the index, while still leaving it in a well-defined
// state that supports searches, inserts, and deletes. Crucially, the
// incremental operations avoid transactions that can contend with foreground
// operations, preferring to instead temporarily duplicate data so that
// foreground operations are not blocked. Each step is designed to be idempotent
// so that it is safe for multiple workers to concurrently attempt the same
// step - only one will win the race, and the others will no-op (or duplicate
// data in harmless ways).
//
// # Idempotency
//
// Idempotency is made possible by taking advantage of the ConditionalPut (CPut)
// operation supported by the KV layer. CPut only writes a KV key if its current
// value matches an expected value. Each partition has a metadata record that is
// used as an expected value. Only if the metadata is in the expected state will
// the fixup operation proceed; if it has changed, then some other agent must
// have intervened. In addition, the fixup operations define state machines for
// the partitions involved in the operation. Each state defines rules about
// what's allowed by that partition, e.g. whether vectors can be added to the
// partition.
//
// # Duplicates
//
// In order to avoid contending with foreground operations, fixup operations use
// a variant of the read-copy-update (RCU) pattern that copies vectors to new
// partitions in one step, and then deletes the vectors from the old partitions
// in another step. Between steps, the vectors exist in both partitions, meaning
// that searches can return duplicates. The search machinery expects this, and
// contains de-duplication logic to handle it. Note that in rare cases where
// vectors are rapidly copied across multiple partitions, it's possible for
// duplicates to remain in the index for arbitrary amounts of time - this is an
// acceptable trade-off for the benefit of greatly reduced contention.
//
// # Testing
//
// Testing a complex state machine is challenging. To make this easier, each
// complex fixup operation supports "stepping". After each mutating step, the
// operation will abort the fixup. This gives tests a chance to perform other
// operations like searches, inserts, splits, or merges, that might interfere
// with the aborted operation. After updating the index, the aborted operation
// can be resumed and the test can validate it handles the interference as
// expected.

// errFixupAborted is returned by a fixup that cannot be completed, either
// because some condition prevents it, or because the worker is stepping as part
// of a test. The worker will remove the fixup from the queue. Depending on the
// fixup, it may get requeued later.
var errFixupAborted = errors.New("fixup aborted")

// fixupWorker is a background worker that processes queued fixups. Multiple
// workers can run at a time in parallel. Once started, a worker continually
// processes fixups until its context is canceled.
// NOTE: fixupWorker is *not* thread-safe.
type fixupWorker struct {
	// fp points back to the processor that spawned this worker.
	fp *FixupProcessor
	// index points back to the vector index to which fixups are applied.
	index *Index
	// rng is a random number generator. If nil, then the global random number
	// generator will be used.
	rng *rand.Rand
	// txn is the current transaction that's being used by the worker. It's set
	// when the worker begins processing a new fixup.
	txn Txn
	// treeKey is the K-means tree in which the worker is currently operating.
	// It's set when the worker begins processing a new fixup.
	treeKey TreeKey
	// singleStep, if true, causes split and merge fixups to abort after each step
	// in their execution. This is used for testing, in order to deterministically
	// interleave multiple fixups together in the same tree.
	singleStep bool

	// Temporary memory that's reused across calls to avoid extra allocations.

	workspace           workspace.T
	tempIndexCtx        Context
	tempVectorsWithKeys []VectorWithKey
	tempChildKey        [1]ChildKey
	tempValueBytes      [1]ValueBytes
}

// ewFixupWorker returns a new worker for the given processor.
func newFixupWorker(fp *FixupProcessor) *fixupWorker {
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
	for {
		next, ok := fw.fp.nextFixup(ctx)
		if !ok {
			break
		}
		fw.treeKey = next.TreeKey
		fw.singleStep = next.SingleStep

		// Invoke the fixup function. Note that we do not hold the lock while
		// processing the fixup.
		var err error
		switch next.Type {
		case splitFixup:
			if fw.index.options.UseNewFixups {
				err = fw.splitPartition(ctx, next.ParentPartitionKey, next.PartitionKey)
			} else {
				err = fw.oldSplitOrMergePartition(ctx, next.ParentPartitionKey, next.PartitionKey)
			}
			if err != nil {
				err = errors.Wrapf(err, "splitting partition %d", next.PartitionKey)
			}

		case mergeFixup:
			if !fw.index.options.UseNewFixups {
				err = fw.oldSplitOrMergePartition(ctx, next.ParentPartitionKey, next.PartitionKey)
				if err != nil {
					err = errors.Wrapf(err, "merging partition %d", next.PartitionKey)
				}
			}

		case vectorDeleteFixup:
			err = fw.deleteVector(ctx, next.PartitionKey, next.VectorKey)
			if err != nil {
				err = errors.Wrap(err, "deleting vector")
			}

		default:
			panic(errors.AssertionFailedf("unknown fixup %d", next.Type))
		}

		if err != nil {
			// This is a background goroutine, so just log error and continue.
			// TODO(andyk): Create a backoff mechanism so that bugs don't cause
			// rapid retries.
			log.Errorf(ctx, "%v", err)
		}

		// Delete already-processed fixup from its pending map, even if the fixup
		// failed, in order to avoid looping over the same fixup.
		fw.fp.removeFixup(next)
	}
}

// oldSplitOrMergePartition splits or merges the partition with the given key
// and parent key, depending on whether it's over-sized or under-sized. This
// operation runs in its own transaction.
// TODO(andyk): Remove this once splitOrMergePartition is ready.
func (fw *fixupWorker) oldSplitOrMergePartition(
	ctx context.Context, parentPartitionKey PartitionKey, partitionKey PartitionKey,
) (err error) {
	// Run the split or merge within a transaction.
	return fw.index.store.RunTransaction(ctx, func(txn Txn) error {
		fw.txn = txn

		// Get the partition to be split or merged from the store.
		partition, err := fw.txn.GetPartition(ctx, fw.treeKey, partitionKey)
		if errors.Is(err, ErrPartitionNotFound) || errors.Is(err, ErrRestartOperation) {
			log.VEventf(ctx, 2, "partition %d no longer exists, do not split or merge", partitionKey)
			return nil
		} else if err != nil {
			return errors.Wrapf(err, "getting partition %d to split or merge", partitionKey)
		}

		// Re-check the size of the partition now that it's locked, using a consistent
		// scan, so that we are not acting based on stale information.
		split := partition.Count() > fw.index.options.MaxPartitionSize
		merge := partitionKey != RootKey && partition.Count() < fw.index.options.MinPartitionSize
		if !split && !merge {
			log.VEventf(ctx, 2, "partition %d size is within bounds, do not split or merge", partitionKey)
			return nil
		}

		// Load the parent of the partition to split or merge.
		var parentPartition *Partition
		if parentPartitionKey != InvalidKey {
			parentPartition, err = fw.txn.GetPartition(ctx, fw.treeKey, parentPartitionKey)
			if errors.Is(err, ErrPartitionNotFound) || errors.Is(err, ErrRestartOperation) {
				log.VEventf(ctx, 2,
					"parent partition %d of partition %d no longer exists, do not split or merge",
					parentPartitionKey, partitionKey)
				return nil
			} else if err != nil {
				return errors.Wrapf(err, "getting parent %d of partition %d to split or merge",
					parentPartitionKey, partitionKey)
			}

			// Don't split or merge the partition if no longer a child of the parent.
			if parentPartition.Find(ChildKey{PartitionKey: partitionKey}) == -1 {
				log.VEventf(ctx, 2, "partition %d is no longer child of partition %d, do not split or merge",
					partitionKey, parentPartitionKey)
				return nil
			}
		}

		// Get the full vectors for the splitting or merging partition's children.
		vectors, err := fw.getFullVectorsForPartition(ctx, partitionKey, partition)
		if err != nil {
			return errors.Wrapf(
				err, "getting full vectors for split or merge of partition %d", partitionKey)
		}

		if split {
			return fw.oldSplitPartition(
				ctx, parentPartitionKey, parentPartition, partitionKey, partition, vectors)
		}
		return fw.mergePartition(
			ctx, parentPartitionKey, parentPartition, partitionKey, partition, vectors)
	})
}

// splitPartition splits the given partition by separating its vectors into one
// of two new replacement partitions.
func (fw *fixupWorker) oldSplitPartition(
	ctx context.Context,
	parentPartitionKey PartitionKey,
	parentPartition *Partition,
	partitionKey PartitionKey,
	partition *Partition,
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
	tempLeftCentroid := fw.workspace.AllocVector(vectors.Dims)
	defer fw.workspace.FreeVector(tempLeftCentroid)
	tempRightCentroid := fw.workspace.AllocVector(vectors.Dims)
	defer fw.workspace.FreeVector(tempRightCentroid)
	tempOffsets := fw.workspace.AllocUint64s(vectors.Count)
	defer fw.workspace.FreeUint64s(tempOffsets)
	kmeans := BalancedKmeans{Workspace: &fw.workspace, Rand: fw.rng}
	tempLeftOffsets, tempRightOffsets := kmeans.ComputeCentroids(
		vectors, tempLeftCentroid, tempRightCentroid, false /* pinLeftCentroid */, tempOffsets)

	leftSplit, rightSplit := oldSplitPartitionData(
		&fw.workspace, fw.index.quantizer, partition, vectors,
		tempLeftOffsets, tempRightOffsets)

	if parentPartition != nil {
		// De-link the splitting partition from its parent partition. This does
		// not delete data or metadata in the partition.
		childKey := ChildKey{PartitionKey: partitionKey}
		if !parentPartition.ReplaceWithLastByKey(childKey) {
			return errors.Wrapf(err, "removing splitting partition %d from its in-memory parent %d",
				partitionKey, parentPartitionKey)
		}

		err := fw.index.removeFromPartition(ctx, fw.txn, fw.treeKey,
			parentPartitionKey, parentPartition.Level(), childKey)
		if err != nil {
			return errors.Wrapf(err, "removing splitting partition %d from its parent %d",
				partitionKey, parentPartitionKey)
		}

		// Only attempt to move vectors if there are siblings.
		if parentPartition.Count() > 1 {
			// Move any vectors to sibling partitions that have closer centroids.
			// Lazily get parent vectors only if they're actually needed.
			var parentVectors vector.Set
			getParentVectors := func() (vector.Set, error) {
				if parentVectors.Dims != 0 {
					return parentVectors, nil
				}
				var err error
				parentVectors, err = fw.getFullVectorsForPartition(
					ctx, parentPartitionKey, parentPartition)
				return parentVectors, err
			}

			err = fw.moveVectorsToSiblings(
				ctx, parentPartition, getParentVectors, partitionKey, &leftSplit)
			if err != nil {
				return err
			}
			err = fw.moveVectorsToSiblings(
				ctx, parentPartition, getParentVectors, partitionKey, &rightSplit)
			if err != nil {
				return err
			}

			// Move any vectors at the same level that are closer to the new split
			// centroids than they are to their own centroids.
			if err = fw.linkNearbyVectors(ctx, partitionKey, leftSplit.Partition); err != nil {
				return err
			}
			if err = fw.linkNearbyVectors(ctx, partitionKey, rightSplit.Partition); err != nil {
				return err
			}
		}
	}

	// Insert the two new partitions into the index. This only adds their data
	// (and metadata) for the partition - they're not yet linked into the K-means
	// tree.
	leftPartitionKey, err := fw.txn.InsertPartition(ctx, fw.treeKey, leftSplit.Partition)
	if err != nil {
		return errors.Wrapf(err, "creating left partition for split of partition %d", partitionKey)
	}
	rightPartitionKey, err := fw.txn.InsertPartition(ctx, fw.treeKey, rightSplit.Partition)
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
		centroids := vector.MakeSet(fw.index.rootQuantizer.GetDims())
		centroids.EnsureCapacity(2)
		centroids.Add(leftSplit.Partition.Centroid())
		centroids.Add(rightSplit.Partition.Centroid())
		quantizedSet := fw.index.rootQuantizer.Quantize(&fw.workspace, centroids)
		childKeys := []ChildKey{
			{PartitionKey: leftPartitionKey},
			{PartitionKey: rightPartitionKey},
		}
		valueBytes := make([]ValueBytes, 2)
		rootMetadata := PartitionMetadata{
			Level:        partition.Level() + 1,
			Centroid:     quantizedSet.GetCentroid(),
			StateDetails: MakeReadyDetails(),
		}
		rootPartition := NewPartition(
			rootMetadata, fw.index.rootQuantizer, quantizedSet, childKeys, valueBytes)
		if err = fw.txn.SetRootPartition(ctx, fw.treeKey, rootPartition); err != nil {
			return errors.Wrapf(err, "setting new root for split of partition %d", partitionKey)
		}

		log.VEventf(ctx, 2, "created new root level with child partitions %d and %d",
			leftPartitionKey, rightPartitionKey)
	} else {
		// Link the two new partitions into the K-means tree by inserting them
		// into the parent level. This can trigger a further split, this time of
		// the parent level.
		idxCtx := fw.reuseIndexContext(fw.txn, fw.treeKey)
		idxCtx.level = parentPartition.Level() + 1
		idxCtx.forInsert = true

		idxCtx.randomized = leftSplit.Partition.Centroid()
		childKey := ChildKey{PartitionKey: leftPartitionKey}
		err = fw.index.insertHelper(ctx, idxCtx, childKey, ValueBytes{})
		if err != nil {
			return errors.Wrapf(err, "inserting left partition for split of partition %d", partitionKey)
		}

		idxCtx.randomized = rightSplit.Partition.Centroid()
		childKey = ChildKey{PartitionKey: rightPartitionKey}
		err = fw.index.insertHelper(ctx, idxCtx, childKey, ValueBytes{})
		if err != nil {
			return errors.Wrapf(err, "inserting right partition for split of partition %d", partitionKey)
		}

		// Delete the old partition.
		if err = fw.txn.DeletePartition(ctx, fw.treeKey, partitionKey); err != nil {
			return errors.Wrapf(err, "deleting partition %d for split", partitionKey)
		}
	}

	if fw.fp.onSuccessfulSplit != nil {
		// Notify listener that a partition has been successfully split.
		fw.fp.onSuccessfulSplit()
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
func oldSplitPartitionData(
	w *workspace.T,
	quantizer quantize.Quantizer,
	splitPartition *Partition,
	vectors vector.Set,
	leftOffsets, rightOffsets []uint64,
) (leftSplit, rightSplit splitData) {
	// Copy centroid distances and child keys so they can be split.
	centroidDistances := slices.Clone(splitPartition.QuantizedSet().GetCentroidDistances())
	childKeys := slices.Clone(splitPartition.ChildKeys())
	valueBytes := slices.Clone(splitPartition.ValueBytes())

	tempVector := w.AllocFloats(quantizer.GetDims())
	defer w.FreeFloats(tempVector)

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

		// Swap centroid distances, child keys, and value bytes.
		centroidDistances[left], centroidDistances[right] =
			centroidDistances[right], centroidDistances[left]
		childKeys[left], childKeys[right] = childKeys[right], childKeys[left]
		valueBytes[left], valueBytes[right] = valueBytes[right], valueBytes[left]
		li--
		ri++
	}

	leftVectorSet := vectors
	rightVectorSet := leftVectorSet.SplitAt(len(leftOffsets))

	leftCentroidDistances := centroidDistances[:len(leftOffsets):len(leftOffsets)]
	leftChildKeys := childKeys[:len(leftOffsets):len(leftOffsets)]
	leftValueBytes := valueBytes[:len(leftOffsets):len(leftOffsets)]
	leftSplit.Init(w, quantizer, leftVectorSet,
		leftCentroidDistances, leftChildKeys, leftValueBytes, splitPartition.Level())

	rightCentroidDistances := centroidDistances[len(leftOffsets):]
	rightChildKeys := childKeys[len(leftOffsets):]
	rightValueBytes := valueBytes[len(leftOffsets):]
	rightSplit.Init(w, quantizer, rightVectorSet,
		rightCentroidDistances, rightChildKeys, rightValueBytes, splitPartition.Level())

	return leftSplit, rightSplit
}

// moveVectorsToSiblings checks each vector in the new split partition to see if
// it's now closer to a sibling partition's centroid than it is to its own
// centroid. If that's true, then move the vector to the sibling partition. Pass
// function to lazily fetch parent vectors, as it's expensive and is only needed
// if vectors actually need to be moved.
func (fw *fixupWorker) moveVectorsToSiblings(
	ctx context.Context,
	parentPartition *Partition,
	getParentVectors func() (vector.Set, error),
	oldPartitionKey PartitionKey,
	split *splitData,
) error {
	for i := 0; i < split.Vectors.Count; i++ {
		if split.Vectors.Count == 1 && split.Partition.Level() != LeafLevel {
			// Don't allow so many vectors to be moved that a non-leaf partition
			// ends up empty. This would violate a key constraint that the K-means
			// tree is always fully balanced.
			break
		}

		vector := split.Vectors.At(i)

		// If distance to new centroid is <= distance to old centroid, then skip.
		// Only vectors that are now further from their new centroid than they were
		// to the old centroid must be considered for moving to a sibling partition.
		// This encodes the condition in Equation 1 from the SPFresh paper.
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
		// new centroid. Note that the pre-split partition has already been removed
		// from the parent partition by this point, so its centroid is not included
		// in parentVectors.
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
		valueBytes := split.Partition.ValueBytes()[i]
		err = fw.index.addToPartition(ctx, fw.txn, fw.treeKey,
			siblingPartitionKey, split.Partition.Level(), vector, childKey, valueBytes)
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
	ctx context.Context, oldPartitionKey PartitionKey, partition *Partition,
) error {
	// TODO(andyk): Add way to filter search set in order to skip vectors deeper
	// down in the search rather than afterwards.
	idxCtx := fw.reuseIndexContext(fw.txn, fw.treeKey)
	idxCtx.options = SearchOptions{ReturnVectors: true}
	idxCtx.level = partition.Level()
	idxCtx.randomized = partition.Centroid()

	// Ensure that the search never returns the last remaining vector in a
	// non-leaf partition, in order to avoid moving it and creating an empty
	// non-leaf partition, which is not allowed by a balanced K-means tree.
	idxCtx.ignoreLonelyVector = partition.Level() != LeafLevel

	// Don't link more vectors than the number of remaining slots in the split
	// partition, to avoid triggering another split.
	maxResults := fw.index.options.MaxPartitionSize - partition.Count()
	if maxResults < 1 {
		return nil
	}
	idxCtx.tempSearchSet = SearchSet{MaxResults: maxResults}
	err := fw.index.searchHelper(ctx, idxCtx, &idxCtx.tempSearchSet)
	if err != nil {
		return errors.Wrapf(err, "searching for vectors to link near splitting partition %d",
			oldPartitionKey)
	}

	tempVector := fw.workspace.AllocVector(fw.index.quantizer.GetDims())
	defer fw.workspace.FreeVector(tempVector)

	// Filter the results.
	results := idxCtx.tempSearchSet.PopResults()
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
		if partition.Level() == LeafLevel {
			fw.index.RandomizeVector(vector, tempVector)
			vector = tempVector
		}

		// Remove the vector from the other partition.
		err = fw.index.removeFromPartition(
			ctx, fw.txn, fw.treeKey, result.ParentPartitionKey, partition.Level(), result.ChildKey)
		if err != nil {
			return errors.Wrapf(err, "removing vector from nearby partition %d during split of %d",
				result.ParentPartitionKey, oldPartitionKey)
		}

		// Add the vector to the split partition.
		partition.Add(&fw.workspace, vector, result.ChildKey, result.ValueBytes, true /* overwrite */)
	}

	return nil
}

// mergePartition merges the given partition by moving all of its vectors to
// sibling partitions and then deleting the merged partition.
func (fw *fixupWorker) mergePartition(
	ctx context.Context,
	parentPartitionKey PartitionKey,
	parentPartition *Partition,
	partitionKey PartitionKey,
	partition *Partition,
	vectors vector.Set,
) (err error) {
	if partitionKey == RootKey {
		return errors.AssertionFailedf("cannot merge the root partition")
	}

	// This check ensures that the tree always stays fully balanced; removing a
	// level always happens at the root.
	if parentPartition.Count() == 1 && parentPartitionKey != RootKey {
		log.VEventf(ctx, 2, "partition %d has no sibling partitions, do not merge", partitionKey)
		return nil
	}

	log.VEventf(ctx, 2, "merging partition %d (%d vectors)",
		partitionKey, len(partition.ChildKeys()))

	// Delete the merging partition from the store. This actually deletes the
	// partition's data and metadata.
	if err = fw.txn.DeletePartition(ctx, fw.treeKey, partitionKey); err != nil {
		return errors.Wrapf(err, "deleting partition %d", partitionKey)
	}

	// If the merging partition is the last partition in the parent, then the
	// entire level needs to be removed from the tree. This is only allowed if
	// the parent partition is the root partition (the sibling partition check
	// made above ensures that this is the case). Reduce the number of levels in
	// the tree by one by merging vectors in the merging partition into the root
	// partition.
	if parentPartition.Count() == 1 {
		if parentPartitionKey != RootKey {
			return errors.AssertionFailedf("only root partition can have zero vectors")
		}
		quantizedSet := fw.index.rootQuantizer.Quantize(&fw.workspace, vectors)
		metadata := PartitionMetadata{
			Level:        partition.Level(),
			Centroid:     quantizedSet.GetCentroid(),
			StateDetails: MakeReadyDetails(),
		}
		rootPartition := NewPartition(
			metadata, fw.index.rootQuantizer, quantizedSet, partition.ChildKeys(), partition.ValueBytes())
		if err = fw.txn.SetRootPartition(ctx, fw.treeKey, rootPartition); err != nil {
			return errors.Wrapf(err, "setting new root for merge of partition %d", partitionKey)
		}

		return nil
	}

	// De-link the merging partition from its parent partition. This does not
	// delete data or metadata in the partition.
	childKey := ChildKey{PartitionKey: partitionKey}
	if !parentPartition.ReplaceWithLastByKey(childKey) {
		log.VEventf(ctx, 2, "partition %d is no longer child of partition %d, do not merge",
			partitionKey, parentPartitionKey)
		return nil
	}
	err = fw.index.removeFromPartition(ctx, fw.txn, fw.treeKey,
		parentPartitionKey, parentPartition.Level(), childKey)
	if err != nil {
		return errors.Wrapf(err, "remove partition %d from parent partition %d",
			partitionKey, parentPartitionKey)
	}

	// Re-insert vectors from deleted partition into remaining partitions at the
	// same level.
	idxCtx := fw.reuseIndexContext(fw.txn, fw.treeKey)
	idxCtx.level = parentPartition.Level()
	idxCtx.forInsert = true

	childKeys := partition.ChildKeys()
	valueBytes := partition.ValueBytes()
	for i := range childKeys {
		idxCtx.original = nil
		idxCtx.randomized = vectors.At(i)
		err = fw.index.insertHelper(ctx, idxCtx, childKeys[i], valueBytes[i])
		if err != nil {
			return errors.Wrapf(err, "inserting vector from merged partition %d", partitionKey)
		}
	}

	return nil
}

// deleteVector deletes a vector from the store that has had its primary key
// deleted in the primary index, but was never deleted from the secondary index.
func (fw *fixupWorker) deleteVector(
	ctx context.Context, partitionKey PartitionKey, vectorKey KeyBytes,
) (err error) {
	// Run the deletion within a transaction.
	return fw.index.store.RunTransaction(ctx, func(txn Txn) error {
		fw.txn = txn
		log.VEventf(ctx, 2, "deleting dangling vector from partition %d", partitionKey)

		// Verify that the vector is still missing from the primary index. This guards
		// against a race condition where a row is created and deleted repeatedly with
		// the same primary key.
		childKey := ChildKey{KeyBytes: vectorKey}
		fw.tempVectorsWithKeys = ensureSliceLen(fw.tempVectorsWithKeys, 1)
		fw.tempVectorsWithKeys[0] = VectorWithKey{Key: childKey}
		if err = fw.txn.GetFullVectors(ctx, fw.treeKey, fw.tempVectorsWithKeys); err != nil {
			return errors.Wrap(err, "getting full vector")
		}
		if fw.tempVectorsWithKeys[0].Vector != nil {
			log.VEventf(ctx, 2, "primary key row exists, do not delete vector")
			return nil
		}

		// If removing from a root partition, check that its level is LeafLevel. It
		// might not be if it was recently split.
		if partitionKey == RootKey {
			metadata, err := fw.txn.GetPartitionMetadata(
				ctx, fw.treeKey, partitionKey, false /* forUpdate */)
			if metadata.Level != LeafLevel {
				// Root partition's level has been updated, so just abort.
				return nil
			} else if err != nil {
				if errors.Is(err, ErrPartitionNotFound) {
					log.VEventf(ctx, 2, "partition %d no longer exists, do not delete vector", partitionKey)
					return nil
				}
				return errors.Wrapf(err, "getting root partition's level")
			}
		}

		err = fw.index.removeFromPartition(ctx, fw.txn, fw.treeKey, partitionKey, LeafLevel, childKey)
		if errors.Is(err, ErrPartitionNotFound) {
			log.VEventf(ctx, 2, "partition %d no longer exists, do not delete vector", partitionKey)
			return nil
		}
		return err
	})
}

// getFullVectorsForPartition fetches the full-size vectors (potentially
// randomized by the quantizer) that are quantized by the given partition.
// Discard any dangling vectors in the partition.
func (fw *fixupWorker) getFullVectorsForPartition(
	ctx context.Context, partitionKey PartitionKey, partition *Partition,
) (vectors vector.Set, err error) {
	const format = "getting %d full vectors for partition %d"

	defer func() {
		err = errors.Wrapf(err, format, partition.Count(), partitionKey)
	}()

	log.VEventf(ctx, 2, format, partition.Count(), partitionKey)

	run := func() error {
		childKeys := partition.ChildKeys()
		fw.tempVectorsWithKeys = ensureSliceLen(fw.tempVectorsWithKeys, len(childKeys))
		for i := range childKeys {
			fw.tempVectorsWithKeys[i] = VectorWithKey{Key: childKeys[i]}
		}
		err = fw.txn.GetFullVectors(ctx, fw.treeKey, fw.tempVectorsWithKeys)
		if err != nil {
			return err
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

		vectors = vector.MakeSet(fw.index.quantizer.GetDims())
		vectors.AddUndefined(len(fw.tempVectorsWithKeys))
		for i := range fw.tempVectorsWithKeys {
			// Leaf vectors from the primary index need to be randomized.
			if partition.Level() == LeafLevel {
				fw.index.RandomizeVector(fw.tempVectorsWithKeys[i].Vector, vectors.At(i))
			} else {
				copy(vectors.At(i), fw.tempVectorsWithKeys[i].Vector)
			}
		}

		return nil
	}

 	// Run in a transaction if not already.
	if fw.txn == nil {
		err = fw.index.store.RunTransaction(ctx, func(txn Txn) error {
			fw.txn = txn
			return run()
		})
	} else {
		err = run()
	}

	return vectors, err
}

// reuseIndexContext initializes the reusable operation context, including
// reusing its temp slices.
func (fw *fixupWorker) reuseIndexContext(txn Txn, treeKey TreeKey) *Context {
	fw.tempIndexCtx = Context{
		txn:                 txn,
		treeKey:             treeKey,
		forInsert:           false,
		tempToSearch:        fw.tempIndexCtx.tempToSearch,
		tempVectorsWithKeys: fw.tempIndexCtx.tempVectorsWithKeys,
	}
	return &fw.tempIndexCtx
}
