// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/internal"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecstore"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/slices"
)

// fixupType enumerates the different kinds of fixups.
type fixupType int

const (
	// splitFixup is a fixup that includes the key of a partition to split as
	// well as the key of its parent partition.
	splitFixup fixupType = iota + 1
)

// maxFixups specifies the maximum number of pending index fixups that can be
// enqueued by foreground threads, waiting for processing. Hitting this limit
// indicates the background goroutine has fallen far behind.
const maxFixups = 100

// fixup describes an index fixup so that it can be enqueued for processing.
// Each fixup type needs to have some subset of the fields defined.
type fixup struct {
	// Type is the kind of fixup.
	Type fixupType
	// PartitionKey is the key of the fixup's target partition, if the fixup
	// operates on a partition.
	PartitionKey vecstore.PartitionKey
	// ParentPartitionKey is the key of the parent of the fixup's target
	// partition, if the fixup operates on a partition
	ParentPartitionKey vecstore.PartitionKey
}

// partitionFixupKey is used as a key in a uniqueness map for partition fixups.
type partitionFixupKey struct {
	// Type is the kind of fixup.
	Type fixupType
	// PartitionKey is the key of the fixup's target partition.
	PartitionKey vecstore.PartitionKey
}

// fixupProcessor applies index fixups in a background goroutine. Fixups repair
// issues like dangling vectors and maintain the index by splitting and merging
// partitions. Rather than interrupt a search or insert by performing a fixup in
// a foreground goroutine, the fixup is enqueued and run later in a background
// goroutine. This scheme avoids adding unpredictable latency to foreground
// operations.
//
// In addition, it’s important that each fixup is performed in its own
// transaction, with no re-entrancy allowed. If a fixup itself triggers another
// fixup, then that will likewise be enqueued and performed in a separate
// transaction, in order to avoid contention and re-entrancy, both of which can
// cause problems.
type fixupProcessor struct {
	// --------------------------------------------------
	// These fields can be accessed on any goroutine once the lock is acquired.
	// --------------------------------------------------
	mu struct {
		syncutil.Mutex

		// pendingPartitions tracks pending fixups that operate on a partition.
		pendingPartitions map[partitionFixupKey]bool
	}

	// --------------------------------------------------
	// These fields can be accessed on any goroutine.
	// --------------------------------------------------

	// fixups is an ordered list of fixups to process.
	fixups chan fixup
	// fixupsLimitHit prevents flooding the log with warning messages when the
	// maxFixups limit has been reached.
	fixupsLimitHit log.EveryN

	// --------------------------------------------------
	// The following fields should only be accessed on a single background
	// goroutine (or a single foreground goroutine in deterministic tests).
	// --------------------------------------------------

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

// Init initializes the fixup processor. If "seed" is non-zero, then the fixup
// processor will use a deterministic random number generator. Otherwise, it
// will use the global random number generator.
func (fp *fixupProcessor) Init(index *VectorIndex, seed int64) {
	fp.index = index
	if seed != 0 {
		// Create a random number generator for the background goroutine.
		fp.rng = rand.New(rand.NewSource(seed))
	}
	fp.mu.pendingPartitions = make(map[partitionFixupKey]bool, maxFixups)
	fp.fixups = make(chan fixup, maxFixups)
	fp.fixupsLimitHit = log.Every(time.Second)
}

// AddSplit enqueues a split fixup for later processing.
func (fp *fixupProcessor) AddSplit(
	ctx context.Context, parentPartitionKey vecstore.PartitionKey, partitionKey vecstore.PartitionKey,
) {
	fp.addFixup(ctx, fixup{
		Type:               splitFixup,
		ParentPartitionKey: parentPartitionKey,
		PartitionKey:       partitionKey,
	})
}

// runAll processes all fixups in the queue. This should only be called by
// tests on one foreground goroutine.
func (fp *fixupProcessor) runAll(ctx context.Context) error {
	for {
		ok, err := fp.run(ctx, false /* wait */)
		if err != nil {
			return err
		}
		if !ok {
			// No more fixups to process.
			return nil
		}
	}
}

// run processes the next fixup in the queue and returns true. If "wait" is
// false, then run returns false if there are no fixups in the queue. If "wait"
// is true, then run blocks until it has processed a fixup or until the context
// is canceled, in which case it returns false.
func (fp *fixupProcessor) run(ctx context.Context, wait bool) (ok bool, err error) {
	// Get next fixup from the queue.
	var next fixup
	if wait {
		// Wait until a fixup is enqueued or the context is canceled.
		select {
		case next = <-fp.fixups:
			break

		case <-ctx.Done():
			// Context was canceled, abort.
			return false, nil
		}
	} else {
		// If no fixup is available, immediately return.
		select {
		case next = <-fp.fixups:
			break

		default:
			return false, nil
		}
	}

	// Invoke the fixup function. Note that we do not hold the lock while
	// processing the fixup.
	switch next.Type {
	case splitFixup:
		if err = fp.splitPartition(ctx, next.ParentPartitionKey, next.PartitionKey); err != nil {
			err = errors.Wrapf(err, "splitting partition %d", next.PartitionKey)
		}
	}

	// Delete already-processed fixup from its pending map, even if the fixup
	// failed, in order to avoid looping over the same fixup.
	fp.mu.Lock()
	defer fp.mu.Unlock()

	switch next.Type {
	case splitFixup:
		key := partitionFixupKey{Type: next.Type, PartitionKey: next.PartitionKey}
		delete(fp.mu.pendingPartitions, key)
	}

	return true, err
}

// addFixup enqueues the given fixup for later processing, assuming there is not
// already a duplicate fixup that's pending.
func (fp *fixupProcessor) addFixup(ctx context.Context, fixup fixup) {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	// Check whether fixup limit has been reached.
	if len(fp.mu.pendingPartitions) >= maxFixups {
		// Don't enqueue the fixup.
		if fp.fixupsLimitHit.ShouldLog() {
			log.Warning(ctx, "reached limit of unprocessed fixups")
		}
		return
	}

	// Don't enqueue fixup if it's already pending.
	switch fixup.Type {
	case splitFixup:
		key := partitionFixupKey{Type: fixup.Type, PartitionKey: fixup.PartitionKey}
		if _, ok := fp.mu.pendingPartitions[key]; ok {
			return
		}
		fp.mu.pendingPartitions[key] = true
	}

	// Note that the channel send operation should never block, since it has
	// maxFixups capacity.
	fp.fixups <- fixup
}

// splitPartition splits the partition with the given key and parent key. This
// runs in its own transaction. For a given index, there is at most one split
// happening per SQL process. However, there can be multiple SQL processes, each
// running a split.
func (fp *fixupProcessor) splitPartition(
	ctx context.Context, parentPartitionKey vecstore.PartitionKey, partitionKey vecstore.PartitionKey,
) (err error) {
	// Run the split within a transaction.
	txn, err := fp.index.store.BeginTransaction(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = fp.index.store.CommitTransaction(ctx, txn)
		} else {
			err = errors.CombineErrors(err, fp.index.store.AbortTransaction(ctx, txn))
		}
	}()

	// Get the partition to be split from the store.
	partition, err := fp.index.store.GetPartition(ctx, txn, partitionKey)
	if errors.Is(err, vecstore.ErrPartitionNotFound) {
		log.VEventf(ctx, 2, "partition %d no longer exists, do not split", partitionKey)
		return nil
	} else if err != nil {
		return errors.Wrapf(err, "getting partition %d to split", partitionKey)
	}

	// Load the parent of the partition to split.
	var parentPartition *vecstore.Partition
	if partitionKey != vecstore.RootKey {
		parentPartition, err = fp.index.store.GetPartition(ctx, txn, parentPartitionKey)
		if errors.Is(err, vecstore.ErrPartitionNotFound) {
			log.VEventf(ctx, 2, "parent partition %d of partition %d no longer exists, do not split",
				parentPartitionKey, partitionKey)
			return nil
		} else if err != nil {
			return errors.Wrapf(err, "getting parent %d of partition %d to split",
				parentPartitionKey, partitionKey)
		}

		if parentPartition.Find(vecstore.ChildKey{PartitionKey: partitionKey}) == -1 {
			log.VEventf(ctx, 2, "partition %d is no longer child of partition %d, do not split",
				partitionKey, parentPartitionKey)
			return nil
		}
	}

	// Get the full vectors for the partition's children.
	vectors, err := fp.getFullVectorsForPartition(ctx, txn, partitionKey, partition)
	if err != nil {
		return errors.Wrapf(err, "getting full vectors for split of partition %d", partitionKey)
	}
	if vectors.Count < fp.index.options.MaxPartitionSize*3/4 {
		// This could happen if the partition had tons of dangling references that
		// need to be cleaned up.
		log.VEventf(ctx, 2, "partition %d has only %d live vectors, do not split",
			partitionKey, vectors.Count)
		return nil
	}

	// Determine which partition children should go into the left split partition
	// and which should go into the right split partition.
	tempOffsets := fp.workspace.AllocUint64s(vectors.Count)
	defer fp.workspace.FreeUint64s(tempOffsets)
	kmeans := BalancedKmeans{Workspace: &fp.workspace, Rand: fp.rng}
	tempLeftOffsets, tempRightOffsets := kmeans.Compute(&vectors, tempOffsets)

	leftSplit, rightSplit := fp.splitPartitionData(
		ctx, partition, &vectors, tempLeftOffsets, tempRightOffsets)

	if parentPartition != nil {
		// De-link the splitting partition from its parent partition.
		childKey := vecstore.ChildKey{PartitionKey: partitionKey}
		_, err = fp.index.removeFromPartition(ctx, txn, parentPartitionKey, childKey)
		if err != nil {
			return errors.Wrapf(err, "removing splitting partition %d from its parent %d",
				partitionKey, parentPartitionKey)
		}

		// TODO(andyk): Move vectors to/from split partition.
	}

	// Insert the two new partitions into the index. This only adds their data
	// (and metadata) for the partition - they're not yet linked into the K-means
	// tree.
	leftPartitionKey, err := fp.index.store.InsertPartition(ctx, txn, leftSplit.Partition)
	if err != nil {
		return errors.Wrapf(err, "creating left partition for split of partition %d", partitionKey)
	}
	rightPartitionKey, err := fp.index.store.InsertPartition(ctx, txn, rightSplit.Partition)
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
	if partitionKey == vecstore.RootKey {
		// Add a new level to the tree by setting a new root partition that points
		// to the two new partitions.
		centroids := vector.MakeSet(fp.index.rootQuantizer.GetRandomDims())
		centroids.EnsureCapacity(2)
		centroids.Add(leftSplit.Partition.Centroid())
		centroids.Add(rightSplit.Partition.Centroid())
		quantizedSet := fp.index.rootQuantizer.Quantize(ctx, &centroids)
		childKeys := []vecstore.ChildKey{
			{PartitionKey: leftPartitionKey},
			{PartitionKey: rightPartitionKey},
		}
		rootPartition := vecstore.NewPartition(
			fp.index.rootQuantizer, quantizedSet, childKeys, partition.Level()+1)
		if err = fp.index.store.SetRootPartition(ctx, txn, rootPartition); err != nil {
			return errors.Wrapf(err, "setting new root for split of partition %d", partitionKey)
		}

		log.VEventf(ctx, 2, "created new root level with child partitions %d and %d",
			leftPartitionKey, rightPartitionKey)
	} else {
		// Link the two new partitions into the K-means tree by inserting them
		// into the parent level. This can trigger a further split, this time of
		// the parent level.
		fp.searchCtx = searchContext{
			Ctx:       ctx,
			Workspace: fp.workspace,
			Txn:       txn,
			Level:     parentPartition.Level() + 1,
		}

		fp.searchCtx.Randomized = leftSplit.Partition.Centroid()
		childKey := vecstore.ChildKey{PartitionKey: leftPartitionKey}
		err = fp.index.insertHelper(&fp.searchCtx, childKey, true /* allowRetry */)
		if err != nil {
			return errors.Wrapf(err, "inserting left partition for split of partition %d", partitionKey)
		}

		fp.searchCtx.Randomized = rightSplit.Partition.Centroid()
		childKey = vecstore.ChildKey{PartitionKey: rightPartitionKey}
		err = fp.index.insertHelper(&fp.searchCtx, childKey, true /* allowRetry */)
		if err != nil {
			return errors.Wrapf(err, "inserting right partition for split of partition %d", partitionKey)
		}

		// Delete the old partition.
		if err = fp.index.store.DeletePartition(ctx, txn, partitionKey); err != nil {
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
func (fp *fixupProcessor) splitPartitionData(
	ctx context.Context,
	splitPartition *vecstore.Partition,
	vectors *vector.Set,
	leftOffsets, rightOffsets []uint64,
) (leftSplit, rightSplit splitData) {
	// Copy centroid distances and child keys so they can be split.
	centroidDistances := slices.Clone(splitPartition.QuantizedSet().GetCentroidDistances())
	childKeys := slices.Clone(splitPartition.ChildKeys())

	tempVector := fp.workspace.AllocFloats(fp.index.quantizer.GetRandomDims())
	defer fp.workspace.FreeFloats(tempVector)

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
			panic("expected equal number of left and right offsets that need to be swapped")
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

	leftVectorSet := *vectors
	rightVectorSet := leftVectorSet.SplitAt(len(leftOffsets))

	leftCentroidDistances := centroidDistances[:len(leftOffsets):len(leftOffsets)]
	leftChildKeys := childKeys[:len(leftOffsets):len(leftOffsets)]
	leftSplit.Init(ctx, fp.index.quantizer, &leftVectorSet,
		leftCentroidDistances, leftChildKeys, splitPartition.Level())

	rightCentroidDistances := centroidDistances[len(leftOffsets):]
	rightChildKeys := childKeys[len(leftOffsets):]
	rightSplit.Init(ctx, fp.index.quantizer, &rightVectorSet,
		rightCentroidDistances, rightChildKeys, splitPartition.Level())

	return leftSplit, rightSplit
}

// getFullVectorsForPartition fetches the full-size vectors (potentially
// randomized by the quantizer) that are quantized by the given partition.
func (fp *fixupProcessor) getFullVectorsForPartition(
	ctx context.Context,
	txn vecstore.Txn,
	partitionKey vecstore.PartitionKey,
	partition *vecstore.Partition,
) (vector.Set, error) {
	childKeys := partition.ChildKeys()
	fp.tempVectorsWithKeys = ensureSliceLen(fp.tempVectorsWithKeys, len(childKeys))
	for i := range childKeys {
		fp.tempVectorsWithKeys[i].Key = childKeys[i]
	}
	err := fp.index.store.GetFullVectors(ctx, txn, fp.tempVectorsWithKeys)
	if err != nil {
		err = errors.Wrapf(err, "getting full vectors of partition %d to split", partitionKey)
		return vector.Set{}, err
	}

	// Remove dangling vector references.
	for i := range fp.tempVectorsWithKeys {
		if fp.tempVectorsWithKeys[i].Vector != nil {
			continue
		}

		// Move last reference to current location and reduce size of slice.
		// TODO(andyk): Enqueue fixup to delete dangling vector from index.
		count := len(fp.tempVectorsWithKeys) - 1
		fp.tempVectorsWithKeys[i] = fp.tempVectorsWithKeys[count]
		fp.tempVectorsWithKeys = fp.tempVectorsWithKeys[:count]
		i--
	}

	vectors := vector.MakeSet(fp.index.quantizer.GetRandomDims())
	vectors.AddUndefined(len(fp.tempVectorsWithKeys))
	for i := range fp.tempVectorsWithKeys {
		// Leaf vectors from the primary index need to be randomized.
		if partition.Level() == vecstore.LeafLevel {
			fp.index.quantizer.RandomizeVector(
				ctx, fp.tempVectorsWithKeys[i].Vector, vectors.At(i), false /* invert */)
		} else {
			copy(vectors.At(i), fp.tempVectorsWithKeys[i].Vector)
		}
	}

	return vectors, nil
}
