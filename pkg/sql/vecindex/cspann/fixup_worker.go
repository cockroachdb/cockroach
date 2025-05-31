// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"context"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/vecdist"
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
	// treeKey is the K-means tree in which the worker is currently operating.
	// It's set when the worker begins processing a new fixup.
	treeKey TreeKey
	// singleStep, if true, causes split and merge fixups to abort after each step
	// in their execution. This is used for testing, in order to deterministically
	// interleave multiple fixups together in the same tree.
	singleStep bool

	// Temporary memory that's reused across calls to avoid extra allocations.

	workspace           workspace.T
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
			err = fw.splitPartition(ctx, next.ParentPartitionKey, next.PartitionKey)
			if err != nil {
				err = errors.Wrapf(err, "splitting partition %d", next.PartitionKey)
			}

		case mergeFixup:

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

// deleteVector deletes a vector from the store that has had its primary key
// deleted in the primary index, but was never deleted from the secondary index.
func (fw *fixupWorker) deleteVector(
	ctx context.Context, partitionKey PartitionKey, vectorKey KeyBytes,
) (err error) {
	log.VEventf(ctx, 2, "deleting dangling vector from partition %d", partitionKey)

	// Run the deletion within a transaction.
	return fw.index.store.RunTransaction(ctx, func(txn Txn) error {
		// Verify that the vector is still missing from the primary index. This guards
		// against a race condition where a row is created and deleted repeatedly with
		// the same primary key.
		childKey := ChildKey{KeyBytes: vectorKey}
		fw.tempVectorsWithKeys = ensureSliceLen(fw.tempVectorsWithKeys, 1)
		fw.tempVectorsWithKeys[0] = VectorWithKey{Key: childKey}
		if err = txn.GetFullVectors(ctx, fw.treeKey, fw.tempVectorsWithKeys); err != nil {
			return errors.Wrap(err, "getting full vector")
		}
		if fw.tempVectorsWithKeys[0].Vector != nil {
			log.VEventf(ctx, 2, "primary key row exists, do not delete vector")
			return nil
		}

		err = fw.index.removeFromPartition(ctx, txn, fw.treeKey, partitionKey, LeafLevel, childKey)
		if errors.Is(err, ErrPartitionNotFound) {
			log.VEventf(ctx, 2, "partition %d no longer exists, do not delete vector", partitionKey)
			return nil
		}
		return err
	})
}

// getFullVectorsForPartition fetches the full-size vectors that are quantized
// by the given partition.
//
// For a leaf partition:
//  1. Fetch the original vectors from the primary index.
//  2. Randomize the vectors.
//  3. For the Cosine distance metric, normalize the vectors.
//
// For an interior partition:
//  1. Fetch the centroids for the child partitions.
//  2. For the Cosine and InnerProduct distance metrics, convert the mean
//     centroids to spherical centroids.
func (fw *fixupWorker) getFullVectorsForPartition(
	ctx context.Context, partitionKey PartitionKey, partition *Partition,
) (vectors vector.Set, err error) {
	const format = "getting %d full vectors for partition %d"

	defer func() {
		err = errors.Wrapf(err, format, partition.Count(), partitionKey)
	}()

	log.VEventf(ctx, 2, format, partition.Count(), partitionKey)

	err = fw.index.store.RunTransaction(ctx, func(txn Txn) error {
		childKeys := partition.ChildKeys()
		fw.tempVectorsWithKeys = ensureSliceLen(fw.tempVectorsWithKeys, len(childKeys))
		for i := range childKeys {
			fw.tempVectorsWithKeys[i] = VectorWithKey{Key: childKeys[i]}
		}
		err = txn.GetFullVectors(ctx, fw.treeKey, fw.tempVectorsWithKeys)
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
			if partition.Level() == LeafLevel {
				// Leaf vectors from the primary index need to be randomized and
				// possibly normalized.
				fw.index.TransformVector(fw.tempVectorsWithKeys[i].Vector, vectors.At(i))
			} else {
				copy(vectors.At(i), fw.tempVectorsWithKeys[i].Vector)

				// Convert mean centroids into spherical centroids for the Cosine
				// and InnerProduct distance metrics.
				switch fw.index.quantizer.GetDistanceMetric() {
				case vecdist.Cosine, vecdist.InnerProduct:
					num32.Normalize(vectors.At(i))
				}
			}
		}

		return nil
	})

	return vectors, err
}
