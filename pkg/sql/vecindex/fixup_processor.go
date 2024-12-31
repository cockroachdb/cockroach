// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/internal"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecstore"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// fixupType enumerates the different kinds of fixups.
type fixupType int

const (
	// splitOrMergeFixup is a fixup that includes the key of a partition to
	// split or merge as well as the key of its parent partition (if it exists).
	// Whether a partition is split or merged depends on its size.
	splitOrMergeFixup fixupType = iota + 1
	// vectorDeleteFixup is a fixup that includes the primary key of a vector to
	// delete from the index, as well as the key of the partition that contains
	// it.
	vectorDeleteFixup
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
	// VectorKey is the primary key of the fixup vector.
	VectorKey vecstore.PrimaryKey
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
//
// All entry methods (i.e. capitalized methods) in fixupProcess are thread-safe.
type fixupProcessor struct {
	// --------------------------------------------------
	// These fields can be accessed on any goroutine once the lock is acquired.
	// --------------------------------------------------
	mu struct {
		syncutil.Mutex

		// pendingPartitions tracks pending split and merge fixups.
		pendingPartitions map[vecstore.PartitionKey]bool

		// pendingVectors tracks pending fixups for deleting vectors.
		pendingVectors map[string]bool

		// waitForFixups broadcasts to any waiters when all fixups are processed.
		waitForFixups sync.Cond
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
	fp.mu.pendingPartitions = make(map[vecstore.PartitionKey]bool, maxFixups)
	fp.mu.pendingVectors = make(map[string]bool, maxFixups)
	fp.mu.waitForFixups.L = &fp.mu
	fp.fixups = make(chan fixup, maxFixups)
	fp.fixupsLimitHit = log.Every(time.Second)
}

// AddSplit enqueues a split fixup for later processing.
func (fp *fixupProcessor) AddSplit(
	ctx context.Context, parentPartitionKey vecstore.PartitionKey, partitionKey vecstore.PartitionKey,
) {
	fp.addFixup(ctx, fixup{
		Type:               splitOrMergeFixup,
		ParentPartitionKey: parentPartitionKey,
		PartitionKey:       partitionKey,
	})
}

// AddMerge enqueues a merge fixup for later processing.
func (fp *fixupProcessor) AddMerge(
	ctx context.Context, parentPartitionKey vecstore.PartitionKey, partitionKey vecstore.PartitionKey,
) {
	fp.addFixup(ctx, fixup{
		Type:               splitOrMergeFixup,
		ParentPartitionKey: parentPartitionKey,
		PartitionKey:       partitionKey,
	})
}

// AddDeleteVector enqueues a vector deletion fixup for later processing.
func (fp *fixupProcessor) AddDeleteVector(
	ctx context.Context, partitionKey vecstore.PartitionKey, vectorKey vecstore.PrimaryKey,
) {
	fp.addFixup(ctx, fixup{
		Type:         vectorDeleteFixup,
		PartitionKey: partitionKey,
		VectorKey:    vectorKey,
	})
}

// Wait blocks until all pending fixups have been processed by the background
// goroutine. This is useful in testing.
func (fp *fixupProcessor) Wait() {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	for len(fp.mu.pendingVectors) > 0 || len(fp.mu.pendingPartitions) > 0 {
		fp.mu.waitForFixups.Wait()
	}
}

// runAll processes all fixups in the queue. This should only be called by tests
// on one foreground goroutine, and only in cases where Start was not called.
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

// clearPending removes all pending fixups from the processor. This should only
// be called on one foreground goroutine, and only in cases where Start was not
// called.
func (fp *fixupProcessor) clearPending() {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	// Clear enqueued fixups.
	for {
		select {
		case <-fp.fixups:
			continue
		default:
		}
		break
	}

	// Clear pending fixups.
	fp.mu.pendingPartitions = make(map[vecstore.PartitionKey]bool)
	fp.mu.pendingVectors = make(map[string]bool)
}

// addFixup enqueues the given fixup for later processing, assuming there is not
// already a duplicate fixup that's pending.
func (fp *fixupProcessor) addFixup(ctx context.Context, fixup fixup) {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	// Check whether fixup limit has been reached.
	if len(fp.mu.pendingPartitions)+len(fp.mu.pendingVectors) >= maxFixups {
		// Don't enqueue the fixup.
		if fp.fixupsLimitHit.ShouldLog() {
			log.Warning(ctx, "reached limit of unprocessed fixups")
		}
		return
	}

	// Don't enqueue fixup if it's already pending.
	switch fixup.Type {
	case splitOrMergeFixup:
		if _, ok := fp.mu.pendingPartitions[fixup.PartitionKey]; ok {
			return
		}
		fp.mu.pendingPartitions[fixup.PartitionKey] = true

	case vectorDeleteFixup:
		if _, ok := fp.mu.pendingVectors[string(fixup.VectorKey)]; ok {
			return
		}
		fp.mu.pendingVectors[string(fixup.VectorKey)] = true

	default:
		panic(errors.AssertionFailedf("unknown fixup %d", fixup.Type))
	}

	// Note that the channel send operation should never block, since it has
	// maxFixups capacity.
	fp.fixups <- fixup
}
