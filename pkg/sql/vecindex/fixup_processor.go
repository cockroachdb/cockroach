// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecstore"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
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
// indicates the background goroutines have fallen far behind.
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
	// These read-only fields can be read on any goroutine.
	// --------------------------------------------------

	// initCtx is the context provided to the Init method. It is passed to fixup
	// workers.
	initCtx context.Context
	// stopper is used to create new workers and signal their quiescence.
	stopper *stop.Stopper
	// index points back to the vector index to which fixups are applied.
	index *VectorIndex
	// seed, if non-zero, specifies that a deterministic random number generator
	// should be used by the fixup processor. This is useful in testing.
	seed int64

	// --------------------------------------------------
	// These fields can be accessed on any goroutine.
	// --------------------------------------------------

	// fixups is an ordered list of fixups to process.
	fixups chan fixup
	// fixupsLimitHit prevents flooding the log with warning messages when the
	// maxFixups limit has been reached.
	fixupsLimitHit log.EveryN
	// cancel can be used to stop background workers even if the stopper has not
	// quiesced. It is called when the vector index is closed.
	cancel func()

	// --------------------------------------------------
	// These fields can be accessed on any goroutine once the lock is acquired.
	// --------------------------------------------------
	mu struct {
		syncutil.Mutex

		// pendingPartitions tracks pending split and merge fixups.
		pendingPartitions map[vecstore.PartitionKey]bool
		// pendingVectors tracks pending fixups for deleting vectors.
		pendingVectors map[string]bool
		// totalWorkers is the number of background workers available to process
		// fixups.
		totalWorkers int
		// runningWorkers is the number of background workers that are actively
		// processing fixups. This is always <= totalWorkers.
		runningWorkers int
		// suspended, if non-nil, prevents background workers from processing
		// fixups. Only once the channel is closed will they begin processing.
		// This is used for testing.
		suspended chan struct{}
		// waitForFixups broadcasts to any waiters when all fixups are processed.
		// This is used for testing.
		waitForFixups sync.Cond
	}
}

// Init initializes the fixup processor. The stopper is used to start new
// background workers. If "seed" is non-zero, then the fixup processor will use
// a deterministic random number generator. Otherwise, it will use the global
// random number generator.
func (fp *fixupProcessor) Init(
	ctx context.Context, stopper *stop.Stopper, index *VectorIndex, seed int64,
) {
	// Background workers should spin down when the stopper begins to quiesce.
	// Also save the cancel function so that workers can be shut down
	// independently of the stopper.
	fp.initCtx, fp.cancel = stopper.WithCancelOnQuiesce(ctx)
	fp.stopper = stopper
	fp.index = index
	fp.seed = seed

	fp.fixups = make(chan fixup, maxFixups)
	fp.fixupsLimitHit = log.Every(time.Second)

	fp.mu.pendingPartitions = make(map[vecstore.PartitionKey]bool, maxFixups)
	fp.mu.pendingVectors = make(map[string]bool, maxFixups)
	fp.mu.waitForFixups.L = &fp.mu
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

// Suspend blocks all background workers from processing fixups until Process is
// called to let them run. This is useful for testing.
func (fp *fixupProcessor) Suspend() {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	if fp.mu.suspended != nil {
		panic(errors.AssertionFailedf("fixup processor was already suspended"))
	}

	fp.mu.suspended = make(chan struct{})
}

// Process waits until all pending fixups have been processed by background
// workers. If background workers have been suspended, they are temporarily
// allowed to run until all fixups have been processed. This is useful for
// testing.
func (fp *fixupProcessor) Process() {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	suspended := fp.mu.suspended
	if suspended != nil {
		// Signal any waiting background workers that they can process fixups.
		close(fp.mu.suspended)
		fp.mu.suspended = nil
	}

	// Wait for all fixups to be processed.
	for len(fp.mu.pendingVectors) > 0 || len(fp.mu.pendingPartitions) > 0 {
		fp.mu.waitForFixups.Wait()
	}

	// Re-suspend the fixup processor if it was suspended.
	if suspended != nil {
		fp.mu.suspended = make(chan struct{})
	}
}

// addFixup enqueues the given fixup for later processing, assuming there is not
// already a duplicate fixup that's pending. It also starts a background worker
// to process the fixup, if needed and allowed.
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

	// If there is an idle worker available, nothing more to do.
	if fp.mu.runningWorkers < fp.mu.totalWorkers {
		return
	}

	// If we've reached the max running worker limit, don't create additional
	// workers.
	if fp.mu.totalWorkers >= fp.index.options.MaxWorkers {
		return
	}

	// Start another worker.
	fp.mu.totalWorkers++

	worker := NewFixupWorker(fp)
	taskName := fmt.Sprintf("vecindex-worker-%d", fp.mu.totalWorkers)
	err := fp.stopper.RunAsyncTask(fp.initCtx, taskName, worker.Start)
	if err != nil {
		// Log error and continue.
		log.Errorf(ctx, "error starting vector index background worker: %v", err)
	}
}

// nextFixup fetches the next fixup in the queue so that it can be processed by
// a background worker. It blocks until there is a fixup available (ok=true) or
// until the processor shuts down (ok=false).
func (fp *fixupProcessor) nextFixup(ctx context.Context) (next fixup, ok bool) {
	select {
	case next = <-fp.fixups:
		// Within the scope of the mutex, increment running workers and check
		// whether processor is suspended.
		suspended := func() chan struct{} {
			fp.mu.Lock()
			defer fp.mu.Unlock()
			fp.mu.runningWorkers++
			return fp.mu.suspended
		}()
		if suspended != nil {
			// Can't process the fixup until the processor is resumed, so wait
			// until that happens.
			select {
			case <-suspended:
				break

			case <-ctx.Done():
				return fixup{}, false
			}
		}

		return next, true

	case <-ctx.Done():
		// Context was canceled, abort.
		return fixup{}, false
	}
}

// removeFixup removes a pending fixup that has been processed by a worker.
func (fp *fixupProcessor) removeFixup(toRemove fixup) {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	switch toRemove.Type {
	case splitOrMergeFixup:
		delete(fp.mu.pendingPartitions, toRemove.PartitionKey)

	case vectorDeleteFixup:
		delete(fp.mu.pendingVectors, string(toRemove.VectorKey))
	}

	fp.mu.runningWorkers--

	// If there are no more pending fixups, notify any waiters.
	if len(fp.mu.pendingPartitions) == 0 && len(fp.mu.pendingVectors) == 0 {
		fp.mu.waitForFixups.Broadcast()
	}
}
