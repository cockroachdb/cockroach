// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/crlib/crtime"
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
const maxFixups = 200

// opsPerSecPerProc specifies how many insert and delete operations per second
// that each processor is expected to drive. If this is in the ballpark, the
// pacer will be able to more quickly converge to the right level of throttling.
const opsPerSecPerProc = 500

// fixup describes an index fixup so that it can be enqueued for processing.
// Each fixup type needs to have some subset of the fields defined.
type fixup struct {
	// Type is the kind of fixup.
	Type fixupType
	// TreeKey identifies the K-means tree to which the fixup is applied.
	TreeKey TreeKey
	// PartitionKey is the key of the fixup's target partition, if the fixup
	// operates on a partition.
	PartitionKey PartitionKey
	// ParentPartitionKey is the key of the parent of the fixup's target
	// partition, if the fixup operates on a partition.
	ParentPartitionKey PartitionKey
	// VectorKey is the primary key of the fixup vector.
	VectorKey KeyBytes
	// CachedKey caches the key for the fixup, suitable for use in a map.
	CachedKey fixupKey
}

// fixupKey is used to detect duplicate fixups on the same partition/vector, so
// that we don't enqueue duplicates. It contains fields from the fixup struct
// that have been converted to types that can be used in a map key.
type fixupKey struct {
	// TreeKey is the fixup.TreeKey field converted to a string so that it's a
	// valid map key.
	// NOTE: This only results in an allocation if TreeKey is not empty.
	TreeKey string
	// PartitionKey is the fixup.PartitionKey field.
	PartitionKey PartitionKey
	// VectorKey is the fixup.VectorKey field converted to a string so that it's
	// a valid map key.
	VectorKey string
}

// makeFixupKey constructs a new key from the given fixup, that can be inserted
// into a map.
func makeFixupKey(f fixup) fixupKey {
	return fixupKey{
		TreeKey: string(f.TreeKey), PartitionKey: f.PartitionKey, VectorKey: string(f.VectorKey)}
}

// FixupProcessor applies index fixups in a background goroutine. Fixups repair
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
type FixupProcessor struct {
	// --------------------------------------------------
	// These read-only fields can be read on any goroutine.
	// --------------------------------------------------

	// initCtx is the context provided to the Init method. It is passed to fixup
	// workers.
	initCtx context.Context
	// stopper is used to create new workers and signal their quiescence.
	stopper *stop.Stopper
	// index points back to the vector index to which fixups are applied.
	index *Index
	// seed, if non-zero, specifies that a deterministic random number generator
	// should be used by the fixup processor. This is useful in testing.
	seed int64
	// minDelay specifies the minimum delay for insert and delete operations.
	// This is used for testing.
	minDelay time.Duration

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

		// pendingFixups tracks fixups that are waiting to be processed.
		pendingFixups map[fixupKey]bool
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
		// pacer limits vector insert/delete throughput if background fixups are
		// falling behind.
		pacer pacer
	}
}

// Init initializes the fixup processor. The stopper is used to start new
// background workers. If "seed" is non-zero, then the fixup processor will use
// a deterministic random number generator. Otherwise, it will use the global
// random number generator.
func (fp *FixupProcessor) Init(
	ctx context.Context, stopper *stop.Stopper, index *Index, seed int64,
) {
	// Background workers should spin down when the stopper begins to quiesce.
	// Also save the cancel function so that workers can be shut down
	// independently of the stopper.
	fp.initCtx, fp.cancel = stopper.WithCancelOnQuiesce(ctx)
	fp.stopper = stopper
	fp.index = index
	fp.seed = seed

	// Initialize the pacer with initial allowed ops/sec proportional to the
	// number of processors.
	initialOpsPerSec := runtime.GOMAXPROCS(-1) * opsPerSecPerProc
	fp.mu.pacer.Init(initialOpsPerSec, 0, crtime.NowMono)

	fp.fixups = make(chan fixup, maxFixups)
	fp.fixupsLimitHit = log.Every(time.Second)

	fp.mu.pendingFixups = make(map[fixupKey]bool, maxFixups)
	fp.mu.waitForFixups.L = &fp.mu
}

// QueueSize returns the current size of the fixup queue.
func (fp *FixupProcessor) QueueSize() int {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	return fp.countFixupsLocked()
}

// AllowedOpsPerSec returns the ops/sec currently allowed by the pacer.
func (fp *FixupProcessor) AllowedOpsPerSec() float64 {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	return fp.mu.pacer.allowedOpsPerSec
}

// DelayInsertOrDelete is called when a vector is about to be inserted into the
// index or deleted from it. It will block for however long the pacer determines
// is needed to allow background index maintenance work to keep up.
func (fp *FixupProcessor) DelayInsertOrDelete(ctx context.Context) error {
	// Get the amount of time to wait. Do this in a separate function so that
	// the mutex is not held while waiting.
	delay := func() time.Duration {
		fp.mu.Lock()
		defer fp.mu.Unlock()

		return fp.mu.pacer.OnInsertOrDelete(fp.countFixupsLocked())
	}()

	// Enforce min delay (used for testing).
	delay = max(delay, fp.minDelay)

	// Wait out the delay or until the context is canceled.
	select {
	case <-time.After(delay):
		break

	case <-ctx.Done():
		fp.mu.Lock()
		defer fp.mu.Unlock()

		// Notify the pacer that the operation was canceled so that it can adjust
		// the token bucket.
		fp.mu.pacer.OnInsertOrDeleteCanceled()
		return ctx.Err()
	}

	return nil
}

// AddSplit enqueues a split fixup for later processing.
func (fp *FixupProcessor) AddSplit(
	ctx context.Context, treeKey TreeKey, parentPartitionKey PartitionKey, partitionKey PartitionKey,
) {
	fp.addFixup(ctx, fixup{
		TreeKey:            treeKey,
		Type:               splitOrMergeFixup,
		ParentPartitionKey: parentPartitionKey,
		PartitionKey:       partitionKey,
	})
}

// AddMerge enqueues a merge fixup for later processing.
func (fp *FixupProcessor) AddMerge(
	ctx context.Context, treeKey TreeKey, parentPartitionKey PartitionKey, partitionKey PartitionKey,
) {
	fp.addFixup(ctx, fixup{
		TreeKey:            treeKey,
		Type:               splitOrMergeFixup,
		ParentPartitionKey: parentPartitionKey,
		PartitionKey:       partitionKey,
	})
}

// AddDeleteVector enqueues a vector deletion fixup for later processing.
func (fp *FixupProcessor) AddDeleteVector(
	ctx context.Context, partitionKey PartitionKey, vectorKey KeyBytes,
) {
	fp.addFixup(ctx, fixup{
		Type:         vectorDeleteFixup,
		PartitionKey: partitionKey,
		VectorKey:    vectorKey,
	})
}

// Suspend blocks all background workers from processing fixups until Process is
// called to let them run. This is useful for testing.
func (fp *FixupProcessor) Suspend() {
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
func (fp *FixupProcessor) Process() {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	suspended := fp.mu.suspended
	if suspended != nil {
		// Signal any waiting background workers that they can process fixups.
		close(fp.mu.suspended)
		fp.mu.suspended = nil
	}

	// Wait for all fixups to be processed.
	for fp.countFixupsLocked() > 0 {
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
func (fp *FixupProcessor) addFixup(ctx context.Context, fixup fixup) {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	// Check whether fixup limit has been reached.
	if fp.countFixupsLocked() >= maxFixups {
		// Don't enqueue the fixup.
		if fp.fixupsLimitHit.ShouldLog() {
			log.Warning(ctx, "reached limit of unprocessed fixups")
		}
		return
	}

	// Don't enqueue fixup if it's already pending.
	fixup.CachedKey = makeFixupKey(fixup)
	if _, ok := fp.mu.pendingFixups[fixup.CachedKey]; ok {
		return
	}
	fp.mu.pendingFixups[fixup.CachedKey] = true

	// Note that the channel send operation should never block, since it has
	// maxFixups capacity.
	fp.fixups <- fixup

	// Notify the pacer that a fixup was just added.
	fp.mu.pacer.OnFixup(fp.countFixupsLocked())

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

	worker := newFixupWorker(fp)
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
func (fp *FixupProcessor) nextFixup(ctx context.Context) (next fixup, ok bool) {
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
func (fp *FixupProcessor) removeFixup(toRemove fixup) {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	delete(fp.mu.pendingFixups, toRemove.CachedKey)

	fp.mu.runningWorkers--

	// Notify the pacer that a fixup was just removed.
	fp.mu.pacer.OnFixup(fp.countFixupsLocked())

	// If there are no more pending fixups, notify any waiters.
	if fp.countFixupsLocked() == 0 {
		fp.mu.waitForFixups.Broadcast()
	}
}

// countFixupsLocked returns the number of fixups that are pending, including
// those that are currently being processed as well as those that are waiting to
// be processed.
func (fp *FixupProcessor) countFixupsLocked() int {
	return len(fp.mu.pendingFixups)
}
