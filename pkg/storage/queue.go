// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"container/heap"
	"sync"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	// purgatoryReportInterval is the duration between reports on
	// purgatory status.
	purgatoryReportInterval = 10 * time.Minute
	// defaultProcessTimeout is the timeout when processing a replica.
	// The timeout prevents a queue from getting stuck on a replica.
	// For example, a replica whose range is not reachable for quorum.
	defaultProcessTimeout = 1 * time.Minute
	// defaultQueueMaxSize is the default max size for a queue.
	defaultQueueMaxSize = 10000
)

// a purgatoryError indicates a replica processing failure which indicates
// the replica can be placed into purgatory for faster retries when the
// failure condition changes.
type purgatoryError interface {
	error
	purgatoryErrorMarker() // dummy method for unique interface
}

// A replicaItem holds a replica and its priority for use with a priority queue.
type replicaItem struct {
	value    roachpb.RangeID
	priority float64
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// A priorityQueue implements heap.Interface and holds replicaItems.
type priorityQueue []*replicaItem

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].priority > pq[j].priority
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index, pq[j].index = i, j
}

func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*replicaItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	old[n-1] = nil  // for gc
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority of a replicaItem in the queue.
func (pq *priorityQueue) update(item *replicaItem, priority float64) {
	item.priority = priority
	heap.Fix(pq, item.index)
}

var (
	errQueueDisabled     = errors.New("queue disabled")
	errQueueStopped      = errors.New("queue stopped")
	errReplicaNotAddable = errors.New("replica shouldn't be added to queue")
)

func isExpectedQueueError(err error) bool {
	cause := errors.Cause(err)
	return err == nil || cause == errQueueDisabled || cause == errReplicaNotAddable
}

// shouldQueueAgain is a helper function to determine whether the
// replica should be queued according to the current time, the last
// time the replica was processed, and the minimum interval between
// successive processing. Specifying minInterval=0 queues all replicas.
// Returns a bool for whether to queue as well as a priority based
// on how long it's been since last processed.
func shouldQueueAgain(now, last hlc.Timestamp, minInterval time.Duration) (bool, float64) {
	if minInterval == 0 || last == (hlc.Timestamp{}) {
		return true, 0
	}
	if diff := now.GoTime().Sub(last.GoTime()); diff >= minInterval {
		priority := float64(1)
		// If there's a non-zero last processed timestamp, adjust the
		// priority by a multiple of how long it's been since the last
		// time this replica was processed.
		if last != (hlc.Timestamp{}) {
			priority = float64(diff.Nanoseconds()) / float64(minInterval.Nanoseconds())
		}
		return true, priority
	}
	return false, 0
}

type queueImpl interface {
	// shouldQueue accepts current time, a replica, and the system config
	// and returns whether it should be queued and if so, at what priority.
	// The Replica is guaranteed to be initialized.
	shouldQueue(
		context.Context, hlc.Timestamp, *Replica, config.SystemConfig,
	) (shouldQueue bool, priority float64)

	// process accepts lease status, a replica, and the system config
	// and executes queue-specific work on it. The Replica is guaranteed
	// to be initialized.
	process(context.Context, *Replica, config.SystemConfig) error

	// timer returns a duration to wait between processing the next item
	// from the queue. The duration of the last processing of a replica
	// is supplied as an argument.
	timer(time.Duration) time.Duration

	// purgatoryChan returns a channel that is signaled when it's time
	// to retry replicas which have been relegated to purgatory due to
	// failures. If purgatoryChan returns nil, failing replicas are not
	// sent to purgatory.
	purgatoryChan() <-chan struct{}
}

type queueConfig struct {
	// maxSize is the maximum number of replicas to queue.
	maxSize int
	// needsLease controls whether this queue requires the range lease to
	// operate on a replica.
	needsLease bool
	// acceptsUnsplitRanges controls whether this queue can process ranges that
	// need to be split due to zone config settings. Ranges are checked before
	// calling queueImpl.shouldQueue and queueImpl.process.
	// This is to avoid giving the queue a replica that spans multiple config
	// zones (which might make the action of the queue ambiguous - e.g. we don't
	// want to try to replicate a range until we know which zone it is in and
	// therefore how many replicas are required).
	acceptsUnsplitRanges bool
	// processTimeout is the timeout for processing a replica.
	processTimeout time.Duration
	// successes is a counter of replicas processed successfully.
	successes *metric.Counter
	// failures is a counter of replicas which failed processing.
	failures *metric.Counter
	// pending is a gauge measuring current replica count pending.
	pending *metric.Gauge
	// processingNanos is a counter measuring total nanoseconds spent processing replicas.
	processingNanos *metric.Counter
	// purgatory is a gauge measuring current replica count in purgatory.
	purgatory *metric.Gauge
}

// baseQueue is the base implementation of the replicaQueue interface.
// Queue implementations should embed a baseQueue and implement queueImpl.
//
// In addition to normal processing of replicas via the replica
// scanner, queues have an optional notion of purgatory, where
// replicas which fail queue processing with a retryable error may be
// sent such that they will be quickly retried when the failure
// condition changes. Queue implementations opt in for purgatory by
// implementing the purgatoryChan method of queueImpl such that it
// returns a non-nil channel.
type baseQueue struct {
	log.AmbientContext

	name string
	// The constructor of the queueImpl structure MUST return a pointer.
	// This is because assigning queueImpl to a function-local, then
	// passing a pointer to it to `makeBaseQueue`, and then returning it
	// from the constructor function will return a queueImpl containing
	// a pointer to a structure which is a copy of the one within which
	// it is contained. DANGER.
	impl   queueImpl
	store  *Store
	gossip *gossip.Gossip
	queueConfig
	incoming chan struct{} // Channel signaled when a new replica is added to the queue.
	mu       struct {
		sync.Locker                                  // Protects all variables in the mu struct
		priorityQ   priorityQueue                    // The priority queue
		replicas    map[roachpb.RangeID]*replicaItem // Map from RangeID to replicaItem (for updating priority)
		purgatory   map[roachpb.RangeID]error        // Map of replicas to processing errors
		stopped     bool
		// Some tests in this package disable queues.
		disabled bool
	}

	// processMu synchronizes execution of processing for a single queue,
	// ensuring that we never process more than a single replica at a time. This
	// is needed because both the main processing loop and the purgatory loop can
	// process replicas.
	processMu sync.Locker
}

// newBaseQueue returns a new instance of baseQueue with the specified
// shouldQueue function to determine which replicas to queue and maxSize to
// limit the growth of the queue. Note that maxSize doesn't prevent new
// replicas from being added, it just limits the total size. Higher priority
// replicas can still be added; their addition simply removes the lowest
// priority replica.
func newBaseQueue(
	name string, impl queueImpl, store *Store, gossip *gossip.Gossip, cfg queueConfig,
) *baseQueue {
	// Use the default process timeout if none specified.
	if cfg.processTimeout == 0 {
		cfg.processTimeout = defaultProcessTimeout
	}

	ambient := store.cfg.AmbientCtx
	ambient.AddLogTag(name, nil)

	bq := baseQueue{
		AmbientContext: ambient,
		name:           name,
		impl:           impl,
		store:          store,
		gossip:         gossip,
		queueConfig:    cfg,
		incoming:       make(chan struct{}, 1),
	}
	bq.mu.Locker = new(syncutil.Mutex)
	bq.mu.replicas = map[roachpb.RangeID]*replicaItem{}
	bq.processMu = new(syncutil.Mutex)

	return &bq
}

// Length returns the current size of the queue.
func (bq *baseQueue) Length() int {
	bq.mu.Lock()
	defer bq.mu.Unlock()
	return bq.mu.priorityQ.Len()
}

// PurgatoryLength returns the current size of purgatory.
func (bq *baseQueue) PurgatoryLength() int {
	bq.mu.Lock()
	defer bq.mu.Unlock()
	return len(bq.mu.purgatory)
}

// SetDisabled turns queue processing off or on as directed.
func (bq *baseQueue) SetDisabled(disabled bool) {
	bq.mu.Lock()
	bq.mu.disabled = disabled
	bq.mu.Unlock()
}

// Disabled returns true is the queue is currently disabled.
func (bq *baseQueue) Disabled() bool {
	bq.mu.Lock()
	defer bq.mu.Unlock()
	return bq.mu.disabled
}

// Start launches a goroutine to process entries in the queue. The
// provided stopper is used to finish processing.
func (bq *baseQueue) Start(clock *hlc.Clock, stopper *stop.Stopper) {
	bq.processLoop(clock, stopper)
}

// Add adds the specified replica to the queue, regardless of the
// return value of bq.shouldQueue. The replica is added with specified
// priority. If the queue is too full, the replica may not be added,
// as the replica with the lowest priority will be dropped. Returns
// (true, nil) if the replica was added, (false, nil) if the replica
// was already present, and (false, err) if the replica could not be
// added for any other reason.
func (bq *baseQueue) Add(repl *Replica, priority float64) (bool, error) {
	bq.mu.Lock()
	defer bq.mu.Unlock()
	ctx := repl.AnnotateCtx(bq.AnnotateCtx(context.TODO()))
	return bq.addInternal(ctx, repl.Desc(), true, priority)
}

// MaybeAdd adds the specified replica if bq.shouldQueue specifies it
// should be queued. Replicas are added to the queue using the priority
// returned by bq.shouldQueue. If the queue is too full, the replica may
// not be added, as the replica with the lowest priority will be
// dropped.
func (bq *baseQueue) MaybeAdd(repl *Replica, now hlc.Timestamp) {
	// Load the system config.
	cfg, cfgOk := bq.gossip.GetSystemConfig()
	requiresSplit := cfgOk && bq.requiresSplit(cfg, repl)

	bq.mu.Lock()
	defer bq.mu.Unlock()

	if bq.mu.stopped || bq.mu.disabled {
		return
	}

	if !repl.IsInitialized() {
		return
	}

	ctx := repl.AnnotateCtx(bq.AnnotateCtx(context.TODO()))

	if !cfgOk {
		if log.V(1) {
			log.Infof(ctx, "no system config available. skipping")
		}
		return
	}

	if requiresSplit {
		// Range needs to be split due to zone configs, but queue does
		// not accept unsplit ranges.
		if log.V(1) {
			log.Infof(ctx, "split needed; not adding")
		}
		return
	}

	if bq.needsLease {
		// Check to see if either we own the lease or do not know who the lease
		// holder is.
		if lease, _ := repl.getLease(); repl.IsLeaseValid(lease, now) &&
			!lease.OwnedBy(repl.store.StoreID()) {
			if log.V(1) {
				log.Infof(ctx, "needs lease; not adding: %+v", lease)
			}
			return
		}
	}

	should, priority := bq.impl.shouldQueue(ctx, now, repl, cfg)
	if _, err := bq.addInternal(ctx, repl.Desc(), should, priority); !isExpectedQueueError(err) {
		log.Errorf(ctx, "unable to add: %s", err)
	}
}

func (bq *baseQueue) requiresSplit(cfg config.SystemConfig, repl *Replica) bool {
	if bq.acceptsUnsplitRanges {
		return false
	}
	// If there's no store (as is the case in some narrow unit tests),
	// the "required" split will never come. In that case, pretend we
	// don't require the split.
	if store := repl.store; store == nil {
		return false
	}
	desc := repl.Desc()
	return cfg.NeedsSplit(desc.StartKey, desc.EndKey)
}

// addInternal adds the replica the queue with specified priority. If
// the replica is already queued, updates the existing
// priority. Expects the queue lock to be held by caller.
func (bq *baseQueue) addInternal(
	ctx context.Context, desc *roachpb.RangeDescriptor, should bool, priority float64,
) (bool, error) {
	if bq.mu.stopped {
		return false, errQueueStopped
	}

	if bq.mu.disabled {
		if log.V(3) {
			log.Infof(ctx, "queue disabled")
		}
		return false, errQueueDisabled
	}

	if !desc.IsInitialized() {
		// We checked this above in MaybeAdd(), but we need to check it
		// again for Add().
		return false, errors.New("replica not initialized")
	}

	// If the replica is currently in purgatory, don't re-add it.
	if _, ok := bq.mu.purgatory[desc.RangeID]; ok {
		return false, nil
	}

	item, ok := bq.mu.replicas[desc.RangeID]
	if !should {
		if ok {
			if log.V(1) {
				log.Infof(ctx, "%s: removing from queue", item.value)
			}
			bq.remove(item)
		}
		return false, errReplicaNotAddable
	} else if ok {
		if item.priority != priority {
			if log.V(1) {
				log.Infof(ctx, "updating priority: %0.3f -> %0.3f", item.priority, priority)
			}
		}
		// Replica has already been added; update priority.
		bq.mu.priorityQ.update(item, priority)
		return false, nil
	}

	if log.V(3) {
		log.Infof(ctx, "adding: priority=%0.3f", priority)
	}
	item = &replicaItem{value: desc.RangeID, priority: priority}
	bq.add(item)

	// If adding this replica has pushed the queue past its maximum size,
	// remove the lowest priority element.
	if pqLen := bq.mu.priorityQ.Len(); pqLen > bq.maxSize {
		bq.remove(bq.mu.priorityQ[pqLen-1])
	}
	// Signal the processLoop that a replica has been added.
	select {
	case bq.incoming <- struct{}{}:
	default:
		// No need to signal again.
	}
	return true, nil
}

// MaybeRemove removes the specified replica from the queue if enqueued.
func (bq *baseQueue) MaybeRemove(rangeID roachpb.RangeID) {
	bq.mu.Lock()
	defer bq.mu.Unlock()

	if bq.mu.stopped {
		return
	}

	if item, ok := bq.mu.replicas[rangeID]; ok {
		ctx := bq.AnnotateCtx(context.TODO())
		if log.V(3) {
			log.Infof(ctx, "%s: removing", item.value)
		}
		bq.remove(item)
	}
}

// processLoop processes the entries in the queue until the provided
// stopper signals exit.
func (bq *baseQueue) processLoop(clock *hlc.Clock, stopper *stop.Stopper) {
	ctx := bq.AnnotateCtx(context.Background())
	stopper.RunWorker(ctx, func(ctx context.Context) {
		defer func() {
			bq.mu.Lock()
			bq.mu.stopped = true
			bq.mu.Unlock()
		}()

		// nextTime is initially nil; we don't start any timers until the queue
		// becomes non-empty.
		var nextTime <-chan time.Time

		immediately := make(chan time.Time)
		close(immediately)

		for {
			select {
			// Exit on stopper.
			case <-stopper.ShouldStop():
				return

			// Incoming signal sets the next time to process if there were previously
			// no replicas in the queue.
			case <-bq.incoming:
				if nextTime == nil {
					// When a replica is added, wake up immediately. This is mainly
					// to facilitate testing without unnecessary sleeps.
					nextTime = immediately

					// In case we're in a test, still block on the impl.
					bq.impl.timer(0)
				}
			// Process replicas as the timer expires.
			case <-nextTime:
				repl := bq.pop()
				var duration time.Duration
				if repl != nil {
					annotatedCtx := repl.AnnotateCtx(ctx)
					if stopper.RunTask(annotatedCtx, func(annotatedCtx context.Context) {
						start := timeutil.Now()
						if err := bq.processReplica(annotatedCtx, repl, clock); err != nil {
							// Maybe add failing replica to purgatory if the queue supports it.
							bq.maybeAddToPurgatory(annotatedCtx, repl, err, clock, stopper)
						}
						duration = timeutil.Since(start)
						if log.V(2) {
							log.Infof(annotatedCtx, "done %s", duration)
						}
						bq.processingNanos.Inc(duration.Nanoseconds())
					}) != nil {
						return
					}
				}
				if bq.Length() == 0 {
					nextTime = nil
				} else {
					nextTime = time.After(bq.impl.timer(duration))
				}
			}
		}
	})
}

// processReplica processes a single replica. This should not be
// called externally to the queue. bq.mu.Lock must not be held
// while calling this method.
func (bq *baseQueue) processReplica(
	queueCtx context.Context, repl *Replica, clock *hlc.Clock,
) error {
	bq.processMu.Lock()
	defer bq.processMu.Unlock()

	// Load the system config.
	cfg, ok := bq.gossip.GetSystemConfig()
	if !ok {
		if log.V(1) {
			log.Infof(queueCtx, "no system config available, skipping")
		}
		return nil
	}

	if bq.requiresSplit(cfg, repl) {
		// Range needs to be split due to zone configs, but queue does
		// not accept unsplit ranges.
		if log.V(3) {
			log.Infof(queueCtx, "split needed; skipping")
		}
		return nil
	}

	// Putting a span in a context means that events will no longer go to the
	// event log. Use queueCtx for events that are intended for the event log.
	ctx, span := bq.AnnotateCtxWithSpan(queueCtx, bq.name)
	defer span.Finish()
	// Also add the Replica annotations to ctx.
	ctx = repl.AnnotateCtx(ctx)
	ctx, cancel := context.WithTimeout(ctx, bq.processTimeout)
	defer cancel()
	if log.V(1) {
		log.Infof(ctx, "processing replica")
	}

	if !repl.IsInitialized() {
		// We checked this when adding the replica, but we need to check it again
		// in case this is a different replica with the same range ID (see #14193).
		return errors.New("cannot process uninitialized replica")
	}

	if err := repl.IsDestroyed(); err != nil {
		if log.V(3) {
			log.Infof(queueCtx, "replica destroyed (%s); skipping", err)
		}
		return nil
	}

	// If the queue requires a replica to have the range lease in
	// order to be processed, check whether this replica has range lease
	// and renew or acquire if necessary.
	if bq.needsLease {
		if _, pErr := repl.redirectOnOrAcquireLease(ctx); pErr != nil {
			switch v := pErr.GetDetail().(type) {
			case *roachpb.NotLeaseHolderError, *roachpb.RangeNotFoundError:
				if log.V(3) {
					log.Infof(queueCtx, "%s; skipping", v)
				}
				return nil
			default:
				return errors.Wrapf(pErr.GoError(), "%s: could not obtain lease", repl)
			}
		}
	}

	if log.V(3) {
		log.Infof(queueCtx, "processing")
	}
	if err := bq.impl.process(ctx, repl, cfg); err != nil {
		return err
	}
	if log.V(3) {
		log.Infof(ctx, "done")
	}
	bq.successes.Inc(1)
	return nil
}

// maybeAddToPurgatory possibly adds the specified replica to the
// purgatory queue, which holds replicas which have failed
// processing. To be added, the failing error must implement
// purgatoryError and the queue implementation must have its own
// mechanism for signaling re-processing of replicas held in
// purgatory.
func (bq *baseQueue) maybeAddToPurgatory(
	ctx context.Context, repl *Replica, triggeringErr error, clock *hlc.Clock, stopper *stop.Stopper,
) {
	// Increment failures metric here to capture all error returns from
	// process().
	bq.failures.Inc(1)

	// Check whether the failure is a purgatory error and whether the queue supports it.
	if _, ok := triggeringErr.(purgatoryError); !ok || bq.impl.purgatoryChan() == nil {
		log.Error(ctx, triggeringErr)
		return
	}
	bq.mu.Lock()
	defer bq.mu.Unlock()

	// First, check whether the replica has already been re-added to queue.
	if _, ok := bq.mu.replicas[repl.RangeID]; ok {
		return
	}

	if log.V(1) {
		log.Info(ctx, errors.Wrap(triggeringErr, "purgatory"))
	}

	item := &replicaItem{value: repl.RangeID}
	bq.mu.replicas[repl.RangeID] = item

	defer func() {
		bq.purgatory.Update(int64(len(bq.mu.purgatory)))
	}()

	// If purgatory already exists, just add to the map and we're done.
	if bq.mu.purgatory != nil {
		bq.mu.purgatory[repl.RangeID] = triggeringErr
		return
	}

	// Otherwise, create purgatory and start processing.
	bq.mu.purgatory = map[roachpb.RangeID]error{
		repl.RangeID: triggeringErr,
	}

	workerCtx := bq.AnnotateCtx(context.Background())
	stopper.RunWorker(workerCtx, func(ctx context.Context) {
		ticker := time.NewTicker(purgatoryReportInterval)
		for {
			select {
			case <-bq.impl.purgatoryChan():
				// Remove all items from purgatory into a copied slice.
				bq.mu.Lock()
				ranges := make([]roachpb.RangeID, 0, len(bq.mu.purgatory))
				for rangeID := range bq.mu.purgatory {
					item := bq.mu.replicas[rangeID]
					ranges = append(ranges, item.value)
					bq.remove(item)
				}
				bq.mu.Unlock()
				for _, id := range ranges {
					repl, err := bq.store.GetReplica(id)
					if err != nil {
						log.Errorf(ctx, "range %s no longer exists on store: %s", id, err)
						return
					}
					annotatedCtx := repl.AnnotateCtx(ctx)
					if stopper.RunTask(annotatedCtx, func(annotatedCtx context.Context) {
						if err := bq.processReplica(annotatedCtx, repl, clock); err != nil {
							bq.maybeAddToPurgatory(annotatedCtx, repl, err, clock, stopper)
						}
					}) != nil {
						return
					}
				}
				bq.mu.Lock()
				if len(bq.mu.purgatory) == 0 {
					log.Infof(ctx, "purgatory is now empty")
					bq.mu.purgatory = nil
					bq.mu.Unlock()
					return
				}
				bq.mu.Unlock()
			case <-ticker.C:
				// Report purgatory status.
				bq.mu.Lock()
				errMap := map[string]int{}
				for _, err := range bq.mu.purgatory {
					errMap[err.Error()]++
				}
				bq.mu.Unlock()
				for errStr, count := range errMap {
					log.Errorf(ctx, "%d replicas failing with %q", count, errStr)
				}
			case <-stopper.ShouldStop():
				return
			}
		}
	})
}

// pop dequeues the highest priority replica, if any, in the queue. Expects
// mutex to be locked.
func (bq *baseQueue) pop() *Replica {
	var repl *Replica
	for repl == nil {
		bq.mu.Lock()

		if bq.mu.priorityQ.Len() == 0 {
			bq.mu.Unlock()
			return nil
		}
		item := heap.Pop(&bq.mu.priorityQ).(*replicaItem)
		bq.pending.Update(int64(bq.mu.priorityQ.Len()))
		delete(bq.mu.replicas, item.value)
		bq.mu.Unlock()
		repl, _ = bq.store.GetReplica(item.value)
	}
	return repl
}

// add adds an element to the priority queue. Caller must hold mutex.
func (bq *baseQueue) add(item *replicaItem) {
	heap.Push(&bq.mu.priorityQ, item)
	bq.pending.Update(int64(bq.mu.priorityQ.Len()))
	bq.mu.replicas[item.value] = item
}

// remove removes an element from purgatory (if it's experienced an
// error) or from the priority queue by index. Caller must hold mutex.
func (bq *baseQueue) remove(item *replicaItem) {
	if _, ok := bq.mu.purgatory[item.value]; ok {
		delete(bq.mu.purgatory, item.value)
		bq.purgatory.Update(int64(len(bq.mu.purgatory)))
	} else {
		heap.Remove(&bq.mu.priorityQ, item.index)
		bq.pending.Update(int64(bq.mu.priorityQ.Len()))
	}
	delete(bq.mu.replicas, item.value)
}

// DrainQueue locks the queue and processes the remaining queued replicas. It
// processes the replicas in the order they're queued in, one at a time.
// Exposed for testing only.
//
// TODO(bdarnell): this method may race with the call to bq.pop() in
// the main loop, in which case it does not guarantee that all
// replicas have been processed by the time it returns. This is most
// noticeable with ForceReplicaGCScanAndProcess, since the replica GC
// queue has many event-driven triggers. This should synchronize
// somehow with processLoop so we wait for anything being handled
// there to finish too. When that's done, the SucceedsSoon at the end
// of TestRemoveRangeWithoutGC (and perhaps others) can be replaced
// with a one-time check.
func (bq *baseQueue) DrainQueue(clock *hlc.Clock) {
	ctx := bq.AnnotateCtx(context.TODO())
	for repl := bq.pop(); repl != nil; repl = bq.pop() {
		annotatedCtx := repl.AnnotateCtx(ctx)
		if err := bq.processReplica(annotatedCtx, repl, clock); err != nil {
			bq.failures.Inc(1)
			log.Error(annotatedCtx, err)
		}
	}
}
