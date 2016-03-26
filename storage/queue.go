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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/net/trace"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/timeutil"
	"github.com/opentracing/opentracing-go"
)

const (
	// purgatoryReportInterval is the duration between reports on purgatory status.
	purgatoryReportInterval = 10 * time.Minute
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
	value    *Replica
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
	errReplicaNotAddable = errors.New("replica shouldn't be added to queue")
)

type queueImpl interface {
	// needsLeaderLease returns whether this queue requires the leader
	// lease to operate on a replica.
	needsLeaderLease() bool

	// acceptsUnsplitRanges returns whether this queue can process
	// ranges that need to be split due to zone config settings.
	// Ranges are checked before calling shouldQueue and process.
	acceptsUnsplitRanges() bool

	// shouldQueue accepts current time, a replica, and the system config
	// and returns whether it should be queued and if so, at what priority.
	shouldQueue(roachpb.Timestamp, *Replica, config.SystemConfig) (shouldQueue bool, priority float64)

	// process accepts current time, a replica, and the system config
	// and executes queue-specific work on it.
	// TODO(nvanbenschoten) this should take a context.Context.
	process(roachpb.Timestamp, *Replica, config.SystemConfig) error

	// timer returns a duration to wait between processing the next item
	// from the queue.
	timer() time.Duration

	// purgatoryChan returns a channel that is signaled when it's time
	// to retry replicas which have been relegated to purgatory due to
	// failures. If purgatoryChan returns nil, failing replicas are not
	// sent to purgatory.
	purgatoryChan() <-chan struct{}
}

type queueLog struct {
	traceLog trace.EventLog
	prefix   string
}

func (l queueLog) Infof(logv bool, format string, a ...interface{}) {
	if logv {
		log.InfofDepth(1, l.prefix+format, a...)
	}
	l.traceLog.Printf(format, a...)
}

func (l queueLog) Errorf(format string, a ...interface{}) {
	log.ErrorfDepth(1, l.prefix+format, a...)
	l.traceLog.Errorf(format, a...)
}

func (l queueLog) Finish() {
	l.traceLog.Finish()
}

// baseQueue is the base implementation of the replicaQueue interface.
// Queue implementations should embed a baseQueue and implement queueImpl.
//
// baseQueue is not thread safe and is intended for usage only from
// the scanner's goroutine.
//
// In addition to normal processing of replicas via the replica
// scanner, queues have an optional notion of purgatory, where
// replicas which fail queue processing with a retryable error may be
// sent such that they will be quickly retried when the failure
// condition changes. Queue implementations opt in for purgatory by
// implementing the purgatoryChan method of queueImpl such that it
// returns a non-nil channel.
type baseQueue struct {
	name string
	// The constructor of the queueImpl structure MUST return a pointer.
	// This is because assigning queueImpl to a function-local, then
	// passing a pointer to it to `makeBaseQueue`, and then returning it
	// from the constructor function will return a queueImpl containing
	// a pointer to a structure which is a copy of the one within which
	// it is contained. DANGER.
	impl     queueImpl
	gossip   *gossip.Gossip
	maxSize  int           // Maximum number of replicas to queue
	incoming chan struct{} // Channel signaled when a new replica is added to the queue.
	mu       struct {
		sync.Locker                                  // Protects all variables in the mu struct
		priorityQ   priorityQueue                    // The priority queue
		replicas    map[roachpb.RangeID]*replicaItem // Map from RangeID to replicaItem (for updating priority)
		purgatory   map[roachpb.RangeID]error        // Map of replicas to processing errors
	}
	// Some tests in this package disable queues.
	disabled int32 // updated atomically

	// TODO(tamird): update all queues to use eventLog.
	eventLog queueLog
}

// makeBaseQueue returns a new instance of baseQueue with the
// specified shouldQueue function to determine which replicas to queue
// and maxSize to limit the growth of the queue. Note that
// maxSize doesn't prevent new replicas from being added, it just
// limits the total size. Higher priority replicas can still be
// added; their addition simply removes the lowest priority replica.
func makeBaseQueue(name string, impl queueImpl, gossip *gossip.Gossip, maxSize int) baseQueue {
	bq := baseQueue{
		name:     name,
		impl:     impl,
		gossip:   gossip,
		maxSize:  maxSize,
		incoming: make(chan struct{}, 1),
		eventLog: queueLog{
			traceLog: trace.NewEventLog("queue", name),
			prefix:   fmt.Sprintf("[%s] ", name),
		},
	}
	bq.mu.Locker = new(sync.Mutex)
	bq.mu.replicas = map[roachpb.RangeID]*replicaItem{}
	return bq
}

// Length returns the current size of the queue.
func (bq *baseQueue) Length() int {
	bq.mu.Lock()
	defer bq.mu.Unlock()
	return bq.mu.priorityQ.Len()
}

// SetDisabled turns queue processing off or on as directed.
func (bq *baseQueue) SetDisabled(disabled bool) {
	if disabled {
		atomic.StoreInt32(&bq.disabled, 1)
	} else {
		atomic.StoreInt32(&bq.disabled, 0)
	}
}

func (bq *baseQueue) Close() {
	bq.eventLog.Finish()
}

// Start launches a goroutine to process entries in the queue. The
// provided stopper is used to finish processing.
func (bq *baseQueue) Start(clock *hlc.Clock, stopper *stop.Stopper) {
	bq.processLoop(clock, stopper)
}

// Add adds the specified replica to the queue, regardless of the return
// value of bq.shouldQueue. The replica is added with specified
// priority. If the queue is too full, the replica may not be added, as
// the replica with the lowest priority will be dropped. Returns an
// error if the replica was not added.
func (bq *baseQueue) Add(repl *Replica, priority float64) error {
	bq.mu.Lock()
	defer bq.mu.Unlock()
	return bq.addInternal(repl, true, priority)
}

// MaybeAdd adds the specified replica if bq.shouldQueue specifies it
// should be queued. Replicas are added to the queue using the priority
// returned by bq.shouldQueue. If the queue is too full, the replica may
// not be added, as the replica with the lowest priority will be
// dropped.
func (bq *baseQueue) MaybeAdd(repl *Replica, now roachpb.Timestamp) {
	// Load the system config.
	cfg, ok := bq.gossip.GetSystemConfig()
	if !ok {
		bq.eventLog.Infof(log.V(1), "no system config available. skipping")
		return
	}

	desc := repl.Desc()
	if !bq.impl.acceptsUnsplitRanges() && cfg.NeedsSplit(desc.StartKey, desc.EndKey) {
		// Range needs to be split due to zone configs, but queue does
		// not accept unsplit ranges.
		bq.eventLog.Infof(log.V(1), "%s: split needed; not adding", repl)
		return
	}

	bq.mu.Lock()
	defer bq.mu.Unlock()
	should, priority := bq.impl.shouldQueue(now, repl, cfg)
	if err := bq.addInternal(repl, should, priority); err != nil {
		bq.eventLog.Infof(log.V(3), "unable to add %s: %s", repl, err)
	}
}

// addInternal adds the replica the queue with specified priority. If the
// replica is already queued, updates the existing priority. Expects the
// queue lock is held by caller. Returns an error if the replica was not
// added.
func (bq *baseQueue) addInternal(repl *Replica, should bool, priority float64) error {
	if atomic.LoadInt32(&bq.disabled) == 1 {
		bq.eventLog.Infof(false, "queue disabled")
		return errQueueDisabled
	}

	// If the replica is currently in purgatory, don't re-add it.
	if _, ok := bq.mu.purgatory[repl.RangeID]; ok {
		return nil
	}

	item, ok := bq.mu.replicas[repl.RangeID]
	if !should {
		if ok {
			bq.eventLog.Infof(false, "%s: removing", item.value)
			bq.remove(item)
		}
		return errReplicaNotAddable
	} else if ok {
		if item.priority != priority {
			bq.eventLog.Infof(false, "%s: updating priority: %0.3f -> %0.3f",
				repl, item.priority, priority)
		}
		// Replica has already been added; update priority.
		bq.mu.priorityQ.update(item, priority)
		return nil
	}

	bq.eventLog.Infof(log.V(3), "%s: adding: priority=%0.3f", repl, priority)
	item = &replicaItem{value: repl, priority: priority}
	heap.Push(&bq.mu.priorityQ, item)
	bq.mu.replicas[repl.RangeID] = item

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
	return nil
}

// MaybeRemove removes the specified replica from the queue if enqueued.
func (bq *baseQueue) MaybeRemove(repl *Replica) {
	bq.mu.Lock()
	defer bq.mu.Unlock()
	if item, ok := bq.mu.replicas[repl.RangeID]; ok {
		bq.eventLog.Infof(log.V(3), "%s: removing", item.value)
		bq.remove(item)
	}
}

// processLoop processes the entries in the queue until the provided
// stopper signals exit.
//
// TODO(spencer): current load should factor into replica processing timer.
func (bq *baseQueue) processLoop(clock *hlc.Clock, stopper *stop.Stopper) {
	stopper.RunWorker(func() {
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
					bq.impl.timer()
				}
			// Process replicas as the timer expires.
			case <-nextTime:
				bq.mu.Lock()
				repl := bq.pop()
				bq.mu.Unlock()
				if repl != nil {
					stopper.RunTask(func() {
						if err := bq.processReplica(repl, clock); err != nil {
							// Maybe add failing replica to purgatory if the queue supports it.
							bq.maybeAddToPurgatory(repl, err, clock, stopper)
						}
					})
				}
				if bq.Length() == 0 {
					nextTime = nil
				} else {
					nextTime = time.After(bq.impl.timer())
				}
			}
		}
	})
}

// processReplica processes a single replica. This should not be
// called externally to the queue. bq.mu.Lock should not be held
// while calling this method.
func (bq *baseQueue) processReplica(repl *Replica, clock *hlc.Clock) error {
	// Load the system config.
	cfg, ok := bq.gossip.GetSystemConfig()
	if !ok {
		bq.eventLog.Infof(log.V(1), "no system config available. skipping")
		return nil
	}

	desc := repl.Desc()
	if !bq.impl.acceptsUnsplitRanges() && cfg.NeedsSplit(desc.StartKey, desc.EndKey) {
		// Range needs to be split due to zone configs, but queue does
		// not accept unsplit ranges.
		bq.eventLog.Infof(log.V(3), "%s: split needed; skipping", repl)
		return nil
	}

	// If the queue requires a replica to have the range leader lease in
	// order to be processed, check whether this replica has leader lease
	// and renew or acquire if necessary.
	if bq.impl.needsLeaderLease() {
		sp := repl.store.Tracer().StartSpan(bq.name)
		ctx := opentracing.ContextWithSpan(repl.context(context.Background()), sp)
		defer sp.Finish()
		// Create a "fake" get request in order to invoke redirectOnOrAcquireLease.
		if err := repl.redirectOnOrAcquireLeaderLease(ctx); err != nil {
			bq.eventLog.Infof(log.V(3), "%s: could not acquire leader lease; skipping", repl)
			return nil
		}
	}

	bq.eventLog.Infof(log.V(3), "%s: processing", repl)
	start := timeutil.Now()
	if err := bq.impl.process(clock.Now(), repl, cfg); err != nil {
		return err
	}
	bq.eventLog.Infof(log.V(2), "%s: done: %s", repl, time.Since(start))
	return nil
}

// maybeAddToPurgatory possibly adds the specified replica to the
// purgatory queue, which holds replicas which have failed
// processing. To be added, the failing error must implement
// purgatoryError and the queue implementation must have its own
// mechanism for signaling re-processing of replicas held in
// purgatory.
func (bq *baseQueue) maybeAddToPurgatory(repl *Replica, err error, clock *hlc.Clock, stopper *stop.Stopper) {
	// Check whether the failure is a purgatory error and whether the queue supports it.
	if _, ok := err.(purgatoryError); !ok || bq.impl.purgatoryChan() == nil {
		bq.eventLog.Errorf("%s: error: %v", repl, err)
		return
	}
	bq.mu.Lock()
	defer bq.mu.Unlock()

	// First, check whether the replica has already been re-added to queue.
	if _, ok := bq.mu.replicas[repl.RangeID]; ok {
		return
	}

	bq.eventLog.Infof(log.V(2), "%s (purgatory): error: %v", repl, err)

	item := &replicaItem{value: repl}
	bq.mu.replicas[repl.RangeID] = item

	// If purgatory already exists, just add to the map and we're done.
	if bq.mu.purgatory != nil {
		bq.mu.purgatory[repl.RangeID] = err
		return
	}

	// Otherwise, create purgatory and start processing.
	bq.mu.purgatory = map[roachpb.RangeID]error{
		repl.RangeID: err,
	}

	stopper.RunWorker(func() {
		ticker := time.NewTicker(purgatoryReportInterval)
		for {
			select {
			case <-bq.impl.purgatoryChan():
				// Remove all items from purgatory into a copied slice.
				bq.mu.Lock()
				repls := make([]*Replica, 0, len(bq.mu.purgatory))
				for rangeID := range bq.mu.purgatory {
					item := bq.mu.replicas[rangeID]
					repls = append(repls, item.value)
					bq.remove(item)
				}
				bq.mu.Unlock()
				for _, repl := range repls {
					stopper.RunTask(func() {
						if err := bq.processReplica(repl, clock); err != nil {
							bq.maybeAddToPurgatory(repl, err, clock, stopper)
						}
					})
				}
				bq.mu.Lock()
				if len(bq.mu.purgatory) == 0 {
					bq.eventLog.Infof(log.V(0), "purgatory is now empty")
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
					bq.eventLog.Errorf("%d replicas failing with %q", count, errStr)
				}
			case <-stopper.ShouldStop():
				return
			}
		}
	})
}

// pop dequeues the highest priority replica in the queue. Returns the
// replica if not empty; otherwise, returns nil. Expects mutex to be
// locked.
func (bq *baseQueue) pop() *Replica {
	if bq.mu.priorityQ.Len() == 0 {
		return nil
	}
	item := heap.Pop(&bq.mu.priorityQ).(*replicaItem)
	delete(bq.mu.replicas, item.value.RangeID)
	return item.value
}

// remove removes an element from purgatory (if it's experienced an
// error) or from the priority queue by index. Caller must hold mutex.
func (bq *baseQueue) remove(item *replicaItem) {
	if _, ok := bq.mu.purgatory[item.value.RangeID]; ok {
		delete(bq.mu.purgatory, item.value.RangeID)
	} else {
		heap.Remove(&bq.mu.priorityQ, item.index)
	}
	delete(bq.mu.replicas, item.value.RangeID)
}

// DrainQueue locks the queue and processes the remaining queued replicas. It
// processes the replicas in the order they're queued in, one at a time.
// Exposed for testing only.
func (bq *baseQueue) DrainQueue(clock *hlc.Clock) {
	bq.mu.Lock()
	repl := bq.pop()
	bq.mu.Unlock()
	for repl != nil {
		if err := bq.processReplica(repl, clock); err != nil {
			bq.eventLog.Errorf("failed processing replica %s: %s", repl, err)
		}
		bq.mu.Lock()
		repl = bq.pop()
		bq.mu.Unlock()
	}
}
