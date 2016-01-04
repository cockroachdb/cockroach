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

	"golang.org/x/net/trace"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

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
	shouldQueue(roachpb.Timestamp, *Replica, *config.SystemConfig) (shouldQueue bool, priority float64)

	// process accepts current time, a replica, and the system config
	// and executes queue-specific work on it.
	process(roachpb.Timestamp, *Replica, *config.SystemConfig) error

	// timer returns a duration to wait between processing the next item
	// from the queue.
	timer() time.Duration
}

type queueLog struct {
	traceLog trace.EventLog
	prefix   string
}

func (l queueLog) Infof(logv bool, format string, a ...interface{}) {
	if logv {
		log.Infof(l.prefix+format, a...)
	}
	l.traceLog.Printf(format, a...)
}

func (l queueLog) Errorf(format string, a ...interface{}) {
	log.Errorf(l.prefix+format, a...)
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
type baseQueue struct {
	name string
	// The constructor of the queueImpl structure MUST return a pointer.
	// This is because assigning queueImpl to a function-local, then
	// passing a pointer to it to `makeBaseQueue`, and then returning it
	// from the constructor function will return a queueImpl containing
	// a pointer to a structure which is a copy of the one within which
	// it is contained. DANGER.
	impl        queueImpl
	gossip      *gossip.Gossip
	maxSize     int                              // Maximum number of replicas to queue
	incoming    chan struct{}                    // Channel signaled when a new replica is added to the queue.
	sync.Locker                                  // Protects priorityQ and replicas
	priorityQ   priorityQueue                    // The priority queue
	replicas    map[roachpb.RangeID]*replicaItem // Map from RangeID to replicaItem (for updating priority)
	// Some tests in this package disable queues.
	disabled int32 // updated atomically

	eventLog queueLog
}

// makeBaseQueue returns a new instance of baseQueue with the
// specified shouldQueue function to determine which replicas to queue
// and maxSize to limit the growth of the queue. Note that
// maxSize doesn't prevent new replicas from being added, it just
// limits the total size. Higher priority replicas can still be
// added; their addition simply removes the lowest priority replica.
func makeBaseQueue(name string, impl queueImpl, gossip *gossip.Gossip, maxSize int) baseQueue {
	return baseQueue{
		name:     name,
		impl:     impl,
		gossip:   gossip,
		maxSize:  maxSize,
		incoming: make(chan struct{}, 1),
		Locker:   new(sync.Mutex),
		replicas: map[roachpb.RangeID]*replicaItem{},
		eventLog: queueLog{
			traceLog: trace.NewEventLog("queue", name),
			prefix:   fmt.Sprintf("[%s] ", name),
		},
	}
}

// Length returns the current size of the queue.
func (bq *baseQueue) Length() int {
	bq.Lock()
	defer bq.Unlock()
	return bq.priorityQ.Len()
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
	bq.Lock()
	defer bq.Unlock()
	return bq.addInternal(repl, true, priority)
}

// MaybeAdd adds the specified replica if bq.shouldQueue specifies it
// should be queued. Replicas are added to the queue using the priority
// returned by bq.shouldQueue. If the queue is too full, the replica may
// not be added, as the replica with the lowest priority will be
// dropped.
func (bq *baseQueue) MaybeAdd(repl *Replica, now roachpb.Timestamp) {
	// Load the system config.
	cfg := bq.gossip.GetSystemConfig()
	if cfg == nil {
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

	bq.Lock()
	defer bq.Unlock()
	should, priority := bq.impl.shouldQueue(now, repl, cfg)
	if err := bq.addInternal(repl, should, priority); err != nil {
		if log.V(3) {
			log.Infof("[%s] unable to add %s: %s", bq.name, repl, err)
		}
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

	rangeID := repl.Desc().RangeID

	item, ok := bq.replicas[rangeID]
	if !should {
		if ok {
			bq.eventLog.Infof(false, "%s: removing", item.value)
			bq.remove(item.index)
		}
		return errReplicaNotAddable
	} else if ok {
		if item.priority != priority {
			bq.eventLog.Infof(false, "%s: updating priority: %0.3f -> %0.3f",
				repl, item.priority, priority)
		}
		// Replica has already been added; update priority.
		bq.priorityQ.update(item, priority)
		return nil
	}

	bq.eventLog.Infof(log.V(3), "%s: adding: priority=%0.3f", repl, priority)
	item = &replicaItem{value: repl, priority: priority}
	heap.Push(&bq.priorityQ, item)
	bq.replicas[rangeID] = item

	// If adding this replica has pushed the queue past its maximum size,
	// remove the lowest priority element.
	if pqLen := bq.priorityQ.Len(); pqLen > bq.maxSize {
		bq.remove(pqLen - 1)
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
	bq.Lock()
	defer bq.Unlock()
	if item, ok := bq.replicas[repl.Desc().RangeID]; ok {
		bq.eventLog.Infof(log.V(3), "%s: removing", item.value)
		bq.remove(item.index)
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
				stopper.RunTask(func() {
					bq.processOne(clock)
				})
				if bq.Length() == 0 {
					nextTime = nil
				} else {
					nextTime = time.After(bq.impl.timer())
				}

			// Exit on stopper.
			case <-stopper.ShouldStop():
				bq.Lock()
				bq.replicas = map[roachpb.RangeID]*replicaItem{}
				bq.priorityQ = nil
				bq.Unlock()
				return
			}
		}
	})
}

// processOne processes the next replica in the queue.
func (bq *baseQueue) processOne(clock *hlc.Clock) {
	bq.Lock()
	repl := bq.pop()
	bq.Unlock()

	if repl == nil {
		return
	}

	bq.processReplica(repl, clock)
}

// processReplica processes a single replica.
// This should not be called externally to the queue.
func (bq *baseQueue) processReplica(repl *Replica, clock *hlc.Clock) {
	start := time.Now()
	now := clock.Now()

	// Load the system config.
	cfg := bq.gossip.GetSystemConfig()
	if cfg == nil {
		log.Infof("no system config available. skipping")
		return
	}

	desc := repl.Desc()
	if !bq.impl.acceptsUnsplitRanges() && cfg.NeedsSplit(desc.StartKey, desc.EndKey) {
		// Range needs to be split due to zone configs, but queue does
		// not accept unsplit ranges.
		bq.eventLog.Infof(log.V(3), "%s: split needed; skipping", repl)
		return
	}

	// If the queue requires a replica to have the range leader lease in
	// order to be processed, check whether this replica has leader lease
	// and renew or acquire if necessary.
	if bq.impl.needsLeaderLease() {
		// Create a "fake" get request in order to invoke redirectOnOrAcquireLease.
		if err := repl.redirectOnOrAcquireLeaderLease(nil /* Trace */, now); err != nil {
			bq.eventLog.Infof(log.V(3), "%s: could not acquire leader lease; skipping", repl)
			return
		}
	}

	bq.eventLog.Infof(log.V(3), "%s: processing", repl)

	if err := bq.impl.process(now, repl, cfg); err != nil {
		bq.eventLog.Errorf("%s: error: %v", repl, err)
	} else {
		bq.eventLog.Infof(log.V(2), "%s: done: %0.2fms", repl,
			time.Since(start).Seconds()*1000)
	}
}

// pop dequeues the highest priority replica in the queue. Returns the
// replica if not empty; otherwise, returns nil. Expects mutex to be
// locked.
func (bq *baseQueue) pop() *Replica {
	if bq.priorityQ.Len() == 0 {
		return nil
	}
	item := heap.Pop(&bq.priorityQ).(*replicaItem)
	delete(bq.replicas, item.value.Desc().RangeID)
	return item.value
}

// remove removes an element from the priority queue by index. Expects
// mutex to be locked.
func (bq *baseQueue) remove(index int) {
	item := heap.Remove(&bq.priorityQ, index).(*replicaItem)
	delete(bq.replicas, item.value.Desc().RangeID)
}

// DrainQueue locks the queue and processes the remaining queued replicas. It
// processes the replicas in the order they're queued in, one at a time.
// Exposed for testing only.
func (bq *baseQueue) DrainQueue(clock *hlc.Clock) {
	bq.Lock()
	defer bq.Unlock()
	for repl := bq.pop(); repl != nil; repl = bq.pop() {
		bq.processReplica(repl, clock)
	}
}
