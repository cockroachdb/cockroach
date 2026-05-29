// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiqueue

import (
	"container/heap"
	"math"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// WeightedPermit is the opaque token returned by WeightedMultiQueue
// dispatches. Each permit must be passed to exactly one Release call on the
// issuing queue (or returned via Cancel before the caller receives it).
type WeightedPermit struct {
	// typeIndex is the queue's internal index for the issuing type, used by
	// Release to update per-type accounting without a map lookup.
	typeIndex int8
	valid     bool
}

// WeightedMultiQueue is a fixed-configuration variant of MultiQueue that
// dispatches across a known set of queue types using a weighted round-robin
// cycle, with optional per-type concurrency caps. All queue types must be
// declared at construction; the dispatch cycle is precomputed.
//
// Each queue type t is assigned Weight consecutive positions in a cycle of
// length sum(Weight). The dispatcher walks the cycle from cyclePosition and
// grants the next free slot to the first eligible type it finds — eligible
// meaning non-empty and below its MaxActive cap. Empty or capped types yield
// their slots, so e.g. an empty high-pri queue lets low-pri receive every
// dispatch (subject to its cap), and a low-pri queue at its cap lets every
// freed slot go to high-pri. Within a type, tasks are heap-ordered by
// priority.
//
// MaxActive caps and the total concurrency limit may be updated at runtime
// via UpdateConfig; weights and the set of types are immutable after
// construction.
type WeightedMultiQueue struct {
	// typeIndex maps a user-facing QueueType to its internal index in the
	// per-type slices below. Immutable after construction.
	typeIndex map[int]int

	// weights is the per-type dispatch weight, indexed by internal type index.
	// Immutable after construction.
	weights []int

	// cycle is the precomputed dispatch order. cycle[k] is the internal type
	// index whose turn it is at cycle position k. len(cycle) == sum(weights).
	//
	// The cycle is built as contiguous per-type blocks (weights [8,1] yield
	// [H,H,H,H,H,H,H,H,L]).
	cycle []int

	mu struct {
		syncutil.Mutex

		concurrencyLimit int
		remainingRuns    int

		// maxActive caps concurrent permits per type; 0 means uncapped. Indexed
		// by internal type index. Mutable via UpdateConfig.
		maxActive []int

		// activeByType counts permits currently held per type.
		activeByType []int

		// waiters is the per-type heap of queued tasks.
		waiters []notifyHeap[WeightedPermit]

		cyclePosition int
	}
}

// TypeConfig configures one queue type within a WeightedMultiQueue.
type TypeConfig struct {
	// QueueType is the user-facing identifier passed to Add/Cancel.
	QueueType int
	// Weight is the dispatch weight (must be positive). Higher weights
	// receive proportionally more dispatches under contention.
	Weight int
	// MaxActive caps concurrent permits held by this type; 0 is uncapped.
	MaxActive int
}

// NewWeightedMultiQueue constructs a queue with the given total concurrency
// and per-type configs. Slice order determines cycle order. QueueType
// identifiers must be unique. Panics if len(types) > math.MaxInt8 (127),
// since WeightedPermit stores the type index in an int8.
func NewWeightedMultiQueue(maxConcurrency int, types []TypeConfig) *WeightedMultiQueue {
	if len(types) == 0 {
		panic(errors.AssertionFailedf("WeightedMultiQueue requires at least one TypeConfig"))
	}
	if len(types) > math.MaxInt8 {
		panic(errors.AssertionFailedf(
			"WeightedMultiQueue supports at most %d types, got %d", math.MaxInt8, len(types)))
	}
	q := &WeightedMultiQueue{
		typeIndex: make(map[int]int, len(types)),
		weights:   make([]int, len(types)),
	}
	q.mu.concurrencyLimit = maxConcurrency
	q.mu.remainingRuns = maxConcurrency
	q.mu.maxActive = make([]int, len(types))
	q.mu.activeByType = make([]int, len(types))
	q.mu.waiters = make([]notifyHeap[WeightedPermit], len(types))
	totalWeight := 0
	for i, tc := range types {
		if tc.Weight <= 0 {
			panic(errors.AssertionFailedf(
				"queue type %d weight must be positive, got %d", tc.QueueType, tc.Weight))
		}
		if tc.MaxActive < 0 {
			panic(errors.AssertionFailedf(
				"queue type %d MaxActive must be non-negative, got %d", tc.QueueType, tc.MaxActive))
		}
		if _, dup := q.typeIndex[tc.QueueType]; dup {
			panic(errors.AssertionFailedf("duplicate queue type %d", tc.QueueType))
		}
		q.typeIndex[tc.QueueType] = i
		q.weights[i] = tc.Weight
		q.mu.maxActive[i] = tc.MaxActive
		totalWeight += tc.Weight
	}
	q.cycle = make([]int, 0, totalWeight)
	for i, w := range q.weights {
		for j := 0; j < w; j++ {
			q.cycle = append(q.cycle, i)
		}
	}
	return q
}

// Add submits a task at the given queueType and priority. Returns an error
// for an unknown queueType, or if the pool is saturated and adding would
// exceed maxQueueLength (when non-negative). The caller must Release the
// returned task's permit (or Cancel before dispatch).
func (q *WeightedMultiQueue) Add(
	queueType int, priority float64, maxQueueLength int64,
) (*Task[WeightedPermit], error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	idx, ok := q.typeIndex[queueType]
	if !ok {
		return nil, errors.AssertionFailedf("unknown queue type %d", queueType)
	}

	// Reject before queueing if the pool is saturated and a length cap was
	// requested. Mirrors MultiQueue.Add.
	if q.mu.remainingRuns == 0 && maxQueueLength >= 0 {
		if int64(q.queueLenLocked()) > maxQueueLength {
			return nil, errors.Newf("queue full: %d queued, max %d",
				q.queueLenLocked(), maxQueueLength)
		}
	}

	task := &Task[WeightedPermit]{
		priority:  priority,
		permitC:   make(chan *WeightedPermit, 1),
		heapIdx:   -1,
		queueType: queueType,
	}
	heap.Push(&q.mu.waiters[idx], task)
	q.tryRunNextLocked()
	return task, nil
}

// Cancel removes a task from its queue. If the dispatcher has already
// delivered a permit, Cancel races the caller's receive: whichever wins
// releases the permit. A no-op if the caller has already received it.
func (q *WeightedMultiQueue) Cancel(task *Task[WeightedPermit]) {
	q.mu.Lock()
	defer q.mu.Unlock()
	idx, ok := q.typeIndex[task.queueType]
	if !ok {
		panic(errors.AssertionFailedf("unknown queue type %d", task.queueType))
	}
	// Tasks removed from the heap before dispatch never incremented
	// activeByType, so only the drain path needs releaseLocked.
	if q.mu.waiters[idx].tryRemove(task) {
		close(task.permitC)
		return
	}
	if p := tryDrainPermit(task); p != nil {
		q.releaseLocked(p)
	}
}

// Release returns a permit to the queue. Panics on double release.
func (q *WeightedMultiQueue) Release(p *WeightedPermit) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.releaseLocked(p)
}

func (q *WeightedMultiQueue) releaseLocked(p *WeightedPermit) {
	if !p.valid {
		panic(errors.AssertionFailedf("double release of permit"))
	}
	p.valid = false
	q.mu.activeByType[p.typeIndex]--
	// After UpdateConfig lowers concurrencyLimit while permits are held, we
	// can be over-active (totalActive > concurrencyLimit, remainingRuns = 0).
	// In that state a release shrinks the over-allocation rather than freeing
	// a slot; remainingRuns stays at 0 until activity drops below the new
	// limit. Once we cross back below, normal accounting resumes.
	if q.totalActiveLocked() < q.mu.concurrencyLimit {
		q.mu.remainingRuns++
	}
	q.tryRunNextLocked()
}

// totalActiveLocked returns the count of currently-held permits across all
// types.
func (q *WeightedMultiQueue) totalActiveLocked() int {
	sum := 0
	for _, a := range q.mu.activeByType {
		sum += a
	}
	return sum
}

// ConfigOption configures a WeightedMultiQueue update applied via
// UpdateConfig. All options in a single UpdateConfig call take effect under
// the same lock acquisition, so observers cannot see an interim state.
type ConfigOption interface {
	apply(*configOptions)
}

type configOptions struct {
	concurrencyLimit *int
	maxActiveByType  map[int]int
}

type configOptionFunc func(*configOptions)

func (f configOptionFunc) apply(o *configOptions) { f(o) }

// WithConcurrencyLimit sets the total concurrency cap. Already-active tasks
// are unaffected; queued tasks may dispatch immediately on a relax.
func WithConcurrencyLimit(n int) ConfigOption {
	return configOptionFunc(func(o *configOptions) {
		o.concurrencyLimit = &n
	})
}

// WithTypeMaxActive sets the per-type cap. Pass 0 to remove. Panics on
// unknown queueType or negative maxActive when applied.
func WithTypeMaxActive(queueType, maxActive int) ConfigOption {
	return configOptionFunc(func(o *configOptions) {
		if o.maxActiveByType == nil {
			o.maxActiveByType = make(map[int]int)
		}
		o.maxActiveByType[queueType] = maxActive
	})
}

// UpdateConfig applies one or more configuration changes atomically with
// respect to dispatch. After the changes are applied it drains any waiters
// newly made eligible (a raise to concurrency or a per-type cap can wake
// many waiters at once).
func (q *WeightedMultiQueue) UpdateConfig(opts ...ConfigOption) {
	var o configOptions
	for _, opt := range opts {
		opt.apply(&o)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if o.concurrencyLimit != nil {
		diff := *o.concurrencyLimit - q.mu.concurrencyLimit
		q.mu.remainingRuns = max(q.mu.remainingRuns+diff, 0)
		q.mu.concurrencyLimit = *o.concurrencyLimit
	}
	for queueType, maxActive := range o.maxActiveByType {
		if maxActive < 0 {
			panic(errors.AssertionFailedf("maxActive must be non-negative, got %d", maxActive))
		}
		idx, ok := q.typeIndex[queueType]
		if !ok {
			panic(errors.AssertionFailedf("unknown queue type %d", queueType))
		}
		q.mu.maxActive[idx] = maxActive
	}
	q.dispatchAllLocked()
}

// AvailableLen returns the number of tasks that can be added without
// queueing. 0 if any tasks are queued.
func (q *WeightedMultiQueue) AvailableLen() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.mu.remainingRuns
}

// QueueLen returns the length of the queue if one more task were added.
func (q *WeightedMultiQueue) QueueLen() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.queueLenLocked()
}

func (q *WeightedMultiQueue) queueLenLocked() int {
	if q.mu.remainingRuns > 0 {
		return 0
	}
	count := 1
	for _, h := range q.mu.waiters {
		count += len(h)
	}
	return count
}

// tryRunNextLocked dispatches at most one task, walking the precomputed
// cycle from cyclePosition for up to one full cycle.
func (q *WeightedMultiQueue) tryRunNextLocked() {
	if q.mu.remainingRuns <= 0 {
		return
	}
	n := len(q.cycle)
	for tries := 0; tries < n; tries++ {
		pos := (q.mu.cyclePosition + tries) % n
		idx := q.cycle[pos]
		if q.mu.waiters[idx].Len() == 0 {
			continue
		}
		if q.mu.maxActive[idx] > 0 && q.mu.activeByType[idx] >= q.mu.maxActive[idx] {
			continue
		}
		task := heap.Pop(&q.mu.waiters[idx]).(*Task[WeightedPermit])
		q.mu.activeByType[idx]++
		task.permitC <- &WeightedPermit{valid: true, typeIndex: int8(idx)}
		q.mu.remainingRuns--
		q.mu.cyclePosition = (pos + 1) % n
		return
	}
}

// dispatchAllLocked drains as many waiters as the current limits allow.
// Add and Release dispatch at most one task per call, which is sufficient
// when capacity grew by one slot. UpdateConfig can grow capacity by many
// slots at once and must wake every newly-eligible waiter.
func (q *WeightedMultiQueue) dispatchAllLocked() {
	for {
		before := q.mu.remainingRuns
		q.tryRunNextLocked()
		if q.mu.remainingRuns == before {
			return
		}
	}
}
