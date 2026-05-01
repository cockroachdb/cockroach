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

// WeightedPermit is the token returned by WeightedMultiQueue dispatches.
// typeIndex is the queue's internal index for the issuing type, used by
// Release to update per-type accounting without a map lookup.
type WeightedPermit struct {
	valid     bool
	typeIndex int8
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
// MaxActive caps may be updated at runtime; weights and the set of types are
// immutable after construction.
type WeightedMultiQueue struct {
	mu syncutil.Mutex

	concurrencyLimit int
	remainingRuns    int

	// typeIndex maps a user-facing QueueType to its internal index in the
	// per-type slices below.
	typeIndex map[int]int

	// Per-type state. All slices have length == number of declared types.
	weights      []int
	maxActive    []int // 0 means uncapped
	activeByType []int
	waiters      []notifyHeap[WeightedPermit]

	// cycle is the precomputed dispatch order. cycle[k] is the internal type
	// index whose turn it is at cycle position k. len(cycle) == sum(weights).
	cycle         []int
	cyclePosition int
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
		concurrencyLimit: maxConcurrency,
		remainingRuns:    maxConcurrency,
		typeIndex:        make(map[int]int, len(types)),
		weights:          make([]int, len(types)),
		maxActive:        make([]int, len(types)),
		activeByType:     make([]int, len(types)),
		waiters:          make([]notifyHeap[WeightedPermit], len(types)),
	}
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
		q.maxActive[i] = tc.MaxActive
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

	if _, ok := q.typeIndex[queueType]; !ok {
		return nil, errors.AssertionFailedf("unknown queue type %d", queueType)
	}

	// Reject before queueing if the pool is saturated and a length cap was
	// requested. Mirrors MultiQueue.Add.
	if q.remainingRuns == 0 && maxQueueLength >= 0 {
		if int64(q.queueLenLocked()) > maxQueueLength {
			return nil, errors.Newf("queue is too long")
		}
	}

	idx := q.typeIndex[queueType]
	task := &Task[WeightedPermit]{
		priority:  priority,
		permitC:   make(chan *WeightedPermit, 1),
		heapIdx:   -1,
		queueType: queueType,
	}
	heap.Push(&q.waiters[idx], task)
	q.tryRunNextLocked()
	return task, nil
}

// Cancel removes a task from its queue. If the dispatcher has already
// delivered a permit, Cancel races the caller's receive: whichever wins
// releases the permit. A no-op if the caller has already received it.
func (q *WeightedMultiQueue) Cancel(task *Task[WeightedPermit]) {
	q.mu.Lock()
	defer q.mu.Unlock()
	idx := q.typeIndex[task.queueType]
	if q.waiters[idx].tryRemove(task) {
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
		panic("double release of permit")
	}
	p.valid = false
	if q.remainingRuns < q.concurrencyLimit {
		q.remainingRuns++
	}
	q.activeByType[p.typeIndex]--
	q.tryRunNextLocked()
}

// UpdateConcurrencyLimit changes the total concurrency cap. Already-active
// tasks are unaffected; queued tasks may dispatch immediately on a relax.
func (q *WeightedMultiQueue) UpdateConcurrencyLimit(newLimit int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	diff := newLimit - q.concurrencyLimit
	q.remainingRuns = max(q.remainingRuns+diff, 0)
	q.concurrencyLimit = newLimit
	q.tryRunNextLocked()
}

// UpdateTypeMaxActive changes the per-type cap. Pass 0 to remove. Panics on
// unknown queueType.
func (q *WeightedMultiQueue) UpdateTypeMaxActive(queueType, maxActive int) {
	if maxActive < 0 {
		panic(errors.AssertionFailedf("maxActive must be non-negative, got %d", maxActive))
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	idx, ok := q.typeIndex[queueType]
	if !ok {
		panic(errors.AssertionFailedf("unknown queue type %d", queueType))
	}
	q.maxActive[idx] = maxActive
	q.tryRunNextLocked()
}

// AvailableLen returns the number of tasks that can be added without
// queueing. 0 if any tasks are queued.
func (q *WeightedMultiQueue) AvailableLen() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.remainingRuns
}

// QueueLen returns the length of the queue if one more task were added.
func (q *WeightedMultiQueue) QueueLen() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.queueLenLocked()
}

func (q *WeightedMultiQueue) queueLenLocked() int {
	if q.remainingRuns > 0 {
		return 0
	}
	count := 1
	for _, h := range q.waiters {
		count += len(h)
	}
	return count
}

// tryRunNextLocked dispatches at most one task, walking the precomputed
// cycle from cyclePosition for up to one full cycle.
func (q *WeightedMultiQueue) tryRunNextLocked() {
	if q.remainingRuns <= 0 {
		return
	}
	n := len(q.cycle)
	for tries := 0; tries < n; tries++ {
		pos := (q.cyclePosition + tries) % n
		idx := q.cycle[pos]
		if q.waiters[idx].Len() == 0 {
			continue
		}
		if q.maxActive[idx] > 0 && q.activeByType[idx] >= q.maxActive[idx] {
			continue
		}
		task := heap.Pop(&q.waiters[idx]).(*Task[WeightedPermit])
		q.activeByType[idx]++
		task.permitC <- &WeightedPermit{valid: true, typeIndex: int8(idx)}
		q.remainingRuns--
		q.cyclePosition = (pos + 1) % n
		return
	}
}
