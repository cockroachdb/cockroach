// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package multiqueue

import (
	"container/heap"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/redact"
)

// Task represents a request for a Permit for a piece of work that needs to be
// done. It is created by a call to MultiQueue.Add. After creation,
// Task.GetWaitChan is called to get a permit, and after all work related to
// this task is done, MultiQueue.Release must be called so future tasks can run.
// Alternatively, if the user decides they no longer want to run their work,
// MultiQueue.Cancel can be called to release the permit without waiting for the
// permit.
type Task struct {
	priority  float64
	queueType int
	heapIdx   int
	permitC   chan *Permit
}

// GetWaitChan returns a permit channel which is used to wait for the permit to
// become available.
func (t *Task) GetWaitChan() <-chan *Permit {
	return t.permitC
}

func (t *Task) String() string {
	return redact.Sprintf("{Queue type : %d, Priority :%f}", t.queueType, t.priority).StripMarkers()
}

// notifyHeap is a standard go heap over tasks.
type notifyHeap []*Task

func (h notifyHeap) Len() int {
	return len(h)
}

func (h notifyHeap) Less(i, j int) bool {
	return h[j].priority < h[i].priority
}

func (h notifyHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIdx = i
	h[j].heapIdx = j
}

func (h *notifyHeap) Push(x interface{}) {
	t := x.(*Task)
	// Set the index to the end, it will be moved later
	t.heapIdx = h.Len()
	*h = append(*h, t)
}

func (h *notifyHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[0 : n-1]
	// No longer in the heap so clear the index
	x.heapIdx = -1

	return x
}

// tryRemove attempts to remove the task from this queue by iterating through
// the queue. Will returns true if the task was successfully removed.
func (h *notifyHeap) tryRemove(task *Task) bool {
	if task.heapIdx < 0 {
		return false
	}
	heap.Remove(h, task.heapIdx)
	return true
}

// MultiQueue is a type that round-robins through a set of typed queues, each
// independently prioritized. A MultiQueue is constructed with a concurrencySem
// which is the number of concurrent jobs this queue will allow to run. Tasks
// are added to the queue using MultiQueue.Add. That will return a channel that
// should be received from. It will be notified when the waiting job is ready to
// be run. Once the job is completed, MultiQueue.TaskDone must be called to
// return the Permit to the queue so that the next Task can be started.
type MultiQueue struct {
	mu             syncutil.Mutex
	remainingRuns  int
	mapping        map[int]int
	lastQueueIndex int
	outstanding    []notifyHeap
}

// NewMultiQueue creates a new queue. The queue is not started, and start needs
// to be called on it first.
func NewMultiQueue(maxConcurrency int) *MultiQueue {
	queue := MultiQueue{
		remainingRuns: maxConcurrency,
		mapping:       make(map[int]int),
	}
	queue.lastQueueIndex = -1

	return &queue
}

// Permit is a token which is returned from a Task.GetWaitChan call.
type Permit struct {
	valid bool
}

// tryRunNextLocked will run the next task in order round-robin through the
// queues and in priority order within a queue. It will return true if it ran a
// task. The MultiQueue.mu lock must be held before calling this func.
func (m *MultiQueue) tryRunNextLocked() {
	// If no permits are left, then we can't run anything.
	if m.remainingRuns <= 0 {
		return
	}

	for i := 0; i < len(m.outstanding); i++ {
		// Start with the next queue in order and iterate through all empty queues.
		// If all queues are empty then return false signaling that nothing was run.
		index := (m.lastQueueIndex + i + 1) % len(m.outstanding)
		if m.outstanding[index].Len() > 0 {
			task := heap.Pop(&m.outstanding[index]).(*Task)
			task.permitC <- &Permit{valid: true}
			m.remainingRuns--
			m.lastQueueIndex = index
			return
		}
	}
}

// Add returns a Task that must be closed (calling Task.Close) to
// release the Permit. The number of types is expected to
// be relatively small and not be changing over time.
func (m *MultiQueue) Add(queueType int, priority float64) *Task {
	m.mu.Lock()
	defer m.mu.Unlock()

	// The mutex starts locked, unlock it when we are ready to run.
	pos, ok := m.mapping[queueType]
	if !ok {
		// Append a new entry to both mapping and outstanding each time there is
		// a new queue type.
		pos = len(m.outstanding)
		m.mapping[queueType] = pos
		m.outstanding = append(m.outstanding, notifyHeap{})
	}
	newTask := Task{
		priority:  priority,
		permitC:   make(chan *Permit, 1),
		heapIdx:   -1,
		queueType: queueType,
	}
	heap.Push(&m.outstanding[pos], &newTask)

	// Once we are done adding a task, signal the main loop in case it finished
	// all its work and was waiting for more work. We are holding the mu lock when
	// signaling, so we guarantee that it will not be able to respond to the
	// signal until after we release the lock.
	m.tryRunNextLocked()

	return &newTask
}

// Cancel will cancel a Task that may not have started yet. This is useful if it
// is determined that it is no longer required to run this Task.
func (m *MultiQueue) Cancel(task *Task) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Find the right queue and try to remove it. Queues monotonically grow, and a
	// Task will track its position within the queue.
	queueIdx := m.mapping[task.queueType]
	ok := m.outstanding[queueIdx].tryRemove(task)
	// If we get here, we are racing with the task being started. The concern is
	// that the caller may also call MultiQueue.Release since the task was
	// started. Either we get the permit or the caller, so we guarantee only one
	// release will be called.
	if !ok {
		select {
		case p, ok := <-task.permitC:
			// Only release if the channel is open, and we can get the permit.
			if ok {
				m.releaseLocked(p)
			}
		default:
			// If we are not able to get the permit, this means the permit has already
			// been given to the caller, and they must call Release on it.
		}
	}
}

// Release needs to be called once the Task that was running has completed and
// is no longer using system resources. This allows the MultiQueue to call the
// next Task.
func (m *MultiQueue) Release(permit *Permit) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseLocked(permit)
}

func (m *MultiQueue) releaseLocked(permit *Permit) {
	if !permit.valid {
		panic("double release of permit")
	}
	permit.valid = false
	m.remainingRuns++
	m.tryRunNextLocked()
}

// Len returns the number of additional tasks that can be added without
// queueing. This will return 0 if there is anything queued. This method should
// only be used for testing.
func (m *MultiQueue) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.remainingRuns
}
