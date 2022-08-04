// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"container/heap"
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Task represents a piece of work that needs to be done.
// When the task is done, it must be closed so that other tasks can run
type Task struct {
	permitC  chan Permit
	priority float64
	queueIdx int
	idx      int
}

// Await returns a permit channel which is used with to wait for the permit to
// become available.
func (t *Task) Await() chan Permit {
	return t.permitC
}

func (t *Task) String() string {
	return fmt.Sprintf("{Queue id : %d, Priority :%f}", t.queueIdx, t.priority)
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
	h[i].idx = i
	h[j].idx = j
}

func (h *notifyHeap) Push(x interface{}) {
	t := x.(*Task)
	// Set the index to the end, it will be moved later
	t.idx = h.Len()
	*h = append(*h, t)
}

func (h *notifyHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	// No longer in the heap so clear the index
	x.idx = -1

	return x
}

// tryRemove attempts to remove the task from this queue by iterating through
// the queue. Will returns true if the task was successfully removed.
func (h *notifyHeap) tryRemove(task *Task) bool {
	if task.idx < 0 {
		return false
	}
	heap.Remove(h, task.idx)
	return true
}

// MultiQueue is a type that round-robins through a set of named queues, each
// independently prioritized. A MultiQueue is constructed with a concurrencySem
// which is the number of concurrent jobs this queue will allow to run. Tasks
// are added to the queue using MultiQueue.Add. That will return a channel that
// should be received from. It will be notified when the waiting job is ready to
// be run. Once the job is completed, MultiQueue.TaskDone must be called to
// return the Permit to the queue so that the next Task can be started.
type MultiQueue struct {
	mu             syncutil.Mutex
	concurrencySem chan Permit
	nameMapping    map[string]int
	lastQueueIndex int
	outstanding    []notifyHeap
	wakeUp         *sync.Cond
	stopping       bool
}

// NewMultiQueue creates a new queue. The queue is not started, and start needs
// to be called on it first.
func NewMultiQueue(maxConcurrency int) *MultiQueue {
	queue := MultiQueue{
		concurrencySem: make(chan Permit, maxConcurrency),
		nameMapping:    make(map[string]int),
	}
	// Fill all the permits in the queue.
	for i := 0; i < maxConcurrency; i++ {
		queue.concurrencySem <- Permit{}
	}

	queue.wakeUp = sync.NewCond(&queue.mu)
	queue.lastQueueIndex = -1
	return &queue
}

// Permit is a token which is returned from a Task.Await call.
type Permit struct{}

// Start begins the main loop of this MultiQueue which will continue until Stop
// is called. A MultiQueue.Start should not be started more than once, or after
// Stop has been called.
func (m *MultiQueue) Start(startCtx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(startCtx, "snapshot-multi-queue-quiesce", func(ctx context.Context) {
		// Wait for the quiesce signal, then wake up the main thread to allow it to complete.
		<-stopper.ShouldQuiesce()
		m.mu.Lock()
		m.stopping = true
		m.wakeUp.Signal()
		m.mu.Unlock()
	})
	_ = stopper.RunAsyncTask(startCtx, "snapshot-multi-queue", func(ctx context.Context) {
		// Run until context is stopped.
		for {
			select {
			case <-stopper.ShouldQuiesce():
				return

			// Try to get one Permit each time through the loop to assign to the Task
			// that is run.
			case gotPermit := <-m.concurrencySem:
				// Hold the lock to make all the in-memory changes.
				m.mu.Lock()
				for !m.stopping {
					// If we can run a Task, then break out of this loop.
					if m.tryRunNext(gotPermit) {
						break
					}
					// If there are no tasks on any queues, wait until one gets added.
					m.wakeUp.Wait()
				}
				m.mu.Unlock()
			}
		}
	})
}

// runNext will run the next task in order based on
func (m *MultiQueue) tryRunNext(permit Permit) bool {
	for i := 0; i < len(m.outstanding); i++ {
		// Start with the next available queue and iterate through empty
		// queues. If all queues are empty then wait for the next add.
		index := (m.lastQueueIndex + i + 1) % len(m.outstanding)
		if m.outstanding[index].Len() > 0 {
			task := heap.Pop(&m.outstanding[index]).(*Task)
			task.permitC <- permit
			m.lastQueueIndex = index
			return true
		}
	}
	return false
}

// Add returns a Task that must be closed (calling Task.Close) to
// release the Permit. The number of names is expected to
// be relatively small and not be changing over time.
func (m *MultiQueue) Add(name string, priority float64) *Task {
	m.mu.Lock()
	defer m.mu.Unlock()

	// the mutex starts locked, unlock when we are ready to run
	pos, ok := m.nameMapping[name]
	if !ok {
		// Append a new entry to both nameMapping and outstanding
		pos = len(m.outstanding)
		m.nameMapping[name] = pos
		m.outstanding = append(m.outstanding, notifyHeap{})
	}
	newTask := Task{
		priority: priority,
		permitC:  make(chan Permit),
		idx:      -1,
		queueIdx: pos,
	}
	heap.Push(&m.outstanding[pos], &newTask)

	// Once we are done adding, signal the main loop in case it finished all its
	// work and was waiting for more work. We are holding the mu lock when
	// signaling, so guarantee that it will not be able to respond to the signal
	// until after we release the lock.
	m.wakeUp.Signal()

	return &newTask
}

// Cancel will cancel a Task that may not have started yet. This is useful if it
// is determined that it is no longer required to run this Task.:w
func (m *MultiQueue) Cancel(task *Task) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Find the right queue and try to remove it. Queues monotonically grow, and a
	// Task will track its position within the queue.
	ok := m.outstanding[task.queueIdx].tryRemove(task)
	if !ok {
		// Assume has already been popped,
		m.Release(<-task.permitC)
	}
}

//Release needs to be called once the Task that was running has completed and
//is no longer using system resources. This allows the MultiQueue to call the
//next Task.
func (m *MultiQueue) Release(permit Permit) {
	m.concurrencySem <- permit
}
