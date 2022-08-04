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
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/redact"
)

// Task represents a request for a permit for a piece of work that needs to be
// done. It is created by a call to MultiQueue.Add. After creation,
// Task.GetWaitChan is called to get a permit, and after all work related to
// this task is done, MultiQueue.Release must be called so future tasks can run.
// Alternatively, if the user decides they no longer want to run their work,
// MultiQueue.Cancel can be called to release the permit without waiting for the
// permit.
type Task struct {
	permitC   chan struct{}
	priority  float64
	queueName string
	heapIdx   int
	doneC     chan struct{}
}

// GetWaitChan returns a permit channel which is used to wait for the permit to
// become available.
func (t *Task) GetWaitChan() chan struct{} {
	return t.permitC
}

func (t *Task) String() string {
	return redact.Sprintf("{Queue name : %s, Priority :%f}", t.queueName, t.priority).StripMarkers()
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

// MultiQueue is a type that round-robins through a set of named queues, each
// independently prioritized. A MultiQueue is constructed with a concurrencySem
// which is the number of concurrent jobs this queue will allow to run. Tasks
// are added to the queue using MultiQueue.Add. That will return a channel that
// should be received from. It will be notified when the waiting job is ready to
// be run. Once the job is completed, MultiQueue.TaskDone must be called to
// return the permit to the queue so that the next Task can be started.
type MultiQueue struct {
	name           string
	concurrencySem chan struct{}

	mu struct {
		syncutil.Mutex
		wakeUp         sync.Cond
		stopped        bool
		nameMapping    map[string]int
		lastQueueIndex int
		outstanding    []notifyHeap
	}
}

// NewMultiQueue creates a new queue. The queue is not started, and start needs
// to be called on it first.
func NewMultiQueue(name string, maxConcurrency int) *MultiQueue {
	queue := MultiQueue{
		concurrencySem: make(chan struct{}, maxConcurrency),
		name:           name,
	}
	queue.mu.wakeUp.L = &queue.mu.Mutex
	queue.mu.nameMapping = make(map[string]int)
	queue.mu.lastQueueIndex = -1

	return &queue
}

// Start begins the main loop of this MultiQueue which will continue until Stop
// is called. A MultiQueue.Start should not be started more than once, or after
// Stop has been called.
func (m *MultiQueue) Start(startCtx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(startCtx, m.name+"-multi-queue-quiesce", func(ctx context.Context) {
		// Wait for quiesce. Once we are quiescing, we need to signal our
		// processing routine to wake up so it may finish.
		defer func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.mu.stopped = true
			m.mu.wakeUp.Signal()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stopper.ShouldQuiesce():
				return
			}
		}
	})
	_ = stopper.RunAsyncTask(startCtx, m.name+"-multi-queue", func(ctx context.Context) {
		defer close(m.concurrencySem)
		for {
			select {
			case m.concurrencySem <- struct{}{}:
				// Got a slot to run a task.
				m.processNextOrWait(ctx, stopper)
			case <-ctx.Done():
				return
			case <-stopper.ShouldQuiesce():
				return
			}
		}
	})
}

// processNext TODO(sarkesian): write comment
func (m *MultiQueue) processNextOrWait(ctx context.Context, stopper *stop.Stopper) {
	if task := m.getNextTask(); task != nil {
		<-task.permitC
		go func() {
			// When the task has completed, it will attempt to read off of its permitC.
			// Upon this happening, we should remove our task's processing token from
			// the concurrencySem.
			select {
			case task.permitC <- struct{}{}:
				close(task.permitC)
				<-m.concurrencySem
			case <-task.doneC:
				close(task.permitC)
				<-m.concurrencySem
			case <-ctx.Done():
				return
			case <-stopper.ShouldQuiesce():
				return
			}
		}()
	} else {
		m.mu.Lock()
		// If we don't have a task to run, wait until we do. When we wake up,
		// remove this iteration's processing token from the concurrencySem.
		if !m.mu.stopped {
			m.mu.wakeUp.Wait()
			<-m.concurrencySem
		}
		m.mu.Unlock()
	}
}

// TODO(sarkesian): add comment
func (m *MultiQueue) getNextTask() *Task {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.getNextTaskLocked()
}

// getNextTaskLocked will round-robin through the queues and get the next task
// to run in priority order within a queue. It will return a *Task if one is
// available, otherwise it will return nil.
// MultiQueue.mu must be held by the caller of this function.
func (m *MultiQueue) getNextTaskLocked() *Task {
	for i := range m.mu.outstanding {
		// Start with the next queue in order and iterate through all empty queues.
		// If all queues are empty then return false signaling that nothing was run.
		index := (m.mu.lastQueueIndex + i + 1) % len(m.mu.outstanding)
		if m.mu.outstanding[index].Len() > 0 {
			task := heap.Pop(&m.mu.outstanding[index]).(*Task)
			m.mu.lastQueueIndex = index
			return task
		}
	}
	return nil
}

// Add returns a Task that must be closed (calling Task.Close) to
// release the Permit. The number of names is expected to
// be relatively small and not be changing over time.
func (m *MultiQueue) Add(name string, priority float64) *Task {
	m.mu.Lock()
	defer m.mu.Unlock()

	// The mutex starts locked, unlock it when we are ready to run.
	pos, ok := m.mu.nameMapping[name]
	if !ok {
		// Append a new entry to both nameMapping and outstanding each time there is
		// a new queue name.
		pos = len(m.mu.outstanding)
		m.mu.nameMapping[name] = pos
		m.mu.outstanding = append(m.mu.outstanding, notifyHeap{})
	}
	newTask := Task{
		priority:  priority,
		permitC:   make(chan struct{}),
		heapIdx:   -1,
		queueName: name,
		doneC:     make(chan struct{}),
	}
	heap.Push(&m.mu.outstanding[pos], &newTask)

	// Once we are done adding a task, signal the main loop in case it finished
	// all its work and was waiting for more work. We are holding the mu lock when
	// signaling, so we guarantee that it will not be able to respond to the
	// signal until after we release the lock.
	m.mu.wakeUp.Signal()

	return &newTask
}

// Cancel will cancel a Task that may not have started yet. This is useful if it
// is determined that it is no longer required to run this Task.
func (m *MultiQueue) Cancel(task *Task) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Find the right queue and try to remove it. Queues monotonically grow, and a
	// Task will track its position within the queue.
	queueIdx := m.mu.nameMapping[task.queueName]
	ok := m.mu.outstanding[queueIdx].tryRemove(task)
	// If we are not able to remove it from the queue, then it is either running
	// or completed.
	if !ok {
		close(task.doneC)
	}
}
