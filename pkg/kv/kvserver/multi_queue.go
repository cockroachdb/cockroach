// Copyright 2018 The Cockroach Authors.
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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// task represents a piece of work that needs to be done.
// When the task is done, it must be closed so that other tasks can run
type task struct {
	myChan   chan struct{}
	priority float64
}

// notifyHeap is a standard go heap over tasks.
type notifyHeap []*task

func (h notifyHeap) Len() int {
	return len(h)
}

func (h notifyHeap) Less(i, j int) bool {
	return h[j].priority < h[i].priority
}

func (h notifyHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *notifyHeap) Push(x interface{}) {
	*h = append(*h, x.(*task))
}

func (h *notifyHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// MultiQueue is the main structure that manages fairly round-robins through
// multiple queues of different priorities. A MultiQueue is constructed with a
// concurrencySem which is the number of concurrent jobs this queue will allow
// to run. Tasks are added to the queue using MultiQueue.Add. That will return a
// channel that should be received from. It will be notified when the waiting
// job is ready to be run. Once the job is completed, MultiQueue.TaskDone must
// be called to return the permit to the queue so that the next task can be
// started.
type MultiQueue struct {
	mu             syncutil.Mutex
	concurrencySem chan struct{}
	nameMapping    map[string]int
	lastQueueIndex int
	outstanding    []notifyHeap
	wakeUp         chan struct{}
	wg             sync.WaitGroup
}

// NewMultiQueue creates a new queue. The queue is not started, and start needs
// to be called on it first.
func NewMultiQueue(concurrencySem chan struct{}) *MultiQueue {
	queue := MultiQueue{
		concurrencySem: concurrencySem,
		nameMapping:    make(map[string]int),
		wakeUp:         make(chan struct{}),
	}
	queue.lastQueueIndex = -1
	return &queue
}

// Start begins the main loop of this MultiQueue which will continue until Stop
// is called. A MultiQueue.Start should not be started more than once, or after
// Stop has been called.
func (m *MultiQueue) Start() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		for {
			// Try to get a permit each time through the loop.
			m.concurrencySem <- struct{}{}

			// Hold the lock to make all the in-memory changes.
			m.mu.Lock()
			var activeChan chan struct{}
			// If there are no tasks on any queues, just go into the wait state until
			// we get the first add.
			if len(m.outstanding) > 0 {
				for i := 0; i < len(m.outstanding); i++ {
					// Start with the next available queue and iterate through empty
					// queues. If all queues are empty then wait for the next add.
					index := (m.lastQueueIndex + i + 1) % len(m.outstanding)
					if m.outstanding[index].Len() > 0 {
						task := heap.Pop(&m.outstanding[index]).(*task)
						activeChan = task.myChan
						m.lastQueueIndex = index
						break
					}
				}
			}
			m.mu.Unlock()

			if activeChan != nil {
				// Signal the receiver outside the mutex.
				activeChan <- struct{}{}
			} else {
				// Nothing to do, wait until we are woken up.
				// Since we didn't give anyone the permit, put it back.
				<-m.concurrencySem
				// Wait for someone else to add something. We go through one extra check
				// loop, but this guarantees there are no races, so we don't miss an Add
				// call
				if _, ok := <-m.wakeUp; !ok {
					break
				}
			}
		}
	}()
}

// Add returns a task that must be closed (calling task.Close) to
// release the permit. The number of names is expected to
// be relatively small and not be changing over time.
func (m *MultiQueue) Add(name string, priority float64) chan struct{} {
	t := task{
		priority: priority,
		myChan:   make(chan struct{}),
	}
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
	heap.Push(&m.outstanding[pos], &t)

	// Notify the MultiQueue that there may be work to do. If it wasn't waiting,
	// it doesn't matter.
	select {
	case m.wakeUp <- struct{}{}:
	default:
	}
	return t.myChan
}

//TaskDone needs to be called once the task that was running has completed and
//is no longer using system resources. This lets the MultiQueue call the next
//task.
func (m *MultiQueue) TaskDone() {
	// Take my permit back out.
	<-m.concurrencySem
}

// Stop initiates a shutdown by marking everything as done and then notifying
// that it is done. After we are able to send this, the main queue will shut
// down soon.
func (m *MultiQueue) Stop() {
	// Clear all outstanding jobs, the user should not call Add after stop is
	// called.
	m.mu.Lock()
	m.outstanding = nil
	m.mu.Unlock()

	// Close the wakeUp semaphore, so we can't wake up anymore.
	close(m.wakeUp)

	// Finally, wait until the main processing loop is done.
	m.wg.Wait()
}
