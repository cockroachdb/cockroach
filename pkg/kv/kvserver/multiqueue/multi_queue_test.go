// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiqueue

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestMultiQueueEmpty makes sure that an empty queue can be created, started
// and stopped.
func TestMultiQueueEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	NewMultiQueue(1)
}

// TestMultiQueueAddTwiceSameQueue makes sure that for a single queue the
// priority is respected.
func TestMultiQueueAddTwiceSameQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	queue := NewMultiQueue(1)
	blocker, _ := queue.Add(0, 0, -1)

	chan1, _ := queue.Add(7, 1.0, -1)
	chan2, _ := queue.Add(7, 2.0, -1)

	permit := <-blocker.GetWaitChan()
	queue.Release(permit)

	// Verify chan2 is higher priority so runs first.
	verifyOrder(t, queue, chan2, chan1)
}

// TestMultiQueueTwoQueues checks that if requests are added to two queue names,
// they are called in a round-robin order. It also verifies that the priority is
// respected for each.
func TestMultiQueueTwoQueues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	queue := NewMultiQueue(1)
	blocker, _ := queue.Add(0, 0, -1)

	a1, _ := queue.Add(5, 4.0, -1)
	a2, _ := queue.Add(5, 5.0, -1)

	b1, _ := queue.Add(6, 1.0, -1)
	b2, _ := queue.Add(6, 2.0, -1)

	permit := <-blocker.GetWaitChan()
	queue.Release(permit)
	// The queue starts with the "second" item added.
	verifyOrder(t, queue, a2, b2, a1, b1)
}

// TestMultiQueueComplex verifies that with multiple queues, some added before
// and some after we start running, that the final order is still as expected.
// The expectation is that it round robins through the queues (a, b, c, ...) and
// runs higher priority tasks before lower priority within a queue.
func TestMultiQueueComplex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	queue := NewMultiQueue(1)
	blocker, _ := queue.Add(0, 0, -1)

	a2, _ := queue.Add(1, 4.0, -1)
	b1, _ := queue.Add(2, 1.1, -1)
	b2, _ := queue.Add(2, 2.1, -1)
	c2, _ := queue.Add(3, 1.2, -1)
	c3, _ := queue.Add(3, 2.2, -1)
	a3, _ := queue.Add(1, 5.0, -1)
	b3, _ := queue.Add(2, 6.1, -1)

	permit := <-blocker.GetWaitChan()
	queue.Release(permit)
	verifyOrder(t, queue, a3, b3, c3, a2, b2, c2, b1)
}

func TestMultiQueueRemove(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	queue := NewMultiQueue(1)
	blocker, _ := queue.Add(0, 0, -1)

	a2, _ := queue.Add(1, 4.0, -1)
	b1, _ := queue.Add(2, 1.1, -1)
	b2, _ := queue.Add(2, 2.1, -1)
	c2, _ := queue.Add(3, 1.2, -1)
	c3, _ := queue.Add(3, 2.2, -1)
	a3, _ := queue.Add(1, 5.0, -1)
	b3, _ := queue.Add(2, 6.1, -1)

	fmt.Println("Beginning cancel")

	queue.Cancel(b2)
	queue.Cancel(b1)

	fmt.Println("Finished cancel")

	permit := <-blocker.GetWaitChan()
	queue.Release(permit)
	verifyOrder(t, queue, a3, b3, c3, a2, c2)
}

func TestMultiQueueCancelOne(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	queue := NewMultiQueue(1)
	task, _ := queue.Add(1, 1, -1)
	queue.Cancel(task)
}

func TestMultiQueueCancelInProgress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	queue := NewMultiQueue(1)

	const a = 1
	const b = 2
	const c = 3

	a3, _ := queue.Add(a, 5.0, -1)
	a2, _ := queue.Add(a, 4.0, -1)
	b1, _ := queue.Add(b, 1.1, -1)
	b2, _ := queue.Add(b, 2.1, -1)
	c3, _ := queue.Add(c, 2.2, -1)
	b3, _ := queue.Add(b, 6.1, -1)

	queue.Cancel(b2)
	queue.Cancel(b1)

	started := 0
	completed := 0
	startTask := func(task *Task) (*Permit, bool) {
		select {
		case permit, ok := <-task.GetWaitChan():
			if ok {
				started++
				return permit, true
			}
		case <-time.After(time.Second):
			t.Fatalf(`should not wait for task on queue %d with priority %f to start`,
				task.queueType, task.priority,
			)
		}
		return nil, false
	}

	completeTask := func(task *Task, permit *Permit) {
		releaseStarted := make(chan struct{})
		releaseFinished := make(chan struct{})
		go func() {
			close(releaseStarted)
			queue.Release(permit)
			close(releaseFinished)
		}()
		<-releaseStarted
		select {
		case <-releaseFinished:
			completed++
		case <-time.After(time.Second):
			t.Fatalf(`should not wait for task on queue %d with priority %f to complete`,
				task.queueType, task.priority,
			)
		}
	}

	// Execute a3.
	a3Permit, ok := startTask(a3)
	require.True(t, ok)
	completeTask(a3, a3Permit)

	// Cancel b3 before starting. Should not be able to get permit.
	queue.Cancel(b3)
	_, ok = startTask(b3)
	require.False(t, ok)

	// Now, should be able to execute c3 immediately.
	c3Permit, ok := startTask(c3)
	require.True(t, ok)

	// A and C started
	require.Equal(t, 2, started)
	// A completed
	require.Equal(t, 1, completed)

	// Complete c3 and cancel after completion.
	completeTask(c3, c3Permit)
	queue.Cancel(c3)

	// Start a2, which is the final item and also should not block to start.
	startTask(a2)

	require.Equal(t, 3, started)
	require.Equal(t, 2, completed)
}

// TestMultiQueueStress calls Add from multiple threads. It chooses different
// names and different priorities for the requests. The goal is simply to make
// sure that all the requests are serviced and nothing hangs or fails.
func TestMultiQueueStress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "cancel", func(t *testing.T, alsoCancel bool) {
		testutils.RunTrueAndFalse(t, "sleep", func(t *testing.T, alsoSleep bool) {

			queue := NewMultiQueue(5)

			numThreads := 10
			numRequests := 500
			var wg sync.WaitGroup
			wg.Add(numThreads)
			var ops int64
			var timeCancels int64

			for i := 0; i < numThreads; i++ {
				go func(name int) {
					for j := 0; j < numRequests; j++ {
						curTask, _ := queue.Add(name, float64(j), -1)
						if alsoCancel && j%99 == 0 {
							queue.Cancel(curTask)
						} else {
							select {
							case <-time.After(400 * time.Microsecond):
								queue.Cancel(curTask)
								atomic.AddInt64(&timeCancels, 1)
							case p := <-curTask.GetWaitChan():
								if alsoSleep && j%10 == 0 {
									// Sleep on 10% of requests to simulate doing work.
									time.Sleep(200 * time.Microsecond)
								}
								queue.Release(p)
							}
						}
						atomic.AddInt64(&ops, 1)
					}
					wg.Done()
				}(i % 4)
			}
			wg.Wait()
			fmt.Printf("Num time cancels %d / %d\n", timeCancels, ops)
			require.Equal(t, int64(numThreads*numRequests), ops)
		})
	})
}

func TestMultiQueueReleaseTwice(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	queue := NewMultiQueue(1)

	task, _ := queue.Add(1, 1, -1)
	p := <-task.GetWaitChan()
	queue.Release(p)
	require.Panics(t, func() { queue.Release(p) })
}

func TestMultiQueueReleaseAfterCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	queue := NewMultiQueue(1)

	task, _ := queue.Add(1, 1, -1)
	p := <-task.GetWaitChan()
	queue.Cancel(task)
	queue.Release(p)
}

func TestMultiQueueLen(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	queue := NewMultiQueue(2)
	require.Equal(t, 2, queue.AvailableLen())
	require.Equal(t, 0, queue.QueueLen())

	task1, _ := queue.Add(1, 1, -1)
	require.Equal(t, 1, queue.AvailableLen())
	require.Equal(t, 0, queue.QueueLen())
	task2, _ := queue.Add(1, 1, -1)
	require.Equal(t, 0, queue.AvailableLen())
	require.Equal(t, 1, queue.QueueLen())
	task3, _ := queue.Add(1, 1, -1)
	require.Equal(t, 0, queue.AvailableLen())
	require.Equal(t, 2, queue.QueueLen())

	queue.Cancel(task1)
	// Finish task 1, but immediately start task3.
	require.Equal(t, 0, queue.AvailableLen())
	require.Equal(t, 1, queue.QueueLen())

	p := <-task2.GetWaitChan()
	queue.Release(p)
	require.Equal(t, 1, queue.AvailableLen())
	require.Equal(t, 0, queue.QueueLen())

	queue.Cancel(task3)
	require.Equal(t, 2, queue.AvailableLen())
	require.Equal(t, 0, queue.QueueLen())
}

func TestMultiQueueFull(t *testing.T) {
	queue := NewMultiQueue(2)
	require.Equal(t, 2, queue.AvailableLen())
	require.Equal(t, 0, queue.QueueLen())

	// Task 1 starts immediately since there is no queue.
	task1, err := queue.Add(1, 1, 0)
	require.NoError(t, err)
	require.Equal(t, 1, queue.AvailableLen())
	require.Equal(t, 0, queue.QueueLen())
	// Task 2 also starts immediately as the queue supports 2 concurrent.
	task2, err := queue.Add(1, 1, 0)
	require.NoError(t, err)
	require.Equal(t, 0, queue.AvailableLen())
	require.Equal(t, 1, queue.QueueLen())
	// Task 3 would be queued so should not be added.
	task3, err := queue.Add(1, 1, 0)
	require.Error(t, err)
	require.Nil(t, task3)
	require.Equal(t, 0, queue.AvailableLen())
	require.Equal(t, 1, queue.QueueLen())
	// Task 4 uses a longer max queue length so should be added.
	task4, err := queue.Add(1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, 0, queue.AvailableLen())
	require.Equal(t, 2, queue.QueueLen())

	queue.Cancel(task1)
	queue.Cancel(task2)
	require.Equal(t, 1, queue.AvailableLen())
	require.Equal(t, 0, queue.QueueLen())
	// After these tasks are done, make sure we can add another one.
	task5, err := queue.Add(1, 1, 0)
	require.NoError(t, err)
	require.Equal(t, 0, queue.AvailableLen())
	require.Equal(t, 1, queue.QueueLen())

	// Cancel all the remaining tasks.
	queue.Cancel(task4)
	queue.Cancel(task5)
}

// verifyOrder makes sure that the chans are called in the specified order.
func verifyOrder(t *testing.T, queue *MultiQueue, tasks ...*Task) {
	// each time, verify that the only available channel is the "next" one in order
	for i, task := range tasks {
		if task == nil {
			require.Fail(t, "Task is nil", "%d", task)
		}

		var found *Permit
		for j, t2 := range tasks[i+1:] {
			select {
			case <-t2.GetWaitChan():
				require.Fail(t, "Queue active when should not be ", "iter %d, chan %d, task %v", i, i+1+j, t2)
			default:
			}
		}
		select {
		case p := <-task.GetWaitChan():
			found = p
		default:
			require.Fail(t, "Queue not active when should be ", "Queue %d : task %v", i, task)
		}
		queue.Release(found)
	}
}

// TestMultiQueueUpdateConcurrencLimit tests the correctness of remaining
// runs after concurrency limit update.
func TestMultiQueueUpdateConcurrencLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	queue := NewMultiQueue(1)
	// Grow the concurrency limit.
	queue.UpdateConcurrencyLimit(5)
	require.Equal(t, 5, queue.remainingRuns)
	// Shrink the concurrency limit.
	queue.UpdateConcurrencyLimit(2)
	require.Equal(t, 2, queue.remainingRuns)
	// Decrease the remaining runs.
	_, _ = queue.Add(1, 1, -1)
	_, _ = queue.Add(2, 1, -1)
	// Shrink the limit gain, make sure the remainingRuns is non-negative.
	queue.UpdateConcurrencyLimit(1)
	require.Equal(t, 0, queue.remainingRuns)
	// Test the edge case of increasing concurrency limits from 0.
	// Tasks should be able to execute after limit adjustments.
	queue = NewMultiQueue(0)
	a1, _ := queue.Add(1, 4.0, -1)
	a2, _ := queue.Add(1, 3.0, -1)
	queue.UpdateConcurrencyLimit(1)
	verifyOrder(t, queue, a1, a2)
	// Test the edge case of decreasing concurrency limit and cancel
	// task will not increase remaining runs beyond the limit.
	queue = NewMultiQueue(2)
	a1, _ = queue.Add(1, 4.0, -1)
	require.Equal(t, 1, queue.remainingRuns)
	queue.updateConcurrencyLimitLocked(1)
	permit := <-a1.GetWaitChan()
	queue.Cancel(a1)
	queue.Release(permit)
	require.Equal(t, 1, queue.remainingRuns)
	// Test the case of updating concurrency limit to 0
	// to freeze the queue.
	queue = NewMultiQueue(2)
	queue.UpdateConcurrencyLimit(0)
	require.Equal(t, 1, queue.QueueLen())
	require.Equal(t, 0, queue.remainingRuns)
	// Queue is frozen adding a new task will not
	// trigger execution due to no remaining runs left.
	_, err := queue.Add(1, 1, 1)
	require.Equal(t, 2, queue.QueueLen())
	require.NoError(t, err)
	require.Equal(t, 0, queue.AvailableLen())
}
