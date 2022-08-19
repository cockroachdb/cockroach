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
	blocker := queue.Add(0, 0)

	chan1 := queue.Add(7, 1.0)
	chan2 := queue.Add(7, 2.0)

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
	blocker := queue.Add(0, 0)

	a1 := queue.Add(5, 4.0)
	a2 := queue.Add(5, 5.0)

	b1 := queue.Add(6, 1.0)
	b2 := queue.Add(6, 2.0)

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
	blocker := queue.Add(0, 0)

	a2 := queue.Add(1, 4.0)
	b1 := queue.Add(2, 1.1)
	b2 := queue.Add(2, 2.1)
	c2 := queue.Add(3, 1.2)
	c3 := queue.Add(3, 2.2)
	a3 := queue.Add(1, 5.0)
	b3 := queue.Add(2, 6.1)

	permit := <-blocker.GetWaitChan()
	queue.Release(permit)
	verifyOrder(t, queue, a3, b3, c3, a2, b2, c2, b1)
}

func TestMultiQueueRemove(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	queue := NewMultiQueue(1)
	blocker := queue.Add(0, 0)

	a2 := queue.Add(1, 4.0)
	b1 := queue.Add(2, 1.1)
	b2 := queue.Add(2, 2.1)
	c2 := queue.Add(3, 1.2)
	c3 := queue.Add(3, 2.2)
	a3 := queue.Add(1, 5.0)
	b3 := queue.Add(2, 6.1)

	fmt.Println("Beginning cancel")

	queue.Cancel(b2)
	queue.Cancel(b1)

	fmt.Println("Finished cancel")

	permit := <-blocker.GetWaitChan()
	queue.Release(permit)
	verifyOrder(t, queue, a3, b3, c3, a2, c2)
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
						curTask := queue.Add(name, float64(j))
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

	task := queue.Add(1, 1)
	p := <-task.GetWaitChan()
	queue.Release(p)
	require.Panics(t, func() { queue.Release(p) })
}

func TestMultiQueueReleaseAfterCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	queue := NewMultiQueue(1)

	task := queue.Add(1, 1)
	p := <-task.GetWaitChan()
	queue.Cancel(task)
	queue.Release(p)
}

func TestMultiQueueLen(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	queue := NewMultiQueue(2)
	require.Equal(t, 2, queue.Len())

	task1 := queue.Add(1, 1)
	require.Equal(t, 1, queue.Len())
	task2 := queue.Add(1, 1)
	require.Equal(t, 0, queue.Len())
	task3 := queue.Add(1, 1)
	require.Equal(t, 0, queue.Len())

	queue.Cancel(task1)
	// Finish task 1, but immediately start task3.
	require.Equal(t, 0, queue.Len())

	p := <-task2.GetWaitChan()
	queue.Release(p)
	require.Equal(t, 1, queue.Len())

	queue.Cancel(task3)
	require.Equal(t, 2, queue.Len())
}

// verifyOrder makes sure that the chans are called in the specified order.
func verifyOrder(t *testing.T, queue *MultiQueue, tasks ...*Task) {
	// each time, verify that the only available channel is the "next" one in order
	for i, task := range tasks {
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
