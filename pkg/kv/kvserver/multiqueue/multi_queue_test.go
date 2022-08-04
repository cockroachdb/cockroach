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
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestMultiQueueEmpty makes sure that an empty queue can be created, started
// and stopped.
func TestMultiQueueEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()

	queue := NewMultiQueue("test", 1)
	queue.Start(ctx, stopper)
	stopper.Stop(ctx)
}

// TestMultiQueueAddTwiceSameQueue makes sure that for a single queue the
// priority is respected.
func TestMultiQueueAddTwiceSameQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	queue := NewMultiQueue("test", 1)

	chan1 := queue.Add("a", 1.0)
	chan2 := queue.Add("a", 2.0)

	queue.Start(ctx, stopper)

	// Verify chan2 is higher priority so runs first.
	verifyOrder(t, queue, chan2, chan1)
	stopper.Stop(ctx)
}

// TestMultiQueueTwoQueues checks that if requests are added to two queue names,
// they are called in a round-robin order. It also verifies that the priority is
// respected for each.
func TestMultiQueueTwoQueues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	queue := NewMultiQueue("test", 1)

	a1 := queue.Add("a", 4.0)
	a2 := queue.Add("a", 5.0)

	b1 := queue.Add("b", 1.0)
	b2 := queue.Add("b", 2.0)

	// The queue starts with the "second" item added.
	queue.Start(ctx, stopper)
	verifyOrder(t, queue, a2, b2, a1, b1)
	stopper.Stop(ctx)
}

// TestMultiQueueComplex verifies that with multiple queues, some added before
// and some after we start running, that the final order is still as expected.
// The expectation is that it round robins through the queues (a, b, c, ...) and
// runs higher priority tasks before lower priority within a queue.
func TestMultiQueueComplex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()

	queue := NewMultiQueue("test", 1)

	a2 := queue.Add("a", 4.0)
	b1 := queue.Add("b", 1.1)
	b2 := queue.Add("b", 2.1)
	c2 := queue.Add("c", 1.2)
	c3 := queue.Add("c", 2.2)
	a3 := queue.Add("a", 5.0)
	b3 := queue.Add("b", 6.1)

	queue.Start(ctx, stopper)

	verifyOrder(t, queue, a3, b3, c3, a2, b2, c2, b1)
	stopper.Stop(ctx)
}

func TestMultiQueueRemove(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()

	queue := NewMultiQueue("test", 1)

	a2 := queue.Add("a", 4.0)
	b1 := queue.Add("b", 1.1)
	b2 := queue.Add("b", 2.1)
	c2 := queue.Add("c", 1.2)
	c3 := queue.Add("c", 2.2)
	a3 := queue.Add("a", 5.0)
	b3 := queue.Add("b", 6.1)

	queue.Cancel(b2)
	queue.Cancel(b1)

	queue.Start(ctx, stopper)

	verifyOrder(t, queue, a3, b3, c3, a2, c2)
	stopper.Stop(ctx)
}

// TestMultiQueueStress calls Add from multiple threads. It chooses different
// names and different priorities for the requests. The goal is simply to make
// sure that all the requests are serviced and nothing hangs or fails.
func TestMultiQueueStress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "cancel", func(t *testing.T, alsoCancel bool) {
		testutils.RunTrueAndFalse(t, "sleep", func(t *testing.T, alsoSleep bool) {

			ctx := context.Background()
			stopper := stop.NewStopper()

			queue := NewMultiQueue("test", 5)
			queue.Start(ctx, stopper)

			numThreads := 10
			numRequests := 1000
			var wg sync.WaitGroup
			wg.Add(numThreads)
			var ops int64
			var timeCancels int64

			for i := 0; i < numThreads; i++ {
				go func(name string) {
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
				}("queue" + fmt.Sprint(i%4))
			}
			wg.Wait()
			fmt.Printf("Num time cancels %d / %d\n", timeCancels, ops)
			require.Equal(t, int64(numThreads*numRequests), ops)
			stopper.Stop(ctx)
		})
	})
}

// verifyOrder makes sure that the chans are called in the specified order.
func verifyOrder(t *testing.T, queue *MultiQueue, tasks ...*Task) {
	// each time, verify that the only available channel is the "next" one in order
	for i, task := range tasks {
		var found Permit
		testutils.SucceedsWithin(t, func() error {
			for j, t2 := range tasks[i+1:] {
				select {
				case <-t2.GetWaitChan():
					return errors.Newf("Queue active when should not be iter %d, chan %d, task %v", i, i+1+j, t2)
				default:
				}
			}
			select {
			case p := <-task.GetWaitChan():
				found = p
			default:
				return errors.Newf("Queue not active when should be Queue %d : task %v", i, task)
			}
			return nil
		}, 2*time.Second)
		queue.Release(found)
	}
}
