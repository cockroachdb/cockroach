// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package balancer

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestBalancer_SelectTenantPod(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	b, err := NewBalancer(ctx, stopper)
	require.NoError(t, err)

	t.Run("no pods", func(t *testing.T) {
		pod, err := b.SelectTenantPod([]*tenant.Pod{})
		require.EqualError(t, err, ErrNoAvailablePods.Error())
		require.Nil(t, pod)
	})

	t.Run("few pods", func(t *testing.T) {
		pod, err := b.SelectTenantPod([]*tenant.Pod{{Addr: "1"}, {Addr: "2"}})
		require.NoError(t, err)
		require.Contains(t, []string{"1", "2"}, pod.Addr)
	})
}

func TestRebalancer_processQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	b, err := NewBalancer(ctx, stopper, MaxConcurrentRebalances(2))
	require.NoError(t, err)

	// Use a custom time source for testing.
	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	timeSource := timeutil.NewManualTime(t0)

	eventCh := make(chan struct{})
	b.testingKnobs.afterProcessQueueItem = func() {
		eventCh <- struct{}{}
	}

	t.Run("retries_up_to_maxTransferAttempts", func(t *testing.T) {
		count := 0
		conn := &testBalancerConnHandle{
			onTransferConnection: func() error {
				count++
				return errors.New("cannot transfer")
			},
		}
		req := &rebalanceRequest{
			createdAt: timeSource.Now(),
			conn:      conn,
			dst:       "foo",
		}
		b.queue.enqueue(req)

		// Wait until the item has been processed.
		<-eventCh

		// Ensure that we only retried up to 3 times.
		require.Equal(t, 3, count)
	})

	t.Run("conn_was_transferred_by_other", func(t *testing.T) {
		count := 0
		conn := &testBalancerConnHandle{}
		conn.onTransferConnection = func() error {
			count++
			// Simulate that connection was transferred by someone else.
			conn.remoteAddr = "foo"
			return errors.New("cannot transfer")
		}
		req := &rebalanceRequest{
			createdAt: timeSource.Now(),
			conn:      conn,
			dst:       "foo",
		}
		b.queue.enqueue(req)

		// Wait until the item has been processed.
		<-eventCh

		// We should only retry once.
		require.Equal(t, 1, count)
	})

	t.Run("conn_was_transferred", func(t *testing.T) {
		count := 0
		conn := &testBalancerConnHandle{}
		conn.onTransferConnection = func() error {
			count++
			conn.remoteAddr = "foo"
			return nil
		}
		req := &rebalanceRequest{
			createdAt: timeSource.Now(),
			conn:      conn,
			dst:       "foo",
		}
		b.queue.enqueue(req)

		// Wait until the item has been processed.
		<-eventCh

		// We should only retry once.
		require.Equal(t, 1, count)
	})

	t.Run("conn_was_closed", func(t *testing.T) {
		count := 0
		conn := &testBalancerConnHandle{}
		conn.onTransferConnection = func() error {
			count++
			return context.Canceled
		}
		req := &rebalanceRequest{
			createdAt: timeSource.Now(),
			conn:      conn,
			dst:       "foo",
		}
		b.queue.enqueue(req)

		// Wait until the item has been processed.
		<-eventCh

		// We should only retry once.
		require.Equal(t, 1, count)
	})

	t.Run("limit_concurrent_rebalances", func(t *testing.T) {
		var wg sync.WaitGroup
		var mu struct {
			syncutil.Mutex
			count int
		}
		b.testingKnobs.beforeProcessQueueItem = func() {
			mu.Lock()
			defer mu.Unlock()

			// Count should not exceed the maximum number of concurrent
			// rebalances defined.
			mu.count++
			require.True(t, mu.count <= 2)
		}
		b.testingKnobs.afterProcessQueueItem = func() {
			mu.Lock()
			defer mu.Unlock()

			mu.count--
			wg.Done()
		}

		const reqCount = 100
		wg.Add(reqCount)
		waitCh := make(chan struct{})
		for i := 0; i < reqCount; i++ {
			conn := &testBalancerConnHandle{
				onTransferConnection: func() error {
					// Wait until all requests have been enqueued.
					<-waitCh
					return nil
				},
			}
			req := &rebalanceRequest{
				createdAt: timeSource.Now(),
				conn:      conn,
				dst:       "foo",
			}
			b.queue.enqueue(req)
		}

		// Close channel to start processing requests.
		close(waitCh)

		// Wait until all transfers have completed.
		wg.Wait()

		// We should only retry once.
		require.Equal(t, 0, mu.count)
	})
}

func TestRebalancerQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	q := newRebalancerQueue()
	require.False(t, q.isClosed())

	// Use a custom time source for testing.
	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	timeSource := timeutil.NewManualTime(t0)

	// Enqueuing a nil request should be a no-op.
	q.enqueue(nil)
	require.Empty(t, q.elements)
	require.Equal(t, 0, q.queue.Len())

	// Create rebalance requests for the same connection handle.
	conn1 := &testBalancerConnHandle{}
	req1 := &rebalanceRequest{
		createdAt: timeSource.Now(),
		conn:      conn1,
		dst:       "foo1",
	}
	timeSource.Advance(5 * time.Second)
	req2 := &rebalanceRequest{
		createdAt: timeSource.Now(),
		conn:      conn1,
		dst:       "foo2",
	}
	timeSource.Advance(5 * time.Second)
	req3 := &rebalanceRequest{
		createdAt: timeSource.Now(),
		conn:      conn1,
		dst:       "foo3",
	}

	// Enqueue in a specific order. req3 overrides req1; req2 is a no-op.
	q.enqueue(req1)
	q.enqueue(req3)
	q.enqueue(req2)
	require.Len(t, q.elements, 1)
	require.Equal(t, 1, q.queue.Len())

	// Create another request.
	conn2 := &testBalancerConnHandle{}
	req4 := &rebalanceRequest{
		createdAt: timeSource.Now(),
		conn:      conn2,
		dst:       "bar1",
	}
	q.enqueue(req4)
	require.Len(t, q.elements, 2)
	require.Equal(t, 2, q.queue.Len())

	// Dequeue the items.
	item := q.dequeue()
	require.Equal(t, req3, item)
	item = q.dequeue()
	require.Equal(t, req4, item)
	require.Empty(t, q.elements)
	require.Equal(t, 0, q.queue.Len())

	// Close the queue. Enqueue is a no-op, dequeue returns immediately with nil.
	q.close()
	require.True(t, q.isClosed())
	q.enqueue(req4)
	require.Empty(t, q.elements)
	require.Equal(t, 0, q.queue.Len())
	require.Nil(t, q.dequeue())
}

func TestRebalancerQueueBlocking(t *testing.T) {
	defer leaktest.AfterTest(t)()

	q := newRebalancerQueue()

	reqCh := make(chan *rebalanceRequest, 10)
	go func() {
		for {
			req := q.dequeue()
			if req == nil {
				break
			}
			reqCh <- req
		}
	}()

	// Use a custom time source for testing.
	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	timeSource := timeutil.NewManualTime(t0)

	const reqCount = 100
	for i := 0; i < reqCount; i++ {
		req := &rebalanceRequest{
			createdAt: timeSource.Now(),
			conn:      &testBalancerConnHandle{},
			dst:       fmt.Sprint(i),
		}
		q.enqueue(req)
		timeSource.Advance(1 * time.Second)
	}

	for i := 0; i < reqCount; i++ {
		req := <-reqCh
		require.Equal(t, fmt.Sprint(i), req.dst)
	}

	q.close()
	require.True(t, q.isClosed())
}

// testBalancerConnHandle is a test connection handle that is used for testing
// the balancer.
type testBalancerConnHandle struct {
	ConnectionHandle
	remoteAddr           string
	onTransferConnection func() error
}

var _ ConnectionHandle = &testBalancerConnHandle{}

// TransferConnection implements the ConnectionHandle interface.
func (h *testBalancerConnHandle) TransferConnection() error {
	return h.onTransferConnection()
}

// ServerRemoteAddr implements the ConnectionHandle interface.
func (h *testBalancerConnHandle) ServerRemoteAddr() string {
	return h.remoteAddr
}
