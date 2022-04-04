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
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
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

	b, err := NewBalancer(ctx, stopper, MaxConcurrentRebalances(1))
	require.NoError(t, err)

	// Use a custom time source for testing.
	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	timeSource := timeutil.NewManualTime(t0)

	// syncReq is used to wait until the test has completed processing the
	// items that are of concern.
	syncCh := make(chan struct{})
	syncReq := &rebalanceRequest{
		createdAt: timeSource.Now(),
		conn: &testBalancerConnHandle{
			onTransferConnection: func() error {
				syncCh <- struct{}{}
				return nil
			},
		},
		dst: "foo",
	}

	t.Run("retries_up_to_maxTransferAttempts", func(t *testing.T) {
		count := 0
		req := &rebalanceRequest{
			createdAt: timeSource.Now(),
			conn: &testBalancerConnHandle{
				onTransferConnection: func() error {
					count++
					return errors.New("cannot transfer")
				},
			},
			dst: "foo",
		}
		b.queue.enqueue(req)

		// Wait until the item has been processed.
		b.queue.enqueue(syncReq)
		<-syncCh

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
		b.queue.enqueue(syncReq)
		<-syncCh

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
		b.queue.enqueue(syncReq)
		<-syncCh

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
		b.queue.enqueue(syncReq)
		<-syncCh

		// We should only retry once.
		require.Equal(t, 1, count)
	})

	t.Run("limit_concurrent_rebalances", func(t *testing.T) {
		const reqCount = 100

		// Allow up to 2 concurrent rebalances.
		b.processSem.SetLimit(2)

		// wg is used to wait until all transfers have completed.
		var wg sync.WaitGroup
		wg.Add(reqCount)

		// waitCh is used to wait until all items have fully been enqueued.
		waitCh := make(chan struct{})

		var count int32
		for i := 0; i < reqCount; i++ {
			req := &rebalanceRequest{
				createdAt: timeSource.Now(),
				conn: &testBalancerConnHandle{
					onTransferConnection: func() error {
						// Block until all requests are enqueued.
						<-waitCh

						defer func() {
							newCount := atomic.AddInt32(&count, -1)
							require.True(t, newCount >= 0)
							wg.Done()
						}()

						// Count should not exceed the maximum number of
						// concurrent rebalances defined.
						newCount := atomic.AddInt32(&count, 1)
						require.True(t, newCount <= 2)
						return nil
					},
				},
				dst: "foo",
			}
			b.queue.enqueue(req)
		}

		// Close the channel to unblock.
		close(waitCh)

		// Wait until all transfers have completed.
		wg.Wait()

		// We should only transfer once for every connection.
		require.Equal(t, int32(0), count)
	})
}

func TestRebalancerQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q, err := newRebalancerQueue(ctx)
	require.NoError(t, err)

	// Use a custom time source for testing.
	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	timeSource := timeutil.NewManualTime(t0)

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
	item, err := q.dequeue(ctx)
	require.NoError(t, err)
	require.Equal(t, req3, item)
	item, err = q.dequeue(ctx)
	require.NoError(t, err)
	require.Equal(t, req4, item)
	require.Empty(t, q.elements)
	require.Equal(t, 0, q.queue.Len())

	// Cancel the context. Dequeue should return immediately with an error.
	cancel()
	req4, err = q.dequeue(ctx)
	require.EqualError(t, err, context.Canceled.Error())
	require.Nil(t, req4)
}

func TestRebalancerQueueBlocking(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q, err := newRebalancerQueue(ctx)
	require.NoError(t, err)

	reqCh := make(chan *rebalanceRequest, 10)
	go func() {
		for {
			req, err := q.dequeue(ctx)
			if err != nil {
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
