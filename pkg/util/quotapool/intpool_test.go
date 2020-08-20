// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package quotapool_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// logSlowAcquisition is an Option to log slow acquisitions.
var logSlowAcquisition = quotapool.OnSlowAcquisition(2*time.Second, quotapool.LogSlowAcquisition)

// TestQuotaPoolBasic tests the minimal expected behavior of the quota pool
// with different sized quota pool and a varying number of goroutines, each
// acquiring a unit quota and releasing it immediately after.
func TestQuotaPoolBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	quotas := []uint64{1, 10, 100, 1000}
	goroutineCounts := []int{1, 10, 100}

	for _, quota := range quotas {
		for _, numGoroutines := range goroutineCounts {
			qp := quotapool.NewIntPool("test", quota)
			ctx := context.Background()
			resCh := make(chan error, numGoroutines)

			for i := 0; i < numGoroutines; i++ {
				go func() {
					alloc, err := qp.Acquire(ctx, 1)
					if err != nil {
						resCh <- err
						return
					}
					alloc.Release()
					resCh <- nil
				}()
			}

			for i := 0; i < numGoroutines; i++ {
				select {
				case <-time.After(5 * time.Second):
					t.Fatal("did not complete acquisitions within 5s")
				case err := <-resCh:
					if err != nil {
						t.Fatal(err)
					}
				}
			}

			if q := qp.ApproximateQuota(); q != quota {
				t.Fatalf("expected quota: %d, got: %d", quota, q)
			}
		}
	}
}

// TestQuotaPoolContextCancellation tests the behavior that for an ongoing
// blocked acquisition, if the context passed in gets canceled the acquisition
// gets canceled too with an error indicating so. This should not affect the
// available quota in the pool.
func TestQuotaPoolContextCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	qp := quotapool.NewIntPool("test", 1)
	alloc, err := qp.Acquire(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() {
		_, canceledErr := qp.Acquire(ctx, 1)
		errCh <- canceledErr
	}()

	cancel()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("context cancellation did not unblock acquisitions within 5s")
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation error, got %v", err)
		}
	}

	alloc.Release()

	if q := qp.ApproximateQuota(); q != 1 {
		t.Fatalf("expected quota: 1, got: %d", q)
	}
}

// TestQuotaPoolClose tests the behavior that for an ongoing blocked
// acquisition if the quota pool gets closed, all ongoing and subsequent
// acquisitions return an *ErrClosed.
func TestQuotaPoolClose(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	qp := quotapool.NewIntPool("test", 1)
	if _, err := qp.Acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}
	const numGoroutines = 5
	resCh := make(chan error, numGoroutines)

	tryAcquire := func() {
		_, err := qp.Acquire(ctx, 1)
		resCh <- err
	}
	for i := 0; i < numGoroutines; i++ {
		go tryAcquire()
	}

	qp.Close("")

	// Second call should be a no-op.
	qp.Close("")

	for i := 0; i < numGoroutines; i++ {
		select {
		case <-time.After(5 * time.Second):
			t.Fatal("quota pool closing did not unblock acquisitions within 5s")
		case err := <-resCh:
			if !errors.HasType(err, (*quotapool.ErrClosed)(nil)) {
				t.Fatal(err)
			}
		}
	}

	go tryAcquire()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("quota pool closing did not unblock acquisitions within 5s")
	case err := <-resCh:
		if !errors.HasType(err, (*quotapool.ErrClosed)(nil)) {
			t.Fatal(err)
		}
	}
}

// TestQuotaPoolCanceledAcquisitions tests the behavior where we enqueue
// multiple acquisitions with canceled contexts and expect any subsequent
// acquisition with a valid context to proceed without error.
func TestQuotaPoolCanceledAcquisitions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	qp := quotapool.NewIntPool("test", 1)
	alloc, err := qp.Acquire(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}

	cancel()
	const numGoroutines = 5

	errCh := make(chan error)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			_, err := qp.Acquire(ctx, 1)
			errCh <- err
		}()
	}

	for i := 0; i < numGoroutines; i++ {
		select {
		case <-time.After(5 * time.Second):
			t.Fatal("context cancellations did not unblock acquisitions within 5s")
		case err := <-errCh:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("expected context cancellation error, got %v", err)
			}
		}
	}

	alloc.Release()
	go func() {
		_, err := qp.Acquire(context.Background(), 1)
		errCh <- err
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("acquisition didn't go through within 5s")
	}
}

// TestQuotaPoolNoops tests that quota pool operations that should be noops are
// so, e.g. quotaPool.acquire(0) and quotaPool.release(0).
func TestQuotaPoolNoops(t *testing.T) {
	defer leaktest.AfterTest(t)()

	qp := quotapool.NewIntPool("test", 1)
	ctx := context.Background()
	initialAlloc, err := qp.Acquire(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Acquisition of blockedAlloc will block until initialAlloc is released.
	errCh := make(chan error)
	var blockedAlloc *quotapool.IntAlloc
	go func() {
		blockedAlloc, err = qp.Acquire(ctx, 1)
		errCh <- err
	}()

	// Allocation of zero should not block.
	emptyAlloc, err := qp.Acquire(ctx, 0)
	if err != nil {
		t.Fatalf("failed to acquire 0 quota: %v", err)
	}
	emptyAlloc.Release() // Release of 0 should do nothing

	initialAlloc.Release()
	select {
	case <-time.After(5 * time.Second):
		t.Fatal("context cancellations did not unblock acquisitions within 5s")
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	}
	if q := qp.ApproximateQuota(); q != 0 {
		t.Fatalf("expected quota: 0, got: %d", q)
	}
	blockedAlloc.Release()
	if q := qp.ApproximateQuota(); q != 1 {
		t.Fatalf("expected quota: 1, got: %d", q)
	}
}

// TestQuotaPoolMaxQuota tests that Acquire cannot acquire more than the
// maximum amount with which the pool was initialized.
func TestQuotaPoolMaxQuota(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const quota = 100
	qp := quotapool.NewIntPool("test", quota)
	ctx := context.Background()
	alloc, err := qp.Acquire(ctx, 2*quota)
	if err != nil {
		t.Fatal(err)
	}
	if got := alloc.Acquired(); got != quota {
		t.Fatalf("expected to acquire the capacity quota %d, instead got %d", quota, got)
	}
	alloc.Release()
	if q := qp.ApproximateQuota(); q != quota {
		t.Fatalf("expected quota: %d, got: %d", quota, q)
	}
}

// TestQuotaPoolCappedAcquisition verifies that when an acquisition request
// greater than the maximum quota is placed, we still allow the acquisition to
// proceed but after having acquired the maximum quota amount.
func TestQuotaPoolCappedAcquisition(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const quota = 1
	qp := quotapool.NewIntPool("test", quota)
	alloc, err := qp.Acquire(context.Background(), quota*100)
	if err != nil {
		t.Fatal(err)
	}

	if q := qp.ApproximateQuota(); q != 0 {
		t.Fatalf("expected quota: %d, got: %d", 0, q)
	}

	alloc.Release()
	if q := qp.ApproximateQuota(); q != quota {
		t.Fatalf("expected quota: %d, got: %d", quota, q)
	}
}

// TestQuotaPoolZeroCapacity verifies that a non-noop acquisition request on a
// pool with zero capacity is immediately rejected, regardless of whether the
// request is permitted to wait or not.
func TestQuotaPoolZeroCapacity(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const quota = 0
	qp := quotapool.NewIntPool("test", quota)
	ctx := context.Background()

	failed, err := qp.Acquire(ctx, 1)
	require.Equal(t, quotapool.ErrNotEnoughQuota, err)
	require.Nil(t, failed)

	failed, err = qp.TryAcquire(ctx, 1)
	require.Equal(t, quotapool.ErrNotEnoughQuota, err)
	require.Nil(t, failed)

	acq1, err := qp.Acquire(ctx, 0)
	require.NoError(t, err)
	acq1.Release()

	acq2, err := qp.TryAcquire(ctx, 0)
	require.NoError(t, err)
	acq2.Release()
}

func TestOnAcquisition(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const quota = 100
	var called bool
	qp := quotapool.NewIntPool("test", quota,
		quotapool.OnAcquisition(func(ctx context.Context, poolName string, _ quotapool.Request, start time.Time,
		) {
			assert.Equal(t, poolName, "test")
			called = true
		}))
	ctx := context.Background()
	alloc, err := qp.Acquire(ctx, 1)
	assert.Nil(t, err)
	assert.True(t, called)
	alloc.Release()
}

// TestSlowAcquisition ensures that the SlowAcquisition callback is called
// when an Acquire call takes longer than the configured timeout.
func TestSlowAcquisition(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// The test will set up an IntPool with 1 quota and a SlowAcquisition callback
	// which closes channels when called by the second goroutine. An initial call
	// to Acquire will take all of the quota. Then a second call with go should be
	// blocked leading to the callback being triggered.

	// In order to prevent the first call to Acquire from triggering the callback
	// we mark its context with a value.
	ctx := context.Background()
	type ctxKey int
	firstKey := ctxKey(1)
	firstCtx := context.WithValue(ctx, firstKey, "foo")
	slowCalled, acquiredCalled := make(chan struct{}), make(chan struct{})
	const poolName = "test"
	f := func(ctx context.Context, name string, _ quotapool.Request, _ time.Time) func() {
		assert.Equal(t, poolName, name)
		if ctx.Value(firstKey) != nil {
			return func() {}
		}
		close(slowCalled)
		return func() {
			close(acquiredCalled)
		}
	}
	qp := quotapool.NewIntPool(poolName, 1, quotapool.OnSlowAcquisition(time.Microsecond, f))
	alloc, err := qp.Acquire(firstCtx, 1)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_, _ = qp.Acquire(ctx, 1)
	}()
	select {
	case <-slowCalled:
	case <-time.After(time.Second):
		t.Fatalf("OnSlowAcquisition not called long after timeout")
	}
	select {
	case <-acquiredCalled:
		t.Fatalf("acquired callback called when insufficient quota was available")
	default:
	}
	alloc.Release()
	select {
	case <-slowCalled:
	case <-time.After(time.Second):
		t.Fatalf("OnSlowAcquisition acquired callback not called long after timeout")
	}
}

// Test that AcquireFunc() is called after IntAlloc.Freeze() is called - so that an ongoing acquisition gets
// the chance to observe that there's no capacity for its request.
func TestQuotaPoolCapacityDecrease(t *testing.T) {
	defer leaktest.AfterTest(t)()

	qp := quotapool.NewIntPool("test", 100)
	ctx := context.Background()

	alloc50, err := qp.Acquire(ctx, 50)
	if err != nil {
		t.Fatal(err)
	}

	first := true
	firstCh := make(chan struct{})
	doneCh := make(chan struct{})
	go func() {
		_, err = qp.AcquireFunc(ctx, func(_ context.Context, pi quotapool.PoolInfo) (took uint64, err error) {
			if first {
				first = false
				close(firstCh)
			}
			if pi.Capacity < 100 {
				return 0, fmt.Errorf("hopeless")
			}
			return 0, quotapool.ErrNotEnoughQuota
		})
		close(doneCh)
	}()

	// Wait for the callback to be called the first time. It should return ErrNotEnoughQuota.
	<-firstCh
	// Now leak the quota. This should call the callback to be called again.
	alloc50.Freeze()
	<-doneCh
	if !testutils.IsError(err, "hopeless") {
		t.Fatalf("expected hopeless error, got: %v", err)
	}
}

func TestIntpoolNoWait(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	qp := quotapool.NewIntPool("test", 2)

	acq1, err := qp.TryAcquire(ctx, 1)
	require.NoError(t, err)

	acq2, err := qp.TryAcquire(ctx, 1)
	require.NoError(t, err)

	failed, err := qp.TryAcquire(ctx, 1)
	require.Equal(t, quotapool.ErrNotEnoughQuota, err)
	require.Nil(t, failed)

	acq1.Release()

	failed, err = qp.TryAcquire(ctx, 2)
	require.Equal(t, quotapool.ErrNotEnoughQuota, err)
	require.Nil(t, failed)

	acq2.Release()

	acq5, err := qp.TryAcquire(ctx, 3)
	require.NoError(t, err)
	require.NotNil(t, acq5)

	failed, err = qp.TryAcquireFunc(ctx, func(ctx context.Context, p quotapool.PoolInfo) (took uint64, err error) {
		require.Equal(t, uint64(0), p.Available)
		return 0, quotapool.ErrNotEnoughQuota
	})
	require.Equal(t, quotapool.ErrNotEnoughQuota, err)
	require.Nil(t, failed)

	acq5.Release()

	acq6, err := qp.TryAcquireFunc(ctx, func(ctx context.Context, p quotapool.PoolInfo) (took uint64, err error) {
		return 1, nil
	})
	require.NoError(t, err)

	acq6.Release()
}

// TestIntpoolRelease tests the Release method of intpool to ensure that it releases
// what is expected and behaves as documented.
func TestIntpoolRelease(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	const numPools = 3
	const capacity = 3
	// Populated full because it's handy for cases where all quota is returned.
	var full [numPools]uint64
	for i := 0; i < numPools; i++ {
		full[i] = capacity
	}
	makePools := func() (pools [numPools]*quotapool.IntPool) {
		for i := 0; i < numPools; i++ {
			pools[i] = quotapool.NewIntPool(strconv.Itoa(i), capacity)
		}
		return pools
	}

	type acquisition struct {
		pool int
		q    uint64
	}
	type testCase struct {
		toAcquire   []*acquisition
		exclude     int
		releasePool int
		expQuota    [numPools]uint64
	}
	// First acquire all the quota, then release all but the trailing exclude
	// allocs into the releasePool and ensure that the pools have expQuota.
	// Finally release the rest of the allocs and ensure that the pools are full.
	runTest := func(c *testCase) func(t *testing.T) {
		return func(t *testing.T) {
			pools := makePools()
			allocs := make([]*quotapool.IntAlloc, len(c.toAcquire))
			for i, acq := range c.toAcquire {
				if acq == nil {
					continue
				}
				require.LessOrEqual(t, acq.q, uint64(capacity))
				alloc, err := pools[acq.pool].Acquire(ctx, acq.q)
				require.NoError(t, err)
				allocs[i] = alloc
			}
			prefix := len(allocs) - c.exclude
			pools[c.releasePool].Release(allocs[:prefix]...)
			for i, p := range pools {
				require.Equal(t, c.expQuota[i], p.ApproximateQuota())
			}
			pools[c.releasePool].Release(allocs[prefix:]...)
			for i, p := range pools {
				require.Equal(t, full[i], p.ApproximateQuota())
			}
		}
	}
	for i, c := range []testCase{
		{
			toAcquire: []*acquisition{
				{0, 1},
				{1, 2},
				{1, 1},
				nil,
			},
			expQuota: full,
		},
		{
			releasePool: 1,
			toAcquire: []*acquisition{
				{0, 1},
				{1, 2},
				{1, 1},
				nil,
			},
			expQuota: full,
		},
		{
			toAcquire: []*acquisition{
				nil,
				{0, capacity},
				{1, capacity},
				{2, capacity},
			},
			exclude:  1,
			expQuota: [numPools]uint64{0: capacity, 1: capacity},
		},
		{
			toAcquire: []*acquisition{
				nil,
				{0, capacity},
				{1, capacity},
				{2, capacity},
			},
			exclude:  3,
			expQuota: [numPools]uint64{},
		},
	} {
		t.Run(strconv.Itoa(i), runTest(&c))
	}
}

// TestLen verifies that the Len() method of the IntPool works as expected.
func TestLen(t *testing.T) {
	defer leaktest.AfterTest(t)()

	qp := quotapool.NewIntPool("test", 1, logSlowAcquisition)
	ctx := context.Background()
	allocCh := make(chan *quotapool.IntAlloc)
	doAcquire := func(ctx context.Context) {
		alloc, err := qp.Acquire(ctx, 1)
		if ctx.Err() == nil && assert.Nil(t, err) {
			allocCh <- alloc
		}
	}
	assertLenSoon := func(exp int) {
		testutils.SucceedsSoon(t, func() error {
			if got := qp.Len(); got != exp {
				return errors.Errorf("expected queue len to be %d, got %d", got, exp)
			}
			return nil
		})
	}
	// Initially qp should have a length of 0.
	assert.Equal(t, 0, qp.Len())
	// Acquire all of the quota from the pool.
	alloc, err := qp.Acquire(ctx, 1)
	assert.Nil(t, err)
	// The length should still be 0.
	assert.Equal(t, 0, qp.Len())
	// Launch a goroutine to acquire quota, ensure that the length increases.
	go doAcquire(ctx)
	assertLenSoon(1)
	// Create more goroutines which will block to be canceled later in order to
	// ensure that cancelations deduct from the length.
	const numToCancel = 12 // an arbitrary number
	ctxToCancel, cancel := context.WithCancel(ctx)
	for i := 0; i < numToCancel; i++ {
		go doAcquire(ctxToCancel)
	}
	// Ensure that all of the new goroutines are reflected in the length.
	assertLenSoon(numToCancel + 1)
	// Launch another goroutine with the default context.
	go doAcquire(ctx)
	assertLenSoon(numToCancel + 2)
	// Cancel some of the goroutines.
	cancel()
	// Ensure that they are soon not reflected in the length.
	assertLenSoon(2)
	// Unblock the first goroutine.
	alloc.Release()
	alloc = <-allocCh
	assert.Equal(t, 1, qp.Len())
	// Unblock the second goroutine.
	alloc.Release()
	<-allocCh
	assert.Equal(t, 0, qp.Len())
}

// TestIntpoolIllegalCapacity ensures that constructing an IntPool with capacity
// in excess of math.MaxInt64 will panic.
func TestIntpoolWithExcessCapacity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, c := range []uint64{
		1, math.MaxInt64 - 1, math.MaxInt64,
	} {
		require.NotPanics(t, func() {
			quotapool.NewIntPool("test", c)
		})
	}
	for _, c := range []uint64{
		math.MaxUint64, math.MaxUint64 - 1, math.MaxInt64 + 1,
	} {
		require.Panics(t, func() {
			quotapool.NewIntPool("test", c)
		})
	}
}

// TestUpdateCapacityFluctuationsPermitExcessAllocs exercises the case where the
// capacity of the IntPool fluctuates. We want to ensure that the amount of
// outstanding quota never exceeds the largest capacity available during the
// time when any of the outstanding allocations were acquired.
func TestUpdateCapacityFluctuationsPreventsExcessAllocs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	qp := quotapool.NewIntPool("test", 1)
	var allocs []*quotapool.IntAlloc
	allocCh := make(chan *quotapool.IntAlloc)
	defer close(allocCh)
	go func() {
		for a := range allocCh {
			if a == nil {
				continue // allow nil channel sends to synchronize
			}
			allocs = append(allocs, a)
		}
	}()
	acquireN := func(n int) {
		var wg sync.WaitGroup
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				alloc, err := qp.Acquire(ctx, 1)
				assert.NoError(t, err)
				allocCh <- alloc
			}()
		}
		wg.Wait()
	}

	acquireN(1)
	qp.UpdateCapacity(100)
	acquireN(99)
	require.Equal(t, uint64(0), qp.ApproximateQuota())
	qp.UpdateCapacity(1)
	// Internally the representation of the quota should be -99 but we don't
	// expose negative quota to users.
	require.Equal(t, uint64(0), qp.ApproximateQuota())

	// Update the capacity so now there's actually 0.
	qp.UpdateCapacity(100)
	require.Equal(t, uint64(0), qp.ApproximateQuota())
	_, err := qp.TryAcquire(ctx, 1)
	require.Equal(t, quotapool.ErrNotEnoughQuota, err)

	// Now update the capacity so that there's 1.
	qp.UpdateCapacity(101)
	require.Equal(t, uint64(1), qp.ApproximateQuota())
	_, err = qp.TryAcquire(ctx, 2)
	require.Equal(t, quotapool.ErrNotEnoughQuota, err)
	acquireN(1)
	allocCh <- nil // synchronize
	// Release all of the quota back to the pool.
	for _, a := range allocs {
		a.Release()
	}
	allocs = nil
	require.Equal(t, uint64(101), qp.ApproximateQuota())
}

func TestQuotaPoolUpdateCapacity(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	ch := make(chan *quotapool.IntAlloc, 1)
	qp := quotapool.NewIntPool("test", 1)
	alloc, err := qp.Acquire(ctx, 1)
	require.NoError(t, err)
	go func() {
		blocked, err := qp.Acquire(ctx, 2)
		assert.NoError(t, err)
		ch <- blocked
	}()
	ensureBlocked := func() {
		t.Helper()
		select {
		case <-time.After(10 * time.Millisecond): // ensure the acquisition fails for now
		case got := <-ch:
			got.Release()
			t.Fatal("expected acquisition to fail")
		}
	}
	ensureBlocked()
	// Update the capacity to 2, the request should still be blocked.
	qp.UpdateCapacity(2)
	ensureBlocked()
	qp.UpdateCapacity(3)
	got := <-ch
	require.Equal(t, uint64(2), got.Acquired())
	require.Equal(t, uint64(3), qp.Capacity())
	alloc.Release()
	require.Equal(t, uint64(1), qp.ApproximateQuota())
}

func TestConcurrentUpdatesAndAcquisitions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	var wg sync.WaitGroup
	const maxCap = 100
	qp := quotapool.NewIntPool("test", maxCap, logSlowAcquisition)
	const N = 100
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runtime.Gosched()
			newCap := uint64(rand.Intn(maxCap-1)) + 1
			qp.UpdateCapacity(newCap)
		}()
	}
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runtime.Gosched()
			acq, err := qp.Acquire(ctx, uint64(rand.Intn(maxCap)))
			assert.NoError(t, err)
			runtime.Gosched()
			acq.Release()
		}()
	}
	wg.Wait()
	qp.UpdateCapacity(maxCap)
	assert.Equal(t, uint64(maxCap), qp.ApproximateQuota())
}

// This test ensures that if you attempt to freeze an alloc which would make
// the IntPool have negative capacity a panic occurs.
func TestFreezeUnavailableCapacityPanics(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	qp := quotapool.NewIntPool("test", 10)
	acq, err := qp.Acquire(ctx, 10)
	require.NoError(t, err)
	qp.UpdateCapacity(9)
	require.Panics(t, func() {
		acq.Freeze()
	})
}

// TestLogSlowAcquisition tests that logging that occur only after a specified
// period indeed does occur after that period.
func TestLogSlowAcquisition(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	mt := timeutil.NewManualTime(t0)
	var called, calledAfter int64
	qp := quotapool.NewIntPool("test", 10,
		quotapool.OnSlowAcquisition(time.Second, func(
			ctx context.Context, poolName string, r quotapool.Request, start time.Time,
		) (onAcquire func()) {
			atomic.AddInt64(&called, 1)
			return func() {
				atomic.AddInt64(&calledAfter, 1)
			}
		}),
		quotapool.WithTimeSource(mt))
	defer qp.Close("")
	ctx := context.Background()
	waitFor1 := func(t *testing.T, p *int64) {
		testutils.SucceedsSoon(t, func() error {
			switch got := atomic.LoadInt64(p); got {
			case 0:
				return errors.Errorf("not yet called")
			case 1:
				return nil
			default:
				t.Fatal("expected to be called once")
				return nil // unreachable
			}
		})
	}
	acq1, err := qp.Acquire(ctx, 1)
	acq2, err := qp.Acquire(ctx, 8)
	require.NoError(t, err)
	var newAck *quotapool.IntAlloc
	errCh := make(chan error, 1)
	acquireFuncCalled := make(chan struct{}, 1)
	go func() {
		newAck, err = qp.AcquireFunc(ctx, func(ctx context.Context, p quotapool.PoolInfo) (took uint64, err error) {
			select {
			case acquireFuncCalled <- struct{}{}:
			default:
			}
			if p.Available < 10 {
				return 0, quotapool.ErrNotEnoughQuota
			}
			return 10, nil
		})
		defer newAck.Release()
		errCh <- err
	}()
	// For the fast path.
	<-acquireFuncCalled
	acq1.Release()
	<-acquireFuncCalled
	mt.Advance(time.Minute)
	waitFor1(t, &called)
	require.Equal(t, int64(0), atomic.LoadInt64(&calledAfter))
	acq2.Release()
	require.NoError(t, <-errCh)
	require.Equal(t, int64(1), atomic.LoadInt64(&calledAfter))
}

// TestCloser tests that the WithCloser option works.
func TestCloser(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	mt := timeutil.NewManualTime(t0)
	closer := make(chan struct{})
	qp := quotapool.NewIntPool("test", 10,
		quotapool.WithCloser(closer),
		quotapool.WithTimeSource(mt))
	ctx := context.Background()
	_, err := qp.Acquire(ctx, 10)
	require.NoError(t, err)
	errCh := make(chan error, 1)
	go func() {
		_, err := qp.Acquire(ctx, 1)
		errCh <- err
	}()
	close(closer)
	require.True(t, quotapool.HasErrClosed(<-errCh))
}
