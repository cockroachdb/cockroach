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
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

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

	errCh := make(chan error)
	go func() {
		_, canceledErr := qp.Acquire(ctx, 1)
		errCh <- canceledErr
	}()

	cancel()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("context cancellation did not unblock acquisitions within 5s")
	case err := <-errCh:
		if err != context.Canceled {
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
			if _, isErrClosed := err.(*quotapool.ErrClosed); !isErrClosed {
				t.Fatal(err)
			}
		}
	}

	go tryAcquire()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("quota pool closing did not unblock acquisitions within 5s")
	case err := <-resCh:
		if _, isErrClosed := err.(*quotapool.ErrClosed); !isErrClosed {
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
			if err != context.Canceled {
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

func TestOnAcquisition(t *testing.T) {
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
		_, err = qp.AcquireFunc(ctx,
			func(_ context.Context, pi quotapool.PoolInfo) (took uint64, err error) {
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

// BenchmarkIntQuotaPool benchmarks the common case where we have sufficient
// quota available in the pool and we repeatedly acquire and release quota.
func BenchmarkIntQuotaPool(b *testing.B) {
	qp := quotapool.NewIntPool("test", 1)
	ctx := context.Background()
	for n := 0; n < b.N; n++ {
		alloc, err := qp.Acquire(ctx, 1)
		if err != nil {
			b.Fatal(err)
		}
		alloc.Release()
	}
	qp.Close("")
}

// BenchmarkConcurrentIntQuotaPool benchmarks concurrent workers in a variety
// of ratios between adequate and inadequate quota to concurrently serve all
// workers.
func BenchmarkConcurrentIntQuotaPool(b *testing.B) {
	// test returns the arguments to b.Run for a given number of workers and
	// quantity of quota.
	test := func(workers int, quota uint64) (string, func(b *testing.B)) {
		return fmt.Sprintf("workers=%d,quota=%d", workers, quota), func(b *testing.B) {
			qp := quotapool.NewIntPool("test", quota, quotapool.LogSlowAcquisition)
			g, ctx := errgroup.WithContext(context.Background())
			runWorker := func(workerNum int) {
				g.Go(func() error {
					for i := workerNum; i < b.N; i += workers {
						alloc, err := qp.Acquire(ctx, 1)
						if err != nil {
							b.Fatal(err)
						}
						runtime.Gosched()
						alloc.Release()
					}
					return nil
				})
			}
			for i := 0; i < workers; i++ {
				runWorker(i)
			}
			if err := g.Wait(); err != nil {
				b.Fatal(err)
			}
			qp.Close("")
		}
	}
	for _, c := range []struct {
		workers int
		quota   uint64
	}{
		{1, 1},
		{2, 2},
		{8, 4},
		{128, 4},
		{512, 128},
		{512, 513},
		{512, 511},
	} {
		b.Run(test(c.workers, c.quota))
	}
}

// TestLen verifies that the Len() method of the IntPool works as expected.
func TestLen(t *testing.T) {
	qp := quotapool.NewIntPool("test", 1, quotapool.LogSlowAcquisition)
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

// BenchmarkIntQuotaPoolFunc benchmarks the common case where we have sufficient
// quota available in the pool and we repeatedly acquire and release quota.
func BenchmarkIntQuotaPoolFunc(b *testing.B) {
	qp := quotapool.NewIntPool("test", 1, quotapool.LogSlowAcquisition)
	ctx := context.Background()
	toAcquire := intRequest(1)
	for n := 0; n < b.N; n++ {
		alloc, err := qp.AcquireFunc(ctx, toAcquire.acquire)
		if err != nil {
			b.Fatal(err)
		} else if acquired := alloc.Acquired(); acquired != 1 {
			b.Fatalf("expected to acquire %d, got %d", 1, acquired)
		}
		alloc.Release()
	}
	qp.Close("")
}

// intRequest is a wrapper to create a IntRequestFunc from an int64.
type intRequest uint64

func (ir intRequest) acquire(_ context.Context, pi quotapool.PoolInfo) (took uint64, err error) {
	if uint64(ir) < pi.Available {
		return 0, quotapool.ErrNotEnoughQuota
	}
	return uint64(ir), nil
}
