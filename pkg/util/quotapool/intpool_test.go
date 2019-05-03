// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package quotapool_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
)

// TestQuotaPoolBasic tests the minimal expected behavior of the quota pool
// with different sized quota pool and a varying number of goroutines, each
// acquiring a unit quota and releasing it immediately after.
func TestQuotaPoolBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	quotas := []int64{1, 10, 100, 1000}
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

// testQuotaPoolContextCancellation tests the behavior that for an ongoing
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
// acquisitions go through.
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

// TestQuotaPoolMaxQuota tests that at no point does the total quota available
// in the pool exceed the maximum amount the pool was initialized with.
func TestQuotaPoolMaxQuota(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const quota = 100
	const numGoroutines = 200
	qp := quotapool.NewIntPool("test", quota)
	ctx := context.Background()
	resCh := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			alloc, err := qp.Acquire(ctx, 2)
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

// BenchmarkQuotaPool benchmarks the common case where we have sufficient
// quota available in the pool and we repeatedly acquire and release quota.
func BenchmarkIntFuncQuotaPool(b *testing.B) {
	qp := quotapool.NewIntPool("test", 1)
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
type intRequest int64

func (ir intRequest) acquire(_ context.Context, v int64) (fulfilled bool, took int64) {
	if int64(ir) < v {
		return false, 0
	}
	return true, int64(ir)
}
