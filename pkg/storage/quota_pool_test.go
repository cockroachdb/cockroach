// Copyright 2017 The Cockroach Authors.
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

package storage

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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
			qp := newQuotaPool(quota)
			ctx := context.Background()
			resCh := make(chan error, numGoroutines)

			for i := 0; i < numGoroutines; i++ {
				go func() {
					if err := qp.acquire(ctx, 1); err != nil {
						resCh <- err
						return
					}
					qp.add(1)
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

			if q := qp.approximateQuota(); q != quota {
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
	qp := newQuotaPool(1)
	if err := qp.acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error)
	go func() {
		errCh <- qp.acquire(ctx, 1)
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

	qp.add(1)

	if q := qp.approximateQuota(); q != 1 {
		t.Fatalf("expected quota: 1, got: %d", q)
	}
}

// TestQuotaPoolClose tests the behavior that for an ongoing blocked
// acquisition if the quota pool gets closed, all ongoing and subsequent
// acquisitions go through.
func TestQuotaPoolClose(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	qp := newQuotaPool(1)
	if err := qp.acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}
	const numGoroutines = 5
	resCh := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			resCh <- qp.acquire(ctx, 1)
		}()
	}

	qp.close()

	// Second call should be a no-op.
	qp.close()

	for i := 0; i < numGoroutines; i++ {
		select {
		case <-time.After(5 * time.Second):
			t.Fatal("quota pool closing did not unblock acquisitions within 5s")
		case err := <-resCh:
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	go func() {
		resCh <- qp.acquire(ctx, 1)
	}()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("quota pool closing did not unblock acquisitions within 5s")
	case err := <-resCh:
		if err != nil {
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
	qp := newQuotaPool(1)
	if err := qp.acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}

	cancel()
	const numGoroutines = 5

	errCh := make(chan error)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			errCh <- qp.acquire(ctx, 1)
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

	qp.add(1)
	go func() {
		errCh <- qp.acquire(context.Background(), 1)
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

	qp := newQuotaPool(1)
	ctx := context.Background()
	if err := qp.acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error)
	go func() {
		errCh <- qp.acquire(ctx, 1)
	}()

	qp.add(0)
	qp.add(1)

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("context cancellations did not unblock acquisitions within 5s")
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	}

	qp.add(0)
	qp.add(1)

	if err := qp.acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if err := qp.acquire(ctx, 0); err != nil {
		t.Fatal(err)
	}
}

// TestQuotaPoolMaxQuota tests that at no point does the total quota available
// in the pool exceed the maximum amount the pool was initialized with.
func TestQuotaPoolMaxQuota(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const quota = 100
	const numGoroutines = 200
	qp := newQuotaPool(quota)
	ctx := context.Background()
	resCh := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			if err := qp.acquire(ctx, 1); err != nil {
				resCh <- err
				return
			}
			qp.add(2)
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
	if q := qp.approximateQuota(); q != quota {
		t.Fatalf("expected quota: %d, got: %d", quota, q)
	}
}

// TestQuotaPoolCappedAcquisition verifies that when an acquisition request
// greater than the maximum quota is placed, we still allow the acquisition to
// proceed but after having acquired the maximum quota amount.
func TestQuotaPoolCappedAcquisition(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const quota = 1
	qp := newQuotaPool(quota)
	if err := qp.acquire(context.Background(), quota*100); err != nil {
		t.Fatal(err)
	}

	if q := qp.approximateQuota(); q != 0 {
		t.Fatalf("expected quota: %d, got: %d", 0, q)
	}

	qp.add(quota)

	if q := qp.approximateQuota(); q != quota {
		t.Fatalf("expected quota: %d, got: %d", quota, q)
	}
}

// BenchmarkQuotaPool benchmarks the common case where we have sufficient
// quota available in the pool and we repeatedly acquire and release quota.
func BenchmarkQuotaPool(b *testing.B) {
	qp := newQuotaPool(1)
	ctx := context.Background()
	for n := 0; n < b.N; n++ {
		if err := qp.acquire(ctx, 1); err != nil {
			b.Fatal(err)
		}
		qp.add(1)
	}
	qp.close()
}
