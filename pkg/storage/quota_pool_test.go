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
//
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)

package storage

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"golang.org/x/net/context"
)

func TestQuotaPoolBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	quotas := []int64{1, 10, 100, 1000}
	threadCounts := []int{1, 10, 100}

	for _, quota := range quotas {
		for _, threadCount := range threadCounts {
			qp := newQuotaPool(quota)
			ctx := context.Background()
			errCh := make(chan error, threadCount)
			var wg sync.WaitGroup
			wg.Add(threadCount)

			for i := 0; i < threadCount; i++ {
				go func() {
					defer wg.Done()
					if err := qp.acquire(ctx, 1); err != nil {
						errCh <- err
					}
					qp.add(1)
				}()
			}

			waitCh := make(chan struct{})
			go func() {
				wg.Wait()
				close(waitCh)
			}()

			select {
			case <-time.After(5 * time.Second):
				t.Fatal("did not complete acquisitions within 5s")
			case <-waitCh:
			}

			select {
			case err := <-errCh:
				t.Fatal(err)
			default:
			}
			if q := qp.quota(); q != quota {
				t.Fatalf("expected quota: %d, got: %d", quota, q)
			}
		}
	}
}

func TestQuotaPoolContextCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	qp := newQuotaPool(1)
	if err := qp.acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := qp.acquire(ctx, 1); err == nil {
			errCh <- errors.New("expected acquisition to fail with ctx cancellation")
		}
	}()

	cancel()
	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)
	}()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("context cancellation did not unblock acquisitions within 5s")
	case <-waitCh:
	}

	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}

	qp.add(1)

	if q := qp.quota(); q != 1 {
		t.Fatalf("expected quota: 1, got: %d", q)
	}
}

func TestQuotaPoolClose(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	qp := newQuotaPool(1)
	if err := qp.acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := qp.acquire(ctx, 1); err == nil {
			errCh <- errors.New("expected acquisition to fail with pool closing")
		}
	}()

	qp.close()

	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)
	}()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("quota pool closing did not unblock acquisitions within 5s")
	case <-waitCh:
	}

	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
}

func TestQuotaPoolCancelledAcquisitions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	qp := newQuotaPool(1)
	if err := qp.acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}

	cancel()

	var wg sync.WaitGroup
	wg.Add(5)
	errCh := make(chan error, 5)
	for i := 0; i < 5; i++ {
		go func() {
			defer wg.Done()
			if err := qp.acquire(ctx, 1); err == nil {
				errCh <- errors.New("expected acquisition to fail with ctx cancellation")
			}
		}()
	}

	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)

	}()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("context cancellations did not unblock acquisitions within 5s")
	case <-waitCh:
	}

	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
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

func TestQuotaPoolNoops(t *testing.T) {
	defer leaktest.AfterTest(t)()

	qp := newQuotaPool(1)
	ctx := context.Background()
	if err := qp.acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := qp.acquire(ctx, 1); err != nil {
			errCh <- err
		}
	}()

	qp.add(0)
	qp.add(1)

	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)

	}()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("context cancellations did not unblock acquisitions within 5s")
	case <-waitCh:
	}

	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
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

func TestQuotaPoolMaxQuota(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const quota = 100
	const threadCount = 100
	qp := newQuotaPool(quota)
	ctx := context.Background()
	errCh := make(chan error, threadCount)
	var wg sync.WaitGroup
	wg.Add(threadCount)

	for i := 0; i < threadCount; i++ {
		go func() {
			defer wg.Done()
			if err := qp.acquire(ctx, 1); err != nil {
				errCh <- err
			}
			qp.add(2)
		}()
	}

	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)
	}()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("did not complete acquisitions within 5s")
	case <-waitCh:
	}

	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
	if q := qp.quota(); q != quota {
		t.Fatalf("expected quota: %d, got: %d", quota, q)
	}
}

func TestQuotaPoolCappedAcquisition(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const quota = 1
	qp := newQuotaPool(quota)
	if err := qp.acquire(context.Background(), quota*100); err != nil {
		t.Fatal(err)
	}
	qp.add(quota)

	if q := qp.quota(); q != quota {
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
