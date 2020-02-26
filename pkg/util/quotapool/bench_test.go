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

	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"golang.org/x/sync/errgroup"
)

// BenchmarkIntQuotaPool benchmarks the common case where we have sufficient
// quota available in the pool and we repeatedly acquire and release quota.
func BenchmarkIntPool(b *testing.B) {
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

func BenchmarkChannelSemaphore(b *testing.B) {
	sem := make(chan struct{}, 1)
	ctx := context.Background()
	for n := 0; n < b.N; n++ {
		select {
		case <-ctx.Done():
		case sem <- struct{}{}:
		}
		select {
		case <-ctx.Done():
		case <-sem:
		}
	}
	close(sem)
}

// BenchmarkConcurrentIntPool benchmarks concurrent workers in a variety
// of ratios between adequate and inadequate quota to concurrently serve all
// workers with the IntPool.
func BenchmarkConcurrentIntPool(b *testing.B) {
	for _, s := range concurrentBenchSpecs {
		b.Run(s.String(), s.benchmarkIntPool)
	}
}

// BenchmarkConcurrentChannelSem benchmarks concurrent workers in a variety
// of ratios between adequate and inadequate quota to concurrently serve all
// workers with a channel-based semaphore to compare the performance against
// the IntPool.
func BenchmarkConcurrentChannelSemaphore(b *testing.B) {
	for _, s := range concurrentBenchSpecs {
		b.Run(s.String(), s.benchmarkChannelSem)
	}
}

// BenchmarkIntQuotaPoolFunc benchmarks the common case where we have sufficient
// quota available in the pool and we repeatedly acquire and release quota.
func BenchmarkIntPoolFunc(b *testing.B) {
	qp := quotapool.NewIntPool("test", 1, logSlowAcquisition)
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

type concurrentBenchSpec struct {
	workers int
	quota   uint64
}

func (s concurrentBenchSpec) benchmarkChannelSem(b *testing.B) {
	sem := make(chan struct{}, s.quota)
	g, ctx := errgroup.WithContext(context.Background())
	runWorker := func(workerNum int) {
		g.Go(func() error {
			for i := workerNum; i < b.N; i += s.workers {
				select {
				case <-ctx.Done():
				case sem <- struct{}{}:
				}
				runtime.Gosched()
				select {
				case <-ctx.Done():
				case <-sem:
				}
			}
			return nil
		})
	}
	for i := 0; i < s.workers; i++ {
		runWorker(i)
	}
	if err := g.Wait(); err != nil {
		b.Fatal(err)
	}
	close(sem)
}

func (s concurrentBenchSpec) benchmarkIntPool(b *testing.B) {
	qp := quotapool.NewIntPool("test", s.quota, logSlowAcquisition)
	g, ctx := errgroup.WithContext(context.Background())
	runWorker := func(workerNum int) {
		g.Go(func() error {
			for i := workerNum; i < b.N; i += s.workers {
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
	for i := 0; i < s.workers; i++ {
		runWorker(i)
	}
	if err := g.Wait(); err != nil {
		b.Fatal(err)
	}
	qp.Close("")
}

func (s concurrentBenchSpec) String() string {
	return fmt.Sprintf("workers=%d,quota=%d", s.workers, s.quota)
}

var concurrentBenchSpecs = []concurrentBenchSpec{
	{1, 1},
	{2, 2},
	{8, 4},
	{128, 4},
	{512, 128},
	{512, 513},
	{512, 511},
	{1024, 4},
	{1024, 4096},
}

// intRequest is a wrapper to create a IntRequestFunc from an int64.
type intRequest uint64

func (ir intRequest) acquire(_ context.Context, pi quotapool.PoolInfo) (took uint64, err error) {
	if uint64(ir) < pi.Available {
		return 0, quotapool.ErrNotEnoughQuota
	}
	return uint64(ir), nil
}
