// Copyright 2018 The Cockroach Authors.
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

package systembench

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/codahale/hdrhistogram"
	"github.com/dustin/go-humanize"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"math"
	"os"
	"os/signal"
	"sync/atomic"
	"time"
)

// CPUBenchmarkType represents a CPU Benchmark.
type CPUBenchmarkType int

const (
	// CPUPrimeTest identifies a prime factoring CPU test.
	CPUPrimeTest CPUBenchmarkType = iota
)

// CPUOptions holds parameters for the test.
type CPUOptions struct {
	Concurrency int
	Duration    time.Duration

	Type CPUBenchmarkType
}

// workerCPUPrimes holds a latency histogram.
type workerCPUPrimes struct {
	latency latency
}

// workerCPUPrimes implements the worker interface.
func (w *workerCPUPrimes) run(ctx context.Context) error {
	dividend := 1
	for {
		limit := math.Sqrt(float64(dividend))
		divisor := 1
		for float64(divisor) <= limit {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			start := timeutil.Now()
			remainder := dividend % divisor
			atomic.AddUint64(&numOps, 1)
			elapsed := timeutil.Since(start)
			w.latency.Lock()
			if err := w.latency.Current.RecordValue(elapsed.Nanoseconds()); err != nil {
				return err
			}
			w.latency.Unlock()
			if remainder == 0 {
				dividend++
				break
			}
			divisor++
		}
	}
}

// workerCPUPrimes implements the worker interface.
func (w *workerCPUPrimes) getLatencyHistogram() *latency {
	return &w.latency
}

// newWorkerCPUPrimes creates a worker that will verify prime numbers by doing standard
// division of the number by all numbers between 2 and the square root of the number.
// If any number gives a remainder of 0, the next number is calculated.
func newWorkerCPUPrimes(ctx context.Context, cpuOptions *CPUOptions) worker {

	w := &workerCPUPrimes{}
	w.latency.WindowedHistogram = hdrhistogram.NewWindowed(1,
		minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)
	return w
}

// RunCPU runs cpu benchmarks specified by cpuOptions.
func RunCPU(cpuOptions CPUOptions) error {
	ctx := context.Background()

	workers := make([]worker, cpuOptions.Concurrency)
	var workerCreator func(ctx context.Context, cpuOptions *CPUOptions) worker

	switch cpuOptions.Type {
	case CPUPrimeTest:
		workerCreator = newWorkerCPUPrimes
	default:
		return errors.Errorf("Please specify a valid subtest.")
	}

	g, ctx := errgroup.WithContext(ctx)

	var cancel func()
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	for i := range workers {
		workers[i] = workerCreator(ctx, &cpuOptions)
	}

	for i := range workers {
		g.Go(func() error {
			return workers[i].run(ctx)
		})
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	errs := make(chan error, 1)
	done := make(chan os.Signal, 3)
	signal.Notify(done, os.Interrupt)

	go func() {
		if err := g.Wait(); err != nil {
			errs <- err
		} else {
			done <- sysutil.Signal(0)
		}
	}()

	if cpuOptions.Duration > 0 {
		go func() {
			time.Sleep(cpuOptions.Duration)
			done <- sysutil.Signal(0)
		}()
	}

	start := timeutil.Now()
	lastNow := start
	var lastOps uint64

	// This is a histogram that combines the output for multiple workers.
	var cumulative *hdrhistogram.Histogram

	for i := 0; ; i++ {
		select {
		case <-ticker.C:
			var h *hdrhistogram.Histogram
			for _, w := range workers {
				workerLatency := w.getLatencyHistogram()
				workerLatency.Lock()
				m := workerLatency.Merge()
				workerLatency.Rotate()
				workerLatency.Unlock()
				if h == nil {
					h = m
				} else {
					h.Merge(m)
				}
			}

			if cumulative == nil {
				cumulative = h
			} else {
				cumulative.Merge(h)
			}

			p50 := h.ValueAtQuantile(50)
			p95 := h.ValueAtQuantile(95)
			p99 := h.ValueAtQuantile(99)
			pMax := h.ValueAtQuantile(100)

			now := timeutil.Now()
			elapsed := now.Sub(lastNow)
			ops := atomic.LoadUint64(&numOps)

			if i%20 == 0 {
				fmt.Println("_elapsed____ops/sec__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			}
			fmt.Printf("%8s %10s %8.1f %8.1f %8.1f %8.1f\n",
				time.Duration(timeutil.Since(start).Seconds()+0.5)*time.Second,
				humanize.Comma(int64(float64(ops-lastOps)/elapsed.Seconds())),
				time.Duration(p50).Seconds()*1000,
				time.Duration(p95).Seconds()*1000,
				time.Duration(p99).Seconds()*1000,
				time.Duration(pMax).Seconds()*1000,
			)
			lastNow = now
			lastOps = ops

		case <-done:
			cancel()
			startElapsed := timeutil.Since(start)
			const totalHeader = "\n_elapsed____ops(total)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)"
			fmt.Println(totalHeader + `__total`)
			fmt.Printf("%8s %13s %8.1f %8.1f %8.1f %8.1f %8.1f\n",
				time.Duration(startElapsed.Seconds())*time.Second,
				humanize.Comma(int64(atomic.LoadUint64(&numOps))),
				time.Duration(cumulative.Mean()).Seconds()*1000,
				time.Duration(cumulative.ValueAtQuantile(50)).Seconds()*1000,
				time.Duration(cumulative.ValueAtQuantile(95)).Seconds()*1000,
				time.Duration(cumulative.ValueAtQuantile(99)).Seconds()*1000,
				time.Duration(cumulative.ValueAtQuantile(100)).Seconds()*1000,
			)
			return nil
		case err := <-errs:
			return err
		}
	}
}
