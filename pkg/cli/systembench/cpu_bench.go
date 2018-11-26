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
	// This is to make sure we're measuring the divisions
	// vs the performance of locking.
	batchSize := 700
	for {
		start := timeutil.Now()
		count := uint64(0)
		for i := 0; i < batchSize; i++ {
			limit := math.Sqrt(float64(dividend))
			divisor := 1
			for float64(divisor) <= limit {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				remainder := dividend % divisor
				count++

				if remainder == 0 {
					dividend++
					break
				}
				divisor++
			}
		}
		elapsed := timeutil.Since(start)
		atomic.AddUint64(&numOps, count)
		w.latency.Lock()
		w.latency.namedHistogram.Record(elapsed)
		w.latency.Unlock()
	}
}

// workerCPUPrimes implements the worker interface.
func (w *workerCPUPrimes) getLatencyHistogram() *latency {
	return &w.latency
}

// newWorkerCPUPrimes creates a worker that will verify prime numbers by doing standard
// division of the number by all numbers between 2 and the square root of the number.
// If any number gives a remainder of 0, the next number is calculated.
func newWorkerCPUPrimes(ctx context.Context, cpuOptions *CPUOptions, registry *histogramRegistry) worker {
	registry.Register("ops")
	w := &workerCPUPrimes{}
	w.latency.namedHistogram = registry.Register("ops")
	return w
}

// RunCPU runs cpu benchmarks specified by cpuOptions.
func RunCPU(cpuOptions CPUOptions) error {
	ctx := context.Background()
	reg := NewHistogramRegistry()

	workers := make([]worker, cpuOptions.Concurrency)
	var workerCreator func(ctx context.Context, cpuOptions *CPUOptions, registry *histogramRegistry) worker

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
		workers[i] = workerCreator(ctx, &cpuOptions, reg)
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

	for i := 0; ; i++ {
		select {
		case <-ticker.C:
			now := timeutil.Now()
			elapsed := now.Sub(lastNow)
			ops := atomic.LoadUint64(&numOps)

			if i%20 == 0 {
				fmt.Println("_elapsed____ops/sec__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			}
			reg.Tick(func(tick histogramTick) {
				h := tick.Hist
				fmt.Printf("%8s %10s %8.1f %8.1f %8.1f %8.1f\n",
					time.Duration(timeutil.Since(start).Seconds()+0.5)*time.Second,
					humanize.Comma(int64(float64(ops-lastOps)/elapsed.Seconds())),
					time.Duration(h.ValueAtQuantile(50)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(95)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(99)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(100)).Seconds()*1000,
				)
			})
			lastNow = now
			lastOps = ops

		case <-done:
			cancel()
			startElapsed := timeutil.Since(start)
			const totalHeader = "\n_elapsed____ops(total)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)"
			fmt.Println(totalHeader + `__total`)
			reg.Tick(func(tick histogramTick) {
				h := tick.Cumulative
				fmt.Printf("%8s %13s %8.1f %8.1f %8.1f %8.1f %8.1f\n",
					time.Duration(startElapsed.Seconds())*time.Second,
					humanize.Comma(int64(atomic.LoadUint64(&numOps))),
					time.Duration(h.Mean()).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(50)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(95)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(99)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(100)).Seconds()*1000)
			})
			return nil
		case err := <-errs:
			return err
		}
	}
}
