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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/codahale/hdrhistogram"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"
)

const (
	minLatency = 100 * time.Microsecond
	maxLatency = 10 * time.Second
)

var numOps uint64
var numBytes uint64

// IOBenchmarkType represents an I/O benchmark.
type IOBenchmarkType int

const (
	// SeqWriteTest identifies a sequential write I/O
	// benchmark.
	SeqWriteTest IOBenchmarkType = iota
)

// IOOptions holds parameters for the test.
type IOOptions struct {
	Dir          string
	Concurrency  int
	Duration     time.Duration
	WriteSize    int64
	SyncInterval int64

	IOBenchMark IOBenchmarkType
}

// latency is a histogram for the latency of a single worker.
type latency struct {
	syncutil.Mutex
	*hdrhistogram.WindowedHistogram
}

// worker represents a single worker process generating load.
type worker interface {
	getLatencyHistogram() *latency
	run(ctx context.Context) error
}

// workerSeqWrite holds a temp file and random byte array to write to
// the temp file.
type workerSeqWrite struct {
	latency  latency
	data     []byte
	tempFile *os.File

	syncInterval int64
}

// newWorkerSeqWrite creates a worker that writes writeSize sized
// sequential blocks with fsync every syncInterval bytes to a file.
func newWorkerSeqWrite(ctx context.Context, file *os.File, iOOptions *IOOptions) worker {
	data := make([]byte, iOOptions.WriteSize)
	_, err := rand.Read(data)
	if err != nil {
		log.Fatalf(ctx, "Failed to fill byte array with random bytes %s", err)
	}

	w := &workerSeqWrite{data: data,
		tempFile:     file,
		syncInterval: iOOptions.SyncInterval,
	}
	w.latency.WindowedHistogram = hdrhistogram.NewWindowed(1,
		minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)
	return w
}

// workerSeqWrite implements the worker interface.
func (w *workerSeqWrite) run(ctx context.Context) error {
	written := int64(0)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		start := timeutil.Now()
		if _, err := w.tempFile.Write(w.data); err != nil {
			w.tempFile.Close()
			return err
		}

		written += int64(len(w.data))
		if written >= w.syncInterval {
			w.tempFile.Sync()
			written = int64(0)
		}

		bytes := uint64(len(w.data))
		atomic.AddUint64(&numOps, 1)
		atomic.AddUint64(&numBytes, bytes)
		elapsed := timeutil.Since(start)
		w.latency.Lock()
		if err := w.latency.Current.RecordValue(elapsed.Nanoseconds()); err != nil {
			return err
		}
		w.latency.Unlock()
	}
	return nil
}

// workerSeqWrite implements the worker interface.
func (w *workerSeqWrite) getLatencyHistogram() *latency {
	return &w.latency
}

// Run runs I/O benchmarks specified by hddOpts.
func Run(ioOpts IOOptions) error {
	ctx := context.Background()

	// Check if the directory exists.
	_, err := os.Stat(ioOpts.Dir)
	if err != nil {
		return errors.Errorf("error: supplied path '%s' must exist", ioOpts.Dir)
	}

	log.Infof(ctx, "writing to %s\n", ioOpts.Dir)

	workers := make([]worker, ioOpts.Concurrency)
	// We're creating a temp file like this instead of using ioutil.TempFile
	// to open in append mode.
	tempName := strconv.Itoa(rand.Int())
	tmpfile, err := os.OpenFile(filepath.Join(ioOpts.Dir, tempName),
		os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		return err
	}
	defer os.Remove(tmpfile.Name()) // clean up

	var workerCreator func(ctx context.Context, file *os.File, iOOptions *IOOptions) worker

	switch ioOpts.IOBenchMark {
	case SeqWriteTest:
		workerCreator = newWorkerSeqWrite
	default:
		return errors.Errorf("Please specify a valid subtest.")
	}

	g, ctx := errgroup.WithContext(ctx)

	var cancel func()
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	for i := range workers {
		workers[i] = workerCreator(ctx, tmpfile, &ioOpts)
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

	if ioOpts.Duration > 0 {
		go func() {
			time.Sleep(ioOpts.Duration)
			done <- sysutil.Signal(0)
		}()
	}

	start := timeutil.Now()
	lastNow := start
	var lastOps uint64
	var lastBytes uint64

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
			bytes := atomic.LoadUint64(&numBytes)

			if i%20 == 0 {
				fmt.Println("_elapsed____ops/sec___mb/sec__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			}
			fmt.Printf("%8s %10.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n",
				time.Duration(timeutil.Since(start).Seconds()+0.5)*time.Second,
				float64(ops-lastOps)/elapsed.Seconds(),
				float64(bytes-lastBytes)/(1024.0*1024.0)/elapsed.Seconds(),
				time.Duration(p50).Seconds()*1000,
				time.Duration(p95).Seconds()*1000,
				time.Duration(p99).Seconds()*1000,
				time.Duration(pMax).Seconds()*1000,
			)
			lastNow = now
			lastOps = ops
			lastBytes = bytes

		case <-done:
			cancel()
			startElapsed := timeutil.Since(start)
			const totalHeader = "\n_elapsed____ops(total)__mb(total)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)"
			fmt.Println(totalHeader + `__total`)
			fmt.Printf("%8s %13d %10.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n",
				time.Duration(startElapsed.Seconds())*time.Second,
				atomic.LoadUint64(&numOps),
				float64(atomic.LoadUint64(&numBytes)/(1024.0*1024.0)),
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
