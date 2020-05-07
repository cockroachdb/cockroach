// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package systembench

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/signal"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

const (
	maxFileSize = 1 << 30 // A gigabyte
)

// DiskBenchmarkType represents an I/O benchmark.
type DiskBenchmarkType int

const (
	// SeqWriteTest identifies a sequential write I/O
	// benchmark.
	SeqWriteTest DiskBenchmarkType = iota
)

// DiskOptions holds parameters for the test.
type DiskOptions struct {
	Dir          string
	Concurrency  int
	Duration     time.Duration
	WriteSize    int64
	SyncInterval int64

	Type DiskBenchmarkType
}

// workerSeqWrite holds a temp file and random byte array to write to
// the temp file.
type workerSeqWrite struct {
	latency *namedHistogram
	data    []byte

	tempFileDir  string
	syncInterval int64
}

// newWorkerSeqWrite creates a worker that writes writeSize sized
// sequential blocks with fsync every syncInterval bytes to a file.
func newWorkerSeqWrite(
	ctx context.Context, diskOptions *DiskOptions, registry *histogramRegistry,
) worker {
	data := make([]byte, diskOptions.WriteSize)
	_, err := rand.Read(data)
	if err != nil {
		log.Fatalf(ctx, "Failed to fill byte array with random bytes %s", err)
	}

	w := &workerSeqWrite{data: data,
		tempFileDir:  diskOptions.Dir,
		syncInterval: diskOptions.SyncInterval,
	}
	w.latency = registry.Register("ops")
	return w
}

// workerSeqWrite implements the worker interface.
func (w *workerSeqWrite) run(ctx context.Context) error {
	writtenPreSync := int64(0)
	writtenInFile := int64(0)

	tempFile, err := newTempFile(w.tempFileDir)
	if err != nil {
		return err
	}

	writeSize := int64(len(w.data))
	defer func() {
		_ = os.Remove(tempFile.Name())
	}()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		start := timeutil.Now()
		if _, err := tempFile.Write(w.data); err != nil {
			tempFile.Close()
			return err
		}

		writtenPreSync += writeSize
		writtenInFile += writeSize

		if writtenPreSync >= w.syncInterval {
			if err := tempFile.Sync(); err != nil {
				return err
			}
			writtenPreSync = int64(0)
		}

		if writtenInFile >= maxFileSize {
			if err := tempFile.Truncate(0); err != nil {
				return err
			}

			if _, err := tempFile.Seek(0, 0); err != nil {
				return err
			}
		}

		bytes := uint64(writeSize)
		atomic.AddUint64(&numOps, 1)
		atomic.AddUint64(&numBytes, bytes)
		w.latency.Record(timeutil.Since(start))
	}
}

// workerSeqWrite implements the worker interface.
func (w *workerSeqWrite) getLatencyHistogram() *namedHistogram {
	return w.latency
}

// newTempFile creates a temporary file, closes it and
// reopens in append mode.
func newTempFile(dir string) (*os.File, error) {
	tempFile, err := ioutil.TempFile(dir, "")
	if err != nil {
		return nil, err
	}

	tempFileName := tempFile.Name()
	if err := tempFile.Close(); err != nil {
		return nil, err
	}

	return os.OpenFile(tempFileName,
		os.O_RDWR|os.O_APPEND, 0640)
}

// Run runs I/O benchmarks specified by diskOpts.
func Run(diskOpts DiskOptions) error {
	ctx := context.Background()
	reg := newHistogramRegistry()

	// Check if the directory exists.
	_, err := os.Stat(diskOpts.Dir)
	if err != nil {
		return errors.Errorf("error: supplied path '%s' must exist", diskOpts.Dir)
	}

	log.Infof(ctx, "writing to %s\n", diskOpts.Dir)

	workers := make([]worker, diskOpts.Concurrency)
	var workerCreator func(ctx context.Context, diskOptions *DiskOptions, registry *histogramRegistry) worker

	switch diskOpts.Type {
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
		workers[i] = workerCreator(ctx, &diskOpts, reg)
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

	if diskOpts.Duration > 0 {
		go func() {
			time.Sleep(diskOpts.Duration)
			done <- sysutil.Signal(0)
		}()
	}

	start := timeutil.Now()
	lastNow := start
	var lastOps uint64
	var lastBytes uint64

	for i := 0; ; i++ {
		select {
		case <-ticker.C:
			now := timeutil.Now()
			elapsed := now.Sub(lastNow)
			ops := atomic.LoadUint64(&numOps)
			bytes := atomic.LoadUint64(&numBytes)

			if i%20 == 0 {
				fmt.Println("_elapsed____ops/sec___mb/sec__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			}
			reg.Tick(func(tick histogramTick) {
				h := tick.Hist
				fmt.Printf("%8s %10.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n",
					time.Duration(timeutil.Since(start).Seconds()+0.5)*time.Second,
					float64(ops-lastOps)/elapsed.Seconds(),
					float64(bytes-lastBytes)/(1024.0*1024.0)/elapsed.Seconds(),
					time.Duration(h.ValueAtQuantile(50)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(95)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(99)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(100)).Seconds()*1000,
				)
			})
			lastNow = now
			lastOps = ops
			lastBytes = bytes

		case <-done:
			cancel()
			startElapsed := timeutil.Since(start)
			const totalHeader = "\n_elapsed____ops(total)__mb(total)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)"
			fmt.Println(totalHeader + `__total`)
			reg.Tick(func(tick histogramTick) {
				h := tick.Cumulative
				fmt.Printf("%8s %13d %10.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n",
					time.Duration(startElapsed.Seconds())*time.Second,
					atomic.LoadUint64(&numOps),
					float64(atomic.LoadUint64(&numBytes)/(1024.0*1024.0)),
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
