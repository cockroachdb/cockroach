// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package syncbench

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/codahale/hdrhistogram"
)

var numOps uint64
var numBytes uint64

const (
	minLatency = 100 * time.Microsecond
	maxLatency = 10 * time.Second
)

func clampLatency(d, min, max time.Duration) time.Duration {
	if d < min {
		return min
	}
	if d > max {
		return max
	}
	return d
}

type worker struct {
	db      storage.Engine
	latency struct {
		syncutil.Mutex
		*hdrhistogram.WindowedHistogram
	}
	logOnly bool
}

func newWorker(db storage.Engine) *worker {
	w := &worker{db: db}
	w.latency.WindowedHistogram = hdrhistogram.NewWindowed(1,
		minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)
	return w
}

func (w *worker) run(wg *sync.WaitGroup) {
	defer wg.Done()

	ctx := context.Background()
	rand := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	var buf []byte

	randBlock := func(min, max int) []byte {
		data := make([]byte, rand.Intn(max-min)+min)
		for i := range data {
			data[i] = byte(rand.Int() & 0xff)
		}
		return data
	}

	for {
		start := timeutil.Now()
		b := w.db.NewBatch()
		if w.logOnly {
			block := randBlock(300, 400)
			if err := b.LogData(block); err != nil {
				log.Fatalf(ctx, "%v", err)
			}
		} else {
			for j := 0; j < 5; j++ {
				block := randBlock(60, 80)
				key := encoding.EncodeUint32Ascending(buf, rand.Uint32())
				if err := b.PutUnversioned(key, block); err != nil {
					log.Fatalf(ctx, "%v", err)
				}
				buf = key[:0]
			}
		}
		bytes := uint64(b.Len())
		if err := b.Commit(true); err != nil {
			log.Fatalf(ctx, "%v", err)
		}
		atomic.AddUint64(&numOps, 1)
		atomic.AddUint64(&numBytes, bytes)
		elapsed := clampLatency(timeutil.Since(start), minLatency, maxLatency)
		w.latency.Lock()
		if err := w.latency.Current.RecordValue(elapsed.Nanoseconds()); err != nil {
			log.Fatalf(ctx, "%v", err)
		}
		w.latency.Unlock()
	}
}

// Options holds parameters for the test.
type Options struct {
	Dir         string
	Concurrency int
	Duration    time.Duration
	LogOnly     bool
}

// Run a test of writing synchronously to the RocksDB WAL.
//
// TODO(tschottdorf): this should receive a RocksDB instance so that the caller
// in cli can use OpenEngine (which in turn allows to use encryption, etc).
func Run(opts Options) error {
	// Check if the directory exists.
	_, err := os.Stat(opts.Dir)
	if err == nil {
		return errors.Errorf("error: supplied path '%s' must not exist", opts.Dir)
	}

	defer func() {
		_ = os.RemoveAll(opts.Dir)
	}()

	fmt.Printf("writing to %s\n", opts.Dir)

	db, err := storage.NewDefaultEngine(
		0,
		base.StorageConfig{
			Settings: cluster.MakeTestingClusterSettings(),
			Dir:      opts.Dir,
		})
	if err != nil {
		return err
	}

	workers := make([]*worker, opts.Concurrency)

	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		workers[i] = newWorker(db)
		workers[i].logOnly = opts.LogOnly
		go workers[i].run(&wg)
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	done := make(chan os.Signal, 3)
	signal.Notify(done, os.Interrupt)

	go func() {
		wg.Wait()
		done <- sysutil.Signal(0)
	}()

	if opts.Duration > 0 {
		go func() {
			time.Sleep(opts.Duration)
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
			var h *hdrhistogram.Histogram
			for _, w := range workers {
				w.latency.Lock()
				m := w.latency.Merge()
				w.latency.Rotate()
				w.latency.Unlock()
				if h == nil {
					h = m
				} else {
					h.Merge(m)
				}
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
			return nil
		}
	}
}
