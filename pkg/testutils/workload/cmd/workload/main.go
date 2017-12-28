// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"context"
	gosql "database/sql"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"github.com/codahale/hdrhistogram"
	"github.com/tylertreat/hdrhistogram-writer"

	"github.com/cockroachdb/cockroach/pkg/testutils/workload"
	_ "github.com/cockroachdb/cockroach/pkg/testutils/workload/kv"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var generator = flag.String("generator", "", "")
var optsRaw = flag.String("opts", "", "")

// concurrency = number of concurrent insertion processes.
var concurrency = flag.Int("concurrency", 2*runtime.NumCPU(), "Number of concurrent writers inserting blocks")

// batch = number of blocks to insert in a single SQL statement.
var batch = flag.Int("batch", 1, "Number of blocks to insert in a single SQL statement")

var tolerateErrors = flag.Bool("tolerate-errors", false, "Keep running on error")

var maxRate = flag.Float64("max-rate", 0, "Maximum frequency of operations (reads/writes). If 0, no limit.")

var maxOps = flag.Uint64("max-ops", 0, "Maximum number of blocks to read/write")
var duration = flag.Duration("duration", 0, "The duration to run. If 0, run forever.")
var drop = flag.Bool("drop", false, "Clear the existing data before starting.")

// Output in HdrHistogram Plotter format. See https://hdrhistogram.github.io/HdrHistogram/plotFiles.html
var histFile = flag.String("hist-file", "", "Write histogram data to file for HdrHistogram Plotter, or stdout if - is specified.")

// numOps keeps a global count of successful operations.
var numOps uint64

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

type blocker struct {
	db      *gosql.DB
	op      func(context.Context) error
	latency struct {
		syncutil.Mutex
		*hdrhistogram.WindowedHistogram
	}
}

func newBlocker(db *gosql.DB, op func(context.Context) error) *blocker {
	b := &blocker{
		db: db,
		op: op,
	}
	b.latency.WindowedHistogram = hdrhistogram.NewWindowed(1,
		minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)
	return b
}

// run is an infinite loop in which the blocker continuously attempts to
// read / write blocks of random data into a table in cockroach DB.
func (b *blocker) run(
	ctx context.Context, errCh chan<- error, wg *sync.WaitGroup, limiter *rate.Limiter,
) {
	defer wg.Done()

	for {
		// Limit how quickly the load generator sends requests based on --max-rate.
		if limiter != nil {
			if err := limiter.Wait(ctx); err != nil {
				panic(err)
			}
		}

		start := timeutil.Now()
		if err := b.op(ctx); err != nil {
			errCh <- err
			continue
		}
		elapsed := clampLatency(timeutil.Since(start), minLatency, maxLatency)
		b.latency.Lock()
		if err := b.latency.Current.RecordValue(elapsed.Nanoseconds()); err != nil {
			log.Fatal(ctx, err)
		}
		b.latency.Unlock()
		v := atomic.AddUint64(&numOps, uint64(*batch))
		if *maxOps > 0 && v >= *maxOps {
			return
		}
	}
}

func setupCockroach(dbURLs []string) (*gosql.DB, error) {
	// Open connection to server and create a database.
	db, dbErr := gosql.Open("cockroach", strings.Join(dbURLs, " "))
	if dbErr != nil {
		return nil, dbErr
	}

	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(*concurrency + 1)
	db.SetMaxIdleConns(*concurrency + 1)

	if *drop {
		if _, err := db.Exec(`DROP DATABASE IF EXISTS test`); err != nil {
			return nil, err
		}
	}
	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		return nil, err
	}
	if _, err := db.Exec("USE test"); err != nil {
		return nil, err
	}

	return db, nil
}

// setupDatabase performs initial setup for the example, creating a database and
// with a single table. If the desired table already exists on the cluster, the
// existing table will be dropped.
func setupDatabase(dbURLs []string) (*gosql.DB, error) {
	parsedURL, err := url.Parse(dbURLs[0])
	if err != nil {
		return nil, err
	}
	parsedURL.Path = "test"

	switch parsedURL.Scheme {
	case "postgres", "postgresql":
		return setupCockroach(dbURLs)
	default:
		return nil, fmt.Errorf("unsupported database: %s", parsedURL.Scheme)
	}
}

var usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s [<db URL> ...]\n\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	ctx := context.Background()
	flag.Usage = usage
	flag.Parse()

	genFn, err := workload.Get(*generator)
	if err != nil {
		log.Fatal(ctx, err)
	}
	opts := map[string]string{
		// TODO(dan): Once the workload runner gets moved into generator, then
		// batch should be just another option.
		`batch`: strconv.Itoa(*batch),
	}
	for _, opt := range strings.Split(*optsRaw, `,`) {
		if len(opt) == 0 {
			continue
		}
		optParts := strings.SplitN(opt, `=`, 2)
		if len(optParts) != 2 {
			log.Fatalf(ctx, `invalid opt: %s`, opt)
		}
		opts[optParts[0]] = opts[optParts[1]]
	}

	gen, err := genFn(opts)
	if err != nil {
		log.Fatal(ctx, err)
	}

	dbURLs := []string{"postgres://root@localhost:26257/test?sslmode=disable"}
	if args := flag.Args(); len(args) >= 1 {
		dbURLs = args
	}

	if *concurrency < 1 {
		log.Fatalf(ctx, "Value of 'concurrency' flag (%d) must be greater than or equal to 1", *concurrency)
	}

	var db *gosql.DB
	{
		var err error
		for err == nil || *tolerateErrors {
			db, err = setupDatabase(dbURLs)
			if err == nil {
				break
			}
			if !*tolerateErrors {
				log.Fatal(ctx, err)
			}
		}
	}
	const batchSize = -1
	if _, err := workload.Setup(db, gen.Tables(), batchSize); err != nil {
		log.Fatalf(ctx, "loading: %+v", err)
	}

	var limiter *rate.Limiter
	if *maxRate > 0 {
		// Create a limiter using maxRate specified on the command line and
		// with allowed burst of 1 at the maximum allowed rate.
		limiter = rate.NewLimiter(rate.Limit(*maxRate), 1)
	}

	ops := gen.Ops()
	if len(ops) != 1 {
		log.Fatal(ctx, `generators with more than one operation are not yet supported`)
	}
	op := ops[0]

	lastNow := timeutil.Now()
	start := lastNow
	var lastOps uint64
	writers := make([]*blocker, *concurrency)

	errCh := make(chan error)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		opFn, err := op.Fn(db)
		if err != nil {
			log.Fatal(ctx, err)
		}
		writers[i] = newBlocker(db, opFn)
		go writers[i].run(ctx, errCh, &wg, limiter)
	}

	var numErr int
	tick := time.Tick(time.Second)
	done := make(chan os.Signal, 3)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		wg.Wait()
		done <- syscall.Signal(0)
	}()

	if *duration > 0 {
		go func() {
			time.Sleep(*duration)
			done <- syscall.Signal(0)
		}()
	}

	defer func() {
		// Output results that mimic Go's built-in benchmark format.

		benchmarkName := strings.Join([]string{
			"BenchmarkWorkload",
			fmt.Sprintf("generator=%s", gen.Name()),
			fmt.Sprintf("concurrency=%d", *concurrency),
			fmt.Sprintf("duration=%s", *duration),
		}, "/")
		for _, opt := range strings.Split(*optsRaw, `,`) {
			if len(opt) == 0 {
				continue
			}
			benchmarkName += `/` + opt
		}

		result := testing.BenchmarkResult{
			N: int(numOps),
			T: timeutil.Since(start),
		}
		fmt.Printf("%s\t%s\n", benchmarkName, result)
	}()

	cumLatency := hdrhistogram.New(minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)

	for i := 0; ; {
		select {
		case err := <-errCh:
			numErr++
			if !*tolerateErrors {
				log.Fatal(ctx, err)
			} else {
				log.Error(ctx, err)
			}
			continue

		case <-tick:
			var h *hdrhistogram.Histogram
			for _, w := range writers {
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

			cumLatency.Merge(h)
			p50 := h.ValueAtQuantile(50)
			p95 := h.ValueAtQuantile(95)
			p99 := h.ValueAtQuantile(99)
			pMax := h.ValueAtQuantile(100)

			now := timeutil.Now()
			elapsed := now.Sub(lastNow)
			ops := atomic.LoadUint64(&numOps)
			if i%20 == 0 {
				fmt.Println("_elapsed___errors__ops/sec(inst)___ops/sec(cum)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			}
			i++
			fmt.Printf("%8s %8d %14.1f %14.1f %8.1f %8.1f %8.1f %8.1f\n",
				time.Duration(timeutil.Since(start).Seconds()+0.5)*time.Second,
				numErr,
				float64(ops-lastOps)/elapsed.Seconds(),
				float64(ops)/timeutil.Since(start).Seconds(),
				time.Duration(p50).Seconds()*1000,
				time.Duration(p95).Seconds()*1000,
				time.Duration(p99).Seconds()*1000,
				time.Duration(pMax).Seconds()*1000)
			lastOps = ops
			lastNow = now

		case <-done:
			for _, w := range writers {
				w.latency.Lock()
				m := w.latency.Merge()
				w.latency.Rotate()
				w.latency.Unlock()
				cumLatency.Merge(m)
			}

			avg := cumLatency.Mean()
			p50 := cumLatency.ValueAtQuantile(50)
			p95 := cumLatency.ValueAtQuantile(95)
			p99 := cumLatency.ValueAtQuantile(99)
			pMax := cumLatency.ValueAtQuantile(100)

			ops := atomic.LoadUint64(&numOps)
			elapsed := timeutil.Since(start).Seconds()
			fmt.Println("\n_elapsed___errors_____ops(total)___ops/sec(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			fmt.Printf("%7.1fs %8d %14d %14.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n\n",
				timeutil.Since(start).Seconds(), numErr,
				ops, float64(ops)/elapsed,
				time.Duration(avg).Seconds()*1000,
				time.Duration(p50).Seconds()*1000,
				time.Duration(p95).Seconds()*1000,
				time.Duration(p99).Seconds()*1000,
				time.Duration(pMax).Seconds()*1000)
			if *histFile == "-" {
				if err := histwriter.WriteDistribution(cumLatency, nil, 1, os.Stdout); err != nil {
					fmt.Printf("failed to write histogram to stdout: %v\n", err)
				}
			} else if *histFile != "" {
				if err := histwriter.WriteDistributionFile(cumLatency, nil, 1, *histFile); err != nil {
					fmt.Printf("failed to write histogram file: %v\n", err)
				}
			}
			return
		}
	}
}
