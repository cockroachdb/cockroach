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
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"github.com/codahale/hdrhistogram"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/tylertreat/hdrhistogram-writer"

	"github.com/cockroachdb/cockroach/pkg/testutils/workload"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const crdbDefaultURI = `postgres://root@localhost:26257/test?sslmode=disable`

var runCmd = &cobra.Command{
	Use:   `run`,
	Short: `Run a workload's operations against a cluster`,
}

var rootFlags = pflag.NewFlagSet(`workload`, pflag.ContinueOnError)
var concurrency = rootFlags.Int(
	"concurrency", 2*runtime.NumCPU(), "Number of concurrent writers inserting blocks")
var tolerateErrors = rootFlags.Bool("tolerate-errors", false, "Keep running on error")
var maxRate = rootFlags.Float64(
	"max-rate", 0, "Maximum frequency of operations (reads/writes). If 0, no limit.")
var maxOps = rootFlags.Uint64("max-ops", 0, "Maximum number of operations to run")
var duration = rootFlags.Duration("duration", 0, "The duration to run. If 0, run forever.")
var drop = rootFlags.Bool("drop", false, "Clear the existing data before starting.")

// Output in HdrHistogram Plotter format. See
// https://hdrhistogram.github.io/HdrHistogram/plotFiles.html
var histFile = rootFlags.String(
	"hist-file", "",
	"Write histogram data to file for HdrHistogram Plotter, or stdout if - is specified.")

func init() {
	for _, meta := range workload.Registered() {
		gen := meta.New()
		genFlags := gen.Flags()
		genHooks := gen.Hooks()

		genCmd := &cobra.Command{Use: meta.Name, Short: meta.Description}
		genCmd.Flags().AddFlagSet(rootFlags)
		genCmd.Flags().AddFlagSet(genFlags)
		genCmd.RunE = func(cmd *cobra.Command, args []string) error {
			if genHooks.Validate != nil {
				if err := genHooks.Validate(); err != nil {
					return err
				}
			}
			return runRun(gen, args)
		}

		runCmd.AddCommand(genCmd)
	}
	rootCmd.AddCommand(runCmd)
}

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

type worker struct {
	db      *gosql.DB
	op      func(context.Context) error
	latency struct {
		syncutil.Mutex
		*hdrhistogram.WindowedHistogram
	}
}

func newWorker(db *gosql.DB, op func(context.Context) error) *worker {
	w := &worker{
		db: db,
		op: op,
	}
	w.latency.WindowedHistogram = hdrhistogram.NewWindowed(1,
		minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)
	return w
}

// run is an infinite loop in which the worker continuously attempts to
// read / write blocks of random data into a table in cockroach DB.
func (w *worker) run(
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
		if err := w.op(ctx); err != nil {
			errCh <- err
			continue
		}
		elapsed := clampLatency(timeutil.Since(start), minLatency, maxLatency)
		w.latency.Lock()
		if err := w.latency.Current.RecordValue(elapsed.Nanoseconds()); err != nil {
			log.Fatal(ctx, err)
		}
		w.latency.Unlock()
		v := atomic.AddUint64(&numOps, 1)
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

func runRun(gen workload.Generator, args []string) error {
	ctx := context.Background()

	dbURLs := []string{crdbDefaultURI}
	if len(args) >= 1 {
		dbURLs = args
	}

	if *concurrency < 1 {
		return errors.Errorf(
			"Value of 'concurrency' flag (%d) must be greater than or equal to 1", *concurrency)
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
				return err
			}
		}
	}
	const batchSize = -1
	if _, err := workload.Setup(db, gen, batchSize); err != nil {
		return err
	}

	var limiter *rate.Limiter
	if *maxRate > 0 {
		// Create a limiter using maxRate specified on the command line and
		// with allowed burst of 1 at the maximum allowed rate.
		limiter = rate.NewLimiter(rate.Limit(*maxRate), 1)
	}

	ops := gen.Ops()
	if len(ops) != 1 {
		return errors.Errorf(`generators with more than one operation are not yet supported`)
	}
	op := ops[0]

	lastNow := timeutil.Now()
	start := lastNow
	var lastOps uint64
	workers := make([]*worker, *concurrency)

	errCh := make(chan error)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		opFn, err := op.Fn(db)
		if err != nil {
			return err
		}
		workers[i] = newWorker(db, opFn)
		go workers[i].run(ctx, errCh, &wg, limiter)
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
			fmt.Sprintf("generator=%s", gen.Meta().Name),
			fmt.Sprintf("concurrency=%d", *concurrency),
			fmt.Sprintf("duration=%s", *duration),
		}, "/")
		// NB: This visits in a deterministic order.
		gen.Flags().Visit(func(f *pflag.Flag) {
			benchmarkName += fmt.Sprintf(`/%s=%s`, f.Name, f.Value)
		})

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
			if *tolerateErrors {
				log.Error(ctx, err)
				continue
			}
			return err

		case <-tick:
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
			for _, w := range workers {
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
				if err := histwriter.WriteDistributionFile(
					cumLatency, nil, 1, *histFile,
				); err != nil {
					fmt.Printf("failed to write histogram file: %v\n", err)
				}
			}
			return nil
		}
	}
}
