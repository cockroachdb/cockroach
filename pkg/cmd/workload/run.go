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
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/tylertreat/hdrhistogram-writer"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
)

var runCmd = &cobra.Command{
	Use:   `run`,
	Short: `Run a workload's operations against a cluster`,
}

var runFlags = pflag.NewFlagSet(`run`, pflag.ContinueOnError)
var tolerateErrors = runFlags.Bool("tolerate-errors", false, "Keep running on error")
var maxRate = runFlags.Float64(
	"max-rate", 0, "Maximum frequency of operations (reads/writes). If 0, no limit.")
var maxOps = runFlags.Uint64("max-ops", 0, "Maximum number of operations to run")
var duration = runFlags.Duration("duration", 0, "The duration to run. If 0, run forever.")
var doInit = runFlags.Bool("init", false, "Automatically run init")
var ramp = runFlags.Duration("ramp", 0*time.Second, "The duration over which to ramp up load.")

var initCmd = &cobra.Command{
	Use:   `init`,
	Short: `Set up tables for a workload`,
}

var initFlags = pflag.NewFlagSet(`init`, pflag.ContinueOnError)
var drop = initFlags.Bool("drop", false, "Drop the existing database, if it exists")

// Output in HdrHistogram Plotter format. See
// https://hdrhistogram.github.io/HdrHistogram/plotFiles.html
var histFile = runFlags.String(
	"hist-file", "",
	"Write histogram data to file for HdrHistogram Plotter, or stdout if - is specified.")

func init() {
	for _, meta := range workload.Registered() {
		gen := meta.New()
		var genFlags *pflag.FlagSet
		if f, ok := gen.(workload.Flagser); ok {
			genFlags = f.Flags().FlagSet
		}

		genInitCmd := &cobra.Command{Use: meta.Name, Short: meta.Description}
		genInitCmd.Flags().AddFlagSet(initFlags)
		genInitCmd.Flags().AddFlagSet(genFlags)
		genInitCmd.Run = cmdHelper(gen, runInit)
		initCmd.AddCommand(genInitCmd)

		genRunCmd := &cobra.Command{Use: meta.Name, Short: meta.Description}
		genRunCmd.Flags().AddFlagSet(runFlags)
		genRunCmd.Flags().AddFlagSet(genFlags)
		initFlags.VisitAll(func(initFlag *pflag.Flag) {
			// Every init flag is a valid run flag that implies the --init option.
			f := *initFlag
			f.Usage += ` (implies --init)`
			genRunCmd.Flags().AddFlag(&f)
		})
		genRunCmd.Run = cmdHelper(gen, runRun)
		runCmd.AddCommand(genRunCmd)
	}
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(runCmd)
}

func cmdHelper(
	gen workload.Generator, fn func(gen workload.Generator, urls []string, dbName string) error,
) func(*cobra.Command, []string) {
	const crdbDefaultURL = `postgres://root@localhost:26257?sslmode=disable`

	return handleErrs(func(cmd *cobra.Command, args []string) error {
		if h, ok := gen.(workload.Hookser); ok {
			if err := h.Hooks().Validate(); err != nil {
				return err
			}
		}

		// HACK: Steal the dbOverride out of flags. This should go away
		// once more of run.go moves inside workload.
		var dbOverride string
		if dbFlag := cmd.Flag(`db`); dbFlag != nil {
			dbOverride = dbFlag.Value.String()
		}
		urls := args
		if len(urls) == 0 {
			urls = []string{crdbDefaultURL}
		}
		dbName, err := workload.SanitizeUrls(gen, dbOverride, urls)
		if err != nil {
			return err
		}
		return fn(gen, urls, dbName)
	})
}

// numOps keeps a global count of successful operations.
var numOps uint64

// workerRun is an infinite loop in which the worker continuously attempts to
// read / write blocks of random data into a table in cockroach DB.
func workerRun(
	ctx context.Context,
	errCh chan<- error,
	wg *sync.WaitGroup,
	limiter *rate.Limiter,
	workFn func(context.Context) error,
) {
	defer wg.Done()

	for {
		// Limit how quickly the load generator sends requests based on --max-rate.
		if limiter != nil {
			if err := limiter.Wait(ctx); err != nil {
				panic(err)
			}
		}

		if err := workFn(ctx); err != nil {
			errCh <- err
			continue
		}

		v := atomic.AddUint64(&numOps, 1)
		if *maxOps > 0 && v >= *maxOps {
			return
		}
	}
}

func runInit(gen workload.Generator, urls []string, dbName string) error {
	ctx := context.Background()

	initDB, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return err
	}

	return runInitImpl(ctx, gen, initDB, dbName)
}

func runInitImpl(
	ctx context.Context, gen workload.Generator, initDB *gosql.DB, dbName string,
) error {
	if *drop {
		if _, err := initDB.ExecContext(ctx, `DROP DATABASE IF EXISTS `+dbName); err != nil {
			return err
		}
	}
	if _, err := initDB.ExecContext(ctx, `CREATE DATABASE IF NOT EXISTS `+dbName); err != nil {
		return err
	}

	const batchSize = -1
	// TODO(dan): Don't hardcode this. Similar to dbOverride, this should be
	// hooked up to a flag directly once once more of run.go moves inside
	// workload.
	const concurrency = 16
	_, err := workload.Setup(ctx, initDB, gen, batchSize, concurrency)
	return err
}

func runRun(gen workload.Generator, urls []string, dbName string) error {
	ctx := context.Background()

	initDB, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return err
	}
	if *doInit || *drop {
		for {
			err = runInitImpl(ctx, gen, initDB, dbName)
			if err == nil {
				break
			}
			if !*tolerateErrors {
				return err
			}
		}
	}

	var limiter *rate.Limiter
	if *maxRate > 0 {
		// Create a limiter using maxRate specified on the command line and
		// with allowed burst of 1 at the maximum allowed rate.
		limiter = rate.NewLimiter(rate.Limit(*maxRate), 1)
	}

	o, ok := gen.(workload.Opser)
	if !ok {
		return errors.Errorf(`no operations defined for %s`, gen.Meta().Name)
	}
	reg := workload.NewHistogramRegistry()
	ops, err := o.Ops(urls, reg)
	if err != nil {
		return err
	}

	for _, table := range gen.Tables() {
		splitConcurrency := len(ops.WorkerFns)
		if err := workload.Split(ctx, initDB, table, splitConcurrency); err != nil {
			return err
		}
	}

	start := timeutil.Now()
	errCh := make(chan error)
	var wg sync.WaitGroup
	sleepTime := *ramp / time.Duration(len(ops.WorkerFns))
	wg.Add(len(ops.WorkerFns))
	go func() {
		for _, workFn := range ops.WorkerFns {
			workFn := workFn
			go workerRun(ctx, errCh, &wg, limiter, workFn)
			time.Sleep(sleepTime)
		}
	}()

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
			startElapsed := timeutil.Since(start)
			reg.Tick(func(t workload.HistogramTick) {
				if i%20 == 0 {
					fmt.Println("_elapsed___errors__ops/sec(inst)___ops/sec(cum)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
				}
				i++
				fmt.Printf("%8s %8d %14.1f %14.1f %8.1f %8.1f %8.1f %8.1f %s\n",
					time.Duration(startElapsed.Seconds()+0.5)*time.Second,
					numErr,
					float64(t.Ops-t.LastOps)/t.Elapsed.Seconds(),
					float64(t.Ops)/startElapsed.Seconds(),
					time.Duration(t.Hist.ValueAtQuantile(50)).Seconds()*1000,
					time.Duration(t.Hist.ValueAtQuantile(95)).Seconds()*1000,
					time.Duration(t.Hist.ValueAtQuantile(99)).Seconds()*1000,
					time.Duration(t.Hist.ValueAtQuantile(100)).Seconds()*1000,
					t.Name,
				)
			})

		case <-done:
			const totalHeader = "\n_elapsed___errors_____ops(total)___ops/sec(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)"
			fmt.Println(totalHeader + `__total`)
			startElapsed := timeutil.Since(start)
			printTotalHist := func(t workload.HistogramTick) {
				if t.Ops == 0 {
					return
				}
				fmt.Printf("%7.1fs %8d %14d %14.1f %8.1f %8.1f %8.1f %8.1f %8.1f  %s\n",
					startElapsed.Seconds(), numErr,
					t.Ops, float64(t.Ops)/startElapsed.Seconds(),
					time.Duration(t.Cumulative.Mean()).Seconds()*1000,
					time.Duration(t.Cumulative.ValueAtQuantile(50)).Seconds()*1000,
					time.Duration(t.Cumulative.ValueAtQuantile(95)).Seconds()*1000,
					time.Duration(t.Cumulative.ValueAtQuantile(99)).Seconds()*1000,
					time.Duration(t.Cumulative.ValueAtQuantile(100)).Seconds()*1000,
					t.Name,
				)
			}

			resultTick := workload.HistogramTick{Name: ops.ResultHist}
			reg.Tick(func(t workload.HistogramTick) {
				printTotalHist(t)
				if ops.ResultHist == `` || ops.ResultHist == t.Name {
					resultTick.Ops += t.Ops
					if resultTick.Cumulative == nil {
						resultTick.Cumulative = t.Cumulative
					} else {
						resultTick.Cumulative.Merge(t.Cumulative)
					}
				}
			})

			fmt.Println(totalHeader + `__result`)
			printTotalHist(resultTick)

			if h, ok := gen.(workload.Hookser); ok {
				if h.Hooks().PostRun != nil {
					if err := h.Hooks().PostRun(start); err != nil {
						fmt.Printf("failed post-run hook: %v\n", err)
					}
				}
			}

			// Output results that mimic Go's built-in benchmark format.
			benchmarkName := strings.Join([]string{
				"BenchmarkWorkload",
				fmt.Sprintf("generator=%s", gen.Meta().Name),
			}, "/")
			if *duration != time.Duration(0) {
				benchmarkName += `/duration=` + duration.String()
			}
			if f, ok := gen.(workload.Flagser); ok {
				// NB: This visits in a deterministic order.
				f.Flags().Visit(func(f *pflag.Flag) {
					benchmarkName += fmt.Sprintf(`/%s=%s`, f.Name, f.Value)
				})
			}
			result := testing.BenchmarkResult{N: int(resultTick.Ops), T: startElapsed}
			fmt.Printf("\n%s\t%s\n", benchmarkName, result)

			if *histFile == "-" {
				if err := histwriter.WriteDistribution(
					resultTick.Cumulative, nil, 1, os.Stdout,
				); err != nil {
					fmt.Printf("failed to write histogram to stdout: %v\n", err)
				}
			} else if *histFile != "" {
				if err := histwriter.WriteDistributionFile(
					resultTick.Cumulative, nil, 1, *histFile,
				); err != nil {
					fmt.Printf("failed to write histogram file: %v\n", err)
				}
			}
			return nil
		}
	}
}
