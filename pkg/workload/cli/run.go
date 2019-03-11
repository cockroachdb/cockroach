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

package cli

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logflags"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"golang.org/x/time/rate"
)

var runFlags = pflag.NewFlagSet(`run`, pflag.ContinueOnError)
var tolerateErrors = runFlags.Bool("tolerate-errors", false, "Keep running on error")
var maxRate = runFlags.Float64(
	"max-rate", 0, "Maximum frequency of operations (reads/writes). If 0, no limit.")
var maxOps = runFlags.Uint64("max-ops", 0, "Maximum number of operations to run")
var duration = runFlags.Duration("duration", 0, "The duration to run. If 0, run forever.")
var doInit = runFlags.Bool("init", false, "Automatically run init")
var ramp = runFlags.Duration("ramp", 0*time.Second, "The duration over which to ramp up load.")

var initFlags = pflag.NewFlagSet(`init`, pflag.ContinueOnError)
var drop = initFlags.Bool("drop", false, "Drop the existing database, if it exists")

var sharedFlags = pflag.NewFlagSet(`shared`, pflag.ContinueOnError)
var pprofport = initFlags.Int("pprofport", 33333, "Port for pprof endpoint.")

var histograms = runFlags.String(
	"histograms", "",
	"File to write per-op incremental and cumulative histogram data.")

func init() {
	AddSubCmd(func(userFacing bool) *cobra.Command {
		var initCmd = SetCmdDefaults(&cobra.Command{
			Use:   `init`,
			Short: `set up tables for a workload`,
		})
		for _, meta := range workload.Registered() {
			gen := meta.New()
			var genFlags *pflag.FlagSet
			if f, ok := gen.(workload.Flagser); ok {
				genFlags = f.Flags().FlagSet
			}

			genInitCmd := SetCmdDefaults(&cobra.Command{
				Use:   meta.Name,
				Short: meta.Description,
				Long:  meta.Description + meta.Details,
				Args:  cobra.ArbitraryArgs,
			})
			genInitCmd.Flags().AddFlagSet(initFlags)
			genInitCmd.Flags().AddFlagSet(sharedFlags)
			genInitCmd.Flags().AddFlagSet(genFlags)
			genInitCmd.Run = CmdHelper(gen, runInit)
			if userFacing && !meta.PublicFacing {
				genInitCmd.Hidden = true
			}
			initCmd.AddCommand(genInitCmd)
		}
		return initCmd
	})
	AddSubCmd(func(userFacing bool) *cobra.Command {
		var runCmd = SetCmdDefaults(&cobra.Command{
			Use:   `run`,
			Short: `run a workload's operations against a cluster`,
		})
		for _, meta := range workload.Registered() {
			gen := meta.New()
			if _, ok := gen.(workload.Opser); !ok {
				// If Opser is not implemented, this would just fail at runtime,
				// so omit it.
				continue
			}

			var genFlags *pflag.FlagSet
			if f, ok := gen.(workload.Flagser); ok {
				genFlags = f.Flags().FlagSet
			}

			genRunCmd := SetCmdDefaults(&cobra.Command{
				Use:   meta.Name,
				Short: meta.Description,
				Long:  meta.Description + meta.Details,
				Args:  cobra.ArbitraryArgs,
			})
			genRunCmd.Flags().AddFlagSet(runFlags)
			genRunCmd.Flags().AddFlagSet(sharedFlags)
			genRunCmd.Flags().AddFlagSet(genFlags)
			initFlags.VisitAll(func(initFlag *pflag.Flag) {
				// Every init flag is a valid run flag that implies the --init option.
				f := *initFlag
				f.Usage += ` (implies --init)`
				genRunCmd.Flags().AddFlag(&f)
			})
			genRunCmd.Run = CmdHelper(gen, runRun)
			if userFacing && !meta.PublicFacing {
				genRunCmd.Hidden = true
			}
			runCmd.AddCommand(genRunCmd)
		}
		return runCmd
	})
}

// CmdHelper handles common workload command logic, such as error handling and
// ensuring the database name in the connection string (if provided) matches the
// expected one.
func CmdHelper(
	gen workload.Generator, fn func(gen workload.Generator, urls []string, dbName string) error,
) func(*cobra.Command, []string) {
	const crdbDefaultURL = `postgres://root@localhost:26257?sslmode=disable`

	return HandleErrs(func(cmd *cobra.Command, args []string) error {
		if ls := cmd.Flags().Lookup(logflags.LogToStderrName); ls != nil {
			if !ls.Changed {
				// Unless the settings were overridden by the user, default to logging
				// to stderr.
				_ = ls.Value.Set(log.Severity_INFO.String())
			}
		}

		if h, ok := gen.(workload.Hookser); ok {
			if h.Hooks().Validate != nil {
				if err := h.Hooks().Validate(); err != nil {
					return errors.Wrapf(err, "could not validate")
				}
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

// SetCmdDefaults ensures that the provided Cobra command will properly report
// an error if the user specifies an invalid subcommand. It is safe to call on
// any Cobra command.
//
// This is a wontfix bug in Cobra: https://github.com/spf13/cobra/pull/329
func SetCmdDefaults(cmd *cobra.Command) *cobra.Command {
	if cmd.Run == nil && cmd.RunE == nil {
		cmd.Run = func(cmd *cobra.Command, args []string) {
			_ = cmd.Usage()
		}
	}
	if cmd.Args == nil {
		cmd.Args = cobra.NoArgs
	}
	return cmd
}

// numOps keeps a global count of successful operations.
var numOps uint64

// workerRun is an infinite loop in which the worker continuously attempts to
// read / write blocks of random data into a table in cockroach DB. The function
// returns only when the provided context is canceled.
func workerRun(
	ctx context.Context,
	errCh chan<- error,
	wg *sync.WaitGroup,
	limiter *rate.Limiter,
	workFn func(context.Context) error,
) {
	if wg != nil {
		defer wg.Done()
	}

	for {
		if ctx.Err() != nil {
			return
		}

		// Limit how quickly the load generator sends requests based on --max-rate.
		if limiter != nil {
			if err := limiter.Wait(ctx); err != nil {
				if err == ctx.Err() {
					return
				}
				panic(err)
			}
		}

		if err := workFn(ctx); err != nil {
			if errors.Cause(err) == ctx.Err() {
				return
			}
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

	startPProfEndPoint(ctx)
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

func startPProfEndPoint(ctx context.Context) {
	b := envutil.EnvOrDefaultInt64("COCKROACH_BLOCK_PROFILE_RATE",
		10000000 /* 1 sample per 10 milliseconds spent blocking */)

	m := envutil.EnvOrDefaultInt("COCKROACH_MUTEX_PROFILE_RATE",
		1000 /* 1 sample per 1000 mutex contention events */)
	runtime.SetBlockProfileRate(int(b))
	runtime.SetMutexProfileFraction(m)

	go func() {
		err := http.ListenAndServe(":"+strconv.Itoa(*pprofport), nil)
		if err != nil {
			log.Error(ctx, err)
		}
	}()
}

func runRun(gen workload.Generator, urls []string, dbName string) error {
	ctx := context.Background()

	startPProfEndPoint(ctx)
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
	reg := histogram.NewRegistry()
	ops, err := o.Ops(urls, reg)
	if err != nil {
		return err
	}

	const splitConcurrency = 384 // TODO(dan): Don't hardcode this.
	for _, table := range gen.Tables() {
		if err := workload.Split(ctx, initDB, table, splitConcurrency); err != nil {
			return err
		}
	}

	start := timeutil.Now()
	errCh := make(chan error)
	var rampDone chan struct{}
	if *ramp > 0 {
		// Create a channel to signal when the ramp period finishes. Will
		// be reset to nil when consumed by the process loop below.
		rampDone = make(chan struct{})
	}

	workersCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()
	var wg sync.WaitGroup
	wg.Add(len(ops.WorkerFns))
	go func() {
		// If a ramp period was specified, start all of the workers gradually
		// with a new context.
		var rampCtx context.Context
		if rampDone != nil {
			var cancel func()
			rampCtx, cancel = context.WithTimeout(workersCtx, *ramp)
			defer cancel()
		}

		for i, workFn := range ops.WorkerFns {
			go func(i int, workFn func(context.Context) error) {
				// If a ramp period was specified, start all of the workers
				// gradually with a new context.
				if rampCtx != nil {
					rampPerWorker := *ramp / time.Duration(len(ops.WorkerFns))
					time.Sleep(time.Duration(i) * rampPerWorker)
					workerRun(rampCtx, errCh, nil /* wg */, limiter, workFn)
				}

				// Start worker again, this time with the main context.
				workerRun(workersCtx, errCh, &wg, limiter, workFn)
			}(i, workFn)
		}

		if rampCtx != nil {
			// Wait for the ramp period to finish, then notify the process loop
			// below to reset timers and histograms.
			<-rampCtx.Done()
			close(rampDone)
		}
	}()

	var numErr int
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	done := make(chan os.Signal, 3)
	signal.Notify(done, exitSignals...)

	go func() {
		wg.Wait()
		done <- os.Interrupt
	}()

	if *duration > 0 {
		go func() {
			time.Sleep(*duration + *ramp)
			done <- os.Interrupt
		}()
	}

	var jsonEnc *json.Encoder
	if *histograms != "" {
		_ = os.MkdirAll(filepath.Dir(*histograms), 0755)
		jsonF, err := os.Create(*histograms)
		if err != nil {
			return err
		}
		jsonEnc = json.NewEncoder(jsonF)
	}

	everySecond := log.Every(time.Second)
	for i := 0; ; {
		select {
		case err := <-errCh:
			numErr++
			if *tolerateErrors {
				if everySecond.ShouldLog() {
					log.Error(ctx, err)
				}
				continue
			}
			return err

		case <-ticker.C:
			startElapsed := timeutil.Since(start)
			reg.Tick(func(t histogram.Tick) {
				if i%20 == 0 {
					fmt.Println("_elapsed___errors__ops/sec(inst)___ops/sec(cum)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
				}
				i++
				fmt.Printf("%8s %8d %14.1f %14.1f %8.1f %8.1f %8.1f %8.1f %s\n",
					time.Duration(startElapsed.Seconds()+0.5)*time.Second,
					numErr,
					float64(t.Hist.TotalCount())/t.Elapsed.Seconds(),
					float64(t.Cumulative.TotalCount())/startElapsed.Seconds(),
					time.Duration(t.Hist.ValueAtQuantile(50)).Seconds()*1000,
					time.Duration(t.Hist.ValueAtQuantile(95)).Seconds()*1000,
					time.Duration(t.Hist.ValueAtQuantile(99)).Seconds()*1000,
					time.Duration(t.Hist.ValueAtQuantile(100)).Seconds()*1000,
					t.Name,
				)
				if jsonEnc != nil && rampDone == nil {
					_ = jsonEnc.Encode(t.Snapshot())
				}
			})

		// Once the load generator is fully ramped up, we reset the histogram
		// and the start time to throw away the stats for the the ramp up period.
		case <-rampDone:
			rampDone = nil
			start = timeutil.Now()
			i = 0
			reg.Tick(func(t histogram.Tick) {
				t.Cumulative.Reset()
				t.Hist.Reset()
			})

		case <-done:
			cancelWorkers()
			if ops.Close != nil {
				ops.Close(ctx)
			}
			const totalHeader = "\n_elapsed___errors_____ops(total)___ops/sec(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)"
			fmt.Println(totalHeader + `__total`)
			startElapsed := timeutil.Since(start)
			printTotalHist := func(t histogram.Tick) {
				if t.Cumulative == nil {
					return
				}
				if t.Cumulative.TotalCount() == 0 {
					return
				}
				fmt.Printf("%7.1fs %8d %14d %14.1f %8.1f %8.1f %8.1f %8.1f %8.1f  %s\n",
					startElapsed.Seconds(), numErr,
					t.Cumulative.TotalCount(),
					float64(t.Cumulative.TotalCount())/startElapsed.Seconds(),
					time.Duration(t.Cumulative.Mean()).Seconds()*1000,
					time.Duration(t.Cumulative.ValueAtQuantile(50)).Seconds()*1000,
					time.Duration(t.Cumulative.ValueAtQuantile(95)).Seconds()*1000,
					time.Duration(t.Cumulative.ValueAtQuantile(99)).Seconds()*1000,
					time.Duration(t.Cumulative.ValueAtQuantile(100)).Seconds()*1000,
					t.Name,
				)
			}

			resultTick := histogram.Tick{Name: ops.ResultHist}
			reg.Tick(func(t histogram.Tick) {
				printTotalHist(t)
				if jsonEnc != nil {
					// Note that we're outputting the delta from the last tick. The
					// cumulative histogram can be computed by merging all of the
					// per-tick histograms.
					_ = jsonEnc.Encode(t.Snapshot())
				}
				if ops.ResultHist == `` || ops.ResultHist == t.Name {
					if resultTick.Cumulative == nil {
						resultTick.Now = t.Now
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
					if err := h.Hooks().PostRun(startElapsed); err != nil {
						fmt.Printf("failed post-run hook: %v\n", err)
					}
				}
			}

			return nil
		}
	}
}
