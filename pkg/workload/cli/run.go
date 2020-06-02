// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"golang.org/x/time/rate"
)

var runFlags = pflag.NewFlagSet(`run`, pflag.ContinueOnError)
var tolerateErrors = runFlags.Bool("tolerate-errors", false, "Keep running on error")
var maxRate = runFlags.Float64(
	"max-rate", 0, "Maximum frequency of operations (reads/writes). If 0, no limit.")
var maxOps = runFlags.Uint64("max-ops", 0, "Maximum number of operations to run")
var duration = runFlags.Duration("duration", 0,
	"The duration to run (in addition to --ramp). If 0, run forever.")
var doInit = runFlags.Bool("init", false, "Automatically run init. DEPRECATED: Use workload init instead.")
var ramp = runFlags.Duration("ramp", 0*time.Second, "The duration over which to ramp up load.")

var initFlags = pflag.NewFlagSet(`init`, pflag.ContinueOnError)
var drop = initFlags.Bool("drop", false, "Drop the existing database, if it exists")

var sharedFlags = pflag.NewFlagSet(`shared`, pflag.ContinueOnError)
var pprofport = sharedFlags.Int("pprofport", 33333, "Port for pprof endpoint.")
var dataLoader = sharedFlags.String("data-loader", `INSERT`,
	"How to load initial table data. Options are INSERT and IMPORT")

var displayEvery = runFlags.Duration("display-every", time.Second, "How much time between every one-line activity reports.")

var displayFormat = runFlags.String("display-format", "simple", "Output display format (simple, incremental-json)")

var histograms = runFlags.String(
	"histograms", "",
	"File to write per-op incremental and cumulative histogram data.")
var histogramsMaxLatency = runFlags.Duration(
	"histograms-max-latency", 100*time.Second,
	"Expected maximum latency of running a query")

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
				Use:   meta.Name + " [pgurl...]",
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
				Use:   meta.Name + " [pgurl...]",
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
				return
			}
		}

		if err := workFn(ctx); err != nil {
			if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
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

	var l workload.InitialDataLoader
	switch strings.ToLower(*dataLoader) {
	case `insert`, `inserts`:
		l = workloadsql.InsertsDataLoader{
			// TODO(dan): Don't hardcode this. Similar to dbOverride, this should be
			// hooked up to a flag directly once once more of run.go moves inside
			// workload.
			Concurrency: 16,
		}
	case `import`, `imports`:
		l = workload.ImportDataLoader
	default:
		return errors.Errorf(`unknown data loader: %s`, *dataLoader)
	}

	_, err := workloadsql.Setup(ctx, initDB, gen, l)
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
			log.Errorf(ctx, "%v", err)
		}
	}()
}

func runRun(gen workload.Generator, urls []string, dbName string) error {
	ctx := context.Background()

	var formatter outputFormat
	switch *displayFormat {
	case "simple":
		formatter = &textFormatter{}
	case "incremental-json":
		formatter = &jsonFormatter{w: os.Stdout}
	default:
		return errors.Errorf("unknown display format: %s", *displayFormat)
	}

	startPProfEndPoint(ctx)
	initDB, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return err
	}
	if *doInit || *drop {
		log.Info(ctx, `DEPRECATION: `+
			`the --init flag on "workload run" will no longer be supported after 19.2`)
		for {
			err = runInitImpl(ctx, gen, initDB, dbName)
			if err == nil {
				break
			}
			if !*tolerateErrors {
				return err
			}
			log.Infof(ctx, "retrying after error during init: %v", err)
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
	reg := histogram.NewRegistry(*histogramsMaxLatency)
	var ops workload.QueryLoad
	for {
		ops, err = o.Ops(urls, reg)
		if err == nil {
			break
		}
		if !*tolerateErrors {
			return err
		}
		log.Infof(ctx, "retrying after error while creating load: %v", err)
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

	ticker := time.NewTicker(*displayEvery)
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

	everySecond := log.Every(*displayEvery)
	for {
		select {
		case err := <-errCh:
			formatter.outputError(err)
			if *tolerateErrors {
				if everySecond.ShouldLog() {
					log.Errorf(ctx, "%v", err)
				}
				continue
			}
			return err

		case <-ticker.C:
			startElapsed := timeutil.Since(start)
			reg.Tick(func(t histogram.Tick) {
				formatter.outputTick(startElapsed, t)
				if jsonEnc != nil && rampDone == nil {
					_ = jsonEnc.Encode(t.Snapshot())
				}
			})

		// Once the load generator is fully ramped up, we reset the histogram
		// and the start time to throw away the stats for the the ramp up period.
		case <-rampDone:
			rampDone = nil
			start = timeutil.Now()
			formatter.rampDone()
			reg.Tick(func(t histogram.Tick) {
				t.Cumulative.Reset()
				t.Hist.Reset()
			})

		case <-done:
			cancelWorkers()
			if ops.Close != nil {
				ops.Close(ctx)
			}

			startElapsed := timeutil.Since(start)
			resultTick := histogram.Tick{Name: ops.ResultHist}
			reg.Tick(func(t histogram.Tick) {
				formatter.outputTotal(startElapsed, t)
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
			formatter.outputResult(startElapsed, resultTick)

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
