// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	gosql "database/sql"
	"database/sql/driver"
	"fmt"
	"io"
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

	"github.com/cockroachdb/cockroach/pkg/util/allstacks"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram/exporter"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"golang.org/x/time/rate"
)

var runFlags = pflag.NewFlagSet(`run`, pflag.ContinueOnError)
var tolerateErrors = runFlags.Bool("tolerate-errors", false, "Keep running on error")
var maxRate = runFlags.Float64(
	"max-rate", 0, "Maximum frequency of operations (reads/writes). If 0, no limit.")
var maxOps = runFlags.Uint64("max-ops", 0, "Maximum number of operations to run")
var countErrors = runFlags.Bool("count-errors", false, "If true, unsuccessful operations count towards --max-ops limit.")
var duration = runFlags.Duration("duration", 0,
	"The duration to run (in addition to --ramp). If 0, run forever.")
var doInit = runFlags.Bool("init", false, "Automatically run init. DEPRECATED: Use workload init instead.")
var ramp = runFlags.Duration("ramp", 0*time.Second, "The duration over which to ramp up load.")

var initFlags = pflag.NewFlagSet(`init`, pflag.ContinueOnError)
var drop = initFlags.Bool("drop", false, "Drop the existing database, if it exists")

var sharedFlags = pflag.NewFlagSet(`shared`, pflag.ContinueOnError)
var pprofport = sharedFlags.Int("pprofport", 33333, "Port for pprof endpoint.")
var dataLoader = sharedFlags.String("data-loader", `AUTO`,
	"How to load initial table data. All workloads support INSERT; some support IMPORT, which AUTO prefers if available.")
var initConns = sharedFlags.Int("init-conns", 16,
	"The number of connections to use during INSERT init")

var displayEvery = runFlags.Duration("display-every", time.Second, "How much time between every one-line activity reports.")

var displayFormat = runFlags.String("display-format", "simple", "Output display format (simple, incremental-json)")

var prometheusPort = sharedFlags.Int(
	"prometheus-port",
	2112,
	"Port to expose prometheus metrics if the workload has a prometheus gatherer set.",
)

// individualOperationReceiverAddr is an address to send latency
// measurements to. By default it will not send anything.
var individualOperationReceiverAddr = sharedFlags.String(
	"operation-receiver",
	"",
	"Optional ip address:port to send latency operation metrics.",
)

var histograms = runFlags.String(
	"histograms", "",
	"File to write per-op incremental and cumulative histogram data.")

var histogramExportFormat = runFlags.String(
	"histogram-export-format", "json",
	"Export format of the histogram data into the `histograms` file. Options: [ json, openmetrics ]")
var histogramsMaxLatency = runFlags.Duration(
	"histograms-max-latency", 100*time.Second,
	"Expected maximum latency of running a query")

var openmetricsLabels = runFlags.String("openmetrics-labels", "",
	"Comma separated list of key value pairs used as labels, used by openmetrics exporter. Eg 'cloud=aws, workload=tpcc'")

var securityFlags = pflag.NewFlagSet(`security`, pflag.ContinueOnError)
var secure = securityFlags.Bool("secure", false,
	"Run in secure mode (sslmode=require). "+
		"Running in secure mode expects the relevant certs to have been created for the user in the certs/ directory."+
		"For example when using root, certs/client.root.crt certs/client.root.key should exist.")
var user = securityFlags.String("user", "root", "Specify a user to run the workload as")
var password = securityFlags.String("password", "", "Optionally specify a password for the user")

func init() {

	_ = sharedFlags.MarkHidden("pprofport")

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
			genInitCmd.Flags().AddFlagSet(securityFlags)
			genInitCmd.Run = CmdHelper(gen, runInit)
			if meta.TestInfraOnly {
				genInitCmd.Long = "THIS COMMAND WAS DEVELOPED FOR INTERNAL TESTING ONLY.\n\n" + genInitCmd.Long
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
			genRunCmd.Flags().AddFlagSet(securityFlags)
			initFlags.VisitAll(func(initFlag *pflag.Flag) {
				// Every init flag is a valid run flag that implies the --init option.
				f := *initFlag
				f.Usage += ` (implies --init)`
				genRunCmd.Flags().AddFlag(&f)
			})
			if meta.TestInfraOnly {
				genRunCmd.Long = "THIS COMMAND WAS DEVELOPED FOR INTERNAL TESTING ONLY.\n\n" + genRunCmd.Long
			}
			genRunCmd.Run = CmdHelper(gen, runRun)
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
	return HandleErrs(func(cmd *cobra.Command, args []string) error {
		// Apply the logging configuration if none was set already.
		if active, _ := log.IsActive(); !active {
			cfg := logconfig.DefaultStderrConfig()
			if err := cfg.Validate(nil /* no default log directory */); err != nil {
				return err
			}
			if _, err := log.ApplyConfig(cfg, nil /* fileSinkMetricsForDir */, nil /* fatalOnLogStall */); err != nil {
				return err
			}
		}

		if h, ok := gen.(workload.Hookser); ok {
			if h.Hooks().Validate != nil {
				if err := h.Hooks().Validate(); err != nil {
					return errors.Wrapf(err, "could not validate")
				}
			}
		}

		var connFlags *workload.ConnFlags
		if cf, ok := gen.(workload.ConnFlagser); ok {
			connFlags = cf.ConnFlags()
		}

		urls := args
		if len(urls) == 0 {
			crdbDefaultURL := fmt.Sprintf(`postgres://%s@localhost:26257?sslmode=disable`, *user)
			if *secure {
				if *password != "" {
					crdbDefaultURL = fmt.Sprintf(
						`postgres://%s:%s@localhost:26257?sslmode=require&sslrootcert=certs/ca.crt`,
						*user, *password)
				} else {
					crdbDefaultURL = fmt.Sprintf(
						// This URL expects the certs to have been created by the user.
						`postgres://%s@localhost:26257?sslcert=certs/client.%s.crt&sslkey=certs/client.%s.key&sslrootcert=certs/ca.crt&sslmode=require`,
						*user, *user, *user)
				}
			}

			urls = []string{crdbDefaultURL}
		}
		dbName, err := workload.SanitizeUrls(gen, connFlags, urls)
		if err != nil {
			return err
		}
		if err := workload.SetUrlConnVars(gen, connFlags, urls); err != nil {
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

// numOps keeps a global count of successful operations (if countErrors is
// false) or of all operations (if countErrors is true).
var numOps atomic.Uint64

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
			if ctx.Err() != nil && (errors.Is(err, ctx.Err()) || errors.Is(err, driver.ErrBadConn)) {
				// lib/pq may return either the `context canceled` error or a
				// `bad connection` error when performing an operation with a context
				// that has been canceled. See https://github.com/lib/pq/pull/1000
				return
			}
			errCh <- err
			if !*countErrors {
				// Continue to the next iteration of the infinite loop only if
				// we are not counting the errors.
				continue
			}
		}

		v := numOps.Add(1)
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
	maybeLogRandomSeed(ctx, gen)
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

	lc := strings.ToLower(*dataLoader)
	if lc == "auto" {
		lc = "insert"
		// Even if it does support fixtures, the CRDB cluster needs to know the
		// workload. This can only be expected if the workload is public-facing.
		// For example, at the time of writing, neither roachmart and ledger are
		// public-facing, but both support fixtures. However, returning true here
		// would result in "pq: unknown generator: roachmart" from the cluster.
		if workload.SupportsFixtures(gen) {
			lc = "import"
		}
	}

	var l workload.InitialDataLoader
	switch lc {
	case `insert`, `inserts`:
		l = workloadsql.InsertsDataLoader{
			Concurrency: *initConns,
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

	// Adding --duration to the generator flags for checking long duration workload in tpcc
	gen.(workload.Flagser).Flags().AddFlag(runFlags.Lookup("duration"))
	var limiter *rate.Limiter
	if *maxRate > 0 {
		// Create a limiter using maxRate specified on the command line and
		// with allowed burst of 1 at the maximum allowed rate.
		limiter = rate.NewLimiter(rate.Limit(*maxRate), 1)
	}

	maybeLogRandomSeed(ctx, gen)
	o, ok := gen.(workload.Opser)
	if !ok {
		return errors.Errorf(`no operations defined for %s`, gen.Meta().Name)
	}

	var publisher histogram.Publisher
	if *individualOperationReceiverAddr != "" {
		publisher = histogram.CreateUdpPublisher(*individualOperationReceiverAddr)
	}

	metricsExporter, file, err := maybeInitAndCreateExporter(gen)
	if err != nil {
		return errors.Wrap(err, "error creating metrics exporter")
	}
	defer func() {
		if metricsExporter != nil {
			if err = metricsExporter.Close(func() error {
				if file == nil {
					log.Infof(ctx, "no file to close")
					return nil
				}

				if err := file.Close(); err != nil {
					return err
				}
				return nil
			}); err != nil {
				log.Warningf(ctx, "failed to close metrics exporter: %v", err)
			}
		}
	}()

	reg := histogram.NewRegistryWithPublisherAndExporter(
		*histogramsMaxLatency,
		gen.Meta().Name,
		publisher,
		metricsExporter,
	)
	reg.Registerer().MustRegister(collectors.NewGoCollector())
	// Expose the prometheus gatherer.
	go func() {
		if err := http.ListenAndServe(
			fmt.Sprintf(":%d", *prometheusPort),
			promhttp.HandlerFor(reg.Gatherer(), promhttp.HandlerOpts{}),
		); err != nil {
			log.Errorf(context.Background(), "error serving prometheus: %v", err)
		}
	}()

	var ops workload.QueryLoad
	prepareStart := timeutil.Now()
	log.Infof(ctx, "creating load generator...")
	// We set up a timer that cancels this context after prepareTimeout,
	// but we'll collect the stacks before we do, so that they can be
	// logged.
	prepareCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stacksCh := make(chan []byte, 1)
	const prepareTimeout = 90 * time.Minute
	defer time.AfterFunc(prepareTimeout, func() {
		stacksCh <- allstacks.Get()
		cancel()
	}).Stop()
	if prepareErr := func(ctx context.Context) error {
		retry := retry.StartWithCtx(ctx, retry.Options{})
		var err error
		for retry.Next() {
			if err != nil {
				log.Warningf(ctx, "retrying after error while creating load: %v", err)
			}
			ops, err = o.Ops(ctx, urls, reg)
			if err == nil {
				return nil
			}
			err = errors.Wrapf(err, "failed to initialize the load generator")
			if !*tolerateErrors {
				return err
			}
		}
		if ctx.Err() != nil {
			// Don't retry endlessly. Note that this retry loop is not under the
			// control of --duration, so we're avoiding retrying endlessly.
			log.Errorf(ctx, "Attempt to create load generator failed. "+
				"It's been more than %s since we started trying to create the load generator "+
				"so we're giving up. Last failure: %s\nStacks:\n%s", prepareTimeout, err, <-stacksCh)
		}
		return err
	}(prepareCtx); prepareErr != nil {
		return prepareErr
	}
	log.Infof(ctx, "creating load generator... done (took %s)", timeutil.Since(prepareStart))

	start := timeutil.Now()
	errCh := make(chan error)
	var rampDone chan struct{}
	if *ramp > 0 {
		// Create a channel to signal when the ramp period finishes. Will
		// be reset to nil when consumed by the process loop below.
		rampDone = make(chan struct{})
	}

	// If ops.Close is specified, defer it to ensure that it is run before
	// exiting.
	if ops.Close != nil {
		defer func() {
			if err := ops.Close(ctx); err != nil {
				fmt.Printf("failed .Close: %v\n", err)
			}
		}()
	}

	workersCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()
	var wg sync.WaitGroup
	wg.Add(len(ops.WorkerFns))
	go func() {
		// If a ramp period was specified, start all the workers gradually
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
				// gradually.
				if rampCtx != nil {
					rampPerWorker := *ramp / time.Duration(len(ops.WorkerFns))
					time.Sleep(time.Duration(i) * rampPerWorker)
				}
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
			// Log the error with %+v so we get the stack trace.
			log.Errorf(ctx, "workload run error: %+v", err)
			return err

		case <-ticker.C:
			startElapsed := timeutil.Since(start)
			reg.Tick(func(t histogram.Tick) {
				formatter.outputTick(startElapsed, t)
				if t.Exporter != nil && rampDone == nil {
					if err := t.Exporter.SnapshotAndWrite(t.Hist, t.Now, t.Elapsed, &t.Name); err != nil {
						log.Warningf(ctx, "histogram: %v", err)
					}
				}
			})

		// Once the load generator is fully ramped up, we reset the histogram
		// and the start time to throw away the stats for the ramp up period.
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

			startElapsed := timeutil.Since(start)
			resultTick := histogram.Tick{Name: ops.ResultHist}
			reg.Tick(func(t histogram.Tick) {
				formatter.outputTotal(startElapsed, t)
				if t.Exporter != nil {
					if err := t.Exporter.SnapshotAndWrite(t.Hist, t.Now, t.Elapsed, &t.Name); err != nil {
						log.Warningf(ctx, "histogram: %v", err)
					}
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

// maybeLogRandomSeed will log the random seed used by the generator,
// if a seed is being used.
func maybeLogRandomSeed(ctx context.Context, gen workload.Generator) {
	if randomSeed := gen.Meta().RandomSeed; randomSeed != nil {
		log.Infof(ctx, "%s", randomSeed.LogMessage())
	}
}

func maybeInitAndCreateExporter(gen workload.Generator) (exporter.Exporter, *os.File, error) {
	if *histograms == "" {
		return nil, nil, nil
	}

	var metricsExporter exporter.Exporter
	var file *os.File

	switch *histogramExportFormat {
	case "json":
		metricsExporter = &exporter.HdrJsonExporter{}
	case "openmetrics":
		labelValues := strings.Split(*openmetricsLabels, ",")
		labels := make(map[string]string)
		for _, label := range labelValues {
			parts := strings.SplitN(label, "=", 2)
			if len(parts) != 2 {
				return nil, nil, errors.Errorf("invalid histogram label %q", label)
			}
			labels[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
		// Append workload generator name as a tag
		labels["workload"] = gen.Meta().Name
		openMetricsExporter := exporter.OpenMetricsExporter{}
		openMetricsExporter.SetLabels(&labels)
		metricsExporter = &openMetricsExporter

	default:
		return nil, nil, errors.Errorf("unknown histogram format: %s", *histogramExportFormat)
	}

	err := metricsExporter.Validate(*histograms)
	if err != nil {
		return nil, nil, err
	}

	err = os.MkdirAll(filepath.Dir(*histograms), 0755)
	if err != nil {
		return nil, nil, err
	}

	file, err = os.Create(*histograms)
	if err != nil {
		return nil, nil, err
	}
	writer := io.Writer(file)

	metricsExporter.Init(&writer)

	return metricsExporter, file, nil
}
