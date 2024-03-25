// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/allstacks"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
)

type testResult int

const (
	// NB: These are in a particular order corresponding to the order we
	// want these tests to appear in the generated Markdown report.
	testResultFailure testResult = iota
	testResultSuccess
	testResultSkip
)

type testReportForGitHub struct {
	name     string
	duration time.Duration
	status   testResult
}

// runTests is the main function for the run and bench commands.
// Assumes initRunFlagsBinariesAndLibraries was called.
func runTests(register func(registry.Registry), filter *registry.TestFilter) error {
	//lint:ignore SA1019 deprecated
	rand.Seed(roachtestflags.GlobalSeed)
	r := makeTestRegistry()

	// actual registering of tests
	// TODO: don't register if we can't run on the specified registry cloud
	register(&r)
	cr := newClusterRegistry()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	runner := newTestRunner(cr, stopper)

	clusterType := roachprodCluster
	bindTo := ""
	parallelism := roachtestflags.Parallelism
	if roachtestflags.Cloud == spec.Local {
		clusterType = localCluster
		if parallelism != 1 {
			fmt.Printf("--local specified. Overriding --parallelism to 1.\n")
			parallelism = 1
		}
	}

	if runtime.GOOS == "darwin" {
		// This will suppress the annoying "Allow incoming network connections" popup from
		// OSX when running a roachtest
		bindTo = "localhost"
	}

	opt := clustersOpt{
		typ:         clusterType,
		clusterName: roachtestflags.ClusterNames,
		// Precedence for resolving the user: cli arg, env.ROACHPROD_USER, current user.
		user:      getUser(roachtestflags.Username),
		cpuQuota:  roachtestflags.CPUQuota,
		clusterID: roachtestflags.ClusterID,
	}
	switch {
	case roachtestflags.DebugAlways:
		opt.debugMode = DebugKeepAlways
	case roachtestflags.DebugOnFailure:
		opt.debugMode = DebugKeepOnFailure
	default:
		opt.debugMode = NoDebug
	}

	if err := runner.runHTTPServer(roachtestflags.HTTPPort, os.Stdout, bindTo); err != nil {
		return err
	}

	specs, err := testsToRun(r, filter, roachtestflags.RunSkipped, roachtestflags.SelectProbability, true)
	if err != nil {
		return err
	}

	n := len(specs)
	if n*roachtestflags.Count < parallelism {
		// Don't spin up more workers than necessary. This has particular
		// implications for the common case of running a single test once: if
		// parallelism is set to 1, we'll use teeToStdout below to get logs to
		// stdout/stderr.
		parallelism = n * roachtestflags.Count
	}
	if opt.debugMode == DebugKeepAlways && n > 1 {
		return errors.Newf("--debug-always is only allowed when running a single test")
	}

	artifactsDir := roachtestflags.ArtifactsDir
	literalArtifactsDir := roachtestflags.LiteralArtifactsDir
	if literalArtifactsDir == "" {
		literalArtifactsDir = artifactsDir
	}

	setLogConfig(artifactsDir)
	runnerDir := filepath.Join(artifactsDir, runnerLogsDir)
	runnerLogPath := filepath.Join(
		runnerDir, fmt.Sprintf("test_runner-%d.log", timeutil.Now().Unix()))
	l, tee := testRunnerLogger(context.Background(), parallelism, runnerLogPath)
	lopt := loggingOpt{
		l:                   l,
		tee:                 tee,
		stdout:              os.Stdout,
		stderr:              os.Stderr,
		artifactsDir:        artifactsDir,
		literalArtifactsDir: literalArtifactsDir,
		runnerLogPath:       runnerLogPath,
	}
	l.Printf("global random seed: %d", roachtestflags.GlobalSeed)
	go func() {
		if err := http.ListenAndServe(
			fmt.Sprintf(":%d", roachtestflags.PromPort),
			promhttp.HandlerFor(r.promRegistry, promhttp.HandlerOpts{}),
		); err != nil {
			l.Errorf("error serving prometheus: %v", err)
		}
	}()
	// We're going to run all the workers (and thus all the tests) in a context
	// that gets canceled when the Interrupt signal is received.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	CtrlC(ctx, l, cancel, cr)
	// Install goroutine leak checker and run it at the end of the entire test
	// run. If a test is leaking a goroutine, then it will likely be still around.
	// We could diff goroutine snapshots before/after each executed test, but that
	// could yield false positives; e.g., user-specified test teardown goroutines
	// may still be running long after the test has completed.
	defer leaktest.AfterTest(l)()

	err = runner.Run(
		ctx, specs, roachtestflags.Count, parallelism, opt,
		testOpts{
			versionsBinaryOverride: roachtestflags.VersionsBinaryOverride,
			skipInit:               roachtestflags.SkipInit,
			goCoverEnabled:         roachtestflags.GoCoverEnabled,
		},
		lopt)

	// Make sure we attempt to clean up. We run with a non-canceled ctx; the
	// ctx above might be canceled in case a signal was received. If that's
	// the case, we're running under a 5s timeout until the CtrlC() goroutine
	// kills the process.
	l.PrintfCtx(ctx, "runTests destroying all clusters")
	cr.destroyAllClusters(context.Background(), l)

	if roachtestflags.TeamCity {
		// Collect the runner logs.
		fmt.Printf("##teamcity[publishArtifacts '%s']\n", filepath.Join(literalArtifactsDir, runnerLogsDir))
	}

	if summaryErr := maybeDumpSummaryMarkdown(runner); summaryErr != nil {
		shout(ctx, l, os.Stdout, "failed to write to GITHUB_STEP_SUMMARY file (%+v)", summaryErr)
	}

	return err
}

// This diverts all the default non fatal logging to a file in `baseDir`. This is particularly
// useful in CI, where without this, stderr/stdout are cluttered with logs from various
// packages used in roachtest like sarama and testutils.
func setLogConfig(baseDir string) {
	logConf := logconfig.DefaultStderrConfig()
	logConf.Sinks.Stderr.Filter = logpb.Severity_FATAL
	if err := logConf.Validate(&baseDir); err != nil {
		panic(err)
	}
	if _, err := log.ApplyConfig(logConf, log.FileSinkMetrics{}); err != nil {
		panic(err)
	}
}

// getUser takes the value passed on the command line and comes up with the
// username to use.
func getUser(userFlag string) string {
	if userFlag != "" {
		return userFlag
	}
	usr, err := user.Current()
	if err != nil {
		panic(fmt.Sprintf("user.Current: %s", err))
	}
	return usr.Username
}

func initRunFlagsBinariesAndLibraries(cmd *cobra.Command) error {
	if roachtestflags.Local {
		if roachtestflags.ClusterNames != "" {
			return fmt.Errorf(
				"cannot specify both an existing cluster (%s) and --local. However, if a local cluster "+
					"already exists, --clusters=local will use it",
				roachtestflags.ClusterNames)
		}
		roachtestflags.Cloud = spec.Local
	}

	if roachtestflags.Count <= 0 {
		return fmt.Errorf("--count (%d) must by greater than 0", roachtestflags.Count)
	}

	if !(0 <= roachtestflags.ARM64Probability && roachtestflags.ARM64Probability <= 1) {
		return fmt.Errorf("'metamorphic-arm64-probability' must be in [0,1]")
	}
	if !(0 <= roachtestflags.FIPSProbability && roachtestflags.FIPSProbability <= 1) {
		return fmt.Errorf("'metamorphic-fips-probability' must be in [0,1]")
	}
	if roachtestflags.ARM64Probability == 1 && roachtestflags.FIPSProbability != 0 {
		return fmt.Errorf("'metamorphic-fips-probability' must be 0 when 'metamorphic-arm64-probability' is 1")
	}
	if roachtestflags.FIPSProbability == 1 && roachtestflags.ARM64Probability != 0 {
		return fmt.Errorf("'metamorphic-arm64-probability' must be 0 when 'metamorphic-fips-probability' is 1")
	}
	if !(0 <= roachtestflags.SelectProbability && roachtestflags.SelectProbability <= 1) {
		return fmt.Errorf("'select-probability' must be in [0,1]")
	}
	arm64Opt := cmd.Flags().Lookup("metamorphic-arm64-probability")
	if !arm64Opt.Changed && runtime.GOARCH == "arm64" && roachtestflags.Cloud == spec.Local {
		fmt.Printf("Detected 'arm64' in 'local mode', setting 'metamorphic-arm64-probability' to 1; use --metamorphic-arm64-probability to run (emulated) with other binaries\n")
		roachtestflags.ARM64Probability = 1
	}
	// Find and validate all required binaries and libraries.
	initBinariesAndLibraries()

	if roachtestflags.ARM64Probability > 0 {
		fmt.Printf("ARM64 clusters will be provisioned with probability %.2f\n", roachtestflags.ARM64Probability)
	}
	amd64Probability := 1 - roachtestflags.ARM64Probability
	if amd64Probability > 0 {
		fmt.Printf("AMD64 clusters will be provisioned with probability %.2f\n", amd64Probability)
	}
	if roachtestflags.FIPSProbability > 0 {
		// N.B. roachtestflags.ARM64Probability < 1, otherwise roachtestflags.FIPSProbability == 0, as per above check.
		// Hence, amd64Probability > 0 is implied.
		fmt.Printf("FIPS clusters will be provisioned with probability %.2f\n", roachtestflags.FIPSProbability*amd64Probability)
	}

	if roachtestflags.SelectProbability > 0 && roachtestflags.SelectProbability < 1 {
		fmt.Printf("Matching tests will be selected with probability %.2f\n", roachtestflags.SelectProbability)
	}
	return nil
}

// CtrlC spawns a goroutine that sits around waiting for SIGINT. Once the first
// signal is received, it calls cancel(), waits 5 seconds, and then calls
// cr.destroyAllClusters(). The expectation is that the main goroutine will
// respond to the cancelation and return, and so the process will be dead by the
// time the 5s elapse.
// If a 2nd signal is received, it calls os.Exit(2).
func CtrlC(ctx context.Context, l *logger.Logger, cancel func(), cr *clusterRegistry) {
	// Shut down test clusters when interrupted (for example CTRL-C).
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() {
		<-sig
		shout(ctx, l, os.Stderr,
			"Signaled received. Canceling workers and waiting up to 5s for them.")
		// Signal runner.Run() to stop.
		cancel()
		<-time.After(5 * time.Second)
		if cr == nil {
			shout(ctx, l, os.Stderr, "5s elapsed. No clusters registered; nothing to destroy.")
			l.Printf("all stacks:\n\n%s\n", allstacks.Get())
			os.Exit(2)
		}
		shout(ctx, l, os.Stderr, "5s elapsed. Will brutally destroy all clusters.")
		// Make sure there are no leftover clusters.
		destroyCh := make(chan struct{})
		go func() {
			// Destroy all clusters. Don't wait more than 5 min for that though.
			destroyCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			l.PrintfCtx(ctx, "CtrlC handler destroying all clusters")
			cr.destroyAllClusters(destroyCtx, l)
			cancel()
			close(destroyCh)
		}()
		// If we get a second CTRL-C, exit immediately.
		select {
		case <-sig:
			shout(ctx, l, os.Stderr, "Second SIGINT received. Quitting. Cluster might be left behind.")
		case <-destroyCh:
			shout(ctx, l, os.Stderr, "Done destroying all clusters.")
		}
		l.Printf("all stacks:\n\n%s\n", allstacks.Get())
		os.Exit(2)
	}()
}

// testRunnerLogger returns a logger to be used by the test runner and a tee
// option for the test logs.
//
// runnerLogPath is the path to the file that will contain the runner's log.
func testRunnerLogger(
	ctx context.Context, parallelism int, runnerLogPath string,
) (*logger.Logger, logger.TeeOptType) {
	teeOpt := logger.NoTee
	if parallelism == 1 {
		teeOpt = logger.TeeToStdout
	}

	var l *logger.Logger
	if teeOpt == logger.TeeToStdout {
		verboseCfg := logger.Config{Stdout: os.Stdout, Stderr: os.Stderr}
		var err error
		l, err = verboseCfg.NewLogger(runnerLogPath)
		if err != nil {
			panic(err)
		}
	} else {
		verboseCfg := logger.Config{}
		var err error
		l, err = verboseCfg.NewLogger(runnerLogPath)
		if err != nil {
			panic(err)
		}
	}
	shout(ctx, l, os.Stdout, "test runner logs in: %s", runnerLogPath)
	return l, teeOpt
}

func maybeDumpSummaryMarkdown(r *testRunner) error {
	if !roachtestflags.GitHubActions {
		return nil
	}
	summaryPath := os.Getenv("GITHUB_STEP_SUMMARY")
	if summaryPath == "" {
		return nil
	}
	summaryFile, err := os.OpenFile(summaryPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	_, err = summaryFile.WriteString(`| TestName | Status | Duration |
| --- | --- | --- |
`)
	if err != nil {
		return err
	}

	var allTests []testReportForGitHub
	for test := range r.status.pass {
		allTests = append(allTests, testReportForGitHub{
			name:     test.Name(),
			duration: test.duration(),
			status:   testResultSuccess,
		})
	}

	for test := range r.status.fail {
		allTests = append(allTests, testReportForGitHub{
			name:     test.Name(),
			duration: test.duration(),
			status:   testResultFailure,
		})
	}

	for test := range r.status.skip {
		allTests = append(allTests, testReportForGitHub{
			name:     test.Name(),
			duration: test.duration(),
			status:   testResultSkip,
		})
	}

	// Sort the test results: first fails, then successes, then skips, and
	// within each category sort by test duration in descending order.
	// Ties are very unlikely to happen but we break them by test name.
	slices.SortFunc(allTests, func(a, b testReportForGitHub) int {
		if a.status < b.status {
			return -1
		} else if a.status > b.status {
			return 1
		} else if a.duration > b.duration {
			return -1
		} else if a.duration < b.duration {
			return 1
		}
		return strings.Compare(a.name, b.name)
	})

	for _, test := range allTests {
		var statusString string
		if test.status == testResultFailure {
			statusString = "❌ FAILED"
		} else if test.status == testResultSuccess {
			statusString = "✅ SUCCESS"
		} else {
			statusString = "🟨 SKIPPED"
		}
		_, err := fmt.Fprintf(summaryFile, "| `%s` | %s | `%s` |\n", test.name, statusString, test.duration.String())
		if err != nil {
			return err
		}
	}

	return nil
}

// runOperation sequentially runs one operation matched by the passed-in filter.
func runOperation(register func(registry.Registry), filter string, clusterName string) error {
	//lint:ignore SA1019 deprecated
	rand.Seed(roachtestflags.GlobalSeed)
	r := makeTestRegistry()

	register(&r)
	ctx := context.Background()
	// NB: root logger with no path always tees to Stdout.
	l, err := logger.RootLogger("", logger.NoTee)
	if err != nil {
		return err
	}
	// TODO(bilal): This is excessive for just getting the number of nodes in the
	// cluster. We should expose a roachprod.Nodes method or so.
	nodes, err := roachprod.PgURL(ctx, l, clusterName, roachtestflags.CertsDir, roachprod.PGURLOptions{})
	if err != nil {
		return errors.Wrap(err, "roachtest: run-operation: error when getting number of nodes")
	}

	cSpec := spec.ClusterSpec{NodeCount: len(nodes)}
	c := &clusterImpl{
		name:       clusterName,
		spec:       cSpec,
		l:          l,
		expiration: cSpec.Expiration(),
		destroyState: destroyState{
			owned: false,
		},
		localCertsDir: roachtestflags.CertsDir,
	}

	specs, err := opsToRun(r, filter)
	if err != nil {
		return err
	}
	var opSpec *registry.OperationSpec
	if len(specs) > 1 {
		opSpec = &specs[rand.Intn(len(specs))]
		l.Printf("more than one operation found for filter %s, randomly selected %s to run", filter, opSpec.Name)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Cancel this context if we get an interrupt.
	CtrlC(ctx, l, cancel, nil /* registry */)
	// Install goroutine leak checker and run it at the end of the entire operation
	// run. This is good hygiene for operations, as operations can one day be
	// called from roachtests as well.
	defer leaktest.AfterTest(l)()

	op := &operationImpl{
		spec:      opSpec,
		cockroach: roachtestflags.CockroachBinaryPath,
		l:         l,
	}
	op.mu.cancel = cancel
	var cleanup registry.OperationCleanup
	func() {
		ctx, cancel := context.WithTimeout(ctx, opSpec.Timeout)
		defer cancel()

		cleanup = opSpec.Run(ctx, op, c)
	}()
	if op.Failed() {
		op.Status("operation failed")
		return op.mu.failures[0]
	}

	if cleanup == nil {
		op.Status("operation ran successfully")
		return nil
	}

	op.Status(fmt.Sprintf("operation ran successfully; waiting %s before cleanup", roachtestflags.WaitBeforeCleanup))
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(roachtestflags.WaitBeforeCleanup):
	}
	op.Status("running cleanup")
	func() {
		ctx, cancel := context.WithTimeout(ctx, opSpec.Timeout)
		defer cancel()

		cleanup.Cleanup(ctx, op, c)
	}()

	if op.Failed() {
		op.Status("operation cleanup failed")
		return op.mu.failures[0]
	}

	return nil
}
