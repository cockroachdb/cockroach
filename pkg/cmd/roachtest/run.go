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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
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

		// This will suppress the annoying "Allow incoming network connections" popup from
		// OSX when running a roachtest
		bindTo = "localhost"

		fmt.Printf("--local specified. Binding http listener to localhost only")
		if parallelism != 1 {
			fmt.Printf("--local specified. Overriding --parallelism to 1.\n")
			parallelism = 1
		}
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
	if _, err := log.ApplyConfig(logConf); err != nil {
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
