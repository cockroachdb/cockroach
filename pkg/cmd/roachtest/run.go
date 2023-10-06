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
	"net/http"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"runtime"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
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

var (
	username string

	parallelism int
	cpuQuota    int

	// Path to a local dir where the test logs and artifacts collected from
	// cluster will be placed.
	artifactsDir string

	// Path to the literal on-agent directory where artifacts are stored.
	// May be different from `artifacts`. Only used for messages to
	// ##teamcity[publishArtifacts] in Teamcity mode.
	literalArtifactsDir string

	httpPort int
	promPort int

	debugOnFailure         bool
	debugAlways            bool
	runSkipped             bool
	skipInit               bool
	goCoverEnabled         bool
	clusterID              string
	count                  int
	versionsBinaryOverride map[string]string

	selectProbability float64
)

// addRunBenchCommonFlags adds flags that are used for the run and bench commands.
// It is called with runCmd and benchCmd.
func addRunBenchCommonFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(
		&clusterName, "cluster", "c", "",
		"Comma-separated list of names existing cluster to use for running tests. "+
			"If fewer than --parallelism names are specified, then the parallelism "+
			"is capped to the number of clusters specified. When a cluster does not exist "+
			"yet, it is created according to the spec.")
	cmd.Flags().BoolVarP(
		&local, "local", "l", local, "run tests locally")
	cmd.Flags().StringVarP(
		&username, "user", "u", os.Getenv("ROACHPROD_USER"),
		"Username to use as a cluster name prefix. "+
			"If blank, the current OS user is detected and specified.")
	cmd.Flags().StringVar(
		&cockroachPath, "cockroach", "", "absolute path to cockroach binary to use")
	cmd.Flags().StringVar(
		&cockroachEAPath, "cockroach-ea", "", "absolute path to cockroach binary with enabled (runtime) assertions (i.e, compiled with crdb_test)")
	cmd.Flags().StringVar(
		&workloadPath, "workload", "", "absolute path to workload binary to use")
	cmd.Flags().Float64Var(
		&encryptionProbability, "metamorphic-encryption-probability", defaultEncryptionProbability,
		"probability that clusters will be created with encryption-at-rest enabled "+
			"for tests that support metamorphic encryption (default 1.0)")
	cmd.Flags().Float64Var(
		&fipsProbability, "metamorphic-fips-probability", defaultFIPSProbability,
		"conditional probability that amd64 clusters will be created with FIPS, i.e., P(fips | amd64), "+
			"for tests that support FIPS and whose CPU architecture is 'amd64' (default 0) "+
			"NOTE: amd64 clusters are created with probability 1-P(arm64), where P(arm64) is 'metamorphic-arm64-probability'. "+
			"Hence, P(fips | amd64) = P(fips) * (1 - P(arm64))")
	cmd.Flags().Float64Var(
		&arm64Probability, "metamorphic-arm64-probability", defaultARM64Probability,
		"probability that clusters will be created with 'arm64' CPU architecture "+
			"for tests that support 'arm64' (default 0)")

	cmd.Flags().StringVar(
		&artifactsDir, "artifacts", "artifacts", "path to artifacts directory")
	cmd.Flags().StringVar(
		&literalArtifactsDir, "artifacts-literal", "", "literal path to on-agent artifacts directory. Used for messages to ##teamcity[publishArtifacts] in --teamcity mode. May be different from --artifacts; defaults to the value of --artifacts if not provided")
	cmd.Flags().StringVar(
		&cloud, "cloud", cloud, "cloud provider to use (local, aws, azure, or gce)")
	cmd.Flags().StringVar(
		&clusterID, "cluster-id", "", "an identifier to use in the name of the test cluster(s)")
	cmd.Flags().IntVar(
		&count, "count", 1, "the number of times to run each test")
	cmd.Flags().BoolVarP(
		&debugOnFailure, "debug", "d", debugOnFailure, "don't wipe and destroy cluster if test fails")
	cmd.Flags().BoolVar(
		&debugAlways, "debug-always", debugAlways, "never wipe and destroy the cluster")
	cmd.Flags().BoolVar(
		&runSkipped, "run-skipped", runSkipped, "run skipped tests")
	cmd.Flags().BoolVar(
		&skipInit, "skip-init", false, "skip initialization step (imports, table creation, etc.) for tests that support it, useful when re-using clusters with --wipe=false")
	cmd.Flags().BoolVar(
		&goCoverEnabled, "go-cover", false, "enable collection of go coverage profiles (requires instrumented cockroach binary)")
	cmd.Flags().IntVarP(
		&parallelism, "parallelism", "p", 10, "number of tests to run in parallel")
	cmd.Flags().StringVar(
		&deprecatedRoachprodBinary, "roachprod", "", "DEPRECATED")
	_ = cmd.Flags().MarkDeprecated("roachprod", "roachtest now uses roachprod as a library")
	cmd.Flags().BoolVar(
		&clusterWipe, "wipe", true,
		"wipe existing cluster before starting test (for use with --cluster)")
	cmd.Flags().StringVar(
		&zonesF, "zones", "",
		"Zones for the cluster. (non-geo tests use the first zone, geo tests use all zones) "+
			"(uses roachprod defaults if empty)")
	cmd.Flags().StringVar(
		&instanceType, "instance-type", instanceType,
		"the instance type to use (see https://aws.amazon.com/ec2/instance-types/, https://cloud.google.com/compute/docs/machine-types or https://docs.microsoft.com/en-us/azure/virtual-machines/windows/sizes)")
	cmd.Flags().IntVar(
		&cpuQuota, "cpu-quota", 300,
		"The number of cloud CPUs roachtest is allowed to use at any one time.")
	cmd.Flags().IntVar(
		&httpPort, "port", 8080, "the port on which to serve the HTTP interface")
	cmd.Flags().BoolVar(
		&localSSDArg, "local-ssd", true, "Use a local SSD instead of an EBS volume (only for use with AWS) (defaults to true if instance type supports local SSDs)")
	cmd.Flags().StringToStringVar(
		&versionsBinaryOverride, "versions-binary-override", nil,
		"List of <version>=<path to cockroach binary>. If a certain version <ver> "+
			"is present in the list, the respective binary will be used when a "+
			"mixed-version test asks for the respective binary, instead of "+
			"`roachprod stage <ver>`. Example: 20.1.4=cockroach-20.1,20.2.0=cockroach-20.2.")
	cmd.Flags().BoolVar(
		&forceCloudCompat, "force-cloud-compat", false, "Includes tests that are not marked as compatible with the cloud used")
	addSuiteAndOwnerFlags(cmd)
}

func addRunFlags(runCmd *cobra.Command) {
	addRunBenchCommonFlags(runCmd)

	runCmd.Flags().StringVar(
		&slackToken, "slack-token", "", "Slack bot token")
	runCmd.Flags().BoolVar(
		&teamCity, "teamcity", false, "include teamcity-specific markers in output")
	runCmd.Flags().BoolVar(
		&disableIssue, "disable-issue", false, "disable posting GitHub issue for failures")
	runCmd.Flags().IntVar(
		&promPort, "prom-port", 2113,
		"the http port on which to expose prom metrics from the roachtest process")
	runCmd.Flags().Float64Var(
		&selectProbability, "select-probability", 1.0,
		"the probability of a matched test being selected to run. Note: this will return at least one test per prefix.")
}

func addBenchFlags(benchCmd *cobra.Command) {
	addRunBenchCommonFlags(benchCmd)
}

// runTests is the main function for the run and bench commands.
// Assumes initRunFlagsBinariesAndLibraries was called.
func runTests(register func(registry.Registry), filter *registry.TestFilter) error {
	r := makeTestRegistry(cloud, instanceType, zonesF, localSSDArg)

	// actual registering of tests
	// TODO: don't register if we can't run on the specified registry cloud
	register(&r)
	cr := newClusterRegistry()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	runner := newTestRunner(cr, stopper)

	clusterType := roachprodCluster
	bindTo := ""
	if cloud == spec.Local {
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
		clusterName: clusterName,
		// Precedence for resolving the user: cli arg, env.ROACHPROD_USER, current user.
		user:      getUser(username),
		cpuQuota:  cpuQuota,
		clusterID: clusterID,
	}
	switch {
	case debugAlways:
		opt.debugMode = DebugKeepAlways
	case debugOnFailure:
		opt.debugMode = DebugKeepOnFailure
	default:
		opt.debugMode = NoDebug
	}

	if err := runner.runHTTPServer(httpPort, os.Stdout, bindTo); err != nil {
		return err
	}

	specs, err := testsToRun(r, filter, runSkipped, selectProbability, true)
	if err != nil {
		return err
	}

	n := len(specs)
	if n*count < parallelism {
		// Don't spin up more workers than necessary. This has particular
		// implications for the common case of running a single test once: if
		// parallelism is set to 1, we'll use teeToStdout below to get logs to
		// stdout/stderr.
		parallelism = n * count
	}
	if opt.debugMode == DebugKeepAlways && n > 1 {
		return errors.Newf("--debug-always is only allowed when running a single test")
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
	go func() {
		if err := http.ListenAndServe(
			fmt.Sprintf(":%d", promPort),
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
		ctx, specs, count, parallelism, opt,
		testOpts{
			versionsBinaryOverride: versionsBinaryOverride,
			skipInit:               skipInit,
			goCoverEnabled:         goCoverEnabled,
		},
		lopt)

	// Make sure we attempt to clean up. We run with a non-canceled ctx; the
	// ctx above might be canceled in case a signal was received. If that's
	// the case, we're running under a 5s timeout until the CtrlC() goroutine
	// kills the process.
	l.PrintfCtx(ctx, "runTests destroying all clusters")
	cr.destroyAllClusters(context.Background(), l)

	if teamCity {
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
	if local {
		if clusterName != "" {
			return fmt.Errorf(
				"cannot specify both an existing cluster (%s) and --local. However, if a local cluster "+
					"already exists, --clusters=local will use it",
				clusterName)
		}
		cloud = spec.Local
	}

	if count <= 0 {
		return fmt.Errorf("--count (%d) must by greater than 0", count)
	}
	if literalArtifactsDir == "" {
		literalArtifactsDir = artifactsDir
	}

	if !(0 <= arm64Probability && arm64Probability <= 1) {
		return fmt.Errorf("'metamorphic-arm64-probability' must be in [0,1]")
	}
	if !(0 <= fipsProbability && fipsProbability <= 1) {
		return fmt.Errorf("'metamorphic-fips-probability' must be in [0,1]")
	}
	if arm64Probability == 1 && fipsProbability != 0 {
		return fmt.Errorf("'metamorphic-fips-probability' must be 0 when 'metamorphic-arm64-probability' is 1")
	}
	if fipsProbability == 1 && arm64Probability != 0 {
		return fmt.Errorf("'metamorphic-arm64-probability' must be 0 when 'metamorphic-fips-probability' is 1")
	}
	if !(0 <= selectProbability && selectProbability <= 1) {
		return fmt.Errorf("'select-probability' must be in [0,1]")
	}
	arm64Opt := cmd.Flags().Lookup("metamorphic-arm64-probability")
	if !arm64Opt.Changed && runtime.GOARCH == "arm64" && cloud == spec.Local {
		fmt.Printf("Detected 'arm64' in 'local mode', setting 'metamorphic-arm64-probability' to 1; use --metamorphic-arm64-probability to run (emulated) with other binaries\n")
		arm64Probability = 1
	}
	// Find and validate all required binaries and libraries.
	initBinariesAndLibraries()

	if arm64Probability > 0 {
		fmt.Printf("ARM64 clusters will be provisioned with probability %.2f\n", arm64Probability)
	}
	amd64Probability := 1 - arm64Probability
	if amd64Probability > 0 {
		fmt.Printf("AMD64 clusters will be provisioned with probability %.2f\n", amd64Probability)
	}
	if fipsProbability > 0 {
		// N.B. arm64Probability < 1, otherwise fipsProbability == 0, as per above check.
		// Hence, amd64Probability > 0 is implied.
		fmt.Printf("FIPS clusters will be provisioned with probability %.2f\n", fipsProbability*amd64Probability)
	}

	if selectProbability > 0 {
		fmt.Printf("Matching tests will be selected with probability %.2f\n", selectProbability)
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
