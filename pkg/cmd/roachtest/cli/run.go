// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV1"
	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost/issues"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/testselector"
	"github.com/cockroachdb/cockroach/pkg/internal/team"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/allstacks"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/version"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
)

type ddEventType int

const (
	eventOpStarted ddEventType = iota
	eventOpRan
	eventOpFinishedCleanup
	eventOpError
)

// runTests is the main function for the run and bench commands.
// Assumes initRunFlagsBinariesAndLibraries was called.
func runTests(register func(registry.Registry), filter *registry.TestFilter) error {
	// On Darwin, start caffeinate to prevent the system from sleeping.
	if runtime.GOOS == "darwin" && roachtestflags.Caffeinate {
		pid := os.Getpid()
		cmd := exec.Command("caffeinate", "-i", "-w", strconv.Itoa(pid))
		if err := cmd.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to start caffeinate: %v\n", err)
		} else {
			defer func() {
				if cmd.Process != nil {
					_ = cmd.Process.Kill()
				}
			}()
		}
	}

	globalSeed := randutil.NewPseudoSeed()
	if globalSeedEnv := os.Getenv("ROACHTEST_GLOBAL_SEED"); globalSeedEnv != "" {
		if parsed, err := strconv.ParseInt(globalSeedEnv, 0, 64); err == nil {
			globalSeed = parsed
		} else {
			return errors.Wrapf(err, "could not parse ROACHTEST_GLOBAL_SEED=%q", globalSeedEnv)
		}
	}
	//lint:ignore SA1019 deprecated
	rand.Seed(globalSeed)
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

	artifactsDir := roachtestflags.ArtifactsDir
	literalArtifactsDir := roachtestflags.LiteralArtifactsDir
	if literalArtifactsDir == "" {
		literalArtifactsDir = artifactsDir
	}
	redirectLogger := redirectCRDBLogger(context.Background(), filepath.Join(artifactsDir, "roachtest.crdb.log"))
	logger.InitCRDBLogConfig(redirectLogger)
	runnerDir := filepath.Join(artifactsDir, runnerLogsDir)
	runnerLogPath := filepath.Join(
		runnerDir, fmt.Sprintf("test_runner-%d.log", timeutil.Now().Unix()))
	l, tee := testRunnerLogger(context.Background(), parallelism, runnerLogPath)
	roachprod.ClearClusterCache = roachtestflags.ClearClusterCache

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

	if opt.debugMode == DebugKeepAlways && n > 1 {
		return errors.Newf("--debug-always is only allowed when running a single test")
	}

	lopt := loggingOpt{
		l:                   l,
		tee:                 tee,
		stdout:              os.Stdout,
		stderr:              os.Stderr,
		artifactsDir:        artifactsDir,
		literalArtifactsDir: literalArtifactsDir,
		runnerLogPath:       runnerLogPath,
	}

	github := &githubIssues{
		disable:     runner.config.disableIssue,
		dryRun:      runner.config.dryRunIssuePosting,
		issuePoster: issues.Post,
		teamLoader:  team.DefaultLoadTeams,
	}

	l.Printf("global random seed: %d", globalSeed)
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
	if false {
		// Install goroutine leak checker and run it at the end of the entire test
		// run. If a test is leaking a goroutine, then it will likely be still around.
		// We could diff goroutine snapshots before/after each executed test, but that
		// could yield false positives; e.g., user-specified test teardown goroutines
		// may still be running long after the test has completed.
		//
		// NB: we currently don't do this since it's been firing for a long time and
		// nobody has cleaned up the leaks. While there are leaks, the leaktest
		// output pollutes stdout and makes roachtest annoying to use.
		//
		// Tracking issue: https://github.com/cockroachdb/cockroach/issues/148196
		defer leaktest.AfterTest(l)()
	}

	// We allow roachprod users to set a default auth-mode through the
	// ROACHPROD_DEFAULT_AUTH_MODE env var. However, roachtests shouldn't
	// use this feature in order to minimize confusion over which auth-mode
	// is being used.
	if err = os.Unsetenv(install.DefaultAuthModeEnv); err != nil {
		return err
	}

	err = runner.Run(
		ctx, specs, roachtestflags.Count, parallelism, opt,
		testOpts{
			versionsBinaryOverride: roachtestflags.VersionsBinaryOverride,
			skipInit:               roachtestflags.SkipInit,
			goCoverEnabled:         roachtestflags.GoCoverEnabled,
			exportOpenMetrics:      roachtestflags.ExportOpenmetrics,
		},
		lopt,
		github)

	// Make sure we attempt to clean up. We run with a non-canceled ctx; the
	// ctx above might be canceled in case a signal was received. If that's
	// the case, we're running under a 5s timeout until the CtrlC() goroutine
	// kills the process.
	l.PrintfCtx(ctx, "runTests destroying all clusters")
	cr.destroyAllClusters(context.Background(), l)

	if roachtestflags.TeamCity {
		// Collect the runner logs.
		fmt.Printf("##teamcity[publishArtifacts '%s' => '%s']\n", filepath.Join(literalArtifactsDir, runnerLogsDir), runnerLogsDir)
	}
	runner.writeTestReports(ctx, l, artifactsDir)

	return err
}

func skipDetails(spec *registry.TestSpec) string {
	if spec.Skip == "" {
		return ""
	}
	details := spec.Skip
	if spec.SkipDetails != "" {
		details += " (" + spec.SkipDetails + ")"
	}
	return details
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
	if !(0 <= roachtestflags.CockroachEAProbability && roachtestflags.CockroachEAProbability <= 1) {
		return fmt.Errorf("'metamorphic-cockroach-ea-probability' must be in [0,1]")
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

	if roachtestflags.Cloud == spec.IBM {
		fmt.Printf("S390x clusters will be provisioned with probability 1\n")
		if roachtestflags.ARM64Probability > 0 || roachtestflags.FIPSProbability > 0 {
			fmt.Printf("Warning: despite --metamorphic-(arm64|fips)-probability argument, ARM64 and FIPS clusters will not be provisioned on IBM Cloud!\n")
		}
	} else {
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
	}

	if roachtestflags.SelectProbability > 0 && roachtestflags.SelectProbability < 1 {
		fmt.Printf("Matching tests will be selected with probability %.2f\n", roachtestflags.SelectProbability)
	}

	for override := range roachtestflags.VersionsBinaryOverride {
		if _, err := version.Parse(override); err != nil {
			return errors.Wrapf(err, "binary version override %s is not a valid version", override)
		}
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
		select {
		case <-sig:
		case <-ctx.Done():
			return
		}
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

func redirectCRDBLogger(ctx context.Context, path string) *logger.Logger {
	verboseCfg := logger.Config{}
	var err error
	l, err := verboseCfg.NewLogger(path)
	if err != nil {
		panic(err)
	}
	shout(ctx, l, os.Stdout, "fallback runner logs in: %s", path)
	return l
}

// maybeEmitDatadogEvent sends an event to Datadog if the passed in ctx has the
// necessary values to communicate with Datadog.
func maybeEmitDatadogEvent(
	ctx context.Context,
	datadogEventsAPI *datadogV1.EventsApi,
	opSpec *registry.OperationSpec,
	clusterName string,
	eventType ddEventType,
	operationID uint64,
	datadogTags []string,
) {
	// The passed in context is not configured to communicate with Datadog.
	_, hasAPIKeys := ctx.Value(datadog.ContextAPIKeys).(map[string]datadog.APIKey)
	_, hasServerVariables := ctx.Value(datadog.ContextServerVariables).(map[string]string)
	if !hasAPIKeys || !hasServerVariables {
		return
	}

	status := "started"
	alertType := datadogV1.EVENTALERTTYPE_INFO

	switch eventType {
	case eventOpStarted:
		status = "started"
		alertType = datadogV1.EVENTALERTTYPE_INFO
	case eventOpRan:
		status = "finished running; waiting for cleanup"
		alertType = datadogV1.EVENTALERTTYPE_SUCCESS
	case eventOpFinishedCleanup:
		status = "cleaned up its state"
		alertType = datadogV1.EVENTALERTTYPE_INFO
	case eventOpError:
		status = "ran with an error"
		alertType = datadogV1.EVENTALERTTYPE_ERROR
	}

	title := fmt.Sprintf("op %s %s", opSpec.Name, status)
	hostname, _ := os.Hostname()

	// We're within a best effort function so we ignore return values.
	_, _, _ = datadogEventsAPI.CreateEvent(ctx, datadogV1.EventCreateRequest{
		AggregationKey: datadog.PtrString(fmt.Sprintf("operation-%d", operationID)),
		AlertType:      &alertType,
		DateHappened:   datadog.PtrInt64(timeutil.Now().Unix()),
		Host:           &hostname,
		SourceTypeName: datadog.PtrString("roachtest"),
		Tags: append(datadogTags,
			fmt.Sprintf("operation-name:%s", opSpec.Name),
			fmt.Sprintf("operation-status:%s", status),
		),
		Text:  fmt.Sprintf("cluster: %s\n", clusterName),
		Title: title,
	})
}

// newDatadogContext adds values to the passed in ctx to configure it to
// communicate with Datadog. If the necessary values to communicate with
// Datadog are not present the context is returned without values added to it.
func newDatadogContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}

	datadogSite := roachtestflags.DatadogSite
	if datadogSite == "" {
		datadogSite = os.Getenv("DD_SITE")
	}

	datadogAPIKey := roachtestflags.DatadogAPIKey
	if datadogAPIKey == "" {
		datadogAPIKey = os.Getenv("DD_API_KEY")
	}

	datadogApplicationKey := roachtestflags.DatadogApplicationKey
	if datadogApplicationKey == "" {
		datadogApplicationKey = os.Getenv("DD_APP_KEY")
	}

	// There isn't enough information to configure the context to communicate
	// with Datadog.
	if datadogSite == "" || datadogAPIKey == "" || datadogApplicationKey == "" {
		return ctx
	}

	ctx = context.WithValue(
		ctx,
		datadog.ContextAPIKeys,
		map[string]datadog.APIKey{
			"apiKeyAuth": {
				Key: datadogAPIKey,
			},
			"appKeyAuth": {
				Key: datadogApplicationKey,
			},
		},
	)

	ctx = context.WithValue(ctx,
		datadog.ContextServerVariables,
		map[string]string{
			"site": datadogSite,
		},
	)

	return ctx
}

// getDatadogTags retrieves the Datadog tags from the datadog-tags CLI
// argument, falling back to the DD_TAGS environment variable if empty.
func getDatadogTags() []string {
	rawTags := roachtestflags.DatadogTags
	if rawTags == "" {
		rawTags = os.Getenv("DD_TAGS")
	}

	if rawTags == "" {
		return []string{}
	}

	return strings.Split(rawTags, ",")
}

// testsToRun determines which tests should be executed based on the filter
// and configuration flags.
func testsToRun(
	r testRegistryImpl,
	filter *registry.TestFilter,
	runSkipped bool,
	selectProbability float64,
	print bool,
) ([]registry.TestSpec, error) {
	specs, hint := filter.FilterWithHint(r.AllTests())
	if len(specs) == 0 {
		msg := filter.NoMatchesHintString(hint)
		if hint == registry.IncompatibleCloud {
			msg += "\nTo include tests that are not compatible with this cloud, use --force-cloud-compat."
		}
		return nil, errors.Newf("%s", msg)
	}

	if roachtestflags.SelectiveTests {
		fmt.Printf("selective Test enabled\n")
		// the test categorization must be complete in 30 seconds
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		updateSpecForSelectiveTests(ctx, specs, func(format string, args ...interface{}) {
			fmt.Fprintf(os.Stdout, format, args...)
		})
	}

	var notSkipped []registry.TestSpec
	for _, s := range specs {
		if s.Skip == "" || runSkipped {
			notSkipped = append(notSkipped, s)
		} else {
			if print && roachtestflags.TeamCity {
				fmt.Fprintf(os.Stdout, "##teamcity[testIgnored name='%s' message='%s']\n",
					s.Name, TeamCityEscape(s.Skip))
			}
			skipDetails := s.Skip
			if skipDetails != "" {
				skipDetails = " (" + s.SkipDetails + ")"
			}
			if print {
				fmt.Fprintf(os.Stdout, "--- SKIP: %s (%s)\n\t%s\n", s.Name, "0.00s", skipDetails)
			}
		}
	}

	if print {
		// We want to show information about all tests/benchmarks which match the
		// pattern(s) but were excluded for other reasons.
		relaxedFilter := registry.TestFilter{
			Name:           filter.Name,
			OnlyBenchmarks: filter.OnlyBenchmarks,
		}
		for _, s := range relaxedFilter.Filter(r.AllTests()) {
			if matches, r := filter.Matches(&s); !matches {
				reason := filter.MatchFailReasonString(r)
				// This test matches the "relaxed" filter but not the original filter.
				if roachtestflags.TeamCity {
					fmt.Fprintf(os.Stdout, "##teamcity[testIgnored name='%s' message='%s']\n", s.Name, reason)
				}
				fmt.Fprintf(os.Stdout, "--- SKIP: %s (%s)\n\t%s\n", s.Name, "0.00s", reason)
			}
		}
	}

	var stdout io.Writer
	if print {
		stdout = os.Stdout
	}
	rng, _ := randutil.NewPseudoRand()
	return selectSpecs(notSkipped, rng, selectProbability, true, stdout), nil
}

// updateSpecForSelectiveTests is responsible for updating the test spec skip and skip details
// based on the test categorization criteria.
// The following steps are performed in this function:
//  1. Queries Snowflake for the test run data.
//  2. The snowflake data sets "selected=true" based on the following criteria:
//     a. the test that has failed at least once in last 30 days
//     b. the test is newer than 20 days
//     c. the test has not been run for more than 7 days
//  2. The rest of the tests returned by snowflake are the successful tests marked as "selected=false".
//  3. Now, an intersection of the tests that are selected by the build (specs) and tests returned by snowflake
//     as successful is taken. This is done to select tests on the next criteria of selecting the 35% of
//     the successful tests.
//  4. The tests that meet the 35% criteria, are marked as "selected=true"
//  5. All tests that are marked "selected=true" are considered for the test run.
func updateSpecForSelectiveTests(
	ctx context.Context, specs []registry.TestSpec, logFunc func(format string, args ...interface{}),
) {
	selectedTestsCount := 0
	allTests, err := testselector.CategoriseTests(ctx,
		testselector.NewDefaultSelectTestsReq(roachtestflags.Cloud, roachtestflags.Suite))
	if err != nil {
		logFunc("running all tests! error selecting tests: %v\n", err)
		return
	}

	// successfulTests are the tests considered by snowflake to not run, but, part of the testSpecs.
	// So, it is an intersection of all tests that are part of the run and all tests that are returned
	// by snowflake as successful.
	// This is why we need the intersection:
	// - testSpec contains all the tests that are currently considered as a part of the current run.
	// - The list of tests returned by selector can contain tests may not be part of the test spec. This can
	//   be because of tests getting decommissioned.
	// Now, we want to take the tests common to both. These are the tests from which we need to select
	// "successfulTestsSelectPct" percent tests to run.
	successfulTests := make([]*testselector.TestDetails, 0)

	// allTestsMap is maintained to check for the test details while skipping a test
	allTestsMap := make(map[string]*testselector.TestDetails)
	// all tests from specs are added as nil to the map
	// this is used in identifying the tests that are part of the build
	for _, test := range specs {
		allTestsMap[test.Name] = nil
	}
	for i := 0; i < len(allTests); i++ {
		td := allTests[i]
		if _, ok := allTestsMap[td.Name]; ok && !td.Selected {
			// adding only the unselected tests that are part of the specs
			// These are tests that have been running successfully
			successfulTests = append(successfulTests, td)
		}
		// populate the test details for the tests returned from snowflake
		allTestsMap[td.Name] = td
	}
	// numberOfTestsToSelect is the number of tests to be selected from the successfulTests based on percentage selection
	numberOfTestsToSelect := int(math.Ceil(float64(len(successfulTests)) * roachtestflags.SuccessfulTestsSelectPct))
	for i := 0; i < numberOfTestsToSelect; i++ {
		successfulTests[i].Selected = true
	}
	logFunc("%d selected out of %d successful tests.\n", numberOfTestsToSelect, len(successfulTests))
	for i := range specs {
		if testShouldBeSkipped(allTestsMap, specs[i], roachtestflags.Suite) {
			if specs[i].Skip == "" {
				// updating only if the test not already skipped
				specs[i].Skip = "test selector"
				specs[i].SkipDetails = "test skipped because it is stable and selective-tests is set."
			}
		} else {
			selectedTestsCount++
		}
		if td, ok := allTestsMap[specs[i].Name]; ok && td != nil {
			// populate the stats as obtained from the test selector
			specs[i].SetStats(td.AvgDurationInMillis, td.LastFailureIsPreempt)
		}
	}
	logFunc("%d out of %d tests selected for the run!\n", selectedTestsCount, len(specs))
}

// testShouldBeSkipped decides whether a test should be skipped based on test details and suite
func testShouldBeSkipped(
	testNamesToRun map[string]*testselector.TestDetails, test registry.TestSpec, suite string,
) bool {
	if test.Randomized {
		return false
	}

	for test.TestSelectionOptOutSuites.IsInitialized() && test.TestSelectionOptOutSuites.Contains(suite) {
		// test should not be skipped for this suite
		return false
	}

	td := testNamesToRun[test.Name]
	return td != nil && !td.Selected
}

// selectSpecs returns a random sample of the given test specs.
// If atLeastOnePerPrefix is true, it guarantees that at least one test is
// selected for each prefix (e.g. kv0/, acceptance/).
// This assumes that specs are sorted by name, which is the case for
// testRegistryImpl.AllTests().
// TODO(smg260): Perhaps expose `atLeastOnePerPrefix` via CLI
func selectSpecs(
	specs []registry.TestSpec,
	rng *rand.Rand,
	samplePct float64,
	atLeastOnePerPrefix bool,
	stdout io.Writer,
) []registry.TestSpec {
	if samplePct == 1 || len(specs) == 0 {
		return specs
	}

	var sampled []registry.TestSpec
	selectedIndexes := make(map[int]struct{})

	prefix := strings.Split(specs[0].Name, "/")[0]
	prefixSelected := false
	prefixIdx := 0

	// Selects one random spec from the range [start, end) and appends it to sampled.
	collectRandomSpecFromRange := func(start, end int) {
		i := start + rng.Intn(end-start)
		sampled = append(sampled, specs[i])
		selectedIndexes[i] = struct{}{}
	}
	for i, s := range specs {
		if atLeastOnePerPrefix {
			currPrefix := strings.Split(s.Name, "/")[0]
			// New prefix. Check we've at least one selected test for the previous prefix.
			if currPrefix != prefix {
				if !prefixSelected {
					collectRandomSpecFromRange(prefixIdx, i)
				}
				prefix = currPrefix
				prefixIdx = i
				prefixSelected = false
			}
		}

		if rng.Float64() < samplePct {
			sampled = append(sampled, s)
			selectedIndexes[i] = struct{}{}
			prefixSelected = true
			continue
		}

		if atLeastOnePerPrefix && i == len(specs)-1 && !prefixSelected {
			// i + 1 since we want to include the last element
			collectRandomSpecFromRange(prefixIdx, i+1)
		}
	}

	// Print a skip message for all tests that are not selected.
	for i, s := range specs {
		if _, ok := selectedIndexes[i]; !ok {
			if stdout != nil && roachtestflags.TeamCity {
				fmt.Fprintf(stdout, "##teamcity[testIgnored name='%s' message='excluded via sampling']\n",
					s.Name)
			}

			if stdout != nil {
				fmt.Fprintf(stdout, "--- SKIP: %s (%s)\n\texcluded via sampling\n", s.Name, "0.00s")
			}
		}
	}

	return sampled
}
