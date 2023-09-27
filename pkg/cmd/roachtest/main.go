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
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"runtime"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/tests"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	_ "github.com/lib/pq" // register postgres driver
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// ExitCodeTestsFailed is the exit code that results from a run of
// roachtest in which the infrastructure worked, but at least one
// test failed.
const ExitCodeTestsFailed = 10

// ExitCodeClusterProvisioningFailed is the exit code that results
// from a run of roachtest in which some clusters could not be
// created due to errors during cloud hardware allocation.
const ExitCodeClusterProvisioningFailed = 11

// runnerLogsDir is the dir under the artifacts root where the test runner log
// and other runner-related logs (i.e. cluster creation logs) will be written.
const runnerLogsDir = "_runner-logs"

// Only used if passed otherwise refer to ClusterSpec.
// If a new flag is added here it should also be added to createFlagsOverride().
func parseCreateOpts(flags *pflag.FlagSet, opts *vm.CreateOpts) {
	// roachprod create flags
	flags.DurationVar(&opts.Lifetime,
		"lifetime", opts.Lifetime, "Lifetime of the cluster")
	flags.BoolVar(&opts.SSDOpts.UseLocalSSD,
		"roachprod-local-ssd", opts.SSDOpts.UseLocalSSD, "Use local SSD")
	flags.StringVar(&opts.SSDOpts.FileSystem,
		"filesystem", opts.SSDOpts.FileSystem, "The underlying file system(ext4/zfs).")
	flags.BoolVar(&opts.SSDOpts.NoExt4Barrier,
		"local-ssd-no-ext4-barrier", opts.SSDOpts.NoExt4Barrier,
		`Mount the local SSD with the "-o nobarrier" flag. `+
			`Ignored if --local-ssd=false is specified.`)
	flags.IntVarP(&overrideNumNodes,
		"nodes", "n", -1, "Total number of nodes")
	flags.IntVarP(&opts.OsVolumeSize,
		"os-volume-size", "", opts.OsVolumeSize, "OS disk volume size in GB")
	flags.BoolVar(&opts.GeoDistributed,
		"geo", opts.GeoDistributed, "Create geo-distributed cluster")
}

func main() {
	rand.Seed(timeutil.Now().UnixNano())
	username := os.Getenv("ROACHPROD_USER")
	parallelism := 10
	var cpuQuota int
	// Path to a local dir where the test logs and artifacts collected from
	// cluster will be placed.
	var artifacts string
	// Path to the literal on-agent directory where artifacts are stored.
	// May be different from `artifacts`. Only used for messages to
	// ##teamcity[publishArtifacts] in Teamcity mode.
	var literalArtifacts string
	var httpPort int
	var debugEnabled bool
	var runSkipped bool
	var skipInit bool
	var clusterID string
	var count = 1
	var versionsBinaryOverride map[string]string

	cobra.EnableCommandSorting = false

	var rootCmd = &cobra.Command{
		Use:   "roachtest [command] (flags)",
		Short: "roachtest tool for testing cockroach clusters",
		Long: `roachtest is a tool for testing cockroach clusters.
`,
		Version: "details:\n" + build.GetInfo().Long(),
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			// Don't bother checking flags for the default help command.
			if cmd.Name() == "help" {
				return nil
			}
			local := cmd.Flags().Lookup("local").Value.String() == "true"
			if local {
				if clusterName != "" {
					return fmt.Errorf(
						"cannot specify both an existing cluster (%s) and --local. However, if a local cluster "+
							"already exists, --clusters=local will use it",
						clusterName)
				}
				cloud = spec.Local
			}
			switch cmd.Name() {
			case "run", "bench", "store-gen":
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
			}
			return nil
		},
	}

	rootCmd.PersistentFlags().StringVarP(
		&clusterName, "cluster", "c", "",
		"Comma-separated list of names existing cluster to use for running tests. "+
			"If fewer than --parallelism names are specified, then the parallelism "+
			"is capped to the number of clusters specified. When a cluster does not exist "+
			"yet, it is created according to the spec.")
	var local bool
	rootCmd.PersistentFlags().BoolVarP(
		&local, "local", "l", local, "run tests locally")
	rootCmd.PersistentFlags().StringVarP(
		&username, "user", "u", username,
		"Username to use as a cluster name prefix. "+
			"If blank, the current OS user is detected and specified.")
	rootCmd.PersistentFlags().StringVar(
		&cockroachPath, "cockroach", "", "absolute path to cockroach binary to use")
	rootCmd.PersistentFlags().StringVar(
		&cockroachEAPath, "cockroach-ea", "", "absolute path to cockroach binary with enabled (runtime) assertions (i.e, compiled with crdb_test)")
	rootCmd.PersistentFlags().StringVar(
		&workloadPath, "workload", "", "absolute path to workload binary to use")
	rootCmd.PersistentFlags().Float64Var(
		&encryptionProbability, "metamorphic-encryption-probability", defaultEncryptionProbability,
		"probability that clusters will be created with encryption-at-rest enabled "+
			"for tests that support metamorphic encryption (default 1.0)")
	rootCmd.PersistentFlags().Float64Var(
		&fipsProbability, "metamorphic-fips-probability", defaultFIPSProbability,
		"conditional probability that amd64 clusters will be created with FIPS, i.e., P(fips | amd64), "+
			"for tests that support FIPS and whose CPU architecture is 'amd64' (default 0) "+
			"NOTE: amd64 clusters are created with probability 1-P(arm64), where P(arm64) is 'metamorphic-arm64-probability'. "+
			"Hence, P(fips | amd64) = P(fips) * (1 - P(arm64))")
	rootCmd.PersistentFlags().Float64Var(
		&arm64Probability, "metamorphic-arm64-probability", defaultARM64Probability,
		"probability that clusters will be created with 'arm64' CPU architecture "+
			"for tests that support 'arm64' (default 0)")

	rootCmd.AddCommand(&cobra.Command{
		Use:   `version`,
		Short: `print version information`,
		RunE: func(cmd *cobra.Command, args []string) error {
			info := build.GetInfo()
			fmt.Println(info.Long())
			return nil
		}},
	)

	var listBench bool

	var listCmd = &cobra.Command{
		Use:   "list [tests]",
		Short: "list tests matching the patterns",
		Long: `List tests that match the given name patterns.

If no pattern is passed, all tests are matched.
Use --bench to list benchmarks instead of tests.

Each test has a set of tags. The tags are used to skip tests which don't match
the tag filter. The tag filter is specified by specifying a pattern with the
"tag:" prefix. 

If multiple "tag:" patterns are specified, the test must match at
least one of them.

Within a single "tag:" pattern, multiple tags can be specified by separating them
with a comma. In this case, the test must match all of the tags.

Examples:

   roachtest list acceptance copy/bank/.*false
   roachtest list tag:owner-kv
   roachtest list tag:weekly

   # match weekly kv owned tests
   roachtest list tag:owner-kv,weekly

   # match weekly kv owner tests or aws tests
   roachtest list tag:owner-kv,weekly tag:aws
`,
		RunE: func(_ *cobra.Command, args []string) error {
			r, err := makeTestRegistry(cloud, instanceType, zonesF, localSSDArg, listBench)
			if err != nil {
				return err
			}
			tests.RegisterTests(&r)

			matchedTests := r.List(args)
			for _, test := range matchedTests {
				var skip string
				if test.Skip != "" && !runSkipped {
					skip = " (skipped: " + test.Skip + ")"
				}
				fmt.Printf("%s [%s]%s\n", test.Name, test.Owner, skip)
			}
			return nil
		},
	}
	listCmd.Flags().BoolVar(
		&listBench, "bench", false, "list benchmarks instead of tests")
	listCmd.Flags().StringVar(
		&cloud, "cloud", cloud, "cloud provider to use (aws, azure, or gce)")

	runFn := func(args []string, benchOnly bool) error {
		if literalArtifacts == "" {
			literalArtifacts = artifacts
		}
		return runTests(tests.RegisterTests, cliCfg{
			args:                   args,
			count:                  count,
			cpuQuota:               cpuQuota,
			debugEnabled:           debugEnabled,
			runSkipped:             runSkipped,
			skipInit:               skipInit,
			httpPort:               httpPort,
			parallelism:            parallelism,
			artifactsDir:           artifacts,
			literalArtifactsDir:    literalArtifacts,
			user:                   getUser(username),
			clusterID:              clusterID,
			versionsBinaryOverride: versionsBinaryOverride,
		}, benchOnly)
	}

	var runCmd = &cobra.Command{
		// Don't display usage when tests fail.
		SilenceUsage: true,
		Use:          "run [tests]",
		Short:        "run automated tests on cockroach cluster",
		Long: `Run automated tests on existing or ephemeral cockroach clusters.

roachtest run takes a list of regex patterns and runs all the matching tests.
If no pattern is given, all tests are run. See "help list" for more details on
the test tags.

If all invoked tests passed, the exit status is zero. If at least one test
failed, it is 10. Any other exit status reports a problem with the test
runner itself.
`,
		RunE: func(_ *cobra.Command, args []string) error {
			return runFn(args, false /* benchOnly */)
		},
	}

	// TODO(irfansharif): We could remove this by directly running `cockroach
	// version` against the binary being tested, instead of what we do today
	// which is defaulting to checking the last git release tag present in the
	// local checkout.
	runCmd.Flags().StringVar(
		&buildTag, "build-tag", "", "build tag (auto-detect if empty)")
	runCmd.Flags().StringVar(
		&slackToken, "slack-token", "", "Slack bot token")
	runCmd.Flags().BoolVar(
		&teamCity, "teamcity", false, "include teamcity-specific markers in output")
	runCmd.Flags().BoolVar(
		&disableIssue, "disable-issue", false, "disable posting GitHub issue for failures")

	var benchCmd = &cobra.Command{
		// Don't display usage when tests fail.
		SilenceUsage: true,
		Use:          "bench [benchmarks]",
		Short:        "run automated benchmarks on cockroach cluster",
		Long:         `Run automated benchmarks on existing or ephemeral cockroach clusters.`,
		RunE: func(_ *cobra.Command, args []string) error {
			return runFn(args, true /* benchOnly */)
		},
	}

	// Register flags shared between `run` and `bench`.
	for _, cmd := range []*cobra.Command{runCmd, benchCmd} {
		cmd.Flags().StringVar(
			&artifacts, "artifacts", "artifacts", "path to artifacts directory")
		cmd.Flags().StringVar(
			&literalArtifacts, "artifacts-literal", "", "literal path to on-agent artifacts directory. Used for messages to ##teamcity[publishArtifacts] in --teamcity mode. May be different from --artifacts; defaults to the value of --artifacts if not provided")
		cmd.Flags().StringVar(
			&cloud, "cloud", cloud, "cloud provider to use (aws, azure, or gce)")
		cmd.Flags().StringVar(
			&clusterID, "cluster-id", "", "an identifier to use in the test cluster's name")
		cmd.Flags().IntVar(
			&count, "count", 1, "the number of times to run each test")
		cmd.Flags().BoolVarP(
			&debugEnabled, "debug", "d", debugEnabled, "don't wipe and destroy cluster if test fails")
		cmd.Flags().BoolVar(
			&runSkipped, "run-skipped", runSkipped, "run skipped tests")
		cmd.Flags().BoolVar(
			&skipInit, "skip-init", false, "skip initialization step (imports, table creation, etc.) for tests that support it, useful when re-using clusters with --wipe=false")
		cmd.Flags().IntVarP(
			&parallelism, "parallelism", "p", parallelism, "number of tests to run in parallel")
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
				"is present in the list,"+"the respective binary will be used when a "+
				"multi-version test asks for the respective binary, instead of "+
				"`roachprod stage <ver>`. Example: 20.1.4=cockroach-20.1,20.2.0=cockroach-20.2.")
	}

	parseCreateOpts(runCmd.Flags(), &overrideOpts)
	overrideFlagset = runCmd.Flags()

	rootCmd.AddCommand(listCmd)
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(benchCmd)

	var err error
	config.OSUser, err = user.Current()
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to lookup current user: %s\n", err)
		os.Exit(1)
	}

	if err := roachprod.InitDirs(); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

	if err := rootCmd.Execute(); err != nil {
		code := 1
		if errors.Is(err, errTestsFailed) {
			code = ExitCodeTestsFailed
		}
		if errors.Is(err, errSomeClusterProvisioningFailed) {
			code = ExitCodeClusterProvisioningFailed
		}
		// Cobra has already printed the error message.
		os.Exit(code)
	}
}

type cliCfg struct {
	args                   []string
	count                  int
	cpuQuota               int
	debugEnabled           bool
	skipInit               bool
	runSkipped             bool
	httpPort               int
	parallelism            int
	artifactsDir           string
	literalArtifactsDir    string
	user                   string
	clusterID              string
	versionsBinaryOverride map[string]string
}

func runTests(register func(registry.Registry), cfg cliCfg, benchOnly bool) error {
	if cfg.count <= 0 {
		return fmt.Errorf("--count (%d) must by greater than 0", cfg.count)
	}
	r, err := makeTestRegistry(cloud, instanceType, zonesF, localSSDArg, benchOnly)
	if err != nil {
		return err
	}

	// actual registering of tests
	// TODO: don't register if we can't run on the specified registry cloud
	register(&r)
	cr := newClusterRegistry()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	runner := newTestRunner(cr, stopper, r.buildVersion)

	filter := registry.NewTestFilter(cfg.args, cfg.runSkipped)
	clusterType := roachprodCluster
	bindTo := ""
	if cloud == spec.Local {
		clusterType = localCluster

		// This will suppress the annoying "Allow incoming network connections" popup from
		// OSX when running a roachtest
		bindTo = "localhost"

		fmt.Printf("--local specified. Binding http listener to localhost only")
		if cfg.parallelism != 1 {
			fmt.Printf("--local specified. Overriding --parallelism to 1.\n")
			cfg.parallelism = 1
		}
	}

	opt := clustersOpt{
		typ:                       clusterType,
		clusterName:               clusterName,
		user:                      getUser(cfg.user),
		cpuQuota:                  cfg.cpuQuota,
		keepClustersOnTestFailure: cfg.debugEnabled,
		clusterID:                 cfg.clusterID,
	}
	if err := runner.runHTTPServer(cfg.httpPort, os.Stdout, bindTo); err != nil {
		return err
	}

	tests := testsToRun(r, filter)
	n := len(tests)
	if n*cfg.count < cfg.parallelism {
		// Don't spin up more workers than necessary. This has particular
		// implications for the common case of running a single test once: if
		// parallelism is set to 1, we'll use teeToStdout below to get logs to
		// stdout/stderr.
		cfg.parallelism = n * cfg.count
	}
	runnerDir := filepath.Join(cfg.artifactsDir, runnerLogsDir)
	runnerLogPath := filepath.Join(
		runnerDir, fmt.Sprintf("test_runner-%d.log", timeutil.Now().Unix()))
	l, tee := testRunnerLogger(context.Background(), cfg.parallelism, runnerLogPath)
	lopt := loggingOpt{
		l:                   l,
		tee:                 tee,
		stdout:              os.Stdout,
		stderr:              os.Stderr,
		artifactsDir:        cfg.artifactsDir,
		literalArtifactsDir: cfg.literalArtifactsDir,
		runnerLogPath:       runnerLogPath,
	}

	// We're going to run all the workers (and thus all the tests) in a context
	// that gets canceled when the Interrupt signal is received.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	CtrlC(ctx, l, cancel, cr)
	err = runner.Run(
		ctx, tests, cfg.count, cfg.parallelism, opt,
		testOpts{
			versionsBinaryOverride: cfg.versionsBinaryOverride,
			skipInit:               cfg.skipInit,
		},
		lopt, nil /* clusterAllocator */)

	// Make sure we attempt to clean up. We run with a non-canceled ctx; the
	// ctx above might be canceled in case a signal was received. If that's
	// the case, we're running under a 5s timeout until the CtrlC() goroutine
	// kills the process.
	l.PrintfCtx(ctx, "runTests destroying all clusters")
	cr.destroyAllClusters(context.Background(), l)

	if teamCity {
		// Collect the runner logs.
		fmt.Printf("##teamcity[publishArtifacts '%s']\n", filepath.Join(cfg.literalArtifactsDir, runnerLogsDir))
	}
	return err
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
			os.Exit(2)
		case <-destroyCh:
			shout(ctx, l, os.Stderr, "Done destroying all clusters.")
			os.Exit(2)
		}
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

func testsToRun(r testRegistryImpl, filter *registry.TestFilter) []registry.TestSpec {
	tests, tagMismatch := r.GetTests(filter)

	var notSkipped []registry.TestSpec
	for _, s := range tests {
		if s.Skip == "" || filter.RunSkipped {
			notSkipped = append(notSkipped, s)
		} else {
			if teamCity {
				fmt.Fprintf(os.Stdout, "##teamcity[testIgnored name='%s' message='%s']\n",
					s.Name, teamCityEscape(s.Skip))
			}
			fmt.Fprintf(os.Stdout, "--- SKIP: %s (%s)\n\t%s\n", s.Name, "0.00s", s.Skip)
		}
	}
	for _, s := range tagMismatch {
		if teamCity {
			fmt.Fprintf(os.Stdout, "##teamcity[testIgnored name='%s' message='tag mismatch']\n",
				s.Name)
		}
		fmt.Fprintf(os.Stdout, "--- SKIP: %s (%s)\n\ttag mismatch\n", s.Name, "0.00s")
	}
	return notSkipped
}
