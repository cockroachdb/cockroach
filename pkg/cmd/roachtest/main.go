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
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/spf13/cobra"
)

// runnerLogsDir is the dir under the artifacts root where the test runner log
// and other runner-related logs (i.e. cluster creation logs) will be written.
const runnerLogsDir = "_runner-logs"

func main() {
	rand.Seed(timeutil.Now().UnixNano())
	username := os.Getenv("ROACHPROD_USER")
	parallelism := 10
	var cpuQuota int
	// Path to a local dir where the test logs and artifacts collected from
	// cluster will be placed.
	var artifacts string
	var httpPort int
	var debugEnabled bool
	var clusterID string
	var count = 1

	cobra.EnableCommandSorting = false

	var rootCmd = &cobra.Command{
		Use:   "roachtest [command] (flags)",
		Short: "roachtest tool for testing cockroach clusters",
		Long: `roachtest is a tool for testing cockroach clusters.
`,

		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			// Don't bother checking flags for the default help command.
			if cmd.Name() == "help" {
				return nil
			}

			if clusterName != "" && local {
				return fmt.Errorf(
					"cannot specify both an existing cluster (%s) and --local. However, if a local cluster "+
						"already exists, --clusters=local will use it",
					clusterName)
			}
			switch cmd.Name() {
			case "run", "bench", "store-gen":
				initBinaries()
			}
			return nil
		},
	}

	rootCmd.PersistentFlags().StringVarP(
		&clusterName, "cluster", "c", "",
		"Comma-separated list of names existing cluster to use for running tests. "+
			"If fewer than --parallelism names are specified, then the parallelism "+
			"is capped to the number of clusters specified.")
	rootCmd.PersistentFlags().BoolVarP(
		&local, "local", "l", local, "run tests locally")
	rootCmd.PersistentFlags().StringVarP(
		&username, "user", "u", username,
		"Username to use as a cluster name prefix. "+
			"If blank, the current OS user is detected and specified.")
	rootCmd.PersistentFlags().StringVar(
		&cockroach, "cockroach", "", "path to cockroach binary to use")
	rootCmd.PersistentFlags().StringVar(
		&workload, "workload", "", "path to workload binary to use")
	f := rootCmd.PersistentFlags().VarPF(
		&encrypt, "encrypt", "", "start cluster with encryption at rest turned on")
	f.NoOptDefVal = "true"

	var listBench bool

	var listCmd = &cobra.Command{
		Use:   "list [tests]",
		Short: "list tests matching the patterns",
		Long: `List tests that match the given name patterns.

If no pattern is passed, all tests are matched.
Use --bench to list benchmarks instead of tests.

Each test has a set of tags. The tags are used to skip tests which don't match
the tag filter. The tag filter is specified by specifying a pattern with the
"tag:" prefix. The default tag filter is "tag:default" which matches any test
that has the "default" tag. Note that tests are selected based on their name,
and skipped based on their tag.

Examples:

   roachtest list acceptance copy/bank/.*false
   roachtest list tag:acceptance
   roachtest list tag:weekly
`,
		RunE: func(_ *cobra.Command, args []string) error {
			r, err := makeTestRegistry()
			if err != nil {
				return err
			}
			if !listBench {
				registerTests(&r)
			} else {
				registerBenchmarks(&r)
			}

			names := r.List(context.Background(), args)
			for _, name := range names {
				fmt.Println(name)
			}
			return nil
		},
	}
	listCmd.Flags().BoolVar(
		&listBench, "bench", false, "list benchmarks instead of tests")

	var runCmd = &cobra.Command{
		// Don't display usage when tests fail.
		SilenceUsage: true,
		Use:          "run [tests]",
		Short:        "run automated tests on cockroach cluster",
		Long: `Run automated tests on existing or ephemeral cockroach clusters.

roachtest run takes a list of regex patterns and runs all the matching tests.
If no pattern is given, all tests are run. See "help list" for more details on
the test tags.
`,
		RunE: func(_ *cobra.Command, args []string) error {
			return runTests(registerTests, cliCfg{
				args:         args,
				count:        count,
				cpuQuota:     cpuQuota,
				debugEnabled: debugEnabled,
				httpPort:     httpPort,
				parallelism:  parallelism,
				artifactsDir: artifacts,
				user:         username,
				clusterID:    clusterID,
			})
		},
	}

	runCmd.Flags().StringVar(
		&buildTag, "build-tag", "", "build tag (auto-detect if empty)")
	runCmd.Flags().StringVar(
		&slackToken, "slack-token", "", "Slack bot token")
	runCmd.Flags().BoolVar(
		&teamCity, "teamcity", false, "include teamcity-specific markers in output")

	var benchCmd = &cobra.Command{
		// Don't display usage when tests fail.
		SilenceUsage: true,
		Use:          "bench [benchmarks]",
		Short:        "run automated benchmarks on cockroach cluster",
		Long:         `Run automated benchmarks on existing or ephemeral cockroach clusters.`,
		RunE: func(_ *cobra.Command, args []string) error {
			return runTests(registerBenchmarks, cliCfg{
				args:         args,
				count:        count,
				cpuQuota:     cpuQuota,
				debugEnabled: debugEnabled,
				httpPort:     httpPort,
				parallelism:  parallelism,
				artifactsDir: artifacts,
				user:         username,
				clusterID:    clusterID,
			})
		},
	}

	// Register flags shared between `run` and `bench`.
	for _, cmd := range []*cobra.Command{runCmd, benchCmd} {
		cmd.Flags().StringVar(
			&artifacts, "artifacts", "artifacts", "path to artifacts directory")
		cmd.Flags().StringVar(
			&cloud, "cloud", cloud, "cloud provider to use (aws or gce)")
		cmd.Flags().StringVar(
			&clusterID, "cluster-id", "", "an identifier to use in the test cluster's name")
		cmd.Flags().IntVar(
			&count, "count", 1, "the number of times to run each test")
		cmd.Flags().BoolVarP(
			&debugEnabled, "debug", "d", debugEnabled, "don't wipe and destroy cluster if test fails")
		cmd.Flags().IntVarP(
			&parallelism, "parallelism", "p", parallelism, "number of tests to run in parallel")
		cmd.Flags().StringVar(
			&roachprod, "roachprod", "", "path to roachprod binary to use")
		cmd.Flags().BoolVar(
			&clusterWipe, "wipe", true,
			"wipe existing cluster before starting test (for use with --cluster)")
		cmd.Flags().StringVar(
			&zonesF, "zones", "", "Zones for the cluster (use roachprod defaults if empty)")
		cmd.Flags().IntVar(
			&cpuQuota, "cpu-quota", 300,
			"The number of cloud CPUs roachtest is allowed to use at any one time.")
		cmd.Flags().IntVar(
			&httpPort, "port", 8080, "the port on which to serve the HTTP interface")
	}

	rootCmd.AddCommand(listCmd)
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(benchCmd)

	if err := rootCmd.Execute(); err != nil {
		// Cobra has already printed the error message.
		os.Exit(1)
	}
}

type cliCfg struct {
	args         []string
	count        int
	cpuQuota     int
	debugEnabled bool
	httpPort     int
	parallelism  int
	artifactsDir string
	user         string
	clusterID    string
}

func runTests(register func(*testRegistry), cfg cliCfg) error {
	if cfg.count <= 0 {
		return fmt.Errorf("--count (%d) must by greater than 0", cfg.count)
	}
	r, err := makeTestRegistry()
	if err != nil {
		return err
	}
	register(&r)
	cr := newClusterRegistry()
	runner := newTestRunner(cr, r.buildVersion)

	filter := newFilter(cfg.args)
	clusterType := roachprodCluster
	if local {
		clusterType = localCluster
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
	if err := runner.runHTTPServer(cfg.httpPort, os.Stdout); err != nil {
		return err
	}

	tests := testsToRun(context.Background(), r, filter)
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
		l:             l,
		tee:           tee,
		stdout:        os.Stdout,
		stderr:        os.Stderr,
		artifactsDir:  cfg.artifactsDir,
		runnerLogPath: runnerLogPath,
	}

	// We're going to run all the workers (and thus all the tests) in a context
	// that gets canceled when the Interrupt signal is received.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	CtrlC(ctx, l, cancel, cr)
	err = runner.Run(ctx, tests, cfg.count, cfg.parallelism, opt, cfg.artifactsDir, lopt)

	// Make sure we attempt to clean up. We run with a non-canceled ctx; the
	// ctx above might be canceled in case a signal was received. If that's
	// the case, we're running under a 5s timeout until the CtrlC() goroutine
	// kills the process.
	l.PrintfCtx(ctx, "runTests destroying all clusters")
	cr.destroyAllClusters(context.Background(), l)

	if teamCity {
		// Collect the runner logs.
		fmt.Printf("##teamcity[publishArtifacts '%s']\n", runnerDir)
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
func CtrlC(ctx context.Context, l *logger, cancel func(), cr *clusterRegistry) {
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
			shout(ctx, l, os.Stderr, "Second SIGINT received. Quitting.")
			os.Exit(2)
		case <-destroyCh:
			shout(ctx, l, os.Stderr, "Done destroying all clusters.")
		}
	}()
}

// testRunnerLogger returns a logger to be used by the test runner and a tee
// option for the test logs.
//
// runnerLogPath is the path to the file that will contain the runner's log.
func testRunnerLogger(
	ctx context.Context, parallelism int, runnerLogPath string,
) (*logger, teeOptType) {
	teeOpt := noTee
	if parallelism == 1 {
		teeOpt = teeToStdout
	}

	var l *logger
	if teeOpt == teeToStdout {
		verboseCfg := loggerConfig{stdout: os.Stdout, stderr: os.Stderr}
		var err error
		l, err = verboseCfg.newLogger(runnerLogPath)
		if err != nil {
			panic(err)
		}
	} else {
		verboseCfg := loggerConfig{}
		var err error
		l, err = verboseCfg.newLogger(runnerLogPath)
		if err != nil {
			panic(err)
		}
	}
	shout(ctx, l, os.Stdout, "test runner logs in: %s", runnerLogPath)
	return l, teeOpt
}

func testsToRun(ctx context.Context, r testRegistry, filter *testFilter) []testSpec {
	tests := r.GetTests(ctx, filter)

	var notSkipped []testSpec
	for _, s := range tests {
		if s.Skip == "" {
			notSkipped = append(notSkipped, s)
		} else {
			if teamCity {
				fmt.Fprintf(os.Stdout, "##teamcity[testIgnored name='%s' message='%s']\n",
					s.Name, teamCityEscape(s.Skip))
			}
			fmt.Fprintf(os.Stdout, "--- SKIP: %s (%s)\n\t%s\n", s.Name, "0.00s", s.Skip)
		}
	}
	return notSkipped
}
