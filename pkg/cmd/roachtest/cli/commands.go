// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"fmt"
	"os"
	"os/user"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operations"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/tests"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/errors"
	_ "github.com/lib/pq" // register postgres driver
	"github.com/spf13/cobra"
)

// Note that the custom exit codes below are not exposed when running
// roachtest on TeamCity. See `teamcity-roachtest-invoke.sh` for more
// details. Also, if the exit codes here change, they need to updated
// on that script accordingly.

const (
	// ExitCodeTestsFailed is the exit code that results from a run of
	// roachtest in which the infrastructure worked, but at least one
	// test failed.
	ExitCodeTestsFailed = 10

	// ExitCodeClusterProvisioningFailed is the exit code that results
	// from a run of roachtest in which some clusters could not be
	// created due to errors during cloud hardware allocation.
	ExitCodeClusterProvisioningFailed = 11

	// ExitCodeGithubPostFailed is the exit code indicating a failure in posting
	// results to GitHub successfully.
	// Note: This error masks the actual roachtest status i.e. this error can
	// occur with any of the other exit codes.
	ExitCodeGithubPostFailed = 12

	// runnerLogsDir is the dir under the artifacts root where the test runner log
	// and other runner-related logs (i.e. cluster creation logs) will be written.
	runnerLogsDir = "_runner-logs"

	// clusterCreateDir is the dir under runnerLogsDir where cluster creation
	// related logs will be written
	clusterCreateDir = "cluster-create"
)

// Initialize sets up and initializes the command-line interface.
func Initialize(rootCmd *cobra.Command) {
	_ = roachprod.InitProviders()

	cobra.EnableCommandSorting = false

	setupCommands(rootCmd)

	var err error
	config.OSUser, err = user.Current()
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to lookup current user: %s\n", err)
		os.Exit(1)
	}
	// Disable spinners and other fancy status messages since all IO is non-interactive.
	config.Quiet = true

	if err := roachprod.InitDirs(); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

// HandleExitCode handles exit codes based on the error returned from command execution.
func HandleExitCode(err error) int {
	if err == nil {
		return 0
	}
	if errors.Is(err, errGithubPostFailed) {
		return ExitCodeGithubPostFailed
	} else if errors.Is(err, errSomeClusterProvisioningFailed) {
		return ExitCodeClusterProvisioningFailed
	} else if errors.Is(err, errTestsFailed) {
		return ExitCodeTestsFailed
	}
	return 1
}

// setupCommands adds all roachtest commands to the root command.
func setupCommands(rootCmd *cobra.Command) {
	rootCmd.AddCommand(&cobra.Command{
		Use:   `version`,
		Short: `print version information`,
		RunE: func(cmd *cobra.Command, args []string) error {
			info := build.GetInfo()
			fmt.Println(info.Long())
			return nil
		}},
	)

	var listCmd = &cobra.Command{
		Use:   "list [regex...]",
		Short: "list tests",
		Long: `List tests that match the flags and given name patterns.

Tests are restricted by the specified --cloud (gce by default). Use
--cloud=all to show tests for all clouds.

Use --bench to restrict to benchmarks.
Use --suite to restrict to tests that are part of the given suite.
Use --owner to restrict to tests that have the given owner.

If patterns are specified, only tests that match either of the given patterns
are listed.

Examples:

   roachtest list acceptance copy/bank/.*false
   roachtest list --owner kv
   roachtest list --suite weekly

   # match weekly kv owned tests
   roachtest list --suite weekly --owner kv
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			r := makeTestRegistry()
			tests.RegisterTests(&r)

			filter, err := makeTestFilter(args)
			if err != nil {
				return err
			}
			// Print a description of the filter to stderr (to keep stdout clean for scripts).
			fmt.Fprintf(cmd.OutOrStderr(), "Listing %s.\n\n", filter.String())
			cmd.SilenceUsage = true

			specs, hint := filter.FilterWithHint(r.AllTests())
			if len(specs) == 0 {
				return errors.Newf("%s", filter.NoMatchesHintString(hint))
			}

			for _, s := range specs {
				var skip, randomized, timeout string
				if s.Skip != "" {
					skip = " (skipped: " + s.Skip + ")"
				}
				var prefix, separator string
				longListing := false
				if s.Randomized {
					longListing = true
					randomized = "randomized"
					separator = ","
				}
				if s.Timeout != 0 {
					longListing = true
					timeout = fmt.Sprintf("%stimeout: %s", separator, s.Timeout)
				}
				if longListing {
					// N.B. use a prefix to separate the extended listing.
					prefix = "  "
				}
				fmt.Printf("%s [%s]%s %s%s%s\n", s.Name, s.Owner, skip, prefix, randomized, timeout)
			}
			return nil
		},
	}
	roachtestflags.AddListFlags(listCmd.Flags())

	var runCmd = &cobra.Command{
		// Don't display usage when tests fail.
		SilenceUsage: true,
		Use:          "run [regex..]",
		Short:        "run automated tests on cockroach cluster",
		Long: `Run automated tests on existing or ephemeral cockroach clusters.

roachtest run takes a list of regex patterns and runs tests matching the tests
as well as the --cloud, --suite, --owner flags. See "help list" for more details
on specifying tests.

If all invoked tests passed, the exit status is zero. If at least one test
failed, it is 10. Any other exit status reports a problem with the test
runner itself.

COCKROACH_ environment variables in the local environment are passed through to
the cluster nodes on start.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := initRunFlagsBinariesAndLibraries(cmd); err != nil {
				return err
			}
			filter, err := makeTestFilter(args)
			if err != nil {
				return err
			}
			fmt.Printf("\nRunning %s.\n\n", filter.String())
			cmd.SilenceUsage = true
			return runTests(tests.RegisterTests, filter)
		},
	}
	roachtestflags.AddRunFlags(runCmd.Flags())

	var benchCmd = &cobra.Command{
		// Don't display usage when tests fail.
		SilenceUsage: true,
		Use:          "bench [regex...]",
		Short:        "run automated benchmarks on cockroach cluster",
		Long:         `Run automated benchmarks on existing or ephemeral cockroach clusters.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := initRunFlagsBinariesAndLibraries(cmd); err != nil {
				return err
			}
			roachtestflags.OnlyBenchmarks = true
			filter, err := makeTestFilter(args)
			if err != nil {
				return err
			}
			fmt.Printf("\nRunning %s.\n\n", filter.String())
			cmd.SilenceUsage = true
			return runTests(tests.RegisterTests, filter)
		},
	}
	roachtestflags.AddRunFlags(benchCmd.Flags())

	var runOperationCmd = &cobra.Command{
		// Don't display usage when the command fails.
		SilenceUsage: true,
		Use:          "run-operation [clusterName] [regex...]",
		Short:        "run one operation on an existing cluster",
		Long: `Run an automated operation on an existing roachprod cluster.
If multiple operations are matched by the passed-in regex filter, one operation
is chosen at random and run. The provided cluster name must already exist in roachprod;
this command does no setup/teardown of clusters.

This command can be used to run operation in parallel and infinitely on a cluster.
Check --parallelism, --run-forever and --wait-before-next-execution flags`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Printf("\nRunning operation %s on %s.\n\n", args[1], args[0])
			cmd.SilenceUsage = true
			return runOperations(operations.RegisterOperations, args[1], args[0])
		},
	}
	roachtestflags.AddRunOpsFlags(runOperationCmd.Flags())

	rootCmd.AddCommand(listCmd)
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(benchCmd)
	rootCmd.AddCommand(runOperationCmd)

	var listOperationCmd = &cobra.Command{
		Use:   "list-operations [regex...]",
		Short: "list all operation names",
		Long: `List all available operations that can be run with the run-operation command.

This command lists the names of all registered operations.

Example:

   roachtest list-operations
   roachtest list-operations node-kill/.*m$
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			r := makeTestRegistry()
			operations.RegisterOperations(&r)

			ops := r.FilteredOperations(registry.MergeRegEx(args))
			for _, op := range ops {
				fmt.Printf("%s\n", op.Name)
			}

			return nil
		},
	}
	rootCmd.AddCommand(listOperationCmd)
}

// ValidateAndConfigure validates and configures args before executing any command.
func ValidateAndConfigure(cmd *cobra.Command, args []string) {
	// Skip validation for commands that are self-sufficient.
	switch cmd.Name() {
	case "help", "version", "list":
		return
	}

	printErrAndExit := func(err error) {
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			os.Exit(1)
		}
	}
	// Validate use-spot flag
	if spotFlagInfo := roachtestflags.Changed(&roachtestflags.UseSpotVM); spotFlagInfo != nil {
		val := strings.ToLower(roachtestflags.UseSpotVM)

		switch val {
		case roachtestflags.NeverUseSpot, roachtestflags.AlwaysUseSpot, roachtestflags.AutoUseSpot:
			roachtestflags.UseSpotVM = val
		default:
			printErrAndExit(fmt.Errorf("unsupported option value %q for option %q; Usage: %s",
				roachtestflags.UseSpotVM, spotFlagInfo.Name, spotFlagInfo.Usage))
		}
	}

	// Test selection and select probability flags are mutually exclusive.
	selectProbFlagInfo := roachtestflags.Changed(&roachtestflags.SelectProbability)
	if roachtestflags.SelectiveTests && selectProbFlagInfo != nil {
		printErrAndExit(fmt.Errorf("select-probability and selective-tests=true are incompatible. Disable one of them"))
	}

	// --cockroach and --cockroach-stage flags are mutually exclusive.
	cockroachFlagInfo := roachtestflags.Changed(&roachtestflags.CockroachPath)
	cockroachStageFlagInfo := roachtestflags.Changed(&roachtestflags.CockroachStage)
	if cockroachFlagInfo != nil && cockroachStageFlagInfo != nil {
		printErrAndExit(fmt.Errorf("--cockroach and --cockroach-stage are mutually exclusive. Use one or the other"))
	}
}
