// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/user"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operations"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/tests"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/testselector"
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

	// runnerLogsDir is the dir under the artifacts root where the test runner log
	// and other runner-related logs (i.e. cluster creation logs) will be written.
	runnerLogsDir = "_runner-logs"
)

func main() {
	cobra.EnableCommandSorting = false

	var rootCmd = &cobra.Command{
		Use:   "roachtest [command] (flags)",
		Short: "roachtest tool for testing cockroach clusters",
		Long: `roachtest is a tool for testing cockroach clusters.
`,
		Version:          "details:\n" + build.GetInfo().Long(),
		PersistentPreRun: validateAndConfigure,
	}

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
			if print {
				fmt.Fprintf(os.Stdout, "--- SKIP: %s (%s)\n\t%s\n", s.Name, "0.00s", s.Skip)
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

	return selectSpecs(notSkipped, selectProbability, true, print), nil
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

func opsToRun(r testRegistryImpl, filter string) ([]registry.OperationSpec, error) {
	regex, err := regexp.Compile(filter)
	if err != nil {
		return nil, err
	}
	var filteredOps []registry.OperationSpec
	for _, opSpec := range r.AllOperations() {
		if regex.MatchString(opSpec.Name) {
			filteredOps = append(filteredOps, opSpec)
		}
	}
	if len(filteredOps) == 0 {
		return nil, errors.New("no matching operations to run")
	}
	return filteredOps, nil
}

// selectSpecs returns a random sample of the given test specs.
// If atLeastOnePerPrefix is true, it guarantees that at least one test is
// selected for each prefix (e.g. kv0/, acceptance/).
// This assumes that specs are sorted by name, which is the case for
// testRegistryImpl.AllTests().
// TODO(smg260): Perhaps expose `atLeastOnePerPrefix` via CLI
func selectSpecs(
	specs []registry.TestSpec, samplePct float64, atLeastOnePerPrefix bool, print bool,
) []registry.TestSpec {
	if samplePct == 1 || len(specs) == 0 {
		return specs
	}

	var sampled []registry.TestSpec
	var selectedIdxs []int

	prefix := strings.Split(specs[0].Name, "/")[0]
	prefixSelected := false
	prefixIdx := 0

	// Selects one random spec from the range [start, end) and appends it to sampled.
	collectRandomSpecFromRange := func(start, end int) {
		i := start + rand.Intn(end-start)
		sampled = append(sampled, specs[i])
		selectedIdxs = append(selectedIdxs, i)
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

		if rand.Float64() < samplePct {
			sampled = append(sampled, s)
			selectedIdxs = append(selectedIdxs, i)
			prefixSelected = true
			continue
		}

		if atLeastOnePerPrefix && i == len(specs)-1 && !prefixSelected {
			// i + 1 since we want to include the last element
			collectRandomSpecFromRange(prefixIdx, i+1)
		}
	}

	p := 0
	// The list would already be sorted were it not for the lookback to
	// ensure at least one test per prefix.
	if atLeastOnePerPrefix {
		sort.Ints(selectedIdxs)
	}
	// This loop depends on an ordered list as we are essentially
	// skipping all values in between the selected indexes.
	for _, i := range selectedIdxs {
		for j := p; j < i; j++ {
			s := specs[j]
			if print && roachtestflags.TeamCity {
				fmt.Fprintf(os.Stdout, "##teamcity[testIgnored name='%s' message='excluded via sampling']\n",
					s.Name)
			}

			if print {
				fmt.Fprintf(os.Stdout, "--- SKIP: %s (%s)\n\texcluded via sampling\n", s.Name, "0.00s")
			}
		}
		p = i + 1
	}

	return sampled
}

// Before executing any command, validate and canonicalize args.
func validateAndConfigure(cmd *cobra.Command, args []string) {
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
}
