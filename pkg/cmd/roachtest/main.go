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
	"fmt"
	"math/rand"
	"os"
	"os/user"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/build"
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
		Version: "details:\n" + build.GetInfo().Long(),
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
			r := makeTestRegistry(roachtestflags.Cloud)
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
				var skip string
				if s.Skip != "" {
					skip = " (skipped: " + s.Skip + ")"
				}
				fmt.Printf("%s [%s]%s\n", s.Name, s.Owner, skip)
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

	rootCmd.AddCommand(listCmd)
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(benchCmd)

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
