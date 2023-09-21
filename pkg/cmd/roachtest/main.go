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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/tests"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
	_ "github.com/lib/pq" // register postgres driver
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
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
			r := makeTestRegistry(cloud, instanceType, zonesF, localSSDArg, listBench)
			tests.RegisterTests(&r)

			filter := registry.NewTestFilter(args, runSkipped)
			specs := testsToRun(r, filter, selectProbability, false)

			for _, s := range specs {
				var skip string
				if s.Skip != "" && !runSkipped {
					skip = " (skipped: " + s.Skip + ")"
				}
				fmt.Printf("%s [%s]%s\n", s.Name, s.Owner, skip)
			}
			return nil
		},
	}
	listCmd.Flags().BoolVar(
		&listBench, "bench", false, "list benchmarks instead of tests")
	listCmd.Flags().StringVar(
		&cloud, "cloud", cloud, "cloud provider to use (aws, azure, or gce)")

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

COCKROACH_ environment variables in the local environment are passed through to
the cluster nodes on start.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := initRunFlagsBinariesAndLibraries(cmd); err != nil {
				return err
			}
			return runTests(tests.RegisterTests, args, false /* benchOnly */)
		},
	}

	var benchCmd = &cobra.Command{
		// Don't display usage when tests fail.
		SilenceUsage: true,
		Use:          "bench [benchmarks]",
		Short:        "run automated benchmarks on cockroach cluster",
		Long:         `Run automated benchmarks on existing or ephemeral cockroach clusters.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := initRunFlagsBinariesAndLibraries(cmd); err != nil {
				return err
			}
			return runTests(tests.RegisterTests, args, true /* benchOnly */)
		},
	}

	// Register flags shared between `run` and `bench`.
	addRunFlags(runCmd)
	addBenchFlags(benchCmd)

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
	r testRegistryImpl, filter *registry.TestFilter, selectProbability float64, print bool,
) []registry.TestSpec {
	specs, tagMismatch := r.GetTests(filter)

	var notSkipped []registry.TestSpec
	for _, s := range specs {
		if s.Skip == "" || filter.RunSkipped {
			notSkipped = append(notSkipped, s)
		} else {
			if print && teamCity {
				fmt.Fprintf(os.Stdout, "##teamcity[testIgnored name='%s' message='%s']\n",
					s.Name, teamCityEscape(s.Skip))
			}
			if print {
				fmt.Fprintf(os.Stdout, "--- SKIP: %s (%s)\n\t%s\n", s.Name, "0.00s", s.Skip)
			}
		}
	}
	for _, s := range tagMismatch {
		if print && teamCity {
			fmt.Fprintf(os.Stdout, "##teamcity[testIgnored name='%s' message='tag mismatch']\n",
				s.Name)
		}
		if print {
			fmt.Fprintf(os.Stdout, "--- SKIP: %s (%s)\n\ttag mismatch\n", s.Name, "0.00s")
		}
	}

	return selectSpecs(notSkipped, selectProbability, true, print)
}

// selectSpecs returns a random sample of the given test specs.
// If atLeastOnePerPrefix is true, it guarantees that at least one test is
// selected for each prefix (e.g. kv0/, acceptance/).
// This assumes that specs are sorted by name, which is the case for
// testRegistryImpl.GetTests().
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
			if print && teamCity {
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
