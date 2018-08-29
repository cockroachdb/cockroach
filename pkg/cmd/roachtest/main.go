// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func main() {
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
				return fmt.Errorf("cannot specify both an existing cluster (%s) and --local", clusterName)
			}
			initBinaries()
			return nil
		},
	}

	rootCmd.PersistentFlags().StringVarP(
		&clusterName, "cluster", "c", "", "name of an existing cluster to use for running tests")
	rootCmd.PersistentFlags().BoolVarP(
		&local, "local", "l", local, "run tests locally")
	rootCmd.PersistentFlags().StringVarP(
		&username, "user", "u", username, "username to run under, detect if blank")
	rootCmd.PersistentFlags().StringVar(
		&cockroach, "cockroach", "", "path to cockroach binary to use")
	rootCmd.PersistentFlags().StringVar(
		&workload, "workload", "", "path to workload binary to use")
	rootCmd.PersistentFlags().BoolVarP(
		&encrypt, "encrypt", "", encrypt, "start cluster with encryption at rest turned on")

	var runCmd = &cobra.Command{
		Use:   "run [tests]",
		Short: "run automated tests on cockroach cluster",
		Long: `Run automated tests on existing or ephemeral cockroach clusters.

Use 'roachtest run -n' to see a list of all tests.
`,
		RunE: func(_ *cobra.Command, args []string) error {
			if count <= 0 {
				return fmt.Errorf("--count (%d) must by greater than 0", count)
			}
			r := newRegistry()
			if buildTag != "" {
				if err := r.setBuildVersion(buildTag); err != nil {
					return err
				}
			} else {
				r.loadBuildVersion()
			}
			registerTests(r)
			os.Exit(r.Run(args))
			return nil
		},
	}

	runCmd.Flags().StringVar(
		&buildTag, "build-tag", "", "build tag (auto-detect if empty)")
	runCmd.Flags().StringVar(
		&slackToken, "slack-token", "", "Slack bot token")
	runCmd.Flags().BoolVar(
		&teamCity, "teamcity", false, "include teamcity-specific markers in output")

	var benchCmd = &cobra.Command{
		Use:   "bench [benchmarks]",
		Short: "run automated benchmarks on cockroach cluster",
		Long: `Run automated benchmarks on existing or ephemeral cockroach clusters.

Use 'roachtest bench -n' to see a list of all benchmarks.
`,
		RunE: func(_ *cobra.Command, args []string) error {
			if count <= 0 {
				return fmt.Errorf("--count (%d) must by greater than 0", count)
			}
			r := newRegistry()
			registerBenchmarks(r)
			os.Exit(r.Run(args))
			return nil
		},
	}

	// Register flags shared between `run` and `bench`.
	for _, cmd := range []*cobra.Command{runCmd, benchCmd} {
		cmd.Flags().StringVar(
			&artifacts, "artifacts", "artifacts", "path to artifacts directory")
		cmd.Flags().StringVar(
			&clusterID, "cluster-id", "", "an identifier to use in the test cluster's name")
		cmd.Flags().IntVar(
			&count, "count", 1, "the number of times to run each test")
		cmd.Flags().BoolVarP(
			&debugEnabled, "debug", "d", debugEnabled, "don't wipe and destroy cluster if test fails")
		cmd.Flags().BoolVarP(
			&dryrun, "dry-run", "n", dryrun, "dry run (don't run tests)")
		cmd.Flags().IntVarP(
			&parallelism, "parallelism", "p", parallelism, "number of tests to run in parallel")
		cmd.Flags().StringVar(
			&roachprod, "roachprod", "", "path to roachprod binary to use")
		cmd.Flags().BoolVar(
			&clusterWipe, "wipe", true,
			"wipe existing cluster before starting test (for use with --cluster)")
		cmd.Flags().StringVar(
			&zonesF, "zones", "", "Zones for the cluster (use roachprod defaults if empty)")
	}

	var storeGenCmd = &cobra.Command{
		Use:   "store-gen [workload]",
		Short: "generate store directory dumps\n",
		Long: `Generate store directory dumps that can quickly bootstrap a
Cockroach cluster with existing data.
`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			r := newRegistry()
			registerStoreGen(r, args)
			// We've only registered one store generation "test" that does its own
			// argument processing, so no need to provide any arguments to r.Run.
			os.Exit(r.Run(nil /* filter */))
			return nil
		},
	}
	storeGenCmd.Flags().IntVarP(
		&stores, "stores", "n", stores, "number of stores to distribute data across")
	storeGenCmd.Flags().SetInterspersed(false) // ignore workload flags
	storeGenCmd.Flags().BoolVarP(
		&debugEnabled, "debug", "d", debugEnabled, "don't wipe and destroy cluster if test fails")

	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(benchCmd)
	rootCmd.AddCommand(storeGenCmd)

	if err := rootCmd.Execute(); err != nil {
		// Cobra has already printed the error message.
		os.Exit(1)
	}
}
