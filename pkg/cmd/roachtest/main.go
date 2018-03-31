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
	"strings"

	"github.com/spf13/cobra"
)

func main() {
	cobra.EnableCommandSorting = false

	var rootCmd = &cobra.Command{
		Use:   "roachtest [command] (flags)",
		Short: "roachtest tool for testing cockroach clusters",
		Long: `roachtest is a tool for testing cockroach clusters.
`,
	}

	var runCmd = &cobra.Command{
		Use:   "run [tests]",
		Short: "run automated tests on cockroach cluster\n",
		Long: `Run automated tests on existing or ephemeral cockroach clusters.

	` + strings.Join(allTests(), "\n\t") + `
`,
		RunE: func(_ *cobra.Command, args []string) error {
			if clusterName != "" && local {
				return fmt.Errorf("cannot specify both an existing cluster (%s) and --local", clusterName)
			}

			initBinaries()
			os.Exit(tests.Run(args))
			return nil
		},
	}

	rootCmd.AddCommand(runCmd)

	runCmd.Flags().BoolVarP(
		&debug, "debug", "d", debug, "don't wipe and destroy cluster if test fails")
	runCmd.Flags().BoolVarP(
		&dryrun, "dry-run", "n", dryrun, "dry run (don't run tests)")
	runCmd.Flags().BoolVarP(
		&local, "local", "l", local, "run tests locally")
	runCmd.Flags().IntVarP(
		&parallelism, "parallelism", "p", parallelism, "number of tests to run in parallel")
	runCmd.Flags().StringVar(
		&artifacts, "artifacts", "artifacts", "path to artifacts directory")
	runCmd.Flags().StringVarP(
		&clusterName, "cluster", "c", "", "name of an existing cluster to use for running tests")
	runCmd.Flags().StringVar(
		&clusterID, "cluster-id", "", "an identifier to use in the test cluster's name")
	runCmd.Flags().BoolVar(
		&clusterWipe, "wipe", true,
		"wipe existing cluster before starting test (for use with --cluster)")
	runCmd.Flags().StringVar(
		&cockroach, "cockroach", "", "path to cockroach binary to use")
	runCmd.Flags().StringVarP(
		&username, "user", "u", username, "username to run under, detect if blank")
	runCmd.Flags().StringVar(
		&workload, "workload", "", "path to workload binary to use")
	runCmd.Flags().StringVar(
		&slackToken, "slack-token", "", "Slack bot token")
	runCmd.Flags().StringVar(
		&zones, "zones", "", "Zones for the cluster (use roachprod defaults if empty)")

	if err := rootCmd.Execute(); err != nil {
		// Cobra has already printed the error message.
		os.Exit(1)
	}
}
