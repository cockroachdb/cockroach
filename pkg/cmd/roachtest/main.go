// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"os"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cli"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "roachtest [command] (flags)",
	Short: "roachtest tool for testing cockroach clusters",
	Long: `roachtest is a tool for testing cockroach clusters.
`,
	Version:          "details:\n" + build.GetInfo().Long(),
	PersistentPreRun: cli.ValidateAndConfigure,
}

func main() {
	cli.Initialize(rootCmd)

	if err := rootCmd.Execute(); err != nil {
		// Cobra has already printed the error message.
		os.Exit(cli.HandleExitCode(err))
	}
}
