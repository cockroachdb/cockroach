// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// cockroach-db-console is a minimal binary that only includes the
// start-db-console command for fast iteration on UI development.
package main

import (
	"os"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/cli/clierror"
	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/spf13/cobra"
)

// Minimal version of the cockroach command that only includes start-db-console
var rootCmd = &cobra.Command{
	Use:   "cockroach-db-console",
	Short: "Standalone CockroachDB Console server",
	Long: `Standalone CockroachDB Console server for UI development.
This is a minimal build that only includes the start-db-console command.`,
	SilenceUsage:  true,
	SilenceErrors: true,
	Version:       string(build.GetInfo().Short()) + "\n(minimal build for DB Console development)",
}

func main() {
	if len(os.Args) == 1 {
		os.Args = append(os.Args, "help")
	}

	// Add only the start-db-console command
	rootCmd.AddCommand(cli.GetStartDBConsoleCmd())

	cmd, _, _ := rootCmd.Find(os.Args[1:])

	err := doMain(cmd)
	errCode := exit.Success()
	if err != nil {
		clierror.OutputError(os.Stderr, err, true, false)
		errCode = exit.UnspecifiedError()
	}

	exit.WithCode(errCode)
}

func doMain(cmd *cobra.Command) error {
	rootCmd.SetArgs(os.Args[1:])
	return rootCmd.Execute()
}
