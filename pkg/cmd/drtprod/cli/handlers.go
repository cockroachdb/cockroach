// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"os"

	"github.com/cockroachdb/cockroach/pkg/cmd/drtprod/cli/commands"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/cli"
	"github.com/spf13/cobra"
)

func init() {
	// Set environment variables for the GCE project and DNS configurations.
	_ = os.Setenv("ROACHPROD_DNS", "drt.crdb.io")
	_ = os.Setenv("ROACHPROD_GCE_DNS_DOMAIN", "drt.crdb.io")
	_ = os.Setenv("ROACHPROD_GCE_DNS_ZONE", "drt")
	_ = os.Setenv("ROACHPROD_GCE_DEFAULT_PROJECT", "cockroach-drt")
}

// Initialize sets up the environment and initializes the command-line interface.
func Initialize(ctx context.Context) {
	// Disable command sorting in Cobra (command-line parser).
	cobra.EnableCommandSorting = false

	// Create the root command and add subcommands.
	rootCommand := commands.GetRootCommand(ctx)
	rootCommand.AddCommand(register(ctx)...)
	cli.Initialize(rootCommand)

	// Execute the root command, exit if an error occurs.
	if err := rootCommand.Execute(); err != nil {
		os.Exit(1)
	}
}
