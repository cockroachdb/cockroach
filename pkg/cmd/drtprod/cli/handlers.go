// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/drtprod/cli/commands"
	"github.com/cockroachdb/cockroach/pkg/cmd/drtprod/helpers"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/spf13/cobra"
)

// Initialize sets up the environment and initializes the command-line interface.
func Initialize(ctx context.Context) {
	// Set environment variables for the GCE project and DNS configurations.
	_ = os.Setenv("ROACHPROD_DNS", "drt.crdb.io")
	_ = os.Setenv("ROACHPROD_GCE_DNS_DOMAIN", "drt.crdb.io")
	_ = os.Setenv("ROACHPROD_GCE_DNS_ZONE", "drt")
	_ = os.Setenv("ROACHPROD_GCE_DEFAULT_PROJECT", "cockroach-drt")
	// Initialize cloud providers for roachprod.
	_ = roachprod.InitProviders()

	// Disable command sorting in Cobra (command-line parser).
	cobra.EnableCommandSorting = false

	// Create the root command and add subcommands.
	rootCommand := commands.GetRootCommand(ctx)
	rootCommand.AddCommand(register(ctx)...)

	// Check if the command is found in drtprod; if not, redirect to roachprod.
	_, _, err := rootCommand.Find(os.Args[1:])
	if err != nil {
		if strings.Contains(err.Error(), "unknown command") {
			// Command not found, execute it in roachprod instead.
			_ = helpers.ExecuteCmd(ctx, "roachprod", "roachprod", os.Args[1:]...)
			return
		}
		// If another error occurs, exit with a failure status.
		os.Exit(1)
	}

	// Execute the root command, exit if an error occurs.
	if err := rootCommand.Execute(); err != nil {
		os.Exit(1)
	}
}
