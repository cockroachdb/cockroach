// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"os"
	"os/exec"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/drtprod/cli/commands"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/cli"
	"github.com/spf13/cobra"
)

// setEnvIfNotExists sets an environment variable only if it doesn't already exist.
func setEnvIfNotExists(key, value string) {
	if _, exists := os.LookupEnv(key); !exists {
		_ = os.Setenv(key, value)
	}
}

func init() {
	// Set environment variables for the GCE project and DNS configurations if not already set.
	setEnvIfNotExists("ROACHPROD_DNS", "drt.crdb.io")
	setEnvIfNotExists("ROACHPROD_GCE_DNS_DOMAIN", "drt.crdb.io")
	setEnvIfNotExists("ROACHPROD_GCE_DNS_ZONE", "drt")
	setEnvIfNotExists("ROACHPROD_GCE_DEFAULT_PROJECT", "cockroach-drt")

	if _, exists := os.LookupEnv("DD_API_KEY"); !exists {
		// set the DD_API_KEY if we are able to fetch it from the secrets.
		// this is for audit logging all events by drtprod
		cmd := exec.Command("gcloud", "--project=cockroach-drt", "secrets", "versions", "access", "latest",
			"--secret", "datadog-api-key")
		output, err := cmd.Output()
		if err == nil && string(output) != "" {
			// std output has the new line in the end. That is trimmed.
			_ = os.Setenv("DD_API_KEY", strings.TrimRight(string(output), "\n"))
		}
	}
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
