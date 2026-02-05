// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package cli provides the initialization logic for the TEF command-line interface.
// This file contains the main initialization sequence that sets up the CLI,
// creates the root command, and registers all plan-specific sub-commands.
package cli

import (
	"context"
	"os"

	"github.com/cockroachdb/cockroach/pkg/cmd/tef/planners"
	"github.com/cockroachdb/cockroach/pkg/cmd/tef/plans"
	"github.com/spf13/cobra"
)

// Initialize sets up and executes the TEF CLI.
// It creates the root command, registers all plan-specific sub-commands, and runs the CLI.
func Initialize() {
	// Create a logger instance for the TEF CLI.
	logger := planners.NewLogger()

	// Create a context with the logger attached.
	ctx := planners.ContextWithLogger(context.Background(), logger)

	// The root Cobra command is created with usage information.
	rootCmd := &cobra.Command{
		Use:   "tef [sub-command] [flags]",
		Short: "Task Execution Framework",
		Long:  "TEF is a task execution framework for managing and running plan sequences",
		Args:  cobra.ExactArgs(1),
	}
	// All registered plans are initialized, and their commands are added to the root.
	pr := planners.NewPlanRegistry()
	plans.RegisterPlans(pr)
	initializeWorkerCLI(ctx, rootCmd, pr.GetRegistries())
	// The CLI is executed, and the program exits with an error code if execution fails.
	if err := rootCmd.Execute(); err != nil {
		// Cobra has already printed the error message to stderr.
		os.Exit(1)
	}
}
