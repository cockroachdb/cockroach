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
	// Create a structured logger instance for the TEF CLI using slog.
	// Uses JSON output with info level by default for better traceability.
	logger := planners.NewLogger("info")

	// Create a context with the logger attached.
	ctx := planners.ContextWithLogger(context.Background(), logger)

	// The root Cobra command is created with usage information.
	rootCmd := &cobra.Command{
		Use:   "tef [sub-command] [flags]",
		Short: "Task Execution Framework",
		Long:  "TEF is a task execution framework for managing and running plan sequences",
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
