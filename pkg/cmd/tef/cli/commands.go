// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// TODO: Implement CLI command generation in initializeWorkerCLI.

package cli

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/tef/planners"
	"github.com/spf13/cobra"
)

// initializeWorkerCLI creates and registers CLI commands for each plan registry.
// For each registered plan, this function should generate the following commands:
//   - start-worker <planname>: Start a worker to process plan executions
//   - execute <planname>: Execute the plan with JSON input
//   - gen-view <planname>: Generate a visual diagram of the workflow
//   - resume <planname>: Resume an async task (if applicable)
//
// The context passed should contain a logger via planners.ContextWithLogger.
// Each command should create a PlannerManager instance and invoke the appropriate
// method (StartWorker, ExecutePlan, etc.).
//
// TODO: Implement command generation for all registered plans. This requires:
//  1. A PlannerManager implementation (e.g., planners/temporal/manager.go)
//  2. Command builders for each plan operation
//  3. Flag registration for connection configuration
func initializeWorkerCLI(_ context.Context, _ *cobra.Command, _ []planners.Registry) {
	// TODO: Add the commands
}
