// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/app"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/config"
	healthcontroller "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/health"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/health"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var workersCmd = &cobra.Command{
	Use:   "workers",
	Short: "Start task workers with metrics endpoint",
	Long: `Start roachprod-centralized task workers without the API server.
This mode is intended for horizontally scaling task processing by running
dedicated worker instances. Only the metrics endpoint is exposed for monitoring.

Requirements:
  - Must use CockroachDB as the database backend (memory mode is not supported)
  - Multiple worker instances can run concurrently, coordinating through the database`,
	RunE: func(cmd *cobra.Command, args []string) error {
		cmd.SilenceUsage = true
		return runWorkers(cmd, args)
	},
}

func init() {
	rootCmd.AddCommand(workersCmd)

	// Add all configuration flags to the workers command so they show up in help
	err := config.AddFlagsToFlagSet(workersCmd.Flags())
	if err != nil {
		panic(fmt.Sprintf("Error adding flags to workers command: %v", err))
	}
}

func runWorkers(cmd *cobra.Command, args []string) error {
	cfg, err := config.InitConfigWithFlagSet(cmd.Flags())
	if err != nil {
		return errors.Wrap(err, "error initializing config")
	}

	// Validate that CockroachDB is being used
	// Memory mode doesn't support distributed workers
	if strings.ToLower(cfg.Database.Type) == "memory" {
		return errors.Newf(
			"workers command cannot be used with memory database backend. "+
				"The memory database backend does not support distributed worker coordination. "+
				"Use the 'api' command for single-instance deployments with memory storage.",
			cfg.Database.Type,
		)
	}

	l := logger.NewLogger(cfg.Log.Level)

	// Create all services using the factory
	services, err := app.NewServicesFromConfig(cfg, l, health.WorkersOnly)
	if err != nil {
		return errors.Wrap(err, "error creating services")
	}

	// Create the application instance in workers-only mode.
	// This will:
	// - Start all service background work (including task workers)
	// - Skip API controllers and HTTP endpoints except for metrics and health
	application, err := app.NewApp(
		app.WithLogger(l),
		app.WithApiPort(cfg.Api.Port),
		app.WithApiBaseURL(cfg.Api.BasePath),
		app.WithApiMetrics(cfg.Api.Metrics.Enabled),
		app.WithApiMetricsPort(cfg.Api.Metrics.Port),
		app.WithServices(services.ToSlice()),
		app.WithApiController(healthcontroller.NewController(services.Health)),
	)
	if err != nil {
		return errors.Wrap(err, "error creating application")
	}

	// Start the application
	// This will start all services and their background work
	err = application.Start(context.Background())
	if err != nil {
		return errors.Wrap(err, "error starting application")
	}

	return nil
}
