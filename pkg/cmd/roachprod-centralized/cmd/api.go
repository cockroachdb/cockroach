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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/config"
	admincontroller "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/admin"
	authcontroller "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/auth"
	clusterscontroller "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/clusters"
	healthcontroller "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/health"
	publicdns "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/public-dns"
	scimcontroller "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/scim"
	serviceaccountscontroller "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/service-accounts"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/health"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var apiCmd = &cobra.Command{
	Use:   "api",
	Short: "Start the API server",
	Long: `Start the roachprod-centralized API server which provides HTTP endpoints
for managing roachprod clusters, tasks, and related operations.`,
	RunE: runAPI,
}

func init() {
	rootCmd.AddCommand(apiCmd)

	// Add all configuration flags to the API command so they show up in help
	err := config.AddFlagsToFlagSet(apiCmd.Flags())
	if err != nil {
		panic(fmt.Sprintf("Error adding flags to API command: %v", err))
	}

	// Add the --no-workers flag for API-only mode
	apiCmd.Flags().Bool("no-workers", false, "Run API server without task workers (requires CockroachDB)")
}

func runAPI(cmd *cobra.Command, args []string) error {
	cfg, err := config.InitConfigWithFlagSet(cmd.Flags())
	if err != nil {
		return errors.Wrap(err, "error initializing config")
	}

	// Check if --no-workers flag is set
	noWorkers, err := cmd.Flags().GetBool("no-workers")
	if err != nil {
		return errors.Wrap(err, "error reading --no-workers flag")
	}

	// If --no-workers is set, validate that we're not using memory backend
	// and set Workers to -1 (sentinel value for "explicitly disabled")
	mode := health.APIWithWorkers
	if noWorkers {
		if strings.ToLower(cfg.Database.Type) == "memory" {
			return errors.Newf(
				"--no-workers cannot be used with memory database backend. " +
					"The memory backend does not support distributed worker coordination. " +
					"Use database.type=cockroachdb for API-only mode.",
			)
		}
		// Use -1 as sentinel value to distinguish "explicitly disabled" from "not set"
		cfg.Tasks.Workers = -1
		mode = health.APIOnly
	}

	l := logger.NewLogger(cfg.Log.Level)

	// Create all services using the factory
	services, err := app.NewServicesFromConfig(cfg, l, mode)
	if err != nil {
		return errors.Wrap(err, "error creating services")
	}

	// Create the authenticator based on configuration
	authenticator, err := app.NewAuthenticatorFromConfig(cfg, l, services.Auth)
	if err != nil {
		return errors.Wrap(err, "error creating authenticator")
	}

	options := []app.IAppOption{
		app.WithLogger(l),
		app.WithApiTrustedProxies(cfg.Api.TrustedProxies),
		app.WithApiPort(cfg.Api.Port),
		app.WithApiBaseURL(cfg.Api.BasePath),
		app.WithApiMetrics(cfg.Api.Metrics.Enabled),
		app.WithApiMetricsPort(cfg.Api.Metrics.Port),
		app.WithApiAuthenticationHeader(cfg.Api.Authentication.Header),
		app.WithApiAuthenticator(authenticator),
		app.WithServices(services.ToSlice()),
		app.WithApiController(healthcontroller.NewController(services.Health)),
		app.WithApiController(clusterscontroller.NewController(services.Clusters)),
		app.WithApiController(publicdns.NewController(services.DNS)),
		app.WithApiController(tasks.NewController(services.Task)),
		app.WithApiController(authcontroller.NewController(services.Auth)),
	}

	if auth.AuthenticationType(strings.ToLower(cfg.Api.Authentication.Type)) == auth.AuthenticationTypeBearer {
		options = append(options,
			app.WithApiController(authcontroller.NewControllerBearerAuth(services.Auth)),
			app.WithApiController(admincontroller.NewController(services.Auth)),
			app.WithApiController(serviceaccountscontroller.NewController(services.Auth)),
			app.WithApiController(scimcontroller.NewController(services.Auth)),
		)
	}

	// Create the application instance with the configured services and controllers.
	// The application instance is responsible for starting the API server and
	// handling requests.
	application, err := app.NewApp(options...)
	if err != nil {
		return errors.Wrap(err, "error creating application")
	}

	// Start the application.
	// This will start the API server and all services.
	err = application.Start(context.Background())
	if err != nil {
		return errors.Wrap(err, "error starting application")
	}

	return nil
}
