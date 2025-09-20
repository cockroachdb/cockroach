// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/app"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/config"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/clusters"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/health"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/tasks"
	cstore "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/clusters/memory"
	tstore "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks/memory"
	sclusters "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters"
	stasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks"
)

const (
	EnvPrefix = "ROACHPROD"
)

func main() {

	cfg, err := config.Load(EnvPrefix)
	if err != nil {
		fmt.Printf("Error loading config: %v\n", err)
		os.Exit(1)
	}

	appCtx := context.Background()

	taskService := stasks.NewService(
		tstore.NewTasksRepository(),
		stasks.Options{
			Workers: 1,
		},
	)

	clustersService := sclusters.NewService(
		cstore.NewClustersRepository(),
		taskService,
		sclusters.Options{
			PeriodicRefreshEnabled: true,
		},
	)

	app, err := app.NewApp(
		app.WithAppLogLevel(cfg.LogLevel),
		app.WithApiPort(cfg.ApiPort),
		app.WithApiBaseURL(cfg.ApiBaseURL),
		app.WithApiMetrics(cfg.ApiMetrics),
		app.WithApiMetricsPort(cfg.ApiMetricsPort),
		app.WithApiAuthenticationDisabled(cfg.ApiAuthenticationDisabled),
		app.WithApiAuthenticationHeader(cfg.ApiAuthenticationHeader),
		app.WithApiAuthenticationAudience(cfg.ApiAuthenticationAudience),
		app.WithService(clustersService),
		app.WithService(taskService),
		app.WithApiController(health.NewController()),
		app.WithApiController(clusters.NewController(clustersService)),
		app.WithApiController(tasks.NewController(taskService)),
	)
	if err != nil {
		os.Exit(1)
	}

	// Start the server
	err = app.Start(appCtx)
	if err != nil {
		os.Exit(1)
	}
}
