// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metrics

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/errors"
)

// GetOrCreateMetricsAPI gets or creates a new MetricsAPI instance.
// If promCfg is nil, it attempts to set up Prometheus. Returns the metrics API and an error if one occurred.
func GetOrCreateMetricsAPI(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	promCfg *prometheus.Config,
) (MetricsAPI, error) {
	var err error
	var cleanup func()

	// If no config provided, set up Prometheus
	if promCfg == nil {
		promCfg, cleanup, err = setupDefaultPrometheus(ctx, t, c)
		if err != nil {
			return nil, err
		}
		// Ensure cleanup happens
		defer func() {
			if err != nil {
				cleanup()
			}
		}()
	}

	// Create the API
	api, err := NewMetricsAPI(ctx, t, c, promCfg)
	if err != nil {
		return nil, err
	}

	return api, nil
}

// setupDefaultPrometheus creates a default Prometheus configuration.
func setupDefaultPrometheus(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
) (*prometheus.Config, func(), error) {
	// Check if we should allow local Prometheus testing
	if c.IsLocal() {
		if os.Getenv("RUN_LOCAL_PROMETHEUS") != "1" {
			return nil, func() {}, errors.New(
				"prometheus is not supported for local clusters. " +
					"Set RUN_LOCAL_PROMETHEUS=1 to enable local Prometheus testing")
		}

		// For local testing, create a config that points to a local Prometheus instance
		t.Status("setting up connection to local Prometheus instance")

		// Get Prometheus address from env or use default
		promAddr := os.Getenv("PROMETHEUS_ADDR")
		if promAddr == "" {
			promAddr = "http://localhost:9090"
		}

		// Create a basic config for local Prometheus
		cfg := &prometheus.Config{}

		// No cleanup needed for local Prometheus
		return cfg, func() {}, nil
	}

	// Create a comprehensive Prometheus configuration for non-local clusters
	t.Status(fmt.Sprintf("setting up prometheus/grafana (<%s)", 2*time.Minute))
	cfg := &prometheus.Config{}
	workloadNode := c.WorkloadNode().InstallNodes()[0]
	cfg.WithPrometheusNode(workloadNode)
	cfg.WithNodeExporter(c.CRDBNodes().InstallNodes())
	cfg.WithCluster(c.CRDBNodes().InstallNodes())

	// Use a quiet logger for Grafana operations
	quietLogger, err := t.L().ChildLogger("start-grafana", logger.QuietStdout, logger.QuietStderr)
	if err != nil {
		return nil, func() {}, errors.Wrap(err, "failed to create quiet logger")
	}

	// Start Grafana
	if err := c.StartGrafana(ctx, quietLogger, cfg); err != nil {
		return nil, func() {}, errors.Wrap(err, "failed to start Grafana")
	}

	// Create a cleanup function to be executed at test completion
	cleanup := func() {
		if t.IsDebug() {
			return // nothing to do in debug mode
		}
		if err := c.StopGrafana(ctx, quietLogger, t.ArtifactsDir()); err != nil {
			t.L().ErrorfCtx(ctx, "error(s) shutting down prom/grafana: %s", err)
		}
	}

	return cfg, cleanup, nil
}
