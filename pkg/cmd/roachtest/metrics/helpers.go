// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metrics

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
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
	// Check if the cluster supports Prometheus
	if !c.IsLocal() && c.Spec().NodeCount < 4 {
		return nil, func() {}, errors.New(
			"cluster does not have enough nodes for Prometheus; need at least 4")
	}

	// Create a basic Prometheus configuration
	cfg := &prometheus.Config{}
	workloadNode := c.WorkloadNode().InstallNodes()[0]
	cfg.PrometheusNode = workloadNode

	// Set up Prometheus and Grafana
	if err := c.StartGrafana(ctx, t.L(), cfg); err != nil {
		return nil, func() {}, errors.Wrap(err, "failed to start Grafana")
	}

	// Create a cleanup function to be executed at test completion
	cleanup := func() {
		if err := c.StopGrafana(ctx, t.L(), t.ArtifactsDir()); err != nil {
			t.L().Printf("error shutting down Prometheus/Grafana: %s", err)
		}
	}

	return cfg, cleanup, nil
}

// HasMemoryLeakDetection creates assertions for memory leak detection.
// It returns a function that compares memory usage between the time the function is created
// and the time it's called, failing if memory has increased by more than the given percentage.
func HasMemoryLeakDetection(
	m MetricsAPI,
	allowedPercentageIncrease float64,
) func() error {
	startTime := time.Now()

	return func() error {
		endTime := time.Now()
		return m.CompareValuesOverTime("process_resident_memory_bytes", startTime, endTime).
			HasIncreasedByLessThan(allowedPercentageIncrease)
	}
}

// HasStableLatency creates assertions for stable latency during operations.
// It returns a function that can be called to check if latency percentiles stayed below thresholds.
func HasStableLatency(
	m MetricsAPI,
	latencyMetric string,
	percentileThresholds map[int]float64,
) func() error {
	return func() error {
		for percentile, threshold := range percentileThresholds {
			query := m.AssertThat(latencyMetric).OverLast("1m")
			if err := query.HasPercentile(percentile, threshold); err != nil {
				return err
			}
		}
		return nil
	}
}

// MonitorMetricsDuring runs the given operation while monitoring metrics,
// and asserts that all checkers pass after the operation completes.
func MonitorMetricsDuring(
	ctx context.Context,
	operation func() error,
	checkers ...func() error,
) error {
	// Run the operation
	if err := operation(); err != nil {
		return err
	}

	// Run all checkers
	var errors []string
	for _, checker := range checkers {
		if err := checker(); err != nil {
			errors = append(errors, err.Error())
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("metric assertions failed: %s", strings.Join(errors, "; "))
	}

	return nil
}

// CheckMetricValue is a helper that allows checking a metric value at any time
func CheckMetricValue(
	m MetricsAPI,
	metricName string,
	checkFn func(float64) error,
) error {
	value, err := m.AssertThat(metricName).Value()
	if err != nil {
		return err
	}

	return checkFn(value)
}

// ThroughputChecker creates a function that checks if throughput is at least a certain value
func ThroughputChecker(
	m MetricsAPI,
	throughputMetric string,
	minThroughput float64,
) func() error {
	return func() error {
		return m.AssertThat(throughputMetric).
			OverLast("1m").
			HasRateAtLeast(minThroughput)
	}
}
