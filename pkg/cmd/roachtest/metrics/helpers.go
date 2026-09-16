// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metrics

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/errors"
)

// CreateMetricsAPI creates a new MetricsAPI instance.
// A valid Prometheus configuration is required.
func CreateMetricsAPI(
	ctx context.Context, t test.Test, c cluster.Cluster, promCfg *prometheus.Config,
) (MetricsAPI, error) {
	// If no config provided, return an error
	if promCfg == nil {
		return nil, errors.New("prometheus config cannot be nil, ensure Prometheus is properly configured")
	}

	// Create the API
	api, err := NewMetricsAPI(ctx, t, c, promCfg)
	if err != nil {
		return nil, err
	}

	return api, nil
}
