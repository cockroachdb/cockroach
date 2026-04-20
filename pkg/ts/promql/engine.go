// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package promql

import (
	"time"

	"github.com/prometheus/prometheus/promql"
)

// NewDefaultEngine creates a Prometheus PromQL engine with reasonable defaults
// for querying CockroachDB's internal time series data.
func NewDefaultEngine() *promql.Engine {
	return promql.NewEngine(promql.EngineOpts{
		MaxSamples: 50_000_000,
		Timeout:    2 * time.Minute,
	})
}
