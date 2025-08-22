// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// metaStartupTestLastUpdated records when the startup test metric was last set.
// We set this once during node startup to the current Unix timestamp.
var metaStartupTestLastUpdated = metric.Metadata{
	Name:        "server.startup-test.last-updated",
	Help:        "Unix timestamp of when the startup test metric was set during server startup",
	Measurement: "Timestamp",
	Unit:        metric.Unit_TIMESTAMP_SEC,
}

// StartupTestMetrics contains a very simple metric that gets populated during
// server startup. It mirrors other startup-populated metrics by recording a
// timestamp in a gauge.
type StartupTestMetrics struct {
	// StartupLastUpdated is set to the Unix timestamp at server startup.
	StartupLastUpdated *metric.Gauge
}

// MetricStruct is part of the metric.Struct interface.
func (StartupTestMetrics) MetricStruct() {}

// MakeStartupTestMetrics constructs the startup test metrics and initializes
// the timestamp gauge to the current time.
func MakeStartupTestMetrics() StartupTestMetrics {
	g := metric.NewGauge(metaStartupTestLastUpdated)
	g.Update(timeutil.Now().Unix())
	return StartupTestMetrics{StartupLastUpdated: g}
}
