// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clustermetrics

import (
	"github.com/cockroachdb/cockroach/pkg/obs/clustermetrics/cmmetrics"
	"github.com/cockroachdb/cockroach/pkg/obs/clustermetrics/cmwriter"
)

type Gauge = cmmetrics.Gauge
type Counter = cmmetrics.Counter
type Stopwatch = cmmetrics.Stopwatch

// Re-exports so callers can use a single clustermetrics import.
var (
	NewCounter    = cmmetrics.NewCounter
	NewGauge      = cmmetrics.NewGauge
	NewStopwatch  = cmmetrics.NewStopwatch
	NewGaugeVec   = cmmetrics.NewGaugeVec
	NewCounterVec = cmmetrics.NewCounterVec
	NewWriter     = cmwriter.NewWriter
)

// Registry re-exports.
var (
	NewRegistry       = cmmetrics.NewRegistry
	NewRegistryReader = cmmetrics.NewRegistryReader
)

// Metadata registry API re-exports.
var (
	RegisterClusterMetric               = cmmetrics.RegisterClusterMetric
	RegisterLabeledClusterMetric        = cmmetrics.RegisterLabeledClusterMetric
	GetClusterMetricMetadata            = cmmetrics.GetClusterMetricMetadata
	TestingRegisterClusterMetric        = cmmetrics.TestingRegisterClusterMetric
	TestingRegisterLabeledClusterMetric = cmmetrics.TestingRegisterLabeledClusterMetric
	TestingAllowNonInitConstruction     = cmmetrics.TestingAllowNonInitConstruction
)
