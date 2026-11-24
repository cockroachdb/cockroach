// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clustermetrics

import (
	"github.com/cockroachdb/cockroach/pkg/obs/clustermetrics/cmmetrics"
	"github.com/cockroachdb/cockroach/pkg/obs/clustermetrics/cmreader"
	"github.com/cockroachdb/cockroach/pkg/obs/clustermetrics/cmwriter"
)

type Gauge = cmmetrics.Gauge
type Counter = cmmetrics.Counter
type WriteStopwatch = cmmetrics.WriteStopwatch

// Re-exports so callers can use a single clustermetrics import.
var (
	NewCounter           = cmmetrics.NewCounter
	NewGauge             = cmmetrics.NewGauge
	NewWriteStopwatch    = cmmetrics.NewWriteStopwatch
	NewGaugeVec          = cmmetrics.NewGaugeVec
	NewCounterVec        = cmmetrics.NewCounterVec
	NewWriteStopwatchVec = cmmetrics.NewWriteStopwatchVec
	NewWriter            = cmwriter.NewWriter
)

// NewRegistryReader creates a new cluster metric registry exposed as
// a read-only metric.RegistryReader. The concrete type is
// package-private to cmreader, preventing external callers from
// mutating the registry.
var NewRegistryReader = cmreader.NewRegistryReader

// Metadata registry API re-exports.
var (
	RegisterClusterMetric               = cmmetrics.RegisterClusterMetric
	RegisterLabeledClusterMetric        = cmmetrics.RegisterLabeledClusterMetric
	GetClusterMetricMetadata            = cmmetrics.GetClusterMetricMetadata
	TestingRegisterClusterMetric        = cmmetrics.TestingRegisterClusterMetric
	TestingRegisterLabeledClusterMetric = cmmetrics.TestingRegisterLabeledClusterMetric
	TestingAllowNonInitConstruction     = cmmetrics.TestingAllowNonInitConstruction
)
