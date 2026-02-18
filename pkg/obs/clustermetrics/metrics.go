// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clustermetrics

import "github.com/cockroachdb/cockroach/pkg/obs/clustermetrics/cmwriter"

type Gauge = cmwriter.Gauge
type Counter = cmwriter.Counter
type Stopwatch = cmwriter.Stopwatch

// Constructor re-exports so callers can use a single clustermetrics import.
var (
	NewCounter    = cmwriter.NewCounter
	NewGauge      = cmwriter.NewGauge
	NewStopwatch  = cmwriter.NewStopwatch
	NewGaugeVec   = cmwriter.NewGaugeVec
	NewCounterVec = cmwriter.NewCounterVec
	NewWriter     = cmwriter.NewWriter
)
