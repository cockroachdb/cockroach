// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import "github.com/cockroachdb/cockroach/pkg/util/metric"

var (
	// The below gauges store the current state of running heartbeat loops.
	// Gauges are useful for examining the current state of a system but can hide
	// information is the face of rapidly changing values. The context
	// additionally keeps counters for the number of heartbeat loops started
	// and completed as well as a counter for the number of heartbeat failures.
	// Together these metrics should provide a picture of the state of current
	// connections.

	metaHeartbeatsNominal = metric.Metadata{
		Name:        "rpc.heartbeats.nominal",
		Help:        "Gauge of current connections in the nominal state (i.e. at least one successful heartbeat)",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}

	metaHeartbeatLoopsStarted = metric.Metadata{
		Name:        "rpc.heartbeats.loops.started",
		Help:        "Counter of connection attempts",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	metaHeartbeatLoopsExited = metric.Metadata{
		Name: "rpc.heartbeats.loops.exited",
		Help: `Counter of failed connections.

This includes both healthy connections that then terminated as well
as connection attempts that failed outright during initial dialing.

Connections that are terminated as part of shutdown are excluded.
`,
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
)

func makeMetrics() Metrics {
	return Metrics{
		HeartbeatLoopsStarted: metric.NewCounter(metaHeartbeatLoopsStarted),
		HeartbeatLoopsExited:  metric.NewCounter(metaHeartbeatLoopsExited),
		HeartbeatsNominal:     metric.NewGauge(metaHeartbeatsNominal),
	}
}

// Metrics is a metrics struct for Context metrics.
// Field X is documented in metaX.
type Metrics struct {
	HeartbeatLoopsStarted *metric.Counter
	HeartbeatLoopsExited  *metric.Counter
	HeartbeatsNominal     *metric.Gauge
}
