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

// We want to have a way to track the number of connection
// but we also want to have a way to know that connection health.
//
// For this we're going to add a variety of metrics.
// One will be a gauge of how many heartbeat loops are in which state
// and another will be a counter for heartbeat failures.

var (
	// The below gauges store the current state of running heartbeat loops.
	// Gauges are useful for examing the current state of a system but can hide
	// information is the face of rapidly changing values. The context
	// additionally keeps counters for the number of heartbeat loops started
	// and completed as well as a counter for the number of heartbeat failures.
	// Together these metrics should provide a picture of the state of current
	// connections.

	metaHeartbeatsInitializing = metric.Metadata{
		Name:        "rpc.heartbeats.initializing",
		Help:        "Gauge of current connections in the initializing state",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	metaHeartbeatsNominal = metric.Metadata{
		Name:        "rpc.heartbeats.nominal",
		Help:        "Gauge of current connections in the nominal state",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	metaHeartbeatsFailed = metric.Metadata{
		Name:        "rpc.heartbeats.failed",
		Help:        "Gauge of current connections in the failed state",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}

	metaHeartbeatLoopsStarted = metric.Metadata{
		Name: "rpc.heartbeats.loops.started",
		Help: "Counter of the number of connection heartbeat loops which " +
			"have been started",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	metaHeartbeatLoopsExited = metric.Metadata{
		Name: "rpc.heartbeats.loops.exited",
		Help: "Counter of the number of connection heartbeat loops which " +
			"have exited with an error",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
)

type heartbeatState int

const (
	heartbeatNotRunning heartbeatState = iota
	heartbeatInitializing
	heartbeatNominal
	heartbeatFailed
)

func makeMetrics() Metrics {
	return Metrics{
		HeartbeatLoopsStarted:  metric.NewCounter(metaHeartbeatLoopsStarted),
		HeartbeatLoopsExited:   metric.NewCounter(metaHeartbeatLoopsExited),
		HeartbeatsInitializing: metric.NewGauge(metaHeartbeatsInitializing),
		HeartbeatsNominal:      metric.NewGauge(metaHeartbeatsNominal),
		HeartbeatsFailed:       metric.NewGauge(metaHeartbeatsFailed),
	}
}

// Metrics is a metrics struct for Context metrics.
type Metrics struct {

	// HeartbeatLoopsStarted is a counter which tracks the number of heartbeat
	// loops which have been started.
	HeartbeatLoopsStarted *metric.Counter

	// HeartbeatLoopsExited is a counter which tracks the number of heartbeat
	// loops which have exited with an error. The only time a heartbeat loop
	// exits without an error is during server shutdown.
	HeartbeatLoopsExited *metric.Counter

	// HeartbeatsInitializing tracks the current number of heartbeat loops
	// which have not yet ever succeeded.
	HeartbeatsInitializing *metric.Gauge
	// HeartbeatsNominal tracks the current number of heartbeat loops which
	// succeeded on their previous attempt.
	HeartbeatsNominal *metric.Gauge
	// HeartbeatsNominal tracks the current number of heartbeat loops which
	// succeeded on their previous attempt.
	HeartbeatsFailed *metric.Gauge
}

// updateHeartbeatState decrements the gauge for the current state and
// increments the gauge for the new state, returning the new state.
func updateHeartbeatState(m *Metrics, old, new heartbeatState) heartbeatState {
	if old == new {
		return new
	}
	if g := heartbeatGauge(m, new); g != nil {
		g.Inc(1)
	}
	if g := heartbeatGauge(m, old); g != nil {
		g.Dec(1)
	}
	return new
}

// heartbeatGauge returns the appropriate gauge for the given heartbeatState.
func heartbeatGauge(m *Metrics, s heartbeatState) (g *metric.Gauge) {
	switch s {
	case heartbeatInitializing:
		g = m.HeartbeatsInitializing
	case heartbeatNominal:
		g = m.HeartbeatsNominal
	case heartbeatFailed:
		g = m.HeartbeatsFailed
	}
	return g
}
