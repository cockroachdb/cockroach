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

import (
	"github.com/VividCortex/ewma"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

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
	metaHeartbeatConnectionFailures = metric.Metadata{
		Name: "rpc.heartbeats.connection_failures",
		Help: `Counter of failed connections.

This includes both healthy connections that then terminated as well
as connection attempts that failed outright during initial dialing.

Connections that are terminated as part of shutdown are excluded.
`,
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	metaHeartbeatRoundTripLatency = metric.Metadata{
		Name:        "rpc.heartbeats.round-trip.latency",
		Unit:        metric.Unit_NANOSECONDS,
		Help:        "Moving average of round-trip latencies between nodes.",
		Measurement: "Latency",
	}
)

func makeMetrics() Metrics {
	return Metrics{
		HeartbeatLoopsStarted:       aggmetric.NewCounter(metaHeartbeatLoopsStarted, "dst"),
		HeartbeatConnectionFailures: aggmetric.NewCounter(metaHeartbeatConnectionFailures, "dst"),
		HeartbeatsNominal:           aggmetric.NewGauge(metaHeartbeatsNominal, "dst"),
		HeartbeatRoundTripLatency: aggmetric.NewHistogram(metric.HistogramOptions{
			Mode:     metric.HistogramModePrometheus,
			Metadata: metaHeartbeatRoundTripLatency,
			Duration: base.DefaultHistogramWindowInterval(),
			Buckets:  metric.IOLatencyBuckets,
		}, "dst"),
	}
}

// Metrics is a metrics struct for Context metrics.
// Field X is documented in metaX.
type Metrics struct {
	HeartbeatLoopsStarted       *aggmetric.AggCounter
	HeartbeatConnectionFailures *aggmetric.AggCounter
	HeartbeatsNominal           *aggmetric.AggGauge
	HeartbeatRoundTripLatency   *aggmetric.AggHistogram

	mu struct {
		syncutil.Mutex
		nm map[roachpb.NodeID]NodeMetrics
	}
}

type EwmaHistogram struct {
	syncutil.RWMutex
	*aggmetric.Histogram
	ma ewma.MovingAverage
}

type NodeMetrics struct {
	HeartbeatLoopsStarted       *aggmetric.Counter
	HeartbeatConnectionFailures *aggmetric.Counter
	HeartbeatsNominal           *aggmetric.Gauge
	HeartbeatRoundTripLatency   *EwmaHistogram
}

func (m *Metrics) loadNodeMetrics(nodeID roachpb.NodeID) NodeMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()
	if nm, ok := m.mu.nm[nodeID]; ok {
		return nm
	}
	if m.mu.nm == nil {
		m.mu.nm = map[roachpb.NodeID]NodeMetrics{}
	}
	nm := NodeMetrics{
		HeartbeatLoopsStarted:       m.HeartbeatLoopsStarted.AddChild(nodeID.String()),
		HeartbeatConnectionFailures: m.HeartbeatConnectionFailures.AddChild(nodeID.String()),
		HeartbeatsNominal:           m.HeartbeatsNominal.AddChild(nodeID.String()),
		HeartbeatRoundTripLatency: &EwmaHistogram{
			Histogram: m.HeartbeatRoundTripLatency.AddChild(nodeID.String()),
			ma:        ewma.NewMovingAverage(),
		},
	}
	m.mu.nm[nodeID] = nm
	return nm
}
