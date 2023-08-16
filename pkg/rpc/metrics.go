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
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
)

var (
	// The below gauges store the current state of running heartbeat loops.
	// Gauges are useful for examining the current state of a system but can hide
	// information is the face of rapidly changing values. The context
	// additionally keeps counters for the number of heartbeat loops started
	// and completed as well as a counter for the number of heartbeat failures.
	// Together these metrics should provide a picture of the state of current
	// connections.

	metaConnectionHealthy = metric.Metadata{
		Name:        "rpc.connection.healthy",
		Help:        "Gauge of current connections in a healthy state (i.e. bidirectionally connected and heartbeating)",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}

	metaConnectionUnhealthy = metric.Metadata{
		Name:        "rpc.connection.unhealthy",
		Help:        "Gauge of current connections in an unhealthy state (not bidirectionally connected or heartbeating)",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}

	metaConnectionInactive = metric.Metadata{
		Name: "rpc.connection.inactive",
		Help: "Gauge of current connections in an inactive state and pending deletion; " +
			"these are not healthy but are not tracked as unhealthy either because " +
			"there is reason to believe that the connection is no longer relevant," +
			"for example if the node has since been seen under a new address",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}

	metaConnectionHealthyNanos = metric.Metadata{
		Name: "rpc.connection.healthy_nanos",
		Help: `Gauge of nanoseconds of healthy connection time

On the prometheus endpoint scraped with the cluster setting 'server.child_metrics.enabled' set,
the constituent parts of this metric are available on a per-peer basis and one can read off
for how long a given peer has been connected`,
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	metaConnectionUnhealthyNanos = metric.Metadata{
		Name: "rpc.connection.unhealthy_nanos",
		Help: `Gauge of nanoseconds of unhealthy connection time.

On the prometheus endpoint scraped with the cluster setting 'server.child_metrics.enabled' set,
the constituent parts of this metric are available on a per-peer basis and one can read off
for how long a given peer has been unreachable`,
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	metaConnectionHeartbeats = metric.Metadata{
		Name:        "rpc.connection.heartbeats",
		Help:        `Counter of successful heartbeats.`,
		Measurement: "Heartbeats",
		Unit:        metric.Unit_COUNT,
	}

	metaConnectionFailures = metric.Metadata{
		Name: "rpc.connection.failures",
		Help: `Counter of failed connections.

This includes both the event in which a healthy connection terminates as well as
unsuccessful reconnection attempts.

Connections that are terminated as part of local node shutdown are excluded.
Decommissioned peers are excluded.
`,
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}

	metaConnectionAvgRoundTripLatency = metric.Metadata{
		Name: "rpc.connection.avg_round_trip_latency",
		Unit: metric.Unit_NANOSECONDS,
		Help: `Sum of exponentially weighted moving average of round-trip latencies, as measured through a gRPC RPC.

Dividing this Gauge by rpc.connection.healthy gives an approximation of average
latency, but the top-level round-trip-latency histogram is more useful. Instead,
users should consult the label families of this metric if they are available
(which requires prometheus and the cluster setting 'server.child_metrics.enabled');
these provide per-peer moving averages.

This metric does not track failed connection. A failed connection's contribution
is reset to zero.
`,
		Measurement: "Latency",
	}
)

func makeMetrics() Metrics {
	childLabels := []string{"remote_node_id", "remote_addr", "class"}
	return Metrics{
		ConnectionHealthy:             aggmetric.NewGauge(metaConnectionHealthy, childLabels...),
		ConnectionUnhealthy:           aggmetric.NewGauge(metaConnectionUnhealthy, childLabels...),
		ConnectionInactive:            aggmetric.NewGauge(metaConnectionInactive, childLabels...),
		ConnectionHealthyFor:          aggmetric.NewGauge(metaConnectionHealthyNanos, childLabels...),
		ConnectionUnhealthyFor:        aggmetric.NewGauge(metaConnectionUnhealthyNanos, childLabels...),
		ConnectionHeartbeats:          aggmetric.NewCounter(metaConnectionHeartbeats, childLabels...),
		ConnectionFailures:            aggmetric.NewCounter(metaConnectionFailures, childLabels...),
		ConnectionAvgRoundTripLatency: aggmetric.NewGauge(metaConnectionAvgRoundTripLatency, childLabels...),
	}
}

// Metrics is a metrics struct for Context metrics.
// Field X is documented in metaX.
type Metrics struct {
	ConnectionHealthy             *aggmetric.AggGauge
	ConnectionUnhealthy           *aggmetric.AggGauge
	ConnectionInactive            *aggmetric.AggGauge
	ConnectionHealthyFor          *aggmetric.AggGauge
	ConnectionUnhealthyFor        *aggmetric.AggGauge
	ConnectionHeartbeats          *aggmetric.AggCounter
	ConnectionFailures            *aggmetric.AggCounter
	ConnectionAvgRoundTripLatency *aggmetric.AggGauge
}

// peerMetrics are metrics that are kept on a per-peer basis.
// Their lifecycle follows that of the associated peer, i.e.
// they are acquired on peer creation, and are released when
// the peer is destroyed.
type peerMetrics struct {
	// IMPORTANT: update Metrics.release when adding any gauges here. It's
	// notoriously easy to leak child gauge values when removing peers. All gauges
	// must be reset before removing, and there must not be any chance that
	// they're set again because even if they're unlinked from the parent, they
	// will continue to add to the parent!
	//
	// See TestMetricsRelease.

	// 1 on first heartbeat success (via reportHealthy), reset after
	// runHeartbeatUntilFailure returns.
	ConnectionHealthy *aggmetric.Gauge
	// Reset on first successful heartbeat (via reportHealthy), 1 after
	// runHeartbeatUntilFailure returns.
	ConnectionUnhealthy *aggmetric.Gauge
	// Set when the peer is inactive, i.e. `deleteAfter` is set but it is still in
	// the peer map (i.e. likely a superseded connection, but we're not sure yet).
	// For such peers the probe only runs on demand and the connection is not
	// healthy but also not tracked as unhealthy.
	ConnectionInactive *aggmetric.Gauge
	// Updated on each successful heartbeat from a local var, reset after
	// runHeartbeatUntilFailure returns.
	ConnectionHealthyFor *aggmetric.Gauge
	// Updated from p.mu.disconnected before each loop around in breakerProbe.run,
	// reset on first heartbeat success (via reportHealthy).
	ConnectionUnhealthyFor *aggmetric.Gauge
	// Updated on each successful heartbeat, reset (along with roundTripLatency)
	// after runHeartbeatUntilFailure returns.
	AvgRoundTripLatency *aggmetric.Gauge
	// roundTripLatency is the source for the AvgRoundTripLatency gauge. We don't
	// want to maintain a full histogram per peer, so instead on each heartbeat we
	// update roundTripLatency and flush the result into AvgRoundTripLatency.
	roundTripLatency ewma.MovingAverage // *not* thread safe

	// Counters.

	// Incremented after each successful heartbeat.
	ConnectionHeartbeats *aggmetric.Counter
	// Updated before each loop around in breakerProbe.run.
	ConnectionFailures *aggmetric.Counter
}

func (m *Metrics) acquire(k peerKey) peerMetrics {
	labelVals := []string{k.NodeID.String(), k.TargetAddr, k.Class.String()}
	return peerMetrics{
		ConnectionHealthy:      m.ConnectionHealthy.AddChild(labelVals...),
		ConnectionUnhealthy:    m.ConnectionUnhealthy.AddChild(labelVals...),
		ConnectionInactive:     m.ConnectionInactive.AddChild(labelVals...),
		ConnectionHealthyFor:   m.ConnectionHealthyFor.AddChild(labelVals...),
		ConnectionUnhealthyFor: m.ConnectionUnhealthyFor.AddChild(labelVals...),
		ConnectionHeartbeats:   m.ConnectionHeartbeats.AddChild(labelVals...),
		ConnectionFailures:     m.ConnectionFailures.AddChild(labelVals...),
		AvgRoundTripLatency:    m.ConnectionAvgRoundTripLatency.AddChild(labelVals...),

		// We use a SimpleEWMA which uses the zero value to mean "uninitialized"
		// and operates on a ~60s decay rate.
		//
		// Note that this is *not* thread safe.
		roundTripLatency: &ewma.SimpleEWMA{},
	}
}

func (pm *peerMetrics) release() {
	// All the gauges should be zero now, or the aggregate will be off forever.
	// Note that this isn't true for counters, as the aggregate *should* track
	// the count of all children that ever existed, even if they have been
	// released. (Releasing a peer doesn't "undo" past heartbeats).
	pm.ConnectionHealthy.Update(0)
	pm.ConnectionUnhealthy.Update(0)
	pm.ConnectionInactive.Update(0)
	pm.ConnectionHealthyFor.Update(0)
	pm.ConnectionUnhealthyFor.Update(0)
	pm.AvgRoundTripLatency.Update(0)
	pm.roundTripLatency.Set(0)

	pm.ConnectionHealthy.Unlink()
	pm.ConnectionUnhealthy.Unlink()
	pm.ConnectionInactive.Unlink()
	pm.ConnectionHealthyFor.Unlink()
	pm.ConnectionUnhealthyFor.Unlink()
	pm.ConnectionHeartbeats.Unlink()
	pm.ConnectionFailures.Unlink()
	pm.AvgRoundTripLatency.Unlink()
}
