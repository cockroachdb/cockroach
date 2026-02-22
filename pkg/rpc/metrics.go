// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	prometheusgo "github.com/prometheus/client_model/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"storj.io/drpc"
	"storj.io/drpc/drpcclient"
	"storj.io/drpc/drpcmux"
)

// gwRequestKey is a field set on the context to indicate a request
// is coming from RPC gateway.
const gwRequestKey = "gw-request"

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
		Visibility:  metric.Metadata_ESSENTIAL,
		Category:    metric.Metadata_NETWORKING,
		HowToUse:    `See Description.`,
	}

	metaConnectionUnhealthy = metric.Metadata{
		Name:        "rpc.connection.unhealthy",
		Help:        "Gauge of current connections in an unhealthy state (not bidirectionally connected or heartbeating)",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
		Visibility:  metric.Metadata_ESSENTIAL,
		Category:    metric.Metadata_NETWORKING,
		HowToUse:    `If the value of this metric is greater than 0, this could indicate a network partition.`,
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
		Visibility:  metric.Metadata_ESSENTIAL,
		Category:    metric.Metadata_NETWORKING,
		HowToUse:    `This can be useful for monitoring the stability and health of connections within your CockroachDB cluster.`,
	}

	metaConnectionUnhealthyNanos = metric.Metadata{
		Name: "rpc.connection.unhealthy_nanos",
		Help: `Gauge of nanoseconds of unhealthy connection time.

On the prometheus endpoint scraped with the cluster setting 'server.child_metrics.enabled' set,
the constituent parts of this metric are available on a per-peer basis and one can read off
for how long a given peer has been unreachable`,
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
		Visibility:  metric.Metadata_ESSENTIAL,
		Category:    metric.Metadata_NETWORKING,
		HowToUse:    `If this duration is greater than 0, this could indicate how long a network partition has been occurring.`,
	}

	metaConnectionHeartbeats = metric.Metadata{
		Name:        "rpc.connection.heartbeats",
		Help:        `Counter of successful heartbeats.`,
		Measurement: "Heartbeats",
		Unit:        metric.Unit_COUNT,
		Visibility:  metric.Metadata_ESSENTIAL,
		Category:    metric.Metadata_NETWORKING,
		HowToUse:    `See Description.`,
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
		Visibility:  metric.Metadata_ESSENTIAL,
		Category:    metric.Metadata_NETWORKING,
		HowToUse:    `See Description.`,
	}

	metaConnectionAvgRoundTripLatency = metric.Metadata{
		Name: "rpc.connection.avg_round_trip_latency",
		Unit: metric.Unit_NANOSECONDS,
		Help: `Sum of exponentially weighted moving average of round-trip latencies, as measured through a gRPC RPC.

Since this metric is based on gRPC RPCs, it is affected by application-level
processing delays and CPU overload effects. See rpc.connection.tcp_rtt for a
metric that is obtained from the kernel's TCP stack.

Dividing this Gauge by rpc.connection.healthy gives an approximation of average
latency, but the top-level round-trip-latency histogram is more useful. Instead,
users should consult the label families of this metric if they are available
(which requires prometheus and the cluster setting 'server.child_metrics.enabled');
these provide per-peer moving averages.

This metric does not track failed connection. A failed connection's contribution
is reset to zero.
`,
		Measurement: "Latency",
		Visibility:  metric.Metadata_ESSENTIAL,
		Category:    metric.Metadata_NETWORKING,
		HowToUse:    `This metric is helpful in understanding general network issues outside of CockroachDB that could be impacting the user’s workload.`,
	}

	metaConnectionTCPRTT = metric.Metadata{
		Name: "rpc.connection.tcp_rtt",
		Unit: metric.Unit_NANOSECONDS,
		Help: `Kernel-level TCP round-trip time as measured by the Linux TCP stack.

This metric reports the smoothed round-trip time (SRTT) as maintained by the
kernel's TCP implementation. Unlike application-level RPC latency measurements,
this reflects pure network latency and is less affected by CPU overload effects.

This metric is only available on Linux.
`,
		Measurement: "Latency",
		Visibility:  metric.Metadata_ESSENTIAL,
		Category:    metric.Metadata_NETWORKING,
		HowToUse:    `High TCP RTT values indicate network issues outside of CockroachDB that could be impacting the user's workload.`,
	}

	metaConnectionTCPRTTVar = metric.Metadata{
		Name: "rpc.connection.tcp_rtt_var",
		Unit: metric.Unit_NANOSECONDS,
		Help: `Kernel-level TCP round-trip time variance as measured by the Linux TCP stack.

This metric reports the smoothed round-trip time variance (RTTVAR) as maintained
by the kernel's TCP implementation. This measures the stability of the
connection latency.

This metric is only available on Linux.
`,
		Measurement: "Latency Variance",
		Visibility:  metric.Metadata_ESSENTIAL,
		Category:    metric.Metadata_NETWORKING,
		HowToUse:    `High TCP RTT variance values indicate network stability issues outside of CockroachDB that could be impacting the user's workload.`,
	}
	metaConnectionConnected = metric.Metadata{
		Name: "rpc.connection.connected",
		Help: `Counter of TCP level connected connections.

This metric is the number of gRPC connections from the TCP level. Unlike rpc.connection.healthy
this metric does not take into account whether the application has been able to heartbeat
over this connection.
`,
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	metaNetworkBytesEgress = metric.Metadata{
		Name:        "rpc.client.bytes.egress",
		Unit:        metric.Unit_BYTES,
		Help:        `Counter of TCP bytes sent via gRPC on connections we initiated.`,
		Measurement: "Bytes",
	}
	metaNetworkBytesIngress = metric.Metadata{
		Name:        "rpc.client.bytes.ingress",
		Unit:        metric.Unit_BYTES,
		Help:        `Counter of TCP bytes received via gRPC on connections we initiated.`,
		Measurement: "Bytes",
	}
	metaRequestHandledDuration = metric.Metadata{
		Name:        "rpc.server.request.duration.nanos",
		Help:        "Duration of an RPC request in nanoseconds.",
		Measurement: "Duration",
		Unit:        metric.Unit_NANOSECONDS,
		MetricType:  prometheusgo.MetricType_HISTOGRAM,
	}
	metaServerRequestStarted = metric.Metadata{
		Name:        "rpc.server.started.total",
		Help:        "Total number of RPCs started on the server.",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
		MetricType:  prometheusgo.MetricType_COUNTER,
	}
	metaServerRequestHandled = metric.Metadata{
		Name:        "rpc.server.handled.total",
		Help:        "Total number of RPCs completed on the server, regardless of success or failure.",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
		MetricType:  prometheusgo.MetricType_COUNTER,
	}
	metaServerRequestHandledDuration = metric.Metadata{
		Name: "rpc.server.handled.nanos",
		Help: "Histogram of response latency (" +
			"nanoseconds) of RPC that had been application-level handled by the" +
			" server.",
		Measurement: "Duration",
		Unit:        metric.Unit_NANOSECONDS,
		MetricType:  prometheusgo.MetricType_HISTOGRAM,
	}
	metaServerMsgSent = metric.Metadata{
		Name:        "rpc.server.msg.sent.total",
		Help:        "Total number of RPC messages sent by the server.",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
		MetricType:  prometheusgo.MetricType_COUNTER,
	}
	metaServerMsgReceived = metric.Metadata{
		Name:        "rpc.server.msg.received.total",
		Help:        "Total number of RPC messages received by the server.",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
		MetricType:  prometheusgo.MetricType_COUNTER,
	}
	metaServerErrors = metric.Metadata{
		Name:        "rpc.server.errors.total",
		Help:        "Total number of RPC errors on the server.",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
		MetricType:  prometheusgo.MetricType_COUNTER,
	}
	metaClientRequestStarted = metric.Metadata{
		Name:        "rpc.client.started.total",
		Help:        "Total number of RPCs started by the client.",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
		MetricType:  prometheusgo.MetricType_COUNTER,
	}
	metaClientRequestHandled = metric.Metadata{
		Name: "rpc.client.handled.total",
		Help: "Total number of RPCs completed on the client, " +
			"regardless of success or failure.",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
		MetricType:  prometheusgo.MetricType_COUNTER,
	}
	metaClientRequestHandledDuration = metric.Metadata{
		Name: "rpc.client.handled.duration.nanos",
		Help: "Histogram of response latency (" +
			"nanoseconds) of the RPC until it is finished by the application.",
		Measurement: "Duration",
		Unit:        metric.Unit_NANOSECONDS,
		MetricType:  prometheusgo.MetricType_HISTOGRAM,
	}
	metaClientMsgSent = metric.Metadata{
		Name:        "rpc.client.msg.sent.total",
		Help:        "Total number of RPC messages sent by the client.",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
		MetricType:  prometheusgo.MetricType_COUNTER,
	}
	metaClientMsgReceived = metric.Metadata{
		Name:        "rpc.client.msg.received.total",
		Help:        "Total number of RPC messages received by the client.",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
		MetricType:  prometheusgo.MetricType_COUNTER,
	}
	metaClientErrors = metric.Metadata{
		Name:        "rpc.client.errors.total",
		Help:        "Total number of RPC errors on the client.",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
		MetricType:  prometheusgo.MetricType_COUNTER,
	}
	metaClientMsgSendDuration = metric.Metadata{
		Name: "rpc.client.msg.send.duration.nanos",
		Help: "Histogram of response latency (" +
			"nanoseconds) of the RPC single message send.",
		Measurement: "Duration",
		Unit:        metric.Unit_NANOSECONDS,
		MetricType:  prometheusgo.MetricType_HISTOGRAM,
	}
	metaClientMsgReceivedDuration = metric.Metadata{
		Name: "rpc.client.msg.received.duration.nanos",
		Help: "Histogram of response latency (" +
			"nanoseconds) of the RPC single message receive.",
		Measurement: "Duration",
		Unit:        metric.Unit_NANOSECONDS,
		MetricType:  prometheusgo.MetricType_HISTOGRAM,
	}
)

func (m *Metrics) makeLabels(k peerKey, remoteLocality roachpb.Locality) []string {
	localLen := len(m.locality.Tiers)

	// length is the shorter of the two, however we always need to fill localLen "slots"
	length := localLen
	if len(remoteLocality.Tiers) < length {
		length = len(remoteLocality.Tiers)
	}

	childLabels := []string{}

	matching := true
	for i := 0; i < length; i++ {
		childLabels = append(childLabels, m.locality.Tiers[i].Value)
		if matching {
			childLabels = append(childLabels, remoteLocality.Tiers[i].Value)
			if m.locality.Tiers[i].Value != remoteLocality.Tiers[i].Value {
				matching = false
			}
		} else {
			// Once we have a difference in locality, pad with empty strings.
			childLabels = append(childLabels, "")
		}
	}
	// Pad with empty strings if the remote locality is shorter than ours.
	for i := length; i < localLen; i++ {
		childLabels = append(childLabels, m.locality.Tiers[i].Value)
		childLabels = append(childLabels, "")
	}
	return childLabels
}

func newMetrics(locality roachpb.Locality) *Metrics {
	childLabels := []string{"remote_node_id", "remote_addr", "class", "protocol"}
	localityLabels := []string{}
	for _, tier := range locality.Tiers {
		localityLabels = append(localityLabels, "source_"+tier.Key)
		localityLabels = append(localityLabels, "destination_"+tier.Key)
	}
	m := Metrics{
		locality:                      locality,
		ConnectionHealthy:             aggmetric.NewGauge(metaConnectionHealthy, childLabels...),
		ConnectionUnhealthy:           aggmetric.NewGauge(metaConnectionUnhealthy, childLabels...),
		ConnectionInactive:            aggmetric.NewGauge(metaConnectionInactive, childLabels...),
		ConnectionHealthyFor:          aggmetric.NewGauge(metaConnectionHealthyNanos, childLabels...),
		ConnectionUnhealthyFor:        aggmetric.NewGauge(metaConnectionUnhealthyNanos, childLabels...),
		ConnectionHeartbeats:          aggmetric.NewCounter(metaConnectionHeartbeats, childLabels...),
		ConnectionFailures:            aggmetric.NewCounter(metaConnectionFailures, childLabels...),
		ConnectionConnected:           aggmetric.NewGauge(metaConnectionConnected, localityLabels...),
		ConnectionBytesSent:           aggmetric.NewCounter(metaNetworkBytesEgress, localityLabels...),
		ConnectionBytesRecv:           aggmetric.NewCounter(metaNetworkBytesIngress, localityLabels...),
		ConnectionAvgRoundTripLatency: aggmetric.NewGauge(metaConnectionAvgRoundTripLatency, childLabels...),
		ConnectionTCPRTT:              aggmetric.NewGauge(metaConnectionTCPRTT, childLabels...),
		ConnectionTCPRTTVar:           aggmetric.NewGauge(metaConnectionTCPRTTVar, childLabels...),
	}
	m.mu.peerMetrics = make(map[string]peerMetrics)
	m.mu.localityMetrics = make(map[string]localityMetrics)
	return &m
}

type ThreadSafeMovingAverage struct {
	syncutil.Mutex
	ma ewma.MovingAverage
}

func (t *ThreadSafeMovingAverage) Set(v float64) {
	t.Lock()
	defer t.Unlock()
	t.ma.Set(v)
}

func (t *ThreadSafeMovingAverage) Add(v float64) {
	t.Lock()
	defer t.Unlock()
	t.ma.Add(v)
}

func (t *ThreadSafeMovingAverage) Value() float64 {
	t.Lock()
	defer t.Unlock()
	return t.ma.Value()
}

// Metrics is a metrics struct for Context metrics.
// Field X is documented in metaX.
type Metrics struct {
	locality                      roachpb.Locality
	ConnectionHealthy             *aggmetric.AggGauge
	ConnectionUnhealthy           *aggmetric.AggGauge
	ConnectionInactive            *aggmetric.AggGauge
	ConnectionHealthyFor          *aggmetric.AggGauge
	ConnectionUnhealthyFor        *aggmetric.AggGauge
	ConnectionHeartbeats          *aggmetric.AggCounter
	ConnectionFailures            *aggmetric.AggCounter
	ConnectionConnected           *aggmetric.AggGauge
	ConnectionBytesSent           *aggmetric.AggCounter
	ConnectionBytesRecv           *aggmetric.AggCounter
	ConnectionAvgRoundTripLatency *aggmetric.AggGauge
	ConnectionTCPRTT              *aggmetric.AggGauge
	ConnectionTCPRTTVar           *aggmetric.AggGauge
	mu                            struct {
		syncutil.Mutex
		// peerMetrics is a map of peerKey to peerMetrics.
		peerMetrics map[string]peerMetrics
		// localityMetrics is a map of localityKey to localityMetrics.
		localityMetrics map[string]localityMetrics
	}
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

	// The invariant is that sum(ConnectionHealthy, ConnectionUnhealthy,
	// ConnectionInactive) == 1 for any connection that run() has been called
	// on. A connection begins in an unhealthy state prior to connection
	// attempt, it then transitions to healthy if the connection succeeds and
	// stays there until it disconnects and then transitions to either unhealthy
	// or inactive depending on whether it is going to attempt to reconnect. Any
	// increment to one of these counter always has to decrement another one to
	// keep this invariant.
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
	// TCP-level round trip time as measured by the kernel's TCP stack.
	// This provides network-level latency without application overhead.
	TCPRTT *aggmetric.Gauge
	// TCP-level round trip time variance as measured by the kernel's TCP stack.
	// This indicates connection stability and jitter.
	TCPRTTVar *aggmetric.Gauge
	// roundTripLatency is the source for the AvgRoundTripLatency gauge. We don't
	// want to maintain a full histogram per peer, so instead on each heartbeat we
	// update roundTripLatency and flush the result into AvgRoundTripLatency.
	roundTripLatency ewma.MovingAverage

	// Counters.

	// Incremented after each successful heartbeat.
	ConnectionHeartbeats *aggmetric.Counter
	// Updated before each loop around in breakerProbe.run.
	ConnectionFailures *aggmetric.Counter
}

type localityMetrics struct {
	ConnectionConnected *aggmetric.Gauge
	ConnectionBytesSent *aggmetric.Counter
	ConnectionBytesRecv *aggmetric.Counter
}

func (m *Metrics) acquire(
	k peerKey, l roachpb.Locality, rpcProtocol string,
) (peerMetrics, localityMetrics) {
	m.mu.Lock()
	defer m.mu.Unlock()
	labelVals := []string{k.NodeID.String(), k.TargetAddr, k.Class.String(), rpcProtocol}
	labelKey := strings.Join(labelVals, ",")
	pm, ok := m.mu.peerMetrics[labelKey]
	if !ok {
		pm = peerMetrics{
			ConnectionHealthy:      m.ConnectionHealthy.AddChild(labelVals...),
			ConnectionUnhealthy:    m.ConnectionUnhealthy.AddChild(labelVals...),
			ConnectionInactive:     m.ConnectionInactive.AddChild(labelVals...),
			ConnectionHealthyFor:   m.ConnectionHealthyFor.AddChild(labelVals...),
			ConnectionUnhealthyFor: m.ConnectionUnhealthyFor.AddChild(labelVals...),
			ConnectionHeartbeats:   m.ConnectionHeartbeats.AddChild(labelVals...),
			ConnectionFailures:     m.ConnectionFailures.AddChild(labelVals...),
			AvgRoundTripLatency:    m.ConnectionAvgRoundTripLatency.AddChild(labelVals...),
			TCPRTT:                 m.ConnectionTCPRTT.AddChild(labelVals...),
			TCPRTTVar:              m.ConnectionTCPRTTVar.AddChild(labelVals...),
			// We use a SimpleEWMA which uses the zero value to mean "uninitialized"
			// and operates on a ~60s decay rate.
			roundTripLatency: &ThreadSafeMovingAverage{ma: &ewma.SimpleEWMA{}},
		}
		m.mu.peerMetrics[labelKey] = pm
	}

	localityLabels := m.makeLabels(k, l)
	localityKey := strings.Join(localityLabels, ",")
	lm, ok := m.mu.localityMetrics[localityKey]
	if !ok {
		lm = localityMetrics{
			ConnectionConnected: m.ConnectionConnected.AddChild(localityLabels...),
			ConnectionBytesSent: m.ConnectionBytesSent.AddChild(localityLabels...),
			ConnectionBytesRecv: m.ConnectionBytesRecv.AddChild(localityLabels...),
		}
		m.mu.localityMetrics[localityKey] = lm
	}

	// We temporarily increment the inactive count until we actually connect.
	pm.ConnectionInactive.Inc(1)
	return pm, lm
}

// ** DRPC request metrics implementation ** //

const (
	RPCMethodLabel     = "methodName"
	RPCStatusCodeLabel = "statusCode"
	RPCProtocolLabel   = "protocol"
	RPCTypeLabel       = "rpcType"
	RPCServiceLabel    = "rpcService"

	rpcProtocolGRPC = "grpc"
	rpcProtocolDRPC = "drpc"
)

type drpcType string

const (
	drpcUnary  drpcType = "unary"
	drpcStream drpcType = "stream"
)

func splitDRPCMethod(rpc string) (string, string) {
	rpc = strings.TrimPrefix(rpc, "/") // remove leading slash
	if i := strings.Index(rpc, "/"); i >= 0 {
		return rpc[:i], rpc[i+1:]
	}
	return "unknown", "unknown"
}

func drpcCodeString(err error) string {
	return status.Code(err).String()
}

type drpcCallMeta struct {
	drpcType    string
	drpcService string
	drpcMethod  string
}

func newDRPCCallMeta(rpc string, isStream bool) drpcCallMeta {
	typ := drpcUnary
	if isStream {
		typ = drpcStream
	}
	service, method := splitDRPCMethod(rpc)
	return drpcCallMeta{
		drpcType:    string(typ),
		drpcService: service,
		drpcMethod:  method,
	}
}

type drpcReport struct {
	callMeta  drpcCallMeta
	startTime time.Time
}

type drpcMetricsReporter interface {
	PostCall(err error, rpcDuration time.Duration)
	PostMsgSend(sendDuration time.Duration)
	PostMsgReceive(recvDuration time.Duration)
}

type drpcMetricsKind int

const (
	drpcMetricsClient drpcMetricsKind = iota
	drpcMetricsServer
)

// DRPCClientRequestMetrics contains client metrics for DRPC requests.
type DRPCClientRequestMetrics struct {
	ClientStarted         *metric.CounterVec
	ClientHandled         *metric.CounterVec
	ClientMsgSent         *metric.CounterVec
	ClientMsgReceived     *metric.CounterVec
	ClientHandledDuration *metric.HistogramVec
	ClientMsgSendDuration *metric.HistogramVec
	ClientMsgRecvDuration *metric.HistogramVec
	ClientErrors          *metric.CounterVec
}

// DRPCServerRequestMetrics contains server metrics for DRPC requests.
type DRPCServerRequestMetrics struct {
	ServerStarted         *metric.CounterVec
	ServerHandled         *metric.CounterVec
	ServerMsgSent         *metric.CounterVec
	ServerMsgReceived     *metric.CounterVec
	ServerHandledDuration *metric.HistogramVec
	ServerErrors          *metric.CounterVec
}

type drpcReporter struct {
	kind          drpcMetricsKind
	clientMetrics *DRPCClientRequestMetrics
	serverMetrics *DRPCServerRequestMetrics
	baseLabels    map[string]string // type, service, method
	codeLabels    map[string]string // type, service, method, code (code mutated per-call)
}

func (r *drpcReporter) PostCall(err error, rpcDuration time.Duration) {
	r.codeLabels[RPCStatusCodeLabel] = drpcCodeString(err)
	switch r.kind {
	case drpcMetricsClient:
		r.clientMetrics.ClientHandled.Inc(r.baseLabels, 1)
		r.clientMetrics.ClientHandledDuration.Observe(r.codeLabels, float64(rpcDuration.Nanoseconds()))
		if err != nil {
			r.clientMetrics.ClientErrors.Inc(r.codeLabels, 1)
		}
	case drpcMetricsServer:
		r.serverMetrics.ServerHandled.Inc(r.baseLabels, 1)
		r.serverMetrics.ServerHandledDuration.Observe(r.codeLabels, float64(rpcDuration.Nanoseconds()))
		if err != nil {
			r.serverMetrics.ServerErrors.Inc(r.codeLabels, 1)
		}
	}
}

func (r *drpcReporter) PostMsgSend(sendDuration time.Duration) {
	switch r.kind {
	case drpcMetricsClient:
		r.clientMetrics.ClientMsgSent.Inc(r.baseLabels, 1)
		r.clientMetrics.ClientMsgSendDuration.Observe(r.baseLabels, float64(sendDuration.Nanoseconds()))
	case drpcMetricsServer:
		r.serverMetrics.ServerMsgSent.Inc(r.baseLabels, 1)
	}
}

func (r *drpcReporter) PostMsgReceive(recvDuration time.Duration) {
	switch r.kind {
	case drpcMetricsClient:
		r.clientMetrics.ClientMsgReceived.Inc(r.baseLabels, 1)
		r.clientMetrics.ClientMsgRecvDuration.Observe(r.baseLabels, float64(recvDuration.Nanoseconds()))
	case drpcMetricsServer:
		r.serverMetrics.ServerMsgReceived.Inc(r.baseLabels, 1)
	}
}

type Reportable struct {
	ClientMetrics *DRPCClientRequestMetrics
	ServerMetrics *DRPCServerRequestMetrics
}

// reporter builds a DRPC metrics reporter with pre-computed label maps.
func (r *Reportable) reporter(
	ctx context.Context, meta drpcCallMeta, kind drpcMetricsKind,
) (drpcMetricsReporter, context.Context) {
	baseLabels := map[string]string{
		RPCTypeLabel:    meta.drpcType,
		RPCServiceLabel: meta.drpcService,
		RPCMethodLabel:  meta.drpcMethod,
	}
	codeLabels := map[string]string{
		RPCTypeLabel:       meta.drpcType,
		RPCServiceLabel:    meta.drpcService,
		RPCMethodLabel:     meta.drpcMethod,
		RPCStatusCodeLabel: "", // set per-call in PostCall
	}
	reporter := &drpcReporter{
		clientMetrics: r.ClientMetrics,
		serverMetrics: r.ServerMetrics,
		kind:          kind,
		baseLabels:    baseLabels,
		codeLabels:    codeLabels,
	}
	return reporter, ctx
}

// serverReporter builds a DRPC reporter and records the start counter.
func (r *Reportable) serverReporter(
	ctx context.Context, meta drpcCallMeta,
) (drpcMetricsReporter, context.Context) {
	reporter, ctx := r.reporter(ctx, meta, drpcMetricsServer)
	r.ServerMetrics.ServerStarted.Inc(reporter.(*drpcReporter).baseLabels, 1)
	return reporter, ctx
}

type monitoredDRPCServerStream struct {
	drpc.Stream

	ctx      context.Context
	reporter drpcMetricsReporter
}

func (s *monitoredDRPCServerStream) Context() context.Context {
	return s.ctx
}

func (s *monitoredDRPCServerStream) MsgSend(msg drpc.Message, enc drpc.Encoding) error {
	start := timeutil.Now()
	err := s.Stream.MsgSend(msg, enc)
	s.reporter.PostMsgSend(timeutil.Since(start))
	return err
}

func (s *monitoredDRPCServerStream) MsgRecv(msg drpc.Message, enc drpc.Encoding) error {
	start := timeutil.Now()
	err := s.Stream.MsgRecv(msg, enc)
	s.reporter.PostMsgReceive(timeutil.Since(start))
	return err
}

func newDRPCReport(callMeta drpcCallMeta) drpcReport {
	return drpcReport{
		startTime: timeutil.Now(),
		callMeta:  callMeta,
	}
}

// RequestMetrics contains metrics for RPC requests.
type RequestMetrics struct {
	Duration *metric.HistogramVec
}

func NewRequestMetrics() *RequestMetrics {
	return &RequestMetrics{
		Duration: metric.NewExportedHistogramVec(
			metaRequestHandledDuration,
			metric.ResponseTime30sBuckets,
			[]string{RPCMethodLabel, RPCStatusCodeLabel}),
	}
}

func NewDRPCServerRequestMetrics() *DRPCServerRequestMetrics {
	return &DRPCServerRequestMetrics{
		ServerStarted: metric.NewExportedCounterVec(
			metaServerRequestStarted,
			[]string{RPCTypeLabel, RPCServiceLabel, RPCMethodLabel}),
		ServerHandled: metric.NewExportedCounterVec(
			metaServerRequestHandled,
			[]string{RPCTypeLabel, RPCServiceLabel, RPCMethodLabel}),
		ServerErrors: metric.NewExportedCounterVec(
			metaServerErrors,
			[]string{RPCTypeLabel, RPCServiceLabel, RPCMethodLabel, RPCStatusCodeLabel}),
		ServerMsgSent: metric.NewExportedCounterVec(
			metaServerMsgSent,
			[]string{RPCTypeLabel, RPCServiceLabel, RPCMethodLabel}),
		ServerMsgReceived: metric.NewExportedCounterVec(
			metaServerMsgReceived,
			[]string{RPCTypeLabel, RPCServiceLabel, RPCMethodLabel}),
		ServerHandledDuration: metric.NewExportedHistogramVec(
			metaServerRequestHandledDuration,
			metric.ResponseTime30sBuckets,
			[]string{RPCTypeLabel, RPCServiceLabel, RPCMethodLabel, RPCStatusCodeLabel}),
	}
}

func NewDRPCClientRequestMetrics() *DRPCClientRequestMetrics {
	return &DRPCClientRequestMetrics{
		ClientStarted: metric.NewExportedCounterVec(
			metaClientRequestStarted,
			[]string{RPCTypeLabel, RPCServiceLabel, RPCMethodLabel}),
		ClientHandled: metric.NewExportedCounterVec(
			metaClientRequestHandled,
			[]string{RPCTypeLabel, RPCServiceLabel, RPCMethodLabel}),
		ClientErrors: metric.NewExportedCounterVec(
			metaClientErrors,
			[]string{RPCTypeLabel, RPCServiceLabel, RPCMethodLabel, RPCStatusCodeLabel}),
		ClientMsgSent: metric.NewExportedCounterVec(
			metaClientMsgSent,
			[]string{RPCTypeLabel, RPCServiceLabel, RPCMethodLabel}),
		ClientMsgReceived: metric.NewExportedCounterVec(
			metaClientMsgReceived,
			[]string{RPCTypeLabel, RPCServiceLabel, RPCMethodLabel}),
		ClientHandledDuration: metric.NewExportedHistogramVec(
			metaClientRequestHandledDuration,
			metric.ResponseTime30sBuckets,
			[]string{RPCTypeLabel, RPCServiceLabel, RPCMethodLabel, RPCStatusCodeLabel}),
		ClientMsgSendDuration: metric.NewExportedHistogramVec(
			metaClientMsgSendDuration,
			metric.ResponseTime30sBuckets,
			[]string{RPCTypeLabel, RPCServiceLabel, RPCMethodLabel}),
		ClientMsgRecvDuration: metric.NewExportedHistogramVec(
			metaClientMsgReceivedDuration,
			metric.ResponseTime30sBuckets,
			[]string{RPCTypeLabel, RPCServiceLabel, RPCMethodLabel}),
	}
}

type RequestMetricsInterceptor grpc.UnaryServerInterceptor

// NewRequestMetricsInterceptor creates a new gRPC server interceptor that records
// the duration of each RPC. The metric is labeled by the method name and the
// status code of the RPC. The interceptor will only record durations if
// shouldRecord returns true. Otherwise, this interceptor will be a no-op.
func NewRequestMetricsInterceptor(
	requestMetrics *RequestMetrics, shouldRecord func(fullMethodName string) bool,
) RequestMetricsInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		if !shouldRecord(info.FullMethod) {
			return handler(ctx, req)
		}

		startTime := timeutil.Now()
		resp, err := handler(ctx, req)
		duration := timeutil.Since(startTime)
		var code codes.Code
		if err != nil {
			code = status.Code(err)
		} else {
			code = codes.OK
		}

		requestMetrics.Duration.Observe(map[string]string{
			RPCMethodLabel:     info.FullMethod,
			RPCStatusCodeLabel: code.String(),
		}, float64(duration.Nanoseconds()))
		return resp, err
	}
}

type DRPCUnaryServerRequestMetricsInterceptor drpcmux.UnaryServerInterceptor
type DRPCStreamServerRequestMetricsInterceptor drpcmux.StreamServerInterceptor

// NewDRPCUnaryServerRequestMetricsInterceptor creates a new DRPC server interceptor
// that records server request metrics for each unary RPC.
// The metric is labeled by the method name and the status code of the RPC.
// The interceptor will only record durations if shouldRecord returns true.
// Otherwise, this interceptor will be a no-op.
func NewDRPCUnaryServerRequestMetricsInterceptor(
	reportable *Reportable, shouldRecord func(rpc string) bool,
) DRPCUnaryServerRequestMetricsInterceptor {
	return func(
		ctx context.Context,
		req any,
		rpc string,
		handler drpcmux.UnaryHandler,
	) (any, error) {
		if !shouldRecord(rpc) {
			return handler(ctx, req)
		}

		r := newDRPCReport(newDRPCCallMeta(rpc, false))

		reporter, newCtx := reportable.serverReporter(ctx, r.callMeta)
		reporter.PostMsgReceive(timeutil.Since(r.startTime))
		resp, err := handler(newCtx, req)
		reporter.PostMsgSend(timeutil.Since(r.startTime))
		reporter.PostCall(err, timeutil.Since(r.startTime))
		return resp, err
	}
}

// NewDRPCStreamServerRequestMetricsInterceptor creates a new DRPC server
// interceptor that records server request metrics for each streaming RPC.
// The metric is labeled by the method name and the status code of the RPC.
// The interceptor will only record durations if shouldRecord returns true.
// Otherwise, this interceptor will be a no-op.
func NewDRPCStreamServerRequestMetricsInterceptor(
	reportable *Reportable, shouldRecord func(rpc string) bool,
) DRPCStreamServerRequestMetricsInterceptor {
	return func(
		stream drpc.Stream,
		rpc string,
		handler drpcmux.StreamHandler,
	) (interface{}, error) {
		if !shouldRecord(rpc) {
			return handler(stream)
		}
		r := newDRPCReport(newDRPCCallMeta(rpc, true))
		reporter, newCtx := reportable.serverReporter(stream.Context(), r.callMeta)
		resp, err := handler(&monitoredDRPCServerStream{Stream: stream,
			ctx: newCtx, reporter: reporter})
		reporter.PostCall(err, timeutil.Since(r.startTime))
		return resp, err
	}
}

// MarkGatewayRequest returns a grpc metadata object that contains the
// gwRequestKey field. This is used by the gRPC gateway that forwards HTTP
// requests to their respective gRPC handlers. See gatewayRequestRecoveryInterceptor below.
func MarkGatewayRequest(ctx context.Context, r *http.Request) metadata.MD {
	return metadata.Pairs(gwRequestKey, "true")
}

// gatewayRequestRecoveryInterceptor recovers from panics in gRPC handlers that
// are invoked due to DB console requests. For these requests, we do not want
// an uncaught panic to crash the node.
func gatewayRequestRecoveryInterceptor(
	ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
) (resp interface{}, err error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		val := md.Get(gwRequestKey)
		if len(val) > 0 {
			defer func() {
				if p := recover(); p != nil {
					logcrash.ReportPanic(ctx, nil, p, 1 /* depth */)
					// The gRPC gateway will put this message in the HTTP response to the client.
					err = errors.New("an unexpected error occurred")
				}
			}()
		}
	}
	resp, err = handler(ctx, req)
	return resp, err
}

// MarkDRPCGatewayRequest annotates ctx so that downstream DRPC calls can
// be recognized as originating from the DB Console HTTP gateway.
func MarkDRPCGatewayRequest(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, gwRequestKey, "true")
}

// drpcGatewayRequestRecoveryInterceptor recovers from panics in DRPC handlers
// that are invoked due to DB console requests. For these requests, we do not
// want an uncaught panic to crash the node.
func drpcGatewayRequestRecoveryInterceptor(
	ctx context.Context, req interface{}, rpc string, handler drpcmux.UnaryHandler,
) (resp interface{}, err error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		val := md.Get(gwRequestKey)
		if len(val) == 1 && val[0] != "" {
			defer func() {
				if p := recover(); p != nil {
					logcrash.ReportPanic(ctx, nil, p, 1 /* depth */)
					// The gRPC gateway will put this message in the HTTP response to the client.
					err = errors.New("an unexpected error occurred")
				}
			}()
		}
	}
	resp, err = handler(ctx, req)
	return resp, err
}

// drpcGatewayRequestCounterInterceptor is a client-side interceptor that
// increments telemetry counters for DRPC requests originating from the HTTP
// gateway. It checks for the gateway request marker and increments
// a counter named after the RPC method.
func drpcGatewayRequestCounterInterceptor(
	ctx context.Context,
	rpc string,
	enc drpc.Encoding,
	in, out drpc.Message,
	cc *drpcclient.ClientConn,
	invoker drpcclient.UnaryInvoker,
) error {
	md, ok := metadata.FromOutgoingContext(ctx)
	if ok {
		val := md.Get(gwRequestKey)
		if len(val) == 1 && val[0] != "" {
			telemetry.Inc(getDRPCGatewayEndpointCounter(rpc))
		}
	}
	return invoker(ctx, rpc, enc, in, out, cc)
}

// getDRPCGatewayEndpointCounter returns a telemetry Counter corresponding to
// the given DRPC method.
func getDRPCGatewayEndpointCounter(method string) telemetry.Counter {
	const counterPrefix = "http.drpc-gateway"
	return telemetry.GetCounter(fmt.Sprintf("%s.%s", counterPrefix, method))
}
