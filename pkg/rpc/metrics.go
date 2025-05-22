// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"net/http"
	"strings"

	"github.com/VividCortex/ewma"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
)

// gwRequestKey is a field set on the context to indicate a request
// is coming from gRPC gateway.
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
		Essential:   true,
		Category:    metric.Metadata_NETWORKING,
		HowToUse:    `See Description.`,
	}

	metaConnectionUnhealthy = metric.Metadata{
		Name:        "rpc.connection.unhealthy",
		Help:        "Gauge of current connections in an unhealthy state (not bidirectionally connected or heartbeating)",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
		Essential:   true,
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
		Essential:   true,
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
		Essential:   true,
		Category:    metric.Metadata_NETWORKING,
		HowToUse:    `If this duration is greater than 0, this could indicate how long a network partition has been occurring.`,
	}

	metaConnectionHeartbeats = metric.Metadata{
		Name:        "rpc.connection.heartbeats",
		Help:        `Counter of successful heartbeats.`,
		Measurement: "Heartbeats",
		Unit:        metric.Unit_COUNT,
		Essential:   true,
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
		Essential:   true,
		Category:    metric.Metadata_NETWORKING,
		HowToUse:    `See Description.`,
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
		Essential:   true,
		Category:    metric.Metadata_NETWORKING,
		HowToUse:    `This metric is helpful in understanding general network issues outside of CockroachDB that could be impacting the user’s workload.`,
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
	metaRequestDuration = metric.Metadata{
		Name:        "rpc.server.request.duration.nanos",
		Help:        "Duration of an grpc request in nanoseconds.",
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
	childLabels := []string{"remote_node_id", "remote_addr", "class"}
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

func (m *Metrics) acquire(k peerKey, l roachpb.Locality) (peerMetrics, localityMetrics) {
	m.mu.Lock()
	defer m.mu.Unlock()
	labelVals := []string{k.NodeID.String(), k.TargetAddr, k.Class.String()}
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

const (
	RpcMethodLabel     = "methodName"
	RpcStatusCodeLabel = "statusCode"
)

// RequestMetrics contains metrics for RPC requests.
type RequestMetrics struct {
	Duration *metric.HistogramVec
}

func NewRequestMetrics() *RequestMetrics {
	return &RequestMetrics{
		Duration: metric.NewExportedHistogramVec(
			metaRequestDuration,
			metric.ResponseTime30sBuckets,
			[]string{RpcMethodLabel, RpcStatusCodeLabel}),
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
			RpcMethodLabel:     info.FullMethod,
			RpcStatusCodeLabel: code.String(),
		}, float64(duration.Nanoseconds()))
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
