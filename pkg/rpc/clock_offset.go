// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"math"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/montanaflynn/stats"
)

// RemoteClockMetrics is the collection of metrics for the clock monitor.
type RemoteClockMetrics struct {
	ClockOffsetMeanNanos         *metric.Gauge
	ClockOffsetStdDevNanos       *metric.Gauge
	ClockOffsetMedianNanos       *metric.Gauge
	ClockOffsetMedianAbsDevNanos *metric.Gauge
	RoundTripLatency             metric.IHistogram
}

// avgLatencyMeasurementAge determines how to exponentially weight the
// moving average of latency measurements. This means that the weight
// will center around the 20th oldest measurement, such that for measurements
// that are made every 3 seconds, the average measurement will be about one
// minute old.
const avgLatencyMeasurementAge = 20.0

var (
	metaClockOffsetMeanNanos = metric.Metadata{
		Name:        "clock-offset.meannanos",
		Help:        "Mean clock offset with other nodes",
		Measurement: "Clock Offset",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaClockOffsetStdDevNanos = metric.Metadata{
		Name:        "clock-offset.stddevnanos",
		Help:        "Stddev clock offset with other nodes",
		Measurement: "Clock Offset",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaClockOffsetMedianNanos = metric.Metadata{
		// An outlier resistant measure of centrality, useful for
		// diagnosing unhealthy nodes.
		// Demo: https://docs.google.com/spreadsheets/d/1gmzQxEVYDKb_b-Mn50ZTje-LqZw6TZwUxxUPY2rG69M/edit?gid=0#gid=0
		Name:        "clock-offset.mediannanos",
		Help:        "Median clock offset with other nodes",
		Measurement: "Clock Offset",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaClockOffsetMedianAbsDevNanos = metric.Metadata{
		// An outlier resistant measure of dispersion, see
		// https://en.wikipedia.org/wiki/Median_absolute_deviation
		// and demo above.
		Name:        "clock-offset.medianabsdevnanos",
		Help:        "Median Absolute Deviation (MAD) with other nodes",
		Measurement: "Clock Offset",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaConnectionRoundTripLatency = metric.Metadata{
		// NB: the name is legacy and should not be changed since customers
		// rely on it.
		Name: "round-trip-latency",
		Help: `Distribution of round-trip latencies with other nodes.

This only reflects successful heartbeats and measures gRPC overhead as well as
possible head-of-line blocking. Elevated values in this metric may hint at
network issues and/or saturation, but they are no proof of them. CPU overload
can similarly elevate this metric. The operator should look towards OS-level
metrics such as packet loss, retransmits, etc, to conclusively diagnose network
issues. Heartbeats are not very frequent (~seconds), so they may not capture
rare or short-lived degradations.
`,
		Measurement: "Round-trip time",
		Unit:        metric.Unit_NANOSECONDS,
	}
)

// A stateful trigger that fires once when exceeding a threshold, then must
// fall below another lower threshold before firing again.
type resettingMaxTrigger bool

func (t *resettingMaxTrigger) triggers(value, resetThreshold, triggerThreshold float64) bool {
	if *t {
		// This is the "recently triggered" state.
		// Never trigger. Transition to "normal" state if below resetThreshold.
		if value < resetThreshold {
			*t = false
		}
	} else {
		// This is the "normal" state.
		// Trigger and transition to "recently triggered" if above triggerThreshold.
		if value > triggerThreshold {
			*t = true
			return true
		}
	}
	return false
}

type latencyInfo struct {
	avgNanos ewma.MovingAverage
	trigger  resettingMaxTrigger
}

// RemoteClockMonitor keeps track of the most recent measurements of remote
// offsets and round-trip latency from this node to connected nodes.
type RemoteClockMonitor struct {
	clock           hlc.WallClock
	toleratedOffset time.Duration
	offsetTTL       time.Duration

	mu struct {
		syncutil.RWMutex
		offsets      map[roachpb.NodeID]RemoteOffset
		latencyInfos map[roachpb.NodeID]*latencyInfo
		connCount    map[roachpb.NodeID]uint
	}

	metrics RemoteClockMetrics
}

// TestingResetLatencyInfos will clear all latency info from the clock monitor.
// It is intended to be used in tests when enabling or disabling injected
// latency.
func (r *RemoteClockMonitor) TestingResetLatencyInfos() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for a := range r.mu.latencyInfos {
		delete(r.mu.latencyInfos, a)
	}
}

// newRemoteClockMonitor returns a monitor with the given server clock. A
// toleratedOffset of 0 disables offset checking and metrics, but still records
// latency metrics.
func newRemoteClockMonitor(
	clock hlc.WallClock,
	toleratedOffset time.Duration,
	offsetTTL time.Duration,
	histogramWindowInterval time.Duration,
) *RemoteClockMonitor {
	r := RemoteClockMonitor{
		clock:           clock,
		toleratedOffset: toleratedOffset,
		offsetTTL:       offsetTTL,
	}
	r.mu.offsets = make(map[roachpb.NodeID]RemoteOffset)
	r.mu.latencyInfos = make(map[roachpb.NodeID]*latencyInfo)
	r.mu.connCount = make(map[roachpb.NodeID]uint)
	if histogramWindowInterval == 0 {
		histogramWindowInterval = time.Duration(math.MaxInt64)
	}
	r.metrics = RemoteClockMetrics{
		ClockOffsetMeanNanos:         metric.NewGauge(metaClockOffsetMeanNanos),
		ClockOffsetStdDevNanos:       metric.NewGauge(metaClockOffsetStdDevNanos),
		ClockOffsetMedianNanos:       metric.NewGauge(metaClockOffsetMedianNanos),
		ClockOffsetMedianAbsDevNanos: metric.NewGauge(metaClockOffsetMedianAbsDevNanos),
		RoundTripLatency: metric.NewHistogram(metric.HistogramOptions{
			Mode:     metric.HistogramModePreferHdrLatency,
			Metadata: metaConnectionRoundTripLatency,
			Duration: histogramWindowInterval,
			// NB: the choice of IO over Network buckets is somewhat debatable, but
			// it's fine. Heartbeats can take >1s which the IO buckets can represent,
			// but the Network buckets top out at 1s.
			BucketConfig: metric.IOLatencyBuckets,
		}),
	}
	return &r
}

// Metrics returns the metrics struct. Useful to examine individual metrics,
// or to add to the registry.
func (r *RemoteClockMonitor) Metrics() *RemoteClockMetrics {
	return &r.metrics
}

// Latency returns the exponentially weighted moving average latency to the
// given node id. Returns true if the measurement is valid, or false if
// we don't have enough samples to compute a reliable average.
func (r *RemoteClockMonitor) Latency(id roachpb.NodeID) (time.Duration, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if info, ok := r.mu.latencyInfos[id]; ok && info.avgNanos.Value() != 0.0 {
		return time.Duration(int64(info.avgNanos.Value())), true
	}
	return 0, false
}

// AllLatencies returns a map of all currently valid latency measurements.
func (r *RemoteClockMonitor) AllLatencies() map[roachpb.NodeID]time.Duration {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make(map[roachpb.NodeID]time.Duration)
	for id, info := range r.mu.latencyInfos {
		if info.avgNanos.Value() != 0.0 {
			result[id] = time.Duration(int64(info.avgNanos.Value()))
		}
	}
	return result
}

// OnConnect tracks connections count per node.
func (r *RemoteClockMonitor) OnConnect(_ context.Context, nodeID roachpb.NodeID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := r.mu.connCount[nodeID]
	count++
	r.mu.connCount[nodeID] = count
}

// OnDisconnect removes all information associated with the provided node when there's no connections remain.
func (r *RemoteClockMonitor) OnDisconnect(_ context.Context, nodeID roachpb.NodeID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	count, ok := r.mu.connCount[nodeID]
	if ok && count > 0 {
		count--
		r.mu.connCount[nodeID] = count
	}
	if count == 0 {
		delete(r.mu.offsets, nodeID)
		delete(r.mu.latencyInfos, nodeID)
		delete(r.mu.connCount, nodeID)
	}
}

// UpdateOffset is a thread-safe way to update the remote clock and latency
// measurements.
//
// It only updates the offset for node if one of the following cases holds:
// 1. There is no prior offset for that node.
// 2. The old offset for node was measured long enough ago to be considered
// stale.
// 3. The new offset's error is smaller than the old offset's error.
//
// Pass a roundTripLatency of 0 or less to avoid recording the latency.
func (r *RemoteClockMonitor) UpdateOffset(
	ctx context.Context, id roachpb.NodeID, offset RemoteOffset, roundTripLatency time.Duration,
) {
	emptyOffset := offset == RemoteOffset{}
	// At startup the remote node's id may not be set. Skip recording latency.
	if id == 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if oldOffset, ok := r.mu.offsets[id]; !ok {
		// We don't have a measurement - if the incoming measurement is not empty,
		// set it.
		if !emptyOffset {
			r.mu.offsets[id] = offset
		}
	} else if oldOffset.isStale(r.offsetTTL, r.clock.Now()) {
		// We have a measurement but it's old - if the incoming measurement is not empty,
		// set it, otherwise delete the old measurement.
		if !emptyOffset {
			r.mu.offsets[id] = offset
		} else {
			// Remove most recent offset because it is outdated and new received offset is empty
			// so there's no reason to either keep previous value or update with new one.
			delete(r.mu.offsets, id)
		}
	} else if offset.Uncertainty < oldOffset.Uncertainty {
		// We have a measurement but its uncertainty is greater than that of the
		// incoming measurement - if the incoming measurement is not empty, set it.
		if !emptyOffset {
			r.mu.offsets[id] = offset
		}
	}

	if roundTripLatency > 0 {
		info, ok := r.mu.latencyInfos[id]
		if !ok {
			info = &latencyInfo{
				avgNanos: ewma.NewMovingAverage(avgLatencyMeasurementAge),
			}
			r.mu.latencyInfos[id] = info
		}

		newLatencyf := float64(roundTripLatency.Nanoseconds())
		prevAvg := info.avgNanos.Value()
		info.avgNanos.Add(newLatencyf)
		r.metrics.RoundTripLatency.RecordValue(roundTripLatency.Nanoseconds())

		// See: https://github.com/cockroachdb/cockroach/issues/96262
		// See: https://github.com/cockroachdb/cockroach/issues/98066
		const thresh = 50 * 1e6 // 50ms
		// If the roundtrip jumps by 50% beyond the previously recorded average, report it in logs.
		// Don't report it again until it falls below 40% above the average.
		// (Also requires latency > thresh to avoid trigger on noise on low-latency connections and
		// the running average to be non-zero to avoid triggering on startup.)
		if newLatencyf > thresh && prevAvg > 0.0 &&
			info.trigger.triggers(newLatencyf, prevAvg*1.4, prevAvg*1.5) {
			log.Health.Warningf(ctx, "latency jump (prev avg %.2fms, current %.2fms)",
				prevAvg/1e6, newLatencyf/1e6)
		}
	}

	if log.V(2) {
		log.Dev.Infof(ctx, "update offset: n%d %v", id, r.mu.offsets[id])
	}
}

// GetOffset returns the last RemoteOffset seen for this node. Returns the
// zero value if no offset is known.
func (r *RemoteClockMonitor) GetOffset(id roachpb.NodeID) RemoteOffset {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.offsets[id]
}

// VerifyClockOffset calculates the number of nodes to which the known offset
// is healthy (as defined by RemoteOffset.isHealthy). It returns nil iff more
// than half the known offsets are healthy, and an error otherwise. A non-nil
// return indicates that this node's clock is unreliable, and that the node
// should terminate.
func (r *RemoteClockMonitor) VerifyClockOffset(ctx context.Context) error {
	// By the contract of the hlc, if the value is 0, then safety checking of the
	// tolerated offset is disabled. However we may still want to propagate the
	// information to a status node.
	if r.toleratedOffset == 0 {
		return nil
	}

	now := r.clock.Now()
	healthyOffsetCount := 0

	offsets, numClocks := func() (stats.Float64Data, int) {
		r.mu.Lock()
		defer r.mu.Unlock()
		// Each measurement is recorded as its minimum and maximum value.
		offs := make(stats.Float64Data, 0, 2*len(r.mu.offsets))
		for id, offset := range r.mu.offsets {
			if offset.isStale(r.offsetTTL, now) {
				delete(r.mu.offsets, id)
				continue
			}
			offs = append(offs, float64(offset.Offset+offset.Uncertainty))
			offs = append(offs, float64(offset.Offset-offset.Uncertainty))
			if offset.isHealthy(ctx, r.toleratedOffset) {
				healthyOffsetCount++
			}
		}
		return offs, len(r.mu.offsets)
	}()

	mean, err := offsets.Mean()
	if err != nil && !errors.Is(err, stats.EmptyInput) {
		return err
	}
	stdDev, err := offsets.StandardDeviation()
	if err != nil && !errors.Is(err, stats.EmptyInput) {
		return err
	}
	median, err := offsets.Median()
	if err != nil && !errors.Is(err, stats.EmptyInput) {
		return err
	}
	medianAbsoluteDeviation, err := offsets.MedianAbsoluteDeviation()
	if err != nil && !errors.Is(err, stats.EmptyInput) {
		return err
	}
	r.metrics.ClockOffsetMeanNanos.Update(int64(mean))
	r.metrics.ClockOffsetStdDevNanos.Update(int64(stdDev))
	r.metrics.ClockOffsetMedianNanos.Update(int64(median))
	r.metrics.ClockOffsetMedianAbsDevNanos.Update(int64(medianAbsoluteDeviation))

	if numClocks > 0 && healthyOffsetCount <= numClocks/2 {
		return errors.Errorf(
			"clock synchronization error: this node is more than %s away from at least half of the known nodes (%d of %d are within the offset)",
			r.toleratedOffset, healthyOffsetCount, numClocks)
	}
	if log.V(1) {
		log.Dev.Infof(ctx, "%d of %d nodes are within the tolerated clock offset of %s", healthyOffsetCount, numClocks, r.toleratedOffset)
	}

	return nil
}

func (r RemoteOffset) isHealthy(ctx context.Context, toleratedOffset time.Duration) bool {
	// Offset may be negative, but Uncertainty is always positive.
	absOffset := r.Offset
	if absOffset < 0 {
		absOffset = -absOffset
	}
	switch {
	case time.Duration(absOffset-r.Uncertainty)*time.Nanosecond > toleratedOffset:
		// The minimum possible true offset exceeds the tolerated offset; definitely
		// unhealthy.
		return false

	case time.Duration(absOffset+r.Uncertainty)*time.Nanosecond < toleratedOffset:
		// The maximum possible true offset does not exceed the tolerated offset;
		// definitely healthy.
		return true

	default:
		// The tolerated offset is in the uncertainty window of the measured offset;
		// health is ambiguous. For now, we err on the side of not spuriously
		// killing nodes.
		log.Health.Warningf(ctx, "uncertain remote offset %s for maximum tolerated offset %s, treating as healthy", r, toleratedOffset)
		return true
	}
}

func (r RemoteOffset) isStale(ttl time.Duration, now time.Time) bool {
	if ttl == 0 {
		return false // ttl disabled
	}
	return r.measuredAt().Add(ttl).Before(now)
}

func updateClockOffsetTracking(
	ctx context.Context,
	remoteClocks *RemoteClockMonitor,
	nodeID roachpb.NodeID,
	sendTime, serverTime, receiveTime time.Time,
	toleratedOffset time.Duration,
) (time.Duration, RemoteOffset, error) {
	pingDuration := receiveTime.Sub(sendTime)
	if remoteClocks == nil {
		// Only a server connecting to another server needs to check clock
		// offsets. A CLI command does not need to update its local HLC, nor does
		// it care that strictly about client-server latency, nor does it need to
		// track the offsets.

		return pingDuration, RemoteOffset{}, nil
	}

	var offset RemoteOffset
	if pingDuration <= maximumPingDurationMult*toleratedOffset {
		// Offset and error are measured using the remote clock reading
		// technique described in
		// http://se.inf.tu-dresden.de/pubs/papers/SRDS1994.pdf, page 6.
		// However, we assume that drift and min message delay are 0, for
		// now.
		offset.MeasuredAt = receiveTime.UnixNano()
		offset.Uncertainty = (pingDuration / 2).Nanoseconds()
		remoteTimeNow := serverTime.Add(pingDuration / 2)
		offset.Offset = remoteTimeNow.Sub(receiveTime).Nanoseconds()
	}
	remoteClocks.UpdateOffset(ctx, nodeID, offset, pingDuration)
	return pingDuration, offset, remoteClocks.VerifyClockOffset(ctx)
}
