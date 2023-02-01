// Copyright 2014 The Cockroach Authors.
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
	ClockOffsetMeanNanos   *metric.Gauge
	ClockOffsetStdDevNanos *metric.Gauge
	LatencyHistogramNanos  metric.IHistogram
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
	metaLatencyHistogramNanos = metric.Metadata{
		Name:        "round-trip-latency",
		Help:        "Distribution of round-trip latencies with other nodes",
		Measurement: "Roundtrip Latency",
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
	clock     hlc.WallClock
	maxOffset time.Duration
	offsetTTL time.Duration

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

// newRemoteClockMonitor returns a monitor with the given server clock.
func newRemoteClockMonitor(
	clock hlc.WallClock,
	maxOffset time.Duration,
	offsetTTL time.Duration,
	histogramWindowInterval time.Duration,
) *RemoteClockMonitor {
	r := RemoteClockMonitor{
		clock:     clock,
		maxOffset: maxOffset,
		offsetTTL: offsetTTL,
	}
	r.mu.offsets = make(map[roachpb.NodeID]RemoteOffset)
	r.mu.latencyInfos = make(map[roachpb.NodeID]*latencyInfo)
	r.mu.connCount = make(map[roachpb.NodeID]uint)
	if histogramWindowInterval == 0 {
		histogramWindowInterval = time.Duration(math.MaxInt64)
	}
	r.metrics = RemoteClockMetrics{
		ClockOffsetMeanNanos:   metric.NewGauge(metaClockOffsetMeanNanos),
		ClockOffsetStdDevNanos: metric.NewGauge(metaClockOffsetStdDevNanos),
		LatencyHistogramNanos: metric.NewHistogram(metric.HistogramOptions{
			Mode:     metric.HistogramModePreferHdrLatency,
			Metadata: metaLatencyHistogramNanos,
			Duration: histogramWindowInterval,
			Buckets:  metric.IOLatencyBuckets,
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
func (r *RemoteClockMonitor) OnConnect(ctx context.Context, nodeID roachpb.NodeID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := r.mu.connCount[nodeID]
	count++
	r.mu.connCount[nodeID] = count
}

// OnDisconnect removes all information associated with the provided node when there's no connections remain.
func (r *RemoteClockMonitor) OnDisconnect(ctx context.Context, nodeID roachpb.NodeID) {
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
		r.metrics.LatencyHistogramNanos.RecordValue(roundTripLatency.Nanoseconds())

		// If the roundtrip jumps by 50% beyond the previously recorded average, report it in logs.
		// Don't report it again until it falls below 40% above the average.
		// (Also requires latency > 1ms to avoid trigger on noise on low-latency connections and
		// the running average to be non-zero to avoid triggering on startup.)
		if newLatencyf > 1e6 && prevAvg > 0.0 &&
			info.trigger.triggers(newLatencyf, prevAvg*1.4, prevAvg*1.5) {
			log.Health.Warningf(ctx, "latency jump (prev avg %.2fms, current %.2fms)",
				prevAvg/1e6, newLatencyf/1e6)
		}
	}

	if log.V(2) {
		log.Dev.Infof(ctx, "update offset: n%d %v", id, r.mu.offsets[id])
	}
}

// VerifyClockOffset calculates the number of nodes to which the known offset
// is healthy (as defined by RemoteOffset.isHealthy). It returns nil iff more
// than half the known offsets are healthy, and an error otherwise. A non-nil
// return indicates that this node's clock is unreliable, and that the node
// should terminate.
func (r *RemoteClockMonitor) VerifyClockOffset(ctx context.Context) error {
	// By the contract of the hlc, if the value is 0, then safety checking of
	// the max offset is disabled. However we may still want to propagate the
	// information to a status node.
	//
	// TODO(tschottdorf): disallow maxOffset == 0 but probably lots of tests to
	// fix.
	if r.maxOffset != 0 {
		now := r.clock.Now()

		healthyOffsetCount := 0

		r.mu.Lock()
		// Each measurement is recorded as its minimum and maximum value.
		offsets := make(stats.Float64Data, 0, 2*len(r.mu.offsets))
		for id, offset := range r.mu.offsets {
			if offset.isStale(r.offsetTTL, now) {
				delete(r.mu.offsets, id)
				continue
			}
			offsets = append(offsets, float64(offset.Offset+offset.Uncertainty))
			offsets = append(offsets, float64(offset.Offset-offset.Uncertainty))
			if offset.isHealthy(ctx, r.maxOffset) {
				healthyOffsetCount++
			}
		}
		numClocks := len(r.mu.offsets)
		r.mu.Unlock()

		mean, err := offsets.Mean()
		if err != nil && !errors.Is(err, stats.EmptyInput) {
			return err
		}
		stdDev, err := offsets.StandardDeviation()
		if err != nil && !errors.Is(err, stats.EmptyInput) {
			return err
		}
		r.metrics.ClockOffsetMeanNanos.Update(int64(mean))
		r.metrics.ClockOffsetStdDevNanos.Update(int64(stdDev))

		if numClocks > 0 && healthyOffsetCount <= numClocks/2 {
			return errors.Errorf(
				"clock synchronization error: this node is more than %s away from at least half of the known nodes (%d of %d are within the offset)",
				r.maxOffset, healthyOffsetCount, numClocks)
		}
		if log.V(1) {
			log.Dev.Infof(ctx, "%d of %d nodes are within the maximum clock offset of %s", healthyOffsetCount, numClocks, r.maxOffset)
		}
	}

	return nil
}

func (r RemoteOffset) isHealthy(ctx context.Context, maxOffset time.Duration) bool {
	// Tolerate up to 80% of the maximum offset.
	toleratedOffset := maxOffset * 4 / 5

	// Offset may be negative, but Uncertainty is always positive.
	absOffset := r.Offset
	if absOffset < 0 {
		absOffset = -absOffset
	}
	switch {
	case time.Duration(absOffset-r.Uncertainty)*time.Nanosecond > toleratedOffset:
		// The minimum possible true offset exceeds the maximum offset; definitely
		// unhealthy.
		return false

	case time.Duration(absOffset+r.Uncertainty)*time.Nanosecond < toleratedOffset:
		// The maximum possible true offset does not exceed the maximum offset;
		// definitely healthy.
		return true

	default:
		// The maximum offset is in the uncertainty window of the measured offset;
		// health is ambiguous. For now, we err on the side of not spuriously
		// killing nodes.
		log.Health.Warningf(ctx, "uncertain remote offset %s for maximum tolerated offset %s, treating as healthy", r, toleratedOffset)
		return true
	}
}

func (r RemoteOffset) isStale(ttl time.Duration, now time.Time) bool {
	return r.measuredAt().Add(ttl).Before(now)
}
