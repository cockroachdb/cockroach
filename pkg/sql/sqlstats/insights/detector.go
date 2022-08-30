// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package insights

import (
	"container/list"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/quantile"
)

type detector interface {
	enabled() bool
	isSlow(*Statement) bool
}

var _ detector = &compositeDetector{}
var _ detector = &anomalyDetector{}
var _ detector = &latencyThresholdDetector{}

type compositeDetector struct {
	detectors []detector
}

func (d *compositeDetector) enabled() bool {
	for _, d := range d.detectors {
		if d.enabled() {
			return true
		}
	}
	return false
}

func (d *compositeDetector) isSlow(statement *Statement) bool {
	// Because some detectors may need to observe all statements to build up
	// their baseline sense of what "normal" is, we avoid short-circuiting.
	result := false
	for _, d := range d.detectors {
		result = d.isSlow(statement) || result
	}
	return result
}

var desiredQuantiles = map[float64]float64{0.5: 0.05, 0.99: 0.001}

type anomalyDetector struct {
	settings *cluster.Settings
	metrics  Metrics
	store    *list.List
	index    map[roachpb.StmtFingerprintID]*list.Element
}

type latencySummaryEntry struct {
	key   roachpb.StmtFingerprintID
	value *quantile.Stream
}

func (d *anomalyDetector) enabled() bool {
	return AnomalyDetectionEnabled.Get(&d.settings.SV)
}

func (d *anomalyDetector) isSlow(stmt *Statement) (decision bool) {
	if !d.enabled() {
		return
	}

	d.withFingerprintLatencySummary(stmt, func(latencySummary *quantile.Stream) {
		latencySummary.Insert(stmt.LatencyInSeconds)
		p50 := latencySummary.Query(0.5)
		p99 := latencySummary.Query(0.99)
		decision = stmt.LatencyInSeconds >= p99 &&
			stmt.LatencyInSeconds >= 2*p50 &&
			stmt.LatencyInSeconds >= AnomalyDetectionLatencyThreshold.Get(&d.settings.SV).Seconds()
	})

	return
}

func (d *anomalyDetector) withFingerprintLatencySummary(
	stmt *Statement, consumer func(latencySummary *quantile.Stream),
) {
	var latencySummary *quantile.Stream

	if element, ok := d.index[stmt.FingerprintID]; ok {
		// We are already tracking latencies for this fingerprint.
		latencySummary = element.Value.(latencySummaryEntry).value
		d.store.MoveToFront(element) // Mark this latency summary as recently used.
	} else if stmt.LatencyInSeconds >= AnomalyDetectionLatencyThreshold.Get(&d.settings.SV).Seconds() {
		// We want to start tracking latencies for this fingerprint.
		latencySummary = quantile.NewTargeted(desiredQuantiles)
		entry := latencySummaryEntry{key: stmt.FingerprintID, value: latencySummary}
		d.index[stmt.FingerprintID] = d.store.PushFront(entry)
		d.metrics.Fingerprints.Inc(1)
		d.metrics.Memory.Inc(latencySummary.ByteSize())
	} else {
		// We don't care about this fingerprint yet.
		return
	}

	previousMemoryUsage := latencySummary.ByteSize()
	consumer(latencySummary)
	d.metrics.Memory.Inc(latencySummary.ByteSize() - previousMemoryUsage)

	// To control our memory usage, possibly evict the latency summary for the least recently seen statement fingerprint.
	if d.metrics.Memory.Value() > AnomalyDetectionMemoryLimit.Get(&d.settings.SV) {
		element := d.store.Back()
		entry := d.store.Remove(element).(latencySummaryEntry)
		delete(d.index, entry.key)
		d.metrics.Evictions.Inc(1)
		d.metrics.Fingerprints.Dec(1)
		d.metrics.Memory.Dec(entry.value.ByteSize())
	}
}

func newAnomalyDetector(settings *cluster.Settings, metrics Metrics) *anomalyDetector {
	return &anomalyDetector{
		settings: settings,
		metrics:  metrics,
		store:    list.New(),
		index:    make(map[roachpb.StmtFingerprintID]*list.Element),
	}
}

type latencyThresholdDetector struct {
	st *cluster.Settings
}

func (d *latencyThresholdDetector) enabled() bool {
	return LatencyThreshold.Get(&d.st.SV) > 0
}

func (d *latencyThresholdDetector) isSlow(s *Statement) bool {
	return d.enabled() && s.LatencyInSeconds >= LatencyThreshold.Get(&d.st.SV).Seconds()
}
