// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package insights

import (
	"container/list"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/util/quantile"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type detector interface {
	enabled() bool
	isSlow(*Statement) bool
}

var _ detector = &compositeDetector{}
var _ detector = &AnomalyDetector{}
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

var desiredQuantiles = map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001}

type AnomalyDetector struct {
	settings *cluster.Settings
	metrics  Metrics
	store    *list.List
	mu       struct {
		syncutil.RWMutex

		index map[appstatspb.StmtFingerprintID]*list.Element
	}
}

type latencySummaryEntry struct {
	key   appstatspb.StmtFingerprintID
	value *quantile.Stream
}

func (d *AnomalyDetector) enabled() bool {
	return AnomalyDetectionEnabled.Get(&d.settings.SV)
}

func (d *AnomalyDetector) isSlow(stmt *Statement) (decision bool) {
	if !d.enabled() {
		return
	}

	d.withFingerprintLatencySummary(stmt, func(latencySummary *quantile.Stream) {
		latencySummary.Insert(stmt.LatencyInSeconds)
		p50 := latencySummary.Query(0.5, true)
		p99 := latencySummary.Query(0.99, true)
		decision = stmt.LatencyInSeconds >= p99 &&
			stmt.LatencyInSeconds >= 2*p50 &&
			stmt.LatencyInSeconds >= AnomalyDetectionLatencyThreshold.Get(&d.settings.SV).Seconds()
	})

	return
}

func (d *AnomalyDetector) GetPercentileValues(id appstatspb.StmtFingerprintID) PercentileValues {
	// Ensure that Query doesn't flush which allows us to take the read lock.
	const shouldFlush = false
	d.mu.RLock()
	defer d.mu.RUnlock()
	latencies := PercentileValues{}
	if entry, ok := d.mu.index[id]; ok {
		latencySummary := entry.Value.(latencySummaryEntry).value
		// If more percentiles are added, update the value of `desiredQuantiles` above
		// to include the new keys.
		latencies.P50 = latencySummary.Query(0.5, shouldFlush)
		latencies.P90 = latencySummary.Query(0.9, shouldFlush)
		latencies.P99 = latencySummary.Query(0.99, shouldFlush)
	}
	return latencies
}

func (d *AnomalyDetector) withFingerprintLatencySummary(
	stmt *Statement, consumer func(latencySummary *quantile.Stream),
) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var latencySummary *quantile.Stream

	if element, ok := d.mu.index[stmt.FingerprintID]; ok {
		// We are already tracking latencies for this fingerprint.
		latencySummary = element.Value.(latencySummaryEntry).value
		d.store.MoveToFront(element) // Mark this latency summary as recently used.
	} else if stmt.LatencyInSeconds >= AnomalyDetectionLatencyThreshold.Get(&d.settings.SV).Seconds() {
		// We want to start tracking latencies for this fingerprint.
		latencySummary = quantile.NewTargeted(desiredQuantiles)
		entry := latencySummaryEntry{key: stmt.FingerprintID, value: latencySummary}
		d.mu.index[stmt.FingerprintID] = d.store.PushFront(entry)
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
		delete(d.mu.index, entry.key)
		d.metrics.Evictions.Inc(1)
		d.metrics.Fingerprints.Dec(1)
		d.metrics.Memory.Dec(entry.value.ByteSize())
	}
}

func newAnomalyDetector(settings *cluster.Settings, metrics Metrics) *AnomalyDetector {
	anomaly := &AnomalyDetector{
		settings: settings,
		metrics:  metrics,
		store:    list.New(),
	}
	anomaly.mu.index = make(map[appstatspb.StmtFingerprintID]*list.Element)

	return anomaly
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

func isFailed(s *Statement) bool {
	return s.Status == Statement_Failed
}

var prefixesToIgnore = []string{"SET ", "EXPLAIN "}

// shouldIgnoreStatement returns true if we don't want to analyze the statement.
func shouldIgnoreStatement(s *Statement) bool {
	for _, start := range prefixesToIgnore {
		if strings.HasPrefix(s.Query, start) {
			return true
		}
	}
	return false
}
