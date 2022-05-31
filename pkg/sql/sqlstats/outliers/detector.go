// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package outliers

import (
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/quantile"
)

type detector interface {
	enabled() bool
	isOutlier(*Outlier_Statement) bool
}

var _ detector = &anyDetector{}
var _ detector = &latencyQuantileDetector{}
var _ detector = &latencyThresholdDetector{}

type anyDetector struct {
	detectors []detector
}

func (a anyDetector) enabled() bool {
	for _, d := range a.detectors {
		if d.enabled() {
			return true
		}
	}
	return false
}

func (a anyDetector) isOutlier(statement *Outlier_Statement) bool {
	// Because some detectors may need to observe all statements to build up
	// their baseline sense of what "normal" is, we avoid short-circuiting.
	result := false
	for _, d := range a.detectors {
		result = d.isOutlier(statement) || result
	}
	return result
}

var desiredQuantiles = map[float64]float64{0.5: 0.05, 0.99: 0.001}

type latencyQuantileDetector struct {
	settings         *cluster.Settings
	metrics          *Metrics
	latencySummaries *cache.UnorderedCache
}

func (d latencyQuantileDetector) enabled() bool {
	return LatencyQuantileDetectionEnabled.Get(&d.settings.SV)
}

func (d *latencyQuantileDetector) isOutlier(s *Outlier_Statement) bool {
	if !d.enabled() {
		return false
	}

	interestingThreshold := LatencyQuantileDetectorInterestingThreshold.Get(&d.settings.SV).Seconds()

	var latencySummary *quantile.Stream
	if value, ok := d.latencySummaries.Get(s.FingerprintID); ok {
		// We are already tracking latencies for this fingerprint.
		latencySummary = value.(*quantile.Stream)
	} else if s.LatencyInSeconds >= interestingThreshold {
		// We want to start tracking latencies for this fingerprint.
		latencySummary = quantile.NewTargeted(desiredQuantiles)
		d.metrics.Fingerprints.Inc(1)
		d.metrics.Memory.Inc(latencySummary.ByteSize())
		d.latencySummaries.Add(s.FingerprintID, latencySummary)
	} else {
		// We don't care about this fingerprint yet.
		return false
	}

	previousMemoryUsage := latencySummary.ByteSize()
	latencySummary.Insert(s.LatencyInSeconds)
	p50 := latencySummary.Query(0.5)
	p99 := latencySummary.Query(0.99)
	d.metrics.Memory.Inc(latencySummary.ByteSize() - previousMemoryUsage)

	return s.LatencyInSeconds >= p99 &&
		s.LatencyInSeconds >= 2*p50 &&
		s.LatencyInSeconds >= interestingThreshold
}

func newLatencyQuantileDetector(settings *cluster.Settings, metrics *Metrics) detector {
	return &latencyQuantileDetector{
		settings: settings,
		metrics:  metrics,
		latencySummaries: cache.NewUnorderedCache(cache.Config{
			Policy: cache.CacheLRU,
			ShouldEvict: func(_ int, _, _ interface{}) bool {
				return metrics.Memory.Value() > LatencyQuantileDetectorMemoryCap.Get(&settings.SV)
			},
			OnEvicted: func(_, value interface{}) {
				stream := value.(*quantile.Stream)
				metrics.Fingerprints.Dec(1)
				metrics.Memory.Dec(stream.ByteSize())
				metrics.Evictions.Inc(1)
			},
		}),
	}
}

type latencyThresholdDetector struct {
	st *cluster.Settings
}

func (l latencyThresholdDetector) enabled() bool {
	return LatencyThreshold.Get(&l.st.SV) > 0
}

func (l latencyThresholdDetector) isOutlier(s *Outlier_Statement) bool {
	return l.enabled() && s.LatencyInSeconds >= LatencyThreshold.Get(&l.st.SV).Seconds()
}
