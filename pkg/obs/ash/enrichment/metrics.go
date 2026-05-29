// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package enrichment

import "github.com/cockroachdb/cockroach/pkg/util/metric"

// Metrics groups counters that describe the activity of the ASH
// enrichment cache. The cache is fed by every SQL execution and read by
// the per-tick enricher and by inbound enrichment RPCs, so the counters
// are useful for understanding both write pressure and the working-set
// hit rate of the cache.
type Metrics struct {
	CachePutCount     *metric.Counter
	CacheGetCount     *metric.Counter
	CacheGetMissCount *metric.Counter
}

var _ metric.Struct = Metrics{}

// MetricStruct implements the metric.Struct interface.
func (Metrics) MetricStruct() {}

// NewMetrics returns a new Metrics with all counters initialized.
func NewMetrics() Metrics {
	return Metrics{
		CachePutCount: metric.NewCounter(metric.Metadata{
			Name:        "obs.ash.enrichment.cache.put.count",
			Help:        "Number of writes to the ASH enrichment cache",
			Measurement: "Cache writes",
			Unit:        metric.Unit_COUNT,
		}),
		CacheGetCount: metric.NewCounter(metric.Metadata{
			Name:        "obs.ash.enrichment.cache.get.count",
			Help:        "Number of reads from the ASH enrichment cache",
			Measurement: "Cache reads",
			Unit:        metric.Unit_COUNT,
		}),
		CacheGetMissCount: metric.NewCounter(metric.Metadata{
			Name:        "obs.ash.enrichment.cache.get_miss.errors",
			Help:        "Number of ASH enrichment cache reads that missed",
			Measurement: "Cache misses",
			Unit:        metric.Unit_COUNT,
		}),
	}
}
