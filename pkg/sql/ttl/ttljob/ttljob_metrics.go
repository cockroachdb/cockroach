// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ttljob

import (
	"regexp"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// RowLevelTTLAggMetrics are the row-level TTL job agg metrics.
type RowLevelTTLAggMetrics struct {
	RangeTotalDuration *aggmetric.AggHistogram
	SelectDuration     *aggmetric.AggHistogram
	DeleteDuration     *aggmetric.AggHistogram
	RowSelections      *aggmetric.AggCounter
	RowDeletions       *aggmetric.AggCounter
	NumActiveRanges    *aggmetric.AggGauge
	TotalRows          *aggmetric.AggGauge
	TotalExpiredRows   *aggmetric.AggGauge

	defaultRowLevelMetrics rowLevelTTLMetrics
	mu                     struct {
		syncutil.Mutex
		m map[string]rowLevelTTLMetrics
	}
}

type rowLevelTTLMetrics struct {
	RangeTotalDuration *aggmetric.Histogram
	SelectDuration     *aggmetric.Histogram
	DeleteDuration     *aggmetric.Histogram
	RowSelections      *aggmetric.Counter
	RowDeletions       *aggmetric.Counter
	NumActiveRanges    *aggmetric.Gauge
	TotalRows          *aggmetric.Gauge
	TotalExpiredRows   *aggmetric.Gauge
}

// MetricStruct implements the metric.Struct interface.
func (m *RowLevelTTLAggMetrics) MetricStruct() {}

func (m *RowLevelTTLAggMetrics) metricsWithChildren(children ...string) rowLevelTTLMetrics {
	return rowLevelTTLMetrics{
		RangeTotalDuration: m.RangeTotalDuration.AddChild(children...),
		SelectDuration:     m.SelectDuration.AddChild(children...),
		DeleteDuration:     m.DeleteDuration.AddChild(children...),
		RowSelections:      m.RowSelections.AddChild(children...),
		RowDeletions:       m.RowDeletions.AddChild(children...),
		NumActiveRanges:    m.NumActiveRanges.AddChild(children...),
		TotalRows:          m.TotalRows.AddChild(children...),
		TotalExpiredRows:   m.TotalExpiredRows.AddChild(children...),
	}
}

var invalidPrometheusRe = regexp.MustCompile(`[^a-zA-Z0-9_]`)

func (m *RowLevelTTLAggMetrics) loadMetrics(labelMetrics bool, relation string) rowLevelTTLMetrics {
	if !labelMetrics {
		return m.defaultRowLevelMetrics
	}
	relation = invalidPrometheusRe.ReplaceAllString(relation, "_")
	m.mu.Lock()
	defer m.mu.Unlock()
	if ret, ok := m.mu.m[relation]; ok {
		return ret
	}
	ret := m.metricsWithChildren(relation)
	m.mu.m[relation] = ret
	return ret
}

func makeRowLevelTTLAggMetrics(histogramWindowInterval time.Duration) metric.Struct {
	sigFigs := 2
	b := aggmetric.MakeBuilder("relation")
	ret := &RowLevelTTLAggMetrics{
		RangeTotalDuration: b.Histogram(
			metric.Metadata{
				Name:        "jobs.row_level_ttl.range_total_duration",
				Help:        "Duration for processing a range during row level TTL.",
				Measurement: "nanoseconds",
				Unit:        metric.Unit_NANOSECONDS,
				MetricType:  io_prometheus_client.MetricType_HISTOGRAM,
			},
			histogramWindowInterval,
			time.Hour.Nanoseconds(),
			sigFigs,
		),
		SelectDuration: b.Histogram(
			metric.Metadata{
				Name:        "jobs.row_level_ttl.select_duration",
				Help:        "Duration for select requests during row level TTL.",
				Measurement: "nanoseconds",
				Unit:        metric.Unit_NANOSECONDS,
				MetricType:  io_prometheus_client.MetricType_HISTOGRAM,
			},
			histogramWindowInterval,
			time.Minute.Nanoseconds(),
			sigFigs,
		),
		DeleteDuration: b.Histogram(
			metric.Metadata{
				Name:        "jobs.row_level_ttl.delete_duration",
				Help:        "Duration for delete requests during row level TTL.",
				Measurement: "nanoseconds",
				Unit:        metric.Unit_NANOSECONDS,
				MetricType:  io_prometheus_client.MetricType_HISTOGRAM,
			},
			histogramWindowInterval,
			time.Minute.Nanoseconds(),
			sigFigs,
		),
		RowSelections: b.Counter(
			metric.Metadata{
				Name:        "jobs.row_level_ttl.rows_selected",
				Help:        "Number of rows selected for deletion by the row level TTL job.",
				Measurement: "num_rows",
				Unit:        metric.Unit_COUNT,
				MetricType:  io_prometheus_client.MetricType_COUNTER,
			},
		),
		RowDeletions: b.Counter(
			metric.Metadata{
				Name:        "jobs.row_level_ttl.rows_deleted",
				Help:        "Number of rows deleted by the row level TTL job.",
				Measurement: "num_rows",
				Unit:        metric.Unit_COUNT,
				MetricType:  io_prometheus_client.MetricType_COUNTER,
			},
		),
		NumActiveRanges: b.Gauge(
			metric.Metadata{
				Name:        "jobs.row_level_ttl.num_active_ranges",
				Help:        "Number of active workers attempting to delete for row level TTL.",
				Measurement: "num_active_workers",
				Unit:        metric.Unit_COUNT,
			},
		),
		TotalRows: b.Gauge(
			metric.Metadata{
				Name:        "jobs.row_level_ttl.total_rows",
				Help:        "Approximate number of rows on the TTL table.",
				Measurement: "total_rows",
				Unit:        metric.Unit_COUNT,
			},
		),
		TotalExpiredRows: b.Gauge(
			metric.Metadata{
				Name:        "jobs.row_level_ttl.total_expired_rows",
				Help:        "Approximate number of rows that have expired the TTL on the TTL table.",
				Measurement: "total_expired_rows",
				Unit:        metric.Unit_COUNT,
			},
		),
	}
	ret.defaultRowLevelMetrics = ret.metricsWithChildren("default")
	ret.mu.m = make(map[string]rowLevelTTLMetrics)
	return ret
}

func init() {
	jobs.MakeRowLevelTTLMetricsHook = makeRowLevelTTLAggMetrics
}
