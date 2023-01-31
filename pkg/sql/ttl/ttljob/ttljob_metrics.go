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
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// RowLevelTTLAggMetrics are the row-level TTL job agg metrics.
type RowLevelTTLAggMetrics struct {
	SpanTotalDuration *aggmetric.AggHistogram
	SelectDuration    *aggmetric.AggHistogram
	DeleteDuration    *aggmetric.AggHistogram
	RowSelections     *aggmetric.AggCounter
	RowDeletions      *aggmetric.AggCounter
	NumActiveSpans    *aggmetric.AggGauge
	TotalRows         *aggmetric.AggGauge
	TotalExpiredRows  *aggmetric.AggGauge

	defaultRowLevelMetrics rowLevelTTLMetrics
	mu                     struct {
		syncutil.Mutex
		m map[string]rowLevelTTLMetrics
	}
}

var _ metric.Struct = (*RowLevelTTLAggMetrics)(nil)

type rowLevelTTLMetrics struct {
	SpanTotalDuration *aggmetric.Histogram
	SelectDuration    *aggmetric.Histogram
	DeleteDuration    *aggmetric.Histogram
	RowSelections     *aggmetric.Counter
	RowDeletions      *aggmetric.Counter
	NumActiveSpans    *aggmetric.Gauge
	TotalRows         *aggmetric.Gauge
	TotalExpiredRows  *aggmetric.Gauge
}

// MetricStruct implements the metric.Struct interface.
func (m *RowLevelTTLAggMetrics) MetricStruct() {}

func (m *RowLevelTTLAggMetrics) metricsWithChildren(children ...string) rowLevelTTLMetrics {
	return rowLevelTTLMetrics{
		SpanTotalDuration: m.SpanTotalDuration.AddChild(children...),
		SelectDuration:    m.SelectDuration.AddChild(children...),
		DeleteDuration:    m.DeleteDuration.AddChild(children...),
		RowSelections:     m.RowSelections.AddChild(children...),
		RowDeletions:      m.RowDeletions.AddChild(children...),
		NumActiveSpans:    m.NumActiveSpans.AddChild(children...),
		TotalRows:         m.TotalRows.AddChild(children...),
		TotalExpiredRows:  m.TotalExpiredRows.AddChild(children...),
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
		SpanTotalDuration: b.Histogram(metric.HistogramOptions{
			Metadata: metric.Metadata{
				Name:        "jobs.row_level_ttl.span_total_duration",
				Help:        "Duration for processing a span during row level TTL.",
				Measurement: "nanoseconds",
				Unit:        metric.Unit_NANOSECONDS,
				MetricType:  io_prometheus_client.MetricType_HISTOGRAM,
			},
			MaxVal:   time.Hour.Nanoseconds(),
			SigFigs:  sigFigs,
			Duration: histogramWindowInterval,
			Buckets:  metric.LongRunning60mLatencyBuckets,
		}),
		SelectDuration: b.Histogram(metric.HistogramOptions{
			Metadata: metric.Metadata{
				Name:        "jobs.row_level_ttl.select_duration",
				Help:        "Duration for select requests during row level TTL.",
				Measurement: "nanoseconds",
				Unit:        metric.Unit_NANOSECONDS,
				MetricType:  io_prometheus_client.MetricType_HISTOGRAM,
			},
			MaxVal:   time.Minute.Nanoseconds(),
			SigFigs:  sigFigs,
			Duration: histogramWindowInterval,
			Buckets:  metric.BatchProcessLatencyBuckets,
		}),
		DeleteDuration: b.Histogram(metric.HistogramOptions{
			Metadata: metric.Metadata{
				Name:        "jobs.row_level_ttl.delete_duration",
				Help:        "Duration for delete requests during row level TTL.",
				Measurement: "nanoseconds",
				Unit:        metric.Unit_NANOSECONDS,
				MetricType:  io_prometheus_client.MetricType_HISTOGRAM,
			},
			MaxVal:   time.Minute.Nanoseconds(),
			SigFigs:  sigFigs,
			Duration: histogramWindowInterval,
			Buckets:  metric.BatchProcessLatencyBuckets,
		}),
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
		NumActiveSpans: b.Gauge(
			metric.Metadata{
				Name:        "jobs.row_level_ttl.num_active_spans",
				Help:        "Number of active spans the TTL job is deleting from.",
				Measurement: "num_active_spans",
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

func (m *rowLevelTTLMetrics) fetchStatistics(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	relationName string,
	details jobspb.RowLevelTTLDetails,
	aostDuration time.Duration,
	ttlExpr catpb.Expression,
) error {
	aost, err := tree.MakeDTimestampTZ(timeutil.Now().Add(aostDuration), time.Microsecond)
	if err != nil {
		return err
	}

	for _, c := range []struct {
		opName string
		query  string
		args   []interface{}
		gauge  *aggmetric.Gauge
	}{
		{
			opName: fmt.Sprintf("ttl num rows stats %s", relationName),
			query:  `SELECT count(1) FROM [%d AS t] AS OF SYSTEM TIME %s`,
			gauge:  m.TotalRows,
		},
		{
			opName: fmt.Sprintf("ttl num expired rows stats %s", relationName),
			query:  `SELECT count(1) FROM [%d AS t] AS OF SYSTEM TIME %s WHERE ` + string(ttlExpr) + ` < $1`,
			args:   []interface{}{details.Cutoff},
			gauge:  m.TotalExpiredRows,
		},
	} {
		// User a super low quality of service (lower than TTL low), as we don't
		// really care if statistics gets left behind and prefer the TTL job to
		// have priority.
		qosLevel := sessiondatapb.SystemLow
		datums, err := execCfg.InternalDB.Executor().QueryRowEx(
			ctx,
			c.opName,
			nil,
			sessiondata.InternalExecutorOverride{
				User:             username.RootUserName(),
				QualityOfService: &qosLevel,
			},
			fmt.Sprintf(c.query, details.TableID, aost.String()),
			c.args...,
		)
		if err != nil {
			return err
		}
		c.gauge.Update(int64(tree.MustBeDInt(datums[0])))
	}
	return nil
}

func init() {
	jobs.MakeRowLevelTTLMetricsHook = makeRowLevelTTLAggMetrics
}
