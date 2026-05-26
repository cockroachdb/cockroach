// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/obs/clustermetrics"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/stretchr/testify/require"
)

// TestClusterMetricsWriterIntegration is an end-to-end test that starts a
// server, registers metrics with the cluster metrics writer, triggers
// flushes, and verifies the metrics were persisted to
// system.cluster_metrics.
func TestClusterMetricsWriterIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer clustermetrics.TestingAllowNonInitConstruction()()

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()
	writer := s.SQLServerInternal().(*SQLServer).ClusterMetricsWriter()
	runner := sqlutils.MakeSQLRunner(s.SQLConn(t))

	// Create and register test metrics up front so each table-driven
	// case can reference its metric via closure.
	counter := clustermetrics.NewCounter(metric.Metadata{Name: "test.int.counter"})
	gauge := clustermetrics.NewGauge(metric.Metadata{Name: "test.int.gauge"})
	counterAccum := clustermetrics.NewCounter(metric.Metadata{Name: "test.int.counter.accum"})
	gaugeUpdate := clustermetrics.NewGauge(metric.Metadata{Name: "test.int.gauge.update"})

	writer.AddMetric(counter)
	writer.AddMetric(gauge)
	writer.AddMetric(counterAccum)
	writer.AddMetric(gaugeUpdate)

	type flushOp struct {
		before func() // called before each flush
		want   int64  // expected DB value after this flush
	}

	tests := []struct {
		name       string
		metricName string
		wantType   string
		flushes    []flushOp
	}{{
		name:       "counter persisted to system table",
		metricName: "test.int.counter",
		wantType:   "COUNTER",
		flushes: []flushOp{{
			before: func() { counter.Inc(42) },
			want:   42,
		}},
	}, {
		name:       "gauge persisted to system table",
		metricName: "test.int.gauge",
		wantType:   "GAUGE",
		flushes: []flushOp{{
			before: func() { gauge.Update(99) },
			want:   99,
		}},
	}, {
		name:       "counter accumulates across flushes",
		metricName: "test.int.counter.accum",
		wantType:   "COUNTER",
		flushes: []flushOp{{
			before: func() { counterAccum.Inc(10) },
			want:   10,
		}, {
			before: func() { counterAccum.Inc(3) },
			want:   13,
		}},
	}, {
		name:       "gauge updates in place",
		metricName: "test.int.gauge.update",
		wantType:   "GAUGE",
		flushes: []flushOp{{
			before: func() { gaugeUpdate.Update(100) },
			want:   100,
		}, {
			before: func() { gaugeUpdate.Update(200) },
			want:   200,
		}},
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, flush := range tc.flushes {
				flush.before()
				writer.Flush(ctx)

				var value int64
				runner.QueryRow(t,
					`SELECT value FROM system.cluster_metrics WHERE name = $1`,
					tc.metricName,
				).Scan(&value)
				require.Equal(t, flush.want, value)
			}

			// Verify the type is correct.
			var typ string
			runner.QueryRow(t,
				`SELECT type FROM system.cluster_metrics WHERE name = $1`,
				tc.metricName,
			).Scan(&typ)
			require.Equal(t, tc.wantType, typ)

			// Verify exactly one row (upsert, not insert).
			var count int
			runner.QueryRow(t,
				`SELECT count(*) FROM system.cluster_metrics WHERE name = $1`,
				tc.metricName,
			).Scan(&count)
			require.Equal(t, 1, count)
		})
	}

	t.Run("struct registration and flush", func(t *testing.T) {
		type appMetrics struct {
			Requests *clustermetrics.Counter
			Latency  *clustermetrics.Gauge
		}
		m := &appMetrics{
			Requests: clustermetrics.NewCounter(metric.Metadata{Name: "test.int.requests"}),
			Latency:  clustermetrics.NewGauge(metric.Metadata{Name: "test.int.latency"}),
		}
		writer.AddMetricStruct(m)

		m.Requests.Inc(100)
		m.Latency.Update(55)
		writer.Flush(ctx)

		var count int
		runner.QueryRow(t,
			`SELECT count(*) FROM system.cluster_metrics WHERE name IN ('test.int.requests', 'test.int.latency')`,
		).Scan(&count)
		require.Equal(t, 2, count)

		var value int64
		runner.QueryRow(t,
			`SELECT value FROM system.cluster_metrics WHERE name = $1`,
			"test.int.requests",
		).Scan(&value)
		require.Equal(t, int64(100), value)

		runner.QueryRow(t,
			`SELECT value FROM system.cluster_metrics WHERE name = $1`,
			"test.int.latency",
		).Scan(&value)
		require.Equal(t, int64(55), value)
	})

	t.Run("operational metrics recorded", func(t *testing.T) {
		writerMetrics := writer.Metrics()
		require.Greater(t, writerMetrics.FlushCount.Count(), int64(0),
			"expected at least one flush to have been recorded")
		require.Greater(t, writerMetrics.MetricsWritten.Count(), int64(0),
			"expected at least one metric to have been written")
		require.Equal(t, int64(0), writerMetrics.FlushErrors.Count(),
			"expected no flush errors")
	})
}
