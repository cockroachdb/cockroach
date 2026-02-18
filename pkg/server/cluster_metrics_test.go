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
// server, registers metrics with the cluster metrics writer, triggers a
// flush, and verifies the metrics were persisted to system.cluster_metrics.
func TestClusterMetricsWriterIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()
	writer := s.SQLServerInternal().(*SQLServer).ClusterMetricsWriter()
	runner := sqlutils.MakeSQLRunner(s.SQLConn(t))

	t.Run("counter persisted to system table", func(t *testing.T) {
		c := clustermetrics.NewCounter(metric.Metadata{Name: "test.integration.counter"})
		writer.AddMetric(c)
		c.Inc(42)

		writer.Flush(ctx)

		var name, typ string
		var value int64
		runner.QueryRow(t,
			`SELECT name, type, value FROM system.cluster_metrics WHERE name = $1`,
			"test.integration.counter",
		).Scan(&name, &typ, &value)
		require.Equal(t, "test.integration.counter", name)
		require.Equal(t, "counter", typ)
		require.Equal(t, int64(42), value)
	})

	t.Run("gauge persisted to system table", func(t *testing.T) {
		g := clustermetrics.NewGauge(metric.Metadata{Name: "test.integration.gauge"})
		writer.AddMetric(g)
		g.Update(99)

		writer.Flush(ctx)

		var name, typ string
		var value int64
		runner.QueryRow(t,
			`SELECT name, type, value FROM system.cluster_metrics WHERE name = $1`,
			"test.integration.gauge",
		).Scan(&name, &typ, &value)
		require.Equal(t, "test.integration.gauge", name)
		require.Equal(t, "gauge", typ)
		require.Equal(t, int64(99), value)
	})

	t.Run("counter resets after flush and accumulates new value", func(t *testing.T) {
		c := clustermetrics.NewCounter(metric.Metadata{Name: "test.integration.counter.reset"})
		writer.AddMetric(c)

		// First flush.
		c.Inc(10)
		writer.Flush(ctx)

		var value int64
		runner.QueryRow(t,
			`SELECT value FROM system.cluster_metrics WHERE name = $1`,
			"test.integration.counter.reset",
		).Scan(&value)
		require.Equal(t, int64(10), value)

		// After flush, counter resets. New increments start from zero.
		c.Inc(3)
		writer.Flush(ctx)

		runner.QueryRow(t,
			`SELECT value FROM system.cluster_metrics WHERE name = $1`,
			"test.integration.counter.reset",
		).Scan(&value)
		require.Equal(t, int64(3), value)
	})

	t.Run("gauge updates in place", func(t *testing.T) {
		g := clustermetrics.NewGauge(metric.Metadata{Name: "test.integration.gauge.update"})
		writer.AddMetric(g)

		g.Update(100)
		writer.Flush(ctx)

		var value int64
		runner.QueryRow(t,
			`SELECT value FROM system.cluster_metrics WHERE name = $1`,
			"test.integration.gauge.update",
		).Scan(&value)
		require.Equal(t, int64(100), value)

		// Update the gauge and flush again.
		g.Update(200)
		writer.Flush(ctx)

		runner.QueryRow(t,
			`SELECT value FROM system.cluster_metrics WHERE name = $1`,
			"test.integration.gauge.update",
		).Scan(&value)
		require.Equal(t, int64(200), value)

		// Verify only one row exists (upsert, not insert).
		var count int
		runner.QueryRow(t,
			`SELECT count(*) FROM system.cluster_metrics WHERE name = $1`,
			"test.integration.gauge.update",
		).Scan(&count)
		require.Equal(t, 1, count)
	})

	t.Run("struct registration and flush", func(t *testing.T) {
		type appMetrics struct {
			Requests *clustermetrics.Counter
			Latency  *clustermetrics.Gauge
		}
		m := &appMetrics{
			Requests: clustermetrics.NewCounter(metric.Metadata{Name: "test.integration.requests"}),
			Latency:  clustermetrics.NewGauge(metric.Metadata{Name: "test.integration.latency"}),
		}
		writer.AddMetricStruct(m)

		m.Requests.Inc(100)
		m.Latency.Update(55)
		writer.Flush(ctx)

		var count int
		runner.QueryRow(t,
			`SELECT count(*) FROM system.cluster_metrics WHERE name LIKE 'test.integration.requests' OR name LIKE 'test.integration.latency'`,
		).Scan(&count)
		require.Equal(t, 2, count)

		var value int64
		runner.QueryRow(t,
			`SELECT value FROM system.cluster_metrics WHERE name = $1`,
			"test.integration.requests",
		).Scan(&value)
		require.Equal(t, int64(100), value)

		runner.QueryRow(t,
			`SELECT value FROM system.cluster_metrics WHERE name = $1`,
			"test.integration.latency",
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
