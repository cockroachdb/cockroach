// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmwriter

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/obs/clustermetrics/cmmetrics"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	prometheusgo "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// sqlStoreTestEnv provides common test infrastructure for SQLStore tests.
type sqlStoreTestEnv struct {
	ctx   context.Context
	store *SQLStore
}

func newSQLStoreTestEnv(t *testing.T) *sqlStoreTestEnv {
	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	t.Cleanup(func() { srv.Stopper().Stop(ctx) })

	s := srv.ApplicationLayer()
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	db := s.InternalDB().(isql.DB)
	store := newSQLStore(db, execCfg.NodeInfo.NodeID, s.ClusterSettings())

	return &sqlStoreTestEnv{
		ctx:   ctx,
		store: store,
	}
}

// findMetric returns the first metric with the given name from a slice.
func findMetric(metrics []cmmetrics.WritableMetric, name string) cmmetrics.WritableMetric {
	for _, m := range metrics {
		if m.GetName(false /* useStaticLabels */) == name {
			return m
		}
	}
	return nil
}

func TestSQLStore_Write(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer cmmetrics.TestingAllowNonInitConstruction()()

	t.Run("counter", func(t *testing.T) {
		env := newSQLStoreTestEnv(t)
		c := cmmetrics.NewCounter(metric.Metadata{Name: "test.counter"})
		c.Inc(42)

		require.NoError(t, env.store.Write(env.ctx, []cmmetrics.WritableMetric{c}))

		metrics, err := env.store.Get(env.ctx)
		require.NoError(t, err)
		require.Len(t, metrics, 1)
		require.Equal(t, "test.counter", metrics[0].GetName(false))
		require.Equal(t, prometheusgo.MetricType_COUNTER.Enum(), metrics[0].GetType())
		require.Equal(t, int64(42), metrics[0].Value())
	})

	t.Run("gauge", func(t *testing.T) {
		env := newSQLStoreTestEnv(t)
		g := cmmetrics.NewGauge(metric.Metadata{Name: "test.gauge"})
		g.Update(100)

		require.NoError(t, env.store.Write(env.ctx, []cmmetrics.WritableMetric{g}))

		metrics, err := env.store.Get(env.ctx)
		require.NoError(t, err)
		require.Len(t, metrics, 1)
		require.Equal(t, "test.gauge", metrics[0].GetName(false))
		require.Equal(t, prometheusgo.MetricType_GAUGE.Enum(), metrics[0].GetType())
		require.Equal(t, int64(100), metrics[0].Value())
	})

	t.Run("counter accumulates on upsert", func(t *testing.T) {
		env := newSQLStoreTestEnv(t)
		c := cmmetrics.NewCounter(metric.Metadata{Name: "test.counter.accum"})
		c.Inc(10)

		require.NoError(t, env.store.Write(env.ctx, []cmmetrics.WritableMetric{c}))

		// Simulate what the writer does: reset, then increment again.
		c.Reset()
		c.Inc(5)
		require.NoError(t, env.store.Write(env.ctx, []cmmetrics.WritableMetric{c}))

		// Counter should accumulate: 10 + 5 = 15.
		metrics, err := env.store.Get(env.ctx)
		require.NoError(t, err)
		require.Len(t, metrics, 1)
		require.Equal(t, int64(15), metrics[0].Value())
	})

	t.Run("gauge replaces on upsert", func(t *testing.T) {
		env := newSQLStoreTestEnv(t)
		g := cmmetrics.NewGauge(metric.Metadata{Name: "test.gauge.replace"})
		g.Update(100)

		require.NoError(t, env.store.Write(env.ctx, []cmmetrics.WritableMetric{g}))

		g.Update(200)
		require.NoError(t, env.store.Write(env.ctx, []cmmetrics.WritableMetric{g}))

		// Gauge should replace: value is 200, not 300.
		metrics, err := env.store.Get(env.ctx)
		require.NoError(t, err)
		require.Len(t, metrics, 1)
		require.Equal(t, int64(200), metrics[0].Value())
	})

	t.Run("stopwatch", func(t *testing.T) {
		env := newSQLStoreTestEnv(t)
		sw := cmmetrics.NewWriteStopwatch(metric.Metadata{Name: "test.stopwatch"}, timeutil.DefaultTimeSource{})
		sw.SetStartTime()

		require.NoError(t, env.store.Write(env.ctx, []cmmetrics.WritableMetric{sw}))

		metrics, err := env.store.Get(env.ctx)
		require.NoError(t, err)
		require.Len(t, metrics, 1)
		require.Equal(t, "test.stopwatch", metrics[0].GetName(false))
		require.Equal(t, prometheusgo.MetricType_GAUGE.Enum(), metrics[0].GetType())
		require.NotZero(t, metrics[0].Value())
	})

	t.Run("stopwatch replaces on upsert", func(t *testing.T) {
		env := newSQLStoreTestEnv(t)
		sw := cmmetrics.NewWriteStopwatch(metric.Metadata{Name: "test.stopwatch.replace"}, timeutil.DefaultTimeSource{})

		// Write initial timestamp.
		sw.Gauge.Update(1000)
		require.NoError(t, env.store.Write(env.ctx, []cmmetrics.WritableMetric{sw}))

		// Write updated timestamp — should replace, not accumulate.
		sw.Gauge.Update(2000)
		require.NoError(t, env.store.Write(env.ctx, []cmmetrics.WritableMetric{sw}))

		metrics, err := env.store.Get(env.ctx)
		require.NoError(t, err)
		require.Len(t, metrics, 1)
		require.Equal(t, int64(2000), metrics[0].Value())
	})

	t.Run("multiple metrics", func(t *testing.T) {
		env := newSQLStoreTestEnv(t)
		c := cmmetrics.NewCounter(metric.Metadata{Name: "test.multi.counter"})
		g := cmmetrics.NewGauge(metric.Metadata{Name: "test.multi.gauge"})
		c.Inc(7)
		g.Update(200)

		require.NoError(t, env.store.Write(env.ctx, []cmmetrics.WritableMetric{c, g}))

		metrics, err := env.store.Get(env.ctx)
		require.NoError(t, err)
		require.Len(t, metrics, 2)

		cm := findMetric(metrics, "test.multi.counter")
		require.NotNil(t, cm)
		require.Equal(t, int64(7), cm.Value())

		gm := findMetric(metrics, "test.multi.gauge")
		require.NotNil(t, gm)
		require.Equal(t, int64(200), gm.Value())
	})
}
