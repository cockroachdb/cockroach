// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clustermetrics

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// storedMetric represents a stored metric value with metadata.
type storedMetric struct {
	Metric    Metric
	Value     int64
	Timestamp time.Time
}

// mapStore is a MetricStore implementation backed by an in-memory map.
type mapStore struct {
	mu struct {
		syncutil.Mutex
		metrics map[string]storedMetric
	}
}

func newMapStore() *mapStore {
	s := &mapStore{}
	s.mu.metrics = make(map[string]storedMetric)
	return s
}

func (s *mapStore) Write(_ context.Context, metrics []Metric) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := timeutil.Now()
	for _, m := range metrics {
		s.mu.metrics[m.GetName(false)] = storedMetric{
			Metric:    m,
			Value:     m.Get(),
			Timestamp: now,
		}
	}
	return nil
}

func (s *mapStore) Get(_ context.Context) ([]Metric, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Metric, 0, len(s.mu.metrics))
	for _, stored := range s.mu.metrics {
		result = append(result, stored.Metric)
	}
	return result, nil
}

func (s *mapStore) get(name string) (storedMetric, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.mu.metrics[name]
	return m, ok
}

func (s *mapStore) getAll() map[string]storedMetric {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]storedMetric, len(s.mu.metrics))
	for k, v := range s.mu.metrics {
		result[k] = v
	}
	return result
}

// testEnv provides common test infrastructure for writer tests.
type testEnv struct {
	ctx           context.Context
	store         *mapStore
	registry      *metric.Registry
	writerMetrics *WriterMetrics
	writer        *Writer
}

func newTestEnv() *testEnv {
	ctx := context.Background()
	store := newMapStore()
	registry := metric.NewRegistry()
	writerMetrics := NewWriterMetrics()
	writer := NewWriter(store, registry, writerMetrics)
	return &testEnv{
		ctx:           ctx,
		store:         store,
		registry:      registry,
		writerMetrics: writerMetrics,
		writer:        writer,
	}
}

func TestWriter_Flush(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("counter accumulation and reset", func(t *testing.T) {
		env := newTestEnv()
		counter := NewCounter(metric.Metadata{Name: "test.counter"})
		env.registry.AddMetric(counter)

		counter.Inc(5)
		counter.Inc(3)
		env.writer.Flush(env.ctx)

		stored, ok := env.store.get("test.counter")
		require.True(t, ok)
		require.Equal(t, int64(8), stored.Value)
		require.Equal(t, int64(0), counter.Count(), "counter should reset after flush")

		// Increment again and verify new value is stored.
		counter.Inc(2)
		env.writer.Flush(env.ctx)

		stored, _ = env.store.get("test.counter")
		require.Equal(t, int64(2), stored.Value)
	})

	t.Run("counter zero not flushed", func(t *testing.T) {
		env := newTestEnv()
		counter := NewCounter(metric.Metadata{Name: "test.counter"})
		env.registry.AddMetric(counter)

		// Don't increment - should not be flushed.
		env.writer.Flush(env.ctx)

		_, ok := env.store.get("test.counter")
		require.False(t, ok, "zero counter should not be flushed")
	})

	t.Run("gauge value and retention", func(t *testing.T) {
		env := newTestEnv()
		gauge := NewGauge(metric.Metadata{Name: "test.gauge"})
		env.registry.AddMetric(gauge)

		gauge.Update(100)
		env.writer.Flush(env.ctx)

		stored, ok := env.store.get("test.gauge")
		require.True(t, ok)
		require.Equal(t, int64(100), stored.Value)
		require.Equal(t, int64(100), gauge.Value(), "gauge should retain value after flush")
	})

	t.Run("gauge unchanged not flushed", func(t *testing.T) {
		env := newTestEnv()
		gauge := NewGauge(metric.Metadata{Name: "test.gauge"})
		env.registry.AddMetric(gauge)

		gauge.Update(50)
		env.writer.Flush(env.ctx)
		require.Equal(t, int64(1), env.writerMetrics.MetricsWritten.Count())

		// Second flush without change should not write.
		env.writer.Flush(env.ctx)
		require.Equal(t, int64(1), env.writerMetrics.MetricsWritten.Count())

		// Update and flush again.
		gauge.Update(75)
		env.writer.Flush(env.ctx)
		require.Equal(t, int64(2), env.writerMetrics.MetricsWritten.Count())

		stored, _ := env.store.get("test.gauge")
		require.Equal(t, int64(75), stored.Value)
	})

	t.Run("gauge unupdated not flushed", func(t *testing.T) {
		env := newTestEnv()
		gauge := NewGauge(metric.Metadata{Name: "test.gauge"})
		env.registry.AddMetric(gauge)

		// Never update - should not be flushed.
		env.writer.Flush(env.ctx)

		_, ok := env.store.get("test.gauge")
		require.False(t, ok, "unupdated gauge should not be flushed")
	})

	t.Run("gauge inc and dec mark dirty", func(t *testing.T) {
		env := newTestEnv()
		gauge := NewGauge(metric.Metadata{Name: "test.gauge"})
		env.registry.AddMetric(gauge)

		gauge.Inc(10)
		env.writer.Flush(env.ctx)

		stored, ok := env.store.get("test.gauge")
		require.True(t, ok)
		require.Equal(t, int64(10), stored.Value)

		gauge.Dec(3)
		env.writer.Flush(env.ctx)

		stored, _ = env.store.get("test.gauge")
		require.Equal(t, int64(7), stored.Value)
	})

	t.Run("multiple metrics", func(t *testing.T) {
		env := newTestEnv()
		counter1 := NewCounter(metric.Metadata{Name: "test.counter1"})
		counter2 := NewCounter(metric.Metadata{Name: "test.counter2"})
		gauge := NewGauge(metric.Metadata{Name: "test.gauge"})

		env.registry.AddMetric(counter1)
		env.registry.AddMetric(counter2)
		env.registry.AddMetric(gauge)

		counter1.Inc(10)
		counter2.Inc(20)
		gauge.Update(30)
		env.writer.Flush(env.ctx)

		allStored := env.store.getAll()
		require.Len(t, allStored, 3)
		require.Equal(t, int64(10), allStored["test.counter1"].Value)
		require.Equal(t, int64(20), allStored["test.counter2"].Value)
		require.Equal(t, int64(30), allStored["test.gauge"].Value)
	})

	t.Run("mixed changed and unchanged", func(t *testing.T) {
		env := newTestEnv()
		counter := NewCounter(metric.Metadata{Name: "test.counter"})
		gauge := NewGauge(metric.Metadata{Name: "test.gauge"})
		env.registry.AddMetric(counter)
		env.registry.AddMetric(gauge)

		counter.Inc(10)
		gauge.Update(100)
		env.writer.Flush(env.ctx)
		require.Equal(t, int64(2), env.writerMetrics.MetricsWritten.Count())

		// Only update counter.
		counter.Inc(5)
		env.writer.Flush(env.ctx)
		require.Equal(t, int64(3), env.writerMetrics.MetricsWritten.Count())

		stored, _ := env.store.get("test.counter")
		require.Equal(t, int64(5), stored.Value)

		stored, _ = env.store.get("test.gauge")
		require.Equal(t, int64(100), stored.Value)
	})

	t.Run("metric struct registration", func(t *testing.T) {
		env := newTestEnv()

		type TestMetrics struct {
			SQLCount  *Counter
			JobStatus *Gauge
		}
		metrics := TestMetrics{
			SQLCount:  NewCounter(metric.Metadata{Name: "test.sql.count"}),
			JobStatus: NewGauge(metric.Metadata{Name: "test.job.status"}),
		}
		env.registry.AddMetricStruct(&metrics)

		metrics.SQLCount.Inc(15)
		metrics.JobStatus.Update(200)
		env.writer.Flush(env.ctx)

		stored, ok := env.store.get("test.sql.count")
		require.True(t, ok)
		require.Equal(t, int64(15), stored.Value)

		stored, ok = env.store.get("test.job.status")
		require.True(t, ok)
		require.Equal(t, int64(200), stored.Value)
	})
}

func TestWriter_OperationalMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("records flush metrics", func(t *testing.T) {
		env := newTestEnv()
		counter := NewCounter(metric.Metadata{Name: "test.counter"})
		env.registry.AddMetric(counter)
		counter.Inc(1)

		env.writer.Flush(env.ctx)

		require.Equal(t, int64(1), env.writerMetrics.FlushCount.Count())
		require.Equal(t, int64(1), env.writerMetrics.MetricsWritten.Count())
		require.Equal(t, int64(0), env.writerMetrics.FlushErrors.Count())
	})

	t.Run("empty flush records nothing", func(t *testing.T) {
		env := newTestEnv()

		env.writer.Flush(env.ctx)

		require.Equal(t, int64(0), env.writerMetrics.FlushCount.Count())
		require.Equal(t, int64(0), env.writerMetrics.MetricsWritten.Count())
	})
}

func TestWriter_PeriodicFlush(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("enabled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		stopper := stop.NewStopper()
		defer stopper.Stop(context.Background())

		store := newMapStore()
		st := cluster.MakeTestingClusterSettings()
		FlushInterval.Override(ctx, &st.SV, 50*time.Millisecond)

		registry := metric.NewRegistry()
		writerMetrics := NewWriterMetrics()
		w := NewWriter(store, registry, writerMetrics)

		counter := NewCounter(metric.Metadata{Name: "test.counter"})
		registry.AddMetric(counter)
		counter.Inc(5)

		require.NoError(t, w.Start(ctx, stopper, &st.SV))

		time.Sleep(150 * time.Millisecond)
		cancel()

		require.GreaterOrEqual(t, writerMetrics.FlushCount.Count(), int64(1))
	})

	t.Run("disabled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		stopper := stop.NewStopper()
		defer stopper.Stop(context.Background())

		store := newMapStore()
		st := cluster.MakeTestingClusterSettings()
		FlushInterval.Override(ctx, &st.SV, 50*time.Millisecond)
		FlushEnabled.Override(ctx, &st.SV, false)

		registry := metric.NewRegistry()
		writerMetrics := NewWriterMetrics()
		w := NewWriter(store, registry, writerMetrics)

		counter := NewCounter(metric.Metadata{Name: "test.counter"})
		registry.AddMetric(counter)
		counter.Inc(5)

		require.NoError(t, w.Start(ctx, stopper, &st.SV))

		time.Sleep(200 * time.Millisecond)
		cancel()

		require.Equal(t, int64(0), writerMetrics.FlushCount.Count())
	})
}
