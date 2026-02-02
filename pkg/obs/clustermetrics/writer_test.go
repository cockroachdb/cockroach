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
	Metric    ClusterMetric
	Value     int64
	Timestamp time.Time
}

// mapStore is a MetricStore implementation backed by an in-memory map.
// Useful for testing.
type mapStore struct {
	mu struct {
		syncutil.Mutex
		metrics map[string]storedMetric
	}
}

// newMapStore creates a new mapStore.
func newMapStore() *mapStore {
	s := &mapStore{}
	s.mu.metrics = make(map[string]storedMetric)
	return s
}

// Write stores the given metrics in memory.
func (s *mapStore) Write(_ context.Context, metrics []ClusterMetric) error {
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

// Get retrieves all stored metrics.
func (s *mapStore) Get(_ context.Context) ([]ClusterMetric, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]ClusterMetric, 0, len(s.mu.metrics))
	for _, stored := range s.mu.metrics {
		result = append(result, stored.Metric)
	}
	return result, nil
}

// get returns the stored metric with the given name, if it exists.
func (s *mapStore) get(name string) (storedMetric, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.mu.metrics[name]
	return m, ok
}

// getAll returns a copy of all stored metrics.
func (s *mapStore) getAll() map[string]storedMetric {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]storedMetric, len(s.mu.metrics))
	for k, v := range s.mu.metrics {
		result[k] = v
	}
	return result
}

func TestWriter_CounterAccumulation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	store := newMapStore()
	registry := metric.NewRegistry()
	w := NewWriter(store, registry, nil)

	counter := NewClusterCounter(metric.Metadata{Name: "cluster.sql.count"})
	registry.AddMetric(counter)

	counter.Inc(5)
	counter.Inc(3)

	w.Flush(ctx)

	stored, ok := store.get("cluster.sql.count")
	require.True(t, ok)
	require.Equal(t, int64(8), stored.Value)

	// Verify counter was reset.
	require.Equal(t, int64(0), counter.Count())

	// Increment again and flush.
	counter.Inc(2)
	w.Flush(ctx)

	stored, _ = store.get("cluster.sql.count")
	require.Equal(t, int64(2), stored.Value)
}

func TestWriter_GaugeValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	store := newMapStore()
	registry := metric.NewRegistry()
	w := NewWriter(store, registry, nil)

	gauge := NewClusterGauge(metric.Metadata{Name: "cluster.job.status"})
	registry.AddMetric(gauge)

	gauge.Update(100)

	w.Flush(ctx)

	stored, ok := store.get("cluster.job.status")
	require.True(t, ok)
	require.Equal(t, int64(100), stored.Value)

	// Gauge retains value after flush.
	require.Equal(t, int64(100), gauge.Value())
}

func TestWriter_GaugeNoReset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	store := newMapStore()
	registry := metric.NewRegistry()
	w := NewWriter(store, registry, nil)

	gauge := NewClusterGauge(metric.Metadata{Name: "cluster.gauge.test"})
	registry.AddMetric(gauge)

	gauge.Update(42)
	w.Flush(ctx)

	// Gauge value should remain.
	require.Equal(t, int64(42), gauge.Value())

	// Flush again without changing the gauge - should not flush since not dirty.
	w.Flush(ctx)

	stored, _ := store.get("cluster.gauge.test")
	require.Equal(t, int64(42), stored.Value)
	require.Equal(t, int64(42), gauge.Value())
}

func TestWriter_MultipleMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	store := newMapStore()
	registry := metric.NewRegistry()
	w := NewWriter(store, registry, nil)

	counter1 := NewClusterCounter(metric.Metadata{Name: "cluster.counter1"})
	counter2 := NewClusterCounter(metric.Metadata{Name: "cluster.counter2"})
	gauge := NewClusterGauge(metric.Metadata{Name: "cluster.gauge1"})

	registry.AddMetric(counter1)
	registry.AddMetric(counter2)
	registry.AddMetric(gauge)

	counter1.Inc(10)
	counter2.Inc(20)
	gauge.Update(30)

	w.Flush(ctx)

	allStored := store.getAll()
	require.Len(t, allStored, 3)

	require.Equal(t, int64(10), allStored["cluster.counter1"].Value)
	require.Equal(t, int64(20), allStored["cluster.counter2"].Value)
	require.Equal(t, int64(30), allStored["cluster.gauge1"].Value)
}

func TestWriter_CounterReset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	store := newMapStore()
	registry := metric.NewRegistry()
	w := NewWriter(store, registry, nil)

	counter := NewClusterCounter(metric.Metadata{Name: "cluster.reset.test"})
	registry.AddMetric(counter)

	counter.Inc(100)
	require.Equal(t, int64(100), counter.Count())

	w.Flush(ctx)

	// Counter should be reset to zero after flush.
	require.Equal(t, int64(0), counter.Count())

	// Stored value should be 100.
	stored, _ := store.get("cluster.reset.test")
	require.Equal(t, int64(100), stored.Value)
}

func TestWriter_FlushMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	store := newMapStore()
	registry := metric.NewRegistry()
	writerMetrics := NewWriterMetrics()
	w := NewWriter(store, registry, writerMetrics)

	counter := NewClusterCounter(metric.Metadata{Name: "cluster.test"})
	registry.AddMetric(counter)
	counter.Inc(1)

	w.Flush(ctx)

	// Check operational metrics were recorded.
	require.Equal(t, int64(1), writerMetrics.FlushCount.Count())
	require.Equal(t, int64(1), writerMetrics.MetricsWritten.Count())
	require.Equal(t, int64(0), writerMetrics.FlushErrors.Count())
}

func TestWriter_EmptyFlush(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	store := newMapStore()
	registry := metric.NewRegistry()
	writerMetrics := NewWriterMetrics()
	w := NewWriter(store, registry, writerMetrics)

	// Flush with no registered metrics.
	w.Flush(ctx)

	// No metrics should have been written.
	require.Equal(t, int64(0), writerMetrics.FlushCount.Count())
	require.Equal(t, int64(0), writerMetrics.MetricsWritten.Count())
}

func TestWriter_Start(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	store := newMapStore()
	st := cluster.MakeTestingClusterSettings()
	FlushInterval.Override(ctx, &st.SV, 50*time.Millisecond)

	registry := metric.NewRegistry()
	writerMetrics := NewWriterMetrics()
	w := NewWriter(store, registry, writerMetrics)

	counter := NewClusterCounter(metric.Metadata{Name: "cluster.periodic.test"})
	registry.AddMetric(counter)
	counter.Inc(5)

	// Start periodic flush in background.
	require.NoError(t, w.Start(ctx, stopper, &st.SV))

	// Wait for at least one flush (accounting for jitter).
	time.Sleep(150 * time.Millisecond)
	cancel()

	require.GreaterOrEqual(t, writerMetrics.FlushCount.Count(), int64(1))
}

func TestWriter_DisabledSetting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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

	counter := NewClusterCounter(metric.Metadata{Name: "cluster.disabled.test"})
	registry.AddMetric(counter)
	counter.Inc(5)

	// Start periodic flush in background.
	require.NoError(t, w.Start(ctx, stopper, &st.SV))

	// Wait for some ticks.
	time.Sleep(200 * time.Millisecond)
	cancel()

	// No flushes should have occurred because writer is disabled.
	require.Equal(t, int64(0), writerMetrics.FlushCount.Count())
}

func TestWriter_MetricStructRegistration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	store := newMapStore()
	registry := metric.NewRegistry()
	w := NewWriter(store, registry, nil)

	type TestClusterMetrics struct {
		SQLCount  *ClusterCounter
		JobStatus *ClusterGauge
	}
	metrics := TestClusterMetrics{
		SQLCount:  NewClusterCounter(metric.Metadata{Name: "cluster.sql.struct.count"}),
		JobStatus: NewClusterGauge(metric.Metadata{Name: "cluster.job.struct.status"}),
	}

	registry.AddMetricStruct(&metrics)

	metrics.SQLCount.Inc(15)
	metrics.JobStatus.Update(200)

	w.Flush(ctx)

	stored, ok := store.get("cluster.sql.struct.count")
	require.True(t, ok)
	require.Equal(t, int64(15), stored.Value)

	stored, ok = store.get("cluster.job.struct.status")
	require.True(t, ok)
	require.Equal(t, int64(200), stored.Value)
}

func TestWriter_ZeroCounterNotFlushed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	store := newMapStore()
	registry := metric.NewRegistry()
	writerMetrics := NewWriterMetrics()
	w := NewWriter(store, registry, writerMetrics)

	counter := NewClusterCounter(metric.Metadata{Name: "cluster.zero.counter"})
	registry.AddMetric(counter)

	// Don't increment the counter - it should be 0.
	require.Equal(t, int64(0), counter.Count())

	w.Flush(ctx)

	// Counter with 0 value should not be flushed.
	_, ok := store.get("cluster.zero.counter")
	require.False(t, ok, "counter with 0 value should not be flushed")

	// No metrics should have been written.
	require.Equal(t, int64(0), writerMetrics.FlushCount.Count())
}

func TestWriter_UnchangedGaugeNotFlushed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	store := newMapStore()
	registry := metric.NewRegistry()
	writerMetrics := NewWriterMetrics()
	w := NewWriter(store, registry, writerMetrics)

	gauge := NewClusterGauge(metric.Metadata{Name: "cluster.unchanged.gauge"})
	registry.AddMetric(gauge)

	gauge.Update(50)

	// First flush should include the gauge.
	w.Flush(ctx)

	require.Equal(t, int64(1), writerMetrics.FlushCount.Count())
	require.Equal(t, int64(1), writerMetrics.MetricsWritten.Count())

	stored, ok := store.get("cluster.unchanged.gauge")
	require.True(t, ok)
	require.Equal(t, int64(50), stored.Value)

	// Second flush without changing gauge should NOT write anything.
	w.Flush(ctx)

	// FlushCount should still be 1 because the second flush had no changes.
	require.Equal(t, int64(1), writerMetrics.FlushCount.Count())
	require.Equal(t, int64(1), writerMetrics.MetricsWritten.Count())

	// Now change the gauge and flush again.
	gauge.Update(75)
	w.Flush(ctx)

	require.Equal(t, int64(2), writerMetrics.FlushCount.Count())
	require.Equal(t, int64(2), writerMetrics.MetricsWritten.Count())

	stored, _ = store.get("cluster.unchanged.gauge")
	require.Equal(t, int64(75), stored.Value)
}

func TestWriter_MixedChangedUnchanged(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	store := newMapStore()
	registry := metric.NewRegistry()
	writerMetrics := NewWriterMetrics()
	w := NewWriter(store, registry, writerMetrics)

	counter := NewClusterCounter(metric.Metadata{Name: "cluster.mixed.counter"})
	gauge := NewClusterGauge(metric.Metadata{Name: "cluster.mixed.gauge"})
	registry.AddMetric(counter)
	registry.AddMetric(gauge)

	counter.Inc(10)
	gauge.Update(100)

	// First flush: both metrics changed.
	w.Flush(ctx)
	require.Equal(t, int64(2), writerMetrics.MetricsWritten.Count())

	// Second flush: only counter incremented, gauge unchanged.
	counter.Inc(5)
	w.Flush(ctx)

	// Only 1 metric (the counter) should be written.
	require.Equal(t, int64(3), writerMetrics.MetricsWritten.Count())

	stored, _ := store.get("cluster.mixed.counter")
	require.Equal(t, int64(5), stored.Value)

	// Gauge should still have the old value in store.
	stored, _ = store.get("cluster.mixed.gauge")
	require.Equal(t, int64(100), stored.Value)
}

func TestWriter_UnupdatedGaugeNotFlushed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	store := newMapStore()
	registry := metric.NewRegistry()
	writerMetrics := NewWriterMetrics()
	w := NewWriter(store, registry, writerMetrics)

	// Register a gauge but never update it.
	gauge := NewClusterGauge(metric.Metadata{Name: "cluster.unupdated.gauge"})
	registry.AddMetric(gauge)

	// Gauge was never updated, so it should not be flushed.
	w.Flush(ctx)

	_, ok := store.get("cluster.unupdated.gauge")
	require.False(t, ok, "gauge that was never updated should not be flushed")
	require.Equal(t, int64(0), writerMetrics.FlushCount.Count())
}

func TestWriter_GaugeIncDec(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	store := newMapStore()
	registry := metric.NewRegistry()
	writerMetrics := NewWriterMetrics()
	w := NewWriter(store, registry, writerMetrics)

	gauge := NewClusterGauge(metric.Metadata{Name: "cluster.incdec.gauge"})
	registry.AddMetric(gauge)

	// Use Inc to update the gauge.
	gauge.Inc(10)
	w.Flush(ctx)

	stored, ok := store.get("cluster.incdec.gauge")
	require.True(t, ok)
	require.Equal(t, int64(10), stored.Value)
	require.Equal(t, int64(1), writerMetrics.FlushCount.Count())

	// Use Dec to update the gauge.
	gauge.Dec(3)
	w.Flush(ctx)

	stored, _ = store.get("cluster.incdec.gauge")
	require.Equal(t, int64(7), stored.Value)
	require.Equal(t, int64(2), writerMetrics.FlushCount.Count())
}
