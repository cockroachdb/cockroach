// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmwriter

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/obs/clustermetrics"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// MetricStore abstracts the storage backend for cluster metrics.
type MetricStore interface {
	// Write writes the given metrics to storage.
	Write(ctx context.Context, metrics []clustermetrics.Metric) error
}

// Writer manages registered cluster metrics and periodically flushes their
// values to a MetricStore. Uses an internal metric.Registry for uniform
// registration and iteration (same pattern as tsdb/prometheus).
type Writer struct {
	store    MetricStore
	registry *metric.Registry
	metrics  *WriterMetrics
}

// NewWriter creates a new Writer backed by an SQLStore. The writer creates
// its own internal registry and operational metrics.
func NewWriter(db isql.DB, sqlIDContainer *base.SQLIDContainer, st *cluster.Settings) *Writer {
	store := newSQLStore(db, sqlIDContainer, st)
	return &Writer{
		store:    store,
		registry: metric.NewRegistry(),
		metrics:  NewWriterMetrics(),
	}
}

// NewWriterWithStore creates a new Writer with the given MetricStore.
// This is useful in tests where a custom store implementation is needed.
func NewWriterWithStore(store MetricStore, metrics *WriterMetrics) *Writer {
	if metrics == nil {
		metrics = NewWriterMetrics()
	}
	return &Writer{
		store:    store,
		registry: metric.NewRegistry(),
		metrics:  metrics,
	}
}

// AddMetric registers a metric with the Writer's internal registry.
func (w *Writer) AddMetric(m metric.Iterable) {
	w.registry.AddMetric(m)
}

// AddMetricStruct registers all metrics in the struct with the Writer's
// internal registry.
func (w *Writer) AddMetricStruct(s interface{}) {
	w.registry.AddMetricStruct(s)
}

// Metrics returns the operational metrics for the Writer. These should be
// registered with a prometheus-exportable registry.
func (w *Writer) Metrics() *WriterMetrics {
	return w.metrics
}

// Flush triggers an immediate flush of all registered metrics to the store.
// Only metrics that have changed since the last flush are written.
// Iterates via registry.Each() to collect dirty metrics.
func (w *Writer) Flush(ctx context.Context) {
	startTime := timeutil.Now()
	var dirty []clustermetrics.Metric

	// Collect dirty metrics to be written.
	w.registry.Each(func(_ string, val interface{}) {
		if m, ok := val.(clustermetrics.Metric); ok {
			if m.IsDirty() {
				dirty = append(dirty, m)
			}
		} else {
			log.Ops.Warningf(ctx, "cannot flush non-cluster-metric of type %T", val)
		}
	})

	if len(dirty) == 0 {
		return
	}

	// Write the metrics to the downstream store.
	err := w.store.Write(ctx, dirty)

	// Record meta metrics.
	elapsed := timeutil.Since(startTime)
	w.metrics.FlushLatency.Update(elapsed.Nanoseconds())
	w.metrics.FlushCount.Inc(1)
	if err != nil {
		w.metrics.FlushErrors.Inc(1)
		log.Ops.Warningf(ctx, "failed to flush cluster metrics: %v", err)
		return
	}
	w.metrics.MetricsWritten.Inc(int64(len(dirty)))

	// Reset metrics after successful write.
	for _, m := range dirty {
		m.Reset()
	}
}

// jitter adds a small jitter to the given duration.
// Returns a duration in the range [dur * 5/6, dur * 7/6).
func jitter(dur time.Duration) time.Duration {
	const jitterFrac = 1.0 / 6.0
	multiplier := 1 + (2*rand.Float64()-1)*jitterFrac
	return time.Duration(float64(dur) * multiplier)
}

// Start starts a background task that periodically calls Flush() on the
// configured interval. Reads FlushInterval and FlushEnabled from cluster settings.
func (w *Writer) Start(ctx context.Context, stopper *stop.Stopper, sv *settings.Values) error {
	return stopper.RunAsyncTask(ctx, "clustermetrics-writer", func(ctx context.Context) {
		interval := FlushInterval.Get(sv)
		timer := time.NewTimer(jitter(interval))
		defer timer.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-stopper.ShouldQuiesce():
				return
			case <-timer.C:
				if !FlushEnabled.Get(sv) {
					timer.Reset(jitter(interval))
					continue
				}
				w.Flush(ctx)
				interval = FlushInterval.Get(sv)
				timer.Reset(jitter(interval))
			}
		}
	})
}
