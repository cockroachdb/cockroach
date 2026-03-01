// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmwriter

import (
	"context"
	"math/rand"
	"reflect"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/obs/clustermetrics/cmmetrics"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// MetricStore abstracts the storage backend for cluster metrics.
type MetricStore interface {
	// Write writes the given metrics to storage.
	Write(ctx context.Context, metrics []cmmetrics.WritableMetric) error
}

// Writer manages registered cluster metrics and periodically flushes their
// values to a MetricStore.
type Writer struct {
	store   MetricStore
	metrics *WriterMetrics
	mu      struct {
		syncutil.Mutex
		tracked map[string]metric.Iterable
	}
}

// NewWriter creates a new Writer backed by an SQLStore.
func NewWriter(db isql.DB, sqlIDContainer *base.SQLIDContainer, st *cluster.Settings) *Writer {
	store := newSQLStore(db, sqlIDContainer, st)
	return NewWriterWithStore(store, NewWriterMetrics())
}

// NewWriterWithStore creates a new Writer with the given MetricStore.
// This is useful in tests where a custom store implementation is needed.
func NewWriterWithStore(store MetricStore, metrics *WriterMetrics) *Writer {
	if metrics == nil {
		metrics = NewWriterMetrics()
	}
	w := &Writer{
		store:   store,
		metrics: metrics,
	}
	w.mu.tracked = make(map[string]metric.Iterable)
	return w
}

// AddMetric registers a metric with the Writer. Overwrites any
// previously registered metric with the same name. Only metrics
// that implement ClusterMetric are accepted; non-cluster metrics
// are rejected with a fatal error in test builds and a warning
// in production.
func (w *Writer) AddMetric(m metric.Iterable) {
	if _, ok := m.(cmmetrics.ClusterMetric); !ok {
		name := m.GetName(false /* useStaticLabels */)
		if buildutil.CrdbTestBuild {
			log.Dev.Fatalf(context.TODO(),
				"non-cluster metric %s (%T) cannot be added to the cluster metrics writer",
				name, m)
		}
		log.Dev.Warningf(context.TODO(),
			"skipping non-cluster metric %s (%T)", name, m)
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.mu.tracked[m.GetName(false /* useStaticLabels */)] = m
}

// AddMetricStruct registers all metric.Iterable fields in the given
// struct with the Writer.
func (w *Writer) AddMetricStruct(s interface{}) {
	v := reflect.ValueOf(s)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if !field.CanInterface() {
			continue
		}
		if m, ok := field.Interface().(metric.Iterable); ok {
			w.AddMetric(m)
		} else if ms, ok := field.Interface().(metric.Struct); ok {
			w.AddMetricStruct(ms)
		}
	}
}

// Metrics returns the operational metrics for the Writer. These should be
// registered with a prometheus-exportable registry.
func (w *Writer) Metrics() *WriterMetrics {
	return w.metrics
}

// resettable is satisfied by both scalar Metric and MetricVec types.
type resettable interface {
	Reset()
}

// collectDirty returns the dirty metrics and their resetters under the lock.
func (w *Writer) collectDirty(
	ctx context.Context,
) (toWrite []cmmetrics.WritableMetric, toReset []resettable) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, tracked := range w.mu.tracked {
		tracked.Inspect(func(val interface{}) {
			switch m := val.(type) {
			case cmmetrics.Metric:
				if m.IsDirty() {
					toWrite = append(toWrite, m)
					toReset = append(toReset, m)
				}
			case cmmetrics.MetricVec:
				shouldReset := false
				m.Each(func(snap cmmetrics.WritableMetric) {
					toWrite = append(toWrite, snap)
					shouldReset = true
				})
				if shouldReset {
					toReset = append(toReset, m)
				}
			default:
				log.Ops.Warningf(ctx,
					"cannot flush non-cluster-metric of type %T", val)
			}
		})
	}
	return toWrite, toReset
}

// Flush triggers an immediate flush of all registered metrics to the store.
// Only metrics that have changed since the last flush are written.
//
// Scalar metrics (Metric) are collected when their dirty flag is set.
// Vector metrics (MetricVec) yield read-only snapshots of all children;
// the parent vec is cleared after a successful write so only values set
// since the last flush appear next time.
func (w *Writer) Flush(ctx context.Context) {
	startTime := timeutil.Now()
	toWrite, toReset := w.collectDirty(ctx)

	if len(toWrite) == 0 {
		return
	}

	// Write the metrics to the downstream store.
	err := w.store.Write(ctx, toWrite)

	// Record meta metrics.
	elapsed := timeutil.Since(startTime)
	w.metrics.FlushLatency.Update(elapsed.Nanoseconds())
	w.metrics.FlushCount.Inc(1)
	if err != nil {
		w.metrics.FlushErrors.Inc(1)
		log.Ops.Warningf(ctx, "failed to flush cluster metrics: %v", err)
		return
	}
	w.metrics.MetricsWritten.Inc(int64(len(toWrite)))

	// Reset metrics after successful write.
	for _, r := range toReset {
		r.Reset()
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
