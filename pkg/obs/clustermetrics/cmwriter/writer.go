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
	// Delete removes the given metrics from storage, identified by
	// their name and labels. Value and type are ignored.
	Delete(ctx context.Context, metrics []cmmetrics.WritableMetric) error
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

// collectDirty returns dirty metrics to write, metrics to delete,
// items to reset after a fully successful flush, and scalar metric
// names to remove from the tracked map after a successful delete.
func (w *Writer) collectDirty(
	ctx context.Context,
) (
	toWrite []cmmetrics.WritableMetric,
	toDelete []cmmetrics.WritableMetric,
	toReset []resettable,
	toUntrack []string,
) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for name, tracked := range w.mu.tracked {
		tracked.Inspect(func(val interface{}) {
			switch m := val.(type) {
			case cmmetrics.Metric:
				if m.IsDeleted() {
					toDelete = append(toDelete, m)
					toUntrack = append(toUntrack, name)
				} else if m.IsDirty() {
					toWrite = append(toWrite, m)
					toReset = append(toReset, m)
				}
			case cmmetrics.MetricVec:
				// Collect deleted children first.
				toDelete = append(toDelete, m.CollectDeleted()...)
				// Then collect dirty children for writing.
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
	return toWrite, toDelete, toReset, toUntrack
}

// Flush triggers an immediate flush of all registered metrics to the store.
// Only metrics that have changed since the last flush are written. Deleted
// metrics are removed from the store before writes so that a delete-then-
// re-add in the same cycle produces the correct final state.
//
// Scalar metrics (Metric) are collected when their dirty flag is set.
// Vector metrics (MetricVec) yield read-only snapshots of all children;
// the parent vec is cleared after a successful write so only values set
// since the last flush appear next time.
//
// Deleted scalar metrics are removed from the tracked map after a
// successful flush. Their deleted flag remains set so that any
// subsequent mutations on the metric instance are silently ignored.
//
// If either the delete or write call fails, the flush returns early
// without resetting any state. Scalar metrics retry naturally on the
// next flush because their dirty/deleted flags remain set. Vec
// deletions use drain semantics and may be lost on failure; this is
// acceptable because cluster metrics have an implicit TTL that will
// eventually clean up stale rows.
func (w *Writer) Flush(ctx context.Context) {
	startTime := timeutil.Now()
	toWrite, toDelete, toReset, toUntrack := w.collectDirty(ctx)

	if len(toWrite) == 0 && len(toDelete) == 0 {
		return
	}

	defer func() {
		elapsed := timeutil.Since(startTime)
		w.metrics.FlushLatency.Update(elapsed.Nanoseconds())
		w.metrics.FlushCount.Inc(1)
	}()

	// Execute deletes before writes so that a vec child deleted and
	// re-added in the same flush cycle ends up with the new value
	// (UPSERT after DELETE).
	if err := w.store.Delete(ctx, toDelete); err != nil {
		w.metrics.FlushErrors.Inc(1)
		log.Ops.Warningf(ctx, "failed to delete cluster metrics: %v", err)
		return
	}

	if err := w.store.Write(ctx, toWrite); err != nil {
		w.metrics.FlushErrors.Inc(1)
		log.Ops.Warningf(ctx, "failed to flush cluster metrics: %v", err)
		return
	}

	// Both operations succeeded — reset all state and record counts.
	w.metrics.MetricsDeleted.Inc(int64(len(toDelete)))
	w.metrics.MetricsWritten.Inc(int64(len(toWrite)))
	for _, r := range toReset {
		r.Reset()
	}

	// Remove deleted scalar metrics from the tracked map. The deleted
	// flag on each metric remains set so future mutations are no-ops.
	if len(toUntrack) > 0 {
		w.untrack(toUntrack)
	}
}

// untrack removes the named metrics from the tracked map.
func (w *Writer) untrack(names []string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, name := range names {
		delete(w.mu.tracked, name)
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
