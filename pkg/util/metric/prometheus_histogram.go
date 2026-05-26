// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metric

import (
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric/tick"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/prometheus/client_golang/prometheus"
	prometheusgo "github.com/prometheus/client_model/go"
)

var _ PrometheusExportable = (*prometheusHistogram)(nil)
var _ WindowedHistogram = (*prometheusHistogram)(nil)
var _ CumulativeHistogram = (*prometheusHistogram)(nil)
var _ IHistogram = (*prometheusHistogram)(nil)

// prometheusHistogram is a prometheus-backed histogram. It collects observed
// values by keeping bucketed counts. For convenience, internally two sets of
// buckets are kept: a cumulative set (i.e. data is never evicted) and a
// windowed set (which keeps only recently collected samples).
//
// This type exists as a fallback for when the goodhistogram-backed Histogram
// is disabled via the COCKROACH_ENABLE_PROMETHEUS_NATIVE_HISTOGRAMS env var.
type prometheusHistogram struct {
	Metadata
	cum prometheus.HistogramInternal

	windowed struct {
		*tick.Ticker
		syncutil.Mutex
		prev, cur atomic.Value
	}
}

// newPrometheusHistogram creates a prometheus-backed histogram suitable for
// recording any kind of quantity. Bucket boundaries are derived from the
// provided BucketConfig (preferred) or explicit Buckets slice.
func newPrometheusHistogram(
	meta Metadata, duration time.Duration, buckets []float64, bucketConfig staticBucketConfig,
) *prometheusHistogram {
	// If no buckets are provided, generate buckets from bucket configuration.
	if buckets == nil && bucketConfig.count != 0 {
		buckets = bucketConfig.GetBucketsFromBucketConfig()
	}
	opts := prometheus.HistogramOpts{
		Buckets: buckets,
	}
	cum := prometheus.NewHistogram(opts)
	h := &prometheusHistogram{
		Metadata: meta,
		cum:      cum,
	}
	h.windowed.Ticker = tick.NewTicker(
		now(),
		duration/WindowedHistogramWrapNum,
		func() {
			h.windowed.Lock()
			defer h.windowed.Unlock()
			if h.windowed.cur.Load() != nil {
				h.windowed.prev.Store(h.windowed.cur.Load())
			}
			h.windowed.cur.Store(prometheus.NewHistogram(opts))
		})
	h.windowed.Ticker.OnTick()
	return h
}

// NextTick returns the next tick timestamp of the underlying tick.Ticker.
func (h *prometheusHistogram) NextTick() time.Time {
	return h.windowed.NextTick()
}

// Tick triggers a tick of this histogram.
func (h *prometheusHistogram) Tick() {
	h.windowed.Tick()
}

// RecordValue adds the given value to the histogram.
func (h *prometheusHistogram) RecordValue(n int64) {
	v := float64(n)
	b := h.cum.FindBucket(v)
	h.cum.ObserveInternal(v, b)
	h.windowed.cur.Load().(prometheus.HistogramInternal).ObserveInternal(v, b)
}

// GetType returns the prometheus type enum for this metric.
func (h *prometheusHistogram) GetType() *prometheusgo.MetricType {
	return prometheusgo.MetricType_HISTOGRAM.Enum()
}

// ToPrometheusMetric returns a filled-in prometheus metric of the right
// type.
func (h *prometheusHistogram) ToPrometheusMetric() *prometheusgo.Metric {
	m := &prometheusgo.Metric{}
	if err := h.cum.Write(m); err != nil {
		panic(err)
	}
	return m
}

func (h *prometheusHistogram) CumulativeSnapshot() HistogramSnapshot {
	return MakeHistogramSnapshot(h.ToPrometheusMetric().Histogram)
}

func (h *prometheusHistogram) WindowedSnapshot() HistogramSnapshot {
	h.windowed.Lock()
	defer h.windowed.Unlock()
	cur := h.windowed.cur.Load().(prometheus.Histogram)
	prev := h.windowed.prev.Load()

	curMetric := &prometheusgo.Metric{}
	if err := cur.Write(curMetric); err != nil {
		panic(err)
	}
	if prev != nil {
		prevMetric := &prometheusgo.Metric{}
		if err := prev.(prometheus.Histogram).Write(prevMetric); err != nil {
			panic(err)
		}
		MergeWindowedHistogram(curMetric.Histogram, prevMetric.Histogram)
	}
	return MakeHistogramSnapshot(curMetric.Histogram)
}

// GetMetadata returns the metric's metadata including the Prometheus
// MetricType.
func (h *prometheusHistogram) GetMetadata() Metadata {
	return h.Metadata
}

// Inspect calls the closure.
func (h *prometheusHistogram) Inspect(f func(interface{})) {
	func() {
		tick.MaybeTick(&h.windowed)
	}()
	f(h)
}
