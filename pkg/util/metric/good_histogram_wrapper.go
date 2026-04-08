// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metric

import (
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric/goodhistogram"
	"github.com/cockroachdb/cockroach/pkg/util/metric/tick"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	prometheusgo "github.com/prometheus/client_model/go"
)

var _ PrometheusExportable = (*GoodHistogramWrapper)(nil)
var _ WindowedHistogram = (*GoodHistogramWrapper)(nil)
var _ CumulativeHistogram = (*GoodHistogramWrapper)(nil)
var _ IHistogram = (*GoodHistogramWrapper)(nil)

// GoodHistogramWrapper adapts a goodhistogram.Histogram to the IHistogram
// interface used by CockroachDB's metric system. It maintains a cumulative
// histogram (for Prometheus export and count/sum TSDB metrics) and a
// windowed pair (prev/cur) rotated by a tick.Ticker (for percentile TSDB
// metrics).
//
// Recording is lock-free: RecordValue atomically increments counters in
// both the cumulative and current-window histograms. The windowed lock is
// only held during tick rotation and WindowedSnapshot.
type GoodHistogramWrapper struct {
	Metadata
	config goodhistogram.Config
	cum    *goodhistogram.Histogram // cumulative, never reset

	windowed struct {
		*tick.Ticker
		syncutil.Mutex
		prev, cur atomic.Value // each stores *goodhistogram.Histogram
	}
}

// newGoodHistogram creates a GoodHistogramWrapper with tick-based window
// rotation. The rotation interval is duration/WindowedHistogramWrapNum,
// matching the pattern used by the Prometheus-backed Histogram.
func newGoodHistogram(
	meta Metadata, duration time.Duration, config goodhistogram.Config,
) *GoodHistogramWrapper {
	h := &GoodHistogramWrapper{
		Metadata: meta,
		config:   config,
		cum:      goodhistogram.NewFromConfig(&config),
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
			h.windowed.cur.Store(goodhistogram.NewFromConfig(&config))
		})
	h.windowed.Ticker.OnTick()
	return h
}

// RecordValue adds the given value to both the cumulative and
// current-window histograms. This is the hot path — lock-free, O(1), no
// allocations.
func (h *GoodHistogramWrapper) RecordValue(n int64) {
	h.cum.Record(n)
	h.windowed.cur.Load().(*goodhistogram.Histogram).Record(n)
}

// NextTick returns the next tick timestamp of the underlying tick.Ticker.
func (h *GoodHistogramWrapper) NextTick() time.Time {
	return h.windowed.NextTick()
}

// Tick triggers a window rotation, regardless of whether the next tick
// interval has elapsed. Used by aggmetric.AggHistogram to synchronize
// rotation across parent and children.
func (h *GoodHistogramWrapper) Tick() {
	h.windowed.Tick()
}

// GetType returns the Prometheus metric type enum.
func (h *GoodHistogramWrapper) GetType() *prometheusgo.MetricType {
	return prometheusgo.MetricType_HISTOGRAM.Enum()
}

// ToPrometheusMetric returns the cumulative histogram as a Prometheus
// metric, suitable for scraping.
func (h *GoodHistogramWrapper) ToPrometheusMetric() *prometheusgo.Metric {
	snap := h.cum.Snapshot()
	ph := snap.ToPrometheusHistogram()
	return &prometheusgo.Metric{Histogram: ph}
}

// CumulativeSnapshot returns a point-in-time snapshot of the cumulative
// (all-time) histogram. Used for count and sum TSDB metrics.
func (h *GoodHistogramWrapper) CumulativeSnapshot() HistogramSnapshot {
	snap := h.cum.Snapshot()
	return &snap
}

// WindowedSnapshot returns a merged snapshot of the prev and cur windows.
// Used for percentile TSDB metrics (p50, p99, etc.).
func (h *GoodHistogramWrapper) WindowedSnapshot() HistogramSnapshot {
	h.windowed.Lock()
	defer h.windowed.Unlock()
	cur, ok := h.windowed.cur.Load().(*goodhistogram.Histogram)
	if !ok || cur == nil {
		return EmptyHistogramSnapshot()
	}
	curSnap := cur.Snapshot()

	prev := h.windowed.prev.Load()
	if prev != nil {
		if prevHist, ok := prev.(*goodhistogram.Histogram); ok {
			prevSnap := prevHist.Snapshot()
			merged := curSnap.Merge(&prevSnap)
			return &merged
		}
	}
	return &curSnap
}

// GetMetadata returns the metric's metadata including the Prometheus
// MetricType.
func (h *GoodHistogramWrapper) GetMetadata() Metadata {
	return h.Metadata
}

// Inspect calls the closure with the histogram, ticking first if needed.
func (h *GoodHistogramWrapper) Inspect(f func(interface{})) {
	func() {
		tick.MaybeTick(&h.windowed)
	}()
	f(h)
}
