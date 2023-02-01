// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metric

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/codahale/hdrhistogram"
	prometheusgo "github.com/prometheus/client_model/go"
)

const (
	// HdrHistogramMaxLatency is the maximum value tracked in latency histograms. Higher
	// values will be recorded as this value instead.
	HdrHistogramMaxLatency = 10 * time.Second

	// The number of histograms to keep in rolling window.
	hdrHistogramHistWrapNum = 2 // TestSampleInterval is passed to histograms during tests which don't
)

// A HdrHistogram collects observed values by keeping bucketed counts. For
// convenience, internally two sets of buckets are kept: A cumulative set (i.e.
// data is never evicted) and a windowed set (which keeps only recently
// collected samples).
//
// Top-level methods generally apply to the cumulative buckets; the windowed
// variant is exposed through the Windowed method.
//
// TODO(#96357): remove HdrHistogram model entirely once the Prometheus
// backed histogram and its bucket boundaries have been reliably proven in
// production.
type HdrHistogram struct {
	Metadata
	maxVal int64
	mu     struct {
		syncutil.Mutex
		cumulative *hdrhistogram.Histogram
		*tickHelper
		sliding *hdrhistogram.WindowedHistogram
	}
}

var _ IHistogram = &HdrHistogram{}
var _ PrometheusExportable = &HdrHistogram{}
var _ Iterable = &HdrHistogram{}

// NewHdrHistogram initializes a given Histogram. The contained windowed histogram
// rotates every 'duration'; both the windowed and the cumulative histogram
// track nonnegative values up to 'maxVal' with 'sigFigs' decimal points of
// precision.
func NewHdrHistogram(
	metadata Metadata, duration time.Duration, maxVal int64, sigFigs int,
) *HdrHistogram {
	h := &HdrHistogram{
		Metadata: metadata,
		maxVal:   maxVal,
	}
	wHist := hdrhistogram.NewWindowed(hdrHistogramHistWrapNum, 0, maxVal, sigFigs)
	h.mu.cumulative = hdrhistogram.New(0, maxVal, sigFigs)
	h.mu.sliding = wHist
	h.mu.tickHelper = &tickHelper{
		nextT:        now(),
		tickInterval: duration / hdrHistogramHistWrapNum,
		onTick: func() {
			wHist.Rotate()
		},
	}
	return h
}

// NewHdrLatency is a convenience function which returns a histogram with
// suitable defaults for latency tracking. Values are expressed in ns,
// are truncated into the interval [0, HdrHistogramMaxLatency] and are recorded
// with one digit of precision (i.e. errors of <10ms at 100ms, <6s at 60s).
//
// The windowed portion of the Histogram retains values for approximately
// histogramWindow.
func NewHdrLatency(metadata Metadata, histogramWindow time.Duration) *HdrHistogram {
	return NewHdrHistogram(
		metadata, histogramWindow, HdrHistogramMaxLatency.Nanoseconds(), 1,
	)
}

// RecordValue adds the given value to the histogram. Recording a value in
// excess of the configured maximum value for that histogram results in
// recording the maximum value instead.
func (h *HdrHistogram) RecordValue(v int64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.mu.sliding.Current.RecordValue(v) != nil {
		_ = h.mu.sliding.Current.RecordValue(h.maxVal)
	}
	if h.mu.cumulative.RecordValue(v) != nil {
		_ = h.mu.cumulative.RecordValue(h.maxVal)
	}
}

// TotalCount returns the (cumulative) number of samples.
func (h *HdrHistogram) TotalCount() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.mu.cumulative.TotalCount()
}

// Min returns the minimum.
func (h *HdrHistogram) Min() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.mu.cumulative.Min()
}

// Inspect calls the closure with the empty string and the receiver.
func (h *HdrHistogram) Inspect(f func(interface{})) {
	h.mu.Lock()
	maybeTick(h.mu.tickHelper)
	h.mu.Unlock()
	f(h)
}

// GetType returns the prometheus type enum for this metric.
func (h *HdrHistogram) GetType() *prometheusgo.MetricType {
	return prometheusgo.MetricType_HISTOGRAM.Enum()
}

// ToPrometheusMetric returns a filled-in prometheus metric of the right type.
func (h *HdrHistogram) ToPrometheusMetric() *prometheusgo.Metric {
	hist := &prometheusgo.Histogram{}

	h.mu.Lock()
	maybeTick(h.mu.tickHelper)
	bars := h.mu.cumulative.Distribution()
	hist.Bucket = make([]*prometheusgo.Bucket, 0, len(bars))

	var cumCount uint64
	var sum float64
	for _, bar := range bars {
		if bar.Count == 0 {
			// No need to expose trivial buckets.
			continue
		}
		upperBound := float64(bar.To)
		sum += upperBound * float64(bar.Count)

		cumCount += uint64(bar.Count)
		curCumCount := cumCount // need a new alloc thanks to bad proto code

		hist.Bucket = append(hist.Bucket, &prometheusgo.Bucket{
			CumulativeCount: &curCumCount,
			UpperBound:      &upperBound,
		})
	}
	hist.SampleCount = &cumCount
	hist.SampleSum = &sum // can do better here; we approximate in the loop
	h.mu.Unlock()

	return &prometheusgo.Metric{
		Histogram: hist,
	}
}

// TotalCountWindowed implements the WindowedHistogram interface.
func (h *HdrHistogram) TotalCountWindowed() int64 {
	return int64(h.ToPrometheusMetricWindowed().Histogram.GetSampleCount())
}

// TotalSumWindowed implements the WindowedHistogram interface.
func (h *HdrHistogram) TotalSumWindowed() float64 {
	return h.ToPrometheusMetricWindowed().Histogram.GetSampleSum()
}

func (h *HdrHistogram) toPrometheusMetricWindowedLocked() *prometheusgo.Metric {
	hist := &prometheusgo.Histogram{}

	maybeTick(h.mu.tickHelper)
	bars := h.mu.sliding.Current.Distribution()
	hist.Bucket = make([]*prometheusgo.Bucket, 0, len(bars))

	var cumCount uint64
	var sum float64
	for _, bar := range bars {
		if bar.Count == 0 {
			// No need to expose trivial buckets.
			continue
		}
		upperBound := float64(bar.To)
		sum += upperBound * float64(bar.Count)

		cumCount += uint64(bar.Count)
		curCumCount := cumCount // need a new alloc thanks to bad proto code

		hist.Bucket = append(hist.Bucket, &prometheusgo.Bucket{
			CumulativeCount: &curCumCount,
			UpperBound:      &upperBound,
		})
	}
	hist.SampleCount = &cumCount
	hist.SampleSum = &sum // can do better here; we approximate in the loop

	return &prometheusgo.Metric{
		Histogram: hist,
	}
}

// ToPrometheusMetricWindowed returns a filled-in prometheus metric of the right type.
func (h *HdrHistogram) ToPrometheusMetricWindowed() *prometheusgo.Metric {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.toPrometheusMetricWindowedLocked()
}

// GetMetadata returns the metric's metadata including the Prometheus
// MetricType.
func (h *HdrHistogram) GetMetadata() Metadata {
	baseMetadata := h.Metadata
	baseMetadata.MetricType = prometheusgo.MetricType_HISTOGRAM
	return baseMetadata
}

func (h *HdrHistogram) ValueAtQuantileWindowed(q float64) float64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	return ValueAtQuantileWindowed(h.toPrometheusMetricWindowedLocked().Histogram, q)
}

func (h *HdrHistogram) Mean() float64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.mu.cumulative.Mean()
}

func (h *HdrHistogram) TotalSum() float64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.ToPrometheusMetric().GetSummary().GetSampleSum()
}
