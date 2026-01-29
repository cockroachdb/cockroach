// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schedulerlatency

import (
	"math"
	"runtime/metrics"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/gogo/protobuf/proto"
	prometheusgo "github.com/prometheus/client_model/go"
)

// runtimeHistogram exports Go runtime scheduler latency metrics to both Prometheus
// and CockroachDB's internal time-series database (TSDB).
//
// The code here is adapted from:
// [1]: github.com/prometheus/client_golang/blob/5b7e8b2e/prometheus/go_collector_latest.go
// [2]: github.com/prometheus/client_golang/blob/5b7e8b2e/prometheus/internal/go_runtime_metrics.go
type runtimeHistogram struct {
	metric.Metadata
	mu struct {
		syncutil.Mutex

		// buckets holds N bucket boundaries that define N-1 histogram bins.
		// Each boundary is an inclusive lower bound, following runtime/metrics
		// conventions. Example: [0, 1ms, 10ms, +Inf] defines 3 bins that count
		// observations in ranges [0, 1ms), [1ms, 10ms), [10ms, +Inf).
		//
		// INVARIANT: len(windowedCounts) == len(cumCounts) == len(buckets)-1.
		buckets []float64

		// windowedCounts[i] holds the count for the i-th bin (range [buckets[i],
		// buckets[i+1])). Contains only the most recent update() (~10s of data).
		// Used by TSDB for percentile calculations where recent data matters.
		windowedCounts []uint64

		// cumCounts[i] holds the cumulative count for the i-th bin since process
		// start. Values only increase. Used by Prometheus which requires
		// monotonically increasing values for rate() and histogram_quantile().
		cumCounts []uint64
	}
	// mult converts bucket boundaries from Go runtime units (seconds) to CRDB
	// units (nanoseconds). As of Go 1.19, runtime histograms are always in
	// seconds.
	mult float64
}

var _ metric.Iterable = &runtimeHistogram{}
var _ metric.PrometheusExportable = &runtimeHistogram{}
var _ metric.WindowedHistogram = (*runtimeHistogram)(nil)
var _ metric.CumulativeHistogram = (*runtimeHistogram)(nil)

// newRuntimeHistogram creates a histogram with the given metadata configured
// with the given buckets. The buckets must be a strict subset of what this
// histogram is updated with and follow the same conventions as those in
// runtime/metrics.
func newRuntimeHistogram(metadata metric.Metadata, buckets []float64) *runtimeHistogram {
	// We need to remove -Inf values. runtime/metrics keeps them around.
	// But -Inf bucket should not be allowed for prometheus histograms.
	if buckets[0] == math.Inf(-1) {
		buckets = buckets[1:]
	}
	metadata.MetricType = prometheusgo.MetricType_HISTOGRAM
	h := &runtimeHistogram{
		Metadata: metadata,
		// Go runtime histograms as of go1.19 are always in seconds whereas
		// CRDB's histograms are in nanoseconds. Hardcode the conversion factor
		// between the two, use it when translating to the prometheus exportable
		// form (also used when writing to CRDB's internal TSDB).
		mult: float64(time.Second.Nanoseconds()),
	}
	h.mu.buckets = buckets
	// Because buckets follows runtime/metrics conventions, there's
	// one more value in the buckets list than there are buckets represented,
	// because in runtime/metrics, the bucket values represent boundaries,
	// and non-Inf boundaries are inclusive lower bounds for that bucket.
	h.mu.windowedCounts = make([]uint64, len(buckets)-1)
	h.mu.cumCounts = make([]uint64, len(buckets)-1)
	return h
}

// update replaces windowedCounts and adds to cumCounts. src is the runtime's
// histogram with fine-grained buckets containing delta counts accumulated since
// the last getAndClearLastStatsHistogram() call (~10s ago).
func (h *runtimeHistogram) update(src *metrics.Float64Histogram) {
	h.mu.Lock()
	defer h.mu.Unlock()
	counts, buckets := src.Counts, src.Buckets

	for i := range h.mu.windowedCounts {
		h.mu.windowedCounts[i] = 0
	}

	// Map runtime's fine-grained buckets to our buckets by summing counts.
	//
	// Example: runtime boundaries [0, 1, 2, 5, 10] with our boundaries [0, 5, 10]
	//   runtime bins: [0,1)=2  [1,2)=3  [2,5)=1  [5,10)=4
	//   our bins:     [0,5)=2+3+1=6     [5,10)=4
	dstIdx := 0
	for srcIdx, count := range counts {
		h.mu.windowedCounts[dstIdx] += count
		h.mu.cumCounts[dstIdx] += count
		// Advance to next destination bin when boundaries align.
		if buckets[srcIdx+1] == h.mu.buckets[dstIdx+1] {
			dstIdx++
		}
	}
}

// writeCountsToMetricLocked converts internal counts into Prometheus histogram format.
func writeCountsToMetricLocked(
	out *prometheusgo.Metric, counts []uint64, buckets []float64, mult float64,
) {
	sum := float64(0)
	dtoBuckets := make([]*prometheusgo.Bucket, 0, len(counts))
	totalCount := uint64(0)

	// Prometheus requires cumulative counts (each bucket includes all smaller buckets),
	// inclusive upper bounds, and implicit +Inf bucket.
	for i, count := range counts {
		totalCount += count
		if count != 0 {
			// N.B. this computed sum is an underestimate since we're using the
			// lower bound of the bucket.
			sum += buckets[i] * mult * float64(count)
		}

		// Skip the +Inf bucket, but only for the bucket list. It must still
		// count for sum and totalCount.
		if math.IsInf(buckets[i+1]*mult, 1) {
			break
		}
		// Float64Histogram's upper bound is exclusive, so make it inclusive by
		// obtaining the next float64 value down, in order.
		upperBound := math.Nextafter(buckets[i+1], buckets[i]) * mult
		dtoBuckets = append(dtoBuckets, &prometheusgo.Bucket{
			CumulativeCount: proto.Uint64(totalCount),
			UpperBound:      proto.Float64(upperBound),
		})
	}
	out.Histogram = &prometheusgo.Histogram{
		Bucket:      dtoBuckets,
		SampleCount: proto.Uint64(totalCount),
		SampleSum:   proto.Float64(sum),
	}
}

// GetType is part of the PrometheusExportable interface.
func (h *runtimeHistogram) GetType() *prometheusgo.MetricType {
	return prometheusgo.MetricType_HISTOGRAM.Enum()
}

// ToPrometheusMetric is part of the PrometheusExportable interface.
//
// NB: The returned counts and sum are monotonically increasing across calls.
// This is required for Prometheus's rate() and histogram_quantile() to work
// correctly.
func (h *runtimeHistogram) ToPrometheusMetric() *prometheusgo.Metric {
	h.mu.Lock()
	defer h.mu.Unlock()
	m := &prometheusgo.Metric{}
	writeCountsToMetricLocked(m, h.mu.cumCounts, h.mu.buckets, h.mult)
	return m
}

// CumulativeSnapshot is part of the metric.CumulativeHistogram interface.
// Returns cumulative counts, used by timeseries for count and sum metrics.
//
// NB: Same as ToPrometheusMetric - values are monotonically increasing.
func (h *runtimeHistogram) CumulativeSnapshot() metric.HistogramSnapshot {
	return metric.MakeHistogramSnapshot(h.ToPrometheusMetric().Histogram)
}

// WindowedSnapshot is part of the metric.WindowedHistogram interface. Returns
// only the most recent update's counts, used by timeseries for percentiles
// (p50, p99) where we want the recent distribution rather than all-time.
func (h *runtimeHistogram) WindowedSnapshot() metric.HistogramSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	m := &prometheusgo.Metric{}
	writeCountsToMetricLocked(m, h.mu.windowedCounts, h.mu.buckets, h.mult)
	return metric.MakeHistogramSnapshot(m.Histogram)
}

// GetMetadata is part of the PrometheusExportable interface.
func (h *runtimeHistogram) GetMetadata() metric.Metadata {
	return h.Metadata
}

// Inspect is part of the Iterable interface.
func (h *runtimeHistogram) Inspect(f func(interface{})) { f(h) }

// reBucketExpAndTrim takes a list of bucket boundaries (lower bound inclusive)
// and down samples the buckets to those a multiple of base apart. The end
// result is a roughly exponential (in many cases, perfectly exponential)
// bucketing scheme. It also trims the bucket range to the specified min and max
// values -- everything outside the range is merged into (-Inf, ..] and [..,
// +Inf) buckets. The following example shows how it works, lifted from
// testdata/histogram_buckets.
//
//		rebucket base=10 min=0ns max=100000h
//		----
//		bucket[  0] width=0s                 boundary=[-Inf, 0s)
//	    bucket[  1] width=1ns                boundary=[0s, 1ns)
//	    bucket[  2] width=9ns                boundary=[1ns, 10ns)
//	    bucket[  3] width=90ns               boundary=[10ns, 100ns)
//	    bucket[  4] width=924ns              boundary=[100ns, 1.024µs)
//	    bucket[  5] width=9.216µs            boundary=[1.024µs, 10.24µs)
//	    bucket[  6] width=92.16µs            boundary=[10.24µs, 102.4µs)
//	    bucket[  7] width=946.176µs          boundary=[102.4µs, 1.048576ms)
//	    bucket[  8] width=9.437184ms         boundary=[1.048576ms, 10.48576ms)
func reBucketExpAndTrim(buckets []float64, base, min, max float64) []float64 {
	// Re-bucket as powers of the given base.
	b := reBucketExp(buckets, base)

	// Merge all buckets greater than the max value into the +Inf bucket.
	for i := range b {
		if i == 0 {
			continue
		}
		if b[i-1] <= max {
			continue
		}

		// We're looking at the boundary after the first time we've crossed the
		// max limit. Since we expect recordings near the max value, we don't
		// want that bucket to end at +Inf, so we merge the bucket after.
		b[i] = math.Inf(1)
		b = b[:i+1]
		break
	}

	// Merge all buckets less than the min value into the -Inf bucket.
	j := 0
	for i := range b {
		if b[i] > min {
			j = i
			break
		}
	}
	// b[j] > min and is the lower-bound of the j-th bucket. The min must be
	// contained in the (j-1)-th bucket. We want to merge 0th bucket
	// until the (j-2)-th one.
	if j <= 2 {
		// Nothing to do (we either have one or no buckets to merge together).
	} else {
		// We want trim the bucket list to start at (j-2)-th bucket, so just
		// have one bucket before the one containing the min.
		b = b[j-2:]
		// b[0] now refers the lower bound of what was previously the (j-2)-th
		// bucket. We make it start at -Inf.
		b[0] = math.Inf(-1)
	}

	return b
}

// reBucketExp reduces bucket boundaries to exponential spacing, keeping only
// boundaries that are roughly 'base' times apart. For example, with base=10:
// [0, 1, 2, ..., 10, 20, ..., 100] → [0, 1, 10, 100].
func reBucketExp(buckets []float64, base float64) []float64 {
	bucket := buckets[0]
	var newBuckets []float64
	// We may see -Inf here, in which case, add it and continue the rebucketing
	// scheme from the next one it since we risk producing NaNs otherwise. We
	// need to preserve -Inf values to maintain runtime/metrics conventions
	if bucket == math.Inf(-1) {
		newBuckets = append(newBuckets, bucket)
		buckets = buckets[1:]
		bucket = buckets[0]
	}

	// From now on, bucket should always have a non-Inf value because Infs are
	// only ever at the ends of the bucket lists, so arithmetic operations on it
	// are non-NaN.
	for i := 1; i < len(buckets); i++ {
		// bucket is the lower bound of the lowest bucket that has not been
		// added to newBuckets. We will add it to newBuckets, but we wait to add
		// it until we find the next bucket that is >= bucket*base.

		if bucket >= 0 && buckets[i] < bucket*base {
			// The next bucket we want to include is at least bucket*base.
			continue
		} else if bucket < 0 && buckets[i] < bucket/base {
			// In this case the bucket we're targeting is negative, and since
			// we're ascending through buckets here, we need to divide to get
			// closer to zero exponentially.
			continue
		}
		newBuckets = append(newBuckets, bucket)
		bucket = buckets[i]
	}

	// The +Inf bucket will always be the last one, and we'll always
	// end up including it here.
	return append(newBuckets, bucket)
}
