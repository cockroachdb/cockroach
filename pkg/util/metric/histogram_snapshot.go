// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metric

import (
	"math"
	"sort"

	prometheusgo "github.com/prometheus/client_model/go"
)

// HistogramSnapshot represents a point-in-time snapshot of a Histogram metric, against
// which calculations like Mean, Total, and Quantiles can be calculated.
//
// This allows a consistent dataset to be used across calculations to avoid inaccuracies
// from one calculation to another, and isolates the supported operations to a single
// implementation.
type HistogramSnapshot struct {
	h *prometheusgo.Histogram
}

// MakeHistogramSnapshot returns a new HistogramSnapshot instance, backed by the provided
// Histogram.
func MakeHistogramSnapshot(h *prometheusgo.Histogram) HistogramSnapshot {
	return HistogramSnapshot{
		h: h,
	}
}

// ValueAtQuantile takes a quantile value [0,100] and returns the interpolated value
// at that quantile for this HistogramSnapshot.
func (hs HistogramSnapshot) ValueAtQuantile(q float64) float64 {
	histogram := hs.h
	buckets := histogram.Bucket
	n := float64(histogram.GetSampleCount())
	if n == 0 {
		return 0
	}

	// NB: The 0.5 is added for rounding purposes; it helps in cases where
	// SampleCount is small.
	rank := uint64(((q / 100) * n) + 0.5)

	// Since we are missing the +Inf bucket, CumulativeCounts may never exceed
	// rank. By omitting the highest bucket we have from the search, the failed
	// search will land on that last bucket and we don't have to do any special
	// checks regarding landing on a non-existent bucket.
	b := sort.Search(len(buckets)-1, func(i int) bool { return *buckets[i].CumulativeCount >= rank })

	var (
		bucketStart float64 // defaults to 0, which we assume is the lower bound of the smallest bucket
		bucketEnd   = *buckets[b].UpperBound
		count       = *buckets[b].CumulativeCount
	)

	// Calculate the linearly interpolated value within the bucket.
	if b > 0 {
		bucketStart = *buckets[b-1].UpperBound
		count -= *buckets[b-1].CumulativeCount
		rank -= *buckets[b-1].CumulativeCount
	}
	val := bucketStart + (bucketEnd-bucketStart)*(float64(rank)/float64(count))
	if math.IsNaN(val) || math.IsInf(val, -1) {
		return 0
	}

	// Should not extrapolate past the upper bound of the largest bucket.
	//
	// NB: SampleCount includes the implicit +Inf bucket but the
	// buckets[len(buckets)-1].UpperBound refers to the largest bucket defined
	// by us -- the client library doesn't give us access to the +Inf bucket
	// which Prometheus uses under the hood. With a high enough quantile, the
	// val computed further below surpasses the upper bound of the largest
	// bucket. Using that interpolated value feels wrong since we'd be
	// extrapolating. Also, for specific metrics if we see our q99 values to be
	// hitting the top-most bucket boundary, that's an indication for us to
	// choose better buckets for more accuracy. It's also worth noting that the
	// prometheus client library does the same thing when the resulting value is
	// in the +Inf bucket, whereby they return the upper bound of the second
	// last bucket -- see [1].
	//
	// [1]: https://github.com/prometheus/prometheus/blob/d9162189/promql/quantile.go#L103.
	if val > *buckets[len(buckets)-1].UpperBound {
		return *buckets[len(buckets)-1].UpperBound
	}

	return val
}

// Mean returns the average for this HistogramSnapshot.
func (hs HistogramSnapshot) Mean() float64 {
	return hs.h.GetSampleSum() / float64(hs.h.GetSampleCount())
}

// Total returns the sample count and sample sum for this HistogramSnapshot.
func (hs HistogramSnapshot) Total() (int64, float64) {
	return int64(hs.h.GetSampleCount()), hs.h.GetSampleSum()
}
