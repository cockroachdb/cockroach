// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metric

import (
	"math"
	"sort"

	"github.com/cockroachdb/goodhistogram"
	prometheusgo "github.com/prometheus/client_model/go"
)

// HistogramSnapshot represents a point-in-time snapshot of a Histogram
// metric, against which calculations like Mean, Total, and Quantiles
// can be calculated.
//
// This allows a consistent dataset to be used across calculations to
// avoid inaccuracies from one calculation to another, and isolates the
// supported operations to a single implementation.
type HistogramSnapshot struct {
	gh *goodhistogram.Snapshot
	// prometheusH is a fallback for snapshots created from
	// prometheusgo.Histogram data (e.g. from external sources like
	// pebble or the Go runtime scheduler). When set, the goodhistogram
	// snapshot is nil.
	prometheusH *prometheusgo.Histogram
}

// MakeHistogramSnapshotFromGoodHistogram returns a new HistogramSnapshot
// backed by a goodhistogram.Snapshot.
func MakeHistogramSnapshotFromGoodHistogram(s goodhistogram.Snapshot) HistogramSnapshot {
	return HistogramSnapshot{gh: &s}
}

// MakeHistogramSnapshot returns a new HistogramSnapshot backed by
// a prometheusgo.Histogram. This constructor exists for compatibility
// with callers that produce prometheus-format histograms (e.g.
// ManualWindowHistogram.Update, schedulerlatency).
func MakeHistogramSnapshot(h *prometheusgo.Histogram) HistogramSnapshot {
	return HistogramSnapshot{prometheusH: h}
}

// ValueAtQuantile takes a quantile value [0,100] and returns the
// interpolated value at that quantile for this HistogramSnapshot.
func (hs HistogramSnapshot) ValueAtQuantile(q float64) float64 {
	if hs.gh != nil {
		return hs.gh.ValueAtQuantile(q / 100)
	}
	return valueAtQuantileFromPrometheus(hs.prometheusH, q)
}

// Mean returns the average for this HistogramSnapshot.
func (hs HistogramSnapshot) Mean() float64 {
	if hs.gh != nil {
		return hs.gh.Mean()
	}
	return hs.prometheusH.GetSampleSum() / float64(hs.prometheusH.GetSampleCount())
}

// Total returns the sample count and sample sum for this
// HistogramSnapshot.
func (hs HistogramSnapshot) Total() (int64, float64) {
	if hs.gh != nil {
		count, sum := hs.gh.Total()
		return count, sum
	}
	return int64(hs.prometheusH.GetSampleCount()), hs.prometheusH.GetSampleSum()
}

// valueAtQuantileFromPrometheus implements the original linear
// interpolation logic for prometheus-backed histogram snapshots.
func valueAtQuantileFromPrometheus(histogram *prometheusgo.Histogram, q float64) float64 {
	buckets := histogram.Bucket
	n := float64(histogram.GetSampleCount())
	if n == 0 {
		return 0
	}

	// NB: The 0.5 is added for rounding purposes; it helps in cases
	// where SampleCount is small.
	rank := uint64(((q / 100) * n) + 0.5)

	// Since we are missing the +Inf bucket, CumulativeCounts may never
	// exceed rank. By omitting the highest bucket we have from the
	// search, the failed search will land on that last bucket and we
	// don't have to do any special checks regarding landing on a
	// non-existent bucket.
	b := sort.Search(len(buckets)-1, func(i int) bool {
		return *buckets[i].CumulativeCount >= rank
	})

	var (
		bucketStart float64
		bucketEnd   = *buckets[b].UpperBound
		count       = *buckets[b].CumulativeCount
	)

	if b > 0 {
		bucketStart = *buckets[b-1].UpperBound
		count -= *buckets[b-1].CumulativeCount
		rank -= *buckets[b-1].CumulativeCount
	}
	var val float64
	if count == 0 {
		if rank == 0 {
			val = bucketStart
		} else {
			val = bucketEnd
		}
	} else {
		val = bucketStart + (bucketEnd-bucketStart)*(float64(rank)/float64(count))
	}
	if math.IsNaN(val) || math.IsInf(val, 0) {
		return 0
	}

	if val > *buckets[len(buckets)-1].UpperBound {
		return *buckets[len(buckets)-1].UpperBound
	}

	return val
}
