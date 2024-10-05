// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metric

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	_ "github.com/cockroachdb/cockroach/pkg/util/log" // for flags
	"github.com/kr/pretty"
	"github.com/prometheus/client_golang/prometheus"
	prometheusgo "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func testMarshal(t *testing.T, m json.Marshaler, exp string) {
	if b, err := m.MarshalJSON(); err != nil || !bytes.Equal(b, []byte(exp)) {
		t.Fatalf("unexpected: err=%v\nbytes=%s\nwanted=%s\nfor:\n%+v", err, b, exp, m)
	}
}

var emptyMetadata = Metadata{Name: ""}

func TestGauge(t *testing.T) {
	g := NewGauge(emptyMetadata)
	g.Update(10)
	if v := g.Value(); v != 10 {
		t.Fatalf("unexpected value: %d", v)
	}
	var wg sync.WaitGroup
	for i := int64(0); i < 10; i++ {
		wg.Add(2)
		go func(i int64) { g.Inc(i); wg.Done() }(i)
		go func(i int64) { g.Dec(i); wg.Done() }(i)
	}
	wg.Wait()
	if v := g.Value(); v != 10 {
		t.Fatalf("unexpected value: %d", v)
	}
	testMarshal(t, g, "10")
}

func TestFunctionalGauge(t *testing.T) {
	valToReturn := int64(10)
	g := NewFunctionalGauge(emptyMetadata, func() int64 { return valToReturn })
	if v := g.Value(); v != 10 {
		t.Fatalf("unexpected value: %d", v)
	}
	valToReturn = 15
	if v := g.Value(); v != 15 {
		t.Fatalf("unexpected value: %d", v)
	}
}

func TestGaugeFloat64(t *testing.T) {
	g := NewGaugeFloat64(emptyMetadata)
	g.Update(10.4)
	if v := g.Value(); v != 10.4 {
		t.Fatalf("unexpected value: %f", v)
	}
	testMarshal(t, g, "10.4")

	var wg sync.WaitGroup
	for i := int64(0); i < 10; i++ {
		wg.Add(2)
		go func(i int64) { g.Inc(float64(i)); wg.Done() }(i)
		go func(i int64) { g.Dec(float64(i)); wg.Done() }(i)
	}
	wg.Wait()
	if v := g.Value(); math.Abs(v-10.4) > 0.001 {
		t.Fatalf("unexpected value: %g", v)
	}
}

func TestCounter(t *testing.T) {
	c := NewCounter(emptyMetadata)
	c.Inc(90)
	if v := c.Count(); v != 90 {
		t.Fatalf("unexpected value: %d", v)
	}

	testMarshal(t, c, "90")
}

func TestCounterFloat64(t *testing.T) {
	g := NewCounterFloat64(emptyMetadata)
	g.Update(10)
	if v := g.Count(); v != 10 {
		t.Fatalf("unexpected value: %f", v)
	}
	testMarshal(t, g, "10")

	var wg sync.WaitGroup
	for i := int64(0); i < 10; i++ {
		wg.Add(1)
		go func(i int64) { g.Inc(float64(i)); wg.Done() }(i)
	}
	wg.Wait()
	if v := g.Count(); math.Abs(v-55.0) > 0.001 {
		t.Fatalf("unexpected value: %g", v)
	}

	for i := int64(55); i < 65; i++ {
		wg.Add(1)
		go func(i int64) { g.Update(float64(i)); wg.Done() }(i)
	}
	wg.Wait()
	if v := g.Count(); math.Abs(v-64.0) > 0.001 {
		t.Fatalf("unexpected value: %g", v)
	}
}

func TestHistogram(t *testing.T) {
	u := func(v int) *uint64 {
		n := uint64(v)
		return &n
	}

	f := func(v int) *float64 {
		n := float64(v)
		return &n
	}

	h := NewHistogram(HistogramOptions{
		Mode:     HistogramModePrometheus,
		Metadata: Metadata{},
		Duration: time.Hour,
		Buckets: []float64{
			1.0,
			5.0,
			10.0,
			25.0,
			100.0,
		},
	})

	// should return 0 if no observations are made
	require.Equal(t, 0.0, h.WindowedSnapshot().ValueAtQuantile(0))

	// 200 is intentionally set us the first value to verify that the function
	// does not return NaN or Inf.
	measurements := []int64{200, 0, 4, 5, 10, 20, 25, 30, 40, 90}
	var expSum float64
	for i, m := range measurements {
		h.RecordValue(m)
		if i == 0 {
			histWindow := h.WindowedSnapshot()
			require.Equal(t, 0.0, histWindow.ValueAtQuantile(0))
			require.Equal(t, 100.0, histWindow.ValueAtQuantile(99))
		}
		expSum += float64(m)
	}

	act := *h.ToPrometheusMetric().Histogram
	exp := prometheusgo.Histogram{
		SampleCount: u(len(measurements)),
		SampleSum:   &expSum,
		Bucket: []*prometheusgo.Bucket{
			{CumulativeCount: u(1), UpperBound: f(1)},
			{CumulativeCount: u(3), UpperBound: f(5)},
			{CumulativeCount: u(4), UpperBound: f(10)},
			{CumulativeCount: u(6), UpperBound: f(25)},
			{CumulativeCount: u(9), UpperBound: f(100)},
			// NB: 200 is greater than the largest defined bucket so prometheus
			// puts it in an implicit bucket with +Inf as the upper bound.
		},
	}

	if !reflect.DeepEqual(act, exp) {
		t.Fatalf("expected differs from actual: %s", pretty.Diff(exp, act))
	}

	histWindow := h.WindowedSnapshot()
	require.Equal(t, 0.0, histWindow.ValueAtQuantile(0))
	require.Equal(t, 1.0, histWindow.ValueAtQuantile(10))
	require.Equal(t, 17.5, histWindow.ValueAtQuantile(50))
	require.Equal(t, 75.0, histWindow.ValueAtQuantile(80))
	require.Equal(t, 100.0, histWindow.ValueAtQuantile(99.99))

	// Assert that native histogram schema is not defined
	require.Nil(t, h.ToPrometheusMetric().Histogram.Schema)
}

func TestNativeHistogram(t *testing.T) {
	defer func(enabled bool) {
		nativeHistogramsEnabled = enabled
	}(nativeHistogramsEnabled)
	nativeHistogramsEnabled = true
	h := NewHistogram(HistogramOptions{
		Mode:     HistogramModePrometheus,
		Metadata: Metadata{},
		Duration: time.Hour,
		BucketConfig: staticBucketConfig{
			distribution: Exponential,
		},
	})

	// Assert that native histogram schema is defined
	require.NotNil(t, h.ToPrometheusMetric().Histogram.Schema)
}

func TestManualWindowHistogram(t *testing.T) {
	u := func(v int) *uint64 {
		n := uint64(v)
		return &n
	}

	f := func(v int) *float64 {
		n := float64(v)
		return &n
	}

	buckets := []float64{
		1.0,
		5.0,
		10.0,
		25.0,
		100.0,
	}

	h := NewManualWindowHistogram(
		Metadata{},
		buckets,
		false, /* withRotate */
	)

	// should return 0 if no observations are made
	require.Equal(t, 0.0, h.WindowedSnapshot().ValueAtQuantile(0))

	histogram := prometheus.NewHistogram(prometheus.HistogramOpts{Buckets: buckets})
	pMetric := &prometheusgo.Metric{}
	// 200 is intentionally set us the first value to verify that the function
	// does not return NaN or Inf.
	measurements := []float64{200, 0, 4, 5, 10, 20, 25, 30, 40, 90}
	var expSum float64
	for _, m := range measurements {
		histogram.Observe(m)
		expSum += m
	}
	require.NoError(t, histogram.Write(pMetric))
	h.Update(histogram, pMetric.Histogram)

	act := *h.ToPrometheusMetric().Histogram
	exp := prometheusgo.Histogram{
		SampleCount: u(len(measurements)),
		SampleSum:   &expSum,
		Bucket: []*prometheusgo.Bucket{
			{CumulativeCount: u(1), UpperBound: f(1)},
			{CumulativeCount: u(3), UpperBound: f(5)},
			{CumulativeCount: u(4), UpperBound: f(10)},
			{CumulativeCount: u(6), UpperBound: f(25)},
			{CumulativeCount: u(9), UpperBound: f(100)},
			// NB: 200 is greater than the largest defined bucket so prometheus
			// puts it in an implicit bucket with +Inf as the upper bound.
		},
	}

	if !reflect.DeepEqual(act, exp) {
		t.Fatalf("expected differs from actual: %s", pretty.Diff(exp, act))
	}

	// Rotate and RecordValue are not supported when using Update. See comment on
	// NewManualWindowHistogram.
	require.Panics(t, func() { h.RecordValue(0) })
	require.Panics(t, func() { _ = h.Rotate() })

	histWindow := h.WindowedSnapshot()
	require.Equal(t, 0.0, histWindow.ValueAtQuantile(0))
	require.Equal(t, 1.0, histWindow.ValueAtQuantile(10))
	require.Equal(t, 17.5, histWindow.ValueAtQuantile(50))
	require.Equal(t, 75.0, histWindow.ValueAtQuantile(80))
	require.Equal(t, 100.0, histWindow.ValueAtQuantile(99.99))

	// This section will test that updating the histogram with new values results
	// in a correctly merged view when quantiles are calculated.
	prev := pMetric
	new := &prometheusgo.Metric{}
	measurements2 := []float64{3, 20, 50}
	for _, m := range measurements2 {
		histogram.Observe(m)
		expSum += m
	}
	require.NoError(t, histogram.Write(new))
	SubtractPrometheusHistograms(new.GetHistogram(), prev.GetHistogram())
	h.Update(histogram, new.GetHistogram())

	// Adding extra values to cumulative histogram to make sure it is not used in
	// the expected outcome.
	histogram.Observe(5)
	histogram.Observe(5)

	act = *h.WindowedSnapshot().h
	exp = prometheusgo.Histogram{
		SampleCount: u(len(measurements) + len(measurements2)),
		SampleSum:   &expSum,
		Bucket: []*prometheusgo.Bucket{
			{CumulativeCount: u(1), UpperBound: f(1)},
			{CumulativeCount: u(4), UpperBound: f(5)},
			{CumulativeCount: u(5), UpperBound: f(10)},
			{CumulativeCount: u(8), UpperBound: f(25)},
			{CumulativeCount: u(12), UpperBound: f(100)},
		},
	}

	if !reflect.DeepEqual(act, exp) {
		t.Fatalf("expected differs from actual: %s", pretty.Diff(exp, act))
	}
}

func TestManualWindowHistogramTicker(t *testing.T) {
	now := time.UnixMicro(1699565116)
	defer TestingSetNow(func() time.Time {
		return now
	})()

	buckets := []float64{
		0.25,
		0.5,
		1.0,
		2.0,
	}

	h := NewManualWindowHistogram(
		Metadata{},
		buckets,
		false, /* withRotate */
	)

	phistogram := prometheus.NewHistogram(prometheus.HistogramOpts{Buckets: buckets})
	pMetric := &prometheusgo.Metric{}

	recordValue := func() {
		phistogram.Observe(1)
		wHistogram := prometheus.NewHistogram(prometheus.HistogramOpts{Buckets: buckets})
		wHistogram.Observe(1)
		require.NoError(t, wHistogram.Write(pMetric))
		h.Update(phistogram, pMetric.Histogram)
	}

	// Test 0 case, sum and count should be 0.
	h.Inspect(func(interface{}) {})
	wCount, wSum := h.WindowedSnapshot().Total()
	require.Equal(t, float64(0), wSum)
	require.Equal(t, int64(0), wCount)

	// Record a value.
	h.Inspect(func(interface{}) {})
	recordValue()
	wCount, wSum = h.WindowedSnapshot().Total()
	require.Equal(t, float64(1), wSum)
	require.Equal(t, int64(1), wCount)

	// Add 30 seconds, the previous value will rotate but the merged window should
	// still have the previous value.
	now = now.Add(30 * time.Second)
	h.Inspect(func(interface{}) {})
	recordValue()
	wCount, wSum = h.WindowedSnapshot().Total()
	require.Equal(t, float64(2), wSum)
	require.Equal(t, int64(2), wCount)

	// Add another 30 seconds, the prev window should have reset, and the new
	// value is now in the prev window, expect 1.
	now = now.Add(30 * time.Second)
	h.Inspect(func(interface{}) {})
	wCount, wSum = h.WindowedSnapshot().Total()
	require.Equal(t, float64(1), wSum)
	require.Equal(t, int64(1), wCount)
}

func TestNewHistogramRotate(t *testing.T) {
	now := time.UnixMicro(1699565116)
	defer TestingSetNow(func() time.Time {
		return now
	})()

	h := NewHistogram(HistogramOptions{
		Mode:     HistogramModePrometheus,
		Metadata: emptyMetadata,
		Duration: 10 * time.Second,
	})
	for i := 0; i < 4; i++ {
		// Windowed histogram is initially empty.
		h.Inspect(func(interface{}) {}) // triggers ticking
		_, sum := h.WindowedSnapshot().Total()
		require.Zero(t, sum)
		// But cumulative histogram has history (if i > 0).
		count, _ := h.CumulativeSnapshot().Total()
		require.EqualValues(t, i, count)
		// Add a measurement and verify it's there.
		{
			h.RecordValue(12345)
			f := float64(12345) + sum
			_, wSum := h.WindowedSnapshot().Total()
			require.Equal(t, wSum, f)
		}
		// Tick. This rotates the histogram.
		now = now.Add(time.Duration(i+1) * 10 * time.Second)
		// Go to beginning.
	}
}

func TestHistogramWindowed(t *testing.T) {
	now := time.UnixMicro(1699565116)
	defer TestingSetNow(func() time.Time {
		return now
	})()

	duration := 10 * time.Second

	h := NewHistogram(HistogramOptions{
		Mode:         HistogramModePrometheus,
		Metadata:     Metadata{},
		Duration:     duration,
		BucketConfig: IOLatencyBuckets,
	})

	measurements := []int64{200000000, 0, 4000000, 5000000, 10000000, 20000000,
		25000000, 30000000, 40000000, 90000000}

	// Sort the measurements so we can calculate the expected quantile values
	// for the first windowed histogram after the measurements have been recorded.
	sortedMeasurements := make([]int64, len(measurements))
	copy(sortedMeasurements, measurements)
	sort.Slice(sortedMeasurements, func(i, j int) bool {
		return sortedMeasurements[i] < sortedMeasurements[j]
	})

	// Calculate the expected quantile values as the lowest bucket values that are
	// greater than each measurement.
	count := 0
	j := 0
	IOLatencyBuckets := IOLatencyBuckets.
		GetBucketsFromBucketConfig()
	var expQuantileValues []float64
	for i := range IOLatencyBuckets {
		if j < len(sortedMeasurements) && IOLatencyBuckets[i] > float64(
			sortedMeasurements[j]) {
			count += 1
			j += 1
			expQuantileValues = append(expQuantileValues, IOLatencyBuckets[i])
		}
	}

	w := 2
	var expHist []prometheusgo.Histogram
	var expSum float64
	var expCount uint64
	for i := 0; i < w; i++ {
		h.Inspect(func(interface{}) {}) // trigger ticking
		if i == 0 {
			// If there is no previous window, we should be unable to calculate mean
			// or quantile without any observations.
			histWindow := h.WindowedSnapshot()
			require.Equal(t, 0.0, histWindow.ValueAtQuantile(99.99))
			if !math.IsNaN(histWindow.Mean()) {
				t.Fatalf("mean should be undefined with no observations")
			}
			// Record all measurements on first iteration.
			for _, m := range measurements {
				h.RecordValue(m)
				expCount += 1
				expSum += float64(m)
			}
			// Because we have 10 observations, we expect quantiles to correspond
			// to observation indices (e.g., the 8th expected quantile value is equal
			// to the value interpolated at the 80th percentile).
			histWindow = h.WindowedSnapshot()
			require.Equal(t, 0.0, histWindow.ValueAtQuantile(0))
			require.Equal(t, expQuantileValues[0], histWindow.ValueAtQuantile(10))
			require.Equal(t, expQuantileValues[4], histWindow.ValueAtQuantile(50))
			require.Equal(t, expQuantileValues[7], histWindow.ValueAtQuantile(80))
			require.Equal(t, expQuantileValues[9], histWindow.ValueAtQuantile(99.99))
		} else {
			// The SampleSum and SampleCount values in the current window before any
			// observations should be equal to those of the previous window, after all
			// observations (the quantile values will also be the same).
			expSum = *expHist[i-1].SampleSum
			expCount = *expHist[i-1].SampleCount

			// After recording a few higher-value observations in the second window,
			// the quantile values will shift in the direction of the observations.
			for _, m := range sortedMeasurements[len(sortedMeasurements)-3 : len(
				sortedMeasurements)-1] {
				h.RecordValue(m)
				expCount += 1
				expSum += float64(m)
			}
			histWindow := h.WindowedSnapshot()
			require.Less(t, expQuantileValues[4], histWindow.ValueAtQuantile(50))
			require.Less(t, expQuantileValues[7], histWindow.ValueAtQuantile(80))
			require.Equal(t, expQuantileValues[9], histWindow.ValueAtQuantile(99.99))
		}

		// In all cases, the windowed mean should be equal to the expected sum/count
		require.Equal(t, expSum/float64(expCount), h.WindowedSnapshot().Mean())

		expHist = append(expHist, prometheusgo.Histogram{
			SampleCount: &expCount,
			SampleSum:   &expSum,
		})

		// Increment Now time to trigger tick on the following iteration.
		now = now.Add(time.Duration(i+1) * (duration / 2))
	}
}

func TestMergeWindowedHistogram(t *testing.T) {
	measurements := []int64{4000000, 90000000}
	opts := prometheus.HistogramOpts{
		Buckets: IOLatencyBuckets.
			GetBucketsFromBucketConfig(),
	}

	prevWindow := prometheus.NewHistogram(opts)
	curWindow := prometheus.NewHistogram(opts)

	cur := &prometheusgo.Metric{}
	prev := &prometheusgo.Metric{}

	prevWindow.Observe(float64(measurements[0]))
	require.NoError(t, prevWindow.Write(prev))
	require.NoError(t, curWindow.Write(cur))

	MergeWindowedHistogram(cur.Histogram, prev.Histogram)
	// Merging a non-empty previous histogram into an empty current histogram
	// should result in the current histogram containing the same sample sum,
	// sample count, and per-bucket cumulative count values as the previous
	// histogram.
	require.Equal(t, uint64(1), *cur.Histogram.SampleCount)
	require.Equal(t, float64(measurements[0]), *cur.Histogram.SampleSum)
	for _, bucket := range cur.Histogram.Bucket {
		if *bucket.UpperBound > float64(measurements[0]) {
			require.Equal(t, uint64(1), *bucket.CumulativeCount)
		}
	}

	curWindow.Observe(float64(measurements[1]))
	require.NoError(t, curWindow.Write(cur))

	MergeWindowedHistogram(cur.Histogram, prev.Histogram)
	// Merging a non-empty previous histogram with a non-empty current histogram
	// should result in the current histogram containing its original sample sum,
	// sample count, and per-bucket cumulative count values,
	// plus those of the previous histogram.
	require.Equal(t, uint64(2), *cur.Histogram.SampleCount)
	require.Equal(t, float64(measurements[0]+measurements[1]),
		*cur.Histogram.SampleSum)
	for _, bucket := range cur.Histogram.Bucket {
		if *bucket.UpperBound > float64(measurements[1]) {
			require.Equal(t, uint64(2), *bucket.CumulativeCount)
		} else if *bucket.UpperBound > float64(measurements[0]) {
			require.Equal(t, uint64(1), *bucket.CumulativeCount)
		}
	}
}
