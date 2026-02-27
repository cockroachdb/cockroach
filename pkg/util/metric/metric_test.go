// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metric

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	_ "github.com/cockroachdb/cockroach/pkg/util/log" // for flags
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/kr/pretty"
	"github.com/prometheus/client_golang/prometheus"
	prometheusgo "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
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

func TestGaugeVector(t *testing.T) {
	g := NewExportedGaugeVec(emptyMetadata, []string{"label1", "label2"})
	ls1 := map[string]string{"label1": "value1", "label2": "value2"}
	ls2 := map[string]string{"label1": "value3", "label2": "value4"}

	g.Update(ls1, 10)
	g.Update(ls2, 10)

	metrics := g.ToPrometheusMetrics()
	require.Len(t, metrics, 2)
	require.Equal(t, *metrics[0].Gauge.Value, 10.0)
	require.Equal(t, *metrics[1].Gauge.Value, 10.0)

	var wg sync.WaitGroup
	for i := int64(0); i < 10; i++ {
		wg.Add(2)
		go func() { g.Inc(ls1, 1); wg.Done() }()
		go func() { g.Inc(ls2, 2); wg.Done() }()
	}
	wg.Wait()

	metrics = g.ToPrometheusMetrics()
	require.Equal(t, 20.0, *metrics[0].Gauge.Value)
	require.Equal(t, 30.0, *metrics[1].Gauge.Value)
	require.Equal(t, "label1", *metrics[0].GetLabel()[0].Name)
	require.Equal(t, "value1", *metrics[0].GetLabel()[0].Value)
	require.Equal(t, "label2", *metrics[0].GetLabel()[1].Name)
	require.Equal(t, "value2", *metrics[0].GetLabel()[1].Value)
	require.Equal(t, "label1", *metrics[1].GetLabel()[0].Name)
	require.Equal(t, "value3", *metrics[1].GetLabel()[0].Value)
	require.Equal(t, "label2", *metrics[1].GetLabel()[1].Name)
	require.Equal(t, "value4", *metrics[1].GetLabel()[1].Value)
}

func TestDerivedGauge(t *testing.T) {

	g := NewDerivedGauge(emptyMetadata, func(i int64) int64 { return i * 10 })

	require.Zero(t, g.Value())

	g.Update(10)
	require.Equal(t, int64(100), g.Value())

	g.Dec(1)
	require.Equal(t, int64(90), g.Value())

	g.Inc(1)
	require.Equal(t, int64(100), g.Value())
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

func TestDerivedGaugeVec(t *testing.T) {
	g := NewDerivedExportedGaugeVec(emptyMetadata, []string{"label1", "label2"}, func(val int64) int64 { return val * 2 })

	ls1 := map[string]string{"label1": "a", "label2": "b"}
	ls2 := map[string]string{"label1": "c", "label2": "d"}

	g.Update(ls1, 10)
	g.Update(ls2, 20)

	metrics := g.ToPrometheusMetrics()
	require.Len(t, metrics, 2)
	require.Equal(t, 20.0, *metrics[0].Gauge.Value)
	require.Equal(t, 40.0, *metrics[1].Gauge.Value)

	// Verify labels are correct.
	require.Equal(t, "label1", *metrics[0].GetLabel()[0].Name)
	require.Equal(t, "a", *metrics[0].GetLabel()[0].Value)
	require.Equal(t, "label2", *metrics[0].GetLabel()[1].Name)
	require.Equal(t, "b", *metrics[0].GetLabel()[1].Value)

	// Update a value and verify the transform applies to the new value.
	g.Update(ls1, 50)
	metrics = g.ToPrometheusMetrics()
	require.Equal(t, 100.0, *metrics[0].Gauge.Value)
	require.Equal(t, 40.0, *metrics[1].Gauge.Value)
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

func TestGaugeFloat64MarshalInfNaN(t *testing.T) {
	g := NewGaugeFloat64(emptyMetadata)

	g.Update(math.Inf(1))
	testMarshal(t, g, "0")

	g.Update(math.Inf(-1))
	testMarshal(t, g, "0")

	g.Update(math.NaN())
	testMarshal(t, g, "0")

	g.Update(42.5)
	testMarshal(t, g, "42.5")
}

func TestCounter(t *testing.T) {
	c := NewCounter(emptyMetadata)
	c.Inc(90)
	if v := c.Count(); v != 90 {
		t.Fatalf("unexpected value: %d", v)
	}

	testMarshal(t, c, "90")
}

func TestUniqueCounter(t *testing.T) {
	c := NewUniqueCounter(emptyMetadata)
	expected := int64(10_000)
	for i := int64(0); i < expected; i++ {
		c.Add([]byte(fmt.Sprintf("test-%d", i)))
	}
	// UniqueCounter is an approximation
	margin := float64(expected) * 0.005
	actual := c.Count()
	if math.Abs(float64(actual-expected)) > margin {
		t.Fatalf("unexpected value: %d", actual)
	}

	testMarshal(t, c, fmt.Sprintf("%d", actual))
}

func TestCounterFloat64(t *testing.T) {
	c := NewCounterFloat64(emptyMetadata)
	c.UpdateIfHigher(10)
	if v := c.Count(); v != 10 {
		t.Fatalf("unexpected value: %f", v)
	}
	testMarshal(t, c, "10")

	var wg sync.WaitGroup
	for i := int64(0); i < 10; i++ {
		wg.Add(1)
		go func(i int64) { c.Inc(float64(i)); wg.Done() }(i)
	}
	wg.Wait()
	if v := c.Count(); math.Abs(v-55.0) > 0.001 {
		t.Fatalf("unexpected value: %g", v)
	}

	for i := int64(55); i < 65; i++ {
		wg.Add(1)
		go func(i int64) { c.UpdateIfHigher(float64(i)); wg.Done() }(i)
	}
	wg.Wait()
	if v := c.Count(); math.Abs(v-64.0) > 0.001 {
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

// TestValueAtQuantileWithEmptyBuckets verifies that ValueAtQuantile does not
// return +Inf when adjacent buckets have the same cumulative count (causing a
// division by zero in the interpolation). This was the root cause of #163958.
func TestValueAtQuantileWithEmptyBuckets(t *testing.T) {
	u := func(v uint64) *uint64 { return &v }
	f := func(v float64) *float64 { return &v }

	// Construct a histogram where the first two buckets have the same
	// cumulative count, meaning the second bucket is empty. This causes
	// count to be 0 in the interpolation, producing +Inf via division by zero.
	h := &prometheusgo.Histogram{
		SampleCount: u(5),
		SampleSum:   f(250),
		Bucket: []*prometheusgo.Bucket{
			{CumulativeCount: u(0), UpperBound: f(1)},
			{CumulativeCount: u(0), UpperBound: f(5)},
			{CumulativeCount: u(2), UpperBound: f(10)},
			{CumulativeCount: u(3), UpperBound: f(25)},
			{CumulativeCount: u(5), UpperBound: f(100)},
		},
	}

	snap := MakeHistogramSnapshot(h)
	for q := 0.0; q <= 100.0; q += 10 {
		val := snap.ValueAtQuantile(q)
		require.False(t, math.IsInf(val, 0), "ValueAtQuantile(%v) returned infinity", q)
		require.False(t, math.IsNaN(val), "ValueAtQuantile(%v) returned NaN", q)
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
			require.Equal(t, f, wSum)
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

type toPromMetricsTC struct {
	labels [][]string
	value  float64
}

func (cv *CounterVec) assertPrometheusMetrics(t *testing.T, tc []toPromMetricsTC) {
	t.Helper()

	promMetrics := cv.ToPrometheusMetrics()
	assert.Len(t, promMetrics, len(tc))

	for i, m := range promMetrics {
		labels := []*prometheusgo.LabelPair{}

		for _, l := range tc[i].labels {
			labels = append(labels, &prometheusgo.LabelPair{
				Name:  &l[0],
				Value: &l[1],
			})
		}

		assert.Equal(t, &prometheusgo.Metric{
			Label: labels,
			Counter: &prometheusgo.Counter{
				Value: &tc[i].value,
			},
		}, m)
	}

}

func TestCounterVec(t *testing.T) {
	t.Run("labels provided match what is declared", func(t *testing.T) {
		c := NewExportedCounterVec(emptyMetadata, []string{"label1", "label2"})
		t.Run("update", func(t *testing.T) {
			assert.NotPanics(t, func() {
				c.Update(map[string]string{
					"label1": "value1",
					"label2": "value2",
				}, 10)

				c.Update(map[string]string{
					"label1": "value3",
					"label2": "value4",
				}, 10)

				// overwrite the previous value
				c.Update(map[string]string{
					"label1": "value3",
					"label2": "value4",
				}, 20)
			})
		})

		t.Run("inc", func(t *testing.T) {
			assert.NotPanics(t, func() {
				c.Inc(map[string]string{
					"label1": "value1",
					"label2": "value2",
				}, 10)

				c.Inc(map[string]string{
					"label1": "value3",
					"label2": "value4",
				}, 10)
			})
		})

		t.Run("count", func(t *testing.T) {
			assert.Equal(t, int64(20), c.Count(map[string]string{
				"label1": "value1",
				"label2": "value2",
			}))

			assert.Equal(t, int64(30), c.Count(map[string]string{
				"label1": "value3",
				"label2": "value4",
			}))

			// timeseries doesn't exist
			assert.Equal(t, int64(0), c.Count(map[string]string{
				"label1": "value5",
				"label2": "value6",
			}))
		})

		t.Run("to prometheus metrics", func(t *testing.T) {
			c.assertPrometheusMetrics(t, []toPromMetricsTC{{
				labels: [][]string{{"label1", "value1"}, {"label2", "value2"}},
				value:  20,
			}, {
				labels: [][]string{{"label1", "value3"}, {"label2", "value4"}},
				value:  30,
			}})
		})
	})

	t.Run("labels provided exceed what is declared", func(t *testing.T) {
		c := NewExportedCounterVec(emptyMetadata, []string{"label1", "label2"})
		t.Run("update", func(t *testing.T) {
			assert.NotPanics(t, func() {
				c.Update(map[string]string{
					"label1": "value1",
					"label2": "value2",
					"label3": "value3",
				}, 10)

				c.Update(map[string]string{
					"label1": "value1",
					"label2": "value2",
					"label3": "value3",
					"label4": "value4",
					"label5": "value5",
				}, 50)
			})
		})

		t.Run("count", func(t *testing.T) {
			assert.NotPanics(t, func() {
				assert.Equal(t, int64(50), c.Count(map[string]string{
					"label1": "value1",
					"label2": "value2",
					"label3": "value3",
					"label6": "value6",
				}))
			})
		})

		t.Run("to prometheus metrics", func(t *testing.T) {
			c.assertPrometheusMetrics(t, []toPromMetricsTC{
				{
					labels: [][]string{{"label1", "value1"}, {"label2", "value2"}},
					value:  50,
				},
			})
		})
		// we don't have to test all operation again
	})

	t.Run("labels provided are less than what is declared", func(t *testing.T) {
		c := NewExportedCounterVec(emptyMetadata, []string{"label1", "label2", "label3"})
		t.Run("update", func(t *testing.T) {
			assert.NotPanics(t, func() {
				c.Update(map[string]string{
					"label1": "value1",
					"label2": "value2",
				}, 10)
			})
		})

		t.Run("count", func(t *testing.T) {
			assert.Equal(t, int64(10), c.Count(map[string]string{
				"label1": "value1",
				"label2": "value2",
				"label3": "",
			}))

			assert.Equal(t, int64(0), c.Count(map[string]string{
				"label1": "value1",
				"label2": "value2",
				"label3": "value3",
			}))
		})

		t.Run("to prometheus metrics", func(t *testing.T) {
			c.assertPrometheusMetrics(t, []toPromMetricsTC{
				{
					labels: [][]string{{"label1", "value1"}, {"label2", "value2"}, {"label3", ""}},
					value:  10,
				},
			})
		})
	})

	t.Run("no matching labels", func(t *testing.T) {
		c := NewExportedCounterVec(emptyMetadata, []string{"label1", "label2"})
		t.Run("update", func(t *testing.T) {
			assert.NotPanics(t, func() {
				c.Update(map[string]string{
					"label3": "value3",
					"label4": "value4",
				}, 10)
			})
		})

		t.Run("count", func(t *testing.T) {
			assert.Equal(t, int64(10), c.Count(map[string]string{
				"label1": "",
				"label2": "",
			}))

			// TODO(arjunmahishi): Handle this case carefully. This is a bug.
			// assert.Equal(t, int64(0), c.Count(map[string]string{
			// 	"label3": "value3",
			// 	"label4": "value4",
			// }))
		})

		t.Run("to prometheus metrics", func(t *testing.T) {
			c.assertPrometheusMetrics(t, []toPromMetricsTC{
				{
					labels: [][]string{{"label1", ""}, {"label2", ""}},
					value:  10,
				},
			})
		})
	})
}

func TestHistogramVec(t *testing.T) {
	t.Run("Observe", func(t *testing.T) {

		h := NewExportedHistogramVec(emptyMetadata, Count1KBuckets, []string{"label1", "label2"})
		h.Observe(map[string]string{
			"label1": "value1",
			"label2": "value2",
		}, 10)

		metrics := h.ToPrometheusMetrics()
		require.Len(t, metrics, 1)
		require.Equal(t, uint64(1), *metrics[0].Histogram.SampleCount)
		require.Equal(t, float64(10), *metrics[0].Histogram.SampleSum)

		h.Observe(map[string]string{
			"label1": "value1",
			"label2": "value2",
		}, 20)

		metrics = h.ToPrometheusMetrics()
		require.Len(t, metrics, 1)
		require.Equal(t, uint64(2), *metrics[0].Histogram.SampleCount)
		require.Equal(t, float64(30), *metrics[0].Histogram.SampleSum)

		h.Observe(map[string]string{
			"label1": "value1",
			"label2": "value3",
		}, 10)

		metrics = h.ToPrometheusMetrics()
		require.Len(t, metrics, 2)
		// metric[0] should be unchanged
		require.Equal(t, uint64(2), *metrics[0].Histogram.SampleCount)
		require.Equal(t, float64(30), *metrics[0].Histogram.SampleSum)

		require.Equal(t, uint64(1), *metrics[1].Histogram.SampleCount)
		require.Equal(t, float64(10), *metrics[1].Histogram.SampleSum)

	})

	t.Run("Observe no matching labels", func(t *testing.T) {
		h := NewExportedHistogramVec(emptyMetadata, Count1KBuckets, []string{"label1", "label2"})
		h.Observe(map[string]string{
			"labelx": "value1",
			"labely": "value2",
		}, 10)

		metrics := h.ToPrometheusMetrics()
		// metric is still recorded
		require.Len(t, metrics, 1)
		require.Equal(t, uint64(1), *metrics[0].Histogram.SampleCount)
		require.Equal(t, float64(10), *metrics[0].Histogram.SampleSum)

		// metric only has pre-defined labels with no values
		labels := metrics[0].Label
		require.Len(t, labels, 2)
		require.Equal(t, "label1", *labels[0].Name)
		require.Equal(t, "", *labels[0].Value)
		require.Equal(t, "label2", *labels[1].Name)
		require.Equal(t, "", *labels[1].Value)
	})

	t.Run("Observe partial matching labels", func(t *testing.T) {
		h := NewExportedHistogramVec(emptyMetadata, Count1KBuckets, []string{"label1", "label2"})
		h.Observe(map[string]string{
			"label1": "value1",
			"labely": "value2",
		}, 10)

		metrics := h.ToPrometheusMetrics()
		// metric is still recorded
		require.Len(t, metrics, 1)
		require.Equal(t, uint64(1), *metrics[0].Histogram.SampleCount)
		require.Equal(t, float64(10), *metrics[0].Histogram.SampleSum)

		// metric only has pre-defined labels
		labels := metrics[0].Label
		require.Len(t, labels, 2)
		require.Equal(t, "label1", *labels[0].Name)
		require.Equal(t, "value1", *labels[0].Value)
		require.Equal(t, "label2", *labels[1].Name)
		require.Equal(t, "", *labels[1].Value)

		h.Observe(map[string]string{
			"label1": "value1",
			"labely": "value3",
		}, 20)

		metrics = h.ToPrometheusMetrics()
		// metric is still recorded
		require.Len(t, metrics, 1)
		require.Equal(t, uint64(2), *metrics[0].Histogram.SampleCount)
		require.Equal(t, float64(30), *metrics[0].Histogram.SampleSum)

		h.Observe(map[string]string{
			"label1": "value1",
			"label2": "value2",
			"labely": "value3",
		}, 1)

		metrics = h.ToPrometheusMetrics()
		// new metric recorded
		require.Len(t, metrics, 2)
		// First metric remains the same
		require.Equal(t, uint64(2), *metrics[0].Histogram.SampleCount)
		require.Equal(t, float64(30), *metrics[0].Histogram.SampleSum)

		require.Equal(t, uint64(1), *metrics[1].Histogram.SampleCount)
		require.Equal(t, float64(1), *metrics[1].Histogram.SampleSum)
	})
}

func BenchmarkHistogramRecordValue(b *testing.B) {
	h := NewHistogram(HistogramOptions{
		Metadata: Metadata{
			Name:       "my.test.metric",
			MetricType: prometheusgo.MetricType_HISTOGRAM,
		},
		Duration:     0,
		BucketConfig: IOLatencyBuckets,
		Mode:         HistogramModePrometheus,
	})

	b.ResetTimer()
	r, _ := randutil.NewTestRand()

	b.Run("insert integers", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			h.RecordValue(int64(i))
		}
	})
	b.Run("insert zero", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			h.RecordValue(0)
		}
	})
	b.Run("random integers", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			h.RecordValue(int64(randutil.RandIntInRange(r, int(IOLatencyBuckets.min), int(IOLatencyBuckets.max))))
		}
	})
}

func TestMetadataGetLabels(t *testing.T) {
	tests := []struct {
		name            string
		metadata        Metadata
		useStaticLabels bool
		wantLabels      []*prometheusgo.LabelPair
	}{
		{
			name: "only regular labels",
			metadata: Metadata{
				Labels: []*LabelPair{
					{Name: proto.String("regular1"), Value: proto.String("value1")},
					{Name: proto.String("regular2"), Value: proto.String("value2")},
				},
			},
			useStaticLabels: false,
			wantLabels: []*prometheusgo.LabelPair{
				{Name: proto.String("regular1"), Value: proto.String("value1")},
				{Name: proto.String("regular2"), Value: proto.String("value2")},
			},
		},
		{
			name: "only static labels",
			metadata: Metadata{
				StaticLabels: []*LabelPair{
					{Name: proto.String("static1"), Value: proto.String("value1")},
					{Name: proto.String("static2"), Value: proto.String("value2")},
				},
			},
			useStaticLabels: true,
			wantLabels: []*prometheusgo.LabelPair{
				{Name: proto.String("static1"), Value: proto.String("value1")},
				{Name: proto.String("static2"), Value: proto.String("value2")},
			},
		},
		{
			name: "both regular and static labels",
			metadata: Metadata{
				Labels: []*LabelPair{
					{Name: proto.String("regular1"), Value: proto.String("value1")},
				},
				StaticLabels: []*LabelPair{
					{Name: proto.String("static1"), Value: proto.String("value1")},
				},
			},
			useStaticLabels: true,
			wantLabels: []*prometheusgo.LabelPair{
				{Name: proto.String("static1"), Value: proto.String("value1")},
				{Name: proto.String("regular1"), Value: proto.String("value1")},
			},
		},
		{
			name: "both regular and static labels but static disabled",
			metadata: Metadata{
				Labels: []*LabelPair{
					{Name: proto.String("regular1"), Value: proto.String("value1")},
				},
				StaticLabels: []*LabelPair{
					{Name: proto.String("static1"), Value: proto.String("value1")},
				},
			},
			useStaticLabels: false,
			wantLabels: []*prometheusgo.LabelPair{
				{Name: proto.String("regular1"), Value: proto.String("value1")},
			},
		},
		{
			name:            "no labels",
			metadata:        Metadata{},
			useStaticLabels: false,
			wantLabels:      []*prometheusgo.LabelPair{},
		},
		{
			name:            "no labels with static enabled",
			metadata:        Metadata{},
			useStaticLabels: true,
			wantLabels:      []*prometheusgo.LabelPair{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.metadata.GetLabels(tt.useStaticLabels)
			if len(got) != len(tt.wantLabels) {
				t.Errorf("GetLabels() returned %d labels, want %d", len(got), len(tt.wantLabels))
				return
			}
			for i := range got {
				if *got[i].Name != *tt.wantLabels[i].Name {
					t.Errorf("label %d: got name %q, want %q", i, *got[i].Name, *tt.wantLabels[i].Name)
				}
				if *got[i].Value != *tt.wantLabels[i].Value {
					t.Errorf("label %d: got value %q, want %q", i, *got[i].Value, *tt.wantLabels[i].Value)
				}
			}
		})
	}
}

func TestMakeLabelPairs(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		want        []*LabelPair
		expectPanic bool
	}{
		{
			name: "empty args",
			args: []string{},
			want: []*LabelPair{},
		},
		{
			name:        "single arg",
			args:        []string{"label1"},
			expectPanic: true,
		},
		{
			name:        "odd number of args",
			args:        []string{"label1", "value1", "label2", "value2", "label3"},
			expectPanic: true,
		},
		{
			name: "even number of args",
			args: []string{"label1", "value1", "label2", "value2"},
			want: []*LabelPair{
				{Name: proto.String("label1"), Value: proto.String("value1")},
				{Name: proto.String("label2"), Value: proto.String("value2")},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expectPanic {
				require.Panics(t, func() { MakeLabelPairs(tt.args...) })
				return
			}

			got := MakeLabelPairs(tt.args...)
			if len(got) != len(tt.want) {
				t.Errorf("MakeLabelPairs() returned %d pairs, want %d", len(got), len(tt.want))
				return
			}
			for i := range got {
				if *got[i].Name != *tt.want[i].Name {
					t.Errorf("pair %d: got name %q, want %q", i, *got[i].Name, *tt.want[i].Name)
				}
				if *got[i].Value != *tt.want[i].Value {
					t.Errorf("pair %d: got value %q, want %q", i, *got[i].Value, *tt.want[i].Value)
				}
			}
		})
	}
}
