// Copyright 2015 The Cockroach Authors.
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
	"bytes"
	"encoding/json"
	"math"
	"reflect"
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
	g.UpdateIfHigher(10)
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
		go func(i int64) { g.UpdateIfHigher(float64(i)); wg.Done() }(i)
	}
	wg.Wait()
	if v := g.Count(); math.Abs(v-64.0) > 0.001 {
		t.Fatalf("unexpected value: %g", v)
	}
}

func setNow(d time.Duration) {
	now = func() time.Time {
		return time.Time{}.Add(d)
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
	require.Equal(t, 0.0, h.ValueAtQuantileWindowed(0))

	// 200 is intentionally set us the first value to verify that the function
	// does not return NaN or Inf.
	measurements := []int64{200, 0, 4, 5, 10, 20, 25, 30, 40, 90}
	var expSum float64
	for i, m := range measurements {
		h.RecordValue(m)
		if i == 0 {
			require.Equal(t, 0.0, h.ValueAtQuantileWindowed(0))
			require.Equal(t, 100.0, h.ValueAtQuantileWindowed(99))
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

	require.Equal(t, 0.0, h.ValueAtQuantileWindowed(0))
	require.Equal(t, 1.0, h.ValueAtQuantileWindowed(10))
	require.Equal(t, 17.5, h.ValueAtQuantileWindowed(50))
	require.Equal(t, 75.0, h.ValueAtQuantileWindowed(80))
	require.Equal(t, 100.0, h.ValueAtQuantileWindowed(99.99))
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
	require.Equal(t, 0.0, h.ValueAtQuantileWindowed(0))

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

	require.Equal(t, 0.0, h.ValueAtQuantileWindowed(0))
	require.Equal(t, 1.0, h.ValueAtQuantileWindowed(10))
	require.Equal(t, 17.5, h.ValueAtQuantileWindowed(50))
	require.Equal(t, 75.0, h.ValueAtQuantileWindowed(80))
	require.Equal(t, 100.0, h.ValueAtQuantileWindowed(99.99))
}

func TestNewHistogramRotate(t *testing.T) {
	defer TestingSetNow(nil)()
	setNow(0)

	h := NewHistogram(HistogramOptions{
		Mode:     HistogramModePrometheus,
		Metadata: emptyMetadata,
		Duration: 10 * time.Second,
		Buckets:  nil,
	})
	for i := 0; i < 4; i++ {
		// Windowed histogram is initially empty.
		h.Inspect(func(interface{}) {}) // triggers ticking
		_, sum := h.TotalWindowed()
		require.Zero(t, sum)
		// But cumulative histogram has history (if i > 0).
		count, _ := h.Total()
		require.EqualValues(t, i, count)

		// Add a measurement and verify it's there.
		{
			h.RecordValue(12345)
			f := float64(12345)
			_, wSum := h.TotalWindowed()
			require.Equal(t, wSum, f)
		}
		// Tick. This rotates the histogram.
		setNow(time.Duration(i+1) * 10 * time.Second)
		// Go to beginning.
	}
}
