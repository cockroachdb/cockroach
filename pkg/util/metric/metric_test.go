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
	"reflect"
	"sync"
	"testing"
	"time"

	_ "github.com/cockroachdb/cockroach/pkg/util/log" // for flags
	"github.com/kr/pretty"
	prometheusgo "github.com/prometheus/client_model/go"
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
}

func TestRate(t *testing.T) {
	r := NewRate(emptyMetadata, time.Minute)
	r.Add(0)
	if v := r.Value(); v != 0 {
		t.Fatalf("unexpected value: %f", v)
	}
	testMarshal(t, r, "0")
}

func TestCounter(t *testing.T) {
	c := NewCounter(emptyMetadata)
	c.Inc(90)
	if v := c.Count(); v != 90 {
		t.Fatalf("unexpected value: %d", v)
	}

	testMarshal(t, c, "90")
}

func setNow(d time.Duration) {
	now = func() time.Time {
		return time.Time{}.Add(d)
	}
}

func TestHistogramPrometheus(t *testing.T) {
	u := func(v int) *uint64 {
		n := uint64(v)
		return &n
	}

	f := func(v int) *float64 {
		n := float64(v)
		return &n
	}

	h := NewHistogram(Metadata{}, time.Hour, 10, 1)
	h.RecordValue(1)
	h.RecordValue(5)
	h.RecordValue(5)
	h.RecordValue(10)
	h.RecordValue(15000) // counts as 10
	act := *h.ToPrometheusMetric().Histogram

	expSum := float64(1*1 + 2*5 + 2*10)

	exp := prometheusgo.Histogram{
		SampleCount: u(5),
		SampleSum:   &expSum,
		Bucket: []*prometheusgo.Bucket{
			{CumulativeCount: u(1), UpperBound: f(1)},
			{CumulativeCount: u(3), UpperBound: f(5)},
			{CumulativeCount: u(5), UpperBound: f(10)},
		},
	}

	if !reflect.DeepEqual(act, exp) {
		t.Fatalf("expected differs from actual: %s", pretty.Diff(exp, act))
	}
}

func TestHistogramRotate(t *testing.T) {
	defer TestingSetNow(nil)()
	setNow(0)
	duration := histWrapNum * time.Second
	h := NewHistogram(emptyMetadata, duration, 1000+10*histWrapNum, 3)
	var cur time.Duration
	for i := 0; i < 3*histWrapNum; i++ {
		v := int64(10 * i)
		h.RecordValue(v)
		cur += time.Second
		setNow(cur)
		cur, windowDuration := h.Windowed()
		if windowDuration != duration {
			t.Fatalf("window changed: is %s, should be %s", windowDuration, duration)
		}

		// When i == histWrapNum-1, we expect the entry from i==0 to move out
		// of the window (since we rotated for the histWrapNum'th time).
		expMin := int64((1 + i - (histWrapNum - 1)) * 10)
		if expMin < 0 {
			expMin = 0
		}

		if min := cur.Min(); min != expMin {
			t.Fatalf("%d: unexpected minimum %d, expected %d", i, min, expMin)
		}

		if max, expMax := cur.Max(), v; max != expMax {
			t.Fatalf("%d: unexpected maximum %d, expected %d", i, max, expMax)
		}
	}
}

func TestRateRotate(t *testing.T) {
	defer TestingSetNow(nil)()
	setNow(0)
	const interval = 10 * time.Second
	r := NewRate(emptyMetadata, interval)

	// Skip the warmup phase of the wrapped EWMA for this test.
	for i := 0; i < 100; i++ {
		r.wrapped.Add(0)
	}

	// Put something nontrivial in.
	r.Add(100)

	for cur := time.Duration(0); cur < 5*interval; cur += time.Second / 2 {
		prevVal := r.Value()
		setNow(cur)
		curVal := r.Value()
		expChange := (cur % time.Second) != 0
		hasChange := prevVal != curVal
		if expChange != hasChange {
			t.Fatalf("%s: expChange %t, hasChange %t (from %v to %v)",
				cur, expChange, hasChange, prevVal, curVal)
		}
	}

	v := r.Value()
	if v > .1 {
		t.Fatalf("final value implausible: %v", v)
	}
}
