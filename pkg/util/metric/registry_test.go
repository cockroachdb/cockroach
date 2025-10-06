// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metric

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/stretchr/testify/require"
)

func (r *Registry) findMetricByName(name string) Iterable {
	r.Lock()
	defer r.Unlock()
	for _, metric := range r.tracked {
		if metric.GetName() == name {
			return metric
		}
	}
	return nil
}

// getCounter returns the Counter in this registry with the given name. If a
// Counter with this name is not present (including if a non-Counter Iterable is
// registered with the name), nil is returned.
func (r *Registry) getCounter(name string) *Counter {
	iterable := r.findMetricByName(name)
	if iterable == nil {
		return nil
	}

	switch t := iterable.(type) {
	case *Counter:
		return t
	default:
	}
	return nil
}

// getGauge returns the Gauge in this registry with the given name. If a Gauge
// with this name is not present (including if a non-Gauge Iterable is
// registered with the name), nil is returned.
func (r *Registry) getGauge(name string) *Gauge {
	iterable := r.findMetricByName(name)
	if iterable == nil {
		return nil
	}
	gauge, ok := iterable.(*Gauge)
	if !ok {
		return nil
	}
	return gauge
}

type NestedStruct struct {
	NestedStructGauge *Gauge
}

// MetricStruct implements the metrics.Struct interface.
func (NestedStruct) MetricStruct() {}

func TestRegistry(t *testing.T) {
	r := NewRegistry()

	topGauge := NewGauge(Metadata{Name: "top.gauge"})
	r.AddMetric(topGauge)

	topFloatGauge := NewGaugeFloat64(Metadata{Name: "top.floatgauge"})
	r.AddMetric(topFloatGauge)

	topCounter := NewCounter(Metadata{Name: "top.counter"})
	r.AddMetric(topCounter)

	r.AddMetric(NewCounterFloat64(Metadata{Name: "top.floatcounter"}))

	r.AddMetric(NewHistogram(HistogramOptions{
		Mode:         HistogramModePrometheus,
		Metadata:     Metadata{Name: "top.histogram"},
		Duration:     time.Minute,
		BucketConfig: Count1KBuckets,
	}))

	bottomGauge := NewGauge(Metadata{Name: "bottom.gauge"})
	r.AddMetric(bottomGauge)

	ms := &struct {
		StructGauge         *Gauge
		StructGauge64       *GaugeFloat64
		StructCounter       *Counter
		StructCounter64     *CounterFloat64
		StructHistogram     IHistogram
		NestedStructGauge   NestedStruct
		ArrayStructCounters [4]*Counter
		MapOfCounters       map[int32]*Counter
		// Ensure that nil struct values in arrays are safe.
		NestedStructArray [2]*NestedStruct
		// A few extra ones: either not exported, or not metric objects.
		privateInt                    int
		NotAMetric                    int
		AlsoNotAMetric                string
		ReallyNotAMetric              *Registry
		DefinitelyNotAnArrayOfMetrics [2]int
	}{
		StructGauge:     NewGauge(Metadata{Name: "struct.gauge"}),
		StructGauge64:   NewGaugeFloat64(Metadata{Name: "struct.gauge64"}),
		StructCounter:   NewCounter(Metadata{Name: "struct.counter"}),
		StructCounter64: NewCounterFloat64(Metadata{Name: "struct.counter64"}),
		StructHistogram: NewHistogram(HistogramOptions{
			Mode:         HistogramModePrometheus,
			Metadata:     Metadata{Name: "struct.histogram"},
			Duration:     time.Minute,
			BucketConfig: Count1KBuckets,
		}),
		NestedStructGauge: NestedStruct{
			NestedStructGauge: NewGauge(Metadata{Name: "nested.struct.gauge"}),
		},
		ArrayStructCounters: [...]*Counter{
			NewCounter(Metadata{Name: "array.struct.counter.0"}),
			NewCounter(Metadata{Name: "array.struct.counter.1"}),
			nil, // skipped
			NewCounter(Metadata{Name: "array.struct.counter.3"}),
		},
		MapOfCounters: map[int32]*Counter{
			1: NewCounter(Metadata{Name: "map.counter.0"}),
			2: NewCounter(Metadata{Name: "map.counter.1"}),
		},
		NestedStructArray: [2]*NestedStruct{
			0: nil, // skipped
			1: {
				NestedStructGauge: NewGauge(Metadata{Name: "nested.struct.array.1.gauge"}),
			},
		},
		NotAMetric:                    0,
		AlsoNotAMetric:                "foo",
		ReallyNotAMetric:              NewRegistry(),
		DefinitelyNotAnArrayOfMetrics: [...]int{1, 2},
	}
	r.AddMetricStruct(ms)

	expNames := map[string]struct{}{
		"top.histogram":               {},
		"top.gauge":                   {},
		"top.floatgauge":              {},
		"top.counter":                 {},
		"top.floatcounter":            {},
		"bottom.gauge":                {},
		"struct.gauge":                {},
		"struct.gauge64":              {},
		"struct.counter":              {},
		"struct.counter64":            {},
		"struct.histogram":            {},
		"nested.struct.gauge":         {},
		"array.struct.counter.0":      {},
		"array.struct.counter.1":      {},
		"array.struct.counter.3":      {},
		"map.counter.0":               {},
		"map.counter.1":               {},
		"nested.struct.array.1.gauge": {},
	}
	totalMetrics := len(expNames)

	r.Each(func(name string, _ interface{}) {
		if _, exist := expNames[name]; !exist {
			t.Errorf("unexpected name: %s", name)
		}
		delete(expNames, name)
	})
	if len(expNames) > 0 {
		t.Fatalf("missed names: %v", expNames)
	}

	// Test RemoveMetric.
	r.RemoveMetric(topFloatGauge)
	r.RemoveMetric(bottomGauge)
	if g := r.getGauge("bottom.gauge"); g != nil {
		t.Errorf("getGauge returned non-nil %v, expected nil", g)
	}
	count := 0
	r.Each(func(name string, _ interface{}) {
		if name == "top.floatgauge" || name == "bottom.gauge" {
			t.Errorf("unexpected name: %s", name)
		}
		count++
	})
	if count != totalMetrics-2 {
		t.Fatalf("not all metrics are present: returned %d, expected %d", count, totalMetrics-2)
	}

	// Test Select
	selectExpNames := map[string]struct{}{
		"top.histogram":   {},
		"top.gauge":       {},
		"not.in.registry": {},
	}

	r.Select(
		selectExpNames,
		func(name string, _ interface{}) {
			if _, exist := selectExpNames[name]; !exist {
				t.Errorf("unexpected name: %s", name)
			}
			delete(selectExpNames, name)
		})
	if len(selectExpNames) != 1 {
		t.Fatalf("missed or selected names not in registry: %v", selectExpNames)
	}

	// Test get functions
	if g := r.getGauge("top.gauge"); g != topGauge {
		t.Errorf("getGauge returned %v, expected %v", g, topGauge)
	}
	if g := r.getGauge("bad"); g != nil {
		t.Errorf("getGauge returned non-nil %v, expected nil", g)
	}
	if g := r.getGauge("top.histogram"); g != nil {
		t.Errorf("getGauge returned non-nil %v of type %T when requesting non-gauge, expected nil", g, g)
	}

	if c := r.getCounter("top.counter"); c != topCounter {
		t.Errorf("getCounter returned %v, expected %v", c, topCounter)
	}
	if c := r.getCounter("bad"); c != nil {
		t.Errorf("getCounter returned non-nil %v, expected nil", c)
	}
	if c := r.getCounter("top.histogram"); c != nil {
		t.Errorf("getCounter returned non-nil %v of type %T when requesting non-counter, expected nil", c, c)
	}
}

type embedMetricStruct struct {
	ExportedCounter *Counter
}

func (embedMetricStruct) MetricStruct() {}

func TestRegistryPanicsWhenAddingUnexportedMetrics(t *testing.T) {
	if !buildutil.CrdbTestBuild {
		t.Logf("skipping test; crdb_test build tag must be set for this test")
		return
	}

	defer testingSetPanicHandler(func(ctx context.Context, msg string, args ...interface{}) {
		panic(fmt.Sprintf(msg, args...))
	})()

	r := NewRegistry()

	// empty struct -- strange, but okay.
	r.AddMetricStruct(struct{}{})

	// Unexported, non-metrics fields are fine.
	r.AddMetricStruct(struct{ unexportedInt int }{0})

	g := NewGauge(Metadata{Name: "private.struct.gauge"})
	c := NewCounter(Metadata{Name: "private.struct.counter"})

	const unnamedStructName = "<unnamed>"
	unexportedErr := func(parentName, fieldName string) string {

		return fmt.Sprintf("metric field %s.%s (or any of embedded metrics) must be exported", parentName, fieldName)
	}

	// Panics when struct has unexported gauge.
	require.PanicsWithValue(t,
		unexportedErr(unnamedStructName, "unexportedGauge"),
		func() {
			r.AddMetricStruct(struct{ unexportedGauge *Gauge }{g})
		},
	)

	// Panics when we have a mix of exported and unexported metrics.
	require.PanicsWithValue(t,
		unexportedErr(unnamedStructName, "unexportedCounter"),
		func() {
			r.AddMetricStruct(struct {
				ExportedGauge     *Gauge
				unexportedCounter *Counter
			}{g, c})
		},
	)

	// Panics when we have a mix of exported and unexported metrics.
	require.PanicsWithValue(t,
		unexportedErr(unnamedStructName, "unexportedSlice"),
		func() {
			r.AddMetricStruct(struct {
				ExportedGauge   *Gauge
				unexportedSlice []*Counter
			}{g, []*Counter{c}})
		},
	)

	// Panics when embedded struct is unexported.
	require.PanicsWithValue(t,
		unexportedErr(unnamedStructName, "embedMetricStruct"),
		func() {
			r.AddMetricStruct(struct {
				ExportedArray [1]*Counter
				embedMetricStruct
			}{
				ExportedArray: [1]*Counter{c},
				embedMetricStruct: embedMetricStruct{
					ExportedCounter: c,
				},
			})
		},
	)

	// Even though we can't directly embed embedMetricStruct (since it's unexported), we can
	// still use it as exported field since struct implements metric.Struct.
	r.AddMetricStruct(struct{ Embed embedMetricStruct }{Embed: embedMetricStruct{ExportedCounter: c}})

	type EmbedBroken struct {
		ExportedCounter *Counter
	}

	// Panics when embedded struct with metrics did not implement metric.Struct.
	require.PanicsWithValue(t,
		"embedded struct field <unnamed>.EmbedBroken (metric.EmbedBroken) does not implement metric.Struct interface",
		func() {
			r.AddMetricStruct(struct {
				ExportedArray [1]*Counter
				EmbedBroken
			}{
				ExportedArray: [1]*Counter{c},
				EmbedBroken: EmbedBroken{
					ExportedCounter: c,
				},
			})
		},
	)
}

func TestRegistryContains(t *testing.T) {
	r := NewRegistry()
	expectedMetrics := []string{"top.gauge", "struct.counter", "struct.histogram"}

	containsCheckPrefixes := func(m string) bool {
		return r.Contains(m) &&
			r.Contains(fmt.Sprintf("cr.node.%s", m)) &&
			r.Contains(fmt.Sprintf("cr.store.%s", m))
	}

	for _, m := range expectedMetrics {
		if containsCheckPrefixes(m) {
			t.Errorf("unexpected metric: %s", m)
		}
	}

	topGauge := NewGauge(Metadata{Name: "top.gauge"})
	r.AddMetric(topGauge)

	ms := &struct {
		StructCounter   *Counter
		StructHistogram IHistogram
	}{
		StructCounter: NewCounter(Metadata{Name: "struct.counter"}),
		StructHistogram: NewHistogram(HistogramOptions{
			Mode:         HistogramModePrometheus,
			Metadata:     Metadata{Name: "struct.histogram"},
			Duration:     time.Minute,
			BucketConfig: Count1KBuckets,
		}),
	}
	r.AddMetricStruct(ms)

	for _, m := range expectedMetrics {
		if !containsCheckPrefixes(m) {
			t.Errorf("missing metric: %s", m)
		}
	}
}
