// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Matt Tracy (matt.r.tracy@gmail.com)

package status

import (
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/kr/pretty"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/ts"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/metric"
)

const sep = "-"

// byTimeAndName is a slice of ts.TimeSeriesData.
type byTimeAndName []ts.TimeSeriesData

// implement sort.Interface for byTimeAndName
func (a byTimeAndName) Len() int      { return len(a) }
func (a byTimeAndName) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byTimeAndName) Less(i, j int) bool {
	if a[i].Name != a[j].Name {
		return a[i].Name < a[j].Name
	}
	if a[i].Datapoints[0].TimestampNanos != a[j].Datapoints[0].TimestampNanos {
		return a[i].Datapoints[0].TimestampNanos < a[j].Datapoints[0].TimestampNanos
	}
	return a[i].Source < a[j].Source
}

var _ sort.Interface = byTimeAndName{}

// byStoreID is a slice of roachpb.StoreID.
type byStoreID []roachpb.StoreID

// implement sort.Interface for byStoreID
func (a byStoreID) Len() int      { return len(a) }
func (a byStoreID) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byStoreID) Less(i, j int) bool {
	return a[i] < a[j]
}

var _ sort.Interface = byStoreID{}

// byStoreDescID is a slice of storage.StoreStatus
type byStoreDescID []StoreStatus

// implement sort.Interface for byStoreDescID.
func (a byStoreDescID) Len() int      { return len(a) }
func (a byStoreDescID) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byStoreDescID) Less(i, j int) bool {
	return a[i].Desc.StoreID < a[j].Desc.StoreID
}

var _ sort.Interface = byStoreDescID{}

// fakeStore implements only the methods of store needed by MetricsRecorder to
// interact with stores.
type fakeStore struct {
	storeID  roachpb.StoreID
	stats    engine.MVCCStats
	desc     roachpb.StoreDescriptor
	registry *metric.Registry
}

func (fs fakeStore) StoreID() roachpb.StoreID {
	return fs.storeID
}

func (fs fakeStore) Descriptor() (*roachpb.StoreDescriptor, error) {
	return &fs.desc, nil
}

func (fs fakeStore) MVCCStats() engine.MVCCStats {
	return fs.stats
}

func (fs fakeStore) Registry() *metric.Registry {
	return fs.registry
}

// TestMetricsRecorder verifies that the metrics recorder properly formats the
// statistics from various registries, both for Time Series and for Status
// Summaries.
func TestMetricsRecorder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// ========================================
	// Construct a series of fake descriptors for use in test.
	// ========================================
	nodeDesc := roachpb.NodeDescriptor{
		NodeID: roachpb.NodeID(1),
	}
	storeDesc1 := roachpb.StoreDescriptor{
		StoreID: roachpb.StoreID(1),
		Capacity: roachpb.StoreCapacity{
			Capacity:  100,
			Available: 50,
		},
	}
	storeDesc2 := roachpb.StoreDescriptor{
		StoreID: roachpb.StoreID(2),
		Capacity: roachpb.StoreCapacity{
			Capacity:  200,
			Available: 75,
		},
	}

	// ========================================
	// Create registries and add them to the recorder (two node-level, two
	// store-level).
	// ========================================
	reg1 := metric.NewRegistry()
	reg2 := metric.NewRegistry()
	store1 := fakeStore{
		storeID:  roachpb.StoreID(1),
		desc:     storeDesc1,
		registry: metric.NewRegistry(),
	}
	store2 := fakeStore{
		storeID:  roachpb.StoreID(2),
		desc:     storeDesc2,
		registry: metric.NewRegistry(),
	}
	manual := hlc.NewManualClock(100)
	recorder := NewMetricsRecorder(hlc.NewClock(manual.UnixNano))
	recorder.AddNodeRegistry("one.%s", reg1)
	recorder.AddNodeRegistry("two.%s", reg1)
	recorder.AddStore(store1)
	recorder.AddStore(store2)
	recorder.NodeStarted(nodeDesc, 50)

	// Ensure the metric system's view of time does not advance during this test
	// as the test expects time to not advance too far which would age the actual
	// data (e.g. in histogram's) unexpectedly.
	defer metric.TestingSetNow(func() time.Time {
		return time.Unix(0, manual.UnixNano()).UTC()
	})()

	// ========================================
	// Generate Metrics Data & Expected Results
	// ========================================

	// Flatten the four registries into an array for ease of use.
	regList := []struct {
		reg    *metric.Registry
		prefix string
		source int64
		isNode bool
	}{
		{
			reg:    reg1,
			prefix: "one.",
			source: 1,
			isNode: true,
		},
		{
			reg:    reg2,
			prefix: "two.",
			source: 1,
			isNode: true,
		},
		{
			reg:    store1.registry,
			prefix: "",
			source: int64(store1.storeID),
			isNode: false,
		},
		{
			reg:    store2.registry,
			prefix: "",
			source: int64(store2.storeID),
			isNode: false,
		},
	}

	// Every registry will have a copy of the following metrics.
	metricNames := []struct {
		name string
		typ  string
		val  int64
	}{
		{"testGauge", "gauge", 20},
		{"testGaugeFloat64", "floatgauge", 20},
		{"testCounter", "counter", 5},
		{"testRate", "rate", 2},
		{"testHistogram", "histogram", 10},
		{"testLatency", "latency", 10},

		// Stats needed for store summaries.
		{"ranges", "counter", 1},
		{"ranges.leader", "gauge", 1},
		{"ranges.replicated", "gauge", 1},
		{"ranges.available", "gauge", 1},
	}

	// Add the metrics to each registry and set their values. At the same time,
	// generate expected time series results and status summary metric values.
	var expected []ts.TimeSeriesData
	expectedNodeSummaryMetrics := make(map[string]float64)
	expectedStoreSummaryMetrics := make(map[string]float64)

	// addExpected generates expected data for a single metric data point.
	addExpected := func(prefix, name string, source, time, val int64, isNode bool) {
		// Generate time series data.
		tsPrefix := "cr.node."
		if !isNode {
			tsPrefix = "cr.store."
		}
		expect := ts.TimeSeriesData{
			Name:   tsPrefix + prefix + name,
			Source: strconv.FormatInt(source, 10),
			Datapoints: []ts.TimeSeriesDatapoint{
				{
					TimestampNanos: time,
					Value:          float64(val),
				},
			},
		}
		expected = append(expected, expect)

		// Generate status summary data.
		if isNode {
			expectedNodeSummaryMetrics[prefix+name] = float64(val)
		} else {
			// This can overwrite the previous value, but this is expected as
			// all stores in our tests have identical values; when comparing
			// status summaries, the same map is used as expected data for all
			// stores.
			expectedStoreSummaryMetrics[prefix+name] = float64(val)
		}
	}

	for _, reg := range regList {
		for _, data := range metricNames {
			switch data.typ {
			case "gauge":
				reg.reg.Gauge(data.name).Update(data.val)
				addExpected(reg.prefix, data.name, reg.source, 100, data.val, reg.isNode)
			case "floatgauge":
				reg.reg.GaugeFloat64(data.name).Update(float64(data.val))
				addExpected(reg.prefix, data.name, reg.source, 100, data.val, reg.isNode)
			case "counter":
				reg.reg.Counter(data.name).Inc(data.val)
				addExpected(reg.prefix, data.name, reg.source, 100, data.val, reg.isNode)
			case "rate":
				reg.reg.Rates(data.name).Add(data.val)
				addExpected(reg.prefix, data.name+"-count", reg.source, 100, data.val, reg.isNode)
				for _, scale := range metric.DefaultTimeScales {
					// Rate data is subject to timing errors in tests. Zero out
					// these values.
					addExpected(reg.prefix, data.name+sep+scale.Name(), reg.source, 100, 0, reg.isNode)
				}
			case "histogram":
				reg.reg.Histogram(data.name, time.Second, 1000, 2).RecordValue(data.val)
				for _, q := range recordHistogramQuantiles {
					addExpected(reg.prefix, data.name+q.suffix, reg.source, 100, data.val, reg.isNode)
				}
			case "latency":
				reg.reg.Latency(data.name).RecordValue(data.val)
				// Latency is simply three histograms (at different resolution
				// time scales).
				for _, scale := range metric.DefaultTimeScales {
					for _, q := range recordHistogramQuantiles {
						addExpected(reg.prefix, data.name+sep+scale.Name()+q.suffix, reg.source, 100, data.val, reg.isNode)
					}
				}
			}
		}
	}

	// ========================================
	// Verify time series data
	// ========================================
	actual := recorder.GetTimeSeriesData()

	// Zero-out timing-sensitive rate values from actual data.
	for _, act := range actual {
		match, err := regexp.MatchString(`testRate-\d+m`, act.Name)
		if err != nil {
			t.Fatal(err)
		}
		if match {
			act.Datapoints[0].Value = 0.0
		}
	}

	// Actual comparison is simple: sort the resulting arrays by time and name,
	// and use reflect.DeepEqual.
	sort.Sort(byTimeAndName(actual))
	sort.Sort(byTimeAndName(expected))
	if a, e := actual, expected; !reflect.DeepEqual(a, e) {
		t.Errorf("recorder did not yield expected time series collection; diff:\n %v", pretty.Diff(e, a))
	}

	// ========================================
	// Verify node summary generation
	// ========================================
	expectedNodeSummary := &NodeStatus{
		Desc:      nodeDesc,
		BuildInfo: util.GetBuildInfo(),
		StartedAt: 50,
		UpdatedAt: 100,
		Metrics:   expectedNodeSummaryMetrics,
		StoreStatuses: []StoreStatus{
			{
				Desc:    storeDesc1,
				Metrics: expectedStoreSummaryMetrics,
			},
			{
				Desc:    storeDesc2,
				Metrics: expectedStoreSummaryMetrics,
			},
		},
	}

	nodeSummary := recorder.GetStatusSummary()
	if nodeSummary == nil {
		t.Fatalf("recorder did not return nodeSummary.")
	}

	sort.Sort(byStoreDescID(nodeSummary.StoreStatuses))
	if a, e := nodeSummary, expectedNodeSummary; !reflect.DeepEqual(a, e) {
		t.Errorf("recorder did not produce expected NodeSummary; diff:\n %v", pretty.Diff(e, a))
	}
}
