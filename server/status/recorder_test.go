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
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/ts"
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
type byStoreDescID []storage.StoreStatus

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

	// Fake descriptors and stats for status summaries.
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
	stats := engine.MVCCStats{
		LiveBytes:       1,
		KeyBytes:        2,
		ValBytes:        3,
		IntentBytes:     4,
		LiveCount:       5,
		KeyCount:        6,
		ValCount:        7,
		IntentCount:     8,
		IntentAge:       9,
		GCBytesAge:      10,
		LastUpdateNanos: 1 * 1E9,
	}

	// Create some registries and add them to the recorder (two at node-level,
	// two at store-level).
	reg1 := metric.NewRegistry()
	reg2 := metric.NewRegistry()
	store1 := fakeStore{
		storeID:  roachpb.StoreID(1),
		stats:    stats,
		desc:     storeDesc1,
		registry: metric.NewRegistry(),
	}
	store2 := fakeStore{
		storeID:  roachpb.StoreID(2),
		stats:    stats,
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

	// Create a flat array of registries, along with metadata for each, to help
	// generate expected results.
	regList := []struct {
		reg    *metric.Registry
		prefix string
		source int64
	}{
		{
			reg:    reg1,
			prefix: "cr.node.one.",
			source: 1,
		},
		{
			reg:    reg2,
			prefix: "cr.node.two.",
			source: 1,
		},
		{
			reg:    store1.registry,
			prefix: "cr.store.",
			source: int64(store1.storeID),
		},
		{
			reg:    store2.registry,
			prefix: "cr.store.",
			source: int64(store2.storeID),
		},
	}

	// Every registry will have the following metrics.
	metricNames := []struct {
		name string
		typ  string
		val  int64
	}{
		{"testGauge", "gauge", 20},
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

	// Add the above metrics to each registry. At the same time, generate
	// expected time series results.
	var expected []ts.TimeSeriesData
	addExpected := func(prefix, name string, source, time, val int64) {
		expect := ts.TimeSeriesData{
			Name:   prefix + name,
			Source: strconv.FormatInt(source, 10),
			Datapoints: []*ts.TimeSeriesDatapoint{
				{
					TimestampNanos: time,
					Value:          float64(val),
				},
			},
		}
		expected = append(expected, expect)
	}

	for _, data := range metricNames {
		for _, reg := range regList {
			switch data.typ {
			case "gauge":
				reg.reg.Gauge(data.name).Update(data.val)
				addExpected(reg.prefix, data.name, reg.source, 100, data.val)
			case "counter":
				reg.reg.Counter(data.name).Inc(data.val)
				addExpected(reg.prefix, data.name, reg.source, 100, data.val)
			case "rate":
				reg.reg.Rates(data.name).Add(data.val)
				addExpected(reg.prefix, data.name+"-count", reg.source, 100, data.val)
				for _, scale := range metric.DefaultTimeScales {
					// Rate data is subject to timing errors in tests. Zero out
					// these values.
					addExpected(reg.prefix, data.name+sep+scale.Name(), reg.source, 100, 0)
				}
			case "histogram":
				reg.reg.Histogram(data.name, time.Second, 1000, 2).RecordValue(data.val)
				for _, q := range recordHistogramQuantiles {
					addExpected(reg.prefix, data.name+q.suffix, reg.source, 100, data.val)
				}
			case "latency":
				reg.reg.Latency(data.name).RecordValue(data.val)
				// Latency is simply three histograms (at different resolution
				// time scales).
				for _, scale := range metric.DefaultTimeScales {
					for _, q := range recordHistogramQuantiles {
						addExpected(reg.prefix, data.name+sep+scale.Name()+q.suffix, reg.source, 100, data.val)
					}
				}
			}
		}
	}

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

	// **** STATUS SUMMARY TESTING
	// Generate an expected node summary and two store summaries. The
	// information here is relatively simple in our test.
	expectedNodeSummary := &NodeStatus{
		Desc:      nodeDesc,
		StartedAt: 50,
		UpdatedAt: 100,
		StoreIDs: []roachpb.StoreID{
			roachpb.StoreID(1),
			roachpb.StoreID(2),
		},
		RangeCount:           2,
		LeaderRangeCount:     2,
		AvailableRangeCount:  2,
		ReplicatedRangeCount: 2,
	}
	expectedStoreSummaries := []storage.StoreStatus{
		{
			Desc:                 storeDesc1,
			NodeID:               roachpb.NodeID(1),
			StartedAt:            50,
			UpdatedAt:            100,
			RangeCount:           1,
			LeaderRangeCount:     1,
			AvailableRangeCount:  1,
			ReplicatedRangeCount: 1,
			Stats:                stats,
		},
		{
			Desc:                 storeDesc2,
			NodeID:               roachpb.NodeID(1),
			StartedAt:            50,
			UpdatedAt:            100,
			RangeCount:           1,
			LeaderRangeCount:     1,
			AvailableRangeCount:  1,
			ReplicatedRangeCount: 1,
			Stats:                stats,
		},
	}
	for _, ss := range expectedStoreSummaries {
		expectedNodeSummary.Stats.Add(ss.Stats)
	}

	nodeSummary, storeSummaries := recorder.GetStatusSummaries()
	if nodeSummary == nil {
		t.Fatalf("recorder did not return nodeSummary.")
	}
	if storeSummaries == nil {
		t.Fatalf("recorder did not return storeSummaries.")
	}

	sort.Sort(byStoreDescID(storeSummaries))
	sort.Sort(byStoreID(nodeSummary.StoreIDs))
	if a, e := nodeSummary, expectedNodeSummary; !reflect.DeepEqual(a, e) {
		t.Errorf("recorder did not produce expected NodeSummary; diff:\n %v", pretty.Diff(e, a))
	}
	if a, e := storeSummaries, expectedStoreSummaries; !reflect.DeepEqual(a, e) {
		t.Errorf("recorder did not produce expected StoreSummaries; diff:\n %v", pretty.Diff(e, a))
	}
}
