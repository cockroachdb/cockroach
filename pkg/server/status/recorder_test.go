// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package status

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/ts/tsutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/system"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/kr/pretty"
	prometheusgo "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"
)

// byTimeAndName is a slice of tspb.TimeSeriesData.
type byTimeAndName []tspb.TimeSeriesData

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
type byStoreDescID []statuspb.StoreStatus

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
	desc     roachpb.StoreDescriptor
	registry *metric.Registry
}

func (fs fakeStore) StoreID() roachpb.StoreID {
	return fs.storeID
}

func (fs fakeStore) Descriptor(_ context.Context, _ bool) (*roachpb.StoreDescriptor, error) {
	return &fs.desc, nil
}

func (fs fakeStore) Registry() *metric.Registry {
	return fs.registry
}

func TestMetricsRecorderLabels(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nodeDesc := roachpb.NodeDescriptor{
		NodeID: roachpb.NodeID(7),
	}
	reg1 := metric.NewRegistry()
	manual := timeutil.NewManualTime(timeutil.Unix(0, 100))
	st := cluster.MakeTestingClusterSettings()
	recorder := NewMetricsRecorder(
		roachpb.SystemTenantID,
		roachpb.NewTenantNameContainer(catconstants.SystemTenantName),
		nil, /* nodeLiveness */
		nil, /* remoteClocks */
		manual,
		st,
	)
	appReg := metric.NewRegistry()
	logReg := metric.NewRegistry()
	sysReg := metric.NewRegistry()
	recorder.AddNode(reg1, appReg, logReg, sysReg, nodeDesc, 50, "foo:26257", "foo:26258", "foo:5432")

	nodeDescTenant := roachpb.NodeDescriptor{
		NodeID: roachpb.NodeID(7),
	}
	regTenant := metric.NewRegistry()
	stTenant := cluster.MakeTestingClusterSettings()
	tenantID, err := roachpb.MakeTenantID(123)
	require.NoError(t, err)

	appNameContainer := roachpb.NewTenantNameContainer("application")
	recorderTenant := NewMetricsRecorder(
		tenantID,
		appNameContainer,
		nil, /* nodeLiveness */
		nil, /* remoteClocks */
		manual,
		stTenant,
	)
	recorderTenant.AddNode(
		regTenant,
		appReg, logReg, sysReg, nodeDescTenant, 50, "foo:26257", "foo:26258", "foo:5432")

	// ========================================
	// Verify that the recorder exports metrics for tenants as text.
	// ========================================

	g := metric.NewGauge(metric.Metadata{Name: "some_metric"})
	reg1.AddMetric(g)
	g.Update(123)

	g2 := metric.NewGauge(metric.Metadata{Name: "some_metric"})
	regTenant.AddMetric(g2)
	g2.Update(456)

	c1 := metric.NewCounter(metric.Metadata{Name: "some_log_metric"})
	logReg.AddMetric(c1)
	c1.Inc(2)

	recorder.AddTenantRegistry(tenantID, regTenant)

	buf := bytes.NewBuffer([]byte{})
	err = recorder.PrintAsText(buf, expfmt.FmtText, false)
	require.NoError(t, err)

	require.Contains(t, buf.String(), `some_metric{node_id="7",tenant="system"} 123`)
	require.Contains(t, buf.String(), `some_metric{node_id="7",tenant="application"} 456`)

	bufTenant := bytes.NewBuffer([]byte{})
	err = recorderTenant.PrintAsText(bufTenant, expfmt.FmtText, false)
	require.NoError(t, err)

	require.NotContains(t, bufTenant.String(), `some_metric{node_id="7",tenant="system"} 123`)
	require.Contains(t, bufTenant.String(), `some_metric{node_id="7",tenant="application"} 456`)

	// Update app name in container and ensure
	// output changes accordingly.
	appNameContainer.Set("application2")

	buf = bytes.NewBuffer([]byte{})
	err = recorder.PrintAsText(buf, expfmt.FmtText, false)
	require.NoError(t, err)

	require.Contains(t, buf.String(), `some_metric{node_id="7",tenant="system"} 123`)
	require.Contains(t, buf.String(), `some_metric{node_id="7",tenant="application2"} 456`)

	bufTenant = bytes.NewBuffer([]byte{})
	err = recorderTenant.PrintAsText(bufTenant, expfmt.FmtText, false)
	require.NoError(t, err)

	require.NotContains(t, bufTenant.String(), `some_metric{node_id="7",tenant="system"} 123`)
	require.Contains(t, bufTenant.String(), `some_metric{node_id="7",tenant="application2"} 456`)

	// ========================================
	// Verify that the recorder processes tenant time series registries
	// ========================================

	expectedData := []tspb.TimeSeriesData{
		// System tenant metrics
		{
			Name:   "cr.node.node-id",
			Source: "7",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{
					TimestampNanos: manual.Now().UnixNano(),
					Value:          float64(7),
				},
			},
		},
		{
			Name:   "cr.node.some_metric",
			Source: "7",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{
					TimestampNanos: manual.Now().UnixNano(),
					Value:          float64(123),
				},
			},
		},
		{
			Name:   "cr.node.some_log_metric",
			Source: "7",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{
					TimestampNanos: manual.Now().UnixNano(),
					Value:          float64(2),
				},
			},
		},
		// App tenant metrics
		{
			Name:   "cr.node.node-id",
			Source: "7-123",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{
					TimestampNanos: manual.Now().UnixNano(),
					Value:          float64(nodeDesc.NodeID),
				},
			},
		},
		{
			Name:   "cr.node.some_metric",
			Source: "7-123",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{
					TimestampNanos: manual.Now().UnixNano(),
					Value:          float64(456),
				},
			},
		},
	}

	actualData := recorder.GetTimeSeriesData(false)

	// compare actual vs expected values
	sort.Sort(byTimeAndName(actualData))
	sort.Sort(byTimeAndName(expectedData))
	if a, e := actualData, expectedData; !reflect.DeepEqual(a, e) {
		t.Errorf("recorder did not yield expected time series collection; diff:\n %v", pretty.Diff(e, a))
	}

	// ========================================
	// Verify that GetTimeSeriesData with includeChildMetrics=true returns child labels
	// ========================================
	// Add aggmetrics with child labels to the app registry
	aggCounter := aggmetric.NewCounter(
		metric.Metadata{
			Name:     "changefeed.emitted_messages",
			Category: metric.Metadata_CHANGEFEEDS,
		},
		"some-id", "feed_id",
	)
	appReg.AddMetric(aggCounter)
	// Add child metrics with specific labels
	child1 := aggCounter.AddChild("123", "feed-a")
	child1.Inc(100)
	child2 := aggCounter.AddChild("456", "feed-b")
	child2.Inc(200)

	aggGauge := aggmetric.NewGauge(
		metric.Metadata{
			Name:     "changefeed.lagging_ranges",
			Category: metric.Metadata_CHANGEFEEDS,
		},
		"db", "status!!",
	)
	appReg.AddMetric(aggGauge)
	// Add child metrics with different label combinations
	child3 := aggGauge.AddChild("mine", "active")
	child3.Update(1)
	child4 := aggGauge.AddChild("yours", "paused")
	child4.Update(0)

	aggHistogram := aggmetric.NewHistogram(
		metric.HistogramOptions{
			Metadata: metric.Metadata{
				Name:     "changefeed.stage.downstream_client_send.latency",
				Category: metric.Metadata_CHANGEFEEDS,
			},
			Duration:     10 * time.Second,
			BucketConfig: metric.IOLatencyBuckets,
			Mode:         metric.HistogramModePrometheus,
		},
		"scope",
	)
	appReg.AddMetric(aggHistogram)
	// Add child histogram metrics
	child5 := aggHistogram.AddChild("default")
	child5.RecordValue(100)
	child5.RecordValue(200)
	child6 := aggHistogram.AddChild("user")
	child6.RecordValue(300)
	child6.RecordValue(400)
	child6.RecordValue(500)

	// Enable the cluster setting for child metrics storage
	ChildMetricsStorageEnabled.Override(context.Background(), &st.SV, true)

	// Get time series data with child metrics enabled
	childData := recorder.GetTimeSeriesData(true)

	appMetricTestCases := []struct {
		name           string
		metricPrefix   string
		labelMatchers  []string
		expectedSource string
		expectedValue  float64
	}{
		{
			name:           "counter feed-a with some_id=123",
			metricPrefix:   "cr.node.changefeed.emitted_messages",
			labelMatchers:  []string{"feed_id=\"feed-a\"", "some_id=\"123\""},
			expectedSource: "7",
			expectedValue:  100,
		},
		{
			name:           "counter feed-b with some_id=456",
			metricPrefix:   "cr.node.changefeed.emitted_messages",
			labelMatchers:  []string{"feed_id=\"feed-b\"", "some_id=\"456\""},
			expectedSource: "7",
			expectedValue:  200,
		},
		{
			name:           "gauge mine with status=active",
			metricPrefix:   "cr.node.changefeed.lagging_ranges",
			labelMatchers:  []string{"db=\"mine\"", "status__=\"active\""},
			expectedSource: "7",
			expectedValue:  1,
		},
		{
			name:           "gauge yours with status=paused",
			metricPrefix:   "cr.node.changefeed.lagging_ranges",
			labelMatchers:  []string{"db=\"yours\"", "status__=\"paused\""},
			expectedSource: "7",
			expectedValue:  0,
		},
		{
			name:           "histogram default -count",
			metricPrefix:   "cr.node.changefeed.stage.downstream_client_send.latency",
			labelMatchers:  []string{"scope=\"default\"", "-count"},
			expectedSource: "7",
			expectedValue:  2,
		},
		{
			name:           "histogram user -count",
			metricPrefix:   "cr.node.changefeed.stage.downstream_client_send.latency",
			labelMatchers:  []string{"scope=\"user\"", "-count"},
			expectedSource: "7",
			expectedValue:  3,
		},
		{
			name:           "histogram default -avg",
			metricPrefix:   "cr.node.changefeed.stage.downstream_client_send.latency",
			labelMatchers:  []string{"scope=\"default\"", "-avg"},
			expectedSource: "7",
			expectedValue:  150,
		},
		{
			name:           "histogram user -avg",
			metricPrefix:   "cr.node.changefeed.stage.downstream_client_send.latency",
			labelMatchers:  []string{"scope=\"user\"", "-avg"},
			expectedSource: "7",
			expectedValue:  400,
		},
	}

	for _, tc := range appMetricTestCases {
		var found bool
		for _, ts := range childData {
			if !strings.Contains(ts.Name, tc.metricPrefix) {
				continue
			}
			allMatch := true
			for _, matcher := range tc.labelMatchers {
				if !strings.Contains(ts.Name, matcher) {
					allMatch = false
					break
				}
			}
			if !allMatch {
				continue
			}
			found = true
			require.Equal(t, tc.expectedSource, ts.Source, "Expected source for %s", tc.name)
			require.Len(t, ts.Datapoints, 1, "Expected 1 datapoint for %s", tc.name)
			require.Equal(t, tc.expectedValue, ts.Datapoints[0].Value, "Expected value for %s", tc.name)
			break
		}
		require.True(t, found, "Expected to find %s", tc.name)
	}

	// ========================================
	// Verify that tenant changefeed child metrics are collected with proper source
	// ========================================

	// Add changefeed aggmetrics to the tenant registry with TsdbRecordLabeled
	tsdbRecordLabeled := true
	tenantAggCounter := aggmetric.NewCounter(
		metric.Metadata{
			Name:              "changefeed.emitted_messages",
			TsdbRecordLabeled: &tsdbRecordLabeled,
			Category:          metric.Metadata_CHANGEFEEDS,
		},
		"scope", "feed_id",
	)
	regTenant.AddMetric(tenantAggCounter)

	// Add child metrics for different changefeeds in the tenant
	tenantChild1 := tenantAggCounter.AddChild("default", "tenant-feed-1")
	tenantChild1.Inc(500)
	tenantChild2 := tenantAggCounter.AddChild("user", "tenant-feed-2")
	tenantChild2.Inc(750)

	tenantAggGauge := aggmetric.NewGauge(
		metric.Metadata{
			Name:              "changefeed.backfill_pending_ranges",
			TsdbRecordLabeled: &tsdbRecordLabeled,
			Category:          metric.Metadata_CHANGEFEEDS,
		},
		"scope",
	)
	regTenant.AddMetric(tenantAggGauge)

	tenantChild3 := tenantAggGauge.AddChild("default")
	tenantChild3.Update(2)
	tenantChild4 := tenantAggGauge.AddChild("user")
	tenantChild4.Update(1)

	// Get time series data with child metrics enabled
	tenantChildData := recorder.GetTimeSeriesData(true)

	tenantMetricTestCases := []struct {
		name           string
		metricPrefix   string
		labelMatchers  []string
		expectedSource string
		expectedValue  float64
	}{
		{
			name:           "tenant counter default with tenant-feed-1",
			metricPrefix:   "cr.node.changefeed.emitted_messages",
			labelMatchers:  []string{`scope="default"`, `feed_id="tenant-feed-1"`},
			expectedSource: "7-123",
			expectedValue:  500,
		},
		{
			name:           "tenant counter user with tenant-feed-2",
			metricPrefix:   "cr.node.changefeed.emitted_messages",
			labelMatchers:  []string{`scope="user"`, `feed_id="tenant-feed-2"`},
			expectedSource: "7-123",
			expectedValue:  750,
		},
		{
			name:           "tenant gauge default",
			metricPrefix:   "cr.node.changefeed.backfill_pending_ranges",
			labelMatchers:  []string{`scope="default"`},
			expectedSource: "7-123",
			expectedValue:  2,
		},
		{
			name:           "tenant gauge user",
			metricPrefix:   "cr.node.changefeed.backfill_pending_ranges",
			labelMatchers:  []string{`scope="user"`},
			expectedSource: "7-123",
			expectedValue:  1,
		},
	}

	for _, tc := range tenantMetricTestCases {
		var found bool
		for _, ts := range tenantChildData {
			if !strings.Contains(ts.Name, tc.metricPrefix) {
				continue
			}
			// Check if all label matchers are present
			allMatch := true
			for _, matcher := range tc.labelMatchers {
				if !strings.Contains(ts.Name, matcher) {
					allMatch = false
					break
				}
			}
			if !allMatch {
				continue
			}
			found = true
			require.Equal(t, tc.expectedSource, ts.Source, "Expected source for %s", tc.name)
			require.Len(t, ts.Datapoints, 1, "Expected 1 datapoint for %s", tc.name)
			require.Equal(t, tc.expectedValue, ts.Datapoints[0].Value, "Expected value for %s", tc.name)
			break
		}
		require.True(t, found, "Expected to find %s", tc.name)
	}
}

func TestRegistryRecorder_RecordChild(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store1 := fakeStore{
		storeID: roachpb.StoreID(1),
		desc: roachpb.StoreDescriptor{
			StoreID: roachpb.StoreID(1),
			Capacity: roachpb.StoreCapacity{
				Capacity:  100,
				Available: 50,
				Used:      50,
			},
		},
		registry: metric.NewRegistry(),
	}
	store2 := fakeStore{
		storeID: roachpb.StoreID(2),
		desc: roachpb.StoreDescriptor{
			StoreID: roachpb.StoreID(2),
			Capacity: roachpb.StoreCapacity{
				Capacity:  200,
				Available: 75,
				Used:      125,
			},
		},
		registry: metric.NewRegistry(),
	}
	systemTenantNameContainer := roachpb.NewTenantNameContainer(catconstants.SystemTenantName)
	manual := timeutil.NewManualTime(timeutil.Unix(0, 100))
	st := cluster.MakeTestingClusterSettings()
	recorder := NewMetricsRecorder(roachpb.SystemTenantID, systemTenantNameContainer, nil, nil, manual, st)
	recorder.AddStore(store1)
	recorder.AddStore(store2)

	tenantIDs := []string{"2", "3"}
	type childMetric struct {
		tenantID string
		value    int64
	}
	type testMetric struct {
		name     string
		typ      string
		children []childMetric
	}
	// Each registry will have a copy of the following metrics.
	metrics := []testMetric{
		{
			name: "testAggGauge",
			typ:  "agggauge",
			children: []childMetric{
				{
					tenantID: "2",
					value:    2,
				},
				{
					tenantID: "3",
					value:    5,
				},
			},
		},
		{
			name: "testAggCounter",
			typ:  "aggcounter",
			children: []childMetric{
				{
					tenantID: "2",
					value:    10,
				},
				{
					tenantID: "3",
					value:    17,
				},
			},
		},
	}

	var expected []tspb.TimeSeriesData
	// addExpected generates expected TimeSeriesData for all child metrics.
	addExpected := func(storeID string, metric *testMetric) {
		for _, child := range metric.children {
			expect := tspb.TimeSeriesData{
				Name:   "cr.store." + metric.name,
				Source: tsutil.MakeTenantSource(storeID, child.tenantID),
				Datapoints: []tspb.TimeSeriesDatapoint{
					{
						TimestampNanos: 100,
						Value:          float64(child.value),
					},
				},
			}
			expected = append(expected, expect)
		}
	}

	tIDLabel := multitenant.TenantIDLabel
	for _, store := range []fakeStore{store1, store2} {
		for _, m := range metrics {
			switch m.typ {
			case "aggcounter":
				ac := aggmetric.NewCounter(metric.Metadata{Name: m.name}, tIDLabel)
				store.registry.AddMetric(ac)
				for _, cm := range m.children {
					c := ac.AddChild(cm.tenantID)
					c.Inc(cm.value)
				}
				addExpected(store.storeID.String(), &m)
			case "agggauge":
				ag := aggmetric.NewGauge(metric.Metadata{Name: m.name}, tIDLabel)
				store.registry.AddMetric(ag)
				for _, cm := range m.children {
					c := ag.AddChild(cm.tenantID)
					c.Inc(cm.value)
				}
				addExpected(store.storeID.String(), &m)
			}
		}
	}
	metricFilter := map[string]struct{}{
		"testAggGauge":   {},
		"testAggCounter": {},
	}
	actual := make([]tspb.TimeSeriesData, 0)
	for _, store := range []fakeStore{store1, store2} {
		for _, tID := range tenantIDs {
			tenantStoreRecorder := registryRecorder{
				registry:       store.registry,
				format:         storeTimeSeriesPrefix,
				source:         tsutil.MakeTenantSource(store.storeID.String(), tID),
				timestampNanos: 100,
			}
			tenantStoreRecorder.recordChild(&actual, metricFilter, &prometheusgo.LabelPair{
				Name:  &tIDLabel,
				Value: &tID,
			})
		}
	}
	sort.Sort(byTimeAndName(actual))
	sort.Sort(byTimeAndName(expected))
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("registryRecorder did not yield expected time series collection for child metrics; diff:\n %v", pretty.Diff(actual, expected))
	}
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
			Used:      50,
		},
	}
	storeDesc2 := roachpb.StoreDescriptor{
		StoreID: roachpb.StoreID(2),
		Capacity: roachpb.StoreCapacity{
			Capacity:  200,
			Available: 75,
			Used:      125,
		},
	}

	// ========================================
	// Create registries and add them to the recorder (two node-level, two
	// store-level).
	// ========================================
	reg1 := metric.NewRegistry()
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
	manual := timeutil.NewManualTime(timeutil.Unix(0, 100))
	st := cluster.MakeTestingClusterSettings()
	recorder := NewMetricsRecorder(roachpb.SystemTenantID, roachpb.NewTenantNameContainer(""), nil, nil, manual, st)
	recorder.AddStore(store1)
	recorder.AddStore(store2)
	appReg := metric.NewRegistry()
	logReg := metric.NewRegistry()
	sysReg := metric.NewRegistry()
	recorder.AddNode(reg1, appReg, logReg, sysReg, nodeDesc, 50, "foo:26257", "foo:26258", "foo:5432")

	// Ensure the metric system's view of time does not advance during this test
	// as the test expects time to not advance too far which would age the actual
	// data (e.g. in histogram's) unexpectedly.
	defer metric.TestingSetNow(func() time.Time {
		return manual.Now()
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
			reg:    reg1,
			prefix: "two.",
			source: 1,
			isNode: true,
		},
		{
			reg:    logReg,
			prefix: "log.",
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
		{"testHistogram", "histogram", 10},
		{"testAggGauge", "agggauge", 4},
		{"testAggCounter", "aggcounter", 7},
		{"testCounterVec", "counterVec", 7},
		{"testGaugeVec", "gaugeVec", 7},
		{"testHistogramVec", "histogramVec", 7},

		// Stats needed for store summaries.
		{"replicas.leaders", "gauge", 1},
		{"replicas.leaseholders", "gauge", 1},
		{"ranges", "gauge", 1},
		{"ranges.unavailable", "gauge", 1},
		{"ranges.underreplicated", "gauge", 1},
	}

	// Add the metrics to each registry and set their values. At the same time,
	// generate expected time series results and status summary metric values.
	var expected []tspb.TimeSeriesData
	expectedNodeSummaryMetrics := make(map[string]float64)
	expectedStoreSummaryMetrics := make(map[string]float64)

	// addExpected generates expected data for a single metric data point.
	addExpected := func(prefix, name string, source, time, val int64, isNode bool) {
		// Generate time series data.
		tsPrefix := "cr.node."
		if !isNode {
			tsPrefix = "cr.store."
		}
		expect := tspb.TimeSeriesData{
			Name:   tsPrefix + prefix + name,
			Source: strconv.FormatInt(source, 10),
			Datapoints: []tspb.TimeSeriesDatapoint{
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

	// Add metric for node ID.
	g := metric.NewGauge(metric.Metadata{Name: "node-id"})
	g.Update(int64(nodeDesc.NodeID))
	addExpected("", "node-id", 1, 100, g.Value(), true)

	for _, reg := range regList {
		for _, data := range metricNames {
			switch data.typ {
			case "gauge":
				g := metric.NewGauge(metric.Metadata{Name: reg.prefix + data.name})
				reg.reg.AddMetric(g)
				g.Update(data.val)
				addExpected(reg.prefix, data.name, reg.source, 100, data.val, reg.isNode)
			case "floatgauge":
				g := metric.NewGaugeFloat64(metric.Metadata{Name: reg.prefix + data.name})
				reg.reg.AddMetric(g)
				g.Update(float64(data.val))
				addExpected(reg.prefix, data.name, reg.source, 100, data.val, reg.isNode)
			case "counter":
				c := metric.NewCounter(metric.Metadata{Name: reg.prefix + data.name})
				reg.reg.AddMetric(c)
				c.Inc((data.val))
				addExpected(reg.prefix, data.name, reg.source, 100, data.val, reg.isNode)
			case "aggcounter":
				ac := aggmetric.NewCounter(metric.Metadata{Name: reg.prefix + data.name}, "foo")
				reg.reg.AddMetric(ac)
				c := ac.AddChild("bar")
				c.Inc((data.val))
				addExpected(reg.prefix, data.name, reg.source, 100, data.val, reg.isNode)
			case "agggauge":
				ac := aggmetric.NewGauge(metric.Metadata{Name: reg.prefix + data.name}, "foo")
				reg.reg.AddMetric(ac)
				c := ac.AddChild("bar")
				c.Inc((data.val))
				addExpected(reg.prefix, data.name, reg.source, 100, data.val, reg.isNode)
			case "histogram":
				h := metric.NewHistogram(metric.HistogramOptions{
					Metadata: metric.Metadata{Name: reg.prefix + data.name},
					Duration: time.Second,
					Buckets:  []float64{1.0, 10.0, 100.0, 1000.0},
					Mode:     metric.HistogramModePrometheus,
				})
				reg.reg.AddMetric(h)
				h.RecordValue(data.val)
				for _, q := range metric.HistogramMetricComputers {
					if !q.IsSummaryMetric {
						continue
					}
					addExpected(reg.prefix, data.name+q.Suffix, reg.source, 100, 10, reg.isNode)
				}
				addExpected(reg.prefix, data.name+"-count", reg.source, 100, 1, reg.isNode)
				addExpected(reg.prefix, data.name+"-sum", reg.source, 100, 10, reg.isNode)
			case "counterVec":
				// Note that we don't call addExpected for this case. metric.PrometheusVector
				// metrics should not be recorded into TSDB.
				cv := metric.NewExportedCounterVec(metric.Metadata{Name: reg.prefix + data.name}, []string{"label1"})
				reg.reg.AddMetric(cv)
				cv.Inc(map[string]string{"label1": "label1"}, data.val)
			case "gaugeVec":
				// Note that we don't call addExpected for this case. metric.PrometheusVector
				// metrics should not be recorded into TSDB.
				gv := metric.NewExportedGaugeVec(metric.Metadata{Name: reg.prefix + data.name}, []string{"label1"})
				reg.reg.AddMetric(gv)
				gv.Update(map[string]string{"label1": "label1"}, data.val)
			case "histogramVec":
				// Note that we don't call addExpected for this case. metric.PrometheusVector
				// metrics should not be recorded into TSDB.
				hv := metric.NewExportedHistogramVec(
					metric.Metadata{Name: reg.prefix + data.name},
					metric.IOLatencyBuckets,
					[]string{"label1"},
				)
				reg.reg.AddMetric(hv)
				hv.Observe(map[string]string{"label1": "label1"}, float64(data.val))
			default:
				t.Fatalf("unexpected: %+v", data)
			}
		}
	}

	// ========================================
	// Verify time series data
	// ========================================
	actual := recorder.GetTimeSeriesData(false)

	// Actual comparison is simple: sort the resulting arrays by time and name,
	// and use reflect.DeepEqual.
	sort.Sort(byTimeAndName(actual))
	sort.Sort(byTimeAndName(expected))
	if a, e := actual, expected; !reflect.DeepEqual(a, e) {
		t.Errorf("recorder did not yield expected time series collection; diff:\n %v", pretty.Diff(e, a))
	}

	totalMemory, err := GetTotalMemory(context.Background())
	if err != nil {
		t.Error("couldn't get total memory", err)
	}

	// ========================================
	// Verify node summary generation
	// ========================================
	expectedNodeSummary := &statuspb.NodeStatus{
		Desc:      nodeDesc,
		BuildInfo: build.GetInfo(),
		StartedAt: 50,
		UpdatedAt: 100,
		Metrics:   expectedNodeSummaryMetrics,
		StoreStatuses: []statuspb.StoreStatus{
			{
				Desc:    storeDesc1,
				Metrics: expectedStoreSummaryMetrics,
			},
			{
				Desc:    storeDesc2,
				Metrics: expectedStoreSummaryMetrics,
			},
		},
		TotalSystemMemory: totalMemory,
		NumCpus:           int32(system.NumCPU()),
		NumVcpus:          GetVCPUs(context.Background()),
	}

	// Make sure there is at least one environment variable that will be
	// reported.
	if err := os.Setenv("GOGC", "100"); err != nil {
		t.Fatal(err)
	}

	nodeSummary := recorder.GenerateNodeStatus(context.Background())
	if nodeSummary == nil {
		t.Fatalf("recorder did not return nodeSummary")
	}
	if len(nodeSummary.Args) == 0 {
		t.Fatalf("expected args to be present")
	}
	if len(nodeSummary.Env) == 0 {
		t.Fatalf("expected env to be present")
	}
	nodeSummary.Args = nil
	nodeSummary.Env = nil
	nodeSummary.Activity = nil
	nodeSummary.Latencies = nil

	sort.Sort(byStoreDescID(nodeSummary.StoreStatuses))
	if a, e := nodeSummary, expectedNodeSummary; !reflect.DeepEqual(a, e) {
		t.Errorf("recorder did not produce expected NodeSummary; diff:\n %s", pretty.Diff(e, a))
	}

	// Make sure that all methods other than GenerateNodeStatus can operate in
	// parallel with each other (i.e. even if recorder.mu is RLocked).
	recorder.mu.RLock()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			if _, err := recorder.MarshalJSON(); err != nil {
				t.Error(err)
			}
			_ = recorder.PrintAsText(io.Discard, expfmt.FmtText, false)
			_ = recorder.GetTimeSeriesData(false)
			wg.Done()
		}()
	}
	wg.Wait()
	recorder.mu.RUnlock()
}

func BenchmarkExtractValueAllocs(b *testing.B) {
	// Create a dummy histogram.
	h := metric.NewHistogram(metric.HistogramOptions{
		Mode: metric.HistogramModePrometheus,
		Metadata: metric.Metadata{
			Name: "benchmark.histogram",
		},
		Duration:     10 * time.Second,
		BucketConfig: metric.IOLatencyBuckets,
	})
	genValues := func() {
		for i := 0; i < 100000; i++ {
			value := rand.Intn(10e9 /* 10s */)
			h.RecordValue(int64(value))
		}
	}
	// Fill in the histogram with 100k dummy values, ranging from [0s, 10s), tick, and then do it again.
	// This ensures we have filled-in histograms for both the previous & current window.
	genValues()
	h.Tick()
	genValues()

	// Run a benchmark and report allocations.
	for n := 0; n < b.N; n++ {
		if err := extractValue(h.GetName(false /* useStaticLabels */), h, func(string, float64) {}); err != nil {
			b.Error(err)
		}
	}
	b.ReportAllocs()
}

func TestRecordChangefeedChildMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()

	manual := timeutil.NewManualTime(timeutil.Unix(0, 100))

	t.Run("empty registry", func(t *testing.T) {
		reg := metric.NewRegistry()
		recorder := registryRecorder{
			registry:       reg,
			format:         nodeTimeSeriesPrefix,
			source:         "test-source",
			timestampNanos: manual.Now().UnixNano(),
		}

		var dest []tspb.TimeSeriesData
		recorder.recordChangefeedChildMetrics(&dest)

		require.Empty(t, dest)
	})

	t.Run("non-changefeed metrics ignored", func(t *testing.T) {
		reg := metric.NewRegistry()

		// Add non-changefeed metrics
		gauge := metric.NewGauge(metric.Metadata{Name: "sql.connections"})
		reg.AddMetric(gauge)
		gauge.Update(10)

		counter := metric.NewCounter(metric.Metadata{Name: "kv.requests"})
		reg.AddMetric(counter)
		counter.Inc(5)

		recorder := registryRecorder{
			registry:       reg,
			format:         nodeTimeSeriesPrefix,
			source:         "test-source",
			timestampNanos: manual.Now().UnixNano(),
		}

		var dest []tspb.TimeSeriesData
		recorder.recordChangefeedChildMetrics(&dest)

		require.Empty(t, dest)
	})

	t.Run("changefeed metrics without child collection", func(t *testing.T) {
		reg := metric.NewRegistry()

		// Add changefeed aggmetric with child collection explicitly disabled
		tsdbRecordLabeled := false
		gauge := aggmetric.NewGauge(
			metric.Metadata{
				Name:              "changefeed.error_retries",
				TsdbRecordLabeled: &tsdbRecordLabeled,
				Category:          metric.Metadata_CHANGEFEEDS,
			},
			"job_id", "feed_id",
		)
		reg.AddMetric(gauge)

		// Add child metrics with labels
		child1 := gauge.AddChild("123", "abc")
		child1.Update(50)

		recorder := registryRecorder{
			registry:       reg,
			format:         nodeTimeSeriesPrefix,
			source:         "test-source",
			timestampNanos: manual.Now().UnixNano(),
		}

		var dest []tspb.TimeSeriesData
		recorder.recordChangefeedChildMetrics(&dest)

		// Should be empty since TsdbRecordLabeled is false
		require.Empty(t, dest)
	})

	t.Run("changefeed aggmetrics with child collection", func(t *testing.T) {
		reg := metric.NewRegistry()

		// Create an aggmetric which supports child metrics and is in the allowed list
		gauge := aggmetric.NewGauge(metric.Metadata{
			Name:     "changefeed.max_behind_nanos",
			Category: metric.Metadata_CHANGEFEEDS,
		}, "job_id", "feed_id")
		reg.AddMetric(gauge)

		// Add child metrics with labels
		child1 := gauge.AddChild("123", "abc")
		child1.Update(50)

		child2 := gauge.AddChild("456", "def")
		child2.Update(75)

		recorder := registryRecorder{
			registry:       reg,
			format:         nodeTimeSeriesPrefix,
			source:         "test-source",
			timestampNanos: manual.Now().UnixNano(),
		}

		var dest []tspb.TimeSeriesData
		recorder.recordChangefeedChildMetrics(&dest)

		// Should have data for child metrics (TsdbRecordLabeled is defaulted true and metric is in allowed list)
		require.Len(t, dest, 2)

		// Verify the data structure
		for _, data := range dest {
			require.Contains(t, data.Name, "changefeed.max_behind_nanos")
			require.Equal(t, "test-source", data.Source)
			require.Len(t, data.Datapoints, 1)
			require.Equal(t, manual.Now().UnixNano(), data.Datapoints[0].TimestampNanos)
			require.Contains(t, []float64{50, 75}, data.Datapoints[0].Value)
		}
	})

	t.Run("cardinality limit enforcement", func(t *testing.T) {
		reg := metric.NewRegistry()

		gauge := aggmetric.NewGauge(metric.Metadata{
			Name:     "changefeed.total_ranges",
			Category: metric.Metadata_CHANGEFEEDS,
		}, "job_id")
		reg.AddMetric(gauge)

		// Add more than 1024 child metrics to test the limit
		for i := 0; i < 1500; i++ {
			child := gauge.AddChild(strconv.Itoa(i))
			child.Update(int64(i))
		}

		recorder := registryRecorder{
			registry:       reg,
			format:         nodeTimeSeriesPrefix,
			source:         "test-source",
			timestampNanos: manual.Now().UnixNano(),
		}

		var dest []tspb.TimeSeriesData
		recorder.recordChangefeedChildMetrics(&dest)

		// Should be limited to 1024 child metrics
		require.Len(t, dest, 1024)
	})

	t.Run("label sanitization and sorting", func(t *testing.T) {
		reg := metric.NewRegistry()

		gauge := aggmetric.NewGauge(metric.Metadata{
			Name:     "changefeed.aggregator_progress",
			Category: metric.Metadata_CHANGEFEEDS,
		}, "job-id", "feed.name")
		reg.AddMetric(gauge)

		// Add child with labels that need sanitization
		child := gauge.AddChild("test@123", "my-feed!")
		child.Update(42)

		recorder := registryRecorder{
			registry:       reg,
			format:         nodeTimeSeriesPrefix,
			source:         "test-source",
			timestampNanos: manual.Now().UnixNano(),
		}

		var dest []tspb.TimeSeriesData
		recorder.recordChangefeedChildMetrics(&dest)

		require.Len(t, dest, 1)

		// Verify that the metric name contains sanitized labels
		metricName := dest[0].Name
		require.Contains(t, metricName, "changefeed.aggregator_progress{feed_name=\"my-feed!\", job_id=\"test@123\"}")
	})

	t.Run("counter, gauge, histogram support", func(t *testing.T) {
		reg := metric.NewRegistry()

		// Test with gauge
		gauge := aggmetric.NewGauge(metric.Metadata{
			Name:     "changefeed.checkpoint_progress",
			Category: metric.Metadata_CHANGEFEEDS,
		}, "type")
		reg.AddMetric(gauge)
		gaugeChild := gauge.AddChild("gauge")
		gaugeChild.Update(100)

		// Test with counter
		counter := aggmetric.NewCounter(metric.Metadata{
			Name:     "changefeed.internal_retry_message_count",
			Category: metric.Metadata_CHANGEFEEDS,
		}, "type")
		reg.AddMetric(counter)
		counterChild := counter.AddChild("counter")
		counterChild.Inc(50)

		// Test with histogram
		histogram := aggmetric.NewHistogram(
			metric.HistogramOptions{
				Metadata: metric.Metadata{
					Name:     "changefeed.emitted_batch_sizes",
					Category: metric.Metadata_CHANGEFEEDS,
				},
				Duration:     10 * time.Second,
				BucketConfig: metric.IOLatencyBuckets,
				Mode:         metric.HistogramModePrometheus,
			},
			"scope",
		)
		reg.AddMetric(histogram)
		histChild := histogram.AddChild("default")
		histChild.RecordValue(100)
		histChild.RecordValue(200)
		histChild.RecordValue(300)

		recorder := registryRecorder{
			registry:       reg,
			format:         nodeTimeSeriesPrefix,
			source:         "test-source",
			timestampNanos: manual.Now().UnixNano(),
		}

		var dest []tspb.TimeSeriesData
		recorder.recordChangefeedChildMetrics(&dest)

		require.Len(t, dest, 13) // 1 gauge + 1 counter + 11 histogram metrics

		// Find gauge and counter data points
		var gaugeValue, counterValue float64
		for _, data := range dest {
			if data.Datapoints[0].Value == 100 {
				gaugeValue = data.Datapoints[0].Value
			} else if data.Datapoints[0].Value == 50 {
				counterValue = data.Datapoints[0].Value
			}
		}

		require.Equal(t, float64(100), gaugeValue)
		require.Equal(t, float64(50), counterValue)

		// Verify that histogram suffixes are present
		foundSuffixes := make(map[string]bool)
		expectedSuffixes := []string{"-count", "-sum", "-p50", "-p75", "-p90", "-p99", "-p99.9", "-p99.99", "-p99.999", "-max", "-avg"}

		for _, data := range dest {
			for _, suffix := range expectedSuffixes {
				if strings.Contains(data.Name, suffix) {
					foundSuffixes[suffix] = true
				}
			}
		}

		require.Equal(t, len(expectedSuffixes), len(foundSuffixes), "Expected to find histogram metric suffixes")
	})
}

func BenchmarkRecordChangefeedChildMetrics(b *testing.B) {
	manual := timeutil.NewManualTime(timeutil.Unix(0, 100))
	enableChildCollection := true

	// Get metrics from the allowed list and convert to slice for indexing
	allowedMetricsList := make([]string, 0, len(tsutil.AllowedChildMetrics))
	for metricName := range tsutil.AllowedChildMetrics {
		allowedMetricsList = append(allowedMetricsList, metricName)
	}

	// Benchmark with varying numbers of child metrics
	for childCount := 10; childCount <= 1024; childCount *= 10 {
		b.Run(fmt.Sprintf("children-%d", childCount), func(b *testing.B) {
			reg := metric.NewRegistry()

			// Create a single gauge with varying numbers of children
			gauge := aggmetric.NewGauge(
				metric.Metadata{
					Name:              allowedMetricsList[0],
					TsdbRecordLabeled: &enableChildCollection,
					Category:          metric.Metadata_CHANGEFEEDS,
				},
				"job_id",
			)
			reg.AddMetric(gauge)

			for i := 0; i < childCount; i++ {
				child := gauge.AddChild(strconv.Itoa(i))
				child.Update(int64(rand.Intn(1000)))
			}

			recorder := registryRecorder{
				registry:       reg,
				format:         nodeTimeSeriesPrefix,
				source:         "test-source",
				timestampNanos: manual.Now().UnixNano(),
			}

			b.ResetTimer()
			b.ReportAllocs()

			for n := 0; n < b.N; n++ {
				var dest []tspb.TimeSeriesData
				recorder.recordChangefeedChildMetrics(&dest)
			}
		})
	}
}
