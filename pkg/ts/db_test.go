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

package ts

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ts/testmodel"
	"github.com/pkg/errors"

	"github.com/kr/pretty"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/localtestcluster"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// testModelRunner is a model-based testing structure used to verify that time
// series data sent to the Cockroach time series DB is stored correctly.
//
// This structure maintains a single ts.DB instance which stores data in a
// monolithic Cockroach Store. It additionally maintains a simple in-memory key
// value map, which is used as a model of the time series data stored in
// Cockroach. The model maintains an expected copy of all keys beginning with
// the time series data prefix.
//
// Each test should send a series of commands to the testModelRunner. Commands
// are dispatched to the ts.DB instance, but are also used to modify the
// in-memory key value model. Tests should periodically compare the in-memory
// model to the actual data stored in the cockroach engine, ensuring that the
// data matches.
type testModelRunner struct {
	t     testing.TB
	model *testmodel.ModelDB
	*localtestcluster.LocalTestCluster
	DB                *DB
	workerMemMonitor  *mon.BytesMonitor
	resultMemMonitor  *mon.BytesMonitor
	queryMemoryBudget int64
}

// newTestModelRunner creates a new testModel instance. The Start() method must
// be called before using it.
func newTestModelRunner(t *testing.T) testModelRunner {
	st := cluster.MakeTestingClusterSettings()
	workerMonitor := mon.MakeUnlimitedMonitor(
		context.Background(),
		"timeseries-test-worker",
		mon.MemoryResource,
		nil,
		nil,
		math.MaxInt64,
		st,
	)
	resultMonitor := mon.MakeUnlimitedMonitor(
		context.Background(),
		"timeseries-test-result",
		mon.MemoryResource,
		nil,
		nil,
		math.MaxInt64,
		st,
	)
	return testModelRunner{
		t:                 t,
		model:             testmodel.NewModelDB(),
		LocalTestCluster:  &localtestcluster.LocalTestCluster{},
		workerMemMonitor:  &workerMonitor,
		resultMemMonitor:  &resultMonitor,
		queryMemoryBudget: math.MaxInt64,
	}
}

// Start constructs and starts the local test server and creates a
// time series DB.
func (tm *testModelRunner) Start() {
	tm.LocalTestCluster.Start(tm.t, testutils.NewNodeTestBaseContext(),
		kv.InitFactoryForLocalTestCluster)
	tm.DB = NewDB(tm.LocalTestCluster.DB, tm.Cfg.Settings)
}

// getActualData returns the actual value of all time series keys in the
// underlying engine. Data is returned as a map of strings to roachpb.Values.
func (tm *testModelRunner) getActualData() map[string]roachpb.Value {
	// Scan over all TS Keys stored in the engine
	startKey := keys.TimeseriesPrefix
	endKey := startKey.PrefixEnd()
	keyValues, _, _, err := engine.MVCCScan(context.Background(), tm.Eng, startKey, endKey, math.MaxInt64, tm.Clock.Now(), true, nil)
	if err != nil {
		tm.t.Fatalf("error scanning TS data from engine: %s", err)
	}

	kvMap := make(map[string]roachpb.Value)
	for _, kv := range keyValues {
		kvMap[string(kv.Key)] = kv.Value
	}

	return kvMap
}

// assertModelCorrect asserts that the model data being maintained by this
// testModel is equivalent to the actual time series data stored in the
// engine. If the actual data does not match the model, this method will print
// out detailed information about the differences between the two data sets.
func (tm *testModelRunner) assertModelCorrect() {
	tm.t.Helper()
	actualData := tm.getActualData()
	modelDisk := tm.getNewModelDiskLayout()
	if a, e := actualData, modelDisk; !reflect.DeepEqual(a, e) {
		for _, diff := range pretty.Diff(a, e) {
			tm.t.Error(diff)
		}
	}
}

func (tm *testModelRunner) getNewModelDiskLayout() map[string]roachpb.Value {
	result := make(map[string]roachpb.Value)
	tm.model.VisitAllSeries(func(name, source string, data testmodel.DataSeries) (testmodel.DataSeries, bool) {
		// For computing the expected disk layout, only consider resolution-specific
		// series.
		resolution, seriesName, valid := getResolutionFromKey(name)
		if !valid {
			return data, false
		}

		tsdata := tspb.TimeSeriesData{
			Name:   seriesName,
			Source: source,
			// Downsample data points according to resolution. Downsampling currently
			// always uses the last data point when storing to disk.
			Datapoints: data,
		}

		slabs, err := tsdata.ToInternal(resolution.SlabDuration(), resolution.SampleDuration())
		if err != nil {
			tm.t.Fatalf("error converting testmodel data to internal format: %s", err.Error())
			return data, false
		}

		for _, slab := range slabs {
			key := MakeDataKey(seriesName, source, resolution, slab.StartTimestampNanos)
			keyStr := string(key)
			var val roachpb.Value
			if err := val.SetProto(&slab); err != nil {
				tm.t.Fatal(err)
			}
			result[keyStr] = val
		}

		return data, false
	})

	return result
}

// assertKeyCount asserts that the model contains the expected number of keys.
// This is used to ensure that data is actually being generated in the test
// model.
func (tm *testModelRunner) assertKeyCount(expected int) {
	if a, e := len(tm.getNewModelDiskLayout()), expected; a != e {
		tm.t.Errorf("model data key count did not match expected value: %d != %d", a, e)
	}
}

func (tm *testModelRunner) storeInModel(r Resolution, data tspb.TimeSeriesData) {
	if !TimeseriesStorageEnabled.Get(&tm.Cfg.Settings.SV) {
		return
	}

	// Store in the new model. Record for both full-resolution data *and* a series
	// for the specific resolution recorded. The series-specific resolution is
	// used to simulate the expected on-disk layout of series in CockroachDB, while
	// the full-resolution data is used to verify query results.
	tm.model.Record(data.Name, data.Source, data.Datapoints)
	tm.model.Record(
		resolutionModelKey(data.Name, r),
		data.Source,
		testmodel.DataSeries(data.Datapoints).GroupByResolution(
			r.SampleDuration(), testmodel.AggregateLast,
		),
	)
}

// resolutionModelKey returns a string to store resolution-specific data in
// the test model.
func resolutionModelKey(name string, r Resolution) string {
	return fmt.Sprintf("@%d.%s", r, name)
}

func getResolutionFromKey(key string) (Resolution, string, bool) {
	if len(key) < 3 || !strings.HasPrefix(key, "@") {
		return 0, key, false
	}

	parts := strings.SplitN(key[1:], ".", 2)
	if len(parts) != 2 {
		return 0, key, false
	}

	val, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, key, false
	}

	return Resolution(val), parts[1], true
}

// storeTimeSeriesData instructs the model to store the given time series data
// in both the model and the system under test.
func (tm *testModelRunner) storeTimeSeriesData(r Resolution, data []tspb.TimeSeriesData) {
	// Store data in the system under test.
	if err := tm.DB.StoreData(context.TODO(), r, data); err != nil {
		tm.t.Fatalf("error storing time series data: %s", err)
	}

	// Store data in the original model.
	// TODO(mrtracy): remove this.
	for _, d := range data {
		tm.storeInModel(r, d)
	}
}

// prune time series from the model. "nowNanos" represents the current time,
// and is used to compute threshold ages. Only time series in the provided list
// of time series/resolution pairs will be considered for deletion.
func (tm *testModelRunner) prune(nowNanos int64, timeSeries ...timeSeriesResolutionInfo) {
	// Prune time series from the system under test.
	if err := tm.DB.pruneTimeSeries(
		context.TODO(),
		tm.LocalTestCluster.DB,
		timeSeries,
		hlc.Timestamp{
			WallTime: nowNanos,
			Logical:  0,
		},
	); err != nil {
		tm.t.Fatalf("error pruning time series data: %s", err)
	}

	// Prune the appropriate resolution-specific series from the test model using
	// VisitSeries.
	thresholds := tm.DB.computeThresholds(nowNanos)
	for _, ts := range timeSeries {
		tm.model.VisitSeries(
			resolutionModelKey(ts.Name, ts.Resolution),
			func(name, source string, data testmodel.DataSeries) (testmodel.DataSeries, bool) {
				pruned := data.TimeSlice(thresholds[ts.Resolution], math.MaxInt64)
				if len(pruned) != len(data) {
					return pruned, true
				}
				return data, false
			},
		)
	}
}

// assertQuery generates a query result from the local test model and compares
// it against the query returned from the server.
//
// My suggestion is to break down the model query into multiple, independently
// verificable steps. These steps will not be memory or computationally
// efficient, but will be conceptually easy to verify; then we can compare its
// results against the real data store with more confidence.
func (tm *testModelRunner) assertQuery(
	name string,
	sources []string,
	downsample, agg *tspb.TimeSeriesQueryAggregator,
	derivative *tspb.TimeSeriesQueryDerivative,
	r Resolution,
	sampleDuration, start, end, interpolationLimit int64,
	expectedDatapointCount, expectedSourceCount int,
) {
	tm.t.Helper()
	// Query the actual server.
	q := tspb.Query{
		Name:             name,
		Downsampler:      downsample,
		SourceAggregator: agg,
		Derivative:       derivative,
		Sources:          sources,
	}

	memContext := tm.makeMemoryContext(interpolationLimit)
	defer memContext.Close(context.TODO())
	actualDatapoints, actualSources, err := tm.DB.QueryMemoryConstrained(
		context.TODO(),
		q,
		r,
		sampleDuration,
		start,
		end,
		memContext,
	)
	if err != nil {
		tm.t.Fatal(err)
	}
	if a, e := len(actualDatapoints), expectedDatapointCount; a != e {
		tm.t.Logf("actual datapoints: %v", actualDatapoints)
		tm.t.Fatal(errors.Errorf("query got %d datapoints, wanted %d", a, e))
	}
	if a, e := len(actualSources), expectedSourceCount; a != e {
		tm.t.Fatal(errors.Errorf("query got %d sources, wanted %d", a, e))
	}

	// Query the testmodel.
	modelDatapoints := tm.model.Query(
		name,
		sources,
		q.GetDownsampler(),
		q.GetSourceAggregator(),
		q.GetDerivative(),
		r.SlabDuration(),
		sampleDuration, start, end, interpolationLimit,
	)
	if a, e := testmodel.DataSeries(actualDatapoints), modelDatapoints; !testmodel.DataSeriesEquivalent(a, e) {
		for _, diff := range pretty.Diff(a, e) {
			tm.t.Error(diff)
		}
	}
}

func (tm *testModelRunner) makeMemoryContext(interpolationLimitNanos int64) QueryMemoryContext {
	return MakeQueryMemoryContext(
		tm.workerMemMonitor,
		tm.resultMemMonitor,
		QueryMemoryOptions{
			BudgetBytes:             tm.queryMemoryBudget,
			EstimatedSources:        tm.model.UniqueSourceCount(),
			InterpolationLimitNanos: interpolationLimitNanos,
		},
	)
}

// modelDataSource is used to create a mock DataSource. It returns a
// deterministic set of data to GetTimeSeriesData, storing the returned data in
// the model whenever GetTimeSeriesData is called. Data is returned until all
// sets are exhausted, at which point the supplied stop.Stopper is stopped.
type modelDataSource struct {
	model       testModelRunner
	datasets    [][]tspb.TimeSeriesData
	r           Resolution
	stopper     *stop.Stopper
	calledCount int
	once        sync.Once
}

// GetTimeSeriesData implements the DataSource interface, returning a predefined
// set of TimeSeriesData to subsequent calls. It stores each TimeSeriesData
// object in the test model before returning it. If all TimeSeriesData objects
// have been returned, this method will stop the provided Stopper.
func (mds *modelDataSource) GetTimeSeriesData() []tspb.TimeSeriesData {
	if len(mds.datasets) == 0 {
		// Stop on goroutine to prevent deadlock.
		go mds.once.Do(func() { mds.stopper.Stop(context.Background()) })
		return nil
	}
	mds.calledCount++
	data := mds.datasets[0]
	mds.datasets = mds.datasets[1:]

	for _, d := range data {
		mds.model.storeInModel(mds.r, d)
	}
	return data
}

// datapoint quickly generates a time series datapoint.
func datapoint(timestamp int64, val float64) tspb.TimeSeriesDatapoint {
	return tspb.TimeSeriesDatapoint{
		TimestampNanos: timestamp,
		Value:          val,
	}
}

// TestStoreTimeSeries is a simple test of the Time Series module, ensuring that
// it is storing time series correctly.
func TestStoreTimeSeries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tm := newTestModelRunner(t)
	tm.Start()
	defer tm.Stop()

	// Basic storage operation: one data point.
	tm.storeTimeSeriesData(Resolution10s, []tspb.TimeSeriesData{
		{
			Name: "test.metric",
			Datapoints: []tspb.TimeSeriesDatapoint{
				datapoint(-446061360000000000, 100),
			},
		},
	})
	tm.assertKeyCount(1)
	tm.assertModelCorrect()

	// Store data with different sources, and with multiple data points that
	// aggregate into the same key.
	tm.storeTimeSeriesData(Resolution10s, []tspb.TimeSeriesData{
		{
			Name:   "test.metric.float",
			Source: "cpu01",
			Datapoints: []tspb.TimeSeriesDatapoint{
				datapoint(1428713843000000000, 100.0),
				datapoint(1428713843000000001, 50.2),
				datapoint(1428713843000000002, 90.9),
			},
		},
	})
	tm.storeTimeSeriesData(Resolution10s, []tspb.TimeSeriesData{
		{
			Name:   "test.metric.float",
			Source: "cpu02",
			Datapoints: []tspb.TimeSeriesDatapoint{
				datapoint(1428713843000000000, 900.8),
				datapoint(1428713843000000001, 30.12),
				datapoint(1428713843000000002, 72.324),
			},
		},
	})
	tm.assertKeyCount(3)
	tm.assertModelCorrect()

	// A single storage operation that stores to multiple keys, including an
	// existing key.
	tm.storeTimeSeriesData(Resolution10s, []tspb.TimeSeriesData{
		{
			Name: "test.metric",
			Datapoints: []tspb.TimeSeriesDatapoint{
				datapoint(-446061360000000000, 200),
				datapoint(450000000000000001, 1),
				datapoint(460000000000000000, 777),
			},
		},
	})
	tm.assertKeyCount(5)
	tm.assertModelCorrect()
}

// TestPollSource verifies that polled data sources are called as expected.
func TestPollSource(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tm := newTestModelRunner(t)
	tm.Start()
	defer tm.Stop()

	testSource := modelDataSource{
		model:   tm,
		r:       Resolution10s,
		stopper: stop.NewStopper(),
		datasets: [][]tspb.TimeSeriesData{
			{
				{
					Name:   "test.metric.float",
					Source: "cpu01",
					Datapoints: []tspb.TimeSeriesDatapoint{
						datapoint(1428713843000000000, 100.0),
						datapoint(1428713843000000001, 50.2),
						datapoint(1428713843000000002, 90.9),
					},
				},
				{
					Name:   "test.metric.float",
					Source: "cpu02",
					Datapoints: []tspb.TimeSeriesDatapoint{
						datapoint(1428713843000000000, 900.8),
						datapoint(1428713843000000001, 30.12),
						datapoint(1428713843000000002, 72.324),
					},
				},
			},
			{
				{
					Name: "test.metric",
					Datapoints: []tspb.TimeSeriesDatapoint{
						datapoint(-446061360000000000, 100),
					},
				},
			},
		},
	}

	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
	tm.DB.PollSource(ambient, &testSource, time.Millisecond, Resolution10s, testSource.stopper)
	<-testSource.stopper.IsStopped()
	if a, e := testSource.calledCount, 2; a != e {
		t.Errorf("testSource was called %d times, expected %d", a, e)
	}
	tm.assertKeyCount(3)
	tm.assertModelCorrect()
}

// TestDisableStorage verifies that disabling timeseries storage via the cluster
// setting works properly.
func TestDisableStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tm := newTestModelRunner(t)
	tm.Start()
	defer tm.Stop()
	TimeseriesStorageEnabled.Override(&tm.Cfg.Settings.SV, false)

	// Basic storage operation: one data point.
	tm.storeTimeSeriesData(Resolution10s, []tspb.TimeSeriesData{
		{
			Name: "test.metric",
			Datapoints: []tspb.TimeSeriesDatapoint{
				datapoint(-446061360000000000, 100),
			},
		},
	})
	tm.assertKeyCount(0)
	tm.assertModelCorrect()

	testSource := modelDataSource{
		model:   tm,
		r:       Resolution10s,
		stopper: stop.NewStopper(),
		datasets: [][]tspb.TimeSeriesData{
			{
				{
					Name:   "test.metric.float",
					Source: "cpu01",
					Datapoints: []tspb.TimeSeriesDatapoint{
						datapoint(1428713843000000000, 100.0),
						datapoint(1428713843000000001, 50.2),
						datapoint(1428713843000000002, 90.9),
					},
				},
				{
					Name:   "test.metric.float",
					Source: "cpu02",
					Datapoints: []tspb.TimeSeriesDatapoint{
						datapoint(1428713843000000000, 900.8),
						datapoint(1428713843000000001, 30.12),
						datapoint(1428713843000000002, 72.324),
					},
				},
			},
			{
				{
					Name: "test.metric",
					Datapoints: []tspb.TimeSeriesDatapoint{
						datapoint(-446061360000000000, 100),
					},
				},
			},
		},
	}

	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
	tm.DB.PollSource(ambient, &testSource, time.Millisecond, Resolution10s, testSource.stopper)
	select {
	case <-testSource.stopper.IsStopped():
		t.Error("testSource data exhausted when polling should have been enabled")
	case <-time.After(50 * time.Millisecond):
		testSource.stopper.Stop(context.Background())
	}
	if a, e := testSource.calledCount, 0; a != e {
		t.Errorf("testSource was called %d times, expected %d", a, e)
	}
	tm.assertKeyCount(0)
	tm.assertModelCorrect()
}

// TestPruneThreshold verifies that `PruneThreshold` returns correct result in nanoseconds
func TestPruneThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tm := newTestModelRunner(t)
	tm.Start()
	defer tm.Stop()
	expected := resolution10sDefaultPruneThreshold.Nanoseconds()
	db := NewDB(nil, tm.Cfg.Settings)
	result := db.PruneThreshold(Resolution10s)
	if expected != result {
		t.Errorf("prune threshold did not match expected value: %d != %d", expected, result)
	}
}
