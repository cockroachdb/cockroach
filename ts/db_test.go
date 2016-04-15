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
// Author: Matt Tracy (matt@cockroachlabs.com)

package ts

import (
	"bytes"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/gogo/protobuf/proto"
)

// testModel is a model-based testing structure used to verify that time
// series data sent to the Cockroach time series DB is stored correctly.
//
// This structure maintains a single ts.DB instance which stores data in a
// monolithic Cockroach Store. It additionally maintains a simple in-memory key
// value map, which is used as a model of the time series data stored in
// Cockroach. The model maintains an expected copy of all keys beginning with
// the time series data prefix.
//
// Each test should send a series of commands to the testModel. Commands are
// dispatched to the ts.DB instance, but are also used to modify the
// in-memory key value model. Tests should periodically compare the in-memory
// model to the actual data stored in the cockroach engine, ensuring that the
// data matches.
type testModel struct {
	t           testing.TB
	modelData   map[string]roachpb.Value
	seenSources map[string]struct{}
	*kv.LocalTestCluster
	DB *DB
}

// newTestModel creates a new testModel instance. The Start() method must
// be called before using it.
func newTestModel(t *testing.T) testModel {
	return testModel{
		t:                t,
		modelData:        make(map[string]roachpb.Value),
		seenSources:      make(map[string]struct{}),
		LocalTestCluster: &kv.LocalTestCluster{},
	}
}

// Start constructs and starts the local test server and creates a
// time series DB.
func (tm *testModel) Start() {
	tm.LocalTestCluster.Start(tm.t)
	tm.DB = NewDB(tm.LocalTestCluster.DB)
}

// getActualData returns the actual value of all time series keys in the
// underlying engine. Data is returned as a map of strings to roachpb.Values.
func (tm *testModel) getActualData() map[string]roachpb.Value {
	// Scan over all TS Keys stored in the engine
	startKey := keys.TimeseriesPrefix
	endKey := startKey.PrefixEnd()
	keyValues, _, err := engine.MVCCScan(context.Background(), tm.Eng, startKey, endKey, 0, tm.Clock.Now(), true, nil)
	if err != nil {
		tm.t.Fatalf("error scanning TS data from engine: %s", err.Error())
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
func (tm *testModel) assertModelCorrect() {
	actualData := tm.getActualData()
	if !reflect.DeepEqual(tm.modelData, actualData) {
		// Provide a detailed differencing of the actual data and the expected
		// model. This is done by comparing individual keys, and printing human
		// readable information about any keys which differ in value between the
		// two data sets.
		var buf bytes.Buffer
		buf.WriteString("Found unexpected differences in model data and actual data:\n")
		for k, vActual := range actualData {
			n, s, r, ts, err := DecodeDataKey([]byte(k))
			if err != nil {
				tm.t.Fatal(err)
			}
			if vModel, ok := tm.modelData[k]; !ok {
				fmt.Fprintf(&buf, "\nKey %s/%s@%d, r:%d from actual data was not found in model", n, s, ts, r)
			} else {
				if !proto.Equal(&vActual, &vModel) {
					fmt.Fprintf(&buf, "\nKey %s/%s@%d, r:%d differs between model and actual:", n, s, ts, r)
					if its, err := vActual.GetTimeseries(); err != nil {
						fmt.Fprintf(&buf, "\nActual value is not a valid time series: %v", vActual)
					} else {
						fmt.Fprintf(&buf, "\nActual value: %s", &its)
					}
					if its, err := vModel.GetTimeseries(); err != nil {
						fmt.Fprintf(&buf, "\nModel value is not a valid time series: %v", vModel)
					} else {
						fmt.Fprintf(&buf, "\nModel value: %s", &its)
					}
				}
			}
		}

		// Detect keys in model which were not present in the actual data.
		for k := range tm.modelData {
			n, s, r, ts, err := DecodeDataKey([]byte(k))
			if err != nil {
				tm.t.Fatal(err)
			}
			if _, ok := actualData[k]; !ok {
				fmt.Fprintf(&buf, "Key %s/%s@%d, r:%d from model was not found in actual data", n, s, ts, r)
			}
		}

		tm.t.Fatal(buf.String())
	}
}

// assertKeyCount asserts that the model contains the expected number of keys.
// This is used to ensure that data is actually being generated in the test
// model.
func (tm *testModel) assertKeyCount(expected int) {
	if a, e := len(tm.modelData), expected; a != e {
		tm.t.Errorf("model data key count did not match expected value: %d != %d", a, e)
	}
}

func (tm *testModel) storeInModel(r Resolution, data TimeSeriesData) {
	// Note the source, used to construct keys for model queries.
	tm.seenSources[data.Source] = struct{}{}

	// Process and store data in the model.
	internalData, err := data.ToInternal(r.KeyDuration(), r.SampleDuration())
	if err != nil {
		tm.t.Fatalf("test could not convert time series to internal format: %s", err.Error())
	}

	for _, idata := range internalData {
		key := MakeDataKey(data.Name, data.Source, r, idata.StartTimestampNanos)
		keyStr := string(key)

		existing, ok := tm.modelData[keyStr]
		var newTs roachpb.InternalTimeSeriesData
		if ok {
			existingTs, err := existing.GetTimeseries()
			if err != nil {
				tm.t.Fatalf("test could not extract time series from existing model value: %s", err.Error())
			}
			newTs, err = engine.MergeInternalTimeSeriesData(existingTs, idata)
			if err != nil {
				tm.t.Fatalf("test could not merge time series into model value: %s", err.Error())
			}
		} else {
			newTs, err = engine.MergeInternalTimeSeriesData(idata)
			if err != nil {
				tm.t.Fatalf("test could not merge time series into model value: %s", err.Error())
			}
		}
		var val roachpb.Value
		if err := val.SetProto(&newTs); err != nil {
			tm.t.Fatal(err)
		}
		tm.modelData[keyStr] = val
	}
}

// storeTimeSeriesData instructs the model to store the given time series data
// in both the model and the system under test.
func (tm *testModel) storeTimeSeriesData(r Resolution, data []TimeSeriesData) {
	// Store data in the system under test.
	if err := tm.DB.StoreData(r, data); err != nil {
		tm.t.Fatalf("error storing time series data: %s", err.Error())
	}

	// Store data in the model.
	for _, d := range data {
		tm.storeInModel(r, d)
	}
}

// modelDataSource is used to create a mock DataSource. It returns a
// deterministic set of data to GetTimeSeriesData, storing the returned data in
// the model whenever GetTimeSeriesData is called. Data is returned until all
// sets are exhausted, at which point the supplied stop.Stopper is stopped.
type modelDataSource struct {
	model       testModel
	datasets    [][]TimeSeriesData
	r           Resolution
	stopper     *stop.Stopper
	calledCount int
	once        sync.Once
}

// GetTimeSeriesData implements the DataSource interface, returning a predefined
// set of TimeSeriesData to subsequent calls. It stores each TimeSeriesData
// object in the test model before returning it. If all TimeSeriesData objects
// have been returned, this method will stop the provided Stopper.
func (mds *modelDataSource) GetTimeSeriesData() []TimeSeriesData {
	if len(mds.datasets) == 0 {
		// Stop on goroutine to prevent deadlock.
		go mds.once.Do(mds.stopper.Stop)
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
func datapoint(timestamp int64, val float64) TimeSeriesDatapoint {
	return TimeSeriesDatapoint{
		TimestampNanos: timestamp,
		Value:          val,
	}
}

// TestStoreTimeSeries is a simple test of the Time Series module, ensuring that
// it is storing time series correctly.
func TestStoreTimeSeries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tm := newTestModel(t)
	tm.Start()
	defer tm.Stop()

	// Basic storage operation: one data point.
	tm.storeTimeSeriesData(Resolution10s, []TimeSeriesData{
		{
			Name: "test.metric",
			Datapoints: []TimeSeriesDatapoint{
				datapoint(-446061360000000000, 100),
			},
		},
	})
	tm.assertKeyCount(1)
	tm.assertModelCorrect()

	// Store data with different sources, and with multiple data points that
	// aggregate into the same key.
	tm.storeTimeSeriesData(Resolution10s, []TimeSeriesData{
		{
			Name:   "test.metric.float",
			Source: "cpu01",
			Datapoints: []TimeSeriesDatapoint{
				datapoint(1428713843000000000, 100.0),
				datapoint(1428713843000000001, 50.2),
				datapoint(1428713843000000002, 90.9),
			},
		},
	})
	tm.storeTimeSeriesData(Resolution10s, []TimeSeriesData{
		{
			Name:   "test.metric.float",
			Source: "cpu02",
			Datapoints: []TimeSeriesDatapoint{
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
	tm.storeTimeSeriesData(Resolution10s, []TimeSeriesData{
		{
			Name: "test.metric",
			Datapoints: []TimeSeriesDatapoint{
				datapoint(-446061360000000001, 200),
				datapoint(450000000000000000, 1),
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
	t.Skip("#3692")
	tm := newTestModel(t)
	tm.Start()
	defer tm.Stop()

	testSource := modelDataSource{
		model:   tm,
		r:       Resolution10s,
		stopper: stop.NewStopper(),
		datasets: [][]TimeSeriesData{
			{
				{
					Name:   "test.metric.float",
					Source: "cpu01",
					Datapoints: []TimeSeriesDatapoint{
						datapoint(1428713843000000000, 100.0),
						datapoint(1428713843000000001, 50.2),
						datapoint(1428713843000000002, 90.9),
					},
				},
				{
					Name:   "test.metric.float",
					Source: "cpu02",
					Datapoints: []TimeSeriesDatapoint{
						datapoint(1428713843000000000, 900.8),
						datapoint(1428713843000000001, 30.12),
						datapoint(1428713843000000002, 72.324),
					},
				},
			},
			{
				{
					Name: "test.metric",
					Datapoints: []TimeSeriesDatapoint{
						datapoint(-446061360000000000, 100),
					},
				},
			},
		},
	}

	tm.DB.PollSource(&testSource, time.Millisecond, Resolution10s, testSource.stopper)
	<-testSource.stopper.IsStopped()
	if a, e := testSource.calledCount, 2; a != e {
		t.Errorf("testSource was called %d times, expected %d", a, e)
	}
	tm.assertKeyCount(3)
	tm.assertModelCorrect()
}
