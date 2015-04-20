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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Matt Tracy (matt.r.tracy@gmail.com)

package ts

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	gogoproto "github.com/gogo/protobuf/proto"
)

// TODO: These values were copied from storage/range_test.go in the
// name of expediency. A better home should be found for common test defaults
// like this.
var (
	testDefaultAcctConfig = proto.AcctConfig{}
	testDefaultPermConfig = proto.PermConfig{
		Read:  []string{"root"},
		Write: []string{"root"},
	}
	testDefaultZoneConfig = proto.ZoneConfig{
		ReplicaAttrs: []proto.Attributes{
			{Attrs: []string{"dc1", "mem"}},
			{Attrs: []string{"dc2", "mem"}},
		},
		RangeMinBytes: 1 << 10, // 1k
		RangeMaxBytes: 1 << 18, // 256k
		GC: &proto.GCPolicy{
			TTLSeconds: 24 * 60 * 60, // 1 day
		},
	}
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
	t         testing.TB
	modelData map[string]*proto.Value
	db        *DB

	engine      engine.Engine
	store       *storage.Store
	transport   multiraft.Transport
	manualClock *hlc.ManualClock
	clock       *hlc.Clock
	stopper     *util.Stopper
}

// newTestModel creates a new testModel instance. The Start() method must
// be called before using it.
func newTestModel(t *testing.T) *testModel {
	return &testModel{
		t:         t,
		modelData: make(map[string]*proto.Value),
	}
}

// Start constructs and starts the ts.DB instance and the cockroach store
// backing it.
func (tm *testModel) Start() {
	// Initialize some supporting objects needed to initialize the store.
	rpcContext := rpc.NewContext(hlc.NewClock(hlc.UnixNano), rpc.LoadInsecureTLSConfig())
	gossip := gossip.New(rpcContext, gossip.TestInterval, gossip.TestBootstrap)
	tm.manualClock = hlc.NewManualClock(0)
	tm.clock = hlc.NewClock(tm.manualClock.UnixNano)
	tm.transport = multiraft.NewLocalRPCTransport()
	tm.stopper = util.NewStopper()
	tm.stopper.AddCloser(tm.transport)

	// Initialize a new in-memory engine.
	tm.engine = engine.NewInMem(proto.Attributes{Attrs: []string{"dc1", "mem"}}, 1<<20)

	// Initialize and bootstrap a store which uses the engine.
	sender := kv.NewLocalSender()
	tm.store = storage.NewStore(tm.clock, tm.engine, client.NewKV(nil, sender), gossip, tm.transport, storage.TestStoreConfig)
	if err := tm.store.Bootstrap(proto.StoreIdent{NodeID: 1, StoreID: 1}, tm.stopper); err != nil {
		tm.t.Fatal(err)
	}
	if err := tm.store.BootstrapRange(); err != nil {
		tm.t.Fatal(err)
	}
	sender.AddStore(tm.store)
	if err := tm.store.Start(tm.stopper); err != nil {
		tm.t.Fatal(err)
	}
	rng, err := tm.store.GetRange(1)
	if err != nil {
		tm.t.Fatal(err)
	}
	// Without this, we'll very sporadically have test failures here since
	// Raft commands are retried, bypassing the response cache.
	// TODO(tschottdorf): remove the trigger when we've fixed the above.
	rng.WaitForElection()

	tm.initConfigs()

	// Initialize the DB instance.
	tm.db = NewDB(client.NewKV(nil, sender))
}

// Stop stops the system under test.
func (tm *testModel) Stop() {
	tm.stopper.Stop()
}

// initConfigs adds some default configuration entries to the data store.
func (tm *testModel) initConfigs() {
	if err := engine.MVCCPutProto(tm.engine, nil, engine.KeyConfigAccountingPrefix, proto.MinTimestamp, nil, &testDefaultAcctConfig); err != nil {
		tm.t.Fatal(err)
	}
	if err := engine.MVCCPutProto(tm.engine, nil, engine.KeyConfigPermissionPrefix, proto.MinTimestamp, nil, &testDefaultPermConfig); err != nil {
		tm.t.Fatal(err)
	}
	if err := engine.MVCCPutProto(tm.engine, nil, engine.KeyConfigZonePrefix, proto.MinTimestamp, nil, &testDefaultZoneConfig); err != nil {
		tm.t.Fatal(err)
	}
}

// getActualData returns the actual value of all time series keys in the
// underlying engine. Data is returned as a map of strings to proto.Values.
func (tm *testModel) getActualData() map[string]*proto.Value {
	// Scan over all TS Keys stored in the engine
	startKey := keyDataPrefix
	endKey := keyDataPrefix.PrefixEnd()
	keyValues, err := engine.MVCCScan(tm.engine, startKey, endKey, 0, tm.clock.Now(), true, nil)
	if err != nil {
		tm.t.Fatalf("error scanning TS data from engine: %s", err.Error())
	}

	kvMap := make(map[string]*proto.Value)
	for _, kv := range keyValues {
		val := kv.Value
		kvMap[string(kv.Key)] = &val
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
		tm.t.Log("Differences in model data and actual data:")
		for k, vActual := range actualData {
			n, s, r, ts := DecodeDataKey([]byte(k))
			if vModel, ok := tm.modelData[k]; !ok {
				tm.t.Logf("\tKey %s/%s@%d, r:%d from actual data was not found in model", n, s, ts, r)
			} else {
				if !gogoproto.Equal(vActual, vModel) {
					tm.t.Logf("\tKey %s/%s@%d, r:%d differs between model and actual:", n, s, ts, r)
					if its, err := proto.InternalTimeSeriesDataFromValue(vActual); err != nil {
						tm.t.Logf("\tActual value is not a valid time series: %v", vActual)
					} else {
						tm.t.Logf("\tActual value: %v", its)
					}
					if its, err := proto.InternalTimeSeriesDataFromValue(vModel); err != nil {
						tm.t.Logf("\tModel value is not a valid time series: %v", vModel)
					} else {
						tm.t.Logf("\tModel value: %v", its)
					}
				}
			}
		}

		// Detect keys in model which were not present in the actual data.
		for k := range tm.modelData {
			n, s, r, ts := DecodeDataKey([]byte(k))
			if _, ok := actualData[k]; !ok {
				tm.t.Logf("Key %s/%s@%d, r:%d from model was not found in actual data", n, s, ts, r)
			}
		}

		tm.t.Fatalf("Failing because model data was not equal to actual data.")
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

// storeTimeSeriesData instructs the model to store the given time series data
// in both the model and the system under test.
func (tm *testModel) storeTimeSeriesData(r Resolution, data proto.TimeSeriesData) {
	// Store data in the system under test.
	if err := tm.db.storeData(r, data); err != nil {
		tm.t.Fatalf("error storing time series data: %s", err.Error())
	}

	// Process and store data in the model.
	internalData, err := data.ToInternal(r.KeyDuration(), r.SampleDuration())
	if err != nil {
		tm.t.Fatalf("test could not convert time series to internal format: %s", err.Error())
	}

	for _, idata := range internalData {
		key := MakeDataKey(data.Name, data.Source, r, idata.StartTimestampNanos)
		keyStr := string(key)

		existing, ok := tm.modelData[keyStr]
		var newTs *proto.InternalTimeSeriesData
		if ok {
			existingTs, err := proto.InternalTimeSeriesDataFromValue(existing)
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
		val, err := newTs.ToValue()
		if err != nil {
			tm.t.Fatal(err)
		}
		tm.modelData[keyStr] = val
	}
}

// intDatapoint quickly generates an integer-valued datapoint.
func intDatapoint(timestamp int64, val int64) *proto.TimeSeriesDatapoint {
	return &proto.TimeSeriesDatapoint{
		TimestampNanos: timestamp,
		IntValue:       gogoproto.Int64(val),
	}
}

// floatDatapoint quickly generates an integer-valued datapoint.
func floatDatapoint(timestamp int64, val float32) *proto.TimeSeriesDatapoint {
	return &proto.TimeSeriesDatapoint{
		TimestampNanos: timestamp,
		FloatValue:     gogoproto.Float32(val),
	}
}

// TestStoreTimeSeries is a simple test of the Time Series module, ensuring that
// it is storing time series correctly.
func TestStoreTimeSeries(t *testing.T) {
	tm := newTestModel(t)
	tm.Start()
	defer tm.Stop()

	// Basic storage operation: one data point.
	tm.storeTimeSeriesData(Resolution10s, proto.TimeSeriesData{
		Name: "test.metric",
		Datapoints: []*proto.TimeSeriesDatapoint{
			intDatapoint(-446061360000000000, 100),
		},
	})
	tm.assertKeyCount(1)
	tm.assertModelCorrect()

	// Store data with different sources, and with multiple data points that
	// aggregate into the same key.
	tm.storeTimeSeriesData(Resolution10s, proto.TimeSeriesData{
		Name:   "test.metric.float",
		Source: "cpu01",
		Datapoints: []*proto.TimeSeriesDatapoint{
			floatDatapoint(1428713843000000000, 100.0),
			floatDatapoint(1428713843000000001, 50.2),
			floatDatapoint(1428713843000000002, 90.9),
		},
	})
	tm.storeTimeSeriesData(Resolution10s, proto.TimeSeriesData{
		Name:   "test.metric.float",
		Source: "cpu02",
		Datapoints: []*proto.TimeSeriesDatapoint{
			floatDatapoint(1428713843000000000, 900.8),
			floatDatapoint(1428713843000000001, 30.12),
			floatDatapoint(1428713843000000002, 72.324),
		},
	})
	tm.assertKeyCount(3)
	tm.assertModelCorrect()

	// A single storage operation that stores to multiple keys, including an
	// existing key.
	tm.storeTimeSeriesData(Resolution10s, proto.TimeSeriesData{
		Name: "test.metric",
		Datapoints: []*proto.TimeSeriesDatapoint{
			intDatapoint(-446061360000000001, 200),
			intDatapoint(450000000000000000, 1),
			intDatapoint(460000000000000000, 777),
		},
	})
	tm.assertKeyCount(5)
	tm.assertModelCorrect()
}
