// Copyright 2016 The Cockroach Authors.
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

package storage_test

import (
	"fmt"
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/kr/pretty"
)

type modelTimeSeriesDataStore struct {
	syncutil.Mutex
	t                  testing.TB
	containsCalled     int
	pruneCalled        int
	pruneSeenStartKeys map[string]struct{}
	pruneSeenEndKeys   map[string]struct{}
}

func (m *modelTimeSeriesDataStore) ContainsTimeSeries(start, end roachpb.RKey) bool {
	if !start.Less(end) {
		m.t.Fatalf("ContainsTimeSeries passed start key %v which is not less than end key %v", start, end)
	}
	m.Lock()
	defer m.Unlock()
	m.containsCalled++
	return true
}

func (m *modelTimeSeriesDataStore) PruneTimeSeries(
	ctx context.Context,
	snapshot engine.Reader,
	start, end roachpb.RKey,
	db *client.DB,
	now hlc.Timestamp,
) error {
	if snapshot == nil {
		m.t.Fatal("PruneTimeSeries was passed a nil snapshot")
	}
	if db == nil {
		m.t.Fatal("PruneTimeSeries was passed a nil client.DB")
	}
	if !start.Less(end) {
		m.t.Fatalf("PruneTimeSeries passed start key %v which is not less than end key %v", start, end)
	}

	m.Lock()
	defer m.Unlock()
	m.pruneCalled++
	m.pruneSeenStartKeys[start.String()] = struct{}{}
	m.pruneSeenEndKeys[end.String()] = struct{}{}
	return nil
}

// TestTimeSeriesMaintenanceQueue verifies shouldQueue and process method
// pass the correct data to the store's TimeSeriesData
func TestTimeSeriesMaintenanceQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	model := &modelTimeSeriesDataStore{
		t:                  t,
		pruneSeenStartKeys: make(map[string]struct{}),
		pruneSeenEndKeys:   make(map[string]struct{}),
	}

	cfg := storage.TestStoreConfig(nil)
	cfg.TimeSeriesDataStore = model
	cfg.TestingKnobs.DisableScanner = true
	cfg.TestingKnobs.DisableSplitQueue = true

	store, stopper := createTestStoreWithConfig(t, cfg)
	defer stopper.Stop()

	// Generate several splits.
	splitKeys := []roachpb.Key{roachpb.Key("c"), roachpb.Key("b"), roachpb.Key("a")}
	for _, k := range splitKeys {
		rng := store.LookupReplica(roachpb.RKey(k), nil)
		args := adminSplitArgs(k, k)
		if _, pErr := client.SendWrappedWith(context.Background(), store, roachpb.Header{
			RangeID: rng.RangeID,
		}, &args); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Generate a list of start/end keys the model should have been passed by
	// the queue. This consists of all split keys, with KeyMin as an additional
	// start and KeyMax as an additional end.
	expectedStartKeys := make(map[string]struct{})
	expectedEndKeys := make(map[string]struct{})
	expectedStartKeys[roachpb.KeyMin.String()] = struct{}{}
	expectedEndKeys[roachpb.KeyMax.String()] = struct{}{}
	for _, expected := range splitKeys {
		expectedStartKeys[expected.String()] = struct{}{}
		expectedEndKeys[expected.String()] = struct{}{}
	}

	// Wait for splits to complete and system config to be available.
	util.SucceedsSoon(t, func() error {
		if a, e := store.ReplicaCount(), len(expectedEndKeys); a != e {
			return fmt.Errorf("expected %d replicas in store; found %d", a, e)
		}
		if _, ok := store.Gossip().GetSystemConfig(); !ok {
			return fmt.Errorf("system config not yet available")
		}
		return nil
	})

	// Force replica scan to run, which will populate the model.
	store.ForceTimeSeriesMaintenanceQueueProcess()

	// Wait for processing to complete.
	util.SucceedsSoon(t, func() error {
		model.Lock()
		defer model.Unlock()
		if a, e := model.containsCalled, len(expectedStartKeys); a != e {
			return fmt.Errorf("ContainsTimeSeries called %d times; expected %d", a, e)
		}
		if a, e := model.pruneCalled, len(expectedStartKeys); a != e {
			return fmt.Errorf("PruneTimeSeries called %d times; expected %d", a, e)
		}
		return nil
	})

	model.Lock()
	defer model.Unlock()
	if a, e := model.pruneSeenStartKeys, expectedStartKeys; !reflect.DeepEqual(a, e) {
		t.Errorf("start keys seen by PruneTimeSeries did not match expectation: %s", pretty.Diff(a, e))
	}
	if a, e := model.pruneSeenEndKeys, expectedEndKeys; !reflect.DeepEqual(a, e) {
		t.Errorf("end keys seen by PruneTimeSeries did not match expectation: %s", pretty.Diff(a, e))
	}
}

// TestTimeSeriesMaintenanceQueueServer verifies that the time series
// maintenance queue runs correctly on a test server.
func TestTimeSeriesMaintenanceQueueServer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	tsrv := s.(*server.TestServer)
	tsdb := tsrv.TsDB()

	// Populate time series data into the server. One time series, with one
	// datapoint at the current time and one datapoint older than the pruning
	// threshold. Datapoint timestamps are set to the midpoint of sample duration
	// periods; this simplifies verification.
	seriesName := "test.metric"
	now := tsrv.Clock().PhysicalNow()
	pastThreshold := now - (ts.Resolution10s.PruneThreshold() * 2)
	sampleDuration := ts.Resolution10s.SampleDuration()
	datapoints := []tspb.TimeSeriesDatapoint{
		{
			TimestampNanos: pastThreshold - pastThreshold%sampleDuration + sampleDuration/2,
			Value:          200.0,
		},
		{
			TimestampNanos: now - now%sampleDuration + sampleDuration/2,
			Value:          100.0,
		},
	}
	if err := tsdb.StoreData(context.TODO(), ts.Resolution10s, []tspb.TimeSeriesData{
		{
			Name:       seriesName,
			Source:     "source1",
			Datapoints: datapoints,
		},
	}); err != nil {
		t.Fatal(err)
	}

	// getDatapoints queries all datapoints in the series from the beginning
	// of time to a point in the near future.
	getDatapoints := func() ([]tspb.TimeSeriesDatapoint, error) {
		dps, _, err := tsdb.Query(
			context.TODO(),
			tspb.Query{Name: seriesName},
			ts.Resolution10s,
			ts.Resolution10s.SampleDuration(),
			0,
			now+ts.Resolution10s.SlabDuration(),
		)
		return dps, err
	}

	// Verify the datapoints are all present.
	actualDatapoints, err := getDatapoints()
	if err != nil {
		t.Fatal(err)
	}
	if a, e := actualDatapoints, datapoints; !reflect.DeepEqual(a, e) {
		t.Fatalf("got datapoints %v, expected %v, diff: %s", a, e, pretty.Diff(a, e))
	}

	// Force pruning.
	storeID := roachpb.StoreID(1)
	store, err := tsrv.Stores().GetStore(roachpb.StoreID(1))
	if err != nil {
		t.Fatalf("error retrieving store %d: %s", storeID, err)
	}
	store.ForceTimeSeriesMaintenanceQueueProcess()

	// Verify the older datapoint has been pruned.
	util.SucceedsSoon(t, func() error {
		actualDatapoints, err = getDatapoints()
		if err != nil {
			return err
		}
		if a, e := actualDatapoints, datapoints[1:]; !reflect.DeepEqual(a, e) {
			return fmt.Errorf("got datapoints %v, expected %v, diff: %s", a, e, pretty.Diff(a, e))
		}
		return nil
	})
}
