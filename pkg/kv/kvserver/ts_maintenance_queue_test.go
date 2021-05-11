// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

type modelTimeSeriesDataStore struct {
	syncutil.Mutex
	t                  testing.TB
	containsCalled     int
	pruneCalled        int
	pruneSeenStartKeys []roachpb.Key
	pruneSeenEndKeys   []roachpb.Key
}

func (m *modelTimeSeriesDataStore) ContainsTimeSeries(start, end roachpb.RKey) bool {
	if !start.Less(end) {
		m.t.Fatalf("ContainsTimeSeries passed start key %v which is not less than end key %v", start, end)
	}
	m.Lock()
	defer m.Unlock()
	m.containsCalled++

	// We're going to consider some user-space ranges as containing timeseries.
	return roachpb.Key("a").Compare(start.AsRawKey()) <= 0 &&
		roachpb.Key("z").Compare(end.AsRawKey()) > 0
}

func (m *modelTimeSeriesDataStore) MaintainTimeSeries(
	ctx context.Context,
	snapshot storage.Reader,
	start, end roachpb.RKey,
	db *kv.DB,
	_ *mon.BytesMonitor,
	_ int64,
	now hlc.Timestamp,
) error {
	if snapshot == nil {
		m.t.Fatal("MaintainTimeSeries was passed a nil snapshot")
	}
	if db == nil {
		m.t.Fatal("MaintainTimeSeries was passed a nil client.DB")
	}
	if !start.Less(end) {
		m.t.Fatalf("MaintainTimeSeries passed start key %v which is not less than end key %v", start, end)
	}

	m.Lock()
	defer m.Unlock()
	m.pruneCalled++
	m.pruneSeenStartKeys = append(m.pruneSeenStartKeys, start.AsRawKey())
	sort.Slice(m.pruneSeenStartKeys, func(i, j int) bool {
		return m.pruneSeenStartKeys[i].Compare(m.pruneSeenStartKeys[j]) < 0
	})
	m.pruneSeenEndKeys = append(m.pruneSeenEndKeys, end.AsRawKey())
	sort.Slice(m.pruneSeenEndKeys, func(i, j int) bool {
		return m.pruneSeenEndKeys[i].Compare(m.pruneSeenEndKeys[j]) < 0
	})
	return nil
}

// TestTimeSeriesMaintenanceQueue verifies shouldQueue and process method
// pass the correct data to the store's TimeSeriesData
func TestTimeSeriesMaintenanceQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	model := &modelTimeSeriesDataStore{t: t}
	manual := hlc.NewHybridManualClock()

	ctx := context.Background()
	serv, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				ClockSource: manual.UnixNano,
			},
			Store: &kvserver.StoreTestingKnobs{
				DisableScanner:      true,
				DisableMergeQueue:   true,
				DisableSplitQueue:   true,
				TimeSeriesDataStore: model,
			},
		},
	})
	s := serv.(*server.TestServer)
	defer s.Stopper().Stop(ctx)
	store, err := s.Stores().GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	// Generate several splits. The "c"-"zz" range is not going to be considered
	// as containing timeseries.
	splitKeys := []roachpb.Key{roachpb.Key("zz"), roachpb.Key("c"), roachpb.Key("b"), roachpb.Key("a")}
	for _, k := range splitKeys {
		repl := store.LookupReplica(roachpb.RKey(k))
		args := adminSplitArgs(k)
		if _, pErr := kv.SendWrappedWith(ctx, store, roachpb.Header{
			RangeID: repl.RangeID,
		}, args); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Generate a list of start/end keys the model should have been passed by
	// the queue. This consists of all split keys, with KeyMin as an additional
	// start and KeyMax as an additional end.
	expectedStartKeys := []roachpb.Key{roachpb.Key("a"), roachpb.Key("b")}

	expectedEndKeys := []roachpb.Key{roachpb.Key("b"), roachpb.Key("c")}

	// Force replica scan to run, which will populate the model.
	now := store.Clock().Now()
	if err := store.ForceTimeSeriesMaintenanceQueueProcess(); err != nil {
		t.Fatal(err)
	}

	// Wait for processing to complete.
	testutils.SucceedsSoon(t, func() error {
		model.Lock()
		defer model.Unlock()
		// containsCalled is dependent on the number of ranges in the cluster, which
		// is larger than the ones we've created.
		if a, e := model.containsCalled, len(expectedStartKeys); a < e {
			return fmt.Errorf("ContainsTimeSeries called %d times; expected %d", a, e)
		}
		if a, e := model.pruneCalled, len(expectedStartKeys); a != e {
			return fmt.Errorf("MaintainTimeSeries called %d times; expected %d", a, e)
		}
		return nil
	})

	model.Lock()
	if a, e := model.pruneSeenStartKeys, expectedStartKeys; !reflect.DeepEqual(a, e) {
		t.Errorf("start keys seen by MaintainTimeSeries did not match expectation: %s", pretty.Diff(a, e))
	}
	if a, e := model.pruneSeenEndKeys, expectedEndKeys; !reflect.DeepEqual(a, e) {
		t.Errorf("end keys seen by MaintainTimeSeries did not match expectation: %s", pretty.Diff(a, e))
	}
	model.Unlock()

	testutils.SucceedsSoon(t, func() error {
		for _, key := range expectedStartKeys {
			repl := store.LookupReplica(roachpb.RKey(key))
			ts, err := repl.GetQueueLastProcessed(ctx, "timeSeriesMaintenance")
			if err != nil {
				return err
			}
			if ts.Less(now) {
				return errors.Errorf("expected last processed (%s) %s > %s", repl, ts, now)
			}
		}
		return nil
	})

	// Force replica scan to run. But because we haven't moved the
	// clock forward, no pruning will take place on second invocation.
	if err := store.ForceTimeSeriesMaintenanceQueueProcess(); err != nil {
		t.Fatal(err)
	}
	model.Lock()
	if a, e := model.containsCalled, len(expectedStartKeys); a < e {
		t.Errorf("ContainsTimeSeries called %d times; expected %d", a, e)
	}
	if a, e := model.pruneCalled, len(expectedStartKeys); a != e {
		t.Errorf("MaintainTimeSeries called %d times; expected %d", a, e)
	}
	model.Unlock()

	// Move clock forward and force to scan again.
	manual.Increment(kvserver.TimeSeriesMaintenanceInterval.Nanoseconds())
	if err := store.ForceTimeSeriesMaintenanceQueueProcess(); err != nil {
		t.Fatal(err)
	}
	testutils.SucceedsSoon(t, func() error {
		model.Lock()
		defer model.Unlock()
		// containsCalled is dependent on the number of ranges in the cluster, which
		// is larger than the ones we've created.
		if a, e := model.containsCalled, len(expectedStartKeys)*2; a < e {
			return errors.Errorf("ContainsTimeSeries called %d times; expected %d", a, e)
		}
		if a, e := model.pruneCalled, len(expectedStartKeys)*2; a != e {
			return errors.Errorf("MaintainTimeSeries called %d times; expected %d", a, e)
		}
		return nil
	})
}

// TestTimeSeriesMaintenanceQueueServer verifies that the time series
// maintenance queue runs correctly on a test server.
func TestTimeSeriesMaintenanceQueueServer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableScanner: true,
			},
		},
	})
	defer s.Stopper().Stop(context.Background())
	tsrv := s.(*server.TestServer)
	tsdb := tsrv.TsDB()

	// Populate time series data into the server. One time series, with one
	// datapoint at the current time and two datapoints older than the pruning
	// threshold. Datapoint timestamps are set to the midpoint of sample duration
	// periods; this simplifies verification.
	seriesName := "test.metric"
	sourceName := "source1"
	now := tsrv.Clock().PhysicalNow()
	nearPast := now - (tsdb.PruneThreshold(ts.Resolution10s) * 2)
	farPast := now - (tsdb.PruneThreshold(ts.Resolution10s) * 4)
	sampleDuration := ts.Resolution10s.SampleDuration()
	datapoints := []tspb.TimeSeriesDatapoint{
		{
			TimestampNanos: farPast - farPast%sampleDuration,
			Value:          100.0,
		},
		{
			TimestampNanos: nearPast - (nearPast)%sampleDuration,
			Value:          200.0,
		},
		{
			TimestampNanos: now - now%sampleDuration,
			Value:          300.0,
		},
	}
	if err := tsdb.StoreData(context.Background(), ts.Resolution10s, []tspb.TimeSeriesData{
		{
			Name:       seriesName,
			Source:     sourceName,
			Datapoints: datapoints,
		},
	}); err != nil {
		t.Fatal(err)
	}

	// Generate a split key at a timestamp halfway between near past and far past.
	splitKey := ts.MakeDataKey(
		seriesName, sourceName, ts.Resolution10s, farPast+(nearPast-farPast)/2,
	)

	// Force a range split in between near past and far past. This guarantees
	// that the pruning operation will issue a DeleteRange which spans ranges.
	if err := db.AdminSplit(context.Background(), splitKey, hlc.MaxTimestamp /* expirationTime */); err != nil {
		t.Fatal(err)
	}

	memMon := mon.NewMonitor(
		"test",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment: use default block size */
		math.MaxInt64, /* noteworthy */
		cluster.MakeTestingClusterSettings(),
	)
	memMon.Start(context.Background(), nil /* pool */, mon.MakeStandaloneBudget(math.MaxInt64))
	defer memMon.Stop(context.Background())
	memContext := ts.MakeQueryMemoryContext(
		memMon,
		memMon,
		ts.QueryMemoryOptions{
			BudgetBytes:             math.MaxInt64 / 8,
			EstimatedSources:        1,
			InterpolationLimitNanos: 0,
		},
	)
	defer memContext.Close(context.Background())

	// getDatapoints queries all datapoints in the series from the beginning
	// of time to a point in the near future.
	getDatapoints := func() ([]tspb.TimeSeriesDatapoint, error) {
		dps, _, err := tsdb.Query(
			context.Background(),
			tspb.Query{Name: seriesName},
			ts.Resolution10s,
			ts.QueryTimespan{
				SampleDurationNanos: ts.Resolution10s.SampleDuration(),
				StartNanos:          0,
				EndNanos:            now + ts.Resolution10s.SlabDuration(),
				NowNanos:            now + (10 * time.Hour).Nanoseconds(),
			},
			memContext,
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
		t.Fatalf("error retrieving store %d: %+v", storeID, err)
	}
	if err := store.ForceTimeSeriesMaintenanceQueueProcess(); err != nil {
		t.Fatal(err)
	}

	// Verify the older datapoint has been pruned.
	testutils.SucceedsSoon(t, func() error {
		actualDatapoints, err = getDatapoints()
		if err != nil {
			return err
		}
		if a, e := actualDatapoints, datapoints[2:]; !reflect.DeepEqual(a, e) {
			return fmt.Errorf("got datapoints %v, expected %v, diff: %s", a, e, pretty.Diff(a, e))
		}
		return nil
	})
}
