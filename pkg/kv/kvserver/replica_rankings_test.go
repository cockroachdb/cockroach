// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	aload "github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/load"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/load"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestReplicaRankings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dimensions := []aload.Dimension{aload.Queries, aload.CPU}
	rr := NewReplicaRankings()

	testCases := []struct {
		replicasByLoad []float64
	}{
		{replicasByLoad: []float64{}},
		{replicasByLoad: []float64{0}},
		{replicasByLoad: []float64{1, 0}},
		{replicasByLoad: []float64{3, 2, 1, 0}},
		{replicasByLoad: []float64{3, 3, 2, 2, 1, 1, 0, 0}},
		{replicasByLoad: []float64{1.1, 1.0, 0.9, -0.9, -1.0, -1.1}},
	}

	for _, tc := range testCases {
		for _, dimension := range dimensions {
			acc := NewReplicaAccumulator(dimensions...)

			// Randomize the order of the inputs each time the test is run. Also make
			// a copy so that we can test on the copy for each dimension without
			// mutating the underlying test case slice.
			rLoad := make([]float64, len(tc.replicasByLoad))
			want := make([]float64, len(tc.replicasByLoad))
			copy(want, tc.replicasByLoad)
			copy(rLoad, tc.replicasByLoad)

			rand.Shuffle(len(rLoad), func(i, j int) {
				rLoad[i], rLoad[j] = rLoad[j], rLoad[i]
			})

			for i, replLoad := range rLoad {
				acc.AddReplica(candidateReplica{
					Replica: &Replica{RangeID: roachpb.RangeID(i)},
					usage: allocator.RangeUsageInfo{
						// We should get the same ordering for both QPS and CPU.
						QueriesPerSecond:         replLoad,
						RequestCPUNanosPerSecond: replLoad,
					},
				})
			}
			rr.Update(acc)

			// Make sure we can read off all expected replicas in the correct order.
			repls := rr.TopLoad(dimension)
			if len(repls) != len(want) {
				t.Errorf("wrong number of replicas in output; got: %v; want: %v", repls, rLoad)
				continue
			}
			for i := range want {
				if repls[i].RangeUsageInfo().Load().Dim(dimension) != want[i] {
					t.Errorf("got %f for %d'th element; want %f (input: %v)",
						repls[i].RangeUsageInfo().Load().Dim(dimension), i, want, rLoad)
					break
				}
			}
			replsCopy := rr.TopLoad(dimension)
			if !reflect.DeepEqual(repls, replsCopy) {
				t.Errorf("got different replicas on second call to topQPS; first call: %v, second call: %v", repls, replsCopy)
			}
		}
	}
}

// TestAddSSTQPSStat verifies that AddSSTableRequests are accounted for
// differently, when present in a BatchRequest, with a divisor set.
func TestAddSSTQPSStat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})

	defer tc.Stopper().Stop(ctx)
	ts := tc.Server(0)
	db := ts.DB()
	conn := tc.ServerConn(0)
	sqlDB := sqlutils.MakeSQLRunner(conn)

	scratchKey := tc.ScratchRange(t)
	nextKey := scratchKey.Next()

	// Construct an sst with 200 keys that will be reused with different divisors.
	sstKeys := make(storageutils.KVs, 200)
	for i := range sstKeys {
		sstKeys[i] = storageutils.PointKV(nextKey.String(), 1, "value")
		nextKey = nextKey.Next()
	}
	sst, start, end := storageutils.MakeSST(t, ts.ClusterSettings(), sstKeys)
	requestSize := float64(len(sst))

	sstReq := &kvpb.AddSSTableRequest{
		RequestHeader: kvpb.RequestHeader{Key: start, EndKey: end},
		Data:          sst,
		MVCCStats:     storageutils.SSTStats(t, sst, 0),
	}

	get := &kvpb.GetRequest{
		RequestHeader: kvpb.RequestHeader{Key: start},
	}

	addSSTBA := &kvpb.BatchRequest{}
	nonSSTBA := &kvpb.BatchRequest{}
	addSSTBA.Add(sstReq)
	nonSSTBA.Add(get)

	// When the factor is set to 0, it is disabled and we expect uniform 1 QPS.
	// In all other cases, we expect 1 + the size of a
	// AddSSTableRequest/factor. If no AddSStableRequest exists within the
	// request, it should be cost 1, regardless of factor.
	testCases := []struct {
		addsstRequestFactor int
		expectedQPS         float64
		ba                  *kvpb.BatchRequest
	}{
		{0, 1, addSSTBA},
		{100, 1, nonSSTBA},
		{10, 1 + requestSize/10, addSSTBA},
		{20, 1 + requestSize/20, addSSTBA},
		{40, 1 + requestSize/40, addSSTBA},
		{100, 1 + requestSize/100, addSSTBA},
	}

	// Send an AddSSTRequest once to create the key range.
	_, pErr := db.NonTransactionalSender().Send(ctx, addSSTBA)
	require.Nil(t, pErr)

	store, err := ts.GetStores().(*Stores).GetStore(ts.GetFirstStoreID())
	require.NoError(t, err)

	repl := store.LookupReplica(roachpb.RKey(start))
	require.NotNil(t, repl)

	// Disable the consistency checker, to avoid interleaving requests
	// artificially inflating QPS due to consistency checking.
	sqlDB.Exec(t, `SET CLUSTER SETTING server.consistency_check.interval = '0'`)

	for _, testCase := range testCases {
		sqlDB.Exec(t, fmt.Sprintf(`SET CLUSTER setting kv.replica_stats.addsst_request_size_factor = %d`, testCase.addsstRequestFactor))

		// Reset the request counts to 0 before sending to clear previous requests.
		repl.loadStats.Reset()

		_, pErr = db.NonTransactionalSender().Send(ctx, testCase.ba)
		require.Nil(t, pErr)

		queriesAfter := repl.loadStats.TestingGetSum(load.Queries)
		// If queries are correctly recorded, we should see increase in query
		// count by the expected QPS. However, it is possible to to get a
		// slightly higher number due to interleaving requests. To avoid a
		// flakey test, we assert that QPS is at least as high as expected,
		// then no greater than 4 requests of expected QPS. If this test is
		// flaky, increase the delta to account for background activity
		// interleaving with measurements.
		require.GreaterOrEqual(t, queriesAfter, testCase.expectedQPS)
		require.InDelta(t, queriesAfter, testCase.expectedQPS, 4)
	}
}

// genVariableRead returns a batch request containing, start-end sequential key reads.
func genVariableRead(ctx context.Context, start, end roachpb.Key) *kvpb.BatchRequest {
	scan := kvpb.NewScan(start, end, false)
	readBa := &kvpb.BatchRequest{}
	readBa.Add(scan)
	return readBa
}

func assertGreaterThanInDelta(t *testing.T, expected float64, actual float64, delta float64) {
	require.GreaterOrEqual(t, actual, expected)
	require.InDelta(t, expected, actual, delta)
}

func TestWriteLoadStatsAccounting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	args := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	}
	args.ServerArgs.Knobs.Store = &StoreTestingKnobs{DisableCanAckBeforeApplication: true}
	tc := serverutils.StartNewTestCluster(t, 1, args)

	const epsilonAllowed = 4

	defer tc.Stopper().Stop(ctx)
	ts := tc.Server(0)
	db := ts.DB()
	conn := tc.ServerConn(0)
	sqlDB := sqlutils.MakeSQLRunner(conn)

	writeSize := float64(9)

	scratchKey := tc.ScratchRange(t)
	testCases := []struct {
		writes       int
		expectedRQPS float64
		expectedWPS  float64
		expectedRPS  float64
		expectedWBPS float64
		expectedRBPS float64
	}{
		{1, 1, 1, 0, writeSize, 0},
		{4, 4, 4, 0, 4 * writeSize, 0},
		{64, 64, 64, 0, 64 * writeSize, 0},
		{111, 111, 111, 0, 111 * writeSize, 0},
		{1234, 1234, 1234, 0, 1234 * writeSize, 0},
	}

	store, err := ts.GetStores().(*Stores).GetStore(ts.GetFirstStoreID())
	require.NoError(t, err)

	repl := store.LookupReplica(roachpb.RKey(scratchKey))
	require.NotNil(t, repl)

	// Disable the consistency checker, to avoid interleaving requests
	// artificially inflating measurement due to consistency checking.
	sqlDB.Exec(t, `SET CLUSTER SETTING server.consistency_check.interval = '0'`)
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.range_split.by_load_enabled = false`)

	for _, testCase := range testCases {
		// This test can flake, where an errant request - not sent here
		// (commonly intent resolution) will artifically inflate the collected
		// metrics. This results in unexpected read/write statistics and a
		// flakey test every few hundred runs. Here we assert that the run
		// should succeed soon, if it fails on the first.
		testutils.SucceedsSoon(t, func() error {
			// Reset the request counts to 0 before sending to clear previous requests.
			repl.loadStats.Reset()

			requestsBefore := repl.loadStats.TestingGetSum(load.Requests)
			writesBefore := repl.loadStats.TestingGetSum(load.WriteKeys)
			readsBefore := repl.loadStats.TestingGetSum(load.ReadKeys)
			readBytesBefore := repl.loadStats.TestingGetSum(load.ReadBytes)
			writeBytesBefore := repl.loadStats.TestingGetSum(load.WriteBytes)

			for i := 0; i < testCase.writes; i++ {
				_, pErr := db.Inc(ctx, scratchKey, 1)
				require.Nil(t, pErr)
			}
			require.Equal(t, 0.0, requestsBefore)
			require.Equal(t, 0.0, writesBefore)
			require.Equal(t, 0.0, readsBefore)
			require.Equal(t, 0.0, writeBytesBefore)
			require.Equal(t, 0.0, readBytesBefore)

			requestsAfter := repl.loadStats.TestingGetSum(load.Requests)
			writesAfter := repl.loadStats.TestingGetSum(load.WriteKeys)
			readsAfter := repl.loadStats.TestingGetSum(load.ReadKeys)
			readBytesAfter := repl.loadStats.TestingGetSum(load.ReadBytes)
			writeBytesAfter := repl.loadStats.TestingGetSum(load.WriteBytes)

			assertGreaterThanInDelta(t, testCase.expectedRQPS, requestsAfter, epsilonAllowed)
			assertGreaterThanInDelta(t, testCase.expectedWPS, writesAfter, epsilonAllowed)
			assertGreaterThanInDelta(t, testCase.expectedRPS, readsAfter, epsilonAllowed)
			assertGreaterThanInDelta(t, testCase.expectedRBPS, readBytesAfter, epsilonAllowed)
			// NB: We assert that the written bytes is greater than the write
			// batch request size. However the size multiplication factor,
			// varies between 3 and 5 so we instead assert that it is greater
			// than the logical bytes.
			require.GreaterOrEqual(t, writeBytesAfter, testCase.expectedWBPS)
			return nil
		})
	}
}

func TestReadLoadMetricAccounting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})

	defer tc.Stopper().Stop(ctx)
	ts := tc.Server(0)
	db := ts.DB()
	conn := tc.ServerConn(0)
	sqlDB := sqlutils.MakeSQLRunner(conn)

	const epsilonAllowed = 4

	scratchKey := tc.ScratchRange(t)
	nextKey := scratchKey.Next()

	scratchKeys := make([]roachpb.Key, 300)
	sstKeys := make(storageutils.KVs, 300)
	for i := range sstKeys {
		scratchKeys[i] = nextKey
		sstKeys[i] = storageutils.PointKV(nextKey.String(), 1, "value")
		nextKey = nextKey.Next()
	}
	sst, start, end := storageutils.MakeSST(t, ts.ClusterSettings(), sstKeys)
	sstReq := &kvpb.AddSSTableRequest{
		RequestHeader: kvpb.RequestHeader{Key: start, EndKey: end},
		Data:          sst,
		MVCCStats:     storageutils.SSTStats(t, sst, 0),
	}

	addSSTBA := &kvpb.BatchRequest{}
	addSSTBA.Add(sstReq)

	// Send an AddSSTRequest once to create the key range.
	_, pErr := db.NonTransactionalSender().Send(ctx, addSSTBA)
	require.Nil(t, pErr)

	get := &kvpb.GetRequest{
		RequestHeader: kvpb.RequestHeader{Key: start},
	}

	getReadBA := &kvpb.BatchRequest{}
	getReadBA.Add(get)

	scan := &kvpb.ScanRequest{
		RequestHeader: kvpb.RequestHeader{Key: start, EndKey: end},
	}

	scanReadBA := &kvpb.BatchRequest{}
	scanReadBA.Add(scan)

	testCases := []struct {
		ba           *kvpb.BatchRequest
		expectedRQPS float64
		expectedWPS  float64
		expectedRPS  float64
		expectedWBPS float64
		expectedRBPS float64
	}{
		{getReadBA, 1, 0, 1, 0, 10},
		{genVariableRead(ctx, start, sstKeys[1].(storage.MVCCKeyValue).Key.Key), 1, 0, 1, 0, 38},
		{genVariableRead(ctx, start, sstKeys[4].(storage.MVCCKeyValue).Key.Key), 1, 0, 4, 0, 176},
		{genVariableRead(ctx, start, sstKeys[64].(storage.MVCCKeyValue).Key.Key), 1, 0, 64, 0, 10496},
	}

	store, err := ts.GetStores().(*Stores).GetStore(ts.GetFirstStoreID())
	require.NoError(t, err)

	repl := store.LookupReplica(roachpb.RKey(start))
	require.NotNil(t, repl)

	replEnd := store.LookupReplica(roachpb.RKey(end))
	require.NotNil(t, repl)

	require.EqualValues(t, repl, replEnd)

	// Disable the consistency checker, to avoid interleaving requests
	// artificially inflating measurement due to consistency checking.
	sqlDB.Exec(t, `SET CLUSTER SETTING server.consistency_check.interval = '0'`)
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.range_split.by_load_enabled = false`)

	for _, testCase := range testCases {
		// This test can flake, where an errant request - not sent here
		// (commonly intent resolution) will artifically inflate the collected
		// metrics. This results in unexpected read/write statistics and a
		// flakey test every few hundred runs. Here we assert that the run
		// should succeed soon, if it fails on the first.
		testutils.SucceedsSoon(t, func() error {
			// Reset the request counts to 0 before sending to clear previous requests.
			// Reset the request counts to 0 before sending to clear previous requests.
			repl.loadStats.Reset()

			requestsBefore := repl.loadStats.TestingGetSum(load.Requests)
			writesBefore := repl.loadStats.TestingGetSum(load.WriteKeys)
			readsBefore := repl.loadStats.TestingGetSum(load.ReadKeys)
			readBytesBefore := repl.loadStats.TestingGetSum(load.ReadBytes)
			writeBytesBefore := repl.loadStats.TestingGetSum(load.WriteBytes)

			_, pErr = db.NonTransactionalSender().Send(ctx, testCase.ba)
			require.Nil(t, pErr)

			require.Equal(t, 0.0, requestsBefore)
			require.Equal(t, 0.0, writesBefore)
			require.Equal(t, 0.0, readsBefore)
			require.Equal(t, 0.0, writeBytesBefore)
			require.Equal(t, 0.0, readBytesBefore)

			requestsAfter := repl.loadStats.TestingGetSum(load.Requests)
			writesAfter := repl.loadStats.TestingGetSum(load.WriteKeys)
			readsAfter := repl.loadStats.TestingGetSum(load.ReadKeys)
			readBytesAfter := repl.loadStats.TestingGetSum(load.ReadBytes)
			writeBytesAfter := repl.loadStats.TestingGetSum(load.WriteBytes)

			assertGreaterThanInDelta(t, testCase.expectedRQPS, requestsAfter, epsilonAllowed)
			assertGreaterThanInDelta(t, testCase.expectedWPS, writesAfter, epsilonAllowed)
			assertGreaterThanInDelta(t, testCase.expectedRPS, readsAfter, epsilonAllowed)
			// Increase the allowance for write bytes, as although this should
			// be zero - there exists some cases where a write will interleave
			// with our testing. This causes the test to flake if it occurs repeatedly.
			// TODO(kvoli): This can be determinsitic, with an general
			// interface that records the type of request or otherwise filters
			// out unwanted types.
			assertGreaterThanInDelta(t, testCase.expectedWBPS, writeBytesAfter, epsilonAllowed*15)
			assertGreaterThanInDelta(t, testCase.expectedRBPS, readBytesAfter, epsilonAllowed)

			return nil
		})
	}
}

func TestNewReplicaRankingsMap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rr := NewReplicaRankingsMap()

	type testCase struct {
		tenantID uint64
		qps      float64
	}

	testCases := [][]testCase{
		{},
		{{1, 1}, {1, 2}, {1, 3}, {1, 4}},
		{{1, 1}, {1, 2}, {2, 0}, {3, 0}},
		{{1, 1}, {1, 2}, {1, 3}, {1, 4},
			{2, 1}, {2, 2}, {2, 3}, {2, 4},
			{3, 1}, {3, 2}, {3, 3}, {3, 4}},
	}

	for _, tc := range testCases {
		acc := NewTenantReplicaAccumulator(aload.Queries, aload.CPU)

		// Randomize the order of the inputs each time the test is run.
		rand.Shuffle(len(tc), func(i, j int) {
			tc[i], tc[j] = tc[j], tc[i]
		})

		expectedReplicasPerTenant := make(map[uint64]int)

		for i, c := range tc {
			cr := candidateReplica{
				Replica: &Replica{RangeID: roachpb.RangeID(i)},
				usage:   allocator.RangeUsageInfo{QueriesPerSecond: c.qps},
			}
			cr.mu.tenantID = roachpb.MustMakeTenantID(c.tenantID)
			acc.AddReplica(cr)

			if l, ok := expectedReplicasPerTenant[c.tenantID]; ok {
				expectedReplicasPerTenant[c.tenantID] = l + 1
			} else {
				expectedReplicasPerTenant[c.tenantID] = 1
			}
		}
		rr.Update(acc)

		for tID, count := range expectedReplicasPerTenant {
			repls := rr.TopLoad(roachpb.MustMakeTenantID(tID), aload.Queries)
			if len(repls) != count {
				t.Errorf("wrong number of replicas in output; got: %v; want: %v", len(repls), count)
				continue
			}
			for i := 0; i < len(repls)-1; i++ {
				if repls[i].RangeUsageInfo().QueriesPerSecond < repls[i+1].RangeUsageInfo().QueriesPerSecond {
					t.Errorf("got %f for %d'th element; it's smaller than QPS of the next element %f", repls[i].RangeUsageInfo().QueriesPerSecond, i, repls[i+1].RangeUsageInfo().QueriesPerSecond)
					break
				}
			}
			replsCopy := rr.TopLoad(roachpb.MustMakeTenantID(tID), aload.Queries)
			for i := 0; i < len(repls); i++ {
				if repls[i].RangeUsageInfo().QueriesPerSecond != replsCopy[i].RangeUsageInfo().QueriesPerSecond {
					t.Errorf("got different results Range ID: %d, QPS: %f, second call: Range ID: %d, QPS: %f", repls[i].GetRangeID(), repls[i].RangeUsageInfo().QueriesPerSecond, replsCopy[i].GetRangeID(), replsCopy[i].RangeUsageInfo().QueriesPerSecond)
				}
			}
		}
	}
}
