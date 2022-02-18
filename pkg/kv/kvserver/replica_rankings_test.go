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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sstutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestReplicaRankings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rr := newReplicaRankings()

	testCases := []struct {
		replicasByQPS []float64
	}{
		{replicasByQPS: []float64{}},
		{replicasByQPS: []float64{0}},
		{replicasByQPS: []float64{1, 0}},
		{replicasByQPS: []float64{3, 2, 1, 0}},
		{replicasByQPS: []float64{3, 3, 2, 2, 1, 1, 0, 0}},
		{replicasByQPS: []float64{1.1, 1.0, 0.9, -0.9, -1.0, -1.1}},
	}

	for _, tc := range testCases {
		acc := rr.newAccumulator()

		// Randomize the order of the inputs each time the test is run.
		want := make([]float64, len(tc.replicasByQPS))
		copy(want, tc.replicasByQPS)
		rand.Shuffle(len(tc.replicasByQPS), func(i, j int) {
			tc.replicasByQPS[i], tc.replicasByQPS[j] = tc.replicasByQPS[j], tc.replicasByQPS[i]
		})

		for i, replQPS := range tc.replicasByQPS {
			acc.addReplica(replicaWithStats{
				repl: &Replica{RangeID: roachpb.RangeID(i)},
				qps:  replQPS,
			})
		}
		rr.update(acc)

		// Make sure we can read off all expected replicas in the correct order.
		repls := rr.topQPS()
		if len(repls) != len(want) {
			t.Errorf("wrong number of replicas in output; got: %v; want: %v", repls, tc.replicasByQPS)
			continue
		}
		for i := range want {
			if repls[i].qps != want[i] {
				t.Errorf("got %f for %d'th element; want %f (input: %v)", repls[i].qps, i, want, tc.replicasByQPS)
				break
			}
		}
		replsCopy := rr.topQPS()
		if !reflect.DeepEqual(repls, replsCopy) {
			t.Errorf("got different replicas on second call to topQPS; first call: %v, second call: %v", repls, replsCopy)
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
	sstKeys := make([]sstutil.KV, 200)
	for i := range sstKeys {
		sstKeys[i] = sstutil.KV{
			KeyString:     nextKey.String(),
			WallTimestamp: 1,
			ValueString:   "value",
		}
		nextKey = nextKey.Next()
	}
	sst, start, end := sstutil.MakeSST(t, ts.ClusterSettings(), sstKeys)
	requestSize := float64(len(sst))

	sstReq := &roachpb.AddSSTableRequest{
		RequestHeader: roachpb.RequestHeader{Key: start, EndKey: end},
		Data:          sst,
		MVCCStats:     sstutil.ComputeStats(t, sst),
	}

	get := &roachpb.GetRequest{
		RequestHeader: roachpb.RequestHeader{Key: start},
	}

	addSSTBA := roachpb.BatchRequest{}
	nonSSTBA := roachpb.BatchRequest{}
	addSSTBA.Add(sstReq)
	nonSSTBA.Add(get)

	// When the factor is set to 0, it is disabled and we expect uniform 1 QPS.
	// In all other cases, we expect 1 + the size of a
	// AddSSTableRequest/factor. If no AddSStableRequest exists within the
	// request, it should be cost 1, regardless of factor.
	testCases := []struct {
		addsstRequestFactor int
		expectedQPS         float64
		ba                  roachpb.BatchRequest
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
		repl.leaseholderStats.resetRequestCounts()

		_, pErr = db.NonTransactionalSender().Send(ctx, testCase.ba)
		require.Nil(t, pErr)

		repl.leaseholderStats.mu.Lock()
		queriesAfter, _ := repl.leaseholderStats.sumQueriesLocked()
		repl.leaseholderStats.mu.Unlock()

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
