// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvfollowerreadsccl

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan/replicaoracle"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

const (
	defaultInterval                          = 3
	defaultFraction                          = .2
	defaultMultiple                          = 3
	expectedFollowerReadOffset time.Duration = 1e9 * /* 1 second */
		-defaultInterval * (1 + defaultFraction*defaultMultiple)
)

func TestEvalFollowerReadOffset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	disableEnterprise := utilccl.TestingEnableEnterprise()
	defer disableEnterprise()
	st := cluster.MakeTestingClusterSettings()
	if offset, err := evalFollowerReadOffset(uuid.MakeV4(), st); err != nil {
		t.Fatal(err)
	} else if offset != expectedFollowerReadOffset {
		t.Fatalf("expected %v, got %v", expectedFollowerReadOffset, offset)
	}
	disableEnterprise()
	_, err := evalFollowerReadOffset(uuid.MakeV4(), st)
	if !testutils.IsError(err, "requires an enterprise license") {
		t.Fatalf("failed to get error when evaluating follower read offset without " +
			"an enterprise license")
	}
}

func TestCanSendToFollower(t *testing.T) {
	defer leaktest.AfterTest(t)()
	disableEnterprise := utilccl.TestingEnableEnterprise()
	defer disableEnterprise()
	st := cluster.MakeTestingClusterSettings()
	kvserver.FollowerReadsEnabled.Override(&st.SV, true)

	old := hlc.Timestamp{
		WallTime: timeutil.Now().Add(2 * expectedFollowerReadOffset).UnixNano(),
	}
	oldHeader := roachpb.Header{Txn: &roachpb.Transaction{
		ReadTimestamp: old,
	}}
	rw := roachpb.BatchRequest{Header: oldHeader}
	rw.Add(&roachpb.PutRequest{})
	if canSendToFollower(uuid.MakeV4(), st, rw) {
		t.Fatalf("should not be able to send a rw request to a follower")
	}
	roNonTxn := roachpb.BatchRequest{Header: oldHeader}
	roNonTxn.Add(&roachpb.QueryTxnRequest{})
	if canSendToFollower(uuid.MakeV4(), st, roNonTxn) {
		t.Fatalf("should not be able to send a non-transactional ro request to a follower")
	}
	roNoTxn := roachpb.BatchRequest{}
	roNoTxn.Add(&roachpb.GetRequest{})
	if canSendToFollower(uuid.MakeV4(), st, roNoTxn) {
		t.Fatalf("should not be able to send a batch with no txn to a follower")
	}
	roOld := roachpb.BatchRequest{Header: oldHeader}
	roOld.Add(&roachpb.GetRequest{})
	if !canSendToFollower(uuid.MakeV4(), st, roOld) {
		t.Fatalf("should be able to send an old ro batch to a follower")
	}
	roRWTxnOld := roachpb.BatchRequest{Header: roachpb.Header{
		Txn: &roachpb.Transaction{
			TxnMeta:       enginepb.TxnMeta{Key: []byte("key")},
			ReadTimestamp: old,
		},
	}}
	roRWTxnOld.Add(&roachpb.GetRequest{})
	if canSendToFollower(uuid.MakeV4(), st, roRWTxnOld) {
		t.Fatalf("should not be able to send a ro request from a rw txn to a follower")
	}
	kvserver.FollowerReadsEnabled.Override(&st.SV, false)
	if canSendToFollower(uuid.MakeV4(), st, roOld) {
		t.Fatalf("should not be able to send an old ro batch to a follower when follower reads are disabled")
	}
	kvserver.FollowerReadsEnabled.Override(&st.SV, true)
	roNew := roachpb.BatchRequest{Header: roachpb.Header{
		Txn: &roachpb.Transaction{
			ReadTimestamp: hlc.Timestamp{WallTime: timeutil.Now().UnixNano()},
		},
	}}
	if canSendToFollower(uuid.MakeV4(), st, roNew) {
		t.Fatalf("should not be able to send a new ro batch to a follower")
	}
	roOldWithNewMax := roachpb.BatchRequest{Header: roachpb.Header{
		Txn: &roachpb.Transaction{
			MaxTimestamp: hlc.Timestamp{WallTime: timeutil.Now().UnixNano()},
		},
	}}
	roOldWithNewMax.Add(&roachpb.GetRequest{})
	if canSendToFollower(uuid.MakeV4(), st, roNew) {
		t.Fatalf("should not be able to send a ro batch with new MaxTimestamp to a follower")
	}
	disableEnterprise()
	if canSendToFollower(uuid.MakeV4(), st, roOld) {
		t.Fatalf("should not be able to send an old ro batch to a follower without enterprise enabled")
	}
}

func TestFollowerReadMultipleValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic from setting followerReadMultiple to .1")
		}
	}()
	st := cluster.MakeTestingClusterSettings()
	followerReadMultiple.Override(&st.SV, .1)
}

// TestOracle tests the OracleFactory exposed by this package.
// This test ends up being rather indirect but works by checking if the type
// of the oracle returned from the factory differs between requests we'd
// expect to support follower reads and that which we'd expect not to.
func TestOracleFactory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	disableEnterprise := utilccl.TestingEnableEnterprise()
	defer disableEnterprise()
	st := cluster.MakeTestingClusterSettings()
	kvserver.FollowerReadsEnabled.Override(&st.SV, true)
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	c := kv.NewDB(log.AmbientContext{
		Tracer: tracing.NewTracer(),
	}, kv.MockTxnSenderFactory{}, hlc.NewClock(hlc.UnixNano, time.Nanosecond), stopper)
	txn := kv.NewTxn(context.Background(), c, 0)
	of := replicaoracle.NewOracleFactory(followerReadAwareChoice, replicaoracle.Config{
		Settings:   st,
		RPCContext: rpcContext,
	})
	noFollowerReadOracle := of.Oracle(txn)
	old := hlc.Timestamp{
		WallTime: timeutil.Now().Add(2 * expectedFollowerReadOffset).UnixNano(),
	}
	txn.SetFixedTimestamp(context.Background(), old)
	followerReadOracle := of.Oracle(txn)
	if reflect.TypeOf(followerReadOracle) == reflect.TypeOf(noFollowerReadOracle) {
		t.Fatalf("expected types of %T and %T to differ", followerReadOracle,
			noFollowerReadOracle)
	}
	disableEnterprise()
	disabledFollowerReadOracle := of.Oracle(txn)
	if reflect.TypeOf(disabledFollowerReadOracle) != reflect.TypeOf(noFollowerReadOracle) {
		t.Fatalf("expected types of %T and %T not to differ", disabledFollowerReadOracle,
			noFollowerReadOracle)
	}
}

// Test that follower reads recover from a situation where a gateway node has
// the right leaseholder cached, but stale followers. This is an integration
// test checking that the cache on the gateway gets updated by the first request
// encountering this situation, and then follower reads work.
func TestFollowerReadsWithStaleDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// This test sleeps for a few sec.
	skip.UnderShort(t)

	ctx := context.Background()
	// The test uses follower_read_timestamp().
	defer utilccl.TestingEnableEnterprise()()

	historicalQuery := `SELECT * FROM test AS OF SYSTEM TIME follower_read_timestamp() WHERE k=2`
	recCh := make(chan tracing.Recording, 1)

	var n2Addr, n3Addr syncutil.AtomicString
	tc := testcluster.StartTestCluster(t, 4,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs:      base.TestServerArgs{UseDatabase: "t"},
			// n4 pretends to have low latency to n2 and n3, so that it tries to use
			// them for follower reads.
			// Also, we're going to collect a trace of the test's final query.
			ServerArgsPerNode: map[int]base.TestServerArgs{
				3: {
					UseDatabase: "t",
					Knobs: base.TestingKnobs{
						KVClient: &kvcoord.ClientTestingKnobs{
							LatencyFunc: func(addr string) (time.Duration, bool) {
								if (addr == n2Addr.Get()) || (addr == n3Addr.Get()) {
									return time.Millisecond, true
								}
								return 100 * time.Millisecond, true
							},
						},
						SQLExecutor: &sql.ExecutorTestingKnobs{
							WithStatementTrace: func(trace tracing.Recording, stmt string) {
								if stmt == historicalQuery {
									recCh <- trace
								}
							},
						},
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)
	n2Addr.Set(tc.Servers[1].RPCAddr())
	n3Addr.Set(tc.Servers[2].RPCAddr())

	n1 := sqlutils.MakeSQLRunner(tc.Conns[0])
	n1.Exec(t, `CREATE DATABASE t`)
	n1.Exec(t, `CREATE TABLE test (k INT PRIMARY KEY)`)
	n1.Exec(t, `ALTER TABLE test EXPERIMENTAL_RELOCATE VALUES (ARRAY[1,2], 1)`)
	// Speed up closing of timestamps, as we'll in order to be able to use
	// follower_read_timestamp().
	// Every 0.2s we'll close the timestamp from 0.4s ago. We'll attempt follower reads
	// for anything below 0.4s * (1 + 0.5 * 20) = 4.4s.
	n1.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '0.4s'`)
	n1.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.close_fraction = 0.5`)
	n1.Exec(t, `SET CLUSTER SETTING kv.follower_read.target_multiple = 20`)

	// Sleep so that we can perform follower reads. The read timestamp needs to be
	// above the timestamp when the table was created. See the calculation above
	// for the sleep duration.
	log.Infof(ctx, "test sleeping for the follower read timestamps to pass the table creation timestamp...")
	time.Sleep(4400 * time.Millisecond)
	log.Infof(ctx, "test sleeping... done")

	// Run a query on n4 to populate its cache.
	n4 := sqlutils.MakeSQLRunner(tc.Conns[3])
	n4.Exec(t, "SELECT * from test WHERE k=1")
	// Check that the cache was indeed populated.
	var tableID uint32
	n1.QueryRow(t, `SELECT id from system.namespace2 WHERE name='test'`).Scan(&tableID)
	tablePrefix := keys.MustAddr(keys.SystemSQLCodec.TablePrefix(tableID))
	n4Cache := tc.Server(3).DistSenderI().(*kvcoord.DistSender).RangeDescriptorCache()
	entry := n4Cache.GetCached(ctx, tablePrefix, false /* inverted */)
	require.NotNil(t, entry)
	require.False(t, entry.Lease().Empty())
	require.Equal(t, roachpb.StoreID(1), entry.Lease().Replica.StoreID)
	require.Equal(t, []roachpb.ReplicaDescriptor{
		{NodeID: 1, StoreID: 1, ReplicaID: 1},
		{NodeID: 2, StoreID: 2, ReplicaID: 2},
	}, entry.Desc().Replicas().All())

	// Relocate the follower. n2 will no longer have a replica.
	n1.Exec(t, `ALTER TABLE test EXPERIMENTAL_RELOCATE VALUES (ARRAY[1,3], 1)`)

	// Execute the query again and assert the cache is updated. This query will
	// not be executed as a follower read since it attempts to use n2 which
	// doesn't have a replica any more and then it tries n1 which returns an
	// updated descriptor.
	n4.Exec(t, historicalQuery)
	// As a sanity check, verify that this was not a follower read.
	rec := <-recCh
	require.False(t, kv.OnlyFollowerReads(rec), "query was not served through follower reads: %s", rec)
	// Check that the cache was properly updated.
	entry = n4Cache.GetCached(ctx, tablePrefix, false /* inverted */)
	require.NotNil(t, entry)
	require.False(t, entry.Lease().Empty())
	require.Equal(t, roachpb.StoreID(1), entry.Lease().Replica.StoreID)
	require.Equal(t, []roachpb.ReplicaDescriptor{
		{NodeID: 1, StoreID: 1, ReplicaID: 1},
		{NodeID: 3, StoreID: 3, ReplicaID: 3},
	}, entry.Desc().Replicas().All())

	// Make a note of the follower reads metric on n3. We'll check that it was
	// incremented.
	var followerReadsCountBefore int64
	err := tc.Servers[2].Stores().VisitStores(func(s *kvserver.Store) error {
		followerReadsCountBefore = s.Metrics().FollowerReadsCount.Count()
		return nil
	})
	require.NoError(t, err)

	// Run a historical query and assert that it's served from the follower (n3).
	// n4 should attempt to route to n3 because we pretend n3 has a lower latency
	// (see testing knob).
	n4.Exec(t, historicalQuery)
	rec = <-recCh

	// Look at the trace and check that we've served a follower read.
	require.True(t, kv.OnlyFollowerReads(rec), "query was not served through follower reads: %s", rec)

	// Check that the follower read metric was incremented.
	var followerReadsCountAfter int64
	err = tc.Servers[2].Stores().VisitStores(func(s *kvserver.Store) error {
		followerReadsCountAfter = s.Metrics().FollowerReadsCount.Count()
		return nil
	})
	require.NoError(t, err)
	require.Greater(t, followerReadsCountAfter, followerReadsCountBefore)
}
