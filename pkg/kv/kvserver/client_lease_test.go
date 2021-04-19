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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestStoreRangeLease verifies that regular ranges (not some special ones at
// the start of the key space) get epoch-based range leases if enabled and
// expiration-based otherwise.
func TestStoreRangeLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						DisableMergeQueue: true,
					},
				},
			},
		},
	)
	defer tc.Stopper().Stop(context.Background())

	store := tc.GetFirstStoreFromServer(t, 0)
	// NodeLivenessKeyMax is a static split point, so this is always
	// the start key of the first range that uses epoch-based
	// leases. Splitting on it here is redundant, but we want to include
	// it in our tests of lease types below.
	splitKeys := []roachpb.Key{
		keys.NodeLivenessKeyMax, roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c"),
	}
	for _, splitKey := range splitKeys {
		tc.SplitRangeOrFatal(t, splitKey)
	}

	rLeft := store.LookupReplica(roachpb.RKeyMin)
	lease, _ := rLeft.GetLease()
	if lt := lease.Type(); lt != roachpb.LeaseExpiration {
		t.Fatalf("expected lease type expiration; got %d", lt)
	}

	// After the expiration, expect an epoch lease for all the ranges if
	// we've enabled epoch based range leases.
	for _, key := range splitKeys {
		repl := store.LookupReplica(roachpb.RKey(key))
		lease, _ = repl.GetLease()
		if lt := lease.Type(); lt != roachpb.LeaseEpoch {
			t.Fatalf("expected lease type epoch; got %d", lt)
		}
	}
}

// TestStoreGossipSystemData verifies that the system-config and node-liveness
// data is gossiped at startup.
func TestStoreGossipSystemData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	zcfg := zonepb.DefaultZoneConfig()
	serverArgs := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableMergeQueue: true,
			},
			Server: &server.TestingKnobs{
				DefaultZoneConfigOverride: &zcfg,
			},
		},
	}
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs:      serverArgs,
		},
	)
	defer tc.Stopper().Stop(context.Background())

	store := tc.GetFirstStoreFromServer(t, 0)
	splitKey := keys.SystemConfigSplitKey
	tc.SplitRangeOrFatal(t, splitKey)
	if _, err := store.DB().Inc(context.Background(), splitKey, 1); err != nil {
		t.Fatalf("failed to increment: %+v", err)
	}

	getSystemConfig := func(s *kvserver.Store) *config.SystemConfig {
		systemConfig := s.Gossip().GetSystemConfig()
		return systemConfig
	}
	getNodeLiveness := func(s *kvserver.Store) livenesspb.Liveness {
		var liveness livenesspb.Liveness
		if err := s.Gossip().GetInfoProto(gossip.MakeNodeLivenessKey(1), &liveness); err == nil {
			return liveness
		}
		return livenesspb.Liveness{}
	}

	// Restart the store and verify that both the system-config and node-liveness
	// data is gossiped.
	tc.AddAndStartServer(t, serverArgs)
	tc.StopServer(0)

	testutils.SucceedsSoon(t, func() error {
		if !getSystemConfig(tc.GetFirstStoreFromServer(t, 1)).DefaultZoneConfig.Equal(zcfg) {
			return errors.New("system config not gossiped")
		}
		if getNodeLiveness(tc.GetFirstStoreFromServer(t, 1)) == (livenesspb.Liveness{}) {
			return errors.New("node liveness not gossiped")
		}
		return nil
	})
}

// TestGossipSystemConfigOnLeaseChange verifies that the system-config gets
// re-gossiped on lease transfer even if it hasn't changed. This helps prevent
// situations where a previous leaseholder can restart and not receive the
// system config because it was the original source of it within the gossip
// network.
func TestGossipSystemConfigOnLeaseChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numStores = 3
	tc := testcluster.StartTestCluster(t, numStores,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		},
	)
	defer tc.Stopper().Stop(context.Background())

	key := keys.SystemConfigSpan.Key
	tc.AddVotersOrFatal(t, key, tc.Target(1), tc.Target(2))

	initialStoreIdx := -1
	for i := range tc.Servers {
		if tc.GetFirstStoreFromServer(t, i).Gossip().InfoOriginatedHere(gossip.KeySystemConfig) {
			initialStoreIdx = i
		}
	}
	if initialStoreIdx == -1 {
		t.Fatalf("no store has gossiped system config; gossip contents: %+v", tc.GetFirstStoreFromServer(t, 0).Gossip().GetInfoStatus())
	}

	newStoreIdx := (initialStoreIdx + 1) % numStores
	if err := tc.TransferRangeLease(tc.LookupRangeOrFatal(t, key), tc.Target(newStoreIdx)); err != nil {
		t.Fatalf("Unexpected error %v", err)
	}
	testutils.SucceedsSoon(t, func() error {
		if tc.GetFirstStoreFromServer(t, initialStoreIdx).Gossip().InfoOriginatedHere(gossip.KeySystemConfig) {
			return errors.New("system config still most recently gossiped by original leaseholder")
		}
		if !tc.GetFirstStoreFromServer(t, newStoreIdx).Gossip().InfoOriginatedHere(gossip.KeySystemConfig) {
			return errors.New("system config not most recently gossiped by new leaseholder")
		}
		return nil
	})
}

func TestGossipNodeLivenessOnLeaseChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numStores = 3
	tc := testcluster.StartTestCluster(t, numStores,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		},
	)
	defer tc.Stopper().Stop(context.Background())

	key := roachpb.RKey(keys.NodeLivenessSpan.Key)
	tc.AddVotersOrFatal(t, key.AsRawKey(), tc.Target(1), tc.Target(2))
	if pErr := tc.WaitForVoters(key.AsRawKey(), tc.Target(1), tc.Target(2)); pErr != nil {
		t.Fatal(pErr)
	}

	desc := tc.LookupRangeOrFatal(t, key.AsRawKey())

	// Turn off liveness heartbeats on all nodes to ensure that updates to node
	// liveness are not triggering gossiping.
	for _, s := range tc.Servers {
		pErr := s.Stores().VisitStores(func(store *kvserver.Store) error {
			store.GetStoreConfig().NodeLiveness.PauseHeartbeatLoopForTest()
			return nil
		})
		if pErr != nil {
			t.Fatal(pErr)
		}
	}

	nodeLivenessKey := gossip.MakeNodeLivenessKey(1)

	initialServerId := -1
	for i, s := range tc.Servers {
		pErr := s.Stores().VisitStores(func(store *kvserver.Store) error {
			if store.Gossip().InfoOriginatedHere(nodeLivenessKey) {
				initialServerId = i
			}
			return nil
		})
		if pErr != nil {
			t.Fatal(pErr)
		}
	}
	if initialServerId == -1 {
		t.Fatalf("no store has gossiped %s; gossip contents: %+v",
			nodeLivenessKey, tc.GetFirstStoreFromServer(t, 0).Gossip().GetInfoStatus())
	}
	log.Infof(context.Background(), "%s gossiped from s%d",
		nodeLivenessKey, initialServerId)

	newServerIdx := (initialServerId + 1) % numStores

	if pErr := tc.TransferRangeLease(desc, tc.Target(newServerIdx)); pErr != nil {
		t.Fatal(pErr)
	}

	testutils.SucceedsSoon(t, func() error {
		if tc.GetFirstStoreFromServer(t, initialServerId).Gossip().InfoOriginatedHere(nodeLivenessKey) {
			return fmt.Errorf("%s still most recently gossiped by original leaseholder", nodeLivenessKey)
		}
		if !tc.GetFirstStoreFromServer(t, newServerIdx).Gossip().InfoOriginatedHere(nodeLivenessKey) {
			return fmt.Errorf("%s not most recently gossiped by new leaseholder", nodeLivenessKey)
		}
		return nil
	})
}

// TestCannotTransferLeaseToVoterOutgoing ensures that the evaluation of lease
// requests for nodes which are already in the VOTER_OUTGOING state will fail.
func TestCannotTransferLeaseToVoterOutgoing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	knobs, ltk := makeReplicationTestKnobs()
	// Add a testing knob to allow us to block the change replicas command
	// while it is being proposed. When we detect that the change replicas
	// command to move n3 to VOTER_OUTGOING has been evaluated, we'll send
	// the request to transfer the lease to n3. The hope is that it will
	// get past the sanity above latch acquisition prior to change replicas
	// command committing.
	var scratchRangeID atomic.Value
	scratchRangeID.Store(roachpb.RangeID(0))
	changeReplicasChan := make(chan chan struct{}, 1)
	shouldBlock := func(args kvserverbase.ProposalFilterArgs) bool {
		// Block if a ChangeReplicas command is removing a node from our range.
		return args.Req.RangeID == scratchRangeID.Load().(roachpb.RangeID) &&
			args.Cmd.ReplicatedEvalResult.ChangeReplicas != nil &&
			len(args.Cmd.ReplicatedEvalResult.ChangeReplicas.Removed()) > 0
	}
	blockIfShould := func(args kvserverbase.ProposalFilterArgs) {
		if shouldBlock(args) {
			ch := make(chan struct{})
			changeReplicasChan <- ch
			<-ch
		}
	}
	knobs.Store.(*kvserver.StoreTestingKnobs).TestingProposalFilter = func(args kvserverbase.ProposalFilterArgs) *roachpb.Error {
		blockIfShould(args)
		return nil
	}
	tc := testcluster.StartTestCluster(t, 4, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	scratchStartKey := tc.ScratchRange(t)
	desc := tc.AddVotersOrFatal(t, scratchStartKey, tc.Targets(1, 2)...)
	scratchRangeID.Store(desc.RangeID)
	// Make sure n1 has the lease to start with.
	err := tc.Server(0).DB().AdminTransferLease(context.Background(),
		scratchStartKey, tc.Target(0).StoreID)
	require.NoError(t, err)

	// The test proceeds as follows:
	//
	//  - Send an AdminChangeReplicasRequest to remove n3 and add n4
	//  - Block the step that moves n3 to VOTER_OUTGOING on changeReplicasChan
	//  - Send an AdminLeaseTransfer to make n3 the leaseholder
	//  - Try really hard to make sure that the lease transfer at least gets to
	//    latch acquisition before unblocking the ChangeReplicas.
	//  - Unblock the ChangeReplicas.
	//  - Make sure the lease transfer fails.

	ltk.withStopAfterJointConfig(func() {
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err = tc.Server(0).DB().AdminChangeReplicas(ctx,
				scratchStartKey, desc, []roachpb.ReplicationChange{
					{ChangeType: roachpb.REMOVE_VOTER, Target: tc.Target(2)},
					{ChangeType: roachpb.ADD_VOTER, Target: tc.Target(3)},
				})
			require.NoError(t, err)
		}()
		ch := <-changeReplicasChan
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := tc.Server(0).DB().AdminTransferLease(context.Background(),
				scratchStartKey, tc.Target(2).StoreID)
			require.Error(t, err)
			require.Regexp(t,
				// The error generated during evaluation.
				"replica cannot hold lease|"+
					// If the lease transfer request has not yet made it to the latching
					// phase by the time we close(ch) below, we can receive the following
					// error due to the sanity checking which happens in
					// AdminTransferLease before attempting to evaluate the lease
					// transfer.
					// We have a sleep loop below to try to encourage the lease transfer
					// to make it past that sanity check prior to letting the change
					// of replicas proceed.
					"cannot transfer lease to replica of type VOTER_DEMOTING_LEARNER", err.Error())
		}()
		// Try really hard to make sure that our request makes it past the
		// sanity check error to the evaluation error.
		for i := 0; i < 100; i++ {
			runtime.Gosched()
			time.Sleep(time.Microsecond)
		}
		close(ch)
		wg.Wait()
	})

}

// TestStoreLeaseTransferTimestampCacheRead verifies that the timestamp cache on
// the new leaseholder is properly updated after a lease transfer to prevent new
// writes from invalidating previously served reads.
func TestStoreLeaseTransferTimestampCacheRead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "future-read", func(t *testing.T, futureRead bool) {
		manualClock := hlc.NewHybridManualClock()
		ctx := context.Background()
		tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						ClockSource: manualClock.UnixNano,
					},
				},
			},
		})
		defer tc.Stopper().Stop(ctx)

		key := []byte("a")
		rangeDesc, err := tc.LookupRange(key)
		require.NoError(t, err)

		// Transfer the lease to Servers[0] so we start in a known state. Otherwise,
		// there might be already a lease owned by a random node.
		require.NoError(t, tc.TransferRangeLease(rangeDesc, tc.Target(0)))

		// Pause the cluster's clock. This ensures that if we perform a read at
		// a future timestamp, the read time remains in the future, regardless
		// of the passage of real time.
		manualClock.Pause()

		// Write a key.
		_, pErr := kv.SendWrapped(ctx, tc.Servers[0].DistSender(), incrementArgs(key, 1))
		require.Nil(t, pErr)

		// Determine when to read.
		readTS := tc.Servers[0].Clock().Now()
		if futureRead {
			readTS = readTS.Add(500*time.Millisecond.Nanoseconds(), 0).WithSynthetic(true)
		}

		// Read the key at readTS.
		// NB: don't use SendWrapped because we want access to br.Timestamp.
		var ba roachpb.BatchRequest
		ba.Timestamp = readTS
		ba.Add(getArgs(key))
		br, pErr := tc.Servers[0].DistSender().Send(ctx, ba)
		require.Nil(t, pErr)
		require.Equal(t, readTS, br.Timestamp)
		v, err := br.Responses[0].GetGet().Value.GetInt()
		require.NoError(t, err)
		require.Equal(t, int64(1), v)

		// Transfer the lease. This should carry over a summary of the old
		// leaseholder's timestamp cache to prevent any writes on the new
		// leaseholder from writing under the previous read.
		require.NoError(t, tc.TransferRangeLease(rangeDesc, tc.Target(1)))

		// Attempt to write under the read on the new leaseholder. The batch
		// should get forwarded to a timestamp after the read.
		// NB: don't use SendWrapped because we want access to br.Timestamp.
		ba = roachpb.BatchRequest{}
		ba.Timestamp = readTS
		ba.Add(incrementArgs(key, 1))
		br, pErr = tc.Servers[0].DistSender().Send(ctx, ba)
		require.Nil(t, pErr)
		require.NotEqual(t, readTS, br.Timestamp)
		require.True(t, readTS.Less(br.Timestamp))
		require.Equal(t, readTS.Synthetic, br.Timestamp.Synthetic)
	})
}

// TestStoreLeaseTransferTimestampCacheTxnRecord checks the error returned by
// attempts to create a txn record after a lease transfer.
func TestStoreLeaseTransferTimestampCacheTxnRecord(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	key := []byte("a")
	rangeDesc, err := tc.LookupRange(key)
	require.NoError(t, err)

	// Transfer the lease to Servers[0] so we start in a known state. Otherwise,
	// there might be already a lease owned by a random node.
	require.NoError(t, tc.TransferRangeLease(rangeDesc, tc.Target(0)))

	// Start a txn and perform a write, so that a txn record has to be created by
	// the EndTxn.
	txn := tc.Servers[0].DB().NewTxn(ctx, "test")
	require.NoError(t, txn.Put(ctx, "a", "val"))
	// After starting the transaction, transfer the lease. This will wipe the
	// timestamp cache, which means that the txn record will not be able to be
	// created (because someone might have already aborted the txn).
	require.NoError(t, tc.TransferRangeLease(rangeDesc, tc.Target(1)))

	err = txn.Commit(ctx)
	require.Regexp(t, `TransactionAbortedError\(ABORT_REASON_NEW_LEASE_PREVENTS_TXN\)`, err)
}

// This test verifies that when a lease is moved to a node that does not match the
// lease preferences the replication queue moves it eagerly back, without considering the
// kv.allocator.min_lease_transfer_interval.
func TestLeasePreferencesRebalance(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	sv := &settings.SV
	// set min lease transfer high, so we know it does affect the lease movement.
	kvserver.MinLeaseTransferInterval.Override(ctx, sv, 24*time.Hour)
	// Place all the leases in us-west.
	zcfg := zonepb.DefaultZoneConfig()
	zcfg.LeasePreferences = []zonepb.LeasePreference{
		{
			Constraints: []zonepb.Constraint{
				{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "us-west"},
			},
		},
	}
	numNodes := 3
	serverArgs := make(map[int]base.TestServerArgs)
	locality := func(region string) roachpb.Locality {
		return roachpb.Locality{
			Tiers: []roachpb.Tier{
				{Key: "region", Value: region},
			},
		}
	}
	localities := []roachpb.Locality{
		locality("us-west"),
		locality("us-east"),
		locality("eu"),
	}
	for i := 0; i < numNodes; i++ {
		serverArgs[i] = base.TestServerArgs{
			Locality: localities[i],
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DefaultZoneConfigOverride: &zcfg,
				},
			},
			Settings: settings,
		}
	}
	tc := testcluster.StartTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode:   base.ReplicationManual,
			ServerArgsPerNode: serverArgs,
		})
	defer tc.Stopper().Stop(ctx)

	key := keys.UserTableDataMin
	tc.SplitRangeOrFatal(t, key)
	tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)
	require.NoError(t, tc.WaitForVoters(key, tc.Targets(1, 2)...))
	desc := tc.LookupRangeOrFatal(t, key)
	leaseHolder, err := tc.FindRangeLeaseHolder(desc, nil)
	require.NoError(t, err)
	require.Equal(t, tc.Target(0), leaseHolder)

	// Manually move lease out of preference.
	tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(1))

	testutils.SucceedsSoon(t, func() error {
		lh, err := tc.FindRangeLeaseHolder(desc, nil)
		if err != nil {
			return err
		}
		if !lh.Equal(tc.Target(1)) {
			return errors.Errorf("Expected leaseholder to be %s but was %s", tc.Target(1), lh)
		}
		return nil
	})

	tc.GetFirstStoreFromServer(t, 1).SetReplicateQueueActive(true)
	require.NoError(t, tc.GetFirstStoreFromServer(t, 1).ForceReplicationScanAndProcess())

	// The lease should be moved back by the rebalance queue to us-west.
	testutils.SucceedsSoon(t, func() error {
		lh, err := tc.FindRangeLeaseHolder(desc, nil)
		if err != nil {
			return err
		}
		if !lh.Equal(tc.Target(0)) {
			return errors.Errorf("Expected leaseholder to be %s but was %s", tc.Target(0), lh)
		}
		return nil
	})
}

// This test replicates the behavior observed in
// https://github.com/cockroachdb/cockroach/issues/62485. We verify that
// when a dc with the leaseholder is lost, a node in a dc that does not have the
// lease preference can steal the lease, upreplicate the range and then give up the
// lease in a single cycle of the replicate_queue.
func TestLeasePreferencesDuringOutage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stickyRegistry := server.NewStickyInMemEnginesRegistry()
	defer stickyRegistry.CloseAllStickyInMemEngines()
	ctx := context.Background()
	manualClock := hlc.NewHybridManualClock()
	// Place all the leases in the us.
	zcfg := zonepb.DefaultZoneConfig()
	zcfg.LeasePreferences = []zonepb.LeasePreference{
		{
			Constraints: []zonepb.Constraint{
				{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "us"},
			},
		},
	}
	numNodes := 6
	serverArgs := make(map[int]base.TestServerArgs)
	locality := func(region string, dc string) roachpb.Locality {
		return roachpb.Locality{
			Tiers: []roachpb.Tier{
				{Key: "region", Value: region},
				{Key: "dc", Value: dc},
			},
		}
	}
	localities := []roachpb.Locality{
		locality("eu", "tr"),
		locality("eu", "tr"),
		locality("us", "sf"),
		locality("us", "sf"),
		locality("us", "mi"),
		locality("us", "mi"),
	}
	for i := 0; i < numNodes; i++ {
		serverArgs[i] = base.TestServerArgs{
			Locality: localities[i],
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					ClockSource:               manualClock.UnixNano,
					DefaultZoneConfigOverride: &zcfg,
					StickyEngineRegistry:      stickyRegistry,
				},
			},
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:               true,
					StickyInMemoryEngineID: strconv.FormatInt(int64(i), 10),
				},
			},
		}
	}
	tc := testcluster.StartTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode:   base.ReplicationManual,
			ServerArgsPerNode: serverArgs,
		})
	defer tc.Stopper().Stop(ctx)

	key := keys.UserTableDataMin
	tc.SplitRangeOrFatal(t, key)
	tc.AddVotersOrFatal(t, key, tc.Targets(2, 4)...)
	repl := tc.GetFirstStoreFromServer(t, 0).LookupReplica(roachpb.RKey(key))
	require.NoError(t, tc.WaitForVoters(key, tc.Targets(2, 4)...))
	tc.TransferRangeLeaseOrFatal(t, *repl.Desc(), tc.Target(2))

	// Shutdown the sf datacenter, which is going to kill the node with the lease.
	tc.StopServer(2)
	tc.StopServer(3)

	wait := func(duration int64) {
		manualClock.Increment(duration)
		// Gossip and heartbeat all the live stores, we do this manually otherwise the
		// allocator on server 0 may see everyone as temporarily dead due to the
		// clock move above.
		for _, i := range []int{0, 1, 4, 5} {
			require.NoError(t, tc.Servers[i].HeartbeatNodeLiveness())
			require.NoError(t, tc.GetFirstStoreFromServer(t, i).GossipStore(ctx, true))
		}
	}
	// We need to wait until 2 and 3 are considered to be dead.
	timeUntilStoreDead := kvserver.TimeUntilStoreDead.Get(&tc.GetFirstStoreFromServer(t, 0).GetStoreConfig().Settings.SV)
	wait(timeUntilStoreDead.Nanoseconds())

	checkDead := func(store *kvserver.Store, storeIdx int) error {
		if dead, timetoDie, err := store.GetStoreConfig().StorePool.IsDead(
			tc.GetFirstStoreFromServer(t, storeIdx).StoreID()); err != nil || !dead {
			// Sometimes a gossip update arrives right after server shutdown and
			// after we manually moved the time, so move it again.
			if err == nil {
				wait(timetoDie.Nanoseconds())
			}
			return errors.Errorf("expected server 2 to be dead, instead err=%v, dead=%v", err, dead)
		}
		return nil
	}

	testutils.SucceedsSoon(t, func() error {
		store := tc.GetFirstStoreFromServer(t, 0)
		sl, _, _ := store.GetStoreConfig().StorePool.GetStoreList()
		if len(sl.Stores()) != 4 {
			return errors.Errorf("expected all 4 remaining stores to be live, but only got %v", sl.Stores())
		}
		if err := checkDead(store, 2); err != nil {
			return err
		}
		if err := checkDead(store, 3); err != nil {
			return err
		}
		return nil
	})

	_, processError, enqueueError := tc.GetFirstStoreFromServer(t, 0).
		ManuallyEnqueue(ctx, "replicate", repl, true)
	require.NoError(t, enqueueError)
	if processError != nil {
		log.Infof(ctx, "a US replica stole lease, manually moving it to the EU.")
		if !strings.Contains(processError.Error(), "does not have the range lease") {
			t.Fatal(processError)
		}
		// The us replica ended up stealing the lease, so we need to manually
		// transfer the lease and then do another run through the replicate queue
		// to move it to the us.
		tc.TransferRangeLeaseOrFatal(t, *repl.Desc(), tc.Target(0))
		testutils.SucceedsSoon(t, func() error {
			if !repl.OwnsValidLease(ctx, tc.Servers[0].Clock().NowAsClockTimestamp()) {
				return errors.Errorf("Expected lease to transfer to server 0")
			}
			return nil
		})
		_, processError, enqueueError = tc.GetFirstStoreFromServer(t, 0).
			ManuallyEnqueue(ctx, "replicate", repl, true)
		require.NoError(t, enqueueError)
		require.NoError(t, processError)
	}

	var newLeaseHolder roachpb.ReplicationTarget
	testutils.SucceedsSoon(t, func() error {
		var err error
		newLeaseHolder, err = tc.FindRangeLeaseHolder(*repl.Desc(), nil)
		return err
	})

	srv, err := tc.FindMemberServer(newLeaseHolder.StoreID)
	require.NoError(t, err)
	region, ok := srv.Locality().Find("region")
	require.True(t, ok)
	require.Equal(t, "us", region)
	require.Equal(t, 3, len(repl.Desc().Replicas().Voters().VoterDescriptors()))
	// Validate that we upreplicated outside of SF.
	for _, replDesc := range repl.Desc().Replicas().Voters().VoterDescriptors() {
		serv, err := tc.FindMemberServer(replDesc.StoreID)
		require.NoError(t, err)
		dc, ok := serv.Locality().Find("dc")
		require.True(t, ok)
		require.NotEqual(t, "sf", dc)
	}
	history := repl.GetLeaseHistory()
	// make sure we see the eu node as a lease holder in the second to last position.
	require.Equal(t, tc.Target(0).NodeID, history[len(history)-2].Replica.NodeID)
}

// This test verifies that when a node starts flapping its liveness, all leases
// move off that node and it does not get any leases back until it heartbeats
// liveness and waits out the server.time_after_store_suspect interval.
func TestLeasesDontThrashWhenNodeBecomesSuspect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This is a hefty test, so we skip it under short and race.
	skip.UnderShort(t)
	skip.UnderRace(t)

	// We introduce constraints so that only n2,n3,n4 are considered when we
	// determine if we should transfer leases based on capacity. This makes sure n2
	// looks desirable as a lease transfer target once it stops being suspect.
	zcfg := zonepb.DefaultZoneConfig()
	zcfg.Constraints = []zonepb.ConstraintsConjunction{
		{
			NumReplicas: 3,
			Constraints: []zonepb.Constraint{
				{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "us-west"},
			},
		},
	}
	locality := func(region string) roachpb.Locality {
		return roachpb.Locality{
			Tiers: []roachpb.Tier{
				{Key: "region", Value: region},
			},
		}
	}
	localities := []roachpb.Locality{
		locality("us-east"),
		locality("us-west"),
		locality("us-west"),
		locality("us-west"),
	}
	// Speed up lease transfers.
	stickyRegistry := server.NewStickyInMemEnginesRegistry()
	defer stickyRegistry.CloseAllStickyInMemEngines()
	ctx := context.Background()
	manualClock := hlc.NewHybridManualClock()
	serverArgs := make(map[int]base.TestServerArgs)
	numNodes := 4
	for i := 0; i < numNodes; i++ {
		serverArgs[i] = base.TestServerArgs{
			Locality: localities[i],
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					ClockSource:               manualClock.UnixNano,
					DefaultZoneConfigOverride: &zcfg,
					StickyEngineRegistry:      stickyRegistry,
				},
			},
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:               true,
					StickyInMemoryEngineID: strconv.FormatInt(int64(i), 10),
				},
			},
		}
	}
	tc := testcluster.StartTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode:   base.ReplicationManual,
			ServerArgsPerNode: serverArgs,
		})
	defer tc.Stopper().Stop(ctx)

	// We are not going to have stats, so disable this so we just rely on
	// the store means.
	_, err := tc.ServerConn(0).Exec(`SET CLUSTER SETTING kv.allocator.load_based_lease_rebalancing.enabled = 'false'`)
	require.NoError(t, err)

	_, rhsDesc := tc.SplitRangeOrFatal(t, keys.UserTableDataMin)
	tc.AddVotersOrFatal(t, rhsDesc.StartKey.AsRawKey(), tc.Targets(1, 2, 3)...)
	tc.RemoveLeaseHolderOrFatal(t, rhsDesc, tc.Target(0), tc.Target(1))

	startKeys := make([]roachpb.Key, 20)
	startKeys[0] = rhsDesc.StartKey.AsRawKey()
	for i := 1; i < 20; i++ {
		startKeys[i] = startKeys[i-1].Next()
		tc.SplitRangeOrFatal(t, startKeys[i])
		require.NoError(t, tc.WaitForVoters(startKeys[i], tc.Targets(1, 2, 3)...))
	}

	leaseOnNonSuspectStores := func(key roachpb.Key) error {
		var repl *kvserver.Replica
		for _, i := range []int{2, 3} {
			repl = tc.GetFirstStoreFromServer(t, i).LookupReplica(roachpb.RKey(key))
			if repl.OwnsValidLease(ctx, tc.Servers[i].Clock().NowAsClockTimestamp()) {
				return nil
			}
		}
		return errors.Errorf("Expected no lease on server 1 for %s", repl)
	}

	allLeasesOnNonSuspectStores := func() error {
		for _, key := range startKeys {
			if err := leaseOnNonSuspectStores(key); err != nil {
				return err
			}
		}
		return nil
	}

	// Make sure that all store pools have seen liveness heartbeats from everyone.
	testutils.SucceedsSoon(t, func() error {
		for i := range tc.Servers {
			for j := range tc.Servers {
				live, err := tc.GetFirstStoreFromServer(t, i).GetStoreConfig().StorePool.IsLive(tc.Target(j).StoreID)
				if err != nil {
					return err
				}
				if !live {
					return errors.Errorf("Expected server %d to be suspect on server %d", j, i)
				}
			}
		}
		return nil
	})

	for _, key := range startKeys {
		repl := tc.GetFirstStoreFromServer(t, 1).LookupReplica(roachpb.RKey(key))
		tc.TransferRangeLeaseOrFatal(t, *repl.Desc(), tc.Target(1))
		testutils.SucceedsSoon(t, func() error {
			if !repl.OwnsValidLease(ctx, tc.Servers[1].Clock().NowAsClockTimestamp()) {
				return errors.Errorf("Expected lease to transfer to server 1 for replica %s", repl)
			}
			return nil
		})
	}

	heartbeat := func(servers ...int) {
		for _, i := range servers {
			testutils.SucceedsSoon(t, tc.Servers[i].HeartbeatNodeLiveness)
		}
	}

	// The node has to lose both it's raft leadership and liveness for leases to
	// move, so the best way to simulate that right now is to just kill the node.
	tc.StopServer(1)
	// We move the time, so that server 1 can start failing its liveness.
	livenessDuration, _ := tc.GetFirstStoreFromServer(t, 0).GetStoreConfig().RaftConfig.NodeLivenessDurations()

	// We dont want the other stores to lose liveness while we move the time, so
	// tick the time a second and a time and make sure they heartbeat.
	for i := 0; i < int(math.Ceil(livenessDuration.Seconds())+1); i++ {
		manualClock.Increment(time.Second.Nanoseconds())
		heartbeat(0, 2, 3)
	}

	testutils.SucceedsSoon(t, func() error {
		for _, i := range []int{2, 3} {
			suspect, err := tc.GetFirstStoreFromServer(t, i).GetStoreConfig().StorePool.IsSuspect(tc.Target(1).StoreID)
			if err != nil {
				return err
			}
			if !suspect {
				return errors.Errorf("Expected server 1 to be suspect on server %d", i)
			}
		}
		return nil
	})

	runThroughTheReplicateQueue := func(key roachpb.Key) {
		for _, i := range []int{2, 3} {
			repl := tc.GetFirstStoreFromServer(t, i).LookupReplica(roachpb.RKey(key))
			require.NotNil(t, repl)
			_, _, enqueueError := tc.GetFirstStoreFromServer(t, i).
				ManuallyEnqueue(ctx, "replicate", repl, true)
			require.NoError(t, enqueueError)
		}
	}

	for _, key := range startKeys {
		testutils.SucceedsSoon(t, func() error {
			runThroughTheReplicateQueue(key)
			return leaseOnNonSuspectStores(key)
		})
	}
	require.NoError(t, tc.RestartServer(1))

	for i := 0; i < int(math.Ceil(livenessDuration.Seconds())); i++ {
		manualClock.Increment(time.Second.Nanoseconds())
		heartbeat(0, 2, 3)
	}
	// Force all the replication queues, server 1 is still suspect so it should not pick up any leases.
	for _, key := range startKeys {
		runThroughTheReplicateQueue(key)
	}
	testutils.SucceedsSoon(t, allLeasesOnNonSuspectStores)
	// Wait out the suspect time.
	suspectDuration := kvserver.TimeAfterStoreSuspect.Get(&tc.GetFirstStoreFromServer(t, 0).GetStoreConfig().Settings.SV)
	for i := 0; i < int(math.Ceil(suspectDuration.Seconds())); i++ {
		manualClock.Increment(time.Second.Nanoseconds())
		heartbeat(0, 1, 2, 3)
	}

	testutils.SucceedsSoon(t, func() error {
		// Server 1 should get some leases back as it's no longer suspect.
		for _, key := range startKeys {
			runThroughTheReplicateQueue(key)
		}
		for _, key := range startKeys {
			repl := tc.GetFirstStoreFromServer(t, 1).LookupReplica(roachpb.RKey(key))
			if repl.OwnsValidLease(ctx, tc.Servers[1].Clock().NowAsClockTimestamp()) {
				return nil
			}
		}
		return errors.Errorf("Expected server 1 to have at lease 1 lease.")
	})
}
