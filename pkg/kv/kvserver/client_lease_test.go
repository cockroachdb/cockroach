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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
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

	testutils.RunTrueAndFalse(t, "enableEpoch", func(t *testing.T, enableEpoch bool) {
		sc := kvserver.TestStoreConfig(nil)
		sc.TestingKnobs.DisableMergeQueue = true
		sc.EnableEpochRangeLeases = enableEpoch
		mtc := &multiTestContext{storeConfig: &sc}
		defer mtc.Stop()
		mtc.Start(t, 1)

		// NodeLivenessKeyMax is a static split point, so this is always
		// the start key of the first range that uses epoch-based
		// leases. Splitting on it here is redundant, but we want to include
		// it in our tests of lease types below.
		splitKeys := []roachpb.Key{
			keys.NodeLivenessKeyMax, roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c"),
		}
		for _, splitKey := range splitKeys {
			splitArgs := adminSplitArgs(splitKey)
			if _, pErr := kv.SendWrapped(context.Background(), mtc.distSenders[0], splitArgs); pErr != nil {
				t.Fatal(pErr)
			}
		}

		rLeft := mtc.stores[0].LookupReplica(roachpb.RKeyMin)
		lease, _ := rLeft.GetLease()
		if lt := lease.Type(); lt != roachpb.LeaseExpiration {
			t.Fatalf("expected lease type expiration; got %d", lt)
		}

		// After the expiration, expect an epoch lease for all the ranges if
		// we've enabled epoch based range leases.
		for _, key := range splitKeys {
			repl := mtc.stores[0].LookupReplica(roachpb.RKey(key))
			lease, _ = repl.GetLease()
			if enableEpoch {
				if lt := lease.Type(); lt != roachpb.LeaseEpoch {
					t.Fatalf("expected lease type epoch; got %d", lt)
				}
			} else {
				if lt := lease.Type(); lt != roachpb.LeaseExpiration {
					t.Fatalf("expected lease type expiration; got %d", lt)
				}
			}
		}
	})
}

// TestStoreRangeLeaseSwitcheroo verifies that ranges can be switched
// between expiration and epoch and back.
func TestStoreRangeLeaseSwitcheroo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := kvserver.TestStoreConfig(nil)
	sc.TestingKnobs.DisableMergeQueue = true
	sc.EnableEpochRangeLeases = true
	sc.Clock = nil // manual clock
	mtc := &multiTestContext{storeConfig: &sc}
	defer mtc.Stop()
	mtc.Start(t, 1)

	splitKey := roachpb.Key("a")
	splitArgs := adminSplitArgs(splitKey)
	if _, pErr := kv.SendWrapped(context.Background(), mtc.distSenders[0], splitArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Allow leases to expire and send commands to ensure we
	// re-acquire, then check types again.
	mtc.advanceClock(context.Background())
	if _, err := mtc.dbs[0].Inc(context.Background(), splitKey, 1); err != nil {
		t.Fatalf("failed to increment: %+v", err)
	}

	// We started with epoch ranges enabled, so verify we have an epoch lease.
	repl := mtc.stores[0].LookupReplica(roachpb.RKey(splitKey))
	lease, _ := repl.GetLease()
	if lt := lease.Type(); lt != roachpb.LeaseEpoch {
		t.Fatalf("expected lease type epoch; got %d", lt)
	}

	// Stop the store and reverse the epoch range lease setting.
	mtc.stopStore(0)
	sc.EnableEpochRangeLeases = false
	mtc.restartStore(0)

	mtc.advanceClock(context.Background())
	if _, err := mtc.dbs[0].Inc(context.Background(), splitKey, 1); err != nil {
		t.Fatalf("failed to increment: %+v", err)
	}

	// Verify we end up with an expiration lease on restart.
	repl = mtc.stores[0].LookupReplica(roachpb.RKey(splitKey))
	lease, _ = repl.GetLease()
	if lt := lease.Type(); lt != roachpb.LeaseExpiration {
		t.Fatalf("expected lease type expiration; got %d", lt)
	}

	// Now, one more time, switch back to epoch-based.
	mtc.stopStore(0)
	sc.EnableEpochRangeLeases = true
	mtc.restartStore(0)

	mtc.advanceClock(context.Background())
	if _, err := mtc.dbs[0].Inc(context.Background(), splitKey, 1); err != nil {
		t.Fatalf("failed to increment: %+v", err)
	}

	// Verify we end up with an epoch lease on restart.
	repl = mtc.stores[0].LookupReplica(roachpb.RKey(splitKey))
	lease, _ = repl.GetLease()
	if lt := lease.Type(); lt != roachpb.LeaseEpoch {
		t.Fatalf("expected lease type epoch; got %d", lt)
	}
}

// TestStoreGossipSystemData verifies that the system-config and node-liveness
// data is gossiped at startup.
func TestStoreGossipSystemData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := kvserver.TestStoreConfig(nil)
	sc.TestingKnobs.DisableMergeQueue = true
	sc.EnableEpochRangeLeases = true
	mtc := &multiTestContext{storeConfig: &sc}
	defer mtc.Stop()
	mtc.Start(t, 1)

	splitKey := keys.SystemConfigSplitKey
	splitArgs := adminSplitArgs(splitKey)
	if _, pErr := kv.SendWrapped(context.Background(), mtc.distSenders[0], splitArgs); pErr != nil {
		t.Fatal(pErr)
	}
	if _, err := mtc.dbs[0].Inc(context.Background(), splitKey, 1); err != nil {
		t.Fatalf("failed to increment: %+v", err)
	}

	mtc.stopStore(0)

	getSystemConfig := func() *config.SystemConfig {
		systemConfig := mtc.gossips[0].GetSystemConfig()
		return systemConfig
	}
	getNodeLiveness := func() kvserverpb.Liveness {
		var liveness kvserverpb.Liveness
		if err := mtc.gossips[0].GetInfoProto(gossip.MakeNodeLivenessKey(1), &liveness); err == nil {
			return liveness
		}
		return kvserverpb.Liveness{}
	}

	// Clear the system-config and node liveness gossip data. This is necessary
	// because multiTestContext.restartStore reuse the Gossip structure.
	if err := mtc.gossips[0].AddInfoProto(
		gossip.KeySystemConfig, &config.SystemConfigEntries{}, 0); err != nil {
		t.Fatal(err)
	}
	if err := mtc.gossips[0].AddInfoProto(
		gossip.MakeNodeLivenessKey(1), &kvserverpb.Liveness{}, 0); err != nil {
		t.Fatal(err)
	}
	testutils.SucceedsSoon(t, func() error {
		if !getSystemConfig().DefaultZoneConfig.Equal(sc.DefaultZoneConfig) {
			return errors.New("system config not empty")
		}
		if getNodeLiveness() != (kvserverpb.Liveness{}) {
			return errors.New("node liveness not empty")
		}
		return nil
	})

	// Restart the store and verify that both the system-config and node-liveness
	// data is gossiped.
	mtc.restartStore(0)
	testutils.SucceedsSoon(t, func() error {
		if !getSystemConfig().DefaultZoneConfig.Equal(sc.DefaultZoneConfig) {
			return errors.New("system config not gossiped")
		}
		if getNodeLiveness() == (kvserverpb.Liveness{}) {
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
	sc := kvserver.TestStoreConfig(nil)
	sc.TestingKnobs.DisableReplicateQueue = true
	mtc := &multiTestContext{storeConfig: &sc}
	defer mtc.Stop()
	const numStores = 3
	mtc.Start(t, numStores)

	rangeID := mtc.stores[0].LookupReplica(roachpb.RKey(keys.SystemConfigSpan.Key)).RangeID
	mtc.replicateRange(rangeID, 1, 2)

	initialStoreIdx := -1
	for i := range mtc.stores {
		if mtc.stores[i].Gossip().InfoOriginatedHere(gossip.KeySystemConfig) {
			initialStoreIdx = i
		}
	}
	if initialStoreIdx == -1 {
		t.Fatalf("no store has gossiped system config; gossip contents: %+v", mtc.stores[0].Gossip().GetInfoStatus())
	}

	newStoreIdx := (initialStoreIdx + 1) % numStores
	mtc.transferLease(context.Background(), rangeID, initialStoreIdx, newStoreIdx)

	testutils.SucceedsSoon(t, func() error {
		if mtc.stores[initialStoreIdx].Gossip().InfoOriginatedHere(gossip.KeySystemConfig) {
			return errors.New("system config still most recently gossiped by original leaseholder")
		}
		if !mtc.stores[newStoreIdx].Gossip().InfoOriginatedHere(gossip.KeySystemConfig) {
			return errors.New("system config not most recently gossiped by new leaseholder")
		}
		return nil
	})
}

func TestGossipNodeLivenessOnLeaseChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := kvserver.TestStoreConfig(nil)
	sc.TestingKnobs.DisableReplicateQueue = true
	mtc := &multiTestContext{storeConfig: &sc}
	defer mtc.Stop()
	const numStores = 3
	mtc.Start(t, numStores)

	rangeID := mtc.stores[0].LookupReplica(roachpb.RKey(keys.NodeLivenessSpan.Key)).RangeID
	mtc.replicateRange(rangeID, 1, 2)

	// Turn off liveness heartbeats on all nodes to ensure that updates to node
	// liveness are not triggering gossiping.
	for i := range mtc.nodeLivenesses {
		mtc.nodeLivenesses[i].PauseHeartbeat(true)
	}

	nodeLivenessKey := gossip.MakeNodeLivenessKey(1)

	initialStoreIdx := -1
	for i := range mtc.stores {
		if mtc.stores[i].Gossip().InfoOriginatedHere(nodeLivenessKey) {
			initialStoreIdx = i
		}
	}
	if initialStoreIdx == -1 {
		t.Fatalf("no store has gossiped %s; gossip contents: %+v",
			nodeLivenessKey, mtc.stores[0].Gossip().GetInfoStatus())
	}
	log.Infof(context.Background(), "%s gossiped from n%d",
		nodeLivenessKey, mtc.stores[initialStoreIdx].Ident.NodeID)

	newStoreIdx := (initialStoreIdx + 1) % numStores
	mtc.transferLease(context.Background(), rangeID, initialStoreIdx, newStoreIdx)

	testutils.SucceedsSoon(t, func() error {
		if mtc.stores[initialStoreIdx].Gossip().InfoOriginatedHere(nodeLivenessKey) {
			return fmt.Errorf("%s still most recently gossiped by original leaseholder", nodeLivenessKey)
		}
		if !mtc.stores[newStoreIdx].Gossip().InfoOriginatedHere(nodeLivenessKey) {
			return fmt.Errorf("%s not most recently gossiped by new leaseholder", nodeLivenessKey)
		}
		return nil
	})
}

// TestCannotTransferLeaseToVoterOutgoing ensures that the evaluation of lease
// requests for nodes which are already in the VOTER_OUTGOING state will fail.
func TestCannotTransferLeaseToVoterOutgoing(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
	desc := tc.AddReplicasOrFatal(t, scratchStartKey, tc.Targets(1, 2)...)
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
					{ChangeType: roachpb.REMOVE_REPLICA, Target: tc.Target(2)},
					{ChangeType: roachpb.ADD_REPLICA, Target: tc.Target(3)},
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
				"replica.*of type VOTER_DEMOTING cannot hold lease|"+
					// If the lease transfer request has not yet made it to the latching
					// phase by the time we close(ch) below, we can receive the following
					// error due to the sanity checking which happens in
					// AdminTransferLease before attempting to evaluate the lease
					// transfer.
					// We have a sleep loop below to try to encourage the lease transfer
					// to make it past that sanity check prior to letting the change
					// of replicas proceed.
					"cannot transfer lease to replica of type VOTER_DEMOTING", err.Error())
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

// Test the error returned by attempts to create a txn record after a lease
// transfer.
func TestTimestampCacheErrorAfterLeaseTransfer(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
