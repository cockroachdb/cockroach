// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/listenerutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestStoreRangeLeas verifies that regular ranges (not special ones
// at the start of the kye space) acquire leader leases once things settle. It's
// the Leader Leasee variant for TestStoreRangeLeaseEpochLeases.
func TestStoreRangeLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	kvserver.OverrideDefaultLeaseType(ctx, &st.SV, roachpb.LeaseLeader)
	manualClock := hlc.NewHybridManualClock()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Settings: st,
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						WallClock: manualClock,
					},
					Store: &kvserver.StoreTestingKnobs{
						DisableMergeQueue: true,
					},
				},
			},
		},
	)
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)
	// NodeLivenessKeyMax is a static split point, so this is always
	// the start key of the first range that uses epoch-based
	// leases. Splitting on it here is redundant, but we want to include
	// it in our tests of lease types below.
	splitKeys := []roachpb.Key{
		keys.NodeLivenessKeyMax, roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c"),
	}
	for _, splitKey := range splitKeys {
		// NB: Splitting requires acquiring a lease, which is what establishes a
		// leader. If there's no leader, the proposed lease acquisition will be for
		// an expiration based lease, even if we're configured to use leader leases.
		tc.SplitRangeOrFatal(t, splitKey)
	}

	// Expire all leases and send a write request to trigger a lease acquisitions.
	// At this point, we have a leader, so the lease acquisition should be for a
	// leader lease.
	manualClock.Increment(tc.Servers[0].RaftConfig().RangeLeaseDuration.Nanoseconds())
	for _, key := range splitKeys {
		incArgs := incrementArgs(key, int64(5))

		testutils.SucceedsSoon(t, func() error {
			_, err := kv.SendWrapped(ctx, store.TestSender(), incArgs)
			return err.GoError()
		})
	}

	// The beginning of the keyspace always uses expiration-based leases.
	rLeft := store.LookupReplica(roachpb.RKeyMin)
	lease, _ := rLeft.GetLease()
	if lt := lease.Type(); lt != roachpb.LeaseExpiration {
		t.Fatalf("expected lease type expiration; got %d", lt)
	}

	// After the expiration based lease, expect the lease type that the test has
	// enabled for all ranges.
	for _, key := range splitKeys {
		repl := store.LookupReplica(roachpb.RKey(key))
		desc := tc.LookupRangeOrFatal(t, key)
		t.Logf("desc: %v", desc)
		lease, _ = repl.GetLease()
		if lt := lease.Type(); lt != roachpb.LeaseLeader {
			t.Errorf("expected lease type %s; got %s for key %v", roachpb.LeaseLeader, lease, key)
		}
	}
}

// TestStoreRangeLeaseEpochLeases verifies that regular ranges (not some special
// ones at the start of the key space) get epoch-based range leases if enabled
// and expiration-based otherwise.
func TestStoreRangeLeaseEpochLeases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	kvserver.OverrideDefaultLeaseType(ctx, &st.SV, roachpb.LeaseEpoch) // override metamorphism

	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Settings: st,
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						DisableMergeQueue: true,
					},
				},
			},
		},
	)
	defer tc.Stopper().Stop(ctx)

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
		pErr := s.GetStores().(*kvserver.Stores).VisitStores(func(store *kvserver.Store) error {
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
		pErr := s.GetStores().(*kvserver.Stores).VisitStores(func(store *kvserver.Store) error {
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
// requests for nodes which are already in the VOTER_DEMOTING_LEARNER state will fail
// (in this test, there is no VOTER_INCOMING node).
func TestCannotTransferLeaseToVoterDemoting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	knobs, ltk := makeReplicationTestKnobs()
	// Add a testing knob to allow us to block the change replicas command
	// while it is being proposed. When we detect that the change replicas
	// command to move n3 to VOTER_DEMOTING_LEARNER has been evaluated, we'll
	// send the request to transfer the lease to n3. The hope is that it will
	// get past the sanity check above latch acquisition prior to the change
	// replicas command committing.
	var scratchRangeID int64
	changeReplicasChan := make(chan chan struct{}, 1)
	shouldBlock := func(args kvserverbase.ProposalFilterArgs) bool {
		// Block if a ChangeReplicas command is removing a node from our range.
		return args.Req.RangeID == roachpb.RangeID(atomic.LoadInt64(&scratchRangeID)) &&
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
	knobs.Store.(*kvserver.StoreTestingKnobs).TestingProposalFilter = func(args kvserverbase.ProposalFilterArgs) *kvpb.Error {
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
	atomic.StoreInt64(&scratchRangeID, int64(desc.RangeID))
	// Make sure n1 has the lease to start with.
	err := tc.Server(0).DB().AdminTransferLease(context.Background(),
		scratchStartKey, tc.Target(0).StoreID)
	require.NoError(t, err)

	// The test proceeds as follows:
	//
	//  - Send an AdminChangeReplicasRequest to remove n3 and add n4
	//  - Block the step that moves n3 to VOTER_DEMOTING_LEARNER on changeReplicasChan
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
				scratchStartKey, desc, []kvpb.ReplicationChange{
					{ChangeType: roachpb.REMOVE_VOTER, Target: tc.Target(2)},
				})
			require.NoError(t, err)
		}()
		ch := <-changeReplicasChan
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := tc.Server(0).DB().AdminTransferLease(ctx,
				scratchStartKey, tc.Target(2).StoreID)
			require.Error(t, err)
			require.True(t, errors.Is(err, roachpb.ErrReplicaCannotHoldLease))
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

// TestTransferLeaseToVoterDemotingFails ensures that the evaluation of lease
// requests for nodes which are already in the VOTER_DEMOTING_LEARNER state fails
// if they weren't previously holding the lease, even if there is a VOTER_INCOMING..
func TestTransferLeaseToVoterDemotingFails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	knobs, ltk := makeReplicationTestKnobs()
	// Add a testing knob to allow us to block the change replicas command
	// while it is being proposed. When we detect that the change replicas
	// command to move n3 to VOTER_DEMOTING_LEARNER has been evaluated, we'll
	// send the request to transfer the lease to n3. The hope is that it will
	// get past the sanity check above latch acquisition prior to the change
	// replicas command committing.
	var scratchRangeID int64
	changeReplicasChan := make(chan chan struct{}, 1)
	shouldBlock := func(args kvserverbase.ProposalFilterArgs) bool {
		// Block if a ChangeReplicas command is removing a node from our range.
		return args.Req.RangeID == roachpb.RangeID(atomic.LoadInt64(&scratchRangeID)) &&
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
	knobs.Store.(*kvserver.StoreTestingKnobs).TestingProposalFilter = func(args kvserverbase.ProposalFilterArgs) *kvpb.Error {
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
	atomic.StoreInt64(&scratchRangeID, int64(desc.RangeID))
	// Make sure n1 has the lease to start with.
	err := tc.Server(0).DB().AdminTransferLease(context.Background(),
		scratchStartKey, tc.Target(0).StoreID)
	require.NoError(t, err)

	// The test proceeds as follows:
	//
	//  - Send an AdminChangeReplicasRequest to remove n3 and add n4
	//  - Block the step that moves n3 to VOTER_DEMOTING_LEARNER on changeReplicasChan
	//  - Send an AdminLeaseTransfer to make n3 the leaseholder
	//  - Make sure that this request fails (since n3 is VOTER_DEMOTING_LEARNER and
	//    wasn't previously leaseholder)
	//  - Unblock the ChangeReplicas.
	//  - Make sure the lease transfer succeeds.

	ltk.withStopAfterJointConfig(func() {
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err = tc.Server(0).DB().AdminChangeReplicas(ctx,
				scratchStartKey, desc, []kvpb.ReplicationChange{
					{ChangeType: roachpb.REMOVE_VOTER, Target: tc.Target(2)},
					{ChangeType: roachpb.ADD_VOTER, Target: tc.Target(3)},
				})
			require.NoError(t, err)
		}()
		ch := <-changeReplicasChan
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Make sure the lease is currently on n1.
			desc, err := tc.LookupRange(scratchStartKey)
			require.NoError(t, err)
			leaseHolder, err := tc.FindRangeLeaseHolder(desc, nil)
			require.NoError(t, err)
			require.Equal(t, tc.Target(0), leaseHolder,
				errors.Errorf("Leaseholder supposed to be on n1."))
			// Try to move the lease to n3, the VOTER_DEMOTING_LEARNER.
			// This should fail since the last leaseholder wasn't n3
			// (wasLastLeaseholder = false in CheckCanReceiveLease).
			err = tc.Server(0).DB().AdminTransferLease(context.Background(),
				scratchStartKey, tc.Target(2).StoreID)
			require.Error(t, err)
			require.True(t, errors.Is(err, roachpb.ErrReplicaCannotHoldLease))
			// Make sure the lease is still on n1.
			leaseHolder, err = tc.FindRangeLeaseHolder(desc, nil)
			require.NoError(t, err)
			require.Equal(t, tc.Target(0), leaseHolder,
				errors.Errorf("Leaseholder supposed to be on n1."))
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

// TestTransferLeaseDuringJointConfigWithDeadIncomingVoter ensures that the
// lease transfer performed during a joint config replication change that is
// replacing the existing leaseholder does not get stuck even if the existing
// leaseholder cannot prove that the incoming leaseholder is caught up on its
// log. It does so by killing the incoming leaseholder before it receives the
// lease and ensuring that the range is able to exit the joint configuration.
//
// Currently, the range exits by bypassing safety checks during the lease
// transfer, sending the lease to the dead incoming voter, letting the lease
// expire, acquiring the lease on one of the non-demoting voters, and exiting.
// The details here may change in the future, but the goal of this test will
// not.
func TestTransferLeaseDuringJointConfigWithDeadIncomingVoter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// The lease request timeout depends on the Raft election timeout, so we set
	// it low to get faster lease expiration (800 ms) and speed up the test.
	var raftCfg base.RaftConfig
	raftCfg.SetDefaults()
	raftCfg.RaftHeartbeatIntervalTicks = 1
	raftCfg.RaftElectionTimeoutTicks = 2

	knobs, ltk := makeReplicationTestKnobs()
	tc := testcluster.StartTestCluster(t, 4, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			RaftConfig: raftCfg,
			Knobs:      knobs,
		},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	key := tc.ScratchRange(t)
	desc := tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)
	// Make sure n1 has the lease to start with.
	err := tc.Server(0).DB().AdminTransferLease(ctx, key, tc.Target(0).StoreID)
	require.NoError(t, err)
	store0, repl0 := getFirstStoreReplica(t, tc.Server(0), key)

	// The test proceeds as follows:
	//
	//  - Send an AdminChangeReplicasRequest to remove n1 (leaseholder) and add n4
	//  - Stop the replication change after entering the joint configuration
	//  - Kill n4 and wait until n1 notices
	//  - Complete the replication change

	// Enter joint config.
	ltk.withStopAfterJointConfig(func() {
		tc.RebalanceVoterOrFatal(ctx, t, key, tc.Target(0), tc.Target(3))
	})
	desc = tc.LookupRangeOrFatal(t, key)
	require.Len(t, desc.Replicas().Descriptors(), 4)
	require.True(t, desc.Replicas().InAtomicReplicationChange(), desc)

	// Kill n4.
	tc.StopServer(3)

	// Wait for n1 to notice.
	testutils.SucceedsSoon(t, func() error {
		// Manually report n4 as unreachable to speed up the test.
		require.NoError(t, repl0.RaftReportUnreachable(4))
		// Check the Raft progress.
		s := repl0.RaftStatus()
		require.Equal(t, raftpb.StateLeader, s.RaftState)
		p := s.Progress
		require.Len(t, p, 4)
		require.Contains(t, p, raftpb.PeerID(4))
		if p[4].State != tracker.StateProbe {
			return errors.Errorf("dead replica not state probe")
		}
		return nil
	})

	// Run the range through the replicate queue on n1.
	traceCtx, rec := tracing.ContextWithRecordingSpan(ctx, store0.GetStoreConfig().Tracer(), "trace-enqueue")
	processErr, err := store0.Enqueue(
		traceCtx, "replicate", repl0, true /* skipShouldQueue */, false /* async */)
	require.NoError(t, err)
	require.NoError(t, processErr)
	formattedTrace := rec().String()
	expectedMessages := []string{
		`transitioning out of joint configuration`,
		`leaseholder .* is being removed through an atomic replication change, transferring lease to`,
		`lease transfer to .* complete`,
	}
	require.NoError(t, testutils.MatchInOrder(formattedTrace, expectedMessages...))

	// Verify that the joint configuration has completed.
	desc = tc.LookupRangeOrFatal(t, key)
	require.Len(t, desc.Replicas().VoterDescriptors(), 3)
	require.False(t, desc.Replicas().InAtomicReplicationChange(), desc)
}

// internalTransferLeaseFailureDuringJointConfig reproduces
// https://github.com/cockroachdb/cockroach/issues/83687
// and makes sure that if lease transfer fails during a joint configuration
// the previous leaseholder will successfully re-aquire the lease.
// The test proceeds as follows:
//   - Creates a range with 3 replicas n1, n2, n3, and makes sure the lease is on n1
//   - Makes sure lease transfers on this range fail from now on
//   - Invokes AdminChangeReplicas to remove n1 and add n4
//   - This causes the range to go into a joint configuration. A lease transfer
//     is attempted to move the lease from n1 to n4 before exiting the joint config,
//     but that fails, causing us to remain in the joint configuration with the original
//     leaseholder having revoked its lease, but everyone else thinking it's still
//     the leaseholder. In this situation, only n1 can re-aquire the lease as long as it is live.
//   - We re-enable lease transfers on this range.
//   - n1 is able to re-aquire the lease, due to the fix in #83686 which enables a
//     VOTER_DEMOTING_LEARNER (n1) replica to get the lease if there's also a VOTER_INCOMING
//     which is the case here (n4) and since n1 was the last leaseholder.
//   - n1 transfers the lease away and the range leaves the joint configuration.
func internalTransferLeaseFailureDuringJointConfig(t *testing.T, isManual bool) {
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	knobs := base.TestingKnobs{Store: &kvserver.StoreTestingKnobs{}}
	// Add a testing knob to allow us to fail all lease transfer commands on this
	// range when they are being proposed.
	var scratchRangeID int64
	shouldFailProposal := func(args kvserverbase.ProposalFilterArgs) bool {
		return args.Req.RangeID == roachpb.RangeID(atomic.LoadInt64(&scratchRangeID)) &&
			args.Req.IsSingleTransferLeaseRequest()
	}
	const failureMsg = "injected lease transfer"
	knobs.Store.(*kvserver.StoreTestingKnobs).TestingProposalFilter = func(args kvserverbase.ProposalFilterArgs) *kvpb.Error {
		if shouldFailProposal(args) {
			// The lease transfer should be configured to bypass safety checks.
			// See maybeTransferLeaseDuringLeaveJoint for an explanation.
			require.True(t, args.Req.Requests[0].GetTransferLease().BypassSafetyChecks)
			return kvpb.NewErrorf(failureMsg)
		}
		return nil
	}
	tc := testcluster.StartTestCluster(t, 4, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	scratchStartKey := tc.ScratchRange(t)
	desc := tc.AddVotersOrFatal(t, scratchStartKey, tc.Targets(1, 2)...)
	// Make sure n1 has the lease to start with.
	err := tc.Server(0).DB().AdminTransferLease(context.Background(),
		scratchStartKey, tc.Target(0).StoreID)
	require.NoError(t, err)

	// The next lease transfer should fail.
	atomic.StoreInt64(&scratchRangeID, int64(desc.RangeID))

	_, err = tc.Server(0).DB().AdminChangeReplicas(ctx,
		scratchStartKey, desc, []kvpb.ReplicationChange{
			{ChangeType: roachpb.REMOVE_VOTER, Target: tc.Target(0)},
			{ChangeType: roachpb.ADD_VOTER, Target: tc.Target(3)},
		})
	require.Error(t, err)
	require.Regexp(t, failureMsg, err)

	// We're now in a joint configuration, n1 already revoked its lease but all
	// other replicas think n1 is the leaseholder. As long as n1 is alive, it is
	// the only one that can get the lease. It will retry and be able to do that
	// thanks to the fix in #83686.
	desc, err = tc.LookupRange(scratchStartKey)
	require.NoError(t, err)
	require.True(t, desc.Replicas().InAtomicReplicationChange())

	// Allow further lease transfers to succeed.
	atomic.StoreInt64(&scratchRangeID, 0)

	if isManual {
		// Manually transfer the lease to n1 (VOTER_DEMOTING_LEARNER).
		err = tc.Server(0).DB().AdminTransferLease(context.Background(),
			scratchStartKey, tc.Target(0).StoreID)
		require.NoError(t, err)
		// Make sure n1 has the lease
		leaseHolder, err := tc.FindRangeLeaseHolder(desc, nil)
		require.NoError(t, err)
		require.Equal(t, tc.Target(0), leaseHolder,
			errors.Errorf("Leaseholder supposed to be on n1."))
	}

	// Complete the replication change.
	atomic.StoreInt64(&scratchRangeID, 0)
	store := tc.GetFirstStoreFromServer(t, 0)
	repl := store.LookupReplica(roachpb.RKey(scratchStartKey))
	_, err = store.Enqueue(
		ctx, "replicate", repl, true /* skipShouldQueue */, false, /* async */
	)
	require.NoError(t, err)
	desc, err = tc.LookupRange(scratchStartKey)
	require.NoError(t, err)
	require.False(t, desc.Replicas().InAtomicReplicationChange())
}

// TestTransferLeaseFailureDuringJointConfig is using
// internalTransferLeaseFailureDuringJointConfig and
// completes the lease transfer to n1 “automatically”
// by relying on replicate queue.
func TestTransferLeaseFailureDuringJointConfigAuto(t *testing.T) {
	defer leaktest.AfterTest(t)()
	internalTransferLeaseFailureDuringJointConfig(t, false)
}

// TestTransferLeaseFailureDuringJointConfigManual is using
// internalTransferLeaseFailureDuringJointConfig and
// completes the lease transfer to n1 “manually” using
// AdminTransferLease.
func TestTransferLeaseFailureDuringJointConfigManual(t *testing.T) {
	defer leaktest.AfterTest(t)()
	internalTransferLeaseFailureDuringJointConfig(t, true)
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
						WallClock: manualClock,
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
		_, pErr := kv.SendWrapped(ctx, tc.Servers[0].DistSenderI().(kv.Sender), incrementArgs(key, 1))
		require.Nil(t, pErr)

		// Determine when to read.
		readTS := tc.Servers[0].Clock().Now()
		if futureRead {
			readTS = readTS.Add(500*time.Millisecond.Nanoseconds(), 0)
		}

		// Read the key at readTS.
		// NB: don't use SendWrapped because we want access to br.Timestamp.
		ba := &kvpb.BatchRequest{}
		ba.Timestamp = readTS
		ba.Add(getArgs(key))
		br, pErr := tc.Servers[0].DistSenderI().(kv.Sender).Send(ctx, ba)
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
		ba = &kvpb.BatchRequest{}
		ba.Timestamp = readTS
		ba.Add(incrementArgs(key, 1))
		br, pErr = tc.Servers[0].DistSenderI().(kv.Sender).Send(ctx, ba)
		require.Nil(t, pErr)
		require.NotEqual(t, readTS, br.Timestamp)
		require.True(t, readTS.Less(br.Timestamp))
	})
}

// TestStoreLeaseTransferTimestampCacheTxnRecord checks whether an error is
// returned by an attempt to create a txn record after a lease transfer.
//
// If the local read summary is given a sufficient size budget then information
// about individual transaction tombstone markers can be passed from the old
// leaseholder to the new leaseholder. This allows the new leaseholder to
// conclusively determine that a transaction tombstone marker did not exist for
// the transaction, avoiding any error when the transaction creates its record.
//
// However, if the local read summary is compressed due to an insufficient size
// budget then the new leaseholder must assume that a transaction tombstone
// marker may have existed for the transaction. As a result, an error is thrown
// when the transaction creates its record.
func TestStoreLeaseTransferTimestampCacheTxnRecord(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "sufficient-budget", func(t *testing.T, budget bool) {
		ctx := context.Background()
		st := cluster.MakeTestingClusterSettings()
		if !budget {
			kvserver.ReadSummaryLocalBudget.Override(ctx, &st.SV, 0)
		}
		tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Settings: st,
			},
		})
		defer tc.Stopper().Stop(ctx)

		key := []byte("a")
		rangeDesc, err := tc.LookupRange(key)
		require.NoError(t, err)

		// Transfer the lease to server 0, so we start in a known state. Otherwise,
		// there might be already a lease owned by a random node.
		require.NoError(t, tc.TransferRangeLease(rangeDesc, tc.Target(0)))

		// Start a txn and perform a write, so that a txn record has to be created by
		// the EndTxn when it eventually commits. Don't commit yet.
		txn1 := tc.Servers[0].DB().NewTxn(ctx, "test")
		require.NoError(t, txn1.Put(ctx, "a", "val"))

		// Start another txn and commit. This writes a txn tombstone marker into
		// server 0's timestamp cache at a higher timestamp than the first txn's
		// tombstone marker will eventually be. Doing so prevents the test from
		// fooling itself and passing if only the high water mark of the timestamp
		// cache is passed in a read summary during the lease transfer.
		txn2 := tc.Servers[0].DB().NewTxn(ctx, "test 2")
		require.NoError(t, txn2.Put(ctx, "b", "val"))
		require.NoError(t, txn2.Commit(ctx))

		// After starting the transaction, transfer the lease. The lease transfer will
		// carry over a sufficiently high resolution summary of the old leaseholder's
		// timestamp cache so that the new leaseholder can still create a txn record
		// for the first txn with certainty that it is not permitting a replay.
		require.NoError(t, tc.TransferRangeLease(rangeDesc, tc.Target(1)))

		// Try to commit the first txn.
		err = txn1.Commit(ctx)
		if budget {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
			require.Regexp(t, `TransactionAbortedError\(ABORT_REASON_TIMESTAMP_CACHE_REJECTED\)`, err)
		}
	})
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

	key := bootstrap.TestingUserTableDataMin(keys.SystemSQLCodec)
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

	tc.GetFirstStoreFromServer(t, 1).TestingSetReplicateQueueActive(true)
	tc.GetFirstStoreFromServer(t, 1).TestingSetLeaseQueueActive(true)
	require.NoError(t, tc.GetFirstStoreFromServer(t, 1).ForceReplicationScanAndProcess())
	require.NoError(t, tc.GetFirstStoreFromServer(t, 1).ForceLeaseQueueProcess())

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

// Tests that when leaseholder is relocated, the lease can be transferred directly to new node
func TestLeaseholderRelocate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stickyRegistry := fs.NewStickyRegistry()
	ctx := context.Background()
	manualClock := hlc.NewHybridManualClock()

	serverArgs := make(map[int]base.TestServerArgs)
	locality := func(region string) roachpb.Locality {
		return roachpb.Locality{
			Tiers: []roachpb.Tier{
				{Key: "region", Value: region},
			},
		}
	}
	localities := []roachpb.Locality{
		locality("eu"),
		locality("eu"),
		locality("us"),
		locality("us"),
	}

	const numNodes = 4
	st := cluster.MakeTestingClusterSettings()
	for i := 0; i < numNodes; i++ {
		serverArgs[i] = base.TestServerArgs{
			Settings: st,
			Locality: localities[i],
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					WallClock:         manualClock,
					StickyVFSRegistry: stickyRegistry,
				},
			},
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:    true,
					StickyVFSID: strconv.FormatInt(int64(i), 10),
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

	_, rhsDesc := tc.SplitRangeOrFatal(t, bootstrap.TestingUserTableDataMin(keys.SystemSQLCodec))

	// We start with having the range under test on (1,2,3).
	tc.AddVotersOrFatal(t, rhsDesc.StartKey.AsRawKey(), tc.Targets(1, 2)...)

	// Make sure the lease is on 3 and is fully upgraded.
	tc.TransferRangeLeaseOrFatal(t, rhsDesc, tc.Target(2))

	var err error
	var leaseHolder roachpb.ReplicationTarget
	testutils.SucceedsSoon(t, func() error {
		leaseHolder, err = tc.FindRangeLeaseHolder(rhsDesc, nil)
		if err != nil {
			return err
		}

		// Check that the lease moved to 3.
		if !leaseHolder.Equal(tc.Target(2)) {
			return errors.Errorf("Leaseholder didn't move.")
		}

		return nil
	})

	gossipLiveness(t, tc)

	testutils.SucceedsSoon(t, func() error {
		// Relocate range 3 -> 4.
		err = tc.Servers[2].DB().
			AdminRelocateRange(
				context.Background(), rhsDesc.StartKey.AsRawKey(),
				tc.Targets(0, 1, 3), nil, false)
		if err != nil {
			return err
		}
		leaseHolder, err = tc.FindRangeLeaseHolder(rhsDesc, nil)
		if err != nil {
			return err
		}
		if leaseHolder.Equal(tc.Target(2)) {
			return errors.Errorf("Leaseholder didn't move.")
		}
		return nil
	})

	// Make sure lease moved to the preferred region.
	testutils.SucceedsSoon(t, func() error {
		leaseHolder, err = tc.FindRangeLeaseHolder(rhsDesc, nil)
		if err != nil {
			return err
		}
		if !leaseHolder.Equal(tc.Target(3)) {
			return errors.Errorf("Leaseholder didn't move.")
		}
		return nil
	})

	// Double check that lease moved directly. The tail of the lease history
	// should all be on leaseHolder.NodeID. We may metamorphically enable
	// kv.expiration_leases_only.enabled, in which case there will be a single
	// expiration lease, but otherwise we'll have transferred an expiration lease
	// and then upgraded to an epoch lease.
	repl := tc.GetFirstStoreFromServer(t, 3).
		LookupReplica(roachpb.RKey(rhsDesc.StartKey.AsRawKey()))
	history := repl.GetLeaseHistory()

	require.Equal(t, leaseHolder.NodeID, history[len(history)-1].Replica.NodeID)
	var prevLeaseHolder roachpb.NodeID
	for i := len(history) - 1; i >= 0; i-- {
		if id := history[i].Replica.NodeID; id != leaseHolder.NodeID {
			prevLeaseHolder = id
			break
		}
	}
	require.Equal(t, tc.Target(2).NodeID, prevLeaseHolder)
}

func gossipLiveness(t *testing.T, tc *testcluster.TestCluster) {
	for i := range tc.Servers {
		testutils.SucceedsSoon(t, tc.Servers[i].HeartbeatNodeLiveness)
	}
	// Make sure that all store pools have seen liveness heartbeats from everyone.
	testutils.SucceedsSoon(t, func() error {
		for i := range tc.Servers {
			for j := range tc.Servers {
				live, err := tc.GetFirstStoreFromServer(t, i).GetStoreConfig().
					StorePool.IsLive(tc.Target(j).StoreID)
				if err != nil {
					return err
				}
				if !live {
					return errors.Errorf("Server %d is suspect on server %d", j, i)
				}
			}
		}
		return nil
	})
}

// This test replicates the behavior observed in
// https://github.com/cockroachdb/cockroach/issues/62485. We verify that when a
// dc with the leaseholder is lost, a node in a dc that does not have the lease
// preference, can steal the lease and transfer it to a preferred locality in a
// short period of time.
func TestLeasePreferencesDuringOutage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This is a hefty test, so we skip it under short.
	skip.UnderShort(t)
	// The test has 5 nodes. Its possible in stress-race for nodes to be starved
	// out heartbeating their liveness.
	skip.UnderRace(t)

	stickyRegistry := fs.NewStickyRegistry()
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
	numNodes := 5
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
		locality("us", "sf"),
		locality("us", "sf"),
		locality("us", "mi"),
		locality("us", "mi"),
	}

	// This test disables the replicate queue. We wish to enable the replicate
	// queue only for range we are testing, after marking some servers as dead.
	// We also wait until the expected live stores are considered live from n1,
	// if we didn't do this it would be possible for the range to process and be
	// seen as unavailable due to manual clock jumps.
	var testRangeID int64
	var clockJumpMu syncutil.Mutex
	atomic.StoreInt64(&testRangeID, -1)
	disabledQueueBypassFn := func(rangeID roachpb.RangeID) bool {
		if rangeID == roachpb.RangeID(atomic.LoadInt64(&testRangeID)) {
			clockJumpMu.Lock()
			defer clockJumpMu.Unlock()
			return true
		}
		return false
	}
	settings := cluster.MakeTestingClusterSettings()
	sv := &settings.SV
	// The remaining live stores (n1,n4,n5) may become suspect due to manual
	// clock jumps. Disable the suspect timer to prevent them becoming suspect
	// when we bump the clocks.
	liveness.TimeAfterNodeSuspect.Override(ctx, sv, 0)
	timeUntilNodeDead := liveness.TimeUntilNodeDead.Get(sv)

	for i := 0; i < numNodes; i++ {
		serverArgs[i] = base.TestServerArgs{
			Settings: settings,
			Locality: localities[i],
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					WallClock:                 manualClock,
					DefaultZoneConfigOverride: &zcfg,
					StickyVFSRegistry:         stickyRegistry,
				},
				Store: &kvserver.StoreTestingKnobs{
					// The Raft leadership may not end up on the eu node, but it needs to
					// be able to acquire the lease anyway.
					AllowLeaseRequestProposalsWhenNotLeader: true,
					BaseQueueDisabledBypassFilter:           disabledQueueBypassFn,
				},
			},
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:    true,
					StickyVFSID: strconv.FormatInt(int64(i), 10),
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

	key := bootstrap.TestingUserTableDataMin(keys.SystemSQLCodec)
	tc.SplitRangeOrFatal(t, key)
	tc.AddVotersOrFatal(t, key, tc.Targets(1, 3)...)
	repl := tc.GetFirstStoreFromServer(t, 0).LookupReplica(roachpb.RKey(key))
	require.NoError(t, tc.WaitForVoters(key, tc.Targets(1, 3)...))
	tc.TransferRangeLeaseOrFatal(t, *repl.Desc(), tc.Target(1))

	func() {
		// Lock the clockJumpMu, in order to prevent processing the test range before
		// the intended stores are considered live from n1. If we didn't do this, it
		// is possible for n1 to process the test range and find it unavailable
		// (unactionable).
		clockJumpMu.Lock()
		defer clockJumpMu.Unlock()

		// Enable queue processing of the test range, right before we stop the sf
		// datacenter. We expect the test range to be enqueued into the replicate
		// queue shortly after.
		rangeID := repl.GetRangeID()
		atomic.StoreInt64(&testRangeID, int64(rangeID))

		// Shutdown the sf datacenter, which is going to kill the node with the lease.
		tc.StopServer(1)
		tc.StopServer(2)

		wait := func(duration time.Duration) {
			manualClock.Increment(duration.Nanoseconds())
			// Gossip and heartbeat all the live stores, we do this manually otherwise the
			// allocator on server 0 may see everyone as temporarily dead due to the
			// clock move above.
			for _, i := range []int{0, 3, 4} {
				require.NoError(t, tc.Servers[i].HeartbeatNodeLiveness())
				require.NoError(t, tc.GetFirstStoreFromServer(t, i).GossipStore(ctx, true))
			}
		}
		wait(timeUntilNodeDead)

		checkDead := func(store *kvserver.Store, storeIdx int) error {
			if dead, timetoDie, err := store.GetStoreConfig().StorePool.IsDead(
				tc.GetFirstStoreFromServer(t, storeIdx).StoreID()); err != nil || !dead {
				// Sometimes a gossip update arrives right after server shutdown and
				// after we manually moved the time, so move it again.
				if err == nil {
					wait(timetoDie)
				}
				// NB: errors.Wrapf(nil, ...) returns nil.
				// nolint:errwrap
				return errors.Errorf("expected server %d to be dead, instead err=%v, dead=%v", storeIdx, err, dead)
			}
			return nil
		}

		testutils.SucceedsSoon(t, func() error {
			store := tc.GetFirstStoreFromServer(t, 0)
			sl, available, _ := store.GetStoreConfig().StorePool.TestingGetStoreList()
			if available != 3 {
				return errors.Errorf(
					"expected all 3 remaining stores to be live, but only got %d, stores=%v",
					available, sl)
			}
			if err := checkDead(store, 1); err != nil {
				return err
			}
			if err := checkDead(store, 2); err != nil {
				return err
			}
			return nil
		})
	}()

	// Send a request to force lease acquisition on _some_ remaining live node.
	// Note, we expect this to be n1 (server 0).
	ba := &kvpb.BatchRequest{}
	ba.Add(getArgs(key))
	_, pErr := tc.Servers[0].DistSenderI().(kv.Sender).Send(ctx, ba)
	require.Nil(t, pErr)

	testutils.SucceedsSoon(t, func() error {
		// Validate that the lease transferred to a preferred locality. n4 (us) and
		// n5 (us) are the only valid stores to be leaseholders during the outage.
		// n1 is the original leaseholder, expect it to not be the leaseholder now.
		if !repl.OwnsValidLease(ctx, tc.Servers[0].Clock().NowAsClockTimestamp()) {
			return nil
		}

		return errors.Errorf(
			"expected no leaseholder in region=us, but found %v",
			repl.CurrentLeaseStatus(ctx),
		)
	})
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

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	kvserver.ExpirationLeasesOnly.Override(ctx, &st.SV, false) // override metamorphism

	// Speed up lease transfers.
	stickyRegistry := fs.NewStickyRegistry()
	manualClock := hlc.NewHybridManualClock()
	serverArgs := make(map[int]base.TestServerArgs)
	numNodes := 4
	for i := 0; i < numNodes; i++ {
		serverArgs[i] = base.TestServerArgs{
			Settings: st,
			Locality: localities[i],
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					WallClock:                 manualClock,
					DefaultZoneConfigOverride: &zcfg,
					StickyVFSRegistry:         stickyRegistry,
				},
			},
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:    true,
					StickyVFSID: strconv.FormatInt(int64(i), 10),
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

	_, rhsDesc := tc.SplitRangeOrFatal(t, bootstrap.TestingUserTableDataMin(keys.SystemSQLCodec))
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
			suspect, err := tc.GetFirstStoreFromServer(t, i).GetStoreConfig().StorePool.IsUnknown(tc.Target(1).StoreID)
			if err != nil {
				return err
			}
			if !suspect {
				return errors.Errorf("Expected server 1 to be in `storeStatusUnknown` on server %d", i)
			}
		}
		return nil
	})

	runThroughTheLeaseQueue := func(key roachpb.Key) {
		for _, i := range []int{2, 3} {
			repl := tc.GetFirstStoreFromServer(t, i).LookupReplica(roachpb.RKey(key))
			require.NotNil(t, repl)
			// We don't know who the leaseholder might be, so ignore errors.
			_, _ = tc.GetFirstStoreFromServer(t, i).Enqueue(
				ctx, "lease", repl, true /* skipShouldQueue */, false, /* async */
			)
		}
	}

	for _, key := range startKeys {
		testutils.SucceedsSoon(t, func() error {
			runThroughTheLeaseQueue(key)
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
		runThroughTheLeaseQueue(key)
	}
	testutils.SucceedsSoon(t, allLeasesOnNonSuspectStores)
	// Wait out the suspect time.
	suspectDuration := liveness.TimeAfterNodeSuspect.Get(&tc.GetFirstStoreFromServer(t, 0).GetStoreConfig().Settings.SV)
	for i := 0; i < int(math.Ceil(suspectDuration.Seconds())); i++ {
		manualClock.Increment(time.Second.Nanoseconds())
		heartbeat(0, 1, 2, 3)
	}

	testutils.SucceedsSoon(t, func() error {
		// Server 1 should get some leases back as it's no longer suspect.
		for _, key := range startKeys {
			runThroughTheLeaseQueue(key)
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

// TestAlterRangeRelocate verifies that the ALTER_RANGE commands work as expected.
func TestAlterRangeRelocate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const numStores = 6
	tc := testcluster.StartTestCluster(t, numStores,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		},
	)
	defer tc.Stopper().Stop(ctx)

	_, rhsDesc := tc.SplitRangeOrFatal(t, bootstrap.TestingUserTableDataMin(keys.SystemSQLCodec))
	tc.AddVotersOrFatal(t, rhsDesc.StartKey.AsRawKey(), tc.Targets(1, 2)...)

	// We start with having the range under test on (1,2,3).
	db := tc.ServerConn(0)
	// Move 2 -> 4.
	_, err := db.Exec("ALTER RANGE $1 RELOCATE FROM $2 TO $3", rhsDesc.RangeID, 2, 4)
	require.NoError(t, err)
	require.NoError(t, tc.WaitForVoters(rhsDesc.StartKey.AsRawKey(), tc.Targets(0, 2, 3)...))
	// Move lease 1 -> 4.
	_, err = db.Exec("ALTER RANGE $1 RELOCATE LEASE TO $2", rhsDesc.RangeID, 4)
	require.NoError(t, err)
	testutils.SucceedsSoon(t, func() error {
		repl := tc.GetFirstStoreFromServer(t, 3).LookupReplica(rhsDesc.StartKey)
		if !repl.OwnsValidLease(ctx, tc.Servers[0].Clock().NowAsClockTimestamp()) {
			return errors.Errorf("Expected lease to transfer to node 4")
		}
		// Do this to avoid snapshot problems below when we do another replica move.
		if repl != tc.GetRaftLeader(t, rhsDesc.StartKey) {
			return errors.Errorf("Expected node 4 to be the raft leader")
		}
		return nil
	})

	// Move lease 3 -> 5.
	_, err = db.Exec("ALTER RANGE RELOCATE FROM $1 TO $2 FOR (SELECT range_id from crdb_internal.ranges_no_leases where range_id = $3)", 3, 5, rhsDesc.RangeID)
	require.NoError(t, err)
	require.NoError(t, tc.WaitForVoters(rhsDesc.StartKey.AsRawKey(), tc.Targets(0, 3, 4)...))
}

// TestAcquireLeaseTimeout is a regression test that lease acquisition timeouts
// always return a NotLeaseHolderError.
func TestAcquireLeaseTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Set a timeout for the test context, to guard against the test getting stuck.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// blockRangeID, when non-zero, will signal the replica to delay lease
	// requests for the given range until the request's context is cancelled, and
	// return the context error.
	var blockRangeID int32

	maybeBlockLeaseRequest := func(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
		// NB: this test was seen hanging with a lease request with a non-cancelable
		// context. So we only block lease requests that have a cancelable context
		// here. Not all callers use context cancellation, in particular there are a
		// few for requestLeaseLocked such as [1].
		//
		// [1]: https://github.com/cockroachdb/cockroach/blob/2eebdb3cbf3799264c59604590ba66a7edf8c3b3/pkg/kv/kvserver/replica_range_lease.go#L1444
		if ba.IsSingleRequestLeaseRequest() && int32(ba.RangeID) == atomic.LoadInt32(&blockRangeID) && ctx.Done() != nil {
			t.Logf("blocked lease request for r%d", ba.RangeID)
			select {
			case <-ctx.Done():
			case <-time.After(10 * time.Second):
				t.Errorf("ctx did not cancel within 10s in blocked lease req")
			}
			return kvpb.NewError(ctx.Err())
		}
		return nil
	}

	manualClock := hlc.NewHybridManualClock()

	// Start a two-node cluster.
	const numNodes = 2
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			RaftConfig: base.RaftConfig{
				// Lease request timeout depends on Raft election timeout, speed it up.
				RaftHeartbeatIntervalTicks: 1,
				RaftElectionTimeoutTicks:   2,
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					WallClock: manualClock,
				},
				Store: &kvserver.StoreTestingKnobs{
					TestingRequestFilter:                    maybeBlockLeaseRequest,
					AllowLeaseRequestProposalsWhenNotLeader: true,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	// Split off a range, upreplicate it to both servers, and move the lease
	// from n1 to n2.
	splitKey := roachpb.Key("a")
	_, desc := tc.SplitRangeOrFatal(t, splitKey)
	tc.AddVotersOrFatal(t, splitKey, tc.Target(1))
	tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(1))
	repl, err := tc.GetFirstStoreFromServer(t, 0).GetReplica(desc.RangeID)
	require.NoError(t, err)

	// Stop n2 and invalidate its leases by forwarding the clock.
	tc.StopServer(1)
	leaseDuration := tc.GetFirstStoreFromServer(t, 0).GetStoreConfig().RangeLeaseDuration
	manualClock.Increment(leaseDuration.Nanoseconds())
	require.False(t, repl.CurrentLeaseStatus(ctx).IsValid())

	// Trying to acquire the lease should error with an empty NLHE, since the
	// range doesn't have quorum.
	var nlhe *kvpb.NotLeaseHolderError
	_, err = repl.TestingAcquireLease(ctx)
	require.Error(t, err)
	require.IsType(t, &kvpb.NotLeaseHolderError{}, err) // check exact type
	require.ErrorAs(t, err, &nlhe)
	require.Empty(t, nlhe.Lease)

	// Now for the real test: block lease requests for the range, and send off a
	// bunch of sequential lease requests with a small delay, which should join
	// onto the same lease request internally. All of these should return a NLHE
	// when they time out, regardless of the internal mechanics.
	atomic.StoreInt32(&blockRangeID, int32(desc.RangeID))

	const attempts = 20
	var wg sync.WaitGroup
	errC := make(chan error, attempts)
	wg.Add(attempts)
	for i := 0; i < attempts; i++ {
		time.Sleep(10 * time.Millisecond)
		go func() {
			_, err := repl.TestingAcquireLease(ctx)
			errC <- err
			wg.Done()
		}()
	}
	wg.Wait()
	close(errC)

	for err := range errC {
		require.Error(t, err)
		require.IsType(t, &kvpb.NotLeaseHolderError{}, err) // check exact type
		require.ErrorAs(t, err, &nlhe)
		require.Empty(t, nlhe.Lease)
	}
}

// TestLeaseTransfersUseExpirationLeaseAndPromoteCorrectly ensures that lease
// transfers use expiration based leases which are eventually promoted to either
// an epoch based lease or a leader lease, depending on cluster settings.
func TestLeaseTransfersUseExpirationLeasesAndPromoteCorrectly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "lease-type", roachpb.EpochAndLeaderLeaseType(), func(t *testing.T, leaseType roachpb.LeaseType) {
		mu := struct {
			syncutil.Mutex
			lease   *roachpb.Lease
			rangeID roachpb.RangeID
		}{}

		ctx := context.Background()
		st := cluster.MakeTestingClusterSettings()

		kvserver.OverrideDefaultLeaseType(ctx, &st.SV, leaseType)
		manualClock := hlc.NewHybridManualClock()
		tci := serverutils.StartCluster(t, 2, base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Settings: st,
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						// Never ticked -- demonstrating that we're not relying on
						// internal timers to upgrade leases.
						WallClock: manualClock,
					},
					Store: &kvserver.StoreTestingKnobs{
						// Disable proactive renewal of expiration based leases. Lease
						// upgrades happen immediately after applying without needing active
						// renewal.
						DisableAutomaticLeaseRenewal: true,
						LeaseUpgradeInterceptor: func(rangeID roachpb.RangeID, lease *roachpb.Lease) {
							mu.Lock()
							defer mu.Unlock()
							if mu.rangeID == rangeID {
								mu.lease = lease
							}
						},
					},
				},
			},
		})
		tc := tci.(*testcluster.TestCluster)
		defer tc.Stopper().Stop(ctx)

		scratchKey := tc.ScratchRange(t)
		// Add a replica; we're going to move the lease to it below.
		desc := tc.AddVotersOrFatal(t, scratchKey, tc.Target(1))
		mu.Lock()
		mu.rangeID = desc.RangeID
		mu.Unlock()

		n2 := tc.Server(1)
		n2Target := tc.Target(1)

		// Transfer the lease from n1 to n2.
		tc.TransferRangeLeaseOrFatal(t, desc, n2Target)
		testutils.SucceedsSoon(t, func() error {
			li, _, err := tc.FindRangeLeaseEx(ctx, desc, nil)
			require.NoError(t, err)
			if !li.Current().OwnedBy(n2.GetFirstStoreID()) {
				return errors.New("lease still owned by n1")
			}
			return nil
		})

		// Expect it to be upgraded to an epoch based lease or leader lease, based
		// on the version of the test we're running.
		upgradedL, _ := tc.WaitForLeaseUpgrade(ctx, t, desc)
		require.Equal(t, leaseType, upgradedL.Type())

		// Expect it to have been upgraded from an expiration based lease.
		mu.Lock()
		expirationL := mu.lease
		mu.Unlock()
		require.Equal(t, roachpb.LeaseExpiration, expirationL.Type())

		// Expect the two leases to have the same sequence number.
		require.Equal(t, expirationL.Sequence, upgradedL.Sequence, "expiration %s; upgraded %s", expirationL, upgradedL)
	})
}

// TestLeaseRequestBumpsEpoch tests that a non-cooperative lease acquisition of
// an expired epoch lease always bumps the epoch of the outgoing leaseholder,
// regardless of the type of lease being acquired.
func TestLeaseRequestBumpsEpoch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "lease-type", roachpb.TestingAllLeaseTypes(), func(t *testing.T, leaseType roachpb.LeaseType) {
		ctx := context.Background()
		st := cluster.MakeTestingClusterSettings()
		kvserver.OverrideDefaultLeaseType(ctx, &st.SV, roachpb.LeaseEpoch)
		manual := hlc.NewHybridManualClock()
		args := base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Settings: st,
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						// Required by TestCluster.MoveRangeLeaseNonCooperatively.
						AllowLeaseRequestProposalsWhenNotLeader: true,
						DisableAutomaticLeaseRenewal:            true,
					},
					Server: &server.TestingKnobs{
						WallClock: manual,
					},
				},
			},
		}
		tc := testcluster.StartTestCluster(t, 3, args)
		defer tc.Stopper().Stop(ctx)

		// Create range and upreplicate.
		key := tc.ScratchRange(t)
		tc.AddVotersOrFatal(t, key, tc.Target(1), tc.Target(2))
		desc := tc.LookupRangeOrFatal(t, key)

		// Make sure n1 has an epoch lease.
		t0 := tc.Target(0)
		tc.TransferRangeLeaseOrFatal(t, desc, t0)
		prevLease, _, err := tc.FindRangeLease(desc, &t0)
		require.NoError(t, err)
		require.Equal(t, roachpb.LeaseEpoch, prevLease.Type())

		// For the leader leases variant of the test, create a partition such that
		// the leadership moves, and only acquire the lease on the newly elected
		// leader.
		newLeaseHolderNodeIdx := 1
		if leaseType == roachpb.LeaseLeader {
			dropRaftMessagesFrom(t, tc.Servers[1], desc, []roachpb.ReplicaID{1}, nil)
			dropRaftMessagesFrom(t, tc.Servers[2], desc, []roachpb.ReplicaID{1}, nil)
			dropRaftMessagesFrom(t, tc.Servers[0], desc, []roachpb.ReplicaID{2, 3}, nil)

			leaderID := waitForPartitionedLeaderStepDownAndNewLeaderToStepUp(t, tc, desc, 0 /* partitionedNodeIdx */, 1 /* otherNodeIdx */)
			leaderReplDesc, found := desc.Replicas().GetReplicaDescriptorByID(leaderID)
			require.True(t, found)
			newLeaseHolderNodeIdx = int(leaderReplDesc.NodeID - 1)
		}

		for _, s := range tc.Servers {
			// Non-cooperatively move the lease to n2.
			kvserver.OverrideDefaultLeaseType(ctx, &s.ClusterSettings().SV, leaseType)
		}
		t1 := tc.Target(newLeaseHolderNodeIdx)
		newLease, err := tc.MoveRangeLeaseNonCooperatively(t, ctx, desc, t1, manual)
		require.NoError(t, err)
		require.NotNil(t, newLease)
		require.Equal(t, leaseType, newLease.Type())

		// Check that n1's liveness epoch was bumped.
		l0 := tc.Server(0).NodeLiveness().(*liveness.NodeLiveness)
		livenesses, err := l0.ScanNodeVitalityFromKV(ctx)
		require.NoError(t, err)
		var liveness livenesspb.Liveness
		for nodeID, l := range livenesses {
			if nodeID == 1 {
				liveness = l.GetInternalLiveness()
				break
			}
		}
		require.NotZero(t, liveness)
		require.Greater(t, liveness.Epoch, prevLease.Epoch)
	})
}

// TestLeaseRequestFromExpirationToEpochOrLeaderDoesNotRegressExpiration tests
// that a promotion from an expiration-based lease to an epoch-based lease or
// leader lease does not permit the expiration time of the lease to regress.
// This is enforced using the min_expiration field.
func TestLeaseRequestFromExpirationToEpochOrLeaderDoesNotRegressExpiration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "lease-type", roachpb.EpochAndLeaderLeaseType(),
		func(t *testing.T, leaseType roachpb.LeaseType) {
			ctx := context.Background()
			st := cluster.MakeTestingClusterSettings()
			kvserver.ExpirationLeasesOnly.Override(ctx, &st.SV, true) // override metamorphism

			tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
				ReplicationMode: base.ReplicationManual,
				ServerArgs: base.TestServerArgs{
					Settings: st,
				},
			})
			defer tc.Stopper().Stop(ctx)

			// Create scratch range.
			key := tc.ScratchRange(t)
			desc := tc.LookupRangeOrFatal(t, key)

			var ts hlc.Timestamp
			if leaseType == roachpb.LeaseEpoch {
				// Pause n1's node liveness heartbeats, to allow its liveness expiration to
				// fall behind.
				l0 := tc.Server(0).NodeLiveness().(*liveness.NodeLiveness)

				// Wait for the node liveness record to have an epoch set. Otherwise,
				// the test might fail later when attempting to acquire an epoch-based
				// lease with this error: unable to get liveness record. This can happen
				// if we pause the node liveness heartbeat before the record is even
				// created.
				testutils.SucceedsSoon(t, func() error {
					l, ok := l0.GetLiveness(tc.Server(0).NodeID())
					if !ok {
						return errors.New("liveness not found")
					}
					if l.Epoch == 0 {
						return errors.New("liveness epoch not set")
					}
					return nil
				})

				l0.PauseHeartbeatLoopForTest()
				l, ok := l0.GetLiveness(tc.Server(0).NodeID())
				require.True(t, ok)
				ts = l.Expiration.ToTimestamp()
			} else if leaseType == roachpb.LeaseLeader {
				// Pause n1's store liveness heartbeats, to allow its liveness
				// expiration to fall behind.
				store, err := tc.Servers[0].GetStores().(*kvserver.Stores).
					GetStore(tc.Servers[0].GetFirstStoreID())
				require.NoError(t, err)
				s1Ident := slpb.StoreIdent{NodeID: roachpb.NodeID(1), StoreID: store.StoreID()}
				_, ts = store.TestingStoreLivenessSupportManager().SupportFrom(s1Ident)

				// Stop store liveness heartbeats.
				dropStoreLivenessHeartbeatsFrom(t, tc.Servers[0], desc, []roachpb.ReplicaID{1}, nil)
			}

			// Make sure n1 has an expiration-based lease.
			s0 := tc.GetFirstStoreFromServer(t, 0)
			repl := s0.LookupReplica(desc.StartKey)
			require.NotNil(t, repl)
			expLease := repl.CurrentLeaseStatus(ctx)
			require.True(t, expLease.IsValid())
			require.Equal(t, roachpb.LeaseExpiration, expLease.Lease.Type())

			// Wait for the expiration-based lease to have a later expiration than the
			// expiration timestamp in n1's liveness record.
			testutils.SucceedsSoon(t, func() error {
				expLease = repl.CurrentLeaseStatus(ctx)
				if expLease.Expiration().Less(ts) {
					return errors.Errorf("lease %v not extended beyond liveness %v", expLease, ts)
				}
				return nil
			})

			// Enable the specified lease type. This will cause automatic lease
			// renewal to try to promote the expiration-based.
			//
			// For epoch based leases, since we have disabled the background node
			// liveness heartbeat loop, it is critical that this lease promotion
			// synchronously heartbeats node liveness before acquiring the
			// epoch-based lease.
			kvserver.OverrideDefaultLeaseType(ctx, &s0.ClusterSettings().SV, leaseType)

			// Wait for that lease promotion to occur.
			_, curLeaseStatus := tc.WaitForLeaseUpgrade(ctx, t, desc)
			require.Equal(t, leaseType, curLeaseStatus.Lease.Type())

			// Once the lease has been promoted to an epoch-based/leader lease, the
			// effective expiration (maintained for epoch leases indirectly in the
			// liveness record and directly in the lease through its min_expiration
			// field, and for leader lease indirectly in the leadSupportUntil and
			// directly in the lease through its min_expiration) must be greater than
			// or equal to that in the preceding expiration-based lease. If this were
			// to regress, a non-cooperative lease failover to a third lease held by a
			// different node could overlap in MVCC time with the first lease (i.e.
			// its start time could precede expLease.Expiration), violating the lease
			// disjointness property.
			//
			// If we disable the `expPromo` and branch in leases/build.go, this
			// assertion fails.
			require.True(t, expLease.Expiration().LessEq(curLeaseStatus.Expiration()))
		})
}

// TestLeaseStartTimeIsLargerThanPrevLeaseEndTimeAfterRestart tests that if a
// node gets restarted, it will reacquire the lease but the start time of the
// new lease will be larger than the end time of the previous lease. This is
// important to avoid situations where we might accept a write operation that
// violates a previously served future read before the restart (We lose the
// timestamp cache after restarts).
func TestLeaseStartTimeIsLargerThanPrevLeaseEndTimeAfterRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "lease-type", roachpb.TestingAllLeaseTypes(),
		func(t *testing.T, leaseType roachpb.LeaseType) {
			if leaseType == roachpb.LeaseEpoch {
				// Doesn't work for epoch leases since the liveness record might not be
				// found in cache.
				skip.WithIssue(t, 59874)
			}
			storeReg := fs.NewStickyRegistry()
			listenerReg := listenerutil.NewListenerRegistry()
			defer listenerReg.Close()

			ctx := context.Background()
			st := cluster.MakeTestingClusterSettings()
			kvserver.OverrideDefaultLeaseType(ctx, &st.SV, leaseType)

			manualClock := hlc.NewHybridManualClock()
			tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
				ReplicationMode:     base.ReplicationManual,
				ReusableListenerReg: listenerReg,
				ServerArgs: base.TestServerArgs{
					Settings: st,
					RaftConfig: base.RaftConfig{
						RaftTickInterval: 100 * time.Millisecond, // speed up the test
					},
					Knobs: base.TestingKnobs{
						Server: &server.TestingKnobs{
							StickyVFSRegistry: storeReg,
							WallClock:         manualClock,
						},
					},
					StoreSpecs: []base.StoreSpec{
						{
							InMemory:    true,
							StickyVFSID: "test",
						},
					},
				},
			})
			defer tc.Stopper().Stop(ctx)

			// Split off a scratch range.
			key := tc.ScratchRange(t)
			desc := tc.LookupRangeOrFatal(t, key)
			repl := tc.GetFirstStoreFromServer(t, 0).GetReplicaIfExists(desc.RangeID)
			require.NotNil(t, repl)

			// Wait for lease upgrade if it's needed.
			tc.MaybeWaitForLeaseUpgrade(ctx, t, desc)

			// Get current lease start and end times.
			lease, _ := repl.GetLease()
			leaseStartTime := lease.Start
			leaseEndTime := repl.CurrentLeaseStatus(ctx).Expiration()

			// Sanity check that the lease end time is after the lease start time.
			require.True(t, leaseStartTime.ToTimestamp().Less(leaseEndTime))

			// Restart the server and make sure that the lease start time is larger than
			// the support end time.
			tc.StopServer(0)
			require.NoError(t, tc.RestartServer(0))

			// Make sure that the lease is valid.
			var curLease roachpb.Lease
			testutils.SucceedsSoon(t, func() error {
				repl = tc.GetFirstStoreFromServer(t, 0).GetReplicaIfExists(desc.RangeID)
				if repl.CurrentLeaseStatus(ctx).State != kvserverpb.LeaseState_VALID {
					return errors.Errorf("lease not valid")
				}
				return nil
			})

			// Make sure that the lease start time is larger than the prev lease end
			// time.
			curLease, _ = repl.GetLease()
			newLeaseStart := curLease.Start
			require.True(t, leaseEndTime.Less(newLeaseStart.ToTimestamp()))
		})
}
