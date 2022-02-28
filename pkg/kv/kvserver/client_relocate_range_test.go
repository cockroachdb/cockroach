// Copyright 2019 The Cockroach Authors.
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
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func relocateAndCheck(
	t *testing.T,
	tc *testcluster.TestCluster,
	startKey roachpb.RKey,
	voterTargets []roachpb.ReplicationTarget,
	nonVoterTargets []roachpb.ReplicationTarget,
) (retries int) {
	every := log.Every(1 * time.Second)
	testutils.SucceedsSoon(t, func() error {
		err := tc.Servers[0].DB().
			AdminRelocateRange(
				context.Background(),
				startKey.AsRawKey(),
				voterTargets,
				nonVoterTargets,
				true, /* transferLeaseToFirstVoter */
			)
		if err != nil {
			if every.ShouldLog() {
				log.Infof(context.Background(), "AdminRelocateRange failed with error: %s", err)
			}
			retries++
		}
		return err
	})
	desc, err := tc.Servers[0].LookupRange(startKey.AsRawKey())
	require.NoError(t, err)
	requireDescMembers(t, desc, append(voterTargets, nonVoterTargets...))
	if len(voterTargets) > 0 {
		requireLeaseAt(t, tc, desc, voterTargets[0])
	}
	return retries
}

func requireRelocationFailure(
	ctx context.Context,
	t *testing.T,
	tc *testcluster.TestCluster,
	startKey roachpb.RKey,
	voterTargets []roachpb.ReplicationTarget,
	nonVoterTargets []roachpb.ReplicationTarget,
	errRegExp string,
) {
	testutils.SucceedsSoon(t, func() error {
		err := tc.Servers[0].DB().AdminRelocateRange(
			ctx,
			startKey.AsRawKey(),
			voterTargets,
			nonVoterTargets,
			true, /* transferLeaseToFirstVoter */
		)
		if kv.IsExpectedRelocateError(err) {
			return err
		}
		require.Regexp(t, errRegExp, err)
		return nil
	})
}

func requireDescMembers(
	t *testing.T, desc roachpb.RangeDescriptor, targets []roachpb.ReplicationTarget,
) {
	t.Helper()
	targets = append([]roachpb.ReplicationTarget(nil), targets...)
	sort.Slice(targets, func(i, j int) bool { return targets[i].StoreID < targets[j].StoreID })

	have := make([]roachpb.ReplicationTarget, 0, len(targets))
	for _, rDesc := range desc.Replicas().Descriptors() {
		have = append(have, roachpb.ReplicationTarget{
			NodeID:  rDesc.NodeID,
			StoreID: rDesc.StoreID,
		})
	}
	sort.Slice(have, func(i, j int) bool { return have[i].StoreID < have[j].StoreID })
	require.Equal(t, targets, have)
}

func requireLeaseAt(
	t *testing.T,
	tc *testcluster.TestCluster,
	desc roachpb.RangeDescriptor,
	target roachpb.ReplicationTarget,
) {
	t.Helper()
	// NB: under stressrace the lease will sometimes be inactive by the time
	// it's returned here, so don't use FindRangeLeaseHolder which fails when
	// that happens.
	testutils.SucceedsSoon(t, func() error {
		// NB: Specifying a `hint` here does not play well with multi-store
		// TestServers. See TODO inside `TestServer.GetRangeLease()`.
		lease, _, err := tc.FindRangeLease(desc, nil /* hint */)
		if err != nil {
			return err
		}
		if target != (roachpb.ReplicationTarget{
			NodeID:  lease.Replica.NodeID,
			StoreID: lease.Replica.StoreID,
		}) {
			return errors.Errorf("lease %v is not held by %+v", lease, target)
		}
		return nil
	})
}

func usesAtomicReplicationChange(ops []roachpb.ReplicationChange) bool {
	// There are 4 sets of operations that are executed atomically:
	// 1. Voter rebalances (ADD_VOTER, REMOVE_VOTER)
	// 2. Non-voter promoted to voter (ADD_VOTER, REMOVE_NON_VOTER)
	// 3. Voter demoted to non-voter (ADD_NON_VOTER, REMOVE_VOTER)
	// 4. Voter swapped with non-voter (ADD_VOTER, REMOVE_NON_VOTER,
	// ADD_NON_VOTER, REMOVE_VOTER)
	if len(ops) >= 2 {
		// Either a simple voter rebalance, or its a non-voter promotion.
		if ops[0].ChangeType == roachpb.ADD_VOTER && ops[1].ChangeType.IsRemoval() {
			return true
		}
	}
	// Demotion of a voter.
	if len(ops) == 2 &&
		ops[0].ChangeType == roachpb.ADD_NON_VOTER && ops[1].ChangeType == roachpb.REMOVE_VOTER {
		return true
	}
	return false
}

func TestAdminRelocateRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	type intercept struct {
		ops         []roachpb.ReplicationChange
		leaseTarget *roachpb.ReplicationTarget
	}
	var intercepted []intercept

	requireNumAtomic := func(expAtomic int, expSingle int, f func()) {
		t.Helper()
		intercepted = nil
		f()
		var actAtomic, actSingle int
		for _, ic := range intercepted {
			if usesAtomicReplicationChange(ic.ops) {
				actAtomic++
			} else {
				actSingle += len(ic.ops)
			}
		}
		assert.Equal(t, expAtomic, actAtomic, "wrong number of atomic changes")
		assert.Equal(t, expSingle, actSingle, "wrong number of single changes")
		if t.Failed() {
			t.Log("all changes:")
			for i, ic := range intercepted {
				t.Logf("%d: %v", i+1, ic.ops)
			}
			t.FailNow()
		}

	}

	knobs := base.TestingKnobs{
		Store: &kvserver.StoreTestingKnobs{
			OnRelocatedOne: func(ops []roachpb.ReplicationChange, leaseTarget *roachpb.ReplicationTarget) {
				intercepted = append(intercepted, intercept{
					ops:         ops,
					leaseTarget: leaseTarget,
				})
			},
		},
	}
	args := base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	}
	tc := testcluster.StartTestCluster(t, 6, args)
	defer tc.Stopper().Stop(ctx)

	// s1 (LH) ---> s2 (LH) s1 s3
	// Pure upreplication.
	k := keys.MustAddr(tc.ScratchRange(t))
	{
		targets := tc.Targets(1, 0, 2)
		// Expect two single additions, and that's it.
		requireNumAtomic(0, 2, func() {
			relocateAndCheck(t, tc, k, targets, nil /* nonVoterTargets */)
		})
	}

	// s1 (LH) s2 s3 ---> s4 (LH) s5 s6.
	// This is trickier because the leaseholder gets removed, and so do all
	// other replicas (i.e. a simple lease transfer at the beginning won't solve
	// the problem).
	{
		targets := tc.Targets(3, 4, 5)
		// Should carry out three swaps. Note that the leaseholder gets removed
		// in the process (i.e. internally the lease must've been moved around
		// to achieve that).
		requireNumAtomic(3, 0, func() {
			relocateAndCheck(t, tc, k, targets, nil /* nonVoterTargets */)
		})
	}

	// s4 (LH) s5 s6 ---> s5 (LH)
	// Pure downreplication.
	{
		requireNumAtomic(0, 2, func() {
			relocateAndCheck(t, tc, k, tc.Targets(4), nil /* nonVoterTargets */)
		})
	}

	// s5 (LH) ---> s3 (LH)
	// Lateral movement while at replication factor one.
	{
		requireNumAtomic(1, 0, func() {
			relocateAndCheck(t, tc, k, tc.Targets(2), nil /* nonVoterTargets */)
		})
	}

	// s3 (LH) ---> s2 (LH) s4 s1 --> s4 (LH) s2 s6 s1 --> s3 (LH) s5
	// A grab bag.
	{
		// s3 -(add)-> s3 s2 -(swap)-> s4 s2 -(add)-> s4 s2 s1 (=s2 s4 s1)
		requireNumAtomic(1, 2, func() {
			relocateAndCheck(t, tc, k, tc.Targets(1, 3, 0), nil /* nonVoterTargets */)
		})
		// s2 s4 s1 -(add)-> s2 s4 s1 s6 (=s4 s2 s6 s1)
		requireNumAtomic(0, 1, func() {
			relocateAndCheck(t, tc, k, tc.Targets(3, 1, 5, 0), nil /* nonVoterTargets */)
		})
		// s4 s2 s6 s1 -(swap)-> s3 s2 s6 s1 -(swap)-> s3 s5 s6 s1 -(del)-> s3 s5 s6 -(del)-> s3 s5
		requireNumAtomic(2, 2, func() {
			relocateAndCheck(t, tc, k, tc.Targets(2, 4), nil /* nonVoterTargets */)
		})
	}

	// Simple non-voter relocations.
	{
		requireNumAtomic(0, 2, func() {
			relocateAndCheck(t, tc, k, tc.Targets(2, 4), tc.Targets(1, 3))
		})
		// Add & remove.
		requireNumAtomic(0, 2, func() {
			relocateAndCheck(t, tc, k, tc.Targets(2, 4), tc.Targets(1, 5))
		})
		// 2 add and 2 remove operations.
		requireNumAtomic(0, 4, func() {
			relocateAndCheck(t, tc, k, tc.Targets(2, 4), tc.Targets(0, 3))
		})
	}

	// Relocation scenarios that require swapping of voters with non-voters.
	{
		// Single swap of voter and non-voter.
		requireNumAtomic(1, 0, func() {
			relocateAndCheck(t, tc, k, tc.Targets(0, 4), tc.Targets(2, 3))
		})
		// Multiple swaps.
		requireNumAtomic(2, 0, func() {
			relocateAndCheck(t, tc, k, tc.Targets(2, 3), tc.Targets(0, 4))
		})
		// Single promotion of non-voter to a voter.
		requireNumAtomic(1, 0, func() {
			relocateAndCheck(t, tc, k, tc.Targets(2, 3, 4), tc.Targets(0))
		})
		// Single demotion of voter to a non-voter.
		requireNumAtomic(1, 0, func() {
			relocateAndCheck(t, tc, k, tc.Targets(2, 4), tc.Targets(0, 3))
		})
	}
}

// TestAdminRelocateRangeWithoutLeaseTransfer tests that `AdminRelocateRange`
// only transfers the lease away to the first voting replica in the target slice
// if the callers asks it to.
func TestAdminRelocateRangeWithoutLeaseTransfer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	}

	tc := testcluster.StartTestCluster(t, 5 /* numNodes */, args)
	defer tc.Stopper().Stop(ctx)

	k := keys.MustAddr(tc.ScratchRange(t))

	// Add voters to the first three nodes.
	relocateAndCheck(t, tc, k, tc.Targets(0, 1, 2), nil /* nonVoterTargets */)

	// Move the last voter without asking for the lease to move.
	err := tc.Servers[0].DB().AdminRelocateRange(
		context.Background(),
		k.AsRawKey(),
		tc.Targets(3, 1, 0),
		nil,   /* nonVoterTargets */
		false, /* transferLeaseToFirstVoter */
	)
	require.NoError(t, err)
	leaseholder, err := tc.FindRangeLeaseHolder(tc.LookupRangeOrFatal(t, k.AsRawKey()), nil /* hint */)
	require.NoError(t, err)
	require.Equal(
		t,
		roachpb.ReplicationTarget{NodeID: leaseholder.NodeID, StoreID: leaseholder.StoreID},
		tc.Target(0),
	)
}

func TestAdminRelocateRangeFailsWithDuplicates(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	}

	tc := testcluster.StartTestCluster(t, numNodes, args)
	defer tc.Stopper().Stop(ctx)

	k := keys.MustAddr(tc.ScratchRange(t))

	tests := []struct {
		voterTargets, nonVoterTargets []int
		expectedErr                   string
	}{
		{
			voterTargets: []int{1, 1, 2},
			expectedErr:  "list of desired voter targets contains duplicates",
		},
		{
			voterTargets:    []int{1, 2},
			nonVoterTargets: []int{0, 1, 0},
			expectedErr:     "list of desired non-voter targets contains duplicates",
		},
		{
			voterTargets:    []int{1, 2},
			nonVoterTargets: []int{1},
			expectedErr:     "list of voter targets overlaps with the list of non-voter targets",
		},
		{
			voterTargets:    []int{1, 2},
			nonVoterTargets: []int{1, 2},
			expectedErr:     "list of voter targets overlaps with the list of non-voter targets",
		},
	}
	for _, subtest := range tests {
		err := tc.Servers[0].DB().AdminRelocateRange(
			context.Background(),
			k.AsRawKey(),
			tc.Targets(subtest.voterTargets...),
			tc.Targets(subtest.nonVoterTargets...),
			true, /* transferLeaseToFirstVoter */
		)
		require.Regexp(t, subtest.expectedErr, err)
	}
}

// TestAdminRelocateRangeRandom runs a series of random relocations on a scratch
// range and checks to ensure that the relocations were successfully executed.
func TestAdminRelocateRangeRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	args := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					DontIgnoreFailureToTransferLease: true,
				},
				NodeLiveness: kvserver.NodeLivenessTestingKnobs{
					// Use a long liveness duration to avoid flakiness under stress on the
					// lease check performed by `relocateAndCheck`.
					LivenessDuration: 20 * time.Second,
				},
			},
		},
	}
	numNodes, numIterations := 5, 10
	if util.RaceEnabled {
		numNodes, numIterations = 3, 1
	}

	randomRelocationTargets := func() (voterTargets, nonVoterTargets []int) {
		targets := make([]int, numNodes)
		for i := 0; i < numNodes; i++ {
			targets[i] = i
		}
		numVoters := 1 + rand.Intn(numNodes) // Need at least one voter.
		rand.Shuffle(numNodes, func(i, j int) {
			targets[i], targets[j] = targets[j], targets[i]
		})

		return targets[:numVoters], targets[numVoters:]
	}

	tc := testcluster.StartTestCluster(t, numNodes, args)
	defer tc.Stopper().Stop(ctx)

	k := keys.MustAddr(tc.ScratchRange(t))
	for i := 0; i < numIterations; i++ {
		voters, nonVoters := randomRelocationTargets()
		relocateAndCheck(t, tc, k, tc.Targets(voters...), tc.Targets(nonVoters...))
	}
}

// Regression test for https://github.com/cockroachdb/cockroach/issues/64325
// which makes sure an in-flight read operation during replica removal won't
// return empty results.
func TestReplicaRemovalDuringGet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc, key, evalDuringReplicaRemoval := setupReplicaRemovalTest(t, ctx)
	defer tc.Stopper().Stop(ctx)

	// Perform write.
	pArgs := putArgs(key, []byte("foo"))
	_, pErr := kv.SendWrapped(ctx, tc.Servers[0].DistSender(), pArgs)
	require.Nil(t, pErr)

	// Perform delayed read during replica removal.
	resp, pErr := evalDuringReplicaRemoval(ctx, getArgs(key))
	require.Nil(t, pErr)
	require.NotNil(t, resp)
	require.NotNil(t, resp.(*roachpb.GetResponse).Value)
	val, err := resp.(*roachpb.GetResponse).Value.GetBytes()
	require.NoError(t, err)
	require.Equal(t, []byte("foo"), val)
}

// Regression test for https://github.com/cockroachdb/cockroach/issues/46329
// which makes sure an in-flight conditional put operation during replica
// removal won't spuriously error due to an unexpectedly missing value.
func TestReplicaRemovalDuringCPut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc, key, evalDuringReplicaRemoval := setupReplicaRemovalTest(t, ctx)
	defer tc.Stopper().Stop(ctx)

	// Perform write.
	pArgs := putArgs(key, []byte("foo"))
	_, pErr := kv.SendWrapped(ctx, tc.Servers[0].DistSender(), pArgs)
	require.Nil(t, pErr)

	// Perform delayed conditional put during replica removal. This will cause
	// an ambiguous result error, as outstanding proposals in the leaseholder
	// replica's proposal queue will be aborted when the replica is removed.
	// If the replica was removed from under us, it would instead return a
	// ConditionFailedError since it finds nil in place of "foo".
	req := cPutArgs(key, []byte("bar"), []byte("foo"))
	_, pErr = evalDuringReplicaRemoval(ctx, req)
	require.NotNil(t, pErr)
	require.IsType(t, &roachpb.AmbiguousResultError{}, pErr.GetDetail())
}

// setupReplicaRemovalTest sets up a test cluster that can be used to test
// request evaluation during replica removal. It returns a running test
// cluster, the first key of a blank scratch range on the replica to be
// removed, and a function that can execute a delayed request just as the
// replica is being removed.
func setupReplicaRemovalTest(
	t *testing.T, ctx context.Context,
) (
	*testcluster.TestCluster,
	roachpb.Key,
	func(context.Context, roachpb.Request) (roachpb.Response, *roachpb.Error),
) {
	t.Helper()

	type magicKey struct{}

	requestReadyC := make(chan struct{}) // signals main thread that request is teed up
	requestEvalC := make(chan struct{})  // signals cluster to evaluate the request
	evalFilter := func(args kvserverbase.FilterArgs) *roachpb.Error {
		if args.Ctx.Value(magicKey{}) != nil {
			requestReadyC <- struct{}{}
			<-requestEvalC
		}
		return nil
	}

	manual := hlc.NewHybridManualClock()
	args := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
						TestingEvalFilter: evalFilter,
					},
					// Required by TestCluster.MoveRangeLeaseNonCooperatively.
					AllowLeaseRequestProposalsWhenNotLeader: true,
				},
				Server: &server.TestingKnobs{
					ClockSource: manual.UnixNano,
				},
			},
		},
	}
	tc := testcluster.StartTestCluster(t, 2, args)

	// Create range and upreplicate.
	key := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, key, tc.Target(1))

	// Return a function that can be used to evaluate a delayed request
	// during replica removal.
	evalDuringReplicaRemoval := func(ctx context.Context, req roachpb.Request) (roachpb.Response, *roachpb.Error) {
		// Submit request and wait for it to block.
		type result struct {
			resp roachpb.Response
			err  *roachpb.Error
		}
		resultC := make(chan result)
		srv := tc.Servers[0]
		err := srv.Stopper().RunAsyncTask(ctx, "request", func(ctx context.Context) {
			reqCtx := context.WithValue(ctx, magicKey{}, struct{}{})
			resp, pErr := kv.SendWrapped(reqCtx, srv.DistSender(), req)
			resultC <- result{resp, pErr}
		})
		require.NoError(t, err)
		<-requestReadyC

		// Transfer leaseholder to other store.
		rangeDesc, err := tc.LookupRange(key)
		require.NoError(t, err)
		repl, err := tc.GetFirstStoreFromServer(t, 0).GetReplica(rangeDesc.RangeID)
		require.NoError(t, err)
		_, err = tc.MoveRangeLeaseNonCooperatively(ctx, rangeDesc, tc.Target(1), manual)
		require.NoError(t, err)

		// Remove first store from raft group.
		tc.RemoveVotersOrFatal(t, key, tc.Target(0))

		// Wait for replica removal. This is a bit iffy. We want to make sure
		// that, in the buggy case, we will typically fail (i.e. the request
		// returns incorrect results because the replica was removed). However,
		// in the non-buggy case the in-flight request will be holding
		// readOnlyCmdMu until evaluated, blocking the replica removal, so
		// waiting for replica removal would deadlock. We therefore take the
		// easy way out by starting an async replica GC and sleeping for a bit.
		err = tc.Stopper().RunAsyncTask(ctx, "replicaGC", func(ctx context.Context) {
			assert.NoError(t, tc.GetFirstStoreFromServer(t, 0).ManualReplicaGC(repl))
		})
		require.NoError(t, err)
		time.Sleep(500 * time.Millisecond)

		// Allow request to resume, and return the result.
		close(requestEvalC)
		r := <-resultC
		return r.resp, r.err
	}

	return tc, key, evalDuringReplicaRemoval
}

// TestAdminRelocateRangeLaterallyAmongStores tests that `AdminRelocateRange` is
// able to relocate ranges laterally (i.e. between stores on the same node).
func TestAdminRelocateRangeLaterallyAmongStores(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	// Set up a test cluster with each node having 2 stores.
	args := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{
				{InMemory: true},
				{InMemory: true},
			},
		},
		ReplicationMode: base.ReplicationManual,
	}
	tc := testcluster.StartTestCluster(t, 5, args)
	defer tc.Stopper().Stop(ctx)

	for i := 0; i < tc.NumServers(); i++ {
		tc.WaitForNStores(t, tc.NumServers()*2, tc.Server(i).GossipI().(*gossip.Gossip))
	}

	scratchKey := keys.MustAddr(tc.ScratchRange(t))
	// Place replicas for the scratch range on stores 1, 3, 5 (i.e. the first
	// store on each of the nodes). Note that the test cluster will start off with
	// (n1,s1) already having a replica.
	scratchDesc := tc.LookupRangeOrFatal(t, scratchKey.AsRawKey())
	_, found := scratchDesc.GetReplicaDescriptor(1)
	require.True(t, found)
	tc.AddVotersOrFatal(t, scratchKey.AsRawKey(), []roachpb.ReplicationTarget{
		{NodeID: 2, StoreID: 3},
		{NodeID: 3, StoreID: 5},
	}...)
	// Now, ask `AdminRelocateRange()` to move all of these replicas laterally.
	relocateAndCheck(
		t, tc, scratchKey, []roachpb.ReplicationTarget{
			{NodeID: 1, StoreID: 2},
			{NodeID: 2, StoreID: 4},
			{NodeID: 3, StoreID: 5},
		}, nil, /* nonVoterTargets */
	)
	// Ensure that this sort of lateral relocation works even across non-voters
	// and voters.
	relocateAndCheck(
		t, tc, scratchKey, []roachpb.ReplicationTarget{
			{NodeID: 2, StoreID: 4},
			{NodeID: 3, StoreID: 5},
		}, []roachpb.ReplicationTarget{
			{NodeID: 1, StoreID: 1},
		},
	)
	relocateAndCheck(
		t, tc, scratchKey, []roachpb.ReplicationTarget{
			{NodeID: 2, StoreID: 4},
			{NodeID: 3, StoreID: 5},
		}, []roachpb.ReplicationTarget{
			{NodeID: 1, StoreID: 2},
		},
	)

	// Ensure that, in case a caller of `AdminRelocateRange` tries to place 2
	// replicas on the same node, a safeguard inside `AdminChangeReplicas()`
	// rejects the operation.
	requireRelocationFailure(
		ctx, t, tc, scratchKey, []roachpb.ReplicationTarget{
			{NodeID: 1, StoreID: 1},
			{NodeID: 1, StoreID: 2},
			{NodeID: 2, StoreID: 4},
			{NodeID: 3, StoreID: 5},
		}, nil, /* nonVoterTargets */
		"node 1 already has a replica", /* errRegExp */
	)
	// Same as above, but for non-voting replicas.
	requireRelocationFailure(
		ctx, t, tc, scratchKey, []roachpb.ReplicationTarget{
			{NodeID: 2, StoreID: 4},
			{NodeID: 3, StoreID: 5},
		}, []roachpb.ReplicationTarget{
			{NodeID: 1, StoreID: 1},
			{NodeID: 1, StoreID: 2},
		}, "node 1 already has a replica", /* errRegExp */
	)
	// Ensure that we can't place 2 replicas on the same node even if one is a
	// voter and the other is a non-voter.
	requireRelocationFailure(
		ctx, t, tc, scratchKey, []roachpb.ReplicationTarget{
			{NodeID: 1, StoreID: 1},
			{NodeID: 2, StoreID: 4},
			{NodeID: 3, StoreID: 5},
		}, []roachpb.ReplicationTarget{
			{NodeID: 1, StoreID: 2},
		}, "node 1 already has a replica", /* errRegExp */
	)
}
