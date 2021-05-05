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
				context.Background(), startKey.AsRawKey(), voterTargets, nonVoterTargets,
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
		lease, _, err := tc.FindRangeLease(desc, &target)
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
		if ops[0].ChangeType == roachpb.ADD_VOTER && ops[1].ChangeType.IsRemoval() {
			return true
		}
	}
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
		err         error
	}
	var intercepted []intercept

	requireNumAtomic := func(expAtomic int, expSingle int, f func() (retries int)) {
		t.Helper()
		intercepted = nil
		retries := f()
		var actAtomic, actSingle int
		for _, ic := range intercepted {
			if ic.err != nil {
				continue
			}
			if usesAtomicReplicationChange(ic.ops) {
				actAtomic++
			} else {
				actSingle += len(ic.ops)
			}
		}
		actAtomic -= retries
		require.Equal(t, expAtomic, actAtomic, "wrong number of atomic changes: %+v", intercepted)
		require.Equal(t, expSingle, actSingle, "wrong number of single changes: %+v", intercepted)
	}

	knobs := base.TestingKnobs{
		Store: &kvserver.StoreTestingKnobs{
			BeforeRelocateOne: func(ops []roachpb.ReplicationChange, leaseTarget *roachpb.ReplicationTarget, err error) {
				intercepted = append(intercepted, intercept{
					ops:         ops,
					leaseTarget: leaseTarget,
					err:         err,
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
		requireNumAtomic(0, 2, func() int {
			return relocateAndCheck(t, tc, k, targets, nil /* nonVoterTargets */)
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
		requireNumAtomic(3, 0, func() int {
			return relocateAndCheck(t, tc, k, targets, nil /* nonVoterTargets */)
		})
	}

	// s4 (LH) s5 s6 ---> s5 (LH)
	// Pure downreplication.
	{
		requireNumAtomic(0, 2, func() int {
			return relocateAndCheck(t, tc, k, tc.Targets(4), nil /* nonVoterTargets */)
		})
	}

	// s5 (LH) ---> s3 (LH)
	// Lateral movement while at replication factor one. In this case atomic
	// replication changes cannot be used; we add-then-remove instead.
	{
		requireNumAtomic(0, 2, func() int {
			return relocateAndCheck(t, tc, k, tc.Targets(2), nil /* nonVoterTargets */)
		})
	}

	// s3 (LH) ---> s2 (LH) s4 s1 --> s4 (LH) s2 s6 s1 --> s3 (LH) s5
	// A grab bag.
	{
		// s3 -(add)-> s3 s2 -(swap)-> s4 s2 -(add)-> s4 s2 s1 (=s2 s4 s1)
		requireNumAtomic(1, 2, func() int {
			return relocateAndCheck(t, tc, k, tc.Targets(1, 3, 0), nil /* nonVoterTargets */)
		})
		// s2 s4 s1 -(add)-> s2 s4 s1 s6 (=s4 s2 s6 s1)
		requireNumAtomic(0, 1, func() int {
			return relocateAndCheck(t, tc, k, tc.Targets(3, 1, 5, 0), nil /* nonVoterTargets */)
		})
		// s4 s2 s6 s1 -(swap)-> s3 s2 s6 s1 -(swap)-> s3 s5 s6 s1 -(del)-> s3 s5 s6 -(del)-> s3 s5
		requireNumAtomic(2, 2, func() int {
			return relocateAndCheck(t, tc, k, tc.Targets(2, 4), nil /* nonVoterTargets */)
		})
	}

	// Simple non-voter relocations.
	{
		requireNumAtomic(0, 2, func() int {
			return relocateAndCheck(t, tc, k, tc.Targets(2, 4), tc.Targets(1, 3))
		})
		// Add & remove.
		requireNumAtomic(0, 2, func() int {
			return relocateAndCheck(t, tc, k, tc.Targets(2, 4), tc.Targets(1, 5))
		})
		// 2 add and 2 remove operations.
		requireNumAtomic(0, 4, func() int {
			return relocateAndCheck(t, tc, k, tc.Targets(2, 4), tc.Targets(0, 3))
		})
	}

	// Relocation scenarios that require swapping of voters with non-voters.
	{
		// Single swap of voter and non-voter.
		requireNumAtomic(1, 0, func() int {
			return relocateAndCheck(t, tc, k, tc.Targets(0, 4), tc.Targets(2, 3))
		})
		// Multiple swaps.
		requireNumAtomic(2, 0, func() int {
			return relocateAndCheck(t, tc, k, tc.Targets(2, 3), tc.Targets(0, 4))
		})
		// Single promotion of non-voter to a voter.
		requireNumAtomic(1, 0, func() int {
			return relocateAndCheck(t, tc, k, tc.Targets(2, 3, 4), tc.Targets(0))
		})
		// Single demotion of voter to a non-voter.
		requireNumAtomic(1, 0, func() int {
			return relocateAndCheck(t, tc, k, tc.Targets(2, 4), tc.Targets(0, 3))
		})
	}
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
			context.Background(), k.AsRawKey(), tc.Targets(subtest.voterTargets...), tc.Targets(subtest.nonVoterTargets...),
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
		err := tc.Stopper().RunAsyncTask(ctx, "request", func(ctx context.Context) {
			reqCtx := context.WithValue(ctx, magicKey{}, struct{}{})
			resp, pErr := kv.SendWrapped(reqCtx, tc.Servers[0].DistSender(), req)
			resultC <- result{resp, pErr}
		})
		require.NoError(t, err)
		<-requestReadyC

		// Transfer leaseholder to other store.
		rangeDesc, err := tc.LookupRange(key)
		require.NoError(t, err)
		repl, err := tc.GetFirstStoreFromServer(t, 0).GetReplica(rangeDesc.RangeID)
		require.NoError(t, err)
		err = tc.MoveRangeLeaseNonCooperatively(rangeDesc, tc.Target(1), manual)
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
