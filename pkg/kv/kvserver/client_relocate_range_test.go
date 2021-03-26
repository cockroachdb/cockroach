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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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
