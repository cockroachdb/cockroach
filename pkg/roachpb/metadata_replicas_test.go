// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachpb

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/raft/confchange"
	"github.com/cockroachdb/cockroach/pkg/raft/quorum"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func rd(typ ReplicaType, id uint64) ReplicaDescriptor {
	return ReplicaDescriptor{
		Type:      typ,
		NodeID:    NodeID(100 * id),
		StoreID:   StoreID(10 * id),
		ReplicaID: ReplicaID(id),
	}
}

func TestVotersLearnersAll(t *testing.T) {

	tests := [][]ReplicaDescriptor{
		{},
		{rd(VOTER_FULL, 1)},
		{rd(LEARNER, 1)},
		{rd(VOTER_FULL, 1), rd(LEARNER, 2), rd(VOTER_FULL, 3)},
		{rd(LEARNER, 1), rd(VOTER_FULL, 2), rd(LEARNER, 3)},
		{rd(VOTER_INCOMING, 1)},
		{rd(VOTER_OUTGOING, 1)},
		{rd(LEARNER, 1), rd(VOTER_OUTGOING, 2), rd(VOTER_INCOMING, 3), rd(VOTER_INCOMING, 4)},
	}
	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			r := MakeReplicaSet(test)
			seen := map[ReplicaDescriptor]struct{}{}
			for _, voter := range r.VoterDescriptors() {
				typ := voter.Type
				switch typ {
				case VOTER_FULL, VOTER_INCOMING:
					seen[voter] = struct{}{}
				default:
					assert.FailNow(t, "unexpectedly got a %s as Voter()", typ)
				}
			}
			for _, learner := range r.LearnerDescriptors() {
				seen[learner] = struct{}{}
				assert.Equal(t, LEARNER, learner.Type)
			}

			all := r.Descriptors()
			// Make sure that VOTER_OUTGOING is the only type that is skipped both
			// by LearnerDescriptors() and VoterDescriptors()
			for _, rd := range all {
				if _, seen := seen[rd]; !seen {
					assert.Equal(t, VOTER_OUTGOING, rd.Type)
				} else {
					assert.NotEqual(t, VOTER_OUTGOING, rd.Type)
				}
			}
			assert.Equal(t, len(test), len(all))
		})
	}
}

func TestReplicaDescriptorsRemove(t *testing.T) {
	tests := []struct {
		replicas []ReplicaDescriptor
		remove   ReplicationTarget
		expected bool
	}{
		{
			remove:   ReplicationTarget{NodeID: 1, StoreID: 1},
			expected: false,
		},
		{
			replicas: []ReplicaDescriptor{{NodeID: 1, StoreID: 1}},
			remove:   ReplicationTarget{NodeID: 2, StoreID: 2},
			expected: false,
		},
		{
			replicas: []ReplicaDescriptor{{NodeID: 1, StoreID: 1}},
			remove:   ReplicationTarget{NodeID: 1, StoreID: 1},
			expected: true,
		},
		{
			// Make sure we sort after the swap in removal.
			replicas: []ReplicaDescriptor{
				{NodeID: 1, StoreID: 1},
				{NodeID: 2, StoreID: 2},
				{NodeID: 3, StoreID: 3},
				{NodeID: 4, StoreID: 4, Type: LEARNER},
			},
			remove:   ReplicationTarget{NodeID: 2, StoreID: 2},
			expected: true,
		},
	}
	for i, test := range tests {
		r := MakeReplicaSet(test.replicas)
		lenBefore := len(r.Descriptors())
		removedDesc, ok := r.RemoveReplica(test.remove.NodeID, test.remove.StoreID)
		assert.Equal(t, test.expected, ok, "testcase %d", i)
		if ok {
			assert.Equal(t, test.remove.NodeID, removedDesc.NodeID, "testcase %d", i)
			assert.Equal(t, test.remove.StoreID, removedDesc.StoreID, "testcase %d", i)
			assert.Equal(t, lenBefore-1, len(r.Descriptors()), "testcase %d", i)
		} else {
			assert.Equal(t, lenBefore, len(r.Descriptors()), "testcase %d", i)
		}
		for _, voter := range r.VoterDescriptors() {
			assert.Equal(t, VOTER_FULL, voter.Type, "testcase %d", i)
		}
		for _, learner := range r.LearnerDescriptors() {
			assert.Equal(t, LEARNER, learner.Type, "testcase %d", i)
		}
	}
}

func TestReplicaDescriptorsConfState(t *testing.T) {
	tests := []struct {
		in  []ReplicaDescriptor
		out string
	}{
		{
			[]ReplicaDescriptor{rd(VOTER_FULL, 1)},
			"Voters:[1] VotersOutgoing:[] Learners:[] LearnersNext:[] AutoLeave:false",
		},
		{
			[]ReplicaDescriptor{rd(LEARNER, 1), rd(VOTER_FULL, 2)},
			"Voters:[2] VotersOutgoing:[] Learners:[1] LearnersNext:[] AutoLeave:false",
		},
		// First joint case. We're adding n3 (via atomic replication changes), so the outgoing
		// config we have to get rid of consists only of n2 (even though n2 remains a voter).
		// Note that we could simplify this config so that it's not joint, but raft expects
		// the config exactly as described by the descriptor so we don't try.
		{
			[]ReplicaDescriptor{rd(LEARNER, 1), rd(VOTER_FULL, 2), rd(VOTER_INCOMING, 3)},
			"Voters:[2 3] VotersOutgoing:[2] Learners:[1] LearnersNext:[] AutoLeave:false",
		},
		// More complex joint change: a replica swap, switching out n4 for n3 from the initial
		// set of voters n2, n4 (plus learner n1 before and after).
		{
			[]ReplicaDescriptor{rd(LEARNER, 1), rd(VOTER_FULL, 2), rd(VOTER_INCOMING, 3), rd(VOTER_OUTGOING, 4)},
			"Voters:[2 3] VotersOutgoing:[2 4] Learners:[1] LearnersNext:[] AutoLeave:false",
		},
		// Upreplicating from n1,n2 to n1,n2,n3,n4.
		{
			[]ReplicaDescriptor{rd(VOTER_FULL, 1), rd(VOTER_FULL, 2), rd(VOTER_INCOMING, 3), rd(VOTER_INCOMING, 4)},
			"Voters:[1 2 3 4] VotersOutgoing:[1 2] Learners:[] LearnersNext:[] AutoLeave:false",
		},
		// Downreplicating from n1,n2,n3,n4 to n1,n2.
		{
			[]ReplicaDescriptor{rd(VOTER_FULL, 1), rd(VOTER_FULL, 2), rd(VOTER_OUTGOING, 3), rd(VOTER_OUTGOING, 4)},
			"Voters:[1 2] VotersOutgoing:[1 2 3 4] Learners:[] LearnersNext:[] AutoLeave:false",
		},
		// Completely switching to a new set of replicas: n1,n2 to n4,n5. Throw a learner in for fun.
		{
			[]ReplicaDescriptor{rd(VOTER_OUTGOING, 1), rd(VOTER_OUTGOING, 2), rd(VOTER_INCOMING, 3), rd(VOTER_INCOMING, 4), rd(LEARNER, 5)},
			"Voters:[3 4] VotersOutgoing:[1 2] Learners:[5] LearnersNext:[] AutoLeave:false",
		},
		// Throw in a voter demotion. The demoting voter should be treated as Outgoing and LearnersNext.
		{
			[]ReplicaDescriptor{rd(VOTER_OUTGOING, 1), rd(VOTER_DEMOTING_LEARNER, 2), rd(VOTER_INCOMING, 3), rd(VOTER_INCOMING, 4), rd(LEARNER, 5)},
			"Voters:[3 4] VotersOutgoing:[1 2] Learners:[5] LearnersNext:[2] AutoLeave:false",
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			r := MakeReplicaSet(test.in)
			cs := r.ConfState().Describe()
			require.Equal(t, test.out, cs)
		})
	}
}

func TestReplicaDescriptorsCanMakeProgress(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type descWithLiveness struct {
		live bool
		ReplicaDescriptor
	}

	for _, test := range []struct {
		rds []descWithLiveness
		exp bool
	}{
		// One out of one voter dead.
		{[]descWithLiveness{{false, rd(VOTER_FULL, 1)}}, false},
		// Three out of three voters dead.
		{[]descWithLiveness{
			{false, rd(VOTER_FULL, 1)},
			{false, rd(VOTER_FULL, 2)},
			{false, rd(VOTER_FULL, 3)},
		}, false},
		// Two out of three voters dead.
		{[]descWithLiveness{
			{false, rd(VOTER_FULL, 1)},
			{true, rd(VOTER_FULL, 2)},
			{false, rd(VOTER_FULL, 3)},
		}, false},
		// Two out of three voters alive.
		{[]descWithLiveness{
			{true, rd(VOTER_FULL, 1)},
			{false, rd(VOTER_FULL, 2)},
			{true, rd(VOTER_FULL, 3)},
		}, true},
		// Two out of three voters alive, but one is an incoming voter. The outgoing
		// group doesn't have quorum.
		{[]descWithLiveness{
			{true, rd(VOTER_FULL, 1)},
			{false, rd(VOTER_FULL, 2)},
			{true, rd(VOTER_INCOMING, 3)},
		}, false},
		// Two out of three voters alive, but one is an outgoing voter. The incoming
		// group doesn't have quorum.
		{[]descWithLiveness{
			{true, rd(VOTER_FULL, 1)},
			{false, rd(VOTER_FULL, 2)},
			{true, rd(VOTER_DEMOTING_LEARNER, 3)},
		}, false},
		// Two out of three voters dead, and they're all incoming voters. (This
		// can't happen in practice because it means there were zero voters prior
		// to the conf change, but still this result is correct, similar to others
		// below).
		{[]descWithLiveness{
			{false, rd(VOTER_INCOMING, 2)},
			{false, rd(VOTER_INCOMING, 1)},
			{true, rd(VOTER_INCOMING, 3)},
		}, false},
		// Two out of three voters dead, and two are outgoing, one incoming.
		{[]descWithLiveness{
			{false, rd(VOTER_INCOMING, 1)},
			{false, rd(VOTER_OUTGOING, 2)},
			{true, rd(VOTER_OUTGOING, 3)},
		}, false},
		// 1 and 3 are alive, but that's not a quorum for (1 3)&&(2 3) which is
		// the config here.
		{[]descWithLiveness{
			{true, rd(VOTER_INCOMING, 1)},
			{false, rd(VOTER_OUTGOING, 2)},
			{true, rd(VOTER_FULL, 3)},
		}, false},
		// Same as above, but all three alive.
		{[]descWithLiveness{
			{true, rd(VOTER_INCOMING, 1)},
			{true, rd(VOTER_OUTGOING, 2)},
			{true, rd(VOTER_FULL, 3)},
		}, true},
		// Same, but there are a few learners that should not matter.
		{[]descWithLiveness{
			{true, rd(VOTER_INCOMING, 1)},
			{true, rd(VOTER_OUTGOING, 2)},
			{true, rd(VOTER_FULL, 3)},
			{false, rd(LEARNER, 4)},
			{false, rd(LEARNER, 5)},
			{false, rd(LEARNER, 6)},
			{false, rd(LEARNER, 7)},
		}, true},
		// Non-joint case that should be live unless the learner is somehow taken
		// into account.
		{[]descWithLiveness{
			{true, rd(VOTER_FULL, 1)},
			{true, rd(VOTER_FULL, 2)},
			{false, rd(VOTER_FULL, 4)},
			{false, rd(LEARNER, 4)},
		}, true},
	} {
		t.Run("", func(t *testing.T) {
			rds := make([]ReplicaDescriptor, 0, len(test.rds))
			for _, rDesc := range test.rds {
				rds = append(rds, rDesc.ReplicaDescriptor)
			}

			act := MakeReplicaSet(rds).CanMakeProgress(func(rd ReplicaDescriptor) bool {
				for _, rdi := range test.rds {
					if rdi.ReplicaID == rd.ReplicaID {
						return rdi.live
					}
				}
				return false
			})
			require.Equal(t, test.exp, act, "input: %+v", test)
		})
	}
}

// Test that ReplicaDescriptors.CanMakeProgress() agrees with the equivalent
// etcd/raft's code. We generate random configs and then see whether out
// determination for unavailability matches etcd/raft.
func TestReplicaDescriptorsCanMakeProgressRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	randutil.SeedForTests()

	var progress, noProgress, skipped int

	start := timeutil.Now()
	for timeutil.Since(start) < 100*time.Millisecond {
		// Generate a random range configuration with between 1 and 7 replicas.
		size := 1 + rand.Intn(6)
		rds := make([]ReplicaDescriptor, size)
		liveness := make([]bool, size)
		// Generate a bunch of bits, each one representing the liveness of a different replica.
		livenessBits := rand.Int31()
		for i := range rds {
			rds[i].ReplicaID = ReplicaID(i + 1)
			rds[i].Type = ReplicaType(rand.Intn(len(ReplicaType_name)))
			liveness[i] = (livenessBits >> i & 1) == 0
		}

		rng := MakeReplicaSet(rds)

		crdbCanMakeProgress := rng.CanMakeProgress(func(rd ReplicaDescriptor) bool {
			return liveness[rd.ReplicaID-1]
		})

		raftCanMakeProgress, skip := func() (res bool, skip bool) {
			cfg, _, err := confchange.Restore(
				confchange.Changer{
					Config:           quorum.MakeEmptyConfig(),
					ProgressMap:      tracker.MakeEmptyProgressMap(),
					MaxInflight:      1,
					MaxInflightBytes: 0,
				},
				rng.ConfState(),
			)
			if err != nil {
				if err.Error() != "removed all voters" {
					t.Fatal(err)
				}
				return false, true
			}
			votes := make(map[raftpb.PeerID]bool, len(rng.wrapped))
			for _, rDesc := range rng.wrapped {
				if liveness[rDesc.ReplicaID-1] {
					votes[raftpb.PeerID(rDesc.ReplicaID)] = true
				}
			}
			return cfg.Voters.VoteResult(votes) == quorum.VoteWon, false
		}()

		if skip {
			// Going to an empty config, which is non-sensical. Skipping input.
			skipped++
			continue
		}
		require.Equalf(t, raftCanMakeProgress, crdbCanMakeProgress,
			"input: %s liveness: %v", rng, liveness)
		if crdbCanMakeProgress {
			progress++
		} else {
			noProgress++
		}
	}
	log.Infof(ctx, "progress: %d cases. no progress: %d cases. skipped: %d cases.",
		progress, noProgress, skipped)
}

func TestReplicaSetOperations(t *testing.T) {
	rs := func(ids ...uint64) ReplicaSet {
		replicas := make([]ReplicaDescriptor, 0, len(ids))
		for _, id := range ids {
			replicas = append(replicas, rd(VOTER_FULL, id))
		}
		return MakeReplicaSet(replicas)
	}
	var empty []ReplicaDescriptor
	t.Run("subtract", func(t *testing.T) {
		require.Equal(t, rs(1, 2, 3).Subtract(rs(2, 3)), rs(1).Descriptors())
		require.Equal(t, rs(1, 2, 3).Subtract(rs()), rs(1, 2, 3).Descriptors())
		require.Equal(t, rs(1, 2, 3).Subtract(rs(4, 5, 6)), rs(1, 2, 3).Descriptors())
		require.Equal(t, rs(1, 2).Subtract(rs(6, 1)), rs(2).Descriptors())
		require.Equal(t, rs().Subtract(rs(6, 1)), empty)
	})
	t.Run("difference", func(t *testing.T) {
		{ // {1,2,3}.difference({2,3,4})
			added, removed := rs(1, 2, 3).Difference(rs(2, 3, 4))
			require.Equal(t, added, rs(4).Descriptors())
			require.Equal(t, removed, rs(1).Descriptors())
		}
		{ // {1,2,3}.difference({1,2,3})
			added, removed := rs(1, 2, 3).Difference(rs(1, 2, 3))
			require.Equal(t, added, empty)
			require.Equal(t, removed, empty)
		}
		{ // {}.difference({1,2,3})
			added, removed := rs().Difference(rs(1, 2, 3))
			require.Equal(t, added, rs(1, 2, 3).Descriptors())
			require.Equal(t, removed, empty)
		}
		{ // {1,2,3}.difference({})
			added, removed := rs(1, 2, 3).Difference(rs())
			require.Equal(t, added, empty)
			require.Equal(t, removed, rs(1, 2, 3).Descriptors())
		}
	})
}
