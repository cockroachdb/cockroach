// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// Raft related tests that are very tightly coupled to epoch based leases. An
// equivalent leader leases variant of these tests should exist elsewhere. As
// such, we should be able to get rid of these tests once epoch based leases are
// no longer supported.

// TestRaftCheckQuorumEpochLeases tests that Raft CheckQuorum works properly
// with epoch based leases, i.e. that a leader will step down if it hasn't heard
// from a quorum of followers in the last election timeout interval.
//
//	               n1 (leader)
//	              x  x
//	             x    x
//	(follower) n2 ---- n3 (follower)
//
// We test this by partitioning the leader away from two followers, using either
// a symmetric or asymmetric partition. In the asymmetric case, the leader can
// send heartbeats to followers, but won't receive responses. Eventually, it
// should step down and the followers should elect a new leader.
//
// We also test this with both quiesced and unquiesced ranges.
func TestRaftCheckQuorumEpochLeases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test is timing-sensitive, so skip it under deadlock detector and
	// race.
	skip.UnderDeadlock(t)
	skip.UnderRace(t)

	testutils.RunTrueAndFalse(t, "symmetric", func(t *testing.T, symmetric bool) {
		testutils.RunTrueAndFalse(t, "quiesce", func(t *testing.T, quiesce bool) {
			ctx := context.Background()

			// Only run the test with epoch based leases.
			st := cluster.MakeTestingClusterSettings()
			kvserver.TransferExpirationLeasesFirstEnabled.Override(ctx, &st.SV, false)
			kvserver.ExpirationLeasesOnly.Override(ctx, &st.SV, false)

			tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
				ReplicationMode: base.ReplicationManual,
				ServerArgs: base.TestServerArgs{
					Settings: st,
					RaftConfig: base.RaftConfig{
						RaftEnableCheckQuorum: true,
						RaftTickInterval:      200 * time.Millisecond, // speed up test
					},
					Knobs: base.TestingKnobs{
						Store: &kvserver.StoreTestingKnobs{
							DisableQuiescence: !quiesce,
						},
					},
				},
			})
			defer tc.Stopper().Stop(ctx)

			logStatus := func(s *raft.Status) {
				t.Helper()
				require.NotNil(t, s)
				t.Logf("n%d %s at term=%d commit=%d", s.ID, s.RaftState, s.Term, s.Commit)
			}

			// Create a range, upreplicate it, and replicate a write.
			sender := tc.GetFirstStoreFromServer(t, 0).TestSender()
			key := tc.ScratchRange(t)
			desc := tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)

			_, pErr := kv.SendWrapped(ctx, sender, incrementArgs(key, 1))
			require.NoError(t, pErr.GoError())
			tc.WaitForValues(t, key, []int64{1, 1, 1})

			repl1, err := tc.GetFirstStoreFromServer(t, 0).GetReplica(desc.RangeID)
			require.NoError(t, err)
			repl2, err := tc.GetFirstStoreFromServer(t, 1).GetReplica(desc.RangeID)
			require.NoError(t, err)
			repl3, err := tc.GetFirstStoreFromServer(t, 2).GetReplica(desc.RangeID)
			require.NoError(t, err)

			// Set up dropping of inbound messages on n1 from n2,n3, but don't
			// activate it yet.
			var partitioned atomic.Bool
			dropRaftMessagesFrom(t, tc.Servers[0], desc, []roachpb.ReplicaID{2, 3}, &partitioned)
			if symmetric {
				// Drop outbound messages from n1 to n2,n3 too.
				dropRaftMessagesFrom(t, tc.Servers[1], desc, []roachpb.ReplicaID{1}, &partitioned)
				dropRaftMessagesFrom(t, tc.Servers[2], desc, []roachpb.ReplicaID{1}, &partitioned)
			}

			// Make sure the lease is on n1 and that everyone has applied it.
			tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(0))
			_, pErr = kv.SendWrapped(ctx, sender, incrementArgs(key, 1))
			require.NoError(t, pErr.GoError())
			tc.WaitForValues(t, key, []int64{2, 2, 2})
			t.Logf("n1 has lease")

			// Wait for the range to quiesce, if enabled.
			if quiesce {
				require.Eventually(t, func() bool {
					return repl1.IsQuiescent() && repl2.IsQuiescent() && repl3.IsQuiescent()
				}, 10*time.Second, 100*time.Millisecond)
				t.Logf("n1, n2, and n3 quiesced")
			} else {
				require.False(t, repl1.IsQuiescent() || repl2.IsQuiescent() || repl3.IsQuiescent())
				t.Logf("n1, n2, and n3 not quiesced")
			}

			// Partition n1.
			partitioned.Store(true)
			t.Logf("n1 partitioned")

			// Fetch the leader's initial status.
			initialStatus := repl1.RaftStatus()
			require.Equal(t, raftpb.StateLeader, initialStatus.RaftState)
			logStatus(initialStatus)

			// Unquiesce the leader if necessary. We have to do so by submitting an
			// empty proposal, otherwise the leader will immediately quiesce again.
			if quiesce {
				ok, err := repl1.MaybeUnquiesceAndPropose()
				require.NoError(t, err)
				require.True(t, ok)
				t.Logf("n1 unquiesced")
			} else {
				require.False(t, repl1.IsQuiescent())
				t.Logf("n1 not quiesced")
			}

			// Wait for the leader to become a candidate.
			require.Eventually(t, func() bool {
				status := repl1.RaftStatus()
				logStatus(status)
				return status.RaftState == raftpb.StatePreCandidate
			}, 10*time.Second, 500*time.Millisecond)
			t.Logf("n1 became pre-candidate")

			// In the case of a symmetric partition of a quiesced range, we have to
			// wake up n2 to elect a new leader.
			if quiesce && symmetric {
				require.True(t, repl2.MaybeUnquiesce())
				t.Logf("n2 unquiesced")
			}

			// n2 or n3 should elect a new leader.
			var leaderStatus *raft.Status
			require.Eventually(t, func() bool {
				for _, status := range []*raft.Status{repl2.RaftStatus(), repl3.RaftStatus()} {
					logStatus(status)
					if status.RaftState == raftpb.StateLeader {
						leaderStatus = status
						return true
					}
				}
				return false
			}, 10*time.Second, 500*time.Millisecond)
			t.Logf("n%d became leader", leaderStatus.ID)

			// n1 should remain pre-candidate, since it doesn't hear about the new
			// leader.
			require.Never(t, func() bool {
				status := repl1.RaftStatus()
				logStatus(status)
				return status.RaftState != raftpb.StatePreCandidate
			}, 3*time.Second, 500*time.Millisecond)
			t.Logf("n1 remains pre-candidate")

			// The existing leader shouldn't have been affected by n1's prevotes.
			var finalStatus *raft.Status
			for _, status := range []*raft.Status{repl2.RaftStatus(), repl3.RaftStatus()} {
				logStatus(status)
				if status.RaftState == raftpb.StateLeader {
					finalStatus = status
					break
				}
			}
			require.NotNil(t, finalStatus)
			require.Equal(t, leaderStatus.ID, finalStatus.ID)
			require.Equal(t, leaderStatus.Term, finalStatus.Term)
		})
	})
}
