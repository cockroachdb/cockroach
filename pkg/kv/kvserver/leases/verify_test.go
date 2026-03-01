// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package leases

import (
	"context"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftutil"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	rafttracker "github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestVerify(t *testing.T) {
	type testCase struct {
		name        string
		st          Settings
		input       VerifyInput
		expErr      string
		expRedirect roachpb.ReplicaDescriptor
	}
	runTests := func(t *testing.T, testCases []testCase) {
		for _, tt := range testCases {
			t.Run(tt.name, func(t *testing.T) {
				st := tt.st
				if st == (Settings{}) {
					st = defaultSettings()
				}
				err := Verify(context.Background(), st, tt.input)
				if tt.expErr == "" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
					require.Regexp(t, tt.expErr, err)

					var redirect roachpb.ReplicaDescriptor
					var nlhe *kvpb.NotLeaseHolderError
					if ok := errors.As(err, &nlhe); ok && nlhe.Lease != nil {
						redirect = nlhe.Lease.Replica
					}
					require.Equal(t, tt.expRedirect, redirect)
				}
			})
		}
	}
	makeRaftStatus := func(lead roachpb.ReplicaID, state raftpb.StateType) *raft.Status {
		var status raft.Status
		status.ID = raftpb.PeerID(repl1.ReplicaID)
		status.Lead = raftpb.PeerID(lead)
		status.RaftState = state
		return &status
	}

	t.Run("acquisition", func(t *testing.T) {
		defaultLeaderInput := func() VerifyInput {
			descCpy := desc
			descCpy.InternalReplicas = append([]roachpb.ReplicaDescriptor(nil), desc.InternalReplicas...)
			return VerifyInput{
				LocalStoreID:   repl1.StoreID,
				LocalReplicaID: repl1.ReplicaID,
				Desc:           &descCpy,
				RaftStatus:     makeRaftStatus(repl1.ReplicaID, raftpb.StateLeader),
				RaftCompacted:  4,
				PrevLease: roachpb.Lease{
					Replica:  repl2,
					Epoch:    2,
					Sequence: 7,
				},
				PrevLeaseExpired:   true,
				NextLeaseHolder:    repl1,
				BypassSafetyChecks: false,
			}
		}
		defaultFollowerInput := func() VerifyInput {
			in := defaultLeaderInput()
			in.RaftStatus = makeRaftStatus(repl2.ReplicaID, raftpb.StateFollower)
			return in
		}

		runTests(t, []testCase{
			{
				name:  "leader",
				input: defaultLeaderInput(),
				// No rejection. The leader can request a lease.
				expErr: ``,
			},
			{
				name: "follower, known eligible leader",
				// Someone else is leader.
				input: defaultFollowerInput(),
				// Rejection. A follower can't request a lease.
				expErr:      `\[NotLeaseHolderError\] refusing to acquire lease on follower`,
				expRedirect: repl2,
			},
			{
				name: "follower, lease extension despite known eligible leader",
				// Someone else is leader, but we're the leaseholder.
				input: func() VerifyInput {
					in := defaultFollowerInput()
					in.PrevLease.Replica = repl1
					in.PrevLeaseExpired = false
					return in
				}(),
				// No rejection of lease extensions.
				expErr: ``,
			},
			{
				name: "follower, lease extension of expired lease despite known eligible leader",
				// Someone else is leader, but we're the leaseholder. However, our lease
				// has expired.
				input: func() VerifyInput {
					in := defaultFollowerInput()
					in.PrevLease.Replica = repl1
					in.PrevLeaseExpired = true
					return in
				}(),
				// Rejection. Lease extension not permitted on follower with expired
				// lease.
				expErr:      `\[NotLeaseHolderError\] refusing to acquire lease on follower`,
				expRedirect: repl2,
			},
			{
				name: "follower, known ineligible leader",
				// Someone else is leader, but the leader's type it ineligible to get
				// the lease. Thus, the local proposal will not be rejected.
				input: func() VerifyInput {
					in := defaultFollowerInput()
					in.Desc.InternalReplicas[1].Type = roachpb.VOTER_DEMOTING_LEARNER
					return in
				}(),
				expErr: ``,
			},
			{
				name: "follower, known leader not in range descriptor",
				// The leader is known by Raft, but the local replica is so far behind
				// that its descriptor doesn't contain the leader replica.
				input: func() VerifyInput {
					in := defaultFollowerInput()
					in.Desc.InternalReplicas[1] = repl3
					return in
				}(),
				// We assume that the leader is eligible, and redirect. However, we
				// don't know who to redirect to, so we don't include a hint.
				expErr:      `\[NotLeaseHolderError\] refusing to acquire lease on follower`,
				expRedirect: roachpb.ReplicaDescriptor{},
			},
			{
				name: "follower, unknown leader",
				// Unknown leader.
				input: func() VerifyInput {
					in := defaultFollowerInput()
					in.RaftStatus.Lead = raft.None
					return in
				}(),
				// No rejection if the leader is unknown. See comments in
				// leases.verifyAcquisition.
				expErr: ``,
			},
			{
				name: "follower, unknown leader, 1x replication",
				// Unknown leader.
				input: func() VerifyInput {
					in := defaultFollowerInput()
					in.RaftStatus.Lead = raft.None
					in.Desc.InternalReplicas = in.Desc.InternalReplicas[:1]
					return in
				}(),
				// No rejection if the leader is unknown with 1x replication. See
				// comments in leases.verifyAcquisition.
				expErr: ``,
			},
			{
				name: "follower, unknown leader, reject lease on leader unknown",
				// Unknown leader.
				st: func() Settings {
					st := defaultSettings()
					st.RejectLeaseOnLeaderUnknown = true
					return st
				}(),
				input: func() VerifyInput {
					in := defaultFollowerInput()
					in.RaftStatus.Lead = raft.None
					return in
				}(),
				// Rejection if the leader is unknown and the setting is set. However,
				// we don't know who to redirect to, so we don't include a hint.
				expErr:      `\[NotLeaseHolderError\] refusing to acquire lease on follower`,
				expRedirect: roachpb.ReplicaDescriptor{},
			},
			{
				name: "follower, unknown leader, reject leader lease on leader unknown",
				st: func() Settings {
					st := defaultSettings()
					return st
				}(),
				input: func() VerifyInput {
					in := defaultFollowerInput()
					// Unknown leader.
					in.RaftStatus.Lead = raft.None
					// Leader lease.
					in.DesiredLeaseType = roachpb.LeaseLeader
					return in
				}(),
				// Rejection if the leader is unknown. However, we don't know who to
				// redirect to, so we don't include a hint.
				expErr:      `\[NotLeaseHolderError\] refusing to acquire lease on follower`,
				expRedirect: roachpb.ReplicaDescriptor{},
			},
		})
	})

	t.Run("transfer", func(t *testing.T) {
		const compactedIndex = 4
		defaultInput := func() VerifyInput {
			descCpy := desc
			descCpy.InternalReplicas = append([]roachpb.ReplicaDescriptor(nil), desc.InternalReplicas...)
			return VerifyInput{
				LocalStoreID:   repl1.StoreID,
				LocalReplicaID: repl1.ReplicaID,
				Desc:           &descCpy,
				RaftStatus:     nil, // set below
				RaftCompacted:  compactedIndex,
				PrevLease: roachpb.Lease{
					Replica:  repl1,
					Epoch:    2,
					Sequence: 7,
				},
				PrevLeaseExpired:   false,
				NextLeaseHolder:    repl2,
				BypassSafetyChecks: false,
			}
		}
		defaultNonLeaderInput := func(state raftpb.StateType) VerifyInput {
			in := defaultInput()
			in.RaftStatus = makeRaftStatus(repl2.ReplicaID, state)
			return in
		}
		const noTargetState rafttracker.StateType = math.MaxUint64
		defaultLeaderInput := func(targetState rafttracker.StateType, targetMatch kvpb.RaftIndex) VerifyInput {
			in := defaultInput()
			in.RaftStatus = makeRaftStatus(repl1.ReplicaID, raftpb.StateLeader)
			in.RaftStatus.Progress = map[raftpb.PeerID]rafttracker.Progress{
				raftpb.PeerID(repl1.ReplicaID): {State: rafttracker.StateReplicate, Match: uint64(in.RaftCompacted)},
			}
			if targetState != noTargetState {
				in.RaftStatus.Progress[raftpb.PeerID(repl2.ReplicaID)] = rafttracker.Progress{
					State: targetState, Match: uint64(targetMatch),
				}
			}
			return in
		}

		const leaseTransferSnapshotErrPrefix = `refusing to transfer lease to .* because target may need a Raft snapshot: `
		const leaseTransferSendQueueErr = `refusing to transfer lease to .* because target has a send queue`
		runTests(t, []testCase{
			{
				name:   "follower",
				input:  defaultNonLeaderInput(raftpb.StateFollower),
				expErr: leaseTransferSnapshotErrPrefix + raftutil.LocalReplicaNotLeader.String(),
			},
			{
				name:   "candidate",
				input:  defaultNonLeaderInput(raftpb.StateCandidate),
				expErr: leaseTransferSnapshotErrPrefix + raftutil.LocalReplicaNotLeader.String(),
			},
			{
				name:   "pre-candidate",
				input:  defaultNonLeaderInput(raftpb.StatePreCandidate),
				expErr: leaseTransferSnapshotErrPrefix + raftutil.LocalReplicaNotLeader.String(),
			},
			{
				name:   "leader, no progress for target",
				input:  defaultLeaderInput(noTargetState, 0),
				expErr: leaseTransferSnapshotErrPrefix + raftutil.ReplicaUnknown.String(),
			},
			{
				name:   "leader, target state probe",
				input:  defaultLeaderInput(rafttracker.StateProbe, 0),
				expErr: leaseTransferSnapshotErrPrefix + raftutil.ReplicaStateProbe.String(),
			},
			{
				name:   "leader, target state snapshot",
				input:  defaultLeaderInput(rafttracker.StateSnapshot, 0),
				expErr: leaseTransferSnapshotErrPrefix + raftutil.ReplicaStateSnapshot.String(),
			},
			{
				name:   "leader, target state replicate, match+1 < firstIndex",
				input:  defaultLeaderInput(rafttracker.StateReplicate, compactedIndex-1),
				expErr: leaseTransferSnapshotErrPrefix + raftutil.ReplicaMatchBelowLeadersFirstIndex.String(),
			},
			{
				name:   "leader, target state replicate, match+1 == firstIndex",
				input:  defaultLeaderInput(rafttracker.StateReplicate, compactedIndex),
				expErr: ``,
			},
			{
				name:   "leader, target state replicate, match+1 > firstIndex",
				input:  defaultLeaderInput(rafttracker.StateReplicate, compactedIndex+1),
				expErr: ``,
			},
			{
				name: "leader, target has send queue",
				input: func() VerifyInput {
					in := defaultLeaderInput(rafttracker.StateReplicate, compactedIndex)
					in.TargetHasSendQueue = true
					return in
				}(),
				expErr: leaseTransferSendQueueErr,
			},
			{
				name: "leader, target has no send queue",
				input: func() VerifyInput {
					in := defaultLeaderInput(rafttracker.StateReplicate, compactedIndex)
					in.TargetHasSendQueue = false
					return in
				}(),
				expErr: ``,
			},
			{
				name: "leader, target has send queue, bypass safety checks",
				input: func() VerifyInput {
					in := defaultLeaderInput(rafttracker.StateReplicate, compactedIndex)
					in.TargetHasSendQueue = true
					in.BypassSafetyChecks = true
					return in
				}(),
				expErr: ``,
			},
		})
	})
}
