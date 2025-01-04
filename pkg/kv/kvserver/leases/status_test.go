// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package leases

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

func TestStatus(t *testing.T) {
	const maxOffset = 100 * time.Nanosecond
	ts := []hlc.ClockTimestamp{
		{WallTime: 500, Logical: 0},  // before lease start
		{WallTime: 1000, Logical: 0}, // lease start
		{WallTime: 2000, Logical: 0}, // within lease
		{WallTime: 2950, Logical: 0}, // within stasis
		{WallTime: 3000, Logical: 0}, // lease expiration
		{WallTime: 3500, Logical: 0}, // expired
		{WallTime: 5000, Logical: 0}, // next expiration
	}
	inStasis := ts[3].ToTimestamp()
	expLease := roachpb.Lease{
		Replica:    repl1,
		Start:      ts[1],
		Expiration: ts[4].ToTimestamp().Clone(),
		ProposedTS: hlc.ClockTimestamp{WallTime: ts[1].WallTime - 100},
	}

	epoLease := roachpb.Lease{
		Replica:    expLease.Replica,
		Start:      expLease.Start,
		Epoch:      5,
		ProposedTS: expLease.ProposedTS,
	}
	epoLeaseMinExp1, epoLeaseMinExp2 := epoLease, epoLease
	epoLeaseMinExp1.MinExpiration = ts[3].ToTimestamp()
	epoLeaseMinExp2.MinExpiration = ts[5].ToTimestamp()

	oldLiveness := livenesspb.Liveness{
		NodeID: 1, Epoch: 4, Expiration: hlc.LegacyTimestamp{WallTime: ts[1].WallTime},
	}
	curLiveness := livenesspb.Liveness{
		NodeID: 1, Epoch: 5, Expiration: hlc.LegacyTimestamp{WallTime: ts[4].WallTime},
	}
	expLiveness := livenesspb.Liveness{
		NodeID: 1, Epoch: 6, Expiration: hlc.LegacyTimestamp{WallTime: ts[6].WallTime},
	}

	leaderLease := roachpb.Lease{
		Replica:       expLease.Replica,
		Start:         expLease.Start,
		Term:          5,
		ProposedTS:    expLease.ProposedTS,
		MinExpiration: ts[2].ToTimestamp(),
	}
	leaderLeaseRemote := leaderLease
	leaderLeaseRemote.Replica = repl2

	raftStatus := func(state raftpb.StateType, term uint64, leadSupport hlc.Timestamp) raft.BasicStatus {
		var s raft.BasicStatus
		s.ID = raftpb.PeerID(repl1.ReplicaID)
		s.RaftState = state
		s.Term = term
		if state == raftpb.StateLeader {
			s.Lead = raftpb.PeerID(repl1.ReplicaID)
		} else {
			s.Lead = raftpb.PeerID(repl2.ReplicaID)
		}
		s.LeadSupportUntil = leadSupport
		return s
	}
	// Basic case.
	leaderStatus := raftStatus(raftpb.StateLeader, 5, ts[4].ToTimestamp())
	// Advanced cases.
	followerStatus := raftStatus(raftpb.StateFollower, 5, hlc.Timestamp{})
	unfortifiedLeaderStatus := raftStatus(raftpb.StateLeader, 5, hlc.Timestamp{})
	steppingDownFollowerStatus := raftStatus(raftpb.StateFollower, 5, ts[4].ToTimestamp())
	followerNewTermUnknownLeadStatus := raftStatus(raftpb.StateFollower, 6, hlc.Timestamp{})
	followerNewTermUnknownLeadStatus.Lead = raft.None
	followerNewTermKnownLeadStatus := raftStatus(raftpb.StateFollower, 6, hlc.Timestamp{})
	leaderNewTermKnownLeadStatus := raftStatus(raftpb.StateLeader, 6, ts[4].ToTimestamp())

	for _, tc := range []struct {
		lease         roachpb.Lease
		now           hlc.ClockTimestamp
		minProposedTS hlc.ClockTimestamp
		reqTS         hlc.Timestamp
		raftStatus    raft.BasicStatus
		liveness      livenesspb.Liveness
		want          kvserverpb.LeaseState
		wantErr       string
	}{
		// Expiration-based lease, EXPIRED.
		{lease: expLease, now: ts[5], want: kvserverpb.LeaseState_EXPIRED},
		{lease: expLease, now: ts[4], want: kvserverpb.LeaseState_EXPIRED},
		// Expiration-based lease, PROSCRIBED.
		{lease: expLease, now: ts[3], minProposedTS: ts[1], want: kvserverpb.LeaseState_PROSCRIBED},
		{lease: expLease, now: ts[3], minProposedTS: ts[2], want: kvserverpb.LeaseState_PROSCRIBED},
		{lease: expLease, now: ts[3], minProposedTS: ts[3], reqTS: inStasis,
			want: kvserverpb.LeaseState_PROSCRIBED},
		{lease: expLease, now: ts[3], minProposedTS: ts[4], reqTS: inStasis,
			want: kvserverpb.LeaseState_PROSCRIBED},
		// Expiration-based lease, UNUSABLE.
		{lease: expLease, now: ts[3], reqTS: inStasis, want: kvserverpb.LeaseState_UNUSABLE},
		// Expiration-based lease, VALID.
		{lease: expLease, now: ts[2], reqTS: ts[2].ToTimestamp(), want: kvserverpb.LeaseState_VALID},

		// Epoch-based lease, EXPIRED.
		{lease: epoLease, now: ts[5], liveness: curLiveness, want: kvserverpb.LeaseState_EXPIRED},
		{lease: epoLease, now: ts[4], liveness: curLiveness, want: kvserverpb.LeaseState_EXPIRED},
		{lease: epoLease, now: ts[2], liveness: expLiveness, want: kvserverpb.LeaseState_EXPIRED},
		{lease: epoLeaseMinExp1, now: ts[5], liveness: curLiveness, want: kvserverpb.LeaseState_EXPIRED},
		{lease: epoLeaseMinExp1, now: ts[4], liveness: curLiveness, want: kvserverpb.LeaseState_EXPIRED},
		{lease: epoLeaseMinExp2, now: ts[5], liveness: curLiveness, want: kvserverpb.LeaseState_EXPIRED},
		// Epoch-based lease, PROSCRIBED.
		{lease: epoLease, now: ts[3], liveness: curLiveness, minProposedTS: ts[1],
			want: kvserverpb.LeaseState_PROSCRIBED},
		{lease: epoLease, now: ts[2], liveness: curLiveness, minProposedTS: ts[2],
			want: kvserverpb.LeaseState_PROSCRIBED},
		// Epoch-based lease, UNUSABLE.
		{lease: epoLease, now: ts[3], liveness: curLiveness, reqTS: ts[3].ToTimestamp(),
			want: kvserverpb.LeaseState_UNUSABLE},
		// Epoch-based lease, VALID.
		{lease: epoLease, now: ts[2], liveness: curLiveness, want: kvserverpb.LeaseState_VALID},
		{lease: epoLeaseMinExp1, now: ts[2], liveness: expLiveness, want: kvserverpb.LeaseState_VALID},
		{lease: epoLeaseMinExp2, now: ts[2], liveness: expLiveness, want: kvserverpb.LeaseState_VALID},
		{lease: epoLeaseMinExp2, now: ts[4], liveness: curLiveness, want: kvserverpb.LeaseState_VALID},
		// Epoch-based lease, ERROR.
		{lease: epoLease, now: ts[2], want: kvserverpb.LeaseState_ERROR,
			wantErr: "liveness record not found"},
		{lease: epoLease, now: ts[2], liveness: oldLiveness, want: kvserverpb.LeaseState_ERROR,
			wantErr: "node liveness info for n1 is stale"},

		// Leader lease, local, EXPIRED.
		{lease: leaderLease, raftStatus: leaderStatus, now: ts[5], want: kvserverpb.LeaseState_EXPIRED},
		{lease: leaderLease, raftStatus: leaderStatus, now: ts[4], want: kvserverpb.LeaseState_EXPIRED},
		// Leader lease, local, PROSCRIBED.
		{lease: leaderLease, raftStatus: leaderStatus, now: ts[3], minProposedTS: ts[1], want: kvserverpb.LeaseState_PROSCRIBED},
		{lease: leaderLease, raftStatus: leaderStatus, now: ts[3], minProposedTS: ts[2], want: kvserverpb.LeaseState_PROSCRIBED},
		{lease: leaderLease, raftStatus: leaderStatus, now: ts[3], minProposedTS: ts[3], reqTS: inStasis, want: kvserverpb.LeaseState_PROSCRIBED},
		{lease: leaderLease, raftStatus: leaderStatus, now: ts[3], minProposedTS: ts[4], reqTS: inStasis, want: kvserverpb.LeaseState_PROSCRIBED},
		// Leader lease, local, UNUSABLE.
		{lease: leaderLease, raftStatus: leaderStatus, now: ts[3], reqTS: inStasis, want: kvserverpb.LeaseState_UNUSABLE},
		// Leader lease, local, VALID.
		{lease: leaderLease, raftStatus: leaderStatus, now: ts[2], reqTS: ts[2].ToTimestamp(), want: kvserverpb.LeaseState_VALID},
		// Leader lease, local, advanced cases, EXPIRED.
		{lease: leaderLease, raftStatus: followerStatus, now: ts[2], want: kvserverpb.LeaseState_EXPIRED},
		{lease: leaderLease, raftStatus: unfortifiedLeaderStatus, now: ts[2], want: kvserverpb.LeaseState_EXPIRED},
		{lease: leaderLease, raftStatus: steppingDownFollowerStatus, now: ts[4], want: kvserverpb.LeaseState_EXPIRED},
		// Leader lease, local, advanced cases, VALID.
		{lease: leaderLease, raftStatus: followerStatus, now: ts[1], want: kvserverpb.LeaseState_VALID},
		{lease: leaderLease, raftStatus: unfortifiedLeaderStatus, now: ts[1], want: kvserverpb.LeaseState_VALID},
		{lease: leaderLease, raftStatus: steppingDownFollowerStatus, now: ts[2], want: kvserverpb.LeaseState_VALID},

		// Leader lease, remote, ERROR.
		{lease: leaderLeaseRemote, raftStatus: followerStatus, now: ts[2], want: kvserverpb.LeaseState_ERROR,
			wantErr: "leader lease is not held locally, cannot determine validity"},
		{lease: leaderLeaseRemote, raftStatus: followerNewTermUnknownLeadStatus, now: ts[2], want: kvserverpb.LeaseState_ERROR,
			wantErr: "leader lease is not held locally, cannot determine validity"},
		// Leader lease, remote, EXPIRED.
		{lease: leaderLeaseRemote, raftStatus: followerNewTermKnownLeadStatus, now: ts[2], want: kvserverpb.LeaseState_EXPIRED},
		{lease: leaderLeaseRemote, raftStatus: leaderNewTermKnownLeadStatus, now: ts[2], want: kvserverpb.LeaseState_EXPIRED},
		// Leader lease, remote, VALID.
		{lease: leaderLeaseRemote, raftStatus: followerStatus, now: ts[1], want: kvserverpb.LeaseState_VALID},
		{lease: leaderLeaseRemote, raftStatus: followerNewTermUnknownLeadStatus, now: ts[1], want: kvserverpb.LeaseState_VALID},
		{lease: leaderLeaseRemote, raftStatus: followerNewTermKnownLeadStatus, now: ts[1], want: kvserverpb.LeaseState_VALID},
		{lease: leaderLeaseRemote, raftStatus: leaderNewTermKnownLeadStatus, now: ts[1], want: kvserverpb.LeaseState_VALID},
	} {
		t.Run("", func(t *testing.T) {
			nl := &mockNodeLiveness{
				record:  liveness.Record{Liveness: tc.liveness},
				missing: tc.liveness == livenesspb.Liveness{},
			}
			in := StatusInput{
				LocalStoreID:       repl1.StoreID,
				MaxOffset:          maxOffset,
				Now:                tc.now,
				MinProposedTs:      tc.minProposedTS,
				MinValidObservedTs: hlc.ClockTimestamp{},
				RaftStatus:         tc.raftStatus,
				RequestTs:          tc.reqTS,
				Lease:              tc.lease,
			}

			got := Status(context.Background(), nl, in)
			if tc.wantErr != "" {
				require.Contains(t, got.ErrInfo, tc.wantErr)
				got.ErrInfo = ""
			}
			exp := kvserverpb.LeaseStatus{
				Lease:       tc.lease,
				Now:         tc.now,
				RequestTime: tc.reqTS,
				State:       tc.want,
				Liveness:    tc.liveness,
			}
			if tc.lease.Type() == roachpb.LeaseLeader && tc.lease.Replica == repl1 {
				exp.LeaderSupport = kvserverpb.RaftLeaderSupport{
					Term:             tc.raftStatus.Term,
					LeadSupportUntil: tc.raftStatus.LeadSupportUntil,
				}
			}
			require.Equal(t, exp, got)
		})
	}
}
