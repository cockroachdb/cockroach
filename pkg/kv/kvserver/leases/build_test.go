// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package leases

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/zerofields"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

var (
	repl1 = roachpb.ReplicaDescriptor{NodeID: 1, StoreID: 1, ReplicaID: 1}
	repl2 = roachpb.ReplicaDescriptor{NodeID: 2, StoreID: 2, ReplicaID: 2}
	repl3 = roachpb.ReplicaDescriptor{NodeID: 3, StoreID: 3, ReplicaID: 3}
	desc  = roachpb.RangeDescriptor{InternalReplicas: []roachpb.ReplicaDescriptor{repl1, repl2}}
	cts10 = hlc.ClockTimestamp{WallTime: 10}
	cts20 = hlc.ClockTimestamp{WallTime: 20}
	cts30 = hlc.ClockTimestamp{WallTime: 30}
	cts40 = hlc.ClockTimestamp{WallTime: 40}
	cts50 = hlc.ClockTimestamp{WallTime: 50}
	ts10  = cts10.ToTimestamp()
	ts30  = cts30.ToTimestamp()
	ts40  = cts40.ToTimestamp()
	ts50  = cts50.ToTimestamp()
)

func TestInputValidation(t *testing.T) {
	testCases := []struct {
		name   string
		input  BuildInput
		expErr string
	}{
		{
			name:   "empty",
			input:  BuildInput{},
			expErr: "no lease target provided",
		},
		{
			name: "no timestamp",
			input: BuildInput{
				NextLeaseHolder: repl2,
			},
			expErr: "no clock timestamp provided",
		},
		{
			name: "invalid minimum lease proposed timestamp",
			input: BuildInput{
				NextLeaseHolder:    repl2,
				Now:                cts20,
				MinLeaseProposedTS: cts30,
			},
			expErr: "clock timestamp earlier than minimum lease proposed timestamp",
		},
		{
			name: "remote transfer",
			input: BuildInput{
				LocalStoreID:    repl1.StoreID,
				Now:             cts20,
				RaftStatus:      &raft.Status{},
				PrevLease:       roachpb.Lease{Replica: repl2, Expiration: &ts30},
				NextLeaseHolder: repl2,
			},
			expErr: "cannot acquire/extend lease for remote replica",
		},
		{
			name: "epoch without liveness",
			input: BuildInput{
				LocalStoreID:     repl1.StoreID,
				Now:              cts20,
				RaftStatus:       &raft.Status{},
				PrevLease:        roachpb.Lease{Replica: repl2, Epoch: 3},
				PrevLeaseExpired: true,
				NextLeaseHolder:  repl1,
			},
			expErr: "previous lease is epoch-based: true, but liveness is set: false",
		},
		{
			name: "expiration with liveness",
			input: BuildInput{
				LocalStoreID:          repl1.StoreID,
				Now:                   cts20,
				RaftStatus:            &raft.Status{},
				PrevLease:             roachpb.Lease{Replica: repl2, Expiration: &ts30},
				PrevLeaseNodeLiveness: defaultNodeLivenessRecord(repl2.NodeID).Liveness,
				PrevLeaseExpired:      true,
				NextLeaseHolder:       repl1,
			},
			expErr: "previous lease is epoch-based: false, but liveness is set: true",
		},
		{
			name: "previous lease expired incorrect, computed false",
			input: BuildInput{
				LocalStoreID:     repl1.StoreID,
				Now:              cts20,
				RaftStatus:       &raft.Status{},
				PrevLease:        roachpb.Lease{Replica: repl2, Expiration: &ts30},
				PrevLeaseExpired: true,
				NextLeaseHolder:  repl1,
			},
			expErr: "PrevLeaseExpired=true, but computed false",
		},
		{
			name: "previous lease expired incorrect, computed true",
			input: BuildInput{
				LocalStoreID:     repl1.StoreID,
				Now:              cts20,
				RaftStatus:       &raft.Status{},
				PrevLease:        roachpb.Lease{Replica: repl2, Expiration: &ts10},
				PrevLeaseExpired: false,
				NextLeaseHolder:  repl1,
			},
			expErr: "PrevLeaseExpired=false, but computed true",
		},
		{
			name: "acquisition before expiration",
			input: BuildInput{
				LocalStoreID:     repl1.StoreID,
				Now:              cts20,
				RaftStatus:       &raft.Status{},
				PrevLease:        roachpb.Lease{Replica: repl2, Expiration: &ts30},
				PrevLeaseExpired: false,
				NextLeaseHolder:  repl1,
			},
			expErr: "cannot acquire lease from another node before it has expired",
		},
		{
			name: "bypass safety checks for acquisition",
			input: BuildInput{
				LocalStoreID:       repl1.StoreID,
				Now:                cts20,
				RaftStatus:         &raft.Status{},
				PrevLease:          roachpb.Lease{Replica: repl2, Expiration: &ts10},
				PrevLeaseExpired:   true,
				NextLeaseHolder:    repl1,
				BypassSafetyChecks: true,
			},
			expErr: "cannot bypass safety checks for lease acquisition/extension",
		},
		{
			name: "valid acquisition",
			input: BuildInput{
				LocalStoreID:     repl1.StoreID,
				Now:              cts20,
				RaftStatus:       &raft.Status{},
				PrevLease:        roachpb.Lease{Replica: repl2, Expiration: &ts10},
				PrevLeaseExpired: true,
				NextLeaseHolder:  repl1,
			},
			expErr: "",
		},
		{
			name: "valid extension",
			input: BuildInput{
				LocalStoreID:    repl1.StoreID,
				Now:             cts20,
				RaftStatus:      &raft.Status{},
				PrevLease:       roachpb.Lease{Replica: repl1, Expiration: &ts30},
				NextLeaseHolder: repl1,
			},
			expErr: "",
		},
		{
			name: "valid transfer",
			input: BuildInput{
				LocalStoreID:    repl1.StoreID,
				Now:             cts20,
				RaftStatus:      &raft.Status{},
				PrevLease:       roachpb.Lease{Replica: repl1, Expiration: &ts30},
				NextLeaseHolder: repl2,
			},
			expErr: "",
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.input.validate()
			if tt.expErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Regexp(t, tt.expErr, err)
			}
		})
	}
}

func defaultSettings() Settings {
	return Settings{
		UseExpirationLeases:               false,
		TransferExpirationLeases:          true,
		PreferLeaderLeasesOverEpochLeases: false,
		RejectLeaseOnLeaderUnknown:        false,
		ExpToEpochEquiv:                   true,
		MinExpirationSupported:            true,
		RangeLeaseDuration:                20,
	}
}

func useExpirationSettings() Settings {
	st := defaultSettings()
	st.UseExpirationLeases = true
	return st
}

func useLeaderSettings() Settings {
	st := defaultSettings()
	st.PreferLeaderLeasesOverEpochLeases = true
	return st
}

func raftStatusFollower(replicaID roachpb.ReplicaID) *raft.Status {
	s := &raft.Status{}
	s.ID = raftpb.PeerID(replicaID)
	s.Term = 5
	s.RaftState = raftpb.StateFollower
	return s
}

func raftStatusLeader(replicaID roachpb.ReplicaID) *raft.Status {
	s := raftStatusFollower(replicaID)
	s.RaftState = raftpb.StateLeader
	s.LeadSupportUntil = ts30
	return s
}

func raftStatusLeaderDuringTransfer(replicaID roachpb.ReplicaID) *raft.Status {
	s := raftStatusLeader(replicaID)
	s.LeadTransferee = raftpb.PeerID(repl2.ReplicaID)
	return s
}

// mockNodeLiveness implements the NodeLiveness interface.
type mockNodeLiveness struct {
	record  liveness.Record
	missing bool
}

func (m *mockNodeLiveness) GetLiveness(_ roachpb.NodeID) (liveness.Record, bool) {
	return m.record, !m.missing
}

func defaultNodeLivenessRecord(nodeID roachpb.NodeID) *liveness.Record {
	return &liveness.Record{Liveness: livenesspb.Liveness{
		NodeID:     nodeID,
		Epoch:      3,
		Expiration: ts30.ToLegacyTimestamp(),
	}}
}

func defaultNodeLiveness() NodeLiveness {
	return &mockNodeLiveness{
		record: *defaultNodeLivenessRecord(repl1.NodeID),
	}
}

func missingNodeLiveness() NodeLiveness {
	return &mockNodeLiveness{
		missing: true,
	}
}

func TestBuild(t *testing.T) {
	type testCase struct {
		name      string
		st        Settings
		nl        NodeLiveness
		input     BuildInput
		expOutput Output
		expErr    string
	}
	runTests := func(t *testing.T, testCases []testCase) {
		for _, tt := range testCases {
			t.Run(tt.name, func(t *testing.T) {
				st := tt.st
				if st == (Settings{}) {
					st = defaultSettings()
				}
				nl := tt.nl
				if nl == nil {
					nl = defaultNodeLiveness()
				}
				require.NoError(t, tt.input.validate())
				out, err := build(st, nl, tt.input)
				if tt.expErr == "" {
					require.NoError(t, err)
					require.Equal(t, tt.expOutput, out)
				} else {
					require.Zero(t, out)
					require.Error(t, err)
					require.Regexp(t, tt.expErr, err)
				}
			})
		}
	}

	t.Run("acquisition", func(t *testing.T) {
		defaultInput := BuildInput{
			LocalStoreID: repl1.StoreID,
			Now:          cts20,
			RaftStatus:   raftStatusLeader(repl1.ReplicaID),
			PrevLease: roachpb.Lease{
				Replica:  repl2,
				Epoch:    2,
				Sequence: 7,
			},
			PrevLeaseNodeLiveness: defaultNodeLivenessRecord(repl2.NodeID).Liveness,
			PrevLeaseExpired:      true,
			NextLeaseHolder:       repl1,
		}
		expirationInput := func() BuildInput {
			i := defaultInput
			i.PrevLease.Expiration = &ts10
			i.PrevLease.Epoch = 0
			i.PrevLeaseNodeLiveness = livenesspb.Liveness{}
			return i
		}()

		runTests(t, []testCase{
			{
				name:  "basic",
				input: defaultInput,
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:         repl1,
						Start:           cts20,
						ProposedTS:      cts20,
						Epoch:           3,
						Sequence:        8,
						AcquisitionType: roachpb.LeaseAcquisitionType_Request,
					},
				},
			},
			{
				name: "basic, previous expired, prior epoch",
				input: func() BuildInput {
					i := defaultInput
					i.PrevLeaseExpired = true
					return i
				}(),
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:         repl1,
						Start:           cts20,
						ProposedTS:      cts20,
						Epoch:           3,
						Sequence:        8,
						AcquisitionType: roachpb.LeaseAcquisitionType_Request,
					},
					// No need to increment other node's node liveness record.
				},
			},
			{
				name: "basic, previous expired, same epoch",
				input: func() BuildInput {
					i := defaultInput
					i.Now = cts30
					i.PrevLease.Epoch = 3
					i.PrevLeaseExpired = true
					return i
				}(),
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:         repl1,
						Start:           cts30,
						ProposedTS:      cts30,
						Epoch:           3,
						Sequence:        8,
						AcquisitionType: roachpb.LeaseAcquisitionType_Request,
					},
					// Requires increment of other node's node liveness record.
					NodeLivenessManipulation: NodeLivenessManipulation{
						Increment: &defaultNodeLivenessRecord(repl2.NodeID).Liveness,
					},
				},
			},
			{
				name:  "replace expiration",
				input: expirationInput,
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica: repl1,
						// Start time backdated to the expiration of the previous lease.
						Start:           hlc.ClockTimestamp{WallTime: 10, Logical: 1},
						ProposedTS:      cts20,
						Epoch:           3,
						Sequence:        8,
						AcquisitionType: roachpb.LeaseAcquisitionType_Request,
					},
				},
			},
			{
				name:  "acquire expiration",
				st:    useExpirationSettings(),
				input: defaultInput,
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:               repl1,
						Start:                 cts20,
						ProposedTS:            cts20,
						Expiration:            &ts40,
						DeprecatedStartStasis: &ts40,
						Sequence:              8,
						AcquisitionType:       roachpb.LeaseAcquisitionType_Request,
					},
				},
			},
			{
				name:  "replace expiration, acquire expiration",
				st:    useExpirationSettings(),
				input: expirationInput,
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica: repl1,
						// Start time backdated to the expiration of the previous lease.
						Start:                 hlc.ClockTimestamp{WallTime: 10, Logical: 1},
						ProposedTS:            cts20,
						Expiration:            &ts40,
						DeprecatedStartStasis: &ts40,
						Sequence:              8,
						AcquisitionType:       roachpb.LeaseAcquisitionType_Request,
					},
				},
			},
			{
				name:  "acquire leader lease",
				st:    useLeaderSettings(),
				input: defaultInput,
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:         repl1,
						Start:           cts20,
						ProposedTS:      cts20,
						Term:            5,
						Sequence:        8,
						AcquisitionType: roachpb.LeaseAcquisitionType_Request,
						MinExpiration:   ts40,
					},
				},
			},
			{
				name:  "replace expiration, acquire leader lease",
				st:    useLeaderSettings(),
				input: expirationInput,
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica: repl1,
						// Start time backdated to the expiration of the previous lease.
						Start:           hlc.ClockTimestamp{WallTime: 10, Logical: 1},
						ProposedTS:      cts20,
						Term:            5,
						Sequence:        8,
						AcquisitionType: roachpb.LeaseAcquisitionType_Request,
						MinExpiration:   ts40,
					},
				},
			},
			{
				name: "acquire leader lease, as raft follower",
				st:   useLeaderSettings(),
				input: func() BuildInput {
					i := defaultInput
					i.RaftStatus = raftStatusFollower(repl1.ReplicaID)
					return i
				}(),
				// The replica is a follower, so it gets an expiration-based lease.
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:               repl1,
						Start:                 cts20,
						ProposedTS:            cts20,
						Expiration:            &ts40,
						DeprecatedStartStasis: &ts40,
						Sequence:              8,
						AcquisitionType:       roachpb.LeaseAcquisitionType_Request,
					},
				},
			},
			{
				name: "acquire leader lease, as raft leader, during leadership transfer",
				st:   useLeaderSettings(),
				input: func() BuildInput {
					i := defaultInput
					i.RaftStatus = raftStatusLeaderDuringTransfer(repl1.ReplicaID)
					return i
				}(),
				// The replica is a leader that is transferring leadership away, so it
				// gets an expiration-based lease.
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:               repl1,
						Start:                 cts20,
						ProposedTS:            cts20,
						Expiration:            &ts40,
						DeprecatedStartStasis: &ts40,
						Sequence:              8,
						AcquisitionType:       roachpb.LeaseAcquisitionType_Request,
					},
				},
			},
			{
				name: "replace expiration, acquire leader lease, as raft follower",
				st:   useLeaderSettings(),
				input: func() BuildInput {
					i := expirationInput
					i.RaftStatus = raftStatusFollower(repl1.ReplicaID)
					return i
				}(),
				// The replica is a follower, so it gets an expiration-based lease.
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica: repl1,
						// Start time backdated to the expiration of the previous lease.
						Start:                 hlc.ClockTimestamp{WallTime: 10, Logical: 1},
						ProposedTS:            cts20,
						Expiration:            &ts40,
						DeprecatedStartStasis: &ts40,
						Sequence:              8,
						AcquisitionType:       roachpb.LeaseAcquisitionType_Request,
					},
				},
			},
			{
				name: "replace expiration, acquire leader lease, as raft leader, during leadership transfer",
				st:   useLeaderSettings(),
				input: func() BuildInput {
					i := expirationInput
					i.RaftStatus = raftStatusLeaderDuringTransfer(repl1.ReplicaID)
					return i
				}(),
				// The replica is a leader that is transferring leadership away, so it
				// gets an expiration-based lease.
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica: repl1,
						// Start time backdated to the expiration of the previous lease.
						Start:                 hlc.ClockTimestamp{WallTime: 10, Logical: 1},
						ProposedTS:            cts20,
						Expiration:            &ts40,
						DeprecatedStartStasis: &ts40,
						Sequence:              8,
						AcquisitionType:       roachpb.LeaseAcquisitionType_Request,
					},
				},
			},
			{
				name:   "missing node liveness",
				nl:     missingNodeLiveness(),
				input:  defaultInput,
				expErr: "liveness record not found in cache",
			},
		})
	})

	t.Run("extension", func(t *testing.T) {
		defaultInput := BuildInput{
			LocalStoreID: repl1.StoreID,
			Now:          cts20,
			RaftStatus:   raftStatusLeader(repl1.ReplicaID),
			PrevLease: roachpb.Lease{
				Replica:  repl1,
				Start:    cts10,
				Epoch:    3,
				Sequence: 7,
			},
			PrevLeaseNodeLiveness: defaultNodeLivenessRecord(repl1.NodeID).Liveness,
			NextLeaseHolder:       repl1,
		}
		expirationInput := func() BuildInput {
			i := defaultInput
			i.PrevLease.Expiration = &ts30
			i.PrevLease.Epoch = 0
			i.PrevLeaseNodeLiveness = livenesspb.Liveness{}
			return i
		}()
		leaderInput := func() BuildInput {
			i := defaultInput
			i.PrevLease.Term = 5
			i.PrevLease.MinExpiration = ts30
			i.PrevLease.Epoch = 0
			i.PrevLeaseNodeLiveness = livenesspb.Liveness{}
			return i
		}()

		runTests(t, []testCase{
			{
				name:  "basic",
				input: defaultInput,
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:         repl1,
						Start:           cts10,
						ProposedTS:      cts20,
						Epoch:           3,
						Sequence:        7, // sequence not changed
						AcquisitionType: roachpb.LeaseAcquisitionType_Request,
					},
				},
			},
			{
				name: "basic, previous expired",
				input: func() BuildInput {
					i := defaultInput
					i.Now = cts30
					i.PrevLeaseExpired = true
					return i
				}(),
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:         repl1,
						Start:           cts10,
						ProposedTS:      cts30,
						Epoch:           3,
						Sequence:        7, // sequence not changed
						AcquisitionType: roachpb.LeaseAcquisitionType_Request,
					},
					// Requires heartbeat of node liveness record.
					NodeLivenessManipulation: NodeLivenessManipulation{
						Heartbeat: &defaultNodeLivenessRecord(repl1.NodeID).Liveness,
					},
				},
			},
			{
				name:  "promote expiration to epoch",
				input: expirationInput,
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:         repl1,
						Start:           cts10,
						ProposedTS:      cts20,
						Epoch:           3,
						Sequence:        7, // sequence not changed
						AcquisitionType: roachpb.LeaseAcquisitionType_Request,
						MinExpiration:   ts30,
					},
				},
			},
			{
				name: "promote expiration to epoch, pre-24.1",
				st: func() Settings {
					st := defaultSettings()
					st.ExpToEpochEquiv = false
					st.MinExpirationSupported = false
					return st
				}(),
				input: expirationInput,
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:         repl1,
						Start:           cts10,
						ProposedTS:      cts20,
						Epoch:           3,
						Sequence:        8, // sequence changed
						AcquisitionType: roachpb.LeaseAcquisitionType_Request,
					},
				},
			},
			{
				name: "promote expiration to epoch, needs min_expiration",
				input: func() BuildInput {
					i := expirationInput
					i.PrevLease.Expiration = &ts40
					return i
				}(),
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:         repl1,
						Start:           cts10,
						ProposedTS:      cts20,
						Epoch:           3,
						Sequence:        7, // sequence not changed
						AcquisitionType: roachpb.LeaseAcquisitionType_Request,
						MinExpiration:   ts40,
					},
				},
			},
			{
				name: "promote expiration to epoch, needs liveness heartbeat, pre-24.2",
				st: func() Settings {
					st := defaultSettings()
					st.MinExpirationSupported = false
					return st
				}(),
				input: func() BuildInput {
					i := expirationInput
					i.PrevLease.Expiration = &ts40
					return i
				}(),
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:         repl1,
						Start:           cts10,
						ProposedTS:      cts20,
						Epoch:           3,
						Sequence:        7, // sequence not changed
						AcquisitionType: roachpb.LeaseAcquisitionType_Request,
					},
					// Requires heartbeat of node liveness record.
					NodeLivenessManipulation: NodeLivenessManipulation{
						Heartbeat:              &defaultNodeLivenessRecord(repl1.NodeID).Liveness,
						HeartbeatMinExpiration: ts40,
					},
				},
			},
			{
				name:  "promote expiration to leader lease",
				st:    useLeaderSettings(),
				input: expirationInput,
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:         repl1,
						Start:           cts10,
						ProposedTS:      cts20,
						Term:            5,
						Sequence:        7, // sequence not changed
						AcquisitionType: roachpb.LeaseAcquisitionType_Request,
						MinExpiration:   ts40,
					},
				},
			},
			{
				name: "promote expiration to leader lease, as raft follower",
				st:   useLeaderSettings(),
				input: func() BuildInput {
					i := expirationInput
					i.RaftStatus = raftStatusFollower(repl1.ReplicaID)
					return i
				}(),
				// The replica is a follower, so it gets an (extended) expiration-based
				// lease.
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:               repl1,
						Start:                 cts10,
						ProposedTS:            cts20,
						Expiration:            &ts40,
						DeprecatedStartStasis: &ts40,
						Sequence:              7, // sequence not changed
						AcquisitionType:       roachpb.LeaseAcquisitionType_Request,
					},
				},
			},
			{
				name: "promote expiration to leader lease, as raft leader, during leadership transfer",
				st:   useLeaderSettings(),
				input: func() BuildInput {
					i := expirationInput
					i.RaftStatus = raftStatusLeaderDuringTransfer(repl1.ReplicaID)
					return i
				}(),
				// The replica is a leader that is transferring leadership away, so it
				// gets an (extended) expiration-based lease.
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:               repl1,
						Start:                 cts10,
						ProposedTS:            cts20,
						Expiration:            &ts40,
						DeprecatedStartStasis: &ts40,
						Sequence:              7, // sequence not changed
						AcquisitionType:       roachpb.LeaseAcquisitionType_Request,
					},
				},
			},
			{
				name:  "extend expiration",
				st:    useExpirationSettings(),
				input: expirationInput,
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:               repl1,
						Start:                 cts10,
						ProposedTS:            cts20,
						Expiration:            &ts40,
						DeprecatedStartStasis: &ts40,
						Sequence:              7, // sequence not changed
						AcquisitionType:       roachpb.LeaseAcquisitionType_Request,
					},
				},
			},
			{
				name: "extend expiration, avoid shortening",
				st:   useExpirationSettings(),
				input: func() BuildInput {
					i := expirationInput
					i.PrevLease.Expiration = &ts50
					return i
				}(),
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:               repl1,
						Start:                 cts10,
						ProposedTS:            cts20,
						Expiration:            &ts50,
						DeprecatedStartStasis: &ts50,
						Sequence:              7, // sequence not changed
						AcquisitionType:       roachpb.LeaseAcquisitionType_Request,
					},
				},
			},
			{
				name:  "extend leader lease",
				st:    useLeaderSettings(),
				input: leaderInput,
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:         repl1,
						Start:           cts10,
						ProposedTS:      cts20,
						Term:            5,
						Sequence:        7, // sequence not changed
						AcquisitionType: roachpb.LeaseAcquisitionType_Request,
						MinExpiration:   ts40,
					},
				},
			},
			{
				name: "extend leader lease, avoid shortening",
				st:   useLeaderSettings(),
				input: func() BuildInput {
					i := leaderInput
					i.PrevLease.MinExpiration = ts50
					return i
				}(),
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:         repl1,
						Start:           cts10,
						ProposedTS:      cts20,
						Term:            5,
						Sequence:        7, // sequence not changed
						AcquisitionType: roachpb.LeaseAcquisitionType_Request,
						MinExpiration:   ts50,
					},
				},
			},
			{
				name: "extend leader lease, avoid shortening, as follower",
				st:   useLeaderSettings(),
				input: func() BuildInput {
					i := leaderInput
					i.PrevLease.MinExpiration = ts50
					i.RaftStatus = raftStatusFollower(repl1.ReplicaID)
					return i
				}(),
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:               repl1,
						Start:                 cts10,
						ProposedTS:            cts20,
						Expiration:            &ts50,
						DeprecatedStartStasis: &ts50,
						Sequence:              8, // sequence not changed
						AcquisitionType:       roachpb.LeaseAcquisitionType_Request,
					},
					PrevLeaseManipulation: PrevLeaseManipulation{
						RevokeAndForwardNextExpiration: true,
					},
				},
			},
			{
				name: "extend leader lease, avoid shortening, as follower with remaining lead support",
				st:   useLeaderSettings(),
				input: func() BuildInput {
					i := leaderInput
					i.PrevLease.MinExpiration = ts40
					// NOTE: even though the replica is a follower, it still has remaining
					// lead support from when it was the leader. It has stepped down and
					// is waiting out the remainder of its lead support before campaigning
					// at a later term.
					i.RaftStatus = raftStatusFollower(repl1.ReplicaID)
					i.RaftStatus.LeadSupportUntil = ts50
					return i
				}(),
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:               repl1,
						Start:                 cts10,
						ProposedTS:            cts20,
						Expiration:            &ts50,
						DeprecatedStartStasis: &ts50,
						Sequence:              8, // sequence not changed
						AcquisitionType:       roachpb.LeaseAcquisitionType_Request,
					},
					PrevLeaseManipulation: PrevLeaseManipulation{
						RevokeAndForwardNextExpiration: true,
					},
				},
			},
			{
				name:  "switch epoch to expiration",
				st:    useExpirationSettings(),
				input: defaultInput,
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:               repl1,
						Start:                 cts10,
						ProposedTS:            cts20,
						Expiration:            &ts40,
						DeprecatedStartStasis: &ts40,
						Sequence:              8, // sequence changed
						AcquisitionType:       roachpb.LeaseAcquisitionType_Request,
					},
					PrevLeaseManipulation: PrevLeaseManipulation{
						RevokeAndForwardNextExpiration: true,
					},
				},
			},
			{
				name:  "switch epoch to leader lease",
				st:    useLeaderSettings(),
				input: defaultInput,
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:         repl1,
						Start:           cts10,
						ProposedTS:      cts20,
						Term:            5,
						Sequence:        8, // sequence changed
						AcquisitionType: roachpb.LeaseAcquisitionType_Request,
						MinExpiration:   ts40,
					},
					PrevLeaseManipulation: PrevLeaseManipulation{
						RevokeAndForwardNextExpiration: true,
					},
				},
			},
			{
				name: "switch epoch to expiration, previous expired",
				st:   useExpirationSettings(),
				input: func() BuildInput {
					i := defaultInput
					i.Now = cts30
					i.PrevLeaseExpired = true
					return i
				}(),
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:               repl1,
						Start:                 cts10,
						ProposedTS:            cts30,
						Expiration:            &ts50,
						DeprecatedStartStasis: &ts50,
						Sequence:              8, // sequence changed
						AcquisitionType:       roachpb.LeaseAcquisitionType_Request,
					},
					NodeLivenessManipulation: NodeLivenessManipulation{
						Heartbeat: &defaultNodeLivenessRecord(repl1.NodeID).Liveness,
					},
				},
			},
			{
				name:  "switch leader lease to expiration",
				st:    useExpirationSettings(),
				input: leaderInput,
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:               repl1,
						Start:                 cts10,
						ProposedTS:            cts20,
						Expiration:            &ts40,
						DeprecatedStartStasis: &ts40,
						Sequence:              8, // sequence changed
						AcquisitionType:       roachpb.LeaseAcquisitionType_Request,
					},
					PrevLeaseManipulation: PrevLeaseManipulation{
						RevokeAndForwardNextExpiration: true,
					},
				},
			},
			{
				name:  "switch leader lease to epoch",
				input: leaderInput,
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:         repl1,
						Start:           cts10,
						ProposedTS:      cts20,
						Epoch:           3,
						Sequence:        8, // sequence changed
						AcquisitionType: roachpb.LeaseAcquisitionType_Request,
						MinExpiration:   hlc.Timestamp{}, // set after revoke
					},
					PrevLeaseManipulation: PrevLeaseManipulation{
						RevokeAndForwardNextExpiration: true,
					},
				},
			},
			{
				name: "switch leader lease to expiration, prev expired",
				st:   useExpirationSettings(),
				input: func() BuildInput {
					i := leaderInput
					i.Now = cts30
					i.PrevLeaseExpired = true
					return i
				}(),
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:               repl1,
						Start:                 cts10,
						ProposedTS:            cts30,
						Expiration:            &ts50,
						DeprecatedStartStasis: &ts50,
						Sequence:              8, // sequence changed
						AcquisitionType:       roachpb.LeaseAcquisitionType_Request,
					},
				},
			},
			{
				name: "min proposed timestamp",
				input: func() BuildInput {
					i := defaultInput
					i.MinLeaseProposedTS = hlc.ClockTimestamp{WallTime: 15}
					return i
				}(),
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:         repl1,
						Start:           hlc.ClockTimestamp{WallTime: 15}, // start forwarded to MinLeaseProposedTS
						ProposedTS:      cts20,
						Epoch:           3,
						Sequence:        8, // sequence changed as a result
						AcquisitionType: roachpb.LeaseAcquisitionType_Request,
					},
				},
			},
			{
				name:   "missing node liveness",
				nl:     missingNodeLiveness(),
				input:  defaultInput,
				expErr: "liveness record not found in cache",
			},
		})
	})

	t.Run("transfer", func(t *testing.T) {
		defaultInput := BuildInput{
			LocalStoreID: repl1.StoreID,
			Now:          cts20,
			RaftStatus:   raftStatusLeader(repl1.ReplicaID),
			PrevLease: roachpb.Lease{
				Replica:  repl1,
				Start:    cts10,
				Epoch:    3,
				Sequence: 7,
			},
			PrevLeaseNodeLiveness: defaultNodeLivenessRecord(repl1.NodeID).Liveness,
			NextLeaseHolder:       repl2,
		}
		expirationInput := func() BuildInput {
			i := defaultInput
			i.PrevLease.Expiration = &ts30
			i.PrevLease.Epoch = 0
			i.PrevLeaseNodeLiveness = livenesspb.Liveness{}
			return i
		}()
		leaderInput := func() BuildInput {
			i := defaultInput
			i.PrevLease.Term = 5
			i.PrevLease.MinExpiration = ts30
			i.PrevLease.Epoch = 0
			i.PrevLeaseNodeLiveness = livenesspb.Liveness{}
			return i
		}()

		runTests(t, []testCase{
			{
				name:  "basic",
				input: defaultInput,
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:         repl2,
						Start:           cts20,
						ProposedTS:      cts20,
						Expiration:      &ts40, // converted to expiration-based lease
						Sequence:        8,
						AcquisitionType: roachpb.LeaseAcquisitionType_Transfer,
					},
					PrevLeaseManipulation: PrevLeaseManipulation{
						RevokeAndForwardNextStart: true,
					},
				},
			},
			{
				name:  "transfer from expiration",
				input: expirationInput,
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:         repl2,
						Start:           cts20,
						ProposedTS:      cts20,
						Expiration:      &ts40,
						Sequence:        8,
						AcquisitionType: roachpb.LeaseAcquisitionType_Transfer,
					},
					PrevLeaseManipulation: PrevLeaseManipulation{
						RevokeAndForwardNextStart: true,
					},
				},
			},
			{
				name:  "transfer from leader lease",
				input: leaderInput,
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:         repl2,
						Start:           cts20,
						ProposedTS:      cts20,
						Expiration:      &ts40,
						Sequence:        8,
						AcquisitionType: roachpb.LeaseAcquisitionType_Transfer,
					},
					PrevLeaseManipulation: PrevLeaseManipulation{
						RevokeAndForwardNextStart: true,
					},
				},
			},
			{
				name: "transfer to epoch",
				st: func() Settings {
					st := defaultSettings()
					st.TransferExpirationLeases = false
					return st
				}(),
				input: expirationInput,
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:         repl2,
						Start:           cts20,
						ProposedTS:      cts20,
						Epoch:           3,
						Sequence:        8,
						AcquisitionType: roachpb.LeaseAcquisitionType_Transfer,
					},
					PrevLeaseManipulation: PrevLeaseManipulation{
						RevokeAndForwardNextStart: true,
					},
				},
			},
			{
				name: "transfer to epoch, missing node liveness",
				st: func() Settings {
					st := defaultSettings()
					st.TransferExpirationLeases = false
					return st
				}(),
				nl:     missingNodeLiveness(),
				input:  expirationInput,
				expErr: "liveness record not found in cache",
			},
			{
				name: "cannot transfer to leader lease",
				st: func() Settings {
					st := defaultSettings()
					st.TransferExpirationLeases = false
					st.PreferLeaderLeasesOverEpochLeases = true
					return st
				}(),
				input: defaultInput,
				// PreferLeaderLeasesOverEpochLeases takes precedence over
				// TransferExpirationLeases. We never transfer to a leader lease.
				expOutput: Output{
					NextLease: roachpb.Lease{
						Replica:         repl2,
						Start:           cts20,
						ProposedTS:      cts20,
						Expiration:      &ts40,
						Sequence:        8,
						AcquisitionType: roachpb.LeaseAcquisitionType_Transfer,
					},
					PrevLeaseManipulation: PrevLeaseManipulation{
						RevokeAndForwardNextStart: true,
					},
				},
			},
		})
	})
}

func TestInputToVerifyInput(t *testing.T) {
	cts := hlc.ClockTimestamp{WallTime: 1, Logical: 1}
	ts := cts.ToTimestamp()
	noZeroBuildInput := BuildInput{
		LocalStoreID:   1,
		LocalReplicaID: 1,
		Desc:           &roachpb.RangeDescriptor{},
		RaftStatus:     &raft.Status{},
		RaftFirstIndex: 1,
		PrevLease: roachpb.Lease{
			Start:      cts,
			Expiration: &ts,
			Replica: roachpb.ReplicaDescriptor{
				NodeID: 1, StoreID: 1, ReplicaID: 1, Type: 1,
			},
			DeprecatedStartStasis: &ts,
			ProposedTS:            cts,
			Epoch:                 1,
			Sequence:              1,
			AcquisitionType:       1,
			MinExpiration:         ts,
			Term:                  1,
		},
		PrevLeaseExpired: true,
		NextLeaseHolder: roachpb.ReplicaDescriptor{
			NodeID: 1, StoreID: 1, ReplicaID: 1, Type: 1,
		},
		BypassSafetyChecks: true,
		DesiredLeaseType:   roachpb.LeaseLeader,
	}
	verifyInput := noZeroBuildInput.toVerifyInput()
	require.NoError(t, zerofields.NoZeroField(verifyInput),
		"make sure you update BuildInput.toVerifyInput for the new field")
}
