// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package leases

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

var (
	repl1 = roachpb.ReplicaDescriptor{NodeID: 1, StoreID: 1, ReplicaID: 1}
	repl2 = roachpb.ReplicaDescriptor{NodeID: 2, StoreID: 2, ReplicaID: 2}
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
			name: "remote transfer",
			input: BuildInput{
				LocalStoreID:    repl1.StoreID,
				Now:             cts20,
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
				PrevLease:        roachpb.Lease{Replica: repl2, Expiration: &ts30},
				PrevLeaseExpired: false,
				NextLeaseHolder:  repl1,
			},
			expErr: "cannot acquire lease from another node before it has expired",
		},
		{
			name: "valid acquisition",
			input: BuildInput{
				LocalStoreID:     repl1.StoreID,
				Now:              cts20,
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
		UseExpirationLeases:      false,
		TransferExpirationLeases: true,
		ExpToEpochEquiv:          true,
		MinExpirationSupported:   true,
		RangeLeaseDuration:       20,
	}
}

func useExpirationSettings() Settings {
	st := defaultSettings()
	st.UseExpirationLeases = true
	return st
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
				out, err := Build(st, nl, tt.input)
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
		})
	})
}
