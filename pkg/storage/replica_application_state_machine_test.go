// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/raftpb"
)

// TestReplicaStateMachineStageChangeReplicas tests the behavior of staging a
// replicated command with a ChangeReplicas trigger in a replicaAppBatch. The
// test exercises the logic of applying both old-style and new-style
// ChangeReplicas triggers.
func TestReplicaStateMachineStageChangeReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	// Lock the replica for the entire test.
	r := tc.repl
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	sm := r.getStateMachine()

	desc := r.Desc()
	replDesc, ok := desc.GetReplicaDescriptor(r.store.StoreID())
	require.True(t, ok)

	makeCmd := func(desc *roachpb.RangeDescriptor, t roachpb.ChangeReplicasTrigger) *replicatedCmd {
		return &replicatedCmd{
			ctx: context.Background(),
			ent: &raftpb.Entry{Index: r.mu.state.RaftAppliedIndex + 1},
			decodedRaftEntry: decodedRaftEntry{
				idKey: makeIDKey(),
				raftCmd: storagepb.RaftCommand{
					ProposerLeaseSequence: r.mu.state.Lease.Sequence,
					MaxLeaseIndex:         r.mu.state.LeaseAppliedIndex + 1,
					ReplicatedEvalResult: storagepb.ReplicatedEvalResult{
						State:          &storagepb.ReplicaState{Desc: desc},
						ChangeReplicas: &storagepb.ChangeReplicas{ChangeReplicasTrigger: t},
						Timestamp:      r.mu.state.GCThreshold.Add(1, 0),
					},
				},
			},
		}
	}

	t.Run("add replica", func(t *testing.T) {
		// Add a new replica to the Range.
		newDesc := *desc
		addedReplDesc := newDesc.AddReplica(replDesc.NodeID+1, replDesc.StoreID+1, roachpb.VOTER_FULL)

		testutils.RunTrueAndFalse(t, "deprecated", func(t *testing.T, deprecated bool) {
			var trigger roachpb.ChangeReplicasTrigger
			if deprecated {
				trigger = roachpb.ChangeReplicasTrigger{
					DeprecatedChangeType: roachpb.ADD_REPLICA,
					DeprecatedReplica:    addedReplDesc,
					DeprecatedUpdatedReplicas: []roachpb.ReplicaDescriptor{
						replDesc,
						addedReplDesc,
					},
					DeprecatedNextReplicaID: addedReplDesc.ReplicaID + 1,
				}
			} else {
				trigger = roachpb.ChangeReplicasTrigger{
					Desc:                  &newDesc,
					InternalAddedReplicas: []roachpb.ReplicaDescriptor{addedReplDesc},
				}
			}

			// Create a new application batch.
			b := sm.NewBatch(false /* ephemeral */).(*replicaAppBatch)
			defer b.Close()

			// Stage a command with the ChangeReplicas trigger.
			cmd := makeCmd(&newDesc, trigger)
			_, err := b.Stage(cmd)
			require.NoError(t, err)
			require.False(t, b.changeRemovesReplica)
			require.Equal(t, b.state.RaftAppliedIndex, cmd.ent.Index)
			require.Equal(t, b.state.LeaseAppliedIndex, cmd.raftCmd.MaxLeaseIndex)

			// Check the replica's destroy status.
			r.mu.Lock()
			require.False(t, r.mu.destroyStatus.Removed())
			r.mu.Unlock()
		})
	})

	t.Run("remove replica", func(t *testing.T) {
		// Remove ourselves from the Range.
		newDesc := *desc
		removedReplDesc, ok := newDesc.RemoveReplica(replDesc.NodeID, replDesc.StoreID)
		require.True(t, ok)

		testutils.RunTrueAndFalse(t, "deprecated", func(t *testing.T, deprecated bool) {
			var trigger roachpb.ChangeReplicasTrigger
			if deprecated {
				trigger = roachpb.ChangeReplicasTrigger{
					DeprecatedChangeType:      roachpb.REMOVE_REPLICA,
					DeprecatedReplica:         removedReplDesc,
					DeprecatedUpdatedReplicas: []roachpb.ReplicaDescriptor{},
					DeprecatedNextReplicaID:   replDesc.ReplicaID + 1,
				}
			} else {
				trigger = roachpb.ChangeReplicasTrigger{
					Desc:                    &newDesc,
					InternalRemovedReplicas: []roachpb.ReplicaDescriptor{removedReplDesc},
				}
			}

			// Create a new application batch.
			b := sm.NewBatch(false /* ephemeral */).(*replicaAppBatch)
			defer b.Close()

			// Stage a command with the ChangeReplicas trigger.
			cmd := makeCmd(&newDesc, trigger)
			_, err := b.Stage(cmd)
			require.NoError(t, err)
			require.True(t, b.changeRemovesReplica)
			require.Equal(t, b.state.RaftAppliedIndex, cmd.ent.Index)
			require.Equal(t, b.state.LeaseAppliedIndex, cmd.raftCmd.MaxLeaseIndex)

			// Check the replica's destroy status.
			r.mu.Lock()
			require.True(t, r.mu.destroyStatus.Removed())
			r.mu.destroyStatus.Set(nil, destroyReasonAlive) // reset
			r.mu.Unlock()
		})
	})
}
