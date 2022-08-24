// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/apply"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// TestReplicaStateMachineChangeReplicas tests the behavior of applying a
// replicated command with a ChangeReplicas trigger in a replicaAppBatch.
// The test exercises the logic of applying both old-style and new-style
// ChangeReplicas triggers.
func TestReplicaStateMachineChangeReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testutils.RunTrueAndFalse(t, "add replica", func(t *testing.T, add bool) {
		tc := testContext{}
		ctx := context.Background()
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		tc.Start(ctx, t, stopper)

		// Lock the replica for the entire test.
		r := tc.repl
		r.raftMu.Lock()
		defer r.raftMu.Unlock()
		sm := r.getStateMachine()

		desc := r.Desc()
		replDesc, ok := desc.GetReplicaDescriptor(r.store.StoreID())
		require.True(t, ok)

		newDesc := *desc
		newDesc.InternalReplicas = append([]roachpb.ReplicaDescriptor(nil), desc.InternalReplicas...)
		var trigger roachpb.ChangeReplicasTrigger
		var confChange raftpb.ConfChange
		if add {
			// Add a new replica to the Range.
			addedReplDesc := newDesc.AddReplica(replDesc.NodeID+1, replDesc.StoreID+1, roachpb.VOTER_FULL)

			trigger = roachpb.ChangeReplicasTrigger{
				Desc:                  &newDesc,
				InternalAddedReplicas: []roachpb.ReplicaDescriptor{addedReplDesc},
			}

			confChange = raftpb.ConfChange{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: uint64(addedReplDesc.ReplicaID),
			}
		} else {
			// Remove ourselves from the Range.
			removedReplDesc, ok := newDesc.RemoveReplica(replDesc.NodeID, replDesc.StoreID)
			require.True(t, ok)

			trigger = roachpb.ChangeReplicasTrigger{
				Desc:                    &newDesc,
				InternalRemovedReplicas: []roachpb.ReplicaDescriptor{removedReplDesc},
			}

			confChange = raftpb.ConfChange{
				Type:   raftpb.ConfChangeRemoveNode,
				NodeID: uint64(removedReplDesc.ReplicaID),
			}
		}

		// Create a new application batch.
		b := sm.NewBatch(false /* ephemeral */).(*replicaAppBatch)
		defer b.Close()

		// Stage a command with the ChangeReplicas trigger.
		cmd := &replicatedCmd{
			ctx: ctx,
			ent: &raftpb.Entry{
				Index: r.mu.state.RaftAppliedIndex + 1,
				Type:  raftpb.EntryConfChange,
			},
			decodedRaftEntry: decodedRaftEntry{
				idKey: makeIDKey(),
				raftCmd: kvserverpb.RaftCommand{
					ProposerLeaseSequence: r.mu.state.Lease.Sequence,
					MaxLeaseIndex:         r.mu.state.LeaseAppliedIndex + 1,
					ReplicatedEvalResult: kvserverpb.ReplicatedEvalResult{
						State:          &kvserverpb.ReplicaState{Desc: &newDesc},
						ChangeReplicas: &kvserverpb.ChangeReplicas{ChangeReplicasTrigger: trigger},
						WriteTimestamp: r.mu.state.GCThreshold.Add(1, 0),
					},
				},
				confChange: &decodedConfChange{
					ConfChangeI: confChange,
				},
			},
		}

		checkedCmd, err := b.Stage(cmd.ctx, cmd)
		require.NoError(t, err)
		require.Equal(t, !add, b.changeRemovesReplica)
		require.Equal(t, b.state.RaftAppliedIndex, cmd.ent.Index)
		require.Equal(t, b.state.LeaseAppliedIndex, cmd.raftCmd.MaxLeaseIndex)

		// Check the replica's destroy status.
		reason, _ := r.IsDestroyed()
		if add {
			require.Equal(t, destroyReasonAlive, reason)
		} else {
			require.Equal(t, destroyReasonRemoved, reason)
		}

		// Apply the batch to the StateMachine.
		err = b.ApplyToStateMachine(ctx)
		require.NoError(t, err)

		// Apply the side effects of the command to the StateMachine.
		_, err = sm.ApplySideEffects(checkedCmd.Ctx(), checkedCmd)
		if add {
			require.NoError(t, err)
		} else {
			require.Equal(t, apply.ErrRemoved, err)
		}

		// Check whether the Replica still exists in the Store.
		_, err = tc.store.GetReplica(r.RangeID)
		if add {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
			require.IsType(t, &roachpb.RangeNotFoundError{}, err)
		}
	})
}

func TestReplicaStateMachineRaftLogTruncationStronglyCoupled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testutils.RunTrueAndFalse(t, "accurate first index", func(t *testing.T, accurate bool) {
		tc := testContext{}
		ctx := context.Background()
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		tc.Start(ctx, t, stopper)
		st := tc.store.ClusterSettings()
		looselyCoupledTruncationEnabled.Override(ctx, &st.SV, false)

		// Lock the replica for the entire test.
		r := tc.repl
		r.raftMu.Lock()
		defer r.raftMu.Unlock()
		sm := r.getStateMachine()

		// Create a new application batch.
		b := sm.NewBatch(false /* ephemeral */).(*replicaAppBatch)
		defer b.Close()

		r.mu.Lock()
		raftAppliedIndex := r.mu.state.RaftAppliedIndex
		truncatedIndex := r.mu.state.TruncatedState.Index
		raftLogSize := r.mu.raftLogSize
		// Overwrite to be trusted, since we want to check if transitions to false
		// or not.
		r.mu.raftLogSizeTrusted = true
		r.mu.Unlock()

		expectedFirstIndex := truncatedIndex + 1
		if !accurate {
			expectedFirstIndex = truncatedIndex
		}
		// Stage a command that truncates one raft log entry which we pretend has a
		// byte size of 1.
		cmd := &replicatedCmd{
			ctx: ctx,
			ent: &raftpb.Entry{
				Index: raftAppliedIndex + 1,
				Type:  raftpb.EntryNormal,
			},
			decodedRaftEntry: decodedRaftEntry{
				idKey: makeIDKey(),
				raftCmd: kvserverpb.RaftCommand{
					ProposerLeaseSequence: r.mu.state.Lease.Sequence,
					MaxLeaseIndex:         r.mu.state.LeaseAppliedIndex + 1,
					ReplicatedEvalResult: kvserverpb.ReplicatedEvalResult{
						State: &kvserverpb.ReplicaState{
							TruncatedState: &roachpb.RaftTruncatedState{
								Index: truncatedIndex + 1,
							},
						},
						RaftLogDelta:           -1,
						RaftExpectedFirstIndex: expectedFirstIndex,
						WriteTimestamp:         r.mu.state.GCThreshold.Add(1, 0),
					},
				},
			},
		}

		checkedCmd, err := b.Stage(cmd.ctx, cmd)
		require.NoError(t, err)

		// Apply the batch to the StateMachine.
		err = b.ApplyToStateMachine(ctx)
		require.NoError(t, err)

		// Apply the side effects of the command to the StateMachine.
		_, err = sm.ApplySideEffects(checkedCmd.Ctx(), checkedCmd)
		require.NoError(t, err)
		func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			require.Equal(t, raftAppliedIndex+1, r.mu.state.RaftAppliedIndex)
			require.Equal(t, truncatedIndex+1, r.mu.state.TruncatedState.Index)
			expectedSize := raftLogSize - 1
			// We typically have a raftLogSize > 0 (based on inspecting some test
			// runs), but we can't be sure.
			if expectedSize < 0 {
				expectedSize = 0
			}
			require.Equal(t, expectedSize, r.mu.raftLogSize)
			require.Equal(t, accurate, r.mu.raftLogSizeTrusted)
			truncState, err := r.mu.stateLoader.LoadRaftTruncatedState(context.Background(), tc.engine)
			require.NoError(t, err)
			require.Equal(t, r.mu.state.TruncatedState.Index, truncState.Index)
		}()
	})
}

func TestReplicaStateMachineRaftLogTruncationLooselyCoupled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testutils.RunTrueAndFalse(t, "accurate first index", func(t *testing.T, accurate bool) {
		tc := testContext{}
		ctx := context.Background()
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		tc.Start(ctx, t, stopper)
		st := tc.store.ClusterSettings()
		looselyCoupledTruncationEnabled.Override(ctx, &st.SV, true)
		// Remove the flush completed callback since we don't want a
		// non-deterministic flush to cause the test to fail.
		tc.store.engine.RegisterFlushCompletedCallback(func() {})
		r := tc.repl
		r.mu.Lock()
		raftAppliedIndex := r.mu.state.RaftAppliedIndex
		truncatedIndex := r.mu.state.TruncatedState.Index
		raftLogSize := r.mu.raftLogSize
		// Overwrite to be trusted, since we want to check if transitions to false
		// or not.
		r.mu.raftLogSizeTrusted = true
		r.mu.Unlock()
		expectedFirstIndex := truncatedIndex + 1
		if !accurate {
			expectedFirstIndex = truncatedIndex
		}

		// Enqueue the truncation.
		func() {
			// Lock the replica.
			r.raftMu.Lock()
			defer r.raftMu.Unlock()
			sm := r.getStateMachine()

			// Create a new application batch.
			b := sm.NewBatch(false /* ephemeral */).(*replicaAppBatch)
			defer b.Close()
			// Stage a command that truncates one raft log entry which we pretend has a
			// byte size of 1.
			cmd := &replicatedCmd{
				ctx: ctx,
				ent: &raftpb.Entry{
					Index: raftAppliedIndex + 1,
					Type:  raftpb.EntryNormal,
				},
				decodedRaftEntry: decodedRaftEntry{
					idKey: makeIDKey(),
					raftCmd: kvserverpb.RaftCommand{
						ProposerLeaseSequence: r.mu.state.Lease.Sequence,
						MaxLeaseIndex:         r.mu.state.LeaseAppliedIndex + 1,
						ReplicatedEvalResult: kvserverpb.ReplicatedEvalResult{
							State: &kvserverpb.ReplicaState{
								TruncatedState: &roachpb.RaftTruncatedState{
									Index: truncatedIndex + 1,
								},
							},
							RaftLogDelta:           -1,
							RaftExpectedFirstIndex: expectedFirstIndex,
							WriteTimestamp:         r.mu.state.GCThreshold.Add(1, 0),
						},
					},
				},
			}

			checkedCmd, err := b.Stage(cmd.ctx, cmd)
			require.NoError(t, err)

			// Apply the batch to the StateMachine.
			err = b.ApplyToStateMachine(ctx)
			require.NoError(t, err)

			// Apply the side effects of the command to the StateMachine.
			_, err = sm.ApplySideEffects(checkedCmd.Ctx(), checkedCmd)
			require.NoError(t, err)
			func() {
				r.mu.Lock()
				defer r.mu.Unlock()
				require.Equal(t, raftAppliedIndex+1, r.mu.state.RaftAppliedIndex)
				// No truncation.
				require.Equal(t, truncatedIndex, r.mu.state.TruncatedState.Index)
				require.True(t, r.mu.raftLogSizeTrusted)
			}()
			require.False(t, r.pendingLogTruncations.isEmptyLocked())
			trunc := r.pendingLogTruncations.frontLocked()
			require.Equal(t, truncatedIndex+1, trunc.Index)
			require.Equal(t, expectedFirstIndex, trunc.expectedFirstIndex)
			require.EqualValues(t, -1, trunc.logDeltaBytes)
			require.True(t, trunc.isDeltaTrusted)
		}()
		require.NoError(t, tc.store.Engine().Flush())
		// Asynchronous call to advance durability.
		tc.store.raftTruncator.durabilityAdvancedCallback()
		expectedSize := raftLogSize - 1
		// We typically have a raftLogSize > 0 (based on inspecting some test
		// runs), but we can't be sure.
		if expectedSize < 0 {
			expectedSize = 0
		}
		// Wait until async truncation is done.
		testutils.SucceedsSoon(t, func() error {
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.mu.state.TruncatedState.Index != truncatedIndex+1 {
				return errors.Errorf("not truncated")
			}
			if r.mu.raftLogSize != expectedSize {
				return errors.Errorf("not truncated")
			}
			if accurate != r.mu.raftLogSizeTrusted {
				return errors.Errorf("not truncated")
			}
			r.pendingLogTruncations.mu.Lock()
			defer r.pendingLogTruncations.mu.Unlock()
			if !r.pendingLogTruncations.isEmptyLocked() {
				return errors.Errorf("not truncated")
			}
			return nil
		})
	})
}
