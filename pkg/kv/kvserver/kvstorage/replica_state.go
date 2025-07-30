// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
)

// LoadedReplicaState represents the state of a Replica loaded from storage, and
// is used to initialize the in-memory Replica instance.
// TODO(pavelkalinnikov): integrate with kvstorage.Replica.
type LoadedReplicaState struct {
	ReplicaID   roachpb.ReplicaID
	LastEntryID logstore.EntryID
	ReplState   kvserverpb.ReplicaState
	TruncState  kvserverpb.RaftTruncatedState

	hardState raftpb.HardState
}

// LoadReplicaState loads the state necessary to create a Replica with the
// specified range descriptor, which can be either initialized or uninitialized.
// It also verifies replica state invariants.
// TODO(pavelkalinnikov): integrate with stateloader.
func LoadReplicaState(
	ctx context.Context,
	eng storage.Reader,
	storeID roachpb.StoreID,
	desc *roachpb.RangeDescriptor,
	replicaID roachpb.ReplicaID,
) (LoadedReplicaState, error) {
	sl := stateloader.Make(desc.RangeID)
	id, err := sl.LoadRaftReplicaID(ctx, eng)
	if err != nil {
		return LoadedReplicaState{}, err
	}
	if loaded := id.ReplicaID; loaded != replicaID {
		return LoadedReplicaState{}, errors.AssertionFailedf(
			"r%d: loaded RaftReplicaID %d does not match %d", desc.RangeID, loaded, replicaID)
	}

	ls := LoadedReplicaState{ReplicaID: replicaID}
	if ls.hardState, err = sl.LoadHardState(ctx, eng); err != nil {
		return LoadedReplicaState{}, err
	}
	if ls.TruncState, err = sl.LoadRaftTruncatedState(ctx, eng); err != nil {
		return LoadedReplicaState{}, err
	}
	if ls.LastEntryID, err = sl.LoadLastEntryID(ctx, eng, ls.TruncState); err != nil {
		return LoadedReplicaState{}, err
	}
	if ls.ReplState, err = sl.Load(ctx, eng, desc); err != nil {
		return LoadedReplicaState{}, err
	}

	if err := ls.check(storeID); err != nil {
		return LoadedReplicaState{}, err
	}
	return ls, nil
}

func (r LoadedReplicaState) FullReplicaID() roachpb.FullReplicaID {
	return roachpb.FullReplicaID{RangeID: r.ReplState.Desc.RangeID, ReplicaID: r.ReplicaID}
}

// check makes sure that the replica invariants hold for the loaded state.
func (r LoadedReplicaState) check(storeID roachpb.StoreID) error {
	desc := r.ReplState.Desc
	if r.ReplicaID == 0 {
		return errors.AssertionFailedf("r%d: replicaID is 0", desc.RangeID)
	}

	if !desc.IsInitialized() {
		// An uninitialized replica must have an empty HardState.Commit at all
		// times. Failure to maintain this invariant indicates corruption. And yet,
		// we have observed this in the wild. See #40213.
		if hs := r.hardState; hs.Commit != 0 {
			return errors.AssertionFailedf(
				"r%d/%d: non-zero HardState.Commit on uninitialized replica: %+v", desc.RangeID, r.ReplicaID, hs)
		}
		// TODO(pavelkalinnikov): assert r.lastIndex == 0?
		return nil
	}
	// desc.IsInitialized() == true

	// INVARIANT: a replica's RangeDescriptor always contains the local Store.
	if replDesc, ok := desc.GetReplicaDescriptor(storeID); !ok {
		return errors.AssertionFailedf("%+v does not contain local store s%d", desc, storeID)
	} else if replDesc.ReplicaID != r.ReplicaID {
		return errors.AssertionFailedf(
			"%+v does not contain replicaID %d for local store s%d", desc, r.ReplicaID, storeID)
	}
	return nil
}

// CreateUninitReplicaTODO is the plan for splitting CreateUninitializedReplica
// into cross-engine writes.
//
//  1. Log storage write (durable):
//     1.1. Write WAG node with the state machine mutation (2).
//  2. State machine mutation:
//     2.1. Write the new RaftReplicaID.
//
// TODO(sep-raft-log): support the status quo in which only 2.1 is written.
const CreateUninitReplicaTODO = 0

// CreateUninitializedReplica creates an uninitialized replica in storage.
// Returns kvpb.RaftGroupDeletedError if this replica can not be created
// because it has been deleted.
func CreateUninitializedReplica(
	ctx context.Context, eng storage.Engine, storeID roachpb.StoreID, id roachpb.FullReplicaID,
) error {
	sl := stateloader.Make(id.RangeID)
	// Before creating the replica, see if there is a tombstone which would
	// indicate that this replica has been removed.
	// TODO(pav-kv): should also check that there is no existing replica, i.e.
	// ReplicaID load should find nothing.
	if ts, err := sl.LoadRangeTombstone(ctx, eng); err != nil {
		return err
	} else if id.ReplicaID < ts.NextReplicaID {
		return &kvpb.RaftGroupDeletedError{}
	}

	// Write the RaftReplicaID for this replica. This is the only place in the
	// CockroachDB code that we are creating a new *uninitialized* replica.
	//
	// Before this point, raft and state machine state of this replica are
	// non-existent. The only RangeID-specific key that can be present is the
	// RangeTombstone inspected above.
	_ = CreateUninitReplicaTODO
	if err := sl.SetRaftReplicaID(ctx, eng, id.ReplicaID); err != nil {
		return err
	}

	// Make sure that storage invariants for this uninitialized replica hold.
	uninitDesc := roachpb.RangeDescriptor{RangeID: id.RangeID}
	_, err := LoadReplicaState(ctx, eng, storeID, &uninitDesc, id.ReplicaID)
	return err
}
