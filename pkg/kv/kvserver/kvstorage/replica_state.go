// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvstorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
	"go.etcd.io/raft/v3/raftpb"
)

// LoadedReplicaState represents the state of a Replica loaded from storage, and
// is used to initialize the in-memory Replica instance.
// TODO(pavelkalinnikov): integrate with kvstorage.Replica.
type LoadedReplicaState struct {
	ReplicaID roachpb.ReplicaID
	LastIndex uint64
	ReplState kvserverpb.ReplicaState

	hardState raftpb.HardState
}

// LoadReplicaState loads the state necessary to create a Replica with the
// specified range descriptor, which can be either initialized or uninitialized.
// TODO(pavelkalinnikov): integrate with stateloader.
func LoadReplicaState(
	ctx context.Context,
	eng storage.Reader,
	desc *roachpb.RangeDescriptor,
	replicaID roachpb.ReplicaID,
) (LoadedReplicaState, error) {
	sl := stateloader.Make(desc.RangeID)
	id, found, err := sl.LoadRaftReplicaID(ctx, eng)
	if err != nil {
		return LoadedReplicaState{}, err
	} else if !found {
		return LoadedReplicaState{}, errors.AssertionFailedf(
			"r%d: RaftReplicaID not found", desc.RangeID)
	} else if loaded := id.ReplicaID; loaded != replicaID {
		return LoadedReplicaState{}, errors.AssertionFailedf(
			"r%d: loaded RaftReplicaID %d does not match %d", desc.RangeID, loaded, replicaID)
	}

	ls := LoadedReplicaState{ReplicaID: replicaID}
	if ls.hardState, err = sl.LoadHardState(ctx, eng); err != nil {
		return LoadedReplicaState{}, err
	}
	if ls.LastIndex, err = sl.LoadLastIndex(ctx, eng); err != nil {
		return LoadedReplicaState{}, err
	}
	if ls.ReplState, err = sl.Load(ctx, eng, desc); err != nil {
		return LoadedReplicaState{}, err
	}
	return ls, nil
}

// Check makes sure that the replica invariants hold for the loaded state.
func (r LoadedReplicaState) Check(storeID roachpb.StoreID) error {
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
