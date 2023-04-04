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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"go.etcd.io/raft/v3/raftpb"
)

// LoadedReplicaState represents the state of a Replica loaded from storage, and
// is used to initialize the in-memory Replica instance.
// TODO(pavelkalinnikov): integrate with kvstorage.Replica.
type LoadedReplicaState struct {
	ReplicaID roachpb.ReplicaID
	LastIndex kvpb.RaftIndex
	ReplState kvserverpb.ReplicaState

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
	if ls.LastIndex, err = sl.LoadLastIndex(ctx, eng); err != nil {
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

// CreateUninitializedReplica creates an uninitialized replica in storage.
// Returns kvpb.RaftGroupDeletedError if this replica can not be created
// because it has been deleted.
func CreateUninitializedReplica(
	ctx context.Context,
	eng storage.Engine,
	storeID roachpb.StoreID,
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
) error {
	// Before creating the replica, see if there is a tombstone which would
	// indicate that this replica has been removed.
	tombstoneKey := keys.RangeTombstoneKey(rangeID)
	var tombstone kvserverpb.RangeTombstone
	if ok, err := storage.MVCCGetProto(
		ctx, eng, tombstoneKey, hlc.Timestamp{}, &tombstone, storage.MVCCGetOptions{},
	); err != nil {
		return err
	} else if ok && replicaID < tombstone.NextReplicaID {
		return &kvpb.RaftGroupDeletedError{}
	}

	// Write the RaftReplicaID for this replica. This is the only place in the
	// CockroachDB code that we are creating a new *uninitialized* replica.
	// Note that it is possible that we have already created the HardState for
	// an uninitialized replica, then crashed, and on recovery are receiving a
	// raft message for the same or later replica.
	// - Same replica: we are overwriting the RaftReplicaID with the same
	//   value, which is harmless.
	// - Later replica: there may be an existing HardState for the older
	//   uninitialized replica with Commit=0 and non-zero Term and Vote. Using
	//   the Term and Vote values for that older replica in the context of
	//   this newer replica is harmless since it just limits the votes for
	//   this replica.
	//
	// Compatibility:
	// - v21.2 and v22.1: v22.1 unilaterally introduces RaftReplicaID (an
	//   unreplicated range-id local key). If a v22.1 binary is rolled back at
	//   a node, the fact that RaftReplicaID was written is harmless to a
	//   v21.2 node since it does not read it. When a v21.2 drops an
	//   initialized range, the RaftReplicaID will also be deleted because the
	//   whole range-ID local key space is deleted.
	// - v22.2: no changes: RaftReplicaID is written, but old Replicas may not
	//   have it yet.
	// - v23.1: at startup, we remove any uninitialized replicas that have a
	//   HardState but no RaftReplicaID, see kvstorage.LoadAndReconcileReplicas.
	//   So after first call to this method we have the invariant that all replicas
	//   have a RaftReplicaID persisted.
	sl := stateloader.Make(rangeID)
	if err := sl.SetRaftReplicaID(ctx, eng, replicaID); err != nil {
		return err
	}

	// Make sure that storage invariants for this uninitialized replica hold.
	uninitDesc := roachpb.RangeDescriptor{RangeID: rangeID}
	_, err := LoadReplicaState(ctx, eng, storeID, &uninitDesc, replicaID)
	return err
}
