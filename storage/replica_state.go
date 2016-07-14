// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package storage

import (
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/storage/storagebase"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/protoutil"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// loadState loads a ReplicaState from disk. The exception is the Desc field,
// which is updated transactionally, and is populated from the supplied
// RangeDescriptor under the convention that that is the latest committed
// version.
func loadState(
	ctx context.Context,
	reader engine.Reader, desc *roachpb.RangeDescriptor,
) (storagebase.ReplicaState, error) {
	var s storagebase.ReplicaState
	// TODO(tschottdorf): figure out whether this is always synchronous with
	// on-disk state (likely iffy during Split/ChangeReplica triggers).
	s.Desc = protoutil.Clone(desc).(*roachpb.RangeDescriptor)
	// Read the range lease.
	var err error
	if s.Lease, err = loadLease(ctx, reader, desc.RangeID); err != nil {
		return storagebase.ReplicaState{}, err
	}

	if s.Frozen, err = loadFrozenStatus(ctx, reader, desc.RangeID); err != nil {
		return storagebase.ReplicaState{}, err
	}

	if s.GCThreshold, err = loadGCThreshold(ctx, reader, desc.RangeID); err != nil {
		return storagebase.ReplicaState{}, err
	}

	if s.RaftAppliedIndex, s.LeaseAppliedIndex, err = loadAppliedIndex(
		ctx, reader, desc.RangeID,
	); err != nil {
		return storagebase.ReplicaState{}, err
	}

	if s.Stats, err = loadMVCCStats(ctx, reader, desc.RangeID); err != nil {
		return storagebase.ReplicaState{}, err
	}

	// The truncated state should not be optional (i.e. the pointer is
	// pointless), but it is and the migration is not worth it.
	truncState, err := loadTruncatedState(ctx, reader, desc.RangeID)
	if err != nil {
		return storagebase.ReplicaState{}, err
	}
	s.TruncatedState = &truncState

	return s, nil
}

// saveState persists the given ReplicaState to disk. It assumes that the
// contained Stats are up-to-date and returns the stats which result from
// writing the updated State.
// As an exception to the rule, the Desc field (whose on-disk state is special
// in that it's a full MVCC value and updated transactionally) is only used for
// its RangeID.
//
// TODO(tschottdorf): consolidate direct permutation and persistence of
// state throughout the Raft path in favor of a more organized approach.
func saveState(
	ctx context.Context, eng engine.ReadWriter, state storagebase.ReplicaState,
) (enginepb.MVCCStats, error) {
	ms, rangeID := &state.Stats, state.Desc.RangeID
	if err := setLease(ctx, eng, ms, rangeID, state.Lease); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := setAppliedIndex(
		ctx, eng, ms, rangeID, state.RaftAppliedIndex, state.LeaseAppliedIndex,
	); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := setFrozenStatus(ctx, eng, ms, rangeID, state.Frozen); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := setGCThreshold(ctx, eng, ms, rangeID, &state.GCThreshold); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := setTruncatedState(ctx, eng, ms, rangeID, *state.TruncatedState); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := setMVCCStats(ctx, eng, rangeID, state.Stats); err != nil {
		return enginepb.MVCCStats{}, err
	}
	return state.Stats, nil
}

func loadLease(
	ctx context.Context, reader engine.Reader, rangeID roachpb.RangeID,
) (*roachpb.Lease, error) {
	lease := &roachpb.Lease{}
	_, err := engine.MVCCGetProto(ctx, reader,
		keys.RangeLeaseKey(rangeID), hlc.ZeroTimestamp,
		true, nil, lease)
	if err != nil {
		return nil, err
	}
	return lease, nil
}

func setLease(
	ctx context.Context,
	eng engine.ReadWriter,
	ms *enginepb.MVCCStats,
	rangeID roachpb.RangeID,
	lease *roachpb.Lease, // TODO(tschottdorf): better if this is never nil
) error {
	if lease == nil {
		return nil
	}
	return engine.MVCCPutProto(
		ctx, eng, ms,
		keys.RangeLeaseKey(rangeID),
		hlc.ZeroTimestamp, nil, lease)
}

// loadAppliedIndex returns the Raft applied index and the lease applied index.
func loadAppliedIndex(
	ctx context.Context, reader engine.Reader, rangeID roachpb.RangeID,
) (uint64, uint64, error) {
	var appliedIndex uint64
	v, _, err := engine.MVCCGet(ctx, reader, keys.RaftAppliedIndexKey(rangeID),
		hlc.ZeroTimestamp, true, nil)
	if err != nil {
		return 0, 0, err
	}
	if v != nil {
		int64AppliedIndex, err := v.GetInt()
		if err != nil {
			return 0, 0, err
		}
		appliedIndex = uint64(int64AppliedIndex)
	}
	// TODO(tschottdorf): code duplication.
	var leaseAppliedIndex uint64
	v, _, err = engine.MVCCGet(ctx, reader, keys.LeaseAppliedIndexKey(rangeID),
		hlc.ZeroTimestamp, true, nil)
	if err != nil {
		return 0, 0, err
	}
	if v != nil {
		int64LeaseAppliedIndex, err := v.GetInt()
		if err != nil {
			return 0, 0, err
		}
		leaseAppliedIndex = uint64(int64LeaseAppliedIndex)
	}

	return appliedIndex, leaseAppliedIndex, nil
}

func setAppliedIndex(
	ctx context.Context,
	eng engine.ReadWriter,
	ms *enginepb.MVCCStats,
	rangeID roachpb.RangeID,
	appliedIndex,
	leaseAppliedIndex uint64,
) error {
	var value roachpb.Value
	value.SetInt(int64(appliedIndex))

	if err := engine.MVCCPut(ctx, eng, ms,
		keys.RaftAppliedIndexKey(rangeID),
		hlc.ZeroTimestamp,
		value,
		nil /* txn */); err != nil {
		return err
	}
	value.SetInt(int64(leaseAppliedIndex))
	return engine.MVCCPut(ctx, eng, ms,
		keys.LeaseAppliedIndexKey(rangeID),
		hlc.ZeroTimestamp,
		value,
		nil /* txn */)
}

func loadTruncatedState(
	ctx context.Context, reader engine.Reader, rangeID roachpb.RangeID,
) (roachpb.RaftTruncatedState, error) {
	var truncState roachpb.RaftTruncatedState
	if _, err := engine.MVCCGetProto(ctx, reader,
		keys.RaftTruncatedStateKey(rangeID), hlc.ZeroTimestamp, true,
		nil, &truncState); err != nil {
		return roachpb.RaftTruncatedState{}, err
	}
	return truncState, nil
}

func setTruncatedState(
	ctx context.Context,
	eng engine.ReadWriter,
	ms *enginepb.MVCCStats,
	rangeID roachpb.RangeID,
	truncState roachpb.RaftTruncatedState,
) error {
	return engine.MVCCPutProto(ctx, eng, ms,
		keys.RaftTruncatedStateKey(rangeID), hlc.ZeroTimestamp, nil, &truncState)
}

func loadGCThreshold(
	ctx context.Context, reader engine.Reader, rangeID roachpb.RangeID,
) (hlc.Timestamp, error) {
	var t hlc.Timestamp
	_, err := engine.MVCCGetProto(ctx, reader, keys.RangeLastGCKey(rangeID),
		hlc.ZeroTimestamp, true, nil, &t)
	return t, err
}

func setGCThreshold(
	ctx context.Context,
	eng engine.ReadWriter,
	ms *enginepb.MVCCStats,
	rangeID roachpb.RangeID,
	threshold *hlc.Timestamp,
) error {
	return engine.MVCCPutProto(ctx, eng, ms,
		keys.RangeLastGCKey(rangeID), hlc.ZeroTimestamp, nil, threshold)
}

func loadMVCCStats(
	ctx context.Context, reader engine.Reader, rangeID roachpb.RangeID,
) (enginepb.MVCCStats, error) {
	var ms enginepb.MVCCStats
	if err := engine.MVCCGetRangeStats(ctx, reader, rangeID, &ms); err != nil {
		return enginepb.MVCCStats{}, err
	}
	return ms, nil
}

func setMVCCStats(
	ctx context.Context, eng engine.ReadWriter, rangeID roachpb.RangeID, newMS enginepb.MVCCStats,
) error {
	return engine.MVCCSetRangeStats(ctx, eng, rangeID, &newMS)
}

func setFrozenStatus(
	ctx context.Context,
	eng engine.ReadWriter,
	ms *enginepb.MVCCStats,
	rangeID roachpb.RangeID,
	frozen bool,
) error {
	var val roachpb.Value
	val.SetBool(frozen)
	return engine.MVCCPut(ctx, eng, ms,
		keys.RangeFrozenStatusKey(rangeID), hlc.ZeroTimestamp, val, nil)
}

func loadFrozenStatus(
	ctx context.Context, reader engine.Reader, rangeID roachpb.RangeID,
) (bool, error) {
	val, _, err := engine.MVCCGet(ctx, reader, keys.RangeFrozenStatusKey(rangeID),
		hlc.ZeroTimestamp, true, nil)
	if err != nil {
		return false, err
	}
	if val == nil {
		return false, nil
	}
	return val.GetBool()
}

// The rest is not technically part of ReplicaState.
// TODO(tschottdorf): more consolidation of ad-hoc structures: last index and
// hard state. These are closely coupled with ReplicaState (and in particular
// with its TruncatedState) but are different in that they are not consistently
// updated through Raft.

func loadLastIndex(
	ctx context.Context, reader engine.Reader, rangeID roachpb.RangeID,
) (uint64, error) {
	lastIndex := uint64(0)
	v, _, err := engine.MVCCGet(ctx, reader,
		keys.RaftLastIndexKey(rangeID),
		hlc.ZeroTimestamp, true /* consistent */, nil)
	if err != nil {
		return 0, err
	}
	if v != nil {
		int64LastIndex, err := v.GetInt()
		if err != nil {
			return 0, err
		}
		lastIndex = uint64(int64LastIndex)
	} else {
		// The log is empty, which means we are either starting from scratch
		// or the entire log has been truncated away.
		lastEnt, err := loadTruncatedState(ctx, reader, rangeID)
		if err != nil {
			return 0, err
		}
		lastIndex = lastEnt.Index
	}
	return lastIndex, nil
}

func setLastIndex(
	ctx context.Context, eng engine.ReadWriter, rangeID roachpb.RangeID, lastIndex uint64,
) error {
	var value roachpb.Value
	value.SetInt(int64(lastIndex))

	return engine.MVCCPut(ctx, eng, nil, keys.RaftLastIndexKey(rangeID),
		hlc.ZeroTimestamp,
		value,
		nil /* txn */)
}

// loadReplicaDestroyedError loads the replica destroyed error for the specified
// range. If there is no error, nil is returned.
func loadReplicaDestroyedError(
	ctx context.Context, reader engine.Reader, rangeID roachpb.RangeID,
) (*roachpb.Error, error) {
	var v roachpb.Error
	found, err := engine.MVCCGetProto(ctx, reader,
		keys.RangeReplicaDestroyedErrorKey(rangeID),
		hlc.ZeroTimestamp, true /* consistent */, nil, &v)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return &v, nil
}

// setReplicaDestroyedError sets an error indicating that the replica has been
// destroyed.
func setReplicaDestroyedError(
	ctx context.Context, eng engine.ReadWriter, rangeID roachpb.RangeID, err *roachpb.Error,
) error {
	return engine.MVCCPutProto(ctx, eng, nil,
		keys.RangeReplicaDestroyedErrorKey(rangeID), hlc.ZeroTimestamp, nil /* txn */, err)
}

func loadHardState(
	ctx context.Context, reader engine.Reader, rangeID roachpb.RangeID,
) (raftpb.HardState, error) {
	var hs raftpb.HardState
	found, err := engine.MVCCGetProto(ctx, reader,
		keys.RaftHardStateKey(rangeID), hlc.ZeroTimestamp, true, nil, &hs)

	if !found || err != nil {
		return raftpb.HardState{}, err
	}
	return hs, nil
}

func setHardState(
	ctx context.Context, batch engine.ReadWriter, rangeID roachpb.RangeID, st raftpb.HardState,
) error {
	return engine.MVCCPutProto(ctx, batch, nil,
		keys.RaftHardStateKey(rangeID),
		hlc.ZeroTimestamp, nil, &st)
}

// synthesizeHardState synthesizes a HardState from the given ReplicaState and
// any existing on-disk HardState in the context of a snapshot, while verifying
// that the application of the snapshot does not violate Raft invariants. It
// must be called after the supplied state and ReadWriter have been updated
// with the result of the snapshot.
// If there is an existing HardState, we must respect it and we must not apply
// a snapshot that would move the state backwards.
func synthesizeHardState(
	ctx context.Context, eng engine.ReadWriter, s storagebase.ReplicaState, oldHS raftpb.HardState,
) error {
	newHS := raftpb.HardState{
		Term: s.TruncatedState.Term,
		// Note that when applying a Raft snapshot, the applied index is
		// equal to the Commit index represented by the snapshot.
		Commit: s.RaftAppliedIndex,
	}

	if oldHS.Commit > newHS.Commit {
		return errors.Errorf("can't decrease HardState.Commit from %d to %d",
			oldHS.Commit, newHS.Commit)
	}
	if oldHS.Term > newHS.Term {
		// The existing HardState is allowed to be ahead of us, which is
		// relevant in practice for the split trigger. We already checked above
		// that we're not rewinding the acknowledged index, and we haven't
		// updated votes yet.
		newHS.Term = oldHS.Term
	}
	// If the existing HardState voted in this term, remember that.
	if oldHS.Term == newHS.Term {
		newHS.Vote = oldHS.Vote
	}
	return errors.Wrapf(setHardState(ctx, eng, s.Desc.RangeID, newHS), "writing HardState %+v", &newHS)
}

// writeInitialState bootstraps a new Raft group (i.e. it is called when we
// bootstrap a Range, or when setting up the right hand side of a split).
// Its main task is to persist a consistent Raft (and associated Replica) state
// which does not start from zero but presupposes a few entries already having
// applied.
// The supplied MVCCStats are used for the Stats field after adjusting for
// persisting the state itself, and the updated stats are returned.
func writeInitialState(
	ctx context.Context, eng engine.ReadWriter, ms enginepb.MVCCStats, desc roachpb.RangeDescriptor,
) (enginepb.MVCCStats, error) {
	var s storagebase.ReplicaState

	s.TruncatedState = &roachpb.RaftTruncatedState{
		Term:  raftInitialLogTerm,
		Index: raftInitialLogIndex,
	}
	s.RaftAppliedIndex = s.TruncatedState.Index
	s.Desc = &roachpb.RangeDescriptor{
		RangeID: desc.RangeID,
	}
	s.Stats = ms

	newMS, err := saveState(ctx, eng, s)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}

	oldHS, err := loadHardState(ctx, eng, desc.RangeID)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}

	if err := synthesizeHardState(ctx, eng, s, oldHS); err != nil {
		return enginepb.MVCCStats{}, err
	}

	if err := setLastIndex(ctx, eng, desc.RangeID, s.TruncatedState.Index); err != nil {
		return enginepb.MVCCStats{}, err
	}

	return newMS, nil
}
