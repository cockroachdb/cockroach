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
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/protoutil"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"golang.org/x/net/context"
)

// TODO(tschottdorf): unified method to update both in-mem and on-disk
// state, similar to how loadState unifies restoring from storage.
func loadState(reader engine.Reader, desc *roachpb.RangeDescriptor) (storagebase.ReplicaState, error) {
	var s storagebase.ReplicaState
	// TODO(tschottdorf): figure out whether this is always synchronous with
	// on-disk state (likely iffy during Split/ChangeReplica triggers).
	s.Desc = protoutil.Clone(desc).(*roachpb.RangeDescriptor)
	// Read the leader lease.
	var err error
	if s.Lease, err = loadLease(reader, desc.RangeID); err != nil {
		return storagebase.ReplicaState{}, err
	}

	if s.Frozen, err = loadFrozenStatus(reader, desc.RangeID); err != nil {
		return storagebase.ReplicaState{}, err
	}

	if s.GCThreshold, err = loadGCThreshold(reader, desc.RangeID); err != nil {
		return storagebase.ReplicaState{}, err
	}

	if s.RaftAppliedIndex, s.LeaseAppliedIndex, err = loadAppliedIndex(
		reader,
		desc.RangeID,
	); err != nil {
		return storagebase.ReplicaState{}, err
	}

	if s.Stats, err = loadMVCCStats(reader, desc.RangeID); err != nil {
		return storagebase.ReplicaState{}, err
	}

	// The truncated state should not be optional (i.e. the pointer is
	// pointless), but it is and the migration is not worth it.
	truncState, err := loadTruncatedState(reader, desc.RangeID)
	if err != nil {
		return storagebase.ReplicaState{}, err
	}
	s.TruncatedState = &truncState

	return s, nil
}

// saveState persists the given ReplicaState to disk. It assumes that the
// contained Stats are up-to-date and returns the stats which result from
// writing the updated State.
func saveState(
	eng engine.ReadWriter, state storagebase.ReplicaState,
) (enginepb.MVCCStats, error) {
	ms, rangeID := &state.Stats, state.Desc.RangeID
	if err := setLease(eng, ms, rangeID, state.Lease); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := setAppliedIndex(
		eng, ms, rangeID, state.RaftAppliedIndex, state.LeaseAppliedIndex,
	); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := setFrozenStatus(eng, ms, rangeID, state.Frozen); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := setGCThreshold(eng, ms, rangeID, &state.GCThreshold); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := setTruncatedState(eng, ms, rangeID, *state.TruncatedState); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := setMVCCStats(eng, rangeID, state.Stats); err != nil {
		return enginepb.MVCCStats{}, err
	}
	return state.Stats, nil
}

func loadLease(reader engine.Reader, rangeID roachpb.RangeID) (*roachpb.Lease, error) {
	lease := &roachpb.Lease{}
	_, err := engine.MVCCGetProto(context.Background(), reader,
		keys.RangeLeaderLeaseKey(rangeID), hlc.ZeroTimestamp,
		true, nil, lease)
	if err != nil {
		return nil, err
	}
	return lease, nil
}

func setLease(
	eng engine.ReadWriter,
	ms *enginepb.MVCCStats,
	rangeID roachpb.RangeID,
	lease *roachpb.Lease, // TODO(tschottdorf): better if this is never nil
) error {
	if lease == nil {
		return nil
	}
	return engine.MVCCPutProto(
		context.Background(), eng, ms,
		keys.RangeLeaderLeaseKey(rangeID),
		hlc.ZeroTimestamp, nil, lease)
}

func loadAppliedIndex(reader engine.Reader, rangeID roachpb.RangeID) (uint64, uint64, error) {
	var appliedIndex uint64
	v, _, err := engine.MVCCGet(context.Background(), reader, keys.RaftAppliedIndexKey(rangeID),
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
	v, _, err = engine.MVCCGet(context.Background(), reader, keys.LeaseAppliedIndexKey(rangeID),
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

func setAppliedIndex(eng engine.ReadWriter, ms *enginepb.MVCCStats, rangeID roachpb.RangeID, appliedIndex, leaseAppliedIndex uint64) error {
	var value roachpb.Value
	value.SetInt(int64(appliedIndex))

	if err := engine.MVCCPut(context.Background(), eng, ms,
		keys.RaftAppliedIndexKey(rangeID),
		hlc.ZeroTimestamp,
		value,
		nil /* txn */); err != nil {
		return err
	}
	value.SetInt(int64(leaseAppliedIndex))
	return engine.MVCCPut(context.Background(), eng, ms,
		keys.LeaseAppliedIndexKey(rangeID),
		hlc.ZeroTimestamp,
		value,
		nil /* txn */)
}

func loadTruncatedState(
	reader engine.Reader, rangeID roachpb.RangeID,
) (roachpb.RaftTruncatedState, error) {
	var truncState roachpb.RaftTruncatedState
	if _, err := engine.MVCCGetProto(context.Background(), reader,
		keys.RaftTruncatedStateKey(rangeID), hlc.ZeroTimestamp, true,
		nil, &truncState); err != nil {
		return roachpb.RaftTruncatedState{}, err
	}
	return truncState, nil
}

func setTruncatedState(
	eng engine.ReadWriter,
	ms *enginepb.MVCCStats,
	rangeID roachpb.RangeID,
	truncState roachpb.RaftTruncatedState,
) error {
	return engine.MVCCPutProto(context.Background(), eng, ms,
		keys.RaftTruncatedStateKey(rangeID), hlc.ZeroTimestamp, nil, &truncState)
}

func loadGCThreshold(reader engine.Reader, rangeID roachpb.RangeID) (hlc.Timestamp, error) {
	var t hlc.Timestamp
	_, err := engine.MVCCGetProto(context.Background(), reader, keys.RangeLastGCKey(rangeID),
		hlc.ZeroTimestamp, true, nil, &t)
	return t, err
}

func setGCThreshold(
	eng engine.ReadWriter, ms *enginepb.MVCCStats, rangeID roachpb.RangeID, threshold *hlc.Timestamp,
) error {
	return engine.MVCCPutProto(context.Background(), eng, ms,
		keys.RangeLastGCKey(rangeID), hlc.ZeroTimestamp, nil, threshold)
}

func loadMVCCStats(reader engine.Reader, rangeID roachpb.RangeID) (enginepb.MVCCStats, error) {
	var ms enginepb.MVCCStats
	if err := engine.MVCCGetRangeStats(context.Background(), reader, rangeID, &ms); err != nil {
		return enginepb.MVCCStats{}, err
	}
	return ms, nil
}

func setMVCCStats(eng engine.ReadWriter, rangeID roachpb.RangeID, newMS enginepb.MVCCStats) error {
	return engine.MVCCSetRangeStats(context.Background(), eng, rangeID, &newMS)
}

func setFrozenStatus(
	eng engine.ReadWriter, ms *enginepb.MVCCStats, rangeID roachpb.RangeID, frozen bool,
) error {
	var val roachpb.Value
	val.SetBool(frozen)
	return engine.MVCCPut(context.Background(), eng, ms,
		keys.RangeFrozenStatusKey(rangeID), hlc.ZeroTimestamp, val, nil)
}

func loadFrozenStatus(reader engine.Reader, rangeID roachpb.RangeID) (bool, error) {
	val, _, err := engine.MVCCGet(context.Background(), reader, keys.RangeFrozenStatusKey(rangeID),
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

func loadLastIndex(reader engine.Reader, rangeID roachpb.RangeID) (uint64, error) {
	lastIndex := uint64(0)
	v, _, err := engine.MVCCGet(context.Background(), reader,
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
		// or the entire log has been truncated away. raftTruncatedState
		// handles both cases.
		lastEnt, err := raftTruncatedState(reader, rangeID)
		if err != nil {
			return 0, err
		}
		lastIndex = lastEnt.Index
	}
	return lastIndex, nil
}

func setLastIndex(eng engine.ReadWriter, rangeID roachpb.RangeID, lastIndex uint64) error {
	var value roachpb.Value
	value.SetInt(int64(lastIndex))

	return engine.MVCCPut(context.Background(), eng, nil, keys.RaftLastIndexKey(rangeID),
		hlc.ZeroTimestamp,
		value,
		nil /* txn */)
}

func loadHardState(
	reader engine.Reader, rangeID roachpb.RangeID,
) (raftpb.HardState, error) {
	var hs raftpb.HardState
	found, err := engine.MVCCGetProto(context.Background(), reader,
		keys.RaftHardStateKey(rangeID), hlc.ZeroTimestamp, true, nil, &hs)

	if !found || err != nil {
		return raftpb.HardState{}, err
	}
	return hs, nil
}

func setHardState(
	batch engine.ReadWriter, rangeID roachpb.RangeID, st raftpb.HardState,
) error {
	return engine.MVCCPutProto(context.Background(), batch, nil,
		keys.RaftHardStateKey(rangeID),
		hlc.ZeroTimestamp, nil, &st)
}

// writeInitialState bootstraps a new Raft group (i.e. it is called when we
// bootstrap a Range, or when setting up the right hand side of a split).
// Its main task is to persist a consistent Raft (and associated Replica) state
// which does not start from zero but presupposes a few entries already having
// applied.
// The supplied MVCCStats are used for the the Stats field after adjusting for
// persisting the state itself, and the updated stats are returned.
func writeInitialState(
	eng engine.ReadWriter, ms enginepb.MVCCStats, rangeID roachpb.RangeID,
) (enginepb.MVCCStats, error) {
	var s storagebase.ReplicaState

	s.TruncatedState = &roachpb.RaftTruncatedState{
		Term:  raftInitialLogTerm,
		Index: raftInitialLogIndex,
	}
	s.RaftAppliedIndex = s.TruncatedState.Index
	s.Desc = &roachpb.RangeDescriptor{
		RangeID: rangeID,
	}
	s.Stats = ms

	newMS, err := saveState(eng, s)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}

	// Load a potentially existing HardState as we may need to preserve
	// information about cast votes. For example, during a Split for which
	// another node's new right-hand side has contacted us before our left-hand
	// side called in here to create the group.
	oldHS, err := loadHardState(eng, rangeID)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}

	newHS := raftpb.HardState{
		Term:   s.TruncatedState.Term,
		Commit: s.TruncatedState.Index,
	}

	if !raft.IsEmptyHardState(oldHS) {
		if oldHS.Commit > newHS.Commit {
			log.Fatalf("new group made progress before being initalized: %+v",
				oldHS)
		}
		if oldHS.Term > newHS.Term {
			newHS.Term = oldHS.Term
		}
		newHS.Vote = oldHS.Vote
	}

	if err := setHardState(eng, rangeID, newHS); err != nil {
		return enginepb.MVCCStats{}, err
	}

	if err := setLastIndex(eng, rangeID, s.TruncatedState.Index); err != nil {
		return enginepb.MVCCStats{}, err
	}

	return newMS, nil
}
