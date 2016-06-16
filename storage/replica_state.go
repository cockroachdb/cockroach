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
		desc.IsInitialized(),
	); err != nil {
		return storagebase.ReplicaState{}, err
	}

	if s.Stats, err = loadMVCCStats(reader, desc.RangeID); err != nil {
		return storagebase.ReplicaState{}, err
	}

	// The truncated state should not be optional (i.e. the pointer is
	// pointless), but it is and the migration is not worth it.
	if truncState, err := loadTruncatedState(reader, desc.RangeID); err != nil {
		return storagebase.ReplicaState{}, err
	} else {
		s.TruncatedState = &truncState
	}

	return s, nil
}

// TODO(tschottdorf): write and use setLease, too.
func loadLease(eng engine.Reader, rangeID roachpb.RangeID) (*roachpb.Lease, error) {
	lease := &roachpb.Lease{}
	if _, err := engine.MVCCGetProto(context.Background(), eng, keys.RangeLeaderLeaseKey(rangeID), hlc.ZeroTimestamp, true, nil, lease); err != nil {
		return nil, err
	}
	return lease, nil
}

func loadTruncatedState(
	eng engine.Reader, rangeID roachpb.RangeID,
) (roachpb.RaftTruncatedState, error) {
	var truncState roachpb.RaftTruncatedState
	if _, err := engine.MVCCGetProto(context.Background(), eng,
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

func setGCThreshold(
	eng engine.ReadWriter, ms *enginepb.MVCCStats, rangeID roachpb.RangeID, threshold *hlc.Timestamp,
) error {
	return engine.MVCCPutProto(context.Background(), eng, ms,
		keys.RangeLastGCKey(rangeID), hlc.ZeroTimestamp, nil, threshold)
}

func loadGCThreshold(eng engine.Reader, rangeID roachpb.RangeID) (hlc.Timestamp, error) {
	var t hlc.Timestamp
	_, err := engine.MVCCGetProto(context.Background(), eng, keys.RangeLastGCKey(rangeID),
		hlc.ZeroTimestamp, true, nil, &t)
	return t, err
}

func loadMVCCStats(eng engine.Reader, rangeID roachpb.RangeID) (enginepb.MVCCStats, error) {
	var ms enginepb.MVCCStats
	if err := engine.MVCCGetRangeStats(context.Background(), eng, rangeID, &ms); err != nil {
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

func loadFrozenStatus(eng engine.Reader, rangeID roachpb.RangeID) (bool, error) {
	val, _, err := engine.MVCCGet(context.Background(), eng, keys.RangeFrozenStatusKey(rangeID),
		hlc.ZeroTimestamp, true, nil)
	if err != nil {
		return false, err
	}
	if val == nil {
		return false, nil
	}
	return val.GetBool()
}
