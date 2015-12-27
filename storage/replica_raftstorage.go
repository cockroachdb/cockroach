// Copyright 2015 The Cockroach Authors.
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
// Author: Ben Darnell

package storage

import (
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/gogo/protobuf/proto"
)

// initialState implements the raft.Storage interface.
func (r *Replica) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	var hs raftpb.HardState
	desc := r.Desc()
	found, err := engine.MVCCGetProto(r.store.Engine(), keys.RaftHardStateKey(desc.RangeID),
		roachpb.ZeroTimestamp, true, nil, &hs)
	if err != nil {
		return raftpb.HardState{}, raftpb.ConfState{}, err
	}
	initialized := r.isInitialized()
	if !found {
		// We don't have a saved HardState, so set up the defaults.
		if initialized {
			// Set the initial log term.
			hs.Term = raftInitialLogTerm
			hs.Commit = raftInitialLogIndex

			atomic.StoreUint64(&r.lastIndex, raftInitialLogIndex)
		} else {
			// This is a new range we are receiving from another node. Start
			// from zero so we will receive a snapshot.
			atomic.StoreUint64(&r.lastIndex, 0)
		}
	} else if initialized && hs.Commit == 0 {
		// Normally, when the commit index changes, raft gives us a new
		// commit index to persist, however, during initialization, which
		// occurs entirely in cockroach, raft has no knowledge of this.
		// By setting this to the initial log index, we avoid a panic in
		// raft caused by this inconsistency.
		hs.Commit = raftInitialLogIndex
	}

	var cs raftpb.ConfState
	// For uninitalized ranges, membership is unknown at this point.
	if found || initialized {
		for _, rep := range desc.Replicas {
			cs.Nodes = append(cs.Nodes, uint64(rep.ReplicaID))
		}
	}

	return hs, cs, nil
}

// Entries implements the raft.Storage interface. Note that maxBytes is advisory
// and this method will always return at least one entry even if it exceeds
// maxBytes. Passing maxBytes equal to zero disables size checking.
// TODO(bdarnell): consider caching for recent entries, if rocksdb's builtin caching
// is insufficient.
func (r *Replica) Entries(lo, hi, maxBytes uint64) ([]raftpb.Entry, error) {
	if lo > hi {
		return nil, util.Errorf("lo:%d is greater than hi:%d", lo, hi)
	}
	// Scan over the log to find the requested entries in the range [lo, hi),
	// stopping once we have enough.
	var ents []raftpb.Entry
	size := uint64(0)
	var ent raftpb.Entry
	expectedIndex := lo
	exceededMaxBytes := false
	scanFunc := func(kv roachpb.KeyValue) (bool, error) {
		if err := kv.Value.GetProto(&ent); err != nil {
			return false, err
		}
		// Exit early if we have any gaps or it has been compacted.
		if ent.Index != expectedIndex {
			return true, nil
		}
		expectedIndex++
		size += uint64(ent.Size())
		ents = append(ents, ent)
		exceededMaxBytes = maxBytes > 0 && size > maxBytes
		return exceededMaxBytes, nil
	}

	rangeID := r.Desc().RangeID
	_, err := engine.MVCCIterate(r.store.Engine(),
		keys.RaftLogKey(rangeID, lo),
		keys.RaftLogKey(rangeID, hi),
		roachpb.ZeroTimestamp,
		true /* consistent */, nil /* txn */, false /* !reverse */, scanFunc)

	if err != nil {
		return nil, err
	}

	// Did the correct number of results come back? If so, we're all good.
	if len(ents) == int(hi)-int(lo) {
		return ents, nil
	}

	// Did we hit the size limit? If so, return what we have.
	if exceededMaxBytes {
		return ents, nil
	}

	// Did we get any results at all? Because something went wrong.
	if len(ents) > 0 {
		// Was the lo already truncated?
		if ents[0].Index > lo {
			return nil, raft.ErrCompacted
		}

		// Was the missing index after the last index?
		lastIndex, err := r.LastIndex()
		if err != nil {
			return nil, err
		}
		if lastIndex <= expectedIndex {
			return nil, raft.ErrUnavailable
		}

		// We have a gap in the record, if so, return a nasty error.
		return nil, util.Errorf("there is a gap in the index record between lo:%d and hi:%d at index:%d", lo, hi, expectedIndex)
	}

	// No results, was it due to unavailability or truncation?
	ts, err := r.raftTruncatedState()
	if err != nil {
		return nil, err
	}
	if ts.Index >= lo {
		// The requested lo index has already been truncated.
		return nil, raft.ErrCompacted
	}
	// The requested lo index does not yet exist.
	return nil, raft.ErrUnavailable
}

// Term implements the raft.Storage interface.
func (r *Replica) Term(i uint64) (uint64, error) {
	ents, err := r.Entries(i, i+1, 0)
	if err == raft.ErrCompacted {
		ts, err := r.raftTruncatedState()
		if err != nil {
			return 0, err
		}
		if i == ts.Index {
			return ts.Term, nil
		}
		return 0, raft.ErrCompacted
	} else if err != nil {
		return 0, err
	}
	if len(ents) == 0 {
		return 0, nil
	}
	return ents[0].Term, nil
}

// LastIndex implements the raft.Storage interface.
func (r *Replica) LastIndex() (uint64, error) {
	return atomic.LoadUint64(&r.lastIndex), nil
}

// raftTruncatedState returns metadata about the log that preceded the first
// current entry. This includes both entries that have been compacted away
// and the dummy entries that make up the starting point of an empty log.
func (r *Replica) raftTruncatedState() (roachpb.RaftTruncatedState, error) {
	if ts := r.getCachedTruncatedState(); ts != nil {
		return *ts, nil
	}
	ts := roachpb.RaftTruncatedState{}
	ok, err := engine.MVCCGetProto(r.store.Engine(), keys.RaftTruncatedStateKey(r.Desc().RangeID),
		roachpb.ZeroTimestamp, true, nil, &ts)
	if err != nil {
		return ts, err
	}
	if !ok {
		if r.isInitialized() {
			// If we created this range, set the initial log index/term.
			ts.Index = raftInitialLogIndex
			ts.Term = raftInitialLogTerm
		} else {
			// This is a new range we are receiving from another node. Start
			// from zero so we will receive a snapshot.
			ts.Index = 0
			ts.Term = 0
		}
	}

	if ts.Index != 0 {
		r.setCachedTruncatedState(&ts)
	}
	return ts, nil
}

// FirstIndex implements the raft.Storage interface.
func (r *Replica) FirstIndex() (uint64, error) {
	ts, err := r.raftTruncatedState()
	if err != nil {
		return 0, err
	}
	return ts.Index + 1, nil
}

// loadAppliedIndex retrieves the applied index from the supplied engine.
func (r *Replica) loadAppliedIndex(eng engine.Engine) (uint64, error) {
	var appliedIndex uint64
	if r.isInitialized() {
		appliedIndex = raftInitialLogIndex
	} else {
		appliedIndex = 0
	}
	v, _, err := engine.MVCCGet(eng, keys.RaftAppliedIndexKey(r.Desc().RangeID),
		roachpb.ZeroTimestamp, true, nil)
	if err != nil {
		return 0, err
	}
	if v != nil {
		int64AppliedIndex, err := v.GetInt()
		if err != nil {
			return 0, err
		}
		appliedIndex = uint64(int64AppliedIndex)
	}
	return appliedIndex, nil
}

// setAppliedIndex persists a new applied index.
func setAppliedIndex(eng engine.Engine, rangeID roachpb.RangeID, appliedIndex uint64) error {
	var value roachpb.Value
	value.SetInt(int64(appliedIndex))

	return engine.MVCCPut(eng, nil, /* stats */
		keys.RaftAppliedIndexKey(rangeID),
		roachpb.ZeroTimestamp,
		value,
		nil /* txn */)
}

// loadLastIndex retrieves the last index from storage.
func (r *Replica) loadLastIndex() (uint64, error) {
	lastIndex := uint64(0)
	v, _, err := engine.MVCCGet(r.store.Engine(),
		keys.RaftLastIndexKey(r.Desc().RangeID),
		roachpb.ZeroTimestamp, true /* consistent */, nil)
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
		lastEnt, err := r.raftTruncatedState()
		if err != nil {
			return 0, err
		}
		lastIndex = lastEnt.Index
	}
	return lastIndex, nil
}

// setLastIndex persists a new last index.
func setLastIndex(eng engine.Engine, rangeID roachpb.RangeID, lastIndex uint64) error {
	var value roachpb.Value
	value.SetInt(int64(lastIndex))

	return engine.MVCCPut(eng, nil, keys.RaftLastIndexKey(rangeID),
		roachpb.ZeroTimestamp,
		value,
		nil /* txn */)
}

// Snapshot implements the raft.Storage interface.
func (r *Replica) Snapshot() (raftpb.Snapshot, error) {
	// Copy all the data from a consistent RocksDB snapshot into a RaftSnapshotData.
	snap := r.store.NewSnapshot()
	defer snap.Close()
	var snapData roachpb.RaftSnapshotData

	// Read the range metadata from the snapshot instead of the members
	// of the Range struct because they might be changed concurrently.
	appliedIndex, err := r.loadAppliedIndex(snap)
	if err != nil {
		return raftpb.Snapshot{}, err
	}

	var desc roachpb.RangeDescriptor
	// We ignore intents on the range descriptor (consistent=false) because we
	// know they cannot be committed yet; operations that modify range
	// descriptors resolve their own intents when they commit.
	ok, err := engine.MVCCGetProto(snap, keys.RangeDescriptorKey(r.Desc().StartKey),
		r.store.Clock().Now(), false /* !consistent */, nil, &desc)
	if err != nil {
		return raftpb.Snapshot{}, util.Errorf("failed to get desc: %s", err)
	}
	if !ok {
		return raftpb.Snapshot{}, util.Errorf("couldn't find range descriptor")
	}

	// Store RangeDescriptor as metadata, it will be retrieved by ApplySnapshot()
	snapData.RangeDescriptor = desc

	// Iterate over all the data in the range, including local-only data like
	// the sequence cache.
	iter := newReplicaDataIterator(&desc, snap)
	defer iter.Close()
	for ; iter.Valid(); iter.Next() {
		key := iter.Key()
		snapData.KV = append(snapData.KV,
			&roachpb.RaftSnapshotData_KeyValue{
				Key:       key.Key,
				Value:     iter.Value(),
				Timestamp: key.Timestamp,
			})
	}

	data, err := proto.Marshal(&snapData)
	if err != nil {
		return raftpb.Snapshot{}, err
	}

	// Synthesize our raftpb.ConfState from desc.
	var cs raftpb.ConfState
	for _, rep := range desc.Replicas {
		cs.Nodes = append(cs.Nodes, uint64(rep.ReplicaID))
	}

	term, err := r.Term(appliedIndex)
	if err != nil {
		return raftpb.Snapshot{}, util.Errorf("failed to fetch term of %d: %s", appliedIndex, err)
	}

	return raftpb.Snapshot{
		Data: data,
		Metadata: raftpb.SnapshotMetadata{
			Index:     appliedIndex,
			Term:      term,
			ConfState: cs,
		},
	}, nil
}

// append the given entries to the raft log.
func (r *Replica) append(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	batch := r.store.Engine().NewBatch()
	defer batch.Close()

	rangeID := r.Desc().RangeID

	for _, ent := range entries {
		err := engine.MVCCPutProto(batch, nil, keys.RaftLogKey(rangeID, ent.Index),
			roachpb.ZeroTimestamp, nil, &ent)
		if err != nil {
			return err
		}
	}
	lastIndex := entries[len(entries)-1].Index
	prevLastIndex := atomic.LoadUint64(&r.lastIndex)
	// Delete any previously appended log entries which never committed.
	for i := lastIndex + 1; i <= prevLastIndex; i++ {
		err := engine.MVCCDelete(batch, nil,
			keys.RaftLogKey(rangeID, i), roachpb.ZeroTimestamp, nil)
		if err != nil {
			return err
		}
	}

	// Commit the batch and update the last index.
	if err := setLastIndex(batch, rangeID, lastIndex); err != nil {
		return err
	}
	if err := batch.Commit(); err != nil {
		return err
	}

	atomic.StoreUint64(&r.lastIndex, lastIndex)
	return nil
}

// updateRangeInfo is called whenever a range is updated by ApplySnapshot
// or is created by range splitting to setup the fields which are
// uninitialized or need updating.
func (r *Replica) updateRangeInfo() error {
	// RangeMaxBytes should be updated by looking up Zone Config in two cases:
	// 1. After snapshot applying, if no updating of zone config
	// for this key range, then maxBytes of this range will not
	// be updated.
	// 2. After a new range is created by range splition, just
	// copying maxBytes from the original range does not work
	// since the original range and the new range might belong
	// to different zones.
	// Load the system config.
	cfg := r.store.Gossip().GetSystemConfig()
	if cfg == nil {
		// This could be before the system config was ever gossiped,
		// or it expired. Let the gossip callback set the info.
		log.Warningf("no system config available, cannot determine range MaxBytes")
		return nil
	}

	// Find zone config for this range.
	zone, err := cfg.GetZoneConfigForKey(r.Desc().StartKey)
	if err != nil {
		return util.Errorf("failed to lookup zone config for Range %s: %s", r, err)
	}

	r.SetMaxBytes(zone.RangeMaxBytes)
	return nil
}

// applySnapshot updates the replica based on the given snapshot.
func (r *Replica) applySnapshot(snap raftpb.Snapshot) error {
	snapData := roachpb.RaftSnapshotData{}
	err := proto.Unmarshal(snap.Data, &snapData)
	if err != nil {
		return err
	}

	rangeID := r.Desc().RangeID

	// First, save the HardState.  The HardState must not be changed
	// because it may record a previous vote cast by this node.
	hardStateKey := keys.RaftHardStateKey(rangeID)
	hardState, _, err := engine.MVCCGet(r.store.Engine(), hardStateKey, roachpb.ZeroTimestamp, true /* consistent */, nil)
	if err != nil {
		return err
	}

	// Extract the updated range descriptor.
	desc := snapData.RangeDescriptor

	batch := r.store.Engine().NewBatch()
	defer batch.Close()

	// Delete everything in the range and recreate it from the snapshot.
	iter := newReplicaDataIterator(&desc, r.store.Engine())
	defer iter.Close()
	for ; iter.Valid(); iter.Next() {
		if err := batch.Clear(iter.Key()); err != nil {
			return err
		}
	}

	// Write the snapshot into the range.
	for _, kv := range snapData.KV {
		mvccKey := engine.MVCCKey{
			Key:       kv.Key,
			Timestamp: kv.Timestamp,
		}
		if err := batch.Put(mvccKey, kv.Value); err != nil {
			return err
		}
	}

	// Restore the saved HardState.
	if hardState == nil {
		err := engine.MVCCDelete(batch, nil, hardStateKey, roachpb.ZeroTimestamp, nil)
		if err != nil {
			return err
		}
	} else {
		err := engine.MVCCPut(batch, nil, hardStateKey, roachpb.ZeroTimestamp, *hardState, nil)
		if err != nil {
			return err
		}
	}

	// Read the leader lease.
	lease, err := loadLeaderLease(batch, desc.RangeID)
	if err != nil {
		return err
	}

	// Load updated range stats. The local newStats variable will be assigned
	// to r.stats after the batch commits.
	newStats, err := newRangeStats(desc.RangeID, batch)
	if err != nil {
		return err
	}

	// The next line sets the persisted last index to the last applied index.
	// This is not a correctness issue, but means that we may have just
	// transferred some entries we're about to re-request from the leader and
	// overwrite.
	// However, raft.MultiNode currently expects this behaviour, and the
	// performance implications are not likely to be drastic. If our feelings
	// about this ever change, we can add a LastIndex field to
	// raftpb.SnapshotMetadata.
	if err := setLastIndex(batch, rangeID, snap.Metadata.Index); err != nil {
		return err
	}

	if err := batch.Commit(); err != nil {
		return err
	}

	// Update the range stats.
	r.stats.Replace(newStats)

	// As outlined above, last and applied index are the same after applying
	// the snapshot.
	atomic.StoreUint64(&r.lastIndex, snap.Metadata.Index)
	atomic.StoreUint64(&r.appliedIndex, snap.Metadata.Index)

	// Atomically update the descriptor and lease.
	if err := r.setDesc(&desc); err != nil {
		return err
	}
	// Update other fields which are uninitialized or need updating.
	// This may not happen if the system config has not yet been loaded.
	// While config update will correctly set the fields, there is no order
	// guarangee in ApplySnapshot.
	// TODO: should go through the standard store lock when adding a replica.
	if err := r.updateRangeInfo(); err != nil {
		return err
	}

	atomic.StorePointer(&r.lease, unsafe.Pointer(lease))
	return nil
}

// setHardState persists the raft HardState.
func (r *Replica) setHardState(st raftpb.HardState) error {
	return engine.MVCCPutProto(r.store.Engine(), nil, keys.RaftHardStateKey(r.Desc().RangeID),
		roachpb.ZeroTimestamp, nil, &st)
}

// Raft commands are encoded with a 1-byte version (currently 0), a 16-byte ID,
// followed by the payload. This inflexible encoding is used so we can efficiently
// parse the command id while processing the logs.
const (
	// The prescribed length for each command ID.
	raftCommandIDLen                = 8
	raftCommandEncodingVersion byte = 0
)

func encodeRaftCommand(commandID string, command []byte) []byte {
	if len(commandID) != raftCommandIDLen {
		log.Fatalf("invalid command ID length; %d != %d", len(commandID), raftCommandIDLen)
	}
	x := make([]byte, 1, 1+raftCommandIDLen+len(command))
	x[0] = raftCommandEncodingVersion
	x = append(x, []byte(commandID)...)
	x = append(x, command...)
	return x
}

func decodeRaftCommand(data []byte) (commandID string, command []byte) {
	if data[0] != raftCommandEncodingVersion {
		log.Fatalf("unknown command encoding version %v", data[0])
	}
	return string(data[1 : 1+raftCommandIDLen]), data[1+raftCommandIDLen:]
}
