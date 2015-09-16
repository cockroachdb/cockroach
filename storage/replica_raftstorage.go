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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Ben Darnell

package storage

import (
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	gogoproto "github.com/gogo/protobuf/proto"
)

var _ multiraft.WriteableGroupStorage = &Replica{}

// InitialState implements the raft.Storage interface.
func (r *Replica) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	var hs raftpb.HardState
	desc := r.Desc()
	found, err := engine.MVCCGetProto(r.rm.Engine(), keys.RaftHardStateKey(desc.RangeID),
		proto.ZeroTimestamp, true, nil, &hs)
	if err != nil {
		return raftpb.HardState{}, raftpb.ConfState{}, err
	}
	if !found {
		// We don't have a saved HardState, so set up the defaults.
		if r.isInitialized() {
			// Set the initial log term.
			hs.Term = raftInitialLogTerm
			hs.Commit = raftInitialLogIndex

			atomic.StoreUint64(&r.lastIndex, raftInitialLogIndex)
		} else {
			// This is a new range we are receiving from another node. Start
			// from zero so we will receive a snapshot.
			atomic.StoreUint64(&r.lastIndex, 0)
		}
	}

	var cs raftpb.ConfState
	// For uninitalized ranges, membership is unknown at this point.
	if found || r.isInitialized() {
		for _, rep := range desc.Replicas {
			cs.Nodes = append(cs.Nodes, uint64(proto.MakeRaftNodeID(rep.NodeID, rep.StoreID)))
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
	// Scan over the log to find the requested entries in the range [lo, hi),
	// stopping once we have enough.
	var ents []raftpb.Entry
	size := uint64(0)
	var ent raftpb.Entry
	scanFunc := func(kv proto.KeyValue) (bool, error) {
		err := gogoproto.Unmarshal(kv.Value.GetBytes(), &ent)
		if err != nil {
			return false, err
		}
		size += uint64(ent.Size())
		ents = append(ents, ent)
		return maxBytes > 0 && size > maxBytes, nil
	}

	rangeID := r.Desc().RangeID

	_, err := engine.MVCCIterate(r.rm.Engine(),
		keys.RaftLogKey(rangeID, lo),
		keys.RaftLogKey(rangeID, hi),
		proto.ZeroTimestamp,
		true /* consistent */, nil /* txn */, false /* !reverse */, scanFunc)

	if err != nil {
		return nil, err
	}

	// If neither the number of entries nor the size limitations had an
	// effect, we weren't able to supply everything the client wanted.
	if len(ents) != int(hi-lo) && (maxBytes == 0 || size < maxBytes) {
		return nil, raft.ErrUnavailable
	}

	return ents, nil
}

// Term implements the raft.Storage interface.
func (r *Replica) Term(i uint64) (uint64, error) {
	ents, err := r.Entries(i, i+1, 0)
	if err == raft.ErrUnavailable {
		ts, err := r.raftTruncatedState()
		if err != nil {
			return 0, err
		}
		if i == ts.Index {
			return ts.Term, nil
		}
		return 0, raft.ErrUnavailable
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
func (r *Replica) raftTruncatedState() (proto.RaftTruncatedState, error) {
	ts := proto.RaftTruncatedState{}
	ok, err := engine.MVCCGetProto(r.rm.Engine(), keys.RaftTruncatedStateKey(r.Desc().RangeID),
		proto.ZeroTimestamp, true, nil, &ts)
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
		proto.ZeroTimestamp, true, nil)
	if err != nil {
		return 0, err
	}
	if v != nil {
		_, appliedIndex = encoding.DecodeUint64(v.Bytes)
	}
	return appliedIndex, nil
}

// setAppliedIndex persists a new applied index.
func setAppliedIndex(eng engine.Engine, rangeID proto.RangeID, appliedIndex uint64) error {
	return engine.MVCCPut(eng, nil, /* stats */
		keys.RaftAppliedIndexKey(rangeID),
		proto.ZeroTimestamp,
		proto.Value{Bytes: encoding.EncodeUint64(nil, appliedIndex)},
		nil /* txn */)
}

// loadLastIndex retrieves the last index from storage.
func (r *Replica) loadLastIndex() (uint64, error) {
	lastIndex := uint64(0)
	v, _, err := engine.MVCCGet(r.rm.Engine(),
		keys.RaftLastIndexKey(r.Desc().RangeID),
		proto.ZeroTimestamp, true /* consistent */, nil)
	if err != nil {
		return 0, err
	}
	if v != nil {
		_, lastIndex = encoding.DecodeUint64(v.Bytes)
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
func setLastIndex(eng engine.Engine, rangeID proto.RangeID, lastIndex uint64) error {
	return engine.MVCCPut(eng, nil, keys.RaftLastIndexKey(rangeID),
		proto.ZeroTimestamp, proto.Value{
			Bytes: encoding.EncodeUint64(nil, lastIndex),
		}, nil)
}

// Snapshot implements the raft.Storage interface.
func (r *Replica) Snapshot() (raftpb.Snapshot, error) {
	// Copy all the data from a consistent RocksDB snapshot into a RaftSnapshotData.
	snap := r.rm.NewSnapshot()
	defer snap.Close()
	var snapData proto.RaftSnapshotData

	// Read the range metadata from the snapshot instead of the members
	// of the Range struct because they might be changed concurrently.
	appliedIndex, err := r.loadAppliedIndex(snap)
	if err != nil {
		return raftpb.Snapshot{}, err
	}

	curDesc := r.Desc()
	var desc proto.RangeDescriptor
	// We ignore intents on the range descriptor (consistent=false) because we
	// know they cannot be committed yet; operations that modify range
	// descriptors resolve their own intents when they commit.
	ok, err := engine.MVCCGetProto(snap, keys.RangeDescriptorKey(curDesc.StartKey),
		r.rm.Clock().Now(), false /* !consistent */, nil, &desc)
	if err != nil {
		return raftpb.Snapshot{}, util.Errorf("failed to get desc: %s", err)
	}
	if !ok {
		return raftpb.Snapshot{}, util.Errorf("couldn't find range descriptor")
	}

	// Store RangeDescriptor as metadata, it will be retrieved by ApplySnapshot()
	snapData.RangeDescriptor = desc

	// Iterate over all the data in the range, including local-only data like
	// the response cache.
	for iter := newRangeDataIterator(curDesc, snap); iter.Valid(); iter.Next() {
		snapData.KV = append(snapData.KV,
			&proto.RaftSnapshotData_KeyValue{Key: iter.Key(), Value: iter.Value()})
	}

	data, err := gogoproto.Marshal(&snapData)
	if err != nil {
		return raftpb.Snapshot{}, err
	}

	// Synthesize our raftpb.ConfState from desc.
	var cs raftpb.ConfState
	for _, rep := range desc.Replicas {
		cs.Nodes = append(cs.Nodes, uint64(proto.MakeRaftNodeID(rep.NodeID, rep.StoreID)))
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

// Append implements the multiraft.WriteableGroupStorage interface.
func (r *Replica) Append(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	batch := r.rm.Engine().NewBatch()
	defer batch.Close()

	rangeID := r.Desc().RangeID

	for _, ent := range entries {
		err := engine.MVCCPutProto(batch, nil, keys.RaftLogKey(rangeID, ent.Index),
			proto.ZeroTimestamp, nil, &ent)
		if err != nil {
			return err
		}
	}
	lastIndex := entries[len(entries)-1].Index
	prevLastIndex := atomic.LoadUint64(&r.lastIndex)
	// Delete any previously appended log entries which never committed.
	for i := lastIndex + 1; i <= prevLastIndex; i++ {
		err := engine.MVCCDelete(batch, nil,
			keys.RaftLogKey(rangeID, i), proto.ZeroTimestamp, nil)
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
	cfg, err := r.rm.Gossip().GetSystemConfig()
	if err != nil {
		return err
	}

	// Find zone config for this range.
	zone, err := cfg.GetZoneConfigForKey(r.Desc().StartKey)
	if err != nil {
		return util.Errorf("failed to lookup zone config for Range %s: %s", r, err)
	}

	r.SetMaxBytes(zone.RangeMaxBytes)
	return nil
}

// ApplySnapshot implements the multiraft.WriteableGroupStorage interface.
func (r *Replica) ApplySnapshot(snap raftpb.Snapshot) error {
	snapData := proto.RaftSnapshotData{}
	err := gogoproto.Unmarshal(snap.Data, &snapData)
	if err != nil {
		return err
	}

	rangeID := r.Desc().RangeID

	// First, save the HardState.  The HardState must not be changed
	// because it may record a previous vote cast by this node.
	hardStateKey := keys.RaftHardStateKey(rangeID)
	hardState, _, err := engine.MVCCGet(r.rm.Engine(), hardStateKey, proto.ZeroTimestamp, true /* consistent */, nil)
	if err != nil {
		return err
	}

	// Extract the updated range descriptor.
	desc := snapData.RangeDescriptor

	batch := r.rm.Engine().NewBatch()
	defer batch.Close()

	// Delete everything in the range and recreate it from the snapshot.
	for iter := newRangeDataIterator(&desc, r.rm.Engine()); iter.Valid(); iter.Next() {
		if err := batch.Clear(iter.Key()); err != nil {
			return err
		}
	}

	// Write the snapshot into the range.
	for _, kv := range snapData.KV {
		if err := batch.Put(kv.Key, kv.Value); err != nil {
			return err
		}
	}

	// Restore the saved HardState.
	if hardState == nil {
		err := engine.MVCCDelete(batch, nil, hardStateKey, proto.ZeroTimestamp, nil)
		if err != nil {
			return err
		}
	} else {
		err := engine.MVCCPut(batch, nil, hardStateKey, proto.ZeroTimestamp, *hardState, nil)
		if err != nil {
			return err
		}
	}

	// Read the leader lease.
	lease, err := loadLeaderLease(batch, desc.RangeID)
	if err != nil {
		return err
	}

	// Copy range stats to new range.
	oldStats := r.stats
	r.stats, err = newRangeStats(desc.RangeID, batch)
	if err != nil {
		r.stats = oldStats
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

	// As outlined above, last and applied index are the same after applying
	// the snapshot.
	atomic.StoreUint64(&r.lastIndex, snap.Metadata.Index)
	atomic.StoreUint64(&r.appliedIndex, snap.Metadata.Index)

	// Atomically update the descriptor and lease.
	if err := r.setDesc(&desc); err != nil {
		return err
	}
	// Update other fields which are uninitialized or need updating.
	if err := r.updateRangeInfo(); err != nil {
		return err
	}

	atomic.StorePointer(&r.lease, unsafe.Pointer(lease))
	return nil
}

// SetHardState implements the multiraft.WriteableGroupStorage interface.
func (r *Replica) SetHardState(st raftpb.HardState) error {
	return engine.MVCCPutProto(r.rm.Engine(), nil, keys.RaftHardStateKey(r.Desc().RangeID),
		proto.ZeroTimestamp, nil, &st)
}
