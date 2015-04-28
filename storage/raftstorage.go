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

	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	gogoproto "github.com/gogo/protobuf/proto"
)

var _ multiraft.WriteableGroupStorage = &Range{}

// InitialState implements the raft.Storage interface.
func (r *Range) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	var hs raftpb.HardState
	found, err := engine.MVCCGetProto(r.rm.Engine(), engine.RaftHardStateKey(r.Desc().RaftID),
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
		for _, rep := range r.Desc().Replicas {
			cs.Nodes = append(cs.Nodes, uint64(MakeRaftNodeID(rep.NodeID, rep.StoreID)))
		}
	}

	return hs, cs, nil
}

// loadLastIndex looks in the engine to find the last log index.
func (r *Range) loadLastIndex() error {
	logKey := engine.RaftLogPrefix(r.Desc().RaftID)
	// The log keys are encoded in descending order, so the first log
	// entry in the database is the last one that was written.
	kvs, err := engine.MVCCScan(r.rm.Engine(),
		logKey, logKey.PrefixEnd(),
		1, proto.ZeroTimestamp, true, nil)
	if err != nil {
		return err
	}
	if len(kvs) > 0 {
		// The log is non-empty, so use the most recent entry's index.
		// The index is encoded in both the key and the value.
		var lastEnt raftpb.Entry
		err := gogoproto.Unmarshal(kvs[0].Value.Bytes, &lastEnt)
		if err != nil {
			return err
		}
		atomic.StoreUint64(&r.lastIndex, lastEnt.Index)
	} else if len(kvs) == 0 {
		// The log is empty, which means we are either starting from scratch
		// or the entire log has been truncated away. raftTruncatedState
		// handles both cases.
		ts, err := r.raftTruncatedState()
		if err != nil {
			return err
		}
		atomic.StoreUint64(&r.lastIndex, ts.Index)
	}
	return nil
}

// Entries implements the raft.Storage interface. Note that maxBytes is advisory
// and this method will always return at least one entry even if it exceeds
// maxBytes.
// TODO(bdarnell): consider caching for recent entries, if rocksdb's builtin caching
// is insufficient.
func (r *Range) Entries(lo, hi, maxBytes uint64) ([]raftpb.Entry, error) {
	// Scan over the log (which is stored backwards) to find the
	// requested entries. Reversing [lo, hi) gives us (hi, lo]; since
	// MVCCScan is inclusive in the other direction we must increment both the
	// start and end keys.
	kvs, err := engine.MVCCScan(r.rm.Engine(),
		engine.RaftLogKey(r.Desc().RaftID, hi).Next(),
		engine.RaftLogKey(r.Desc().RaftID, lo).Next(),
		0, proto.ZeroTimestamp, true, nil)
	if err != nil {
		return nil, err
	}
	ents := make([]raftpb.Entry, 0, len(kvs))
	for _, kv := range kvs {
		var ent raftpb.Entry
		err = gogoproto.Unmarshal(kv.Value.GetBytes(), &ent)
		if err != nil {
			return nil, err
		}
		ents = append(ents, ent)
	}
	if len(ents) != int(hi-lo) {
		return nil, raft.ErrUnavailable
	}
	// Reverse the log to get it back into the proper order.
	for i, j := 0, len(ents)-1; i < j; i, j = i+1, j-1 {
		ents[i], ents[j] = ents[j], ents[i]
	}

	// TODO(bdarnell): apply the limit earlier instead of after loading everything.
	size := ents[0].Size()
	for i := 1; i < len(ents); i++ {
		size += ents[i].Size()
		if uint64(size) > maxBytes {
			return ents[:i], nil
		}
	}

	return ents, nil
}

// Term implements the raft.Storage interface.
func (r *Range) Term(i uint64) (uint64, error) {
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
func (r *Range) LastIndex() (uint64, error) {
	return atomic.LoadUint64(&r.lastIndex), nil
}

// raftTruncatedState returns metadata about the log that preceded the first
// current entry. This includes both entries that have been compacted away
// and the dummy entries that make up the starting point of an empty log.
func (r *Range) raftTruncatedState() (proto.RaftTruncatedState, error) {
	ts := proto.RaftTruncatedState{}
	ok, err := engine.MVCCGetProto(r.rm.Engine(), engine.RaftTruncatedStateKey(r.Desc().RaftID),
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
func (r *Range) FirstIndex() (uint64, error) {
	ts, err := r.raftTruncatedState()
	if err != nil {
		return 0, err
	}
	return ts.Index + 1, nil
}

func loadAppliedIndex(eng engine.Engine, raftID int64) (uint64, error) {
	appliedIndex := uint64(0)
	v, err := engine.MVCCGet(eng, engine.RaftAppliedIndexKey(raftID),
		proto.ZeroTimestamp, true, nil)
	if err != nil {
		return 0, err
	}
	if v != nil {
		_, appliedIndex = encoding.DecodeUint64(v.Bytes)
	}
	return appliedIndex, nil
}

// Snapshot implements the raft.Storage interface.
func (r *Range) Snapshot() (raftpb.Snapshot, error) {
	// Copy all the data from a consistent RocksDB snapshot into a RaftSnapshotData.
	snap := r.rm.NewSnapshot()
	defer snap.Close()
	var snapData proto.RaftSnapshotData

	// Read the range metadata from the snapshot instead of the members
	// of the Range struct because they might be changed concurrently.
	appliedIndex, err := loadAppliedIndex(snap, r.Desc().RaftID)
	if err != nil {
		return raftpb.Snapshot{}, err
	}
	var desc proto.RangeDescriptor
	// We ignore intents on the range descriptor (consistent=false) because we know
	// they cannot be committed yet. (operations that modify range descriptors resolve their
	// own intents when they commit).
	ok, err := engine.MVCCGetProto(snap, engine.RangeDescriptorKey(r.Desc().StartKey),
		r.rm.Clock().Now(), false, nil, &desc)
	if err != nil {
		return raftpb.Snapshot{}, util.Errorf("failed to get desc: %s", err)
	} else if !ok {
		return raftpb.Snapshot{}, util.Errorf("couldn't find range descriptor")
	}

	// Iterate over all the data in the range, including local-only data like
	// the response cache.
	for iter := newRangeDataIterator(r, snap); iter.Valid(); iter.Next() {
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
		cs.Nodes = append(cs.Nodes, uint64(MakeRaftNodeID(rep.NodeID, rep.StoreID)))
	}

	term, err := r.Term(appliedIndex)
	if err != nil {
		return raftpb.Snapshot{}, err
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
func (r *Range) Append(entries []raftpb.Entry) error {
	batch := r.rm.Engine().NewBatch()
	defer batch.Close()

	for _, ent := range entries {
		err := engine.MVCCPutProto(batch, nil, engine.RaftLogKey(r.Desc().RaftID, ent.Index),
			proto.ZeroTimestamp, nil, &ent)
		if err != nil {
			return err
		}
	}
	// TODO(bdarnell): if the last entry's index < lastIndex, delete any remaining old entries.
	err := batch.Commit()
	if err != nil {
		return err
	}
	atomic.StoreUint64(&r.lastIndex, entries[len(entries)-1].Index)
	return nil
}

// ApplySnapshot implements the multiraft.WriteableGroupStorage interface.
func (r *Range) ApplySnapshot(snap raftpb.Snapshot) error {
	snapData := proto.RaftSnapshotData{}
	err := gogoproto.Unmarshal(snap.Data, &snapData)
	if err != nil {
		return nil
	}

	// First, save the HardState.  The HardState must not be changed
	// because it may record a previous vote cast by this node.
	hardStateKey := engine.RaftHardStateKey(r.Desc().RaftID)
	hardState, err := engine.MVCCGet(r.rm.Engine(), hardStateKey, proto.ZeroTimestamp, true, nil)
	if err != nil {
		return nil
	}

	batch := r.rm.Engine().NewBatch()
	defer batch.Close()

	// Delete everything in the range and recreate it from the snapshot.
	for iter := newRangeDataIterator(r, r.rm.Engine()); iter.Valid(); iter.Next() {
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

	// Read the updated range descriptor.
	var desc proto.RangeDescriptor
	if _, err := engine.MVCCGetProto(batch, engine.RangeDescriptorKey(r.Desc().StartKey),
		r.rm.Clock().Now(), false, nil, &desc); err != nil {
		return err
	}

	// Read the leader lease.
	lease, err := loadLeaderLease(batch, desc.RaftID)
	if err != nil {
		return err
	}

	// Read range stats.
	stats, err := newRangeStats(desc.RaftID, batch)
	if err != nil {
		return err
	}

	if err := batch.Commit(); err != nil {
		return err
	}

	// Save the descriptor, applied index, lease and stats to our member variables.
	r.SetDesc(&desc)
	atomic.StoreUint64(&r.appliedIndex, snap.Metadata.Index)
	atomic.StorePointer(&r.lease, unsafe.Pointer(lease))
	r.stats = stats

	// TODO(bdarnell): extract the real last index.
	// snap.Metadata.Index is the last applied index, but our snapshot may have given us
	// some unapplied entries too. It's safe to set lastIndex too low (the entries will
	// be re-sent), but it would be better to set this to the last entry in the log.
	atomic.StoreUint64(&r.lastIndex, snap.Metadata.Index)
	return err
}

// SetHardState implements the multiraft.WriteableGroupStorage interface.
func (r *Range) SetHardState(st raftpb.HardState) error {
	return engine.MVCCPutProto(r.rm.Engine(), nil, engine.RaftHardStateKey(r.Desc().RaftID),
		proto.ZeroTimestamp, nil, &st)
}
