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
	"fmt"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

var _ raft.Storage = (*Replica)(nil)

// All calls to raft.RawNode require that an exclusive lock is held.
// All of the functions exposed via the raft.Storage interface will in
// turn be called from RawNode. So the lock that guards raftGroup must
// be the same as the lock that guards all the inner fields.
//
// Many of the methods defined in this file are wrappers around static
// functions. This is done to facilitate their use from
// Replica.Snapshot(), where it is important that all the data that
// goes into the snapshot comes from a consistent view of the
// database, and not the replica's in-memory state or via a reference
// to Replica.store.Engine().

// InitialState implements the raft.Storage interface.
// InitialState requires that the replica lock be held.
func (r *Replica) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	ctx := r.AnnotateCtx(context.TODO())
	hs, err := r.stateLoader.loadHardState(ctx, r.store.Engine())
	// For uninitialized ranges, membership is unknown at this point.
	if raft.IsEmptyHardState(hs) || err != nil {
		return raftpb.HardState{}, raftpb.ConfState{}, err
	}
	var cs raftpb.ConfState
	for _, rep := range r.mu.state.Desc.Replicas {
		cs.Nodes = append(cs.Nodes, uint64(rep.ReplicaID))
	}

	return hs, cs, nil
}

// Entries implements the raft.Storage interface. Note that maxBytes is advisory
// and this method will always return at least one entry even if it exceeds
// maxBytes. Passing maxBytes equal to zero disables size checking.
// Entries requires that the replica lock is held.
func (r *Replica) Entries(lo, hi, maxBytes uint64) ([]raftpb.Entry, error) {
	snap := r.store.NewSnapshot()
	defer snap.Close()
	ctx := r.AnnotateCtx(context.TODO())
	return entries(ctx, snap, r.RangeID, r.store.raftEntryCache, lo, hi, maxBytes)
}

func entries(
	ctx context.Context,
	e engine.Reader,
	rangeID roachpb.RangeID,
	eCache *raftEntryCache,
	lo, hi, maxBytes uint64,
) ([]raftpb.Entry, error) {
	if lo > hi {
		return nil, errors.Errorf("lo:%d is greater than hi:%d", lo, hi)
	}

	n := hi - lo
	if n > 100 {
		n = 100
	}
	ents := make([]raftpb.Entry, 0, n)

	ents, size, hitIndex := eCache.getEntries(ents, rangeID, lo, hi, maxBytes)
	// Return results if the correct number of results came back or if
	// we ran into the max bytes limit.
	if uint64(len(ents)) == hi-lo || (maxBytes > 0 && size > maxBytes) {
		return ents, nil
	}

	// Scan over the log to find the requested entries in the range [lo, hi),
	// stopping once we have enough.
	expectedIndex := hitIndex

	var ent raftpb.Entry
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

	if err := iterateEntries(ctx, e, rangeID, expectedIndex, hi, scanFunc); err != nil {
		return nil, err
	}
	// Cache the fetched entries.
	eCache.addEntries(rangeID, ents)

	// Did the correct number of results come back? If so, we're all good.
	if uint64(len(ents)) == hi-lo {
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
		lastIndex, err := loadLastIndex(ctx, e, rangeID)
		if err != nil {
			return nil, err
		}
		if lastIndex <= expectedIndex {
			return nil, raft.ErrUnavailable
		}

		// We have a gap in the record, if so, return a nasty error.
		return nil, errors.Errorf("there is a gap in the index record between lo:%d and hi:%d at index:%d", lo, hi, expectedIndex)
	}

	// No results, was it due to unavailability or truncation?
	ts, err := loadTruncatedState(ctx, e, rangeID)
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

func iterateEntries(
	ctx context.Context,
	e engine.Reader,
	rangeID roachpb.RangeID,
	lo,
	hi uint64,
	scanFunc func(roachpb.KeyValue) (bool, error),
) error {
	_, err := engine.MVCCIterate(
		ctx, e,
		keys.RaftLogKey(rangeID, lo),
		keys.RaftLogKey(rangeID, hi),
		hlc.Timestamp{},
		true,  /* consistent */
		nil,   /* txn */
		false, /* !reverse */
		scanFunc,
	)
	return err
}

// Term implements the raft.Storage interface.
// Term requires that the replica lock is held.
func (r *Replica) Term(i uint64) (uint64, error) {
	snap := r.store.NewSnapshot()
	defer snap.Close()
	ctx := r.AnnotateCtx(context.TODO())
	return term(ctx, snap, r.RangeID, r.store.raftEntryCache, i)
}

func term(
	ctx context.Context, eng engine.Reader, rangeID roachpb.RangeID, eCache *raftEntryCache, i uint64,
) (uint64, error) {
	ents, err := entries(ctx, eng, rangeID, eCache, i, i+1, 0)
	if err == raft.ErrCompacted {
		ts, err := loadTruncatedState(ctx, eng, rangeID)
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
// LastIndex requires that the replica lock is held.
func (r *Replica) LastIndex() (uint64, error) {
	return r.mu.lastIndex, nil
}

// raftTruncatedStateLocked returns metadata about the log that preceded the
// first current entry. This includes both entries that have been compacted away
// and the dummy entries that make up the starting point of an empty log.
// raftTruncatedStateLocked requires that the replica lock be held.
func (r *Replica) raftTruncatedStateLocked(
	ctx context.Context,
) (roachpb.RaftTruncatedState, error) {
	if r.mu.state.TruncatedState != nil {
		return *r.mu.state.TruncatedState, nil
	}
	ts, err := r.stateLoader.loadTruncatedState(ctx, r.store.Engine())
	if err != nil {
		return ts, err
	}
	if ts.Index != 0 {
		r.mu.state.TruncatedState = &ts
	}
	return ts, nil
}

// FirstIndex implements the raft.Storage interface.
// FirstIndex requires that the replica lock is held.
func (r *Replica) FirstIndex() (uint64, error) {
	ctx := r.AnnotateCtx(context.TODO())
	ts, err := r.raftTruncatedStateLocked(ctx)
	if err != nil {
		return 0, err
	}
	return ts.Index + 1, nil
}

// GetFirstIndex is the same function as FirstIndex but it does not
// require that the replica lock is held.
func (r *Replica) GetFirstIndex() (uint64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.FirstIndex()
}

// Snapshot implements the raft.Storage interface. Snapshot requires that the
// replica lock is held. Note that the returned snapshot is a placeholder and
// does not contain any of the replica data. The snapshot is actually generated
// (and sent) by the Raft snapshot queue.
func (r *Replica) Snapshot() (raftpb.Snapshot, error) {
	r.mu.AssertHeld()
	appliedIndex := r.mu.state.RaftAppliedIndex
	term, err := r.Term(appliedIndex)
	if err != nil {
		return raftpb.Snapshot{}, err
	}
	return raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: appliedIndex,
			Term:  term,
		},
	}, nil
}

// GetSnapshot returns a snapshot of the replica appropriate for sending to a
// replica. If this method returns without error, callers must eventually call
// OutgoingSnapshot.Close.
func (r *Replica) GetSnapshot(ctx context.Context, snapType string) (*OutgoingSnapshot, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rangeID := r.RangeID

	if r.exceedsDoubleSplitSizeRLocked() {
		maxBytes := r.mu.maxBytes
		size := r.mu.state.Stats.Total()
		err := errors.Errorf(
			"%s: not generating %s snapshot because replica is too large: %d > 2 * %d",
			r, snapType, size, maxBytes)
		return &OutgoingSnapshot{}, err
	}

	startKey := r.mu.state.Desc.StartKey
	ctx, sp := r.AnnotateCtxWithSpan(ctx, "snapshot")
	defer sp.Finish()
	snap := r.store.NewSnapshot()
	log.Eventf(ctx, "new engine snapshot for replica %s", r)

	// Delegate to a static function to make sure that we do not depend
	// on any indirect calls to r.store.Engine() (or other in-memory
	// state of the Replica). Everything must come from the snapshot.
	snapData, err := snapshot(ctx, snapType, snap, rangeID, r.store.raftEntryCache, startKey)
	if err != nil {
		log.Errorf(ctx, "error generating snapshot: %s", err)
		return nil, err
	}
	return &snapData, nil
}

// OutgoingSnapshot contains the data required to stream a snapshot to a
// recipient. Once one is created, it needs to be closed via Close() to prevent
// resource leakage.
type OutgoingSnapshot struct {
	SnapUUID uuid.UUID
	// The Raft snapshot message to send. Contains SnapUUID as its data.
	RaftSnap raftpb.Snapshot
	// The RocksDB snapshot that will be streamed from.
	EngineSnap engine.Reader
	// The complete range iterator for the snapshot to stream.
	Iter *ReplicaDataIterator
	// The replica state within the snapshot.
	State storagebase.ReplicaState
}

// Close releases the resources associated with the snapshot.
func (s *OutgoingSnapshot) Close() {
	s.Iter.Close()
	s.EngineSnap.Close()
}

// IncomingSnapshot contains the data for an incoming streaming snapshot message.
type IncomingSnapshot struct {
	SnapUUID uuid.UUID
	// The RocksDB BatchReprs that make up this snapshot.
	Batches [][]byte
	// The Raft log entries for this snapshot.
	LogEntries [][]byte
	// The replica state at the time the snapshot was generated (never nil).
	State    *storagebase.ReplicaState
	snapType string
}

// snapshot creates an OutgoingSnapshot containing a rocksdb snapshot for the
// given range. Note that snapshot() is called without Replica.raftMu held.
func snapshot(
	ctx context.Context,
	snapType string,
	snap engine.Reader,
	rangeID roachpb.RangeID,
	eCache *raftEntryCache,
	startKey roachpb.RKey,
) (OutgoingSnapshot, error) {
	var desc roachpb.RangeDescriptor
	// We ignore intents on the range descriptor (consistent=false) because we
	// know they cannot be committed yet; operations that modify range
	// descriptors resolve their own intents when they commit.
	ok, err := engine.MVCCGetProto(ctx, snap, keys.RangeDescriptorKey(startKey),
		hlc.MaxTimestamp, false /* !consistent */, nil, &desc)
	if err != nil {
		return OutgoingSnapshot{}, errors.Errorf("failed to get desc: %s", err)
	}
	if !ok {
		return OutgoingSnapshot{}, errors.Errorf("couldn't find range descriptor")
	}

	var snapData roachpb.RaftSnapshotData
	// Store RangeDescriptor as metadata, it will be retrieved by ApplySnapshot()
	snapData.RangeDescriptor = desc

	// Read the range metadata from the snapshot instead of the members
	// of the Range struct because they might be changed concurrently.
	appliedIndex, _, err := loadAppliedIndex(ctx, snap, rangeID)
	if err != nil {
		return OutgoingSnapshot{}, err
	}

	// Synthesize our raftpb.ConfState from desc.
	var cs raftpb.ConfState
	for _, rep := range desc.Replicas {
		cs.Nodes = append(cs.Nodes, uint64(rep.ReplicaID))
	}

	term, err := term(ctx, snap, rangeID, eCache, appliedIndex)
	if err != nil {
		return OutgoingSnapshot{}, errors.Errorf("failed to fetch term of %d: %s", appliedIndex, err)
	}

	rsl := makeReplicaStateLoader(rangeID)
	state, err := rsl.load(ctx, snap, &desc)
	if err != nil {
		return OutgoingSnapshot{}, err
	}

	// Intentionally let this iterator and the snapshot escape so that the
	// streamer can send chunks from it bit by bit.
	iter := NewReplicaDataIterator(&desc, snap, true /* replicatedOnly */)
	snapUUID := uuid.MakeV4()

	log.Infof(ctx, "generated %s snapshot %s at index %d",
		snapType, snapUUID.Short(), appliedIndex)
	return OutgoingSnapshot{
		EngineSnap: snap,
		Iter:       iter,
		State:      state,
		SnapUUID:   snapUUID,
		RaftSnap: raftpb.Snapshot{
			Data: snapUUID.GetBytes(),
			Metadata: raftpb.SnapshotMetadata{
				Index:     appliedIndex,
				Term:      term,
				ConfState: cs,
			},
		},
	}, nil
}

// append the given entries to the raft log. Takes the previous values of
// r.mu.lastIndex and r.mu.raftLogSize, and returns new values. We do this
// rather than modifying them directly because these modifications need to be
// atomic with the commit of the batch.
func (r *Replica) append(
	ctx context.Context,
	batch engine.ReadWriter,
	prevLastIndex uint64,
	prevRaftLogSize int64,
	entries []raftpb.Entry,
) (uint64, int64, error) {
	if len(entries) == 0 {
		return prevLastIndex, prevRaftLogSize, nil
	}
	var diff enginepb.MVCCStats
	var value roachpb.Value
	for i := range entries {
		ent := &entries[i]
		key := r.stateLoader.RaftLogKey(ent.Index)
		if err := value.SetProto(ent); err != nil {
			return 0, 0, err
		}
		value.InitChecksum(key)
		var err error
		if ent.Index > prevLastIndex {
			err = engine.MVCCBlindPut(ctx, batch, &diff, key, hlc.Timestamp{}, value, nil /* txn */)
		} else {
			err = engine.MVCCPut(ctx, batch, &diff, key, hlc.Timestamp{}, value, nil /* txn */)
		}
		if err != nil {
			return 0, 0, err
		}
	}

	// Delete any previously appended log entries which never committed.
	lastIndex := entries[len(entries)-1].Index
	for i := lastIndex + 1; i <= prevLastIndex; i++ {
		err := engine.MVCCDelete(ctx, batch, &diff, r.stateLoader.RaftLogKey(i),
			hlc.Timestamp{}, nil /* txn */)
		if err != nil {
			return 0, 0, err
		}
	}

	if err := r.stateLoader.setLastIndex(ctx, batch, lastIndex); err != nil {
		return 0, 0, err
	}

	raftLogSize := prevRaftLogSize + diff.SysBytes

	return lastIndex, raftLogSize, nil
}

// updateRangeInfo is called whenever a range is updated by ApplySnapshot
// or is created by range splitting to setup the fields which are
// uninitialized or need updating.
func (r *Replica) updateRangeInfo(desc *roachpb.RangeDescriptor) error {
	// RangeMaxBytes should be updated by looking up Zone Config in two cases:
	// 1. After applying a snapshot, if the zone config was not updated for
	// this key range, then maxBytes of this range will not be updated either.
	// 2. After a new range is created by a split, only copying maxBytes from
	// the original range wont work as the original and new ranges might belong
	// to different zones.
	// Load the system config.
	cfg, ok := r.store.Gossip().GetSystemConfig()
	if !ok {
		// This could be before the system config was ever gossiped,
		// or it expired. Let the gossip callback set the info.
		ctx := r.AnnotateCtx(context.TODO())
		log.Warningf(ctx, "no system config available, cannot determine range MaxBytes")
		return nil
	}

	// Find zone config for this range.
	zone, err := cfg.GetZoneConfigForKey(desc.StartKey)
	if err != nil {
		return errors.Errorf("%s: failed to lookup zone config: %s", r, err)
	}

	r.SetMaxBytes(zone.RangeMaxBytes)
	return nil
}

const (
	snapTypeRaft       = "Raft"
	snapTypePreemptive = "preemptive"
)

func clearRangeData(desc *roachpb.RangeDescriptor, eng engine.Engine, batch engine.Batch) error {
	iter := eng.NewIterator(false)
	defer iter.Close()

	const metadataRanges = 2
	for i, keyRange := range makeAllKeyRanges(desc) {
		// The metadata ranges have a relatively small number of keys making usage
		// of range tombstones (as created by ClearRange) a pessimization.
		var err error
		if i < metadataRanges {
			err = batch.ClearIterRange(iter, keyRange.start, keyRange.end)
		} else {
			err = batch.ClearRange(keyRange.start, keyRange.end)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// applySnapshot updates the replica based on the given snapshot and associated
// HardState (which may be empty, as Raft may apply some snapshots which don't
// require an update to the HardState). All snapshots must pass through Raft
// for correctness, i.e. the parameters to this method must be taken from
// a raft.Ready. It is the caller's responsibility to call
// r.store.processRangeDescriptorUpdate(r) after a successful applySnapshot.
func (r *Replica) applySnapshot(
	ctx context.Context, inSnap IncomingSnapshot, snap raftpb.Snapshot, hs raftpb.HardState,
) (err error) {
	s := *inSnap.State
	if s.Desc.RangeID != r.RangeID {
		log.Fatalf(ctx, "unexpected range ID %d", s.Desc.RangeID)
	}

	r.mu.RLock()
	raftLogSize := r.mu.raftLogSize
	r.mu.RUnlock()

	snapType := inSnap.snapType
	defer func() {
		if err == nil {
			if snapType == snapTypeRaft {
				r.store.metrics.RangeSnapshotsNormalApplied.Inc(1)
			} else {
				r.store.metrics.RangeSnapshotsPreemptiveApplied.Inc(1)
			}
		}
	}()

	if raft.IsEmptySnap(snap) {
		// Raft discarded the snapshot, indicating that our local state is
		// already ahead of what the snapshot provides. But we count it for
		// stats (see the defer above).
		return nil
	}

	var stats struct {
		clear   time.Time
		batch   time.Time
		entries time.Time
		commit  time.Time
	}

	var size int
	for _, b := range inSnap.Batches {
		size += len(b)
	}
	for _, e := range inSnap.LogEntries {
		size += len(e)
	}

	log.Infof(ctx, "applying %s snapshot at index %d "+
		"(id=%s, encoded size=%d, %d rocksdb batches, %d log entries)",
		snapType, snap.Metadata.Index, inSnap.SnapUUID.Short(),
		size, len(inSnap.Batches), len(inSnap.LogEntries))
	defer func(start time.Time) {
		now := timeutil.Now()
		log.Infof(ctx, "applied %s snapshot in %0.0fms [clear=%0.0fms batch=%0.0fms entries=%0.0fms commit=%0.0fms]",
			snapType, now.Sub(start).Seconds()*1000,
			stats.clear.Sub(start).Seconds()*1000,
			stats.batch.Sub(stats.clear).Seconds()*1000,
			stats.entries.Sub(stats.batch).Seconds()*1000,
			stats.commit.Sub(stats.entries).Seconds()*1000)
	}(timeutil.Now())

	// Use a more efficient write-only batch because we don't need to do any
	// reads from the batch.
	batch := r.store.Engine().NewWriteOnlyBatch()
	defer batch.Close()

	// Delete everything in the range and recreate it from the snapshot.
	// We need to delete any old Raft log entries here because any log entries
	// that predate the snapshot will be orphaned and never truncated or GC'd.
	if err := clearRangeData(s.Desc, r.store.Engine(), batch); err != nil {
		return err
	}
	stats.clear = timeutil.Now()

	// Write the snapshot into the range.
	for _, batchRepr := range inSnap.Batches {
		if err := batch.ApplyBatchRepr(batchRepr, false); err != nil {
			return err
		}
	}

	// The log entries are all written to distinct keys so we can use a
	// distinct batch.
	distinctBatch := batch.Distinct()
	stats.batch = timeutil.Now()

	logEntries := make([]raftpb.Entry, len(inSnap.LogEntries))
	for i, bytes := range inSnap.LogEntries {
		if err := logEntries[i].Unmarshal(bytes); err != nil {
			return err
		}
	}
	// Write the snapshot's Raft log into the range.
	_, raftLogSize, err = r.append(ctx, distinctBatch, 0, raftLogSize, logEntries)
	if err != nil {
		return err
	}
	stats.entries = timeutil.Now()

	// Note that we don't require that Raft supply us with a nonempty HardState
	// on a snapshot. We don't want to make that assumption because it's not
	// guaranteed by the contract. Raft *must* send us a HardState when it
	// increases the committed index as a result of the snapshot, but who is to
	// say it isn't going to accept a snapshot which is identical to the current
	// state?
	if !raft.IsEmptyHardState(hs) {
		if err := r.stateLoader.setHardState(ctx, distinctBatch, hs); err != nil {
			return errors.Wrapf(err, "unable to persist HardState %+v", &hs)
		}
	}

	// We need to close the distinct batch and start using the normal batch for
	// the read below.
	distinctBatch.Close()

	// As outlined above, last and applied index are the same after applying
	// the snapshot (i.e. the snapshot has no uncommitted tail).
	if s.RaftAppliedIndex != snap.Metadata.Index {
		log.Fatalf(ctx, "snapshot RaftAppliedIndex %d doesn't match its metadata index %d",
			s.RaftAppliedIndex, snap.Metadata.Index)
	}

	// We've written Raft log entries, so we need to sync the WAL.
	if err := batch.Commit(syncRaftLog.Get()); err != nil {
		return err
	}
	stats.commit = timeutil.Now()

	r.mu.Lock()
	// We set the persisted last index to the last applied index. This is
	// not a correctness issue, but means that we may have just transferred
	// some entries we're about to re-request from the leader and overwrite.
	// However, raft.MultiNode currently expects this behaviour, and the
	// performance implications are not likely to be drastic. If our
	// feelings about this ever change, we can add a LastIndex field to
	// raftpb.SnapshotMetadata.
	r.mu.lastIndex = s.RaftAppliedIndex
	r.mu.raftLogSize = raftLogSize
	// Update the range and store stats.
	r.store.metrics.subtractMVCCStats(r.mu.state.Stats)
	r.store.metrics.addMVCCStats(s.Stats)
	r.mu.state = s
	r.assertStateRLocked(r.store.Engine())
	r.mu.Unlock()

	// As the last deferred action after committing the batch, update other
	// fields which are uninitialized or need updating. This may not happen
	// if the system config has not yet been loaded. While config update
	// will correctly set the fields, there is no order guarantee in
	// ApplySnapshot.
	// TODO: should go through the standard store lock when adding a replica.
	if err := r.updateRangeInfo(s.Desc); err != nil {
		panic(err)
	}

	r.setDescWithoutProcessUpdate(s.Desc)
	return nil
}

// Raft commands are encoded with a 1-byte version (currently 0), an 8-byte ID,
// followed by the payload. This inflexible encoding is used so we can efficiently
// parse the command id while processing the logs.
// TODO(bdarnell): Is this commandID still appropriate for our needs?
const (
	// The prescribed length for each command ID.
	raftCommandIDLen                = 8
	raftCommandEncodingVersion byte = 0
	// The no-split bit is now unused, but we still apply the mask to the first
	// byte of the command for backward compatibility.
	raftCommandNoSplitBit  = 1 << 7
	raftCommandNoSplitMask = raftCommandNoSplitBit - 1
)

// encode a command ID, an encoded storagebase.RaftCommand, and
// whether the command contains a split.
func encodeRaftCommand(commandID storagebase.CmdIDKey, command []byte) []byte {
	if len(commandID) != raftCommandIDLen {
		panic(fmt.Sprintf("invalid command ID length; %d != %d", len(commandID), raftCommandIDLen))
	}
	x := make([]byte, 1, 1+raftCommandIDLen+len(command))
	x[0] = raftCommandEncodingVersion
	x = append(x, []byte(commandID)...)
	x = append(x, command...)
	return x
}

// DecodeRaftCommand splits a raftpb.Entry.Data into its commandID and
// command portions. The caller is responsible for checking that the data
// is not empty (which indicates a dummy entry generated by raft rather
// than a real command). Usage is mostly internal to the storage package
// but is exported for use by debugging tools.
func DecodeRaftCommand(data []byte) (storagebase.CmdIDKey, []byte) {
	if data[0]&raftCommandNoSplitMask != raftCommandEncodingVersion {
		panic(fmt.Sprintf("unknown command encoding version %v", data[0]))
	}
	return storagebase.CmdIDKey(data[1 : 1+raftCommandIDLen]), data[1+raftCommandIDLen:]
}
