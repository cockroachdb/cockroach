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

package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/rditer"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// replicaRaftStorage implements the raft.Storage interface.
type replicaRaftStorage Replica

var _ raft.Storage = (*replicaRaftStorage)(nil)

// All calls to raft.RawNode require that both Replica.raftMu and
// Replica.mu are held. All of the functions exposed via the
// raft.Storage interface will in turn be called from RawNode, so none
// of these methods may acquire either lock, but they may require
// their caller to hold one or both locks (even though they do not
// follow our "Locked" naming convention). Specific locking
// requirements are noted in each method's comments.
//
// Many of the methods defined in this file are wrappers around static
// functions. This is done to facilitate their use from
// Replica.Snapshot(), where it is important that all the data that
// goes into the snapshot comes from a consistent view of the
// database, and not the replica's in-memory state or via a reference
// to Replica.store.Engine().

// InitialState implements the raft.Storage interface.
// InitialState requires that r.mu is held.
func (r *replicaRaftStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	ctx := r.AnnotateCtx(context.TODO())
	hs, err := r.mu.stateLoader.LoadHardState(ctx, r.store.Engine())
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
// maxBytes. Passing maxBytes equal to zero disables size checking. Sideloaded
// proposals count towards maxBytes with their payloads inlined.
func (r *replicaRaftStorage) Entries(lo, hi, maxBytes uint64) ([]raftpb.Entry, error) {
	readonly := r.store.Engine().NewReadOnly()
	defer readonly.Close()
	ctx := r.AnnotateCtx(context.TODO())
	return entries(ctx, r.mu.stateLoader, readonly, r.RangeID, r.store.raftEntryCache,
		r.raftMu.sideloaded, lo, hi, maxBytes)
}

// raftEntriesLocked requires that r.mu is held.
func (r *Replica) raftEntriesLocked(lo, hi, maxBytes uint64) ([]raftpb.Entry, error) {
	return (*replicaRaftStorage)(r).Entries(lo, hi, maxBytes)
}

// entries retrieves entries from the engine. To accommodate loading the term,
// `sideloaded` can be supplied as nil, in which case sideloaded entries will
// not be inlined, the raft entry cache will not be populated with *any* of the
// loaded entries, and maxBytes will not be applied to the payloads.
func entries(
	ctx context.Context,
	rsl stateloader.StateLoader,
	e engine.Reader,
	rangeID roachpb.RangeID,
	eCache *raftEntryCache,
	sideloaded sideloadStorage,
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

	// Whether we can populate the Raft entries cache. False if we found a
	// sideloaded proposal, but the caller didn't give us a sideloaded storage.
	canCache := true

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

		if sniffSideloadedRaftCommand(ent.Data) {
			canCache = canCache && sideloaded != nil
			if sideloaded != nil {
				newEnt, err := maybeInlineSideloadedRaftCommand(
					ctx, rangeID, ent, sideloaded, eCache,
				)
				if err != nil {
					return true, err
				}
				if newEnt != nil {
					ent = *newEnt
				}
			}
		}

		// Note that we track the size of proposals with payloads inlined.
		size += uint64(ent.Size())

		ents = append(ents, ent)
		exceededMaxBytes = maxBytes > 0 && size > maxBytes
		return exceededMaxBytes, nil
	}

	if err := iterateEntries(ctx, e, rangeID, expectedIndex, hi, scanFunc); err != nil {
		return nil, err
	}
	// Cache the fetched entries, if we may.
	if canCache {
		eCache.addEntries(rangeID, ents)
	}

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
		lastIndex, err := rsl.LoadLastIndex(ctx, e)
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
	ts, err := rsl.LoadTruncatedState(ctx, e)
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
		false, /* tombstones */
		nil,   /* txn */
		false, /* reverse */
		scanFunc,
	)
	return err
}

// invalidLastTerm is an out-of-band value for r.mu.lastTerm that
// invalidates lastTerm caching and forces retrieval of Term(lastTerm)
// from the raftEntryCache/RocksDB.
const invalidLastTerm = 0

// Term implements the raft.Storage interface.
func (r *replicaRaftStorage) Term(i uint64) (uint64, error) {
	// TODO(nvanbenschoten): should we set r.mu.lastTerm when
	//   r.mu.lastIndex == i && r.mu.lastTerm == invalidLastTerm?
	if r.mu.lastIndex == i && r.mu.lastTerm != invalidLastTerm {
		return r.mu.lastTerm, nil
	}
	// Try to retrieve the term for the desired entry from the entry cache.
	if term, ok := r.store.raftEntryCache.getTerm(r.RangeID, i); ok {
		return term, nil
	}
	readonly := r.store.Engine().NewReadOnly()
	defer readonly.Close()
	ctx := r.AnnotateCtx(context.TODO())
	return term(ctx, r.mu.stateLoader, readonly, r.RangeID, r.store.raftEntryCache, i)
}

// raftTermLocked requires that r.mu is locked for reading.
func (r *Replica) raftTermRLocked(i uint64) (uint64, error) {
	return (*replicaRaftStorage)(r).Term(i)
}

func term(
	ctx context.Context,
	rsl stateloader.StateLoader,
	eng engine.Reader,
	rangeID roachpb.RangeID,
	eCache *raftEntryCache,
	i uint64,
) (uint64, error) {
	// entries() accepts a `nil` sideloaded storage and will skip inlining of
	// sideloaded entries. We only need the term, so this is what we do.
	ents, err := entries(ctx, rsl, eng, rangeID, eCache, nil /* sideloaded */, i, i+1, 0)
	if err == raft.ErrCompacted {
		ts, err := rsl.LoadTruncatedState(ctx, eng)
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
func (r *replicaRaftStorage) LastIndex() (uint64, error) {
	return r.mu.lastIndex, nil
}

// raftLastIndexLocked requires that r.mu is held.
func (r *Replica) raftLastIndexLocked() (uint64, error) {
	return (*replicaRaftStorage)(r).LastIndex()
}

// raftTruncatedStateLocked returns metadata about the log that preceded the
// first current entry. This includes both entries that have been compacted away
// and the dummy entries that make up the starting point of an empty log.
// raftTruncatedStateLocked requires that r.mu is held.
func (r *Replica) raftTruncatedStateLocked(
	ctx context.Context,
) (roachpb.RaftTruncatedState, error) {
	if r.mu.state.TruncatedState != nil {
		return *r.mu.state.TruncatedState, nil
	}
	ts, err := r.mu.stateLoader.LoadTruncatedState(ctx, r.store.Engine())
	if err != nil {
		return ts, err
	}
	if ts.Index != 0 {
		r.mu.state.TruncatedState = &ts
	}
	return ts, nil
}

// FirstIndex implements the raft.Storage interface.
func (r *replicaRaftStorage) FirstIndex() (uint64, error) {
	ctx := r.AnnotateCtx(context.TODO())
	ts, err := (*Replica)(r).raftTruncatedStateLocked(ctx)
	if err != nil {
		return 0, err
	}
	return ts.Index + 1, nil
}

// raftFirstIndexLocked requires that r.mu is held.
func (r *Replica) raftFirstIndexLocked() (uint64, error) {
	return (*replicaRaftStorage)(r).FirstIndex()
}

// GetFirstIndex is the same function as raftFirstIndexLocked but it requires
// that r.mu is not held.
func (r *Replica) GetFirstIndex() (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.raftFirstIndexLocked()
}

// Snapshot implements the raft.Storage interface. Snapshot requires that
// r.mu is held. Note that the returned snapshot is a placeholder and
// does not contain any of the replica data. The snapshot is actually generated
// (and sent) by the Raft snapshot queue.
func (r *replicaRaftStorage) Snapshot() (raftpb.Snapshot, error) {
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

// raftSnapshotLocked requires that r.mu is held.
func (r *Replica) raftSnapshotLocked() (raftpb.Snapshot, error) {
	return (*replicaRaftStorage)(r).Snapshot()
}

// GetSnapshot returns a snapshot of the replica appropriate for sending to a
// replica. If this method returns without error, callers must eventually call
// OutgoingSnapshot.Close.
func (r *Replica) GetSnapshot(
	ctx context.Context, snapType string,
) (_ *OutgoingSnapshot, err error) {
	// Get a snapshot while holding raftMu to make sure we're not seeing "half
	// an AddSSTable" (i.e. a state in which an SSTable has been linked in, but
	// the corresponding Raft command not applied yet).
	r.raftMu.Lock()
	snap := r.store.engine.NewSnapshot()
	r.raftMu.Unlock()

	defer func() {
		if err != nil {
			snap.Close()
		}
	}()

	r.mu.RLock()
	defer r.mu.RUnlock()
	rangeID := r.RangeID

	startKey := r.mu.state.Desc.StartKey
	ctx, sp := r.AnnotateCtxWithSpan(ctx, "snapshot")
	defer sp.Finish()

	log.Eventf(ctx, "new engine snapshot for replica %s", r)

	// Delegate to a static function to make sure that we do not depend
	// on any indirect calls to r.store.Engine() (or other in-memory
	// state of the Replica). Everything must come from the snapshot.
	withSideloaded := func(fn func(sideloadStorage) error) error {
		r.raftMu.Lock()
		defer r.raftMu.Unlock()
		return fn(r.raftMu.sideloaded)
	}
	// NB: We have Replica.mu read-locked, but we need it write-locked in order
	// to use Replica.mu.stateLoader. This call is not performance sensitive, so
	// create a new state loader.
	snapData, err := snapshot(
		ctx, stateloader.Make(r.store.cfg.Settings, rangeID), snapType,
		snap, rangeID, r.store.raftEntryCache, withSideloaded, startKey,
	)
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
	Iter *rditer.ReplicaDataIterator
	// The replica state within the snapshot.
	State storagebase.ReplicaState
	// Allows access the the original Replica's sideloaded storage. Note that
	// this isn't a snapshot of the sideloaded storage congruent with EngineSnap
	// or RaftSnap -- a log truncation could have removed files from the
	// sideloaded storage in the meantime.
	WithSideloaded func(func(sideloadStorage) error) error
	RaftEntryCache *raftEntryCache
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
	rsl stateloader.StateLoader,
	snapType string,
	snap engine.Reader,
	rangeID roachpb.RangeID,
	eCache *raftEntryCache,
	withSideloaded func(func(sideloadStorage) error) error,
	startKey roachpb.RKey,
) (OutgoingSnapshot, error) {
	var desc roachpb.RangeDescriptor
	// We ignore intents on the range descriptor (consistent=false) because we
	// know they cannot be committed yet; operations that modify range
	// descriptors resolve their own intents when they commit.
	ok, err := engine.MVCCGetProto(ctx, snap, keys.RangeDescriptorKey(startKey),
		hlc.MaxTimestamp, false /* consistent */, nil, &desc)
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
	appliedIndex, _, err := rsl.LoadAppliedIndex(ctx, snap)
	if err != nil {
		return OutgoingSnapshot{}, err
	}

	// Synthesize our raftpb.ConfState from desc.
	var cs raftpb.ConfState
	for _, rep := range desc.Replicas {
		cs.Nodes = append(cs.Nodes, uint64(rep.ReplicaID))
	}

	term, err := term(ctx, rsl, snap, rangeID, eCache, appliedIndex)
	if err != nil {
		return OutgoingSnapshot{}, errors.Errorf("failed to fetch term of %d: %s", appliedIndex, err)
	}

	state, err := rsl.Load(ctx, snap, &desc)
	if err != nil {
		return OutgoingSnapshot{}, err
	}

	// Intentionally let this iterator and the snapshot escape so that the
	// streamer can send chunks from it bit by bit.
	iter := rditer.NewReplicaDataIterator(&desc, snap, true /* replicatedOnly */)
	snapUUID := uuid.MakeV4()

	log.Infof(ctx, "generated %s snapshot %s at index %d",
		snapType, snapUUID.Short(), appliedIndex)
	return OutgoingSnapshot{
		RaftEntryCache: eCache,
		WithSideloaded: withSideloaded,
		EngineSnap:     snap,
		Iter:           iter,
		State:          state,
		SnapUUID:       snapUUID,
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
// r.mu.lastIndex, r.mu.lastTerm, and r.mu.raftLogSize, and returns new values.
// We do this rather than modifying them directly because these modifications
// need to be atomic with the commit of the batch. This method requires that
// r.raftMu is held.
//
// append is intentionally oblivious to the existence of sideloaded proposals.
// They are managed by the caller, including cleaning up obsolete on-disk
// payloads in case the log tail is replaced.
func (r *Replica) append(
	ctx context.Context,
	batch engine.ReadWriter,
	prevLastIndex uint64,
	prevLastTerm uint64,
	prevRaftLogSize int64,
	entries []raftpb.Entry,
) (uint64, uint64, int64, error) {
	if len(entries) == 0 {
		return prevLastIndex, prevLastTerm, prevRaftLogSize, nil
	}
	var diff enginepb.MVCCStats
	var value roachpb.Value
	for i := range entries {
		ent := &entries[i]
		key := r.raftMu.stateLoader.RaftLogKey(ent.Index)

		if err := value.SetProto(ent); err != nil {
			return 0, 0, 0, err
		}
		value.InitChecksum(key)
		var err error
		if ent.Index > prevLastIndex {
			err = engine.MVCCBlindPut(ctx, batch, &diff, key, hlc.Timestamp{}, value, nil /* txn */)
		} else {
			err = engine.MVCCPut(ctx, batch, &diff, key, hlc.Timestamp{}, value, nil /* txn */)
		}
		if err != nil {
			return 0, 0, 0, err
		}
	}

	// Delete any previously appended log entries which never committed.
	lastIndex := entries[len(entries)-1].Index
	lastTerm := entries[len(entries)-1].Term
	for i := lastIndex + 1; i <= prevLastIndex; i++ {
		// Note that the caller is in charge of deleting any sideloaded payloads
		// (which they must only do *after* the batch has committed).
		err := engine.MVCCDelete(ctx, batch, &diff, r.raftMu.stateLoader.RaftLogKey(i),
			hlc.Timestamp{}, nil /* txn */)
		if err != nil {
			return 0, 0, 0, err
		}
	}

	if err := r.raftMu.stateLoader.SetLastIndex(ctx, batch, lastIndex); err != nil {
		return 0, 0, 0, err
	}

	raftLogSize := prevRaftLogSize + diff.SysBytes

	return lastIndex, lastTerm, raftLogSize, nil
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

func clearRangeData(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	keyCount int64,
	eng engine.Engine,
	batch engine.Batch,
) error {
	iter := eng.NewIterator(false)
	defer iter.Close()

	// It is expensive for there to be many range deletion tombstones in the same
	// sstable because all of the tombstones in an sstable are loaded whenever the
	// sstable is accessed. So we avoid using range deletion unless there is some
	// minimum number of keys. The value here was pulled out of thin air. It might
	// be better to make this dependent on the size of the data being deleted. Or
	// perhaps we should fix RocksDB to handle large numbers of tombstones in an
	// sstable better.
	const clearRangeMinKeys = 64
	const metadataRanges = 2
	for i, keyRange := range rditer.MakeAllKeyRanges(desc) {
		// Metadata ranges always have too few keys to justify ClearRange (see
		// above), but the data range's key count needs to be explicitly checked.
		var err error
		if i >= metadataRanges && keyCount >= clearRangeMinKeys {
			err = batch.ClearRange(keyRange.Start, keyRange.End)
		} else {
			err = batch.ClearIterRange(iter, keyRange.Start, keyRange.End)
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
// This method requires that r.raftMu is held.
func (r *Replica) applySnapshot(
	ctx context.Context, inSnap IncomingSnapshot, snap raftpb.Snapshot, hs raftpb.HardState,
) (err error) {
	s := *inSnap.State
	if s.Desc.RangeID != r.RangeID {
		log.Fatalf(ctx, "unexpected range ID %d", s.Desc.RangeID)
	}

	r.mu.RLock()
	replicaID := r.mu.replicaID
	keyCount := r.mu.state.Stats.KeyCount
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

	// Before clearing out the range, grab any existing legacy Raft tombstone
	// since we have to write it back later.
	var existingLegacyTombstone *roachpb.RaftTombstone
	{
		legacyTombstoneKey := keys.RaftTombstoneIncorrectLegacyKey(r.RangeID)
		var tomb roachpb.RaftTombstone
		// Intentionally read from the engine to avoid the write-only batch (this is
		// allowed since raftMu is held).
		ok, err := engine.MVCCGetProto(
			ctx, r.store.Engine(), legacyTombstoneKey, hlc.Timestamp{}, true /* consistent */, nil, /* txn */
			&tomb)
		if err != nil {
			return err
		}
		if ok {
			existingLegacyTombstone = &tomb
		}
	}

	// Delete everything in the range and recreate it from the snapshot.
	// We need to delete any old Raft log entries here because any log entries
	// that predate the snapshot will be orphaned and never truncated or GC'd.
	if err := clearRangeData(ctx, s.Desc, keyCount, r.store.Engine(), batch); err != nil {
		return err
	}
	stats.clear = timeutil.Now()

	// Write the snapshot into the range.
	for _, batchRepr := range inSnap.Batches {
		if err := batch.ApplyBatchRepr(batchRepr, false); err != nil {
			return err
		}
	}

	// Nodes before v2.0 may send an incorrect Raft tombstone (see #12154) that
	// was supposed to be unreplicated. Simply remove it, but note how we go
	// through some trouble to ensure that our potential own tombstone survives
	// the snapshot application: we can't have a preemptive snapshot (which may
	// not be followed by a corresponding replication change) wipe out our Raft
	// tombstone.
	//
	// NB: this can be removed in v2.1. This is because when we are running a
	// binary at v2.1, we know that peers are at least running v2.0, which will
	// never send out these snapshots.
	if err := clearLegacyTombstone(batch, r.RangeID); err != nil {
		return errors.Wrap(err, "while clearing legacy tombstone key")
	}

	// If before this snapshot there was a legacy tombstone, it was removed and
	// we must issue a replacement. Note that we *could* check the cluster version
	// and write a new-style tombstone here, but that is more complicated as we'd
	// need to incorporate any existing new-style tombstone in the update. It's
	// more straightforward to propagate the legacy tombstone; there's no
	// requirement that snapshots participate in the migration.
	if existingLegacyTombstone != nil {
		err := engine.MVCCPutProto(
			ctx, batch, nil /* ms */, keys.RaftTombstoneIncorrectLegacyKey(r.RangeID), hlc.Timestamp{}, nil /* txn */, existingLegacyTombstone,
		)
		if err != nil {
			return err
		}
	}

	// The log entries are all written to distinct keys so we can use a
	// distinct batch.
	distinctBatch := batch.Distinct()
	stats.batch = timeutil.Now()

	logEntries := make([]raftpb.Entry, len(inSnap.LogEntries))
	for i, bytes := range inSnap.LogEntries {
		if err := protoutil.Unmarshal(bytes, &logEntries[i]); err != nil {
			return err
		}
	}
	// If this replica doesn't know its ReplicaID yet, we're applying a
	// preemptive snapshot. In this case, we're going to have to write the
	// sideloaded proposals into the Raft log. Otherwise, sideload.
	var raftLogSize int64
	thinEntries := logEntries
	if replicaID != 0 {
		var err error
		var sideloadedEntriesSize int64
		thinEntries, sideloadedEntriesSize, err = r.maybeSideloadEntriesRaftMuLocked(ctx, logEntries)
		if err != nil {
			return err
		}
		raftLogSize += sideloadedEntriesSize
	}

	// Write the snapshot's Raft log into the range.
	var lastTerm uint64
	_, lastTerm, raftLogSize, err = r.append(
		ctx, distinctBatch, 0, invalidLastTerm, raftLogSize, thinEntries,
	)
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
	//
	// Note that since this snapshot comes from Raft, we don't have to synthesize
	// the HardState -- Raft wouldn't ask us to update the HardState in incorrect
	// ways.
	if !raft.IsEmptyHardState(hs) {
		if err := r.raftMu.stateLoader.SetHardState(ctx, distinctBatch, hs); err != nil {
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
	if err := batch.Commit(syncRaftLog.Get(&r.store.cfg.Settings.SV)); err != nil {
		return err
	}
	stats.commit = timeutil.Now()

	r.mu.Lock()
	// We set the persisted last index to the last applied index. This is
	// not a correctness issue, but means that we may have just transferred
	// some entries we're about to re-request from the leader and overwrite.
	// However, raft.MultiNode currently expects this behavior, and the
	// performance implications are not likely to be drastic. If our
	// feelings about this ever change, we can add a LastIndex field to
	// raftpb.SnapshotMetadata.
	r.mu.lastIndex = s.RaftAppliedIndex
	r.mu.lastTerm = lastTerm
	r.mu.raftLogSize = raftLogSize
	// Update the range and store stats.
	r.store.metrics.subtractMVCCStats(*r.mu.state.Stats)
	r.store.metrics.addMVCCStats(*s.Stats)
	r.mu.state = s
	r.assertStateLocked(ctx, r.store.Engine())
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

type raftCommandEncodingVersion byte

// Raft commands are encoded with a 1-byte version (currently 0 or 1), an 8-byte
// ID, followed by the payload. This inflexible encoding is used so we can
// efficiently parse the command id while processing the logs.
//
// TODO(bdarnell): is this commandID still appropriate for our needs?
const (
	// The prescribed length for each command ID.
	raftCommandIDLen = 8
	// The initial Raft command version, used for all regular Raft traffic.
	raftVersionStandard raftCommandEncodingVersion = 0
	// A proposal containing an SSTable which preferably should be sideloaded
	// (i.e. not stored in the Raft log wholesale). Can be treated as a regular
	// proposal when arriving on the wire, but when retrieved from the local
	// Raft log it necessary to inline the payload first as it has usually
	// been sideloaded.
	raftVersionSideloaded raftCommandEncodingVersion = 1
	// The no-split bit is now unused, but we still apply the mask to the first
	// byte of the command for backward compatibility.
	//
	// TODO(tschottdorf): predates v1.0 by a significant margin. Remove.
	raftCommandNoSplitBit  = 1 << 7
	raftCommandNoSplitMask = raftCommandNoSplitBit - 1
)

func encodeRaftCommandV1(commandID storagebase.CmdIDKey, command []byte) []byte {
	return encodeRaftCommand(raftVersionStandard, commandID, command)
}

func encodeRaftCommandV2(commandID storagebase.CmdIDKey, command []byte) []byte {
	return encodeRaftCommand(raftVersionSideloaded, commandID, command)
}

// encode a command ID, an encoded storagebase.RaftCommand, and
// whether the command contains a split.
func encodeRaftCommand(
	version raftCommandEncodingVersion, commandID storagebase.CmdIDKey, command []byte,
) []byte {
	if len(commandID) != raftCommandIDLen {
		panic(fmt.Sprintf("invalid command ID length; %d != %d", len(commandID), raftCommandIDLen))
	}
	x := make([]byte, 1, 1+raftCommandIDLen+len(command))
	x[0] = byte(version)
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
	v := raftCommandEncodingVersion(data[0] & raftCommandNoSplitMask)
	if v != raftVersionStandard && v != raftVersionSideloaded {
		panic(fmt.Sprintf("unknown command encoding version %v", data[0]))
	}
	return storagebase.CmdIDKey(data[1 : 1+raftCommandIDLen]), data[1+raftCommandIDLen:]
}
