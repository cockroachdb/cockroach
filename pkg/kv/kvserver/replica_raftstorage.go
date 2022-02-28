// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftentry"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/readsummary"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
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
	cs := r.mu.state.Desc.Replicas().ConfState()
	return hs, cs, nil
}

// Entries implements the raft.Storage interface. Note that maxBytes is advisory
// and this method will always return at least one entry even if it exceeds
// maxBytes. Sideloaded proposals count towards maxBytes with their payloads inlined.
func (r *replicaRaftStorage) Entries(lo, hi, maxBytes uint64) ([]raftpb.Entry, error) {
	readonly := r.store.Engine().NewReadOnly(storage.StandardDurability)
	defer readonly.Close()
	ctx := r.AnnotateCtx(context.TODO())
	if r.raftMu.sideloaded == nil {
		return nil, errors.New("sideloaded storage is uninitialized")
	}
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
	reader storage.Reader,
	rangeID roachpb.RangeID,
	eCache *raftentry.Cache,
	sideloaded SideloadStorage,
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

	ents, size, hitIndex, exceededMaxBytes := eCache.Scan(ents, rangeID, lo, hi, maxBytes)

	// Return results if the correct number of results came back or if
	// we ran into the max bytes limit.
	if uint64(len(ents)) == hi-lo || exceededMaxBytes {
		return ents, nil
	}

	// Scan over the log to find the requested entries in the range [lo, hi),
	// stopping once we have enough.
	expectedIndex := hitIndex

	// Whether we can populate the Raft entries cache. False if we found a
	// sideloaded proposal, but the caller didn't give us a sideloaded storage.
	canCache := true

	scanFunc := func(ent raftpb.Entry) error {
		// Exit early if we have any gaps or it has been compacted.
		if ent.Index != expectedIndex {
			return iterutil.StopIteration()
		}
		expectedIndex++

		if sniffSideloadedRaftCommand(ent.Data) {
			canCache = canCache && sideloaded != nil
			if sideloaded != nil {
				newEnt, err := maybeInlineSideloadedRaftCommand(
					ctx, rangeID, ent, sideloaded, eCache,
				)
				if err != nil {
					return err
				}
				if newEnt != nil {
					ent = *newEnt
				}
			}
		}

		// Note that we track the size of proposals with payloads inlined.
		size += uint64(ent.Size())
		if size > maxBytes {
			exceededMaxBytes = true
			if len(ents) > 0 {
				if exceededMaxBytes {
					return iterutil.StopIteration()
				}
				return nil
			}
		}
		ents = append(ents, ent)
		if exceededMaxBytes {
			return iterutil.StopIteration()
		}
		return nil
	}

	if err := iterateEntries(ctx, reader, rangeID, expectedIndex, hi, scanFunc); err != nil {
		return nil, err
	}
	// Cache the fetched entries, if we may.
	if canCache {
		eCache.Add(rangeID, ents, false /* truncate */)
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
		lastIndex, err := rsl.LoadLastIndex(ctx, reader)
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
	ts, err := rsl.LoadRaftTruncatedState(ctx, reader)
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

// iterateEntries iterates over each of the Raft log entries in the range
// [lo,hi). At each step of the iteration, f() is invoked with the current log
// entry.
//
// The function does not accept a maximum number of entries or bytes. Instead,
// callers should enforce any limits by returning iterutil.StopIteration from
// the iteration function to terminate iteration early, if necessary.
func iterateEntries(
	ctx context.Context,
	reader storage.Reader,
	rangeID roachpb.RangeID,
	lo, hi uint64,
	f func(raftpb.Entry) error,
) error {
	key := keys.RaftLogKey(rangeID, lo)
	endKey := keys.RaftLogKey(rangeID, hi)
	iter := reader.NewMVCCIterator(storage.MVCCKeyIterKind, storage.IterOptions{
		UpperBound: endKey,
	})
	defer iter.Close()

	var meta enginepb.MVCCMetadata
	var ent raftpb.Entry

	iter.SeekGE(storage.MakeMVCCMetadataKey(key))
	for ; ; iter.Next() {
		if ok, err := iter.Valid(); err != nil || !ok {
			return err
		}

		if err := protoutil.Unmarshal(iter.UnsafeValue(), &meta); err != nil {
			return errors.Wrap(err, "unable to decode MVCCMetadata")
		}
		if err := storage.MakeValue(meta).GetProto(&ent); err != nil {
			return errors.Wrap(err, "unable to unmarshal raft Entry")
		}
		if err := f(ent); err != nil {
			if iterutil.Done(err) {
				return nil
			}
			return err
		}
	}
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
	if e, ok := r.store.raftEntryCache.Get(r.RangeID, i); ok {
		return e.Term, nil
	}
	readonly := r.store.Engine().NewReadOnly(storage.StandardDurability)
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
	reader storage.Reader,
	rangeID roachpb.RangeID,
	eCache *raftentry.Cache,
	i uint64,
) (uint64, error) {
	// entries() accepts a `nil` sideloaded storage and will skip inlining of
	// sideloaded entries. We only need the term, so this is what we do.
	ents, err := entries(ctx, rsl, reader, rangeID, eCache, nil /* sideloaded */, i, i+1, math.MaxUint64 /* maxBytes */)
	if errors.Is(err, raft.ErrCompacted) {
		ts, err := rsl.LoadRaftTruncatedState(ctx, reader)
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
	ts, err := r.mu.stateLoader.LoadRaftTruncatedState(ctx, r.store.Engine())
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

// GetLeaseAppliedIndex returns the lease index of the last applied command.
func (r *Replica) GetLeaseAppliedIndex() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.state.LeaseAppliedIndex
}

// Snapshot implements the raft.Storage interface. Snapshot requires that
// r.mu is held. Note that the returned snapshot is a placeholder and
// does not contain any of the replica data. The snapshot is actually generated
// (and sent) by the Raft snapshot queue.
//
// More specifically, this method is called by etcd/raft in
// (*raftLog).snapshot. Raft expects that it generates the snapshot (by
// calling Snapshot) and that "sending" the result actually sends the
// snapshot. In CockroachDB, that message is intercepted (at the sender) and
// instead we add the replica (the raft leader) to the raft snapshot queue,
// and when its turn comes we look at the raft state for followers that want a
// snapshot, and then send one. That actual sending path does not call this
// Snapshot method.
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
	ctx context.Context, snapType kvserverpb.SnapshotRequest_Type, recipientStore roachpb.StoreID,
) (_ *OutgoingSnapshot, err error) {
	snapUUID := uuid.MakeV4()
	// Get a snapshot while holding raftMu to make sure we're not seeing "half
	// an AddSSTable" (i.e. a state in which an SSTable has been linked in, but
	// the corresponding Raft command not applied yet).
	r.raftMu.Lock()
	snap := r.store.engine.NewSnapshot()
	{
		r.mu.Lock()
		// We will fetch the applied index later again, from snap. The
		// appliedIndex fetched here is narrowly used for adding a log truncation
		// constraint to prevent log entries > appliedIndex from being removed.
		// Note that the appliedIndex maintained in Replica actually lags the one
		// in the engine, since replicaAppBatch.ApplyToStateMachine commits the
		// engine batch and then acquires Replica.mu to update
		// Replica.mu.state.RaftAppliedIndex. The use of a possibly stale value
		// here is harmless since using a lower index in this constraint, than the
		// actual snapshot index, preserves more from a log truncation
		// perspective.
		//
		// TODO(sumeer): despite the above justification, this is unnecessarily
		// complicated. Consider loading the RaftAppliedIndex from the snap for
		// this use case.
		appliedIndex := r.mu.state.RaftAppliedIndex
		// Cleared when OutgoingSnapshot closes.
		r.addSnapshotLogTruncationConstraintLocked(ctx, snapUUID, appliedIndex, recipientStore)
		r.mu.Unlock()
	}
	r.raftMu.Unlock()

	release := func() {
		now := timeutil.Now()
		r.completeSnapshotLogTruncationConstraint(ctx, snapUUID, now)
	}

	defer func() {
		if err != nil {
			release()
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
	withSideloaded := func(fn func(SideloadStorage) error) error {
		r.raftMu.Lock()
		defer r.raftMu.Unlock()
		return fn(r.raftMu.sideloaded)
	}
	// NB: We have Replica.mu read-locked, but we need it write-locked in order
	// to use Replica.mu.stateLoader. This call is not performance sensitive, so
	// create a new state loader.
	snapData, err := snapshot(
		ctx, snapUUID, stateloader.Make(rangeID), snapType,
		snap, rangeID, r.store.raftEntryCache, withSideloaded, startKey,
	)
	if err != nil {
		log.Errorf(ctx, "error generating snapshot: %+v", err)
		return nil, err
	}
	snapData.onClose = release
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
	EngineSnap storage.Reader
	// The complete range iterator for the snapshot to stream.
	Iter *rditer.ReplicaEngineDataIterator
	// The replica state within the snapshot.
	State kvserverpb.ReplicaState
	// Allows access the original Replica's sideloaded storage. Note that
	// this isn't a snapshot of the sideloaded storage congruent with EngineSnap
	// or RaftSnap -- a log truncation could have removed files from the
	// sideloaded storage in the meantime.
	WithSideloaded func(func(SideloadStorage) error) error
	RaftEntryCache *raftentry.Cache
	snapType       kvserverpb.SnapshotRequest_Type
	onClose        func()
}

func (s OutgoingSnapshot) String() string {
	return redact.StringWithoutMarkers(s)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (s OutgoingSnapshot) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("%s snapshot %s at applied index %d",
		s.snapType, redact.Safe(s.SnapUUID.Short()), s.State.RaftAppliedIndex)
}

// Close releases the resources associated with the snapshot.
func (s *OutgoingSnapshot) Close() {
	s.Iter.Close()
	s.EngineSnap.Close()
	if s.onClose != nil {
		s.onClose()
	}
}

// IncomingSnapshot contains the data for an incoming streaming snapshot message.
type IncomingSnapshot struct {
	SnapUUID uuid.UUID
	// The storage interface for the underlying SSTs.
	SSTStorageScratch *SSTSnapshotStorageScratch
	FromReplica       roachpb.ReplicaDescriptor
	// The descriptor in the snapshot, never nil.
	Desc             *roachpb.RangeDescriptor
	DataSize         int64
	snapType         kvserverpb.SnapshotRequest_Type
	placeholder      *ReplicaPlaceholder
	raftAppliedIndex uint64 // logging only
}

func (s IncomingSnapshot) String() string {
	return redact.StringWithoutMarkers(s)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (s IncomingSnapshot) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("%s snapshot %s from %s at applied index %d",
		s.snapType, redact.Safe(s.SnapUUID.Short()), s.FromReplica, s.raftAppliedIndex)
}

// snapshot creates an OutgoingSnapshot containing a pebble snapshot for the
// given range. Note that snapshot() is called without Replica.raftMu held.
func snapshot(
	ctx context.Context,
	snapUUID uuid.UUID,
	rsl stateloader.StateLoader,
	snapType kvserverpb.SnapshotRequest_Type,
	snap storage.Reader,
	rangeID roachpb.RangeID,
	eCache *raftentry.Cache,
	withSideloaded func(func(SideloadStorage) error) error,
	startKey roachpb.RKey,
) (OutgoingSnapshot, error) {
	var desc roachpb.RangeDescriptor
	// We ignore intents on the range descriptor (consistent=false) because we
	// know they cannot be committed yet; operations that modify range
	// descriptors resolve their own intents when they commit.
	ok, err := storage.MVCCGetProto(ctx, snap, keys.RangeDescriptorKey(startKey),
		hlc.MaxTimestamp, &desc, storage.MVCCGetOptions{Inconsistent: true})
	if err != nil {
		return OutgoingSnapshot{}, errors.Wrap(err, "failed to get desc")
	}
	if !ok {
		return OutgoingSnapshot{}, errors.Mark(errors.Errorf("couldn't find range descriptor"), errMarkSnapshotError)
	}

	state, err := rsl.Load(ctx, snap, &desc)
	if err != nil {
		return OutgoingSnapshot{}, err
	}

	term, err := term(ctx, rsl, snap, rangeID, eCache, state.RaftAppliedIndex)
	// If we've migrated to populating RaftAppliedIndexTerm, check that the term
	// from the two sources are equal.
	if state.RaftAppliedIndexTerm != 0 && term != state.RaftAppliedIndexTerm {
		return OutgoingSnapshot{},
			errors.AssertionFailedf("unequal terms %d != %d", term, state.RaftAppliedIndexTerm)
	}
	if err != nil {
		return OutgoingSnapshot{}, errors.Wrapf(err, "failed to fetch term of %d", state.RaftAppliedIndex)
	}

	// Intentionally let this iterator and the snapshot escape so that the
	// streamer can send chunks from it bit by bit.
	iter := rditer.NewReplicaEngineDataIterator(&desc, snap, true /* replicatedOnly */)

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
				Index: state.RaftAppliedIndex,
				Term:  term,
				// Synthesize our raftpb.ConfState from desc.
				ConfState: desc.Replicas().ConfState(),
			},
		},
		snapType: snapType,
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
//
// NOTE: This method takes a engine.Writer because reads are unnecessary when
// prevLastIndex is 0 and prevLastTerm is invalidLastTerm. In the case where
// reading is necessary (I.E. entries are getting overwritten or deleted), a
// engine.ReadWriter must be passed in.
func (r *Replica) append(
	ctx context.Context,
	writer storage.Writer,
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
			err = storage.MVCCBlindPut(ctx, writer, &diff, key, hlc.Timestamp{}, value, nil /* txn */)
		} else {
			// We type assert `writer` to also be an engine.ReadWriter only in
			// the case where we're replacing existing entries.
			eng, ok := writer.(storage.ReadWriter)
			if !ok {
				panic("expected writer to be a engine.ReadWriter when overwriting log entries")
			}
			err = storage.MVCCPut(ctx, eng, &diff, key, hlc.Timestamp{}, value, nil /* txn */)
		}
		if err != nil {
			return 0, 0, 0, err
		}
	}

	lastIndex := entries[len(entries)-1].Index
	lastTerm := entries[len(entries)-1].Term
	// Delete any previously appended log entries which never committed.
	if prevLastIndex > 0 {
		// We type assert `writer` to also be an engine.ReadWriter only in the
		// case where we're deleting existing entries.
		eng, ok := writer.(storage.ReadWriter)
		if !ok {
			panic("expected writer to be a engine.ReadWriter when deleting log entries")
		}
		for i := lastIndex + 1; i <= prevLastIndex; i++ {
			// Note that the caller is in charge of deleting any sideloaded payloads
			// (which they must only do *after* the batch has committed).
			err := storage.MVCCDelete(ctx, eng, &diff, r.raftMu.stateLoader.RaftLogKey(i),
				hlc.Timestamp{}, nil /* txn */)
			if err != nil {
				return 0, 0, 0, err
			}
		}
	}

	raftLogSize := prevRaftLogSize + diff.SysBytes
	return lastIndex, lastTerm, raftLogSize, nil
}

// updateRangeInfo is called whenever a range is updated by ApplySnapshot
// or is created by range splitting to setup the fields which are
// uninitialized or need updating.
func (r *Replica) updateRangeInfo(ctx context.Context, desc *roachpb.RangeDescriptor) error {
	// RangeMaxBytes should be updated by looking up Zone Config in two cases:
	// 1. After applying a snapshot, if the zone config was not updated for
	// this key range, then maxBytes of this range will not be updated either.
	// 2. After a new range is created by a split, only copying maxBytes from
	// the original range wont work as the original and new ranges might belong
	// to different zones.
	// Load the system config.
	confReader, err := r.store.GetConfReader(ctx)
	if errors.Is(err, errSysCfgUnavailable) {
		// This could be before the system config was ever gossiped, or it
		// expired. Let the gossip callback set the info.
		log.Warningf(ctx, "unable to retrieve conf reader, cannot determine range MaxBytes")
		return nil
	}
	if err != nil {
		return err
	}

	// Find span config for this range.
	conf, err := confReader.GetSpanConfigForKey(ctx, desc.StartKey)
	if err != nil {
		return errors.Wrapf(err, "%s: failed to lookup span config", r)
	}

	r.SetSpanConfig(conf)
	return nil
}

// clearRangeData clears the data associated with a range descriptor. If
// rangeIDLocalOnly is true, then only the range-id local keys are deleted.
// Otherwise, the range-id local keys, range local keys, and user keys are all
// deleted. If mustClearRange is true, ClearRange will always be used to remove
// the keys. Otherwise, ClearRangeWithHeuristic will be used, which chooses
// ClearRange or ClearIterRange depending on how many keys there are in the
// range.
func clearRangeData(
	desc *roachpb.RangeDescriptor,
	reader storage.Reader,
	writer storage.Writer,
	rangeIDLocalOnly bool,
	mustClearRange bool,
) error {
	var keyRanges []rditer.KeyRange
	if rangeIDLocalOnly {
		keyRanges = []rditer.KeyRange{rditer.MakeRangeIDLocalKeyRange(desc.RangeID, false)}
	} else {
		keyRanges = rditer.MakeAllKeyRanges(desc)
	}
	var clearRangeFn func(storage.Reader, storage.Writer, roachpb.Key, roachpb.Key) error
	if mustClearRange {
		clearRangeFn = func(reader storage.Reader, writer storage.Writer, start, end roachpb.Key) error {
			return writer.ClearRawRange(start, end)
		}
	} else {
		clearRangeFn = storage.ClearRangeWithHeuristic
	}

	for _, keyRange := range keyRanges {
		if err := clearRangeFn(reader, writer, keyRange.Start, keyRange.End); err != nil {
			return err
		}
	}
	return nil
}

// applySnapshot updates the replica and its store based on the given
// (non-empty) snapshot and associated HardState. All snapshots must pass
// through Raft for correctness, i.e. the parameters to this method must be
// taken from a raft.Ready. Any replicas specified in subsumedRepls will be
// destroyed atomically with the application of the snapshot.
//
// If there is a placeholder associated with r, applySnapshot will remove that
// placeholder from the store if and only if it does not return an error.
//
// This method requires that r.raftMu is held, as well as the raftMus of any
// replicas in subsumedRepls.
//
// TODO(benesch): the way this replica method reaches into its store to update
// replicasByKey is unfortunate, but the fix requires a substantial refactor to
// maintain the necessary synchronization.
func (r *Replica) applySnapshot(
	ctx context.Context,
	inSnap IncomingSnapshot,
	nonemptySnap raftpb.Snapshot,
	hs raftpb.HardState,
	subsumedRepls []*Replica,
) (err error) {
	desc := inSnap.Desc
	if desc.RangeID != r.RangeID {
		log.Fatalf(ctx, "unexpected range ID %d", desc.RangeID)
	}

	isInitialSnap := !r.IsInitialized()
	{
		var from, to roachpb.RKey
		if isInitialSnap {
			// For uninitialized replicas, there must be a placeholder that covers
			// the snapshot's bounds, so basically check that. A synchronous check
			// here would be simpler but this works well enough.
			d := inSnap.placeholder.Desc()
			from, to = d.StartKey, d.EndKey
			defer r.store.maybeAssertNoHole(ctx, from, to)()
		} else {
			// For snapshots to existing replicas, from and to usually match (i.e.
			// nothing is asserted) but if the snapshot spans a merge then we're
			// going to assert that we're transferring the keyspace from the subsumed
			// replicas to this replica seamlessly.
			d := r.Desc()
			from, to = d.EndKey, inSnap.Desc.EndKey
			defer r.store.maybeAssertNoHole(ctx, from, to)()
		}
	}
	defer func() {
		if e := recover(); e != nil {
			// Re-panic to avoid the log.Fatal() below.
			panic(e)
		}
		if err == nil {
			desc, err := r.GetReplicaDescriptor()
			if err != nil {
				log.Fatalf(ctx, "could not fetch replica descriptor for range after applying snapshot: %v", err)
			}
			if isInitialSnap {
				r.store.metrics.RangeSnapshotsAppliedForInitialUpreplication.Inc(1)
			} else {
				switch typ := desc.GetType(); typ {
				// NB: A replica of type LEARNER can receive a non-initial snapshot (via
				// the snapshot queue) if we end up truncating the raft log before it
				// gets promoted to a voter. We count such snapshot applications as
				// "applied by voters" here, since the LEARNER will soon be promoted to
				// a voting replica.
				case roachpb.VOTER_FULL, roachpb.VOTER_INCOMING, roachpb.VOTER_DEMOTING_LEARNER,
					roachpb.VOTER_OUTGOING, roachpb.LEARNER, roachpb.VOTER_DEMOTING_NON_VOTER:
					r.store.metrics.RangeSnapshotsAppliedByVoters.Inc(1)
				case roachpb.NON_VOTER:
					r.store.metrics.RangeSnapshotsAppliedByNonVoters.Inc(1)
				default:
					log.Fatalf(ctx, "unexpected replica type %s while applying snapshot", typ)
				}
			}
		}
	}()

	if raft.IsEmptyHardState(hs) {
		// Raft will never provide an empty HardState if it is providing a
		// nonempty snapshot because we discard snapshots that do not increase
		// the commit index.
		log.Fatalf(ctx, "found empty HardState for non-empty Snapshot %+v", nonemptySnap)
	}

	var stats struct {
		// Time to process subsumed replicas.
		subsumedReplicas time.Time
		// Time to ingest SSTs.
		ingestion time.Time
	}
	log.Infof(ctx, "applying %s", inSnap)
	defer func(start time.Time) {
		var logDetails redact.StringBuilder
		logDetails.Printf("total=%0.0fms", timeutil.Since(start).Seconds()*1000)
		logDetails.Printf(" data=%s", humanizeutil.IBytes(inSnap.DataSize))
		if len(subsumedRepls) > 0 {
			logDetails.Printf(" subsumedReplicas=%d@%0.0fms",
				len(subsumedRepls), stats.subsumedReplicas.Sub(start).Seconds()*1000)
		}
		logDetails.Printf(" ingestion=%d@%0.0fms", len(inSnap.SSTStorageScratch.SSTs()),
			stats.ingestion.Sub(stats.subsumedReplicas).Seconds()*1000)
		log.Infof(ctx, "applied %s (%s)", inSnap, logDetails)
	}(timeutil.Now())

	unreplicatedSSTFile := &storage.MemFile{}
	unreplicatedSST := storage.MakeIngestionSSTWriter(
		ctx, r.ClusterSettings(), unreplicatedSSTFile,
	)
	defer unreplicatedSST.Close()

	// Clearing the unreplicated state.
	unreplicatedPrefixKey := keys.MakeRangeIDUnreplicatedPrefix(r.RangeID)
	unreplicatedStart := unreplicatedPrefixKey
	unreplicatedEnd := unreplicatedPrefixKey.PrefixEnd()
	if err = unreplicatedSST.ClearRawRange(unreplicatedStart, unreplicatedEnd); err != nil {
		return errors.Wrapf(err, "error clearing range of unreplicated SST writer")
	}

	// Update HardState.
	if err := r.raftMu.stateLoader.SetHardState(ctx, &unreplicatedSST, hs); err != nil {
		return errors.Wrapf(err, "unable to write HardState to unreplicated SST writer")
	}
	// We've cleared all the raft state above, so we are forced to write the
	// RaftReplicaID again here.
	if err := r.raftMu.stateLoader.SetRaftReplicaID(
		ctx, &unreplicatedSST, r.replicaID); err != nil {
		return errors.Wrapf(err, "unable to write RaftReplicaID to unreplicated SST writer")
	}

	// Update Raft entries.
	r.store.raftEntryCache.Drop(r.RangeID)

	if err := r.raftMu.stateLoader.SetRaftTruncatedState(
		ctx, &unreplicatedSST,
		&roachpb.RaftTruncatedState{
			Index: nonemptySnap.Metadata.Index,
			Term:  nonemptySnap.Metadata.Term,
		},
	); err != nil {
		return errors.Wrapf(err, "unable to write TruncatedState to unreplicated SST writer")
	}

	if err := unreplicatedSST.Finish(); err != nil {
		return err
	}
	if unreplicatedSST.DataSize > 0 {
		// TODO(itsbilal): Write to SST directly in unreplicatedSST rather than
		// buffering in a MemFile first.
		if err := inSnap.SSTStorageScratch.WriteSST(ctx, unreplicatedSSTFile.Data()); err != nil {
			return err
		}
	}

	// If we're subsuming a replica below, we don't have its last NextReplicaID,
	// nor can we obtain it. That's OK: we can just be conservative and use the
	// maximum possible replica ID. preDestroyRaftMuLocked will write a replica
	// tombstone using this maximum possible replica ID, which would normally be
	// problematic, as it would prevent this store from ever having a new replica
	// of the removed range. In this case, however, it's copacetic, as subsumed
	// ranges _can't_ have new replicas.
	if err := r.clearSubsumedReplicaDiskData(ctx, inSnap.SSTStorageScratch, desc, subsumedRepls, mergedTombstoneReplicaID); err != nil {
		return err
	}
	stats.subsumedReplicas = timeutil.Now()

	// Ingest all SSTs atomically.
	if fn := r.store.cfg.TestingKnobs.BeforeSnapshotSSTIngestion; fn != nil {
		if err := fn(inSnap, inSnap.snapType, inSnap.SSTStorageScratch.SSTs()); err != nil {
			return err
		}
	}
	if err := r.store.engine.IngestExternalFiles(ctx, inSnap.SSTStorageScratch.SSTs()); err != nil {
		return errors.Wrapf(err, "while ingesting %s", inSnap.SSTStorageScratch.SSTs())
	}
	stats.ingestion = timeutil.Now()

	state, err := stateloader.Make(desc.RangeID).Load(ctx, r.store.engine, desc)
	if err != nil {
		log.Fatalf(ctx, "unable to load replica state: %s", err)
	}

	if state.RaftAppliedIndex != nonemptySnap.Metadata.Index {
		log.Fatalf(ctx, "snapshot RaftAppliedIndex %d doesn't match its metadata index %d",
			state.RaftAppliedIndex, nonemptySnap.Metadata.Index)
	}
	// If we've migrated to populating RaftAppliedIndexTerm, check that the term
	// from the two sources are equal.
	if state.RaftAppliedIndexTerm != 0 && state.RaftAppliedIndexTerm != nonemptySnap.Metadata.Term {
		log.Fatalf(ctx, "snapshot RaftAppliedIndexTerm %d doesn't match its metadata term %d",
			state.RaftAppliedIndexTerm, nonemptySnap.Metadata.Term)
	}

	// The on-disk state is now committed, but the corresponding in-memory state
	// has not yet been updated. Any errors past this point must therefore be
	// treated as fatal.

	subPHs, err := r.clearSubsumedReplicaInMemoryData(ctx, subsumedRepls, mergedTombstoneReplicaID)
	if err != nil {
		log.Fatalf(ctx, "failed to clear in-memory data of subsumed replicas while applying snapshot: %+v", err)
	}

	// Read the prior read summary for this range, which was included in the
	// snapshot. We may need to use it to bump our timestamp cache if we
	// discover that we are the leaseholder as of the snapshot's log index.
	prioReadSum, err := readsummary.Load(ctx, r.store.engine, r.RangeID)
	if err != nil {
		log.Fatalf(ctx, "failed to read prior read summary after applying snapshot: %+v", err)
	}

	// Atomically swap the placeholder, if any, for the replica, and update the
	// replica's state. Note that this is intentionally in one critical section.
	// to avoid exposing an inconsistent in-memory state. We did however already
	// consume the SSTs above, meaning that at this point the in-memory state lags
	// the on-disk state.

	r.store.mu.Lock()
	if inSnap.placeholder != nil {
		subPHs = append(subPHs, inSnap.placeholder)
	}
	for _, ph := range subPHs {
		_, err := r.store.removePlaceholderLocked(ctx, ph, removePlaceholderFilled)
		if err != nil {
			log.Fatalf(ctx, "unable to remove placeholder %s: %s", ph, err)
		}
	}

	// NB: we lock `r.mu` only now because removePlaceholderLocked operates on
	// replicasByKey and this may end up calling r.Desc().
	r.mu.Lock()
	r.setDescLockedRaftMuLocked(ctx, desc)
	if err := r.store.maybeMarkReplicaInitializedLockedReplLocked(ctx, r); err != nil {
		log.Fatalf(ctx, "unable to mark replica initialized while applying snapshot: %+v", err)
	}
	// NOTE: even though we acquired the store mutex first (according to the
	// lock ordering rules described on Store.mu), it is safe to drop it first
	// without risking a lock-ordering deadlock.
	r.store.mu.Unlock()

	// We set the persisted last index to the last applied index. This is
	// not a correctness issue, but means that we may have just transferred
	// some entries we're about to re-request from the leader and overwrite.
	// However, raft.MultiNode currently expects this behavior, and the
	// performance implications are not likely to be drastic. If our
	// feelings about this ever change, we can add a LastIndex field to
	// raftpb.SnapshotMetadata.
	r.mu.lastIndex = state.RaftAppliedIndex

	// TODO(sumeer): We should be able to set this to
	// nonemptySnap.Metadata.Term. See
	// https://github.com/cockroachdb/cockroach/pull/75675#pullrequestreview-867926687
	// for a discussion regarding this.
	r.mu.lastTerm = invalidLastTerm
	r.mu.raftLogSize = 0
	// Update the store stats for the data in the snapshot.
	r.store.metrics.subtractMVCCStats(ctx, r.tenantMetricsRef, *r.mu.state.Stats)
	r.store.metrics.addMVCCStats(ctx, r.tenantMetricsRef, *state.Stats)
	lastKnownLease := r.mu.state.Lease
	// Update the rest of the Raft state. Changes to r.mu.state.Desc must be
	// managed by r.setDescRaftMuLocked and changes to r.mu.state.Lease must be handled
	// by r.leasePostApply, but we called those above, so now it's safe to
	// wholesale replace r.mu.state.
	r.mu.state = state
	// Snapshots typically have fewer log entries than the leaseholder. The next
	// time we hold the lease, recompute the log size before making decisions.
	r.mu.raftLogSizeTrusted = false

	// Invoke the leasePostApply method to ensure we properly initialize the
	// replica according to whether it holds the lease. We allow jumps in the
	// lease sequence because there may be multiple lease changes accounted for
	// in the snapshot.
	r.leasePostApplyLocked(ctx, lastKnownLease, state.Lease /* newLease */, prioReadSum, allowLeaseJump)

	// Similarly, if we subsumed any replicas through the snapshot (meaning that
	// we missed the application of a merge) and we are the new leaseholder, we
	// make sure to update the timestamp cache using the prior read summary to
	// account for any reads that were served on the right-hand side range(s).
	if len(subsumedRepls) > 0 && state.Lease.Replica.ReplicaID == r.replicaID && prioReadSum != nil {
		applyReadSummaryToTimestampCache(r.store.tsCache, r.descRLocked(), *prioReadSum)
	}

	// Inform the concurrency manager that this replica just applied a snapshot.
	r.concMgr.OnReplicaSnapshotApplied()

	r.mu.Unlock()

	// Assert that the in-memory and on-disk states of the Replica are congruent
	// after the application of the snapshot. Do so under a read lock, as this
	// operation can be expensive. This is safe, as we hold the Replica.raftMu
	// across both Replica.mu critical sections.
	r.mu.RLock()
	r.assertStateRaftMuLockedReplicaMuRLocked(ctx, r.store.Engine())
	r.mu.RUnlock()

	// The rangefeed processor is listening for the logical ops attached to
	// each raft command. These will be lost during a snapshot, so disconnect
	// the rangefeed, if one exists.
	r.disconnectRangefeedWithReason(
		roachpb.RangeFeedRetryError_REASON_RAFT_SNAPSHOT,
	)

	// Update the replica's cached byte thresholds. This is a no-op if the system
	// config is not available, in which case we rely on the next gossip update
	// to perform the update.
	if err := r.updateRangeInfo(ctx, desc); err != nil {
		log.Fatalf(ctx, "unable to update range info while applying snapshot: %+v", err)
	}

	return nil
}

// clearSubsumedReplicaDiskData clears the on disk data of the subsumed
// replicas by creating SSTs with range deletion tombstones. We have to be
// careful here not to have overlapping ranges with the SSTs we have already
// created since that will throw an error while we are ingesting them. This
// method requires that each of the subsumed replicas raftMu is held.
func (r *Replica) clearSubsumedReplicaDiskData(
	ctx context.Context,
	scratch *SSTSnapshotStorageScratch,
	desc *roachpb.RangeDescriptor,
	subsumedRepls []*Replica,
	subsumedNextReplicaID roachpb.ReplicaID,
) error {
	// NB: we don't clear RangeID local key ranges here. That happens
	// via the call to preDestroyRaftMuLocked.
	getKeyRanges := rditer.MakeReplicatedKeyRangesExceptRangeID
	keyRanges := getKeyRanges(desc)
	totalKeyRanges := append([]rditer.KeyRange(nil), keyRanges...)
	for _, sr := range subsumedRepls {
		// We mark the replica as destroyed so that new commands are not
		// accepted. This destroy status will be detected after the batch
		// commits by clearSubsumedReplicaInMemoryData() to finish the removal.
		sr.readOnlyCmdMu.Lock()
		sr.mu.Lock()
		sr.mu.destroyStatus.Set(
			roachpb.NewRangeNotFoundError(sr.RangeID, sr.store.StoreID()),
			destroyReasonRemoved)
		sr.mu.Unlock()
		sr.readOnlyCmdMu.Unlock()

		// We have to create an SST for the subsumed replica's range-id local keys.
		subsumedReplSSTFile := &storage.MemFile{}
		subsumedReplSST := storage.MakeIngestionSSTWriter(
			ctx, r.ClusterSettings(), subsumedReplSSTFile,
		)
		defer subsumedReplSST.Close()
		// NOTE: We set mustClearRange to true because we are setting
		// RangeTombstoneKey. Since Clears and Puts need to be done in increasing
		// order of keys, it is not safe to use ClearRangeIter.
		if err := sr.preDestroyRaftMuLocked(
			ctx,
			r.store.Engine(),
			&subsumedReplSST,
			subsumedNextReplicaID,
			true, /* clearRangeIDLocalOnly */
			true, /* mustClearRange */
		); err != nil {
			subsumedReplSST.Close()
			return err
		}
		if err := subsumedReplSST.Finish(); err != nil {
			return err
		}
		if subsumedReplSST.DataSize > 0 {
			// TODO(itsbilal): Write to SST directly in subsumedReplSST rather than
			// buffering in a MemFile first.
			if err := scratch.WriteSST(ctx, subsumedReplSSTFile.Data()); err != nil {
				return err
			}
		}

		srKeyRanges := getKeyRanges(sr.Desc())
		// Compute the total key space covered by the current replica and all
		// subsumed replicas.
		for i := range srKeyRanges {
			if srKeyRanges[i].Start.Compare(totalKeyRanges[i].Start) < 0 {
				totalKeyRanges[i].Start = srKeyRanges[i].Start
			}
			if srKeyRanges[i].End.Compare(totalKeyRanges[i].End) > 0 {
				totalKeyRanges[i].End = srKeyRanges[i].End
			}
		}
	}

	// We might have to create SSTs for the range local keys, lock table keys,
	// and user keys depending on if the subsumed replicas are not fully
	// contained by the replica in our snapshot. The following is an example to
	// this case happening.
	//
	// a       b       c       d
	// |---1---|-------2-------|  S1
	// |---1-------------------|  S2
	// |---1-----------|---3---|  S3
	//
	// Since the merge is the first operation to happen, a follower could be down
	// before it completes. It is reasonable for a snapshot for r1 from S3 to
	// subsume both r1 and r2 in S1.
	for i := range keyRanges {
		if totalKeyRanges[i].End.Compare(keyRanges[i].End) > 0 {
			subsumedReplSSTFile := &storage.MemFile{}
			subsumedReplSST := storage.MakeIngestionSSTWriter(
				ctx, r.ClusterSettings(), subsumedReplSSTFile,
			)
			defer subsumedReplSST.Close()
			if err := storage.ClearRangeWithHeuristic(
				r.store.Engine(),
				&subsumedReplSST,
				keyRanges[i].End,
				totalKeyRanges[i].End,
			); err != nil {
				subsumedReplSST.Close()
				return err
			}
			if err := subsumedReplSST.Finish(); err != nil {
				return err
			}
			if subsumedReplSST.DataSize > 0 {
				// TODO(itsbilal): Write to SST directly in subsumedReplSST rather than
				// buffering in a MemFile first.
				if err := scratch.WriteSST(ctx, subsumedReplSSTFile.Data()); err != nil {
					return err
				}
			}
		}
		// The snapshot must never subsume a replica that extends the range of the
		// replica to the left. This is because splits and merges (the only
		// operation that change the key bounds) always leave the start key intact.
		// Extending to the left implies that either we merged "to the left" (we
		// don't), or that we're applying a snapshot for another range (we don't do
		// that either). Something is severely wrong for this to happen.
		if totalKeyRanges[i].Start.Compare(keyRanges[i].Start) < 0 {
			log.Fatalf(ctx, "subsuming replica to our left; key range: %v; total key range %v",
				keyRanges[i], totalKeyRanges[i])
		}
	}
	return nil
}

// clearSubsumedReplicaInMemoryData clears the in-memory data of the subsumed
// replicas. This method requires that each of the subsumed replicas raftMu is
// held.
func (r *Replica) clearSubsumedReplicaInMemoryData(
	ctx context.Context, subsumedRepls []*Replica, subsumedNextReplicaID roachpb.ReplicaID,
) ([]*ReplicaPlaceholder, error) {
	//
	var phs []*ReplicaPlaceholder
	for _, sr := range subsumedRepls {
		// We already hold sr's raftMu, so we must call removeReplicaImpl directly.
		// As the subsuming range is planning to expand to cover the subsumed ranges,
		// we introduce corresponding placeholders and return them to the caller to
		// consume. Without this, there would be a risk of errant snapshots being
		// allowed in (perhaps not involving any of the RangeIDs known to the merge
		// but still touching its keyspace) and causing corruption.
		ph, err := r.store.removeInitializedReplicaRaftMuLocked(ctx, sr, subsumedNextReplicaID, RemoveOptions{
			// The data was already destroyed by clearSubsumedReplicaDiskData.
			DestroyData:       false,
			InsertPlaceholder: true,
		})
		if err != nil {
			return nil, err
		}
		phs = append(phs, ph)
		// We removed sr's data when we committed the batch. Finish subsumption by
		// updating the in-memory bookkeping.
		if err := sr.postDestroyRaftMuLocked(ctx, sr.GetMVCCStats()); err != nil {
			return nil, err
		}
	}
	return phs, nil
}
