// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
)

// replicaLogStorage implements the raft.LogStorage interface.
//
// All methods require r.mu held for writes or reads. The specific requirements
// are noted in a method's comment, or can be found in assertions at the
// beginning of the implementation methods.
//
// The method names do not follow our "Locked" naming conventions, due to being
// an implementation of an interface from a different package, but in most cases
// they delegate to a method that does follow the convention.
//
// TODO(pav-kv): make it a proper type, and integrate with the logstore package.
type replicaLogStorage Replica

// asLogStorage returns the raft.LogStorage implementation of this replica.
func (r *Replica) asLogStorage() *replicaLogStorage {
	return (*replicaLogStorage)(r)
}

// Entries implements the raft.LogStorage interface.
//
// NB: maxBytes is advisory, and this method returns at least one entry (unless
// there are none in the requested interval), even if its size exceeds maxBytes.
// Sideloaded entries count towards maxBytes with their payloads inlined.
//
// Entries can return log entries that are not yet durable / synced in storage.
//
// Requires that r.mu is held for writing.
// TODO(pav-kv): make it possible to call with only raftMu held.
func (r *replicaLogStorage) Entries(lo, hi uint64, maxBytes uint64) ([]raftpb.Entry, error) {
	entries, err := r.entriesLocked(
		kvpb.RaftIndex(lo), kvpb.RaftIndex(hi), maxBytes)
	if err != nil {
		r.reportRaftStorageError(err)
	}
	return entries, err
}

// entriesLocked implements the Entries() call.
func (r *replicaLogStorage) entriesLocked(
	lo, hi kvpb.RaftIndex, maxBytes uint64,
) ([]raftpb.Entry, error) {
	// The call is always initiated by RawNode, under r.mu. Need it locked for
	// writes, for r.mu.stateLoader.
	//
	// TODO(pav-kv): we have a large class of cases when we would rather only hold
	// raftMu while reading the entries. The r.mu lock should be narrow.
	r.mu.AssertHeld()
	// Writes to the storage engine and the sideloaded storage are made under
	// raftMu only. Since we are holding r.mu, but may or may not be holding
	// raftMu, this read could be racing with a write.
	//
	// Such races are prevented at a higher level, in RawNode. Raft never reads at
	// a log index for which there is at least one in-flight entry (possibly
	// multiple, issued at different leader terms) to storage. It always reads
	// "stable" entries.
	//
	// NB: without this guarantee, there would be a concern with the sideloaded
	// storage: it doesn't provide a consistent snapshot to the reader, unlike the
	// storage engine. Its Put method writes / syncs a file sequentially, so a
	// racing reader would be able to read partial entries.
	//
	// TODO(pav-kv): we need better safety guardrails here. The log storage type
	// can remember the readable bounds, and assert that reads do not cross them.
	// TODO(pav-kv): r.raftMu.bytesAccount is broken - can't rely on raftMu here.
	entries, _, loadedSize, err := logstore.LoadEntries(
		r.AnnotateCtx(context.TODO()),
		r.mu.stateLoader.StateLoader, r.store.TODOEngine(), r.RangeID,
		r.store.raftEntryCache, r.raftMu.sideloaded, lo, hi, maxBytes,
		&r.raftMu.bytesAccount,
	)
	r.store.metrics.RaftStorageReadBytes.Inc(int64(loadedSize))
	return entries, err
}

// raftEntriesLocked implements the Entries() call.
func (r *Replica) raftEntriesLocked(
	lo, hi kvpb.RaftIndex, maxBytes uint64,
) ([]raftpb.Entry, error) {
	return r.asLogStorage().entriesLocked(lo, hi, maxBytes)
}

// Term implements the raft.LogStorage interface.
// Requires that r.mu is held for writing.
func (r *replicaLogStorage) Term(i uint64) (uint64, error) {
	term, err := r.termLocked(kvpb.RaftIndex(i))
	if err != nil {
		r.reportRaftStorageError(err)
	}
	return uint64(term), err
}

// termLocked implements the Term() call.
func (r *replicaLogStorage) termLocked(i kvpb.RaftIndex) (kvpb.RaftTerm, error) {
	// TODO(pav-kv): make it possible to read with only raftMu held.
	r.mu.AssertHeld()
	if r.shMu.lastIndexNotDurable == i {
		return r.shMu.lastTermNotDurable, nil
	}
	return logstore.LoadTerm(r.AnnotateCtx(context.TODO()),
		r.mu.stateLoader.StateLoader, r.store.TODOEngine(), r.RangeID,
		r.store.raftEntryCache, i,
	)
}

// raftTermLocked implements the Term() call.
func (r *Replica) raftTermLocked(i kvpb.RaftIndex) (kvpb.RaftTerm, error) {
	return r.asLogStorage().termLocked(i)
}

// GetTerm returns the term of the entry at the given index in the raft log.
// Requires that r.mu is not held.
func (r *Replica) GetTerm(i kvpb.RaftIndex) (kvpb.RaftTerm, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.raftTermLocked(i)
}

// LastIndex implements the raft.LogStorage interface.
// Requires that r.mu is held for reading.
func (r *replicaLogStorage) LastIndex() uint64 {
	return uint64((*Replica)(r).raftLastIndexRLocked())
}

// raftLastIndexRLocked implements the LastIndex() call.
func (r *Replica) raftLastIndexRLocked() kvpb.RaftIndex {
	return r.shMu.lastIndexNotDurable
}

// GetLastIndex returns the index of the last entry in the raft log.
// Requires that r.mu is not held.
func (r *Replica) GetLastIndex() kvpb.RaftIndex {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.raftLastIndexRLocked()
}

// FirstIndex implements the raft.LogStorage interface.
// Requires that r.mu is held for reading.
func (r *replicaLogStorage) FirstIndex() uint64 {
	return uint64((*Replica)(r).raftFirstIndexRLocked())
}

// raftFirstIndexRLocked implements the FirstIndex() call.
func (r *Replica) raftFirstIndexRLocked() kvpb.RaftIndex {
	return r.shMu.raftTruncState.Index + 1
}

// GetFirstIndex returns the index of the first entry in the raft log.
// Requires that r.mu is not held.
func (r *Replica) GetFirstIndex() kvpb.RaftIndex {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.raftFirstIndexRLocked()
}

// LogSnapshot returns an immutable point-in-time snapshot of the log storage.
//
// Requires that r.raftMu is held for writing, and r.mu for reading. In
// addition, r.raftMu must be held continuously throughout the lifetime of the
// returned snapshot.
func (r *replicaLogStorage) LogSnapshot() raft.LogStorageSnapshot {
	r.raftMu.AssertHeld()
	r.mu.AssertRHeld()
	return (*replicaRaftMuLogSnap)(r)
}

// replicaRaftMuLogSnap implements the raft.LogStorageSnapshot interface.
//
// The type implements a limited version of a raft log storage snapshot, without
// needing a storage engine snapshot. It relies on r.raftMu being held
// throughout the raft.LogStorageSnapshot lifetime. Since raft writes are
// blocked while r.raftMu is held, this type behaves like a consistent storage
// snapshot until r.raftMu is released.
//
// TODO(pav-kv): equip this wrapper with correctness checks, e.g. that the log
// storage hasn't been written to while we hold a snapshot. A more relaxed
// version of it should assert that only the relevant part of the log hasn't
// been overwritten, e.g. a new term leader hasn't appended a log slice that
// truncates the log and overwrites log indices in our snapshot.
//
// This would require auditing and integrating with the write paths. Today, this
// type implements only reads, and writes are in various places like the
// logstore.LogStore type, or applySnapshot.
type replicaRaftMuLogSnap replicaLogStorage

// Entries implements the raft.LogStorageSnapshot interface.
// Requires that r.raftMu is held.
func (r *replicaRaftMuLogSnap) Entries(lo, hi, maxBytes uint64) ([]raftpb.Entry, error) {
	entries, err := r.entriesRaftMuLocked(
		kvpb.RaftIndex(lo), kvpb.RaftIndex(hi), maxBytes)
	if err != nil {
		(*replicaLogStorage)(r).reportRaftStorageError(err)
	}
	return entries, err
}

// entriesRaftMuLocked implements the Entries() call.
func (r *replicaRaftMuLogSnap) entriesRaftMuLocked(
	lo, hi kvpb.RaftIndex, maxBytes uint64,
) ([]raftpb.Entry, error) {
	// NB: writes to the storage engine and the sideloaded storage are made under
	// raftMu only, so we are not racing with new writes. In addition, raft never
	// tries to read "unstable" entries that correspond to ongoing writes.
	r.raftMu.AssertHeld()
	// TODO(pav-kv): de-duplicate this code and the one where r.mu must be held.
	entries, _, loadedSize, err := logstore.LoadEntries(
		r.AnnotateCtx(context.TODO()),
		r.raftMu.stateLoader.StateLoader, r.store.TODOEngine(), r.RangeID,
		r.store.raftEntryCache, r.raftMu.sideloaded, lo, hi, maxBytes,
		&r.raftMu.bytesAccount,
	)
	r.store.metrics.RaftStorageReadBytes.Inc(int64(loadedSize))
	return entries, err
}

// Term implements the raft.LogStorageSnapshot interface.
// Requires that r.raftMu is held.
func (r *replicaRaftMuLogSnap) Term(i uint64) (uint64, error) {
	term, err := r.termRaftMuLocked(kvpb.RaftIndex(i))
	if err != nil {
		(*replicaLogStorage)(r).reportRaftStorageError(err)
	}
	return uint64(term), err
}

// termRaftMuLocked implements the Term() call.
func (r *replicaRaftMuLogSnap) termRaftMuLocked(i kvpb.RaftIndex) (kvpb.RaftTerm, error) {
	r.raftMu.AssertHeld()
	// NB: the r.mu fields accessed here are always written under both r.raftMu
	// and r.mu, and the reads are safe under r.raftMu.
	if r.shMu.lastIndexNotDurable == i {
		return r.shMu.lastTermNotDurable, nil
	}
	return logstore.LoadTerm(r.AnnotateCtx(context.TODO()),
		r.raftMu.stateLoader.StateLoader, r.store.TODOEngine(), r.RangeID,
		r.store.raftEntryCache, i,
	)
}

// LastIndex implements the raft.LogStorageSnapshot interface.
// Requires that r.raftMu is held.
func (r *replicaRaftMuLogSnap) LastIndex() uint64 {
	// NB: lastIndexNotDurable is updated under both r.raftMu and r.mu, so it is
	// safe to access while holding any of these mutexes. We enforce raftMu
	// because this is a raftMu-based snapshot.
	r.raftMu.AssertHeld()
	return uint64(r.shMu.lastIndexNotDurable)
}

// FirstIndex implements the raft.LogStorageSnapshot interface.
// Requires that r.raftMu is held.
func (r *replicaRaftMuLogSnap) FirstIndex() uint64 {
	r.raftMu.AssertHeld()
	return uint64(r.shMu.raftTruncState.Index + 1)
}

// LogSnapshot implements the raft.LogStorageSnapshot interface.
func (r *replicaRaftMuLogSnap) LogSnapshot() raft.LogStorageSnapshot {
	return r
}
