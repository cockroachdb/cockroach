// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
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
	// Check whether the first requested entry is already logically truncated. It
	// may or may not be physically truncated, since the RaftTruncatedState is
	// updated before the truncation is enacted.
	if lo <= r.shMu.raftTruncState.Index {
		return nil, raft.ErrCompacted
	}
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
	entries, _, loadedSize, err := logstore.LoadEntries(
		r.AnnotateCtx(context.TODO()),
		r.store.LogEngine(), r.RangeID, r.store.raftEntryCache, r.raftMu.sideloaded,
		lo, hi, maxBytes,
		nil, // bytesAccount is not used when reading under Replica.mu
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
func (r *replicaLogStorage) Term(index uint64) (uint64, error) {
	r.mu.AssertHeld()
	term, err := (*Replica)(r).raftTermShMuLocked(kvpb.RaftIndex(index))
	if err != nil {
		r.reportRaftStorageError(err)
	}
	return uint64(term), err
}

// raftTermShMuLocked implements the Term() call. Requires that either
// Replica.mu or Replica.raftMu is held, at least for reads.
//
// TODO(pav-kv): figure out a zero-cost-in-prod way to assert that either of two
// mutexes is held. Can't use the regular AssertHeld() here.
func (r *Replica) raftTermShMuLocked(index kvpb.RaftIndex) (kvpb.RaftTerm, error) {
	// Check whether the entry is already logically truncated, or is at a bound.
	// It may or may not be physically truncated, since the RaftTruncatedState is
	// updated before the truncation is enacted.
	// NB: two common cases are checked first.
	if r.shMu.lastIndexNotDurable == index {
		return r.shMu.lastTermNotDurable, nil
	} else if index == r.shMu.raftTruncState.Index {
		return r.shMu.raftTruncState.Term, nil
	} else if index < r.shMu.raftTruncState.Index {
		return 0, raft.ErrCompacted
	} else if index > r.shMu.lastIndexNotDurable {
		return 0, raft.ErrUnavailable
	}
	// Check if the entry is cached, to avoid storage access.
	if entry, found := r.store.raftEntryCache.Get(r.RangeID, index); found {
		return kvpb.RaftTerm(entry.Term), nil
	}

	entry, err := logstore.LoadEntry(r.AnnotateCtx(context.TODO()),
		r.store.LogEngine(), r.RangeID, index)
	if err != nil {
		return 0, err
	}
	// Cache the entry except if it is sideloaded. We don't load/inline the
	// sideloaded entries here to keep the term fetching cheap.
	// TODO(pav-kv): consider not caching here, after measuring if it makes any
	// difference.
	if typ, _, err := raftlog.EncodingOf(entry); err != nil {
		return 0, err
	} else if !typ.IsSideloaded() {
		r.store.raftEntryCache.Add(r.RangeID, []raftpb.Entry{entry}, false /* truncate */)
	}
	return kvpb.RaftTerm(entry.Term), nil
}

// GetTerm returns the term of the entry at the given index in the raft log.
// Requires that r.mu is not held.
func (r *Replica) GetTerm(index kvpb.RaftIndex) (kvpb.RaftTerm, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.raftTermShMuLocked(index)
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

// Compacted implements the raft.LogStorage interface.
// Requires that r.mu is held for reading.
func (r *replicaLogStorage) Compacted() uint64 {
	return uint64((*Replica)(r).raftCompactedIndexRLocked())
}

// raftCompactedIndexRLocked implements the Compacted() call.
func (r *Replica) raftCompactedIndexRLocked() kvpb.RaftIndex {
	return r.shMu.raftTruncState.Index
}

// GetCompactedIndex returns the compacted index of the raft log.
// Requires that r.mu is not held.
func (r *Replica) GetCompactedIndex() kvpb.RaftIndex {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.raftCompactedIndexRLocked()
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
// logstore.LogStore type, or applySnapshotRaftMuLocked.
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
	// Check whether the first requested entry is already logically truncated. It
	// may or may not be physically truncated, since the RaftTruncatedState is
	// updated before the truncation is enacted.
	// TODO(pav-kv): de-duplicate this code and the one where r.mu must be held.
	if lo <= r.shMu.raftTruncState.Index {
		return nil, raft.ErrCompacted
	}
	entries, _, loadedSize, err := logstore.LoadEntries(
		r.AnnotateCtx(context.TODO()),
		r.store.LogEngine(), r.RangeID, r.store.raftEntryCache, r.raftMu.sideloaded,
		lo, hi, maxBytes,
		&r.raftMu.bytesAccount,
	)
	r.store.metrics.RaftStorageReadBytes.Inc(int64(loadedSize))
	return entries, err
}

// Term implements the raft.LogStorageSnapshot interface.
// Requires that r.raftMu is held.
func (r *replicaRaftMuLogSnap) Term(index uint64) (uint64, error) {
	r.raftMu.AssertHeld()
	term, err := (*Replica)(r).raftTermShMuLocked(kvpb.RaftIndex(index))
	if err != nil {
		(*replicaLogStorage)(r).reportRaftStorageError(err)
	}
	return uint64(term), err
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

// Compacted implements the raft.LogStorageSnapshot interface.
// Requires that r.raftMu is held.
func (r *replicaRaftMuLogSnap) Compacted() uint64 {
	r.raftMu.AssertHeld()
	return uint64(r.shMu.raftTruncState.Index)
}

// LogSnapshot implements the raft.LogStorageSnapshot interface.
func (r *replicaRaftMuLogSnap) LogSnapshot() raft.LogStorageSnapshot {
	return r
}
