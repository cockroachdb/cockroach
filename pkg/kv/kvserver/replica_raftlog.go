// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type mutexPair struct {
	mu     *syncutil.RWMutex
	raftMu *syncutil.Mutex
}

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
// TODO(pav-kv): integrate better with the logstore package.
type replicaLogStorage struct {
	// ctx is the log storage context, which includes the log tags from the parent
	// node, store and replica.
	ctx context.Context
	// mux contains the read and write mutex. For Replica, this is Replica.mu and
	// Replica.raftMu, correspondingly.
	mux mutexPair

	// mu contains the fields protected by mux.mu. Equivalent to Replica.mu.
	mu struct {
		// NB: there are two state loaders, in raftMu and mu, depending on which
		// lock is being held.
		stateLoader logstore.StateLoader
	}
	// raftMu contains the fields protected by mux.raftMu. Equivalent to
	// Replica.raftMu.
	raftMu struct {
		// NB: there are two state loaders, in raftMu and mu, depending on which
		// lock is being held.
		stateLoader logstore.StateLoader
		// bytesAccount accounts bytes used by various Raft components, like entries
		// to be applied. Currently, it only tracks bytes used by committed entries
		// being applied to the state machine.
		bytesAccount logstore.BytesAccount
	}

	// shMu contains "shared" fields which are mutated while both mu and raftMu
	// are held for writes. They can be accessed when either of the two mutexes is
	// held.
	shMu struct {
		// raftTruncState contains the raft log truncation state, i.e. the ID of the
		// last entry of the log prefix that has been compacted out from the raft
		// log storage.
		raftTruncState kvserverpb.RaftTruncatedState
		// Last index/term written to the raft log (not necessarily durable locally
		// or committed by the group). Note that lastTermNotDurable may be 0 (and
		// thus invalid) even when lastIndexNotDurable is known, in which case the
		// term will have to be retrieved from the Raft log entry. Use the
		// invalidLastTerm constant for this case.
		lastIndexNotDurable kvpb.RaftIndex
		lastTermNotDurable  kvpb.RaftTerm
		// raftLogSize is the approximate size in bytes of the persisted raft
		// log, including sideloaded entries' payloads. The value itself is not
		// persisted and is computed lazily, paced by the raft log truncation
		// queue which will recompute the log size when it finds it
		// uninitialized. This recomputation mechanism isn't relevant for ranges
		// which see regular write activity (for those the log size will deviate
		// from zero quickly, and so it won't be recomputed but will undercount
		// until the first truncation is carried out), but it prevents a large
		// dormant Raft log from sitting around forever, which has caused problems
		// in the past.
		//
		// Note that both raftLogSize and raftLogSizeTrusted do not include the
		// effect of pending log truncations (see Replica.pendingLogTruncations).
		// Hence, they are fine for metrics etc., but not for deciding whether we
		// should create another pending truncation. For the latter, we compute
		// the post-pending-truncation size using pendingLogTruncations.
		raftLogSize int64
		// If raftLogSizeTrusted is false, don't trust the above raftLogSize until
		// it has been recomputed.
		raftLogSizeTrusted bool
		// raftLogLastCheckSize is the value of raftLogSize the last time the Raft
		// log was checked for truncation or at the time of the last Raft log
		// truncation.
		raftLogLastCheckSize int64
	}

	// raftEntriesMonitor tracks memory used by raft entries.
	raftEntriesMonitor *logstore.SoftLimit
	// ls provides access to the raft log storage. Set once, never nil.
	ls     *logstore.LogStore
	onSync logstore.SyncCallback

	metrics *StoreMetrics
}

// asLogStorage returns the raft.LogStorage implementation of this replica.
func (r *Replica) asLogStorage() *replicaLogStorage {
	return r.logStorage
}

func (r *replicaLogStorage) attachRaftEntriesMonitorRaftMuLocked() {
	r.raftMu.bytesAccount = r.raftEntriesMonitor.NewAccount(r.metrics.RaftLoadedEntriesBytes)
}

func (r *replicaLogStorage) detachRaftEntriesMonitorRaftMuLocked() {
	// Return all the used bytes back to the limiter.
	r.raftMu.bytesAccount.Clear()
	// De-initialize the account so that log storage Entries() calls don't track
	// the entries anymore.
	r.raftMu.bytesAccount = logstore.BytesAccount{}
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
	r.mux.mu.AssertHeld()
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
		r.ctx, r.mu.stateLoader, r.ls.Engine, r.ls.RangeID,
		r.ls.EntryCache, r.ls.Sideload, lo, hi, maxBytes,
		nil, // bytesAccount is not used when reading under Replica.mu
	)
	r.metrics.RaftStorageReadBytes.Inc(int64(loadedSize))
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
	r.mux.mu.AssertHeld()
	if r.shMu.lastIndexNotDurable == i {
		return r.shMu.lastTermNotDurable, nil
	}
	return logstore.LoadTerm(
		r.ctx, r.mu.stateLoader, r.ls.Engine, r.ls.RangeID,
		r.ls.EntryCache, i,
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
	return uint64(r.shMu.lastIndexNotDurable)
}

// raftLastIndexRLocked implements the LastIndex() call.
func (r *Replica) raftLastIndexRLocked() kvpb.RaftIndex {
	return kvpb.RaftIndex(r.asLogStorage().LastIndex())
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
	r.mux.mu.AssertRHeld()
	return uint64(r.shMu.raftTruncState.Index)
}

// raftCompactedIndexRLocked implements the Compacted() call.
func (r *Replica) raftCompactedIndexRLocked() kvpb.RaftIndex {
	return kvpb.RaftIndex(r.asLogStorage().Compacted())
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
	r.mux.raftMu.AssertHeld()
	r.mux.mu.AssertRHeld()
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
	r.mux.raftMu.AssertHeld()
	// TODO(pav-kv): de-duplicate this code and the one where r.mu must be held.
	entries, _, loadedSize, err := logstore.LoadEntries(
		r.ctx, r.raftMu.stateLoader, r.ls.Engine, r.ls.RangeID,
		r.ls.EntryCache, r.ls.Sideload, lo, hi, maxBytes,
		nil, // &r.raftMu.bytesAccount, // FIXME
	)
	r.metrics.RaftStorageReadBytes.Inc(int64(loadedSize))
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
	r.mux.raftMu.AssertHeld()
	if r.shMu.lastIndexNotDurable == i {
		return r.shMu.lastTermNotDurable, nil
	}
	return logstore.LoadTerm(
		r.ctx, r.raftMu.stateLoader, r.ls.Engine, r.ls.RangeID,
		r.ls.EntryCache, i,
	)
}

// LastIndex implements the raft.LogStorageSnapshot interface.
// Requires that r.raftMu is held.
func (r *replicaRaftMuLogSnap) LastIndex() uint64 {
	r.mux.raftMu.AssertHeld()
	return uint64(r.shMu.lastIndexNotDurable)
}

// Compacted implements the raft.LogStorageSnapshot interface.
// Requires that r.raftMu is held.
func (r *replicaRaftMuLogSnap) Compacted() uint64 {
	r.mux.raftMu.AssertHeld()
	return uint64(r.shMu.raftTruncState.Index)
}

// LogSnapshot implements the raft.LogStorageSnapshot interface.
func (r *replicaRaftMuLogSnap) LogSnapshot() raft.LogStorageSnapshot {
	return r
}

func (r *replicaLogStorage) reportRaftStorageError(err error) {
	if raftStorageErrorLogger.ShouldLog() {
		log.Errorf(r.ctx, "error in raft.LogStorage %v", err)
	}
	r.metrics.RaftStorageError.Inc(1)
}
