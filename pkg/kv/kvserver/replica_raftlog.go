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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftentry"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// replicaLogStorage implements the raft.LogStorage interface.
//
// replicaLogStorage shares two mutexes with the Replica: mu and raftMu. This
// gives Replica ability to access to the log storage transactionally with other
// operations it may need to perform. Mutex locking and unlocking always happens
// outside replicaLogStorage. Each method specifies which locks must be held
// when calling it, or this can also be found in the assertions inside methods.
//
// Not all method names follow our "Locked" naming conventions, due to being an
// implementation of an interface from a different package, but in most cases
// they delegate to a method that does follow the convention.
//
// TODO(pav-kv): integrate better with the logstore package.
type replicaLogStorage struct {
	// ctx is the log storage context, which includes the log tags from the parent
	// node, store and replica.
	ctx context.Context

	// mu contains the fields protected by the "read" mutex. For Replica, it
	// points to Replica.mu, and shares its semantics and locking order.
	mu struct {
		*syncutil.RWMutex
	}
	// raftMu contains the fields protected by the "write" mutex. For Replica, it
	// points to Replica.raftMu, and shares its semantics and locking order.
	raftMu struct {
		*syncutil.Mutex
		// bytesAccount accounts bytes used by various Raft components, like entries
		// to be applied. Currently, it only tracks bytes used by committed entries
		// being applied to the state machine.
		bytesAccount logstore.BytesAccount
	}
	// shMu contains "shared" fields which are mutated while both mu and raftMu
	// are held for writes. They can be accessed when either of the two mutexes is
	// held.
	shMu struct {
		// trunc contains the raft log truncation state, i.e. the ID of the last
		// entry of the log prefix that has been compacted out from the raft log
		// storage.
		trunc kvserverpb.RaftTruncatedState
		// last is the index/term of the last entry written to the raft log (not
		// necessarily durable locally or committed by the group).
		last logstore.EntryID
		// size is the approximate size in bytes of the persisted raft log,
		// including sideloaded entries' payloads. The value itself is not persisted
		// and is computed lazily, paced by the raft log truncation queue which will
		// recompute the log size when it finds it uninitialized. This recomputation
		// mechanism isn't relevant for ranges which see regular write activity (for
		// those, the log size will deviate from zero quickly, and so it won't be
		// recomputed but will undercount until the first truncation is carried
		// out), but it prevents a large dormant Raft log from sitting around
		// forever, which has caused problems in the past.
		//
		// Note that both size and sizeTrusted do not include the effect of pending
		// log truncations (see Replica.pendingLogTruncations). Hence, they are fine
		// for metrics etc., but not for deciding whether we should create another
		// pending truncation. For the latter, we compute the post-truncation size
		// using pendingLogTruncations.
		size int64
		// If sizeTrusted is false, don't trust the above size until it has been
		// recomputed.
		sizeTrusted bool
		// lastCheckSize is the value of size the last time the Raft log was checked
		// for truncation or at the time of the last Raft log truncation.
		lastCheckSize int64
	}

	// raftEntriesMonitor tracks memory used by raft entries.
	raftEntriesMonitor *logstore.SoftLimit
	// cache provides access to cached raft log entries. Set once, never nil.
	cache *raftentry.Cache
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
	r.raftMu.bytesAccount = r.raftEntriesMonitor.NewAccount(
		r.metrics.RaftLoadedEntriesBytes)
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
	r.mu.AssertHeld()
	// Check whether the first requested entry is already logically truncated. It
	// may or may not be physically truncated, since the RaftTruncatedState is
	// updated before the truncation is enacted.
	if lo <= r.shMu.trunc.Index {
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
		r.ctx, r.ls.Engine, r.ls.RangeID, r.cache, r.ls.Sideload,
		lo, hi, maxBytes,
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
func (r *replicaLogStorage) Term(index uint64) (uint64, error) {
	r.mu.AssertHeld()
	term, err := r.raftTermShMuLocked(kvpb.RaftIndex(index))
	if err != nil {
		r.reportRaftStorageError(err)
	}
	return uint64(term), err
}

func (r *Replica) raftTermShMuLocked(index kvpb.RaftIndex) (kvpb.RaftTerm, error) {
	return r.logStorage.raftTermShMuLocked(index)
}

// raftTermShMuLocked implements the Term() call. Requires that either
// Replica.mu or Replica.raftMu is held, at least for reads.
//
// TODO(pav-kv): figure out a zero-cost-in-prod way to assert that either of two
// mutexes is held. Can't use the regular AssertHeld() here.
func (r *replicaLogStorage) raftTermShMuLocked(index kvpb.RaftIndex) (kvpb.RaftTerm, error) {
	// Check whether the entry is already logically truncated, or is at a bound.
	// It may or may not be physically truncated, since the RaftTruncatedState is
	// updated before the truncation is enacted.
	// NB: two common cases are checked first.
	if r.shMu.last.Index == index {
		return r.shMu.last.Term, nil
	} else if index == r.shMu.trunc.Index {
		return r.shMu.trunc.Term, nil
	} else if index < r.shMu.trunc.Index {
		return 0, raft.ErrCompacted
	} else if index > r.shMu.last.Index {
		return 0, raft.ErrUnavailable
	}
	// Check if the entry is cached, to avoid storage access.
	if entry, found := r.cache.Get(r.ls.RangeID, index); found {
		return kvpb.RaftTerm(entry.Term), nil
	}

	entry, err := logstore.LoadEntry(r.ctx, r.ls.Engine, r.ls.RangeID, index)
	if err != nil {
		return 0, err
	}
	// Cache the entry except if it is sideloaded. We don't load/inline the
	// sideloaded entries here to keep the term fetching cheap.
	//
	// TODO(pav-kv): consider not caching here, after measuring or guessing
	// whether it makes any difference. It might be harmful to the cache since
	// terms tend to be loaded randomly, and the cache assumes moving forward. We
	// also now have the term cache in raft which makes optimizations here
	// unnecessary. Plus, there is a longer-term better solution not involving
	// entry loads: the term cache can be maintained in storage. See #136296.
	if typ, _, err := raftlog.EncodingOf(entry); err != nil {
		return 0, err
	} else if !typ.IsSideloaded() {
		r.cache.Add(r.ls.RangeID, []raftpb.Entry{entry}, false /* truncate */)
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
	return uint64(r.shMu.last.Index)
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
	r.mu.AssertRHeld()
	return uint64(r.shMu.trunc.Index)
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
	if lo <= r.shMu.trunc.Index {
		return nil, raft.ErrCompacted
	}
	entries, _, loadedSize, err := logstore.LoadEntries(
		r.ctx, r.ls.Engine, r.ls.RangeID, r.cache, r.ls.Sideload,
		lo, hi, maxBytes,
		&r.raftMu.bytesAccount,
	)
	r.metrics.RaftStorageReadBytes.Inc(int64(loadedSize))
	return entries, err
}

// Term implements the raft.LogStorageSnapshot interface.
// Requires that r.raftMu is held.
func (r *replicaRaftMuLogSnap) Term(index uint64) (uint64, error) {
	r.raftMu.AssertHeld()
	term, err := (*replicaLogStorage)(r).raftTermShMuLocked(kvpb.RaftIndex(index))
	if err != nil {
		(*replicaLogStorage)(r).reportRaftStorageError(err)
	}
	return uint64(term), err
}

// LastIndex implements the raft.LogStorageSnapshot interface.
// Requires that r.raftMu is held.
func (r *replicaRaftMuLogSnap) LastIndex() uint64 {
	r.raftMu.AssertHeld()
	return uint64(r.shMu.last.Index)
}

// Compacted implements the raft.LogStorageSnapshot interface.
// Requires that r.raftMu is held.
func (r *replicaRaftMuLogSnap) Compacted() uint64 {
	r.raftMu.AssertHeld()
	return uint64(r.shMu.trunc.Index)
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
