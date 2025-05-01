// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/raft"
)

// Raft log storage writes are carried out under raftMu which blocks all other
// writes for this raft instance. Note that it does not prevent concurrent log
// reads from within RawNode when it only holds Replica.mu. However, RawNode
// never attempts to read log indices that have pending writes. See also the
// replicaRaftStorage comment for more details.
//
// For performance reasons, the raft log storage state (such as the last index)
// needs to be accessible under Replica.mu. When appending is done, the state
// needs to be transferred into the Replica.mu section.
//
// Appends can truncate a suffix of the log and overwrite it with entries
// proposed by a new leader. An important corollary is that the log state
// observed by Replica.mu-only sections is stale, because there can be a
// concurrent raftMu writer changing these indices/entries.
//
// TODO(pav-kv): audit if there are any places relying on this stale raft log
// state too heavily. The consistent source of truth about the raft log state
// under Replica.mu is the RawNode / unstable, and preferably should be used.
//
// Raft log storage writes are asynchronous, in a limited sense. The write
// batches are committed to Pebble immediately, but flush/sync is communicated
// asynchronously (with a few synchronous case exceptions, see the code down the
// appendRaftMuLocked stack).
//
// The sync acknowledgements from storage are stashed in Replica.localMsgs and
// delivered back to RawNode by deliverLocalRaftMsgsRaftMuLockedReplicaMuLocked
// at the next opportunity.
//
// TODO(pav-kv): move and describe raft log compaction lifecycle code here.

// stateRaftMuLocked returns the current raft log storage state.
func (r *replicaLogStorage) stateRaftMuLocked() logstore.RaftState {
	return logstore.RaftState{
		LastIndex: r.shMu.last.Index,
		LastTerm:  r.shMu.last.Term,
		ByteSize:  r.shMu.size,
	}
}

// appendRaftMuLocked carries out a raft log append, and returns the new raft
// log storage state.
func (r *replicaLogStorage) appendRaftMuLocked(
	ctx context.Context, app raft.StorageAppend, stats *logstore.AppendStats,
) (logstore.RaftState, error) {
	r.raftMu.AssertHeld()
	state := r.stateRaftMuLocked()
	state, err := r.ls.StoreEntries(ctx, state, app, r.onSync, stats)
	if err != nil {
		return state, err
	}
	// Update raft log entry cache. We clear any older, uncommitted log entries
	// and cache the latest ones.
	//
	// In the blocking log sync case, these entries are already durable. In the
	// non-blocking case, these entries have been written to storage (so reads of
	// the engine will see them), but they are not yet durable. This means that
	// the entry cache can lead the durable log. This is allowed by raft, which
	// maintains its own tracking of entry durability by splitting its log into an
	// unstable portion for entries that are not known to be durable and a stable
	// portion for entries that are known to be durable.
	//
	// TODO(pav-kv): for safety, decompose this update into two steps: before
	// writing to storage, truncate the suffix of the cache if entries are
	// overwritten; after the write, append new entries to the cache. Currently,
	// the cache can contain stale entries while storage is already updated, and
	// the only hope is that nobody tries to read it under Replica.mu only.
	r.cache.Add(r.ls.RangeID, app.Entries, true /* truncate */)
	return state, nil
}

// updateStateRaftMuLockedMuLocked updates the in-memory reflection of the raft
// log storage state.
func (r *replicaLogStorage) updateStateRaftMuLockedMuLocked(state logstore.RaftState) {
	r.shMu.last.Index = state.LastIndex
	r.shMu.last.Term = state.LastTerm
	r.shMu.size = state.ByteSize
}

// updateLogSize recomputes the raft log size, and updates Replica's in-memory
// knowledge about this size. Returns the computed log size.
//
// Replica.{raftMu,mu} must not be held.
func (r *replicaLogStorage) updateLogSize(ctx context.Context) (int64, error) {
	// We need to hold raftMu both to access the sideloaded storage and to make
	// sure concurrent raft activity doesn't foul up our update to the cached
	// in-memory values.
	r.raftMu.Lock()
	defer r.raftMu.Unlock()

	size, err := r.ls.ComputeSize(ctx)
	if err != nil {
		return 0, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.shMu.size = size
	r.shMu.lastCheckSize = size
	r.shMu.sizeTrusted = true
	return size, nil
}
