// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
)

// Raft log storage writes are carried out under raftMu which blocks all other
// writes for this raft instance. Note that it does not prevent concurrent log
// reads from within RawNode when it only holds Replica.mu. However, RawNode
// never attempts to read log indices that have pending writes. See also the
// replicaRaftStorage comment for more details.
//
// TODO(#131063): there is one subtle exception - the log can be compacted
// concurrently with reading from its "readable" prefix. Incorrect ordering of
// raftMu/mu updates during log compactions can cause read attempts for deleted
// indices. We should fix it.
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
		LastIndex: r.shMu.lastIndexNotDurable,
		LastTerm:  r.shMu.lastTermNotDurable,
		ByteSize:  r.shMu.raftLogSize,
	}
}

// appendRaftMuLocked carries out a raft log append, and returns the new raft
// log storage state.
func (r *replicaLogStorage) appendRaftMuLocked(
	ctx context.Context, app logstore.MsgStorageAppend, stats *logstore.AppendStats,
) (logstore.RaftState, error) {
	state := r.stateRaftMuLocked()
	cb := (*replicaSyncCallback)(r)
	return r.raftMu.logStorage.StoreEntries(ctx, state, app, cb, stats)
}

// updateStateRaftMuLockedMuLocked updates the in-memory reflection of the raft
// log storage state.
func (r *replicaLogStorage) updateStateRaftMuLockedMuLocked(state logstore.RaftState) {
	r.shMu.lastIndexNotDurable = state.LastIndex
	r.shMu.lastTermNotDurable = state.LastTerm
	r.shMu.raftLogSize = state.ByteSize
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

	size, err := r.raftMu.logStorage.ComputeSize(ctx)
	if err != nil {
		return 0, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.shMu.raftLogSize = size
	r.shMu.raftLogLastCheckSize = size
	r.shMu.raftLogSizeTrusted = true
	return size, nil
}
