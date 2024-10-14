// This code has been modified from its original form by The Cockroach Authors.
// All modifications are Copyright 2024 The Cockroach Authors.
//
// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"github.com/cockroachdb/cockroach/pkg/raft/raftlogger"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
)

// unstable is a suffix of the raft log pending to be written to Storage. The
// "log" can be represented by a snapshot, and/or a contiguous slice of entries.
//
// The possible states:
//  1. Both the snapshot and the entries LogSlice are empty. This means the log
//     is fully in Storage. The LogSlice.prev is the lastEntryID of the log.
//  2. The snapshot is empty, and the LogSlice is non-empty. The state up to
//     (including) LogSlice.prev is in Storage, and the LogSlice is pending.
//  3. The snapshot is non-empty, and the LogSlice is empty. The snapshot
//     overrides the entire log in Storage.
//  4. Both the snapshot and LogSlice are non-empty. The snapshot immediately
//     precedes the entries, i.e. LogSlice.prev == snapshot.lastEntryID. This
//     state overrides the entire log in Storage.
//
// The type serves two roles. First, it holds on to the latest snapshot / log
// entries until they are handed to storage for persistence (via Ready API) and
// subsequently acknowledged. This provides the RawNode with a consistent view
// on the latest state of the log: the raftLog struct combines a log prefix from
// Storage with the suffix held by unstable.
//
// Second, it supports the asynchronous log writes protocol. The snapshot and
// the entries are handed to storage in the order that guarantees consistency of
// the raftLog. Writes on the storage happen in the same order, and issue
// acknowledgements delivered back to unstable. On acknowledgement, the snapshot
// and the entries are released from memory when it is safe to do so. There is
// no strict requirement on the order of acknowledgement delivery.
//
// TODO(pav-kv): describe the order requirements in more detail when accTerm
// (LogSlice.term) is integrated into the protocol.
//
// Note that the in-memory prefix of the log can contain entries at indices less
// than Storage.LastIndex(). This means that the next write to storage might
// need to truncate the log before persisting the new suffix. Such a situation
// happens when there is a leader change, and the new leader overwrites entries
// that haven't been committed yet.
//
// TODO(pav-kv): decouple the "in progress" part into a separate struct which
// drives the storage write protocol.
type unstable struct {
	// snapshot is the pending unstable snapshot, if any.
	//
	// Invariant: snapshot == nil ==> !snapshotInProgress
	// Invariant: snapshot != nil ==> snapshot.lastEntryID == LogSlice.prev
	//
	// The last invariant enforces the order of handling a situation when there is
	// both a snapshot and entries. The snapshot write must be acknowledged first,
	// before entries are acknowledged and the LogSlice moves forward.
	snapshot *pb.Snapshot

	// LogSlice is the suffix of the raft log that is not yet written to storage.
	// If all the entries are written, or covered by the pending snapshot, then
	// LogSlice.entries is empty.
	//
	// Invariant: snapshot != nil ==> LogSlice.prev == snapshot.lastEntryID
	// Invariant: snapshot == nil ==> LogSlice.prev is in Storage
	// Invariant: LogSlice.lastEntryID() is the end of the log at all times
	//
	// Invariant: LogSlice.term, a.k.a. the "last accepted term", is the term of
	// the leader whose append (either entries or snapshot) we accepted last. Our
	// state is consistent with the leader log at this term.
	LogSlice

	// snapshotInProgress is true if the snapshot is being written to storage.
	//
	// Invariant: snapshotInProgress ==> snapshot != nil
	snapshotInProgress bool
	// entryInProgress is the index of the last entry in LogSlice already present
	// in, or being written to storage.
	//
	// Invariant: prev.index <= entryInProgress <= lastIndex()
	// Invariant: entryInProgress > prev.index && snapshot != nil ==> snapshotInProgress
	//
	// The last invariant enforces the order of handling a situation when there is
	// both a snapshot and entries. The snapshot must be sent to storage first, or
	// together with the entries.
	entryInProgress uint64

	logger raftlogger.Logger
}

func newUnstable(last entryID, logger raftlogger.Logger) unstable {
	// To initialize the last accepted term (LogSlice.term) correctly, we make
	// sure its invariant is true: the log is a prefix of the term's leader's log.
	// This can be achieved by conservatively initializing to the term of the last
	// log entry.
	//
	// We can't pick any lower term because the lower term's leader can't have
	// last.term entries in it. We can't pick a higher term because we don't have
	// any information about the higher-term leaders and their logs. So last.term
	// is the only valid choice.
	//
	// TODO(pav-kv): persist the accepted term in HardState and load it. Our
	// initialization is conservative. Before restart, the accepted term could
	// have been higher. Setting a higher term (ideally, matching the current
	// leader Term) gives us more information about the log, and then allows
	// bumping its commit index sooner than when the next MsgApp arrives.
	return unstable{
		LogSlice:        LogSlice{term: last.term, prev: last},
		entryInProgress: last.index,
		logger:          logger,
	}
}

// maybeFirstIndex returns the index of the first possible entry in entries
// if it has a snapshot.
func (u *unstable) maybeFirstIndex() (uint64, bool) {
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index + 1, true
	}
	return 0, false
}

// nextEntries returns the unstable entries that are not already in the process
// of being written to storage.
func (u *unstable) nextEntries() []pb.Entry {
	if u.entryInProgress == u.lastIndex() {
		return nil
	}
	return u.entries[u.entryInProgress-u.prev.index:]
}

// nextSnapshot returns the unstable snapshot, if one exists that is not already
// in the process of being written to storage.
func (u *unstable) nextSnapshot() *pb.Snapshot {
	if u.snapshot == nil || u.snapshotInProgress {
		return nil
	}
	return u.snapshot
}

// acceptInProgress marks all entries and the snapshot, if any, in the unstable
// as having begun the process of being written to storage. The entries/snapshot
// will no longer be returned from nextEntries/nextSnapshot. However, new
// entries/snapshots added after a call to acceptInProgress will be returned
// from those methods, until the next call to acceptInProgress.
func (u *unstable) acceptInProgress() {
	u.snapshotInProgress = u.snapshot != nil
	u.entryInProgress = u.lastIndex()
}

// stableTo marks entries up to the entry at the specified (term, index) mark as
// being successfully written to stable storage.
//
// The method makes sure the entries can not be overwritten by an in-progress
// log append. See the related comment in newStorageAppendRespMsg.
func (u *unstable) stableTo(mark LogMark) {
	if mark.Term != u.term {
		// The last accepted term has changed. Ignore. This is possible if part or
		// all of the unstable log was replaced between that time that a set of
		// entries started to be written to stable storage and when they finished.
		u.logger.Infof("mark (term,index)=(%d,%d) mismatched the last accepted "+
			"term %d in unstable log; ignoring ", mark.Term, mark.Index, u.term)
		return
	}
	if u.snapshot != nil && mark.Index == u.snapshot.Metadata.Index {
		// Index matched unstable snapshot, not unstable entry. Ignore.
		u.logger.Infof("entry at index %d matched unstable snapshot; ignoring", mark.Index)
		return
	}
	if mark.Index <= u.prev.index || mark.Index > u.lastIndex() {
		// Unstable entry missing. Ignore.
		u.logger.Infof("entry at index %d missing from unstable log; ignoring", mark.Index)
		return
	}
	if u.snapshot != nil {
		u.logger.Panicf("mark %+v acked earlier than the snapshot(in-progress=%t): %s",
			mark, u.snapshotInProgress, DescribeSnapshot(*u.snapshot))
	}
	u.LogSlice = u.forward(mark.Index)
	// TODO(pav-kv): why can mark.index overtake u.entryInProgress? Probably bugs
	// in tests using the log writes incorrectly, e.g. TestLeaderStartReplication
	// takes nextUnstableEnts() without acceptInProgress().
	u.entryInProgress = max(u.entryInProgress, mark.Index)
	u.shrinkEntriesArray()
}

// shrinkEntriesArray discards the underlying array used by the entries slice
// if most of it isn't being used. This avoids holding references to a bunch of
// potentially large entries that aren't needed anymore. Simply clearing the
// entries wouldn't be safe because clients might still be using them.
func (u *unstable) shrinkEntriesArray() {
	// We replace the array if we're using less than half of the space in
	// it. This number is fairly arbitrary, chosen as an attempt to balance
	// memory usage vs number of allocations. It could probably be improved
	// with some focused tuning.
	const lenMultiple = 2
	if len(u.entries) == 0 {
		u.entries = nil
	} else if len(u.entries)*lenMultiple < cap(u.entries) {
		newEntries := make([]pb.Entry, len(u.entries))
		copy(newEntries, u.entries)
		u.entries = newEntries
	}
}

func (u *unstable) stableSnapTo(i uint64) {
	if u.snapshot != nil && u.snapshot.Metadata.Index == i {
		u.snapshotInProgress = false
		u.snapshot = nil
	}
}

// restore resets the state to the given snapshot. It effectively truncates the
// log after s.lastEntryID(), so the caller must ensure this is safe.
func (u *unstable) restore(s snapshot) bool {
	// All logs >= s.term are consistent with the log covered by the snapshot. If
	// our log is such, disallow restoring a snapshot at indices below our last
	// index, to avoid truncating a meaningful suffix of the log and losing its
	// durability which could have been used to commit entries in this suffix.
	//
	// In this case, the caller must have advanced the commit index instead.
	// Alternatively, we could retain a suffix of the log instead of truncating
	// it, but at the time of writing the whole stack (including storage) is
	// written such that a snapshot always clears the log.
	if s.term <= u.term && s.lastIndex() < u.lastIndex() {
		return false
	}
	// If s.term <= u.term, our log is consistent with the snapshot. Set the new
	// accepted term to the max of the two so that u.term does not regress. At the
	// time of writing, s.term is always >= u.term, because s.term == raft.Term
	// and u.term <= raft.Term. It could be relaxed in the future.
	term := max(u.term, s.term)

	u.snapshot = &s.snap
	u.LogSlice = LogSlice{term: term, prev: s.lastEntryID()}
	u.snapshotInProgress = false
	u.entryInProgress = u.prev.index
	return true
}

// append adds the given log slice to the end of the log. Returns false if this
// can not be done.
func (u *unstable) append(a LogSlice) bool {
	if a.term < u.term {
		return false // append from an outdated log
	} else if a.prev != u.lastEntryID() {
		return false // not a valid append at the end of the log
	}
	u.term = a.term // update the last accepted term
	u.entries = append(u.entries, a.entries...)
	return true
}

func (u *unstable) truncateAndAppend(a LogSlice) bool {
	if a.term < u.term {
		return false // append from an outdated log
	}
	// Fast path for appends at the end of the log.
	last := u.lastEntryID()
	if a.prev == last {
		u.term = a.term // update the last accepted term
		u.entries = append(u.entries, a.entries...)
		return true
	}
	// If a.prev.index > last.index, we can not accept this write because it will
	// introduce a gap in the log.
	//
	// If a.prev.index == last.index, then the last entry term did not match in
	// the check above, so we must reject this case too.
	if a.prev.index >= last.index {
		return false
	}
	// Below, we handle the index regression case, a.prev.index < last.index.
	//
	// Within the same leader term, we enforce the log to be append-only, and only
	// allow index regressions (which cause log truncations) when a.term > u.term.
	if a.term == u.term {
		return false
	}

	// The caller checks that a.prev.index >= commit, i.e. we are not truncating
	// committed entries. By extension, a.prev.index >= commit >= snapshot.index.
	// So we do not expect the following check to fail.
	//
	// It is a defense-in-depth guarding the invariant: if snapshot != nil then
	// prev == snapshot.{term,index}. The code regresses prev, so we don't want
	// the snapshot ID to get out of sync with it.
	if u.snapshot != nil && a.prev.index < u.snapshot.Metadata.Index {
		u.logger.Panicf("appending at %+v before snapshot %s", a.prev, DescribeSnapshot(*u.snapshot))
		return false
	}

	// Truncate the log and append new entries. Regress the entryInProgress mark
	// to reflect that the truncated entries are no longer considered in progress.
	if a.prev.index <= u.prev.index {
		u.LogSlice = a // replace the entire LogSlice with the latest append
		// TODO(pav-kv): clean up the logging message. It will change all datadriven
		// test outputs, so do it in a contained PR.
		u.logger.Infof("replace the unstable entries from index %d", a.prev.index+1)
	} else {
		u.term = a.term // update the last accepted term
		// Use the full slice expression to cause copy-on-write on this or a
		// subsequent (if a.entries is empty) append to u.entries. The truncated
		// part of the old slice can still be referenced elsewhere.
		keep := u.entries[:a.prev.index-u.prev.index]
		u.entries = append(keep[:len(keep):len(keep)], a.entries...)
		u.logger.Infof("truncate the unstable entries before index %d", a.prev.index+1)
	}
	u.entryInProgress = min(u.entryInProgress, a.prev.index)
	return true
}
