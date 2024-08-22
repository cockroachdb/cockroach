// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rac2

import (
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/redact"
)

// LogMark is a position in a log consistent with the leader at a specific term.
type LogMark = raft.LogMark

// AdmittedVector contains admitted log indices for each priority.
type AdmittedVector [raftpb.NumPriorities]uint64

// LogTracker tracks the durable and logically admitted state of a raft log.
//
// Writes to a raft log are ordered by LogMark (term, index) where term is the
// leader term on whose behalf entries are currently written, and index is the
// entry position in the log as seen by this leader.
//
// Integration with storage guarantees that completion of a (term, index)
// write/sync means that all writes made under lower terms, or same term and
// lower log indices, have been completed. A similar guarantee comes for
// admissions at each priority.
type LogTracker struct {
	// last is the latest log mark observed by the tracker.
	last LogMark
	// stable is the durable log index, in the last.Term log coordinate system.
	// Entries in (stable, last.Index] are still in the write queue.
	stable uint64
	// admitted contains admitted log indices for each priority, in the last.Term
	// log coordinate system. Entries in (admitted[pri], last.Index], if any, are
	// still in the admission queue, and are tracked by the waiting slice below.
	admitted AdmittedVector
	// waiting contains, for each priority, entries waiting for admission.
	//
	// Invariants:
	//	- waiting[pri][i].Term <= last.Term
	//	- admitted[pri] < waiting[pri][i].Index <= last.Index
	//	- waiting[pri][i].Index < waiting[pri][i+1].Index
	//	- waiting[pri][i].Term <= waiting[pri][i+1].Term
	waiting [raftpb.NumPriorities][]LogMark
}

// NewLogTracker returns a LogTracker initialized to the given log mark. The
// caller must make sure that the log mark is durable.
func NewLogTracker(stable LogMark) LogTracker {
	var admitted AdmittedVector
	for pri := range admitted {
		admitted[pri] = stable.Index
	}
	return LogTracker{last: stable, stable: stable.Index, admitted: admitted}
}

// Stable returns the currently observed stable log mark.
func (l *LogTracker) Stable() LogMark {
	return LogMark{Term: l.last.Term, Index: l.stable}
}

// Admitted returns the currently observed admitted log state, tied to the
// latest observed leader term.
//
// The admitted indices are capped at the stable log index, and bumped to stable
// index for priorities which don't have any entries waiting for admission.
func (l *LogTracker) Admitted() (term uint64, _ AdmittedVector) {
	var admitted AdmittedVector
	for pri, index := range l.admitted {
		if len(l.waiting[pri]) == 0 {
			admitted[pri] = l.stable
		} else {
			admitted[pri] = min(index, l.stable)
		}
	}
	return l.last.Term, admitted
}

// Append informs the tracker that log entries at indices (after, to.Index] are
// about to be sent to stable storage, on behalf of the to.Term leader.
//
// All log storage writes must be registered with the Append call, ordered by
// LogMark, with no gaps.
func (l *LogTracker) Append(after uint64, to LogMark) bool {
	// Fast path. We are at the same term. The log must be contiguous.
	if to.Term == l.last.Term {
		if after != l.last.Index {
			return false
		}
		l.last.Index = to.Index
		return true
	}
	if to.Term < l.last.Term || after > l.last.Index {
		// Does not happen. Log writes are always ordered by LogMark, and have no
		// gaps. Gaps can only appear when the log is cleared in response to storing
		// a snapshot, which is handled by the Snap method.
		return false
	}
	// Invariant: after.Term > l.last.Term && after.Index <= l.last.Index.
	l.last = to
	// The effective stable and admitted indices can potentially regress here.
	// This happens when a new leader overwrites a suffix of the log.
	l.stable = min(l.stable, after)
	for pri, index := range l.admitted {
		l.admitted[pri] = min(index, after)
	}
	// Entries at index > after.Index from previous terms are now obsolete.
	for pri, marks := range l.waiting {
		l.waiting[pri] = truncate(marks, after)
	}
	return true
}

// Register informs the tracker that the entry at the given log mark is about to
// be sent to admission queue with the given priority. Must be called in the
// LogMark order, at most once for each entry.
//
// Typically, every batch of appended log entries should call Append once,
// followed by a sequence of Register calls for individual entries that are
// subject to admission control.
func (l *LogTracker) Register(at LogMark, pri raftpb.Priority) bool {
	if at.Term != l.last.Term || at.Index <= l.admitted[pri] {
		return false
	}
	ln := len(l.waiting[pri])
	if ln != 0 && !at.After(l.waiting[pri][ln-1]) {
		return false
	}
	l.waiting[pri] = append(l.waiting[pri], at)
	return true
}

// LogSynced informs the tracker that the log up to the given LogMark has been
// persisted to stable storage.
//
// All writes are done in the LogMark order, but the corresponding LogSynced
// calls can be skipped or invoked in any order, e.g. due to delivery
// concurrency. The tracker keeps the latest (in the logical sense) stable mark.
func (l *LogTracker) LogSynced(stable LogMark) bool {
	if stable.After(l.last) {
		// Does not happen. The write must have been registered with Append call.
		return false
	}
	// Invariant: Term <= l.last.Term.
	if stable.Term == l.last.Term {
		l.stable = max(l.stable, stable.Index)
	}
	// TODO(pav-kv): we can move the stable index up for a stale Term if we track
	// leader term for each entry, or forks of the log. See the Admit method.
	return true
}

// Admit informs the tracker that the log up to the given LogMark has been
// logically admitted to storage, at the given priority.
//
// All writes are done in the LogMark order, but the corresponding Admit calls
// can be skipped or invoked in any order, e.g. due to delivery concurrency. The
// tracker keeps the latest (in the logical sense) admitted marks.
func (l *LogTracker) Admit(at LogMark, pri raftpb.Priority) bool {
	if at.After(l.last) {
		// Does not happen. The write must have been registered with Append call.
		return false
	}
	waiting := skip(l.waiting[pri], at)
	if len(waiting) != 0 {
		l.admitted[pri] = waiting[0].Index - 1
	} else if at.Term == l.last.Term {
		l.admitted[pri] = max(l.admitted[pri], at.Index)
	}
	l.waiting[pri] = waiting
	return true
}

// SnapSynced informs the tracker that a snapshot at the given log mark has been
// stored/synced, and the log is cleared.
func (l *LogTracker) SnapSynced(mark LogMark) bool {
	if !mark.After(l.last) {
		return false
	}
	l.last = mark
	l.stable = mark.Index
	return true
}

// String returns a string representation of the LogTracker.
func (l *LogTracker) String() string {
	return redact.StringWithoutMarkers(l)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (l *LogTracker) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("mark:%+v, stable:%d, admitted:%v", l.last, l.stable, l.admitted)
}

// skip returns a suffix of the ordered log marks slice, with all marks <= until
// removed from it.
func skip(marks []LogMark, until LogMark) []LogMark {
	for i, ln := 0, len(marks); i < ln; i++ {
		if marks[i].After(until) {
			return marks[i:]
		}
	}
	return marks[len(marks):]
}

// truncate returns a prefix of the ordered log marks slice, with all marks at
// index > after removed from it.
func truncate(marks []LogMark, after uint64) []LogMark {
	for i := len(marks); i > 0; i-- {
		if marks[i-1].Index <= after {
			return marks[:i]
		}
	}
	return marks[:0]
}
