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

import "github.com/cockroachdb/cockroach/pkg/raft"

// LogMark is a position in a log consistent with the leader at a specific term.
type LogMark = raft.LogMark

// LogTracker tracks the durable state of a raft log.
//
// Writes to a raft log are ordered by LogMark (term, index) where term is the
// leader term on whose behalf entries are currently written, and index is the
// entry position in the log as seen by this leader.
//
// Integration with storage guarantees that completion of a (term, index)
// write/sync means that all writes made under lower terms, or same term and
// lower log indices, have been completed.
type LogTracker struct {
	// last is the latest log mark observed by the tracker.
	last LogMark
	// stable is the durable log index, in the last.Term log coordinate system.
	// Entries in (stable, last.Index] are still in the write queue.
	stable uint64
	// TODO(pav-kv): track log entry admissions too.
}

// NewLogTracker returns a LogTracker initialized to the given log mark. The
// caller must make sure that the log mark is durable.
func NewLogTracker(stable LogMark) LogTracker {
	return LogTracker{last: stable, stable: stable.Index}
}

// Stable returns the currently observed stable log mark.
func (l *LogTracker) Stable() LogMark {
	return LogMark{Term: l.last.Term, Index: l.stable}
}

// NextIndex returns the index following the last observed log index.
func (l *LogTracker) NextIndex() uint64 {
	return l.last.Index + 1
}

// Append informs the tracker that entries with indices (after, to.Index] have
// been sent to storage write queue on behalf of the to.Term leader.
func (l *LogTracker) Append(after uint64, to LogMark) bool {
	if to.Term < l.last.Term || after > l.last.Index {
		// Does not happen. Log writes are always ordered by (term, index), and have
		// no gaps. Gaps can only appear when the log is cleared in response to
		// storing a snapshot, which is handled by the Snap method.
		return false
	} else if to.Term > l.last.Term {
		l.last = to
		// NB: the effective stable index can potentially regress here, when the
		// leader term goes up. This corresponds to a situation when a new leader
		// overwrites a suffix of the log.
		l.stable = min(l.stable, after)
		return true
	}
	// Below, we handle the to.Term == l.last.Term case.
	if after != l.last.Index {
		// Does not happen. Log writes under the same leader don't have gaps.
		return false
	}
	l.last.Index = to.Index
	return true
}

// Sync informs the tracker that the log up to the given LogMark has been
// persisted to stable storage.
//
// All writes are done in the LogMark order, but the Sync calls corresponding to
// completed writes can be skipped or invoked in any order, e.g. due to delivery
// concurrency. The tracker keeps the latest (in the logical sense) stable mark.
func (l *LogTracker) Sync(stable LogMark) bool {
	if stable.After(l.last) {
		// Does not happen. The write must have been registered with Append call.
		return false
	}
	// Invariant: Term <= l.last.Term.
	if stable.Term == l.last.Term {
		l.stable = max(l.stable, stable.Index)
	}
	// TODO(pav-kv): we can move the stable index up for a stale Term if we track
	// leader term for each entry, or forks of the log.
	return true
}

// Snap informs the tracker that a snapshot at the given log mark has been
// stored/synced, and the log is cleared.
func (l *LogTracker) Snap(mark LogMark) bool {
	if !mark.After(l.last) {
		return false
	}
	l.last = mark
	l.stable = mark.Index
	return true
}
