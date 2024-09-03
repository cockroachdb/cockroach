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
	"context"

	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// LogMark is a position in a log consistent with the leader at a specific term.
type LogMark = raft.LogMark

// AdmittedVector contains admitted log indices for each priority.
type AdmittedVector struct {
	// Term is the leader term to which the admitted log indices relate to.
	Term uint64
	// Admitted contains admitted indices in the Term's log.
	Admitted [raftpb.NumPriorities]uint64
}

// LogTracker tracks the durable and logically admitted state of a raft log.
//
// Writes to a raft log are ordered by LogMark (term, index) where term is the
// leader term on whose behalf entries are currently written, and index is the
// entry position in the log as seen by this leader. The index can only regress
// if term goes up, and a new leader overwrites a suffix of the log.
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
	// waiting contains, for each priority, entries present in the last.Term log
	// waiting for admission.
	//
	// Invariants:
	//	- waiting[pri][i].Index <= last.Index
	//	- waiting[pri][i].Term <= last.Term
	//	- waiting[pri][i].Index < waiting[pri][i+1].Index
	//	- waiting[pri][i].Term <= waiting[pri][i+1].Term
	waiting [raftpb.NumPriorities][]LogMark
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

// Admitted returns the current admitted log state, tied to the latest observed
// leader term.
//
// The admitted index for a priority is computed as one before the min entry
// index in the waiting queue. The indices are capped at the stable log index,
// and bumped to stable index for priorities which don't have any entries
// waiting for admission.
//
// Guarantees:
//   - the term never regresses,
//   - admitted indices never regress if the term does not change,
//   - indices converge to the stable index which converges to the last index.
func (l *LogTracker) Admitted() AdmittedVector {
	a := AdmittedVector{Term: l.last.Term}
	for pri := range a.Admitted {
		index := l.stable
		if len(l.waiting[pri]) != 0 {
			index = min(index, l.waiting[pri][0].Index-1)
		}
		a.Admitted[pri] = index
	}
	return a
}

// Append informs the tracker that log entries at indices (after, to.Index] are
// about to be sent to stable storage, on behalf of the to.Term leader.
//
// All log storage writes must be registered with the Append call, ordered by
// LogMark, with no gaps. Any entries in the (after, to.Index] batch, that are
// subject to admission control, should be registered with the Register call
// before the batch is sent to storage.
func (l *LogTracker) Append(ctx context.Context, after uint64, to LogMark) {
	// Fast path. We are at the same term. The log must be contiguous.
	if to.Term == l.last.Term {
		if after != l.last.Index {
			l.errorf(ctx, "append (%d,%d]@%d out of order", after, to.Index, to.Term)
			return
		}
		l.last.Index = to.Index
		return
	}
	if to.Term < l.last.Term || after > l.last.Index {
		// Does not happen. Log writes are always ordered by LogMark, and have no
		// gaps. Gaps can only appear when the log is cleared in response to storing
		// a snapshot, which is handled by the SnapSynced method.
		l.errorf(ctx, "append (%d,%d]@%d out of order", after, to.Index, to.Term)
		return
	}
	// Invariant: to.Term > l.last.Term && after <= l.last.Index.
	l.last = to
	// The effective stable and admitted indices can potentially regress here.
	// This happens when a new leader overwrites a suffix of the log.
	l.stable = min(l.stable, after)
	// Entries at index > after.Index from previous terms are now obsolete.
	for pri, marks := range l.waiting {
		l.waiting[pri] = truncate(marks, after)
	}
}

// Register informs the tracker that the entry at the given log mark is about to
// be sent to admission queue with the given priority. Must be called in the
// LogMark order, at most once for each entry.
//
// Typically, every batch of appended log entries should call Append once,
// followed by a sequence of Register calls for individual entries that are
// subject to admission control.
func (l *LogTracker) Register(ctx context.Context, at LogMark, pri raftpb.Priority) {
	if at.Term != l.last.Term || at.Index <= l.stable {
		// Does not happen. Entries must be registered before being sent to storage,
		// so an entry can't become stable before the Register call.
		l.errorf(ctx, "admission register %+v [pri=%v] out of order", at, pri)
		return
	}
	ln := len(l.waiting[pri])
	if ln != 0 && at.Index <= l.waiting[pri][ln-1].Index {
		l.errorf(ctx, "admission register %+v [pri=%v] out of order", at, pri)
		return
	}
	l.waiting[pri] = append(l.waiting[pri], at)
}

// LogSynced informs the tracker that the log up to the given LogMark has been
// persisted to stable storage.
//
// All writes are done in the LogMark order, but the corresponding LogSynced
// calls can be skipped or invoked in any order, e.g. due to delivery
// concurrency. The tracker keeps the latest (in the logical sense) stable mark.
func (l *LogTracker) LogSynced(ctx context.Context, stable LogMark) {
	if stable.After(l.last) {
		// Does not happen. The write must have been registered with Append call.
		l.errorf(ctx, "syncing mark %+v before appending it", stable)
		return
	}
	// TODO(pav-kv): we can move the stable index up for a stale Term too, if we
	// track leader term for each entry (see LogAdmitted), or forks of the log.
	if stable.Term == l.last.Term && stable.Index > l.stable {
		l.stable = stable.Index
	}
}

// LogAdmitted informs the tracker that the log up to the given LogMark has been
// logically admitted to storage, at the given priority.
//
// All writes are done in the LogMark order, but the corresponding LogAdmitted
// calls can be skipped or invoked in any order, e.g. due to delivery
// concurrency. The tracker accounts for the latest (in the logical sense)
// admission.
//
// LogAdmitted and LogSynced for the same entry/mark can happen in any order
// too, since the write and admission queues are decoupled from each other, and
// there can be concurrency in the signal delivery. LogTracker dampens this by
// capping admitted indices at stable index (see Admitted), so that admissions
// appear to happen after log syncs.
func (l *LogTracker) LogAdmitted(ctx context.Context, at LogMark, pri raftpb.Priority) {
	if at.After(l.last) {
		// Does not happen. The write must have been registered with Append call.
		l.errorf(ctx, "admitting mark %+v before appending it", at)
		return
	}
	waiting := l.waiting[pri]
	// There is nothing to admit, or it's a stale admission.
	if len(waiting) == 0 || at.Index < waiting[0].Index {
		return
	}
	// Remove waiting entries preceding the admitted mark. Due to invariants, this
	// is always a prefix of the queue.
	for i, mark := range waiting {
		if mark.After(at) {
			l.waiting[pri] = waiting[i:]
			return
		}
	}
	// The entire queue is admitted, clear it.
	l.waiting[pri] = waiting[len(waiting):]
}

// SnapSynced informs the tracker that a snapshot at the given log mark has been
// stored/synced, and the log is cleared.
func (l *LogTracker) SnapSynced(ctx context.Context, mark LogMark) {
	if !mark.After(l.last) {
		l.errorf(ctx, "syncing stale snapshot %+v", mark)
		return
	}
	// Fake an append spanning the gap between the log and the snapshot. It will,
	// if necessary, truncate the stable index and remove entries waiting for
	// admission that became obsolete.
	l.Append(ctx, min(l.last.Index, mark.Index), mark)
	l.LogSynced(ctx, mark)
}

// String returns a string representation of the LogTracker.
func (l *LogTracker) String() string {
	return redact.StringWithoutMarkers(l)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (l *LogTracker) SafeFormat(w redact.SafePrinter, _ rune) {
	admitted := l.Admitted().Admitted
	w.Printf("mark:%+v, stable:%d, admitted:%v", l.last, l.stable, admitted)
}

func (l *LogTracker) errorf(ctx context.Context, format string, args ...any) {
	format += "\n%s"
	args = append(args, l.String())
	if buildutil.CrdbTestBuild {
		panic(errors.AssertionFailedf(format, args...))
	} else {
		log.Errorf(ctx, format, args...)
	}
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
