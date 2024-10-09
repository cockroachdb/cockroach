// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rac2

import (
	"context"
	"fmt"
	"strings"

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

// Merge merges two admitted vectors into one. A higher-term vector always wins.
// If the terms match, the admitted indices are computed as max of the two, for
// each priority.
func (av AdmittedVector) Merge(other AdmittedVector) AdmittedVector {
	if other.Term > av.Term {
		return other
	} else if other.Term < av.Term {
		return av
	}
	for pri, my := range av.Admitted {
		if their := other.Admitted[pri]; their > my {
			av.Admitted[pri] = their
		}
	}
	return av
}

func (av AdmittedVector) String() string {
	return redact.StringWithoutMarkers(av)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (av AdmittedVector) SafeFormat(w redact.SafePrinter, _ rune) {
	var buf redact.StringBuilder
	buf.Printf("term:%d, admitted:[", av.Term)
	for pri, index := range av.Admitted {
		if pri > 0 {
			buf.Printf(",")
		}
		buf.Printf("%s:%d", raftpb.Priority(pri), index)
	}
	buf.Printf("]")
	w.Printf("%v", buf)
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
//
// For admissions, we've chosen to track individual indices instead of the
// latest index, to allow the system to self correct under inconsistencies
// between the sender (leader) and receiver (replica). For instance, consider
// the case that the sender was tracking indices (3, 5, 8) under a certain
// priority, while the receiver only tracked (3, 8). When 3 is admitted at the
// receiver, the admitted index can advance to 7, allowing 5 to be considered
// admitted at the leader. In comparison, if we only tracked the latest index 8,
// then when 3 was admitted, we could only advance the admitted index to 3.
//
// A secondary reason to track individual indices is that it naturally allows
// the admitted index to advance to stable index without lag in the case where
// there is continuous traffic, but sparseness of indices for a priority. For
// example, if we have indices (10, 20, 30, 40, ...) coming in at high priority,
// and entries are getting admitted with some lag, then when stable=25 and entry
// 20 is admitted, we can advance the admitted index to 25, and not wait for 30
// to be admitted too.
//
// We can revisit the decision to track individual indices if we find the memory
// or compute overhead to be significant.
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
	waiting [raftpb.NumPriorities]CircularBuffer[LogMark]
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
	for pri := range l.waiting {
		index := l.stable
		if l.waiting[pri].Length() != 0 {
			index = min(index, l.waiting[pri].At(0).Index-1)
		}
		a.Admitted[pri] = index
	}
	return a
}

// Snap informs the tracker that a snapshot at the given log mark is about to be
// sent to storage, and the log will be truncated.
//
// Returns true if the admitted vector has changed. The only way it can change
// is when the leader term goes up, and indices potentially regress after the
// log is truncated by this newer term write.
func (l *LogTracker) Snap(ctx context.Context, mark LogMark) bool {
	if !mark.After(l.last) {
		l.errorf(ctx, "registering stale snapshot %+v", mark)
		return false
	}
	// Fake an append spanning the gap between the log and the snapshot. It will,
	// if necessary, truncate the stable index and remove entries waiting for
	// admission that became obsolete.
	//
	// Some entries waiting in the queue remain such, even though not all of them
	// might be actually in the pre-image of the snapshot. We chose this in order
	// to retain some backpressure, all these entries were written anyway and wait
	// for storage admission.
	return l.Append(ctx, min(l.last.Index, mark.Index), mark)
}

// Append informs the tracker that log entries at indices (after, to.Index] are
// about to be sent to stable storage, on behalf of the to.Term leader.
//
// All log storage writes must be registered with the Append call, ordered by
// LogMark, with no gaps. Any entries in the (after, to.Index] batch, that are
// subject to admission control, should be registered with the Register call
// before the batch is sent to storage.
//
// Returns true if the admitted vector has changed. The only way it can change
// is when the leader term goes up, and indices potentially regress after the
// log is truncated by this newer term write.
func (l *LogTracker) Append(ctx context.Context, after uint64, to LogMark) bool {
	// Fast path. We are at the same term. The log must be contiguous.
	if to.Term == l.last.Term {
		if after != l.last.Index {
			l.errorf(ctx, "append (%d,%d]@%d out of order", after, to.Index, to.Term)
			return false
		}
		l.last.Index = to.Index
		return false
	}
	if to.Term < l.last.Term || after > l.last.Index {
		// Does not happen. Log writes are always ordered by LogMark, and have no
		// gaps. Gaps can only appear when the log is cleared in response to storing
		// a snapshot, which is handled by the SnapSynced method.
		l.errorf(ctx, "append (%d,%d]@%d out of order", after, to.Index, to.Term)
		return false
	}
	// Invariant: to.Term > l.last.Term && after <= l.last.Index.
	l.last = to
	// The effective stable and admitted indices can potentially regress here.
	// This happens when a new leader overwrites a suffix of the log.
	l.stable = min(l.stable, after)
	// Entries at index > after.Index from previous terms are now obsolete.
	for pri := range l.waiting {
		truncate(&l.waiting[pri], after)
	}
	// The leader term was bumped, so the admitted vector is new for this term,
	// and is considered "changed".
	return true
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
	ln := l.waiting[pri].Length()
	if ln != 0 && at.Index <= l.waiting[pri].At(ln-1).Index {
		l.errorf(ctx, "admission register %+v [pri=%v] out of order", at, pri)
		return
	}
	l.waiting[pri].Push(at)
}

// LogSynced informs the tracker that the log up to the given LogMark has been
// persisted to stable storage.
//
// All writes are done in the LogMark order, but the corresponding LogSynced
// calls can be skipped or invoked in any order, e.g. due to delivery
// concurrency. The tracker keeps the latest (in the logical sense) stable mark.
//
// Returns true if the admitted vector has advanced.
func (l *LogTracker) LogSynced(ctx context.Context, stable LogMark) bool {
	if stable.After(l.last) {
		// Does not happen. The write must have been registered with Append call.
		l.errorf(ctx, "syncing mark %+v before appending it", stable)
		return false
	}
	if stable.Term != l.last.Term || stable.Index <= l.stable {
		// TODO(pav-kv): we can move the stable index up for a stale Term too, if we
		// track leader term for each entry (see LogAdmitted), or forks of the log.
		return false
	}
	maybeAdmitted := l.stable + 1
	l.stable = stable.Index
	// The admitted index at a priority has advanced if its queue was empty or
	// leading the stable index by more than one.
	for pri := range l.waiting {
		marks := &l.waiting[pri]
		// Example: stable index was 5 before this call. If marks[0].Index <= 6 then
		// we can't advance past 5 even if stable index advances to a higher value.
		// But if marks[0].Index >= 7 we can advance to marks[0].Index-1 which is
		// greater than the old stable index.
		if marks.Length() == 0 || marks.At(0).Index > maybeAdmitted {
			return true
		}
	}
	return false
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
//
// Returns true if the admitted vector has advanced.
func (l *LogTracker) LogAdmitted(ctx context.Context, at LogMark, pri raftpb.Priority) bool {
	if at.After(l.last) {
		// Does not happen. The write must have been registered with Append call.
		l.errorf(ctx, "admitting mark %+v before appending it", at)
		return false
	}
	waiting := &l.waiting[pri]
	ln := waiting.Length()
	// There is nothing to admit, or it's a stale admission.
	if ln == 0 || waiting.At(0).After(at) {
		return false
	}
	// At least one waiting entry can be admitted. The admitted index was at
	// min(l.stable, waiting[0].Index-1). If waiting[0].Index-1 < l.stable, the
	// min increases after the first entry is removed from the queue.
	updated := waiting.At(0).Index <= l.stable
	// Remove all entries up to the admitted mark. Due to invariants, this is
	// always a prefix of the queue.
	for i := 1; i < ln; i++ {
		if waiting.At(i).After(at) {
			waiting.Pop(i)
			return updated
		}
	}
	// The entire queue is admitted, clear it.
	waiting.Prefix(0)
	return updated
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

// DebugString returns a debug string for this tracker.
func (l *LogTracker) DebugString() string {
	var b strings.Builder
	fmt.Fprint(&b, l.String())
	for pri := range l.waiting {
		marks := &l.waiting[pri]
		n := marks.Length()
		if n == 0 {
			continue
		}
		fmt.Fprintf(&b, "\n%s:", raftpb.Priority(pri))
		for i := 0; i < n; i++ {
			fmt.Fprintf(&b, " %+v", marks.At(i))
		}
	}
	return b.String()
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

// truncate updates the slice to be a prefix of the ordered log marks slice,
// with all marks at index > after removed from it.
func truncate(marks *CircularBuffer[LogMark], after uint64) {
	n := marks.Length()
	for i := n; i > 0; i-- {
		if marks.At(i-1).Index <= after {
			marks.Prefix(i)
			return
		}
	}
	marks.Prefix(0)
}
