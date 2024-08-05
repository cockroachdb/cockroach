// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package replica_rac2

import (
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/errors"
)

// lowPriOverrideState records which raft log entries have their priority
// overridden to be raftpb.LowPri. Used at follower replicas.
//
// The lifetime of a particular log index begins when the entry is appended
// to the in-memory log, and ends when the entry is sent to storage with the
// priority that is extracted from this struct. The same log index can
// reappear in this struct multiple times, when a new raft leader overwrites a
// suffix of the log proposed by the previous leader, or if the existing
// leader retransmits an entry.
//
// sideChannelForLowPriOverride is called when stepping the MsgApps in a
// RaftMessageRequest, and getEffectivePriority is called from within
// handleRaftReady when processing the corresponding MsgStorageAppend.
//
// Maintaining accuracy in the presence of an existing leader (leader with the
// latest term) doing retransmits is tricky, since the caller does not know
// which MsgApps were accepted by Raft during a step (for older terms,
// lowPriOverrideState has its own trivial logic to reject information). We
// can have two kinds of situations:
//
//   - [C1] e10 was stepped, but not accepted, since e9 was missing. e10 is
//     stepped successfully later.
//
//   - [C2] e10 was successfully stepped, but the leader did not realize it, and
//     later resends e10 that is ignored.
//
// We specify a weak correctness requirement: if a leader has not overridden
// to LowPri, the lowPriOverrideState should not think it has. This prevents a
// situation where an entry is consuming regular tokens on the leader, but
// will be admitted as elastic work on the follower.
//
// That is, we allow for the following error: the leader has overridden to
// LowPri, but lowPriOverrideState thinks it has not.
//
// RACv2 has an invariant that if a leader uses LowPri override once for an
// entry, it must be in the send-queue, and will never again be used without
// an override. This gives us an easy way to achieve the correctness
// requirement -- use the specification of override (or lack thereof) from the
// first time that a leader provides us information for an index, and don't
// change it.
//
// For C2, consider the following sequence:
// - e10 NormalPri stepped successfully
// - e10 LowPri override not stepped successfully
// - e10 emerges in handleRaftReady: we will use NormalPri
//
// For C1, consider the following sequence:
//   - e10 NormalPri not stepped successfully
//   - e10 LowPri override stepped successfully
//   - e10 emerges in handleRaftReady: we will use NormalPri, though should have
//     used LowPri.
//
// TODO(sumeer): consider increasing the accuracy by returning information
// from step.
type lowPriOverrideState struct {
	// intervals for which information has been provided.
	//
	// A call to getEffectivePriority for index i causes a prefix of indices <=
	// i to be discarded.
	intervals []interval
	// Highest term observed so far.
	leaderTerm uint64
}

// Represents [first, last].
type interval struct {
	first          uint64
	last           uint64
	lowPriOverride bool
}

// sideChannelForLowPriOverride is called on follower replicas, and specifies
// information for [first, last]. An index can never be queried using
// getEffectivePriority without it being first specified in
// sideChannelForLowPriOverride.
//
// A new leader can overwrite previous log entries. And the existing leader
// can retransmit entries, with a LowPri override. In the case of an existing
// leader we only extend the existing state, as discussed in the correctness
// comment above.
//
// INVARIANT: first <= last.
func (p *lowPriOverrideState) sideChannelForLowPriOverride(
	leaderTerm uint64, first, last uint64, lowPriOverride bool,
) {
	if leaderTerm < p.leaderTerm {
		return
	}
	n := len(p.intervals)
	if leaderTerm > p.leaderTerm {
		p.leaderTerm = leaderTerm
		// Drop all intervals starting at or after the first index. Do it from the
		// back, so that the append-only case is the fast path. When a suffix of
		// entries is overwritten, the cost of this loop is an amortized O(1).
		for ; n > 0 && p.intervals[n-1].first >= first; n-- {
		}
		p.intervals = p.intervals[:n]
		// INVARIANT: n > 0 => p.intervals[n-1].first < first.

		// Adjust the last interval if it overlaps with the one being added.
		if n > 0 && p.intervals[n-1].last >= first {
			// Truncate the last interval.
			p.intervals[n-1].last = first - 1
		}
	} else {
		// Common case: existing leader.
		if n > 0 {
			if p.intervals[n-1].last >= last {
				return
			}
			// INVARIANT: p.intervals[n-1].last < last, so p.intervals[n-1].last + 1 <= last.
			if p.intervals[n-1].last >= first {
				// Adjust first to not overlap with the last interval.
				first = p.intervals[n-1].last + 1
			}
		}
	}
	// INVARIANT:
	// - n is the size of the existing intervals.
	// - [first, last] is non-empty.
	// - The last interval, if any, does not overlap with [first,last].

	// Append to, or extend the existing last interval
	if n > 0 && p.intervals[n-1].lowPriOverride == lowPriOverride &&
		p.intervals[n-1].last+1 >= first {
		// Extend the last interval.
		p.intervals[n-1].last = last
		return
	}
	p.intervals = append(p.intervals,
		interval{first: first, last: last, lowPriOverride: lowPriOverride})
}

func (p *lowPriOverrideState) getEffectivePriority(
	index uint64, pri raftpb.Priority,
) raftpb.Priority {
	// Garbage collect intervals ending before the given index.
	drop := 0
	for n := len(p.intervals); drop < n && p.intervals[drop].last < index; drop++ {
	}
	p.intervals = p.intervals[drop:]
	n := len(p.intervals)
	// INVARIANT: if n > 0, p.intervals[0].last >= index.

	// If there is no interval containing the index, return the original
	// priority. We need to tolerate this since this may be case C1, and this is
	// index 9, so we ignored when sideChannelForLowPriOverride provided
	// information for 9.
	if n == 0 || p.intervals[0].first > index {
		return pri
	}
	lowPriOverride := p.intervals[0].lowPriOverride
	// Remove the prefix of indices <= index.
	if p.intervals[0].last > index {
		p.intervals[0].first = index + 1
	} else {
		p.intervals = p.intervals[1:]
	}
	if lowPriOverride {
		return raftpb.LowPri
	}
	return pri
}

// waitingForAdmissionState records the indices of individual entries that are
// waiting for admission in the AC queues. These are added after emerging as
// MsgStorageAppend in handleRaftReady, hence we can expect monotonicity of
// indices within a leader term, and if the term changes, we can assume that
// non-monotonicity means raft log indices being overwritten and the log being
// truncated. These assumptions simplify the processing of add, since it can
// wipe out higher indices.
//
// We've chosen to track individual indices instead of the latest index, to
// allow the system to self correct under inconsistencies between the sender
// (leader) and receiver (replica). For instance, consider the case that the
// sender was tracking indices (3, 5, 8) under a certain priority, while the
// receiver only tracked (3, 8) (we've discussed an inaccuracy case in
// lowPriOverrideState, and we also want to be defensive in having some
// self-correcting ability when it comes to distributed protocols). When 3 is
// admitted at the receiver, the admitted index can advance to 7, allowing 5
// to be considered admitted at the leader. In comparison, if we only tracked
// the latest index (8), then when 3 was admitted, we could only advance the
// admitted index to 3. A secondary reason to track individual indices is that
// it naturally allows the admitted index to advance up to match without lag
// in the case where there is continuous traffic, but sparseness of indices
// for a priority. For example, if we have indices (10, 20, 30, 40, ...)
// coming in at high priority, and entries are getting admitted with some lag,
// then when match=25 and entry 20 is admitted, we can advance the admitted
// index to 25, and not have to wait for 30 to be admitted too.
//
// We can revisit the decision to track individual indices if we find the
// memory or compute overhead to be significant.
type waitingForAdmissionState struct {
	// The indices for each priority are in increasing index order.
	//
	// Say the indices for a priority are 3, 6, 10. We track the individual
	// indices since when 3 is popped, we can advance admitted for that priority
	// to 5. When 6 is popped, admitted can advance to 9. We should never have a
	// situation where indices are popped out of order, but we tolerate that by
	// popping the prefix upto the index being popped.
	waiting [raftpb.NumPriorities][]admissionEntry
}

type admissionEntry struct {
	index      uint64
	leaderTerm uint64
}

func (w *waitingForAdmissionState) add(leaderTerm uint64, index uint64, pri raftpb.Priority) {
	n := len(w.waiting[pri])
	i := n
	// Linear scan, and all the scanned items will be removed.
	for ; i > 0; i-- {
		if w.waiting[pri][i-1].index < index {
			break
		}
		if buildutil.CrdbTestBuild {
			if overwrittenTerm := w.waiting[pri][i-1].leaderTerm; overwrittenTerm >= leaderTerm {
				panic(errors.AssertionFailedf("overwritten entry has leaderTerm %d >= %d",
					overwrittenTerm, leaderTerm))
			}
		}
	}
	// Uncommon case: i < n.
	if i < n {
		w.waiting[pri] = w.waiting[pri][:i]
		n = i
	}
	if buildutil.CrdbTestBuild {
		if n > 0 && w.waiting[pri][n-1].leaderTerm > leaderTerm {
			panic(errors.AssertionFailedf("non-monotonic leader terms %d >= %d",
				w.waiting[pri][n-1].leaderTerm, leaderTerm))
		}
	}
	w.waiting[pri] = append(w.waiting[pri], admissionEntry{
		index:      index,
		leaderTerm: leaderTerm,
	})
}

func (w *waitingForAdmissionState) remove(
	leaderTerm uint64, index uint64, pri raftpb.Priority,
) (admittedMayAdvance bool) {
	// We expect to typically find the exact entry, except if it was removed
	// because of the term advancing and the entry being overwritten in the
	// log. Due to the monotonicity assumption in add, we simply do a linear
	// scan and remove a prefix.
	//
	// pos is the last entry that is removed.
	pos := -1
	n := len(w.waiting[pri])
	for ; pos+1 < n; pos++ {
		w := w.waiting[pri][pos+1]
		if w.leaderTerm > leaderTerm || w.index > index {
			break
		}
	}
	w.waiting[pri] = w.waiting[pri][pos+1:]
	return pos >= 0
}

func (w *waitingForAdmissionState) computeAdmitted(
	stableIndex uint64,
) [raftpb.NumPriorities]uint64 {
	var admitted [raftpb.NumPriorities]uint64
	for i := range w.waiting {
		admitted[i] = stableIndex
		if len(w.waiting[i]) > 0 {
			upperBoundAdmitted := w.waiting[i][0].index - 1
			if upperBoundAdmitted < admitted[i] {
				admitted[i] = upperBoundAdmitted
			}
		}
	}
	return admitted
}
