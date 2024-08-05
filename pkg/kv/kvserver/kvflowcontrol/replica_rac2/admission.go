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
	"cmp"
	"slices"

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
// Maintaining accuracy in the presence of an existing leader doing retransmits
// is tricky, since the caller does not know which MsgApps were accepted by
// Raft during a step. We can have two kinds of situations:
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
	newLeader := false
	if leaderTerm < p.leaderTerm {
		return
	} else if leaderTerm > p.leaderTerm {
		newLeader = true
		p.leaderTerm = leaderTerm
	}
	n := len(p.intervals)

	// Given either n == 0, or n-1 is the last element and p.intervals[n-1].last
	// < first.
	extendOrAppend := func() {
		if n != 0 && p.intervals[n-1].lowPriOverride == lowPriOverride &&
			p.intervals[n-1].last+1 >= first {
			// Extend it.
			p.intervals[n-1].last = last
			return
		}
		p.intervals = append(p.intervals,
			interval{first: first, last: last, lowPriOverride: lowPriOverride})
	}
	if newLeader {
		// Drop all intervals starting at or after the first index. Do it from the
		// back, so that the append-only case is the fast path. When a suffix of
		// entries is overwritten, the cost of this loop is an amortized O(1).
		for ; n != 0 && p.intervals[n-1].first >= first; n-- {
		}
		p.intervals = p.intervals[:n]
		// INVARIANT: keep > 0 => p.intervals[keep-1].first < first.

		// Adjust the last interval if it overlaps with the one being added.
		if n != 0 && p.intervals[n-1].last >= first {
			// Truncate the last interval.
			p.intervals[n-1].last = first - 1
		}
		extendOrAppend()
		return
	}
	// Common case: existing leader.
	if n > 0 {
		if p.intervals[n-1].last >= last {
			return
		}
		// INVARIANT: p.intervals[n-1].last < last, so p.intervals[n-1].last + 1 <= last.
		if p.intervals[n-1].last >= first {
			first = p.intervals[n-1].last + 1
		}
	}
	extendOrAppend()
}

func (p *lowPriOverrideState) getEffectivePriority(
	index uint64, pri raftpb.Priority,
) (effectivePri raftpb.Priority) {
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
// non-monotonicity means raft log indices being overwritten. These
// assumptions simplify the processing of add, since it can wipe out higher
// indices.
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
			if w.waiting[pri][i-1].leaderTerm >= leaderTerm {
				panic(errors.AssertionFailedf("overwritten entry has leaderTerm %d >= %d",
					w.waiting[pri][i-1].leaderTerm, leaderTerm))
			}
		}
	}
	// Uncommon case: i < n.
	if i < n {
		w.waiting[pri] = w.waiting[pri][:i]
	}
	w.waiting[pri] = append(w.waiting[pri], admissionEntry{
		index:      index,
		leaderTerm: leaderTerm,
	})
}

func (w *waitingForAdmissionState) remove(
	leaderTerm uint64, index uint64, pri raftpb.Priority,
) (admittedMayAdvance bool) {
	// Two cases:
	// - index is found: the leaderTerm could be stale, so we ignore in that
	//   case. Else we admit the prefix <= index.
	// - index is not found: the leaderTerm must be stale.
	pos := 0
	found := false
	// Fast path the common case.
	if len(w.waiting[pri]) > 0 && w.waiting[pri][0].index == index {
		found = true
	} else {
		pos, found = slices.BinarySearchFunc(w.waiting[pri], index,
			func(a admissionEntry, index uint64) int {
				return cmp.Compare(a.index, index)
			})
	}
	if !found {
		return false
	}
	if w.waiting[pri][pos].leaderTerm == leaderTerm {
		w.waiting[pri] = w.waiting[pri][pos+1:]
		return true
	}
	return false
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
