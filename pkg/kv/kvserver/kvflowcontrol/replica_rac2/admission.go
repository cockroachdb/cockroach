// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replica_rac2

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/rac2"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
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
	intervals rac2.CircularBuffer[interval]
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
// Returns true iff the leaderTerm was not stale.
//
// INVARIANT: first <= last.
func (p *lowPriOverrideState) sideChannelForLowPriOverride(
	leaderTerm uint64, first, last uint64, lowPriOverride bool,
) bool {
	if leaderTerm < p.leaderTerm {
		return false
	}
	n := p.intervals.Length()
	if leaderTerm > p.leaderTerm {
		p.leaderTerm = leaderTerm
		// Drop all intervals starting at or after the first index. Do it from the
		// back, so that the append-only case is the fast path. When a suffix of
		// entries is overwritten, the cost of this loop is an amortized O(1).
		for ; n > 0 && p.intervals.At(n-1).first >= first; n-- {
		}
		p.intervals.Prefix(n)
		// INVARIANT: n > 0 => p.intervals[n-1].first < first.

		// Adjust the last interval if it overlaps with the one being added.
		if n > 0 && p.intervals.At(n-1).last >= first {
			lastInterval := p.intervals.At(n - 1)
			// Truncate the last interval.
			lastInterval.last = first - 1
			p.intervals.SetLast(lastInterval)
		}
	} else {
		// Common case: existing leader.
		if n > 0 {
			lastInterval := p.intervals.At(n - 1)
			if lastInterval.last >= last {
				return true
			}
			// INVARIANT: lastInterval.last < last, so lastInterval.last + 1 <= last.
			if lastInterval.last >= first {
				// Adjust first to not overlap with the last interval.
				first = lastInterval.last + 1
			}
		}
	}
	// INVARIANT:
	// - n is the size of the existing intervals.
	// - [first, last] is non-empty.
	// - The last interval, if any, does not overlap with [first,last].

	// Append to, or extend the existing last interval
	if n > 0 {
		lastInterval := p.intervals.At(n - 1)
		if lastInterval.lowPriOverride == lowPriOverride && lastInterval.last+1 >= first {
			// Extend the last interval.
			lastInterval.last = last
			p.intervals.SetLast(lastInterval)
			return true
		}
	}
	p.intervals.Push(interval{first: first, last: last, lowPriOverride: lowPriOverride})
	return true
}

// sideChannelForV1Leader returns true iff the leaderTerm advanced.
func (p *lowPriOverrideState) sideChannelForV1Leader(leaderTerm uint64) bool {
	if leaderTerm <= p.leaderTerm {
		return false
	}
	p.leaderTerm = leaderTerm
	p.intervals.Prefix(0)
	return true
}

func (p *lowPriOverrideState) getEffectivePriority(
	index uint64, pri raftpb.Priority,
) raftpb.Priority {
	// Garbage collect intervals ending before the given index.
	drop := 0
	for n := p.intervals.Length(); drop < n && p.intervals.At(drop).last < index; drop++ {
	}
	p.intervals.Pop(drop)
	n := p.intervals.Length()
	// INVARIANT: if n > 0, p.intervals[0].last >= index.

	// If there is no interval containing the index, return the original
	// priority. We need to tolerate this since this may be case C1, and this is
	// index 9, so we ignored when sideChannelForLowPriOverride provided
	// information for 9.
	if n == 0 {
		return pri
	}
	firstInterval := p.intervals.At(0)
	if firstInterval.first > index {
		return pri
	}
	lowPriOverride := firstInterval.lowPriOverride
	// Remove the prefix of indices <= index.
	if firstInterval.last > index {
		firstInterval.first = index + 1
		p.intervals.SetFirst(firstInterval)
	} else {
		p.intervals.Pop(1)
	}
	if lowPriOverride {
		return raftpb.LowPri
	}
	return pri
}
