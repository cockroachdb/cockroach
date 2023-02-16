// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package admission

// sequencer issues monotonic sequence numbers derived from observed
// <create-time,raft-term,raft-index> tuples. The sequence numbers are roughly
// monotonic with respect to log positions -- if log positions advance, so do
// the issued sequence numbers. The sequence numbers are also kept close to the
// maximum observed create time (similar to an HLC).
//
// This is a purpose-built data structure for below-raft admission control where
// we want to assign each AC-queued work a "sequence number" for FIFO ordering
// within a <tenant,priority>. It's not safe for concurrent access.
type sequencer struct {
	// maxCreateTime ratchets to the highest observed create time. We generate
	// sequence numbers by adding this to sequenceDelta, where the delta term is
	// incremented whenever the raft term or index within the raft term
	// advances. This provides monotonicity with respect to log positions. The
	// sequence numbers we issue are kept close to the highest observed create
	// time (similar to HLC), so when we observe jumps in maxCreateTime, we
	// reduce the accumulated sequenceDelta by as much possible.
	maxCreateTime int64
	// maxRaftTerm and maxEntryIndexInTerm ratchets to the highest observed raft
	// term and highest observed index within that term. The sequencer observe
	// term and index transitions to ensure that subsequently issued sequence
	// numbers continue to be monotonic.
	maxRaftTerm, maxEntryIndexInTerm uint64
	// sequenceDelta is added to the ever-ratcheting maxCreateTime to generate
	// monotonic sequence numbers as raft terms/indexes and maxCreateTime
	// advance.
	sequenceDelta int64
}

func (s *sequencer) sequence(createTime int64, raftTerm, raftIndex uint64) int64 {
	if createTime > s.maxCreateTime {
		delta := createTime - s.maxCreateTime
		// We advance the maxCreateTime by delta and simultaneously reduce
		// sequenceDelta by just that much to keep issuing sequence numbers
		// close to maxCreateTime. We'll increment the sequenceDelta to
		// guarantee monotonicity (consider the case where maxCreateTime and
		// sequenceDelta were incremented/decremented by N respectively).
		s.maxCreateTime += delta
		s.sequenceDelta -= delta
		s.sequenceDelta += 1 // to ensure monotonicity
		if s.sequenceDelta < 0 {
			// Enforce a floor. We've advanced maxCreateTime greater than what
			// we had in sequenceDelta so this still gives us monotonicity and
			// lets us sequence numbers close to maxCreateTime.
			s.sequenceDelta = 0
		}
	}

	if s.maxRaftTerm < raftTerm {
		// The raft term advanced. Reset the tracking state for term/index.
		s.maxRaftTerm, s.maxEntryIndexInTerm = raftTerm, raftIndex
		s.sequenceDelta += 1 // to ensure monotonicity
	} else if raftTerm == s.maxRaftTerm && s.maxEntryIndexInTerm < raftIndex {
		// The raft index advanced within the same term. Set tracking state.
		s.maxEntryIndexInTerm = raftIndex
		s.sequenceDelta += 1 // to ensure monotonicity
	}
	return s.maxCreateTime + s.sequenceDelta
}
