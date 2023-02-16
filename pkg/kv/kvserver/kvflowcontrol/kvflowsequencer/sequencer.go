// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvflowsequencer

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Sequencer issues monotonic sequence numbers derived from observed
// <create-time,raft-term,raft-index> tuples. The sequence numbers are roughly
// monotonic with respect to log positions -- if log positions advance, so do
// the issued sequence numbers. The sequence numbers are also kept close to the
// maximum observed create time (similar to an HLC).
//
// This is a purpose-built data structure for below-raft admission control where
// we want to assign each AC-queued work a "sequence number" for FIFO ordering
// within a <tenant,priority>. It's not safe for concurrent access.
type Sequencer struct {
	// maxCreateTime ratchets to the highest observed create time. We generate
	// sequence numbers by adding this to sequenceDelta, where the delta term is
	// incremented whenever the raft term or index within the raft term
	// advances. This provides monotonicity with respect to log positions. The
	// sequence numbers we issue are kept close to the highest observed create
	// time (similar to HLC), so when we observe jumps in maxCreateTime, we
	// reduce the accumulated sequenceDelta by as much possible.
	maxCreateTime int64
	// maxRaftTerm and maxEntryIndexInTerm ratchets to the highest observed raft
	// term and highest observed index within that term. The sequencer observes
	// term and index transitions to ensure that subsequently issued sequence
	// numbers continue to be monotonic.
	maxRaftTerm, maxEntryIndexInTerm uint64
	// sequenceDelta is added to the ever-ratcheting maxCreateTime to generate
	// monotonic sequence numbers as raft terms/indexes and maxCreateTime
	// advance.
	sequenceDelta int64
}

// New returns a new Sequencer.
func New() *Sequencer {
	return &Sequencer{}
}

// Sequence returns a monotonically increasing timestamps derived from the
// provided <create-time,log-position> tuple. If either the create-time or
// log-position advance, so does the returned timestamp.
func (s *Sequencer) Sequence(ct time.Time, pos kvflowcontrolpb.RaftLogPosition) time.Time {
	createTime := ct.UnixNano()
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

	if s.maxRaftTerm < pos.Term {
		// The raft term advanced. Reset the tracking state for term/index.
		s.maxRaftTerm, s.maxEntryIndexInTerm = pos.Term, pos.Index
		s.sequenceDelta += 1 // to ensure monotonicity
	} else if pos.Term == s.maxRaftTerm && s.maxEntryIndexInTerm < pos.Index {
		// The raft index advanced within the same term. Set tracking state.
		s.maxEntryIndexInTerm = pos.Index
		s.sequenceDelta += 1 // to ensure monotonicity
	}
	return timeutil.FromUnixNanos(s.maxCreateTime + s.sequenceDelta)
}
