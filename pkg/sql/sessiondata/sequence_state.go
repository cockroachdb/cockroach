// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sessiondata

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// SequenceState stores session-scoped state used by sequence builtins.
//
// All public methods of SequenceState are thread-safe, as the structure is
// meant to be shared by statements executing in parallel on a session.
type SequenceState struct {
	mu struct {
		syncutil.Mutex
		// latestValues stores the last value obtained by nextval() in this session
		// by descriptor id.
		latestValues map[uint32]int64

		// lastSequenceIncremented records the descriptor id of the last sequence
		// nextval() was called on in this session.
		lastSequenceIncremented uint32
	}
}

// NewSequenceState creates a SequenceState.
func NewSequenceState() *SequenceState {
	ss := SequenceState{}
	ss.mu.latestValues = make(map[uint32]int64)
	return &ss
}

// NextVal ever called returns true if a sequence has ever been incremented on
// this session.
func (ss *SequenceState) nextValEverCalledLocked() bool {
	return len(ss.mu.latestValues) > 0
}

// RecordValue records the latest manipulation of a sequence done by a session.
func (ss *SequenceState) RecordValue(seqID uint32, val int64) {
	ss.mu.Lock()
	ss.mu.lastSequenceIncremented = seqID
	ss.mu.latestValues[seqID] = val
	ss.mu.Unlock()
}

// SetLastSequenceIncremented sets the id of the last incremented sequence.
// Usually this id is set through RecordValue().
func (ss *SequenceState) SetLastSequenceIncremented(seqID uint32) {
	ss.mu.Lock()
	ss.mu.lastSequenceIncremented = seqID
	ss.mu.Unlock()
}

// GetLastValue returns the value most recently obtained by
// nextval() for the last sequence for which RecordLatestVal() was called.
func (ss *SequenceState) GetLastValue() (int64, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	if !ss.nextValEverCalledLocked() {
		return 0, pgerror.New(
			pgcode.ObjectNotInPrerequisiteState, "lastval is not yet defined in this session")
	}

	return ss.mu.latestValues[ss.mu.lastSequenceIncremented], nil
}

// GetLastValueByID returns the value most recently obtained by nextval() for
// the given sequence in this session.
// The bool retval is false if RecordLatestVal() was never called on the
// requested sequence.
func (ss *SequenceState) GetLastValueByID(seqID uint32) (int64, bool) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	val, ok := ss.mu.latestValues[seqID]
	return val, ok
}

// Export returns a copy of the SequenceState's state - the latestValues and
// lastSequenceIncremented. If there are no values in latestValues, the returned
// map will be nil.
// lastSequenceIncremented is only defined if latestValues is non-empty.
func (ss *SequenceState) Export() (map[uint32]int64, uint32) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	var res map[uint32]int64
	if len(ss.mu.latestValues) > 0 {
		res = make(map[uint32]int64, len(ss.mu.latestValues))
		for k, v := range ss.mu.latestValues {
			res[k] = v
		}
	}
	return res, ss.mu.lastSequenceIncremented
}
