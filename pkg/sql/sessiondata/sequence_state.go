// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sessiondata

import (
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

// GetLastValue returns the value most recently obtained by
// nextval() for the last sequence for which RecordLatestVal() was called.
func (ss *SequenceState) GetLastValue() (int64, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	if !ss.nextValEverCalledLocked() {
		return 0, pgerror.NewError(
			pgerror.CodeObjectNotInPrerequisiteStateError, "lastval is not yet defined in this session")
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
