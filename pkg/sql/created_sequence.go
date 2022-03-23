// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

type createdSequences interface {
	// addCreatedSequence adds a sequence to the set of sequences created in the current transaction.
	addCreatedSequence(id uint32)
	// isCreatedSequence checks if a sequence was created in the current transaction.
	isCreatedSequence(id uint32) bool
}

type connExCreatedSequencesAccessor struct {
	ex *connExecutor
}

func (c connExCreatedSequencesAccessor) addCreatedSequence(id uint32) {
	c.ex.extraTxnState.createdSequences[id] = struct{}{}
}

func (c connExCreatedSequencesAccessor) isCreatedSequence(id uint32) bool {
	_, ok := c.ex.extraTxnState.createdSequences[id]
	return ok
}

// noopCreatedSequences is the default impl used by the planner when the connExecutor is not available.
type noopCreatedSequences struct{}

func (createdSequences noopCreatedSequences) addCreatedSequence(id uint32) {
	panic("addCreatedSequence not supported")
}

func (createdSequences noopCreatedSequences) isCreatedSequence(id uint32) bool {
	return false
}
