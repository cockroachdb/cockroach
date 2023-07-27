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

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/errors"
)

type createdSequences interface {
	// addCreatedSequence adds a sequence to the set of sequences created or
	// restarted in the current transaction.
	addCreatedSequence(id descpb.ID) error
	// isCreatedSequence checks if a sequence was created or restarted in the
	// current transaction.
	isCreatedSequence(id descpb.ID) bool
}

type connExCreatedSequencesAccessor struct {
	ex *connExecutor
}

func (c connExCreatedSequencesAccessor) addCreatedSequence(id descpb.ID) error {
	c.ex.extraTxnState.createdSequences[id] = struct{}{}
	return nil
}

func (c connExCreatedSequencesAccessor) isCreatedSequence(id descpb.ID) bool {
	_, ok := c.ex.extraTxnState.createdSequences[id]
	return ok
}

// emptyCreatedSequences is the default impl used by the planner when the connExecutor is not available.
type emptyCreatedSequences struct{}

func (createdSequences emptyCreatedSequences) addCreatedSequence(id descpb.ID) error {
	return errors.AssertionFailedf("addCreatedSequence not supported in emptyCreatedSequences")
}

func (createdSequences emptyCreatedSequences) isCreatedSequence(id descpb.ID) bool {
	return false
}
