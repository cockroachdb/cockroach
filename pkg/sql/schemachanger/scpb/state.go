// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scpb

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// CurrentState is a TargetState decorated with the current status of the
// elements in the target state.
type CurrentState struct {
	TargetState
	Current []Status

	// InRollback captures whether the job is currently rolling back.
	// This is important to ensure that the job can be moved to the proper
	// failed state upon restore.
	//
	// Note, if this value is true, the targets have had their directions
	// flipped already.
	//
	InRollback bool
}

// DeepCopy returns a deep copy of the receiver.
func (s CurrentState) DeepCopy() CurrentState {
	return CurrentState{
		TargetState: *protoutil.Clone(&s.TargetState).(*TargetState),
		Current:     append(make([]Status, 0, len(s.Current)), s.Current...),
	}
}

// Rollback idempotently marks the current state as InRollback. If the
// CurrentState was not previously marked as InRollback, it reverses the
// directions of all the targets.
func (s *CurrentState) Rollback() {
	if s.InRollback {
		return
	}
	for i := range s.Targets {
		t := &s.Targets[i]
		switch t.TargetStatus {
		case Status_ABSENT:
			t.TargetStatus = Status_PUBLIC
		default:
			t.TargetStatus = Status_ABSENT
		}
	}
	s.InRollback = true
}

// NumStatus is the number of values which Status may take on.
var NumStatus = len(Status_name)

// Element represents a logical component of a catalog entry's schema (e.g., an
// index or column in a table).
type Element interface {
	protoutil.Message

	// element is intended to be implemented only by the members of ElementProto.
	// Code generation implements this method.
	element()
}

//go:generate go run element_generator.go --in elements.proto --out elements_generated.go
//go:generate go run element_uml_generator.go --out uml/table.puml

// Element returns an Element from its wrapper for serialization.
func (e *ElementProto) Element() Element {
	return e.GetValue().(Element)
}

// MakeTarget constructs a new Target. The passed elem must be one of the oneOf
// members of Element. If not, this call will panic.
func MakeTarget(status TargetStatus, elem Element, metadata *TargetMetadata) Target {
	t := Target{
		TargetStatus: status.Status(),
	}
	if metadata != nil {
		t.Metadata = *protoutil.Clone(metadata).(*TargetMetadata)
	}
	if !t.SetValue(elem) {
		panic(errors.Errorf("unknown element type %T", elem))
	}
	return t
}

// SourceElementID elements ID's for identifying parent elements.
// This ID is dynamically allocated when any parent element is
// created and has no relation to the descriptor ID.
type SourceElementID uint32

// Clone will make a deep copy of the DescriptorState.
func (m *DescriptorState) Clone() *DescriptorState {
	if m == nil {
		return nil
	}
	return protoutil.Clone(m).(*DescriptorState)
}

// MakeCurrentStateFromDescriptors constructs a CurrentState object from a
// slice of DescriptorState object from which the current state has been
// decomposed.
func MakeCurrentStateFromDescriptors(descriptorStates []*DescriptorState) (CurrentState, error) {
	var s CurrentState
	var targetRanks []uint32
	var rollback bool
	stmts := make(map[uint32]Statement)
	for i, cs := range descriptorStates {
		if i == 0 {
			rollback = cs.InRollback
		} else if rollback != cs.InRollback {
			return CurrentState{}, errors.AssertionFailedf(
				"job %d: conflicting rollback statuses between descriptors",
				cs.JobID,
			)
		}
		s.Current = append(s.Current, cs.CurrentStatuses...)
		s.Targets = append(s.Targets, cs.Targets...)
		targetRanks = append(targetRanks, cs.TargetRanks...)
		for _, stmt := range cs.RelevantStatements {
			if existing, ok := stmts[stmt.StatementRank]; ok {
				if existing.Statement != stmt.Statement.Statement {
					return CurrentState{}, errors.AssertionFailedf(
						"job %d: statement %q does not match %q for rank %d",
						cs.JobID,
						existing.Statement,
						stmt.Statement,
						stmt.StatementRank,
					)
				}
			}
			stmts[stmt.StatementRank] = stmt.Statement
		}
		s.Authorization = cs.Authorization
	}
	sort.Sort(&stateAndRanks{CurrentState: &s, ranks: targetRanks})
	var sr stmtsAndRanks
	for rank, stmt := range stmts {
		sr.stmts = append(sr.stmts, stmt)
		sr.ranks = append(sr.ranks, rank)
	}
	sort.Sort(&sr)
	s.Statements = sr.stmts
	s.InRollback = rollback
	return s, nil
}

type stateAndRanks struct {
	*CurrentState
	ranks []uint32
}

var _ sort.Interface = (*stateAndRanks)(nil)

func (s *stateAndRanks) Len() int           { return len(s.Targets) }
func (s *stateAndRanks) Less(i, j int) bool { return s.ranks[i] < s.ranks[j] }
func (s *stateAndRanks) Swap(i, j int) {
	s.ranks[i], s.ranks[j] = s.ranks[j], s.ranks[i]
	s.Targets[i], s.Targets[j] = s.Targets[j], s.Targets[i]
	s.Current[i], s.Current[j] = s.Current[j], s.Current[i]
}

type stmtsAndRanks struct {
	stmts []Statement
	ranks []uint32
}

func (s *stmtsAndRanks) Len() int           { return len(s.stmts) }
func (s *stmtsAndRanks) Less(i, j int) bool { return s.ranks[i] < s.ranks[j] }
func (s stmtsAndRanks) Swap(i, j int) {
	s.ranks[i], s.ranks[j] = s.ranks[j], s.ranks[i]
	s.stmts[i], s.stmts[j] = s.stmts[j], s.stmts[i]
}

var _ sort.Interface = (*stmtsAndRanks)(nil)
