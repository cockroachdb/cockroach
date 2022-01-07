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
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// CurrentState represents the current state of the schema change system.
type CurrentState struct {
	TargetState
	Nodes []*Node
}

// Statuses returns a slice of statuses extracted from the Nodes.
func (s *CurrentState) Statuses() []Status {
	statuses := make([]Status, len(s.Nodes))
	for i := range s.Nodes {
		statuses[i] = s.Nodes[i].Status
	}
	return statuses
}

// NumStatus is the number of values which Status may take on.
var NumStatus = len(Status_name)

// Node represents a Target with a given status.
type Node struct {
	*Target
	Status
}

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

// NewTarget constructs a new Target. The passed elem must be one of the oneOf
// members of Element. If not, this call will panic.
func NewTarget(status Status, elem Element, metadata *TargetMetadata) *Target {
	t := Target{
		TargetStatus: status,
	}
	if metadata != nil {
		t.Metadata = *protoutil.Clone(metadata).(*TargetMetadata)
	}
	if !t.SetValue(elem) {
		panic(errors.Errorf("unknown element type %T", elem))
	}
	return &t
}

// SourceElementID elements ID's for identifying parent elements.
// This ID is dynamically allocated when any parent element is
// created and has no relation to the descriptor ID.
type SourceElementID uint32

// ElementMetadata contains materialized metadata for an element,
// where references inside the TargetMetadata are resolved to
// their actual values. This structure is mainly used during opgen
// where we need to know these values to emit event log entries for
// example.
type ElementMetadata struct {
	TargetMetadata
	Username  string
	AppName   string
	Statement string
}

// DeepCopy returns a deep copy of the state.
func (s *CurrentState) DeepCopy() CurrentState {
	return MakeCurrentState(s.TargetState, s.Statuses())
}

// ShallowCopy returns a shallow copy of the state, in which only the backing
// slice of Nodes is newly allocated.
func (s *CurrentState) ShallowCopy() CurrentState {
	return CurrentState{
		TargetState: s.TargetState,
		Nodes:       append(make([]*Node, 0, len(s.Nodes)), s.Nodes...),
	}
}

// MakeCurrentState returns a CurrentState with a deep copy of the target state.
// The current status slice may be nil, which defaults to ABSENT for all values.
func MakeCurrentState(targetState TargetState, current []Status) CurrentState {
	ret := CurrentState{
		TargetState: *protoutil.Clone(&targetState).(*TargetState),
		Nodes:       make([]*Node, len(targetState.Targets)),
	}
	for i := range targetState.Targets {
		ret.Nodes[i] = &Node{Target: &ret.Targets[i]}
	}
	for i, status := range current {
		ret.Nodes[i].Status = status
	}
	return ret
}
