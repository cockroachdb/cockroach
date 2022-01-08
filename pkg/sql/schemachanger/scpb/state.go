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

// CurrentState is a TargetState decorated with the current status of the
// elements in the target state.
type CurrentState struct {
	TargetState
	Current []Status
}

// DeepCopy returns a deep copy of the receiver.
func (s CurrentState) DeepCopy() CurrentState {
	return CurrentState{
		TargetState: *protoutil.Clone(&s.TargetState).(*TargetState),
		Current:     append(make([]Status, 0, len(s.Current)), s.Current...),
	}
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
func MakeTarget(status Status, elem Element, metadata *TargetMetadata) Target {
	t := Target{
		TargetStatus: status,
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
