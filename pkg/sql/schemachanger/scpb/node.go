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

// State represents a current or potential future state of the
// schema change system. Additionally, it tracks any metadata
// for the schema change such as the Statements and Authorization
// information. Nodes may refer to this information for different
// purposes.
type State struct {
	Nodes         []*Node
	Statements    []*Statement
	Authorization Authorization
}

// Statuses returns a slice of statuses extracted from the Nodes.
func (s *State) Statuses() []Status {
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

//go:generate go run element_generator.go --in scpb.proto --out elements_generated.go
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
