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
// schema change system.
type State []*Node

// NumStatus is the number of values which Status may take on.
var NumStatus = len(Status_name)

// Node represents a Target with a given status.
type Node struct {
	Target *Target
	Status Status
}

// GetElement returns the target's element.
func (n *Node) GetElement() Element {
	return n.Target.GetElement()
}

// Container represents something which contains an Element.
// It is implemented by all of the concrete Element types (which return
// themselves) as well as Node and Target.
type Container interface {
	GetElement() Element
}

// Element represents a logical component of a catalog entry's schema (e.g., an
// index or column in a table). It is implemented by Node, Target, and the
// members of ElementProto.
type Element interface {
	Container
	protoutil.Message
	element()
}

// GetElement returns an Element from its wrapper for serialization.
func (e *ElementProto) GetElement() Element {
	return e.GetValue().(Element)
}

// NewTarget constructs a new Target. The passed elem must be one of the oneOf
// members of Element. If not, this call will panic.
func NewTarget(dir Target_Direction, elem Element) *Target {
	t := Target{
		Direction: dir,
	}
	if !t.SetValue(elem) {
		panic(errors.Errorf("unknown element type %T", elem))
	}
	return &t
}

// GetElement implements the Container interface.
func (m *Column) GetElement() Element { return m }

// GetElement implements the Container interface.
func (m *PrimaryIndex) GetElement() Element { return m }

// GetElement implements the Container interface.
func (m *SecondaryIndex) GetElement() Element { return m }

// GetElement implements the Container interface.
func (m *SequenceDependency) GetElement() Element { return m }

// GetElement implements the Container interface.
func (m *UniqueConstraint) GetElement() Element { return m }

// GetElement implements the Container interface.
func (m *CheckConstraint) GetElement() Element { return m }

// GetElement implements the Container interface.
func (m *Sequence) GetElement() Element { return m }

// GetElement implements the Container interface.
func (m *DefaultExpression) GetElement() Element { return m }

// GetElement implements the Container interface.
func (m *View) GetElement() Element { return m }

// GetElement implements the Container interface.
func (m *TypeReference) GetElement() Element { return m }

// GetElement implements the Container interface.
func (m *Table) GetElement() Element { return m }

// GetElement implements the Container interface.
func (m *InboundForeignKey) GetElement() Element { return m }

// GetElement implements the Container interface.
func (m *OutboundForeignKey) GetElement() Element { return m }

// GetElement implements the Container interface.
func (m *RelationDependedOnBy) GetElement() Element { return m }

// GetElement implements the Container interface.
func (m *SequenceOwnedBy) GetElement() Element { return m }

// GetElement implements the Container interface.
func (m *Type) GetElement() Element { return m }

// GetElement implements the Container interface.
func (m *Schema) GetElement() Element { return m }

// GetElement implements the Container interface.
func (m *Database) GetElement() Element { return m }

func (m *Column) element()               {}
func (m *PrimaryIndex) element()         {}
func (m *SecondaryIndex) element()       {}
func (m *SequenceDependency) element()   {}
func (m *UniqueConstraint) element()     {}
func (m *CheckConstraint) element()      {}
func (m *Sequence) element()             {}
func (m *DefaultExpression) element()    {}
func (m *View) element()                 {}
func (m *TypeReference) element()        {}
func (m *Table) element()                {}
func (m *InboundForeignKey) element()    {}
func (m *OutboundForeignKey) element()   {}
func (m *RelationDependedOnBy) element() {}
func (m *SequenceOwnedBy) element()      {}
func (m *Type) element()                 {}
func (m *Schema) element()               {}
func (m *Database) element()             {}
