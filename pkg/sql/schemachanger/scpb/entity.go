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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// Entity represents something which contains an Element.
// It is implemented by all of the concrete Element types (which return
// themselves) as well as Node and Target.
type Entity interface {
	eav.Entity

	// GetElement returns the underlying element. The returned value should be
	// an actual element struct which is part of the Ele
	GetElement() Element
}

// Element represents a logical component of a catalog entry's schema (e.g., an
// index or column in a table). It is implemented by Node, Target, and the
// members of ElementProto.
type Element interface {
	Entity
	protoutil.Message
	element()
}

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

// Attributes is part of the eav.Entity interface.
func (n *Node) Attributes() eav.OrdinalSet {
	return n.Target.Attributes().Add(AttrStatus.Ordinal())
}

// Get is part of the eav.Entity interface.
func (n *Node) Get(a eav.Attribute) eav.Value {
	switch a {
	case AttrStatus:
		return (*eav.Int32)(&n.Status)
	default:
		return n.Target.Get(a)
	}
}

// Attributes is part of the eav.Entity interface.
func (m *Target) Attributes() eav.OrdinalSet {
	return m.GetElement().Attributes().Add(AttrDirection.Ordinal())
}

// Get is part of the eav.Entity interface.
func (m *Target) Get(a eav.Attribute) eav.Value {
	switch a {
	case AttrDirection:
		return (*eav.Int32)(&m.Direction)
	default:
		return m.GetElement().Get(a)
	}
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
