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

// NumStates is the number of values which State may take on.
var NumStates = len(State_name)

// Node represents a Target in a given state.
type Node struct {
	Target *Target
	State  State
}

// Element returns the target's element.
func (n *Node) Element() Element {
	return n.Target.Element()
}

// Element represents a logical component of a catalog entry's schema (e.g., an
// index or column in a table).
type Element interface {
	protoutil.Message

	// getAttribute returns the value of a given attribute of an element.
	// If the attribute is not defined on the element, nil will be returned.
	getAttribute(Attribute) attributeValue
}

// Element returns an Element from its wrapper for serialization.
func (e *ElementProto) Element() Element {
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

func (e *Column) getAttribute(attribute Attribute) attributeValue {
	switch attribute {
	case AttributeType:
		return getElementTypeID(e)
	case AttributeDescID:
		return (*descID)(&e.TableID)
	case AttributeColumnID:
		return (*columnID)(&e.Column.ID)
	case AttributeElementName:
		return (*elementName)(&e.Column.Name)
	default:
		return nil
	}
}

func (e *PrimaryIndex) getAttribute(attr Attribute) attributeValue {
	switch attr {
	case AttributeType:
		return getElementTypeID(e)
	case AttributeDescID:
		return (*descID)(&e.TableID)
	case AttributeIndexID:
		return (*indexID)(&e.Index.ID)
	case AttributeElementName:
		return (*elementName)(&e.Index.Name)
	default:
		return nil
	}
}

func (e *SecondaryIndex) getAttribute(attr Attribute) attributeValue {
	switch attr {
	case AttributeType:
		return getElementTypeID(e)
	case AttributeDescID:
		return (*descID)(&e.TableID)
	case AttributeIndexID:
		return (*indexID)(&e.Index.ID)
	case AttributeElementName:
		return (*elementName)(&e.Index.Name)
	default:
		return nil
	}
}

func (e *SequenceDependency) getAttribute(attr Attribute) attributeValue {
	switch attr {
	case AttributeType:
		return getElementTypeID(e)
	case AttributeDescID:
		return (*descID)(&e.SequenceID)
	case AttributeDepID:
		return (*descID)(&e.TableID)
	case AttributeColumnID:
		return (*columnID)(&e.ColumnID)
	default:
		return nil
	}
}

func (e *UniqueConstraint) getAttribute(attr Attribute) attributeValue {
	switch attr {
	case AttributeType:
		return getElementTypeID(e)
	case AttributeDescID:
		return (*descID)(&e.TableID)
	case AttributeIndexID:
		return (*indexID)(&e.IndexID)
	default:
		return nil
	}
}

func (e *CheckConstraint) getAttribute(attr Attribute) attributeValue {
	switch attr {
	case AttributeType:
		return getElementTypeID(e)
	case AttributeDescID:
		return (*descID)(&e.TableID)
	case AttributeElementName:
		return (*elementName)(&e.Name)
	default:
		return nil
	}
}

func (e *Sequence) getAttribute(attr Attribute) attributeValue {
	switch attr {
	case AttributeType:
		return getElementTypeID(e)
	case AttributeDescID:
		return (*descID)(&e.SequenceID)
	default:
		return nil
	}
}

func (e *DefaultExpression) getAttribute(attr Attribute) attributeValue {
	switch attr {
	case AttributeType:
		return getElementTypeID(e)
	case AttributeDescID:
		return (*descID)(&e.TableID)
	case AttributeColumnID:
		return (*columnID)(&e.ColumnID)
	default:
		return nil
	}
}

func (e *View) getAttribute(attr Attribute) attributeValue {
	switch attr {
	case AttributeType:
		return getElementTypeID(e)
	case AttributeDescID:
		return (*descID)(&e.TableID)
	default:
		return nil
	}
}

func (e *TypeReference) getAttribute(attr Attribute) attributeValue {
	switch attr {
	case AttributeType:
		return getElementTypeID(e)
	case AttributeDescID:
		return (*descID)(&e.DescID)
	case AttributeDepID:
		return (*descID)(&e.TypeID)
	default:
		return nil
	}
}

func (e *Table) getAttribute(attr Attribute) attributeValue {
	switch attr {
	case AttributeType:
		return getElementTypeID(e)
	case AttributeDescID:
		return (*descID)(&e.TableID)
	default:
		return nil
	}
}

func (e *InboundForeignKey) getAttribute(attr Attribute) attributeValue {
	switch attr {
	case AttributeType:
		return getElementTypeID(e)
	case AttributeDescID:
		return (*descID)(&e.OriginID)
	case AttributeDepID:
		return (*descID)(&e.ReferenceID)
	case AttributeElementName:
		return (*elementName)(&e.Name)
	default:
		return nil
	}
}

func (e *OutboundForeignKey) getAttribute(attr Attribute) attributeValue {
	switch attr {
	case AttributeType:
		return getElementTypeID(e)
	case AttributeDescID:
		return (*descID)(&e.OriginID)
	case AttributeDepID:
		return (*descID)(&e.ReferenceID)
	case AttributeElementName:
		return (*elementName)(&e.Name)
	default:
		return nil
	}
}

func (e *RelationDependedOnBy) getAttribute(attr Attribute) attributeValue {
	switch attr {
	case AttributeType:
		return getElementTypeID(e)
	case AttributeDescID:
		return (*descID)(&e.TableID)
	case AttributeDepID:
		return (*descID)(&e.DependedOnBy)
	default:
		return nil
	}
}

func (e *SequenceOwnedBy) getAttribute(attr Attribute) attributeValue {
	switch attr {
	case AttributeType:
		return getElementTypeID(e)
	case AttributeDescID:
		return (*descID)(&e.SequenceID)
	case AttributeDepID:
		return (*descID)(&e.OwnerTableID)
	default:
		return nil
	}
}
