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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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
	DescriptorID() descpb.ID

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

// DescriptorID implements the Element interface.
func (e *Column) DescriptorID() descpb.ID { return e.TableID }

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

// DescriptorID implements the Element interface.
func (e *PrimaryIndex) DescriptorID() descpb.ID { return e.TableID }

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

// DescriptorID implements the Element interface.
func (e *SecondaryIndex) DescriptorID() descpb.ID { return e.TableID }

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

// DescriptorID implements the Element interface.
func (e *SequenceDependency) DescriptorID() descpb.ID { return e.SequenceID }

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

// DescriptorID implements the Element interface.
func (e *UniqueConstraint) DescriptorID() descpb.ID { return e.TableID }

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

// DescriptorID implements the Element interface.
func (e *CheckConstraint) DescriptorID() descpb.ID { return e.TableID }

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

// DescriptorID implements the Element interface.
func (e *Sequence) DescriptorID() descpb.ID { return e.SequenceID }

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

// DescriptorID implements the Element interface.
func (e *DefaultExpression) DescriptorID() descpb.ID { return e.TableID }

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

// DescriptorID implements the Element interface.
func (e *View) DescriptorID() descpb.ID { return e.TableID }

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

// DescriptorID implements the Element interface.
func (e *TypeReference) DescriptorID() descpb.ID { return e.DescID }

func (e *TypeReference) getAttribute(attr Attribute) attributeValue {
	switch attr {
	case AttributeType:
		return getElementTypeID(e)
	case AttributeDescID:
		return (*descID)(&e.TypeID)
	case AttributeDepID:
		return (*descID)(&e.DescID)
	default:
		return nil
	}
}

// DescriptorID implements the Element interface.
func (e *Table) DescriptorID() descpb.ID { return e.TableID }

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

// DescriptorID implements the Element interface.
func (e *InboundForeignKey) DescriptorID() descpb.ID { return e.OriginID }

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

// DescriptorID implements the Element interface.
func (e *OutboundForeignKey) DescriptorID() descpb.ID { return e.OriginID }

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

// DescriptorID implements the Element interface.
func (e *RelationDependedOnBy) DescriptorID() descpb.ID { return e.TableID }

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

// DescriptorID implements the Element interface.
func (e *SequenceOwnedBy) DescriptorID() descpb.ID { return e.SequenceID }
