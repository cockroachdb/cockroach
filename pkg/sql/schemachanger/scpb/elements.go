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
	"reflect"
	"strconv"
	"strings"

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
	GetAttributes() Attributes
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

// ElementTypeID represents type ID of a element
type ElementTypeID int

var typeToElementID map[reflect.Type]ElementTypeID
var elementIDToString map[ElementTypeID]string

func init() {
	typ := reflect.TypeOf((*ElementProto)(nil)).Elem()
	typeToElementID = make(map[reflect.Type]ElementTypeID, typ.NumField())
	elementIDToString = make(map[ElementTypeID]string, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		protoFlags := strings.Split(f.Tag.Get("protobuf"), ",")
		id, err := strconv.Atoi(protoFlags[1])
		if err != nil {
			panic(errors.Wrapf(err, "failed to extract ID from protobuf tag: %q", protoFlags))
		}
		typeToElementID[f.Type] = ElementTypeID(id)
		elementIDToString[ElementTypeID(id)] = strings.TrimPrefix(f.Type.String(), "*scpb.")
	}
}

// ElementType determines the type ID of a element
func ElementType(el Element) ElementTypeID {
	return typeToElementID[reflect.TypeOf(el)]
}

// ElementIDToString determines the type ID of a element
func ElementIDToString(id ElementTypeID) string {
	return elementIDToString[id]
}

// DescriptorID implements the Element interface.
func (e *Column) DescriptorID() descpb.ID { return e.TableID }

// GetAttributes implements the Element interface
func (e *Column) GetAttributes() Attributes {
	return makeAttributes([]attributeValue{
		{key: AttributeType, value: ElementType(e)},
		{key: AttributeDescID, value: DescID(e.TableID)},
		{key: AttributeColumnID, value: ColumnID(e.Column.ID)},
		{key: AttributeElementName, value: ElementName(e.Column.Name)},
	})
}

// DescriptorID implements the Element interface.
func (e *PrimaryIndex) DescriptorID() descpb.ID { return e.TableID }

// GetAttributes implements the Element interface
func (e *PrimaryIndex) GetAttributes() Attributes {
	return makeAttributes([]attributeValue{
		{key: AttributeType, value: ElementType(e)},
		{key: AttributeDescID, value: DescID(e.TableID)},
		{key: AttributeIndexID, value: IndexID(e.Index.ID)},
		{key: AttributeElementName, value: ElementName(e.Index.Name)},
	})
}

// DescriptorID implements the Element interface.
func (e *SecondaryIndex) DescriptorID() descpb.ID { return e.TableID }

// GetAttributes implements the Element interface
func (e *SecondaryIndex) GetAttributes() Attributes {
	return makeAttributes([]attributeValue{
		{key: AttributeType, value: ElementType(e)},
		{key: AttributeDescID, value: DescID(e.TableID)},
		{key: AttributeIndexID, value: IndexID(e.Index.ID)},
		{key: AttributeElementName, value: ElementName(e.Index.Name)},
	})
}

// DescriptorID implements the Element interface.
func (e *SequenceDependency) DescriptorID() descpb.ID { return e.SequenceID }

// GetAttributes implements the Element interface
func (e *SequenceDependency) GetAttributes() Attributes {
	return makeAttributes([]attributeValue{
		{key: AttributeType, value: ElementType(e)},
		{key: AttributeDescID, value: DescID(e.SequenceID)},
		{key: AttributeDepID, value: DescID(e.TableID)},
		{key: AttributeColumnID, value: ColumnID(e.ColumnID)},
	})
}

// DescriptorID implements the Element interface.
func (e *UniqueConstraint) DescriptorID() descpb.ID { return e.TableID }

// GetAttributes implements the Element interface
func (e *UniqueConstraint) GetAttributes() Attributes {
	return makeAttributes([]attributeValue{
		{key: AttributeType, value: ElementType(e)},
		{key: AttributeDescID, value: DescID(e.TableID)},
		{key: AttributeIndexID, value: IndexID(e.IndexID)},
	})
}

// DescriptorID implements the Element interface.
func (e *CheckConstraint) DescriptorID() descpb.ID { return e.TableID }

// GetAttributes implements the Element interface
func (e *CheckConstraint) GetAttributes() Attributes {
	return makeAttributes([]attributeValue{
		{key: AttributeType, value: ElementType(e)},
		{key: AttributeElementName, value: ElementName(e.Name)},
		{key: AttributeDescID, value: DescID(e.TableID)},
	})
}

// DescriptorID implements the Element interface.
func (e *Sequence) DescriptorID() descpb.ID { return e.TableID }

// GetAttributes implements the Element interface
func (e *Sequence) GetAttributes() Attributes {
	return makeAttributes([]attributeValue{
		{key: AttributeType, value: ElementType(e)},
		{key: AttributeDescID, value: DescID(e.TableID)},
	})
}

// DescriptorID implements the Element interface.
func (e *DefaultExpression) DescriptorID() descpb.ID { return e.TableID }

// GetAttributes implements the Element interface
func (e *DefaultExpression) GetAttributes() Attributes {
	return makeAttributes([]attributeValue{
		{key: AttributeType, value: ElementType(e)},
		{key: AttributeDescID, value: DescID(e.TableID)},
		{key: AttributeColumnID, value: ColumnID(e.ColumnID)},
	})
}

// DescriptorID implements the Element interface.
func (e *View) DescriptorID() descpb.ID { return e.TableID }

// GetAttributes implements the Element interface
func (e *View) GetAttributes() Attributes {
	return makeAttributes([]attributeValue{
		{key: AttributeType, value: ElementType(e)},
		{key: AttributeDescID, value: DescID(e.TableID)},
	})
}

// DescriptorID implements the Element interface
func (e *TypeReference) DescriptorID() descpb.ID { return e.DescID }

// GetAttributes implements the Element interface
func (e *TypeReference) GetAttributes() Attributes {
	return makeAttributes([]attributeValue{
		{key: AttributeType, value: ElementType(e)},
		{key: AttributeDescID, value: DescID(e.TypeID)},
		{key: AttributeDepID, value: DescID(e.DescID)},
	})
}
