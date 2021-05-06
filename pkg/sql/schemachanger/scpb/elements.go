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

func init() {
	typ := reflect.TypeOf((*ElementProto)(nil)).Elem()
	typeToElementID = make(map[reflect.Type]ElementTypeID, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		protoFlags := strings.Split(f.Tag.Get("protobuf"), ",")
		id, err := strconv.Atoi(protoFlags[1])
		if err != nil {
			panic(errors.Wrapf(err, "failed to extract ID from protobuf tag: %q", protoFlags))
		}
		typeToElementID[f.Type] = ElementTypeID(id)
	}
}

// ElementType determines the type ID of a element
func ElementType(el Element) ElementTypeID {
	return typeToElementID[reflect.TypeOf(el)]
}

// DescriptorID implements the Element interface.
func (e *Column) DescriptorID() descpb.ID { return e.TableID }

// GetAttributes implements the Element interface
func (e *Column) GetAttributes() Attributes {
	return Attributes{
		values: []attributeValue{
			{key: AttributeColumnID, value: ColumnID(e.Column.ID)},
			{key: AttributeDescID, value: DescID(e.TableID)},
			{key: AttributeElementName, value: ElementName(e.Column.Name)},
			{key: AttributeType, value: ElementType(e)},
		},
	}
}

// DescriptorID implements the Element interface.
func (e *PrimaryIndex) DescriptorID() descpb.ID { return e.TableID }

// GetAttributes implements the Element interface
func (e *PrimaryIndex) GetAttributes() Attributes {
	return Attributes{
		values: []attributeValue{
			{key: AttributeIndexID, value: IndexID(e.Index.ID)},
			{key: AttributeDescID, value: DescID(e.TableID)},
			{key: AttributeElementName, value: ElementName(e.Index.Name)},
			{key: AttributeType, value: ElementType(e)},
		},
	}
}

// DescriptorID implements the Element interface.
func (e *SecondaryIndex) DescriptorID() descpb.ID { return e.TableID }

// GetAttributes implements the Element interface
func (e *SecondaryIndex) GetAttributes() Attributes {
	return Attributes{
		values: []attributeValue{
			{key: AttributeIndexID, value: IndexID(e.Index.ID)},
			{key: AttributeDescID, value: DescID(e.TableID)},
			{key: AttributeElementName, value: ElementName(e.Index.Name)},
			{key: AttributeType, value: ElementType(e)},
		},
	}
}

// DescriptorID implements the Element interface.
func (e *SequenceDependency) DescriptorID() descpb.ID { return e.SequenceID }

// GetAttributes implements the Element interface
func (e *SequenceDependency) GetAttributes() Attributes {
	return Attributes{
		values: []attributeValue{
			{key: AttributeColumnID, value: ColumnID(e.ColumnID)},
			{key: AttributeDepID, value: DescID(e.TableID)},
			{key: AttributeDescID, value: DescID(e.SequenceID)},
			{key: AttributeType, value: ElementType(e)},
		},
	}
}

// DescriptorID implements the Element interface.
func (e *UniqueConstraint) DescriptorID() descpb.ID { return e.TableID }

// GetAttributes implements the Element interface
func (e *UniqueConstraint) GetAttributes() Attributes {
	return Attributes{
		values: []attributeValue{
			{key: AttributeIndexID, value: IndexID(e.IndexID)},
			{key: AttributeDescID, value: DescID(e.TableID)},
			{key: AttributeType, value: ElementType(e)},
		},
	}
}

// DescriptorID implements the Element interface.
func (e *CheckConstraint) DescriptorID() descpb.ID { return e.TableID }

// GetAttributes implements the Element interface
func (e *CheckConstraint) GetAttributes() Attributes {
	return Attributes{
		values: []attributeValue{
			{key: AttributeDescID, value: DescID(e.TableID)},
			{key: AttributeElementName, value: ElementName(e.Name)},
			{key: AttributeType, value: ElementType(e)},
		},
	}
}

// DescriptorID implements the Element interface.
func (e *Sequence) DescriptorID() descpb.ID { return e.TableID }

// GetAttributes implements the Element interface
func (e *Sequence) GetAttributes() Attributes {
	return Attributes{
		values: []attributeValue{
			{key: AttributeDescID, value: DescID(e.TableID)},
			{key: AttributeType, value: ElementType(e)},
		},
	}
}

// DescriptorID implements the Element interface.
func (e *DefaultExpression) DescriptorID() descpb.ID { return e.TableID }

// GetAttributes implements the Element interface
func (e *DefaultExpression) GetAttributes() Attributes {
	return Attributes{
		values: []attributeValue{
			{key: AttributeColumnID, value: ColumnID(e.ColumnID)},
			{key: AttributeDescID, value: DescID(e.TableID)},
			{key: AttributeType, value: ElementType(e)},
		},
	}
}

// DescriptorID implements the Element interface.
func (e *View) DescriptorID() descpb.ID { return e.TableID }

// GetAttributes implements the Element interface
func (e *View) GetAttributes() Attributes {
	return Attributes{
		values: []attributeValue{
			{key: AttributeDescID, value: DescID(e.TableID)},
			{key: AttributeType, value: ElementType(e)},
		},
	}
}

// DescriptorID implements the Element interface
func (e *TypeReference) DescriptorID() descpb.ID { return e.DescID }

// GetAttributes implements the Element interface
func (e *TypeReference) GetAttributes() Attributes {
	return Attributes{
		values: []attributeValue{
			{key: AttributeDepID, value: DescID(e.DescID)},
			{key: AttributeDescID, value: DescID(e.TypeID)},
			{key: AttributeType, value: ElementType(e)},
		},
	}
}
