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
	"fmt"
	"reflect"
	"sort"
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

// Attribute are keys used for finger prints of objects
// for comparing uniqueness
type Attribute int

// DescID descriptor ID
type DescID descpb.ID

// ColumnID column ID
type ColumnID descpb.ColumnID

// IndexID index ID
type IndexID descpb.IndexID

// ElementName name of the element
type ElementName string

// AttributeValue implements generic methods any attribute values
type AttributeValue interface {
	fmt.Stringer
}

func (id DescID) String() string {
	return strconv.Itoa(int(id))
}

func (id ColumnID) String() string {
	return strconv.Itoa(int(id))
}

func (id IndexID) String() string {
	return strconv.Itoa(int(id))
}

func (id ElementTypeID) String() string {
	return strconv.Itoa(int(id))
}

func (name ElementName) String() string {
	return string(name)
}

type attributeValue struct {
	key   Attribute
	value AttributeValue
}

// Attributes contains all attributes for a given
// element
type Attributes struct {
	values []attributeValue // will be a sorted set
}

// Get fetches an attribute by the index
func (a Attributes) Get(attribute Attribute) AttributeValue {
	idx := sort.Search(len(a.values), func(i int) bool {
		return a.values[i].key > attribute
	})

	if idx == len(a.values) {
		return nil
	}
	return a.values[idx].value
}

// Compare compares two attribute values
// for less or equal
func Compare(a, b attributeValue) (less, eq bool) {
	if a.key != b.key {
		return a.key < b.key, false
	}

	switch first := a.value.(type) {
	case ColumnID:
		second, ok := b.value.(ColumnID)
		if !ok {
			panic("Different types for same key")
		}
		return first < second, first == second
	case DescID:
		second, ok := b.value.(DescID)
		if !ok {
			panic("Different types for same key")
		}
		return first < second, first == second
	case IndexID:
		second, ok := b.value.(IndexID)
		if !ok {
			panic("Different types for same key")
		}
		return first < second, first == second
	case ElementTypeID:
		second, ok := b.value.(ElementTypeID)
		if !ok {
			panic("Different types for same key")
		}
		return first < second, first == second
	case ElementName:
		second, ok := b.value.(ElementName)
		if !ok {
			panic("Different types for same key")
		}
		return first < second, first == second
	default:
		panic("Unknown attribute value type")
	}
	return false, false
}

// Equal compares if two attribute sets are equal
func (a Attributes) Equal(other Attributes) bool {
	for idx, firstAttr := range a.values {
		secondAttr := other.values[idx]
		if _, match := Compare(firstAttr, secondAttr); !match {
			return false
		}
	}
	return true
}

// String seralizes attribute into a string
func (a Attributes) String() (result string) {
	for _, attrib := range a.values {
		result += fmt.Sprintf("%s : %s\n", attrib.key.String(), attrib.value.String())
	}
	return result
}

//go:generate stringer -type=Attribute  -trimprefix=Attribute
const (
	_ Attribute = iota
	//AttributeColumnID column ID
	AttributeColumnID Attribute = 1
	// AttributeElementName name of the element
	AttributeElementName Attribute = 2
	// AttributeIndexID index ID
	AttributeIndexID Attribute = 3
	// AttributeDescID main descriptor ID
	AttributeDescID Attribute = 4
	// AttributeDepID dependent descriptor ID
	AttributeDepID Attribute = 5
	// AttributeType type id of the element
	AttributeType Attribute = 6
)

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
