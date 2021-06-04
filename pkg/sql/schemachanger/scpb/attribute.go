// Copyright 2021 The Cockroach Authors.
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
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/errors"
)

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

// AttributeValue implements generic methods any attribute values.
type AttributeValue interface {
	fmt.Stringer
}

// String implements AttributeValue.
func (id DescID) String() string {
	return strconv.Itoa(int(id))
}

// String implements AttributeValue.
func (id ColumnID) String() string {
	return strconv.Itoa(int(id))
}

// String implements AttributeValue.
func (id IndexID) String() string {
	return strconv.Itoa(int(id))
}

// String implements AttributeValue.
func (id ElementTypeID) String() string {
	return ElementIDToString(id)
}

// String implements AttributeValue.
func (name ElementName) String() string {
	buffer := bytes.Buffer{}
	lexbase.EncodeEscapedSQLIdent(&buffer, string(name))
	return buffer.String()
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

// Constructor for attributes
func makeAttributes(values []attributeValue) Attributes {
	sort.Slice(values, func(i, j int) bool {
		return values[i].key < values[j].key
	})
	return Attributes{values: values}
}

// Get fetches an attribute by the index.
func (a Attributes) Get(attribute Attribute) AttributeValue {
	idx := sort.Search(len(a.values), func(i int) bool {
		return a.values[i].key >= attribute
	})

	if idx == len(a.values) {
		return nil
	}
	return a.values[idx].value
}

// Compare compares two attribute values
// for less or equal.
func Compare(a, b attributeValue) (less, eq bool) {
	if a.key != b.key {
		return a.key < b.key, false
	}
	if aType, bType := reflect.TypeOf(a), reflect.TypeOf(b); aType != bType {
		panic(errors.AssertionFailedf("different types for key %s: %s != %s", a.key, aType, bType))
	}
	switch av := a.value.(type) {
	case ColumnID:
		bv := b.value.(ColumnID)
		return av < bv, av == bv
	case DescID:
		second := b.value.(DescID)
		return av < second, av == second
	case IndexID:
		second := b.value.(IndexID)
		return av < second, av == second
	case ElementTypeID:
		second := b.value.(ElementTypeID)
		return av < second, av == second
	case ElementName:
		second := b.value.(ElementName)
		return av < second, av == second
	default:
		panic(errors.AssertionFailedf("unknown attribute value type %T for key %s", av, a.key))
	}
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
func (a Attributes) String() string {
	result := strings.Builder{}
	result.WriteString(a.Get(AttributeType).String())
	result.WriteString(":{")
	for attribIdx, attrib := range a.values {
		if attrib.key == AttributeType {
			continue
		}
		result.WriteString(attrib.key.String())
		result.WriteString(": ")
		result.WriteString(attrib.value.String())
		if attribIdx < len(a.values)-1 {
			result.WriteString(", ")
		}
	}
	result.WriteString("}")
	return result.String()
}

//go:generate stringer -type=Attribute  -trimprefix=Attribute
const (
	_ Attribute = iota
	// AttributeType type id of the element
	AttributeType Attribute = 1
	// AttributeDescID main descriptor ID
	AttributeDescID Attribute = 2
	// AttributeDepID dependent descriptor ID
	AttributeDepID Attribute = 3
	//AttributeColumnID column ID
	AttributeColumnID Attribute = 4
	// AttributeElementName name of the element
	AttributeElementName Attribute = 5
	// AttributeIndexID index ID
	AttributeIndexID Attribute = 6
)
