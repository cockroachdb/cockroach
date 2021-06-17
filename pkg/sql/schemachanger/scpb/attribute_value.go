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
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/errors"
)

// attributeValue is a value corresponding to a given attribute of an element.
type attributeValue interface {
	fmt.Stringer

	// compare compares this to another attributeValue. If the types
	// are the same,
	compare(other attributeValue) (ok, less, eq bool)
}

// GetDescID returns the ID of the descriptor to which this element
// corresponds.
func GetDescID(e Element) descpb.ID {
	if id, ok := e.getAttribute(AttributeDescID).(*descID); ok {
		return descpb.ID(*id)
	}
	// See TestAllElementsHaveDescID for why this is safe.
	panic(errors.AssertionFailedf(
		"element of type %T returned a nil descriptor ID", e))
}

type descID descpb.ID

func (id descID) String() string {
	return strconv.Itoa(int(id))
}

func (id descID) compare(other attributeValue) (ok, less, eq bool) {
	if o, ok := other.(*descID); ok {
		return true, id < *o, id == *o
	}
	return false, false, false
}

type columnID descpb.ColumnID

func (id columnID) String() string {
	return strconv.Itoa(int(id))
}

func (id columnID) compare(other attributeValue) (ok, less, eq bool) {
	if o, ok := other.(*columnID); ok {
		return true, id < *o, id == *o
	}
	return false, false, false
}

type indexID descpb.IndexID

func (id indexID) String() string {
	return strconv.Itoa(int(id))
}

func (id indexID) compare(other attributeValue) (ok, less, eq bool) {
	if o, ok := other.(*indexID); ok {
		return true, id < *o, id == *o
	}
	return false, false, false
}

type elementName string

func (name elementName) String() string {
	buffer := bytes.Buffer{}
	lexbase.EncodeEscapedSQLIdent(&buffer, string(name))
	return buffer.String()
}

func (name elementName) compare(other attributeValue) (ok, less, eq bool) {
	if o, ok := other.(*elementName); ok {
		return true, name < *o, name == *o
	}
	return false, false, false
}

// elementTypeID represents type ID of a element.
// Its value correspond to the protobuf field ID of the element in the
// ElementProto oneOf.
type elementTypeID int

// getElementTypeID determines the type ID of a element.
func getElementTypeID(el Element) *elementTypeID {
	return elementIDs[typeToElementID[reflect.TypeOf(el)]]
}

func (id elementTypeID) String() string {
	return elementIDToString[id]
}

func (id elementTypeID) compare(other attributeValue) (ok, less, eq bool) {
	if o, ok := other.(*elementTypeID); ok {
		return true, id < *o, id == *o
	}
	return false, false, false
}

var typeToElementID map[reflect.Type]elementTypeID
var elementIDToString map[elementTypeID]string

// elementIDs is a mapping to the canonical values which will be used by
// the elements to avoid allocating every time we access the attribute.
var elementIDs map[elementTypeID]*elementTypeID

func init() {
	typ := reflect.TypeOf((*ElementProto)(nil)).Elem()
	typeToElementID = make(map[reflect.Type]elementTypeID, typ.NumField())
	elementIDToString = make(map[elementTypeID]string, typ.NumField())
	elementIDs = make(map[elementTypeID]*elementTypeID, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		protoFlags := strings.Split(f.Tag.Get("protobuf"), ",")
		id, err := strconv.Atoi(protoFlags[1])
		if err != nil {
			panic(errors.Wrapf(err, "failed to extract ID from protobuf tag: %q", protoFlags))
		}
		typeToElementID[f.Type] = elementTypeID(id)
		elementID := elementTypeID(id)
		elementIDToString[elementID] = strings.TrimPrefix(f.Type.String(), "*scpb.")
		elementIDs[elementID] = &elementID
	}
}
