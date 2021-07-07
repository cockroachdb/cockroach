// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scattr

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// Value is a value corresponding to a given attribute of an element.
type Value interface {
	fmt.Stringer

	// compare compares this to another Value. If the types
	// are the same,
	compare(other Value) (ok, less, eq bool)
}

// GetDescID returns the ID of the descriptor to which this element
// corresponds.
func GetDescID(e scpb.Element) descpb.ID {
	if id, ok := Get(DescID, e).(*descID); ok {
		return descpb.ID(*id)
	}
	// See TestAllElementsHaveDescID for why this is safe.
	panic(errors.AssertionFailedf(
		"element of type %T returned a nil descriptor ID", e))
}

type descID descpb.ID

func makeDescID(id *descpb.ID) Value { return (*descID)(id) }

func (id descID) String() string { return strconv.Itoa(int(id)) }
func (id descID) compare(other Value) (ok, less, eq bool) {
	if o, ok := other.(*descID); ok {
		return true, id < *o, id == *o
	}
	return false, false, false
}

type columnID descpb.ColumnID

func makeColumnID(id *descpb.ColumnID) Value { return (*columnID)(id) }

func (id columnID) String() string { return strconv.Itoa(int(id)) }
func (id columnID) compare(other Value) (ok, less, eq bool) {
	if o, ok := other.(*columnID); ok {
		return true, id < *o, id == *o
	}
	return false, false, false
}

type indexID descpb.IndexID

func makeIndexID(id *descpb.IndexID) Value { return (*indexID)(id) }

func (id indexID) String() string { return strconv.Itoa(int(id)) }
func (id indexID) compare(other Value) (ok, less, eq bool) {
	if o, ok := other.(*indexID); ok {
		return true, id < *o, id == *o
	}
	return false, false, false
}

type name string

func makeName(n *string) Value { return (*name)(n) }

func (n name) String() string {
	buffer := bytes.Buffer{}
	lexbase.EncodeEscapedSQLIdent(&buffer, string(n))
	return buffer.String()
}

func (n name) compare(other Value) (ok, less, eq bool) {
	if o, ok := other.(*name); ok {
		return true, n < *o, n == *o
	}
	return false, false, false
}

// typeID represents type ID of a element.
// Its value correspond to the protobuf field ID of the element in the
// ElementProto oneOf.
type typeID int

func getElementType(el protoutil.Message) Value {
	return elementIDs[typeToElementID[reflect.TypeOf(el)]]
}

func (id typeID) String() string { return elementIDToString[id] }
func (id typeID) compare(other Value) (ok, less, eq bool) {
	if o, ok := other.(*typeID); ok {
		return true, id < *o, id == *o
	}
	return false, false, false
}

var (
	typeToElementID   map[reflect.Type]typeID
	elementIDToString map[typeID]string

	// elementIDs is a mapping to the canonical values which will be used by
	// the elements to avoid allocating every time we access the attribute.
	elementIDs map[typeID]*typeID
)

func init() {
	typ := reflect.TypeOf((*scpb.ElementProto)(nil)).Elem()
	typeToElementID = make(map[reflect.Type]typeID, typ.NumField())
	elementIDToString = make(map[typeID]string, typ.NumField())
	elementIDs = make(map[typeID]*typeID, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		protoFlags := strings.Split(f.Tag.Get("protobuf"), ",")
		id, err := strconv.Atoi(protoFlags[1])
		if err != nil {
			panic(errors.Wrapf(err, "failed to extract ID from protobuf tag: %q", protoFlags))
		}
		typeToElementID[f.Type] = typeID(id)
		elementID := typeID(id)
		elementIDToString[elementID] = strings.TrimPrefix(f.Type.String(), "*scpb.")
		elementIDs[elementID] = &elementID
	}
}
