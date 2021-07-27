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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

// GetDescID returns the ID stored in the Entity.
func GetDescID(e Entity) descpb.ID {
	if idVal, ok := e.Get(AttrDescID).(*eav.Uint32); ok {
		return descpb.ID(*idVal)
	}
	return descpb.InvalidID
}

// getElementType returns a value corresponding to the type of the element.
func getElementType(e Entity) *eav.Int32 {
	initOnce.Do(initElementTypes)
	return elementIDs[typeToElementID[reflect.TypeOf(e.GetElement())]]
}

// Definitions of useful values.
var (
	AddDirection  = (*eav.Int32)(proto.Int32(int32(Target_ADD)))
	DropDirection = (*eav.Int32)(proto.Int32(int32(Target_DROP)))

	PublicStatus             = (*eav.Int32)(proto.Int32(int32(Status_PUBLIC)))
	DeleteOnlyStatus         = (*eav.Int32)(proto.Int32(int32(Status_DELETE_ONLY)))
	DeleteAndWriteOnlyStatus = (*eav.Int32)(proto.Int32(int32(Status_DELETE_AND_WRITE_ONLY)))
	AbsentStatus             = (*eav.Int32)(proto.Int32(int32(Status_ABSENT)))

	TableElement          = getElementType((*Table)(nil))
	TypeElement           = getElementType((*Type)(nil))
	TypeReferenceElement  = getElementType((*TypeReference)(nil))
	PrimaryIndexElement   = getElementType((*PrimaryIndex)(nil))
	SecondaryIndexElement = getElementType((*SecondaryIndex)(nil))
	ColumnElement         = getElementType((*Column)(nil))

	// Defeat the unused linter.
	_ = []eav.Value{
		AddDirection, DropDirection,
		PublicStatus, DeleteOnlyStatus, DeleteAndWriteOnlyStatus, AbsentStatus,
		PrimaryIndexElement, SecondaryIndexElement, ColumnElement,
	}
)

// typeID represents type ID of a element.
// Its value correspond to the protobuf field ID of the element in the
// ElementProto oneOf.
type typeID int32

var (
	initOnce sync.Once

	typeToElementID map[reflect.Type]typeID

	elementNames map[typeID]string

	// elementIDs is a mapping to the canonical values which will be used by
	// the elements to avoid allocating every time we access the attribute.
	elementIDs map[typeID]*eav.Int32
)

func initElementTypes() {
	typ := reflect.TypeOf((*ElementProto)(nil)).Elem()
	typeToElementID = make(map[reflect.Type]typeID, typ.NumField())
	elementIDs = make(map[typeID]*eav.Int32, typ.NumField())
	elementNames = make(map[typeID]string, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		protoFlags := strings.Split(f.Tag.Get("protobuf"), ",")
		id, err := strconv.Atoi(protoFlags[1])
		if err != nil {
			panic(errors.Wrapf(err, "failed to extract ID from protobuf tag: %q", protoFlags))
		}
		typeToElementID[f.Type] = typeID(id)
		elementID := typeID(id)
		elementIDs[elementID] = (*eav.Int32)(&elementID)
		elementNames[elementID] = strings.TrimPrefix(f.Type.String(), "*scpb.")
	}
}
