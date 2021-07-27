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

import "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav"

// Attr are keys used for finger prints of objects
// for comparing uniqueness
type Attr int

// Ordinal is part of the eav.Attribute interface.
func (i Attr) Ordinal() eav.Ordinal { return eav.Ordinal(i) }

// Type is part of the eav.Attribute interface.
func (i Attr) Type() eav.Type { return attrTypes[i] }

var attrTypes = [...]eav.Type{
	AttrElementType:      eav.TypeInt32,
	AttrDescID:           eav.TypeUint32,
	AttrReferencedDescID: eav.TypeUint32,
	AttrColumnID:         eav.TypeUint32,
	AttrName:             eav.TypeString,
	AttrIndexID:          eav.TypeUint32,
	AttrDirection:        eav.TypeInt32,
	AttrStatus:           eav.TypeInt32,
}

var _ eav.Attribute = Attr(0)

//go:generate stringer -type=Attr -trimprefix=Attr
const (

	// AttrElementType type id of the element.
	AttrElementType Attr = iota
	// AttrDescID is the descriptor ID to which this element belongs.
	AttrDescID
	// AttrReferencedDescID is the descriptor ID to which this element refers.
	AttrReferencedDescID
	//AttrColumnID is the column ID to which this element corresponds.
	AttrColumnID
	// AttrName is the name of the element.
	AttrName
	// AttrIndexID is the index ID to which this element corresponds.
	AttrIndexID
	// AttrDirection is the direction of a Target or Node.
	AttrDirection
	// AttrStatus is the Status of a Node.
	AttrStatus

	NumAttrs int = iota
)

// AttrSchema returns the eav.Schema used for entities.
func AttrSchema() eav.Schema {
	return &attrSet
}

type attributes [NumAttrs]eav.Attribute

var attrSet = attributes{
	AttrElementType,
	AttrDescID,
	AttrReferencedDescID,
	AttrColumnID,
	AttrName,
	AttrIndexID,
	AttrDirection,
	AttrStatus,
}

var attributeOrdinals = eav.MakeOrdinalSetWithAttributes(attrSet[:])

func (a attributes) Attributes() eav.OrdinalSet     { return attributeOrdinals }
func (a attributes) At(i eav.Ordinal) eav.Attribute { return a[i] }

// Compare compares two elements by their attributes.
func Compare(a, b Entity) (less, eq bool) {
	return eav.Compare(&attrSet, a, b)
}

// Equal compares two elements by their attributes.
func Equal(a, b Entity) (eq bool) {
	return eav.Equal(&attrSet, a, b)
}
