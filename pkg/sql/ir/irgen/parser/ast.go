// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package parser

import "strconv"

// Def represents either an enumeration, a primitive type, a struct, or a sum.
type Def struct {
	Kind  DefKind
	Name  TypeNameOccur
	Items []DefItem
}

// DefKind determines the type of a definition.
type DefKind int

// DefKind values.
const (
	EnumDef DefKind = iota
	PrimDef
	StructDef
	SumDef
)

// DefItem represents either an enumeration constant, a struct field, a sum
// alternative, or a reserved tag. Enumeration constants have a name. Struct
// fields have a name and a type. Sum alternatives have a type. Reserved tags
// have neither a name nor a type.
type DefItem struct {
	Name    ItemNameOccur
	IsSlice bool
	Type    TypeNameOccur
	Tag     TagOccur
}

// IsReserved returns true if the item is a tag reservation.
func (i DefItem) IsReserved() bool {
	return i.Name.Name == "" && i.Type.Name == ""
}

// TypeNameOccur represents an occurrence of a TypeName in the input.
type TypeNameOccur struct {
	Name TypeName
	Pos  Pos
}

// ItemNameOccur represents an occurrence of an ItemName in the input.
type ItemNameOccur struct {
	Name ItemName
	Pos  Pos
}

// TagOccur represents an occurrence of a Tag in the input.
type TagOccur struct {
	Tag Tag
	Pos Pos
}

// TypeName represents the name of a type (primitive or not).
type TypeName string

func (tn TypeName) String() string { return string(tn) }

// ItemName represents the name of an enumeration constant or a struct field.
type ItemName string

func (in ItemName) String() string { return string(in) }

// Tag represents a protobuf tag number.
type Tag int32

func (t Tag) String() string { return strconv.FormatInt(int64(t), 10) }

type strOccur struct {
	Str string
	Pos Pos
}
