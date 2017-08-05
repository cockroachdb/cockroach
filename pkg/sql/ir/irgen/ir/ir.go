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

package ir

import (
	"github.com/cockroachdb/cockroach/pkg/sql/ir/irgen/parser"
)

// NamedType represents a type definition.
type NamedType struct {
	Name parser.TypeName
	// Type is nil for primitive types.
	Type Type
}

func (t *NamedType) String() string {
	return t.Name.String()
}

// Type represents an anonymous type of any kind.
type Type interface {
	isType()
}

// EnumType represents an anonymous enumeration.
type EnumType struct {
	Items []EnumItem
}

// EnumItem represents an enumeration constant.
type EnumItem struct {
	Name parser.ItemName
	Tag  parser.Tag
}

func (EnumType) isType() {}

// StructType represents an anonymous struct.
type StructType struct {
	Items []StructItem
}

// StructItem represents a struct field.
type StructItem struct {
	Name    parser.ItemName
	IsSlice bool
	Type    *NamedType
	Tag     parser.Tag
}

func (StructType) isType() {}

// SumType represents an anonymous sum.
type SumType struct {
	Items []SumItem
}

// SumItem represents a sum alternative.
type SumItem struct {
	Type *NamedType
	Tag  parser.Tag
}

func (SumType) isType() {}
