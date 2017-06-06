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
	return string(t.Name)
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
