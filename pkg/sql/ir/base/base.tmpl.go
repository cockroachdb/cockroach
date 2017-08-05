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

package base

import "fmt"

// node is a generic ADT node type, with slots for references to other nodes and
// enumeration values. Values that don't fit and values of other types go in the
// extra field.
type node struct {
	refs  ['r']*node
	enums ['e']enum
	extra
}

type enum int32

type extra interface {
	extraRefs() []*node
}

// Allocator allocates nodes in batches. Construct Allocators with NewAllocator
// and pass them by value.
type Allocator struct {
	nodes *[]node
}

// NewAllocator constructs a new Allocator.
func NewAllocator() Allocator {
	nodes := make([]node, 16)
	return Allocator{&nodes}
}

// new allocates a new node. Users of this package should use the appropriate
// R() method, which is type safe.
func (a Allocator) new() *node {
	nodes := *a.nodes
	if len(nodes) == 0 {
		nodes = make([]node, 256)
	}
	x := &nodes[0]
	*a.nodes = nodes[1:]
	return x
}

// @for enum

// ---- Enum ---- //

// Enum is an enumeration type.
type Enum enum

// Enum constants.
const (
	// @for item
	EnumName Enum = 't'
	// @done item
)

func (x Enum) String() string {
	switch x {
	// @for item
	case EnumName:
		return "Name"
	// @done item
	default:
		return fmt.Sprintf("<unknown Enum %d>", x)
	}
}

// @done enum

// @for struct

// ---- Struct ---- //

// Struct is the type of a reference to an immutable record.
type Struct struct{ ref *node }

// StructValue is the logical type of a record. Immutable records are stored in
// nodes.
type StructValue struct {
	// @for item
	Name Type // 't'
	// @done item
}

// @if extraField

type extraStruct struct {
	// @for extraField
	Name Type
	// @done extraField
}

func (x extraStruct) extraRefs() []*node { return macroGetExtraRefs(x) }

// @fi extraField

// R constructs a reference to an immutable record.
func (x StructValue) R(a Allocator) Struct {
	ref := a.new()
	// @if extraField
	extra := &extraStruct{}
	ref.extra = extra
	// @fi extraField

	// @for item
	macroSetName(ref, extra, x.Name)
	// @done item
	return Struct{ref}
}

// @for item

// Name returns the Name field.
func (x Struct) Name() Type { return macroGetName(x.ref) }

// @done item

// V unpacks a reference into a value.
func (x Struct) V() StructValue {
	return StructValue{
		// @for item
		macroGetName(x.ref),
		// @done item
	}
}

// @for item

// WithName constructs a new value where the value of Name has been replaced by
// the argument.
func (x StructValue) WithName(y Type) StructValue {
	x.Name = y
	return x
}

// @done item

// @done struct

// @for sum

// ---- Sum ---- //

// Sum is the type of a tagged union of records.
type Sum struct {
	tag SumTag
	ref *node
}

// SumTag is the tag type.
type SumTag enum

// SumTag constants.
const (
	// @for item
	SumType SumTag = 't'
	// @done item
)

func (x SumTag) String() string {
	switch x {
	// @for item
	case SumType:
		return "Type"
	// @done item
	default:
		return fmt.Sprintf("<unknown SumTag %d>", x)
	}
}

// Tag returns the tag.
func (x Sum) Tag() SumTag { return x.tag }

// @for item

// Sum performs an upcast.
func (x Type) Sum() Sum { return Sum{SumType, x.ref} }

// Type performs a downcast. If the downcast fails, return false.
func (x Sum) Type() (Type, bool) {
	if x.tag != SumType {
		return Type{}, false
	}
	return Type{x.ref}, true
}

// MustBeType performs a downcast. If the downcast fails, panic.
func (x Sum) MustBeType() Type {
	if x.tag != SumType {
		panic(fmt.Sprintf("type assertion failed: expected Type but got %s", x.tag))
	}
	return Type{x.ref}
}

// @done item

// @done sum
