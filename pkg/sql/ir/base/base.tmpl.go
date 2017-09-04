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

import (
	"fmt"
)

// node is a generic ADT node type, with slots for references to other nodes and
// enumeration values. Values that don't fit and values of other types go in the
// extra field.
type node struct {
	refs [ºnumRefsPerNode]*node
	nums [ºnumNumsPerNode]numvalslot
	strs [ºnumStrsPerNode]string
	extra
}

// enum is the type to define the tag part for working copies of IR
// node with sum and enum types.
type enum uint32

// numvalslot is the type used to store integer values in persistent IR nodes.
// must be larger than or as large as enum.
type numvalslot ºnumValSlotType

type extra interface {
	extraRefs() []*node
}

// Allocator allocates nodes in batches. Construct Allocators with NewAllocator
// and pass them by value.
type Allocator struct {
	nodes []node
}

// MakeAllocator constructs a new Allocator.
func MakeAllocator() Allocator {
	nodes := make([]node, 16)
	return Allocator{nodes}
}

// NewAllocator allocates a new Allocator.
func NewAllocator() *Allocator {
	a := MakeAllocator()
	return &a
}

// new allocates a new node. Users of this package should use the appropriate
// R() method, which is type safe.
func (a *Allocator) new() *node {
	nodes := a.nodes
	if len(nodes) == 0 {
		nodes = make([]node, 256)
	}
	x := &nodes[0]
	a.nodes = nodes[1:]
	return x
}

// @for enum

// ---- ºEnum ---- //

// ºEnum is an enumeration type.
type ºEnum enum

// ºEnum constants.
const (
	// @for item
	ºEnumºItem ºEnum = ºtag
	// @done item
)

func (x ºEnum) String() string {
	switch x {
	// @for item
	case ºEnumºItem:
		return "ºItem"
	// @done item
	default:
		return fmt.Sprintf("<unknown ºEnum %d>", x)
	}
}

// @done enum

// @for struct

// ---- ºStruct ---- //

// ºStruct is the type of a reference to an immutable record.
type ºStruct struct{ ref *node }

// @for slot

const ºStruct_Slot_ºslotName_Type = ºslotType
const ºStruct_Slot_ºslotName_Num = ºslotNum
const ºStruct_Slot_ºslotName_BitSize = ºslotBitSize
const ºStruct_Slot_ºslotName_BitOffset = ºslotBitOffset
const ºStruct_Slot_ºslotName_ByteSize = ºslotByteSize
const ºStruct_Slot_ºslotName_ByteOffset = ºslotByteOffset
const ºStruct_Slot_ºslotName_ValueMask = ºslotValueMask

// @done slot

// ºStructValue is the logical type of a record. Immutable records are stored in
// nodes.
type ºStructValue struct {
	// @for item
	ºItem ºtype // ºtag
	// @done item
}

// @if extraField

type extraºStruct struct {
	// @for extraField
	ºItem ºtype
	// @done extraField
}

func (x extraºStruct) extraRefs() []*node { return ºgetExtraRefs(x) }

// @fi extraField

// R constructs a reference to an immutable record.
func (x ºStructValue) R(a *Allocator) ºStruct {
	ref := a.new()
	// @if extraField
	extra := &extraºStruct{}
	ref.extra = extra
	// @fi extraField

	// @for item
	ºsetField(ref, extra, x.ºItem)
	// @done item
	return ºStruct{ref}
}

// @for item

// ºItem returns the ºItem field.
func (x ºStruct) ºItem() ºtype { return ºgetField(x.ref) }

// @done item

// V unpacks a reference into a value.
func (x ºStruct) V() ºStructValue {
	return ºStructValue{
		// @for item
		ºItem: ºgetField(x.ref),
		// @done item
	}
}

// @for item

// WithºItem constructs a new value where the value of ºItem has been replaced by
// the argument.
func (x ºStructValue) WithºItem(y ºtype) ºStructValue {
	x.ºItem = y
	return x
}

// @done item

// @done struct

// @for sum

// ---- ºSum ---- //

// ºSum is the type of a tagged union of records.
type ºSum struct {
	tag ºSumTag
	ref *node
}

// ºSumTag is the tag type.
type ºSumTag enum

// ºSumTag constants.
const (
	// @for item
	ºSumºtype ºSumTag = ºtag
	// @done item
)

func (x ºSumTag) String() string {
	switch x {
	// @for item
	case ºSumºtype:
		return "ºtype"
	// @done item
	default:
		return fmt.Sprintf("<unknown ºSumTag %d>", x)
	}
}

// Tag returns the tag.
func (x ºSum) Tag() ºSumTag { return x.tag }

// @for item

// ºSum performs an upcast.
func (x ºtype) ºSum() ºSum { return ºSum{ºSumºtype, x.ref} }

// ºtype performs a downcast. If the downcast fails, return false.
func (x ºSum) ºtype() (ºtype, bool) {
	if x.tag != ºSumºtype {
		return ºtype{}, false
	}
	return ºtype{x.ref}, true
}

// MustBeºtype performs a downcast. If the downcast fails, panic.
func (x ºSum) MustBeºtype() ºtype {
	if x.tag != ºSumºtype {
		panic(fmt.Sprintf("type assertion failed: expected ºtype but got %s", x.tag))
	}
	return ºtype{x.ref}
}

// @done item

// @done sum
