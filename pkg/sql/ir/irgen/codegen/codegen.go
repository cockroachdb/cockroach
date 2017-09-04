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

package codegen

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/ir/irgen/ir"
	"github.com/cockroachdb/cockroach/pkg/sql/ir/irgen/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/ir/irgen/template"
)

// Config holds configuration parameters for code generation.
type Config struct {
	// NumNumericSlots is the number of numeric slots.
	NumNumericSlots int
	// NumSlotSize is the size of a numeric slot in the in-memory
	// representation, in bits.
	NumericSlotSize int

	// NumRefSlots is the number of reference slots.
	NumRefSlots int

	// NumStrSlots is the number of slots for "string" values. Set to zero
	// to avoid using dedicated string slots.
	NumStrSlots int

	// Pack, if true, attempts to reuse slots for multiple
	// numeric values.
	Pack bool
}

// Pervasives are predefined types in every language.
var Pervasives = []parser.Def{
	{Kind: parser.PrimDef, Name: parser.TypeNameOccur{Name: "bool"}},
	{Kind: parser.PrimDef, Name: parser.TypeNameOccur{Name: "string"}},
	{Kind: parser.PrimDef, Name: parser.TypeNameOccur{Name: "int8"}},
	{Kind: parser.PrimDef, Name: parser.TypeNameOccur{Name: "uint8"}},
	{Kind: parser.PrimDef, Name: parser.TypeNameOccur{Name: "int16"}},
	{Kind: parser.PrimDef, Name: parser.TypeNameOccur{Name: "uint16"}},
	{Kind: parser.PrimDef, Name: parser.TypeNameOccur{Name: "int32"}},
	{Kind: parser.PrimDef, Name: parser.TypeNameOccur{Name: "uint32"}},
	{Kind: parser.PrimDef, Name: parser.TypeNameOccur{Name: "int64"}},
	{Kind: parser.PrimDef, Name: parser.TypeNameOccur{Name: "uint64"}},
	{Kind: parser.PrimDef, Name: parser.TypeNameOccur{Name: "float32"}},
	{Kind: parser.PrimDef, Name: parser.TypeNameOccur{Name: "float64"}},
}

// DefaultConfig is to be used as default configuration for code generation.
var DefaultConfig = Config{
	NumNumericSlots: 2,
	NumericSlotSize: 64,
	NumRefSlots:     2,
	NumStrSlots:     1,
	Pack:            true,
}

// LanguageDef is a list of ADT definitions.
type LanguageDef []ir.NamedType

// Generate generates code for the given languages.
func (cfg *Config) Generate(w io.Writer, tmpl *template.Template, languages ...LanguageDef) error {
	root := template.NewRoot()

	// Common configuration parameters.
	root.AddReplacementf("numRefsPerNode", "%d", cfg.NumRefSlots)
	root.AddReplacementf("numNumsPerNode", "%d", cfg.NumNumericSlots)
	root.AddReplacementf("numStrsPerNode", "%d", cfg.NumStrSlots)
	root.AddReplacementf("numValSlotType", "uint%d", cfg.NumericSlotSize)

	for _, language := range languages {
		for _, namedType := range language {
			switch t := namedType.Type.(type) {
			case nil:
				// Primary type. Nothing to do.

			case *ir.EnumType:
				node := root.NewChild("enum")
				node.AddReplacement("Enum", namedType.Name)
				for _, i := range t.Items {
					child := node.NewChild("item")
					child.AddReplacement(paramName, i.Name)
					child.AddReplacement(paramTag, i.Tag)
				}

			case *ir.SumType:
				node := root.NewChild("sum")
				node.AddReplacement("Sum", namedType.Name)
				for _, i := range t.Items {
					child := node.NewChild("item")
					child.AddReplacement(paramType, i.Type)
					child.AddReplacement(paramTag, i.Tag)
				}

			case *ir.StructType:
				// Structs (product types).
				node := root.NewChild("struct")
				node.AddReplacement("Struct", namedType.Name)
				a := slotAllocator{
					Config:     cfg,
					structNode: node,
				}

				// Allocate the slots - this decides which fields go where in the
				// persistent in-memory representation of nodes.
				if err := a.allocateSlots(namedType.Name, t.Items); err != nil {
					return err
				}

				// Allow the template to inspect the slot structure.
				for _, sslotName := range a.slotNames {
					slotName := parser.ItemName(sslotName)
					slot := a.slots[slotName]
					child := node.NewChild("slot")
					child.AddReplacement(paramSlotName, slotName)
					child.AddReplacement(paramSlotType, fmt.Sprintf("%d", slot.slotType))
					child.AddReplacement(paramSlotNum, fmt.Sprintf("%d", slot.slotNum))
					if slot.slotType == slotTypeNum {
						child.AddReplacement(paramSlotBitSize, fmt.Sprintf("%d", slot.szBits))
						child.AddReplacement(paramSlotByteSize, fmt.Sprintf("%d", (slot.szBits+7)/8))
						child.AddReplacement(paramSlotBitOffset, fmt.Sprintf("%d", slot.bitOffset))
						child.AddReplacement(paramSlotByteOffset, fmt.Sprintf("%d", slot.bitOffset/8))
						child.AddReplacement(paramSlotValueMask, fmt.Sprintf("%#x", uint64((1<<uint(slot.szBits))-1)))
					} else {
						child.AddReplacement(paramSlotBitSize, "-1")
						child.AddReplacement(paramSlotByteSize, "-1")
						child.AddReplacement(paramSlotBitOffset, "-1")
						child.AddReplacement(paramSlotByteOffset, "-1")
						child.AddReplacement(paramSlotValueMask, "-1")
					}
				}

				// Populate the field members.
				for _, i := range t.Items {
					child := node.NewChild("item")

					// isAtomic is defined for non-reference
					// types, including enums.
					// This can be used as conditional for value comparisons.
					// A "deep equal" operator, for example, needs to recurse
					// on non-atomic types only.

					// isPrimitive is defined only for primitive types.
					// This can be used as conditional for "leaf" behavior.

					isAtomic := true
					if i.Type.Type != nil {
						_ = child.NewChild("isNotPrimitive")
						_, isAtomic = i.Type.Type.(*ir.EnumType)
					} else {
						_ = child.NewChild("isPrimitive")
					}
					if !isAtomic {
						_ = child.NewChild("isNotAtomic")
					} else {
						_ = child.NewChild("isAtomic")
					}

					child.AddReplacement(paramName, i.Name)
					child.AddReplacement(paramType, i.Type)
					child.AddReplacement(paramTypeTitle, strings.Title(i.Type.String()))
					child.AddReplacement(paramTag, i.Tag)
					child.AddReplacement(macroGetName, a.slots[i.Name].getName)
					child.AddReplacement(macroSetName, a.slots[i.Name].setName)
				}
				node.AddReplacement(macroGetExtraRefs, a.finish())

			default:
				panic(fmt.Sprintf("case %T is not handled", t))
			}
		}
	}
	return tmpl.Instantiate(w, root)
}

const (
	paramName           = "Item"
	paramType           = "type"
	paramTypeTitle      = "Type"
	paramTag            = "tag"
	macroGetName        = `getField\(([.º\w]+)\)`
	macroSetName        = `setField\(([.º\w]+), ([.º\w]+), ([.º\w]+)\)`
	macroGetExtraRefs   = `getExtraRefs\(([.º\w]+)\)`
	paramSlotName       = "slotName"
	paramSlotType       = "slotType"
	paramSlotNum        = "slotNum"
	paramSlotBitSize    = "slotBitSize"
	paramSlotBitOffset  = "slotBitOffset"
	paramSlotByteSize   = "slotByteSize"
	paramSlotByteOffset = "slotByteOffset"
	paramSlotValueMask  = "slotValueMask"
)

// The interesting part of code generation is fitting structs into nodes.
//
// Each [IR] node consists of slots, i.e. spaces in memory where to put
// values. The goal of slot allocation is to decide which struct field goes
// to which slot.
//
// Each node consists of slots, i.e. spaces in memory where to put
// values.
// The goal of packing is to decide which struct field goes
// to which slot.
//
// There are four kinds of slots: numeric, string, references and extra.
// The numeric, string and reference slots are called "dedicated".
// Dedicated slots come in finite amount!
// For example, in the default configuration, there are 2 numeric slots,
// 1 string slot and 2 reference slots.
//
// In general, we prefer a dedicated slot. When dedicated slots are
// exhausted for a particular type (e.g. when encountering the 3rd
// numeric field in a struct in the default configuration), we spill
// to the extra slots. Extra slots expand on demand without limit.
//
// We support two modes: packed and unpacked.
//
// Understanding unpacked mode can serve as foundation to better
// understand packed mode.
//
// How unpacked mode works:
//
// In that mode, each numeric field uses one numeric slot; each
// reference to a struct uses one reference slot; and each reference
// to a sum uses both a numeric slot (for the tag) and a reference
// slot (for the value). Every other type uses an extra
// slot. When dedicated slots are exhausted, an extra slot is also
// used. For example:
//
// type BinExprValue struct {
//	Left  Expr
//	Op    BinOp
//	Right Expr
// }
// type BinExpr struct { *node }
//
// //// Packing with 3 numeric slots:
// func (x BinExprValue) R(a Allocator) BinExpr {
//	node := a.new()
//	node.nums[0] = numvalslot(x.Left.tag)
//	node.nums[1] = numvalslot(x.Op)
//	node.nums[2] = numvalslot(x.Right.tag)
//	node.refs[0] = x.Left.ref
//	node.refs[1] = x.Right.ref
//	return BinExpr{node}
// }
// func (x BinExpr) Left() Expr  { return Expr{ExprTag(x.node.nums[0]), x.node.refs[0]} }
// func (x BinExpr) Op() BinOp   { return BinOp(x.ref.nums[1]) }
// func (x BinExpr) Right() Expr { return Expr{ExprTag(x.node.nums[2]), x.node.refs[1]} }
//
// //// Packing with just 2 numeric slots, like in the default configuration:
// type extraBinExpr struct {
//	Right__Tag ExprTag
// }
// func (x BinExprValue) R(a Allocator) BinExpr {
//	ref := a.new()
//	ref.nums[0] = numvalslot(x.Left.tag)
//	ref.nums[1] = numvalslot(x.Op)
//	ref.refs[0] = x.Left.ref
//	ref.refs[1] = x.Right.ref
//	ref.extra = &extraBinExpr{}
//	extra.Right__Tag = x.Right.tag
//	return BinExpr{ref}
// }
// func (x BinExpr) Left() Expr  { return Expr{ExprTag(x.ref.nums[0]), x.ref.refs[0]} }
// func (x BinExpr) Op() BinOp   { return BinOp(x.ref.nums[1]) }
// func (x BinExpr) Right() Expr { return Expr{x.ref.extra.(*extraBinExpr).Right__Tag, x.ref.refs[1]} }
//
//
// How packed mode works:
//
// The general idea of packing dedicated slots until they are
// exhausted, and then spilling to extra slots, remains. What is
// different is that the algorithm now tries to fit multiple numeric
// fields in the same numeric slot, to conserve memory. The algorithm
// starts with the largest fields first, to reduce fragmentation. This
// incidentally implies that the fields are not stored in declaration
// order.
// For example:
//
// //// Observe how all 3 numeric values are now packed in a single slot!
// func (x BinExprValue) R(a Allocator) BinExpr {
//	ref := a.new()
//	ref.nums[0] = (ref.nums[0] &^ (BinExpr_Slot_Left__Tag_ValueMask  << BinExpr_Slot_Left__Tag_BitOffset))  | (numvalslot(x.Left.tag)  << BinExpr_Slot_Left__Tag_BitOffset)
//	ref.nums[0] = (ref.nums[0] &^ (BinExpr_Slot_Op_ValueMask         << BinExpr_Slot_Op_BitOffset))         | (numvalslot(x.Op)        << BinExpr_Slot_Op_BitOffset)
//	ref.nums[0] = (ref.nums[0] &^ (BinExpr_Slot_Right__Tag_ValueMask << BinExpr_Slot_Right__Tag_BitOffset)) | (numvalslot(x.Right.tag) << BinExpr_Slot_Right__Tag_BitOffset)
//	ref.refs[0] = x.Left.ref
//	ref.refs[1] = x.Right.ref
//	return BinExpr{ref}
// }
//
// //// Note: the size in bits for sum types is computed automatically
// //// depending on the number of variants.
// const BinExpr_Slot_Left__Tag_BitOffset = 0
// const BinExpr_Slot_Left__Tag_ValueMask = 0x3
// const BinExpr_Slot_Op_BitOffset = 2
// const BinExpr_Slot_Op_ValueMask = 0x3
// const BinExpr_Slot_Right__Tag_BitOffset = 4
// const BinExpr_Slot_Right__Tag_ValueMask = 0x3
//
// func (x BinExpr) Left() Expr {
//	return Expr{ExprTag((x.ref.nums[0] >> BinExpr_Slot_Left__Tag_BitOffset) & BinExpr_Slot_Left__Tag_ValueMask), x.ref.refs[0]}
// }
// func (x BinExpr) Op() BinOp {
//	return BinOp((x.ref.nums[0] >> BinExpr_Slot_Op_BitOffset) & BinExpr_Slot_Op_ValueMask)
// }
// func (x BinExpr) Right() Expr {
//	return Expr{ExprTag((x.ref.nums[0] >> BinExpr_Slot_Right__Tag_BitOffset) & BinExpr_Slot_Right__Tag_ValueMask), x.ref.refs[1]}
// }
//
//
// In addition, in both packed and unpacked mode we have a special handling
// for floating point types, which can be enabled with the configuration flag "allow unsafe".
//
// In safe mode, float values are always sent to extra slots.
// In unsafe mode, float values are stored in numeric slots. The Go
// unsafe package is used to access them (since a numeric cast would
// lose the value).
//

// slotAllocator contains the allocation variables for a single struct type.
type slotAllocator struct {
	*Config
	curRefs, curNums, curStrs int
	numExtraRefs              int
	structNode                *template.Node

	slotNames []string
	slots     map[parser.ItemName]slot
}

// tSizes are the sizes in bits of the predefined primitive types.
var tSizes = map[parser.TypeName]int{
	"bool": 1,
	"int8": 8, "uint8": 8,
	"int16": 16, "uint16": 16,
	"int32": 32, "uint32": 32, "float32": 32,
	"int64": 64, "uint64": 64, "float64": 64,
}

// slot describes one slot in a struct.
type slot struct {
	// getName is the Go code to access the value in a node.
	// The substring "$1" is replaced by a node reference expression
	// during code generation (macroGetName).
	getName string

	// setName is the Go code to write a value in a node.
	// The substrings are replaced as follows during
	// code generation (macroSetName):
	// $1 - an expression evaluating to the node reference;
	// $2 - the reference to extra slots, if one is needed;
	// $3 - the value being written to the node, for a set.
	setName string

	// slotType is the kind of node (numeric, string, reference, extra, extra ref).
	slotType slotType

	// slotNum is the index of the slot in its own category.
	// In packed mode, multiple slots can share the same slotNum,
	// and when they do their offset (defined below) differs.
	slotNum int

	// For numeric slots, we have a size in bits and an offset. In
	// unpacked mode, the size is the size of a slot and the offset is
	// zero.
	szBits    int
	bitOffset int
}

// slotType is used to annotate slot metadata.
type slotType int

const (
	slotTypeRef slotType = iota
	slotTypeNum
	slotTypeStr
	slotTypeExtra
	slotTypeExtraRef
)

// ilog2 returns the first value P so that 2^P is equal to or greater
// than n. For example ilog2(1) = 0, ilog2(4) = 2, ilog2(5) = 3. Note
// that this is different from "find position of leftmost bit".
func ilog2(n int) int {
	return int(math.Ceil(math.Log2(float64(n))))
}

// allocateSlots implements the algorithm described above.
func (a *slotAllocator) allocateSlots(sName parser.TypeName, items []ir.StructItem) error {
	// First phase: compute the desired sizes for every item of non-reference type that
	// can be stored in numeric slots.
	// This includes:
	// - primitive types of integer type;
	// - tags of sum references;
	// - float values if unsafe mode is unabled.

	// allSizes contains one unique value per size actually used
	// by fields in this struct.
	allSizes := make([]int, 0, len(items))

	// slotItem is a helper struct defining a pair (slot name, slot type)
	// It mirrors the struct field name and type in all cases except
	// for the tags of sum references, in which case the word "Tag" is
	// concatenated to both parts.
	type slotItem struct {
		name parser.ItemName
		typ  parser.TypeName
	}

	// itemsInEachSize collects, for each field size, the items
	// that have that size.
	itemsInEachSize := make(map[int][]slotItem)

	// Analyze the struct.
	for _, item := range items {
		// isz is the bit size we'll use for the slot.
		var isz int
		// name and typ are the fields we'll store in the slotItem struct.
		name := item.Name
		typ := item.Type.Name

		if s, ok := item.Type.Type.(*ir.SumType); ok {
			// Sum types need a space for the tag.
			// We compute the size of that space to be just
			// sufficient to store all possible tag values.
			maxTag := parser.Tag(1)
			for _, it := range s.Items {
				if maxTag < it.Tag {
					maxTag = it.Tag
				}
			}
			isz = ilog2(int(maxTag + 1))
			name = name + "__Tag"
			typ = typ + "Tag"
		} else if e, ok := item.Type.Type.(*ir.EnumType); ok {
			// Like for struct types, we compute a size
			// that is just sufficient to store all possible
			// enum values.
			maxTag := parser.Tag(1)
			for _, it := range e.Items {
				if maxTag < it.Tag {
					maxTag = it.Tag
				}
			}
			isz = ilog2(int(maxTag + 1))
		} else if sz, ok := tSizes[item.Type.Name]; ok {
			// For primitive types with a known size, use that.
			isz = sz
		}
		if isz == 0 || isz > a.NumericSlotSize {
			// The type was not handled:
			// - primitive type of unknown size,
			// - primitive type that cannot fit in a numeric slot because
			//   it's too large (e.g. -num-slot-size configured smaller than
			//   the largest primitive type used in the struct)
			// - float type and unsafe mode is disabled,
			// - a struct reference.
			continue
		}
		if !a.Pack {
			// If the user specifies unpacked mode, we pretend the value is
			// as large as the slot. This enables simplified accessors.
			isz = a.NumericSlotSize
		}

		// Store the computed size for later.
		if _, ok := itemsInEachSize[isz]; !ok {
			allSizes = append(allSizes, isz)
		}
		itemsInEachSize[isz] = append(itemsInEachSize[isz], slotItem{name: name, typ: typ})
	}

	// We want to look at the fields from largest to smallest. So we
	// need the sizes to be sorted.
	sort.Ints(allSizes)

	// Second phase is to allocate all the numeric slots, in decreasing
	// field size.

	a.slots = make(map[parser.ItemName]slot)

	// seen collects all the field/slot names that are handled in this
	// phase. This will be later used to skip the slots already handled
	// here, in the third phase below.
	seen := make(map[parser.ItemName]struct{})

	// curNums tracks the index of the current "active" numeric slot
	// where we can put field data in.
	a.curNums = 0
	// curOffsetInNumSlot is the offset of the next bit position
	// available to put data in the current active numeric slot.
	curOffsetInNumSlot := 0

	// Allocate from largest to smallest.
	for j := len(allSizes) - 1; j >= 0 && a.curNums < a.NumericSlotSize; j-- {
		sz := allSizes[j]
		itemsOfThisSize := itemsInEachSize[sz]

		for _, i := range itemsOfThisSize {
			if curOffsetInNumSlot+sz > a.NumericSlotSize {
				// Current slot is exhausted. Allocate a new slot.
				a.curNums++
				curOffsetInNumSlot = 0
			}
			if a.curNums >= a.NumNumericSlots {
				// No more numeric slots available. Give up.
				// We'll just use extra slots below.
				break
			}

			seen[i.name] = struct{}{}
			a.slots[i.name] = a.makeNumSlot(sName, i.name, i.typ, a.curNums, curOffsetInNumSlot, sz)
			curOffsetInNumSlot += sz
		}
	}

	// Third phase: allocate everything else.
	// This includes numeric fields that have to go to extra slots,
	// but also string fields that can go to dedicated string slots,
	// and of course the reference fields.

	for _, i := range items {
		if _, ok := seen[i.Name]; ok {
			// Already allocated above, nothing to do.
			continue
		}

		switch t := i.Type.Type.(type) {
		case nil, (*ir.EnumType):
			if i.Type.Name == "string" {
				a.slots[i.Name] = a.allocateStrSlot(i.Name, i.Type.Name)
			} else {
				// Primitive or enum type, but we already have exhausted all
				// numeric slots above. So give up and allocate an extra slot
				// instead.
				a.slots[i.Name] = a.allocateExtraSlot(i.Name, i.Type.Name)
			}

		case *ir.StructType:
			s := a.allocateRefSlot()
			s.getName = fmt.Sprintf("%s{%s}", i.Type, s.getName)
			// s.setName is unchanged.
			a.slots[i.Name] = s

		case *ir.SumType:
			// Have we already allocated a tag slot for this sum reference?
			// If so, reuse that.
			es, ok := a.slots[i.Name+"__Tag"]
			if !ok {
				// The allocation of a slot for the tag may have failed because
				// all the numeric slots were exhausted. In that case,
				// take an extra slot.
				es = a.allocateExtraSlot(i.Name+"__Tag", i.Type.Name+"Tag")
			}

			s := a.allocateRefSlot()
			s.getName = fmt.Sprintf("%s{%s, %s}", i.Type, es.getName, s.getName)
			s.setName = fmt.Sprintf("%s\n\t%s", strings.Replace(es.setName, "$3", "$3.tag", -1), s.setName)
			a.slots[i.Name] = s

		default:
			panic(fmt.Sprintf("case %T is not handled", t))
		}
	}

	// Allocated all slots, prepare the name array so that the consumer
	// can access the slot data in deterministic order.
	a.slotNames = make([]string, 0, len(a.slots))
	for k := range a.slots {
		a.slotNames = append(a.slotNames, string(k))
	}
	sort.Strings(a.slotNames)

	return nil
}

func (a *slotAllocator) allocateExtraSlot(name parser.ItemName, typ parser.TypeName) slot {
	extra := a.structNode.NewChild("extraField")
	extra.AddReplacement(paramName, name)
	extra.AddReplacement(paramType, typ)
	return slot{
		getName:  fmt.Sprintf("$1.extra.(*extraºStruct).%s", name),
		setName:  fmt.Sprintf("$2.%s = $3", name),
		slotType: slotTypeExtra,
		slotNum:  -1,
	}
}

func (a *slotAllocator) allocateStrSlot(name parser.ItemName, typ parser.TypeName) slot {
	if a.curStrs >= a.NumStrSlots {
		return a.allocateExtraSlot(name, typ)
	}

	j := a.curStrs
	a.curStrs++
	return slot{
		getName:  fmt.Sprintf("$1.strs[%d]", j),
		setName:  fmt.Sprintf("$1.strs[%d] = $3", j),
		slotType: slotTypeStr,
		slotNum:  j,
	}
}

func (a *slotAllocator) allocateRefSlot() slot {
	if a.curRefs >= a.NumRefSlots {
		j := a.numExtraRefs
		a.numExtraRefs++
		return slot{
			getName:  fmt.Sprintf("$1.extra.(*extraºStruct).refs[%d]", j),
			setName:  fmt.Sprintf("$2.refs[%d] = $3.ref", j),
			slotType: slotTypeExtraRef,
			slotNum:  j,
		}
	}

	j := a.curRefs
	a.curRefs++
	return slot{
		getName:  fmt.Sprintf("$1.refs[%d]", j),
		setName:  fmt.Sprintf("$1.refs[%d] = $3.ref", j),
		slotType: slotTypeRef,
		slotNum:  j,
	}
}

func (a *slotAllocator) makeNumSlot(
	sName parser.TypeName, name parser.ItemName, typ parser.TypeName, slotNum, offset, sz int,
) slot {
	s := slot{
		slotType:  slotTypeNum,
		slotNum:   slotNum,
		szBits:    sz,
		bitOffset: offset,
	}
	isFloat := strings.HasPrefix(typ.String(), "float")
	floatSz := tSizes[typ]

	if typ == "bool" {
		// Argh, all this special casing! All this is only needed because
		// Go doesn't know how to convert between bool and integer types.
		if sz < a.NumericSlotSize {
			// Packed mode.
			s.getName = fmt.Sprintf("!(0 == ($1.nums[%[1]d] >> %[2]s_Slot_%[3]s_BitOffset & %[2]s_Slot_%[3]s_ValueMask))", slotNum, sName, name)
			s.setName = fmt.Sprintf(
				"if $3 { $1.nums[%[1]d] |= (numvalslot(1) << %[2]s_Slot_%[3]s_BitOffset); } "+
					"else { $1.nums[%[1]d] &^= (numvalslot(1) << %[2]s_Slot_%[3]s_BitOffset); }",
				slotNum, sName, name)
		} else {
			// Unpacked mode.
			s.getName = fmt.Sprintf("!(0 == $1.nums[%d])", slotNum)
			s.setName = fmt.Sprintf("if $3 { $1.nums[%[1]d] = 1 } else { $1.nums[%[1]d] = 0 }", slotNum)
		}
	} else {
		// General case.
		if sz < a.NumericSlotSize {
			// Packed mode.
			if isFloat {
				s.getName = fmt.Sprintf("math.Float%[1]dfrombits(uint%[1]d(($1.nums[%[2]d] >>  %[3]s_Slot_%[4]s_BitOffset) &  %[3]s_Slot_%[4]s_ValueMask))", floatSz, slotNum, sName, name)
				s.setName = fmt.Sprintf("$1.nums[%[1]d] = ($1.nums[%[1]d] &^ (%[2]s_Slot_%[3]s_ValueMask << %[2]s_Slot_%[3]s_BitOffset)) | (numvalslot(math.Float%[4]dbits($3)) << %[2]s_Slot_%[3]s_BitOffset)", slotNum, sName, name, floatSz)
			} else {
				s.getName = fmt.Sprintf("%[1]s(($1.nums[%[2]d] >> %[3]s_Slot_%[4]s_BitOffset) & %[3]s_Slot_%[4]s_ValueMask)", typ, slotNum, sName, name)
				s.setName = fmt.Sprintf("$1.nums[%[1]d] = ($1.nums[%[1]d] &^ (%[2]s_Slot_%[3]s_ValueMask << %[2]s_Slot_%[3]s_BitOffset)) | (numvalslot($3) << %[2]s_Slot_%[3]s_BitOffset)",
					slotNum, sName, name)
			}
		} else {
			// Unpacked mode.
			if isFloat {
				s.getName = fmt.Sprintf("math.Float%[1]dfrombits(uint%[1]d($1.nums[%[2]d]))", floatSz, slotNum)
				s.setName = fmt.Sprintf("$1.nums[%[1]d] = numvalslot(math.Float%[2]dbits($3))", slotNum, floatSz)
			} else {
				s.getName = fmt.Sprintf("%[1]s($1.nums[%[2]d])", typ, slotNum)
				s.setName = fmt.Sprintf("$1.nums[%[1]d] = numvalslot($3)", slotNum)
			}
		}
	}
	return s
}

func (a *slotAllocator) finish() (getExtraRefs string) {
	if a.numExtraRefs == 0 {
		return "nil"
	}
	extra := a.structNode.NewChild("extraField")
	extra.AddReplacement(paramName, "refs")
	extra.AddReplacementf(paramType, "[%d]*node", a.numExtraRefs)
	return "$1.refs[:]"
}
