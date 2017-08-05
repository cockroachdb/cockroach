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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/ir/irgen/ir"
	"github.com/cockroachdb/cockroach/pkg/sql/ir/irgen/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/ir/irgen/template"
)

// Generate generates code for the given types.
func Generate(
	w io.Writer, tmpl *template.Template, numRefs, numEnums int, namedTypeses ...[]ir.NamedType,
) error {
	root := template.NewRoot()
	root.AddReplacementf("'r'", "%d", numRefs)
	root.AddReplacementf("'e'", "%d", numEnums)
	for _, namedTypes := range namedTypeses {
		for _, namedType := range namedTypes {
			switch t := namedType.Type.(type) {
			case nil:
			case *ir.EnumType:
				node := root.NewChild("enum")
				node.AddReplacement("Enum", namedType.Name)
				for _, i := range t.Items {
					child := node.NewChild("item")
					child.AddReplacement(paramName, i.Name)
					child.AddReplacement(paramTag, i.Tag)
				}
			case *ir.StructType:
				node := root.NewChild("struct")
				node.AddReplacement("Struct", namedType.Name)
				a := slotAllocator{numRefs: numRefs, numEnums: numEnums, structNode: node}
				for _, i := range t.Items {
					child := node.NewChild("item")
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
					child.AddReplacement(paramTypeName, strings.Title(i.Type.String()))
					child.AddReplacement(paramTag, i.Tag)
					getName, setName := a.allocateSlots(i)
					child.AddReplacement(macroGetName, getName)
					child.AddReplacement(macroSetName, setName)
				}
				node.AddReplacement(macroGetExtraRefs, a.finish())
			case *ir.SumType:
				node := root.NewChild("sum")
				node.AddReplacement("Sum", namedType.Name)
				for _, i := range t.Items {
					child := node.NewChild("item")
					child.AddReplacement(paramType, i.Type)
					child.AddReplacement(paramTag, i.Tag)
				}
			default:
				panic(fmt.Sprintf("case %T is not handled", t))
			}
		}
	}
	return tmpl.Instantiate(w, root)
}

// The interesting part of code generation is packing structs into nodes.
// Enumerations need one enum slot; references to structs need one reference
// slot; references to sums need an enum slot and a reference slot; everything
// else needs one extra slot. When the pool of in-node enum or reference slots
// is exhausted, we allocate an extra slot instead.

const (
	paramName         = "Name"
	paramType         = "Type"
	paramTypeName     = "TypName"
	paramTag          = "'t'"
	macroGetName      = `macroGetName\(([.\w]+)\)`
	macroSetName      = `macroSetName\(([.\w]+), ([.\w]+), ([.\w]+)\)`
	macroGetExtraRefs = `macroGetExtraRefs\(([.\w]+)\)`
)

type slotAllocator struct {
	numRefs, numExtraRefs, numEnums int
	structNode                      *template.Node
}

func (a *slotAllocator) allocateSlots(i ir.StructItem) (getName, setName string) {
	switch t := i.Type.Type.(type) {
	case nil:
		return a.allocatePrimSlot(i.Name, i.Type.Name)
	case *ir.EnumType:
		return a.allocateEnumSlot(i.Name, i.Type.Name)
	case *ir.StructType:
		getRef, setRef := a.allocateRefSlot()
		return fmt.Sprintf("%s{%s}", i.Type, getRef),
			setRef
	case *ir.SumType:
		getTag, setTag := a.allocateEnumSlot(i.Name, i.Type.Name+"Tag")
		getRef, setRef := a.allocateRefSlot()
		return fmt.Sprintf("%s{%s, %s}", i.Type, getTag, getRef),
			fmt.Sprintf("%s\n\t%s", strings.Replace(setTag, "$3", "$3.tag", -1), setRef)
	default:
		panic(fmt.Sprintf("case %T is not handled", t))
	}
}

func (a *slotAllocator) allocatePrimSlot(
	name parser.ItemName, typ parser.TypeName,
) (getName, setName string) {
	extra := a.structNode.NewChild("extraField")
	extra.AddReplacement(paramName, name)
	extra.AddReplacement(paramType, typ)
	return fmt.Sprintf("$1.extra.(*extraStruct).%s", name),
		fmt.Sprintf("$2.%s = $3", name)
}

func (a *slotAllocator) allocateRefSlot() (getName, setName string) {
	if a.numRefs > 0 {
		a.numRefs--
		j := a.numRefs
		return fmt.Sprintf("$1.refs[%d]", j),
			fmt.Sprintf("$1.refs[%d] = $3.ref", j)
	}
	j := a.numExtraRefs
	a.numExtraRefs++
	return fmt.Sprintf("$1.extra.(*extraStruct).refs[%d]", j),
		fmt.Sprintf("$2.refs[%d] = $3.ref", j)
}

func (a *slotAllocator) allocateEnumSlot(
	name parser.ItemName, typ parser.TypeName,
) (getName, setName string) {
	if a.numEnums > 0 {
		a.numEnums--
		return fmt.Sprintf("%s($1.enums[%d])", typ, a.numEnums),
			fmt.Sprintf("$1.enums[%d] = enum($3)", a.numEnums)
	}
	return a.allocatePrimSlot(name, typ)
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
