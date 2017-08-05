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

package analyzer

import (
	"fmt"
	"go/ast"

	"github.com/cockroachdb/cockroach/pkg/sql/ir/irgen/ir"
	"github.com/cockroachdb/cockroach/pkg/sql/ir/irgen/parser"
)

// Analyze performs semantic analysis on parser definitions.
func Analyze(defs []parser.Def) ([]ir.NamedType, error) {
	namedTypes := make([]ir.NamedType, len(defs))
	symTab := make(symbolTable, len(defs))

	// First pass: check for duplicate names and tag numbers.
	for j, d := range defs {
		if d.Kind != parser.PrimDef && !ast.IsExported(d.Name.Name.String()) {
			return nil, &parser.PosError{Pos: d.Name.Pos, Err: fmt.Errorf("name %q is not exported", d.Name.Name)}
		}
		sym, ok := symTab[d.Name.Name]
		if ok {
			return nil, &parser.PosError{Pos: d.Name.Pos,
				Err: fmt.Errorf("type %q was defined previously at %s", d.Name.Name, sym.pos)}
		}
		namedTypes[j].Name = d.Name.Name
		symTab[d.Name.Name] = symbol{pos: d.Name.Pos, kind: d.Kind, typ: &namedTypes[j]}

		switch d.Kind {
		case parser.EnumDef, parser.StructDef:
			itemTab := make(map[parser.ItemName]parser.Pos, len(d.Items))
			for _, i := range d.Items {
				if i.IsReserved() {
					continue
				}
				if !ast.IsExported(i.Name.Name.String()) {
					return nil, &parser.PosError{Pos: i.Name.Pos, Err: fmt.Errorf("name %q is not exported", i.Name.Name)}
				}
				pos, ok := itemTab[i.Name.Name]
				if ok {
					return nil, &parser.PosError{Pos: i.Name.Pos,
						Err: fmt.Errorf("enumeration constant or struct field %q appeared previously in this definition at %s", i.Name.Name, pos)}
				}
				itemTab[i.Name.Name] = i.Name.Pos
			}
		case parser.SumDef:
			itemTab := make(map[parser.TypeName]parser.Pos, len(d.Items))
			for _, i := range d.Items {
				if i.IsReserved() {
					continue
				}
				pos, ok := itemTab[i.Type.Name]
				if ok {
					return nil, &parser.PosError{Pos: i.Type.Pos,
						Err: fmt.Errorf("sum alternative %q appeared previously in this definition at %s", i.Type.Name, pos)}
				}
				itemTab[i.Type.Name] = i.Type.Pos
			}
		}

		tagTab := make(map[parser.Tag]parser.Pos, len(d.Items))
		for _, i := range d.Items {
			pos, ok := tagTab[i.Tag.Tag]
			if ok {
				return nil, &parser.PosError{Pos: i.Tag.Pos,
					Err: fmt.Errorf("tag %d appeared previously in this definition at %s", i.Tag.Tag, pos)}
			}
			tagTab[i.Tag.Tag] = i.Tag.Pos
		}
	}

	// Second pass: resolve names and construct types.
	for j, d := range defs {
		typ, err := makeType(symTab, d)
		if err != nil {
			return nil, err
		}
		namedTypes[j].Type = typ
	}
	return namedTypes, nil
}

type symbolTable map[parser.TypeName]symbol

type symbol struct {
	pos  parser.Pos
	kind parser.DefKind
	typ  *ir.NamedType
}

func (symTab symbolTable) find(typeName parser.TypeNameOccur) (symbol, error) {
	sym, ok := symTab[typeName.Name]
	if !ok {
		return symbol{}, &parser.PosError{Pos: typeName.Pos,
			Err: fmt.Errorf("type %q was not defined", typeName.Name)}
	}
	return sym, nil
}

func makeType(symTab symbolTable, d parser.Def) (ir.Type, error) {
	switch d.Kind {
	case parser.EnumDef:
		typ := &ir.EnumType{Items: make([]ir.EnumItem, 0, len(d.Items))}
		for _, i := range d.Items {
			if !i.IsReserved() {
				typ.Items = append(typ.Items, ir.EnumItem{Name: i.Name.Name, Tag: i.Tag.Tag})
			}
		}
		return typ, nil
	case parser.PrimDef:
		return nil, nil
	case parser.StructDef:
		typ := &ir.StructType{Items: make([]ir.StructItem, 0, len(d.Items))}
		for _, i := range d.Items {
			if i.IsReserved() {
				continue
			}
			sym, err := symTab.find(i.Type)
			if err != nil {
				return nil, err
			}
			typ.Items = append(typ.Items, ir.StructItem{
				Name:    i.Name.Name,
				IsSlice: i.IsSlice,
				Type:    sym.typ,
				Tag:     i.Tag.Tag,
			})
		}
		return typ, nil
	case parser.SumDef:
		typ := &ir.SumType{Items: make([]ir.SumItem, 0, len(d.Items))}
		for _, i := range d.Items {
			if i.IsReserved() {
				continue
			}
			sym, err := symTab.find(i.Type)
			if err != nil {
				return nil, err
			}
			if sym.kind != parser.StructDef {
				return nil, &parser.PosError{Pos: i.Type.Pos,
					Err: fmt.Errorf("sum alternative %q is not a struct", i.Type.Name)}
			}
			typ.Items = append(typ.Items, ir.SumItem{Type: sym.typ, Tag: i.Tag.Tag})
		}
		return typ, nil
	default:
		panic(fmt.Sprintf("case %d is not handled", d.Kind))
	}
}
