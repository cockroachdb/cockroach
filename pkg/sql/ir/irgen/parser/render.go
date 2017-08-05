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

import (
	"bytes"
	"fmt"
)

// ToString converts an IR tree to a human-readable representation,
// using Format.
func ToString(ast interface{}) string {
	var buf bytes.Buffer
	Format(&buf, ast)
	return buf.String()
}

var kindNames = map[DefKind]string{
	EnumDef:   `enum`,
	PrimDef:   `prim`,
	StructDef: `struct`,
	SumDef:    `sum`,
}

// Format converts an IR tree to a human-readable representation.
func Format(buf *bytes.Buffer, ast interface{}) {
	switch n := ast.(type) {
	case []Def:
		for i, def := range n {
			if i > 0 {
				buf.WriteByte('\n')
			}
			Format(buf, def)
		}
	case Def:
		buf.WriteString(kindNames[n.Kind])
		buf.WriteByte(' ')
		buf.WriteString(n.Name.Name.String())
		if n.Kind != PrimDef {
			buf.WriteString(" {\n")
			Format(buf, n.Items)
			buf.WriteString("}\n")
		} else {
			buf.WriteString(";\n")
		}
	case []DefItem:
		for _, item := range n {
			buf.WriteByte(' ')
			Format(buf, item)
			buf.WriteByte('\n')
		}
	case DefItem:
		if n.Type.Name != "" {
			buf.WriteString(n.Type.Name.String())
			if n.IsSlice {
				buf.WriteString("[]")
			}
			buf.WriteByte(' ')
		}
		if n.Name.Name != "" {
			buf.WriteString(n.Name.Name.String())
			buf.WriteByte(' ')
		}
		if n.Type.Name != "" || n.Name.Name != "" {
			buf.WriteString("=")
		} else {
			buf.WriteString("reserved")
		}
		fmt.Fprintf(buf, " %d;", n.Tag.Tag)
	default:
		panic(fmt.Sprintf("unknown type: %T", ast))
	}
}
