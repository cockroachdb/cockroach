// Copyright 2018 The Cockroach Authors.
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

package main

import (
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/lang"
)

type factoryGen struct {
	compiled *lang.CompiledExpr
	w        *matchWriter
}

func (g *factoryGen) generate(compiled *lang.CompiledExpr, w io.Writer) {
	g.compiled = compiled
	g.w = &matchWriter{writer: w}

	g.w.writeIndent("package xform\n\n")

	g.w.nest("import (\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt/opt\"\n")
	g.w.unnest(1, ")\n\n")

	g.genConstructFuncs()
	g.genDynamicConstructLookup()
}

// genConstructFuncs generates the factory Construct functions for each
// expression type.
func (g *factoryGen) genConstructFuncs() {
	for _, define := range filterEnforcerDefines(g.compiled.Defines) {
		varName := fmt.Sprintf("_%sExpr", unTitle(string(define.Name)))

		g.w.writeIndent("func (_f *factory) Construct%s(\n", define.Name)

		for _, field := range define.Fields {
			fieldName := unTitle(string(field.Name))
			g.w.writeIndent("  %s opt.%s,\n", fieldName, mapType(string(field.Type)))
		}

		g.w.nest(") opt.GroupID {\n")

		g.w.writeIndent("%s := make%sExpr(", varName, define.Name)

		for i, field := range define.Fields {
			if i != 0 {
				g.w.write(", ")
			}
			g.w.write("%s", unTitle(string(field.Name)))
		}

		g.w.write(")\n")
		g.w.writeIndent("_group := _f.mem.lookupGroupByFingerprint(%s.fingerprint())\n", varName)
		g.w.nest("if _group != 0 {\n")
		g.w.writeIndent("return _group\n")
		g.w.unnest(1, "}\n\n")

		g.w.writeIndent("return _f.onConstruct(_f.mem.memoizeNormExpr((*memoExpr)(&%s)))\n", varName)
		g.w.unnest(1, "}\n\n")
	}
}

// genDynamicConstructLookup generates a lookup table used by the factory's
// DynamicConstruct method. This method constructs expressions from a dynamic
// type and arguments.
func (g *factoryGen) genDynamicConstructLookup() {
	defines := filterEnforcerDefines(g.compiled.Defines)

	funcType := "func(f *factory, children []opt.GroupID, private opt.PrivateID) opt.GroupID"
	g.w.writeIndent("type dynConstructLookupFunc %s\n", funcType)

	g.w.writeIndent("var dynConstructLookup [%d]dynConstructLookupFunc\n\n", len(defines)+1)

	g.w.nest("func init() {\n")
	g.w.writeIndent("// UnknownOp\n")
	g.w.nest("dynConstructLookup[opt.UnknownOp] = %s {\n", funcType)
	g.w.writeIndent("  panic(\"op type not initialized\")\n")
	g.w.unnest(1, "}\n\n")

	for _, define := range defines {
		g.w.writeIndent("// %sOp\n", define.Name)
		g.w.nest("dynConstructLookup[opt.%sOp] = %s {\n", define.Name, funcType)

		g.w.writeIndent("return f.Construct%s(", define.Name)
		for i, field := range define.Fields {
			if i != 0 {
				g.w.write(", ")
			}

			if isListType(string(field.Type)) {
				if i == 0 {
					g.w.write("f.StoreList(children)")
				} else {
					g.w.write("f.StoreList(children[%d:])", i)
				}
			} else if isPrivateType(string(field.Type)) {
				g.w.write("private")
			} else {
				g.w.write("children[%d]", i)
			}
		}
		g.w.write(")\n")

		g.w.unnest(1, "}\n\n")
	}

	g.w.unnest(1, "}\n\n")

	args := "op opt.Operator, children []opt.GroupID, private opt.PrivateID"
	g.w.nest("func (f *factory) DynamicConstruct(%s) opt.GroupID {\n", args)
	g.w.writeIndent("return dynConstructLookup[op](f, children, private)\n")
	g.w.unnest(1, "}\n")
}
