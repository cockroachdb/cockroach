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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/lang"
)

// exprsGen generates the memo expression structs used by the optimizer, as
// well as lookup tables used to implement the ExprView methods.
type exprsGen struct {
	compiled *lang.CompiledExpr
	w        io.Writer
}

func (g *exprsGen) generate(compiled *lang.CompiledExpr, w io.Writer) {
	g.compiled = compiled
	g.w = w

	fmt.Fprintf(g.w, "package memo\n\n")

	fmt.Fprintf(g.w, "import (\n")
	fmt.Fprintf(g.w, "  \"github.com/cockroachdb/cockroach/pkg/sql/opt\"\n")
	fmt.Fprintf(g.w, "  \"github.com/cockroachdb/cockroach/pkg/sql/sem/tree\"\n")
	fmt.Fprintf(g.w, "  \"github.com/cockroachdb/cockroach/pkg/sql/sem/types\"\n")
	fmt.Fprintf(g.w, ")\n\n")

	g.genLayoutTable()
	g.genTagLookup()
	g.genIsTag()

	// Skip enforcers, since they are not memoized.
	for _, define := range g.compiled.Defines.WithoutTag("Enforcer") {
		g.genExprType(define)
		g.genExprFuncs(define)
	}

	g.genMemoFuncs()
}

// genLayoutTable generates the layout table; see opLayout.
func (g *exprsGen) genLayoutTable() {
	fmt.Fprintf(g.w, "var opLayoutTable = [...]opLayout{\n")
	fmt.Fprintf(g.w, "  opt.UnknownOp: 0xFF, // will cause a crash if used\n")
	for _, define := range g.compiled.Defines {
		var count, listVal, privVal int

		count = len(define.Fields)
		if privateField(define) != nil {
			privVal = count
			count--
		}
		list := listField(define)
		if list != nil {
			listVal = count
			if privVal != 0 {
				// The list takes two slots; adjust the private position.
				privVal++
			}
			count--
		}
		fmt.Fprintf(
			g.w, "  opt.%sOp: makeOpLayout(%d /*base*/, %d /*list*/, %d /*priv*/),\n",
			define.Name, count, listVal, privVal,
		)
	}
	fmt.Fprintf(g.w, "}\n\n")
}

// genTagLookup generates a lookup table used to implement the ExprView IsXXX
// methods for each different define tag. These methods indicate whether the
// expression is associated with that particular tag.
func (g *exprsGen) genTagLookup() {
	for _, tag := range g.compiled.DefineTags {
		fmt.Fprintf(g.w, "var is%sLookup = [...]bool{\n", tag)
		fmt.Fprintf(g.w, "  opt.UnknownOp: false,\n\n")

		for _, define := range g.compiled.Defines {
			fmt.Fprintf(g.w, "  opt.%sOp: %v,\n", define.Name, define.Tags.Contains(tag))
		}

		fmt.Fprintf(g.w, "}\n\n")
	}
}

// genIsTag generates IsXXX tag methods on ExprView and Expr for every unique
// tag.
func (g *exprsGen) genIsTag() {
	for _, tag := range g.compiled.DefineTags {
		fmt.Fprintf(g.w, "func (ev ExprView) Is%s() bool {\n", tag)
		fmt.Fprintf(g.w, "  return is%sLookup[ev.op]\n", tag)
		fmt.Fprintf(g.w, "}\n\n")
	}

	for _, tag := range g.compiled.DefineTags {
		fmt.Fprintf(g.w, "func (e *Expr) Is%s() bool {\n", tag)
		fmt.Fprintf(g.w, "  return is%sLookup[e.op]\n", tag)
		fmt.Fprintf(g.w, "}\n\n")
	}
}

// genExprType generates the type definition for the expression, as well as a
// constructor function.
func (g *exprsGen) genExprType(define *lang.DefineExpr) {
	opType := fmt.Sprintf("%sOp", define.Name)
	exprType := fmt.Sprintf("%sExpr", define.Name)

	// Generate comment for the expression type.
	generateDefineComments(g.w, define, exprType)

	// Generate the expression type.
	fmt.Fprintf(g.w, "type %s Expr\n\n", exprType)

	// Generate a strongly-typed constructor function for the type.
	fmt.Fprintf(g.w, "func Make%s(", exprType)
	for i, field := range define.Fields {
		if i != 0 {
			fmt.Fprint(g.w, ", ")
		}
		fmt.Fprintf(g.w, "%s %s", unTitle(string(field.Name)), mapType(string(field.Type)))
	}
	fmt.Fprintf(g.w, ") %s {\n", exprType)
	fmt.Fprintf(g.w, "  return %s{op: opt.%s, state: exprState{", exprType, opType)

	for i, field := range define.Fields {
		fieldName := unTitle(string(field.Name))

		if i != 0 {
			fmt.Fprintf(g.w, ", ")
		}

		if isListType(string(field.Type)) {
			fmt.Fprintf(g.w, "%s.Offset, %s.Length", fieldName, fieldName)
		} else {
			fmt.Fprintf(g.w, "uint32(%s)", fieldName)
		}
	}

	fmt.Fprint(g.w, "}}\n")
	fmt.Fprint(g.w, "}\n\n")
}

// genExprFuncs generates the expression's accessor functions, one for each
// field in the type.
func (g *exprsGen) genExprFuncs(define *lang.DefineExpr) {
	opType := fmt.Sprintf("%sOp", define.Name)
	exprType := fmt.Sprintf("%sExpr", define.Name)

	// Generate the strongly-typed accessor methods.
	stateIndex := 0
	for _, field := range define.Fields {
		fieldType := mapType(string(field.Type))

		fmt.Fprintf(g.w, "func (e *%s) %s() %s {\n", exprType, field.Name, fieldType)
		if isListType(string(field.Type)) {
			format := "  return ListID{Offset: e.state[%d], Length: e.state[%d]}\n"
			fmt.Fprintf(g.w, format, stateIndex, stateIndex+1)
			stateIndex += 2
		} else if isPrivateType(string(field.Type)) {
			fmt.Fprintf(g.w, "  return PrivateID(e.state[%d])\n", stateIndex)
			stateIndex++
		} else {
			fmt.Fprintf(g.w, "  return GroupID(e.state[%d])\n", stateIndex)
			stateIndex++
		}
		fmt.Fprintf(g.w, "}\n\n")
	}

	// Generate the fingerprint method.
	fmt.Fprintf(g.w, "func (e *%s) Fingerprint() Fingerprint {\n", exprType)
	fmt.Fprintf(g.w, "  return Fingerprint(*e)\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate a conversion method from Expr to the more specialized
	// expression type.
	fmt.Fprintf(g.w, "func (e *Expr) As%s() *%s {\n", define.Name, exprType)
	fmt.Fprintf(g.w, "  if e.op != opt.%s {\n", opType)
	fmt.Fprintf(g.w, "    return nil\n")
	fmt.Fprintf(g.w, "  }\n")

	fmt.Fprintf(g.w, "  return (*%s)(e)\n", exprType)
	fmt.Fprintf(g.w, "}\n\n")
}

// genMemoFuncs generates methods on the memo.
func (g *exprsGen) genMemoFuncs() {
	for _, typ := range getUniquePrivateTypes(g.compiled.Defines) {
		// Remove memo package qualifier from types.
		goType := strings.Replace(mapPrivateType(typ), "memo.", "", -1)

		fmt.Fprintf(g.w, "// Intern%s adds the given value to the memo and returns an ID that\n", typ)
		fmt.Fprintf(g.w, "// can be used for later lookup. If the same value was added previously, \n")
		fmt.Fprintf(g.w, "// this method is a no-op and returns the ID of the previous value.\n")
		fmt.Fprintf(g.w, "func (m *Memo) Intern%s(val %s) PrivateID {\n", typ, goType)
		fmt.Fprintf(g.w, "return m.privateStorage.intern%s(val)", typ)
		fmt.Fprintf(g.w, "}\n\n")
	}
}
