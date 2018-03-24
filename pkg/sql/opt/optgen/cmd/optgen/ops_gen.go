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
	"bytes"
	"fmt"
	"io"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/lang"
)

// opsGen generates the operator enumeration used by the optimizer.
type opsGen struct {
	compiled *lang.CompiledExpr
	w        io.Writer
}

func (g *opsGen) generate(compiled *lang.CompiledExpr, w io.Writer) {
	g.compiled = compiled
	g.w = w

	fmt.Fprintf(g.w, "package opt\n\n")

	g.genOperatorEnum()
	g.genOperatorNames()
	g.genOperatorsByTag()
}

func (g *opsGen) genOperatorEnum() {
	fmt.Fprintf(g.w, "const (\n")
	fmt.Fprintf(g.w, "  UnknownOp Operator = iota\n\n")

	g.genOperatorEnumByTag("Enforcer")
	g.genOperatorEnumByTag("Relational")
	g.genOperatorEnumByTag("Scalar")

	fmt.Fprintf(g.w, "  // NumOperators tracks the total count of operators.\n")
	fmt.Fprintf(g.w, "  NumOperators\n")
	fmt.Fprintf(g.w, ")\n\n")
}

func (g *opsGen) genOperatorNames() {
	var names bytes.Buffer
	var indexes bytes.Buffer

	genByTag := func(tag string) {
		for _, define := range g.compiled.Defines.WithTag(tag) {
			fmt.Fprintf(&indexes, "%d, ", names.Len())

			// Trim the Op suffix and convert to "dash case".
			fmt.Fprint(&names, dashCase(string(define.Name)))
		}
	}

	fmt.Fprint(&names, "unknown")
	fmt.Fprint(&indexes, "0, ")

	genByTag("Enforcer")
	genByTag("Relational")
	genByTag("Scalar")

	fmt.Fprintf(g.w, "const opNames = \"%s\"\n\n", names.String())

	fmt.Fprintf(g.w, "var opIndexes = [...]uint32{%s%d}\n\n", indexes.String(), names.Len())
}

func (g *opsGen) genOperatorEnumByTag(tag string) {
	fmt.Fprintf(g.w, "  // ------------------------------------------------------------ \n")
	fmt.Fprintf(g.w, "  // %s Operators\n", tag)
	fmt.Fprintf(g.w, "  // ------------------------------------------------------------ \n")
	for _, define := range g.compiled.Defines.WithTag(tag) {
		fmt.Fprintf(g.w, "\n")
		generateDefineComments(g.w, define, string(define.Name)+"Op")
		fmt.Fprintf(g.w, "  %sOp\n", define.Name)
	}
	fmt.Fprintf(g.w, "\n")
}

func (g *opsGen) genOperatorsByTag() {
	for _, tag := range g.compiled.DefineTags {
		fmt.Fprintf(g.w, "var %sOperators = [...]Operator{\n", tag)
		for _, define := range g.compiled.Defines.WithTag(tag) {
			fmt.Fprintf(g.w, "  %sOp,\n", define.Name)
		}
		fmt.Fprintf(g.w, "}\n\n")
	}
}

// dashCase converts camel-case identifiers into "dash case", where uppercase
// letters in the middle of the identifier are replaced by a dash followed
// by the lowercase version the letter. Example:
//   InnerJoinApply => inner-join-apply
func dashCase(s string) string {
	var buf bytes.Buffer

	for i, ch := range s {
		if unicode.IsUpper(ch) {
			if i != 0 {
				buf.WriteByte('-')
			}

			buf.WriteRune(unicode.ToLower(ch))
		} else {
			buf.WriteRune(ch)
		}
	}

	return buf.String()
}
