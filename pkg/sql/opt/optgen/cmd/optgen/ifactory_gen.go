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

// ifactoryGen generates the Factory interface which is part of the opt
// package. factoryGen generates the implementation of this interface as part
// of the xform package. The interface and implementation are separated so that
// the Factory interface can be used across multiple packages without causing
// cyclical dependencies among them. All the packages can import the shared
// opt package rather than directly relying upon one another.
type ifactoryGen struct {
	compiled *lang.CompiledExpr
	w        io.Writer
}

func (g *ifactoryGen) generate(compiled *lang.CompiledExpr, w io.Writer) {
	g.compiled = compiled
	g.w = w

	fmt.Fprintf(g.w, "package opt\n\n")

	fmt.Fprintf(w, "type Factory interface {\n")

	fmt.Fprintf(g.w, "  // Metadata returns the query-specific metadata, which includes information\n")
	fmt.Fprintf(g.w, "  // about the columns and tables used in this particular query.\n")
	fmt.Fprintf(g.w, "  Metadata() *Metadata\n\n")

	fmt.Fprintf(g.w, "  // InternList adds the given list of group IDs to memo storage and returns\n")
	fmt.Fprintf(g.w, "  // an ID that can be used for later lookup. If the same list was added\n")
	fmt.Fprintf(g.w, "  // previously, this method is a no-op and returns the ID of the previous\n")
	fmt.Fprintf(g.w, "  // value.\n")
	fmt.Fprintf(g.w, "  InternList(items []GroupID) ListID\n\n")

	fmt.Fprintf(g.w, "  // InternPrivate adds the given private value to the memo and returns an ID\n")
	fmt.Fprintf(g.w, "  // that can be used for later lookup. If the same value was added before,\n")
	fmt.Fprintf(g.w, "  // then this method is a no-op and returns the ID of the previous value.\n")
	fmt.Fprintf(g.w, "  InternPrivate(private interface{}) PrivateID\n\n")

	fmt.Fprintf(g.w, "  // DynamicConstruct dynamically constructs an operator with the given type\n")
	fmt.Fprintf(g.w, "  // and operands. It is equivalent to a switch statement that calls the\n")
	fmt.Fprintf(g.w, "  // ConstructXXX method that corresponds to the given operator.\n")
	fmt.Fprintf(g.w, "  DynamicConstruct(op Operator, operands DynamicOperands) GroupID\n\n")

	g.genMethodsByTag("Relational")
	g.genMethodsByTag("Scalar")
	fmt.Fprintf(w, "}\n")
}

func (g *ifactoryGen) genMethodsByTag(tag string) {
	fmt.Fprintf(g.w, "  // ------------------------------------------------------------ \n")
	fmt.Fprintf(g.w, "  // %s Operators\n", tag)
	fmt.Fprintf(g.w, "  // ------------------------------------------------------------ \n\n")

	for _, define := range g.compiled.Defines {
		if !define.Tags.Contains(tag) {
			continue
		}

		format := "  // Construct%s constructs an expression for the %s operator.\n"
		fmt.Fprintf(g.w, format, define.Name, define.Name)
		generateDefineComments(g.w, define, string(define.Name))

		fmt.Fprintf(g.w, "  Construct%s(", define.Name)
		for i, field := range define.Fields {
			if i != 0 {
				fmt.Fprintf(g.w, ", ")
			}
			fmt.Fprintf(g.w, "%s %s", unTitle(string(field.Name)), mapType(string(field.Type)))
		}
		fmt.Fprintf(g.w, ") GroupID\n\n")
	}

	fmt.Fprintf(g.w, "\n")
}
