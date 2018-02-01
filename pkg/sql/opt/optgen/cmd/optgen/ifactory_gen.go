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

	fmt.Fprintf(g.w, "  // StoreList allocates storage for a list of group IDs in the memo and\n")
	fmt.Fprintf(g.w, "  // returns an ID that can be used for later lookup.\n")
	fmt.Fprintf(g.w, "  StoreList(items []GroupID) ListID\n\n")

	fmt.Fprintf(g.w, "  // InternPrivate adds the given private value to the memo and returns an ID\n")
	fmt.Fprintf(g.w, "  // that can be used for later lookup. If the same value was added before,\n")
	fmt.Fprintf(g.w, "  // then this method is a no-op and returns the ID of the previous value.\n")
	fmt.Fprintf(g.w, "  InternPrivate(private interface{}) PrivateID\n\n")

	fmt.Fprintf(g.w, "  // DynamicConstruct dynamically constructs an operator with the given type\n")
	fmt.Fprintf(g.w, "  // and operands. It is equivalent to a switch statement that calls the\n")
	fmt.Fprintf(g.w, "  // ConstructXXX method that corresponds to the given operator.\n")
	fmt.Fprintf(g.w, "  DynamicConstruct(op Operator, children []GroupID, private PrivateID) GroupID\n\n")

	g.genMethodsByTag("Scalar")
	g.genMethodsByTag("Relational")
	fmt.Fprintf(w, "}\n")
}

func (g *ifactoryGen) genMethodsByTag(tag string) {
	fmt.Fprintf(g.w, "  // %s operator constructors.\n", tag)

	for _, define := range g.compiled.Defines {
		if !define.Tags.Contains(tag) {
			continue
		}

		fmt.Fprintf(g.w, "  Construct%s(", define.Name)
		for i, field := range define.Fields {
			if i != 0 {
				fmt.Fprintf(g.w, ", ")
			}
			fmt.Fprintf(g.w, "%s %s", unTitle(string(field.Name)), mapType(string(field.Type)))
		}
		fmt.Fprintf(g.w, ") GroupID\n")
	}

	fmt.Fprintf(g.w, "\n")
}

// filterEnforcerDefines constructs a new define set with any enforcer ops
// removed from the specified set.
func filterEnforcerDefines(defines lang.DefineSetExpr) lang.DefineSetExpr {
	newDefines := make(lang.DefineSetExpr, 0, len(defines))
	for _, define := range defines {
		if define.Tags.Contains("Enforcer") {
			// Don't create factory methods for enforcers, since they're only
			// created by the optimizer.
			continue
		}
		newDefines = append(newDefines, define)
	}
	return newDefines
}
