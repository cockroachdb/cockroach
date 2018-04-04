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

// ruleNamesGen generates an enumeration containing the names of all the rules.
type ruleNamesGen struct {
	compiled *lang.CompiledExpr
	w        io.Writer
	unique   map[lang.StringExpr]struct{}
}

func (g *ruleNamesGen) generate(compiled *lang.CompiledExpr, w io.Writer) {
	g.compiled = compiled
	g.w = w
	g.unique = make(map[lang.StringExpr]struct{})

	fmt.Fprintf(g.w, "package opt\n\n")

	fmt.Fprintf(g.w, "const (\n")
	fmt.Fprintf(g.w, "  startAutoRule RuleName = iota + NumManualRuleNames\n\n")

	g.genRuleNameEnumByTag("Normalize")
	g.genRuleNameEnumByTag("Explore")

	fmt.Fprintf(g.w, "  // NumRuleNames tracks the total count of rule names.\n")
	fmt.Fprintf(g.w, "  NumRuleNames\n")
	fmt.Fprintf(g.w, ")\n\n")
}

func (g *ruleNamesGen) genRuleNameEnumByTag(tag string) {
	fmt.Fprintf(g.w, "  // ------------------------------------------------------------ \n")
	fmt.Fprintf(g.w, "  // %s Rule Names\n", tag)
	fmt.Fprintf(g.w, "  // ------------------------------------------------------------ \n")
	for _, rule := range g.compiled.Rules {
		if !rule.Tags.Contains(tag) {
			continue
		}

		// Only include each unique rule name once, not once per operator.
		if _, ok := g.unique[rule.Name]; ok {
			continue
		}
		g.unique[rule.Name] = struct{}{}

		fmt.Fprintf(g.w, "  %s\n", rule.Name)
	}
	fmt.Fprintf(g.w, "\n")
}
