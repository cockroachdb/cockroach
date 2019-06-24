// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	fmt.Fprintf(g.w, "  // startExploreRule tracks the number of normalization rules;\n")
	fmt.Fprintf(g.w, "  // all rules greater than this value are exploration rules.\n")
	fmt.Fprintf(g.w, "  startExploreRule\n\n")
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
