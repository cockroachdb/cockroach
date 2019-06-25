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
	"io"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/lang"
)

// explorerGen generates code for the explorer, which searches for logically
// equivalent expressions and adds them to the memo.
type explorerGen struct {
	compiled *lang.CompiledExpr
	md       *metadata
	w        *matchWriter
	ruleGen  newRuleGen
}

func (g *explorerGen) generate(compiled *lang.CompiledExpr, w io.Writer) {
	g.compiled = compiled
	g.md = newMetadata(compiled, "xform")
	g.w = &matchWriter{writer: w}
	g.ruleGen.init(compiled, g.md, g.w)

	g.w.writeIndent("package xform\n\n")

	g.w.nestIndent("import (\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt/memo\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/sem/tree\"\n")
	g.w.unnest(")\n\n")

	g.genDispatcher()
	g.genRuleFuncs()
}

// genDispatcher generates a switch statement that calls an exploration method
// for each define statement that has an explore rule defined. The code is
// similar to this:
//
//   func (_e *explorer) exploreGroupMember(
//     state *exploreState,
//     member memo.RelExpr,
//     ordinal int,
//   ) (_fullyExplored bool) {
//     switch t := member.(type) {
//       case *memo.ScanNode:
//         return _e.exploreScan(state, t, ordinal)
//       case *memo.SelectNode:
//         return _e.exploreSelect(state, t, ordinal)
//     }
//
//     // No rules for other operator types.
//     return true
//   }
//
func (g *explorerGen) genDispatcher() {
	g.w.nestIndent("func (_e *explorer) exploreGroupMember(\n")
	g.w.writeIndent("state *exploreState,\n")
	g.w.writeIndent("member memo.RelExpr,\n")
	g.w.writeIndent("ordinal int,\n")
	g.w.unnest(") (_fullyExplored bool)")
	g.w.nest(" {\n")
	g.w.writeIndent("switch t := member.(type) {\n")

	for _, define := range g.compiled.Defines {
		// Only include exploration rules.
		rules := g.compiled.LookupMatchingRules(string(define.Name)).WithTag("Explore")
		if len(rules) > 0 {
			opTyp := g.md.typeOf(define)
			format := "case *%s: return _e.explore%s(state, t, ordinal)\n"
			g.w.writeIndent(format, opTyp.name, define.Name)
		}
	}

	g.w.writeIndent("}\n\n")
	g.w.writeIndent("// No rules for other operator types.\n")
	g.w.writeIndent("return true\n")
	g.w.unnest("}\n\n")
}

// genRuleFuncs generates a method for each operator that has at least one
// explore rule defined. The code is similar to this:
//
//   func (_e *explorer) exploreScan(
//     _rootState *exploreState,
//     _root *memo.ScanNode,
//     _rootOrd int,
//   ) (_fullyExplored bool) {
//     _fullyExplored = true
//
//     ... exploration rule code goes here ...
//
//     return _fullyExplored
//   }
//
func (g *explorerGen) genRuleFuncs() {
	for _, define := range g.compiled.Defines {
		rules := g.compiled.LookupMatchingRules(string(define.Name)).WithTag("Explore")
		if len(rules) == 0 {
			continue
		}

		opTyp := g.md.typeOf(define)

		g.w.nestIndent("func (_e *explorer) explore%s(\n", define.Name)
		g.w.writeIndent("_rootState *exploreState,\n")
		g.w.writeIndent("_root *%s,\n", opTyp.name)
		g.w.writeIndent("_rootOrd int,\n")
		g.w.unnest(") (_fullyExplored bool)")
		g.w.nest(" {\n")
		g.w.writeIndent("_fullyExplored = true\n\n")

		sortRulesByPriority(rules)
		for _, rule := range rules {
			g.ruleGen.genRule(rule)
		}

		g.w.writeIndent("return _fullyExplored\n")
		g.w.unnest("}\n\n")
	}
}
