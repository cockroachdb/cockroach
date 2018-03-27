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

// explorerGen generates code for the explorer, which searches for logically
// equivalent expressions and adds them to the memo.
type explorerGen struct {
	compiled *lang.CompiledExpr
	w        *matchWriter
	ruleGen  ruleGen
}

func (g *explorerGen) generate(compiled *lang.CompiledExpr, w io.Writer) {
	g.compiled = compiled
	g.w = &matchWriter{writer: w}
	g.ruleGen.init(compiled, g.w)

	g.w.writeIndent("package xform\n\n")

	g.w.nestIndent("import (\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt/memo\"\n")
	g.w.unnest(")\n\n")

	g.genDispatcher()
	g.genRuleFuncs()
}

// genDispatcher generates a switch statement that calls an exploration method
// for each define statement that has an explore rule defined. The code is
// similar to this:
//
//   func (_e *explorer) exploreExpr(
//     _state *exploreState, _eid memo.ExprID,
//   ) (_fullyExplored bool) {
//     _expr := _e.mem.Expr(_eid)
//     switch _expr.Operator() {
//       case opt.ScanOp: return _e.exploreScan(_state, _eid, _expr.AsScan())
//       case opt.SelectOp: return _e.exploreSelect(_state, _eid, _expr.AsSelect())
//     }
//
//     // No rules for other operator types.
//     return true
//   }
//
func (g *explorerGen) genDispatcher() {
	g.w.nestIndent("func (_e *explorer) exploreExpr(\n")
	g.w.writeIndent("_state *exploreState, _eid memo.ExprID,\n")
	g.w.unnest(") (_fullyExplored bool)")
	g.w.nestIndent(" {\n")
	g.w.writeIndent("_expr := _e.mem.Expr(_eid)\n")
	g.w.writeIndent("switch _expr.Operator() {\n")

	for _, define := range g.compiled.Defines {
		// Only include exploration rules.
		rules := g.compiled.LookupMatchingRules(string(define.Name)).WithTag("Explore")
		if len(rules) > 0 {
			format := "case opt.%sOp: return _e.explore%s(_state, _eid, _expr.As%s())\n"
			g.w.writeIndent(format, define.Name, define.Name, define.Name)
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
//     _state *exploreState, _scan memo.GroupID, _scanExpr memo.ScanExpr,
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

		groupVar := fmt.Sprintf("_%s", unTitle(string(define.Name)))
		exprVar := fmt.Sprintf("_%sExpr", unTitle(string(define.Name)))

		g.w.nestIndent("func (_e *explorer) explore%s(\n", define.Name)
		format := "_state *exploreState, %s memo.GroupID, %s memo.%sExpr,\n"
		g.w.writeIndent(format, groupVar, exprVar, define.Name)
		g.w.unnest(") (_fullyExplored bool) ")
		g.w.nest("{\n")
		g.w.writeIndent("_fullyExplored = true\n\n")

		for _, rule := range rules {
			g.ruleGen.genRule(rule)
		}

		g.w.writeIndent("return _fullyExplored\n")
		g.w.unnest("}\n\n")
	}
}
