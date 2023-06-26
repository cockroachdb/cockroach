// Copyright 2023 The Cockroach Authors.
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

// fastPathGen generates code for the optimizer fast paths, which match
// "trivial" queries and directly produce the final plan without performing the
// rest of the optimization process.
type fastPathGen struct {
	compiled *lang.CompiledExpr
	md       *metadata
	w        *matchWriter
	ruleGen  newRuleGen
}

func (g *fastPathGen) generate(compiled *lang.CompiledExpr, w io.Writer) {
	g.compiled = compiled
	g.md = newMetadata(compiled, "fastpath")
	g.w = &matchWriter{writer: w}
	g.ruleGen.init(compiled, g.md, g.w)

	g.w.writeIndent("package fastpath\n\n")

	g.w.nestIndent("import (\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt/memo\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical\"\n")
	g.w.unnest(")\n\n")

	g.genDispatcher()
	g.genRuleFuncs()
}

func (g *fastPathGen) genDispatcher() {
	g.w.writeIndent("// tryFastPath should not be used directly; use TryOptimizerFastPath instead.\n")
	g.w.nestIndent("func (_fp *fastPathExplorer) tryFastPath() (_rel memo.RelExpr, _ok bool)")
	g.w.nest("{\n")
	g.w.writeIndent("_root := _fp.f.Memo().RootExpr().(memo.RelExpr)\n")
	g.w.writeIndent("_required := _fp.f.Memo().RootProps()\n")
	g.w.writeIndent("switch t := _root.(type) {\n")

	for _, define := range g.compiled.Defines {
		// Only include fast-path rules.
		rules := g.compiled.LookupMatchingRules(string(define.Name)).WithTag("FastPath")
		if len(rules) > 0 {
			opTyp := g.md.typeOf(define)
			format := "case *%s: return _fp.fastPath%s(t, _required)\n"
			g.w.writeIndent(format, opTyp.name, define.Name)
		}
	}

	g.w.writeIndent("}\n\n")
	g.w.writeIndent("// No rules for other operator types.\n")
	g.w.writeIndent("return nil, false")
	g.w.unnest("}\n\n")
}

func (g *fastPathGen) genRuleFuncs() {
	for _, define := range g.compiled.Defines {
		rules := g.compiled.LookupMatchingRules(string(define.Name)).WithTag("FastPath")
		if len(rules) == 0 {
			continue
		}

		opTyp := g.md.typeOf(define)

		g.w.nestIndent("func (_fp *fastPathExplorer) fastPath%s(\n", define.Name)
		g.w.writeIndent("_root *%s,\n", opTyp.name)
		g.w.writeIndent("_required *physical.Required,\n")
		g.w.unnest(") (_rel memo.RelExpr, _ok bool)")
		g.w.nest(" {\n")
		g.w.writeIndent("opt.MaybeInjectOptimizerTestingPanic(_fp.ctx, _fp.evalCtx)\n")

		sortRulesByPriority(rules)
		for _, rule := range rules {
			g.ruleGen.genRule(rule)
		}

		g.w.writeIndent("return nil, false\n")
		g.w.unnest("}\n\n")
	}
}
