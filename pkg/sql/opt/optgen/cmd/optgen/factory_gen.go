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

// factoryGen generates implementation code for the factory that supports
// building normalized expression trees.
type factoryGen struct {
	compiled *lang.CompiledExpr
	w        *matchWriter
	ruleGen  ruleGen
}

func (g *factoryGen) generate(compiled *lang.CompiledExpr, w io.Writer) {
	g.compiled = compiled
	g.w = &matchWriter{writer: w}
	g.ruleGen.init(compiled, g.w)

	g.w.writeIndent("package norm\n\n")

	g.w.nestIndent("import (\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt/memo\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/sem/tree\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/sem/types\"\n")
	g.w.unnest(")\n\n")

	g.genInternPrivateFuncs()
	g.genConstructFuncs()
	g.genDynamicConstructLookup()
}

func (g *factoryGen) genInternPrivateFuncs() {
	for _, typ := range getUniquePrivateTypes(g.compiled.Defines) {
		g.w.writeIndent("// Intern%s adds the given value to the memo and returns an ID that\n", typ)
		g.w.writeIndent("// can be used for later lookup. If the same value was added previously, \n")
		g.w.writeIndent("// this method is a no-op and returns the ID of the previous value.\n")
		g.w.nestIndent("func (_f *Factory) Intern%s(val %s) memo.PrivateID {\n", typ, mapPrivateType(typ))
		g.w.writeIndent("return _f.mem.Intern%s(val)", typ)
		g.w.unnest("}\n\n")
	}
}

// genConstructFuncs generates the factory Construct functions for each
// expression type. The code is similar to this:
//
//   // ConstructScan constructs an expression for the Scan operator.
//   func (_f *Factory) ConstructScan(
//     def memo.PrivateID,
//   ) memo.GroupID {
//     _scanExpr := memo.MakeScanExpr(def)
//     _group := _f.mem.GroupByFingerprint(_scanExpr.Fingerprint())
//     if _group != 0 {
//       return _group
//     }
//
//     ... normalization rule code goes here ...
//
//     return _f.onConstruct(_f.mem.MemoizeNormExpr(memo.Expr(_scanExpr)))
//   }
//
func (g *factoryGen) genConstructFuncs() {
	for _, define := range g.compiled.Defines.WithoutTag("Enforcer") {
		varName := fmt.Sprintf("_%sExpr", unTitle(string(define.Name)))

		format := "// Construct%s constructs an expression for the %s operator.\n"
		g.w.writeIndent(format, define.Name, define.Name)
		generateDefineComments(g.w.writer, define, string(define.Name))
		g.w.nestIndent("func (_f *Factory) Construct%s(\n", define.Name)

		for _, field := range define.Fields {
			fieldName := unTitle(string(field.Name))
			g.w.writeIndent("%s memo.%s,\n", fieldName, mapType(string(field.Type)))
		}

		g.w.unnest(") memo.GroupID ")
		g.w.nestIndent("{\n")
		g.w.writeIndent("%s := memo.Make%sExpr(", varName, define.Name)

		for i, field := range define.Fields {
			if i != 0 {
				g.w.write(", ")
			}
			g.w.write("%s", unTitle(string(field.Name)))
		}

		g.w.write(")\n")
		g.w.writeIndent("_group := _f.mem.GroupByFingerprint(%s.Fingerprint())\n", varName)
		g.w.nestIndent("if _group != 0 {\n")
		g.w.writeIndent("return _group\n")
		g.w.unnest("}\n\n")

		// Only include normalization rules for the current define.
		rules := g.compiled.LookupMatchingRules(string(define.Name)).WithTag("Normalize")
		for _, rule := range rules {
			g.ruleGen.genRule(rule)
		}
		if len(rules) > 0 {
			g.w.newline()
		}

		g.w.writeIndent("return _f.onConstruct(_f.mem.MemoizeNormExpr(_f.evalCtx, memo.Expr(%s)))\n", varName)
		g.w.unnest("}\n\n")
	}
}

// genDynamicConstructLookup generates a lookup table used by the factory's
// DynamicConstruct method. The DynamicConstruct method constructs expressions
// from a dynamic type and arguments. The code looks similar to this:
//
//   type dynConstructLookupFunc func(f *Factory, operands DynamicOperands) memo.GroupID
//
//   var dynConstructLookup [opt.NumOperators]dynConstructLookupFunc
//
//   func init() {
//     // ScanOp
//     dynConstructLookup[opt.ScanOp] = func(f *Factory, operands DynamicOperands) memo.GroupID {
//       return f.ConstructScan(memo.PrivateID(operands[0]))
//     }
//
//     // SelectOp
//     dynConstructLookup[opt.SelectOp] = func(f *Factory, operands DynamicOperands) memo.GroupID {
//       return f.ConstructSelect(memo.GroupID(operands[0]), memo.GroupID(operands[1]))
//     }
//
//     ... code for other ops ...
//   }
//
func (g *factoryGen) genDynamicConstructLookup() {
	funcType := "func(f *Factory, operands DynamicOperands) memo.GroupID"
	g.w.writeIndent("type dynConstructLookupFunc %s\n", funcType)

	g.w.writeIndent("var dynConstructLookup [opt.NumOperators]dynConstructLookupFunc\n\n")

	g.w.nestIndent("func init() {\n")
	g.w.writeIndent("// UnknownOp\n")
	g.w.nestIndent("dynConstructLookup[opt.UnknownOp] = %s {\n", funcType)
	g.w.writeIndent("  panic(\"op type not initialized\")\n")
	g.w.unnest("}\n\n")

	for _, define := range g.compiled.Defines.WithoutTag("Enforcer") {
		g.w.writeIndent("// %sOp\n", define.Name)
		g.w.nestIndent("dynConstructLookup[opt.%sOp] = %s {\n", define.Name, funcType)

		g.w.writeIndent("return f.Construct%s(", define.Name)
		for i, field := range define.Fields {
			if i != 0 {
				g.w.write(", ")
			}

			if isListType(string(field.Type)) {
				g.w.write("operands[%d].ListID()", i)
			} else if isPrivateType(string(field.Type)) {
				g.w.write("memo.PrivateID(operands[%d])", i)
			} else {
				g.w.write("memo.GroupID(operands[%d])", i)
			}
		}
		g.w.write(")\n")

		g.w.unnest("}\n\n")
	}

	g.w.unnest("}\n\n")

	args := "op opt.Operator, operands DynamicOperands"
	g.w.nestIndent("func (f *Factory) DynamicConstruct(%s) memo.GroupID {\n", args)
	g.w.writeIndent("return dynConstructLookup[op](f, operands)\n")
	g.w.unnest("}\n")
}
