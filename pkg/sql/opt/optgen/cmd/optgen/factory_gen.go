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
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/coltypes\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt/memo\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt/props\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/sem/tree\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/sem/types\"\n")
	g.w.unnest(")\n\n")

	g.genInternPrivateFuncs()
	g.genConstructFuncs()
	g.genAssignPlaceholders()
	g.genDynamicConstruct()
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
//     return _f.onConstruct(memo.Expr(_scanExpr))
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
		sortRulesByPriority(rules)
		for _, rule := range rules {
			g.ruleGen.genRule(rule)
		}
		if len(rules) > 0 {
			g.w.newline()
		}

		g.w.writeIndent("return _f.onConstruct(memo.Expr(%s))\n", varName)
		g.w.unnest("}\n\n")
	}
}

// genAssignPlaceholders generates code that recursively walks a subtree looking
// for Placeholder operators and replacing them with their assigned values. This
// will trigger the rebuild of that node's ancestors, as well as triggering
// additional normalization rules that can substantially rewrite the tree. The
// generated code is similar to this:
//
//   func (f *Factory) assignPlaceholders(group memo.GroupID) memo.GroupID {
//     if !f.mem.GroupProperties(group).HasPlaceholder() {
//       return group
//     }
//     expr := f.mem.NormExpr(group)
//     switch expr.Operator() {
//     case opt.SelectOp:
//       selectExpr := expr.AsSelect()
//       input := f.assignPlaceholders(selectExpr.Input())
//       filter := f.assignPlaceholders(selectExpr.Filter())
//       return f.ConstructSelect(input, filter)
//
func (g *factoryGen) genAssignPlaceholders() {
	g.w.nestIndent("func (f *Factory) assignPlaceholders(group memo.GroupID) (memo.GroupID, error) {\n")
	g.w.nestIndent("if !f.mem.GroupProperties(group).HasPlaceholder() {\n")
	g.w.writeIndent("return group, nil\n")
	g.w.unnest("}\n")
	g.w.writeIndent("expr := f.mem.NormExpr(group)\n")

	g.w.writeIndent("switch expr.Operator() {\n")

	for _, define := range g.compiled.Defines.WithoutTag("Enforcer") {
		// Determine if the operator can be skipped altogether:
		//   1. The operator is an aggregate. Aggregates never contain placeholders
		//      because non-Variable children always get rewritten as part of an
		//      input projection.
		//   2. The operator has no children.
		if define.Tags.Contains("Aggregate") {
			continue
		}

		skipOp := true
		for _, field := range define.Fields {
			if !isPrivateType(string(field.Type)) {
				skipOp = false
				break
			}
		}
		if skipOp {
			continue
		}

		varName := fmt.Sprintf("%sExpr", unTitle(string(define.Name)))

		g.w.nestIndent("case opt.%sOp:\n", define.Name)
		if len(define.Fields) > 0 {
			g.w.writeIndent("%s := expr.As%s()\n", varName, define.Name)
		}

		// Determine whether a list builder is needed.
		for _, field := range define.Fields {
			if isListType(string(field.Type)) {
				g.w.writeIndent("lb := MakeListBuilder(&f.funcs)\n")
				break
			}
		}

		for _, field := range define.Fields {
			fieldVarName := unTitle(string(field.Name))

			if isListType(string(field.Type)) {
				g.w.nestIndent("for _, item := range f.mem.LookupList(%s.%s()) {\n", varName, field.Name)
				g.w.writeIndent("newItem, err := f.assignPlaceholders(item)\n")
				g.w.nestIndent("if err != nil {\n")
				g.w.writeIndent("return 0, err\n")
				g.w.unnest("}\n")
				g.w.writeIndent("lb.AddItem(newItem)\n")
				g.w.unnest("}\n")
				g.w.writeIndent("%s := lb.BuildList()\n", fieldVarName)
			} else if isPrivateType(string(field.Type)) {
				g.w.writeIndent("%s := %s.%s()\n", fieldVarName, varName, field.Name)
			} else {
				g.w.writeIndent("%s, err := f.assignPlaceholders(%s.%s())\n", fieldVarName, varName, field.Name)
				g.w.nestIndent("if err != nil {\n")
				g.w.writeIndent("return 0, err\n")
				g.w.unnest("}\n")
			}
		}

		g.w.writeIndent("return f.Construct%s(", define.Name)
		for i, field := range define.Fields {
			if i != 0 {
				g.w.write(", ")
			}
			g.w.write(unTitle(string(field.Name)))
		}
		g.w.unnest("), nil\n")
	}

	// Generate code that assigns a placeholder value.
	g.w.nestIndent("case opt.PlaceholderOp:\n")
	g.w.writeIndent("value := expr.AsPlaceholder().Value()\n")
	g.w.writeIndent("placeholder := f.mem.LookupPrivate(value).(*tree.Placeholder)\n")
	g.w.writeIndent("d, err := placeholder.Eval(f.evalCtx)\n")
	g.w.nestIndent("if err != nil {\n")
	g.w.writeIndent("return 0, err\n")
	g.w.unnest("}\n")
	g.w.writeIndent("return f.ConstructConstVal(d), nil")

	g.w.unnest("}\n")
	g.w.writeIndent("panic(\"unhandled operator\")\n")
	g.w.unnest("}\n\n")
}

// genDynamicConstruct generates the factory's DynamicConstruct method, which
// constructs expressions from a dynamic type and arguments. The code looks
// similar to this:
//
//   type dynConstructFunc func(f *Factory, operands memo.DynamicOperands) memo.GroupID
//
//   var dynConstructLookup [opt.NumOperators]dynConstructFunc
//
//   func init() {
//     // ScanOp
//     dynConstructLookup[opt.ScanOp] = func(f *Factory, operands memo.DynamicOperands) memo.GroupID {
//       return f.ConstructScan(memo.PrivateID(operands[0]))
//     }
//
//     // SelectOp
//     dynConstructLookup[opt.SelectOp] = func(f *Factory, operands memo.DynamicOperands) memo.GroupID {
//       return f.ConstructSelect(memo.GroupID(operands[0]), memo.GroupID(operands[1]))
//     }
//
//     ... code for other ops ...
//   }
//
func (g *factoryGen) genDynamicConstruct() {
	funcType := "func(f *Factory, operands memo.DynamicOperands) memo.GroupID"
	g.w.writeIndent("type dynConstructFunc %s\n", funcType)

	g.w.writeIndent("var dynConstructLookup [opt.NumOperators]dynConstructFunc\n\n")

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

	args := "op opt.Operator, operands memo.DynamicOperands"
	g.w.nestIndent("func (f *Factory) DynamicConstruct(%s) memo.GroupID {\n", args)
	g.w.writeIndent("return dynConstructLookup[op](f, operands)\n")
	g.w.unnest("}\n")
}
