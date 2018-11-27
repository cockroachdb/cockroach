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
	"io"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/lang"
)

// factoryGen generates implementation code for the factory that supports
// building normalized expression trees.
type factoryGen struct {
	compiled *lang.CompiledExpr
	md       *metadata
	w        *matchWriter
	ruleGen  newRuleGen
}

func (g *factoryGen) generate(compiled *lang.CompiledExpr, w io.Writer) {
	g.compiled = compiled
	g.md = newMetadata(compiled, "norm")
	g.w = &matchWriter{writer: w}
	g.ruleGen.init(compiled, g.md, g.w)

	g.w.writeIndent("package norm\n\n")

	g.w.nestIndent("import (\n")
	g.w.writeIndent("\"fmt\"\n")
	g.w.writeIndent("\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/coltypes\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt/memo\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/sem/tree\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/sem/types\"\n")
	g.w.unnest(")\n\n")

	g.genConstructFuncs()
	g.genReconstruct()
	g.genAssignPlaceholders()
	g.genDynamicConstruct()
}

// genConstructFuncs generates the factory Construct functions for each
// expression type. The code is similar to this:
//
//   // ConstructSelect constructs an expression for the Select operator.
//   func (_f *Factory) ConstructSelect(
//     input memo.RelExpr,
//     filters memo.FiltersExpr,
//   ) memo.RelExpr {
//
//     ... normalization rule code goes here ...
//
//     nd := _f.mem.MemoizeSelect(input, filters)
//     return _f.onConstructRelational(nd)
//   }
//
func (g *factoryGen) genConstructFuncs() {
	defines := g.compiled.Defines.
		WithoutTag("Enforcer").
		WithoutTag("List").
		WithoutTag("ListItem").
		WithoutTag("Private")

	for _, define := range defines {
		// Generate Construct method.
		format := "// Construct%s constructs an expression for the %s operator.\n"
		g.w.writeIndent(format, define.Name, define.Name)
		generateComments(g.w.writer, define.Comments, string(define.Name), string(define.Name))

		g.w.nestIndent("func (_f *Factory) Construct%s(\n", define.Name)

		for _, field := range define.Fields {
			fieldTyp := g.md.typeOf(field)
			fieldName := g.md.fieldName(field)
			g.w.writeIndent("%s %s,\n", unTitle(fieldName), fieldTyp.asParam())
		}

		if define.Tags.Contains("Relational") {
			g.w.unnest(") memo.RelExpr")
		} else {
			g.w.unnest(") opt.ScalarExpr")
		}

		g.w.nest(" {\n")

		// Only include normalization rules for the current define.
		rules := g.compiled.LookupMatchingRules(string(define.Name)).WithTag("Normalize")
		sortRulesByPriority(rules)
		for _, rule := range rules {
			g.ruleGen.genRule(rule)
		}
		if len(rules) > 0 {
			g.w.newline()
		}

		g.w.writeIndent("e := _f.mem.Memoize%s(", define.Name)
		for i, field := range define.Fields {
			if i != 0 {
				g.w.write(", ")
			}
			g.w.write("%s", unTitle(g.md.fieldName(field)))
		}

		g.w.write(")\n")

		if define.Tags.Contains("Relational") {
			g.w.writeIndent("return _f.onConstructRelational(e)\n")
		} else {
			g.w.writeIndent("return _f.onConstructScalar(e)\n")
		}

		g.w.unnest("}\n\n")
	}
}

// genReconstruct generates a method on the factory that offers a convenient way
// to rebuild an expression tree.
func (g *factoryGen) genReconstruct() {
	g.w.writeIndent("// Reconstruct enables an expression subtree to be rewritten under the control\n")
	g.w.writeIndent("// of the caller. It passes each child of the given expression to the replace\n")
	g.w.writeIndent("// callback. The caller can continue traversing the expression tree within the\n")
	g.w.writeIndent("// callback by recursively calling Reconstruct. It can also return a replacement\n")
	g.w.writeIndent("// expression; if it does, then Reconstruct will rebuild the operator via a call\n")
	g.w.writeIndent("// to the corresponding factory Construct method. Here is example usage:\n")
	g.w.writeIndent("//\n")
	g.w.writeIndent("//   var replace func(e opt.Expr, replace ReconstructFunc) opt.Expr\n")
	g.w.writeIndent("//   replace = func(e opt.Expr, replace ReconstructFunc) opt.Expr {\n")
	g.w.writeIndent("//     if e.Op() == opt.VariableOp {\n")
	g.w.writeIndent("//       return ReplaceVar(e)\n")
	g.w.writeIndent("//     }\n")
	g.w.writeIndent("//     return e.Reconstruct(e, replace)\n")
	g.w.writeIndent("//   }\n")
	g.w.writeIndent("//   replace(root, replace)\n")
	g.w.writeIndent("//\n")
	g.w.writeIndent("// Here, all variables in the tree are being replaced by some other expression\n")
	g.w.writeIndent("// in a pre-order traversal of the tree. Post-order traversal is trivially\n")
	g.w.writeIndent("// achieved by moving the e.Reconstruct call to the top of the replace function\n")
	g.w.writeIndent("// rather than bottom.\n")
	g.w.nestIndent("func (f *Factory) Reconstruct(e opt.Expr, replace ReconstructFunc) opt.Expr {\n")
	g.w.writeIndent("switch t := e.(type) {\n")

	defines := g.compiled.Defines.WithoutTag("Enforcer").WithoutTag("ListItem").WithoutTag("Private")
	for _, define := range defines {
		opTyp := g.md.typeOf(define)
		childFields := g.md.childFields(define)
		privateField := g.md.privateField(define)

		g.w.nestIndent("case *%s:\n", opTyp.name)

		if len(childFields) != 0 {
			for _, child := range childFields {
				childName := g.md.fieldName(child)
				childTyp := g.md.typeOf(child)

				if childTyp.isListType() {
					g.w.writeIndent("%s, %sChanged := f.reconstruct%s(t.%s, replace)\n",
						unTitle(childName), unTitle(childName), childTyp.friendlyName, childName)
				} else {
					g.w.writeIndent("%s := replace(t.%s).(%s)\n",
						unTitle(childName), childName, childTyp.name)
				}
			}

			g.w.writeIndent("if ")
			for i, child := range childFields {
				childName := g.md.fieldName(child)
				childTyp := g.md.typeOf(child)

				if i != 0 {
					g.w.write(" || ")
				}
				if childTyp.isListType() {
					g.w.write("%sChanged", unTitle(childName))
				} else {
					g.w.write("%s != t.%s", unTitle(childName), childName)
				}
			}
			g.w.nest(" {\n")

			g.w.writeIndent("return f.Construct%s(", define.Name)
			for i, child := range childFields {
				childName := g.md.fieldName(child)

				if i != 0 {
					g.w.write(", ")
				}
				g.w.write("%s", unTitle(childName))
			}
			if privateField != nil {
				if len(childFields) != 0 {
					g.w.write(", ")
				}
				if g.md.typeOf(privateField).passByVal {
					g.w.write("t.%s", g.md.fieldName(privateField))
				} else {
					g.w.write("&t.%s", g.md.fieldName(privateField))
				}
			}
			g.w.write(")\n")
			g.w.unnest("}\n")
		} else {
			// If this is a list type, then call a list-specific reconstruct method.
			if opTyp.isListType() {
				g.w.nestIndent("if after, changed := f.reconstruct%s(*t, replace); changed {\n",
					opTyp.friendlyName)
				g.w.writeIndent("return &after\n")
				g.w.unnest("}\n")
			}
		}

		g.w.writeIndent("return t\n")
		g.w.unnest("\n")
	}

	g.w.writeIndent("}\n")
	g.w.writeIndent("panic(fmt.Sprintf(\"unhandled op %%s\", e.Op()))\n")
	g.w.unnest("}\n\n")

	for _, define := range g.compiled.Defines.WithTag("List") {
		opTyp := g.md.typeOf(define)
		itemTyp := opTyp.listItemType
		itemDefine := g.compiled.LookupDefine(itemTyp.friendlyName)

		g.w.nestIndent("func (f *Factory) reconstruct%s(list %s, replace ReconstructFunc) (_ %s, changed bool) {\n",
			opTyp.friendlyName, opTyp.name, opTyp.name)

		// This is a list-typed child.
		g.w.writeIndent("var newList []%s\n", itemTyp.name)
		g.w.nestIndent("for i := range list {\n")
		if itemTyp.isGenerated {
			g.w.writeIndent("before := list[i].%s\n", g.md.fieldName(itemDefine.Fields[0]))
		} else {
			g.w.writeIndent("before := list[i]\n")
		}
		g.w.writeIndent("after := replace(before).(opt.ScalarExpr)\n")
		g.w.nestIndent("if before != after {\n")
		g.w.nestIndent("if newList == nil {\n")
		g.w.writeIndent("newList = make([]%s, len(list))\n", itemTyp.name)
		g.w.writeIndent("copy(newList, list[:i])\n")
		g.w.unnest("}\n")
		if itemTyp.isGenerated {
			g.w.writeIndent("newList[i].%s = after\n", g.md.fieldName(itemDefine.Fields[0]))

			// Now copy additional exported private fields.
			for _, field := range expandFields(g.compiled, itemDefine)[1:] {
				if isExportedField(field) {
					fieldName := g.md.fieldName(field)
					g.w.writeIndent("newList[i].%s = list[i].%s\n", fieldName, fieldName)
				}
			}
		} else {
			g.w.writeIndent("newList[i] = after\n")
		}
		g.w.unnest("}")
		g.w.nest(" else if newList != nil {")
		g.w.writeIndent("newList[i] = list[i]\n")
		g.w.unnest("}\n")
		g.w.unnest("}\n")
		g.w.nest("if newList == nil {\n")
		g.w.writeIndent("  return list, false\n")
		g.w.unnest("}\n")
		g.w.writeIndent("return newList, true\n")
		g.w.unnest("}\n\n")
	}
}

// genAssignPlaceholders generates a method to copy an expression tree, but with
// any placeholders replaced by their assigned values.
func (g *factoryGen) genAssignPlaceholders() {
	g.w.nestIndent("func (f *Factory) assignPlaceholders(src opt.Expr) (dst opt.Expr)")
	g.w.nest(" {\n")
	g.w.writeIndent("switch t := src.(type) {\n")

	defines := g.compiled.Defines.
		WithoutTag("Enforcer").
		WithoutTag("List").
		WithoutTag("ListItem").
		WithoutTag("Private")

	for _, define := range defines {
		opTyp := g.md.typeOf(define)
		childFields := g.md.childFields(define)
		privateField := g.md.privateField(define)

		g.w.nestIndent("case *%s:\n", opTyp.name)

		if define.Name == "Placeholder" {
			g.w.writeIndent("d, err := t.Value.Eval(f.evalCtx)\n")
			g.w.nestIndent("if err != nil {\n")
			g.w.writeIndent("panic(placeholderError{err})\n")
			g.w.unnest("}\n")
			g.w.writeIndent("return f.ConstructConstVal(d)\n")
			g.w.unnest("\n")
			continue
		}

		if define.Tags.Contains("Relational") || len(childFields) != 0 {
			if len(childFields) != 0 {
				g.w.nestIndent("return f.Construct%s(\n", define.Name)
				for _, child := range childFields {
					childTyp := g.md.typeOf(child)
					childName := g.md.fieldName(child)

					if childTyp.isListType() {
						g.w.writeIndent("f.assign%sPlaceholders(t.%s),\n",
							childTyp.friendlyName, childName)
					} else {
						g.w.writeIndent("f.assignPlaceholders(t.%s).(%s),\n",
							childName, childTyp.name)
					}
				}
				if privateField != nil {
					fieldName := g.md.fieldName(privateField)
					g.w.writeIndent("%st.%s,\n", g.md.fieldLoadPrefix(privateField), fieldName)
				}
				g.w.unnest(")\n")
			} else {
				g.w.writeIndent("return f.mem.Memoize%s(", define.Name)
				if privateField != nil {
					fieldName := g.md.fieldName(privateField)
					g.w.write("%st.%s", g.md.fieldLoadPrefix(privateField), fieldName)
				}
				g.w.write(")\n")
			}
		} else {
			g.w.writeIndent("return t\n")
		}
		g.w.unnest("\n")
	}

	g.w.writeIndent("}\n")
	g.w.writeIndent("panic(fmt.Sprintf(\"unhandled op %%s\", src.Op()))\n")
	g.w.unnest("}\n\n")

	for _, define := range g.compiled.Defines.WithTag("List") {
		opTyp := g.md.typeOf(define)
		itemType := opTyp.listItemType
		itemDefine := g.compiled.LookupDefine(itemType.friendlyName)

		g.w.nestIndent("func (f *Factory) assign%sPlaceholders(src %s) (dst %s) {\n",
			opTyp.friendlyName, opTyp.name, opTyp.name)

		g.w.writeIndent("dst = make(%s, len(src))\n", opTyp.name)
		g.w.nestIndent("for i := range src {\n")
		if itemType.isGenerated {
			// All list item types generated by Optgen can have at most one input
			// field (always the first field). Any other fields must be privates.
			// And placeholders only need to be assigned for input fields.
			firstFieldName := g.md.fieldName(itemDefine.Fields[0])
			g.w.writeIndent("dst[i].%s = f.assignPlaceholders(src[i].%s).(opt.ScalarExpr)\n",
				firstFieldName, firstFieldName)

			// Now copy additional exported private fields.
			for _, field := range expandFields(g.compiled, itemDefine)[1:] {
				if isExportedField(field) {
					fieldName := g.md.fieldName(field)
					g.w.writeIndent("dst[i].%s = src[i].%s\n", fieldName, fieldName)
				}
			}
		} else {
			g.w.writeIndent("dst[i] = f.assignPlaceholders(src[i]).(opt.ScalarExpr)\n")
		}
		g.w.unnest("}\n")
		g.w.writeIndent("return dst\n")
		g.w.unnest("}\n\n")
	}
}

// genDynamicConstruct generates the factory's DynamicConstruct method, which
// constructs expressions from a dynamic type and arguments. The code looks
// similar to this:
//
//   func (f *Factory) DynamicConstruct(op opt.Operator, args ...interface{}) opt.Node {
//     switch op {
//     case opt.ProjectOp:
//       return f.ConstructProject(
//         args[0].(memo.RelNode),
//         *args[1].(*memo.ProjectionsExpr),
//         *args[2].(*opt.ColSet),
//       )
//
//     ... cases for other ops ...
//   }
//
func (g *factoryGen) genDynamicConstruct() {
	g.w.nestIndent("func (f *Factory) DynamicConstruct(op opt.Operator, args ...interface{}) opt.Expr {\n")
	g.w.writeIndent("switch op {\n")

	defines := g.compiled.Defines.
		WithoutTag("Enforcer").
		WithoutTag("List").
		WithoutTag("ListItem").
		WithoutTag("Private")

	for _, define := range defines {
		g.w.writeIndent("case opt.%sOp:\n", define.Name)
		g.w.nestIndent("return f.Construct%s(\n", define.Name)

		for i, field := range define.Fields {
			fieldTyp := g.md.typeOf(field)
			if fieldTyp.isPointer {
				g.w.writeIndent("args[%d].(%s),\n", i, fieldTyp.name)
			} else if fieldTyp.passByVal {
				g.w.writeIndent("*args[%d].(*%s),\n", i, fieldTyp.name)
			} else {
				g.w.writeIndent("args[%d].(*%s),\n", i, fieldTyp.name)
			}
		}

		g.w.unnest(")\n")
	}

	g.w.writeIndent("}\n")
	g.w.writeIndent("panic(fmt.Sprintf(\"cannot dynamically construct operator %%s\", op))\n")
	g.w.unnest("}\n")
}
