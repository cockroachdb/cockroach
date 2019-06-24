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
	g.w.writeIndent("\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt/memo\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/sem/tree\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/types\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/errors\"\n")
	g.w.unnest(")\n\n")

	g.genConstructFuncs()
	g.genReplace()
	g.genCopyAndReplaceDefault()
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

// genReplace generates a method on the factory that offers a convenient way
// to replace all or part of an expression tree.
func (g *factoryGen) genReplace() {
	g.w.writeIndent("// Replace enables an expression subtree to be rewritten under the control of\n")
	g.w.writeIndent("// the caller. It passes each child of the given expression to the replace\n")
	g.w.writeIndent("// callback. The caller can continue traversing the expression tree within the\n")
	g.w.writeIndent("// callback by recursively calling Replace. It can also return a replacement\n")
	g.w.writeIndent("// expression; if it does, then Replace will rebuild the operator and its\n")
	g.w.writeIndent("// ancestors via a calls to the corresponding factory Construct methods. Here\n")
	g.w.writeIndent("// is example usage:\n")
	g.w.writeIndent("//\n")
	g.w.writeIndent("//   var replace func(e opt.Expr, replace ReplaceFunc) opt.Expr\n")
	g.w.writeIndent("//   replace = func(e opt.Expr, replace ReplaceFunc) opt.Expr {\n")
	g.w.writeIndent("//     if e.Op() == opt.VariableOp {\n")
	g.w.writeIndent("//       return getReplaceVar(e)\n")
	g.w.writeIndent("//     }\n")
	g.w.writeIndent("//     return e.Replace(e, replace)\n")
	g.w.writeIndent("//   }\n")
	g.w.writeIndent("//   replace(root, replace)\n")
	g.w.writeIndent("//\n")
	g.w.writeIndent("// Here, all variables in the tree are being replaced by some other expression\n")
	g.w.writeIndent("// in a pre-order traversal of the tree. Post-order traversal is trivially\n")
	g.w.writeIndent("// achieved by moving the e.Replace call to the top of the replace function\n")
	g.w.writeIndent("// rather than bottom.\n")
	g.w.nestIndent("func (f *Factory) Replace(e opt.Expr, replace ReplaceFunc) opt.Expr {\n")
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
					g.w.writeIndent("%s, %sChanged := f.replace%s(t.%s, replace)\n",
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
				g.w.nestIndent("if after, changed := f.replace%s(*t, replace); changed {\n",
					opTyp.friendlyName)
				g.w.writeIndent("return &after\n")
				g.w.unnest("}\n")
			}
		}

		g.w.writeIndent("return t\n")
		g.w.unnest("\n")
	}

	g.w.writeIndent("}\n")
	g.w.writeIndent("panic(errors.AssertionFailedf(\"unhandled op %%s\", errors.Safe(e.Op())))\n")
	g.w.unnest("}\n\n")

	for _, define := range g.compiled.Defines.WithTag("List") {
		opTyp := g.md.typeOf(define)
		itemTyp := opTyp.listItemType
		itemDefine := g.compiled.LookupDefine(itemTyp.friendlyName)

		g.w.nestIndent("func (f *Factory) replace%s(list %s, replace ReplaceFunc) (_ %s, changed bool) {\n",
			opTyp.friendlyName, opTyp.name, opTyp.name)

		// This is a list-typed child.
		g.w.writeIndent("var newList []%s\n", itemTyp.name)
		g.w.nestIndent("for i := range list {\n")
		if itemTyp.isGenerated {
			g.w.writeIndent("before := list[i].%s\n", g.md.fieldName(itemDefine.Fields[0]))
			g.w.writeIndent("after := replace(before).(%s)\n", g.md.lookupType(string(itemDefine.Fields[0].Type)).fullName)
		} else {
			g.w.writeIndent("before := list[i]\n")
			g.w.writeIndent("after := replace(before).(%s)\n", itemTyp.fullName)
		}
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

// genCopyAndReplaceDefault generates a method on the factory that performs the
// default traversal and cloning behavior for the factory's CopyAndReplace
// method.
func (g *factoryGen) genCopyAndReplaceDefault() {
	g.w.writeIndent("// CopyAndReplaceDefault performs the default traversal and cloning behavior\n")
	g.w.writeIndent("// for the CopyAndReplace method. It constructs a copy of the given source\n")
	g.w.writeIndent("// operator using children copied (and potentially remapped) by the given replace\n")
	g.w.writeIndent("// function. See comments for CopyAndReplace for more details.\n")
	g.w.nestIndent("func (f *Factory) CopyAndReplaceDefault(src opt.Expr, replace ReplaceFunc) (dst opt.Expr)")
	g.w.nest("{\n")
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
		if define.Tags.Contains("Relational") || len(childFields) != 0 {
			// If the operator has no children, then call Memoize directly, since
			// all normalizations were already applied on the source operator.
			if len(childFields) != 0 {
				g.w.nestIndent("return f.Construct%s(\n", define.Name)
				for _, child := range childFields {
					childTyp := g.md.typeOf(child)
					childName := g.md.fieldName(child)

					if childTyp.isListType() {
						g.w.writeIndent("f.copyAndReplaceDefault%s(t.%s, replace),\n",
							childTyp.friendlyName, childName)
					} else {
						g.w.writeIndent("f.invokeReplace(t.%s, replace).(%s),\n", childName, childTyp.name)
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
	g.w.writeIndent("panic(errors.AssertionFailedf(\"unhandled op %%s\", errors.Safe(src.Op())))\n")
	g.w.unnest("}\n\n")

	for _, define := range g.compiled.Defines.WithTag("List") {
		opTyp := g.md.typeOf(define)
		itemType := opTyp.listItemType
		itemDefine := g.compiled.LookupDefine(itemType.friendlyName)

		g.w.nestIndent("func (f *Factory) copyAndReplaceDefault%s(src %s, replace ReplaceFunc) (dst %s) {\n",
			opTyp.friendlyName, opTyp.name, opTyp.name)

		g.w.writeIndent("dst = make(%s, len(src))\n", opTyp.name)
		g.w.nestIndent("for i := range src {\n")
		if itemType.isGenerated {
			// All list item types generated by Optgen can have at most one input
			// field (always the first field). Any other fields must be privates.
			// And placeholders only need to be assigned for input fields.
			firstFieldName := g.md.fieldName(itemDefine.Fields[0])
			firstFieldType := g.md.lookupType(string(itemDefine.Fields[0].Type)).fullName
			g.w.writeIndent("dst[i].%s = f.invokeReplace(src[i].%s, replace).(%s)\n",
				firstFieldName, firstFieldName, firstFieldType)

			// Now copy additional exported private fields.
			for _, field := range expandFields(g.compiled, itemDefine)[1:] {
				if isExportedField(field) {
					fieldName := g.md.fieldName(field)
					g.w.writeIndent("dst[i].%s = src[i].%s\n", fieldName, fieldName)
				}
			}
		} else {
			g.w.writeIndent("dst[i] = f.invokeReplace(src[i], replace).(%s)\n", itemType.fullName)
		}
		g.w.unnest("}\n")
		g.w.writeIndent("return dst\n")
		g.w.unnest("}\n\n")
	}

	g.w.writeIndent("// invokeReplace wraps the user-provided replace function. See comments for\n")
	g.w.writeIndent("// CopyAndReplace for more details.\n")
	g.w.nestIndent("func (f *Factory) invokeReplace(src opt.Expr, replace ReplaceFunc) (dst opt.Expr)")
	g.w.nest("{\n")
	g.w.nest("if rel, ok := src.(memo.RelExpr); ok {\n")
	g.w.writeIndent("src = rel.FirstExpr()\n")
	g.w.unnest("}\n")
	g.w.nest("return replace(src)\n")
	g.w.unnest("}\n\n")
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
	g.w.writeIndent("panic(errors.AssertionFailedf(\"cannot dynamically construct operator %%s\", errors.Safe(op)))\n")
	g.w.unnest("}\n")
}
