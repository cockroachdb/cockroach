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
	"bytes"
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/lang"
)

// factoryGen generates implementation code for the opt.Factory interface. See
// the ifactoryGen comment for more details.
type factoryGen struct {
	compiled   *lang.CompiledExpr
	w          *matchWriter
	uniquifier uniquifier
}

func (g *factoryGen) generate(compiled *lang.CompiledExpr, w io.Writer) {
	g.compiled = compiled
	g.w = &matchWriter{writer: w}

	g.w.writeIndent("package xform\n\n")

	g.w.nest("import (\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt\"\n")
	g.w.unnest(")\n\n")

	g.genConstructFuncs()
	g.genDynamicConstructLookup()
}

// genConstructFuncs generates the factory Construct functions for each
// expression type.
func (g *factoryGen) genConstructFuncs() {
	for _, define := range filterEnforcerDefines(g.compiled.Defines) {
		varName := fmt.Sprintf("_%sExpr", unTitle(string(define.Name)))

		format := "// Construct%s constructs an expression for the %s operator.\n"
		g.w.writeIndent(format, define.Name, define.Name)
		generateDefineComments(g.w.writer, define, string(define.Name))
		g.w.writeIndent("func (_f *factory) Construct%s(\n", define.Name)

		for _, field := range define.Fields {
			fieldName := unTitle(string(field.Name))
			g.w.writeIndent("  %s opt.%s,\n", fieldName, mapType(string(field.Type)))
		}

		g.w.nest(") opt.GroupID {\n")

		g.w.writeIndent("%s := make%sExpr(", varName, define.Name)

		for i, field := range define.Fields {
			if i != 0 {
				g.w.write(", ")
			}
			g.w.write("%s", unTitle(string(field.Name)))
		}

		g.w.write(")\n")
		g.w.writeIndent("_group := _f.mem.lookupGroupByFingerprint(%s.fingerprint())\n", varName)
		g.w.nest("if _group != 0 {\n")
		g.w.writeIndent("return _group\n")
		g.w.unnest("}\n\n")

		g.w.nest("if !_f.allowOptimizations() {\n")
		g.w.writeIndent("return _f.mem.memoizeNormExpr((*memoExpr)(&%s))\n", varName)
		g.w.unnest("}\n\n")

		rules := g.compiled.LookupMatchingRules(string(define.Name))
		if len(rules) > 0 {
			for _, rule := range rules {
				g.genRule(rule)
			}

			g.w.newline()
		}

		g.w.writeIndent("return _f.onConstruct(_f.mem.memoizeNormExpr((*memoExpr)(&%s)))\n", varName)
		g.w.unnest("}\n\n")
	}
}

// genRule generates match and replace code for one rule within the scope of
// a particular op construction method.
func (g *factoryGen) genRule(rule *lang.RuleExpr) {
	matchName := string(rule.Match.Names[0])
	define := g.compiled.LookupDefine(matchName)
	varName := fmt.Sprintf("_%sExpr", unTitle(string(define.Name)))

	g.uniquifier.init()
	g.w.writeIndent("// [%s]\n", rule.Name)
	marker := g.w.nest("{\n")

	for index, matchArg := range rule.Match.Args {
		fieldName := unTitle(string(define.Fields[index].Name))
		g.genMatch(matchArg, fieldName, false /* noMatch */)
	}

	g.w.writeIndent("_f.reportOptimization()\n")
	g.w.writeIndent("_group = ")
	g.genNestedExpr(rule.Replace)
	g.w.newline()
	g.w.writeIndent("_f.mem.addAltFingerprint(%s.fingerprint(), _group)\n", varName)
	g.w.writeIndent("return _group\n")

	g.w.unnestToMarker(marker, "}\n")
	g.w.newline()
}

// genMatch recursively generates matching code for a rule. The given match
// expression can be anything supported within a match by the Optgen language.
// The code generation strategy is fairly simple: matchers are generated as a
// series of nested "if" statements. Within the final "if" statement, the
// replacement expression is created. Because matchers are often nested within
// one another, the matchWriter helper class allows the if nesting to be
// independent of recursive genMatch calls.
//
// The contextName parameter is the name of the Go variable or the Go field
// expression that is bound to the expression that is currently being matched
// against. For example:
//
//   for i, _listArg := range _f.mem.lookupList(projections) {
//     _innerJoinExpr := _f.mem.lookupNormExpr(_listArg).asInnerJoin()
//     if _innerJoinExpr != nil {
//       _selectExpr := _f.mem.lookupNormExpr(_innerJoinExpr.left()).asSelect()
//       ...
//     }
//   }
//
// In that example, the contextName starts out as "projections", which is the
// name of one of the top-level op fields that's being list matched. Then, the
// contextName recursively becomes _listArg, which is bound to one of the
// expressions in the list. And finally, it becomes _innerJoinExpr.left(),
// which returns the left operand of the inner join op, and which is matched
// against in the innermost if statement.
//
// Matchers test whether the context expression "matches" according to the
// semantics of that matcher. For example, the child matcher will generate code
// that tests whether the expression's opname and its children match the
// pattern. The list matcher will generate code that tests whether a list
// expression contains an item that matches the list item matcher.
//
// If true, the noMatch flag inverts the matching logic. The matcher will now
// generate code that tests whether the context expression *doesn't* match
// according to the semantics of the matcher. Some matchers do not currently
// support generating code when noMatch is true.
func (g *factoryGen) genMatch(match lang.Expr, contextName string, noMatch bool) {
	switch t := match.(type) {
	case *lang.MatchExpr:
		g.genMatchNameAndChildren(t, contextName, noMatch)

	case *lang.CustomFuncExpr:
		g.genMatchCustom(t, noMatch)

	case *lang.MatchAndExpr:
		if noMatch {
			panic("noMatch is not yet supported by the and match op")
		}

		g.genMatch(t.Left, contextName, noMatch)
		g.genMatch(t.Right, contextName, noMatch)

	case *lang.MatchNotExpr:
		// Flip the noMatch flag so that the input expression will test for the
		// inverse. No code needs to be generated here because each matcher is
		// responsible for handling the noMatch flag (or not).
		g.genMatch(t.Input, contextName, !noMatch)

	case *lang.BindExpr:
		if string(t.Label) != contextName {
			g.w.writeIndent("%s := %s\n", t.Label, contextName)
		}

		g.genMatch(t.Target, contextName, noMatch)

	case *lang.StringExpr:
		if noMatch {
			g.w.nest("if %s != m.mem.internPrivate(%s) {\n", contextName, t)
		} else {
			g.w.nest("if %s == m.mem.internPrivate(%s) {\n", contextName, t)
		}

	case *lang.MatchAnyExpr:
		if noMatch {
			g.w.nest("if false {\n")
		}

	case *lang.MatchListAnyExpr:
		if noMatch {
			panic("noMatch is not yet supported by the list match any op")
		}
		g.w.nest("for _, _item := range _f.mem.lookupList(%s) {\n", contextName)
		g.genMatch(t.MatchItem, "_item", noMatch)

	case *lang.MatchListFirstExpr:
		if noMatch {
			panic("noMatch is not yet supported by the list match first op")
		}
		g.w.nest("if %s.Length > 0 {\n", contextName)
		g.w.writeIndent("_item := _f.mem.lookupList(%s)[0]\n", contextName)
		g.genMatch(t.MatchItem, "_item", noMatch)

	case *lang.MatchListLastExpr:
		if noMatch {
			panic("noMatch is not yet supported by the list match first op")
		}
		g.w.nest("if %s.Length > 0 {\n", contextName)
		g.w.writeIndent("_item := _f.mem.lookupList(%s)[%s.Length-1]\n", contextName, contextName)
		g.genMatch(t.MatchItem, "_item", noMatch)

	case *lang.MatchListSingleExpr:
		if noMatch {
			panic("noMatch is not yet supported by the list match first op")
		}
		g.w.nest("if %s.Length == 1 {\n", contextName)
		g.w.writeIndent("_item := _f.mem.lookupList(%s)[0]\n", contextName)
		g.genMatch(t.MatchItem, "_item", noMatch)

	case *lang.MatchListEmptyExpr:
		if noMatch {
			panic("noMatch is not yet supported by the list match first op")
		}
		g.w.nest("if %s.Length == 0 {\n", contextName)

	default:
		panic(fmt.Sprintf("unrecognized match expression: %v", match))
	}
}

// genMatchNameAndChildren generates code to match the opname and children of
// the context expression.
func (g *factoryGen) genMatchNameAndChildren(
	match *lang.MatchExpr, contextName string, noMatch bool,
) {
	// The name/child matcher can match multiple parts of the context
	// expression, including its name and zero or more of its children. If
	// noMatch is false, then all of these parts must match in order for the
	// whole to match. If noMatch is true, then at least one of the parts must
	// *not* match in order for the whole to match. This is equivalent to
	// negating an AND expression in boolean logic:
	//   NOT(<cond1> AND <cond2>) == NOT(<cond1>) OR NOT(<cond2>)
	//
	// If either of the conditions does not match, then the overall expression
	// matches.
	//
	// When noMatch is false, then the code generator generates a series of
	// if statements, one for each part of the expression that needs to be
	// matched. If execution enters the innermost if statement, then matching
	// succeeded:
	//   if <match-type> {
	//     if <match-child1> {
	//       if <match-child2> {
	//         ...
	//
	// However, if noMatch is true and execution reaches the innermost if
	// statement, then it means that matching failed, and the execution path
	// needs to be inverted. The code generator does this by creating a match
	// flag, and then testing that flag after breaking out of the nested if
	// statements:
	//   _match := false
	//   if <match-type> {
	//     if <match-child1> {
	//       if <match-child2> {
	//         _match = true
	//       }
	//     }
	//   }
	//
	//   if !_match {
	//     ...
	//
	// All of this is only necessary if there are actually multiple parts of
	// the expression to match. If there's just an opname to match and no
	// children (e.g. just matching (Eq) with no child match args), then that
	// can easily be done by flipping == to/from != in one if statement.
	// There's no need to invert execution logic in that case.
	invertExecution := noMatch && len(match.Args) != 0
	if invertExecution {
		g.w.writeIndent("_match := false\n")

		// Since execution is inverted, we now match the opname and children as
		// if noMatch were false. The result of that will be inverted below.
		noMatch = false
	}

	// Mark current nesting level, so that the noMatch case can close the right
	// number of levels.
	marker := g.w.marker()

	// If the match expression matches more than one name, or if it matches a
	// tag name, then more dynamic code must be generated.
	matchName := string(match.Names[0])
	isDynamicMatch := len(match.Names) > 1 || g.compiled.LookupDefine(matchName) == nil

	// Match expression name.
	if isDynamicMatch {
		g.genDynamicMatch(match, match.Names, contextName, noMatch)
	} else {
		g.genConstantMatch(match, matchName, contextName, noMatch)
	}

	if invertExecution {
		g.w.writeIndent("_match = true\n")
		g.w.unnestToMarker(marker, "}\n")
		g.w.newline()
		g.w.nest("if !_match {\n")
	}
}

// genConstantMatch is called when the MatchExpr has only one define name
// to match (i.e. no tags). In this case, the type of the expression to match
// is statically known, and so the generated code can directly manipulate
// strongly-typed expression structs (e.g. selectExpr, innerJoinExpr, etc).
func (g *factoryGen) genConstantMatch(
	match *lang.MatchExpr, opName string, contextName string, noMatch bool,
) {
	varName := g.uniquifier.makeUnique(fmt.Sprintf("_%s", unTitle(opName)))

	// Match expression name.
	g.w.writeIndent("%s := _f.mem.lookupNormExpr(%s).as%s()\n", varName, contextName, opName)

	if noMatch {
		g.w.nest("if %s == nil {\n", varName)
		if len(match.Args) > 0 {
			panic("noMatch=true only supported without args")
		}
		return
	}

	g.w.nest("if %s != nil {\n", varName)

	// Match expression children in the same order as arguments to the match
	// operator. If there are fewer arguments than there are children, then
	// only the first N children need to be matched.
	for index, matchArg := range match.Args {
		fieldName := unTitle(string(g.compiled.LookupDefine(opName).Fields[index].Name))
		g.genMatch(matchArg, fmt.Sprintf("%s.%s()", varName, fieldName), false /* noMatch */)
	}
}

// genDynamicMatch is called when the MatchExpr is matching a tag name, or
// is matching multiple names. It matches expression children using ExprView,
// which can dynamically get children by index without knowing the specific
// type of operator.
func (g *factoryGen) genDynamicMatch(
	match *lang.MatchExpr, names lang.NamesExpr, contextName string, noMatch bool,
) {
	// Match expression name.
	normName := g.uniquifier.makeUnique("_norm")
	g.w.writeIndent("%s := _f.mem.lookupNormExpr(%s)\n", normName, contextName)

	var buf bytes.Buffer
	for i, name := range names {
		if i != 0 {
			buf.WriteString(" || ")
		}

		define := g.compiled.LookupDefine(string(name))
		if define != nil {
			// Match operator name.
			fmt.Fprintf(&buf, "%s.op == opt.%sOp", normName, name)
		} else {
			// Match tag name.
			fmt.Fprintf(&buf, "%s.is%s()", normName, name)
		}
	}

	if noMatch {
		g.w.nest("if !(%s) {\n", buf.String())
		if len(match.Args) > 0 {
			panic("noMatch=true only supported without args")
		}
		return
	}

	g.w.nest("if %s {\n", buf.String())

	if len(match.Args) > 0 {
		// Construct an Expr to use for matching children.
		exprName := g.uniquifier.makeUnique("_e")
		g.w.writeIndent("%s := makeExprView(_f.mem, %s, opt.NormPhysPropsID)\n", exprName, contextName)

		// Match expression children in the same order as arguments to the match
		// operator. If there are fewer arguments than there are children, then
		// only the first N children need to be matched.
		for index, matchArg := range match.Args {
			g.genMatch(matchArg, fmt.Sprintf("%s.ChildGroup(%d)", exprName, index), false /* noMatch */)
		}
	}
}

// genMatchCustom generates code to invoke a custom matching function.
func (g *factoryGen) genMatchCustom(matchCustom *lang.CustomFuncExpr, noMatch bool) {
	if noMatch {
		g.w.write("if !")
	} else {
		g.w.write("if ")
	}

	g.genNestedExpr(matchCustom)

	g.w.nest(" {\n")
}

// genNestedExpr recursively generates an Optgen expression as one large Go
// expression.
func (g *factoryGen) genNestedExpr(e lang.Expr) {
	switch t := e.(type) {
	case *lang.ConstructExpr:
		g.genConstruct(t)

	case *lang.CustomFuncExpr:
		if t.Name == "OpName" {
			// Handle OpName function that couldn't be statically resolved by
			// looking up op name at runtime.
			ref := t.Args[0].(*lang.RefExpr)
			g.w.write("_f.mem.lookupNormExpr(%s).op", ref.Label)
		} else {
			funcName := unTitle(string(t.Name))
			g.w.write("_f.%s(", funcName)
			for index, arg := range t.Args {
				if index != 0 {
					g.w.write(", ")
				}
				g.genNestedExpr(arg)
			}
			g.w.write(")")
		}

	case *lang.RefExpr:
		g.w.write(string(t.Label))

	case *lang.StringExpr:
		// Literal string expressions construct DString datums.
		g.w.write("m.mem.internPrivate(tree.NewDString(%s))", t)

	case *lang.NameExpr:
		// OpName literal expressions construct an op identifier like SelectOp,
		// which can be passed as a function argument.
		g.w.write("opt.%sOp", t)

	default:
		panic(fmt.Sprintf("unhandled expression: %s", e))
	}
}

// genConstruct generates code to invoke an op construction function or a user-
// defined custom function.
func (g *factoryGen) genConstruct(construct *lang.ConstructExpr) {
	switch t := construct.Name.(type) {
	case *lang.NameExpr:
		// Standard op construction function.
		g.w.write("_f.Construct%s(", *t)

	case *lang.CustomFuncExpr:
		// Construct expression based on dynamic type of referenced op.
		ref := t.Args[0].(*lang.RefExpr)
		g.w.write("_f.DynamicConstruct(_f.mem.lookupNormExpr(%s).op, []GroupID{", ref.Label)

	default:
		panic(fmt.Sprintf("unexpected name expression: %s", construct.Name))
	}

	for index, arg := range construct.Args {
		if index != 0 {
			g.w.write(", ")
		}
		g.genNestedExpr(arg)
	}

	if construct.Name.Op() == lang.CustomFuncOp {
		g.w.write("}, 0)")
	} else {
		g.w.write(")")
	}
}

// genDynamicConstructLookup generates a lookup table used by the factory's
// DynamicConstruct method. This method constructs expressions from a dynamic
// type and arguments.
func (g *factoryGen) genDynamicConstructLookup() {
	defines := filterEnforcerDefines(g.compiled.Defines)

	funcType := "func(f *factory, children []opt.GroupID, private opt.PrivateID) opt.GroupID"
	g.w.writeIndent("type dynConstructLookupFunc %s\n", funcType)

	g.w.writeIndent("var dynConstructLookup [%d]dynConstructLookupFunc\n\n", len(defines)+1)

	g.w.nest("func init() {\n")
	g.w.writeIndent("// UnknownOp\n")
	g.w.nest("dynConstructLookup[opt.UnknownOp] = %s {\n", funcType)
	g.w.writeIndent("  panic(\"op type not initialized\")\n")
	g.w.unnest("}\n\n")

	for _, define := range defines {
		g.w.writeIndent("// %sOp\n", define.Name)
		g.w.nest("dynConstructLookup[opt.%sOp] = %s {\n", define.Name, funcType)

		g.w.writeIndent("return f.Construct%s(", define.Name)
		for i, field := range define.Fields {
			if i != 0 {
				g.w.write(", ")
			}

			if isListType(string(field.Type)) {
				if i == 0 {
					g.w.write("f.InternList(children)")
				} else {
					g.w.write("f.InternList(children[%d:])", i)
				}
			} else if isPrivateType(string(field.Type)) {
				g.w.write("private")
			} else {
				g.w.write("children[%d]", i)
			}
		}
		g.w.write(")\n")

		g.w.unnest("}\n\n")
	}

	g.w.unnest("}\n\n")

	args := "op opt.Operator, children []opt.GroupID, private opt.PrivateID"
	g.w.nest("func (f *factory) DynamicConstruct(%s) opt.GroupID {\n", args)
	g.w.writeIndent("return dynConstructLookup[op](f, children, private)\n")
	g.w.unnest("}\n")
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
