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
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt/opt\"\n")
	g.w.unnest(1, ")\n\n")

	g.genConstructFuncs()
	g.genDynamicConstructLookup()
}

// genConstructFuncs generates the factory Construct functions for each
// expression type.
func (g *factoryGen) genConstructFuncs() {
	for _, define := range filterEnforcerDefines(g.compiled.Defines) {
		varName := fmt.Sprintf("_%sExpr", unTitle(string(define.Name)))

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
		g.w.unnest(1, "}\n\n")

		rules := g.compiled.LookupMatchingRules(string(define.Name))
		if len(rules) > 0 {
			g.w.nest("if !_f.allowOptimizations() {\n")
			g.w.writeIndent("return _f.mem.memoizeNormExpr((*memoExpr)(&%s))\n", varName)
			g.w.unnest(1, "}\n\n")
		}

		for _, rule := range rules {
			g.genRule(rule)
		}

		if len(rules) > 0 {
			g.w.write("\n")
		}

		g.w.writeIndent("return _f.onConstruct(_f.mem.memoizeNormExpr((*memoExpr)(&%s)))\n", varName)
		g.w.unnest(1, "}\n\n")
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
	g.w.nest("{\n")

	for index, matchArg := range rule.Match.Args {
		fieldName := unTitle(string(define.Fields[index].Name))
		g.genMatch(matchArg, fieldName, false)
	}

	g.w.writeIndent("_f.reportOptimization()\n")
	g.w.writeIndent("_group = ")
	g.genReplace(rule, rule.Replace)
	g.w.write("\n")
	g.w.writeIndent("_f.mem.addAltFingerprint(%s.fingerprint(), _group)\n", varName)
	g.w.writeIndent("return _group\n")

	g.w.unnest(g.w.nesting-1, "}\n")
	g.w.writeIndent("\n")
}

// genMatch recursively generates matching code for a rule. The given match
// expression can be anything supported within a match by the Optgen language.
// The code generation strategy is fairly simple: matchers are generated as a
// series of nested "if" statements. Within the final "if" statement, the
// replacement expression is created. Because matchers are often not nested
// within one another, the matchWriter helper class allows the if nesting to
// be independent of recursive genMatch calls.
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
// which returns the left argument of the inner join op, and which is matched
// against in the innermost if statement.
//
// The negate flag indicates whether the match operator should test for match
// or for no-match. Some matchers do not currently support the no-match case.
func (g *factoryGen) genMatch(match lang.Expr, contextName string, negate bool) {
	if match, ok := match.(*lang.MatchExpr); ok {
		g.genMatchArgs(match, contextName, negate)
		return
	}

	if matchInvoke, ok := match.(*lang.MatchInvokeExpr); ok {
		g.genMatchInvoke(matchInvoke, negate)
		return
	}

	if matchAnd, ok := match.(*lang.MatchAndExpr); ok {
		if negate {
			panic("negate is not yet supported by the and match op")
		}

		g.genMatch(matchAnd.Left, contextName, negate)
		g.genMatch(matchAnd.Right, contextName, negate)
		return
	}

	if not, ok := match.(*lang.MatchNotExpr); ok {
		g.genMatch(not.Input, contextName, !negate)
		return
	}

	if bind, ok := match.(*lang.BindExpr); ok {
		if string(bind.Label) != contextName {
			g.w.writeIndent("%s := %s\n", bind.Label, contextName)
		}

		g.genMatch(bind.Target, contextName, negate)
		return
	}

	if str, ok := match.(*lang.StringExpr); ok {
		if negate {
			g.w.nest("if %s != m.mem.internPrivate(%s) {\n", contextName, str)
		} else {
			g.w.nest("if %s == m.mem.internPrivate(%s) {\n", contextName, str)
		}
		return
	}

	if _, ok := match.(*lang.MatchAnyExpr); ok {
		if negate {
			g.w.nest("if false {\n")
		}
		return
	}

	if matchList, ok := match.(*lang.MatchListExpr); ok {
		if negate {
			panic("negate is not yet supported by the list match op")
		}

		g.w.nest("for _, _item := range _f.mem.lookupList(%s) {\n", contextName)
		g.genMatch(matchList.MatchItem, "_item", negate)
		return
	}

	panic(fmt.Sprintf("unrecognized match expression: %v", match))
}

// genMatchArgs generates code to match the arguments of an operator. Each
// argument opens up a new nesting level which will be closed by a calling
// function somewhere up the recursive chain depending on the boolean logic.
func (g *factoryGen) genMatchArgs(match *lang.MatchExpr, contextName string, negate bool) {
	if negate && len(match.Args) != 0 {
		g.w.writeIndent("_match := false\n")
	}

	// Save current nesting level, so that negation case can close the right
	// number of levels.
	nesting := g.w.nesting

	// If the match expression matches more than one name, or if it matches a
	// tag name, then more dynamic code must be generated.
	matchName := string(match.Names[0])
	isDynamicMatch := len(match.Names) > 1 || g.compiled.LookupDefine(matchName) == nil
	if isDynamicMatch {
		g.genDynamicMatchArgs(match, match.Names, contextName, negate)
	} else {
		g.genConstantMatchArgs(match, matchName, contextName, negate)
	}

	if negate && len(match.Args) != 0 {
		g.w.writeIndent("_match = true\n")
		g.w.unnest(g.w.nesting-nesting, "}\n")
		g.w.writeIndent("\n")
		g.w.nest("if !_match {\n")
	}
}

// genConstantMatchArgs is called when the MatchExpr has only one define name
// to match (i.e. no tags). In this case, the type of the expression to match
// is statically known, and so the generated code can directly manipulate
// strongly-typed expression structs (e.g. selectExpr, innerJoinExpr, etc).
func (g *factoryGen) genConstantMatchArgs(
	match *lang.MatchExpr, opName string, contextName string, negate bool,
) {
	varName := g.uniquifier.makeUnique(fmt.Sprintf("_%s", unTitle(opName)))

	g.w.writeIndent("%s := _f.mem.lookupNormExpr(%s).as%s()\n", varName, contextName, opName)

	if negate && len(match.Args) == 0 {
		g.w.nest("if %s == nil {\n", varName)
	} else {
		g.w.nest("if %s != nil {\n", varName)
	}

	for index, matchArg := range match.Args {
		fieldName := string(g.compiled.LookupDefine(opName).Fields[index].Name)
		g.genMatch(matchArg, fmt.Sprintf("%s.%s()", varName, fieldName), false)
	}
}

// genDynamicMatchArgs is called when the MatchExpr is matching a tag name, or
// is matching multiple names. It matches its arguments using ExprView, which
// can dynamically get children by index without knowing the specific type of
// operator.
func (g *factoryGen) genDynamicMatchArgs(
	match *lang.MatchExpr, names lang.OpNamesExpr, contextName string, negate bool,
) {
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
			fmt.Fprintf(&buf, "%s.op == %sOp", normName, name)
		} else {
			// Match tag name.
			fmt.Fprintf(&buf, "is%sLookup[%s.op]", name, normName)
		}
	}

	if negate && len(match.Args) == 0 {
		g.w.nest("if !(%s) {\n", buf.String())
	} else {
		g.w.nest("if %s {\n", buf.String())
	}

	if len(match.Args) > 0 {
		// Construct an Expr to use for matching children.
		exprName := g.uniquifier.makeUnique("_e")
		g.w.writeIndent("%s := makeExprView(_f.mem, %s, defaultPhysPropsID)\n", exprName, contextName)

		for index, matchArg := range match.Args {
			g.genMatch(matchArg, fmt.Sprintf("%s.ChildGroup(%d)", exprName, index), false)
		}
	}
}

// genMatchInvoke generates code to invoke a custom matching function.
func (g *factoryGen) genMatchInvoke(matchInvoke *lang.MatchInvokeExpr, negate bool) {
	funcName := unTitle(string(matchInvoke.FuncName))

	if negate {
		g.w.nest("if !_f.%s(", funcName)
	} else {
		g.w.nest("if _f.%s(", funcName)
	}

	for index, matchArg := range matchInvoke.Args {
		ref := matchArg.(*lang.RefExpr)

		if index != 0 {
			g.w.write(", ")
		}

		g.w.write(string(ref.Label))
	}

	g.w.write(") {\n")
}

// genReplace recursively generates the replacement expression as one large Go
// expression.
func (g *factoryGen) genReplace(rule *lang.RuleExpr, replace lang.Expr) {
	if construct, ok := replace.(*lang.ConstructExpr); ok {
		if strName, ok := construct.Name.(*lang.StringExpr); ok {
			name := string(*strName)
			define := g.compiled.LookupDefine(name)
			if define != nil {
				// Standard op construction function.
				g.w.write("_f.Construct%s(", name)
			} else {
				// Custom function.
				g.w.write("_f.%s(", unTitle(name))
			}
		}

		if opName, ok := construct.Name.(*lang.OpNameExpr); ok {
			g.w.write("_f.Construct%s(", string(*opName))
		}

		if opNameConstruct, ok := construct.Name.(*lang.ConstructExpr); ok {
			// Must be the OpName function.
			ref := opNameConstruct.Args[0].(*lang.RefExpr)
			g.w.write("_f.DynamicConstruct(_f.mem.lookupNormExpr(%s).op, []GroupID{", ref.Label)
		}

		for index, elem := range construct.Args {
			if index != 0 {
				g.w.write(", ")
			}

			g.genReplace(rule, elem)
		}

		if construct.Name.Op() == lang.ConstructOp {
			g.w.write("}, 0)")
		} else {
			g.w.write(")")
		}

		return
	}

	// internList will store the list in the memo and return a ListID that can
	// later retrieve it.
	if constructList, ok := replace.(*lang.ConstructListExpr); ok {
		g.w.write("_f.mem.internList([]GroupID{")

		for index, elem := range constructList.Items {
			if index != 0 {
				g.w.write(", ")
			}

			g.genReplace(rule, elem)
		}

		g.w.write("})")
		return
	}

	if ref, ok := replace.(*lang.RefExpr); ok {
		g.w.write(string(ref.Label))
		return
	}

	// Literal string expressions construct DString datums.
	if str, ok := replace.(*lang.StringExpr); ok {
		g.w.write("m.mem.internPrivate(tree.NewDString(%s))", str)
		return
	}

	// OpName literal expressions construct an op identifier like SelectOp,
	// which can be passed to a custom constructor function.
	if opName, ok := replace.(*lang.OpNameExpr); ok {
		g.w.write(opName.String())
		return
	}

	panic(fmt.Sprintf("unrecognized replace expression: %v", replace))
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
	g.w.unnest(1, "}\n\n")

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

		g.w.unnest(1, "}\n\n")
	}

	g.w.unnest(1, "}\n\n")

	args := "op opt.Operator, children []opt.GroupID, private opt.PrivateID"
	g.w.nest("func (f *factory) DynamicConstruct(%s) opt.GroupID {\n", args)
	g.w.writeIndent("return dynConstructLookup[op](f, children, private)\n")
	g.w.unnest(1, "}\n")
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
