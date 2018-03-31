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

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/lang"
)

// ruleGen generates implementation code for normalization and exploration
// rules. The code is very similar for both kinds of rules, but with some
// important differences. The biggest difference is that exploration rules
// must try to match every expression in a group, while normalization rules
// only match the normalized expression (i.e. 0th expression in group). This
// difference becomes even more pronounced when there are multiple nested
// match expressions, such as in:
//
//   (InnerJoin
//     (Select $input:* $filter:*)
//     $t:*
//     $on:*
//   )
//
// If the inner-join group has 3 expressions and the select group has 2
// expressions, then an exploration rule must consider 6 possible combinations.
// It does this by generating a loop rather than an if statement (as in the
// normalization case), similar to this:
//
//   for _ord := 0; _ord < _state.end; _ord++ {
//     _eid := memo.ExprID{Group: _innerJoin.Left(), Expr: _ord}
//     _selectExpr := _e.mem.Expr(_eid).AsSelect()
//     if _selectExpr != nil {
//
// If the join contained another match pattern, it would be another loop nested
// within that loop. If this was a Normalize rule, then the code would look
// like this instead:
//
//     _selectExpr := _e.mem.NormExpr(left).AsSelect()
//     if _selectExpr != nil {
//
// ruleGen will also do a short-circuiting optimization designed to avoid
// duplicate work for exploration rules. The *exploreState passed to each
// method remembers which expressions were previously explored. Combinations
// where every nested loop is bound to a previously explored expression can be
// skipped. When this optimization is added to the above example, the code would
// instead look like this:
//
//   start := memo.ExprOrdinal(0)
//   if _partlyExplored {
//     start = _state.start
//   }
//   for _ord := start; _ord < _state.end; _ord++ {
//     _eid := memo.ExprID{Group: _innerJoin.Left(), Expr: _ord}
//     _selectExpr := _e.mem.Expr(_eid).AsSelect()
//     if _selectExpr != nil {
//
// If the expression bound to each outer loop has been explored (i.e. if
// _partlyExplored is true), then this logic skips over expressions that have
// already been explored by this inner Select match loop.
//
type ruleGen struct {
	compiled   *lang.CompiledExpr
	w          *matchWriter
	uniquifier uniquifier
	normalize  bool
	exprLookup string
	thisVar    string

	// innerExploreMatch is the innermost match expression in an explore rule.
	// Match expressions in an explore rule generate nested "for" loops, and
	// the innermost loop can skip over previously explored expressions.
	innerExploreMatch *lang.MatchExpr
}

func (g *ruleGen) init(compiled *lang.CompiledExpr, w *matchWriter) {
	g.compiled = compiled
	g.w = w
}

// genRule generates match and replace code for one rule within the scope of
// a particular op construction method.
func (g *ruleGen) genRule(rule *lang.RuleExpr) {
	matchName := string(rule.Match.Names[0])
	define := g.compiled.LookupDefine(matchName)

	// Determine whether the rule is a normalization or exploration rule and
	// set up context accordingly.
	g.normalize = rule.Tags.Contains("Normalize")
	var exprName string
	if g.normalize {
		g.exprLookup = "NormExpr"
		g.thisVar = "_f"
		exprName = fmt.Sprintf("_%sExpr", unTitle(string(define.Name)))
	} else {
		g.exprLookup = "Expr"
		g.thisVar = "_e"
		g.innerExploreMatch = g.findInnerExploreMatch(rule.Match)
		exprName = "_rootExpr"
	}

	g.uniquifier.init()
	g.uniquifier.makeUnique(exprName)

	g.w.writeIndent("// [%s]\n", rule.Name)
	marker := g.w.nestIndent("{\n")

	if g.normalize {
		// For normalization rules, expression fields are passed as parameters
		// to the factory method, so use those parameters directly.
		for index, matchArg := range rule.Match.Args {
			contextName := unTitle(string(define.Fields[index].Name))
			g.genMatch(matchArg, contextName, false /* noMatch */)
		}

		g.genNormalizeReplace(define, rule)
	} else {
		// For exploration rules, the memo expression is passed as a parameter,
		// so use accessors to get its fields.
		if rule.Match == g.innerExploreMatch {
			// The top-level match is the only match in this rule. Skip the
			// expression if it was processed in a previous exploration pass.
			g.w.nestIndent("if _root.Expr >= _rootState.start {\n")
		} else {
			// Initialize _partlyExplored for the top-level match. This variable
			// will be shadowed by each nested loop. Only if all loops are bound
			// to already explored expressions can the innermost match skip.
			g.w.writeIndent("_partlyExplored := _root.Expr < _rootState.start\n")
		}

		for index, matchArg := range rule.Match.Args {
			contextName := fmt.Sprintf("%s.%s()", exprName, define.Fields[index].Name)
			g.genMatch(matchArg, contextName, false /* noMatch */)
		}

		g.genExploreReplace(define, rule)
	}

	g.w.unnestToMarker(marker, "}\n")
	g.w.newline()
}

// genMatch recursively generates matching code for a rule. The given match
// expression can be anything supported within a match by the Optgen language.
// The code generation strategy is fairly simple: matchers are generated as a
// series of nested "if" or "for" statements. Within the final nested statement,
// the replacement expression is created. Because matchers are often nested
// within one another, the matchWriter helper class allows the if nesting to be
// independent of recursive genMatch calls.
//
// The contextName parameter is the name of the Go variable or the Go field
// expression that is bound to the expression that is currently being matched
// against. For example:
//
//   for i, _listArg := range _f.mem.LookupList(projections) {
//     _innerJoinExpr := _f.mem.NormExpr(_listArg).AsInnerJoin()
//     if _innerJoinExpr != nil {
//       _selectExpr := _f.mem.NormExpr(_innerJoinExpr.Left()).AsSelect()
//       ...
//     }
//   }
//
// In that example, the contextName starts out as "projections", which is the
// name of one of the top-level op fields that's being list matched. Then, the
// contextName recursively becomes _listArg, which is bound to one of the
// expressions in the list. And finally, it becomes _innerJoinExpr.Left(),
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
// generate code that tests whether the context expression *does not* match
// according to the semantics of the matcher. Some matchers do not currently
// support generating code when noMatch is true.
func (g *ruleGen) genMatch(match lang.Expr, contextName string, noMatch bool) {
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
			g.w.nestIndent("if %s != m.mem.InternPrivate(%s) {\n", contextName, t)
		} else {
			g.w.nestIndent("if %s == m.mem.InternPrivate(%s) {\n", contextName, t)
		}

	case *lang.MatchAnyExpr:
		if noMatch {
			g.w.nestIndent("if false {\n")
		}

	case *lang.MatchListAnyExpr, *lang.MatchListFirstExpr, *lang.MatchListLastExpr,
		*lang.MatchListSingleExpr, *lang.MatchListEmptyExpr:
		// Handle all list matchers.
		g.genMatchList(match, contextName, noMatch)

	default:
		panic(fmt.Sprintf("unrecognized match expression: %v", match))
	}
}

func (g *ruleGen) genMatchList(match lang.Expr, contextName string, noMatch bool) {
	if !g.normalize {
		panic("list matchers are not yet supported in exploration rules")
	}

	switch t := match.(type) {
	case *lang.MatchListAnyExpr:
		if noMatch {
			panic("noMatch is not yet supported by the list match any op")
		}
		g.w.nestIndent("for _, _item := range %s.mem.LookupList(%s) {\n", g.thisVar, contextName)
		g.genMatch(t.MatchItem, "_item", noMatch)

	case *lang.MatchListFirstExpr:
		if noMatch {
			panic("noMatch is not yet supported by the list match first op")
		}
		g.w.nestIndent("if %s.Length > 0 {\n", contextName)
		g.w.writeIndent("_item := %s.mem.LookupList(%s)[0]\n", g.thisVar, contextName)
		g.genMatch(t.MatchItem, "_item", noMatch)

	case *lang.MatchListLastExpr:
		if noMatch {
			panic("noMatch is not yet supported by the list match last op")
		}
		g.w.nestIndent("if %s.Length > 0 {\n", contextName)
		format := "_item := %s.mem.LookupList(%s)[%s.Length-1]\n"
		g.w.writeIndent(format, g.thisVar, contextName, contextName)
		g.genMatch(t.MatchItem, "_item", noMatch)

	case *lang.MatchListSingleExpr:
		if noMatch {
			if t.MatchItem.Op() != lang.MatchAnyOp {
				panic("noMatch is not yet fully supported by the list match single op")
			}
			g.w.nestIndent("if %s.Length != 1 {\n", contextName)
		} else {
			g.w.nestIndent("if %s.Length == 1 {\n", contextName)
			g.w.writeIndent("_item := %s.mem.LookupList(%s)[0]\n", g.thisVar, contextName)
			g.genMatch(t.MatchItem, "_item", noMatch)
		}

	case *lang.MatchListEmptyExpr:
		if noMatch {
			g.w.nestIndent("if %s.Length != 0 {\n", contextName)
		} else {
			g.w.nestIndent("if %s.Length == 0 {\n", contextName)
		}

	default:
		panic(fmt.Sprintf("unrecognized list match expression: %v", match))
	}
}

// genMatchNameAndChildren generates code to match the opname and children of
// the context expression.
func (g *ruleGen) genMatchNameAndChildren(match *lang.MatchExpr, contextName string, noMatch bool) {
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
		if !g.normalize {
			panic("nomatch with a name/child matcher is not yet supported in explore rules")
		}

		g.w.writeIndent("_match := false\n")

		// Since execution is inverted, we now match the opname and children as
		// if noMatch were false. The result of that will be inverted below.
		noMatch = false
	}

	// Mark current nesting level, so that the noMatch case can close the right
	// number of levels.
	marker := g.w.marker()

	// Generate extra looping code in case of exploration patterns, since any
	// expression in the group can match, rather than just the normalized
	// expression.
	if !g.normalize {
		// If matching only scalar expressions, then don't need to loop over all
		// expressions in the group, since scalar groups have at most one
		// normalized expression.
		if g.onlyScalarMatchNames(match.Names) {
			g.w.writeIndent("_eid := memo.MakeNormExprID(%s)\n", contextName)
		} else {
			// Ensure child group is explored before matching its expressions. If
			// the child group was already explored, it will immediately return.
			g.w.writeIndent("_state := _e.exploreGroup(%s)\n", contextName)
			g.w.nestIndent("if !_state.fullyExplored {\n")
			g.w.writeIndent("_fullyExplored = false\n")
			g.w.unnest("}\n")

			if match == g.innerExploreMatch {
				// This is the innermost match expression, so skip over already
				// explored expressions if all outer loops are also bound to
				// already explored expressions.
				g.w.writeIndent("start := memo.ExprOrdinal(0)\n")
				g.w.nestIndent("if _partlyExplored {\n")
				g.w.writeIndent("start = _state.start\n")
				g.w.unnest("}\n")
				g.w.nestIndent("for _ord := start; _ord < _state.end; _ord++ {\n")
			} else {
				// This match expression still has at least one more nested match
				// statement within it, so update _partlyExplored each time through
				// the loop so that the innermost match knows whether it can skip.
				// Note that _partlyExplored is deliberately shadowed so that each
				// loop has its own copy. If any outer copy is false, then every
				// inner copy should also be false.
				g.w.nestIndent("for _ord := 0; _ord < _state.end; _ord++ {\n")
				g.w.writeIndent("_partlyExplored := _partlyExplored && _ord < _state.start\n")
			}

			g.w.writeIndent("_eid := memo.ExprID{Group: %s, Expr: _ord}\n", contextName)
		}
		contextName = "_eid"
	}

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
		g.w.nestIndent("if !_match {\n")
	}
}

// findInnerExploreMatch walks the match expression and returns the match
// expression that will generate the innermost for loop. See comments on the
// ruleGen struct for more details.
func (g *ruleGen) findInnerExploreMatch(match *lang.MatchExpr) *lang.MatchExpr {
	inner := match

	var visitFunc lang.VisitFunc
	visitFunc = func(e lang.Expr) lang.Expr {
		if m, ok := e.(*lang.MatchExpr); ok {
			if !g.onlyScalarMatchNames(m.Names) {
				inner = m
			}
		}
		return e.Visit(visitFunc)
	}

	match.Visit(visitFunc)
	return inner
}

// onlyScalarMatchNames returns true if all the names are either:
//   1. The name of a define having the Scalar tag.
//   2. Or a tag that is only used by defines having the Scalar tag.
func (g *ruleGen) onlyScalarMatchNames(names lang.NamesExpr) bool {
	for _, name := range names {
		define := g.compiled.LookupDefine(string(name))
		if define != nil {
			if !define.Tags.Contains("Scalar") {
				return false
			}
		} else {
			// Must be a tag name, so make sure that all defines with the tag
			// also have the Scalar tag.
			for _, define := range g.compiled.Defines.WithTag(string(name)) {
				if !define.Tags.Contains("Scalar") {
					return false
				}
			}
		}
	}
	return true
}

// genConstantMatch is called when the MatchExpr has only one define name
// to match (i.e. no tags). In this case, the type of the expression to match
// is statically known, and so the generated code can directly manipulate
// strongly-typed expression structs (e.g. SelectExpr, InnerJoinExpr, etc).
func (g *ruleGen) genConstantMatch(
	match *lang.MatchExpr, opName string, contextName string, noMatch bool,
) {
	exprName := g.uniquifier.makeUnique(fmt.Sprintf("_%sExpr", unTitle(opName)))

	// Match expression name.
	format := "%s := %s.mem.%s(%s).As%s()\n"
	g.w.writeIndent(format, exprName, g.thisVar, g.exprLookup, contextName, opName)

	if noMatch {
		g.w.nestIndent("if %s == nil {\n", exprName)
		if len(match.Args) > 0 {
			panic("noMatch=true only supported without args")
		}
		return
	}

	g.w.nestIndent("if %s != nil {\n", exprName)

	// Match expression children in the same order as arguments to the match
	// operator. If there are fewer arguments than there are children, then
	// only the first N children need to be matched.
	for index, matchArg := range match.Args {
		fieldName := g.compiled.LookupDefine(opName).Fields[index].Name
		g.genMatch(matchArg, fmt.Sprintf("%s.%s()", exprName, fieldName), false /* noMatch */)
	}
}

// genDynamicMatch is called when the MatchExpr is matching a tag name, or
// is matching multiple names. It matches expression children by dynamically
// getting children by index, without knowing the specific type of operator.
func (g *ruleGen) genDynamicMatch(
	match *lang.MatchExpr, names lang.NamesExpr, contextName string, noMatch bool,
) {
	exprName := g.uniquifier.makeUnique("_expr")
	g.w.writeIndent("%s := %s.mem.%s(%s)\n", exprName, g.thisVar, g.exprLookup, contextName)

	var buf bytes.Buffer
	for i, name := range names {
		if i != 0 {
			buf.WriteString(" || ")
		}

		define := g.compiled.LookupDefine(string(name))
		if define != nil {
			// Match operator name.
			fmt.Fprintf(&buf, "%s.Operator() == opt.%sOp", exprName, name)
		} else {
			// Match tag name.
			fmt.Fprintf(&buf, "%s.Is%s()", exprName, name)
		}
	}

	if noMatch {
		g.w.nestIndent("if !(%s) {\n", buf.String())
		if len(match.Args) > 0 {
			panic("noMatch=true only supported without args")
		}
		return
	}

	g.w.nestIndent("if %s {\n", buf.String())

	if len(match.Args) > 0 {
		// Match expression children in the same order as arguments to the match
		// operator. If there are fewer arguments than there are children, then
		// only the first N children need to be matched.
		for index, matchArg := range match.Args {
			childGroup := fmt.Sprintf("%s.ChildGroup(%s.mem, %d)", exprName, g.thisVar, index)
			g.genMatch(matchArg, childGroup, false /* noMatch */)
		}
	}
}

// genMatchCustom generates code to invoke a custom matching function.
func (g *ruleGen) genMatchCustom(matchCustom *lang.CustomFuncExpr, noMatch bool) {
	if noMatch {
		g.w.nestIndent("if !")
	} else {
		g.w.nestIndent("if ")
	}

	g.genNestedExpr(matchCustom)

	g.w.write(" {\n")
}

// genNormalizeReplace generates the replace pattern code for normalization
// rules. Normalization rules recursively call other factory methods in order to
// construct results. They also need to detect rule invocation cycles when the
// DetectCycle tag is present on the rule.
func (g *ruleGen) genNormalizeReplace(define *lang.DefineExpr, rule *lang.RuleExpr) {
	exprName := fmt.Sprintf("_%sExpr", unTitle(string(define.Name)))

	// If the DetectCycle tag is specified on the rule, then generate a bit
	// of additional code that detects cyclical rule invocations. Add the
	// current expression's fingerprint into the ruleCycles map. If the
	// replacement pattern recursively invokes the same rule, then this code
	// will see that the fingerprint is already in the map, and will skip the
	// rule.
	detectCycle := rule.Tags.Contains("DetectCycle")
	if detectCycle {
		// Perform the cycle check before calling onRuleMatch, so that it
		// isn't notified of a rule that will just be skipped.
		g.w.nest("if !_f.ruleCycles[%s.Fingerprint()] {\n", exprName)
	}

	g.w.nestIndent("if _f.onRuleMatch == nil || _f.onRuleMatch(opt.%s) {\n", rule.Name)

	if detectCycle {
		g.w.writeIndent("_f.ruleCycles[%s.Fingerprint()] = true\n", exprName)
	}

	g.w.writeIndent("_group = ")
	g.genNestedExpr(rule.Replace)
	g.w.newline()

	if detectCycle {
		// Once any recursive calls are complete, the fingerprint is no longer
		// needed to prevent cycles, so free up memory.
		g.w.writeIndent("delete(_f.ruleCycles, %s.Fingerprint())\n", exprName)

		// Cyclical rules + onRuleMatch can interact as follows:
		//   1. Rule A recursively invokes itself.
		//   2. The inner A calls AddAltFingerprint with its expression fingerprint
		//      and normalized result group.
		//   3. The outer A would normally resolve to the same normalized result
		//      group, except that onRuleMatch blocks it from completing. Without
		//      a check, it would try to map the same fingerprint to a different
		//      group, causing AddAltFingerprint to panic.
		//
		// Non-cyclical rules do not have this problem, because they short-circuit
		// at the top-level GroupByFingerprint lookup. The solution for cyclical
		// rules is to add an extra call to GroupByFingerprint, in order to see
		// if a nested invocation has already mapped the fingerprint to a group.
		// If so, don't call AddAltFingerprint.
		g.w.nestIndent("if _f.mem.GroupByFingerprint(%s.Fingerprint()) == 0 {\n", exprName)
		g.w.writeIndent("_f.mem.AddAltFingerprint(%s.Fingerprint(), _group)\n", exprName)
		g.w.unnest("}\n")
	} else {
		g.w.writeIndent("_f.mem.AddAltFingerprint(%s.Fingerprint(), _group)\n", exprName)
	}

	g.w.writeIndent("return _group\n")
}

// genExploreReplace generates the replace pattern code for exploration rules.
// Like normalization rules, an exploration rule constructs sub-expressions
// using the factory. However, it constructs the top-level expression using the
// raw MakeXXXExpr method, and passes it to Memo.MemoizeDenormExpr, which adds
// the expression to an existing group.
func (g *ruleGen) genExploreReplace(define *lang.DefineExpr, rule *lang.RuleExpr) {
	g.w.nestIndent("if _e.o.onRuleMatch == nil || _e.o.onRuleMatch(opt.%s) {\n", rule.Name)

	switch t := rule.Replace.(type) {
	case *lang.ConstructExpr:
		name, ok := t.Name.(*lang.NameExpr)
		if !ok {
			panic("exploration pattern with dynamic replace name not yet supported")
		}
		g.w.nestIndent("_expr := memo.Make%sExpr(\n", *name)
		for _, arg := range t.Args {
			g.w.writeIndent("")
			g.genNestedExpr(arg)
			g.w.write(",\n")
		}
		g.w.unnest(")\n")
		g.w.writeIndent("_e.mem.MemoizeDenormExpr(_root.Group, memo.Expr(_expr))\n")

	case *lang.CustomFuncExpr:
		// Top-level custom function returns a memo.Expr slice, so iterate
		// through that and memoize each expression.
		g.w.writeIndent("exprs := ")
		g.genNestedExpr(rule.Replace)
		g.w.newline()
		g.w.nestIndent("for i := range exprs {\n")
		g.w.writeIndent("_e.mem.MemoizeDenormExpr(_root.Group, exprs[i])\n")
		g.w.unnest("}\n")

	default:
		panic(fmt.Sprintf("unsupported replace expression in explore rule: %s", rule.Replace))
	}

	g.w.unnest("}\n")
}

// genNestedExpr recursively generates an Optgen expression as one large Go
// expression.
func (g *ruleGen) genNestedExpr(e lang.Expr) {
	switch t := e.(type) {
	case *lang.ConstructExpr:
		g.genConstruct(t)

	case *lang.ConstructListExpr:
		g.genConstructList(t)

	case *lang.CustomFuncExpr:
		g.genCustomFunc(t)

	case *lang.RefExpr:
		g.w.write(string(t.Label))

	case *lang.StringExpr:
		// Literal string expressions construct DString datums.
		g.w.write("m.mem.InternPrivate(tree.NewDString(%s))", t)

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
func (g *ruleGen) genConstruct(construct *lang.ConstructExpr) {
	var factoryVar string
	if g.normalize {
		factoryVar = "_f"
	} else {
		factoryVar = "_e.f"
	}

	switch t := construct.Name.(type) {
	case *lang.NameExpr:
		// Standard op construction function.
		g.w.nest("%s.Construct%s(\n", factoryVar, *t)
		for _, arg := range construct.Args {
			g.w.writeIndent("")
			g.genNestedExpr(arg)
			g.w.write(",\n")
		}
		g.w.unnest(")")

	case *lang.CustomFuncExpr:
		// Construct expression based on dynamic type of referenced op.
		ref := t.Args[0].(*lang.RefExpr)
		g.w.nest("%s.DynamicConstruct(\n", factoryVar)
		g.w.writeIndent("%s.mem.NormExpr(%s).Operator(),\n", g.thisVar, ref.Label)
		g.w.nestIndent("DynamicOperands{\n")
		for _, arg := range construct.Args {
			g.w.writeIndent("DynamicID(")
			g.genNestedExpr(arg)
			g.w.write("),\n")
		}
		g.w.unnest("},\n")
		g.w.unnest(")")

	default:
		panic(fmt.Sprintf("unexpected name expression: %s", construct.Name))
	}
}

// genCustomFunc generates code to invoke a custom replace function.
func (g *ruleGen) genCustomFunc(customFunc *lang.CustomFuncExpr) {
	if customFunc.Name == "OpName" {
		// Handle OpName function that couldn't be statically resolved by
		// looking up op name at runtime.
		ref := customFunc.Args[0].(*lang.RefExpr)
		g.w.write("%s.mem.%s(%s).Operator()", g.thisVar, g.exprLookup, ref.Label)
	} else {
		funcName := unTitle(string(customFunc.Name))
		g.w.write("%s.%s(", g.thisVar, funcName)
		for index, arg := range customFunc.Args {
			if index != 0 {
				g.w.write(", ")
			}
			g.genNestedExpr(arg)
		}
		g.w.write(")")
	}
}

// genConstructList generates code to construct an interned list of items.
func (g *ruleGen) genConstructList(list *lang.ConstructListExpr) {
	g.w.write("%s.mem.InternList([]memo.GroupID{", g.thisVar)
	for i, item := range list.Items {
		if i != 0 {
			g.w.write(", ")
		}
		g.genNestedExpr(item)
	}
	g.w.write("})")
}
