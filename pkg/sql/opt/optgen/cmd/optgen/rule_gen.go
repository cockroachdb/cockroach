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
	"bytes"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/lang"
)

// contextDecl stores the Go code that accesses the expression that is currently
// being matched against. It also contains the type of that expression. See the
// genMatch comment header for an example.
type contextDecl struct {
	code         string
	typ          *typeDef
	untypedAlias string
}

// newRuleGen generates implementation code for normalization and exploration
// rules. The code is very similar for both kinds of rules, but with some
// important differences. The biggest difference is that exploration rules
// must try to match every expression in a group, while normalization rules
// only match the normalized expression (i.e. first expression in group). This
// difference becomes even more pronounced when there are multiple nested
// match expressions, such as in:
//
//   (InnerJoin
//     (Select $input:* $filters:*)
//     $right:*
//     $on:*
//   )
//
// If the inner-join group has 3 expressions and the select group has 2
// expressions, then an exploration rule must consider 6 possible combinations.
// It does this by generating a loop rather than an if statement (as in the
// normalization case), similar to this:
//
//   var _member memo.RelExpr
//   for _ord := 0; _ord < _state.end; _ord++ {
//     if _member == nil {
//       _member = _innerJoin.Left.FirstExpr()
//     } else {
//       _member = _member.NextExpr()
//     }
//     _select, _ := _member.(*SelectExpr)
//     if _select != nil {
//
// If the join contained another match pattern, it would be another loop nested
// within that loop. If this was a Normalize rule, then the code would look
// like this instead:
//
//     _select := _innerJoin.Left.(*SelectExpr)
//     if _select != nil {
//
// ruleGen will also do a short-circuiting optimization designed to avoid
// duplicate work for exploration rules. The *exploreState passed to each
// method remembers which expressions were previously explored. Combinations
// where every nested loop is bound to a previously explored expression can be
// skipped. When this optimization is added to the above example, the code would
// instead look more like this:
//
//   _partlyExplored := _innerJoinOrd < _innerJoinState.start
//   _state := _e.lookupExploreState(_innerJoin.Left)
//   var _member memo.RelExpr
//   for _ord := 0; _ord < _state.end; _ord++ {
//     if _member == nil {
//       _member = _innerJoin.Left.FirstExpr()
//     } else {
//       _member = _member.NextExpr()
//     }
//     if !_partlyExplored || _ord >= _state.start {
//       _select, _ := _member.(*SelectExpr)
//       if _select != nil {
//
// If the inner join expression has already been explored (i.e. if
// _partlyExplored is true), then this logic only explores newly added Left
// children.
//
type newRuleGen struct {
	compiled     *lang.CompiledExpr
	md           *metadata
	w            *matchWriter
	uniquifier   uniquifier
	normalize    bool
	thisVar      string
	factoryVar   string
	boundStmts   map[lang.Expr]string
	typedAliases map[string]string

	// innerExploreMatch is the innermost match expression in an explore rule.
	// Match expressions in an explore rule generate nested "for" loops, and
	// the innermost loop can skip over previously explored expressions.
	innerExploreMatch *lang.FuncExpr
}

func (g *newRuleGen) init(compiled *lang.CompiledExpr, md *metadata, w *matchWriter) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*g = newRuleGen{
		compiled: compiled,
		md:       md,
		w:        w,
	}
}

// genRule generates match and replace code for one rule within the scope of
// a particular op construction method.
func (g *newRuleGen) genRule(rule *lang.RuleExpr) {
	g.uniquifier.init()
	g.boundStmts = make(map[lang.Expr]string)
	g.typedAliases = make(map[string]string)

	matchName := rule.Match.SingleName()
	define := g.compiled.LookupDefine(matchName)

	// Determine whether the rule is a normalization or exploration rule and
	// set up context accordingly.
	g.normalize = rule.Tags.Contains("Normalize")
	if g.normalize {
		g.thisVar = "_f"
		g.factoryVar = "_f"
	} else {
		g.thisVar = "_e"
		g.factoryVar = "_e.f"
		g.innerExploreMatch = g.findInnerExploreMatch(rule.Match)
	}

	g.w.writeIndent("// [%s]\n", rule.Name)
	marker := g.w.nestIndent("{\n")

	if g.normalize {
		// For normalization rules, expression fields are passed as parameters
		// to the factory method, so use those parameters directly.
		for index, matchArg := range rule.Match.Args {
			field := define.Fields[index]
			context := &contextDecl{
				code: unTitle(g.md.fieldName(field)),
				typ:  g.md.typeOf(field),
			}
			g.genMatch(matchArg, context, false /* noMatch */)
		}

		g.genNormalizeReplace(define, rule)
	} else {
		// For exploration rules, the memo expression is passed as a parameter,
		// so use accessors to get its fields.
		if rule.Match == g.innerExploreMatch {
			// The top-level match is the only match in this rule. Skip the
			// expression if it was processed in a previous exploration pass.
			g.w.nestIndent("if _rootOrd >= _rootState.start {\n")
		} else {
			// Initialize _partlyExplored for the top-level match. This variable
			// will be shadowed by each nested loop. Only if all loops are bound
			// to already explored expressions can the innermost match skip.
			g.w.writeIndent("_partlyExplored := _rootOrd < _rootState.start\n")
		}

		context := &contextDecl{code: "_root", typ: g.md.typeOf(define)}
		g.genConstantMatchArgs(rule.Match, define, context)
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
// The context parameter contains the Go code (variable name, field accessor
// expression, slice accessor expression, etc.) that accesses the expression
// that is currently being matched against. It also contains the type of that
// expression. For example:
//
//   for i := range elems {
//     _item := &elems[i]
//     _eq := _item.(*memo.EqExpr)
//     if _eq != nil {
//       _const := _eq.Left.(*memo.ConstExpr)
//       if _const != nil {
//         ...
//       }
//     }
//   }
//
// In that example, the context starts out as "elems", which is the top-level
// Tuple operator field that's being list matched. Then, the context recursively
// becomes _item, which is bound to one of the expressions in the list. And
// finally, it becomes _eq.Left, which returns the left operand of the Eq
// operator, and which is matched against in the innermost if statement.
//
// Matchers test whether the context expression "matches" according to the
// semantics of that matcher. For example, the child matcher will generate code
// that tests whether the expression's opname and its children match the
// pattern. The list matcher will generate code that tests whether a list
// expression contains an item that matches the list item matcher, and so on.
//
// If true, the noMatch flag inverts the matching logic. The matcher will now
// generate code that tests whether the context expression *does not* match
// according to the semantics of the matcher. Some matchers do not currently
// support generating code when noMatch is true.
func (g *newRuleGen) genMatch(match lang.Expr, context *contextDecl, noMatch bool) {
	switch t := match.(type) {
	case *lang.FuncExpr:
		g.genMatchNameAndChildren(t, context, noMatch)

	case *lang.CustomFuncExpr:
		g.genMatchCustom(t, noMatch)

	case *lang.LetExpr:
		g.genMatchLet(t, noMatch)

	case *lang.AndExpr:
		if noMatch {
			panic("noMatch is not yet supported by the and match op")
		}
		g.genMatch(t.Left, context, noMatch)
		g.genMatch(t.Right, context, noMatch)

	case *lang.NotExpr:
		// Flip the noMatch flag so that the input expression will test for the
		// inverse. No code needs to be generated here because each matcher is
		// responsible for handling the noMatch flag (or not).
		g.genMatch(t.Input, context, !noMatch)

	case *lang.BindExpr:
		// Alias the context variable.
		if string(t.Label) != context.code {
			g.w.writeIndent("%s := %s\n", t.Label, context.code)
		}
		newContext := &contextDecl{code: string(t.Label), typ: context.typ}

		// Keep track of the untyped alias so that we can "shadow" it with
		// a typed version later, if possible.
		newContext.untypedAlias = newContext.code

		g.genMatch(t.Target, newContext, noMatch)

	case *lang.StringExpr:
		// Delegate to custom function that matches String values.
		if noMatch {
			g.w.nestIndent("if !%s.funcs.EqualsString(%s, %s) {\n", g.factoryVar, context.code, t)
		} else {
			g.w.nestIndent("if %s.funcs.EqualsString(%s, %s) {\n", g.factoryVar, context.code, t)
		}

	case *lang.NumberExpr:
		// Delegate to custom function that matches Int, Float, and Decimal types.
		if noMatch {
			g.w.nestIndent("if !%s.funcs.EqualsNumber(%s, %s) {\n", g.factoryVar, context.code, t)
		} else {
			g.w.nestIndent("if %s.funcs.EqualsNumber(%s, %s) {\n", g.factoryVar, context.code, t)
		}

	case *lang.AnyExpr:
		if noMatch {
			g.w.nestIndent("if false {\n")
		}

	case *lang.ListExpr:
		// Handle all list matchers.
		g.genMatchList(t, context, noMatch)

	default:
		panic(fmt.Sprintf("unrecognized match expression: %v", match))
	}
}

// genMatchList generates code for the Optgen list operator. This operator
// has any number of ListAnyOp children (produced by '...' syntax) arranged
// around at most one non-ListAnyOp child. The following variants are possible:
//
//   match child in any position  : [ ... <child> ... ]
//   match child in first position: [ <child> ... ]
//   match child in last position : [ ... <child> ]
//   match singleton list         : [ <child> ]
//   match empty list             : [ ]
//   match any  list              : [ ... ]
//
func (g *newRuleGen) genMatchList(match *lang.ListExpr, context *contextDecl, noMatch bool) {
	// The list's type should already have been set by the Optgen compiler, and
	// should be the name of a list type.
	listTyp := g.md.lookupType(match.Typ.(*lang.ExternalDataType).Name)
	if listTyp == nil {
		panic(fmt.Sprintf("match field does not have a list type: %s", match))
	}
	listItemTyp := listTyp.listItemType

	// Determine which list matching variant to use.
	matchItem := lang.Expr(nil)
	isFirst := false
	isLast := false
	for i, item := range match.Items {
		if item.Op() != lang.ListAnyOp {
			matchItem = item
			if i == 0 {
				isFirst = true
			}
			if i == len(match.Items)-1 {
				isLast = true
			}
		}
	}

	// Handle empty list [] or no-op list [ ... ] case.
	if matchItem == nil {
		if len(match.Items) == 0 {
			if noMatch {
				g.w.nestIndent("if len(%s) != 0 {\n", context.code)
			} else {
				g.w.nestIndent("if len(%s) == 0 {\n", context.code)
			}
		}
		return
	}

	var contextName string
	switch {
	case isFirst && isLast:
		// Match single item.
		if noMatch {
			if matchItem.Op() != lang.AnyOp {
				panic("noMatch is not yet fully supported by the list match single op")
			}
			g.w.nestIndent("if len(%s) != 1 {\n", context.code)
			return
		}

		g.w.nestIndent("if len(%s) == 1 {\n", context.code)
		contextName = context.code + "[0]"

	case isFirst && !isLast:
		// Match first item in list.
		if noMatch {
			panic("noMatch is not yet supported by the list match first op")
		}
		g.w.nestIndent("if len(%s) > 0 {\n", context.code)
		contextName = context.code + "[0]"

	case !isFirst && isLast:
		// Match last item in list.
		if noMatch {
			panic("noMatch is not yet supported by the list match last op")
		}
		g.w.nestIndent("if len(%s) > 0 {\n", context.code)
		contextName = fmt.Sprintf("%s[len(%s)-1]", context.code, context.code)

	case !isFirst && !isLast:
		// Match any item in list.
		if noMatch {
			panic("noMatch is not yet supported by the list match any op")
		}
		g.w.nestIndent("for i := range %s {\n", context.code)
		contextName = context.code + "[i]"
	}
	contextName = g.makeListItemRef(contextName, listItemTyp)
	newContext := &contextDecl{code: contextName, typ: listItemTyp}

	// Store the expression in a variable, since it may be expensive to evaluate
	// multiple times. If already binding the item, use that variable, else use
	// a temporary _item variable.
	switch matchItem.(type) {
	case *lang.BindExpr:
		g.genMatch(matchItem, newContext, noMatch)

	case *lang.AnyExpr:
		// Don't need to bind item in case of matching [ * ], [ ... * ... ], etc.

	default:
		newContext = &contextDecl{code: "_item", typ: newContext.typ}
		g.w.writeIndent("_item := %s\n", contextName)
		g.genMatch(matchItem, newContext, noMatch)
	}
}

// makeListItemRef returns a list item reference expression. Some list operators
// inline children into the list slice, whereas others keep only pointers:
//
//   _item := &project.Projections[i]
//   vs.
//   _item := tuple.Elems[i]
//
// If the list item type has its own Optgen define, then it is a generated type,
// and will be inlined in its owning list slice. Otherwise, the list slice is
// []opt.ScalarExpr, which doesn't need to deref items.
func (g *newRuleGen) makeListItemRef(item string, typ *typeDef) string {
	if typ.isGenerated {
		return "&" + item
	}
	return item
}

// genMatchNameAndChildren generates code to match the opname and children of
// the context expression.
func (g *newRuleGen) genMatchNameAndChildren(
	match *lang.FuncExpr, context *contextDecl, noMatch bool,
) {
	// The name/child matcher can match multiple parts of the context expression,
	// including its name and zero or more of its children. If noMatch is false,
	// then all of these parts must match in order for the whole to match. If
	// noMatch is true, then at least one of the parts must *not* match in order
	// for the whole to match. This is equivalent to negating an AND expression
	// in boolean logic:
	//
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
		if !g.onlyScalarMatchNames(match) {
			// If the child group is not yet fully explored, then neither is the
			// root group, since new members in any descendant group make
			// additional matches possible.
			g.w.writeIndent("_state := _e.lookupExploreState(%s)\n", context.code)
			g.w.nestIndent("if !_state.fullyExplored {\n")
			g.w.writeIndent("_fullyExplored = false\n")
			g.w.unnest("}\n")
			g.w.writeIndent("var _member memo.RelExpr\n")
			g.w.nestIndent("for _ord := 0; _ord < _state.end; _ord++ {\n")
			g.w.nestIndent("if _member == nil {\n")
			g.w.writeIndent("_member = %s.FirstExpr()\n", context.code)
			g.w.unnest("}")
			g.w.nest("else {\n")
			g.w.writeIndent("_member = _member.NextExpr()\n")
			g.w.unnest("}\n")

			if match == g.innerExploreMatch {
				// This is the innermost match expression, so skip over already
				// explored expressions if all outer loops are also bound to
				// already explored expressions.
				g.w.nestIndent("if !_partlyExplored || _ord >= _state.start {\n")
			} else {
				// This match expression still has at least one more nested match
				// statement within it, so update _partlyExplored each time through
				// the loop so that the innermost match knows whether it can skip.
				// Note that _partlyExplored is deliberately shadowed so that each
				// loop has its own copy. If any outer copy is false, then every
				// inner copy should also be false.
				g.w.writeIndent("_partlyExplored := _partlyExplored && _ord < _state.start\n")
			}

			// Pass along the input context's untyped alias.
			context = &contextDecl{code: "_member", typ: g.md.lookupType("RelExpr"), untypedAlias: context.untypedAlias}
		}
	}

	// If the match expression matches more than one name, then more dynamic code
	// must be generated.
	dt := match.Typ.(*lang.DefineSetDataType)
	if len(dt.Defines) == 1 {
		g.genConstantMatch(match, dt.Defines[0], context, noMatch)
	} else {
		g.genDynamicMatch(match, dt.Defines, context, noMatch)
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
func (g *newRuleGen) findInnerExploreMatch(match *lang.FuncExpr) *lang.FuncExpr {
	inner := match

	var visitFunc lang.VisitFunc
	visitFunc = func(e lang.Expr) lang.Expr {
		if m, ok := e.(*lang.FuncExpr); ok {
			if !g.onlyScalarMatchNames(m) {
				inner = m
			}
		}
		return e.Visit(visitFunc)
	}

	match.Visit(visitFunc)
	return inner
}

// onlyScalarMatchNames returns true if each name references a define that has
// a Scalar tag.
func (g *newRuleGen) onlyScalarMatchNames(match *lang.FuncExpr) bool {
	for _, define := range match.Typ.(*lang.DefineSetDataType).Defines {
		if !define.Tags.Contains("Scalar") {
			return false
		}
	}
	return true
}

// genConstantMatch is called when the MatchExpr has only one define name
// to match (i.e. no tags). In this case, the type of the expression to match
// is statically known, and so the generated code can directly manipulate
// strongly-typed expression structs (e.g. SelectExpr, InnerJoinExpr, etc).
func (g *newRuleGen) genConstantMatch(
	match *lang.FuncExpr, opDef *lang.DefineExpr, context *contextDecl, noMatch bool,
) {
	// If the match type is a list item type, then no need to check for a match,
	// since it's already assumed to match (because lists are strongly-typed).
	if !opDef.Tags.Contains("ListItem") && context.typ.isInterface {
		// Cast context to concrete type of op to match.
		opTyp := g.md.typeOf(opDef)
		contextName := g.uniquifier.makeUnique(fmt.Sprintf("_%s", unTitle(string(opDef.Name))))
		newContext := &contextDecl{code: contextName, typ: opTyp}
		g.w.writeIndent("%s, _ := %s.(*%s)\n", newContext.code, context.code, opTyp.name)

		if noMatch {
			g.w.nestIndent("if %s == nil {\n", newContext.code)
			if len(match.Args) > 0 {
				panic("noMatch=true only supported without args")
			}
			return
		}

		// "Shadow" the untyped alias with the new typed version because it is
		// guaranteed to be a specific type within the scope of the if statement
		// written below.
		g.typedAliases[context.untypedAlias] = contextName
		g.w.nestIndent("if %s != nil {\n", newContext.code)
		context = newContext
	}

	g.genConstantMatchArgs(match, opDef, context)
}

func (g *newRuleGen) genConstantMatchArgs(
	match *lang.FuncExpr, opDef *lang.DefineExpr, context *contextDecl,
) {
	// Match expression children in the same order as arguments to the match
	// operator. If there are fewer arguments than there are children, then
	// only the first N children need to be matched.
	for i, matchArg := range match.Args {
		field := opDef.Fields[i]
		fieldName := g.md.fieldName(field)
		fieldTyp := g.md.typeOf(field)

		// Use fieldLoadPrefix, since an operator field is being treated as a
		// parameter.
		contextName := fmt.Sprintf("%s%s.%s", fieldLoadPrefix(fieldTyp), context.code, fieldName)
		newContext := &contextDecl{code: contextName, typ: g.md.typeOf(field)}
		g.genMatch(matchArg, newContext, false /* noMatch */)
	}
}

// genDynamicMatch is called when the MatchExpr is matching a tag name, or is
// matching multiple names. It matches expression children by dynamically
// getting children by index, without knowing the specific type of operator.
func (g *newRuleGen) genDynamicMatch(
	match *lang.FuncExpr, defines lang.DefineSetExpr, context *contextDecl, noMatch bool,
) {
	var buf bytes.Buffer
	for i, name := range match.NameChoice() {
		if i != 0 {
			buf.WriteString(" || ")
		}

		// The name is a tag if there is no operator define with that name.
		if g.compiled.LookupDefine(string(name)) != nil {
			// Match operator name.
			fmt.Fprintf(&buf, "%s.Op() == opt.%sOp", context.code, name)
		} else {
			// Match tag name.
			fmt.Fprintf(&buf, "opt.Is%sOp(%s)", name, context.code)
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
		// Match define fields in the same order as arguments to the match operator.
		// If there are fewer arguments than there are fields, then only the first
		// N fields need to be matched.
		for i, matchArg := range match.Args {
			// The compiler has already verified that all the defines have the
			// same types for each field, so arbitrarily pick the first define.
			field := defines[0].Fields[i]
			fieldTyp := g.md.typeOf(field)

			// Convert dynamic expression child or private to parameter type.
			var contextName string
			if fieldTyp.isExpr {
				contextName = fmt.Sprintf("%s.Child(%d)", context.code, i)
			} else {
				contextName = fmt.Sprintf("%s.Private()", context.code)
			}
			contextName = castFromDynamicParam(contextName, fieldTyp)

			newContext := &contextDecl{code: contextName, typ: fieldTyp}
			g.genMatch(matchArg, newContext, false /* noMatch */)
		}
	}
}

// genMatchCustom generates code to invoke a custom matching function.
func (g *newRuleGen) genMatchCustom(matchCustom *lang.CustomFuncExpr, noMatch bool) {
	g.genBoundStatements(matchCustom)

	if noMatch {
		g.w.nestIndent("if !")
	} else {
		g.w.nestIndent("if ")
	}

	g.genNestedExpr(matchCustom)

	g.w.write(" {\n")
}

// genMatchLet generates code for let expression matching.
func (g *newRuleGen) genMatchLet(let *lang.LetExpr, noMatch bool) {
	g.genBoundStatements(let)

	if noMatch {
		g.w.nestIndent("if !")
	} else {
		g.w.nestIndent("if ")
	}

	g.genNestedExpr(let)

	g.w.write(" {\n")
}

// genNormalizeReplace generates the replace pattern code for normalization
// rules. Normalization rules recursively call other factory methods in order to
// construct results. They also need to detect rule invocation cycles when the
// DetectCycle tag is present on the rule.
func (g *newRuleGen) genNormalizeReplace(define *lang.DefineExpr, rule *lang.RuleExpr) {
	g.w.nestIndent("if _f.matchedRule == nil || _f.matchedRule(opt.%s) {\n", rule.Name)

	g.genBoundStatements(rule.Replace)
	g.w.writeIndent("_expr := ")
	g.genNestedExpr(rule.Replace)
	if rule.Replace.InferredType() == lang.AnyDataType {
		g.genTypeConversion(define)
	}
	g.w.writeIndent("\n")

	// Notify listeners that rule was applied.
	g.w.nestIndent("if _f.appliedRule != nil {\n")
	g.w.writeIndent("_f.appliedRule(opt.%s, nil, _expr)\n", rule.Name)
	g.w.unnest("}\n")

	g.w.writeIndent("_f.constructorStackDepth--\n")
	g.w.writeIndent("return _expr\n")
}

// genExploreReplace generates the replace pattern code for exploration rules.
// Like normalization rules, an exploration rule constructs sub-expressions
// using the factory. However, it constructs the top-level expression on the
// stack and passes it to the corresponding AddXXXToGroup method, which adds the
// expression to an existing memo group.
func (g *newRuleGen) genExploreReplace(define *lang.DefineExpr, rule *lang.RuleExpr) {
	g.w.nestIndent("if _e.o.matchedRule == nil || _e.o.matchedRule(opt.%s) {\n", rule.Name)

	switch t := rule.Replace.(type) {
	case *lang.FuncExpr:
		name, ok := t.Name.(*lang.NameExpr)
		if !ok {
			panic("exploration pattern with dynamic replace name not yet supported")
		}
		opDef := g.compiled.LookupDefine(string(*name))
		opTyp := g.md.typeOf(opDef)

		g.genBoundStatements(t)
		g.w.nestIndent("_expr := &%s{\n", opTyp.name)
		for i, arg := range t.Args {
			field := opDef.Fields[i]

			// Use fieldStorePrefix, since parameter is being stored into a field.
			g.w.writeIndent("%s: %s", g.md.fieldName(field), fieldStorePrefix(g.md.typeOf(field)))
			g.genNestedExpr(arg)
			g.w.write(",\n")
		}
		g.w.unnest("}\n")
		g.w.writeIndent("_interned := _e.mem.Add%sToGroup(_expr, _root)\n", opDef.Name)

		// Notify listeners that rule was applied. If the expression was already
		// part of the memo, then _interned != _expr, and this rule is a no-op.
		g.w.nestIndent("if _e.o.appliedRule != nil {\n")
		g.w.nestIndent("if _interned != _expr {\n")
		g.w.writeIndent("_e.o.appliedRule(opt.%s, _root, nil)\n", rule.Name)
		g.w.unnest("}")
		g.w.nest("else {\n")
		g.w.writeIndent("_e.o.appliedRule(opt.%s, _root, _interned)\n", rule.Name)
		g.w.unnest("}")
		g.w.unnest("}\n")

	case *lang.CustomFuncExpr:
		// Remember the last member in the group. Any expressions added by the
		// custom function will be added after that member.
		g.w.writeIndent("var _last memo.RelExpr\n")
		g.w.nestIndent("if _e.o.appliedRule != nil {\n")
		g.w.writeIndent("_last = memo.LastGroupMember(_root)\n")
		g.w.unnest("}\n")

		// Pass the root expression to the top-level custom function as its first
		// parameter. It will add any generated expressions to that expressions's
		// group.
		g.genBoundStatements(rule.Replace)

		g.w.writeIndent("%s.funcs.%s(_root, ", g.thisVar, t.Name)
		for index, arg := range t.Args {
			if index != 0 {
				g.w.write(", ")
			}
			g.genNestedExpr(arg)
		}
		g.w.write(")\n")

		// Notify listeners that rule was applied, passing the linked list of
		// expressions added after _last (or nil if none were added).
		g.w.nestIndent("if _e.o.appliedRule != nil {\n")
		g.w.writeIndent("_e.o.appliedRule(opt.%s, _root, _last.NextExpr())\n", rule.Name)
		g.w.unnest("}\n")

	default:
		panic(fmt.Sprintf("unsupported replace expression in explore rule: %s", rule.Replace))
	}

	g.w.unnest("}\n")
}

// genBoundStatements is called before genNestedExpr in order to generate zero
// or more statements that construct subtrees of the given expression that are
// bound to variables. Those variables can be used when constructing other parts
// of the result tree. For example:
//
//   (InnerJoin $left:* $right:* $on:*)
//   =>
//   (InnerJoin
//     $varname:(SomeFunc $left)
//     $varname2:(Select $varname (SomeOtherFunc $right))
//     (MakeOn $varname $varname2)
//   )
//
//   varname := _f.funcs.SomeFunc(left)
//   varname2 := _f.ConstructSelect(varname, _f.funcs.SomeOtherFunc(right))
//
func (g *newRuleGen) genBoundStatements(e lang.Expr) {
	var visitFunc lang.VisitFunc
	visitFunc = func(e lang.Expr) lang.Expr {
		// Post-order traversal so that all descendants are generated before
		// generating ancestor.
		e.Visit(visitFunc)

		switch t := e.(type) {
		case *lang.BindExpr:
			g.w.writeIndent("%s := ", t.Label)
			g.genNestedExpr(t.Target)
			g.w.newline()
			g.boundStmts[t] = string(t.Label)

		case *lang.FuncExpr:
			if !t.HasDynamicName() {
				break
			}

			// The compiler has already verified that all the possible operators
			// that the function can construct have the same types for each field,
			// so arbitrarily pick the first define.
			opDef := t.Typ.(*lang.DefineSetDataType).Defines[0]

			// Extract sub-expressions that may need the address operator "&"
			// applied to them, as Go doesn't allow that in expressions in some
			// cases (e.g. &_f.funcs.CustomFunc()).
			for i, arg := range t.Args {
				// Don't extract empty list construction.
				if list, ok := arg.(*lang.ListExpr); ok && len(list.Items) == 0 {
					continue
				}

				// Non-pointer types that are passed by value (e.g. FiltersExpr slice
				// and opt.ColumnID) may have "&" applied to them, so extract them.
				argTyp := g.md.typeOf(opDef.Fields[i])
				if !argTyp.passByVal || argTyp.isPointer {
					continue
				}

				var label string
				if ref, ok := arg.(*lang.RefExpr); ok {
					label = string(ref.Label)
				} else {
					label = g.uniquifier.makeUnique("_arg")
				}

				g.w.writeIndent("%s := ", label)
				g.genNestedExpr(arg)
				g.w.newline()
				g.boundStmts[arg] = label
			}
		case *lang.LetExpr:
			var vars strings.Builder
			for i, label := range t.Labels {
				if i != 0 {
					vars.WriteString(", ")
				}
				vars.WriteString(string(label))
			}
			customFunc := t.Target.(*lang.CustomFuncExpr)
			g.w.writeIndent("%s := ", vars.String())
			g.genCustomFunc(customFunc)
			g.w.newline()
		}
		return e
	}
	visitFunc(e)
}

// genNestedExpr recursively generates an Optgen expression. Bound expressions
// should have already been generated as statements by genBoundStatements, so
// that genNestedExpr can generate references to those statements.
func (g *newRuleGen) genNestedExpr(e lang.Expr) {
	if label, ok := g.boundStmts[e]; ok {
		g.w.write(label)
		return
	}

	switch t := e.(type) {
	case *lang.FuncExpr:
		g.genConstruct(t)

	case *lang.ListExpr:
		g.genConstructList(t)

	case *lang.CustomFuncExpr:
		g.genCustomFunc(t)

	case *lang.RefExpr:
		varName := string(t.Label)
		if typed, ok := g.typedAliases[varName]; ok {
			// If there is a typed version of the alias, use it instead of the
			// untyped version.
			varName = typed
		}
		g.w.write(varName)

	case *lang.LetExpr:
		g.w.write(string(t.Result.Label))

	case *lang.StringExpr:
		// Literal string expressions construct DString datums.
		g.w.write("tree.NewDString(%s)", t)

	case *lang.NumberExpr:
		// Literal numeric expressions construct DInt datums.
		g.w.write("tree.NewDInt(%s)", t)

	case *lang.NameExpr:
		// OpName literal expressions construct an op identifier like SelectOp,
		// which can be passed as a function argument.
		g.w.write("opt.%sOp", t)

	default:
		panic(fmt.Sprintf("unhandled expression: %s", e))
	}
}

// genConstruct generates code to invoke an op construction function (either
// static or dynamic).
func (g *newRuleGen) genConstruct(construct *lang.FuncExpr) {
	if !construct.HasDynamicName() {
		// Standard op construction function.
		opName := string(*construct.Name.(*lang.NameExpr))
		g.w.nest("%s.Construct%s(\n", g.factoryVar, opName)

		for _, arg := range construct.Args {
			g.w.writeIndent("")
			g.genNestedExpr(arg)
			g.w.write(",\n")
		}

		g.w.unnest(")")
	} else {
		// Construct expression based on dynamic type of referenced op.
		fn := construct.Name.(*lang.CustomFuncExpr)
		if fn.Name != "OpName" {
			panic(fmt.Sprintf("unhandled custom function dynamic name expression: %s", fn.Name))
		}
		ref := fn.Args[0].(*lang.RefExpr)

		// The compiler has already verified that all the defines have the
		// same types for each field, so arbitrarily pick the first define.
		opDef := construct.Typ.(*lang.DefineSetDataType).Defines[0]

		g.w.nest("%s.DynamicConstruct(\n", g.factoryVar)
		g.w.writeIndent("%s.Op(),\n", ref.Label)
		for i, arg := range construct.Args {
			field := opDef.Fields[i]
			fieldTyp := g.md.typeOf(field)

			// Need to take address of any non-pointer values that would normally
			// be passed by value. When these values are typed dynamically as
			// opt.Expr, they are passed by reference (see comment for
			// castFromDynamicParam for more details).
			g.w.writeIndent("")
			if fieldTyp.passByVal && !fieldTyp.isPointer {
				g.w.write("&")
			}
			g.genNestedExpr(arg)
			g.w.write(",\n")
		}
		g.w.unnest(")")

		// Add type conversion to get expected output type.
		g.genTypeConversion(opDef)
	}
}

func (g *newRuleGen) genTypeConversion(define *lang.DefineExpr) {
	if define.Tags.Contains("Relational") {
		g.w.write(".(memo.RelExpr)")
	} else {
		g.w.write(".(opt.ScalarExpr)")
	}
}

// genCustomFunc generates code to invoke a custom replace function.
func (g *newRuleGen) genCustomFunc(customFunc *lang.CustomFuncExpr) {
	if customFunc.Name == "OpName" {
		// Handle OpName function that couldn't be statically resolved by
		// looking up op name at runtime.
		ref := customFunc.Args[0].(*lang.RefExpr)
		g.w.write("%s.Op()", ref.Label)
	} else if customFunc.Name == "Root" {
		// Handle Root function.
		if g.normalize {
			// The root expression can only be accessed during exploration.
			panic("the root expression can only be accessed during exploration")
		}
		g.w.write("_root")
	} else {
		funcName := string(customFunc.Name)
		g.w.write("%s.funcs.%s(", g.thisVar, funcName)
		for index, arg := range customFunc.Args {
			if index != 0 {
				g.w.write(", ")
			}
			g.genNestedExpr(arg)
		}
		g.w.write(")")
	}
}

// genConstructList generates code to construct an interned list of items:
//
//   ProjectionsList{
//     _f.ConstructProjectionsItem(elem, 1),
//     _f.ConstructProjectionsItem(elem2, 2),
//   }
//
func (g *newRuleGen) genConstructList(list *lang.ListExpr) {
	// The list's type should already have been set by the Optgen compiler, and
	// should be the name of a list type.
	listTyp := g.md.lookupType(list.Typ.(*lang.ExternalDataType).Name)
	if listTyp == nil {
		panic(fmt.Sprintf("match field does not have a list type: %s", list))
	}
	listItemTyp := listTyp.listItemType

	// Handle empty list case.
	if len(list.Items) == 0 {
		g.w.write("memo.Empty%s", listTyp.friendlyName)
		return
	}

	g.w.nest("%s{\n", listTyp.name)
	for _, item := range list.Items {
		if listItemTyp.isGenerated {
			listItemDef := g.compiled.LookupDefine(listItemTyp.friendlyName)

			g.w.nestIndent("%s.Construct%s(\n", g.factoryVar, listItemDef.Name)
			for _, subItem := range item.(*lang.FuncExpr).Args {
				g.genNestedExpr(subItem)
				g.w.write(",\n")
			}
			g.w.unnest("),\n")
		} else {
			g.genNestedExpr(item)
			g.w.write(",\n")
		}
	}
	g.w.unnest("}")
}

// sortRulesByPriority sorts the rules in-place, in priority order. Rules with
// higher priority are sorted before rules with lower priority. Rule priority
// is based on the presence or absence of the HighPriority or LowPriority tag
// on the rule:
//
//   - Rules with the HighPriority tag are run first.
//   - Rules with no priority tag are run next.
//   - Rules with the LowPriority tag are run last.
//
// Rules in the same priority group are run in the same order as they appear in
// the rules files (and rule files are run in alphabetical order).
func sortRulesByPriority(rules lang.RuleSetExpr) {
	rules.Sort(func(left, right *lang.RuleExpr) bool {
		// Handle case where left is high priority.
		if left.Tags.Contains("HighPriority") {
			return !right.Tags.Contains("HighPriority")
		}

		// Handle case where left is normal priority (no priority tags).
		if !left.Tags.Contains("LowPriority") {
			return right.Tags.Contains("LowPriority")
		}

		// Handle case where left is low priority.
		return false
	})
}
