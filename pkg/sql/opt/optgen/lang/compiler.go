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

package lang

import (
	"bytes"
	"errors"
	"fmt"
)

// CompiledExpr is the result of Optgen scanning, parsing, and semantic
// analysis. It contains the set of definitions and rules that were compiled
// from the Optgen input files.
type CompiledExpr struct {
	Defines     DefineSetExpr
	Rules       RuleSetExpr
	DefineTags  []string
	defineIndex map[string]*DefineExpr
	matchIndex  map[string]RuleSetExpr
}

// LookupDefine returns the DefineExpr with the given name.
func (c *CompiledExpr) LookupDefine(name string) *DefineExpr {
	return c.defineIndex[name]
}

// LookupMatchingRules returns the set of rules that match the given opname at
// the top-level, or nil if none do. For example, "InnerJoin" would match this
// rule:
//   [CommuteJoin]
//   (InnerJoin $r:* $s:*) => (InnerJoin $s $r)
func (c *CompiledExpr) LookupMatchingRules(name string) RuleSetExpr {
	return c.matchIndex[name]
}

func (c *CompiledExpr) String() string {
	var buf bytes.Buffer
	buf.WriteString("(Compiled\n")

	writeIndent(&buf, 1)
	buf.WriteString("(Defines\n")
	for _, define := range c.Defines {
		writeIndent(&buf, 2)
		define.Format(&buf, 2)
		buf.WriteString("\n")
	}
	writeIndent(&buf, 1)
	buf.WriteString(")\n")

	writeIndent(&buf, 1)
	buf.WriteString("(Rules\n")
	for _, rule := range c.Rules {
		writeIndent(&buf, 2)
		rule.Format(&buf, 2)
		buf.WriteString("\n")
	}
	writeIndent(&buf, 1)
	buf.WriteString(")\n")

	buf.WriteString(")\n")

	return buf.String()
}

// Compiler compiles Optgen language input files and builds a CompiledExpr
// result from them. Compilation consists of scanning/parsing the files, which
// produces an AST, and is followed by semantic analysis and limited rewrites
// on the AST which the compiler performs.
type Compiler struct {
	parser   *Parser
	compiled *CompiledExpr
	errors   []error
}

// NewCompiler constructs a new instance of the Optgen compiler, with the
// specified list of file paths as its input files. The Compile method
// must be called in order to compile the input files.
func NewCompiler(files ...string) *Compiler {
	compiled := &CompiledExpr{
		defineIndex: make(map[string]*DefineExpr),
		matchIndex:  make(map[string]RuleSetExpr),
	}
	return &Compiler{parser: NewParser(files...), compiled: compiled}
}

// SetFileResolver overrides the default method of opening input files. The
// default resolver will use os.Open to open input files from disk. Callers
// can use this method to open input files in some other way.
func (c *Compiler) SetFileResolver(resolver FileResolver) {
	// Forward call to the parser, which actually calls the resolver.
	c.parser.SetFileResolver(resolver)
}

// Compile parses and compiles the input files and returns the resulting
// CompiledExpr. If there are errors, then Compile returns nil, and the errors
// are returned by the Errors function.
func (c *Compiler) Compile() *CompiledExpr {
	root := c.parser.Parse()
	if root == nil {
		c.errors = c.parser.Errors()
		return nil
	}

	if !c.compileDefines(root.Defines) {
		return nil
	}

	if !c.compileRules(root.Rules) {
		return nil
	}

	return c.compiled
}

// Errors returns the collection of errors that occurred during compilation. If
// no errors occurred, then Errors returns nil.
func (c *Compiler) Errors() []error {
	return c.errors
}

func (c *Compiler) compileDefines(defines DefineSetExpr) bool {
	c.compiled.Defines = defines

	unique := make(map[TagExpr]bool)

	for _, define := range defines {
		// Record the define in the index for fast lookup.
		name := string(define.Name)
		_, ok := c.compiled.defineIndex[name]
		if ok {
			c.addErr(define.Source(), fmt.Errorf("duplicate '%s' define statement", name))
		}

		c.compiled.defineIndex[name] = define

		// Determine unique set of tags.
		for _, tag := range define.Tags {
			if !unique[tag] {
				c.compiled.DefineTags = append(c.compiled.DefineTags, string(tag))
				unique[tag] = true
			}
		}
	}

	return true
}

func (c *Compiler) compileRules(rules RuleSetExpr) bool {
	unique := make(map[StringExpr]bool)

	for _, rule := range rules {
		// Ensure that rule names are unique.
		_, ok := unique[rule.Name]
		if ok {
			c.addErr(rule.Source(), fmt.Errorf("duplicate rule name '%s'", rule.Name))
		}
		unique[rule.Name] = true

		var ruleCompiler ruleCompiler
		ruleCompiler.compile(c, rule)
	}

	// Index compiled rules by the op that they match at the top-level of the
	// rule.
	for _, rule := range c.compiled.Rules {
		name := string(rule.Match.Names[0])
		existing := c.compiled.matchIndex[name]
		c.compiled.matchIndex[name] = append(existing, rule)
	}

	return len(c.errors) == 0
}

func (c *Compiler) addErr(src *SourceLoc, err error) {
	if src != nil {
		err = fmt.Errorf("%s: %s", src, err.Error())
	}
	c.errors = append(c.errors, err)
}

// ruleCompiler compiles a single rule. It is a separate struct in order to
// keep state for the ruleContentCompiler.
type ruleCompiler struct {
	compiler *Compiler
	compiled *CompiledExpr
	rule     *RuleExpr

	// bindings tracks variable bindings in order to ensure uniqueness.
	bindings map[StringExpr]*BindExpr

	// opName keeps the root match name in order to compile the OpName built-in
	// function.
	opName *NameExpr
}

func (c *ruleCompiler) compile(compiler *Compiler, rule *RuleExpr) {
	c.compiler = compiler
	c.compiled = compiler.compiled
	c.rule = rule

	// Expand root rules that match multiple operators into a separate match
	// expression for each matching operator.
	for _, name := range rule.Match.Names {
		defines := c.findMatchingDefines(string(name))
		if len(defines) == 0 {
			// No defines with that tag found, which is not allowed.
			defines = nil
			c.compiler.addErr(rule.Match.Source(), fmt.Errorf("unrecognized match name '%s'", name))
		}

		for _, define := range defines {
			// Create a rule for every matching define.
			c.expandRule(NameExpr(define.Name))
		}
	}
}

// expandRule rewrites the current rule to match the given opname rather than
// a list of names or a tag name. This transformation makes it easier for the
// code generator, since all rules will never match more than one op at the
// top-level.
func (c *ruleCompiler) expandRule(opName NameExpr) {
	// Remember the root opname in case its needed to compile the OpName
	// built-in function.
	c.opName = &opName
	c.bindings = make(map[StringExpr]*BindExpr)

	// Construct new match expression that matches a single name.
	match := &MatchExpr{Src: c.rule.Match.Src, Names: NamesExpr{opName}}
	match.Args = append(match.Args, c.rule.Match.Args...)

	compiler := ruleContentCompiler{compiler: c, src: c.rule.Src, matchPattern: true}
	match = compiler.compile(match).(*MatchExpr)

	compiler = ruleContentCompiler{compiler: c, src: c.rule.Src, matchPattern: false}
	replace := compiler.compile(c.rule.Replace)

	newRule := &RuleExpr{
		Src:      c.rule.Src,
		Name:     c.rule.Name,
		Comments: c.rule.Comments,
		Tags:     c.rule.Tags,
		Match:    match,
		Replace:  replace,
	}

	c.checkLiteralNames(newRule)

	c.compiled.Rules = append(c.compiled.Rules, newRule)
}

// checkLiteralNames traverses the expression tree and verifies that each
// literal name expressions is in a legal location (i.e. not in match pattern),
// and that it matches an operator name.
func (c *ruleCompiler) checkLiteralNames(rule *RuleExpr) {
	allowLiteralName := false
	src := rule.Src

	var fn func(Expr) Expr
	fn = func(e Expr) Expr {
		// Save current value of allowLiteralName and restore after visit.
		saveName := allowLiteralName

		// Remember source information.
		saveSrc := src
		if e.Source() != nil {
			src = e.Source()
		}

		// Only visit the arguments of the Match, Construct, and CustomFunc
		// expressions, since literal names are allowed in their name operands.
		switch t := e.(type) {
		case *MatchExpr:
			allowLiteralName = false
			t.Args.Visit(fn)

		case *ConstructExpr:
			allowLiteralName = true
			t.Args.Visit(fn)

		case *CustomFuncExpr:
			allowLiteralName = true
			t.Args.Visit(fn)

		case *NameExpr:
			if !allowLiteralName {
				c.compiler.addErr(src, errors.New("cannot match literal name"))
			} else {
				define := c.compiler.compiled.LookupDefine(string(*t))
				if define == nil {
					c.compiler.addErr(src, fmt.Errorf("%s is not an operator name", *t))
				}
			}

		default:
			// Recurse into every child of other kinds of expressions.
			e.Visit(fn)
		}

		allowLiteralName = saveName
		src = saveSrc
		return e
	}

	if _, ok := rule.Replace.(*NameExpr); ok {
		c.compiler.addErr(src, errors.New("replace pattern cannot be a literal name"))
		return
	}

	rule.Visit(fn)
}

// findMatchingDefines returns the set of define expressions which either
// exactly match the given name, or else have a tag that matches the given
// name. If no matches can be found, then findMatchingDefines returns nil.
func (c *ruleCompiler) findMatchingDefines(name string) []*DefineExpr {
	var defines []*DefineExpr

	compiled := c.compiler.compiled
	define := compiled.LookupDefine(name)
	if define != nil {
		defines = append(defines, define)
	} else {
		// Name might be a tag name, so find all defines with that tag.
		for _, define := range compiled.Defines {
			if define.Tags.Contains(name) {
				defines = append(defines, define)
			}
		}
	}

	return defines
}

// ruleContentCompiler is the workhorse of rule compilation. It is recursively
// constructed on the stack in order to keep scoping context for match and
// construct expressions. Semantics can change depending on the context.
type ruleContentCompiler struct {
	compiler *ruleCompiler

	// src is the source location of the nearest match or construct expression,
	// and is used when the source location isn't otherwise available.
	src *SourceLoc

	// matchPattern is true when compiling in the scope of a match pattern, and
	// false when compiling in the scope of a replace pattern.
	matchPattern bool

	// customFunc is true when compiling in the scope of a custom match or
	// replace function, and false when compiling in the scope of an op matcher
	// or op constructor.
	customFunc bool
}

func (c *ruleContentCompiler) compile(e Expr) Expr {
	// Recurse into match or construct operator separately, since they will need
	// to ceate new context before visiting arguments.
	switch t := e.(type) {
	case *MatchExpr:
		return c.compileMatch(t)

	case *ConstructExpr:
		return c.compileConstruct(t)

	case *BindExpr:
		// If in a match pattern and not in a custom match function, then ensure
		// that binding labels are unique.
		if c.matchPattern && !c.customFunc {
			_, ok := c.compiler.bindings[t.Label]
			if ok {
				c.addErr(t, fmt.Errorf("duplicate bind label '%s'", t.Label))
			}
			c.compiler.bindings[t.Label] = t
		} else {
			c.addDisallowedErr(t, "cannot bind arguments")
		}

	case *RefExpr:
		if c.matchPattern && !c.customFunc {
			c.addDisallowedErr(t, "cannot use variable references")
		} else {
			// Check that referenced variable exists.
			_, ok := c.compiler.bindings[t.Label]
			if !ok {
				c.addErr(t, fmt.Errorf("unrecognized variable name '%s'", t.Label))
			}
		}

	case *MatchListAnyExpr, *MatchListEmptyExpr, *MatchListFirstExpr,
		*MatchListLastExpr, *MatchListSingleExpr:
		if !c.matchPattern || c.customFunc {
			c.addDisallowedErr(t, "cannot use lists")
		}

	case *MatchAndExpr, *MatchNotExpr:
		if !c.matchPattern || c.customFunc {
			c.addDisallowedErr(t, "cannot use boolean expressions")
		}

	case *MatchAnyExpr:
		if !c.matchPattern || c.customFunc {
			c.addDisallowedErr(t, "cannot use wildcard matcher")
		}
	}

	// Pre-order traversal.
	return e.Visit(c.compile)
}

func (c *ruleContentCompiler) compileMatch(match *MatchExpr) Expr {
	// Ensure that all match names are defined and check whether this is a
	// custom match function invocation.
	var customFunc *CustomFuncExpr
	for _, name := range match.Names {
		defines := c.compiler.findMatchingDefines(string(name))
		if defines == nil {
			// This must be an invocation of a custom match function, because
			// there is no matching define.
			if len(match.Names) != 1 {
				c.addErr(match, errors.New("custom function cannot have multiple names"))
				return match
			}

			// Handle built-in function.
			if match.Names[0] == "OpName" {
				opName, ok := c.compileOpName(match, match.Args)
				if ok {
					return opName
				}

				// Fall through and create CustomFuncExpr if opname can't be
				// determined at compile-time.
			}

			// Create a CustomFuncExpr to make it easier to distinguish between
			// op matchers and and custom function invocations.
			customFunc = &CustomFuncExpr{Name: match.Names[0], Args: match.Args}
			break
		}
	}

	if c.customFunc && customFunc == nil {
		c.addErr(match, errors.New("custom function name cannot be an operator name"))
		return match
	}

	// Create nested context and recurse into children.
	nested := ruleContentCompiler{
		compiler:     c.compiler,
		src:          match.Source(),
		matchPattern: c.matchPattern,
		customFunc:   customFunc != nil,
	}

	if customFunc == nil {
		return match.Visit(nested.compile)
	}
	return customFunc.Visit(nested.compile)
}

func (c *ruleContentCompiler) compileConstruct(construct *ConstructExpr) Expr {
	var customFunc *CustomFuncExpr
	if name, ok := construct.Name.(*NameExpr); ok {
		// Create a CustomFuncExpr if not constructing an operator. This makes
		// it easier to distinguish between op constructors and custom function
		// invocations.
		defines := c.compiler.findMatchingDefines(string(*name))
		if defines != nil {
			// Don't allow construct name to be a tag name.
			if len(defines) > 1 || string(defines[0].Name) != string(*name) {
				c.addErr(construct, fmt.Errorf("construct name cannot be a tag"))
			}
		} else {
			// Handle built-in OpName function.
			if *name == "OpName" {
				opName, ok := c.compileOpName(construct, construct.Args)
				if ok {
					return opName
				}

				// Fall through and create CustomFuncExpr if opname can't be
				// determined at compile-time.
			}

			customFunc = &CustomFuncExpr{Name: *name, Args: construct.Args, Src: construct.Src}
		}
	}

	// Create nested context and recurse into children.
	nested := ruleContentCompiler{
		compiler:     c.compiler,
		src:          construct.Source(),
		matchPattern: c.matchPattern,
		customFunc:   customFunc != nil,
	}

	if customFunc == nil {
		return construct.Visit(nested.compile)
	}
	return customFunc.Visit(nested.compile)
}

func (c *ruleContentCompiler) compileOpName(fn Expr, args ListExpr) (_ Expr, ok bool) {
	if len(args) > 1 {
		c.addErr(fn, fmt.Errorf("too many arguments to OpName function"))
		return fn, false
	}

	if len(args) == 0 {
		// No args to OpName function refers to top-level match operator.
		return c.compiler.opName, true
	}

	// Otherwise expect a single variable reference argument.
	ref, ok := args[0].(*RefExpr)
	if !ok {
		c.addErr(fn, fmt.Errorf("invalid OpName argument: argument must be a variable reference"))
		return fn, false
	}

	// Get the match name of the expression bound to the variable, if it's
	// constant.
	bind, ok := c.compiler.bindings[ref.Label]
	if ok {
		if match, ok := bind.Target.(*MatchExpr); ok {
			// Handle common case where match expression is bound to a single
			// name that matches a definition name.
			opName, ok := c.extractConstantName(match)
			if ok {
				return opName, true
			}
		}
	}
	return fn, false
}

// extractConstantName checks for the special, but common case where the given
// expression matches a single constant opname, rather than multiple names or
// a define tag.
func (c *ruleContentCompiler) extractConstantName(match *MatchExpr) (name *NameExpr, ok bool) {
	// If matching multiple names, then return false.
	if len(match.Names) != 1 {
		return nil, false
	}

	// If name is a tag name, then return false.
	def := c.compiler.compiled.LookupDefine(string(match.Names[0]))
	if def == nil {
		return nil, false
	}

	return &match.Names[0], true
}

// addDisallowedErr creates an error prefixed by one of the following strings,
// depending on the context:
//   match pattern
//   replace pattern
//   custom match function
//   custom replace function
func (c *ruleContentCompiler) addDisallowedErr(loc Expr, disallowed string) {
	if c.matchPattern {
		if c.customFunc {
			c.addErr(loc, fmt.Errorf("custom match function %s", disallowed))
		} else {
			c.addErr(loc, fmt.Errorf("match pattern %s", disallowed))
		}
	} else {
		if c.customFunc {
			c.addErr(loc, fmt.Errorf("custom replace function %s", disallowed))
		} else {
			c.addErr(loc, fmt.Errorf("replace pattern %s", disallowed))
		}
	}
}

func (c *ruleContentCompiler) addErr(loc Expr, err error) {
	src := loc.Source()
	if src == nil {
		src = c.src
	}
	c.compiler.compiler.addErr(src, err)
}
