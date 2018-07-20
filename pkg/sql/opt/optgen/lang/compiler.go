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

// LookupMatchingDefines returns the set of define expressions which either
// exactly match the given name, or else have a tag that matches the given
// name. If no matches can be found, then LookupMatchingDefines returns nil.
func (c *CompiledExpr) LookupMatchingDefines(name string) DefineSetExpr {
	var defines DefineSetExpr
	define := c.LookupDefine(name)
	if define != nil {
		defines = append(defines, define)
	} else {
		// Name might be a tag name, so find all defines with that tag.
		for _, define := range c.Defines {
			if define.Tags.Contains(name) {
				defines = append(defines, define)
			}
		}
	}
	return defines
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
		name := rule.Match.SingleName()
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

	if _, ok := rule.Match.Name.(*FuncExpr); ok {
		// Function name is itself a function that dynamically determines name.
		c.compiler.addErr(rule.Match.Source(), errors.New("cannot match dynamic name"))
		return
	}

	// Expand root rules that match multiple operators into a separate match
	// expression for each matching operator.
	for _, name := range rule.Match.NameChoice() {
		defines := c.compiled.LookupMatchingDefines(string(name))
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
	// Remember the root opname in case it's needed to compile the OpName
	// built-in function.
	c.opName = &opName
	c.bindings = make(map[StringExpr]*BindExpr)

	// Construct new match expression that matches a single name.
	match := &FuncExpr{Src: c.rule.Match.Src, Name: &NamesExpr{opName}}
	match.Args = append(match.Args, c.rule.Match.Args...)

	compiler := ruleContentCompiler{compiler: c, src: c.rule.Src, matchPattern: true}
	match = compiler.compile(match).(*FuncExpr)

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

	c.compiled.Rules = append(c.compiled.Rules, newRule)
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
	case *FuncExpr:
		return c.compileFunc(t)

	case *BindExpr:
		// Ensure that binding labels are unique.
		_, ok := c.compiler.bindings[t.Label]
		if ok {
			c.addErr(t, fmt.Errorf("duplicate bind label '%s'", t.Label))
		}
		c.compiler.bindings[t.Label] = t

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

	case *ListExpr:
		if c.matchPattern && c.customFunc {
			c.addDisallowedErr(t, "cannot use lists")
		} else {
			c.compileList(t)
		}

	case *AndExpr, *NotExpr:
		if !c.matchPattern || c.customFunc {
			c.addDisallowedErr(t, "cannot use boolean expressions")
		}

	case *NameExpr:
		if c.matchPattern && !c.customFunc {
			c.addErr(t, fmt.Errorf("cannot match literal name '%s'", *t))
		} else {
			define := c.compiler.compiled.LookupDefine(string(*t))
			if define == nil {
				c.addErr(t, fmt.Errorf("%s is not an operator name", *t))
			}
		}

	case *AnyExpr:
		if !c.matchPattern || c.customFunc {
			c.addDisallowedErr(t, "cannot use wildcard matcher")
		}
	}

	// Pre-order traversal.
	return e.Visit(c.compile)
}

func (c *ruleContentCompiler) compileList(list *ListExpr) {
	foundNotAny := false
	for _, item := range list.Items {
		if item.Op() == ListAnyOp {
			if !c.matchPattern {
				c.addErr(list, errors.New("list constructor cannot use '...'"))
			}
		} else {
			if c.matchPattern && foundNotAny {
				c.addErr(item, errors.New("list matcher cannot contain multiple expressions"))
				break
			}
			foundNotAny = true
		}
	}
}

func (c *ruleContentCompiler) compileFunc(fn *FuncExpr) Expr {
	// Create nested context and recurse into children.
	nested := ruleContentCompiler{
		compiler:     c.compiler,
		src:          fn.Source(),
		matchPattern: c.matchPattern,
	}

	funcName := fn.Name

	if nameExpr, ok := funcName.(*FuncExpr); ok {
		// Function name is itself a function that dynamically determines name.
		if c.matchPattern {
			c.addErr(fn, errors.New("cannot match dynamic name"))
		}

		funcName = c.compileFunc(nameExpr)
	} else {
		// Ensure that all function names are defined and check whether this is a
		// custom match function invocation.
		for _, name := range fn.NameChoice() {
			defines := c.compiler.compiled.LookupMatchingDefines(string(name))
			if defines != nil {
				if !c.matchPattern {
					if len(fn.NameChoice()) != 1 {
						c.addErr(fn, errors.New("constructor cannot have multiple names"))
						return fn
					}

					// Don't allow replace pattern name to be a tag name.
					if len(defines) > 1 || string(defines[0].Name) != string(name) {
						c.addErr(fn, fmt.Errorf("construct name cannot be a tag"))
					}
				}

				// Normalize single name into NameExpr rather than NamesExpr.
				if len(fn.NameChoice()) == 1 {
					funcName = &fn.NameChoice()[0]
				}

				// Ensure that each operator to be matched has a sufficient number
				// of fields.
				for _, define := range defines {
					if len(define.Fields) < len(fn.Args) {
						c.addErr(fn, fmt.Errorf("%s has only %d fields", define.Name, len(define.Fields)))
					}
				}
			} else {
				// This must be an invocation of a custom function, because
				// there is no matching define.
				if len(fn.NameChoice()) != 1 {
					c.addErr(fn, errors.New("custom function cannot have multiple names"))
					return fn
				}

				// Handle built-in function.
				if name == "OpName" {
					opName, ok := c.compileOpName(fn)
					if ok {
						return opName
					}

					// Fall through and create CustomFuncExpr if opname can't be
					// determined at compile-time.
				}

				// Save name of custom function.
				funcName = &fn.NameChoice()[0]
				nested.customFunc = true
				break
			}
		}
	}

	if c.matchPattern && c.customFunc && !nested.customFunc {
		c.addErr(fn, errors.New("custom function name cannot be an operator name"))
		return fn
	}

	args := fn.Args.Visit(nested.compile).(*SliceExpr)

	if nested.customFunc {
		// Create a CustomFuncExpr to make it easier to distinguish between
		// op matchers and and custom function invocations.
		return &CustomFuncExpr{Name: *funcName.(*NameExpr), Args: *args, Src: fn.Source()}
	}
	return &FuncExpr{Name: funcName, Args: *args, Src: fn.Source()}
}

func (c *ruleContentCompiler) compileOpName(fn *FuncExpr) (_ Expr, ok bool) {
	if len(fn.Args) > 1 {
		c.addErr(fn, fmt.Errorf("too many arguments to OpName function"))
		return fn, false
	}

	if len(fn.Args) == 0 {
		// No args to OpName function refers to top-level match operator.
		return c.compiler.opName, true
	}

	// Otherwise expect a single variable reference argument.
	ref, ok := fn.Args[0].(*RefExpr)
	if !ok {
		c.addErr(fn, fmt.Errorf("invalid OpName argument: argument must be a variable reference"))
		return fn, false
	}

	// Get the name of the function expression bound to the variable, if it's
	// constant.
	bind, ok := c.compiler.bindings[ref.Label]
	if ok {
		if target, ok := bind.Target.(*FuncExpr); ok {
			// Handle common case where target is a function expression with a single
			// name that matches a definition name.
			opName, ok := c.extractConstantName(target)
			if ok {
				return opName, true
			}
		}
	}
	return fn, false
}

// extractConstantName checks for the special, but common case where the given
// function expression has a single constant opname, rather than multiple names
// or a define tag.
func (c *ruleContentCompiler) extractConstantName(fn *FuncExpr) (name *NameExpr, ok bool) {
	// If function has multiple names, then return false.
	names, ok := fn.Name.(*NamesExpr)
	if ok && len(*names) != 1 {
		return nil, false
	}

	// If name is a tag name, then return false.
	def := c.compiler.compiled.LookupDefine(string((*names)[0]))
	if def == nil {
		return nil, false
	}

	return &(*names)[0], true
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
