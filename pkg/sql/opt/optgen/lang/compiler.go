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
}

// LookupDefine uses the define index to find the DefineExpr with the given
// name.
func (c *CompiledExpr) LookupDefine(name string) *DefineExpr {
	return c.defineIndex[name]
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
	compiled := &CompiledExpr{defineIndex: make(map[string]*DefineExpr)}
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

	return len(c.errors) == 0
}

func (c *Compiler) addErr(src *SourceLoc, err error) {
	if src != nil {
		err = fmt.Errorf("%s: %s", src, err.Error())
	}
	c.errors = append(c.errors, err)
}

// ruleCompiler compiles a single rule. It is a separate struct in order to
// keep state for accept functions.
type ruleCompiler struct {
	compiler *Compiler
	compiled *CompiledExpr
	rule     *RuleExpr

	// State used by accept functions.
	labels map[StringExpr]bool
	opName *OpNameExpr
}

func (c *ruleCompiler) compile(compiler *Compiler, rule *RuleExpr) {
	c.compiler = compiler
	c.compiled = compiler.compiled
	c.rule = rule

	// Expand root rules that match multiple operators into a separate match
	// expression for each matching operator.
	for _, name := range rule.Match.Names {
		for _, define := range c.findMatchingDefines(rule.Match, string(name)) {
			// Create a rule for every match.
			c.expandRule(OpNameExpr(define.Name))
		}
	}
}

// expandRule rewrites the current rule to match the given opname rather than
// a list of names or a tag name. This transformation makes it easier for the
// code generator, since all rules will never match more than one op at the
// top-level.
func (c *ruleCompiler) expandRule(opName OpNameExpr) {
	// Remember the root opname in case its needed to compile the OpName
	// built-in function.
	c.opName = &opName
	c.labels = make(map[StringExpr]bool)

	// Construct new match expression that matches a single name.
	match := &MatchExpr{Src: c.rule.Match.Src, Names: OpNamesExpr{opName}}
	match.Args = append(match.Args, c.rule.Match.Args...)

	match = match.Visit(c.acceptRuleMatchExpr).(*MatchExpr)
	replace := c.rule.Replace.Visit(c.acceptRuleReplaceExpr)

	newRule := &RuleExpr{
		Src:     c.rule.Src,
		Name:    c.rule.Name,
		Tags:    c.rule.Tags,
		Match:   match,
		Replace: replace,
	}
	c.compiled.Rules = append(c.compiled.Rules, newRule)
}

// acceptRuleMatchExpr does semantic checks on expressions within the rule's
// match expression.
func (c *ruleCompiler) acceptRuleMatchExpr(expr Expr) Expr {
	// Ensure that every match expression matches valid names.
	if match, ok := expr.(*MatchExpr); ok {
		for _, name := range match.Names {
			c.findMatchingDefines(match, string(name))
		}
	}

	if bind, ok := expr.(*BindExpr); ok {
		_, ok := c.labels[bind.Label]
		if ok {
			c.compiler.addErr(bind.Source(), fmt.Errorf("duplicate bind label '%s'", bind.Label))
		}
		c.labels[bind.Label] = true
	}

	return expr
}

// acceptRuleReplaceExpr performs semantic checks and rewrites on expressions
// within the rule's replace expression.
func (c *ruleCompiler) acceptRuleReplaceExpr(expr Expr) Expr {
	if construct, ok := expr.(*ConstructExpr); ok {
		// Handle built-in OpName function.
		if strName, ok := construct.OpName.(*StringExpr); ok && string(*strName) == "OpName" {
			if len(construct.Args) > 1 {
				src := construct.Source()
				c.compiler.addErr(src, fmt.Errorf("too many arguments to OpName function"))
				return expr
			}

			if len(construct.Args) == 0 {
				// No args to OpName function refers to top-level match operator.
				return c.opName
			}

			// Otherwise expect a single variable reference argument.
			ref, ok := construct.Args[0].(*RefExpr)
			if !ok {
				format := "invalid OpName argument: argument must be a variable reference"
				c.compiler.addErr(construct.Source(), fmt.Errorf(format))
				return expr
			}

			// Get the match name of the expression bound to the variable, if
			// it's constant.
			opName := c.resolveOpName(c.rule.Match, ref)
			if opName != nil {
				return opName
			}
		}
	}

	return expr
}

// findMatchingDefines returns the set of define expressions which either
// exactly match the given name, or else have a tag that matches the given
// name.
func (c *ruleCompiler) findMatchingDefines(match *MatchExpr, name string) []*DefineExpr {
	var defines []*DefineExpr

	define := c.compiled.LookupDefine(name)
	if define != nil {
		defines = append(defines, define)
	} else {
		// Name must be a tag name, so find all defines with that tag.
		found := false
		for _, define := range c.compiled.Defines {
			if define.Tags.Contains(name) {
				defines = append(defines, define)
				found = true
			}
		}

		if !found {
			// No defines with that tag found, which is not allowed.
			defines = nil
			c.compiler.addErr(match.Source(), fmt.Errorf("unrecognized match name '%s'", name))
		}
	}

	return defines
}

// resolveOpName searches the given expression subtree for match expressions
// that are bound to the referenced variable. If such an expression is found,
// then resolveOpName tries to extract a constant opname from it.
func (c *ruleCompiler) resolveOpName(expr Expr, ref *RefExpr) *OpNameExpr {
	if bind, ok := expr.(*BindExpr); ok {
		if bind.Label == ref.Label {
			if match, ok := bind.Target.(*MatchExpr); ok {
				// Handle common case where match expression is bound to a
				// single name that matches a definition name.
				opName, ok := c.extractConstantName(match)
				if ok {
					return &opName
				}
			} else {
				format := "invalid OpName argument: $%s must be bound to a match expression"
				c.compiler.addErr(ref.Source(), fmt.Errorf(format, ref.Label))
			}
			return nil
		}
	}

	for i := 0; i < expr.ChildCount(); i++ {
		child := expr.Child(i)
		if name := c.resolveOpName(child, ref); name != nil {
			return name
		}
	}

	return nil
}

// extractConstantName checks for the special, but common case where the given
// expression matches a single constant opname, rather than multiple names or
// a define tag.
func (c *ruleCompiler) extractConstantName(match *MatchExpr) (name OpNameExpr, ok bool) {
	// If matching multiple names, then return false.
	if len(match.Names) != 1 {
		return
	}

	// If name is a tag name, then return false.
	def := c.compiled.LookupDefine(string(match.Names[0]))
	if def == nil {
		return
	}

	name = match.Names[0]
	ok = true
	return
}
