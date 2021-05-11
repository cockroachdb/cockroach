// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package lang

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/errors"
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

	// bindings tracks variable bindings in order to ensure uniqueness and to
	// infer types.
	bindings map[StringExpr]DataType

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
	// Remember current error count in order to detect whether ruleContentCompiler
	// adds additional errors.
	errCntBefore := len(c.compiler.errors)

	// Remember the root opname in case it's needed to compile the OpName
	// built-in function.
	c.opName = &opName
	c.bindings = make(map[StringExpr]DataType)

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

	// Infer data types for expressions within the match and replace patterns.
	// Do this only if the rule triggered no errors.
	if errCntBefore == len(c.compiler.errors) {
		c.inferTypes(newRule.Match, AnyDataType)
		c.inferTypes(newRule.Replace, AnyDataType)
	}

	c.compiled.Rules = append(c.compiled.Rules, newRule)
}

// inferTypes walks the tree and annotates it with inferred data types. It
// reports any typing errors it encounters. Each expression is annotated with
// either its "bottom-up" type which it infers from its inputs, or else its
// "top-down" type (the suggested argument), which is passed down from its
// ancestor(s). Each operator has its own rules of which to use.
func (c *ruleCompiler) inferTypes(e Expr, suggested DataType) {
	switch t := e.(type) {
	case *FuncExpr:
		var defType *DefineSetDataType
		if t.HasDynamicName() {
			// Special-case the OpName built-in function.
			nameFunc, ok := t.Name.(*CustomFuncExpr)
			if !ok || nameFunc.Name != "OpName" {
				panic(fmt.Sprintf("%s not allowed as dynamic function name", t.Name))
			}

			// Inherit type of the opname target.
			label := nameFunc.Args[0].(*RefExpr).Label
			t.Typ = c.bindings[label]
			if t.Typ == nil {
				panic(fmt.Sprintf("$%s does not have its type set", label))
			}

			defType, ok = t.Typ.(*DefineSetDataType)
			if !ok {
				err := errors.New("cannot infer type of construction expression")
				c.compiler.addErr(nameFunc.Args[0].Source(), err)
				break
			}

			// If the OpName refers to a single operator, rewrite it as a simple
			// static name.
			if len(defType.Defines) == 1 {
				name := NameExpr(defType.Defines[0].Name)
				t.Name = &name
			}
		} else {
			// Construct list of defines that can be matched.
			names := t.NameChoice()
			defines := make(DefineSetExpr, 0, len(names))
			for _, name := range names {
				defines = append(defines, c.compiled.LookupMatchingDefines(string(name))...)
			}

			// Set the data type of the function.
			defType = &DefineSetDataType{Defines: defines}
			t.Typ = defType
		}

		// First define in list is considered the "prototype" that all others
		// match. The matching is checked in ruleContentCompiler.compileFunc.
		prototype := defType.Defines[0]

		if len(t.Args) > len(prototype.Fields) {
			err := fmt.Errorf("%s has too many args", e.Op())
			c.compiler.addErr(t.Source(), err)
			break
		}

		// Recurse on name and arguments.
		c.inferTypes(t.Name, AnyDataType)
		for i, arg := range t.Args {
			suggested := &ExternalDataType{Name: string(prototype.Fields[i].Type)}
			c.inferTypes(arg, suggested)
		}

	case *CustomFuncExpr:
		// Return type of custom function isn't known, but might be inferred from
		// context in which it's used.
		t.Typ = suggested

		// Recurse on arguments, passing AnyDataType as suggested type, because
		// no information is known about their types.
		for _, arg := range t.Args {
			c.inferTypes(arg, AnyDataType)
		}

	case *LetExpr:
		// Set type of the let to type of its result ref or the suggested type.
		typ := c.bindings[t.Result.Label]
		if typ == nil {
			panic(fmt.Sprintf("$%s does not have its type set", t.Result.Label))
		}
		t.Typ = mostRestrictiveDataType(typ, suggested)

	case *BindExpr:
		// Set type of binding to the type of its target.
		c.inferTypes(t.Target, suggested)
		t.Typ = t.Target.InferredType()

		// Update type in bindings map.
		c.bindings[t.Label] = t.Typ

	case *RefExpr:
		// Set type of ref to type of its binding or the suggested type.
		typ := c.bindings[t.Label]
		if typ == nil {
			panic(fmt.Sprintf("$%s does not have its type set", t.Label))
		}
		t.Typ = mostRestrictiveDataType(typ, suggested)

	case *AndExpr:
		// Assign most restrictive type to And expression.
		c.inferTypes(t.Left, suggested)
		c.inferTypes(t.Right, suggested)
		if DoTypesContradict(t.Left.InferredType(), t.Right.InferredType()) {
			err := fmt.Errorf("match patterns contradict one another; both cannot match")
			c.compiler.addErr(t.Source(), err)
		}
		t.Typ = mostRestrictiveDataType(t.Left.InferredType(), t.Right.InferredType())

	case *NotExpr:
		// Fall back on suggested type, since only type that doesn't match is known.
		c.inferTypes(t.Input, suggested)
		t.Typ = suggested

	case *ListExpr:
		// Assign most restrictive type to list expression.
		t.Typ = mostRestrictiveDataType(ListDataType, suggested)
		for _, item := range t.Items {
			c.inferTypes(item, AnyDataType)
		}

	case *AnyExpr:
		t.Typ = suggested

	case *StringExpr, *StringsExpr, *NumberExpr, *ListAnyExpr, *NameExpr, *NamesExpr:
		// Type already known; nothing to infer.

	default:
		panic(fmt.Sprintf("unhandled expression: %s", t))
	}
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

	// let is true when compiling in the scope of a let expression, and false
	// otherwise.
	let bool
}

func (c *ruleContentCompiler) compile(e Expr) Expr {
	// Recurse into match or construct operator separately, since they will need
	// to create new context before visiting arguments.
	switch t := e.(type) {
	case *FuncExpr:
		return c.compileFunc(t)

	case *LetExpr:
		return c.compileLet(t)

	case *BindExpr:
		return c.compileBind(t)

	case *RefExpr:
		if c.matchPattern && !c.customFunc && !c.let {
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

func (c *ruleContentCompiler) compileBind(bind *BindExpr) Expr {
	// Ensure that binding labels are unique.
	_, ok := c.compiler.bindings[bind.Label]
	if ok {
		c.addErr(bind, fmt.Errorf("duplicate bind label '%s'", bind.Label))
	}

	// Initialize binding before visiting, since it might be recursively
	// referenced, as in:
	//
	//   $input:* & (Func $input)
	//
	c.compiler.bindings[bind.Label] = AnyDataType
	newBind := bind.Visit(c.compile).(*BindExpr)

	return newBind
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

func (c *ruleContentCompiler) compileLet(let *LetExpr) Expr {
	// Create nested context and recurse into children.
	nested := ruleContentCompiler{
		compiler:     c.compiler,
		src:          let.Source(),
		matchPattern: c.matchPattern,
		let:          true,
	}

	// Target must be a CustomFunc.
	target, ok := nested.compile(let.Target).(*CustomFuncExpr)
	if !ok {
		c.addErr(let, fmt.Errorf("let target must be a custom function"))
	}

	// Ensure that binding labels are unique.
	for _, label := range let.Labels {
		_, ok := c.compiler.bindings[label]
		if ok {
			c.addErr(let, fmt.Errorf("duplicate bind label '%s'", label))
		}

		// Initialize the binding.
		c.compiler.bindings[label] = AnyDataType
	}

	labels := nested.compile(&let.Labels).(*StringsExpr)
	result := nested.compile(let.Result).(*RefExpr)

	return &LetExpr{
		Labels: *labels,
		Target: target,
		Result: result,
		Src:    let.Source(),
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
		names, ok := c.checkNames(fn)
		if !ok {
			return nil
		}

		// Normalize single name into NameExpr rather than NamesExpr.
		if len(names) == 1 {
			funcName = &names[0]
		} else {
			funcName = &names
		}

		var prototype *DefineExpr
		for _, name := range names {
			defines := c.compiler.compiled.LookupMatchingDefines(string(name))
			if defines != nil {
				// Ensure that each operator has at least as many operands as the
				// given function has arguments. The types of those arguments must
				// be the same across all the operators.
				for _, define := range defines {
					if len(define.Fields) < len(fn.Args) {
						c.addErr(fn, fmt.Errorf("%s has only %d fields", define.Name, len(define.Fields)))
						continue
					}

					if prototype == nil {
						// Save the first define in order to compare it against all
						// others.
						prototype = define
						continue
					}

					for i := range fn.Args {
						if define.Fields[i].Type != prototype.Fields[i].Type {
							c.addErr(fn, fmt.Errorf("%s and %s fields do not have same types",
								define.Name, prototype.Name))
						}
					}
				}
			} else {
				// This must be an invocation of a custom function, because there is
				// no matching define.
				if len(names) != 1 {
					c.addErr(fn, errors.New("custom function cannot have multiple names"))
					return fn
				}

				// Handle built-in functions.
				if name == "OpName" {
					opName, ok := c.compileOpName(fn)
					if ok {
						return opName
					}

					// Fall through and create OpName as a CustomFuncExpr. It may
					// be rewritten during type inference if it can be proved it
					// always constructs a single operator.
				}

				nested.customFunc = true
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

// checkNames ensures that all function names are valid operator names or tag
// names, and that they are legal in the current context. checkNames returns
// the list of names as a NameExpr, as well as a boolean indicating whether they
// passed all validity checks.
func (c *ruleContentCompiler) checkNames(fn *FuncExpr) (names NamesExpr, ok bool) {
	switch t := fn.Name.(type) {
	case *NamesExpr:
		names = *t
	case *NameExpr:
		names = NamesExpr{*t}
	default:
		// Name dynamically derived by function.
		return NamesExpr{}, false
	}

	// Don't allow replace pattern to have multiple names or a tag name.
	if !c.matchPattern {
		if len(names) != 1 {
			c.addErr(fn, errors.New("constructor cannot have multiple names"))
			return NamesExpr{}, false
		}

		defines := c.compiler.compiled.LookupMatchingDefines(string(names[0]))
		if len(defines) == 0 {
			// Must be custom function name.
			return names, true
		}

		define := c.compiler.compiled.LookupDefine(string(names[0]))
		if define == nil {
			c.addErr(fn, fmt.Errorf("construct name cannot be a tag"))
			return NamesExpr{}, false
		}
	}

	return names, true
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
	_, ok = fn.Args[0].(*RefExpr)
	if !ok {
		c.addErr(fn, fmt.Errorf("invalid OpName argument: argument must be a variable reference"))
		return fn, false
	}

	return fn, false
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

// mostRestrictiveDataType returns the more restrictive of the two data types,
// or the left data type if they are equally restrictive.
func mostRestrictiveDataType(left, right DataType) DataType {
	if IsTypeMoreRestrictive(right, left) {
		return right
	}
	return left
}
