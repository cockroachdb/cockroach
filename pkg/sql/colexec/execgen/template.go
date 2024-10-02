// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execgen

import (
	"fmt"
	"go/token"
	"regexp"
	"sort"
	"strings"

	"github.com/dave/dst"
	"github.com/dave/dst/dstutil"
)

type templateInfo struct {
	funcInfos map[string]*funcInfo

	letInfos map[string]*letInfo
}

type templateParamInfo struct {
	fieldOrdinal int
	field        *dst.Field
}

type funcInfo struct {
	decl           *dst.FuncDecl
	templateParams []templateParamInfo

	// instantiateArgs is a list of lists of arguments that were passed explicitly
	// as execgen:instantiate declarations.
	instantiateArgs [][]string
}

// letInfo contains a list of all of the values in an execgen:let declaration.
type letInfo struct {
	// typ is a type literal.
	typ  *dst.ArrayType
	vals []string
}

// Match // execgen:template<foo, bar>
var templateRe = regexp.MustCompile(`\/\/ execgen:template<((?:(?:\w+),?\W*)+)>`)

// Match // execgen:instantiate<foo, bar>
var instantiateRe = regexp.MustCompile(`\/\/ execgen:instantiate<((?:(?:\w+),?\W*)+)>`)

// replaceTemplateVars removes the template arguments from a callsite of a
// templated function. It returns the template arguments that were used, and a
// new CallExpr that doesn't have the template arguments.
func replaceTemplateVars(
	info *funcInfo, call *dst.CallExpr,
) (templateArgs []dst.Expr, newCall *dst.CallExpr, mangledName string) {
	if len(info.templateParams) == 0 {
		return nil, call, ""
	}
	templateArgs = make([]dst.Expr, len(info.templateParams))
	// Collect template arguments.
	for i, param := range info.templateParams {
		templateArgs[i] = dst.Clone(call.Args[param.fieldOrdinal]).(dst.Expr)
		// Clear the decorations so that argument comments are not used in
		// template function names.
		templateArgs[i].Decorations().Start.Clear()
		templateArgs[i].Decorations().End.Clear()
	}
	// Remove template vars from callsite.
	newArgs := make([]dst.Expr, 0, len(call.Args)-len(info.templateParams))
	for i := range call.Args {
		skip := false
		for _, p := range info.templateParams {
			if p.fieldOrdinal == i {
				skip = true
				break
			}
		}
		if !skip {
			newArgs = append(newArgs, dst.Clone(call.Args[i]).(dst.Expr))
		}
	}
	ret := dst.Clone(call).(*dst.CallExpr)
	newName := getTemplateVariantName(info, templateArgs)
	ret.Fun = newName
	ret.Args = newArgs
	return templateArgs, ret, newName.Name
}

// monomorphizeTemplate produces a variant of the input function body, given the
// definition of the function in funcInfo, and the concrete, template-time values
// that the function is being invoked with. It will try to find conditional
// statements that use the template variables and output only the branches that
// match.
//
// For example, given the function:
// // execgen:inline
// // execgen:template<t, i>
//
//	func b(t bool, i int) int {
//	  if t {
//	    x = 3
//	  } else {
//	    x = 4
//	  }
//	  switch i {
//	    case 5: fmt.Println("5")
//	    case 6: fmt.Println("6")
//	  }
//	  return x
//	}
//
// and a caller
//
//	b(true, 5)
//
// this function will generate
//
//	if true {
//	  x = 3
//	} else {
//	  x = 4
//	}
//	switch 5 {
//	  case 5: fmt.Println("5")
//	  case 6: fmt.Println("6")
//	}
//	return x
//
// in its first pass. However, because the if's condition (true, in this case)
// is a logical expression containing boolean literals, and the switch statement
// is a switch on a template variable alone, a second pass "folds"
// the conditionals and replaces them like so:
//
//	x = 3
//	fmt.Println(5)
//	return x
//
// Note that this method lexically replaces all formal parameters, so together
// with createTemplateFuncVariant, it enables templates to call other templates
// with template variables.
func monomorphizeTemplate(n dst.Node, info *funcInfo, args []dst.Expr) dst.Node {
	// Create map from formal param name to arg.
	paramMap := make(map[string]dst.Expr)
	for i, p := range info.templateParams {
		paramMap[p.field.Names[0].Name] = args[i]
	}
	templateSwitches := make(map[*dst.SwitchStmt]struct{})
	n = dstutil.Apply(n, func(cursor *dstutil.Cursor) bool {
		// Replace all usages of the formal parameter with the template arg.
		c := cursor.Node()
		switch t := c.(type) {
		case *dst.Ident:
			if arg := paramMap[t.Name]; arg != nil {
				p := cursor.Parent()
				if s, ok := p.(*dst.SwitchStmt); ok {
					if s.Tag.(*dst.Ident) == t {
						// Write down the switch statements we see that are of the form:
						// switch <templateParam> {
						// ...
						// }
						// We'll replace these later.
						templateSwitches[s] = struct{}{}
					}
				}
				cursor.Replace(dst.Clone(arg))
			}
		}
		return true
	}, nil)

	return foldConditionals(n, info, templateSwitches)
}

// foldConditionals edits conditional statements to try to remove branches that
// are statically falsifiable. It works with two cases:
//
// if <bool> { } else { } and if !<bool> { } else { }
//
// execgen:switch
//
//	switch <ident> {
//	  case <otherIdent>:
//	  case <ident>:
//	  ...
//	}
func foldConditionals(
	n dst.Node, info *funcInfo, templateSwitches map[*dst.SwitchStmt]struct{},
) dst.Node {
	return dstutil.Apply(n, func(cursor *dstutil.Cursor) bool {
		n := cursor.Node()
		switch n := n.(type) {
		case *dst.SwitchStmt:
			if _, ok := templateSwitches[n]; !ok {
				// Not a template switch.
				return true
			}
			t := prettyPrintExprs(n.Tag)
			for _, item := range n.Body.List {
				c := item.(*dst.CaseClause)
				for _, e := range c.List {
					if prettyPrintExprs(e) == t {
						body := &dst.BlockStmt{
							List: c.Body,
							Decs: dst.BlockStmtDecorations{
								NodeDecs: c.Decs.NodeDecs,
								Lbrace:   c.Decs.Colon,
							},
						}
						newBody := foldConditionals(body, info, templateSwitches).(*dst.BlockStmt)
						insertBlockStmt(cursor, newBody)
						cursor.Delete()
						return true
					}
				}
			}
		case *dst.IfStmt:
			ret, ok := tryEvalBool(n.Cond)
			if !ok {
				return true
			}
			// Since we're replacing the node, make sure we preserve any comments.
			if len(n.Decs.NodeDecs.Start) > 0 {
				cursor.InsertBefore(&dst.AssignStmt{
					Tok: token.ASSIGN,
					Lhs: []dst.Expr{dst.NewIdent("_")},
					Rhs: []dst.Expr{
						&dst.BasicLit{
							Kind:  token.STRING,
							Value: "true",
						},
					},
					Decs: dst.AssignStmtDecorations{
						NodeDecs: n.Decs.NodeDecs,
					},
				})
			}
			if ret {
				// Replace with the if side.
				newBody := foldConditionals(n.Body, info, templateSwitches).(*dst.BlockStmt)
				insertBlockStmt(cursor, newBody)
				cursor.Delete()
				return true
			}
			// Replace with the else side, if it exists.
			if n.Else != nil {
				newElse := foldConditionals(n.Else, info, templateSwitches)
				switch e := newElse.(type) {
				case *dst.BlockStmt:
					insertBlockStmt(cursor, e)
					cursor.Delete()
				default:
					cursor.Replace(newElse)
				}
			} else {
				cursor.Delete()
			}
		}
		return true
	}, nil)
}

// tryEvalBool attempts to statically evaluate the input expr as a logical
// combination of boolean literals (like false || true). It returns the result
// of the evaluation and whether or not the expression was actually evaluable
// as such.
func tryEvalBool(n dst.Expr) (ret bool, ok bool) {
	switch n := n.(type) {
	case *dst.UnaryExpr:
		// !<expr>
		if n.Op == token.NOT {
			ret, ok = tryEvalBool(n.X)
			ret = !ret
			return ret, ok
		}
		return false, false
	case *dst.BinaryExpr:
		// expr && expr or expr || expr
		if n.Op != token.LAND && n.Op != token.LOR {
			return false, false
		}
		l, ok := tryEvalBool(n.X)
		if !ok {
			return false, false
		}
		r, ok := tryEvalBool(n.Y)
		if !ok {
			return false, false
		}
		switch n.Op {
		case token.LAND:
			return l && r, true
		case token.LOR:
			return l || r, true
		default:
			panic("unreachable")
		}
	case *dst.Ident:
		switch n.Name {
		case "true":
			return true, true
		case "false":
			return false, true
		}
		return false, false
	}
	return false, false
}

func insertBlockStmt(cursor *dstutil.Cursor, block *dst.BlockStmt) {
	// Make sure to preserve comments.
	cursor.InsertBefore(&dst.EmptyStmt{
		Implicit: true,
		Decs: dst.EmptyStmtDecorations{
			NodeDecs: dst.NodeDecs{
				Before: dst.NewLine,
				Start:  trimLeadingNewLines(append(block.Decs.Lbrace, block.Decs.NodeDecs.Start...)),
				End:    block.Decs.End,
				After:  dst.NewLine,
			}},
	})
	for _, stmt := range block.List {
		cursor.InsertBefore(stmt)
	}
}

// trimTemplateDeclMatches takes a list of matches from an execgen:blah<a,b,c>
// regexp match and returns the trimmed list of a, b, and c.
func trimTemplateDeclMatches(matches []string) []string {
	match := matches[1]

	templateVars := strings.Split(match, ",")
	for i, v := range templateVars {
		templateVars[i] = strings.TrimSpace(v)
	}
	return templateVars
}

const runtimeToTemplateSuffix = "_runtime_to_template"

// findTemplateDecls, given an AST, finds all functions annotated with
// execgen:template<foo,bar>, and returns a funcInfo for each of them, and
// finds all var decls annotated with execgen:let, returning a letInfo for
// each of them.
func findTemplateDecls(f *dst.File) templateInfo {
	ret := templateInfo{
		funcInfos: make(map[string]*funcInfo),
		letInfos:  make(map[string]*letInfo),
	}

	dstutil.Apply(f, func(cursor *dstutil.Cursor) bool {
		n := cursor.Node()
		switch n := n.(type) {
		case *dst.FuncDecl:
			var templateVars []string
			var instantiateArgs [][]string
			i := 0
			for _, dec := range n.Decs.Start {
				if matches := templateRe.FindStringSubmatch(dec); matches != nil {
					templateVars = trimTemplateDeclMatches(matches)
					continue
				}

				if matches := instantiateRe.FindStringSubmatch(dec); matches != nil {
					instantiateMatches := trimTemplateDeclMatches(matches)
					newInstantiateArgs := expandInstantiateArgs(instantiateMatches, ret.letInfos)
					instantiateArgs = append(instantiateArgs, newInstantiateArgs...)
					// Eventually let's delete the instantiate comments as well.
					continue
				}
				// Filter decorations in place.
				n.Decs.Start[i] = dec
				i++
			}
			n.Decs.Start = n.Decs.Start[:i]
			if templateVars == nil {
				return false
			}

			// Process template funcs: find template params from runtime definition
			// and save in funcInfo.
			info := &funcInfo{
				instantiateArgs: instantiateArgs,
			}
			for _, v := range templateVars {
				var found bool
				for i, f := range n.Type.Params.List {
					// We can safely 0-index here because fields always have at least
					// one name, and we've already banned the case where they have more
					// than one. (e.g. func a (a int, b int, c, d int))
					if f.Names[0].Name == v {
						info.templateParams = append(info.templateParams, templateParamInfo{
							fieldOrdinal: i,
							field:        dst.Clone(f).(*dst.Field),
						})
						found = true
						break
					}
				}
				if !found {
					panic(fmt.Errorf("template var %s not found", v))
				}
			}
			// Delete template params from runtime definition.
			newParamList := make([]*dst.Field, 0, len(n.Type.Params.List)-len(info.templateParams))
			for i, field := range n.Type.Params.List {
				var skip bool
				for _, p := range info.templateParams {
					if i == p.fieldOrdinal {
						skip = true
						break
					}
				}
				if !skip {
					newParamList = append(newParamList, field)
				}
			}
			funcDecs := n.Decs
			// Replace the template function with a const marker, just so we can keep
			// the comments above the template function available.
			cursor.InsertBefore(&dst.GenDecl{
				Tok: token.CONST,
				Specs: []dst.Spec{
					&dst.ValueSpec{
						Names: []*dst.Ident{dst.NewIdent("_")},
						Values: []dst.Expr{
							&dst.BasicLit{
								Kind:  token.STRING,
								Value: fmt.Sprintf(`"template_%s"`, n.Name.Name),
							},
						},
					},
				},
				Decs: dst.GenDeclDecorations{
					NodeDecs: funcDecs.NodeDecs,
				},
			})
			oldParamList := n.Type.Params.List
			n.Type.Params.List = newParamList
			n.Decs.Start = trimStartDecs(n)
			info.decl = n
			ret.funcInfos[info.decl.Name.Name] = info

			for _, args := range info.instantiateArgs {
				exprList := make([]dst.Expr, len(args))
				for j := range args {
					exprList[j] = dst.NewIdent(args[j])
				}
				createTemplateFuncVariant(f, info, exprList)
			}

			// Now, we need to generate the "look up table" that allows us to convert
			// runtime values into template values for the template args.

			// We only do this if there were execgen:instantiate statements, since we
			// assume that if there were no such statements, the concrete callsites
			// were already present.

			if info.instantiateArgs != nil {
				runtimeArgs := make([]dst.Expr, len(n.Type.Params.List))
				for i, p := range n.Type.Params.List {
					runtimeArgs[i] = dst.NewIdent(p.Names[0].Name)
				}
				decl := &dst.FuncDecl{
					Name: dst.NewIdent(fmt.Sprintf("%s%s", info.decl.Name.Name, runtimeToTemplateSuffix)),
					Type: dst.Clone(info.decl.Type).(*dst.FuncType),
					Body: &dst.BlockStmt{
						List: []dst.Stmt{
							generateSwitchStatementLookup(info, runtimeArgs, templateVars, info.instantiateArgs),
						},
					},
				}
				decl.Type.Params.List = oldParamList
				cursor.InsertAfter(decl)
			}
			cursor.Delete()

		case *dst.GenDecl:
			// Search for execgen:let declarations.
			isLet := false
			for _, dec := range n.Decs.Start {
				if dec == "// execgen:let" {
					isLet = true
					break
				}
			}
			if !isLet {
				return true
			}
			if n.Tok != token.VAR {
				panic("execgen:let only allowed on vars")
			}
			for _, spec := range n.Specs {
				n := spec.(*dst.ValueSpec)
				if len(n.Names) != 1 || len(n.Values) != 1 {
					panic("execgen:let must have 1 name and one value per var")
				}
				info := &letInfo{}
				name := n.Names[0].Name
				c, ok := n.Values[0].(*dst.CompositeLit)
				if !ok {
					panic("execgen:let must use a composite literal value")
				}
				typ, ok := c.Type.(*dst.ArrayType)
				if !ok {
					panic("execgen:let must be on an array type literal")
				}
				info.vals = make([]string, len(c.Elts))
				info.typ = typ
				for i := range c.Elts {
					info.vals[i] = prettyPrintExprs(c.Elts[i])
				}
				ret.letInfos[name] = info
			}

			cursor.Delete()
		}
		return true
	}, nil)

	return ret
}

// expandInstantiateArgs takes a list of strings, the arguments to an
// execgen:instantiate annotation, and returns a list of list of strings, after
// combinatorially expanding any execgen:let lists in the instantiate arguments.
// For example, given the instantiateArgs:
// ["Bools", "Bools", 3]
// and an execgen:let that maps "Bools" to ["true", "false"], we'd return the
// list of lists:
// [true, true, 3]
// [true, false, 3]
// [false, true, 3]
// [false, false, 3]
func expandInstantiateArgs(instantiateArgs []string, letInfos map[string]*letInfo) [][]string {
	expandedArgs := make([][]string, len(instantiateArgs))
	for i, arg := range instantiateArgs {
		if info := letInfos[arg]; info != nil {
			expandedArgs[i] = info.vals
		} else {
			expandedArgs[i] = []string{arg}
		}
	}
	return generateInstantiateCombinations(expandedArgs)
}

func generateInstantiateCombinations(args [][]string) [][]string {
	if len(args) == 1 {
		// Base case: transform the final options list into an arguments list of
		// lists where each arguments list is a single element containing one of
		// the final options.
		// For example, given [[true, false]], we'll return:
		// [[true], [false]]
		ret := make([][]string, len(args[0]))
		for i, arg := range args[0] {
			ret[i] = []string{arg}
		}
		return ret
	}
	rest := generateInstantiateCombinations(args[1:])
	ret := make([][]string, 0, len(rest)*len(args[0]))
	for _, argOption := range args[0] {
		// For every option of argument, prepend it to every args list from
		// the recursive step.
		for _, args := range rest {
			ret = append(ret, append([]string{argOption}, args...))
		}
	}
	return ret
}

// generateSwitchStatementLookup ...
// remainingArgs is a list of lists of actual instantiations. For example, if
// we had:
// execgen:instantiate<red, potato>
// execgen:instantiate<red, orange>
// execgen:instantiate<yellow, orange>
//
// remainingArgs would be {{red, potato}, {red, orange}, {yellow orange}}
func generateSwitchStatementLookup(
	info *funcInfo, curArgs []dst.Expr, remainingTemplateParams []string, remainingArgs [][]string,
) *dst.SwitchStmt {
	ret := &dst.SwitchStmt{
		Tag:  dst.NewIdent(remainingTemplateParams[0]),
		Body: &dst.BlockStmt{},
	}
	defaultCase := &dst.CaseClause{
		Body: []dst.Stmt{
			mustParseStmt(`panic(fmt.Sprint("unknown value", ` + remainingTemplateParams[0] + `))`),
		},
	}
	if len(remainingArgs[0]) == 1 {
		// Base case. We finished switching on all template params, time to actually
		// invoke the fully specialized function.
		stmtList := make([]dst.Stmt, len(remainingArgs)+1)
		for i := range remainingArgs {
			argList := append(curArgs, dst.NewIdent(remainingArgs[i][0]))
			call := &dst.CallExpr{
				Fun:  dst.NewIdent(info.decl.Name.Name),
				Args: argList,
			}
			_, call, _ = replaceTemplateVars(info, call)
			var stmt dst.Stmt
			if info.decl.Type.Results != nil {
				stmt = &dst.ReturnStmt{Results: []dst.Expr{call}}
			} else {
				stmt = &dst.ExprStmt{X: call}
			}
			stmtList[i] = &dst.CaseClause{
				List: []dst.Expr{dst.NewIdent(remainingArgs[i][0])},
				Body: []dst.Stmt{stmt},
			}
		}
		stmtList[len(stmtList)-1] = defaultCase
		ret.Body.List = stmtList
		return ret
	}

	// Recursive case: we have more args to deal with
	groupedArgs := make(map[string][][]string)
	for _, argList := range remainingArgs {
		firstArg := argList[0]
		groupedArgs[firstArg] = append(groupedArgs[firstArg], argList[1:])
	}
	stmtList := make([]dst.Stmt, len(groupedArgs)+1)
	// Sort firstArgs lexicographically, so we have a consistent output order.
	firstArgs := make([]string, 0, len(groupedArgs))
	for firstArg := range groupedArgs {
		firstArgs = append(firstArgs, firstArg)
	}
	sort.Strings(firstArgs)

	for i, firstArg := range firstArgs {
		restArgs := groupedArgs[firstArg]
		argList := append(curArgs, dst.NewIdent(firstArg))
		stmtList[i] = &dst.CaseClause{
			List: []dst.Expr{dst.NewIdent(firstArg)},
			Body: []dst.Stmt{generateSwitchStatementLookup(
				info,
				argList,
				remainingTemplateParams[1:],
				restArgs,
			)},
		}
	}
	stmtList[len(stmtList)-1] = defaultCase
	ret.Body.List = stmtList
	return ret
}

var nameMangler = strings.NewReplacer(".", "DOT", "*", "STAR")

func getTemplateVariantName(info *funcInfo, args []dst.Expr) *dst.Ident {
	var newName strings.Builder
	newName.WriteString(info.decl.Name.Name)
	for j := range args {
		newName.WriteByte('_')
		newName.WriteString(prettyPrintExprs(args[j]))
	}
	s := newName.String()
	s = nameMangler.Replace(s)
	return dst.NewIdent(s)
}

func trimStartDecs(n *dst.FuncDecl) []string {
	// The function declaration node can accidentally capture extra comments that
	// we want to leave in their original position, and not duplicate. So, remove
	// any decorations that are separated from the function declaration by one or
	// more newlines.
	startDecs := n.Decs.Start.All()
	for i := len(startDecs) - 1; i >= 0; i-- {
		if strings.TrimSpace(startDecs[i]) == "" {
			return startDecs[i+1:]
		}
	}
	return startDecs
}

func trimLeadingNewLines(decs []string) []string {
	var i int
	for ; i < len(decs); i++ {
		if strings.TrimSpace(decs[i]) != "" {
			break
		}
	}
	return decs[i:]
}

// replaceAndExpandTemplates finds all CallExprs in the input AST that are calling
// the functions that had been annotated with // execgen:template that are
// passed in via the templateFuncInfos map. It recursively replaces the
// CallExprs with their expanded, mangled template function names, and creates
// the requisite monomorphized FuncDecls on demand.
//
// For example, given a template function:
//
// // execgen:template<b>
//
//	func foo (a int, b bool) {
//	  if b {
//	    return a
//	  } else {
//	    return a + 1
//	  }
//	}
//
// And callsites:
//
// foo(a, true)
// foo(a, false)
//
// This function will add 2 new func decls to the AST:
//
//	func foo_true(a int) {
//	  return a
//	}
//
//	func foo_false(a int) {
//	  return a + 1
//	}
func replaceAndExpandTemplates(f *dst.File, templateFuncInfos map[string]*funcInfo) dst.Node {
	// First, create the DAG of template functions. This DAG points from template
	// function to any other template functions that are called from within its
	// body that propagate template arguments.
	// First, find all "roots": template CallExprs that only have concrete
	// arguments.
	var q []*dst.CallExpr
	dstutil.Apply(f, func(cursor *dstutil.Cursor) bool {
		n := cursor.Node()
		switch n := n.(type) {
		case *dst.FuncDecl:
			q = append(q, findConcreteTemplateCallSites(n, templateFuncInfos)...)
		}
		return true
	}, nil)

	// For every remaining concrete call site, replace it with its mangled template
	// function call, and generate the requisite monomorphized template function
	// if we haven't already.
	//
	// Then, process the new monomorphized template function and add any newly
	// created concrete template call sites to the queue. Do this until we have no
	// more concrete template call sites.
	seenCallsites := make(map[string]struct{})
	for len(q) > 0 {
		q = q[:0]
		dstutil.Apply(f, func(cursor *dstutil.Cursor) bool {
			n := cursor.Node()
			switch n := n.(type) {
			case *dst.CallExpr:
				ident, ok := n.Fun.(*dst.Ident)
				if !ok {
					return true
				}
				info, ok := templateFuncInfos[ident.Name]
				if !ok {
					// Nothing to do, it's not a templated function.
					return true
				}
				// Critical moment: We need to know whether to replace with concrete
				// input args or to replace the call with the lookup version.
				if info.instantiateArgs != nil {
					n.Fun = dst.NewIdent(fmt.Sprintf("%s%s", info.decl.Name.Name, runtimeToTemplateSuffix))
					cursor.Replace(n)
					return true
				}
				templateArgs, newCall, newName := replaceTemplateVars(info, n)
				cursor.Replace(newCall)
				// Have we already replaced this template function with these args?
				funcInstance := newName + prettyPrintExprs(templateArgs...)
				if _, ok := seenCallsites[funcInstance]; !ok {
					seenCallsites[funcInstance] = struct{}{}
					newFuncVariant := createTemplateFuncVariant(f, info, templateArgs)
					q = append(q, findConcreteTemplateCallSites(newFuncVariant, templateFuncInfos)...)
				}
			}
			return true
		}, nil)
	}
	return nil
}

// findConcreteTemplateCallSites finds all CallExprs within the input funcDecl
// that do not contain template arguments and thus can be immediately replaced.
func findConcreteTemplateCallSites(
	funcDecl *dst.FuncDecl, templateFuncInfos map[string]*funcInfo,
) []*dst.CallExpr {
	info, calledFromTemplate := templateFuncInfos[funcDecl.Name.Name]
	var ret []*dst.CallExpr
	dstutil.Apply(funcDecl, func(cursor *dstutil.Cursor) bool {
		n := cursor.Node()
		switch callExpr := n.(type) {
		case *dst.CallExpr:
			ident, ok := callExpr.Fun.(*dst.Ident)
			if !ok {
				return true
			}
			_, ok = templateFuncInfos[ident.Name]
			if !ok {
				// Nothing to do, it's not a templated function.
				return true
			}
			if !calledFromTemplate {
				// All arguments are concrete since the callsite isn't within another
				// templated function decl.
				ret = append(ret, callExpr)
				return true
			}
			for i := range callExpr.Args {
				switch a := callExpr.Args[i].(type) {
				case *dst.Ident:
					for _, param := range info.templateParams {
						if param.field.Names[0].Name == a.Name {
							// Found a propagated template parameter, so we don't return
							// this CallExpr (it's not concrete).
							// NOTE: This is broken in the presence of shadowing.
							// Let's assume nobody shadows template vars for now.
							return true
						}
					}
				}
			}
			ret = append(ret, callExpr)
		}
		return true
	}, nil)
	return ret
}

// expandTemplates is the main entry point to the templater. Given a dst.File,
// it modifies the dst.File to include all expanded template functions, and
// edits call sites to call the newly expanded functions.
func expandTemplates(f *dst.File) {
	templateInfo := findTemplateDecls(f)
	replaceAndExpandTemplates(f, templateInfo.funcInfos)
}

// createTemplateFuncVariant creates a variant of the input funcInfo given the
// template arguments passed in args, and adds the variant to the end of the
// input file.
func createTemplateFuncVariant(f *dst.File, info *funcInfo, args []dst.Expr) *dst.FuncDecl {
	n := info.decl
	directives := n.Decs.NodeDecs.Start
	newBody := monomorphizeTemplate(dst.Clone(n.Body).(*dst.BlockStmt), info, args).(*dst.BlockStmt)
	newName := getTemplateVariantName(info, args)
	ret := &dst.FuncDecl{
		Name: newName,
		Type: dst.Clone(info.decl.Type).(*dst.FuncType),
		Body: newBody,
		Decs: dst.FuncDeclDecorations{
			NodeDecs: dst.NodeDecs{
				Before: dst.EmptyLine,
				Start:  directives,
			},
		},
	}
	f.Decls = append(f.Decls, ret)
	return ret
}
