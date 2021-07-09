// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execgen

import (
	"fmt"
	"go/token"
	"regexp"
	"strings"

	"github.com/dave/dst"
	"github.com/dave/dst/dstutil"
)

type templateParamInfo struct {
	fieldOrdinal int
	field        *dst.Field
}

type funcInfo struct {
	decl           *dst.FuncDecl
	templateParams []templateParamInfo
}

// Match // execgen:template<foo, bar>
var templateRe = regexp.MustCompile(`\/\/ execgen:template<((?:(?:\w+),?\W*)+)>`)

// replaceTemplateVars removes the template arguments from a callsite of a
// templated function. It returns the template arguments that were used, and a
// new CallExpr that doesn't have the template arguments.
func replaceTemplateVars(
	info *funcInfo, call *dst.CallExpr,
) (templateArgs []dst.Expr, newCall *dst.CallExpr) {
	if len(info.templateParams) == 0 {
		return nil, call
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
			newArgs = append(newArgs, call.Args[i])
		}
	}
	ret := dst.Clone(call).(*dst.CallExpr)
	ret.Args = newArgs
	return templateArgs, ret
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
// func b(t bool, i int) int {
//   if t {
//     x = 3
//   } else {
//     x = 4
//   }
//   switch i {
//     case 5: fmt.Println("5")
//     case 6: fmt.Println("6")
//   }
//   return x
// }
//
// and a caller
//   b(true, 5)
// this function will generate
//   if true {
//     x = 3
//   } else {
//     x = 4
//   }
//   switch 5 {
//     case 5: fmt.Println("5")
//     case 6: fmt.Println("6")
//   }
//   return x
//
// in its first pass. However, because the if's condition (true, in this case)
// is a logical expression containing boolean literals, and the switch statement
// is a switch on a template variable alone, a second pass "folds"
// the conditionals and replaces them like so:
//
//   x = 3
//   fmt.Println(5)
//   return x
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
// switch <ident> {
//   case <otherIdent>:
//   case <ident>:
//   ...
// }
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

// findTemplateFuncs, given an AST, finds all functions annotated with
// execgen:template<foo,bar>, and returns a funcInfo for each of them.
func findTemplateFuncs(f *dst.File) map[string]*funcInfo {
	ret := make(map[string]*funcInfo)

	dstutil.Apply(f, func(cursor *dstutil.Cursor) bool {
		n := cursor.Node()
		switch n := n.(type) {
		case *dst.FuncDecl:
			var templateVars []string
			var templateDecPosition int
			for i, dec := range n.Decorations().Start.All() {
				if matches := templateRe.FindStringSubmatch(dec); matches != nil {
					match := matches[1]
					// Match now looks like foo, bar
					templateVars = strings.Split(match, ",")
					for i, v := range templateVars {
						templateVars[i] = strings.TrimSpace(v)
					}
					templateDecPosition = i
					break
				}
			}
			if templateVars == nil {
				return false
			}
			// Remove the template decoration.
			n.Decs.Start = append(
				n.Decs.Start[:templateDecPosition],
				n.Decs.Start[templateDecPosition+1:]...)

			// Process template funcs: find template params from runtime definition
			// and save in funcInfo.
			info := &funcInfo{}
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
			n.Type.Params.List = newParamList
			n.Decs.Start = trimStartDecs(n)
			info.decl = n
			ret[info.decl.Name.Name] = info
			cursor.Delete()
		}
		return true
	}, nil)

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
// func foo (a int, b bool) {
//   if b {
//     return a
//   } else {
//     return a + 1
//   }
// }
//
// And callsites:
//
// foo(a, true)
// foo(a, false)
//
// This function will add 2 new func decls to the AST:
//
// func foo_true(a int) {
//   return a
// }
//
// func foo_false(a int) {
//   return a + 1
// }
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
				templateArgs, newCall := replaceTemplateVars(info, n)
				name := getTemplateVariantName(info, templateArgs)
				newCall.Fun = name
				cursor.Replace(newCall)
				// Have we already replaced this template function with these args?
				funcInstance := name.Name + prettyPrintExprs(templateArgs...)
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
	funcInfos := findTemplateFuncs(f)
	replaceAndExpandTemplates(f, funcInfos)
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
