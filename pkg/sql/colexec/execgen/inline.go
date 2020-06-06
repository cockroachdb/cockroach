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
	"bytes"
	"fmt"
	"go/parser"
	"go/token"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/dave/dst/dstutil"
)

// InlineFuncs takes an input file's contents and inlines all functions
// annotated with // execgen:inline into their callsites via AST manipulation.
func InlineFuncs(inputFileContents string) (string, error) {
	f, err := decorator.ParseFile(token.NewFileSet(), "", inputFileContents, parser.ParseComments)
	if err != nil {
		return "", err
	}

	templateFuncMap := make(map[string]funcInfo)

	// First, run over the input file, searching for functions that are annotated
	// with execgen:inline.
	n := extractInlineFuncDecls(f, templateFuncMap)

	// Do a second pass over the AST, this time replacing calls to the inlined
	// functions with the inlined function itself.
	dstutil.Apply(n, func(cursor *dstutil.Cursor) bool {
		n := cursor.Node()
		// There are two cases. AssignStmt, which are like:
		// a = foo()
		// and ExprStmt, which are simply:
		// foo()
		// AssignStmts need to do extra work for inlining, because we have to
		// simulate producing return values.
		switch n := n.(type) {
		case *dst.AssignStmt:
			// Search for assignment function call:
			// a = foo()
			callExpr, ok := n.Rhs[0].(*dst.CallExpr)
			if !ok {
				return true
			}
			funcInfo := getTemplateFunc(templateFuncMap, callExpr)
			if funcInfo == nil {
				return true
			}
			if len(n.Rhs) > 1 {
				panic("can't do template replacement with more than a single RHS to a CallExpr")
			}

			// Now we've got a callExpr. We need to inline the function call, and
			// convert the result into the assignment variable.

			decl := funcInfo.decl
			// Produce declarations for each return value of the function to inline.
			retValDeclStmt, retValNames := extractReturnValues(decl)
			// inlinedStatements is a BlockStmt (a set of statements within curly
			// braces) that contains the entirety of the statements that result from
			// inlining the call. We make this a BlockStmt to avoid issues with
			// variable shadowing.
			// The first thing that goes in the BlockStmt is the ret val declarations.
			// When we're done, the BlockStmt for a statement
			//    a, b = foo(x, y)
			// where foo was defined as
			//    func foo(b string, c string) { ... }
			// will look like:
			// {
			//    var (
			//     __retval_0 bool
			//     __retval_1 int
			//    )
			//    ...
			//    {
			//       b := x
			//       c := y
			//       ... the contents of func foo() except its return ...
			//       {
			//          // If foo() had `return true, j`, we'll generate the code:
			//          __retval_0 = true
			//          __retval_1 = j
			//       }
			//    }
			//    a   = __retval_0
			//    b   = __retval_1
			// }
			inlinedStatements := &dst.BlockStmt{
				List: []dst.Stmt{retValDeclStmt},
			}
			body := dst.Clone(decl.Body).(*dst.BlockStmt)

			// Replace template vars.
			templateArgs, callExpr := replaceTemplateVars(funcInfo, callExpr)

			// Replace template conditionals.
			body = monomorphizeTemplate(body, funcInfo, templateArgs)

			// Replace return statements with assignments to the return values.
			// Make a copy of the function to inline, and walk through it, replacing
			// return statements at the end of the body with assignments to the return
			// value declarations we made first.
			body = replaceReturnStatements(decl.Name.Name, body, func(stmt *dst.ReturnStmt) dst.Stmt {
				returnAssignmentSpecs := make([]dst.Stmt, len(retValNames))
				for i := range retValNames {
					returnAssignmentSpecs[i] = &dst.AssignStmt{
						Lhs: []dst.Expr{dst.NewIdent(retValNames[i])},
						Tok: token.ASSIGN,
						Rhs: []dst.Expr{stmt.Results[i]},
					}
				}
				// Replace the return with the new assignments.
				return &dst.BlockStmt{List: returnAssignmentSpecs}
			})
			// Reassign input parameters to formal parameters.
			reassignmentStmt := getFormalParamReassignments(decl, callExpr)
			inlinedStatements.List = append(inlinedStatements.List, &dst.BlockStmt{
				List: append([]dst.Stmt{reassignmentStmt}, body.List...),
			})
			// Assign mangled return values to the original assignment variables.
			newAssignment := dst.Clone(n).(*dst.AssignStmt)
			newAssignment.Rhs = make([]dst.Expr, len(retValNames))
			for i := range retValNames {
				newAssignment.Rhs[i] = dst.NewIdent(retValNames[i])
			}
			inlinedStatements.List = append(inlinedStatements.List, newAssignment)
			cursor.Replace(inlinedStatements)

		case *dst.ExprStmt:
			// Search for raw function call:
			// foo()
			callExpr, ok := n.X.(*dst.CallExpr)
			if !ok {
				return true
			}
			funcInfo := getTemplateFunc(templateFuncMap, callExpr)
			if funcInfo == nil {
				return true
			}
			decl := funcInfo.decl

			reassignments := getFormalParamReassignments(decl, callExpr)

			// This case is simpler than the AssignStmt case. It's identical, except
			// there is no mangled return value name block, nor re-assignment to
			// the mangled returns after the inlined function.
			funcBlock := &dst.BlockStmt{
				List: []dst.Stmt{reassignments},
			}
			body := dst.Clone(decl.Body).(*dst.BlockStmt)

			// Replace template vars.
			templateArgs, callExpr := replaceTemplateVars(funcInfo, callExpr)

			// Replace template conditionals.
			body = monomorphizeTemplate(body, funcInfo, templateArgs)

			// Remove return values if there are any, since we're ignoring returns
			// as a raw function call.
			body = replaceReturnStatements(decl.Name.Name, body, nil)
			// Add the inlined function body to the block.
			funcBlock.List = append(funcBlock.List, body.List...)

			cursor.Replace(funcBlock)
		}
		return true
	}, nil)

	b := bytes.Buffer{}
	_ = decorator.Fprint(&b, f)
	return b.String(), nil
}

func monomorphizeTemplate(body *dst.BlockStmt, info *funcInfo, args []dst.Expr) *dst.BlockStmt {
	return dstutil.Apply(body, func(cursor *dstutil.Cursor) bool {
		n := cursor.Node()
		switch n := n.(type) {
		case *dst.IfStmt:
			switch c := n.Cond.(type) {
			case *dst.Ident:
				for i, p := range info.templateParams {
					if ident, ok := p.field.Type.(*dst.Ident); !ok || ident.Name != "bool" {
						// Can only template bool types right now.
						continue
					}
					if c.Name == p.field.Names[0].Name {
						if ident, ok := args[i].(*dst.Ident); ok {
							if ident.Name == "true" {
								for _, stmt := range n.Body.List {
									cursor.InsertAfter(stmt)
								}
								cursor.Delete()
								return true
							} else if n.Else != nil {
								switch e := n.Else.(type) {
								case *dst.BlockStmt:
									for _, stmt := range e.List {
										cursor.InsertAfter(stmt)
									}
									cursor.Delete()
								default:
									cursor.Replace(n.Else)
								}
								return true
							} else {
								cursor.Delete()
							}
						}
					}
				}
			}
		}

		return true
	}, nil).(*dst.BlockStmt)
}

// extractInlineFuncDecls searches the input file for functions that are
// annotated with execgen:inline, extracts them into templateFuncMap, and
// deletes them from the AST.
func extractInlineFuncDecls(f *dst.File, templateFuncMap map[string]funcInfo) dst.Node {
	return dstutil.Apply(f, func(cursor *dstutil.Cursor) bool {
		n := cursor.Node()
		switch n := n.(type) {
		case *dst.FuncDecl:
			var mustInline bool
			var templateVars []string
			for _, dec := range n.Decorations().Start.All() {
				if dec == "// execgen:inline" {
					mustInline = true
				}
				if matches := templateRe.FindStringSubmatch(dec); matches != nil {
					match := matches[1]
					// Match now looks like foo, bar
					templateVars = strings.Split(match, ",")
					for i, v := range templateVars {
						templateVars[i] = strings.TrimSpace(v)
					}
				}
			}
			if !mustInline {
				if templateVars != nil {
					panic("can't currently template without inlining")
				}
				// Nothing to do, but recurse further.
				return true
			}
			for _, p := range n.Type.Params.List {
				if len(p.Names) > 1 {
					panic("can't currently deal with multiple names per type in decls")
				}
			}

			// Process template funcs: find template params from runtime definition
			// and save in funcInfo.
			var info funcInfo
			for _, v := range templateVars {
				var found bool
				for i, f := range n.Type.Params.List {
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
			info.decl = n

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
			n.Type.Params.List = newParamList

			// Store the function in a map.
			templateFuncMap[n.Name.Name] = info
			// Replace the function textually with a fake constant, such as:
			// `const _ = "inlined_blahFunc"`. We do this instead
			// of completely deleting it to prevent "important comments" above the
			// function to be deleted, such as template comments like {{end}}. This
			// is kind of a quirk of the way the comments are parsed, but nonetheless
			// this is an easy fix so we'll leave it for now.
			// TODO(jordan): We can't delete this until later, because template funcs
			// can call other template funcs and we have to propagate the static
			// template values... we really need to make a graph.
			cursor.Replace(&dst.GenDecl{
				Tok: token.CONST,
				Specs: []dst.Spec{
					&dst.ValueSpec{
						Names: []*dst.Ident{dst.NewIdent("_")},
						Values: []dst.Expr{
							&dst.BasicLit{
								Kind:  token.STRING,
								Value: fmt.Sprintf(`"inlined_%s"`, n.Name.Name),
							},
						},
					},
				},
				Decs: dst.GenDeclDecorations{
					NodeDecs: n.Decs.NodeDecs,
				},
			})
			return false
		}
		return true
	}, nil)
}

// extractReturnValues generates return value variables. It will produce one
// statement per return return value of the input FuncDecl. For example, for
// a FuncDecl that returns two boolean arguments, lastVal and lastValNull,
// two statements will be returned:
//	var __retval_lastVal bool
//	var __retval_lastValNull bool
// The second return is a slice of the names of each of the mangled return
// declarations, in this example, __retval_lastVal and __retval_lastValNull.
func extractReturnValues(decl *dst.FuncDecl) (retValDeclStmt dst.Stmt, retValNames []string) {
	if decl.Type.Results == nil {
		return &dst.EmptyStmt{}, nil
	}
	results := decl.Type.Results.List
	retValNames = make([]string, len(results))
	specs := make([]dst.Spec, len(results))
	for i, result := range results {
		var retvalName string
		// Make a mangled name.
		if len(result.Names) == 0 {
			retvalName = fmt.Sprintf("__retval_%d", i)
		} else {
			retvalName = fmt.Sprintf("__retval_%s", result.Names[0])
		}
		retValNames[i] = retvalName
		specs[i] = &dst.ValueSpec{
			Names: []*dst.Ident{dst.NewIdent(retvalName)},
			Type:  dst.Clone(result.Type).(dst.Expr),
		}
	}
	retVal := &dst.DeclStmt{
		Decl: &dst.GenDecl{
			Tok:   token.VAR,
			Specs: specs,
		},
	}
	return retVal, retValNames
}

// getFormalParamReassignments creates a new DEFINE (:=) statement per parameter
// to a FuncDecl, which makes a fresh variable with the same name as the formal
// parameter name and assigns it to the corresponding name in the CallExpr.
//
// For example, given a FuncDecl:
//
// func foo(a int, b string) { ... }
//
// and a CallExpr
//
// foo(x, y)
//
// we'll return the statement:
//
// var (
//   a int = x
//   b string = y
// )
//
// In the case where the formal parameter name is the same as the the input
// parameter name, no extra assignment is created.
func getFormalParamReassignments(decl *dst.FuncDecl, callExpr *dst.CallExpr) dst.Stmt {
	formalParams := decl.Type.Params.List
	reassignmentSpecs := make([]dst.Spec, 0, len(formalParams))
	for i, formalParam := range formalParams {
		if inputIdent, ok := callExpr.Args[i].(*dst.Ident); ok && inputIdent.Name == formalParam.Names[0].Name {
			continue
		}
		reassignmentSpecs = append(reassignmentSpecs, &dst.ValueSpec{
			Names:  []*dst.Ident{dst.NewIdent(formalParam.Names[0].Name)},
			Type:   dst.Clone(formalParam.Type).(dst.Expr),
			Values: []dst.Expr{callExpr.Args[i]},
		})
	}
	if len(reassignmentSpecs) == 0 {
		return &dst.EmptyStmt{}
	}
	return &dst.DeclStmt{
		Decl: &dst.GenDecl{
			Tok:   token.VAR,
			Specs: reassignmentSpecs,
		},
	}
}

// replaceReturnStatements edits the input BlockStmt, from the function funcName,
// replacing ReturnStmts at the end of the BlockStmts with the results of
// applying returnEditor on the ReturnStmt or deleting them if the modifier is
// nil.
// It will panic if any return statements are not in the final position of the
// input block.
func replaceReturnStatements(
	funcName string, stmt *dst.BlockStmt, returnModifier func(*dst.ReturnStmt) dst.Stmt,
) *dst.BlockStmt {
	// Remove return values if there are any, since we're ignoring returns
	// as a raw function call.
	var seenReturn bool
	return dstutil.Apply(stmt, func(cursor *dstutil.Cursor) bool {
		if seenReturn {
			panic(fmt.Errorf("can't inline function %s: return not at end of body (found %s)", funcName, cursor.Node()))
		}
		n := cursor.Node()
		switch t := n.(type) {
		case *dst.FuncLit:
			// A FuncLit is a function literal, like:
			// x := func() int { return 3 }
			// We don't recurse into function literals since the return statements
			// they contain aren't relevant to the inliner.
			return false
		case *dst.ReturnStmt:
			seenReturn = true
			if returnModifier == nil {
				cursor.Delete()
				return false
			}
			cursor.Replace(returnModifier(t))
			return false
		}
		return true
	}, nil).(*dst.BlockStmt)
}

// getTemplateFunc returns the corresponding FuncDecl for a CallExpr from the
// map, using the CallExpr's name to look up the FuncDecl from templateFuncs.
func getTemplateFunc(templateFuncs map[string]funcInfo, n *dst.CallExpr) *funcInfo {
	ident, ok := n.Fun.(*dst.Ident)
	if !ok {
		return nil
	}

	info, ok := templateFuncs[ident.Name]
	if !ok {
		return nil
	}
	decl := info.decl
	if decl.Type.Params.NumFields()+len(info.templateParams) != len(n.Args) {
		panic(errors.Newf(
			"%s expected %d arguments, found %d",
			decl.Name, decl.Type.Params.NumFields(), len(n.Args)),
		)
	}
	return &info
}
