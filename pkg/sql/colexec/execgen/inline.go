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

	"github.com/cockroachdb/errors"
	"github.com/dave/dst"
	"github.com/dave/dst/dstutil"
)

// inlineFuncs takes an input file's contents and inlines all functions
// annotated with // execgen:inline into their callsites via AST manipulation.
func inlineFuncs(f *dst.File) {
	// First, run over the input file, searching for functions that are annotated
	// with execgen:inline.
	inlineFuncMap := extractInlineFuncDecls(f)

	// Do a second pass over the AST, this time replacing calls to the inlined
	// functions with the inlined function itself.
	inlineFunc(inlineFuncMap, f)
}

func inlineFunc(inlineFuncMap map[string]funcInfo, n dst.Node) dst.Node {
	var funcIdx int
	return dstutil.Apply(n, func(cursor *dstutil.Cursor) bool {
		cursor.Index()
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
			funcInfo := getInlinedFunc(inlineFuncMap, callExpr)
			if funcInfo == nil {
				return true
			}
			// We want to recurse here because funcInfo itself might have calls to
			// inlined functions.
			funcInfo.decl = inlineFunc(inlineFuncMap, funcInfo.decl).(*dst.FuncDecl)

			if len(n.Rhs) > 1 {
				panic("can't do template replacement with more than a single RHS to a CallExpr")
			}

			if n.Tok == token.DEFINE {
				// We need to put a variable declaration for the new defined variables
				// in the parent scope.
				newDefinitions := &dst.GenDecl{
					Tok:   token.VAR,
					Specs: make([]dst.Spec, len(n.Lhs)),
				}

				for i, e := range n.Lhs {
					// If we had foo, bar := thingToInline(), we'd get
					// var (
					//   foo int
					//   bar int
					// )
					newDefinitions.Specs[i] = &dst.ValueSpec{
						Names: []*dst.Ident{dst.NewIdent(e.(*dst.Ident).Name)},
						Type:  dst.Clone(funcInfo.decl.Type.Results.List[i].Type).(dst.Expr),
					}
				}

				cursor.InsertBefore(&dst.DeclStmt{Decl: newDefinitions})
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
				Decs: dst.BlockStmtDecorations{
					NodeDecs: n.Decs.NodeDecs,
				},
			}
			body := dst.Clone(decl.Body).(*dst.BlockStmt)

			// Replace return statements with assignments to the return values.
			// Make a copy of the function to inline, and walk through it, replacing
			// return statements at the end of the body with assignments to the return
			// value declarations we made first.
			body = replaceReturnStatements(decl.Name.Name, funcIdx, body, func(stmt *dst.ReturnStmt) dst.Stmt {
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
			newAssignment.Tok = token.ASSIGN
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
			funcInfo := getInlinedFunc(inlineFuncMap, callExpr)
			if funcInfo == nil {
				return true
			}
			// We want to recurse here because funcInfo itself might have calls to
			// inlined functions.
			funcInfo.decl = inlineFunc(inlineFuncMap, funcInfo.decl).(*dst.FuncDecl)
			decl := funcInfo.decl

			reassignments := getFormalParamReassignments(decl, callExpr)

			// This case is simpler than the AssignStmt case. It's identical, except
			// there is no mangled return value name block, nor re-assignment to
			// the mangled returns after the inlined function.
			funcBlock := &dst.BlockStmt{
				List: []dst.Stmt{reassignments},
				Decs: dst.BlockStmtDecorations{
					NodeDecs: n.Decs.NodeDecs,
				},
			}
			body := dst.Clone(decl.Body).(*dst.BlockStmt)

			// Remove return values if there are any, since we're ignoring returns
			// as a raw function call.
			body = replaceReturnStatements(decl.Name.Name, funcIdx, body, nil)
			// Add the inlined function body to the block.
			funcBlock.List = append(funcBlock.List, body.List...)

			cursor.Replace(funcBlock)
		default:
			return true
		}
		funcIdx++
		return true
	}, nil)
}

// extractInlineFuncDecls searches the input file for functions that are
// annotated with execgen:inline, extracts them into templateFuncMap, and
// deletes them from the AST.
func extractInlineFuncDecls(f *dst.File) map[string]funcInfo {
	ret := make(map[string]funcInfo)
	dstutil.Apply(f, func(cursor *dstutil.Cursor) bool {
		n := cursor.Node()
		switch n := n.(type) {
		case *dst.FuncDecl:
			var mustInline bool
			for _, dec := range n.Decorations().Start.All() {
				if dec == "// execgen:inline" {
					mustInline = true
				}
			}
			if !mustInline {
				// Nothing to do, but recurse further.
				return true
			}
			for _, p := range n.Type.Params.List {
				if len(p.Names) > 1 {
					// If we have a definition like this:
					// func a (a, b int) int
					// We're just giving up for now out of complete laziness.
					panic("can't currently deal with multiple names per type in decls")
				}
			}

			var info funcInfo
			info.decl = dst.Clone(n).(*dst.FuncDecl)
			// Store the function in a map.
			ret[n.Name.Name] = info

			// Replace the function textually with a fake constant, such as:
			// `const _ = "inlined_blahFunc"`. We do this instead
			// of completely deleting it to prevent "important comments" above the
			// function to be deleted, such as template comments like {{end}}. This
			// is kind of a quirk of the way the comments are parsed, but nonetheless
			// this is an easy fix so we'll leave it for now.
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
	return ret
}

// extractReturnValues generates return value variables. It will produce one
// statement per return value of the input FuncDecl. For example, for
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
	return &dst.DeclStmt{
		Decl: &dst.GenDecl{
			Tok:   token.VAR,
			Specs: specs,
		},
	}, retValNames
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
// In the case where the formal parameter name is the same as the input
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
	funcName string, funcIdx int, stmt *dst.BlockStmt, returnModifier func(*dst.ReturnStmt) dst.Stmt,
) *dst.BlockStmt {
	if len(stmt.List) == 0 {
		return stmt
	}
	// Insert an explicit return at the end if there isn't one.
	// We'll need to edit this later to make early returns work properly.
	lastStmt := stmt.List[len(stmt.List)-1]
	if _, ok := lastStmt.(*dst.ReturnStmt); !ok {
		ret := &dst.ReturnStmt{}
		stmt.List = append(stmt.List, ret)
		lastStmt = ret
	}
	retStmt := lastStmt.(*dst.ReturnStmt)
	if returnModifier == nil {
		stmt.List[len(stmt.List)-1] = &dst.EmptyStmt{}
	} else {
		stmt.List[len(stmt.List)-1] = returnModifier(retStmt)
	}

	label := dst.NewIdent(fmt.Sprintf("%s_return_%d", funcName, funcIdx))

	// Find returns that weren't at the end of the function and replace them with
	// labeled gotos.
	var foundInlineReturn bool
	stmt = dstutil.Apply(stmt, func(cursor *dstutil.Cursor) bool {
		n := cursor.Node()
		switch n := n.(type) {
		case *dst.FuncLit:
			// A FuncLit is a function literal, like:
			// x := func() int { return 3 }
			// We don't recurse into function literals since the return statements
			// they contain aren't relevant to the inliner.
			return false
		case *dst.ReturnStmt:
			foundInlineReturn = true
			gotoStmt := &dst.BranchStmt{
				Tok:   token.GOTO,
				Label: dst.Clone(label).(*dst.Ident),
			}
			if returnModifier != nil {
				cursor.Replace(returnModifier(n))
				cursor.InsertAfter(gotoStmt)
			} else {
				cursor.Replace(gotoStmt)
			}
			return false
		}
		return true
	}, nil).(*dst.BlockStmt)

	if foundInlineReturn {
		// Add the label at the end.
		stmt.List = append(stmt.List,
			&dst.LabeledStmt{
				Label: label,
				Stmt:  &dst.EmptyStmt{Implicit: true},
			})
	}
	return stmt
}

// getInlinedFunc returns the corresponding FuncDecl for a CallExpr from the
// map, using the CallExpr's name to look up the FuncDecl from templateFuncs.
func getInlinedFunc(templateFuncs map[string]funcInfo, n *dst.CallExpr) *funcInfo {
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
