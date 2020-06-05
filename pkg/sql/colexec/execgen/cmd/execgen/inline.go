// Copyright 2020 The Cockroach Authors.
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
	"go/parser"
	"go/token"

	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/fmtsafe/testdata/src/github.com/cockroachdb/errors"
	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/dave/dst/dstutil"
)

func inlineFuncs(inputFile string) (string, error) {
	f, err := decorator.ParseFile(token.NewFileSet(), "", inputFile, parser.ParseComments)
	if err != nil {
		return "", err
	}

	templateFuncMap := make(map[string]*dst.FuncDecl)

	dstutil.Apply(f, func(cursor *dstutil.Cursor) bool {
		n := cursor.Node()
		switch n := n.(type) {
		case *dst.FuncDecl:
			var mustInline bool
			for _, dec := range n.Decorations().Start.All() {
				if dec == "// execgen:inline" {
					mustInline = true
					break
				}
			}
			if !mustInline {
				// Nothing to do, but recurse further.
				return true
			}
			for _, p := range n.Type.Params.List {
				if len(p.Names) > 1 {
					panic("can't currently deal with multiple names per type in decls")
				}
			}
			templateFuncMap[n.Name.Name] = n
			cursor.Replace(&dst.GenDecl{
				Tok: token.CONST,
				Specs: []dst.Spec{
					&dst.ValueSpec{
						Names: []*dst.Ident{dst.NewIdent("_")},
						Values: []dst.Expr{
							&dst.BasicLit{
								Kind:  token.STRING,
								Value: `"inlined"`,
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

	dstutil.Apply(f, func(cursor *dstutil.Cursor) bool {
		n := cursor.Node()
		switch n := n.(type) {
		case *dst.AssignStmt:
			// Search for assignment function call:
			// a = foo()
			if len(n.Rhs) > 1 {
				// Can't do template replacement with more than one RHS yet.
				return true
			}
			callExpr, ok := n.Rhs[0].(*dst.CallExpr)
			if !ok {
				return true
			}
			decl := getTemplateFunc(templateFuncMap, callExpr)
			if decl == nil {
				return true
			}
			// Now we've got a callExpr. We need to inline the function call, and
			// convert the result into the assignment variable.

			// Generate return value variables.
			results := decl.Type.Results.List
			retVals := make([]dst.Stmt, len(results))
			retValNames := make([]string, len(results))
			for i, result := range results {
				var retvalName string
				// Make a mangled name.
				if len(result.Names) == 0 {
					retvalName = fmt.Sprintf("__retval_%d", i)
				} else {
					retvalName = fmt.Sprintf("__retval_%s", result.Names[0])
				}
				retValNames[i] = retvalName
				retVals[i] = &dst.DeclStmt{
					Decl: &dst.GenDecl{
						Tok: token.VAR,
						Specs: []dst.Spec{
							&dst.ValueSpec{
								Names: []*dst.Ident{dst.NewIdent(retvalName)},
								Type:  dst.Clone(result.Type).(dst.Expr),
							},
						},
					},
				}
			}
			body := dst.Clone(decl.Body).(*dst.BlockStmt)
			// Replace return statements with assignments to the return values.
			var seenReturn bool
			dstutil.Apply(body, func(cursor *dstutil.Cursor) bool {
				if seenReturn {
					panic(fmt.Errorf("can't inline function %s: return not at end of body", decl.Name))
				}
				n := cursor.Node()
				switch n := n.(type) {
				case *dst.FuncLit:
					// Don't recurse into functions since they can contain out of band
					// return statements.
					return false
				case *dst.ReturnStmt:
					seenReturn = true
					// Replace the return statement with an assignment to the mangled
					// names.
					if len(n.Results) != len(retVals) {
						panic(errors.Newf("invalid return value in %s: %d returns, expected %d",
							decl.Name, len(n.Results), len(retVals)))
					}
					returnAssignments := make([]dst.Stmt, len(retVals))
					for i := range retVals {
						returnAssignments[i] = &dst.AssignStmt{
							Lhs: []dst.Expr{dst.NewIdent(retValNames[i])},
							Tok: token.ASSIGN,
							Rhs: []dst.Expr{n.Results[i]},
						}
					}
					cursor.Replace(&dst.BlockStmt{List: returnAssignments})
					return false
				}
				return true
			}, nil)
			outerBlock := &dst.BlockStmt{List: retVals}
			// Reassign input parameters to formal parameters.
			reassignments := getFormalParamReassignments(decl, callExpr)
			outerBlock.List = append(outerBlock.List, &dst.BlockStmt{
				List: append(reassignments, body.List...),
			})
			// Assign mangled return values to the original assignment variables.
			newAssignment := dst.Clone(n).(*dst.AssignStmt)
			newAssignment.Rhs = make([]dst.Expr, len(retValNames))
			for i := range retValNames {
				newAssignment.Rhs[i] = dst.NewIdent(retValNames[i])
			}
			outerBlock.List = append(outerBlock.List, newAssignment)
			cursor.Replace(outerBlock)

		case *dst.ExprStmt:
			// Search for raw function call:
			// foo()
			callExpr, ok := n.X.(*dst.CallExpr)
			if !ok {
				return true
			}
			decl := getTemplateFunc(templateFuncMap, callExpr)
			if decl == nil {
				return true
			}

			reassignments := getFormalParamReassignments(decl, callExpr)

			funcBlock := &dst.BlockStmt{
				List: reassignments,
			}
			body := dst.Clone(decl.Body)

			// Remove return values if there are any, since we're ignoring returns
			// as a raw function call.
			dstutil.Apply(body, func(cursor *dstutil.Cursor) bool {
				n := cursor.Node()
				switch n.(type) {
				case *dst.FuncLit:
					// Don't recurse into functions since they can contain out of band
					// return statements.
					return false
				case *dst.ReturnStmt:
					cursor.Delete()
				}
				return true
			}, nil)
			// No return values.
			funcBlock.List = append(funcBlock.List, body.(*dst.BlockStmt).List...)

			cursor.Replace(funcBlock)
		}
		return true
	}, nil)

	b := bytes.Buffer{}
	decorator.Fprint(&b, f)
	return b.String(), nil
}

func getFormalParamReassignments(decl *dst.FuncDecl, callExpr *dst.CallExpr) []dst.Stmt {
	formalParams := decl.Type.Params.List
	reassignments := make([]dst.Stmt, 0, len(formalParams))
	for i, formalParam := range formalParams {
		if inputIdent, ok := callExpr.Args[i].(*dst.Ident); ok && inputIdent.Name == formalParam.Names[0].Name {
			continue
		}
		reassignments = append(reassignments, &dst.AssignStmt{
			Lhs: []dst.Expr{
				dst.NewIdent(formalParam.Names[0].Name),
			},
			Tok: token.DEFINE,
			Rhs: []dst.Expr{callExpr.Args[i]},
		})
	}
	return reassignments
}

func getTemplateFunc(templateFuncs map[string]*dst.FuncDecl, n *dst.CallExpr) *dst.FuncDecl {
	ident, ok := n.Fun.(*dst.Ident)
	if !ok {
		return nil
	}

	decl, ok := templateFuncs[ident.Name]
	if !ok {
		return nil
	}
	if decl.Type.Params.NumFields() != len(n.Args) {
		panic(errors.Newf(
			"%s expected %d arguments, found %d",
			decl.Name, decl.Type.Params.NumFields(), len(n.Args)),
		)
	}
	return decl
}
