// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package passesutil provides useful functionality for implementing passes.
package passesutil

import (
	"fmt"
	"go/ast"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/ast/astutil"
)

// FindContainingFile finds the file for an ast Node in a Pass.
func FindContainingFile(pass *analysis.Pass, n ast.Node) *ast.File {
	fPos := pass.Fset.File(n.Pos())
	for _, f := range pass.Files {
		if pass.Fset.File(f.Pos()) == fPos {
			return f
		}
	}
	panic(fmt.Errorf("cannot file file for %v", n))
}

// HasNolintComment returns true if the comments on the passed node have
// the comment "nolint:<nolintName>" appearing anywhere in it.
func HasNolintComment(pass *analysis.Pass, n ast.Node, nolintName string) bool {
	f := FindContainingFile(pass, n)
	relevant, containing := findNodesInBlock(f, n)
	cm := ast.NewCommentMap(pass.Fset, containing, f.Comments)
	// Check to see if any of the relevant ast nodes contain the relevant comment.
	nolintComment := "nolint:" + nolintName
	for _, cn := range relevant {
		// Ident nodes have all comments on the ident in containing. This may be
		// too many. Imagine there is a comment on the ident somewhere else in the
		// decl block, we wouldn't want that to affect a later conversion.
		//
		// We want to filter this down to all comments on the ident inside
		// the relevant statement. To do this we reject comments on idents which
		// have their slash outside the outermost relevant node. We do this
		// by inspecting the position of the comment relative to the outermost
		// relevant node. This is sound because we can't have a comment on an ident
		// alone as an ident is not a statement.
		_, isIdent := cn.(*ast.Ident)
		for _, cg := range cm[cn] {
			for _, c := range cg.List {
				if !strings.Contains(c.Text, nolintComment) {
					continue
				}
				outermost := relevant[len(relevant)-1]
				if isIdent && (cg.Pos() < outermost.Pos() || cg.End() > outermost.End()) {
					continue
				}
				return true
			}
		}
	}
	return false
}

// findNodesInBlock finds all expressions and statements that occur underneath
// the block or decl closest to n. The idea is that we want to find comments
// which occur in in the block or decl which includes the ast node n for
// filtering. We want to filter the comments down to all comments
// which are associated with n or any expression up to a statement in the
// closest encloding block or decl. This is to deal with multi-line
// expressions or with cases where the relevant expression occurs in an
// init clause of an if or for and the comment is on the preceding line.
//
// For example, imagine that n is the *ast.CallExpr on the method foo in the
// following snippet:
//
//  func nonsense() bool {
//      if v := (g.foo() + 1) > 2; !v {
//          return true
//      }
//      return false
//  }
//
// This function would return all of the nodes up to the `IfStmt` as relevant
// and would return the `BlockStmt` of the function nonsense as containing.
//
func findNodesInBlock(f *ast.File, n ast.Node) (relevant []ast.Node, containing ast.Node) {
	stack, _ := astutil.PathEnclosingInterval(f, n.Pos(), n.End())
	// Add all of the children of n to the set of relevant nodes.
	ast.Walk(funcVisitor(func(node ast.Node) {
		relevant = append(relevant, node)
	}), n)

	// Nodes were just added with the parent at the beginning and children at the
	// end. Reverse it.
	reverseNodes(relevant)

	// Add the parents up to the enclosing block or declaration block and discover
	// that containing node.
	containing = f // worst-case
	for _, n := range stack {
		switch n.(type) {
		case *ast.GenDecl, *ast.BlockStmt:
			containing = n
			return relevant, containing
		default:
			// Add all of the parents of n up to the containing BlockStmt or GenDecl
			// to the set of relevant nodes.
			relevant = append(relevant, n)
		}
	}
	return relevant, containing
}

func reverseNodes(n []ast.Node) {
	for i := 0; i < len(n)/2; i++ {
		n[i], n[len(n)-i-1] = n[len(n)-i-1], n[i]
	}
}

type funcVisitor func(node ast.Node)

var _ ast.Visitor = (funcVisitor)(nil)

func (f funcVisitor) Visit(node ast.Node) (w ast.Visitor) {
	if node != nil {
		f(node)
	}
	return f
}
