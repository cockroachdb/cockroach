// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import "github.com/cockroachdb/cockroach/pkg/util"

// Visitor to check if an expression contains values that would be scrubbed
// for statement fingerprinting: e.g. literals, placeholders, tuples or arrays
// containing only literals or placeholders.
type exprCollapsibleVisitor struct {
	containsInterestingExpr bool
}

var _ Visitor = &exprCollapsibleVisitor{}

func (v *exprCollapsibleVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	switch expr.(type) {
	case Datum, Constant, *Placeholder:
		return false, expr
	case *Tuple, *Array, *CastExpr:
		return true, expr
	default:
		v.containsInterestingExpr = true
		return false, expr
	}
}

func isExprCollapsible(expr Expr) bool {
	v := exprCollapsibleVisitor{}
	WalkExprConst(&v, expr)
	return !v.containsInterestingExpr
}

func (v *exprCollapsibleVisitor) VisitPost(expr Expr) Expr { return expr }

type exprOnlyNamesVisitor struct {
	hash          util.FNV64
	hasOtherExprs bool
}

var _ Visitor = &exprOnlyNamesVisitor{}

func (v *exprOnlyNamesVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	switch e := expr.(type) {
	case *UnresolvedName:
		for _, p := range e.Parts {
			v.hash.AddString(p)
		}
		return false, expr
	case *ColumnAccessExpr:
		v.hash.AddString(string(e.ColName))
		return true, expr
	case *CastExpr:
		return true, expr
	default:
		v.hasOtherExprs = true
		return false, expr
	}
}

func (v *exprOnlyNamesVisitor) VisitPost(expr Expr) Expr { return expr }

func isExprOnlyNames(expr Expr) (bool, uint64) {
	v := exprOnlyNamesVisitor{hash: util.MakeFNV64()}
	WalkExprConst(&v, expr)
	return !v.hasOtherExprs, v.hash.Sum()
}
