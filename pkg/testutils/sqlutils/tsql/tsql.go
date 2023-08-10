// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package tsql (tree SQL) is a collection of helper functions to build SQL
// queries using the tree package.
//
// It is intended to aid in making legible and composable query generators. It
// should only be utilized within tests as it will panic to create a more
// pleasant UX.
package tsql

import (
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins" // Registers builtin functions.
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/errors"
)

var (
	GT = treecmp.MakeComparisonOperator(treecmp.GT)
	GE = treecmp.MakeComparisonOperator(treecmp.GE)
	LT = treecmp.MakeComparisonOperator(treecmp.LT)
	LE = treecmp.MakeComparisonOperator(treecmp.LE)
)

// ToString is a convenience method to convert a tree.NodeFormatter to a
// string.
func ToString(e tree.NodeFormatter) string {
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	e.Format(fmtCtx)
	return fmtCtx.CloseAndGetString()
}

// Func returns a *tree.FuncExpr representing a the given function and
// arguments. Usage:
//
//	Func("levenshtein", "str1", "str2")
func Func(name string, args ...any) *tree.FuncExpr {
	exprs := make([]tree.Expr, len(args))
	for i, arg := range args {
		exprs[i] = toExpr(arg)
	}
	return &tree.FuncExpr{
		Func:  tree.WrapFunction(name),
		Exprs: exprs,
	}
}

// Cmp is a convenience method to generate *tree.ComparisonExprs.
func Cmp(left tree.Expr, op treecmp.ComparisonOperator, right tree.Expr) *tree.ComparisonExpr {
	return &tree.ComparisonExpr{
		Operator: op,
		Left:     left,
		Right:    right,
	}
}

// And foldl's the given expressions by combining them with tree.AndExpr's. If
// exprs is empty, nil is returned. If exprs has a single element, it will be
// returned unmodified.
func And(exprs ...tree.Expr) tree.Expr {
	if len(exprs) == 0 {
		return nil
	}

	acc := exprs[0]
	for i := range exprs[1:] {
		acc = &tree.AndExpr{
			Left:  acc,
			Right: exprs[i+1],
		}
	}
	return acc
}

func Array(values ...any) tree.Expr {
	exprs := make([]tree.Expr, len(values))
	for i := range values {
		exprs[i] = toExpr(values[i])
	}
	return &tree.Array{Exprs: exprs}
}

func toExpr(val any) tree.Expr {
	switch e := val.(type) {
	case tree.Expr:
		return e

	case string:
		return tree.NewDString(e)

	case int:
		return tree.NewDInt(tree.DInt(e))

	case bool:
		if e {
			return tree.DBoolTrue
		}
		return tree.DBoolFalse

	default:
		panic(errors.AssertionFailedf("unhandled type: %T", val))
	}
}
