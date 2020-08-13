// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// {{/*
// +build execgen_template
//
// This file is the execgen template for default_cmp_expr.eg.go. It's
// formatted in a special way, so it's both valid Go and a valid text/template
// input. This permits editing this file with editor support.
//
// */}}

package colexec

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// comparisonExprAdapter is a utility interface that is implemented by several
// structs that behave as an adapter from tree.ComparisonExpr to a vectorized
// friendly model.
type comparisonExprAdapter interface {
	eval(left, right tree.Datum) (tree.Datum, error)
}

func newComparisonExprAdapter(
	expr *tree.ComparisonExpr, evalCtx *tree.EvalContext,
) comparisonExprAdapter {
	base := cmpExprAdapterBase{
		fn:      expr.Fn.Fn,
		evalCtx: evalCtx,
	}
	op := expr.Operator
	if op.HasSubOperator() {
		return &cmpWithSubOperatorExprAdapter{
			cmpExprAdapterBase: base,
			expr:               expr,
		}
	}
	nullable := expr.Fn.NullableArgs
	_, _, _, flipped, negate := tree.FoldComparisonExpr(op, nil /* left */, nil /* right */)
	if nullable {
		if flipped {
			if negate {
				return &cmpNullableFlippedNegateExprAdapter{cmpExprAdapterBase: base}
			}
			return &cmpNullableFlippedExprAdapter{cmpExprAdapterBase: base}
		} else {
			if negate {
				return &cmpNullableNegateExprAdapter{cmpExprAdapterBase: base}
			}
			return &cmpNullableExprAdapter{cmpExprAdapterBase: base}
		}
	}
	if flipped {
		if negate {
			return &cmpFlippedNegateExprAdapter{cmpExprAdapterBase: base}
		}
		return &cmpFlippedExprAdapter{cmpExprAdapterBase: base}
	}
	if negate {
		return &cmpNegateExprAdapter{cmpExprAdapterBase: base}
	}
	return &cmpExprAdapter{cmpExprAdapterBase: base}
}

type cmpExprAdapterBase struct {
	fn      tree.TwoArgFn
	evalCtx *tree.EvalContext
}

// {{range .}}

type _EXPR_NAME struct {
	cmpExprAdapterBase
}

var _ comparisonExprAdapter = &_EXPR_NAME{}

func (c *_EXPR_NAME) eval(left, right tree.Datum) (tree.Datum, error) {
	// {{if not .NullableArgs}}
	if left == tree.DNull || right == tree.DNull {
		return tree.DNull, nil
	}
	// {{end}}
	// {{if .FlippedArgs}}
	left, right = right, left
	// {{end}}
	d, err := c.fn(c.evalCtx, left, right)
	if d == tree.DNull || err != nil {
		return d, err
	}
	b, ok := d.(*tree.DBool)
	if !ok {
		return nil, errors.AssertionFailedf("%v is %T and not *DBool", d, d)
	}
	// {{if .Negate}}
	result := tree.MakeDBool(!*b)
	// {{else}}
	result := tree.MakeDBool(*b)
	// {{end}}
	return result, nil
}

// {{end}}

type cmpWithSubOperatorExprAdapter struct {
	cmpExprAdapterBase
	expr *tree.ComparisonExpr
}

var _ comparisonExprAdapter = &cmpWithSubOperatorExprAdapter{}

func (c *cmpWithSubOperatorExprAdapter) eval(left, right tree.Datum) (tree.Datum, error) {
	return tree.EvalComparisonExprWithSubOperator(c.evalCtx, c.expr, left, right)
}
