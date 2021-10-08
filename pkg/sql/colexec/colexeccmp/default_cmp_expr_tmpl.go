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

package colexeccmp

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

type cmpExprAdapterBase struct {
	fn      tree.TwoArgFn
	evalCtx *tree.EvalContext
}

// {{range .}}

type _EXPR_NAME struct {
	cmpExprAdapterBase
}

var _ ComparisonExprAdapter = &_EXPR_NAME{}

func (c *_EXPR_NAME) Eval(left, right tree.Datum) (tree.Datum, error) {
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

var _ ComparisonExprAdapter = &cmpWithSubOperatorExprAdapter{}

func (c *cmpWithSubOperatorExprAdapter) Eval(left, right tree.Datum) (tree.Datum, error) {
	return tree.EvalComparisonExprWithSubOperator(c.evalCtx, c.expr, left, right)
}
