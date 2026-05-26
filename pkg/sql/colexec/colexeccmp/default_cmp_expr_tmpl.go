// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// {{/*
//go:build execgen_template

//
// This file is the execgen template for default_cmp_expr.eg.go. It's
// formatted in a special way, so it's both valid Go and a valid text/template
// input. This permits editing this file with editor support.
//
// */}}

package colexeccmp

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

type cmpExprAdapterBase struct {
	op      tree.BinaryEvalOp
	evalCtx *eval.Context
}

// {{range .}}

type _EXPR_NAME struct {
	cmpExprAdapterBase
}

var _ ComparisonExprAdapter = &_EXPR_NAME{}

func (c *_EXPR_NAME) Eval(ctx context.Context, left, right tree.Datum) (tree.Datum, error) {
	// {{if not .CalledOnNullInput}}
	if left == tree.DNull || right == tree.DNull {
		return tree.DNull, nil
	}
	// {{end}}
	// {{if .FlippedArgs}}
	left, right = right, left
	// {{end}}
	d, err := eval.BinaryOp(ctx, c.evalCtx, c.op, left, right)
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

func (c *cmpWithSubOperatorExprAdapter) Eval(
	ctx context.Context, left, right tree.Datum,
) (tree.Datum, error) {
	return eval.ComparisonExprWithSubOperator(ctx, c.evalCtx, c.expr, left, right)
}
