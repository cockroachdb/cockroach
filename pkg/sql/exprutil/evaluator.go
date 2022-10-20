// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exprutil

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// Evaluator is a utility for evaluating expressions to go types.
type Evaluator struct {
	sc *tree.SemaContext
	ec *eval.Context
	op string
}

// MakeEvaluator constructs a new Evaluator.
func MakeEvaluator(op string, sc *tree.SemaContext, ec *eval.Context) Evaluator {
	return Evaluator{op: op, sc: sc, ec: ec}
}

// StringArray evaluates expr to a string slice.
func (e Evaluator) StringArray(ctx context.Context, exprs tree.Exprs) ([]string, error) {
	strs := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		str, err := e.String(ctx, expr)
		if err != nil {
			return nil, err
		}
		strs = append(strs, str)
	}
	return strs, nil
}

// String evaluates expr to a string.
func (e Evaluator) String(ctx context.Context, expr tree.Expr) (string, error) {
	d, err := e.evalScalar(ctx, expr, types.String)
	if err != nil {
		return "", err
	}
	return string(tree.MustBeDString(d)), nil
}

// Bool evaluates expr to a bool.
func (e Evaluator) Bool(ctx context.Context, expr tree.Expr) (bool, error) {
	d, err := e.evalScalar(ctx, expr, types.Bool)
	if err != nil {
		return false, err
	}
	return bool(tree.MustBeDBool(d)), nil
}

// KVOptions evaluates the provided expressions as a string map.
func (e Evaluator) KVOptions(
	ctx context.Context, opts tree.KVOptions, optValidate KVOptionValidationMap,
) (map[string]string, error) {
	res := make(map[string]string, len(opts))
	for _, opt := range opts {
		if err := optValidate.validate(opt); err != nil {
			return nil, err
		}
		s, err := e.String(ctx, opt.Value)
		if err != nil {
			return nil, err
		}
		res[string(opt.Key)] = s
	}
	return res, nil
}

// LazyStringOrNullFunc is used to lazily evaluate a string expression.
type LazyStringOrNullFunc = func() (isNull bool, _ string, _ error)

// LazyStringOrNull will eagerly type-check the expression as a string and will
// return a function which can be invoked to evaluate the string. This is
// useful in cases where you want to type check during planning but may not
// have placeholder values populated and thus cannot evaluate the expression.
func (e Evaluator) LazyStringOrNull(
	ctx context.Context, expr tree.Expr,
) (LazyStringOrNullFunc, error) {
	if err := (Strings{expr}).typeCheck(ctx, e.op, e.sc); err != nil {
		return nil, err
	}
	return func() (isNull bool, _ string, _ error) {
		d, err := e.evalScalarMaybeNull(ctx, expr, types.String)
		if err != nil {
			return false, "", err
		}
		if d == tree.DNull {
			return true, "", nil
		}
		return false, string(tree.MustBeDString(d)), err
	}, nil
}

func (e Evaluator) evalScalar(
	ctx context.Context, expr tree.Expr, typ *types.T,
) (tree.Datum, error) {
	d, err := e.evalScalarMaybeNull(ctx, expr, typ)
	if err != nil {
		return nil, err
	}
	if d == tree.DNull {
		return nil, errors.Errorf("expected %s, got NULL", typ)
	}
	return d, nil
}

func (e Evaluator) evalScalarMaybeNull(
	ctx context.Context, expr tree.Expr, typ *types.T,
) (tree.Datum, error) {
	typedE, err := tree.TypeCheckAndRequire(ctx, expr, e.sc, typ, e.op)
	if err != nil {
		return nil, err
	}
	return eval.Expr(ctx, e.ec, typedE)
}
