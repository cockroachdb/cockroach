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
	return string(*d.(*tree.DString)), nil
}

// Bool evaluates expr to a bool.
func (e Evaluator) Bool(ctx context.Context, expr tree.Expr) (bool, error) {
	d, err := e.evalScalar(ctx, expr, types.Bool)
	if err != nil {
		return false, err
	}
	return bool(*d.(*tree.DBool)), nil
}

// KVOptions evaluates the provided expressions as a string map.
func (e Evaluator) KVOptions(
	ctx context.Context, opts tree.KVOptions, optValidate KVOptionValidationMap,
) (map[string]string, error) {
	res := make(map[string]string, len(opts))
	for _, opt := range opts {
		k := string(opt.Key)
		validate, ok := optValidate[k]
		if !ok {
			return nil, errors.Errorf("invalid option %q", k)
		}
		if opt.Value == nil {
			if validate == KVStringOptRequireValue {
				return nil, errors.Errorf("option %q requires a value", k)
			}
			res[k] = ""
			continue
		}
		if validate == KVStringOptRequireNoValue {
			return nil, errors.Errorf("option %q does not take a value", k)
		}
		s, err := e.String(ctx, opt.Value)
		if err != nil {
			return nil, err
		}
		res[k] = s
	}
	return res, nil
}

func (e Evaluator) evalScalar(
	ctx context.Context, expr tree.Expr, typ *types.T,
) (tree.Datum, error) {
	typedE, err := tree.TypeCheckAndRequire(ctx, expr, e.sc, typ, e.op)
	if err != nil {
		return nil, err
	}
	d, err := eval.Expr(ctx, e.ec, typedE)
	if err != nil {
		return nil, err
	}
	if d == tree.DNull {
		return nil, errors.Errorf("expected %s, got NULL", typ)
	}
	return d, nil
}
