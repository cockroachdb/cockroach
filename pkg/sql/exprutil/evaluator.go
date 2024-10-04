// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package exprutil

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
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

// Int evaluates expr to an int.
func (e Evaluator) Int(ctx context.Context, expr tree.Expr) (int64, error) {
	d, err := e.evalScalar(ctx, expr, types.Int)
	if err != nil {
		return 0, err
	}
	return int64(tree.MustBeDInt(d)), nil
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

// Duration evaluates expr to a duration.Duration.
func (e Evaluator) Duration(ctx context.Context, expr tree.Expr) (duration.Duration, error) {
	d, err := e.evalScalar(ctx, expr, types.Interval)
	if err != nil {
		return duration.Duration{}, err
	}
	return tree.MustBeDInterval(d).Duration, nil
}

// TenantSpec evaluates expr to a tenant specification.
func (e Evaluator) TenantSpec(
	ctx context.Context, ts *tree.TenantSpec,
) (isName bool, tenantID uint64, tenantName string, err error) {
	if ts.All {
		return false, 0, "", errors.AssertionFailedf("programming error: cannot use ALL here")
	}
	if ts.IsName {
		// If the expression is a simple identifier, handle
		// that specially: we promote that identifier to a SQL string.
		// This is alike what is done for CREATE USER.
		expr := paramparse.UnresolvedNameToStrVal(ts.Expr)
		d, err := e.evalScalar(ctx, expr, types.String)
		if err != nil {
			return true, 0, "", err
		}
		return true, 0, string(tree.MustBeDString(d)), nil
	}
	d, err := e.evalScalar(ctx, ts.Expr, types.Int)
	if err != nil {
		return false, 0, "", err
	}
	return false, uint64(tree.MustBeDInt(d)), "", nil
}

// KVOptions evaluates the provided expressions as a string map.
func (e Evaluator) KVOptions(
	ctx context.Context, opts tree.KVOptions, optValidate KVOptionValidationMap,
) (map[string]string, error) {
	res := make(map[string]string, len(opts))
	for _, opt := range opts {
		k := string(opt.Key)
		if err := optValidate.validate(opt); err != nil {
			return nil, err
		}
		if opt.Value == nil {
			res[k] = ""
			continue
		}
		s, err := e.String(ctx, opt.Value)
		if err != nil {
			return nil, err
		}
		res[k] = s
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
