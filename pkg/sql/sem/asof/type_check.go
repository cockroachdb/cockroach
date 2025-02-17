// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package asof

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// TypeCheckSystemTimeExpr type checks an Expr as a system time. It
// accepts the same types as AS OF SYSTEM TIME expressions and
// functions that evaluate to one of those types.
//
// The types need to be kept in sync with those supported by
// asof.DatumToHLC.
//
// TODO(ssd): AOST and SPLIT are restricted to the use of constant expressions
// or particular follower-read related functions. Do we want to do that here as well?
// One nice side effect of allowing functions is that users can use NOW().
func TypeCheckSystemTimeExpr(
	ctx context.Context, semaCtx *tree.SemaContext, systemTimeExpr tree.Expr, op string,
) (tree.TypedExpr, error) {
	typedExpr, err := tree.TypeCheckAndRequire(ctx, systemTimeExpr, semaCtx, types.AnyElement, op)
	if err != nil {
		return nil, err
	}

	switch typedExpr.ResolvedType().Family() {
	case types.IntervalFamily, types.TimestampTZFamily, types.TimestampFamily, types.StringFamily, types.DecimalFamily, types.IntFamily:
		return typedExpr, nil
	default:
		return nil, errors.Errorf("expected string, timestamp, decimal, interval, or integer, got %s", typedExpr.ResolvedType())
	}
}

// EvalSystemTimeExpr evaluates an Expr as a system time. It accepts
// the same types as AS OF SYSTEM TIME expressions and functions that
// evaluate to one of those types.
func EvalSystemTimeExpr(
	ctx context.Context,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
	systemTimeExpr tree.Expr,
	op string,
	usage DatumToHLCUsage,
) (hlc.Timestamp, error) {
	typedExpr, err := TypeCheckSystemTimeExpr(ctx, semaCtx, systemTimeExpr, op)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	d, err := eval.Expr(ctx, evalCtx, typedExpr)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	if d == tree.DNull {
		return hlc.MaxTimestamp, nil
	}
	stmtTimestamp := evalCtx.GetStmtTimestamp()
	return DatumToHLC(evalCtx, stmtTimestamp, d, usage)
}
