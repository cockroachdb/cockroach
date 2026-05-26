// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
)

// UnaryOp will evaluate a tree.UnaryEvalOp on a Datum into another Datum.
func UnaryOp(
	ctx context.Context, evalCtx *Context, op tree.UnaryEvalOp, in tree.Datum,
) (tree.Datum, error) {
	return op.Eval(ctx, (*evaluator)(evalCtx), in)
}

func (e *evaluator) EvalCbrtDecimalOp(
	ctx context.Context, _ *tree.CbrtDecimalOp, d tree.Datum,
) (tree.Datum, error) {
	dec := &d.(*tree.DDecimal).Decimal
	return DecimalCbrt(dec)
}

func (e *evaluator) EvalCbrtFloatOp(
	ctx context.Context, _ *tree.CbrtFloatOp, d tree.Datum,
) (tree.Datum, error) {
	return Cbrt(float64(*d.(*tree.DFloat)))
}

func (e *evaluator) EvalComplementINetOp(
	ctx context.Context, _ *tree.ComplementINetOp, d tree.Datum,
) (tree.Datum, error) {
	ipAddr := tree.MustBeDIPAddr(d).IPAddr
	return tree.NewDIPAddr(tree.DIPAddr{IPAddr: ipAddr.Complement()}), nil
}

func (e *evaluator) EvalComplementIntOp(
	ctx context.Context, _ *tree.ComplementIntOp, d tree.Datum,
) (tree.Datum, error) {
	return tree.NewDInt(^tree.MustBeDInt(d)), nil
}

func (e *evaluator) EvalComplementVarBitOp(
	ctx context.Context, _ *tree.ComplementVarBitOp, d tree.Datum,
) (tree.Datum, error) {
	p := tree.MustBeDBitArray(d)
	return &tree.DBitArray{
		BitArray: bitarray.Not(p.BitArray),
	}, nil
}

func (e *evaluator) EvalSqrtDecimalOp(
	ctx context.Context, _ *tree.SqrtDecimalOp, d tree.Datum,
) (tree.Datum, error) {
	dec := &d.(*tree.DDecimal).Decimal
	return DecimalSqrt(dec)
}

func (e *evaluator) EvalSqrtFloatOp(
	ctx context.Context, _ *tree.SqrtFloatOp, d tree.Datum,
) (tree.Datum, error) {
	return Sqrt(float64(*d.(*tree.DFloat)))
}

func (e *evaluator) EvalUnaryMinusDecimalOp(
	ctx context.Context, _ *tree.UnaryMinusDecimalOp, d tree.Datum,
) (tree.Datum, error) {
	dec := &d.(*tree.DDecimal).Decimal
	dd := &tree.DDecimal{}
	dd.Decimal.Neg(dec)
	return dd, nil
}

func (e *evaluator) EvalUnaryMinusFloatOp(
	ctx context.Context, _ *tree.UnaryMinusFloatOp, d tree.Datum,
) (tree.Datum, error) {
	return tree.NewDFloat(-*d.(*tree.DFloat)), nil
}

func (e *evaluator) EvalUnaryMinusIntOp(
	ctx context.Context, _ *tree.UnaryMinusIntOp, d tree.Datum,
) (tree.Datum, error) {
	i := tree.MustBeDInt(d)
	if i == math.MinInt64 {
		return nil, tree.ErrIntOutOfRange
	}
	return tree.NewDInt(-i), nil
}

func (e *evaluator) EvalUnaryMinusIntervalOp(
	ctx context.Context, _ *tree.UnaryMinusIntervalOp, d tree.Datum,
) (tree.Datum, error) {
	i := d.(*tree.DInterval).Duration
	i.SetNanos(-i.Nanos())
	i.Days = -i.Days
	i.Months = -i.Months
	return &tree.DInterval{Duration: i}, nil
}
