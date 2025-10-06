// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"math"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// IntPow computes the value of x^y.
func IntPow(x, y tree.DInt) (*tree.DInt, error) {
	xd := apd.New(int64(x), 0)
	yd := apd.New(int64(y), 0)
	_, err := tree.DecimalCtx.Pow(xd, xd, yd)
	if err != nil {
		return nil, err
	}
	i, err := xd.Int64()
	if err != nil {
		return nil, tree.ErrIntOutOfRange
	}
	return tree.NewDInt(tree.DInt(i)), nil
}

// Sqrt returns the square root of x.
func Sqrt(x float64) (*tree.DFloat, error) {
	if x < 0.0 {
		return nil, tree.ErrSqrtOfNegNumber
	}
	return tree.NewDFloat(tree.DFloat(math.Sqrt(x))), nil
}

// DecimalSqrt returns the square root of x.
func DecimalSqrt(x *apd.Decimal) (*tree.DDecimal, error) {
	if x.Sign() < 0 {
		return nil, tree.ErrSqrtOfNegNumber
	}
	dd := &tree.DDecimal{}
	_, err := tree.DecimalCtx.Sqrt(&dd.Decimal, x)
	return dd, err
}

// Cbrt returns the cube root of x.
func Cbrt(x float64) (*tree.DFloat, error) {
	return tree.NewDFloat(tree.DFloat(math.Cbrt(x))), nil
}

// DecimalCbrt returns the cube root of x.
func DecimalCbrt(x *apd.Decimal) (*tree.DDecimal, error) {
	dd := &tree.DDecimal{}
	_, err := tree.DecimalCtx.Cbrt(&dd.Decimal, x)
	return dd, err
}
