// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"math"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

var (
	errZeroToNegativePower       = pgerror.New(pgcode.InvalidArgumentForPowerFunction, "zero raised to a negative power is undefined")
	errNegativeToNonIntegerPower = pgerror.New(pgcode.InvalidArgumentForPowerFunction, "a negative number raised to a non-integer power yields a complex result")

	zero   = apd.New(0, 0)
	one    = apd.New(1, 0)
	negOne = apd.New(-1, 0)
	posInf = &apd.Decimal{
		Form:     apd.Infinite,
		Negative: false,
	}
)

// IntPow computes the value of x^y.
func IntPow(x, y tree.DInt) (*tree.DInt, error) {
	xd := apd.New(int64(x), 0)
	yd := apd.New(int64(y), 0)
	err := DecimalPow(tree.DecimalCtx, xd, xd, yd)
	if err != nil {
		return nil, err
	}
	i, err := xd.Int64()
	if err != nil {
		return nil, tree.ErrIntOutOfRange
	}
	return tree.NewDInt(tree.DInt(i)), nil
}

// DecimalPow wraps apd.Context.Pow() as a helper function and handles special
// cases that apd.Context.Pow() does not. See handlePowEdgeCases for details.
func DecimalPow(ctx *apd.Context, d, x, y *apd.Decimal) error {
	e, err := handlePowEdgeCases(x, y)
	if err != nil {
		return err
	}
	if e != nil {
		d.Set(e)
		return nil
	}
	// TODO(normanchenn): do something with the condition.
	_, err = ctx.Pow(d, x, y)
	if err != nil {
		return err
	}
	return nil
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

func isOne(d *apd.Decimal) bool {
	return d.Cmp(one) == 0
}

func isNegOne(d *apd.Decimal) bool {
	return d.Cmp(negOne) == 0
}

func isNotInteger(d *apd.Decimal) bool {
	var frac apd.Decimal
	d.Modf(nil, &frac)
	return !frac.IsZero()
}

func isAbsUnderOne(d *apd.Decimal) bool {
	var abs apd.Decimal
	abs.Abs(d)
	return abs.Cmp(one) == -1
}

// handlePowEdgeCases validates and handles special cases for power function arguments
// according to Postgres behavior. This function implements special case handling
// in places that differ from the General Decimal Arithmetic spec used by apd. The
// implementation follows postgres/src/backend/utils/adt/numeric.c:numeric_power,
// which adheres to the POSIX pow(3) spec.
// Special cases handled here:
// - NaN ^ 0 = 1
// - 1 ^ NaN = 1
// - 0 ^ negative = error
// - negative ^ non-integer = error
// - any ^ 0 = 1 (including 0^0)
// - |x| < 1 cases with (+/-) infinite y
// - |x| > 1 cases with (+/-) infinite y
// - -infinity ^ negative = 0
func handlePowEdgeCases(x, y *apd.Decimal) (*apd.Decimal, error) {
	// NaN ^ 0 = 1.
	if x.Form == apd.NaN && y.IsZero() {
		return one, nil
	}
	// 1 ^ NaN = 1.
	if isOne(x) && y.Form == apd.NaN {
		return one, nil
	}

	// Zero raised to a negative power is undefined.
	if x.IsZero() && y.Sign() == -1 {
		return nil, errZeroToNegativePower
	}
	// A negative number raised to a non-integer power yields a complex result.
	if x.Sign() == -1 && isNotInteger(y) {
		return nil, errNegativeToNonIntegerPower
	}

	// For x = 1 and any value of y, 1.0 shall be returned.
	if isOne(x) {
		return one, nil
	}

	// For any value of x and y is 0, 1.0 shall be returned.
	// apd.Context.Pow() would return NaN for 0 ^ 0.
	if y.IsZero() {
		return one, nil
	}

	// If x is 0 and y is positive, 0 shall be returned. This is handled in apd.

	// If x is -1 and y is (+/-)Inf, 1.0 shall be returned.
	if isNegOne(x) && y.Form == apd.Infinite {
		return one, nil
	}

	// If |x| < 1 and y is -Inf, +Inf is returned.
	if isAbsUnderOne(x) && y.Negative && y.Form == apd.Infinite {
		return posInf, nil
	}
	// If |x| < 1 and y is +Inf, +0 shall be returned.
	if isAbsUnderOne(x) && !y.Negative && y.Form == apd.Infinite {
		return zero, nil
	}
	// If |x| > 1 and y is -Inf, +0 shall be returned.
	if !isAbsUnderOne(x) && y.Negative && y.Form == apd.Infinite {
		return zero, nil
	}
	// If |x| > 1 and y is +Inf, +Inf shall be returned.
	if !isAbsUnderOne(x) && !y.Negative && y.Form == apd.Infinite {
		return posInf, nil
	}

	// If x is +Inf and y < 0, 0 shall be returned. This is handled in apd.
	// If x is +Inf and y > 0, +Inf shall be returned. This is handled in apd.

	// If x is -Inf and y < 0, 0 shall be returned.
	if x.Negative && x.Form == apd.Infinite && y.Negative {
		return zero, nil
	}

	// If x is -Inf and y > 0 and y is odd, -Inf shall be returned. This is handled in apd.
	// If x is -Inf and y > 0 and y is even, +Inf shall be returned. This is handled in apd.
	return nil, nil
}
