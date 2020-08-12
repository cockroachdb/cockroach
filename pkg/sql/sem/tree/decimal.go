// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"fmt"
	"math"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

var (
	// DecimalCtx is the default context for decimal operations. Any change
	// in the exponent limits must still guarantee a safe conversion to the
	// postgres binary decimal format in the wire protocol, which uses an
	// int16. See pgwire/types.go.
	DecimalCtx = &apd.Context{
		Precision:   20,
		Rounding:    apd.RoundHalfUp,
		MaxExponent: 2000,
		MinExponent: -2000,
		// Don't error on invalid operation, return NaN instead.
		Traps: apd.DefaultTraps &^ apd.InvalidOperation,
	}
	// ExactCtx is a decimal context with exact precision.
	ExactCtx = DecimalCtx.WithPrecision(0)
	// HighPrecisionCtx is a decimal context with high precision.
	HighPrecisionCtx = DecimalCtx.WithPrecision(2000)
	// IntermediateCtx is a decimal context with additional precision for
	// intermediate calculations to protect against order changes that can
	// happen in dist SQL. The additional 5 allows the stress test to pass.
	// See #13689 for more analysis and other algorithms.
	IntermediateCtx = DecimalCtx.WithPrecision(DecimalCtx.Precision + 5)
	// RoundCtx is a decimal context with high precision and RoundHalfEven
	// rounding.
	RoundCtx = func() *apd.Context {
		ctx := *HighPrecisionCtx
		ctx.Rounding = apd.RoundHalfEven
		return &ctx
	}()

	errScaleOutOfRange = pgerror.New(pgcode.NumericValueOutOfRange, "scale out of range")
)

// LimitDecimalWidth limits d's precision (total number of digits) and scale
// (number of digits after the decimal point). Note that this any limiting will
// modify the decimal in-place.
func LimitDecimalWidth(d *apd.Decimal, precision, scale int) error {
	if d.Form != apd.Finite || precision <= 0 {
		return nil
	}
	// Use +1 here because it is inverted later.
	if scale < math.MinInt32+1 || scale > math.MaxInt32 {
		return errScaleOutOfRange
	}
	if scale > precision {
		return pgerror.Newf(pgcode.InvalidParameterValue, "scale (%d) must be between 0 and precision (%d)", scale, precision)
	}

	// http://www.postgresql.org/docs/9.5/static/datatype-numeric.html
	// "If the scale of a value to be stored is greater than
	// the declared scale of the column, the system will round the
	// value to the specified number of fractional digits. Then,
	// if the number of digits to the left of the decimal point
	// exceeds the declared precision minus the declared scale, an
	// error is raised."

	c := DecimalCtx.WithPrecision(uint32(precision))
	c.Traps = apd.InvalidOperation

	if _, err := c.Quantize(d, d, -int32(scale)); err != nil {
		var lt string
		switch v := precision - scale; v {
		case 0:
			lt = "1"
		default:
			lt = fmt.Sprintf("10^%d", v)
		}
		return pgerror.Newf(pgcode.NumericValueOutOfRange, "value with precision %d, scale %d must round to an absolute value less than %s", precision, scale, lt)
	}
	return nil
}
