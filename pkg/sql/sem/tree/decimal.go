// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package tree

import (
	"fmt"
	"math"

	"github.com/cockroachdb/apd"
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
		Traps:       apd.DefaultTraps,
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

	errScaleOutOfRange = pgerror.NewError(pgerror.CodeNumericValueOutOfRangeError, "scale out of range")
)

// LimitDecimalWidth limits d's precision (total number of digits) and scale
// (number of digits after the decimal point).
func LimitDecimalWidth(d *apd.Decimal, precision, scale int) error {
	if d.Form != apd.Finite || precision <= 0 {
		return nil
	}
	// Use +1 here because it is inverted later.
	if scale < math.MinInt32+1 || scale > math.MaxInt32 {
		return errScaleOutOfRange
	}
	if scale > precision {
		return pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError, "scale (%d) must be between 0 and precision (%d)", scale, precision)
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
		return pgerror.NewErrorf(pgerror.CodeNumericValueOutOfRangeError, "value with precision %d, scale %d must round to an absolute value less than %s", precision, scale, lt)
	}
	return nil
}
