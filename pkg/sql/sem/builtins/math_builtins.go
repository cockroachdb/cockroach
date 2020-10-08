// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins

import (
	"math"
	"time"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

func initMathBuiltins() {
	// Add all mathBuiltins to the Builtins map after a sanity check.
	for k, v := range mathBuiltins {
		if _, exists := builtins[k]; exists {
			panic("duplicate builtin: " + k)
		}
		builtins[k] = v
	}
}

var (
	errAbsOfMinInt64  = pgerror.New(pgcode.NumericValueOutOfRange, "abs of min integer value (-9223372036854775808) not defined")
	errLogOfNegNumber = pgerror.New(pgcode.InvalidArgumentForLogarithm, "cannot take logarithm of a negative number")
	errLogOfZero      = pgerror.New(pgcode.InvalidArgumentForLogarithm, "cannot take logarithm of zero")
)

const (
	degToRad = math.Pi / 180.0
	radToDeg = 180.0 / math.Pi
)

// math builtins contains the math built-in functions indexed by name.
//
// For use in other packages, see AllBuiltinNames and GetBuiltinProperties().
var mathBuiltins = map[string]builtinDefinition{
	"abs": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Abs(x))), nil
		}, "Calculates the absolute value of `val`.", tree.VolatilityImmutable),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			dd := &tree.DDecimal{}
			dd.Abs(x)
			return dd, nil
		}, "Calculates the absolute value of `val`.", tree.VolatilityImmutable),
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				x := tree.MustBeDInt(args[0])
				switch {
				case x == math.MinInt64:
					return nil, errAbsOfMinInt64
				case x < 0:
					return tree.NewDInt(-x), nil
				}
				return args[0], nil
			},
			Info:       "Calculates the absolute value of `val`.",
			Volatility: tree.VolatilityImmutable,
		},
	),

	"acos": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Acos(x))), nil
		}, "Calculates the inverse cosine of `val`.", tree.VolatilityImmutable),
	),

	"acosd": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(radToDeg * math.Acos(x))), nil
		}, "Calculates the inverse cosine of `val` with the result in degrees", tree.VolatilityImmutable),
	),

	"acosh": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Acosh(x))), nil
		}, "Calculates the inverse hyperbolic cosine of `val`.", tree.VolatilityImmutable),
	),

	"asin": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Asin(x))), nil
		}, "Calculates the inverse sine of `val`.", tree.VolatilityImmutable),
	),

	"asind": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(radToDeg * math.Asin(x))), nil
		}, "Calculates the inverse sine of `val` with the result in degrees.", tree.VolatilityImmutable),
	),

	"asinh": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Asinh(x))), nil
		}, "Calculates the inverse hyperbolic sine of `val`.", tree.VolatilityImmutable),
	),

	"atan": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Atan(x))), nil
		}, "Calculates the inverse tangent of `val`.", tree.VolatilityImmutable),
	),

	"atand": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(radToDeg * math.Atan(x))), nil
		}, "Calculates the inverse tangent of `val` with the result in degrees.", tree.VolatilityImmutable),
	),

	"atanh": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Atanh(x))), nil
		}, "Calculates the inverse hyperbolic tangent of `val`.", tree.VolatilityImmutable),
	),

	"atan2": makeBuiltin(defProps(),
		floatOverload2("x", "y", func(x, y float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Atan2(x, y))), nil
		}, "Calculates the inverse tangent of `x`/`y`.", tree.VolatilityImmutable),
	),

	"atan2d": makeBuiltin(defProps(),
		floatOverload2("x", "y", func(x, y float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(radToDeg * math.Atan2(x, y))), nil
		}, "Calculates the inverse tangent of `x`/`y` with the result in degrees", tree.VolatilityImmutable),
	),

	"cbrt": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.Cbrt(x)
		}, "Calculates the cube root (∛) of `val`.", tree.VolatilityImmutable),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			return tree.DecimalCbrt(x)
		}, "Calculates the cube root (∛) of `val`.", tree.VolatilityImmutable),
	),

	"ceil":    ceilImpl,
	"ceiling": ceilImpl,

	"cos": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Cos(x))), nil
		}, "Calculates the cosine of `val`.", tree.VolatilityImmutable),
	),

	"cosd": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Cos(degToRad * x))), nil
		}, "Calculates the cosine of `val` where `val` is in degrees.", tree.VolatilityImmutable),
	),

	"cosh": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Cosh(x))), nil
		}, "Calculates the hyperbolic cosine of `val`.", tree.VolatilityImmutable),
	),

	"cot": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(1 / math.Tan(x))), nil
		}, "Calculates the cotangent of `val`.", tree.VolatilityImmutable),
	),

	"cotd": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(1 / math.Tan(degToRad*x))), nil
		}, "Calculates the cotangent of `val` where `val` is in degrees.", tree.VolatilityImmutable),
	),

	"degrees": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(radToDeg * x)), nil
		}, "Converts `val` as a radian value to a degree value.", tree.VolatilityImmutable),
	),

	"div": makeBuiltin(defProps(),
		floatOverload2("x", "y", func(x, y float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Trunc(x / y))), nil
		}, "Calculates the integer quotient of `x`/`y`.", tree.VolatilityImmutable),
		decimalOverload2("x", "y", func(x, y *apd.Decimal) (tree.Datum, error) {
			if y.Sign() == 0 {
				return nil, tree.ErrDivByZero
			}
			dd := &tree.DDecimal{}
			_, err := tree.HighPrecisionCtx.QuoInteger(&dd.Decimal, x, y)
			return dd, err
		}, "Calculates the integer quotient of `x`/`y`.", tree.VolatilityImmutable),
		tree.Overload{
			Types:      tree.ArgTypes{{"x", types.Int}, {"y", types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				y := tree.MustBeDInt(args[1])
				if y == 0 {
					return nil, tree.ErrDivByZero
				}
				x := tree.MustBeDInt(args[0])
				return tree.NewDInt(x / y), nil
			},
			Info:       "Calculates the integer quotient of `x`/`y`.",
			Volatility: tree.VolatilityImmutable,
		},
	),

	"exp": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Exp(x))), nil
		}, "Calculates *e* ^ `val`.", tree.VolatilityImmutable),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			dd := &tree.DDecimal{}
			_, err := tree.DecimalCtx.Exp(&dd.Decimal, x)
			return dd, err
		}, "Calculates *e* ^ `val`.", tree.VolatilityImmutable),
	),

	"floor": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Floor(x))), nil
		}, "Calculates the largest integer not greater than `val`.", tree.VolatilityImmutable),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			dd := &tree.DDecimal{}
			_, err := tree.ExactCtx.Floor(&dd.Decimal, x)
			return dd, err
		}, "Calculates the largest integer not greater than `val`.", tree.VolatilityImmutable),
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.Int}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDFloat(tree.DFloat(float64(*args[0].(*tree.DInt)))), nil
			},
			Info:       "Calculates the largest integer not greater than `val`.",
			Volatility: tree.VolatilityImmutable,
		},
	),

	"isnan": makeBuiltin(defProps(),
		tree.Overload{
			// Can't use floatBuiltin1 here because this one returns
			// a boolean.
			Types:      tree.ArgTypes{{"val", types.Float}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDBool(tree.DBool(math.IsNaN(float64(*args[0].(*tree.DFloat))))), nil
			},
			Info:       "Returns true if `val` is NaN, false otherwise.",
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.Decimal}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				isNaN := args[0].(*tree.DDecimal).Decimal.Form == apd.NaN
				return tree.MakeDBool(tree.DBool(isNaN)), nil
			},
			Info:       "Returns true if `val` is NaN, false otherwise.",
			Volatility: tree.VolatilityImmutable,
		},
	),

	"ln": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Log(x))), nil
		}, "Calculates the natural log of `val`.", tree.VolatilityImmutable),
		decimalLogFn(tree.DecimalCtx.Ln, "Calculates the natural log of `val`.", tree.VolatilityImmutable),
	),

	"log": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Log10(x))), nil
		}, "Calculates the base 10 log of `val`.", tree.VolatilityImmutable),
		floatOverload2("b", "x", func(b, x float64) (tree.Datum, error) {
			switch {
			case x < 0.0:
				return nil, errLogOfNegNumber
			case x == 0.0:
				return nil, errLogOfZero
			}
			switch {
			case b < 0.0:
				return nil, errLogOfNegNumber
			case b == 0.0:
				return nil, errLogOfZero
			}
			return tree.NewDFloat(tree.DFloat(math.Log10(x) / math.Log10(b))), nil
		}, "Calculates the base `b` log of `val`.", tree.VolatilityImmutable),
		decimalLogFn(tree.DecimalCtx.Log10, "Calculates the base 10 log of `val`.", tree.VolatilityImmutable),
		decimalOverload2("b", "x", func(b, x *apd.Decimal) (tree.Datum, error) {
			switch x.Sign() {
			case -1:
				return nil, errLogOfNegNumber
			case 0:
				return nil, errLogOfZero
			}
			switch b.Sign() {
			case -1:
				return nil, errLogOfNegNumber
			case 0:
				return nil, errLogOfZero
			}

			top := new(apd.Decimal)
			if _, err := tree.IntermediateCtx.Ln(top, x); err != nil {
				return nil, err
			}
			bot := new(apd.Decimal)
			if _, err := tree.IntermediateCtx.Ln(bot, b); err != nil {
				return nil, err
			}

			dd := &tree.DDecimal{}
			_, err := tree.DecimalCtx.Quo(&dd.Decimal, top, bot)
			return dd, err
		}, "Calculates the base `b` log of `val`.", tree.VolatilityImmutable),
	),

	"mod": makeBuiltin(defProps(),
		floatOverload2("x", "y", func(x, y float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Mod(x, y))), nil
		}, "Calculates `x`%`y`.", tree.VolatilityImmutable),
		decimalOverload2("x", "y", func(x, y *apd.Decimal) (tree.Datum, error) {
			if y.Sign() == 0 {
				return nil, tree.ErrDivByZero
			}
			dd := &tree.DDecimal{}
			_, err := tree.HighPrecisionCtx.Rem(&dd.Decimal, x, y)
			return dd, err
		}, "Calculates `x`%`y`.", tree.VolatilityImmutable),
		tree.Overload{
			Types:      tree.ArgTypes{{"x", types.Int}, {"y", types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				y := tree.MustBeDInt(args[1])
				if y == 0 {
					return nil, tree.ErrDivByZero
				}
				x := tree.MustBeDInt(args[0])
				return tree.NewDInt(x % y), nil
			},
			Info:       "Calculates `x`%`y`.",
			Volatility: tree.VolatilityImmutable,
		},
	),

	"pi": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDFloat(math.Pi), nil
			},
			Info:       "Returns the value for pi (3.141592653589793).",
			Volatility: tree.VolatilityImmutable,
		},
	),

	"pow":   powImpls,
	"power": powImpls,

	"radians": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(x * degToRad)), nil
		}, "Converts `val` as a degree value to a radians value.", tree.VolatilityImmutable),
	),

	"round": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.RoundToEven(x))), nil
		}, "Rounds `val` to the nearest integer using half to even (banker's) rounding.", tree.VolatilityImmutable),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			return roundDecimal(x, 0)
		}, "Rounds `val` to the nearest integer, half away from zero: "+
			"round(+/-2.4) = +/-2, round(+/-2.5) = +/-3.", tree.VolatilityImmutable),
		tree.Overload{
			Types:      tree.ArgTypes{{"input", types.Float}, {"decimal_accuracy", types.Int}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				f := float64(*args[0].(*tree.DFloat))
				if math.IsInf(f, 0) || math.IsNaN(f) {
					return args[0], nil
				}
				var x apd.Decimal
				if _, err := x.SetFloat64(f); err != nil {
					return nil, err
				}

				// TODO(mjibson): make sure this fits in an int32.
				scale := int32(tree.MustBeDInt(args[1]))

				var d apd.Decimal
				if _, err := tree.RoundCtx.Quantize(&d, &x, -scale); err != nil {
					return nil, err
				}

				f, err := d.Float64()
				if err != nil {
					return nil, err
				}

				return tree.NewDFloat(tree.DFloat(f)), nil
			},
			Info: "Keeps `decimal_accuracy` number of figures to the right of the zero position " +
				" in `input` using half to even (banker's) rounding.",
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"input", types.Decimal}, {"decimal_accuracy", types.Int}},
			ReturnType: tree.FixedReturnType(types.Decimal),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// TODO(mjibson): make sure this fits in an int32.
				scale := int32(tree.MustBeDInt(args[1]))
				return roundDecimal(&args[0].(*tree.DDecimal).Decimal, scale)
			},
			Info: "Keeps `decimal_accuracy` number of figures to the right of the zero position " +
				"in `input` using half away from zero rounding. If `decimal_accuracy` " +
				"is not in the range -2^31...(2^31-1), the results are undefined.",
			Volatility: tree.VolatilityImmutable,
		},
	),

	"sin": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Sin(x))), nil
		}, "Calculates the sine of `val`.", tree.VolatilityImmutable),
	),

	"sind": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Sin(degToRad * x))), nil
		}, "Calculates the sine of `val` where `val` is in degrees.", tree.VolatilityImmutable),
	),

	"sinh": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Sinh(x))), nil
		}, "Calculates the hyperbolic sine of `val`.", tree.VolatilityImmutable),
	),

	"sign": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			switch {
			case x < 0:
				return tree.NewDFloat(-1), nil
			case x == 0:
				return tree.NewDFloat(0), nil
			}
			return tree.NewDFloat(1), nil
		}, "Determines the sign of `val`: **1** for positive; **0** for 0 values; **-1** for "+
			"negative.", tree.VolatilityImmutable),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			d := &tree.DDecimal{}
			d.Decimal.SetInt64(int64(x.Sign()))
			return d, nil
		}, "Determines the sign of `val`: **1** for positive; **0** for 0 values; **-1** for "+
			"negative.", tree.VolatilityImmutable),
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				x := tree.MustBeDInt(args[0])
				switch {
				case x < 0:
					return tree.NewDInt(-1), nil
				case x == 0:
					return tree.DZero, nil
				}
				return tree.NewDInt(1), nil
			},
			Info: "Determines the sign of `val`: **1** for positive; **0** for 0 values; **-1** " +
				"for negative.",
			Volatility: tree.VolatilityImmutable,
		},
	),

	"sqrt": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.Sqrt(x)
		}, "Calculates the square root of `val`.", tree.VolatilityImmutable),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			return tree.DecimalSqrt(x)
		}, "Calculates the square root of `val`.", tree.VolatilityImmutable),
	),

	"tan": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Tan(x))), nil
		}, "Calculates the tangent of `val`.", tree.VolatilityImmutable),
	),

	"tand": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Tan(degToRad * x))), nil
		}, "Calculates the tangent of `val` where `val` is in degrees.", tree.VolatilityImmutable),
	),

	"tanh": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Tanh(x))), nil
		}, "Calculates the hyperbolic tangent of `val`.", tree.VolatilityImmutable),
	),

	"trunc": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Trunc(x))), nil
		}, "Truncates the decimal values of `val`.", tree.VolatilityImmutable),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			dd := &tree.DDecimal{}
			x.Modf(&dd.Decimal, nil)
			return dd, nil
		}, "Truncates the decimal values of `val`.", tree.VolatilityImmutable),
	),

	"width_bucket": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{{"operand", types.Decimal}, {"b1", types.Decimal},
				{"b2", types.Decimal}, {"count", types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				operand, _ := args[0].(*tree.DDecimal).Float64()
				b1, _ := args[1].(*tree.DDecimal).Float64()
				b2, _ := args[2].(*tree.DDecimal).Float64()
				count := int(tree.MustBeDInt(args[3]))
				return tree.NewDInt(tree.DInt(widthBucket(operand, b1, b2, count))), nil
			},
			Info: "return the bucket number to which operand would be assigned in a histogram having count " +
				"equal-width buckets spanning the range b1 to b2.",
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{{"operand", types.Int}, {"b1", types.Int},
				{"b2", types.Int}, {"count", types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				operand := float64(tree.MustBeDInt(args[0]))
				b1 := float64(tree.MustBeDInt(args[1]))
				b2 := float64(tree.MustBeDInt(args[2]))
				count := int(tree.MustBeDInt(args[3]))
				return tree.NewDInt(tree.DInt(widthBucket(operand, b1, b2, count))), nil
			},
			Info: "return the bucket number to which operand would be assigned in a histogram having count " +
				"equal-width buckets spanning the range b1 to b2.",
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"operand", types.Any}, {"thresholds", types.AnyArray}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				operand := args[0]
				thresholds := tree.MustBeDArray(args[1])

				if !operand.ResolvedType().Equivalent(thresholds.ParamTyp) {
					return tree.NewDInt(0), errors.New("operand and thresholds must be of the same type")
				}

				for i, v := range thresholds.Array {
					if operand.Compare(ctx, v) < 0 {
						return tree.NewDInt(tree.DInt(i)), nil
					}
				}

				return tree.NewDInt(tree.DInt(thresholds.Len())), nil
			},
			Info: "return the bucket number to which operand would be assigned given an array listing the " +
				"lower bounds of the buckets; returns 0 for an input less than the first lower bound; the " +
				"thresholds array must be sorted, smallest first, or unexpected results will be obtained",
			Volatility: tree.VolatilityImmutable,
		},
	),
}

var ceilImpl = makeBuiltin(defProps(),
	floatOverload1(func(x float64) (tree.Datum, error) {
		return tree.NewDFloat(tree.DFloat(math.Ceil(x))), nil
	}, "Calculates the smallest integer not smaller than `val`.", tree.VolatilityImmutable),
	decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
		dd := &tree.DDecimal{}
		_, err := tree.ExactCtx.Ceil(&dd.Decimal, x)
		if dd.IsZero() {
			dd.Negative = false
		}
		return dd, err
	}, "Calculates the smallest integer not smaller than `val`.", tree.VolatilityImmutable),
	tree.Overload{
		Types:      tree.ArgTypes{{"val", types.Int}},
		ReturnType: tree.FixedReturnType(types.Float),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(float64(*args[0].(*tree.DInt)))), nil
		},
		Info:       "Calculates the smallest integer not smaller than `val`.",
		Volatility: tree.VolatilityImmutable,
	},
)

var powImpls = makeBuiltin(defProps(),
	floatOverload2("x", "y", func(x, y float64) (tree.Datum, error) {
		return tree.NewDFloat(tree.DFloat(math.Pow(x, y))), nil
	}, "Calculates `x`^`y`.", tree.VolatilityImmutable),
	decimalOverload2("x", "y", func(x, y *apd.Decimal) (tree.Datum, error) {
		dd := &tree.DDecimal{}
		_, err := tree.DecimalCtx.Pow(&dd.Decimal, x, y)
		return dd, err
	}, "Calculates `x`^`y`.", tree.VolatilityImmutable),
	tree.Overload{
		Types: tree.ArgTypes{
			{"x", types.Int},
			{"y", types.Int},
		},
		ReturnType: tree.FixedReturnType(types.Int),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return tree.IntPow(tree.MustBeDInt(args[0]), tree.MustBeDInt(args[1]))
		},
		Info:       "Calculates `x`^`y`.",
		Volatility: tree.VolatilityImmutable,
	},
)

func decimalLogFn(
	logFn func(*apd.Decimal, *apd.Decimal) (apd.Condition, error),
	info string,
	volatility tree.Volatility,
) tree.Overload {
	return decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
		switch x.Sign() {
		case -1:
			return nil, errLogOfNegNumber
		case 0:
			return nil, errLogOfZero
		}
		dd := &tree.DDecimal{}
		_, err := logFn(&dd.Decimal, x)
		return dd, err
	}, info, tree.VolatilityImmutable)
}

func floatOverload1(
	f func(float64) (tree.Datum, error), info string, volatility tree.Volatility,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{"val", types.Float}},
		ReturnType: tree.FixedReturnType(types.Float),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(float64(*args[0].(*tree.DFloat)))
		},
		Info:       info,
		Volatility: volatility,
	}
}

func floatOverload2(
	a, b string,
	f func(float64, float64) (tree.Datum, error),
	info string,
	volatility tree.Volatility,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{a, types.Float}, {b, types.Float}},
		ReturnType: tree.FixedReturnType(types.Float),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(float64(*args[0].(*tree.DFloat)),
				float64(*args[1].(*tree.DFloat)))
		},
		Info:       info,
		Volatility: volatility,
	}
}

func decimalOverload1(
	f func(*apd.Decimal) (tree.Datum, error), info string, volatility tree.Volatility,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{"val", types.Decimal}},
		ReturnType: tree.FixedReturnType(types.Decimal),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			dec := &args[0].(*tree.DDecimal).Decimal
			return f(dec)
		},
		Info:       info,
		Volatility: volatility,
	}
}

func decimalOverload2(
	a, b string,
	f func(*apd.Decimal, *apd.Decimal) (tree.Datum, error),
	info string,
	volatility tree.Volatility,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{a, types.Decimal}, {b, types.Decimal}},
		ReturnType: tree.FixedReturnType(types.Decimal),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			dec1 := &args[0].(*tree.DDecimal).Decimal
			dec2 := &args[1].(*tree.DDecimal).Decimal
			return f(dec1, dec2)
		},
		Info:       info,
		Volatility: volatility,
	}
}

// roundDDecimal avoids creation of a new DDecimal in common case where no
// rounding is necessary.
func roundDDecimal(d *tree.DDecimal, scale int32) (tree.Datum, error) {
	// Fast path: check if number of digits after decimal point is already low
	// enough.
	if -d.Exponent <= scale {
		return d, nil
	}
	return roundDecimal(&d.Decimal, scale)
}

func roundDecimal(x *apd.Decimal, scale int32) (tree.Datum, error) {
	dd := &tree.DDecimal{}
	_, err := tree.HighPrecisionCtx.Quantize(&dd.Decimal, x, -scale)
	return dd, err
}

var uniqueIntState struct {
	syncutil.Mutex
	timestamp uint64
}

var uniqueIntEpoch = time.Date(2015, time.January, 1, 0, 0, 0, 0, time.UTC).UnixNano()

// widthBucket returns the bucket number to which operand would be assigned in a histogram having count
// equal-width buckets spanning the range b1 to b2
func widthBucket(operand float64, b1 float64, b2 float64, count int) int {
	bucket := 0
	if (b1 < b2 && operand > b2) || (b1 > b2 && operand < b2) {
		return count + 1
	}

	if (b1 < b2 && operand < b1) || (b1 > b2 && operand > b1) {
		return 0
	}

	width := (b2 - b1) / float64(count)
	difference := operand - b1
	bucket = int(math.Floor(difference/width) + 1)

	return bucket
}
