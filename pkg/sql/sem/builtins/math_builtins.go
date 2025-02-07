// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins

import (
	"context"
	"math"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func init() {
	for k, v := range mathBuiltins {
		const enforceClass = true
		registerBuiltin(k, v, tree.NormalClass, enforceClass)
	}
}

var (
	errAbsOfMinInt64  = pgerror.New(pgcode.NumericValueOutOfRange, "abs of min integer value (-9223372036854775808) not defined")
	errLogOfNegNumber = pgerror.New(pgcode.InvalidArgumentForLogarithm, "cannot take logarithm of a negative number")
	errLogOfZero      = pgerror.New(pgcode.InvalidArgumentForLogarithm, "cannot take logarithm of zero")

	bigTen = apd.NewBigInt(10)
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
		}, "Calculates the absolute value of `val`.", volatility.Immutable),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			dd := &tree.DDecimal{}
			dd.Abs(x)
			return dd, nil
		}, "Calculates the absolute value of `val`.", volatility.Immutable),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
	),

	"acos": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Acos(x))), nil
		}, "Calculates the inverse cosine of `val`.", volatility.Immutable),
	),

	"acosd": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(radToDeg * math.Acos(x))), nil
		}, "Calculates the inverse cosine of `val` with the result in degrees", volatility.Immutable),
	),

	"acosh": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Acosh(x))), nil
		}, "Calculates the inverse hyperbolic cosine of `val`.", volatility.Immutable),
	),

	"asin": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Asin(x))), nil
		}, "Calculates the inverse sine of `val`.", volatility.Immutable),
	),

	"asind": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(radToDeg * math.Asin(x))), nil
		}, "Calculates the inverse sine of `val` with the result in degrees.", volatility.Immutable),
	),

	"asinh": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Asinh(x))), nil
		}, "Calculates the inverse hyperbolic sine of `val`.", volatility.Immutable),
	),

	"atan": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Atan(x))), nil
		}, "Calculates the inverse tangent of `val`.", volatility.Immutable),
	),

	"atand": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(radToDeg * math.Atan(x))), nil
		}, "Calculates the inverse tangent of `val` with the result in degrees.", volatility.Immutable),
	),

	"atanh": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Atanh(x))), nil
		}, "Calculates the inverse hyperbolic tangent of `val`.", volatility.Immutable),
	),

	"atan2": makeBuiltin(defProps(),
		floatOverload2("x", "y", func(x, y float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Atan2(x, y))), nil
		}, "Calculates the inverse tangent of `x`/`y`.", volatility.Immutable),
	),

	"atan2d": makeBuiltin(defProps(),
		floatOverload2("x", "y", func(x, y float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(radToDeg * math.Atan2(x, y))), nil
		}, "Calculates the inverse tangent of `x`/`y` with the result in degrees", volatility.Immutable),
	),

	"cbrt": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return eval.Cbrt(x)
		}, "Calculates the cube root (∛) of `val`.", volatility.Immutable),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			return eval.DecimalCbrt(x)
		}, "Calculates the cube root (∛) of `val`.", volatility.Immutable),
	),

	"ceil":    ceilImpl(),
	"ceiling": ceilImpl(),

	"cos": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Cos(x))), nil
		}, "Calculates the cosine of `val`.", volatility.Immutable),
	),

	"cosd": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Cos(degToRad * x))), nil
		}, "Calculates the cosine of `val` where `val` is in degrees.", volatility.Immutable),
	),

	"cosh": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Cosh(x))), nil
		}, "Calculates the hyperbolic cosine of `val`.", volatility.Immutable),
	),

	"cot": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(1 / math.Tan(x))), nil
		}, "Calculates the cotangent of `val`.", volatility.Immutable),
	),

	"cotd": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(1 / math.Tan(degToRad*x))), nil
		}, "Calculates the cotangent of `val` where `val` is in degrees.", volatility.Immutable),
	),

	"degrees": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(radToDeg * x)), nil
		}, "Converts `val` as a radian value to a degree value.", volatility.Immutable),
	),

	"div": makeBuiltin(defProps(),
		floatOverload2("x", "y", func(x, y float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Trunc(x / y))), nil
		}, "Calculates the integer quotient of `x`/`y`.", volatility.Immutable),
		decimalOverload2("x", "y", func(x, y *apd.Decimal) (tree.Datum, error) {
			if y.Sign() == 0 {
				return nil, tree.ErrDivByZero
			}
			dd := &tree.DDecimal{}
			_, err := tree.HighPrecisionCtx.QuoInteger(&dd.Decimal, x, y)
			return dd, err
		}, "Calculates the integer quotient of `x`/`y`.", volatility.Immutable),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "x", Typ: types.Int}, {Name: "y", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				y := tree.MustBeDInt(args[1])
				if y == 0 {
					return nil, tree.ErrDivByZero
				}
				x := tree.MustBeDInt(args[0])
				return tree.NewDInt(x / y), nil
			},
			Info:       "Calculates the integer quotient of `x`/`y`.",
			Volatility: volatility.Immutable,
		},
	),

	"exp": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Exp(x))), nil
		}, "Calculates *e* ^ `val`.", volatility.Immutable),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			dd := &tree.DDecimal{}
			_, err := tree.DecimalCtx.Exp(&dd.Decimal, x)
			return dd, err
		}, "Calculates *e* ^ `val`.", volatility.Immutable),
	),

	"floor": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Floor(x))), nil
		}, "Calculates the largest integer not greater than `val`.", volatility.Immutable),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			dd := &tree.DDecimal{}
			_, err := tree.ExactCtx.Floor(&dd.Decimal, x)
			return dd, err
		}, "Calculates the largest integer not greater than `val`.", volatility.Immutable),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.NewDFloat(tree.DFloat(float64(*args[0].(*tree.DInt)))), nil
			},
			Info:       "Calculates the largest integer not greater than `val`.",
			Volatility: volatility.Immutable,
		},
	),

	"isnan": makeBuiltin(defProps(),
		tree.Overload{
			// Can't use floatBuiltin1 here because this one returns
			// a boolean.
			Types:      tree.ParamTypes{{Name: "val", Typ: types.Float}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDBool(tree.DBool(math.IsNaN(float64(*args[0].(*tree.DFloat))))), nil
			},
			Info:       "Returns true if `val` is NaN, false otherwise.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.Decimal}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				isNaN := args[0].(*tree.DDecimal).Decimal.Form == apd.NaN
				return tree.MakeDBool(tree.DBool(isNaN)), nil
			},
			Info:       "Returns true if `val` is NaN, false otherwise.",
			Volatility: volatility.Immutable,
		},
	),

	"ln": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Log(x))), nil
		}, "Calculates the natural log of `val`.", volatility.Immutable),
		decimalLogFn(tree.DecimalCtx.Ln, "Calculates the natural log of `val`."),
	),

	"log": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Log10(x))), nil
		}, "Calculates the base 10 log of `val`.", volatility.Immutable),
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
		}, "Calculates the base `b` log of `val`.", volatility.Immutable),
		decimalLogFn(tree.DecimalCtx.Log10, "Calculates the base 10 log of `val`."),
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
		}, "Calculates the base `b` log of `val`.", volatility.Immutable),
	),

	"mod": makeBuiltin(defProps(),
		floatOverload2("x", "y", func(x, y float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Mod(x, y))), nil
		}, "Calculates `x`%`y`.", volatility.Immutable),
		decimalOverload2("x", "y", func(x, y *apd.Decimal) (tree.Datum, error) {
			if y.Sign() == 0 {
				return nil, tree.ErrDivByZero
			}
			dd := &tree.DDecimal{}
			_, err := tree.HighPrecisionCtx.Rem(&dd.Decimal, x, y)
			return dd, err
		}, "Calculates `x`%`y`.", volatility.Immutable),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "x", Typ: types.Int}, {Name: "y", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				y := tree.MustBeDInt(args[1])
				if y == 0 {
					return nil, tree.ErrDivByZero
				}
				x := tree.MustBeDInt(args[0])
				return tree.NewDInt(x % y), nil
			},
			Info:       "Calculates `x`%`y`.",
			Volatility: volatility.Immutable,
		},
	),

	"pi": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.NewDFloat(math.Pi), nil
			},
			Info:       "Returns the value for pi (3.141592653589793).",
			Volatility: volatility.Immutable,
		},
	),

	"pow":   powImpls(),
	"power": powImpls(),

	"radians": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(x * degToRad)), nil
		}, "Converts `val` as a degree value to a radians value.", volatility.Immutable),
	),

	"round": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.RoundToEven(x))), nil
		}, "Rounds `val` to the nearest integer using half to even (banker's) rounding.", volatility.Immutable),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			return roundDecimal(x, 0)
		}, "Rounds `val` to the nearest integer, half away from zero: "+
			"round(+/-2.4) = +/-2, round(+/-2.5) = +/-3.", volatility.Immutable),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "input", Typ: types.Float}, {Name: "decimal_accuracy", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "input", Typ: types.Decimal}, {Name: "decimal_accuracy", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Decimal),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				// TODO(mjibson): make sure this fits in an int32.
				scale := int32(tree.MustBeDInt(args[1]))
				return roundDecimal(&args[0].(*tree.DDecimal).Decimal, scale)
			},
			Info: "Keeps `decimal_accuracy` number of figures to the right of the zero position " +
				"in `input` using half away from zero rounding. If `decimal_accuracy` " +
				"is not in the range -2^31...(2^31-1), the results are undefined.",
			Volatility: volatility.Immutable,
		},
	),

	"sin": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Sin(x))), nil
		}, "Calculates the sine of `val`.", volatility.Immutable),
	),

	"sind": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Sin(degToRad * x))), nil
		}, "Calculates the sine of `val` where `val` is in degrees.", volatility.Immutable),
	),

	"sinh": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Sinh(x))), nil
		}, "Calculates the hyperbolic sine of `val`.", volatility.Immutable),
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
			"negative.", volatility.Immutable),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			d := &tree.DDecimal{}
			d.Decimal.SetInt64(int64(x.Sign()))
			return d, nil
		}, "Determines the sign of `val`: **1** for positive; **0** for 0 values; **-1** for "+
			"negative.", volatility.Immutable),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
	),

	"sqrt": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return eval.Sqrt(x)
		}, "Calculates the square root of `val`.", volatility.Immutable),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			return eval.DecimalSqrt(x)
		}, "Calculates the square root of `val`.", volatility.Immutable),
	),

	"tan": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Tan(x))), nil
		}, "Calculates the tangent of `val`.", volatility.Immutable),
	),

	"tand": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Tan(degToRad * x))), nil
		}, "Calculates the tangent of `val` where `val` is in degrees.", volatility.Immutable),
	),

	"tanh": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Tanh(x))), nil
		}, "Calculates the hyperbolic tangent of `val`.", volatility.Immutable),
	),

	"trunc": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Trunc(x))), nil
		}, "Truncates the decimal values of `val`.", volatility.Immutable),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			if x.Form == apd.NaN || x.Form == apd.Infinite {
				return &tree.DDecimal{Decimal: *x}, nil
			}
			dd := &tree.DDecimal{}
			x.Modf(&dd.Decimal, nil)
			return dd, nil
		}, "Truncates the decimal values of `val`.", volatility.Immutable),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.Decimal}, {Name: "scale", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Decimal),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				// The algorithm here is also used in shopspring/decimal; see
				// https://github.com/shopspring/decimal/blob/f55dd564545cec84cf84f7a53fb3025cdbec1c4f/decimal.go#L1315
				dec := tree.MustBeDDecimal(args[0]).Decimal
				scale := -int64(tree.MustBeDInt(args[1]))
				if scale > int64(tree.DecimalCtx.MaxExponent) {
					scale = int64(tree.DecimalCtx.MaxExponent)
				} else if scale < int64(tree.DecimalCtx.MinExponent) {
					scale = int64(tree.DecimalCtx.MinExponent)
				}
				if dec.Form == apd.NaN || dec.Form == apd.Infinite || scale == int64(dec.Exponent) {
					return &tree.DDecimal{Decimal: dec}, nil
				} else if scale >= (dec.NumDigits() + int64(dec.Exponent)) {
					return &tree.DDecimal{Decimal: *decimalZero}, nil
				}
				ret := &tree.DDecimal{}
				diff := math.Abs(float64(scale) - float64(dec.Exponent))
				expScale := apd.NewBigInt(0).Exp(bigTen, apd.NewBigInt(int64(diff)), nil)
				if scale > int64(dec.Exponent) {
					_ = ret.Coeff.Quo(&dec.Coeff, expScale)
				} else if scale < int64(dec.Exponent) {
					_ = ret.Coeff.Mul(&dec.Coeff, expScale)
				}
				ret.Exponent = int32(scale)
				ret.Negative = dec.Negative
				return ret, nil
			},
			Info:       "Truncate `val` to `scale` decimal places",
			Volatility: volatility.Immutable,
		},
	),

	"width_bucket": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ParamTypes{{Name: "operand", Typ: types.Decimal}, {Name: "b1", Typ: types.Decimal},
				{Name: "b2", Typ: types.Decimal}, {Name: "count", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				operand, _ := args[0].(*tree.DDecimal).Float64()
				b1, _ := args[1].(*tree.DDecimal).Float64()
				b2, _ := args[2].(*tree.DDecimal).Float64()
				if math.IsInf(operand, 0) || math.IsInf(b1, 0) || math.IsInf(b2, 0) {
					return nil, pgerror.New(
						pgcode.InvalidParameterValue,
						"operand, lower bound, and upper bound cannot be infinity",
					)
				}
				count := int(tree.MustBeDInt(args[3]))
				return tree.NewDInt(tree.DInt(widthBucket(operand, b1, b2, count))), nil
			},
			Info: "return the bucket number to which operand would be assigned in a histogram having count " +
				"equal-width buckets spanning the range b1 to b2.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{{Name: "operand", Typ: types.Int}, {Name: "b1", Typ: types.Int},
				{Name: "b2", Typ: types.Int}, {Name: "count", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				operand := float64(tree.MustBeDInt(args[0]))
				b1 := float64(tree.MustBeDInt(args[1]))
				b2 := float64(tree.MustBeDInt(args[2]))
				count := int(tree.MustBeDInt(args[3]))
				return tree.NewDInt(tree.DInt(widthBucket(operand, b1, b2, count))), nil
			},
			Info: "return the bucket number to which operand would be assigned in a histogram having count " +
				"equal-width buckets spanning the range b1 to b2.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "operand", Typ: types.AnyElement}, {Name: "thresholds", Typ: types.AnyArray}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				operand := args[0]
				thresholds := tree.MustBeDArray(args[1])

				if !operand.ResolvedType().Equivalent(thresholds.ParamTyp) {
					return tree.NewDInt(0), errors.New("operand and thresholds must be of the same type")
				}

				for i, v := range thresholds.Array {
					if cmp, err := operand.Compare(ctx, evalCtx, v); err != nil {
						return tree.NewDInt(0), err
					} else if cmp < 0 {
						return tree.NewDInt(tree.DInt(i)), nil
					}
				}

				return tree.NewDInt(tree.DInt(thresholds.Len())), nil
			},
			Info: "return the bucket number to which operand would be assigned given an array listing the " +
				"lower bounds of the buckets; returns 0 for an input less than the first lower bound; the " +
				"thresholds array must be sorted, smallest first, or unexpected results will be obtained",
			Volatility: volatility.Immutable,
		},
	),
}

func ceilImpl() builtinDefinition {
	return makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Ceil(x))), nil
		}, "Calculates the smallest integer not smaller than `val`.", volatility.Immutable),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			dd := &tree.DDecimal{}
			_, err := tree.ExactCtx.Ceil(&dd.Decimal, x)
			if dd.IsZero() {
				dd.Negative = false
			}
			return dd, err
		}, "Calculates the smallest integer not smaller than `val`.", volatility.Immutable),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.NewDFloat(tree.DFloat(float64(*args[0].(*tree.DInt)))), nil
			},
			Info:       "Calculates the smallest integer not smaller than `val`.",
			Volatility: volatility.Immutable,
		},
	)
}

func powImpls() builtinDefinition {
	return makeBuiltin(defProps(),
		floatOverload2("x", "y", func(x, y float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Pow(x, y))), nil
		}, "Calculates `x`^`y`.", volatility.Immutable),
		decimalOverload2("x", "y", func(x, y *apd.Decimal) (tree.Datum, error) {
			dd := &tree.DDecimal{}
			_, err := tree.DecimalCtx.Pow(&dd.Decimal, x, y)
			return dd, err
		}, "Calculates `x`^`y`.", volatility.Immutable),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "x", Typ: types.Int},
				{Name: "y", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return eval.IntPow(tree.MustBeDInt(args[0]), tree.MustBeDInt(args[1]))
			},
			Info:       "Calculates `x`^`y`.",
			Volatility: volatility.Immutable,
		},
	)
}

func decimalLogFn(
	logFn func(*apd.Decimal, *apd.Decimal) (apd.Condition, error), info string,
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
	}, info, volatility.Immutable)
}

func floatOverload1(
	f func(float64) (tree.Datum, error), info string, volatility volatility.V,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ParamTypes{{Name: "val", Typ: types.Float}},
		ReturnType: tree.FixedReturnType(types.Float),
		Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
			return f(float64(*args[0].(*tree.DFloat)))
		},
		Info:       info,
		Volatility: volatility,
	}
}

func floatOverload2(
	a, b string, f func(float64, float64) (tree.Datum, error), info string, volatility volatility.V,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ParamTypes{{Name: a, Typ: types.Float}, {Name: b, Typ: types.Float}},
		ReturnType: tree.FixedReturnType(types.Float),
		Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
			return f(float64(*args[0].(*tree.DFloat)),
				float64(*args[1].(*tree.DFloat)))
		},
		Info:       info,
		Volatility: volatility,
	}
}

func decimalOverload1(
	f func(*apd.Decimal) (tree.Datum, error), info string, volatility volatility.V,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ParamTypes{{Name: "val", Typ: types.Decimal}},
		ReturnType: tree.FixedReturnType(types.Decimal),
		Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
	volatility volatility.V,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ParamTypes{{Name: a, Typ: types.Decimal}, {Name: b, Typ: types.Decimal}},
		ReturnType: tree.FixedReturnType(types.Decimal),
		Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
	if dd.IsZero() {
		dd.Negative = false
	}
	return dd, err
}

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
