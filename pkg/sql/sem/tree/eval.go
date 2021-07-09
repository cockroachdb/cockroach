// Copyright 2015 The Cockroach Authors.
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
	"context"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/arith"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

var (
	// ErrIntOutOfRange is reported when integer arithmetic overflows.
	ErrIntOutOfRange = pgerror.New(pgcode.NumericValueOutOfRange, "integer out of range")
	// ErrInt4OutOfRange is reported when casting to INT4 overflows.
	ErrInt4OutOfRange = pgerror.New(pgcode.NumericValueOutOfRange, "integer out of range for type int4")
	// ErrInt2OutOfRange is reported when casting to INT2 overflows.
	ErrInt2OutOfRange = pgerror.New(pgcode.NumericValueOutOfRange, "integer out of range for type int2")
	// ErrFloatOutOfRange is reported when float arithmetic overflows.
	ErrFloatOutOfRange = pgerror.New(pgcode.NumericValueOutOfRange, "float out of range")
	errDecOutOfRange   = pgerror.New(pgcode.NumericValueOutOfRange, "decimal out of range")

	// ErrDivByZero is reported on a division by zero.
	ErrDivByZero       = pgerror.New(pgcode.DivisionByZero, "division by zero")
	errSqrtOfNegNumber = pgerror.New(pgcode.InvalidArgumentForPowerFunction, "cannot take square root of a negative number")

	// ErrShiftArgOutOfRange is reported when a shift argument is out of range.
	ErrShiftArgOutOfRange = pgerror.New(pgcode.InvalidParameterValue, "shift argument out of range")

	big10E6  = big.NewInt(1e6)
	big10E10 = big.NewInt(1e10)
)

// NewCannotMixBitArraySizesError creates an error for the case when a bitwise
// aggregate function is called on bit arrays with different sizes.
func NewCannotMixBitArraySizesError(op string) error {
	return pgerror.Newf(pgcode.StringDataLengthMismatch,
		"cannot %s bit strings of different sizes", op)
}

// UnaryOp is a unary operator.
type UnaryOp struct {
	Typ        *types.T
	ReturnType *types.T
	Fn         func(*EvalContext, Datum) (Datum, error)
	Volatility Volatility

	types   TypeList
	retType ReturnTyper

	// counter, if non-nil, should be incremented every time the
	// operator is type checked.
	counter telemetry.Counter
}

func (op *UnaryOp) params() TypeList {
	return op.types
}

func (op *UnaryOp) returnType() ReturnTyper {
	return op.retType
}

func (*UnaryOp) preferred() bool {
	return false
}

func unaryOpFixups(
	ops map[UnaryOperatorSymbol]unaryOpOverload,
) map[UnaryOperatorSymbol]unaryOpOverload {
	for op, overload := range ops {
		for i, impl := range overload {
			casted := impl.(*UnaryOp)
			casted.types = ArgTypes{{"arg", casted.Typ}}
			casted.retType = FixedReturnType(casted.ReturnType)
			ops[op][i] = casted
		}
	}
	return ops
}

// unaryOpOverload is an overloaded set of unary operator implementations.
type unaryOpOverload []overloadImpl

// UnaryOps contains the unary operations indexed by operation type.
var UnaryOps = unaryOpFixups(map[UnaryOperatorSymbol]unaryOpOverload{
	UnaryMinus: {
		&UnaryOp{
			Typ:        types.Int,
			ReturnType: types.Int,
			Fn: func(_ *EvalContext, d Datum) (Datum, error) {
				i := MustBeDInt(d)
				if i == math.MinInt64 {
					return nil, ErrIntOutOfRange
				}
				return NewDInt(-i), nil
			},
			Volatility: VolatilityImmutable,
		},
		&UnaryOp{
			Typ:        types.Float,
			ReturnType: types.Float,
			Fn: func(_ *EvalContext, d Datum) (Datum, error) {
				return NewDFloat(-*d.(*DFloat)), nil
			},
			Volatility: VolatilityImmutable,
		},
		&UnaryOp{
			Typ:        types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, d Datum) (Datum, error) {
				dec := &d.(*DDecimal).Decimal
				dd := &DDecimal{}
				dd.Decimal.Neg(dec)
				return dd, nil
			},
			Volatility: VolatilityImmutable,
		},
		&UnaryOp{
			Typ:        types.Interval,
			ReturnType: types.Interval,
			Fn: func(_ *EvalContext, d Datum) (Datum, error) {
				i := d.(*DInterval).Duration
				i.SetNanos(-i.Nanos())
				i.Days = -i.Days
				i.Months = -i.Months
				return &DInterval{Duration: i}, nil
			},
			Volatility: VolatilityImmutable,
		},
	},

	UnaryComplement: {
		&UnaryOp{
			Typ:        types.Int,
			ReturnType: types.Int,
			Fn: func(_ *EvalContext, d Datum) (Datum, error) {
				return NewDInt(^MustBeDInt(d)), nil
			},
			Volatility: VolatilityImmutable,
		},
		&UnaryOp{
			Typ:        types.VarBit,
			ReturnType: types.VarBit,
			Fn: func(_ *EvalContext, d Datum) (Datum, error) {
				p := MustBeDBitArray(d)
				return &DBitArray{BitArray: bitarray.Not(p.BitArray)}, nil
			},
			Volatility: VolatilityImmutable,
		},
		&UnaryOp{
			Typ:        types.INet,
			ReturnType: types.INet,
			Fn: func(_ *EvalContext, d Datum) (Datum, error) {
				ipAddr := MustBeDIPAddr(d).IPAddr
				return NewDIPAddr(DIPAddr{ipAddr.Complement()}), nil
			},
			Volatility: VolatilityImmutable,
		},
	},

	UnarySqrt: {
		&UnaryOp{
			Typ:        types.Float,
			ReturnType: types.Float,
			Fn: func(_ *EvalContext, d Datum) (Datum, error) {
				return Sqrt(float64(*d.(*DFloat)))
			},
			Volatility: VolatilityImmutable,
		},
		&UnaryOp{
			Typ:        types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, d Datum) (Datum, error) {
				dec := &d.(*DDecimal).Decimal
				return DecimalSqrt(dec)
			},
			Volatility: VolatilityImmutable,
		},
	},

	UnaryCbrt: {
		&UnaryOp{
			Typ:        types.Float,
			ReturnType: types.Float,
			Fn: func(_ *EvalContext, d Datum) (Datum, error) {
				return Cbrt(float64(*d.(*DFloat)))
			},
			Volatility: VolatilityImmutable,
		},
		&UnaryOp{
			Typ:        types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, d Datum) (Datum, error) {
				dec := &d.(*DDecimal).Decimal
				return DecimalCbrt(dec)
			},
			Volatility: VolatilityImmutable,
		},
	},
})

// TwoArgFn is a function that operates on two Datum arguments.
type TwoArgFn func(*EvalContext, Datum, Datum) (Datum, error)

// BinOp is a binary operator.
type BinOp struct {
	LeftType     *types.T
	RightType    *types.T
	ReturnType   *types.T
	NullableArgs bool
	Fn           TwoArgFn
	Volatility   Volatility

	types   TypeList
	retType ReturnTyper

	// counter, if non-nil, should be incremented every time the
	// operator is type checked.
	counter telemetry.Counter
}

func (op *BinOp) params() TypeList {
	return op.types
}

func (op *BinOp) matchParams(l, r *types.T) bool {
	return op.params().MatchAt(l, 0) && op.params().MatchAt(r, 1)
}

func (op *BinOp) returnType() ReturnTyper {
	return op.retType
}

func (*BinOp) preferred() bool {
	return false
}

// AppendToMaybeNullArray appends an element to an array. If the first
// argument is NULL, an array of one element is created.
func AppendToMaybeNullArray(typ *types.T, left Datum, right Datum) (Datum, error) {
	result := NewDArray(typ)
	if left != DNull {
		for _, e := range MustBeDArray(left).Array {
			if err := result.Append(e); err != nil {
				return nil, err
			}
		}
	}
	if err := result.Append(right); err != nil {
		return nil, err
	}
	return result, nil
}

// PrependToMaybeNullArray prepends an element in the front of an arrray.
// If the argument is NULL, an array of one element is created.
func PrependToMaybeNullArray(typ *types.T, left Datum, right Datum) (Datum, error) {
	result := NewDArray(typ)
	if err := result.Append(left); err != nil {
		return nil, err
	}
	if right != DNull {
		for _, e := range MustBeDArray(right).Array {
			if err := result.Append(e); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

// TODO(justin): these might be improved by making arrays into an interface and
// then introducing a ConcatenatedArray implementation which just references two
// existing arrays. This would optimize the common case of appending an element
// (or array) to an array from O(n) to O(1).
func initArrayElementConcatenation() {
	for _, t := range types.Scalar {
		typ := t
		BinOps[Concat] = append(BinOps[Concat], &BinOp{
			LeftType:     types.MakeArray(typ),
			RightType:    typ,
			ReturnType:   types.MakeArray(typ),
			NullableArgs: true,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return AppendToMaybeNullArray(typ, left, right)
			},
			Volatility: VolatilityImmutable,
		})

		BinOps[Concat] = append(BinOps[Concat], &BinOp{
			LeftType:     typ,
			RightType:    types.MakeArray(typ),
			ReturnType:   types.MakeArray(typ),
			NullableArgs: true,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return PrependToMaybeNullArray(typ, left, right)
			},
			Volatility: VolatilityImmutable,
		})
	}
}

// ConcatArrays concatenates two arrays.
func ConcatArrays(typ *types.T, left Datum, right Datum) (Datum, error) {
	if left == DNull && right == DNull {
		return DNull, nil
	}
	result := NewDArray(typ)
	if left != DNull {
		for _, e := range MustBeDArray(left).Array {
			if err := result.Append(e); err != nil {
				return nil, err
			}
		}
	}
	if right != DNull {
		for _, e := range MustBeDArray(right).Array {
			if err := result.Append(e); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

// ArrayContains return true if the haystack contains all needles.
func ArrayContains(ctx *EvalContext, haystack *DArray, needles *DArray) (*DBool, error) {
	if !haystack.ParamTyp.Equivalent(needles.ParamTyp) {
		return DBoolFalse, pgerror.New(pgcode.DatatypeMismatch, "cannot compare arrays with different element types")
	}
	for _, needle := range needles.Array {
		// Nulls don't compare to each other in @> syntax.
		if needle == DNull {
			return DBoolFalse, nil
		}
		var found bool
		for _, hay := range haystack.Array {
			if needle.Compare(ctx, hay) == 0 {
				found = true
				break
			}
		}
		if !found {
			return DBoolFalse, nil
		}
	}
	return DBoolTrue, nil
}

// JSONExistsAny return true if any value in dArray is exist in the json
func JSONExistsAny(_ *EvalContext, json DJSON, dArray *DArray) (*DBool, error) {
	// TODO(justin): this can be optimized.
	for _, k := range dArray.Array {
		if k == DNull {
			continue
		}
		e, err := json.JSON.Exists(string(MustBeDString(k)))
		if err != nil {
			return nil, err
		}
		if e {
			return DBoolTrue, nil
		}
	}
	return DBoolFalse, nil
}

func initArrayToArrayConcatenation() {
	for _, t := range types.Scalar {
		typ := t
		BinOps[Concat] = append(BinOps[Concat], &BinOp{
			LeftType:     types.MakeArray(typ),
			RightType:    types.MakeArray(typ),
			ReturnType:   types.MakeArray(typ),
			NullableArgs: true,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return ConcatArrays(typ, left, right)
			},
			Volatility: VolatilityImmutable,
		})
	}
}

// initNonArrayToNonArrayConcatenation initializes string + nonarrayelement
// and nonarrayelement + string concatenation.
func initNonArrayToNonArrayConcatenation() {
	addConcat := func(leftType, rightType *types.T, volatility Volatility) {
		BinOps[Concat] = append(BinOps[Concat], &BinOp{
			LeftType:     leftType,
			RightType:    rightType,
			ReturnType:   types.String,
			NullableArgs: false,
			Fn: func(evalCtx *EvalContext, left Datum, right Datum) (Datum, error) {
				if leftType == types.String {
					casted, err := PerformCast(evalCtx, right, types.String)
					if err != nil {
						return nil, err
					}
					return NewDString(string(MustBeDString(left)) + string(MustBeDString(casted))), nil
				}
				if rightType == types.String {
					casted, err := PerformCast(evalCtx, left, types.String)
					if err != nil {
						return nil, err
					}
					return NewDString(string(MustBeDString(casted)) + string(MustBeDString(right))), nil
				}
				return nil, errors.New("neither LHS or RHS matched DString")
			},
			Volatility: volatility,
		})
	}
	fromTypeToVolatility := make(map[types.Family]Volatility)
	for _, cast := range validCasts {
		if cast.to == types.StringFamily {
			fromTypeToVolatility[cast.from] = cast.volatility
		}
	}
	// We allow tuple + string concatenation, as well as any scalar types.
	for _, t := range append([]*types.T{types.AnyTuple}, types.Scalar...) {
		// Do not re-add String+String or String+Bytes, as they already exist
		// and have predefined correct behavior.
		if t != types.String && t != types.Bytes {
			addConcat(t, types.String, fromTypeToVolatility[t.Family()])
			addConcat(types.String, t, fromTypeToVolatility[t.Family()])
		}
	}
}

func init() {
	initArrayElementConcatenation()
	initArrayToArrayConcatenation()
	initNonArrayToNonArrayConcatenation()
}

func init() {
	for op, overload := range BinOps {
		for i, impl := range overload {
			casted := impl.(*BinOp)
			casted.types = ArgTypes{{"left", casted.LeftType}, {"right", casted.RightType}}
			casted.retType = FixedReturnType(casted.ReturnType)
			BinOps[op][i] = casted
		}
	}
}

// binOpOverload is an overloaded set of binary operator implementations.
type binOpOverload []overloadImpl

func (o binOpOverload) lookupImpl(left, right *types.T) (*BinOp, bool) {
	for _, fn := range o {
		casted := fn.(*BinOp)
		if casted.matchParams(left, right) {
			return casted, true
		}
	}
	return nil, false
}

// GetJSONPath is used for the #> and #>> operators.
func GetJSONPath(j json.JSON, ary DArray) (json.JSON, error) {
	// TODO(justin): this is slightly annoying because we have to allocate
	// a new array since the JSON package isn't aware of DArray.
	path := make([]string, len(ary.Array))
	for i, v := range ary.Array {
		if v == DNull {
			return nil, nil
		}
		path[i] = string(MustBeDString(v))
	}
	return json.FetchPath(j, path)
}

// BinOps contains the binary operations indexed by operation type.
var BinOps = map[BinaryOperatorSymbol]binOpOverload{
	Bitand: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDInt(MustBeDInt(left) & MustBeDInt(right)), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.VarBit,
			RightType:  types.VarBit,
			ReturnType: types.VarBit,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				lhs := MustBeDBitArray(left)
				rhs := MustBeDBitArray(right)
				if lhs.BitLen() != rhs.BitLen() {
					return nil, NewCannotMixBitArraySizesError("AND")
				}
				return &DBitArray{
					BitArray: bitarray.And(lhs.BitArray, rhs.BitArray),
				}, nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.INet,
			RightType:  types.INet,
			ReturnType: types.INet,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				ipAddr := MustBeDIPAddr(left).IPAddr
				other := MustBeDIPAddr(right).IPAddr
				newIPAddr, err := ipAddr.And(&other)
				return NewDIPAddr(DIPAddr{newIPAddr}), err
			},
			Volatility: VolatilityImmutable,
		},
	},

	Bitor: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDInt(MustBeDInt(left) | MustBeDInt(right)), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.VarBit,
			RightType:  types.VarBit,
			ReturnType: types.VarBit,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				lhs := MustBeDBitArray(left)
				rhs := MustBeDBitArray(right)
				if lhs.BitLen() != rhs.BitLen() {
					return nil, NewCannotMixBitArraySizesError("OR")
				}
				return &DBitArray{
					BitArray: bitarray.Or(lhs.BitArray, rhs.BitArray),
				}, nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.INet,
			RightType:  types.INet,
			ReturnType: types.INet,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				ipAddr := MustBeDIPAddr(left).IPAddr
				other := MustBeDIPAddr(right).IPAddr
				newIPAddr, err := ipAddr.Or(&other)
				return NewDIPAddr(DIPAddr{newIPAddr}), err
			},
			Volatility: VolatilityImmutable,
		},
	},

	Bitxor: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDInt(MustBeDInt(left) ^ MustBeDInt(right)), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.VarBit,
			RightType:  types.VarBit,
			ReturnType: types.VarBit,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				lhs := MustBeDBitArray(left)
				rhs := MustBeDBitArray(right)
				if lhs.BitLen() != rhs.BitLen() {
					return nil, NewCannotMixBitArraySizesError("XOR")
				}
				return &DBitArray{
					BitArray: bitarray.Xor(lhs.BitArray, rhs.BitArray),
				}, nil
			},
			Volatility: VolatilityImmutable,
		},
	},

	Plus: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				a, b := MustBeDInt(left), MustBeDInt(right)
				r, ok := arith.AddWithOverflow(int64(a), int64(b))
				if !ok {
					return nil, ErrIntOutOfRange
				}
				return NewDInt(DInt(r)), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDFloat(*left.(*DFloat) + *right.(*DFloat)), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				_, err := ExactCtx.Add(&dd.Decimal, l, r)
				return dd, err
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := MustBeDInt(right)
				dd := &DDecimal{}
				dd.SetInt64(int64(r))
				_, err := ExactCtx.Add(&dd.Decimal, l, &dd.Decimal)
				return dd, err
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := MustBeDInt(left)
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				dd.SetInt64(int64(l))
				_, err := ExactCtx.Add(&dd.Decimal, &dd.Decimal, r)
				return dd, err
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Date,
			RightType:  types.Int,
			ReturnType: types.Date,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				d, err := left.(*DDate).AddDays(int64(MustBeDInt(right)))
				if err != nil {
					return nil, err
				}
				return NewDDate(d), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Date,
			ReturnType: types.Date,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				d, err := right.(*DDate).AddDays(int64(MustBeDInt(left)))
				if err != nil {
					return nil, err
				}
				return NewDDate(d), nil

			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Date,
			RightType:  types.Time,
			ReturnType: types.Timestamp,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				leftTime, err := left.(*DDate).ToTime()
				if err != nil {
					return nil, err
				}
				t := time.Duration(*right.(*DTime)) * time.Microsecond
				return MakeDTimestamp(leftTime.Add(t), time.Microsecond)
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Time,
			RightType:  types.Date,
			ReturnType: types.Timestamp,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				rightTime, err := right.(*DDate).ToTime()
				if err != nil {
					return nil, err
				}
				t := time.Duration(*left.(*DTime)) * time.Microsecond
				return MakeDTimestamp(rightTime.Add(t), time.Microsecond)
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Date,
			RightType:  types.TimeTZ,
			ReturnType: types.TimestampTZ,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				leftTime, err := left.(*DDate).ToTime()
				if err != nil {
					return nil, err
				}
				t := leftTime.Add(right.(*DTimeTZ).ToDuration())
				return MakeDTimestampTZ(t, time.Microsecond)
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.TimeTZ,
			RightType:  types.Date,
			ReturnType: types.TimestampTZ,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				rightTime, err := right.(*DDate).ToTime()
				if err != nil {
					return nil, err
				}
				t := rightTime.Add(left.(*DTimeTZ).ToDuration())
				return MakeDTimestampTZ(t, time.Microsecond)
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Time,
			RightType:  types.Interval,
			ReturnType: types.Time,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				t := timeofday.TimeOfDay(*left.(*DTime))
				return MakeDTime(t.Add(right.(*DInterval).Duration)), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Time,
			ReturnType: types.Time,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				t := timeofday.TimeOfDay(*right.(*DTime))
				return MakeDTime(t.Add(left.(*DInterval).Duration)), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.TimeTZ,
			RightType:  types.Interval,
			ReturnType: types.TimeTZ,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				t := left.(*DTimeTZ)
				duration := right.(*DInterval).Duration
				return NewDTimeTZFromOffset(t.Add(duration), t.OffsetSecs), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.TimeTZ,
			ReturnType: types.TimeTZ,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				t := right.(*DTimeTZ)
				duration := left.(*DInterval).Duration
				return NewDTimeTZFromOffset(t.Add(duration), t.OffsetSecs), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Timestamp,
			RightType:  types.Interval,
			ReturnType: types.Timestamp,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return MakeDTimestamp(duration.Add(
					left.(*DTimestamp).Time, right.(*DInterval).Duration), time.Microsecond)
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Timestamp,
			ReturnType: types.Timestamp,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return MakeDTimestamp(duration.Add(
					right.(*DTimestamp).Time, left.(*DInterval).Duration), time.Microsecond)
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.TimestampTZ,
			RightType:  types.Interval,
			ReturnType: types.TimestampTZ,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				// Convert time to be in the given timezone, as math relies on matching timezones..
				t := duration.Add(left.(*DTimestampTZ).Time.In(ctx.GetLocation()), right.(*DInterval).Duration)
				return MakeDTimestampTZ(t, time.Microsecond)
			},
			Volatility: VolatilityStable,
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.TimestampTZ,
			ReturnType: types.TimestampTZ,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				// Convert time to be in the given timezone, as math relies on matching timezones..
				t := duration.Add(right.(*DTimestampTZ).Time.In(ctx.GetLocation()), left.(*DInterval).Duration)
				return MakeDTimestampTZ(t, time.Microsecond)
			},
			Volatility: VolatilityStable,
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Interval,
			ReturnType: types.Interval,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return &DInterval{Duration: left.(*DInterval).Duration.Add(right.(*DInterval).Duration)}, nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Date,
			RightType:  types.Interval,
			ReturnType: types.Timestamp,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				leftTime, err := left.(*DDate).ToTime()
				if err != nil {
					return nil, err
				}
				t := duration.Add(leftTime, right.(*DInterval).Duration)
				return MakeDTimestamp(t, time.Microsecond)
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Date,
			ReturnType: types.Timestamp,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				rightTime, err := right.(*DDate).ToTime()
				if err != nil {
					return nil, err
				}
				t := duration.Add(rightTime, left.(*DInterval).Duration)
				return MakeDTimestamp(t, time.Microsecond)
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.INet,
			RightType:  types.Int,
			ReturnType: types.INet,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				ipAddr := MustBeDIPAddr(left).IPAddr
				i := MustBeDInt(right)
				newIPAddr, err := ipAddr.Add(int64(i))
				return NewDIPAddr(DIPAddr{newIPAddr}), err
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.INet,
			ReturnType: types.INet,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				i := MustBeDInt(left)
				ipAddr := MustBeDIPAddr(right).IPAddr
				newIPAddr, err := ipAddr.Add(int64(i))
				return NewDIPAddr(DIPAddr{newIPAddr}), err
			},
			Volatility: VolatilityImmutable,
		},
	},

	Minus: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				a, b := MustBeDInt(left), MustBeDInt(right)
				r, ok := arith.SubWithOverflow(int64(a), int64(b))
				if !ok {
					return nil, ErrIntOutOfRange
				}
				return NewDInt(DInt(r)), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDFloat(*left.(*DFloat) - *right.(*DFloat)), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				_, err := ExactCtx.Sub(&dd.Decimal, l, r)
				return dd, err
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := MustBeDInt(right)
				dd := &DDecimal{}
				dd.SetInt64(int64(r))
				_, err := ExactCtx.Sub(&dd.Decimal, l, &dd.Decimal)
				return dd, err
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := MustBeDInt(left)
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				dd.SetInt64(int64(l))
				_, err := ExactCtx.Sub(&dd.Decimal, &dd.Decimal, r)
				return dd, err
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Date,
			RightType:  types.Int,
			ReturnType: types.Date,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				d, err := left.(*DDate).SubDays(int64(MustBeDInt(right)))
				if err != nil {
					return nil, err
				}
				return NewDDate(d), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Date,
			RightType:  types.Date,
			ReturnType: types.Int,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l, r := left.(*DDate).Date, right.(*DDate).Date
				if !l.IsFinite() || !r.IsFinite() {
					return nil, pgerror.New(pgcode.DatetimeFieldOverflow, "cannot subtract infinite dates")
				}
				a := l.PGEpochDays()
				b := r.PGEpochDays()
				// This can't overflow because they are upconverted from int32 to int64.
				return NewDInt(DInt(int64(a) - int64(b))), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Date,
			RightType:  types.Time,
			ReturnType: types.Timestamp,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				leftTime, err := left.(*DDate).ToTime()
				if err != nil {
					return nil, err
				}
				t := time.Duration(*right.(*DTime)) * time.Microsecond
				return MakeDTimestamp(leftTime.Add(-1*t), time.Microsecond)
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Time,
			RightType:  types.Time,
			ReturnType: types.Interval,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				t1 := timeofday.TimeOfDay(*left.(*DTime))
				t2 := timeofday.TimeOfDay(*right.(*DTime))
				diff := timeofday.Difference(t1, t2)
				return &DInterval{Duration: diff}, nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Timestamp,
			RightType:  types.Timestamp,
			ReturnType: types.Interval,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				nanos := left.(*DTimestamp).Sub(right.(*DTimestamp).Time).Nanoseconds()
				return &DInterval{Duration: duration.MakeDurationJustifyHours(nanos, 0, 0)}, nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.TimestampTZ,
			RightType:  types.TimestampTZ,
			ReturnType: types.Interval,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				nanos := left.(*DTimestampTZ).Sub(right.(*DTimestampTZ).Time).Nanoseconds()
				return &DInterval{Duration: duration.MakeDurationJustifyHours(nanos, 0, 0)}, nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Timestamp,
			RightType:  types.TimestampTZ,
			ReturnType: types.Interval,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				// These two quantities aren't directly comparable. Convert the
				// TimestampTZ to a timestamp first.
				stripped, err := right.(*DTimestampTZ).stripTimeZone(ctx)
				if err != nil {
					return nil, err
				}
				nanos := left.(*DTimestamp).Sub(stripped.Time).Nanoseconds()
				return &DInterval{Duration: duration.MakeDurationJustifyHours(nanos, 0, 0)}, nil
			},
			Volatility: VolatilityStable,
		},
		&BinOp{
			LeftType:   types.TimestampTZ,
			RightType:  types.Timestamp,
			ReturnType: types.Interval,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				// These two quantities aren't directly comparable. Convert the
				// TimestampTZ to a timestamp first.
				stripped, err := left.(*DTimestampTZ).stripTimeZone(ctx)
				if err != nil {
					return nil, err
				}
				nanos := stripped.Sub(right.(*DTimestamp).Time).Nanoseconds()
				return &DInterval{Duration: duration.MakeDurationJustifyHours(nanos, 0, 0)}, nil
			},
			Volatility: VolatilityStable,
		},
		&BinOp{
			LeftType:   types.Time,
			RightType:  types.Interval,
			ReturnType: types.Time,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				t := timeofday.TimeOfDay(*left.(*DTime))
				return MakeDTime(t.Add(right.(*DInterval).Duration.Mul(-1))), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.TimeTZ,
			RightType:  types.Interval,
			ReturnType: types.TimeTZ,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				t := left.(*DTimeTZ)
				duration := right.(*DInterval).Duration
				return NewDTimeTZFromOffset(t.Add(duration.Mul(-1)), t.OffsetSecs), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Timestamp,
			RightType:  types.Interval,
			ReturnType: types.Timestamp,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return MakeDTimestamp(duration.Add(
					left.(*DTimestamp).Time, right.(*DInterval).Duration.Mul(-1)), time.Microsecond)
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.TimestampTZ,
			RightType:  types.Interval,
			ReturnType: types.TimestampTZ,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				t := duration.Add(
					left.(*DTimestampTZ).Time.In(ctx.GetLocation()),
					right.(*DInterval).Duration.Mul(-1),
				)
				return MakeDTimestampTZ(t, time.Microsecond)
			},
			Volatility: VolatilityStable,
		},
		&BinOp{
			LeftType:   types.Date,
			RightType:  types.Interval,
			ReturnType: types.Timestamp,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				leftTime, err := left.(*DDate).ToTime()
				if err != nil {
					return nil, err
				}
				t := duration.Add(leftTime, right.(*DInterval).Duration.Mul(-1))
				return MakeDTimestamp(t, time.Microsecond)
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Interval,
			ReturnType: types.Interval,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return &DInterval{Duration: left.(*DInterval).Duration.Sub(right.(*DInterval).Duration)}, nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Jsonb,
			RightType:  types.String,
			ReturnType: types.Jsonb,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				j, _, err := left.(*DJSON).JSON.RemoveString(string(MustBeDString(right)))
				if err != nil {
					return nil, err
				}
				return &DJSON{j}, nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Jsonb,
			RightType:  types.Int,
			ReturnType: types.Jsonb,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				j, _, err := left.(*DJSON).JSON.RemoveIndex(int(MustBeDInt(right)))
				if err != nil {
					return nil, err
				}
				return &DJSON{j}, nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Jsonb,
			RightType:  types.MakeArray(types.String),
			ReturnType: types.Jsonb,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				j := left.(*DJSON).JSON
				arr := *MustBeDArray(right)

				for _, str := range arr.Array {
					if str == DNull {
						continue
					}
					var err error
					j, _, err = j.RemoveString(string(MustBeDString(str)))
					if err != nil {
						return nil, err
					}
				}
				return &DJSON{j}, nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.INet,
			RightType:  types.INet,
			ReturnType: types.Int,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				ipAddr := MustBeDIPAddr(left).IPAddr
				other := MustBeDIPAddr(right).IPAddr
				diff, err := ipAddr.SubIPAddr(&other)
				return NewDInt(DInt(diff)), err
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			// Note: postgres ver 10 does NOT have Int - INet. Throws ERROR: 42883.
			LeftType:   types.INet,
			RightType:  types.Int,
			ReturnType: types.INet,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				ipAddr := MustBeDIPAddr(left).IPAddr
				i := MustBeDInt(right)
				newIPAddr, err := ipAddr.Sub(int64(i))
				return NewDIPAddr(DIPAddr{newIPAddr}), err
			},
			Volatility: VolatilityImmutable,
		},
	},

	Mult: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				// See Rob Pike's implementation from
				// https://groups.google.com/d/msg/golang-nuts/h5oSN5t3Au4/KaNQREhZh0QJ

				a, b := MustBeDInt(left), MustBeDInt(right)
				c := a * b
				if a == 0 || b == 0 || a == 1 || b == 1 {
					// ignore
				} else if a == math.MinInt64 || b == math.MinInt64 {
					// This test is required to detect math.MinInt64 * -1.
					return nil, ErrIntOutOfRange
				} else if c/b != a {
					return nil, ErrIntOutOfRange
				}
				return NewDInt(c), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDFloat(*left.(*DFloat) * *right.(*DFloat)), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				_, err := ExactCtx.Mul(&dd.Decimal, l, r)
				return dd, err
			},
			Volatility: VolatilityImmutable,
		},
		// The following two overloads are needed because DInt/DInt = DDecimal. Due
		// to this operation, normalization may sometimes create a DInt * DDecimal
		// operation.
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := MustBeDInt(right)
				dd := &DDecimal{}
				dd.SetInt64(int64(r))
				_, err := ExactCtx.Mul(&dd.Decimal, l, &dd.Decimal)
				return dd, err
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := MustBeDInt(left)
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				dd.SetInt64(int64(l))
				_, err := ExactCtx.Mul(&dd.Decimal, &dd.Decimal, r)
				return dd, err
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Interval,
			ReturnType: types.Interval,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return &DInterval{Duration: right.(*DInterval).Duration.Mul(int64(MustBeDInt(left)))}, nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Int,
			ReturnType: types.Interval,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return &DInterval{Duration: left.(*DInterval).Duration.Mul(int64(MustBeDInt(right)))}, nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Float,
			ReturnType: types.Interval,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				r := float64(*right.(*DFloat))
				return &DInterval{Duration: left.(*DInterval).Duration.MulFloat(r)}, nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Float,
			RightType:  types.Interval,
			ReturnType: types.Interval,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := float64(*left.(*DFloat))
				return &DInterval{Duration: right.(*DInterval).Duration.MulFloat(l)}, nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Interval,
			ReturnType: types.Interval,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				t, err := l.Float64()
				if err != nil {
					return nil, err
				}
				return &DInterval{Duration: right.(*DInterval).Duration.MulFloat(t)}, nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Decimal,
			ReturnType: types.Interval,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				r := &right.(*DDecimal).Decimal
				t, err := r.Float64()
				if err != nil {
					return nil, err
				}
				return &DInterval{Duration: left.(*DInterval).Duration.MulFloat(t)}, nil
			},
			Volatility: VolatilityImmutable,
		},
	},

	Div: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				rInt := MustBeDInt(right)
				if rInt == 0 {
					return nil, ErrDivByZero
				}
				div := ctx.getTmpDec().SetInt64(int64(rInt))
				dd := &DDecimal{}
				dd.SetInt64(int64(MustBeDInt(left)))
				_, err := DecimalCtx.Quo(&dd.Decimal, &dd.Decimal, div)
				return dd, err
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				r := *right.(*DFloat)
				if r == 0.0 {
					return nil, ErrDivByZero
				}
				return NewDFloat(*left.(*DFloat) / r), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := &right.(*DDecimal).Decimal
				if r.IsZero() {
					return nil, ErrDivByZero
				}
				dd := &DDecimal{}
				_, err := DecimalCtx.Quo(&dd.Decimal, l, r)
				return dd, err
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := MustBeDInt(right)
				if r == 0 {
					return nil, ErrDivByZero
				}
				dd := &DDecimal{}
				dd.SetInt64(int64(r))
				_, err := DecimalCtx.Quo(&dd.Decimal, l, &dd.Decimal)
				return dd, err
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := MustBeDInt(left)
				r := &right.(*DDecimal).Decimal
				if r.IsZero() {
					return nil, ErrDivByZero
				}
				dd := &DDecimal{}
				dd.SetInt64(int64(l))
				_, err := DecimalCtx.Quo(&dd.Decimal, &dd.Decimal, r)
				return dd, err
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Int,
			ReturnType: types.Interval,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				rInt := MustBeDInt(right)
				if rInt == 0 {
					return nil, ErrDivByZero
				}
				return &DInterval{Duration: left.(*DInterval).Duration.Div(int64(rInt))}, nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Float,
			ReturnType: types.Interval,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				r := float64(*right.(*DFloat))
				if r == 0.0 {
					return nil, ErrDivByZero
				}
				return &DInterval{Duration: left.(*DInterval).Duration.DivFloat(r)}, nil
			},
			Volatility: VolatilityImmutable,
		},
	},

	FloorDiv: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				rInt := MustBeDInt(right)
				if rInt == 0 {
					return nil, ErrDivByZero
				}
				return NewDInt(MustBeDInt(left) / rInt), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := float64(*left.(*DFloat))
				r := float64(*right.(*DFloat))
				if r == 0.0 {
					return nil, ErrDivByZero
				}
				return NewDFloat(DFloat(math.Trunc(l / r))), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := &right.(*DDecimal).Decimal
				if r.IsZero() {
					return nil, ErrDivByZero
				}
				dd := &DDecimal{}
				_, err := HighPrecisionCtx.QuoInteger(&dd.Decimal, l, r)
				return dd, err
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := MustBeDInt(right)
				if r == 0 {
					return nil, ErrDivByZero
				}
				dd := &DDecimal{}
				dd.SetInt64(int64(r))
				_, err := HighPrecisionCtx.QuoInteger(&dd.Decimal, l, &dd.Decimal)
				return dd, err
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := MustBeDInt(left)
				r := &right.(*DDecimal).Decimal
				if r.IsZero() {
					return nil, ErrDivByZero
				}
				dd := &DDecimal{}
				dd.SetInt64(int64(l))
				_, err := HighPrecisionCtx.QuoInteger(&dd.Decimal, &dd.Decimal, r)
				return dd, err
			},
			Volatility: VolatilityImmutable,
		},
	},

	Mod: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				r := MustBeDInt(right)
				if r == 0 {
					return nil, ErrDivByZero
				}
				return NewDInt(MustBeDInt(left) % r), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := float64(*left.(*DFloat))
				r := float64(*right.(*DFloat))
				if r == 0.0 {
					return nil, ErrDivByZero
				}
				return NewDFloat(DFloat(math.Mod(l, r))), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := &right.(*DDecimal).Decimal
				if r.IsZero() {
					return nil, ErrDivByZero
				}
				dd := &DDecimal{}
				_, err := HighPrecisionCtx.Rem(&dd.Decimal, l, r)
				return dd, err
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := MustBeDInt(right)
				if r == 0 {
					return nil, ErrDivByZero
				}
				dd := &DDecimal{}
				dd.SetInt64(int64(r))
				_, err := HighPrecisionCtx.Rem(&dd.Decimal, l, &dd.Decimal)
				return dd, err
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := MustBeDInt(left)
				r := &right.(*DDecimal).Decimal
				if r.IsZero() {
					return nil, ErrDivByZero
				}
				dd := &DDecimal{}
				dd.SetInt64(int64(l))
				_, err := HighPrecisionCtx.Rem(&dd.Decimal, &dd.Decimal, r)
				return dd, err
			},
			Volatility: VolatilityImmutable,
		},
	},

	Concat: {
		&BinOp{
			LeftType:   types.String,
			RightType:  types.String,
			ReturnType: types.String,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDString(string(MustBeDString(left) + MustBeDString(right))), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Bytes,
			RightType:  types.Bytes,
			ReturnType: types.Bytes,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDBytes(*left.(*DBytes) + *right.(*DBytes)), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.VarBit,
			RightType:  types.VarBit,
			ReturnType: types.VarBit,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				lhs := MustBeDBitArray(left)
				rhs := MustBeDBitArray(right)
				return &DBitArray{
					BitArray: bitarray.Concat(lhs.BitArray, rhs.BitArray),
				}, nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Jsonb,
			RightType:  types.Jsonb,
			ReturnType: types.Jsonb,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				j, err := MustBeDJSON(left).JSON.Concat(MustBeDJSON(right).JSON)
				if err != nil {
					return nil, err
				}
				return &DJSON{j}, nil
			},
			Volatility: VolatilityImmutable,
		},
	},

	// TODO(pmattis): Check that the shift is valid.
	LShift: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				rval := MustBeDInt(right)
				if rval < 0 || rval >= 64 {
					telemetry.Inc(sqltelemetry.LargeLShiftArgumentCounter)
					return nil, ErrShiftArgOutOfRange
				}
				return NewDInt(MustBeDInt(left) << uint(rval)), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.VarBit,
			RightType:  types.Int,
			ReturnType: types.VarBit,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				lhs := MustBeDBitArray(left)
				rhs := MustBeDInt(right)
				return &DBitArray{
					BitArray: lhs.BitArray.LeftShiftAny(int64(rhs)),
				}, nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.INet,
			RightType:  types.INet,
			ReturnType: types.Bool,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				ipAddr := MustBeDIPAddr(left).IPAddr
				other := MustBeDIPAddr(right).IPAddr
				return MakeDBool(DBool(ipAddr.ContainedBy(&other))), nil
			},
			Volatility: VolatilityImmutable,
		},
	},

	RShift: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				rval := MustBeDInt(right)
				if rval < 0 || rval >= 64 {
					telemetry.Inc(sqltelemetry.LargeRShiftArgumentCounter)
					return nil, ErrShiftArgOutOfRange
				}
				return NewDInt(MustBeDInt(left) >> uint(rval)), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.VarBit,
			RightType:  types.Int,
			ReturnType: types.VarBit,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				lhs := MustBeDBitArray(left)
				rhs := MustBeDInt(right)
				return &DBitArray{
					BitArray: lhs.BitArray.LeftShiftAny(-int64(rhs)),
				}, nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.INet,
			RightType:  types.INet,
			ReturnType: types.Bool,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				ipAddr := MustBeDIPAddr(left).IPAddr
				other := MustBeDIPAddr(right).IPAddr
				return MakeDBool(DBool(ipAddr.Contains(&other))), nil
			},
			Volatility: VolatilityImmutable,
		},
	},

	Pow: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return IntPow(MustBeDInt(left), MustBeDInt(right))
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				f := math.Pow(float64(*left.(*DFloat)), float64(*right.(*DFloat)))
				return NewDFloat(DFloat(f)), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				_, err := DecimalCtx.Pow(&dd.Decimal, l, r)
				return dd, err
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := MustBeDInt(right)
				dd := &DDecimal{}
				dd.SetInt64(int64(r))
				_, err := DecimalCtx.Pow(&dd.Decimal, l, &dd.Decimal)
				return dd, err
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := MustBeDInt(left)
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				dd.SetInt64(int64(l))
				_, err := DecimalCtx.Pow(&dd.Decimal, &dd.Decimal, r)
				return dd, err
			},
			Volatility: VolatilityImmutable,
		},
	},

	JSONFetchVal: {
		&BinOp{
			LeftType:   types.Jsonb,
			RightType:  types.String,
			ReturnType: types.Jsonb,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				j, err := left.(*DJSON).JSON.FetchValKey(string(MustBeDString(right)))
				if err != nil {
					return nil, err
				}
				if j == nil {
					return DNull, nil
				}
				return &DJSON{j}, nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Jsonb,
			RightType:  types.Int,
			ReturnType: types.Jsonb,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				j, err := left.(*DJSON).JSON.FetchValIdx(int(MustBeDInt(right)))
				if err != nil {
					return nil, err
				}
				if j == nil {
					return DNull, nil
				}
				return &DJSON{j}, nil
			},
			Volatility: VolatilityImmutable,
		},
	},

	JSONFetchValPath: {
		&BinOp{
			LeftType:   types.Jsonb,
			RightType:  types.MakeArray(types.String),
			ReturnType: types.Jsonb,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				path, err := GetJSONPath(left.(*DJSON).JSON, *MustBeDArray(right))
				if err != nil {
					return nil, err
				}
				if path == nil {
					return DNull, nil
				}
				return &DJSON{path}, nil
			},
			Volatility: VolatilityImmutable,
		},
	},

	JSONFetchText: {
		&BinOp{
			LeftType:   types.Jsonb,
			RightType:  types.String,
			ReturnType: types.String,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				res, err := left.(*DJSON).JSON.FetchValKey(string(MustBeDString(right)))
				if err != nil {
					return nil, err
				}
				if res == nil {
					return DNull, nil
				}
				text, err := res.AsText()
				if err != nil {
					return nil, err
				}
				if text == nil {
					return DNull, nil
				}
				return NewDString(*text), nil
			},
			Volatility: VolatilityImmutable,
		},
		&BinOp{
			LeftType:   types.Jsonb,
			RightType:  types.Int,
			ReturnType: types.String,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				res, err := left.(*DJSON).JSON.FetchValIdx(int(MustBeDInt(right)))
				if err != nil {
					return nil, err
				}
				if res == nil {
					return DNull, nil
				}
				text, err := res.AsText()
				if err != nil {
					return nil, err
				}
				if text == nil {
					return DNull, nil
				}
				return NewDString(*text), nil
			},
			Volatility: VolatilityImmutable,
		},
	},

	JSONFetchTextPath: {
		&BinOp{
			LeftType:   types.Jsonb,
			RightType:  types.MakeArray(types.String),
			ReturnType: types.String,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				res, err := GetJSONPath(left.(*DJSON).JSON, *MustBeDArray(right))
				if err != nil {
					return nil, err
				}
				if res == nil {
					return DNull, nil
				}
				text, err := res.AsText()
				if err != nil {
					return nil, err
				}
				if text == nil {
					return DNull, nil
				}
				return NewDString(*text), nil
			},
			Volatility: VolatilityImmutable,
		},
	},
}

// CmpOp is a comparison operator.
type CmpOp struct {
	types TypeList

	LeftType  *types.T
	RightType *types.T

	// Datum return type is a union between *DBool and dNull.
	Fn TwoArgFn

	// counter, if non-nil, should be incremented every time the
	// operator is type checked.
	counter telemetry.Counter

	// If NullableArgs is false, the operator returns NULL
	// whenever either argument is NULL.
	NullableArgs bool

	Volatility Volatility

	isPreferred bool
}

func (op *CmpOp) params() TypeList {
	return op.types
}

func (op *CmpOp) matchParams(l, r *types.T) bool {
	return op.params().MatchAt(l, 0) && op.params().MatchAt(r, 1)
}

var cmpOpReturnType = FixedReturnType(types.Bool)

func (op *CmpOp) returnType() ReturnTyper {
	return cmpOpReturnType
}

func (op *CmpOp) preferred() bool {
	return op.isPreferred
}

func cmpOpFixups(
	cmpOps map[ComparisonOperatorSymbol]cmpOpOverload,
) map[ComparisonOperatorSymbol]cmpOpOverload {
	findVolatility := func(op ComparisonOperatorSymbol, t *types.T) Volatility {
		for _, impl := range cmpOps[EQ] {
			o := impl.(*CmpOp)
			if o.LeftType.Equivalent(t) && o.RightType.Equivalent(t) {
				return o.Volatility
			}
		}
		panic(errors.AssertionFailedf("could not find cmp op %s(%s,%s)", op, t, t))
	}

	// Array equality comparisons.
	for _, t := range types.Scalar {
		cmpOps[EQ] = append(cmpOps[EQ], &CmpOp{
			LeftType:   types.MakeArray(t),
			RightType:  types.MakeArray(t),
			Fn:         cmpOpScalarEQFn,
			Volatility: findVolatility(EQ, t),
		})
		cmpOps[LE] = append(cmpOps[LE], &CmpOp{
			LeftType:   types.MakeArray(t),
			RightType:  types.MakeArray(t),
			Fn:         cmpOpScalarLEFn,
			Volatility: findVolatility(LE, t),
		})
		cmpOps[LT] = append(cmpOps[LT], &CmpOp{
			LeftType:   types.MakeArray(t),
			RightType:  types.MakeArray(t),
			Fn:         cmpOpScalarLTFn,
			Volatility: findVolatility(LT, t),
		})

		cmpOps[IsNotDistinctFrom] = append(cmpOps[IsNotDistinctFrom], &CmpOp{
			LeftType:     types.MakeArray(t),
			RightType:    types.MakeArray(t),
			Fn:           cmpOpScalarIsFn,
			NullableArgs: true,
			Volatility:   findVolatility(IsNotDistinctFrom, t),
		})
	}

	for op, overload := range cmpOps {
		for i, impl := range overload {
			casted := impl.(*CmpOp)
			casted.types = ArgTypes{{"left", casted.LeftType}, {"right", casted.RightType}}
			cmpOps[op][i] = casted
		}
	}

	return cmpOps
}

// cmpOpOverload is an overloaded set of comparison operator implementations.
type cmpOpOverload []overloadImpl

func (o cmpOpOverload) LookupImpl(left, right *types.T) (*CmpOp, bool) {
	for _, fn := range o {
		casted := fn.(*CmpOp)
		if casted.matchParams(left, right) {
			return casted, true
		}
	}
	return nil, false
}

func makeCmpOpOverload(
	fn func(ctx *EvalContext, left, right Datum) (Datum, error),
	a, b *types.T,
	nullableArgs bool,
	v Volatility,
) *CmpOp {
	return &CmpOp{
		LeftType:     a,
		RightType:    b,
		Fn:           fn,
		NullableArgs: nullableArgs,
		Volatility:   v,
	}
}

func makeEqFn(a, b *types.T, v Volatility) *CmpOp {
	return makeCmpOpOverload(cmpOpScalarEQFn, a, b, false /* NullableArgs */, v)
}
func makeLtFn(a, b *types.T, v Volatility) *CmpOp {
	return makeCmpOpOverload(cmpOpScalarLTFn, a, b, false /* NullableArgs */, v)
}
func makeLeFn(a, b *types.T, v Volatility) *CmpOp {
	return makeCmpOpOverload(cmpOpScalarLEFn, a, b, false /* NullableArgs */, v)
}
func makeIsFn(a, b *types.T, v Volatility) *CmpOp {
	return makeCmpOpOverload(cmpOpScalarIsFn, a, b, true /* NullableArgs */, v)
}

// CmpOps contains the comparison operations indexed by operation type.
var CmpOps = cmpOpFixups(map[ComparisonOperatorSymbol]cmpOpOverload{
	EQ: {
		// Single-type comparisons.
		makeEqFn(types.AnyEnum, types.AnyEnum, VolatilityImmutable),
		makeEqFn(types.Bool, types.Bool, VolatilityLeakProof),
		makeEqFn(types.Bytes, types.Bytes, VolatilityLeakProof),
		makeEqFn(types.Date, types.Date, VolatilityLeakProof),
		makeEqFn(types.Decimal, types.Decimal, VolatilityImmutable),
		// Note: it is an error to compare two strings with different collations;
		// the operator is leak proof under the assumption that these cases will be
		// detected during type checking.
		makeEqFn(types.AnyCollatedString, types.AnyCollatedString, VolatilityLeakProof),
		makeEqFn(types.Float, types.Float, VolatilityLeakProof),
		makeEqFn(types.Box2D, types.Box2D, VolatilityLeakProof),
		makeEqFn(types.Geography, types.Geography, VolatilityLeakProof),
		makeEqFn(types.Geometry, types.Geometry, VolatilityLeakProof),
		makeEqFn(types.INet, types.INet, VolatilityLeakProof),
		makeEqFn(types.Int, types.Int, VolatilityLeakProof),
		makeEqFn(types.Interval, types.Interval, VolatilityLeakProof),
		makeEqFn(types.Jsonb, types.Jsonb, VolatilityImmutable),
		makeEqFn(types.Oid, types.Oid, VolatilityLeakProof),
		makeEqFn(types.String, types.String, VolatilityLeakProof),
		makeEqFn(types.Time, types.Time, VolatilityLeakProof),
		makeEqFn(types.TimeTZ, types.TimeTZ, VolatilityLeakProof),
		makeEqFn(types.Timestamp, types.Timestamp, VolatilityLeakProof),
		makeEqFn(types.TimestampTZ, types.TimestampTZ, VolatilityLeakProof),
		makeEqFn(types.Uuid, types.Uuid, VolatilityLeakProof),
		makeEqFn(types.VarBit, types.VarBit, VolatilityLeakProof),

		// Mixed-type comparisons.
		makeEqFn(types.Date, types.Timestamp, VolatilityImmutable),
		makeEqFn(types.Date, types.TimestampTZ, VolatilityStable),
		makeEqFn(types.Decimal, types.Float, VolatilityLeakProof),
		makeEqFn(types.Decimal, types.Int, VolatilityLeakProof),
		makeEqFn(types.Float, types.Decimal, VolatilityLeakProof),
		makeEqFn(types.Float, types.Int, VolatilityLeakProof),
		makeEqFn(types.Int, types.Decimal, VolatilityLeakProof),
		makeEqFn(types.Int, types.Float, VolatilityLeakProof),
		makeEqFn(types.Int, types.Oid, VolatilityLeakProof),
		makeEqFn(types.Oid, types.Int, VolatilityLeakProof),
		makeEqFn(types.Timestamp, types.Date, VolatilityImmutable),
		makeEqFn(types.Timestamp, types.TimestampTZ, VolatilityStable),
		makeEqFn(types.TimestampTZ, types.Date, VolatilityStable),
		makeEqFn(types.TimestampTZ, types.Timestamp, VolatilityStable),
		makeEqFn(types.Time, types.TimeTZ, VolatilityStable),
		makeEqFn(types.TimeTZ, types.Time, VolatilityStable),

		// Tuple comparison.
		&CmpOp{
			LeftType:  types.AnyTuple,
			RightType: types.AnyTuple,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				return cmpOpTupleFn(ctx, *left.(*DTuple), *right.(*DTuple), MakeComparisonOperator(EQ)), nil
			},
			Volatility: VolatilityImmutable,
		},
	},

	LT: {
		// Single-type comparisons.
		makeLtFn(types.AnyEnum, types.AnyEnum, VolatilityImmutable),
		makeLtFn(types.Bool, types.Bool, VolatilityLeakProof),
		makeLtFn(types.Bytes, types.Bytes, VolatilityLeakProof),
		makeLtFn(types.Date, types.Date, VolatilityLeakProof),
		makeLtFn(types.Decimal, types.Decimal, VolatilityImmutable),
		// Note: it is an error to compare two strings with different collations;
		// the operator is leak proof under the assumption that these cases will be
		// detected during type checking.
		makeLtFn(types.AnyCollatedString, types.AnyCollatedString, VolatilityLeakProof),
		makeLtFn(types.Float, types.Float, VolatilityLeakProof),
		makeLtFn(types.Box2D, types.Box2D, VolatilityLeakProof),
		makeLtFn(types.Geography, types.Geography, VolatilityLeakProof),
		makeLtFn(types.Geometry, types.Geometry, VolatilityLeakProof),
		makeLtFn(types.INet, types.INet, VolatilityLeakProof),
		makeLtFn(types.Int, types.Int, VolatilityLeakProof),
		makeLtFn(types.Interval, types.Interval, VolatilityLeakProof),
		makeLtFn(types.Oid, types.Oid, VolatilityLeakProof),
		makeLtFn(types.String, types.String, VolatilityLeakProof),
		makeLtFn(types.Time, types.Time, VolatilityLeakProof),
		makeLtFn(types.TimeTZ, types.TimeTZ, VolatilityLeakProof),
		makeLtFn(types.Timestamp, types.Timestamp, VolatilityLeakProof),
		makeLtFn(types.TimestampTZ, types.TimestampTZ, VolatilityLeakProof),
		makeLtFn(types.Uuid, types.Uuid, VolatilityLeakProof),
		makeLtFn(types.VarBit, types.VarBit, VolatilityLeakProof),

		// Mixed-type comparisons.
		makeLtFn(types.Date, types.Timestamp, VolatilityImmutable),
		makeLtFn(types.Date, types.TimestampTZ, VolatilityStable),
		makeLtFn(types.Decimal, types.Float, VolatilityLeakProof),
		makeLtFn(types.Decimal, types.Int, VolatilityLeakProof),
		makeLtFn(types.Float, types.Decimal, VolatilityLeakProof),
		makeLtFn(types.Float, types.Int, VolatilityLeakProof),
		makeLtFn(types.Int, types.Decimal, VolatilityLeakProof),
		makeLtFn(types.Int, types.Float, VolatilityLeakProof),
		makeLtFn(types.Int, types.Oid, VolatilityLeakProof),
		makeLtFn(types.Oid, types.Int, VolatilityLeakProof),
		makeLtFn(types.Timestamp, types.Date, VolatilityImmutable),
		makeLtFn(types.Timestamp, types.TimestampTZ, VolatilityStable),
		makeLtFn(types.TimestampTZ, types.Date, VolatilityStable),
		makeLtFn(types.TimestampTZ, types.Timestamp, VolatilityStable),
		makeLtFn(types.Time, types.TimeTZ, VolatilityStable),
		makeLtFn(types.TimeTZ, types.Time, VolatilityStable),

		// Tuple comparison.
		&CmpOp{
			LeftType:  types.AnyTuple,
			RightType: types.AnyTuple,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				return cmpOpTupleFn(ctx, *left.(*DTuple), *right.(*DTuple), MakeComparisonOperator(LT)), nil
			},
			Volatility: VolatilityImmutable,
		},
	},

	LE: {
		// Single-type comparisons.
		makeLeFn(types.AnyEnum, types.AnyEnum, VolatilityImmutable),
		makeLeFn(types.Bool, types.Bool, VolatilityLeakProof),
		makeLeFn(types.Bytes, types.Bytes, VolatilityLeakProof),
		makeLeFn(types.Date, types.Date, VolatilityLeakProof),
		makeLeFn(types.Decimal, types.Decimal, VolatilityImmutable),
		// Note: it is an error to compare two strings with different collations;
		// the operator is leak proof under the assumption that these cases will be
		// detected during type checking.
		makeLeFn(types.AnyCollatedString, types.AnyCollatedString, VolatilityLeakProof),
		makeLeFn(types.Float, types.Float, VolatilityLeakProof),
		makeLeFn(types.Box2D, types.Box2D, VolatilityLeakProof),
		makeLeFn(types.Geography, types.Geography, VolatilityLeakProof),
		makeLeFn(types.Geometry, types.Geometry, VolatilityLeakProof),
		makeLeFn(types.INet, types.INet, VolatilityLeakProof),
		makeLeFn(types.Int, types.Int, VolatilityLeakProof),
		makeLeFn(types.Interval, types.Interval, VolatilityLeakProof),
		makeLeFn(types.Oid, types.Oid, VolatilityLeakProof),
		makeLeFn(types.String, types.String, VolatilityLeakProof),
		makeLeFn(types.Time, types.Time, VolatilityLeakProof),
		makeLeFn(types.TimeTZ, types.TimeTZ, VolatilityLeakProof),
		makeLeFn(types.Timestamp, types.Timestamp, VolatilityLeakProof),
		makeLeFn(types.TimestampTZ, types.TimestampTZ, VolatilityLeakProof),
		makeLeFn(types.Uuid, types.Uuid, VolatilityLeakProof),
		makeLeFn(types.VarBit, types.VarBit, VolatilityLeakProof),

		// Mixed-type comparisons.
		makeLeFn(types.Date, types.Timestamp, VolatilityImmutable),
		makeLeFn(types.Date, types.TimestampTZ, VolatilityStable),
		makeLeFn(types.Decimal, types.Float, VolatilityLeakProof),
		makeLeFn(types.Decimal, types.Int, VolatilityLeakProof),
		makeLeFn(types.Float, types.Decimal, VolatilityLeakProof),
		makeLeFn(types.Float, types.Int, VolatilityLeakProof),
		makeLeFn(types.Int, types.Decimal, VolatilityLeakProof),
		makeLeFn(types.Int, types.Float, VolatilityLeakProof),
		makeLeFn(types.Int, types.Oid, VolatilityLeakProof),
		makeLeFn(types.Oid, types.Int, VolatilityLeakProof),
		makeLeFn(types.Timestamp, types.Date, VolatilityImmutable),
		makeLeFn(types.Timestamp, types.TimestampTZ, VolatilityStable),
		makeLeFn(types.TimestampTZ, types.Date, VolatilityStable),
		makeLeFn(types.TimestampTZ, types.Timestamp, VolatilityStable),
		makeLeFn(types.Time, types.TimeTZ, VolatilityStable),
		makeLeFn(types.TimeTZ, types.Time, VolatilityStable),

		// Tuple comparison.
		&CmpOp{
			LeftType:  types.AnyTuple,
			RightType: types.AnyTuple,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				return cmpOpTupleFn(ctx, *left.(*DTuple), *right.(*DTuple), MakeComparisonOperator(LE)), nil
			},
			Volatility: VolatilityImmutable,
		},
	},

	IsNotDistinctFrom: {
		&CmpOp{
			LeftType:     types.Unknown,
			RightType:    types.Unknown,
			Fn:           cmpOpScalarIsFn,
			NullableArgs: true,
			// Avoids ambiguous comparison error for NULL IS NOT DISTINCT FROM NULL>
			isPreferred: true,
			Volatility:  VolatilityLeakProof,
		},
		// Single-type comparisons.
		makeIsFn(types.AnyEnum, types.AnyEnum, VolatilityImmutable),
		makeIsFn(types.Bool, types.Bool, VolatilityLeakProof),
		makeIsFn(types.Bytes, types.Bytes, VolatilityLeakProof),
		makeIsFn(types.Date, types.Date, VolatilityLeakProof),
		makeIsFn(types.Decimal, types.Decimal, VolatilityImmutable),
		// Note: it is an error to compare two strings with different collations;
		// the operator is leak proof under the assumption that these cases will be
		// detected during type checking.
		makeIsFn(types.AnyCollatedString, types.AnyCollatedString, VolatilityLeakProof),
		makeIsFn(types.Float, types.Float, VolatilityLeakProof),
		makeIsFn(types.Box2D, types.Box2D, VolatilityLeakProof),
		makeIsFn(types.Geography, types.Geography, VolatilityLeakProof),
		makeIsFn(types.Geometry, types.Geometry, VolatilityLeakProof),
		makeIsFn(types.INet, types.INet, VolatilityLeakProof),
		makeIsFn(types.Int, types.Int, VolatilityLeakProof),
		makeIsFn(types.Interval, types.Interval, VolatilityLeakProof),
		makeIsFn(types.Jsonb, types.Jsonb, VolatilityImmutable),
		makeIsFn(types.Oid, types.Oid, VolatilityLeakProof),
		makeIsFn(types.String, types.String, VolatilityLeakProof),
		makeIsFn(types.Time, types.Time, VolatilityLeakProof),
		makeIsFn(types.TimeTZ, types.TimeTZ, VolatilityLeakProof),
		makeIsFn(types.Timestamp, types.Timestamp, VolatilityLeakProof),
		makeIsFn(types.TimestampTZ, types.TimestampTZ, VolatilityLeakProof),
		makeIsFn(types.Uuid, types.Uuid, VolatilityLeakProof),
		makeIsFn(types.VarBit, types.VarBit, VolatilityLeakProof),

		// Mixed-type comparisons.
		makeIsFn(types.Date, types.Timestamp, VolatilityImmutable),
		makeIsFn(types.Date, types.TimestampTZ, VolatilityStable),
		makeIsFn(types.Decimal, types.Float, VolatilityLeakProof),
		makeIsFn(types.Decimal, types.Int, VolatilityLeakProof),
		makeIsFn(types.Float, types.Decimal, VolatilityLeakProof),
		makeIsFn(types.Float, types.Int, VolatilityLeakProof),
		makeIsFn(types.Int, types.Decimal, VolatilityLeakProof),
		makeIsFn(types.Int, types.Float, VolatilityLeakProof),
		makeIsFn(types.Int, types.Oid, VolatilityLeakProof),
		makeIsFn(types.Oid, types.Int, VolatilityLeakProof),
		makeIsFn(types.Timestamp, types.Date, VolatilityImmutable),
		makeIsFn(types.Timestamp, types.TimestampTZ, VolatilityStable),
		makeIsFn(types.TimestampTZ, types.Date, VolatilityStable),
		makeIsFn(types.TimestampTZ, types.Timestamp, VolatilityStable),
		makeIsFn(types.Time, types.TimeTZ, VolatilityStable),
		makeIsFn(types.TimeTZ, types.Time, VolatilityStable),

		// Tuple comparison.
		&CmpOp{
			LeftType:     types.AnyTuple,
			RightType:    types.AnyTuple,
			NullableArgs: true,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				if left == DNull || right == DNull {
					return MakeDBool(left == DNull && right == DNull), nil
				}
				return cmpOpTupleFn(ctx, *left.(*DTuple), *right.(*DTuple), MakeComparisonOperator(IsNotDistinctFrom)), nil
			},
			Volatility: VolatilityImmutable,
		},
	},

	In: {
		makeEvalTupleIn(types.AnyEnum, VolatilityLeakProof),
		makeEvalTupleIn(types.Bool, VolatilityLeakProof),
		makeEvalTupleIn(types.Bytes, VolatilityLeakProof),
		makeEvalTupleIn(types.Date, VolatilityLeakProof),
		makeEvalTupleIn(types.Decimal, VolatilityLeakProof),
		makeEvalTupleIn(types.AnyCollatedString, VolatilityLeakProof),
		makeEvalTupleIn(types.AnyTuple, VolatilityLeakProof),
		makeEvalTupleIn(types.Float, VolatilityLeakProof),
		makeEvalTupleIn(types.Box2D, VolatilityLeakProof),
		makeEvalTupleIn(types.Geography, VolatilityLeakProof),
		makeEvalTupleIn(types.Geometry, VolatilityLeakProof),
		makeEvalTupleIn(types.INet, VolatilityLeakProof),
		makeEvalTupleIn(types.Int, VolatilityLeakProof),
		makeEvalTupleIn(types.Interval, VolatilityLeakProof),
		makeEvalTupleIn(types.Jsonb, VolatilityLeakProof),
		makeEvalTupleIn(types.Oid, VolatilityLeakProof),
		makeEvalTupleIn(types.String, VolatilityLeakProof),
		makeEvalTupleIn(types.Time, VolatilityLeakProof),
		makeEvalTupleIn(types.TimeTZ, VolatilityLeakProof),
		makeEvalTupleIn(types.Timestamp, VolatilityLeakProof),
		makeEvalTupleIn(types.TimestampTZ, VolatilityLeakProof),
		makeEvalTupleIn(types.Uuid, VolatilityLeakProof),
		makeEvalTupleIn(types.VarBit, VolatilityLeakProof),
	},

	Like: {
		&CmpOp{
			LeftType:  types.String,
			RightType: types.String,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				return matchLike(ctx, left, right, false)
			},
			Volatility: VolatilityLeakProof,
		},
	},

	ILike: {
		&CmpOp{
			LeftType:  types.String,
			RightType: types.String,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				return matchLike(ctx, left, right, true)
			},
			Volatility: VolatilityLeakProof,
		},
	},

	SimilarTo: {
		&CmpOp{
			LeftType:  types.String,
			RightType: types.String,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				key := similarToKey{s: string(MustBeDString(right)), escape: '\\'}
				return matchRegexpWithKey(ctx, left, key)
			},
			Volatility: VolatilityLeakProof,
		},
	},

	RegMatch: append(
		cmpOpOverload{
			&CmpOp{
				LeftType:  types.String,
				RightType: types.String,
				Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
					key := regexpKey{s: string(MustBeDString(right)), caseInsensitive: false}
					return matchRegexpWithKey(ctx, left, key)
				},
				Volatility: VolatilityImmutable,
			},
		},
		makeBox2DComparisonOperators(
			func(lhs, rhs *geo.CartesianBoundingBox) bool {
				return lhs.Covers(rhs)
			},
		)...,
	),

	RegIMatch: {
		&CmpOp{
			LeftType:  types.String,
			RightType: types.String,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				key := regexpKey{s: string(MustBeDString(right)), caseInsensitive: true}
				return matchRegexpWithKey(ctx, left, key)
			},
			Volatility: VolatilityImmutable,
		},
	},

	JSONExists: {
		&CmpOp{
			LeftType:  types.Jsonb,
			RightType: types.String,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				e, err := left.(*DJSON).JSON.Exists(string(MustBeDString(right)))
				if err != nil {
					return nil, err
				}
				if e {
					return DBoolTrue, nil
				}
				return DBoolFalse, nil
			},
			Volatility: VolatilityImmutable,
		},
	},

	JSONSomeExists: {
		&CmpOp{
			LeftType:  types.Jsonb,
			RightType: types.StringArray,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				return JSONExistsAny(ctx, MustBeDJSON(left), MustBeDArray(right))
			},
			Volatility: VolatilityImmutable,
		},
	},

	JSONAllExists: {
		&CmpOp{
			LeftType:  types.Jsonb,
			RightType: types.StringArray,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				// TODO(justin): this can be optimized.
				for _, k := range MustBeDArray(right).Array {
					if k == DNull {
						continue
					}
					e, err := left.(*DJSON).JSON.Exists(string(MustBeDString(k)))
					if err != nil {
						return nil, err
					}
					if !e {
						return DBoolFalse, nil
					}
				}
				return DBoolTrue, nil
			},
			Volatility: VolatilityImmutable,
		},
	},

	Contains: {
		&CmpOp{
			LeftType:  types.AnyArray,
			RightType: types.AnyArray,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				haystack := MustBeDArray(left)
				needles := MustBeDArray(right)
				return ArrayContains(ctx, haystack, needles)
			},
			Volatility: VolatilityImmutable,
		},
		&CmpOp{
			LeftType:  types.Jsonb,
			RightType: types.Jsonb,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				c, err := json.Contains(left.(*DJSON).JSON, right.(*DJSON).JSON)
				if err != nil {
					return nil, err
				}
				return MakeDBool(DBool(c)), nil
			},
			Volatility: VolatilityImmutable,
		},
	},

	ContainedBy: {
		&CmpOp{
			LeftType:  types.AnyArray,
			RightType: types.AnyArray,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				needles := MustBeDArray(left)
				haystack := MustBeDArray(right)
				return ArrayContains(ctx, haystack, needles)
			},
			Volatility: VolatilityImmutable,
		},
		&CmpOp{
			LeftType:  types.Jsonb,
			RightType: types.Jsonb,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				c, err := json.Contains(right.(*DJSON).JSON, left.(*DJSON).JSON)
				if err != nil {
					return nil, err
				}
				return MakeDBool(DBool(c)), nil
			},
			Volatility: VolatilityImmutable,
		},
	},
	Overlaps: append(
		cmpOpOverload{
			&CmpOp{
				LeftType:  types.AnyArray,
				RightType: types.AnyArray,
				Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
					array := MustBeDArray(left)
					other := MustBeDArray(right)
					if !array.ParamTyp.Equivalent(other.ParamTyp) {
						return nil, pgerror.New(pgcode.DatatypeMismatch, "cannot compare arrays with different element types")
					}
					for _, needle := range array.Array {
						// Nulls don't compare to each other in && syntax.
						if needle == DNull {
							continue
						}
						for _, hay := range other.Array {
							if needle.Compare(ctx, hay) == 0 {
								return DBoolTrue, nil
							}
						}
					}
					return DBoolFalse, nil
				},
				Volatility: VolatilityImmutable,
			},
			&CmpOp{
				LeftType:  types.INet,
				RightType: types.INet,
				Fn: func(_ *EvalContext, left, right Datum) (Datum, error) {
					ipAddr := MustBeDIPAddr(left).IPAddr
					other := MustBeDIPAddr(right).IPAddr
					return MakeDBool(DBool(ipAddr.ContainsOrContainedBy(&other))), nil
				},
				Volatility: VolatilityImmutable,
			},
		},
		makeBox2DComparisonOperators(
			func(lhs, rhs *geo.CartesianBoundingBox) bool {
				return lhs.Intersects(rhs)
			},
		)...,
	),
})

const experimentalBox2DClusterSettingName = "sql.spatial.experimental_box2d_comparison_operators.enabled"

var experimentalBox2DClusterSetting = settings.RegisterBoolSetting(
	experimentalBox2DClusterSettingName,
	"enables the use of certain experimental box2d comparison operators",
	false,
).WithPublic()

func checkExperimentalBox2DComparisonOperatorEnabled(ctx *EvalContext) error {
	if !experimentalBox2DClusterSetting.Get(&ctx.Settings.SV) {
		return errors.WithHintf(
			pgerror.Newf(
				pgcode.FeatureNotSupported,
				"this box2d comparison operator is experimental",
			),
			"To enable box2d comparators, use `SET CLUSTER SETTING %s = on`.",
			experimentalBox2DClusterSettingName,
		)
	}
	return nil
}

func makeBox2DComparisonOperators(op func(lhs, rhs *geo.CartesianBoundingBox) bool) cmpOpOverload {
	return cmpOpOverload{
		&CmpOp{
			LeftType:  types.Box2D,
			RightType: types.Box2D,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				if err := checkExperimentalBox2DComparisonOperatorEnabled(ctx); err != nil {
					return nil, err
				}
				ret := op(
					&MustBeDBox2D(left).CartesianBoundingBox,
					&MustBeDBox2D(right).CartesianBoundingBox,
				)
				return MakeDBool(DBool(ret)), nil
			},
			Volatility: VolatilityImmutable,
		},
		&CmpOp{
			LeftType:  types.Box2D,
			RightType: types.Geometry,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				if err := checkExperimentalBox2DComparisonOperatorEnabled(ctx); err != nil {
					return nil, err
				}
				ret := op(
					&MustBeDBox2D(left).CartesianBoundingBox,
					MustBeDGeometry(right).CartesianBoundingBox(),
				)
				return MakeDBool(DBool(ret)), nil
			},
			Volatility: VolatilityImmutable,
		},
		&CmpOp{
			LeftType:  types.Geometry,
			RightType: types.Box2D,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				if err := checkExperimentalBox2DComparisonOperatorEnabled(ctx); err != nil {
					return nil, err
				}
				ret := op(
					MustBeDGeometry(left).CartesianBoundingBox(),
					&MustBeDBox2D(right).CartesianBoundingBox,
				)
				return MakeDBool(DBool(ret)), nil
			},
			Volatility: VolatilityImmutable,
		},
		&CmpOp{
			LeftType:  types.Geometry,
			RightType: types.Geometry,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				if err := checkExperimentalBox2DComparisonOperatorEnabled(ctx); err != nil {
					return nil, err
				}
				ret := op(
					MustBeDGeometry(left).CartesianBoundingBox(),
					MustBeDGeometry(right).CartesianBoundingBox(),
				)
				return MakeDBool(DBool(ret)), nil
			},
			Volatility: VolatilityImmutable,
		},
	}
}

// This map contains the inverses for operators in the CmpOps map that have
// inverses.
var cmpOpsInverse map[ComparisonOperatorSymbol]ComparisonOperatorSymbol

func init() {
	cmpOpsInverse = make(map[ComparisonOperatorSymbol]ComparisonOperatorSymbol)
	for cmpOpIdx := range comparisonOpName {
		cmpOp := ComparisonOperatorSymbol(cmpOpIdx)
		newOp, _, _, _, _ := FoldComparisonExpr(MakeComparisonOperator(cmpOp), DNull, DNull)
		if newOp.Symbol != cmpOp {
			cmpOpsInverse[newOp.Symbol] = cmpOp
			cmpOpsInverse[cmpOp] = newOp.Symbol
		}
	}
}

func boolFromCmp(cmp int, op ComparisonOperator) *DBool {
	switch op.Symbol {
	case EQ, IsNotDistinctFrom:
		return MakeDBool(cmp == 0)
	case LT:
		return MakeDBool(cmp < 0)
	case LE:
		return MakeDBool(cmp <= 0)
	default:
		panic(errors.AssertionFailedf("unexpected ComparisonOperator in boolFromCmp: %v", errors.Safe(op)))
	}
}

func cmpOpScalarFn(ctx *EvalContext, left, right Datum, op ComparisonOperator) Datum {
	// Before deferring to the Datum.Compare method, check for values that should
	// be handled differently during SQL comparison evaluation than they should when
	// ordering Datum values.
	if left == DNull || right == DNull {
		switch op.Symbol {
		case IsNotDistinctFrom:
			return MakeDBool((left == DNull) == (right == DNull))

		default:
			// If either Datum is NULL, the result of the comparison is NULL.
			return DNull
		}
	}
	cmp := left.Compare(ctx, right)
	return boolFromCmp(cmp, op)
}

func cmpOpScalarEQFn(ctx *EvalContext, left, right Datum) (Datum, error) {
	return cmpOpScalarFn(ctx, left, right, MakeComparisonOperator(EQ)), nil
}
func cmpOpScalarLTFn(ctx *EvalContext, left, right Datum) (Datum, error) {
	return cmpOpScalarFn(ctx, left, right, MakeComparisonOperator(LT)), nil
}
func cmpOpScalarLEFn(ctx *EvalContext, left, right Datum) (Datum, error) {
	return cmpOpScalarFn(ctx, left, right, MakeComparisonOperator(LE)), nil
}
func cmpOpScalarIsFn(ctx *EvalContext, left, right Datum) (Datum, error) {
	return cmpOpScalarFn(ctx, left, right, MakeComparisonOperator(IsNotDistinctFrom)), nil
}

func cmpOpTupleFn(ctx *EvalContext, left, right DTuple, op ComparisonOperator) Datum {
	cmp := 0
	sawNull := false
	for i, leftElem := range left.D {
		rightElem := right.D[i]
		// Like with cmpOpScalarFn, check for values that need to be handled
		// differently than when ordering Datums.
		if leftElem == DNull || rightElem == DNull {
			switch op.Symbol {
			case EQ:
				// If either Datum is NULL and the op is EQ, we continue the
				// comparison and the result is only NULL if the other (non-NULL)
				// elements are equal. This is because NULL is thought of as "unknown",
				// so a NULL equality comparison does not prevent the equality from
				// being proven false, but does prevent it from being proven true.
				sawNull = true

			case IsNotDistinctFrom:
				// For IS NOT DISTINCT FROM, NULLs are "equal".
				if leftElem != DNull || rightElem != DNull {
					return DBoolFalse
				}

			default:
				// If either Datum is NULL and the op is not EQ or IS NOT DISTINCT FROM,
				// we short-circuit the evaluation and the result of the comparison is
				// NULL. This is because NULL is thought of as "unknown" and tuple
				// inequality is defined lexicographically, so once a NULL comparison is
				// seen, the result of the entire tuple comparison is unknown.
				return DNull
			}
		} else {
			cmp = leftElem.Compare(ctx, rightElem)
			if cmp != 0 {
				break
			}
		}
	}
	b := boolFromCmp(cmp, op)
	if b == DBoolTrue && sawNull {
		// The op is EQ and all non-NULL elements are equal, but we saw at least
		// one NULL element. Since NULL comparisons are treated as unknown, the
		// result of the comparison becomes unknown (NULL).
		return DNull
	}
	return b
}

func makeEvalTupleIn(typ *types.T, v Volatility) *CmpOp {
	return &CmpOp{
		LeftType:  typ,
		RightType: types.AnyTuple,
		Fn: func(ctx *EvalContext, arg, values Datum) (Datum, error) {
			vtuple := values.(*DTuple)
			// If the tuple was sorted during normalization, we can perform an
			// efficient binary search to find if the arg is in the tuple (as
			// long as the arg doesn't contain any NULLs).
			if len(vtuple.D) == 0 {
				// If the rhs tuple is empty, the result is always false (even if arg is
				// or contains NULL).
				return DBoolFalse, nil
			}
			if arg == DNull {
				return DNull, nil
			}
			argTuple, argIsTuple := arg.(*DTuple)
			if vtuple.Sorted() && !(argIsTuple && argTuple.ContainsNull()) {
				// The right-hand tuple is already sorted and contains no NULLs, and the
				// left side is not NULL (e.g. `NULL IN (1, 2)`) or a tuple that
				// contains NULL (e.g. `(1, NULL) IN ((1, 2), (3, 4))`).
				//
				// We can use binary search to make a determination in this case. This
				// is the common case when tuples don't contain NULLs.
				_, result := vtuple.SearchSorted(ctx, arg)
				return MakeDBool(DBool(result)), nil
			}

			sawNull := false
			if !argIsTuple {
				// The left-hand side is not a tuple, e.g. `1 IN (1, 2)`.
				for _, val := range vtuple.D {
					if val == DNull {
						sawNull = true
					} else if val.Compare(ctx, arg) == 0 {
						return DBoolTrue, nil
					}
				}
			} else {
				// The left-hand side is a tuple, e.g. `(1, 2) IN ((1, 2), (3, 4))`.
				for _, val := range vtuple.D {
					if val == DNull {
						// We allow for a null value to be in the list of tuples, so we
						// need to check that upfront.
						sawNull = true
					} else {
						// Use the EQ function which properly handles NULLs.
						if res := cmpOpTupleFn(ctx, *argTuple, *val.(*DTuple), MakeComparisonOperator(EQ)); res == DNull {
							sawNull = true
						} else if res == DBoolTrue {
							return DBoolTrue, nil
						}
					}
				}
			}
			if sawNull {
				return DNull, nil
			}
			return DBoolFalse, nil
		},
		NullableArgs: true,
		Volatility:   v,
	}
}

// evalDatumsCmp evaluates Datums (slice of Datum) using the provided
// sub-operator type (ANY/SOME, ALL) and its CmpOp with the left Datum.
// It returns the result of the ANY/SOME/ALL predicate.
//
// A NULL result is returned if there exists a NULL element and:
//   ANY/SOME: no comparisons evaluate to true
//   ALL: no comparisons evaluate to false
//
// For example, given 1 < ANY (SELECT * FROM generate_series(1,3))
// (right is a DTuple), evalTupleCmp would be called with:
//   evalDatumsCmp(ctx, LT, Any, CmpOp(LT, leftType, rightParamType), leftDatum, rightTuple.D).
// Similarly, given 1 < ANY (ARRAY[1, 2, 3]) (right is a DArray),
// evalArrayCmp would be called with:
//   evalDatumsCmp(ctx, LT, Any, CmpOp(LT, leftType, rightParamType), leftDatum, rightArray.Array).
func evalDatumsCmp(
	ctx *EvalContext, op, subOp ComparisonOperator, fn *CmpOp, left Datum, right Datums,
) (Datum, error) {
	all := op.Symbol == All
	any := !all
	sawNull := false
	for _, elem := range right {
		if elem == DNull {
			sawNull = true
			continue
		}

		_, newLeft, newRight, _, not := FoldComparisonExpr(subOp, left, elem)
		d, err := fn.Fn(ctx, newLeft.(Datum), newRight.(Datum))
		if err != nil {
			return nil, err
		}
		if d == DNull {
			sawNull = true
			continue
		}

		b := d.(*DBool)
		res := *b != DBool(not)
		if any && res {
			return DBoolTrue, nil
		} else if all && !res {
			return DBoolFalse, nil
		}
	}

	if sawNull {
		// If the right-hand array contains any null elements and no [false,true]
		// comparison result is obtained, the result of [ALL,ANY] will be null.
		return DNull, nil
	}

	if all {
		// ALL are true && !sawNull
		return DBoolTrue, nil
	}
	// ANY is false && !sawNull
	return DBoolFalse, nil
}

// MatchLikeEscape matches 'unescaped' with 'pattern' using custom escape character 'escape' which
// must be either empty (which disables the escape mechanism) or a single unicode character.
func MatchLikeEscape(
	ctx *EvalContext, unescaped, pattern, escape string, caseInsensitive bool,
) (Datum, error) {
	var escapeRune rune
	if len(escape) > 0 {
		var width int
		escapeRune, width = utf8.DecodeRuneInString(escape)
		if len(escape) > width {
			return DBoolFalse, pgerror.Newf(pgcode.InvalidEscapeSequence, "invalid escape string")
		}
	}

	if len(unescaped) == 0 {
		// An empty string only matches with an empty pattern or a pattern
		// consisting only of '%' (if this wildcard is not used as a custom escape
		// character). To match PostgreSQL's behavior, we have a special handling
		// of this case.
		for _, c := range pattern {
			if c != '%' || (c == '%' && escape == `%`) {
				return DBoolFalse, nil
			}
		}
		return DBoolTrue, nil
	}

	like, err := optimizedLikeFunc(pattern, caseInsensitive, escapeRune)
	if err != nil {
		return DBoolFalse, pgerror.Newf(
			pgcode.InvalidRegularExpression, "LIKE regexp compilation failed: %v", err)
	}

	if like == nil {
		re, err := ConvertLikeToRegexp(ctx, pattern, caseInsensitive, escapeRune)
		if err != nil {
			return DBoolFalse, err
		}
		like = func(s string) (bool, error) {
			return re.MatchString(s), nil
		}
	}
	matches, err := like(unescaped)
	return MakeDBool(DBool(matches)), err
}

// ConvertLikeToRegexp compiles the specified LIKE pattern as an equivalent
// regular expression.
func ConvertLikeToRegexp(
	ctx *EvalContext, pattern string, caseInsensitive bool, escape rune,
) (*regexp.Regexp, error) {
	key := likeKey{s: pattern, caseInsensitive: caseInsensitive, escape: escape}
	re, err := ctx.ReCache.GetRegexp(key)
	if err != nil {
		return nil, pgerror.Newf(
			pgcode.InvalidRegularExpression, "LIKE regexp compilation failed: %v", err)
	}
	return re, nil
}

func matchLike(ctx *EvalContext, left, right Datum, caseInsensitive bool) (Datum, error) {
	if left == DNull || right == DNull {
		return DNull, nil
	}
	s, pattern := string(MustBeDString(left)), string(MustBeDString(right))
	if len(s) == 0 {
		// An empty string only matches with an empty pattern or a pattern
		// consisting only of '%'. To match PostgreSQL's behavior, we have a
		// special handling of this case.
		for _, c := range pattern {
			if c != '%' {
				return DBoolFalse, nil
			}
		}
		return DBoolTrue, nil
	}

	like, err := optimizedLikeFunc(pattern, caseInsensitive, '\\')
	if err != nil {
		return DBoolFalse, pgerror.Newf(
			pgcode.InvalidRegularExpression, "LIKE regexp compilation failed: %v", err)
	}

	if like == nil {
		re, err := ConvertLikeToRegexp(ctx, pattern, caseInsensitive, '\\')
		if err != nil {
			return DBoolFalse, err
		}
		like = func(s string) (bool, error) {
			return re.MatchString(s), nil
		}
	}
	matches, err := like(s)
	return MakeDBool(DBool(matches)), err
}

func matchRegexpWithKey(ctx *EvalContext, str Datum, key RegexpCacheKey) (Datum, error) {
	re, err := ctx.ReCache.GetRegexp(key)
	if err != nil {
		return DBoolFalse, err
	}
	return MakeDBool(DBool(re.MatchString(string(MustBeDString(str))))), nil
}

// MultipleResultsError is returned by QueryRow when more than one result is
// encountered.
type MultipleResultsError struct {
	SQL string // the query that produced this error
}

func (e *MultipleResultsError) Error() string {
	return fmt.Sprintf("%s: unexpected multiple results", e.SQL)
}

// DatabaseRegionConfig is a wrapper around multiregion.RegionConfig
// related methods which avoids a circular dependency between descpb and tree.
type DatabaseRegionConfig interface {
	IsValidRegionNameString(r string) bool
	PrimaryRegionString() string
}

// EvalDatabase consists of functions that reference the session database
// and is to be used from EvalContext.
type EvalDatabase interface {
	// CurrentDatabaseRegionConfig returns the RegionConfig of the current
	// session database.
	CurrentDatabaseRegionConfig(ctx context.Context) (DatabaseRegionConfig, error)

	// ValidateAllMultiRegionZoneConfigsInCurrentDatabase validates whether the current
	// database's multi-region zone configs are correctly setup. This includes
	// all tables within the database.
	ValidateAllMultiRegionZoneConfigsInCurrentDatabase(ctx context.Context) error

	// ParseQualifiedTableName parses a SQL string of the form
	// `[ database_name . ] [ schema_name . ] table_name`.
	// NB: this is deprecated! Use parser.ParseQualifiedTableName when possible.
	ParseQualifiedTableName(sql string) (*TableName, error)

	// ResolveTableName expands the given table name and
	// makes it point to a valid object.
	// If the database name is not given, it uses the search path to find it, and
	// sets it on the returned TableName.
	// It returns the ID of the resolved table, and an error if the table doesn't exist.
	ResolveTableName(ctx context.Context, tn *TableName) (ID, error)

	// SchemaExists looks up the schema with the given name and determines
	// whether it exists.
	SchemaExists(ctx context.Context, dbName, scName string) (found bool, err error)

	// IsTableVisible checks if the table with the given ID belongs to a schema
	// on the given sessiondata.SearchPath.
	IsTableVisible(
		ctx context.Context, curDB string, searchPath sessiondata.SearchPath, tableID oid.Oid,
	) (isVisible bool, exists bool, err error)

	// IsTypeVisible checks if the type with the given ID belongs to a schema
	// on the given sessiondata.SearchPath.
	IsTypeVisible(
		ctx context.Context, curDB string, searchPath sessiondata.SearchPath, typeID oid.Oid,
	) (isVisible bool, exists bool, err error)

	// HasPrivilege returns whether the current user has privilege to access
	// the given object.
	HasPrivilege(
		ctx context.Context,
		specifier HasPrivilegeSpecifier,
		user security.SQLUsername,
		kind privilege.Kind,
		withGrantOpt bool,
	) (bool, error)
}

// HasPrivilegeSpecifier specifies an object to lookup privilege for.
type HasPrivilegeSpecifier struct {
	// Only one of these is filled.
	TableName *string
	TableOID  *oid.Oid

	// Only one of these is filled.
	// Only used if TableName or TableOID is specified.
	ColumnName   *Name
	ColumnAttNum *uint32
}

// TypeResolver is an interface for resolving types and type OIDs.
type TypeResolver interface {
	TypeReferenceResolver

	// ResolveOIDFromString looks up the populated value of the OID with the
	// desired resultType which matches the provided name.
	//
	// The return value is a fresh DOid of the input oid.Oid with name and OID
	// set to the result of the query. If there was not exactly one result to the
	// query, an error will be returned.
	ResolveOIDFromString(
		ctx context.Context, resultType *types.T, toResolve *DString,
	) (*DOid, error)

	// ResolveOIDFromOID looks up the populated value of the oid with the
	// desired resultType which matches the provided oid.
	//
	// The return value is a fresh DOid of the input oid.Oid with name and OID
	// set to the result of the query. If there was not exactly one result to the
	// query, an error will be returned.
	ResolveOIDFromOID(
		ctx context.Context, resultType *types.T, toResolve *DOid,
	) (*DOid, error)
}

// EvalPlanner is a limited planner that can be used from EvalContext.
type EvalPlanner interface {
	EvalDatabase
	TypeResolver

	// GetImmutableTableInterfaceByID returns an interface{} with
	// catalog.TableDescriptor to avoid a circular dependency.
	GetImmutableTableInterfaceByID(ctx context.Context, id int) (interface{}, error)

	// GetTypeFromValidSQLSyntax parses a column type when the input
	// string uses the parseable SQL representation of a type name, e.g.
	// `INT(13)`, `mytype`, `"mytype"`, `pg_catalog.int4` or `"public".mytype`.
	GetTypeFromValidSQLSyntax(sql string) (*types.T, error)

	// EvalSubquery returns the Datum for the given subquery node.
	EvalSubquery(expr *Subquery) (Datum, error)

	// UnsafeUpsertDescriptor is used to repair descriptors in dire
	// circumstances. See the comment on the planner implementation.
	UnsafeUpsertDescriptor(
		ctx context.Context, descID int64, encodedDescriptor []byte, force bool,
	) error

	// UnsafeDeleteDescriptor is used to repair descriptors in dire
	// circumstances. See the comment on the planner implementation.
	UnsafeDeleteDescriptor(ctx context.Context, descID int64, force bool) error

	// ForceDeleteTableData cleans up underlying data for a table
	// descriptor ID. See the comment on the planner implementation.
	ForceDeleteTableData(ctx context.Context, descID int64) error

	// UnsafeUpsertNamespaceEntry is used to repair namespace entries in dire
	// circumstances. See the comment on the planner implementation.
	UnsafeUpsertNamespaceEntry(
		ctx context.Context,
		parentID, parentSchemaID int64,
		name string,
		descID int64,
		force bool,
	) error

	// UnsafeDeleteNamespaceEntry is used to repair namespace entries in dire
	// circumstances. See the comment on the planner implementation.
	UnsafeDeleteNamespaceEntry(
		ctx context.Context,
		parentID, parentSchemaID int64,
		name string,
		descID int64,
		force bool,
	) error

	// MemberOfWithAdminOption is used to collect a list of roles (direct and
	// indirect) that the member is part of. See the comment on the planner
	// implementation in authorization.go
	MemberOfWithAdminOption(
		ctx context.Context,
		member security.SQLUsername,
	) (map[security.SQLUsername]bool, error)
}

// CompactEngineSpanFunc is used to compact an engine key span at the given
// (nodeID, storeID). If we add more overloads to the compact_span builtin,
// this parameter list should be changed to a struct union to accommodate
// those overloads.
type CompactEngineSpanFunc func(
	ctx context.Context, nodeID, storeID int32, startKey, endKey []byte,
) error

// EvalSessionAccessor is a limited interface to access session variables.
type EvalSessionAccessor interface {
	// SetConfig sets a session variable to a new value.
	//
	// This interface only supports strings as this is sufficient for
	// pg_catalog.set_config().
	SetSessionVar(ctx context.Context, settingName, newValue string) error

	// GetSessionVar retrieves the current value of a session variable.
	GetSessionVar(ctx context.Context, settingName string, missingOk bool) (bool, string, error)

	// HasAdminRole returns true iff the current session user has the admin role.
	HasAdminRole(ctx context.Context) (bool, error)

	// HasAdminRole returns nil iff the current session user has the specified
	// role option.
	HasRoleOption(ctx context.Context, roleOption roleoption.Option) (bool, error)
}

// ClientNoticeSender is a limited interface to send notices to the
// client.
//
// TODO(knz): as of this writing, the implementations of this
// interface only work on the gateway node (i.e. not from
// distributed processors).
type ClientNoticeSender interface {
	// BufferClientNotice buffers the notice to send to the client.
	// This is flushed before the connection is closed.
	BufferClientNotice(ctx context.Context, notice pgnotice.Notice)
}

// InternalExecutor is a subset of sqlutil.InternalExecutor (which, in turn, is
// implemented by sql.InternalExecutor) used by this sem/tree package which
// can't even import sqlutil.
//
// Note that the functions offered here should be avoided when possible. They
// execute the query as root if an user hadn't been previously set on the
// executor through SetSessionData(). These functions are deprecated in
// sql.InternalExecutor in favor of a safer interface. Unfortunately, those
// safer functions cannot be exposed through this interface because they depend
// on sqlbase, and this package cannot import sqlbase. When possible, downcast
// this to sqlutil.InternalExecutor or sql.InternalExecutor, and use the
// alternatives.
type InternalExecutor interface {
	// QueryRow is part of the sqlutil.InternalExecutor interface.
	QueryRow(
		ctx context.Context, opName string, txn *kv.Txn, stmt string, qargs ...interface{},
	) (Datums, error)
}

// PrivilegedAccessor gives access to certain queries that would otherwise
// require someone with RootUser access to query a given data source.
// It is defined independently to prevent a circular dependency on sql, tree and sqlbase.
type PrivilegedAccessor interface {
	// LookupNamespaceID returns the id of the namespace given it's parent id and name.
	// It is meant as a replacement for looking up the system.namespace directly.
	// Returns the id, a bool representing whether the namespace exists, and an error
	// if there is one.
	LookupNamespaceID(
		ctx context.Context, parentID int64, name string,
	) (DInt, bool, error)

	// LookupZoneConfig returns the zone config given a namespace id.
	// It is meant as a replacement for looking up system.zones directly.
	// Returns the config byte array, a bool representing whether the namespace exists,
	// and an error if there is one.
	LookupZoneConfigByNamespaceID(ctx context.Context, id int64) (DBytes, bool, error)
}

// SequenceOperators is used for various sql related functions that can
// be used from EvalContext.
type SequenceOperators interface {
	EvalDatabase

	// GetSerialSequenceNameFromColumn returns the sequence name for a given table and column
	// provided it is part of a SERIAL sequence.
	// Returns an empty string if the sequence name does not exist.
	GetSerialSequenceNameFromColumn(ctx context.Context, tableName *TableName, columnName Name) (*TableName, error)

	// IncrementSequence increments the given sequence and returns the result.
	// It returns an error if the given name is not a sequence.
	// The caller must ensure that seqName is fully qualified already.
	IncrementSequence(ctx context.Context, seqName *TableName) (int64, error)

	// GetLatestValueInSessionForSequence returns the value most recently obtained by
	// nextval() for the given sequence in this session.
	GetLatestValueInSessionForSequence(ctx context.Context, seqName *TableName) (int64, error)

	// SetSequenceValue sets the sequence's value.
	// If isCalled is false, the sequence is set such that the next time nextval() is called,
	// `newVal` is returned. Otherwise, the next call to nextval will return
	// `newVal + seqOpts.Increment`.
	SetSequenceValue(ctx context.Context, seqName *TableName, newVal int64, isCalled bool) error

	// IncrementSequenceByID increments the given sequence and returns the result.
	// It returns an error if the given ID is not a sequence.
	// Takes in a sequence ID rather than a name, unlike IncrementSequence.
	IncrementSequenceByID(ctx context.Context, seqID int64) (int64, error)

	// GetLatestValueInSessionForSequenceByID returns the value most recently obtained by
	// nextval() for the given sequence in this session.
	// Takes in a sequence ID rather than a name, unlike GetLatestValueInSessionForSequence.
	GetLatestValueInSessionForSequenceByID(ctx context.Context, seqID int64) (int64, error)

	// SetSequenceValueByID sets the sequence's value.
	// If isCalled is false, the sequence is set such that the next time nextval() is called,
	// `newVal` is returned. Otherwise, the next call to nextval will return
	// `newVal + seqOpts.Increment`.
	// Takes in a sequence ID rather than a name, unlike SetSequenceValue.
	SetSequenceValueByID(ctx context.Context, seqID int64, newVal int64, isCalled bool) error
}

// TenantOperator is capable of interacting with tenant state, allowing SQL
// builtin functions to create and destroy tenants. The methods will return
// errors when run by any tenant other than the system tenant.
type TenantOperator interface {
	// CreateTenant attempts to install a new tenant in the system. It returns
	// an error if the tenant already exists. The new tenant is created at the
	// current active version of the cluster performing the create.
	CreateTenant(ctx context.Context, tenantID uint64) error

	// DestroyTenant attempts to uninstall an existing tenant from the system.
	// It returns an error if the tenant does not exist.
	DestroyTenant(ctx context.Context, tenantID uint64) error

	// GCTenant attempts to garbage collect a DROP tenant from the system. Upon
	// success it also removes the tenant record.
	// It returns an error if the tenant does not exist.
	GCTenant(ctx context.Context, tenantID uint64) error
}

// JoinTokenCreator is capable of creating and persisting join tokens, allowing
// SQL builtin functions to create join tokens. The methods will return errors
// when run on multi-tenant clusters or with this functionality unavailable.
type JoinTokenCreator interface {
	// CreateJoinToken creates a new ephemeral join token and persists it
	// across the cluster. This join token can then be used to have new nodes
	// join the cluster and exchange certificates securely.
	CreateJoinToken(ctx context.Context) (string, error)
}

// EvalContextTestingKnobs contains test knobs.
type EvalContextTestingKnobs struct {
	// AssertFuncExprReturnTypes indicates whether FuncExpr evaluations
	// should assert that the returned Datum matches the expected
	// ReturnType of the function.
	AssertFuncExprReturnTypes bool
	// AssertUnaryExprReturnTypes indicates whether UnaryExpr evaluations
	// should assert that the returned Datum matches the expected
	// ReturnType of the function.
	AssertUnaryExprReturnTypes bool
	// AssertBinaryExprReturnTypes indicates whether BinaryExpr
	// evaluations should assert that the returned Datum matches the
	// expected ReturnType of the function.
	AssertBinaryExprReturnTypes bool
	// DisableOptimizerRuleProbability is the probability that any given
	// transformation rule in the optimizer is disabled.
	DisableOptimizerRuleProbability float64
	// OptimizerCostPerturbation is used to randomly perturb the estimated
	// cost of each expression in the query tree for the purpose of creating
	// alternate query plans in the optimizer.
	OptimizerCostPerturbation float64
	// If set, mutations.MaxBatchSize and row.getKVBatchSize will be overridden
	// to use the non-test value.
	ForceProductionBatchSizes bool

	CallbackGenerators map[string]*CallbackValueGenerator
}

var _ base.ModuleTestingKnobs = &EvalContextTestingKnobs{}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*EvalContextTestingKnobs) ModuleTestingKnobs() {}

// SQLStatsResetter is an interface embedded in EvalCtx which can be used by
// the builtins to reset SQL stats in the cluster. This interface is introduced
// to avoid circular dependency.
type SQLStatsResetter interface {
	ResetClusterSQLStats(ctx context.Context) error
}

// EvalContext defines the context in which to evaluate an expression, allowing
// the retrieval of state such as the node ID or statement start time.
//
// ATTENTION: Some fields from this struct (particularly, but not exclusively,
// from SessionData) are also represented in execinfrapb.EvalContext. Whenever
// something that affects DistSQL execution is added, it needs to be marshaled
// through that proto too.
// TODO(andrei): remove or limit the duplication.
//
// NOTE(andrei): EvalContext is dusty; it started as a collection of fields
// needed by expression evaluation, but it has grown quite large; some of the
// things in it don't seem to belong in this low-level package (e.g. Planner).
// In the sql package it is embedded by extendedEvalContext, which adds some
// more fields from the sql package. Through that extendedEvalContext, this
// struct now generally used by planNodes.
type EvalContext struct {
	// Session variables. This is a read-only copy of the values owned by the
	// Session.
	SessionData *sessiondata.SessionData
	// TxnState is a string representation of the current transactional state.
	TxnState string
	// TxnReadOnly specifies if the current transaction is read-only.
	TxnReadOnly bool
	TxnImplicit bool

	Settings    *cluster.Settings
	ClusterID   uuid.UUID
	ClusterName string
	NodeID      *base.SQLIDContainer
	Codec       keys.SQLCodec

	// Locality contains the location of the current node as a set of user-defined
	// key/value pairs, ordered from most inclusive to least inclusive. If there
	// are no tiers, then the node's location is not known. Example:
	//
	//   [region=us,dc=east]
	//
	Locality roachpb.Locality

	// The statement timestamp. May be different for every statement.
	// Used for statement_timestamp().
	StmtTimestamp time.Time
	// The transaction timestamp. Needs to stay stable for the lifetime
	// of a transaction. Used for now(), current_timestamp(),
	// transaction_timestamp() and the like.
	TxnTimestamp time.Time

	// Placeholders relates placeholder names to their type and, later, value.
	// This pointer should always be set to the location of the PlaceholderInfo
	// in the corresponding SemaContext during normal execution. Placeholders are
	// available during Eval to permit lookup of a particular placeholder's
	// underlying datum, if available.
	Placeholders *PlaceholderInfo

	// Annotations augments the AST with extra information. This pointer should
	// always be set to the location of the Annotations in the corresponding
	// SemaContext.
	Annotations *Annotations

	// IVarContainer is used to evaluate IndexedVars.
	IVarContainer IndexedVarContainer
	// iVarContainerStack is used when we swap out IVarContainers in order to
	// evaluate an intermediate expression. This keeps track of those which we
	// need to restore once we finish evaluating it.
	iVarContainerStack []IndexedVarContainer

	// Context holds the context in which the expression is evaluated.
	Context context.Context

	// InternalExecutor gives access to an executor to be used for running
	// "internal" statements. It may seem bizarre that "expression evaluation" may
	// need to run a statement, and yet many builtin functions do it.
	// Note that the executor will be "session-bound" - it will inherit session
	// variables from a parent session.
	InternalExecutor InternalExecutor

	Planner EvalPlanner

	PrivilegedAccessor PrivilegedAccessor

	SessionAccessor EvalSessionAccessor

	ClientNoticeSender ClientNoticeSender

	Sequence SequenceOperators

	Tenant TenantOperator

	JoinTokenCreator JoinTokenCreator

	// The transaction in which the statement is executing.
	Txn *kv.Txn
	// A handle to the database.
	DB *kv.DB

	ReCache *RegexpCache
	tmpDec  apd.Decimal

	// TODO(mjibson): remove prepareOnly in favor of a 2-step prepare-exec solution
	// that is also able to save the plan to skip work during the exec step.
	PrepareOnly bool

	// SkipNormalize indicates whether expressions should be normalized
	// (false) or not (true).  It is set to true conditionally by
	// EXPLAIN(TYPES[, NORMALIZE]).
	SkipNormalize bool

	CollationEnv CollationEnvironment

	TestingKnobs EvalContextTestingKnobs

	Mon *mon.BytesMonitor

	// SingleDatumAggMemAccount is a memory account that all aggregate builtins
	// that store a single datum will share to account for the memory needed to
	// perform the aggregation (i.e. memory not reported by AggregateFunc.Size
	// method). This memory account exists so that such aggregate functions
	// could "batch" their reservations - otherwise, we end up a situation
	// where each aggregate function struct grows its own memory account by
	// tiny amount, yet the account reserves a lot more resulting in
	// significantly overestimating the memory usage.
	SingleDatumAggMemAccount *mon.BoundAccount

	SQLLivenessReader sqlliveness.Reader

	SQLStatsResetter SQLStatsResetter

	// CompactEngineSpan is used to force compaction of a span in a store.
	CompactEngineSpan CompactEngineSpanFunc
}

// MakeTestingEvalContext returns an EvalContext that includes a MemoryMonitor.
func MakeTestingEvalContext(st *cluster.Settings) EvalContext {
	monitor := mon.NewMonitor(
		"test-monitor",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	return MakeTestingEvalContextWithMon(st, monitor)
}

// MakeTestingEvalContextWithMon returns an EvalContext with the given
// MemoryMonitor. Ownership of the memory monitor is transferred to the
// EvalContext so do not start or close the memory monitor.
func MakeTestingEvalContextWithMon(st *cluster.Settings, monitor *mon.BytesMonitor) EvalContext {
	ctx := EvalContext{
		Codec:       keys.SystemSQLCodec,
		Txn:         &kv.Txn{},
		SessionData: &sessiondata.SessionData{},
		Settings:    st,
		NodeID:      base.TestingIDContainer,
	}
	monitor.Start(context.Background(), nil /* pool */, mon.MakeStandaloneBudget(math.MaxInt64))
	ctx.Mon = monitor
	ctx.Context = context.TODO()
	now := timeutil.Now()
	ctx.SetTxnTimestamp(now)
	ctx.SetStmtTimestamp(now)
	return ctx
}

// Copy returns a deep copy of ctx.
func (ctx *EvalContext) Copy() *EvalContext {
	ctxCopy := *ctx
	ctxCopy.iVarContainerStack = make([]IndexedVarContainer, len(ctx.iVarContainerStack), cap(ctx.iVarContainerStack))
	copy(ctxCopy.iVarContainerStack, ctx.iVarContainerStack)
	return &ctxCopy
}

// PushIVarContainer replaces the current IVarContainer with a different one -
// pushing the current one onto a stack to be replaced later once
// PopIVarContainer is called.
func (ctx *EvalContext) PushIVarContainer(c IndexedVarContainer) {
	ctx.iVarContainerStack = append(ctx.iVarContainerStack, ctx.IVarContainer)
	ctx.IVarContainer = c
}

// PopIVarContainer discards the current IVarContainer on the EvalContext,
// replacing it with an older one.
func (ctx *EvalContext) PopIVarContainer() {
	ctx.IVarContainer = ctx.iVarContainerStack[len(ctx.iVarContainerStack)-1]
	ctx.iVarContainerStack = ctx.iVarContainerStack[:len(ctx.iVarContainerStack)-1]
}

// NewTestingEvalContext is a convenience version of MakeTestingEvalContext
// that returns a pointer.
func NewTestingEvalContext(st *cluster.Settings) *EvalContext {
	ctx := MakeTestingEvalContext(st)
	return &ctx
}

// Stop closes out the EvalContext and must be called once it is no longer in use.
func (ctx *EvalContext) Stop(c context.Context) {
	ctx.Mon.Stop(c)
}

// FmtCtx creates a FmtCtx with the given options as well as the EvalContext's session data.
func (ctx *EvalContext) FmtCtx(f FmtFlags, opts ...FmtCtxOption) *FmtCtx {
	if ctx.SessionData != nil {
		opts = append(
			[]FmtCtxOption{FmtDataConversionConfig(ctx.SessionData.DataConversionConfig)},
			opts...,
		)
	}
	return NewFmtCtx(
		f,
		opts...,
	)
}

// GetStmtTimestamp retrieves the current statement timestamp as per
// the evaluation context. The timestamp is guaranteed to be nonzero.
func (ctx *EvalContext) GetStmtTimestamp() time.Time {
	// TODO(knz): a zero timestamp should never be read, even during
	// Prepare. This will need to be addressed.
	if !ctx.PrepareOnly && ctx.StmtTimestamp.IsZero() {
		panic(errors.AssertionFailedf("zero statement timestamp in EvalContext"))
	}
	return ctx.StmtTimestamp
}

// GetClusterTimestamp retrieves the current cluster timestamp as per
// the evaluation context. The timestamp is guaranteed to be nonzero.
func (ctx *EvalContext) GetClusterTimestamp() *DDecimal {
	ts := ctx.Txn.CommitTimestamp()
	if ts.IsEmpty() {
		panic(errors.AssertionFailedf("zero cluster timestamp in txn"))
	}
	return TimestampToDecimalDatum(ts)
}

// HasPlaceholders returns true if this EvalContext's placeholders have been
// assigned. Will be false during Prepare.
func (ctx *EvalContext) HasPlaceholders() bool {
	return ctx.Placeholders != nil
}

// TimestampToDecimal converts the logical timestamp into a decimal
// value with the number of nanoseconds in the integer part and the
// logical counter in the decimal part.
func TimestampToDecimal(ts hlc.Timestamp) apd.Decimal {
	// Compute Walltime * 10^10 + Logical.
	// We need 10 decimals for the Logical field because its maximum
	// value is 4294967295 (2^32-1), a value with 10 decimal digits.
	var res apd.Decimal
	val := &res.Coeff
	val.SetInt64(ts.WallTime)
	val.Mul(val, big10E10)
	val.Add(val, big.NewInt(int64(ts.Logical)))

	// val must be positive. If it was set to a negative value above,
	// transfer the sign to res.Negative.
	res.Negative = val.Sign() < 0
	val.Abs(val)

	// Shift 10 decimals to the right, so that the logical
	// field appears as fractional part.
	res.Exponent = -10
	return res
}

// DecimalToInexactDTimestamp is the inverse of TimestampToDecimal. It converts
// a decimal constructed from an hlc.Timestamp into an approximate DTimestamp
// containing the walltime of the hlc.Timestamp.
func DecimalToInexactDTimestamp(d *DDecimal) (*DTimestamp, error) {
	var coef big.Int
	coef.Set(&d.Decimal.Coeff)
	// The physical portion of the HLC is stored shifted up by 10^10, so shift
	// it down and clear out the logical component.
	coef.Div(&coef, big10E10)
	if !coef.IsInt64() {
		return nil, pgerror.Newf(pgcode.DatetimeFieldOverflow, "timestamp value out of range: %s", d.String())
	}
	return TimestampToInexactDTimestamp(hlc.Timestamp{WallTime: coef.Int64()}), nil
}

// TimestampToDecimalDatum is the same as TimestampToDecimal, but
// returns a datum.
func TimestampToDecimalDatum(ts hlc.Timestamp) *DDecimal {
	res := TimestampToDecimal(ts)
	return &DDecimal{
		Decimal: res,
	}
}

// TimestampToInexactDTimestamp converts the logical timestamp into an
// inexact DTimestamp by dropping the logical counter and using the wall
// time at the microsecond precision.
func TimestampToInexactDTimestamp(ts hlc.Timestamp) *DTimestamp {
	return MustMakeDTimestamp(timeutil.Unix(0, ts.WallTime), time.Microsecond)
}

// GetRelativeParseTime implements ParseTimeContext.
func (ctx *EvalContext) GetRelativeParseTime() time.Time {
	ret := ctx.TxnTimestamp
	if ret.IsZero() {
		ret = timeutil.Now()
	}
	return ret.In(ctx.GetLocation())
}

// GetTxnTimestamp retrieves the current transaction timestamp as per
// the evaluation context. The timestamp is guaranteed to be nonzero.
func (ctx *EvalContext) GetTxnTimestamp(precision time.Duration) *DTimestampTZ {
	// TODO(knz): a zero timestamp should never be read, even during
	// Prepare. This will need to be addressed.
	if !ctx.PrepareOnly && ctx.TxnTimestamp.IsZero() {
		panic(errors.AssertionFailedf("zero transaction timestamp in EvalContext"))
	}
	return MustMakeDTimestampTZ(ctx.GetRelativeParseTime(), precision)
}

// GetTxnTimestampNoZone retrieves the current transaction timestamp as per
// the evaluation context. The timestamp is guaranteed to be nonzero.
func (ctx *EvalContext) GetTxnTimestampNoZone(precision time.Duration) *DTimestamp {
	// TODO(knz): a zero timestamp should never be read, even during
	// Prepare. This will need to be addressed.
	if !ctx.PrepareOnly && ctx.TxnTimestamp.IsZero() {
		panic(errors.AssertionFailedf("zero transaction timestamp in EvalContext"))
	}
	// Move the time to UTC, but keeping the location's time.
	t := ctx.GetRelativeParseTime()
	_, offsetSecs := t.Zone()
	return MustMakeDTimestamp(t.Add(time.Second*time.Duration(offsetSecs)).In(time.UTC), precision)
}

// GetTxnTime retrieves the current transaction time as per
// the evaluation context.
func (ctx *EvalContext) GetTxnTime(precision time.Duration) *DTimeTZ {
	// TODO(knz): a zero timestamp should never be read, even during
	// Prepare. This will need to be addressed.
	if !ctx.PrepareOnly && ctx.TxnTimestamp.IsZero() {
		panic(errors.AssertionFailedf("zero transaction timestamp in EvalContext"))
	}
	return NewDTimeTZFromTime(ctx.GetRelativeParseTime().Round(precision))
}

// GetTxnTimeNoZone retrieves the current transaction time as per
// the evaluation context.
func (ctx *EvalContext) GetTxnTimeNoZone(precision time.Duration) *DTime {
	// TODO(knz): a zero timestamp should never be read, even during
	// Prepare. This will need to be addressed.
	if !ctx.PrepareOnly && ctx.TxnTimestamp.IsZero() {
		panic(errors.AssertionFailedf("zero transaction timestamp in EvalContext"))
	}
	return MakeDTime(timeofday.FromTime(ctx.GetRelativeParseTime().Round(precision)))
}

// SetTxnTimestamp sets the corresponding timestamp in the EvalContext.
func (ctx *EvalContext) SetTxnTimestamp(ts time.Time) {
	ctx.TxnTimestamp = ts
}

// SetStmtTimestamp sets the corresponding timestamp in the EvalContext.
func (ctx *EvalContext) SetStmtTimestamp(ts time.Time) {
	ctx.StmtTimestamp = ts
}

// GetLocation returns the session timezone.
func (ctx *EvalContext) GetLocation() *time.Location {
	return ctx.SessionData.GetLocation()
}

// Ctx returns the session's context.
func (ctx *EvalContext) Ctx() context.Context {
	return ctx.Context
}

func (ctx *EvalContext) getTmpDec() *apd.Decimal {
	return &ctx.tmpDec
}

// Eval implements the TypedExpr interface.
func (expr *AndExpr) Eval(ctx *EvalContext) (Datum, error) {
	left, err := expr.Left.(TypedExpr).Eval(ctx)
	if err != nil {
		return nil, err
	}
	if left != DNull {
		if v, err := GetBool(left); err != nil {
			return nil, err
		} else if !v {
			return left, nil
		}
	}
	right, err := expr.Right.(TypedExpr).Eval(ctx)
	if err != nil {
		return nil, err
	}
	if right == DNull {
		return DNull, nil
	}
	if v, err := GetBool(right); err != nil {
		return nil, err
	} else if !v {
		return right, nil
	}
	return left, nil
}

// Eval implements the TypedExpr interface.
func (expr *BinaryExpr) Eval(ctx *EvalContext) (Datum, error) {
	left, err := expr.Left.(TypedExpr).Eval(ctx)
	if err != nil {
		return nil, err
	}
	if left == DNull && !expr.Fn.NullableArgs {
		return DNull, nil
	}
	right, err := expr.Right.(TypedExpr).Eval(ctx)
	if err != nil {
		return nil, err
	}
	if right == DNull && !expr.Fn.NullableArgs {
		return DNull, nil
	}
	res, err := expr.Fn.Fn(ctx, left, right)
	if err != nil {
		return nil, err
	}
	if ctx.TestingKnobs.AssertBinaryExprReturnTypes {
		if err := ensureExpectedType(expr.Fn.ReturnType, res); err != nil {
			return nil, errors.NewAssertionErrorWithWrappedErrf(err,
				"binary op %q", expr)
		}
	}
	return res, err
}

// Eval implements the TypedExpr interface.
func (expr *CaseExpr) Eval(ctx *EvalContext) (Datum, error) {
	if expr.Expr != nil {
		// CASE <val> WHEN <expr> THEN ...
		//
		// For each "when" expression we compare for equality to <val>.
		val, err := expr.Expr.(TypedExpr).Eval(ctx)
		if err != nil {
			return nil, err
		}

		for _, when := range expr.Whens {
			arg, err := when.Cond.(TypedExpr).Eval(ctx)
			if err != nil {
				return nil, err
			}
			d, err := evalComparison(ctx, MakeComparisonOperator(EQ), val, arg)
			if err != nil {
				return nil, err
			}
			if v, err := GetBool(d); err != nil {
				return nil, err
			} else if v {
				return when.Val.(TypedExpr).Eval(ctx)
			}
		}
	} else {
		// CASE WHEN <bool-expr> THEN ...
		for _, when := range expr.Whens {
			d, err := when.Cond.(TypedExpr).Eval(ctx)
			if err != nil {
				return nil, err
			}
			if v, err := GetBool(d); err != nil {
				return nil, err
			} else if v {
				return when.Val.(TypedExpr).Eval(ctx)
			}
		}
	}

	if expr.Else != nil {
		return expr.Else.(TypedExpr).Eval(ctx)
	}
	return DNull, nil
}

// pgSignatureRegexp matches a Postgres function type signature, capturing the
// name of the function into group 1.
// e.g. function(a, b, c) or function( a )
var pgSignatureRegexp = regexp.MustCompile(`^\s*([\w\."]+)\s*\((?:(?:\s*[\w"]+\s*,)*\s*[\w"]+)?\s*\)\s*$`)

// Eval implements the TypedExpr interface.
func (expr *CastExpr) Eval(ctx *EvalContext) (Datum, error) {
	d, err := expr.Expr.(TypedExpr).Eval(ctx)
	if err != nil {
		return nil, err
	}

	// NULL cast to anything is NULL.
	if d == DNull {
		return d, nil
	}
	d = UnwrapDatum(ctx, d)
	return PerformCast(ctx, d, expr.ResolvedType())
}

// Eval implements the TypedExpr interface.
func (expr *IndirectionExpr) Eval(ctx *EvalContext) (Datum, error) {
	var subscriptIdx int
	for i, t := range expr.Indirection {
		if t.Slice || i > 0 {
			return nil, errors.AssertionFailedf("unsupported feature should have been rejected during planning")
		}

		d, err := t.Begin.(TypedExpr).Eval(ctx)
		if err != nil {
			return nil, err
		}
		if d == DNull {
			return d, nil
		}
		subscriptIdx = int(MustBeDInt(d))
	}

	d, err := expr.Expr.(TypedExpr).Eval(ctx)
	if err != nil {
		return nil, err
	}
	if d == DNull {
		return d, nil
	}

	// Index into the DArray, using 1-indexing.
	arr := MustBeDArray(d)

	// VECTOR types use 0-indexing.
	switch arr.customOid {
	case oid.T_oidvector, oid.T_int2vector:
		subscriptIdx++
	}
	if subscriptIdx < 1 || subscriptIdx > arr.Len() {
		return DNull, nil
	}
	return arr.Array[subscriptIdx-1], nil
}

// Eval implements the TypedExpr interface.
func (expr *CollateExpr) Eval(ctx *EvalContext) (Datum, error) {
	d, err := expr.Expr.(TypedExpr).Eval(ctx)
	if err != nil {
		return nil, err
	}
	unwrapped := UnwrapDatum(ctx, d)
	if unwrapped == DNull {
		return DNull, nil
	}
	switch d := unwrapped.(type) {
	case *DString:
		return NewDCollatedString(string(*d), expr.Locale, &ctx.CollationEnv)
	case *DCollatedString:
		return NewDCollatedString(d.Contents, expr.Locale, &ctx.CollationEnv)
	default:
		return nil, pgerror.Newf(pgcode.DatatypeMismatch, "incompatible type for COLLATE: %s", d)
	}
}

// Eval implements the TypedExpr interface.
func (expr *ColumnAccessExpr) Eval(ctx *EvalContext) (Datum, error) {
	d, err := expr.Expr.(TypedExpr).Eval(ctx)
	if err != nil {
		return nil, err
	}
	return d.(*DTuple).D[expr.ColIndex], nil
}

// Eval implements the TypedExpr interface.
func (expr *CoalesceExpr) Eval(ctx *EvalContext) (Datum, error) {
	for _, e := range expr.Exprs {
		d, err := e.(TypedExpr).Eval(ctx)
		if err != nil {
			return nil, err
		}
		if d != DNull {
			return d, nil
		}
	}
	return DNull, nil
}

// Eval implements the TypedExpr interface.
// Note: if you're modifying this function, please make sure to adjust
// colexeccmp.ComparisonExprAdapter implementations accordingly.
func (expr *ComparisonExpr) Eval(ctx *EvalContext) (Datum, error) {
	left, err := expr.Left.(TypedExpr).Eval(ctx)
	if err != nil {
		return nil, err
	}
	right, err := expr.Right.(TypedExpr).Eval(ctx)
	if err != nil {
		return nil, err
	}

	op := expr.Operator
	if op.Symbol.HasSubOperator() {
		return EvalComparisonExprWithSubOperator(ctx, expr, left, right)
	}

	_, newLeft, newRight, _, not := FoldComparisonExpr(op, left, right)
	if !expr.Fn.NullableArgs && (newLeft == DNull || newRight == DNull) {
		return DNull, nil
	}
	d, err := expr.Fn.Fn(ctx, newLeft.(Datum), newRight.(Datum))
	if d == DNull || err != nil {
		return d, err
	}
	b, ok := d.(*DBool)
	if !ok {
		return nil, errors.AssertionFailedf("%v is %T and not *DBool", d, d)
	}
	return MakeDBool(*b != DBool(not)), nil
}

// EvalComparisonExprWithSubOperator evaluates a comparison expression that has
// sub-operator.
func EvalComparisonExprWithSubOperator(
	ctx *EvalContext, expr *ComparisonExpr, left, right Datum,
) (Datum, error) {
	var datums Datums
	// Right is either a tuple or an array of Datums.
	if !expr.Fn.NullableArgs && right == DNull {
		return DNull, nil
	} else if tuple, ok := AsDTuple(right); ok {
		datums = tuple.D
	} else if array, ok := AsDArray(right); ok {
		datums = array.Array
	} else {
		return nil, errors.AssertionFailedf("unhandled right expression %s", right)
	}
	return evalDatumsCmp(ctx, expr.Operator, expr.SubOperator, expr.Fn, left, datums)
}

// EvalArgsAndGetGenerator evaluates the arguments and instantiates a
// ValueGenerator for use by set projections.
func (expr *FuncExpr) EvalArgsAndGetGenerator(ctx *EvalContext) (ValueGenerator, error) {
	if expr.fn == nil || expr.fnProps.Class != GeneratorClass {
		return nil, errors.AssertionFailedf("cannot call EvalArgsAndGetGenerator() on non-aggregate function: %q", ErrString(expr))
	}
	nullArg, args, err := expr.evalArgs(ctx)
	if err != nil || nullArg {
		return nil, err
	}
	return expr.fn.Generator(ctx, args)
}

// evalArgs evaluates just the function application's arguments.
// The returned bool indicates that the NULL should be propagated.
func (expr *FuncExpr) evalArgs(ctx *EvalContext) (bool, Datums, error) {
	args := make(Datums, len(expr.Exprs))
	for i, e := range expr.Exprs {
		arg, err := e.(TypedExpr).Eval(ctx)
		if err != nil {
			return false, nil, err
		}
		if arg == DNull && !expr.fnProps.NullableArgs {
			return true, nil, nil
		}
		args[i] = arg
	}
	return false, args, nil
}

// MaybeWrapError updates non-nil error depending on the FuncExpr to provide
// more context.
func (expr *FuncExpr) MaybeWrapError(err error) error {
	// If we are facing an explicit error, propagate it unchanged.
	fName := expr.Func.String()
	if fName == `crdb_internal.force_error` {
		return err
	}
	// Otherwise, wrap it with context.
	newErr := errors.Wrapf(err, "%s()", errors.Safe(fName))
	// Count function errors as it flows out of the system. We need to handle
	// them this way because if we are facing a retry error, in particular those
	// generated by crdb_internal.force_retry(), Wrap() will propagate it as a
	// non-pgerror error (so that the executor can see it with the right type).
	newErr = errors.WithTelemetry(newErr, fName+"()")
	return newErr
}

// Eval implements the TypedExpr interface.
func (expr *FuncExpr) Eval(ctx *EvalContext) (Datum, error) {
	nullResult, args, err := expr.evalArgs(ctx)
	if err != nil {
		return nil, err
	}
	if nullResult {
		return DNull, err
	}

	res, err := expr.fn.Fn(ctx, args)
	if err != nil {
		return nil, expr.MaybeWrapError(err)
	}
	if ctx.TestingKnobs.AssertFuncExprReturnTypes {
		if err := ensureExpectedType(expr.fn.FixedReturnType(), res); err != nil {
			return nil, errors.NewAssertionErrorWithWrappedErrf(err, "function %q", expr)
		}
	}
	return res, nil
}

// ensureExpectedType will return an error if a datum does not match the
// provided type. If the expected type is Any or if the datum is a Null
// type, then no error will be returned.
func ensureExpectedType(exp *types.T, d Datum) error {
	if !(exp.Family() == types.AnyFamily || d.ResolvedType().Family() == types.UnknownFamily ||
		d.ResolvedType().Equivalent(exp)) {
		return errors.AssertionFailedf(
			"expected return type %q, got: %q", errors.Safe(exp), errors.Safe(d.ResolvedType()))
	}
	return nil
}

// Eval implements the TypedExpr interface.
func (expr *IfErrExpr) Eval(ctx *EvalContext) (Datum, error) {
	cond, evalErr := expr.Cond.(TypedExpr).Eval(ctx)
	if evalErr == nil {
		if expr.Else == nil {
			return DBoolFalse, nil
		}
		return cond, nil
	}
	if expr.ErrCode != nil {
		errpat, err := expr.ErrCode.(TypedExpr).Eval(ctx)
		if err != nil {
			return nil, err
		}
		if errpat == DNull {
			return nil, evalErr
		}
		errpatStr := string(MustBeDString(errpat))
		if code := pgerror.GetPGCode(evalErr); code != pgcode.MakeCode(errpatStr) {
			return nil, evalErr
		}
	}
	if expr.Else == nil {
		return DBoolTrue, nil
	}
	return expr.Else.(TypedExpr).Eval(ctx)
}

// Eval implements the TypedExpr interface.
func (expr *IfExpr) Eval(ctx *EvalContext) (Datum, error) {
	cond, err := expr.Cond.(TypedExpr).Eval(ctx)
	if err != nil {
		return nil, err
	}
	if cond == DBoolTrue {
		return expr.True.(TypedExpr).Eval(ctx)
	}
	return expr.Else.(TypedExpr).Eval(ctx)
}

// Eval implements the TypedExpr interface.
func (expr *IsOfTypeExpr) Eval(ctx *EvalContext) (Datum, error) {
	d, err := expr.Expr.(TypedExpr).Eval(ctx)
	if err != nil {
		return nil, err
	}
	datumTyp := d.ResolvedType()

	for _, t := range expr.ResolvedTypes() {
		if datumTyp.Equivalent(t) {
			return MakeDBool(DBool(!expr.Not)), nil
		}
	}
	return MakeDBool(DBool(expr.Not)), nil
}

// Eval implements the TypedExpr interface.
func (expr *NotExpr) Eval(ctx *EvalContext) (Datum, error) {
	d, err := expr.Expr.(TypedExpr).Eval(ctx)
	if err != nil {
		return nil, err
	}
	if d == DNull {
		return DNull, nil
	}
	v, err := GetBool(d)
	if err != nil {
		return nil, err
	}
	return MakeDBool(!v), nil
}

// Eval implements the TypedExpr interface.
func (expr *IsNullExpr) Eval(ctx *EvalContext) (Datum, error) {
	d, err := expr.Expr.(TypedExpr).Eval(ctx)
	if err != nil {
		return nil, err
	}
	if d == DNull {
		return MakeDBool(true), nil
	}
	if t, ok := d.(*DTuple); ok {
		// A tuple IS NULL if all elements are NULL.
		for _, tupleDatum := range t.D {
			if tupleDatum != DNull {
				return MakeDBool(false), nil
			}
		}
		return MakeDBool(true), nil
	}
	return MakeDBool(false), nil
}

// Eval implements the TypedExpr interface.
func (expr *IsNotNullExpr) Eval(ctx *EvalContext) (Datum, error) {
	d, err := expr.Expr.(TypedExpr).Eval(ctx)
	if err != nil {
		return nil, err
	}
	if d == DNull {
		return MakeDBool(false), nil
	}
	if t, ok := d.(*DTuple); ok {
		// A tuple IS NOT NULL if all elements are not NULL.
		for _, tupleDatum := range t.D {
			if tupleDatum == DNull {
				return MakeDBool(false), nil
			}
		}
		return MakeDBool(true), nil
	}
	return MakeDBool(true), nil
}

// Eval implements the TypedExpr interface.
func (expr *NullIfExpr) Eval(ctx *EvalContext) (Datum, error) {
	expr1, err := expr.Expr1.(TypedExpr).Eval(ctx)
	if err != nil {
		return nil, err
	}
	expr2, err := expr.Expr2.(TypedExpr).Eval(ctx)
	if err != nil {
		return nil, err
	}
	cond, err := evalComparison(ctx, MakeComparisonOperator(EQ), expr1, expr2)
	if err != nil {
		return nil, err
	}
	if cond == DBoolTrue {
		return DNull, nil
	}
	return expr1, nil
}

// Eval implements the TypedExpr interface.
func (expr *OrExpr) Eval(ctx *EvalContext) (Datum, error) {
	left, err := expr.Left.(TypedExpr).Eval(ctx)
	if err != nil {
		return nil, err
	}
	if left != DNull {
		if v, err := GetBool(left); err != nil {
			return nil, err
		} else if v {
			return left, nil
		}
	}
	right, err := expr.Right.(TypedExpr).Eval(ctx)
	if err != nil {
		return nil, err
	}
	if right == DNull {
		return DNull, nil
	}
	if v, err := GetBool(right); err != nil {
		return nil, err
	} else if v {
		return right, nil
	}
	if left == DNull {
		return DNull, nil
	}
	return DBoolFalse, nil
}

// Eval implements the TypedExpr interface.
func (expr *ParenExpr) Eval(ctx *EvalContext) (Datum, error) {
	return expr.Expr.(TypedExpr).Eval(ctx)
}

// Eval implements the TypedExpr interface.
func (expr *RangeCond) Eval(ctx *EvalContext) (Datum, error) {
	return nil, errors.AssertionFailedf("unhandled type %T", expr)
}

// Eval implements the TypedExpr interface.
func (expr *UnaryExpr) Eval(ctx *EvalContext) (Datum, error) {
	d, err := expr.Expr.(TypedExpr).Eval(ctx)
	if err != nil {
		return nil, err
	}
	if d == DNull {
		return DNull, nil
	}
	res, err := expr.fn.Fn(ctx, d)
	if err != nil {
		return nil, err
	}
	if ctx.TestingKnobs.AssertUnaryExprReturnTypes {
		if err := ensureExpectedType(expr.fn.ReturnType, res); err != nil {
			return nil, errors.NewAssertionErrorWithWrappedErrf(err, "unary op %q", expr)
		}
	}
	return res, err
}

// Eval implements the TypedExpr interface.
func (expr DefaultVal) Eval(ctx *EvalContext) (Datum, error) {
	return nil, errors.AssertionFailedf("unhandled type %T", expr)
}

// Eval implements the TypedExpr interface.
func (expr UnqualifiedStar) Eval(ctx *EvalContext) (Datum, error) {
	return nil, errors.AssertionFailedf("unhandled type %T", expr)
}

// Eval implements the TypedExpr interface.
func (expr *UnresolvedName) Eval(ctx *EvalContext) (Datum, error) {
	return nil, errors.AssertionFailedf("unhandled type %T", expr)
}

// Eval implements the TypedExpr interface.
func (expr *AllColumnsSelector) Eval(ctx *EvalContext) (Datum, error) {
	return nil, errors.AssertionFailedf("unhandled type %T", expr)
}

// Eval implements the TypedExpr interface.
func (expr *TupleStar) Eval(ctx *EvalContext) (Datum, error) {
	return nil, errors.AssertionFailedf("unhandled type %T", expr)
}

// Eval implements the TypedExpr interface.
func (expr *ColumnItem) Eval(ctx *EvalContext) (Datum, error) {
	return nil, errors.AssertionFailedf("unhandled type %T", expr)
}

// Eval implements the TypedExpr interface.
func (t *Tuple) Eval(ctx *EvalContext) (Datum, error) {
	tuple := NewDTupleWithLen(t.typ, len(t.Exprs))
	for i, v := range t.Exprs {
		d, err := v.(TypedExpr).Eval(ctx)
		if err != nil {
			return nil, err
		}
		tuple.D[i] = d
	}
	return tuple, nil
}

// arrayOfType returns a fresh DArray of the input type.
func arrayOfType(typ *types.T) (*DArray, error) {
	if typ.Family() != types.ArrayFamily {
		return nil, errors.AssertionFailedf("array node type (%v) is not types.TArray", typ)
	}
	if err := types.CheckArrayElementType(typ.ArrayContents()); err != nil {
		return nil, err
	}
	return NewDArray(typ.ArrayContents()), nil
}

// Eval implements the TypedExpr interface.
func (t *Array) Eval(ctx *EvalContext) (Datum, error) {
	array, err := arrayOfType(t.ResolvedType())
	if err != nil {
		return nil, err
	}

	for _, v := range t.Exprs {
		d, err := v.(TypedExpr).Eval(ctx)
		if err != nil {
			return nil, err
		}
		if err := array.Append(d); err != nil {
			return nil, err
		}
	}
	return array, nil
}

// Eval implements the TypedExpr interface.
func (expr *Subquery) Eval(ctx *EvalContext) (Datum, error) {
	return ctx.Planner.EvalSubquery(expr)
}

// Eval implements the TypedExpr interface.
func (t *ArrayFlatten) Eval(ctx *EvalContext) (Datum, error) {
	array, err := arrayOfType(t.ResolvedType())
	if err != nil {
		return nil, err
	}

	d, err := t.Subquery.(TypedExpr).Eval(ctx)
	if err != nil {
		return nil, err
	}

	tuple, ok := d.(*DTuple)
	if !ok {
		return nil, errors.AssertionFailedf("array subquery result (%v) is not DTuple", d)
	}
	array.Array = tuple.D
	return array, nil
}

// Eval implements the TypedExpr interface.
func (t *DBitArray) Eval(_ *EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the TypedExpr interface.
func (t *DBool) Eval(_ *EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the TypedExpr interface.
func (t *DBytes) Eval(_ *EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the TypedExpr interface.
func (t *DUuid) Eval(_ *EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the TypedExpr interface.
func (t *DIPAddr) Eval(_ *EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the TypedExpr interface.
func (t *DDate) Eval(_ *EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the TypedExpr interface.
func (t *DTime) Eval(_ *EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the TypedExpr interface.
func (t *DTimeTZ) Eval(_ *EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the TypedExpr interface.
func (t *DFloat) Eval(_ *EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the TypedExpr interface.
func (t *DDecimal) Eval(_ *EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the TypedExpr interface.
func (t *DInt) Eval(_ *EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the TypedExpr interface.
func (t *DInterval) Eval(_ *EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the TypedExpr interface.
func (t *DBox2D) Eval(_ *EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the TypedExpr interface.
func (t *DGeography) Eval(_ *EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the TypedExpr interface.
func (t *DGeometry) Eval(_ *EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the TypedExpr interface.
func (t *DEnum) Eval(_ *EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the TypedExpr interface.
func (t *DJSON) Eval(_ *EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the TypedExpr interface.
func (t dNull) Eval(_ *EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the TypedExpr interface.
func (t *DString) Eval(_ *EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the TypedExpr interface.
func (t *DCollatedString) Eval(_ *EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the TypedExpr interface.
func (t *DTimestamp) Eval(_ *EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the TypedExpr interface.
func (t *DTimestampTZ) Eval(_ *EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the TypedExpr interface.
func (t *DTuple) Eval(_ *EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the TypedExpr interface.
func (t *DArray) Eval(_ *EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the TypedExpr interface.
func (t *DOid) Eval(_ *EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the TypedExpr interface.
func (t *DOidWrapper) Eval(_ *EvalContext) (Datum, error) {
	return t, nil
}

func makeNoValueProvidedForPlaceholderErr(pIdx PlaceholderIdx) error {
	return pgerror.Newf(pgcode.UndefinedParameter,
		"no value provided for placeholder: $%d", pIdx+1,
	)
}

// Eval implements the TypedExpr interface.
func (t *Placeholder) Eval(ctx *EvalContext) (Datum, error) {
	if !ctx.HasPlaceholders() {
		// While preparing a query, there will be no available placeholders. A
		// placeholder evaluates to itself at this point.
		return t, nil
	}
	e, ok := ctx.Placeholders.Value(t.Idx)
	if !ok {
		return nil, makeNoValueProvidedForPlaceholderErr(t.Idx)
	}
	// Placeholder expressions cannot contain other placeholders, so we do
	// not need to recurse.
	typ := ctx.Placeholders.Types[t.Idx]
	if typ == nil {
		// All placeholders should be typed at this point.
		return nil, errors.AssertionFailedf("missing type for placeholder %s", t)
	}
	if !e.ResolvedType().Equivalent(typ) {
		// This happens when we overrode the placeholder's type during type
		// checking, since the placeholder's type hint didn't match the desired
		// type for the placeholder. In this case, we cast the expression to
		// the desired type.
		// TODO(jordan): introduce a restriction on what casts are allowed here.
		cast := NewTypedCastExpr(e, typ)
		return cast.Eval(ctx)
	}
	return e.Eval(ctx)
}

func evalComparison(ctx *EvalContext, op ComparisonOperator, left, right Datum) (Datum, error) {
	if left == DNull || right == DNull {
		return DNull, nil
	}
	ltype := left.ResolvedType()
	rtype := right.ResolvedType()
	if fn, ok := CmpOps[op.Symbol].LookupImpl(ltype, rtype); ok {
		return fn.Fn(ctx, left, right)
	}
	return nil, pgerror.Newf(
		pgcode.UndefinedFunction, "unsupported comparison operator: <%s> %s <%s>", ltype, op, rtype)
}

// FoldComparisonExpr folds a given comparison operation and its expressions
// into an equivalent operation that will hit in the CmpOps map, returning
// this new operation, along with potentially flipped operands and "flipped"
// and "not" flags.
func FoldComparisonExpr(
	op ComparisonOperator, left, right Expr,
) (newOp ComparisonOperator, newLeft Expr, newRight Expr, flipped bool, not bool) {
	switch op.Symbol {
	case NE:
		// NE(left, right) is implemented as !EQ(left, right).
		return MakeComparisonOperator(EQ), left, right, false, true
	case GT:
		// GT(left, right) is implemented as LT(right, left)
		return MakeComparisonOperator(LT), right, left, true, false
	case GE:
		// GE(left, right) is implemented as LE(right, left)
		return MakeComparisonOperator(LE), right, left, true, false
	case NotIn:
		// NotIn(left, right) is implemented as !IN(left, right)
		return MakeComparisonOperator(In), left, right, false, true
	case NotLike:
		// NotLike(left, right) is implemented as !Like(left, right)
		return MakeComparisonOperator(Like), left, right, false, true
	case NotILike:
		// NotILike(left, right) is implemented as !ILike(left, right)
		return MakeComparisonOperator(ILike), left, right, false, true
	case NotSimilarTo:
		// NotSimilarTo(left, right) is implemented as !SimilarTo(left, right)
		return MakeComparisonOperator(SimilarTo), left, right, false, true
	case NotRegMatch:
		// NotRegMatch(left, right) is implemented as !RegMatch(left, right)
		return MakeComparisonOperator(RegMatch), left, right, false, true
	case NotRegIMatch:
		// NotRegIMatch(left, right) is implemented as !RegIMatch(left, right)
		return MakeComparisonOperator(RegIMatch), left, right, false, true
	case IsDistinctFrom:
		// IsDistinctFrom(left, right) is implemented as !IsNotDistinctFrom(left, right)
		// Note: this seems backwards, but IS NOT DISTINCT FROM is an extended
		// version of IS and IS DISTINCT FROM is an extended version of IS NOT.
		return MakeComparisonOperator(IsNotDistinctFrom), left, right, false, true
	}
	return op, left, right, false, false
}

// hasUnescapedSuffix returns true if the ending byte is suffix and s has an
// even number of escapeTokens preceding suffix. Otherwise hasUnescapedSuffix
// will return false.
func hasUnescapedSuffix(s string, suffix byte, escapeToken string) bool {
	if s[len(s)-1] == suffix {
		var count int
		idx := len(s) - len(escapeToken) - 1
		for idx >= 0 && s[idx:idx+len(escapeToken)] == escapeToken {
			count++
			idx -= len(escapeToken)
		}
		return count%2 == 0
	}
	return false
}

// Simplifies LIKE/ILIKE expressions that do not need full regular expressions to
// evaluate the condition. For example, when the expression is just checking to see
// if a string starts with a given pattern.
func optimizedLikeFunc(
	pattern string, caseInsensitive bool, escape rune,
) (func(string) (bool, error), error) {
	switch len(pattern) {
	case 0:
		return func(s string) (bool, error) {
			return s == "", nil
		}, nil
	case 1:
		switch pattern[0] {
		case '%':
			if escape == '%' {
				return nil, pgerror.Newf(pgcode.InvalidEscapeSequence, "LIKE pattern must not end with escape character")
			}
			return func(s string) (bool, error) {
				return true, nil
			}, nil
		case '_':
			if escape == '_' {
				return nil, pgerror.Newf(pgcode.InvalidEscapeSequence, "LIKE pattern must not end with escape character")
			}
			return func(s string) (bool, error) {
				if len(s) == 0 {
					return false, nil
				}
				firstChar, _ := utf8.DecodeRuneInString(s)
				if firstChar == utf8.RuneError {
					return false, errors.Errorf("invalid encoding of the first character in string %s", s)
				}
				return len(s) == len(string(firstChar)), nil
			}, nil
		}
	default:
		if !strings.ContainsAny(pattern[1:len(pattern)-1], "_%") {
			// Patterns with even number of escape characters preceding the ending
			// `%` will have anyEnd set to true (if `%` itself is not an escape
			// character). Otherwise anyEnd will be set to false.
			anyEnd := hasUnescapedSuffix(pattern, '%', string(escape)) && escape != '%'
			// If '%' is the escape character, then it's not a wildcard.
			anyStart := pattern[0] == '%' && escape != '%'

			// Patterns with even number of escape characters preceding the ending
			// `_` will have singleAnyEnd set to true (if `_` itself is not an escape
			// character). Otherwise singleAnyEnd will be set to false.
			singleAnyEnd := hasUnescapedSuffix(pattern, '_', string(escape)) && escape != '_'
			// If '_' is the escape character, then it's not a wildcard.
			singleAnyStart := pattern[0] == '_' && escape != '_'

			// Since we've already checked for escaped characters
			// at the end, we can un-escape every character.
			// This is required since we do direct string
			// comparison.
			var err error
			if pattern, err = unescapePattern(pattern, string(escape), true /* emitEscapeCharacterLastError */); err != nil {
				return nil, err
			}
			switch {
			case anyEnd && anyStart:
				return func(s string) (bool, error) {
					substr := pattern[1 : len(pattern)-1]
					if caseInsensitive {
						s, substr = strings.ToUpper(s), strings.ToUpper(substr)
					}
					return strings.Contains(s, substr), nil
				}, nil

			case anyEnd:
				return func(s string) (bool, error) {
					prefix := pattern[:len(pattern)-1]
					if singleAnyStart {
						if len(s) == 0 {
							return false, nil
						}
						prefix = prefix[1:]
						firstChar, _ := utf8.DecodeRuneInString(s)
						if firstChar == utf8.RuneError {
							return false, errors.Errorf("invalid encoding of the first character in string %s", s)
						}
						s = s[len(string(firstChar)):]
					}
					if caseInsensitive {
						s, prefix = strings.ToUpper(s), strings.ToUpper(prefix)
					}
					return strings.HasPrefix(s, prefix), nil
				}, nil

			case anyStart:
				return func(s string) (bool, error) {
					suffix := pattern[1:]
					if singleAnyEnd {
						if len(s) == 0 {
							return false, nil
						}

						suffix = suffix[:len(suffix)-1]
						lastChar, _ := utf8.DecodeLastRuneInString(s)
						if lastChar == utf8.RuneError {
							return false, errors.Errorf("invalid encoding of the last character in string %s", s)
						}
						s = s[:len(s)-len(string(lastChar))]
					}
					if caseInsensitive {
						s, suffix = strings.ToUpper(s), strings.ToUpper(suffix)
					}
					return strings.HasSuffix(s, suffix), nil
				}, nil

			case singleAnyStart || singleAnyEnd:
				return func(s string) (bool, error) {
					if len(s) < 1 {
						return false, nil
					}
					firstChar, _ := utf8.DecodeRuneInString(s)
					if firstChar == utf8.RuneError {
						return false, errors.Errorf("invalid encoding of the first character in string %s", s)
					}
					lastChar, _ := utf8.DecodeLastRuneInString(s)
					if lastChar == utf8.RuneError {
						return false, errors.Errorf("invalid encoding of the last character in string %s", s)
					}
					if singleAnyStart && singleAnyEnd && len(string(firstChar))+len(string(lastChar)) > len(s) {
						return false, nil
					}

					if singleAnyStart {
						pattern = pattern[1:]
						s = s[len(string(firstChar)):]
					}

					if singleAnyEnd {
						pattern = pattern[:len(pattern)-1]
						s = s[:len(s)-len(string(lastChar))]
					}

					if caseInsensitive {
						s, pattern = strings.ToUpper(s), strings.ToUpper(pattern)
					}

					// We don't have to check for
					// prefixes/suffixes since we do not
					// have '%':
					//  - singleAnyEnd && anyStart handled
					//    in case anyStart
					//  - singleAnyStart && anyEnd handled
					//    in case anyEnd
					return s == pattern, nil
				}, nil
			}
		}
	}
	return nil, nil
}

type likeKey struct {
	s               string
	caseInsensitive bool
	escape          rune
}

// unescapePattern unescapes a pattern for a given escape token.
// It handles escaped escape tokens properly by maintaining them as the escape
// token in the return string.
// For example, suppose we have escape token `\` (e.g. `B` is escaped in
// `A\BC` and `\` is escaped in `A\\C`).
// We need to convert
//    `\` --> ``
//    `\\` --> `\`
// We cannot simply use strings.Replace for each conversion since the first
// conversion will incorrectly replace our escaped escape token `\\` with ``.
// Another example is if our escape token is `\\` (e.g. after
// regexp.QuoteMeta).
// We need to convert
//    `\\` --> ``
//    `\\\\` --> `\\`
func unescapePattern(
	pattern, escapeToken string, emitEscapeCharacterLastError bool,
) (string, error) {
	escapedEscapeToken := escapeToken + escapeToken

	// We need to subtract the escaped escape tokens to avoid double
	// counting.
	nEscapes := strings.Count(pattern, escapeToken) - strings.Count(pattern, escapedEscapeToken)
	if nEscapes == 0 {
		return pattern, nil
	}

	// Allocate buffer for final un-escaped pattern.
	ret := make([]byte, len(pattern)-nEscapes*len(escapeToken))
	retWidth := 0
	for i := 0; i < nEscapes; i++ {
		nextIdx := strings.Index(pattern, escapeToken)
		if nextIdx == len(pattern)-len(escapeToken) && emitEscapeCharacterLastError {
			return "", pgerror.Newf(pgcode.InvalidEscapeSequence, `LIKE pattern must not end with escape character`)
		}

		retWidth += copy(ret[retWidth:], pattern[:nextIdx])

		if nextIdx < len(pattern)-len(escapedEscapeToken) && pattern[nextIdx:nextIdx+len(escapedEscapeToken)] == escapedEscapeToken {
			// We have an escaped escape token.
			// We want to keep it as the original escape token in
			// the return string.
			retWidth += copy(ret[retWidth:], escapeToken)
			pattern = pattern[nextIdx+len(escapedEscapeToken):]
			continue
		}

		// Skip over the escape character we removed.
		pattern = pattern[nextIdx+len(escapeToken):]
	}

	retWidth += copy(ret[retWidth:], pattern)
	return string(ret[0:retWidth]), nil
}

// replaceUnescaped replaces all instances of oldStr that are not escaped (read:
// preceded) with the specified unescape token with newStr.
// For example, with an escape token of `\\`
//    replaceUnescaped("TE\\__ST", "_", ".", `\\`) --> "TE\\_.ST"
//    replaceUnescaped("TE\\%%ST", "%", ".*", `\\`) --> "TE\\%.*ST"
// If the preceding escape token is escaped, then oldStr will be replaced.
// For example
//    replaceUnescaped("TE\\\\_ST", "_", ".", `\\`) --> "TE\\\\.ST"
func replaceUnescaped(s, oldStr, newStr string, escapeToken string) string {
	// We count the number of occurrences of 'oldStr'.
	// This however can be an overestimate since the oldStr token could be
	// escaped.  e.g. `\\_`.
	nOld := strings.Count(s, oldStr)
	if nOld == 0 {
		return s
	}

	// Allocate buffer for final string.
	// This can be an overestimate since some of the oldStr tokens may
	// be escaped.
	// This is fine since we keep track of the running number of bytes
	// actually copied.
	// It's rather difficult to count the exact number of unescaped
	// tokens without manually iterating through the entire string and
	// keeping track of escaped escape tokens.
	retLen := len(s)
	// If len(newStr) - len(oldStr) < 0, then this can under-allocate which
	// will not behave correctly with copy.
	if addnBytes := nOld * (len(newStr) - len(oldStr)); addnBytes > 0 {
		retLen += addnBytes
	}
	ret := make([]byte, retLen)
	retWidth := 0
	start := 0
OldLoop:
	for i := 0; i < nOld; i++ {
		nextIdx := start + strings.Index(s[start:], oldStr)

		escaped := false
		for {
			// We need to look behind to check if the escape token
			// is really an escape token.
			// E.g. if our specified escape token is `\\` and oldStr
			// is `_`, then
			//    `\\_` --> escaped
			//    `\\\\_` --> not escaped
			//    `\\\\\\_` --> escaped
			curIdx := nextIdx
			lookbehindIdx := curIdx - len(escapeToken)
			for lookbehindIdx >= 0 && s[lookbehindIdx:curIdx] == escapeToken {
				escaped = !escaped
				curIdx = lookbehindIdx
				lookbehindIdx = curIdx - len(escapeToken)
			}

			// The token was not be escaped. Proceed.
			if !escaped {
				break
			}

			// Token was escaped. Copy everything over and continue.
			retWidth += copy(ret[retWidth:], s[start:nextIdx+len(oldStr)])
			start = nextIdx + len(oldStr)

			// Continue with next oldStr token.
			continue OldLoop
		}

		// Token was not escaped so we replace it with newStr.
		// Two copies is more efficient than concatenating the slices.
		retWidth += copy(ret[retWidth:], s[start:nextIdx])
		retWidth += copy(ret[retWidth:], newStr)
		start = nextIdx + len(oldStr)
	}

	retWidth += copy(ret[retWidth:], s[start:])
	return string(ret[0:retWidth])
}

// Replaces all custom escape characters in s with `\\` only when they are unescaped.          (1)
// E.g. original pattern       after QuoteMeta       after replaceCustomEscape with '@' as escape
//        '@w@w'          ->      '@w@w'        ->        '\\w\\w'
//        '@\@\'          ->      '@\\@\\'      ->        '\\\\\\\\'
//
// When an escape character is escaped, we replace it with its single occurrence.              (2)
// E.g. original pattern       after QuoteMeta       after replaceCustomEscape with '@' as escape
//        '@@w@w'         ->      '@@w@w'       ->        '@w\\w'
//        '@@@\'          ->      '@@@\\'       ->        '@\\\\'
//
// At the same time, we do not want to confuse original backslashes (which
// after QuoteMeta are '\\') with backslashes that replace our custom escape characters,
// so we escape these original backslashes again by converting '\\' into '\\\\'.               (3)
// E.g. original pattern       after QuoteMeta       after replaceCustomEscape with '@' as escape
//        '@\'            ->      '@\\'         ->        '\\\\\\'
//        '@\@@@\'        ->      '@\\@@@\\'    ->        '\\\\\\@\\\\\\'
//
// Explanation of the last example:
// 1. we replace '@' with '\\' since it's unescaped;
// 2. we escape single original backslash ('\' is not our escape character, so we want
// the pattern to understand it) by putting an extra backslash in front of it. However,
// we later will call unescapePattern, so we need to double our double backslashes.
// Therefore, '\\' is converted into '\\\\'.
// 3. '@@' is replaced by '@' because it is escaped escape character.
// 4. '@' is replaced with '\\' since it's unescaped.
// 5. Similar logic to step 2: '\\' -> '\\\\'.
//
// We always need to keep in mind that later call of unescapePattern
// to actually unescape '\\' and '\\\\' is necessary and that
// escape must be a single unicode character and not `\`.
func replaceCustomEscape(s string, escape rune) (string, error) {
	changed, retLen, err := calculateLengthAfterReplacingCustomEscape(s, escape)
	if err != nil {
		return "", err
	}
	if !changed {
		return s, nil
	}

	sLen := len(s)
	ret := make([]byte, retLen)
	retIndex, sIndex := 0, 0
	for retIndex < retLen {
		sRune, w := utf8.DecodeRuneInString(s[sIndex:])
		if sRune == escape {
			// We encountered an escape character.
			if sIndex+w < sLen {
				// Escape character is not the last character in s, so we need
				// to look ahead to figure out how to process it.
				tRune, _ := utf8.DecodeRuneInString(s[(sIndex + w):])
				if tRune == escape {
					// Escape character is escaped, so we replace its two occurrences with just one. See (2).
					// We copied only one escape character to ret, so we advance retIndex only by w.
					// Since we've already processed two characters in s, we advance sIndex by 2*w.
					utf8.EncodeRune(ret[retIndex:], escape)
					retIndex += w
					sIndex += 2 * w
				} else {
					// Escape character is unescaped, so we replace it with `\\`. See (1).
					// Since we've added two bytes to ret, we advance retIndex by 2.
					// We processed only a single escape character in s, we advance sIndex by w.
					ret[retIndex] = '\\'
					ret[retIndex+1] = '\\'
					retIndex += 2
					sIndex += w
				}
			} else {
				// Escape character is the last character in s which is an error
				// that must have been caught in calculateLengthAfterReplacingCustomEscape.
				return "", errors.AssertionFailedf(
					"unexpected: escape character is the last one in replaceCustomEscape.")
			}
		} else if s[sIndex] == '\\' {
			// We encountered a backslash, so we need to look ahead to figure out how
			// to process it.
			if sIndex+1 == sLen {
				// This case should never be reached since it should
				// have been caught in calculateLengthAfterReplacingCustomEscape.
				return "", errors.AssertionFailedf(
					"unexpected: a single backslash encountered in replaceCustomEscape.")
			} else if s[sIndex+1] == '\\' {
				// We want to escape '\\' to `\\\\` for correct processing later by unescapePattern. See (3).
				// Since we've added four characters to ret, we advance retIndex by 4.
				// Since we've already processed two characters in s, we advance sIndex by 2.
				ret[retIndex] = '\\'
				ret[retIndex+1] = '\\'
				ret[retIndex+2] = '\\'
				ret[retIndex+3] = '\\'
				retIndex += 4
				sIndex += 2
			} else {
				// A metacharacter other than a backslash is escaped here.
				// Note: all metacharacters are encoded as a single byte, so it is
				// correct to just convert it to string and to compare against a char
				// in s.
				if string(s[sIndex+1]) == string(escape) {
					// The metacharacter is our custom escape character. We need to look
					// ahead to process it.
					if sIndex+2 == sLen {
						// Escape character is the last character in s which is an error
						// that must have been caught in calculateLengthAfterReplacingCustomEscape.
						return "", errors.AssertionFailedf(
							"unexpected: escape character is the last one in replaceCustomEscape.")
					}
					if sIndex+4 <= sLen {
						if s[sIndex+2] == '\\' && string(s[sIndex+3]) == string(escape) {
							// We have a sequence of `\`+escape+`\`+escape which is replaced
							// by `\`+escape.
							ret[retIndex] = '\\'
							// Note: all metacharacters are encoded as a single byte, so it
							// is safe to just convert it to string and take the first
							// character.
							ret[retIndex+1] = string(escape)[0]
							retIndex += 2
							sIndex += 4
							continue
						}
					}
					// The metacharacter is escaping something different than itself, so
					// `\`+escape will be replaced by `\`.
					ret[retIndex] = '\\'
					retIndex++
					sIndex += 2
				} else {
					// The metacharacter is not our custom escape character, so we're
					// simply copying the backslash and the metacharacter.
					ret[retIndex] = '\\'
					ret[retIndex+1] = s[sIndex+1]
					retIndex += 2
					sIndex += 2
				}
			}
		} else {
			// Regular symbol, so we simply copy it.
			copy(ret[retIndex:], s[sIndex:sIndex+w])
			retIndex += w
			sIndex += w
		}
	}
	return string(ret), nil
}

// calculateLengthAfterReplacingCustomEscape returns whether the pattern changes, the length
// of the resulting pattern after calling replaceCustomEscape, and any error if found.
func calculateLengthAfterReplacingCustomEscape(s string, escape rune) (bool, int, error) {
	changed := false
	retLen, sLen := 0, len(s)
	for i := 0; i < sLen; {
		sRune, w := utf8.DecodeRuneInString(s[i:])
		if sRune == escape {
			// We encountered an escape character.
			if i+w < sLen {
				// Escape character is not the last character in s, so we need
				// to look ahead to figure out how to process it.
				tRune, _ := utf8.DecodeRuneInString(s[(i + w):])
				if tRune == escape {
					// Escape character is escaped, so we'll replace its two occurrences with just one.
					// See (2) in the comment above replaceCustomEscape.
					changed = true
					retLen += w
					i += 2 * w
				} else {
					// Escape character is unescaped, so we'll replace it with `\\`.
					// See (1) in the comment above replaceCustomEscape.
					changed = true
					retLen += 2
					i += w
				}
			} else {
				// Escape character is the last character in s, so we need to return an error.
				return false, 0, pgerror.Newf(pgcode.InvalidEscapeSequence, "LIKE pattern must not end with escape character")
			}
		} else if s[i] == '\\' {
			// We encountered a backslash, so we need to look ahead to figure out how
			// to process it.
			if i+1 == sLen {
				// This case should never be reached because the backslash should be
				// escaping one of regexp metacharacters.
				return false, 0, pgerror.Newf(pgcode.InvalidEscapeSequence, "Unexpected behavior during processing custom escape character.")
			} else if s[i+1] == '\\' {
				// We'll want to escape '\\' to `\\\\` for correct processing later by
				// unescapePattern. See (3) in the comment above replaceCustomEscape.
				changed = true
				retLen += 4
				i += 2
			} else {
				// A metacharacter other than a backslash is escaped here.
				if string(s[i+1]) == string(escape) {
					// The metacharacter is our custom escape character. We need to look
					// ahead to process it.
					if i+2 == sLen {
						// Escape character is the last character in s, so we need to return an error.
						return false, 0, pgerror.Newf(pgcode.InvalidEscapeSequence, "LIKE pattern must not end with escape character")
					}
					if i+4 <= sLen {
						if s[i+2] == '\\' && string(s[i+3]) == string(escape) {
							// We have a sequence of `\`+escape+`\`+escape which will be
							// replaced by `\`+escape.
							changed = true
							retLen += 2
							i += 4
							continue
						}
					}
					// The metacharacter is escaping something different than itself, so
					// `\`+escape will be replaced by `\`.
					changed = true
					retLen++
					i += 2
				} else {
					// The metacharacter is not our custom escape character, so we're
					// simply copying the backslash and the metacharacter.
					retLen += 2
					i += 2
				}
			}
		} else {
			// Regular symbol, so we'll simply copy it.
			retLen += w
			i += w
		}
	}
	return changed, retLen, nil
}

// Pattern implements the RegexpCacheKey interface.
// The strategy for handling custom escape character
// is to convert all unescaped escape character into '\'.
// k.escape can either be empty or a single character.
func (k likeKey) Pattern() (string, error) {
	// QuoteMeta escapes all regexp metacharacters (`\.+*?()|[]{}^$`) with a `\`.
	pattern := regexp.QuoteMeta(k.s)
	var err error
	if k.escape == 0 {
		// Replace all LIKE/ILIKE specific wildcards with standard wildcards
		// (escape character is empty - escape mechanism is turned off - so
		// all '%' and '_' are actual wildcards regardless of what precedes them.
		pattern = strings.Replace(pattern, `%`, `.*`, -1)
		pattern = strings.Replace(pattern, `_`, `.`, -1)
	} else if k.escape == '\\' {
		// Replace LIKE/ILIKE specific wildcards with standard wildcards only when
		// these wildcards are not escaped by '\\' (equivalent of '\' after QuoteMeta).
		pattern = replaceUnescaped(pattern, `%`, `.*`, `\\`)
		pattern = replaceUnescaped(pattern, `_`, `.`, `\\`)
	} else {
		// k.escape is non-empty and not `\`.
		// If `%` is escape character, then it's not a wildcard.
		if k.escape != '%' {
			// Replace LIKE/ILIKE specific wildcards '%' only if it's unescaped.
			if k.escape == '.' {
				// '.' is the escape character, so for correct processing later by
				// replaceCustomEscape we need to escape it by itself.
				pattern = replaceUnescaped(pattern, `%`, `..*`, regexp.QuoteMeta(string(k.escape)))
			} else if k.escape == '*' {
				// '*' is the escape character, so for correct processing later by
				// replaceCustomEscape we need to escape it by itself.
				pattern = replaceUnescaped(pattern, `%`, `.**`, regexp.QuoteMeta(string(k.escape)))
			} else {
				pattern = replaceUnescaped(pattern, `%`, `.*`, regexp.QuoteMeta(string(k.escape)))
			}
		}
		// If `_` is escape character, then it's not a wildcard.
		if k.escape != '_' {
			// Replace LIKE/ILIKE specific wildcards '_' only if it's unescaped.
			if k.escape == '.' {
				// '.' is the escape character, so for correct processing later by
				// replaceCustomEscape we need to escape it by itself.
				pattern = replaceUnescaped(pattern, `_`, `..`, regexp.QuoteMeta(string(k.escape)))
			} else {
				pattern = replaceUnescaped(pattern, `_`, `.`, regexp.QuoteMeta(string(k.escape)))
			}
		}

		// If a sequence of symbols ` escape+`\\` ` is unescaped, then that escape
		// character escapes backslash in the original pattern (we need to use double
		// backslash because of QuoteMeta behavior), so we want to "consume" the escape character.
		pattern = replaceUnescaped(pattern, string(k.escape)+`\\`, `\\`, regexp.QuoteMeta(string(k.escape)))

		// We want to replace all escape characters with `\\` only
		// when they are unescaped. When an escape character is escaped,
		// we replace it with its single occurrence.
		if pattern, err = replaceCustomEscape(pattern, k.escape); err != nil {
			return pattern, err
		}
	}

	// After QuoteMeta, all '\' were converted to '\\'.
	// After replaceCustomEscape, our custom unescaped escape characters were converted to `\\`,
	// so now our pattern contains only '\\' as escape tokens.
	// We need to unescape escaped escape tokens `\\` (now `\\\\`) and
	// other escaped characters `\A` (now `\\A`).
	if k.escape != 0 {
		// We do not want to return an error when pattern ends with the supposed escape character `\`
		// whereas the actual escape character is not `\`. The case when the pattern ends with
		// an actual escape character is handled in replaceCustomEscape. For example, with '-' as
		// the escape character on pattern 'abc\\' we do not want to return an error 'pattern ends
		// with escape character' because '\\' is not an escape character in this case.
		if pattern, err = unescapePattern(
			pattern,
			`\\`,
			k.escape == '\\', /* emitEscapeCharacterLastError */
		); err != nil {
			return "", err
		}
	}

	return anchorPattern(pattern, k.caseInsensitive), nil
}

type similarToKey struct {
	s      string
	escape rune
}

// Pattern implements the RegexpCacheKey interface.
func (k similarToKey) Pattern() (string, error) {
	pattern := similarEscapeCustomChar(k.s, k.escape, k.escape != 0)
	return anchorPattern(pattern, false), nil
}

// SimilarToEscape checks if 'unescaped' is SIMILAR TO 'pattern' using custom escape token 'escape'
// which must be either empty (which disables the escape mechanism) or a single unicode character.
func SimilarToEscape(ctx *EvalContext, unescaped, pattern, escape string) (Datum, error) {
	key, err := makeSimilarToKey(pattern, escape)
	if err != nil {
		return DBoolFalse, err
	}
	return matchRegexpWithKey(ctx, NewDString(unescaped), key)
}

// SimilarPattern converts a SQL regexp 'pattern' to a POSIX regexp 'pattern' using custom escape token 'escape'
// which must be either empty (which disables the escape mechanism) or a single unicode character.
func SimilarPattern(pattern, escape string) (Datum, error) {
	key, err := makeSimilarToKey(pattern, escape)
	if err != nil {
		return nil, err
	}
	pattern, err = key.Pattern()
	if err != nil {
		return nil, err
	}
	return NewDString(pattern), nil
}

// makeSimilarToKey makes a similarToKey using the given 'pattern' and 'escape'.
func makeSimilarToKey(pattern, escape string) (similarToKey, error) {
	var escapeRune rune
	var width int
	escapeRune, width = utf8.DecodeRuneInString(escape)
	if len(escape) > width {
		return similarToKey{}, pgerror.Newf(pgcode.InvalidEscapeSequence, "invalid escape string")
	}
	key := similarToKey{s: pattern, escape: escapeRune}
	return key, nil
}

type regexpKey struct {
	s               string
	caseInsensitive bool
}

// Pattern implements the RegexpCacheKey interface.
func (k regexpKey) Pattern() (string, error) {
	if k.caseInsensitive {
		return caseInsensitive(k.s), nil
	}
	return k.s, nil
}

// SimilarEscape converts a SQL:2008 regexp pattern to POSIX style, so it can
// be used by our regexp engine.
func SimilarEscape(pattern string) string {
	return similarEscapeCustomChar(pattern, '\\', true)
}

// similarEscapeCustomChar converts a SQL:2008 regexp pattern to POSIX style,
// so it can be used by our regexp engine. This version of the function allows
// for a custom escape character.
// 'isEscapeNonEmpty' signals whether 'escapeChar' should be treated as empty.
func similarEscapeCustomChar(pattern string, escapeChar rune, isEscapeNonEmpty bool) string {
	patternBuilder := make([]rune, 0, utf8.RuneCountInString(pattern))

	inCharClass := false
	afterEscape := false
	numQuotes := 0
	for _, c := range pattern {
		switch {
		case afterEscape:
			// For SUBSTRING patterns
			if c == '"' && !inCharClass {
				if numQuotes%2 == 0 {
					patternBuilder = append(patternBuilder, '(')
				} else {
					patternBuilder = append(patternBuilder, ')')
				}
				numQuotes++
			} else if c == escapeChar && len(string(escapeChar)) > 1 {
				// We encountered escaped escape unicode character represented by at least two bytes,
				// so we keep only its single occurrence and need not to prepend it by '\'.
				patternBuilder = append(patternBuilder, c)
			} else {
				patternBuilder = append(patternBuilder, '\\', c)
			}
			afterEscape = false
		case utf8.ValidRune(escapeChar) && c == escapeChar && isEscapeNonEmpty:
			// SQL99 escape character; do not immediately send to output
			afterEscape = true
		case inCharClass:
			if c == '\\' {
				patternBuilder = append(patternBuilder, '\\')
			}
			patternBuilder = append(patternBuilder, c)
			if c == ']' {
				inCharClass = false
			}
		case c == '[':
			patternBuilder = append(patternBuilder, c)
			inCharClass = true
		case c == '%':
			patternBuilder = append(patternBuilder, '.', '*')
		case c == '_':
			patternBuilder = append(patternBuilder, '.')
		case c == '(':
			// Convert to non-capturing parenthesis
			patternBuilder = append(patternBuilder, '(', '?', ':')
		case c == '\\', c == '.', c == '^', c == '$':
			// Escape these characters because they are NOT
			// metacharacters for SQL-style regexp
			patternBuilder = append(patternBuilder, '\\', c)
		default:
			patternBuilder = append(patternBuilder, c)
		}
	}

	return string(patternBuilder)
}

// caseInsensitive surrounds the transformed input string with
//   (?i: ... )
// which uses a non-capturing set of parens to turn a case sensitive
// regular expression pattern into a case insensitive regular
// expression pattern.
func caseInsensitive(pattern string) string {
	return fmt.Sprintf("(?i:%s)", pattern)
}

// anchorPattern surrounds the transformed input string with
//   ^(?s: ... )$
// which requires some explanation.  We need "^" and "$" to force
// the pattern to match the entire input string as per SQL99 spec.
// The "(?:" and ")" are a non-capturing set of parens; we have to have
// parens in case the string contains "|", else the "^" and "$" will
// be bound into the first and last alternatives which is not what we
// want, and the parens must be non capturing because we don't want them
// to count when selecting output for SUBSTRING.
// "?s" turns on "dot all" mode meaning a dot will match any single character
// (without turning this mode on, the dot matches any single character except
// for line breaks).
func anchorPattern(pattern string, caseInsensitive bool) string {
	if caseInsensitive {
		return fmt.Sprintf("^(?si:%s)$", pattern)
	}
	return fmt.Sprintf("^(?s:%s)$", pattern)
}

// FindEqualComparisonFunction looks up an overload of the "=" operator
// for a given pair of input operand types.
func FindEqualComparisonFunction(leftType, rightType *types.T) (TwoArgFn, bool) {
	fn, found := CmpOps[EQ].LookupImpl(leftType, rightType)
	if found {
		return fn.Fn, true
	}
	return nil, false
}

// IntPow computes the value of x^y.
func IntPow(x, y DInt) (*DInt, error) {
	xd := apd.New(int64(x), 0)
	yd := apd.New(int64(y), 0)
	_, err := DecimalCtx.Pow(xd, xd, yd)
	if err != nil {
		return nil, err
	}
	i, err := xd.Int64()
	if err != nil {
		return nil, ErrIntOutOfRange
	}
	return NewDInt(DInt(i)), nil
}

// PickFromTuple picks the greatest (or least value) from a tuple.
func PickFromTuple(ctx *EvalContext, greatest bool, args Datums) (Datum, error) {
	g := args[0]
	// Pick a greater (or smaller) value.
	for _, d := range args[1:] {
		var eval Datum
		var err error
		if greatest {
			eval, err = evalComparison(ctx, MakeComparisonOperator(LT), g, d)
		} else {
			eval, err = evalComparison(ctx, MakeComparisonOperator(LT), d, g)
		}
		if err != nil {
			return nil, err
		}
		if eval == DBoolTrue ||
			(eval == DNull && g == DNull) {
			g = d
		}
	}
	return g, nil
}

// CallbackValueGenerator is a ValueGenerator that calls a supplied callback for
// producing the values. To be used with
// EvalContextTestingKnobs.CallbackGenerators.
type CallbackValueGenerator struct {
	// cb is the callback to be called for producing values. It gets passed in 0
	// as prev initially, and the value it previously returned for subsequent
	// invocations. Once it returns -1 or an error, it will not be invoked any
	// more.
	cb  func(ctx context.Context, prev int, txn *kv.Txn) (int, error)
	val int
	txn *kv.Txn
}

var _ ValueGenerator = &CallbackValueGenerator{}

// NewCallbackValueGenerator creates a new CallbackValueGenerator.
func NewCallbackValueGenerator(
	cb func(ctx context.Context, prev int, txn *kv.Txn) (int, error),
) *CallbackValueGenerator {
	return &CallbackValueGenerator{
		cb: cb,
	}
}

// ResolvedType is part of the ValueGenerator interface.
func (c *CallbackValueGenerator) ResolvedType() *types.T {
	return types.Int
}

// Start is part of the ValueGenerator interface.
func (c *CallbackValueGenerator) Start(_ context.Context, txn *kv.Txn) error {
	c.txn = txn
	return nil
}

// Next is part of the ValueGenerator interface.
func (c *CallbackValueGenerator) Next(ctx context.Context) (bool, error) {
	var err error
	c.val, err = c.cb(ctx, c.val, c.txn)
	if err != nil {
		return false, err
	}
	if c.val == -1 {
		return false, nil
	}
	return true, nil
}

// Values is part of the ValueGenerator interface.
func (c *CallbackValueGenerator) Values() (Datums, error) {
	return Datums{NewDInt(DInt(c.val))}, nil
}

// Close is part of the ValueGenerator interface.
func (c *CallbackValueGenerator) Close(_ context.Context) {}

// Sqrt returns the square root of x.
func Sqrt(x float64) (*DFloat, error) {
	if x < 0.0 {
		return nil, errSqrtOfNegNumber
	}
	return NewDFloat(DFloat(math.Sqrt(x))), nil
}

// DecimalSqrt returns the square root of x.
func DecimalSqrt(x *apd.Decimal) (*DDecimal, error) {
	if x.Sign() < 0 {
		return nil, errSqrtOfNegNumber
	}
	dd := &DDecimal{}
	_, err := DecimalCtx.Sqrt(&dd.Decimal, x)
	return dd, err
}

// Cbrt returns the cube root of x.
func Cbrt(x float64) (*DFloat, error) {
	return NewDFloat(DFloat(math.Cbrt(x))), nil
}

// DecimalCbrt returns the cube root of x.
func DecimalCbrt(x *apd.Decimal) (*DDecimal, error) {
	dd := &DDecimal{}
	_, err := DecimalCtx.Cbrt(&dd.Decimal, x)
	return dd, err
}
