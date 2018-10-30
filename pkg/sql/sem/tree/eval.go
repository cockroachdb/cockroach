// Copyright 2015 The Cockroach Authors.
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
	"context"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lib/pq/oid"
	"github.com/pkg/errors"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/arith"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

var (
	errIntOutOfRange   = pgerror.NewError(pgerror.CodeNumericValueOutOfRangeError, "integer out of range")
	errFloatOutOfRange = pgerror.NewError(pgerror.CodeNumericValueOutOfRangeError, "float out of range")
	errDecOutOfRange   = pgerror.NewError(pgerror.CodeNumericValueOutOfRangeError, "decimal out of range")

	// ErrDivByZero is reported on a division by zero.
	ErrDivByZero = pgerror.NewError(pgerror.CodeDivisionByZeroError, "division by zero")
	// ErrZeroModulus is reported when computing the rest of a division by zero.
	ErrZeroModulus = pgerror.NewError(pgerror.CodeDivisionByZeroError, "zero modulus")

	big10E6  = big.NewInt(1e6)
	big10E10 = big.NewInt(1e10)
)

// SecondsInDay is the number of seconds in a Day.
const SecondsInDay = 24 * 60 * 60

// UnaryOp is a unary operator.
type UnaryOp struct {
	Typ        types.T
	ReturnType types.T
	Fn         func(*EvalContext, Datum) (Datum, error)

	types   TypeList
	retType ReturnTyper
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

func init() {
	for op, overload := range UnaryOps {
		for i, impl := range overload {
			casted := impl.(*UnaryOp)
			casted.types = ArgTypes{{"arg", casted.Typ}}
			casted.retType = FixedReturnType(casted.ReturnType)
			UnaryOps[op][i] = casted
		}
	}
}

// unaryOpOverload is an overloaded set of unary operator implementations.
type unaryOpOverload []overloadImpl

// UnaryOps contains the unary operations indexed by operation type.
var UnaryOps = map[UnaryOperator]unaryOpOverload{
	UnaryMinus: {
		&UnaryOp{
			Typ:        types.Int,
			ReturnType: types.Int,
			Fn: func(_ *EvalContext, d Datum) (Datum, error) {
				i := MustBeDInt(d)
				if i == math.MinInt64 {
					return nil, errIntOutOfRange
				}
				return NewDInt(-i), nil
			},
		},
		&UnaryOp{
			Typ:        types.Float,
			ReturnType: types.Float,
			Fn: func(_ *EvalContext, d Datum) (Datum, error) {
				return NewDFloat(-*d.(*DFloat)), nil
			},
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
		},
		&UnaryOp{
			Typ:        types.Interval,
			ReturnType: types.Interval,
			Fn: func(_ *EvalContext, d Datum) (Datum, error) {
				i := d.(*DInterval).Duration
				i.Nanos = -i.Nanos
				i.Days = -i.Days
				i.Months = -i.Months
				return &DInterval{Duration: i}, nil
			},
		},
	},

	UnaryComplement: {
		&UnaryOp{
			Typ:        types.Int,
			ReturnType: types.Int,
			Fn: func(_ *EvalContext, d Datum) (Datum, error) {
				return NewDInt(^MustBeDInt(d)), nil
			},
		},
		&UnaryOp{
			Typ:        types.BitArray,
			ReturnType: types.BitArray,
			Fn: func(_ *EvalContext, d Datum) (Datum, error) {
				p := MustBeDBitArray(d)
				return &DBitArray{BitArray: bitarray.Not(p.BitArray)}, nil
			},
		},
		&UnaryOp{
			Typ:        types.INet,
			ReturnType: types.INet,
			Fn: func(_ *EvalContext, d Datum) (Datum, error) {
				ipAddr := MustBeDIPAddr(d).IPAddr
				return NewDIPAddr(DIPAddr{ipAddr.Complement()}), nil
			},
		},
	},
}

// BinOp is a binary operator.
type BinOp struct {
	LeftType     types.T
	RightType    types.T
	ReturnType   types.T
	NullableArgs bool
	Fn           func(*EvalContext, Datum, Datum) (Datum, error)

	types   TypeList
	retType ReturnTyper
}

func (op *BinOp) params() TypeList {
	return op.types
}

func (op *BinOp) matchParams(l, r types.T) bool {
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
func AppendToMaybeNullArray(typ types.T, left Datum, right Datum) (Datum, error) {
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
func PrependToMaybeNullArray(typ types.T, left Datum, right Datum) (Datum, error) {
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
	for _, t := range types.AnyNonArray {
		typ := t
		BinOps[Concat] = append(BinOps[Concat], &BinOp{
			LeftType:     types.TArray{Typ: typ},
			RightType:    typ,
			ReturnType:   types.TArray{Typ: typ},
			NullableArgs: true,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return AppendToMaybeNullArray(typ, left, right)
			},
		})

		BinOps[Concat] = append(BinOps[Concat], &BinOp{
			LeftType:     typ,
			RightType:    types.TArray{Typ: typ},
			ReturnType:   types.TArray{Typ: typ},
			NullableArgs: true,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return PrependToMaybeNullArray(typ, left, right)
			},
		})
	}
}

// ConcatArrays concatenates two arrays.
func ConcatArrays(typ types.T, left Datum, right Datum) (Datum, error) {
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

func initArrayToArrayConcatenation() {
	for _, t := range types.AnyNonArray {
		typ := t
		BinOps[Concat] = append(BinOps[Concat], &BinOp{
			LeftType:     types.TArray{Typ: typ},
			RightType:    types.TArray{Typ: typ},
			ReturnType:   types.TArray{Typ: typ},
			NullableArgs: true,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return ConcatArrays(typ, left, right)
			},
		})
	}
}

func init() {
	initArrayElementConcatenation()
	initArrayToArrayConcatenation()
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

func (o binOpOverload) lookupImpl(left, right types.T) (*BinOp, bool) {
	for _, fn := range o {
		casted := fn.(*BinOp)
		if casted.matchParams(left, right) {
			return casted, true
		}
	}
	return nil, false
}

// getJSONPath is used for the #> and #>> operators.
func getJSONPath(j DJSON, ary DArray) (Datum, error) {
	// TODO(justin): this is slightly annoying because we have to allocate
	// a new array since the JSON package isn't aware of DArray.
	path := make([]string, len(ary.Array))
	for i, v := range ary.Array {
		if v == DNull {
			return DNull, nil
		}
		path[i] = string(MustBeDString(v))
	}
	result, err := json.FetchPath(j.JSON, path)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return DNull, nil
	}
	return &DJSON{result}, nil
}

func newCannotMixBitArraySizesError(op string) error {
	return pgerror.NewErrorf(pgerror.CodeStringDataLengthMismatchError,
		"cannot %s bit strings of different sizes", op)
}

// BinOps contains the binary operations indexed by operation type.
var BinOps = map[BinaryOperator]binOpOverload{
	Bitand: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDInt(MustBeDInt(left) & MustBeDInt(right)), nil
			},
		},
		&BinOp{
			LeftType:   types.BitArray,
			RightType:  types.BitArray,
			ReturnType: types.BitArray,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				lhs := MustBeDBitArray(left)
				rhs := MustBeDBitArray(right)
				if lhs.BitLen() != rhs.BitLen() {
					return nil, newCannotMixBitArraySizesError("AND")
				}
				return &DBitArray{
					BitArray: bitarray.And(lhs.BitArray, rhs.BitArray),
				}, nil
			},
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
		},
		&BinOp{
			LeftType:   types.BitArray,
			RightType:  types.BitArray,
			ReturnType: types.BitArray,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				lhs := MustBeDBitArray(left)
				rhs := MustBeDBitArray(right)
				if lhs.BitLen() != rhs.BitLen() {
					return nil, newCannotMixBitArraySizesError("OR")
				}
				return &DBitArray{
					BitArray: bitarray.Or(lhs.BitArray, rhs.BitArray),
				}, nil
			},
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
		},
		&BinOp{
			LeftType:   types.BitArray,
			RightType:  types.BitArray,
			ReturnType: types.BitArray,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				lhs := MustBeDBitArray(left)
				rhs := MustBeDBitArray(right)
				if lhs.BitLen() != rhs.BitLen() {
					return nil, newCannotMixBitArraySizesError("XOR")
				}
				return &DBitArray{
					BitArray: bitarray.Xor(lhs.BitArray, rhs.BitArray),
				}, nil
			},
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
					return nil, errIntOutOfRange
				}
				return NewDInt(DInt(r)), nil
			},
		},
		&BinOp{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDFloat(*left.(*DFloat) + *right.(*DFloat)), nil
			},
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
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := MustBeDInt(right)
				dd := &DDecimal{}
				dd.SetFinite(int64(r), 0)
				_, err := ExactCtx.Add(&dd.Decimal, l, &dd.Decimal)
				return dd, err
			},
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := MustBeDInt(left)
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				dd.SetFinite(int64(l), 0)
				_, err := ExactCtx.Add(&dd.Decimal, &dd.Decimal, r)
				return dd, err
			},
		},
		&BinOp{
			LeftType:   types.Date,
			RightType:  types.Int,
			ReturnType: types.Date,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDDate(*left.(*DDate) + DDate(MustBeDInt(right))), nil
			},
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Date,
			ReturnType: types.Date,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDDate(DDate(MustBeDInt(left)) + *right.(*DDate)), nil
			},
		},
		&BinOp{
			LeftType:   types.Date,
			RightType:  types.Time,
			ReturnType: types.Timestamp,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				d := MakeDTimestampTZFromDate(time.UTC, left.(*DDate))
				t := time.Duration(*right.(*DTime)) * time.Microsecond
				return MakeDTimestamp(d.Add(t), time.Microsecond), nil
			},
		},
		&BinOp{
			LeftType:   types.Time,
			RightType:  types.Date,
			ReturnType: types.Timestamp,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				d := MakeDTimestampTZFromDate(time.UTC, right.(*DDate))
				t := time.Duration(*left.(*DTime)) * time.Microsecond
				return MakeDTimestamp(d.Add(t), time.Microsecond), nil
			},
		},
		&BinOp{
			LeftType:   types.Time,
			RightType:  types.Interval,
			ReturnType: types.Time,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				t := timeofday.TimeOfDay(*left.(*DTime))
				return MakeDTime(t.Add(right.(*DInterval).Duration)), nil
			},
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Time,
			ReturnType: types.Time,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				t := timeofday.TimeOfDay(*right.(*DTime))
				return MakeDTime(t.Add(left.(*DInterval).Duration)), nil
			},
		},
		&BinOp{
			LeftType:   types.Timestamp,
			RightType:  types.Interval,
			ReturnType: types.Timestamp,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				return MakeDTimestamp(duration.Add(ctx,
					left.(*DTimestamp).Time, right.(*DInterval).Duration), time.Microsecond), nil
			},
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Timestamp,
			ReturnType: types.Timestamp,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				return MakeDTimestamp(duration.Add(ctx,
					right.(*DTimestamp).Time, left.(*DInterval).Duration), time.Microsecond), nil
			},
		},
		&BinOp{
			LeftType:   types.TimestampTZ,
			RightType:  types.Interval,
			ReturnType: types.TimestampTZ,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				t := duration.Add(ctx, left.(*DTimestampTZ).Time, right.(*DInterval).Duration)
				return MakeDTimestampTZ(t, time.Microsecond), nil
			},
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.TimestampTZ,
			ReturnType: types.TimestampTZ,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				t := duration.Add(ctx, right.(*DTimestampTZ).Time, left.(*DInterval).Duration)
				return MakeDTimestampTZ(t, time.Microsecond), nil
			},
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Interval,
			ReturnType: types.Interval,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return &DInterval{Duration: left.(*DInterval).Duration.Add(right.(*DInterval).Duration)}, nil
			},
		},
		&BinOp{
			LeftType:   types.Date,
			RightType:  types.Interval,
			ReturnType: types.TimestampTZ,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				leftTZ := MakeDTimestampTZFromDate(ctx.GetLocation(), left.(*DDate))
				t := duration.Add(ctx, leftTZ.Time, right.(*DInterval).Duration)
				return MakeDTimestampTZ(t, time.Microsecond), nil
			},
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Date,
			ReturnType: types.TimestampTZ,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				rightTZ := MakeDTimestampTZFromDate(ctx.GetLocation(), right.(*DDate))
				t := duration.Add(ctx, rightTZ.Time, left.(*DInterval).Duration)
				return MakeDTimestampTZ(t, time.Microsecond), nil
			},
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
		},
	},

	Minus: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				a, b := MustBeDInt(left), MustBeDInt(right)
				if b < 0 && a > math.MaxInt64+b {
					return nil, errIntOutOfRange
				}
				if b > 0 && a < math.MinInt64+b {
					return nil, errIntOutOfRange
				}
				return NewDInt(a - b), nil
			},
		},
		&BinOp{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDFloat(*left.(*DFloat) - *right.(*DFloat)), nil
			},
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
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := MustBeDInt(right)
				dd := &DDecimal{}
				dd.SetFinite(int64(r), 0)
				_, err := ExactCtx.Sub(&dd.Decimal, l, &dd.Decimal)
				return dd, err
			},
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := MustBeDInt(left)
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				dd.SetFinite(int64(l), 0)
				_, err := ExactCtx.Sub(&dd.Decimal, &dd.Decimal, r)
				return dd, err
			},
		},
		&BinOp{
			LeftType:   types.Date,
			RightType:  types.Int,
			ReturnType: types.Date,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDDate(*left.(*DDate) - DDate(MustBeDInt(right))), nil
			},
		},
		&BinOp{
			LeftType:   types.Date,
			RightType:  types.Date,
			ReturnType: types.Int,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDInt(DInt(*left.(*DDate) - *right.(*DDate))), nil
			},
		},
		&BinOp{
			LeftType:   types.Date,
			RightType:  types.Time,
			ReturnType: types.Timestamp,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				d := MakeDTimestampTZFromDate(time.UTC, left.(*DDate))
				t := time.Duration(*right.(*DTime)) * time.Microsecond
				return MakeDTimestamp(d.Add(-1*t), time.Microsecond), nil
			},
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
		},
		&BinOp{
			LeftType:   types.Timestamp,
			RightType:  types.Timestamp,
			ReturnType: types.Interval,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				nanos := left.(*DTimestamp).Sub(right.(*DTimestamp).Time).Nanoseconds()
				return &DInterval{Duration: duration.Duration{Nanos: nanos}}, nil
			},
		},
		&BinOp{
			LeftType:   types.TimestampTZ,
			RightType:  types.TimestampTZ,
			ReturnType: types.Interval,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				nanos := left.(*DTimestampTZ).Sub(right.(*DTimestampTZ).Time).Nanoseconds()
				return &DInterval{Duration: duration.Duration{Nanos: nanos}}, nil
			},
		},
		&BinOp{
			LeftType:   types.Timestamp,
			RightType:  types.TimestampTZ,
			ReturnType: types.Interval,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				// These two quantities aren't directly comparable. Convert the
				// TimestampTZ to a timestamp first.
				nanos := left.(*DTimestamp).Sub(right.(*DTimestampTZ).stripTimeZone(ctx).Time).Nanoseconds()
				return &DInterval{Duration: duration.Duration{Nanos: nanos}}, nil
			},
		},
		&BinOp{
			LeftType:   types.TimestampTZ,
			RightType:  types.Timestamp,
			ReturnType: types.Interval,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				// These two quantities aren't directly comparable. Convert the
				// TimestampTZ to a timestamp first.
				nanos := left.(*DTimestampTZ).stripTimeZone(ctx).Sub(right.(*DTimestamp).Time).Nanoseconds()
				return &DInterval{Duration: duration.Duration{Nanos: nanos}}, nil
			},
		},
		&BinOp{
			LeftType:   types.Time,
			RightType:  types.Interval,
			ReturnType: types.Time,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				t := timeofday.TimeOfDay(*left.(*DTime))
				return MakeDTime(t.Add(right.(*DInterval).Duration.Mul(-1))), nil
			},
		},
		&BinOp{
			LeftType:   types.Timestamp,
			RightType:  types.Interval,
			ReturnType: types.Timestamp,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				return MakeDTimestamp(duration.Add(ctx,
					left.(*DTimestamp).Time, right.(*DInterval).Duration.Mul(-1)), time.Microsecond), nil
			},
		},
		&BinOp{
			LeftType:   types.TimestampTZ,
			RightType:  types.Interval,
			ReturnType: types.TimestampTZ,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				t := duration.Add(ctx,
					left.(*DTimestampTZ).Time, right.(*DInterval).Duration.Mul(-1))
				return MakeDTimestampTZ(t, time.Microsecond), nil
			},
		},
		&BinOp{
			LeftType:   types.Date,
			RightType:  types.Interval,
			ReturnType: types.TimestampTZ,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				leftTZ := MakeDTimestampTZFromDate(ctx.GetLocation(), left.(*DDate))
				t := duration.Add(ctx,
					leftTZ.Time, right.(*DInterval).Duration.Mul(-1))
				return MakeDTimestampTZ(t, time.Microsecond), nil
			},
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Interval,
			ReturnType: types.Interval,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return &DInterval{Duration: left.(*DInterval).Duration.Sub(right.(*DInterval).Duration)}, nil
			},
		},
		&BinOp{
			LeftType:   types.JSON,
			RightType:  types.String,
			ReturnType: types.JSON,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				j, _, err := left.(*DJSON).JSON.RemoveString(string(MustBeDString(right)))
				if err != nil {
					return nil, err
				}
				return &DJSON{j}, nil
			},
		},
		&BinOp{
			LeftType:   types.JSON,
			RightType:  types.Int,
			ReturnType: types.JSON,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				j, _, err := left.(*DJSON).JSON.RemoveIndex(int(MustBeDInt(right)))
				if err != nil {
					return nil, err
				}
				return &DJSON{j}, nil
			},
		},
		&BinOp{
			LeftType:   types.JSON,
			RightType:  types.TArray{Typ: types.String},
			ReturnType: types.JSON,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				j := left.(*DJSON).JSON
				arr := *MustBeDArray(right)

				for _, str := range arr.Array {
					var err error
					j, _, err = j.RemoveString(string(MustBeDString(str)))
					if err != nil {
						return nil, err
					}
				}
				return &DJSON{j}, nil
			},
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
					return nil, errIntOutOfRange
				} else if c/b != a {
					return nil, errIntOutOfRange
				}
				return NewDInt(c), nil
			},
		},
		&BinOp{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDFloat(*left.(*DFloat) * *right.(*DFloat)), nil
			},
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
				dd.SetFinite(int64(r), 0)
				_, err := ExactCtx.Mul(&dd.Decimal, l, &dd.Decimal)
				return dd, err
			},
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := MustBeDInt(left)
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				dd.SetFinite(int64(l), 0)
				_, err := ExactCtx.Mul(&dd.Decimal, &dd.Decimal, r)
				return dd, err
			},
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Interval,
			ReturnType: types.Interval,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return &DInterval{Duration: right.(*DInterval).Duration.Mul(int64(MustBeDInt(left)))}, nil
			},
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Int,
			ReturnType: types.Interval,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return &DInterval{Duration: left.(*DInterval).Duration.Mul(int64(MustBeDInt(right)))}, nil
			},
		},
		&BinOp{
			LeftType:   types.Interval,
			RightType:  types.Float,
			ReturnType: types.Interval,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				r := float64(*right.(*DFloat))
				return &DInterval{Duration: left.(*DInterval).Duration.MulFloat(r)}, nil
			},
		},
		&BinOp{
			LeftType:   types.Float,
			RightType:  types.Interval,
			ReturnType: types.Interval,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := float64(*left.(*DFloat))
				return &DInterval{Duration: right.(*DInterval).Duration.MulFloat(l)}, nil
			},
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
		},
	},

	Div: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				rInt := MustBeDInt(right)
				div := ctx.getTmpDec().SetFinite(int64(rInt), 0)
				dd := &DDecimal{}
				dd.SetFinite(int64(MustBeDInt(left)), 0)
				cond, err := DecimalCtx.Quo(&dd.Decimal, &dd.Decimal, div)
				if cond.DivisionByZero() {
					return dd, ErrDivByZero
				}
				return dd, err
			},
		},
		&BinOp{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDFloat(*left.(*DFloat) / *right.(*DFloat)), nil
			},
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				cond, err := DecimalCtx.Quo(&dd.Decimal, l, r)
				if cond.DivisionByZero() {
					return dd, ErrDivByZero
				}
				return dd, err
			},
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := MustBeDInt(right)
				dd := &DDecimal{}
				dd.SetFinite(int64(r), 0)
				cond, err := DecimalCtx.Quo(&dd.Decimal, l, &dd.Decimal)
				if cond.DivisionByZero() {
					return dd, ErrDivByZero
				}
				return dd, err
			},
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := MustBeDInt(left)
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				dd.SetFinite(int64(l), 0)
				cond, err := DecimalCtx.Quo(&dd.Decimal, &dd.Decimal, r)
				if cond.DivisionByZero() {
					return dd, ErrDivByZero
				}
				return dd, err
			},
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
		},
		&BinOp{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := float64(*left.(*DFloat))
				r := float64(*right.(*DFloat))
				return NewDFloat(DFloat(math.Trunc(l / r))), nil
			},
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				_, err := HighPrecisionCtx.QuoInteger(&dd.Decimal, l, r)
				return dd, err
			},
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
				dd.SetFinite(int64(r), 0)
				_, err := HighPrecisionCtx.QuoInteger(&dd.Decimal, l, &dd.Decimal)
				return dd, err
			},
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := MustBeDInt(left)
				r := &right.(*DDecimal).Decimal
				if r.Sign() == 0 {
					return nil, ErrDivByZero
				}
				dd := &DDecimal{}
				dd.SetFinite(int64(l), 0)
				_, err := HighPrecisionCtx.QuoInteger(&dd.Decimal, &dd.Decimal, r)
				return dd, err
			},
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
					return nil, ErrZeroModulus
				}
				return NewDInt(MustBeDInt(left) % r), nil
			},
		},
		&BinOp{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDFloat(DFloat(math.Mod(float64(*left.(*DFloat)), float64(*right.(*DFloat))))), nil
			},
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				_, err := HighPrecisionCtx.Rem(&dd.Decimal, l, r)
				return dd, err
			},
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := MustBeDInt(right)
				dd := &DDecimal{}
				dd.SetFinite(int64(r), 0)
				_, err := HighPrecisionCtx.Rem(&dd.Decimal, l, &dd.Decimal)
				return dd, err
			},
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := MustBeDInt(left)
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				dd.SetFinite(int64(l), 0)
				_, err := HighPrecisionCtx.Rem(&dd.Decimal, &dd.Decimal, r)
				return dd, err
			},
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
		},
		&BinOp{
			LeftType:   types.Bytes,
			RightType:  types.Bytes,
			ReturnType: types.Bytes,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDBytes(*left.(*DBytes) + *right.(*DBytes)), nil
			},
		},
		&BinOp{
			LeftType:   types.BitArray,
			RightType:  types.BitArray,
			ReturnType: types.BitArray,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				lhs := MustBeDBitArray(left)
				rhs := MustBeDBitArray(right)
				return &DBitArray{
					BitArray: bitarray.Concat(lhs.BitArray, rhs.BitArray),
				}, nil
			},
		},
		&BinOp{
			LeftType:   types.JSON,
			RightType:  types.JSON,
			ReturnType: types.JSON,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				j, err := MustBeDJSON(left).JSON.Concat(MustBeDJSON(right).JSON)
				if err != nil {
					return nil, err
				}
				return &DJSON{j}, nil
			},
		},
	},

	// TODO(pmattis): Check that the shift is valid.
	LShift: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDInt(MustBeDInt(left) << uint(MustBeDInt(right))), nil
			},
		},
		&BinOp{
			LeftType:   types.BitArray,
			RightType:  types.Int,
			ReturnType: types.BitArray,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				lhs := MustBeDBitArray(left)
				rhs := MustBeDInt(right)
				return &DBitArray{
					BitArray: lhs.BitArray.LeftShiftAny(int64(rhs)),
				}, nil
			},
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
		},
	},

	RShift: {
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Int,
			ReturnType: types.Int,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDInt(MustBeDInt(left) >> uint(MustBeDInt(right))), nil
			},
		},
		&BinOp{
			LeftType:   types.BitArray,
			RightType:  types.Int,
			ReturnType: types.BitArray,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				lhs := MustBeDBitArray(left)
				rhs := MustBeDInt(right)
				return &DBitArray{
					BitArray: lhs.BitArray.LeftShiftAny(-int64(rhs)),
				}, nil
			},
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
		},
		&BinOp{
			LeftType:   types.Float,
			RightType:  types.Float,
			ReturnType: types.Float,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				f := math.Pow(float64(*left.(*DFloat)), float64(*right.(*DFloat)))
				return NewDFloat(DFloat(f)), nil
			},
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
		},
		&BinOp{
			LeftType:   types.Decimal,
			RightType:  types.Int,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := MustBeDInt(right)
				dd := &DDecimal{}
				dd.SetFinite(int64(r), 0)
				_, err := DecimalCtx.Pow(&dd.Decimal, l, &dd.Decimal)
				return dd, err
			},
		},
		&BinOp{
			LeftType:   types.Int,
			RightType:  types.Decimal,
			ReturnType: types.Decimal,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := MustBeDInt(left)
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				dd.SetFinite(int64(l), 0)
				_, err := DecimalCtx.Pow(&dd.Decimal, &dd.Decimal, r)
				return dd, err
			},
		},
	},

	JSONFetchVal: {
		&BinOp{
			LeftType:   types.JSON,
			RightType:  types.String,
			ReturnType: types.JSON,
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
		},
		&BinOp{
			LeftType:   types.JSON,
			RightType:  types.Int,
			ReturnType: types.JSON,
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
		},
	},

	JSONFetchValPath: {
		&BinOp{
			LeftType:   types.JSON,
			RightType:  types.TArray{Typ: types.String},
			ReturnType: types.JSON,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return getJSONPath(*left.(*DJSON), *MustBeDArray(right))
			},
		},
	},

	JSONFetchText: {
		&BinOp{
			LeftType:   types.JSON,
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
		},
		&BinOp{
			LeftType:   types.JSON,
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
		},
	},

	JSONFetchTextPath: {
		&BinOp{
			LeftType:   types.JSON,
			RightType:  types.TArray{Typ: types.String},
			ReturnType: types.String,
			Fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				res, err := getJSONPath(*left.(*DJSON), *MustBeDArray(right))
				if err != nil {
					return nil, err
				}
				if res == DNull {
					return DNull, nil
				}
				text, err := res.(*DJSON).JSON.AsText()
				if err != nil {
					return nil, err
				}
				if text == nil {
					return DNull, nil
				}
				return NewDString(*text), nil
			},
		},
	},
}

// timestampMinusBinOp is the implementation of the subtraction
// between types.TimestampTZ operands.
var timestampMinusBinOp *BinOp

// TimestampDifference computes the interval difference between two
// TimestampTZ datums. The result is a DInterval. The caller must
// ensure that the arguments are of the proper Datum type.
func TimestampDifference(ctx *EvalContext, start, end Datum) (Datum, error) {
	return timestampMinusBinOp.Fn(ctx, start, end)
}

func init() {
	timestampMinusBinOp, _ = BinOps[Minus].lookupImpl(types.TimestampTZ, types.TimestampTZ)
}

// CmpOp is a comparison operator.
type CmpOp struct {
	LeftType  types.T
	RightType types.T

	// If NullableArgs is false, the operator returns NULL
	// whenever either argument is NULL.
	NullableArgs bool

	// Datum return type is a union between *DBool and dNull.
	Fn func(*EvalContext, Datum, Datum) (Datum, error)

	types       TypeList
	isPreferred bool
}

func (op *CmpOp) params() TypeList {
	return op.types
}

func (op *CmpOp) matchParams(l, r types.T) bool {
	return op.params().MatchAt(l, 0) && op.params().MatchAt(r, 1)
}

var cmpOpReturnType = FixedReturnType(types.Bool)

func (op *CmpOp) returnType() ReturnTyper {
	return cmpOpReturnType
}

func (op *CmpOp) preferred() bool {
	return op.isPreferred
}

func init() {
	// Array equality comparisons.
	for _, t := range types.AnyNonArray {
		CmpOps[EQ] = append(CmpOps[EQ], &CmpOp{
			LeftType:  types.TArray{Typ: t},
			RightType: types.TArray{Typ: t},
			Fn:        cmpOpScalarEQFn,
		})

		CmpOps[IsNotDistinctFrom] = append(CmpOps[IsNotDistinctFrom], &CmpOp{
			LeftType:     types.TArray{Typ: t},
			RightType:    types.TArray{Typ: t},
			Fn:           cmpOpScalarIsFn,
			NullableArgs: true,
		})
	}
}

func init() {
	for op, overload := range CmpOps {
		for i, impl := range overload {
			casted := impl.(*CmpOp)
			casted.types = ArgTypes{{"left", casted.LeftType}, {"right", casted.RightType}}
			CmpOps[op][i] = casted
		}
	}
}

// cmpOpOverload is an overloaded set of comparison operator implementations.
type cmpOpOverload []overloadImpl

func (o cmpOpOverload) lookupImpl(left, right types.T) (*CmpOp, bool) {
	for _, fn := range o {
		casted := fn.(*CmpOp)
		if casted.matchParams(left, right) {
			return casted, true
		}
	}
	return nil, false
}

func makeCmpOpOverload(
	fn func(ctx *EvalContext, left, right Datum) (Datum, error), a, b types.T, nullableArgs bool,
) *CmpOp {
	return &CmpOp{
		LeftType:     a,
		RightType:    b,
		Fn:           fn,
		NullableArgs: nullableArgs,
	}
}

func makeEqFn(a, b types.T) *CmpOp {
	return makeCmpOpOverload(cmpOpScalarEQFn, a, b, false /* NullableArgs */)
}
func makeLtFn(a, b types.T) *CmpOp {
	return makeCmpOpOverload(cmpOpScalarLTFn, a, b, false /* NullableArgs */)
}
func makeLeFn(a, b types.T) *CmpOp {
	return makeCmpOpOverload(cmpOpScalarLEFn, a, b, false /* NullableArgs */)
}
func makeIsFn(a, b types.T) *CmpOp {
	return makeCmpOpOverload(cmpOpScalarIsFn, a, b, true /* NullableArgs */)
}

// CmpOps contains the comparison operations indexed by operation type.
var CmpOps = map[ComparisonOperator]cmpOpOverload{
	EQ: {
		// Single-type comparisons.
		makeEqFn(types.Bool, types.Bool),
		makeEqFn(types.Bytes, types.Bytes),
		makeEqFn(types.Date, types.Date),
		makeEqFn(types.Decimal, types.Decimal),
		makeEqFn(types.FamCollatedString, types.FamCollatedString),
		makeEqFn(types.Float, types.Float),
		makeEqFn(types.INet, types.INet),
		makeEqFn(types.Int, types.Int),
		makeEqFn(types.Interval, types.Interval),
		makeEqFn(types.JSON, types.JSON),
		makeEqFn(types.Oid, types.Oid),
		makeEqFn(types.String, types.String),
		makeEqFn(types.Time, types.Time),
		makeEqFn(types.Timestamp, types.Timestamp),
		makeEqFn(types.TimestampTZ, types.TimestampTZ),
		makeEqFn(types.UUID, types.UUID),
		makeEqFn(types.BitArray, types.BitArray),

		// Mixed-type comparisons.
		makeEqFn(types.Date, types.Timestamp),
		makeEqFn(types.Date, types.TimestampTZ),
		makeEqFn(types.Decimal, types.Float),
		makeEqFn(types.Decimal, types.Int),
		makeEqFn(types.Float, types.Decimal),
		makeEqFn(types.Float, types.Int),
		makeEqFn(types.Int, types.Decimal),
		makeEqFn(types.Int, types.Float),
		makeEqFn(types.Timestamp, types.Date),
		makeEqFn(types.Timestamp, types.TimestampTZ),
		makeEqFn(types.TimestampTZ, types.Date),
		makeEqFn(types.TimestampTZ, types.Timestamp),

		// Tuple comparison.
		&CmpOp{
			LeftType:  types.FamTuple,
			RightType: types.FamTuple,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				return cmpOpTupleFn(ctx, *left.(*DTuple), *right.(*DTuple), EQ), nil
			},
		},
	},

	LT: {
		// Single-type comparisons.
		makeLtFn(types.Bool, types.Bool),
		makeLtFn(types.Bytes, types.Bytes),
		makeLtFn(types.Date, types.Date),
		makeLtFn(types.Decimal, types.Decimal),
		makeLtFn(types.FamCollatedString, types.FamCollatedString),
		makeLtFn(types.Float, types.Float),
		makeLtFn(types.INet, types.INet),
		makeLtFn(types.Int, types.Int),
		makeLtFn(types.Interval, types.Interval),
		makeLtFn(types.Oid, types.Oid),
		makeLtFn(types.String, types.String),
		makeLtFn(types.Time, types.Time),
		makeLtFn(types.Timestamp, types.Timestamp),
		makeLtFn(types.TimestampTZ, types.TimestampTZ),
		makeLtFn(types.UUID, types.UUID),
		makeLtFn(types.BitArray, types.BitArray),

		// Mixed-type comparisons.
		makeLtFn(types.Date, types.Timestamp),
		makeLtFn(types.Date, types.TimestampTZ),
		makeLtFn(types.Decimal, types.Float),
		makeLtFn(types.Decimal, types.Int),
		makeLtFn(types.Float, types.Decimal),
		makeLtFn(types.Float, types.Int),
		makeLtFn(types.Int, types.Decimal),
		makeLtFn(types.Int, types.Float),
		makeLtFn(types.Timestamp, types.Date),
		makeLtFn(types.Timestamp, types.TimestampTZ),
		makeLtFn(types.TimestampTZ, types.Date),
		makeLtFn(types.TimestampTZ, types.Timestamp),

		// Tuple comparison.
		&CmpOp{
			LeftType:  types.FamTuple,
			RightType: types.FamTuple,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				return cmpOpTupleFn(ctx, *left.(*DTuple), *right.(*DTuple), LT), nil
			},
		},
	},

	LE: {
		// Single-type comparisons.
		makeLeFn(types.Bool, types.Bool),
		makeLeFn(types.Bytes, types.Bytes),
		makeLeFn(types.Date, types.Date),
		makeLeFn(types.Decimal, types.Decimal),
		makeLeFn(types.FamCollatedString, types.FamCollatedString),
		makeLeFn(types.Float, types.Float),
		makeLeFn(types.INet, types.INet),
		makeLeFn(types.Int, types.Int),
		makeLeFn(types.Interval, types.Interval),
		makeLeFn(types.Oid, types.Oid),
		makeLeFn(types.String, types.String),
		makeLeFn(types.Time, types.Time),
		makeLeFn(types.Timestamp, types.Timestamp),
		makeLeFn(types.TimestampTZ, types.TimestampTZ),
		makeLeFn(types.UUID, types.UUID),
		makeLeFn(types.BitArray, types.BitArray),

		// Mixed-type comparisons.
		makeLeFn(types.Date, types.Timestamp),
		makeLeFn(types.Date, types.TimestampTZ),
		makeLeFn(types.Decimal, types.Float),
		makeLeFn(types.Decimal, types.Int),
		makeLeFn(types.Float, types.Decimal),
		makeLeFn(types.Float, types.Int),
		makeLeFn(types.Int, types.Decimal),
		makeLeFn(types.Int, types.Float),
		makeLeFn(types.Timestamp, types.Date),
		makeLeFn(types.Timestamp, types.TimestampTZ),
		makeLeFn(types.TimestampTZ, types.Date),
		makeLeFn(types.TimestampTZ, types.Timestamp),

		// Tuple comparison.
		&CmpOp{
			LeftType:  types.FamTuple,
			RightType: types.FamTuple,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				return cmpOpTupleFn(ctx, *left.(*DTuple), *right.(*DTuple), LE), nil
			},
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
		},
		// Single-type comparisons.
		makeIsFn(types.Bool, types.Bool),
		makeIsFn(types.Bytes, types.Bytes),
		makeIsFn(types.Date, types.Date),
		makeIsFn(types.Decimal, types.Decimal),
		makeIsFn(types.FamCollatedString, types.FamCollatedString),
		makeIsFn(types.Float, types.Float),
		makeIsFn(types.INet, types.INet),
		makeIsFn(types.Int, types.Int),
		makeIsFn(types.Interval, types.Interval),
		makeIsFn(types.JSON, types.JSON),
		makeIsFn(types.Oid, types.Oid),
		makeIsFn(types.String, types.String),
		makeIsFn(types.Time, types.Time),
		makeIsFn(types.Timestamp, types.Timestamp),
		makeIsFn(types.TimestampTZ, types.TimestampTZ),
		makeIsFn(types.UUID, types.UUID),
		makeIsFn(types.BitArray, types.BitArray),

		// Mixed-type comparisons.
		makeIsFn(types.Date, types.Timestamp),
		makeIsFn(types.Date, types.TimestampTZ),
		makeIsFn(types.Decimal, types.Float),
		makeIsFn(types.Decimal, types.Int),
		makeIsFn(types.Float, types.Decimal),
		makeIsFn(types.Float, types.Int),
		makeIsFn(types.Int, types.Decimal),
		makeIsFn(types.Int, types.Float),
		makeIsFn(types.Timestamp, types.Date),
		makeIsFn(types.Timestamp, types.TimestampTZ),
		makeIsFn(types.TimestampTZ, types.Date),
		makeIsFn(types.TimestampTZ, types.Timestamp),

		// Tuple comparison.
		&CmpOp{
			LeftType:     types.FamTuple,
			RightType:    types.FamTuple,
			NullableArgs: true,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				if left == DNull || right == DNull {
					return MakeDBool(left == DNull && right == DNull), nil
				}
				return cmpOpTupleFn(ctx, *left.(*DTuple), *right.(*DTuple), IsNotDistinctFrom), nil
			},
		},
	},

	In: {
		makeEvalTupleIn(types.Bool),
		makeEvalTupleIn(types.Bytes),
		makeEvalTupleIn(types.Date),
		makeEvalTupleIn(types.Decimal),
		makeEvalTupleIn(types.FamCollatedString),
		makeEvalTupleIn(types.FamTuple),
		makeEvalTupleIn(types.Float),
		makeEvalTupleIn(types.INet),
		makeEvalTupleIn(types.Int),
		makeEvalTupleIn(types.Interval),
		makeEvalTupleIn(types.JSON),
		makeEvalTupleIn(types.Oid),
		makeEvalTupleIn(types.String),
		makeEvalTupleIn(types.Time),
		makeEvalTupleIn(types.Timestamp),
		makeEvalTupleIn(types.TimestampTZ),
		makeEvalTupleIn(types.UUID),
		makeEvalTupleIn(types.BitArray),
	},

	Like: {
		&CmpOp{
			LeftType:  types.String,
			RightType: types.String,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				return matchLike(ctx, left, right, false)
			},
		},
	},

	ILike: {
		&CmpOp{
			LeftType:  types.String,
			RightType: types.String,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				return matchLike(ctx, left, right, true)
			},
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
		},
	},

	RegMatch: {
		&CmpOp{
			LeftType:  types.String,
			RightType: types.String,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				key := regexpKey{s: string(MustBeDString(right)), caseInsensitive: false}
				return matchRegexpWithKey(ctx, left, key)
			},
		},
	},

	RegIMatch: {
		&CmpOp{
			LeftType:  types.String,
			RightType: types.String,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				key := regexpKey{s: string(MustBeDString(right)), caseInsensitive: true}
				return matchRegexpWithKey(ctx, left, key)
			},
		},
	},

	JSONExists: {
		&CmpOp{
			LeftType:  types.JSON,
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
		},
	},

	JSONSomeExists: {
		&CmpOp{
			LeftType:  types.JSON,
			RightType: types.TArray{Typ: types.String},
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
					if e {
						return DBoolTrue, nil
					}
				}
				return DBoolFalse, nil
			},
		},
	},

	JSONAllExists: {
		&CmpOp{
			LeftType:  types.JSON,
			RightType: types.TArray{Typ: types.String},
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
		},
	},

	Contains: {
		&CmpOp{
			LeftType:  types.JSON,
			RightType: types.JSON,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				c, err := json.Contains(left.(*DJSON).JSON, right.(*DJSON).JSON)
				if err != nil {
					return nil, err
				}
				return MakeDBool(DBool(c)), nil
			},
		},
	},

	ContainedBy: {
		&CmpOp{
			LeftType:  types.JSON,
			RightType: types.JSON,
			Fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				c, err := json.Contains(right.(*DJSON).JSON, left.(*DJSON).JSON)
				if err != nil {
					return nil, err
				}
				return MakeDBool(DBool(c)), nil
			},
		},
	},
}

// This map contains the inverses for operators in the CmpOps map that have
// inverses.
var cmpOpsInverse map[ComparisonOperator]ComparisonOperator

func init() {
	cmpOpsInverse = make(map[ComparisonOperator]ComparisonOperator)
	for cmpOpIdx := range comparisonOpName {
		cmpOp := ComparisonOperator(cmpOpIdx)
		newOp, _, _, _, _ := foldComparisonExpr(cmpOp, DNull, DNull)
		if newOp != cmpOp {
			cmpOpsInverse[newOp] = cmpOp
			cmpOpsInverse[cmpOp] = newOp
		}
	}
}

func boolFromCmp(cmp int, op ComparisonOperator) *DBool {
	switch op {
	case EQ, IsNotDistinctFrom:
		return MakeDBool(cmp == 0)
	case LT:
		return MakeDBool(cmp < 0)
	case LE:
		return MakeDBool(cmp <= 0)
	default:
		panic(fmt.Sprintf("unexpected ComparisonOperator in boolFromCmp: %v", op))
	}
}

func cmpOpScalarFn(ctx *EvalContext, left, right Datum, op ComparisonOperator) Datum {
	// Before deferring to the Datum.Compare method, check for values that should
	// be handled differently during SQL comparison evaluation than they should when
	// ordering Datum values.
	if left == DNull || right == DNull {
		switch op {
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
	return cmpOpScalarFn(ctx, left, right, EQ), nil
}
func cmpOpScalarLTFn(ctx *EvalContext, left, right Datum) (Datum, error) {
	return cmpOpScalarFn(ctx, left, right, LT), nil
}
func cmpOpScalarLEFn(ctx *EvalContext, left, right Datum) (Datum, error) {
	return cmpOpScalarFn(ctx, left, right, LE), nil
}
func cmpOpScalarIsFn(ctx *EvalContext, left, right Datum) (Datum, error) {
	return cmpOpScalarFn(ctx, left, right, IsNotDistinctFrom), nil
}

func cmpOpTupleFn(ctx *EvalContext, left, right DTuple, op ComparisonOperator) Datum {
	cmp := 0
	sawNull := false
	for i, leftElem := range left.D {
		rightElem := right.D[i]
		// Like with cmpOpScalarFn, check for values that need to be handled
		// differently than when ordering Datums.
		if leftElem == DNull || rightElem == DNull {
			switch op {
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

func makeEvalTupleIn(typ types.T) *CmpOp {
	return &CmpOp{
		LeftType:  typ,
		RightType: types.FamTuple,
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
					// Use the EQ function which properly handles NULLs.
					if res := cmpOpTupleFn(ctx, *argTuple, *val.(*DTuple), EQ); res == DNull {
						sawNull = true
					} else if res == DBoolTrue {
						return DBoolTrue, nil
					}
				}
			}
			if sawNull {
				return DNull, nil
			}
			return DBoolFalse, nil
		},
		NullableArgs: true,
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
	all := op == All
	any := !all
	sawNull := false
	for _, elem := range right {
		if elem == DNull {
			sawNull = true
			continue
		}

		_, newLeft, newRight, _, not := foldComparisonExpr(subOp, left, elem)
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
			return DBoolFalse, pgerror.NewErrorf(pgerror.CodeInvalidEscapeSequenceError, "invalid escape string")
		}
	}

	like, err := optimizedLikeFunc(pattern, caseInsensitive, escapeRune)
	if err != nil {
		return DBoolFalse, pgerror.NewErrorf(
			pgerror.CodeInvalidRegularExpressionError, "LIKE regexp compilation failed: %v", err)
	}

	if like == nil {
		key := likeKey{s: pattern, caseInsensitive: caseInsensitive, escape: escapeRune}
		re, err := ctx.ReCache.GetRegexp(key)
		if err != nil {
			return DBoolFalse, pgerror.NewErrorf(
				pgerror.CodeInvalidRegularExpressionError, "LIKE regexp compilation failed: %v", err)
		}
		like = re.MatchString
	}
	return MakeDBool(DBool(like(unescaped))), nil
}

func matchLike(ctx *EvalContext, left, right Datum, caseInsensitive bool) (Datum, error) {
	pattern := string(MustBeDString(right))
	like, err := optimizedLikeFunc(pattern, caseInsensitive, '\\')
	if err != nil {
		return DBoolFalse, pgerror.NewErrorf(
			pgerror.CodeInvalidRegularExpressionError, "LIKE regexp compilation failed: %v", err)
	}

	if like == nil {
		key := likeKey{s: pattern, caseInsensitive: caseInsensitive, escape: '\\'}
		re, err := ctx.ReCache.GetRegexp(key)
		if err != nil {
			return DBoolFalse, pgerror.NewErrorf(
				pgerror.CodeInvalidRegularExpressionError, "LIKE regexp compilation failed: %v", err)
		}
		like = re.MatchString
	}
	return MakeDBool(DBool(like(string(MustBeDString(left))))), nil
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

// EvalDatabase consists of functions that reference the session database
// and is to be used from EvalContext.
type EvalDatabase interface {
	// ParseQualifiedTableName parses a SQL string of the form
	// `[ database_name . ] [ schema_name . ] table_name`.
	ParseQualifiedTableName(ctx context.Context, sql string) (*TableName, error)

	// ResolveTableName expands the given table name and
	// makes it point to a valid object.
	// If the database name is not given, it uses the search path to find it, and
	// sets it on the returned TableName.
	// It returns an error if the table doesn't exist.
	ResolveTableName(ctx context.Context, tn *TableName) error

	// LookupSchema looks up the schema with the given name in the given
	// database.
	LookupSchema(ctx context.Context, dbName, scName string) (found bool, scMeta SchemaMeta, err error)
}

// EvalPlanner is a limited planner that can be used from EvalContext.
type EvalPlanner interface {
	EvalDatabase
	// ParseType parses a column type.
	ParseType(sql string) (coltypes.CastTargetType, error)

	// EvalSubquery returns the Datum for the given subquery node.
	EvalSubquery(expr *Subquery) (Datum, error)
}

// SessionBoundInternalExecutor is a subset of sqlutil.InternalExecutor used by
// this sem/tree package which can't even import sqlutil. Executor used through
// this interface are always "session-bound" - they inherit session variables
// from a parent session.
type SessionBoundInternalExecutor interface {
	// QueryRow is part of the sqlutil.InternalExecutor interface.
	QueryRow(
		ctx context.Context, opName string, txn *client.Txn, stmt string, qargs ...interface{},
	) (Datums, error)
}

// SequenceOperators is used for various sql related functions that can
// be used from EvalContext.
type SequenceOperators interface {
	EvalDatabase
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
}

var _ base.ModuleTestingKnobs = &EvalContextTestingKnobs{}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*EvalContextTestingKnobs) ModuleTestingKnobs() {}

// EvalContext defines the context in which to evaluate an expression, allowing
// the retrieval of state such as the node ID or statement start time.
//
// ATTENTION: Some fields from this struct (particularly, but not exclusively,
// from SessionData) are also represented in distsqlrun.EvalContext. Whenever
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

	Settings  *cluster.Settings
	ClusterID uuid.UUID
	NodeID    roachpb.NodeID
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
	InternalExecutor SessionBoundInternalExecutor

	Planner EvalPlanner

	Sequence SequenceOperators

	// Ths transaction in which the statement is executing.
	Txn *client.Txn

	ReCache *RegexpCache
	tmpDec  apd.Decimal

	// TODO(mjibson): remove prepareOnly in favor of a 2-step prepare-exec solution
	// that is also able to save the plan to skip work during the exec step.
	PrepareOnly bool

	// SkipNormalize indicates whether expressions should be normalized
	// (false) or not (true).  It is set to true conditionally by
	// EXPLAIN(TYPES[, NORMALIZE]).
	SkipNormalize bool

	collationEnv CollationEnvironment

	TestingKnobs EvalContextTestingKnobs

	Mon *mon.BytesMonitor

	// ActiveMemAcc is the account to which values are allocated during
	// evaluation. It can change over the course of evaluation, such as on a
	// per-row basis.
	ActiveMemAcc *mon.BoundAccount
}

// MakeTestingEvalContext returns an EvalContext that includes a MemoryMonitor.
func MakeTestingEvalContext(st *cluster.Settings) EvalContext {
	monitor := mon.MakeMonitor(
		"test-monitor",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	return MakeTestingEvalContextWithMon(st, &monitor)
}

// MakeTestingEvalContextWithMon returns an EvalContext with the given
// MemoryMonitor. Ownership of the memory monitor is transferred to the
// EvalContext so do not start or close the memory monitor.
func MakeTestingEvalContextWithMon(st *cluster.Settings, monitor *mon.BytesMonitor) EvalContext {
	ctx := EvalContext{
		Txn:         &client.Txn{},
		SessionData: &sessiondata.SessionData{},
		Settings:    st,
	}
	monitor.Start(context.Background(), nil /* pool */, mon.MakeStandaloneBudget(math.MaxInt64))
	ctx.Mon = monitor
	ctx.Context = context.TODO()
	acc := monitor.MakeBoundAccount()
	ctx.ActiveMemAcc = &acc
	now := timeutil.Now()
	ctx.SetTxnTimestamp(now)
	ctx.SetStmtTimestamp(now)
	return ctx
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

// GetStmtTimestamp retrieves the current statement timestamp as per
// the evaluation context. The timestamp is guaranteed to be nonzero.
func (ctx *EvalContext) GetStmtTimestamp() time.Time {
	// TODO(knz): a zero timestamp should never be read, even during
	// Prepare. This will need to be addressed.
	if !ctx.PrepareOnly && ctx.StmtTimestamp.IsZero() {
		panic("zero statement timestamp in EvalContext")
	}
	return ctx.StmtTimestamp
}

// GetClusterTimestamp retrieves the current cluster timestamp as per
// the evaluation context. The timestamp is guaranteed to be nonzero.
func (ctx *EvalContext) GetClusterTimestamp() *DDecimal {
	ts := ctx.Txn.CommitTimestamp()
	if ts == (hlc.Timestamp{}) {
		panic("zero cluster timestamp in txn")
	}
	return TimestampToDecimal(ts)
}

// HasPlaceholders returns true if this EvalContext's placeholders have been
// assigned. Will be false during Prepare.
func (ctx *EvalContext) HasPlaceholders() bool {
	return ctx.Placeholders != nil
}

// TimestampToDecimal converts the logical timestamp into a decimal
// value with the number of nanoseconds in the integer part and the
// logical counter in the decimal part.
func TimestampToDecimal(ts hlc.Timestamp) *DDecimal {
	// Compute Walltime * 10^10 + Logical.
	// We need 10 decimals for the Logical field because its maximum
	// value is 4294967295 (2^32-1), a value with 10 decimal digits.
	var res DDecimal
	val := &res.Coeff
	val.SetInt64(ts.WallTime)
	val.Mul(val, big10E10)
	val.Add(val, big.NewInt(int64(ts.Logical)))

	// Shift 10 decimals to the right, so that the logical
	// field appears as fractional part.
	res.Decimal.Exponent = -10
	return &res
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
		panic("zero transaction timestamp in EvalContext")
	}
	return MakeDTimestampTZ(ctx.TxnTimestamp, precision)
}

// GetTxnTimestampNoZone retrieves the current transaction timestamp as per
// the evaluation context. The timestamp is guaranteed to be nonzero.
func (ctx *EvalContext) GetTxnTimestampNoZone(precision time.Duration) *DTimestamp {
	// TODO(knz): a zero timestamp should never be read, even during
	// Prepare. This will need to be addressed.
	if !ctx.PrepareOnly && ctx.TxnTimestamp.IsZero() {
		panic("zero transaction timestamp in EvalContext")
	}
	return MakeDTimestamp(ctx.TxnTimestamp, precision)
}

// SetTxnTimestamp sets the corresponding timestamp in the EvalContext.
func (ctx *EvalContext) SetTxnTimestamp(ts time.Time) {
	ctx.TxnTimestamp = ts
}

// SetStmtTimestamp sets the corresponding timestamp in the EvalContext.
func (ctx *EvalContext) SetStmtTimestamp(ts time.Time) {
	ctx.StmtTimestamp = ts
}

// GetAdditionMode implements duration.Context.
func (ctx *EvalContext) GetAdditionMode() duration.AdditionMode {
	if ctx == nil {
		return duration.AdditionModeCompatible
	}
	return ctx.SessionData.DurationAdditionMode
}

// GetLocation returns the session timezone.
func (ctx *EvalContext) GetLocation() *time.Location {
	if ctx.SessionData.DataConversion.Location == nil {
		return time.UTC
	}
	return ctx.SessionData.DataConversion.Location
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
	if left == DNull && !expr.fn.NullableArgs {
		return DNull, nil
	}
	right, err := expr.Right.(TypedExpr).Eval(ctx)
	if err != nil {
		return nil, err
	}
	if right == DNull && !expr.fn.NullableArgs {
		return DNull, nil
	}
	res, err := expr.fn.Fn(ctx, left, right)
	if err != nil {
		return nil, err
	}
	if ctx.TestingKnobs.AssertBinaryExprReturnTypes {
		if err := ensureExpectedType(expr.fn.ReturnType, res); err != nil {
			return nil, errors.Wrapf(err, "binary op %q", expr.String())
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
			d, err := evalComparison(ctx, EQ, val, arg)
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
var pgSignatureRegexp = regexp.MustCompile(`^\s*([\w\.]+)\s*\((?:(?:\s*\w+\s*,)*\s*\w+)?\s*\)\s*$`)

// regTypeInfo contains details on a pg_catalog table that has a reg* type.
type regTypeInfo struct {
	tableName string
	// nameCol is the name of the column that contains the table's entity name.
	nameCol string
	// objName is a human-readable name describing the objects in the table.
	objName string
	// errType is the pg error code in case the object does not exist.
	errType string
}

// regTypeInfos maps an coltypes.TOid to a regTypeInfo that describes the
// pg_catalog table that contains the entities of the type of the key.
var regTypeInfos = map[*coltypes.TOid]regTypeInfo{
	coltypes.RegClass:     {"pg_class", "relname", "relation", pgerror.CodeUndefinedTableError},
	coltypes.RegType:      {"pg_type", "typname", "type", pgerror.CodeUndefinedObjectError},
	coltypes.RegProc:      {"pg_proc", "proname", "function", pgerror.CodeUndefinedFunctionError},
	coltypes.RegProcedure: {"pg_proc", "proname", "function", pgerror.CodeUndefinedFunctionError},
	coltypes.RegNamespace: {"pg_namespace", "nspname", "namespace", pgerror.CodeUndefinedObjectError},
}

// queryOidWithJoin looks up the name or OID of an input OID or string in the
// pg_catalog table that the input coltypes.TOid belongs to. If the input Datum
// is a DOid, the relevant table will be queried by OID; if the input is a
// DString, the table will be queried by its name column.
//
// The return value is a fresh DOid of the input coltypes.TOid with name and OID
// set to the result of the query. If there was not exactly one result to the
// query, an error will be returned.
func queryOidWithJoin(
	ctx *EvalContext, typ *coltypes.TOid, d Datum, joinClause string, additionalWhere string,
) (*DOid, error) {
	ret := &DOid{semanticType: typ}
	info := regTypeInfos[typ]
	var queryCol string
	switch d.(type) {
	case *DOid:
		queryCol = "oid"
	case *DString:
		queryCol = info.nameCol
	default:
		panic(fmt.Sprintf("invalid argument to OID cast: %s", d))
	}
	results, err := ctx.InternalExecutor.QueryRow(
		ctx.Ctx(), "queryOidWithJoin",
		ctx.Txn,
		fmt.Sprintf(
			"SELECT %s.oid, %s FROM pg_catalog.%s %s WHERE %s = $1 %s",
			info.tableName, info.nameCol, info.tableName, joinClause, queryCol, additionalWhere),
		d)
	if err != nil {
		if _, ok := err.(*MultipleResultsError); ok {
			return nil, pgerror.NewErrorf(pgerror.CodeAmbiguousAliasError,
				"more than one %s named %s", info.objName, d)
		}
		return nil, err
	}
	if results.Len() == 0 {
		return nil, pgerror.NewErrorf(info.errType, "%s %s does not exist", info.objName, d)
	}
	ret.DInt = results[0].(*DOid).DInt
	ret.name = AsStringWithFlags(results[1], FmtBareStrings)
	return ret, nil
}

func queryOid(ctx *EvalContext, typ *coltypes.TOid, d Datum) (*DOid, error) {
	return queryOidWithJoin(ctx, typ, d, "", "")
}

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
	return PerformCast(ctx, d, expr.Type)
}

// PerformCast performs a cast from the provided Datum to the specified
// CastTargetType.
func PerformCast(ctx *EvalContext, d Datum, t coltypes.CastTargetType) (Datum, error) {
	switch typ := t.(type) {
	case *coltypes.TBitArray:
		switch v := d.(type) {
		case *DBitArray:
			if typ.Width == 0 || v.BitLen() == typ.Width {
				return d, nil
			}
			var a DBitArray
			a.BitArray = v.BitArray.ToWidth(typ.Width)
			return &a, nil
		case *DInt:
			return NewDBitArrayFromInt(int64(*v), typ.Width)
		case *DString:
			res, err := bitarray.Parse(string(*v))
			if err != nil {
				return nil, err
			}
			if typ.Width > 0 {
				res = res.ToWidth(typ.Width)
			}
			return &DBitArray{BitArray: res}, nil
		case *DCollatedString:
			res, err := bitarray.Parse(v.Contents)
			if err != nil {
				return nil, err
			}
			if typ.Width > 0 {
				res = res.ToWidth(typ.Width)
			}
			return &DBitArray{BitArray: res}, nil
		}

	case *coltypes.TBool:
		switch v := d.(type) {
		case *DBool:
			return d, nil
		case *DInt:
			return MakeDBool(*v != 0), nil
		case *DFloat:
			return MakeDBool(*v != 0), nil
		case *DDecimal:
			return MakeDBool(v.Sign() != 0), nil
		case *DString:
			return ParseDBool(string(*v))
		case *DCollatedString:
			return ParseDBool(v.Contents)
		}

	case *coltypes.TInt:
		var res *DInt
		switch v := d.(type) {
		case *DBitArray:
			res = v.AsDInt(uint(typ.Width))
		case *DBool:
			if *v {
				res = NewDInt(1)
			} else {
				res = DZero
			}
		case *DInt:
			// TODO(knz): enforce the coltype width here.
			res = v
		case *DFloat:
			f := float64(*v)
			// Use `<=` and `>=` here instead of just `<` and `>` because when
			// math.MaxInt64 and math.MinInt64 are converted to float64s, they are
			// rounded to numbers with larger absolute values. Note that the first
			// next FP value after and strictly greater than float64(math.MinInt64)
			// is -9223372036854774784 (= float64(math.MinInt64)+513) and the first
			// previous value and strictly smaller than float64(math.MaxInt64)
			// is 9223372036854774784 (= float64(math.MaxInt64)-513), and both are
			// convertible to int without overflow.
			if math.IsNaN(f) || f <= float64(math.MinInt64) || f >= float64(math.MaxInt64) {
				return nil, errIntOutOfRange
			}
			res = NewDInt(DInt(f))
		case *DDecimal:
			d := ctx.getTmpDec()
			_, err := DecimalCtx.RoundToIntegralValue(d, &v.Decimal)
			if err != nil {
				return nil, err
			}
			i, err := d.Int64()
			if err != nil {
				return nil, errIntOutOfRange
			}
			res = NewDInt(DInt(i))
		case *DString:
			var err error
			if res, err = ParseDInt(string(*v)); err != nil {
				return nil, err
			}
		case *DCollatedString:
			var err error
			if res, err = ParseDInt(v.Contents); err != nil {
				return nil, err
			}
		case *DTimestamp:
			res = NewDInt(DInt(v.Unix()))
		case *DTimestampTZ:
			res = NewDInt(DInt(v.Unix()))
		case *DDate:
			res = NewDInt(DInt(int64(*v)))
		case *DInterval:
			iv, ok := v.AsInt64()
			if !ok {
				return nil, errIntOutOfRange
			}
			res = NewDInt(DInt(iv))
		case *DOid:
			res = &v.DInt
		}
		if res != nil {
			return res, nil
		}

	case *coltypes.TFloat:
		switch v := d.(type) {
		case *DBool:
			if *v {
				return NewDFloat(1), nil
			}
			return NewDFloat(0), nil
		case *DInt:
			return NewDFloat(DFloat(*v)), nil
		case *DFloat:
			return d, nil
		case *DDecimal:
			f, err := v.Float64()
			if err != nil {
				return nil, errFloatOutOfRange
			}
			return NewDFloat(DFloat(f)), nil
		case *DString:
			return ParseDFloat(string(*v))
		case *DCollatedString:
			return ParseDFloat(v.Contents)
		case *DTimestamp:
			micros := float64(v.Nanosecond() / int(time.Microsecond))
			return NewDFloat(DFloat(float64(v.Unix()) + micros*1e-6)), nil
		case *DTimestampTZ:
			micros := float64(v.Nanosecond() / int(time.Microsecond))
			return NewDFloat(DFloat(float64(v.Unix()) + micros*1e-6)), nil
		case *DDate:
			return NewDFloat(DFloat(float64(*v))), nil
		case *DInterval:
			return NewDFloat(DFloat(v.AsFloat64())), nil
		}

	case *coltypes.TDecimal:
		var dd DDecimal
		var err error
		unset := false
		switch v := d.(type) {
		case *DBool:
			if *v {
				dd.SetFinite(1, 0)
			}
		case *DInt:
			dd.SetFinite(int64(*v), 0)
		case *DDate:
			dd.SetFinite(int64(*v), 0)
		case *DFloat:
			_, err = dd.SetFloat64(float64(*v))
		case *DDecimal:
			// Small optimization to avoid copying into dd in normal case.
			if typ.Prec == 0 {
				return d, nil
			}
			dd = *v
		case *DString:
			err = dd.SetString(string(*v))
		case *DCollatedString:
			err = dd.SetString(v.Contents)
		case *DTimestamp:
			val := &dd.Coeff
			val.SetInt64(v.Unix())
			val.Mul(val, big10E6)
			micros := v.Nanosecond() / int(time.Microsecond)
			val.Add(val, big.NewInt(int64(micros)))
			dd.Exponent = -6
		case *DTimestampTZ:
			val := &dd.Coeff
			val.SetInt64(v.Unix())
			val.Mul(val, big10E6)
			micros := v.Nanosecond() / int(time.Microsecond)
			val.Add(val, big.NewInt(int64(micros)))
			dd.Exponent = -6
		case *DInterval:
			v.AsBigInt(&dd.Coeff)
			dd.Exponent = -9
		default:
			unset = true
		}
		if err != nil {
			return nil, err
		}
		if !unset {
			err = LimitDecimalWidth(&dd.Decimal, typ.Prec, typ.Scale)
			return &dd, err
		}

	case *coltypes.TString, *coltypes.TCollatedString, *coltypes.TName:
		var s string
		switch t := d.(type) {
		case *DBitArray:
			s = t.BitArray.String()
		case *DFloat:
			s = strconv.FormatFloat(float64(*t), 'g',
				ctx.SessionData.DataConversion.GetFloatPrec(), 64)
		case *DBool, *DInt, *DDecimal:
			s = d.String()
		case *DTimestamp, *DTimestampTZ, *DDate, *DTime:
			s = AsStringWithFlags(d, FmtBareStrings)
		case *DTuple:
			s = AsStringWithFlags(d, FmtPgwireText)
		case *DArray:
			s = AsStringWithFlags(d, FmtPgwireText)
		case *DInterval:
			// When converting an interval to string, we need a string representation
			// of the duration (e.g. "5s") and not of the interval itself (e.g.
			// "INTERVAL '5s'").
			s = t.ValueAsString()
		case *DUuid:
			s = t.UUID.String()
		case *DIPAddr:
			s = t.String()
		case *DString:
			s = string(*t)
		case *DCollatedString:
			s = t.Contents
		case *DBytes:
			s = lex.EncodeByteArrayToRawBytes(string(*t),
				ctx.SessionData.DataConversion.BytesEncodeFormat, false /* skipHexPrefix */)
		case *DOid:
			s = t.name
		case *DJSON:
			s = t.JSON.String()
		}
		switch c := t.(type) {
		case *coltypes.TString:
			// If the string type specifies a limit we truncate to that limit:
			//   'hello'::CHAR(2) -> 'he'
			// This is true of all the string type variants.
			if c.N > 0 && c.N < uint(len(s)) {
				s = s[:c.N]
			}
			return NewDString(s), nil
		case *coltypes.TCollatedString:
			// Ditto truncation like for TString.
			if c.N > 0 && c.N < uint(len(s)) {
				s = s[:c.N]
			}
			return NewDCollatedString(s, c.Locale, &ctx.collationEnv), nil
		case *coltypes.TName:
			return NewDName(s), nil
		}

	case *coltypes.TBytes:
		switch t := d.(type) {
		case *DString:
			return ParseDByte(string(*t))
		case *DCollatedString:
			return NewDBytes(DBytes(t.Contents)), nil
		case *DUuid:
			return NewDBytes(DBytes(t.GetBytes())), nil
		case *DBytes:
			return d, nil
		}

	case *coltypes.TUUID:
		switch t := d.(type) {
		case *DString:
			return ParseDUuidFromString(string(*t))
		case *DCollatedString:
			return ParseDUuidFromString(t.Contents)
		case *DBytes:
			return ParseDUuidFromBytes([]byte(*t))
		case *DUuid:
			return d, nil
		}

	case *coltypes.TIPAddr:
		switch t := d.(type) {
		case *DString:
			return ParseDIPAddrFromINetString(string(*t))
		case *DCollatedString:
			return ParseDIPAddrFromINetString(t.Contents)
		case *DIPAddr:
			return d, nil
		}

	case *coltypes.TDate:
		switch d := d.(type) {
		case *DString:
			return ParseDDate(ctx, string(*d))
		case *DCollatedString:
			return ParseDDate(ctx, d.Contents)
		case *DDate:
			return d, nil
		case *DInt:
			return NewDDate(DDate(int64(*d))), nil
		case *DTimestampTZ:
			return NewDDateFromTime(d.Time, ctx.GetLocation()), nil
		case *DTimestamp:
			return NewDDateFromTime(d.Time, time.UTC), nil
		}

	case *coltypes.TTime:
		switch d := d.(type) {
		case *DString:
			return ParseDTime(ctx, string(*d))
		case *DCollatedString:
			return ParseDTime(ctx, d.Contents)
		case *DTime:
			return d, nil
		case *DTimestamp:
			return MakeDTime(timeofday.FromTime(d.Time)), nil
		case *DTimestampTZ:
			return MakeDTime(timeofday.FromTime(d.Time)), nil
		case *DInterval:
			return MakeDTime(timeofday.Min.Add(d.Duration)), nil
		}

	case *coltypes.TTimestamp:
		// TODO(knz): Timestamp from float, decimal.
		switch d := d.(type) {
		case *DString:
			return ParseDTimestamp(ctx, string(*d), time.Microsecond)
		case *DCollatedString:
			return ParseDTimestamp(ctx, d.Contents, time.Microsecond)
		case *DDate:
			year, month, day := timeutil.Unix(int64(*d)*SecondsInDay, 0).Date()
			return MakeDTimestamp(time.Date(year, month, day, 0, 0, 0, 0, time.UTC), time.Microsecond), nil
		case *DInt:
			return MakeDTimestamp(timeutil.Unix(int64(*d), 0), time.Second), nil
		case *DTimestamp:
			return d, nil
		case *DTimestampTZ:
			// Strip time zone. Timestamps don't carry their location.
			return d.stripTimeZone(ctx), nil
		}

	case *coltypes.TTimestampTZ:
		// TODO(knz): TimestampTZ from float, decimal.
		switch d := d.(type) {
		case *DString:
			return ParseDTimestampTZ(ctx, string(*d), time.Microsecond)
		case *DCollatedString:
			return ParseDTimestampTZ(ctx, d.Contents, time.Microsecond)
		case *DDate:
			return MakeDTimestampTZFromDate(ctx.GetLocation(), d), nil
		case *DTimestamp:
			_, before := d.Time.Zone()
			_, after := d.Time.In(ctx.GetLocation()).Zone()
			return MakeDTimestampTZ(d.Time.Add(time.Duration(before-after)*time.Second), time.Microsecond), nil
		case *DInt:
			return MakeDTimestampTZ(timeutil.Unix(int64(*d), 0), time.Second), nil
		case *DTimestampTZ:
			return d, nil
		}

	case *coltypes.TInterval:
		switch v := d.(type) {
		case *DString:
			return ParseDInterval(string(*v))
		case *DCollatedString:
			return ParseDInterval(v.Contents)
		case *DInt:
			return &DInterval{Duration: duration.FromInt64(int64(*v))}, nil
		case *DFloat:
			return &DInterval{Duration: duration.FromFloat64(float64(*v))}, nil
		case *DTime:
			return &DInterval{Duration: duration.Duration{Nanos: int64(*v) * 1000}}, nil
		case *DDecimal:
			d := ctx.getTmpDec()
			dnanos := v.Decimal
			dnanos.Exponent += 9
			// We need HighPrecisionCtx because duration values can contain
			// upward of 35 decimal digits and DecimalCtx only provides 25.
			_, err := HighPrecisionCtx.Quantize(d, &dnanos, 0)
			if err != nil {
				return nil, err
			}
			if dnanos.Negative {
				d.Coeff.Neg(&d.Coeff)
			}
			dv, ok := duration.FromBigInt(&d.Coeff)
			if !ok {
				return nil, errDecOutOfRange
			}
			return &DInterval{Duration: dv}, nil
		case *DInterval:
			return d, nil
		}
	case *coltypes.TJSON:
		switch v := d.(type) {
		case *DString:
			return ParseDJSON(string(*v))
		case *DJSON:
			return v, nil
		}
	case *coltypes.TArray:
		switch v := d.(type) {
		case *DString:
			return ParseDArrayFromString(ctx, string(*v), typ.ParamType)
		case *DArray:
			paramType := coltypes.CastTargetToDatumType(typ.ParamType)
			dcast := NewDArray(paramType)
			for _, e := range v.Array {
				ecast := DNull
				if e != DNull {
					var err error
					ecast, err = PerformCast(ctx, e, typ.ParamType)
					if err != nil {
						return nil, err
					}
				}

				if err := dcast.Append(ecast); err != nil {
					return nil, err
				}
			}
			return dcast, nil
		}
	case *coltypes.TOid:
		switch v := d.(type) {
		case *DOid:
			switch typ {
			case coltypes.Oid:
				return &DOid{semanticType: typ, DInt: v.DInt}, nil
			default:
				oid, err := queryOid(ctx, typ, v)
				if err != nil {
					oid = NewDOid(v.DInt)
					oid.semanticType = typ
				}
				return oid, nil
			}
		case *DInt:
			switch typ {
			case coltypes.Oid:
				return &DOid{semanticType: typ, DInt: *v}, nil
			default:
				tmpOid := NewDOid(*v)
				oid, err := queryOid(ctx, typ, tmpOid)
				if err != nil {
					oid = tmpOid
					oid.semanticType = typ
				}
				return oid, nil
			}
		case *DString:
			s := string(*v)
			// Trim whitespace and unwrap outer quotes if necessary.
			// This is required to mimic postgres.
			s = strings.TrimSpace(s)
			origS := s
			if len(s) > 1 && s[0] == '"' && s[len(s)-1] == '"' {
				s = s[1 : len(s)-1]
			}

			switch typ {
			case coltypes.Oid:
				i, err := ParseDInt(s)
				if err != nil {
					return nil, err
				}
				return &DOid{semanticType: typ, DInt: *i}, nil
			case coltypes.RegProc, coltypes.RegProcedure:
				// Trim procedure type parameters, e.g. `max(int)` becomes `max`.
				// Postgres only does this when the cast is ::regprocedure, but we're
				// going to always do it.
				// We additionally do not yet implement disambiguation based on type
				// parameters: we return the match iff there is exactly one.
				s = pgSignatureRegexp.ReplaceAllString(s, "$1")
				// Resolve function name.
				substrs := strings.Split(s, ".")
				if len(substrs) > 3 {
					// A fully qualified function name in pg's dialect can contain
					// at most 3 parts: db.schema.funname.
					// For example mydb.pg_catalog.max().
					// Anything longer is always invalid.
					return nil, pgerror.NewErrorf(pgerror.CodeSyntaxError,
						"invalid function name: %s", s)
				}
				name := UnresolvedName{NumParts: len(substrs)}
				for i := 0; i < len(substrs); i++ {
					name.Parts[i] = substrs[len(substrs)-1-i]
				}
				funcDef, err := name.ResolveFunction(ctx.SessionData.SearchPath)
				if err != nil {
					return nil, err
				}
				return queryOid(ctx, typ, NewDString(funcDef.Name))
			case coltypes.RegType:
				colType, err := ctx.Planner.ParseType(s)
				if err == nil {
					datumType := coltypes.CastTargetToDatumType(colType)
					return &DOid{semanticType: typ, DInt: DInt(datumType.Oid()), name: datumType.SQLName()}, nil
				}
				// Fall back to searching pg_type, since we don't provide syntax for
				// every postgres type that we understand OIDs for.
				// Trim type modifiers, e.g. `numeric(10,3)` becomes `numeric`.
				s = pgSignatureRegexp.ReplaceAllString(s, "$1")
				return queryOid(ctx, typ, NewDString(s))

			case coltypes.RegClass:
				tn, err := ctx.Planner.ParseQualifiedTableName(ctx.Ctx(), origS)
				if err != nil {
					return nil, err
				}
				if err := ctx.Planner.ResolveTableName(ctx.Ctx(), tn); err != nil {
					return nil, err
				}
				// Determining the table's OID requires joining against the databases
				// table because we only have the database name, not its OID, which is
				// what is stored in pg_class. This extra join means we can't use
				// queryOid like everyone else.
				return queryOidWithJoin(ctx, typ, NewDString(tn.Table()),
					"JOIN pg_catalog.pg_namespace ON relnamespace = pg_namespace.oid",
					fmt.Sprintf("AND nspname = '%s'", tn.Schema()))
			default:
				return queryOid(ctx, typ, NewDString(s))
			}
		}
	}

	return nil, pgerror.NewErrorf(
		pgerror.CodeCannotCoerceError, "invalid cast: %s -> %s", d.ResolvedType(), t)
}

// Eval implements the TypedExpr interface.
func (expr *IndirectionExpr) Eval(ctx *EvalContext) (Datum, error) {
	var subscriptIdx int
	for i, t := range expr.Indirection {
		if t.Slice {
			return nil, pgerror.UnimplementedWithIssueErrorf(2115, "ARRAY slicing in %s", expr)
		}
		if i > 0 {
			return nil, pgerror.UnimplementedWithIssueErrorf(2115, "multidimensional ARRAY %s", expr)
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
	if w, ok := d.(*DOidWrapper); ok {
		switch w.Oid {
		case oid.T_oidvector, oid.T_int2vector:
			subscriptIdx++
		}
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
		return NewDCollatedString(string(*d), expr.Locale, &ctx.collationEnv), nil
	case *DCollatedString:
		return NewDCollatedString(d.Contents, expr.Locale, &ctx.collationEnv), nil
	default:
		return nil, pgerror.NewErrorf(pgerror.CodeDatatypeMismatchError, "incompatible type for COLLATE: %s", d)
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
	if op.hasSubOperator() {
		var datums Datums
		// Right is either a tuple or an array of Datums.
		if tuple, ok := AsDTuple(right); ok {
			datums = tuple.D
		} else if array, ok := AsDArray(right); ok {
			datums = array.Array
		} else {
			return nil, pgerror.NewAssertionErrorf("unhandled right expression %s", right)
		}
		return evalDatumsCmp(ctx, op, expr.SubOperator, expr.fn, left, datums)
	}

	_, newLeft, newRight, _, not := foldComparisonExpr(op, left, right)
	if !expr.fn.NullableArgs && (newLeft == DNull || newRight == DNull) {
		return DNull, nil
	}
	d, err := expr.fn.Fn(ctx, newLeft.(Datum), newRight.(Datum))
	if err != nil {
		return nil, err
	}
	if b, ok := d.(*DBool); ok {
		return MakeDBool(*b != DBool(not)), nil
	}
	return d, nil
}

// EvalArgsAndGetGenerator evaluates the arguments and instanciates a
// ValueGenerator for use by set projections.
func (expr *FuncExpr) EvalArgsAndGetGenerator(ctx *EvalContext) (ValueGenerator, error) {
	if expr.fn == nil || expr.fnProps.Class != GeneratorClass {
		return nil, pgerror.NewAssertionErrorf("cannot call EvalArgsAndGetGenerator() on non-aggregate function: %q", ErrString(expr))
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
		// If we are facing a retry error, in particular those generated
		// by crdb_internal.force_retry(), propagate it unchanged, so that
		// the executor can see it with the right type.
		if _, ok := err.(*roachpb.HandledRetryableTxnError); ok {
			return nil, err
		}
		// If we are facing an explicit error, propagate it unchanged.
		fName := expr.Func.String()
		if fName == `crdb_internal.force_error` {
			return nil, err
		}
		return nil, pgerror.Wrap(err, pgerror.CodeDataExceptionError, fName+"()")
	}
	if ctx.TestingKnobs.AssertFuncExprReturnTypes {
		if err := ensureExpectedType(expr.fn.FixedReturnType(), res); err != nil {
			return nil, errors.Wrapf(err, "function %q", expr.String())
		}
	}
	return res, nil
}

// ensureExpectedType will return an error if a datum does not match the
// provided type. If the expected type is Any or if the datum is a Null
// type, then no error will be returned.
func ensureExpectedType(exp types.T, d Datum) error {
	if !(exp.FamilyEqual(types.Any) || d.ResolvedType().Equivalent(types.Unknown) ||
		d.ResolvedType().Equivalent(exp)) {
		return errors.Errorf("expected return type %q, got: %q", exp, d.ResolvedType())
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
		pqErr, ok := pgerror.GetPGCause(evalErr)
		if !ok || pqErr.Code != errpatStr {
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

	for _, t := range expr.Types {
		wantTyp := coltypes.CastTargetToDatumType(t)
		if datumTyp.Equivalent(wantTyp) {
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
func (expr *NullIfExpr) Eval(ctx *EvalContext) (Datum, error) {
	expr1, err := expr.Expr1.(TypedExpr).Eval(ctx)
	if err != nil {
		return nil, err
	}
	expr2, err := expr.Expr2.(TypedExpr).Eval(ctx)
	if err != nil {
		return nil, err
	}
	cond, err := evalComparison(ctx, EQ, expr1, expr2)
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
	return nil, pgerror.NewAssertionErrorf("unhandled type %T", expr)
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
			return nil, errors.Wrapf(err, "unary op %q", expr.String())
		}
	}
	return res, err
}

// Eval implements the TypedExpr interface.
func (expr DefaultVal) Eval(ctx *EvalContext) (Datum, error) {
	return nil, pgerror.NewAssertionErrorf("unhandled type %T", expr)
}

// Eval implements the TypedExpr interface.
func (expr UnqualifiedStar) Eval(ctx *EvalContext) (Datum, error) {
	return nil, pgerror.NewAssertionErrorf("unhandled type %T", expr)
}

// Eval implements the TypedExpr interface.
func (expr *UnresolvedName) Eval(ctx *EvalContext) (Datum, error) {
	return nil, pgerror.NewAssertionErrorf("unhandled type %T", expr)
}

// Eval implements the TypedExpr interface.
func (expr *AllColumnsSelector) Eval(ctx *EvalContext) (Datum, error) {
	return nil, pgerror.NewAssertionErrorf("unhandled type %T", expr)
}

// Eval implements the TypedExpr interface.
func (expr *TupleStar) Eval(ctx *EvalContext) (Datum, error) {
	return nil, pgerror.NewAssertionErrorf("unhandled type %T", expr)
}

// Eval implements the TypedExpr interface.
func (expr *ColumnItem) Eval(ctx *EvalContext) (Datum, error) {
	return nil, pgerror.NewAssertionErrorf("unhandled type %T", expr)
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
func arrayOfType(typ types.T) (*DArray, error) {
	arrayTyp, ok := typ.(types.TArray)
	if !ok {
		return nil, pgerror.NewAssertionErrorf("array node type (%v) is not types.TArray", typ)
	}
	if !types.IsValidArrayElementType(arrayTyp.Typ) {
		return nil, pgerror.NewErrorf(pgerror.CodeFeatureNotSupportedError, "arrays of %s not allowed", arrayTyp.Typ)
	}
	return NewDArray(arrayTyp.Typ), nil
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
		return nil, pgerror.NewAssertionErrorf("array subquery result (%v) is not DTuple", d)
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

// Eval implements the TypedExpr interface.
func (t *Placeholder) Eval(ctx *EvalContext) (Datum, error) {
	if !ctx.HasPlaceholders() {
		// While preparing a query, there will be no available placeholders. A
		// placeholder evaluates to itself at this point.
		return t, nil
	}
	e, ok := ctx.Placeholders.Value(t.Name)
	if !ok {
		return nil, pgerror.NewErrorf(pgerror.CodeUndefinedParameterError,
			"no value provided for placeholder: $%s", t.Name)
	}
	// Placeholder expressions cannot contain other placeholders, so we do
	// not need to recurse.
	typ, typed := ctx.Placeholders.Type(t.Name, false)
	if !typed {
		// All placeholders should be typed at this point.
		return nil, pgerror.NewAssertionErrorf("missing type for placeholder %s", t.Name)
	}
	if !e.ResolvedType().Equivalent(typ) {
		// This happens when we overrode the placeholder's type during type
		// checking, since the placeholder's type hint didn't match the desired
		// type for the placeholder. In this case, we cast the expression to
		// the desired type.
		// TODO(jordan): introduce a restriction on what casts are allowed here.
		colType, err := coltypes.DatumTypeToColumnType(typ)
		if err != nil {
			return nil, err
		}
		cast := &CastExpr{Expr: e, Type: colType}
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
	if fn, ok := CmpOps[op].lookupImpl(ltype, rtype); ok {
		return fn.Fn(ctx, left, right)
	}
	return nil, pgerror.NewErrorf(
		pgerror.CodeUndefinedFunctionError, "unsupported comparison operator: <%s> %s <%s>", ltype, op, rtype)
}

// foldComparisonExpr folds a given comparison operation and its expressions
// into an equivalent operation that will hit in the CmpOps map, returning
// this new operation, along with potentially flipped operands and "flipped"
// and "not" flags.
func foldComparisonExpr(
	op ComparisonOperator, left, right Expr,
) (newOp ComparisonOperator, newLeft Expr, newRight Expr, flipped bool, not bool) {
	switch op {
	case NE:
		// NE(left, right) is implemented as !EQ(left, right).
		return EQ, left, right, false, true
	case GT:
		// GT(left, right) is implemented as LT(right, left)
		return LT, right, left, true, false
	case GE:
		// GE(left, right) is implemented as LE(right, left)
		return LE, right, left, true, false
	case NotIn:
		// NotIn(left, right) is implemented as !IN(left, right)
		return In, left, right, false, true
	case NotLike:
		// NotLike(left, right) is implemented as !Like(left, right)
		return Like, left, right, false, true
	case NotILike:
		// NotILike(left, right) is implemented as !ILike(left, right)
		return ILike, left, right, false, true
	case NotSimilarTo:
		// NotSimilarTo(left, right) is implemented as !SimilarTo(left, right)
		return SimilarTo, left, right, false, true
	case NotRegMatch:
		// NotRegMatch(left, right) is implemented as !RegMatch(left, right)
		return RegMatch, left, right, false, true
	case NotRegIMatch:
		// NotRegIMatch(left, right) is implemented as !RegIMatch(left, right)
		return RegIMatch, left, right, false, true
	case IsDistinctFrom:
		// IsDistinctFrom(left, right) is implemented as !IsNotDistinctFrom(left, right)
		// Note: this seems backwards, but IS NOT DISTINCT FROM is an extended
		// version of IS and IS DISTINCT FROM is an extended version of IS NOT.
		return IsNotDistinctFrom, left, right, false, true
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
) (func(string) bool, error) {
	switch len(pattern) {
	case 0:
		return func(s string) bool {
			return s == ""
		}, nil
	case 1:
		switch pattern[0] {
		case '%':
			if escape == '%' {
				return nil, pgerror.NewErrorf(pgerror.CodeInvalidEscapeSequenceError, "LIKE pattern must not end with escape character")
			}
			return func(s string) bool {
				return true
			}, nil
		case '_':
			if escape == '_' {
				return nil, pgerror.NewErrorf(pgerror.CodeInvalidEscapeSequenceError, "LIKE pattern must not end with escape character")
			}
			return func(s string) bool {
				return len(s) == 1
			}, nil
		}
	default:
		if !strings.ContainsAny(pattern[1:len(pattern)-1], "_%") {
			// Patterns with even number of escape characters preceding the ending `%` will have
			// anyEnd set to true. Otherwise anyEnd will be set to false.
			anyEnd := hasUnescapedSuffix(pattern, '%', string(escape))
			// If '%' is the escape character, then it's not a wildcard.
			anyStart := pattern[0] == '%' && escape != '%'

			// Patterns with even number of escape characters preceding the ending `_` will have
			// singleAnyEnd set to true. Otherwise singleAnyEnd will be set to false.
			singleAnyEnd := hasUnescapedSuffix(pattern, '_', string(escape))
			// If '_' is the escape character, then it's not a wildcard.
			singleAnyStart := pattern[0] == '_' && escape != '_'

			// Since we've already checked for escaped characters
			// at the end, we can un-escape every character.
			// This is required since we do direct string
			// comparison.
			var err error
			if pattern, err = unescapePattern(pattern, string(escape), escape == '\\'); err != nil {
				return nil, err
			}
			switch {
			case anyEnd && anyStart:
				return func(s string) bool {
					substr := pattern[1 : len(pattern)-1]
					if caseInsensitive {
						s, substr = strings.ToUpper(s), strings.ToUpper(substr)
					}
					return strings.Contains(s, substr)
				}, nil

			case anyEnd:
				return func(s string) bool {
					prefix := pattern[:len(pattern)-1]
					if singleAnyStart {
						if len(s) == 0 {
							return false
						}

						prefix = prefix[1:]
						s = s[1:]
					}
					if caseInsensitive {
						s, prefix = strings.ToUpper(s), strings.ToUpper(prefix)
					}
					return strings.HasPrefix(s, prefix)
				}, nil

			case anyStart:
				return func(s string) bool {
					suffix := pattern[1:]
					if singleAnyEnd {
						if len(s) == 0 {
							return false
						}

						suffix = suffix[:len(suffix)-1]
						s = s[:len(s)-1]
					}
					if caseInsensitive {
						s, suffix = strings.ToUpper(s), strings.ToUpper(suffix)
					}
					return strings.HasSuffix(s, suffix)
				}, nil

			case singleAnyStart || singleAnyEnd:
				return func(s string) bool {
					if len(s) < 1 || (singleAnyStart && singleAnyEnd && len(s) < 2) {
						return false
					}

					if singleAnyStart {
						pattern = pattern[1:]
						s = s[1:]
					}

					if singleAnyEnd {
						pattern = pattern[:len(pattern)-1]
						s = s[:len(s)-1]
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
					return s == pattern
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
func unescapePattern(pattern, escapeToken string, isEscapeTokenCustom bool) (string, error) {
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
		if nextIdx == len(pattern)-len(escapeToken) && !isEscapeTokenCustom {
			return "", pgerror.NewErrorf(pgerror.CodeInvalidEscapeSequenceError, `LIKE pattern must not end with escape character`)
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
// we later will call unescapePattern, so we need to double our double backslahes.
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
				panic("unexpected: escape character is the last one in replaceCustomEscape.")
			}
		} else if s[sIndex] == '\\' {
			// We encountered a backslash which after QuoteMeta should have been doubled.
			if sIndex+1 == sLen || s[sIndex+1] != '\\' {
				// This case should never be reached, and it should
				// have been caught in calculateLengthAfterReplacingCustomEscape.
				panic("unexpected: a backslash is not doubled in replaceCustomEscape.")
			}
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
			// Regular character, so we simply copy it.
			ret[retIndex] = s[sIndex]
			retIndex++
			sIndex++
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
				return false, 0, pgerror.NewErrorf(pgerror.CodeInvalidEscapeSequenceError, "LIKE pattern must not end with escape character")
			}
		} else if s[i] == '\\' {
			// We encountered a backslash which after QuoteMeta should have been doubled.
			if i+1 == sLen || s[i+1] != '\\' {
				// This case should never be reached.
				return false, 0, pgerror.NewErrorf(pgerror.CodeInvalidEscapeSequenceError, "Unexpected behavior during processing custom escape character.")
			}
			// We'll want to escape '\\' to `\\\\` for correct processing later by unescapePattern.
			// See (3) in the comment above replaceCustomEscape.
			changed = true
			retLen += 4
			i += 2
		} else {
			// Regular character, so we'll simply copy it.
			retLen++
			i++
		}
	}
	return changed, retLen, nil
}

// Pattern implements the RegexpCacheKey interface.
// The strategy for handling custom escape character
// is to convert all unescaped escape character into '\'.
// k.escape can either be empty or a single character.
func (k likeKey) Pattern() (string, error) {
	// QuoteMeta escapes `\` to `\\`.
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
			pattern = replaceUnescaped(pattern, `%`, `.*`, string(k.escape))
		}
		// If `_` is escape character, then it's not a wildcard.
		if k.escape != '_' {
			// Replace LIKE/ILIKE specific wildcards '_' only if it's unescaped.
			pattern = replaceUnescaped(pattern, `_`, `.`, string(k.escape))
		}

		// If a sequence of symbols ` escape+`\\` ` is unescaped, then that escape
		// character escapes backslash in the original pattern (we need to use double
		// backslash because of QuoteMeta behavior), so we want to "consume" the escape character.
		pattern = replaceUnescaped(pattern, string(k.escape)+`\\`, `\\`, string(k.escape))

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
		if pattern, err = unescapePattern(pattern, `\\`, k.escape == '\\'); err != nil {
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
	var escapeRune rune
	if len(escape) > 0 {
		var width int
		escapeRune, width = utf8.DecodeRuneInString(escape)
		if len(escape) > width {
			return DBoolFalse, pgerror.NewErrorf(pgerror.CodeInvalidEscapeSequenceError, "invalid escape string")
		}
	}
	key := similarToKey{s: pattern, escape: escapeRune}
	return matchRegexpWithKey(ctx, NewDString(unescaped), key)
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
//   ^(?: ... )$
// which requires some explanation.  We need "^" and "$" to force
// the pattern to match the entire input string as per SQL99 spec.
// The "(?:" and ")" are a non-capturing set of parens; we have to have
// parens in case the string contains "|", else the "^" and "$" will
// be bound into the first and last alternatives which is not what we
// want, and the parens must be non capturing because we don't want them
// to count when selecting output for SUBSTRING.
func anchorPattern(pattern string, caseInsensitive bool) string {
	if caseInsensitive {
		return fmt.Sprintf("^(?i:%s)$", pattern)
	}
	return fmt.Sprintf("^(?:%s)$", pattern)
}

// FindEqualComparisonFunction looks up an overload of the "=" operator
// for a given pair of input operand types.
func FindEqualComparisonFunction(
	leftType, rightType types.T,
) (func(*EvalContext, Datum, Datum) (Datum, error), bool) {
	fn, found := CmpOps[EQ].lookupImpl(leftType, rightType)
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
		return nil, errIntOutOfRange
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
			eval, err = evalComparison(ctx, LT, g, d)
		} else {
			eval, err = evalComparison(ctx, LT, d, g)
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
