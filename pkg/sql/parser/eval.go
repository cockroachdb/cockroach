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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package parser

import (
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var (
	errZeroModulus     = errors.New("zero modulus")
	errDivByZero       = errors.New("division by zero")
	errIntOutOfRange   = errors.New("integer out of range")
	errFloatOutOfRange = errors.New("float out of range")

	big10E6  = big.NewInt(1e6)
	big10E10 = big.NewInt(1e10)
)

// secondsInDay is the number of seconds in a day.
const secondsInDay = 24 * 60 * 60

// UnaryOp is a unary operator.
type UnaryOp struct {
	Typ        Type
	ReturnType Type
	fn         func(*EvalContext, Datum) (Datum, error)

	types   typeList
	retType returnTyper
}

func (op UnaryOp) params() typeList {
	return op.types
}

func (op UnaryOp) returnType() returnTyper {
	return op.retType
}

func (UnaryOp) preferred() bool {
	return false
}

func init() {
	for op, overload := range UnaryOps {
		for i, impl := range overload {
			impl.types = ArgTypes{{"arg", impl.Typ}}
			impl.retType = fixedReturnType(impl.ReturnType)
			UnaryOps[op][i] = impl
		}
	}
}

// unaryOpOverload is an overloaded set of unary operator implementations.
type unaryOpOverload []UnaryOp

// UnaryOps contains the unary operations indexed by operation type.
var UnaryOps = map[UnaryOperator]unaryOpOverload{
	UnaryPlus: {
		UnaryOp{
			Typ:        TypeInt,
			ReturnType: TypeInt,
			fn: func(_ *EvalContext, d Datum) (Datum, error) {
				return d, nil
			},
		},
		UnaryOp{
			Typ:        TypeFloat,
			ReturnType: TypeFloat,
			fn: func(_ *EvalContext, d Datum) (Datum, error) {
				return d, nil
			},
		},
		UnaryOp{
			Typ:        TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, d Datum) (Datum, error) {
				return d, nil
			},
		},
	},

	UnaryMinus: {
		UnaryOp{
			Typ:        TypeInt,
			ReturnType: TypeInt,
			fn: func(_ *EvalContext, d Datum) (Datum, error) {
				i := MustBeDInt(d)
				if i == math.MinInt64 {
					return nil, errIntOutOfRange
				}
				return NewDInt(-i), nil
			},
		},
		UnaryOp{
			Typ:        TypeFloat,
			ReturnType: TypeFloat,
			fn: func(_ *EvalContext, d Datum) (Datum, error) {
				return NewDFloat(-*d.(*DFloat)), nil
			},
		},
		UnaryOp{
			Typ:        TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, d Datum) (Datum, error) {
				dec := &d.(*DDecimal).Decimal
				dd := &DDecimal{}
				dd.Decimal.Neg(dec)
				return dd, nil
			},
		},
		UnaryOp{
			Typ:        TypeInterval,
			ReturnType: TypeInterval,
			fn: func(_ *EvalContext, d Datum) (Datum, error) {
				i := d.(*DInterval).Duration
				i.Nanos = -i.Nanos
				i.Days = -i.Days
				i.Months = -i.Months
				return &DInterval{Duration: i}, nil
			},
		},
	},

	UnaryComplement: {
		UnaryOp{
			Typ:        TypeInt,
			ReturnType: TypeInt,
			fn: func(_ *EvalContext, d Datum) (Datum, error) {
				return NewDInt(^MustBeDInt(d)), nil
			},
		},
	},
}

// BinOp is a binary operator.
type BinOp struct {
	LeftType   Type
	RightType  Type
	ReturnType Type
	fn         func(*EvalContext, Datum, Datum) (Datum, error)

	types   typeList
	retType returnTyper
}

func (op BinOp) params() typeList {
	return op.types
}

func (op BinOp) matchParams(l, r Type) bool {
	return op.params().matchAt(l, 0) && op.params().matchAt(r, 1)
}

func (op BinOp) returnType() returnTyper {
	return op.retType
}

func (BinOp) preferred() bool {
	return false
}

func init() {
	for op, overload := range BinOps {
		for i, impl := range overload {
			impl.types = ArgTypes{{"left", impl.LeftType}, {"right", impl.RightType}}
			impl.retType = fixedReturnType(impl.ReturnType)
			BinOps[op][i] = impl
		}
	}
}

// binOpOverload is an overloaded set of binary operator implementations.
type binOpOverload []BinOp

func (o binOpOverload) lookupImpl(left, right Type) (BinOp, bool) {
	for _, fn := range o {
		if fn.matchParams(left, right) {
			return fn, true
		}
	}
	return BinOp{}, false
}

// BinOps contains the binary operations indexed by operation type.
var BinOps = map[BinaryOperator]binOpOverload{
	Bitand: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeInt,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDInt(MustBeDInt(left) & MustBeDInt(right)), nil
			},
		},
	},

	Bitor: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeInt,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDInt(MustBeDInt(left) | MustBeDInt(right)), nil
			},
		},
	},

	Bitxor: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeInt,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDInt(MustBeDInt(left) ^ MustBeDInt(right)), nil
			},
		},
	},

	Plus: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeInt,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				a, b := MustBeDInt(left), MustBeDInt(right)
				if b > 0 && a > math.MaxInt64-b {
					return nil, errIntOutOfRange
				}
				if b < 0 && a < math.MinInt64-b {
					return nil, errIntOutOfRange
				}
				return NewDInt(a + b), nil
			},
		},
		BinOp{
			LeftType:   TypeFloat,
			RightType:  TypeFloat,
			ReturnType: TypeFloat,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDFloat(*left.(*DFloat) + *right.(*DFloat)), nil
			},
		},
		BinOp{
			LeftType:   TypeDecimal,
			RightType:  TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				_, err := ExactCtx.Add(&dd.Decimal, l, r)
				return dd, err
			},
		},
		BinOp{
			LeftType:   TypeDecimal,
			RightType:  TypeInt,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := MustBeDInt(right)
				dd := &DDecimal{}
				dd.SetCoefficient(int64(r))
				_, err := ExactCtx.Add(&dd.Decimal, l, &dd.Decimal)
				return dd, err
			},
		},
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := MustBeDInt(left)
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				dd.SetCoefficient(int64(l))
				_, err := ExactCtx.Add(&dd.Decimal, &dd.Decimal, r)
				return dd, err
			},
		},
		BinOp{
			LeftType:   TypeDate,
			RightType:  TypeInt,
			ReturnType: TypeDate,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDDate(*left.(*DDate) + DDate(MustBeDInt(right))), nil
			},
		},
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeDate,
			ReturnType: TypeDate,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDDate(DDate(MustBeDInt(left)) + *right.(*DDate)), nil
			},
		},
		BinOp{
			LeftType:   TypeTimestamp,
			RightType:  TypeInterval,
			ReturnType: TypeTimestamp,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return MakeDTimestamp(duration.Add(left.(*DTimestamp).Time, right.(*DInterval).Duration), time.Microsecond), nil
			},
		},
		BinOp{
			LeftType:   TypeInterval,
			RightType:  TypeTimestamp,
			ReturnType: TypeTimestamp,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return MakeDTimestamp(duration.Add(right.(*DTimestamp).Time, left.(*DInterval).Duration), time.Microsecond), nil
			},
		},
		BinOp{
			LeftType:   TypeTimestampTZ,
			RightType:  TypeInterval,
			ReturnType: TypeTimestampTZ,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				t := duration.Add(left.(*DTimestampTZ).Time, right.(*DInterval).Duration)
				return MakeDTimestampTZ(t, time.Microsecond), nil
			},
		},
		BinOp{
			LeftType:   TypeInterval,
			RightType:  TypeTimestampTZ,
			ReturnType: TypeTimestampTZ,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				t := duration.Add(right.(*DTimestampTZ).Time, left.(*DInterval).Duration)
				return MakeDTimestampTZ(t, time.Microsecond), nil
			},
		},
		BinOp{
			LeftType:   TypeInterval,
			RightType:  TypeInterval,
			ReturnType: TypeInterval,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return &DInterval{Duration: left.(*DInterval).Duration.Add(right.(*DInterval).Duration)}, nil
			},
		},
		BinOp{
			LeftType:   TypeDate,
			RightType:  TypeInterval,
			ReturnType: TypeTimestampTZ,
			fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				leftTZ := MakeDTimestampTZFromDate(ctx.GetLocation(), left.(*DDate))
				t := duration.Add(leftTZ.Time, right.(*DInterval).Duration)
				return MakeDTimestampTZ(t, time.Microsecond), nil
			},
		},
		BinOp{
			LeftType:   TypeInterval,
			RightType:  TypeDate,
			ReturnType: TypeTimestampTZ,
			fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				rightTZ := MakeDTimestampTZFromDate(ctx.GetLocation(), right.(*DDate))
				t := duration.Add(rightTZ.Time, left.(*DInterval).Duration)
				return MakeDTimestampTZ(t, time.Microsecond), nil
			},
		},
	},

	Minus: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeInt,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
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
		BinOp{
			LeftType:   TypeFloat,
			RightType:  TypeFloat,
			ReturnType: TypeFloat,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDFloat(*left.(*DFloat) - *right.(*DFloat)), nil
			},
		},
		BinOp{
			LeftType:   TypeDecimal,
			RightType:  TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				_, err := ExactCtx.Sub(&dd.Decimal, l, r)
				return dd, err
			},
		},
		BinOp{
			LeftType:   TypeDecimal,
			RightType:  TypeInt,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := MustBeDInt(right)
				dd := &DDecimal{}
				dd.SetCoefficient(int64(r))
				_, err := ExactCtx.Sub(&dd.Decimal, l, &dd.Decimal)
				return dd, err
			},
		},
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := MustBeDInt(left)
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				dd.SetCoefficient(int64(l))
				_, err := ExactCtx.Sub(&dd.Decimal, &dd.Decimal, r)
				return dd, err
			},
		},
		BinOp{
			LeftType:   TypeDate,
			RightType:  TypeInt,
			ReturnType: TypeDate,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDDate(*left.(*DDate) - DDate(MustBeDInt(right))), nil
			},
		},
		BinOp{
			LeftType:   TypeDate,
			RightType:  TypeDate,
			ReturnType: TypeInt,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDInt(DInt(*left.(*DDate) - *right.(*DDate))), nil
			},
		},
		BinOp{
			LeftType:   TypeTimestamp,
			RightType:  TypeTimestamp,
			ReturnType: TypeInterval,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				nanos := left.(*DTimestamp).Sub(right.(*DTimestamp).Time).Nanoseconds()
				return &DInterval{Duration: duration.Duration{Nanos: nanos}}, nil
			},
		},
		BinOp{
			LeftType:   TypeTimestampTZ,
			RightType:  TypeTimestampTZ,
			ReturnType: TypeInterval,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				nanos := left.(*DTimestampTZ).Sub(right.(*DTimestampTZ).Time).Nanoseconds()
				return &DInterval{Duration: duration.Duration{Nanos: nanos}}, nil
			},
		},
		BinOp{
			LeftType:   TypeTimestamp,
			RightType:  TypeTimestampTZ,
			ReturnType: TypeInterval,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				nanos := left.(*DTimestamp).Sub(right.(*DTimestampTZ).Time).Nanoseconds()
				return &DInterval{Duration: duration.Duration{Nanos: nanos}}, nil
			},
		},
		BinOp{
			LeftType:   TypeTimestampTZ,
			RightType:  TypeTimestamp,
			ReturnType: TypeInterval,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				nanos := left.(*DTimestampTZ).Sub(right.(*DTimestamp).Time).Nanoseconds()
				return &DInterval{Duration: duration.Duration{Nanos: nanos}}, nil
			},
		},
		BinOp{
			LeftType:   TypeTimestamp,
			RightType:  TypeInterval,
			ReturnType: TypeTimestamp,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return MakeDTimestamp(duration.Add(left.(*DTimestamp).Time, right.(*DInterval).Duration.Mul(-1)), time.Microsecond), nil
			},
		},
		BinOp{
			LeftType:   TypeTimestampTZ,
			RightType:  TypeInterval,
			ReturnType: TypeTimestampTZ,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				t := duration.Add(left.(*DTimestampTZ).Time, right.(*DInterval).Duration.Mul(-1))
				return MakeDTimestampTZ(t, time.Microsecond), nil
			},
		},
		BinOp{
			LeftType:   TypeDate,
			RightType:  TypeInterval,
			ReturnType: TypeTimestampTZ,
			fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				leftTZ := MakeDTimestampTZFromDate(ctx.GetLocation(), left.(*DDate))
				t := duration.Add(leftTZ.Time, right.(*DInterval).Duration.Mul(-1))
				return MakeDTimestampTZ(t, time.Microsecond), nil
			},
		},
		BinOp{
			LeftType:   TypeInterval,
			RightType:  TypeInterval,
			ReturnType: TypeInterval,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return &DInterval{Duration: left.(*DInterval).Duration.Sub(right.(*DInterval).Duration)}, nil
			},
		},
	},

	Mult: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeInt,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
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
		BinOp{
			LeftType:   TypeFloat,
			RightType:  TypeFloat,
			ReturnType: TypeFloat,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDFloat(*left.(*DFloat) * *right.(*DFloat)), nil
			},
		},
		BinOp{
			LeftType:   TypeDecimal,
			RightType:  TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				_, err := ExactCtx.Mul(&dd.Decimal, l, r)
				return dd, err
			},
		},
		// The following two overloads are needed becauase DInt/DInt = DDecimal. Due to this
		// operation, normalization may sometimes create a DInt * DDecimal operation.
		BinOp{
			LeftType:   TypeDecimal,
			RightType:  TypeInt,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := MustBeDInt(right)
				dd := &DDecimal{}
				dd.SetCoefficient(int64(r))
				_, err := ExactCtx.Mul(&dd.Decimal, l, &dd.Decimal)
				return dd, err
			},
		},
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := MustBeDInt(left)
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				dd.SetCoefficient(int64(l))
				_, err := ExactCtx.Mul(&dd.Decimal, &dd.Decimal, r)
				return dd, err
			},
		},
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInterval,
			ReturnType: TypeInterval,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return &DInterval{Duration: right.(*DInterval).Duration.Mul(int64(MustBeDInt(left)))}, nil
			},
		},
		BinOp{
			LeftType:   TypeInterval,
			RightType:  TypeInt,
			ReturnType: TypeInterval,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return &DInterval{Duration: left.(*DInterval).Duration.Mul(int64(MustBeDInt(right)))}, nil
			},
		},
		BinOp{
			LeftType:   TypeInterval,
			RightType:  TypeFloat,
			ReturnType: TypeInterval,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				r := float64(*right.(*DFloat))
				return &DInterval{Duration: left.(*DInterval).Duration.MulFloat(r)}, nil
			},
		},
	},

	Div: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeDecimal,
			fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				rInt := MustBeDInt(right)
				div := ctx.getTmpDec().SetCoefficient(int64(rInt)).SetExponent(0)
				dd := &DDecimal{}
				dd.SetCoefficient(int64(MustBeDInt(left)))
				_, err := DecimalCtx.Quo(&dd.Decimal, &dd.Decimal, div)
				return dd, err
			},
		},
		BinOp{
			LeftType:   TypeFloat,
			RightType:  TypeFloat,
			ReturnType: TypeFloat,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDFloat(*left.(*DFloat) / *right.(*DFloat)), nil
			},
		},
		BinOp{
			LeftType:   TypeDecimal,
			RightType:  TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				_, err := DecimalCtx.Quo(&dd.Decimal, l, r)
				return dd, err
			},
		},
		BinOp{
			LeftType:   TypeDecimal,
			RightType:  TypeInt,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := MustBeDInt(right)
				dd := &DDecimal{}
				dd.SetCoefficient(int64(r))
				_, err := DecimalCtx.Quo(&dd.Decimal, l, &dd.Decimal)
				return dd, err
			},
		},
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := MustBeDInt(left)
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				dd.SetCoefficient(int64(l))
				_, err := DecimalCtx.Quo(&dd.Decimal, &dd.Decimal, r)
				return dd, err
			},
		},
		BinOp{
			LeftType:   TypeInterval,
			RightType:  TypeInt,
			ReturnType: TypeInterval,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				rInt := MustBeDInt(right)
				if rInt == 0 {
					return nil, errDivByZero
				}
				return &DInterval{Duration: left.(*DInterval).Duration.Div(int64(rInt))}, nil
			},
		},
		BinOp{
			LeftType:   TypeInterval,
			RightType:  TypeFloat,
			ReturnType: TypeInterval,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				r := float64(*right.(*DFloat))
				if r == 0.0 {
					return nil, errDivByZero
				}
				return &DInterval{Duration: left.(*DInterval).Duration.DivFloat(r)}, nil
			},
		},
	},

	FloorDiv: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeInt,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				rInt := MustBeDInt(right)
				if rInt == 0 {
					return nil, errDivByZero
				}
				return NewDInt(MustBeDInt(left) / rInt), nil
			},
		},
		BinOp{
			LeftType:   TypeFloat,
			RightType:  TypeFloat,
			ReturnType: TypeFloat,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := float64(*left.(*DFloat))
				r := float64(*right.(*DFloat))
				return NewDFloat(DFloat(math.Trunc(l / r))), nil
			},
		},
		BinOp{
			LeftType:   TypeDecimal,
			RightType:  TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				_, err := HighPrecisionCtx.QuoInteger(&dd.Decimal, l, r)
				return dd, err
			},
		},
		BinOp{
			LeftType:   TypeDecimal,
			RightType:  TypeInt,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := MustBeDInt(right)
				if r == 0 {
					return nil, errDivByZero
				}
				dd := &DDecimal{}
				dd.SetCoefficient(int64(r))
				_, err := HighPrecisionCtx.QuoInteger(&dd.Decimal, l, &dd.Decimal)
				return dd, err
			},
		},
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := MustBeDInt(left)
				r := &right.(*DDecimal).Decimal
				if r.Sign() == 0 {
					return nil, errDivByZero
				}
				dd := &DDecimal{}
				dd.SetCoefficient(int64(l))
				_, err := HighPrecisionCtx.QuoInteger(&dd.Decimal, &dd.Decimal, r)
				return dd, err
			},
		},
	},

	Mod: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeInt,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				r := MustBeDInt(right)
				if r == 0 {
					return nil, errZeroModulus
				}
				return NewDInt(MustBeDInt(left) % r), nil
			},
		},
		BinOp{
			LeftType:   TypeFloat,
			RightType:  TypeFloat,
			ReturnType: TypeFloat,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDFloat(DFloat(math.Mod(float64(*left.(*DFloat)), float64(*right.(*DFloat))))), nil
			},
		},
		BinOp{
			LeftType:   TypeDecimal,
			RightType:  TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				_, err := HighPrecisionCtx.Rem(&dd.Decimal, l, r)
				return dd, err
			},
		},
		BinOp{
			LeftType:   TypeDecimal,
			RightType:  TypeInt,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := MustBeDInt(right)
				dd := &DDecimal{}
				dd.SetCoefficient(int64(r))
				_, err := HighPrecisionCtx.Rem(&dd.Decimal, l, &dd.Decimal)
				return dd, err
			},
		},
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := MustBeDInt(left)
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				dd.SetCoefficient(int64(l))
				_, err := HighPrecisionCtx.Rem(&dd.Decimal, &dd.Decimal, r)
				return dd, err
			},
		},
	},

	Concat: {
		BinOp{
			LeftType:   TypeString,
			RightType:  TypeString,
			ReturnType: TypeString,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDString(string(MustBeDString(left) + MustBeDString(right))), nil
			},
		},
		BinOp{
			LeftType:   TypeBytes,
			RightType:  TypeBytes,
			ReturnType: TypeBytes,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDBytes(*left.(*DBytes) + *right.(*DBytes)), nil
			},
		},
	},

	// TODO(pmattis): Check that the shift is valid.
	LShift: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeInt,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDInt(MustBeDInt(left) << uint(MustBeDInt(right))), nil
			},
		},
	},

	RShift: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeInt,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDInt(MustBeDInt(left) >> uint(MustBeDInt(right))), nil
			},
		},
	},

	Pow: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeInt,
			fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				return intPow(MustBeDInt(left), MustBeDInt(right))
			},
		},
		BinOp{
			LeftType:   TypeFloat,
			RightType:  TypeFloat,
			ReturnType: TypeFloat,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				f := math.Pow(float64(*left.(*DFloat)), float64(*right.(*DFloat)))
				return NewDFloat(DFloat(f)), nil
			},
		},
		BinOp{
			LeftType:   TypeDecimal,
			RightType:  TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				_, err := DecimalCtx.Pow(&dd.Decimal, l, r)
				return dd, err
			},
		},
		BinOp{
			LeftType:   TypeDecimal,
			RightType:  TypeInt,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Decimal
				r := MustBeDInt(right)
				dd := &DDecimal{}
				dd.SetCoefficient(int64(r))
				_, err := DecimalCtx.Pow(&dd.Decimal, l, &dd.Decimal)
				return dd, err
			},
		},
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := MustBeDInt(left)
				r := &right.(*DDecimal).Decimal
				dd := &DDecimal{}
				dd.SetCoefficient(int64(l))
				_, err := DecimalCtx.Pow(&dd.Decimal, &dd.Decimal, r)
				return dd, err
			},
		},
	},
}

var timestampMinusBinOp BinOp

func init() {
	timestampMinusBinOp, _ = BinOps[Minus].lookupImpl(TypeTimestampTZ, TypeTimestampTZ)
}

// CmpOp is a comparison operator.
type CmpOp struct {
	LeftType  Type
	RightType Type

	// Datum return type is a union between *DBool and dNull.
	fn func(*EvalContext, Datum, Datum) (Datum, error)

	types typeList
}

func (op CmpOp) params() typeList {
	return op.types
}

func (op CmpOp) matchParams(l, r Type) bool {
	return op.params().matchAt(l, 0) && op.params().matchAt(r, 1)
}

var cmpOpReturnType = fixedReturnType(TypeBool)

func (op CmpOp) returnType() returnTyper {
	return cmpOpReturnType
}

func (CmpOp) preferred() bool {
	return false
}

func init() {
	for op, overload := range CmpOps {
		for i, impl := range overload {
			impl.types = ArgTypes{{"left", impl.LeftType}, {"right", impl.RightType}}
			CmpOps[op][i] = impl
		}
	}
}

// cmpOpOverload is an overloaded set of comparison operator implementations.
type cmpOpOverload []CmpOp

func (o cmpOpOverload) lookupImpl(left, right Type) (CmpOp, bool) {
	for _, fn := range o {
		if fn.matchParams(left, right) {
			return fn, true
		}
	}
	return CmpOp{}, false
}

// CmpOps contains the comparison operations indexed by operation type.
var CmpOps = map[ComparisonOperator]cmpOpOverload{
	EQ: {
		CmpOp{
			LeftType:  TypeString,
			RightType: TypeString,
			fn:        cmpOpScalarEQFn,
		},
		CmpOp{
			LeftType:  TypeCollatedString,
			RightType: TypeCollatedString,
			fn:        cmpOpScalarEQFn,
		},
		CmpOp{
			LeftType:  TypeBytes,
			RightType: TypeBytes,
			fn:        cmpOpScalarEQFn,
		},
		CmpOp{
			LeftType:  TypeBool,
			RightType: TypeBool,
			fn:        cmpOpScalarEQFn,
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeInt,
			fn:        cmpOpScalarEQFn,
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeFloat,
			fn:        cmpOpScalarEQFn,
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeDecimal,
			fn:        cmpOpScalarEQFn,
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeInt,
			fn:        cmpOpScalarEQFn,
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeFloat,
			fn:        cmpOpScalarEQFn,
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeInt,
			fn:        cmpOpScalarEQFn,
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeDecimal,
			fn:        cmpOpScalarEQFn,
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeFloat,
			fn:        cmpOpScalarEQFn,
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeDecimal,
			fn:        cmpOpScalarEQFn,
		},
		CmpOp{
			LeftType:  TypeDate,
			RightType: TypeDate,
			fn:        cmpOpScalarEQFn,
		},
		CmpOp{
			LeftType:  TypeTimestamp,
			RightType: TypeTimestamp,
			fn:        cmpOpScalarEQFn,
		},
		CmpOp{
			LeftType:  TypeTimestampTZ,
			RightType: TypeTimestampTZ,
			fn:        cmpOpScalarEQFn,
		},
		CmpOp{
			LeftType:  TypeTimestamp,
			RightType: TypeTimestampTZ,
			fn:        cmpOpScalarEQFn,
		},
		CmpOp{
			LeftType:  TypeTimestampTZ,
			RightType: TypeTimestamp,
			fn:        cmpOpScalarEQFn,
		},
		CmpOp{
			LeftType:  TypeDate,
			RightType: TypeTimestampTZ,
			fn:        cmpOpScalarEQFn,
		},
		CmpOp{
			LeftType:  TypeDate,
			RightType: TypeTimestamp,
			fn:        cmpOpScalarEQFn,
		},
		CmpOp{
			LeftType:  TypeTimestampTZ,
			RightType: TypeDate,
			fn:        cmpOpScalarEQFn,
		},
		CmpOp{
			LeftType:  TypeTimestamp,
			RightType: TypeDate,
			fn:        cmpOpScalarEQFn,
		},
		CmpOp{
			LeftType:  TypeInterval,
			RightType: TypeInterval,
			fn:        cmpOpScalarEQFn,
		},
		CmpOp{
			LeftType:  TypeOid,
			RightType: TypeOid,
			fn:        cmpOpScalarEQFn,
		},
		CmpOp{
			LeftType:  TypeTuple,
			RightType: TypeTuple,
			fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				return cmpOpTupleFn(ctx, *left.(*DTuple), *right.(*DTuple), EQ), nil
			},
		},
	},

	LT: {
		CmpOp{
			LeftType:  TypeString,
			RightType: TypeString,
			fn:        cmpOpScalarLTFn,
		},
		CmpOp{
			LeftType:  TypeCollatedString,
			RightType: TypeCollatedString,
			fn:        cmpOpScalarLTFn,
		},
		CmpOp{
			LeftType:  TypeBytes,
			RightType: TypeBytes,
			fn:        cmpOpScalarLTFn,
		},
		CmpOp{
			LeftType:  TypeBool,
			RightType: TypeBool,
			fn:        cmpOpScalarLTFn,
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeInt,
			fn:        cmpOpScalarLTFn,
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeFloat,
			fn:        cmpOpScalarLTFn,
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeDecimal,
			fn:        cmpOpScalarLTFn,
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeInt,
			fn:        cmpOpScalarLTFn,
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeFloat,
			fn:        cmpOpScalarLTFn,
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeInt,
			fn:        cmpOpScalarLTFn,
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeDecimal,
			fn:        cmpOpScalarLTFn,
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeFloat,
			fn:        cmpOpScalarLTFn,
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeDecimal,
			fn:        cmpOpScalarLTFn,
		},
		CmpOp{
			LeftType:  TypeDate,
			RightType: TypeDate,
			fn:        cmpOpScalarLTFn,
		},
		CmpOp{
			LeftType:  TypeTimestamp,
			RightType: TypeTimestamp,
			fn:        cmpOpScalarLTFn,
		},
		CmpOp{
			LeftType:  TypeTimestampTZ,
			RightType: TypeTimestampTZ,
			fn:        cmpOpScalarLTFn,
		},
		CmpOp{
			LeftType:  TypeTimestamp,
			RightType: TypeTimestampTZ,
			fn:        cmpOpScalarLTFn,
		},
		CmpOp{
			LeftType:  TypeTimestampTZ,
			RightType: TypeTimestamp,
			fn:        cmpOpScalarLTFn,
		},
		CmpOp{
			LeftType:  TypeDate,
			RightType: TypeTimestampTZ,
			fn:        cmpOpScalarLTFn,
		},
		CmpOp{
			LeftType:  TypeDate,
			RightType: TypeTimestamp,
			fn:        cmpOpScalarLTFn,
		},
		CmpOp{
			LeftType:  TypeTimestampTZ,
			RightType: TypeDate,
			fn:        cmpOpScalarLTFn,
		},
		CmpOp{
			LeftType:  TypeTimestamp,
			RightType: TypeDate,
			fn:        cmpOpScalarLTFn,
		},
		CmpOp{
			LeftType:  TypeInterval,
			RightType: TypeInterval,
			fn:        cmpOpScalarLTFn,
		},
		CmpOp{
			LeftType:  TypeTuple,
			RightType: TypeTuple,
			fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				return cmpOpTupleFn(ctx, *left.(*DTuple), *right.(*DTuple), LT), nil
			},
		},
	},

	LE: {
		CmpOp{
			LeftType:  TypeString,
			RightType: TypeString,
			fn:        cmpOpScalarLEFn,
		},
		CmpOp{
			LeftType:  TypeCollatedString,
			RightType: TypeCollatedString,
			fn:        cmpOpScalarLEFn,
		},
		CmpOp{
			LeftType:  TypeBytes,
			RightType: TypeBytes,
			fn:        cmpOpScalarLEFn,
		},
		CmpOp{
			LeftType:  TypeBool,
			RightType: TypeBool,
			fn:        cmpOpScalarLEFn,
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeInt,
			fn:        cmpOpScalarLEFn,
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeFloat,
			fn:        cmpOpScalarLEFn,
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeDecimal,
			fn:        cmpOpScalarLEFn,
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeInt,
			fn:        cmpOpScalarLEFn,
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeFloat,
			fn:        cmpOpScalarLEFn,
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeInt,
			fn:        cmpOpScalarLEFn,
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeDecimal,
			fn:        cmpOpScalarLEFn,
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeFloat,
			fn:        cmpOpScalarLEFn,
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeDecimal,
			fn:        cmpOpScalarLEFn,
		},
		CmpOp{
			LeftType:  TypeDate,
			RightType: TypeDate,
			fn:        cmpOpScalarLEFn,
		},
		CmpOp{
			LeftType:  TypeTimestamp,
			RightType: TypeTimestamp,
			fn:        cmpOpScalarLEFn,
		},
		CmpOp{
			LeftType:  TypeTimestampTZ,
			RightType: TypeTimestampTZ,
			fn:        cmpOpScalarLEFn,
		},
		CmpOp{
			LeftType:  TypeTimestampTZ,
			RightType: TypeTimestamp,
			fn:        cmpOpScalarLEFn,
		},
		CmpOp{
			LeftType:  TypeTimestamp,
			RightType: TypeTimestampTZ,
			fn:        cmpOpScalarLEFn,
		},
		CmpOp{
			LeftType:  TypeDate,
			RightType: TypeTimestampTZ,
			fn:        cmpOpScalarLEFn,
		},
		CmpOp{
			LeftType:  TypeDate,
			RightType: TypeTimestamp,
			fn:        cmpOpScalarLEFn,
		},
		CmpOp{
			LeftType:  TypeTimestampTZ,
			RightType: TypeDate,
			fn:        cmpOpScalarLEFn,
		},
		CmpOp{
			LeftType:  TypeTimestamp,
			RightType: TypeDate,
			fn:        cmpOpScalarLEFn,
		},
		CmpOp{
			LeftType:  TypeInterval,
			RightType: TypeInterval,
			fn:        cmpOpScalarLEFn,
		},
		CmpOp{
			LeftType:  TypeTuple,
			RightType: TypeTuple,
			fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				return cmpOpTupleFn(ctx, *left.(*DTuple), *right.(*DTuple), LE), nil
			},
		},
	},

	In: {
		makeEvalTupleIn(TypeBool),
		makeEvalTupleIn(TypeInt),
		makeEvalTupleIn(TypeFloat),
		makeEvalTupleIn(TypeDecimal),
		makeEvalTupleIn(TypeString),
		makeEvalTupleIn(TypeCollatedString),
		makeEvalTupleIn(TypeBytes),
		makeEvalTupleIn(TypeDate),
		makeEvalTupleIn(TypeTimestamp),
		makeEvalTupleIn(TypeTimestampTZ),
		makeEvalTupleIn(TypeInterval),
		makeEvalTupleIn(TypeTuple),
		makeEvalTupleIn(TypeOid),
	},

	Like: {
		CmpOp{
			LeftType:  TypeString,
			RightType: TypeString,
			fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				return matchLike(ctx, left, right, false)
			},
		},
	},

	ILike: {
		CmpOp{
			LeftType:  TypeString,
			RightType: TypeString,
			fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				return matchLike(ctx, left, right, true)
			},
		},
	},

	SimilarTo: {
		CmpOp{
			LeftType:  TypeString,
			RightType: TypeString,
			fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				key := similarToKey(MustBeDString(right))
				return matchRegexpWithKey(ctx, left, key)
			},
		},
	},

	RegMatch: {
		CmpOp{
			LeftType:  TypeString,
			RightType: TypeString,
			fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				key := regexpKey{s: string(MustBeDString(right)), caseInsensitive: false}
				return matchRegexpWithKey(ctx, left, key)
			},
		},
	},

	RegIMatch: {
		CmpOp{
			LeftType:  TypeString,
			RightType: TypeString,
			fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				key := regexpKey{s: string(MustBeDString(right)), caseInsensitive: true}
				return matchRegexpWithKey(ctx, left, key)
			},
		},
	},
}

func isNaN(d Datum) bool {
	switch t := d.(type) {
	case *DFloat:
		return math.IsNaN(float64(*t))
	case *DDecimal:
		return t.Decimal.Form == apd.NaN
	default:
		return false
	}
}

func boolFromCmp(cmp int, op ComparisonOperator) *DBool {
	switch op {
	case EQ:
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
		// If either Datum is NULL, the result of the comparison is NULL.
		return DNull
	}
	if isNaN(left) || isNaN(right) {
		// If either Datum is NaN, the result of the comparison is False.
		return DBoolFalse
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

func cmpOpTupleFn(ctx *EvalContext, left, right DTuple, op ComparisonOperator) Datum {
	cmp := 0
	for i, leftElem := range left.D {
		rightElem := right.D[i]
		// Like with cmpOpScalarFn, check for values that need to be handled
		// differently than when ordering Datums.
		if leftElem == DNull || rightElem == DNull {
			// If either Datum is NULL, the result of the comparison is NULL.
			return DNull
		}
		if isNaN(leftElem) || isNaN(rightElem) {
			// If either Datum is NaN, the result of the comparison is False.
			return DBoolFalse
		}
		cmp = leftElem.Compare(ctx, rightElem)
		if cmp != 0 {
			break
		}
	}
	return boolFromCmp(cmp, op)
}

func makeEvalTupleIn(typ Type) CmpOp {
	return CmpOp{
		LeftType:  typ,
		RightType: TypeTuple,
		fn: func(ctx *EvalContext, arg, values Datum) (Datum, error) {
			if arg == DNull {
				return DBoolFalse, nil
			}

			// If the tuple was sorted during normalization, we can perform
			// an efficient binary search to find if the arg is in the tuple.
			vtuple := values.(*DTuple)
			if vtuple.Sorted {
				_, found := vtuple.SearchSorted(ctx, arg)
				if !found && len(vtuple.D) > 0 && vtuple.D[0] == DNull {
					// If the tuple contained any null elements and no matches are found,
					// the result of IN will be null. The null element will at the front
					// if the tuple is sorted because null is less than any non-null value.
					return DNull, nil
				}
				return MakeDBool(DBool(found)), nil
			}

			// If the tuple was not sorted, which happens in cases where it
			// is not constant across rows, then we must fall back to iterating
			// through the entire tuple.
			sawNull := false
			for _, val := range vtuple.D {
				if val == DNull {
					sawNull = true
				} else if val.Compare(ctx, arg) == 0 {
					return DBoolTrue, nil
				}
			}
			if sawNull {
				// If the tuple contains any null elements and no matches are found, the
				// result of IN will be null.
				return DNull, nil
			}
			return DBoolFalse, nil
		},
	}
}

// evalArrayCmp evaluates the array comparison using the provided sub-operator type
// and its CmpOp with the left Datum and the right array of Datums.
//
// For example, given 1 < ANY (ARRAY[1, 2, 3]), evalArrayCmp would be called with:
//   evalArrayCmp(ctx, LT, CmpOp(LT, leftType, rightParamType), leftDatum, rightArray).
func evalArrayCmp(
	ctx *EvalContext, subOp ComparisonOperator, fn CmpOp, left Datum, right *DArray, all bool,
) (Datum, error) {
	allTrue := true
	anyTrue := false
	sawNull := false
	for _, elem := range right.Array {
		if elem == DNull {
			sawNull = true
			continue
		}

		_, newLeft, newRight, _, not := foldComparisonExpr(subOp, left, elem)
		d, err := fn.fn(ctx, newLeft.(Datum), newRight.(Datum))
		if err != nil {
			return nil, err
		}
		if d == DNull {
			sawNull = true
			continue
		}
		b := d.(*DBool)
		res := *b != DBool(not)
		if res {
			anyTrue = true
		} else {
			allTrue = false
		}
	}

	if all {
		if !allTrue {
			return DBoolFalse, nil
		}
		if sawNull {
			// If the right-hand array contains any null elements and no false
			// comparison result is obtained, the result of ALL will be null.
			return DNull, nil
		}
		// allTrue && !sawNull
		return DBoolTrue, nil
	}

	// !all
	if anyTrue {
		return DBoolTrue, nil
	}
	if sawNull {
		// If the right-hand array contains any null elements and no true
		// comparison result is obtained, the result of ANY will be null.
		return DNull, nil
	}
	// !anyTrue && !sawNull
	return DBoolFalse, nil
}

func matchLike(ctx *EvalContext, left, right Datum, caseInsensitive bool) (Datum, error) {
	pattern := string(MustBeDString(right))
	like := optimizedLikeFunc(pattern, caseInsensitive)
	if like == nil {
		key := likeKey{s: pattern, caseInsensitive: caseInsensitive}
		re, err := ctx.ReCache.GetRegexp(key)
		if err != nil {
			return DBoolFalse, fmt.Errorf("LIKE regexp compilation failed: %v", err)
		}
		like = re.MatchString
	}
	return MakeDBool(DBool(like(string(MustBeDString(left))))), nil
}

func matchRegexpWithKey(ctx *EvalContext, str Datum, key regexpCacheKey) (Datum, error) {
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

// EvalPlanner is a limited planner that can be used from EvalContext.
type EvalPlanner interface {
	// QueryRow executes a SQL query string where exactly 1 result row is
	// expected and returns that row.
	QueryRow(ctx context.Context, sql string, args ...interface{}) (Datums, error)

	// QualifyWithDatabase resolves a possibly unqualified table name into a
	// normalized table name that is qualified by database.
	QualifyWithDatabase(ctx context.Context, t *NormalizableTableName) (*TableName, error)
}

// contextHolder is a wrapper that returns a Context.
type contextHolder func() context.Context

// EvalContext defines the context in which to evaluate an expression, allowing
// the retrieval of state such as the node ID or statement start time.
//
// ATTENTION: Fields from this struct are also represented in
// distsqlrun.EvalContext. Make sure to keep the two in sync.
// TODO(andrei): remove or limit the duplication.
type EvalContext struct {
	NodeID roachpb.NodeID
	// The statement timestamp. May be different for every statement.
	// Used for statement_timestamp().
	stmtTimestamp time.Time
	// The transaction timestamp. Needs to stay stable for the lifetime
	// of a transaction. Used for now(), current_timestamp(),
	// transaction_timestamp() and the like.
	txnTimestamp time.Time
	// The cluster timestamp. Needs to be stable for the lifetime of the
	// transaction. Used for cluster_logical_timestamp().
	clusterTimestamp hlc.Timestamp
	// Location references the *Location on the current Session.
	Location **time.Location
	// Database is the database in the current Session.
	Database string
	// SearchPath is the search path for databases used when encountering an
	// unqualified table name. Names in the search path are normalized already.
	// This must not be modified (this is shared from the session).
	SearchPath SearchPath
	// Ctx represents the context in which the expression is evaluated. This will
	// point to the Session's context container.
	// NOTE: seems a bit lazy to hold a pointer to the session's context here,
	// instead of making sure the right context is explicitly set before the
	// EvalContext is used. But there's already precedent with the Location field,
	// and also at the time of writing, EvalContexts are initialized with the
	// planner and not mutated.
	Ctx contextHolder

	Planner EvalPlanner

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

	Mon *mon.MemoryMonitor
}

// MakeTestingEvalContext returns an EvalContext that includes a MemoryMonitor.
func MakeTestingEvalContext() EvalContext {
	ctx := EvalContext{}
	monitor := mon.MakeMonitor(
		"test-monitor",
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
	)
	monitor.Start(context.Background(), nil, mon.MakeStandaloneBudget(math.MaxInt64))
	ctx.Mon = &monitor
	ctx.Ctx = context.Background
	now := timeutil.Now()
	ctx.SetTxnTimestamp(now)
	ctx.SetStmtTimestamp(now)
	ctx.SetClusterTimestamp(hlc.Timestamp{WallTime: now.Unix()})
	return ctx
}

// GetStmtTimestamp retrieves the current statement timestamp as per
// the evaluation context. The timestamp is guaranteed to be nonzero.
func (ctx *EvalContext) GetStmtTimestamp() time.Time {
	// TODO(knz): a zero timestamp should never be read, even during
	// Prepare. This will need to be addressed.
	if !ctx.PrepareOnly && ctx.stmtTimestamp.IsZero() {
		panic("zero statement timestamp in EvalContext")
	}
	return ctx.stmtTimestamp
}

// GetClusterTimestamp retrieves the current cluster timestamp as per
// the evaluation context. The timestamp is guaranteed to be nonzero.
func (ctx *EvalContext) GetClusterTimestamp() *DDecimal {
	// TODO(knz): a zero timestamp should never be read, even during
	// Prepare. This will need to be addressed.
	if !ctx.PrepareOnly && ctx.clusterTimestamp == (hlc.Timestamp{}) {
		panic("zero cluster timestamp in EvalContext")
	}

	return TimestampToDecimal(ctx.clusterTimestamp)
}

// GetClusterTimestampRaw exposes the clusterTimestamp field. Also see
// GetClusterTimestamp().
func (ctx *EvalContext) GetClusterTimestampRaw() hlc.Timestamp {
	if !ctx.PrepareOnly && ctx.clusterTimestamp == (hlc.Timestamp{}) {
		panic("zero cluster timestamp in EvalContext")
	}
	return ctx.clusterTimestamp
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

// GetTxnTimestamp retrieves the current transaction timestamp as per
// the evaluation context. The timestamp is guaranteed to be nonzero.
func (ctx *EvalContext) GetTxnTimestamp(precision time.Duration) *DTimestampTZ {
	// TODO(knz): a zero timestamp should never be read, even during
	// Prepare. This will need to be addressed.
	if !ctx.PrepareOnly && ctx.txnTimestamp.IsZero() {
		panic("zero transaction timestamp in EvalContext")
	}
	return MakeDTimestampTZ(ctx.txnTimestamp, precision)
}

// GetTxnTimestampNoZone retrieves the current transaction timestamp as per
// the evaluation context. The timestamp is guaranteed to be nonzero.
func (ctx *EvalContext) GetTxnTimestampNoZone(precision time.Duration) *DTimestamp {
	// TODO(knz): a zero timestamp should never be read, even during
	// Prepare. This will need to be addressed.
	if !ctx.PrepareOnly && ctx.txnTimestamp.IsZero() {
		panic("zero transaction timestamp in EvalContext")
	}
	return MakeDTimestamp(ctx.txnTimestamp, precision)
}

// GetTxnTimestampRaw exposes the txnTimestamp field. Also see GetTxnTimestamp()
// and GetTxnTimestampNoZone().
func (ctx *EvalContext) GetTxnTimestampRaw() time.Time {
	if !ctx.PrepareOnly && ctx.txnTimestamp.IsZero() {
		panic("zero transaction timestamp in EvalContext")
	}
	return ctx.txnTimestamp
}

// SetTxnTimestamp sets the corresponding timestamp in the EvalContext.
func (ctx *EvalContext) SetTxnTimestamp(ts time.Time) {
	ctx.txnTimestamp = ts
}

// SetStmtTimestamp sets the corresponding timestamp in the EvalContext.
func (ctx *EvalContext) SetStmtTimestamp(ts time.Time) {
	ctx.stmtTimestamp = ts
}

// SetClusterTimestamp sets the corresponding timestamp in the EvalContext.
func (ctx *EvalContext) SetClusterTimestamp(ts hlc.Timestamp) {
	ctx.clusterTimestamp = ts
}

// GetLocation returns the session timezone.
func (ctx *EvalContext) GetLocation() *time.Location {
	if ctx.Location == nil || *ctx.Location == nil {
		return time.UTC
	}
	return *ctx.Location
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
	if left == DNull {
		return DNull, nil
	}
	right, err := expr.Right.(TypedExpr).Eval(ctx)
	if err != nil {
		return nil, err
	}
	if right == DNull {
		return DNull, nil
	}
	return expr.fn.fn(ctx, left, right)
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
}

// regTypeInfos maps an OidColType to a regTypeInfo that describes the
// pg_catalog table that contains the entities of the type of the key.
var regTypeInfos = map[*OidColType]regTypeInfo{
	oidColTypeRegClass:     {"pg_class", "relname", "relation"},
	oidColTypeRegType:      {"pg_type", "typname", "type"},
	oidColTypeRegProc:      {"pg_proc", "proname", "function"},
	oidColTypeRegProcedure: {"pg_proc", "proname", "function"},
	oidColTypeRegNamespace: {"pg_namespace", "nspname", "namespace"},
}

// queryOidWithJoin looks up the name or OID of an input OID or string in the
// pg_catalog table that the input OidColType belongs to. If the input Datum
// is a DOid, the relevant table will be queried by OID; if the input is a
// DString, the table will be queried by its name column.
//
// The return value is a fresh DOid of the input OidColType with name and OID
// set to the result of the query. If there was not exactly one result to the
// query, an error will be returned.
func queryOidWithJoin(
	ctx *EvalContext, typ *OidColType, d Datum, joinClause string, additionalWhere string,
) (*DOid, error) {
	ret := &DOid{kind: typ}
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
	results, err := ctx.Planner.QueryRow(
		ctx.Ctx(),
		fmt.Sprintf(
			"SELECT %s.oid, %s FROM pg_catalog.%s %s WHERE %s = $1 %s",
			info.tableName, info.nameCol, info.tableName, joinClause, queryCol, additionalWhere),
		d)
	if err != nil {
		if _, ok := err.(*MultipleResultsError); ok {
			return nil, errors.Errorf("more than one %s named %s", info.objName, d)
		}
		return nil, err
	}
	if results.Len() == 0 {
		return nil, errors.Errorf("%s %s does not exist", info.objName, d)
	}
	ret.DInt = results[0].(*DOid).DInt
	ret.name = AsStringWithFlags(results[1], FmtBareStrings)
	return ret, nil
}

func queryOid(ctx *EvalContext, typ *OidColType, d Datum) (*DOid, error) {
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
	d = UnwrapDatum(d)

	switch typ := expr.Type.(type) {
	case *BoolColType:
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

	case *IntColType:
		var res *DInt
		switch v := d.(type) {
		case *DBool:
			if *v {
				res = NewDInt(1)
			} else {
				res = DZero
			}
		case *DInt:
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
			_, err := DecimalCtx.ToIntegral(d, &v.Decimal)
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
			res = NewDInt(DInt(v.Nanos / 1000000000))
		case *DOid:
			res = &v.DInt
		}
		return res, nil

	case *FloatColType:
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
			micros := v.Nanos / 1000
			return NewDFloat(DFloat(float64(micros) / 1e-6)), nil
		}

	case *DecimalColType:
		switch v := d.(type) {
		case *DBool:
			dd := &DDecimal{}
			if *v {
				dd.SetCoefficient(1)
			}
			return dd, nil
		case *DInt:
			dd := &DDecimal{}
			dd.SetCoefficient(int64(*v))
			return dd, nil
		case *DDate:
			dd := &DDecimal{}
			dd.SetCoefficient(int64(*v))
			return dd, nil
		case *DFloat:
			dd := &DDecimal{}
			_, err := dd.SetFloat64(float64(*v))
			return dd, err
		case *DDecimal:
			return d, nil
		case *DString:
			return ParseDDecimal(string(*v))
		case *DCollatedString:
			return ParseDDecimal(v.Contents)
		case *DTimestamp:
			var res DDecimal
			val := &res.Coeff
			val.SetInt64(v.Unix())
			val.Mul(val, big10E6)
			micros := v.Nanosecond() / int(time.Microsecond)
			val.Add(val, big.NewInt(int64(micros)))
			res.Decimal.Exponent = -6
			return &res, nil
		case *DTimestampTZ:
			var res DDecimal
			val := &res.Coeff
			val.SetInt64(v.Unix())
			val.Mul(val, big10E6)
			micros := v.Nanosecond() / int(time.Microsecond)
			val.Add(val, big.NewInt(int64(micros)))
			res.Decimal.Exponent = -6
			return &res, nil
		case *DInterval:
			var res DDecimal
			val := &res.Coeff
			val.SetInt64(v.Nanos / 1000)
			res.Decimal.Exponent = -6
			return &res, nil
		}

	case *StringColType, *CollatedStringColType, *NameColType:
		var s string
		switch t := d.(type) {
		case *DBool, *DInt, *DFloat, *DDecimal, dNull:
			s = d.String()
		case *DTimestamp, *DTimestampTZ, *DDate:
			s = AsStringWithFlags(d, FmtBareStrings)
		case *DInterval:
			// When converting an interval to string, we need a string representation
			// of the duration (e.g. "5s") and not of the interval itself (e.g.
			// "INTERVAL '5s'").
			s = t.ValueAsString()
		case *DString:
			s = string(*t)
		case *DCollatedString:
			s = t.Contents
		case *DBytes:
			if !utf8.ValidString(string(*t)) {
				return nil, fmt.Errorf("invalid utf8: %q", string(*t))
			}
			s = string(*t)
		case *DOid:
			s = t.name
		}
		switch c := expr.Type.(type) {
		case *StringColType:
			// If the CHAR type specifies a limit we truncate to that limit:
			//   'hello'::CHAR(2) -> 'he'
			if c.N > 0 && c.N < len(s) {
				s = s[:c.N]
			}
			return NewDString(s), nil
		case *CollatedStringColType:
			if c.N > 0 && c.N < len(s) {
				s = s[:c.N]
			}
			return NewDCollatedString(s, c.Locale, &ctx.collationEnv), nil
		case *NameColType:
			return NewDName(s), nil
		default:
			panic(fmt.Sprintf("missing case for cast to string: %T", c))
		}

	case *BytesColType:
		switch t := d.(type) {
		case *DString:
			return NewDBytes(DBytes(*t)), nil
		case *DCollatedString:
			return NewDBytes(DBytes(t.Contents)), nil
		case *DBytes:
			return d, nil
		}

	case *DateColType:
		switch d := d.(type) {
		case *DString:
			return ParseDDate(string(*d), ctx.GetLocation())
		case *DCollatedString:
			return ParseDDate(d.Contents, ctx.GetLocation())
		case *DDate:
			return d, nil
		case *DInt:
			return NewDDate(DDate(int64(*d))), nil
		case *DTimestampTZ:
			return NewDDateFromTime(d.Time, ctx.GetLocation()), nil
		case *DTimestamp:
			return NewDDateFromTime(d.Time, time.UTC), nil
		}

	case *TimestampColType:
		// TODO(knz): Timestamp from float, decimal.
		switch d := d.(type) {
		case *DString:
			return ParseDTimestamp(string(*d), time.Microsecond)
		case *DCollatedString:
			return ParseDTimestamp(d.Contents, time.Microsecond)
		case *DDate:
			year, month, day := time.Unix(int64(*d)*secondsInDay, 0).UTC().Date()
			return MakeDTimestamp(time.Date(year, month, day, 0, 0, 0, 0, time.UTC), time.Microsecond), nil
		case *DInt:
			return MakeDTimestamp(time.Unix(int64(*d), 0).UTC(), time.Second), nil
		case *DTimestamp:
			return d, nil
		case *DTimestampTZ:
			return MakeDTimestamp(d.Time, time.Microsecond), nil
		}

	case *TimestampTZColType:
		// TODO(knz): TimestampTZ from float, decimal.
		switch d := d.(type) {
		case *DString:
			return ParseDTimestampTZ(string(*d), ctx.GetLocation(), time.Microsecond)
		case *DCollatedString:
			return ParseDTimestampTZ(d.Contents, ctx.GetLocation(), time.Microsecond)
		case *DDate:
			return MakeDTimestampTZFromDate(ctx.GetLocation(), d), nil
		case *DTimestamp:
			return MakeDTimestampTZ(d.Time, time.Microsecond), nil
		case *DInt:
			return MakeDTimestampTZ(time.Unix(int64(*d), 0).UTC(), time.Second), nil
		case *DTimestampTZ:
			return d, nil
		}

	case *IntervalColType:
		// TODO(knz): Interval from float, decimal.
		switch v := d.(type) {
		case *DString:
			return ParseDInterval(string(*v))
		case *DCollatedString:
			return ParseDInterval(v.Contents)
		case *DInt:
			// An integer duration represents a duration in microseconds.
			return &DInterval{Duration: duration.Duration{Nanos: int64(*v) * 1000}}, nil
		case *DInterval:
			return d, nil
		}
	case *OidColType:
		switch v := d.(type) {
		case *DOid:
			switch typ {
			case oidColTypeOid:
				return &DOid{kind: typ, DInt: v.DInt}, nil
			default:
				oid, err := queryOid(ctx, typ, v)
				if err != nil {
					oid = NewDOid(v.DInt)
					oid.kind = typ
				}
				return oid, nil
			}
		case *DInt:
			switch typ {
			case oidColTypeOid:
				return &DOid{kind: typ, DInt: *v}, nil
			default:
				tmpOid := NewDOid(*v)
				oid, err := queryOid(ctx, typ, tmpOid)
				if err != nil {
					oid = tmpOid
					oid.kind = typ
				}
				return oid, nil
			}
		case *DString:
			s := string(*v)
			// Trim whitespace and unwrap outer quotes if necessary.
			// This is required to mimic postgres.
			s = strings.TrimSpace(s)
			if len(s) > 1 && s[0] == '"' && s[len(s)-1] == '"' {
				s = s[1 : len(s)-1]
			}

			switch typ {
			case oidColTypeOid:
				i, err := ParseDInt(s)
				if err != nil {
					return nil, err
				}
				return &DOid{kind: typ, DInt: *i}, nil
			case oidColTypeRegProc, oidColTypeRegProcedure:
				// Trim procedure type parameters, e.g. `max(int)` becomes `max`.
				// Postgres only does this when the cast is ::regprocedure, but we're
				// going to always do it.
				// We additionally do not yet implement disambiguation based on type
				// parameters: we return the match iff there is exactly one.
				s = pgSignatureRegexp.ReplaceAllString(s, "$1")
				// Resolve function name.
				substrs := strings.Split(s, ".")
				name := UnresolvedName{}
				for _, substr := range substrs {
					name = append(name, Name(substr))
				}
				funcDef, err := name.ResolveFunction(ctx.SearchPath)
				if err != nil {
					return nil, err
				}
				return queryOid(ctx, typ, NewDString(funcDef.Name))
			case oidColTypeRegType:
				colType, err := ParseType(s)
				if err == nil {
					datumType := CastTargetToDatumType(colType)
					return &DOid{kind: typ, DInt: DInt(datumType.Oid()), name: datumType.SQLName()}, nil
				}
				// Fall back to searching pg_type, since we don't provide syntax for
				// every postgres type that we understand OIDs for.
				// Trim type modifiers, e.g. `numeric(10,3)` becomes `numeric`.
				s = pgSignatureRegexp.ReplaceAllString(s, "$1")
				return queryOid(ctx, typ, NewDString(s))

			case oidColTypeRegClass:
				// Resolving a table name requires looking at the search path to
				// determine the database that owns it.
				t := &NormalizableTableName{
					TableNameReference: UnresolvedName{
						Name(s),
					}}
				tn, err := ctx.Planner.QualifyWithDatabase(ctx.Ctx(), t)
				if err != nil {
					return nil, err
				}
				// Determining the table's OID requires joining against the databases
				// table because we only have the database name, not its OID, which is
				// what is stored in pg_class. This extra join means we can't use
				// queryOid like everyone else.
				return queryOidWithJoin(ctx, typ, NewDString(tn.Table()),
					"JOIN pg_catalog.pg_namespace ON relnamespace = pg_namespace.oid",
					fmt.Sprintf("AND nspname = '%s'", tn.Database()))
			default:
				return queryOid(ctx, typ, NewDString(s))
			}
		}
	}

	return nil, fmt.Errorf("invalid cast: %s -> %s", d.ResolvedType(), expr.Type)
}

// Eval implements the TypedExpr interface.
func (expr *IndirectionExpr) Eval(ctx *EvalContext) (Datum, error) {
	var subscriptIdx int
	for i, t := range expr.Indirection {
		if t.Slice {
			return nil, util.UnimplementedWithIssueErrorf(2115, "ARRAY slicing in %s", expr)
		}
		if i > 0 {
			return nil, util.UnimplementedWithIssueErrorf(2115, "multidimensional ARRAY %s", expr)
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

	// INT2VECTOR uses 0-indexing.
	if w, ok := d.(*DOidWrapper); ok && w.Oid == oid.T_int2vector {
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
		return DNull, err
	}
	switch d := UnwrapDatum(d).(type) {
	case *DString:
		return NewDCollatedString(string(*d), expr.Locale, &ctx.collationEnv), nil
	case *DCollatedString:
		return NewDCollatedString(d.Contents, expr.Locale, &ctx.collationEnv), nil
	default:
		panic(fmt.Sprintf("invalid argument to COLLATE: %s", d))
	}
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

	if left == DNull || right == DNull {
		switch expr.Operator {
		case IsDistinctFrom:
			return MakeDBool(!DBool(left == DNull && right == DNull)), nil
		case IsNotDistinctFrom:
			return MakeDBool(left == DNull && right == DNull), nil
		case Is:
			// IS and IS NOT can compare against NULL.
			return MakeDBool(left == right), nil
		case IsNot:
			return MakeDBool(left != right), nil
		default:
			return DNull, nil
		}
	}

	op := expr.Operator
	if op.hasSubOperator() {
		return evalArrayCmp(ctx, expr.SubOperator, expr.fn, left, MustBeDArray(right), op == All)
	}

	_, newLeft, newRight, _, not := foldComparisonExpr(op, left, right)
	d, err := expr.fn.fn(ctx, newLeft.(Datum), newRight.(Datum))
	if err != nil {
		return nil, err
	}
	if b, ok := d.(*DBool); ok {
		return MakeDBool(*b != DBool(not)), nil
	}
	return d, nil
}

// Eval implements the TypedExpr interface.
func (t *ExistsExpr) Eval(ctx *EvalContext) (Datum, error) {
	// Exists expressions are handled during subquery expansion.
	return nil, errors.Errorf("unhandled type %T", t)
}

// Eval implements the TypedExpr interface.
func (expr *FuncExpr) Eval(ctx *EvalContext) (Datum, error) {
	args := NewDTupleWithCap(len(expr.Exprs))
	for _, e := range expr.Exprs {
		arg, err := e.(TypedExpr).Eval(ctx)
		if err != nil {
			return nil, err
		}
		args.D = append(args.D, arg)
	}

	if !expr.fn.Types.match([]Type(args.ResolvedType().(TTuple))) {
		// The argument types no longer match the memoized function. This happens
		// when a non-NULL argument becomes NULL and the function does not support
		// NULL arguments. For example, "SELECT LOWER(col) FROM TABLE" where col is
		// nullable. The SELECT does not error, but returns a NULL value for that
		// select expression.
		return DNull, nil
	}

	res, err := expr.fn.fn(ctx, args.D)
	if err != nil {
		// If we are facing a retry error, in particular those generated
		// by crdb_internal.force_retry(), propagate it unchanged, so that
		// the executor can see it with the right type.
		if _, ok := err.(*roachpb.HandledRetryableTxnError); ok {
			return nil, err
		}
		return nil, fmt.Errorf("%s(): %v", expr.Func, err)
	}
	return res, nil
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
		wantTyp := CastTargetToDatumType(t)
		if datumTyp.FamilyEqual(wantTyp) {
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
	return nil, errors.Errorf("unhandled type %T", expr)
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
	return expr.fn.fn(ctx, d)
}

// Eval implements the TypedExpr interface.
func (expr DefaultVal) Eval(ctx *EvalContext) (Datum, error) {
	return nil, errors.Errorf("unhandled type %T", expr)
}

// Eval implements the TypedExpr interface.
func (expr UnqualifiedStar) Eval(ctx *EvalContext) (Datum, error) {
	return nil, errors.Errorf("unhandled type %T", expr)
}

// Eval implements the TypedExpr interface.
func (expr UnresolvedName) Eval(ctx *EvalContext) (Datum, error) {
	return nil, errors.Errorf("unhandled type %T", expr)
}

// Eval implements the TypedExpr interface.
func (expr *AllColumnsSelector) Eval(ctx *EvalContext) (Datum, error) {
	return nil, errors.Errorf("unhandled type %T", expr)
}

// Eval implements the TypedExpr interface.
func (expr *ColumnItem) Eval(ctx *EvalContext) (Datum, error) {
	return nil, errors.Errorf("unhandled type %T", expr)
}

// Eval implements the TypedExpr interface.
func (t *Tuple) Eval(ctx *EvalContext) (Datum, error) {
	tuple := NewDTupleWithCap(len(t.Exprs))
	for _, v := range t.Exprs {
		d, err := v.(TypedExpr).Eval(ctx)
		if err != nil {
			return nil, err
		}
		tuple.D = append(tuple.D, d)
	}
	return tuple, nil
}

// arrayOfType returns a fresh DArray of the input type.
func arrayOfType(typ Type) (*DArray, error) {
	arrayTyp, ok := typ.(TArray)
	if !ok {
		return nil, errors.Errorf("array node type (%v) is not TArray", typ)
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
		return nil, errors.Errorf("array subquery result (%v) is not DTuple", d)
	}
	array.Array = tuple.D
	return array, nil
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
func (t *DDate) Eval(_ *EvalContext) (Datum, error) {
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
func (t *DTable) Eval(_ *EvalContext) (Datum, error) {
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
func (node *Placeholder) Eval(_ *EvalContext) (Datum, error) {
	return nil, fmt.Errorf("no value provided for placeholder: $%s", node.Name)
}

func evalComparison(ctx *EvalContext, op ComparisonOperator, left, right Datum) (Datum, error) {
	if left == DNull || right == DNull {
		return DNull, nil
	}
	ltype := left.ResolvedType()
	rtype := right.ResolvedType()
	if fn, ok := CmpOps[op].lookupImpl(ltype, rtype); ok {
		return fn.fn(ctx, left, right)
	}
	return nil, fmt.Errorf("unsupported comparison operator: <%s> %s <%s>", ltype, op, rtype)
}

// foldComparisonExpr folds a given comparison operation and its expressions
// into an equivalent operation that will hit in the cmpOps map, returning
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
		// IsDistinctFrom(left, right) is implemented as !EQ(left, right)
		//
		// Note the special handling of NULLs and IS DISTINCT FROM is needed
		// before this expression fold.
		return EQ, left, right, false, true
	case IsNotDistinctFrom:
		// IsNotDistinctFrom(left, right) is implemented as EQ(left, right)
		//
		// Note the special handling of NULLs and IS NOT DISTINCT FROM is needed
		// before this expression fold.
		return EQ, left, right, false, false
	case Is:
		// Is(left, right) is implemented as EQ(left, right)
		//
		// Note the special handling of NULLs and IS is needed before this
		// expression fold.
		return EQ, left, right, false, false
	case IsNot:
		// IsNot(left, right) is implemented as !EQ(left, right)
		//
		// Note the special handling of NULLs and IS NOT is needed before this
		// expression fold.
		return EQ, left, right, false, true
	}
	return op, left, right, false, false
}

// Simplifies LIKE/ILIKE expressions that do not need full regular expressions to
// evaluate the condition. For example, when the expression is just checking to see
// if a string starts with a given pattern.
func optimizedLikeFunc(pattern string, caseInsensitive bool) func(string) bool {
	switch len(pattern) {
	case 0:
		return func(s string) bool {
			return s == ""
		}
	case 1:
		switch pattern[0] {
		case '%':
			return func(s string) bool {
				return true
			}
		case '_':
			return func(s string) bool {
				return len(s) == 1
			}
		}
	default:
		if !strings.ContainsAny(pattern[1:len(pattern)-1], "_%") {
			// Cases like "something\%" are not optimized, but this does not affect correctness.
			anyEnd := pattern[len(pattern)-1] == '%' && pattern[len(pattern)-2] != '\\'
			anyStart := pattern[0] == '%'
			switch {
			case anyEnd && anyStart:
				return func(s string) bool {
					substr := pattern[1 : len(pattern)-1]
					if caseInsensitive {
						s, substr = strings.ToUpper(s), strings.ToUpper(substr)
					}
					return strings.Contains(s, substr)
				}
			case anyEnd:
				return func(s string) bool {
					prefix := pattern[:len(pattern)-1]
					if caseInsensitive {
						s, prefix = strings.ToUpper(s), strings.ToUpper(prefix)
					}
					return strings.HasPrefix(s, prefix)
				}
			case anyStart:
				return func(s string) bool {
					suffix := pattern[1:]
					if caseInsensitive {
						s, suffix = strings.ToUpper(s), strings.ToUpper(suffix)
					}
					return strings.HasSuffix(s, suffix)
				}
			}
		}
	}
	return nil
}

type likeKey struct {
	s               string
	caseInsensitive bool
}

func (k likeKey) pattern() (string, error) {
	pattern := regexp.QuoteMeta(k.s)
	// Replace LIKE/ILIKE specific wildcards with standard wildcards
	pattern = strings.Replace(pattern, "%", ".*", -1)
	pattern = strings.Replace(pattern, "_", ".", -1)
	return anchorPattern(pattern, k.caseInsensitive), nil
}

type similarToKey string

func (k similarToKey) pattern() (string, error) {
	pattern := SimilarEscape(string(k))
	return anchorPattern(pattern, false), nil
}

type regexpKey struct {
	s               string
	caseInsensitive bool
}

func (k regexpKey) pattern() (string, error) {
	if k.caseInsensitive {
		return caseInsensitive(k.s), nil
	}
	return k.s, nil
}

// SimilarEscape converts a SQL:2008 regexp pattern to POSIX style, so it can
// be used by our regexp engine.
func SimilarEscape(pattern string) string {
	return similarEscapeCustomChar(pattern, '\\')
}

// similarEscapeCustomChar converts a SQL:2008 regexp pattern to POSIX style,
// so it can be used by our regexp engine. This version of the function allows
// for a custom escape character.
func similarEscapeCustomChar(pattern string, escapeChar rune) string {
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
			} else {
				patternBuilder = append(patternBuilder, '\\', c)
			}
			afterEscape = false
		case utf8.ValidRune(escapeChar) && c == escapeChar:
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
	leftType, rightType Type,
) (func(*EvalContext, Datum, Datum) (Datum, error), bool) {
	fn, found := CmpOps[EQ].lookupImpl(leftType, rightType)
	if found {
		return fn.fn, true
	}
	return nil, false
}
