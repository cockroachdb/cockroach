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
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/net/context"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/decimal"
	"github.com/cockroachdb/cockroach/util/duration"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/pkg/errors"
)

var (
	errZeroModulus     = errors.New("zero modulus")
	errDivByZero       = errors.New("division by zero")
	errIntOutOfRange   = errors.New("integer out of range")
	errFloatOutOfRange = errors.New("float out of range")
)

// secondsInDay is the number of seconds in a day.
const secondsInDay = 24 * 60 * 60

// UnaryOp is a unary operator.
type UnaryOp struct {
	Typ        Datum
	ReturnType Datum
	fn         func(*EvalContext, Datum) (Datum, error)
	types      typeList
}

func (op UnaryOp) params() typeList {
	return op.types
}

func (op UnaryOp) returnType() Datum {
	return op.ReturnType
}

func init() {
	for op, overload := range UnaryOps {
		for i, impl := range overload {
			impl.types = SingleType{impl.Typ}
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
				return NewDInt(-*d.(*DInt)), nil
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
				dec := &d.(*DDecimal).Dec
				dd := &DDecimal{}
				dd.Neg(dec)
				return dd, nil
			},
		},
	},

	UnaryComplement: {
		UnaryOp{
			Typ:        TypeInt,
			ReturnType: TypeInt,
			fn: func(_ *EvalContext, d Datum) (Datum, error) {
				return NewDInt(^*d.(*DInt)), nil
			},
		},
	},
}

// BinOp is a binary operator.
type BinOp struct {
	LeftType   Datum
	RightType  Datum
	ReturnType Datum
	fn         func(*EvalContext, Datum, Datum) (Datum, error)
	types      typeList
}

func (op BinOp) params() typeList {
	return op.types
}

func (op BinOp) matchParams(l, r Datum) bool {
	return op.params().matchAt(l, 0) && op.params().matchAt(r, 1)
}

func (op BinOp) returnType() Datum {
	return op.ReturnType
}

func init() {
	for op, overload := range BinOps {
		for i, impl := range overload {
			impl.types = ArgTypes{impl.LeftType, impl.RightType}
			BinOps[op][i] = impl
		}
	}
}

// binOpOverload is an overloaded set of binary operator implementations.
type binOpOverload []BinOp

func (o binOpOverload) lookupImpl(left, right Datum) (BinOp, bool) {
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
				return NewDInt(*left.(*DInt) & *right.(*DInt)), nil
			},
		},
	},

	Bitor: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeInt,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDInt(*left.(*DInt) | *right.(*DInt)), nil
			},
		},
	},

	Bitxor: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeInt,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDInt(*left.(*DInt) ^ *right.(*DInt)), nil
			},
		},
	},

	// TODO(pmattis): Overflow/underflow checks?

	Plus: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeInt,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDInt(*left.(*DInt) + *right.(*DInt)), nil
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
				l := &left.(*DDecimal).Dec
				r := &right.(*DDecimal).Dec
				dd := &DDecimal{}
				dd.Add(l, r)
				return dd, nil
			},
		},
		BinOp{
			LeftType:   TypeDecimal,
			RightType:  TypeInt,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Dec
				r := *right.(*DInt)
				dd := &DDecimal{}
				dd.SetUnscaled(int64(r))
				dd.Add(l, &dd.Dec)
				return dd, nil
			},
		},
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := *left.(*DInt)
				r := &right.(*DDecimal).Dec
				dd := &DDecimal{}
				dd.SetUnscaled(int64(l))
				dd.Add(&dd.Dec, r)
				return dd, nil
			},
		},
		BinOp{
			LeftType:   TypeDate,
			RightType:  TypeInt,
			ReturnType: TypeDate,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDDate(*left.(*DDate) + DDate(*right.(*DInt))), nil
			},
		},
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeDate,
			ReturnType: TypeDate,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDDate(DDate(*left.(*DInt)) + *right.(*DDate)), nil
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
	},

	Minus: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeInt,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDInt(*left.(*DInt) - *right.(*DInt)), nil
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
				l := &left.(*DDecimal).Dec
				r := &right.(*DDecimal).Dec
				dd := &DDecimal{}
				dd.Sub(l, r)
				return dd, nil
			},
		},
		BinOp{
			LeftType:   TypeDecimal,
			RightType:  TypeInt,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Dec
				r := *right.(*DInt)
				dd := &DDecimal{}
				dd.SetUnscaled(int64(r))
				dd.Sub(l, &dd.Dec)
				return dd, nil
			},
		},
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := *left.(*DInt)
				r := &right.(*DDecimal).Dec
				dd := &DDecimal{}
				dd.SetUnscaled(int64(l))
				dd.Sub(&dd.Dec, r)
				return dd, nil
			},
		},
		BinOp{
			LeftType:   TypeDate,
			RightType:  TypeInt,
			ReturnType: TypeDate,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDDate(*left.(*DDate) - DDate(*right.(*DInt))), nil
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
				return NewDInt(*left.(*DInt) * *right.(*DInt)), nil
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
				l := &left.(*DDecimal).Dec
				r := &right.(*DDecimal).Dec
				dd := &DDecimal{}
				dd.Mul(l, r)
				return dd, nil
			},
		},
		// The following two overloads are needed becauase DInt/DInt = DDecimal. Due to this
		// operation, normalization may sometimes create a DInt * DDecimal operation.
		BinOp{
			LeftType:   TypeDecimal,
			RightType:  TypeInt,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Dec
				r := *right.(*DInt)
				dd := &DDecimal{}
				dd.SetUnscaled(int64(r))
				dd.Mul(l, &dd.Dec)
				return dd, nil
			},
		},
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := *left.(*DInt)
				r := &right.(*DDecimal).Dec
				dd := &DDecimal{}
				dd.SetUnscaled(int64(l))
				dd.Mul(&dd.Dec, r)
				return dd, nil
			},
		},
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInterval,
			ReturnType: TypeInterval,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return &DInterval{Duration: right.(*DInterval).Duration.Mul(int64(*left.(*DInt)))}, nil
			},
		},
		BinOp{
			LeftType:   TypeInterval,
			RightType:  TypeInt,
			ReturnType: TypeInterval,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return &DInterval{Duration: left.(*DInterval).Duration.Mul(int64(*right.(*DInt)))}, nil
			},
		},
	},

	Div: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeDecimal,
			fn: func(ctx *EvalContext, left Datum, right Datum) (Datum, error) {
				rInt := *right.(*DInt)
				if rInt == 0 {
					return nil, errDivByZero
				}
				div := ctx.getTmpDec().SetUnscaled(int64(rInt)).SetScale(0)
				dd := &DDecimal{}
				dd.SetUnscaled(int64(*left.(*DInt)))
				dd.QuoRound(&dd.Dec, div, decimal.Precision, inf.RoundHalfUp)
				return dd, nil
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
				l := &left.(*DDecimal).Dec
				r := &right.(*DDecimal).Dec
				if r.Sign() == 0 {
					return nil, errDivByZero
				}
				dd := &DDecimal{}
				dd.QuoRound(l, r, decimal.Precision, inf.RoundHalfUp)
				return dd, nil
			},
		},
		BinOp{
			LeftType:   TypeDecimal,
			RightType:  TypeInt,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Dec
				r := *right.(*DInt)
				if r == 0 {
					return nil, errDivByZero
				}
				dd := &DDecimal{}
				dd.SetUnscaled(int64(r))
				dd.QuoRound(l, &dd.Dec, decimal.Precision, inf.RoundHalfUp)
				return dd, nil
			},
		},
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := *left.(*DInt)
				r := &right.(*DDecimal).Dec
				if r.Sign() == 0 {
					return nil, errDivByZero
				}
				dd := &DDecimal{}
				dd.SetUnscaled(int64(l))
				dd.QuoRound(&dd.Dec, r, decimal.Precision, inf.RoundHalfUp)
				return dd, nil
			},
		},
		BinOp{
			LeftType:   TypeInterval,
			RightType:  TypeInt,
			ReturnType: TypeInterval,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				rInt := *right.(*DInt)
				if rInt == 0 {
					return nil, errDivByZero
				}
				return &DInterval{Duration: left.(*DInterval).Duration.Div(int64(rInt))}, nil
			},
		},
	},

	FloorDiv: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeInt,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				rInt := *right.(*DInt)
				if rInt == 0 {
					return nil, errDivByZero
				}
				return NewDInt(*left.(*DInt) / rInt), nil
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
				l := left.(*DDecimal).Dec
				r := right.(*DDecimal).Dec
				if r.Sign() == 0 {
					return nil, errZeroModulus
				}
				dd := &DDecimal{}
				dd.QuoRound(&l, &r, 0, inf.RoundDown)
				return dd, nil
			},
		},
		BinOp{
			LeftType:   TypeDecimal,
			RightType:  TypeInt,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Dec
				r := *right.(*DInt)
				if r == 0 {
					return nil, errDivByZero
				}
				dd := &DDecimal{}
				dd.SetUnscaled(int64(r))
				dd.QuoRound(l, &dd.Dec, 0, inf.RoundDown)
				return dd, nil
			},
		},
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := *left.(*DInt)
				r := &right.(*DDecimal).Dec
				if r.Sign() == 0 {
					return nil, errDivByZero
				}
				dd := &DDecimal{}
				dd.SetUnscaled(int64(l))
				dd.QuoRound(&dd.Dec, r, 0, inf.RoundDown)
				return dd, nil
			},
		},
	},

	Mod: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeInt,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				r := *right.(*DInt)
				if r == 0 {
					return nil, errZeroModulus
				}
				return NewDInt(*left.(*DInt) % r), nil
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
				l := &left.(*DDecimal).Dec
				r := &right.(*DDecimal).Dec
				if r.Sign() == 0 {
					return nil, errZeroModulus
				}
				dd := &DDecimal{}
				decimal.Mod(&dd.Dec, l, r)
				return dd, nil
			},
		},
		BinOp{
			LeftType:   TypeDecimal,
			RightType:  TypeInt,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Dec
				r := *right.(*DInt)
				if r == 0 {
					return nil, errZeroModulus
				}
				dd := &DDecimal{}
				dd.SetUnscaled(int64(r))
				decimal.Mod(&dd.Dec, l, &dd.Dec)
				return dd, nil
			},
		},
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				l := *left.(*DInt)
				r := &right.(*DDecimal).Dec
				if r.Sign() == 0 {
					return nil, errZeroModulus
				}
				dd := &DDecimal{}
				dd.SetUnscaled(int64(l))
				decimal.Mod(&dd.Dec, &dd.Dec, r)
				return dd, nil
			},
		},
	},

	Concat: {
		BinOp{
			LeftType:   TypeString,
			RightType:  TypeString,
			ReturnType: TypeString,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDString(string(*left.(*DString) + *right.(*DString))), nil
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
				return NewDInt(*left.(*DInt) << uint(*right.(*DInt))), nil
			},
		},
	},

	RShift: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeInt,
			fn: func(_ *EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDInt(*left.(*DInt) >> uint(*right.(*DInt))), nil
			},
		},
	},
}

var timestampMinusBinOp BinOp

func init() {
	timestampMinusBinOp, _ = BinOps[Minus].lookupImpl(TypeTimestamp, TypeTimestamp)
}

// CmpOp is a comparison operator.
type CmpOp struct {
	LeftType  Datum
	RightType Datum
	fn        func(*EvalContext, Datum, Datum) (DBool, error)
	types     typeList
}

func (op CmpOp) params() typeList {
	return op.types
}

func (op CmpOp) matchParams(l, r Datum) bool {
	return op.params().matchAt(l, 0) && op.params().matchAt(r, 1)
}

func (op CmpOp) returnType() Datum {
	return TypeBool
}

func init() {
	for op, overload := range CmpOps {
		for i, impl := range overload {
			impl.types = ArgTypes{impl.LeftType, impl.RightType}
			CmpOps[op][i] = impl
		}
	}
}

// cmpOpOverload is an overloaded set of comparison operator implementations.
type cmpOpOverload []CmpOp

func (o cmpOpOverload) lookupImpl(left, right Datum) (CmpOp, bool) {
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
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DString) == *right.(*DString)), nil
			},
		},
		CmpOp{
			LeftType:  TypeBytes,
			RightType: TypeBytes,

			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DBytes) == *right.(*DBytes)), nil
			},
		},
		CmpOp{
			LeftType:  TypeBool,
			RightType: TypeBool,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DBool) == *right.(*DBool)), nil
			},
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeInt,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DInt) == *right.(*DInt)), nil
			},
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeFloat,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DFloat) == *right.(*DFloat)), nil
			},
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				l := &left.(*DDecimal).Dec
				r := &right.(*DDecimal).Dec
				return DBool(l.Cmp(r) == 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeInt,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DFloat) == DFloat(*right.(*DInt))), nil
			},
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeFloat,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(DFloat(*left.(*DInt)) == *right.(*DFloat)), nil
			},
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeInt,
			fn: func(ctx *EvalContext, left Datum, right Datum) (DBool, error) {
				l := &left.(*DDecimal).Dec
				r := ctx.getTmpDec().SetUnscaled(int64(*right.(*DInt))).SetScale(0)
				return DBool(l.Cmp(r) == 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeDecimal,
			fn: func(ctx *EvalContext, left Datum, right Datum) (DBool, error) {
				l := ctx.getTmpDec().SetUnscaled(int64(*left.(*DInt))).SetScale(0)
				r := &right.(*DDecimal).Dec
				return DBool(l.Cmp(r) == 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeFloat,
			fn: func(ctx *EvalContext, left Datum, right Datum) (DBool, error) {
				l := &left.(*DDecimal).Dec
				r := decimal.SetFromFloat(ctx.getTmpDec(), float64(*right.(*DFloat)))
				return DBool(l.Cmp(r) == 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeDecimal,
			fn: func(ctx *EvalContext, left Datum, right Datum) (DBool, error) {
				l := decimal.SetFromFloat(ctx.getTmpDec(), float64(*left.(*DFloat)))
				r := &right.(*DDecimal).Dec
				return DBool(l.Cmp(r) == 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeDate,
			RightType: TypeDate,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DDate) == *right.(*DDate)), nil
			},
		},
		CmpOp{
			LeftType:  TypeTimestamp,
			RightType: TypeTimestamp,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(left.(*DTimestamp).Equal(right.(*DTimestamp).Time)), nil
			},
		},
		CmpOp{
			LeftType:  TypeTimestampTZ,
			RightType: TypeTimestampTZ,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(left.(*DTimestampTZ).Equal(right.(*DTimestampTZ).Time)), nil
			},
		},
		CmpOp{
			LeftType:  TypeInterval,
			RightType: TypeInterval,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DInterval) == *right.(*DInterval)), nil
			},
		},
		CmpOp{
			LeftType:  TypeTuple,
			RightType: TypeTuple,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				c, err := cmpTuple(left, right)
				return DBool(c == 0), err
			},
		},
	},

	LT: {
		CmpOp{
			LeftType:  TypeString,
			RightType: TypeString,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DString) < *right.(*DString)), nil
			},
		},
		CmpOp{
			LeftType:  TypeBytes,
			RightType: TypeBytes,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DBytes) < *right.(*DBytes)), nil
			},
		},
		CmpOp{
			LeftType:  TypeBool,
			RightType: TypeBool,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return !*left.(*DBool) && *right.(*DBool), nil
			},
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeInt,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DInt) < *right.(*DInt)), nil
			},
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeFloat,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DFloat) < *right.(*DFloat)), nil
			},
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				l := &left.(*DDecimal).Dec
				r := &right.(*DDecimal).Dec
				return DBool(l.Cmp(r) < 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeInt,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DFloat) < DFloat(*right.(*DInt))), nil
			},
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeFloat,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(DFloat(*left.(*DInt)) < *right.(*DFloat)), nil
			},
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeInt,
			fn: func(ctx *EvalContext, left Datum, right Datum) (DBool, error) {
				l := &left.(*DDecimal).Dec
				r := ctx.getTmpDec().SetUnscaled(int64(*right.(*DInt))).SetScale(0)
				return DBool(l.Cmp(r) < 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeDecimal,
			fn: func(ctx *EvalContext, left Datum, right Datum) (DBool, error) {
				l := ctx.getTmpDec().SetUnscaled(int64(*left.(*DInt))).SetScale(0)
				r := &right.(*DDecimal).Dec
				return DBool(l.Cmp(r) < 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeFloat,
			fn: func(ctx *EvalContext, left Datum, right Datum) (DBool, error) {
				l := &left.(*DDecimal).Dec
				r := decimal.SetFromFloat(ctx.getTmpDec(), float64(*right.(*DFloat)))
				return DBool(l.Cmp(r) < 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeDecimal,
			fn: func(ctx *EvalContext, left Datum, right Datum) (DBool, error) {
				l := decimal.SetFromFloat(ctx.getTmpDec(), float64(*left.(*DFloat)))
				r := &right.(*DDecimal).Dec
				return DBool(l.Cmp(r) < 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeDate,
			RightType: TypeDate,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DDate) < *right.(*DDate)), nil
			},
		},
		CmpOp{
			LeftType:  TypeTimestamp,
			RightType: TypeTimestamp,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(left.(*DTimestamp).Before(right.(*DTimestamp).Time)), nil
			},
		},
		CmpOp{
			LeftType:  TypeTimestampTZ,
			RightType: TypeTimestampTZ,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(left.(*DTimestampTZ).Before(right.(*DTimestampTZ).Time)), nil
			},
		},
		CmpOp{
			LeftType:  TypeInterval,
			RightType: TypeInterval,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(left.(*DInterval).Duration.Compare(right.(*DInterval).Duration) < 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeTuple,
			RightType: TypeTuple,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				c, err := cmpTuple(left, right)
				return DBool(c < 0), err
			},
		},
	},

	LE: {
		CmpOp{
			LeftType:  TypeString,
			RightType: TypeString,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DString) <= *right.(*DString)), nil
			},
		},
		CmpOp{
			LeftType:  TypeBytes,
			RightType: TypeBytes,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DBytes) <= *right.(*DBytes)), nil
			},
		},
		CmpOp{
			LeftType:  TypeBool,
			RightType: TypeBool,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return !*left.(*DBool) || *right.(*DBool), nil
			},
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeInt,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DInt) <= *right.(*DInt)), nil
			},
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeFloat,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DFloat) <= *right.(*DFloat)), nil
			},
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeDecimal,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				l := &left.(*DDecimal).Dec
				r := &right.(*DDecimal).Dec
				return DBool(l.Cmp(r) <= 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeInt,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DFloat) <= DFloat(*right.(*DInt))), nil
			},
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeFloat,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(DFloat(*left.(*DInt)) <= *right.(*DFloat)), nil
			},
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeInt,
			fn: func(ctx *EvalContext, left Datum, right Datum) (DBool, error) {
				l := &left.(*DDecimal).Dec
				r := ctx.getTmpDec().SetUnscaled(int64(*right.(*DInt))).SetScale(0)
				return DBool(l.Cmp(r) <= 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeDecimal,
			fn: func(ctx *EvalContext, left Datum, right Datum) (DBool, error) {
				l := ctx.getTmpDec().SetUnscaled(int64(*left.(*DInt))).SetScale(0)
				r := &right.(*DDecimal).Dec
				return DBool(l.Cmp(r) <= 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeFloat,
			fn: func(ctx *EvalContext, left Datum, right Datum) (DBool, error) {
				l := &left.(*DDecimal).Dec
				r := decimal.SetFromFloat(ctx.getTmpDec(), float64(*right.(*DFloat)))
				return DBool(l.Cmp(r) <= 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeDecimal,
			fn: func(ctx *EvalContext, left Datum, right Datum) (DBool, error) {
				l := decimal.SetFromFloat(ctx.getTmpDec(), float64(*left.(*DFloat)))
				r := &right.(*DDecimal).Dec
				return DBool(l.Cmp(r) <= 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeDate,
			RightType: TypeDate,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DDate) <= *right.(*DDate)), nil
			},
		},
		CmpOp{
			LeftType:  TypeTimestamp,
			RightType: TypeTimestamp,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return !DBool(right.(*DTimestamp).Before(left.(*DTimestamp).Time)), nil
			},
		},
		CmpOp{
			LeftType:  TypeTimestampTZ,
			RightType: TypeTimestampTZ,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return !DBool(right.(*DTimestampTZ).Before(left.(*DTimestampTZ).Time)), nil
			},
		},
		CmpOp{
			LeftType:  TypeInterval,
			RightType: TypeInterval,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(left.(*DInterval).Duration.Compare(right.(*DInterval).Duration) <= 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeTuple,
			RightType: TypeTuple,
			fn: func(_ *EvalContext, left Datum, right Datum) (DBool, error) {
				c, err := cmpTuple(left, right)
				return DBool(c <= 0), err
			},
		},
	},

	In: {
		makeEvalTupleIn(TypeBool),
		makeEvalTupleIn(TypeInt),
		makeEvalTupleIn(TypeFloat),
		makeEvalTupleIn(TypeDecimal),
		makeEvalTupleIn(TypeString),
		makeEvalTupleIn(TypeBytes),
		makeEvalTupleIn(TypeDate),
		makeEvalTupleIn(TypeTimestamp),
		makeEvalTupleIn(TypeTimestampTZ),
		makeEvalTupleIn(TypeInterval),
		makeEvalTupleIn(TypeTuple),
	},

	Like: {
		CmpOp{
			LeftType:  TypeString,
			RightType: TypeString,
			fn: func(ctx *EvalContext, left Datum, right Datum) (DBool, error) {
				return matchLike(ctx, left, right, false)
			},
		},
	},

	ILike: {
		CmpOp{
			LeftType:  TypeString,
			RightType: TypeString,
			fn: func(ctx *EvalContext, left Datum, right Datum) (DBool, error) {
				return matchLike(ctx, left, right, true)
			},
		},
	},

	SimilarTo: {
		CmpOp{
			LeftType:  TypeString,
			RightType: TypeString,
			fn: func(ctx *EvalContext, left Datum, right Datum) (DBool, error) {
				key := similarToKey(*right.(*DString))
				return matchRegexpWithKey(ctx, left, key)
			},
		},
	},

	RegMatch: {
		CmpOp{
			LeftType:  TypeString,
			RightType: TypeString,
			fn: func(ctx *EvalContext, left Datum, right Datum) (DBool, error) {
				key := regexpKey{s: string(*right.(*DString)), caseInsensitive: false}
				return matchRegexpWithKey(ctx, left, key)
			},
		},
	},

	RegIMatch: {
		CmpOp{
			LeftType:  TypeString,
			RightType: TypeString,
			fn: func(ctx *EvalContext, left Datum, right Datum) (DBool, error) {
				key := regexpKey{s: string(*right.(*DString)), caseInsensitive: true}
				return matchRegexpWithKey(ctx, left, key)
			},
		},
	},
}

var errCmpNull = errors.New("NULL comparison")

func cmpTuple(ldatum, rdatum Datum) (int, error) {
	left := *ldatum.(*DTuple)
	right := *rdatum.(*DTuple)
	for i, l := range left {
		r := right[i]
		if l == DNull || r == DNull {
			return 0, errCmpNull
		}
		c := l.Compare(r)
		if c != 0 {
			return c, nil
		}
	}
	return 0, nil
}

func makeEvalTupleIn(d Datum) CmpOp {
	return CmpOp{
		LeftType:  d,
		RightType: TypeTuple,
		fn: func(_ *EvalContext, arg, values Datum) (DBool, error) {
			if arg == DNull {
				return DBool(false), nil
			}

			vtuple := *values.(*DTuple)
			i := sort.Search(len(vtuple), func(i int) bool { return vtuple[i].Compare(arg) >= 0 })
			found := i < len(vtuple) && vtuple[i].Compare(arg) == 0
			return DBool(found), nil
		},
	}
}

func matchLike(ctx *EvalContext, left, right Datum, caseInsensitive bool) (DBool, error) {
	pattern := string(*right.(*DString))
	like := optimizedLikeFunc(pattern, caseInsensitive)
	if like == nil {
		key := likeKey{s: pattern, caseInsensitive: caseInsensitive}
		re, err := ctx.ReCache.GetRegexp(key)
		if err != nil {
			return DBool(false), fmt.Errorf("LIKE regexp compilation failed: %v", err)
		}
		like = re.MatchString
	}
	return DBool(like(string(*left.(*DString)))), nil
}

func matchRegexpWithKey(ctx *EvalContext, str Datum, key regexpCacheKey) (DBool, error) {
	re, err := ctx.ReCache.GetRegexp(key)
	if err != nil {
		return DBool(false), err
	}
	return DBool(re.MatchString(string(*str.(*DString)))), nil
}

// EvalContext defines the context in which to evaluate an expression, allowing
// the retrieval of state such as the node ID or statement start time.
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

	ReCache *RegexpCache
	tmpDec  inf.Dec

	// TODO(mjibson): remove prepareOnly in favor of a 2-step prepare-exec solution
	// that is also able to save the plan to skip work during the exec step.
	PrepareOnly bool

	// SkipNormalize indicates whether expressions should be normalized
	// (false) or not (true).  It is set to true conditionally by
	// EXPLAIN(TYPES[, NORMALIZE]).
	SkipNormalize bool
}

// GetStmtTimestamp retrieves the current statement timestamp as per
// the evaluation context. The timestamp is guaranteed to be nonzero.
func (ctx *EvalContext) GetStmtTimestamp() *DTimestamp {
	// TODO(knz) a zero timestamp should never be read, even during
	// Prepare. This will need to be addressed.
	if !ctx.PrepareOnly && ctx.stmtTimestamp.IsZero() {
		panic("zero statement timestamp in EvalContext")
	}
	return MakeDTimestamp(ctx.stmtTimestamp, time.Microsecond)
}

// GetClusterTimestamp retrieves the current cluster timestamp as per
// the evaluation context. The timestamp is guaranteed to be nonzero.
func (ctx *EvalContext) GetClusterTimestamp() *DDecimal {
	// TODO(knz) a zero timestamp should never be read, even during
	// Prepare. This will need to be addressed.
	if !ctx.PrepareOnly {
		if ctx.clusterTimestamp == hlc.ZeroTimestamp {
			panic("zero cluster timestamp in EvalContext")
		}
	}

	// Compute Walltime * 10^10 + Logical.
	// We need 10 decimals for the Logical field because its maximum
	// value is 4294967295 (2^32-1), a value with 10 decimal digits.
	var res DDecimal
	val := res.UnscaledBig()
	val.SetInt64(ctx.clusterTimestamp.WallTime)
	val.Mul(val, decimal.PowerOfTenInt(10))
	val.Add(val, big.NewInt(int64(ctx.clusterTimestamp.Logical)))

	// Shift 10 decimals to the right, so that the logical
	// field appears as fractional part.
	res.Dec.SetScale(10)
	return &res
}

// GetTxnTimestamp retrieves the current transaction timestamp as per
// the evaluation context. The timestamp is guaranteed to be nonzero.
func (ctx *EvalContext) GetTxnTimestamp(precision time.Duration) *DTimestamp {
	// TODO(knz) a zero timestamp should never be read, even during
	// Prepare. This will need to be addressed.
	if !ctx.PrepareOnly && ctx.txnTimestamp.IsZero() {
		panic("zero transaction timestamp in EvalContext")
	}
	return MakeDTimestamp(ctx.txnTimestamp, precision)
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

func (ctx *EvalContext) getTmpDec() *inf.Dec {
	return &ctx.tmpDec
}

// Eval implements the TypedExpr interface.
func (expr *AndExpr) Eval(ctx *EvalContext) (Datum, error) {
	left, err := expr.Left.(TypedExpr).Eval(ctx)
	if err != nil {
		return DNull, err
	}
	if left != DNull {
		if v, err := GetBool(left); err != nil {
			return DNull, err
		} else if !v {
			return left, nil
		}
	}
	right, err := expr.Right.(TypedExpr).Eval(ctx)
	if err != nil {
		return DNull, err
	}
	if right == DNull {
		return DNull, nil
	}
	if v, err := GetBool(right); err != nil {
		return DNull, err
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
			return DNull, err
		}

		for _, when := range expr.Whens {
			arg, err := when.Cond.(TypedExpr).Eval(ctx)
			if err != nil {
				return DNull, err
			}
			d, err := evalComparison(ctx, EQ, val, arg)
			if err != nil {
				return DNull, err
			}
			if v, err := GetBool(d); err != nil {
				return DNull, err
			} else if v {
				return when.Val.(TypedExpr).Eval(ctx)
			}
		}
	} else {
		// CASE WHEN <bool-expr> THEN ...
		for _, when := range expr.Whens {
			d, err := when.Cond.(TypedExpr).Eval(ctx)
			if err != nil {
				return DNull, err
			}
			if v, err := GetBool(d); err != nil {
				return DNull, err
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

	switch expr.Type.(type) {
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
		}

	case *IntColType:
		switch v := d.(type) {
		case *DBool:
			if *v {
				return NewDInt(1), nil
			}
			return NewDInt(0), nil
		case *DInt:
			return d, nil
		case *DFloat:
			f, err := round(float64(*v), 0)
			if err != nil {
				panic(fmt.Sprintf("round should never fail with digits hardcoded to 0: %s", err))
			}
			return NewDInt(DInt(*f.(*DFloat))), nil
		case *DDecimal:
			dec := new(inf.Dec)
			dec.Round(&v.Dec, 0, inf.RoundHalfUp)
			i, ok := dec.Unscaled()
			if !ok {
				return nil, errIntOutOfRange
			}
			return NewDInt(DInt(i)), nil
		case *DString:
			return ParseDInt(string(*v))
		}

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
			f, err := decimal.Float64FromDec(&v.Dec)
			if err != nil {
				return nil, errFloatOutOfRange
			}
			return NewDFloat(DFloat(f)), nil
		case *DString:
			return ParseDFloat(string(*v))
		}

	case *DecimalColType:
		switch v := d.(type) {
		case *DBool:
			dd := &DDecimal{}
			if *v {
				dd.SetUnscaled(1)
			}
			return dd, nil
		case *DInt:
			dd := &DDecimal{}
			dd.SetUnscaled(int64(*v))
			return dd, nil
		case *DFloat:
			dd := &DDecimal{}
			decimal.SetFromFloat(&dd.Dec, float64(*v))
			return dd, nil
		case *DDecimal:
			return d, nil
		case *DString:
			return ParseDDecimal(string(*v))
		}

	case *StringColType:
		var s DString
		switch t := d.(type) {
		case *DBool, *DInt, *DFloat, *DDecimal, *DTimestamp, *DTimestampTZ, dNull:
			s = DString(d.String())
		case *DString:
			s = *t
		case *DBytes:
			if !utf8.ValidString(string(*t)) {
				return nil, fmt.Errorf("invalid utf8: %q", string(*t))
			}
			s = DString(*t)
		}
		if c, ok := expr.Type.(*StringColType); ok {
			// If the CHAR type specifies a limit we truncate to that limit:
			//   'hello'::CHAR(2) -> 'he'
			if c.N > 0 && c.N < len(s) {
				s = s[:c.N]
			}
		}
		return &s, nil

	case *BytesColType:
		switch t := d.(type) {
		case *DString:
			return NewDBytes(DBytes(*t)), nil
		case *DBytes:
			return d, nil
		}

	case *DateColType:
		switch d := d.(type) {
		case *DString:
			return ParseDDate(string(*d), ctx.GetLocation())
		case *DDate:
			return d, nil
		case *DTimestamp:
			return NewDDateFromTime(d.Time, ctx.GetLocation()), nil
		}

	case *TimestampColType:
		switch d := d.(type) {
		case *DString:
			return ParseDTimestamp(string(*d), ctx.GetLocation(), time.Microsecond)
		case *DDate:
			year, month, day := time.Unix(int64(*d)*secondsInDay, 0).UTC().Date()
			return MakeDTimestamp(time.Date(year, month, day, 0, 0, 0, 0, ctx.GetLocation()), time.Microsecond), nil
		case *DTimestamp:
			return d, nil
		case *DTimestampTZ:
			return MakeDTimestamp(d.Time, time.Microsecond), nil
		}

	case *TimestampTZColType:
		switch d := d.(type) {
		case *DString:
			return ParseDTimestampTZ(string(*d), ctx.GetLocation(), time.Microsecond)
		case *DDate:
			year, month, day := time.Unix(int64(*d)*secondsInDay, 0).UTC().Date()
			return MakeDTimestampTZ(time.Date(year, month, day, 0, 0, 0, 0, ctx.GetLocation()), time.Microsecond), nil
		case *DTimestamp:
			return MakeDTimestampTZ(d.Time, time.Microsecond), nil
		case *DTimestampTZ:
			return d, nil
		}

	case *IntervalColType:
		switch v := d.(type) {
		case *DString:
			return ParseDInterval(string(*v))
		case *DInt:
			// An integer duration represents a duration in nanoseconds.
			return &DInterval{Duration: duration.Duration{Nanos: int64(*v)}}, nil
		case *DInterval:
			return d, nil
		}
	}

	return nil, fmt.Errorf("invalid cast: %s -> %s", d.Type(), expr.Type)
}

// Eval implements the TypedExpr interface.
func (expr *AnnotateTypeExpr) Eval(ctx *EvalContext) (Datum, error) {
	return expr.Expr.(TypedExpr).Eval(ctx)
}

// Eval implements the TypedExpr interface.
func (expr *CoalesceExpr) Eval(ctx *EvalContext) (Datum, error) {
	for _, e := range expr.Exprs {
		d, err := e.(TypedExpr).Eval(ctx)
		if err != nil {
			return DNull, err
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
		return DNull, err
	}
	right, err := expr.Right.(TypedExpr).Eval(ctx)
	if err != nil {
		return DNull, err
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

	_, newLeft, newRight, _, not := foldComparisonExpr(expr.Operator, left, right)
	d, err := expr.fn.fn(ctx, newLeft.(Datum), newRight.(Datum))
	if err == errCmpNull {
		return DNull, nil
	}
	if err == nil && not {
		return MakeDBool(!d), nil
	}
	return MakeDBool(d), err
}

// Eval implements the TypedExpr interface.
func (t *ExistsExpr) Eval(ctx *EvalContext) (Datum, error) {
	// Exists expressions are handled during subquery expansion.
	return nil, errors.Errorf("unhandled type %T", t)
}

// Eval implements the TypedExpr interface.
func (expr *FuncExpr) Eval(ctx *EvalContext) (Datum, error) {
	args := make(DTuple, 0, len(expr.Exprs))
	for _, e := range expr.Exprs {
		arg, err := e.(TypedExpr).Eval(ctx)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}

	if !expr.fn.Types.match(ArgTypes(args)) {
		// The argument types no longer match the memoized function. This happens
		// when a non-NULL argument becomes NULL and the function does not support
		// NULL arguments. For example, "SELECT LOWER(col) FROM TABLE" where col is
		// nullable. The SELECT does not error, but returns a NULL value for that
		// select expression.
		return DNull, nil
	}

	res, err := expr.fn.fn(ctx, args)
	if err != nil {
		return nil, fmt.Errorf("%s: %v", expr.Name, err)
	}
	return res, nil
}

// Eval implements the TypedExpr interface.
func (expr *OverlayExpr) Eval(ctx *EvalContext) (Datum, error) {
	return nil, errors.Errorf("unhandled type %T", expr)
}

// Eval implements the TypedExpr interface.
func (expr *IfExpr) Eval(ctx *EvalContext) (Datum, error) {
	cond, err := expr.Cond.(TypedExpr).Eval(ctx)
	if err != nil {
		return DNull, err
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
		return DNull, err
	}

	result := DBool(true)
	if expr.Not {
		result = !result
	}

	switch d.(type) {
	case *DBool:
		for _, t := range expr.Types {
			if _, ok := t.(*BoolColType); ok {
				return MakeDBool(result), nil
			}
		}

	case *DInt:
		for _, t := range expr.Types {
			if _, ok := t.(*IntColType); ok {
				return MakeDBool(result), nil
			}
		}

	case *DFloat:
		for _, t := range expr.Types {
			if _, ok := t.(*FloatColType); ok {
				return MakeDBool(result), nil
			}
		}

	case *DDecimal:
		for _, t := range expr.Types {
			if _, ok := t.(*DecimalColType); ok {
				return MakeDBool(result), nil
			}
		}

	case *DString:
		for _, t := range expr.Types {
			if _, ok := t.(*StringColType); ok {
				return MakeDBool(result), nil
			}
		}

	case *DBytes:
		for _, t := range expr.Types {
			if _, ok := t.(*BytesColType); ok {
				return MakeDBool(result), nil
			}
		}

	case *DDate:
		for _, t := range expr.Types {
			if _, ok := t.(*DateColType); ok {
				return MakeDBool(result), nil
			}
		}

	case *DTimestamp:
		for _, t := range expr.Types {
			if _, ok := t.(*TimestampColType); ok {
				return MakeDBool(result), nil
			}
		}

	case *DTimestampTZ:
		for _, t := range expr.Types {
			if _, ok := t.(*TimestampTZColType); ok {
				return MakeDBool(result), nil
			}
		}

	case *DInterval:
		for _, t := range expr.Types {
			if _, ok := t.(*IntervalColType); ok {
				return MakeDBool(result), nil
			}
		}
	}

	return MakeDBool(!result), nil
}

// Eval implements the TypedExpr interface.
func (expr *NotExpr) Eval(ctx *EvalContext) (Datum, error) {
	d, err := expr.Expr.(TypedExpr).Eval(ctx)
	if err != nil {
		return DNull, err
	}
	if d == DNull {
		return DNull, nil
	}
	v, err := GetBool(d)
	if err != nil {
		return DNull, err
	}
	return MakeDBool(!v), nil
}

// Eval implements the TypedExpr interface.
func (expr *NullIfExpr) Eval(ctx *EvalContext) (Datum, error) {
	expr1, err := expr.Expr1.(TypedExpr).Eval(ctx)
	if err != nil {
		return DNull, err
	}
	expr2, err := expr.Expr2.(TypedExpr).Eval(ctx)
	if err != nil {
		return DNull, err
	}
	cond, err := evalComparison(ctx, EQ, expr1, expr2)
	if err != nil {
		return DNull, err
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
		return DNull, err
	}
	if left != DNull {
		if v, err := GetBool(left); err != nil {
			return DNull, err
		} else if v {
			return left, nil
		}
	}
	right, err := expr.Right.(TypedExpr).Eval(ctx)
	if err != nil {
		return DNull, err
	}
	if right == DNull {
		return DNull, nil
	}
	if v, err := GetBool(right); err != nil {
		return DNull, err
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
func (expr *RangeCond) Eval(_ *EvalContext) (Datum, error) {
	log.Errorf(context.TODO(), "unhandled type %T passed to Eval", expr)
	return nil, errors.Errorf("unhandled type %T", expr)
}

// Eval implements the TypedExpr interface.
func (expr *Subquery) Eval(_ *EvalContext) (Datum, error) {
	// Subquery expressions are handled during subquery expansion.
	log.Errorf(context.TODO(), "unhandled type %T passed to Eval", expr)
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
func (expr DefaultVal) Eval(_ *EvalContext) (Datum, error) {
	log.Errorf(context.TODO(), "unhandled type %T passed to Eval", expr)
	return nil, errors.Errorf("unhandled type %T", expr)
}

// Eval implements the TypedExpr interface.
func (expr *QualifiedName) Eval(ctx *EvalContext) (Datum, error) {
	log.Errorf(context.TODO(), "unhandled type %T passed to Eval", expr)
	return nil, errors.Errorf("unhandled type %T", expr)
}

// Eval implements the TypedExpr interface.
func (t *Tuple) Eval(ctx *EvalContext) (Datum, error) {
	tuple := make(DTuple, 0, len(t.Exprs))
	for _, v := range t.Exprs {
		d, err := v.(TypedExpr).Eval(ctx)
		if err != nil {
			return DNull, err
		}
		tuple = append(tuple, d)
	}
	return &tuple, nil
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
func (t *DPlaceholder) Eval(_ *EvalContext) (Datum, error) {
	return t, fmt.Errorf("no value provided for placeholder: $%s", t.name)
}

func evalComparison(ctx *EvalContext, op ComparisonOperator, left, right Datum) (Datum, error) {
	if left == DNull || right == DNull {
		return DNull, nil
	}

	if fn, ok := CmpOps[op].lookupImpl(left, right); ok {
		v, err := fn.fn(ctx, left, right)
		return MakeDBool(v), err
	}

	return nil, fmt.Errorf("unsupported comparison operator: <%s> %s <%s>",
		left.Type(), op, right.Type())
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
func FindEqualComparisonFunction(leftType, rightType Datum) (func(*EvalContext, Datum, Datum) (DBool, error), bool) {
	fn, found := CmpOps[EQ].lookupImpl(leftType, rightType)
	if found {
		return fn.fn, true
	}
	return nil, false
}
