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
	"errors"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/decimal"
	"github.com/cockroachdb/cockroach/util/duration"
	"github.com/cockroachdb/cockroach/util/log"
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
	fn         func(EvalContext, Datum) (Datum, error)
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
			fn: func(_ EvalContext, d Datum) (Datum, error) {
				return d, nil
			},
		},
		UnaryOp{
			Typ:        TypeFloat,
			ReturnType: TypeFloat,
			fn: func(_ EvalContext, d Datum) (Datum, error) {
				return d, nil
			},
		},
		UnaryOp{
			Typ:        TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ EvalContext, d Datum) (Datum, error) {
				return d, nil
			},
		},
	},

	UnaryMinus: {
		UnaryOp{
			Typ:        TypeInt,
			ReturnType: TypeInt,
			fn: func(_ EvalContext, d Datum) (Datum, error) {
				return NewDInt(-*d.(*DInt)), nil
			},
		},
		UnaryOp{
			Typ:        TypeFloat,
			ReturnType: TypeFloat,
			fn: func(_ EvalContext, d Datum) (Datum, error) {
				return NewDFloat(-*d.(*DFloat)), nil
			},
		},
		UnaryOp{
			Typ:        TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ EvalContext, d Datum) (Datum, error) {
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
			fn: func(_ EvalContext, d Datum) (Datum, error) {
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
	fn         func(EvalContext, Datum, Datum) (Datum, error)
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
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDInt(*left.(*DInt) & *right.(*DInt)), nil
			},
		},
	},

	Bitor: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeInt,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDInt(*left.(*DInt) | *right.(*DInt)), nil
			},
		},
	},

	Bitxor: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeInt,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
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
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDInt(*left.(*DInt) + *right.(*DInt)), nil
			},
		},
		BinOp{
			LeftType:   TypeFloat,
			RightType:  TypeFloat,
			ReturnType: TypeFloat,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDFloat(*left.(*DFloat) + *right.(*DFloat)), nil
			},
		},
		BinOp{
			LeftType:   TypeDecimal,
			RightType:  TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Dec
				r := &right.(*DDecimal).Dec
				dd := &DDecimal{}
				dd.Add(l, r)
				return dd, nil
			},
		},
		BinOp{
			LeftType:   TypeDate,
			RightType:  TypeInt,
			ReturnType: TypeDate,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDDate(*left.(*DDate) + DDate(*right.(*DInt))), nil
			},
		},
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeDate,
			ReturnType: TypeDate,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDDate(DDate(*left.(*DInt)) + *right.(*DDate)), nil
			},
		},
		BinOp{
			LeftType:   TypeTimestamp,
			RightType:  TypeInterval,
			ReturnType: TypeTimestamp,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				return MakeDTimestamp(duration.Add(left.(*DTimestamp).Time, right.(*DInterval).Duration), time.Microsecond), nil
			},
		},
		BinOp{
			LeftType:   TypeInterval,
			RightType:  TypeTimestamp,
			ReturnType: TypeTimestamp,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				return MakeDTimestamp(duration.Add(right.(*DTimestamp).Time, left.(*DInterval).Duration), time.Microsecond), nil
			},
		},
		BinOp{
			LeftType:   TypeTimestampTZ,
			RightType:  TypeInterval,
			ReturnType: TypeTimestampTZ,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				t := duration.Add(left.(*DTimestampTZ).Time, right.(*DInterval).Duration)
				return MakeDTimestampTZ(t, time.Microsecond), nil
			},
		},
		BinOp{
			LeftType:   TypeInterval,
			RightType:  TypeTimestampTZ,
			ReturnType: TypeTimestampTZ,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				t := duration.Add(right.(*DTimestampTZ).Time, left.(*DInterval).Duration)
				return MakeDTimestampTZ(t, time.Microsecond), nil
			},
		},
		BinOp{
			LeftType:   TypeInterval,
			RightType:  TypeInterval,
			ReturnType: TypeInterval,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				return &DInterval{Duration: left.(*DInterval).Duration.Add(right.(*DInterval).Duration)}, nil
			},
		},
	},

	Minus: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeInt,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDInt(*left.(*DInt) - *right.(*DInt)), nil
			},
		},
		BinOp{
			LeftType:   TypeFloat,
			RightType:  TypeFloat,
			ReturnType: TypeFloat,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDFloat(*left.(*DFloat) - *right.(*DFloat)), nil
			},
		},
		BinOp{
			LeftType:   TypeDecimal,
			RightType:  TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Dec
				r := &right.(*DDecimal).Dec
				dd := &DDecimal{}
				dd.Sub(l, r)
				return dd, nil
			},
		},
		BinOp{
			LeftType:   TypeDate,
			RightType:  TypeInt,
			ReturnType: TypeDate,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDDate(*left.(*DDate) - DDate(*right.(*DInt))), nil
			},
		},
		BinOp{
			LeftType:   TypeDate,
			RightType:  TypeDate,
			ReturnType: TypeInt,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDInt(DInt(*left.(*DDate) - *right.(*DDate))), nil
			},
		},
		BinOp{
			LeftType:   TypeTimestamp,
			RightType:  TypeTimestamp,
			ReturnType: TypeInterval,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				nanos := left.(*DTimestamp).Sub(right.(*DTimestamp).Time).Nanoseconds()
				return &DInterval{Duration: duration.Duration{Nanos: nanos}}, nil
			},
		},
		BinOp{
			LeftType:   TypeTimestampTZ,
			RightType:  TypeTimestampTZ,
			ReturnType: TypeInterval,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				nanos := left.(*DTimestampTZ).Sub(right.(*DTimestampTZ).Time).Nanoseconds()
				return &DInterval{Duration: duration.Duration{Nanos: nanos}}, nil
			},
		},
		BinOp{
			LeftType:   TypeTimestamp,
			RightType:  TypeInterval,
			ReturnType: TypeTimestamp,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				return MakeDTimestamp(duration.Add(left.(*DTimestamp).Time, right.(*DInterval).Duration.Mul(-1)), time.Microsecond), nil
			},
		},
		BinOp{
			LeftType:   TypeTimestampTZ,
			RightType:  TypeInterval,
			ReturnType: TypeTimestampTZ,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				t := duration.Add(left.(*DTimestampTZ).Time, right.(*DInterval).Duration.Mul(-1))
				return MakeDTimestampTZ(t, time.Microsecond), nil
			},
		},
		BinOp{
			LeftType:   TypeInterval,
			RightType:  TypeInterval,
			ReturnType: TypeInterval,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				return &DInterval{Duration: left.(*DInterval).Duration.Sub(right.(*DInterval).Duration)}, nil
			},
		},
	},

	Mult: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeInt,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDInt(*left.(*DInt) * *right.(*DInt)), nil
			},
		},
		BinOp{
			LeftType:   TypeFloat,
			RightType:  TypeFloat,
			ReturnType: TypeFloat,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDFloat(*left.(*DFloat) * *right.(*DFloat)), nil
			},
		},
		// The following two overloads are needed becauase DInt/DInt = DDecimal. Due to this
		// operation, normalization may sometimes create a DInt * DDecimal operation.
		BinOp{
			LeftType:   TypeDecimal,
			RightType:  TypeInt,
			ReturnType: TypeDecimal,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Dec
				r := *right.(*DInt)
				dd := &DDecimal{}
				dd.SetUnscaled(int64(r))
				dd.Mul(&dd.Dec, l)
				return dd, nil
			},
		},
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				l := *left.(*DInt)
				r := &right.(*DDecimal).Dec
				dd := &DDecimal{}
				dd.SetUnscaled(int64(l))
				dd.Mul(&dd.Dec, r)
				return dd, nil
			},
		},
		BinOp{
			LeftType:   TypeDecimal,
			RightType:  TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				l := &left.(*DDecimal).Dec
				r := &right.(*DDecimal).Dec
				dd := &DDecimal{}
				dd.Mul(l, r)
				return dd, nil
			},
		},
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInterval,
			ReturnType: TypeInterval,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				return &DInterval{Duration: right.(*DInterval).Duration.Mul(int64(*left.(*DInt)))}, nil
			},
		},
		BinOp{
			LeftType:   TypeInterval,
			RightType:  TypeInt,
			ReturnType: TypeInterval,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				return &DInterval{Duration: left.(*DInterval).Duration.Mul(int64(*right.(*DInt)))}, nil
			},
		},
	},

	Div: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeDecimal,
			fn: func(ctx EvalContext, left Datum, right Datum) (Datum, error) {
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
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDFloat(*left.(*DFloat) / *right.(*DFloat)), nil
			},
		},
		BinOp{
			LeftType:   TypeDecimal,
			RightType:  TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
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
			LeftType:   TypeInterval,
			RightType:  TypeInt,
			ReturnType: TypeInterval,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				rInt := *right.(*DInt)
				if rInt == 0 {
					return nil, errDivByZero
				}
				return &DInterval{Duration: left.(*DInterval).Duration.Div(int64(rInt))}, nil
			},
		},
	},

	Mod: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeInt,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
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
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDFloat(DFloat(math.Mod(float64(*left.(*DFloat)), float64(*right.(*DFloat))))), nil
			},
		},
		BinOp{
			LeftType:   TypeDecimal,
			RightType:  TypeDecimal,
			ReturnType: TypeDecimal,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
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
	},

	Concat: {
		BinOp{
			LeftType:   TypeString,
			RightType:  TypeString,
			ReturnType: TypeString,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDString(string(*left.(*DString) + *right.(*DString))), nil
			},
		},
		BinOp{
			LeftType:   TypeBytes,
			RightType:  TypeBytes,
			ReturnType: TypeBytes,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
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
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
				return NewDInt(*left.(*DInt) << uint(*right.(*DInt))), nil
			},
		},
	},

	RShift: {
		BinOp{
			LeftType:   TypeInt,
			RightType:  TypeInt,
			ReturnType: TypeInt,
			fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
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
	fn        func(EvalContext, Datum, Datum) (DBool, error)
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
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DString) == *right.(*DString)), nil
			},
		},
		CmpOp{
			LeftType:  TypeBytes,
			RightType: TypeBytes,

			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DBytes) == *right.(*DBytes)), nil
			},
		},
		CmpOp{
			LeftType:  TypeBool,
			RightType: TypeBool,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DBool) == *right.(*DBool)), nil
			},
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeInt,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DInt) == *right.(*DInt)), nil
			},
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeFloat,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DFloat) == *right.(*DFloat)), nil
			},
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeDecimal,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				l := &left.(*DDecimal).Dec
				r := &right.(*DDecimal).Dec
				return DBool(l.Cmp(r) == 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeInt,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DFloat) == DFloat(*right.(*DInt))), nil
			},
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeFloat,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(DFloat(*left.(*DInt)) == *right.(*DFloat)), nil
			},
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeInt,
			fn: func(ctx EvalContext, left Datum, right Datum) (DBool, error) {
				l := &left.(*DDecimal).Dec
				r := ctx.getTmpDec().SetUnscaled(int64(*right.(*DInt))).SetScale(0)
				return DBool(l.Cmp(r) == 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeDecimal,
			fn: func(ctx EvalContext, left Datum, right Datum) (DBool, error) {
				l := ctx.getTmpDec().SetUnscaled(int64(*left.(*DInt))).SetScale(0)
				r := &right.(*DDecimal).Dec
				return DBool(l.Cmp(r) == 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeFloat,
			fn: func(ctx EvalContext, left Datum, right Datum) (DBool, error) {
				l := &left.(*DDecimal).Dec
				r := decimal.SetFromFloat(ctx.getTmpDec(), float64(*right.(*DFloat)))
				return DBool(l.Cmp(r) == 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeDecimal,
			fn: func(ctx EvalContext, left Datum, right Datum) (DBool, error) {
				l := decimal.SetFromFloat(ctx.getTmpDec(), float64(*left.(*DFloat)))
				r := &right.(*DDecimal).Dec
				return DBool(l.Cmp(r) == 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeDate,
			RightType: TypeDate,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(left.(*DDate) == right.(*DDate)), nil
			},
		},
		CmpOp{
			LeftType:  TypeTimestamp,
			RightType: TypeTimestamp,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(left.(*DTimestamp).Equal(right.(*DTimestamp).Time)), nil
			},
		},
		CmpOp{
			LeftType:  TypeTimestampTZ,
			RightType: TypeTimestampTZ,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(left.(*DTimestampTZ).Equal(right.(*DTimestampTZ).Time)), nil
			},
		},
		CmpOp{
			LeftType:  TypeInterval,
			RightType: TypeInterval,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DInterval) == *right.(*DInterval)), nil
			},
		},
		CmpOp{
			LeftType:  TypeTuple,
			RightType: TypeTuple,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				c, err := cmpTuple(left, right)
				return DBool(c == 0), err
			},
		},
	},

	LT: {
		CmpOp{
			LeftType:  TypeString,
			RightType: TypeString,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DString) < *right.(*DString)), nil
			},
		},
		CmpOp{
			LeftType:  TypeBytes,
			RightType: TypeBytes,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DBytes) < *right.(*DBytes)), nil
			},
		},
		CmpOp{
			LeftType:  TypeBool,
			RightType: TypeBool,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(!*left.(*DBool) && *right.(*DBool)), nil
			},
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeInt,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DInt) < *right.(*DInt)), nil
			},
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeFloat,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DFloat) < *right.(*DFloat)), nil
			},
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeDecimal,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				l := &left.(*DDecimal).Dec
				r := &right.(*DDecimal).Dec
				return DBool(l.Cmp(r) < 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeInt,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DFloat) < DFloat(*right.(*DInt))), nil
			},
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeFloat,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(DFloat(*left.(*DInt)) < *right.(*DFloat)), nil
			},
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeInt,
			fn: func(ctx EvalContext, left Datum, right Datum) (DBool, error) {
				l := &left.(*DDecimal).Dec
				r := ctx.getTmpDec().SetUnscaled(int64(*right.(*DInt))).SetScale(0)
				return DBool(l.Cmp(r) < 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeDecimal,
			fn: func(ctx EvalContext, left Datum, right Datum) (DBool, error) {
				l := ctx.getTmpDec().SetUnscaled(int64(*left.(*DInt))).SetScale(0)
				r := &right.(*DDecimal).Dec
				return DBool(l.Cmp(r) < 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeFloat,
			fn: func(ctx EvalContext, left Datum, right Datum) (DBool, error) {
				l := &left.(*DDecimal).Dec
				r := decimal.SetFromFloat(ctx.getTmpDec(), float64(*right.(*DFloat)))
				return DBool(l.Cmp(r) < 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeDecimal,
			fn: func(ctx EvalContext, left Datum, right Datum) (DBool, error) {
				l := decimal.SetFromFloat(ctx.getTmpDec(), float64(*left.(*DFloat)))
				r := &right.(*DDecimal).Dec
				return DBool(l.Cmp(r) < 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeDate,
			RightType: TypeDate,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DDate) < *right.(*DDate)), nil
			},
		},
		CmpOp{
			LeftType:  TypeTimestamp,
			RightType: TypeTimestamp,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(left.(*DTimestamp).Before(right.(*DTimestamp).Time)), nil
			},
		},
		CmpOp{
			LeftType:  TypeTimestampTZ,
			RightType: TypeTimestampTZ,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(left.(*DTimestampTZ).Before(right.(*DTimestampTZ).Time)), nil
			},
		},
		CmpOp{
			LeftType:  TypeInterval,
			RightType: TypeInterval,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(left.(*DInterval).Duration.Compare(right.(*DInterval).Duration) < 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeTuple,
			RightType: TypeTuple,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				c, err := cmpTuple(left, right)
				return DBool(c < 0), err
			},
		},
	},

	LE: {
		CmpOp{
			LeftType:  TypeString,
			RightType: TypeString,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DString) <= *right.(*DString)), nil
			},
		},
		CmpOp{
			LeftType:  TypeBytes,
			RightType: TypeBytes,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DBytes) <= *right.(*DBytes)), nil
			},
		},
		CmpOp{
			LeftType:  TypeBool,
			RightType: TypeBool,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(!*left.(*DBool) || *right.(*DBool)), nil
			},
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeInt,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DInt) <= *right.(*DInt)), nil
			},
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeFloat,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DFloat) <= *right.(*DFloat)), nil
			},
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeDecimal,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				l := &left.(*DDecimal).Dec
				r := &right.(*DDecimal).Dec
				return DBool(l.Cmp(r) <= 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeInt,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DFloat) <= DFloat(*right.(*DInt))), nil
			},
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeFloat,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(DFloat(*left.(*DInt)) <= *right.(*DFloat)), nil
			},
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeInt,
			fn: func(ctx EvalContext, left Datum, right Datum) (DBool, error) {
				l := &left.(*DDecimal).Dec
				r := ctx.getTmpDec().SetUnscaled(int64(*right.(*DInt))).SetScale(0)
				return DBool(l.Cmp(r) <= 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeInt,
			RightType: TypeDecimal,
			fn: func(ctx EvalContext, left Datum, right Datum) (DBool, error) {
				l := ctx.getTmpDec().SetUnscaled(int64(*left.(*DInt))).SetScale(0)
				r := &right.(*DDecimal).Dec
				return DBool(l.Cmp(r) <= 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeDecimal,
			RightType: TypeFloat,
			fn: func(ctx EvalContext, left Datum, right Datum) (DBool, error) {
				l := &left.(*DDecimal).Dec
				r := decimal.SetFromFloat(ctx.getTmpDec(), float64(*right.(*DFloat)))
				return DBool(l.Cmp(r) <= 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeFloat,
			RightType: TypeDecimal,
			fn: func(ctx EvalContext, left Datum, right Datum) (DBool, error) {
				l := decimal.SetFromFloat(ctx.getTmpDec(), float64(*left.(*DFloat)))
				r := &right.(*DDecimal).Dec
				return DBool(l.Cmp(r) <= 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeDate,
			RightType: TypeDate,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(*left.(*DDate) <= *right.(*DDate)), nil
			},
		},
		CmpOp{
			LeftType:  TypeTimestamp,
			RightType: TypeTimestamp,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return !DBool(right.(*DTimestamp).Before(left.(*DTimestamp).Time)), nil
			},
		},
		CmpOp{
			LeftType:  TypeTimestampTZ,
			RightType: TypeTimestampTZ,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return !DBool(right.(*DTimestampTZ).Before(left.(*DTimestampTZ).Time)), nil
			},
		},
		CmpOp{
			LeftType:  TypeInterval,
			RightType: TypeInterval,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				return DBool(left.(*DInterval).Duration.Compare(right.(*DInterval).Duration) <= 0), nil
			},
		},
		CmpOp{
			LeftType:  TypeTuple,
			RightType: TypeTuple,
			fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
				c, err := cmpTuple(left, right)
				return DBool(c <= 0), err
			},
		},
	},

	In: {
		makeEvalTupleIn(TypeBool),
		makeEvalTupleIn(TypeInt),
		makeEvalTupleIn(TypeFloat),
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
			fn: func(ctx EvalContext, left Datum, right Datum) (DBool, error) {
				pattern := string(*right.(*DString))
				like := optimizedLikeFunc(pattern)
				if like == nil {
					key := likeKey(pattern)
					re, err := ctx.ReCache.GetRegexp(key)
					if err != nil {
						return DBool(false), fmt.Errorf("LIKE regexp compilation failed: %v", err)
					}
					like = re.MatchString
				}
				return DBool(like(string(*left.(*DString)))), nil
			},
		},
	},

	SimilarTo: {
		CmpOp{
			LeftType:  TypeString,
			RightType: TypeString,
			fn: func(ctx EvalContext, left Datum, right Datum) (DBool, error) {
				key := similarToKey(*right.(*DString))
				re, err := ctx.ReCache.GetRegexp(key)
				if err != nil {
					return DBool(false), err
				}
				return DBool(re.MatchString(string(*left.(*DString)))), nil
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
		fn: func(_ EvalContext, arg, values Datum) (DBool, error) {
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
	clusterTimestamp roachpb.Timestamp

	ReCache  *RegexpCache
	TmpDec   *inf.Dec
	Location *time.Location
	Args     MapArgs

	// TODO(mjibson): remove prepareOnly in favor of a 2-step prepare-exec solution
	// that is also able to save the plan to skip work during the exec step.
	PrepareOnly bool
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
		if ctx.clusterTimestamp == roachpb.ZeroTimestamp {
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
func (ctx *EvalContext) SetClusterTimestamp(ts roachpb.Timestamp) {
	ctx.clusterTimestamp = ts
}

var defaultContext = EvalContext{Location: time.UTC}

// GetLocation returns the session timezone.
func (ctx EvalContext) GetLocation() *time.Location {
	if ctx.Location == nil {
		return time.UTC
	}
	return ctx.Location
}

// makeDDate constructs a DDate from a time.Time in the session time zone.
func (ctx EvalContext) makeDDate(t time.Time) (*DDate, error) {
	year, month, day := t.In(ctx.GetLocation()).Date()
	secs := time.Date(year, month, day, 0, 0, 0, 0, time.UTC).Unix()
	return NewDDate(DDate(secs / secondsInDay)), nil
}

func (ctx EvalContext) getTmpDec() *inf.Dec {
	if ctx.TmpDec != nil {
		return ctx.TmpDec
	}
	return new(inf.Dec)
}

// Eval implements the Expr interface.
func (expr *AndExpr) Eval(ctx EvalContext) (Datum, error) {
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

// Eval implements the Expr interface.
func (expr *BinaryExpr) Eval(ctx EvalContext) (Datum, error) {
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

// Eval implements the Expr interface.
func (expr *CaseExpr) Eval(ctx EvalContext) (Datum, error) {
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

// Eval implements the Expr interface.
func (expr *CastExpr) Eval(ctx EvalContext) (Datum, error) {
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
			// TODO(pmattis): strconv.ParseBool is more permissive than the SQL
			// spec. Is that ok?
			b, err := strconv.ParseBool(string(*v))
			if err != nil {
				return nil, err
			}
			return MakeDBool(DBool(b)), nil
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
			i, err := strconv.ParseInt(string(*v), 0, 64)
			if err != nil {
				return nil, err
			}
			return NewDInt(DInt(i)), nil
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
			f, err := strconv.ParseFloat(string(*v), 64)
			if err != nil {
				return nil, err
			}
			return NewDFloat(DFloat(f)), nil
		}

	case *DecimalColType:
		dd := &DDecimal{}
		switch v := d.(type) {
		case *DBool:
			if *v {
				dd.SetUnscaled(1)
			}
			return dd, nil
		case *DInt:
			dd.SetUnscaled(int64(*v))
			return dd, nil
		case *DFloat:
			decimal.SetFromFloat(&dd.Dec, float64(*v))
			return dd, nil
		case *DDecimal:
			return d, nil
		case *DString:
			if _, ok := dd.SetString(string(*v)); !ok {
				return nil, fmt.Errorf("could not parse string %q as decimal", v)
			}
			return dd, nil
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
			return ParseDate(*d)
		case *DDate:
			return d, nil
		case *DTimestamp:
			return ctx.makeDDate(d.Time)
		}

	case *TimestampColType:
		switch d := d.(type) {
		case *DString:
			return ctx.ParseTimestamp(*d, time.Microsecond)
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
			t, err := ctx.ParseTimestamp(*d, time.Microsecond)
			return MakeDTimestampTZ(t.Time, time.Microsecond), err
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
			// At this time the only supported interval formats are:
			// - Postgres compatible.
			// - iso8601 format (with designators only), see interval.go for
			//   sources of documentation.
			// - Golang time.parseDuration compatible.

			// If it's a blank string, exit early.
			if len(*v) == 0 {
				return nil, fmt.Errorf("could not parse string %s as an interval", *v)
			}

			str := string(*v)
			if str[0] == 'P' {
				// If it has a leading P we're most likely working with an iso8601
				// interval.
				dur, err := iso8601ToDuration(str)
				return &DInterval{Duration: dur}, err
			} else if strings.ContainsRune(str, ' ') {
				// If it has a space, then we're most likely a postgres string,
				// as neither iso8601 nor golang permit spaces.
				dur, err := postgresToDuration(str)
				return &DInterval{Duration: dur}, err
			} else {
				// Fallback to golang durations.
				dur, err := time.ParseDuration(str)
				return &DInterval{Duration: duration.Duration{Nanos: dur.Nanoseconds()}}, err
			}
		case *DInt:
			// An integer duration represents a duration in nanoseconds.
			return &DInterval{Duration: duration.Duration{Nanos: int64(*v)}}, nil
		case *DInterval:
			return d, nil
		}
	}

	return nil, fmt.Errorf("invalid cast: %s -> %s", d.Type(), expr.Type)
}

// Eval implements the Expr interface.
func (expr *CoalesceExpr) Eval(ctx EvalContext) (Datum, error) {
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

// Eval implements the Expr interface.
func (expr *ComparisonExpr) Eval(ctx EvalContext) (Datum, error) {
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

// Eval implements the Expr interface.
func (t *ExistsExpr) Eval(ctx EvalContext) (Datum, error) {
	// Exists expressions are handled during subquery expansion.
	return nil, util.Errorf("unhandled type %T", t)
}

// Eval implements the Expr interface.
func (expr *FuncExpr) Eval(ctx EvalContext) (Datum, error) {
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

// Eval implements the Expr interface.
func (expr *OverlayExpr) Eval(ctx EvalContext) (Datum, error) {
	return nil, util.Errorf("unhandled type %T", expr)
}

// Eval implements the Expr interface.
func (expr *IfExpr) Eval(ctx EvalContext) (Datum, error) {
	cond, err := expr.Cond.(TypedExpr).Eval(ctx)
	if err != nil {
		return DNull, err
	}
	if cond == DBoolTrue {
		return expr.True.(TypedExpr).Eval(ctx)
	}
	return expr.Else.(TypedExpr).Eval(ctx)
}

// Eval implements the Expr interface.
func (expr *IsOfTypeExpr) Eval(ctx EvalContext) (Datum, error) {
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

// Eval implements the Expr interface.
func (expr *NotExpr) Eval(ctx EvalContext) (Datum, error) {
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

// Eval implements the Expr interface.
func (expr *NullIfExpr) Eval(ctx EvalContext) (Datum, error) {
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

// Eval implements the Expr interface.
func (expr *OrExpr) Eval(ctx EvalContext) (Datum, error) {
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

// Eval implements the Expr interface.
func (expr *ParenExpr) Eval(ctx EvalContext) (Datum, error) {
	return expr.Expr.(TypedExpr).Eval(ctx)
}

// Eval implements the Expr interface.
func (expr *RangeCond) Eval(_ EvalContext) (Datum, error) {
	log.Errorf("unhandled type %T passed to Eval", expr)
	return nil, util.Errorf("unhandled type %T", expr)
}

// Eval implements the Expr interface.
func (expr *Subquery) Eval(_ EvalContext) (Datum, error) {
	// Subquery expressions are handled during subquery expansion.
	log.Errorf("unhandled type %T passed to Eval", expr)
	return nil, util.Errorf("unhandled type %T", expr)
}

// Eval implements the Expr interface.
func (expr *UnaryExpr) Eval(ctx EvalContext) (Datum, error) {
	d, err := expr.Expr.(TypedExpr).Eval(ctx)
	if err != nil {
		return nil, err
	}
	if d == DNull {
		return DNull, nil
	}
	return expr.fn.fn(ctx, d)
}

// Eval implements the Expr interface.
func (expr DefaultVal) Eval(_ EvalContext) (Datum, error) {
	log.Errorf("unhandled type %T passed to Eval", expr)
	return nil, util.Errorf("unhandled type %T", expr)
}

// Eval implements the Expr interface.
func (expr *QualifiedName) Eval(ctx EvalContext) (Datum, error) {
	log.Errorf("unhandled type %T passed to Eval", expr)
	return nil, util.Errorf("unhandled type %T", expr)
}

// Eval implements the Expr interface.
func (t *Tuple) Eval(ctx EvalContext) (Datum, error) {
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

// Eval implements the Expr interface.
func (t *DBool) Eval(_ EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the Expr interface.
func (t *DBytes) Eval(_ EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the Expr interface.
func (t *DDate) Eval(_ EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the Expr interface.
func (t *DFloat) Eval(_ EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the Expr interface.
func (t *DDecimal) Eval(_ EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the Expr interface.
func (t *DInt) Eval(_ EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the Expr interface.
func (t *DInterval) Eval(_ EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the Expr interface.
func (t dNull) Eval(_ EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the Expr interface.
func (t *DString) Eval(_ EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the Expr interface.
func (t *DTimestamp) Eval(_ EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the Expr interface.
func (t *DTimestampTZ) Eval(_ EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the Expr interface.
func (t *DTuple) Eval(_ EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the Expr interface.
func (t *DValArg) Eval(_ EvalContext) (Datum, error) {
	return t, nil
}

func evalComparison(ctx EvalContext, op ComparisonOperator, left, right Datum) (Datum, error) {
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
	case NotSimilarTo:
		// NotSimilarTo(left, right) is implemented as !SimilarTo(left, right)
		return SimilarTo, left, right, false, true
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

// time.Time formats.
const (
	dateFormat                            = "2006-01-02"
	timestampFormat                       = "2006-01-02 15:04:05"
	timestampWithOffsetZoneFormat         = timestampFormat + "-07:00"
	timestampWithNamedZoneFormat          = timestampFormat + " MST"
	timestampRFC3339NanoWithoutZoneFormat = "2006-01-02T15:04:05"

	timestampNodeFormat = timestampFormat + ".999999-07:00"
	timestampFormatNS   = timestampFormat + ".999999999"
)

var dateFormats = []string{
	dateFormat,
	time.RFC3339Nano,
}

var timeFormats = []string{
	dateFormat,
	timestampWithOffsetZoneFormat,
	timestampFormat,
	timestampWithNamedZoneFormat,
	time.RFC3339Nano,
	timestampRFC3339NanoWithoutZoneFormat,
}

// ParseDate parses a date.
func ParseDate(s DString) (*DDate, error) {
	// No need to ParseInLocation here because we're only parsing dates.
	for _, format := range dateFormats {
		if t, err := time.Parse(format, string(s)); err == nil {
			return defaultContext.makeDDate(t)
		}
	}
	return NewDDate(0), fmt.Errorf("could not parse '%s' in any supported date format", s)
}

// ParseTimestamp parses the timestamp.
func (ctx EvalContext) ParseTimestamp(s DString, precision time.Duration) (*DTimestamp, error) {
	str := string(s)

	for _, format := range timeFormats {
		if t, err := time.ParseInLocation(format, str, ctx.GetLocation()); err == nil {
			return MakeDTimestamp(t, precision), nil
		}
	}

	return &DTimestamp{}, fmt.Errorf("could not parse '%s' in any supported timestamp format", s)
}

// Simplifies LIKE expressions that do not need full regular expressions to evaluate the condition.
// For example, when the expression is just checking to see if a string starts with a given
// pattern.
func optimizedLikeFunc(pattern string) func(string) bool {
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
					return strings.Contains(s, pattern[1:len(pattern)-1])
				}
			case anyEnd:
				return func(s string) bool {
					return strings.HasPrefix(s, pattern[:len(pattern)-1])
				}
			case anyStart:
				return func(s string) bool {
					return strings.HasSuffix(s, pattern[1:])
				}
			}
		}
	}
	return nil
}

type likeKey string

func (k likeKey) pattern() (string, error) {
	pattern := regexp.QuoteMeta(string(k))
	// Replace LIKE specific wildcards with standard wildcards
	pattern = strings.Replace(pattern, "%", ".*", -1)
	pattern = strings.Replace(pattern, "_", ".", -1)
	return anchorPattern(pattern, false), nil
}

type similarToKey string

func (k similarToKey) pattern() (string, error) {
	pattern := SimilarEscape(string(k))
	return anchorPattern(pattern, false), nil
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
