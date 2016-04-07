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
	"reflect"
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
	ReturnType Datum
	fn         func(EvalContext, Datum) (Datum, error)
}

// UnaryArgs is a unary operation.
type UnaryArgs struct {
	Op      UnaryOperator
	ArgType reflect.Type
}

// UnaryOps contains the unary operations indexed by operation type and
// argument type.
var UnaryOps = map[UnaryArgs]UnaryOp{
	UnaryArgs{UnaryPlus, intType}: {
		ReturnType: DummyInt,
		fn: func(_ EvalContext, d Datum) (Datum, error) {
			return d, nil
		},
	},
	UnaryArgs{UnaryPlus, floatType}: {
		ReturnType: DummyFloat,
		fn: func(_ EvalContext, d Datum) (Datum, error) {
			return d, nil
		},
	},
	UnaryArgs{UnaryPlus, decimalType}: {
		ReturnType: DummyDecimal,
		fn: func(_ EvalContext, d Datum) (Datum, error) {
			return d, nil
		},
	},

	UnaryArgs{UnaryMinus, intType}: {
		ReturnType: DummyInt,
		fn: func(_ EvalContext, d Datum) (Datum, error) {
			return -d.(DInt), nil
		},
	},
	UnaryArgs{UnaryMinus, floatType}: {
		ReturnType: DummyFloat,
		fn: func(_ EvalContext, d Datum) (Datum, error) {
			return -d.(DFloat), nil
		},
	},
	UnaryArgs{UnaryMinus, decimalType}: {
		ReturnType: DummyDecimal,
		fn: func(_ EvalContext, d Datum) (Datum, error) {
			dec := d.(*DDecimal)
			dd := &DDecimal{}
			dd.Neg(&dec.Dec)
			return dd, nil
		},
	},

	UnaryArgs{UnaryComplement, intType}: {
		ReturnType: DummyInt,
		fn: func(_ EvalContext, d Datum) (Datum, error) {
			return ^d.(DInt), nil
		},
	},
}

// BinOp is a binary operator.
type BinOp struct {
	ReturnType Datum
	fn         func(EvalContext, Datum, Datum) (Datum, error)
}

// BinArgs is a binary operation.
type BinArgs struct {
	Op        BinaryOp
	LeftType  reflect.Type
	RightType reflect.Type
}

// BinOps contains the binary operations indexed by operation type and argument
// types.
var BinOps = map[BinArgs]BinOp{
	BinArgs{Bitand, intType, intType}: {
		ReturnType: DummyInt,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DInt) & right.(DInt), nil
		},
	},

	BinArgs{Bitor, intType, intType}: {
		ReturnType: DummyInt,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DInt) | right.(DInt), nil
		},
	},

	BinArgs{Bitxor, intType, intType}: {
		ReturnType: DummyInt,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DInt) ^ right.(DInt), nil
		},
	},

	// TODO(pmattis): Overflow/underflow checks?

	BinArgs{Plus, intType, intType}: {
		ReturnType: DummyInt,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DInt) + right.(DInt), nil
		},
	},
	BinArgs{Plus, floatType, floatType}: {
		ReturnType: DummyFloat,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DFloat) + right.(DFloat), nil
		},
	},
	BinArgs{Plus, decimalType, decimalType}: {
		ReturnType: DummyDecimal,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			l := left.(*DDecimal).Dec
			r := right.(*DDecimal).Dec
			dd := &DDecimal{}
			dd.Add(&l, &r)
			return dd, nil
		},
	},
	BinArgs{Plus, dateType, intType}: {
		ReturnType: DummyDate,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DDate) + DDate(right.(DInt)), nil
		},
	},
	BinArgs{Plus, intType, dateType}: {
		ReturnType: DummyDate,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return DDate(left.(DInt)) + right.(DDate), nil
		},
	},
	BinArgs{Plus, timestampType, intervalType}: {
		ReturnType: DummyTimestamp,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return DTimestamp{Time: duration.Add(left.(DTimestamp).Time, right.(DInterval).Duration)}, nil
		},
	},
	BinArgs{Plus, intervalType, timestampType}: {
		ReturnType: DummyTimestamp,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return DTimestamp{Time: duration.Add(right.(DTimestamp).Time, left.(DInterval).Duration)}, nil
		},
	},
	BinArgs{Plus, intervalType, intervalType}: {
		ReturnType: DummyInterval,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return DInterval{Duration: left.(DInterval).Duration.Add(right.(DInterval).Duration)}, nil
		},
	},
	BinArgs{Minus, intType, intType}: {
		ReturnType: DummyInt,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DInt) - right.(DInt), nil
		},
	},
	BinArgs{Minus, floatType, floatType}: {
		ReturnType: DummyFloat,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DFloat) - right.(DFloat), nil
		},
	},
	BinArgs{Minus, decimalType, decimalType}: {
		ReturnType: DummyDecimal,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			l := left.(*DDecimal).Dec
			r := right.(*DDecimal).Dec
			dd := &DDecimal{}
			dd.Sub(&l, &r)
			return dd, nil
		},
	},
	BinArgs{Minus, dateType, intType}: {
		ReturnType: DummyDate,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DDate) - DDate(right.(DInt)), nil
		},
	},
	BinArgs{Minus, dateType, dateType}: {
		ReturnType: DummyInt,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return DInt(left.(DDate) - right.(DDate)), nil
		},
	},
	BinArgs{Minus, timestampType, timestampType}: {
		ReturnType: DummyInterval,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			nanos := left.(DTimestamp).Sub(right.(DTimestamp).Time).Nanoseconds()
			return DInterval{Duration: duration.Duration{Nanos: nanos}}, nil
		},
	},
	BinArgs{Minus, timestampType, intervalType}: {
		ReturnType: DummyTimestamp,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return DTimestamp{Time: duration.Add(left.(DTimestamp).Time, right.(DInterval).Duration.Mul(-1))}, nil
		},
	},
	BinArgs{Minus, intervalType, intervalType}: {
		ReturnType: DummyInterval,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return DInterval{Duration: left.(DInterval).Duration.Sub(right.(DInterval).Duration)}, nil
		},
	},

	BinArgs{Mult, intType, intType}: {
		ReturnType: DummyInt,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DInt) * right.(DInt), nil
		},
	},
	BinArgs{Mult, floatType, floatType}: {
		ReturnType: DummyFloat,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DFloat) * right.(DFloat), nil
		},
	},
	BinArgs{Mult, decimalType, decimalType}: {
		ReturnType: DummyDecimal,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			l := left.(*DDecimal).Dec
			r := right.(*DDecimal).Dec
			dd := &DDecimal{}
			dd.Mul(&l, &r)
			return dd, nil
		},
	},
	BinArgs{Mult, intType, intervalType}: {
		ReturnType: DummyInterval,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return DInterval{Duration: right.(DInterval).Duration.Mul(int64(left.(DInt)))}, nil
		},
	},
	BinArgs{Mult, intervalType, intType}: {
		ReturnType: DummyInterval,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return DInterval{Duration: left.(DInterval).Duration.Mul(int64(right.(DInt)))}, nil
		},
	},

	BinArgs{Div, intType, intType}: {
		ReturnType: DummyFloat,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			rInt := right.(DInt)
			if rInt == 0 {
				return nil, errDivByZero
			}
			return DFloat(left.(DInt)) / DFloat(rInt), nil
		},
	},
	BinArgs{Div, floatType, floatType}: {
		ReturnType: DummyFloat,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DFloat) / right.(DFloat), nil
		},
	},
	BinArgs{Div, decimalType, decimalType}: {
		ReturnType: DummyDecimal,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			l := left.(*DDecimal).Dec
			r := right.(*DDecimal).Dec
			if r.Sign() == 0 {
				return nil, errDivByZero
			}
			dd := &DDecimal{}
			dd.QuoRound(&l, &r, decimal.Precision, inf.RoundHalfUp)
			return dd, nil
		},
	},
	BinArgs{Div, intervalType, intType}: {
		ReturnType: DummyInterval,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			rInt := right.(DInt)
			if rInt == 0 {
				return nil, errDivByZero
			}
			return DInterval{Duration: left.(DInterval).Duration.Div(int64(rInt))}, nil
		},
	},

	BinArgs{Mod, intType, intType}: {
		ReturnType: DummyInt,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			r := right.(DInt)
			if r == 0 {
				return nil, errZeroModulus
			}
			return left.(DInt) % r, nil
		},
	},
	BinArgs{Mod, floatType, floatType}: {
		ReturnType: DummyFloat,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return DFloat(math.Mod(float64(left.(DFloat)), float64(right.(DFloat)))), nil
		},
	},
	BinArgs{Mod, decimalType, decimalType}: {
		ReturnType: DummyDecimal,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			l := left.(*DDecimal).Dec
			r := right.(*DDecimal).Dec
			if r.Sign() == 0 {
				return nil, errZeroModulus
			}
			dd := &DDecimal{}
			decimal.Mod(&dd.Dec, &l, &r)
			return dd, nil
		},
	},

	BinArgs{Concat, stringType, stringType}: {
		ReturnType: DummyString,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DString) + right.(DString), nil
		},
	},
	BinArgs{Concat, bytesType, bytesType}: {
		ReturnType: DummyBytes,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DBytes) + right.(DBytes), nil
		},
	},

	// TODO(pmattis): Check that the shift is valid.
	BinArgs{LShift, intType, intType}: {
		ReturnType: DummyInt,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DInt) << uint(right.(DInt)), nil
		},
	},
	BinArgs{RShift, intType, intType}: {
		ReturnType: DummyInt,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DInt) >> uint(right.(DInt)), nil
		},
	},
}

// CmpArgs is a comparison operation.
type CmpArgs struct {
	Op        ComparisonOp
	LeftType  reflect.Type
	RightType reflect.Type
}

// CmpOp is a comparison operator.
type CmpOp struct {
	fn func(EvalContext, Datum, Datum) (DBool, error)
}

var cmpOpResultType = reflect.New(reflect.TypeOf(CmpOp{}).Field(0).Type.Out(0)).Elem().Interface().(DBool)

// CmpOps contains the comparison operations indexed by operation type and
// argument types.
var CmpOps = map[CmpArgs]CmpOp{
	CmpArgs{EQ, stringType, stringType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DString) == right.(DString)), nil
		},
	},
	CmpArgs{EQ, bytesType, bytesType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DBytes) == right.(DBytes)), nil
		},
	},
	CmpArgs{EQ, boolType, boolType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DBool) == right.(DBool)), nil
		},
	},
	CmpArgs{EQ, intType, intType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DInt) == right.(DInt)), nil
		},
	},
	CmpArgs{EQ, floatType, floatType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DFloat) == right.(DFloat)), nil
		},
	},
	CmpArgs{EQ, decimalType, decimalType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			l := left.(*DDecimal).Dec
			r := right.(*DDecimal).Dec
			return DBool(l.Cmp(&r) == 0), nil
		},
	},
	CmpArgs{EQ, floatType, intType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DFloat) == DFloat(right.(DInt))), nil
		},
	},
	CmpArgs{EQ, intType, floatType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(DFloat(left.(DInt)) == right.(DFloat)), nil
		},
	},
	CmpArgs{EQ, decimalType, intType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			l := left.(*DDecimal).Dec
			r := inf.NewDec(int64(right.(DInt)), 0)
			return DBool(l.Cmp(r) == 0), nil
		},
	},
	CmpArgs{EQ, intType, decimalType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			l := inf.NewDec(int64(left.(DInt)), 0)
			r := right.(*DDecimal).Dec
			return DBool(l.Cmp(&r) == 0), nil
		},
	},
	CmpArgs{EQ, decimalType, floatType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			l := left.(*DDecimal).Dec
			r := decimal.NewDecFromFloat(float64(right.(DFloat)))
			return DBool(l.Cmp(r) == 0), nil
		},
	},
	CmpArgs{EQ, floatType, decimalType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			l := decimal.NewDecFromFloat(float64(left.(DFloat)))
			r := right.(*DDecimal).Dec
			return DBool(l.Cmp(&r) == 0), nil
		},
	},
	CmpArgs{EQ, dateType, dateType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DDate) == right.(DDate)), nil
		},
	},
	CmpArgs{EQ, timestampType, timestampType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DTimestamp).Equal(right.(DTimestamp).Time)), nil
		},
	},
	CmpArgs{EQ, intervalType, intervalType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DInterval) == right.(DInterval)), nil
		},
	},

	CmpArgs{LT, stringType, stringType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DString) < right.(DString)), nil
		},
	},
	CmpArgs{LT, bytesType, bytesType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DBytes) < right.(DBytes)), nil
		},
	},
	CmpArgs{LT, boolType, boolType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(!left.(DBool) && right.(DBool)), nil
		},
	},
	CmpArgs{LT, intType, intType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DInt) < right.(DInt)), nil
		},
	},
	CmpArgs{LT, floatType, floatType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DFloat) < right.(DFloat)), nil
		},
	},
	CmpArgs{LT, decimalType, decimalType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			l := left.(*DDecimal).Dec
			r := right.(*DDecimal).Dec
			return DBool(l.Cmp(&r) < 0), nil
		},
	},
	CmpArgs{LT, floatType, intType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DFloat) < DFloat(right.(DInt))), nil
		},
	},
	CmpArgs{LT, intType, floatType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(DFloat(left.(DInt)) < right.(DFloat)), nil
		},
	},
	CmpArgs{LT, decimalType, intType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			l := left.(*DDecimal).Dec
			r := inf.NewDec(int64(right.(DInt)), 0)
			return DBool(l.Cmp(r) < 0), nil
		},
	},
	CmpArgs{LT, intType, decimalType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			l := inf.NewDec(int64(left.(DInt)), 0)
			r := right.(*DDecimal).Dec
			return DBool(l.Cmp(&r) < 0), nil
		},
	},
	CmpArgs{LT, decimalType, floatType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			l := left.(*DDecimal).Dec
			r := decimal.NewDecFromFloat(float64(right.(DFloat)))
			return DBool(l.Cmp(r) < 0), nil
		},
	},
	CmpArgs{LT, floatType, decimalType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			l := decimal.NewDecFromFloat(float64(left.(DFloat)))
			r := right.(*DDecimal).Dec
			return DBool(l.Cmp(&r) < 0), nil
		},
	},
	CmpArgs{LT, dateType, dateType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DDate) < right.(DDate)), nil
		},
	},
	CmpArgs{LT, timestampType, timestampType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DTimestamp).Before(right.(DTimestamp).Time)), nil
		},
	},
	CmpArgs{LT, intervalType, intervalType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DInterval).Duration.Compare(right.(DInterval).Duration) < 0), nil
		},
	},

	CmpArgs{LE, stringType, stringType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DString) <= right.(DString)), nil
		},
	},
	CmpArgs{LE, bytesType, bytesType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DBytes) <= right.(DBytes)), nil
		},
	},
	CmpArgs{LE, boolType, boolType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(!left.(DBool) || right.(DBool)), nil
		},
	},
	CmpArgs{LE, intType, intType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DInt) <= right.(DInt)), nil
		},
	},
	CmpArgs{LE, floatType, floatType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DFloat) <= right.(DFloat)), nil
		},
	},
	CmpArgs{LE, decimalType, decimalType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			l := left.(*DDecimal).Dec
			r := right.(*DDecimal).Dec
			return DBool(l.Cmp(&r) <= 0), nil
		},
	},
	CmpArgs{LE, floatType, intType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DFloat) <= DFloat(right.(DInt))), nil
		},
	},
	CmpArgs{LE, intType, floatType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(DFloat(left.(DInt)) <= right.(DFloat)), nil
		},
	},
	CmpArgs{LE, decimalType, intType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			l := left.(*DDecimal).Dec
			r := inf.NewDec(int64(right.(DInt)), 0)
			return DBool(l.Cmp(r) <= 0), nil
		},
	},
	CmpArgs{LE, intType, decimalType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			l := inf.NewDec(int64(left.(DInt)), 0)
			r := right.(*DDecimal).Dec
			return DBool(l.Cmp(&r) <= 0), nil
		},
	},
	CmpArgs{LE, decimalType, floatType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			l := left.(*DDecimal).Dec
			r := decimal.NewDecFromFloat(float64(right.(DFloat)))
			return DBool(l.Cmp(r) <= 0), nil
		},
	},
	CmpArgs{LE, floatType, decimalType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			l := decimal.NewDecFromFloat(float64(left.(DFloat)))
			r := right.(*DDecimal).Dec
			return DBool(l.Cmp(&r) <= 0), nil
		},
	},
	CmpArgs{LE, dateType, dateType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DDate) <= right.(DDate)), nil
		},
	},
	CmpArgs{LE, timestampType, timestampType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return !DBool(right.(DTimestamp).Before(left.(DTimestamp).Time)), nil
		},
	},
	CmpArgs{LE, intervalType, intervalType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DInterval).Duration.Compare(right.(DInterval).Duration) <= 0), nil
		},
	},

	CmpArgs{Like, stringType, stringType}: {
		fn: func(ctx EvalContext, left Datum, right Datum) (DBool, error) {
			pattern := string(right.(DString))
			like := optimizedLikeFunc(pattern)
			if like == nil {
				key := likeKey(pattern)
				re, err := ctx.ReCache.GetRegexp(key)
				if err != nil {
					return DBool(false), fmt.Errorf("LIKE regexp compilation failed: %v", err)
				}
				like = re.MatchString
			}
			return DBool(like(string(left.(DString)))), nil
		},
	},

	CmpArgs{SimilarTo, stringType, stringType}: {
		fn: func(ctx EvalContext, left Datum, right Datum) (DBool, error) {
			key := similarToKey(right.(DString))
			re, err := ctx.ReCache.GetRegexp(key)
			if err != nil {
				return DBool(false), err
			}
			return DBool(re.MatchString(string(left.(DString)))), nil
		},
	},
}

var evalTupleEQ = CmpOp{
	fn: func(ctx EvalContext, ldatum, rdatum Datum) (DBool, error) {
		left := ldatum.(DTuple)
		right := rdatum.(DTuple)
		if len(left) != len(right) {
			return DBool(false), nil
		}
		for i := range left {
			d, err := evalComparison(ctx, EQ, left[i], right[i])
			if err != nil {
				return DBool(false), err
			}
			if v, err := GetBool(d); err != nil {
				return DBool(false), err
			} else if !v {
				return v, nil
			}
		}
		return DBool(true), nil
	},
}

var evalTupleIN = CmpOp{
	fn: func(_ EvalContext, arg, values Datum) (DBool, error) {
		if arg == DNull {
			return DBool(false), nil
		}

		vtuple := values.(DTuple)
		i := sort.Search(len(vtuple), func(i int) bool { return vtuple[i].Compare(arg) >= 0 })
		found := i < len(vtuple) && vtuple[i].Compare(arg) == 0
		return DBool(found), nil
	},
}

func init() {
	// This avoids an init-loop if we try to initialize this operation when
	// cmpOps is declared. The loop is caused by evalTupleEQ using cmpOps
	// internally.
	CmpOps[CmpArgs{EQ, tupleType, tupleType}] = evalTupleEQ

	CmpOps[CmpArgs{In, boolType, tupleType}] = evalTupleIN
	CmpOps[CmpArgs{In, intType, tupleType}] = evalTupleIN
	CmpOps[CmpArgs{In, floatType, tupleType}] = evalTupleIN
	CmpOps[CmpArgs{In, stringType, tupleType}] = evalTupleIN
	CmpOps[CmpArgs{In, bytesType, tupleType}] = evalTupleIN
	CmpOps[CmpArgs{In, dateType, tupleType}] = evalTupleIN
	CmpOps[CmpArgs{In, timestampType, tupleType}] = evalTupleIN
	CmpOps[CmpArgs{In, intervalType, tupleType}] = evalTupleIN
	CmpOps[CmpArgs{In, tupleType, tupleType}] = evalTupleIN
}

// EvalContext defines the context in which to evaluate an expression, allowing
// the retrieval of state such as the node ID or statement start time.
type EvalContext struct {
	NodeID roachpb.NodeID
	// The statement timestamp. May be different for every statement.
	// Used for statement_timestamp().
	stmtTimestamp DTimestamp
	// The transaction timestamp. Needs to stay stable for the lifetime
	// of a transaction. Used for now(), current_timestamp(),
	// transaction_timestamp() and the like.
	txnTimestamp DTimestamp
	// The cluster timestamp. Needs to be stable for the lifetime of the
	// transaction. Used for cluster_logical_timestamp().
	clusterTimestamp roachpb.Timestamp

	ReCache     *RegexpCache
	GetLocation func() (*time.Location, error)
	Args        MapArgs

	// TODO(mjibson): remove prepareOnly in favor of a 2-step prepare-exec solution
	// that is also able to save the plan to skip work during the exec step.
	PrepareOnly bool
}

// GetStmtTimestamp retrieves the current statement timestamp as per
// the evaluation context. The timestamp is guaranteed to be nonzero.
func (ctx *EvalContext) GetStmtTimestamp() DTimestamp {
	// TODO(knz) a zero timestamp should never be read, even during
	// Prepare. This will need to be addressed.
	if !ctx.PrepareOnly && ctx.stmtTimestamp.Time.IsZero() {
		panic("zero statement timestamp in EvalContext")
	}
	return ctx.stmtTimestamp
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
	var val, sp big.Int
	val.SetInt64(ctx.clusterTimestamp.WallTime)
	val.Mul(&val, tenBillion)
	sp.SetInt64(int64(ctx.clusterTimestamp.Logical))
	val.Add(&val, &sp)
	// Store the result.
	res := &DDecimal{}
	res.Dec.SetUnscaledBig(&val)
	// Shift 10 decimals to the right, so that the logical
	// field appears as fractional part.
	res.Dec.SetScale(10)

	return res
}

// GetTxnTimestamp retrieves the current transaction timestamp as per
// the evaluation context. The timestamp is guaranteed to be nonzero.
func (ctx *EvalContext) GetTxnTimestamp() DTimestamp {
	// TODO(knz) a zero timestamp should never be read, even during
	// Prepare. This will need to be addressed.
	if !ctx.PrepareOnly && ctx.txnTimestamp.Time.IsZero() {
		panic("zero transaction timestamp in EvalContext")
	}
	return ctx.txnTimestamp
}

// SetTxnTimestamp sets the corresponding timestamp in the EvalContext.
func (ctx *EvalContext) SetTxnTimestamp(ts time.Time) {
	ctx.txnTimestamp.Time = ts
}

// SetStmtTimestamp sets the corresponding timestamp in the EvalContext.
func (ctx *EvalContext) SetStmtTimestamp(ts time.Time) {
	ctx.stmtTimestamp.Time = ts
}

var tenBillion = big.NewInt(1e10)

// SetClusterTimestamp sets the corresponding timestamp in the EvalContext.
func (ctx *EvalContext) SetClusterTimestamp(ts roachpb.Timestamp) {
	ctx.clusterTimestamp = ts
}

var defaultContext = EvalContext{
	GetLocation: func() (*time.Location, error) {
		return time.UTC, nil
	},
}

// makeDDate constructs a DDate from a time.Time in the session time zone.
func (ctx EvalContext) makeDDate(t time.Time) (DDate, error) {
	loc, err := ctx.GetLocation()
	if err != nil {
		return DDate(0), err
	}
	year, month, day := t.In(loc).Date()
	secs := time.Date(year, month, day, 0, 0, 0, 0, time.UTC).Unix()
	return DDate(secs / secondsInDay), nil
}

// Eval implements the Expr interface.
func (expr *AndExpr) Eval(ctx EvalContext) (Datum, error) {
	left, err := expr.Left.Eval(ctx)
	if err != nil {
		return DNull, err
	}
	if left != DNull {
		if v, err := GetBool(left); err != nil {
			return DNull, err
		} else if !v {
			return v, nil
		}
	}
	right, err := expr.Right.Eval(ctx)
	if err != nil {
		return DNull, err
	}
	if right == DNull {
		return DNull, nil
	}
	if v, err := GetBool(right); err != nil {
		return DNull, err
	} else if !v {
		return v, nil
	}
	return left, nil
}

// Eval implements the Expr interface.
func (expr *BinaryExpr) Eval(ctx EvalContext) (Datum, error) {
	left, err := expr.Left.Eval(ctx)
	if err != nil {
		return nil, err
	}
	if left == DNull {
		return DNull, nil
	}
	right, err := expr.Right.Eval(ctx)
	if err != nil {
		return nil, err
	}
	if right == DNull {
		return DNull, nil
	}

	if expr.fn.fn == nil {
		if _, err := expr.TypeCheck(ctx.Args); err != nil {
			return nil, err
		}
	}
	return expr.fn.fn(ctx, left, right)
}

// Eval implements the Expr interface.
func (expr *CaseExpr) Eval(ctx EvalContext) (Datum, error) {
	if expr.Expr != nil {
		// CASE <val> WHEN <expr> THEN ...
		//
		// For each "when" expression we compare for equality to <val>.
		val, err := expr.Expr.Eval(ctx)
		if err != nil {
			return DNull, err
		}

		for _, when := range expr.Whens {
			arg, err := when.Cond.Eval(ctx)
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
				return when.Val.Eval(ctx)
			}
		}
	} else {
		// CASE WHEN <bool-expr> THEN ...
		for _, when := range expr.Whens {
			d, err := when.Cond.Eval(ctx)
			if err != nil {
				return DNull, err
			}
			if v, err := GetBool(d); err != nil {
				return DNull, err
			} else if v {
				return when.Val.Eval(ctx)
			}
		}
	}

	if expr.Else != nil {
		return expr.Else.Eval(ctx)
	}
	return DNull, nil
}

// Eval implements the Expr interface.
func (expr *CastExpr) Eval(ctx EvalContext) (Datum, error) {
	d, err := expr.Expr.Eval(ctx)
	if err != nil {
		return nil, err
	}

	// NULL cast to anything is NULL.
	if d == DNull {
		return d, nil
	}

	switch expr.Type.(type) {
	case *BoolType:
		switch v := d.(type) {
		case DBool:
			return d, nil
		case DInt:
			return DBool(v != 0), nil
		case DFloat:
			return DBool(v != 0), nil
		case *DDecimal:
			return DBool(v.Sign() != 0), nil
		case DString:
			// TODO(pmattis): strconv.ParseBool is more permissive than the SQL
			// spec. Is that ok?
			b, err := strconv.ParseBool(string(v))
			if err != nil {
				return nil, err
			}
			return DBool(b), nil
		}

	case *IntType:
		switch v := d.(type) {
		case DBool:
			if v {
				return DInt(1), nil
			}
			return DInt(0), nil
		case DInt:
			return d, nil
		case DFloat:
			f, err := round(float64(v), 0)
			if err != nil {
				panic(fmt.Sprintf("round should never fail with digits hardcoded to 0: %s", err))
			}
			return DInt(f.(DFloat)), nil
		case *DDecimal:
			dec := new(inf.Dec)
			dec.Round(&v.Dec, 0, inf.RoundHalfUp)
			i, ok := dec.Unscaled()
			if !ok {
				return nil, errIntOutOfRange
			}
			return DInt(i), nil
		case DString:
			i, err := strconv.ParseInt(string(v), 0, 64)
			if err != nil {
				return nil, err
			}
			return DInt(i), nil
		}

	case *FloatType:
		switch v := d.(type) {
		case DBool:
			if v {
				return DFloat(1), nil
			}
			return DFloat(0), nil
		case DInt:
			return DFloat(v), nil
		case DFloat:
			return d, nil
		case *DDecimal:
			f, err := decimal.Float64FromDec(&v.Dec)
			if err != nil {
				return nil, errFloatOutOfRange
			}
			return DFloat(f), nil
		case DString:
			f, err := strconv.ParseFloat(string(v), 64)
			if err != nil {
				return nil, err
			}
			return DFloat(f), nil
		}

	case *DecimalType:
		dd := &DDecimal{}
		switch v := d.(type) {
		case DBool:
			if v {
				dd.SetUnscaled(1)
			}
			return dd, nil
		case DInt:
			dd.SetUnscaled(int64(v))
			return dd, nil
		case DFloat:
			decimal.SetFromFloat(&dd.Dec, float64(v))
			return dd, nil
		case *DDecimal:
			return d, nil
		case DString:
			if _, ok := dd.SetString(string(v)); !ok {
				return nil, fmt.Errorf("could not parse string %q as decimal", v)
			}
			return dd, nil
		}

	case *StringType:
		var s DString
		switch t := d.(type) {
		case DBool, DInt, DFloat, *DDecimal, dNull:
			s = DString(d.String())
		case DString:
			s = t
		case DBytes:
			if !utf8.ValidString(string(t)) {
				return nil, fmt.Errorf("invalid utf8: %q", string(t))
			}
			s = DString(t)
		}
		if c, ok := expr.Type.(*StringType); ok {
			// If the CHAR type specifies a limit we truncate to that limit:
			//   'hello'::CHAR(2) -> 'he'
			if c.N > 0 && c.N < len(s) {
				s = s[:c.N]
			}
		}
		return s, nil

	case *BytesType:
		switch t := d.(type) {
		case DString:
			return DBytes(t), nil
		case DBytes:
			return d, nil
		}

	case *DateType:
		switch d := d.(type) {
		case DString:
			return ParseDate(d)
		case DTimestamp:
			return ctx.makeDDate(d.Time)
		}

	case *TimestampType:
		switch d := d.(type) {
		case DString:
			return ctx.ParseTimestamp(d)
		case DDate:
			loc, err := ctx.GetLocation()
			if err != nil {
				return nil, err
			}
			year, month, day := time.Unix(int64(d)*secondsInDay, 0).UTC().Date()
			return DTimestamp{Time: time.Date(year, month, day, 0, 0, 0, 0, loc)}, nil
		}

	case *IntervalType:
		switch d.(type) {
		case DString:
			// We use the Golang format for specifying duration.
			// TODO(vivek): we might consider using the postgres format as well.
			d, err := time.ParseDuration(string(d.(DString)))
			return DInterval{Duration: duration.Duration{Nanos: d.Nanoseconds()}}, err

		case DInt:
			// An integer duration represents a duration in nanoseconds.
			return DInterval{Duration: duration.Duration{Nanos: int64(d.(DInt))}}, nil
		}
	}

	return nil, fmt.Errorf("invalid cast: %s -> %s", d.Type(), expr.Type)
}

// Eval implements the Expr interface.
func (expr *CoalesceExpr) Eval(ctx EvalContext) (Datum, error) {
	for _, e := range expr.Exprs {
		d, err := e.Eval(ctx)
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
	left, err := expr.Left.Eval(ctx)
	if err != nil {
		return DNull, err
	}
	right, err := expr.Right.Eval(ctx)
	if err != nil {
		return DNull, err
	}

	if left == DNull || right == DNull {
		switch expr.Operator {
		case IsDistinctFrom:
			return !DBool(left == DNull && right == DNull), nil
		case IsNotDistinctFrom:
			return DBool(left == DNull && right == DNull), nil
		case Is:
			// IS and IS NOT can compare against NULL.
			return DBool(left == right), nil
		case IsNot:
			return DBool(left != right), nil
		default:
			return DNull, nil
		}
	}

	// Make sure the expression's cmpOp function is memoized
	if expr.fn.fn == nil {
		if _, err := expr.TypeCheck(ctx.Args); err != nil {
			return DNull, err
		}

		// If cmpOp's function is still nil, return unsupported op error
		if expr.fn.fn == nil {
			return nil, fmt.Errorf("unsupported comparison operator: <%s> %s <%s>",
				left.Type(), expr.Operator, right.Type())
		}
	}

	_, newLeft, newRight, not := foldComparisonExpr(expr.Operator, left, right)
	d, err := expr.fn.fn(ctx, newLeft, newRight)
	if err == nil && not {
		return !d, nil
	}
	return d, err
}

// Eval implements the Expr interface.
func (t *ExistsExpr) Eval(ctx EvalContext) (Datum, error) {
	// Exists expressions are handled during subquery expansion.
	return nil, util.Errorf("unhandled type %T", t)
}

// Eval implements the Expr interface.
func (expr *FuncExpr) Eval(ctx EvalContext) (Datum, error) {
	args := make(DTuple, 0, len(expr.Exprs))
	types := make(ArgTypes, 0, len(expr.Exprs))
	for _, e := range expr.Exprs {
		arg, err := e.Eval(ctx)
		if err != nil {
			return DNull, err
		}
		args = append(args, arg)
		types = append(types, reflect.TypeOf(arg))
	}

	if expr.fn.fn == nil {
		if _, err := expr.TypeCheck(ctx.Args); err != nil {
			return DNull, err
		}
	}

	if !expr.fn.Types.match(types) {
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
func (expr *IfExpr) Eval(ctx EvalContext) (Datum, error) {
	cond, err := expr.Cond.Eval(ctx)
	if err != nil {
		return DNull, err
	}
	if cond == DBool(true) {
		return expr.True.Eval(ctx)
	}
	return expr.Else.Eval(ctx)
}

// Eval implements the Expr interface.
func (expr *IsOfTypeExpr) Eval(ctx EvalContext) (Datum, error) {
	d, err := expr.Expr.Eval(ctx)
	if err != nil {
		return DNull, err
	}

	result := DBool(true)
	if expr.Not {
		result = !result
	}

	switch d.(type) {
	case DBool:
		for _, t := range expr.Types {
			if _, ok := t.(*BoolType); ok {
				return result, nil
			}
		}

	case DInt:
		for _, t := range expr.Types {
			if _, ok := t.(*IntType); ok {
				return result, nil
			}
		}

	case DFloat:
		for _, t := range expr.Types {
			if _, ok := t.(*FloatType); ok {
				return result, nil
			}
		}

	case *DDecimal:
		for _, t := range expr.Types {
			if _, ok := t.(*DecimalType); ok {
				return result, nil
			}
		}

	case DString:
		for _, t := range expr.Types {
			if _, ok := t.(*StringType); ok {
				return result, nil
			}
		}

	case DBytes:
		for _, t := range expr.Types {
			if _, ok := t.(*BytesType); ok {
				return result, nil
			}
		}

	case DDate:
		for _, t := range expr.Types {
			if _, ok := t.(*DateType); ok {
				return result, nil
			}
		}

	case DTimestamp:
		for _, t := range expr.Types {
			if _, ok := t.(*TimestampType); ok {
				return result, nil
			}
		}

	case DInterval:
		for _, t := range expr.Types {
			if _, ok := t.(*IntervalType); ok {
				return result, nil
			}
		}
	}

	return !result, nil
}

// Eval implements the Expr interface.
func (expr *NotExpr) Eval(ctx EvalContext) (Datum, error) {
	d, err := expr.Expr.Eval(ctx)
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
	return !v, nil
}

// Eval implements the Expr interface.
func (expr *NullIfExpr) Eval(ctx EvalContext) (Datum, error) {
	expr1, err := expr.Expr1.Eval(ctx)
	if err != nil {
		return DNull, err
	}
	expr2, err := expr.Expr2.Eval(ctx)
	if err != nil {
		return DNull, err
	}
	cond, err := evalComparison(ctx, EQ, expr1, expr2)
	if err != nil {
		return DNull, err
	}
	if cond == DBool(true) {
		return DNull, nil
	}
	return expr1, nil
}

// Eval implements the Expr interface.
func (expr *OrExpr) Eval(ctx EvalContext) (Datum, error) {
	left, err := expr.Left.Eval(ctx)
	if err != nil {
		return DNull, err
	}
	if left != DNull {
		if v, err := GetBool(left); err != nil {
			return DNull, err
		} else if v {
			return v, nil
		}
	}
	right, err := expr.Right.Eval(ctx)
	if err != nil {
		return DNull, err
	}
	if right == DNull {
		return DNull, nil
	}
	if v, err := GetBool(right); err != nil {
		return DNull, err
	} else if v {
		return v, nil
	}
	if left == DNull {
		return DNull, nil
	}
	return DBool(false), nil
}

// Eval implements the Expr interface.
func (t *QualifiedName) Eval(_ EvalContext) (Datum, error) {
	return nil, fmt.Errorf("qualified name \"%s\" not found", t)
}

// Eval implements the Expr interface.
func (t *RangeCond) Eval(_ EvalContext) (Datum, error) {
	return nil, util.Errorf("unhandled type %T", t)
}

// Eval implements the Expr interface.
func (t *Subquery) Eval(_ EvalContext) (Datum, error) {
	// Subquery expressions are handled during subquery expansion.
	return nil, util.Errorf("unhandled type %T", t)
}

// Eval implements the Expr interface.
func (expr *UnaryExpr) Eval(ctx EvalContext) (Datum, error) {
	d, err := expr.Expr.Eval(ctx)
	if err != nil {
		return DNull, err
	}
	if expr.fn.fn == nil {
		if _, err := expr.TypeCheck(ctx.Args); err != nil {
			return DNull, err
		}
	}
	if expr.dtype != reflect.TypeOf(d) {
		// The argument type no longer match the memoized function. This happens
		// when a non-NULL argument becomes NULL. For example, "SELECT -col FROM
		// table" where col is nullable. The SELECT does not error, but returns a
		// NULL value for that select expression.
		return DNull, nil
	}
	return expr.fn.fn(ctx, d)
}

// Eval implements the Expr interface.
func (t Array) Eval(_ EvalContext) (Datum, error) {
	return nil, util.Errorf("unhandled type %T", t)
}

// Eval implements the Expr interface.
func (t DefaultVal) Eval(_ EvalContext) (Datum, error) {
	return nil, util.Errorf("unhandled type %T", t)
}

// Eval implements the Expr interface.
func (t *IntVal) Eval(_ EvalContext) (Datum, error) {
	if t.Val < 0 {
		return nil, fmt.Errorf("integer value out of range: %s", t)
	}
	return DInt(t.Val), nil
}

// Eval implements the Expr interface.
func (t NumVal) Eval(_ EvalContext) (Datum, error) {
	// TODO(nvanbenschoten) this should be changed to
	// evaulate NumVals as DDecimals by default, once
	// type coercion is improved.
	//
	// d := new(inf.Dec)
	// if _, ok := d.SetString(string(t)); !ok {
	//    return ...
	// }
	// return DDecimal{Dec: d}, nil

	v, err := strconv.ParseFloat(string(t), 64)
	if err != nil {
		return DNull, err
	}
	return DFloat(v), nil
}

func evalExprs(ctx EvalContext, exprs []Expr) (Datum, error) {
	tuple := make(DTuple, 0, len(exprs))
	for _, v := range exprs {
		d, err := v.Eval(ctx)
		if err != nil {
			return DNull, err
		}
		tuple = append(tuple, d)
	}
	return tuple, nil
}

// Eval implements the Expr interface.
func (t *Row) Eval(ctx EvalContext) (Datum, error) {
	return evalExprs(ctx, t.Exprs)
}

// Eval implements the Expr interface.
func (t *Tuple) Eval(ctx EvalContext) (Datum, error) {
	return evalExprs(ctx, t.Exprs)
}

// Eval implements the Expr interface.
func (t ValArg) Eval(_ EvalContext) (Datum, error) {
	return DValArg{name: t.name}, nil
}

// Eval implements the Expr interface.
func (t DBool) Eval(_ EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the Expr interface.
func (t DBytes) Eval(_ EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the Expr interface.
func (t DDate) Eval(_ EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the Expr interface.
func (t DFloat) Eval(_ EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the Expr interface.
func (t *DDecimal) Eval(_ EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the Expr interface.
func (t DInt) Eval(_ EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the Expr interface.
func (t DInterval) Eval(_ EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the Expr interface.
func (t dNull) Eval(_ EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the Expr interface.
func (t DString) Eval(_ EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the Expr interface.
func (t DTimestamp) Eval(_ EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the Expr interface.
func (t DTuple) Eval(_ EvalContext) (Datum, error) {
	return t, nil
}

// Eval implements the Expr interface.
func (t DValArg) Eval(_ EvalContext) (Datum, error) {
	return t, nil
}

func evalComparison(ctx EvalContext, op ComparisonOp, left, right Datum) (Datum, error) {
	if left == DNull || right == DNull {
		return DNull, nil
	}

	if f, ok := CmpOps[CmpArgs{op, reflect.TypeOf(left), reflect.TypeOf(right)}]; ok {
		return f.fn(ctx, left, right)
	}

	return nil, fmt.Errorf("unsupported comparison operator: <%s> %s <%s>",
		left.Type(), op, right.Type())
}

// foldComparisonExpr folds a given comparison operation and its datum into an
// equivalent operation that will hit in the cmpOps map, returning this new
// operation, along with potentially flipped operands and a "not" flag.
func foldComparisonExpr(op ComparisonOp, dummyLeft, dummyRight Datum) (ComparisonOp, Datum, Datum, bool) {
	switch op {
	case NE:
		// NE(left, right) is implemented as !EQ(left, right).
		return EQ, dummyLeft, dummyRight, true
	case GT:
		// GT(left, right) is implemented as LT(right, left)
		return LT, dummyRight, dummyLeft, false
	case GE:
		// GE(left, right) is implemented as LE(right, left)
		return LE, dummyRight, dummyLeft, false
	case NotIn:
		// NotIn(left, right) is implemented as !IN(left, right)
		return In, dummyLeft, dummyRight, true
	case NotLike:
		// NotLike(left, right) is implemented as !Like(left, right)
		return Like, dummyLeft, dummyRight, true
	case NotSimilarTo:
		// NotSimilarTo(left, right) is implemented as !SimilarTo(left, right)
		return SimilarTo, dummyLeft, dummyRight, true
	case IsDistinctFrom:
		// IsDistinctFrom(left, right) is implemented as !EQ(left, right)
		//
		// Note the special handling of NULLs and IS DISTINCT FROM is needed
		// before this expression fold.
		return EQ, dummyLeft, dummyRight, true
	case IsNotDistinctFrom:
		// IsNotDistinctFrom(left, right) is implemented as EQ(left, right)
		//
		// Note the special handling of NULLs and IS NOT DISTINCT FROM is needed
		// before this expression fold.
		return EQ, dummyLeft, dummyRight, false
	case Is:
		// Is(left, right) is implemented as EQ(left, right)
		//
		// Note the special handling of NULLs and IS is needed before this
		// expression fold.
		return EQ, dummyLeft, dummyRight, false
	case IsNot:
		// IsNot(left, right) is implemented as !EQ(left, right)
		//
		// Note the special handling of NULLs and IS NOT is needed before this
		// expression fold.
		return EQ, dummyLeft, dummyRight, true
	}
	return op, dummyLeft, dummyRight, false
}

// time.Time formats.
const (
	dateFormat                            = "2006-01-02"
	timestampFormat                       = "2006-01-02 15:04:05.999999999"
	timestampWithOffsetZoneFormat         = "2006-01-02 15:04:05.999999999-07:00"
	timestampWithNamedZoneFormat          = "2006-01-02 15:04:05.999999999 MST"
	timestampRFC3339NanoWithoutZoneFormat = "2006-01-02T15:04:05.999999999"
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
func ParseDate(s DString) (DDate, error) {
	// No need to ParseInLocation here because we're only parsing dates.
	for _, format := range dateFormats {
		if t, err := time.Parse(format, string(s)); err == nil {
			return defaultContext.makeDDate(t)
		}
	}
	return DDate(0), fmt.Errorf("could not parse %s in any supported date format", s)
}

// ParseTimestamp parses the timestamp.
func (ctx EvalContext) ParseTimestamp(s DString) (DTimestamp, error) {
	loc := time.UTC
	if ctx.GetLocation != nil {
		var err error
		if loc, err = ctx.GetLocation(); err != nil {
			return DTimestamp{}, err
		}
	}

	str := string(s)

	for _, format := range timeFormats {
		if t, err := time.ParseInLocation(format, str, loc); err == nil {
			return DTimestamp{Time: t}, nil
		}
	}

	return DTimestamp{}, fmt.Errorf("could not parse %s in any supported timestamp format", s)
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
