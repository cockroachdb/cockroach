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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package parser

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
)

var (
	errZeroModulus = errors.New("zero modulus")
	errDivByZero   = errors.New("division by zero")
)

// secondsInDay is the number of seconds in a day.
const secondsInDay = 24 * 60 * 60

type unaryOp struct {
	returnType Datum
	fn         func(EvalContext, Datum) (Datum, error)
}

type unaryArgs struct {
	op      UnaryOp
	argType reflect.Type
}

// unaryOps contains the unary operations indexed by operation type and
// argument type.
var unaryOps = map[unaryArgs]unaryOp{
	unaryArgs{UnaryPlus, intType}: {
		returnType: DummyInt,
		fn: func(_ EvalContext, d Datum) (Datum, error) {
			return d, nil
		},
	},
	unaryArgs{UnaryPlus, floatType}: {
		returnType: DummyFloat,
		fn: func(_ EvalContext, d Datum) (Datum, error) {
			return d, nil
		},
	},

	unaryArgs{UnaryMinus, intType}: {
		returnType: DummyInt,
		fn: func(_ EvalContext, d Datum) (Datum, error) {
			return -d.(DInt), nil
		},
	},
	unaryArgs{UnaryMinus, floatType}: {
		returnType: DummyFloat,
		fn: func(_ EvalContext, d Datum) (Datum, error) {
			return -d.(DFloat), nil
		},
	},

	unaryArgs{UnaryComplement, intType}: {
		returnType: DummyInt,
		fn: func(_ EvalContext, d Datum) (Datum, error) {
			return ^d.(DInt), nil
		},
	},
}

type binOp struct {
	returnType Datum
	fn         func(EvalContext, Datum, Datum) (Datum, error)
}

type binArgs struct {
	op        BinaryOp
	leftType  reflect.Type
	rightType reflect.Type
}

// binOps contains the binary operations indexed by operation type and argument
// types.
var binOps = map[binArgs]binOp{
	binArgs{Bitand, intType, intType}: {
		returnType: DummyInt,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DInt) & right.(DInt), nil
		},
	},

	binArgs{Bitor, intType, intType}: {
		returnType: DummyInt,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DInt) | right.(DInt), nil
		},
	},

	binArgs{Bitxor, intType, intType}: {
		returnType: DummyInt,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DInt) ^ right.(DInt), nil
		},
	},

	// TODO(pmattis): Overflow/underflow checks?

	binArgs{Plus, intType, intType}: {
		returnType: DummyInt,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DInt) + right.(DInt), nil
		},
	},
	binArgs{Plus, floatType, floatType}: {
		returnType: DummyFloat,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DFloat) + right.(DFloat), nil
		},
	},
	binArgs{Plus, dateType, intType}: {
		returnType: DummyDate,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DDate) + DDate(right.(DInt)), nil
		},
	},
	binArgs{Plus, intType, dateType}: {
		returnType: DummyDate,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return DDate(left.(DInt)) + right.(DDate), nil
		},
	},
	binArgs{Plus, timestampType, intervalType}: {
		returnType: DummyTimestamp,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return DTimestamp{Time: left.(DTimestamp).Add(right.(DInterval).Duration)}, nil
		},
	},
	binArgs{Plus, intervalType, timestampType}: {
		returnType: DummyTimestamp,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return DTimestamp{Time: right.(DTimestamp).Add(left.(DInterval).Duration)}, nil
		},
	},
	binArgs{Plus, intervalType, intervalType}: {
		returnType: DummyInterval,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return DInterval{Duration: left.(DInterval).Duration + right.(DInterval).Duration}, nil
		},
	},
	binArgs{Minus, intType, intType}: {
		returnType: DummyInt,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DInt) - right.(DInt), nil
		},
	},
	binArgs{Minus, floatType, floatType}: {
		returnType: DummyFloat,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DFloat) - right.(DFloat), nil
		},
	},
	binArgs{Minus, dateType, intType}: {
		returnType: DummyDate,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DDate) - DDate(right.(DInt)), nil
		},
	},
	binArgs{Minus, dateType, dateType}: {
		returnType: DummyInt,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return DInt(left.(DDate) - right.(DDate)), nil
		},
	},
	binArgs{Minus, timestampType, timestampType}: {
		returnType: DummyInterval,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return DInterval{Duration: left.(DTimestamp).Sub(right.(DTimestamp).Time)}, nil
		},
	},
	binArgs{Minus, timestampType, intervalType}: {
		returnType: DummyTimestamp,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return DTimestamp{Time: left.(DTimestamp).Add(-right.(DInterval).Duration)}, nil
		},
	},
	binArgs{Minus, intervalType, intervalType}: {
		returnType: DummyInterval,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return DInterval{Duration: left.(DInterval).Duration - right.(DInterval).Duration}, nil
		},
	},

	binArgs{Mult, intType, intType}: {
		returnType: DummyInt,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DInt) * right.(DInt), nil
		},
	},
	binArgs{Mult, floatType, floatType}: {
		returnType: DummyFloat,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DFloat) * right.(DFloat), nil
		},
	},
	binArgs{Mult, intType, intervalType}: {
		returnType: DummyInterval,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return DInterval{Duration: time.Duration(left.(DInt)) * right.(DInterval).Duration}, nil
		},
	},
	binArgs{Mult, intervalType, intType}: {
		returnType: DummyInterval,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return DInterval{Duration: left.(DInterval).Duration * time.Duration(right.(DInt))}, nil
		},
	},

	binArgs{Div, intType, intType}: {
		returnType: DummyFloat,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			rInt := right.(DInt)
			if rInt == 0 {
				return nil, errDivByZero
			}
			return DFloat(left.(DInt)) / DFloat(rInt), nil
		},
	},
	binArgs{Div, floatType, floatType}: {
		returnType: DummyFloat,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DFloat) / right.(DFloat), nil
		},
	},
	binArgs{Div, intervalType, intType}: {
		returnType: DummyInterval,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			rInt := right.(DInt)
			if rInt == 0 {
				return nil, errDivByZero
			}
			return DInterval{Duration: left.(DInterval).Duration / time.Duration(rInt)}, nil
		},
	},

	binArgs{Mod, intType, intType}: {
		returnType: DummyInt,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			r := right.(DInt)
			if r == 0 {
				return nil, errZeroModulus
			}
			return left.(DInt) % r, nil
		},
	},
	binArgs{Mod, floatType, floatType}: {
		returnType: DummyFloat,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return DFloat(math.Mod(float64(left.(DFloat)), float64(right.(DFloat)))), nil
		},
	},

	binArgs{Concat, stringType, stringType}: {
		returnType: DummyString,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DString) + right.(DString), nil
		},
	},
	binArgs{Concat, bytesType, bytesType}: {
		returnType: DummyBytes,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DBytes) + right.(DBytes), nil
		},
	},

	// TODO(pmattis): Check that the shift is valid.
	binArgs{LShift, intType, intType}: {
		returnType: DummyInt,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DInt) << uint(right.(DInt)), nil
		},
	},
	binArgs{RShift, intType, intType}: {
		returnType: DummyInt,
		fn: func(_ EvalContext, left Datum, right Datum) (Datum, error) {
			return left.(DInt) >> uint(right.(DInt)), nil
		},
	},
}

type cmpArgs struct {
	op        ComparisonOp
	leftType  reflect.Type
	rightType reflect.Type
}

type cmpOp struct {
	fn func(EvalContext, Datum, Datum) (DBool, error)
}

var cmpOpResultType = reflect.New(reflect.TypeOf(cmpOp{}).Field(0).Type.Out(0)).Elem().Interface().(DBool)

// cmpOps contains the comparison operations indexed by operation type and
// argument types.
var cmpOps = map[cmpArgs]cmpOp{
	cmpArgs{EQ, stringType, stringType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DString) == right.(DString)), nil
		},
	},
	cmpArgs{EQ, bytesType, bytesType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DBytes) == right.(DBytes)), nil
		},
	},
	cmpArgs{EQ, boolType, boolType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DBool) == right.(DBool)), nil
		},
	},
	cmpArgs{EQ, intType, intType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DInt) == right.(DInt)), nil
		},
	},
	cmpArgs{EQ, floatType, floatType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DFloat) == right.(DFloat)), nil
		},
	},
	cmpArgs{EQ, floatType, intType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DFloat) == DFloat(right.(DInt))), nil
		},
	},
	cmpArgs{EQ, intType, floatType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(DFloat(left.(DInt)) == right.(DFloat)), nil
		},
	},
	cmpArgs{EQ, dateType, dateType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DDate) == right.(DDate)), nil
		},
	},
	cmpArgs{EQ, timestampType, timestampType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DTimestamp).Equal(right.(DTimestamp).Time)), nil
		},
	},
	cmpArgs{EQ, intervalType, intervalType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DInterval) == right.(DInterval)), nil
		},
	},

	cmpArgs{LT, stringType, stringType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DString) < right.(DString)), nil
		},
	},
	cmpArgs{LT, bytesType, bytesType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DBytes) < right.(DBytes)), nil
		},
	},
	cmpArgs{LT, boolType, boolType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(!left.(DBool) && right.(DBool)), nil
		},
	},
	cmpArgs{LT, intType, intType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DInt) < right.(DInt)), nil
		},
	},
	cmpArgs{LT, floatType, floatType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DFloat) < right.(DFloat)), nil
		},
	},
	cmpArgs{LT, floatType, intType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DFloat) < DFloat(right.(DInt))), nil
		},
	},
	cmpArgs{LT, intType, floatType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(DFloat(left.(DInt)) < right.(DFloat)), nil
		},
	},
	cmpArgs{LT, dateType, dateType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DDate) < right.(DDate)), nil
		},
	},
	cmpArgs{LT, timestampType, timestampType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DTimestamp).Before(right.(DTimestamp).Time)), nil
		},
	},
	cmpArgs{LT, intervalType, intervalType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DInterval).Duration < right.(DInterval).Duration), nil
		},
	},

	cmpArgs{LE, stringType, stringType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DString) <= right.(DString)), nil
		},
	},
	cmpArgs{LE, bytesType, bytesType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DBytes) <= right.(DBytes)), nil
		},
	},
	cmpArgs{LE, boolType, boolType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(!left.(DBool) || right.(DBool)), nil
		},
	},
	cmpArgs{LE, intType, intType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DInt) <= right.(DInt)), nil
		},
	},
	cmpArgs{LE, floatType, floatType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DFloat) <= right.(DFloat)), nil
		},
	},
	cmpArgs{LE, floatType, intType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DFloat) <= DFloat(right.(DInt))), nil
		},
	},
	cmpArgs{LE, intType, floatType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(DFloat(left.(DInt)) <= right.(DFloat)), nil
		},
	},
	cmpArgs{LE, dateType, dateType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DDate) <= right.(DDate)), nil
		},
	},
	cmpArgs{LE, timestampType, timestampType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return !DBool(right.(DTimestamp).Before(left.(DTimestamp).Time)), nil
		},
	},
	cmpArgs{LE, intervalType, intervalType}: {
		fn: func(_ EvalContext, left Datum, right Datum) (DBool, error) {
			return DBool(left.(DInterval).Duration <= right.(DInterval).Duration), nil
		},
	},

	cmpArgs{Like, stringType, stringType}: {
		fn: func(ctx EvalContext, left Datum, right Datum) (DBool, error) {
			key := likeKey(right.(DString))
			re, err := ctx.ReCache.GetRegexp(key)
			if err != nil {
				panic(fmt.Sprintf("LIKE regexp compilations should not fail: %v", err))
			}
			return DBool(re.MatchString(string(left.(DString)))), nil
		},
	},

	cmpArgs{SimilarTo, stringType, stringType}: {
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

var evalTupleEQ = cmpOp{
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
			if v, err := getBool(d); err != nil {
				return DBool(false), err
			} else if !v {
				return v, nil
			}
		}
		return DBool(true), nil
	},
}

var evalTupleIN = cmpOp{
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
	cmpOps[cmpArgs{EQ, tupleType, tupleType}] = evalTupleEQ

	cmpOps[cmpArgs{In, boolType, tupleType}] = evalTupleIN
	cmpOps[cmpArgs{In, intType, tupleType}] = evalTupleIN
	cmpOps[cmpArgs{In, floatType, tupleType}] = evalTupleIN
	cmpOps[cmpArgs{In, stringType, tupleType}] = evalTupleIN
	cmpOps[cmpArgs{In, bytesType, tupleType}] = evalTupleIN
	cmpOps[cmpArgs{In, dateType, tupleType}] = evalTupleIN
	cmpOps[cmpArgs{In, timestampType, tupleType}] = evalTupleIN
	cmpOps[cmpArgs{In, intervalType, tupleType}] = evalTupleIN
	cmpOps[cmpArgs{In, tupleType, tupleType}] = evalTupleIN
}

// EvalContext defines the context in which to evaluate an expression, allowing
// the retrieval of state such as the node ID or statement start time.
type EvalContext struct {
	NodeID        roachpb.NodeID
	StmtTimestamp DTimestamp
	TxnTimestamp  DTimestamp
	ReCache       *RegexpCache
	GetLocation   func() (*time.Location, error)
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
		if v, err := getBool(left); err != nil {
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
	if v, err := getBool(right); err != nil {
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
		if _, err := expr.TypeCheck(); err != nil {
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
			if v, err := getBool(d); err != nil {
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
			if v, err := getBool(d); err != nil {
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
		return DNull, err
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
		case DString:
			// TODO(pmattis): strconv.ParseBool is more permissive than the SQL
			// spec. Is that ok?
			b, err := strconv.ParseBool(string(v))
			if err != nil {
				return DNull, err
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
			return DInt(v), nil
		case DString:
			i, err := strconv.ParseInt(string(v), 0, 64)
			if err != nil {
				return DNull, err
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
		case DString:
			f, err := strconv.ParseFloat(string(v), 64)
			if err != nil {
				return DNull, err
			}
			return DFloat(f), nil
		}

	case *StringType:
		var s DString
		switch t := d.(type) {
		case DBool, DInt, DFloat, dNull:
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
				return DNull, err
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
			return DInterval{Duration: d}, err

		case DInt:
			// An integer duration represents a duration in nanoseconds.
			return DInterval{Duration: time.Duration(d.(DInt))}, nil
		}
		// TODO(pmattis): unimplemented.
		// case *DecimalType:
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
		if _, err := expr.TypeCheck(); err != nil {
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
	return t.Subquery.Eval(ctx)
}

// Eval implements the Expr interface.
func (expr *FuncExpr) Eval(ctx EvalContext) (Datum, error) {
	args := make(DTuple, 0, len(expr.Exprs))
	types := make(typeList, 0, len(expr.Exprs))
	for _, e := range expr.Exprs {
		arg, err := e.Eval(ctx)
		if err != nil {
			return DNull, err
		}
		args = append(args, arg)
		types = append(types, reflect.TypeOf(arg))
	}

	if expr.fn.fn == nil {
		if _, err := expr.TypeCheck(); err != nil {
			return DNull, err
		}
	}

	if !expr.fn.match(types) {
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
	v, err := getBool(d)
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
		if v, err := getBool(left); err != nil {
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
	if v, err := getBool(right); err != nil {
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
	return nil, util.Errorf("unhandled type %T", t)
}

// Eval implements the Expr interface.
func (expr *UnaryExpr) Eval(ctx EvalContext) (Datum, error) {
	d, err := expr.Expr.Eval(ctx)
	if err != nil {
		return DNull, err
	}
	if expr.fn.fn == nil {
		if _, err := expr.TypeCheck(); err != nil {
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
	v, err := strconv.ParseFloat(string(t), 64)
	if err != nil {
		return DNull, err
	}
	return DFloat(v), nil
}

// Eval implements the Expr interface.
func (t Row) Eval(ctx EvalContext) (Datum, error) {
	return Tuple(t).Eval(ctx)
}

// Eval implements the Expr interface.
func (t Tuple) Eval(ctx EvalContext) (Datum, error) {
	tuple := make(DTuple, 0, len(t))
	for _, v := range t {
		d, err := v.Eval(ctx)
		if err != nil {
			return DNull, err
		}
		tuple = append(tuple, d)
	}
	return tuple, nil
}

// Eval implements the Expr interface.
func (t ValArg) Eval(_ EvalContext) (Datum, error) {
	return nil, util.Errorf("unhandled type %T", t)
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

func evalComparison(ctx EvalContext, op ComparisonOp, left, right Datum) (Datum, error) {
	if left == DNull || right == DNull {
		return DNull, nil
	}

	if f, ok := cmpOps[cmpArgs{op, reflect.TypeOf(left), reflect.TypeOf(right)}]; ok {
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
	dateFormat                    = "2006-01-02"
	timestampFormat               = "2006-01-02 15:04:05.999999999"
	TimestampWithOffsetZoneFormat = "2006-01-02 15:04:05.999999999-07:00"
	timestampWithNamedZoneFormat  = "2006-01-02 15:04:05.999999999 MST"
)

// ParseDate parses a date.
func ParseDate(s DString) (DDate, error) {
	// No need to ParseInLocation here because we're only parsing dates.
	t, err := time.Parse(dateFormat, string(s))
	if err != nil {
		return DDate(0), err
	}
	return defaultContext.makeDDate(t)
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
	var err error

	for _, format := range []string{
		dateFormat,
		TimestampWithOffsetZoneFormat,
		timestampFormat,
		timestampWithNamedZoneFormat,
	} {
		var t time.Time
		if t, err = time.ParseInLocation(format, str, loc); err == nil {
			return DTimestamp{Time: t}, nil
		}
	}

	return DTimestamp{}, err
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
