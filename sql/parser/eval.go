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
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/util"
)

var errZeroModulus = errors.New("zero modulus")
var errDivByZero = errors.New("division by zero")

type unaryOp struct {
	returnType Datum
	fn         func(Datum) (Datum, error)
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
		fn: func(d Datum) (Datum, error) {
			return d, nil
		},
	},
	unaryArgs{UnaryPlus, floatType}: {
		returnType: DummyFloat,
		fn: func(d Datum) (Datum, error) {
			return d, nil
		},
	},

	unaryArgs{UnaryMinus, intType}: {
		returnType: DummyInt,
		fn: func(d Datum) (Datum, error) {
			return -d.(DInt), nil
		},
	},
	unaryArgs{UnaryMinus, floatType}: {
		returnType: DummyFloat,
		fn: func(d Datum) (Datum, error) {
			return -d.(DFloat), nil
		},
	},

	unaryArgs{UnaryComplement, intType}: {
		returnType: DummyInt,
		fn: func(d Datum) (Datum, error) {
			return ^d.(DInt), nil
		},
	},
}

type binOp struct {
	returnType Datum
	fn         func(Datum, Datum) (Datum, error)
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
		fn: func(left Datum, right Datum) (Datum, error) {
			return left.(DInt) & right.(DInt), nil
		},
	},

	binArgs{Bitor, intType, intType}: {
		returnType: DummyInt,
		fn: func(left Datum, right Datum) (Datum, error) {
			return left.(DInt) | right.(DInt), nil
		},
	},

	binArgs{Bitxor, intType, intType}: {
		returnType: DummyInt,
		fn: func(left Datum, right Datum) (Datum, error) {
			return left.(DInt) ^ right.(DInt), nil
		},
	},

	// TODO(pmattis): Overflow/underflow checks?

	binArgs{Plus, intType, intType}: {
		returnType: DummyInt,
		fn: func(left Datum, right Datum) (Datum, error) {
			return left.(DInt) + right.(DInt), nil
		},
	},
	binArgs{Plus, floatType, floatType}: {
		returnType: DummyFloat,
		fn: func(left Datum, right Datum) (Datum, error) {
			return left.(DFloat) + right.(DFloat), nil
		},
	},
	binArgs{Plus, dateType, intervalType}: {
		returnType: DummyTimestamp,
		fn: func(left Datum, right Datum) (Datum, error) {
			return DTimestamp{Time: left.(DDate).Add(right.(DInterval).Duration)}, nil
		},
	},
	binArgs{Plus, intervalType, dateType}: {
		returnType: DummyTimestamp,
		fn: func(left Datum, right Datum) (Datum, error) {
			return DTimestamp{Time: right.(DDate).Add(left.(DInterval).Duration)}, nil
		},
	},
	binArgs{Plus, timestampType, intervalType}: {
		returnType: DummyTimestamp,
		fn: func(left Datum, right Datum) (Datum, error) {
			return DTimestamp{Time: left.(DTimestamp).Add(right.(DInterval).Duration)}, nil
		},
	},
	binArgs{Plus, intervalType, timestampType}: {
		returnType: DummyTimestamp,
		fn: func(left Datum, right Datum) (Datum, error) {
			return DTimestamp{Time: right.(DTimestamp).Add(left.(DInterval).Duration)}, nil
		},
	},
	binArgs{Plus, intervalType, intervalType}: {
		returnType: DummyInterval,
		fn: func(left Datum, right Datum) (Datum, error) {
			return DInterval{Duration: left.(DInterval).Duration + right.(DInterval).Duration}, nil
		},
	},

	binArgs{Minus, intType, intType}: {
		returnType: DummyInt,
		fn: func(left Datum, right Datum) (Datum, error) {
			return left.(DInt) - right.(DInt), nil
		},
	},
	binArgs{Minus, floatType, floatType}: {
		returnType: DummyFloat,
		fn: func(left Datum, right Datum) (Datum, error) {
			return left.(DFloat) - right.(DFloat), nil
		},
	},
	binArgs{Minus, dateType, intervalType}: {
		returnType: DummyTimestamp,
		fn: func(left Datum, right Datum) (Datum, error) {
			return DTimestamp{Time: left.(DDate).Add(-right.(DInterval).Duration)}, nil
		},
	},
	binArgs{Minus, dateType, dateType}: {
		returnType: DummyInterval,
		fn: func(left Datum, right Datum) (Datum, error) {
			return DInterval{Duration: left.(DDate).Sub(right.(DDate).Time)}, nil
		},
	},
	binArgs{Minus, timestampType, timestampType}: {
		returnType: DummyInterval,
		fn: func(left Datum, right Datum) (Datum, error) {
			return DInterval{Duration: left.(DTimestamp).Sub(right.(DTimestamp).Time)}, nil
		},
	},
	binArgs{Minus, timestampType, dateType}: {
		returnType: DummyInterval,
		fn: func(left Datum, right Datum) (Datum, error) {
			return DInterval{Duration: left.(DTimestamp).Sub(right.(DDate).Time)}, nil
		},
	},
	binArgs{Minus, dateType, timestampType}: {
		returnType: DummyInterval,
		fn: func(left Datum, right Datum) (Datum, error) {
			return DInterval{Duration: left.(DDate).Sub(right.(DTimestamp).Time)}, nil
		},
	},
	binArgs{Minus, timestampType, intervalType}: {
		returnType: DummyTimestamp,
		fn: func(left Datum, right Datum) (Datum, error) {
			return DTimestamp{Time: left.(DTimestamp).Add(-right.(DInterval).Duration)}, nil
		},
	},
	binArgs{Minus, intervalType, intervalType}: {
		returnType: DummyInterval,
		fn: func(left Datum, right Datum) (Datum, error) {
			return DInterval{Duration: left.(DInterval).Duration - right.(DInterval).Duration}, nil
		},
	},

	binArgs{Mult, intType, intType}: {
		returnType: DummyInt,
		fn: func(left Datum, right Datum) (Datum, error) {
			return left.(DInt) * right.(DInt), nil
		},
	},
	binArgs{Mult, floatType, floatType}: {
		returnType: DummyFloat,
		fn: func(left Datum, right Datum) (Datum, error) {
			return left.(DFloat) * right.(DFloat), nil
		},
	},
	binArgs{Mult, intType, intervalType}: {
		returnType: DummyInterval,
		fn: func(left Datum, right Datum) (Datum, error) {
			return DInterval{Duration: time.Duration(left.(DInt)) * right.(DInterval).Duration}, nil
		},
	},
	binArgs{Mult, intervalType, intType}: {
		returnType: DummyInterval,
		fn: func(left Datum, right Datum) (Datum, error) {
			return DInterval{Duration: left.(DInterval).Duration * time.Duration(right.(DInt))}, nil
		},
	},

	binArgs{Div, intType, intType}: {
		returnType: DummyFloat,
		fn: func(left Datum, right Datum) (Datum, error) {
			rInt := right.(DInt)
			if rInt == 0 {
				return nil, errDivByZero
			}
			return DFloat(left.(DInt)) / DFloat(rInt), nil
		},
	},
	binArgs{Div, floatType, floatType}: {
		returnType: DummyFloat,
		fn: func(left Datum, right Datum) (Datum, error) {
			return left.(DFloat) / right.(DFloat), nil
		},
	},
	binArgs{Div, intervalType, intType}: {
		returnType: DummyInterval,
		fn: func(left Datum, right Datum) (Datum, error) {
			rInt := right.(DInt)
			if rInt == 0 {
				return nil, errDivByZero
			}
			return DInterval{Duration: left.(DInterval).Duration / time.Duration(rInt)}, nil
		},
	},

	binArgs{Mod, intType, intType}: {
		returnType: DummyInt,
		fn: func(left Datum, right Datum) (Datum, error) {
			r := right.(DInt)
			if r == 0 {
				return nil, errZeroModulus
			}
			return left.(DInt) % r, nil
		},
	},
	binArgs{Mod, floatType, floatType}: {
		returnType: DummyFloat,
		fn: func(left Datum, right Datum) (Datum, error) {
			return DFloat(math.Mod(float64(left.(DFloat)), float64(right.(DFloat)))), nil
		},
	},

	binArgs{Concat, stringType, stringType}: {
		returnType: DummyString,
		fn: func(left Datum, right Datum) (Datum, error) {
			return left.(DString) + right.(DString), nil
		},
	},

	// TODO(pmattis): Check that the shift is valid.
	binArgs{LShift, intType, intType}: {
		returnType: DummyInt,
		fn: func(left Datum, right Datum) (Datum, error) {
			return left.(DInt) << uint(right.(DInt)), nil
		},
	},
	binArgs{RShift, intType, intType}: {
		returnType: DummyInt,
		fn: func(left Datum, right Datum) (Datum, error) {
			return left.(DInt) >> uint(right.(DInt)), nil
		},
	},
}

type cmpArgs struct {
	op        ComparisonOp
	leftType  reflect.Type
	rightType reflect.Type
}

var cmpOpResultType = reflect.New(reflect.TypeOf(cmpOps).Elem().Out(0)).Elem().Interface().(DBool)

// cmpOps contains the comparison operations indexed by operation type and
// argument types.
var cmpOps = map[cmpArgs]func(Datum, Datum) (DBool, error){
	cmpArgs{EQ, stringType, stringType}: func(left Datum, right Datum) (DBool, error) {
		return DBool(left.(DString) == right.(DString)), nil
	},
	cmpArgs{EQ, boolType, boolType}: func(left Datum, right Datum) (DBool, error) {
		return DBool(left.(DBool) == right.(DBool)), nil
	},
	cmpArgs{EQ, intType, intType}: func(left Datum, right Datum) (DBool, error) {
		return DBool(left.(DInt) == right.(DInt)), nil
	},
	cmpArgs{EQ, floatType, floatType}: func(left Datum, right Datum) (DBool, error) {
		return DBool(left.(DFloat) == right.(DFloat)), nil
	},
	cmpArgs{EQ, dateType, dateType}: func(left Datum, right Datum) (DBool, error) {
		return DBool(left.(DDate).Equal(right.(DDate).Time)), nil
	},
	cmpArgs{EQ, timestampType, timestampType}: func(left Datum, right Datum) (DBool, error) {
		return DBool(left.(DTimestamp).Equal(right.(DTimestamp).Time)), nil
	},
	cmpArgs{EQ, intervalType, intervalType}: func(left Datum, right Datum) (DBool, error) {
		return DBool(left.(DInterval) == right.(DInterval)), nil
	},

	cmpArgs{LT, stringType, stringType}: func(left Datum, right Datum) (DBool, error) {
		return DBool(left.(DString) < right.(DString)), nil
	},
	cmpArgs{LT, boolType, boolType}: func(left Datum, right Datum) (DBool, error) {
		return DBool(!left.(DBool) && right.(DBool)), nil
	},
	cmpArgs{LT, intType, intType}: func(left Datum, right Datum) (DBool, error) {
		return DBool(left.(DInt) < right.(DInt)), nil
	},
	cmpArgs{LT, floatType, floatType}: func(left Datum, right Datum) (DBool, error) {
		return DBool(left.(DFloat) < right.(DFloat)), nil
	},
	cmpArgs{LT, dateType, dateType}: func(left Datum, right Datum) (DBool, error) {
		return DBool(left.(DDate).Before(right.(DDate).Time)), nil
	},
	cmpArgs{LT, timestampType, timestampType}: func(left Datum, right Datum) (DBool, error) {
		return DBool(left.(DTimestamp).Before(right.(DTimestamp).Time)), nil
	},
	cmpArgs{LT, intervalType, intervalType}: func(left Datum, right Datum) (DBool, error) {
		return DBool(left.(DInterval).Duration < right.(DInterval).Duration), nil
	},

	cmpArgs{LE, stringType, stringType}: func(left Datum, right Datum) (DBool, error) {
		return DBool(left.(DString) <= right.(DString)), nil
	},
	cmpArgs{LE, boolType, boolType}: func(left Datum, right Datum) (DBool, error) {
		return DBool(!left.(DBool) || right.(DBool)), nil
	},
	cmpArgs{LE, intType, intType}: func(left Datum, right Datum) (DBool, error) {
		return DBool(left.(DInt) <= right.(DInt)), nil
	},
	cmpArgs{LE, floatType, floatType}: func(left Datum, right Datum) (DBool, error) {
		return DBool(left.(DFloat) <= right.(DFloat)), nil
	},
	cmpArgs{LE, dateType, dateType}: func(left Datum, right Datum) (DBool, error) {
		return DBool(right.(DDate).Before(left.(DDate).Time)), nil
	},
	cmpArgs{LE, timestampType, timestampType}: func(left Datum, right Datum) (DBool, error) {
		return DBool(right.(DTimestamp).Before(left.(DTimestamp).Time)), nil
	},
	cmpArgs{LE, intervalType, intervalType}: func(left Datum, right Datum) (DBool, error) {
		return DBool(left.(DInterval).Duration <= right.(DInterval).Duration), nil
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
	cmpOps[cmpArgs{In, dateType, tupleType}] = evalTupleIN
	cmpOps[cmpArgs{In, timestampType, tupleType}] = evalTupleIN
	cmpOps[cmpArgs{In, intervalType, tupleType}] = evalTupleIN
	cmpOps[cmpArgs{In, tupleType, tupleType}] = evalTupleIN
}

// EvalExpr evaluates an SQL expression. Expression evaluation is a mostly
// straightforward walk over the parse tree. The only significant complexity is
// the handling of types and implicit conversions. See binOps and cmpOps for
// more details. Note that expression evaluation returns an error if certain
// node types are encountered: ValArg, QualifiedName or Subquery. These nodes
// should be replaced prior to expression evaluation by an appropriate
// WalkExpr. For example, ValArg should be replace by the argument passed from
// the client.
func EvalExpr(expr Expr) (Datum, error) {
	switch t := expr.(type) {
	case *AndExpr:
		return evalAndExpr(t)

	case *OrExpr:
		return evalOrExpr(t)

	case *NotExpr:
		return evalNotExpr(t)

	case Row:
		// NormalizeExpr transforms this into Tuple.

	case *ParenExpr:
		// NormalizeExpr unwraps this.

	case *ComparisonExpr:
		return evalComparisonExpr(t)

	case *RangeCond:
		// NormalizeExpr transforms this into an AndExpr.

	case *IsExpr:
		return evalIsExpr(t)

	case *IsOfTypeExpr:
		return evalIsOfTypeExpr(t)

	case *ExistsExpr:
		// The subquery within the exists should have been executed before
		// expression evaluation and the exists nodes replaced with the result.

	case BytesVal:
		return DString(t), nil

	case StrVal:
		return DString(t), nil

	case IntVal:
		if t < 0 {
			return DNull, fmt.Errorf("integer value out of range: %s", t)
		}
		return DInt(t), nil

	case NumVal:
		v, err := strconv.ParseFloat(string(t), 64)
		if err != nil {
			return DNull, err
		}
		return DFloat(v), nil

	case BoolVal:
		return DBool(t), nil

	case ValArg:
		// Placeholders should have been replaced before expression evaluation.

	case *QualifiedName:
		return DNull, fmt.Errorf("qualified name \"%s\" not found", t)

	case Tuple:
		tuple := make(DTuple, 0, len(t))
		for _, v := range t {
			d, err := EvalExpr(v)
			if err != nil {
				return DNull, err
			}
			tuple = append(tuple, d)
		}
		return tuple, nil

	case DReference:
		return t.Datum(), nil

	case Datum:
		return t, nil

	case *Subquery:
		// The subquery should have been executed before expression evaluation and
		// the result placed into the expression tree.

	case *BinaryExpr:
		return evalBinaryExpr(t)

	case *UnaryExpr:
		return evalUnaryExpr(t)

	case *FuncExpr:
		return evalFuncExpr(t)

	case *CaseExpr:
		return evalCaseExpr(t)

	case *CastExpr:
		return evalCastExpr(t)

	default:
		return DNull, util.Errorf("eval: unsupported expression: %T", expr)
	}

	return DNull, util.Errorf("eval: unexpected expression: %T", expr)
}

func evalAndExpr(expr *AndExpr) (Datum, error) {
	left, err := EvalExpr(expr.Left)
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
	right, err := EvalExpr(expr.Right)
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

func evalOrExpr(expr *OrExpr) (Datum, error) {
	left, err := EvalExpr(expr.Left)
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
	right, err := EvalExpr(expr.Right)
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

func evalNotExpr(expr *NotExpr) (Datum, error) {
	d, err := EvalExpr(expr.Expr)
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

func evalIsExpr(expr *IsExpr) (Datum, error) {
	d, err := EvalExpr(expr.Expr)
	if err != nil {
		return DNull, err
	}
	switch expr.Operator {
	case IsNull, IsUnknown:
		return DBool(d == DNull), nil
	case IsNotNull, IsNotUnknown:
		return DBool(d != DNull), nil
	case IsTrue:
		return DBool(d == DBool(true)), nil
	case IsNotTrue:
		return DBool(d != DBool(true)), nil
	case IsFalse:
		return DBool(d == DBool(false)), nil
	case IsNotFalse:
		return DBool(d != DBool(false)), nil
	default:
		return DNull, util.Errorf("eval: unsupported IS operator: %d", expr.Operator)
	}
}

func evalIsOfTypeExpr(expr *IsOfTypeExpr) (Datum, error) {
	d, err := EvalExpr(expr.Expr)
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

func evalComparisonExpr(expr *ComparisonExpr) (Datum, error) {
	left, err := EvalExpr(expr.Left)
	if err != nil {
		return DNull, err
	}
	right, err := EvalExpr(expr.Right)
	if err != nil {
		return DNull, err
	}

	return evalComparisonOp(expr.Operator, left, right)
}

func evalComparisonOp(op ComparisonOp, left, right Datum) (Datum, error) {
	if left == DNull || right == DNull {
		switch op {
		case IsDistinctFrom:
			return !DBool(left == DNull && right == DNull), nil
		case IsNotDistinctFrom:
			return DBool(left == DNull && right == DNull), nil
		}
		return DNull, nil
	}

	not := false
	switch op {
	case NE:
		// NE(left, right) is implemented as !EQ(left, right).
		not = true
		op = EQ
	case GT:
		// GT(left, right) is implemented as LT(right, left)
		op = LT
		left, right = right, left
	case GE:
		// GE(left, right) is implemented as LE(right, left)
		op = LE
		left, right = right, left
	case NotIn:
		// NotIn(left, right) is implemented as !IN(left, right)
		not = true
		op = In
	case IsDistinctFrom:
		// IsDistinctFrom(left, right) is implemented as !EQ(left, right)
		//
		// Note the special handling of NULLs and IS DISTINCT FROM above.
		not = true
		op = EQ
	case IsNotDistinctFrom:
		// IsNotDistinctFrom(left, right) is implemented as EQ(left, right)
		//
		// Note the special handling of NULLs and IS NOT DISTINCT FROM above.
		op = EQ
	}

	// TODO(pmattis): Memoize the cmpOps lookup as we've done for unaryOps and
	// binOps.
	if f, ok := cmpOps[cmpArgs{op, reflect.TypeOf(left), reflect.TypeOf(right)}]; ok {
		d, err := f(left, right)
		if err == nil && not {
			return !d, nil
		}
		return d, err
	}

	switch op {
	case Like, NotLike, SimilarTo, NotSimilarTo:
		return DNull, util.Errorf("TODO(pmattis): unsupported comparison operator: %s", op)
	}

	return DNull, fmt.Errorf("unsupported comparison operator: <%s> %s <%s>",
		left.Type(), op, right.Type())
}

func evalBinaryExpr(expr *BinaryExpr) (Datum, error) {
	left, err := EvalExpr(expr.Left)
	if err != nil {
		return DNull, err
	}
	right, err := EvalExpr(expr.Right)
	if err != nil {
		return DNull, err
	}

	if expr.fn.fn == nil {
		if _, err := typeCheckBinaryExpr(expr); err != nil {
			return DNull, err
		}
	}

	if expr.ltype != reflect.TypeOf(left) || expr.rtype != reflect.TypeOf(right) {
		// The argument types no longer match the memoized function. This happens
		// when a non-NULL argument becomes NULL. For example, "SELECT col+1 FROM
		// table" where col is nullable. The SELECT does not error, but returns a
		// NULL value for that select expression.
		return DNull, nil
	}
	return expr.fn.fn(left, right)
}

func evalUnaryExpr(expr *UnaryExpr) (Datum, error) {
	d, err := EvalExpr(expr.Expr)
	if err != nil {
		return DNull, err
	}
	if expr.fn.fn == nil {
		if _, err := typeCheckUnaryExpr(expr); err != nil {
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
	return expr.fn.fn(d)
}

func evalFuncExpr(expr *FuncExpr) (Datum, error) {
	args := make(DTuple, 0, len(expr.Exprs))
	types := make(typeList, 0, len(expr.Exprs))
	for _, e := range expr.Exprs {
		arg, err := EvalExpr(e)
		if err != nil {
			return DNull, err
		}
		args = append(args, arg)
		types = append(types, reflect.TypeOf(arg))
	}

	if expr.fn.fn == nil {
		if _, err := typeCheckFuncExpr(expr); err != nil {
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

	res, err := expr.fn.fn(args)
	if err != nil {
		return DNull, fmt.Errorf("%s: %v", expr.Name, err)
	}
	return res, nil
}

func evalCaseExpr(expr *CaseExpr) (Datum, error) {
	if expr.Expr != nil {
		// CASE <val> WHEN <expr> THEN ...
		//
		// For each "when" expression we compare for equality to <val>.
		val, err := EvalExpr(expr.Expr)
		if err != nil {
			return DNull, err
		}

		for _, when := range expr.Whens {
			arg, err := EvalExpr(when.Cond)
			if err != nil {
				return DNull, err
			}
			d, err := evalComparisonOp(EQ, val, arg)
			if err != nil {
				return DNull, err
			}
			if v, err := getBool(d); err != nil {
				return DNull, err
			} else if v {
				return EvalExpr(when.Val)
			}
		}
	} else {
		// CASE WHEN <bool-expr> THEN ...
		for _, when := range expr.Whens {
			d, err := EvalExpr(when.Cond)
			if err != nil {
				return DNull, err
			}
			if v, err := getBool(d); err != nil {
				return DNull, err
			} else if v {
				return EvalExpr(when.Val)
			}
		}
	}

	if expr.Else != nil {
		return EvalExpr(expr.Else)
	}
	return DNull, nil
}

func evalTupleEQ(ldatum, rdatum Datum) (DBool, error) {
	left := ldatum.(DTuple)
	right := rdatum.(DTuple)
	if len(left) != len(right) {
		return DBool(false), nil
	}
	for i := range left {
		d, err := evalComparisonOp(EQ, left[i], right[i])
		if err != nil {
			return DummyBool, err
		}
		if v, err := getBool(d); err != nil {
			return DummyBool, err
		} else if !v {
			return v, nil
		}
	}
	return DBool(true), nil
}

func evalTupleIN(arg, values Datum) (DBool, error) {
	if arg == DNull {
		return DBool(false), nil
	}

	vtuple := values.(DTuple)

	// TODO(pmattis): If we're evaluating the expression multiple times we should
	// use a map when possible. This works as long as arg is not a tuple. Note
	// that the usage of the map is currently disabled via the "&& false" because
	// building the map is a pessimization if we're only evaluating the
	// expression once. We need to determine when the expression will be
	// evaluated multiple times before enabling. Also need to figure out a way to
	// use the map approach for tuples. One idea is to encode the tuples into
	// strings and then use a map of strings.
	if _, ok := arg.(DTuple); !ok && false {
		m := make(map[Datum]struct{}, len(vtuple))
		for _, val := range vtuple {
			if reflect.TypeOf(arg) != reflect.TypeOf(val) {
				return DummyBool, fmt.Errorf("unsupported comparison operator: <%s> %s <%s>",
					arg.Type(), EQ, val.Type())
			}
			m[val] = struct{}{}
		}
		if _, exists := m[arg]; exists {
			return DBool(true), nil
		}
	} else {
		for _, val := range vtuple {
			d, err := evalComparisonOp(EQ, arg, val)
			if err != nil {
				return DummyBool, err
			}
			if v, err := getBool(d); err != nil {
				return DummyBool, err
			} else if v {
				return v, nil
			}
		}
	}

	return DBool(false), nil
}

func evalCastExpr(expr *CastExpr) (Datum, error) {
	d, err := EvalExpr(expr.Expr)
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

	case *StringType, *BytesType:
		var s DString
		switch d.(type) {
		case DBool, DInt, DFloat, dNull:
			s = DString(d.String())
		case DString:
			s = d.(DString)
		}
		if c, ok := expr.Type.(*StringType); ok {
			// If the CHAR type specifies a limit we truncate to that limit:
			//   'hello'::CHAR(2) -> 'he'
			if c.N > 0 && c.N < len(s) {
				s = s[:c.N]
			}
		}
		return s, nil

	case *DateType:
		switch d := d.(type) {
		case DString:
			return ParseDate(d)
		case DTimestamp:
			return DDate{Time: d.Truncate(24 * time.Hour)}, nil
		}

	case *TimestampType:
		switch d := d.(type) {
		case DString:
			return ParseTimestamp(d)
		case DDate:
			return DTimestamp{Time: d.Time}, nil
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

	return DNull, fmt.Errorf("invalid cast: %s -> %s", d.Type(), expr.Type)
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
	str := string(s)
	t, err := time.Parse(dateFormat, str)
	if err == nil {
		return DDate{Time: t}, nil
	}
	// Parse other formats in the future
	return DummyDate, err
}

// ParseTimestamp parses the timestamp.
func ParseTimestamp(s DString) (DTimestamp, error) {
	str := string(s)
	t, err := time.Parse(dateFormat, str)
	if err == nil {
		return DTimestamp{Time: t}, nil
	}
	t, err = time.Parse(timestampFormat, str)
	if err == nil {
		t = t.UTC()
		return DTimestamp{Time: t}, nil
	}
	t, err = time.Parse(TimestampWithOffsetZoneFormat, str)
	if err == nil {
		t = t.UTC()
		return DTimestamp{Time: t}, nil
	}
	t, err = time.Parse(timestampWithNamedZoneFormat, str)
	if err == nil {
		// Parsing using a named time zone is imperfect for two reasons:
		// 1. Some named time zones are ambiguous (PST can be US PST and
		// phillipines PST), and 2. The code needs to have access to the entire
		// database of named timed zones in order to get some time offset,
		// and it's not clear what are the memory requirements for that.
		// TODO(vivek): Implement SET TIME ZONE to set a time zone and use
		// time.ParseInLocation()
		return DummyTimestamp, util.Errorf("TODO(vivek): named time zone input not supported")
	}
	// Parse other formats in the future.
	return DummyTimestamp, err
}
