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

package parser2

import (
	"fmt"
	"math"
	"strconv"

	"github.com/cockroachdb/cockroach/sql/sqlwire"
)

type opType int

const (
	intOp opType = iota
	uintOp
	floatOp
	stringOp
)

var null = sqlwire.Datum{}

// Env defines the interface for retrieving column values.
type Env interface {
	Get(name string) (sqlwire.Datum, bool)
}

// mapEnv is an Env implementation using a map.
type mapEnv map[string]sqlwire.Datum

func (e mapEnv) Get(name string) (sqlwire.Datum, bool) {
	d, ok := e[name]
	return d, ok
}

// EvalExpr evaluates an SQL expression in the context of an
// environment. Expression evaluation is a mostly straightforward walk over the
// parse tree. The only significant complexity is the handling of types and
// implicit conversions. See prepareComparisonArgs and prepareBinaryArgs for
// details.
func EvalExpr(expr Expr, env Env) (sqlwire.Datum, error) {
	switch t := expr.(type) {
	case *AndExpr:
		return evalAndExpr(t, env)

	case *OrExpr:
		return evalOrExpr(t, env)

	case *NotExpr:
		return evalNotExpr(t, env)

	case *ParenExpr:
		return EvalExpr(t.Expr, env)

	case *ComparisonExpr:
		return evalComparisonExpr(t, env)

	case *RangeCond:
		return evalRangeCond(t, env)

	case *NullCheck:
		return evalNullCheck(t, env)

	case *ExistsExpr:
		// The subquery within the exists should have been executed before
		// expression evaluation and the exists nodes replaced with the result.

	case BytesVal:
		v := string(t)
		return sqlwire.Datum{StringVal: &v}, nil

	case StrVal:
		v := string(t)
		return sqlwire.Datum{StringVal: &v}, nil

	case IntVal:
		v := uint64(t)
		return sqlwire.Datum{UintVal: &v}, nil

	case NumVal:
		v, err := strconv.ParseFloat(string(t), 64)
		if err != nil {
			return null, err
		}
		return sqlwire.Datum{FloatVal: &v}, nil

	case BoolVal:
		return boolToDatum(bool(t)), nil

	case ValArg:
		// Placeholders should have been replaced before expression evaluation.

	case NullVal:
		return null, nil

	case QualifiedName:
		if d, ok := env.Get(t.String()); ok {
			return d, nil
		}
		return null, fmt.Errorf("column \"%s\" not found", t)

	case Tuple:
		if len(t) != 1 {
			return null, fmt.Errorf("unsupported expression type: %T: %s", expr, expr)
		}
		return EvalExpr(t[0], env)

	case *Subquery:
		// The subquery should have been executed before expression evaluation and
		// the result placed into the expression tree.

	case *BinaryExpr:
		return evalBinaryExpr(t, env)

	case *UnaryExpr:
		return evalUnaryExpr(t, env)

	case *FuncExpr:
		return evalFuncExpr(t, env)

	case *CaseExpr:
		return evalCaseExpr(t, env)
	}

	return null, fmt.Errorf("unsupported expression type: %T", expr)
}

func evalAndExpr(expr *AndExpr, env Env) (sqlwire.Datum, error) {
	left, err := EvalExpr(expr.Left, env)
	if err != nil {
		return null, err
	}
	if v, err := left.Bool(); err != nil {
		return null, err
	} else if !v {
		return boolToDatum(false), nil
	}
	right, err := EvalExpr(expr.Right, env)
	if err != nil {
		return null, err
	}
	if v, err := right.Bool(); err != nil {
		return null, err
	} else if !v {
		return boolToDatum(false), nil
	}
	return boolToDatum(true), nil
}

func evalOrExpr(expr *OrExpr, env Env) (sqlwire.Datum, error) {
	left, err := EvalExpr(expr.Left, env)
	if err != nil {
		return null, err
	}
	if v, err := left.Bool(); err != nil {
		return null, err
	} else if v {
		return boolToDatum(true), nil
	}
	right, err := EvalExpr(expr.Right, env)
	if err != nil {
		return null, err
	}
	if v, err := right.Bool(); err != nil {
		return null, err
	} else if v {
		return boolToDatum(true), nil
	}
	return boolToDatum(false), nil
}

func evalNotExpr(expr *NotExpr, env Env) (sqlwire.Datum, error) {
	d, err := EvalExpr(expr.Expr, env)
	if err != nil {
		return null, err
	}
	v, err := d.Bool()
	if err != nil {
		return null, err
	}
	return boolToDatum(!v), nil
}

func evalRangeCond(expr *RangeCond, env Env) (sqlwire.Datum, error) {
	// TODO(pmattis): This could be more efficient or done ahead of time.
	d, err := EvalExpr(&AndExpr{
		Left: &ComparisonExpr{
			Operator: GE,
			Left:     expr.Left,
			Right:    expr.From,
		},
		Right: &ComparisonExpr{
			Operator: LE,
			Left:     expr.Left,
			Right:    expr.To,
		},
	}, nil)
	if err != nil {
		return null, err
	}
	if expr.Not {
		*d.BoolVal = !*d.BoolVal
	}
	return d, nil
}

func evalNullCheck(expr *NullCheck, env Env) (sqlwire.Datum, error) {
	d, err := EvalExpr(expr.Expr, env)
	if err != nil {
		return null, err
	}
	v := d.IsNull()
	if expr.Not {
		v = !v
	}
	return boolToDatum(v), nil
}

// Prepare the arguments for a comparison operation. The returned arguments
// will have the same type.
func prepareComparisonArgs(left, right sqlwire.Datum) (opType, sqlwire.Datum, sqlwire.Datum, error) {
	// If both arguments are strings (or string-like), compare as strings.
	if (left.BytesVal != nil || left.StringVal != nil) &&
		(right.BytesVal != nil || right.StringVal != nil) {
		return stringOp, left.ToString(), right.ToString(), nil
	}

	// If both arguments are uints, compare as unsigned.
	if left.UintVal != nil && right.UintVal != nil {
		return uintOp, left, right, nil
	}

	var err error

	// If both arguments are integers (signed or unsigned), compare as integers.
	if (left.BoolVal != nil || left.IntVal != nil || left.UintVal != nil) &&
		(right.BoolVal != nil || right.IntVal != nil || right.UintVal != nil) {
		left, err = left.ToInt()
		if err != nil {
			return intOp, null, null, err
		}
		right, err = right.ToInt()
		if err != nil {
			return intOp, null, null, err
		}
		return intOp, left, right, nil
	}

	// In all other cases, compare as floats.
	left, err = left.ToFloat()
	if err != nil {
		return intOp, null, null, err
	}
	right, err = right.ToFloat()
	if err != nil {
		return intOp, null, null, err
	}
	return floatOp, left, right, nil
}

func evalComparisonExpr(expr *ComparisonExpr, env Env) (sqlwire.Datum, error) {
	left, err := EvalExpr(expr.Left, env)
	if err != nil {
		return null, err
	}
	right, err := EvalExpr(expr.Right, env)
	if err != nil {
		return null, err
	}

	op := expr.Operator

	if left.IsNull() || right.IsNull() {
		return null, nil
	}

	var typ opType
	var v bool

	switch op {
	case EQ:
		typ, left, right, err = prepareComparisonArgs(left, right)
		switch typ {
		case intOp:
			v = *left.IntVal == *right.IntVal
		case uintOp:
			v = *left.UintVal == *right.UintVal
		case floatOp:
			v = *left.FloatVal == *right.FloatVal
		case stringOp:
			v = *left.StringVal == *right.StringVal
		default:
			panic(fmt.Sprintf("unsupported op type: %d", typ))
		}

	case LT:
		typ, left, right, err = prepareComparisonArgs(left, right)
		switch typ {
		case intOp:
			v = *left.IntVal < *right.IntVal
		case uintOp:
			v = *left.UintVal < *right.UintVal
		case floatOp:
			v = *left.FloatVal < *right.FloatVal
		case stringOp:
			v = *left.StringVal < *right.StringVal
		default:
			panic(fmt.Sprintf("unsupported op type: %d", typ))
		}

	case LE:
		typ, left, right, err = prepareComparisonArgs(left, right)
		switch typ {
		case intOp:
			v = *left.IntVal <= *right.IntVal
		case uintOp:
			v = *left.UintVal <= *right.UintVal
		case floatOp:
			v = *left.FloatVal <= *right.FloatVal
		case stringOp:
			v = *left.StringVal <= *right.StringVal
		default:
			panic(fmt.Sprintf("unsupported op type: %d", typ))
		}

	case GT:
		typ, left, right, err = prepareComparisonArgs(left, right)
		switch typ {
		case intOp:
			v = *left.IntVal > *right.IntVal
		case uintOp:
			v = *left.UintVal > *right.UintVal
		case floatOp:
			v = *left.FloatVal > *right.FloatVal
		case stringOp:
			v = *left.StringVal > *right.StringVal
		default:
			panic(fmt.Sprintf("unsupported op type: %d", typ))
		}

	case GE:
		typ, left, right, err = prepareComparisonArgs(left, right)
		switch typ {
		case intOp:
			v = *left.IntVal >= *right.IntVal
		case uintOp:
			v = *left.UintVal >= *right.UintVal
		case floatOp:
			v = *left.FloatVal >= *right.FloatVal
		case stringOp:
			v = *left.StringVal >= *right.StringVal
		default:
			panic(fmt.Sprintf("unsupported op type: %d", typ))
		}

	case NE:
		typ, left, right, err = prepareComparisonArgs(left, right)
		switch typ {
		case intOp:
			v = *left.IntVal != *right.IntVal
		case uintOp:
			v = *left.UintVal != *right.UintVal
		case floatOp:
			v = *left.FloatVal != *right.FloatVal
		case stringOp:
			v = *left.StringVal != *right.StringVal
		default:
			panic(fmt.Sprintf("unsupported op type: %d", typ))
		}

	case In, NotIn, Like, NotLike:
		return null, fmt.Errorf("TODO(pmattis): unsupported comparison operator: %s", op)
	}

	return boolToDatum(v), nil
}

// Prepare the arguments for a binary operation. The returned arguments will
// have the same type. The typ parameter specifies the allowed types for the
// operation. For example, bit-operations should specify intOp or uintOp to
// indicate that they do not operate on floating point arguments. Float
// operations may still reduce to intOp or uintOp if the operands support it.
func prepareBinaryArgs(typ opType, left, right sqlwire.Datum) (opType, sqlwire.Datum, sqlwire.Datum, error) {
	var err error

	switch typ {
	case intOp, uintOp:
		if left.UintVal != nil || right.UintVal != nil {
			left, err = left.ToUint()
			if err != nil {
				return uintOp, null, null, err
			}
			right, err = right.ToUint()
			if err != nil {
				return uintOp, null, null, err
			}
			return uintOp, left, right, nil
		}
		left, err = left.ToInt()
		if err != nil {
			return intOp, null, null, err
		}
		right, err = right.ToInt()
		if err != nil {
			return intOp, null, null, err
		}
		return intOp, left, right, nil

	case floatOp:
		if (left.UintVal != nil && (right.IntVal != nil || right.UintVal != nil)) ||
			(right.UintVal != nil && (left.IntVal != nil || left.UintVal != nil)) {
			left, err = left.ToUint()
			if err != nil {
				return uintOp, null, null, err
			}
			right, err = right.ToUint()
			if err != nil {
				return uintOp, null, null, err
			}
			return uintOp, left, right, nil
		}
		if left.IntVal != nil && right.IntVal != nil {
			return intOp, left, right, nil
		}
	}

	left, err = left.ToFloat()
	if err != nil {
		return floatOp, null, null, err
	}
	right, err = right.ToFloat()
	if err != nil {
		return floatOp, null, null, err
	}
	return floatOp, left, right, nil
}

func evalBinaryExpr(expr *BinaryExpr, env Env) (sqlwire.Datum, error) {
	left, err := EvalExpr(expr.Left, env)
	if err != nil {
		return null, err
	}
	right, err := EvalExpr(expr.Right, env)
	if err != nil {
		return null, err
	}

	// TODO(pmattis): Overflow/underflow checks?

	var typ opType

	switch expr.Operator {
	case Bitand:
		typ, left, right, err = prepareBinaryArgs(intOp, left, right)
		if err != nil {
			return null, err
		}
		switch typ {
		case uintOp:
			*left.UintVal &= *right.UintVal
		case intOp:
			*left.IntVal &= *right.IntVal
		default:
			panic(fmt.Sprintf("unsupported op type: %d", typ))
		}
		return left, nil

	case Bitor:
		typ, left, right, err = prepareBinaryArgs(intOp, left, right)
		if err != nil {
			return null, err
		}
		switch typ {
		case uintOp:
			*left.UintVal |= *right.UintVal
		case intOp:
			*left.IntVal |= *right.IntVal
		default:
			panic(fmt.Sprintf("unsupported op type: %d", typ))
		}
		return left, nil

	case Bitxor:
		typ, left, right, err = prepareBinaryArgs(intOp, left, right)
		if err != nil {
			return null, err
		}
		switch typ {
		case uintOp:
			*left.UintVal ^= *right.UintVal
		case intOp:
			*left.IntVal ^= *right.IntVal
		default:
			panic(fmt.Sprintf("unsupported op type: %d", typ))
		}
		return left, nil

	case Plus:
		typ, left, right, err = prepareBinaryArgs(floatOp, left, right)
		if err != nil {
			return null, err
		}
		switch typ {
		case uintOp:
			*left.UintVal += *right.UintVal
		case intOp:
			*left.IntVal += *right.IntVal
		case floatOp:
			*left.FloatVal += *right.FloatVal
		default:
			panic(fmt.Sprintf("unsupported op type: %d", typ))
		}
		return left, nil

	case Minus:
		typ, left, right, err = prepareBinaryArgs(floatOp, left, right)
		if err != nil {
			return null, err
		}
		switch typ {
		case uintOp:
			// If the unsigned subtraction would result in a negative number, convert
			// to signed values.
			if *right.UintVal > *left.UintVal {
				v := -int64(*right.UintVal - *left.UintVal)
				left = sqlwire.Datum{IntVal: &v}
			} else {
				*left.UintVal -= *right.UintVal
			}
		case intOp:
			*left.IntVal -= *right.IntVal
		case floatOp:
			*left.FloatVal -= *right.FloatVal
		default:
			panic(fmt.Sprintf("unsupported op type: %d", typ))
		}
		return left, nil

	case Mult:
		typ, left, right, err = prepareBinaryArgs(floatOp, left, right)
		if err != nil {
			return null, err
		}
		switch typ {
		case uintOp:
			*left.UintVal *= *right.UintVal
		case intOp:
			*left.IntVal *= *right.IntVal
		case floatOp:
			*left.FloatVal *= *right.FloatVal
		default:
			panic(fmt.Sprintf("unsupported op type: %d", typ))
		}
		return left, nil

	case Div:
		// Division always operates on floats. TODO(pmattis): Is this correct?
		left, err = left.ToFloat()
		if err != nil {
			return null, err
		}
		right, err = right.ToFloat()
		if err != nil {
			return null, err
		}
		*left.FloatVal /= *right.FloatVal
		return left, nil

	case Mod:
		typ, left, right, err = prepareBinaryArgs(floatOp, left, right)
		if err != nil {
			return null, err
		}
		switch typ {
		case uintOp:
			*left.UintVal %= *right.UintVal
		case intOp:
			*left.IntVal %= *right.IntVal
		case floatOp:
			*left.FloatVal = math.Mod(*left.FloatVal, *right.FloatVal)
		default:
			panic(fmt.Sprintf("unsupported op type: %d", typ))
		}
		return left, nil

	case Concat:
		s := left.String() + right.String()
		return sqlwire.Datum{StringVal: &s}, nil
	}

	return null, fmt.Errorf("unsupported binary operator: %c", expr.Operator)
}

func evalUnaryExpr(expr *UnaryExpr, env Env) (sqlwire.Datum, error) {
	d, err := EvalExpr(expr.Expr, env)
	if err != nil {
		return null, err
	}
	switch expr.Operator {
	case UnaryPlus:
		return d, nil

	case UnaryMinus:
		var err error
		if d.IntVal != nil {
			*d.IntVal = -*d.IntVal
		} else if d.UintVal != nil {
			d, err = d.ToInt()
			if err != nil {
				return null, err
			}
			*d.IntVal = -*d.IntVal
		} else if d.FloatVal != nil {
			*d.FloatVal = -*d.FloatVal
		} else {
			d, err = d.ToFloat()
			if err != nil {
				return null, err
			}
			*d.FloatVal = -*d.FloatVal
		}
		return d, nil

	case UnaryComplement:
		d, err = d.ToUint()
		if err != nil {
			return null, err
		}
		*d.UintVal = ^*d.UintVal
		return d, nil
	}
	return null, fmt.Errorf("unsupported unary operator: %c", expr.Operator)
}

func evalFuncExpr(expr *FuncExpr, env Env) (sqlwire.Datum, error) {
	return null, fmt.Errorf("TODO(pmattis): unsupported expression type: %T", expr)
}

func evalCaseExpr(expr *CaseExpr, env Env) (sqlwire.Datum, error) {
	if expr.Expr != nil {
		// These are expressions of the form `CASE <val> WHEN <val> THEN ...`. The
		// parser doesn't properly support them yet.
		return null, fmt.Errorf("TODO(pmattis): unsupported simple case expression: %T", expr)
	}

	for _, when := range expr.Whens {
		d, err := EvalExpr(when.Cond, env)
		if err != nil {
			return null, err
		}
		if v, err := d.Bool(); err != nil {
			return null, err
		} else if v {
			return EvalExpr(when.Val, env)
		}
	}

	if expr.Else != nil {
		return EvalExpr(expr.Else, env)
	}
	return null, nil
}

func boolToDatum(v bool) sqlwire.Datum {
	return sqlwire.Datum{BoolVal: &v}
}
