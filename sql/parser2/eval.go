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
)

// TODO(pmattis):
//
// - Support tuples (i.e. []Datum) and tuple operations (a IN (b, c, d)).
//
// - Support decimal arithmetic.

type opType int

const (
	intOp opType = iota
	floatOp
)

// A Datum holds either a bool, int64, float64, string or []Datum.
type Datum interface {
	IsNull() bool
	ToBool() (Datum, error)
	ToInt() (Datum, error)
	ToFloat() (Datum, error)
	String() string
}

var _ Datum = dbool(false)
var _ Datum = dint(0)
var _ Datum = dfloat(0)
var _ Datum = dstring("")
var _ Datum = dnull{}

type dbool bool

func (d dbool) IsNull() bool {
	return false
}

func (d dbool) ToBool() (Datum, error) {
	return d, nil
}

func (d dbool) ToInt() (Datum, error) {
	if d {
		return dint(1), nil
	}
	return dint(0), nil
}

func (d dbool) ToFloat() (Datum, error) {
	if d {
		return dfloat(1), nil
	}
	return dfloat(0), nil
}

func (d dbool) String() string {
	if d {
		return "true"
	}
	return "false"
}

type dint int64

func (d dint) IsNull() bool {
	return false
}

func (d dint) ToBool() (Datum, error) {
	return dbool(d != 0), nil
}

func (d dint) ToInt() (Datum, error) {
	return d, nil
}

func (d dint) ToFloat() (Datum, error) {
	return dfloat(d), nil
}

func (d dint) String() string {
	return strconv.FormatInt(int64(d), 10)
}

type dfloat float64

func (d dfloat) IsNull() bool {
	return false
}

func (d dfloat) ToBool() (Datum, error) {
	return dbool(d != 0), nil
}

func (d dfloat) ToInt() (Datum, error) {
	return dint(d), nil
}

func (d dfloat) ToFloat() (Datum, error) {
	return d, nil
}

func (d dfloat) String() string {
	return strconv.FormatFloat(float64(d), 'g', -1, 64)
}

type dstring string

func (d dstring) IsNull() bool {
	return false
}

func (d dstring) ToBool() (Datum, error) {
	// TODO(pmattis): ParseBool is more permissive than the SQL grammar accepting
	// "t" and "f". Is this conversion even necessary?
	v, err := strconv.ParseBool(string(d))
	if err != nil {
		return null, err
	}
	return dbool(v), nil
}

func (d dstring) ToInt() (Datum, error) {
	v, err := strconv.ParseInt(string(d), 0, 64)
	if err != nil {
		return null, err
	}
	return dint(v), nil
}

func (d dstring) ToFloat() (Datum, error) {
	v, err := strconv.ParseFloat(string(d), 64)
	if err != nil {
		return null, err
	}
	return dfloat(v), nil
}

func (d dstring) String() string {
	return string(d)
}

type dnull struct{}

func (d dnull) IsNull() bool {
	return true
}

func (d dnull) ToBool() (Datum, error) {
	return d, fmt.Errorf("cannot convert NULL to bool")
}

func (d dnull) ToInt() (Datum, error) {
	return d, fmt.Errorf("cannot convert NULL to int")
}

func (d dnull) ToFloat() (Datum, error) {
	return d, fmt.Errorf("cannot convert NULL to float")
}

func (d dnull) String() string {
	return "NULL"
}

var null = dnull{}

// Env defines the interface for retrieving column values.
type Env interface {
	Get(name string) (Datum, bool)
}

// mapEnv is an Env implementation using a map.
type mapEnv map[string]Datum

func (e mapEnv) Get(name string) (Datum, bool) {
	d, ok := e[name]
	return d, ok
}

// EvalExpr evaluates an SQL expression in the context of an
// environment. Expression evaluation is a mostly straightforward walk over the
// parse tree. The only significant complexity is the handling of types and
// implicit conversions. See prepareComparisonArgs and prepareBinaryArgs for
// details.
func EvalExpr(expr Expr, env Env) (Datum, error) {
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
		return dstring(t), nil

	case StrVal:
		return dstring(t), nil

	case IntVal:
		return dint(t), nil

	case NumVal:
		v, err := strconv.ParseFloat(string(t), 64)
		if err != nil {
			return null, err
		}
		return dfloat(v), nil

	case BoolVal:
		return dbool(t), nil

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

func evalAndExpr(expr *AndExpr, env Env) (Datum, error) {
	left, err := EvalExpr(expr.Left, env)
	if err != nil {
		return null, err
	}
	if v, err := left.ToBool(); err != nil {
		return null, err
	} else if !v.(dbool) {
		return dbool(false), nil
	}
	right, err := EvalExpr(expr.Right, env)
	if err != nil {
		return null, err
	}
	if v, err := right.ToBool(); err != nil {
		return null, err
	} else if !v.(dbool) {
		return dbool(false), nil
	}
	return dbool(true), nil
}

func evalOrExpr(expr *OrExpr, env Env) (Datum, error) {
	left, err := EvalExpr(expr.Left, env)
	if err != nil {
		return null, err
	}
	if v, err := left.ToBool(); err != nil {
		return null, err
	} else if v.(dbool) {
		return dbool(true), nil
	}
	right, err := EvalExpr(expr.Right, env)
	if err != nil {
		return null, err
	}
	if v, err := right.ToBool(); err != nil {
		return null, err
	} else if v.(dbool) {
		return dbool(true), nil
	}
	return dbool(false), nil
}

func evalNotExpr(expr *NotExpr, env Env) (Datum, error) {
	d, err := EvalExpr(expr.Expr, env)
	if err != nil {
		return null, err
	}
	v, err := d.ToBool()
	if err != nil {
		return null, err
	}
	return !v.(dbool), nil
}

func evalRangeCond(expr *RangeCond, env Env) (Datum, error) {
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
		v, err := d.ToBool()
		if err != nil {
			return null, err
		}
		return !v.(dbool), nil
	}
	return d, nil
}

func evalNullCheck(expr *NullCheck, env Env) (Datum, error) {
	d, err := EvalExpr(expr.Expr, env)
	if err != nil {
		return null, err
	}
	v := d.IsNull()
	if expr.Not {
		v = !v
	}
	return dbool(v), nil
}

// Prepare the arguments for a comparison operation. The returned arguments
// will have the same type.
func prepareComparisonArgs(left, right Datum) (Datum, Datum, error) {
	var err error

	switch left.(type) {
	case dstring:
		switch right.(type) {
		case dstring:
			// If both arguments are strings (or string-like), compare as strings.
			return left, right, nil
		}

	case dbool, dint:
		switch right.(type) {
		case dbool, dint:
			// If both arguments are integers or bools, compare as integers.
			left, err = left.ToInt()
			if err != nil {
				return null, null, err
			}
			right, err = right.ToInt()
			if err != nil {
				return null, null, err
			}
			return left, right, nil
		}
	}

	// In all other cases, compare as floats.
	left, err = left.ToFloat()
	if err != nil {
		return null, null, err
	}
	right, err = right.ToFloat()
	if err != nil {
		return null, null, err
	}
	return left, right, nil
}

func evalComparisonExpr(expr *ComparisonExpr, env Env) (Datum, error) {
	left, err := EvalExpr(expr.Left, env)
	if err != nil {
		return null, err
	}
	right, err := EvalExpr(expr.Right, env)
	if err != nil {
		return null, err
	}

	if left.IsNull() || right.IsNull() {
		return null, nil
	}

	var v bool
	switch expr.Operator {
	case EQ:
		left, right, err = prepareComparisonArgs(left, right)
		switch left.(type) {
		case dint:
			v = left.(dint) == right.(dint)
		case dfloat:
			v = left.(dfloat) == right.(dfloat)
		case dstring:
			v = left.(dstring) == right.(dstring)
		default:
			panic(fmt.Sprintf("unsupported type: %T", left))
		}

	case LT:
		left, right, err = prepareComparisonArgs(left, right)
		switch left.(type) {
		case dint:
			v = left.(dint) < right.(dint)
		case dfloat:
			v = left.(dfloat) < right.(dfloat)
		case dstring:
			v = left.(dstring) < right.(dstring)
		default:
			panic(fmt.Sprintf("unsupported type: %T", left))
		}

	case LE:
		left, right, err = prepareComparisonArgs(left, right)
		switch left.(type) {
		case dint:
			v = left.(dint) <= right.(dint)
		case dfloat:
			v = left.(dfloat) <= right.(dfloat)
		case dstring:
			v = left.(dstring) <= right.(dstring)
		default:
			panic(fmt.Sprintf("unsupported type: %T", left))
		}

	case GT:
		left, right, err = prepareComparisonArgs(left, right)
		switch left.(type) {
		case dint:
			v = left.(dint) > right.(dint)
		case dfloat:
			v = left.(dfloat) > right.(dfloat)
		case dstring:
			v = left.(dstring) > right.(dstring)
		default:
			panic(fmt.Sprintf("unsupported type: %T", left))
		}

	case GE:
		left, right, err = prepareComparisonArgs(left, right)
		switch left.(type) {
		case dint:
			v = left.(dint) >= right.(dint)
		case dfloat:
			v = left.(dfloat) >= right.(dfloat)
		case dstring:
			v = left.(dstring) >= right.(dstring)
		default:
			panic(fmt.Sprintf("unsupported type: %T", left))
		}

	case NE:
		left, right, err = prepareComparisonArgs(left, right)
		switch left.(type) {
		case dint:
			v = left.(dint) != right.(dint)
		case dfloat:
			v = left.(dfloat) != right.(dfloat)
		case dstring:
			v = left.(dstring) != right.(dstring)
		default:
			panic(fmt.Sprintf("unsupported type: %T", left))
		}

	case In, NotIn, Like, NotLike:
		return null, fmt.Errorf("TODO(pmattis): unsupported comparison operator: %s", expr.Operator)
	}

	return dbool(v), nil
}

// Prepare the arguments for a binary operation. The returned arguments will
// have the same type. The typ parameter specifies the allowed types for the
// operation. For example, bit-operations should specify intOp to indicate that
// they do not operate on floating point arguments. Float operations may still
// reduce to intOp if the operands support it.
func prepareBinaryArgs(typ opType, left, right Datum) (Datum, Datum, error) {
	var err error

	switch typ {
	case intOp:
		left, err = left.ToInt()
		if err != nil {
			return null, null, err
		}
		right, err = right.ToInt()
		if err != nil {
			return null, null, err
		}
		return left, right, nil

	case floatOp:
		switch left.(type) {
		case dint:
			switch right.(type) {
			case dint:
				return left, right, nil
			}
		}
	}

	left, err = left.ToFloat()
	if err != nil {
		return null, null, err
	}
	right, err = right.ToFloat()
	if err != nil {
		return null, null, err
	}
	return left, right, nil
}

func evalBinaryExpr(expr *BinaryExpr, env Env) (Datum, error) {
	left, err := EvalExpr(expr.Left, env)
	if err != nil {
		return null, err
	}
	right, err := EvalExpr(expr.Right, env)
	if err != nil {
		return null, err
	}

	// TODO(pmattis): Overflow/underflow checks?

	switch expr.Operator {
	case Bitand:
		left, right, err = prepareBinaryArgs(intOp, left, right)
		if err != nil {
			return null, err
		}
		switch left.(type) {
		case dint:
			left = left.(dint) & right.(dint)
		default:
			panic(fmt.Sprintf("unsupported type: %T", left))
		}
		return left, nil

	case Bitor:
		left, right, err = prepareBinaryArgs(intOp, left, right)
		if err != nil {
			return null, err
		}
		switch left.(type) {
		case dint:
			left = left.(dint) | right.(dint)
		default:
			panic(fmt.Sprintf("unsupported type: %T", left))
		}
		return left, nil

	case Bitxor:
		left, right, err = prepareBinaryArgs(intOp, left, right)
		if err != nil {
			return null, err
		}
		switch left.(type) {
		case dint:
			left = left.(dint) ^ right.(dint)
		default:
			panic(fmt.Sprintf("unsupported type: %T", left))
		}
		return left, nil

	case Plus:
		left, right, err = prepareBinaryArgs(floatOp, left, right)
		if err != nil {
			return null, err
		}
		switch left.(type) {
		case dint:
			left = left.(dint) + right.(dint)
		case dfloat:
			left = left.(dfloat) + right.(dfloat)
		default:
			panic(fmt.Sprintf("unsupported type: %T", left))
		}
		return left, nil

	case Minus:
		left, right, err = prepareBinaryArgs(floatOp, left, right)
		if err != nil {
			return null, err
		}
		switch left.(type) {
		case dint:
			left = left.(dint) - right.(dint)
		case dfloat:
			left = left.(dfloat) - right.(dfloat)
		default:
			panic(fmt.Sprintf("unsupported type: %T", left))
		}
		return left, nil

	case Mult:
		left, right, err = prepareBinaryArgs(floatOp, left, right)
		if err != nil {
			return null, err
		}
		switch left.(type) {
		case dint:
			left = left.(dint) * right.(dint)
		case dfloat:
			left = left.(dfloat) * right.(dfloat)
		default:
			panic(fmt.Sprintf("unsupported type: %T", left))
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
		left = left.(dfloat) / right.(dfloat)
		return left, nil

	case Mod:
		left, right, err = prepareBinaryArgs(floatOp, left, right)
		if err != nil {
			return null, err
		}
		switch left.(type) {
		case dint:
			left = left.(dint) % right.(dint)
		case dfloat:
			left = dfloat(math.Mod(float64(left.(dfloat)), float64(right.(dfloat))))
		default:
			panic(fmt.Sprintf("unsupported type: %T", left))
		}
		return left, nil

	case Concat:
		s := left.String() + right.String()
		return dstring(s), nil
	}

	return null, fmt.Errorf("unsupported binary operator: %c", expr.Operator)
}

func evalUnaryExpr(expr *UnaryExpr, env Env) (Datum, error) {
	d, err := EvalExpr(expr.Expr, env)
	if err != nil {
		return null, err
	}
	switch expr.Operator {
	case UnaryPlus:
		return d, nil

	case UnaryMinus:
		var err error
		switch t := d.(type) {
		case dint:
			return -t, nil
		case dfloat:
			return -t, nil
		}
		d, err = d.ToFloat()
		if err != nil {
			return null, err
		}
		return -d.(dfloat), nil

	case UnaryComplement:
		switch d.(type) {
		case dint:
			return ^d.(dint), nil
		}
		return null, fmt.Errorf("unary complement not supported for: %T", d)
	}
	return null, fmt.Errorf("unsupported unary operator: %c", expr.Operator)
}

func evalFuncExpr(expr *FuncExpr, env Env) (Datum, error) {
	return null, fmt.Errorf("TODO(pmattis): unsupported expression type: %T", expr)
}

func evalCaseExpr(expr *CaseExpr, env Env) (Datum, error) {
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
		if v, err := d.ToBool(); err != nil {
			return null, err
		} else if v.(dbool) {
			return EvalExpr(when.Val, env)
		}
	}

	if expr.Else != nil {
		return EvalExpr(expr.Else, env)
	}
	return null, nil
}
