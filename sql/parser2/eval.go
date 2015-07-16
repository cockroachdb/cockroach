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
	"reflect"
	"strconv"
)

// TODO(pmattis):
//
// - Support tuples (i.e. []Datum) and tuple operations (a IN (b, c, d)).
//
// - Support decimal arithmetic.

// A Datum holds either a bool, int64, float64, string or []Datum.
type Datum interface {
	// Bool returns the Datum as a bool or an error if the Datum cannot be
	// converted to a bool.
	Bool() (dbool, error)
	Type() string
	String() string
}

var _ Datum = dbool(false)
var _ Datum = dint(0)
var _ Datum = dfloat(0)
var _ Datum = dstring("")
var _ Datum = dnull{}

type dbool bool

func (d dbool) Bool() (dbool, error) {
	return d, nil
}

func (d dbool) Type() string {
	return "bool"
}

func (d dbool) String() string {
	if d {
		return "true"
	}
	return "false"
}

type dint int64

// TODO(pmattis): Do we want to allow implicit conversions of int to bool?  See
// #1626.
func (d dint) Bool() (dbool, error) {
	return dbool(d != 0), nil
}

func (d dint) Type() string {
	return "int"
}

func (d dint) String() string {
	return strconv.FormatInt(int64(d), 10)
}

type dfloat float64

// TODO(pmattis): Do we want to allow implicit conversions of float to bool?
// See #1626.
func (d dfloat) Bool() (dbool, error) {
	return dbool(d != 0), nil
}

func (d dfloat) Type() string {
	return "float"
}

func (d dfloat) String() string {
	return strconv.FormatFloat(float64(d), 'g', -1, 64)
}

type dstring string

func (d dstring) Bool() (dbool, error) {
	return false, fmt.Errorf("cannot convert string to bool")
}

func (d dstring) Type() string {
	return "string"
}

func (d dstring) String() string {
	return string(d)
}

type dnull struct{}

func (d dnull) Bool() (dbool, error) {
	return false, fmt.Errorf("cannot convert NULL to bool")
}

func (d dnull) Type() string {
	return "NULL"
}

func (d dnull) String() string {
	return "NULL"
}

var null = dnull{}

var (
	boolType   = reflect.TypeOf(dbool(false))
	intType    = reflect.TypeOf(dint(0))
	floatType  = reflect.TypeOf(dfloat(0))
	stringType = reflect.TypeOf(dstring(""))
	nullType   = reflect.TypeOf(null)
)

type unaryArgs struct {
	op      UnaryOp
	argType reflect.Type
}

// unaryOps contains the unary operations indexed by operation type and
// argument type.
var unaryOps = map[unaryArgs]func(Datum) (Datum, error){
	unaryArgs{UnaryPlus, intType}: func(d Datum) (Datum, error) {
		return d, nil
	},
	unaryArgs{UnaryPlus, floatType}: func(d Datum) (Datum, error) {
		return d, nil
	},

	unaryArgs{UnaryMinus, intType}: func(d Datum) (Datum, error) {
		return -d.(dint), nil
	},
	unaryArgs{UnaryMinus, floatType}: func(d Datum) (Datum, error) {
		return -d.(dfloat), nil
	},

	unaryArgs{UnaryComplement, intType}: func(d Datum) (Datum, error) {
		return ^d.(dint), nil
	},
}

type binArgs struct {
	op        BinaryOp
	leftType  reflect.Type
	rightType reflect.Type
}

// binOps contains the binary operations indexed by operation type and argument
// types.
var binOps = map[binArgs]func(Datum, Datum) (Datum, error){
	binArgs{Bitand, intType, intType}: func(left Datum, right Datum) (Datum, error) {
		return left.(dint) & right.(dint), nil
	},

	binArgs{Bitor, intType, intType}: func(left Datum, right Datum) (Datum, error) {
		return left.(dint) | right.(dint), nil
	},

	binArgs{Bitxor, intType, intType}: func(left Datum, right Datum) (Datum, error) {
		return left.(dint) ^ right.(dint), nil
	},

	// TODO(pmattis): Overflow/underflow checks?

	binArgs{Plus, intType, intType}: func(left Datum, right Datum) (Datum, error) {
		return left.(dint) + right.(dint), nil
	},
	binArgs{Plus, intType, floatType}: func(left Datum, right Datum) (Datum, error) {
		return dfloat(left.(dint)) + right.(dfloat), nil
	},
	binArgs{Plus, floatType, intType}: func(left Datum, right Datum) (Datum, error) {
		return left.(dfloat) + dfloat(right.(dint)), nil
	},
	binArgs{Plus, floatType, floatType}: func(left Datum, right Datum) (Datum, error) {
		return left.(dfloat) + right.(dfloat), nil
	},

	binArgs{Minus, intType, intType}: func(left Datum, right Datum) (Datum, error) {
		return left.(dint) - right.(dint), nil
	},
	binArgs{Minus, intType, floatType}: func(left Datum, right Datum) (Datum, error) {
		return dfloat(left.(dint)) - right.(dfloat), nil
	},
	binArgs{Minus, floatType, intType}: func(left Datum, right Datum) (Datum, error) {
		return left.(dfloat) - dfloat(right.(dint)), nil
	},
	binArgs{Minus, floatType, floatType}: func(left Datum, right Datum) (Datum, error) {
		return left.(dfloat) - right.(dfloat), nil
	},

	binArgs{Mult, intType, intType}: func(left Datum, right Datum) (Datum, error) {
		return left.(dint) * right.(dint), nil
	},
	binArgs{Mult, intType, floatType}: func(left Datum, right Datum) (Datum, error) {
		return dfloat(left.(dint)) * right.(dfloat), nil
	},
	binArgs{Mult, floatType, intType}: func(left Datum, right Datum) (Datum, error) {
		return left.(dfloat) * dfloat(right.(dint)), nil
	},
	binArgs{Mult, floatType, floatType}: func(left Datum, right Datum) (Datum, error) {
		return left.(dfloat) * right.(dfloat), nil
	},

	binArgs{Div, intType, intType}: func(left Datum, right Datum) (Datum, error) {
		return dfloat(left.(dint)) / dfloat(right.(dint)), nil
	},
	binArgs{Div, intType, floatType}: func(left Datum, right Datum) (Datum, error) {
		return dfloat(left.(dint)) / right.(dfloat), nil
	},
	binArgs{Div, floatType, intType}: func(left Datum, right Datum) (Datum, error) {
		return left.(dfloat) / dfloat(right.(dint)), nil
	},
	binArgs{Div, floatType, floatType}: func(left Datum, right Datum) (Datum, error) {
		return left.(dfloat) / right.(dfloat), nil
	},

	binArgs{Mod, intType, intType}: func(left Datum, right Datum) (Datum, error) {
		return left.(dint) % right.(dint), nil
	},
	binArgs{Mod, intType, floatType}: func(left Datum, right Datum) (Datum, error) {
		return dfloat(math.Mod(float64(left.(dint)), float64(right.(dfloat)))), nil
	},
	binArgs{Mod, floatType, intType}: func(left Datum, right Datum) (Datum, error) {
		return dfloat(math.Mod(float64(left.(dfloat)), float64(right.(dint)))), nil
	},
	binArgs{Mod, floatType, floatType}: func(left Datum, right Datum) (Datum, error) {
		return dfloat(math.Mod(float64(left.(dfloat)), float64(right.(dfloat)))), nil
	},

	binArgs{Concat, stringType, stringType}: func(left Datum, right Datum) (Datum, error) {
		return left.(dstring) + right.(dstring), nil
	},
	binArgs{Concat, boolType, stringType}: func(left Datum, right Datum) (Datum, error) {
		return dstring(left.String()) + right.(dstring), nil
	},
	binArgs{Concat, stringType, boolType}: func(left Datum, right Datum) (Datum, error) {
		return left.(dstring) + dstring(right.String()), nil
	},
	binArgs{Concat, intType, stringType}: func(left Datum, right Datum) (Datum, error) {
		return dstring(left.String()) + right.(dstring), nil
	},
	binArgs{Concat, stringType, intType}: func(left Datum, right Datum) (Datum, error) {
		return left.(dstring) + dstring(right.String()), nil
	},
	binArgs{Concat, floatType, stringType}: func(left Datum, right Datum) (Datum, error) {
		return dstring(left.String()) + right.(dstring), nil
	},
	binArgs{Concat, stringType, floatType}: func(left Datum, right Datum) (Datum, error) {
		return left.(dstring) + dstring(right.String()), nil
	},
}

type cmpArgs struct {
	op        ComparisonOp
	leftType  reflect.Type
	rightType reflect.Type
}

// cmpOps contains the comparison operations indexed by operation type and
// argument types.
var cmpOps = map[cmpArgs]func(Datum, Datum) (Datum, error){
	cmpArgs{EQ, stringType, stringType}: func(left Datum, right Datum) (Datum, error) {
		return dbool(left.(dstring) == right.(dstring)), nil
	},
	cmpArgs{EQ, boolType, boolType}: func(left Datum, right Datum) (Datum, error) {
		return dbool(left.(dbool) == right.(dbool)), nil
	},
	cmpArgs{EQ, intType, intType}: func(left Datum, right Datum) (Datum, error) {
		return dbool(left.(dint) == right.(dint)), nil
	},
	cmpArgs{EQ, floatType, floatType}: func(left Datum, right Datum) (Datum, error) {
		return dbool(left.(dfloat) == right.(dfloat)), nil
	},

	cmpArgs{LT, stringType, stringType}: func(left Datum, right Datum) (Datum, error) {
		return dbool(left.(dstring) < right.(dstring)), nil
	},
	cmpArgs{LT, boolType, boolType}: func(left Datum, right Datum) (Datum, error) {
		return dbool(!left.(dbool) && right.(dbool)), nil
	},
	cmpArgs{LT, intType, intType}: func(left Datum, right Datum) (Datum, error) {
		return dbool(left.(dint) < right.(dint)), nil
	},
	cmpArgs{LT, floatType, floatType}: func(left Datum, right Datum) (Datum, error) {
		return dbool(left.(dfloat) < right.(dfloat)), nil
	},

	cmpArgs{LE, stringType, stringType}: func(left Datum, right Datum) (Datum, error) {
		return dbool(left.(dstring) <= right.(dstring)), nil
	},
	cmpArgs{LE, boolType, boolType}: func(left Datum, right Datum) (Datum, error) {
		return dbool(!left.(dbool) || right.(dbool)), nil
	},
	cmpArgs{LE, intType, intType}: func(left Datum, right Datum) (Datum, error) {
		return dbool(left.(dint) <= right.(dint)), nil
	},
	cmpArgs{LE, floatType, floatType}: func(left Datum, right Datum) (Datum, error) {
		return dbool(left.(dfloat) <= right.(dfloat)), nil
	},
}

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
// implicit conversions. See binOps and cmpOps for more details.
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
	if left == null {
		return null, nil
	}
	if v, err := left.Bool(); err != nil {
		return null, err
	} else if !v {
		return v, nil
	}
	right, err := EvalExpr(expr.Right, env)
	if err != nil {
		return null, err
	}
	if right == null {
		return null, nil
	}
	if v, err := right.Bool(); err != nil {
		return null, err
	} else if !v {
		return v, nil
	}
	return dbool(true), nil
}

func evalOrExpr(expr *OrExpr, env Env) (Datum, error) {
	left, err := EvalExpr(expr.Left, env)
	if err != nil {
		return null, err
	}
	if left != null {
		if v, err := left.Bool(); err != nil {
			return null, err
		} else if v {
			return v, nil
		}
	}
	right, err := EvalExpr(expr.Right, env)
	if err != nil {
		return null, err
	}
	if right == null {
		return null, nil
	}
	if v, err := right.Bool(); err != nil {
		return null, err
	} else if v {
		return v, nil
	}
	if left == null {
		return null, nil
	}
	return dbool(false), nil
}

func evalNotExpr(expr *NotExpr, env Env) (Datum, error) {
	d, err := EvalExpr(expr.Expr, env)
	if err != nil {
		return null, err
	}
	if d == null {
		return null, nil
	}
	v, err := d.Bool()
	if err != nil {
		return null, err
	}
	return !v, nil
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
		v, err := d.Bool()
		if err != nil {
			return null, err
		}
		return !v, nil
	}
	return d, nil
}

func evalNullCheck(expr *NullCheck, env Env) (Datum, error) {
	d, err := EvalExpr(expr.Expr, env)
	if err != nil {
		return null, err
	}
	v := d == null
	if expr.Not {
		v = !v
	}
	return dbool(v), nil
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

	if left == null || right == null {
		return null, nil
	}

	not := false
	op := expr.Operator
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
	}

	f := cmpOps[cmpArgs{op, reflect.TypeOf(left), reflect.TypeOf(right)}]
	if f != nil {
		d, err := f(left, right)
		if err == nil && not {
			return !d.(dbool), nil
		}
		return d, err
	}

	switch op {
	case In, NotIn, Like, NotLike:
		return null, fmt.Errorf("TODO(pmattis): unsupported comparison operator: %s", expr.Operator)
	}

	return null, fmt.Errorf("unsupported comparison operator: <%s> %s <%s>",
		left.Type(), expr.Operator, right.Type())
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
	f := binOps[binArgs{expr.Operator, reflect.TypeOf(left), reflect.TypeOf(right)}]
	if f != nil {
		return f(left, right)
	}
	return null, fmt.Errorf("unsupported binary operator: <%s> %s <%s>",
		left.Type(), expr.Operator, right.Type())
}

func evalUnaryExpr(expr *UnaryExpr, env Env) (Datum, error) {
	d, err := EvalExpr(expr.Expr, env)
	if err != nil {
		return null, err
	}
	f := unaryOps[unaryArgs{expr.Operator, reflect.TypeOf(d)}]
	if f != nil {
		return f(d)
	}
	return null, fmt.Errorf("unsupported unary operator: %s <%s>",
		expr.Operator, d.Type())
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
