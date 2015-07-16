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
	"bytes"
	"fmt"
	"math"
	"reflect"
	"strconv"
)

// TODO(pmattis):
//
// - Support decimal arithmetic.
//
// - Allow partial expression evaluation to simplify expressions before being
//   used in where clauses. Make Datum implement Expr and change EvalExpr to
//   return an Expr.

// A Datum holds either a bool, int64, float64, string or []Datum.
type Datum interface {
	Type() string
	String() string
}

var _ Datum = dbool(false)
var _ Datum = dint(0)
var _ Datum = dfloat(0)
var _ Datum = dstring("")
var _ Datum = dtuple{}
var _ Datum = dnull{}

type dbool bool

func getBool(d Datum) (dbool, error) {
	if v, ok := d.(dbool); ok {
		return v, nil
	}
	return false, fmt.Errorf("cannot convert %s to bool", d.Type())
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

func (d dint) Type() string {
	return "int"
}

func (d dint) String() string {
	return strconv.FormatInt(int64(d), 10)
}

type dfloat float64

func (d dfloat) Type() string {
	return "float"
}

func (d dfloat) String() string {
	return strconv.FormatFloat(float64(d), 'g', -1, 64)
}

type dstring string

func (d dstring) Type() string {
	return "string"
}

func (d dstring) String() string {
	return string(d)
}

type dtuple []Datum

func (d dtuple) Type() string {
	return "tuple"
}

func (d dtuple) String() string {
	var buf bytes.Buffer
	_ = buf.WriteByte('(')
	for i, v := range d {
		if i > 0 {
			_, _ = buf.WriteString(", ")
		}
		_, _ = buf.WriteString(v.String())
	}
	_ = buf.WriteByte(')')
	return buf.String()
}

type dnull struct{}

var null = dnull{}

func (d dnull) Type() string {
	return "NULL"
}

func (d dnull) String() string {
	return "NULL"
}

var (
	boolType   = reflect.TypeOf(dbool(false))
	intType    = reflect.TypeOf(dint(0))
	floatType  = reflect.TypeOf(dfloat(0))
	stringType = reflect.TypeOf(dstring(""))
	tupleType  = reflect.TypeOf(dtuple{})
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

	// TODO(pmattis): Should we allow the implicit conversion from int to float
	// below. Once we have cast operators we could remove them. See #1626.

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

func init() {
	// This avoids an init-loop if we try to initialize this operation when
	// cmpOps is declared. The loop is caused by evalTupleEQ using cmpOps
	// internally.
	cmpOps[cmpArgs{EQ, tupleType, tupleType}] = evalTupleEQ

	cmpOps[cmpArgs{In, boolType, tupleType}] = evalTupleIN
	cmpOps[cmpArgs{In, intType, tupleType}] = evalTupleIN
	cmpOps[cmpArgs{In, floatType, tupleType}] = evalTupleIN
	cmpOps[cmpArgs{In, stringType, tupleType}] = evalTupleIN
	cmpOps[cmpArgs{In, tupleType, tupleType}] = evalTupleIN
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
		if len(t) == 1 {
			return EvalExpr(t[0], env)
		}
		tuple := make(dtuple, 0, len(t))
		for _, v := range t {
			d, err := EvalExpr(v, env)
			if err != nil {
				return null, err
			}
			tuple = append(tuple, d)
		}
		return tuple, nil

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
	if v, err := getBool(left); err != nil {
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
	if v, err := getBool(right); err != nil {
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
		if v, err := getBool(left); err != nil {
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
	if v, err := getBool(right); err != nil {
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
	v, err := getBool(d)
	if err != nil {
		return null, err
	}
	return !v, nil
}

func evalRangeCond(expr *RangeCond, env Env) (Datum, error) {
	// A range such as "left BETWEEN from AND to" is equivalent to "left >= from
	// AND left <= to". The only tricky part is that we evaluate "left" only
	// once.

	left, err := EvalExpr(expr.Left, env)
	if err != nil {
		return null, err
	}

	limits := [2]struct {
		op   ComparisonOp
		expr Expr
	}{
		{GE, expr.From},
		{LE, expr.To},
	}

	var v dbool
	for _, l := range limits {
		arg, err := EvalExpr(l.expr, env)
		if err != nil {
			return null, err
		}
		cmp, err := evalComparisonOp(l.op, left, arg)
		if err != nil {
			return null, err
		}
		if v, err = getBool(cmp); err != nil {
			return null, err
		} else if !v {
			break
		}
	}

	if expr.Not {
		return !v, nil
	}
	return v, nil
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

	return evalComparisonOp(expr.Operator, left, right)
}

func evalComparisonOp(op ComparisonOp, left, right Datum) (Datum, error) {
	if left == null || right == null {
		return null, nil
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
	case Like, NotLike:
		return null, fmt.Errorf("TODO(pmattis): unsupported comparison operator: %s", op)
	}

	return null, fmt.Errorf("unsupported comparison operator: <%s> %s <%s>",
		left.Type(), op, right.Type())
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
		// CASE <val> WHEN <expr> THEN ...
		//
		// For each "when" expression we compare for equality to <val>.
		val, err := EvalExpr(expr.Expr, env)
		if err != nil {
			return null, err
		}

		for _, when := range expr.Whens {
			arg, err := EvalExpr(when.Cond, env)
			if err != nil {
				return null, err
			}
			d, err := evalComparisonOp(EQ, val, arg)
			if err != nil {
				return null, err
			}
			if v, err := getBool(d); err != nil {
				return null, err
			} else if v {
				return EvalExpr(when.Val, env)
			}
		}
	} else {
		// CASE WHEN <bool-expr> THEN ...
		for _, when := range expr.Whens {
			d, err := EvalExpr(when.Cond, env)
			if err != nil {
				return null, err
			}
			if v, err := getBool(d); err != nil {
				return null, err
			} else if v {
				return EvalExpr(when.Val, env)
			}
		}
	}

	if expr.Else != nil {
		return EvalExpr(expr.Else, env)
	}
	return null, nil
}

func evalTupleEQ(ldatum, rdatum Datum) (Datum, error) {
	left := ldatum.(dtuple)
	right := rdatum.(dtuple)
	if len(left) != len(right) {
		return dbool(false), nil
	}
	for i := range left {
		d, err := evalComparisonOp(EQ, left[i], right[i])
		if err != nil {
			return null, err
		}
		if v, err := getBool(d); err != nil {
			return null, err
		} else if !v {
			return v, nil
		}
	}
	return dbool(true), nil
}

func evalTupleIN(arg, values Datum) (Datum, error) {
	if arg == null {
		return dbool(false), nil
	}

	vtuple := values.(dtuple)

	// TODO(pmattis): If we're evaluating the expression multiple times we should
	// use a map when possible. This works as long as arg is not a tuple. Note
	// that the usage of the map is currently disabled via the "&& false" because
	// building the map is a pessimization if we're only evaluating the
	// expression once. We need to determine when the expression will be
	// evaluated multiple times before enabling. Also need to figure out a way to
	// use the map approach for tuples. One idea is to encode the tuples into
	// strings and then use a map of strings.
	if _, ok := arg.(dtuple); !ok && false {
		m := make(map[Datum]struct{}, len(vtuple))
		for _, val := range vtuple {
			if reflect.TypeOf(arg) != reflect.TypeOf(val) {
				return null, fmt.Errorf("unsupported comparison operator: <%s> %s <%s>",
					arg.Type(), EQ, val.Type())
			}
			m[val] = struct{}{}
		}
		if _, exists := m[arg]; exists {
			return dbool(true), nil
		}
	} else {
		// TODO(pmattis): We should probably first check that all of the values are
		// type compatible with the arg.
		for _, val := range vtuple {
			d, err := evalComparisonOp(EQ, arg, val)
			if err != nil {
				return null, err
			}
			if v, err := getBool(d); err != nil {
				return null, err
			} else if v {
				return v, nil
			}
		}
	}

	return dbool(false), nil
}
