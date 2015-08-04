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
	"bytes"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
)

var errZeroModulus = errors.New("zero modulus")

// TODO(pmattis):
//
// - Support decimal arithmetic.
//
// - Allow partial expression evaluation to simplify expressions before being
//   used in where clauses. Make Datum implement Expr and change EvalExpr to
//   return an Expr.

// A Datum holds either a bool, int64, float64, string or []Datum.
type Datum interface {
	Expr
	Type() string
}

var _ Datum = DBool(false)
var _ Datum = DInt(0)
var _ Datum = DFloat(0)
var _ Datum = DString("")
var _ Datum = DTuple{}
var _ Datum = DNull{}

// DBool is the boolean Datum.
type DBool bool

func getBool(d Datum) (DBool, error) {
	if v, ok := d.(DBool); ok {
		return v, nil
	}
	return false, fmt.Errorf("cannot convert %s to bool", d.Type())
}

// Type implements the Datum interface.
func (d DBool) Type() string {
	return "bool"
}

func (d DBool) String() string {
	return BoolVal(d).String()
}

// DInt is the int Datum.
type DInt int64

// Type implements the Datum interface.
func (d DInt) Type() string {
	return "int"
}

func (d DInt) String() string {
	return strconv.FormatInt(int64(d), 10)
}

// DFloat is the float Datum.
type DFloat float64

// Type implements the Datum interface.
func (d DFloat) Type() string {
	return "float"
}

func (d DFloat) String() string {
	return strconv.FormatFloat(float64(d), 'g', -1, 64)
}

// DString is the string Datum.
type DString string

// Type implements the Datum interface.
func (d DString) Type() string {
	return "string"
}

func (d DString) String() string {
	return StrVal(d).String()
}

// DTuple is the tuple Datum.
type DTuple []Datum

// Type implements the Datum interface.
func (d DTuple) Type() string {
	return "tuple"
}

func (d DTuple) String() string {
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

// DNull is the NULL Datum.
type DNull struct{}

var null = DNull{}

// Type implements the Datum interface.
func (d DNull) Type() string {
	return "NULL"
}

func (d DNull) String() string {
	return "NULL"
}

var (
	boolType   = reflect.TypeOf(DBool(false))
	intType    = reflect.TypeOf(DInt(0))
	floatType  = reflect.TypeOf(DFloat(0))
	stringType = reflect.TypeOf(DString(""))
	tupleType  = reflect.TypeOf(DTuple{})
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
		return -d.(DInt), nil
	},
	unaryArgs{UnaryMinus, floatType}: func(d Datum) (Datum, error) {
		return -d.(DFloat), nil
	},

	unaryArgs{UnaryComplement, intType}: func(d Datum) (Datum, error) {
		return ^d.(DInt), nil
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
		return left.(DInt) & right.(DInt), nil
	},

	binArgs{Bitor, intType, intType}: func(left Datum, right Datum) (Datum, error) {
		return left.(DInt) | right.(DInt), nil
	},

	binArgs{Bitxor, intType, intType}: func(left Datum, right Datum) (Datum, error) {
		return left.(DInt) ^ right.(DInt), nil
	},

	// TODO(pmattis): Overflow/underflow checks?

	// TODO(pmattis): Should we allow the implicit conversion from int to float
	// below. Once we have cast operators we could remove them. See #1626.

	binArgs{Plus, intType, intType}: func(left Datum, right Datum) (Datum, error) {
		return left.(DInt) + right.(DInt), nil
	},
	binArgs{Plus, floatType, floatType}: func(left Datum, right Datum) (Datum, error) {
		return left.(DFloat) + right.(DFloat), nil
	},

	binArgs{Minus, intType, intType}: func(left Datum, right Datum) (Datum, error) {
		return left.(DInt) - right.(DInt), nil
	},
	binArgs{Minus, floatType, floatType}: func(left Datum, right Datum) (Datum, error) {
		return left.(DFloat) - right.(DFloat), nil
	},

	binArgs{Mult, intType, intType}: func(left Datum, right Datum) (Datum, error) {
		return left.(DInt) * right.(DInt), nil
	},
	binArgs{Mult, floatType, floatType}: func(left Datum, right Datum) (Datum, error) {
		return left.(DFloat) * right.(DFloat), nil
	},

	binArgs{Div, intType, intType}: func(left Datum, right Datum) (Datum, error) {
		return DFloat(left.(DInt)) / DFloat(right.(DInt)), nil
	},
	binArgs{Div, floatType, floatType}: func(left Datum, right Datum) (Datum, error) {
		return left.(DFloat) / right.(DFloat), nil
	},

	binArgs{Mod, intType, intType}: func(left Datum, right Datum) (Datum, error) {
		r := right.(DInt)
		if r == 0 {
			return nil, errZeroModulus
		}
		return left.(DInt) % r, nil
	},
	binArgs{Mod, floatType, floatType}: func(left Datum, right Datum) (Datum, error) {
		return DFloat(math.Mod(float64(left.(DFloat)), float64(right.(DFloat)))), nil
	},

	binArgs{Concat, stringType, stringType}: func(left Datum, right Datum) (Datum, error) {
		return left.(DString) + right.(DString), nil
	},
	binArgs{Concat, boolType, stringType}: func(left Datum, right Datum) (Datum, error) {
		return DString(left.String()) + right.(DString), nil
	},
	binArgs{Concat, stringType, boolType}: func(left Datum, right Datum) (Datum, error) {
		return left.(DString) + DString(right.String()), nil
	},
	binArgs{Concat, intType, stringType}: func(left Datum, right Datum) (Datum, error) {
		return DString(left.String()) + right.(DString), nil
	},
	binArgs{Concat, stringType, intType}: func(left Datum, right Datum) (Datum, error) {
		return left.(DString) + DString(right.String()), nil
	},
	binArgs{Concat, floatType, stringType}: func(left Datum, right Datum) (Datum, error) {
		return DString(left.String()) + right.(DString), nil
	},
	binArgs{Concat, stringType, floatType}: func(left Datum, right Datum) (Datum, error) {
		return left.(DString) + DString(right.String()), nil
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
		return DBool(left.(DString) == right.(DString)), nil
	},
	cmpArgs{EQ, boolType, boolType}: func(left Datum, right Datum) (Datum, error) {
		return DBool(left.(DBool) == right.(DBool)), nil
	},
	cmpArgs{EQ, intType, intType}: func(left Datum, right Datum) (Datum, error) {
		return DBool(left.(DInt) == right.(DInt)), nil
	},
	cmpArgs{EQ, floatType, floatType}: func(left Datum, right Datum) (Datum, error) {
		return DBool(left.(DFloat) == right.(DFloat)), nil
	},

	cmpArgs{LT, stringType, stringType}: func(left Datum, right Datum) (Datum, error) {
		return DBool(left.(DString) < right.(DString)), nil
	},
	cmpArgs{LT, boolType, boolType}: func(left Datum, right Datum) (Datum, error) {
		return DBool(!left.(DBool) && right.(DBool)), nil
	},
	cmpArgs{LT, intType, intType}: func(left Datum, right Datum) (Datum, error) {
		return DBool(left.(DInt) < right.(DInt)), nil
	},
	cmpArgs{LT, floatType, floatType}: func(left Datum, right Datum) (Datum, error) {
		return DBool(left.(DFloat) < right.(DFloat)), nil
	},

	cmpArgs{LE, stringType, stringType}: func(left Datum, right Datum) (Datum, error) {
		return DBool(left.(DString) <= right.(DString)), nil
	},
	cmpArgs{LE, boolType, boolType}: func(left Datum, right Datum) (Datum, error) {
		return DBool(!left.(DBool) || right.(DBool)), nil
	},
	cmpArgs{LE, intType, intType}: func(left Datum, right Datum) (Datum, error) {
		return DBool(left.(DInt) <= right.(DInt)), nil
	},
	cmpArgs{LE, floatType, floatType}: func(left Datum, right Datum) (Datum, error) {
		return DBool(left.(DFloat) <= right.(DFloat)), nil
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

// nilEnv is an empty environment. It's useful to avoid nil checks.
type nilEnv struct{}

func (*nilEnv) Get(_ string) (Datum, bool) {
	return nil, false
}

var emptyEnv *nilEnv

// EvalExpr evaluates an SQL expression in the context of an
// environment. Expression evaluation is a mostly straightforward walk over the
// parse tree. The only significant complexity is the handling of types and
// implicit conversions. See binOps and cmpOps for more details.
func EvalExpr(expr Expr, env Env) (Datum, error) {
	if env == nil {
		// This avoids having to worry about `env` being a nil interface
		// anywhere else.
		env = emptyEnv
	}

	switch t := expr.(type) {
	case Row:
		// Row and Tuple are synonymous: convert Row to Tuple to simplify logic
		// below.
		expr = Tuple(t)
	}

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
		return DString(t), nil

	case StrVal:
		return DString(t), nil

	case IntVal:
		return DInt(t), nil

	case NumVal:
		v, err := strconv.ParseFloat(string(t), 64)
		if err != nil {
			return null, err
		}
		return DFloat(v), nil

	case BoolVal:
		return DBool(t), nil

	case ValArg:
		// Placeholders should have been replaced before expression evaluation.

	case NullVal:
		return null, nil

	case *QualifiedName:
		if d, ok := env.Get(t.String()); ok {
			return d, nil
		}
		return null, fmt.Errorf("column \"%s\" not found", t)

	case Tuple:
		tuple := make(DTuple, 0, len(t))
		for _, v := range t {
			d, err := EvalExpr(v, env)
			if err != nil {
				return null, err
			}
			tuple = append(tuple, d)
		}
		return tuple, nil

	case Datum:
		return t, nil

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

	case *CastExpr:
		return evalCastExpr(t, env)

	default:
		panic(fmt.Sprintf("eval: unsupported expression type: %T", expr))
	}

	return null, fmt.Errorf("eval: unexpected expression: %T", expr)
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
	return DBool(true), nil
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
	return DBool(false), nil
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

	var v DBool
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
	return DBool(v), nil
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
			return !d.(DBool), nil
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
	name := strings.ToLower(expr.Name.String())
	b, ok := builtins[name]
	if !ok {
		return null, fmt.Errorf("%s: unknown function", expr.Name)
	}
	if b.nArgs != -1 && b.nArgs != len(expr.Exprs) {
		return null, fmt.Errorf("%s: incorrect number of arguments: %d vs %d",
			expr.Name, b.nArgs, len(expr.Exprs))
	}

	args := make(DTuple, 0, len(expr.Exprs))
	for _, e := range expr.Exprs {
		arg, err := EvalExpr(e, env)
		if err != nil {
			return null, err
		}
		args = append(args, arg)
	}

	res, err := b.fn(args)
	if err != nil {
		return null, fmt.Errorf("%s: %v", expr.Name, err)
	}
	return res, nil
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
	left := ldatum.(DTuple)
	right := rdatum.(DTuple)
	if len(left) != len(right) {
		return DBool(false), nil
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
	return DBool(true), nil
}

func evalTupleIN(arg, values Datum) (Datum, error) {
	if arg == null {
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
				return null, fmt.Errorf("unsupported comparison operator: <%s> %s <%s>",
					arg.Type(), EQ, val.Type())
			}
			m[val] = struct{}{}
		}
		if _, exists := m[arg]; exists {
			return DBool(true), nil
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

	return DBool(false), nil
}

func evalCastExpr(expr *CastExpr, env Env) (Datum, error) {
	d, err := EvalExpr(expr.Expr, env)
	if err != nil {
		return null, err
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
				return null, err
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
				return null, err
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
				return null, err
			}
			return DFloat(f), nil
		}

	case *CharType, *TextType, *BlobType:
		var s DString
		switch d.(type) {
		case DBool, DInt, DFloat, DNull:
			s = DString(d.String())
		case DString:
			s = d.(DString)
		}
		if c, ok := expr.Type.(*CharType); ok {
			// If the CHAR type specifies a limit we truncate to that limit:
			//   'hello'::CHAR(2) -> 'he'
			if c.N > 0 && c.N < len(s) {
				s = s[:c.N]
			}
		}
		return s, nil

		// TODO(pmattis): unimplemented.
		// case *BitType:
		// case *DecimalType:
		// case *DateType:
		// case *TimeType:
		// case *TimestampType:
	}

	return null, fmt.Errorf("invalid cast: %s -> %s", d.Type(), expr.Type)
}
