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

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
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
	// Compare returns -1 if the receiver is less than other, 0 if receiver is
	// equal to other and +1 if receiver is greater than other.
	Compare(other Datum) int
	// Next returns the next datum. If the receiver is "a" and the returned datum
	// is "b", then "a < b" and no other datum will compare such that "a < c <
	// b".
	Next() Datum
	// IsMax returns true if the datum is equal to the maximum value the datum
	// type can hold.
	IsMax() bool
	// IsMin returns true if the datum is equal to the minimum value the datum
	// type can hold.
	IsMin() bool
}

var _ Datum = DBool(false)
var _ Datum = DInt(0)
var _ Datum = DFloat(0)
var _ Datum = DString("")
var _ Datum = DTuple{}
var _ Datum = dNull{}

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

// Compare implements the Datum interface.
func (d DBool) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(DBool)
	if !ok {
		panic(fmt.Sprintf("unsupported comparison: %s to %s", d.Type(), other.Type()))
	}
	if !d && v {
		return -1
	}
	if d && !v {
		return 1
	}
	return 0
}

// Next implements the Datum interface.
func (d DBool) Next() Datum {
	return DBool(true)
}

// IsMax implements the Datum interface.
func (d DBool) IsMax() bool {
	return d == true
}

// IsMin implements the Datum interface.
func (d DBool) IsMin() bool {
	return d == false
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

// Compare implements the Datum interface.
func (d DInt) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(DInt)
	if !ok {
		panic(fmt.Sprintf("unsupported comparison: %s to %s", d.Type(), other.Type()))
	}
	if d < v {
		return -1
	}
	if d > v {
		return 1
	}
	return 0
}

// Next implements the Datum interface.
func (d DInt) Next() Datum {
	return d + 1
}

// IsMax implements the Datum interface.
func (d DInt) IsMax() bool {
	return d == math.MaxInt64
}

// IsMin implements the Datum interface.
func (d DInt) IsMin() bool {
	return d == math.MinInt64
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

// Compare implements the Datum interface.
func (d DFloat) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(DFloat)
	if !ok {
		panic(fmt.Sprintf("unsupported comparison: %s to %s", d.Type(), other.Type()))
	}
	if d < v {
		return -1
	}
	if d > v {
		return 1
	}
	return 0
}

// Next implements the Datum interface.
func (d DFloat) Next() Datum {
	return DFloat(math.Nextafter(float64(d), math.Inf(1)))
}

// IsMax implements the Datum interface.
func (d DFloat) IsMax() bool {
	// Using >= accounts for +inf as well.
	return d >= math.MaxFloat64
}

// IsMin implements the Datum interface.
func (d DFloat) IsMin() bool {
	// Using <= accounts for -inf as well.
	return d <= -math.MaxFloat64
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

// Compare implements the Datum interface.
func (d DString) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(DString)
	if !ok {
		panic(fmt.Sprintf("unsupported comparison: %s to %s", d.Type(), other.Type()))
	}
	if d < v {
		return -1
	}
	if d > v {
		return 1
	}
	return 0
}

// Next implements the Datum interface.
func (d DString) Next() Datum {
	return DString(proto.Key(d).Next())
}

// IsMax implements the Datum interface.
func (d DString) IsMax() bool {
	return false
}

// IsMin implements the Datum interface.
func (d DString) IsMin() bool {
	return len(d) == 0
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

// Compare implements the Datum interface.
func (d DTuple) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(DTuple)
	if !ok {
		panic(fmt.Sprintf("unsupported comparison: %s to %s", d.Type(), other.Type()))
	}
	n := len(d)
	if n > len(v) {
		n = len(v)
	}
	for i := 0; i < n; i++ {
		c := d[i].Compare(v[i])
		if c != 0 {
			return c
		}
	}
	if len(d) < len(v) {
		return -1
	}
	if len(d) > len(v) {
		return 1
	}
	return 0
}

// Next implements the Datum interface.
func (d DTuple) Next() Datum {
	n := make(DTuple, len(d))
	copy(n, d)
	n[len(n)-1] = n[len(n)-1].Next()
	return n
}

// IsMax implements the Datum interface.
func (d DTuple) IsMax() bool {
	return false
}

// IsMin implements the Datum interface.
func (d DTuple) IsMin() bool {
	return false
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

func (d DTuple) Len() int {
	return len(d)
}

func (d DTuple) Less(i, j int) bool {
	return d[i].Compare(d[j]) < 0
}

func (d DTuple) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

type dNull struct{}

// DNull is the NULL Datum.
var DNull = dNull{}

// Type implements the Datum interface.
func (d dNull) Type() string {
	return "NULL"
}

// Compare implements the Datum interface.
func (d dNull) Compare(other Datum) int {
	if other == DNull {
		return 0
	}
	return -1
}

// Next implements the Datum interface.
func (d dNull) Next() Datum {
	panic("dNull.Next not supported")
}

// IsMax implements the Datum interface.
func (d dNull) IsMax() bool {
	return true
}

// IsMin implements the Datum interface.
func (d dNull) IsMin() bool {
	return true
}

func (d dNull) String() string {
	return "NULL"
}

// DReference holds a pointer to a Datum. It is used as a level of indirection
// to replace QualifiedNames with a node whose value can change on each row.
type DReference interface {
	Datum() Datum
}

var (
	boolType   = reflect.TypeOf(DBool(false))
	intType    = reflect.TypeOf(DInt(0))
	floatType  = reflect.TypeOf(DFloat(0))
	stringType = reflect.TypeOf(DString(""))
	tupleType  = reflect.TypeOf(DTuple{})
	nullType   = reflect.TypeOf(DNull)
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

	// TODO(pmattis): Check that the shift is valid.
	binArgs{LShift, intType, intType}: func(left Datum, right Datum) (Datum, error) {
		return left.(DInt) << uint(right.(DInt)), nil
	},
	binArgs{RShift, intType, intType}: func(left Datum, right Datum) (Datum, error) {
		return left.(DInt) >> uint(right.(DInt)), nil
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
	case Row:
		// Row and Tuple are synonymous: convert Row to Tuple to simplify logic
		// below.
		expr = Tuple(t)
	}

	switch t := expr.(type) {
	case *AndExpr:
		return evalAndExpr(t)

	case *OrExpr:
		return evalOrExpr(t)

	case *NotExpr:
		return evalNotExpr(t)

	case *ParenExpr:
		return EvalExpr(t.Expr)

	case *ComparisonExpr:
		return evalComparisonExpr(t)

	case *RangeCond:
		return evalRangeCond(t)

	case *NullCheck:
		return evalNullCheck(t)

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

	case NullVal:
		return DNull, nil

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
		panic(fmt.Sprintf("eval: unsupported expression type: %T", expr))
	}

	return DNull, fmt.Errorf("eval: unexpected expression: %T", expr)
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

func evalRangeCond(expr *RangeCond) (Datum, error) {
	// A range such as "left BETWEEN from AND to" is equivalent to "left >= from
	// AND left <= to". The only tricky part is that we evaluate "left" only
	// once.

	left, err := EvalExpr(expr.Left)
	if err != nil {
		return DNull, err
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
		arg, err := EvalExpr(l.expr)
		if err != nil {
			return DNull, err
		}
		cmp, err := evalComparisonOp(l.op, left, arg)
		if err != nil {
			return DNull, err
		}
		if v, err = getBool(cmp); err != nil {
			return DNull, err
		} else if !v {
			break
		}
	}

	if expr.Not {
		return !v, nil
	}
	return v, nil
}

func evalNullCheck(expr *NullCheck) (Datum, error) {
	d, err := EvalExpr(expr.Expr)
	if err != nil {
		return DNull, err
	}
	v := d == DNull
	if expr.Not {
		v = !v
	}
	return DBool(v), nil
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
	}

	// TODO(pmattis): Memoize the cmpOps lookup as we've done for unaryOps and
	// binOps.
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
	ltype := reflect.TypeOf(left)
	rtype := reflect.TypeOf(right)
	if expr.fn == nil {
		expr.fn = binOps[binArgs{expr.Operator, ltype, rtype}]
		expr.ltype = ltype
		expr.rtype = rtype
	}
	if expr.fn == nil {
		return DNull, fmt.Errorf("unsupported binary operator: <%s> %s <%s>",
			left.Type(), expr.Operator, right.Type())
	}
	if expr.ltype != ltype || expr.rtype != rtype {
		// The argument types no longer match the memoized function. This happens
		// when a non-NULL argument becomes NULL. For example, "SELECT col+1 FROM
		// table" where col is nullable. The SELECT does not error, but returns a
		// NULL value for that select expression.
		return DNull, nil
	}
	return expr.fn(left, right)
}

func evalUnaryExpr(expr *UnaryExpr) (Datum, error) {
	d, err := EvalExpr(expr.Expr)
	if err != nil {
		return DNull, err
	}
	dtype := reflect.TypeOf(d)
	if expr.fn == nil {
		expr.fn = unaryOps[unaryArgs{expr.Operator, dtype}]
		expr.dtype = dtype
	}
	if expr.fn == nil {
		return DNull, fmt.Errorf("unsupported unary operator: %s <%s>",
			expr.Operator, d.Type())
	}
	if expr.dtype != dtype {
		// The argument type no longer match the memoized function. This happens
		// when a non-NULL argument becomes NULL. For example, "SELECT -col FROM
		// table" where col is nullable. The SELECT does not error, but returns a
		// NULL value for that select expression.
		return DNull, nil
	}
	return expr.fn(d)
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
		if len(expr.Name.Indirect) > 0 {
			// We don't support qualified function names (yet).
			return DNull, fmt.Errorf("unknown function: %s", expr.Name)
		}

		name := string(expr.Name.Base)
		candidates, ok := builtins[strings.ToLower(name)]
		if !ok {
			return DNull, fmt.Errorf("unknown function: %s", name)
		}

		for _, candidate := range candidates {
			if candidate.match(types) {
				expr.fn = candidate
				break
			}
		}
	}

	if expr.fn.fn == nil {
		typeNames := make([]string, 0, len(args))
		for _, arg := range args {
			typeNames = append(typeNames, arg.Type())
		}
		return DNull, fmt.Errorf("unknown signature for %s: %s(%s)",
			expr.Name, expr.Name, strings.Join(typeNames, ", "))
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
	whenVals := make([]Datum, 0, len(expr.Whens))
	var condType, valType reflect.Type
	for _, when := range expr.Whens {
		cond, err := EvalExpr(when.Cond)
		if err != nil {
			return DNull, err
		}
		if condType == nil {
			condType = reflect.TypeOf(cond)
		} else if cond != DNull && reflect.TypeOf(cond) != condType {
			return DNull, fmt.Errorf("incompatible condition type %s", cond.Type())
		}

		val, err := EvalExpr(when.Val)
		if err != nil {
			return DNull, err
		}

		if valType == nil {
			valType = reflect.TypeOf(val)
		} else if val != DNull && reflect.TypeOf(val) != valType {
			return DNull, fmt.Errorf("incompatible value type %s", val.Type())
		}

		whenVals = append(whenVals, val)
	}

	var elseVal Datum
	if expr.Else != nil {
		val, err := EvalExpr(expr.Else)
		if err != nil {
			return DNull, err
		}
		if val != DNull && reflect.TypeOf(val) != valType {
			return DNull, fmt.Errorf("incompatible value type %s", val.Type())
		}
		elseVal = val
	}

	if expr.Expr != nil {
		// CASE <val> WHEN <expr> THEN ...
		//
		// For each "when" expression we compare for equality to <val>.
		val, err := EvalExpr(expr.Expr)
		if err != nil {
			return DNull, err
		}

		for i, when := range expr.Whens {
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
				return whenVals[i], nil
			}
		}
	} else {
		// CASE WHEN <bool-expr> THEN ...
		for i, when := range expr.Whens {
			d, err := EvalExpr(when.Cond)
			if err != nil {
				return DNull, err
			}
			if v, err := getBool(d); err != nil {
				return DNull, err
			} else if v {
				return whenVals[i], nil
			}
		}
	}

	if elseVal != nil {
		return elseVal, nil
	}
	return DNull, nil
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
			return DNull, err
		}
		if v, err := getBool(d); err != nil {
			return DNull, err
		} else if !v {
			return v, nil
		}
	}
	return DBool(true), nil
}

func evalTupleIN(arg, values Datum) (Datum, error) {
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
				return DNull, fmt.Errorf("unsupported comparison operator: <%s> %s <%s>",
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
				return DNull, err
			}
			if v, err := getBool(d); err != nil {
				return DNull, err
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

		// TODO(pmattis): unimplemented.
		// case *DecimalType:
		// case *DateType:
		// case *TimestampType:
	}

	return DNull, fmt.Errorf("invalid cast: %s -> %s", d.Type(), expr.Type)
}
