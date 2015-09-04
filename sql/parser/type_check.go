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
// Author: Tamir Duberstein (tamird@gmail.com)

package parser

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/util"
)

// NormalizeAndTypeCheckExpr is a combination of NormalizeExpr and
// TypeCheckExpr. It returns returns an error if either of
// NormalizeExpr or TypeCheckExpr return one, and otherwise returns
// the Expr returned by NormalizeExpr.
func NormalizeAndTypeCheckExpr(expr Expr) (Expr, error) {
	var err error
	expr, err = NormalizeExpr(expr)
	if err != nil {
		return nil, err
	}
	_, err = TypeCheckExpr(expr)
	if err != nil {
		return nil, err
	}
	return expr, nil
}

// TypeCheckExpr type-checks an SQL expression. Expression checking is
// a mostly straightforward walk over the parse tree.
func TypeCheckExpr(expr Expr) (Datum, error) {
	switch t := expr.(type) {
	case *AndExpr:
		return typeCheckBooleanExprs(t.Left, t.Right)

	case *OrExpr:
		return typeCheckBooleanExprs(t.Left, t.Right)

	case *NotExpr:
		return typeCheckBooleanExprs(t.Expr)

	case Row:
		// NormalizeExpr transforms this into Tuple.

	case *ParenExpr:
		// NormalizeExpr unwraps this.

	case *ComparisonExpr:
		return typeCheckComparisonExpr(t)

	case *RangeCond:
		// NormalizeExpr transforms this into an AndExpr.

	case *NullCheck:
		if _, err := TypeCheckExpr(t.Expr); err != nil {
			return nil, err
		}
		return DummyBool, nil

	case *ExistsExpr:
		// The subquery within the exists should have been executed before
		// expression evaluation and the exists nodes replaced with the result.

	case BytesVal, StrVal:
		return DummyString, nil

	case IntVal:
		return DummyInt, nil

	case NumVal:
		return DummyFloat, nil

	case BoolVal:
		return DummyBool, nil

	case ValArg:
		// Placeholders should have been replaced before expression evaluation.

	case *QualifiedName:
		return nil, fmt.Errorf("qualified name \"%s\" not found", t)

	case Tuple:
		tuple := make(DTuple, 0, len(t))
		for _, v := range t {
			d, err := TypeCheckExpr(v)
			if err != nil {
				return nil, err
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
		return typeCheckBinaryExpr(t)

	case *UnaryExpr:
		return typeCheckUnaryExpr(t)

	case *FuncExpr:
		return typeCheckFuncExpr(t)

	case *CaseExpr:
		return typeCheckCaseExpr(t)

	case *CastExpr:
		return typeCheckCastExpr(t)

	default:
		return nil, util.Errorf("typeCheck: unsupported expression: %T", expr)
	}

	return nil, util.Errorf("typeCheck: unexpected expression: %T", expr)
}

func typeCheckBooleanExprs(exprs ...Expr) (Datum, error) {
	for _, expr := range exprs {
		dummyExpr, err := TypeCheckExpr(expr)
		if err != nil {
			return nil, err
		}
		if dummyExpr == DNull {
			continue
		}
		if _, ok := dummyExpr.(DBool); !ok {
			return nil, fmt.Errorf("incompatible AND argument type %s", dummyExpr.Type())
		}
	}
	return DummyBool, nil
}

// TODO(tamird): reduce duplication with evalComparisonExpr.
func typeCheckComparisonExpr(expr *ComparisonExpr) (Datum, error) {
	leftType, err := TypeCheckExpr(expr.Left)
	if err != nil {
		return nil, err
	}
	rightType, err := TypeCheckExpr(expr.Right)
	if err != nil {
		return nil, err
	}

	return typeCheckComparisonOp(expr.Operator, leftType, rightType)
}

func typeCheckComparisonOp(op ComparisonOp, dummyLeft, dummyRight Datum) (Datum, error) {
	if dummyLeft == DNull || dummyRight == DNull {
		return DNull, nil
	}

	switch op {
	case NE:
		// NE(left, right) is implemented as !EQ(left, right).
		op = EQ
	case GT:
		// GT(left, right) is implemented as LT(right, left)
		op = LT
		dummyLeft, dummyRight = dummyRight, dummyLeft
	case GE:
		// GE(left, right) is implemented as LE(right, left)
		op = LE
		dummyLeft, dummyRight = dummyRight, dummyLeft
	case NotIn:
		// NotIn(left, right) is implemented as !IN(left, right)
		op = In
	}

	lType := reflect.TypeOf(dummyLeft)
	rType := reflect.TypeOf(dummyRight)

	// TODO(pmattis): Memoize the cmpOps lookup as we've done for unaryOps and
	// binOps.
	if _, ok := cmpOps[cmpArgs{op, lType, rType}]; ok {
		if op == EQ && lType == tupleType && rType == tupleType {
			if err := typeCheckTupleEQ(dummyLeft, dummyRight); err != nil {
				return nil, err
			}
		} else if op == In && rType == tupleType {
			if err := typeCheckTupleIN(dummyLeft, dummyRight); err != nil {
				return nil, err
			}
		}

		return cmpOpResultType, nil
	}

	switch op {
	case Like, NotLike, SimilarTo, NotSimilarTo:
		return nil, util.Errorf("TODO(pmattis): unsupported comparison operator: %s", op)
	}

	return nil, fmt.Errorf("unsupported comparison operator: <%s> %s <%s>",
		dummyLeft.Type(), op, dummyRight.Type())
}

func typeCheckBinaryExpr(expr *BinaryExpr) (Datum, error) {
	dummyLeft, err := TypeCheckExpr(expr.Left)
	if err != nil {
		return nil, err
	}
	dummyRight, err := TypeCheckExpr(expr.Right)
	if err != nil {
		return nil, err
	}
	expr.ltype = reflect.TypeOf(dummyLeft)
	expr.rtype = reflect.TypeOf(dummyRight)

	var ok bool
	if expr.fn, ok = binOps[binArgs{expr.Operator, expr.ltype, expr.rtype}]; ok {
		return expr.fn.returnType, nil
	}

	return nil, fmt.Errorf("unsupported binary operator: <%s> %s <%s>",
		dummyLeft.Type(), expr.Operator, dummyRight.Type())
}

func typeCheckUnaryExpr(expr *UnaryExpr) (Datum, error) {
	dummyExpr, err := TypeCheckExpr(expr.Expr)
	if err != nil {
		return nil, err
	}

	expr.dtype = reflect.TypeOf(dummyExpr)

	var ok bool
	if expr.fn, ok = unaryOps[unaryArgs{expr.Operator, expr.dtype}]; ok {
		return expr.fn.returnType, nil
	}

	return nil, fmt.Errorf("unsupported unary operator: %s <%s>",
		expr.Operator, dummyExpr.Type())
}

func typeCheckFuncExpr(expr *FuncExpr) (Datum, error) {
	dummyArgs := make(DTuple, 0, len(expr.Exprs))
	types := make(typeList, 0, len(expr.Exprs))
	for _, e := range expr.Exprs {
		dummyArg, err := TypeCheckExpr(e)
		if err != nil {
			return DNull, err
		}
		dummyArgs = append(dummyArgs, dummyArg)
		types = append(types, reflect.TypeOf(dummyArg))
	}

	if len(expr.Name.Indirect) > 0 {
		// We don't support qualified function names (yet).
		return nil, fmt.Errorf("unknown function: %s", expr.Name)
	}

	name := string(expr.Name.Base)
	candidates, ok := builtins[strings.ToLower(name)]
	if !ok {
		return nil, fmt.Errorf("unknown function: %s", name)
	}

	for _, candidate := range candidates {
		if candidate.match(types) {
			expr.fn = candidate
			break
		}
	}

	if expr.fn.fn == nil {
		typeNames := make([]string, 0, len(dummyArgs))
		for _, dummyArg := range dummyArgs {
			typeNames = append(typeNames, dummyArg.Type())
		}
		return nil, fmt.Errorf("unknown signature for %s: %s(%s)",
			expr.Name, expr.Name, strings.Join(typeNames, ", "))
	}

	if expr.fn.returnType != nil {
		return expr.fn.returnType, nil
	}

	// If the function doesn't encode its return type, we're gonna have
	// to call it.
	res, err := expr.fn.fn(dummyArgs)
	if err != nil {
		return nil, fmt.Errorf("%s: %v", expr.Name, err)
	}
	return res, nil
}

func typeCheckCaseExpr(expr *CaseExpr) (Datum, error) {
	var dummyCond, dummyVal Datum
	var condType, valType reflect.Type

	if expr.Expr != nil {
		var err error
		dummyCond, err = TypeCheckExpr(expr.Expr)
		if err != nil {
			return nil, err
		}
		condType = reflect.TypeOf(dummyCond)
	}

	if expr.Else != nil {
		var err error
		dummyVal, err = TypeCheckExpr(expr.Else)
		if err != nil {
			return nil, err
		}
		valType = reflect.TypeOf(dummyVal)
	}

	for _, when := range expr.Whens {
		nextDummyCond, err := TypeCheckExpr(when.Cond)
		if err != nil {
			return nil, err
		}
		nextCondType := reflect.TypeOf(nextDummyCond)
		if condType == nil || condType == nullType {
			dummyCond = nextDummyCond
			condType = nextCondType
		} else if !(nextCondType == nullType || nextCondType == condType) {
			return nil, fmt.Errorf("incompatible condition types %s, %s", dummyCond.Type(), nextDummyCond.Type())
		}

		nextDummyVal, err := TypeCheckExpr(when.Val)
		if err != nil {
			return nil, err
		}
		nextValType := reflect.TypeOf(nextDummyVal)
		if valType == nil || valType == nullType {
			dummyVal = nextDummyVal
			valType = nextValType
		} else if !(nextValType == nullType || nextValType == valType) {
			return nil, fmt.Errorf("incompatible value types %s, %s", dummyVal.Type(), nextDummyVal.Type())
		}
	}

	return dummyVal, nil
}

func typeCheckTupleEQ(lDummy, rDummy Datum) error {
	lTuple := lDummy.(DTuple)
	rTuple := rDummy.(DTuple)
	if len(lTuple) != len(rTuple) {
		return fmt.Errorf("unequal number of entries in tuple expressions: %d, %d", len(lTuple), len(rTuple))
	}

	for i := range lTuple {
		if _, err := typeCheckComparisonOp(EQ, lTuple[i], rTuple[i]); err != nil {
			return err
		}
	}

	return nil
}

func typeCheckTupleIN(arg, values Datum) error {
	if arg == DNull {
		return nil
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
				return fmt.Errorf("unsupported comparison operator: <%s> %s <%s>",
					arg.Type(), EQ, val.Type())
			}
			m[val] = struct{}{}
		}
		if _, exists := m[arg]; exists {
			return nil
		}
	} else {
		for _, val := range vtuple {
			if _, err := typeCheckComparisonOp(EQ, arg, val); err != nil {
				return err
			}
		}
	}

	return nil
}

func typeCheckCastExpr(expr *CastExpr) (Datum, error) {
	dummyExpr, err := TypeCheckExpr(expr.Expr)
	if err != nil {
		return nil, err
	}

	switch expr.Type.(type) {
	case *BoolType:
		switch dummyExpr {
		case DummyBool, DummyInt, DummyFloat, DummyString:
			return DummyBool, nil
		}

	case *IntType:
		switch dummyExpr {
		case DummyBool, DummyInt, DummyFloat, DummyString:
			return DummyInt, nil
		}

	case *FloatType:
		switch dummyExpr {
		case DummyBool, DummyInt, DummyFloat, DummyString:
			return DummyFloat, nil
		}

	case *StringType, *BytesType:
		switch dummyExpr {
		case DummyBool, DummyInt, DummyFloat, DNull, DummyString:
			return DummyString, nil
		}

	case *DateType:
		switch dummyExpr {
		case DummyString, DummyTimestamp:
			return DummyDate, nil
		}

	case *TimestampType:
		switch dummyExpr {
		case DummyString, DummyDate:
			return DummyTimestamp, nil
		}

	case *IntervalType:
		switch dummyExpr {
		case DummyString, DummyInt:
			return DummyInterval, nil
		}

	// TODO(pmattis): unimplemented.
	case *DecimalType:
	}

	return nil, fmt.Errorf("invalid cast: %s -> %s", dummyExpr.Type(), expr.Type)
}
