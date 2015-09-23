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
func (ctx EvalContext) NormalizeAndTypeCheckExpr(expr Expr) (Expr, error) {
	var err error
	expr, err = ctx.NormalizeExpr(expr)
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

	case *IsExpr:
		if _, err := TypeCheckExpr(t.Expr); err != nil {
			return nil, err
		}
		return DummyBool, nil

	case *IsOfTypeExpr:
		if _, err := TypeCheckExpr(t.Expr); err != nil {
			return nil, err
		}
		return DummyBool, nil

	case *ExistsExpr:
		return TypeCheckExpr(t.Subquery)

	case *IfExpr:
		return typeCheckIfExpr(t)

	case *NullIfExpr:
		return typeCheckNullIfExpr(t)

	case *CoalesceExpr:
		return typeCheckCoalesceExpr(t)

	case DString:
		return DummyString, nil

	case DBytes:
		return DummyBytes, nil

	case DInt, IntVal:
		return DummyInt, nil

	case DFloat, NumVal:
		return DummyFloat, nil

	case DBool:
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
		// Avoid type checking subqueries. We need the subquery to be expanded in
		// order to do so properly.
		return DNull, nil

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
	case NotLike:
		// NotLike(left, right) is implemented as !Like(left, right)
		op = Like
	case NotSimilarTo:
		// NotSimilarTo(left, right) is implemented as !SimilarTo(left, right)
		op = SimilarTo
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

	return nil, fmt.Errorf("unsupported comparison operator: <%s> %s <%s>",
		dummyLeft.Type(), op, dummyRight.Type())
}

func typeCheckIfExpr(expr *IfExpr) (Datum, error) {
	cond, err := TypeCheckExpr(expr.Cond)
	if err != nil {
		return nil, err
	}
	if cond != DNull && cond != DummyBool {
		return nil, fmt.Errorf("IF condition must be a boolean: %s", cond.Type())
	}
	dummyTrue, err := TypeCheckExpr(expr.True)
	if err != nil {
		return nil, err
	}
	dummyElse, err := TypeCheckExpr(expr.Else)
	if err != nil {
		return nil, err
	}
	if dummyTrue == DNull {
		return dummyElse, nil
	}
	if dummyElse == DNull {
		return dummyTrue, nil
	}
	if dummyTrue != dummyElse {
		return nil, fmt.Errorf("incompatible IF expressions %s, %s", dummyTrue.Type(), dummyElse.Type())
	}
	return dummyTrue, nil
}

func typeCheckNullIfExpr(expr *NullIfExpr) (Datum, error) {
	expr1, err := TypeCheckExpr(expr.Expr1)
	if err != nil {
		return nil, err
	}
	expr2, err := TypeCheckExpr(expr.Expr2)
	if err != nil {
		return nil, err
	}
	if expr1 == DNull {
		return expr2, nil
	}
	if expr2 != DNull && expr1 != expr2 {
		return nil, fmt.Errorf("incompatible NULLIF expressions %s, %s", expr1.Type(), expr2.Type())
	}
	return expr1, nil
}

func typeCheckCoalesceExpr(expr *CoalesceExpr) (Datum, error) {
	var dummyArg Datum
	for _, e := range expr.Exprs {
		arg, err := TypeCheckExpr(e)
		if err != nil {
			return nil, err
		}
		if dummyArg == nil || dummyArg == DNull {
			dummyArg = arg
		} else if dummyArg != arg && arg != DNull {
			return nil, fmt.Errorf("incompatible %s expressions %s, %s", expr.Name, dummyArg.Type(), arg.Type())
		}
	}
	return dummyArg, nil
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
	// Cache is warm and `fn` encodes its return type.
	if expr.fn.returnType != nil {
		return expr.fn.returnType, nil
	}

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

	// Cache is cold, do the lookup.
	if expr.fn.fn == nil {
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

		// Function lookup failed.
		if expr.fn.fn == nil {
			typeNames := make([]string, 0, len(dummyArgs))
			for _, dummyArg := range dummyArgs {
				typeNames = append(typeNames, dummyArg.Type())
			}
			return nil, fmt.Errorf("unknown signature for %s: %s(%s)",
				expr.Name, expr.Name, strings.Join(typeNames, ", "))
		}
	}

	// Function lookup succeeded and `fn` encodes its return type.
	if expr.fn.returnType != nil {
		return expr.fn.returnType, nil
	}

	// Function lookup succeeded but `fn` doesn't encode its return type.
	// We need to call the function with dummy arguments.
	res, err := expr.fn.fn(defaultContext, dummyArgs)
	if err != nil {
		return nil, fmt.Errorf("%s: %v", expr.Name, err)
	}
	return res, nil
}

func typeCheckCaseExpr(expr *CaseExpr) (Datum, error) {
	var dummyCond, dummyVal Datum

	if expr.Expr != nil {
		var err error
		dummyCond, err = TypeCheckExpr(expr.Expr)
		if err != nil {
			return nil, err
		}
	}

	if expr.Else != nil {
		var err error
		dummyVal, err = TypeCheckExpr(expr.Else)
		if err != nil {
			return nil, err
		}
	}

	for _, when := range expr.Whens {
		nextDummyCond, err := TypeCheckExpr(when.Cond)
		if err != nil {
			return nil, err
		}
		if dummyCond == nil || dummyCond == DNull {
			dummyCond = nextDummyCond
		} else if !(nextDummyCond == DNull || nextDummyCond == dummyCond) {
			return nil, fmt.Errorf("incompatible condition types %s, %s", dummyCond.Type(), nextDummyCond.Type())
		}

		nextDummyVal, err := TypeCheckExpr(when.Val)
		if err != nil {
			return nil, err
		}
		if dummyVal == nil || dummyVal == DNull {
			dummyVal = nextDummyVal
		} else if !(nextDummyVal == DNull || nextDummyVal == dummyVal) {
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

	case *StringType:
		switch dummyExpr {
		case DummyBool, DummyInt, DummyFloat, DNull, DummyString, DummyBytes:
			return DummyString, nil
		}

	case *BytesType:
		switch dummyExpr {
		case DummyBytes, DummyString:
			return DummyBytes, nil
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
