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

// TypeCheck implements the Expr interface.
func (expr *AndExpr) TypeCheck() (Datum, error) {
	return typeCheckBooleanExprs(expr.Left, expr.Right)
}

// TypeCheck implements the Expr interface.
func (expr *BinaryExpr) TypeCheck() (Datum, error) {
	dummyLeft, err := expr.Left.TypeCheck()
	if err != nil {
		return nil, err
	}
	if dummyLeft == DNull {
		return DNull, nil
	}
	dummyRight, err := expr.Right.TypeCheck()
	if err != nil {
		return nil, err
	}
	if dummyRight == DNull {
		return DNull, nil
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

// TypeCheck implements the Expr interface.
func (expr *CaseExpr) TypeCheck() (Datum, error) {
	var dummyCond, dummyVal Datum

	if expr.Expr != nil {
		var err error
		dummyCond, err = expr.Expr.TypeCheck()
		if err != nil {
			return nil, err
		}
	}

	if expr.Else != nil {
		var err error
		dummyVal, err = expr.Else.TypeCheck()
		if err != nil {
			return nil, err
		}
	}

	for _, when := range expr.Whens {
		nextDummyCond, err := when.Cond.TypeCheck()
		if err != nil {
			return nil, err
		}
		if dummyCond == nil || dummyCond == DNull {
			dummyCond = nextDummyCond
		} else if !(nextDummyCond == DNull || nextDummyCond == dummyCond) {
			return nil, fmt.Errorf("incompatible condition types %s, %s", dummyCond.Type(), nextDummyCond.Type())
		}

		nextDummyVal, err := when.Val.TypeCheck()
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

// TypeCheck implements the Expr interface.
func (expr *CastExpr) TypeCheck() (Datum, error) {
	dummyExpr, err := expr.Expr.TypeCheck()
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

// TypeCheck implements the Expr interface.
func (expr *CoalesceExpr) TypeCheck() (Datum, error) {
	var dummyArg Datum
	for _, e := range expr.Exprs {
		arg, err := e.TypeCheck()
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

// TypeCheck implements the Expr interface.
func (expr *ComparisonExpr) TypeCheck() (Datum, error) {
	leftType, err := expr.Left.TypeCheck()
	if err != nil {
		return nil, err
	}
	rightType, err := expr.Right.TypeCheck()
	if err != nil {
		return nil, err
	}
	d, cmp, err := typeCheckComparisonOp(expr.Operator, leftType, rightType)
	expr.fn = cmp
	return d, err
}

// TypeCheck implements the Expr interface.
func (expr *ExistsExpr) TypeCheck() (Datum, error) {
	return expr.Subquery.TypeCheck()
}

// TypeCheck implements the Expr interface.
func (expr *FuncExpr) TypeCheck() (Datum, error) {
	// Cache is warm and `fn` encodes its return type.
	if expr.fn.returnType != nil {
		return expr.fn.returnType, nil
	}

	dummyArgs := make(DTuple, 0, len(expr.Exprs))
	types := make(typeList, 0, len(expr.Exprs))
	for _, e := range expr.Exprs {
		dummyArg, err := e.TypeCheck()
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

// TypeCheck implements the Expr interface.
func (expr *IfExpr) TypeCheck() (Datum, error) {
	cond, err := expr.Cond.TypeCheck()
	if err != nil {
		return nil, err
	}
	if cond != DNull && cond != DummyBool {
		return nil, fmt.Errorf("IF condition must be a boolean: %s", cond.Type())
	}
	dummyTrue, err := expr.True.TypeCheck()
	if err != nil {
		return nil, err
	}
	dummyElse, err := expr.Else.TypeCheck()
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

// TypeCheck implements the Expr interface.
func (expr *IsOfTypeExpr) TypeCheck() (Datum, error) {
	if _, err := expr.Expr.TypeCheck(); err != nil {
		return nil, err
	}
	return DummyBool, nil
}

// TypeCheck implements the Expr interface.
func (expr *NotExpr) TypeCheck() (Datum, error) {
	return typeCheckBooleanExprs(expr.Expr)
}

// TypeCheck implements the Expr interface.
func (expr *NullIfExpr) TypeCheck() (Datum, error) {
	expr1, err := expr.Expr1.TypeCheck()
	if err != nil {
		return nil, err
	}
	expr2, err := expr.Expr2.TypeCheck()
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

// TypeCheck implements the Expr interface.
func (expr *OrExpr) TypeCheck() (Datum, error) {
	return typeCheckBooleanExprs(expr.Left, expr.Right)
}

// TypeCheck implements the Expr interface.
func (expr *QualifiedName) TypeCheck() (Datum, error) {
	return nil, fmt.Errorf("qualified name \"%s\" not found", expr)
}

// TypeCheck implements the Expr interface.
func (expr *RangeCond) TypeCheck() (Datum, error) {
	leftType, err := expr.Left.TypeCheck()
	if err != nil {
		return nil, err
	}
	fromType, err := expr.From.TypeCheck()
	if err != nil {
		return nil, err
	}
	toType, err := expr.To.TypeCheck()
	if err != nil {
		return nil, err
	}

	if _, _, err := typeCheckComparisonOp(GT, leftType, fromType); err != nil {
		return nil, err
	}
	if _, _, err := typeCheckComparisonOp(LT, leftType, toType); err != nil {
		return nil, err
	}

	return cmpOpResultType, nil
}

// TypeCheck implements the Expr interface.
func (expr *Subquery) TypeCheck() (Datum, error) {
	// Avoid type checking subqueries. We need the subquery to be expanded in
	// order to do so properly.
	return DNull, nil
}

// TypeCheck implements the Expr interface.
func (expr *UnaryExpr) TypeCheck() (Datum, error) {
	dummyExpr, err := expr.Expr.TypeCheck()
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

// TypeCheck implements the Expr interface.
func (expr Array) TypeCheck() (Datum, error) {
	return nil, util.Errorf("unhandled type %T", expr)
}

// TypeCheck implements the Expr interface.
func (expr DefaultVal) TypeCheck() (Datum, error) {
	return nil, util.Errorf("unhandled type %T", expr)
}

// TypeCheck implements the Expr interface.
func (expr IntVal) TypeCheck() (Datum, error) {
	return DummyInt, nil
}

// TypeCheck implements the Expr interface.
func (expr NumVal) TypeCheck() (Datum, error) {
	return DummyFloat, nil
}

// TypeCheck implements the Expr interface.
func (expr Row) TypeCheck() (Datum, error) {
	return Tuple(expr).TypeCheck()
}

// TypeCheck implements the Expr interface.
func (expr Tuple) TypeCheck() (Datum, error) {
	tuple := make(DTuple, 0, len(expr))
	for _, v := range expr {
		d, err := v.TypeCheck()
		if err != nil {
			return nil, err
		}
		tuple = append(tuple, d)
	}
	return tuple, nil
}

// TypeCheck implements the Expr interface.
func (expr ValArg) TypeCheck() (Datum, error) {
	return nil, util.Errorf("unhandled type %T", expr)
}

// TypeCheck implements the Expr interface.
func (expr DBool) TypeCheck() (Datum, error) {
	return DummyBool, nil
}

// TypeCheck implements the Expr interface.
func (expr DBytes) TypeCheck() (Datum, error) {
	return DummyBytes, nil
}

// TypeCheck implements the Expr interface.
func (expr DDate) TypeCheck() (Datum, error) {
	return DummyDate, nil
}

// TypeCheck implements the Expr interface.
func (expr DFloat) TypeCheck() (Datum, error) {
	return DummyFloat, nil
}

// TypeCheck implements the Expr interface.
func (expr DInt) TypeCheck() (Datum, error) {
	return DummyInt, nil
}

// TypeCheck implements the Expr interface.
func (expr DInterval) TypeCheck() (Datum, error) {
	return DummyInterval, nil
}

// TypeCheck implements the Expr interface.
func (expr dNull) TypeCheck() (Datum, error) {
	return DNull, nil
}

// TypeCheck implements the Expr interface.
func (expr DString) TypeCheck() (Datum, error) {
	return DummyString, nil
}

// TypeCheck implements the Expr interface.
func (expr DTimestamp) TypeCheck() (Datum, error) {
	return DummyTimestamp, nil
}

// TypeCheck implements the Expr interface.
func (expr DTuple) TypeCheck() (Datum, error) {
	tuple := make(DTuple, 0, len(expr))
	for _, v := range expr {
		d, err := v.TypeCheck()
		if err != nil {
			return nil, err
		}
		tuple = append(tuple, d)
	}
	return tuple, nil
}

func typeCheckBooleanExprs(exprs ...Expr) (Datum, error) {
	for _, expr := range exprs {
		dummyExpr, err := expr.TypeCheck()
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

func typeCheckComparisonOp(op ComparisonOp, dummyLeft, dummyRight Datum) (Datum, cmpOp, error) {
	if dummyLeft == DNull || dummyRight == DNull {
		switch op {
		case Is, IsNot, IsDistinctFrom, IsNotDistinctFrom:
			// TODO(pmattis): For IS {UNKNOWN,TRUE,FALSE} we should be requiring that
			// dummyLeft == DummyBool. We currently can't distinguish NULL from
			// UNKNOWN. Is it important to do so?
			return DummyBool, cmpOp{}, nil
		default:
			return DNull, cmpOp{}, nil
		}
	}
	op, dummyLeft, dummyRight, _ = foldComparisonExpr(op, dummyLeft, dummyRight)
	lType := reflect.TypeOf(dummyLeft)
	rType := reflect.TypeOf(dummyRight)

	if cmp, ok := cmpOps[cmpArgs{op, lType, rType}]; ok {
		if op == EQ && lType == tupleType && rType == tupleType {
			if err := typeCheckTupleEQ(dummyLeft, dummyRight); err != nil {
				return nil, cmpOp{}, err
			}
		} else if op == In && rType == tupleType {
			if err := typeCheckTupleIN(dummyLeft, dummyRight); err != nil {
				return nil, cmpOp{}, err
			}
		}

		return cmpOpResultType, cmp, nil
	}

	return nil, cmpOp{}, fmt.Errorf("unsupported comparison operator: <%s> %s <%s>",
		dummyLeft.Type(), op, dummyRight.Type())
}

func typeCheckTupleEQ(lDummy, rDummy Datum) error {
	lTuple := lDummy.(DTuple)
	rTuple := rDummy.(DTuple)
	if len(lTuple) != len(rTuple) {
		return fmt.Errorf("unequal number of entries in tuple expressions: %d, %d", len(lTuple), len(rTuple))
	}

	for i := range lTuple {
		if _, _, err := typeCheckComparisonOp(EQ, lTuple[i], rTuple[i]); err != nil {
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
	for _, val := range vtuple {
		if _, _, err := typeCheckComparisonOp(EQ, arg, val); err != nil {
			return err
		}
	}

	return nil
}
