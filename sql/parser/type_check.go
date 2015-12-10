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

var (
	typeBytes     = func(DTuple) (Datum, error) { return DummyBytes, nil }
	typeDate      = func(DTuple) (Datum, error) { return DummyDate, nil }
	typeFloat     = func(DTuple) (Datum, error) { return DummyFloat, nil }
	typeInt       = func(DTuple) (Datum, error) { return DummyInt, nil }
	typeInterval  = func(DTuple) (Datum, error) { return DummyInterval, nil }
	typeString    = func(DTuple) (Datum, error) { return DummyString, nil }
	typeTimestamp = func(DTuple) (Datum, error) { return DummyTimestamp, nil }
)

// TypeCheck implements the Expr interface.
func (expr *AndExpr) TypeCheck(args MapArgs) (Datum, error) {
	return typeCheckBooleanExprs("AND", args, expr.Left, expr.Right)
}

// TypeCheck implements the Expr interface.
func (expr *BinaryExpr) TypeCheck(args MapArgs) (Datum, error) {
	dummyLeft, err := expr.Left.TypeCheck(args)
	if err != nil {
		return nil, err
	}
	if dummyLeft == DNull {
		return DNull, nil
	}
	dummyRight, err := expr.Right.TypeCheck(args)
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
func (expr *CaseExpr) TypeCheck(args MapArgs) (Datum, error) {
	var dummyCond, dummyVal Datum

	if expr.Expr != nil {
		var err error
		dummyCond, err = expr.Expr.TypeCheck(args)
		if err != nil {
			return nil, err
		}
	}

	if expr.Else != nil {
		var err error
		dummyVal, err = expr.Else.TypeCheck(args)
		if err != nil {
			return nil, err
		}
	}

	for _, when := range expr.Whens {
		nextDummyCond, err := when.Cond.TypeCheck(args)
		if err != nil {
			return nil, err
		}
		if dummyCond == nil || dummyCond == DNull {
			dummyCond = nextDummyCond
		} else if !(nextDummyCond == DNull || nextDummyCond == dummyCond) {
			return nil, fmt.Errorf("incompatible condition types %s, %s", dummyCond.Type(), nextDummyCond.Type())
		}

		nextDummyVal, err := when.Val.TypeCheck(args)
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
func (expr *CastExpr) TypeCheck(args MapArgs) (Datum, error) {
	dummyExpr, err := expr.Expr.TypeCheck(args)
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
func (expr *CoalesceExpr) TypeCheck(args MapArgs) (Datum, error) {
	var dummyArg Datum
	for _, e := range expr.Exprs {
		arg, err := e.TypeCheck(args)
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
func (expr *ComparisonExpr) TypeCheck(args MapArgs) (Datum, error) {
	leftType, err := expr.Left.TypeCheck(args)
	if err != nil {
		return nil, err
	}
	rightType, err := expr.Right.TypeCheck(args)
	if err != nil {
		return nil, err
	}
	d, cmp, err := typeCheckComparisonOp(expr.Operator, leftType, rightType)
	expr.fn = cmp
	return d, err
}

// TypeCheck implements the Expr interface.
func (expr *ExistsExpr) TypeCheck(args MapArgs) (Datum, error) {
	_, err := expr.Subquery.TypeCheck(args)
	if err != nil {
		return nil, err
	}
	return DummyBool, nil
}

// TypeCheck implements the Expr interface.
func (expr *FuncExpr) TypeCheck(args MapArgs) (Datum, error) {
	dummyArgs := make(DTuple, 0, len(expr.Exprs))
	types := make(typeList, 0, len(expr.Exprs))
	for _, e := range expr.Exprs {
		dummyArg, err := e.TypeCheck(args)
		if err != nil {
			return DNull, err
		}
		dummyArgs = append(dummyArgs, dummyArg)
		types = append(types, reflect.TypeOf(dummyArg))
	}

	// Cache is warm and `fn` encodes its return type.
	if expr.fn.returnType != nil {
		datum, err := expr.fn.returnType(dummyArgs)
		if err != nil {
			return nil, fmt.Errorf("%s: %v", expr.Name, err)
		}
		return datum, nil
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
		datum, err := expr.fn.returnType(dummyArgs)
		if err != nil {
			return nil, fmt.Errorf("%s: %v", expr.Name, err)
		}
		return datum, nil
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
func (expr *IfExpr) TypeCheck(args MapArgs) (Datum, error) {
	cond, err := expr.Cond.TypeCheck(args)
	if err != nil {
		return nil, err
	}
	if cond != DNull && cond != DummyBool {
		return nil, fmt.Errorf("IF condition must be a boolean: %s", cond.Type())
	}
	dummyTrue, err := expr.True.TypeCheck(args)
	if err != nil {
		return nil, err
	}
	dummyElse, err := expr.Else.TypeCheck(args)
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
func (expr *IsOfTypeExpr) TypeCheck(args MapArgs) (Datum, error) {
	if _, err := expr.Expr.TypeCheck(args); err != nil {
		return nil, err
	}
	return DummyBool, nil
}

// TypeCheck implements the Expr interface.
func (expr *NotExpr) TypeCheck(args MapArgs) (Datum, error) {
	return typeCheckBooleanExprs("NOT", args, expr.Expr)
}

// TypeCheck implements the Expr interface.
func (expr *NullIfExpr) TypeCheck(args MapArgs) (Datum, error) {
	expr1, err := expr.Expr1.TypeCheck(args)
	if err != nil {
		return nil, err
	}
	expr2, err := expr.Expr2.TypeCheck(args)
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
func (expr *OrExpr) TypeCheck(args MapArgs) (Datum, error) {
	return typeCheckBooleanExprs("OR", args, expr.Left, expr.Right)
}

// TypeCheck implements the Expr interface.
func (expr *QualifiedName) TypeCheck(args MapArgs) (Datum, error) {
	return nil, fmt.Errorf("qualified name \"%s\" not found", expr)
}

// TypeCheck implements the Expr interface.
func (expr *RangeCond) TypeCheck(args MapArgs) (Datum, error) {
	leftType, err := expr.Left.TypeCheck(args)
	if err != nil {
		return nil, err
	}
	fromType, err := expr.From.TypeCheck(args)
	if err != nil {
		return nil, err
	}
	toType, err := expr.To.TypeCheck(args)
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
func (expr *Subquery) TypeCheck(args MapArgs) (Datum, error) {
	// Avoid type checking subqueries. We need the subquery to be expanded in
	// order to do so properly.
	return DNull, nil
}

// TypeCheck implements the Expr interface.
func (expr *UnaryExpr) TypeCheck(args MapArgs) (Datum, error) {
	dummyExpr, err := expr.Expr.TypeCheck(args)
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
func (expr Array) TypeCheck(args MapArgs) (Datum, error) {
	return nil, util.Errorf("unhandled type %T", expr)
}

// TypeCheck implements the Expr interface.
func (expr DefaultVal) TypeCheck(args MapArgs) (Datum, error) {
	return nil, util.Errorf("unhandled type %T", expr)
}

// TypeCheck implements the Expr interface.
func (expr IntVal) TypeCheck(args MapArgs) (Datum, error) {
	return DummyInt, nil
}

// TypeCheck implements the Expr interface.
func (expr NumVal) TypeCheck(args MapArgs) (Datum, error) {
	return DummyFloat, nil
}

// TypeCheck implements the Expr interface.
func (expr Row) TypeCheck(args MapArgs) (Datum, error) {
	return Tuple(expr).TypeCheck(args)
}

// TypeCheck implements the Expr interface.
func (expr Tuple) TypeCheck(args MapArgs) (Datum, error) {
	tuple := make(DTuple, 0, len(expr))
	for _, v := range expr {
		d, err := v.TypeCheck(args)
		if err != nil {
			return nil, err
		}
		tuple = append(tuple, d)
	}
	return tuple, nil
}

// TypeCheck implements the Expr interface.
func (expr ValArg) TypeCheck(args MapArgs) (Datum, error) {
	if args == nil {
		return nil, util.Errorf("unhandled parameter: %s", expr, expr)
	}
	return DValArg(expr.name), nil
}

// TypeCheck implements the Expr interface.
func (expr DBool) TypeCheck(args MapArgs) (Datum, error) {
	return DummyBool, nil
}

// TypeCheck implements the Expr interface.
func (expr DBytes) TypeCheck(args MapArgs) (Datum, error) {
	return DummyBytes, nil
}

// TypeCheck implements the Expr interface.
func (expr DDate) TypeCheck(args MapArgs) (Datum, error) {
	return DummyDate, nil
}

// TypeCheck implements the Expr interface.
func (expr DFloat) TypeCheck(args MapArgs) (Datum, error) {
	return DummyFloat, nil
}

// TypeCheck implements the Expr interface.
func (expr DInt) TypeCheck(args MapArgs) (Datum, error) {
	return DummyInt, nil
}

// TypeCheck implements the Expr interface.
func (expr DInterval) TypeCheck(args MapArgs) (Datum, error) {
	return DummyInterval, nil
}

// TypeCheck implements the Expr interface.
func (expr dNull) TypeCheck(args MapArgs) (Datum, error) {
	return DNull, nil
}

// TypeCheck implements the Expr interface.
func (expr DString) TypeCheck(args MapArgs) (Datum, error) {
	return DummyString, nil
}

// TypeCheck implements the Expr interface.
func (expr DTimestamp) TypeCheck(args MapArgs) (Datum, error) {
	return DummyTimestamp, nil
}

// TypeCheck implements the Expr interface.
func (expr DTuple) TypeCheck(args MapArgs) (Datum, error) {
	tuple := make(DTuple, 0, len(expr))
	for _, v := range expr {
		d, err := v.TypeCheck(args)
		if err != nil {
			return nil, err
		}
		tuple = append(tuple, d)
	}
	return tuple, nil
}

// TypeCheck implements the Expr interface.
func (expr DValArg) TypeCheck(args MapArgs) (Datum, error) {
	return dummyValArg, nil
}

func typeCheckBooleanExprs(op string, args MapArgs, exprs ...Expr) (Datum, error) {
	for _, expr := range exprs {
		dummyExpr, err := expr.TypeCheck(args)
		if err != nil {
			return nil, err
		}
		if dummyExpr == DNull {
			continue
		}
		if args != nil {
			if v, ok := dummyExpr.(DValArg); ok {
				args[string(v)] = DummyBool
				continue
			}
		}
		if _, ok := dummyExpr.(DBool); !ok {
			return nil, fmt.Errorf("incompatible %s argument type: %s", op, dummyExpr.Type())
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
