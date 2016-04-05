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
// permissions and limitations under the License.
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
	// TypeBytes returns a bytes datum.
	TypeBytes = func(MapArgs, DTuple) (Datum, error) { return DummyBytes, nil }
	// TypeDate returns a date datum.
	TypeDate = func(MapArgs, DTuple) (Datum, error) { return DummyDate, nil }
	// TypeFloat returns a float datum.
	TypeFloat = func(MapArgs, DTuple) (Datum, error) { return DummyFloat, nil }
	// TypeDecimal returns a decimal datum.
	TypeDecimal = func(MapArgs, DTuple) (Datum, error) { return DummyDecimal, nil }
	// TypeInt returns an int datum.
	TypeInt = func(MapArgs, DTuple) (Datum, error) { return DummyInt, nil }
	// TypeInterval returns an interval datum.
	TypeInterval = func(MapArgs, DTuple) (Datum, error) { return DummyInterval, nil }
	// TypeString returns a string datum.
	TypeString = func(MapArgs, DTuple) (Datum, error) { return DummyString, nil }
	// TypeTimestamp returns a timestamp datum.
	TypeTimestamp = func(MapArgs, DTuple) (Datum, error) { return DummyTimestamp, nil }
)

// TypeCheck implements the Expr interface.
func (expr *AndExpr) TypeCheck(args MapArgs) (Datum, error) {
	return typeCheckBooleanExprs(args, "AND", expr.Left, expr.Right)
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

	if expr.ltype == valargType {
		if _, err := args.SetInferredType(dummyLeft, dummyRight); err != nil {
			return nil, err
		}
		expr.ltype = expr.rtype
	} else if expr.rtype == valargType {
		if _, err := args.SetInferredType(dummyRight, dummyLeft); err != nil {
			return nil, err
		}
		expr.rtype = expr.ltype
	}

	var ok bool
	if expr.fn, ok = BinOps[BinArgs{expr.Operator, expr.ltype, expr.rtype}]; ok {
		return expr.fn.ReturnType, nil
	}

	return nil, fmt.Errorf("unsupported binary operator: <%s> %s <%s>",
		dummyLeft.Type(), expr.Operator, dummyRight.Type())
}

// TypeCheck implements the Expr interface.
func (expr *CaseExpr) TypeCheck(args MapArgs) (Datum, error) {
	var dummyCond Datum

	// If expr.Expr is nil, the WHEN clauses contain boolean expressions.
	condArgType := DummyBool
	if expr.Expr != nil {
		var err error
		dummyCond, err = expr.Expr.TypeCheck(args)
		if err != nil {
			return nil, err
		}
		if _, ok := dummyCond.(DValArg); ok {
			return nil, fmt.Errorf("could not determine data type of parameter %s", dummyCond)
		}
		condArgType = dummyCond
	}

	dummyVal := DNull
	if expr.Else != nil {
		var err error
		dummyVal, err = expr.Else.TypeCheck(args)
		if err != nil {
			return nil, err
		}
		if _, ok := dummyVal.(DValArg); ok {
			return nil, fmt.Errorf("could not determine data type of parameter %s", dummyVal)
		}
	}

	for _, when := range expr.Whens {
		nextDummyCond, err := when.Cond.TypeCheck(args)
		if err != nil {
			return nil, err
		}
		if set, err := args.SetInferredType(nextDummyCond, condArgType); err != nil {
			return nil, err
		} else if set != nil {
			nextDummyCond = set
		}
		if dummyCond == nil || dummyCond == DNull {
			dummyCond = nextDummyCond
		} else if !(nextDummyCond == DNull || nextDummyCond.TypeEqual(dummyCond)) {
			return nil, fmt.Errorf("incompatible condition types %s, %s", dummyCond.Type(), nextDummyCond.Type())
		}

		nextDummyVal, err := when.Val.TypeCheck(args)
		if err != nil {
			return nil, err
		}
		if _, ok := nextDummyVal.(DValArg); ok && dummyVal == DNull {
			return nil, fmt.Errorf("could not determine data type of parameter %s", nextDummyVal)
		}
		if set, err := args.SetInferredType(nextDummyVal, dummyVal); err != nil {
			return nil, err
		} else if set != nil {
			nextDummyVal = set
		}
		if dummyVal == DNull {
			dummyVal = nextDummyVal
		} else if !(nextDummyVal == DNull || nextDummyVal.TypeEqual(dummyVal)) {
			return nil, fmt.Errorf("incompatible value types %s, %s", dummyVal.Type(), nextDummyVal.Type())
		}
	}

	return dummyVal, nil
}

var (
	boolCastTypes      = []Datum{DNull, DummyBool, DummyInt, DummyFloat, DummyDecimal, DummyString}
	intCastTypes       = []Datum{DNull, DummyBool, DummyInt, DummyFloat, DummyDecimal, DummyString}
	floatCastTypes     = []Datum{DNull, DummyBool, DummyInt, DummyFloat, DummyDecimal, DummyString}
	decimalCastTypes   = []Datum{DNull, DummyBool, DummyInt, DummyFloat, DummyDecimal, DummyString}
	stringCastTypes    = []Datum{DNull, DummyBool, DummyInt, DummyFloat, DummyDecimal, DummyString, DummyBytes}
	bytesCastTypes     = []Datum{DNull, DummyBytes, DummyString}
	dateCastTypes      = []Datum{DNull, DummyString, DummyTimestamp}
	timestampCastTypes = []Datum{DNull, DummyString, DummyDate}
	intervalCastTypes  = []Datum{DNull, DummyString, DummyInt}
)

// TypeCheck implements the Expr interface.
func (expr *CastExpr) TypeCheck(args MapArgs) (Datum, error) {
	dummyExpr, err := expr.Expr.TypeCheck(args)
	if err != nil {
		return nil, err
	}

	if set, err := args.SetInferredType(dummyExpr, DummyString); err != nil {
		return nil, err
	} else if set != nil {
		dummyExpr = DummyString
	}

	var returnDatum Datum
	var validTypes []Datum
	switch expr.Type.(type) {
	case *BoolType:
		returnDatum = DummyBool
		validTypes = boolCastTypes

	case *IntType:
		returnDatum = DummyInt
		validTypes = intCastTypes

	case *FloatType:
		returnDatum = DummyFloat
		validTypes = floatCastTypes

	case *DecimalType:
		returnDatum = DummyDecimal
		validTypes = decimalCastTypes

	case *StringType:
		returnDatum = DummyString
		validTypes = stringCastTypes

	case *BytesType:
		returnDatum = DummyBytes
		validTypes = bytesCastTypes

	case *DateType:
		returnDatum = DummyDate
		validTypes = dateCastTypes

	case *TimestampType:
		returnDatum = DummyTimestamp
		validTypes = timestampCastTypes

	case *IntervalType:
		returnDatum = DummyInterval
		validTypes = intervalCastTypes
	}

	for _, t := range validTypes {
		if dummyExpr.TypeEqual(t) {
			return returnDatum, nil
		}
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
		} else if !(dummyArg.TypeEqual(arg) || arg == DNull) {
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
	d, cmp, err := typeCheckComparisonOp(args, expr.Operator, leftType, rightType)
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
	types := make(ArgTypes, 0, len(expr.Exprs))
	for _, e := range expr.Exprs {
		dummyArg, err := e.TypeCheck(args)
		if err != nil {
			return DNull, err
		}
		dummyArgs = append(dummyArgs, dummyArg)
		types = append(types, reflect.TypeOf(dummyArg))
	}

	// Cache is warm and `fn` encodes its return type.
	if expr.fn.ReturnType != nil {
		datum, err := expr.fn.ReturnType(args, dummyArgs)
		if err != nil {
			return nil, fmt.Errorf("%s: %v", expr.Name, err)
		}
		return datum, nil
	}

	// Cache is cold, do the lookup.
	if !expr.fnFound {
		if len(expr.Name.Indirect) > 0 {
			// We don't support qualified function names (yet).
			return nil, fmt.Errorf("unknown function: %s", expr.Name)
		}

		name := string(expr.Name.Base)
		candidates, ok := Builtins[strings.ToLower(name)]
		if !ok {
			return nil, fmt.Errorf("unknown function: %s", name)
		}

		for _, candidate := range candidates {
			if candidate.Types.match(types) {
				expr.fn = candidate
				expr.fnFound = true
				break
			}
		}

		// Function lookup failed.
		if !expr.fnFound {
			typeNames := make([]string, 0, len(dummyArgs))
			for _, dummyArg := range dummyArgs {
				typeNames = append(typeNames, dummyArg.Type())
			}
			return nil, fmt.Errorf("unknown signature for %s: %s(%s)",
				expr.Name, expr.Name, strings.Join(typeNames, ", "))
		}
	}

	// Function lookup succeeded and `fn` encodes its return type.
	if expr.fn.ReturnType != nil {
		datum, err := expr.fn.ReturnType(args, dummyArgs)
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
	if !(cond == DNull || cond.TypeEqual(DummyBool)) {
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
	if !dummyTrue.TypeEqual(dummyElse) {
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
	return typeCheckBooleanExprs(args, "NOT", expr.Expr)
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
	if !(expr2 == DNull || expr1.TypeEqual(expr2)) {
		return nil, fmt.Errorf("incompatible NULLIF expressions %s, %s", expr1.Type(), expr2.Type())
	}
	return expr1, nil
}

// TypeCheck implements the Expr interface.
func (expr *OrExpr) TypeCheck(args MapArgs) (Datum, error) {
	return typeCheckBooleanExprs(args, "OR", expr.Left, expr.Right)
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

	if _, _, err := typeCheckComparisonOp(args, GT, leftType, fromType); err != nil {
		return nil, err
	}
	if _, _, err := typeCheckComparisonOp(args, LT, leftType, toType); err != nil {
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
	if expr.fn, ok = UnaryOps[UnaryArgs{expr.Operator, expr.dtype}]; ok {
		return expr.fn.ReturnType, nil
	}

	return nil, fmt.Errorf("unsupported unary operator: %s <%s>",
		expr.Operator, dummyExpr.Type())
}

// TypeCheck implements the Expr interface.
func (expr *Array) TypeCheck(args MapArgs) (Datum, error) {
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

func typeCheckExprs(args MapArgs, exprs []Expr) (Datum, error) {
	tuple := make(DTuple, 0, len(exprs))
	for _, v := range exprs {
		d, err := v.TypeCheck(args)
		if err != nil {
			return nil, err
		}
		tuple = append(tuple, d)
	}
	return tuple, nil
}

// TypeCheck implements the Expr interface.
func (expr *Row) TypeCheck(args MapArgs) (Datum, error) {
	return typeCheckExprs(args, expr.Exprs)
}

// TypeCheck implements the Expr interface.
func (expr *Tuple) TypeCheck(args MapArgs) (Datum, error) {
	return typeCheckExprs(args, expr.Exprs)
}

// TypeCheck implements the Expr interface.
func (expr ValArg) TypeCheck(args MapArgs) (Datum, error) {
	if v, ok := args[expr.name]; ok {
		return v, nil
	}
	return DValArg{name: expr.name}, nil
}

// TypeCheck implements the Expr interface.
func (expr DValArg) TypeCheck(args MapArgs) (Datum, error) {
	return nil, util.Errorf("unhandled type %T", expr)
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
func (expr DDecimal) TypeCheck(args MapArgs) (Datum, error) {
	return DummyDecimal, nil
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

func typeCheckBooleanExprs(args MapArgs, op string, exprs ...Expr) (Datum, error) {
	for _, expr := range exprs {
		dummyExpr, err := expr.TypeCheck(args)
		if err != nil {
			return nil, err
		}
		if dummyExpr == DNull {
			continue
		}
		if set, err := args.SetInferredType(dummyExpr, DummyBool); err != nil {
			return nil, err
		} else if set != nil {
			continue
		}
		if !dummyExpr.TypeEqual(DummyBool) {
			return nil, fmt.Errorf("incompatible %s argument type: %s", op, dummyExpr.Type())
		}
	}
	return DummyBool, nil
}

func typeCheckComparisonOp(args MapArgs, op ComparisonOp, dummyLeft, dummyRight Datum) (Datum, CmpOp, error) {
	if set, err := args.SetInferredType(dummyLeft, dummyRight); err != nil {
		return nil, CmpOp{}, err
	} else if set != nil {
		dummyLeft = set
	} else if set, err := args.SetInferredType(dummyRight, dummyLeft); err != nil {
		return nil, CmpOp{}, err
	} else if set != nil {
		dummyRight = set
	}

	if dummyLeft == DNull || dummyRight == DNull {
		switch op {
		case Is, IsNot, IsDistinctFrom, IsNotDistinctFrom:
			// TODO(pmattis): For IS {UNKNOWN,TRUE,FALSE} we should be requiring that
			// dummyLeft.TypeEquals(DummyBool). We currently can't distinguish NULL from
			// UNKNOWN. Is it important to do so?
			return DummyBool, CmpOp{}, nil
		default:
			return DNull, CmpOp{}, nil
		}
	}
	op, dummyLeft, dummyRight, _ = foldComparisonExpr(op, dummyLeft, dummyRight)
	lType := reflect.TypeOf(dummyLeft)
	rType := reflect.TypeOf(dummyRight)

	if cmp, ok := CmpOps[CmpArgs{op, lType, rType}]; ok {
		if op == EQ && lType == tupleType && rType == tupleType {
			if err := typeCheckTupleEQ(args, dummyLeft, dummyRight); err != nil {
				return nil, CmpOp{}, err
			}
		} else if op == In && rType == tupleType {
			if err := typeCheckTupleIN(args, dummyLeft, dummyRight); err != nil {
				return nil, CmpOp{}, err
			}
		}

		return cmpOpResultType, cmp, nil
	}

	return nil, CmpOp{}, fmt.Errorf("unsupported comparison operator: <%s> %s <%s>",
		dummyLeft.Type(), op, dummyRight.Type())
}

func typeCheckTupleEQ(args MapArgs, lDummy, rDummy Datum) error {
	lTuple := lDummy.(DTuple)
	rTuple := rDummy.(DTuple)
	if len(lTuple) != len(rTuple) {
		return fmt.Errorf("unequal number of entries in tuple expressions: %d, %d", len(lTuple), len(rTuple))
	}

	for i := range lTuple {
		if _, _, err := typeCheckComparisonOp(args, EQ, lTuple[i], rTuple[i]); err != nil {
			return err
		}
	}

	return nil
}

func typeCheckTupleIN(args MapArgs, arg, values Datum) error {
	if arg == DNull {
		return nil
	}

	vtuple := values.(DTuple)
	for _, val := range vtuple {
		if _, _, err := typeCheckComparisonOp(args, EQ, arg, val); err != nil {
			return err
		}
	}

	return nil
}
