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
	"go/constant"
	"strings"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/decimal"
)

var (
	// TypeBytes returns a bytes datum.
	TypeBytes = DummyBytes
	// TypeDate returns a date datum.
	TypeDate = DummyDate
	// TypeFloat returns a float datum.
	TypeFloat = DummyFloat
	// TypeDecimal returns a decimal datum.
	TypeDecimal = DummyDecimal
	// TypeInt returns an int datum.
	TypeInt = DummyInt
	// TypeInterval returns an interval datum.
	TypeInterval = DummyInterval
	// TypeString returns a string datum.
	TypeString = DummyString
	// TypeTimestamp returns a timestamp datum.
	TypeTimestamp = DummyTimestamp
)

type parameterTypeAmbiguityError struct {
	v ValArg
}

func (err parameterTypeAmbiguityError) Error() string {
	return fmt.Sprintf("could not determine data type of parameter %s", err.v)
}

func decorateTypeCheckError(err error, format string, a ...interface{}) error {
	if _, ok := err.(parameterTypeAmbiguityError); ok {
		return err
	}
	return fmt.Errorf(format+": %v", append(a, err)...)
}

// TypeCheck implements the Expr interface.
func (expr *AndExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	leftTyped, err := typeCheckBooleanExpr(args, "AND argument", expr.Left)
	if err != nil {
		return nil, err
	}
	rightTyped, err := typeCheckBooleanExpr(args, "AND argument", expr.Right)
	if err != nil {
		return nil, err
	}
	expr.Left, expr.Right = leftTyped, rightTyped
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *BinaryExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	ops := BinOps[expr.Operator]
	overloads := make([]overload, len(ops))
	for i := range ops {
		overloads[i] = ops[i]
	}

	typedSubExprs, fn, err := typeCheckOverloadedExprs(args, desired, overloads, expr.Left, expr.Right)
	if err != nil {
		return nil, err
	}

	leftTyped, rightTyped := typedSubExprs[0], typedSubExprs[1]
	leftReturn := leftTyped.ReturnType()
	rightReturn := rightTyped.ReturnType()
	if leftReturn == DNull || rightReturn == DNull {
		return DNull, nil
	}

	if fn == nil {
		var desStr string
		if desired != nil {
			desStr = fmt.Sprintf(" = <%s>", desired.Type())
		}
		return nil, fmt.Errorf("unsupported binary operator: <%s> %s <%s>%s",
			leftReturn.Type(), expr.Operator, rightReturn.Type(), desStr)
	}
	expr.Left, expr.Right = leftTyped, rightTyped
	expr.fn = fn.(BinOp)
	expr.typeAnnotation.typ = fn.returnType()
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *CaseExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	var err error
	tmpExprs := make([]Expr, 0, len(expr.Whens)+1)
	if expr.Expr != nil {
		tmpExprs = tmpExprs[:0]
		tmpExprs = append(tmpExprs, expr.Expr)
		for _, when := range expr.Whens {
			tmpExprs = append(tmpExprs, when.Cond)
		}

		typedSubExprs, _, err := typeCheckSameTypedExprs(args, nil, tmpExprs...)
		if err != nil {
			return nil, decorateTypeCheckError(err, "incompatible condition type")
		}
		expr.Expr = typedSubExprs[0]
		for i, whenCond := range typedSubExprs[1:] {
			expr.Whens[i].Cond = whenCond
		}
	} else {
		// If expr.Expr is nil, the WHEN clauses contain boolean expressions.
		for i, when := range expr.Whens {
			if expr.Whens[i].Cond, err = typeCheckBooleanExpr(args, "condition", when.Cond); err != nil {
				return nil, err
			}
		}
	}

	tmpExprs = tmpExprs[:0]
	for _, when := range expr.Whens {
		tmpExprs = append(tmpExprs, when.Val)
	}
	if expr.Else != nil {
		tmpExprs = append(tmpExprs, expr.Else)
	}
	typedSubExprs, retType, err := typeCheckSameTypedExprs(args, desired, tmpExprs...)
	if err != nil {
		return nil, decorateTypeCheckError(err, "incompatible value type")
	}
	if expr.Else != nil {
		expr.Else = typedSubExprs[len(typedSubExprs)-1]
		typedSubExprs = typedSubExprs[:len(typedSubExprs)-1]
	}
	for i, whenVal := range typedSubExprs {
		expr.Whens[i].Val = whenVal
	}
	expr.typeAnnotation.typ = retType
	return expr, nil
}

var (
	boolCastTypes      = []Datum{DNull, DummyBool, DummyInt, DummyFloat, DummyDecimal, DummyString}
	intCastTypes       = []Datum{DNull, DummyBool, DummyInt, DummyFloat, DummyDecimal, DummyString}
	floatCastTypes     = []Datum{DNull, DummyBool, DummyInt, DummyFloat, DummyDecimal, DummyString}
	decimalCastTypes   = []Datum{DNull, DummyBool, DummyInt, DummyFloat, DummyDecimal, DummyString}
	stringCastTypes    = []Datum{DNull, DummyBool, DummyInt, DummyFloat, DummyDecimal, DummyString, DummyBytes}
	bytesCastTypes     = []Datum{DNull, DummyBytes, DummyString}
	dateCastTypes      = []Datum{DNull, DummyString, DummyDate, DummyTimestamp}
	timestampCastTypes = []Datum{DNull, DummyString, DummyDate, DummyTimestamp, DummyTimestampTZ}
	intervalCastTypes  = []Datum{DNull, DummyString, DummyInt, DummyInterval}
)

// TypeCheck implements the Expr interface.
func (expr *CastExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
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

	case *TimestampTZType:
		returnDatum = DummyTimestampTZ
		validTypes = timestampCastTypes

	case *IntervalType:
		returnDatum = DummyInterval
		validTypes = intervalCastTypes
	}

	if isUnresolvedArgument(args, expr.Expr) {
		desired = DummyString
	} else if desired != nil && desired.TypeEqual(returnDatum) {
		desired = nil
	}
	typedSubExpr, err := expr.Expr.TypeCheck(args, desired)
	if err != nil {
		return nil, err
	}

	castFrom := typedSubExpr.ReturnType()
	for _, t := range validTypes {
		if castFrom.TypeEqual(t) {
			expr.Expr = typedSubExpr
			expr.typeAnnotation.typ = returnDatum
			return expr, nil
		}
	}

	return nil, fmt.Errorf("invalid cast: %s -> %s", castFrom.Type(), expr.Type)
}

// TypeCheck implements the Expr interface.
func (expr *CoalesceExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	typedSubExprs, retType, err := typeCheckSameTypedExprs(args, desired, expr.Exprs...)
	if err != nil {
		return nil, decorateTypeCheckError(err, fmt.Sprintf("incompatible %s expressions", expr.Name))
	}

	for i, subExpr := range typedSubExprs {
		expr.Exprs[i] = subExpr
	}
	expr.typeAnnotation.typ = retType
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *ComparisonExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	leftTyped, rightTyped, fn, err := typeCheckComparisonOp(args, expr.Operator, expr.Left, expr.Right)
	if err != nil {
		return nil, err
	}
	expr.Left, expr.Right, expr.fn = leftTyped, rightTyped, fn
	return expr, err
}

// TypeCheck implements the Expr interface.
func (expr *ExistsExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	subqueryTyped, err := expr.Subquery.TypeCheck(args, desired)
	if err != nil {
		return nil, err
	}
	expr.Subquery = subqueryTyped
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *FuncExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	if len(expr.Name.Indirect) > 0 {
		// We don't support qualified function names (yet).
		return nil, fmt.Errorf("unknown function: %s", expr.Name)
	}

	name := string(expr.Name.Base)
	candidates, ok := Builtins[strings.ToLower(name)]
	if !ok {
		return nil, fmt.Errorf("unknown function: %s", name)
	}

	overloads := make([]overload, len(candidates))
	for i := range candidates {
		overloads[i] = candidates[i]
	}
	typedSubExprs, fn, err := typeCheckOverloadedExprs(args, desired, overloads, expr.Exprs...)
	if err != nil {
		return nil, fmt.Errorf("%s: %v", name, err)
	} else if fn == nil {
		typeNames := make([]string, 0, len(expr.Exprs))
		for _, expr := range typedSubExprs {
			typeNames = append(typeNames, expr.ReturnType().Type())
		}
		var desStr string
		if desired != nil {
			desStr = fmt.Sprintf(" = <%s>", desired.Type())
		}
		return nil, fmt.Errorf("unknown signature for %s: %s(%s)%s",
			expr.Name, expr.Name, strings.Join(typeNames, ", "), desStr)
	}

	for i, subExpr := range typedSubExprs {
		expr.Exprs[i] = subExpr
	}
	expr.fn = fn.(Builtin)
	returnType := fn.returnType()
	if _, ok = expr.fn.params().(AnyType); ok {
		if len(typedSubExprs) > 0 {
			returnType = typedSubExprs[0].ReturnType()
		} else {
			returnType = DNull
		}
	}
	expr.typeAnnotation.typ = returnType
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *IfExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	var err error
	if expr.Cond, err = typeCheckBooleanExpr(args, "IF condition", expr.Cond); err != nil {
		return nil, err
	}

	typedSubExprs, retType, err := typeCheckSameTypedExprs(args, desired, expr.True, expr.Else)
	if err != nil {
		return nil, decorateTypeCheckError(err, "incompatible IF expressions")
	}

	expr.True, expr.Else = typedSubExprs[0], typedSubExprs[1]
	expr.typeAnnotation.typ = retType
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *IsOfTypeExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	exprTyped, err := expr.Expr.TypeCheck(args, nil)
	if err != nil {
		return nil, err
	}
	expr.Expr = exprTyped
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *NotExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	exprTyped, err := typeCheckBooleanExpr(args, "NOT argument", expr.Expr)
	if err != nil {
		return nil, err
	}
	expr.Expr = exprTyped
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *NullIfExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	typedSubExprs, retType, err := typeCheckSameTypedExprs(args, desired, expr.Expr1, expr.Expr2)
	if err != nil {
		return nil, decorateTypeCheckError(err, "incompatible NULLIF expressions")
	}

	expr.Expr1, expr.Expr2 = typedSubExprs[0], typedSubExprs[1]
	expr.typeAnnotation.typ = retType
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *OrExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	leftTyped, err := typeCheckBooleanExpr(args, "OR argument", expr.Left)
	if err != nil {
		return nil, err
	}
	rightTyped, err := typeCheckBooleanExpr(args, "OR argument", expr.Right)
	if err != nil {
		return nil, err
	}
	expr.Left, expr.Right = leftTyped, rightTyped
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *ParenExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	exprTyped, err := expr.Expr.TypeCheck(args, desired)
	if err != nil {
		return nil, err
	}
	expr.Expr = exprTyped
	expr.typeAnnotation.typ = exprTyped.ReturnType()
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *QualifiedName) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	return nil, fmt.Errorf("qualified name \"%s\" not found", expr)
}

// TypeCheck implements the Expr interface.
func (expr *RangeCond) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	typedSubExprs, _, err := typeCheckSameTypedExprs(args, nil, expr.Left, expr.From, expr.To)
	if err != nil {
		return nil, err
	}

	expr.Left, expr.From, expr.To = typedSubExprs[0], typedSubExprs[1], typedSubExprs[2]
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *Subquery) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	// Avoid type checking subqueries. We need the subquery to be expanded in
	// order to do so properly.
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *UnaryExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	ops := UnaryOps[expr.Operator]
	overloads := make([]overload, len(ops))
	for i := range ops {
		overloads[i] = ops[i]
	}

	typedSubExprs, fn, err := typeCheckOverloadedExprs(args, desired, overloads, expr.Expr)
	if err != nil {
		return nil, err
	}

	exprTyped := typedSubExprs[0]
	exprReturn := exprTyped.ReturnType()
	if exprReturn == DNull {
		return DNull, nil
	}

	if fn == nil {
		var desStr string
		if desired != nil {
			desStr = fmt.Sprintf(" = <%s>", desired.Type())
		}
		return nil, fmt.Errorf("unsupported unary operator: %s <%s>%s",
			expr.Operator, exprReturn.Type(), desStr)
	}
	expr.Expr = exprTyped
	expr.fn = fn.(UnaryOp)
	expr.typeAnnotation.typ = fn.returnType()
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *Array) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	return nil, util.Errorf("unhandled type %T", expr)
}

// TypeCheck implements the Expr interface.
func (expr DefaultVal) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	return nil, util.Errorf("unhandled type %T", expr)
}

// TypeCheck implements the Expr interface.
func (expr *ConstVal) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	if desired != nil && expr.canBecomeType(desired) {
		return expr.resolveAsType(desired)
	}

	natural := expr.naturalType()
	if natural != nil {
		return expr.resolveAsType(natural)
	}
	return nil, fmt.Errorf("could not determine prefered type for ConstVal %v", expr.Value)
}

func (expr *ConstVal) resolveAsType(typ Datum) (TypedExpr, error) {
	switch typ {
	case DummyInt:
		i, exact := constant.Int64Val(constant.ToInt(expr.Value))
		if !exact {
			return nil, fmt.Errorf("integer value out of range: %v", expr.Value)
		}
		return NewDInt(DInt(i)), nil
	case DummyFloat:
		f, _ := constant.Float64Val(constant.ToFloat(expr.Value))
		return NewDFloat(DFloat(f)), nil
	case DummyDecimal:
		dd := &DDecimal{}
		s := expr.ExactString()
		if idx := strings.IndexRune(s, '/'); idx != -1 {
			// Handle constant.ratVal, which will return a rational string
			// like 6/7. If only we could call big.Rat.FloatString() on it...
			num, den := s[:idx], s[idx+1:]
			if _, ok := dd.SetString(num); !ok {
				return nil, fmt.Errorf("could not evaluate numerator of %v as Datum type DDecimal from string %q", expr, num)
			}
			denDec := new(inf.Dec)
			if _, ok := denDec.SetString(den); !ok {
				return nil, fmt.Errorf("could not evaluate denominator %v as Datum type DDecimal from string %q", expr, den)
			}
			dd.QuoRound(&dd.Dec, denDec, decimal.Precision, inf.RoundHalfUp)

			// Get rid of trailing zeros. We probaby want to remove this
			if s = dd.Dec.String(); strings.ContainsRune(s, '.') {
				for {
					switch s[len(s)-1] {
					case '0':
						s = s[:len(s)-1]
						continue
					case '.':
						s = s[:len(s)-1]
					}
					break
				}
				if _, ok := dd.SetString(s); !ok {
					return nil, fmt.Errorf("could not evaluate %v as Datum type DDecimal from string %q", expr, s)
				}
			}
		} else {
			if _, ok := dd.SetString(s); !ok {
				return nil, fmt.Errorf("could not evaluate %v as Datum type DDecimal from string %q", expr, s)
			}
		}
		return dd, nil
	default:
		return nil, fmt.Errorf("could not evaluate ConstVal %v into a %T", expr, typ)
	}
}

var constTypePreference = []Datum{DummyInt, DummyFloat, DummyDecimal}

var intFloatDecimalAvailable = map[Datum]struct{}{
	DummyInt:     {},
	DummyFloat:   {},
	DummyDecimal: {},
}

// var intDecimalAvailable = map[Datum]struct{}{
// 	DummyInt:     {},
// 	DummyDecimal: {},
// }
var floatDecimalAvailable = map[Datum]struct{}{
	DummyFloat:   {},
	DummyDecimal: {},
}
var decimalAvailable = map[Datum]struct{}{
	DummyDecimal: {},
}

func (expr *ConstVal) canBecomeType(desired Datum) bool {
	switch desired {
	case DummyInt:
		return expr.canBeInt64()
	case DummyFloat:
		// TODO(nvanbenschoten) float overflow?
		return true
	case DummyDecimal:
		return true
	default:
		return false
	}
}

func (expr *ConstVal) availableTypes() map[Datum]struct{} {
	switch {
	case expr.shouldBeInt64():
		return intFloatDecimalAvailable
	case expr.Kind() == constant.Float:
		return floatDecimalAvailable
	default:
		return decimalAvailable
	}
}

func (expr *ConstVal) naturalType() Datum {
	available := expr.availableTypes()
	for _, t := range constTypePreference {
		if _, ok := available[t]; ok {
			return t
		}
	}
	return nil
}

func typeCheckExprs(args MapArgs, exprs []Expr, desired Datum) ([]Expr, *DTuple, error) {
	types := make(DTuple, 0, len(exprs))
	for i, expr := range exprs {
		var desiredElem Datum
		if t, ok := desired.(*DTuple); ok && len(*t) > i {
			desiredElem = (*t)[i]
		}
		var err error
		exprs[i], err = expr.TypeCheck(args, desiredElem)
		if err != nil {
			return nil, nil, err
		}
		types = append(types, exprs[i].(TypedExpr).ReturnType())
	}
	return exprs, &types, nil
}

// TypeCheck implements the Expr interface.
func (expr *Row) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	typedExprs, types, err := typeCheckExprs(args, expr.Exprs, desired)
	if err != nil {
		return nil, err
	}
	expr.Exprs, expr.types = typedExprs, types
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *Tuple) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	typedExprs, types, err := typeCheckExprs(args, expr.Exprs, desired)
	if err != nil {
		return nil, err
	}
	expr.Exprs, expr.types = typedExprs, types
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr ValArg) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	dVal := &DValArg{name: expr.name}
	if v, ok := args[expr.name]; ok {
		dVal.typeAnnotation.typ = v
		return dVal, nil
	}
	if desired == nil || desired == DNull {
		return nil, parameterTypeAmbiguityError{expr}
	}
	if _, err := args.SetInferredType(dVal, desired); err != nil {
		return nil, err
	}
	dVal.typeAnnotation.typ = desired
	return dVal, nil
}

func typeCheckBooleanExpr(args MapArgs, op string, expr Expr) (TypedExpr, error) {
	typedExpr, err := expr.TypeCheck(args, DummyBool)
	if err != nil {
		return nil, err
	}
	if typ := typedExpr.ReturnType(); !(typ == DNull || typ.TypeEqual(DummyBool)) {
		return nil, fmt.Errorf("incompatible %s type: %s", op, typ.Type())
	}
	return typedExpr, nil
}

func typeCheckComparisonOp(args MapArgs, op ComparisonOp, left, right Expr) (TypedExpr, TypedExpr, CmpOp, error) {
	foldedOp, foldedLeft, foldedRight, switched, _ := foldComparisonExpr(op, left, right)

	ops := CmpOps[foldedOp]
	overloads := make([]overload, len(ops))
	for i := range ops {
		overloads[i] = ops[i]
	}

	typedSubExprs, fn, err := typeCheckOverloadedExprs(args, nil, overloads, foldedLeft, foldedRight)
	if err != nil {
		return nil, nil, CmpOp{}, err
	}

	leftExpr, rightExpr := typedSubExprs[0], typedSubExprs[1]
	leftReturn := leftExpr.(TypedExpr).ReturnType()
	rightReturn := rightExpr.(TypedExpr).ReturnType()
	if switched {
		leftExpr, rightExpr = rightExpr, leftExpr
	}
	if leftReturn == DNull || rightReturn == DNull {
		switch op {
		case Is, IsNot, IsDistinctFrom, IsNotDistinctFrom:
			// TODO(pmattis): For IS {UNKNOWN,TRUE,FALSE} we should be requiring that
			// dummyLeft.TypeEquals(DummyBool). We currently can't distinguish NULL from
			// UNKNOWN. Is it important to do so?
			return leftExpr, rightExpr, CmpOp{}, nil
		default:
			// TODO NULL?
			return leftExpr, rightExpr, CmpOp{}, nil
		}
	}

	if fn == nil {
		return nil, nil, CmpOp{}, fmt.Errorf("unsupported comparison operator: <%s> %s <%s>",
			leftReturn.Type(), op, rightReturn.Type())
	}

	cmpOp := fn.(CmpOp)
	if op == In && cmpOp.RightType.TypeEqual(dummyTuple) {
		if err := verifyTupleIN(args, leftReturn, rightReturn); err != nil {
			return nil, nil, CmpOp{}, err
		}
	} else if cmpOp.LeftType.TypeEqual(dummyTuple) && cmpOp.RightType.TypeEqual(dummyTuple) {
		if err := verifyTupleCmp(args, leftReturn, rightReturn); err != nil {
			return nil, nil, CmpOp{}, err
		}
	}

	return leftExpr, rightExpr, cmpOp, nil
}

func verifyTupleCmp(args MapArgs, leftTupleType, rightTupleType Datum) error {
	lTuple := *leftTupleType.(*DTuple)
	rTuple := *rightTupleType.(*DTuple)
	if len(lTuple) != len(rTuple) {
		return fmt.Errorf("unequal number of entries in tuple expressions: %d, %d", len(lTuple), len(rTuple))
	}

	for i := range lTuple {
		if _, _, _, err := typeCheckComparisonOp(args, EQ, lTuple[i], rTuple[i]); err != nil {
			return err
		}
	}

	return nil
}

func verifyTupleIN(args MapArgs, arg, values Datum) error {
	if arg == DNull {
		return nil
	}

	vtuple := *values.(*DTuple)
	for _, val := range vtuple {
		if _, _, _, err := typeCheckComparisonOp(args, EQ, arg, val); err != nil {
			return err
		}
	}

	return nil
}

func isUnresolvedConstVal(expr Expr) bool {
	// TODO(nvanbenschoten) move to const.go
	if _, ok := expr.(*ConstVal); ok {
		return true
	}
	return false
}

func isUnresolvedArgument(args MapArgs, expr Expr) bool {
	if t, ok := expr.(ValArg); ok {
		if _, ok := args[t.name]; !ok {
			return true
		}
	}
	return false
}

func isUnresolvedVariable(args MapArgs, expr Expr) bool {
	// TODO(nvanbenschoten) move to expr.go
	if t, ok := expr.(ValArg); ok {
		_, ok := args[t.name]
		return !ok
	}
	if _, ok := expr.(*QualifiedName); ok {
		return true
	}
	return false
}

type indexedExpr struct {
	e Expr
	i int
}

// typeCheckSameTypedExprs ... TODO
func typeCheckSameTypedExprs(args MapArgs, desired Datum, exprs ...Expr) ([]TypedExpr, Datum, error) {
	switch len(exprs) {
	case 0:
		return nil, nil, nil
	case 1:
		typedExpr, err := exprs[0].TypeCheck(args, desired)
		if err != nil {
			return nil, nil, err
		}
		return []TypedExpr{typedExpr}, typedExpr.ReturnType(), nil
	}

	// Hold the resolved type expressions of the provided exprs, in order.
	typedExprs := make([]TypedExpr, len(exprs))

	// Split the expressions into three groups of indexed expressions.
	var resolvedExprs, constExprs, valExprs []indexedExpr
	for i, expr := range exprs {
		idxExpr := indexedExpr{e: expr, i: i}
		switch {
		case isUnresolvedConstVal(expr):
			constExprs = append(constExprs, idxExpr)
		case isUnresolvedVariable(args, expr):
			valExprs = append(valExprs, idxExpr)
		default:
			resolvedExprs = append(resolvedExprs, idxExpr)
		}
	}

	// Used to set ValArgs to the desired typ. If the typ is not provided or is
	// null, an error will be thrown.
	typeCheckSameTypedArgs := func(typ Datum) error {
		for _, valExpr := range valExprs {
			typedExpr, err := valExpr.e.TypeCheck(args, typ)
			if err != nil {
				return err
			}
			typedExprs[valExpr.i] = typedExpr
		}
		return nil
	}

	// Used to type check constants to the same type. An optional typ can be
	// provided to signify the desired shared type, which can be set to the
	// required shared type using the second parameter.
	typeCheckSameTypedConsts := func(typ Datum, required bool) (Datum, error) {
		setTypeForConsts := func(typ Datum) error {
			for _, constVal := range constExprs {
				typedExpr, err := constVal.e.TypeCheck(args, typ)
				if err != nil {
					return err
				}
				typedExprs[constVal.i] = typedExpr
			}
			return nil
		}

		// If typ is not nil, all consts try to become typ.
		if typ != nil {
			all := true
			for _, constVal := range constExprs {
				if !constVal.e.(*ConstVal).canBecomeType(typ) {
					if required {
						typedExpr, err := constVal.e.TypeCheck(args, nil)
						if err != nil {
							return nil, err
						}
						return nil, fmt.Errorf("expected %s to be of type %s, found type %s", constVal.e, typ.Type(), typedExpr.ReturnType().Type())
					}
					all = false
					break
				}
			}
			if all {
				return typ, setTypeForConsts(typ)
			}
		}

		// If all consts could not become typ, use their best shared type.
		constValExprs := make([]*ConstVal, len(constExprs))
		for i, constVal := range constExprs {
			constValExprs[i] = constVal.e.(*ConstVal)
		}
		bestType := commonConstantType(constValExprs...)
		return bestType, setTypeForConsts(bestType)
	}

	// Used to type check all constants with the optional desired type. The
	// type that is chosen here will then be set to any arguments.
	typeCheckConstsAndArgsWithDesired := func() ([]TypedExpr, Datum, error) {
		typ, err := typeCheckSameTypedConsts(desired, false)
		if err != nil {
			return nil, nil, err
		}
		if len(constExprs) > 0 {
			if err := typeCheckSameTypedArgs(typ); err != nil {
				return nil, nil, err
			}
		}
		return typedExprs, typ, nil
	}

	switch {
	case len(resolvedExprs) == 0 && len(constExprs) == 0:
		if err := typeCheckSameTypedArgs(desired); err != nil {
			return nil, nil, err
		}
		return typedExprs, desired, nil
	case len(resolvedExprs) == 0:
		return typeCheckConstsAndArgsWithDesired()
	default:
		firstValidIdx := -1
		firstValidType := DNull
		for i, resExpr := range resolvedExprs {
			typedExpr, err := resExpr.e.TypeCheck(args, desired)
			if err != nil {
				return nil, nil, err
			}
			typedExprs[resExpr.i] = typedExpr
			if returnType := typedExpr.ReturnType(); returnType != DNull {
				firstValidType = returnType
				firstValidIdx = i
				break
			}
		}

		if firstValidType == DNull {
			switch {
			case len(constExprs) > 0:
				return typeCheckConstsAndArgsWithDesired()
			case len(valExprs) > 0:
				err := typeCheckSameTypedArgs(nil)
				return nil, nil, err
			default:
				return typedExprs, DNull, nil
			}
		}

		for _, resExpr := range resolvedExprs[firstValidIdx+1:] {
			typedExpr, err := resExpr.e.TypeCheck(args, firstValidType)
			if err != nil {
				return nil, nil, err
			}
			typedExprs[resExpr.i] = typedExpr
			if returnType := typedExpr.ReturnType(); !(returnType == DNull || returnType.TypeEqual(firstValidType)) {
				return nil, nil, fmt.Errorf("expected %s to be of type %s, found type %s", resExpr.e, firstValidType.Type(), returnType.Type())
			}
		}
		if len(constExprs) > 0 {
			if _, err := typeCheckSameTypedConsts(firstValidType, true); err != nil {
				return nil, nil, err
			}
		}
		if len(valExprs) > 0 {
			if err := typeCheckSameTypedArgs(firstValidType); err != nil {
				return nil, nil, err
			}
		}
		return typedExprs, firstValidType, nil
	}
}

func commonConstantType(vals ...*ConstVal) Datum {
	bestType := 0
	for _, c := range vals {
		avail := c.availableTypes()
		for {
			// This will not work if the available types are not strictly
			// supersets of their previous types in order of preference.
			if _, ok := avail[constTypePreference[bestType]]; ok {
				break
			}
			bestType++
			if bestType == len(constTypePreference)-1 {
				return constTypePreference[bestType]
			}
		}
	}
	return constTypePreference[bestType]
}
