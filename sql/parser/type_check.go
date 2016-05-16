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
	"strings"

	"github.com/cockroachdb/cockroach/util"
)

var (
	// TypeBool is the type of a DBool.
	TypeBool Datum = DBoolFalse
	// TypeInt is the type of a DInt.
	TypeInt Datum = NewDInt(0)
	// TypeFloat is the type of a DFloat.
	TypeFloat Datum = NewDFloat(0)
	// TypeDecimal is the type of a DDecimal.
	TypeDecimal Datum = &DDecimal{}
	// TypeString is the type of a DString.
	TypeString Datum = NewDString("")
	// TypeBytes is the type of a DBytes.
	TypeBytes Datum = NewDBytes("")
	// TypeDate is the type of a DDate.
	TypeDate Datum = NewDDate(0)
	// TypeTimestamp is the type of a DTimestamp.
	TypeTimestamp Datum = &DTimestamp{}
	// TypeTimestampTZ is the type of a DTimestamp.
	TypeTimestampTZ Datum = &DTimestampTZ{}
	// TypeInterval is the type of a DInterval.
	TypeInterval Datum = &DInterval{}
	// TypeTuple is the type of a DTuple.
	TypeTuple Datum = &DTuple{}
)

type parameterTypeAmbiguityError struct {
	v ValArg
}

func (err parameterTypeAmbiguityError) Error() string {
	return fmt.Sprintf("could not determine data type of parameter %s", err.v)
}

type unexpectedTypeError struct {
	expr      Expr
	want, got Datum
}

func (err unexpectedTypeError) Error() string {
	return fmt.Sprintf("expected %s to be of type %s, found type %s",
		err.expr, err.want.Type(), err.got.Type())
}

func decorateTypeCheckError(err error, format string, a ...interface{}) error {
	if _, ok := err.(parameterTypeAmbiguityError); ok {
		return err
	}
	return fmt.Errorf(format+": %v", append(a, err)...)
}

// TypeCheck implements the Expr interface.
func (expr *AndExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	leftTyped, err := typeCheckAndRequireBoolean(args, expr.Left, "AND argument")
	if err != nil {
		return nil, err
	}
	rightTyped, err := typeCheckAndRequireBoolean(args, expr.Right, "AND argument")
	if err != nil {
		return nil, err
	}
	expr.Left, expr.Right = leftTyped, rightTyped
	expr.typ = TypeBool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *BinaryExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	ops := BinOps[expr.Operator]
	overloads := make([]overloadImpl, len(ops))
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
			desStr = fmt.Sprintf(" (desired <%s>)", desired.Type())
		}
		return nil, fmt.Errorf("unsupported binary operator: <%s> %s <%s>%s",
			leftReturn.Type(), expr.Operator, rightReturn.Type(), desStr)
	}
	expr.Left, expr.Right = leftTyped, rightTyped
	expr.fn = fn.(BinOp)
	expr.typ = fn.returnType()
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
			if expr.Whens[i].Cond, err = typeCheckAndRequireBoolean(args, when.Cond, "condition"); err != nil {
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
	expr.typ = retType
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *CastExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	returnDatum, validTypes := expr.castTypeAndValidArgTypes()

	desired = nil
	if args.IsUnresolvedArgument(expr.Expr) {
		desired = TypeString
	}
	typedSubExpr, err := expr.Expr.TypeCheck(args, desired)
	if err != nil {
		return nil, err
	}

	castFrom := typedSubExpr.ReturnType()
	for _, t := range validTypes {
		if castFrom.TypeEqual(t) {
			expr.Expr = typedSubExpr
			expr.typ = returnDatum
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
	expr.typ = retType
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *ComparisonExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	leftTyped, rightTyped, fn, err := typeCheckComparisonOp(args, expr.Operator, expr.Left, expr.Right)
	if err != nil {
		return nil, err
	}
	expr.Left, expr.Right = leftTyped, rightTyped
	expr.fn = fn
	expr.typ = TypeBool
	return expr, err
}

// TypeCheck implements the Expr interface.
func (expr *ExistsExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	subqueryTyped, err := expr.Subquery.TypeCheck(args, NoTypePreference)
	if err != nil {
		return nil, err
	}
	expr.Subquery = subqueryTyped
	expr.typ = TypeBool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *FuncExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	if len(expr.Name.Indirect) > 0 {
		// We don't support qualified function names (yet).
		return nil, fmt.Errorf("unknown function: %s", expr.Name)
	}

	name := string(expr.Name.Base)
	// Optimize for the case where name is already normalized to upper/lower
	// case. Note that the Builtins map contains duplicate entries for
	// upper/lower case names.
	candidates, ok := Builtins[name]
	if !ok {
		candidates, ok = Builtins[strings.ToLower(name)]
		if !ok {
			return nil, fmt.Errorf("unknown function: %s", name)
		}
	}

	overloads := make([]overloadImpl, len(candidates))
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
			desStr = fmt.Sprintf(" (desired <%s>)", desired.Type())
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
	expr.typ = returnType
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *IfExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	var err error
	if expr.Cond, err = typeCheckAndRequireBoolean(args, expr.Cond, "IF condition"); err != nil {
		return nil, err
	}

	typedSubExprs, retType, err := typeCheckSameTypedExprs(args, desired, expr.True, expr.Else)
	if err != nil {
		return nil, decorateTypeCheckError(err, "incompatible IF expressions")
	}

	expr.True, expr.Else = typedSubExprs[0], typedSubExprs[1]
	expr.typ = retType
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *IsOfTypeExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	exprTyped, err := expr.Expr.TypeCheck(args, NoTypePreference)
	if err != nil {
		return nil, err
	}
	expr.Expr = exprTyped
	expr.typ = TypeBool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *NotExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	exprTyped, err := typeCheckAndRequireBoolean(args, expr.Expr, "NOT argument")
	if err != nil {
		return nil, err
	}
	expr.Expr = exprTyped
	expr.typ = TypeBool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *NullIfExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	typedSubExprs, retType, err := typeCheckSameTypedExprs(args, desired, expr.Expr1, expr.Expr2)
	if err != nil {
		return nil, decorateTypeCheckError(err, "incompatible NULLIF expressions")
	}

	expr.Expr1, expr.Expr2 = typedSubExprs[0], typedSubExprs[1]
	expr.typ = retType
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *OrExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	leftTyped, err := typeCheckAndRequireBoolean(args, expr.Left, "OR argument")
	if err != nil {
		return nil, err
	}
	rightTyped, err := typeCheckAndRequireBoolean(args, expr.Right, "OR argument")
	if err != nil {
		return nil, err
	}
	expr.Left, expr.Right = leftTyped, rightTyped
	expr.typ = TypeBool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *ParenExpr) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	exprTyped, err := expr.Expr.TypeCheck(args, desired)
	if err != nil {
		return nil, err
	}
	expr.Expr = exprTyped
	expr.typ = exprTyped.ReturnType()
	return expr, nil
}

// qualifiedNameTypes is a mapping of qualified names to types that can be mocked out
// for tests to allow the qualified names to be type checked without throwing an error.
var qualifiedNameTypes map[string]Datum

func mockQualifiedNameTypes(types map[string]Datum) func() {
	qualifiedNameTypes = types
	return func() {
		qualifiedNameTypes = nil
	}
}

// TypeCheck implements the Expr interface.
func (expr *QualifiedName) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	name := expr.String()
	if _, ok := qualifiedNameTypes[name]; ok {
		return expr, nil
	}
	return nil, fmt.Errorf("qualified name \"%s\" not found", name)
}

// TypeCheck implements the Expr interface.
func (expr *RangeCond) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	typedSubExprs, _, err := typeCheckSameTypedExprs(args, nil, expr.Left, expr.From, expr.To)
	if err != nil {
		return nil, err
	}

	expr.Left, expr.From, expr.To = typedSubExprs[0], typedSubExprs[1], typedSubExprs[2]
	expr.typ = TypeBool
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
	overloads := make([]overloadImpl, len(ops))
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
			desStr = fmt.Sprintf(" (desired <%s>)", desired.Type())
		}
		return nil, fmt.Errorf("unsupported unary operator: %s <%s>%s",
			expr.Operator, exprReturn.Type(), desStr)
	}
	expr.Expr = exprTyped
	expr.fn = fn.(UnaryOp)
	expr.typ = fn.returnType()
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
func (expr *NumVal) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	return typeCheckConstant(expr, desired)
}

// TypeCheck implements the Expr interface.
func (expr *StrVal) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	return typeCheckConstant(expr, desired)
}

// TypeCheck implements the Expr interface.
func (expr *Tuple) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	expr.types = make(DTuple, len(expr.Exprs))
	for i, subExpr := range expr.Exprs {
		var desiredElem Datum
		if t, ok := desired.(*DTuple); ok && len(*t) > i {
			desiredElem = (*t)[i]
		}
		typedExpr, err := subExpr.TypeCheck(args, desiredElem)
		if err != nil {
			return nil, err
		}
		expr.Exprs[i] = typedExpr
		expr.types[i] = typedExpr.ReturnType()
	}
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr ValArg) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) {
	dVal := &DValArg{name: expr.Name}
	if v, ok := args[expr.Name]; ok {
		dVal.typeAnnotation.typ = v
		return dVal, nil
	}
	if desired == NoTypePreference || desired == DNull {
		return nil, parameterTypeAmbiguityError{expr}
	}
	if _, err := args.SetInferredType(dVal, desired); err != nil {
		return nil, err
	}
	dVal.typeAnnotation.typ = desired
	return dVal, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DBool) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DInt) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DFloat) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DDecimal) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DString) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DBytes) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DDate) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTimestamp) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTimestampTZ) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DInterval) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTuple) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d dNull) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DValArg) TypeCheck(args MapArgs, desired Datum) (TypedExpr, error) { return d, nil }

func typeCheckAndRequireBoolean(args MapArgs, expr Expr, op string) (TypedExpr, error) {
	return typeCheckAndRequire(args, expr, TypeBool, op)
}

func typeCheckAndRequire(args MapArgs, expr Expr, required Datum, op string) (TypedExpr, error) {
	typedExpr, err := expr.TypeCheck(args, required)
	if err != nil {
		return nil, err
	}
	if typ := typedExpr.ReturnType(); !(typ == DNull || typ.TypeEqual(required)) {
		return nil, fmt.Errorf("incompatible %s type: %s", op, typ.Type())
	}
	return typedExpr, nil
}

const (
	unsupportedCompErrFmtWithTypes = "unsupported comparison operator: <%s> %s <%s>"
	unsupportedCompErrFmtWithExprs = "unsupported comparison operator: %s %s %s: %v"
)

func typeCheckComparisonOp(
	args MapArgs, op ComparisonOperator, left, right Expr,
) (TypedExpr, TypedExpr, CmpOp, error) {
	foldedOp, foldedLeft, foldedRight, switched, _ := foldComparisonExpr(op, left, right)
	ops := CmpOps[foldedOp]

	_, leftIsTuple := foldedLeft.(*Tuple)
	rightTuple, rightIsTuple := foldedRight.(*Tuple)
	switch {
	case foldedOp == In && rightIsTuple:
		sameTypeExprs := make([]Expr, 0, len(rightTuple.Exprs)+1)
		sameTypeExprs = append(sameTypeExprs, foldedLeft)
		sameTypeExprs = append(sameTypeExprs, rightTuple.Exprs...)

		typedSubExprs, retType, err := typeCheckSameTypedExprs(args, nil, sameTypeExprs...)
		if err != nil {
			return nil, nil, CmpOp{}, fmt.Errorf(unsupportedCompErrFmtWithExprs,
				left, op, right, err)
		}

		fn, ok := ops.lookupImpl(retType, TypeTuple)
		if !ok {
			return nil, nil, CmpOp{}, fmt.Errorf(unsupportedCompErrFmtWithTypes,
				retType.Type(), op, TypeTuple.Type())
		}

		typedLeft := typedSubExprs[0]
		typedSubExprs = typedSubExprs[1:]

		rightTuple.Exprs = rightTuple.Exprs[:0]
		rightTuple.types = make(DTuple, 0, len(typedSubExprs))
		for _, typedExpr := range typedSubExprs {
			rightTuple.Exprs = append(rightTuple.Exprs, typedExpr)
			rightTuple.types = append(rightTuple.types, retType)
		}
		if switched {
			return rightTuple, typedLeft, fn, nil
		}
		return typedLeft, rightTuple, fn, nil
	case leftIsTuple && rightIsTuple:
		fn, ok := ops.lookupImpl(TypeTuple, TypeTuple)
		if !ok {
			return nil, nil, CmpOp{}, fmt.Errorf(unsupportedCompErrFmtWithTypes,
				TypeTuple.Type(), op, TypeTuple.Type())
		}
		// Using non-folded left and right to avoid having to swap later.
		typedSubExprs, _, err := typeCheckSameTypedTupleExprs(args, nil, left, right)
		if err != nil {
			return nil, nil, CmpOp{}, err
		}
		return typedSubExprs[0], typedSubExprs[1], fn, nil
	}

	overloads := make([]overloadImpl, len(ops))
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
			// TypeLeft.TypeEquals(TypeBool). We currently can't distinguish NULL from
			// UNKNOWN. Is it important to do so?
			return leftExpr, rightExpr, CmpOp{}, nil
		default:
			return leftExpr, rightExpr, CmpOp{}, nil
		}
	}

	if fn == nil {
		return nil, nil, CmpOp{}, fmt.Errorf(unsupportedCompErrFmtWithTypes, leftReturn.Type(),
			op, rightReturn.Type())
	}
	return leftExpr, rightExpr, fn.(CmpOp), nil
}

type indexedExpr struct {
	e Expr
	i int
}

// typeCheckSameTypedExprs type checks a list of expressions, asserting that all
// resolved TypeExprs have the same type. An optional desired type can be provided,
// which will hint that type which the expressions should resolve to, if possible.
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

	// Handle tuples, which will in turn call into this function recursively for each element.
	if _, ok := exprs[0].(*Tuple); ok {
		return typeCheckSameTypedTupleExprs(args, desired, exprs...)
	}

	// Hold the resolved type expressions of the provided exprs, in order.
	// TODO(nvanbenschoten) Look into reducing allocations here.
	typedExprs := make([]TypedExpr, len(exprs))

	// Split the expressions into three groups of indexed expressions.
	var resolvableExprs, constExprs, valExprs []indexedExpr
	for i, expr := range exprs {
		idxExpr := indexedExpr{e: expr, i: i}
		switch {
		case isNumericConstant(expr):
			constExprs = append(constExprs, idxExpr)
		case args.IsUnresolvedArgument(expr):
			valExprs = append(valExprs, idxExpr)
		default:
			resolvableExprs = append(resolvableExprs, idxExpr)
		}
	}

	// Used to set ValArgs to the desired typ. If the typ is not provided or is
	// nil, an error will be thrown.
	typeCheckSameTypedArgs := func(typ Datum) error {
		for _, valExpr := range valExprs {
			typedExpr, err := typeCheckAndRequire(args, valExpr.e, typ, "parameter")
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
		setTypeForConsts := func(typ Datum) {
			for _, numVal := range constExprs {
				typedExpr, err := typeCheckAndRequire(args, numVal.e, typ, "numeric constant")
				if err != nil {
					panic(err)
				}
				typedExprs[numVal.i] = typedExpr
			}
		}

		// If typ is not nil, all consts try to become typ.
		if typ != nil {
			all := true
			for _, numVal := range constExprs {
				if !canConstantBecome(numVal.e.(*NumVal), typ) {
					if required {
						typedExpr, err := numVal.e.TypeCheck(args, NoTypePreference)
						if err != nil {
							return nil, err
						}
						return nil, unexpectedTypeError{numVal.e, typ, typedExpr.ReturnType()}
					}
					all = false
					break
				}
			}
			if all {
				setTypeForConsts(typ)
				return typ, nil
			}
		}

		// If all consts could not become typ, use their best shared type.
		bestType := commonNumericConstantType(constExprs)
		setTypeForConsts(bestType)
		return bestType, nil
	}

	// Used to type check all constants with the optional desired type. The
	// type that is chosen here will then be set to any arguments.
	typeCheckConstsAndArgsWithDesired := func() ([]TypedExpr, Datum, error) {
		typ, err := typeCheckSameTypedConsts(desired, false)
		if err != nil {
			return nil, nil, err
		}
		if len(valExprs) > 0 {
			if err := typeCheckSameTypedArgs(typ); err != nil {
				return nil, nil, err
			}
		}
		return typedExprs, typ, nil
	}

	switch {
	case len(resolvableExprs) == 0 && len(constExprs) == 0:
		if err := typeCheckSameTypedArgs(desired); err != nil {
			return nil, nil, err
		}
		return typedExprs, desired, nil
	case len(resolvableExprs) == 0:
		return typeCheckConstsAndArgsWithDesired()
	default:
		firstValidIdx := -1
		firstValidType := DNull
		for i, resExpr := range resolvableExprs {
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
				if err == nil {
					panic("type checking parameters without a type should throw an error")
				}
				return nil, nil, err
			default:
				return typedExprs, DNull, nil
			}
		}

		for _, resExpr := range resolvableExprs[firstValidIdx+1:] {
			typedExpr, err := resExpr.e.TypeCheck(args, firstValidType)
			if err != nil {
				return nil, nil, err
			}
			if typ := typedExpr.ReturnType(); !(typ.TypeEqual(firstValidType) || typ == DNull) {
				return nil, nil, unexpectedTypeError{resExpr.e, firstValidType, typ}
			}
			typedExprs[resExpr.i] = typedExpr
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

// typeCheckSameTypedTupleExprs type checks a list of expressions, asserting that all
// are tuples which have the same type. The function expects the first provided expression
// to be a tuple, and will panic if it is not. However, it does not expect all other
// expressions are tuples, and will return a sane error if they are not. An optional
// desired type can be provided, which will hint that type which the expressions should
// resolve to, if possible.
func typeCheckSameTypedTupleExprs(args MapArgs, desired Datum, exprs ...Expr) ([]TypedExpr, Datum, error) {
	// Hold the resolved type expressions of the provided exprs, in order.
	// TODO(nvanbenschoten) Look into reducing allocations here.
	typedExprs := make([]TypedExpr, len(exprs))

	// All other exprs must be tuples.
	first := exprs[0].(*Tuple)
	if err := checkAllExprsAreTuples(args, exprs[1:]); err != nil {
		return nil, nil, err
	}

	// All tuples must have the same length.
	firstLen := len(first.Exprs)
	if err := checkAllTuplesHaveLength(exprs[1:], firstLen); err != nil {
		return nil, nil, err
	}

	// Pull out desired types.
	var desiredTuple DTuple
	if t, ok := desired.(*DTuple); ok {
		desiredTuple = *t
	}

	// All expressions at the same indexes must be the same type.
	resTypes := make(DTuple, firstLen)
	sameTypeExprs := make([]Expr, len(exprs))
	for elemIdx := range first.Exprs {
		for tupleIdx, expr := range exprs {
			sameTypeExprs[tupleIdx] = expr.(*Tuple).Exprs[elemIdx]
		}
		desiredElem := NoTypePreference
		if len(desiredTuple) > elemIdx {
			desiredElem = desiredTuple[elemIdx]
		}
		typedSubExprs, resType, err := typeCheckSameTypedExprs(args, desiredElem, sameTypeExprs...)
		if err != nil {
			return nil, nil, fmt.Errorf("tuples %s are not the same type: %v", Exprs(exprs), err)
		}
		for j, typedExpr := range typedSubExprs {
			exprs[j].(*Tuple).Exprs[elemIdx] = typedExpr
		}
		resTypes[elemIdx] = resType
	}
	for tupleIdx, expr := range exprs {
		expr.(*Tuple).types = resTypes
		typedExprs[tupleIdx] = expr.(TypedExpr)
	}
	return typedExprs, &resTypes, nil
}

func checkAllExprsAreTuples(args MapArgs, exprs []Expr) error {
	for _, expr := range exprs {
		if _, ok := expr.(*Tuple); !ok {
			typedExpr, err := expr.TypeCheck(args, NoTypePreference)
			if err != nil {
				return err
			}
			return unexpectedTypeError{expr, TypeTuple, typedExpr.ReturnType()}
		}
	}
	return nil
}

func checkAllTuplesHaveLength(exprs []Expr, expectedLen int) error {
	for _, expr := range exprs {
		t := expr.(*Tuple)
		if len(t.Exprs) != expectedLen {
			return fmt.Errorf("expected tuple %v to have a length of %d", t, expectedLen)
		}
	}
	return nil
}

type placeholderAnnotationVisitor struct {
	placeholders map[string]annotationState
}

type annotationState struct {
	every bool
	typ   Datum
}

func (v *placeholderAnnotationVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	switch t := expr.(type) {
	// TODO(nvanbenschoten) Add support for explicit type annotations.
	// case *TypeAnnotationExpr:
	case *CastExpr:
		if arg, ok := t.Expr.(ValArg); ok {
			castType, _ := t.castTypeAndValidArgTypes()
			if state, ok := v.placeholders[arg.Name]; ok {
				if state.every && !castType.TypeEqual(state.typ) {
					state.every = false
					v.placeholders[arg.Name] = state
				}
			} else {
				v.placeholders[arg.Name] = annotationState{
					every: true,
					typ:   castType,
				}
			}
			return false, expr
		}
	case ValArg:
		v.placeholders[t.Name] = annotationState{every: false}
	}
	return true, expr
}

func (*placeholderAnnotationVisitor) VisitPost(expr Expr) Expr { return expr }

// ProcessPlaceholderAnnotations performs an order-independent global traversal of the
// provided Statement, annotating all placeholders with a type in either of the following
// situations.
// - the placeholder is the subject of an explicit type annotation in at least one
//   of its occurrences. If it is subject to multiple explicit type annotations where
//   the types are not all in agreement, and error will be thrown.
// - the placeholder is the subject to an implicit type annotation, meaning that it
//   is not subject to an explicit type annotation, and that in all occurrences of the
//   placeholder, it is subject to a cast to the same type. If it is subject to casts
//   of multiple types, no error will be thrown, but the placeholder will not be
//   annotated.
//
// TODO(nvanbenschoten) Add support for explicit placeholder annotations.
// TODO(nvanbenschoten) Can this visitor and map be preallocated (like normalizeVisitor)?
func ProcessPlaceholderAnnotations(stmt Statement, args MapArgs) error {
	v := placeholderAnnotationVisitor{make(map[string]annotationState)}
	WalkStmt(&v, stmt)
	for placeholder, state := range v.placeholders {
		if _, ok := args[placeholder]; !ok && state.every {
			args[placeholder] = state.typ
		}
	}
	return nil
}
