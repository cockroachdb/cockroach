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
	"time"

	"github.com/pkg/errors"
)

// SemaContext defines the context in which to perform semantic analysis on an
// expression syntax tree.
type SemaContext struct {
	// Placeholders relates placeholder names to their type and, later, value.
	Placeholders *PlaceholderInfo

	// Location references the *Location on the current Session.
	Location **time.Location
}

// MakeSemaContext initializes a simple SemaContext suitable
// for "lightweight" type checking such as the one performed for default
// expressions.
func MakeSemaContext() SemaContext {
	return SemaContext{Placeholders: NewPlaceholderInfo()}
}

// isUnresolvedPlaceholder provides a nil-safe method to determine whether expr is an
// unresolved placeholder.
func (sc *SemaContext) isUnresolvedPlaceholder(expr Expr) bool {
	if sc == nil {
		return false
	}
	return sc.Placeholders.IsUnresolvedPlaceholder(expr)
}

// GetLocation returns the session timezone.
func (sc *SemaContext) getLocation() *time.Location {
	if sc == nil || sc.Location == nil || *sc.Location == nil {
		return time.UTC
	}
	return *sc.Location
}

type placeholderTypeAmbiguityError struct {
	v *Placeholder
}

func (err placeholderTypeAmbiguityError) Error() string {
	return fmt.Sprintf("could not determine data type of placeholder %s", err.v)
}

type unexpectedTypeError struct {
	expr      Expr
	want, got Type
}

func (err unexpectedTypeError) Error() string {
	return fmt.Sprintf("expected %s to be of type %s, found type %s", err.expr, err.want, err.got)
}

func decorateTypeCheckError(err error, format string, a ...interface{}) error {
	if _, ok := err.(placeholderTypeAmbiguityError); ok {
		return err
	}
	return fmt.Errorf(format+": %v", append(a, err)...)
}

// TypeCheck implements the Expr interface.
func (expr *AndExpr) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	leftTyped, err := typeCheckAndRequireBoolean(ctx, expr.Left, "AND argument")
	if err != nil {
		return nil, err
	}
	rightTyped, err := typeCheckAndRequireBoolean(ctx, expr.Right, "AND argument")
	if err != nil {
		return nil, err
	}
	expr.Left, expr.Right = leftTyped, rightTyped
	expr.typ = TypeBool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *BinaryExpr) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	ops := BinOps[expr.Operator]
	overloads := make([]overloadImpl, len(ops))
	for i := range ops {
		overloads[i] = ops[i]
	}

	typedSubExprs, fn, err := typeCheckOverloadedExprs(ctx, desired, overloads, expr.Left, expr.Right)
	if err != nil {
		return nil, err
	}

	leftTyped, rightTyped := typedSubExprs[0], typedSubExprs[1]
	leftReturn := leftTyped.ResolvedType()
	rightReturn := rightTyped.ResolvedType()
	if leftReturn == TypeNull || rightReturn == TypeNull {
		return DNull, nil
	}

	if fn == nil {
		var desStr string
		if desired != nil {
			desStr = fmt.Sprintf(" (desired <%s>)", desired)
		}
		return nil, fmt.Errorf("unsupported binary operator: <%s> %s <%s>%s",
			leftReturn, expr.Operator, rightReturn, desStr)
	}
	expr.Left, expr.Right = leftTyped, rightTyped
	expr.fn = fn.(BinOp)
	expr.typ = fn.returnType()
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *CaseExpr) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	var err error
	tmpExprs := make([]Expr, 0, len(expr.Whens)+1)
	if expr.Expr != nil {
		tmpExprs = tmpExprs[:0]
		tmpExprs = append(tmpExprs, expr.Expr)
		for _, when := range expr.Whens {
			tmpExprs = append(tmpExprs, when.Cond)
		}

		typedSubExprs, _, err := typeCheckSameTypedExprs(ctx, nil, tmpExprs...)
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
			if expr.Whens[i].Cond, err = typeCheckAndRequireBoolean(ctx, when.Cond, "condition"); err != nil {
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
	typedSubExprs, retType, err := typeCheckSameTypedExprs(ctx, desired, tmpExprs...)
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
func (expr *CastExpr) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	returnDatum, validTypes := expr.castTypeAndValidArgTypes()

	// The desired type provided to a CastExpr is ignored. Instead, NoTypePreference
	// is passed to the child of the cast. There are two exceptions, described below.
	desired = NoTypePreference
	switch {
	case isConstant(expr.Expr):
		if canConstantBecome(expr.Expr.(Constant), returnDatum) {
			// If a Constant is subject to a cast which it can naturally become (which
			// is in its resolvable type set), we desire the cast's type for the Constant,
			// which will result in the CastExpr becoming an identity cast.
			desired = returnDatum
		}
	case ctx.isUnresolvedPlaceholder(expr.Expr):
		// This case will be triggered if ProcessPlaceholderAnnotations found
		// the same placeholder in another location where it was either not
		// the child of a cast, or was the child of a cast to a different type.
		// In this case, we default to inferring a STRING for the placeholder.
		desired = TypeString
	}

	typedSubExpr, err := expr.Expr.TypeCheck(ctx, desired)
	if err != nil {
		return nil, err
	}

	castFrom := typedSubExpr.ResolvedType()
	for _, t := range validTypes {
		if castFrom.Equal(t) {
			expr.Expr = typedSubExpr
			expr.typ = returnDatum
			return expr, nil
		}
	}

	return nil, fmt.Errorf("invalid cast: %s -> %s", castFrom, expr.Type)
}

// TypeCheck implements the Expr interface.
func (expr *AnnotateTypeExpr) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	annotType := expr.annotationType()
	subExpr, err := typeCheckAndRequire(ctx, expr.Expr, annotType,
		fmt.Sprintf("type assertion for %v as %s, found", expr.Expr, annotType))
	if err != nil {
		return nil, err
	}
	expr.Expr = subExpr
	expr.typ = subExpr.ResolvedType()
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *CoalesceExpr) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	typedSubExprs, retType, err := typeCheckSameTypedExprs(ctx, desired, expr.Exprs...)
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
func (expr *ComparisonExpr) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	leftTyped, rightTyped, fn, err := typeCheckComparisonOp(ctx, expr.Operator, expr.Left, expr.Right)
	if err != nil {
		return nil, err
	}
	expr.Left, expr.Right = leftTyped, rightTyped
	expr.fn = fn
	expr.typ = TypeBool
	return expr, err
}

// TypeCheck implements the Expr interface.
func (expr *ExistsExpr) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	subqueryTyped, err := expr.Subquery.TypeCheck(ctx, NoTypePreference)
	if err != nil {
		return nil, err
	}
	expr.Subquery = subqueryTyped
	expr.typ = TypeBool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *FuncExpr) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	fname, err := expr.Name.Normalize()
	if err != nil {
		return nil, err
	}

	if len(fname.Context) > 0 {
		// We don't support qualified function names (yet).
		return nil, fmt.Errorf("unknown function: %s", fname)
	}

	name := string(fname.FunctionName)
	// Optimize for the case where name is already normalized to upper/lower
	// case. Note that the Builtins map contains duplicate entries for
	// upper/lower case names.
	candidates, ok := Builtins[name]
	if !ok {
		candidates, ok = Builtins[strings.ToLower(name)]
	}
	if !ok {
		return nil, fmt.Errorf("unknown function: %s", name)
	}

	overloads := make([]overloadImpl, len(candidates))
	for i := range candidates {
		overloads[i] = candidates[i]
	}
	typedSubExprs, fn, err := typeCheckOverloadedExprs(ctx, desired, overloads, expr.Exprs...)
	if err != nil {
		return nil, fmt.Errorf("%s: %v", name, err)
	} else if fn == nil {
		typeNames := make([]string, 0, len(expr.Exprs))
		for _, expr := range typedSubExprs {
			typeNames = append(typeNames, expr.ResolvedType().String())
		}
		var desStr string
		if desired != nil {
			desStr = fmt.Sprintf(" (desired <%s>)", desired)
		}
		return nil, fmt.Errorf("unknown signature for %s: %s(%s)%s",
			expr.Name, expr.Name, strings.Join(typeNames, ", "), desStr)
	}

	if expr.WindowDef != nil {
		for i, partition := range expr.WindowDef.Partitions {
			typedPartition, err := partition.TypeCheck(ctx, NoTypePreference)
			if err != nil {
				return nil, err
			}
			expr.WindowDef.Partitions[i] = typedPartition
		}
	}

	builtin := fn.(Builtin)
	if expr.IsWindowFunctionApplication() {
		// Make sure the window function application is of either a built-in window
		// function or of a builtin aggregate function.
		switch builtin.class {
		case AggregateClass:
		case WindowClass:
		default:
			return nil, fmt.Errorf("OVER specified, but %s is not a window function nor an "+
				"aggregate function", expr.Name)
		}
	} else {
		// Make sure the window function builtins are used as window function applications.
		switch builtin.class {
		case WindowClass:
			return nil, fmt.Errorf("window function %s requires an OVER clause", expr.Name)
		}
	}

	for i, subExpr := range typedSubExprs {
		expr.Exprs[i] = subExpr
	}
	expr.fn = builtin
	returnType := fn.returnType()
	if _, ok = expr.fn.params().(AnyType); ok {
		if len(typedSubExprs) > 0 {
			returnType = typedSubExprs[0].ResolvedType()
		} else {
			returnType = TypeNull
		}
	}
	expr.typ = returnType
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *IfExpr) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	var err error
	if expr.Cond, err = typeCheckAndRequireBoolean(ctx, expr.Cond, "IF condition"); err != nil {
		return nil, err
	}

	typedSubExprs, retType, err := typeCheckSameTypedExprs(ctx, desired, expr.True, expr.Else)
	if err != nil {
		return nil, decorateTypeCheckError(err, "incompatible IF expressions")
	}

	expr.True, expr.Else = typedSubExprs[0], typedSubExprs[1]
	expr.typ = retType
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *IsOfTypeExpr) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	exprTyped, err := expr.Expr.TypeCheck(ctx, NoTypePreference)
	if err != nil {
		return nil, err
	}
	expr.Expr = exprTyped
	expr.typ = TypeBool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *NotExpr) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	exprTyped, err := typeCheckAndRequireBoolean(ctx, expr.Expr, "NOT argument")
	if err != nil {
		return nil, err
	}
	expr.Expr = exprTyped
	expr.typ = TypeBool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *NullIfExpr) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	typedSubExprs, retType, err := typeCheckSameTypedExprs(ctx, desired, expr.Expr1, expr.Expr2)
	if err != nil {
		return nil, decorateTypeCheckError(err, "incompatible NULLIF expressions")
	}

	expr.Expr1, expr.Expr2 = typedSubExprs[0], typedSubExprs[1]
	expr.typ = retType
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *OrExpr) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	leftTyped, err := typeCheckAndRequireBoolean(ctx, expr.Left, "OR argument")
	if err != nil {
		return nil, err
	}
	rightTyped, err := typeCheckAndRequireBoolean(ctx, expr.Right, "OR argument")
	if err != nil {
		return nil, err
	}
	expr.Left, expr.Right = leftTyped, rightTyped
	expr.typ = TypeBool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *ParenExpr) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	exprTyped, err := expr.Expr.TypeCheck(ctx, desired)
	if err != nil {
		return nil, err
	}
	expr.Expr = exprTyped
	expr.typ = exprTyped.ResolvedType()
	return expr, nil
}

// presetTypesForTesting is a mapping of qualified names to types that can be mocked out
// for tests to allow the qualified names to be type checked without throwing an error.
var presetTypesForTesting map[string]Type

func mockNameTypes(types map[string]Type) func() {
	presetTypesForTesting = types
	return func() {
		presetTypesForTesting = nil
	}
}

// TypeCheck implements the Expr interface.  This function has a valid
// implementation only for testing within this package. During query
// execution, ColumnItems are replaced to IndexedVars prior to type
// checking.
func (expr *ColumnItem) TypeCheck(_ *SemaContext, desired Type) (TypedExpr, error) {
	name := expr.String()
	if _, ok := presetTypesForTesting[name]; ok {
		return expr, nil
	}
	return nil, fmt.Errorf("name \"%s\" is not defined", name)
}

// TypeCheck implements the Expr interface.
func (expr UnqualifiedStar) TypeCheck(_ *SemaContext, desired Type) (TypedExpr, error) {
	return nil, errors.New("cannot use \"*\" in this context")
}

// TypeCheck implements the Expr interface.
func (expr UnresolvedName) TypeCheck(s *SemaContext, desired Type) (TypedExpr, error) {
	v, err := expr.NormalizeVarName()
	if err != nil {
		return nil, err
	}
	return v.TypeCheck(s, desired)
}

// TypeCheck implements the Expr interface.
func (expr *AllColumnsSelector) TypeCheck(_ *SemaContext, desired Type) (TypedExpr, error) {
	return nil, fmt.Errorf("cannot use %q in this context", expr)
}

// TypeCheck implements the Expr interface.
func (expr *RangeCond) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	typedSubExprs, _, err := typeCheckSameTypedExprs(ctx, nil, expr.Left, expr.From, expr.To)
	if err != nil {
		return nil, err
	}

	expr.Left, expr.From, expr.To = typedSubExprs[0], typedSubExprs[1], typedSubExprs[2]
	expr.typ = TypeBool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *Subquery) TypeCheck(_ *SemaContext, desired Type) (TypedExpr, error) {
	panic("subquery nodes must be replaced before type checking")
}

// TypeCheck implements the Expr interface.
func (expr *UnaryExpr) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	ops := UnaryOps[expr.Operator]
	overloads := make([]overloadImpl, len(ops))
	for i := range ops {
		overloads[i] = ops[i]
	}

	typedSubExprs, fn, err := typeCheckOverloadedExprs(ctx, desired, overloads, expr.Expr)
	if err != nil {
		return nil, err
	}

	exprTyped := typedSubExprs[0]
	exprReturn := exprTyped.ResolvedType()
	if exprReturn == TypeNull {
		return DNull, nil
	}

	if fn == nil {
		var desStr string
		if desired != nil {
			desStr = fmt.Sprintf(" (desired <%s>)", desired)
		}
		return nil, fmt.Errorf("unsupported unary operator: %s <%s>%s",
			expr.Operator, exprReturn, desStr)
	}
	expr.Expr = exprTyped
	expr.fn = fn.(UnaryOp)
	expr.typ = fn.returnType()
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *Array) TypeCheck(_ *SemaContext, desired Type) (TypedExpr, error) {
	return nil, errors.Errorf("unhandled type %T", expr)
}

// TypeCheck implements the Expr interface.
func (expr DefaultVal) TypeCheck(_ *SemaContext, desired Type) (TypedExpr, error) {
	return nil, errors.Errorf("unhandled type %T", expr)
}

// TypeCheck implements the Expr interface.
func (expr *NumVal) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	return typeCheckConstant(expr, ctx, desired)
}

// TypeCheck implements the Expr interface.
func (expr *StrVal) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	return typeCheckConstant(expr, ctx, desired)
}

// TypeCheck implements the Expr interface.
func (expr *Tuple) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	expr.types = make(TTuple, len(expr.Exprs))
	for i, subExpr := range expr.Exprs {
		desiredElem := NoTypePreference
		if t, ok := desired.(TTuple); ok && len(t) > i {
			desiredElem = t[i]
		}
		typedExpr, err := subExpr.TypeCheck(ctx, desiredElem)
		if err != nil {
			return nil, err
		}
		expr.Exprs[i] = typedExpr
		expr.types[i] = typedExpr.ResolvedType()
	}
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *Placeholder) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	// If there is a value known already, immediately substitute with it.
	if v, ok := ctx.Placeholders.Value(expr.Name); ok {
		return v, nil
	}

	// Otherwise, perform placeholder typing.
	if typ, ok := ctx.Placeholders.Type(expr.Name); ok {
		expr.typ = typ
		return expr, nil
	}
	if desired == NoTypePreference || desired == TypeNull {
		return nil, placeholderTypeAmbiguityError{expr}
	}
	if err := ctx.Placeholders.SetType(expr.Name, desired); err != nil {
		return nil, err
	}
	expr.typ = desired
	return expr, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DBool) TypeCheck(_ *SemaContext, desired Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DInt) TypeCheck(_ *SemaContext, desired Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DFloat) TypeCheck(_ *SemaContext, desired Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DDecimal) TypeCheck(_ *SemaContext, desired Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DUTF8String) TypeCheck(_ *SemaContext, desired Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DBytes) TypeCheck(_ *SemaContext, desired Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DDate) TypeCheck(_ *SemaContext, desired Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTimestampNoTZ) TypeCheck(_ *SemaContext, desired Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTimestampTZ) TypeCheck(_ *SemaContext, desired Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DInterval) TypeCheck(_ *SemaContext, desired Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTuple) TypeCheck(_ *SemaContext, desired Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DArray) TypeCheck(_ *SemaContext, desired Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d dNull) TypeCheck(_ *SemaContext, desired Type) (TypedExpr, error) { return d, nil }

func typeCheckAndRequireBoolean(ctx *SemaContext, expr Expr, op string) (TypedExpr, error) {
	return typeCheckAndRequire(ctx, expr, TypeBool, op)
}

func typeCheckAndRequire(ctx *SemaContext, expr Expr, required Type, op string) (TypedExpr, error) {
	typedExpr, err := expr.TypeCheck(ctx, required)
	if err != nil {
		return nil, err
	}
	if typ := typedExpr.ResolvedType(); !(typ == TypeNull || typ.Equal(required)) {
		return nil, fmt.Errorf("incompatible %s type: %s", op, typ)
	}
	return typedExpr, nil
}

const (
	unsupportedCompErrFmtWithTypes = "unsupported comparison operator: <%s> %s <%s>"
	unsupportedCompErrFmtWithExprs = "unsupported comparison operator: %s %s %s: %v"
)

func typeCheckComparisonOp(
	ctx *SemaContext, op ComparisonOperator, left, right Expr,
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

		typedSubExprs, retType, err := typeCheckSameTypedExprs(ctx, nil, sameTypeExprs...)
		if err != nil {
			return nil, nil, CmpOp{}, fmt.Errorf(unsupportedCompErrFmtWithExprs,
				left, op, right, err)
		}

		fn, ok := ops.lookupImpl(retType, TTuple(nil))
		if !ok {
			return nil, nil, CmpOp{}, fmt.Errorf(unsupportedCompErrFmtWithTypes, retType, op, TTuple(nil))
		}

		typedLeft := typedSubExprs[0]
		typedSubExprs = typedSubExprs[1:]

		rightTuple.types = make(TTuple, len(typedSubExprs))
		for i, typedExpr := range typedSubExprs {
			rightTuple.Exprs[i] = typedExpr
			rightTuple.types[i] = retType
		}
		if switched {
			return rightTuple, typedLeft, fn, nil
		}
		return typedLeft, rightTuple, fn, nil
	case leftIsTuple && rightIsTuple:
		fn, ok := ops.lookupImpl(TTuple(nil), TTuple(nil))
		if !ok {
			return nil, nil, CmpOp{}, fmt.Errorf(unsupportedCompErrFmtWithTypes, TTuple(nil), op, TTuple(nil))
		}
		// Using non-folded left and right to avoid having to swap later.
		typedSubExprs, _, err := typeCheckSameTypedTupleExprs(ctx, nil, left, right)
		if err != nil {
			return nil, nil, CmpOp{}, err
		}
		return typedSubExprs[0], typedSubExprs[1], fn, nil
	}

	overloads := make([]overloadImpl, len(ops))
	for i := range ops {
		overloads[i] = ops[i]
	}
	typedSubExprs, fn, err := typeCheckOverloadedExprs(ctx, nil, overloads, foldedLeft, foldedRight)
	if err != nil {
		return nil, nil, CmpOp{}, err
	}

	leftExpr, rightExpr := typedSubExprs[0], typedSubExprs[1]
	if switched {
		leftExpr, rightExpr = rightExpr, leftExpr
	}
	leftReturn := leftExpr.(TypedExpr).ResolvedType()
	rightReturn := rightExpr.(TypedExpr).ResolvedType()
	if leftReturn == TypeNull || rightReturn == TypeNull {
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
		return nil, nil, CmpOp{},
			fmt.Errorf(unsupportedCompErrFmtWithTypes, leftReturn, op, rightReturn)
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
func typeCheckSameTypedExprs(
	ctx *SemaContext, desired Type, exprs ...Expr,
) ([]TypedExpr, Type, error) {
	switch len(exprs) {
	case 0:
		return nil, nil, nil
	case 1:
		typedExpr, err := exprs[0].TypeCheck(ctx, desired)
		if err != nil {
			return nil, nil, err
		}
		return []TypedExpr{typedExpr}, typedExpr.ResolvedType(), nil
	}

	// Handle tuples, which will in turn call into this function recursively for each element.
	if _, ok := exprs[0].(*Tuple); ok {
		return typeCheckSameTypedTupleExprs(ctx, desired, exprs...)
	}

	// Hold the resolved type expressions of the provided exprs, in order.
	// TODO(nvanbenschoten) Look into reducing allocations here.
	typedExprs := make([]TypedExpr, len(exprs))

	// Split the expressions into three groups of indexed expressions:
	// - Placeholders
	// - Numeric constants
	// - All other Exprs
	var resolvableExprs, constExprs, placeholderExprs []indexedExpr
	for i, expr := range exprs {
		idxExpr := indexedExpr{e: expr, i: i}
		switch {
		case isNumericConstant(expr):
			constExprs = append(constExprs, idxExpr)
		case ctx.isUnresolvedPlaceholder(expr):
			placeholderExprs = append(placeholderExprs, idxExpr)
		default:
			resolvableExprs = append(resolvableExprs, idxExpr)
		}
	}

	// Used to set placeholders to the desired typ. If the typ is not provided or is
	// nil, an error will be thrown.
	typeCheckSameTypedPlaceholders := func(typ Type) error {
		for _, placeholderExpr := range placeholderExprs {
			typedExpr, err := typeCheckAndRequire(ctx, placeholderExpr.e, typ, "placeholder")
			if err != nil {
				return err
			}
			typedExprs[placeholderExpr.i] = typedExpr
		}
		return nil
	}

	// Used to type check constants to the same type. An optional typ can be
	// provided to signify the desired shared type, which can be set to the
	// required shared type using the second parameter.
	typeCheckSameTypedConsts := func(typ Type, required bool) (Type, error) {
		setTypeForConsts := func(typ Type) {
			for _, constExpr := range constExprs {
				typedExpr, err := typeCheckAndRequire(ctx, constExpr.e, typ, "numeric constant")
				if err != nil {
					panic(err)
				}
				typedExprs[constExpr.i] = typedExpr
			}
		}

		// If typ is not nil, all consts try to become typ.
		if typ != nil {
			all := true
			for _, constExpr := range constExprs {
				if !canConstantBecome(constExpr.e.(Constant), typ) {
					if required {
						typedExpr, err := constExpr.e.TypeCheck(ctx, NoTypePreference)
						if err != nil {
							return nil, err
						}
						return nil, unexpectedTypeError{constExpr.e, typ, typedExpr.ResolvedType()}
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
	// type that is chosen here will then be set to any placeholders.
	typeCheckConstsAndPlaceholdersWithDesired := func() ([]TypedExpr, Type, error) {
		typ, err := typeCheckSameTypedConsts(desired, false)
		if err != nil {
			return nil, nil, err
		}
		if len(placeholderExprs) > 0 {
			if err := typeCheckSameTypedPlaceholders(typ); err != nil {
				return nil, nil, err
			}
		}
		return typedExprs, typ, nil
	}

	switch {
	case len(resolvableExprs) == 0 && len(constExprs) == 0:
		if err := typeCheckSameTypedPlaceholders(desired); err != nil {
			return nil, nil, err
		}
		return typedExprs, desired, nil
	case len(resolvableExprs) == 0:
		return typeCheckConstsAndPlaceholdersWithDesired()
	default:
		firstValidIdx := -1
		firstValidType := TypeNull
		for i, resExpr := range resolvableExprs {
			typedExpr, err := resExpr.e.TypeCheck(ctx, desired)
			if err != nil {
				return nil, nil, err
			}
			typedExprs[resExpr.i] = typedExpr
			if returnType := typedExpr.ResolvedType(); returnType != TypeNull {
				firstValidType = returnType
				firstValidIdx = i
				break
			}
		}

		if firstValidType == TypeNull {
			switch {
			case len(constExprs) > 0:
				return typeCheckConstsAndPlaceholdersWithDesired()
			case len(placeholderExprs) > 0:
				err := typeCheckSameTypedPlaceholders(nil)
				if err == nil {
					panic("type checking parameters without a type should throw an error")
				}
				return nil, nil, err
			default:
				return typedExprs, TypeNull, nil
			}
		}

		for _, resExpr := range resolvableExprs[firstValidIdx+1:] {
			typedExpr, err := resExpr.e.TypeCheck(ctx, firstValidType)
			if err != nil {
				return nil, nil, err
			}
			if typ := typedExpr.ResolvedType(); !(typ.Equal(firstValidType) || typ == TypeNull) {
				return nil, nil, unexpectedTypeError{resExpr.e, firstValidType, typ}
			}
			typedExprs[resExpr.i] = typedExpr
		}
		if len(constExprs) > 0 {
			if _, err := typeCheckSameTypedConsts(firstValidType, true); err != nil {
				return nil, nil, err
			}
		}
		if len(placeholderExprs) > 0 {
			if err := typeCheckSameTypedPlaceholders(firstValidType); err != nil {
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
func typeCheckSameTypedTupleExprs(
	ctx *SemaContext, desired Type, exprs ...Expr,
) ([]TypedExpr, Type, error) {
	// Hold the resolved type expressions of the provided exprs, in order.
	// TODO(nvanbenschoten) Look into reducing allocations here.
	typedExprs := make([]TypedExpr, len(exprs))

	// All other exprs must be tuples.
	first := exprs[0].(*Tuple)
	if err := checkAllExprsAreTuples(ctx, exprs[1:]); err != nil {
		return nil, nil, err
	}

	// All tuples must have the same length.
	firstLen := len(first.Exprs)
	if err := checkAllTuplesHaveLength(exprs[1:], firstLen); err != nil {
		return nil, nil, err
	}

	// Pull out desired types.
	var desiredTuple TTuple
	if t, ok := desired.(TTuple); ok {
		desiredTuple = t
	}

	// All expressions at the same indexes must be the same type.
	resTypes := make(TTuple, firstLen)
	sameTypeExprs := make([]Expr, len(exprs))
	for elemIdx := range first.Exprs {
		for tupleIdx, expr := range exprs {
			sameTypeExprs[tupleIdx] = expr.(*Tuple).Exprs[elemIdx]
		}
		desiredElem := NoTypePreference
		if len(desiredTuple) > elemIdx {
			desiredElem = desiredTuple[elemIdx]
		}
		typedSubExprs, resType, err := typeCheckSameTypedExprs(ctx, desiredElem, sameTypeExprs...)
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
	return typedExprs, resTypes, nil
}

func checkAllExprsAreTuples(ctx *SemaContext, exprs []Expr) error {
	for _, expr := range exprs {
		if _, ok := expr.(*Tuple); !ok {
			typedExpr, err := expr.TypeCheck(ctx, NoTypePreference)
			if err != nil {
				return err
			}
			return unexpectedTypeError{expr, TTuple(nil), typedExpr.ResolvedType()}
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

// annotationState holds the state of an unreseolved type annotation for a given placeholder.
type annotationState struct {
	sawAssertion   bool // marks if the placeholder has been subject to at least one type assetion
	shouldAnnotate bool // marks if the placeholder should be annotated with the type typ
	typ            Type
}

func (v *placeholderAnnotationVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	switch t := expr.(type) {
	case *AnnotateTypeExpr:
		if arg, ok := t.Expr.(*Placeholder); ok {
			assertType := t.annotationType()
			if state, ok := v.placeholders[arg.Name]; ok && state.sawAssertion {
				if state.shouldAnnotate && !assertType.Equal(state.typ) {
					state.shouldAnnotate = false
					v.placeholders[arg.Name] = state
				}
			} else {
				// Ignore any previous casts now that we see an annotation.
				v.placeholders[arg.Name] = annotationState{
					sawAssertion:   true,
					shouldAnnotate: true,
					typ:            assertType,
				}
			}
			return false, expr
		}
	case *CastExpr:
		if arg, ok := t.Expr.(*Placeholder); ok {
			castType, _ := t.castTypeAndValidArgTypes()
			if state, ok := v.placeholders[arg.Name]; ok {
				// Ignore casts once an assertion has been seen.
				if state.sawAssertion {
					return false, expr
				}

				if state.shouldAnnotate && !castType.Equal(state.typ) {
					state.shouldAnnotate = false
					v.placeholders[arg.Name] = state
				}
			} else {
				v.placeholders[arg.Name] = annotationState{
					shouldAnnotate: true,
					typ:            castType,
				}
			}
			return false, expr
		}
	case *Placeholder:
		if state, ok := v.placeholders[t.Name]; !(ok && state.sawAssertion) {
			// Ignore non-annotated placeholders once an assertion has been seen.
			state.shouldAnnotate = false
			v.placeholders[t.Name] = state
		}
		return false, expr
	}
	return true, expr
}

func (*placeholderAnnotationVisitor) VisitPost(expr Expr) Expr { return expr }

// ProcessPlaceholderAnnotations performs an order-independent global traversal of the
// provided Statement, annotating all placeholders with a type in either of the following
// situations.
// - the placeholder is the subject of an explicit type annotation in at least one
//   of its occurrences. If it is subject to multiple explicit type annotations where
//   the types are not all in agreement, or if the placeholder already has an inferred
//   type in the placeholder map which conflicts with the explicit type annotation
//   type, an error will be thrown.
// - the placeholder is the subject to an implicit type annotation, meaning that it
//   is not subject to an explicit type annotation, and that in all occurrences of the
//   placeholder, it is subject to a cast to the same type. If it is subject to casts
//   of multiple types, no error will be thrown, but the placeholder type will not be
//   inferred. If a type has already been assigned for the placeholder in the placeholder
//   map, no error will be thrown, and the placeholder will keep it's previously
//   inferred type.
//
// TODO(nvanbenschoten) Can this visitor and map be preallocated (like normalizeVisitor)?
func (p PlaceholderTypes) ProcessPlaceholderAnnotations(stmt Statement) error {
	v := placeholderAnnotationVisitor{make(map[string]annotationState)}

	// We treat existing inferred types in the MapPlaceholderTypes as initial assertions.
	for placeholder, typ := range p {
		v.placeholders[placeholder] = annotationState{
			sawAssertion:   true,
			shouldAnnotate: true,
			typ:            typ,
		}
	}

	WalkStmt(&v, stmt)
	for placeholder, state := range v.placeholders {
		if state.shouldAnnotate {
			p[placeholder] = state.typ
		} else if state.sawAssertion {
			// If we should not annotate the type but we did see a type assertion,
			// there were conflicting type assertions.
			if prevType, ok := p[placeholder]; ok {
				return fmt.Errorf("found type annotation around %s that conflicts with previously "+
					"inferred type %s", placeholder, prevType)
			}
			return fmt.Errorf("found multiple conflicting type annotations around %s", placeholder)
		}
	}
	return nil
}
