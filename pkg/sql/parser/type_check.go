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

	"golang.org/x/text/language"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/pkg/errors"
)

// SemaContext defines the context in which to perform semantic analysis on an
// expression syntax tree.
type SemaContext struct {
	// Placeholders relates placeholder names to their type and, later, value.
	Placeholders PlaceholderInfo

	// Location references the *Location on the current Session.
	Location **time.Location

	// SearchPath indicates where to search for unqualified function
	// names. The path elements must be normalized via Name.Normalize()
	// already.
	SearchPath []string

	// privileged, if true, enables "unsafe" builtins, e.g. those
	// from the crdb_internal namespace. Must be set only for
	// the root user.
	// TODO(knz): this attribute can be moved to EvalContext pending #15363.
	privileged bool
}

// MakeSemaContext initializes a simple SemaContext suitable
// for "lightweight" type checking such as the one performed for default
// expressions.
func MakeSemaContext(privileged bool) SemaContext {
	return SemaContext{
		Placeholders: MakePlaceholderInfo(),
		privileged:   privileged,
	}
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
		if desired != TypeAny {
			desStr = fmt.Sprintf(" (desired <%s>)", desired)
		}
		return nil, fmt.Errorf("unsupported binary operator: <%s> %s <%s>%s",
			leftReturn, expr.Operator, rightReturn, desStr)
	}
	expr.Left, expr.Right = leftTyped, rightTyped
	expr.fn = fn.(BinOp)
	expr.typ = fn.returnType()(typedSubExprs)
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

		typedSubExprs, _, err := typeCheckSameTypedExprs(ctx, TypeAny, tmpExprs...)
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
			typedCond, err := typeCheckAndRequireBoolean(ctx, when.Cond, "condition")
			if err != nil {
				return nil, err
			}
			expr.Whens[i].Cond = typedCond
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
func (expr *CastExpr) TypeCheck(ctx *SemaContext, _ Type) (TypedExpr, error) {
	returnType := expr.castType()

	// The desired type provided to a CastExpr is ignored. Instead,
	// TypeAny is passed to the child of the cast. There are two
	// exceptions, described below.
	desired := TypeAny
	switch {
	case isConstant(expr.Expr):
		if canConstantBecome(expr.Expr.(Constant), returnType) {
			// If a Constant is subject to a cast which it can naturally become (which
			// is in its resolvable type set), we desire the cast's type for the Constant,
			// which will result in the CastExpr becoming an identity cast.
			desired = returnType

			// If the type doesn't have any possible parameters (like length,
			// precision), the CastExpr becomes a no-op and can be elided.
			switch expr.Type.(type) {
			case *BoolColType, *DateColType, *TimestampColType, *TimestampTZColType,
				*IntervalColType, *BytesColType:
				return expr.Expr.TypeCheck(ctx, returnType)
			}
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
	for _, t := range validCastTypes(returnType) {
		if castFrom.FamilyEqual(t) {
			expr.Expr = typedSubExpr
			expr.typ = returnType
			return expr, nil
		}
	}

	return nil, fmt.Errorf("invalid cast: %s -> %s", castFrom, expr.Type)
}

// TypeCheck implements the Expr interface.
func (expr *IndirectionExpr) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	for i, t := range expr.Indirection {
		if t.Slice {
			return nil, util.UnimplementedWithIssueErrorf(2115, "ARRAY slicing in %s", expr)
		}
		if i > 0 {
			return nil, util.UnimplementedWithIssueErrorf(2115, "multidimensional ARRAY %s", expr)
		}

		beginExpr, err := typeCheckAndRequire(ctx, t.Begin, TypeInt, "ARRAY subscript")
		if err != nil {
			return nil, err
		}
		t.Begin = beginExpr
	}

	subExpr, err := expr.Expr.TypeCheck(ctx, TArray{desired})
	if err != nil {
		return nil, err
	}
	typ := UnwrapType(subExpr.ResolvedType())
	arrType, ok := typ.(TArray)
	if !ok {
		return nil, errors.Errorf("cannot subscript type %s because it is not an array", typ)
	}
	expr.Expr = subExpr
	expr.typ = arrType.Typ
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *AnnotateTypeExpr) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	annotType := expr.annotationType()
	subExpr, err := typeCheckAndRequire(ctx, expr.Expr, annotType,
		fmt.Sprintf("type annotation for %v as %s, found", expr.Expr, annotType))
	if err != nil {
		return nil, err
	}
	return subExpr, nil
}

// TypeCheck implements the Expr interface.
func (expr *CollateExpr) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	_, err := language.Parse(expr.Locale)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid locale %s", expr.Locale)
	}
	subExpr, err := expr.Expr.TypeCheck(ctx, TypeString)
	if err != nil {
		return nil, err
	}
	switch t := subExpr.ResolvedType().(type) {
	case tString, TCollatedString:
		expr.Expr = subExpr
		expr.typ = TCollatedString{expr.Locale}
		return expr, nil
	default:
		return nil, fmt.Errorf("incompatible type for COLLATE: %s", t)
	}
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
	var leftTyped, rightTyped TypedExpr
	var fn CmpOp
	var err error
	if expr.Operator.hasSubOperator() {
		leftTyped, rightTyped, fn, err = typeCheckComparisonOpWithSubOperator(ctx,
			expr.Operator,
			expr.SubOperator,
			expr.Left,
			expr.Right,
		)
	} else {
		leftTyped, rightTyped, fn, err = typeCheckComparisonOp(ctx,
			expr.Operator,
			expr.Left,
			expr.Right,
		)
	}
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
	subqueryTyped, err := expr.Subquery.TypeCheck(ctx, TypeAny)
	if err != nil {
		return nil, err
	}
	expr.Subquery = subqueryTyped
	expr.typ = TypeBool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *FuncExpr) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	var searchPath SearchPath
	if ctx != nil {
		searchPath = ctx.SearchPath
	}
	def, err := expr.Func.Resolve(searchPath)
	if err != nil {
		return nil, err
	}

	overloads := make([]overloadImpl, len(def.Definition))
	for i, d := range def.Definition {
		overloads[i] = d
	}
	typedSubExprs, fn, err := typeCheckOverloadedExprs(ctx, desired, overloads, expr.Exprs...)
	if err != nil {
		return nil, fmt.Errorf("%s(): %v", def.Name, err)
	} else if fn == nil {
		typeNames := make([]string, 0, len(expr.Exprs))
		for _, expr := range typedSubExprs {
			typeNames = append(typeNames, expr.ResolvedType().String())
		}
		var desStr string
		if desired != TypeAny {
			desStr = fmt.Sprintf(" (desired <%s>)", desired)
		}
		return nil, fmt.Errorf("unknown signature: %s(%s)%s",
			expr.Func, strings.Join(typeNames, ", "), desStr)
	}

	if expr.WindowDef != nil {
		for i, partition := range expr.WindowDef.Partitions {
			typedPartition, err := partition.TypeCheck(ctx, TypeAny)
			if err != nil {
				return nil, err
			}
			expr.WindowDef.Partitions[i] = typedPartition
		}
		for i, orderBy := range expr.WindowDef.OrderBy {
			typedOrderBy, err := orderBy.Expr.TypeCheck(ctx, TypeAny)
			if err != nil {
				return nil, err
			}
			expr.WindowDef.OrderBy[i].Expr = typedOrderBy
		}
	}

	if expr.Filter != nil {
		typedFilter, err := typeCheckAndRequireBoolean(ctx, expr.Filter, "FILTER expression")
		if err != nil {
			return nil, err
		}
		expr.Filter = typedFilter
	}

	builtin := fn.(Builtin)
	if expr.IsWindowFunctionApplication() {
		// Make sure the window function application is of either a built-in window
		// function or of a builtin aggregate function.
		switch builtin.class {
		case AggregateClass:
		case WindowClass:
		default:
			return nil, fmt.Errorf("OVER specified, but %s() is neither a window function nor an "+
				"aggregate function", expr.Func)
		}

		if expr.Filter != nil {
			return nil, fmt.Errorf("FILTER within a window function call is not yet supported")
		}
	} else {
		// Make sure the window function builtins are used as window function applications.
		switch builtin.class {
		case WindowClass:
			return nil, fmt.Errorf("window function %s() requires an OVER clause", expr.Func)
		}
	}

	if expr.Filter != nil {
		if builtin.class != AggregateClass {
			// Same error message as Postgres.
			return nil, fmt.Errorf("FILTER specified but %s() is not an aggregate function", expr.Func)
		}

	}

	// Check that the built-in is allowed for the current user.
	// TODO(knz): this check can be moved to evaluation time pending #15363.
	if builtin.privileged && !ctx.privileged {
		return nil, pgerror.NewErrorf(pgerror.CodeInsufficientPrivilegeError,
			"insufficient privilege to use %s", expr.Func)
	}

	for i, subExpr := range typedSubExprs {
		expr.Exprs[i] = subExpr
	}
	expr.fn = builtin
	expr.typ = builtin.returnType()(typedSubExprs)
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *IfExpr) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	typedCond, err := typeCheckAndRequireBoolean(ctx, expr.Cond, "IF condition")
	if err != nil {
		return nil, err
	}

	typedSubExprs, retType, err := typeCheckSameTypedExprs(ctx, desired, expr.True, expr.Else)
	if err != nil {
		return nil, decorateTypeCheckError(err, "incompatible IF expressions")
	}

	expr.Cond = typedCond
	expr.True, expr.Else = typedSubExprs[0], typedSubExprs[1]
	expr.typ = retType
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *IsOfTypeExpr) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	exprTyped, err := expr.Expr.TypeCheck(ctx, TypeAny)
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
	leftTyped, fromTyped, _, err := typeCheckComparisonOp(ctx, GT, expr.Left, expr.From)
	if err != nil {
		return nil, err
	}
	_, toTyped, _, err := typeCheckComparisonOp(ctx, LT, expr.Left, expr.To)
	if err != nil {
		return nil, err
	}

	expr.Left, expr.From, expr.To = leftTyped, fromTyped, toTyped
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
		if desired != TypeAny {
			desStr = fmt.Sprintf(" (desired <%s>)", desired)
		}
		return nil, fmt.Errorf("unsupported unary operator: %s <%s>%s",
			expr.Operator, exprReturn, desStr)
	}
	expr.Expr = exprTyped
	expr.fn = fn.(UnaryOp)
	expr.typ = fn.returnType()(typedSubExprs)
	return expr, nil
}

var errInvalidDefaultUsage = errors.New("DEFAULT can only appear in a VALUES list within INSERT or on the right side of a SET within UPDATE")

// TypeCheck implements the Expr interface.
func (expr DefaultVal) TypeCheck(_ *SemaContext, desired Type) (TypedExpr, error) {
	return nil, errInvalidDefaultUsage
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
		desiredElem := TypeAny
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

var errAmbiguousArrayType = errors.Errorf("cannot determine type of empty array. " +
	"Consider annotating with the desired type, for example ARRAY[]:::int[]")

// TypeCheck implements the Expr interface.
func (expr *Array) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	desiredParam := TypeAny
	if arr, ok := desired.(TArray); ok {
		desiredParam = arr.Typ
	}

	if len(expr.Exprs) == 0 {
		if desiredParam == TypeAny {
			return nil, errAmbiguousArrayType
		}
		expr.typ = TArray{desiredParam}
		return expr, nil
	}

	typedSubExprs, typ, err := typeCheckSameTypedExprs(ctx, desiredParam, expr.Exprs...)
	if err != nil {
		return nil, err
	}

	expr.typ = TArray{typ}
	for i := range typedSubExprs {
		expr.Exprs[i] = typedSubExprs[i]
	}
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *ArrayFlatten) TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error) {
	desiredParam := TypeAny
	if arr, ok := desired.(TArray); ok {
		desiredParam = arr.Typ
	}

	subqueryTyped, err := expr.Subquery.TypeCheck(ctx, desiredParam)
	if err != nil {
		return nil, err
	}
	expr.Subquery = subqueryTyped
	subqueryType := subqueryTyped.ResolvedType()
	switch subqueryType {
	case TypeInt:
		expr.typ = TypeIntArray
	case TypeString:
		expr.typ = TypeStringArray
	default:
		return nil, errors.Errorf("unhandled parameterized array type %T", subqueryType)
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
	if desired.IsAmbiguous() {
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
func (d *DBool) TypeCheck(_ *SemaContext, _ Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DInt) TypeCheck(_ *SemaContext, _ Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DFloat) TypeCheck(_ *SemaContext, _ Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DDecimal) TypeCheck(_ *SemaContext, _ Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DString) TypeCheck(_ *SemaContext, _ Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DCollatedString) TypeCheck(_ *SemaContext, _ Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DBytes) TypeCheck(_ *SemaContext, _ Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DDate) TypeCheck(_ *SemaContext, _ Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTimestamp) TypeCheck(_ *SemaContext, _ Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTimestampTZ) TypeCheck(_ *SemaContext, _ Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DInterval) TypeCheck(_ *SemaContext, _ Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTuple) TypeCheck(_ *SemaContext, _ Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DArray) TypeCheck(_ *SemaContext, _ Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTable) TypeCheck(_ *SemaContext, _ Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DOid) TypeCheck(_ *SemaContext, _ Type) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DOidWrapper) TypeCheck(_ *SemaContext, _ Type) (TypedExpr, error) { return d, nil }

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
	if typ := typedExpr.ResolvedType(); !(typ == TypeNull || typ.Equivalent(required)) {
		return nil, fmt.Errorf("incompatible %s type: %s", op, typ)
	}
	return typedExpr, nil
}

const (
	unsupportedCompErrFmtWithTypes         = "unsupported comparison operator: <%s> %s <%s>"
	unsupportedCompErrFmtWithTypesAndSubOp = "unsupported comparison operator: <%s> %s %s <%s>"
	unsupportedCompErrFmtWithExprs         = "unsupported comparison operator: %s %s %s: %v"
	unsupportedCompErrFmtWithExprsAndSubOp = "unsupported comparison operator: %s %s %s %s: %v"
)

func typeCheckComparisonOpWithSubOperator(
	ctx *SemaContext, op, subOp ComparisonOperator, left, right Expr,
) (TypedExpr, TypedExpr, CmpOp, error) {
	// Determine the set of comparisons are possible for the sub-operation,
	// which will be memoized.
	foldedOp, _, _, _, _ := foldComparisonExpr(subOp, nil, nil)
	ops := CmpOps[foldedOp]

	var cmpTypeLeft, cmpTypeRight Type
	var leftTyped, rightTyped TypedExpr
	if array, isConstructor := StripParens(right).(*Array); isConstructor {
		// If the right expression is an (optionally nested) array constructor, we
		// perform type inference on the array elements and the left expression.
		sameTypeExprs := make([]Expr, len(array.Exprs)+1)
		sameTypeExprs[0] = left
		copy(sameTypeExprs[1:], array.Exprs)

		typedSubExprs, retType, err := typeCheckSameTypedExprs(ctx, TypeAny, sameTypeExprs...)
		if err != nil {
			return nil, nil, CmpOp{}, fmt.Errorf(unsupportedCompErrFmtWithExprsAndSubOp,
				left, op, subOp, right, err)
		}

		// Determine TypedExpr and comparison type for left operand.
		leftTyped = typedSubExprs[0]
		cmpTypeLeft = retType

		// Determine TypedExpr and comparison type for right operand, making sure
		// all ParenExprs on the right are properly type checked.
		for i, typedExpr := range typedSubExprs[1:] {
			array.Exprs[i] = typedExpr
		}
		array.typ = TArray{retType}

		rightParen := right
		for {
			if p, ok := rightParen.(*ParenExpr); ok {
				p.typ = array.typ
				rightParen = p.Expr
				continue
			}
			break
		}
		rightTyped = right.(TypedExpr)
		cmpTypeRight = retType

		// Return early without looking up a CmpOp if the comparison type is TypeNull.
		if retType == TypeNull {
			return leftTyped, rightTyped, CmpOp{}, nil
		}
	} else {
		// If the right expression is not an array constructor, we type the left
		// expression in isolation.
		var err error
		leftTyped, err = left.TypeCheck(ctx, TypeAny)
		if err != nil {
			return nil, nil, CmpOp{}, err
		}
		cmpTypeLeft = leftTyped.ResolvedType()

		// We then type the right expression desiring an array of the left's type.
		rightTyped, err = right.TypeCheck(ctx, TArray{cmpTypeLeft})
		if err != nil {
			return nil, nil, CmpOp{}, err
		}

		rightReturn := rightTyped.ResolvedType()
		if cmpTypeLeft == TypeNull || rightReturn == TypeNull {
			return leftTyped, rightTyped, CmpOp{}, nil
		}

		rightArr, ok := UnwrapType(rightReturn).(TArray)
		if !ok {
			return nil, nil, CmpOp{},
				errors.Errorf(unsupportedCompErrFmtWithExprsAndSubOp, left, subOp, op, right,
					fmt.Sprintf("op %s array requires array on right side", op))
		}
		cmpTypeRight = rightArr.Typ
	}

	fn, ok := ops.lookupImpl(cmpTypeLeft, cmpTypeRight)
	if !ok {
		return nil, nil, CmpOp{}, fmt.Errorf(unsupportedCompErrFmtWithTypesAndSubOp,
			cmpTypeLeft, subOp, op, TArray{cmpTypeRight})
	}
	return leftTyped, rightTyped, fn, nil
}

func typeCheckComparisonOp(
	ctx *SemaContext, op ComparisonOperator, left, right Expr,
) (TypedExpr, TypedExpr, CmpOp, error) {
	foldedOp, foldedLeft, foldedRight, switched, _ := foldComparisonExpr(op, left, right)
	ops := CmpOps[foldedOp]

	_, leftIsTuple := foldedLeft.(*Tuple)
	rightTuple, rightIsTuple := foldedRight.(*Tuple)
	switch {
	case foldedOp == In && rightIsTuple:
		sameTypeExprs := make([]Expr, len(rightTuple.Exprs)+1)
		sameTypeExprs[0] = foldedLeft
		copy(sameTypeExprs[1:], rightTuple.Exprs)

		typedSubExprs, retType, err := typeCheckSameTypedExprs(ctx, TypeAny, sameTypeExprs...)
		if err != nil {
			return nil, nil, CmpOp{}, fmt.Errorf(unsupportedCompErrFmtWithExprs,
				left, op, right, err)
		}

		fn, ok := ops.lookupImpl(retType, TypeTuple)
		if !ok {
			return nil, nil, CmpOp{}, fmt.Errorf(unsupportedCompErrFmtWithTypes, retType, op, TypeTuple)
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
		fn, ok := ops.lookupImpl(TypeTuple, TypeTuple)
		if !ok {
			return nil, nil, CmpOp{}, fmt.Errorf(unsupportedCompErrFmtWithTypes, TypeTuple, op, TypeTuple)
		}
		// Using non-folded left and right to avoid having to swap later.
		typedLeft, typedRight, err := typeCheckTupleComparison(ctx, op, left.(*Tuple), right.(*Tuple))
		if err != nil {
			return nil, nil, CmpOp{}, err
		}
		return typedLeft, typedRight, fn, nil
	}

	overloads := make([]overloadImpl, len(ops))
	for i := range ops {
		overloads[i] = ops[i]
	}
	typedSubExprs, fn, err := typeCheckOverloadedExprs(ctx, TypeAny, overloads, foldedLeft, foldedRight)
	if err != nil {
		return nil, nil, CmpOp{}, err
	}

	leftExpr, rightExpr := typedSubExprs[0], typedSubExprs[1]
	if switched {
		leftExpr, rightExpr = rightExpr, leftExpr
	}
	leftReturn := leftExpr.ResolvedType()
	rightReturn := rightExpr.ResolvedType()
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

	if fn == nil ||
		(leftReturn.FamilyEqual(TypeCollatedString) && !leftReturn.Equivalent(rightReturn)) {
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
	// TODO(nvanbenschoten): Look into reducing allocations here.
	typedExprs := make([]TypedExpr, len(exprs))

	// Split the expressions into three groups of indexed expressions:
	// - Placeholders
	// - Constants
	// - All other Exprs
	var resolvableExprs, constExprs, placeholderExprs []indexedExpr
	for i, expr := range exprs {
		idxExpr := indexedExpr{e: expr, i: i}
		switch {
		case isConstant(expr):
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
		setTypeForConsts := func(typ Type) (Type, error) {
			for _, constExpr := range constExprs {
				typedExpr, err := typeCheckAndRequire(ctx, constExpr.e, typ, "constant")
				if err != nil {
					// In this case, even though the constExpr has been shown to be
					// upcastable to typ based on canConstantBecome, it can't actually be
					// parsed as typ.
					return nil, err
				}
				typedExprs[constExpr.i] = typedExpr
			}
			return typ, nil
		}

		// If typ is not a wildcard, all consts try to become typ.
		if typ != TypeAny {
			all := true
			for _, constExpr := range constExprs {
				if !canConstantBecome(constExpr.e.(Constant), typ) {
					if required {
						typedExpr, err := constExpr.e.TypeCheck(ctx, TypeAny)
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
				return setTypeForConsts(typ)
			}
		}

		// If not all constExprs could become typ but they have a mutual
		// resolvable type, use this common type.
		if bestType, ok := commonConstantType(constExprs); ok {
			return setTypeForConsts(bestType)
		}

		// If not, we want to force an error because the constants cannot all
		// become the same type.
		reqTyp := typ
		for _, constExpr := range constExprs {
			typedExpr, err := constExpr.e.TypeCheck(ctx, reqTyp)
			if err != nil {
				return nil, err
			}
			if typ := typedExpr.ResolvedType(); !typ.Equivalent(reqTyp) {
				return nil, unexpectedTypeError{constExpr.e, reqTyp, typ}
			}
			if reqTyp == TypeAny {
				reqTyp = typedExpr.ResolvedType()
			}
		}
		panic("should throw error above")
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
			if typ := typedExpr.ResolvedType(); !(typ.Equivalent(firstValidType) || typ == TypeNull) {
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

// typeCheckTupleComparison type checks a comparison between two tuples,
// asserting that the elements of the two tuples are comparable at each index.
func typeCheckTupleComparison(
	ctx *SemaContext, op ComparisonOperator, left *Tuple, right *Tuple,
) (TypedExpr, TypedExpr, error) {
	// All tuples must have the same length.
	tupLen := len(left.Exprs)
	if err := checkTupleHasLength(right, tupLen); err != nil {
		return nil, nil, err
	}
	left.types = make(TTuple, tupLen)
	right.types = make(TTuple, tupLen)
	for elemIdx := range left.Exprs {
		leftSubExpr := left.Exprs[elemIdx]
		rightSubExpr := right.Exprs[elemIdx]
		leftSubExprTyped, rightSubExprTyped, _, err := typeCheckComparisonOp(ctx, op, leftSubExpr, rightSubExpr)
		if err != nil {
			return nil, nil, fmt.Errorf("tuples %s are not comparable at index %d: %s",
				Exprs([]Expr{left, right}), elemIdx+1, err)
		}
		left.Exprs[elemIdx] = leftSubExprTyped
		left.types[elemIdx] = leftSubExprTyped.ResolvedType()
		right.Exprs[elemIdx] = rightSubExprTyped
		right.types[elemIdx] = rightSubExprTyped.ResolvedType()
	}
	return left, right, nil
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
	// TODO(nvanbenschoten): Look into reducing allocations here.
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
		desiredElem := TypeAny
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
			typedExpr, err := expr.TypeCheck(ctx, TypeAny)
			if err != nil {
				return err
			}
			return unexpectedTypeError{expr, TypeTuple, typedExpr.ResolvedType()}
		}
	}
	return nil
}

func checkAllTuplesHaveLength(exprs []Expr, expectedLen int) error {
	for _, expr := range exprs {
		if err := checkTupleHasLength(expr.(*Tuple), expectedLen); err != nil {
			return err
		}
	}
	return nil
}

func checkTupleHasLength(t *Tuple, expectedLen int) error {
	if len(t.Exprs) != expectedLen {
		return fmt.Errorf("expected tuple %v to have a length of %d", t, expectedLen)
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
				if state.shouldAnnotate && !assertType.Equivalent(state.typ) {
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
			castType := t.castType()
			if state, ok := v.placeholders[arg.Name]; ok {
				// Ignore casts once an assertion has been seen.
				if state.sawAssertion {
					return false, expr
				}

				if state.shouldAnnotate && !castType.Equivalent(state.typ) {
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
// TODO(nvanbenschoten): Can this visitor and map be preallocated (like normalizeVisitor)?
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
