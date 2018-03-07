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

package tree

import (
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/text/language"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// SemaContext defines the context in which to perform semantic analysis on an
// expression syntax tree.
type SemaContext struct {
	// Placeholders relates placeholder names to their type and, later, value.
	Placeholders PlaceholderInfo

	// IVarContainer is used to resolve the types of IndexedVars.
	IVarContainer IndexedVarContainer

	// Location references the *Location on the current Session.
	Location **time.Location

	// SearchPath indicates where to search for unqualified function
	// names. The path elements must be normalized via Name.Normalize()
	// already.
	SearchPath sessiondata.SearchPath

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
	want, got types.T
}

func (err unexpectedTypeError) Error() string {
	return fmt.Sprintf("expected %s to be of type %s, found type %s", err.expr, err.want, err.got)
}

func decorateTypeCheckError(err error, format string, a ...interface{}) error {
	if _, ok := err.(placeholderTypeAmbiguityError); ok {
		return err
	}
	return errors.Wrapf(err, format, a...)
}

// TypeCheck performs type checking on the provided expression tree, returning
// the new typed expression tree, which additionally permits evaluation and type
// introspection globally and on each sub-tree.
//
// While doing so, it will fold numeric constants and bind placeholder names to
// their inferred types in the provided context. The optional desired parameter can
// be used to hint the desired type for the root of the resulting typed expression
// tree. Like with Expr.TypeCheck, it is not valid to provide a nil desired
// type. Instead, call it with the wildcard type types.Any if no specific type is
// desired.
func TypeCheck(expr Expr, ctx *SemaContext, desired types.T) (TypedExpr, error) {
	if desired == nil {
		panic("the desired type for tree.TypeCheck cannot be nil, use types.Any instead")
	}

	expr, err := FoldConstantLiterals(expr)
	if err != nil {
		return nil, err
	}
	return expr.TypeCheck(ctx, desired)
}

// TypeCheckAndRequire performs type checking on the provided expression tree in
// an identical manner to TypeCheck. It then asserts that the resulting TypedExpr
// has the provided return type, returning both the typed expression and an error
// if it does not.
func TypeCheckAndRequire(
	expr Expr, ctx *SemaContext, required types.T, op string,
) (TypedExpr, error) {
	typedExpr, err := TypeCheck(expr, ctx, required)
	if err != nil {
		return nil, err
	}
	if typ := typedExpr.ResolvedType(); !(typ.Equivalent(required) || typ == types.Unknown) {
		return typedExpr, pgerror.NewErrorf(
			pgerror.CodeDatatypeMismatchError, "argument of %s must be type %s, not type %s", op, required, typ)
	}
	return typedExpr, nil
}

// TypeCheck implements the Expr interface.
func (expr *AndExpr) TypeCheck(ctx *SemaContext, desired types.T) (TypedExpr, error) {
	leftTyped, err := typeCheckAndRequireBoolean(ctx, expr.Left, "AND argument")
	if err != nil {
		return nil, err
	}
	rightTyped, err := typeCheckAndRequireBoolean(ctx, expr.Right, "AND argument")
	if err != nil {
		return nil, err
	}
	expr.Left, expr.Right = leftTyped, rightTyped
	expr.typ = types.Bool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *BinaryExpr) TypeCheck(ctx *SemaContext, desired types.T) (TypedExpr, error) {
	ops := BinOps[expr.Operator]

	typedSubExprs, fns, err := typeCheckOverloadedExprs(ctx, desired, ops, true, expr.Left, expr.Right)
	if err != nil {
		return nil, err
	}

	leftTyped, rightTyped := typedSubExprs[0], typedSubExprs[1]
	leftReturn := leftTyped.ResolvedType()
	rightReturn := rightTyped.ResolvedType()

	// Return NULL if at least one overload is possible, NULL is an argument,
	// and none of the overloads accept NULL.
	if leftReturn == types.Unknown || rightReturn == types.Unknown {
		if len(fns) > 0 {
			noneAcceptNull := true
			for _, e := range fns {
				if e.(BinOp).NullableArgs {
					noneAcceptNull = false
					break
				}
			}
			if noneAcceptNull {
				return DNull, nil
			}
		}
	}

	// Throw a typing error if overload resolution found either no compatible candidates
	// or if it found an ambiguity.
	if len(fns) != 1 {
		var desStr string
		if desired != types.Any {
			desStr = fmt.Sprintf(" (desired <%s>)", desired)
		}
		sig := fmt.Sprintf("<%s> %s <%s>%s", leftReturn, expr.Operator, rightReturn, desStr)
		if len(fns) == 0 {
			return nil,
				pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError, unsupportedBinaryOpErrFmt, sig)
		}
		fnsStr := formatCandidates(expr.Operator.String(), fns)
		return nil,
			pgerror.NewErrorf(pgerror.CodeAmbiguousFunctionError,
				ambiguousBinaryOpErrFmt, sig).SetHintf(candidatesHintFmt, fnsStr)
	}

	binOp := fns[0].(BinOp)
	expr.Left, expr.Right = leftTyped, rightTyped
	expr.fn = binOp
	expr.typ = binOp.returnType()(typedSubExprs)
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *CaseExpr) TypeCheck(ctx *SemaContext, desired types.T) (TypedExpr, error) {
	var err error
	tmpExprs := make([]Expr, 0, len(expr.Whens)+1)
	if expr.Expr != nil {
		tmpExprs = tmpExprs[:0]
		tmpExprs = append(tmpExprs, expr.Expr)
		for _, when := range expr.Whens {
			tmpExprs = append(tmpExprs, when.Cond)
		}

		typedSubExprs, _, err := TypeCheckSameTypedExprs(ctx, types.Any, tmpExprs...)
		if err != nil {
			return nil, decorateTypeCheckError(err, "incompatible condition type:")
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
	typedSubExprs, retType, err := TypeCheckSameTypedExprs(ctx, desired, tmpExprs...)
	if err != nil {
		return nil, decorateTypeCheckError(err, "incompatible value type:")
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

func isCastDeepValid(castFrom, castTo types.T) bool {
	castFrom = types.UnwrapType(castFrom)
	castTo = types.UnwrapType(castTo)
	if castTo.FamilyEqual(types.FamArray) && castFrom.FamilyEqual(types.FamArray) {
		return isCastDeepValid(castFrom.(types.TArray).Typ, castTo.(types.TArray).Typ)
	}
	for _, t := range validCastTypes(castTo) {
		if castFrom.FamilyEqual(t) {
			return true
		}
	}
	return false
}

// TypeCheck implements the Expr interface.
func (expr *CastExpr) TypeCheck(ctx *SemaContext, _ types.T) (TypedExpr, error) {
	returnType := expr.castType()

	// The desired type provided to a CastExpr is ignored. Instead,
	// types.Any is passed to the child of the cast. There are two
	// exceptions, described below.
	desired := types.Any
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
			case *coltypes.TBool, *coltypes.TDate, *coltypes.TTime, *coltypes.TTimestamp, *coltypes.TTimestampTZ,
				*coltypes.TInterval, *coltypes.TBytes:
				return expr.Expr.TypeCheck(ctx, returnType)
			}
		}
	case ctx.isUnresolvedPlaceholder(expr.Expr):
		// This case will be triggered if ProcessPlaceholderAnnotations found
		// the same placeholder in another location where it was either not
		// the child of a cast, or was the child of a cast to a different type.
		// In this case, we default to inferring a STRING for the placeholder.
		desired = types.String
	}

	typedSubExpr, err := expr.Expr.TypeCheck(ctx, desired)
	if err != nil {
		return nil, err
	}

	castFrom := typedSubExpr.ResolvedType()

	if isCastDeepValid(castFrom, returnType) {
		expr.Expr = typedSubExpr
		expr.typ = returnType
		return expr, nil
	}

	return nil, pgerror.NewErrorf(pgerror.CodeCannotCoerceError, "invalid cast: %s -> %s", castFrom, expr.Type)
}

// TypeCheck implements the Expr interface.
func (expr *IndirectionExpr) TypeCheck(ctx *SemaContext, desired types.T) (TypedExpr, error) {
	for i, t := range expr.Indirection {
		if t.Slice {
			return nil, pgerror.UnimplementedWithIssueErrorf(2115, "ARRAY slicing in %s", expr)
		}
		if i > 0 {
			return nil, pgerror.UnimplementedWithIssueErrorf(2115, "multidimensional ARRAY %s", expr)
		}

		beginExpr, err := typeCheckAndRequire(ctx, t.Begin, types.Int, "ARRAY subscript")
		if err != nil {
			return nil, err
		}
		t.Begin = beginExpr
	}

	subExpr, err := expr.Expr.TypeCheck(ctx, types.TArray{Typ: desired})
	if err != nil {
		return nil, err
	}
	typ := types.UnwrapType(subExpr.ResolvedType())
	arrType, ok := typ.(types.TArray)
	if !ok {
		return nil, pgerror.NewErrorf(pgerror.CodeDatatypeMismatchError, "cannot subscript type %s because it is not an array", typ)
	}
	expr.Expr = subExpr
	expr.typ = arrType.Typ
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *AnnotateTypeExpr) TypeCheck(ctx *SemaContext, desired types.T) (TypedExpr, error) {
	annotType := expr.annotationType()
	subExpr, err := typeCheckAndRequire(ctx, expr.Expr, annotType,
		fmt.Sprintf("type annotation for %v as %s, found", expr.Expr, annotType))
	if err != nil {
		return nil, err
	}
	return subExpr, nil
}

// TypeCheck implements the Expr interface.
func (expr *CollateExpr) TypeCheck(ctx *SemaContext, desired types.T) (TypedExpr, error) {
	_, err := language.Parse(expr.Locale)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid locale %s", expr.Locale)
	}
	subExpr, err := expr.Expr.TypeCheck(ctx, types.String)
	if err != nil {
		return nil, err
	}
	t := subExpr.ResolvedType()
	if types.IsStringType(t) || t == types.Unknown {
		expr.Expr = subExpr
		expr.typ = types.TCollatedString{Locale: expr.Locale}
		return expr, nil
	}
	return nil, pgerror.NewErrorf(pgerror.CodeDatatypeMismatchError, "incompatible type for COLLATE: %s", t)
}

// TypeCheck implements the Expr interface.
func (expr *CoalesceExpr) TypeCheck(ctx *SemaContext, desired types.T) (TypedExpr, error) {
	typedSubExprs, retType, err := TypeCheckSameTypedExprs(ctx, desired, expr.Exprs...)
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
func (expr *ComparisonExpr) TypeCheck(ctx *SemaContext, desired types.T) (TypedExpr, error) {
	var leftTyped, rightTyped TypedExpr
	var fn CmpOp
	var alwaysNull bool
	var err error
	if expr.Operator.hasSubOperator() {
		leftTyped, rightTyped, fn, alwaysNull, err = typeCheckComparisonOpWithSubOperator(ctx,
			expr.Operator,
			expr.SubOperator,
			expr.Left,
			expr.Right,
		)
	} else {
		leftTyped, rightTyped, fn, alwaysNull, err = typeCheckComparisonOp(ctx,
			expr.Operator,
			expr.Left,
			expr.Right,
		)
	}
	if err != nil {
		return nil, err
	}

	if alwaysNull {
		return DNull, nil
	}

	expr.Left, expr.Right = leftTyped, rightTyped
	expr.fn = fn
	expr.typ = types.Bool
	return expr, nil
}

var (
	errOrderByIndexInWindow = pgerror.NewError(pgerror.CodeFeatureNotSupportedError, "ORDER BY INDEX in window definition is not supported")
	errFilterWithinWindow   = pgerror.NewErrorf(pgerror.CodeFeatureNotSupportedError, "FILTER within a window function call is not yet supported")
	errStarNotAllowed       = pgerror.NewError(pgerror.CodeSyntaxError, "cannot use \"*\" in this context")
	errInvalidDefaultUsage  = pgerror.NewError(pgerror.CodeSyntaxError, "DEFAULT can only appear in a VALUES list within INSERT or on the right side of a SET")
	errInvalidMaxUsage      = pgerror.NewError(pgerror.CodeSyntaxError, "MAXVALUE can only appear within a range partition expression")
	errInvalidMinUsage      = pgerror.NewError(pgerror.CodeSyntaxError, "MINVALUE can only appear within a range partition expression")
)

// TypeCheck implements the Expr interface.
func (expr *FuncExpr) TypeCheck(ctx *SemaContext, desired types.T) (TypedExpr, error) {
	var searchPath sessiondata.SearchPath
	if ctx != nil {
		searchPath = ctx.SearchPath
	}
	def, err := expr.Func.Resolve(searchPath)
	if err != nil {
		return nil, err
	}

	typedSubExprs, fns, err := typeCheckOverloadedExprs(ctx, desired, def.Definition, false, expr.Exprs...)
	if err != nil {
		return nil, errors.Wrapf(err, "%s()", def.Name)
	}

	// Return NULL if at least one overload is possible and NULL is an argument.
	if len(fns) > 0 {
		// However, if any of the possible candidate functions can handle NULL
		// arguments, we don't want to take the NULL argument fast-path.
		handledNull := false
		for _, fn := range fns {
			if fn.(*Builtin).NullableArgs {
				handledNull = true
				break
			}
		}
		if !handledNull {
			for _, expr := range typedSubExprs {
				if expr.ResolvedType() == types.Unknown {
					return DNull, nil
				}
			}
		}
	}

	// Throw a typing error if overload resolution found either no compatible candidates
	// or if it found an ambiguity.
	// TODO(nvanbenschoten): now that we can distinguish these, we can improve the
	//   error message the two report (e.g. "add casts please")
	if len(fns) != 1 {
		typeNames := make([]string, 0, len(expr.Exprs))
		for _, expr := range typedSubExprs {
			typeNames = append(typeNames, expr.ResolvedType().String())
		}
		var desStr string
		if desired != types.Any {
			desStr = fmt.Sprintf(" (desired <%s>)", desired)
		}
		sig := fmt.Sprintf("%s(%s)%s", &expr.Func, strings.Join(typeNames, ", "), desStr)
		if len(fns) == 0 {
			return nil, pgerror.NewErrorf(pgerror.CodeUndefinedFunctionError, "unknown signature: %s", sig)
		}
		fnsStr := formatCandidates(expr.Func.String(), fns)
		return nil, pgerror.NewErrorf(pgerror.CodeAmbiguousFunctionError, "ambiguous call: %s, candidates are:\n%s", sig, fnsStr)
	}

	if expr.WindowDef != nil {
		for i, partition := range expr.WindowDef.Partitions {
			typedPartition, err := partition.TypeCheck(ctx, types.Any)
			if err != nil {
				return nil, err
			}
			expr.WindowDef.Partitions[i] = typedPartition
		}
		for i, orderBy := range expr.WindowDef.OrderBy {
			if orderBy.OrderType != OrderByColumn {
				return nil, errOrderByIndexInWindow
			}
			typedOrderBy, err := orderBy.Expr.TypeCheck(ctx, types.Any)
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

	builtin := fns[0].(*Builtin)
	if expr.IsWindowFunctionApplication() {
		// Make sure the window function application is of either a built-in window
		// function or of a builtin aggregate function.
		switch builtin.Class {
		case AggregateClass:
		case WindowClass:
		default:
			return nil, pgerror.NewErrorf(pgerror.CodeWrongObjectTypeError,
				"OVER specified, but %s() is neither a window function nor an aggregate function",
				&expr.Func)
		}

		if expr.Filter != nil {
			return nil, errFilterWithinWindow
		}
	} else {
		// Make sure the window function builtins are used as window function applications.
		switch builtin.Class {
		case WindowClass:
			return nil, pgerror.NewErrorf(pgerror.CodeWrongObjectTypeError,
				"window function %s() requires an OVER clause", &expr.Func)
		}
	}

	if expr.Filter != nil {
		if builtin.Class != AggregateClass {
			// Same error message as Postgres.
			return nil, pgerror.NewErrorf(pgerror.CodeWrongObjectTypeError,
				"FILTER specified but %s() is not an aggregate function", &expr.Func)
		}

	}

	// Check that the built-in is allowed for the current user.
	// TODO(knz): this check can be moved to evaluation time pending #15363.
	if builtin.Privileged && !ctx.privileged {
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
func (expr *IfExpr) TypeCheck(ctx *SemaContext, desired types.T) (TypedExpr, error) {
	typedCond, err := typeCheckAndRequireBoolean(ctx, expr.Cond, "IF condition")
	if err != nil {
		return nil, err
	}

	typedSubExprs, retType, err := TypeCheckSameTypedExprs(ctx, desired, expr.True, expr.Else)
	if err != nil {
		return nil, decorateTypeCheckError(err, "incompatible IF expressions")
	}

	expr.Cond = typedCond
	expr.True, expr.Else = typedSubExprs[0], typedSubExprs[1]
	expr.typ = retType
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *IsOfTypeExpr) TypeCheck(ctx *SemaContext, desired types.T) (TypedExpr, error) {
	exprTyped, err := expr.Expr.TypeCheck(ctx, types.Any)
	if err != nil {
		return nil, err
	}
	expr.Expr = exprTyped
	expr.typ = types.Bool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *NotExpr) TypeCheck(ctx *SemaContext, desired types.T) (TypedExpr, error) {
	exprTyped, err := typeCheckAndRequireBoolean(ctx, expr.Expr, "NOT argument")
	if err != nil {
		return nil, err
	}
	expr.Expr = exprTyped
	expr.typ = types.Bool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *NullIfExpr) TypeCheck(ctx *SemaContext, desired types.T) (TypedExpr, error) {
	typedSubExprs, retType, err := TypeCheckSameTypedExprs(ctx, desired, expr.Expr1, expr.Expr2)
	if err != nil {
		return nil, decorateTypeCheckError(err, "incompatible NULLIF expressions")
	}

	expr.Expr1, expr.Expr2 = typedSubExprs[0], typedSubExprs[1]
	expr.typ = retType
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *OrExpr) TypeCheck(ctx *SemaContext, desired types.T) (TypedExpr, error) {
	leftTyped, err := typeCheckAndRequireBoolean(ctx, expr.Left, "OR argument")
	if err != nil {
		return nil, err
	}
	rightTyped, err := typeCheckAndRequireBoolean(ctx, expr.Right, "OR argument")
	if err != nil {
		return nil, err
	}
	expr.Left, expr.Right = leftTyped, rightTyped
	expr.typ = types.Bool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *ParenExpr) TypeCheck(ctx *SemaContext, desired types.T) (TypedExpr, error) {
	exprTyped, err := expr.Expr.TypeCheck(ctx, desired)
	if err != nil {
		return nil, err
	}
	// Parentheses are semantically unimportant and can be removed/replaced
	// with its nested expression in our plan. This makes type checking cleaner.
	return exprTyped, nil
}

// TypeCheck implements the Expr interface.  This function has a valid
// implementation only for testing within this package. During query
// execution, ColumnItems are replaced to IndexedVars prior to type
// checking.
func (expr *ColumnItem) TypeCheck(_ *SemaContext, desired types.T) (TypedExpr, error) {
	name := expr.String()
	if _, ok := presetTypesForTesting[name]; ok {
		return expr, nil
	}
	return nil, pgerror.NewErrorf(pgerror.CodeInternalError, "name \"%s\" is not defined", name)
}

// TypeCheck implements the Expr interface.
func (expr UnqualifiedStar) TypeCheck(_ *SemaContext, desired types.T) (TypedExpr, error) {
	return nil, errStarNotAllowed
}

// TypeCheck implements the Expr interface.
func (expr *UnresolvedName) TypeCheck(s *SemaContext, desired types.T) (TypedExpr, error) {
	v, err := expr.NormalizeVarName()
	if err != nil {
		return nil, err
	}
	return v.TypeCheck(s, desired)
}

// TypeCheck implements the Expr interface.
func (expr *AllColumnsSelector) TypeCheck(_ *SemaContext, desired types.T) (TypedExpr, error) {
	return nil, pgerror.NewErrorf(pgerror.CodeSyntaxError, "cannot use %q in this context", expr)
}

// TypeCheck implements the Expr interface.
func (expr *RangeCond) TypeCheck(ctx *SemaContext, desired types.T) (TypedExpr, error) {
	leftTyped, fromTyped, _, _, err := typeCheckComparisonOp(ctx, GT, expr.Left, expr.From)
	if err != nil {
		return nil, err
	}
	_, toTyped, _, _, err := typeCheckComparisonOp(ctx, LT, expr.Left, expr.To)
	if err != nil {
		return nil, err
	}

	expr.Left, expr.From, expr.To = leftTyped, fromTyped, toTyped
	expr.typ = types.Bool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *Subquery) TypeCheck(_ *SemaContext, _ types.T) (TypedExpr, error) {
	expr.assertTyped()
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *UnaryExpr) TypeCheck(ctx *SemaContext, desired types.T) (TypedExpr, error) {
	ops := UnaryOps[expr.Operator]

	typedSubExprs, fns, err := typeCheckOverloadedExprs(ctx, desired, ops, false, expr.Expr)
	if err != nil {
		return nil, err
	}

	exprTyped := typedSubExprs[0]
	exprReturn := exprTyped.ResolvedType()

	// Return NULL if at least one overload is possible and NULL is an argument.
	if len(fns) > 0 {
		if exprReturn == types.Unknown {
			return DNull, nil
		}
	}

	// Throw a typing error if overload resolution found either no compatible candidates
	// or if it found an ambiguity.
	if len(fns) != 1 {
		var desStr string
		if desired != types.Any {
			desStr = fmt.Sprintf(" (desired <%s>)", desired)
		}
		sig := fmt.Sprintf("%s <%s>%s", expr.Operator, exprReturn, desStr)
		if len(fns) == 0 {
			return nil,
				pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError, unsupportedUnaryOpErrFmt, sig)
		}
		fnsStr := formatCandidates(expr.Operator.String(), fns)
		return nil, pgerror.NewErrorf(pgerror.CodeAmbiguousFunctionError,
			ambiguousUnaryOpErrFmt, sig).SetHintf(candidatesHintFmt, fnsStr)
	}

	unaryOp := fns[0].(UnaryOp)
	expr.Expr = exprTyped
	expr.fn = unaryOp
	expr.typ = unaryOp.returnType()(typedSubExprs)
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr DefaultVal) TypeCheck(_ *SemaContext, desired types.T) (TypedExpr, error) {
	return nil, errInvalidDefaultUsage
}

// TypeCheck implements the Expr interface.
func (expr MinVal) TypeCheck(_ *SemaContext, desired types.T) (TypedExpr, error) {
	return nil, errInvalidMinUsage
}

// TypeCheck implements the Expr interface.
func (expr MaxVal) TypeCheck(_ *SemaContext, desired types.T) (TypedExpr, error) {
	return nil, errInvalidMaxUsage
}

// TypeCheck implements the Expr interface.
func (expr *NumVal) TypeCheck(ctx *SemaContext, desired types.T) (TypedExpr, error) {
	return typeCheckConstant(expr, ctx, desired)
}

// TypeCheck implements the Expr interface.
func (expr *StrVal) TypeCheck(ctx *SemaContext, desired types.T) (TypedExpr, error) {
	return typeCheckConstant(expr, ctx, desired)
}

// TypeCheck implements the Expr interface.
func (expr *Tuple) TypeCheck(ctx *SemaContext, desired types.T) (TypedExpr, error) {
	expr.types = make(types.TTuple, len(expr.Exprs))
	for i, subExpr := range expr.Exprs {
		desiredElem := types.Any
		if t, ok := desired.(types.TTuple); ok && len(t) > i {
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

var errAmbiguousArrayType = pgerror.NewErrorf(pgerror.CodeIndeterminateDatatypeError, "cannot determine type of empty array. "+
	"Consider annotating with the desired type, for example ARRAY[]:::int[]")

// TypeCheck implements the Expr interface.
func (expr *Array) TypeCheck(ctx *SemaContext, desired types.T) (TypedExpr, error) {
	desiredParam := types.Any
	if arr, ok := desired.(types.TArray); ok {
		desiredParam = arr.Typ
	}

	if len(expr.Exprs) == 0 {
		if desiredParam == types.Any {
			return nil, errAmbiguousArrayType
		}
		expr.typ = types.TArray{Typ: desiredParam}
		return expr, nil
	}

	typedSubExprs, typ, err := TypeCheckSameTypedExprs(ctx, desiredParam, expr.Exprs...)
	if err != nil {
		return nil, err
	}

	expr.typ = types.TArray{Typ: typ}
	for i := range typedSubExprs {
		expr.Exprs[i] = typedSubExprs[i]
	}
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *ArrayFlatten) TypeCheck(ctx *SemaContext, desired types.T) (TypedExpr, error) {
	desiredParam := types.Any
	if arr, ok := desired.(types.TArray); ok {
		desiredParam = arr.Typ
	}

	subqueryTyped, err := expr.Subquery.TypeCheck(ctx, desiredParam)
	if err != nil {
		return nil, err
	}
	expr.Subquery = subqueryTyped
	expr.typ = types.TArray{Typ: subqueryTyped.ResolvedType()}
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *Placeholder) TypeCheck(ctx *SemaContext, desired types.T) (TypedExpr, error) {
	// Perform placeholder typing. This function is only called during Prepare,
	// when there are no available values for the placeholders yet, because
	// during Execute all placeholders are replaced from the AST before type
	// checking.
	if typ, ok := ctx.Placeholders.Type(expr.Name, true); ok {
		if !desired.Equivalent(typ) {
			// This indicates there's a conflict between what the type system thinks
			// the type for this position should be, and the actual type of the
			// placeholder. This actual placeholder type could be either a type hint
			// (from pgwire or from a SQL PREPARE), or the actual value type.
			//
			// To resolve this situation, we *override* the placeholder type with what
			// the type system expects. Then, when the value is actually sent to us
			// later, we cast the input value (whose type is the expected type) to the
			// desired type here.
			typ = desired
		}
		// We call SetType regardless of the above condition to inform the
		// placeholder struct that this placeholder is locked to its type and cannot
		// be overridden again.
		if err := ctx.Placeholders.SetType(expr.Name, typ); err != nil {
			return nil, err
		}
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
func (d *DBool) TypeCheck(_ *SemaContext, _ types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DInt) TypeCheck(_ *SemaContext, _ types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DFloat) TypeCheck(_ *SemaContext, _ types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DDecimal) TypeCheck(_ *SemaContext, _ types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DString) TypeCheck(_ *SemaContext, _ types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DCollatedString) TypeCheck(_ *SemaContext, _ types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DBytes) TypeCheck(_ *SemaContext, _ types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DUuid) TypeCheck(_ *SemaContext, _ types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DIPAddr) TypeCheck(_ *SemaContext, _ types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DDate) TypeCheck(_ *SemaContext, _ types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTime) TypeCheck(_ *SemaContext, _ types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTimestamp) TypeCheck(_ *SemaContext, _ types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTimestampTZ) TypeCheck(_ *SemaContext, _ types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DInterval) TypeCheck(_ *SemaContext, _ types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DJSON) TypeCheck(_ *SemaContext, _ types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTuple) TypeCheck(_ *SemaContext, _ types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DArray) TypeCheck(_ *SemaContext, _ types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTable) TypeCheck(_ *SemaContext, _ types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DOid) TypeCheck(_ *SemaContext, _ types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DOidWrapper) TypeCheck(_ *SemaContext, _ types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d dNull) TypeCheck(_ *SemaContext, desired types.T) (TypedExpr, error) { return d, nil }

// typeCheckAndRequireTupleElems asserts that all elements in the Tuple
// can be typed as required and are equivalent to required. Note that one would invoke
// with the required element type and NOT types.TTuple (as opposed to how Tuple.TypeCheck operates).
// For example, (1, 2.5) with required types.Decimal would raise a sane error whereas (1.0, 2.5)
// with required types.Decimal would pass.
//
// It is only valid to pass in a Tuple expression
func typeCheckAndRequireTupleElems(
	ctx *SemaContext, expr Expr, required types.T,
) (TypedExpr, error) {
	tuple := expr.(*Tuple)
	tuple.types = make(types.TTuple, len(tuple.Exprs))
	for i, subExpr := range tuple.Exprs {
		// Require that the sub expression is equivalent (or may be inferred) to the required type.
		typedExpr, err := typeCheckAndRequire(ctx, subExpr, required, "tuple element")
		if err != nil {
			return nil, err
		}
		tuple.Exprs[i] = typedExpr
		tuple.types[i] = typedExpr.ResolvedType()
	}
	return tuple, nil
}

func typeCheckAndRequireBoolean(ctx *SemaContext, expr Expr, op string) (TypedExpr, error) {
	return typeCheckAndRequire(ctx, expr, types.Bool, op)
}

func typeCheckAndRequire(
	ctx *SemaContext, expr Expr, required types.T, op string,
) (TypedExpr, error) {
	typedExpr, err := expr.TypeCheck(ctx, required)
	if err != nil {
		return nil, err
	}
	if typ := typedExpr.ResolvedType(); !(typ == types.Unknown || typ.Equivalent(required)) {
		return nil, pgerror.NewErrorf(pgerror.CodeDatatypeMismatchError, "incompatible %s type: %s", op, typ)
	}
	return typedExpr, nil
}

const (
	compSignatureFmt          = "<%s> %s <%s>"
	compSignatureWithSubOpFmt = "<%s> %s %s <%s>"
	compExprsFmt              = "%s %s %s: %v"
	compExprsWithSubOpFmt     = "%s %s %s %s: %v"
	unsupportedCompErrFmt     = "unsupported comparison operator: %s"
	unsupportedUnaryOpErrFmt  = "unsupported unary operator: %s"
	unsupportedBinaryOpErrFmt = "unsupported binary operator: %s"
	ambiguousCompErrFmt       = "ambiguous comparison operator: %s"
	ambiguousUnaryOpErrFmt    = "ambiguous unary operator: %s"
	ambiguousBinaryOpErrFmt   = "ambiguous binary operator: %s"
	candidatesHintFmt         = "candidates are:\n%s"
)

func typeCheckComparisonOpWithSubOperator(
	ctx *SemaContext, op, subOp ComparisonOperator, left, right Expr,
) (_ TypedExpr, _ TypedExpr, _ CmpOp, alwaysNull bool, _ error) {
	// Parentheses are semantically unimportant and can be removed/replaced
	// with its nested expression in our plan. This makes type checking cleaner.
	left = StripParens(left)
	right = StripParens(right)

	// Determine the set of comparisons are possible for the sub-operation,
	// which will be memoized.
	foldedOp, _, _, _, _ := foldComparisonExpr(subOp, nil, nil)
	ops := CmpOps[foldedOp]

	var cmpTypeLeft, cmpTypeRight types.T
	var leftTyped, rightTyped TypedExpr
	if array, isConstructor := right.(*Array); isConstructor {
		// If the right expression is an (optionally nested) array constructor, we
		// perform type inference on the array elements and the left expression.
		sameTypeExprs := make([]Expr, len(array.Exprs)+1)
		sameTypeExprs[0] = left
		copy(sameTypeExprs[1:], array.Exprs)

		typedSubExprs, retType, err := TypeCheckSameTypedExprs(ctx, types.Any, sameTypeExprs...)
		if err != nil {
			sigWithErr := fmt.Sprintf(compExprsWithSubOpFmt, left, subOp, op, right, err)
			return nil, nil, CmpOp{}, false,
				pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError, unsupportedCompErrFmt, sigWithErr)
		}

		// Determine TypedExpr and comparison type for left operand.
		leftTyped = typedSubExprs[0]
		cmpTypeLeft = retType

		// Determine TypedExpr and comparison type for right operand, making sure
		// all ParenExprs on the right are properly type checked.
		for i, typedExpr := range typedSubExprs[1:] {
			array.Exprs[i] = typedExpr
		}
		array.typ = types.TArray{Typ: retType}

		rightTyped = array
		cmpTypeRight = retType

		// Return early without looking up a CmpOp if the comparison type is types.Null.
		if leftTyped.ResolvedType() == types.Unknown || retType == types.Unknown {
			return leftTyped, rightTyped, CmpOp{}, true /* alwaysNull */, nil
		}
	} else {
		// If the right expression is not an array constructor, we type the left
		// expression in isolation.
		var err error
		leftTyped, err = left.TypeCheck(ctx, types.Any)
		if err != nil {
			return nil, nil, CmpOp{}, false, err
		}
		cmpTypeLeft = leftTyped.ResolvedType()

		if tuple, ok := right.(*Tuple); ok {
			// If right expression is a tuple, we require that all elements' inferred
			// type is equivalent to the left's type.
			rightTyped, err = typeCheckAndRequireTupleElems(ctx, tuple, cmpTypeLeft)
			if err != nil {
				return nil, nil, CmpOp{}, false, err
			}
		} else {
			// Try to type the right expression as an array of the left's type.
			// If right is an sql.subquery Expr, it should already be typed.
			// TODO(richardwu): If right is a subquery, we should really
			// propagate the left type as a desired type for the result column.
			rightTyped, err = right.TypeCheck(ctx, types.TArray{Typ: cmpTypeLeft})
			if err != nil {
				return nil, nil, CmpOp{}, false, err
			}
		}

		rightReturn := rightTyped.ResolvedType()
		if cmpTypeLeft == types.Unknown || rightReturn == types.Unknown {
			return leftTyped, rightTyped, CmpOp{}, true /* alwaysNull */, nil
		}

		switch rightUnwrapped := types.UnwrapType(rightReturn).(type) {
		case types.TArray:
			cmpTypeRight = rightUnwrapped.Typ
		case types.TTuple:
			if len(rightUnwrapped) == 0 {
				// Literal tuple contains no elements (subquery tuples always contain
				// one and only one element since subqueries are asserted to return
				// one column of results in analyzeExpr in analyze.go).
				return nil, nil, CmpOp{}, false, subOpCompError(cmpTypeLeft, rightReturn, subOp, op)
			}
			// Literal tuples were type checked such that all elements have equivalent types.
			// Subqueries only contain one element from analyzeExpr in analyze.go.
			// Therefore, we can take the first element's type as the right type.
			cmpTypeRight = rightUnwrapped[0]
		default:
			sigWithErr := fmt.Sprintf(compExprsWithSubOpFmt, left, subOp, op, right,
				fmt.Sprintf("op %s <right> requires array, tuple or subquery on right side", op))
			return nil, nil, CmpOp{}, false, pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError, unsupportedCompErrFmt, sigWithErr)
		}
	}
	fn, ok := ops.lookupImpl(cmpTypeLeft, cmpTypeRight)
	if !ok {
		return nil, nil, CmpOp{}, false, subOpCompError(cmpTypeLeft, rightTyped.ResolvedType(), subOp, op)
	}
	return leftTyped, rightTyped, fn, false, nil
}

func subOpCompError(leftType, rightType types.T, subOp, op ComparisonOperator) *pgerror.Error {
	sig := fmt.Sprintf(compSignatureWithSubOpFmt, leftType, subOp, op, rightType)
	return pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError, unsupportedCompErrFmt, sig)
}

// typeCheckSubqueryWithIn checks the case where the right side of an IN
// expression is a subquery.
func typeCheckSubqueryWithIn(left, right types.T) error {
	if rTuple, ok := right.(types.TTuple); ok {
		// Subqueries come through as a tuple{T}, so T IN tuple{T} should be
		// accepted.
		if len(rTuple) != 1 {
			return pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError,
				unsupportedCompErrFmt, fmt.Sprintf(compSignatureFmt, left, In, right))
		}
		if !left.Equivalent(rTuple[0]) {
			return pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError,
				unsupportedCompErrFmt, fmt.Sprintf(compSignatureFmt, left, In, right))
		}
	}
	return nil
}

func typeCheckComparisonOp(
	ctx *SemaContext, op ComparisonOperator, left, right Expr,
) (_ TypedExpr, _ TypedExpr, _ CmpOp, alwaysNull bool, _ error) {
	foldedOp, foldedLeft, foldedRight, switched, _ := foldComparisonExpr(op, left, right)
	ops := CmpOps[foldedOp]

	_, leftIsTuple := foldedLeft.(*Tuple)
	rightTuple, rightIsTuple := foldedRight.(*Tuple)
	switch {
	case foldedOp == In && rightIsTuple:
		sameTypeExprs := make([]Expr, len(rightTuple.Exprs)+1)
		sameTypeExprs[0] = foldedLeft
		copy(sameTypeExprs[1:], rightTuple.Exprs)

		typedSubExprs, retType, err := TypeCheckSameTypedExprs(ctx, types.Any, sameTypeExprs...)
		if err != nil {
			sigWithErr := fmt.Sprintf(compExprsFmt, left, op, right, err)
			return nil, nil, CmpOp{}, false,
				pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError, unsupportedCompErrFmt, sigWithErr)
		}

		fn, ok := ops.lookupImpl(retType, types.FamTuple)
		if !ok {
			sig := fmt.Sprintf(compSignatureFmt, retType, op, types.FamTuple)
			return nil, nil, CmpOp{}, false,
				pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError, unsupportedCompErrFmt, sig)
		}

		typedLeft := typedSubExprs[0]
		typedSubExprs = typedSubExprs[1:]

		rightTuple.types = make(types.TTuple, len(typedSubExprs))
		for i, typedExpr := range typedSubExprs {
			rightTuple.Exprs[i] = typedExpr
			rightTuple.types[i] = retType
		}
		if switched {
			return rightTuple, typedLeft, fn, false, nil
		}
		return typedLeft, rightTuple, fn, false, nil

	case leftIsTuple && rightIsTuple:
		fn, ok := ops.lookupImpl(types.FamTuple, types.FamTuple)
		if !ok {
			sig := fmt.Sprintf(compSignatureFmt, types.FamTuple, op, types.FamTuple)
			return nil, nil, CmpOp{}, false,
				pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError, unsupportedCompErrFmt, sig)
		}
		// Using non-folded left and right to avoid having to swap later.
		typedLeft, typedRight, err := typeCheckTupleComparison(ctx, op, left.(*Tuple), right.(*Tuple))
		if err != nil {
			return nil, nil, CmpOp{}, false, err
		}
		return typedLeft, typedRight, fn, false, nil
	}

	// For comparisons, we do not stimulate the typing of untyped NULL with the
	// other side's type, because comparisons of NULL with anything else are
	// defined to return NULL anyways. Should the SQL dialect ever be extended with
	// comparisons that can return non-NULL on NULL input, the `inBinOp` parameter
	// may need altering.
	typedSubExprs, fns, err := typeCheckOverloadedExprs(
		ctx, types.Any, ops, true /* inBinOp */, foldedLeft, foldedRight,
	)
	if err != nil {
		return nil, nil, CmpOp{}, false, err
	}

	leftExpr, rightExpr := typedSubExprs[0], typedSubExprs[1]
	if switched {
		leftExpr, rightExpr = rightExpr, leftExpr
	}
	leftReturn := leftExpr.ResolvedType()
	rightReturn := rightExpr.ResolvedType()

	if foldedOp == In {
		if err := typeCheckSubqueryWithIn(leftReturn, rightReturn); err != nil {
			return nil, nil, CmpOp{}, false, err
		}
	}

	// Return early if at least one overload is possible, NULL is an argument,
	// and none of the overloads accept NULL.
	if leftReturn == types.Unknown || rightReturn == types.Unknown {
		if len(fns) > 0 {
			noneAcceptNull := true
			for _, e := range fns {
				if e.(CmpOp).NullableArgs {
					noneAcceptNull = false
					break
				}
			}
			if noneAcceptNull {
				return leftExpr, rightExpr, CmpOp{}, true /* alwaysNull */, err
			}
		}
	}

	// Throw a typing error if overload resolution found either no compatible candidates
	// or if it found an ambiguity.
	collationMismatch := leftReturn.FamilyEqual(types.FamCollatedString) && !leftReturn.Equivalent(rightReturn)
	if len(fns) != 1 || collationMismatch {
		sig := fmt.Sprintf(compSignatureFmt, leftReturn, op, rightReturn)
		if len(fns) == 0 || collationMismatch {
			return nil, nil, CmpOp{}, false,
				pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError, unsupportedCompErrFmt, sig)
		}
		fnsStr := formatCandidates(op.String(), fns)
		return nil, nil, CmpOp{}, false,
			pgerror.NewErrorf(pgerror.CodeAmbiguousFunctionError,
				ambiguousCompErrFmt, sig).SetHintf(candidatesHintFmt, fnsStr)
	}

	return leftExpr, rightExpr, fns[0].(CmpOp), false, nil
}

type typeCheckExprsState struct {
	ctx *SemaContext

	exprs           []Expr
	typedExprs      []TypedExpr
	constIdxs       []int // index into exprs/typedExprs
	placeholderIdxs []int // index into exprs/typedExprs
	resolvableIdxs  []int // index into exprs/typedExprs
}

// TypeCheckSameTypedExprs type checks a list of expressions, asserting that all
// resolved TypeExprs have the same type. An optional desired type can be provided,
// which will hint that type which the expressions should resolve to, if possible.
func TypeCheckSameTypedExprs(
	ctx *SemaContext, desired types.T, exprs ...Expr,
) ([]TypedExpr, types.T, error) {
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

	constIdxs, placeholderIdxs, resolvableIdxs := typeCheckSplitExprs(ctx, exprs)

	s := typeCheckExprsState{
		ctx:             ctx,
		exprs:           exprs,
		typedExprs:      typedExprs,
		constIdxs:       constIdxs,
		placeholderIdxs: placeholderIdxs,
		resolvableIdxs:  resolvableIdxs,
	}

	switch {
	case len(resolvableIdxs) == 0 && len(constIdxs) == 0:
		if err := typeCheckSameTypedPlaceholders(s, desired); err != nil {
			return nil, nil, err
		}
		return typedExprs, desired, nil
	case len(resolvableIdxs) == 0:
		return typeCheckConstsAndPlaceholdersWithDesired(s, desired)
	default:
		firstValidIdx := -1
		firstValidType := types.Unknown
		for i, j := range resolvableIdxs {
			typedExpr, err := exprs[j].TypeCheck(ctx, desired)
			if err != nil {
				return nil, nil, err
			}
			typedExprs[j] = typedExpr
			if returnType := typedExpr.ResolvedType(); returnType != types.Unknown {
				firstValidType = returnType
				firstValidIdx = i
				break
			}
		}

		if firstValidType == types.Unknown {
			switch {
			case len(constIdxs) > 0:
				return typeCheckConstsAndPlaceholdersWithDesired(s, desired)
			case len(placeholderIdxs) > 0:
				return nil, nil, placeholderTypeAmbiguityError{s.exprs[placeholderIdxs[0]].(*Placeholder)}
			default:
				return typedExprs, types.Unknown, nil
			}
		}

		for _, i := range resolvableIdxs[firstValidIdx+1:] {
			typedExpr, err := exprs[i].TypeCheck(ctx, firstValidType)
			if err != nil {
				return nil, nil, err
			}
			if typ := typedExpr.ResolvedType(); !(typ.Equivalent(firstValidType) || typ == types.Unknown) {
				return nil, nil, unexpectedTypeError{exprs[i], firstValidType, typ}
			}
			typedExprs[i] = typedExpr
		}
		if len(constIdxs) > 0 {
			if _, err := typeCheckSameTypedConsts(s, firstValidType, true); err != nil {
				return nil, nil, err
			}
		}
		if len(placeholderIdxs) > 0 {
			if err := typeCheckSameTypedPlaceholders(s, firstValidType); err != nil {
				return nil, nil, err
			}
		}
		return typedExprs, firstValidType, nil
	}
}

// Used to set placeholders to the desired typ.
func typeCheckSameTypedPlaceholders(s typeCheckExprsState, typ types.T) error {
	for _, i := range s.placeholderIdxs {
		typedExpr, err := typeCheckAndRequire(s.ctx, s.exprs[i], typ, "placeholder")
		if err != nil {
			return err
		}
		s.typedExprs[i] = typedExpr
	}
	return nil
}

// Used to type check constants to the same type. An optional typ can be
// provided to signify the desired shared type, which can be set to the
// required shared type using the second parameter.
func typeCheckSameTypedConsts(s typeCheckExprsState, typ types.T, required bool) (types.T, error) {
	setTypeForConsts := func(typ types.T) (types.T, error) {
		for _, i := range s.constIdxs {
			typedExpr, err := typeCheckAndRequire(s.ctx, s.exprs[i], typ, "constant")
			if err != nil {
				// In this case, even though the constExpr has been shown to be
				// upcastable to typ based on canConstantBecome, it can't actually be
				// parsed as typ.
				return nil, err
			}
			s.typedExprs[i] = typedExpr
		}
		return typ, nil
	}

	// If typ is not a wildcard, all consts try to become typ.
	if typ != types.Any {
		all := true
		for _, i := range s.constIdxs {
			if !canConstantBecome(s.exprs[i].(Constant), typ) {
				if required {
					typedExpr, err := s.exprs[i].TypeCheck(s.ctx, types.Any)
					if err != nil {
						return nil, err
					}
					return nil, unexpectedTypeError{s.exprs[i], typ, typedExpr.ResolvedType()}
				}
				all = false
				break
			}
		}
		if all {
			return setTypeForConsts(typ)
		}
	}

	// If not all constIdxs could become typ but they have a mutual
	// resolvable type, use this common type.
	if bestType, ok := commonConstantType(s.exprs, s.constIdxs); ok {
		return setTypeForConsts(bestType)
	}

	// If not, we want to force an error because the constants cannot all
	// become the same type.
	reqTyp := typ
	for _, i := range s.constIdxs {
		typedExpr, err := s.exprs[i].TypeCheck(s.ctx, reqTyp)
		if err != nil {
			return nil, err
		}
		if typ := typedExpr.ResolvedType(); !typ.Equivalent(reqTyp) {
			return nil, unexpectedTypeError{s.exprs[i], reqTyp, typ}
		}
		if reqTyp == types.Any {
			reqTyp = typedExpr.ResolvedType()
		}
	}
	panic("should throw error above")
}

// Used to type check all constants with the optional desired type. The
// type that is chosen here will then be set to any placeholders.
func typeCheckConstsAndPlaceholdersWithDesired(
	s typeCheckExprsState, desired types.T,
) ([]TypedExpr, types.T, error) {
	typ, err := typeCheckSameTypedConsts(s, desired, false)
	if err != nil {
		return nil, nil, err
	}
	if len(s.placeholderIdxs) > 0 {
		if err := typeCheckSameTypedPlaceholders(s, typ); err != nil {
			return nil, nil, err
		}
	}
	return s.typedExprs, typ, nil
}

// typeCheckSplitExprs splits the expressions into three groups of indexes:
// - Constants
// - Placeholders
// - All other Exprs
func typeCheckSplitExprs(
	ctx *SemaContext, exprs []Expr,
) (constIdxs []int, placeholderIdxs []int, resolvableIdxs []int) {
	for i, expr := range exprs {
		switch {
		case isConstant(expr):
			constIdxs = append(constIdxs, i)
		case ctx.isUnresolvedPlaceholder(expr):
			placeholderIdxs = append(placeholderIdxs, i)
		default:
			resolvableIdxs = append(resolvableIdxs, i)
		}
	}
	return constIdxs, placeholderIdxs, resolvableIdxs
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
	left.types = make(types.TTuple, tupLen)
	right.types = make(types.TTuple, tupLen)
	for elemIdx := range left.Exprs {
		leftSubExpr := left.Exprs[elemIdx]
		rightSubExpr := right.Exprs[elemIdx]
		leftSubExprTyped, rightSubExprTyped, _, _, err := typeCheckComparisonOp(ctx, op, leftSubExpr, rightSubExpr)
		if err != nil {
			exps := Exprs([]Expr{left, right})
			return nil, nil, pgerror.NewErrorf(pgerror.CodeDatatypeMismatchError, "tuples %s are not comparable at index %d: %s",
				&exps, elemIdx+1, err)
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
	ctx *SemaContext, desired types.T, exprs ...Expr,
) ([]TypedExpr, types.T, error) {
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
	var desiredTuple types.TTuple
	if t, ok := desired.(types.TTuple); ok {
		desiredTuple = t
	}

	// All expressions at the same indexes must be the same type.
	resTypes := make(types.TTuple, firstLen)
	sameTypeExprs := make([]Expr, len(exprs))
	for elemIdx := range first.Exprs {
		for tupleIdx, expr := range exprs {
			sameTypeExprs[tupleIdx] = expr.(*Tuple).Exprs[elemIdx]
		}
		desiredElem := types.Any
		if len(desiredTuple) > elemIdx {
			desiredElem = desiredTuple[elemIdx]
		}
		typedSubExprs, resType, err := TypeCheckSameTypedExprs(ctx, desiredElem, sameTypeExprs...)
		if err != nil {
			return nil, nil, pgerror.NewErrorf(pgerror.CodeDatatypeMismatchError, "tuples %s are not the same type: %v", Exprs(exprs), err)
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
			typedExpr, err := expr.TypeCheck(ctx, types.Any)
			if err != nil {
				return err
			}
			return unexpectedTypeError{expr, types.FamTuple, typedExpr.ResolvedType()}
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
		return pgerror.NewErrorf(pgerror.CodeDatatypeMismatchError, "expected tuple %v to have a length of %d", t, expectedLen)
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
	typ            types.T
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
				return pgerror.NewErrorf(pgerror.CodeDatatypeMismatchError, "found type annotation around %s that conflicts with previously "+
					"inferred type %s", placeholder, prevType)
			}
			return pgerror.NewErrorf(pgerror.CodeDatatypeMismatchError, "found multiple conflicting type annotations around %s", placeholder)
		}
	}
	return nil
}

// StripMemoizedFuncs strips memoized function references from expression trees.
// This is necessary to permit equality checks using reflect.DeepEqual.
// Used in testing.
func StripMemoizedFuncs(expr Expr) Expr {
	expr, _ = WalkExpr(stripFuncsVisitor{}, expr)
	return expr
}

type stripFuncsVisitor struct{}

func (v stripFuncsVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	switch t := expr.(type) {
	case *UnaryExpr:
		t.fn = UnaryOp{}
	case *BinaryExpr:
		t.fn = BinOp{}
	case *ComparisonExpr:
		t.fn = CmpOp{}
	case *FuncExpr:
		t.fn = nil
	}
	return true, expr
}

func (stripFuncsVisitor) VisitPost(expr Expr) Expr { return expr }
