// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/text/language"
)

// SemaContext defines the context in which to perform semantic analysis on an
// expression syntax tree.
type SemaContext struct {
	// Annotations augments the AST with extra information.
	Annotations Annotations

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

	// AsOfTimestamp denotes the explicit AS OF SYSTEM TIME timestamp for the
	// query, if any. If the query is not an AS OF SYSTEM TIME query,
	// AsOfTimestamp is nil.
	// TODO(knz): we may want to support table readers at arbitrary
	// timestamps, so that each FROM clause can have its own
	// timestamp. In that case, the timestamp would not be set
	// globally for the entire txn and this field would not be needed.
	AsOfTimestamp *hlc.Timestamp

	Properties SemaProperties
}

// SemaProperties is a holder for required and derived properties
// during semantic analysis. It provides scoping semantics via its
// Restore() method, see below.
type SemaProperties struct {
	// required constraints type checking to only accept certain kinds
	// of expressions. See SetConstraint
	required semaRequirements

	// Derived is populated during semantic analysis with properties
	// from the expression being analyzed.  The caller is responsible
	// for re-initializing this when needed.
	Derived ScalarProperties
}

type semaRequirements struct {
	// context is the name of the semantic anlysis context, for use in
	// error messages.
	context string

	// The various reject flags reject specific forms of scalar
	// expressions. The default for this struct with false everywhere
	// ensures that anything is allowed.
	rejectFlags SemaRejectFlags
}

// Require resets the derived properties and sets required constraints.
func (s *SemaProperties) Require(context string, rejectFlags SemaRejectFlags) {
	s.required.context = context
	s.required.rejectFlags = rejectFlags
	s.Derived.Clear()
}

// IsSet checks if the given rejectFlag is set as a required property.
func (s *SemaProperties) IsSet(rejectFlags SemaRejectFlags) bool {
	return s.required.rejectFlags&rejectFlags != 0
}

// Restore restores a copy of a SemaProperties. Use with:
// defer semaCtx.Properties.Restore(semaCtx.Properties)
func (s *SemaProperties) Restore(orig SemaProperties) {
	*s = orig
}

// SemaRejectFlags contains flags to filter out certain kinds of
// expressions.
type SemaRejectFlags int

// Valid values for SemaRejectFlags.
const (
	// RejectAggregates rejects min(), max(), etc.
	RejectAggregates SemaRejectFlags = 1 << iota

	// RejectNestedAggregates rejects any use of aggregates inside the
	// argument list of another function call, which can itself be an aggregate
	// (RejectAggregates notwithstanding).
	RejectNestedAggregates

	// RejectNestedWindows rejects any use of window functions inside the
	// argument list of another window function.
	RejectNestedWindowFunctions

	// RejectWindowApplications rejects "x() over y", etc.
	RejectWindowApplications

	// RejectGenerators rejects any use of SRFs, e.g "generate_series()".
	RejectGenerators

	// RejectNestedGenerators rejects any use of SRFs inside the
	// argument list of another function call, which can itself be a SRF
	// (RejectGenerators notwithstanding).
	// This is used e.g. when processing the calls inside ROWS FROM.
	RejectNestedGenerators

	// RejectImpureFunctions rejects any non-const functions like now().
	RejectImpureFunctions

	// RejectSubqueries rejects subqueries in scalar contexts.
	RejectSubqueries

	// RejectSpecial is used in common places like the LIMIT clause.
	RejectSpecial SemaRejectFlags = RejectAggregates | RejectGenerators | RejectWindowApplications
)

// ScalarProperties contains the properties of the current scalar
// expression discovered during semantic analysis. The properties
// are collected prior to simplification, so some of the properties
// may not hold anymore by the time semantic analysis completes.
type ScalarProperties struct {
	// SeenAggregate is set to true if the expression originally
	// contained an aggregation.
	SeenAggregate bool

	// SeenWindowApplication is set to true if the expression originally
	// contained a window function.
	SeenWindowApplication bool

	// SeenGenerator is set to true if the expression originally
	// contained a SRF.
	SeenGenerator bool

	// SeenImpureFunctions is set to true if the expression originally
	// contained an impure function.
	SeenImpure bool

	// inFuncExpr is temporarily set to true while type checking the
	// parameters of a function. Used to process RejectNestedGenerators
	// properly.
	inFuncExpr bool

	// InWindowFunc is temporarily set to true while type checking the
	// parameters of a window function in order to reject nested window
	// functions.
	InWindowFunc bool
}

// Clear resets the scalar properties to defaults.
func (sp *ScalarProperties) Clear() {
	*sp = ScalarProperties{}
}

// MakeSemaContext initializes a simple SemaContext suitable
// for "lightweight" type checking such as the one performed for default
// expressions.
// Note: if queries with placeholders are going to be used,
// SemaContext.Placeholders.Init must be called separately.
func MakeSemaContext() SemaContext {
	return SemaContext{}
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
func (sc *SemaContext) GetLocation() *time.Location {
	if sc == nil || sc.Location == nil || *sc.Location == nil {
		return time.UTC
	}
	return *sc.Location
}

// GetAdditionMode implements ParseTimeContext.
func (sc *SemaContext) GetAdditionMode() duration.AdditionMode {
	return duration.AdditionModeCompatible
}

// GetRelativeParseTime implements ParseTimeContext.
func (sc *SemaContext) GetRelativeParseTime() time.Time {
	return timeutil.Now().In(sc.GetLocation())
}

func placeholderTypeAmbiguityError(idx PlaceholderIdx) error {
	return pgerror.WithCandidateCode(
		placeholderTypeAmbiguityErr{idx},
		pgcode.InvalidParameterValue)
}

type placeholderTypeAmbiguityErr struct {
	idx PlaceholderIdx
}

func (err placeholderTypeAmbiguityErr) Error() string {
	return fmt.Sprintf("could not determine data type of placeholder %s", err.idx)
}

func unexpectedTypeError(expr Expr, want, got *types.T) error {
	return pgerror.Newf(pgcode.InvalidParameterValue,
		"expected %s to be of type %s, found type %s", expr, errors.Safe(want), errors.Safe(got))
}

func decorateTypeCheckError(err error, format string, a ...interface{}) error {
	if e, ok := errors.UnwrapAll(err).(placeholderTypeAmbiguityErr); ok {
		return e
	}
	return pgerror.Wrapf(err, pgcode.InvalidParameterValue, format, a...)
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
func TypeCheck(expr Expr, ctx *SemaContext, desired *types.T) (TypedExpr, error) {
	if desired == nil {
		return nil, errors.AssertionFailedf(
			"the desired type for tree.TypeCheck cannot be nil, use types.Any instead: %T", expr)
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
	expr Expr, ctx *SemaContext, required *types.T, op string,
) (TypedExpr, error) {
	typedExpr, err := TypeCheck(expr, ctx, required)
	if err != nil {
		return nil, err
	}
	if typ := typedExpr.ResolvedType(); !(typ.Equivalent(required) || typ.Family() == types.UnknownFamily) {
		return typedExpr, pgerror.Newf(
			pgcode.DatatypeMismatch, "argument of %s must be type %s, not type %s", op, required, typ)
	}
	return typedExpr, nil
}

// TypeCheck implements the Expr interface.
func (expr *AndExpr) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
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
func (expr *BinaryExpr) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
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
	if leftReturn.Family() == types.UnknownFamily || rightReturn.Family() == types.UnknownFamily {
		if len(fns) > 0 {
			noneAcceptNull := true
			for _, e := range fns {
				if e.(*BinOp).NullableArgs {
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
		if desired.Family() != types.AnyFamily {
			desStr = fmt.Sprintf(" (desired <%s>)", desired)
		}
		sig := fmt.Sprintf("<%s> %s <%s>%s", leftReturn, expr.Operator, rightReturn, desStr)
		if len(fns) == 0 {
			return nil,
				pgerror.Newf(pgcode.InvalidParameterValue, unsupportedBinaryOpErrFmt, sig)
		}
		fnsStr := formatCandidates(expr.Operator.String(), fns)
		err = pgerror.Newf(pgcode.AmbiguousFunction, ambiguousBinaryOpErrFmt, sig)
		err = errors.WithHintf(err, candidatesHintFmt, fnsStr)
		return nil, err
	}

	binOp := fns[0].(*BinOp)

	// Register operator usage in telemetry.
	if binOp.counter != nil {
		telemetry.Inc(binOp.counter)
	}

	expr.Left, expr.Right = leftTyped, rightTyped
	expr.fn = binOp
	expr.typ = binOp.returnType()(typedSubExprs)
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *CaseExpr) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
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

func isCastDeepValid(castFrom, castTo *types.T) (bool, telemetry.Counter) {
	if castTo.Family() == types.ArrayFamily && castFrom.Family() == types.ArrayFamily {
		ok, c := isCastDeepValid(castFrom.ArrayContents(), castTo.ArrayContents())
		if ok {
			telemetry.Inc(sqltelemetry.ArrayCastCounter)
		}
		return ok, c
	}
	for _, t := range validCastTypes(castTo) {
		if castFrom.Family() == t.fromT.Family() {
			return true, t.counter
		}
	}
	return false, nil
}

func isEmptyArray(expr Expr) bool {
	a, ok := expr.(*Array)
	return ok && len(a.Exprs) == 0
}

// TypeCheck implements the Expr interface.
func (expr *CastExpr) TypeCheck(ctx *SemaContext, _ *types.T) (TypedExpr, error) {
	// The desired type provided to a CastExpr is ignored. Instead,
	// types.Any is passed to the child of the cast. There are two
	// exceptions, described below.
	desired := types.Any
	switch {
	case isConstant(expr.Expr):
		if canConstantBecome(expr.Expr.(Constant), expr.Type) {
			// If a Constant is subject to a cast which it can naturally become (which
			// is in its resolvable type set), we desire the cast's type for the Constant,
			// which will result in the CastExpr becoming an identity cast.
			desired = expr.Type

			// If the type doesn't have any possible parameters (like length,
			// precision), the CastExpr becomes a no-op and can be elided.
			switch expr.Type.Family() {
			case types.BoolFamily, types.DateFamily, types.TimeFamily, types.TimestampFamily, types.TimestampTZFamily,
				types.IntervalFamily, types.BytesFamily:
				return expr.Expr.TypeCheck(ctx, expr.Type)
			}
		}
	case ctx.isUnresolvedPlaceholder(expr.Expr):
		// This case will be triggered if ProcessPlaceholderAnnotations found
		// the same placeholder in another location where it was either not
		// the child of a cast, or was the child of a cast to a different type.
		// In this case, we default to inferring a STRING for the placeholder.
		desired = types.String
	case isEmptyArray(expr.Expr):
		// An empty array can't be type-checked with a desired parameter of
		// types.Any. If we're going to cast to another array type, which is a
		// common pattern in SQL (select array[]::int[]), use the cast type as the
		// the desired type.
		if expr.Type.Family() == types.ArrayFamily {
			desired = expr.Type
		}
	}

	typedSubExpr, err := expr.Expr.TypeCheck(ctx, desired)
	if err != nil {
		return nil, err
	}

	castFrom := typedSubExpr.ResolvedType()

	if ok, c := isCastDeepValid(castFrom, expr.Type); ok {
		telemetry.Inc(c)
		expr.Expr = typedSubExpr
		expr.typ = expr.Type
		return expr, nil
	}

	return nil, pgerror.Newf(pgcode.CannotCoerce, "invalid cast: %s -> %s", castFrom, expr.Type)
}

// TypeCheck implements the Expr interface.
func (expr *IndirectionExpr) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
	for i, t := range expr.Indirection {
		if t.Slice {
			return nil, unimplemented.NewWithIssuef(32551, "ARRAY slicing in %s", expr)
		}
		if i > 0 {
			return nil, unimplemented.NewWithIssueDetailf(32552, "ind", "multidimensional indexing: %s", expr)
		}

		beginExpr, err := typeCheckAndRequire(ctx, t.Begin, types.Int, "ARRAY subscript")
		if err != nil {
			return nil, err
		}
		t.Begin = beginExpr
	}

	subExpr, err := expr.Expr.TypeCheck(ctx, types.MakeArray(desired))
	if err != nil {
		return nil, err
	}
	typ := subExpr.ResolvedType()
	if typ.Family() != types.ArrayFamily {
		return nil, pgerror.Newf(pgcode.DatatypeMismatch, "cannot subscript type %s because it is not an array", typ)
	}
	expr.Expr = subExpr
	expr.typ = typ.ArrayContents()

	telemetry.Inc(sqltelemetry.ArraySubscriptCounter)
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *AnnotateTypeExpr) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
	subExpr, err := typeCheckAndRequire(ctx, expr.Expr, expr.Type,
		fmt.Sprintf("type annotation for %v as %s, found", expr.Expr, expr.Type))
	if err != nil {
		return nil, err
	}
	return subExpr, nil
}

// TypeCheck implements the Expr interface.
func (expr *CollateExpr) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
	_, err := language.Parse(expr.Locale)
	if err != nil {
		return nil, pgerror.Wrapf(err, pgcode.InvalidParameterValue,
			"invalid locale %s", expr.Locale)
	}
	subExpr, err := expr.Expr.TypeCheck(ctx, types.String)
	if err != nil {
		return nil, err
	}
	t := subExpr.ResolvedType()
	if types.IsStringType(t) || t.Family() == types.UnknownFamily {
		expr.Expr = subExpr
		expr.typ = types.MakeCollatedString(types.String, expr.Locale)
		return expr, nil
	}
	return nil, pgerror.Newf(pgcode.DatatypeMismatch,
		"incompatible type for COLLATE: %s", t)
}

// NewTypeIsNotCompositeError generates an error suitable to report
// when a ColumnAccessExpr or TupleStar is applied to a non-composite
// type.
func NewTypeIsNotCompositeError(resolvedType *types.T) error {
	return pgerror.Newf(pgcode.WrongObjectType,
		"type %s is not composite", resolvedType,
	)
}

// TypeCheck implements the Expr interface.
func (expr *TupleStar) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
	subExpr, err := expr.Expr.TypeCheck(ctx, desired)
	if err != nil {
		return nil, err
	}
	expr.Expr = subExpr
	resolvedType := subExpr.ResolvedType()

	// Alghough we're going to elide the tuple star, we need to ensure
	// the expression is indeed a labeled tuple first.
	if resolvedType.Family() != types.TupleFamily || len(resolvedType.TupleLabels()) == 0 {
		return nil, NewTypeIsNotCompositeError(resolvedType)
	}

	return subExpr, err
}

// ResolvedType implements the TypedExpr interface.
func (expr *TupleStar) ResolvedType() *types.T {
	return expr.Expr.(TypedExpr).ResolvedType()
}

// TypeCheck implements the Expr interface.
func (expr *ColumnAccessExpr) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
	// If the context requires types T, we need to ask "Any tuple with
	// at least this label and the element type T for this label" from
	// the sub-expression. Of course, our type system does not support
	// this. So drop the type constraint instead.
	subExpr, err := expr.Expr.TypeCheck(ctx, types.Any)
	if err != nil {
		return nil, err
	}

	expr.Expr = subExpr
	resolvedType := subExpr.ResolvedType()

	if resolvedType.Family() != types.TupleFamily || len(resolvedType.TupleLabels()) == 0 {
		return nil, NewTypeIsNotCompositeError(resolvedType)
	}

	// Go through all of the labels to find a match.
	expr.ColIndex = -1
	for i, label := range resolvedType.TupleLabels() {
		if label == expr.ColName {
			expr.ColIndex = i
			break
		}
	}
	if expr.ColIndex < 0 {
		return nil, pgerror.Newf(pgcode.DatatypeMismatch,
			"could not identify column %q in %s",
			ErrNameStringP(&expr.ColName), resolvedType,
		)
	}

	// Optimization: if the expression is actually a tuple, then
	// simplify the tuple straight away.
	if tExpr, ok := expr.Expr.(*Tuple); ok {
		return tExpr.Exprs[expr.ColIndex].(TypedExpr), nil
	}

	// Otherwise, let the expression be, it's probably more complex.
	// Just annotate the type of the result properly.
	expr.typ = &resolvedType.TupleContents()[expr.ColIndex]
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *CoalesceExpr) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
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
func (expr *ComparisonExpr) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
	var leftTyped, rightTyped TypedExpr
	var fn *CmpOp
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

	// Register operator usage in telemetry.
	if fn.counter != nil {
		telemetry.Inc(fn.counter)
	}

	expr.Left, expr.Right = leftTyped, rightTyped
	expr.fn = fn
	expr.typ = types.Bool
	return expr, nil
}

var (
	errStarNotAllowed      = pgerror.New(pgcode.Syntax, "cannot use \"*\" in this context")
	errInvalidDefaultUsage = pgerror.New(pgcode.Syntax, "DEFAULT can only appear in a VALUES list within INSERT or on the right side of a SET")
	errInvalidMaxUsage     = pgerror.New(pgcode.Syntax, "MAXVALUE can only appear within a range partition expression")
	errInvalidMinUsage     = pgerror.New(pgcode.Syntax, "MINVALUE can only appear within a range partition expression")
	errPrivateFunction     = pgerror.New(pgcode.ReservedName, "function reserved for internal use")
)

// NewAggInAggError creates an error for the case when an aggregate function is
// contained within another aggregate function.
func NewAggInAggError() error {
	return pgerror.Newf(pgcode.Grouping, "aggregate function calls cannot be nested")
}

// NewInvalidNestedSRFError creates a rejection for a nested SRF.
func NewInvalidNestedSRFError(context string) error {
	return pgerror.Newf(pgcode.FeatureNotSupported,
		"set-returning functions must appear at the top level of %s", context)
}

// NewInvalidFunctionUsageError creates a rejection for a special function.
func NewInvalidFunctionUsageError(class FunctionClass, context string) error {
	var cat string
	var code string
	switch class {
	case AggregateClass:
		cat = "aggregate"
		code = pgcode.Grouping
	case WindowClass:
		cat = "window"
		code = pgcode.Windowing
	case GeneratorClass:
		cat = "generator"
		code = pgcode.FeatureNotSupported
	}
	return pgerror.Newf(code, "%s functions are not allowed in %s", cat, context)
}

// checkFunctionUsage checks whether a given built-in function is
// allowed in the current context.
func (sc *SemaContext) checkFunctionUsage(expr *FuncExpr, def *FunctionDefinition) error {
	if def.UnsupportedWithIssue != 0 {
		// Note: no need to embed the function name in the message; the
		// caller will add the function name as prefix.
		const msg = "this function is not supported"
		if def.UnsupportedWithIssue < 0 {
			return unimplemented.New(def.Name+"()", msg)
		}
		return unimplemented.NewWithIssueDetail(def.UnsupportedWithIssue, def.Name, msg)
	}
	if def.Private {
		return pgerror.Wrapf(errPrivateFunction, pgcode.ReservedName,
			"%s()", errors.Safe(def.Name))
	}
	if sc == nil {
		// We can't check anything further. Give up.
		return nil
	}

	if expr.IsWindowFunctionApplication() {
		if sc.Properties.required.rejectFlags&RejectWindowApplications != 0 {
			return NewInvalidFunctionUsageError(WindowClass, sc.Properties.required.context)
		}

		if sc.Properties.Derived.InWindowFunc &&
			sc.Properties.required.rejectFlags&RejectNestedWindowFunctions != 0 {
			return pgerror.Newf(pgcode.Windowing, "window function calls cannot be nested")
		}
		sc.Properties.Derived.SeenWindowApplication = true
	} else {
		// If it is an aggregate function *not used OVER a window*, then
		// we have an aggregation.
		if def.Class == AggregateClass {
			if sc.Properties.Derived.inFuncExpr &&
				sc.Properties.required.rejectFlags&RejectNestedAggregates != 0 {
				return NewAggInAggError()
			}
			if sc.Properties.required.rejectFlags&RejectAggregates != 0 {
				return NewInvalidFunctionUsageError(AggregateClass, sc.Properties.required.context)
			}
			sc.Properties.Derived.SeenAggregate = true
		}
	}
	if def.Class == GeneratorClass {
		if sc.Properties.Derived.inFuncExpr &&
			sc.Properties.required.rejectFlags&RejectNestedGenerators != 0 {
			return NewInvalidNestedSRFError(sc.Properties.required.context)
		}
		if sc.Properties.required.rejectFlags&RejectGenerators != 0 {
			return NewInvalidFunctionUsageError(GeneratorClass, sc.Properties.required.context)
		}
		sc.Properties.Derived.SeenGenerator = true
	}
	if def.Impure {
		if sc.Properties.required.rejectFlags&RejectImpureFunctions != 0 {
			// The code FeatureNotSupported is a bit misleading here,
			// because we probably can't support the feature at all. However
			// this error code matches PostgreSQL's in the same conditions.
			return pgerror.Newf(pgcode.FeatureNotSupported,
				"impure functions are not allowed in %s", sc.Properties.required.context)
		}
		sc.Properties.Derived.SeenImpure = true
	}
	return nil
}

// CheckIsWindowOrAgg returns an error if the function definition is not a
// window function or an aggregate.
func CheckIsWindowOrAgg(def *FunctionDefinition) error {
	switch def.Class {
	case AggregateClass:
	case WindowClass:
	default:
		return pgerror.Newf(pgcode.WrongObjectType,
			"OVER specified, but %s() is neither a window function nor an aggregate function",
			def.Name)
	}
	return nil
}

// TypeCheck implements the Expr interface.
func (expr *FuncExpr) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
	var searchPath sessiondata.SearchPath
	if ctx != nil {
		searchPath = ctx.SearchPath
	}
	def, err := expr.Func.Resolve(searchPath)
	if err != nil {
		return nil, err
	}

	if err := ctx.checkFunctionUsage(expr, def); err != nil {
		return nil, pgerror.Wrapf(err, pgcode.InvalidParameterValue,
			"%s()", def.Name)
	}
	if ctx != nil {
		// We'll need to remember we are in a function application to
		// generate suitable errors in checkFunctionUsage().  We cannot
		// set ctx.inFuncExpr earlier (in particular not before the call
		// to checkFunctionUsage() above) because the top-level FuncExpr
		// must be acceptable even if it is a SRF and
		// RejectNestedGenerators is set.
		defer func(ctx *SemaContext, prevFunc bool, prevWindow bool) {
			ctx.Properties.Derived.inFuncExpr = prevFunc
			ctx.Properties.Derived.InWindowFunc = prevWindow
		}(
			ctx,
			ctx.Properties.Derived.inFuncExpr,
			ctx.Properties.Derived.InWindowFunc,
		)
		ctx.Properties.Derived.inFuncExpr = true
		if expr.WindowDef != nil {
			ctx.Properties.Derived.InWindowFunc = true
		}
	}

	typedSubExprs, fns, err := typeCheckOverloadedExprs(ctx, desired, def.Definition, false, expr.Exprs...)
	if err != nil {
		return nil, pgerror.Wrapf(err, pgcode.InvalidParameterValue,
			"%s()", def.Name)
	}

	// Return NULL if at least one overload is possible, no overload accepts
	// NULL arguments, the function isn't a generator builtin, and NULL is given
	// as an argument.
	if !def.NullableArgs && def.FunctionProperties.Class != GeneratorClass {
		for _, expr := range typedSubExprs {
			if expr.ResolvedType().Family() == types.UnknownFamily {
				return DNull, nil
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
		if desired.Family() != types.AnyFamily {
			desStr = fmt.Sprintf(" (desired <%s>)", desired)
		}
		sig := fmt.Sprintf("%s(%s)%s", &expr.Func, strings.Join(typeNames, ", "), desStr)
		if len(fns) == 0 {
			return nil, pgerror.Newf(pgcode.UndefinedFunction, "unknown signature: %s", sig)
		}
		fnsStr := formatCandidates(expr.Func.String(), fns)
		return nil, pgerror.Newf(pgcode.AmbiguousFunction, "ambiguous call: %s, candidates are:\n%s", sig, fnsStr)
	}
	overloadImpl := fns[0].(*Overload)

	if expr.IsWindowFunctionApplication() {
		// Make sure the window function application is of either a built-in window
		// function or of a builtin aggregate function.
		if err := CheckIsWindowOrAgg(def); err != nil {
			return nil, err
		}
		if expr.Type == DistinctFuncType {
			return nil, pgerror.New(pgcode.FeatureNotSupported, "DISTINCT is not implemented for window functions")
		}
	} else {
		// Make sure the window function builtins are used as window function applications.
		if def.Class == WindowClass {
			return nil, pgerror.Newf(pgcode.WrongObjectType,
				"window function %s() requires an OVER clause", &expr.Func)
		}
	}

	if expr.Filter != nil {
		if def.Class != AggregateClass {
			// Same error message as Postgres. If we have a window function, only
			// aggregates accept a FILTER clause.
			return nil, pgerror.Newf(pgcode.WrongObjectType,
				"FILTER specified but %s() is not an aggregate function", &expr.Func)
		}

		typedFilter, err := typeCheckAndRequireBoolean(ctx, expr.Filter, "FILTER expression")
		if err != nil {
			return nil, err
		}
		expr.Filter = typedFilter
	}

	if expr.OrderBy != nil {
		for i := range expr.OrderBy {
			typedExpr, err := expr.OrderBy[i].Expr.TypeCheck(ctx, types.Any)
			if err != nil {
				return nil, err
			}
			expr.OrderBy[i].Expr = typedExpr
		}
	}

	for i, subExpr := range typedSubExprs {
		expr.Exprs[i] = subExpr
	}
	expr.fn = overloadImpl
	expr.fnProps = &def.FunctionProperties
	expr.typ = overloadImpl.returnType()(typedSubExprs)
	if expr.typ == UnknownReturnType {
		typeNames := make([]string, 0, len(expr.Exprs))
		for _, expr := range typedSubExprs {
			typeNames = append(typeNames, expr.ResolvedType().String())
		}
		return nil, pgerror.Newf(
			pgcode.DatatypeMismatch,
			"could not determine polymorphic type: %s(%s)",
			&expr.Func,
			strings.Join(typeNames, ", "),
		)
	}
	if overloadImpl.counter != nil {
		telemetry.Inc(overloadImpl.counter)
	}
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *IfErrExpr) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
	var typedCond, typedElse TypedExpr
	var retType *types.T
	var err error
	if expr.Else == nil {
		typedCond, err = expr.Cond.TypeCheck(ctx, types.Any)
		if err != nil {
			return nil, err
		}
		retType = types.Bool
	} else {
		var typedSubExprs []TypedExpr
		typedSubExprs, retType, err = TypeCheckSameTypedExprs(ctx, desired, expr.Cond, expr.Else)
		if err != nil {
			return nil, decorateTypeCheckError(err, "incompatible IFERROR expressions")
		}
		typedCond, typedElse = typedSubExprs[0], typedSubExprs[1]
	}

	var typedErrCode TypedExpr
	if expr.ErrCode != nil {
		typedErrCode, err = typeCheckAndRequire(ctx, expr.ErrCode, types.String, "IFERROR")
		if err != nil {
			return nil, err
		}
	}

	expr.Cond = typedCond
	expr.Else = typedElse
	expr.ErrCode = typedErrCode
	expr.typ = retType

	telemetry.Inc(sqltelemetry.IfErrCounter)
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *IfExpr) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
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
func (expr *IsOfTypeExpr) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
	exprTyped, err := expr.Expr.TypeCheck(ctx, types.Any)
	if err != nil {
		return nil, err
	}
	expr.Expr = exprTyped
	expr.typ = types.Bool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *NotExpr) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
	exprTyped, err := typeCheckAndRequireBoolean(ctx, expr.Expr, "NOT argument")
	if err != nil {
		return nil, err
	}
	expr.Expr = exprTyped
	expr.typ = types.Bool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *NullIfExpr) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
	typedSubExprs, retType, err := TypeCheckSameTypedExprs(ctx, desired, expr.Expr1, expr.Expr2)
	if err != nil {
		return nil, decorateTypeCheckError(err, "incompatible NULLIF expressions")
	}

	expr.Expr1, expr.Expr2 = typedSubExprs[0], typedSubExprs[1]
	expr.typ = retType
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *OrExpr) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
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
func (expr *ParenExpr) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
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
func (expr *ColumnItem) TypeCheck(_ *SemaContext, desired *types.T) (TypedExpr, error) {
	name := expr.String()
	if _, ok := presetTypesForTesting[name]; ok {
		return expr, nil
	}
	return nil, pgerror.Newf(pgcode.UndefinedColumn,
		"column %q does not exist", ErrString(expr))
}

// TypeCheck implements the Expr interface.
func (expr UnqualifiedStar) TypeCheck(_ *SemaContext, desired *types.T) (TypedExpr, error) {
	return nil, errStarNotAllowed
}

// TypeCheck implements the Expr interface.
func (expr *UnresolvedName) TypeCheck(s *SemaContext, desired *types.T) (TypedExpr, error) {
	v, err := expr.NormalizeVarName()
	if err != nil {
		return nil, err
	}
	return v.TypeCheck(s, desired)
}

// TypeCheck implements the Expr interface.
func (expr *AllColumnsSelector) TypeCheck(_ *SemaContext, desired *types.T) (TypedExpr, error) {
	return nil, pgerror.Newf(pgcode.Syntax, "cannot use %q in this context", expr)
}

// TypeCheck implements the Expr interface.
func (expr *RangeCond) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
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
func (expr *Subquery) TypeCheck(sc *SemaContext, _ *types.T) (TypedExpr, error) {
	if sc != nil && sc.Properties.required.rejectFlags&RejectSubqueries != 0 {
		return nil, pgerror.Newf(pgcode.FeatureNotSupported,
			"subqueries are not allowed in %s", sc.Properties.required.context)
	}
	expr.assertTyped()
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *UnaryExpr) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
	ops := UnaryOps[expr.Operator]

	typedSubExprs, fns, err := typeCheckOverloadedExprs(ctx, desired, ops, false, expr.Expr)
	if err != nil {
		return nil, err
	}

	exprTyped := typedSubExprs[0]
	exprReturn := exprTyped.ResolvedType()

	// Return NULL if at least one overload is possible and NULL is an argument.
	if len(fns) > 0 {
		if exprReturn.Family() == types.UnknownFamily {
			return DNull, nil
		}
	}

	// Throw a typing error if overload resolution found either no compatible candidates
	// or if it found an ambiguity.
	if len(fns) != 1 {
		var desStr string
		if desired.Family() != types.AnyFamily {
			desStr = fmt.Sprintf(" (desired <%s>)", desired)
		}
		sig := fmt.Sprintf("%s <%s>%s", expr.Operator, exprReturn, desStr)
		if len(fns) == 0 {
			return nil,
				pgerror.Newf(pgcode.InvalidParameterValue, unsupportedUnaryOpErrFmt, sig)
		}
		fnsStr := formatCandidates(expr.Operator.String(), fns)
		err = pgerror.Newf(pgcode.AmbiguousFunction, ambiguousUnaryOpErrFmt, sig)
		err = errors.WithHintf(err, candidatesHintFmt, fnsStr)
		return nil, err
	}

	unaryOp := fns[0].(*UnaryOp)

	// Register operator usage in telemetry.
	if unaryOp.counter != nil {
		telemetry.Inc(unaryOp.counter)
	}

	expr.Expr = exprTyped
	expr.fn = unaryOp
	expr.typ = unaryOp.returnType()(typedSubExprs)
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr DefaultVal) TypeCheck(_ *SemaContext, desired *types.T) (TypedExpr, error) {
	return nil, errInvalidDefaultUsage
}

// TypeCheck implements the Expr interface.
func (expr PartitionMinVal) TypeCheck(_ *SemaContext, desired *types.T) (TypedExpr, error) {
	return nil, errInvalidMinUsage
}

// TypeCheck implements the Expr interface.
func (expr PartitionMaxVal) TypeCheck(_ *SemaContext, desired *types.T) (TypedExpr, error) {
	return nil, errInvalidMaxUsage
}

// TypeCheck implements the Expr interface.
func (expr *NumVal) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
	return typeCheckConstant(expr, ctx, desired)
}

// TypeCheck implements the Expr interface.
func (expr *StrVal) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
	return typeCheckConstant(expr, ctx, desired)
}

// TypeCheck implements the Expr interface.
func (expr *Tuple) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
	// Ensure the number of labels matches the number of expressions.
	if len(expr.Labels) > 0 && len(expr.Labels) != len(expr.Exprs) {
		return nil, pgerror.Newf(pgcode.Syntax,
			"mismatch in tuple definition: %d expressions, %d labels",
			len(expr.Exprs), len(expr.Labels),
		)
	}

	var labels []string
	contents := make([]types.T, len(expr.Exprs))
	for i, subExpr := range expr.Exprs {
		desiredElem := types.Any
		if desired.Family() == types.TupleFamily && len(desired.TupleContents()) > i {
			desiredElem = &desired.TupleContents()[i]
		}
		typedExpr, err := subExpr.TypeCheck(ctx, desiredElem)
		if err != nil {
			return nil, err
		}
		expr.Exprs[i] = typedExpr
		contents[i] = *typedExpr.ResolvedType()
	}
	// Copy the labels if there are any.
	if len(expr.Labels) > 0 {
		// Ensure that there are no repeat labels.
		for i := range expr.Labels {
			for j := 0; j < i; j++ {
				if expr.Labels[i] == expr.Labels[j] {
					return nil, pgerror.Newf(pgcode.Syntax,
						"found duplicate tuple label: %q", ErrNameStringP(&expr.Labels[i]),
					)
				}
			}
		}

		labels = make([]string, len(expr.Labels))
		for i := range expr.Labels {
			labels[i] = lex.NormalizeName(expr.Labels[i])
		}
	}
	expr.typ = types.MakeLabeledTuple(contents, labels)
	return expr, nil
}

var errAmbiguousArrayType = pgerror.Newf(pgcode.IndeterminateDatatype, "cannot determine type of empty array. "+
	"Consider annotating with the desired type, for example ARRAY[]:::int[]")

// TypeCheck implements the Expr interface.
func (expr *Array) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
	desiredParam := types.Any
	if desired.Family() == types.ArrayFamily {
		desiredParam = desired.ArrayContents()
	}

	if len(expr.Exprs) == 0 {
		if desiredParam.Family() == types.AnyFamily {
			return nil, errAmbiguousArrayType
		}
		expr.typ = types.MakeArray(desiredParam)
		return expr, nil
	}

	typedSubExprs, typ, err := TypeCheckSameTypedExprs(ctx, desiredParam, expr.Exprs...)
	if err != nil {
		return nil, err
	}

	expr.typ = types.MakeArray(typ)
	for i := range typedSubExprs {
		expr.Exprs[i] = typedSubExprs[i]
	}

	telemetry.Inc(sqltelemetry.ArrayConstructorCounter)
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *ArrayFlatten) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
	desiredParam := types.Any
	if desired.Family() == types.ArrayFamily {
		desiredParam = desired.ArrayContents()
	}

	subqueryTyped, err := expr.Subquery.TypeCheck(ctx, desiredParam)
	if err != nil {
		return nil, err
	}
	expr.Subquery = subqueryTyped
	expr.typ = types.MakeArray(subqueryTyped.ResolvedType())

	telemetry.Inc(sqltelemetry.ArrayFlattenCounter)
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *Placeholder) TypeCheck(ctx *SemaContext, desired *types.T) (TypedExpr, error) {
	// Perform placeholder typing. This function is only called during Prepare,
	// when there are no available values for the placeholders yet, because
	// during Execute all placeholders are replaced from the AST before type
	// checking.
	if typ, ok := ctx.Placeholders.Type(expr.Idx); ok {
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
		if err := ctx.Placeholders.SetType(expr.Idx, typ); err != nil {
			return nil, err
		}
		expr.typ = typ
		return expr, nil
	}
	if desired.IsAmbiguous() {
		return nil, placeholderTypeAmbiguityError(expr.Idx)
	}
	if err := ctx.Placeholders.SetType(expr.Idx, desired); err != nil {
		return nil, err
	}
	expr.typ = desired
	return expr, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DBitArray) TypeCheck(_ *SemaContext, _ *types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DBool) TypeCheck(_ *SemaContext, _ *types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DInt) TypeCheck(_ *SemaContext, _ *types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DFloat) TypeCheck(_ *SemaContext, _ *types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DDecimal) TypeCheck(_ *SemaContext, _ *types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DString) TypeCheck(_ *SemaContext, _ *types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DCollatedString) TypeCheck(_ *SemaContext, _ *types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DBytes) TypeCheck(_ *SemaContext, _ *types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DUuid) TypeCheck(_ *SemaContext, _ *types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DIPAddr) TypeCheck(_ *SemaContext, _ *types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DDate) TypeCheck(_ *SemaContext, _ *types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTime) TypeCheck(_ *SemaContext, _ *types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTimestamp) TypeCheck(_ *SemaContext, _ *types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTimestampTZ) TypeCheck(_ *SemaContext, _ *types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DInterval) TypeCheck(_ *SemaContext, _ *types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DJSON) TypeCheck(_ *SemaContext, _ *types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTuple) TypeCheck(_ *SemaContext, _ *types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DArray) TypeCheck(_ *SemaContext, desired *types.T) (TypedExpr, error) {
	// Type-checking arrays is different from normal datums, since there are
	// situations in which an array's type is ambiguous without a desired type.
	// Consider the following examples. They're typed as `unknown[]` until
	// something asserts otherwise. In these circumstances, we're allowed to just
	// mark the array's type as the desired one.
	// ARRAY[]
	// ARRAY[NULL, NULL]
	if (d.ParamTyp == types.Unknown || d.ParamTyp == types.Any) && (!d.HasNonNulls) {
		if desired.Family() != types.ArrayFamily {
			// We can't desire a non-array type here.
			return d, nil
		}
		dCopy := &DArray{}
		*dCopy = *d
		dCopy.ParamTyp = desired.ArrayContents()
		return dCopy, nil
	}
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DOid) TypeCheck(_ *SemaContext, _ *types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DOidWrapper) TypeCheck(_ *SemaContext, _ *types.T) (TypedExpr, error) { return d, nil }

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d dNull) TypeCheck(_ *SemaContext, desired *types.T) (TypedExpr, error) { return d, nil }

// typeCheckAndRequireTupleElems asserts that all elements in the Tuple are
// comparable to the input Expr given the input comparison operator.
func typeCheckAndRequireTupleElems(
	ctx *SemaContext, expr TypedExpr, tuple *Tuple, op ComparisonOperator,
) (TypedExpr, error) {
	tuple.typ = types.MakeTuple(make([]types.T, len(tuple.Exprs)))
	for i, subExpr := range tuple.Exprs {
		// Require that the sub expression is comparable to the required type.
		_, rightTyped, _, _, err := typeCheckComparisonOp(ctx, op, expr, subExpr)
		if err != nil {
			return nil, err
		}
		tuple.Exprs[i] = rightTyped
		tuple.typ.TupleContents()[i] = *rightTyped.ResolvedType()
	}
	return tuple, nil
}

func typeCheckAndRequireBoolean(ctx *SemaContext, expr Expr, op string) (TypedExpr, error) {
	return typeCheckAndRequire(ctx, expr, types.Bool, op)
}

func typeCheckAndRequire(
	ctx *SemaContext, expr Expr, required *types.T, op string,
) (TypedExpr, error) {
	typedExpr, err := expr.TypeCheck(ctx, required)
	if err != nil {
		return nil, err
	}
	if typ := typedExpr.ResolvedType(); !(typ.Family() == types.UnknownFamily || typ.Equivalent(required)) {
		return nil, pgerror.Newf(pgcode.DatatypeMismatch, "incompatible %s type: %s", op, typ)
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
) (_ TypedExpr, _ TypedExpr, _ *CmpOp, alwaysNull bool, _ error) {
	// Parentheses are semantically unimportant and can be removed/replaced
	// with its nested expression in our plan. This makes type checking cleaner.
	left = StripParens(left)
	right = StripParens(right)

	// Determine the set of comparisons are possible for the sub-operation,
	// which will be memoized.
	foldedOp, _, _, _, _ := foldComparisonExpr(subOp, nil, nil)
	ops := CmpOps[foldedOp]

	var cmpTypeLeft, cmpTypeRight *types.T
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
			return nil, nil, nil, false,
				pgerror.Newf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sigWithErr)
		}

		// Determine TypedExpr and comparison type for left operand.
		leftTyped = typedSubExprs[0]
		cmpTypeLeft = retType

		// Determine TypedExpr and comparison type for right operand, making sure
		// all ParenExprs on the right are properly type checked.
		for i, typedExpr := range typedSubExprs[1:] {
			array.Exprs[i] = typedExpr
		}
		array.typ = types.MakeArray(retType)

		rightTyped = array
		cmpTypeRight = retType

		// Return early without looking up a CmpOp if the comparison type is types.Null.
		if leftTyped.ResolvedType().Family() == types.UnknownFamily || retType.Family() == types.UnknownFamily {
			return leftTyped, rightTyped, nil, true /* alwaysNull */, nil
		}
	} else {
		// If the right expression is not an array constructor, we type the left
		// expression in isolation.
		var err error
		leftTyped, err = left.TypeCheck(ctx, types.Any)
		if err != nil {
			return nil, nil, nil, false, err
		}
		cmpTypeLeft = leftTyped.ResolvedType()

		if tuple, ok := right.(*Tuple); ok {
			// If right expression is a tuple, we require that all elements' inferred
			// type is equivalent to the left's type.
			rightTyped, err = typeCheckAndRequireTupleElems(ctx, leftTyped, tuple, subOp)
			if err != nil {
				return nil, nil, nil, false, err
			}
		} else {
			// Try to type the right expression as an array of the left's type.
			// If right is an sql.subquery Expr, it should already be typed.
			// TODO(richardwu): If right is a subquery, we should really
			// propagate the left type as a desired type for the result column.
			rightTyped, err = right.TypeCheck(ctx, types.MakeArray(cmpTypeLeft))
			if err != nil {
				return nil, nil, nil, false, err
			}
		}

		rightReturn := rightTyped.ResolvedType()
		if cmpTypeLeft.Family() == types.UnknownFamily || rightReturn.Family() == types.UnknownFamily {
			return leftTyped, rightTyped, nil, true /* alwaysNull */, nil
		}

		switch rightReturn.Family() {
		case types.ArrayFamily:
			cmpTypeRight = rightReturn.ArrayContents()
		case types.TupleFamily:
			if len(rightReturn.TupleContents()) == 0 {
				// Literal tuple contains no elements, or subquery tuple returns 0 rows.
				cmpTypeRight = cmpTypeLeft
			} else {
				// Literal tuples and subqueries were type checked such that all
				// elements have comparable types with the left. Once that's true, we
				// can safely grab the first element's type as the right type for the
				// purposes of computing the correct comparison function below, since
				// if two datum types are comparable, it's legal to call .Compare on
				// one with the other.
				cmpTypeRight = &rightReturn.TupleContents()[0]
			}
		default:
			sigWithErr := fmt.Sprintf(compExprsWithSubOpFmt, left, subOp, op, right,
				fmt.Sprintf("op %s <right> requires array, tuple or subquery on right side", op))
			return nil, nil, nil, false, pgerror.Newf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sigWithErr)
		}
	}
	fn, ok := ops.lookupImpl(cmpTypeLeft, cmpTypeRight)
	if !ok {
		return nil, nil, nil, false, subOpCompError(cmpTypeLeft, rightTyped.ResolvedType(), subOp, op)
	}
	return leftTyped, rightTyped, fn, false, nil
}

func subOpCompError(leftType, rightType *types.T, subOp, op ComparisonOperator) error {
	sig := fmt.Sprintf(compSignatureWithSubOpFmt, leftType, subOp, op, rightType)
	return pgerror.Newf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sig)
}

// typeCheckSubqueryWithIn checks the case where the right side of an IN
// expression is a subquery.
func typeCheckSubqueryWithIn(left, right *types.T) error {
	if right.Family() == types.TupleFamily {
		// Subqueries come through as a tuple{T}, so T IN tuple{T} should be
		// accepted.
		if len(right.TupleContents()) != 1 {
			return pgerror.Newf(pgcode.InvalidParameterValue,
				unsupportedCompErrFmt, fmt.Sprintf(compSignatureFmt, left, In, right))
		}
		if !left.Equivalent(&right.TupleContents()[0]) {
			return pgerror.Newf(pgcode.InvalidParameterValue,
				unsupportedCompErrFmt, fmt.Sprintf(compSignatureFmt, left, In, right))
		}
	}
	return nil
}

func typeCheckComparisonOp(
	ctx *SemaContext, op ComparisonOperator, left, right Expr,
) (_ TypedExpr, _ TypedExpr, _ *CmpOp, alwaysNull bool, _ error) {
	foldedOp, foldedLeft, foldedRight, switched, _ := foldComparisonExpr(op, left, right)
	ops := CmpOps[foldedOp]

	_, leftIsTuple := foldedLeft.(*Tuple)
	rightTuple, rightIsTuple := foldedRight.(*Tuple)

	_, rightIsSubquery := foldedRight.(SubqueryExpr)
	switch {
	case foldedOp == In && rightIsTuple:
		sameTypeExprs := make([]Expr, len(rightTuple.Exprs)+1)
		sameTypeExprs[0] = foldedLeft
		copy(sameTypeExprs[1:], rightTuple.Exprs)

		typedSubExprs, retType, err := TypeCheckSameTypedExprs(ctx, types.Any, sameTypeExprs...)
		if err != nil {
			sigWithErr := fmt.Sprintf(compExprsFmt, left, op, right, err)
			return nil, nil, nil, false,
				pgerror.Newf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sigWithErr)
		}

		fn, ok := ops.lookupImpl(retType, types.AnyTuple)
		if !ok {
			sig := fmt.Sprintf(compSignatureFmt, retType, op, types.AnyTuple)
			return nil, nil, nil, false,
				pgerror.Newf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sig)
		}

		typedLeft := typedSubExprs[0]
		typedSubExprs = typedSubExprs[1:]

		rightTuple.typ = types.MakeTuple(make([]types.T, len(typedSubExprs)))
		for i, typedExpr := range typedSubExprs {
			rightTuple.Exprs[i] = typedExpr
			rightTuple.typ.TupleContents()[i] = *retType
		}
		if switched {
			return rightTuple, typedLeft, fn, false, nil
		}
		return typedLeft, rightTuple, fn, false, nil

	case foldedOp == In && rightIsSubquery:
		typedLeft, err := foldedLeft.TypeCheck(ctx, types.Any)
		if err != nil {
			sigWithErr := fmt.Sprintf(compExprsFmt, left, op, right, err)
			return nil, nil, nil, false,
				pgerror.Newf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sigWithErr)
		}

		typ := typedLeft.ResolvedType()
		fn, ok := ops.lookupImpl(typ, types.AnyTuple)
		if !ok {
			sig := fmt.Sprintf(compSignatureFmt, typ, op, types.AnyTuple)
			return nil, nil, nil, false,
				pgerror.Newf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sig)
		}

		desired := types.MakeTuple([]types.T{*typ})
		typedRight, err := foldedRight.TypeCheck(ctx, desired)
		if err != nil {
			sigWithErr := fmt.Sprintf(compExprsFmt, left, op, right, err)
			return nil, nil, nil, false,
				pgerror.Newf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sigWithErr)
		}

		if err := typeCheckSubqueryWithIn(
			typedLeft.ResolvedType(), typedRight.ResolvedType(),
		); err != nil {
			return nil, nil, nil, false, err
		}
		return typedLeft, typedRight, fn, false, nil

	case leftIsTuple && rightIsTuple:
		fn, ok := ops.lookupImpl(types.AnyTuple, types.AnyTuple)
		if !ok {
			sig := fmt.Sprintf(compSignatureFmt, types.AnyTuple, op, types.AnyTuple)
			return nil, nil, nil, false,
				pgerror.Newf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sig)
		}
		// Using non-folded left and right to avoid having to swap later.
		typedLeft, typedRight, err := typeCheckTupleComparison(ctx, op, left.(*Tuple), right.(*Tuple))
		if err != nil {
			return nil, nil, nil, false, err
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
		return nil, nil, nil, false, err
	}

	leftExpr, rightExpr := typedSubExprs[0], typedSubExprs[1]
	if switched {
		leftExpr, rightExpr = rightExpr, leftExpr
	}
	leftReturn := leftExpr.ResolvedType()
	rightReturn := rightExpr.ResolvedType()

	// Return early if at least one overload is possible, NULL is an argument,
	// and none of the overloads accept NULL.
	if leftReturn.Family() == types.UnknownFamily || rightReturn.Family() == types.UnknownFamily {
		if len(fns) > 0 {
			noneAcceptNull := true
			for _, e := range fns {
				if e.(*CmpOp).NullableArgs {
					noneAcceptNull = false
					break
				}
			}
			if noneAcceptNull {
				return leftExpr, rightExpr, nil, true /* alwaysNull */, err
			}
		}
	}

	// Throw a typing error if overload resolution found either no compatible candidates
	// or if it found an ambiguity.
	collationMismatch :=
		leftReturn.Family() == types.CollatedStringFamily && !leftReturn.Equivalent(rightReturn)
	if len(fns) != 1 || collationMismatch {
		sig := fmt.Sprintf(compSignatureFmt, leftReturn, op, rightReturn)
		if len(fns) == 0 || collationMismatch {
			return nil, nil, nil, false,
				pgerror.Newf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sig)
		}
		fnsStr := formatCandidates(op.String(), fns)
		err = pgerror.Newf(pgcode.AmbiguousFunction, ambiguousCompErrFmt, sig)
		err = errors.WithHintf(err, candidatesHintFmt, fnsStr)
		return nil, nil, nil, false, err
	}

	return leftExpr, rightExpr, fns[0].(*CmpOp), false, nil
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
	ctx *SemaContext, desired *types.T, exprs ...Expr,
) ([]TypedExpr, *types.T, error) {
	switch len(exprs) {
	case 0:
		return nil, nil, nil
	case 1:
		typedExpr, err := exprs[0].TypeCheck(ctx, desired)
		if err != nil {
			return nil, nil, err
		}
		typ := typedExpr.ResolvedType()
		if typ == types.Unknown && desired != types.Any {
			// The expression had a NULL type, so we can return the desired type as
			// the expression type.
			typ = desired
		}
		return []TypedExpr{typedExpr}, typ, nil
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
			if returnType := typedExpr.ResolvedType(); returnType.Family() != types.UnknownFamily {
				firstValidType = returnType
				firstValidIdx = i
				break
			}
		}

		if firstValidType.Family() == types.UnknownFamily {
			// We got to the end without finding a non-null expression.
			switch {
			case len(constIdxs) > 0:
				return typeCheckConstsAndPlaceholdersWithDesired(s, desired)
			case len(placeholderIdxs) > 0:
				p := s.exprs[placeholderIdxs[0]].(*Placeholder)
				return nil, nil, placeholderTypeAmbiguityError(p.Idx)
			default:
				if desired != types.Any {
					return typedExprs, desired, nil
				}
				return typedExprs, types.Unknown, nil
			}
		}

		for _, i := range resolvableIdxs[firstValidIdx+1:] {
			typedExpr, err := exprs[i].TypeCheck(ctx, firstValidType)
			if err != nil {
				return nil, nil, err
			}
			if typ := typedExpr.ResolvedType(); !(typ.Equivalent(firstValidType) || typ.Family() == types.UnknownFamily) {
				return nil, nil, unexpectedTypeError(exprs[i], firstValidType, typ)
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
func typeCheckSameTypedPlaceholders(s typeCheckExprsState, typ *types.T) error {
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
func typeCheckSameTypedConsts(
	s typeCheckExprsState, typ *types.T, required bool,
) (*types.T, error) {
	setTypeForConsts := func(typ *types.T) (*types.T, error) {
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
	if typ.Family() != types.AnyFamily {
		all := true
		for _, i := range s.constIdxs {
			if !canConstantBecome(s.exprs[i].(Constant), typ) {
				if required {
					typedExpr, err := s.exprs[i].TypeCheck(s.ctx, types.Any)
					if err != nil {
						return nil, err
					}
					return nil, unexpectedTypeError(s.exprs[i], typ, typedExpr.ResolvedType())
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
			return nil, unexpectedTypeError(s.exprs[i], reqTyp, typ)
		}
		if reqTyp.Family() == types.AnyFamily {
			reqTyp = typedExpr.ResolvedType()
		}
	}
	return nil, errors.AssertionFailedf("should throw error above")
}

// Used to type check all constants with the optional desired type. The
// type that is chosen here will then be set to any placeholders.
func typeCheckConstsAndPlaceholdersWithDesired(
	s typeCheckExprsState, desired *types.T,
) ([]TypedExpr, *types.T, error) {
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
	left.typ = types.MakeTuple(make([]types.T, tupLen))
	right.typ = types.MakeTuple(make([]types.T, tupLen))
	for elemIdx := range left.Exprs {
		leftSubExpr := left.Exprs[elemIdx]
		rightSubExpr := right.Exprs[elemIdx]
		leftSubExprTyped, rightSubExprTyped, _, _, err := typeCheckComparisonOp(ctx, op, leftSubExpr, rightSubExpr)
		if err != nil {
			exps := Exprs([]Expr{left, right})
			return nil, nil, pgerror.Newf(pgcode.DatatypeMismatch, "tuples %s are not comparable at index %d: %s",
				&exps, elemIdx+1, err)
		}
		left.Exprs[elemIdx] = leftSubExprTyped
		left.typ.TupleContents()[elemIdx] = *leftSubExprTyped.ResolvedType()
		right.Exprs[elemIdx] = rightSubExprTyped
		right.typ.TupleContents()[elemIdx] = *rightSubExprTyped.ResolvedType()
	}
	return left, right, nil
}

// typeCheckSameTypedTupleExprs type checks a list of expressions, asserting
// that all are tuples which have the same type or nulls. The function expects
// the first provided expression to be a tuple, and will panic if it is not.
// However, it does not expect all other expressions are tuples or nulls, and
// will return a sane error if they are not. An optional desired type can be
// provided, which will hint that type which the expressions should resolve to,
// if possible.
func typeCheckSameTypedTupleExprs(
	ctx *SemaContext, desired *types.T, exprs ...Expr,
) ([]TypedExpr, *types.T, error) {
	// Hold the resolved type expressions of the provided exprs, in order.
	// TODO(nvanbenschoten): Look into reducing allocations here.
	typedExprs := make([]TypedExpr, len(exprs))

	// All other exprs must be tuples.
	first := exprs[0].(*Tuple)
	if err := checkAllExprsAreTuplesOrNulls(ctx, exprs[1:]); err != nil {
		return nil, nil, err
	}

	// All tuples must have the same length.
	firstLen := len(first.Exprs)
	if err := checkAllTuplesHaveLength(exprs[1:], firstLen); err != nil {
		return nil, nil, err
	}

	// All expressions within tuples at the same indexes must be the same type.
	resTypes := types.MakeTuple(make([]types.T, firstLen))
	sameTypeExprs := make([]Expr, 0, len(exprs))
	// We will be skipping nulls, so we need to keep track at which indices in
	// exprs are the non-null tuples.
	sameTypeExprsIndices := make([]int, 0, len(exprs))
	for elemIdx := range first.Exprs {
		sameTypeExprs = sameTypeExprs[:0]
		sameTypeExprsIndices = sameTypeExprsIndices[:0]
		for exprIdx, expr := range exprs {
			if expr == DNull {
				continue
			}
			sameTypeExprs = append(sameTypeExprs, expr.(*Tuple).Exprs[elemIdx])
			sameTypeExprsIndices = append(sameTypeExprsIndices, exprIdx)
		}
		desiredElem := types.Any
		if len(desired.TupleContents()) > elemIdx {
			desiredElem = &desired.TupleContents()[elemIdx]
		}
		typedSubExprs, resType, err := TypeCheckSameTypedExprs(ctx, desiredElem, sameTypeExprs...)
		if err != nil {
			return nil, nil, pgerror.Newf(pgcode.DatatypeMismatch, "tuples %s are not the same type: %v", Exprs(exprs), err)
		}
		for j, typedExpr := range typedSubExprs {
			tupleIdx := sameTypeExprsIndices[j]
			exprs[tupleIdx].(*Tuple).Exprs[elemIdx] = typedExpr
		}
		resTypes.TupleContents()[elemIdx] = *resType
	}
	for tupleIdx, expr := range exprs {
		if expr != DNull {
			expr.(*Tuple).typ = resTypes
		}
		typedExprs[tupleIdx] = expr.(TypedExpr)
	}
	return typedExprs, resTypes, nil
}

// checkAllExprsAreTuplesOrNulls checks that all expressions in exprs are
// either tuples or nulls.
func checkAllExprsAreTuplesOrNulls(ctx *SemaContext, exprs []Expr) error {
	for _, expr := range exprs {
		_, isTuple := expr.(*Tuple)
		isNull := expr == DNull
		if !(isTuple || isNull) {
			typedExpr, err := expr.TypeCheck(ctx, types.Any)
			if err != nil {
				return err
			}
			return unexpectedTypeError(expr, types.AnyTuple, typedExpr.ResolvedType())
		}
	}
	return nil
}

// checkAllTuplesHaveLength checks that all tuples in exprs have the expected
// length. Note that all nulls are skipped in this check.
func checkAllTuplesHaveLength(exprs []Expr, expectedLen int) error {
	for _, expr := range exprs {
		if expr == DNull {
			continue
		}
		if err := checkTupleHasLength(expr.(*Tuple), expectedLen); err != nil {
			return err
		}
	}
	return nil
}

func checkTupleHasLength(t *Tuple, expectedLen int) error {
	if len(t.Exprs) != expectedLen {
		return pgerror.Newf(pgcode.DatatypeMismatch, "expected tuple %v to have a length of %d", t, expectedLen)
	}
	return nil
}

type placeholderAnnotationVisitor struct {
	types PlaceholderTypes
	state []annotationState
	err   error
	// errIdx stores the placeholder to which err applies. Used to select the
	// error for the smallest index.
	errIdx PlaceholderIdx
}

// annotationState keeps extra information relating to the type of a
// placeholder.
type annotationState uint8

const (
	noType annotationState = iota

	// typeFromHint indicates that the type for the placeholder is set from a
	// provided type hint (from pgwire, or PREPARE AS arguments).
	typeFromHint

	// typeFromAnnotation indicates that the type for this placeholder is set from
	// a type annotation.
	typeFromAnnotation

	// typeFromCast indicates that the type for this placeholder is set from
	// a cast. This type can be replaced if an annotation is found, or discarded
	// if conflicting casts are found.
	typeFromCast

	// conflictingCasts indicates that we haven't found an annotation for the
	// placeholder, and we cannot determine the type from casts: either we found
	// conflicting casts, or we found an appearance of the placeholder without a
	// cast.
	conflictingCasts
)

func (v *placeholderAnnotationVisitor) setErr(idx PlaceholderIdx, err error) {
	if v.err == nil || v.errIdx >= idx {
		v.err = err
		v.errIdx = idx
	}
}

func (v *placeholderAnnotationVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	switch t := expr.(type) {
	case *AnnotateTypeExpr:
		if arg, ok := t.Expr.(*Placeholder); ok {
			switch v.state[arg.Idx] {
			case noType, typeFromCast, conflictingCasts:
				// An annotation overrides casts.
				v.types[arg.Idx] = t.Type
				v.state[arg.Idx] = typeFromAnnotation

			case typeFromAnnotation:
				// Verify that the annotations are consistent.
				if !t.Type.Equivalent(v.types[arg.Idx]) {
					v.setErr(arg.Idx, pgerror.Newf(
						pgcode.DatatypeMismatch,
						"multiple conflicting type annotations around %s",
						arg.Idx,
					))
				}

			case typeFromHint:
				// Verify that the annotation is consistent with the type hint.
				if prevType := v.types[arg.Idx]; !t.Type.Equivalent(prevType) {
					v.setErr(arg.Idx, pgerror.Newf(
						pgcode.DatatypeMismatch,
						"type annotation around %s conflicts with specified type %s",
						arg.Idx, v.types[arg.Idx],
					))
				}

			default:
				panic(errors.AssertionFailedf("unhandled state: %v", errors.Safe(v.state[arg.Idx])))
			}
			return false, expr
		}

	case *CastExpr:
		if arg, ok := t.Expr.(*Placeholder); ok {
			switch v.state[arg.Idx] {
			case noType:
				v.types[arg.Idx] = t.Type
				v.state[arg.Idx] = typeFromCast

			case typeFromCast:
				// Verify that the casts are consistent.
				if !t.Type.Equivalent(v.types[arg.Idx]) {
					v.state[arg.Idx] = conflictingCasts
					v.types[arg.Idx] = nil
				}

			case typeFromHint, typeFromAnnotation:
				// A cast never overrides a hinted or annotated type.

			case conflictingCasts:
				// We already saw inconsistent casts, or a "bare" placeholder; ignore
				// this cast.

			default:
				panic(errors.AssertionFailedf("unhandled state: %v", v.state[arg.Idx]))
			}
			return false, expr
		}

	case *Placeholder:
		switch v.state[t.Idx] {
		case noType, typeFromCast:
			// A "bare" placeholder prevents type determination from casts.
			v.state[t.Idx] = conflictingCasts
			v.types[t.Idx] = nil

		case typeFromHint, typeFromAnnotation:
			// We are not relying on casts to determine the type, nothing to do.

		case conflictingCasts:
			// We already decided not to use casts, nothing to do.

		default:
			panic(errors.AssertionFailedf("unhandled state: %v", v.state[t.Idx]))
		}
	}
	return true, expr
}

func (*placeholderAnnotationVisitor) VisitPost(expr Expr) Expr { return expr }

// ProcessPlaceholderAnnotations performs an order-independent global traversal of the
// provided Statement, annotating all placeholders with a type in either of the following
// situations:
//
//  - the placeholder is the subject of an explicit type annotation in at least one
//    of its occurrences. If it is subject to multiple explicit type annotations
//    where the types are not all in agreement, or if the placeholder already has
//    a type hint in the placeholder map which conflicts with the explicit type
//    annotation type, an error will be thrown.
//
//  - the placeholder is the subject to a cast of the same type in all
//    occurrences of the placeholder. If the placeholder is subject to casts of
//    multiple types, or if it has occurrences without a cast, no error will be
//    thrown but the type will not be inferred. If the placeholder already has a
//    type hint, that type will be kept regardless of any casts.
//
// See docs/RFCS/20160203_typing.md for more details on placeholder typing (in
// particular section "First pass: placeholder annotations").
//
// The typeHints slice contains the client-provided hints and is populated with
// any newly assigned types. It is assumed to be pre-sized to the number of
// placeholders in the statement and is populated accordingly.
//
// TODO(nvanbenschoten): Can this visitor and map be preallocated (like normalizeVisitor)?
func ProcessPlaceholderAnnotations(stmt Statement, typeHints PlaceholderTypes) error {
	v := placeholderAnnotationVisitor{
		types: typeHints,
		state: make([]annotationState, len(typeHints)),
	}

	for placeholder := range typeHints {
		if typeHints[placeholder] != nil {
			v.state[placeholder] = typeFromHint
		}
	}

	walkStmt(&v, stmt)
	return v.err
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
		t.fn = nil
	case *BinaryExpr:
		t.fn = nil
	case *ComparisonExpr:
		t.fn = nil
	case *FuncExpr:
		t.fn = nil
		t.fnProps = nil
	}
	return true, expr
}

func (stripFuncsVisitor) VisitPost(expr Expr) Expr { return expr }
