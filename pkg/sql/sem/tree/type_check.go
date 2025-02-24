// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/collatedstring"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"golang.org/x/text/language"
)

// OnTypeCheck* functions are hooks which get called if not nil and the
// type checking of the relevant function is made.
var (
	OnTypeCheckArraySubscript   func()
	OnTypeCheckJSONBSubscript   func()
	OnTypeCheckIfErr            func()
	OnTypeCheckArrayConstructor func()
	OnTypeCheckArrayFlatten     func()
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

	// SearchPath indicates where to search for unqualified function
	// names. The path elements must be normalized via Name.Normalize()
	// already.
	SearchPath SearchPath

	// TypeResolver manages resolving type names into *types.T's.
	TypeResolver TypeReferenceResolver

	// FunctionResolver manages resolving functions names into
	// *FunctionDefinitons.
	FunctionResolver FunctionReferenceResolver

	// NameResolver is used to resolve the fully qualified
	// name of a table given its ID.
	NameResolver QualifiedNameResolver

	Properties SemaProperties

	// DateStyle refers to the DateStyle to parse as.
	DateStyle pgdate.DateStyle
	// IntervalStyle refers to the IntervalStyle to parse as.
	IntervalStyle duration.IntervalStyle

	// UnsupportedTypeChecker is used to determine whether a builtin data type is
	// supported by the current cluster version. It may be unset.
	UnsupportedTypeChecker UnsupportedTypeChecker
}

// SemaProperties is a holder for required and derived properties
// during semantic analysis. It provides scoping semantics via its
// Restore() method, see below.
type SemaProperties struct {
	// required constraints type checking to only accept certain kinds
	// of expressions. See Require.
	required semaRequirements

	// Derived is populated during semantic analysis with properties
	// from the expression being analyzed.  The caller is responsible
	// for re-initializing this when needed.
	Derived ScalarProperties

	// Ancestors is mutated during semantic analysis to provide contextual
	// information for each descendent during traversal of sub-expressions.
	Ancestors ScalarAncestors

	// IgnoreUnpreferredOverloads is set to true when "unpreferred" overloads
	// should not be used during type-checking and overload resolution.
	IgnoreUnpreferredOverloads bool
}

type semaRequirements struct {
	// context is the name of the semantic analysis context, for use in
	// error messages.
	context string

	// The various reject flags reject specific forms of scalar
	// expressions. The default for this struct with false everywhere
	// ensures that anything is allowed.
	rejectFlags SemaRejectFlags
}

// Require resets the derived properties and the scalar ancestors, and sets
// required constraints. It must only be called before starting semantic
// analysis and during traversal by semantic analysis itself.
func (s *SemaProperties) Require(context string, rejectFlags SemaRejectFlags) {
	s.required.context = context
	s.required.rejectFlags = rejectFlags
	s.Derived.Clear()
	s.Ancestors.clear()
}

// Reject adds all of the given flags to the set of required constraints of s.
func (s *SemaProperties) Reject(rejectFlags SemaRejectFlags) {
	s.required.rejectFlags |= rejectFlags
}

// IsSet checks if all of the given rejectFlags are set as a required property.
func (s *SemaProperties) IsSet(rejectFlags SemaRejectFlags) bool {
	return s.required.rejectFlags&rejectFlags == rejectFlags
}

// Restore restores a copy of a SemaProperties. Use with:
// defer semaCtx.Properties.Restore(semaCtx.Properties)
func (s *SemaProperties) Restore(orig SemaProperties) {
	*s = orig
}

// Context returns the required context string set by Require.
func (s *SemaProperties) Context() string {
	return s.required.context
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

	// RejectNestedWindowFunctions rejects any use of window functions inside the
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

	// RejectStableOperators rejects any stable functions or operators (including
	// casts).
	RejectStableOperators

	// RejectVolatileFunctions rejects any volatile functions.
	RejectVolatileFunctions

	// RejectSubqueries rejects subqueries in scalar contexts.
	RejectSubqueries

	// RejectProcedures rejects procedures in scalar contexts.
	RejectProcedures

	// RejectSpecial is used in common places like the LIMIT clause.
	RejectSpecial = RejectAggregates |
		RejectGenerators |
		RejectWindowApplications |
		RejectProcedures
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
}

// Clear resets the scalar properties to defaults.
func (sp *ScalarProperties) Clear() {
	*sp = ScalarProperties{}
}

// ScalarAncestors provides context for the current scalar expression during
// semantic analysis. Ancestors are temporarily modified by expressions so that
// their descendent expressions can be analyzed with respect to their ancestors.
type ScalarAncestors byte

const (
	// FuncExprAncestor is temporarily added to ScalarAncestors while type
	// checking the parameters of a function. Used to process
	// RejectNestedGenerators properly.
	FuncExprAncestor ScalarAncestors = 1 << iota

	// WindowFuncAncestor is temporarily added to ScalarAncestors while type
	// checking the parameters of a window function in order to reject nested
	// window functions.
	WindowFuncAncestor

	// ConditionalAncestor is temporarily added to ScalarAncestors while type
	// checking condition expressions CASE, COALESCE, and IF. Used to reject
	// set-returning functions within conditional expressions.
	ConditionalAncestor
)

// Push adds the given ancestor to s.
func (s *ScalarAncestors) Push(other ScalarAncestors) {
	*s = *s | other
}

// Has returns true if s has all of the given ancestors.
func (s ScalarAncestors) Has(other ScalarAncestors) bool {
	return s&other == other
}

// PopTo returns s to the given set of ancestors. Use with:
//
//	defer semaCtx.Properties.Ancestors.PopTo(semaCtx.Properties.Ancestors)
func (s *ScalarAncestors) PopTo(orig ScalarAncestors) {
	*s = orig
}

// clear resets s to the default set of ancestors.
func (s *ScalarAncestors) clear() {
	*s = 0
}

// MakeSemaContext initializes a simple SemaContext suitable for "lightweight"
// type checking such as the one performed for default expressions.
//
// Note: if queries with placeholders are going to be used,
// SemaContext.Placeholders.Init must be called separately.
//
// resolver is optional and can implement any number of TypeReferenceResolver,
// FunctionReferenceResolver, and QualifiedNameResolver interfaces.
func MakeSemaContext(resolver interface{}) SemaContext {
	typeResolver, _ := resolver.(TypeReferenceResolver)
	functionResolver, _ := resolver.(FunctionReferenceResolver)
	nameResolver, _ := resolver.(QualifiedNameResolver)
	return SemaContext{
		TypeResolver:     typeResolver,
		FunctionResolver: functionResolver,
		NameResolver:     nameResolver,
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

// GetTypeResolver returns the TypeReferenceResolver.
func (sc *SemaContext) GetTypeResolver() TypeReferenceResolver {
	if sc == nil {
		return nil
	}
	return sc.TypeResolver
}

func placeholderTypeAmbiguityError(idx PlaceholderIdx) error {
	return errors.WithHint(
		pgerror.WithCandidateCode(
			&placeholderTypeAmbiguityErr{idx},
			pgcode.IndeterminateDatatype),
		"consider adding explicit type casts to the placeholder arguments",
	)
}

type placeholderTypeAmbiguityErr struct {
	idx PlaceholderIdx
}

func (err *placeholderTypeAmbiguityErr) Error() string {
	return fmt.Sprintf("could not determine data type of placeholder %s", err.idx)
}

func unexpectedTypeError(expr Expr, want, got *types.T) error {
	return pgerror.Newf(pgcode.InvalidParameterValue,
		"expected %s to be of type %s, found type %s", expr, errors.Safe(want), errors.Safe(got))
}

func decorateTypeCheckError(err error, format string, a ...interface{}) error {
	switch pgerror.GetPGCode(err) {
	case pgcode.Grouping, pgcode.Windowing, pgcode.FeatureNotSupported:
	// Error due to syntax, e.g. using generator function in a Case expr.
	// Fall through.
	default:
		if !errors.HasType(err, (*placeholderTypeAmbiguityErr)(nil)) {
			return pgerror.Wrapf(err, pgcode.InvalidParameterValue, format, a...)
		}
	}
	return errors.WithStack(err)
}

// TypeCheck performs type checking on the provided expression tree, returning
// the new typed expression tree, which additionally permits evaluation and type
// introspection globally and on each sub-tree.
//
// While doing so, it will fold numeric constants and bind placeholder names to
// their inferred types in the provided context. The optional desired parameter can
// be used to hint the desired type for the root of the resulting typed expression
// tree. Like with Expr.TypeCheck, it is not valid to provide a nil desired
// type. Instead, call it with the wildcard type types.AnyElement if no specific type is
// desired.
func TypeCheck(
	ctx context.Context, expr Expr, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	if desired == nil {
		return nil, errors.AssertionFailedf(
			"the desired type for tree.TypeCheck cannot be nil, use types.AnyElement instead: %T", expr)
	}

	return expr.TypeCheck(ctx, semaCtx, desired)
}

// TypeCheckAndRequire performs type checking on the provided expression tree in
// an identical manner to TypeCheck. It then asserts that the resulting TypedExpr
// has the provided return type, returning both the typed expression and an error
// if it does not.
func TypeCheckAndRequire(
	ctx context.Context, expr Expr, semaCtx *SemaContext, required *types.T, op string,
) (TypedExpr, error) {
	typedExpr, err := TypeCheck(ctx, expr, semaCtx, required)
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
func (expr *AndExpr) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	leftTyped, err := typeCheckAndRequireBoolean(ctx, semaCtx, expr.Left, "AND argument")
	if err != nil {
		return nil, err
	}
	rightTyped, err := typeCheckAndRequireBoolean(ctx, semaCtx, expr.Right, "AND argument")
	if err != nil {
		return nil, err
	}
	expr.Left, expr.Right = leftTyped, rightTyped
	expr.typ = types.Bool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *BinaryExpr) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	ops := BinOps[expr.Operator.Symbol]

	const inBinOp = true
	s := getOverloadTypeChecker(ops, expr.Left, expr.Right)
	defer s.release()
	if err := s.typeCheckOverloadedExprs(ctx, semaCtx, desired, inBinOp); err != nil {
		return nil, err
	}
	typedSubExprs := s.typedExprs
	leftTyped, rightTyped := typedSubExprs[0], typedSubExprs[1]
	leftReturn := leftTyped.ResolvedType()
	rightReturn := rightTyped.ResolvedType()

	// Return NULL if at least one overload is possible, NULL is an argument,
	// and none of the overloads accept NULL.
	if leftReturn.Family() == types.UnknownFamily || rightReturn.Family() == types.UnknownFamily {
		if len(s.overloadIdxs) > 0 {
			noneAcceptNull := true
			for _, idx := range s.overloadIdxs {
				if ops.overloads[idx].CalledOnNullInput {
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
	if len(s.overloadIdxs) != 1 {
		var desStr string
		if desired.Family() != types.AnyFamily {
			desStr = fmt.Sprintf(" (returning <%s>)", desired)
		}
		sig := fmt.Sprintf("<%s> %s <%s>%s", leftReturn, expr.Operator, rightReturn, desStr)
		if len(s.overloadIdxs) == 0 {
			return nil,
				pgerror.Newf(pgcode.InvalidParameterValue, unsupportedBinaryOpErrFmt, sig)
		}
		fnsStr := formatCandidates(
			expr.Operator.String(), s.overloads, s.overloadIdxs,
		)
		err := pgerror.Newf(pgcode.AmbiguousFunction, ambiguousBinaryOpErrFmt, sig)
		err = errors.WithHintf(err, candidatesHintFmt, fnsStr)
		return nil, err
	}

	binOp := ops.overloads[s.overloadIdxs[0]]
	if err := semaCtx.checkVolatility(binOp.Volatility); err != nil {
		return nil, pgerror.Wrapf(err, pgcode.InvalidParameterValue, "%s", expr.Operator)
	}

	if binOp.OnTypeCheck != nil {
		binOp.OnTypeCheck()
	}

	expr.Left, expr.Right = leftTyped, rightTyped
	expr.Op = binOp
	expr.typ = binOp.returnType()(typedSubExprs)
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *CaseExpr) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	if semaCtx != nil {
		defer semaCtx.Properties.Ancestors.PopTo(semaCtx.Properties.Ancestors)
		semaCtx.Properties.Ancestors.Push(ConditionalAncestor)
	}
	var err error
	tmpExprs := make([]Expr, 0, len(expr.Whens)+1)
	if expr.Expr != nil {
		tmpExprs = tmpExprs[:0]
		tmpExprs = append(tmpExprs, expr.Expr)
		for _, when := range expr.Whens {
			tmpExprs = append(tmpExprs, when.Cond)
		}

		typedSubExprs, _, err := typeCheckSameTypedExprs(ctx, semaCtx, types.AnyElement, tmpExprs...)
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
			typedCond, err := typeCheckAndRequireBoolean(ctx, semaCtx, when.Cond, "condition")
			if err != nil {
				return nil, err
			}
			expr.Whens[i].Cond = typedCond
		}
	}

	tmpExprs = tmpExprs[:0]
	// As described in the Postgres docs, CASE treats its ELSE clause (if any) as
	// the "first" input.
	// See https://www.postgresql.org/docs/15/typeconv-union-case.html#ftn.id-1.5.9.10.9.6.1.1.
	if expr.Else != nil {
		tmpExprs = append(tmpExprs, expr.Else)
	}
	for _, when := range expr.Whens {
		tmpExprs = append(tmpExprs, when.Val)
	}
	typedSubExprs, retType, err := typeCheckSameTypedExprs(ctx, semaCtx, desired, tmpExprs...)
	if err != nil {
		return nil, decorateTypeCheckError(err, "incompatible value type")
	}
	if expr.Else != nil {
		expr.Else = typedSubExprs[0]
		typedSubExprs = typedSubExprs[1:]
	}
	for i, whenVal := range typedSubExprs {
		expr.Whens[i].Val = whenVal
	}
	expr.typ = retType
	return expr, nil
}

func invalidCastError(castFrom, castTo *types.T) error {
	return pgerror.Newf(pgcode.CannotCoerce, "invalid cast: %s -> %s", castFrom, castTo)
}

// resolveCast checks that the cast from the two types is valid. If allowStable
// is false, it also checks that the cast has Immutable.
//
// On success, any relevant telemetry counters are incremented.
func resolveCast(context string, castFrom, castTo *types.T, allowStable bool) error {
	toFamily := castTo.Family()
	fromFamily := castFrom.Family()
	switch {
	case toFamily == types.ArrayFamily && fromFamily == types.ArrayFamily:
		err := resolveCast(context, castFrom.ArrayContents(), castTo.ArrayContents(), allowStable)
		if err != nil {
			return err
		}
		onCastTypeCheckHook(fromFamily, toFamily)
		return nil

	case toFamily == types.EnumFamily && fromFamily == types.EnumFamily:
		// Casts from ENUM to ENUM type can only succeed if the two types are the
		// same.
		if !castFrom.Equivalent(castTo) {
			return invalidCastError(castFrom, castTo)
		}
		onCastTypeCheckHook(fromFamily, toFamily)
		return nil

	case toFamily == types.TupleFamily && fromFamily == types.TupleFamily:
		// Casts from tuple to tuple type succeed if the lengths of the tuples are
		// the same, and if there are casts resolvable across all of the elements
		// pointwise. Casts to AnyTuple are always allowed since they are
		// implemented as a no-op.
		if castTo.Identical(types.AnyTuple) {
			return nil
		}
		fromTuple := castFrom.TupleContents()
		toTuple := castTo.TupleContents()
		if len(fromTuple) != len(toTuple) {
			return invalidCastError(castFrom, castTo)
		}
		for i, from := range fromTuple {
			to := toTuple[i]
			err := resolveCast(context, from, to, allowStable)
			if err != nil {
				return err
			}
		}
		onCastTypeCheckHook(fromFamily, toFamily)
		return nil

	default:
		cast, ok := cast.LookupCast(castFrom, castTo)
		if !ok {
			return invalidCastError(castFrom, castTo)
		}
		if !allowStable && cast.Volatility >= volatility.Stable {
			err := NewContextDependentOpsNotAllowedError(context)
			err = pgerror.Wrapf(err, pgcode.InvalidParameterValue, "%s::%s", castFrom, castTo)
			if cast.VolatilityHint != "" {
				err = errors.WithHint(err, cast.VolatilityHint)
			}
			return err
		}
		onCastTypeCheckHook(fromFamily, toFamily)
		return nil
	}
}

// CastCounterType represents a cast from one family to another.
type CastCounterType struct {
	From, To types.Family
}

// OnCastTypeCheck is a map of CastCounterTypes to their hook.
var OnCastTypeCheck map[CastCounterType]func()

// onCastTypeCheckHook performs the registered hook for the given cast on type check.
func onCastTypeCheckHook(from, to types.Family) {
	// The given map has not been populated, so do not expect any telemetry.
	if OnCastTypeCheck == nil {
		return
	}
	if f, ok := OnCastTypeCheck[CastCounterType{From: from, To: to}]; ok {
		f()
		return
	}
	panic(errors.AssertionFailedf("no cast counter found for cast from %s to %s", from.Name(), to.Name()))
}

func isArrayExpr(expr Expr) bool {
	_, ok := expr.(*Array)
	return ok
}

// TypeCheck implements the Expr interface.
func (expr *CastExpr) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, _ *types.T,
) (TypedExpr, error) {
	// The desired type provided to a CastExpr is ignored. Instead, types.AnyElement is
	// passed to the child of the cast. There are a few exceptions, described
	// below.
	desired := types.AnyElement
	exprType, err := ResolveType(ctx, expr.Type, semaCtx.GetTypeResolver())
	if err != nil {
		return nil, err
	}
	if err = CheckUnsupportedType(ctx, semaCtx, exprType); err != nil {
		return nil, err
	}
	expr.Type = exprType

	// Do not elide casts for placeholders, since if the statement gets re-prepared,
	// the cast may be needed to infer the placeholder type.
	_, isPlaceholder := expr.Expr.(*Placeholder)
	canElideCast := !isPlaceholder

	switch {
	case isConstant(expr.Expr):
		c := expr.Expr.(Constant)
		if canConstantBecome(c, exprType) {
			// If a Constant is subject to a cast which it can naturally become (which
			// is in its resolvable type set), we desire the cast's type for the
			// Constant. In many cases, the CastExpr will then become a no-op and will
			// be elided below. In other cases, the types may be equivalent but not
			// Identical (e.g. string::char(2) or oid::regclass) and the CastExpr is
			// still needed.
			desired = exprType
		}
	case semaCtx.isUnresolvedPlaceholder(expr.Expr):
		// If the placeholder has not yet been resolved, then we can make its
		// expected type be the cast type. If it already has been resolved, but the
		// type we gave it before is not compatible with the usage here, then
		// type-checking will fail as desired.
		desired = exprType
	case isArrayExpr(expr.Expr):
		// If we're going to cast to another array type, which is a common pattern
		// in SQL (select array[]::int[], select array[$1]::int[]), use the cast
		// type as the the desired type.
		if exprType.Family() == types.ArrayFamily {
			// We can't elide the cast for arrays if the underlying typmod information
			// is changed from the base type (e.g. `'{"hello", "world"}'::char(2)[]`
			// should not be elided or the char(2) is lost).
			// This needs to be checked here; otherwise the expr.Expr.TypeCheck below
			// will have the same resolved type as exprType, which forces an incorrect
			// elision.
			contents := exprType.ArrayContents()
			if baseType, ok := types.OidToType[contents.Oid()]; ok && !baseType.Identical(contents) {
				canElideCast = false
			}
			desired = exprType
		}
	}

	typedSubExpr, err := expr.Expr.TypeCheck(ctx, semaCtx, desired)
	if err != nil {
		return nil, err
	}

	// Elide the cast if it is a no-op.
	if canElideCast && typedSubExpr.ResolvedType().Identical(exprType) {
		return typedSubExpr, nil
	}

	castFrom := typedSubExpr.ResolvedType()
	allowStable := true
	context := ""
	if semaCtx != nil && semaCtx.Properties.IsSet(RejectStableOperators) {
		allowStable = false
		context = semaCtx.Properties.required.context
	}
	err = resolveCast(context, castFrom, exprType, allowStable)
	if err != nil {
		return nil, err
	}
	if exprType.Identical(types.Trigger) {
		// Trigger is not allowed in casts. This happens after resolving the cast to
		// ensure that we return an "invalid cast" error when postgres does.
		return nil, CannotAcceptTriggerErr
	}
	expr.Expr = typedSubExpr
	expr.Type = exprType
	expr.typ = exprType
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *IndirectionExpr) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	subExpr, err := expr.Expr.TypeCheck(ctx, semaCtx, types.MakeArray(desired))
	if err != nil {
		return nil, err
	}
	typ := subExpr.ResolvedType()
	expr.Expr = subExpr

	switch typ.Family() {
	case types.ArrayFamily:
		expr.typ = typ.ArrayContents()
		for i, t := range expr.Indirection {
			if t.Slice {
				return nil, unimplemented.NewWithIssuef(32551, "ARRAY slicing in %s", expr)
			}
			if i > 0 {
				return nil, unimplemented.NewWithIssueDetailf(32552, "ind", "multidimensional indexing: %s", expr)
			}

			beginExpr, err := typeCheckAndRequire(ctx, semaCtx, t.Begin, types.Int, "ARRAY subscript")
			if err != nil {
				return nil, err
			}
			t.Begin = beginExpr
		}

		if OnTypeCheckArraySubscript != nil {
			OnTypeCheckArraySubscript()
		}
	case types.JsonFamily:
		expr.typ = typ
		for _, t := range expr.Indirection {
			if t.Slice {
				return nil, pgerror.Newf(pgcode.DatatypeMismatch, "jsonb subscript does not support slices")
			}
			beginExpr, err := t.Begin.TypeCheck(ctx, semaCtx, types.AnyElement)
			if err != nil {
				return nil, err
			}
			switch beginExpr.ResolvedType().Family() {
			case types.IntFamily, types.StringFamily, types.UnknownFamily:
			default:
				return nil, errors.WithHint(
					pgerror.Newf(
						pgcode.DatatypeMismatch,
						"unexpected JSON subscript type: %s",
						beginExpr.ResolvedType().SQLString(),
					),
					"subscript type must be integer or text",
				)
			}
			t.Begin = beginExpr
		}

		if OnTypeCheckJSONBSubscript != nil {
			OnTypeCheckJSONBSubscript()
		}
	default:
		return nil, pgerror.Newf(pgcode.DatatypeMismatch, "cannot subscript type %s because it is not an array or json object", typ)
	}
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *AnnotateTypeExpr) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	annotateType, err := ResolveType(ctx, expr.Type, semaCtx.GetTypeResolver())
	if err != nil {
		return nil, err
	}
	if annotateType.Identical(types.Trigger) {
		return nil, CannotAcceptTriggerErr
	}
	if err = CheckUnsupportedType(ctx, semaCtx, annotateType); err != nil {
		return nil, err
	}
	expr.Type = annotateType
	subExpr, err := typeCheckAndRequire(
		ctx,
		semaCtx,
		expr.Expr,
		annotateType,
		fmt.Sprintf(
			"type annotation for %v as %s, found",
			expr.Expr,
			annotateType,
		),
	)
	if err != nil {
		return nil, err
	}
	return subExpr, nil
}

// TypeCheck implements the Expr interface.
func (expr *CollateExpr) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	subExpr, err := expr.Expr.TypeCheck(ctx, semaCtx, types.String)
	if err != nil {
		return nil, err
	}
	if collatedstring.IsDefaultEquivalentCollation(expr.Locale) {
		return subExpr, nil
	}
	if _, err := language.Parse(expr.Locale); err != nil {
		return nil, pgerror.Wrapf(err, pgcode.InvalidParameterValue,
			"invalid locale %s", expr.Locale)
	}
	// setType is a recursive helper function to handle type checking of COLLATE
	// on arrays.
	var setType func(t *types.T) (*CollateExpr, error)
	setType = func(t *types.T) (*CollateExpr, error) {
		if types.IsStringType(t) {
			expr.Expr = subExpr
			expr.typ = types.MakeCollatedString(t, expr.Locale)
			return expr, nil
		} else if t.Family() == types.ArrayFamily {
			if e, err := setType(t.ArrayContents()); err == nil {
				e.typ = types.MakeArray(e.typ)
				return e, nil
			}
		} else if t.Family() == types.UnknownFamily {
			expr.Expr = subExpr
			expr.typ = types.MakeCollatedString(types.String, expr.Locale)
			return expr, nil
		}
		return nil, pgerror.Newf(pgcode.DatatypeMismatch,
			"incompatible type for COLLATE: %s", t)
	}
	return setType(subExpr.ResolvedType())
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
func (expr *TupleStar) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	subExpr, err := expr.Expr.TypeCheck(ctx, semaCtx, desired)
	if err != nil {
		return nil, err
	}
	expr.Expr = subExpr
	resolvedType := subExpr.ResolvedType()

	// We need to ensure the expression is a tuple.
	if resolvedType.Family() != types.TupleFamily {
		return nil, NewTypeIsNotCompositeError(resolvedType)
	}

	return subExpr, err
}

// ResolvedType implements the TypedExpr interface.
func (expr *TupleStar) ResolvedType() *types.T {
	return expr.Expr.(TypedExpr).ResolvedType()
}

// TypeCheck implements the Expr interface.
func (expr *ColumnAccessExpr) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	// If the context requires types T, we need to ask "Any tuple with
	// at least this label and the element type T for this label" from
	// the sub-expression. Of course, our type system does not support
	// this. So drop the type constraint instead.
	subExpr, err := expr.Expr.TypeCheck(ctx, semaCtx, types.AnyElement)
	if err != nil {
		return nil, err
	}

	expr.Expr = subExpr
	resolvedType := subExpr.ResolvedType()

	if resolvedType.Family() != types.TupleFamily {
		return nil, NewTypeIsNotCompositeError(resolvedType)
	}

	if !expr.ByIndex && len(resolvedType.TupleLabels()) == 0 {
		return nil, pgerror.Newf(pgcode.UndefinedColumn, "could not identify column %q in record data type",
			expr.ColName)
	}

	if expr.ByIndex {
		// By-index reference. Verify that the index is valid.
		if expr.ColIndex < 0 || expr.ColIndex >= len(resolvedType.TupleContents()) {
			return nil, pgerror.Newf(pgcode.Syntax, "tuple column %d does not exist", expr.ColIndex+1)
		}
	} else {
		// Go through all of the labels to find a match.
		expr.ColIndex = -1
		for i, label := range resolvedType.TupleLabels() {
			if label == string(expr.ColName) {
				if expr.ColIndex != -1 {
					// Found a duplicate label.
					return nil, pgerror.Newf(pgcode.AmbiguousColumn, "column reference %q is ambiguous", label)
				}
				expr.ColIndex = i
			}
		}
		if expr.ColIndex < 0 {
			return nil, pgerror.Newf(pgcode.UndefinedColumn,
				"could not identify column %q in %s",
				ErrString(&expr.ColName), resolvedType,
			)
		}
	}

	// Optimization: if the expression is actually a tuple, then
	// simplify the tuple straight away.
	if tExpr, ok := expr.Expr.(*Tuple); ok {
		return tExpr.Exprs[expr.ColIndex].(TypedExpr), nil
	}

	// Otherwise, let the expression be, it's probably more complex.
	// Just annotate the type of the result properly.
	expr.typ = resolvedType.TupleContents()[expr.ColIndex]
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *CoalesceExpr) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	if semaCtx != nil {
		defer semaCtx.Properties.Ancestors.PopTo(semaCtx.Properties.Ancestors)
		semaCtx.Properties.Ancestors.Push(ConditionalAncestor)
	}
	typedSubExprs, retType, err := typeCheckSameTypedExprs(ctx, semaCtx, desired, expr.Exprs...)
	if err != nil {
		return nil, decorateTypeCheckError(err, "incompatible %s expressions", redact.Safe(expr.Name))
	}

	for i, subExpr := range typedSubExprs {
		expr.Exprs[i] = subExpr
	}
	expr.typ = retType
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *ComparisonExpr) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	var leftTyped, rightTyped TypedExpr
	var cmpOp *CmpOp
	var cmpOpSym treecmp.ComparisonOperatorSymbol
	var alwaysNull bool
	var err error
	if expr.Operator.Symbol.HasSubOperator() {
		leftTyped, rightTyped, cmpOp, alwaysNull, err = typeCheckComparisonOpWithSubOperator(
			ctx,
			semaCtx,
			expr.Operator,
			expr.SubOperator,
			expr.Left,
			expr.Right,
		)
		cmpOpSym = expr.SubOperator.Symbol
	} else {
		leftTyped, rightTyped, cmpOp, alwaysNull, err = typeCheckComparisonOp(
			ctx,
			semaCtx,
			expr.Operator,
			expr.Left,
			expr.Right,
		)
		cmpOpSym = expr.Operator.Symbol
	}
	if err == nil {
		err = runValidations(cmpOpSym, leftTyped.ResolvedType(), rightTyped.ResolvedType(),
			[]types.Family{types.RefCursorFamily, types.JsonpathFamily})
	}
	if err != nil {
		return nil, err
	}

	if alwaysNull {
		return DNull, nil
	}

	if err := semaCtx.checkVolatility(cmpOp.Volatility); err != nil {
		return nil, pgerror.Wrapf(err, pgcode.InvalidParameterValue, "%s", expr.Operator)
	}

	if cmpOp.OnTypeCheck != nil {
		cmpOp.OnTypeCheck()
	}

	expr.Left, expr.Right = leftTyped, rightTyped
	expr.Op = cmpOp
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
	var code pgcode.Code
	switch class {
	case AggregateClass:
		cat = "aggregate"
		code = pgcode.Grouping
	case WindowClass:
		cat = "window"
		code = pgcode.Windowing
	case GeneratorClass:
		cat = "set-returning"
		code = pgcode.FeatureNotSupported
	}
	return pgerror.Newf(code, "%s functions are not allowed in %s", cat, context)
}

// checkFunctionUsage checks whether a given built-in function is
// allowed in the current context.
func (sc *SemaContext) checkFunctionUsage(expr *FuncExpr, def *ResolvedFunctionDefinition) error {
	if sc == nil {
		// We can't check anything further. Give up.
		return nil
	}

	// TODO(Chengxiong): Consider doing this check when we narrow down to an
	// overload. This is fine at the moment since we don't allow creating
	// aggregate/window functions yet. But, ideally, we should figure out a way
	// to do this check after overload resolution.
	fnCls, err := def.GetClass()
	if err != nil {
		return err
	}
	if expr.IsWindowFunctionApplication() {
		if sc.Properties.IsSet(RejectWindowApplications) {
			return NewInvalidFunctionUsageError(WindowClass, sc.Properties.required.context)
		}

		if sc.Properties.Ancestors.Has(WindowFuncAncestor) &&
			sc.Properties.IsSet(RejectNestedWindowFunctions) {
			return pgerror.Newf(pgcode.Windowing, "window function calls cannot be nested")
		}
		sc.Properties.Derived.SeenWindowApplication = true
	} else {
		// If it is an aggregate function *not used OVER a window*, then
		// we have an aggregation.
		if fnCls == AggregateClass {
			if sc.Properties.Ancestors.Has(FuncExprAncestor) &&
				sc.Properties.IsSet(RejectNestedAggregates) {
				return NewAggInAggError()
			}
			if sc.Properties.IsSet(RejectAggregates) {
				return NewInvalidFunctionUsageError(AggregateClass, sc.Properties.required.context)
			}
			sc.Properties.Derived.SeenAggregate = true
		}
	}
	if fnCls == GeneratorClass {
		if sc.Properties.Ancestors.Has(FuncExprAncestor) &&
			sc.Properties.IsSet(RejectNestedGenerators) {
			return NewInvalidNestedSRFError(sc.Properties.required.context)
		}
		if sc.Properties.IsSet(RejectGenerators) {
			return NewInvalidFunctionUsageError(GeneratorClass, sc.Properties.required.context)
		}
		if sc.Properties.Ancestors.Has(ConditionalAncestor) {
			return NewInvalidFunctionUsageError(GeneratorClass, "conditional expressions")
		}
		sc.Properties.Derived.SeenGenerator = true
	}
	return nil
}

// NewContextDependentOpsNotAllowedError creates an error for the case when
// context-dependent operators are not allowed in the given context.
func NewContextDependentOpsNotAllowedError(context string) error {
	// The code FeatureNotSupported is a bit misleading here,
	// because we probably can't support the feature at all. However
	// this error code matches PostgreSQL's in the same conditions.
	return pgerror.Newf(pgcode.FeatureNotSupported,
		"context-dependent operators are not allowed in %s", context,
	)
}

// TypeCheckContext returns the semantic analysis context, for use in creating
// error messages.
func (sc *SemaContext) TypeCheckContext() string {
	return sc.Properties.required.context
}

// checkVolatility checks whether an operator with the given Volatility is
// allowed in the current context.
func (sc *SemaContext) checkVolatility(v volatility.V) error {
	if sc == nil {
		return nil
	}
	switch v {
	case volatility.Volatile:
		if sc.Properties.IsSet(RejectVolatileFunctions) {
			// The code FeatureNotSupported is a bit misleading here,
			// because we probably can't support the feature at all. However
			// this error code matches PostgreSQL's in the same conditions.
			return pgerror.Newf(pgcode.FeatureNotSupported,
				"volatile functions are not allowed in %s", sc.Properties.required.context)
		}
	case volatility.Stable:
		if sc.Properties.IsSet(RejectStableOperators) {
			return NewContextDependentOpsNotAllowedError(sc.Properties.required.context)
		}
	}
	return nil
}

// CheckIsWindowOrAgg returns an error if the function definition is not a
// window function or an aggregate.
func CheckIsWindowOrAgg(def *ResolvedFunctionDefinition) error {
	cls, err := def.GetClass()
	if err != nil {
		return err
	}
	switch cls {
	case AggregateClass:
	case WindowClass:
	default:
		return pgerror.Newf(pgcode.WrongObjectType,
			"OVER specified, but %s() is neither a window function nor an aggregate function",
			def.Name)
	}
	return nil
}

func (expr *FuncExpr) typeCheckWithFuncAncestor(semaCtx *SemaContext, fn func() error) error {
	if semaCtx != nil {
		defer semaCtx.Properties.Restore(semaCtx.Properties)
		// We'll need to remember we are in a function application to generate
		// suitable errors in checkFunctionUsage().
		semaCtx.Properties.Ancestors.Push(FuncExprAncestor)
		if expr.WindowDef != nil {
			semaCtx.Properties.Ancestors.Push(WindowFuncAncestor)
		}
		// Disallow procedures in function arguments.
		semaCtx.Properties.Reject(RejectProcedures)
	}
	return fn()
}

// TypeCheck implements the Expr interface.
func (expr *FuncExpr) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	if expr.fn != nil && expr.fn.Type != BuiltinRoutine && expr.typ != nil {
		// Don't overwrite the resolved properties for a user-defined routine if the
		// routine has already been resolved.
		return expr, nil
	}

	searchPath := EmptySearchPath
	var resolver FunctionReferenceResolver
	if semaCtx != nil {
		if semaCtx.SearchPath != nil {
			searchPath = semaCtx.SearchPath
		}
		resolver = semaCtx.FunctionResolver
	}

	_, functionResolvedByID := expr.Func.FunctionReference.(*FunctionOID)

	def, err := expr.Func.Resolve(ctx, searchPath, resolver)
	if err != nil {
		if errors.Is(err, ErrRoutineUndefined) && expr.InCall {
			return nil, errors.WithHint(
				pgerror.Newf(pgcode.UndefinedFunction, "procedure %s does not exist", expr.Func),
				"No procedure matches the given name and argument types. "+
					"You might need to add explicit type casts.",
			)
		}
		return nil, err
	}

	if err := semaCtx.checkFunctionUsage(expr, def); err != nil {
		return nil, pgerror.Wrapf(err, pgcode.InvalidParameterValue,
			"%s()", def.Name)
	}

	typeNames := func(typedExprs []TypedExpr) string {
		var sb strings.Builder
		sb.WriteByte('(')
		for i := range typedExprs {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(typedExprs[i].ResolvedType().Name())
		}
		sb.WriteByte(')')
		return sb.String()
	}

	s := getOverloadTypeChecker(
		(*qualifiedOverloads)(&def.Overloads), expr.Exprs...,
	)
	defer s.release()

	if err = expr.typeCheckWithFuncAncestor(semaCtx, func() error {
		if err := s.typeCheckOverloadedExprs(ctx, semaCtx, desired, false /* inBinOp */); err != nil {
			return pgerror.Wrapf(err, pgcode.InvalidParameterValue, "%s()", def.Name)
		}
		if expr.InCall && len(s.overloadIdxs) == 0 {
			// Since we didn't find the overload, let's see whether the user
			// made a mistake and attempted to call a UDF. We attempt this by
			// temporarily adjusting the RoutineType of each UDF to be
			// Procedure, then running type-checking on adjusted overloads, and
			// resetting the UDF overloads to their original state.
			var functionIdxs []int
			var functionOverloads []QualifiedOverload
			for idx, o := range def.Overloads {
				if o.Type == UDFRoutine {
					o.Type = ProcedureRoutine
					functionIdxs = append(functionIdxs, idx)
					functionOverloads = append(functionOverloads, o)
				}
			}
			if len(functionIdxs) > 0 {
				defer func() {
					for _, idx := range functionIdxs {
						def.Overloads[idx].Type = UDFRoutine
					}
				}()
				s2 := getOverloadTypeChecker((*qualifiedOverloads)(&functionOverloads), expr.Exprs...)
				defer s2.release()
				err2 := s2.typeCheckOverloadedExprs(ctx, semaCtx, desired, false /* inBinOp */)
				if err2 == nil && len(s2.overloadIdxs) > 0 {
					// This time we found a match, so return the proper error.
					return errors.WithHint(
						pgerror.Newf(
							pgcode.WrongObjectType,
							"%s%s is not a procedure", def.Name, typeNames(s2.typedExprs),
						),
						"To call a function, use SELECT.",
					)
				}
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	var hasUDFOverload bool
	var calledOnNullInputFns, notCalledOnNullInputFns intsets.Fast
	for _, idx := range s.overloadIdxs {
		if def.Overloads[idx].CalledOnNullInput {
			calledOnNullInputFns.Add(int(idx))
		} else {
			notCalledOnNullInputFns.Add(int(idx))
		}
		// TODO(harding): Check if this is a record-returning UDF instead.
		if def.Overloads[idx].Type == UDFRoutine {
			hasUDFOverload = true
		}
	}

	// If the function is an aggregate that does not accept null arguments and we
	// have arguments of unknown type, see if we can assign type string instead.
	// TODO(rytaft): If there are no overloads with string inputs, Postgres
	// chooses the overload with preferred type for the given category. For
	// example, float8 is the preferred type for the numeric category in Postgres.
	// To match Postgres' behavior, we should add that logic here too.
	funcCls, err := def.GetClass()
	if err != nil {
		return nil, err
	}
	if funcCls == AggregateClass {
		for i := range s.typedExprs {
			if s.typedExprs[i].ResolvedType().Family() == types.UnknownFamily {
				var filtered intsets.Fast
				for j, ok := notCalledOnNullInputFns.Next(0); ok; j, ok = notCalledOnNullInputFns.Next(j + 1) {
					if def.Overloads[j].params().GetAt(i).Equivalent(types.String) {
						filtered.Add(j)
					}
				}

				// Only use the filtered list if it's not empty.
				if filtered.Len() > 0 {
					notCalledOnNullInputFns = filtered

					// Cast the expression to a string so the execution engine will find
					// the correct overload.
					s.typedExprs[i] = NewTypedCastExpr(s.typedExprs[i], types.String)
				}
			}
		}
		truncated := s.overloadIdxs[:0]
		for _, idx := range s.overloadIdxs {
			if calledOnNullInputFns.Contains(int(idx)) ||
				notCalledOnNullInputFns.Contains(int(idx)) {
				truncated = append(truncated, idx)
			}
		}
		s.overloadIdxs = truncated
	}

	if len(s.overloadIdxs) == 0 {
		if expr.InCall {
			return nil, errors.WithHint(
				pgerror.Newf(pgcode.UndefinedFunction, "procedure %s%s does not exist",
					def.Name, typeNames(s.typedExprs)),
				"No procedure matches the given name and argument types. "+
					"You might need to add explicit type casts.",
			)
		}
		return nil, errors.WithHint(
			pgerror.Newf(pgcode.UndefinedFunction, "unknown signature: %s", getFuncSig(expr, s.typedExprs, desired)),
			"No function matches the given name and argument types. You might need to add explicit type casts.",
		)
	}

	// Return NULL if at least one overload is possible, no overload accepts
	// NULL arguments, the function isn't a generator or aggregate builtin, and
	// NULL is given as an argument. We do not perform this transformation
	// within a CALL statement because procedures are always called on NULL
	// input, and because optbuilder requires a FuncExpr to remain after
	// type-checking.
	if len(s.overloadIdxs) > 0 && calledOnNullInputFns.Len() == 0 && funcCls != GeneratorClass &&
		funcCls != AggregateClass && !hasUDFOverload &&
		semaCtx != nil && !expr.InCall {
		for _, expr := range s.typedExprs {
			if expr.ResolvedType().Family() == types.UnknownFamily {
				return DNull, nil
			}
		}
	}

	var favoredOverload QualifiedOverload
	if functionResolvedByID {
		// If the function is resolved by OID, we know that there is always only one
		// overload qualified. As long as it passes the argument type checks above,
		// there is no need to worry about the search path.
		favoredOverload = def.Overloads[0]
	} else {
		// Get overloads from the most significant schema in search path.
		favoredOverload, err = getMostSignificantOverload(
			def.Overloads, s.overloads, s.overloadIdxs, searchPath, expr, s.typedExprs,
			func() string { return getFuncSig(expr, s.typedExprs, desired) },
		)
		if err != nil {
			return nil, err
		}
	}

	// Just pick the first overload from the search path.
	overloadImpl := favoredOverload.Overload
	if overloadImpl.Private {
		return nil, pgerror.Wrapf(errPrivateFunction, pgcode.ReservedName,
			"%s()", errors.Safe(def.Name))
	}
	if resolver != nil && overloadImpl.UDFContainsOnlySignature {
		_, overloadImpl, err = resolver.ResolveFunctionByOID(ctx, overloadImpl.Oid)
		if err != nil {
			return nil, err
		}
	}

	// Some builtins are disabled in certain contexts for Postgres compatibility.
	if overloadImpl.Type == BuiltinRoutine {
		switch def.Name {
		case "min", "max":
			// Special case: for REFCURSOR, we disallow min/max during type-checking
			// despite having overloads for REFCURSOR. This maintains compatibility
			// with postgres without having to add special checks in optimizer rules
			// for REFCURSOR.
			if len(s.typedExprs) > 0 && s.typedExprs[0].ResolvedType().Family() == types.RefCursorFamily {
				return nil, pgerror.Newf(pgcode.UndefinedFunction, "function %s(refcursor) does not exist", def.Name)
			}
		}
	}

	if overloadImpl.Type == ProcedureRoutine && semaCtx.Properties.IsSet(RejectProcedures) {
		return nil, errors.WithHint(
			pgerror.Newf(
				pgcode.WrongObjectType,
				"%s(%s) is a procedure", def.Name, overloadImpl.Types.String(),
			),
			"To call a procedure, use CALL.",
		)
	}

	if expr.IsWindowFunctionApplication() {
		// Make sure the window function application is of either a built-in window
		// function or of a builtin aggregate function.
		if overloadImpl.Class != WindowClass && overloadImpl.Class != AggregateClass {
			return nil, pgerror.Newf(pgcode.WrongObjectType,
				"OVER specified, but %s() is neither a window function nor an aggregate function",
				def.Name)
		}
		if expr.Type == DistinctFuncType {
			return nil, pgerror.New(pgcode.FeatureNotSupported, "DISTINCT is not implemented for window functions")
		}
	} else {
		// Make sure the window function builtins are used as window function applications.
		if !expr.InCall && funcCls == WindowClass {
			return nil, pgerror.Newf(pgcode.WrongObjectType,
				"window function %s() requires an OVER clause", &expr.Func)
		}
	}

	if expr.Filter != nil {
		if funcCls != AggregateClass {
			// Same error message as Postgres. If we have a window function, only
			// aggregates accept a FILTER clause.
			return nil, pgerror.Newf(pgcode.WrongObjectType,
				"FILTER specified but %s() is not an aggregate function", &expr.Func)
		}

		if err = expr.typeCheckWithFuncAncestor(semaCtx, func() error {
			typedFilter, err := typeCheckAndRequireBoolean(ctx, semaCtx, expr.Filter, "FILTER expression")
			if err != nil {
				return err
			}
			expr.Filter = typedFilter
			return nil
		}); err != nil {
			return nil, err
		}
	}

	if expr.OrderBy != nil {
		if err = expr.typeCheckWithFuncAncestor(semaCtx, func() error {
			for i := range expr.OrderBy {
				typedExpr, err := expr.OrderBy[i].Expr.TypeCheck(ctx, semaCtx, types.AnyElement)
				if err != nil {
					return err
				}
				expr.OrderBy[i].Expr = typedExpr
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}

	for i, subExpr := range s.typedExprs {
		expr.Exprs[i] = subExpr
	}

	expr.Func.FunctionReference = def
	expr.fn = overloadImpl
	expr.fnProps = &overloadImpl.FunctionProperties
	expr.typ = overloadImpl.returnType()(s.typedExprs)
	if expr.typ == UnknownReturnType {
		typeNames := make([]string, 0, len(expr.Exprs))
		for _, expr := range s.typedExprs {
			typeNames = append(typeNames, expr.ResolvedType().String())
		}
		return nil, pgerror.Newf(
			pgcode.DatatypeMismatch,
			"could not determine polymorphic type: %s(%s)",
			&expr.Func,
			strings.Join(typeNames, ", "),
		)
	}
	if err := semaCtx.checkVolatility(overloadImpl.Volatility); err != nil {
		return nil, pgerror.Wrapf(err, pgcode.InvalidParameterValue, "%s()", def.Name)
	}
	if overloadImpl.OnTypeCheck != nil {
		overloadImpl.OnTypeCheck()
	}
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *IfErrExpr) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	var typedCond, typedElse TypedExpr
	var retType *types.T
	var err error
	if expr.Else == nil {
		typedCond, err = expr.Cond.TypeCheck(ctx, semaCtx, types.AnyElement)
		if err != nil {
			return nil, err
		}
		retType = types.Bool
	} else {
		var typedSubExprs []TypedExpr
		typedSubExprs, retType, err = typeCheckSameTypedExprs(ctx, semaCtx, desired, expr.Cond, expr.Else)
		if err != nil {
			return nil, decorateTypeCheckError(err, "incompatible IFERROR expressions")
		}
		typedCond, typedElse = typedSubExprs[0], typedSubExprs[1]
	}

	var typedErrCode TypedExpr
	if expr.ErrCode != nil {
		typedErrCode, err = typeCheckAndRequire(ctx, semaCtx, expr.ErrCode, types.String, "IFERROR")
		if err != nil {
			return nil, err
		}
	}

	expr.Cond = typedCond
	expr.Else = typedElse
	expr.ErrCode = typedErrCode
	expr.typ = retType

	if OnTypeCheckIfErr != nil {
		OnTypeCheckIfErr()
	}
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *IfExpr) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	if semaCtx != nil {
		defer semaCtx.Properties.Ancestors.PopTo(semaCtx.Properties.Ancestors)
		semaCtx.Properties.Ancestors.Push(ConditionalAncestor)
	}
	typedCond, err := typeCheckAndRequireBoolean(ctx, semaCtx, expr.Cond, "IF condition")
	if err != nil {
		return nil, err
	}

	typedSubExprs, retType, err := typeCheckSameTypedExprs(ctx, semaCtx, desired, expr.True, expr.Else)
	if err != nil {
		return nil, decorateTypeCheckError(err, "incompatible IF expressions")
	}

	expr.Cond = typedCond
	expr.True, expr.Else = typedSubExprs[0], typedSubExprs[1]
	expr.typ = retType
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *IsOfTypeExpr) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	exprTyped, err := expr.Expr.TypeCheck(ctx, semaCtx, types.AnyElement)
	if err != nil {
		return nil, err
	}
	expr.resolvedTypes = make([]*types.T, len(expr.Types))
	for i := range expr.Types {
		typ, err := ResolveType(ctx, expr.Types[i], semaCtx.GetTypeResolver())
		if err != nil {
			return nil, err
		}
		expr.Types[i] = typ
		expr.resolvedTypes[i] = typ
	}
	expr.Expr = exprTyped
	expr.typ = types.Bool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *NotExpr) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	exprTyped, err := typeCheckAndRequireBoolean(ctx, semaCtx, expr.Expr, "NOT argument")
	if err != nil {
		return nil, err
	}
	expr.Expr = exprTyped
	expr.typ = types.Bool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *IsNullExpr) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	exprTyped, err := expr.Expr.TypeCheck(ctx, semaCtx, types.AnyElement)
	if err != nil {
		return nil, err
	}
	expr.Expr = exprTyped
	expr.typ = types.Bool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *IsNotNullExpr) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	exprTyped, err := expr.Expr.TypeCheck(ctx, semaCtx, types.AnyElement)
	if err != nil {
		return nil, err
	}
	expr.Expr = exprTyped
	expr.typ = types.Bool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *NullIfExpr) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	typedSubExprs, retType, err := typeCheckSameTypedExprs(ctx, semaCtx, desired, expr.Expr1, expr.Expr2)
	if err != nil {
		return nil, decorateTypeCheckError(err, "incompatible NULLIF expressions")
	}
	leftType := typedSubExprs[0].ResolvedType()
	rightType := typedSubExprs[1].ResolvedType()
	_, ok := CmpOps[treecmp.EQ].LookupImpl(leftType, rightType)
	if !ok {
		op := treecmp.ComparisonOperator{Symbol: treecmp.EQ}
		err = pgerror.Newf(
			pgcode.UndefinedFunction, "unsupported comparison operator: <%s> %s <%s>",
			leftType, op, rightType)
		return nil, decorateTypeCheckError(err, "incompatible NULLIF expressions")
	}
	err = checkComparison(treecmp.EQ, leftType, rightType, types.RefCursorFamily)
	if err != nil {
		return nil, err
	}

	expr.Expr1, expr.Expr2 = typedSubExprs[0], typedSubExprs[1]
	expr.typ = retType
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *OrExpr) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	leftTyped, err := typeCheckAndRequireBoolean(ctx, semaCtx, expr.Left, "OR argument")
	if err != nil {
		return nil, err
	}
	rightTyped, err := typeCheckAndRequireBoolean(ctx, semaCtx, expr.Right, "OR argument")
	if err != nil {
		return nil, err
	}
	expr.Left, expr.Right = leftTyped, rightTyped
	expr.typ = types.Bool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *ParenExpr) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	exprTyped, err := expr.Expr.TypeCheck(ctx, semaCtx, desired)
	if err != nil {
		return nil, err
	}
	// Parentheses are semantically unimportant and in most cases can be
	// removed/replaced with its nested expression in our plan. This makes type
	// checking cleaner. Collated string expressions need the parentheses to
	// parse correctly in some cases.
	if _, ok := exprTyped.(*CollateExpr); ok {
		expr.Expr = exprTyped
		expr.typ = exprTyped.ResolvedType()
		return expr, nil
	}
	return exprTyped, nil
}

// TypeCheck implements the Expr interface.  This function has a valid
// implementation only for testing within this package. During query
// execution, ColumnItems are replaced to IndexedVars prior to type
// checking.
func (expr *ColumnItem) TypeCheck(
	_ context.Context, _ *SemaContext, desired *types.T,
) (TypedExpr, error) {
	name := expr.String()
	if _, ok := presetTypesForTesting[name]; ok {
		return expr, nil
	}
	return nil, pgerror.Newf(pgcode.UndefinedColumn,
		"column %q does not exist", ErrString(expr))
}

// TypeCheck implements the Expr interface.
func (expr UnqualifiedStar) TypeCheck(
	_ context.Context, _ *SemaContext, desired *types.T,
) (TypedExpr, error) {
	return nil, errStarNotAllowed
}

// TypeCheck implements the Expr interface.
func (expr *UnresolvedName) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	v, err := expr.NormalizeVarName()
	if err != nil {
		return nil, err
	}
	return v.TypeCheck(ctx, semaCtx, desired)
}

// TypeCheck implements the Expr interface.
func (expr *AllColumnsSelector) TypeCheck(
	_ context.Context, _ *SemaContext, desired *types.T,
) (TypedExpr, error) {
	return nil, pgerror.Newf(pgcode.Syntax, "cannot use %q in this context", expr)
}

// TypeCheck implements the Expr interface.
func (expr *RangeCond) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	leftFromTyped, fromTyped, _, _, err := typeCheckComparisonOp(ctx, semaCtx, treecmp.MakeComparisonOperator(treecmp.GT), expr.Left, expr.From)
	if err == nil {
		err = checkComparison(treecmp.GT, leftFromTyped.ResolvedType(), fromTyped.ResolvedType(), types.RefCursorFamily)
	}
	if err != nil {
		return nil, err
	}
	leftToTyped, toTyped, _, _, err := typeCheckComparisonOp(ctx, semaCtx, treecmp.MakeComparisonOperator(treecmp.LT), expr.Left, expr.To)
	if err == nil {
		err = checkComparison(treecmp.LT, leftToTyped.ResolvedType(), toTyped.ResolvedType(), types.RefCursorFamily)
	}
	if err != nil {
		return nil, err
	}
	// Ensure that the boundaries of the comparison are well typed.
	_, _, _, _, err = typeCheckComparisonOp(ctx, semaCtx, treecmp.MakeComparisonOperator(treecmp.LT), expr.From, expr.To)
	if err != nil {
		return nil, err
	}
	expr.Left, expr.From = leftFromTyped, fromTyped
	expr.leftTo, expr.To = leftToTyped, toTyped
	expr.typ = types.Bool
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *Subquery) TypeCheck(_ context.Context, sc *SemaContext, _ *types.T) (TypedExpr, error) {
	if sc != nil && sc.Properties.IsSet(RejectSubqueries) {
		return nil, pgerror.Newf(pgcode.FeatureNotSupported,
			"subqueries are not allowed in %s", sc.Properties.required.context)
	}
	expr.assertTyped()
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *UnaryExpr) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	ops := UnaryOps[expr.Operator.Symbol]

	s := getOverloadTypeChecker(ops, expr.Expr)
	defer s.release()
	if err := s.typeCheckOverloadedExprs(ctx, semaCtx, desired, false); err != nil {
		return nil, err
	}

	typedSubExprs := s.typedExprs
	exprTyped := typedSubExprs[0]
	exprReturn := exprTyped.ResolvedType()

	// Return NULL if at least one overload is possible and NULL is an argument.
	numOps := len(s.overloadIdxs)
	if numOps > 0 {
		if exprReturn.Family() == types.UnknownFamily {
			return DNull, nil
		}
	}

	// Throw a typing error if overload resolution found either no compatible candidates
	// or if it found an ambiguity.
	if numOps != 1 {
		var desStr string
		if desired.Family() != types.AnyFamily {
			desStr = fmt.Sprintf(" (returning <%s>)", desired)
		}
		sig := fmt.Sprintf("%s <%s>%s", expr.Operator, exprReturn, desStr)
		if numOps == 0 {
			return nil,
				pgerror.Newf(pgcode.InvalidParameterValue, unsupportedUnaryOpErrFmt, sig)
		}
		fnsStr := formatCandidates(
			expr.Operator.String(), s.overloads, s.overloadIdxs,
		)
		err := pgerror.Newf(pgcode.AmbiguousFunction, ambiguousUnaryOpErrFmt, sig)
		err = errors.WithHintf(err, candidatesHintFmt, fnsStr)
		return nil, err
	}

	unaryOp := ops.overloads[s.overloadIdxs[0]]
	if err := semaCtx.checkVolatility(unaryOp.Volatility); err != nil {
		return nil, pgerror.Wrapf(err, pgcode.InvalidParameterValue, "%s", expr.Operator)
	}

	if unaryOp.OnTypeCheck != nil {
		unaryOp.OnTypeCheck()
	}

	expr.Expr = exprTyped
	expr.op = unaryOp
	expr.typ = unaryOp.returnType()(typedSubExprs)
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr DefaultVal) TypeCheck(
	_ context.Context, _ *SemaContext, desired *types.T,
) (TypedExpr, error) {
	return nil, errInvalidDefaultUsage
}

// TypeCheck implements the Expr interface.
func (expr PartitionMinVal) TypeCheck(
	_ context.Context, _ *SemaContext, desired *types.T,
) (TypedExpr, error) {
	return nil, errInvalidMinUsage
}

// TypeCheck implements the Expr interface.
func (expr PartitionMaxVal) TypeCheck(
	_ context.Context, _ *SemaContext, desired *types.T,
) (TypedExpr, error) {
	return nil, errInvalidMaxUsage
}

// TypeCheck implements the Expr interface.
func (expr *NumVal) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	return typeCheckConstant(ctx, semaCtx, expr, desired)
}

// TypeCheck implements the Expr interface.
func (expr *StrVal) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	return typeCheckConstant(ctx, semaCtx, expr, desired)
}

// TypeCheck implements the Expr interface.
func (expr *Tuple) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	// Ensure the number of labels matches the number of expressions.
	if len(expr.Labels) > 0 && len(expr.Labels) != len(expr.Exprs) {
		return nil, pgerror.Newf(pgcode.Syntax,
			"mismatch in tuple definition: %d expressions, %d labels",
			len(expr.Exprs), len(expr.Labels),
		)
	}

	var labels []string
	contents := make([]*types.T, len(expr.Exprs))
	for i, subExpr := range expr.Exprs {
		desiredElem := types.AnyElement
		if desired.Family() == types.TupleFamily && len(desired.TupleContents()) > i {
			desiredElem = desired.TupleContents()[i]
		}
		typedExpr, err := subExpr.TypeCheck(ctx, semaCtx, desiredElem)
		if err != nil {
			return nil, err
		}
		expr.Exprs[i] = typedExpr
		contents[i] = typedExpr.ResolvedType()
	}
	// Copy the labels if there are any.
	if len(expr.Labels) > 0 {
		labels = make([]string, len(expr.Labels))
		copy(labels, expr.Labels)
	}
	expr.typ = types.MakeLabeledTuple(contents, labels)
	return expr, nil
}

var errAmbiguousArrayType = pgerror.Newf(pgcode.IndeterminateDatatype, "cannot determine type of empty array. "+
	"Consider casting to the desired type, for example ARRAY[]::int[]")

// TypeCheck implements the Expr interface.
func (expr *Array) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	desiredParam := types.AnyElement
	if desired.Family() == types.ArrayFamily {
		desiredParam = desired.ArrayContents()
	}

	if len(expr.Exprs) == 0 {
		if expr.typ == nil || expr.typ.Family() != types.ArrayFamily {
			if desiredParam.Family() == types.AnyFamily {
				return nil, errAmbiguousArrayType
			}
			expr.typ = types.MakeArray(desiredParam)
		}
		return expr, nil
	}

	typedSubExprs, typ, err := typeCheckSameTypedExprs(ctx, semaCtx, desiredParam, expr.Exprs...)
	if err != nil {
		return nil, err
	}

	expr.typ = types.MakeArray(typ)
	for i := range typedSubExprs {
		expr.Exprs[i] = typedSubExprs[i]
	}

	if OnTypeCheckArrayConstructor != nil {
		OnTypeCheckArrayConstructor()
	}
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *ArrayFlatten) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	desiredParam := types.AnyElement
	if desired.Family() == types.ArrayFamily {
		desiredParam = desired.ArrayContents()
	}

	subqueryTyped, err := expr.Subquery.TypeCheck(ctx, semaCtx, desiredParam)
	if err != nil {
		return nil, err
	}
	expr.Subquery = subqueryTyped
	expr.typ = types.MakeArray(subqueryTyped.ResolvedType())

	if OnTypeCheckArrayFlatten != nil {
		OnTypeCheckArrayFlatten()
	}
	return expr, nil
}

// TypeCheck implements the Expr interface.
func (expr *Placeholder) TypeCheck(
	ctx context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	// Perform placeholder typing. This function is only called during Prepare,
	// when there are no available values for the placeholders yet, because
	// during Execute all placeholders are replaced from the AST before type
	// checking.
	//
	// The width of a placeholder value is not known during Prepare, so we
	// remove type modifiers from the desired type so that a value of any width
	// will fit within the placeholder type.
	desired = desired.WithoutTypeModifiers()
	if typ, ok, err := semaCtx.Placeholders.Type(expr.Idx); err != nil {
		return expr, err
	} else if ok {
		typ = typ.WithoutTypeModifiers()
		if !desired.Equivalent(typ) || (typ.IsAmbiguous() && !desired.IsAmbiguous()) {
			// This indicates either:
			// - There's a conflict between what the type system thinks
			//   the type for this position should be, and the actual type of the
			//   placeholder.
			// - A type was already set for the placeholder, but it was ambiguous. If
			//   the desired type is not ambiguous then it can be used as the
			//   placeholder type. This can happen during overload type checking: an
			//   overload that operates on collated strings might cause the type
			//   checker to assign AnyCollatedString to a placeholder, but a later
			//   stage of type checking can further refine the desired type.
			//
			// This actual placeholder type could be either a type hint
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
		if err := semaCtx.Placeholders.SetType(expr.Idx, typ); err != nil {
			return nil, err
		}
		expr.typ = typ
		return expr, nil
	}
	if desired.IsAmbiguous() {
		return nil, placeholderTypeAmbiguityError(expr.Idx)
	}
	if err := semaCtx.Placeholders.SetType(expr.Idx, desired); err != nil {
		return nil, err
	}
	expr.typ = desired
	return expr, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DBitArray) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DBool) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DInt) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DFloat) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DEnum) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DDecimal) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DString) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DCollatedString) TypeCheck(
	_ context.Context, _ *SemaContext, _ *types.T,
) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DBytes) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DEncodedKey) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DUuid) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DIPAddr) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DDate) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTime) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTimeTZ) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTimestamp) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTimestampTZ) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DInterval) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DBox2D) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DPGLSN) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DPGVector) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DGeography) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DGeometry) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DJSON) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DJsonpath) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTSQuery) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTSVector) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DTuple) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DVoid) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DArray) TypeCheck(_ context.Context, _ *SemaContext, desired *types.T) (TypedExpr, error) {
	// Type-checking arrays is different from normal datums, since there are
	// situations in which an array's type is ambiguous without a desired type.
	// Consider the following examples. They're typed as `unknown[]` until
	// something asserts otherwise. In these circumstances, we're allowed to just
	// mark the array's type as the desired one.
	// ARRAY[]
	// ARRAY[NULL, NULL]
	if (d.ParamTyp.Family() == types.UnknownFamily || d.ParamTyp.Family() == types.AnyFamily) &&
		(!d.HasNonNulls) {
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
func (d *DOid) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d *DOidWrapper) TypeCheck(_ context.Context, _ *SemaContext, _ *types.T) (TypedExpr, error) {
	return d, nil
}

// TypeCheck implements the Expr interface. It is implemented as an idempotent
// identity function for Datum.
func (d dNull) TypeCheck(_ context.Context, _ *SemaContext, desired *types.T) (TypedExpr, error) {
	return d, nil
}

// typeCheckAndRequireTupleElems asserts that all elements in the Tuple are
// comparable to the input Expr given the input comparison operator.
func typeCheckAndRequireTupleElems(
	ctx context.Context,
	semaCtx *SemaContext,
	expr TypedExpr,
	tuple *Tuple,
	op treecmp.ComparisonOperator,
) (TypedExpr, error) {
	tuple.typ = types.MakeTuple(make([]*types.T, len(tuple.Exprs)))
	for i, subExpr := range tuple.Exprs {
		// Require that the sub expression is comparable to the required type.
		_, rightTyped, _, _, err := typeCheckComparisonOp(ctx, semaCtx, op, expr, subExpr, true /* disallowSwitch */)
		if err != nil {
			return nil, err
		}
		tuple.Exprs[i] = rightTyped
		tuple.typ.TupleContents()[i] = rightTyped.ResolvedType()
	}
	if len(tuple.typ.TupleContents()) > 0 && tuple.typ.TupleContents()[0].Family() == types.CollatedStringFamily {
		// Make sure that all elements of the tuple have the correct locale set
		// for collated strings. Note that if the locales were different, an
		// error would have been already emitted, so here we are only upgrading
		// an empty locale (which might be set for NULLs) to a non-empty (if
		// such is found).
		var typWithLocale *types.T
		for _, typ := range tuple.typ.TupleContents() {
			if typ.Locale() != "" {
				typWithLocale = typ
				break
			}
		}
		if typWithLocale != nil {
			for i := range tuple.typ.TupleContents() {
				tuple.typ.TupleContents()[i] = typWithLocale
			}
		}
	}
	return tuple, nil
}

func typeCheckAndRequireBoolean(
	ctx context.Context, semaCtx *SemaContext, expr Expr, op string,
) (TypedExpr, error) {
	return typeCheckAndRequire(ctx, semaCtx, expr, types.Bool, op)
}

func typeCheckAndRequire(
	ctx context.Context, semaCtx *SemaContext, expr Expr, required *types.T, op string,
) (TypedExpr, error) {
	typedExpr, err := expr.TypeCheck(ctx, semaCtx, required)
	if err != nil {
		return nil, err
	}
	if typ := typedExpr.ResolvedType(); !(typ.Family() == types.UnknownFamily || typ.Equivalent(required)) {
		// A void literal is output as an empty string (see DVoid.Format), so type
		// annotation of an empty string as VOID ('':::VOID) should succeed.
		if required.Family() == types.VoidFamily && typ.Family() == types.StringFamily {
			if str, ok := expr.(*StrVal); ok {
				if str.s == "" {
					return DVoidDatum, nil
				}
			}
		}
		return nil, pgerror.Newf(pgcode.DatatypeMismatch, "incompatible %s type: %s", op, typ)
	}
	return typedExpr, nil
}

const (
	compSignatureFmt          = "<%s> %s <%s>"
	compSignatureWithSubOpFmt = "<%s> %s %s <%s>"
	compExprsFmt              = "%s %s %s: %v"
	compExprsWithSubOpFmt     = "%s %s %s %s: %v"
	invalidCompErrFmt         = "invalid comparison between different %s types: %s"
	unsupportedCompErrFmt     = "unsupported comparison operator: %s"
	unsupportedUnaryOpErrFmt  = "unsupported unary operator: %s"
	unsupportedBinaryOpErrFmt = "unsupported binary operator: %s"
	ambiguousCompErrFmt       = "ambiguous comparison operator: %s"
	ambiguousUnaryOpErrFmt    = "ambiguous unary operator: %s"
	ambiguousBinaryOpErrFmt   = "ambiguous binary operator: %s"
	candidatesHintFmt         = "candidates are:\n%s"
)

func typeCheckComparisonOpWithSubOperator(
	ctx context.Context, semaCtx *SemaContext, op, subOp treecmp.ComparisonOperator, left, right Expr,
) (_ TypedExpr, _ TypedExpr, _ *CmpOp, alwaysNull bool, _ error) {
	// Parentheses are semantically unimportant and can be removed/replaced
	// with its nested expression in our plan. This makes type checking cleaner.
	left = StripParens(left)
	right = StripParens(right)

	// Determine the set of comparisons are possible for the sub-operation,
	// which will be memoized.
	foldedOp, _, _, _, _ := FoldComparisonExpr(subOp, nil, nil)
	ops := CmpOps[foldedOp.Symbol]

	var cmpTypeLeft, cmpTypeRight *types.T
	var leftTyped, rightTyped TypedExpr
	if array, isConstructor := right.(*Array); isConstructor {
		// If the right expression is an (optionally nested) array constructor, we
		// perform type inference on the array elements and the left expression.
		sameTypeExprs := make([]Expr, len(array.Exprs)+1)
		sameTypeExprs[0] = left
		copy(sameTypeExprs[1:], array.Exprs)

		typedSubExprs, retType, err := typeCheckSameTypedExprs(ctx, semaCtx, types.AnyElement, sameTypeExprs...)
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
		leftTyped, err = left.TypeCheck(ctx, semaCtx, types.AnyElement)
		if err != nil {
			return nil, nil, nil, false, err
		}
		cmpTypeLeft = leftTyped.ResolvedType()

		if tuple, ok := right.(*Tuple); ok {
			// If right expression is a tuple, we require that all elements' inferred
			// type is equivalent to the left's type.
			rightTyped, err = typeCheckAndRequireTupleElems(ctx, semaCtx, leftTyped, tuple, subOp)
			if err != nil {
				return nil, nil, nil, false, err
			}
		} else {
			// Try to type the right expression as an array of the left's type.
			// If right is an sql.subquery Expr, it should already be typed.
			// TODO(richardwu): If right is a subquery, we should really
			// propagate the left type as a desired type for the result column.
			rightTyped, err = right.TypeCheck(ctx, semaCtx, types.MakeArray(cmpTypeLeft))
			if err != nil {
				return nil, nil, nil, false, err
			}
		}

		rightReturn := rightTyped.ResolvedType()
		if rightReturn.Family() == types.UnknownFamily {
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
				cmpTypeRight = rightReturn.TupleContents()[0]
			}
		default:
			sigWithErr := fmt.Sprintf(compExprsWithSubOpFmt, left, subOp, op, right,
				fmt.Sprintf("op %s <right> requires array, tuple or subquery on right side", op))
			return nil, nil, nil, false, pgerror.Newf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sigWithErr)
		}
	}
	fn, ok := ops.LookupImpl(cmpTypeLeft, cmpTypeRight)
	if !ok || !deepCheckValidCmpOp(ops, cmpTypeLeft, cmpTypeRight) {
		return nil, nil, nil, false, subOpCompError(cmpTypeLeft, rightTyped.ResolvedType(), subOp, op)
	}
	return leftTyped, rightTyped, fn, false, nil
}

// deepCheckValidCmpOp performs extra checks that a given operation is valid
// when the types are tuples.
func deepCheckValidCmpOp(ops *CmpOpOverloads, leftType, rightType *types.T) bool {
	if leftType.Family() == types.TupleFamily && rightType.Family() == types.TupleFamily {
		l := leftType.TupleContents()
		r := rightType.TupleContents()
		if len(l) != len(r) {
			return false
		}
		for i := range l {
			if _, ok := ops.LookupImpl(l[i], r[i]); !ok {
				return false
			}
			if !deepCheckValidCmpOp(ops, l[i], r[i]) {
				return false
			}
		}
	}
	return true
}

func subOpCompError(leftType, rightType *types.T, subOp, op treecmp.ComparisonOperator) error {
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
				unsupportedCompErrFmt, fmt.Sprintf(compSignatureFmt, left, treecmp.In, right))
		}
		if !left.EquivalentOrNull(right.TupleContents()[0], false /* allowNullTupleEquivalence */) {
			return pgerror.Newf(pgcode.InvalidParameterValue,
				unsupportedCompErrFmt, fmt.Sprintf(compSignatureFmt, left, treecmp.In, right))
		}
	}
	return nil
}

// The first element of `params` is disallowSwitch, which when true means the
// first parameter to the overload type checker must be the original left
// expression.
func typeCheckComparisonOp(
	ctx context.Context,
	semaCtx *SemaContext,
	op treecmp.ComparisonOperator,
	left, right Expr,
	params ...bool,
) (_ TypedExpr, _ TypedExpr, _ *CmpOp, alwaysNull bool, _ error) {
	disallowSwitch := false
	if len(params) > 0 {
		disallowSwitch = true
	}
	// Parentheses are semantically unimportant and can be removed/replaced
	// with its nested expression in our plan. This makes type checking cleaner.
	left = StripParens(left)
	right = StripParens(right)

	foldedOp, foldedLeft, foldedRight, switched, _ := FoldComparisonExpr(op, left, right)
	ops := CmpOps[foldedOp.Symbol]

	_, leftIsTuple := foldedLeft.(*Tuple)
	_, rightIsTuple := foldedRight.(*Tuple)
	_, rightIsSubquery := foldedRight.(SubqueryExpr)
	var tsMatchesWithText bool
	var typedLeft, typedRight TypedExpr
	var leftFamily, rightFamily types.Family
	var err error

	// Do an initial check for TEXT @@ XXX special cases which might need to
	// inject a to_tsvector or plainto_tsquery function call.
	if op.Symbol == treecmp.TSMatches {
		if switched {
			// The order of operators matters as to which function call to apply.
			foldedLeft, foldedRight = foldedRight, foldedLeft
			switched = false
		}
		disallowSwitch = true
		typedLeft, err = foldedLeft.TypeCheck(ctx, semaCtx, types.AnyElement)
		if err != nil {
			sigWithErr := fmt.Sprintf(compExprsFmt, left, op, right, err)
			return nil, nil, nil, false,
				pgerror.Newf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sigWithErr)
		}
		typedRight, err = foldedRight.TypeCheck(ctx, semaCtx, types.AnyElement)
		if err != nil {
			sigWithErr := fmt.Sprintf(compExprsFmt, left, op, right, err)
			return nil, nil, nil, false,
				pgerror.Newf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sigWithErr)
		}
		leftFamily = typedLeft.ResolvedType().Family()
		rightFamily = typedRight.ResolvedType().Family()
		if leftFamily == types.StringFamily || rightFamily == types.StringFamily {
			tsMatchesWithText = true
		}
	}

	handleTupleTypeMismatch := false
	switch {
	case foldedOp.Symbol == treecmp.In && rightIsTuple:
		rightTuple := foldedRight.(*Tuple)
		sameTypeExprs := make([]Expr, len(rightTuple.Exprs)+1)
		sameTypeExprs[0] = foldedLeft
		copy(sameTypeExprs[1:], rightTuple.Exprs)

		typedSubExprs, retType, err := typeCheckSameTypedExprs(ctx, semaCtx, types.AnyElement, sameTypeExprs...)
		if err != nil {
			sigWithErr := fmt.Sprintf(compExprsFmt, left, op, right, err)
			return nil, nil, nil, false,
				pgerror.Newf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sigWithErr)
		}

		fn, ok := ops.LookupImpl(retType, types.AnyTuple)
		if !ok {
			sig := fmt.Sprintf(compSignatureFmt, retType, op, types.AnyTuple)
			return nil, nil, nil, false,
				pgerror.Newf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sig)
		}

		typedLeft = typedSubExprs[0]
		typedSubExprs = typedSubExprs[1:]

		rightTuple.typ = types.MakeTuple(make([]*types.T, len(typedSubExprs)))
		for i, typedExpr := range typedSubExprs {
			rightTuple.Exprs[i] = typedExpr
			rightTuple.typ.TupleContents()[i] = retType
		}
		if switched {
			return rightTuple, typedLeft, fn, false, nil
		}
		return typedLeft, rightTuple, fn, false, nil

	case foldedOp.Symbol == treecmp.In && rightIsSubquery:
		typedLeft, err = foldedLeft.TypeCheck(ctx, semaCtx, types.AnyElement)
		if err != nil {
			sigWithErr := fmt.Sprintf(compExprsFmt, left, op, right, err)
			return nil, nil, nil, false,
				pgerror.Newf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sigWithErr)
		}

		typ := typedLeft.ResolvedType()
		fn, ok := ops.LookupImpl(typ, types.AnyTuple)
		if !ok {
			sig := fmt.Sprintf(compSignatureFmt, typ, op, types.AnyTuple)
			return nil, nil, nil, false,
				pgerror.Newf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sig)
		}

		desired := typ
		if desired.Family() != types.TupleFamily {
			desired = types.MakeTuple([]*types.T{typ})
		}

		typedRight, err = foldedRight.TypeCheck(ctx, semaCtx, desired)
		if err != nil {
			sigWithErr := fmt.Sprintf(compExprsFmt, left, op, right, err)
			return nil, nil, nil, false,
				pgerror.Newf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sigWithErr)
		}

		if err = typeCheckSubqueryWithIn(
			typedLeft.ResolvedType(), typedRight.ResolvedType(),
		); err != nil {
			return nil, nil, nil, false, err
		}
		return typedLeft, typedRight, fn, false, nil

	case leftIsTuple && rightIsTuple:
		fn, ok := ops.LookupImpl(types.AnyTuple, types.AnyTuple)
		if !ok {
			sig := fmt.Sprintf(compSignatureFmt, types.AnyTuple, op, types.AnyTuple)
			return nil, nil, nil, false,
				pgerror.Newf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sig)
		}
		// Using non-folded left and right to avoid having to swap later.
		typedLeft, typedRight, err = typeCheckTupleComparison(ctx, semaCtx, op, left.(*Tuple), right.(*Tuple))
		if err != nil {
			return nil, nil, nil, false, err
		}
		return typedLeft, typedRight, fn, false, nil

	case leftIsTuple || rightIsTuple:
		var errLeft, errRight error
		// Tuple must compare with a tuple type, as handled above.
		typedLeft, errLeft = foldedLeft.TypeCheck(ctx, semaCtx, types.AnyElement)
		typedRight, errRight = foldedRight.TypeCheck(ctx, semaCtx, types.AnyElement)
		if errLeft == nil && errRight == nil &&
			((typedLeft.ResolvedType().Family() == types.TupleFamily &&
				typedRight.ResolvedType().Family() != types.TupleFamily) ||
				(typedRight.ResolvedType().Family() == types.TupleFamily &&
					typedLeft.ResolvedType().Family() != types.TupleFamily)) {
			handleTupleTypeMismatch = true
		}
	case tsMatchesWithText:
		// Apply rules from:
		// https://www.postgresql.org/docs/current/textsearch-intro.html#TEXTSEARCH-MATCHING
		// Perform the following type conversions:
		//            initial             |              result
		// -------------------------------------------------------------------------
		// a::TEXT @@ b::TEXT             |  to_tsvector(a) @@ plainto_tsquery(b)
		// a::TEXT @@ b::TSQUERY          |  to_tsvector(a) @@ b
		// a::TSQUERY @@ b::TEXT          |  a @@ to_tsvector(b)
		if leftFamily == types.StringFamily {
			if rightFamily == types.StringFamily || rightFamily == types.TSQueryFamily {
				leftExprs := make(Exprs, 1)
				leftExprs[0] = typedLeft
				foldedLeft = &FuncExpr{Func: WrapFunction("to_tsvector"), Exprs: leftExprs, AggType: GeneralAgg}
			}
		}

		funcName := "plainto_tsquery"
		if rightFamily == types.StringFamily {
			if leftFamily == types.StringFamily || leftFamily == types.TSQueryFamily {
				if leftFamily == types.TSQueryFamily {
					funcName = "to_tsvector"
				}
				rightExprs := make(Exprs, 1)
				rightExprs[0] = typedRight
				foldedRight = &FuncExpr{Func: WrapFunction(funcName), Exprs: rightExprs, AggType: GeneralAgg}
			}
		}
	}

	// For comparisons, we do not stimulate the typing of untyped NULL with the
	// other side's type, because comparisons of NULL with anything else are
	// defined to return NULL anyways. Should the SQL dialect ever be extended with
	// comparisons that can return non-NULL on NULL input, the `inBinOp` parameter
	// may need altering.
	var s *overloadTypeChecker
	// The overload type checker does not have symmetric behavior (the order of
	// the expression arguments matters). Find the order which does not error out
	// and initialize the type checker with that order. We purposely do not return
	// errors here because the overload type checker can coerce types in some
	// cases to avoid errors. Comparisons with placeholders aren't examined as
	// that may not take placeholder hints into account. Comparisons with columns
	// aren't examined as it's usually best to allow type coercion to the type of
	// the column, if possible.
	placeholderComparison := false
	columnComparison := false
	if _, ok := foldedLeft.(VariableExpr); ok {
		columnComparison = true
	}
	if _, ok := foldedRight.(VariableExpr); ok {
		columnComparison = true
	}
	if _, ok := foldedLeft.(*Placeholder); ok {
		placeholderComparison = true
	}
	if _, ok := foldedRight.(*Placeholder); ok {
		placeholderComparison = true
	}
	if !disallowSwitch && !placeholderComparison && !columnComparison {
		_, _, err := typeCheckSameTypedExprs(ctx, semaCtx, types.AnyElement, foldedLeft, foldedRight)
		if err != nil {
			_, _, err = typeCheckSameTypedExprs(ctx, semaCtx, types.AnyElement, foldedRight, foldedLeft)
			if err == nil {
				s = getOverloadTypeChecker(ops, foldedRight, foldedLeft)
				switched = !switched
			}
		}
	}
	if s == nil {
		if disallowSwitch && switched {
			s = getOverloadTypeChecker(ops, foldedRight, foldedLeft)
			switched = false
		} else {
			s = getOverloadTypeChecker(ops, foldedLeft, foldedRight)
		}
	}
	defer s.release()
	if err := s.typeCheckOverloadedExprs(ctx, semaCtx, types.AnyElement, true); err != nil {
		return nil, nil, nil, false, err
	}
	typedSubExprs := s.typedExprs

	leftExpr, rightExpr := typedSubExprs[0], typedSubExprs[1]
	if switched {
		leftExpr, rightExpr = rightExpr, leftExpr
	}
	leftReturn := leftExpr.ResolvedType()
	rightReturn := rightExpr.ResolvedType()
	leftFamily = leftReturn.Family()
	rightFamily = rightReturn.Family()

	// Return early if at least one overload is possible, NULL is an argument,
	// and none of the overloads accept NULL.
	nullComparison := false
	if leftFamily == types.UnknownFamily || rightFamily == types.UnknownFamily {
		nullComparison = true
		if len(s.overloadIdxs) > 0 {
			noneAcceptNull := true
			for _, idx := range s.overloadIdxs {
				e := ops.overloads[idx]
				if e.CalledOnNullInput {
					noneAcceptNull = false
					break
				}
			}
			if noneAcceptNull {
				return leftExpr, rightExpr, nil, true /* alwaysNull */, nil
			}
		}
	}

	leftIsGeneric := leftFamily == types.CollatedStringFamily || leftFamily == types.ArrayFamily || leftFamily == types.EnumFamily
	rightIsGeneric := rightFamily == types.CollatedStringFamily || rightFamily == types.ArrayFamily || rightFamily == types.EnumFamily
	genericComparison := leftIsGeneric && rightIsGeneric

	typeMismatch := false
	if (genericComparison || handleTupleTypeMismatch) && !nullComparison {
		// A generic comparison (one between two generic types, like arrays) is not
		// well-typed if the two input types are not equivalent, unless one of the
		// sides is NULL.
		typeMismatch = !leftReturn.Equivalent(rightReturn)
	}

	// Throw a typing error if overload resolution found either no compatible candidates
	// or if it found an ambiguity.
	if len(s.overloadIdxs) != 1 || typeMismatch {
		sig := fmt.Sprintf(compSignatureFmt, leftReturn, op, rightReturn)
		if len(s.overloadIdxs) == 0 || typeMismatch {
			// For some typeMismatch errors, we want to emit a more specific error
			// message than "unknown comparison". In particular, comparison between
			// two different enum types is invalid, rather than just unsupported.
			if typeMismatch && leftFamily == types.EnumFamily && rightFamily == types.EnumFamily {
				return nil, nil, nil, false,
					pgerror.Newf(pgcode.InvalidParameterValue, invalidCompErrFmt, "enum", sig)
			}
			return nil, nil, nil, false,
				pgerror.Newf(pgcode.InvalidParameterValue, unsupportedCompErrFmt, sig)
		}
		fnsStr := formatCandidates(op.String(), s.overloads, s.overloadIdxs)
		err := pgerror.Newf(pgcode.AmbiguousFunction, ambiguousCompErrFmt, sig)
		err = errors.WithHintf(err, candidatesHintFmt, fnsStr)
		return nil, nil, nil, false, err
	}
	return leftExpr, rightExpr, ops.overloads[s.overloadIdxs[0]], false, nil
}

type typeCheckExprsState struct {
	ctx     context.Context
	semaCtx *SemaContext

	exprs           []Expr
	typedExprs      []TypedExpr
	constIdxs       intsets.Fast // index into exprs/typedExprs
	placeholderIdxs intsets.Fast // index into exprs/typedExprs
	resolvableIdxs  intsets.Fast // index into exprs/typedExprs
}

func findFirstTupleIndex(exprs ...Expr) (index int, ok bool) {
	for i, expr := range exprs {
		if _, ok := expr.(*Tuple); ok {
			return i, true
		}
	}
	return 0, false
}

// typeCheckSameTypedExprs type checks a list of expressions, asserting that all
// resolved TypeExprs have the same type. An optional desired type can be provided,
// which will hint that type which the expressions should resolve to, if possible.
func typeCheckSameTypedExprs(
	ctx context.Context, semaCtx *SemaContext, desired *types.T, exprs ...Expr,
) ([]TypedExpr, *types.T, error) {
	switch len(exprs) {
	case 0:
		return nil, nil, nil
	case 1:
		typedExpr, err := exprs[0].TypeCheck(ctx, semaCtx, desired)
		if err != nil {
			return nil, nil, err
		}
		typ := typedExpr.ResolvedType()
		if typ.Family() == types.UnknownFamily && !desired.IsWildcardType() {
			// The expression had a NULL type, so we can return the desired type as
			// the expression type.
			typ = desired
		}
		return []TypedExpr{typedExpr}, typ, nil
	}

	// Handle tuples, which will in turn call into this function recursively for
	// each element.
	// TODO(msirek): Rewrite typeCheckSameTypedTupleExprs to handle all types of
	// expressions which could resolve to a type family of `TupleFamily`, like a
	// VALUES clause. Logic in `typeCheckSameTypedTupleExprs` states that the call
	// to `TypeCheck` should be deferred until the common type is determined. So,
	// we would need a way to determine which expressions are in the tuple family
	// without inspecting the AST node and without calling `TypeCheck`. Does the
	// call to `TypeCheck` really need to be deferred?
	if idx, ok := findFirstTupleIndex(exprs...); ok {
		if _, ok := exprs[idx].(*Tuple); ok {
			// typeCheckSameTypedTupleExprs expects the first expression in the slice
			// to be a tuple.
			exprs[0], exprs[idx] = exprs[idx], exprs[0]
			typedExprs, commonType, err := typeCheckSameTypedTupleExprs(ctx, semaCtx, desired, exprs...)
			if err == nil {
				typedExprs[0], typedExprs[idx] = typedExprs[idx], typedExprs[0]
			}
			return typedExprs, commonType, err
		}
	}

	// Hold the resolved type expressions of the provided exprs, in order.
	// TODO(nvanbenschoten): Look into reducing allocations here.
	typedExprs := make([]TypedExpr, len(exprs))

	constIdxs, placeholderIdxs, resolvableIdxs := typeCheckSplitExprs(exprs)

	s := typeCheckExprsState{
		ctx:             ctx,
		semaCtx:         semaCtx,
		exprs:           exprs,
		typedExprs:      typedExprs,
		constIdxs:       constIdxs,
		placeholderIdxs: placeholderIdxs,
		resolvableIdxs:  resolvableIdxs,
	}

	switch {
	case resolvableIdxs.Empty() && constIdxs.Empty():
		typ, err := typeCheckSameTypedPlaceholders(s, desired)
		if err != nil {
			return nil, nil, err
		}
		return typedExprs, typ, nil
	case resolvableIdxs.Empty():
		return typeCheckConstsAndPlaceholdersWithDesired(s, desired)
	default:
		firstValidIdx := -1
		candidateType := types.Unknown
		for i, ok := s.resolvableIdxs.Next(0); ok; i, ok = s.resolvableIdxs.Next(i + 1) {
			typedExpr, err := exprs[i].TypeCheck(ctx, semaCtx, desired)
			if err != nil {
				return nil, nil, err
			}
			typedExprs[i] = typedExpr
			if returnType := typedExpr.ResolvedType(); returnType.Family() != types.UnknownFamily {
				candidateType = returnType
				firstValidIdx = i
				break
			}
		}

		if candidateType.Family() == types.UnknownFamily {
			// We got to the end without finding a non-null expression.
			switch {
			case !constIdxs.Empty():
				return typeCheckConstsAndPlaceholdersWithDesired(s, desired)
			case !placeholderIdxs.Empty():
				typ, err := typeCheckSameTypedPlaceholders(s, desired)
				if err != nil {
					return nil, nil, err
				}
				return typedExprs, typ, nil
			default:
				if !desired.IsWildcardType() {
					return typedExprs, desired, nil
				}
				return typedExprs, types.Unknown, nil
			}
		}

		for i, ok := s.resolvableIdxs.Next(firstValidIdx + 1); ok; i, ok = s.resolvableIdxs.Next(i + 1) {
			typedExpr, err := exprs[i].TypeCheck(ctx, semaCtx, candidateType)
			if err != nil {
				return nil, nil, err
			}
			// From the Postgres docs
			// https://www.postgresql.org/docs/15/typeconv-union-case.html:
			// If the candidate type can be implicitly converted to the other
			// type, but not vice-versa, select the other type as the new
			// candidate type.
			if typ := typedExpr.ResolvedType(); cast.ValidCast(candidateType, typ, cast.ContextImplicit) {
				if !cast.ValidCast(typ, candidateType, cast.ContextImplicit) {
					candidateType = typ
				}
			}
			// TODO(mgartner): Remove this check now that we check the types
			// below.
			if typ := typedExpr.ResolvedType(); !(typ.Equivalent(candidateType) || typ.Family() == types.UnknownFamily) {
				return nil, nil, unexpectedTypeError(exprs[i], candidateType, typ)
			}
			typedExprs[i] = typedExpr
		}
		if !constIdxs.Empty() {
			if _, err := typeCheckSameTypedConsts(s, candidateType, true); err != nil {
				return nil, nil, err
			}
		}
		if !placeholderIdxs.Empty() {
			if _, err := typeCheckSameTypedPlaceholders(s, candidateType); err != nil {
				return nil, nil, err
			}
		}
		// Now we check that each expression can be implicit cast to the
		// candidate type, and add the cast if necessary. If any expressions
		// cannot be cast, type-checking fails. This is described in Step 6 of
		// Postgres's "UNION, CASE, and Related Constructs" type conversion
		// documentation:
		// https://www.postgresql.org/docs/15/typeconv-union-case.html
		for i, e := range typedExprs {
			typ := e.ResolvedType()
			// TODO(mgartner): There should probably be a cast if the types are
			// not identical, not just if the types are not equivalent.
			if typ.Equivalent(candidateType) || typ.Family() == types.UnknownFamily {
				continue
			}
			if !cast.ValidCast(typ, candidateType, cast.ContextImplicit) {
				return nil, nil, unexpectedTypeError(exprs[i], candidateType, typ)
			}
			typedExprs[i] = NewTypedCastExpr(e, candidateType)
		}
		return typedExprs, candidateType, nil
	}
}

// Used to set placeholders to the desired typ.
func typeCheckSameTypedPlaceholders(s typeCheckExprsState, typ *types.T) (*types.T, error) {
	for i, ok := s.placeholderIdxs.Next(0); ok; i, ok = s.placeholderIdxs.Next(i + 1) {
		typedExpr, err := typeCheckAndRequire(s.ctx, s.semaCtx, s.exprs[i], typ, "placeholder")
		if err != nil {
			return nil, err
		}
		s.typedExprs[i] = typedExpr
		typ = typedExpr.ResolvedType()
	}
	return typ, nil
}

// Used to type check constants to the same type. An optional typ can be
// provided to signify the desired shared type, which can be set to the
// required shared type using the second parameter.
func typeCheckSameTypedConsts(
	s typeCheckExprsState, typ *types.T, required bool,
) (*types.T, error) {
	setTypeForConsts := func(typ *types.T) (*types.T, error) {
		for i, ok := s.constIdxs.Next(0); ok; i, ok = s.constIdxs.Next(i + 1) {
			typedExpr, err := typeCheckAndRequire(s.ctx, s.semaCtx, s.exprs[i], typ, "constant")
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
	if !typ.IsWildcardType() {
		all := true
		for i, ok := s.constIdxs.Next(0); ok; i, ok = s.constIdxs.Next(i + 1) {
			if !canConstantBecome(s.exprs[i].(Constant), typ) {
				if required {
					typedExpr, err := s.exprs[i].TypeCheck(s.ctx, s.semaCtx, types.AnyElement)
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
			// Constants do not have types with modifiers so clear the modifiers
			// of typ before setting it.
			return setTypeForConsts(typ.WithoutTypeModifiers())
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
	for i, ok := s.constIdxs.Next(0); ok; i, ok = s.constIdxs.Next(i + 1) {
		typedExpr, err := s.exprs[i].TypeCheck(s.ctx, s.semaCtx, reqTyp)
		if err != nil {
			return nil, err
		}
		if typ := typedExpr.ResolvedType(); !typ.Equivalent(reqTyp) {
			return nil, unexpectedTypeError(s.exprs[i], reqTyp, typ)
		}
		if reqTyp.IsWildcardType() {
			reqTyp = typedExpr.ResolvedType()
		}
	}
	return nil, errors.AssertionFailedf("should throw error above")
}

// Used to type check all constants with the optional desired type. First,
// placeholders with type hints are checked, then constants are checked to
// match the resulting type. The type that is chosen here will then be set
// to any unresolved placeholders.
func typeCheckConstsAndPlaceholdersWithDesired(
	s typeCheckExprsState, desired *types.T,
) ([]TypedExpr, *types.T, error) {
	if !s.placeholderIdxs.Empty() {
		for i, ok := s.placeholderIdxs.Next(0); ok; i, ok = s.placeholderIdxs.Next(i + 1) {
			if !s.semaCtx.isUnresolvedPlaceholder(s.exprs[i]) {
				typedExpr, err := typeCheckAndRequire(s.ctx, s.semaCtx, s.exprs[i], desired, "placeholder")
				if err != nil {
					return nil, nil, err
				}
				s.typedExprs[i] = typedExpr
				desired = typedExpr.ResolvedType()
			}
		}
	}
	typ, err := typeCheckSameTypedConsts(s, desired, false)
	if err != nil {
		return nil, nil, err
	}
	if !s.placeholderIdxs.Empty() {
		typ, err = typeCheckSameTypedPlaceholders(s, typ)
		if err != nil {
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
	exprs []Expr,
) (constIdxs intsets.Fast, placeholderIdxs intsets.Fast, resolvableIdxs intsets.Fast) {
	for i, expr := range exprs {
		switch {
		case isConstant(expr):
			constIdxs.Add(i)
		case isPlaceholder(expr):
			placeholderIdxs.Add(i)
		default:
			resolvableIdxs.Add(i)
		}
	}
	return constIdxs, placeholderIdxs, resolvableIdxs
}

// typeCheckTupleComparison type checks a comparison between two tuples,
// asserting that the elements of the two tuples are comparable at each index.
func typeCheckTupleComparison(
	ctx context.Context,
	semaCtx *SemaContext,
	op treecmp.ComparisonOperator,
	left *Tuple,
	right *Tuple,
) (TypedExpr, TypedExpr, error) {
	// All tuples must have the same length.
	tupLen := len(left.Exprs)
	if err := checkTupleHasLength(right, tupLen); err != nil {
		return nil, nil, err
	}
	left.typ = types.MakeTuple(make([]*types.T, tupLen))
	right.typ = types.MakeTuple(make([]*types.T, tupLen))
	for elemIdx := range left.Exprs {
		leftSubExpr := left.Exprs[elemIdx]
		rightSubExpr := right.Exprs[elemIdx]
		leftSubExprTyped, rightSubExprTyped, _, _, err := typeCheckComparisonOp(ctx, semaCtx, op, leftSubExpr, rightSubExpr)
		if err != nil {
			exps := Exprs([]Expr{left, right})
			return nil, nil, pgerror.Wrapf(err, pgcode.DatatypeMismatch, "tuples %s are not comparable at index %d",
				&exps, elemIdx+1)
		}
		left.Exprs[elemIdx] = leftSubExprTyped
		left.typ.TupleContents()[elemIdx] = leftSubExprTyped.ResolvedType()
		right.Exprs[elemIdx] = rightSubExprTyped
		right.typ.TupleContents()[elemIdx] = rightSubExprTyped.ResolvedType()
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
	ctx context.Context, semaCtx *SemaContext, desired *types.T, exprs ...Expr,
) ([]TypedExpr, *types.T, error) {
	// All other exprs must be tuples.
	first := exprs[0].(*Tuple)
	if err := checkAllExprsAreTuplesOrNulls(ctx, semaCtx, exprs[1:]); err != nil {
		return nil, nil, err
	}

	// All tuples must have the same length.
	firstLen := len(first.Exprs)
	if err := checkAllTuplesHaveLength(exprs[1:], firstLen); err != nil {
		return nil, nil, err
	}

	// All labeled tuples must have the same number of expressions as labels.
	for i := range exprs {
		if e, ok := exprs[i].(*Tuple); ok && len(e.Labels) > 0 && len(e.Labels) != len(e.Exprs) {
			return nil, nil, pgerror.Newf(pgcode.Syntax,
				"mismatch in tuple definition: %d expressions, %d labels",
				len(e.Exprs), len(e.Labels),
			)
		}
	}

	// All expressions within tuples at the same indexes must be the same type.
	resTypes := types.MakeLabeledTuple(make([]*types.T, firstLen), first.Labels)
	sameTypeExprs := make([]Expr, 0, len(exprs))
	// We will be skipping nulls, so we need to keep track at which indices in
	// exprs are the non-null tuples.
	sameTypeExprsIndices := make([]int, 0, len(exprs))
	for elemIdx := range first.Exprs {
		sameTypeExprs = sameTypeExprs[:0]
		sameTypeExprsIndices = sameTypeExprsIndices[:0]
		for exprIdx, expr := range exprs {
			// Skip expressions that are not Tuple expressions (e.g. NULLs or CastExpr).
			// They are checked at the end of this function.
			if _, isTuple := expr.(*Tuple); !isTuple {
				continue
			}
			sameTypeExprs = append(sameTypeExprs, expr.(*Tuple).Exprs[elemIdx])
			sameTypeExprsIndices = append(sameTypeExprsIndices, exprIdx)
		}
		desiredElem := types.AnyElement
		if len(desired.TupleContents()) > elemIdx {
			desiredElem = desired.TupleContents()[elemIdx]
		}
		typedSubExprs, resType, err := typeCheckSameTypedExprs(ctx, semaCtx, desiredElem, sameTypeExprs...)
		if err != nil {
			return nil, nil, pgerror.Wrapf(err, pgcode.DatatypeMismatch, "tuples %s are not the same type", Exprs(exprs))
		}
		for j, typedExpr := range typedSubExprs {
			tupleIdx := sameTypeExprsIndices[j]
			exprs[tupleIdx].(*Tuple).Exprs[elemIdx] = typedExpr
		}
		resTypes.TupleContents()[elemIdx] = resType
	}
	// Hold the resolved type expressions of the provided exprs, in order.
	// TODO(nvanbenschoten): Look into reducing allocations here.
	typedExprs := make([]TypedExpr, len(exprs))
	for tupleIdx, expr := range exprs {
		if t, isTuple := expr.(*Tuple); isTuple {
			// For Tuple exprs we can update the type with what we've inferred.
			t.typ = resTypes
			typedExprs[tupleIdx] = t
		} else {
			typedExpr, err := expr.TypeCheck(ctx, semaCtx, resTypes)
			if err != nil {
				return nil, nil, err
			}
			if !typedExpr.ResolvedType().EquivalentOrNull(resTypes, true /* allowNullTupleEquivalence */) {
				return nil, nil, unexpectedTypeError(expr, resTypes, typedExpr.ResolvedType())
			}
			typedExprs[tupleIdx] = typedExpr
		}
	}
	return typedExprs, resTypes, nil
}

// checkAllExprsAreTuplesOrNulls checks that all expressions in exprs are
// either tuples or nulls.
func checkAllExprsAreTuplesOrNulls(ctx context.Context, semaCtx *SemaContext, exprs []Expr) error {
	for _, expr := range exprs {
		_, isTuple := expr.(*Tuple)
		isNull, err := isNullOrAnnotatedNullTuple(ctx, semaCtx, expr)
		if err != nil {
			return err
		}
		if !(isTuple || isNull) {
			// We avoid calling TypeCheck on Tuple exprs since that causes the
			// types to be resolved, which we only want to do later in type-checking.
			typedExpr, err := expr.TypeCheck(ctx, semaCtx, types.AnyElement)
			if err != nil {
				return err
			}
			if typedExpr.ResolvedType().Family() != types.TupleFamily {
				return unexpectedTypeError(expr, types.AnyTuple, typedExpr.ResolvedType())
			}
		}
	}
	return nil
}

// checkAllTuplesHaveLength checks that all tuples in exprs have the expected
// length. We only need to check Tuple exprs, since other expressions like
// CastExpr are handled later in type-checking
func checkAllTuplesHaveLength(exprs []Expr, expectedLen int) error {
	for _, expr := range exprs {
		if t, isTuple := expr.(*Tuple); isTuple {
			if err := checkTupleHasLength(t, expectedLen); err != nil {
				return err
			}
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

// isNullOrAnnotatedNullTuple returns true if the given expression is a DNull or
// a DNull that is wrapped by an AnnotateTypeExpr with a type identical to
// AnyTuple (e.g., NULL:::RECORD). An AnnotateTypeExpr should never have a tuple
// type not identical to AnyTuple, because other tuple types could only be
// expressed as composite types, and these are not yet supported. If the
// AnnotateTypeExpr has a non-tuple type, the function returns false.
func isNullOrAnnotatedNullTuple(
	ctx context.Context, semaCtx *SemaContext, expr Expr,
) (bool, error) {
	if expr == DNull {
		return true, nil
	}
	if annotate, ok := expr.(*AnnotateTypeExpr); ok && annotate.Expr == DNull {
		annotateType, err := ResolveType(ctx, annotate.Type, semaCtx.GetTypeResolver())
		if err != nil {
			return false, err
		}
		return annotateType.Identical(types.AnyTuple), nil
	}
	return false, nil
}

type placeholderAnnotationVisitor struct {
	types PlaceholderTypes
	state []annotationState
	err   error
	ctx   *SemaContext
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
			tType, err := ResolveType(context.TODO(), t.Type, v.ctx.GetTypeResolver())
			if err != nil {
				v.setErr(arg.Idx, err)
				return false, expr
			}
			switch v.state[arg.Idx] {
			case noType, typeFromCast, conflictingCasts:
				// An annotation overrides casts.
				v.types[arg.Idx] = tType
				v.state[arg.Idx] = typeFromAnnotation

			case typeFromAnnotation:
				// Verify that the annotations are consistent.
				if !tType.Equivalent(v.types[arg.Idx]) {
					v.setErr(arg.Idx, pgerror.Newf(
						pgcode.DatatypeMismatch,
						"multiple conflicting type annotations around %s",
						arg.Idx,
					))
				}

			case typeFromHint:
				// Verify that the annotation is consistent with the type hint.
				if prevType := v.types[arg.Idx]; !tType.Equivalent(prevType) {
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
			tType, err := ResolveType(context.TODO(), t.Type, v.ctx.GetTypeResolver())
			if err != nil {
				v.setErr(arg.Idx, err)
				return false, expr
			}
			switch v.state[arg.Idx] {
			case noType:
				v.types[arg.Idx] = tType
				v.state[arg.Idx] = typeFromCast

			case typeFromCast:
				// Verify that the casts are consistent.
				if !tType.Equivalent(v.types[arg.Idx]) {
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
//   - the placeholder is the subject of an explicit type annotation in at least one
//     of its occurrences. If it is subject to multiple explicit type annotations
//     where the types are not all in agreement, or if the placeholder already has
//     a type hint in the placeholder map which conflicts with the explicit type
//     annotation type, an error will be thrown.
//
//   - the placeholder is the subject to a cast of the same type in all
//     occurrences of the placeholder. If the placeholder is subject to casts of
//     multiple types, or if it has occurrences without a cast, no error will be
//     thrown but the type will not be inferred. If the placeholder already has a
//     type hint, that type will be kept regardless of any casts.
//
// See docs/RFCS/20160203_typing.md for more details on placeholder typing (in
// particular section "First pass: placeholder annotations").
//
// The typeHints slice contains the client-provided hints and is populated with
// any newly assigned types. It is assumed to be pre-sized to the number of
// placeholders in the statement and is populated accordingly.
//
// TODO(nvanbenschoten): Can this visitor and map be preallocated (like normalizeVisitor)?
func ProcessPlaceholderAnnotations(
	semaCtx *SemaContext, stmt Statement, typeHints PlaceholderTypes,
) error {
	v := placeholderAnnotationVisitor{
		types: typeHints,
		state: make([]annotationState, len(typeHints)),
		ctx:   semaCtx,
	}

	for placeholder := range typeHints {
		if typeHints[placeholder] != nil {
			v.state[placeholder] = typeFromHint
		}
	}

	WalkStmt(&v, stmt)
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
		t.op = nil
	case *BinaryExpr:
		t.Op = nil
	case *ComparisonExpr:
		t.Op = nil
	case *FuncExpr:
		t.fn = nil
		t.fnProps = nil
	}
	return true, expr
}

func (stripFuncsVisitor) VisitPost(expr Expr) Expr { return expr }

// getMostSignificantOverload returns the overload from the most significant
// schema. If there are more than one overload available from the most
// significant schema, ambiguity error will be thrown. If search path is not
// given or no UDF found, there should be only one candidate overload and be
// returned. Otherwise, ambiguity error is also thrown.
//
// Note: even the input is a slice of overloadImpl, they're essentially a slice
// of QualifiedOverload. Also, the input should not be empty.
func getMostSignificantOverload(
	qualifiedOverloads []QualifiedOverload,
	overloads []overloadImpl,
	filter []uint8,
	searchPath SearchPath,
	expr *FuncExpr,
	typedInputExprs []TypedExpr,
	getFuncSig func() string,
) (QualifiedOverload, error) {
	ambiguousError := func() error {
		if expr.InCall {
			return pgerror.Newf(pgcode.AmbiguousFunction, "procedure %s is not unique", getFuncSig())
		}
		return pgerror.Newf(
			pgcode.AmbiguousFunction,
			"ambiguous call: %s, candidates are:\n%s",
			getFuncSig(),
			// Use "overloads" for the errors string since we want to print out
			// candidates from all schemas.
			formatCandidates(expr.Func.String(), overloads, filter),
		)
	}
	checkAmbiguity := func(oImpls []uint8) (QualifiedOverload, error) {
		if len(oImpls) == 1 {
			return qualifiedOverloads[oImpls[0]], nil
		}

		// Since there are ambiguity errors when udf with the same family
		// parameter types, function signature has to be sorted out to directly
		// match a specific UDF overload for execution.
		var allArgTypes []*types.T
		for _, exp := range typedInputExprs {
			allArgTypes = append(allArgTypes, exp.ResolvedType())
		}
		matchIdx := -1
		foundMatch := false
		for k, idx := range oImpls {
			candidate := overloads[idx]
			srcParams := candidate.params()
			matches := srcParams.MatchIdentical(allArgTypes)
			if !matches {
				routineType, outParamOrdinals, _ := candidate.outParamInfo()
				defaultExprs := candidate.defaultExprs()
				if len(outParamOrdinals) == 0 && len(defaultExprs) == 0 {
					continue
				}
				// When we have some OUT parameters and / or DEFAULT expressions
				// for input parameters, simply matching against all argument
				// types might be insufficient.
				var inputTypes []*types.T
				if expr.InCall && routineType == ProcedureRoutine && len(outParamOrdinals) > 0 {
					// OUT parameters for procedures must be specified in the
					// CALL invocation, but they should be ignored when
					// comparing against input parameters. (Note that type
					// coercibility for OUT parameters has already been
					// performed.)
					inputTypes = make([]*types.T, 0, len(typedInputExprs)-len(outParamOrdinals))
					var numOutParamsSeen int
					for i, t := range allArgTypes {
						if numOutParamsSeen < len(outParamOrdinals) && i == int(outParamOrdinals[numOutParamsSeen]) {
							numOutParamsSeen++
							continue
						}
						inputTypes = append(inputTypes, t)
					}
				} else {
					inputTypes = allArgTypes
				}
				// TODO(88947): this logic might need to change to support
				// VARIADIC.
				ovInputTypes, ok := srcParams.(ParamTypes)
				if !ok {
					return QualifiedOverload{}, errors.AssertionFailedf("overload params is %T and not ParamTypes", srcParams)
				}
				if len(defaultExprs) > 0 {
					// Some parameters have DEFAULT expressions, so it might be
					// the case that some input arguments were omitted.
					numOmittedExprs := len(ovInputTypes) - len(inputTypes)
					if numOmittedExprs > 0 && numOmittedExprs <= len(defaultExprs) {
						ovInputTypes = ovInputTypes[:len(ovInputTypes)-numOmittedExprs]
					}
				}
				matches = ovInputTypes.MatchIdentical(inputTypes)
			}
			if matches {
				if foundMatch {
					// Throw ambiguity error if there are more than one
					// candidate overloads from same schema.
					return QualifiedOverload{}, ambiguousError()
				}
				foundMatch = true
				matchIdx = k
			}
		}
		if !foundMatch {
			return QualifiedOverload{}, ambiguousError()
		}
		return qualifiedOverloads[oImpls[matchIdx]], nil
	}

	if searchPath == nil || searchPath == EmptySearchPath {
		return checkAmbiguity(filter)
	}

	udfFound := false
	uniqueSchema := true
	seenSchema := ""
	for _, idx := range filter {
		o := qualifiedOverloads[idx]
		if o.Type == UDFRoutine {
			// This check is only concerned with user-defined functions, not
			// with builtin functions defined with a SQL string body. For this
			// reason we check o.Type instead of o.HasSQLBody().
			udfFound = true
		}
		if seenSchema != "" && o.Schema != seenSchema {
			uniqueSchema = false
		}
		seenSchema = o.Schema
	}

	if !udfFound || uniqueSchema {
		// If there is no UDF, there is no need to go through the search path. This
		// is based on the fact that pg_catalog is either implicitly or explicitly
		// in the search path. And there are quite a few hacks which make builtin
		// function overrides with name qualified with a different schema from the
		// original one which is not in search path.
		//
		// If all overloads are from the same schema, there is no need to go through
		// the search path as well. This is because we only resolve functions from
		// explicit schema or schemas on the search path. So if overloads are from
		// the same schema, overloads are either from an explicit schema or from a
		// schema on search path.
		return checkAmbiguity(filter)
	}

	found := false
	var ret QualifiedOverload
	for i, n := 0, searchPath.NumElements(); i < n; i++ {
		schema := searchPath.GetSchema(i)
		for _, idx := range filter {
			if r := qualifiedOverloads[idx]; r.Schema == schema {
				// Only throw "ambiguous function" error for user-defined functions.
				if found && r.Type == UDFRoutine {
					return QualifiedOverload{}, ambiguousError()
				}
				found = true
				ret = r
			}
		}
		if found {
			break
		}
	}
	if !found {
		// This should never happen. Otherwise, it means we get function from a
		// schema no on the given search path or we try to resolve a function on an
		// explicit schema, but get some function from other schemas are fetched.
		return QualifiedOverload{}, pgerror.Newf(pgcode.UndefinedFunction, "unknown signature: %s", getFuncSig())
	}
	return ret, nil
}

// ResolvePolymorphicArgTypes iterates through the list of routine parameters
// and supplied arguments (including default expressions), and attempts to
// determine the concrete types for any polymorphic-typed parameters. It returns
// true if the supplied argument types are valid, as well as the determined
// element type (nil if there were no polymorphic parameters).
//
// CRDB currently supports two polymorphic types:
// * ANYELEMENT allows any argument type.
// * ANYARRAY allows only array types.
//
// The rules for argument validity are as follows:
//  1. The arguments supplied for ANYELEMENT parameters must all have the same
//     type.
//  2. The supplied types for ANYARRAY parameters must match each other, and the
//     array *element* type must match all ANYELEMENT parameters.
//  3. NULL arguments are exempt from the above two rules. However, there must
//     be at least one non-NULL argument in order to resolve a concrete type.
//
// anyElemTyp, if non-nil, allows the caller to pass in the expected concrete
// type for ANYELEMENT parameters.
//
// enforceConsistency, if true, indicates that ResolvePolymorphicArgTypes should
// throw a suitable error in the case of invalid arguments, rather than
// returning with ok=false. Note: we do this instead of always returning the
// error because error construction can be expensive.
func ResolvePolymorphicArgTypes(
	paramTypes ParamTypes, argTypes []*types.T, anyElemTyp *types.T, enforceConsistency bool,
) (ok bool, numPolyParams int, _ *types.T) {
	var anyArrayTyp *types.T
	for i := range paramTypes {
		paramTyp := paramTypes[i].Typ
		if !paramTyp.IsPolymorphicType() {
			continue
		}
		argTyp := argTypes[i]
		numPolyParams++
		if argTyp.Family() == types.UnknownFamily {
			continue
		}
		maybeMakeNotAlikeErr := func(polyTypeName string, actualTyp, expectedTyp *types.T) {
			if enforceConsistency {
				err := pgerror.Newf(pgcode.DatatypeMismatch,
					"arguments declared \"%s\" are not all alike", polyTypeName,
				)
				panic(errors.WithDetailf(err, "%s versus %s", actualTyp, expectedTyp))
			}
		}
		switch paramTyp.Family() {
		case types.AnyFamily:
			if anyElemTyp == nil {
				anyElemTyp = argTyp
			} else if !anyElemTyp.Identical(argTyp) {
				maybeMakeNotAlikeErr("anyelement", anyElemTyp, argTyp)
				return false, 0, nil
			}
		case types.ArrayFamily:
			if anyArrayTyp == nil {
				anyArrayTyp = argTyp
			} else if !anyArrayTyp.Identical(argTyp) {
				maybeMakeNotAlikeErr("anyarray", anyArrayTyp, argTyp)
				return false, 0, nil
			}
		case types.EnumFamily:
			panic(unimplemented.NewWithIssue(123048, "ANYENUM is not yet supported"))
		default:
			panic(errors.AssertionFailedf("unexpected type: %s", paramTyp.SQLStringForError()))
		}
	}
	if numPolyParams == 0 {
		return true, 0, nil
	}
	if anyArrayTyp != nil {
		if anyArrayTyp.Family() != types.ArrayFamily {
			if enforceConsistency {
				panic(pgerror.Newf(pgcode.DatatypeMismatch,
					"argument declared anyarray is not an array but type %s", anyArrayTyp,
				))
			}
			return false, 0, nil
		}
		if anyElemTyp == nil {
			// Derive the type from the array element type.
			anyElemTyp = anyArrayTyp.ArrayContents()
		} else if !anyArrayTyp.ArrayContents().Identical(anyElemTyp) {
			if enforceConsistency {
				err := pgerror.New(pgcode.DatatypeMismatch,
					"argument declared anyarray is not consistent with argument declared anyelement",
				)
				panic(errors.WithDetailf(err, "%s versus %s", anyArrayTyp, anyElemTyp))
			}
			return false, 0, nil
		}
	}
	return true, numPolyParams, anyElemTyp
}

// UnsupportedTypeChecker is used to check that a type is supported by the
// current cluster version. It is an interface because some packages cannot
// import the clusterversion package.
type UnsupportedTypeChecker interface {
	// CheckType returns an error if the given type is not supported by the
	// current cluster version.
	CheckType(ctx context.Context, typ *types.T) error
}

// CheckUnsupportedType returns an error if the given type is not supported by
// the current cluster version. If the given SemaContext is nil or
// uninitialized, CheckUnsupportedType returns nil.
func CheckUnsupportedType(ctx context.Context, semaCtx *SemaContext, typ *types.T) error {
	if semaCtx == nil || semaCtx.UnsupportedTypeChecker == nil {
		// Sometimes TypeCheck() is called with a nil SemaContext for tests, and
		// some (non-test) locations don't initialize all fields of the SemaContext.
		return nil
	}
	return semaCtx.UnsupportedTypeChecker.CheckType(ctx, typ)
}

var CannotAcceptTriggerErr = pgerror.New(pgcode.FeatureNotSupported,
	"cannot accept a value of type trigger",
)

// checkComparison checks whether the given types are or contain the
// given family, which is invalid for comparison. We don't simply remove
// the relevant comparison overloads because we rely on their existence in
// various locations throughout the codebase.
func checkComparison(
	op treecmp.ComparisonOperatorSymbol, left, right *types.T, family types.Family,
) error {
	if (op == treecmp.IsNotDistinctFrom || op == treecmp.IsDistinctFrom) &&
		(left.Family() == family && right.Family() == types.UnknownFamily) {
		// Special case: "<family> IS [NOT] DISTINCT FROM NULL" is allowed.
		return nil
	}
	if left.Family() == family || right.Family() == family {
		return pgerror.Newf(pgcode.UndefinedFunction,
			"unsupported comparison operator: %s %s %s", left, op, right,
		)
	}
	var checkRecursive func(*types.T) bool
	checkRecursive = func(typ *types.T) bool {
		switch typ.Family() {
		case family:
			return true
		case types.TupleFamily:
			for _, t := range typ.TupleContents() {
				if checkRecursive(t) {
					return true
				}
			}
		case types.ArrayFamily:
			return checkRecursive(typ.ArrayContents())
		}
		return false
	}
	if checkRecursive(left) || checkRecursive(right) {
		return pgerror.Newf(pgcode.UndefinedFunction,
			"could not identify a comparison function for family %q", family.String(),
		)
	}
	return nil
}

func runValidations(
	op treecmp.ComparisonOperatorSymbol, left, right *types.T, families []types.Family,
) error {
	for _, family := range families {
		if err := checkComparison(op, left, right, family); err != nil {
			return err
		}
	}
	return nil
}
