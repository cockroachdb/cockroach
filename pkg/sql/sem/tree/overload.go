// Copyright 2016 The Cockroach Authors.
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
	"bytes"
	"context"
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// SpecializedVectorizedBuiltin is used to map overloads
// to the vectorized operator that is specific to
// that implementation of the builtin function.
type SpecializedVectorizedBuiltin int

// TODO (rohany): What is the best place to put this list?
// I want to put it in builtins or exec, but those create an import
// cycle with exec. tree is imported by both of them, so
// this package seems like a good place to do it.

// Keep this list alphabetized so that it is easy to manage.
const (
	_ SpecializedVectorizedBuiltin = iota
	SubstringStringIntInt
)

// Overload is one of the overloads of a built-in function.
// Each FunctionDefinition may contain one or more overloads.
type Overload struct {
	Types      TypeList
	ReturnType ReturnTyper
	Volatility Volatility

	// PreferredOverload determines overload resolution as follows.
	// When multiple overloads are eligible based on types even after all of of
	// the heuristics to pick one have been used, if one of the overloads is a
	// Overload with the `PreferredOverload` flag set to true it can be selected
	// rather than returning a no-such-method error.
	// This should generally be avoided -- avoiding introducing ambiguous
	// overloads in the first place is a much better solution -- and only done
	// after consultation with @knz @nvanbenschoten.
	PreferredOverload bool

	// Info is a description of the function, which is surfaced on the CockroachDB
	// docs site on the "Functions and Operators" page. Descriptions typically use
	// third-person with the function as an implicit subject (e.g. "Calculates
	// infinity"), but should focus more on ease of understanding so other structures
	// might be more appropriate.
	Info string

	AggregateFunc func([]*types.T, *EvalContext, Datums) AggregateFunc
	WindowFunc    func([]*types.T, *EvalContext) WindowFunc
	Fn            func(*EvalContext, Datums) (Datum, error)
	Generator     GeneratorFactory

	// SQLFn must be set for overloads of type SQLClass. It should return a SQL
	// statement which will be executed as a common table expression in the query.
	SQLFn func(*EvalContext, Datums) (string, error)

	// counter, if non-nil, should be incremented upon successful
	// type check of expressions using this overload.
	counter telemetry.Counter

	// SpecializedVecBuiltin is used to let the vectorized engine
	// know when an Overload has a specialized vectorized operator.
	SpecializedVecBuiltin SpecializedVectorizedBuiltin

	// IgnoreVolatilityCheck ignores checking the functions overload's
	// volatility against Postgres's volatility at test time.
	// This should be used with caution.
	IgnoreVolatilityCheck bool

	// Oid is the cached oidHasher.BuiltinOid result for this Overload. It's
	// populated at init-time.
	Oid oid.Oid
}

// params implements the overloadImpl interface.
func (b Overload) params() TypeList { return b.Types }

// returnType implements the overloadImpl interface.
func (b Overload) returnType() ReturnTyper { return b.ReturnType }

// preferred implements the overloadImpl interface.
func (b Overload) preferred() bool { return b.PreferredOverload }

// FixedReturnType returns a fixed type that the function returns, returning Any
// if the return type is based on the function's arguments.
func (b Overload) FixedReturnType() *types.T {
	if b.ReturnType == nil {
		return nil
	}
	return returnTypeToFixedType(b.ReturnType, nil)
}

// InferReturnTypeFromInputArgTypes returns the type that the function returns,
// inferring the type based on the function's inputTypes if necessary.
func (b Overload) InferReturnTypeFromInputArgTypes(inputTypes []*types.T) *types.T {
	retTyp := b.FixedReturnType()
	// If the output type of the function depends on its inputs, then
	// the output of FixedReturnType will be ambiguous. In the ambiguous
	// cases, use the information about the input types to construct the
	// appropriate output type. The tree.ReturnTyper interface is
	// []tree.TypedExpr -> *types.T, so construct the []tree.TypedExpr
	// from the types that we know are the inputs. Note that we don't
	// try to create datums of each input type, and instead use this
	// "TypedDummy" construct. This is because some types don't have resident
	// members (like an ENUM with no values), and we shouldn't error out
	// trying to infer the return type in those cases.
	if retTyp.IsAmbiguous() {
		args := make([]TypedExpr, len(inputTypes))
		for i, t := range inputTypes {
			args[i] = &TypedDummy{Typ: t}
		}
		// Evaluate ReturnType with the fake input set of arguments.
		retTyp = returnTypeToFixedType(b.ReturnType, args)
	}
	return retTyp
}

// Signature returns a human-readable signature.
// If simplify is bool, tuple-returning functions with just
// 1 tuple element unwrap the return type in the signature.
func (b Overload) Signature(simplify bool) string {
	retType := b.FixedReturnType()
	if simplify {
		if retType.Family() == types.TupleFamily && len(retType.TupleContents()) == 1 {
			retType = retType.TupleContents()[0]
		}
	}
	return fmt.Sprintf("(%s) -> %s", b.Types.String(), retType)
}

// overloadImpl is an implementation of an overloaded function. It provides
// access to the parameter type list  and the return type of the implementation.
//
// This is a more general type than Overload defined above, because it also
// works with the built-in binary and unary operators.
type overloadImpl interface {
	params() TypeList
	returnType() ReturnTyper
	// allows manually resolving preference between multiple compatible overloads
	preferred() bool
}

var _ overloadImpl = &Overload{}
var _ overloadImpl = &UnaryOp{}
var _ overloadImpl = &BinOp{}

// GetParamsAndReturnType gets the parameters and return type of an
// overloadImpl.
func GetParamsAndReturnType(impl overloadImpl) (TypeList, ReturnTyper) {
	return impl.params(), impl.returnType()
}

// TypeList is a list of types representing a function parameter list.
type TypeList interface {
	// Match checks if all types in the TypeList match the corresponding elements in types.
	Match(types []*types.T) bool
	// MatchAt checks if the parameter type at index i of the TypeList matches type typ.
	// In all implementations, types.Null will match with each parameter type, allowing
	// NULL values to be used as arguments.
	MatchAt(typ *types.T, i int) bool
	// matchLen checks that the TypeList can support l parameters.
	MatchLen(l int) bool
	// getAt returns the type at the given index in the TypeList, or nil if the TypeList
	// cannot have a parameter at index i.
	GetAt(i int) *types.T
	// Length returns the number of types in the list
	Length() int
	// Types returns a realized copy of the list. variadic lists return a list of size one.
	Types() []*types.T
	// String returns a human readable signature
	String() string
}

var _ TypeList = ArgTypes{}
var _ TypeList = HomogeneousType{}
var _ TypeList = VariadicType{}

// ArgTypes is very similar to ArgTypes except it allows keeping a string
// name for each argument as well and using those when printing the
// human-readable signature.
type ArgTypes []struct {
	Name string
	Typ  *types.T
}

// Match is part of the TypeList interface.
func (a ArgTypes) Match(types []*types.T) bool {
	if len(types) != len(a) {
		return false
	}
	for i := range types {
		if !a.MatchAt(types[i], i) {
			return false
		}
	}
	return true
}

// MatchAt is part of the TypeList interface.
func (a ArgTypes) MatchAt(typ *types.T, i int) bool {
	// The parameterized types for Tuples are checked in the type checking
	// routines before getting here, so we only need to check if the argument
	// type is a types.TUPLE below. This allows us to avoid defining overloads
	// for types.Tuple{}, types.Tuple{types.Any}, types.Tuple{types.Any, types.Any},
	// etc. for Tuple operators.
	if typ.Family() == types.TupleFamily {
		typ = types.AnyTuple
	}
	return i < len(a) && (typ.Family() == types.UnknownFamily || a[i].Typ.Equivalent(typ))
}

// MatchLen is part of the TypeList interface.
func (a ArgTypes) MatchLen(l int) bool {
	return len(a) == l
}

// GetAt is part of the TypeList interface.
func (a ArgTypes) GetAt(i int) *types.T {
	return a[i].Typ
}

// Length is part of the TypeList interface.
func (a ArgTypes) Length() int {
	return len(a)
}

// Types is part of the TypeList interface.
func (a ArgTypes) Types() []*types.T {
	n := len(a)
	ret := make([]*types.T, n)
	for i, s := range a {
		ret[i] = s.Typ
	}
	return ret
}

func (a ArgTypes) String() string {
	var s bytes.Buffer
	for i, arg := range a {
		if i > 0 {
			s.WriteString(", ")
		}
		s.WriteString(arg.Name)
		s.WriteString(": ")
		s.WriteString(arg.Typ.String())
	}
	return s.String()
}

// HomogeneousType is a TypeList implementation that accepts any arguments, as
// long as all are the same type or NULL. The homogeneous constraint is enforced
// in typeCheckOverloadedExprs.
type HomogeneousType struct{}

// Match is part of the TypeList interface.
func (HomogeneousType) Match(types []*types.T) bool {
	return true
}

// MatchAt is part of the TypeList interface.
func (HomogeneousType) MatchAt(typ *types.T, i int) bool {
	return true
}

// MatchLen is part of the TypeList interface.
func (HomogeneousType) MatchLen(l int) bool {
	return true
}

// GetAt is part of the TypeList interface.
func (HomogeneousType) GetAt(i int) *types.T {
	return types.Any
}

// Length is part of the TypeList interface.
func (HomogeneousType) Length() int {
	return 1
}

// Types is part of the TypeList interface.
func (HomogeneousType) Types() []*types.T {
	return []*types.T{types.Any}
}

func (HomogeneousType) String() string {
	return "anyelement..."
}

// VariadicType is a TypeList implementation which accepts a fixed number of
// arguments at the beginning and an arbitrary number of homogenous arguments
// at the end.
type VariadicType struct {
	FixedTypes []*types.T
	VarType    *types.T
}

// Match is part of the TypeList interface.
func (v VariadicType) Match(types []*types.T) bool {
	for i := range types {
		if !v.MatchAt(types[i], i) {
			return false
		}
	}
	return true
}

// MatchAt is part of the TypeList interface.
func (v VariadicType) MatchAt(typ *types.T, i int) bool {
	if i < len(v.FixedTypes) {
		return typ.Family() == types.UnknownFamily || v.FixedTypes[i].Equivalent(typ)
	}
	return typ.Family() == types.UnknownFamily || v.VarType.Equivalent(typ)
}

// MatchLen is part of the TypeList interface.
func (v VariadicType) MatchLen(l int) bool {
	return l >= len(v.FixedTypes)
}

// GetAt is part of the TypeList interface.
func (v VariadicType) GetAt(i int) *types.T {
	if i < len(v.FixedTypes) {
		return v.FixedTypes[i]
	}
	return v.VarType
}

// Length is part of the TypeList interface.
func (v VariadicType) Length() int {
	return len(v.FixedTypes) + 1
}

// Types is part of the TypeList interface.
func (v VariadicType) Types() []*types.T {
	result := make([]*types.T, len(v.FixedTypes)+1)
	for i := range v.FixedTypes {
		result[i] = v.FixedTypes[i]
	}
	result[len(result)-1] = v.VarType
	return result
}

func (v VariadicType) String() string {
	var s bytes.Buffer
	for i, t := range v.FixedTypes {
		if i != 0 {
			s.WriteString(", ")
		}
		s.WriteString(t.String())
	}
	if len(v.FixedTypes) > 0 {
		s.WriteString(", ")
	}
	fmt.Fprintf(&s, "%s...", v.VarType)
	return s.String()
}

// UnknownReturnType is returned from ReturnTypers when the arguments provided are
// not sufficient to determine a return type. This is necessary for cases like overload
// resolution, where the argument types are not resolved yet so the type-level function
// will be called without argument types. If a ReturnTyper returns unknownReturnType,
// then the candidate function set cannot be refined. This means that only ReturnTypers
// that never return unknownReturnType, like those created with FixedReturnType, can
// help reduce overload ambiguity.
var UnknownReturnType *types.T

// ReturnTyper defines the type-level function in which a builtin function's return type
// is determined. ReturnTypers should make sure to return unknownReturnType when necessary.
type ReturnTyper func(args []TypedExpr) *types.T

// FixedReturnType functions simply return a fixed type, independent of argument types.
func FixedReturnType(typ *types.T) ReturnTyper {
	return func(args []TypedExpr) *types.T { return typ }
}

// IdentityReturnType creates a returnType that is a projection of the idx'th
// argument type.
func IdentityReturnType(idx int) ReturnTyper {
	return func(args []TypedExpr) *types.T {
		if len(args) == 0 {
			return UnknownReturnType
		}
		return args[idx].ResolvedType()
	}
}

// ArrayOfFirstNonNullReturnType returns an array type from the first non-null
// type in the argument list.
func ArrayOfFirstNonNullReturnType() ReturnTyper {
	return func(args []TypedExpr) *types.T {
		if len(args) == 0 {
			return UnknownReturnType
		}
		for _, arg := range args {
			if t := arg.ResolvedType(); t.Family() != types.UnknownFamily {
				return types.MakeArray(t)
			}
		}
		return types.Unknown
	}
}

// FirstNonNullReturnType returns the type of the first non-null argument, or
// types.Unknown if all arguments are null. There must be at least one argument,
// or else FirstNonNullReturnType returns UnknownReturnType. This method is used
// with HomogeneousType functions, in which all arguments have been checked to
// have the same type (or be null).
func FirstNonNullReturnType() ReturnTyper {
	return func(args []TypedExpr) *types.T {
		if len(args) == 0 {
			return UnknownReturnType
		}
		for _, arg := range args {
			if t := arg.ResolvedType(); t.Family() != types.UnknownFamily {
				return t
			}
		}
		return types.Unknown
	}
}

func returnTypeToFixedType(s ReturnTyper, inputTyps []TypedExpr) *types.T {
	if t := s(inputTyps); t != UnknownReturnType {
		return t
	}
	return types.Any
}

type typeCheckOverloadState struct {
	overloads       []overloadImpl
	overloadIdxs    []uint8 // index into overloads
	exprs           []Expr
	typedExprs      []TypedExpr
	resolvableIdxs  []int // index into exprs/typedExprs
	constIdxs       []int // index into exprs/typedExprs
	placeholderIdxs []int // index into exprs/typedExprs
}

// typeCheckOverloadedExprs determines the correct overload to use for the given set of
// expression parameters, along with an optional desired return type. It returns the expression
// parameters after being type checked, along with a slice of candidate overloadImpls. The
// slice may have length:
//   0: overload resolution failed because no compatible overloads were found
//   1: overload resolution succeeded
//  2+: overload resolution failed because of ambiguity
// The inBinOp parameter denotes whether this type check is occurring within a binary operator,
// in which case we may need to make a guess that the two parameters are of the same type if one
// of them is NULL.
func typeCheckOverloadedExprs(
	ctx context.Context,
	semaCtx *SemaContext,
	desired *types.T,
	overloads []overloadImpl,
	inBinOp bool,
	exprs ...Expr,
) ([]TypedExpr, []overloadImpl, error) {
	if len(overloads) > math.MaxUint8 {
		return nil, nil, errors.AssertionFailedf("too many overloads (%d > 255)", len(overloads))
	}

	var s typeCheckOverloadState
	s.exprs = exprs
	s.overloads = overloads

	// Special-case the HomogeneousType overload. We determine its return type by checking that
	// all parameters have the same type.
	for i, overload := range overloads {
		// Only one overload can be provided if it has parameters with HomogeneousType.
		if _, ok := overload.params().(HomogeneousType); ok {
			if len(overloads) > 1 {
				return nil, nil, errors.AssertionFailedf(
					"only one overload can have HomogeneousType parameters")
			}
			typedExprs, _, err := TypeCheckSameTypedExprs(ctx, semaCtx, desired, exprs...)
			if err != nil {
				return nil, nil, err
			}
			return typedExprs, overloads[i : i+1], nil
		}
	}

	// Hold the resolved type expressions of the provided exprs, in order.
	s.typedExprs = make([]TypedExpr, len(exprs))
	s.constIdxs, s.placeholderIdxs, s.resolvableIdxs = typeCheckSplitExprs(ctx, semaCtx, exprs)

	// If no overloads are provided, just type check parameters and return.
	if len(overloads) == 0 {
		for _, i := range s.resolvableIdxs {
			typ, err := exprs[i].TypeCheck(ctx, semaCtx, types.Any)
			if err != nil {
				return nil, nil, pgerror.Wrapf(err, pgcode.InvalidParameterValue,
					"error type checking resolved expression:")
			}
			s.typedExprs[i] = typ
		}
		if err := defaultTypeCheck(ctx, semaCtx, &s, false); err != nil {
			return nil, nil, err
		}
		return s.typedExprs, nil, nil
	}

	s.overloadIdxs = make([]uint8, len(overloads))
	for i := 0; i < len(overloads); i++ {
		s.overloadIdxs[i] = uint8(i)
	}

	// Filter out incorrect parameter length overloads.
	s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
		func(o overloadImpl) bool {
			return o.params().MatchLen(len(exprs))
		})

	// Filter out overloads which constants cannot become.
	for _, i := range s.constIdxs {
		constExpr := exprs[i].(Constant)
		s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
			func(o overloadImpl) bool {
				return canConstantBecome(constExpr, o.params().GetAt(i))
			})
	}

	// TODO(nvanbenschoten): We should add a filtering step here to filter
	// out impossible candidates based on identical parameters. For instance,
	// f(int, float) is not a possible candidate for the expression f($1, $1).

	// Filter out overloads on resolved types.
	for _, i := range s.resolvableIdxs {
		paramDesired := types.Any

		// If all remaining candidates require the same type for this parameter,
		// begin desiring that type for the corresponding argument expression.
		// Note that this is always the case when we have a single overload left.
		var sameType *types.T
		for _, ovIdx := range s.overloadIdxs {
			typ := s.overloads[ovIdx].params().GetAt(i)
			if sameType == nil {
				sameType = typ
			} else if !typ.Identical(sameType) {
				sameType = nil
				break
			}
		}
		if sameType != nil {
			paramDesired = sameType
		}
		typ, err := exprs[i].TypeCheck(ctx, semaCtx, paramDesired)
		if err != nil {
			return nil, nil, err
		}
		s.typedExprs[i] = typ
		s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
			func(o overloadImpl) bool {
				return o.params().MatchAt(typ.ResolvedType(), i)
			})
	}

	// At this point, all remaining overload candidates accept the argument list,
	// so we begin checking for a single remaining candidate implementation to choose.
	// In case there is more than one candidate remaining, the following code uses
	// heuristics to find a most preferable candidate.
	if ok, typedExprs, fns, err := checkReturn(ctx, semaCtx, &s); ok {
		return typedExprs, fns, err
	}

	// The first heuristic is to prefer candidates that return the desired type.
	if desired.Family() != types.AnyFamily {
		s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
			func(o overloadImpl) bool {
				// For now, we only filter on the return type for overloads with
				// fixed return types. This could be improved, but is not currently
				// critical because we have no cases of functions with multiple
				// overloads that do not all expose FixedReturnTypes.
				if t := o.returnType()(nil); t != UnknownReturnType {
					return t.Equivalent(desired)
				}
				return true
			})
		if ok, typedExprs, fns, err := checkReturn(ctx, semaCtx, &s); ok {
			return typedExprs, fns, err
		}
	}

	var homogeneousTyp *types.T
	if len(s.resolvableIdxs) > 0 {
		homogeneousTyp = s.typedExprs[s.resolvableIdxs[0]].ResolvedType()
		for _, i := range s.resolvableIdxs[1:] {
			if !homogeneousTyp.Equivalent(s.typedExprs[i].ResolvedType()) {
				homogeneousTyp = nil
				break
			}
		}
	}

	if len(s.constIdxs) > 0 {
		allConstantsAreHomogenous := false
		if ok, typedExprs, fns, err := filterAttempt(ctx, semaCtx, &s, func() {
			// The second heuristic is to prefer candidates where all constants can
			// become a homogeneous type, if all resolvable expressions became one.
			// This is only possible if resolvable expressions were resolved
			// homogeneously up to this point.
			if homogeneousTyp != nil {
				allConstantsAreHomogenous = true
				for _, i := range s.constIdxs {
					if !canConstantBecome(exprs[i].(Constant), homogeneousTyp) {
						allConstantsAreHomogenous = false
						break
					}
				}
				if allConstantsAreHomogenous {
					for _, i := range s.constIdxs {
						s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
							func(o overloadImpl) bool {
								return o.params().GetAt(i).Equivalent(homogeneousTyp)
							})
					}
				}
			}
		}); ok {
			return typedExprs, fns, err
		}

		if ok, typedExprs, fns, err := filterAttempt(ctx, semaCtx, &s, func() {
			// The third heuristic is to prefer candidates where all constants can
			// become their "natural" types.
			for _, i := range s.constIdxs {
				natural := naturalConstantType(exprs[i].(Constant))
				if natural != nil {
					s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
						func(o overloadImpl) bool {
							return o.params().GetAt(i).Equivalent(natural)
						})
				}
			}
		}); ok {
			return typedExprs, fns, err
		}

		// At this point, it's worth seeing if we have constants that can't actually
		// parse as the type that canConstantBecome claims they can. For example,
		// every string literal will report that it can become an interval, but most
		// string literals do not encode valid intervals. This may uncover some
		// overloads with invalid type signatures.
		//
		// This parsing is sufficiently expensive (see the comment on
		// StrVal.AvailableTypes) that we wait until now, when we've eliminated most
		// overloads from consideration, so that we only need to check each constant
		// against a limited set of types. We can't hold off on this parsing any
		// longer, though: the remaining heuristics are overly aggressive and will
		// falsely reject the only valid overload in some cases.
		//
		// This case is broken into two parts. We first attempt to use the
		// information about the homogeneity of our constants collected by previous
		// heuristic passes. If:
		// * all our constants are homogeneous
		// * we only have a single overload left
		// * the constant overload parameters are homogeneous as well
		// then match this overload with the homogeneous constants. Otherwise,
		// continue to filter overloads by whether or not the constants can parse
		// into the desired types of the overloads.
		// This first case is important when resolving overloads for operations
		// between user-defined types, where we need to propagate the concrete
		// resolved type information over to the constants, rather than attempting
		// to resolve constants as the placeholder type for the user defined type
		// family (like `AnyEnum`).
		if len(s.overloadIdxs) == 1 && allConstantsAreHomogenous {
			overloadParamsAreHomogenous := true
			p := s.overloads[s.overloadIdxs[0]].params()
			for _, i := range s.constIdxs {
				if !p.GetAt(i).Equivalent(homogeneousTyp) {
					overloadParamsAreHomogenous = false
					break
				}
			}
			if overloadParamsAreHomogenous {
				// Type check our constants using the homogeneous type rather than
				// the type in overload parameter. This lets us type check user defined
				// types with a concrete type instance, rather than an ambiguous type.
				for _, i := range s.constIdxs {
					typ, err := s.exprs[i].TypeCheck(ctx, semaCtx, homogeneousTyp)
					if err != nil {
						return nil, nil, err
					}
					s.typedExprs[i] = typ
				}
				_, typedExprs, fn, err := checkReturnPlaceholdersAtIdx(ctx, semaCtx, &s, int(s.overloadIdxs[0]))
				return typedExprs, fn, err
			}
		}
		for _, i := range s.constIdxs {
			constExpr := exprs[i].(Constant)
			s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
				func(o overloadImpl) bool {
					semaCtx := MakeSemaContext()
					_, err := constExpr.ResolveAsType(ctx, &semaCtx, o.params().GetAt(i))
					return err == nil
				})
		}
		if ok, typedExprs, fn, err := checkReturn(ctx, semaCtx, &s); ok {
			return typedExprs, fn, err
		}

		// The fourth heuristic is to prefer candidates that accepts the "best"
		// mutual type in the resolvable type set of all constants.
		if bestConstType, ok := commonConstantType(s.exprs, s.constIdxs); ok {
			for _, i := range s.constIdxs {
				s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
					func(o overloadImpl) bool {
						return o.params().GetAt(i).Equivalent(bestConstType)
					})
			}
			if ok, typedExprs, fns, err := checkReturn(ctx, semaCtx, &s); ok {
				return typedExprs, fns, err
			}
			if homogeneousTyp != nil {
				if !homogeneousTyp.Equivalent(bestConstType) {
					homogeneousTyp = nil
				}
			} else {
				homogeneousTyp = bestConstType
			}
		}
	}

	// The fifth heuristic is to prefer candidates where all placeholders can be
	// given the same type as all constants and resolvable expressions. This is
	// only possible if all constants and resolvable expressions were resolved
	// homogeneously up to this point.
	if homogeneousTyp != nil && len(s.placeholderIdxs) > 0 {
		// Before we continue, try to propagate the homogeneous type to the
		// placeholders. This might not have happened yet, if the overloads'
		// parameter types are ambiguous (like in the case of tuple-tuple binary
		// operators).
		for _, i := range s.placeholderIdxs {
			if _, err := exprs[i].TypeCheck(ctx, semaCtx, homogeneousTyp); err != nil {
				return nil, nil, err
			}
			s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
				func(o overloadImpl) bool {
					return o.params().GetAt(i).Equivalent(homogeneousTyp)
				})
		}
		if ok, typedExprs, fns, err := checkReturn(ctx, semaCtx, &s); ok {
			return typedExprs, fns, err
		}
	}

	// In a binary expression, in the case of one of the arguments being untyped NULL,
	// we prefer overloads where we infer the type of the NULL to be the same as the
	// other argument. This is used to differentiate the behavior of
	// STRING[] || NULL and STRING || NULL.
	if inBinOp && len(s.exprs) == 2 {
		if ok, typedExprs, fns, err := filterAttempt(ctx, semaCtx, &s, func() {
			var err error
			left := s.typedExprs[0]
			if left == nil {
				left, err = s.exprs[0].TypeCheck(ctx, semaCtx, types.Any)
				if err != nil {
					return
				}
			}
			right := s.typedExprs[1]
			if right == nil {
				right, err = s.exprs[1].TypeCheck(ctx, semaCtx, types.Any)
				if err != nil {
					return
				}
			}
			leftType := left.ResolvedType()
			rightType := right.ResolvedType()
			leftIsNull := leftType.Family() == types.UnknownFamily
			rightIsNull := rightType.Family() == types.UnknownFamily
			oneIsNull := (leftIsNull || rightIsNull) && !(leftIsNull && rightIsNull)
			if oneIsNull {
				if leftIsNull {
					leftType = rightType
				}
				if rightIsNull {
					rightType = leftType
				}
				s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
					func(o overloadImpl) bool {
						return o.params().GetAt(0).Equivalent(leftType) &&
							o.params().GetAt(1).Equivalent(rightType)
					})
			}
		}); ok {
			return typedExprs, fns, err
		}
	}

	// After the previous heuristic, in a binary expression, in the case of one of the arguments being untyped
	// NULL, we prefer overloads where we infer the type of the NULL to be a STRING. This is used
	// to choose INT || NULL::STRING over INT || NULL::INT[].
	if inBinOp && len(s.exprs) == 2 {
		if ok, typedExprs, fns, err := filterAttempt(ctx, semaCtx, &s, func() {
			var err error
			left := s.typedExprs[0]
			if left == nil {
				left, err = s.exprs[0].TypeCheck(ctx, semaCtx, types.Any)
				if err != nil {
					return
				}
			}
			right := s.typedExprs[1]
			if right == nil {
				right, err = s.exprs[1].TypeCheck(ctx, semaCtx, types.Any)
				if err != nil {
					return
				}
			}
			leftType := left.ResolvedType()
			rightType := right.ResolvedType()
			leftIsNull := leftType.Family() == types.UnknownFamily
			rightIsNull := rightType.Family() == types.UnknownFamily
			oneIsNull := (leftIsNull || rightIsNull) && !(leftIsNull && rightIsNull)
			if oneIsNull {
				if leftIsNull {
					leftType = types.String
				}
				if rightIsNull {
					rightType = types.String
				}
				s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
					func(o overloadImpl) bool {
						return o.params().GetAt(0).Equivalent(leftType) &&
							o.params().GetAt(1).Equivalent(rightType)
					})
			}
		}); ok {
			return typedExprs, fns, err
		}
	}

	// The final heuristic is to defer to preferred candidates, if available.
	if ok, typedExprs, fns, err := filterAttempt(ctx, semaCtx, &s, func() {
		s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs, func(o overloadImpl) bool {
			return o.preferred()
		})
	}); ok {
		return typedExprs, fns, err
	}

	if err := defaultTypeCheck(ctx, semaCtx, &s, len(s.overloads) > 0); err != nil {
		return nil, nil, err
	}

	possibleOverloads := make([]overloadImpl, len(s.overloadIdxs))
	for i, o := range s.overloadIdxs {
		possibleOverloads[i] = s.overloads[o]
	}
	return s.typedExprs, possibleOverloads, nil
}

// filterAttempt attempts to filter the overloads down to a single candidate.
// If it succeeds, it will return true, along with the overload (in a slice for
// convenience) and a possible error. If it fails, it will return false and
// undo any filtering performed during the attempt.
func filterAttempt(
	ctx context.Context, semaCtx *SemaContext, s *typeCheckOverloadState, attempt func(),
) (ok bool, _ []TypedExpr, _ []overloadImpl, _ error) {
	before := s.overloadIdxs
	attempt()
	if len(s.overloadIdxs) == 1 {
		ok, typedExprs, fns, err := checkReturn(ctx, semaCtx, s)
		if err != nil {
			return false, nil, nil, err
		}
		if ok {
			return true, typedExprs, fns, err
		}
	}
	s.overloadIdxs = before
	return false, nil, nil, nil
}

// filterOverloads filters overloads which do not satisfy the predicate.
func filterOverloads(
	overloads []overloadImpl, overloadIdxs []uint8, fn func(overloadImpl) bool,
) []uint8 {
	for i := 0; i < len(overloadIdxs); {
		if fn(overloads[overloadIdxs[i]]) {
			i++
		} else {
			overloadIdxs[i], overloadIdxs[len(overloadIdxs)-1] = overloadIdxs[len(overloadIdxs)-1], overloadIdxs[i]
			overloadIdxs = overloadIdxs[:len(overloadIdxs)-1]
		}
	}
	return overloadIdxs
}

// defaultTypeCheck type checks the constant and placeholder expressions without a preference
// and adds them to the type checked slice.
func defaultTypeCheck(
	ctx context.Context, semaCtx *SemaContext, s *typeCheckOverloadState, errorOnPlaceholders bool,
) error {
	for _, i := range s.constIdxs {
		typ, err := s.exprs[i].TypeCheck(ctx, semaCtx, types.Any)
		if err != nil {
			return pgerror.Wrapf(err, pgcode.InvalidParameterValue,
				"error type checking constant value")
		}
		s.typedExprs[i] = typ
	}
	for _, i := range s.placeholderIdxs {
		if errorOnPlaceholders {
			_, err := s.exprs[i].TypeCheck(ctx, semaCtx, types.Any)
			return err
		}
		// If we dont want to error on args, avoid type checking them without a desired type.
		s.typedExprs[i] = StripParens(s.exprs[i]).(*Placeholder)
	}
	return nil
}

// checkReturn checks the number of remaining overloaded function
// implementations.
// Returns ok=true if we should stop overload resolution, and returning either
// 1. the chosen overload in a slice, or
// 2. nil,
// along with the typed arguments.
// This modifies values within s as scratch slices, but only in the case where
// it returns true, which signals to the calling function that it should
// immediately return, so any mutations to s are irrelevant.
func checkReturn(
	ctx context.Context, semaCtx *SemaContext, s *typeCheckOverloadState,
) (ok bool, _ []TypedExpr, _ []overloadImpl, _ error) {
	switch len(s.overloadIdxs) {
	case 0:
		if err := defaultTypeCheck(ctx, semaCtx, s, false); err != nil {
			return false, nil, nil, err
		}
		return true, s.typedExprs, nil, nil

	case 1:
		idx := s.overloadIdxs[0]
		o := s.overloads[idx]
		p := o.params()
		for _, i := range s.constIdxs {
			des := p.GetAt(i)
			typ, err := s.exprs[i].TypeCheck(ctx, semaCtx, des)
			if err != nil {
				return false, s.typedExprs, nil, pgerror.Wrapf(
					err, pgcode.InvalidParameterValue,
					"error type checking constant value",
				)
			}
			if des != nil && !typ.ResolvedType().Equivalent(des) {
				return false, nil, nil, errors.AssertionFailedf(
					"desired constant value type %s but set type %s",
					log.Safe(des), log.Safe(typ.ResolvedType()),
				)
			}
			s.typedExprs[i] = typ
		}

		return checkReturnPlaceholdersAtIdx(ctx, semaCtx, s, int(idx))

	default:
		return false, nil, nil, nil
	}
}

// checkReturnPlaceholdersAtIdx checks that the placeholders for the
// overload at the input index are valid. It has the same return values
// as checkReturn.
func checkReturnPlaceholdersAtIdx(
	ctx context.Context, semaCtx *SemaContext, s *typeCheckOverloadState, idx int,
) (bool, []TypedExpr, []overloadImpl, error) {
	o := s.overloads[idx]
	p := o.params()
	for _, i := range s.placeholderIdxs {
		des := p.GetAt(i)
		typ, err := s.exprs[i].TypeCheck(ctx, semaCtx, des)
		if err != nil {
			if des.IsAmbiguous() {
				return false, nil, nil, nil
			}
			return false, nil, nil, err
		}
		s.typedExprs[i] = typ
	}
	return true, s.typedExprs, s.overloads[idx : idx+1], nil
}

func formatCandidates(prefix string, candidates []overloadImpl) string {
	var buf bytes.Buffer
	for _, candidate := range candidates {
		buf.WriteString(prefix)
		buf.WriteByte('(')
		params := candidate.params()
		tLen := params.Length()
		inputTyps := make([]TypedExpr, tLen)
		for i := 0; i < tLen; i++ {
			t := params.GetAt(i)
			inputTyps[i] = &TypedDummy{Typ: t}
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(t.String())
		}
		buf.WriteString(") -> ")
		buf.WriteString(returnTypeToFixedType(candidate.returnType(), inputTyps).String())
		if candidate.preferred() {
			buf.WriteString(" [preferred]")
		}
		buf.WriteByte('\n')
	}
	return buf.String()
}
