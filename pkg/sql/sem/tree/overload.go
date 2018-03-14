// Copyright 2016 The Cockroach Authors.
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
	"bytes"
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/pkg/errors"
)

// overloadImpl is an implementation of an overloaded function. It provides
// access to the parameter type list  and the return type of the implementation.
type overloadImpl interface {
	params() TypeList
	returnType() ReturnTyper
	// allows manually resolving preference between multiple compatible overloads
	preferred() bool
}

// GetParamsAndReturnType gets the parameters and return type of an
// overloadImpl.
func GetParamsAndReturnType(impl overloadImpl) (TypeList, ReturnTyper) {
	return impl.params(), impl.returnType()
}

// TypeList is a list of types representing a function parameter list.
type TypeList interface {
	// match checks if all types in the TypeList match the corresponding elements in types.
	match(types []types.T) bool
	// matchAt checks if the parameter type at index i of the TypeList matches type typ.
	// In all implementations, types.Null will match with each parameter type, allowing
	// NULL values to be used as arguments.
	matchAt(typ types.T, i int) bool
	// matchLen checks that the TypeList can support l parameters.
	matchLen(l int) bool
	// getAt returns the type at the given index in the TypeList, or nil if the TypeList
	// cannot have a parameter at index i.
	getAt(i int) types.T
	// Length returns the number of types in the list
	Length() int
	// Types returns a realized copy of the list. variadic lists return a list of size one.
	Types() []types.T
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
	Typ  types.T
}

func (a ArgTypes) match(types []types.T) bool {
	if len(types) != len(a) {
		return false
	}
	for i := range types {
		if !a.matchAt(types[i], i) {
			return false
		}
	}
	return true
}

func (a ArgTypes) matchAt(typ types.T, i int) bool {
	// The parameterized types for Tuples are checked in the type checking
	// routines before getting here, so we only need to check if the argument
	// type is a types.FamTuple below. This allows us to avoid defining overloads
	// for types.FamTuple{}, types.FamTuple{types.Any}, types.FamTuple{types.Any, types.Any}, etc.
	// for Tuple operators.
	if typ.FamilyEqual(types.FamTuple) {
		typ = types.FamTuple
	}
	return i < len(a) && (typ == types.Unknown || a[i].Typ.Equivalent(typ))
}

func (a ArgTypes) matchLen(l int) bool {
	return len(a) == l
}

func (a ArgTypes) getAt(i int) types.T {
	return a[i].Typ
}

// Length implements the TypeList interface.
func (a ArgTypes) Length() int {
	return len(a)
}

// Types implements the TypeList interface.
func (a ArgTypes) Types() []types.T {
	n := len(a)
	ret := make([]types.T, n)
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

func (HomogeneousType) match(types []types.T) bool {
	return true
}

func (HomogeneousType) matchAt(typ types.T, i int) bool {
	return true
}

func (HomogeneousType) matchLen(l int) bool {
	return true
}

func (HomogeneousType) getAt(i int) types.T {
	return types.Any
}

// Length implements the TypeList interface.
func (HomogeneousType) Length() int {
	return 1
}

// Types implements the TypeList interface.
func (HomogeneousType) Types() []types.T {
	return []types.T{types.Any}
}

func (HomogeneousType) String() string {
	return "anyelement..."
}

// VariadicType is a TypeList implementation which accepts a fixed number of
// arguments at the beginning and an arbitrary number of homogenous arguments
// at the end.
type VariadicType struct {
	FixedTypes []types.T
	VarType    types.T
}

func (v VariadicType) match(types []types.T) bool {
	for i := range types {
		if !v.matchAt(types[i], i) {
			return false
		}
	}
	return true
}

func (v VariadicType) matchAt(typ types.T, i int) bool {
	if i < len(v.FixedTypes) {
		return typ == types.Unknown || v.FixedTypes[i].Equivalent(typ)
	}
	return typ == types.Unknown || v.VarType.Equivalent(typ)
}

func (v VariadicType) matchLen(l int) bool {
	return l >= len(v.FixedTypes)
}

func (v VariadicType) getAt(i int) types.T {
	if i < len(v.FixedTypes) {
		return v.FixedTypes[i]
	}
	return v.VarType
}

// Length implements the TypeList interface.
func (v VariadicType) Length() int {
	return len(v.FixedTypes) + 1
}

// Types implements the TypeList interface.
func (v VariadicType) Types() []types.T {
	result := make([]types.T, len(v.FixedTypes)+1)
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
var UnknownReturnType types.T

// ReturnTyper defines the type-level function in which a builtin function's return type
// is determined. ReturnTypers should make sure to return unknownReturnType when necessary.
type ReturnTyper func(args []TypedExpr) types.T

// FixedReturnType functions simply return a fixed type, independent of argument types.
func FixedReturnType(typ types.T) ReturnTyper {
	return func(args []TypedExpr) types.T { return typ }
}

// IdentityReturnType creates a returnType that is a projection of the idx'th
// argument type.
func IdentityReturnType(idx int) ReturnTyper {
	return func(args []TypedExpr) types.T {
		if len(args) == 0 {
			return UnknownReturnType
		}
		return args[idx].ResolvedType()
	}
}

func returnTypeToFixedType(s ReturnTyper) types.T {
	if t := s(nil); t != UnknownReturnType {
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
	ctx *SemaContext, desired types.T, overloads []overloadImpl, inBinOp bool, exprs ...Expr,
) ([]TypedExpr, []overloadImpl, error) {
	if len(overloads) > math.MaxUint8 {
		return nil, nil, pgerror.NewErrorf(pgerror.CodeInternalError, "too many overloads (%d > 255)", len(overloads))
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
				panic("only one overload can have HomogeneousType parameters")
			}
			typedExprs, _, err := TypeCheckSameTypedExprs(ctx, desired, exprs...)
			if err != nil {
				return nil, nil, err
			}
			return typedExprs, overloads[i : i+1], nil
		}
	}

	// Hold the resolved type expressions of the provided exprs, in order.
	s.typedExprs = make([]TypedExpr, len(exprs))
	s.constIdxs, s.placeholderIdxs, s.resolvableIdxs = typeCheckSplitExprs(ctx, exprs)

	// If no overloads are provided, just type check parameters and return.
	if len(overloads) == 0 {
		for _, i := range s.resolvableIdxs {
			typ, err := exprs[i].TypeCheck(ctx, types.Any)
			if err != nil {
				return nil, nil, errors.Wrap(err, "error type checking resolved expression:")
			}
			s.typedExprs[i] = typ
		}
		var err error
		if s, err = defaultTypeCheck(ctx, s, false); err != nil {
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
			return o.params().matchLen(len(exprs))
		})

	// Filter out overloads which constants cannot become.
	for _, i := range s.constIdxs {
		constExpr := exprs[i].(Constant)
		s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
			func(o overloadImpl) bool {
				return canConstantBecome(constExpr, o.params().getAt(i))
			})
	}

	// TODO(nvanbenschoten): We should add a filtering step here to filter
	// out impossible candidates based on identical parameters. For instance,
	// f(int, float) is not a possible candidate for the expression f($1, $1).

	// Filter out overloads on resolved types.
	for _, i := range s.resolvableIdxs {
		paramDesired := types.Any
		if len(s.overloadIdxs) == 1 {
			// Once we get down to a single overload candidate, begin desiring its
			// parameter types for the corresponding argument expressions.
			paramDesired = s.overloads[s.overloadIdxs[0]].params().getAt(i)
		}
		typ, err := exprs[i].TypeCheck(ctx, paramDesired)
		if err != nil {
			return nil, nil, err
		}
		s.typedExprs[i] = typ
		s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
			func(o overloadImpl) bool {
				return o.params().matchAt(typ.ResolvedType(), i)
			})
	}

	// At this point, all remaining overload candidates accept the argument list,
	// so we begin checking for a single remaining candidate implementation to choose.
	// In case there is more than one candidate remaining, the following code uses
	// heuristics to find a most preferable candidate.
	if types, fns, ok, err := checkReturn(ctx, s); ok {
		return types, fns, err
	}

	// The first heuristic is to prefer candidates that return the desired type.
	if desired != types.Any {
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
		if types, fns, ok, err := checkReturn(ctx, s); ok {
			return types, fns, err
		}
	}

	var homogeneousTyp types.T
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
		if ok, fns, err := filterAttempt(ctx, &s, func() {
			// The second heuristic is to prefer candidates where all constants can
			// become a homogeneous type, if all resolvable expressions became one.
			// This is only possible resolvable expressions were resolved
			// homogeneously up to this point.
			if homogeneousTyp != nil {
				all := true
				for _, i := range s.constIdxs {
					if !canConstantBecome(exprs[i].(Constant), homogeneousTyp) {
						all = false
						break
					}
				}
				if all {
					for _, i := range s.constIdxs {
						s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
							func(o overloadImpl) bool {
								return o.params().getAt(i).Equivalent(homogeneousTyp)
							})
					}
				}
			}
		}); ok {
			return s.typedExprs, fns, err
		}

		if ok, fns, err := filterAttempt(ctx, &s, func() {
			// The third heuristic is to prefer candidates where all constants can
			// become their "natural" types.
			for _, i := range s.constIdxs {
				natural := naturalConstantType(exprs[i].(Constant))
				if natural != nil {
					s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
						func(o overloadImpl) bool {
							return o.params().getAt(i).Equivalent(natural)
						})
				}
			}
		}); ok {
			return s.typedExprs, fns, err
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
		for _, i := range s.constIdxs {
			constExpr := exprs[i].(Constant)
			s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
				func(o overloadImpl) bool {
					_, err := constExpr.ResolveAsType(&SemaContext{}, o.params().getAt(i))
					return err == nil
				})
		}
		if types, fn, ok, err := checkReturn(ctx, s); ok {
			return types, fn, err
		}

		// The fourth heuristic is to prefer candidates that accepts the "best"
		// mutual type in the resolvable type set of all constants.
		if bestConstType, ok := commonConstantType(s.exprs, s.constIdxs); ok {
			for _, i := range s.constIdxs {
				s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
					func(o overloadImpl) bool {
						return o.params().getAt(i).Equivalent(bestConstType)
					})
			}
			if types, fns, ok, err := checkReturn(ctx, s); ok {
				return types, fns, err
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
		for _, i := range s.placeholderIdxs {
			s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
				func(o overloadImpl) bool {
					return o.params().getAt(i).Equivalent(homogeneousTyp)
				})
		}
		if types, fns, ok, err := checkReturn(ctx, s); ok {
			return types, fns, err
		}
	}

	// In a binary expression, in the case of one of the arguments being untyped NULL,
	// we prefer overloads where we infer the type of the NULL to be the same as the
	// other argument. This is used to differentiate the behavior of
	// STRING[] || NULL and STRING || NULL.
	if inBinOp && len(s.exprs) == 2 {
		if ok, fns, err := filterAttempt(ctx, &s, func() {
			var err error
			left := s.typedExprs[0]
			if left == nil {
				left, err = s.exprs[0].TypeCheck(ctx, types.Any)
				if err != nil {
					return
				}
			}
			right := s.typedExprs[1]
			if right == nil {
				right, err = s.exprs[1].TypeCheck(ctx, types.Any)
				if err != nil {
					return
				}
			}
			leftType := left.ResolvedType()
			rightType := right.ResolvedType()
			leftIsNull := leftType == types.Unknown
			rightIsNull := rightType == types.Unknown
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
						return o.params().getAt(0).Equivalent(leftType) &&
							o.params().getAt(1).Equivalent(rightType)
					})
			}
		}); ok {
			return s.typedExprs, fns, err
		}
	}

	// The final heuristic is to defer to preferred candidates, if available.
	if ok, fns, err := filterAttempt(ctx, &s, func() {
		s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs, func(o overloadImpl) bool {
			return o.preferred()
		})
	}); ok {
		return s.typedExprs, fns, err
	}

	if _, err := defaultTypeCheck(ctx, s, len(s.overloads) > 0); err != nil {
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
	ctx *SemaContext, s *typeCheckOverloadState, attempt func(),
) (bool, []overloadImpl, error) {
	before := s.overloadIdxs
	attempt()
	if len(s.overloadIdxs) == 1 {
		_, fns, _, err := checkReturn(ctx, *s)
		return true, fns, err
	}
	s.overloadIdxs = before
	return false, nil, nil
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
	ctx *SemaContext, s typeCheckOverloadState, errorOnPlaceholders bool,
) (typeCheckOverloadState, error) {
	for _, i := range s.constIdxs {
		typ, err := s.exprs[i].TypeCheck(ctx, types.Any)
		if err != nil {
			return s, errors.Wrap(err, "error type checking constant value")
		}
		s.typedExprs[i] = typ
	}
	for _, i := range s.placeholderIdxs {
		if errorOnPlaceholders {
			_, err := s.exprs[i].TypeCheck(ctx, types.Any)
			return s, err
		}
		// If we dont want to error on args, avoid type checking them without a desired type.
		s.typedExprs[i] = StripParens(s.exprs[i]).(*Placeholder)
	}
	return s, nil
}

// checkReturn checks the number of remaining overloaded function
// implementations.
// Returns true if we should stop overload resolution, and returning either
// 1. the chosen overload in a slice, or
// 2. nil,
// along with the typed arguments.
// This modifies values within s as scratch slices, but only in the case where
// it returns true, which signals to the calling function that it should
// immediately return, so any mutations to s are irrelevant.
func checkReturn(
	ctx *SemaContext, s typeCheckOverloadState,
) ([]TypedExpr, []overloadImpl, bool, error) {
	switch len(s.overloadIdxs) {
	case 0:
		var err error
		if s, err = defaultTypeCheck(ctx, s, false); err != nil {
			return s.typedExprs, nil, true, err
		}
		return s.typedExprs, nil, true, nil
	case 1:
		idx := s.overloadIdxs[0]
		o := s.overloads[idx]
		p := o.params()
		for _, i := range s.constIdxs {
			des := p.getAt(i)
			typ, err := s.exprs[i].TypeCheck(ctx, des)
			if err != nil {
				return s.typedExprs, nil, true, errors.Wrap(err, "error type checking constant value")
			} else if des != nil && !typ.ResolvedType().Equivalent(des) {
				panic(pgerror.NewErrorf(
					pgerror.CodeInternalError, "desired constant value type %s but set type %s", des, typ.ResolvedType()))
			}
			s.typedExprs[i] = typ
		}

		for _, i := range s.placeholderIdxs {
			des := p.getAt(i)
			typ, err := s.exprs[i].TypeCheck(ctx, des)
			if err != nil {
				return s.typedExprs, nil, true, err
			}
			s.typedExprs[i] = typ
		}
		return s.typedExprs, s.overloads[idx : idx+1], true, nil
	default:
		return nil, nil, false, nil
	}
}

func formatCandidates(prefix string, candidates []overloadImpl) string {
	var buf bytes.Buffer
	for _, candidate := range candidates {
		buf.WriteString(prefix)
		buf.WriteByte('(')
		params := candidate.params()
		tLen := params.Length()
		for i := 0; i < tLen; i++ {
			t := params.getAt(i)
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(t.String())
		}
		buf.WriteString(") -> ")
		buf.WriteString(returnTypeToFixedType(candidate.returnType()).String())
		if candidate.preferred() {
			buf.WriteString(" [preferred]")
		}
		buf.WriteByte('\n')
	}
	return buf.String()
}
