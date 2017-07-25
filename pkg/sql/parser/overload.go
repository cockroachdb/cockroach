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

package parser

import (
	"bytes"
	"fmt"
	"math"
)

// overloadImpl is an implementation of an overloaded function. It provides
// access to the parameter type list  and the return type of the implementation.
type overloadImpl interface {
	params() typeList
	returnType() returnTyper
	// allows manually resolving preference between multiple compatible overloads
	preferred() bool
}

// typeList is a list of types representing a function parameter list.
type typeList interface {
	// match checks if all types in the typeList match the corresponding elements in types.
	match(types []Type) bool
	// matchAt checks if the parameter type at index i of the typeList matches type typ.
	// In all implementations, TypeNull will match with each parameter type, allowing
	// NULL values to be used as arguments.
	matchAt(typ Type, i int) bool
	// matchLen checks that the typeList can support l parameters.
	matchLen(l int) bool
	// getAt returns the type at the given index in the typeList, or nil if the typeList
	// cannot have a parameter at index i.
	getAt(i int) Type
	// Length returns the number of types in the list
	Length() int
	// Types returns a realized copy of the list. variadic lists return a list of size one.
	Types() []Type
	// String returns a human readable signature
	String() string
}

var _ typeList = ArgTypes{}
var _ typeList = HomogeneousType{}
var _ typeList = VariadicType{}

// ArgTypes is very similar to ArgTypes except it allows keeping a string
// name for each argument as well and using those when printing the
// human-readable signature.
type ArgTypes []struct {
	Name string
	Typ  Type
}

func (a ArgTypes) match(types []Type) bool {
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

func (a ArgTypes) matchAt(typ Type, i int) bool {
	// The parameterized types for Tuples are checked in the type checking
	// routines before getting here, so we only need to check if the argument
	// type is a TypeTuple below. This allows us to avoid defining overloads
	// for TypeTuple{}, TypeTuple{TypeAny}, TypeTuple{TypeAny, TypeAny}, etc.
	// for Tuple operators.
	if typ.FamilyEqual(TypeTuple) {
		typ = TypeTuple
	}
	return i < len(a) && (typ == TypeNull || a[i].Typ.Equivalent(typ))
}

func (a ArgTypes) matchLen(l int) bool {
	return len(a) == l
}

func (a ArgTypes) getAt(i int) Type {
	return a[i].Typ
}

// Length implements the typeList interface.
func (a ArgTypes) Length() int {
	return len(a)
}

// Types implements the typeList interface.
func (a ArgTypes) Types() []Type {
	n := len(a)
	ret := make([]Type, n)
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

// HomogeneousType is a typeList implementation that accepts any arguments, as
// long as all are the same type or NULL. The homogeneous constraint is enforced
// in typeCheckOverloadedExprs.
type HomogeneousType struct{}

func (HomogeneousType) match(types []Type) bool {
	return true
}

func (HomogeneousType) matchAt(typ Type, i int) bool {
	return true
}

func (HomogeneousType) matchLen(l int) bool {
	return true
}

func (HomogeneousType) getAt(i int) Type {
	return TypeAny
}

// Length implements the typeList interface.
func (HomogeneousType) Length() int {
	return 1
}

// Types implements the typeList interface.
func (HomogeneousType) Types() []Type {
	return []Type{TypeAny}
}

func (HomogeneousType) String() string {
	return "anyelement..."
}

// VariadicType is a typeList implementation which accepts any number of
// arguments and matches when each argument is either NULL or of the type
// typ.
type VariadicType struct {
	Typ Type
}

func (v VariadicType) match(types []Type) bool {
	for i := range types {
		if !v.matchAt(types[i], i) {
			return false
		}
	}
	return true
}

func (v VariadicType) matchAt(typ Type, i int) bool {
	return typ == TypeNull || v.Typ.Equivalent(typ)
}

func (v VariadicType) matchLen(l int) bool {
	return true
}

func (v VariadicType) getAt(i int) Type {
	return v.Typ
}

// Length implements the typeList interface.
func (v VariadicType) Length() int {
	return 1
}

// Types implements the typeList interface.
func (v VariadicType) Types() []Type {
	return []Type{v.Typ}
}

func (v VariadicType) String() string {
	return fmt.Sprintf("%s...", v.Typ)
}

// unknownReturnType is returned from returnTypers when the arguments provided are
// not sufficient to determine a return type. This is necessary for cases like overload
// resolution, where the argument types are not resolved yet so the type-level function
// will be called without argument types. If a returnTyper returns unknownReturnType,
// then the candidate function set cannot be refined. This means that only returnTypers
// that never return unknownReturnType, like those created with fixedReturnType, can
// help reduce overload ambiguity.
var unknownReturnType Type

// returnTyper defines the type-level function in which a builtin function's return type
// is determined. returnTypers should make sure to return unknownReturnType when necessary.
type returnTyper func(args []TypedExpr) Type

// fixedReturnType functions simply return a fixed type, independent of argument types.
func fixedReturnType(typ Type) returnTyper {
	return func(args []TypedExpr) Type { return typ }
}

// identityReturnType creates a returnType that is a projection of the idx'th
// argument type.
func identityReturnType(idx int) returnTyper {
	return func(args []TypedExpr) Type {
		if len(args) == 0 {
			return unknownReturnType
		}
		return args[idx].ResolvedType()
	}
}

func returnTypeToFixedType(s returnTyper) Type {
	if t := s(nil); t != unknownReturnType {
		return t
	}
	return TypeAny
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
	ctx *SemaContext, desired Type, overloads []overloadImpl, inBinOp bool, exprs ...Expr,
) ([]TypedExpr, []overloadImpl, error) {
	if len(overloads) > math.MaxUint8 {
		return nil, nil, fmt.Errorf("too many overloads (%d > 255)", len(overloads))
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
			typedExprs, _, err := typeCheckSameTypedExprs(ctx, desired, exprs...)
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
			typ, err := exprs[i].TypeCheck(ctx, TypeAny)
			if err != nil {
				return nil, nil, fmt.Errorf("error type checking resolved expression: %v", err)
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
		paramDesired := TypeAny
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
	if desired != TypeAny {
		s.overloadIdxs = filterOverloads(s.overloads, s.overloadIdxs,
			func(o overloadImpl) bool {
				// For now, we only filter on the return type for overloads with
				// fixed return types. This could be improved, but is not currently
				// critical because we have no cases of functions with multiple
				// overloads that do not all expose fixedReturnTypes.
				if t := o.returnType()(nil); t != unknownReturnType {
					return t.Equivalent(desired)
				}
				return true
			})
		if types, fns, ok, err := checkReturn(ctx, s); ok {
			return types, fns, err
		}
	}

	var homogeneousTyp Type
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
			// The second heuristic is to prefer candidates where all numeric constants can become
			// a homogeneous type, if all resolvable expressions became one. This is only possible
			// resolvable expressions were resolved homogeneously up to this point.
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
			// The third heuristic is to prefer candidates where all numeric constants can become
			// their "natural"" types.
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

		// The fourth heuristic is to prefer candidates that accepts the "best" mutual
		// type in the resolvable type set of all numeric constants.
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

	// The fifth heuristic is to prefer candidates where all placeholders can be given the same type
	// as all numeric constants and resolvable expressions. This is only possible if all numeric
	// constants and resolvable expressions were resolved homogeneously up to this point.
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
	// other argument. This is used to differentiate the behaviour of
	// STRING[] || NULL and STRING || NULL.
	if inBinOp && len(s.exprs) == 2 {
		if ok, fns, err := filterAttempt(ctx, &s, func() {
			var err error
			left := s.typedExprs[0]
			if left == nil {
				left, err = s.exprs[0].TypeCheck(ctx, TypeAny)
				if err != nil {
					return
				}
			}
			right := s.typedExprs[1]
			if right == nil {
				right, err = s.exprs[1].TypeCheck(ctx, TypeAny)
				if err != nil {
					return
				}
			}
			leftType := left.ResolvedType()
			rightType := right.ResolvedType()
			leftIsNull := leftType == TypeNull
			rightIsNull := rightType == TypeNull
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
		typ, err := s.exprs[i].TypeCheck(ctx, TypeAny)
		if err != nil {
			return s, fmt.Errorf("error type checking constant value: %v", err)
		}
		s.typedExprs[i] = typ
	}
	for _, i := range s.placeholderIdxs {
		if errorOnPlaceholders {
			_, err := s.exprs[i].TypeCheck(ctx, TypeAny)
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
				return s.typedExprs, nil, true, fmt.Errorf("error type checking constant value: %v", err)
			} else if des != nil && !typ.ResolvedType().Equivalent(des) {
				panic(fmt.Errorf("desired constant value type %s but set type %s", des, typ.ResolvedType()))
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
