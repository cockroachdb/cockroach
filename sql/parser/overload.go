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
//
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package parser

import "fmt"

// overloadImpl is an implementation of an overloaded function. It provides
// access to the parameter type list  and the return type of the implementation.
type overloadImpl interface {
	params() typeList
	returnType() Datum
}

// typeList is a list of types representing a function parameter list.
type typeList interface {
	// match checks if all types in the typeList match the corresponding elements in types.
	match(types ArgTypes) bool
	// matchAt checks if the parameter type at index i of the typeList matches type typ.
	matchAt(typ Datum, i int) bool
	// matchLen checks that the typeList can support l parameters.
	matchLen(l int) bool
	// getAt returns the type at the given index in the typeList, or nil if the typeList
	// cannot have a parameter at index i.
	getAt(i int) Datum
}

var _ typeList = ArgTypes{}
var _ typeList = AnyType{}
var _ typeList = VariadicType{}
var _ typeList = SingleType{}

// ArgTypes is a typeList implementation that accepts a specific number of
// argument types.
type ArgTypes []Datum

func (a ArgTypes) match(types ArgTypes) bool {
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

func (a ArgTypes) matchAt(typ Datum, i int) bool {
	if i >= len(a) {
		return false
	}
	if _, ok := typ.(*DTuple); ok {
		typ = TypeTuple
	}
	return a[i].TypeEqual(typ)
}

func (a ArgTypes) matchLen(l int) bool {
	return len(a) == l
}

func (a ArgTypes) getAt(i int) Datum {
	return a[i]
}

// AnyType is a typeList implementation that accepts any arguments.
type AnyType struct{}

func (AnyType) match(types ArgTypes) bool {
	return true
}

func (AnyType) matchAt(typ Datum, i int) bool {
	return true
}

func (AnyType) matchLen(l int) bool {
	return true
}

func (AnyType) getAt(i int) Datum {
	panic("getAt called on AnyType")
}

// VariadicType is a typeList implementation which accepts any number of
// arguments and matches when each argument is either NULL or of the type
// typ.
type VariadicType struct {
	Typ Datum
}

func (v VariadicType) match(types ArgTypes) bool {
	for i := range types {
		if !v.matchAt(types[i], i) {
			return false
		}
	}
	return true
}

func (v VariadicType) matchAt(typ Datum, i int) bool {
	return typ == DNull || typ.TypeEqual(v.Typ)
}

func (v VariadicType) matchLen(l int) bool {
	return true
}

func (v VariadicType) getAt(i int) Datum {
	return v.Typ
}

// SingleType is a typeList implementation which accepts a single
// argument of type typ. It is logically identical to an ArgTypes
// implementation with length 1, but avoids the slice allocation.
type SingleType struct {
	Typ Datum
}

func (s SingleType) match(types ArgTypes) bool {
	if len(types) != 1 {
		return false
	}
	return s.matchAt(types[0], 0)
}

func (s SingleType) matchAt(typ Datum, i int) bool {
	if i != 0 {
		return false
	}
	return typ.TypeEqual(s.Typ)
}

func (s SingleType) matchLen(l int) bool {
	return l == 1
}

func (s SingleType) getAt(i int) Datum {
	if i != 0 {
		return nil
	}
	return s.Typ
}

// typeCheckOverloadedExprs determines the correct overload to use for the given set of
// expression parameters, along with an optional desired return type. It returns the expression
// parameters after being type checked, along with the chosen overloadImpl. If an overloaded
// function implementation could not be determined, the overloadImpl return value will be nil.
func typeCheckOverloadedExprs(
	ctx *SemaContext, desired Datum, overloads []overloadImpl, exprs ...Expr,
) ([]TypedExpr, overloadImpl, error) {
	// Special-case the AnyType overload. We determine its return type by checking that
	// all parameters have the same type.
	for _, overload := range overloads {
		// Only one overload can be provided if it has parameters with AnyType.
		if _, ok := overload.params().(AnyType); ok {
			if len(overloads) > 1 {
				return nil, nil, fmt.Errorf("only one overload can have parameters with AnyType")
			}
			typedExprs, _, err := typeCheckSameTypedExprs(ctx, desired, exprs...)
			if err != nil {
				return nil, nil, err
			}
			return typedExprs, overload, nil
		}
	}

	// Hold the resolved type expressions of the provided exprs, in order.
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

	// defaultTypeCheck type checks the constant and placeholder expressions without a preference
	// and adds them to the type checked slice.
	defaultTypeCheck := func(errorOnPlaceholders bool) error {
		for _, expr := range constExprs {
			typ, err := expr.e.TypeCheck(ctx, nil)
			if err != nil {
				return fmt.Errorf("error type checking constant value: %v", err)
			}
			typedExprs[expr.i] = typ
		}
		for _, expr := range placeholderExprs {
			if errorOnPlaceholders {
				_, err := expr.e.TypeCheck(ctx, nil)
				return err
			}
			// If we dont want to error on args, avoid type checking them without a desired type.
			typedExprs[expr.i] = &DPlaceholder{
				name: StripParens(expr.e).(Placeholder).Name,
				pmap: ctx.Placeholders,
			}
		}
		return nil
	}

	// If no overloads are provided, just type check parameters and return.
	if len(overloads) == 0 {
		for _, expr := range resolvableExprs {
			typ, err := expr.e.TypeCheck(ctx, nil)
			if err != nil {
				return nil, nil, fmt.Errorf("error type checking resolved expression: %v", err)
			}
			typedExprs[expr.i] = typ
		}
		if err := defaultTypeCheck(false); err != nil {
			return nil, nil, err
		}
		return typedExprs, nil, nil
	}

	// Function to filter overloads which return false from the provided closure.
	filterOverloads := func(fn func(overloadImpl) bool) {
		for i := 0; i < len(overloads); {
			if fn(overloads[i]) {
				i++
			} else {
				overloads[i], overloads[len(overloads)-1] = overloads[len(overloads)-1], overloads[i]
				overloads = overloads[:len(overloads)-1]
			}
		}
	}

	// Filter out incorrect parameter length overloads.
	filterOverloads(func(o overloadImpl) bool {
		return o.params().matchLen(len(exprs))
	})

	// Filter out overloads which constants cannot become.
	for _, expr := range constExprs {
		constExpr := expr.e.(Constant)
		filterOverloads(func(o overloadImpl) bool {
			return canConstantBecome(constExpr, o.params().getAt(expr.i))
		})
	}

	// TODO(nvanbenschoten) We should add a filtering step here to filter
	// out impossible candidates based on identical parameters. For instance,
	// f(int, float) is not a possible candidate for the expression f($1, $1).

	// Filter out overloads on resolved types.
	for _, expr := range resolvableExprs {
		paramDesired := NoTypePreference
		if len(overloads) == 1 {
			// Once we get down to a single overload candidate, begin desiring its
			// parameter types for the corresponding argument expressions.
			paramDesired = overloads[0].params().getAt(expr.i)
		}
		typ, err := expr.e.TypeCheck(ctx, paramDesired)
		if err != nil {
			return nil, nil, err
		}
		typedExprs[expr.i] = typ
		filterOverloads(func(o overloadImpl) bool {
			return o.params().matchAt(typ.ReturnType(), expr.i)
		})
	}

	// checkReturn checks the number of remaining overloaded function implementations, returning
	// if we should stop overload resolution, along with a nullable overloadImpl to return if
	// we should stop overload resolution.
	checkReturn := func() (bool, overloadImpl, error) {
		switch len(overloads) {
		case 0:
			if err := defaultTypeCheck(false); err != nil {
				return true, nil, err
			}
			return true, nil, nil
		case 1:
			o := overloads[0]
			p := o.params()
			for _, expr := range constExprs {
				des := p.getAt(expr.i)
				typ, err := expr.e.TypeCheck(ctx, des)
				if err != nil {
					return true, nil, fmt.Errorf("error type checking constant value: %v", err)
				} else if des != nil && !typ.ReturnType().TypeEqual(des) {
					panic(fmt.Errorf("desired constant value type %s but set type %s",
						des.Type(), typ.ReturnType().Type()))
				}
				typedExprs[expr.i] = typ
			}

			for _, expr := range placeholderExprs {
				des := p.getAt(expr.i)
				typ, err := expr.e.TypeCheck(ctx, des)
				if err != nil {
					return true, nil, err
				}
				typedExprs[expr.i] = typ
			}
			return true, o, nil
		default:
			return false, nil, nil
		}
	}
	// At this point, all remaining overload candidates accept the argument list,
	// so we begin checking for a single remaining candidate implementation to choose.
	// In case there is more than one candidate remaining, the following code uses
	// heuristics to find a most preferable candidate.
	if ok, fn, err := checkReturn(); ok {
		return typedExprs, fn, err
	}

	// The first heuristic is to prefer candidates that return the desired type.
	if desired != nil {
		filterOverloads(func(o overloadImpl) bool {
			return o.returnType().TypeEqual(desired)
		})
		if ok, fn, err := checkReturn(); ok {
			return typedExprs, fn, err
		}
	}

	var homogeneousTyp Datum
	if len(resolvableExprs) > 0 {
		homogeneousTyp = typedExprs[resolvableExprs[0].i].ReturnType()
		for _, resExprs := range resolvableExprs[1:] {
			if !homogeneousTyp.TypeEqual(typedExprs[resExprs.i].ReturnType()) {
				homogeneousTyp = nil
				break
			}
		}
	}

	var bestConstType Datum
	if len(constExprs) > 0 {
		before := overloads

		// The second heuristic is to prefer candidates where all numeric constants can become
		// a homogeneous type, if all resolvable expressions became one. This is only possible
		// resolvable expressions were resolved homogeneously up to this point.
		if homogeneousTyp != nil {
			all := true
			for _, expr := range constExprs {
				if !canConstantBecome(expr.e.(Constant), homogeneousTyp) {
					all = false
					break
				}
			}
			if all {
				for _, expr := range constExprs {
					filterOverloads(func(o overloadImpl) bool {
						return o.params().getAt(expr.i).TypeEqual(homogeneousTyp)
					})
				}
			}
		}
		if len(overloads) == 1 {
			if ok, fn, err := checkReturn(); ok {
				return typedExprs, fn, err
			}
		}
		// Restore the expressions if this did not work.
		overloads = before

		// The third heuristic is to prefer candidates where all numeric constants can become
		// their "natural"" types.
		for _, expr := range constExprs {
			natural := naturalConstantType(expr.e.(Constant))
			if natural != nil {
				filterOverloads(func(o overloadImpl) bool {
					return o.params().getAt(expr.i).TypeEqual(natural)
				})
			}
		}
		if len(overloads) == 1 {
			if ok, fn, err := checkReturn(); ok {
				return typedExprs, fn, err
			}
		}
		// Restore the expressions if this did not work.
		overloads = before

		// The fourth heuristic is to prefer candidates that accepts the "best" mutual
		// type in the resolvable type set of all numeric constants.
		bestConstType = commonNumericConstantType(constExprs)
		for _, expr := range constExprs {
			filterOverloads(func(o overloadImpl) bool {
				return o.params().getAt(expr.i).TypeEqual(bestConstType)
			})
		}
		if ok, fn, err := checkReturn(); ok {
			return typedExprs, fn, err
		}
		if homogeneousTyp != nil {
			if !homogeneousTyp.TypeEqual(bestConstType) {
				homogeneousTyp = nil
			}
		} else {
			homogeneousTyp = bestConstType
		}
	}

	// The fifth heuristic is to prefer candidates where all placeholders can be given the same type
	// as all numeric constants and resolvable expressions. This is only possible if all numeric
	// constants and resolvable expressions were resolved homogeneously up to this point.
	if homogeneousTyp != nil && len(placeholderExprs) > 0 {
		for _, expr := range placeholderExprs {
			filterOverloads(func(o overloadImpl) bool {
				return o.params().getAt(expr.i).TypeEqual(homogeneousTyp)
			})
		}
		if ok, fn, err := checkReturn(); ok {
			return typedExprs, fn, err
		}
	}

	if err := defaultTypeCheck(len(overloads) > 0); err != nil {
		return nil, nil, err
	}
	return typedExprs, nil, nil
}
