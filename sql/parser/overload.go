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
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package parser

import "fmt"

type overload interface {
	params() typeList
	returnType() Datum
}

type typeList interface {
	match(types ArgTypes) bool
	matchAt(typ Datum, i int) bool
	matchLen(l int) bool
	getAt(i int) Datum
}

// ArgTypes accepts a specific number of argument types.
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
		typ = dummyTuple
	}
	return a[i].TypeEqual(typ)
}

func (a ArgTypes) matchLen(l int) bool {
	return len(a) == l
}

func (a ArgTypes) getAt(i int) Datum {
	return a[i]
}

// AnyType accepts any arguments.
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

// VariadicType is an implementation of typeList which matches when
// each argument is either NULL or of the type typ.
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

// SingleType ...
type SingleType struct {
	Typ Datum
}

func (s SingleType) match(types ArgTypes) bool {
	for i := range types {
		if !s.matchAt(types[i], i) {
			return false
		}
	}
	return true
}

func (s SingleType) matchAt(typ Datum, i int) bool {
	if i != 0 {
		return false
	}
	return typ == DNull || typ.TypeEqual(s.Typ)
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
// expression parameters, along with an optional desired return type. The order of presidence
// in chosing the correct overload is as follows:
// 1. overloads are filtered based on argument length
// 2. overloads are filtered based on the types of resolved types
// 3. overloads are filtered based on the possible types of constants
// - if only one overload is left here, select it
// 4. if all resolved parameters are homogeneous and all constants can become this type, filter
// - if only one overload is left here, select it
// 5. overloads are filtered based on the desired return type, if one is provided
// - if only one overload is left here, select it
// 6. overloads are filtered based on the "best" homogeneous constant type
// - if only one overload is left here, select it
// 7. if all other parameters are homogeneous, overloads are filtered based on the type for ValArgs
// - if only one overload is left here, select it
// - else we can not determine the desired overload
func typeCheckOverloadedExprs(args MapArgs, desired Datum, overloads []overload, exprs ...Expr) ([]TypedExpr, overload, error) {
	// Special-case the AnyType overload. We determine it's return type be checking that
	// all parameters have the same type.
	for _, overload := range overloads {
		// Only one overload can be provided if it has parameters with AnyType.
		if _, ok := overload.params().(AnyType); ok {
			if len(overloads) > 1 {
				return nil, nil, fmt.Errorf("only one overload can have parameters with AnyType")
			}
			typedExprs, _, err := typeCheckSameTypedExprs(args, desired, exprs...)
			if err != nil {
				return nil, nil, err
			}
			return typedExprs, overload, nil
		}
	}

	// Hold the resolved type expressions of the provided exprs, in order.
	typedExprs := make([]TypedExpr, len(exprs))

	var resolvedExprs, constExprs, valExprs []indexedExpr
	for i, expr := range exprs {
		idxExpr := indexedExpr{e: expr, i: i}
		switch {
		case isNumericConstant(expr):
			constExprs = append(constExprs, idxExpr)
		case isUnresolvedVariable(args, expr):
			valExprs = append(valExprs, idxExpr)
		default:
			resolvedExprs = append(resolvedExprs, idxExpr)
		}
	}

	// defaultTypeCheck type checks the constant and valArg expressions without a preference
	// and adds them to the type checked slice.
	defaultTypeCheck := func() error {
		for _, expr := range constExprs {
			typ, err := expr.e.TypeCheck(args, nil)
			if err != nil {
				return fmt.Errorf("error type checking constant value: %v", err)
			}
			typedExprs[expr.i] = typ
		}
		for _, expr := range valExprs {
			// TODO(nvanbenschoten) MORTY
			typedExprs[expr.i] = &DValArg{name: expr.e.(ValArg).name}
		}
		return nil
	}

	// If no overloads are provided, just type check parameters and return.
	if len(overloads) == 0 {
		for _, expr := range resolvedExprs {
			typ, err := expr.e.TypeCheck(args, nil)
			if err != nil {
				return nil, nil, fmt.Errorf("error type checking resolved expression: %v", err)
			}
			typedExprs[expr.i] = typ
		}
		if err := defaultTypeCheck(); err != nil {
			return nil, nil, err
		}
		return typedExprs, nil, nil
	}

	// Function to filter overloads which return false from the provided closure.
	filterOverloads := func(fn func(overload) bool) {
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
	filterOverloads(func(o overload) bool {
		return o.params().matchLen(len(exprs))
	})

	// Filter out overloads on resolved types.
	for _, expr := range resolvedExprs {
		var paramDesired Datum
		if len(overloads) == 1 {
			paramDesired = overloads[0].params().getAt(expr.i)
		}
		typ, err := expr.e.TypeCheck(args, paramDesired)
		if err != nil {
			return nil, nil, err
		}
		typedExprs[expr.i] = typ
		filterOverloads(func(o overload) bool {
			return o.params().matchAt(typ.ReturnType(), expr.i)
		})
	}

	// Filter out overloads which constants cannot become.
	for _, expr := range constExprs {
		constExpr := expr.e.(*NumVal)
		filterOverloads(func(o overload) bool {
			return canConstantBecome(constExpr, o.params().getAt(expr.i))
		})
	}

	checkReturn := func() (bool, overload, error) {
		switch len(overloads) {
		case 0:
			if err := defaultTypeCheck(); err != nil {
				return true, nil, err
			}
			return true, nil, nil
		case 1:
			o := overloads[0]
			p := o.params()
			for _, expr := range constExprs {
				des := p.getAt(expr.i)
				typ, err := expr.e.TypeCheck(args, des)
				if err != nil {
					return true, nil, fmt.Errorf("error type checking constant value: %v", err)
				} else if des != nil && !typ.ReturnType().TypeEqual(des) {
					panic(fmt.Errorf("desired constant value type %s but set type %s", des.Type(), typ.ReturnType().Type()))
				}
				typedExprs[expr.i] = typ
			}

			for _, expr := range valExprs {
				typ := p.getAt(expr.i)
				if _, err := expr.e.TypeCheck(args, typ); err != nil {
					return true, nil, err
				}
				typedExprs[expr.i] = typ
			}
			return true, o, nil
		default:
			return false, nil, nil
		}
	}
	if ok, fn, err := checkReturn(); ok {
		return typedExprs, fn, err
	}

	// Filter out overloads which return the desired type.
	if desired != nil {
		filterOverloads(func(o overload) bool {
			return o.returnType().TypeEqual(desired)
		})
		if ok, fn, err := checkReturn(); ok {
			return typedExprs, fn, err
		}
	}

	var homogeneousTyp Datum
	if len(resolvedExprs) > 0 {
		homogeneousTyp = typedExprs[resolvedExprs[0].i].ReturnType()
		for _, resExprs := range resolvedExprs[1:] {
			if !homogeneousTyp.TypeEqual(typedExprs[resExprs.i].ReturnType()) {
				homogeneousTyp = nil
				break
			}
		}
	}

	var bestConstType Datum
	if len(constExprs) > 0 {
		numVals := make([]*NumVal, len(constExprs))
		for i, expr := range constExprs {
			numVals[i] = expr.e.(*NumVal)
		}
		before := overloads

		// Check if all constants can become the homogeneous type.
		if homogeneousTyp != nil {
			all := true
			for _, constExpr := range numVals {
				if !canConstantBecome(constExpr, homogeneousTyp) {
					all = false
					break
				}
			}
			if all {
				for _, expr := range constExprs {
					filterOverloads(func(o overload) bool {
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

		// Check if an overload fits with the natural constant types.
		for i, expr := range constExprs {
			natural := naturalConstantType(numVals[i])
			if natural != nil {
				filterOverloads(func(o overload) bool {
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

		// Check if an overload fits with the "best" mutual constant types.
		bestConstType = commonNumericConstantType(numVals...)
		for _, expr := range constExprs {
			filterOverloads(func(o overload) bool {
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

	// If all other parameters are homogeneous, we favor this type for ValArgs.
	if homogeneousTyp != nil && len(valExprs) > 0 {
		for _, expr := range valExprs {
			filterOverloads(func(o overload) bool {
				return o.params().getAt(expr.i).TypeEqual(homogeneousTyp)
			})
		}
		if ok, fn, err := checkReturn(); ok {
			return typedExprs, fn, err
		}
	}

	if err := defaultTypeCheck(); err != nil {
		return nil, nil, err
	}
	return typedExprs, nil, nil
}
