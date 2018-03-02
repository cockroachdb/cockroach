// Copyright 2018 The Cockroach Authors.
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

package xform

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// inferType derives the type of the given scalar expression. Depending upon
// the operator, the type may be fixed, or it may be dependent upon the
// operands. inferType is called during initial construction of the expression,
// so its logical properties are not yet available.
func inferType(ev ExprView) types.T {
	fn := typingFuncMap[ev.Operator()]
	if fn == nil {
		panic(fmt.Sprintf("type inference for %v is not yet implemented", ev.Operator()))
	}
	return fn(ev)
}

type typingFunc func(ev ExprView) types.T

// typingFuncMap is a lookup table from scalar operator type to a function
// which returns the data type of an instance of that operator.
var typingFuncMap [opt.NumOperators]typingFunc

func init() {
	typingFuncMap = [opt.NumOperators]typingFunc{
		opt.VariableOp:        typeVariable,
		opt.ConstOp:           typeAsTypedExpr,
		opt.PlaceholderOp:     typeAsTypedExpr,
		opt.UnsupportedExprOp: typeAsTypedExpr,
		opt.TupleOp:           typeAsTuple,
		opt.ProjectionsOp:     typeAsAny,
		opt.AggregationsOp:    typeAsAny,
		opt.ExistsOp:          typeAsBool,
		opt.FunctionOp:        typeFunction,
		opt.CoalesceOp:        typeCoalesce,
		opt.CastOp:            typeCast,
	}

	for _, op := range opt.BooleanOperators {
		typingFuncMap[op] = typeAsBool
	}

	for _, op := range opt.ComparisonOperators {
		typingFuncMap[op] = typeAsBool
	}

	for _, op := range opt.BinaryOperators {
		typingFuncMap[op] = typeAsBinary
	}

	for _, op := range opt.UnaryOperators {
		typingFuncMap[op] = typeAsUnary
	}
}

// typeVariable returns the type of a variable expression, which is stored in
// the query metadata and acessed by column index.
func typeVariable(ev ExprView) types.T {
	colIndex := ev.Private().(opt.ColumnIndex)
	typ := ev.Metadata().ColumnType(colIndex)
	if typ == nil {
		panic(fmt.Sprintf("column %d does not have type", colIndex))
	}
	return typ
}

// typeAsBool returns the fixed boolean type.
func typeAsBool(_ ExprView) types.T {
	return types.Bool
}

// typeAsTuple constructs a tuple type that is composed of the types of all the
// expression's children.
func typeAsTuple(ev ExprView) types.T {
	types := make(types.TTuple, ev.ChildCount())
	for i := 0; i < ev.ChildCount(); i++ {
		child := ev.Child(i)
		types[i] = child.Logical().Scalar.Type
	}
	return types
}

// typeAsTypedExpr returns the resolved type of the private field, with the
// assumption that it is a tree.TypedExpr.
func typeAsTypedExpr(ev ExprView) types.T {
	return ev.Private().(tree.TypedExpr).ResolvedType()
}

// typeAsUnary returns the type of a unary expression by hooking into the sql
// semantics code that searches for unary operator overloads.
func typeAsUnary(ev ExprView) types.T {
	unaryOp := opt.UnaryOpReverseMap[ev.Operator()]

	input := ev.Child(0)
	inputType := input.Logical().Scalar.Type

	// Find the unary op that matches the type of the expression's child.
	for _, op := range tree.UnaryOps[unaryOp] {
		o := op.(tree.UnaryOp)
		if inputType.Equivalent(o.Typ) {
			return o.ReturnType
		}
	}

	panic(fmt.Sprintf("could not find type for unary expression: %v", ev))
}

// typeAsBinary returns the type of a binary expression by hooking into the sql
// semantics code that searches for binary operator overloads.
func typeAsBinary(ev ExprView) types.T {
	binOp := opt.BinaryOpReverseMap[ev.Operator()]

	left := ev.Child(0)
	leftType := left.Logical().Scalar.Type

	right := ev.Child(1)
	rightType := right.Logical().Scalar.Type

	// Find the binary op that matches the type of the expression's left and
	// right children.
	for _, op := range tree.BinOps[binOp] {
		o := op.(tree.BinOp)

		if leftType == types.Unknown {
			if rightType.Equivalent(o.RightType) {
				return o.ReturnType
			}
		} else if rightType == types.Unknown {
			if leftType.Equivalent(o.LeftType) {
				return o.ReturnType
			}
		} else {
			if leftType.Equivalent(o.LeftType) && rightType.Equivalent(o.RightType) {
				return o.ReturnType
			}
		}
	}

	panic(fmt.Sprintf("could not find type for binary expression: %v", ev))
}

// typeFunction returns the type of a function expression by extracting it from
// the function's private field, which is an instance of opt.FuncDef.
func typeFunction(ev ExprView) types.T {
	return ev.Private().(opt.FuncDef).Type
}

// typeAsAny returns types.Any for an operator that never has its type used.
// This avoids wasting time constructing a type that's not not needed. For
// example, the Projections scalar operator holds a list of expressions on
// behalf of the Project operator, but its type is never used.
func typeAsAny(_ ExprView) types.T {
	return types.Any
}

// typeCoalesce returns the type of a coalesce expression, which is equal to
// the type of its children.
func typeCoalesce(ev ExprView) types.T {
	for i := 0; i < ev.ChildCount(); i++ {
		childType := ev.Child(i).Logical().Scalar.Type
		if childType != types.Unknown {
			return childType
		}
	}
	return types.Unknown
}

func typeCast(ev ExprView) types.T {
	return ev.Private().(types.T)
}
