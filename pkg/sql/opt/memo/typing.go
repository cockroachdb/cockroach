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

package memo

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// InferType derives the type of the given scalar expression. Depending upon
// the operator, the type may be fixed, or it may be dependent upon the
// operands. InferType is called during initial construction of the expression,
// so its logical properties are not yet available.
func InferType(ev ExprView) types.T {
	fn := typingFuncMap[ev.Operator()]
	if fn == nil {
		panic(fmt.Sprintf("type inference for %v is not yet implemented", ev.Operator()))
	}
	return fn(ev)
}

// InferUnaryType infers the return type of a unary operator, given the type of
// its input.
func InferUnaryType(op opt.Operator, inputType types.T) types.T {
	unaryOp := opt.UnaryOpReverseMap[op]

	// Find the unary op that matches the type of the expression's child.
	for _, op := range tree.UnaryOps[unaryOp] {
		o := op.(tree.UnaryOp)
		if inputType.Equivalent(o.Typ) {
			return o.ReturnType
		}
	}
	panic(fmt.Sprintf("could not find type for unary expression %s", op))
}

// InferBinaryType infers the return type of a binary operator, given the type
// of its inputs.
func InferBinaryType(op opt.Operator, leftType, rightType types.T) types.T {
	o, ok := findBinaryOverload(op, leftType, rightType)
	if !ok {
		panic(fmt.Sprintf("could not find type for binary expression %s", op))
	}
	return o.returnType
}

// BinaryOverloadExists returns true if the given binary operator exists with the
// given arguments.
func BinaryOverloadExists(op opt.Operator, leftType, rightType types.T) bool {
	_, ok := findBinaryOverload(op, leftType, rightType)
	return ok
}

// BinaryAllowsNullArgs returns true if the given binary operator allows null
// arguments, and cannot therefore be folded away to null.
func BinaryAllowsNullArgs(op opt.Operator, leftType, rightType types.T) bool {
	o, ok := findBinaryOverload(op, leftType, rightType)
	if !ok {
		panic(fmt.Sprintf("could not find overload for binary expression %s", op))
	}
	return o.allowNullArgs
}

// FindAggregateOverload finds an aggregate function overload that matches the
// given aggregate function expression. It panics if no match can be found.
func FindAggregateOverload(ev ExprView) (name string, overload *tree.Overload) {
	name = opt.AggregateOpReverseMap[ev.Operator()]
	_, overloads := builtins.GetBuiltinProperties(name)
	for o := range overloads {
		overload = &overloads[o]
		matches := true
		for i := 0; i < ev.ChildCount(); i++ {
			typ := ev.Child(i).Logical().Scalar.Type
			if !overload.Types.MatchAt(typ, i) {
				matches = false
				break
			}
		}
		if matches {
			return name, overload
		}
	}
	panic(fmt.Sprintf("could not find overload for %s aggregate", name))
}

type typingFunc func(ev ExprView) types.T

// typingFuncMap is a lookup table from scalar operator type to a function
// which returns the data type of an instance of that operator.
var typingFuncMap [opt.NumOperators]typingFunc

func init() {
	typingFuncMap = [opt.NumOperators]typingFunc{
		opt.VariableOp:        typeVariable,
		opt.ConstOp:           typeAsTypedExpr,
		opt.NullOp:            typeAsPrivate,
		opt.PlaceholderOp:     typeAsTypedExpr,
		opt.UnsupportedExprOp: typeAsTypedExpr,
		opt.TupleOp:           typeAsPrivate,
		opt.ProjectionsOp:     typeAsAny,
		opt.AggregationsOp:    typeAsAny,
		opt.MergeOnOp:         typeAsAny,
		opt.ExistsOp:          typeAsBool,
		opt.AnyOp:             typeAsBool,
		opt.FunctionOp:        typeFunction,
		opt.CoalesceOp:        typeCoalesce,
		opt.CaseOp:            typeCase,
		opt.WhenOp:            typeWhen,
		opt.CastOp:            typeCast,
		opt.SubqueryOp:        typeSubquery,
		opt.ArrayOp:           typeAsPrivate,
		opt.ColumnAccessOp:    typeColumnAccess,

		// Override default typeAsAggregate behavior for aggregate functions with
		// a large number of possible overloads or where ReturnType depends on
		// argument types.
		opt.ArrayAggOp:   typeArrayAgg,
		opt.MaxOp:        typeAsFirstArg,
		opt.MinOp:        typeAsFirstArg,
		opt.AnyNotNullOp: typeAsFirstArg,
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

	for _, op := range opt.AggregateOperators {
		// Fill in any that are not already added to the typingFuncMap above.
		if typingFuncMap[op] == nil {
			typingFuncMap[op] = typeAsAggregate
		}
	}
}

// typeVariable returns the type of a variable expression, which is stored in
// the query metadata and accessed by column id.
func typeVariable(ev ExprView) types.T {
	colID := ev.Private().(opt.ColumnID)
	typ := ev.Metadata().ColumnType(colID)
	if typ == nil {
		panic(fmt.Sprintf("column %d does not have type", colID))
	}
	return typ
}

// typeArrayAgg returns an array type with element type equal to the type of the
// aggregate expression's first (and only) argument.
func typeArrayAgg(ev ExprView) types.T {
	typ := ev.Child(0).Logical().Scalar.Type
	return types.TArray{Typ: typ}
}

// typeAsBool returns the fixed boolean type.
func typeAsBool(_ ExprView) types.T {
	return types.Bool
}

// typeAsFirstArg returns the type of the expression's 0th argument.
func typeAsFirstArg(ev ExprView) types.T {
	return ev.Child(0).Logical().Scalar.Type
}

// typeAsTypedExpr returns the resolved type of the private field, with the
// assumption that it is a tree.TypedExpr.
func typeAsTypedExpr(ev ExprView) types.T {
	return ev.Private().(tree.TypedExpr).ResolvedType()
}

// typeAsUnary returns the type of a unary expression by hooking into the sql
// semantics code that searches for unary operator overloads.
func typeAsUnary(ev ExprView) types.T {
	return InferUnaryType(ev.Operator(), ev.Child(0).Logical().Scalar.Type)
}

// typeAsBinary returns the type of a binary expression by hooking into the sql
// semantics code that searches for binary operator overloads.
func typeAsBinary(ev ExprView) types.T {
	leftType := ev.Child(0).Logical().Scalar.Type
	rightType := ev.Child(1).Logical().Scalar.Type
	return InferBinaryType(ev.Operator(), leftType, rightType)
}

// typeFunction returns the type of a function expression by extracting it from
// the function's private field, which is an instance of *opt.FuncOpDef.
func typeFunction(ev ExprView) types.T {
	return ev.Private().(*FuncOpDef).Type
}

// typeAsAggregate returns the type of an aggregate expression by hooking into
// the sql semantics code that searches for aggregate operator overloads.
func typeAsAggregate(ev ExprView) types.T {
	// Only handle cases where the return type is not dependent on argument
	// types (i.e. pass nil to the ReturnTyper). Aggregates with return types
	// that depend on argument types are handled separately.
	_, overload := FindAggregateOverload(ev)
	t := overload.ReturnType(nil)
	if t == tree.UnknownReturnType {
		panic("unknown aggregate return type")
	}
	return t
}

// typeAsAny returns types.Any for an operator that never has its type used.
// This avoids wasting time constructing a type that's not not needed. For
// example, the Projections scalar operator holds a list of expressions on
// behalf of the Project operator, but its type is never used.
func typeAsAny(_ ExprView) types.T {
	return types.Any
}

// typeCoalesce returns the type of a coalesce expression, which is equal to
// the type of its first non-null child.
func typeCoalesce(ev ExprView) types.T {
	for i := 0; i < ev.ChildCount(); i++ {
		childType := ev.Child(i).Logical().Scalar.Type
		if childType != types.Unknown {
			return childType
		}
	}
	return types.Unknown
}

// typeCase returns the type of a CASE expression, which is
// of the form:
//   CASE [ <cond> ]
//       WHEN <condval1> THEN <expr1>
//     [ WHEN <condval2> THEN <expr2> ] ...
//     [ ELSE <expr> ]
//   END
// The type is equal to the type of the WHEN <condval> THEN <expr> clauses, or
// the type of the ELSE <expr> value if all the previous types are unknown.
func typeCase(ev ExprView) types.T {
	// Skip over the first child since that corresponds to the input <cond>.
	for i := 1; i < ev.ChildCount(); i++ {
		childType := ev.Child(i).Logical().Scalar.Type
		if childType != types.Unknown {
			return childType
		}
	}
	return types.Unknown
}

// typeWhen returns the type of a WHEN <condval> THEN <expr> clause inside a
// CASE statement.
func typeWhen(ev ExprView) types.T {
	val := ev.Child(1)
	return val.Logical().Scalar.Type
}

// typeCast returns the type of a CAST operator.
func typeCast(ev ExprView) types.T {
	return coltypes.CastTargetToDatumType(ev.Private().(coltypes.T))
}

// typeAsPrivate returns a type extracted from the expression's private field,
// which is an instance of types.T.
func typeAsPrivate(ev ExprView) types.T {
	return ev.Private().(types.T)
}

// typeSubquery returns the type of a subquery, which is equal to the type of
// its first (and only) column.
func typeSubquery(ev ExprView) types.T {
	colID, _ := ev.Child(0).Logical().Relational.OutputCols.Next(0)
	return ev.Metadata().ColumnType(opt.ColumnID(colID))
}

func typeColumnAccess(ev ExprView) types.T {
	colIdx := ev.Private().(TupleOrdinal)
	typ := ev.Child(0).Logical().Scalar.Type.(types.TTuple)
	return typ.Types[colIdx]
}

// overload encapsulates information about a binary operator overload, to be
// used for type inference and null folding. The tree.BinOp struct does not
// work well for this use case, because it is quite large, and was not defined
// in a way allowing it to be passed by reference (without extra allocation).
type overload struct {
	// returnType of the overload. This depends on the argument types.
	returnType types.T

	// allowNullArgs is true if the operator allows null arguments, and cannot
	// therefore be folded away to null.
	allowNullArgs bool
}

// findBinaryOverload finds the correct type signature overload for the
// specified binary operator, given the types of its inputs. If an overload is
// found, findBinaryOverload returns true, plus information about the overload.
// If an overload is not found, findBinaryOverload returns false.
func findBinaryOverload(op opt.Operator, leftType, rightType types.T) (_ overload, ok bool) {
	binOp := opt.BinaryOpReverseMap[op]

	// Find the binary op that matches the type of the expression's left and
	// right children. No more than one match should ever be found. The
	// TestTypingBinaryAssumptions test ensures this will be the case even if
	// new operators or overloads are added.
	for _, binOverloads := range tree.BinOps[binOp] {
		o := binOverloads.(tree.BinOp)

		if leftType == types.Unknown {
			if rightType.Equivalent(o.RightType) {
				return overload{returnType: o.ReturnType, allowNullArgs: o.NullableArgs}, true
			}
		} else if rightType == types.Unknown {
			if leftType.Equivalent(o.LeftType) {
				return overload{returnType: o.ReturnType, allowNullArgs: o.NullableArgs}, true
			}
		} else {
			if leftType.Equivalent(o.LeftType) && rightType.Equivalent(o.RightType) {
				return overload{returnType: o.ReturnType, allowNullArgs: o.NullableArgs}, true
			}
		}
	}
	return overload{}, false
}

// EvalUnaryOp evaluates a unary expression on top of a constant value, returning
// a datum referring to the evaluated result. If an appropriate overload is not
// found, EvalUnaryOp returns an error.
func EvalUnaryOp(evalCtx *tree.EvalContext, op opt.Operator, ev ExprView) (tree.Datum, error) {
	if !ev.IsConstValue() {
		panic("expected const value")
	}

	unaryOp := opt.UnaryOpReverseMap[op]
	datum := ExtractConstDatum(ev)

	typ := datum.ResolvedType()
	for _, unaryOverloads := range tree.UnaryOps[unaryOp] {
		o := unaryOverloads.(tree.UnaryOp)
		if o.Typ.Equivalent(typ) {
			return o.Fn(evalCtx, datum)
		}
	}
	return nil, fmt.Errorf("no overload found for %s applied to %s", op, typ)
}
