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

// InferType derives the type of the given scalar expression and stores it in
// the expression's Type field. Depending upon the operator, the type may be
// fixed, or it may be dependent upon the expression children.
func InferType(mem *Memo, e opt.ScalarExpr) types.T {
	// Special-case Variable, since it's the only expression that needs the memo.
	if e.Op() == opt.VariableOp {
		return typeVariable(mem, e)
	}

	fn := typingFuncMap[e.Op()]
	if fn == nil {
		panic(fmt.Sprintf("type inference for %v is not yet implemented", e.Op()))
	}
	return fn(e)
}

// InferUnaryType infers the return type of a unary operator, given the type of
// its input.
func InferUnaryType(op opt.Operator, inputType types.T) types.T {
	unaryOp := opt.UnaryOpReverseMap[op]

	// Find the unary op that matches the type of the expression's child.
	for _, op := range tree.UnaryOps[unaryOp] {
		o := op.(*tree.UnaryOp)
		if inputType.Equivalent(o.Typ) {
			return o.ReturnType
		}
	}
	panic(fmt.Sprintf("could not find type for unary expression %s", op))
}

// InferBinaryType infers the return type of a binary expression, given the type
// of its inputs.
func InferBinaryType(op opt.Operator, leftType, rightType types.T) types.T {
	o, ok := FindBinaryOverload(op, leftType, rightType)
	if !ok {
		panic(fmt.Sprintf("could not find type for binary expression %s", op))
	}
	return o.ReturnType
}

// BinaryOverloadExists returns true if the given binary operator exists with the
// given arguments.
func BinaryOverloadExists(op opt.Operator, leftType, rightType types.T) bool {
	_, ok := FindBinaryOverload(op, leftType, rightType)
	return ok
}

// BinaryAllowsNullArgs returns true if the given binary operator allows null
// arguments, and cannot therefore be folded away to null.
func BinaryAllowsNullArgs(op opt.Operator, leftType, rightType types.T) bool {
	o, ok := FindBinaryOverload(op, leftType, rightType)
	if !ok {
		panic(fmt.Sprintf("could not find overload for binary expression %s", op))
	}
	return o.NullableArgs
}

// FindAggregateOverload finds an aggregate function overload that matches the
// given aggregate function expression. It panics if no match can be found.
func FindAggregateOverload(e opt.ScalarExpr) (name string, overload *tree.Overload) {
	name = opt.AggregateOpReverseMap[e.Op()]
	_, overloads := builtins.GetBuiltinProperties(name)
	for o := range overloads {
		overload = &overloads[o]
		matches := true
		for i, n := 0, e.ChildCount(); i < n; i++ {
			typ := e.Child(i).(opt.ScalarExpr).DataType()
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

type typingFunc func(e opt.ScalarExpr) types.T

// typingFuncMap is a lookup table from scalar operator type to a function
// which returns the data type of an instance of that operator.
var typingFuncMap map[opt.Operator]typingFunc

func init() {
	typingFuncMap = make(map[opt.Operator]typingFunc)
	typingFuncMap[opt.ConstOp] = typeAsTypedExpr
	typingFuncMap[opt.PlaceholderOp] = typeAsTypedExpr
	typingFuncMap[opt.UnsupportedExprOp] = typeAsTypedExpr
	typingFuncMap[opt.CoalesceOp] = typeCoalesce
	typingFuncMap[opt.CaseOp] = typeCase
	typingFuncMap[opt.WhenOp] = typeWhen
	typingFuncMap[opt.CastOp] = typeCast
	typingFuncMap[opt.SubqueryOp] = typeSubquery
	typingFuncMap[opt.ColumnAccessOp] = typeColumnAccess
	typingFuncMap[opt.IndirectionOp] = typeIndirection

	// Override default typeAsAggregate behavior for aggregate functions with
	// a large number of possible overloads or where ReturnType depends on
	// argument types.
	typingFuncMap[opt.ArrayAggOp] = typeArrayAgg
	typingFuncMap[opt.MaxOp] = typeAsFirstArg
	typingFuncMap[opt.MinOp] = typeAsFirstArg
	typingFuncMap[opt.ConstAggOp] = typeAsFirstArg
	typingFuncMap[opt.ConstNotNullAggOp] = typeAsFirstArg
	typingFuncMap[opt.AnyNotNullAggOp] = typeAsFirstArg
	typingFuncMap[opt.FirstAggOp] = typeAsFirstArg

	// Modifiers for aggregations pass through their argument.
	typingFuncMap[opt.AggDistinctOp] = typeAsFirstArg

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
func typeVariable(mem *Memo, e opt.ScalarExpr) types.T {
	variable := e.(*VariableExpr)
	typ := mem.Metadata().ColumnType(variable.Col)
	if typ == nil {
		panic(fmt.Sprintf("column %d does not have type", variable.Col))
	}
	return typ
}

// typeArrayAgg returns an array type with element type equal to the type of the
// aggregate expression's first (and only) argument.
func typeArrayAgg(e opt.ScalarExpr) types.T {
	arrayAgg := e.(*ArrayAggExpr)
	typ := arrayAgg.Input.DataType()
	return types.TArray{Typ: typ}
}

// typeIndirection returns the type of the element of the array.
func typeIndirection(e opt.ScalarExpr) types.T {
	return types.UnwrapType(e.Child(0).(opt.ScalarExpr).DataType()).(types.TArray).Typ
}

// typeAsFirstArg returns the type of the expression's 0th argument.
func typeAsFirstArg(e opt.ScalarExpr) types.T {
	return e.Child(0).(opt.ScalarExpr).DataType()
}

// typeAsTypedExpr returns the resolved type of the private field, with the
// assumption that it is a tree.TypedExpr.
func typeAsTypedExpr(e opt.ScalarExpr) types.T {
	return e.Private().(tree.TypedExpr).ResolvedType()
}

// typeAsUnary returns the type of a unary expression by hooking into the sql
// semantics code that searches for unary operator overloads.
func typeAsUnary(e opt.ScalarExpr) types.T {
	return InferUnaryType(e.Op(), e.Child(0).(opt.ScalarExpr).DataType())
}

// typeAsBinary returns the type of a binary expression by hooking into the sql
// semantics code that searches for binary operator overloads.
func typeAsBinary(e opt.ScalarExpr) types.T {
	leftType := e.Child(0).(opt.ScalarExpr).DataType()
	rightType := e.Child(1).(opt.ScalarExpr).DataType()
	return InferBinaryType(e.Op(), leftType, rightType)
}

// typeAsAggregate returns the type of an aggregate expression by hooking into
// the sql semantics code that searches for aggregate operator overloads.
func typeAsAggregate(e opt.ScalarExpr) types.T {
	// Only handle cases where the return type is not dependent on argument
	// types (i.e. pass nil to the ReturnTyper). Aggregates with return types
	// that depend on argument types are handled separately.
	_, overload := FindAggregateOverload(e)
	t := overload.ReturnType(nil)
	if t == tree.UnknownReturnType {
		panic(fmt.Sprintf("unknown aggregate return type. e:\n%s", e))
	}
	return t
}

// typeCoalesce returns the type of a coalesce expression, which is equal to
// the type of its first non-null child.
func typeCoalesce(e opt.ScalarExpr) types.T {
	for _, arg := range e.(*CoalesceExpr).Args {
		childType := arg.DataType()
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
func typeCase(e opt.ScalarExpr) types.T {
	caseExpr := e.(*CaseExpr)
	for _, when := range caseExpr.Whens {
		childType := when.DataType()
		if childType != types.Unknown {
			return childType
		}
	}
	return caseExpr.OrElse.DataType()
}

// typeWhen returns the type of a WHEN <condval> THEN <expr> clause inside a
// CASE statement.
func typeWhen(e opt.ScalarExpr) types.T {
	return e.(*WhenExpr).Value.DataType()
}

// typeCast returns the type of a CAST operator.
func typeCast(e opt.ScalarExpr) types.T {
	return coltypes.CastTargetToDatumType(e.(*CastExpr).TargetTyp)
}

// typeSubquery returns the type of a subquery, which is equal to the type of
// its first (and only) column.
func typeSubquery(e opt.ScalarExpr) types.T {
	input := e.Child(0).(RelExpr)
	colID, _ := input.Relational().OutputCols.Next(0)
	return input.Memo().Metadata().ColumnType(opt.ColumnID(colID))
}

func typeColumnAccess(e opt.ScalarExpr) types.T {
	colAccess := e.(*ColumnAccessExpr)
	typ := colAccess.Input.DataType().(types.TTuple)
	return typ.Types[colAccess.Idx]
}

// FindBinaryOverload finds the correct type signature overload for the
// specified binary operator, given the types of its inputs. If an overload is
// found, FindBinaryOverload returns true, plus a pointer to the overload.
// If an overload is not found, FindBinaryOverload returns false.
func FindBinaryOverload(op opt.Operator, leftType, rightType types.T) (_ *tree.BinOp, ok bool) {
	bin := opt.BinaryOpReverseMap[op]

	// Find the binary op that matches the type of the expression's left and
	// right children. No more than one match should ever be found. The
	// TestTypingBinaryAssumptions test ensures this will be the case even if
	// new operators or overloads are added.
	for _, binOverloads := range tree.BinOps[bin] {
		o := binOverloads.(*tree.BinOp)

		if leftType == types.Unknown {
			if rightType.Equivalent(o.RightType) {
				return o, true
			}
		} else if rightType == types.Unknown {
			if leftType.Equivalent(o.LeftType) {
				return o, true
			}
		} else {
			if leftType.Equivalent(o.LeftType) && rightType.Equivalent(o.RightType) {
				return o, true
			}
		}
	}
	return nil, false
}

// FindUnaryOverload finds the correct type signature overload for the
// specified unary operator, given the type of its input. If an overload is
// found, FindUnaryOverload returns true, plus a pointer to the overload.
// If an overload is not found, FindUnaryOverload returns false.
func FindUnaryOverload(op opt.Operator, typ types.T) (_ *tree.UnaryOp, ok bool) {
	unary := opt.UnaryOpReverseMap[op]

	for _, unaryOverloads := range tree.UnaryOps[unary] {
		o := unaryOverloads.(*tree.UnaryOp)
		if o.Typ.Equivalent(typ) {
			return o, true
		}
	}
	return nil, false
}

// FindComparisonOverload finds the correct type signature overload for the
// specified comparison operator, given the types of its inputs. If an overload
// is found, FindComparisonOverload returns true, plus a pointer to the
// overload. If an overload is not found, FindComparisonOverload returns false.
func FindComparisonOverload(op opt.Operator, leftType, rightType types.T) (_ *tree.CmpOp, ok bool) {
	comp := opt.ComparisonOpReverseMap[op]

	// Find the comparison op that matches the type of the expression's left and
	// right children. No more than one match should ever be found. The
	// TestTypingComparisonAssumptions test ensures this will be the case even if
	// new operators or overloads are added.
	for _, cmpOverloads := range tree.CmpOps[comp] {
		o := cmpOverloads.(*tree.CmpOp)

		if leftType == types.Unknown {
			if rightType.Equivalent(o.RightType) {
				return o, true
			}
		} else if rightType == types.Unknown {
			if leftType.Equivalent(o.LeftType) {
				return o, true
			}
		} else {
			if leftType.Equivalent(o.LeftType) && rightType.Equivalent(o.RightType) {
				return o, true
			}
		}
	}
	return nil, false
}
