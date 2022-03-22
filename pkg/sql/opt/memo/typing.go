// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package memo

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// InferType derives the type of the given scalar expression. The result is
// stored in the expression's Type field by Memoize[Expr] functions. Depending
// upon the operator, the type may be fixed, or it may be dependent upon the
// expression children.
func InferType(mem *Memo, e opt.ScalarExpr) *types.T {
	// Special-case Variable, since it's the only expression that needs the memo.
	if e.Op() == opt.VariableOp {
		return typeVariable(mem, e)
	}

	fn := typingFuncMap[e.Op()]
	if fn == nil {
		panic(errors.AssertionFailedf("type inference for %v is not yet implemented", redact.Safe(e.Op())))
	}
	return fn(e)
}

// InferUnaryType infers the return type of a unary operator, given the type of
// its input.
func InferUnaryType(op opt.Operator, inputType *types.T) *types.T {
	unaryOp := opt.UnaryOpReverseMap[op]

	// Find the unary op that matches the type of the expression's child.
	for _, op := range tree.UnaryOps[unaryOp] {
		o := op.(*tree.UnaryOp)
		if inputType.Equivalent(o.Typ) {
			return o.ReturnType
		}
	}
	panic(errors.AssertionFailedf("could not find type for unary expression %s", redact.Safe(op)))
}

// InferBinaryType infers the return type of a binary expression, given the type
// of its inputs.
func InferBinaryType(op opt.Operator, leftType, rightType *types.T) *types.T {
	o, ok := FindBinaryOverload(op, leftType, rightType)
	if !ok {
		panic(errors.AssertionFailedf("could not find type for binary expression %s", redact.Safe(op)))
	}
	return o.ReturnType
}

// InferWhensType returns the type of a CASE expression, which is
// of the form:
//   CASE [ <cond> ]
//       WHEN <condval1> THEN <expr1>
//     [ WHEN <condval2> THEN <expr2> ] ...
//     [ ELSE <expr> ]
//   END
// All possible values should have the same type, and that is the type of the
// case.
func InferWhensType(whens ScalarListExpr, orElse opt.ScalarExpr) *types.T {
	result := orElse.DataType()
	// Sanity check.
	for _, when := range whens {
		if !result.Equivalent(when.DataType()) {
			panic(errors.AssertionFailedf("inconsistent Case return types %s %s", when.DataType(), result))
		}
	}
	return result
}

// BinaryOverloadExists returns true if the given binary operator exists with the
// given arguments.
func BinaryOverloadExists(op opt.Operator, leftType, rightType *types.T) bool {
	_, ok := FindBinaryOverload(op, leftType, rightType)
	return ok
}

// BinaryAllowsNullArgs returns true if the given binary operator allows null
// arguments, and cannot therefore be folded away to null.
func BinaryAllowsNullArgs(op opt.Operator, leftType, rightType *types.T) bool {
	o, ok := FindBinaryOverload(op, leftType, rightType)
	if !ok {
		panic(errors.AssertionFailedf("could not find overload for binary expression %s", redact.Safe(op)))
	}
	return o.NullableArgs
}

// AggregateOverloadExists returns whether or not the given operator has a
// unary overload which takes the given type as input.
func AggregateOverloadExists(agg opt.Operator, typ *types.T) bool {
	name := opt.AggregateOpReverseMap[agg]
	_, overloads := builtins.GetBuiltinProperties(name)
	for _, o := range overloads {
		if o.Types.MatchAt(typ, 0) {
			return true
		}
	}
	return false
}

// FindFunction returns the function properties and overload of the function
// with the given name and argument types matching the children of the given
// input.
func FindFunction(
	e opt.ScalarExpr, name string,
) (props *tree.FunctionProperties, overload *tree.Overload, ok bool) {
	props, overloads := builtins.GetBuiltinProperties(name)
	for o := range overloads {
		overload = &overloads[o]
		if overload.Types.Length() != e.ChildCount() {
			continue
		}
		matches := true
		for i, n := 0, e.ChildCount(); i < n; i++ {
			typ := e.Child(i).(opt.ScalarExpr).DataType()
			if !overload.Types.MatchAt(typ, i) {
				matches = false
				break
			}
		}
		if matches {
			return props, overload, true
		}
	}
	return nil, nil, false
}

// FindWindowOverload finds a window function overload that matches the
// given window function expression. It panics if no match can be found.
func FindWindowOverload(e opt.ScalarExpr) (name string, overload *tree.Overload) {
	name = opt.WindowOpReverseMap[e.Op()]
	_, overload, ok := FindFunction(e, name)
	if ok {
		return name, overload
	}
	// NB: all aggregate functions can be used as window functions.
	return FindAggregateOverload(e)
}

// FindAggregateOverload finds an aggregate function overload that matches the
// given aggregate function expression. It panics if no match can be found.
func FindAggregateOverload(e opt.ScalarExpr) (name string, overload *tree.Overload) {
	name = opt.AggregateOpReverseMap[e.Op()]
	_, overload, ok := FindFunction(e, name)
	if ok {
		return name, overload
	}
	panic(errors.AssertionFailedf("could not find overload for %s aggregate", name))
}

type typingFunc func(e opt.ScalarExpr) *types.T

// typingFuncMap is a lookup table from scalar operator type to a function
// which returns the data type of an instance of that operator.
var typingFuncMap map[opt.Operator]typingFunc

func init() {
	typingFuncMap = make(map[opt.Operator]typingFunc)
	typingFuncMap[opt.PlaceholderOp] = typeAsTypedExpr
	typingFuncMap[opt.CoalesceOp] = typeCoalesce
	typingFuncMap[opt.CaseOp] = typeCase
	typingFuncMap[opt.WhenOp] = typeWhen
	typingFuncMap[opt.CastOp] = typeCast
	typingFuncMap[opt.SubqueryOp] = typeSubquery
	typingFuncMap[opt.ColumnAccessOp] = typeColumnAccess
	typingFuncMap[opt.IndirectionOp] = typeIndirection
	typingFuncMap[opt.CollateOp] = typeCollate
	typingFuncMap[opt.ArrayFlattenOp] = typeArrayFlatten
	typingFuncMap[opt.IfErrOp] = typeIfErr

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

	typingFuncMap[opt.LagOp] = typeAsFirstArg
	typingFuncMap[opt.LeadOp] = typeAsFirstArg
	typingFuncMap[opt.NthValueOp] = typeAsFirstArg

	// Modifiers for aggregations pass through their argument.
	typingFuncMap[opt.AggDistinctOp] = typeAsFirstArg
	typingFuncMap[opt.AggFilterOp] = typeAsFirstArg
	typingFuncMap[opt.WindowFromOffsetOp] = typeAsFirstArg
	typingFuncMap[opt.WindowToOffsetOp] = typeAsFirstArg

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

	for _, op := range opt.WindowOperators {
		if typingFuncMap[op] == nil {
			typingFuncMap[op] = typeAsWindow
		}
	}
}

// typeVariable returns the type of a variable expression, which is stored in
// the query metadata and accessed by column id.
func typeVariable(mem *Memo, e opt.ScalarExpr) *types.T {
	variable := e.(*VariableExpr)
	typ := mem.Metadata().ColumnMeta(variable.Col).Type
	if typ == nil {
		panic(errors.AssertionFailedf("column %d does not have type", redact.Safe(variable.Col)))
	}
	return typ
}

// typeArrayAgg returns an array type with element type equal to the type of the
// aggregate expression's first (and only) argument.
func typeArrayAgg(e opt.ScalarExpr) *types.T {
	arrayAgg := e.(*ArrayAggExpr)
	typ := arrayAgg.Input.DataType()
	return types.MakeArray(typ)
}

// typeIndirection returns the type of the element of the array.
func typeIndirection(e opt.ScalarExpr) *types.T {
	return e.Child(0).(opt.ScalarExpr).DataType().ArrayContents()
}

// typeCollate returns the collated string typed with the given locale.
func typeCollate(e opt.ScalarExpr) *types.T {
	locale := e.(*CollateExpr).Locale
	return types.MakeCollatedString(types.String, locale)
}

// typeArrayFlatten returns the type of the subquery as an array.
func typeArrayFlatten(e opt.ScalarExpr) *types.T {
	input := e.Child(0).(RelExpr)
	colID := e.(*ArrayFlattenExpr).RequestedCol
	return types.MakeArray(input.Memo().Metadata().ColumnMeta(colID).Type)
}

// typeIfErr returns the type of the IfErrExpr. The type is boolean if
// there is no OrElse, and the type of Cond/OrElse otherwise.
func typeIfErr(e opt.ScalarExpr) *types.T {
	if e.(*IfErrExpr).OrElse.ChildCount() == 0 {
		return types.Bool
	}
	return e.(*IfErrExpr).Cond.DataType()
}

// typeAsFirstArg returns the type of the expression's 0th argument.
func typeAsFirstArg(e opt.ScalarExpr) *types.T {
	return e.Child(0).(opt.ScalarExpr).DataType()
}

// typeAsTypedExpr returns the resolved type of the private field, with the
// assumption that it is a tree.TypedExpr.
func typeAsTypedExpr(e opt.ScalarExpr) *types.T {
	return e.Private().(tree.TypedExpr).ResolvedType()
}

// typeAsUnary returns the type of a unary expression by hooking into the sql
// semantics code that searches for unary operator overloads.
func typeAsUnary(e opt.ScalarExpr) *types.T {
	return InferUnaryType(e.Op(), e.Child(0).(opt.ScalarExpr).DataType())
}

// typeAsBinary returns the type of a binary expression by hooking into the sql
// semantics code that searches for binary operator overloads.
func typeAsBinary(e opt.ScalarExpr) *types.T {
	leftType := e.Child(0).(opt.ScalarExpr).DataType()
	rightType := e.Child(1).(opt.ScalarExpr).DataType()
	return InferBinaryType(e.Op(), leftType, rightType)
}

// typeAsAggregate returns the type of an aggregate expression by hooking into
// the sql semantics code that searches for aggregate operator overloads.
func typeAsAggregate(e opt.ScalarExpr) *types.T {
	// Only handle cases where the return type is not dependent on argument
	// types (i.e. pass nil to the ReturnTyper). Aggregates with return types
	// that depend on argument types are handled separately.
	_, overload := FindAggregateOverload(e)
	t := overload.ReturnType(nil)
	if t == tree.UnknownReturnType {
		panic(errors.AssertionFailedf("unknown aggregate return type. e:\n%s", e))
	}
	return t
}

// typeAsWindow returns the type of a window function expression similar to
// typeAsAggregate.
func typeAsWindow(e opt.ScalarExpr) *types.T {
	_, overload := FindWindowOverload(e)
	t := overload.ReturnType(nil)
	if t == tree.UnknownReturnType {
		panic(errors.AssertionFailedf("unknown window return type. e:\n%s", e))
	}

	return t
}

// typeCoalesce returns the type of a coalesce expression, which is equal to
// the type of its first non-null child.
func typeCoalesce(e opt.ScalarExpr) *types.T {
	for _, arg := range e.(*CoalesceExpr).Args {
		childType := arg.DataType()
		if childType.Family() != types.UnknownFamily {
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
func typeCase(e opt.ScalarExpr) *types.T {
	caseExpr := e.(*CaseExpr)
	return InferWhensType(caseExpr.Whens, caseExpr.OrElse)
}

// typeWhen returns the type of a WHEN <condval> THEN <expr> clause inside a
// CASE statement.
func typeWhen(e opt.ScalarExpr) *types.T {
	return e.(*WhenExpr).Value.DataType()
}

// typeCast returns the type of a CAST operator.
func typeCast(e opt.ScalarExpr) *types.T {
	return e.(*CastExpr).Typ
}

// typeSubquery returns the type of a subquery, which is equal to the type of
// its first (and only) column.
func typeSubquery(e opt.ScalarExpr) *types.T {
	input := e.Child(0).(RelExpr)
	colID := input.Relational().OutputCols.SingleColumn()
	return input.Memo().Metadata().ColumnMeta(colID).Type
}

func typeColumnAccess(e opt.ScalarExpr) *types.T {
	colAccess := e.(*ColumnAccessExpr)
	typ := colAccess.Input.DataType()
	return typ.TupleContents()[colAccess.Idx]
}

// FindBinaryOverload finds the correct type signature overload for the
// specified binary operator, given the types of its inputs. If an overload is
// found, FindBinaryOverload returns true, plus a pointer to the overload.
// If an overload is not found, FindBinaryOverload returns false.
func FindBinaryOverload(op opt.Operator, leftType, rightType *types.T) (_ *tree.BinOp, ok bool) {
	bin := opt.BinaryOpReverseMap[op]

	// Find the binary op that matches the type of the expression's left and
	// right children. No more than one match should ever be found. The
	// TestTypingBinaryAssumptions test ensures this will be the case even if
	// new operators or overloads are added.
	for _, binOverloads := range tree.BinOps[bin] {
		o := binOverloads.(*tree.BinOp)

		if leftType.Family() == types.UnknownFamily {
			if rightType.Equivalent(o.RightType) {
				return o, true
			}
		} else if rightType.Family() == types.UnknownFamily {
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
func FindUnaryOverload(op opt.Operator, typ *types.T) (_ *tree.UnaryOp, ok bool) {
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
// is found, FindComparisonOverload returns a pointer to the overload and
// ok=true. It also returns "flipped" and "not" flags. The "flipped" flag
// indicates whether the original left and right operands should be flipped
// with the returned overload. The "not" flag indicates whether the result of
// the comparison operation should be negated. If an overload is not found,
// FindComparisonOverload returns ok=false.
func FindComparisonOverload(
	op opt.Operator, leftType, rightType *types.T,
) (_ *tree.CmpOp, flipped, not, ok bool) {
	op, flipped, not = NormalizeComparison(op)
	comp := opt.ComparisonOpReverseMap[op]

	if flipped {
		leftType, rightType = rightType, leftType
	}

	// Find the comparison op that matches the type of the expression's left and
	// right children. No more than one match should ever be found. The
	// TestTypingComparisonAssumptions test ensures this will be the case even if
	// new operators or overloads are added.
	for _, cmpOverloads := range tree.CmpOps[comp] {
		o := cmpOverloads.(*tree.CmpOp)

		if leftType.Family() == types.UnknownFamily {
			if rightType.Equivalent(o.RightType) {
				return o, flipped, not, true
			}
		} else if rightType.Family() == types.UnknownFamily {
			if leftType.Equivalent(o.LeftType) {
				return o, flipped, not, true
			}
		} else {
			if leftType.Equivalent(o.LeftType) && rightType.Equivalent(o.RightType) {
				return o, flipped, not, true
			}
		}
	}
	return nil, false, false, false
}

// NormalizeComparison maps a given comparison operator into an equivalent
// operator that exists in the tree.CmpOps map, returning this new operator,
// along with "flipped" and "not" flags. The "flipped" flag indicates whether
// the left and right operands should be flipped with the new operator. The
// "not" flag indicates whether the result of the comparison operation should
// be negated.
func NormalizeComparison(op opt.Operator) (newOp opt.Operator, flipped, not bool) {
	switch op {
	case opt.NeOp:
		// Ne(left, right) is implemented as !Eq(left, right).
		return opt.EqOp, false, true
	case opt.GtOp:
		// Gt(left, right) is implemented as Lt(right, left)
		return opt.LtOp, true, false
	case opt.GeOp:
		// Ge(left, right) is implemented as Le(right, left)
		return opt.LeOp, true, false
	case opt.NotInOp:
		// NotIn(left, right) is implemented as !In(left, right)
		return opt.InOp, false, true
	case opt.NotLikeOp:
		// NotLike(left, right) is implemented as !Like(left, right)
		return opt.LikeOp, false, true
	case opt.NotILikeOp:
		// NotILike(left, right) is implemented as !ILike(left, right)
		return opt.ILikeOp, false, true
	case opt.NotSimilarToOp:
		// NotSimilarTo(left, right) is implemented as !SimilarTo(left, right)
		return opt.SimilarToOp, false, true
	case opt.NotRegMatchOp:
		// NotRegMatch(left, right) is implemented as !RegMatch(left, right)
		return opt.RegMatchOp, false, true
	case opt.NotRegIMatchOp:
		// NotRegIMatch(left, right) is implemented as !RegIMatch(left, right)
		return opt.RegIMatchOp, false, true
	case opt.IsNotOp:
		// IsNot(left, right) is implemented as !Is(left, right)
		return opt.IsOp, false, true
	}
	return op, false, false
}
