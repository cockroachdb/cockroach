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

package opt

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// Operator describes the type of operation that a memo expression performs.
// Some operators are relational (join, select, project) and others are scalar
// (and, or, plus, variable).
type Operator uint16

// String returns the name of the operator as a string.
func (op Operator) String() string {
	if op >= Operator(len(opNames)-1) {
		return fmt.Sprintf("Operator(%d)", op)
	}
	return opNames[opNameIndexes[op]:opNameIndexes[op+1]]
}

// SyntaxTag returns the name of the operator using the SQL syntax that most
// closely matches it.
func (op Operator) SyntaxTag() string {
	// Handle any special cases where default codegen tag isn't best choice as
	// switch cases.
	switch op {
	default:
		// Use default codegen tag, which is mechanically derived from the
		// operator name.
		if op >= Operator(len(opNames)-1) {
			// Use UNKNOWN.
			op = 0
		}
		return opSyntaxTags[opSyntaxTagIndexes[op]:opSyntaxTagIndexes[op+1]]
	}
}

// Expr is a node in an expression tree. It offers methods to traverse and
// inspect the tree. Each node in the tree has an enumerated operator type, zero
// or more children, and an optional private value. The entire tree can be
// easily visited using a pattern like this:
//
//   var visit func(e Expr)
//   visit := func(e Expr) {
//     for i, n := 0, e.ChildCount(); i < n; i++ {
//       visit(e.Child(i))
//     }
//   }
//
type Expr interface {
	// Op returns the operator type of the expression.
	Op() Operator

	// ChildCount returns the number of children of the expression.
	ChildCount() int

	// Child returns the nth child of the expression.
	Child(nth int) Expr

	// Private returns operator-specific data. Callers are expected to know the
	// type and format of the data, which will differ from operator to operator.
	// For example, an operator may choose to return one of its fields, or perhaps
	// a pointer to itself, or nil if there is nothing useful to return.
	Private() interface{}

	// String returns a human-readable string representation for the expression
	// that can be used for debugging and testing.
	String() string
}

// ScalarExpr is a scalar expression, which is an expression that returns a
// primitive-typed value like boolean or string rather than rows and columns.
type ScalarExpr interface {
	Expr

	// DataType is the SQL type of the expression.
	DataType() types.T
}

// MutableExpr is implemented by expressions that allow their children to be
// updated.
type MutableExpr interface {
	// SetChild updates the nth child of the expression to instead be the given
	// child expression.
	SetChild(nth int, child Expr)
}

// ComparisonOpMap maps from a semantic tree comparison operator type to an
// optimizer operator type.
var ComparisonOpMap [tree.NumComparisonOperators]Operator

// ComparisonOpReverseMap maps from an optimizer operator type to a semantic
// tree comparison operator type.
var ComparisonOpReverseMap = map[Operator]tree.ComparisonOperator{
	EqOp:             tree.EQ,
	LtOp:             tree.LT,
	GtOp:             tree.GT,
	LeOp:             tree.LE,
	GeOp:             tree.GE,
	NeOp:             tree.NE,
	InOp:             tree.In,
	NotInOp:          tree.NotIn,
	LikeOp:           tree.Like,
	NotLikeOp:        tree.NotLike,
	ILikeOp:          tree.ILike,
	NotILikeOp:       tree.NotILike,
	SimilarToOp:      tree.SimilarTo,
	NotSimilarToOp:   tree.NotSimilarTo,
	RegMatchOp:       tree.RegMatch,
	NotRegMatchOp:    tree.NotRegMatch,
	RegIMatchOp:      tree.RegIMatch,
	NotRegIMatchOp:   tree.NotRegIMatch,
	IsOp:             tree.IsNotDistinctFrom,
	IsNotOp:          tree.IsDistinctFrom,
	ContainsOp:       tree.Contains,
	JsonExistsOp:     tree.JSONExists,
	JsonSomeExistsOp: tree.JSONSomeExists,
	JsonAllExistsOp:  tree.JSONAllExists,
}

// BinaryOpReverseMap maps from an optimizer operator type to a semantic tree
// binary operator type.
var BinaryOpReverseMap = map[Operator]tree.BinaryOperator{
	BitandOp:        tree.Bitand,
	BitorOp:         tree.Bitor,
	BitxorOp:        tree.Bitxor,
	PlusOp:          tree.Plus,
	MinusOp:         tree.Minus,
	MultOp:          tree.Mult,
	DivOp:           tree.Div,
	FloorDivOp:      tree.FloorDiv,
	ModOp:           tree.Mod,
	PowOp:           tree.Pow,
	ConcatOp:        tree.Concat,
	LShiftOp:        tree.LShift,
	RShiftOp:        tree.RShift,
	FetchValOp:      tree.JSONFetchVal,
	FetchTextOp:     tree.JSONFetchText,
	FetchValPathOp:  tree.JSONFetchValPath,
	FetchTextPathOp: tree.JSONFetchTextPath,
}

// UnaryOpReverseMap maps from an optimizer operator type to a semantic tree
// unary operator type.
var UnaryOpReverseMap = map[Operator]tree.UnaryOperator{
	UnaryMinusOp:      tree.UnaryMinus,
	UnaryComplementOp: tree.UnaryComplement,
}

// AggregateOpReverseMap maps from an optimizer operator type to the name of an
// aggregation function.
var AggregateOpReverseMap = map[Operator]string{
	ArrayAggOp:        "array_agg",
	AvgOp:             "avg",
	BoolAndOp:         "bool_and",
	BoolOrOp:          "bool_or",
	ConcatAggOp:       "concat_agg",
	CountOp:           "count",
	CountRowsOp:       "count_rows",
	MaxOp:             "max",
	MinOp:             "min",
	SumIntOp:          "sum_int",
	SumOp:             "sum",
	SqrDiffOp:         "sqrdiff",
	VarianceOp:        "variance",
	StdDevOp:          "stddev",
	XorAggOp:          "xor_agg",
	JsonAggOp:         "json_agg",
	JsonbAggOp:        "jsonb_agg",
	StringAggOp:       "string_agg",
	ConstAggOp:        "any_not_null",
	ConstNotNullAggOp: "any_not_null",
	AnyNotNullAggOp:   "any_not_null",
}

// NegateOpMap maps from a comparison operator type to its negated operator
// type, as if the Not operator was applied to it. Some comparison operators,
// like Contains and JsonExists, do not have negated versions.
var NegateOpMap = map[Operator]Operator{
	EqOp:           NeOp,
	LtOp:           GeOp,
	GtOp:           LeOp,
	LeOp:           GtOp,
	GeOp:           LtOp,
	NeOp:           EqOp,
	InOp:           NotInOp,
	NotInOp:        InOp,
	LikeOp:         NotLikeOp,
	NotLikeOp:      LikeOp,
	ILikeOp:        NotILikeOp,
	NotILikeOp:     ILikeOp,
	SimilarToOp:    NotSimilarToOp,
	NotSimilarToOp: SimilarToOp,
	RegMatchOp:     NotRegMatchOp,
	NotRegMatchOp:  RegMatchOp,
	RegIMatchOp:    NotRegIMatchOp,
	NotRegIMatchOp: RegIMatchOp,
	IsOp:           IsNotOp,
	IsNotOp:        IsOp,
}

// BoolOperatorRequiresNotNullArgs returns true if the operator can never
// evaluate to true if one of its children is NULL.
func BoolOperatorRequiresNotNullArgs(op Operator) bool {
	switch op {
	case
		EqOp, LtOp, LeOp, GtOp, GeOp, NeOp,
		LikeOp, NotLikeOp, ILikeOp, NotILikeOp, SimilarToOp, NotSimilarToOp,
		RegMatchOp, NotRegMatchOp, RegIMatchOp, NotRegIMatchOp:
		return true
	}
	return false
}

// AggregateIsOrderingSensitive returns true if the given aggregate operator is
// non-commutative. That is, it can give different results based on the order
// values are fed to it.
func AggregateIsOrderingSensitive(op Operator) bool {
	switch op {
	case ArrayAggOp, ConcatAggOp, JsonAggOp, JsonbAggOp, StringAggOp:
		return true
	}
	return false
}

// AggregateIgnoresNulls returns true if the given aggregate operator has a
// single input, and if it always evaluates to the same result regardless of
// how many NULL values are included in that input, in any order.
func AggregateIgnoresNulls(op Operator) bool {
	switch op {
	case AvgOp, BoolAndOp, BoolOrOp, CountOp, MaxOp, MinOp, SumIntOp, SumOp,
		SqrDiffOp, VarianceOp, StdDevOp, XorAggOp, ConstNotNullAggOp,
		AnyNotNullAggOp, StringAggOp:
		return true
	}
	return false
}

// AggregateIsNullOnEmpty returns true if the given aggregate operator has a
// single input, and if it returns NULL when the input set contains no values.
// This group of aggregates overlaps considerably with the AggregateIgnoresNulls
// group, with the notable exception of COUNT, which returns zero instead of
// NULL when its input is empty.
func AggregateIsNullOnEmpty(op Operator) bool {
	switch op {
	case AvgOp, BoolAndOp, BoolOrOp, MaxOp, MinOp, SumIntOp, SumOp, SqrDiffOp,
		VarianceOp, StdDevOp, XorAggOp, ConstAggOp, ConstNotNullAggOp, ArrayAggOp,
		ConcatAggOp, JsonAggOp, JsonbAggOp, AnyNotNullAggOp, StringAggOp:
		return true
	}
	return false
}

func init() {
	for optOp, treeOp := range ComparisonOpReverseMap {
		ComparisonOpMap[treeOp] = optOp
	}
}
