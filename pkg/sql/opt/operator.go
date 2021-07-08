// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opt

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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

// ScalarID is the type of the memo-unique identifier given to every scalar
// expression.
type ScalarID int

// ScalarExpr is a scalar expression, which is an expression that returns a
// primitive-typed value like boolean or string rather than rows and columns.
type ScalarExpr interface {
	Expr

	// ID is a unique (within the context of a memo) ID that can be
	// used to define a total order over ScalarExprs.
	ID() ScalarID

	// DataType is the SQL type of the expression.
	DataType() *types.T
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
var ComparisonOpMap [tree.NumComparisonOperatorSymbols]Operator

// ComparisonOpReverseMap maps from an optimizer operator type to a semantic
// tree comparison operator type.
var ComparisonOpReverseMap = map[Operator]tree.ComparisonOperatorSymbol{
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
	ContainedByOp:    tree.ContainedBy,
	JsonExistsOp:     tree.JSONExists,
	JsonSomeExistsOp: tree.JSONSomeExists,
	JsonAllExistsOp:  tree.JSONAllExists,
	OverlapsOp:       tree.Overlaps,
	BBoxCoversOp:     tree.RegMatch,
	BBoxIntersectsOp: tree.Overlaps,
}

// BinaryOpReverseMap maps from an optimizer operator type to a semantic tree
// binary operator type.
var BinaryOpReverseMap = map[Operator]tree.BinaryOperatorSymbol{
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
var UnaryOpReverseMap = map[Operator]tree.UnaryOperatorSymbol{
	UnaryMinusOp:      tree.UnaryMinus,
	UnaryComplementOp: tree.UnaryComplement,
	UnarySqrtOp:       tree.UnarySqrt,
	UnaryCbrtOp:       tree.UnaryCbrt,
}

// AggregateOpReverseMap maps from an optimizer operator type to the name of an
// aggregation function.
var AggregateOpReverseMap = map[Operator]string{
	ArrayAggOp:            "array_agg",
	AvgOp:                 "avg",
	BitAndAggOp:           "bit_and",
	BitOrAggOp:            "bit_or",
	BoolAndOp:             "bool_and",
	BoolOrOp:              "bool_or",
	ConcatAggOp:           "concat_agg",
	CountOp:               "count",
	CorrOp:                "corr",
	CountRowsOp:           "count_rows",
	CovarPopOp:            "covar_pop",
	CovarSampOp:           "covar_samp",
	RegressionAvgXOp:      "regr_avgx",
	RegressionAvgYOp:      "regr_avgy",
	RegressionInterceptOp: "regr_intercept",
	RegressionR2Op:        "regr_r2",
	RegressionSlopeOp:     "regr_slope",
	RegressionSXXOp:       "regr_sxx",
	RegressionSXYOp:       "regr_sxy",
	RegressionSYYOp:       "regr_syy",
	RegressionCountOp:     "regr_count",
	MaxOp:                 "max",
	MinOp:                 "min",
	SumIntOp:              "sum_int",
	SumOp:                 "sum",
	SqrDiffOp:             "sqrdiff",
	VarianceOp:            "variance",
	StdDevOp:              "stddev",
	XorAggOp:              "xor_agg",
	JsonAggOp:             "json_agg",
	JsonbAggOp:            "jsonb_agg",
	JsonObjectAggOp:       "json_object_agg",
	JsonbObjectAggOp:      "jsonb_object_agg",
	StringAggOp:           "string_agg",
	ConstAggOp:            "any_not_null",
	ConstNotNullAggOp:     "any_not_null",
	AnyNotNullAggOp:       "any_not_null",
	PercentileDiscOp:      "percentile_disc_impl",
	PercentileContOp:      "percentile_cont_impl",
	VarPopOp:              "var_pop",
	StdDevPopOp:           "stddev_pop",
	STMakeLineOp:          "st_makeline",
	STUnionOp:             "st_union",
	STCollectOp:           "st_collect",
	STExtentOp:            "st_extent",
}

// WindowOpReverseMap maps from an optimizer operator type to the name of a
// window function.
var WindowOpReverseMap = map[Operator]string{
	RankOp:        "rank",
	RowNumberOp:   "row_number",
	DenseRankOp:   "dense_rank",
	PercentRankOp: "percent_rank",
	CumeDistOp:    "cume_dist",
	NtileOp:       "ntile",
	LagOp:         "lag",
	LeadOp:        "lead",
	FirstValueOp:  "first_value",
	LastValueOp:   "last_value",
	NthValueOp:    "nth_value",
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

// ScalarOperatorTransmitsNulls returns true if the given scalar operator always
// returns NULL when at least one of its inputs is NULL.
func ScalarOperatorTransmitsNulls(op Operator) bool {
	switch op {
	case BitandOp, BitorOp, BitxorOp, PlusOp, MinusOp, MultOp, DivOp, FloorDivOp,
		ModOp, PowOp, EqOp, NeOp, LtOp, GtOp, LeOp, GeOp, LikeOp, NotLikeOp, ILikeOp,
		NotILikeOp, SimilarToOp, NotSimilarToOp, RegMatchOp, NotRegMatchOp, RegIMatchOp,
		NotRegIMatchOp, ConstOp, BBoxCoversOp, BBoxIntersectsOp:
		return true

	default:
		return false
	}
}

// BoolOperatorRequiresNotNullArgs returns true if the operator can never
// evaluate to true if one of its children is NULL.
func BoolOperatorRequiresNotNullArgs(op Operator) bool {
	switch op {
	case
		EqOp, LtOp, LeOp, GtOp, GeOp, NeOp,
		LikeOp, NotLikeOp, ILikeOp, NotILikeOp, SimilarToOp, NotSimilarToOp,
		RegMatchOp, NotRegMatchOp, RegIMatchOp, NotRegIMatchOp, BBoxCoversOp,
		BBoxIntersectsOp:
		return true
	}
	return false
}

// AggregateIgnoresNulls returns true if the given aggregate operator ignores
// rows where its first argument evaluates to NULL. In other words, it always
// evaluates to the same result even if those rows are filtered. For example:
//
//   SELECT string_agg(x, y)
//   FROM (VALUES ('foo', ','), ('bar', ','), (NULL, ',')) t(x, y)
//
// In this example, the NULL row can be removed from the input, and the
// string_agg function still returns the same result. Contrast this to the
// array_agg function:
//
//   SELECT array_agg(x)
//   FROM (VALUES ('foo'), (NULL), ('bar')) t(x)
//
// If the NULL row is removed here, array_agg returns {foo,bar} instead of
// {foo,NULL,bar}.
func AggregateIgnoresNulls(op Operator) bool {
	switch op {

	case AnyNotNullAggOp, AvgOp, BitAndAggOp, BitOrAggOp, BoolAndOp, BoolOrOp,
		ConstNotNullAggOp, CorrOp, CountOp, MaxOp, MinOp, SqrDiffOp, StdDevOp,
		StringAggOp, SumOp, SumIntOp, VarianceOp, XorAggOp, PercentileDiscOp,
		PercentileContOp, STMakeLineOp, STCollectOp, STExtentOp, STUnionOp, StdDevPopOp,
		VarPopOp, CovarPopOp, CovarSampOp, RegressionAvgXOp, RegressionAvgYOp,
		RegressionInterceptOp, RegressionR2Op, RegressionSlopeOp, RegressionSXXOp,
		RegressionSXYOp, RegressionSYYOp, RegressionCountOp:
		return true

	case ArrayAggOp, ConcatAggOp, ConstAggOp, CountRowsOp, FirstAggOp, JsonAggOp,
		JsonbAggOp, JsonObjectAggOp, JsonbObjectAggOp:
		return false

	default:
		panic(errors.AssertionFailedf("unhandled op %s", log.Safe(op)))
	}
}

// AggregateIsNullOnEmpty returns true if the given aggregate operator returns
// NULL when the input set contains no values. This group of aggregates turns
// out to be the inverse of AggregateIsNeverNull in practice.
func AggregateIsNullOnEmpty(op Operator) bool {
	switch op {

	case AnyNotNullAggOp, ArrayAggOp, AvgOp, BitAndAggOp,
		BitOrAggOp, BoolAndOp, BoolOrOp, ConcatAggOp, ConstAggOp,
		ConstNotNullAggOp, CorrOp, FirstAggOp, JsonAggOp, JsonbAggOp,
		MaxOp, MinOp, SqrDiffOp, StdDevOp, STMakeLineOp, StringAggOp, SumOp, SumIntOp,
		VarianceOp, XorAggOp, PercentileDiscOp, PercentileContOp,
		JsonObjectAggOp, JsonbObjectAggOp, StdDevPopOp, STCollectOp, STExtentOp, STUnionOp,
		VarPopOp, CovarPopOp, CovarSampOp, RegressionAvgXOp, RegressionAvgYOp,
		RegressionInterceptOp, RegressionR2Op, RegressionSlopeOp, RegressionSXXOp,
		RegressionSXYOp, RegressionSYYOp:
		return true

	case CountOp, CountRowsOp, RegressionCountOp:
		return false

	default:
		panic(errors.AssertionFailedf("unhandled op %s", log.Safe(op)))
	}
}

// AggregateIsNeverNullOnNonNullInput returns true if the given aggregate
// operator never returns NULL when the input set contains at least one non-NULL
// value. This is true of most aggregates.
//
// For multi-input aggregations, returns true if the aggregate is never NULL
// when all inputs have at least a non-NULL value (though not necessarily on the
// same input row).
func AggregateIsNeverNullOnNonNullInput(op Operator) bool {
	switch op {

	case AnyNotNullAggOp, ArrayAggOp, AvgOp, BitAndAggOp,
		BitOrAggOp, BoolAndOp, BoolOrOp, ConcatAggOp, ConstAggOp,
		ConstNotNullAggOp, CountOp, CountRowsOp, FirstAggOp,
		JsonAggOp, JsonbAggOp, MaxOp, MinOp, SqrDiffOp, STMakeLineOp,
		StringAggOp, SumOp, SumIntOp, XorAggOp, PercentileDiscOp, PercentileContOp,
		JsonObjectAggOp, JsonbObjectAggOp, StdDevPopOp, STCollectOp, STExtentOp, STUnionOp,
		VarPopOp, CovarPopOp, RegressionAvgXOp, RegressionAvgYOp, RegressionSXXOp,
		RegressionSXYOp, RegressionSYYOp, RegressionCountOp:
		return true

	case VarianceOp, StdDevOp, CorrOp, CovarSampOp, RegressionInterceptOp,
		RegressionR2Op, RegressionSlopeOp:
		// These aggregations return NULL if they are given a single not-NULL input.
		return false

	default:
		panic(errors.AssertionFailedf("unhandled op %s", log.Safe(op)))
	}
}

// AggregateIsNeverNull returns true if the given aggregate operator never
// returns NULL, even if the input is empty, or one more more inputs are NULL.
func AggregateIsNeverNull(op Operator) bool {
	switch op {
	case CountOp, CountRowsOp, RegressionCountOp:
		return true
	}
	return false
}

// AggregatesCanMerge returns true if the given inner and outer operators can be
// replaced with a single equivalent operator, assuming the outer operator is
// aggregating on the inner and that both operators are unordered. In other
// words, the inner-outer aggregate pair forms a valid "decomposition" of a
// single aggregate. For example, the following pairs of queries are equivalent:
//
//   SELECT sum(s) FROM (SELECT sum(y) FROM xy GROUP BY x) AS f(s);
//   SELECT sum(y) FROM xy;
//
//   SELECT sum_int(c) FROM (SELECT count(y) FROM xy GROUP BY x) AS f(c);
//   SELECT count(y) FROM xy;
//
// Note: some aggregates like StringAggOp are decomposable in theory, but in
// practice can not be easily merged as in the examples above.
func AggregatesCanMerge(inner, outer Operator) bool {
	switch inner {

	case AnyNotNullAggOp, BitAndAggOp, BitOrAggOp, BoolAndOp,
		BoolOrOp, ConstAggOp, ConstNotNullAggOp, FirstAggOp,
		MaxOp, MinOp, STMakeLineOp, STExtentOp, STUnionOp, SumOp, SumIntOp, XorAggOp:
		return inner == outer

	case CountOp, CountRowsOp:
		// Only SumIntOp can be used here because SumOp outputs a decimal value,
		// while CountOp and CountRowsOp both output int values.
		return outer == SumIntOp

	case ArrayAggOp, AvgOp, ConcatAggOp, CorrOp, JsonAggOp, JsonbAggOp,
		JsonObjectAggOp, JsonbObjectAggOp, PercentileContOp, PercentileDiscOp,
		SqrDiffOp, STCollectOp, StdDevOp, StringAggOp, VarianceOp, StdDevPopOp,
		VarPopOp, CovarPopOp, CovarSampOp, RegressionAvgXOp, RegressionAvgYOp,
		RegressionInterceptOp, RegressionR2Op, RegressionSlopeOp, RegressionSXXOp,
		RegressionSXYOp, RegressionSYYOp, RegressionCountOp:
		return false

	default:
		panic(errors.AssertionFailedf("unhandled ops: %s, %s", log.Safe(inner), log.Safe(outer)))
	}
}

// AggregateIgnoresDuplicates returns true if the output of the given aggregate
// operator does not change when duplicate rows are added to the input.
func AggregateIgnoresDuplicates(op Operator) bool {
	switch op {
	case AnyNotNullAggOp, BitAndAggOp, BitOrAggOp, BoolAndOp, BoolOrOp,
		ConstAggOp, ConstNotNullAggOp, FirstAggOp, MaxOp, MinOp, STExtentOp, STUnionOp:
		return true

	case ArrayAggOp, AvgOp, ConcatAggOp, CountOp, CorrOp, CountRowsOp, SumIntOp,
		SumOp, SqrDiffOp, VarianceOp, StdDevOp, XorAggOp, JsonAggOp, JsonbAggOp,
		StringAggOp, PercentileDiscOp, PercentileContOp, StdDevPopOp, STMakeLineOp,
		VarPopOp, JsonObjectAggOp, JsonbObjectAggOp, STCollectOp, CovarPopOp,
		CovarSampOp, RegressionAvgXOp, RegressionAvgYOp, RegressionInterceptOp,
		RegressionR2Op, RegressionSlopeOp, RegressionSXXOp, RegressionSXYOp,
		RegressionSYYOp, RegressionCountOp:
		return false

	default:
		panic(errors.AssertionFailedf("unhandled op %s", log.Safe(op)))
	}
}

// OpaqueMetadata is an object stored in OpaqueRelExpr and passed
// through to the exec factory.
type OpaqueMetadata interface {
	ImplementsOpaqueMetadata()

	// String is a short description used when printing optimizer trees and when
	// forming error messages; it should be the SQL statement tag.
	String() string

	// Columns returns the columns that are produced by this operator.
	Columns() colinfo.ResultColumns
}

func init() {
	for optOp, treeOp := range ComparisonOpReverseMap {
		ComparisonOpMap[treeOp] = optOp
	}
}
