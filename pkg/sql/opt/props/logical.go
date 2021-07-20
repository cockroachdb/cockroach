// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package props

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
)

// AvailableRuleProps is a bit set that indicates when lazily-populated Rule
// properties are initialized and ready for use.
type AvailableRuleProps int8

const (
	// PruneCols is set when the Relational.Rule.PruneCols field is populated.
	PruneCols AvailableRuleProps = 1 << iota

	// RejectNullCols is set when the Relational.Rule.RejectNullCols field is
	// populated.
	RejectNullCols

	// InterestingOrderings is set when the Relational.Rule.InterestingOrderings
	// field is populated.
	InterestingOrderings

	// HasHoistableSubquery is set when the Scalar.Rule.HasHoistableSubquery
	// is populated.
	HasHoistableSubquery

	// UnfilteredCols is set when the Relational.Rule.UnfilteredCols field is
	// populated.
	UnfilteredCols

	// WithUses is set when the Shared.Rule.WithUses field is populated.
	WithUses
)

// Shared are properties that are shared by both relational and scalar
// expressions.
type Shared struct {
	// Populated is set to true once the properties have been built for the
	// operator.
	Populated bool

	// HasSubquery is true if the subtree rooted at this node contains a subquery.
	// The subquery can be a Subquery, Exists, Any, or ArrayFlatten expression.
	// Subqueries are the only place where a relational node can be nested within a
	// scalar expression.
	HasSubquery bool

	// HasCorrelatedSubquery is true if the scalar expression tree contains a
	// subquery having one or more outer columns. The subquery can be a Subquery,
	// Exists, or Any operator. These operators usually need to be hoisted out of
	// scalar expression trees and turned into top-level apply joins. This
	// property makes detection fast and easy so that the hoister doesn't waste
	// time searching subtrees that don't contain subqueries.
	HasCorrelatedSubquery bool

	// VolatilitySet contains the set of volatilities contained in the expression.
	VolatilitySet VolatilitySet

	// CanMutate is true if the subtree rooted at this expression contains at
	// least one operator that modifies schema (like CreateTable) or writes or
	// deletes rows (like Insert).
	CanMutate bool

	// HasPlaceholder is true if the subtree rooted at this expression contains
	// at least one Placeholder operator.
	HasPlaceholder bool

	// OuterCols is the set of columns that are referenced by variables within
	// this sub-expression, but are not bound within the scope of the expression.
	// For example:
	//
	//   SELECT *
	//   FROM a
	//   WHERE EXISTS(SELECT * FROM b WHERE b.x = a.x AND b.y = 5)
	//
	// For the EXISTS expression, a.x is an outer column, meaning that it is
	// defined "outside" the EXISTS expression (hence the name "outer"). The
	// SELECT expression binds the b.x and b.y references, so they are not part
	// of the outer column set. The outer SELECT binds the a.x column, and so
	// its outer column set is empty.
	//
	// Note that what constitutes an "outer column" is dependent on an
	// expression's location in the query. For example, while the b.x and b.y
	// columns are not outer columns on the EXISTS expression, they *are* outer
	// columns on the inner WHERE condition.
	OuterCols opt.ColSet

	// Rule props are lazily calculated and typically only apply to a single
	// rule. See the comment above Relational.Rule for more details.
	Rule struct {
		// WithUses tracks information about the WithScans inside the given
		// expression which reference WithIDs outside of that expression.
		WithUses WithUsesMap
	}
}

// WithUsesMap stores information about each WithScan referencing an outside
// WithID, grouped by each WithID.
type WithUsesMap map[opt.WithID]WithUseInfo

// WithUseInfo contains information about the usage of a specific WithID.
type WithUseInfo struct {
	// Count is the number of WithScan operators which reference this WithID.
	Count int

	// UsedCols is the union of columns used by all WithScan operators which
	// reference this WithID.
	UsedCols opt.ColSet
}

// Relational properties describe the content and characteristics of relational
// data returned by all expression variants within a memo group. While each
// expression in the group may return rows or columns in a different order, or
// compute the result using different algorithms, the same set of data is
// returned and can then be  transformed into whatever layout or presentation
// format that is desired, according to the required physical properties.
type Relational struct {
	Shared

	// OutputCols is the set of columns that can be projected by the expression.
	// Ordering, naming, and duplication of columns is not representable by this
	// property; those are physical properties.
	OutputCols opt.ColSet

	// NotNullCols is the subset of output columns which cannot be NULL. The
	// nullability of columns flows from the inputs and can also be derived from
	// filters that reject nulls.
	NotNullCols opt.ColSet

	// Cardinality is the number of rows that can be returned from this relational
	// expression. The number of rows will always be between the inclusive Min and
	// Max bounds. If Max=math.MaxUint32, then there is no limit to the number of
	// rows returned by the expression.
	Cardinality Cardinality

	// FuncDepSet is a set of functional dependencies (FDs) that encode useful
	// relationships between columns in a base or derived relation. Given two sets
	// of columns A and B, a functional dependency A-->B holds if A uniquely
	// determines B. In other words, if two different rows have equal values for
	// columns in A, then those two rows will also have equal values for columns
	// in B. For example:
	//
	//   a1 a2 b1
	//   --------
	//   1  2  5
	//   1  2  5
	//
	// FDs assist the optimizer in proving useful properties about query results.
	// This information powers many optimizations, including eliminating
	// unnecessary DISTINCT operators, simplifying ORDER BY columns, removing
	// Max1Row operators, and mapping semi-joins to inner-joins.
	//
	// The methods that are most useful for optimizations are:
	//   Key: extract a candidate key for the relation
	//   ColsAreStrictKey: determine if a set of columns uniquely identify rows
	//   ReduceCols: discard redundant columns to create a candidate key
	//
	// For more details, see the header comment for FuncDepSet.
	FuncDeps FuncDepSet

	// Stats is the set of statistics that apply to this relational expression.
	// See statistics.go and memo/statistics_builder.go for more details.
	Stats Statistics

	// Rule encapsulates the set of properties that are maintained to assist
	// with specific sets of transformation rules. They are not intended to be
	// general purpose in nature. Typically, they're used by rules which need to
	// decide whether to push operators down into the tree. These properties
	// "bubble up" information about the subtree which can aid in that decision.
	//
	// Whereas the other logical relational properties are filled in by the memo
	// package upon creation of a new memo group, the rules properties are filled
	// in by one of the transformation packages, since deriving rule properties
	// is so closely tied with maintenance of the rules that depend upon them.
	// For example, the PruneCols set is connected to the PruneCols normalization
	// rules. The decision about which columns to add to PruneCols depends upon
	// what works best for those rules. Neither the rules nor their properties
	// can be considered in isolation, without considering the other.
	Rule struct {
		// Available contains bits that indicate whether lazily-populated Rule
		// properties have been initialized. For example, if the UnfilteredCols
		// bit is set, then the Rule.UnfilteredCols field has been initialized
		// and is ready for use.
		Available AvailableRuleProps

		// PruneCols is the subset of output columns that can potentially be
		// eliminated by one of the PruneCols normalization rules. Those rules
		// operate by pushing a Project operator down the tree that discards
		// unused columns. For example:
		//
		//   SELECT y FROM xyz WHERE x=1 ORDER BY y LIMIT 1
		//
		// The z column is never referenced, either by the filter or by the
		// limit, and would be part of the PruneCols set for the Limit operator.
		// The final Project operator could then push down a pruning Project
		// operator that eliminated the z column from its subtree.
		//
		// PruneCols is built bottom-up. It typically starts out containing the
		// complete set of output columns in a leaf expression, but quickly
		// empties out at higher levels of the expression tree as the columns
		// are referenced. Drawing from the example above:
		//
		//   Limit PruneCols : [z]
		//   Select PruneCols: [y, z]
		//   Scan PruneCols  : [x, y, z]
		//
		// Only a small number of relational operators are capable of pruning
		// columns (e.g. Scan, Project). A pruning Project operator pushed down
		// the tree must journey downwards until it finds a pruning-capable
		// operator. If a column is part of PruneCols, then it is guaranteed that
		// such an operator exists at the end of the journey. Operators that are
		// not capable of filtering columns (like Explain) will not add any of
		// their columns to this set.
		//
		// PruneCols is lazily populated by rules in prune_cols.opt. It is
		// only valid once the Rule.Available.PruneCols bit has been set.
		PruneCols opt.ColSet

		// RejectNullCols is the subset of nullable output columns that can
		// potentially be made not-null by one of the RejectNull normalization
		// rules. Those rules work in concert with the predicate pushdown rules
		// to synthesize a "col IS NOT NULL" filter and push it down the tree.
		// See the header comments for the reject_nulls.opt file for more
		// information and an example.
		//
		// RejectNullCols is built bottom-up by rulePropsBuilder, and only contains
		// nullable outer join columns that can be simplified. The columns can be
		// propagated up through multiple operators, giving higher levels of the
		// tree a window into the structure of the tree several layers down. In
		// particular, the null rejection rules use this property to determine when
		// it's advantageous to synthesize a new "IS NOT NULL" filter. Without this
		// information, the rules can clutter the tree with extraneous and
		// marginally useful null filters.
		//
		// RejectNullCols is lazily populated by rules in reject_nulls.opt. It is
		// only valid once the Rule.Available.RejectNullCols bit has been set.
		RejectNullCols opt.ColSet

		// InterestingOrderings is a list of orderings that potentially could be
		// provided by the operator without sorting. Interesting orderings normally
		// come from scans (index orders) and are bubbled up through some operators.
		//
		// Note that all prefixes of an interesting order are "interesting"; the
		// list doesn't need to contain orderings that are prefixes of some other
		// ordering in the list.
		//
		// InterestingOrderings is lazily populated by interesting_orderings.go.
		// It is only valid once the Rule.Available.InterestingOrderings bit has
		// been set.
		InterestingOrderings OrderingSet

		// UnfilteredCols is the set of all columns for which rows from their base
		// table are guaranteed not to have been filtered. Rows may be duplicated,
		// but no rows can be missing. Even columns which are not output columns are
		// included as long as table rows are guaranteed not filtered. For example,
		// an unconstrained, unlimited Scan operator can add all columns from its
		// table to this property, but a Select operator cannot add any columns, as
		// it may have filtered rows.
		//
		// UnfilteredCols is lazily populated by GetJoinMultiplicityFromInputs. It
		// is only valid once the Rule.Available.UnfilteredCols bit has been set.
		UnfilteredCols opt.ColSet
	}
}

// Scalar properties are logical properties that are computed for scalar
// expressions that return primitive-valued types. Scalar properties are
// lazily populated on request.
type Scalar struct {
	Shared

	// Constraints is the set of constraints deduced from a boolean expression.
	// For the expression to be true, all constraints in the set must be
	// satisfied. The constraints are not guaranteed to be exactly equivalent to
	// the expression, see TightConstraints.
	Constraints *constraint.Set

	// FuncDeps is a set of functional dependencies (FDs) inferred from a
	// boolean expression. This field is only populated for Filters expressions.
	//
	//  - Constant column FDs such as ()-->(1,2) from conjuncts such as
	//    x = 5 AND y = 10.
	//  - Equivalent column FDs such as (1)==(2), (2)==(1) from conjuncts such
	//    as x = y.
	//
	// It is useful to calculate FDs on Filters expressions, because it allows
	// additional filters to be inferred for push-down. For example, consider
	// the query:
	//
	//   SELECT * FROM a, b WHERE a.x = b.x AND a.x > 5;
	//
	// By adding the equivalency FD for a.x = b.x, we can infer an additional
	// filter, b.x > 5. This allows us to rewrite the query as:
	//
	//   SELECT * FROM (SELECT * FROM a WHERE a.x > 5) AS a,
	//     (SELECT * FROM b WHERE b.x > 5) AS b WHERE a.x = b.x;
	//
	// For more details, see the header comment for FuncDepSet.
	FuncDeps FuncDepSet

	// TightConstraints is true if the expression is exactly equivalent to the
	// constraints. If it is false, the constraints are weaker than the
	// expression.
	TightConstraints bool

	// Rule encapsulates the set of properties that are maintained to assist
	// with specific sets of transformation rules. See the Relational.Rule
	// comment for more details.
	Rule struct {
		// Available contains bits that indicate whether lazily-populated Rule
		// properties have been initialized. For example, if the
		// HasHoistableSubquery bit is set, then the Rule.HasHoistableSubquery
		// field has been initialized and is ready for use.
		Available AvailableRuleProps

		// HasHoistableSubquery is true if the scalar expression tree contains a
		// subquery having one or more outer columns, and if the subquery needs
		// to be hoisted up into its parent query as part of query decorrelation.
		// The subquery can be a Subquery, Exists, or Any operator. These operators
		// need to be hoisted out of scalar expression trees and turned into top-
		// level apply joins. This property makes detection fast and easy so that
		// the hoister doesn't waste time searching subtrees that don't contain
		// subqueries.
		//
		// HasHoistableSubquery is lazily populated by rules in decorrelate.opt.
		// It is only valid once the Rule.Available.HasHoistableSubquery bit has
		// been set.
		HasHoistableSubquery bool
	}
}

// IsAvailable returns true if the specified rule property has been populated
// on this relational properties instance.
func (r *Relational) IsAvailable(p AvailableRuleProps) bool {
	return (r.Rule.Available & p) != 0
}

// SetAvailable sets the available bits for the given properties, in order to
// mark them as populated on this relational properties instance.
func (r *Relational) SetAvailable(p AvailableRuleProps) {
	r.Rule.Available |= p
}

// IsAvailable returns true if the specified rule property has been populated
// on this scalar properties instance.
func (s *Scalar) IsAvailable(p AvailableRuleProps) bool {
	return (s.Rule.Available & p) != 0
}

// SetAvailable sets the available bits for the given properties, in order to
// mark them as populated on this scalar properties instance.
func (s *Scalar) SetAvailable(p AvailableRuleProps) {
	s.Rule.Available |= p
}
