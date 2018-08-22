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

package props

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// Logical properties describe the content and characteristics of data returned
// by all expression variants within a memo group. While each expression in the
// group may return rows or columns in a different order, or compute the result
// using different algorithms, the same set of data is returned and can then be
// transformed into whatever layout or presentation format that is desired,
// according to the required physical properties.
type Logical struct {
	// Relational contains the set of properties that describe relational
	// operators, like select, join, and project. It is nil for scalar
	// operators.
	Relational *Relational

	// Scalar contains the set of properties that describe scalar operators,
	// like And, Plus, and Const. It is nil for relational operators.
	Scalar *Scalar
}

// AvailableRuleProps is a bit set that indicates when lazily-populated Rule
// properties are initialized and ready for use.
type AvailableRuleProps int

const (
	// PruneCols is set when the Relational.Rule.PruneCols field is populated.
	PruneCols AvailableRuleProps = 1 << iota

	// RejectNullCols is set when the Relational.Rule.RejectNullCols field is
	// populated.
	RejectNullCols

	// InterestingOrderings is set when the Relational.Rule.InterestingOrderings
	// field is populated.
	InterestingOrderings

	// UnfilteredCols is set when the Relational.Rule.UnfilteredCols field is
	// populated.
	UnfilteredCols

	// HasHoistableSubquery is set when the Scalar.Rule.HasHoistableSubquery
	// is populated.
	HasHoistableSubquery
)

// Relational properties are the subset of logical properties that are computed
// for relational expressions that return rows and columns rather than scalar
// values.
type Relational struct {
	// OutputCols is the set of columns that can be projected by the
	// expression. Ordering, naming, and duplication of columns is not
	// representable by this property; those are physical properties.
	OutputCols opt.ColSet

	// NotNullCols is the subset of output columns which cannot be NULL.
	// The NULL-ability of columns flows from the inputs and can also be
	// derived from filters that are NULL-intolerant.
	NotNullCols opt.ColSet

	// OuterCols is the set of columns that are referenced by variables within
	// this relational sub-expression, but are not bound within the scope of
	// the expression. For example:
	//
	//   SELECT *
	//   FROM a
	//   WHERE EXISTS(SELECT * FROM b WHERE b.x = a.x AND b.y = 5)
	//
	// For the inner SELECT expression, a.x is an outer column, meaning that it
	// is defined "outside" the SELECT expression (hence the name "outer"). The
	// SELECT expression binds the b.x and b.y references, so they are not
	// part of the outer column set. The outer SELECT binds the a.x column, and
	// so its outer column set is empty.
	OuterCols opt.ColSet

	// CanHaveSideEffects is true if the subtree rooted at this expression might
	// trigger a run-time error, might modify outside state, or might not always
	// return the same output given the same input. For more details, see the
	// comment for Logical.CanHaveSideEffects.
	CanHaveSideEffects bool

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

	// Cardinality is the number of rows that can be returned from this relational
	// expression. The number of rows will always be between the inclusive Min and
	// Max bounds. If Max=math.MaxUint32, then there is no limit to the number of
	// rows returned by the expression.
	Cardinality Cardinality

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
		InterestingOrderings opt.OrderingSet

		// UnfilteredCols is the set of output columns that have values for every
		// row in their owner table. Rows may be duplicated, but no rows can be
		// missing. For example, an unconstrained, unlimited Scan operator can
		// add all of its output columns to this property, but a Select operator
		// cannot add any columns, as it may have filtered rows.
		//
		// UnfilteredCols is lazily populated by the SimplifyLeftJoinWithFilters
		// and SimplifyRightJoinWithFilters rules. It is only valid once the
		// Rule.Available.UnfilteredCols bit has been set.
		UnfilteredCols opt.ColSet
	}
}

// Scalar properties are the subset of logical properties that are computed for
// scalar expressions that return primitive-valued types.
type Scalar struct {
	// Type is the data type of the scalar expression (int, string, etc).
	Type types.T

	// OuterCols is the set of columns that are referenced by variables within
	// this scalar sub-expression, but are not bound within the scope of the
	// expression. For example:
	//
	//   SELECT *
	//   FROM a
	//   WHERE EXISTS(SELECT * FROM b WHERE b.x = a.x AND b.y = 5)
	//
	// For the EXISTS expression, only a.x is an outer column, meaning that
	// only it is defined "outside" the EXISTS expression (hence the name
	// "outer"). Note that what constitutes an "outer column" is dependent on
	// an expression's location in the query. For example, while the b.x and
	// b.y columns are not outer columns on the EXISTS expression, they *are*
	// outer columns on the inner WHERE condition.
	OuterCols opt.ColSet

	// CanHaveSideEffects is true if the subtree rooted at this expression might
	// trigger a run-time error, might modify outside state, or might not always
	// return the same output given the same input. For more details, see the
	// comment for Logical.CanHaveSideEffects.
	CanHaveSideEffects bool

	// HasCorrelatedSubquery is true if the scalar expression tree contains a
	// subquery having one or more outer columns. The subquery can be a Subquery,
	// Exists, or Any operator. These operators need to be hoisted out of scalar
	// expression trees and turned into top-level apply joins. This property makes
	// detection fast and easy so that the hoister doesn't waste time searching
	// subtrees that don't contain subqueries.
	HasCorrelatedSubquery bool

	// Constraints is the set of constraints deduced from a boolean expression.
	// For the expression to be true, all constraints in the set must be
	// satisfied.
	// This field is populated lazily, as necessary.
	Constraints *constraint.Set

	// TightConstraints is true if the expression is exactly equivalent to the
	// constraints. If it is false, the constraints are weaker than the
	// expression.
	// This field is populated lazily, as necessary.
	TightConstraints bool

	// FuncDeps is a set of functional dependencies (FDs) inferred from a
	// boolean expression. This field is only populated for Filters expressions.
	// FDs that can be inferred from Filters expressions include:
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

// OuterCols is a helper method that returns either the relational or scalar
// OuterCols field, depending on the operator's type.
func (p *Logical) OuterCols() opt.ColSet {
	if p.Scalar != nil {
		return p.Scalar.OuterCols
	}
	return p.Relational.OuterCols
}

// CanHaveSideEffects is true if the expression modifies state outside its own
// scope, or if depends upon state that may change across evaluations. An
// expression can have side effects if it can do any of the following:
//
//   1. Trigger a run-time error
//        10 / col                           -- division by zero error possible
//        crdb_internal.force_error('', '')  -- triggers run-time error
//
//   2. Modify outside session or database state
//        nextval(seq)                -- modifies database sequence value
//        SELECT * FROM [INSERT ...]  -- inserts rows into database
//
//   3. Return different results when repeatedly called with same input
//        ORDER BY random()       -- random can return different values
//        ts < clock_timestamp()  -- clock_timestamp can return different values
//
// The optimizer makes *only* the following side-effect related guarantees:
//
//   1. CASE/IF branches are only evaluated if the branch condition is true.
//      Therefore, the following is guaranteed to never raise a divide by zero
//      error, regardless of how cleverly the optimizer rewrites the expression:
//
//        CASE WHEN divisor<>0 THEN dividend / divisor ELSE NULL END
//
//      While this example is trivial, a more complex example might have
//      correlated subqueries that cannot be hoisted outside the CASE expression
//      in the usual way, since that would trigger premature evaluation.
//
//   2. Expressions with side effects are never treated as constant expressions,
//      even though they do not depend on other columns in the query:
//
//        SELECT * FROM xy ORDER BY random()
//
//      If the random() expression were treated as a constant, then the ORDER BY
//      could be dropped by the optimizer, since ordering by a constant is a
//      no-op. Instead, the optimizer treats it like it would an expression that
//      depends upon a column.
//
//   3. A common table expression (CTE) with side effects will only be evaluated
//      one time. This will typically prevent inlining of the CTE into the query
//      body. For example:
//
//        WITH a AS (INSERT ... RETURNING ...) SELECT * FROM a, a
//
//      Although the "a" CTE is referenced twice, it must be evaluated only one
//      time (and its results cached to satisfy the second reference).
//
// As long as the optimizer provides these guarantees, it is free to rewrite,
// reorder, duplicate, and eliminate as if no side effects were present. As an
// example, the optimizer is free to eliminate the unused "nextval" column in
// this query:
//
//   SELECT x FROM (SELECT nextval(seq), x FROM xy)
//   =>
//   SELECT x FROM xy
//
// It's also allowed to duplicate side-effecting expressions during predicate
// pushdown:
//
//   SELECT * FROM xy INNER JOIN xz ON xy.x=xz.x WHERE xy.x=random()
//   =>
//   SELECT *
//   FROM (SELECT * FROM xy WHERE xy.x=random())
//   INNER JOIN (SELECT * FROM xz WHERE xz.x=random())
//   ON xy.x=xz.x
//
func (p *Logical) CanHaveSideEffects() bool {
	if p.Scalar != nil {
		return p.Scalar.CanHaveSideEffects
	}
	return p.Relational.CanHaveSideEffects
}

// Verify runs consistency checks against the logical properties, in order to
// ensure that they conform to several invariants:
//
//   1. Functional dependencies are internally consistent.
//   2. Not null columns are a subset of output columns.
//   3. Outer columns do not intersect output columns.
//   4. If functional dependencies indicate that the relation can have at most
//      one row, then the cardinality reflects that as well.
//
func (p *Logical) Verify() {
	scalar := p.Scalar
	if scalar != nil {
		scalar.FuncDeps.Verify()

		if p.Relational != nil {
			panic("relational and scalar properties cannot both be set")
		}
		return
	}

	relational := p.Relational
	relational.FuncDeps.Verify()

	if !relational.NotNullCols.SubsetOf(relational.OutputCols) {
		panic(fmt.Sprintf("not null cols %s not a subset of output cols %s",
			relational.NotNullCols, relational.OutputCols))
	}
	if relational.OuterCols.Intersects(relational.OutputCols) {
		panic(fmt.Sprintf("outer cols %s intersect output cols %s",
			relational.OuterCols, relational.OutputCols))
	}
	if relational.FuncDeps.HasMax1Row() {
		if relational.Cardinality.Max > 1 {
			panic(fmt.Sprintf(
				"max cardinality must be <= 1 if FDs have max 1 row: %s", relational.Cardinality))
		}
	}
}
