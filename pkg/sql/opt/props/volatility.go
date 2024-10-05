// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package props

import "github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"

// VolatilitySet tracks the set of operator volatilities contained inside an
// expression. See volatility.V for more info on volatility values.
//
// The reason why we use a set (rather than the "maximum" volatility) is that
// for plan caching purposes, we want to distinguish the case when a stable
// operator is used - regardless of whether a volatile operator is used. For
// example, consider these two statements:
//
//	(1) INSERT INTO t VALUES (gen_random_uuid(), '2020-10-09')
//	(2) INSERT INTO t VALUES (gen_random_uuid(), now())
//
// For (1) we can cache the final optimized plan. For (2), we can only cache the
// memo if we don't constant fold stable operators, and subsequently fold them
// each time we try to execute an instance of the query.
//
// The optimizer makes *only* the following side-effect related guarantees:
//
//  1. CASE/IF branches are only evaluated if the branch condition is true or
//     if all operators are Leakproof. Therefore, the following is guaranteed
//     to never raise a divide by zero error, regardless of how cleverly the
//     optimizer rewrites the expression:
//
//     CASE WHEN divisor<>0 THEN dividend / divisor ELSE NULL END
//
//     While this example is trivial, a more complex example might have
//     correlated subqueries that cannot be hoisted outside the CASE
//     expression in the usual way, since that would trigger premature
//     evaluation.
//
//     However, there is a notable exception to this guarantee. When a branch
//     is an uncorrelated subquery, it will be evaluated if a previous
//     conditional does not evaluate to true at optimization-time. This is due
//     to the fact that subqueries are eagerly evaluated when query execution
//     begins. See #20298.
//
//  2. Volatile expressions are never treated as constant expressions, even
//     though they do not depend on other columns in the query:
//
//     SELECT * FROM xy ORDER BY random()
//
//     If the random() expression were treated as a constant, then the ORDER
//     BY could be dropped by the optimizer, since ordering by a constant is
//     a no-op. Instead, the optimizer treats it like it would an expression
//     that depends upon a column.
//
//  3. A common table expression (CTE) containing Volatile operators will only
//     be evaluated one time. This will typically prevent inlining of the CTE
//     into the query body. For example:
//
//     WITH a AS (INSERT ... RETURNING ...) SELECT * FROM a, a
//
//     Although the "a" CTE is referenced twice, it must be evaluated only
//     one time (and its results cached to satisfy the second reference).
//
// As long as the optimizer provides these guarantees, it is free to rewrite,
// reorder, duplicate, and eliminate as if no side effects were present. As an
// example, the optimizer is free to eliminate the unused "nextval" column in
// this query:
//
//	SELECT x FROM (SELECT nextval(seq), x FROM xy)
//	=>
//	SELECT x FROM xy
//
// It's also allowed to duplicate side-effecting expressions during predicate
// pushdown:
//
//	SELECT * FROM xy INNER JOIN xz ON xy.x=xz.x WHERE xy.x=random()
//	=>
//	SELECT *
//	FROM (SELECT * FROM xy WHERE xy.x=random())
//	INNER JOIN (SELECT * FROM xz WHERE xz.x=random())
//	ON xy.x=xz.x
type VolatilitySet uint8

// Add a volatility to the set.
func (vs *VolatilitySet) Add(v volatility.V) {
	*vs |= volatilityBit(v)
}

// AddImmutable is a convenience shorthand for adding Immutable.
func (vs *VolatilitySet) AddImmutable() {
	vs.Add(volatility.Immutable)
}

// AddStable is a convenience shorthand for adding Stable.
func (vs *VolatilitySet) AddStable() {
	vs.Add(volatility.Stable)
}

// AddVolatile is a convenience shorthand for adding Volatile.
func (vs *VolatilitySet) AddVolatile() {
	vs.Add(volatility.Volatile)
}

// UnionWith sets the receiver to the union of the two volatility sets.
func (vs *VolatilitySet) UnionWith(other VolatilitySet) {
	*vs = *vs | other
}

// IsLeakproof returns true if the set is empty or only contains
// Leakproof.
func (vs VolatilitySet) IsLeakproof() bool {
	return vs == 0 || vs == volatilityBit(volatility.Leakproof)
}

// HasImmutable returns true if the set contains Immutable.
func (vs VolatilitySet) HasImmutable() bool {
	return (vs & volatilityBit(volatility.Immutable)) != 0
}

// HasStable returns true if the set contains Stable.
func (vs VolatilitySet) HasStable() bool {
	return (vs & volatilityBit(volatility.Stable)) != 0
}

// HasVolatile returns true if the set contains Volatile.
func (vs VolatilitySet) HasVolatile() bool {
	return (vs & volatilityBit(volatility.Volatile)) != 0
}

// ToVolatility returns the least restrictive volatility value in the set.
func (vs VolatilitySet) ToVolatility() volatility.V {
	if vs.HasVolatile() {
		return volatility.Volatile
	} else if vs.HasStable() {
		return volatility.Stable
	} else if vs.HasImmutable() {
		return volatility.Immutable
	}
	return volatility.Leakproof
}

func (vs VolatilitySet) String() string {
	// The only properties we care about are IsLeakproof(), HasStable() and
	// HasVolatile(). We print one of the strings below:
	//
	//    String            | IsLeakproof | HasStable | HasVolatile
	//   -------------------+-------------+-----------+-------------
	//    "leakproof"       | true        | false     | false
	//    "immutable"       | false       | false     | false
	//    "stable"          | false       | true      | false
	//    "volatile"        | false       | false     | true
	//    "stable+volatile" | false       | true      | true
	//
	// These are the only valid combinations for these properties.
	//
	if vs.IsLeakproof() {
		return "leakproof"
	}
	hasStable := vs.HasStable()
	hasVolatile := vs.HasVolatile()
	switch {
	case !hasStable && !hasVolatile:
		return "immutable"
	case hasStable && !hasVolatile:
		return "stable"
	case hasVolatile && !hasStable:
		return "volatile"
	default:
		return "stable+volatile"
	}
}

func volatilityBit(v volatility.V) VolatilitySet {
	return 1 << VolatilitySet(v)
}
