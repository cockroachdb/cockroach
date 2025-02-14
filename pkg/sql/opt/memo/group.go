// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memo

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
)

// exprGroup represents a group of relational query plans that are logically
// equivalent to on another. The group points to the first member of the group,
// and subsequent members can be accessed via calls to RelExpr.NextExpr. The
// group maintains the logical properties shared by members of the group, as
// well as the physical properties and cost of the best expression in the group
// once optimization is complete.
//
// See comments for Memo, RelExpr, Relational, and Physical for more details.
type exprGroup interface {
	// firstExpr points to the first member expression in the group. Other members
	// of the group can be accessed via calls to RelExpr.NextExpr.
	firstExpr() RelExpr

	// relational are the relational properties shared by members of the group.
	relational() *props.Relational

	// bestProps returns a per-group instance of bestProps. This is the zero
	// value until optimization is complete.
	bestProps() *bestProps
}

// bestProps contains the properties of the "best" expression in group. The best
// expression is the expression which is part of the lowest-cost tree for the
// overall query. It is well-defined because the lowest-cost tree does not
// contain multiple expressions from the same group.
//
// These are not properties of the group per se but they are stored within each
// group for efficiency.
type bestProps struct {
	// Required properties with respect to which the best expression was
	// optimized.
	required *physical.Required

	// Provided properties, which must be compatible with the required properties.
	provided *physical.Provided

	// Cost of the best expression.
	cost Cost
}
