// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memo

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/errors"
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

// poisonedGroup wraps an original exprGroup and allows firstExpr() to work
// (so that copy operations can proceed) but panics on relational() and
// bestProps() calls. This is used to mark UDF body expressions that should
// not have their properties accessed directly without first being copied to
// a new memo.
type poisonedGroup struct {
	original exprGroup
}

// MakePoisonedGroup creates a new poisonedGroup that wraps the given group.
func MakePoisonedGroup(original exprGroup) *poisonedGroup {
	return &poisonedGroup{original: original}
}

var _ exprGroup = (*poisonedGroup)(nil)

// firstExpr delegates to the original group so that copy operations can work.
func (p *poisonedGroup) firstExpr() RelExpr {
	return p.original.firstExpr()
}

func (*poisonedGroup) relational() *props.Relational {
	panic(errors.AssertionFailedf(
		"poisoned expression accessed without copying to new memo; " +
			"UDF body expressions must be copied before use"))
}

func (*poisonedGroup) bestProps() *bestProps {
	panic(errors.AssertionFailedf(
		"poisoned expression accessed without copying to new memo; " +
			"UDF body expressions must be copied before use"))
}

// IsPoisoned returns true if the group is a poisonedGroup.
func IsPoisoned(g exprGroup) bool {
	_, ok := g.(*poisonedGroup)
	return ok
}
