// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ordering

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

func selectCanProvideOrdering(expr memo.RelExpr, required *props.OrderingChoice) bool {
	// Select operator can always pass through ordering to its input.
	return true
}

func selectBuildChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	if childIdx != 0 {
		return props.OrderingChoice{}
	}
	child := parent.(*memo.SelectExpr).Input

	// Use interesting orderings from the child to "guide" the required ordering
	// that gets passed down and make it more likely that the child will be able
	// to provide the ordering. This step is needed before the call to
	// trimColumnGroups, since that function will arbitrarily remove columns from
	// each group to ensure all columns in the group are equivalent according to
	// the child FDs.
	//
	// For example, suppose the required ordering is +(1|2),+3, and the child has
	// an interesting ordering of +1,+2,+3. +1,+2,+3 implies +(1|2),+3, so the
	// child can provide the ordering. However, trimColumnGroups will convert
	// +(1|2),+3 to +1,+3, which the child cannot provide. We should instead pass
	// down +1,+2,+3 as the required ordering to avoid an unnecessary sort.
	//
	// See #33023 for more details.
	orders := DeriveInterestingOrderings(child)
	for i := range orders {
		if orders[i].Implies(required) {
			// Get the common prefix of this ordering and the required ordering to
			// make sure the child required ordering is no stronger than needed. Since
			// we know that orders[i] implies required, the result of CommonPrefix
			// will also imply required. For example, if orders[i] is +1,+2 and
			// required is +1, we should only pass down +1 as the child required
			// ordering.
			order := orders[i].CommonPrefix(required)
			required = &order
			break
		}
	}
	return trimColumnGroups(required, &child.Relational().FuncDeps)
}

// trimColumnGroups removes columns from ColumnOrderingChoice groups as
// necessary, so that all columns in each group are equivalent according to
// the given FDs. It is used when the parent expression can have column
// equivalences that the input expression does not (for example a Select with an
// equality condition); the columns in a group must be equivalent at the level
// of the operator where the ordering is required.
func trimColumnGroups(required *props.OrderingChoice, fds *props.FuncDepSet) props.OrderingChoice {
	res := *required
	copied := false
	for i := range res.Columns {
		c := &res.Columns[i]
		eqGroup := fds.ComputeEquivGroup(c.AnyID())
		if !c.Group.SubsetOf(eqGroup) {
			if !copied {
				res = res.Copy()
				copied = true
			}
			res.Columns[i].Group = c.Group.Intersection(eqGroup)
		}
	}
	return res
}

func selectBuildProvided(expr memo.RelExpr, required *props.OrderingChoice) opt.Ordering {
	s := expr.(*memo.SelectExpr)
	rel := s.Relational()
	// We don't need to remap columns, but we want to remove columns that are now
	// unnecessary (e.g. because the Select constrains them to be constant).
	return remapProvided(s.Input.ProvidedPhysical().Ordering, &rel.FuncDeps, rel.OutputCols)
}
